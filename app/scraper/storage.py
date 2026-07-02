import hashlib
from datetime import date

import pandas as pd
from sqlalchemy import text

SCHEMA_NAME = "tb"
TABLE_NAME = "pac_partidas"

SQL_CREATE_SCHEMA = f"""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{SCHEMA_NAME}')
BEGIN EXEC('CREATE SCHEMA {SCHEMA_NAME}'); END;
"""

SQL_CREATE_TABLE = f"""
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = '{TABLE_NAME}' AND s.name = '{SCHEMA_NAME}'
)
BEGIN
    CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (
        id INT IDENTITY(1,1) PRIMARY KEY,
        Nro NVARCHAR(50) NULL,
        Partida_Pres NVARCHAR(255) NULL,
        CPC NVARCHAR(255) NULL,
        T_Compra NVARCHAR(100) NULL,
        T_Regimen NVARCHAR(100) NULL,
        Fondo_BID NVARCHAR(255) NULL,
        Tipo_Presupuesto NVARCHAR(255) NULL,
        Tipo_Producto NVARCHAR(255) NULL,
        Cat_Electronico NVARCHAR(255) NULL,
        Procedimiento NVARCHAR(255) NULL,
        Descripcion NVARCHAR(MAX) NULL,
        Cantidad NVARCHAR(100) NULL,
        Unidad_Medida NVARCHAR(100) NULL,
        Costo_Unitario NVARCHAR(100) NULL,
        V_Total NVARCHAR(100) NULL,
        Extra NVARCHAR(255) NULL,
        Periodo NVARCHAR(100) NULL,
        V_Total_Numeric DECIMAL(18,2) NULL,
        Tipo_Tabla NVARCHAR(50) NULL,
        Entidad NVARCHAR(500) NULL,
        url NVARCHAR(1000) NULL,
        Nombre_Comercial NVARCHAR(500) NULL,
        Provincia NVARCHAR(255) NULL,
        Ciudad NVARCHAR(255) NULL,
        Fecha_Carga DATE NULL,
        hash_registro NVARCHAR(32) NOT NULL
    );
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes i
    INNER JOIN sys.objects o ON i.object_id = o.object_id
    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
    WHERE i.name = 'UX_{TABLE_NAME}_hash' AND o.name = '{TABLE_NAME}' AND s.name = '{SCHEMA_NAME}'
)
BEGIN
    CREATE UNIQUE INDEX UX_{TABLE_NAME}_hash ON {SCHEMA_NAME}.{TABLE_NAME}(hash_registro);
END;
"""

# Índices para acelerar las queries de filtrado y agrupación
SQL_CREATE_INDEXES = f"""
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_{TABLE_NAME}_Provincia' AND object_id = OBJECT_ID('{SCHEMA_NAME}.{TABLE_NAME}'))
    CREATE INDEX IX_{TABLE_NAME}_Provincia ON {SCHEMA_NAME}.{TABLE_NAME}(Provincia) INCLUDE (V_Total_Numeric, Entidad, Ciudad);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_{TABLE_NAME}_Ciudad' AND object_id = OBJECT_ID('{SCHEMA_NAME}.{TABLE_NAME}'))
    CREATE INDEX IX_{TABLE_NAME}_Ciudad ON {SCHEMA_NAME}.{TABLE_NAME}(Ciudad) INCLUDE (V_Total_Numeric, Entidad, Provincia);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_{TABLE_NAME}_Entidad' AND object_id = OBJECT_ID('{SCHEMA_NAME}.{TABLE_NAME}'))
    CREATE INDEX IX_{TABLE_NAME}_Entidad ON {SCHEMA_NAME}.{TABLE_NAME}(Entidad) INCLUDE (V_Total_Numeric, Provincia, Ciudad);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_{TABLE_NAME}_T_Compra' AND object_id = OBJECT_ID('{SCHEMA_NAME}.{TABLE_NAME}'))
    CREATE INDEX IX_{TABLE_NAME}_T_Compra ON {SCHEMA_NAME}.{TABLE_NAME}(T_Compra) INCLUDE (V_Total_Numeric);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_{TABLE_NAME}_Procedimiento' AND object_id = OBJECT_ID('{SCHEMA_NAME}.{TABLE_NAME}'))
    CREATE INDEX IX_{TABLE_NAME}_Procedimiento ON {SCHEMA_NAME}.{TABLE_NAME}(Procedimiento) INCLUDE (V_Total_Numeric);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_{TABLE_NAME}_Fecha_Carga' AND object_id = OBJECT_ID('{SCHEMA_NAME}.{TABLE_NAME}'))
    CREATE INDEX IX_{TABLE_NAME}_Fecha_Carga ON {SCHEMA_NAME}.{TABLE_NAME}(Fecha_Carga DESC) INCLUDE (V_Total_Numeric, Provincia, Entidad);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_{TABLE_NAME}_V_Total_Numeric' AND object_id = OBJECT_ID('{SCHEMA_NAME}.{TABLE_NAME}'))
    CREATE INDEX IX_{TABLE_NAME}_V_Total_Numeric ON {SCHEMA_NAME}.{TABLE_NAME}(V_Total_Numeric DESC);
"""


def _get_engine():
    from app.db import get_engine
    return get_engine()


def crear_tabla_pac_partidas() -> None:
    engine = _get_engine()
    with engine.begin() as conn:
        conn.execute(text(SQL_CREATE_SCHEMA))
        conn.execute(text(SQL_CREATE_TABLE))
    # Crear índices en transacciones separadas (no se puede junto con CREATE TABLE)
    for stmt in SQL_CREATE_INDEXES.strip().split("\n\n"):
        stmt = stmt.strip()
        if stmt:
            with engine.begin() as conn:
                conn.execute(text(stmt))


def normalizar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "Fecha_Carga" in df.columns:
        df["Fecha_Carga"] = pd.to_datetime(df["Fecha_Carga"], errors="coerce").dt.date
    if "V_Total_Numeric" in df.columns:
        df["V_Total_Numeric"] = pd.to_numeric(df["V_Total_Numeric"], errors="coerce")
    for col in df.columns:
        if col != "V_Total_Numeric":
            df[col] = df[col].where(df[col].notna(), None)
            if df[col].dtype == "object":
                df[col] = df[col].astype(str)
                df[col] = df[col].replace({"nan": None, "None": None})
    return df


def generar_hash_registro(row: pd.Series) -> str:
    # Fecha_Carga excluida: es metadata de carga, no identidad del contrato.
    # Incluirla causaba que el mismo contrato se insertase de nuevo cada día.
    campos = [
        str(row.get("Entidad", "") or ""),
        str(row.get("Partida_Pres", "") or ""),
        str(row.get("Descripcion", "") or ""),
        str(row.get("Procedimiento", "") or ""),
        str(row.get("url", "") or ""),
        str(row.get("T_Compra", "") or ""),
        str(row.get("V_Total", "") or ""),
    ]
    return hashlib.md5("|".join(campos).encode("utf-8")).hexdigest()


def preparar_dataframe_para_sql(df: pd.DataFrame) -> pd.DataFrame:
    df = normalizar_dataframe(df)
    columnas_esperadas = [
        "Nro", "Partida_Pres", "CPC", "T_Compra", "T_Regimen", "Fondo_BID",
        "Tipo_Presupuesto", "Tipo_Producto", "Cat_Electronico", "Procedimiento",
        "Descripcion", "Cantidad", "Unidad_Medida", "Costo_Unitario", "V_Total",
        "Extra", "Periodo", "V_Total_Numeric", "Tipo_Tabla", "Entidad", "url",
        "Nombre_Comercial", "Provincia", "Ciudad", "Fecha_Carga",
    ]
    for col in columnas_esperadas:
        if col not in df.columns:
            df[col] = None
    df = df[columnas_esperadas]
    df["Fecha_Carga"] = pd.to_datetime(df["Fecha_Carga"], errors="coerce").dt.date
    df["Fecha_Carga"] = df["Fecha_Carga"].fillna(date.today())
    df["hash_registro"] = df.apply(generar_hash_registro, axis=1)
    df = df.drop_duplicates(subset=["hash_registro"])
    return df


def guardar_dataframe_sin_duplicados(
    df: pd.DataFrame,
    tabla: str = TABLE_NAME,
    esquema: str = SCHEMA_NAME,
) -> int:
    if df.empty:
        return 0

    engine = _get_engine()
    df = preparar_dataframe_para_sql(df)
    tabla_tmp = f"{tabla}_tmp"

    with engine.begin() as conn:
        conn.execute(text(f"""
            IF OBJECT_ID('{esquema}.{tabla_tmp}', 'U') IS NOT NULL
                DROP TABLE {esquema}.{tabla_tmp};
        """))

    df.to_sql(tabla_tmp, con=engine, schema=esquema, if_exists="replace", index=False, chunksize=5000)

    columnas_sql = ", ".join(df.columns.tolist())
    merge_sql = f"""
    INSERT INTO {esquema}.{tabla} ({columnas_sql})
    SELECT {columnas_sql} FROM {esquema}.{tabla_tmp} tmp
    WHERE NOT EXISTS (
        SELECT 1 FROM {esquema}.{tabla} t WHERE t.hash_registro = tmp.hash_registro
    );
    DROP TABLE {esquema}.{tabla_tmp};
    """

    with engine.begin() as conn:
        conn.execute(text(merge_sql))

    return len(df)
