"""
Servicio de preferencias de usuario.
Persiste en DB lo que antes se guardaba en localStorage (vistas guardadas, favoritos, series comparador).
Por ahora usa user_id = "anonimo"; cuando se integre Microsoft Auth se reemplaza por el sub/oid del token.
"""
import json
from typing import Optional
from sqlalchemy import text

SQL_CREATE_TABLAS = """
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'tb')
BEGIN EXEC('CREATE SCHEMA tb'); END;

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'user_preferences' AND s.name = 'tb'
)
BEGIN
    CREATE TABLE tb.user_preferences (
        id INT IDENTITY(1,1) PRIMARY KEY,
        user_id NVARCHAR(255) NOT NULL,
        pref_key NVARCHAR(100) NOT NULL,
        pref_value NVARCHAR(MAX) NULL,
        updated_at DATETIME NOT NULL DEFAULT GETDATE(),
        CONSTRAINT UQ_user_preferences UNIQUE (user_id, pref_key)
    );
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'user_bookmarks' AND s.name = 'tb'
)
BEGIN
    CREATE TABLE tb.user_bookmarks (
        id INT IDENTITY(1,1) PRIMARY KEY,
        user_id NVARCHAR(255) NOT NULL,
        pac_id INT NOT NULL,
        Entidad NVARCHAR(500) NULL,
        Descripcion NVARCHAR(MAX) NULL,
        V_Total_Numeric DECIMAL(18,2) NULL,
        Provincia NVARCHAR(255) NULL,
        created_at DATETIME NOT NULL DEFAULT GETDATE(),
        CONSTRAINT UQ_user_bookmarks UNIQUE (user_id, pac_id)
    );
END;
"""


def _engine():
    from app.db import get_engine
    return get_engine()


def inicializar_tablas_preferencias() -> None:
    with _engine().begin() as conn:
        conn.execute(text(SQL_CREATE_TABLAS))


# ── Preferencias genéricas (clave-valor JSON) ─────────────────────────────────

def get_preferencia(user_id: str, key: str, default=None):
    with _engine().connect() as conn:
        row = conn.execute(
            text("SELECT pref_value FROM tb.user_preferences WHERE user_id=:uid AND pref_key=:key"),
            {"uid": user_id, "key": key},
        ).fetchone()
    if row is None:
        return default
    try:
        return json.loads(row[0])
    except Exception:
        return row[0]


def set_preferencia(user_id: str, key: str, value) -> None:
    serialized = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
    with _engine().begin() as conn:
        conn.execute(text("""
            MERGE tb.user_preferences AS target
            USING (SELECT :uid AS user_id, :key AS pref_key) AS src
            ON target.user_id = src.user_id AND target.pref_key = src.pref_key
            WHEN MATCHED THEN
                UPDATE SET pref_value = :val, updated_at = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (user_id, pref_key, pref_value) VALUES (:uid, :key, :val);
        """), {"uid": user_id, "key": key, "val": serialized})


def delete_preferencia(user_id: str, key: str) -> None:
    with _engine().begin() as conn:
        conn.execute(
            text("DELETE FROM tb.user_preferences WHERE user_id=:uid AND pref_key=:key"),
            {"uid": user_id, "key": key},
        )


def get_todas_preferencias(user_id: str) -> dict:
    with _engine().connect() as conn:
        rows = conn.execute(
            text("SELECT pref_key, pref_value FROM tb.user_preferences WHERE user_id=:uid"),
            {"uid": user_id},
        ).fetchall()
    result = {}
    for row in rows:
        try:
            result[row[0]] = json.loads(row[1])
        except Exception:
            result[row[0]] = row[1]
    return result


# ── Bookmarks ─────────────────────────────────────────────────────────────────

def get_bookmarks(user_id: str) -> list[dict]:
    with _engine().connect() as conn:
        rows = conn.execute(
            text("""
                SELECT b.id, b.pac_id, b.Entidad, b.Descripcion, b.V_Total_Numeric,
                       b.Provincia, b.created_at
                FROM tb.user_bookmarks b
                WHERE b.user_id = :uid
                ORDER BY b.created_at DESC
            """),
            {"uid": user_id},
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def add_bookmark(user_id: str, pac_id: int, entidad: Optional[str], descripcion: Optional[str],
                 v_total: Optional[float], provincia: Optional[str]) -> None:
    with _engine().begin() as conn:
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM tb.user_bookmarks WHERE user_id=:uid AND pac_id=:pid)
                INSERT INTO tb.user_bookmarks (user_id, pac_id, Entidad, Descripcion, V_Total_Numeric, Provincia)
                VALUES (:uid, :pid, :entidad, :desc, :vt, :prov)
        """), {"uid": user_id, "pid": pac_id, "entidad": entidad, "desc": descripcion, "vt": v_total, "prov": provincia})


def remove_bookmark(user_id: str, pac_id: int) -> None:
    with _engine().begin() as conn:
        conn.execute(
            text("DELETE FROM tb.user_bookmarks WHERE user_id=:uid AND pac_id=:pid"),
            {"uid": user_id, "pid": pac_id},
        )


def check_bookmark_changes(user_id: str) -> list[dict]:
    """
    Para cada bookmark del usuario, busca si existe una versión más reciente del mismo
    contrato (mismo Entidad + Descripcion) con un valor diferente al guardado.
    Retorna los que tienen cambios detectados.
    """
    with _engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT
                b.pac_id,
                b.Entidad AS entidad_guardada,
                b.Descripcion AS descripcion,
                b.V_Total_Numeric AS valor_guardado,
                b.Provincia AS provincia_guardada,
                b.created_at AS guardado_en,
                latest.id AS latest_pac_id,
                latest.V_Total_Numeric AS valor_actual,
                latest.Fecha_Carga AS fecha_actual,
                latest.V_Total AS v_total_actual_str
            FROM tb.user_bookmarks b
            OUTER APPLY (
                SELECT TOP 1
                    p.id,
                    p.V_Total_Numeric,
                    p.Fecha_Carga,
                    p.V_Total
                FROM tb.pac_partidas p
                WHERE p.Entidad = b.Entidad
                  AND (b.Descripcion IS NULL OR p.Descripcion = b.Descripcion)
                  AND p.id != b.pac_id
                ORDER BY p.Fecha_Carga DESC
            ) latest
            WHERE b.user_id = :uid
              AND latest.id IS NOT NULL
              AND ABS(ISNULL(latest.V_Total_Numeric, 0) - ISNULL(b.V_Total_Numeric, 0)) > 0.01
        """), {"uid": user_id}).fetchall()
    return [dict(r._mapping) for r in rows]
