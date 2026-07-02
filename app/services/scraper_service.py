import threading
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import text

SQL_CREATE_ENTIDADES = """
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'tb')
BEGIN EXEC('CREATE SCHEMA tb'); END;

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'entidades' AND s.name = 'tb'
)
BEGIN
    CREATE TABLE tb.entidades (
        id INT IDENTITY(1,1) PRIMARY KEY,
        Nombre_Comercial NVARCHAR(500) NOT NULL,
        Url NVARCHAR(1000) NOT NULL,
        Provincia NVARCHAR(255) NULL,
        Ciudad NVARCHAR(255) NULL,
        Activo BIT NOT NULL DEFAULT 1,
        Fecha_Creacion DATETIME NOT NULL DEFAULT GETDATE(),
        Fecha_Modificacion DATETIME NULL
    );
END;
"""

SQL_CREATE_SCRAPER_LOGS = """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'scraper_logs' AND s.name = 'tb'
)
BEGIN
    CREATE TABLE tb.scraper_logs (
        id INT IDENTITY(1,1) PRIMARY KEY,
        inicio DATETIME NOT NULL,
        fin DATETIME NULL,
        estado NVARCHAR(20) NOT NULL DEFAULT 'corriendo',
        entidades_procesadas INT NULL,
        registros_insertados INT NULL,
        mensaje NVARCHAR(MAX) NULL,
        error NVARCHAR(MAX) NULL
    );
END;
"""

# In-memory state (single process — survives between requests)
_state: dict = {
    "running": False,
    "started_at": None,
    "last_log_id": None,
}
_state_lock = threading.Lock()


def _engine():
    from app.db import get_engine
    return get_engine()


# ── Inicialización ────────────────────────────────────────────────────────────

def inicializar_tablas() -> None:
    from app.scraper.storage import crear_tabla_pac_partidas
    engine = _engine()
    with engine.begin() as conn:
        conn.execute(text(SQL_CREATE_ENTIDADES))
        conn.execute(text(SQL_CREATE_SCRAPER_LOGS))
    crear_tabla_pac_partidas()


# ── CRUD Entidades ────────────────────────────────────────────────────────────

def listar_entidades(solo_activas: bool = False) -> list[dict]:
    where = "WHERE Activo = 1" if solo_activas else ""
    sql = f"""
        SELECT id, Nombre_Comercial, Url, Provincia, Ciudad, Activo,
               Fecha_Creacion, Fecha_Modificacion
        FROM tb.entidades {where}
        ORDER BY Nombre_Comercial
    """
    with _engine().connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return [dict(r._mapping) for r in rows]


def crear_entidad(nombre: str, url: str, provincia: Optional[str], ciudad: Optional[str]) -> int:
    sql = text("""
        INSERT INTO tb.entidades (Nombre_Comercial, Url, Provincia, Ciudad)
        OUTPUT INSERTED.id
        VALUES (:nombre, :url, :provincia, :ciudad)
    """)
    with _engine().begin() as conn:
        result = conn.execute(sql, {"nombre": nombre, "url": url, "provincia": provincia, "ciudad": ciudad})
        return result.fetchone()[0]


def actualizar_entidad(id: int, nombre: str, url: str, provincia: Optional[str], ciudad: Optional[str], activo: bool) -> None:
    sql = text("""
        UPDATE tb.entidades
        SET Nombre_Comercial = :nombre, Url = :url, Provincia = :provincia,
            Ciudad = :ciudad, Activo = :activo, Fecha_Modificacion = GETDATE()
        WHERE id = :id
    """)
    with _engine().begin() as conn:
        conn.execute(sql, {"id": id, "nombre": nombre, "url": url,
                           "provincia": provincia, "ciudad": ciudad, "activo": int(activo)})


def eliminar_entidad(id: int) -> None:
    with _engine().begin() as conn:
        conn.execute(text("DELETE FROM tb.entidades WHERE id = :id"), {"id": id})


def importar_desde_json(entidades: list[dict]) -> tuple[int, int]:
    """Upsert de entidades desde lista de dicts con claves 'Nombre Comercial', 'Url', 'Provincia', 'Ciudad'.
    Si la URL ya existe, actualiza los datos. Si no existe, inserta.
    Retorna (nuevas, actualizadas)."""
    nuevas = 0
    actualizadas = 0
    with _engine().begin() as conn:
        for e in entidades:
            nombre = str(e.get("Nombre Comercial", "") or "").strip()
            url = str(e.get("Url", "") or "").strip()
            if not nombre or not url:
                continue
            existing = conn.execute(
                text("SELECT id FROM tb.entidades WHERE Url = :url"), {"url": url}
            ).fetchone()
            if existing:
                conn.execute(
                    text("""
                        UPDATE tb.entidades
                        SET Nombre_Comercial = :n, Provincia = :p, Ciudad = :c,
                            Fecha_Modificacion = GETDATE()
                        WHERE Url = :u
                    """),
                    {"n": nombre, "u": url, "p": e.get("Provincia"), "c": e.get("Ciudad")},
                )
                actualizadas += 1
            else:
                conn.execute(
                    text("INSERT INTO tb.entidades (Nombre_Comercial, Url, Provincia, Ciudad) VALUES (:n, :u, :p, :c)"),
                    {"n": nombre, "u": url, "p": e.get("Provincia"), "c": e.get("Ciudad")},
                )
                nuevas += 1
    return nuevas, actualizadas


# ── Scraper status / logs ─────────────────────────────────────────────────────

def get_status() -> dict:
    with _engine().connect() as conn:
        row = conn.execute(text("""
            SELECT TOP 1 id, inicio, fin, estado, entidades_procesadas,
                         registros_insertados, mensaje, error
            FROM tb.scraper_logs ORDER BY inicio DESC
        """)).fetchone()
    last_run = dict(row._mapping) if row else None
    with _state_lock:
        running = _state["running"]
        started_at = _state["started_at"]
    return {
        "running": running,
        "started_at": started_at.isoformat() if started_at else None,
        "last_run": last_run,
    }


def get_logs(page: int = 1, page_size: int = 20) -> dict:
    offset = (page - 1) * page_size
    with _engine().connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM tb.scraper_logs")).scalar()
        rows = conn.execute(text(f"""
            SELECT id, inicio, fin, estado, entidades_procesadas,
                   registros_insertados, mensaje, error
            FROM tb.scraper_logs
            ORDER BY inicio DESC
            OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY
        """)).fetchall()
    return {
        "items": [dict(r._mapping) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# ── Background scraping ───────────────────────────────────────────────────────

def _run_background(log_id: int, entidad_id: Optional[int] = None) -> None:
    from app.scraper.engine import ejecutar_scraping
    from app.scraper.storage import guardar_dataframe_sin_duplicados

    engine = _engine()
    try:
        entidades = listar_entidades(solo_activas=True)
        if entidad_id is not None:
            entidades = [e for e in entidades if e["id"] == entidad_id]
        if not entidades:
            raise ValueError("No hay entidades activas para scrapear." if entidad_id is None else f"Entidad {entidad_id} no encontrada o no está activa.")

        df_empresas = pd.DataFrame(entidades).rename(columns={"Nombre_Comercial": "Nombre Comercial"})
        df_partidas = ejecutar_scraping(df_empresas)

        registros = 0
        if not df_partidas.empty:
            registros = guardar_dataframe_sin_duplicados(df_partidas)

        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE tb.scraper_logs
                SET fin = GETDATE(), estado = 'completado',
                    entidades_procesadas = :ep, registros_insertados = :ri,
                    mensaje = :msg
                WHERE id = :id
            """), {
                "ep": len(entidades),
                "ri": registros,
                "msg": f"Completado. {registros} registros nuevos insertados.",
                "id": log_id,
            })
    except Exception as exc:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE tb.scraper_logs
                SET fin = GETDATE(), estado = 'error', error = :err
                WHERE id = :id
            """), {"err": str(exc)[:4000], "id": log_id})
    finally:
        with _state_lock:
            _state["running"] = False
            _state["started_at"] = None


def iniciar_scraping(entidad_id: Optional[int] = None) -> int:
    with _state_lock:
        if _state["running"]:
            raise ValueError("Ya hay un scraping en curso. Espera a que finalice.")
        _state["running"] = True
        _state["started_at"] = datetime.now()

    try:
        with _engine().begin() as conn:
            result = conn.execute(text("""
                INSERT INTO tb.scraper_logs (inicio, estado)
                OUTPUT INSERTED.id
                VALUES (GETDATE(), 'corriendo')
            """))
            log_id = result.fetchone()[0]
        _state["last_log_id"] = log_id
    except Exception:
        with _state_lock:
            _state["running"] = False
            _state["started_at"] = None
        raise

    thread = threading.Thread(target=_run_background, args=(log_id, entidad_id), daemon=True)
    thread.start()
    return log_id
