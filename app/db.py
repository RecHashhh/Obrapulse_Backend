from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from app.core.config import settings

engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,       # valida conexión antes de usarla del pool
    pool_recycle=1800,        # descarta conexiones después de 30 min (evita que SQL Server las cierre primero)
    pool_size=5,              # conexiones persistentes en el pool
    max_overflow=10,          # conexiones extra bajo carga (total máx 15)
    pool_timeout=30,          # espera máx 30s para obtener una conexión del pool
    connect_args={"timeout": 20},  # timeout de red al abrir nueva conexión
    future=True,
)

def get_engine():
    return engine

def check_db_health() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except OperationalError:
        return False
    except Exception:
        return False