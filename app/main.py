import os
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.db import check_db_health
from app.routes.admin import router as admin_router
from app.routes.auth import router as auth_router
from app.routes.entidades import router as entidades_router
from app.routes.pac import router as pac_router
from app.routes.preferencias import router as preferencias_router
from app.routes.scraper import router as scraper_router
from app.services.auth_service import inicializar_tabla_usuarios
from app.services.preferencias_service import inicializar_tablas_preferencias
from app.services.scraper_service import inicializar_tablas, iniciar_scraping

scheduler = BackgroundScheduler()


def _scheduled_scraping() -> None:
    try:
        iniciar_scraping()
    except Exception:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    inicializar_tablas()
    inicializar_tablas_preferencias()
    inicializar_tabla_usuarios()
    scheduler.add_job(
        _scheduled_scraping,
        "cron",
        day_of_week="mon",
        hour=2,
        minute=0,
        id="weekly_scraping",
        replace_existing=True,
    )
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(
    title="PAC API",
    version="2.0.0",
    description="API para consultar partidas PAC desde SQL Server",
    lifespan=lifespan,
)

_local_origins = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5174",
    "http://localhost:3000",
]
_extra = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
_allowed_origins = _local_origins + _extra

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pac_router, prefix="/api/pac", tags=["PAC"])
app.include_router(entidades_router, prefix="/api/entidades", tags=["Entidades"])
app.include_router(scraper_router, prefix="/api/scraper", tags=["Scraper"])
app.include_router(preferencias_router, prefix="/api/preferencias", tags=["Preferencias"])
app.include_router(auth_router, prefix="/api/auth", tags=["Auth"])
app.include_router(admin_router, prefix="/api/admin", tags=["Admin"])


@app.get("/")
def root():
    return {"message": "PAC API funcionando"}


@app.get("/health")
def health():
    return {
        "api": "ok",
        "database": "ok" if check_db_health() else "error",
    }
