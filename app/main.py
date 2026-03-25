from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.db import check_db_health
from app.routes.pac import router as pac_router

app = FastAPI(
    title="PAC API",
    version="1.0.0",
    description="API para consultar partidas PAC desde SQL Server",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pac_router, prefix="/api/pac", tags=["PAC"])


@app.get("/")
def root():
    return {"message": "PAC API funcionando"}


@app.get("/health")
def health():
    return {
        "api": "ok",
        "database": "ok" if check_db_health() else "error"
    }