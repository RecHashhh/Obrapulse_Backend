from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from app.services.auth_service import get_current_user, require_analista_or_admin
from app.services.scraper_service import get_logs, get_status, iniciar_scraping

router = APIRouter()


@router.post("/run")
def run_scraper(entidad_id: Optional[int] = None, _: dict = Depends(require_analista_or_admin)):
    """Dispara el scraping en background. Retorna 409 si ya hay uno en curso.
    Si se pasa entidad_id, solo procesa esa entidad."""
    try:
        log_id = iniciar_scraping(entidad_id=entidad_id)
        return {"mensaje": "Scraping iniciado correctamente", "log_id": log_id}
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.get("/status")
def status(_: dict = Depends(get_current_user)):
    return get_status()


@router.get("/logs")
def logs(page: int = 1, page_size: int = 20, _: dict = Depends(get_current_user)):
    return get_logs(page, page_size)
