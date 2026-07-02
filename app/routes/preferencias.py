from typing import Any, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.services.auth_service import get_current_user
from app.services.preferencias_service import (
    add_bookmark,
    check_bookmark_changes,
    delete_preferencia,
    get_bookmarks,
    get_preferencia,
    get_todas_preferencias,
    remove_bookmark,
    set_preferencia,
)

router = APIRouter()


# ── Preferencias genéricas ────────────────────────────────────────────────────

@router.get("")
def listar_preferencias(user: dict = Depends(get_current_user)):
    return get_todas_preferencias(user["email"])


@router.get("/{key}")
def obtener_preferencia(key: str, user: dict = Depends(get_current_user)):
    val = get_preferencia(user["email"], key)
    return {"key": key, "value": val}


class PrefBody(BaseModel):
    value: Any


@router.put("/{key}")
def guardar_preferencia(key: str, body: PrefBody, user: dict = Depends(get_current_user)):
    set_preferencia(user["email"], key, body.value)
    return {"mensaje": "Preferencia guardada"}


@router.delete("/{key}")
def borrar_preferencia(key: str, user: dict = Depends(get_current_user)):
    delete_preferencia(user["email"], key)
    return {"mensaje": "Preferencia eliminada"}


# ── Bookmarks ─────────────────────────────────────────────────────────────────

@router.get("/bookmarks/list")
def listar_bookmarks(user: dict = Depends(get_current_user)):
    return get_bookmarks(user["email"])


class BookmarkBody(BaseModel):
    pac_id: int
    entidad: Optional[str] = None
    descripcion: Optional[str] = None
    v_total: Optional[float] = None
    provincia: Optional[str] = None


@router.post("/bookmarks")
def agregar_bookmark(body: BookmarkBody, user: dict = Depends(get_current_user)):
    add_bookmark(
        user["email"],
        body.pac_id,
        body.entidad,
        body.descripcion,
        body.v_total,
        body.provincia,
    )
    return {"mensaje": "Favorito agregado"}


@router.delete("/bookmarks/{pac_id}")
def eliminar_bookmark(pac_id: int, user: dict = Depends(get_current_user)):
    remove_bookmark(user["email"], pac_id)
    return {"mensaje": "Favorito eliminado"}


@router.get("/bookmarks/changes")
def verificar_cambios_bookmarks(user: dict = Depends(get_current_user)):
    """Detecta bookmarks cuyo contrato tiene una versión más reciente con valor diferente."""
    return check_bookmark_changes(user["email"])
