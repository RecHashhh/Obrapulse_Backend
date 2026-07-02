from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.services.auth_service import get_current_user, require_analista_or_admin
from app.services.scraper_service import (
    actualizar_entidad,
    crear_entidad,
    eliminar_entidad,
    importar_desde_json,
    listar_entidades,
)

router = APIRouter()


class EntidadCreate(BaseModel):
    nombre: str
    url: str
    provincia: Optional[str] = None
    ciudad: Optional[str] = None


class EntidadUpdate(BaseModel):
    nombre: str
    url: str
    provincia: Optional[str] = None
    ciudad: Optional[str] = None
    activo: bool = True


@router.get("")
def listar(solo_activas: bool = False, _: dict = Depends(get_current_user)):
    return listar_entidades(solo_activas)


@router.post("", status_code=201)
def crear(body: EntidadCreate, _: dict = Depends(require_analista_or_admin)):
    new_id = crear_entidad(body.nombre, body.url, body.provincia, body.ciudad)
    return {"id": new_id, "mensaje": "Entidad creada correctamente"}


@router.put("/{id}")
def actualizar(id: int, body: EntidadUpdate, _: dict = Depends(require_analista_or_admin)):
    actualizar_entidad(id, body.nombre, body.url, body.provincia, body.ciudad, body.activo)
    return {"mensaje": "Entidad actualizada correctamente"}


@router.delete("/{id}")
def eliminar(id: int, _: dict = Depends(require_analista_or_admin)):
    eliminar_entidad(id)
    return {"mensaje": "Entidad eliminada correctamente"}


@router.post("/importar-json")
def importar_json(entidades: list[dict], _: dict = Depends(require_analista_or_admin)):
    nuevas, actualizadas = importar_desde_json(entidades)
    return {
        "importados": nuevas,
        "actualizados": actualizadas,
        "mensaje": f"{nuevas} entidades nuevas, {actualizadas} actualizadas",
    }
