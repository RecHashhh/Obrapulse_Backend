from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr

from app.services.auth_service import (
    actualizar_usuario,
    crear_usuario,
    eliminar_usuario,
    listar_usuarios,
    require_admin,
)

router = APIRouter()


class UsuarioCreate(BaseModel):
    email: str
    nombre: Optional[str] = None
    rol: str = "analista"


class UsuarioUpdate(BaseModel):
    rol: Optional[str] = None
    activo: Optional[bool] = None
    nombre: Optional[str] = None


@router.get("/usuarios")
def list_usuarios(admin: dict = Depends(require_admin)):
    return listar_usuarios()


@router.post("/usuarios", status_code=201)
def create_usuario(body: UsuarioCreate, admin: dict = Depends(require_admin)):
    roles_validos = {"administrador", "analista", "consultor"}
    if body.rol not in roles_validos:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Rol inválido. Use: {roles_validos}")
    return crear_usuario(body.email, body.nombre, body.rol)


@router.put("/usuarios/{user_id}")
def update_usuario(user_id: int, body: UsuarioUpdate, admin: dict = Depends(require_admin)):
    if body.rol is not None:
        roles_validos = {"administrador", "analista", "consultor"}
        if body.rol not in roles_validos:
            from fastapi import HTTPException
            raise HTTPException(status_code=400, detail=f"Rol inválido. Use: {roles_validos}")
    return actualizar_usuario(user_id, rol=body.rol, activo=body.activo, nombre=body.nombre)


@router.delete("/usuarios/{user_id}")
def delete_usuario(user_id: int, admin: dict = Depends(require_admin)):
    return eliminar_usuario(user_id, requester_email=admin["email"])
