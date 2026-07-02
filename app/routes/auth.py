from fastapi import APIRouter, Depends

from app.services.auth_service import get_current_user

router = APIRouter()


@router.get("/me")
def auth_me(user: dict = Depends(get_current_user)):
    """Devuelve el perfil del usuario autenticado y actualiza su último acceso."""
    return user
