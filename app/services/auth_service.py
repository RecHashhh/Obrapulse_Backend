import json
import threading
import time
from typing import Optional

import jwt
import requests as http_requests
from fastapi import Depends, Header, HTTPException
from jwt.algorithms import RSAAlgorithm
from sqlalchemy import text

from app.core.config import settings
from app.db import get_engine

ADMIN_EMAIL = "william.garzon@yachaytech.edu.ec"

_jwks_cache: dict = {"keys": None, "ts": 0.0}
_jwks_lock = threading.Lock()
_JWKS_TTL = 3600.0


def _get_jwks() -> list:
    with _jwks_lock:
        now = time.monotonic()
        if _jwks_cache["keys"] and (now - _jwks_cache["ts"] < _JWKS_TTL):
            return _jwks_cache["keys"]
        url = (
            f"https://login.microsoftonline.com/{settings.AZURE_TENANT_ID}"
            "/discovery/v2.0/keys"
        )
        resp = http_requests.get(url, timeout=10)
        resp.raise_for_status()
        keys = resp.json()["keys"]
        _jwks_cache["keys"] = keys
        _jwks_cache["ts"] = now
        return keys


def validar_token_azure(token: str) -> dict:
    try:
        header = jwt.get_unverified_header(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Token malformado")

    kid = header.get("kid")
    if not kid:
        raise HTTPException(status_code=401, detail="Token sin kid")

    try:
        jwks = _get_jwks()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"No se pudieron obtener las claves de Azure: {exc}",
        )

    rsa_key = None
    for key_data in jwks:
        if key_data.get("kid") == kid:
            rsa_key = RSAAlgorithm.from_jwk(json.dumps(key_data))
            break

    if rsa_key is None:
        # Force-refresh JWKS once in case keys were rotated
        _jwks_cache["ts"] = 0.0
        for key_data in _get_jwks():
            if key_data.get("kid") == kid:
                rsa_key = RSAAlgorithm.from_jwk(json.dumps(key_data))
                break

    if rsa_key is None:
        raise HTTPException(status_code=401, detail="Clave de firma no encontrada")

    try:
        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=["RS256"],
            audience=settings.AZURE_CLIENT_ID,
            issuer=f"https://login.microsoftonline.com/{settings.AZURE_TENANT_ID}/v2.0",
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidAudienceError:
        raise HTTPException(status_code=401, detail="Token para otra aplicación")
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail=f"Token inválido: {exc}")

    return payload


# ── Tabla usuarios ────────────────────────────────────────────────────────────

def inicializar_tabla_usuarios() -> None:
    engine = get_engine()
    sql_create = """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'tb' AND TABLE_NAME = 'usuarios'
    )
    CREATE TABLE tb.usuarios (
        id             INT IDENTITY(1,1) PRIMARY KEY,
        azure_oid      NVARCHAR(255) NULL,
        email          NVARCHAR(255) NOT NULL,
        nombre         NVARCHAR(500) NULL,
        cargo_azure    NVARCHAR(255) NULL,
        rol            NVARCHAR(50)  NOT NULL DEFAULT 'analista',
        activo         BIT           NOT NULL DEFAULT 1,
        fecha_creacion DATETIME      NOT NULL DEFAULT GETDATE(),
        ultimo_acceso  DATETIME      NULL,
        CONSTRAINT UQ_tb_usuarios_email UNIQUE (email)
    );
    """
    admins = [
        ("william.garzon@yachaytech.edu.ec", "William Garzon"),
        ("agarzon@ripconciv.com", "A. Garzon"),
    ]
    seed_parts = []
    for email, nombre in admins:
        seed_parts.append(
            f"IF NOT EXISTS (SELECT 1 FROM tb.usuarios WHERE email = '{email}')\n"
            f"    INSERT INTO tb.usuarios (email, nombre, rol, activo)\n"
            f"    VALUES ('{email}', '{nombre}', 'administrador', 1);"
        )
    sql_seed = "\n".join(seed_parts)
    with engine.begin() as conn:
        conn.execute(text(sql_create))
        conn.execute(text(sql_seed))


def _row_to_dict(row) -> dict:
    return {
        "id": row[0],
        "azure_oid": row[1],
        "email": row[2],
        "nombre": row[3],
        "cargo_azure": row[4],
        "rol": row[5],
        "activo": bool(row[6]),
        "fecha_creacion": row[7].isoformat() if row[7] else None,
        "ultimo_acceso": row[8].isoformat() if row[8] else None,
    }


def get_usuario_by_email(email: str) -> Optional[dict]:
    engine = get_engine()
    sql = text(
        "SELECT id, azure_oid, email, nombre, cargo_azure, rol, activo,"
        " fecha_creacion, ultimo_acceso FROM tb.usuarios WHERE email = :email"
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"email": email.lower()}).fetchone()
    return _row_to_dict(row) if row else None


def actualizar_ultimo_acceso(user_id: int, azure_oid: str, nombre: str) -> None:
    engine = get_engine()
    sql = text(
        "UPDATE tb.usuarios SET ultimo_acceso = GETDATE(),"
        " azure_oid = COALESCE(azure_oid, :oid),"
        " nombre = CASE WHEN nombre IS NULL OR nombre = '' THEN :nombre ELSE nombre END"
        " WHERE id = :id"
    )
    with engine.begin() as conn:
        conn.execute(sql, {"oid": azure_oid, "nombre": nombre, "id": user_id})


# ── Admin CRUD ────────────────────────────────────────────────────────────────

def listar_usuarios() -> list:
    engine = get_engine()
    sql = text(
        "SELECT id, azure_oid, email, nombre, cargo_azure, rol, activo,"
        " fecha_creacion, ultimo_acceso FROM tb.usuarios ORDER BY fecha_creacion DESC"
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return [_row_to_dict(r) for r in rows]


def crear_usuario(email: str, nombre: Optional[str], rol: str) -> dict:
    engine = get_engine()
    email = email.lower().strip()
    with engine.connect() as conn:
        existing = conn.execute(
            text("SELECT id FROM tb.usuarios WHERE email = :e"), {"e": email}
        ).fetchone()
    if existing:
        raise HTTPException(status_code=409, detail="El email ya está registrado")

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO tb.usuarios (email, nombre, rol, activo)"
                " VALUES (:email, :nombre, :rol, 1)"
            ),
            {"email": email, "nombre": nombre or None, "rol": rol},
        )
    return get_usuario_by_email(email)


def actualizar_usuario(
    user_id: int,
    rol: Optional[str] = None,
    activo: Optional[bool] = None,
    nombre: Optional[str] = None,
) -> dict:
    engine = get_engine()
    sets = []
    params: dict = {"id": user_id}
    if rol is not None:
        sets.append("rol = :rol")
        params["rol"] = rol
    if activo is not None:
        sets.append("activo = :activo")
        params["activo"] = 1 if activo else 0
    if nombre is not None:
        sets.append("nombre = :nombre")
        params["nombre"] = nombre
    if not sets:
        raise HTTPException(status_code=400, detail="Nada que actualizar")

    with engine.begin() as conn:
        conn.execute(text(f"UPDATE tb.usuarios SET {', '.join(sets)} WHERE id = :id"), params)

    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, azure_oid, email, nombre, cargo_azure, rol, activo,"
                " fecha_creacion, ultimo_acceso FROM tb.usuarios WHERE id = :id"
            ),
            {"id": user_id},
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return _row_to_dict(row)


def eliminar_usuario(user_id: int, requester_email: str) -> dict:
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT email FROM tb.usuarios WHERE id = :id"), {"id": user_id}
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    if row[0].lower() == requester_email.lower():
        raise HTTPException(status_code=400, detail="No puedes eliminar tu propia cuenta")
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM tb.usuarios WHERE id = :id"), {"id": user_id})
    return {"ok": True}


# ── FastAPI dependencies ──────────────────────────────────────────────────────

def get_current_user(authorization: Optional[str] = Header(default=None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="No autenticado")

    token = authorization.split(" ", 1)[1]
    claims = validar_token_azure(token)

    email = (
        claims.get("preferred_username")
        or claims.get("email")
        or claims.get("upn")
        or ""
    ).lower()
    azure_oid = claims.get("oid", "")
    nombre = claims.get("name", "")

    if not email:
        raise HTTPException(status_code=401, detail="Token sin email")

    user = get_usuario_by_email(email)
    if not user or not user["activo"]:
        raise HTTPException(
            status_code=403,
            detail="No tienes acceso a ObraPulse. Contacta al administrador.",
        )

    actualizar_ultimo_acceso(user["id"], azure_oid, nombre)
    return user


def require_admin(user: dict = Depends(get_current_user)) -> dict:
    if user["rol"] != "administrador":
        raise HTTPException(status_code=403, detail="Requiere rol Administrador")
    return user


def require_analista_or_admin(user: dict = Depends(get_current_user)) -> dict:
    if user["rol"] not in ("administrador", "analista"):
        raise HTTPException(
            status_code=403,
            detail="Requiere rol Analista o Administrador",
        )
    return user
