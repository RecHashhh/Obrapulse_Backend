# ObraPulse Backend

API REST para consulta, analitica y exportacion de datos PAC, desarrollada con FastAPI y conectada a SQL Server.

## Resumen

Este backend provee:

- Endpoints para KPIs, tablas y analitica contextual de PAC.
- Filtros dinamicos por entidad, territorio, tipo de compra, procedimiento y rangos de fecha/valor.
- Exportacion de datos en CSV y Excel.
- Endpoint de salud de API y base de datos.

## Stack Tecnologico

- FastAPI
- Uvicorn
- SQLAlchemy
- pyodbc (SQL Server)
- pandas / openpyxl (exportaciones)

## Estructura del Proyecto

```text
app/
  core/
    config.py        # variables de entorno y configuracion
  routes/
    pac.py           # endpoints API PAC
  services/
    pac_service.py   # consultas SQL y logica de negocio
  db.py              # engine y health check de base de datos
  main.py            # app FastAPI y middlewares
```

## Requisitos

- Python 3.11+ recomendado
- SQL Server accesible por red
- Cadena de conexion valida en DATABASE_URL

## Variables de Entorno

Crear archivo .env en la raiz del proyecto:

```env
DATABASE_URL=Driver={ODBC Driver 18 for SQL Server};Server=TU_SERVIDOR;Database=TU_BD;UID=TU_USUARIO;PWD=TU_PASSWORD;TrustServerCertificate=yes;
API_HOST=127.0.0.1
API_PORT=8000
API_DEBUG=True
```

Nota: en despliegue (Render) se configura DATABASE_URL en la seccion Environment Variables.

## Instalacion Local

```bash
# Crear el entorno virtual
python -m venv .venv

# En PowerShell (recomendado):
# Si obtienes error de ejecución de scripts, habilita para el usuario actual:
# Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned -Force
# Luego activa el entorno:
. .venv\Scripts\Activate.ps1

# Alternativa (CMD):
.venv\Scripts\activate.bat

# O, para una activación puntual sin cambiar la política:
# powershell -ExecutionPolicy Bypass -NoProfile -Command ". .venv\Scripts\Activate.ps1"

# Instalar dependencias
pip install -r requirements.txt
```

## Ejecucion Local

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

API disponible en:

- http://localhost:8000
- http://localhost:8000/docs
- http://localhost:8000/health

## Endpoints Principales

Base PAC:

- GET /api/pac
- GET /api/pac/kpis
- GET /api/pac/dashboard-contextual
- GET /api/pac/catalogos-dinamicos

Rankings y distribuciones:

- GET /api/pac/top-provincias
- GET /api/pac/top-ciudades
- GET /api/pac/top-entidades
- GET /api/pac/top-procedimientos
- GET /api/pac/distribucion-tipo-compra
- GET /api/pac/distribucion-procedimiento
- GET /api/pac/evolucion-fecha
- GET /api/pac/histograma-montos

Territorial:

- GET /api/pac/top-entidades-por-provincia
- GET /api/pac/entidades-por-provincia

Exportacion:

- GET /api/pac/export/csv
- GET /api/pac/export/excel

## Despliegue en Render

Este repositorio incluye Dockerfile y render.yaml.

Pasos:

1. Conectar repositorio en Render como Web Service.
2. Runtime: Docker (automatico por Dockerfile).
3. Definir variable DATABASE_URL en Render.
4. Deploy.
5. Verificar salud en /health.

## Integracion con Frontend

En el frontend (Vercel/Netlify), configurar:

```env
VITE_API_URL=https://TU-BACKEND.onrender.com
```

## Seguridad y Produccion

- No subir .env al repositorio.
- Restringir CORS a dominios permitidos en produccion.
- Rotar credenciales y usar usuarios de base con permisos minimos.

## Autoria

Desarrollado por William Garzon.
