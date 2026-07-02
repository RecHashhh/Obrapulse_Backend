#!/bin/bash
set -e

# Instala ODBC Driver 17 si no está presente (solo en primer arranque)
if ! command -v odbcinst &> /dev/null; then
    echo "[startup] Instalando ODBC Driver 17 for SQL Server..."
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
    echo "deb [arch=amd64,armhf,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" \
        > /etc/apt/sources.list.d/mssql-release.list
    apt-get update -q
    ACCEPT_EULA=Y DEBIAN_FRONTEND=noninteractive apt-get install -y msodbcsql17 unixodbc-dev
    echo "[startup] ODBC Driver instalado."
fi

echo "[startup] Iniciando uvicorn..."
uvicorn app.main:app --host 0.0.0.0 --port 8000
