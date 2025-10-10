import os
from dotenv import load_dotenv

# Cargar variables desde .env
load_dotenv()

# Configuración SQL Server
SQLSERVER_CONFIG = {
    "user": os.getenv("SQLSERVER_USER"),
    "password": os.getenv("SQLSERVER_PASSWORD"),
    "host": os.getenv("SQLSERVER_HOST"),
    "port": os.getenv("SQLSERVER_PORT"),
    "database": os.getenv("SQLSERVER_DB"),
}

# Configuración PostgreSQL
POSTGRES_CONFIG = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
}
