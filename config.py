#!/usr/bin/env python3
"""
Конфигурация для генератора SQL VIEW.
Содержит настройки подключения к БД и пути к файлам.
"""

import os
from pathlib import Path

# Базовый путь проекта
BASE_DIR = Path(__file__).parent

# Параметры подключения к базе данных
MSSQL_HOST = os.getenv("MSSQL_HOST", "localhost")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "database_name")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME", "username")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "password")

# Формируем строку подключения
def get_connection_string() -> str:
    """
    Формирует строку подключения к MS SQL Server.
    
    Returns:
        Строка подключения
    """
    return (
        f"Server={MSSQL_HOST};"
        f"Database={MSSQL_DATABASE};"
        f"UID={MSSQL_USERNAME};"
        f"PWD={MSSQL_PASSWORD};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=10;"
    )

# Пути к файлам по умолчанию
DEFAULT_STRUCTURE_FILE = BASE_DIR / "input" / "Структура.docx"
DEFAULT_OUTPUT_DIR = BASE_DIR / "output"

# Настройки генерации по умолчанию
DEFAULT_MAX_DEPTH = 5
DEFAULT_FIX_DATES = True

# Порт для Streamlit UI
STREAMLIT_PORT = 8512

















