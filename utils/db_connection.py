#!/usr/bin/env python3
"""
Утилиты для подключения к базе данных MS SQL Server.
"""

from typing import Optional
from mssql_python import connect
from mssql_python.exceptions import DatabaseError, InterfaceError
import config


def create_connection(connection_string: Optional[str] = None):
    """
    Создает подключение к базе данных MS SQL Server.
    
    Args:
        connection_string: Строка подключения. Если не указана, используется из config.
        
    Returns:
        Объект подключения к БД
        
    Raises:
        InterfaceError: Ошибка подключения
        DatabaseError: Ошибка базы данных
    """
    if connection_string is None:
        connection_string = config.get_connection_string()
    
    return connect(connection_string)


def test_connection(connection_string: Optional[str] = None) -> tuple[bool, str]:
    """
    Проверяет подключение к базе данных.
    
    Args:
        connection_string: Строка подключения. Если не указана, используется из config.
        
    Returns:
        Кортеж (успех, сообщение)
    """
    try:
        conn = create_connection(connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        return True, "Подключение успешно"
    except InterfaceError as e:
        return False, f"Ошибка подключения: {e}"
    except DatabaseError as e:
        return False, f"Ошибка базы данных: {e}"
    except Exception as e:
        return False, f"Неожиданная ошибка: {e}"


def get_connection_string_from_params(host: str, database: str, username: str, password: str) -> str:
    """
    Формирует строку подключения из параметров.
    
    Args:
        host: Хост сервера
        database: Имя базы данных
        username: Имя пользователя
        password: Пароль
        
    Returns:
        Строка подключения
    """
    return (
        f"Server={host};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
    )

















