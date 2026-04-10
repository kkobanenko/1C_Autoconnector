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


def _parse_connection_string_parts(connection_string: str) -> dict:
    """Разбирает простую строку подключения вида key=value;key=value; в словарь."""
    parts = {}
    for item in (connection_string or "").split(";"):
        item = item.strip()
        if not item or "=" not in item:
            continue
        key, value = item.split("=", 1)
        parts[key.strip()] = value.strip()
    return parts


def _replace_database_in_connection_string(connection_string: str, database: str) -> str:
    """
    Возвращает ту же строку подключения, но с другим значением Database.
    Это нужно для мягкой диагностики: если логин проходит в master, а в целевую БД нет,
    значит проблема, скорее всего, в имени БД или правах именно на неё.
    """
    parts = _parse_connection_string_parts(connection_string)
    normalized = {k.lower(): k for k in parts}
    db_key = normalized.get("database", "Database")
    parts[db_key] = database
    return ";".join(f"{k}={v}" for k, v in parts.items()) + ";"


def _build_connection_error_info(exc: Exception) -> dict:
    """
    Нормализует типовые ошибки подключения SQL Server в понятный для UI формат.

    Возвращает словарь с коротким сообщением, подсказкой и технической деталью.
    Логика специально простая и основана на тексте драйвера, чтобы работала
    одинаково для подключения и для повторных обращений к БД.
    """
    technical_message = str(exc).strip() or exc.__class__.__name__
    lower_message = technical_message.lower()
    category = "unknown"
    message = "Не удалось подключиться к базе данных."
    hint = "Проверьте параметры подключения и попробуйте ещё раз."

    # Ошибки авторизации SQL Server часто содержат Login failed / 18456.
    if (
        "18456" in lower_message
        or "login failed" in lower_message
        or "authentication failed" in lower_message
        or "ошибка входа" in lower_message
        or "при входе в систему пользователя" in lower_message
    ):
        category = "auth"
        message = "Не удалось выполнить вход в SQL Server."
        hint = "Проверьте логин и пароль пользователя."
    # SQL Server при неверном имени БД или отсутствии прав на неё обычно пишет Cannot open database.
    elif (
        "cannot open database" in lower_message
        or "unknown database" in lower_message
        or "database does not exist" in lower_message
        or "invalid catalog" in lower_message
    ):
        category = "database"
        message = "Не удалось открыть указанную базу данных."
        hint = "Проверьте имя базы данных и права пользователя на неё."
    # Сюда попадают сетевые проблемы, недоступный сервер, таймауты и ошибки драйвера.
    elif (
        "timeout" in lower_message
        or "timed out" in lower_message
        or "server does not exist" in lower_message
        or "could not open a connection" in lower_message
        or "network-related" in lower_message
        or "adaptive server is unavailable" in lower_message
        or "connection refused" in lower_message
        or "name or service not known" in lower_message
        or "temporary failure in name resolution" in lower_message
        or "nodename nor servname provided" in lower_message
        or "tcp provider" in lower_message
        or "named pipes provider" in lower_message
    ):
        category = "server"
        message = "Не удалось подключиться к серверу SQL Server."
        hint = "Проверьте адрес сервера, доступность сети и что SQL Server запущен."
    elif (
        "data source name not found" in lower_message
        or "can't open lib" in lower_message
        or "cannot open lib" in lower_message
        or "driver manager" in lower_message
        or "driver not capable" in lower_message
    ):
        category = "driver"
        message = "Не удалось инициализировать драйвер подключения к SQL Server."
        hint = "Проверьте установленный драйвер и параметры окружения."

    return {
        "category": category,
        "message": message,
        "hint": hint,
        "technical_message": technical_message,
        "user_message": (
            f"{message} {hint}\n"
            f"Техническая деталь: {technical_message}"
        ),
    }


def get_connection_error_info(exc: Exception) -> dict:
    """Публичная обёртка над нормализацией ошибок подключения."""
    return _build_connection_error_info(exc)


def test_connection_details(connection_string: Optional[str] = None) -> tuple[bool, dict]:
    """
    Проверяет подключение к БД и возвращает структурированный результат для UI.
    """
    conn = None
    cursor = None
    try:
        conn = create_connection(connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        return True, {
            "category": "ok",
            "message": "Подключение успешно",
            "hint": "",
            "technical_message": "",
            "user_message": "Подключение успешно",
        }
    except (InterfaceError, DatabaseError, Exception) as exc:
        info = _build_connection_error_info(exc)
        # Если ошибка выглядит как auth, пробуем войти в master с теми же логином/паролем.
        # Это позволяет отличить "неверный пароль" от "логин есть, но целевая БД не открывается".
        if info["category"] == "auth" and connection_string:
            try:
                master_conn = _replace_database_in_connection_string(connection_string, "master")
                probe = create_connection(master_conn)
                probe_cursor = probe.cursor()
                probe_cursor.execute("SELECT 1")
                probe_cursor.fetchone()
                probe_cursor.close()
                probe.close()
                info = {
                    "category": "database",
                    "message": "Не удалось открыть указанную базу данных.",
                    "hint": "Проверьте имя базы данных и права пользователя на неё.",
                    "technical_message": info["technical_message"],
                    "user_message": (
                        "Не удалось открыть указанную базу данных. "
                        "Проверьте имя базы данных и права пользователя на неё.\n"
                        f"Техническая деталь: {info['technical_message']}"
                    ),
                }
            except Exception:
                pass
        return False, info
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def test_connection(connection_string: Optional[str] = None) -> tuple[bool, str]:
    """
    Проверяет подключение к базе данных.
    
    Args:
        connection_string: Строка подключения. Если не указана, используется из config.
        
    Returns:
        Кортеж (успех, сообщение)
    """
    success, info = test_connection_details(connection_string)
    return success, info["user_message"]


def get_db_signature(host: str, database: str) -> str:
    """
    Стабильная подпись пары «сервер + база» для привязки кэшей и UI к конкретной БД.
    Логин/пароль сюда не входят: индексы на диске в StructureAnalyzer именуются по host+database.
    """
    h = (host or "").strip()
    d = (database or "").strip()
    return f"{h}|{d}"


def get_db_signature_from_connection_string(connection_string: Optional[str] = None) -> str:
    """Извлекает Server и Database из строки подключения и возвращает get_db_signature(...)."""
    if not connection_string:
        return "|"
    host, database = "unknown", "unknown"
    for part in connection_string.split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        key, val = part.split("=", 1)
        kl = key.strip().lower()
        if kl == "server":
            host = val.strip()
        elif kl == "database":
            database = val.strip()
    return get_db_signature(host, database)


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

















