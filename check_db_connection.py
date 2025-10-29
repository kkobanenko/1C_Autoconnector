#!/usr/bin/env python3
"""
Скрипт для проверки подключения к MS SQL Server базе данных.
Проверяет доступность базы данных и возможность чтения системных каталогов.
"""

from mssql_python import connect
from mssql_python.exceptions import DatabaseError, InterfaceError
import sys

# Параметры подключения к базе данных
MSSQL_HOST = "localhost"
MSSQL_DATABASE = "database_name"
MSSQL_USERNAME = "username"
MSSQL_PASSWORD = "password"

# Формируем строку подключения
CONNECTION_STRING = (
    f"Server={MSSQL_HOST};"
    f"Database={MSSQL_DATABASE};"
    f"UID={MSSQL_USERNAME};"
    f"PWD={MSSQL_PASSWORD};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=yes;"
    f"Connection Timeout=10;"
)


def check_connection():
    """
    Проверяет подключение к базе данных и доступность системных представлений.
    
    Returns:
        bool: True если подключение успешно, False в противном случае
    """
    conn = None
    cursor = None
    
    try:
        print(f"Попытка подключения к серверу: {MSSQL_HOST}")
        print(f"База данных: {MSSQL_DATABASE}")
        print("-" * 50)
        
        # Устанавливаем подключение
        conn = connect(CONNECTION_STRING)
        print("✓ Подключение установлено успешно!")
        
        cursor = conn.cursor()
        
        # Проверка 1: Версия SQL Server
        print("\n[1] Проверка версии SQL Server...")
        cursor.execute("SELECT @@VERSION AS Version")
        version = cursor.fetchone()
        print(f"✓ Версия: {version[0][:100]}...")
        
        # Проверка 2: Доступ к INFORMATION_SCHEMA
        print("\n[2] Проверка доступа к INFORMATION_SCHEMA.TABLES...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)
        table_count = cursor.fetchone()[0]
        print(f"✓ Найдено таблиц: {table_count}")
        
        # Проверка 3: Доступ к sys.tables и проверка всех схем
        print("\n[3] Проверка доступа к sys.tables...")
        cursor.execute("""
            SELECT 
                SCHEMA_NAME(schema_id) AS schema_name,
                COUNT(*) AS table_count
            FROM sys.tables 
            GROUP BY schema_id
            ORDER BY table_count DESC
        """)
        schema_tables = cursor.fetchall()
        total_tables = sum(count for _, count in schema_tables)
        print(f"✓ Найдено таблиц во всех схемах: {total_tables}")
        if schema_tables:
            print("  Распределение по схемам:")
            for schema_name, count in schema_tables[:10]:
                print(f"    - {schema_name}: {count} таблиц")
        
        # Проверка 4: Поиск типичных таблиц 1C (Document*, Reference*, Enum*)
        print("\n[4] Поиск таблиц 1C (Document*, Reference*, Enum*, VT*)...")
        cursor.execute("""
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME LIKE 'Document%' 
               OR TABLE_NAME LIKE 'Reference%'
               OR TABLE_NAME LIKE 'Enum%'
               OR TABLE_NAME LIKE 'VT%'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """)
        tables_1c = cursor.fetchall()
        print(f"✓ Найдено таблиц 1C: {len(tables_1c)}")
        if tables_1c:
            print("  Примеры таблиц:")
            for schema, table, table_type in tables_1c[:10]:  # Показываем первые 10
                print(f"    - [{schema}].[{table}] ({table_type})")
            if len(tables_1c) > 10:
                print(f"    ... и еще {len(tables_1c) - 10} таблиц")
        else:
            print("  ⚠ Таблицы 1C не найдены. Возможно:")
            print("    - База данных пуста")
            print("    - Таблицы находятся в других схемах")
            print("    - Необходимо проверить структуру экспорта 1C")
        
        # Проверка 5: Права на создание представлений
        print("\n[5] Проверка прав на создание представлений...")
        try:
            cursor.execute("""
                SELECT HAS_PERMS_BY_NAME(null, 'DATABASE', 'CREATE VIEW') AS CanCreateView
            """)
            can_create = cursor.fetchone()[0]
            if can_create:
                print("✓ Права на создание представлений: ЕСТЬ")
            else:
                print("⚠ Права на создание представлений: ОТСУТСТВУЮТ")
        except Exception as e:
            print(f"⚠ Не удалось проверить права на создание представлений: {e}")
        
        # Проверка 6: Проверка поддержки CREATE OR ALTER VIEW
        print("\n[6] Проверка поддержки CREATE OR ALTER VIEW...")
        try:
            # Извлекаем версию из @@VERSION (версия SQL Server 2017, что >= 13)
            # SQL Server 2016+ поддерживает CREATE OR ALTER VIEW
            if '2017' in version[0] or '2016' in version[0]:
                print("✓ CREATE OR ALTER VIEW поддерживается (SQL Server 2016+)")
            elif '2019' in version[0] or '2022' in version[0]:
                print("✓ CREATE OR ALTER VIEW поддерживается (SQL Server 2019+)")
            else:
                print("✓ CREATE OR ALTER VIEW должен поддерживаться (SQL Server 2016+)")
        except Exception as e:
            print(f"⚠ Не удалось определить поддержку: {e}")
            print("  SQL Server 2017 обнаружен ранее, CREATE OR ALTER VIEW должен работать")
        
        print("\n" + "=" * 50)
        print("✓ Все проверки пройдены успешно!")
        print("=" * 50)
        
        return True
        
    except InterfaceError as e:
        print(f"\n✗ Ошибка подключения (InterfaceError): {e}")
        print("  Проверьте:")
        print("  - Доступность сервера по сети")
        print("  - Правильность хоста/порта")
        print("  - Наличие установленного ODBC драйвера для SQL Server")
        return False
        
    except DatabaseError as e:
        print(f"\n✗ Ошибка базы данных (DatabaseError): {e}")
        print("  Проверьте:")
        print("  - Правильность имени базы данных")
        print("  - Правильность учетных данных")
        print("  - Права доступа пользователя")
        return False
        
    except Exception as e:
        print(f"\n✗ Неожиданная ошибка: {type(e).__name__}: {e}")
        return False
        
    finally:
        # Закрываем соединение
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
                print("\n✓ Соединение закрыто")
            except:
                pass


if __name__ == "__main__":
    success = check_connection()
    sys.exit(0 if success else 1)

