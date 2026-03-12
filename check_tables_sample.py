#!/usr/bin/env python3
"""
Скрипт для проверки структуры таблиц в базе данных.
"""

from mssql_python import connect

# Параметры подключения
CONNECTION_STRING = (
    "Server=localhost;"
    "Database=database_name;"
    "UID=username;"
    "PWD=password;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

conn = connect(CONNECTION_STRING)
cursor = conn.cursor()

print("Примеры таблиц в базе данных:\n")

# Получаем первые 20 таблиц
cursor.execute("""
    SELECT TOP 20
        TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
""")
tables = cursor.fetchall()
print("Первые 20 таблиц:")
for table in tables:
    print(f"  - {table[0]}")

print("\n" + "-" * 60)

# Получаем таблицы с необычными именами
cursor.execute("""
    SELECT TOP 20
        TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
      AND (TABLE_NAME LIKE '%[0-9]%' OR TABLE_NAME LIKE '[_]%')
    ORDER BY TABLE_NAME
""")
tables = cursor.fetchall()
print("\nТаблицы с цифрами или начинающиеся с подчеркивания:")
for table in tables:
    print(f"  - {table[0]}")

print("\n" + "-" * 60)

# Проверяем структуру одной из таблиц
cursor.execute("""
    SELECT TOP 1 TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
""")
sample_table = cursor.fetchone()
if sample_table:
    table_name = sample_table[0]
    print(f"\nСтруктура таблицы '{table_name}':")
    cursor.execute(f"""
        SELECT TOP 10
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """)
    columns = cursor.fetchall()
    for col in columns:
        print(f"  - {col[0]} ({col[1]}, NULL: {col[2]})")

cursor.close()
conn.close()

