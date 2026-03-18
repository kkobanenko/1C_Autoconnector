#!/usr/bin/env python3
"""
Тестовый скрипт для проверки эвристики поиска таблицы по GUID.
Проверяет, что для поля _Fld10042_RRRef с значением 0x8A2F00505694192711EEDF729C92E5CA
находится таблица _Document545.
"""

from db.structure_analyzer import StructureAnalyzer
from builders.relationship_builder import RelationshipBuilder

def test_guid_heuristic():
    """Тестирует эвристику поиска таблицы по GUID."""
    
    # Подключаемся к БД
    analyzer = StructureAnalyzer()
    analyzer.connect()
    
    # Тестовые данные
    test_table = "_Document653"
    test_field = "_Fld10042_RRRef"
    test_guid_hex = "0x8A2F00505694192711EEDF729C92E5CA"
    expected_table = "_Document545"
    
    print(f"Тестирование эвристики поиска таблицы по GUID")
    print(f"Таблица: {test_table}")
    print(f"Поле: {test_field}")
    print(f"Ожидаемое значение GUID: {test_guid_hex}")
    print(f"Ожидаемая таблица: {expected_table}")
    print("-" * 60)
    
    # Преобразуем GUID из hex в bytes
    guid_hex_clean = test_guid_hex.replace("0x", "").replace(" ", "")
    guid_bytes = bytes.fromhex(guid_hex_clean)
    print(f"GUID в bytes: {guid_bytes.hex()}")
    print("-" * 60)
    
    # Строим индекс GUID
    print("\n1. Построение индекса GUID...")
    analyzer.clear_guid_index_cache()
    guid_index = analyzer.build_guid_index()
    print(f"   Индекс содержит {len(guid_index)} записей")
    
    # Проверяем, есть ли наш GUID в индексе
    if guid_bytes in guid_index:
        found_table = guid_index[guid_bytes]
        print(f"   ✓ GUID найден в индексе! Таблица: {found_table}")
        if found_table == expected_table or found_table.endswith(expected_table.lstrip('_')):
            print(f"   ✓ Таблица совпадает с ожидаемой!")
        else:
            print(f"   ✗ Таблица не совпадает! Ожидалось: {expected_table}, найдено: {found_table}")
    else:
        print(f"   ✗ GUID не найден в индексе!")
        print(f"   Проверяем, есть ли таблица {expected_table} в индексе...")
        # Ищем таблицу в индексе
        tables_in_index = set(guid_index.values())
        if expected_table in tables_in_index or any(t.endswith(expected_table.lstrip('_')) for t in tables_in_index):
            print(f"   Таблица {expected_table} присутствует в индексе, но с другим GUID")
        else:
            print(f"   Таблица {expected_table} отсутствует в индексе")
    
    # Проверяем, что поле существует в таблице
    print(f"\n2. Проверка поля {test_field} в таблице {test_table}...")
    binary16_fields = analyzer.get_binary16_fields(test_table)
    if test_field in binary16_fields:
        print(f"   ✓ Поле {test_field} найдено в таблице")
    else:
        print(f"   ✗ Поле {test_field} не найдено в таблице")
        print(f"   Найденные поля binary(16): {binary16_fields[:10]}...")
    
    # Получаем первое значение из поля
    print(f"\n3. Получение первого значения из поля {test_field}...")
    schema, table = analyzer._parse_table_name(test_table)
    cursor = analyzer.conn.cursor()
    
    query = f"""
        SELECT TOP 1 [{test_field}]
        FROM [{schema}].[{table}]
        WHERE [{test_field}] IS NOT NULL
        AND [{test_field}] != 0x00000000000000000000000000000000
    """
    
    cursor.execute(query)
    row = cursor.fetchone()
    cursor.close()
    
    if row and row[0]:
        field_guid = row[0]
        if isinstance(field_guid, bytearray):
            field_guid_bytes = bytes(field_guid)
        elif isinstance(field_guid, bytes):
            field_guid_bytes = field_guid
        else:
            field_guid_bytes = bytes(field_guid)
        
        print(f"   Найдено значение: 0x{field_guid_bytes.hex().upper()}")
        
        if field_guid_bytes == guid_bytes:
            print(f"   ✓ Значение совпадает с тестовым!")
        else:
            print(f"   ✗ Значение не совпадает с тестовым")
        
        # Ищем таблицу по этому GUID
        found_table = analyzer.find_table_by_guid(field_guid_bytes, guid_index)
        if found_table:
            print(f"   ✓ Таблица найдена по GUID: {found_table}")
            if found_table == expected_table or found_table.endswith(expected_table.lstrip('_')):
                print(f"   ✓ Таблица совпадает с ожидаемой!")
            else:
                print(f"   ✗ Таблица не совпадает! Ожидалось: {expected_table}, найдено: {found_table}")
        else:
            print(f"   ✗ Таблица не найдена по GUID")
    else:
        print(f"   ✗ Не найдено ненулевых значений в поле")
    
    # Тестируем RelationshipBuilder
    print(f"\n4. Тестирование RelationshipBuilder...")
    relationship_builder = RelationshipBuilder(analyzer)
    relationship_builder.build_relationship_graph([test_table])
    
    relationships = relationship_builder.get_related_tables(test_table)
    print(f"   Найдено связей: {len(relationships)}")
    
    # Ищем наше поле в связях
    if test_field in relationships:
        target_table = relationships[test_field]
        print(f"   ✓ Поле {test_field} найдено в связях! Целевая таблица: {target_table}")
        if target_table == expected_table or target_table.endswith(expected_table.lstrip('_')):
            print(f"   ✓ Целевая таблица совпадает с ожидаемой!")
        else:
            print(f"   ✗ Целевая таблица не совпадает! Ожидалось: {expected_table}, найдено: {target_table}")
    else:
        # Пробуем варианты имени поля
        field_variants = [
            test_field,
            test_field.lstrip('_'),
            '_' + test_field.lstrip('_'),
            test_field.replace('RRRef', 'RRef'),
            test_field.replace('_RRRef', '_RRef'),
        ]
        found = False
        for variant in field_variants:
            if variant in relationships:
                target_table = relationships[variant]
                print(f"   ✓ Поле найдено как '{variant}' в связях! Целевая таблица: {target_table}")
                if target_table == expected_table or target_table.endswith(expected_table.lstrip('_')):
                    print(f"   ✓ Целевая таблица совпадает с ожидаемой!")
                else:
                    print(f"   ✗ Целевая таблица не совпадает! Ожидалось: {expected_table}, найдено: {target_table}")
                found = True
                break
        
        if not found:
            print(f"   ✗ Поле {test_field} не найдено в связях")
            print(f"   Доступные поля в связях: {list(relationships.keys())[:10]}...")
    
    analyzer.close()
    print("\n" + "=" * 60)
    print("Тестирование завершено")

if __name__ == "__main__":
    test_guid_heuristic()














