#!/usr/bin/env python3
"""
Проверяет наличие GUID в основной таблице _Document545 и её табличных частях.
"""

from db.structure_analyzer import StructureAnalyzer

def check_guid_in_tables():
    """Проверяет наличие GUID в таблицах."""
    
    analyzer = StructureAnalyzer()
    analyzer.connect()
    
    test_guid_hex = "0x8A2F00505694192711EEDF729C92E5CA"
    guid_hex_clean = test_guid_hex.replace("0x", "").replace(" ", "")
    guid_bytes = bytes.fromhex(guid_hex_clean)
    
    print(f"Проверка GUID: {test_guid_hex}")
    print("=" * 60)
    
    # Проверяем основную таблицу _Document545
    main_table = "_Document545"
    print(f"\n1. Проверка основной таблицы: {main_table}")
    
    schema, table = analyzer._parse_table_name(main_table)
    
    # Получаем все поля типа binary(16), которые могут быть PK
    columns = analyzer.get_table_columns(main_table)
    pk_candidates = []
    
    # Формальные PK
    formal_pk = analyzer.get_primary_keys(main_table)
    pk_candidates.extend(formal_pk)
    
    # Поля, заканчивающиеся на _IDRRef или IDRRef
    for col in columns:
        col_name = col['name']
        col_type = col['data_type']
        col_max_length = col.get('max_length')
        
        if col_type in ['binary', 'varbinary'] and col_max_length == 16:
            if (col_name == '_IDRRef' or 
                col_name == 'IDRRef' or 
                col_name.endswith('_IDRRef') or 
                col_name.endswith('IDRRef')):
                if col_name not in pk_candidates:
                    pk_candidates.append(col_name)
    
    print(f"   Кандидаты на PK: {pk_candidates}")
    
    cursor = analyzer.conn.cursor()
    
    for pk_col in pk_candidates:
        try:
            query = f"""
                SELECT COUNT(*) 
                FROM [{schema}].[{table}]
                WHERE [{pk_col}] = ?
            """
            cursor.execute(query, (guid_bytes,))
            count = cursor.fetchone()[0]
            if count > 0:
                print(f"   ✓ GUID найден в поле {pk_col}! Количество записей: {count}")
            else:
                print(f"   ✗ GUID не найден в поле {pk_col}")
        except Exception as e:
            print(f"   ✗ Ошибка при проверке поля {pk_col}: {e}")
    
    # Проверяем табличную часть _Document545_VT4114
    vt_table = "_Document545_VT4114"
    print(f"\n2. Проверка табличной части: {vt_table}")
    
    schema_vt, table_vt = analyzer._parse_table_name(vt_table)
    
    columns_vt = analyzer.get_table_columns(vt_table)
    pk_candidates_vt = []
    
    formal_pk_vt = analyzer.get_primary_keys(vt_table)
    pk_candidates_vt.extend(formal_pk_vt)
    
    for col in columns_vt:
        col_name = col['name']
        col_type = col['data_type']
        col_max_length = col.get('max_length')
        
        if col_type in ['binary', 'varbinary'] and col_max_length == 16:
            if (col_name == '_IDRRef' or 
                col_name == 'IDRRef' or 
                col_name.endswith('_IDRRef') or 
                col_name.endswith('IDRRef')):
                if col_name not in pk_candidates_vt:
                    pk_candidates_vt.append(col_name)
    
    print(f"   Кандидаты на PK: {pk_candidates_vt}")
    
    for pk_col in pk_candidates_vt:
        try:
            query = f"""
                SELECT COUNT(*) 
                FROM [{schema_vt}].[{table_vt}]
                WHERE [{pk_col}] = ?
            """
            cursor.execute(query, (guid_bytes,))
            count = cursor.fetchone()[0]
            if count > 0:
                print(f"   ✓ GUID найден в поле {pk_col}! Количество записей: {count}")
            else:
                print(f"   ✗ GUID не найден в поле {pk_col}")
        except Exception as e:
            print(f"   ✗ Ошибка при проверке поля {pk_col}: {e}")
    
    cursor.close()
    analyzer.close()
    
    print("\n" + "=" * 60)
    print("Проверка завершена")

if __name__ == "__main__":
    check_guid_in_tables()














