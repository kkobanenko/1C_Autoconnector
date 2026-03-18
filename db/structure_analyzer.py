#!/usr/bin/env python3
"""
Анализатор структуры базы данных MS SQL Server.
Получает метаданные таблиц, полей, первичных и внешних ключей.
"""

from typing import Callable, Dict, List, Optional, Set, Tuple
import json
import base64
from pathlib import Path
from utils.db_connection import create_connection


class StructureAnalyzer:
    """
    Класс для анализа структуры базы данных.
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Инициализация анализатора.
        
        Args:
            connection_string: Строка подключения к БД. Если не указана, используется из config.
        """
        self.connection_string = connection_string
        self.conn = None
        
        # Кэш метаданных
        self._tables_cache: Optional[Set[str]] = None
        self._columns_cache: Dict[str, List[Dict]] = {}
        self._primary_keys_cache: Dict[str, List[str]] = {}
        self._foreign_keys_cache: Dict[str, List[Dict]] = {}
        self._guid_to_table_cache: Optional[Dict[bytes, str]] = None  # Кэш GUID -> таблица
        self._relationship_index: Optional[Dict[str, Dict[str, List[str]]]] = None  # Индекс связей: {table → {field → [target_table, ...]}}
        self._unresolved_fields: Optional[Dict[str, List[str]]] = None  # Висячие ключи: {table → [field, ...]}
        self._field_stats_cache: Optional[Dict[str, Dict[str, dict]]] = None  # Кэш статистики полей
    
    def connect(self):
        """Подключается к базе данных."""
        if self.conn is None:
            self.conn = create_connection(self.connection_string)
    
    def close(self):
        """Закрывает подключение к базе данных."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def __enter__(self):
        """Контекстный менеджер: вход."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер: выход."""
        self.close()
    
    def get_all_tables(self) -> Set[str]:
        """
        Получает список всех таблиц в базе данных.
        
        Returns:
            Множество имен таблиц
        """
        if self._tables_cache is not None:
            return self._tables_cache
        
        self.connect()
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        """)
        
        tables = set()
        for schema, table_name in cursor.fetchall():
            # Формируем полное имя таблицы: [schema].[table]
            full_name = f"[{schema}].[{table_name}]"
            tables.add(full_name)
            # Также добавляем без схемы для удобства
            tables.add(table_name)
        
        cursor.close()
        self._tables_cache = tables
        return tables
    
    def table_exists(self, table_name: str) -> bool:
        """
        Проверяет существование таблицы в БД.
        
        Args:
            table_name: Имя таблицы (может быть с префиксом подчеркивания или без)
            
        Returns:
            True если таблица существует, False иначе
        """
        tables = self.get_all_tables()
        
        # Нормализуем имя таблицы
        normalized = self._normalize_table_name(table_name)
        
        # Проверяем различные варианты имени
        variants = [
            normalized,
            normalized.lstrip('_'),
            f"dbo.{normalized}",
            f"[dbo].[{normalized}]",
            f"dbo.{normalized.lstrip('_')}",
            f"[dbo].[{normalized.lstrip('_')}]"
        ]
        
        return any(variant in tables for variant in variants)
    
    def get_table_columns(self, table_name: str) -> List[Dict]:
        """
        Получает список колонок таблицы с их типами данных.
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Список словарей с информацией о колонках:
            [
                {
                    'name': 'ID',
                    'data_type': 'binary',
                    'max_length': 16,
                    'is_nullable': 'NO',
                    ...
                },
                ...
            ]
        """
        # Проверяем кэш
        normalized = self._normalize_table_name(table_name)
        if normalized in self._columns_cache:
            return self._columns_cache[normalized]
        
        self.connect()
        cursor = self.conn.cursor()
        
        # Нормализуем имя таблицы для запроса
        schema, table = self._parse_table_name(normalized)
        
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, (schema, table))
        
        columns = []
        for row in cursor.fetchall():
            col_name, data_type, max_length, precision, scale, is_nullable, ordinal = row
            
            # Определяем полный тип данных
            full_type = data_type
            if data_type in ['varchar', 'nvarchar', 'char', 'nchar', 'binary', 'varbinary']:
                if max_length == -1:
                    full_type = f"{data_type}(MAX)"
                elif max_length:
                    full_type = f"{data_type}({max_length})"
            elif data_type in ['decimal', 'numeric']:
                if precision and scale:
                    full_type = f"{data_type}({precision},{scale})"
            elif data_type in ['datetime2', 'datetimeoffset', 'time']:
                if scale is not None:
                    full_type = f"{data_type}({scale})"
            
            columns.append({
                'name': col_name,
                'data_type': data_type,
                'full_type': full_type,
                'max_length': max_length,
                'precision': precision,
                'scale': scale,
                'is_nullable': is_nullable == 'YES',
                'ordinal_position': ordinal
            })
        
        cursor.close()
        
        # Сохраняем в кэш
        self._columns_cache[normalized] = columns
        return columns
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """
        Получает список первичных ключей таблицы.
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Список имен колонок, являющихся первичными ключами
        """
        normalized = self._normalize_table_name(table_name)
        if normalized in self._primary_keys_cache:
            return self._primary_keys_cache[normalized]
        
        self.connect()
        cursor = self.conn.cursor()
        
        schema, table = self._parse_table_name(normalized)
        
        cursor.execute("""
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA = ?
                AND tc.TABLE_NAME = ?
            ORDER BY kcu.ORDINAL_POSITION
        """, (schema, table))
        
        pk_columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        self._primary_keys_cache[normalized] = pk_columns
        return pk_columns
    
    def has_at_least_two_distinct_values(self, table_name: str, field_name: str) -> bool:
        """
        Проверяет, есть ли в поле таблицы хотя бы два различных значения (ленивый способ).
        Использует TOP 2 для оптимизации - не считает все уникальные значения.
        
        Args:
            table_name: Имя таблицы
            field_name: Имя поля
            
        Returns:
            True если найдено >= 2 уникальных значений, False иначе (включая ошибки)
        """
        import sys
        
        try:
            # Нормализуем имя таблицы
            normalized = self._normalize_table_name(table_name)
            
            # Парсим схему и таблицу
            schema, table = self._parse_table_name(normalized)
            
            # Логируем нормализованные имена для диагностики
            print(f"[DEBUG] Проверка уникальных значений: исходная таблица='{table_name}' -> нормализованная='{normalized}' -> схема='{schema}', таблица='{table}', поле='{field_name}'", file=sys.stderr)
            
            # Подключаемся к БД если нужно
            self.connect()
            cursor = self.conn.cursor()
            
            # Выполняем оптимизированный запрос: получаем только первые 2 уникальных значения
            # В SQL Server правильный синтаксис: SELECT DISTINCT TOP N, а не SELECT TOP N DISTINCT
            query = f"""
                SELECT DISTINCT TOP 2 [{field_name}]
                FROM [{schema}].[{table}]
                WHERE [{field_name}] IS NOT NULL
            """
            
            # Логируем SQL запрос перед выполнением
            print(f"[DEBUG] SQL запрос: {query.strip()}", file=sys.stderr)
            
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            
            # Логируем количество найденных строк
            row_count = len(rows)
            print(f"[DEBUG] Найдено строк: {row_count}", file=sys.stderr)
            
            # Если получили 2 или более строк, значит есть хотя бы 2 уникальных значения
            result = row_count >= 2
            print(f"[DEBUG] Результат проверки: {result} (>= 2 уникальных значений)", file=sys.stderr)
            
            return result
            
        except Exception as e:
            # Логируем ошибку для диагностики (можно убрать в продакшене)
            error_msg = f"[ERROR] Ошибка при проверке уникальных значений для {table_name}.{field_name}: {type(e).__name__}: {str(e)}"
            print(error_msg, file=sys.stderr)
            # При любой ошибке возвращаем False (консервативный подход)
            return False
    
    def get_foreign_keys(self, table_name: str) -> List[Dict]:
        """
        Получает список внешних ключей таблицы.
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Список словарей с информацией о внешних ключах:
            [
                {
                    'fk_name': 'FK_...',
                    'column_name': 'ParentID',
                    'referenced_table': 'Reference123',
                    'referenced_column': 'ID'
                },
                ...
            ]
        """
        normalized = self._normalize_table_name(table_name)
        if normalized in self._foreign_keys_cache:
            return self._foreign_keys_cache[normalized]
        
        self.connect()
        cursor = self.conn.cursor()
        
        schema, table = self._parse_table_name(normalized)
        
        # Используем sys.foreign_keys для получения информации о внешних ключах
        cursor.execute("""
            SELECT 
                fk.name AS FK_NAME,
                c.name AS COLUMN_NAME,
                OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS REFERENCED_SCHEMA,
                OBJECT_NAME(fk.referenced_object_id) AS REFERENCED_TABLE,
                rc.name AS REFERENCED_COLUMN
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc
                ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.columns c
                ON fkc.parent_object_id = c.object_id
                AND fkc.parent_column_id = c.column_id
            INNER JOIN sys.columns rc
                ON fkc.referenced_object_id = rc.object_id
                AND fkc.referenced_column_id = rc.column_id
            WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = ?
                AND OBJECT_NAME(fk.parent_object_id) = ?
            ORDER BY fkc.constraint_column_id
        """, (schema, table))
        
        foreign_keys = []
        for row in cursor.fetchall():
            fk_name, col_name, ref_schema, ref_table, ref_column = row
            foreign_keys.append({
                'fk_name': fk_name,
                'column_name': col_name,
                'referenced_schema': ref_schema,
                'referenced_table': ref_table,
                'referenced_column': ref_column
            })
        
        cursor.close()
        
        self._foreign_keys_cache[normalized] = foreign_keys
        return foreign_keys
    
    def get_binary16_fields(self, table_name: str) -> List[str]:
        """
        Получает список полей типа binary(16) или varbinary(16) в таблице.
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Список имен полей типа binary(16) или varbinary(16)
        """
        columns = self.get_table_columns(table_name)
        binary16_fields = []
        
        for col in columns:
            # В 1С часто используется varbinary вместо binary
            if col['data_type'] in ['binary', 'varbinary'] and col['max_length'] == 16:
                binary16_fields.append(col['name'])
        
        return binary16_fields
    
    def get_datetime2_fields(self, table_name: str) -> List[str]:
        """
        Получает список полей типа datetime2(0) в таблице.
        
        В SQL Server для datetime2(0) значение NUMERIC_SCALE может быть None или 0.
        Проверяем как через scale, так и через full_type.
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Список имен полей типа datetime2(0)
        """
        columns = self.get_table_columns(table_name)
        datetime2_fields = []
        
        for col in columns:
            if col['data_type'] == 'datetime2':
                scale = col.get('scale')
                full_type = col.get('full_type', '')
                
                # Проверяем: scale равен 0 или None (для datetime2(0) в SQL Server scale может быть None)
                # Также проверяем full_type на наличие datetime2(0) или просто datetime2 (если scale is None)
                if (scale == 0 or 
                    scale is None or 
                    full_type == 'datetime2(0)' or 
                    (full_type == 'datetime2' and scale is None)):
                    datetime2_fields.append(col['name'])
        
        return datetime2_fields
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Получает приблизительное количество строк в таблице.
        Использует sys.dm_db_partition_stats для быстрого подсчёта без полного сканирования.
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Приблизительное количество строк
        """
        try:
            self.connect()
            cursor = self.conn.cursor()
            normalized = self._normalize_table_name(table_name)
            schema, table = self._parse_table_name(normalized)
            
            # Быстрый приблизительный подсчёт через системные DMV
            cursor.execute("""
                SELECT SUM(p.rows)
                FROM sys.partitions p
                INNER JOIN sys.tables t ON p.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = ? AND t.name = ?
                AND p.index_id IN (0, 1)
            """, (schema, table))
            
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0] is not None:
                return int(result[0])
            return 0
        except Exception:
            return 0
    
    def get_vt_tables(self, table_name: str) -> List[str]:
        """
        Находит табличные части (VT-таблицы) для данной таблицы.
        В 1С табличные части именуются по шаблону: _TableName_VTnumber
        Например: _Document653 → _Document653_VT12345
        
        Args:
            table_name: Имя таблицы (например, _Document653)
            
        Returns:
            Список имён VT-таблиц
        """
        normalized = self._normalize_table_name(table_name)
        # Убираем квадратные скобки
        clean_name = normalized.strip('[]')
        
        all_tables = self.get_all_tables()
        vt_tables = []
        
        # Ищем таблицы, начинающиеся с "tableName_VT"
        prefix = f"{clean_name}_VT"
        for t in all_tables:
            t_clean = t.strip('[]')
            # Пропускаем полные имена со схемой
            if '.' in t_clean:
                continue
            if t_clean.startswith(prefix):
                vt_tables.append(t_clean)
        
        return sorted(vt_tables)
    
    def _normalize_table_name(self, table_name: str) -> str:
        """
        Нормализует имя таблицы (добавляет подчеркивание если нужно).
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Нормализованное имя таблицы
        """
        # Если имя содержит точку (табличная часть), обрабатываем отдельно
        # Табличные части соединяются подчеркиванием: Document653.VT10121 -> _Document653_VT10121
        if '.' in table_name:
            parts = table_name.split('.')
            normalized_parts = []
            for i, part in enumerate(parts):
                part = part.strip('[]')
                # Для первой части добавляем подчеркивание если нужно
                if i == 0:
                    if not part.startswith('_'):
                        normalized_parts.append('_' + part)
                    else:
                        normalized_parts.append(part)
                else:
                    # Для остальных частей добавляем подчеркивание если нужно, но убираем его из начала
                    # так как join уже добавит подчеркивание между частями
                    part_clean = part.lstrip('_')
                    if part_clean:
                        normalized_parts.append('_' + part_clean)
                    else:
                        normalized_parts.append(part)
            # Соединяем части одним подчеркиванием вместо точки
            result = '_'.join(normalized_parts)
            # Убираем возможные двойные подчеркивания
            while '__' in result:
                result = result.replace('__', '_')
            return result
        
        # Убираем квадратные скобки если есть
        table_name = table_name.strip('[]')
        
        # Добавляем подчеркивание если нужно
        if not table_name.startswith('_'):
            return '_' + table_name
        return table_name
    
    def _parse_table_name(self, table_name: str) -> Tuple[str, str]:
        """
        Парсит имя таблицы на схему и имя таблицы.
        
        Args:
            table_name: Имя таблицы (может быть с точкой или без)
            
        Returns:
            Кортеж (schema, table_name) - сохраняет подчеркивание в имени таблицы
        """
        # Убираем квадратные скобки
        table_name = table_name.strip('[]')
        
        # Если есть точка, разделяем на схему и таблицу
        if '.' in table_name:
            parts = table_name.split('.', 1)
            schema = parts[0].strip('[]')
            table = parts[1].strip('[]')
            # Сохраняем подчеркивание в имени таблицы
            return schema, table
        
        # По умолчанию схема dbo, сохраняем подчеркивание в имени таблицы
        return 'dbo', table_name
    
    def build_guid_index(self, limit_per_table: int = 50000, force_rebuild: bool = False, progress_callback=None) -> Dict[bytes, str]:
        """
        Строит индекс GUID -> таблица для быстрого поиска целевых таблиц.
        Если индекс уже есть в памяти или на диске — использует его.
        
        Args:
            limit_per_table: Максимальное количество GUID для выборки из каждого поля
            force_rebuild: Если True — перестраивает индекс заново, игнорируя кэш и файл
            progress_callback: Функция обратного вызова progress_callback(current, total, table_name)
            
        Returns:
            Словарь {guid_bytes: table_name}
        """
        # 1. Если не force_rebuild и есть в памяти — возвращаем
        if not force_rebuild and self._guid_to_table_cache is not None:
            return self._guid_to_table_cache
        
        # 2. Если не force_rebuild — пробуем загрузить с диска
        if not force_rebuild:
            loaded = self.load_guid_index()
            if loaded is not None:
                self._guid_to_table_cache = loaded
                print(f"GUID-индекс загружен с диска: {len(loaded)} записей")
                return loaded
        
        self.connect()
        cursor = self.conn.cursor()
        
        guid_index: Dict[bytes, str] = {}
        all_tables = self.get_all_tables()
        
        # Фильтруем только таблицы 1С
        # Убираем полные имена со схемой и квадратными скобками
        tables_1c = []
        for t in all_tables:
            # Пропускаем полные имена со схемой в квадратных скобках
            if t.startswith('[') and '.' in t:
                continue
            # Берем только простые имена таблиц
            table_simple = t.strip('[]')
            if '.' in table_simple:
                table_simple = table_simple.split('.')[-1]
            
            if (table_simple.startswith('_') or 
                table_simple.startswith('Document') or 
                table_simple.startswith('Reference') or 
                table_simple.startswith('Enum')):
                tables_1c.append(table_simple)
        
        print(f"Построение индекса GUID для {len(tables_1c)} таблиц...")
        
        # Разделяем таблицы на основные и табличные части
        # Табличные части обычно содержат _VT или VT в названии
        main_tables = []
        tabular_parts = []
        
        for table_name in tables_1c:
            # Нормализуем имя для проверки
            normalized_check = self._normalize_table_name(table_name)
            # Проверяем, является ли таблица табличной частью
            # Табличные части обычно имеют формат _TableName_VTNumber или содержат VT в названии
            if '_VT' in normalized_check:
                tabular_parts.append(table_name)
            else:
                # Дополнительная проверка: если имя содержит VT как отдельную часть
                parts = normalized_check.split('_')
                if 'VT' in parts:
                    tabular_parts.append(table_name)
                else:
                    main_tables.append(table_name)
        
        # Сначала обрабатываем основные таблицы, потом табличные части
        # Это гарантирует, что при конфликте GUID приоритет будет у основной таблицы
        tables_ordered = main_tables + tabular_parts
        
        for idx_i, table_name in enumerate(tables_ordered):
            if progress_callback:
                progress_callback(idx_i, len(tables_ordered), table_name)
            try:
                normalized = self._normalize_table_name(table_name)
                schema, table = self._parse_table_name(normalized)
                
                # Получаем все колонки таблицы
                columns = self.get_table_columns(normalized)
                
                # Находим поля, которые могут использоваться как первичные ключи:
                # 1. Формальные первичные ключи
                # 2. Поля типа binary(16)/varbinary(16), которые равны или заканчиваются на "_IDRRef" или "IDRRef"
                pk_candidate_columns = []
                
                # Сначала получаем формальные первичные ключи
                formal_pk_columns = self.get_primary_keys(normalized)
                pk_candidate_columns.extend(formal_pk_columns)
                
                # Затем ищем поля binary(16) с подстрокой "ID" в имени
                # (_IDRRef, _ParentIDRRef, _OwnerIDRRef, _PredefinedID и т.п.)
                for col in columns:
                    col_name = col['name']
                    col_type = col['data_type']
                    col_max_length = col.get('max_length')
                    
                    if col_type in ['binary', 'varbinary'] and col_max_length == 16:
                        if 'ID' in col_name.upper() and col_name not in pk_candidate_columns:
                            pk_candidate_columns.append(col_name)
                
                # Если не нашли подходящих полей, пробуем стандартные имена для 1С
                if not pk_candidate_columns:
                    pk_candidate_columns = ['ID', '_IDRRef', 'IDRRef', '_ID']
                
                # Для каждого найденного поля получаем значения GUID
                for pk_column in pk_candidate_columns:
                    try:
                        # Получаем первые N ненулевых значений GUID из этого поля
                        query = f"""
                            SELECT TOP ({limit_per_table}) [{pk_column}]
                            FROM [{schema}].[{table}]
                            WHERE [{pk_column}] IS NOT NULL
                            AND [{pk_column}] != 0x00000000000000000000000000000000
                        """
                        
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        
                        for row in rows:
                            guid_value = row[0]
                            if guid_value:
                                # Преобразуем в bytes (может быть bytearray или bytes)
                                if isinstance(guid_value, bytearray):
                                    guid_bytes = bytes(guid_value)
                                elif isinstance(guid_value, bytes):
                                    guid_bytes = guid_value
                                else:
                                    # Пробуем преобразовать через memoryview
                                    try:
                                        guid_bytes = bytes(guid_value)
                                    except:
                                        continue
                                
                                if len(guid_bytes) == 16:  # binary(16)
                                    # Логика приоритета: основная таблица важнее табличной части
                                    if guid_bytes not in guid_index:
                                        # GUID еще не в индексе - добавляем
                                        guid_index[guid_bytes] = normalized
                                    else:
                                        # GUID уже есть в индексе - проверяем приоритет
                                        existing_table = guid_index[guid_bytes]
                                        # Определяем, является ли текущая таблица табличной частью
                                        is_current_tabular = '_VT' in normalized or 'VT' in normalized.split('_')
                                        # Определяем, является ли существующая таблица табличной частью
                                        is_existing_tabular = '_VT' in existing_table or 'VT' in existing_table.split('_')
                                        
                                        # Если текущая таблица - основная, а существующая - табличная часть,
                                        # заменяем запись (основная таблица имеет приоритет)
                                        if not is_current_tabular and is_existing_tabular:
                                            guid_index[guid_bytes] = normalized
                    except Exception as e:
                        # Пропускаем поля с ошибками
                        continue
                    
            except Exception as e:
                # Пропускаем проблемные таблицы
                continue
        
        cursor.close()
        self._guid_to_table_cache = guid_index
        print(f"Индекс построен: {len(guid_index)} GUID -> таблицы")
        
        # Автоматически сохраняем на диск
        self.save_guid_index(guid_index)
        
        return guid_index
    
    def find_table_by_guid(self, guid_value: bytes, guid_index: Optional[Dict[bytes, str]] = None) -> Optional[str]:
        """
        Находит таблицу по значению GUID в первичном ключе.
        
        Args:
            guid_value: Значение GUID (bytes, 16 байт)
            guid_index: Индекс GUID -> таблица. Если None, используется кэш или строится новый.
            
        Returns:
            Имя таблицы или None если не найдено
        """
        if guid_index is None:
            guid_index = self.build_guid_index()
        
        return guid_index.get(guid_value)
    
    def clear_guid_index_cache(self):
        """
        Очищает кэш индекса GUID -> таблица (только в памяти).
        """
        self._guid_to_table_cache = None

    def _parse_connection_params(self) -> Tuple[str, str]:
        """Извлекает host и database из строки подключения."""
        host = 'unknown'
        database = 'unknown'
        if self.connection_string:
            for part in self.connection_string.split(';'):
                part = part.strip()
                if '=' in part:
                    key, val = part.split('=', 1)
                    key_lower = key.strip().lower()
                    if key_lower == 'server':
                        host = val.strip()
                    elif key_lower == 'database':
                        database = val.strip()
        return host, database

    def _get_guid_index_path(self) -> Path:
        """Возвращает путь к файлу GUID-индекса для текущего подключения."""
        import hashlib
        from config import DEFAULT_OUTPUT_DIR
        output_dir = Path(DEFAULT_OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        host, database = self._parse_connection_params()
        # Создаём уникальное имя файла из host+database
        key = f"{host}_{database}"
        # Санитизируем для имени файла
        safe_key = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in key)
        # Если слишком длинное — добавляем хэш
        if len(safe_key) > 60:
            h = hashlib.md5(key.encode()).hexdigest()[:8]
            safe_key = safe_key[:50] + '_' + h
        return output_dir / f"guid_index_{safe_key}.json"

    def save_guid_index(self, guid_index: Optional[Dict[bytes, str]] = None) -> bool:
        """
        Сохраняет GUID-индекс на диск с метаданными подключения.
        
        Returns:
            True если сохранение успешно
        """
        if guid_index is None:
            guid_index = self._guid_to_table_cache
        if guid_index is None:
            return False
        
        try:
            from datetime import datetime
            
            serializable = {}
            for guid_bytes, table_name in guid_index.items():
                key = base64.b64encode(guid_bytes).decode('ascii')
                serializable[key] = table_name
            
            host, database = self._parse_connection_params()
            
            path = self._get_guid_index_path()
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({
                    'version': 2,
                    'metadata': {
                        'host': host,
                        'database': database,
                        'built_at': datetime.now().isoformat(),
                        'count': len(serializable)
                    },
                    'index': serializable
                }, f, ensure_ascii=False)
            
            print(f"GUID-индекс сохранён: {path} ({len(serializable)} записей)")
            return True
        except Exception as e:
            print(f"Ошибка сохранения GUID-индекса: {e}")
            return False

    def load_guid_index(
        self,
        progress_callback: Optional[Callable[[float, str], None]] = None
    ) -> Optional[Dict[bytes, str]]:
        """
        Загружает GUID-индекс с диска.
        Проверяет соответствие сохранённого индекса текущему подключению.

        Args:
            progress_callback: Опциональный callback(progress: float, text: str) для отображения прогресса.
                              progress от 0.0 до 1.0, text — текст статуса.

        Returns:
            Словарь {guid_bytes: table_name} или None если файл не найден / не соответствует
        """
        def _report(prog: float, txt: str):
            if progress_callback:
                progress_callback(prog, txt)

        try:
            path = self._get_guid_index_path()
            if not path.exists():
                return None

            _report(0.05, "Чтение файла...")
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            _report(0.15, "Проверка подключения...")
            metadata = data.get('metadata', {})
            saved_host = metadata.get('host', '')
            saved_db = metadata.get('database', '')
            current_host, current_db = self._parse_connection_params()

            if saved_host != current_host or saved_db != current_db:
                print(f"GUID-индекс не соответствует: сохранён для {saved_host}/{saved_db}, "
                      f"текущее подключение {current_host}/{current_db}")
                return None

            serializable = data.get('index', {})
            total = len(serializable)
            guid_index: Dict[bytes, str] = {}
            batch_size = max(5000, total // 50)  # Обновлять прогресс каждые ~2%
            processed = 0

            for b64_key, table_name in serializable.items():
                guid_bytes = base64.b64decode(b64_key)
                guid_index[guid_bytes] = table_name
                processed += 1
                if processed % batch_size == 0 and total > 0:
                    pct = 0.15 + 0.85 * (processed / total)
                    _report(pct, f"Декодирование: {processed:,} / {total:,}")

            _report(1.0, "Готово")
            return guid_index
        except Exception as e:
            print(f"Ошибка загрузки GUID-индекса: {e}")
            return None

    def get_guid_index_metadata(self) -> Optional[Dict]:
        """
        Возвращает метаданные сохранённого GUID-индекса (без загрузки самого индекса).
        
        Returns:
            {'host': str, 'database': str, 'built_at': str, 'count': int} или None
        """
        try:
            path = self._get_guid_index_path()
            if not path.exists():
                return None
            
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return data.get('metadata')
        except Exception:
            return None

    def delete_guid_index_file(self) -> bool:
        """Удаляет файл GUID-индекса с диска."""
        try:
            path = self._get_guid_index_path()
            if path.exists():
                path.unlink()
                return True
            return False
        except Exception:
            return False


    # ═══════════════════════════════════════════════════════════════════
    # Индекс связей (relationship index): {table → {field → target_table}}
    # ═══════════════════════════════════════════════════════════════════

    def estimate_relationship_index_build(self) -> Tuple[int, int, float]:
        """
        Оценивает объём и время построения индекса связей.
        
        Returns:
            (n_tables, n_queries_estimate, time_estimate_sec)
        """
        all_tables = self.get_all_tables()
        tables_1c = []
        for t in all_tables:
            if t.startswith('[') and '.' in t:
                continue
            table_simple = t.strip('[]')
            if '.' in table_simple:
                table_simple = table_simple.split('.')[-1]
            if (table_simple.startswith('_') or
                table_simple.startswith('Document') or
                table_simple.startswith('Reference') or
                table_simple.startswith('Enum')):
                tables_1c.append(table_simple)
        n_tables = len(tables_1c)
        if n_tables == 0:
            return 0, 0, 0.0
        # Выборочно считаем поля binary(16) в первых 20 таблицах для оценки
        sample_size = min(20, n_tables)
        total_fields = 0
        for i in range(sample_size):
            try:
                norm = self._normalize_table_name(tables_1c[i])
                fields = self.get_binary16_fields(norm)
                if fields:
                    # Исключаем IDRRef, ID, Version, Marked
                    n = sum(1 for f in fields if f.lstrip('_') not in ('IDRRef', 'ID', 'Version', 'Marked'))
                    total_fields += n
            except Exception:
                pass
        avg_per_table = total_fields / sample_size if sample_size > 0 else 8
        n_queries = int(n_tables * avg_per_table)
        # ~0.03–0.1 сек на запрос в зависимости от размера таблицы
        time_estimate = n_queries * 0.05
        return n_tables, n_queries, time_estimate

    def build_relationship_index(
        self,
        guid_index: Optional[Dict[bytes, str]] = None,
        force_rebuild: bool = False,
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> Dict[str, Dict[str, List[str]]]:
        """
        Строит индекс связей для ВСЕХ таблиц 1С.
        Для каждого поля binary(16) собирает ВСЕ целевые таблицы (по частоте).
        Поля с данными, но без найденной цели — висячие ключи (_unresolved_fields).

        Args:
            guid_index: Основной GUID-индекс {guid_bytes → table_name}. Если None — берёт из кэша.
            force_rebuild: Пересобрать, даже если кэш есть
            progress_callback: callback(current, total, table_name)

        Returns:
            {table_name: {field_name: [target_table, ...]}} — список отсортирован по убыванию частоты
        """
        if not force_rebuild and self._relationship_index is not None:
            return self._relationship_index

        if not force_rebuild:
            loaded = self._load_relationship_index()
            if loaded is not None:
                self._relationship_index = loaded
                return loaded

        if guid_index is None:
            guid_index = self._guid_to_table_cache
        if not guid_index:
            return {}

        self.connect()
        cursor = self.conn.cursor()

        all_tables = self.get_all_tables()
        tables_1c = []
        for t in all_tables:
            if t.startswith('[') and '.' in t:
                continue
            table_simple = t.strip('[]')
            if '.' in table_simple:
                table_simple = table_simple.split('.')[-1]
            if (table_simple.startswith('_') or
                table_simple.startswith('Document') or
                table_simple.startswith('Reference') or
                table_simple.startswith('Enum')):
                tables_1c.append(table_simple)

        rel_index: Dict[str, Dict[str, List[str]]] = {}
        unresolved_index: Dict[str, List[str]] = {}
        total = len(tables_1c)

        for idx, table_name in enumerate(tables_1c):
            if progress_callback:
                progress_callback(idx, total, table_name)
            try:
                normalized = self._normalize_table_name(table_name)
                binary16_fields = self.get_binary16_fields(normalized)
                if not binary16_fields:
                    continue

                schema, table = self._parse_table_name(normalized)
                table_rels: Dict[str, List[str]] = {}
                unresolved_fields: List[str] = []

                for field_name in binary16_fields:
                    field_clean = field_name.lstrip('_')
                    if field_clean in ('IDRRef', 'ID', 'Version', 'Marked'):
                        continue
                    try:
                        query = (
                            f"SELECT DISTINCT TOP 50000 [{field_name}] "
                            f"FROM [{schema}].[{table}] "
                            f"WHERE [{field_name}] IS NOT NULL "
                            f"AND [{field_name}] != 0x00000000000000000000000000000000"
                        )
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        field_targets: Dict[str, int] = {}
                        has_data = False
                        for row in rows:
                            if not row or not row[0]:
                                continue
                            has_data = True
                            guid_value = row[0]
                            if isinstance(guid_value, bytearray):
                                guid_bytes = bytes(guid_value)
                            elif isinstance(guid_value, bytes):
                                guid_bytes = guid_value
                            else:
                                guid_bytes = bytes(guid_value)
                            if len(guid_bytes) == 16:
                                target = guid_index.get(guid_bytes)
                                if target:
                                    field_targets[target] = field_targets.get(target, 0) + 1

                        if field_targets:
                            sorted_targets = sorted(
                                field_targets.keys(),
                                key=lambda t: field_targets[t],
                                reverse=True
                            )
                            table_rels[field_name] = sorted_targets
                        elif has_data:
                            unresolved_fields.append(field_name)
                    except Exception:
                        continue

                if table_rels:
                    rel_index[normalized] = table_rels
                if unresolved_fields:
                    unresolved_index[normalized] = unresolved_fields
            except Exception:
                continue

        cursor.close()
        self._relationship_index = rel_index
        self._unresolved_fields = unresolved_index
        self._save_relationship_index(rel_index, unresolved_index)
        return rel_index

    def _get_relationship_index_path(self) -> Path:
        """Путь к файлу индекса связей на диске."""
        host, database = self._parse_connection_params()
        safe_key = f"{host}_{database}".replace('\\', '_').replace('/', '_').replace(':', '_')
        output_dir = Path('output')
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / f"relationship_index_{safe_key}.json"

    def _save_relationship_index(
        self,
        rel_index: Dict[str, Dict[str, List[str]]],
        unresolved: Optional[Dict[str, List[str]]] = None
    ) -> bool:
        """Сохраняет индекс связей на диск (version 2)."""
        try:
            from datetime import datetime
            host, database = self._parse_connection_params()
            path = self._get_relationship_index_path()
            total_fields = sum(len(v) for v in rel_index.values())
            unresolved = unresolved or {}
            ur_total = sum(len(v) for v in unresolved.values())
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({
                    'version': 2,
                    'metadata': {
                        'host': host,
                        'database': database,
                        'built_at': datetime.now().isoformat(),
                        'tables': len(rel_index),
                        'fields': total_fields,
                        'unresolved_fields': ur_total,
                    },
                    'index': rel_index,
                    'unresolved': unresolved,
                }, f, ensure_ascii=False)
            return True
        except Exception:
            return False

    def _load_relationship_index(self) -> Optional[Dict[str, Dict[str, List[str]]]]:
        """Загружает индекс связей с диска. Автоконвертация v1→v2."""
        try:
            path = self._get_relationship_index_path()
            if not path.exists():
                return None
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            metadata = data.get('metadata', {})
            current_host, current_db = self._parse_connection_params()
            if metadata.get('host') != current_host or metadata.get('database') != current_db:
                return None
            raw_index = data.get('index', {})
            # Автоконвертация v1 → v2: str → [str]
            converted: Dict[str, Dict[str, List[str]]] = {}
            for table, fields in raw_index.items():
                converted[table] = {}
                for field, target in fields.items():
                    if isinstance(target, str):
                        converted[table][field] = [target]
                    elif isinstance(target, list):
                        converted[table][field] = target
                    else:
                        converted[table][field] = [str(target)]
            self._unresolved_fields = data.get('unresolved', {})
            return converted
        except Exception:
            return None

    def get_relationship_index_metadata(self) -> Optional[Dict]:
        """Метаданные сохранённого индекса связей (без загрузки)."""
        try:
            path = self._get_relationship_index_path()
            if not path.exists():
                return None
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get('metadata')
        except Exception:
            return None

    # ═══════════════════════════════════════════════════════════════════
    # Field Stats (анализ кардинальности полей)
    # ═══════════════════════════════════════════════════════════════════

    def build_field_stats(
        self,
        sample_size: int = 2000,
        junk_threshold: int = 1,
        force_rebuild: bool = False,
        progress_callback=None
    ) -> Dict[str, Dict[str, dict]]:
        """
        Строит статистику кардинальности полей для всех таблиц 1С.
        
        Для каждого поля каждой таблицы вычисляет COUNT(DISTINCT)
        по выборке TOP(sample_size) строк.
        Поля с distinct_count <= junk_threshold помечаются как мусорные.
        
        Args:
            sample_size: Размер выборки (TOP N) для подсчёта уникальных значений
            junk_threshold: Порог мусорности (<=)
            force_rebuild: Пересобрать, даже если кэш есть
            progress_callback: Функция обратного вызова progress_callback(current, total, table_name)
            
        Returns:
            {table_name: {field_name: {distinct_count, is_junk, data_type, max_length}}}
        """
        if not force_rebuild and self._field_stats_cache is not None:
            return self._field_stats_cache

        if not force_rebuild:
            loaded = self.load_field_stats()
            if loaded is not None:
                self._field_stats_cache = loaded
                print(f"Статистика полей загружена с диска: {sum(len(v) for v in loaded.values())} полей в {len(loaded)} таблицах")
                return loaded

        self.connect()
        cursor = self.conn.cursor()

        all_tables = self.get_all_tables()

        # Фильтруем только таблицы 1С
        tables_1c = []
        for t in all_tables:
            if t.startswith('[') and '.' in t:
                continue
            table_simple = t.strip('[]')
            if '.' in table_simple:
                table_simple = table_simple.split('.')[-1]
            if (table_simple.startswith('_') or
                table_simple.startswith('Document') or
                table_simple.startswith('Reference') or
                table_simple.startswith('Enum')):
                tables_1c.append(table_simple)

        field_stats: Dict[str, Dict[str, dict]] = {}
        total = len(tables_1c)
        print(f"Анализ кардинальности полей для {total} таблиц (sample={sample_size})...")

        for idx, table_name in enumerate(tables_1c):
            if progress_callback:
                progress_callback(idx, total, table_name)

            try:
                normalized = self._normalize_table_name(table_name)
                schema, table = self._parse_table_name(normalized)
                columns = self.get_table_columns(normalized)

                if not columns:
                    continue

                table_stats: Dict[str, dict] = {}

                for col in columns:
                    col_name = col['name']
                    col_type = col.get('data_type', 'unknown')
                    col_max_length = col.get('max_length')

                    try:
                        query = f"""
                            SELECT COUNT(DISTINCT [{col_name}]) AS dc
                            FROM (SELECT TOP ({sample_size}) [{col_name}]
                                  FROM [{schema}].[{table}]) AS _sample
                        """
                        cursor.execute(query)
                        row = cursor.fetchone()
                        dc = row[0] if row else 0

                        table_stats[col_name] = {
                            'distinct_count': dc,
                            'is_junk': dc <= junk_threshold,
                            'data_type': col_type,
                            'max_length': col_max_length
                        }
                    except Exception:
                        table_stats[col_name] = {
                            'distinct_count': -1,
                            'is_junk': False,
                            'data_type': col_type,
                            'max_length': col_max_length
                        }

                if table_stats:
                    field_stats[normalized] = table_stats

            except Exception:
                continue

        cursor.close()
        self._field_stats_cache = field_stats

        total_fields = sum(len(v) for v in field_stats.values())
        junk_fields = sum(1 for t in field_stats.values() for f in t.values() if f.get('is_junk'))
        print(f"Статистика полей построена: {total_fields} полей в {len(field_stats)} таблицах, мусорных: {junk_fields}")

        self.save_field_stats(field_stats, sample_size=sample_size, junk_threshold=junk_threshold)
        return field_stats

    def _get_field_stats_path(self) -> Path:
        """Возвращает путь к файлу статистики полей."""
        import hashlib
        from config import DEFAULT_OUTPUT_DIR
        output_dir = Path(DEFAULT_OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)

        host, database = self._parse_connection_params()
        key = f"{host}_{database}"
        safe_key = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in key)
        if len(safe_key) > 60:
            h = hashlib.md5(key.encode()).hexdigest()[:8]
            safe_key = safe_key[:50] + '_' + h
        return output_dir / f"field_stats_{safe_key}.json"

    def save_field_stats(
        self,
        field_stats: Optional[Dict[str, Dict[str, dict]]] = None,
        sample_size: int = 2000,
        junk_threshold: int = 1
    ) -> bool:
        """Сохраняет статистику полей на диск."""
        if field_stats is None:
            field_stats = self._field_stats_cache
        if field_stats is None:
            return False

        try:
            from datetime import datetime
            host, database = self._parse_connection_params()

            total_fields = sum(len(v) for v in field_stats.values())
            junk_fields = sum(1 for t in field_stats.values() for f in t.values() if f.get('is_junk'))

            path = self._get_field_stats_path()
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({
                    'version': 1,
                    'metadata': {
                        'host': host,
                        'database': database,
                        'built_at': datetime.now().isoformat(),
                        'table_count': len(field_stats),
                        'total_fields': total_fields,
                        'junk_fields': junk_fields,
                        'sample_size': sample_size,
                        'junk_threshold': junk_threshold
                    },
                    'stats': field_stats
                }, f, ensure_ascii=False)

            print(f"Статистика полей сохранена: {path}")
            return True
        except Exception as e:
            print(f"Ошибка сохранения статистики полей: {e}")
            return False

    def load_field_stats(
        self,
        progress_callback: Optional[Callable[[float, str], None]] = None
    ) -> Optional[Dict[str, Dict[str, dict]]]:
        """Загружает статистику полей с диска."""
        def _report(prog: float, txt: str):
            if progress_callback:
                progress_callback(prog, txt)

        try:
            path = self._get_field_stats_path()
            if not path.exists():
                return None

            _report(0.1, "Чтение файла статистики...")
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            _report(0.8, "Проверка подключения...")
            metadata = data.get('metadata', {})
            saved_host = metadata.get('host', '')
            saved_db = metadata.get('database', '')
            current_host, current_db = self._parse_connection_params()

            if saved_host != current_host or saved_db != current_db:
                print(f"Статистика полей не соответствует текущему подключению")
                return None

            _report(1.0, "Готово")
            return data.get('stats', {})
        except Exception as e:
            print(f"Ошибка загрузки статистики полей: {e}")
            return None

    def get_field_stats_metadata(self) -> Optional[Dict]:
        """Возвращает метаданные сохранённой статистики полей."""
        try:
            path = self._get_field_stats_path()
            if not path.exists():
                return None

            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            return data.get('metadata')
        except Exception:
            return None

    def clear_field_stats_cache(self):
        """Очищает кэш статистики полей (только в памяти)."""
        self._field_stats_cache = None

    def is_junk_field(self, table_name: str, field_name: str) -> bool:
        """Проверяет, является ли поле мусорным."""
        if self._field_stats_cache is None:
            return False
        normalized = self._normalize_table_name(table_name)
        table_stats = self._field_stats_cache.get(normalized, {})
        field_info = table_stats.get(field_name, {})
        return field_info.get('is_junk', False)

    def get_field_distinct_count(self, table_name: str, field_name: str) -> int:
        """Возвращает количество уникальных значений поля (-1 если нет данных)."""
        if self._field_stats_cache is None:
            return -1
        normalized = self._normalize_table_name(table_name)
        table_stats = self._field_stats_cache.get(normalized, {})
        field_info = table_stats.get(field_name, {})
        return field_info.get('distinct_count', -1)
