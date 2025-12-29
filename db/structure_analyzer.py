#!/usr/bin/env python3
"""
Анализатор структуры базы данных MS SQL Server.
Получает метаданные таблиц, полей, первичных и внешних ключей.
"""

from typing import Dict, List, Optional, Set, Tuple
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
        self._distinct_count_cache: Dict[str, int] = {}  # Кэш количества уникальных значений
    
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
    
    def build_guid_index(self, limit_per_table: int = 100) -> Dict[bytes, str]:
        """
        Строит индекс GUID -> таблица для быстрого поиска целевых таблиц.
        Для каждой таблицы проверяет поля, которые:
        - Имеют тип binary(16) или varbinary(16)
        - Равны или заканчиваются на "_IDRRef" или "IDRRef"
        Берет первые N ненулевых значений из этих полей.
        
        Args:
            limit_per_table: Максимальное количество GUID для выборки из каждого поля
            
        Returns:
            Словарь {guid_bytes: table_name}
        """
        if self._guid_to_table_cache is not None:
            return self._guid_to_table_cache
        
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
        
        for table_name in tables_ordered:
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
                
                # Затем ищем поля, которые заканчиваются на "_IDRRef" или "IDRRef"
                for col in columns:
                    col_name = col['name']
                    col_type = col['data_type']
                    col_max_length = col.get('max_length')
                    
                    # Проверяем, что это binary(16) или varbinary(16)
                    if col_type in ['binary', 'varbinary'] and col_max_length == 16:
                        # Проверяем, что имя поля равно или заканчивается на "_IDRRef" или "IDRRef"
                        if (col_name == '_IDRRef' or 
                            col_name == 'IDRRef' or 
                            col_name.endswith('_IDRRef') or 
                            col_name.endswith('IDRRef')):
                            if col_name not in pk_candidate_columns:
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
        Очищает кэш индекса GUID -> таблица.
        Полезно для перестроения индекса с новой логикой.
        """
        self._guid_to_table_cache = None
    
    def get_distinct_count(self, table_name: str, field_name: str) -> int:
        """
        Получает количество уникальных значений в поле таблицы.
        
        Args:
            table_name: Имя таблицы
            field_name: Имя поля
            
        Returns:
            Количество уникальных значений (0 в случае ошибки)
        """
        # Кэширование результатов
        cache_key = f"{table_name}|{field_name}"
        if cache_key in self._distinct_count_cache:
            return self._distinct_count_cache[cache_key]
        
        try:
            self.connect()
            schema, table = self._parse_table_name(table_name)
            cursor = self.conn.cursor()
            
            # Экранируем имена для безопасности
            query = f"SELECT COUNT(DISTINCT [{field_name}]) FROM [{schema}].[{table}]"
            cursor.execute(query)
            result = cursor.fetchone()[0]
            cursor.close()
            
            count = result if result is not None else 0
            self._distinct_count_cache[cache_key] = count
            return count
        except Exception as e:
            # В случае ошибки возвращаем 0 (консервативный подход)
            # Не логируем ошибку, чтобы не засорять вывод
            self._distinct_count_cache[cache_key] = 0
            return 0

