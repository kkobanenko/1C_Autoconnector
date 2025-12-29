#!/usr/bin/env python3
"""
Построитель графа связей между таблицами.
Анализирует схему БД и строит граф связей по полям типа binary(16).
"""

from typing import Dict, List, Optional, Set
from db.structure_analyzer import StructureAnalyzer


class RelationshipBuilder:
    """
    Класс для построения графа связей между таблицами.
    """
    
    def __init__(self, analyzer: StructureAnalyzer):
        """
        Инициализация построителя.
        
        Args:
            analyzer: Экземпляр StructureAnalyzer для работы с БД
        """
        self.analyzer = analyzer
        self.relationship_graph: Dict[str, Dict[str, str]] = {}  # {table: {field: target_table}}
        self._guid_index: Optional[Dict[bytes, str]] = None  # Кэш индекса GUID -> таблица
    
    def build_relationship_graph(self, table_names: Optional[List[str]] = None) -> Dict[str, Dict[str, str]]:
        """
        Строит граф связей между таблицами.
        
        Args:
            table_names: Список таблиц для анализа. Если None, анализируются все таблицы.
            
        Returns:
            Граф связей: {table_name: {field_name: target_table_name}}
        """
        self.relationship_graph = {}
        
        # Строим индекс GUID -> таблица заранее для ускорения поиска
        # Очищаем кэш в analyzer, чтобы гарантировать использование новой логики
        print("Построение индекса GUID для определения связей...")
        self.analyzer.clear_guid_index_cache()
        self._guid_index = self.analyzer.build_guid_index(limit_per_table=100)
        
        # Если таблицы не указаны, получаем все таблицы из БД
        if table_names is None:
            all_tables = self.analyzer.get_all_tables()
            # Фильтруем только таблицы 1С (с подчеркиванием или начинающиеся с Document/Reference/Enum)
            # Убираем полные имена со схемой
            table_names = []
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
                    table_names.append(table_simple)
        
        # Анализируем каждую таблицу
        for table_name in table_names:
            # Нормализуем имя таблицы
            normalized = self._normalize_table_name(table_name)
            
            # Получаем поля типа binary(16)
            binary16_fields = self.analyzer.get_binary16_fields(normalized)
            
            if not binary16_fields:
                continue
            
            # Инициализируем граф для этой таблицы
            if normalized not in self.relationship_graph:
                self.relationship_graph[normalized] = {}
            
            # Для каждого binary(16) поля пытаемся найти целевую таблицу
            for field_name in binary16_fields:
                target_table = self._find_target_table(normalized, field_name)
                if target_table:
                    # В 1С поля binary(16) имеют суффикс RRef (например, _Fld10028RRef)
                    # Сохраняем в графе БЕЗ суффикса для удобства поиска
                    # Но также сохраняем и с суффиксом на случай если поиск идет по полному имени
                    field_key = field_name
                    if field_name.endswith('RRef'):
                        # Сохраняем без суффикса как основной ключ
                        field_key_no_rref = field_name[:-5]  # Убираем 'RRef'
                        self.relationship_graph[normalized][field_key_no_rref] = target_table
                        # Также сохраняем с подчеркиванием если его нет
                        if not field_key_no_rref.startswith('_'):
                            self.relationship_graph[normalized]['_' + field_key_no_rref] = target_table
                    # Сохраняем и с полным именем на всякий случай
                    self.relationship_graph[normalized][field_name] = target_table
        
        return self.relationship_graph
    
    def _find_target_table(self, table_name: str, field_name: str) -> Optional[str]:
        """
        Находит целевую таблицу для поля binary(16).
        
        Приоритет:
        1. Внешние ключи из sys.foreign_keys
        2. Эвристика: поле обычно ссылается на таблицу, где ID является PK
        
        Args:
            table_name: Имя таблицы
            field_name: Имя поля (может быть с подчеркиванием или без)
            
        Returns:
            Имя целевой таблицы или None если не найдено
        """
        # Метод 1: Проверяем внешние ключи
        foreign_keys = self.analyzer.get_foreign_keys(table_name)
        
        # Пробуем найти точное совпадение
        for fk in foreign_keys:
            if fk['column_name'] == field_name:
                ref_table = fk['referenced_table']
                normalized_ref = self._normalize_table_name(ref_table)
                return normalized_ref
        
        # Пробуем найти без подчеркивания в начале поля
        if field_name.startswith('_'):
            field_name_clean = field_name.lstrip('_')
            for fk in foreign_keys:
                if fk['column_name'] == field_name_clean or fk['column_name'] == field_name:
                    ref_table = fk['referenced_table']
                    normalized_ref = self._normalize_table_name(ref_table)
                    return normalized_ref
        
        # Пробуем найти с подчеркиванием в начале поля
        if not field_name.startswith('_'):
            field_name_with_underscore = '_' + field_name
            for fk in foreign_keys:
                if fk['column_name'] == field_name_with_underscore or fk['column_name'] == field_name:
                    ref_table = fk['referenced_table']
                    normalized_ref = self._normalize_table_name(ref_table)
                    return normalized_ref
        
        # Метод 2: Эвристика для 1С
        # В 1С поля binary(16) с суффиксом RRef обычно ссылаются на Reference таблицы
        # Если внешних ключей нет, используем эвристику на основе доступных таблиц
        
        # Получаем список всех таблиц в БД
        all_tables = self.analyzer.get_all_tables()
        
        # В 1С поля RRef обычно ссылаются на Reference таблицы
        # Для начала попробуем найти любую Reference таблицу
        # В реальности нужно использовать поля TYPE и RTRef для точного определения
        # Но для простоты используем эвристику: если поле заканчивается на RRef и не является ID,
        # то это ссылка на Reference таблицу
        
        field_clean = field_name.lstrip('_')
        
        # Пропускаем системные поля (ID, Version и т.д.)
        if field_clean in ['IDRRef', 'ID', 'Version', 'Marked']:
            return None
        
        # Метод 3: Эвристика по GUID - находим первое ненулевое значение поля
        # и ищем таблицу, в первичном ключе которой есть такое значение
        # Обрабатываем все поля binary(16), которые заканчиваются на RRef или RRRef
        # (в 1С могут быть варианты: RRef, RRRef, _RRef, _RRRef и т.д.)
        # Также пробуем для всех остальных полей binary(16), которые не являются системными
        target_table = self._find_target_table_by_guid(table_name, field_name)
        if target_table:
            return target_table
        
        return None
    
    def _find_target_table_by_guid(self, table_name: str, field_name: str) -> Optional[str]:
        """
        Находит целевую таблицу для поля binary(16) используя эвристику по GUID.
        Берет первое ненулевое значение поля и ищет таблицу, в PK которой есть такое значение.
        
        Args:
            table_name: Имя таблицы
            field_name: Имя поля binary(16)
            
        Returns:
            Имя целевой таблицы или None если не найдено
        """
        try:
            # Строим индекс GUID -> таблица если еще не построен
            if self._guid_index is None:
                self._guid_index = self.analyzer.build_guid_index(limit_per_table=100)
            
            # Получаем первое ненулевое значение GUID из поля
            normalized = self._normalize_table_name(table_name)
            schema, table = self.analyzer._parse_table_name(normalized)
            
            self.analyzer.connect()
            cursor = self.analyzer.conn.cursor()
            
            # Ищем первое ненулевое значение
            query = f"""
                SELECT TOP 1 [{field_name}]
                FROM [{schema}].[{table}]
                WHERE [{field_name}] IS NOT NULL
                AND [{field_name}] != 0x00000000000000000000000000000000
            """
            
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            
            if not row or not row[0]:
                return None
            
            guid_value = row[0]
            if not guid_value:
                return None
            
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
                    return None
            
            if len(guid_bytes) != 16:
                return None
            
            # Ищем таблицу по GUID в индексе
            target_table = self.analyzer.find_table_by_guid(guid_bytes, self._guid_index)
            
            if target_table:
                normalized_target = self._normalize_table_name(target_table)
                return normalized_target
            
            return None
            
        except Exception as e:
            # В случае ошибки возвращаем None
            return None
    
    def get_related_tables(self, table_name: str) -> Dict[str, str]:
        """
        Получает список связанных таблиц для заданной таблицы.
        Если таблицы нет в графе связей, динамически строит связи через GUID индекс.
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Словарь {field_name: target_table_name}
        """
        normalized = self._normalize_table_name(table_name)
        
        # Если таблица уже в графе связей, возвращаем её связи
        if normalized in self.relationship_graph:
            return self.relationship_graph[normalized]
        
        # Если таблицы нет в графе, динамически строим связи для неё
        # Это нужно для рекурсивной обработки таблиц второго уровня и выше
        relationships = {}
        
        # Получаем поля типа binary(16)
        binary16_fields = self.analyzer.get_binary16_fields(normalized)
        
        if not binary16_fields:
            return relationships
        
        # Для каждого binary(16) поля пытаемся найти целевую таблицу
        for field_name in binary16_fields:
            target_table = self._find_target_table(normalized, field_name)
            if target_table:
                # В 1С поля binary(16) имеют суффикс RRef (например, _Fld10028RRef)
                # Сохраняем в графе БЕЗ суффикса для удобства поиска
                # Но также сохраняем и с суффиксом на случай если поиск идет по полному имени
                field_key = field_name
                if field_name.endswith('RRef'):
                    # Сохраняем без суффикса как основной ключ
                    field_key_no_rref = field_name[:-5]  # Убираем 'RRef'
                    relationships[field_key_no_rref] = target_table
                    # Также сохраняем с подчеркиванием если его нет
                    if not field_key_no_rref.startswith('_'):
                        relationships['_' + field_key_no_rref] = target_table
                # Сохраняем и с полным именем на всякий случай
                relationships[field_name] = target_table
        
        # Сохраняем в граф для будущего использования
        if relationships:
            self.relationship_graph[normalized] = relationships
        
        return relationships
    
    def _normalize_table_name(self, table_name: str) -> str:
        """
        Нормализует имя таблицы.
        
        Args:
            table_name: Имя таблицы
            
        Returns:
            Нормализованное имя таблицы
        """
        # Убираем квадратные скобки
        table_name = table_name.strip('[]')
        
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
        
        # Добавляем подчеркивание если нужно
        if not table_name.startswith('_'):
            return '_' + table_name
        return table_name
    
    def get_all_relationships(self) -> Dict[str, Dict[str, str]]:
        """
        Возвращает весь граф связей.
        
        Returns:
            Граф связей: {table_name: {field_name: target_table_name}}
        """
        return self.relationship_graph
    
    def find_reverse_relationships(self, base_table: str, limit_guids: int = 100) -> List[Dict]:
        """
        Находит все таблицы, которые ссылаются на базовую таблицу через поля binary(16).
        
        Алгоритм:
        1. Находит первичный ключ базовой таблицы (поле, равное или заканчивающееся на "_IDRRef" или "IDRRef")
        2. Получает несколько GUID значений из этого поля
        3. Для каждой таблицы в БД проверяет наличие полей binary(16), значения которых совпадают с этими GUID
        4. Использует GUID индекс для ускорения поиска
        
        Args:
            base_table: Имя базовой таблицы
            limit_guids: Максимальное количество GUID для проверки из PK базовой таблицы
            
        Returns:
            Список словарей с информацией о связях:
            [
                {
                    'source_table': str,  # Таблица, которая ссылается на базовую
                    'source_alias': str,   # Алиас исходной таблицы (будет сгенерирован позже)
                    'field_name': str,     # Поле binary(16) в исходной таблице
                    'target_table': str,   # Базовая таблица
                    'target_alias': str,   # Алиас базовой таблицы (будет сгенерирован позже)
                    'relationship_key': str # Уникальный ключ связи
                },
                ...
            ]
        """
        try:
            normalized_base = self._normalize_table_name(base_table)
            schema_base, table_base = self.analyzer._parse_table_name(normalized_base)
            
            # 1. Находим первичный ключ базовой таблицы
            pk_columns = self.analyzer.get_primary_keys(normalized_base)
            
            # Также ищем поля, которые заканчиваются на "_IDRRef" или "IDRRef"
            columns = self.analyzer.get_table_columns(normalized_base)
            pk_candidate_columns = list(pk_columns)
            
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
            
            if not pk_candidate_columns:
                return []
            
            # Берем первое поле-кандидат как PK
            pk_field = pk_candidate_columns[0]
            
            # 2. Получаем GUID значения из PK базовой таблицы
            self.analyzer.connect()
            cursor = self.analyzer.conn.cursor()
            
            query = f"""
                SELECT TOP {limit_guids} [{pk_field}]
                FROM [{schema_base}].[{table_base}]
                WHERE [{pk_field}] IS NOT NULL
                AND [{pk_field}] != 0x00000000000000000000000000000000
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            
            if not rows:
                return []
            
            # Преобразуем GUID в bytes
            base_guids = []
            for row in rows:
                guid_value = row[0]
                if not guid_value:
                    continue
                
                # Преобразуем в bytes
                if isinstance(guid_value, bytearray):
                    guid_bytes = bytes(guid_value)
                elif isinstance(guid_value, bytes):
                    guid_bytes = guid_value
                else:
                    try:
                        guid_bytes = bytes(guid_value)
                    except:
                        continue
                
                if len(guid_bytes) == 16:
                    base_guids.append(guid_bytes)
            
            if not base_guids:
                return []
            
            # 3. Для каждой таблицы в БД проверяем наличие полей binary(16), которые ссылаются на базовую
            reverse_relationships = []
            found_relationships = set()  # Для предотвращения дубликатов
            
            # Получаем все таблицы 1С
            all_tables = self.analyzer.get_all_tables()
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
            
            # Строим GUID индекс если еще не построен
            if self._guid_index is None:
                self._guid_index = self.analyzer.build_guid_index(limit_per_table=100)
            
            # Для каждой таблицы проверяем поля binary(16)
            for table_name in tables_1c:
                # Пропускаем саму базовую таблицу
                normalized_table = self._normalize_table_name(table_name)
                if normalized_table == normalized_base:
                    continue
                
                # Получаем поля типа binary(16)
                binary16_fields = self.analyzer.get_binary16_fields(normalized_table)
                if not binary16_fields:
                    continue
                
                schema_table, table_table = self.analyzer._parse_table_name(normalized_table)
                
                # Для каждого поля binary(16) проверяем, есть ли в нем значения из base_guids
                for field_name in binary16_fields:
                    # Пропускаем системные поля
                    field_clean = field_name.lstrip('_')
                    if field_clean in ['IDRRef', 'ID', 'Version', 'Marked']:
                        continue
                    
                    # Проверяем, есть ли в этом поле значения из base_guids
                    try:
                        self.analyzer.connect()
                        cursor = self.analyzer.conn.cursor()
                        
                        # Формируем условие для проверки всех GUID сразу
                        guid_conditions = []
                        for guid_bytes in base_guids[:10]:  # Ограничиваем до 10 для производительности
                            guid_hex = '0x' + guid_bytes.hex()
                            guid_conditions.append(f"[{field_name}] = {guid_hex}")
                        
                        if not guid_conditions:
                            cursor.close()
                            continue
                        
                        query = f"""
                            SELECT TOP 1 [{field_name}]
                            FROM [{schema_table}].[{table_table}]
                            WHERE {' OR '.join(guid_conditions)}
                        """
                        
                        cursor.execute(query)
                        row = cursor.fetchone()
                        cursor.close()
                        
                        if row and row[0]:
                            # Найдено совпадение - создаем связь
                            relationship_key = f"{normalized_table}|{field_name}|{normalized_base}|reverse"
                            
                            # Проверяем, не добавляли ли мы уже эту связь
                            if relationship_key not in found_relationships:
                                found_relationships.add(relationship_key)
                                reverse_relationships.append({
                                    'source_table': normalized_table,
                                    'source_alias': '',  # Будет сгенерирован позже
                                    'field_name': field_name,
                                    'target_table': normalized_base,
                                    'target_alias': '',  # Будет сгенерирован позже
                                    'relationship_key': relationship_key,
                                    'direction': 'reverse'
                                })
                    except Exception as e:
                        # Пропускаем ошибки при проверке конкретного поля
                        continue
            
            return reverse_relationships
            
        except Exception as e:
            # В случае ошибки возвращаем пустой список
            return []
    
    def collect_all_relationships_for_display(self, base_table: str, structure_parser=None) -> List[Dict]:
        """
        Собирает все связи (прямые и обратные) с базовой таблицей для отображения в UI.
        
        Args:
            base_table: Имя базовой таблицы
            structure_parser: Парсер структуры для получения человеческих названий (опционально)
            
        Returns:
            Список словарей с информацией о связях:
            [
                {
                    'source_table': str,
                    'source_alias': str,
                    'field_name': str,
                    'target_table': str,
                    'target_alias': str,
                    'depth': int,  # Всегда 1 для этого метода
                    'relationship_key': str,
                    'direction': 'forward' | 'reverse'
                },
                ...
            ]
        """
        relationships = []
        
        # 1. Собираем прямые связи (базовая таблица → другие таблицы)
        normalized_base = self._normalize_table_name(base_table)
        forward_relations = self.get_related_tables(normalized_base)
        
        # Генерируем алиас для базовой таблицы
        base_alias = self._generate_table_alias(base_table, structure_parser)
        
        # Счетчик для уникальности алиасов
        alias_counter = {}
        
        for field_name, target_table in forward_relations.items():
            # Генерируем алиас для целевой таблицы
            base_target_alias = self._generate_table_alias(target_table, structure_parser, normalized_base, field_name)
            
            # Добавляем порядковый номер, если таблица уже встречалась
            if target_table in alias_counter:
                alias_counter[target_table] += 1
                target_alias = f"{base_target_alias}_{alias_counter[target_table]}"
            else:
                alias_counter[target_table] = 1
                target_alias = base_target_alias
            
            relationship_key = f"{normalized_base}|{field_name}|{target_table}|{target_alias}"
            
            relationships.append({
                'source_table': normalized_base,
                'source_alias': base_alias,
                'field_name': field_name,
                'target_table': target_table,
                'target_alias': target_alias,
                'depth': 1,
                'relationship_key': relationship_key,
                'direction': 'forward'
            })
        
        # 2. Собираем обратные связи (другие таблицы → базовая таблица)
        reverse_relations = self.find_reverse_relationships(base_table, limit_guids=100)
        
        for rel in reverse_relations:
            # Генерируем алиасы для обратных связей
            source_alias = self._generate_table_alias(rel['source_table'], structure_parser)
            target_alias = self._generate_table_alias(rel['target_table'], structure_parser)
            
            # Обновляем алиасы в связи
            rel['source_alias'] = source_alias
            rel['target_alias'] = target_alias
            rel['depth'] = 1
            
            relationships.append(rel)
        
        return relationships
    
    def _generate_table_alias(self, table_name: str, structure_parser=None, source_table: str = None, field_name: str = None) -> str:
        """
        Генерирует алиас для таблицы на основе человеческого названия.
        Использует ту же логику, что и ViewGenerator._get_table_alias.
        
        Args:
            table_name: Имя таблицы
            structure_parser: Парсер структуры для получения человеческих названий
            source_table: Исходная таблица (для генерации уникального алиаса)
            field_name: Имя поля (для генерации уникального алиаса)
            
        Returns:
            Алиас таблицы
        """
        # Получаем человеческое название таблицы
        human_name = None
        if structure_parser:
            human_name = structure_parser.get_table_human_name(table_name)
        
        if human_name:
            # Используем человеческое название
            # Заменяем точки и специальные символы
            alias = human_name.replace('.', '_').replace(' ', '_').replace('-', '_')
        else:
            # Используем техническое название
            if source_table and field_name:
                source_short = source_table.lstrip('_').replace('.', '_')
                field_short = field_name
                target_short = table_name.lstrip('_').replace('.', '_')
                alias = f"{source_short}_{field_short}_{target_short}"
            else:
                alias = table_name.lstrip('_').replace('.', '_')
        
        # Убираем недопустимые символы для SQL идентификатора
        alias = ''.join(c if c.isalnum() or c == '_' else '_' for c in alias)
        
        # Алиас не может начинаться с цифры
        if alias and alias[0].isdigit():
            alias = 'T' + alias
        
        return alias

