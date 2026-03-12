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
        # Используем кэш: если индекс уже загружен (в память или с диска), не пересоздаём
        if self.analyzer._guid_to_table_cache is not None:
            self._guid_index = self.analyzer._guid_to_table_cache
            print(f"GUID-индекс уже закэширован: {len(self._guid_index)} записей")
        else:
            print("Построение индекса GUID для определения связей...")
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
                    # Сохраняем только реальные имена полей (из binary16_fields).
                    # Не добавляем field_key_no_rref: итерация по графу использует ключи как имена полей,
                    # и добавление варианта без RRef (напр. _Document653_I из _Document653_IDRRef) создаёт
                    # связи по несуществующим полям.
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
            # Кэшируем пустой результат, чтобы не запрашивать повторно
            self.relationship_graph[normalized] = relationships
            return relationships
        
        # Для каждого binary(16) поля пытаемся найти целевую таблицу
        for field_name in binary16_fields:
            target_table = self._find_target_table(normalized, field_name)
            if target_table:
                # Сохраняем только реальные имена полей (из binary16_fields).
                # Не добавляем field_key_no_rref: build_mixed_graph итерирует по ключам как по именам полей,
                # добавление варианта без RRef создаёт связи по несуществующим полям (напр. _Document653_I).
                relationships[field_name] = target_table
        
        # ВСЕГДА кэшируем результат (даже пустой) для предотвращения повторных SQL-запросов
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
        Находит таблицы, ссылающиеся на base_table.

        Источники (в порядке приоритета, объединяются):
        1. Индекс связей (analyzer._relationship_index) — полный набор всех таблиц.
        2. Кэш прямых связей (self.relationship_graph) — fallback для таблиц, обработанных BFS.
        """
        normalized_base = self._normalize_table_name(base_table)
        reverse_relationships = []
        found_keys: Set[str] = set()

        def _add(table_name: str, field_name: str):
            rk = f"{table_name}|{field_name}|{normalized_base}|reverse"
            if rk in found_keys:
                return
            found_keys.add(rk)
            reverse_relationships.append({
                'source_table': table_name,
                'source_alias': '',
                'field_name': field_name,
                'target_table': normalized_base,
                'target_alias': '',
                'relationship_key': rk,
                'direction': 'reverse'
            })

        # 1. Индекс связей (полный, построен на шаге 2)
        rel_idx = self.analyzer._relationship_index
        if rel_idx:
            for table_name, rels in rel_idx.items():
                if table_name == normalized_base:
                    continue
                for field_name, target_table in rels.items():
                    if self._normalize_table_name(target_table) == normalized_base:
                        _add(table_name, field_name)

        # 2. Кэш прямых связей (fallback — таблицы, обработанные BFS)
        for table_name, rels in self.relationship_graph.items():
            if table_name == normalized_base:
                continue
            for field_name, target_table in rels.items():
                if self._normalize_table_name(target_table) == normalized_base:
                    _add(table_name, field_name)

        return reverse_relationships
    
    def find_reverse_relationships_LEGACY(self, base_table: str, limit_guids: int = 100) -> List[Dict]:
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
    
    def build_mixed_graph(
        self,
        base_table: str,
        max_depth_down: int = 3,
        max_depth_up: int = 1,
        structure_parser=None,
        limit_guids: int = 100
    ) -> List[Dict]:
        """
        Строит граф связей со смешанным обходом (вниз и вверх) от корневой таблицы.

        На каждом шаге из текущей таблицы ищутся связи в обоих направлениях:
          ↓ вниз — по binary(16) полям текущей таблицы
          ↑ вверх — поиск таблиц, ссылающихся на текущую

        Ограничения считаются по каждой ветке (пути от корня):
          steps_down ≤ max_depth_down
          steps_up   ≤ max_depth_up

        Таблицы НЕ дедуплицируются — одна таблица может встречаться многократно
        (по разным полям/путям). Самоссылки допускаются.
        Зацикливание невозможно: конечный бюджет шагов гарантирует завершение.

        Args:
            base_table: Корневая таблица
            max_depth_down: Макс. суммарных шагов вниз по ветке
            max_depth_up: Макс. суммарных шагов вверх по ветке
            structure_parser: Парсер структуры (для алиасов)
            limit_guids: Лимит GUID при поиске обратных связей

        Returns:
            Список dict-ов с полями:
                source_table, source_alias, field_name,
                target_table, target_alias,
                direction ('forward'|'reverse'),
                steps_down, steps_up, depth,
                relationship_key
        """
        import time as _time
        import logging
        _log = logging.getLogger('build_mixed_graph')
        _log.setLevel(logging.DEBUG)
        if not _log.handlers:
            _h = logging.StreamHandler()
            _h.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
            _log.addHandler(_h)

        _t0 = _time.time()
        _log.info("══ START build_mixed_graph ══  base=%s  down=%s  up=%s", base_table, max_depth_down, max_depth_up)

        normalized_base = self._normalize_table_name(base_table)
        _log.info("  normalized_base = %s", normalized_base)

        # Строим GUID-индекс если ещё не построен — нужен для определения связей
        _t1 = _time.time()
        if self._guid_index is None:
            if self.analyzer._guid_to_table_cache is not None:
                self._guid_index = self.analyzer._guid_to_table_cache
                _log.info("  GUID index from cache: %d entries  (%.1fs)", len(self._guid_index), _time.time() - _t1)
            else:
                _log.info("  Building GUID index from scratch...")
                self._guid_index = self.analyzer.build_guid_index(limit_per_table=100)
                _log.info("  GUID index built: %d entries  (%.1fs)", len(self._guid_index), _time.time() - _t1)
        else:
            _log.info("  GUID index already loaded: %d entries", len(self._guid_index))

        # Убедимся что граф связей построен для корневой таблицы
        _t2 = _time.time()
        root_rels = self.get_related_tables(normalized_base)
        _log.info("  Root table forward rels: %d fields  (%.1fs)", len(root_rels), _time.time() - _t2)

        results: List[Dict] = []
        alias_counter: Dict[str, int] = {}
        visited_keys: Set[str] = set()

        reverse_cache: Dict[str, List[Dict]] = {}

        def _get_alias(table_name: str, source_table: str = None, field_name: str = None) -> str:
            base_alias = self._generate_table_alias(table_name, structure_parser, source_table, field_name)
            if table_name in alias_counter:
                alias_counter[table_name] += 1
                return f"{base_alias}_{alias_counter[table_name]}"
            else:
                alias_counter[table_name] = 1
                return base_alias

        def _find_reverse_for_table(table_name: str) -> List[Dict]:
            if table_name in reverse_cache:
                return reverse_cache[table_name]
            _tr = _time.time()
            rev = self.find_reverse_relationships(table_name, limit_guids=limit_guids)
            reverse_cache[table_name] = rev
            _log.info("      ↑ reverse for %s: %d rels  (%.1fs)", table_name, len(rev), _time.time() - _tr)
            return rev

        from collections import deque
        queue = deque()
        queue.append((normalized_base, 0, 0))

        _bfs_iter = 0
        _log.info("  BFS START  queue=1")

        while queue:
            current_table, sd, su = queue.popleft()
            _bfs_iter += 1
            _ti = _time.time()

            if _bfs_iter <= 50 or _bfs_iter % 20 == 0:
                _log.info("  [BFS #%d] table=%s  sd=%d su=%d  queue=%d  results=%d",
                          _bfs_iter, current_table, sd, su, len(queue), len(results))

            # ── Шаги ВНИЗ ──
            # Длина пути до дочернего узла = sd+su+1; не превышаем max_depth_down + max_depth_up
            if sd < max_depth_down and (sd + su + 1) <= (max_depth_down + max_depth_up):
                _tf = _time.time()
                forward_rels = self.get_related_tables(current_table)
                _elapsed_fwd = _time.time() - _tf
                if _elapsed_fwd > 1.0:
                    _log.warning("    ↓ get_related_tables(%s) SLOW: %.1fs  (%d fields)", current_table, _elapsed_fwd, len(forward_rels))

                for field_name, target_table in forward_rels.items():
                    if not self.analyzer.table_exists(target_table):
                        continue

                    new_sd = sd + 1
                    new_su = su
                    rk = f"{current_table}|{field_name}|{target_table}|fwd|sd{new_sd}_su{new_su}"
                    if rk in visited_keys:
                        continue
                    visited_keys.add(rk)

                    target_alias = _get_alias(target_table, current_table, field_name)
                    source_alias = _get_alias(current_table) if current_table != normalized_base else self._generate_table_alias(current_table, structure_parser)

                    results.append({
                        'source_table': current_table,
                        'source_alias': source_alias,
                        'field_name': field_name,
                        'target_table': target_table,
                        'target_alias': target_alias,
                        'direction': 'forward',
                        'steps_down': new_sd,
                        'steps_up': new_su,
                        'depth': new_sd + new_su,
                        'relationship_key': rk,
                    })

                    if new_sd < max_depth_down or new_su < max_depth_up:
                        queue.append((target_table, new_sd, new_su))

            # ── Шаги ВВЕРХ ──
            # Длина пути до дочернего узла = sd+su+1; не превышаем max_depth_down + max_depth_up
            if su < max_depth_up and (sd + su + 1) <= (max_depth_down + max_depth_up):
                rev_rels = _find_reverse_for_table(current_table)
                for rev_rel in rev_rels:
                    src_table = rev_rel['source_table']
                    field_name = rev_rel['field_name']

                    new_sd = sd
                    new_su = su + 1
                    rk = f"{src_table}|{field_name}|{current_table}|rev|sd{new_sd}_su{new_su}"
                    if rk in visited_keys:
                        continue
                    visited_keys.add(rk)

                    source_alias = _get_alias(src_table, current_table, field_name)
                    target_alias = self._generate_table_alias(current_table, structure_parser)

                    results.append({
                        'source_table': src_table,
                        'source_alias': source_alias,
                        'field_name': field_name,
                        'target_table': current_table,
                        'target_alias': target_alias,
                        'direction': 'reverse',
                        'steps_down': new_sd,
                        'steps_up': new_su,
                        'depth': new_sd + new_su,
                        'relationship_key': rk,
                    })

                    if new_sd < max_depth_down or new_su < max_depth_up:
                        queue.append((src_table, new_sd, new_su))

        _elapsed_total = _time.time() - _t0
        _log.info("══ END build_mixed_graph ══  results=%d  bfs_iters=%d  visited=%d  total=%.1fs",
                  len(results), _bfs_iter, len(visited_keys), _elapsed_total)

        results = self.filter_graph(results, normalized_base, base_table, max_depth_down, max_depth_up)
        _log.info("  after filter_graph: kept %d rels", len(results))

        return results

    @staticmethod
    def filter_graph(
        results: List[Dict],
        normalized_base: str,
        base_table: str,
        max_depth_down: int,
        max_depth_up: int
    ) -> List[Dict]:
        """
        Постобработка графа:
        1) Исключает «возвраты» — пары соседних рёбер (прямое + обратное)
           по одной паре таблиц/полей в противоположных направлениях.
           Возвратное ребро и вся ветвь за ним удаляются.
        2) Исключает связи с tree_path_length > max_depth_down + max_depth_up.

        Вызывается как при построении графа (build_mixed_graph),
        так и при загрузке из файла.
        """
        max_path = max_depth_down + max_depth_up

        # Строим дерево: parent → [child_rels]
        _children: Dict[str, List[Dict]] = {}
        for rel in results:
            direction = rel.get('direction', 'forward')
            parent = rel['source_table'] if direction == 'forward' else rel['target_table']
            _children.setdefault(parent, []).append(rel)
        for k in _children:
            _children[k].sort(key=lambda r: (0 if r.get('direction') != 'reverse' else 1))

        def _is_return_edge(parent_rel, child_rel):
            """child_rel — возврат по parent_rel (одна пара таблиц, противоположные направления, любые поля)."""
            if not parent_rel:
                return False
            return (
                parent_rel['source_table'] == child_rel['source_table']
                and parent_rel['target_table'] == child_rel['target_table']
                and parent_rel.get('direction') != child_rel.get('direction')
            )

        kept_rk: Set[str] = set()
        rel_key_to_path_length: Dict[str, int] = {}
        visited_rk: Set[str] = set()

        def _dfs_filter(table_name: str, path_len: int, parent_rel):
            for rel in _children.get(table_name, []):
                rk = rel['relationship_key']
                if rk in visited_rk:
                    continue
                visited_rk.add(rk)
                if _is_return_edge(parent_rel, rel):
                    continue
                child_path = path_len + 1
                if child_path > max_path:
                    continue
                rel_key_to_path_length[rk] = child_path
                kept_rk.add(rk)
                direction = rel.get('direction', 'forward')
                child = rel['target_table'] if direction == 'forward' else rel['source_table']
                _dfs_filter(child, child_path, rel)

        _dfs_filter(normalized_base, 0, None)
        if normalized_base != base_table:
            _dfs_filter(base_table, 0, None)

        # Повторный проход: запускаем DFS из таблиц, достигнутых основным DFS,
        # но чьи дочерние рёбра ещё не обработаны (например, Doc653 достигнут через VT)
        _rk_to_rel = {r['relationship_key']: r for r in results}
        _visited_tables: Set[str] = {normalized_base, base_table}
        changed = True
        while changed:
            changed = False
            for rk in list(kept_rk):
                rel = _rk_to_rel.get(rk)
                if not rel:
                    continue
                direction = rel.get('direction', 'forward')
                child = rel['target_table'] if direction == 'forward' else rel['source_table']
                if child not in _visited_tables and child in _children:
                    _visited_tables.add(child)
                    _dfs_filter(child, rel_key_to_path_length[rk], rel)
                    changed = True

        # Сироты: связи, не достигнутые DFS. Фильтруем по max_path и по возвратам.
        # Индекс принятых рёбер: (source_table, target_table, direction) — без field_name
        _kept_edge_keys: Set[tuple] = set()
        for r in results:
            if r['relationship_key'] in kept_rk:
                _kept_edge_keys.add((r['source_table'], r['target_table'], r.get('direction', 'forward')))

        for rel in results:
            rk = rel['relationship_key']
            if rk in visited_rk:
                continue
            visited_rk.add(rk)
            pl = rel.get('steps_down', 0) + rel.get('steps_up', 0)
            if pl > max_path:
                continue
            # Проверка на возврат: если ребро с теми же таблицами но противоположным направлением уже принято
            src, tgt = rel['source_table'], rel['target_table']
            opposite_dir = 'reverse' if rel.get('direction', 'forward') == 'forward' else 'forward'
            if (src, tgt, opposite_dir) in _kept_edge_keys:
                continue
            rel_key_to_path_length[rk] = pl
            kept_rk.add(rk)
            _kept_edge_keys.add((src, tgt, rel.get('direction', 'forward')))

        return [r for r in results if r['relationship_key'] in kept_rk]

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

