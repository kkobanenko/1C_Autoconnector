#!/usr/bin/env python3
"""
Генератор SQL VIEW с рекурсивными JOIN до указанного уровня вложенности.
Генерирует представления с человеческими названиями полей и исправлением дат.

Поддерживает два стиля именования полей:
- classic: Поле (глубина 0) / Алиас_Поле (глубина > 0)
- dotted: Поле (глубина 0) / Таблица.Поле (глубина > 0)  — Вариант B из PRD

Поддерживает три формата вывода:
- view: CREATE OR ALTER VIEW ... AS SELECT ...
- select: только SELECT ...
- both: оба варианта в одном файле
"""

from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from db.structure_analyzer import StructureAnalyzer
from builders.relationship_builder import RelationshipBuilder
from parsers.structure_parser import StructureParser


class ViewGenerator:
    """
    Класс для генерации SQL VIEW с рекурсивными JOIN.
    """
    
    def __init__(
        self,
        analyzer: StructureAnalyzer,
        relationship_builder: RelationshipBuilder,
        structure_parser: StructureParser,
        fix_dates: bool = True
    ):
        """
        Инициализация генератора.
        
        Args:
            analyzer: Анализатор структуры БД
            relationship_builder: Построитель графа связей
            structure_parser: Парсер структуры из .docx
            fix_dates: Исправлять ли искаженные даты
        """
        self.analyzer = analyzer
        self.relationship_builder = relationship_builder
        self.structure_parser = structure_parser
        self.fix_dates = fix_dates
        
        # Структуры для генерации SQL
        self.joins: List[str] = []  # Список JOIN'ов
        self.selected_fields: List[str] = []  # Список полей для SELECT
        self.table_aliases: Dict[str, str] = {}  # {table_name: alias}
        self.alias_counter: Dict[str, int] = {}  # Счетчик для создания уникальных алиасов
        self.table_config: Optional[Dict[str, Dict]] = None  # Конфигурация таблиц: {relationship_key: {enabled: bool, join_type: str}}
        self.naming_style: str = 'classic'  # Стиль именования: 'classic' | 'dotted'
    
    def collect_all_relationships(
        self,
        fact_table: str,
        max_depth: int = 5
    ) -> List[Dict]:
        """
        Собирает информацию о всех таблицах, которые будут привязаны.
        Используется для предварительного отображения графа связей пользователю.
        
        Args:
            fact_table: Имя таблицы фактов (человеческое или техническое)
            max_depth: Максимальный уровень рекурсии
            
        Returns:
            Список словарей с информацией о связях:
            [
                {
                    'source_table': str,
                    'source_alias': str,
                    'field_name': str,
                    'target_table': str,
                    'target_alias': str,
                    'depth': int,
                    'relationship_key': str  # Уникальный ключ для идентификации связи
                },
                ...
            ]
        """
        # Очищаем временные структуры
        temp_aliases: Dict[str, str] = {}
        temp_alias_counter: Dict[str, int] = {}
        relationships: List[Dict] = []
        visited_relationships: Set[str] = set()  # Для предотвращения дубликатов
        
        # Определяем имя таблицы в БД
        fact_table_db = self._resolve_table_name(fact_table)
        if not fact_table_db:
            raise ValueError(f"Таблица '{fact_table}' не найдена")
        
        # Если исходное имя начиналось с подчеркивания, убеждаемся что оно сохранено
        if fact_table.strip().startswith('_') and not fact_table_db.startswith('_'):
            table_with_underscore = '_' + fact_table_db.lstrip('_')
            if self.analyzer.table_exists(table_with_underscore):
                fact_table_db = table_with_underscore
        
        # Проверяем существование таблицы в БД
        if not self.analyzer.table_exists(fact_table_db):
            raise ValueError(f"Таблица '{fact_table_db}' не существует в БД")
        
        # Рекурсивная функция для сбора связей
        def _collect_relationships_recursive(
            table_name: str,
            source_alias: str,
            current_depth: int,
            max_depth: int
        ):
            """Рекурсивно собирает информацию о связях."""
            if current_depth >= max_depth:
                return
            
            # Получаем колонки таблицы
            columns = self.analyzer.get_table_columns(table_name)
            
            # Обрабатываем каждое binary(16) поле
            for col in columns:
                col_name = col['name']
                col_type = col['data_type']
                col_max_length = col.get('max_length')
                
                # Пропускаем не binary(16) поля
                if col_type not in ['binary', 'varbinary'] or col_max_length != 16:
                    continue
                
                # Получаем целевую таблицу из графа связей
                relationships_dict = self.relationship_builder.get_related_tables(table_name)
                
                # Пробуем найти с разными вариантами имени поля
                target_table = relationships_dict.get(col_name)
                
                # Если не найдено, пробуем без суффикса RRef (4 символа)
                if not target_table and col_name.endswith('RRef'):
                    field_name_no_rref = col_name[:-4]  # Убираем 'RRef'
                    target_table = relationships_dict.get(field_name_no_rref)
                    if not target_table and field_name_no_rref.startswith('_'):
                        target_table = relationships_dict.get(field_name_no_rref.lstrip('_'))
                
                # Если не найдено, пробуем с подчеркиванием
                if not target_table and not col_name.startswith('_'):
                    target_table = relationships_dict.get('_' + col_name)
                
                if not target_table:
                    continue
                
                # Проверяем, что таблица существует
                if not self.analyzer.table_exists(target_table):
                    continue
                
                # Генерируем алиас для целевой таблицы
                human_name = self.structure_parser.get_table_human_name(target_table)
                if human_name:
                    base_alias = human_name.replace('.', '_').replace(' ', '_').replace('-', '_')
                    if target_table in temp_alias_counter:
                        temp_alias_counter[target_table] += 1
                        target_alias = f"{base_alias}_{temp_alias_counter[target_table]}"
                    else:
                        temp_alias_counter[target_table] = 1
                        target_alias = base_alias
                else:
                    if table_name and col_name:
                        source_short = table_name.lstrip('_').replace('.', '_')
                        field_short = col_name.lstrip('_').replace('RRef', '')
                        target_short = target_table.lstrip('_').replace('.', '_')
                        target_alias = f"{source_short}_{field_short}_{target_short}"
                    else:
                        target_alias = target_table.lstrip('_').replace('.', '_')
                
                # Создаем уникальный ключ связи
                relationship_key = f"{table_name}|{col_name}|{target_table}|{target_alias}"
                
                # Если связь уже обработана, пропускаем
                if relationship_key in visited_relationships:
                    continue
                
                visited_relationships.add(relationship_key)
                
                # Добавляем информацию о связи
                relationships.append({
                    'source_table': table_name,
                    'source_alias': source_alias,
                    'field_name': col_name,
                    'target_table': target_table,
                    'target_alias': target_alias,
                    'depth': current_depth + 1,
                    'relationship_key': relationship_key
                })
                
                # Рекурсивно обрабатываем целевую таблицу
                _collect_relationships_recursive(
                    target_table,
                    target_alias,
                    current_depth + 1,
                    max_depth
                )
        
        # Начинаем сбор с основной таблицы
        main_alias = self._get_table_alias(fact_table_db, None, None)
        temp_aliases[fact_table_db] = main_alias
        
        # Собираем все связи рекурсивно
        _collect_relationships_recursive(fact_table_db, main_alias, 0, max_depth)
        
        return relationships
    
    def generate_view_from_relationships(
        self,
        fact_table: str,
        relationships: List[Dict],
        table_config: Dict[str, Dict],
        excluded_fields: Dict[str, Set[str]],
        view_name: str = None,
        output_format: str = 'view',
        naming_style: str = 'classic'
    ) -> str:
        """
        Генерирует SQL VIEW из предварительно построенного списка связей (relationships).
        Учитывает только включённые таблицы (table_config) и отобранные поля (excluded_fields).
        Гарантирует уникальность имён колонок в результирующем запросе.

        Args:
            fact_table: Имя таблицы фактов
            relationships: Список связей из build_mixed_graph (с relationship_key, source_table, target_table, ...)
            table_config: {relationship_key: {enabled: bool, join_type: str}}
            excluded_fields: {relationship_key или "__root__{table}": set(field_names)} — исключённые поля
            view_name: Имя представления
            output_format: 'view' | 'select' | 'both'
            naming_style: 'classic' | 'dotted'

        Returns:
            SQL скрипт
        """
        self.table_config = table_config
        self.naming_style = naming_style
        self.joins = []
        self.selected_fields = []
        self.table_aliases = {}
        used_column_aliases: Set[str] = set()

        # Нормализуем excluded_fields: значения могут быть set или list (из JSON)
        _excl: Dict[str, Set[str]] = {}
        for k, v in excluded_fields.items():
            _excl[k] = set(v) if not isinstance(v, set) else v
        excluded_fields = _excl

        fact_table_db = self._resolve_table_name(fact_table)
        if not fact_table_db:
            raise ValueError(f"Таблица '{fact_table}' не найдена")
        if fact_table.strip().startswith('_') and not fact_table_db.startswith('_'):
            table_with_underscore = '_' + fact_table_db.lstrip('_')
            if self.analyzer.table_exists(table_with_underscore):
                fact_table_db = table_with_underscore
        
        # Варианты имени корневой таблицы (используем relationship_builder для совпадения с build_mixed_graph)
        rb_norm = self.relationship_builder._normalize_table_name
        norm_root = rb_norm(fact_table_db)
        root_names = {fact_table_db, norm_root, fact_table_db.lstrip('_'), norm_root.lstrip('_')}
        if fact_table_db.startswith('_'):
            root_names.add(fact_table_db[1:])
        if norm_root.startswith('_'):
            root_names.add(norm_root[1:])

        # Алиасы корневой таблицы из relationships (для совпадения с JOIN)
        root_aliases: Set[str] = set()
        for rel in relationships:
            src, tgt = rel.get('source_table'), rel.get('target_table')
            src_n = rb_norm(src) if src else ""
            tgt_n = rb_norm(tgt) if tgt else ""
            src_is_root = src in root_names or src_n in root_names or src_n == norm_root
            tgt_is_root = tgt in root_names or tgt_n in root_names or tgt_n == norm_root
            if src_is_root:
                a = rel.get('source_alias')
                if a:
                    root_aliases.add(a)
            if tgt_is_root:
                a = rel.get('target_alias')
                if a:
                    root_aliases.add(a)
        if not root_aliases:
            human = self.structure_parser.get_table_human_name(fact_table_db)
            main_alias = (human or fact_table_db.lstrip('_')).replace('.', '_').replace(' ', '_').replace('-', '_')
            main_alias = ''.join(c if c.isalnum() or c == '_' else '_' for c in main_alias)
            if main_alias and main_alias[0].isdigit():
                main_alias = 'T' + main_alias
            root_aliases = {main_alias}
        main_alias = next(iter(root_aliases))
        self.table_aliases[fact_table_db] = main_alias

        # DFS-порядок связей (как в UI)
        def _build_dfs_order():
            children: Dict[str, List[Dict]] = {}
            for rel in relationships:
                direction = rel.get('direction', 'forward')
                parent = rel['source_table'] if direction == 'forward' else rel['target_table']
                pn = rb_norm(parent)
                for key in (parent, pn):
                    if rel not in children.setdefault(key, []):
                        children[key].append(rel)
            for k in children:
                children[k].sort(key=lambda r: (0 if r.get('direction') != 'reverse' else 1))
            result, visited = [], set()
            def _dfs(t):
                tn = rb_norm(t)
                for key in (t, tn):
                    for rel in children.get(key, []):
                        rk = rel['relationship_key']
                        if rk in visited:
                            continue
                        visited.add(rk)
                        if table_config.get(rk, {}).get('enabled', False):
                            result.append(rel)
                        direction = rel.get('direction', 'forward')
                        child = rel['target_table'] if direction == 'forward' else rel['source_table']
                        _dfs(child)
            _dfs(rb_norm(fact_table_db))
            if rb_norm(fact_table_db) != fact_table_db:
                _dfs(fact_table_db)
            return result

        sorted_rels = _build_dfs_order()

        def _ensure_unique_alias(base_alias: str) -> str:
            """Создаёт уникальный алиас колонки."""
            alias = base_alias
            suffix = 2
            while alias in used_column_aliases:
                alias = f"{base_alias}_{suffix}"
                suffix += 1
            used_column_aliases.add(alias)
            return alias

        def _add_fields_for_table(
            table_name: str,
            alias: str,
            excl_key: str,
            is_root: bool
        ):
            """Добавляет поля таблицы (только не исключённые)."""
            columns = self.analyzer.get_table_columns(table_name)
            datetime2_fields = set(self.analyzer.get_datetime2_fields(table_name))
            excluded = excluded_fields.get(excl_key, set())

            for col in columns:
                col_name = col['name']
                col_type = col['data_type']
                col_max_length = col.get('max_length')
                if col_type in ['binary', 'varbinary'] and col_max_length == 16:
                    continue
                if col_name in excluded:
                    continue

                human_name = self.structure_parser.get_field_human_name(table_name, col_name) or col_name
                field_ref = f"[{alias}].[{col_name}]"

                if is_root:
                    field_alias = _ensure_unique_alias(human_name)
                elif naming_style == 'dotted':
                    table_human = self.structure_parser.get_table_human_name(table_name)
                    table_label = table_human if table_human else alias
                    field_alias = _ensure_unique_alias(f"{table_label}.{human_name}")
                else:
                    field_alias = _ensure_unique_alias(f"{alias}_{human_name}")

                if self.fix_dates and col_name in datetime2_fields:
                    field_expr = (
                        f"CASE WHEN YEAR([{alias}].[{col_name}]) >= 3000 "
                        f"THEN DATEADD(YEAR, -2000, [{alias}].[{col_name}]) "
                        f"ELSE [{alias}].[{col_name}] END AS [{field_alias}]"
                    )
                else:
                    field_expr = f"{field_ref} AS [{field_alias}]"
                self.selected_fields.append(field_expr)

        def _get_pk_column(table_name: str) -> str:
            """Возвращает имя колонки первичного ключа таблицы."""
            pk_cols = self.analyzer.get_primary_keys(table_name)
            if pk_cols:
                return pk_cols[0]
            cols = self.analyzer.get_table_columns(table_name)
            for c in cols:
                if c['name'] in ['_IDRRef', 'IDRRef', '_ID', 'ID']:
                    return c['name']
            for c in cols:
                if c['name'].endswith('_IDRRef') or c['name'].endswith('IDRRef'):
                    return c['name']
            return 'ID'

        # Поля корневой таблицы
        root_key = f"__root__{fact_table_db}"
        _add_fields_for_table(fact_table_db, main_alias, root_key, is_root=True)

        # JOIN и поля связанных таблиц (в joined_aliases — все алиасы корня для сопоставления)
        joined_aliases: Set[str] = set(root_aliases)
        for rel in sorted_rels:
            rk = rel['relationship_key']
            direction = rel.get('direction', 'forward')
            join_type = table_config.get(rk, {}).get('join_type', 'INNER JOIN').upper()
            if join_type not in ('LEFT JOIN', 'INNER JOIN', 'RIGHT JOIN'):
                join_type = 'INNER JOIN'

            source_table, source_alias = rel['source_table'], rel['source_alias']
            target_table, target_alias = rel['target_table'], rel['target_alias']
            field_name = rel['field_name']

            if direction == 'forward':
                new_table, new_alias = target_table, target_alias
                existing_table = source_table
                existing_alias = source_alias
            else:
                new_table, new_alias = source_table, source_alias
                existing_table = target_table
                existing_alias = target_alias

            # Используем алиас родительской таблицы из table_aliases, если она уже присоединена.
            # build_mixed_graph может выдавать разные алиасы для одной таблицы (напр. _2),
            # поэтому source_alias/target_alias из связи может не совпадать с уже добавленным.
            alias_for_join = (
                self.table_aliases.get(existing_table)
                or self.table_aliases.get(rb_norm(existing_table))
                or existing_alias
            )

            if alias_for_join not in joined_aliases:
                continue

            pk_col = _get_pk_column(new_table)
            schema, tbl = self.analyzer._parse_table_name(new_table)
            join_sql = (
                f"{join_type} [{schema}].[{tbl}] AS [{new_alias}] "
                f"ON [{alias_for_join}].[{field_name}] = [{new_alias}].[{pk_col}]"
            )
            self.joins.append(join_sql)
            self.table_aliases[new_table] = new_alias
            joined_aliases.add(new_alias)

            _add_fields_for_table(new_table, new_alias, rk, is_root=False)

        if not self.selected_fields:
            raise ValueError(
                "Нет выбранных полей. Включите хотя бы одну связь и отметьте поля в настройке таблиц."
            )

        if view_name is None:
            human_name = self.structure_parser.get_table_human_name(fact_table_db)
            view_name = ("vw_" + human_name.replace('.', '_').replace(' ', '_')) if human_name else f"vw_{fact_table_db.lstrip('_')}"

        select_body = "SELECT\n" + ",\n".join("    " + f for f in self.selected_fields)
        select_body += f"\nFROM "
        schema, table = self.analyzer._parse_table_name(fact_table_db)
        select_body += f"[{schema}].[{table}] AS [{main_alias}]\n"
        if self.joins:
            select_body += "\n".join(self.joins)

        header = self._generate_header(fact_table_db, view_name, len(sorted_rels))
        if output_format == 'select':
            return header + select_body
        elif output_format == 'both':
            view_sql = f"CREATE OR ALTER VIEW [{view_name}] AS\n{select_body}"
            sep = "\n\nGO\n\n" + "-- " + "=" * 60 + "\n-- Чистый SELECT запрос (без создания VIEW)\n-- " + "=" * 60 + "\n\n"
            return header + view_sql + sep + select_body
        return header + f"CREATE OR ALTER VIEW [{view_name}] AS\n{select_body}"

    def generate_view(
        self,
        fact_table: str,
        view_name: str = None,
        max_depth: int = 5,
        table_config: Optional[Dict[str, Dict]] = None,
        output_format: str = 'view',
        naming_style: str = 'classic'
    ) -> str:
        """
        Генерирует SQL скрипт для создания VIEW.
        
        Args:
            fact_table: Имя таблицы фактов (человеческое или техническое)
            view_name: Имя представления (если None, генерируется автоматически)
            max_depth: Максимальный уровень рекурсии
            table_config: Конфигурация таблиц: {relationship_key: {enabled: bool, join_type: str}}
            output_format: Формат вывода: 'view' | 'select' | 'both'
            naming_style: Стиль именования полей: 'classic' | 'dotted' (Вариант B: Таблица.Поле)
            
        Returns:
            SQL скрипт
        """
        # Сохраняем конфигурацию
        self.table_config = table_config or {}
        self.naming_style = naming_style
        
        # Очищаем структуры
        self.joins = []
        self.selected_fields = []
        self.table_aliases = {}
        self.alias_counter = {}
        
        # Определяем имя таблицы в БД
        fact_table_db = self._resolve_table_name(fact_table)
        if not fact_table_db:
            raise ValueError(f"Таблица '{fact_table}' не найдена")
        
        # Если исходное имя начиналось с подчеркивания, убеждаемся что оно сохранено
        if fact_table.strip().startswith('_') and not fact_table_db.startswith('_'):
            table_with_underscore = '_' + fact_table_db.lstrip('_')
            if self.analyzer.table_exists(table_with_underscore):
                fact_table_db = table_with_underscore
        
        # Проверяем существование таблицы в БД
        if not self.analyzer.table_exists(fact_table_db):
            raise ValueError(f"Таблица '{fact_table_db}' не существует в БД")
        
        # Алиас для основной таблицы
        main_alias = self._get_table_alias(fact_table_db, None, None)
        self.table_aliases[fact_table_db] = main_alias
        
        # Генерируем поля основной таблицы
        self._add_table_fields(fact_table_db, main_alias, 0, max_depth)
        
        # Генерируем имя представления
        if view_name is None:
            human_name = self.structure_parser.get_table_human_name(fact_table_db)
            if human_name:
                view_name = "vw_" + human_name.replace('.', '_').replace(' ', '_')
            else:
                view_name = f"vw_{fact_table_db.lstrip('_')}"
        
        # Формируем тело запроса (SELECT + FROM + JOINs)
        select_body = "SELECT\n"
        select_body += ",\n".join("    " + field for field in self.selected_fields)
        select_body += "\nFROM "
        schema, table = self.analyzer._parse_table_name(fact_table_db)
        select_body += f"[{schema}].[{table}] AS [{main_alias}]\n"
        if self.joins:
            select_body += "\n".join(self.joins)
        
        # Заголовок с метаданными
        header = self._generate_header(fact_table_db, view_name, max_depth)
        
        # Формируем итоговый SQL в зависимости от формата
        if output_format == 'select':
            return header + select_body
        elif output_format == 'both':
            view_sql = f"CREATE OR ALTER VIEW [{view_name}] AS\n{select_body}"
            separator = "\n\nGO\n\n" + "-- " + "=" * 60 + "\n-- Чистый SELECT запрос (без создания VIEW)\n-- " + "=" * 60 + "\n\n"
            return header + view_sql + separator + select_body
        else:  # 'view'
            return header + f"CREATE OR ALTER VIEW [{view_name}] AS\n{select_body}"
    
    def _generate_header(self, fact_table_db: str, view_name: str, max_depth: int) -> str:
        """Генерирует заголовок с метаданными."""
        human_name = self.structure_parser.get_table_human_name(fact_table_db) or fact_table_db
        lines = [
            f"-- {'=' * 60}",
            f"-- Сгенерировано: Генератор SQL VIEW для 1С",
            f"-- Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"-- Таблица фактов: {human_name} ({fact_table_db})",
            f"-- Глубина JOIN: {max_depth}",
            f"-- Количество JOIN: {len(self.joins)}",
            f"-- Количество полей: {len(self.selected_fields)}",
            f"-- {'=' * 60}",
            "",
        ]
        return "\n".join(lines) + "\n"
    
    def _add_table_fields(
        self,
        table_name: str,
        alias: str,
        current_depth: int,
        max_depth: int
    ):
        """
        Рекурсивно добавляет поля таблицы и обрабатывает связанные таблицы.
        
        Args:
            table_name: Имя таблицы
            alias: Алиас таблицы
            current_depth: Текущий уровень рекурсии
            max_depth: Максимальный уровень рекурсии
        """
        # Получаем колонки таблицы
        columns = self.analyzer.get_table_columns(table_name)
        
        # Получаем список полей datetime2(0) для исправления дат
        datetime2_fields = set(self.analyzer.get_datetime2_fields(table_name))
        
        # Добавляем поля текущей таблицы
        for col in columns:
            col_name = col['name']
            col_type = col['data_type']
            col_max_length = col.get('max_length')
            
            # Пропускаем поля binary(16) или varbinary(16) - они используются только для JOIN
            # В 1С часто используется varbinary вместо binary
            if col_type in ['binary', 'varbinary'] and col_max_length == 16:
                # Обрабатываем связь с другой таблицей
                if current_depth < max_depth:
                    self._process_relationship(table_name, alias, col_name, current_depth, max_depth)
                continue
            
            # Получаем человеческое название поля
            human_name = self.structure_parser.get_field_human_name(table_name, col_name)
            if not human_name:
                human_name = col_name
            
            # Формируем имя поля в SELECT
            field_ref = f"[{alias}].[{col_name}]"
            
            # Формируем уникальный алиас поля
            # Для основной таблицы (глубина 0) используем простое название
            # Для связанных таблиц — зависит от naming_style
            if current_depth == 0:
                # Основная таблица - используем простое человеческое название
                field_alias = human_name
            elif getattr(self, 'naming_style', 'classic') == 'dotted':
                # Вариант B: Таблица.Поле (например, Контрагент.Наименование)
                table_human = self.structure_parser.get_table_human_name(table_name)
                table_label = table_human if table_human else alias
                field_alias = f"{table_label}.{human_name}"
            else:
                # Classic: Алиас_Поле
                field_alias = f"{alias}_{human_name}"
            
            # Исправляем даты если нужно
            # Если значение поля datetime2(0) больше 3000 года, уменьшаем на 2000 лет
            if self.fix_dates and col_name in datetime2_fields:
                field_expr = (
                    f"CASE WHEN YEAR([{alias}].[{col_name}]) >= 3000 "
                    f"THEN DATEADD(YEAR, -2000, [{alias}].[{col_name}]) "
                    f"ELSE [{alias}].[{col_name}] END AS [{field_alias}]"
                )
            else:
                field_expr = f"{field_ref} AS [{field_alias}]"
            
            self.selected_fields.append(field_expr)
    
    def _process_relationship(
        self,
        source_table: str,
        source_alias: str,
        field_name: str,
        current_depth: int,
        max_depth: int
    ):
        """
        Обрабатывает связь с другой таблицей: добавляет JOIN и рекурсивно обрабатывает целевую таблицу.
        
        Args:
            source_table: Исходная таблица
            source_alias: Алиас исходной таблицы
            field_name: Имя поля связи
            current_depth: Текущий уровень рекурсии
            max_depth: Максимальный уровень рекурсии
        """
        # Получаем целевую таблицу из графа связей
        relationships = self.relationship_builder.get_related_tables(source_table)
        
        # В 1С поля binary(16) имеют суффикс RRef (например, _Fld10028RRef)
        # Граф хранит реальные имена полей из БД; fallback без RRef — для совместимости
        target_table = relationships.get(field_name)
        
        # Если не найдено, пробуем без суффикса RRef (4 символа)
        if not target_table and field_name.endswith('RRef'):
            field_name_no_rref = field_name[:-4]  # Убираем 'RRef'
            target_table = relationships.get(field_name_no_rref)
            # Также пробуем без подчеркивания в начале
            if not target_table and field_name_no_rref.startswith('_'):
                target_table = relationships.get(field_name_no_rref.lstrip('_'))
        
        # Если не найдено, пробуем с подчеркиванием
        if not target_table and not field_name.startswith('_'):
            target_table = relationships.get('_' + field_name)
        
        if not target_table:
            return
        
        # Проверяем, что таблица существует
        if not self.analyzer.table_exists(target_table):
            return
        
        # Получаем алиас для целевой таблицы
        target_alias = self._get_table_alias(target_table, source_table, field_name)
        
        # Создаем уникальный ключ связи для проверки конфигурации
        relationship_key = f"{source_table}|{field_name}|{target_table}|{target_alias}"
        
        # Проверяем конфигурацию пользователя
        # Если конфигурация задана и связь отключена, пропускаем
        if self.table_config:
            config_item = self.table_config.get(relationship_key)
            if config_item is not None and not config_item.get('enabled', True):
                return
        
        # Если JOIN уже добавлен для этой таблицы с этим алиасом, пропускаем
        if target_table in self.table_aliases and self.table_aliases[target_table] == target_alias:
            return
        
        # Сохраняем алиас
        self.table_aliases[target_table] = target_alias
        
        # Получаем первичный ключ целевой таблицы
        # Для табличных частей нужно искать поле, которое равно или заканчивается на _IDRRef
        pk_columns = self.analyzer.get_primary_keys(target_table)
        pk_column = None
        
        if pk_columns:
            pk_column = pk_columns[0]
        else:
            # Если PK не найден, ищем поле по правилам для 1С
            target_columns = self.analyzer.get_table_columns(target_table)
            
            # Проверяем, является ли таблица табличной частью (содержит _VT)
            is_tabular_part = '_VT' in target_table
            
            if is_tabular_part:
                # Для табличных частей определяем имя основной таблицы
                # Например, _Reference193_VT30459 -> _Reference193
                # Разделяем по _VT (табличные части всегда имеют формат TableName_VTNumber)
                table_parts = target_table.split('_VT', 1)  # Разделяем только по первому вхождению
                if table_parts and len(table_parts) > 0:
                    main_table_name = table_parts[0]  # _Reference193
                    
                    # Ищем поле вида _Reference193_IDRRef или заканчивающееся на _IDRRef
                    # Сначала ищем точное совпадение с именем основной таблицы
                    expected_pk_name = f"{main_table_name}_IDRRef"
                    
                    # Ищем поля binary(16), которые равны или заканчиваются на _IDRRef или IDRRef
                    idrref_fields = []
                    for col in target_columns:
                        col_name = col['name']
                        col_type = col['data_type']
                        col_max_length = col.get('max_length')
                        
                        # Проверяем, что это binary(16) или varbinary(16)
                        if col_type in ['binary', 'varbinary'] and col_max_length == 16:
                            # Проверяем, что поле равно или заканчивается на _IDRRef или IDRRef
                            if (col_name == '_IDRRef' or 
                                col_name == 'IDRRef' or 
                                col_name.endswith('_IDRRef') or 
                                col_name.endswith('IDRRef')):
                                idrref_fields.append(col_name)
                                # Приоритет полю с именем основной таблицы
                                if col_name == expected_pk_name:
                                    pk_column = col_name
                                    break
                    
                    # Если не нашли точное совпадение, берем первое найденное поле _IDRRef
                    if not pk_column and idrref_fields:
                        pk_column = idrref_fields[0]
            
            # Если не нашли поле для табличной части или это не табличная часть,
            # проверяем стандартные имена для 1С
            if not pk_column:
                id_fields = [c['name'] for c in target_columns if c['name'] in ['ID', '_IDRRef', 'IDRRef', '_ID']]
                if id_fields:
                    pk_column = id_fields[0]
                else:
                    # В последнюю очередь ищем любое поле, заканчивающееся на _IDRRef
                    idrref_fields = []
                    for col in target_columns:
                        col_name = col['name']
                        col_type = col['data_type']
                        col_max_length = col.get('max_length')
                        
                        if col_type in ['binary', 'varbinary'] and col_max_length == 16:
                            if (col_name == '_IDRRef' or 
                                col_name == 'IDRRef' or 
                                col_name.endswith('_IDRRef') or 
                                col_name.endswith('IDRRef')):
                                idrref_fields.append(col_name)
                    
                    if idrref_fields:
                        pk_column = idrref_fields[0]
                    else:
                        pk_column = 'ID'  # По умолчанию (может вызвать ошибку, но лучше чем ничего)
        
        # Определяем тип JOIN из конфигурации (по умолчанию INNER JOIN)
        join_type = "INNER JOIN"
        if self.table_config:
            config_item = self.table_config.get(relationship_key)
            if config_item and 'join_type' in config_item:
                join_type = config_item['join_type'].upper()
                # Проверяем валидность типа JOIN
                if join_type not in ['LEFT JOIN', 'INNER JOIN', 'RIGHT JOIN']:
                    join_type = "INNER JOIN"  # По умолчанию
        
        # Формируем JOIN
        schema, table = self.analyzer._parse_table_name(target_table)
        join_sql = (
            f"{join_type} [{schema}].[{table}] AS [{target_alias}] "
            f"ON [{source_alias}].[{field_name}] = [{target_alias}].[{pk_column}]"
        )
        self.joins.append(join_sql)
        
        # Рекурсивно обрабатываем целевую таблицу
        self._add_table_fields(target_table, target_alias, current_depth + 1, max_depth)
    
    def _get_table_alias(
        self,
        table_name: str,
        source_table: Optional[str],
        field_name: Optional[str]
    ) -> str:
        """
        Генерирует алиас для таблицы.
        
        Правила:
        - Если есть человеческое название: используем его + порядковый номер (если таблица уже привязана)
        - Иначе: {SourceTable}_{FieldName}_{TargetTable}
        
        Args:
            table_name: Имя таблицы
            source_table: Имя исходной таблицы (для связи)
            field_name: Имя поля связи
            
        Returns:
            Алиас таблицы
        """
        # Получаем человеческое название таблицы
        human_name = self.structure_parser.get_table_human_name(table_name)
        
        if human_name:
            # Используем человеческое название
            # Заменяем точки и специальные символы
            base_alias = human_name.replace('.', '_').replace(' ', '_').replace('-', '_')
            
            # Если таблица уже привязана, добавляем порядковый номер
            if table_name in self.alias_counter:
                self.alias_counter[table_name] += 1
                alias = f"{base_alias}_{self.alias_counter[table_name]}"
            else:
                self.alias_counter[table_name] = 1
                alias = base_alias
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
        if alias[0].isdigit():
            alias = 'T' + alias
        
        return alias
    
    def _resolve_table_name(self, table_name: str) -> Optional[str]:
        """
        Разрешает имя таблицы (человеческое или техническое) в имя в БД.
        
        Args:
            table_name: Имя таблицы (может быть человеческим или техническим)
            
        Returns:
            Имя таблицы в БД или None если не найдено
        """
        # Убираем пробелы
        table_name = table_name.strip()
        
        # Если имя начинается с подчеркивания - это техническое имя БД
        # Используем его как есть (сохраняем подчеркивание)
        if table_name.startswith('_'):
            # Проверяем существование с разными вариантами имени
            if self.analyzer.table_exists(table_name):
                return table_name
            # Также проверяем с нормализацией (на случай если в БД имя в другом формате)
            normalized = self._normalize_table_name(table_name)
            if normalized != table_name and self.analyzer.table_exists(normalized):
                return normalized
            return None
        
        # Сначала проверяем, является ли это человеческим названием
        db_name = self.structure_parser.get_table_db_name(table_name)
        if db_name:
            return db_name
        
        # Если не найдено, проверяем как техническое имя (добавляем подчеркивание)
        normalized = self._normalize_table_name(table_name)
        if self.analyzer.table_exists(normalized):
            return normalized
        
        return None
    
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

