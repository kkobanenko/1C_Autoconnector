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

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import Counter, defaultdict
from db.structure_analyzer import StructureAnalyzer
from builders.relationship_builder import RelationshipBuilder
from parsers.structure_parser import StructureParser

# Допустимые ключевые слова JOIN в T-SQL (как в UI генератора).
_ALLOWED_SQL_JOIN_TYPES = frozenset({
    'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL OUTER JOIN',
})

# Лимит SQL Server на число столбцов в одном VIEW (ошибка 4505 при превышении).
SQL_SERVER_VIEW_MAX_COLUMNS = 1024
# В комментариях SQL не перечислять больше строк на таблицу (остальное — «ещё K»).
_TRUNCATION_COMMENT_MAX_PER_TABLE = 50


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
        # После generate_view / generate_view_from_relationships: отчёт об обрезке до SQL_SERVER_VIEW_MAX_COLUMNS (для UI).
        self.last_sql_truncation_report: Optional[Dict[str, Any]] = None
        # Только SELECT: число столбцов > лимита VIEW — предупреждение в шапке SQL и опционально в UI.
        self.last_select_exceeds_view_limit: bool = False
    
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
                
                # Получаем целевую таблицу из графа связей (List[str] — берём первую/главную)
                relationships_dict = self.relationship_builder.get_related_tables(table_name)
                def _first_target(val):
                    if isinstance(val, list) and val:
                        return val[0]
                    return val if isinstance(val, str) else None
                target_table = _first_target(relationships_dict.get(col_name))
                if not target_table and col_name.endswith('RRef'):
                    field_name_no_rref = col_name[:-4]
                    target_table = _first_target(relationships_dict.get(field_name_no_rref))
                    if not target_table and field_name_no_rref.startswith('_'):
                        target_table = _first_target(relationships_dict.get(field_name_no_rref.lstrip('_')))
                if not target_table and not col_name.startswith('_'):
                    target_table = _first_target(relationships_dict.get('_' + col_name))
                if not target_table:
                    continue
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

    def get_effective_relationships(
        self,
        fact_table: str,
        relationships: List[Dict],
        table_config: Dict[str, Dict]
    ) -> Tuple[List[Dict], Set[str]]:
        """
        Возвращает связи для визуализации/SQL: включённые + транзитные (путь к включённым).
        Используется в «Визуализировать конфигурацию» и совпадает с набором JOIN в SQL.

        Returns:
            (list of relationships, set of transit-only relationship_keys)
        """
        fact_table_db = self._resolve_table_name(fact_table)
        if not fact_table_db:
            return [], set()
        if fact_table.strip().startswith('_') and not fact_table_db.startswith('_'):
            table_with_underscore = '_' + fact_table_db.lstrip('_')
            if self.analyzer.table_exists(table_with_underscore):
                fact_table_db = table_with_underscore

        rb_norm = self.relationship_builder._normalize_table_name
        norm_root = rb_norm(fact_table_db)

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

        enabled_targets: Set[str] = set()
        for rel in relationships:
            if table_config.get(rel['relationship_key'], {}).get('enabled', False):
                direction = rel.get('direction', 'forward')
                joined = rel['target_table'] if direction == 'forward' else rel['source_table']
                enabled_targets.add(joined)
                enabled_targets.add(rb_norm(joined))

        changed = True
        while changed:
            changed = False
            for rel in relationships:
                direction = rel.get('direction', 'forward')
                child = rel['target_table'] if direction == 'forward' else rel['source_table']
                cn = rb_norm(child)
                if cn in enabled_targets or child in enabled_targets:
                    parent = rel['source_table'] if direction == 'forward' else rel['target_table']
                    pn = rb_norm(parent)
                    if pn not in enabled_targets and parent not in enabled_targets:
                        enabled_targets.add(pn)
                        enabled_targets.add(parent)
                        changed = True

        result, visited, transit_only_rks = [], set(), set()

        def _dfs(t):
            tn = rb_norm(t)
            for key in (t, tn):
                for rel in children.get(key, []):
                    rk = rel['relationship_key']
                    if rk in visited:
                        continue
                    visited.add(rk)
                    direction = rel.get('direction', 'forward')
                    child = rel['target_table'] if direction == 'forward' else rel['source_table']
                    cn = rb_norm(child)
                    is_enabled = table_config.get(rk, {}).get('enabled', False)
                    is_needed = cn in enabled_targets or child in enabled_targets
                    if is_enabled:
                        result.append(rel)
                    elif is_needed:
                        result.append(rel)
                        transit_only_rks.add(rk)
                    _dfs(child)

        _dfs(norm_root)
        if norm_root != fact_table_db:
            _dfs(fact_table_db)
        return result, transit_only_rks

    def generate_view_from_relationships(
        self,
        fact_table: str,
        relationships: List[Dict],
        table_config: Dict[str, Dict],
        excluded_fields: Dict[str, Set[str]],
        view_name: str = None,
        output_format: str = 'view',
        naming_style: str = 'classic',
        max_depth_down: int = 999,
        max_depth_up: int = 999,
        paths_from_root: Optional[Dict[str, List[str]]] = None
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
            max_depth_down: Макс. шагов вниз по пути (ограничение DFS)
            max_depth_up: Макс. шагов вверх по пути (ограничение DFS)
            paths_from_root: {relationship_key: [rk1, rk2, ...]} — путь от корня до узла (из UI).
                Если задан, sorted_rels строится из этих путей вместо DFS.

        Returns:
            SQL скрипт
        """
        self.table_config = table_config
        self.naming_style = naming_style
        self.joins = []
        self.selected_fields = []
        self.last_sql_truncation_report = None
        self.last_select_exceeds_view_limit = False
        self.table_aliases = {}
        used_column_aliases: Set[str] = set()
        # Алиасы таблиц в FROM/JOIN должны быть глобально уникальны (в т.ч. при коллизии
        # после замены символов в имени или при расхождении логики графа и счётчиков).
        used_join_aliases: Set[str] = set()

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
        used_join_aliases.add(main_alias)

        # DFS-порядок связей (как в UI). Включаем транзитные связи — путь к включённым таблицам.
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

            # Таблицы, до которых нужно дойти: включённые + их родители по пути от корня
            enabled_targets: Set[str] = set()
            enabled_rks = [rk for rk, cfg in table_config.items() if cfg.get('enabled')]
            for rel in relationships:
                if table_config.get(rel['relationship_key'], {}).get('enabled', False):
                    direction = rel.get('direction', 'forward')
                    joined = rel['target_table'] if direction == 'forward' else rel['source_table']
                    enabled_targets.add(joined)
                    enabled_targets.add(rb_norm(joined))

            # Расширяем: добавляем только родителя, который является корнем (путь root -> enabled).
            # У enabled может быть несколько «родителей» (reverse rels), но путь от root — только один.
            norm_root = rb_norm(fact_table_db)
            root_names = {fact_table_db, norm_root}
            for rel in relationships:
                if not table_config.get(rel['relationship_key'], {}).get('enabled', False):
                    continue
                direction = rel.get('direction', 'forward')
                child = rel['target_table'] if direction == 'forward' else rel['source_table']
                parent = rel['source_table'] if direction == 'forward' else rel['target_table']
                # Для enabled rel добавляем parent только если это корень (путь root -> child)
                if rb_norm(parent) in root_names or parent in root_names:
                    enabled_targets.add(norm_root)
                    enabled_targets.add(fact_table_db)
                    break

            # Транзитные связи: нужны для пути, но не включены пользователем (не добавляем их поля)
            transit_only_rks: Set[str] = set()

            _max_path = max_depth_down + max_depth_up

            result, visited = [], set()

            def _dfs(t, parent_sd: int, parent_su: int):
                """parent_sd/parent_su — накопительные шаги вниз/вверх от корня до t."""
                tn = rb_norm(t)
                for key in (t, tn):
                    for rel in children.get(key, []):
                        rk = rel['relationship_key']
                        if rk in visited:
                            continue
                        direction = rel.get('direction', 'forward')
                        if direction == 'forward':
                            child_sd = parent_sd + 1
                            child_su = parent_su
                        else:
                            child_sd = parent_sd
                            child_su = parent_su + 1
                        # Ограничение по длине пути: не обрабатывать и не рекурсировать
                        if child_sd > max_depth_down or child_su > max_depth_up:
                            continue
                        if child_sd + child_su > _max_path:
                            continue
                        visited.add(rk)
                        child = rel['target_table'] if direction == 'forward' else rel['source_table']
                        cn = rb_norm(child)
                        is_enabled = table_config.get(rk, {}).get('enabled', False)
                        is_needed = cn in enabled_targets or child in enabled_targets
                        if is_enabled:
                            result.append(rel)
                            _dfs(child, child_sd, child_su)
                        elif is_needed:
                            result.append(rel)
                            transit_only_rks.add(rk)
                            _dfs(child, child_sd, child_su)

            _dfs(rb_norm(fact_table_db), 0, 0)
            if rb_norm(fact_table_db) != fact_table_db:
                _dfs(fact_table_db, 0, 0)

            return result, transit_only_rks

        rk_to_rel = {r['relationship_key']: r for r in relationships}
        if paths_from_root:
            # Строим sorted_rels из путей UI (порядок первого вхождения)
            sorted_rks = []
            enabled_rks = [rk for rk, cfg in (table_config or {}).items() if cfg.get('enabled')]
            for rk in enabled_rks:
                path = paths_from_root.get(rk, [rk])
                for p in path:
                    if p not in sorted_rks:
                        sorted_rks.append(p)
            sorted_rels = [rk_to_rel[rk] for rk in sorted_rks if rk in rk_to_rel]
            transit_only_rks = {rk for rk in sorted_rks if rk in rk_to_rel and rk not in enabled_rks}
        else:
            sorted_rels, transit_only_rks = _build_dfs_order()
            transit_only_rks = set(transit_only_rks) if transit_only_rks else set()

        # Валидация: предупреждение при расхождении числа JOIN'ов и включённых связей
        _enabled_count = sum(1 for cfg in (table_config or {}).values() if cfg.get('enabled'))
        if _enabled_count > 0 and len(sorted_rels) == 0:
            logging.warning(
                f"[ViewGenerator] Включено связей: {_enabled_count}, но sorted_rels пуст. "
                "Проверьте граф и table_config."
            )
        elif _enabled_count > 0 and len(sorted_rels) > _enabled_count * 3:
            logging.warning(
                f"[ViewGenerator] Возможное расхождение: включено {_enabled_count} связей, "
                f"JOIN'ов в sorted_rels: {len(sorted_rels)}. Ожидаемо при транзитных путях, "
                "но проверьте при аномалиях."
            )

        def _ensure_unique_alias(base_alias: str) -> str:
            """Создаёт уникальный алиас колонки."""
            alias = base_alias
            suffix = 2
            while alias in used_column_aliases:
                alias = f"{base_alias}_{suffix}"
                suffix += 1
                used_column_aliases.add(alias)
            return alias

        # Накопление строк SELECT с метаданными (порядок = порядок во VIEW при обрезке).
        select_rows: List[Dict[str, str]] = []

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
                if col_name in excluded:
                    continue
                # binary(16): раньше всегда пропускали; теперь включаем при явном выборе (не в excluded)

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
                select_rows.append({
                    "expr": field_expr,
                    "table_db": table_name,
                    "col_tech": col_name,
                    "output_alias": field_alias,
                    "excl_key": excl_key,
                })

        # Поля корневой таблицы
        root_key = f"__root__{fact_table_db}"
        _add_fields_for_table(fact_table_db, main_alias, root_key, is_root=True)

        # JOIN и поля связанных таблиц (в joined_aliases — все алиасы корня для сопоставления)
        joined_aliases: Set[str] = set(root_aliases)
        for rel in sorted_rels:
            rk = rel['relationship_key']
            direction = rel.get('direction', 'forward')
            _raw_cfg_jt = table_config.get(rk, {}).get('join_type', 'INNER JOIN')
            join_type = str(_raw_cfg_jt).upper()
            if join_type not in _ALLOWED_SQL_JOIN_TYPES:
                logging.warning(
                    "[ViewGenerator] Неизвестный join_type %r для связи %s (sorted_rels), подставляется INNER JOIN.",
                    _raw_cfg_jt,
                    rk,
                )
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

            # Гарантируем уникальность корреляционного имени в SQL (дубликаты ломают SELECT).
            _base_alias = new_alias
            _suf = 2
            while new_alias in used_join_aliases:
                new_alias = f"{_base_alias}_{_suf}"
                _suf += 1
            used_join_aliases.add(new_alias)

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

            # Для обратных связей: field_name — FK в потомке (new_table), родитель — по PK.
            # Для прямых: field_name — FK в родителе (existing), потомок — по PK.
            if direction == 'reverse':
                parent_pk = self._resolve_pk_column_for_join(existing_table)
                schema, tbl = self.analyzer._parse_table_name(new_table)
                join_sql = (
                    f"{join_type} [{schema}].[{tbl}] AS [{new_alias}] "
                    f"ON [{alias_for_join}].[{parent_pk}] = [{new_alias}].[{field_name}]"
                )
            else:
                pk_col = self._resolve_pk_column_for_join(new_table)
                schema, tbl = self.analyzer._parse_table_name(new_table)
                join_sql = (
                    f"{join_type} [{schema}].[{tbl}] AS [{new_alias}] "
                    f"ON [{alias_for_join}].[{field_name}] = [{new_alias}].[{pk_col}]"
                )
            self.joins.append(join_sql)
            self.table_aliases[new_table] = new_alias
            joined_aliases.add(new_alias)

            # Транзитные связи: только JOIN, поля не добавляем
            if rk not in transit_only_rks:
                _add_fields_for_table(new_table, new_alias, rk, is_root=False)

        if not select_rows:
            raise ValueError(
                "Нет выбранных полей. Включите хотя бы одну связь и отметьте поля в настройке таблиц."
            )

        if view_name is None:
            human_name = self.structure_parser.get_table_human_name(fact_table_db)
            view_name = ("vw_" + human_name.replace('.', '_').replace(' ', '_')) if human_name else f"vw_{fact_table_db.lstrip('_')}"

        # Собираем таблицы и выбранные поля для заголовка (только где выбрано хотя бы одно)
        def _sel_cols(cols_list, excl_set):
            """Выбранные поля (не в excl_set); binary(16) включаем при явном выборе."""
            return [c['name'] for c in cols_list if c['name'] not in excl_set]
        tables_with_selected: Dict[str, List[str]] = {}
        root_key = f"__root__{fact_table_db}"
        root_cols = self.analyzer.get_table_columns(fact_table_db)
        root_sel = _sel_cols(root_cols, excluded_fields.get(root_key, set()))
        if root_sel:
            tables_with_selected[fact_table_db] = root_sel
        for rel in sorted_rels:
            rk = rel['relationship_key']
            if rk in transit_only_rks:
                continue
            direction = rel.get('direction', 'forward')
            tgt = rel['target_table'] if direction == 'forward' else rel['source_table']
            cols = self.analyzer.get_table_columns(tgt)
            sel = _sel_cols(cols, excluded_fields.get(rk, set()))
            if sel:
                tables_with_selected[tgt] = sel

        return self._compose_sql_output_with_view_limit(
            select_rows=select_rows,
            fact_table_db=fact_table_db,
            main_alias=main_alias,
            view_name=view_name,
            header_depth_metric=len(sorted_rels),
            output_format=output_format,
            tables_with_selected=tables_with_selected,
        )

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
        self._select_field_rows: List[Dict[str, str]] = []
        self.last_sql_truncation_report = None
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

        rows = self._select_field_rows
        if len(rows) != len(self.selected_fields):
            rows = [
                {
                    "expr": e,
                    "table_db": "",
                    "col_tech": "",
                    "output_alias": "",
                    "excl_key": "",
                }
                for e in self.selected_fields
            ]

        return self._compose_sql_output_with_view_limit(
            select_rows=rows,
            fact_table_db=fact_table_db,
            main_alias=main_alias,
            view_name=view_name,
            header_depth_metric=max_depth,
            output_format=output_format,
            tables_with_selected=None,
        )
    
    def _select_body_from_rows(
        self,
        rows: List[Dict[str, str]],
        main_alias: str,
        fact_table_db: str,
    ) -> str:
        """Собирает SELECT ... FROM ... JOIN из списка элементов с ключом 'expr'."""
        exprs = [r["expr"] for r in rows]
        select_body = "SELECT\n" + ",\n".join("    " + f for f in exprs)
        select_body += "\nFROM "
        schema, table = self.analyzer._parse_table_name(fact_table_db)
        select_body += f"[{schema}].[{table}] AS [{main_alias}]\n"
        if self.joins:
            select_body += "\n".join(self.joins)
        return select_body

    def _omitted_field_comment_lines(self, omitted: List[Dict[str, str]]) -> List[str]:
        """Комментарии с перечнем полей, не попавших во VIEW."""
        if not omitted:
            return []
        max_pt = _TRUNCATION_COMMENT_MAX_PER_TABLE
        by_tbl: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        for r in omitted:
            by_tbl[r["table_db"]].append(r)
        lines = [
            "-- ────────────────────────────────────────────────────────",
            "-- Поля, не вошедшие во VIEW (лимит SQL Server 1024 столбца):",
            "-- ────────────────────────────────────────────────────────",
        ]
        for tbl in sorted(by_tbl.keys(), key=lambda t: (-len(by_tbl[t]), t)):
            trows = by_tbl[tbl]
            th = self.structure_parser.get_table_human_name(tbl) or tbl
            lines.append(f"-- Таблица «{th}» ({tbl}) — отброшено: {len(trows)}")
            show = trows[:max_pt]
            for r in show:
                lines.append(f"--   {r['col_tech']}  →  результирующее имя [{r['output_alias']}]")
            if len(trows) > max_pt:
                lines.append(f"--   ... ещё {len(trows) - max_pt} полей из этой таблицы")
        lines.append("-- ────────────────────────────────────────────────────────")
        return lines

    def _make_truncation_report(
        self,
        omitted: List[Dict[str, str]],
        view_n: int,
        full_n: int,
    ) -> Dict[str, Any]:
        """Сводка для st.warning и отладки (после обрезки VIEW)."""
        cnt = Counter(r["table_db"] for r in omitted)
        top = cnt.most_common(20)
        top_tables: List[Dict[str, Any]] = []
        summary_parts: List[str] = []
        for tbl, n in top[:8]:
            th = self.structure_parser.get_table_human_name(tbl) or tbl
            top_tables.append({"table": tbl, "count": n, "human": th})
            summary_parts.append(f"{th}: {n}")
        msg = (
            f"Во VIEW включено {view_n} из {full_n} столбцов. "
            f"Отсечено от VIEW: {len(omitted)}. "
            f"Топ таблиц по числу отсечённых полей: {', '.join(summary_parts)}"
        )
        return {
            "truncated": True,
            "omitted_count": len(omitted),
            "view_column_count": view_n,
            "full_column_count": full_n,
            "top_tables": top_tables,
            "summary_message": msg,
        }

    def _compose_sql_output_with_view_limit(
        self,
        *,
        select_rows: List[Dict[str, str]],
        fact_table_db: str,
        main_alias: str,
        view_name: str,
        header_depth_metric: int,
        output_format: str,
        tables_with_selected: Optional[Dict[str, List[str]]] = None,
    ) -> str:
        """
        Формирует итоговый SQL с учётом лимита столбцов VIEW.
        Для 'view' и 'both' тело CREATE VIEW обрезается до SQL_SERVER_VIEW_MAX_COLUMNS.
        Для 'both' второй SELECT (после GO) — полный. Для 'select' обрезки нет; при N>1024 — предупреждение в шапке.
        """
        n_full = len(select_rows)
        self.selected_fields = [r["expr"] for r in select_rows]

        self.last_sql_truncation_report = None
        need_truncate_view = output_format in ("view", "both") and n_full > SQL_SERVER_VIEW_MAX_COLUMNS
        view_rows = (
            select_rows[:SQL_SERVER_VIEW_MAX_COLUMNS] if need_truncate_view else select_rows
        )
        omitted = select_rows[len(view_rows) :] if need_truncate_view else []

        if need_truncate_view and omitted:
            self.last_sql_truncation_report = self._make_truncation_report(
                omitted, len(view_rows), n_full
            )

        select_body_full = self._select_body_from_rows(
            select_rows, main_alias, fact_table_db
        )
        select_body_view = self._select_body_from_rows(
            view_rows, main_alias, fact_table_db
        )

        trunc_lines = self._omitted_field_comment_lines(omitted)
        warn_select_only = output_format == "select" and n_full > SQL_SERVER_VIEW_MAX_COLUMNS
        if warn_select_only:
            self.last_select_exceeds_view_limit = True

        # В шапке «во VIEW» — только если реально генерируется обрезанный VIEW.
        col_view = len(view_rows) if need_truncate_view else None
        col_full = n_full if need_truncate_view else None

        header = self._generate_header(
            fact_table_db,
            view_name,
            header_depth_metric,
            tables_with_selected=tables_with_selected,
            column_count_in_view=col_view,
            column_count_full_select=col_full,
            truncation_comment_lines=trunc_lines if trunc_lines else None,
            warn_select_exceeds_view_limit=warn_select_only,
            note_view_vs_full_select=output_format == "both" and need_truncate_view,
        )

        if output_format == "select":
            return header + select_body_full
        if output_format == "both":
            view_sql = f"CREATE OR ALTER VIEW [{view_name}] AS\n{select_body_view}"
            sep = (
                "\n\nGO\n\n"
                + "-- " + "=" * 60 + "\n"
                + "-- Чистый SELECT запрос (без создания VIEW), полный список столбцов\n"
                + "-- " + "=" * 60 + "\n\n"
            )
            return header + view_sql + sep + select_body_full
        return header + f"CREATE OR ALTER VIEW [{view_name}] AS\n{select_body_view}"

    def _generate_header(
        self,
        fact_table_db: str,
        view_name: str,
        max_depth: int,
        tables_with_selected: Optional[Dict[str, List[str]]] = None,
        *,
        column_count_in_view: Optional[int] = None,
        column_count_full_select: Optional[int] = None,
        truncation_comment_lines: Optional[List[str]] = None,
        warn_select_exceeds_view_limit: bool = False,
        note_view_vs_full_select: bool = False,
    ) -> str:
        """Генерирует заголовок с метаданными."""
        human_name = self.structure_parser.get_table_human_name(fact_table_db) or fact_table_db
        lines = [
            f"-- {'=' * 60}",
            f"-- Сгенерировано: Генератор SQL VIEW для 1С",
            f"-- Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"-- Таблица фактов: {human_name} ({fact_table_db})",
            f"-- Глубина JOIN: {max_depth}",
            f"-- Количество JOIN: {len(self.joins)}",
        ]
        if column_count_in_view is not None:
            lines.append(f"-- Количество полей во VIEW: {column_count_in_view}")
        else:
            lines.append(f"-- Количество полей: {len(self.selected_fields)}")
        if (
            column_count_full_select is not None
            and column_count_in_view is not None
            and column_count_full_select > column_count_in_view
        ):
            lines.append(
                f"-- Полный SELECT (все выбранные поля): {column_count_full_select}"
            )
            lines.append(
                f"-- Отсечено от VIEW из‑за лимита SQL Server: "
                f"{column_count_full_select - column_count_in_view}"
            )
        if note_view_vs_full_select:
            lines.append(
                "-- В этом файле блок CREATE VIEW содержит не более 1024 столбцов; "
                "ниже после GO — полный SELECT со всеми столбцами."
            )
        if warn_select_exceeds_view_limit:
            lines.append(
                "-- ВНИМАНИЕ: число столбцов > 1024 — весь этот SELECT нельзя обернуть "
                "в одно представление CREATE VIEW в SQL Server (лимит 1024 столбца на VIEW)."
            )
        if truncation_comment_lines:
            lines.extend(truncation_comment_lines)
        if tables_with_selected is not None and tables_with_selected:
            lines.append("-- Таблицы и выбранные поля (по конфигурации, до обрезки VIEW):")
            for tbl, fields in tables_with_selected.items():
                t_human = self.structure_parser.get_table_human_name(tbl) or tbl
                lines.append(f"--   {t_human} ({tbl}): {', '.join(fields)}")
        lines.extend([
            f"-- {'=' * 60}",
            "",
        ])
        return "\n".join(lines) + "\n"
    
    def _resolve_pk_column_for_join(self, target_table: str) -> str:
        """
        Имя колонки на стороне присоединяемой таблицы для условия JOIN (для типовых таблиц 1С — _IDRRef).
        Возвращает только реально существующее поле; не подставляет «ID», если колонки нет в метаданных.

        Логика совпадает с веткой JOIN в _process_relationship, чтобы путь generate_view_from_relationships
        не использовал упрощённый fallback.
        """
        target_columns = self.analyzer.get_table_columns(target_table)
        if not target_columns:
            raise ValueError(
                f"[ViewGenerator] Нет колонок в метаданных для таблицы {target_table!r} (JOIN к ключу невозможен)."
            )
        name_set = {c['name'] for c in target_columns}

        pk_columns = self.analyzer.get_primary_keys(target_table)
        if pk_columns:
            for pk in pk_columns:
                if pk in name_set:
                    return pk

        is_tabular_part = '_VT' in target_table
        if is_tabular_part:
            table_parts = target_table.split('_VT', 1)
            main_table_name = table_parts[0] if table_parts else ''
            if main_table_name:
                expected_pk_name = f"{main_table_name}_IDRRef"
                idrref_fields: List[str] = []
                pk_column: Optional[str] = None
                for col in target_columns:
                    col_name = col['name']
                    col_type = col['data_type']
                    col_max_length = col.get('max_length')
                    if col_type in ['binary', 'varbinary'] and col_max_length == 16:
                        if (
                            col_name == '_IDRRef'
                            or col_name == 'IDRRef'
                            or col_name.endswith('_IDRRef')
                            or col_name.endswith('IDRRef')
                        ):
                            idrref_fields.append(col_name)
                            if col_name == expected_pk_name:
                                pk_column = col_name
                                break
                if pk_column:
                    return pk_column
                if idrref_fields:
                    return idrref_fields[0]

        for std_name in ('_IDRRef', 'IDRRef', '_ID', 'ID'):
            if std_name in name_set:
                return std_name

        for col in target_columns:
            cn = col['name']
            if cn.endswith('_IDRRef') or cn.endswith('IDRRef'):
                return cn

        idrref_fields2: List[str] = []
        for col in target_columns:
            col_name = col['name']
            col_type = col['data_type']
            col_max_length = col.get('max_length')
            if col_type in ['binary', 'varbinary'] and col_max_length == 16:
                if (
                    col_name == '_IDRRef'
                    or col_name == 'IDRRef'
                    or col_name.endswith('_IDRRef')
                    or col_name.endswith('IDRRef')
                ):
                    idrref_fields2.append(col_name)
        if idrref_fields2:
            return idrref_fields2[0]

        # Таблицы перечислений 1С в БД: ключ ссылочного соответствия — порядок в перечислении.
        if target_table.startswith('_Enum') and '_EnumOrder' in name_set:
            return '_EnumOrder'

        # Регистрация изменений / прочие служебные: ссылка не всегда заканчивается на IDRRef (_NodeTRef, *_RRef).
        for col in target_columns:
            col_name = col['name']
            col_type = col['data_type']
            col_max_length = col.get('max_length')
            if col_type in ['binary', 'varbinary'] and col_max_length == 16:
                if col_name == '_NodeTRef' or 'RRef' in col_name:
                    return col_name

        sample = sorted(name_set)
        if len(sample) > 48:
            sample = sample[:48] + ['...']
        raise ValueError(
            f"[ViewGenerator] Не удалось определить колонку ключа для JOIN с {target_table!r}. "
            f"Доступные колонки: {sample}"
        )

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
            self._select_field_rows.append({
                "expr": field_expr,
                "table_db": table_name,
                "col_tech": col_name,
                "output_alias": field_alias,
                "excl_key": f"__legacy__{table_name}",
            })

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
        # Получаем целевую таблицу из графа связей (List[str] — берём первую/главную)
        relationships = self.relationship_builder.get_related_tables(source_table)
        def _first_target(val):
            if isinstance(val, list) and val:
                return val[0]
            return val if isinstance(val, str) else None
        target_table = _first_target(relationships.get(field_name))
        if not target_table and field_name.endswith('RRef'):
            field_name_no_rref = field_name[:-4]
            target_table = _first_target(relationships.get(field_name_no_rref))
            if not target_table and field_name_no_rref.startswith('_'):
                target_table = _first_target(relationships.get(field_name_no_rref.lstrip('_')))
        if not target_table and not field_name.startswith('_'):
            target_table = _first_target(relationships.get('_' + field_name))
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
        
        pk_column = self._resolve_pk_column_for_join(target_table)
        
        # Определяем тип JOIN из конфигурации (по умолчанию INNER JOIN)
        join_type = "INNER JOIN"
        if self.table_config:
            config_item = self.table_config.get(relationship_key)
            if config_item and 'join_type' in config_item:
                _raw_jt = config_item['join_type']
                join_type = str(_raw_jt).upper()
                if join_type not in _ALLOWED_SQL_JOIN_TYPES:
                    logging.warning(
                        "[ViewGenerator] Неизвестный join_type %r для связи %s, подставляется INNER JOIN.",
                        _raw_jt,
                        relationship_key,
                    )
                    join_type = "INNER JOIN"
        
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

