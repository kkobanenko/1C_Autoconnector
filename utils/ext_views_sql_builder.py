# -*- coding: utf-8 -*-
"""
Генерация SQL для схемы ext: CREATE SCHEMA, CREATE OR ALTER VIEW, extended properties.

Используется мастером генератора: человекочитаемые имена из StructureParser,
связи ref16 — из индекса связей StructureAnalyzer (если построен).

В JSON полей/таблицы (ключи rt/rf — см. глоссарий в шапке скрипта) — списки объектов со ссылками.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# Маркер последней фазы в on_table_progress: склейка extended properties после цикла по таблицам.
EXT_PROGRESS_EP_PHASE = "__ep__"

# Полное имя JSON-ключа -> короткий код (уменьшает размер extended properties).
# Значения (короткие коды) должны быть уникальны — проверка в _validate_json_key_glossary().
JSON_KEY_FULL_TO_SHORT: Dict[str, str] = {
    # Общие ключи meta field / table
    "tech_name": "tn",
    "human_name": "hn",
    "refs_to": "rt",
    "refs_from": "rf",
    "refs_from_note": "rfn",
    "tech_table": "tt",
    "human_table": "ht",
    "table_kind": "tk",
    # Элементы списков ссылок (одинаковая структура в rt и rf)
    "source_table_tech": "stt",
    "source_table_human": "sth",
    "source_field_tech": "sft",
    "source_field_human": "sfh",
    "target_table_tech": "ttt",
    "target_table_human": "tth",
}

# Разделители без пробелов — меньше байт в каждом литерале N'...'
_JSON_COMPACT_SEP = (",", ":")


def _validate_json_key_glossary() -> None:
    """Падает при дублирующихся коротких кодах или пустых значениях."""
    short_to_full: Dict[str, str] = {}
    for full, short in JSON_KEY_FULL_TO_SHORT.items():
        if not short:
            raise ValueError(f"Пустое сокращение для ключа {full!r}")
        if short in short_to_full:
            raise ValueError(
                f"Коллизия коротких JSON-ключей: {short!r} используется и для "
                f"{short_to_full[short]!r}, и для {full!r}"
            )
        short_to_full[short] = full


def _sql_lines_json_glossary() -> List[str]:
    """Строки комментариев T-SQL: глоссарий полное_имя -> код (стабильный порядок)."""
    out: List[str] = [
        "-- Глоссарий сокращённых ключей JSON (ext.table_meta / ext.field_meta):",
    ]
    for full in sorted(JSON_KEY_FULL_TO_SHORT.keys()):
        out.append(f"--   {full} -> {JSON_KEY_FULL_TO_SHORT[full]}")
    out.append("-- Формат: компактный JSON; схема ключей: short-keys-v1.")
    return out


def _shorten_ext_meta_payload(obj: Any) -> Any:
    """
    Рекурсивно заменяет длинные ключи на короткие по JSON_KEY_FULL_TO_SHORT.
    Неизвестный ключ — ошибка, чтобы не молча терять данные.
    """
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if k not in JSON_KEY_FULL_TO_SHORT:
                raise ValueError(f"Неизвестный ключ ext-meta JSON: {k!r}")
            sk = JSON_KEY_FULL_TO_SHORT[k]
            out[sk] = _shorten_ext_meta_payload(v)
        return out
    if isinstance(obj, list):
        return [_shorten_ext_meta_payload(x) for x in obj]
    return obj


def _dump_ext_meta_json(payload_long_keys: Dict[str, Any]) -> str:
    """Сериализация meta: короткие ключи + компактные разделители."""
    compact = _shorten_ext_meta_payload(payload_long_keys)
    return json.dumps(compact, ensure_ascii=False, separators=_JSON_COMPACT_SEP)


_validate_json_key_glossary()

def classify_ext_table_type(table_name: str) -> Optional[str]:
    """
    Возвращает класс таблицы для набора ext или None, если таблица не входит в набор.
    VT — табличная часть (имя содержит _VT).
    """
    clean = table_name.lstrip("_").strip()
    if "_VT" in clean or "_vt" in clean.upper():
        return "VT"
    if clean.startswith("Document"):
        return "Document"
    if clean.startswith("Reference"):
        return "Reference"
    if clean.startswith("Enum"):
        return "Enum"
    return None


def _bracket_ident(name: str) -> str:
    """Идентификатор T-SQL в квадратных скобках; ] экранируется как ]]."""
    if name is None:
        return "[]"
    escaped = name.replace("]", "]]")
    return f"[{escaped}]"


def _sql_string_literal(s: str) -> str:
    """Строковый литерал N'...' для Unicode; кавычки удваиваются."""
    if s is None:
        return "N''"
    return "N'" + s.replace("'", "''") + "'"


def _safe_table_human(structure_parser: Any, table_name: str) -> str:
    """Человекочитаемое имя таблицы из парсера или пустая строка."""
    try:
        h = structure_parser.get_table_human_name(table_name)
        return (h or "").strip()
    except Exception:
        return ""


def _safe_field_human(structure_parser: Any, table_name: str, field_name: str) -> str:
    """Человекочитаемое имя поля из парсера или пустая строка."""
    try:
        h = structure_parser.get_field_human_name(table_name, field_name)
        return (h or "").strip()
    except Exception:
        return ""


def build_reverse_reference_index(
    rel_index: Optional[Dict[str, Dict[str, List[str]]]],
) -> Dict[str, List[Tuple[str, str]]]:
    """
    Обратный индекс: целевая таблица (нормализованное имя) ->
    список пар (исходная_таблица, поле), из которых есть ссылки на цель.
    """
    out: Dict[str, List[Tuple[str, str]]] = {}
    if not rel_index:
        return out
    for src_tbl, fields in rel_index.items():
        for fld, targets in (fields or {}).items():
            for tgt in targets or []:
                if not tgt:
                    continue
                out.setdefault(tgt, []).append((src_tbl, fld))
    for k in list(out.keys()):
        # Уникальные пары, стабильный порядок
        seen: Set[Tuple[str, str]] = set()
        uniq: List[Tuple[str, str]] = []
        for pair in out[k]:
            if pair not in seen:
                seen.add(pair)
                uniq.append(pair)
        out[k] = sorted(uniq, key=lambda x: (x[0], x[1]))
    return out


def _field_meta_json(
    tech_field: str,
    human_field: Optional[str],
    refs_to: List[Dict[str, str]],
    refs_from: List[Dict[str, str]],
    refs_from_note: str = "",
) -> str:
    payload: Dict[str, Any] = {
        "tech_name": tech_field,
        "human_name": human_field or "",
        "refs_to": refs_to,
        "refs_from": refs_from,
    }
    if refs_from_note:
        payload["refs_from_note"] = refs_from_note
    return _dump_ext_meta_json(payload)


def _table_meta_json(
    tech_table: str,
    human_table: Optional[str],
    table_kind: str,
    refs_from_all: List[Dict[str, str]],
    refs_from_note: str = "",
) -> str:
    payload: Dict[str, Any] = {
        "tech_table": tech_table,
        "human_table": human_table or "",
        "table_kind": table_kind,
        "refs_from": refs_from_all,
    }
    if refs_from_note:
        payload["refs_from_note"] = refs_from_note
    return _dump_ext_meta_json(payload)


def _truncate_ident(s: str, max_len: int = 110) -> str:
    """Ограничение длины части имени объекта (лимит идентификатора MS SQL 128)."""
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def qualify_table_for_from(analyzer: Any, normalized_table: str) -> str:
    """Три части [schema].[table] для FROM."""
    norm = analyzer._normalize_table_name(normalized_table)
    schema, table = analyzer._parse_table_name(norm)
    return f"{_bracket_ident(schema)}.{_bracket_ident(table)}"


def build_ext_views_sql(
    analyzer: Any,
    structure_parser: Any,
    all_table_names: List[str],
    database_name: str,
    *,
    on_table_progress: Optional[Callable[[int, int, str], None]] = None,
) -> str:
    """
    Собирает один скрипт T-SQL: схема ext, представления, extended properties.

    Args:
        analyzer: StructureAnalyzer с подключением; индекс связей может быть None.
        structure_parser: StructureParser с human-именами.
        all_table_names: Список имён таблиц (как в мастере — простые имена).
        database_name: Имя БД для комментария в шапке скрипта.
        on_table_progress: Опционально — отчёт о ходе генерации: (шаг, всего, метка).
            Шаг от 0 до всего по таблицам; метка — имя таблицы или пустая строка на старте;
            в конце один вызов с меткой EXT_PROGRESS_EP_PHASE (склейка extended properties).

    Returns:
        Текст SQL (UTF-8).
    """
    rel_index = getattr(analyzer, "_relationship_index", None) or None
    rev = build_reverse_reference_index(rel_index)

    lines: List[str] = [
        f"-- Набор представлений схемы [ext] для БД: {database_name}",
        "-- Сгенерировано мастером «Генератор VIEW». Проверьте права (CREATE SCHEMA, CREATE VIEW).",
        "-- Связи ref16: из индекса связей (секция 2). Если индекс не построен — списки rt/rf пустые.",
        "-- Пакеты и GO:",
        "--   Строка GO — не инструкция T-SQL; это разделитель пакетов для SSMS и sqlcmd.",
        "--   Через драйверы (ODBC, JDBC, pyodbc и т.п.) выполняйте фрагменты между GO отдельными execute.",
        "--   Если в сохранённом файле нет строк GO — пересоберите скрипт кнопкой «Построить набор представлений ext» и скачайте заново.",
        "",
    ]
    lines.extend(_sql_lines_json_glossary())
    lines.extend(
        [
            "",
            "SET NOCOUNT ON;",
            "",
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ext')",
            "    EXEC(N'CREATE SCHEMA ext');",
            "",
            "-- Конец пакета: пролог (SET NOCOUNT, при необходимости CREATE SCHEMA ext)",
            "GO",
            "",
        ]
    )

    # Сортировка таблиц по имени для стабильного diff
    candidates = []
    for raw in sorted(set(all_table_names)):
        kind = classify_ext_table_type(raw)
        if kind is None:
            continue
        if not analyzer.table_exists(raw):
            continue
        norm = analyzer._normalize_table_name(raw)
        candidates.append((norm, kind))

    total_candidates = len(candidates)
    if on_table_progress is not None:
        # Нулевой шаг: известно число таблиц в наборе, цикл ещё не начался.
        on_table_progress(0, total_candidates, "")

    ep_statements: List[str] = []

    def add_ep_object(
        schema: str,
        obj_type: str,
        obj_name: str,
        prop_name: str,
        prop_value: str,
    ) -> None:
        esc_schema = _sql_string_literal(schema)
        esc_obj = _sql_string_literal(obj_name)
        esc_prop = _sql_string_literal(prop_name)
        # Одна общая переменная @ep_value: сначала SET JSON, затем IF EXISTS → update / else add.
        ep_statements.append(f"SET @ep_value = {_sql_string_literal(prop_value)};")
        ep_statements.append(
            f"IF EXISTS (SELECT 1 FROM sys.extended_properties ep "
            f"INNER JOIN sys.objects o ON ep.major_id = o.object_id "
            f"INNER JOIN sys.schemas s ON o.schema_id = s.schema_id "
            f"WHERE s.name = {esc_schema} AND o.name = {esc_obj} "
            f"AND ep.minor_id = 0 AND ep.name = {esc_prop})\n"
            f"    EXEC sys.sp_updateextendedproperty "
            f"@name = {esc_prop}, @value = @ep_value, "
            f"@level0type = N'SCHEMA', @level0name = {esc_schema}, "
            f"@level1type = N'{obj_type}', @level1name = {esc_obj};\n"
            f"ELSE\n"
            f"    EXEC sys.sp_addextendedproperty "
            f"@name = {esc_prop}, @value = @ep_value, "
            f"@level0type = N'SCHEMA', @level0name = {esc_schema}, "
            f"@level1type = N'{obj_type}', @level1name = {esc_obj};"
        )

    def add_ep_column(
        schema: str,
        view_name: str,
        col_name: str,
        prop_name: str,
        prop_value: str,
    ) -> None:
        esc_schema = _sql_string_literal(schema)
        esc_view = _sql_string_literal(view_name)
        esc_col = _sql_string_literal(col_name)
        esc_prop = _sql_string_literal(prop_name)
        ep_statements.append(f"SET @ep_value = {_sql_string_literal(prop_value)};")
        ep_statements.append(
            f"IF EXISTS (SELECT 1 FROM sys.extended_properties ep "
            f"INNER JOIN sys.columns c ON ep.major_id = c.object_id AND ep.minor_id = c.column_id "
            f"INNER JOIN sys.objects o ON c.object_id = o.object_id "
            f"INNER JOIN sys.schemas s ON o.schema_id = s.schema_id "
            f"WHERE s.name = {esc_schema} AND o.name = {esc_view} AND c.name = {esc_col} "
            f"AND ep.name = {esc_prop})\n"
            f"    EXEC sys.sp_updateextendedproperty "
            f"@name = {esc_prop}, @value = @ep_value, "
            f"@level0type = N'SCHEMA', @level0name = {esc_schema}, "
            f"@level1type = N'VIEW', @level1name = {esc_view}, "
            f"@level2type = N'COLUMN', @level2name = {esc_col};\n"
            f"ELSE\n"
            f"    EXEC sys.sp_addextendedproperty "
            f"@name = {esc_prop}, @value = @ep_value, "
            f"@level0type = N'SCHEMA', @level0name = {esc_schema}, "
            f"@level1type = N'VIEW', @level1name = {esc_view}, "
            f"@level2type = N'COLUMN', @level2name = {esc_col};"
        )

    for step_idx, (norm_tbl, kind) in enumerate(candidates, start=1):
        human_tbl = None
        try:
            human_tbl = structure_parser.get_table_human_name(norm_tbl)
        except Exception:
            human_tbl = None

        cols = analyzer.get_table_columns(norm_tbl)
        if not cols:
            if on_table_progress is not None:
                on_table_progress(step_idx, total_candidates, norm_tbl)
            continue

        # Имя VIEW: тех + (human) — укоротить при необходимости
        base_view_name = norm_tbl
        if human_tbl:
            base_view_name = f"{norm_tbl} ({human_tbl})"
        view_name_short = _truncate_ident(base_view_name, 118)
        view_quoted = _bracket_ident(view_name_short)

        from_qualified = qualify_table_for_from(analyzer, norm_tbl)

        # Входящие ссылки на эту таблицу (по нормализованному имени цели)
        incoming_pairs = rev.get(norm_tbl, [])
        _max_in = 500
        refs_from_entries: List[Dict[str, str]] = []
        for src_tbl, fld in incoming_pairs[:_max_in]:
            refs_from_entries.append(
                {
                    "source_table_tech": src_tbl,
                    "source_table_human": _safe_table_human(structure_parser, src_tbl),
                    "source_field_tech": fld,
                    "source_field_human": _safe_field_human(structure_parser, src_tbl, fld),
                    "target_table_tech": norm_tbl,
                    "target_table_human": human_tbl or "",
                }
            )
        refs_from_note = ""
        if len(incoming_pairs) > _max_in:
            refs_from_note = f"Показано {_max_in} из {len(incoming_pairs)} входящих ссылок."

        select_parts: List[str] = []
        col_aliases_for_ep: List[Tuple[str, str]] = []

        fld_map = rel_index.get(norm_tbl, {}) if rel_index else {}

        for col in cols:
            cname = col.get("name") or ""
            if not cname:
                continue
            human_f = None
            try:
                human_f = structure_parser.get_field_human_name(norm_tbl, cname)
            except Exception:
                human_f = None

            if human_f:
                alias_raw = f"{cname} ({human_f})"
            else:
                alias_raw = cname
            alias_raw = _truncate_ident(alias_raw, 120)
            alias_br = _bracket_ident(alias_raw)

            col_src = f"{from_qualified}.{_bracket_ident(cname)}"
            select_parts.append(f"    {col_src} AS {alias_br}")

            # ext.field_meta и список rt — только для ref16 (binary/varbinary длины 16): меньше объёма SQL.
            dt = (col.get("data_type") or "").lower()
            ml = col.get("max_length")
            is_ref16 = dt in ("binary", "varbinary") and (ml == 16 or str(ml) == "16")
            if is_ref16:
                refs_to_entries: List[Dict[str, str]] = []
                if cname in fld_map:
                    for tgt in fld_map[cname]:
                        refs_to_entries.append(
                            {
                                "source_table_tech": norm_tbl,
                                "source_table_human": human_tbl or "",
                                "source_field_tech": cname,
                                "source_field_human": human_f or "",
                                "target_table_tech": tgt,
                                "target_table_human": _safe_table_human(structure_parser, tgt),
                            }
                        )
                meta = _field_meta_json(
                    cname,
                    human_f,
                    refs_to_entries,
                    refs_from_entries,
                    refs_from_note=refs_from_note,
                )
                col_aliases_for_ep.append((alias_raw, meta))

        if not select_parts:
            if on_table_progress is not None:
                on_table_progress(step_idx, total_candidates, norm_tbl)
            continue

        view_body = ",\n".join(select_parts)
        lines.append(f"CREATE OR ALTER VIEW ext.{view_quoted}")
        lines.append("AS")
        lines.append(f"SELECT\n{view_body}")
        lines.append(f"FROM {from_qualified};")
        lines.append(
            "-- Конец пакета: одно представление ext (CREATE OR ALTER VIEW …)"
        )
        lines.append("GO")
        lines.append("")

        tbl_meta = _table_meta_json(
            norm_tbl, human_tbl, kind, refs_from_entries, refs_from_note=refs_from_note
        )
        add_ep_object("ext", "VIEW", view_name_short, "ext.table_meta", tbl_meta)

        for alias_raw, meta_json in col_aliases_for_ep:
            add_ep_column("ext", view_name_short, alias_raw, "ext.field_meta", meta_json)

        if on_table_progress is not None:
            on_table_progress(step_idx, total_candidates, norm_tbl)

    if on_table_progress is not None and total_candidates > 0:
        on_table_progress(total_candidates, total_candidates, EXT_PROGRESS_EP_PHASE)

    lines.append(
        "-- Extended properties: ext.table_meta на каждое представление; "
        "ext.field_meta только для колонок ref16 (binary/varbinary(16))"
    )
    if ep_statements:
        lines.append("DECLARE @ep_value nvarchar(3750);")
        lines.append("")
    lines.extend(ep_statements)
    lines.append("")
    lines.append("-- Конец пакета: extended properties (sp_addextendedproperty / sp_updateextendedproperty)")
    lines.append("GO")
    lines.append("")
    lines.append("-- Конец скрипта ext")
    return "\n".join(lines)
