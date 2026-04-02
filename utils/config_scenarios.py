# -*- coding: utf-8 -*-
"""
Готовые сценарии первичного заполнения table_config / excluded_fields в мастере генератора.

Сценарии не трогают Streamlit — только чистые структуры данных. Применение и rerun — на странице.
"""

from typing import Any, Dict, List, Optional, Set, Tuple

# Дефолтный JOIN для связей, у которых раньше не было записи в table_config.
_DEFAULT_JOIN_TYPE = "INNER JOIN"


def _is_ref16_column(data_type: Optional[str], max_length: Optional[Any]) -> bool:
    """
    Поле считается «ссылкой» в смысле сценария: binary(16) или varbinary(16),
    как в StructureAnalyzer.get_binary16_fields и ref16 в UI.
    """
    dt = (data_type or "").lower()
    if dt not in ("binary", "varbinary"):
        return False
    try:
        ml = int(max_length) if max_length is not None else -1
    except (TypeError, ValueError):
        return False
    return ml == 16


def _excluded_names_for_table(analyzer: Any, table_name: str) -> Set[str]:
    """
    Множество имён полей, которые нужно исключить из SELECT:
    все binary(16)/varbinary(16) и все мусорные (is_junk_field).
    """
    cols = analyzer.get_table_columns(table_name)
    out: Set[str] = set()
    for col in cols:
        name = col["name"]
        if _is_ref16_column(col.get("data_type"), col.get("max_length")):
            out.add(name)
            continue
        if analyzer.is_junk_field(table_name, name):
            out.add(name)
    return out


def _edge_is_self_referential(rel: dict, analyzer: Any) -> bool:
    """
    Самоссылка: одна и та же таблица на обоих концах ребра после нормализации имён
    (как в StructureAnalyzer / RelationshipBuilder — иначе «сырые» строки могут различаться).
    """
    src = rel.get("source_table") or ""
    tgt = rel.get("target_table") or ""
    if not src or not tgt:
        return False
    norm = analyzer._normalize_table_name
    return norm(src) == norm(tgt)


def _apply_all_except_refs_impl(
    analyzer: Any,
    fact_table: str,
    relationships: List[dict],
    table_config: Optional[Dict[str, dict]],
    disable_self_referential_edges: bool,
) -> Tuple[Optional[Dict[str, dict]], Optional[Dict[str, Set[str]]], Optional[str]]:
    """
    Общая логика сценариев «кроме ссылок»: excluded как ref16 ∪ junk;
    для каждого ребра — join_type из старого конфига или INNER JOIN.

    Если disable_self_referential_edges=True, рёбра-самоссылки получают enabled=False
    и для стороны JOIN все поля попадают в excluded_fields (иначе шаг 10 снова
    помечает узел включённым по числу выбранных полей).
    """
    if getattr(analyzer, "_field_stats_cache", None) is None:
        return (
            None,
            None,
            "Нет статистики кардинальности полей (секция 3). "
            "Постройте или загрузите статистику — иначе мусорные поля не определены.",
        )
    if not fact_table or not relationships:
        return None, None, "Нет таблицы фактов или граф связей пуст."

    tc_old = table_config or {}
    new_tc: Dict[str, dict] = {}
    for rel in relationships:
        rk = rel.get("relationship_key")
        if not rk:
            continue
        prev = tc_old.get(rk, {})
        # По умолчанию все связи включаем; самоссылки отключаем только во втором сценарии.
        enabled = True
        if disable_self_referential_edges and _edge_is_self_referential(rel, analyzer):
            enabled = False
        new_tc[rk] = {
            "enabled": enabled,
            "join_type": str(prev.get("join_type") or _DEFAULT_JOIN_TYPE),
        }

    root_key = f"__root__{fact_table}"
    new_excl: Dict[str, Set[str]] = {root_key: _excluded_names_for_table(analyzer, fact_table)}

    for rel in relationships:
        rk = rel.get("relationship_key")
        if not rk:
            continue
        direction = rel.get("direction", "forward")
        if direction == "forward":
            show_table = rel["target_table"]
        else:
            show_table = rel["source_table"]
        # Для самоссылок во втором сценарии — исключаем все колонки show_table,
        # иначе UI считает узел включённым (enabled в table_config пересчитывается из полей).
        if disable_self_referential_edges and _edge_is_self_referential(rel, analyzer):
            cols = analyzer.get_table_columns(show_table)
            new_excl[rk] = {c["name"] for c in cols} if cols else set()
        else:
            new_excl[rk] = _excluded_names_for_table(analyzer, show_table)

    return new_tc, new_excl, None


def apply_scenario_all_except_refs(
    analyzer: Any,
    fact_table: str,
    relationships: List[dict],
    table_config: Optional[Dict[str, dict]] = None,
) -> Tuple[Optional[Dict[str, dict]], Optional[Dict[str, Set[str]]], Optional[str]]:
    """
    Сценарий «Все, кроме ссылок»:
    - все связи графа enabled=True, join_type из старого конфига или INNER JOIN;
    - для корня и каждой таблицы на рёбре: excluded_fields = ref16 ∪ junk.

    Возвращает (table_config, excluded_fields, сообщение_ошибки).
    Сообщение_ошибки не None — применять сценарий нельзя.
    """
    return _apply_all_except_refs_impl(
        analyzer, fact_table, relationships, table_config, False
    )


def apply_scenario_all_except_refs_no_self(
    analyzer: Any,
    fact_table: str,
    relationships: List[dict],
    table_config: Optional[Dict[str, dict]] = None,
) -> Tuple[Optional[Dict[str, dict]], Optional[Dict[str, Set[str]]], Optional[str]]:
    """
    Как «Все, кроме ссылок», но рёбра-самоссылки (та же таблица после нормализации имён)
    получают enabled=False и полное исключение полей на стороне JOIN.
    """
    return _apply_all_except_refs_impl(
        analyzer, fact_table, relationships, table_config, True
    )


def apply_scenario_by_id(
    scenario_id: str,
    analyzer: Any,
    fact_table: str,
    relationships: List[dict],
    table_config: Optional[Dict[str, dict]] = None,
) -> Tuple[Optional[Dict[str, dict]], Optional[Dict[str, Set[str]]], Optional[str]]:
    """Точка входа по id сценария (для selectbox на странице)."""
    if scenario_id == "all_except_refs":
        return apply_scenario_all_except_refs(
            analyzer, fact_table, relationships, table_config
        )
    if scenario_id == "all_except_refs_no_self":
        return apply_scenario_all_except_refs_no_self(
            analyzer, fact_table, relationships, table_config
        )
    return None, None, f"Неизвестный сценарий: {scenario_id!r}"


# Метаданные для UI (без лямбд — проще поддерживать).
GEN_CONFIG_SCENARIO_OPTIONS: List[Dict[str, str]] = [
    {
        "id": "all_except_refs",
        "title": "Все, кроме ссылок",
        "description": (
            "Включает все связи графа. В SELECT — все поля таблиц графа, кроме "
            "binary(16)/varbinary(16) и полей, помеченных как мусорные по статистике (секция 3). "
            "Дальше настройки можно изменить вручную."
        ),
    },
    {
        "id": "all_except_refs_no_self",
        "title": "Все кроме ссылок, таблицы без самоссылок",
        "description": (
            "Как «Все, кроме ссылок», но ребра к той же таблице (после нормализации имён) "
            "отключены: enabled=False и все поля стороны JOIN исключены, чтобы узел не "
            "остался «включённым» в интерфейсе шага 10. Остальные связи — как в базовом сценарии."
        ),
    },
]
