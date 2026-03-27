# -*- coding: utf-8 -*-
"""
Сводка контекста в боковой панели Streamlit (БД, структура, таблица фактов, граф, конфигурация).
Без вывода пароля из строки подключения.
"""
from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import streamlit as st

ContextMode = Literal["home", "gen", "manual"]


def parse_connection_display(connection_string: Optional[str]) -> dict:
    """
    Извлекает из ODBC-подобной строки только Server, Database, UID (без PWD).
    """
    out = {"server": None, "database": None, "uid": None}
    if not connection_string or not isinstance(connection_string, str):
        return out
    for part in connection_string.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        key, _, val = part.partition("=")
        kl = key.strip().upper()
        v = val.strip()
        if kl == "SERVER":
            out["server"] = v
        elif kl == "DATABASE":
            out["database"] = v
        elif kl == "UID":
            out["uid"] = v
    return out


def _md_kv(label: str, value: Optional[str]) -> str:
    v = (value or "").strip() or "—"
    return f"**{label}:** {v}"


def _render_sql_copy_expander(sql: Optional[object], *, empty_hint_after_output: bool = False) -> None:
    """Показ SQL из сессии для выделения и копирования (Ctrl+C)."""
    if sql is None:
        text = ""
    elif isinstance(sql, str):
        text = sql.strip()
    else:
        text = str(sql).strip()
    if text:
        with st.expander("SQL для копирования", expanded=False):
            st.caption("Выделите текст в блоке ниже и скопируйте (Ctrl+C).")
            st.code(text, language="sql")
    elif empty_hint_after_output:
        st.caption("SQL в сессии нет — сгенерируйте запрос в соответствующей секции.")


def render_context_sidebar(mode: ContextMode) -> None:
    """Рендерит expander «Контекст» в sidebar под навигацией Streamlit."""
    st.sidebar.divider()
    with st.sidebar.expander("Контекст", expanded=False):
        if mode == "home":
            st.caption(
                "Подключитесь к БД и откройте **Генератор VIEW** или **Ручной режим** — "
                "здесь появится сводка текущих параметров."
            )
            return

        cs = st.session_state.get("connection_string")
        tested = bool(st.session_state.get("connection_tested"))
        conn = parse_connection_display(cs)
        if not tested or not cs:
            st.markdown(_md_kv("БД", "не подключена"))
        else:
            parts = []
            if conn.get("server"):
                parts.append(f"**Сервер:** `{conn['server']}`")
            if conn.get("database"):
                parts.append(f"**База:** `{conn['database']}`")
            if conn.get("uid"):
                parts.append(f"**Пользователь:** `{conn['uid']}`")
            st.markdown("\n".join(parts) if parts else "**БД:** подключение без деталей")

        if mode == "gen":
            _render_gen_details(conn)
        elif mode == "manual":
            _render_manual_details()


def _render_gen_details(conn: dict) -> None:
    """Детали для пошагового генератора (ключи gen_*)."""
    spath = st.session_state.get("gen_structure_file_path")
    if spath:
        st.markdown(_md_kv("Структура", str(Path(spath).name)))
        st.caption(spath)
    else:
        st.markdown(_md_kv("Структура", "— (ещё не зафиксирована на этом прогоне)"))

    ft = st.session_state.get("gen_fact_table_db")
    if ft:
        sp = st.session_state.get("gen_structure_parser")
        human = sp.get_table_human_name(ft) if sp else None
        if human:
            st.markdown(_md_kv("Таблица фактов", f"{human} (`{ft}`)"))
        else:
            st.markdown(_md_kv("Таблица фактов", f"`{ft}`"))
    else:
        st.markdown(_md_kv("Таблица фактов", "—"))

    built = bool(st.session_state.get("gen_graph_built"))
    rels = st.session_state.get("gen_relationships_collected")
    n_rel = len(rels) if isinstance(rels, list) else 0
    gh = st.session_state.get("gen_graph_hash")
    ga = st.session_state.get("gen_graph_built_at")
    md = st.session_state.get("gen_graph_max_depth")
    mdu = st.session_state.get("gen_graph_max_depth_up")
    if md is None:
        md = st.session_state.get("gen_max_depth")
    if mdu is None:
        mdu = st.session_state.get("gen_max_depth_up")

    if built and gh:
        _gh_full = str(gh)
        gh_s = _gh_full[:12] + ("…" if len(_gh_full) > 12 else "")
        st.markdown(_md_kv("Граф", f"построен, hash `{gh_s}`, связей: {n_rel}"))
        if md is not None and mdu is not None:
            st.caption(f"Глубины (граф): ↓{md} ↑{mdu}")
        if ga:
            st.caption(f"Собран: {ga}")
    elif gh and not built:
        _gh_full = str(gh)
        _gh_d = _gh_full[:12] + ("…" if len(_gh_full) > 12 else "")
        st.markdown(_md_kv("Граф", f"hash в ui_state `{_gh_d}`, граф не загружен в сессии"))
    else:
        st.markdown(_md_kv("Граф", "не построен"))
        if md is not None and mdu is not None:
            st.caption(f"Параметры (форма): ↓{md} ↑{mdu}")

    cname = (st.session_state.get("_cfg_save_name") or "").strip()
    cvn = (st.session_state.get("_cfg_save_view_name") or "").strip()
    lf = st.session_state.get("gen_loaded_cfg_filename")
    lfp = st.session_state.get("gen_loaded_cfg_filepath")
    if lf or lfp:
        st.markdown(_md_kv("Конфигурация (файл)", str(lf or Path(str(lfp)).name)))
        if lfp:
            st.caption(str(lfp))
        if cname or cvn:
            st.caption(f"Черновик: {cname or '—'} / VIEW: `{cvn or '—'}`")
    elif cname or cvn:
        st.markdown(_md_kv("Конфигурация", f"{cname or '—'} / VIEW `{cvn or '—'}`"))
        st.caption("Черновик (с диска не загружали)")
    else:
        st.markdown(_md_kv("Конфигурация", "черновик (имя не задано)"))

    assess = st.session_state.get("gen_assessment")
    if assess is not None:
        sl = getattr(assess, "score_label", None) or getattr(assess, "score", None) or "?"
        st.caption(f"Оценка факта: **{sl}**")

    fmt = st.session_state.get("gen_output_fmt")
    # Последний реально записанный/загруженный путь; gen_output_file — значение поля (шаблон).
    ofile = st.session_state.get("gen_last_written_sql_path") or st.session_state.get("gen_output_file")
    if fmt or ofile:
        st.caption(f"Вывод: {fmt or '—'}" + (f" · файл `{ofile}`" if ofile else ""))
    _render_sql_copy_expander(
        st.session_state.get("gen_generated_sql"),
        empty_hint_after_output=bool(fmt or ofile),
    )


def _render_manual_details() -> None:
    """Детали для ручного режима (ключи без префикса gen_*)."""
    spath = st.session_state.get("manual_structure_file_path")
    if spath:
        st.markdown(_md_kv("Структура", str(Path(spath).name)))
        st.caption(spath)
    else:
        st.markdown(_md_kv("Структура", "—"))

    ft = st.session_state.get("fact_table_db")
    sp = st.session_state.get("structure_parser")
    if ft:
        human = sp.get_table_human_name(ft) if sp else None
        if human:
            st.markdown(_md_kv("Таблица фактов", f"{human} (`{ft}`)"))
        else:
            st.markdown(_md_kv("Таблица фактов", f"`{ft}`"))
    else:
        st.markdown(_md_kv("Таблица фактов", "—"))

    built = bool(st.session_state.get("graph_built"))
    rels = st.session_state.get("relationships_collected")
    n_rel = len(rels) if isinstance(rels, list) else 0
    if built:
        st.markdown(_md_kv("Граф", f"построен, связей: {n_rel}"))
    else:
        st.markdown(_md_kv("Граф", "не построен"))

    st.markdown(_md_kv("Конфигурация", "— (мастер cfg_*.json только в генераторе)"))

    _render_sql_copy_expander(st.session_state.get("generated_sql"), empty_hint_after_output=False)