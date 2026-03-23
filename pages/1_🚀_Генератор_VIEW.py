#!/usr/bin/env python3
"""
Генератор VIEW — основная страница с пошаговым пайплайном.
Этапы: Выбор таблицы → Оценка → Граф связей → Фильтрация → Генерация SQL.
"""

import streamlit as st
from streamlit_scroll_to_top import scroll_to_here
import json
import re
import traceback
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from parsers.structure_parser import StructureParser
from db.structure_analyzer import StructureAnalyzer
from builders.relationship_builder import RelationshipBuilder
from generators.view_generator import ViewGenerator
from utils.db_connection import test_connection, get_connection_string_from_params
from analyzers.fact_table_assessor import FactTableAssessor
from analyzers.field_filter import FieldFilter
import config


def _parse_cfg_tags_csv(tags_str):
    """Теги из строки «a, b» в список lower (для сохранения и SQL-meta)."""
    if not tags_str:
        return []
    return [t.strip().lower() for t in tags_str.split(',') if t.strip()]


def _cfg_view_name_chars_valid(name):
    """VIEW name: только буквы (Unicode), цифры и подчёркивание."""
    if not name:
        return True
    return bool(re.match(r'^[\w]+$', name, re.UNICODE))


def _apply_loaded_cfg_metadata_to_widgets(full_data):
    """Заполняет виджеты секции 11 из metadata загруженного cfg JSON."""
    _lm = full_data.get('metadata', {}) or {}
    st.session_state['_cfg_save_name'] = _lm.get('name') or ''
    st.session_state['_cfg_save_view_name'] = _lm.get('view_name') or ''
    st.session_state['_cfg_save_description'] = _lm.get('description') or ''
    _tags = _lm.get('tags', [])
    st.session_state['_cfg_save_tags'] = ', '.join(_tags) if isinstance(_tags, list) else str(_tags or '')


def _normalize_table_name_for_cfg_cache(table_name: str) -> str:
    """
    Та же логика, что StructureAnalyzer._normalize_table_name — только str, без БД.
    Нужна внутри st.cache_data, куда нельзя передавать экземпляр анализатора.
    """
    if not table_name:
        return ''
    if '.' in table_name:
        parts = table_name.split('.')
        normalized_parts = []
        for i, part in enumerate(parts):
            part = part.strip('[]')
            if i == 0:
                if not part.startswith('_'):
                    normalized_parts.append('_' + part)
                else:
                    normalized_parts.append(part)
            else:
                part_clean = part.lstrip('_')
                if part_clean:
                    normalized_parts.append('_' + part_clean)
                else:
                    normalized_parts.append(part)
        result = '_'.join(normalized_parts)
        while '__' in result:
            result = result.replace('__', '_')
        return result
    table_name = table_name.strip('[]')
    if not table_name.startswith('_'):
        return '_' + table_name
    return table_name


@st.cache_data(ttl=5)
def _load_configs_for_graph(
    configs_dir_str: str,
    current_graph_hash: str | None,
    fact_table_norm: str,
    max_depth_val,
    max_depth_up_val,
):
    """
    Читает все cfg_*.json с диска и возвращает метаданные, подходящие под текущий граф/факт.
    Кэш 5 с + явный .clear() после сохранения/удаления.
    """
    out = []
    _dir = Path(configs_dir_str)
    if not _dir.exists():
        return out
    for fp in sorted(_dir.glob('cfg_*.json'), reverse=True):
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
            meta = data.get('metadata', {}) or {}
            cfg_fact = meta.get('fact_table', '')
            if not cfg_fact:
                continue
            cfg_graph_hash = meta.get('graph_hash')
            if current_graph_hash and cfg_graph_hash:
                if cfg_graph_hash != current_graph_hash:
                    continue
            else:
                cfg_norm = _normalize_table_name_for_cfg_cache(cfg_fact)
                if cfg_norm != fact_table_norm:
                    continue
                if meta.get('max_depth') != max_depth_val or meta.get('max_depth_up') != max_depth_up_val:
                    continue
            meta = dict(meta)
            meta['filename'] = fp.name
            meta['filepath'] = str(fp)
            out.append(meta)
        except Exception:
            continue
    return out


@st.cache_data(ttl=5)
def _load_all_config_and_sql_metadata(configs_dir_str: str, sql_dir_str: str):
    """
    Все cfg и sql метаданные с диска (без фильтра по графу) — для сквозного поиска.
    Фильтрация по строке запроса остаётся в UI.
    """
    cfgs = []
    sqls = []
    p_cfg = Path(configs_dir_str)
    p_sql = Path(sql_dir_str)
    if p_cfg.exists():
        for fp in sorted(p_cfg.glob('cfg_*.json'), reverse=True):
            try:
                with open(fp, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                meta = dict(data.get('metadata', {}) or {})
                meta['filepath'] = str(fp)
                meta['filename'] = fp.name
                cfgs.append(meta)
            except Exception:
                continue
    if p_sql.exists():
        for fp in sorted(p_sql.glob('sql_*.json'), reverse=True):
            try:
                with open(fp, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
                meta = dict(meta)
                meta['filepath'] = str(fp)
                meta['filename'] = fp.name
                sqls.append(meta)
            except Exception:
                continue
    return cfgs, sqls


def _invalidate_project_config_metadata_caches():
    """После записи/удаления cfg на диск — сброс кэшей списков и поиска."""
    _load_configs_for_graph.clear()
    _load_all_config_and_sql_metadata.clear()


def _render_cfg_sql_global_search(search_query: str):
    """Сквозной поиск по всем cfg_*.json и sql_*.json (без фильтра по графу)."""
    _CONFIGS_DIR_SEARCH = Path(config.DEFAULT_OUTPUT_DIR) / "configs"
    _SQL_DIR_SEARCH = Path(config.DEFAULT_OUTPUT_DIR) / "sql"
    _all_cfg, _all_sql = _load_all_config_and_sql_metadata(
        str(_CONFIGS_DIR_SEARCH.resolve()),
        str(_SQL_DIR_SEARCH.resolve()),
    )

    _search_cfg_results = []
    for meta in _all_cfg:
        _name = (meta.get('name') or '').lower()
        _desc = (meta.get('description') or '').lower()
        _tags = ' '.join(meta.get('tags', []) or []).lower()
        _human = (meta.get('human_name') or '').lower()
        _fact = (meta.get('fact_table') or '').lower()
        _vn = (meta.get('view_name') or '').lower()
        _haystack = f"{_name} {_desc} {_tags} {_human} {_fact} {_vn}"
        if search_query in _haystack:
            _search_cfg_results.append(meta)

    _search_sql_results = []
    for meta in _all_sql:
        _disp_name = (meta.get('name') or meta.get('human_name') or '').lower()
        _cfg_nm = (meta.get('config_name') or '').lower()
        _vn_sql = (meta.get('view_name') or '').lower()
        _desc = (meta.get('description') or '').lower()
        _tags = ' '.join(meta.get('tags', []) or []).lower()
        _fact = (meta.get('fact_table') or '').lower()
        _haystack = f"{_disp_name} {_cfg_nm} {_vn_sql} {_desc} {_tags} {_fact}"
        if search_query in _haystack:
            _search_sql_results.append(meta)

    st.caption(f"Результаты поиска «{search_query}»: {len(_search_cfg_results)} конфигураций, {len(_search_sql_results)} SQL")

    if _search_cfg_results:
        with st.expander(f"📂 Конфигурации ({len(_search_cfg_results)})", expanded=True):
            for j, scm in enumerate(_search_cfg_results):
                _s_name = scm.get('name') or scm.get('human_name') or scm.get('fact_table', '?')
                _s_tags = scm.get('tags', []) or []
                _s_tags_str = ' '.join(f'[{t}]' for t in _s_tags) if _s_tags else ''
                _s_saved = str(scm.get('saved_at', '?'))[:16].replace('T', ' ')
                _s_desc_short = (scm.get('description') or '')[:80]
                _s_label = f"{_s_name} | {_s_saved} {_s_tags_str}"
                if _s_desc_short:
                    _s_label += f" — {_s_desc_short}"

                scol_load, scol_del = st.columns([5, 1])
                with scol_load:
                    if st.button(f"📌 {_s_label}", key=f"search_cfg_load_{j}", use_container_width=True):
                        with open(scm['filepath'], 'r', encoding='utf-8') as f:
                            full_data = json.load(f)
                        td_list_load = full_data.get('metadata', {}).get('tables_detail', [])
                        _missing_path = [
                            t.get('table', '?') for t in td_list_load
                            if t.get('role') != 'root' and 'path_from_root' not in t
                        ]
                        if _missing_path:
                            st.error(
                                f"⚠️ Конфигурация устарела (нет path_from_root). "
                                f"Таблицы: {_missing_path[:5]}{'...' if len(_missing_path) > 5 else ''}"
                            )
                        else:
                            loaded_tc = full_data.get('table_config', {})
                            loaded_excl = full_data.get('excluded_fields', {})
                            restored_excl = {}
                            for k, v in loaded_excl.items():
                                restored_excl[k] = set(v) if isinstance(v, list) else v
                            st.session_state._pending_cfg_load = {
                                'table_config': loaded_tc,
                                'excluded_fields': restored_excl,
                            }
                            st.session_state.gen_table_config = loaded_tc
                            st.session_state.gen_excluded_fields = restored_excl
                            _apply_loaded_cfg_metadata_to_widgets(full_data)
                            st.success("✅ Конфигурация загружена из поиска")
                        st.rerun()
                with scol_del:
                    st.caption('')

    if _search_sql_results:
        with st.expander(f"📄 SQL-результаты ({len(_search_sql_results)})", expanded=True):
            for j, ssm in enumerate(_search_sql_results):
                _ss_name = ssm.get('human_name') or ssm.get('fact_table', '?')
                _ss_tags = ssm.get('tags', []) or []
                _ss_tags_str = ' '.join(f'[{t}]' for t in _ss_tags) if _ss_tags else ''
                _ss_saved = str(ssm.get('saved_at', '?'))[:16].replace('T', ' ')
                _ss_label = f"{_ss_name} | {ssm.get('sql_lines', '?')} строк | {_ss_saved} {_ss_tags_str}"
                _sql_file = ssm.get('sql_file', '')
                _sql_path = _SQL_DIR_SEARCH / _sql_file if _sql_file else None
                if _sql_path and _sql_path.exists():
                    _sql_content = _sql_path.read_text(encoding='utf-8')
                    st.download_button(
                        f"📥 {_ss_label}",
                        data=_sql_content.encode('utf-8'),
                        file_name=_sql_file or 'export.sql',
                        mime='text/sql',
                        key=f"search_sql_dl_{j}",
                    )
                else:
                    st.caption(f"📄 {_ss_label} (файл не найден)")

    if not _search_cfg_results and not _search_sql_results:
        st.info(f"Ничего не найдено по запросу «{search_query}»")


# ─── Персистентность состояния ─────────────────────────────────────────────
_UI_STATE_FILE = Path(config.DEFAULT_OUTPUT_DIR) / "ui_state.json"
_LAST_SESSION_FILE = Path(config.DEFAULT_OUTPUT_DIR) / "last_session.json"

def _load_ui_state() -> dict:
    """Загружает сохранённое состояние UI с диска."""
    try:
        if _UI_STATE_FILE.exists():
            with open(_UI_STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save_ui_state(state: dict):
    """Сохраняет состояние UI на диск."""
    try:
        _UI_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        existing = _load_ui_state()
        existing.update(state)
        with open(_UI_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _load_last_session() -> dict | None:
    """Загружает last_session.json. Возвращает None если файла нет или не подходит."""
    try:
        if not _LAST_SESSION_FILE.exists():
            return None
        with open(_LAST_SESSION_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

def _save_last_session(fact_table: str, graph_hash: str, table_config: dict, excluded_fields: dict):
    """Сохраняет table_config и excluded_fields в last_session.json."""
    try:
        _LAST_SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
        excl_serializable = {}
        for k, v in excluded_fields.items():
            excl_serializable[k] = list(v) if isinstance(v, set) else v
        # Количество колонок по rk (для быстрого bulk-loop без вызова get_columns на каждую связь)
        _nt = st.session_state.get('gen_rel_n_total', {})
        data = {
            'fact_table': fact_table,
            'graph_hash': graph_hash,
            'table_config': table_config,
            'excluded_fields': excl_serializable,
            'rel_n_total': dict(_nt),
        }
        with open(_LAST_SESSION_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _reset_graph_state():
    """Сбрасывает граф и связанные настройки при смене таблицы."""
    st.session_state.gen_relationships_collected = None
    st.session_state.gen_graph_built = False
    st.session_state.gen_graph_hash = None
    st.session_state.gen_graph_built_at = None
    st.session_state.gen_graph_max_depth = None
    st.session_state.gen_graph_max_depth_up = None
    st.session_state.gen_table_config = {}
    st.session_state.gen_excluded_fields = {}
    st.session_state.gen_rel_n_total = {}
    st.session_state.gen_generated_sql = None
    _save_ui_state({'gen_graph_hash': None})

_saved_ui = _load_ui_state()


# ─── Инициализация session state ──────────────────────────────────────────
for key, default in {
    'connection_string': None,
    'connection_tested': False,
    'gen_analyzer': None,
    'gen_structure_parser': None,
    'gen_relationship_builder': None,
    'gen_relationships_collected': None,
    'gen_table_config': {},
    'gen_excluded_fields': {},
    'gen_rel_n_total': {},  # relationship_key / __root__ → число колонок таблицы (для O(1) n_included)
    'gen_graph_built': False,
    'gen_generated_sql': None,
    'gen_fact_table_db': _saved_ui.get('gen_fact_table_db'),
    'gen_assessment': None,
    'gen_graph_hash': _saved_ui.get('gen_graph_hash'),
    'gen_graph_built_at': None,
    'gen_graph_max_depth': None,
    'gen_graph_max_depth_up': None,
    '_last_cred_key': None,
    'gen_show_junk_fields': False,  # Показывать мусорные поля в настройке таблиц (по умолчанию скрыты)
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# Восстановление output format и file из ui_state (до создания виджетов)
_fmt_map = {'view': 'CREATE VIEW', 'select': 'SELECT', 'both': 'Оба'}
if 'gen_output_fmt' not in st.session_state and _saved_ui.get('gen_output_format'):
    st.session_state.gen_output_fmt = _fmt_map.get(_saved_ui.get('gen_output_format'), 'CREATE VIEW')
if 'gen_output_file' not in st.session_state and _saved_ui.get('gen_output_file'):
    st.session_state.gen_output_file = _saved_ui.get('gen_output_file')


st.title("🚀 Генератор VIEW")
st.caption("Пошаговый мастер создания SQL представлений из БД 1С")
st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# СЕКЦИЯ 1: ПОДКЛЮЧЕНИЕ К БД
# ═══════════════════════════════════════════════════════════════════════════
st.header("1. 🔌 Подключение к базе данных")

_env_host = config.MSSQL_HOST
_env_db = config.MSSQL_DATABASE
_env_user = config.MSSQL_USERNAME
_env_pass = config.MSSQL_PASSWORD
_env_configured = (
    _env_host != "localhost"
    and _env_db != "database_name"
    and _env_user != "username"
    and _env_pass != "password"
)

if _env_configured:
    cred_source = st.radio(
        "Источник credentials:",
        ["🔒 Из переменных окружения (.env)", "✏️ Ввести вручную"],
        horizontal=True,
        key="gen_cred_source"
    )
else:
    cred_source = "✏️ Ввести вручную"
    st.info("ℹ️ Credentials не найдены в переменных окружения.")

if cred_source == "🔒 Из переменных окружения (.env)":
    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Host", value=_env_host, disabled=True, key="gen_env_host")
        st.text_input("Database", value=_env_db, disabled=True, key="gen_env_db")
    with col2:
        st.text_input("Username", value=_env_user, disabled=True, key="gen_env_user")
        st.text_input("Password", value="••••••••", disabled=True, key="gen_env_pass")
    db_host, db_database, db_username, db_password = _env_host, _env_db, _env_user, _env_pass
else:
    col1, col2 = st.columns(2)
    with col1:
        db_host = st.text_input("Host", value=_env_host if _env_configured else "", key="gen_man_host")
        db_database = st.text_input("Database", value=_env_db if _env_configured else "", key="gen_man_db")
    with col2:
        db_username = st.text_input("Username", value=_env_user if _env_configured else "", key="gen_man_user")
        db_password = st.text_input("Password", value=_env_pass if _env_configured else "", type="password", key="gen_man_pass")

# Автоматическая проверка подключения
_cred_key = f"{db_host}|{db_database}|{db_username}|{db_password}"
if db_host and db_database and db_username and db_password:
    if st.session_state.get('_last_cred_key') != _cred_key:
        connection_string = get_connection_string_from_params(
            db_host, db_database, db_username, db_password
        )
        with st.spinner("Проверка подключения..."):
            success, message = test_connection(connection_string)
        if success:
            st.success(f"✅ {message}")
            st.session_state.connection_string = connection_string
            st.session_state.connection_tested = True
        else:
            st.error(f"❌ {message}")
            st.session_state.connection_tested = False
        st.session_state['_last_cred_key'] = _cred_key
    else:
        if st.session_state.connection_tested:
            st.success("✅ Подключение активно")
        else:
            st.error("❌ Подключение не удалось")
else:
    st.warning("⚠️ Заполните все поля подключения.")
    st.session_state.connection_tested = False

if not st.session_state.connection_tested:
    st.stop()

# Создаём/переиспользуем analyzer (нужен для GUID-индекса и далее)
if st.session_state.gen_analyzer is None:
    st.session_state.gen_analyzer = StructureAnalyzer(st.session_state.connection_string)
analyzer = st.session_state.gen_analyzer

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# СЕКЦИЯ 1.5: GUID-ИНДЕКС
# ═══════════════════════════════════════════════════════════════════════════
st.header("2. 🗄️ GUID-индекс и индекс связей", help=(
    "**GUID-индекс** — карта соответствий GUID записей → таблицы.\n"
    "Позволяет определить, на какую таблицу ссылается поле binary(16) по значению GUID.\n\n"
    "**Индекс связей** — карта {таблица → {поле → целевая_таблица}} для ВСЕХ таблиц 1С.\n"
    "Строится на основе GUID-индекса. Используется для поиска обратных связей "
    "(какие таблицы ссылаются на выбранную).\n\n"
    "Оба индекса строятся один раз и сохраняются на диск для каждой БД."
))

# Проверяем состояние индекса
_guid_loaded = analyzer._guid_to_table_cache is not None
_guid_metadata = analyzer.get_guid_index_metadata()
_guid_on_disk = _guid_metadata is not None

if _guid_loaded:
    st.success(f"✅ GUID-индекс загружен в память ({len(analyzer._guid_to_table_cache):,} записей)")
elif _guid_on_disk:
    _guid_progress_bar = st.progress(0, text="Загрузка GUID-индекса с диска...")
    try:
        def _guid_load_cb(prog, txt):
            _guid_progress_bar.progress(prog, text=txt)
        loaded = analyzer.load_guid_index(progress_callback=_guid_load_cb)
        if loaded:
            analyzer._guid_to_table_cache = loaded
            _guid_progress_bar.progress(1.0, text="Готово!")
            st.success(f"✅ GUID-индекс загружен с диска ({len(loaded):,} записей)")
            _guid_loaded = True
        else:
            st.warning("⚠️ Файл индекса не соответствует текущему подключению или повреждён.")
    finally:
        _guid_progress_bar.empty()
else:
    st.info("ℹ️ GUID-индекс не найден для этой БД. Постройте для полной оценки и поиска связей.")

if _guid_metadata:
    with st.expander("📋 Информация о сохранённом индексе", expanded=False):
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.markdown(f"**Сервер:** `{_guid_metadata.get('host', '?')}`")
            st.markdown(f"**База данных:** `{_guid_metadata.get('database', '?')}`")
        with col_m2:
            built_at = _guid_metadata.get('built_at', '?')
            if built_at and built_at != '?':
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(built_at)
                    built_at_display = dt.strftime("%d.%m.%Y %H:%M:%S")
                except Exception:
                    built_at_display = built_at
            else:
                built_at_display = '?'
            st.markdown(f"**Построен:** `{built_at_display}`")
            st.markdown(f"**Записей:** `{_guid_metadata.get('count', '?'):,}`")

col_build, col_rebuild = st.columns(2)
with col_build:
    if not _guid_loaded and st.button("🔨 Построить GUID-индекс", type="primary", key="gen_build_guid"):
        _guid_build_ok = False
        _guid_progress = st.progress(0, text="Построение GUID-индекса...")
        try:
            def _guid_cb(cur, total, tbl):
                _guid_progress.progress(cur / total if total else 0, text=f"[{cur+1}/{total}] {tbl}")
            idx = analyzer.build_guid_index(force_rebuild=True, progress_callback=_guid_cb)
            _guid_progress.progress(1.0, text="Готово!")
            st.success(f"✅ Индекс построен и сохранён: {len(idx):,} записей")
            _guid_build_ok = True
        except Exception as e:
            st.error(f"❌ Ошибка: {e}")
        if _guid_build_ok:
            st.rerun()

with col_rebuild:
    if _guid_loaded and st.button("🔄 Перестроить индекс", key="gen_rebuild_guid"):
        _guid_rebuild_ok = False
        _guid_progress2 = st.progress(0, text="Перестроение GUID-индекса...")
        try:
            analyzer.clear_guid_index_cache()
            def _guid_cb2(cur, total, tbl):
                _guid_progress2.progress(cur / total if total else 0, text=f"[{cur+1}/{total}] {tbl}")
            idx = analyzer.build_guid_index(force_rebuild=True, progress_callback=_guid_cb2)
            _guid_progress2.progress(1.0, text="Готово!")
            st.success(f"✅ Индекс перестроен: {len(idx):,} записей")
            _guid_rebuild_ok = True
        except Exception as e:
            st.error(f"❌ Ошибка: {e}")
        if _guid_rebuild_ok:
            st.rerun()

# Визуализация GUID-индекса
if _guid_loaded:
    if st.button("📊 Визуализировать GUID-индекс", key="gen_viz_guid"):
        with st.spinner("Отрисовка диаграммы GUID-индекса..."):
            try:
                from utils.guid_index_visualizer import render_guid_index
                viz_dir = Path(config.DEFAULT_OUTPUT_DIR) / "visualizations"
                viz_path = viz_dir / f"guid_index_{db_database}.jpg"
                render_guid_index(
                    guid_index=analyzer._guid_to_table_cache,
                    output_path=str(viz_path),
                    title=f"GUID-индекс: {db_database}",
                    top_n=40,
                    structure_parser=sp if 'sp' in dir() else None,
                    metadata=_guid_metadata,
                    dpi=150
                )
                st.session_state['_guid_viz_path'] = str(viz_path)
                st.success(f"✅ Диаграмма сохранена: `{viz_path}`")
            except Exception as e:
                st.error(f"❌ Ошибка визуализации: {e}")

    # Показываем последнюю визуализацию
    _viz_path = st.session_state.get('_guid_viz_path')
    if _viz_path and Path(_viz_path).exists():
        st.image(_viz_path, use_column_width=True)

# --- Индекс связей ---
st.subheader("🔗 Индекс связей (relationship index)")
_rel_idx_loaded = analyzer._relationship_index is not None
_rel_idx_metadata = analyzer.get_relationship_index_metadata()
_rel_idx_on_disk = _rel_idx_metadata is not None

if _rel_idx_loaded:
    _ri_total = sum(len(v) for v in analyzer._relationship_index.values())
    st.success(f"✅ Индекс связей загружен: {len(analyzer._relationship_index):,} таблиц, {_ri_total:,} полей")
elif _rel_idx_on_disk:
    _ri = analyzer._load_relationship_index()
    if _ri is not None:
        analyzer._relationship_index = _ri
        _ri_total = sum(len(v) for v in _ri.values())
        st.success(f"✅ Индекс связей загружен с диска: {len(_ri):,} таблиц, {_ri_total:,} полей")
        _rel_idx_loaded = True
    else:
        st.warning("⚠️ Файл индекса связей не соответствует текущему подключению.")
else:
    if _guid_loaded:
        st.info("ℹ️ Индекс связей не найден. Постройте для полного поиска обратных связей.")
    else:
        st.info("ℹ️ Для построения индекса связей сначала постройте GUID-индекс.")

if _rel_idx_metadata:
    with st.expander("📋 Информация об индексе связей", expanded=False):
        col_ri1, col_ri2 = st.columns(2)
        with col_ri1:
            st.markdown(f"**Таблиц:** `{_rel_idx_metadata.get('tables', '?')}`")
            st.markdown(f"**Полей:** `{_rel_idx_metadata.get('fields', '?')}`")
        with col_ri2:
            _ri_built = _rel_idx_metadata.get('built_at', '?')
            if _ri_built and _ri_built != '?':
                try:
                    from datetime import datetime as _dt_ri
                    _ri_built_display = _dt_ri.fromisoformat(_ri_built).strftime("%d.%m.%Y %H:%M:%S")
                except Exception:
                    _ri_built_display = _ri_built
            else:
                _ri_built_display = '?'
            st.markdown(f"**Построен:** `{_ri_built_display}`")

col_build_ri, col_rebuild_ri = st.columns(2)
with col_build_ri:
    if not _rel_idx_loaded and _guid_loaded:
        if st.button("🔨 Построить индекс связей", type="primary", key="gen_build_rel_idx"):
            _ri_ok = False
            _ri_prog = st.progress(0, text="Построение индекса связей...")
            try:
                def _ri_cb(cur, total, tbl):
                    _ri_prog.progress(cur / total if total else 0, text=f"[{cur+1}/{total}] {tbl}")
                ri = analyzer.build_relationship_index(
                    guid_index=analyzer._guid_to_table_cache,
                    force_rebuild=True,
                    progress_callback=_ri_cb
                )
                _ri_prog.progress(1.0, text="Готово!")
                _ri_total = sum(len(v) for v in ri.values())
                st.success(f"✅ Индекс связей построен: {len(ri):,} таблиц, {_ri_total:,} полей")
                _ri_ok = True
            except Exception as e:
                st.error(f"❌ Ошибка: {e}")
            if _ri_ok:
                st.rerun()

with col_rebuild_ri:
    if _rel_idx_loaded and _guid_loaded:
        if st.button("🔄 Перестроить индекс связей", key="gen_rebuild_rel_idx"):
            _ri_ok2 = False
            _ri_prog2 = st.progress(0, text="Перестроение индекса связей...")
            try:
                def _ri_cb2(cur, total, tbl):
                    _ri_prog2.progress(cur / total if total else 0, text=f"[{cur+1}/{total}] {tbl}")
                ri = analyzer.build_relationship_index(
                    guid_index=analyzer._guid_to_table_cache,
                    force_rebuild=True,
                    progress_callback=_ri_cb2
                )
                _ri_prog2.progress(1.0, text="Готово!")
                _ri_total = sum(len(v) for v in ri.values())
                st.success(f"✅ Индекс связей перестроен: {len(ri):,} таблиц, {_ri_total:,} полей")
                _ri_ok2 = True
            except Exception as e:
                st.error(f"❌ Ошибка: {e}")
            if _ri_ok2:
                st.rerun()

# Висячие ключи (unresolved) — поля с данными, но без найденной цели в guid_index
_unresolved = getattr(analyzer, '_unresolved_fields', None) or {}
if _unresolved:
    _ur_total = sum(len(v) for v in _unresolved.values())
    with st.expander(f"⚠️ Висячие ключи: {_ur_total} полей в {len(_unresolved)} таблицах", expanded=False):
        for tbl, fields in sorted(_unresolved.items()):
            st.text(f"  {tbl}: {', '.join(fields)}")

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════
# СЕКЦИЯ 1.5: СТАТИСТИКА КАРДИНАЛЬНОСТИ ПОЛЕЙ (Field Stats)
# ═══════════════════════════════════════════════════════════════════════════
st.header("3. 📊 Статистика кардинальности полей")

st.caption(
    "Анализ уникальных значений (COUNT DISTINCT) каждого поля каждой таблицы 1С. "
    "Поля с единственным значением (кардинальность ≤ порога) помечаются как «мусорные» "
    "и могут быть исключены из VIEW."
)

# Проверяем состояние
_fs_loaded = analyzer._field_stats_cache is not None
_fs_metadata = analyzer.get_field_stats_metadata()
_fs_on_disk = _fs_metadata is not None

if _fs_loaded:
    _total_f = sum(len(v) for v in analyzer._field_stats_cache.values())
    _junk_f = sum(1 for t in analyzer._field_stats_cache.values() for f in t.values() if f.get('is_junk'))
    st.success(f"✅ Статистика загружена: {_total_f:,} полей в {len(analyzer._field_stats_cache):,} таблицах, 🗑️ мусорных: {_junk_f:,} ({_junk_f*100//_total_f if _total_f else 0}%)")
elif _fs_on_disk:
    _fs_progress_bar = st.progress(0, text="Загрузка статистики полей с диска...")
    try:
        def _fs_load_cb(prog, txt):
            _fs_progress_bar.progress(prog, text=txt)
        _loaded_fs = analyzer.load_field_stats(progress_callback=_fs_load_cb)
        if _loaded_fs:
            analyzer._field_stats_cache = _loaded_fs
            _total_f = sum(len(v) for v in _loaded_fs.values())
            _junk_f = sum(1 for t in _loaded_fs.values() for f in t.values() if f.get('is_junk'))
            _fs_progress_bar.progress(1.0, text="Готово!")
            st.success(f"✅ Статистика загружена с диска: {_total_f:,} полей, 🗑️ мусорных: {_junk_f:,}")
            _fs_loaded = True
        else:
            st.warning("⚠️ Файл статистики повреждён или не соответствует подключению.")
    finally:
        _fs_progress_bar.empty()
else:
    st.info("ℹ️ Статистика полей не найдена. Постройте для выявления мусорных полей.")

if _fs_metadata:
    with st.expander("📋 Информация о сохранённой статистике", expanded=False):
        col_fs1, col_fs2 = st.columns(2)
        with col_fs1:
            st.markdown(f"**Сервер:** `{_fs_metadata.get('host', '?')}`")
            st.markdown(f"**База данных:** `{_fs_metadata.get('database', '?')}`")
            st.markdown(f"**Выборка:** `TOP {_fs_metadata.get('sample_size', '?')}`")
        with col_fs2:
            _fs_built = _fs_metadata.get('built_at', '?')
            if _fs_built and _fs_built != '?':
                try:
                    from datetime import datetime as _dt_fs
                    _fs_built_display = _dt_fs.fromisoformat(_fs_built).strftime("%d.%m.%Y %H:%M:%S")
                except Exception:
                    _fs_built_display = _fs_built
            else:
                _fs_built_display = '?'
            st.markdown(f"**Построена:** `{_fs_built_display}`")
            st.markdown(f"**Таблиц:** `{_fs_metadata.get('table_count', '?'):,}`")
            st.markdown(f"**Полей:** `{_fs_metadata.get('total_fields', '?'):,}` (🗑️ {_fs_metadata.get('junk_fields', '?'):,})")
            st.markdown(f"**Порог мусорности:** ≤ `{_fs_metadata.get('junk_threshold', 1)}`")

col_fs_build, col_fs_rebuild = st.columns(2)
with col_fs_build:
    if not _fs_loaded and st.button("🔨 Построить статистику полей", type="primary", key="gen_build_fs"):
        _fs_progress = st.progress(0, text="Анализ полей...")
        _fs_build_ok = False
        try:
            def _fs_cb(cur, total, tbl):
                _fs_progress.progress(cur / total if total else 0, text=f"[{cur+1}/{total}] {tbl}")
            _fs_result = analyzer.build_field_stats(force_rebuild=True, progress_callback=_fs_cb)
            _fs_progress.progress(1.0, text="Готово!")
            st.success(f"✅ Статистика построена и сохранена: {sum(len(v) for v in _fs_result.values()):,} полей")
            _fs_build_ok = True
        except Exception as e:
            st.error(f"❌ Ошибка: {e}")
        if _fs_build_ok:
            st.rerun()

with col_fs_rebuild:
    if _fs_loaded and st.button("🔄 Перестроить статистику", key="gen_rebuild_fs"):
        _fs_progress2 = st.progress(0, text="Перестроение...")
        _fs_rebuild_ok = False
        try:
            analyzer.clear_field_stats_cache()
            def _fs_cb2(cur, total, tbl):
                _fs_progress2.progress(cur / total if total else 0, text=f"[{cur+1}/{total}] {tbl}")
            _fs_result2 = analyzer.build_field_stats(force_rebuild=True, progress_callback=_fs_cb2)
            _fs_progress2.progress(1.0, text="Готово!")
            st.success(f"✅ Статистика перестроена: {sum(len(v) for v in _fs_result2.values()):,} полей")
            _fs_rebuild_ok = True
        except Exception as e:
            st.error(f"❌ Ошибка: {e}")
        if _fs_rebuild_ok:
            st.rerun()

# Визуализация статистики полей
if _fs_loaded and analyzer._field_stats_cache:
    with st.expander("📊 Статистика по таблицам", expanded=False):
        _fs_cache = analyzer._field_stats_cache
        _fs_sp = st.session_state.get('gen_structure_parser')
        
        _fs_table_options = []
        for t in sorted(_fs_cache.keys()):
            human = _fs_sp.get_table_human_name(t) if _fs_sp else None
            label = f"{human} ({t})" if human else t
            _fs_table_options.append((label, t))

        _fs_filter = st.text_input("🔍 Фильтр по имени таблицы:", key="gen_fs_filter", placeholder="Введите часть имени или техназвания...")
        if _fs_filter:
            _flt = _fs_filter.lower()
            _fs_table_options = [opt for opt in _fs_table_options if _flt in opt[0].lower()]

        if _fs_table_options:
            _fs_labels = [opt[0] for opt in _fs_table_options]
            _fs_selected_label = st.selectbox(
                "Выберите таблицу:",
                _fs_labels,
                key="gen_fs_table_select"
            )
            
            _fs_selected = next((t for lbl, t in _fs_table_options if lbl == _fs_selected_label), None)
            
            if _fs_selected and _fs_selected in _fs_cache:
                _tbl_stats = _fs_cache[_fs_selected]
                _tbl_total = len(_tbl_stats)
                _tbl_junk = sum(1 for f in _tbl_stats.values() if f.get('is_junk'))
                st.markdown(f"**{_fs_selected}**: {_tbl_total} полей, 🗑️ мусорных: {_tbl_junk}")

                # Строим таблицу
                _rel_idx = getattr(analyzer, '_relationship_index', None) or {}
                _table_rels = _rel_idx.get(_fs_selected, {}) or _rel_idx.get(
                    analyzer._normalize_table_name(_fs_selected) if hasattr(analyzer, '_normalize_table_name') else _fs_selected, {}
                )
                _rows = []
                for fname, finfo in sorted(_tbl_stats.items(), key=lambda x: x[1].get('distinct_count', 0)):
                    dc = finfo.get('distinct_count', -1)
                    is_junk = finfo.get('is_junk', False)
                    dt = finfo.get('data_type', '?')
                    ml = finfo.get('max_length', '')
                    marker = "🗑️" if is_junk else "✅"
                    
                    human_fname = _fs_sp.get_field_human_name(_fs_selected, fname) if _fs_sp else None
                    fname_display = f"{human_fname} ({fname})" if human_fname else fname
                    
                    # Связанная таблица: только для binary(16), из relationship_index (List[str])
                    _is_binary16 = dt and 'binary' in str(dt).lower() and (ml == 16 or str(ml) == '16')
                    _targets = _table_rels.get(fname, []) if _is_binary16 else []
                    if isinstance(_targets, str):
                        _targets = [_targets]
                    if _targets:
                        _linked = ", ".join(_targets)
                    elif _is_binary16:
                        _ur = getattr(analyzer, '_unresolved_fields', None) or {}
                        _is_hanging = fname in (_ur.get(_fs_selected, []))
                        _linked = "🔑 висячий" if (_is_hanging and not is_junk) else "—"
                    else:
                        _linked = ""
                    
                    _rows.append({
                        "": marker,
                        "Поле": fname_display,
                        "Тип": f"{dt}({ml})" if ml else dt,
                        "DISTINCT": dc,
                        "Статус": "мусор" if is_junk else "ок",
                        "Связанная таблица": _linked
                    })
                st.dataframe(_rows, use_container_width=True, hide_index=True)
        else:
            st.info("Нет таблиц, соответствующих фильтру.")

st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════
# СЕКЦИЯ 2: ФАЙЛ СТРУКТУРЫ
# ═══════════════════════════════════════════════════════════════════════════
st.header("4. 📂 Файл структуры")

structure_file_option = st.radio(
    "Источник файла структуры:",
    ["По умолчанию", "Загрузить файл", "Указать путь"],
    horizontal=True,
    key="gen_struct_src"
)

structure_file_path = None
if structure_file_option == "По умолчанию":
    default_path = Path(config.DEFAULT_STRUCTURE_FILE)
    if default_path.exists():
        structure_file_path = str(default_path)
        st.success(f"✅ Файл: `{default_path.name}`")
    else:
        st.error(f"❌ Файл по умолчанию не найден: {default_path}")
elif structure_file_option == "Загрузить файл":
    uploaded = st.file_uploader("Загрузите файл структуры (.docx)", type=['docx'], key="gen_upload")
    if uploaded:
        import tempfile
        tmp = Path(tempfile.mkdtemp()) / uploaded.name
        tmp.write_bytes(uploaded.read())
        structure_file_path = str(tmp)
        st.success(f"✅ Загружен: {uploaded.name}")
else:
    custom_path = st.text_input("Путь к файлу:", key="gen_struct_path")
    if custom_path and Path(custom_path).exists():
        structure_file_path = custom_path
        st.success(f"✅ Файл: {Path(custom_path).name}")
    elif custom_path:
        st.error("❌ Файл не найден")

if not structure_file_path:
    st.stop()

# Парсим структуру (с кешированием)
@st.cache_resource
def _parse_structure(file_path):
    parser = StructureParser(file_path)
    parser.parse()
    return parser

try:
    sp = _parse_structure(structure_file_path)
    st.session_state.gen_structure_parser = sp
except Exception as e:
    st.error(f"❌ Ошибка парсинга: {e}")
    st.stop()

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# СЕКЦИЯ 3: ВЫБОР ТАБЛИЦЫ ФАКТОВ (Этап 0)
# ═══════════════════════════════════════════════════════════════════════════
st.header("5. 📋 Выбор таблицы фактов")

sp = st.session_state.gen_structure_parser

# Получаем все таблицы
@st.cache_data(ttl=600)
def _get_table_list(_connection_string):
    """Получает список таблиц из БД."""
    a = StructureAnalyzer(_connection_string)
    try:
        all_tables = a.get_all_tables()
        # Фильтруем: только простые имена без схемы
        simple_tables = sorted([
            t for t in all_tables
            if not t.startswith('[') and '.' not in t
        ])
        return simple_tables
    finally:
        a.close()

all_tables = _get_table_list(st.session_state.connection_string)

# Классификация таблиц
def _classify_table(name):
    clean = name.lstrip('_')
    if '_VT' in clean:
        return 'VT (табл. часть)'
    if clean.startswith('Document'):
        return 'Document'
    if clean.startswith('AccumRg') or clean.startswith('AccumReg'):
        return 'AccumRg'
    if clean.startswith('InfoRg') or clean.startswith('InfoReg'):
        return 'InfoRg'
    if clean.startswith('Reference'):
        return 'Reference'
    if clean.startswith('Enum'):
        return 'Enum'
    return 'Другое'

# Применяем отложенный сброс фильтров (от кнопки "быстрого выбора" в избранном).
# Делаем это ДО создания виджетов, иначе Streamlit запретит менять session state.
if '_pending_gen_reset' in st.session_state:
    _pr = st.session_state._pending_gen_reset
    _new_table = _pr.get('gen_fact_table_db')
    if _new_table != st.session_state.get('gen_fact_table_db'):
        _reset_graph_state()
    st.session_state.gen_type_filter = _pr.get('gen_type_filter', 'Все')
    st.session_state.gen_table_search = _pr.get('gen_table_search', '')
    st.session_state.gen_fact_table_db = _new_table
    del st.session_state._pending_gen_reset

# Фильтр по типу
_favorites = _saved_ui.get('favorites', {})
_type_options = ["Все", "⭐ Избранное", "Document", "AccumRg", "InfoRg", "Reference", "VT (табл. часть)", "Enum"]
_saved_type_filter = _saved_ui.get('gen_type_filter', 'Все')
_saved_type_idx = _type_options.index(_saved_type_filter) if _saved_type_filter in _type_options else 0

type_filter = st.radio(
    "Фильтр по типу:",
    _type_options,
    index=_saved_type_idx,
    horizontal=True,
    key="gen_type_filter"
)

# Поиск по строке
_saved_search = _saved_ui.get('gen_table_search', '')
search_query = st.text_input(
    "🔍 Поиск таблицы (по имени или техническому названию):",
    value=_saved_search,
    key="gen_table_search",
    placeholder="Введите часть названия, например: Реализация или Document653"
)

# Сохраняем фильтры на диск при изменении
_save_ui_state({'gen_type_filter': type_filter, 'gen_table_search': search_query})
table_options = []
search_lower = search_query.strip().lower()

if type_filter == "⭐ Избранное":
    # Показываем только избранные таблицы
    if not _favorites:
        st.info("ℹ️ Список избранного пуст. Добавьте таблицы через кнопку ⭐.")
        st.stop()
    for fav_table, fav_data in _favorites.items():
        fav_label = fav_data.get('label', fav_table)
        fav_comment = fav_data.get('comment', '')
        label = f"{fav_label} 💬 {fav_comment}" if fav_comment else fav_label
        # Фильтруем по строке поиска (если задана)
        if search_lower and search_lower not in label.lower():
            continue
        table_options.append((label, fav_table))
else:
    for t in all_tables:
        table_type = _classify_table(t)
        if type_filter != "Все" and table_type != type_filter:
            continue
        human = sp.get_table_human_name(t) if sp else None
        if human:
            label = f"{human} ({t})"
        else:
            label = t
        # Фильтруем по строке поиска
        if search_lower and search_lower not in label.lower():
            continue
        table_options.append((label, t))

if not table_options:
    st.warning("Ни одна таблица не найдена с текущим фильтром.")
    st.stop()

# Определяем начальный индекс по последнему выбору
_labels = [opt[0] for opt in table_options]
_default_idx = 0
_prev_table = st.session_state.get('gen_fact_table_db')
if _prev_table:
    for i, (lbl, db_n) in enumerate(table_options):
        if db_n == _prev_table:
            _default_idx = i
            break

selected_label = st.selectbox(
    f"Выберите таблицу фактов ({len(table_options)} найдено):",
    _labels,
    index=_default_idx,
    key="gen_table_select"
)

# Извлекаем техническое имя
selected_table_db = None
for label, db_name in table_options:
    if label == selected_label:
        selected_table_db = db_name
        break

if not selected_table_db:
    st.stop()

# Сброс графа при смене таблицы
if selected_table_db != st.session_state.get('gen_fact_table_db'):
    _reset_graph_state()

st.session_state.gen_fact_table_db = selected_table_db
_save_ui_state({'gen_fact_table_db': selected_table_db})

# ─── Избранные таблицы ────────────────────────────────────────────────────
_favorites = _saved_ui.get('favorites', {})  # {table_name: {"comment": str, "label": str}}

# Кнопка добавления в избранное
_is_fav = selected_table_db in _favorites
col_fav_add, col_fav_info = st.columns([1, 3])
with col_fav_add:
    if _is_fav:
        if st.button("💛 В избранном", key="gen_fav_already", disabled=True):
            pass
    else:
        if st.button("⭐ В избранное", key="gen_fav_add"):
            _favorites[selected_table_db] = {
                "comment": "",
                "label": selected_label
            }
            _save_ui_state({'favorites': _favorites})
            st.rerun()
with col_fav_info:
    if _is_fav:
        st.caption(f"Комментарий: {_favorites[selected_table_db].get('comment', '') or '—'}")

# Блок избранного
if _favorites:
    with st.expander(f"⭐ Избранные таблицы ({len(_favorites)})", expanded=False):
        _fav_changed = False
        _fav_to_remove = []

        for fav_table, fav_data in list(_favorites.items()):
            fav_label = fav_data.get('label', fav_table)
            fav_comment = fav_data.get('comment', '')

            col_sel, col_cmt, col_del = st.columns([3, 4, 1])
            with col_sel:
                # Кнопка быстрого выбора
                if st.button(f"📌 {fav_label}", key=f"fav_sel_{fav_table}", use_container_width=True):
                    # Нельзя менять gen_type_filter/gen_table_search после создания виджетов.
                    # Сохраняем отложенный сброс — применится в следующем прогоне до виджетов.
                    st.session_state._pending_gen_reset = {
                        'gen_type_filter': 'Все',
                        'gen_table_search': '',
                        'gen_fact_table_db': fav_table
                    }
                    _save_ui_state({
                        'gen_fact_table_db': fav_table,
                        'gen_type_filter': 'Все',
                        'gen_table_search': ''
                    })
                    st.rerun()
            with col_cmt:
                new_comment = st.text_input(
                    "Комментарий",
                    value=fav_comment,
                    key=f"fav_cmt_{fav_table}",
                    label_visibility="collapsed",
                    placeholder="Добавить комментарий..."
                )
                if new_comment != fav_comment:
                    _favorites[fav_table]['comment'] = new_comment
                    _fav_changed = True
            with col_del:
                if st.button("🗑️", key=f"fav_del_{fav_table}"):
                    _fav_to_remove.append(fav_table)

        # Применяем удаления
        for t in _fav_to_remove:
            del _favorites[t]
            _fav_changed = True

        if _fav_changed:
            _save_ui_state({'favorites': _favorites})
            if _fav_to_remove:
                st.rerun()

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# СЕКЦИЯ 4: ОЦЕНКА ТАБЛИЦЫ ФАКТОВ (Этап 2)
# ═══════════════════════════════════════════════════════════════════════════
st.header("6. ✅ Оценка таблицы фактов")

assessor = FactTableAssessor(analyzer, sp)
with st.spinner("Анализ таблицы..."):
    assessment = assessor.assess(selected_table_db)
st.session_state.gen_assessment = assessment

# Цветная карточка
if assessment.score == 'good':
    st.success(assessment.score_label)
elif assessment.score == 'maybe':
    st.warning(assessment.score_label)
else:
    st.error(assessment.score_label)

# Детали эвристик
with st.expander("📊 Детали оценки", expanded=(assessment.score != 'good')):
    for w in assessment.warnings:
        icon = "✅" if w.severity == 'positive' else "⚠️" if w.severity == 'neutral' else "❌"
        weight_str = f"+{w.weight}" if w.weight > 0 else str(w.weight)
        st.markdown(f"**{icon} [{w.heuristic_id}]** {w.message} *(вес: {weight_str})*")
    st.caption(f"Итоговый вес: **{assessment.total_weight}** (≥3 = хорошо, ≥0 = может подойти, <0 = не подходит)")

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# СЕКЦИЯ 5: ПАРАМЕТРЫ (Этапы 3-4)
# ═══════════════════════════════════════════════════════════════════════════
st.header("7. ⚙️ Параметры", help=(
    "Параметры построения графа связей и генерации SQL.\n\n"
    "• **Рекурсия вниз (↓)** — сколько уровней прямых ссылок (binary(16) → _IDRRef) "
    "обходить от корневой таблицы. Каждый уровень добавляет справочники, "
    "на которые ссылаются поля текущего уровня.\n\n"
    "• **Рекурсия вверх (↑)** — сколько уровней обратных связей включать. "
    "Обратная связь — это таблица, которая сама ссылается на текущую "
    "(например, табличная часть документа). 0 = только прямые связи.\n\n"
    "• **Исправлять даты 1С** — если включено, даты ≥ 3000 года сдвигаются "
    "на −2000 лет (стандартная коррекция 1С:Предприятие).\n\n"
    "• **Именование полей** — стиль алиасов колонок в SQL:\n"
    "  - *Таблица.Поле* — `Справочник.Контрагенты.Наименование`\n"
    "  - *Алиас_Поле* — `Справочник_Контрагенты_Наименование`"
))

# Загружаем сохранённые параметры
_def_depth = _saved_ui.get('param_max_depth', 5)
_def_depth_up = _saved_ui.get('param_max_depth_up', 1)
_def_fix_dates = _saved_ui.get('param_fix_dates', True)
_naming_options = ["Таблица.Поле", "Алиас_Поле"]
_def_naming_idx = _naming_options.index(_saved_ui.get('param_naming', 'Таблица.Поле')) if _saved_ui.get('param_naming') in _naming_options else 0

col1, col2, col3, col4 = st.columns(4)
with col1:
    max_depth = st.slider(
        "Максимальный уровень рекурсии вниз",
        0, 10, _def_depth,
        key="gen_max_depth",
        help=(
            "Глубина прямых связей: сколько уровней справочников обходить "
            "через binary(16) ссылки из таблицы фактов вниз по иерархии."
        )
    )
with col2:
    max_depth_up = st.slider(
        "Максимальный уровень рекурсии вверх",
        0, 10, _def_depth_up,
        key="gen_max_depth_up",
        help=(
            "Глубина обратных связей: сколько уровней таблиц, которые ссылаются "
            "на таблицу фактов снизу вверх, включать в запрос. "
            "0 = не включать обратные связи; "
            "1 = прямые родители (например, заголовки документов)."
        )
    )
with col3:
    fix_dates = st.checkbox("Исправлять даты 1С", value=_def_fix_dates, key="gen_fix_dates")
with col4:
    naming_style = st.radio(
        "Именование полей:",
        _naming_options,
        index=_def_naming_idx,
        horizontal=True,
        key="gen_naming"
    )
    naming_style_code = 'dotted' if naming_style == "Таблица.Поле" else 'classic'

# Сохраняем текущие параметры на диск для следующей сессии
_save_ui_state({
    'param_max_depth': max_depth,
    'param_max_depth_up': max_depth_up,
    'param_fix_dates': fix_dates,
    'param_naming': naming_style,
})

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════
# СЕКЦИЯ 6: ПОСТРОЕНИЕ ГРАФА СВЯЗЕЙ (Этап 3)
# ═══════════════════════════════════════════════════════════════════════════
st.header("8. 🔗 Граф связей", help=(
    "Граф связей — это карта всех таблиц, связанных с выбранной таблицей фактов "
    "через поля binary(16) (ссылки GUID в 1С).\n\n"
    "**Зачем нужен:**\n"
    "- Определяет, какие справочники и документы будут включены в итоговый VIEW\n"
    "- Позволяет увидеть структуру связей до генерации SQL\n"
    "- Даёт возможность включить/отключить отдельные связи\n\n"
    "**Как строится:**\n"
    "1. Рекурсия вниз — обходит ссылки из таблицы фактов на справочники\n"
    "2. Рекурсия вверх — находит таблицы, которые ссылаются НА таблицу фактов\n\n"
    "Построенный граф можно сохранить и использовать повторно."
))

# ─── Функции сохранения/загрузки графов ───────────────────────────────────
_GRAPHS_DIR = Path(config.DEFAULT_OUTPUT_DIR) / "graphs"


def _compute_graph_hash(table_name, max_depth, max_depth_up, structure_file_path, connection_string, analyzer_obj):
    """
    Вычисляет SHA256-хэш от параметров, определяющих граф.
    Входные данные:
      1. Корневая таблица
      2. max_depth, max_depth_up
      3. SHA256 содержимого файла структуры
      4. connection_string
      5. Отсортированный список таблиц БД
    """
    import hashlib
    h = hashlib.sha256()
    # 1-2. Параметры
    h.update(f"{table_name}|{max_depth}|{max_depth_up}".encode('utf-8'))
    # 3. Файл структуры
    try:
        sf = Path(structure_file_path)
        if sf.exists():
            h.update(sf.read_bytes())
    except Exception:
        h.update(b'__no_structure__')
    # 4. Connection string
    h.update((connection_string or '').encode('utf-8'))
    # 5. Список таблиц БД
    try:
        all_tables = sorted(analyzer_obj.get_all_tables())
        h.update('|'.join(all_tables).encode('utf-8'))
    except Exception:
        h.update(b'__no_tables__')
    return h.hexdigest()


def _save_graph(table_name, relationships, params, graph_hash=None):
    """Сохраняет граф на диск."""
    from datetime import datetime
    _GRAPHS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now()
    safe_name = table_name.replace('.', '_').replace(' ', '_').lstrip('_')
    filename = f"graph_{safe_name}_{ts.strftime('%Y%m%d_%H%M%S')}.json"
    data = {
        'metadata': {
            'table': table_name,
            'human_name': sp.get_table_human_name(table_name) if sp else None,
            'max_depth': params.get('max_depth'),
            'max_depth_up': params.get('max_depth_up'),
            'built_at': ts.isoformat(),
            'relationship_count': len(relationships),
            'graph_hash': graph_hash,
        },
        'relationships': relationships
    }
    with open(_GRAPHS_DIR / filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return filename

@st.cache_data(ttl=30)
def _list_saved_graphs_cached():
    """Возвращает список сохранённых графов (кэш 30 сек для ускорения)."""
    if not _GRAPHS_DIR.exists():
        return []
    graphs = []
    for fp in sorted(_GRAPHS_DIR.glob("graph_*.json"), reverse=True):
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
            meta = data.get('metadata', {})
            meta['filename'] = fp.name
            meta['filepath'] = str(fp)
            graphs.append(meta)
        except Exception:
            continue
    return graphs

def _list_saved_graphs():
    return _list_saved_graphs_cached()

def _load_graph(filepath):
    """Загружает граф с диска и применяет постобработку (фильтр возвратов + ограничение длины пути)."""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    rels = data.get('relationships', [])
    meta = data.get('metadata', {})
    # Применяем постобработку — для графов, сохранённых до добавления фильтрации
    base_table = meta.get('table', '')
    max_down = meta.get('max_depth', 3)
    max_up = meta.get('max_depth_up', 1)
    if base_table and rels:
        norm_base = base_table.strip('[]')
        if not norm_base.startswith('_'):
            norm_base = '_' + norm_base
        # Нормализатор для filter_graph: strip [] + ensure _ prefix
        def _simple_norm(t):
            t = t.strip('[]')
            if not t.startswith('_'):
                t = '_' + t
            return t
        rels = RelationshipBuilder.filter_graph(rels, norm_base, base_table, max_down, max_up,
                                                normalize_fn=_simple_norm)
    return rels, meta

def _delete_graph(filepath):
    """Удаляет файл графа."""
    try:
        Path(filepath).unlink()
        return True
    except Exception:
        return False

def _find_graph_by_hash(fact_table: str, graph_hash: str) -> str | None:
    """Ищет файл графа с заданным graph_hash и table. Возвращает filepath или None."""
    if not graph_hash or not fact_table or not _GRAPHS_DIR.exists():
        return None
    for fp in _GRAPHS_DIR.glob("graph_*.json"):
        try:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
            meta = data.get('metadata', {})
            if meta.get('graph_hash') == graph_hash and meta.get('table') == fact_table:
                return str(fp)
        except Exception:
            continue
    return None

# ─── Автозагрузка графа при старте (по сохранённому graph_hash) ────────────
_saved_hash = _saved_ui.get('gen_graph_hash')
_saved_fact = _saved_ui.get('gen_fact_table_db')
if (
    not st.session_state.gen_graph_built
    and _saved_hash
    and _saved_fact
    and selected_table_db == _saved_fact
):
    _graph_file = _find_graph_by_hash(selected_table_db, _saved_hash)
    if _graph_file:
        rels, meta = _load_graph(_graph_file)
        st.session_state.gen_relationships_collected = rels
        st.session_state.gen_graph_built = True
        st.session_state.gen_graph_hash = meta.get('graph_hash')
        st.session_state.gen_graph_built_at = meta.get('built_at')
        st.session_state.gen_graph_max_depth = meta.get('max_depth')
        st.session_state.gen_graph_max_depth_up = meta.get('max_depth_up')
        tc = {}
        for rel in rels:
            tc[rel['relationship_key']] = {'enabled': False, 'join_type': 'INNER JOIN'}
        st.session_state.gen_table_config = tc
        ls = _load_last_session()
        if ls and ls.get('fact_table') == selected_table_db and ls.get('graph_hash') == _saved_hash:
            st.session_state.gen_table_config = ls.get('table_config', tc)
            loaded_excl = ls.get('excluded_fields', {})
            restored = {}
            for k, v in loaded_excl.items():
                restored[k] = set(v) if isinstance(v, list) else v
            st.session_state.gen_excluded_fields = restored
            _loaded_nt = ls.get('rel_n_total') or {}
            st.session_state.gen_rel_n_total = {str(k): int(v) for k, v in _loaded_nt.items()} if _loaded_nt else {}
            st.session_state._pending_cfg_load = {
                'table_config': st.session_state.gen_table_config,
                'excluded_fields': st.session_state.gen_excluded_fields,
            }
        st.rerun()

# ─── Сохранённые графы ────────────────────────────────────────────────────
saved_graphs_all = _list_saved_graphs()
saved_graphs = [g for g in saved_graphs_all if g.get('table') == selected_table_db]
_human_fact = sp.get_table_human_name(selected_table_db) if sp else None
_saved_graphs_title = f"{_human_fact} ({selected_table_db})" if _human_fact else selected_table_db
if saved_graphs:
    with st.expander(f"📂 Сохранённые графы для таблицы {_saved_graphs_title} ({len(saved_graphs)})", expanded=False):
        for i, gm in enumerate(saved_graphs):
            built_at = gm.get('built_at', '?')
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(built_at)
                built_display = dt.strftime("%d.%m.%Y %H:%M")
            except Exception:
                built_display = built_at

            human = gm.get('human_name') or gm.get('table', '?')
            saved_hash = gm.get('graph_hash')
            hash_short = f" #{saved_hash[:8]}" if saved_hash else ""
            label = f"{human} | ↓{gm.get('max_depth','?')} ↑{gm.get('max_depth_up','?')} | {gm.get('relationship_count','?')} связей | {built_display}{hash_short}"

            col_load, col_del = st.columns([5, 1])
            with col_load:
                if st.button(f"📌 {label}", key=f"graph_load_{i}", use_container_width=True):
                    rels, meta = _load_graph(gm['filepath'])
                    st.session_state.gen_relationships_collected = rels
                    st.session_state.gen_graph_built = True
                    st.session_state.gen_graph_hash = meta.get('graph_hash')
                    st.session_state.gen_graph_built_at = meta.get('built_at')
                    st.session_state.gen_graph_max_depth = meta.get('max_depth')
                    st.session_state.gen_graph_max_depth_up = meta.get('max_depth_up')
                    # Инициализируем конфигурацию (дефолт: все поля не выбраны)
                    tc = {}
                    for rel in rels:
                        tc[rel['relationship_key']] = {'enabled': False, 'join_type': 'INNER JOIN'}
                    st.session_state.gen_table_config = tc
                    st.session_state.gen_excluded_fields = {}
                    st.session_state.gen_rel_n_total = {}
                    _save_ui_state({'gen_graph_hash': meta.get('graph_hash')})
                    # Проверяем актуальность
                    saved_hash = meta.get('graph_hash')
                    if saved_hash and selected_table_db and analyzer:
                        current_hash = _compute_graph_hash(
                            selected_table_db,
                            st.session_state.get('gen_max_depth', 3),
                            st.session_state.get('gen_max_depth_up', 1),
                            structure_file_path, st.session_state.connection_string, analyzer
                        )
                        if current_hash != saved_hash:
                            st.warning("⚠️ Параметры или структура БД изменились с момента сохранения графа. Рекомендуется перестроить.")
                    st.success(f"✅ Граф загружен: {meta.get('relationship_count', len(rels))} связей")
                    st.rerun()
            with col_del:
                if st.button("🗑️", key=f"graph_del_{i}"):
                    _delete_graph(gm['filepath'])
                    _list_saved_graphs_cached.clear()
                    st.rerun()

# ─── Построение нового графа ──────────────────────────────────────────────
if st.button("🔍 Построить граф связей", type="primary", key="gen_build_graph"):
    # Вычисляем хэш текущих параметров
    _new_hash = _compute_graph_hash(
        selected_table_db, max_depth, max_depth_up,
        structure_file_path, st.session_state.connection_string, analyzer
    )
    # Проверяем: граф с таким хэшем уже в памяти?
    if (
        st.session_state.gen_graph_built
        and st.session_state.gen_graph_hash == _new_hash
        and st.session_state.gen_relationships_collected
    ):
        st.info(
            f"ℹ️ Граф уже построен с этими параметрами "
            f"({len(st.session_state.gen_relationships_collected)} связей, хэш #{_new_hash[:8]}). "
            f"Повторное построение не требуется."
        )
    else:
        import time as _ui_time, logging as _ui_logging
        _ui_log = _ui_logging.getLogger('ui_build_graph')
        _ui_log.setLevel(_ui_logging.DEBUG)
        if not _ui_log.handlers:
            _uh = _ui_logging.StreamHandler()
            _uh.setFormatter(_ui_logging.Formatter('[UI] %(message)s'))
            _ui_log.addHandler(_uh)

        _build_ok = False
        with st.status("🔍 Построение графа связей...", expanded=True) as _status:
            try:
                _ui_t0 = _ui_time.time()
                st.write(f"📌 Таблица: `{selected_table_db}`, ↓{max_depth}, ↑{max_depth_up}")
                _ui_log.info("== UI BUILD START == table=%s down=%d up=%d", selected_table_db, max_depth, max_depth_up)

                # ── Шаг 1: Гарантируем наличие GUID-индекса ──
                if analyzer._guid_to_table_cache is not None:
                    st.write(f"✅ GUID-индекс в памяти: {len(analyzer._guid_to_table_cache):,} записей")
                    _ui_log.info("  GUID index already in memory: %d entries", len(analyzer._guid_to_table_cache))
                else:
                    _pb = st.progress(0, text="Загрузка GUID-индекса с диска...")
                    try:
                        def _cb(p, t):
                            _pb.progress(p, text=t)
                        _loaded_idx = analyzer.load_guid_index(progress_callback=_cb)
                    finally:
                        _pb.empty()
                    if _loaded_idx:
                        analyzer._guid_to_table_cache = _loaded_idx
                        st.write(f"✅ GUID-индекс загружен с диска: {len(_loaded_idx):,} записей")
                        _ui_log.info("  GUID index loaded from disk: %d entries", len(_loaded_idx))
                    else:
                        st.write("⏳ GUID-индекс не найден — строю с нуля (это может занять несколько минут)...")
                        _ui_log.info("  Building GUID index from scratch...")
                        _t_guid = _ui_time.time()
                        _built_idx = analyzer.build_guid_index()
                        st.write(f"✅ GUID-индекс построен: {len(_built_idx):,} записей за {_ui_time.time() - _t_guid:.1f}с")
                        _ui_log.info("  GUID index built: %d entries (%.1fs)", len(_built_idx), _ui_time.time() - _t_guid)

                # ── Шаг 1.5: Гарантируем наличие индекса связей ──
                if analyzer._relationship_index is not None:
                    _ri_cnt = sum(len(v) for v in analyzer._relationship_index.values())
                    st.write(f"✅ Индекс связей в памяти: {len(analyzer._relationship_index):,} таблиц, {_ri_cnt:,} полей")
                else:
                    _ri_disk = analyzer._load_relationship_index()
                    if _ri_disk is not None:
                        analyzer._relationship_index = _ri_disk
                        _ri_cnt = sum(len(v) for v in _ri_disk.values())
                        st.write(f"✅ Индекс связей загружен с диска: {len(_ri_disk):,} таблиц, {_ri_cnt:,} полей")
                    else:
                        _n_tbl, _n_q, _t_est = analyzer.estimate_relationship_index_build()
                        st.write(
                            f"⏳ Индекс связей не найден — строю: ~{_n_tbl:,} таблиц, ~{_n_q:,} запросов, "
                            f"оценка времени: {_t_est:.0f}–{_t_est * 2:.0f} сек..."
                        )
                        _t_ri = _ui_time.time()
                        _ri_built = analyzer.build_relationship_index(
                            guid_index=analyzer._guid_to_table_cache,
                            force_rebuild=True
                        )
                        _ri_cnt = sum(len(v) for v in _ri_built.values())
                        st.write(f"✅ Индекс связей построен: {len(_ri_built):,} таблиц, {_ri_cnt:,} полей за {_ui_time.time() - _t_ri:.1f}с")

                # ── Шаг 2: Создаём builder ──
                st.write("⏳ Создание RelationshipBuilder...")
                rb = RelationshipBuilder(analyzer)
                st.session_state.gen_relationship_builder = rb
                _ui_log.info("  RelationshipBuilder created (%.1fs)", _ui_time.time() - _ui_t0)

                # ── Шаг 3: BFS-обход ──
                st.write("⏳ BFS-обход графа... Подробные логи — в терминале сервера.")
                _ui_t1 = _ui_time.time()
                relationships = rb.build_mixed_graph(
                    selected_table_db,
                    max_depth_down=max_depth,
                    max_depth_up=max_depth_up,
                    structure_parser=sp,
                    limit_guids=100
                )
                _ui_log.info("  build_mixed_graph done: %d rels (%.1fs)", len(relationships), _ui_time.time() - _ui_t1)
                st.write(f"✅ Обход завершён: **{len(relationships)}** связей за **{_ui_time.time() - _ui_t1:.1f}** сек.")

                from datetime import datetime as _dt_build
                st.session_state.gen_relationships_collected = relationships
                st.session_state.gen_graph_built = True
                st.session_state.gen_graph_hash = _new_hash
                st.session_state.gen_graph_built_at = _dt_build.now().isoformat()
                st.session_state.gen_graph_max_depth = max_depth
                st.session_state.gen_graph_max_depth_up = max_depth_up
                _save_ui_state({'gen_graph_hash': _new_hash})

                # Инициализируем конфигурацию таблиц (все ВЫКЛЮЧЕНЫ по умолчанию, все поля не выбраны)
                table_config = {}
                for rel in relationships:
                    table_config[rel['relationship_key']] = {
                        'enabled': False,
                        'join_type': 'INNER JOIN'
                    }
                st.session_state.gen_table_config = table_config
                st.session_state.gen_excluded_fields = {}
                st.session_state.gen_rel_n_total = {}

                # Сохраняем граф на диск (с хэшем)
                st.write("💾 Сохранение графа на диск...")
                _save_graph(selected_table_db, relationships, {
                    'max_depth': max_depth,
                    'max_depth_up': max_depth_up
                }, graph_hash=_new_hash)
                _list_saved_graphs_cached.clear()

                # Автоматически обнаруживаем VT-таблицы
                vt_tables = analyzer.get_vt_tables(selected_table_db)
                if vt_tables:
                    st.write(f"🔗 Табличные части: {', '.join(vt_tables[:5])}" +
                             (f" и ещё {len(vt_tables) - 5}" if len(vt_tables) > 5 else ""))

                _elapsed = _ui_time.time() - _ui_t0
                _ui_log.info("== UI BUILD END == %d rels, total %.1fs", len(relationships), _elapsed)
                _status.update(label=f"✅ Найдено {len(relationships)} связей ({_elapsed:.1f}с, хэш #{_new_hash[:8]})", state="complete")
                _build_ok = True

            except Exception as e:
                _ui_log.exception("UI BUILD ERROR")
                _status.update(label="❌ Ошибка построения графа", state="error")
                st.error(f"❌ Ошибка: {str(e)}\n```\n{traceback.format_exc()}\n```")

        # st.rerun() ВЫНЕСЕН за пределы try/except и st.status,
        # чтобы RerunException не ловился блоком except
        if _build_ok:
            st.rerun()

# ─── Показ результата текущего графа ──────────────────────────────────────
if st.session_state.gen_graph_built and st.session_state.gen_relationships_collected:
    _cur_rels = st.session_state.gen_relationships_collected
    _fwd = [r for r in _cur_rels if r.get('direction') != 'reverse']
    _rev = [r for r in _cur_rels if r.get('direction') == 'reverse']
    _max_sd = max((r.get('steps_down', r.get('depth', 0)) for r in _cur_rels), default=0)
    _max_su = max((r.get('steps_up', 0) for r in _cur_rels), default=0)

    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    with col_s1:
        st.metric("Всего связей", len(_cur_rels))
    with col_s2:
        st.metric("↓ Прямых", len(_fwd))
    with col_s3:
        st.metric("↑ Обратных", len(_rev))
    with col_s4:
        st.metric("Макс. глубина", f"↓{_max_sd} ↑{_max_su}")

    # Краткая сводка по уникальным таблицам
    _unique_tables = set()
    for r in _cur_rels:
        _unique_tables.add(r.get('target_table', ''))
        _unique_tables.add(r.get('source_table', ''))
    _unique_tables.discard(selected_table_db)
    _unique_tables.discard('')
    st.caption(f"Уникальных связанных таблиц: **{len(_unique_tables)}** | Макс. шагов: **↓{_max_sd} ↑{_max_su}**")

    # Кнопка визуализации графа
    if st.button("🖼️ Визуализировать граф связей", key="gen_viz_graph"):
        with st.spinner("Отрисовка сетевого графа..."):
            try:
                from utils.guid_index_visualizer import render_relationship_graph
                viz_dir = Path(config.DEFAULT_OUTPUT_DIR) / "visualizations"
                viz_path = viz_dir / f"graph_{selected_table_db.lstrip('_')}.jpg"
                human_fact = sp.get_table_human_name(selected_table_db) if sp else None
                graph_title = f"Граф связей: {human_fact or selected_table_db}"
                render_relationship_graph(
                    relationships=_cur_rels,
                    fact_table=selected_table_db,
                    output_path=str(viz_path),
                    title=graph_title,
                    structure_parser=sp,
                    dpi=150
                )
                st.session_state['_graph_viz_path'] = str(viz_path)
                st.success(f"✅ Граф сохранён: `{viz_path}`")
            except Exception as e:
                st.error(f"❌ Ошибка визуализации: {e}")

    _graph_viz = st.session_state.get('_graph_viz_path')
    if _graph_viz and Path(_graph_viz).exists():
        st.image(_graph_viz, use_column_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# СЕКЦИЯ 7: НАСТРОЙКА СВЯЗЕЙ (Этап 4)
# ═══════════════════════════════════════════════════════════════════════════
_USE_FRAGMENT = hasattr(st, 'fragment')

def _render_config_section():
    """Блок настройки таблиц и связей. При st.fragment — rerun только этого блока."""
    # Обработка отложенного перехода: нельзя менять gen_show_only_selected после создания checkbox
    if st.session_state.get('gen_jump_pending'):
        st.session_state.gen_show_only_selected = False
        st.session_state.gen_rels_page = st.session_state.get('gen_jump_target_page', 0)
        st.session_state.gen_jump_pending = False
        if 'gen_jump_target_page' in st.session_state:
            del st.session_state['gen_jump_target_page']
        st.rerun()

    # Навигация по binary(16): _nav_scroll_to_rk — куда прокрутить; _nav_back_to_rk — откуда пришли (для «Назад»)
    if '_nav_scroll_to_rk' not in st.session_state:
        st.session_state._nav_scroll_to_rk = None
    if '_nav_back_to_rk' not in st.session_state:
        st.session_state._nav_back_to_rk = None
    if '_nav_arrived_at_rk' not in st.session_state:
        st.session_state._nav_arrived_at_rk = None

    relationships = st.session_state.gen_relationships_collected
    table_config = dict(st.session_state.gen_table_config)
    excluded_fields = st.session_state.gen_excluded_fields
    selected_table_db = st.session_state.gen_fact_table_db
    analyzer = st.session_state.gen_analyzer
    sp = st.session_state.gen_structure_parser

    # Дозаполняем gen_rel_n_total для ключей из excluded (старый last_session без rel_n_total, загрузка конфига)
    _nt_map_fill = st.session_state.gen_rel_n_total
    _rk_to_rel_fill = {r['relationship_key']: r for r in relationships}
    for _k_fill in list(excluded_fields.keys()):
        if _nt_map_fill.get(_k_fill, 0) > 0:
            continue
        if _k_fill.startswith('__root__'):
            _tbl_fill = _k_fill.replace('__root__', '', 1)
            _cols_fill = _get_table_columns_cached(st.session_state.connection_string, _tbl_fill)
            _nt_map_fill[_k_fill] = len(_cols_fill or [])
        else:
            _rel_f = _rk_to_rel_fill.get(_k_fill)
            if _rel_f:
                _df = _rel_f.get('direction', 'forward')
                _tbl_fill = _rel_f['target_table'] if _df == 'forward' else _rel_f['source_table']
                _cols_fill = _get_table_columns_cached(st.session_state.connection_string, _tbl_fill)
                _nt_map_fill[_k_fill] = len(_cols_fill or [])

    st.header("10. 🗂️ Настройка таблиц и связей")
    st.caption(f"Найдено {len(relationships)} связей. Отключите ненужные таблицы/поля или измените тип JOIN.")

    # Поиск в полях связей — в начале секции для видимости без прокрутки
    search_in_rels = st.text_input(
        "Поиск в полях связей",
        value=st.session_state.get('gen_filter_search', ''),
        key="gen_filter_search",
        placeholder="Подстрока в таблицах, полях (без учёта регистра). Пусто — показывать все.",
        help="Поиск без учёта регистра в именах таблиц и полей. Пусто — показывать все."
    )

    # Глобальный параметр: показывать мусорные поля (влияет только на отображение, не на признак выбора)
    show_junk_fields = st.checkbox(
        "🗑️ Показывать мусорные поля",
        value=st.session_state.get('gen_show_junk_fields', False),
        key="gen_show_junk_fields",
        help="Если выключено — мусорные поля скрыты из списка. Признак выбора поля сохраняется независимо."
    )

    def _is_field_visible_for_display(table_name, field_name):
        """Поле показывается в форме, если оно не мусорное ИЛИ включён показ мусорных."""
        if show_junk_fields:
            return True
        return not analyzer.is_junk_field(table_name, field_name)

    # ─── Кэш колонок связанных таблиц (один раз) ─────────────────────────
    if 'gen_columns_cache' not in st.session_state:
        st.session_state.gen_columns_cache = {}
    _col_cache = st.session_state.gen_columns_cache

    def _classify_col_type(ctype, clen):
        """Классифицирует тип колонки в категорию для фильтра."""
        ct = (ctype or '').lower()
        if ct == 'binary' and clen == 16:
            return 'ref16'
        if ct in ('binary', 'varbinary', 'image'):
            return 'ref_other'
        if ct in ('bit',):
            return 'bool'
        if ct in ('datetime', 'datetime2', 'date', 'time', 'smalldatetime', 'datetimeoffset'):
            return 'date'
        if ct in ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext', 'xml'):
            return 'text'
        if ct in ('int', 'bigint', 'smallint', 'tinyint', 'float', 'real',
                  'decimal', 'numeric', 'money', 'smallmoney'):
            return 'number'
        return 'other'

    _TYPE_FILTER_LABELS = {
        'ref16': 'Ссылки binary(16)',
        'ref_other': 'Binary(не 16)',
        'bool': 'Boolean',
        'date': 'Даты',
        'text': 'Текст',
        'number': 'Числа',
        'other': 'Другое',
    }

    def _render_type_filters(panel_id, cols):
        """Рисует чекбоксы фильтров типов. Возвращает set видимых категорий."""
        # Определяем какие категории присутствуют в данных
        present = set()
        for _, ctype, clen in cols:
            present.add(_classify_col_type(ctype, clen))

        if len(present) <= 1:
            return present  # нечего фильтровать

        st.caption("Показать типы:")
        visible = set()
        filter_cols = st.columns(len(present))
        for i, cat in enumerate(c for c in _TYPE_FILTER_LABELS if c in present):
            with filter_cols[i]:
                filt_key = f"_tf_{panel_id}_{cat}"
                if filt_key not in st.session_state:
                    st.session_state[filt_key] = True
                if st.checkbox(_TYPE_FILTER_LABELS[cat], key=filt_key):
                    visible.add(cat)
        return visible

    # ─── Инициализация excluded_fields если нет ───────────────────────────
    if 'gen_excluded_fields' not in st.session_state:
        st.session_state.gen_excluded_fields = {}
    excluded_fields = st.session_state.gen_excluded_fields
    if 'gen_rel_n_total' not in st.session_state:
        st.session_state.gen_rel_n_total = {}

    _DEFAULT_JOIN = 'INNER JOIN'

    # Строим порядок DFS: дочерние таблицы показываются сразу под родителем.
    # tree_indent, tree_level, tree_path_length — по дереву (вниз +1, вверх -1)
    # Накопительные sd/su по пути ограничиваются max_depth_down/max_depth_up.
    def _build_dfs_order():
        """Строит плоский список relationships в порядке DFS и rel_key_to_indent/level/path_length/path."""
        # Лимиты глубины из session_state (сохраняются при построении/загрузке графа)
        _max_dd = st.session_state.get('gen_graph_max_depth') or st.session_state.get('gen_max_depth', 999)
        _max_du = st.session_state.get('gen_graph_max_depth_up') or st.session_state.get('gen_max_depth_up', 999)
        if not isinstance(_max_dd, int):
            try:
                _max_dd = int(_max_dd)
            except (TypeError, ValueError):
                _max_dd = 999
        if not isinstance(_max_du, int):
            try:
                _max_du = int(_max_du)
            except (TypeError, ValueError):
                _max_du = 999

        _children = {}
        _norm = analyzer._normalize_table_name
        for rel in relationships:
            direction = rel.get('direction', 'forward')
            parent = rel['source_table'] if direction == 'forward' else rel['target_table']
            # Добавляем под оригинальным и нормализованным именем parent,
            # чтобы DFS из norm_root нашёл reverse-связи корня (ключ "_Document653" vs "Document653").
            parent_norm = _norm(parent)
            for pkey in (parent, parent_norm):
                lst = _children.setdefault(pkey, [])
                if rel not in lst:
                    lst.append(rel)
        for k in _children:
            _children[k].sort(key=lambda r: (0 if r.get('direction') != 'reverse' else 1))

        norm_root = _norm(selected_table_db)
        result = []
        visited_rk = set()
        rel_key_to_indent = {}
        rel_key_to_level = {}
        rel_key_to_path_length = {}
        # Полный путь от корня: список relationship_key всех рёбер от корня до текущего узла
        rel_key_to_path = {}
        # Накопительные sd/su для каждого ребра (для подписей (↓x ↑y))
        rel_key_to_cumulative_sd = {}
        rel_key_to_cumulative_su = {}

        def _dfs(table_name, parent_indent, parent_sd, parent_su, parent_path, table_path):
            """parent_sd/parent_su — накопительные шаги вниз/вверх от корня до parent."""
            for rel in _children.get(table_name, []):
                rk = rel['relationship_key']
                if rk in visited_rk:
                    continue
                direction = rel.get('direction', 'forward')
                # Вычисляем накопительные sd/su для этого ребра
                if direction == 'forward':
                    child_sd = parent_sd + 1
                    child_su = parent_su
                else:
                    child_sd = parent_sd
                    child_su = parent_su + 1
                # Ограничение: не добавляем ребро, если sd > max_depth_down или su > max_depth_up
                if child_sd > _max_dd or child_su > _max_du:
                    continue
                visited_rk.add(rk)
                delta = 1 if direction == 'forward' else -1
                child_indent = parent_indent + delta
                child_level = child_sd - child_su
                child_path_length = child_sd + child_su
                child_path = parent_path + [rk]
                rel_key_to_indent[rk] = child_indent
                rel_key_to_level[rk] = child_level
                rel_key_to_path_length[rk] = child_path_length
                rel_key_to_path[rk] = child_path
                rel_key_to_cumulative_sd[rk] = child_sd
                rel_key_to_cumulative_su[rk] = child_su
                result.append(rel)
                child = rel['target_table'] if direction == 'forward' else rel['source_table']
                # Не заходим повторно в таблицу по текущему пути:
                # иначе можно "вернуться" в корень из глубины и вычислить соседние рёбра
                # корня с неверным контекстом (теряются узлы уровня -1).
                child_norm = _norm(child)
                if child in table_path or child_norm in table_path:
                    continue
                _dfs(child, child_indent, child_sd, child_su, child_path, table_path | {child, child_norm})

        # Если выбранная таблица — табличная часть (_VT), документ должен быть первым корнем,
        # чтобы обратная связь (табл. часть → документ) давала уровень -1.
        roots_to_process = []
        if '_VT' in norm_root:
            doc_from_vt = norm_root.rsplit('_VT', 1)[0]
            if doc_from_vt and doc_from_vt in _children:
                roots_to_process.append(doc_from_vt)
        roots_to_process.append(norm_root)
        if norm_root != selected_table_db and selected_table_db not in roots_to_process:
            roots_to_process.append(selected_table_db)

        for root in roots_to_process:
            _dfs(root, 0, 0, 0, [], {root, _norm(root)})
        for rel in relationships:
            if rel['relationship_key'] not in visited_rk:
                rk = rel['relationship_key']
                sd = rel.get('steps_down', 0)
                su = rel.get('steps_up', 0)
                # Сироты: проверяем лимиты sd/su
                if sd > _max_dd or su > _max_du:
                    continue
                rel_key_to_indent[rk] = sd - su
                rel_key_to_level[rk] = sd - su
                rel_key_to_path_length[rk] = sd + su
                rel_key_to_path[rk] = [rk]
                rel_key_to_cumulative_sd[rk] = sd
                rel_key_to_cumulative_su[rk] = su
                result.append(rel)
        return (result, rel_key_to_indent, rel_key_to_level, rel_key_to_path_length,
                rel_key_to_path, rel_key_to_cumulative_sd, rel_key_to_cumulative_su)

    (_sorted_rels, _rel_key_to_indent, _rel_key_to_level, _rel_key_to_path_length,
     _rel_key_to_path, _rel_key_to_cumulative_sd, _rel_key_to_cumulative_su) = _build_dfs_order()

    # Сохраняем для кнопки «Сохранить» и генерации SQL (path_from_root)
    st.session_state.gen_rel_key_to_path = _rel_key_to_path

    # Словарь rk → rel для быстрого поиска связи по ключу (нужен для отображения пути)
    _rk_to_rel = {r['relationship_key']: r for r in relationships}

    # Маппинг (show_table, field_name) -> [target_rk, ...] для binary(16) полей
    from collections import defaultdict
    _field_to_target = defaultdict(list)
    for rel in relationships:
        rk = rel['relationship_key']
        direction = rel.get('direction', 'forward')
        if direction == 'forward':
            show_t = rel['source_table']
        else:
            show_t = rel['target_table']
        fn = rel['field_name']
        if rk not in _field_to_target[(show_t, fn)]:
            _field_to_target[(show_t, fn)].append(rk)

    _root_key = f"__root__{selected_table_db}"

    def _on_field_toggle(rk_arg, cname_arg, fkey_arg):
        """on_change callback: обновляет excluded_fields из виджета. excluded_fields — единственный источник правды."""
        val = st.session_state.get(fkey_arg)
        if val is None:
            return
        excl = st.session_state.get('gen_excluded_fields', {})
        s = excl.get(rk_arg, set())
        if isinstance(s, list):
            s = set(s)
        if val:
            s.discard(cname_arg)
        else:
            s.add(cname_arg)
        excl[rk_arg] = s
        st.session_state.gen_excluded_fields = excl

    def _sync_excluded_for_rel(rel):
        """Инициализирует excluded_fields для связи (если нет) и возвращает (n_included, n_total, cols)."""
        rk = rel['relationship_key']
        direction = rel.get('direction', 'forward')
        show_table = rel['source_table'] if direction == 'reverse' else rel['target_table']
        cols = _get_table_columns_cached(st.session_state.connection_string, show_table)
        if not cols:
            return 0, 0, []
        if rk not in excluded_fields:
            excluded_fields[rk] = {c[0] for c in cols}
        n_total = len(cols)
        # Кеш числа колонок для быстрого bulk-loop без повторных get_columns
        st.session_state.gen_rel_n_total[rk] = n_total
        n_included = n_total - len(excluded_fields[rk])
        return n_included, n_total, cols

    def _render_rel_node(rel, show_only_selected=False, node_number=None):
        """Рендерит узел связи: раскрываемый блок с полями. enabled = хотя бы одно поле выбрано."""
        rk = rel['relationship_key']
        cfg = table_config.get(rk, {'enabled': False, 'join_type': _DEFAULT_JOIN})

        direction = rel.get('direction', 'forward')
        dir_icon = "↑" if direction == 'reverse' else "↓"
        show_table = rel['source_table'] if direction == 'reverse' else rel['target_table']

        level = _rel_key_to_level.get(rk, rel.get('steps_down', 0) - rel.get('steps_up', 0))
        path_length = _rel_key_to_path_length.get(rk, rel.get('steps_down', 0) + rel.get('steps_up', 0))

        human_show = sp.get_table_human_name(show_table) if sp else None
        display_name = f"{human_show} ({show_table})" if human_show else show_table
        human_field = sp.get_field_human_name(rel['source_table'], rel['field_name']) if sp else None
        field_display = human_field or rel['field_name']

        # Синхронизируем excluded_fields и вычисляем n_included
        n_included, n_total, cols = _sync_excluded_for_rel(rel)
        _expanded_nodes = set(st.session_state.get('_config_expanded_nodes', set()))
        is_expanded = rk in _expanded_nodes

        # enabled = true если выбрано хотя бы одно поле
        enabled = n_included > 0
        st.session_state[f"gen_en_{rk}"] = enabled

        status_icon = "🟢" if enabled else "⚪"
        num_prefix = f"#{node_number} " if node_number is not None else ""
        level_path = f"ур.{level} дп.{path_length} "
        header = f"{num_prefix}{level_path}{status_icon} {dir_icon} **{display_name}** — полей: {n_included}/{n_total}"

        # Отступ по дереву: 3 символа на уровень (вниз=+1, вверх=-1)
        tree_indent = _rel_key_to_indent.get(rk, 0)
        indent = max(0.2, 0.3 * (2 + tree_indent))
        _cols = st.columns([indent, 10 - indent])
        with _cols[1]:
            # Прокрутка к узлу при навигации по binary(16)
            if st.session_state.get('_nav_scroll_to_rk') == rk:
                scroll_to_here(0, key=f"scroll_{rk}")
                st.session_state._nav_scroll_to_rk = None
                st.session_state._nav_arrived_at_rk = rk

            if not is_expanded:
                # Свёрнутый узел: заголовок + кнопка развернуть
                table_config[rk] = {'enabled': enabled, 'join_type': cfg.get('join_type', _DEFAULT_JOIN)}
                row1, row2 = st.columns([4, 1])
                with row1:
                    st.markdown(header)
                with row2:
                    if st.button("▼ Развернуть", key=f"expand_{rk}"):
                        _expanded_nodes.add(rk)
                        st.session_state['_config_expanded_nodes'] = _expanded_nodes
                        st.session_state.gen_excluded_fields = excluded_fields
                        st.rerun()
            else:
                # Развёрнутый узел: кнопка свернуть, link, join_type, поля
                show_back = st.session_state.get('_nav_arrived_at_rk') == rk
                if show_back:
                    back_rel = _rk_to_rel.get(st.session_state.get('_nav_back_to_rk'))
                    if back_rel:
                        direction_b = back_rel.get('direction', 'forward')
                        back_show = back_rel['source_table'] if direction_b == 'reverse' else back_rel['target_table']
                        back_human = sp.get_table_human_name(back_show) if sp else None
                        back_label = f"{back_human} ({back_show})" if back_human else back_show
                    elif st.session_state.get('_nav_back_to_rk') == _root_key:
                        back_human = sp.get_table_human_name(selected_table_db) if sp else None
                        back_label = f"{back_human} ({selected_table_db})" if back_human else selected_table_db
                    else:
                        back_label = "источник"
                    row1, row2, row3 = st.columns([3, 1, 1])
                    with row2:
                        if st.button(f"← Назад к {back_label}", key=f"nav_back_{rk}"):
                            _back_rk = st.session_state._nav_back_to_rk
                            _expanded_nodes.add(_back_rk)
                            st.session_state['_config_expanded_nodes'] = _expanded_nodes
                            st.session_state._nav_scroll_to_rk = _back_rk
                            st.session_state._nav_back_to_rk = None
                            st.session_state._nav_arrived_at_rk = None
                            st.session_state.gen_excluded_fields = excluded_fields
                            st.rerun()
                    with row3:
                        if st.button("▲ Свернуть", key=f"collapse_{rk}"):
                            _expanded_nodes.discard(rk)
                            st.session_state['_config_expanded_nodes'] = _expanded_nodes
                            st.session_state._nav_arrived_at_rk = None
                            st.session_state._nav_back_to_rk = None
                            st.session_state.gen_excluded_fields = excluded_fields
                            st.rerun()
                else:
                    row1, row2 = st.columns([4, 1])
                    with row2:
                        if st.button("▲ Свернуть", key=f"collapse_{rk}"):
                            _expanded_nodes.discard(rk)
                            st.session_state['_config_expanded_nodes'] = _expanded_nodes
                            st.session_state.gen_excluded_fields = excluded_fields
                            st.rerun()
                with row1:
                    st.markdown(header)

                col_link, col_jt = st.columns([4, 2])
                with col_link:
                    # Полный путь от корня до текущего узла: все рёбра с нумерацией
                    # (↓x ↑y) — накопительные значения sd/su от корня до данного ребра
                    edge_path = _rel_key_to_path.get(rk, [rk])
                    for edge_idx, edge_rk in enumerate(edge_path, start=1):
                        edge_rel = _rk_to_rel.get(edge_rk)
                        if not edge_rel:
                            continue
                        edge_dir = edge_rel.get('direction', 'forward')
                        arrow = "→" if edge_dir == 'forward' else "←"
                        edge_sd = _rel_key_to_cumulative_sd.get(edge_rk, edge_rel.get('steps_down', 0))
                        edge_su = _rel_key_to_cumulative_su.get(edge_rk, edge_rel.get('steps_up', 0))
                        edge_steps = f" (↓{edge_sd} ↑{edge_su})" if (edge_sd or edge_su) else ""
                        st.caption(f"{edge_idx}. 🔗 `{edge_rel['source_table']}`.`{edge_rel['field_name']}` {arrow} `{edge_rel['target_table']}`._IDRRef{edge_steps}")
                with col_jt:
                    join_type = st.selectbox(
                        "JOIN", ["LEFT JOIN", "INNER JOIN"],
                        index=1 if cfg.get('join_type', _DEFAULT_JOIN) == 'INNER JOIN' else 0,
                        key=f"gen_jt_{rk}",
                        label_visibility="collapsed"
                    )

                table_config[rk] = {'enabled': enabled, 'join_type': join_type}

                if cols:
                    tgt = show_table
                    excl_key = rk

                    if show_only_selected:
                        selected_names = [c[0] for c in cols if c[0] not in excluded_fields.get(excl_key, set())]
                        if selected_names:
                            st.caption("Выбранные поля:")
                            st.write(", ".join(selected_names[:20]) + (" ..." if len(selected_names) > 20 else ""))
                    else:
                        # Поля показываются сразу при раскрытии (без кнопки «Поля»)
                        qcol1, qcol2, qcol3 = st.columns([1, 1, 4])
                        with qcol1:
                            if st.button("✅ Все", key=f"gen_fa_{rk}"):
                                for cname, _, _ in cols:
                                    if _is_field_visible_for_display(tgt, cname):
                                        st.session_state[f"gen_f_{rk}_{cname}"] = True
                                excluded_fields[excl_key] = {
                                    c[0] for c in cols
                                    if not _is_field_visible_for_display(tgt, c[0])
                                    and c[0] in excluded_fields.get(excl_key, set())
                                }
                                st.session_state.gen_excluded_fields = excluded_fields
                        with qcol2:
                            if st.button("❌ Ничего", key=f"gen_fn_{rk}"):
                                for cname, _, _ in cols:
                                    if _is_field_visible_for_display(tgt, cname):
                                        st.session_state[f"gen_f_{rk}_{cname}"] = False
                                excluded_fields[excl_key] = {
                                    c[0] for c in cols if _is_field_visible_for_display(tgt, c[0])
                                } | {
                                    c[0] for c in cols
                                    if not _is_field_visible_for_display(tgt, c[0])
                                    and c[0] in excluded_fields.get(excl_key, set())
                                }
                                st.session_state.gen_excluded_fields = excluded_fields

                        visible_cols = [(c, t, l) for c, t, l in cols if _is_field_visible_for_display(tgt, c)]
                        visible_types = _render_type_filters(rk, visible_cols)
                        col_left, col_right = st.columns(2)
                        vis_idx = 0
                        for cname, ctype, clen in visible_cols:
                            cat = _classify_col_type(ctype, clen)
                            if cat not in visible_types:
                                continue
                            type_str = f"{ctype}({clen})" if clen else ctype
                            human_c = sp.get_field_human_name(tgt, cname) if sp else None
                            c_label = f"{human_c} (`{cname}` {type_str})" if human_c else f"`{cname}` {type_str}"
                            target_col = col_left if vis_idx % 2 == 0 else col_right
                            with target_col:
                                is_included = cname not in excluded_fields.get(excl_key, set())
                                target_rks = _field_to_target.get((tgt, cname), []) if cat == 'ref16' else []
                                if cat == 'ref16':
                                    if len(target_rks) > 1:
                                        cb_col, sel_col, btn_col = st.columns([3, 2, 1])
                                        with cb_col:
                                            _fkey = f"gen_f_{rk}_{cname}"
                                            st.checkbox(c_label, value=is_included, key=_fkey,
                                                        on_change=_on_field_toggle, args=(excl_key, cname, _fkey))
                                        with sel_col:
                                            _nav_opts = []
                                            for trk in target_rks:
                                                tr = _rk_to_rel.get(trk)
                                                if tr:
                                                    t_tbl = tr['target_table'] if tr.get('direction') == 'forward' else tr['source_table']
                                                    t_h = sp.get_table_human_name(t_tbl) if sp else ''
                                                    _nav_opts.append((f"{t_h} ({t_tbl})" if t_h else t_tbl, trk))
                                                else:
                                                    _nav_opts.append((trk, trk))
                                            _sel_labels = [o[0] for o in _nav_opts]
                                            _chosen_idx = st.selectbox(
                                                "Цель", range(len(_sel_labels)),
                                                format_func=lambda i: _sel_labels[i],
                                                key=f"nav_sel_{rk}_{cname}",
                                                label_visibility="collapsed"
                                            )
                                            _chosen_rk = _nav_opts[_chosen_idx][1]
                                        with btn_col:
                                            if st.button("→", key=f"nav_to_{rk}_{cname}", help="Перейти к настройке таблицы"):
                                                _expanded_nodes.add(_chosen_rk)
                                                st.session_state['_config_expanded_nodes'] = _expanded_nodes
                                                st.session_state._nav_back_to_rk = rk
                                                st.session_state._nav_scroll_to_rk = _chosen_rk
                                                st.session_state.gen_excluded_fields = excluded_fields
                                                st.rerun()
                                    elif len(target_rks) == 1:
                                        cb_col, btn_col = st.columns([4, 1])
                                        with cb_col:
                                            _fkey = f"gen_f_{rk}_{cname}"
                                            st.checkbox(c_label, value=is_included, key=_fkey,
                                                        on_change=_on_field_toggle, args=(excl_key, cname, _fkey))
                                        with btn_col:
                                            _trk = target_rks[0]
                                            trg_rel = _rk_to_rel.get(_trk)
                                            trg_t = trg_rel['target_table'] if trg_rel and trg_rel.get('direction') == 'forward' else (trg_rel['source_table'] if trg_rel else '')
                                            trg_h = sp.get_table_human_name(trg_t) if sp and trg_t else ''
                                            tip = f"Перейти к настройке таблицы {trg_h or trg_t}"
                                            if st.button("→", key=f"nav_to_{rk}_{cname}", help=tip):
                                                _expanded_nodes.add(_trk)
                                                st.session_state['_config_expanded_nodes'] = _expanded_nodes
                                                st.session_state._nav_back_to_rk = rk
                                                st.session_state._nav_scroll_to_rk = _trk
                                                st.session_state.gen_excluded_fields = excluded_fields
                                                st.rerun()
                                    else:
                                        cb_col, btn_col = st.columns([4, 1])
                                        with cb_col:
                                            _fkey = f"gen_f_{rk}_{cname}"
                                            st.checkbox(c_label, value=is_included, key=_fkey,
                                                        on_change=_on_field_toggle, args=(excl_key, cname, _fkey))
                                        with btn_col:
                                            st.button("→", key=f"nav_to_{rk}_{cname}", disabled=True, help="Нет связи в графе")
                                else:
                                    _fkey = f"gen_f_{rk}_{cname}"
                                    st.checkbox(c_label, value=is_included, key=_fkey,
                                                on_change=_on_field_toggle, args=(excl_key, cname, _fkey))
                            vis_idx += 1
                        st.markdown("---")

    # ─── Корневая таблица (таблица фактов) ──────────────────────────────
    _root_key = f"__root__{selected_table_db}"
    human_root = sp.get_table_human_name(selected_table_db) if sp else None
    root_display = f"{human_root} ({selected_table_db})" if human_root else selected_table_db

    root_cols = _get_table_columns_cached(st.session_state.connection_string, selected_table_db)
    n_total_root = len(root_cols) if root_cols else 0
    n_included_root = 0
    if root_cols:
        if _root_key not in excluded_fields:
            excluded_fields[_root_key] = {c[0] for c in root_cols}
        st.session_state.gen_rel_n_total[_root_key] = n_total_root
        n_included_root = n_total_root - len(excluded_fields[_root_key])

    root_header = f"⭐ Корневая таблица: {root_display} — полей: {n_included_root}/{n_total_root}"
    with st.expander(root_header, expanded=True):
        if root_cols:
            # Прокрутка к корню при навигации «Назад»
            if st.session_state.get('_nav_scroll_to_rk') == _root_key:
                scroll_to_here(0, key=f"scroll_{_root_key}")
                st.session_state._nav_scroll_to_rk = None
                st.session_state._nav_arrived_at_rk = _root_key

            # Кнопка «← Назад» при возврате из связанной таблицы
            if st.session_state.get('_nav_arrived_at_rk') == _root_key:
                back_rk = st.session_state.get('_nav_back_to_rk')
                back_rel = _rk_to_rel.get(back_rk) if back_rk else None
                if back_rel:
                    direction_b = back_rel.get('direction', 'forward')
                    back_show = back_rel['source_table'] if direction_b == 'reverse' else back_rel['target_table']
                    back_human = sp.get_table_human_name(back_show) if sp else None
                    back_label = f"{back_human} ({back_show})" if back_human else back_show
                else:
                    back_label = "источник"
                if st.button(f"← Назад к {back_label}", key=f"nav_back_{_root_key}"):
                    _expanded_nodes = set(st.session_state.get('_config_expanded_nodes', set()))
                    _expanded_nodes.add(back_rk)
                    st.session_state['_config_expanded_nodes'] = _expanded_nodes
                    st.session_state._nav_scroll_to_rk = back_rk
                    st.session_state._nav_back_to_rk = None
                    st.session_state._nav_arrived_at_rk = None
                    st.session_state.gen_excluded_fields = excluded_fields
                    st.rerun()
            # Список полей сразу при раскрытии expander корня (без кнопки «Поля …»).
            qcol1, qcol2, qcol3 = st.columns([1, 1, 4])
            with qcol1:
                if st.button("✅ Все", key=f"gen_fa_{_root_key}"):
                    for cname, _, _ in root_cols:
                        if _is_field_visible_for_display(selected_table_db, cname):
                            st.session_state[f"gen_f_{_root_key}_{cname}"] = True
                    excluded_fields[_root_key] = {
                        c[0] for c in root_cols
                        if not _is_field_visible_for_display(selected_table_db, c[0])
                        and c[0] in excluded_fields[_root_key]
                    }
                    st.session_state.gen_excluded_fields = excluded_fields
            with qcol2:
                if st.button("❌ Ничего", key=f"gen_fn_{_root_key}"):
                    for cname, _, _ in root_cols:
                        if _is_field_visible_for_display(selected_table_db, cname):
                            st.session_state[f"gen_f_{_root_key}_{cname}"] = False
                    excluded_fields[_root_key] = {
                        c[0] for c in root_cols
                        if _is_field_visible_for_display(selected_table_db, c[0])
                    } | {
                        c[0] for c in root_cols
                        if not _is_field_visible_for_display(selected_table_db, c[0])
                        and c[0] in excluded_fields[_root_key]
                    }
                    st.session_state.gen_excluded_fields = excluded_fields

            # Фильтр по типам (только для видимых полей)
            visible_root_cols = [(c, t, l) for c, t, l in root_cols if _is_field_visible_for_display(selected_table_db, c)]
            visible_types = _render_type_filters(_root_key, visible_root_cols)

            col_left, col_right = st.columns(2)
            vis_idx = 0
            for cname, ctype, clen in visible_root_cols:
                cat = _classify_col_type(ctype, clen)
                if cat not in visible_types:
                    continue
                type_str = f"{ctype}({clen})" if clen else ctype
                human_c = sp.get_field_human_name(selected_table_db, cname) if sp else None
                c_label = f"{human_c} (`{cname}` {type_str})" if human_c else f"`{cname}` {type_str}"
                target_col = col_left if vis_idx % 2 == 0 else col_right
                with target_col:
                    is_included = cname not in excluded_fields[_root_key]
                    target_rks = _field_to_target.get((selected_table_db, cname), []) if cat == 'ref16' else []
                    if cat == 'ref16':
                        if len(target_rks) > 1:
                            cb_col, sel_col, btn_col = st.columns([3, 2, 1])
                            with cb_col:
                                _fkey = f"gen_f_{_root_key}_{cname}"
                                st.checkbox(c_label, value=is_included, key=_fkey,
                                            on_change=_on_field_toggle, args=(_root_key, cname, _fkey))
                            with sel_col:
                                _nav_opts_r = []
                                for trk in target_rks:
                                    tr = _rk_to_rel.get(trk)
                                    if tr:
                                        t_tbl = tr['target_table'] if tr.get('direction') == 'forward' else tr['source_table']
                                        t_h = sp.get_table_human_name(t_tbl) if sp else ''
                                        _nav_opts_r.append((f"{t_h} ({t_tbl})" if t_h else t_tbl, trk))
                                    else:
                                        _nav_opts_r.append((trk, trk))
                                _sel_labels_r = [o[0] for o in _nav_opts_r]
                                _chosen_idx_r = st.selectbox(
                                    "Цель", range(len(_sel_labels_r)),
                                    format_func=lambda i: _sel_labels_r[i],
                                    key=f"nav_sel_{_root_key}_{cname}",
                                    label_visibility="collapsed"
                                )
                                _chosen_rk_r = _nav_opts_r[_chosen_idx_r][1]
                            with btn_col:
                                if st.button("→", key=f"nav_to_{_root_key}_{cname}", help="Перейти к настройке таблицы"):
                                    _expanded_nodes = set(st.session_state.get('_config_expanded_nodes', set()))
                                    _expanded_nodes.add(_chosen_rk_r)
                                    st.session_state['_config_expanded_nodes'] = _expanded_nodes
                                    st.session_state._nav_back_to_rk = _root_key
                                    st.session_state._nav_scroll_to_rk = _chosen_rk_r
                                    st.session_state.gen_excluded_fields = excluded_fields
                                    st.rerun()
                        elif len(target_rks) == 1:
                            cb_col, btn_col = st.columns([4, 1])
                            with cb_col:
                                _fkey = f"gen_f_{_root_key}_{cname}"
                                st.checkbox(c_label, value=is_included, key=_fkey,
                                            on_change=_on_field_toggle, args=(_root_key, cname, _fkey))
                            with btn_col:
                                _trk = target_rks[0]
                                trg_rel = _rk_to_rel.get(_trk)
                                trg_t = trg_rel['target_table'] if trg_rel and trg_rel.get('direction') == 'forward' else (trg_rel['source_table'] if trg_rel else '')
                                trg_h = sp.get_table_human_name(trg_t) if sp and trg_t else ''
                                tip = f"Перейти к настройке таблицы {trg_h or trg_t}"
                                if st.button("→", key=f"nav_to_{_root_key}_{cname}", help=tip):
                                    _expanded_nodes = set(st.session_state.get('_config_expanded_nodes', set()))
                                    _expanded_nodes.add(_trk)
                                    st.session_state['_config_expanded_nodes'] = _expanded_nodes
                                    st.session_state._nav_back_to_rk = _root_key
                                    st.session_state._nav_scroll_to_rk = _trk
                                    st.session_state.gen_excluded_fields = excluded_fields
                                    st.rerun()
                        else:
                            cb_col, btn_col = st.columns([4, 1])
                            with cb_col:
                                _fkey = f"gen_f_{_root_key}_{cname}"
                                st.checkbox(c_label, value=is_included, key=_fkey,
                                            on_change=_on_field_toggle, args=(_root_key, cname, _fkey))
                            with btn_col:
                                st.button("→", key=f"nav_to_{_root_key}_{cname}", disabled=True, help="Нет связи в графе")
                    else:
                        _fkey = f"gen_f_{_root_key}_{cname}"
                        st.checkbox(c_label, value=is_included, key=_fkey,
                                    on_change=_on_field_toggle, args=(_root_key, cname, _fkey))
                vis_idx += 1
            st.markdown("---")
        else:
            st.warning("Не удалось получить список полей корневой таблицы.")

    # Связанные таблицы — пагинация: 50 на страницу для ускорения
    REL_PER_PAGE = 50
    # enabled / n_included для всех связей: только dict lookups (без get_columns на каждую связь)
    _rel_n_total_cache = st.session_state.get('gen_rel_n_total') or {}
    _rel_n_included = {}
    for _r in _sorted_rels:
        _rk = _r['relationship_key']
        _excl_set = excluded_fields.get(_rk)
        _nt = _rel_n_total_cache.get(_rk, 0)
        if _excl_set is not None and _nt > 0:
            _ni = _nt - len(_excl_set)
        else:
            _ni = 0
        _rel_n_included[_rk] = _ni
        _enabled = _ni > 0
        st.session_state[f"gen_en_{_rk}"] = _enabled
        _cfg = table_config.get(_rk, {'join_type': _DEFAULT_JOIN})
        table_config[_rk] = {'enabled': _enabled, 'join_type': _cfg.get('join_type', _DEFAULT_JOIN)}

    # Проверка пути до корня — ОТКЛЮЧЕНО: вызывает много обращений к БД и зависание при большом графе.
    # TODO: перевести на отложенное выполнение или выполнять только при генерации SQL.

    # Уникальные уровни и длины пути из текущего графа (для фильтров)
    _all_levels = sorted(set(_rel_key_to_level.get(r['relationship_key'], 0) for r in _sorted_rels))
    _all_path_lengths = sorted(set(_rel_key_to_path_length.get(r['relationship_key'], 0) for r in _sorted_rels))

    # Сброс фильтров при смене графа (новый хэш) — по умолчанию выбрано всё
    _gh = st.session_state.get('gen_graph_hash')
    if 'gen_filter_graph_hash' not in st.session_state or st.session_state.gen_filter_graph_hash != _gh:
        st.session_state.gen_filter_graph_hash = _gh
        st.session_state.gen_filter_levels = _all_levels.copy()
        st.session_state.gen_filter_path_lengths = _all_path_lengths.copy()
    # Гарантия наличия ключей (на случай устаревшего session_state)
    if 'gen_filter_levels' not in st.session_state:
        st.session_state.gen_filter_levels = _all_levels.copy()
    if 'gen_filter_path_lengths' not in st.session_state:
        st.session_state.gen_filter_path_lengths = _all_path_lengths.copy()

    _col_cb, _col_lv, _col_pl = st.columns([1, 2, 2])
    with _col_cb:
        show_only_selected = st.checkbox(
            "Показывать только выбранное",
            value=st.session_state.get('gen_show_only_selected', False),
            key="gen_show_only_selected",
            help="Показать только таблицы с выбранными полями; внутри узла — только выбранные поля"
        )
    with _col_lv:
        selected_levels = st.multiselect(
            "Уровень (ур)",
            options=_all_levels,
            default=st.session_state.get('gen_filter_levels', _all_levels),
            key="gen_filter_levels",
            help="Фильтр по уровню дерева. По умолчанию — все."
        )
    with _col_pl:
        selected_path_lengths = st.multiselect(
            "Длина пути (дп)",
            options=_all_path_lengths,
            default=st.session_state.get('gen_filter_path_lengths', _all_path_lengths),
            key="gen_filter_path_lengths",
            help="Фильтр по длине пути. По умолчанию — все."
        )

    # search_in_rels уже отрендерен выше (в начале секции)
    search_in_rels = st.session_state.get('gen_filter_search', '')

    # Пустой выбор = показывать все (fallback при первом рендере)
    _filter_by_level = set(selected_levels) if selected_levels else set(_all_levels)
    _filter_by_path = set(selected_path_lengths) if selected_path_lengths else set(_all_path_lengths)

    _rels_to_show = _sorted_rels
    _rels_to_show = [
        r for r in _rels_to_show
        if _rel_key_to_level.get(r['relationship_key'], 0) in _filter_by_level
        and _rel_key_to_path_length.get(r['relationship_key'], 0) in _filter_by_path
    ]
    if show_only_selected:
        _rels_to_show = [r for r in _rels_to_show if _rel_n_included.get(r['relationship_key'], 0) > 0]

    # Фильтр по поиску в полях связей (по узлам): ищем во всём тексте узла
    _search_str = (search_in_rels or '').strip()
    if _search_str:
        _search_lower = _search_str.lower()

        def _node_for_rel(r):
            direction = r.get('direction', 'forward')
            return r['target_table'] if direction == 'forward' else r['source_table']

        def _build_searchable_text_for_rel(r):
            """Собирает весь текст узла для поиска: человеческое/техническое имя таблицы, рёбра пути, человеческие имена полей."""
            direction = r.get('direction', 'forward')
            show_table = r['source_table'] if direction == 'reverse' else r['target_table']
            parts = []
            human_show = sp.get_table_human_name(show_table) if sp else None
            if human_show:
                parts.append(human_show)
            parts.append(show_table)
            edge_path = _rel_key_to_path.get(r['relationship_key'], [r['relationship_key']])
            for edge_rk in edge_path:
                edge_rel = _rk_to_rel.get(edge_rk)
                if not edge_rel:
                    continue
                src = edge_rel.get('source_table', '')
                tgt = edge_rel.get('target_table', '')
                fld = edge_rel.get('field_name', '')
                parts.extend([src, tgt, fld])
                human_fld = sp.get_field_human_name(src, fld) if sp else None
                if human_fld:
                    parts.append(human_fld)
                arrow = "→" if edge_rel.get('direction') == 'forward' else "←"
                parts.append(f"{src}.{fld} {arrow} {tgt}._IDRRef")
            return " ".join(str(p) for p in parts).lower()

        def _rel_path_matches_search(r):
            searchable = _build_searchable_text_for_rel(r)
            return _search_lower in searchable

        # Строим matching_nodes по полному графу: узел входит, если хотя бы одна его связь совпадает
        _matching_nodes = set()
        for r in _sorted_rels:
            if _rel_path_matches_search(r):
                _matching_nodes.add(_node_for_rel(r))
        _rels_to_show = [r for r in _rels_to_show if _node_for_rel(r) in _matching_nodes]

    # Номера всегда из полного списка _sorted_rels (для поиска таблицы при переключении режимов)
    _rel_key_to_full_num = {r['relationship_key']: i + 1 for i, r in enumerate(_sorted_rels)}

    n_total = len(_rels_to_show)
    n_pages = max(1, (n_total + REL_PER_PAGE - 1) // REL_PER_PAGE)
    if 'gen_rels_page' not in st.session_state:
        st.session_state.gen_rels_page = 0
    # При навигации по binary(16) переключаем страницу на целевой узел
    _nav_rk = st.session_state.get('_nav_scroll_to_rk')
    if _nav_rk and _nav_rk != _root_key and _rels_to_show:
        _target_idx = next((i for i, r in enumerate(_rels_to_show) if r['relationship_key'] == _nav_rk), 0)
        st.session_state.gen_rels_page = min(_target_idx // REL_PER_PAGE, n_pages - 1)
    page = max(0, min(st.session_state.gen_rels_page, n_pages - 1))
    st.session_state.gen_rels_page = page

    st.subheader(f"🔗 Связанные таблицы ({n_total})")
    if not _sorted_rels:
        st.info(
            "ℹ️ Граф не содержит связей для выбранной таблицы.\n\n"
            "**Вы можете сгенерировать SQL** только с полями корневой таблицы — прокрутите вниз до блока «🚀 Генерация SQL»."
        )
    elif show_only_selected and not _rels_to_show:
        st.info("ℹ️ Нет таблиц с выбранными полями. Снимите флажок «Показывать только выбранное» или выберите поля в таблицах.")
    elif not _rels_to_show:
        _msg = "ℹ️ Нет таблиц по выбранным фильтрам"
        if _search_str:
            _msg += " (уровень/длина пути/поиск)"
        else:
            _msg += " (уровень/длина пути)"
        st.info(_msg + ". Расширьте выбор в фильтрах.")
    else:
        page_start = page * REL_PER_PAGE
        page_end = min(page_start + REL_PER_PAGE, n_total)
        n_total_full = len(_sorted_rels)

        # Переход по номеру таблицы (номера из полного списка)
        _col_jump1, _col_jump2, _col_jump3 = st.columns([2, 1, 1])
        with _col_jump1:
            st.caption("Перейти к таблице №")
        with _col_jump2:
            _jump_num = st.number_input(
                "Номер",
                min_value=1,
                max_value=n_total_full,
                value=min(st.session_state.get("gen_jump_to_num", 1), n_total_full),
                key="gen_jump_to_num",
                label_visibility="collapsed"
            )
        with _col_jump3:
            if st.button("Перейти", key="gen_jump_btn"):
                st.session_state.gen_jump_pending = True
                st.session_state.gen_jump_target_page = (_jump_num - 1) // REL_PER_PAGE
                st.rerun()
        if show_only_selected:
            st.caption("ℹ️ Номера соответствуют полному списку (все таблицы)")

        if n_pages > 1:
            col_first, col_prev, col_center, col_next, col_last = st.columns([1, 1, 2, 1, 1])
            with col_first:
                if st.button("⏮ В начало", key="gen_rels_first", disabled=(page <= 0)):
                    st.session_state.gen_rels_page = 0
                    st.session_state.gen_excluded_fields = excluded_fields
                    st.rerun()
            with col_prev:
                if st.button("◀ Пред.", key="gen_rels_prev", disabled=(page <= 0)):
                    st.session_state.gen_rels_page = page - 1
                    st.session_state.gen_excluded_fields = excluded_fields
                    st.rerun()
            with col_center:
                # Синхронизируем значение selectbox с текущей страницей (gen_rels_page — источник истины)
                st.session_state.gen_rels_page_select = page + 1
                _page_options = list(range(1, n_pages + 1))
                _page_1 = st.selectbox(
                    "Страница",
                    options=_page_options,
                    index=page,
                    key="gen_rels_page_select",
                    label_visibility="collapsed"
                )
                if _page_1 != page + 1:
                    st.session_state.gen_rels_page = _page_1 - 1
                    st.session_state.gen_excluded_fields = excluded_fields
                    st.rerun()
                st.caption(f"{page + 1} из {n_pages} (показаны таблицы с {page_start + 1} до {page_end} из {n_total})")
            with col_next:
                if st.button("След. ▶", key="gen_rels_next", disabled=(page >= n_pages - 1)):
                    st.session_state.gen_rels_page = page + 1
                    st.session_state.gen_excluded_fields = excluded_fields
                    st.rerun()
            with col_last:
                if st.button("В конец ⏭", key="gen_rels_last", disabled=(page >= n_pages - 1)):
                    st.session_state.gen_rels_page = n_pages - 1
                    st.session_state.gen_excluded_fields = excluded_fields
                    st.rerun()

        for rel in _rels_to_show[page_start:page_end]:
            node_num = _rel_key_to_full_num.get(rel['relationship_key'])  # номер в полном списке
            _render_rel_node(rel, show_only_selected=show_only_selected, node_number=node_num)

    st.session_state.gen_table_config = table_config
    st.session_state.gen_excluded_fields = excluded_fields

    # Сохраняем в last_session для восстановления после перезагрузки
    _gh = st.session_state.get('gen_graph_hash')
    _ft = st.session_state.get('gen_fact_table_db')
    if _gh and _ft:
        _save_last_session(_ft, _gh, table_config, excluded_fields)

    # Счётчик активных связей
    active = sum(1 for v in table_config.values() if v.get('enabled', True))
    total_fields_excluded = sum(len(v) for v in excluded_fields.values())
    st.info(f"📊 Активные связи: **{active}** из {len(relationships)}"
            + (f" | Исключённых полей: **{total_fields_excluded}**" if total_fields_excluded else ""))

    st.markdown("---")

if _USE_FRAGMENT:
    _render_config_section = st.fragment(_render_config_section)

if st.session_state.gen_graph_built and st.session_state.gen_relationships_collected:
    @st.cache_data(ttl=600)
    def _get_table_columns_cached(_conn_str, table_name):
        """Получает колонки таблицы (кэш по сессии)."""
        a = StructureAnalyzer(_conn_str)
        try:
            cols = a.get_table_columns(table_name)
            return [(c['name'], c['data_type'], c.get('max_length')) for c in cols]
        except Exception:
            return []
        finally:
            a.close()

    # ═══════════════════════════════════════════════════════════════════════
    # СЕКЦИЯ 7b: КОНФИГУРАЦИИ ПРОЕКТА (выбор/создание перед настройкой)
    # ═══════════════════════════════════════════════════════════════════════
    def _render_section9_project_configs_body():
        
        _CONFIGS_DIR_PRE = Path(config.DEFAULT_OUTPUT_DIR) / "configs"
        _pre_selected_table_db = st.session_state.gen_fact_table_db
        _pre_sp = st.session_state.gen_structure_parser
        _pre_analyzer = st.session_state.gen_analyzer
        _pre_relationships = st.session_state.gen_relationships_collected
        
        _pre_human_gt = _pre_sp.get_table_human_name(_pre_selected_table_db) or _pre_selected_table_db if _pre_sp else _pre_selected_table_db
        _pre_current_graph_hash = st.session_state.get('gen_graph_hash')
        _pre_md = st.session_state.get('gen_graph_max_depth') or st.session_state.get('gen_max_depth', '?')
        _pre_mdu = st.session_state.get('gen_graph_max_depth_up') or st.session_state.get('gen_max_depth_up', '?')
        
        # Кнопка «Новая конфигурация»
        if st.button("🆕 Новая конфигурация (сброс)", key="gen_new_cfg"):
            tc_new = {}
            for rel in _pre_relationships:
                tc_new[rel['relationship_key']] = {'enabled': False, 'join_type': 'INNER JOIN'}
            st.session_state.gen_table_config = tc_new
            # Дефолт «все поля не выбраны»: excluded = {key: set(все колонки)} для корня и всех связей
            excl_all = {}
            nt_all = {}
            _root_key = f"__root__{_pre_selected_table_db}"
            _root_cols = _get_table_columns_cached(st.session_state.connection_string, _pre_selected_table_db)
            excl_all[_root_key] = {c[0] for c in (_root_cols or [])}
            nt_all[_root_key] = len(_root_cols or [])
            for _rel in _pre_relationships:
                _rk = _rel['relationship_key']
                _dir = _rel.get('direction', 'forward')
                _tbl = _rel['target_table'] if _dir == 'forward' else _rel['source_table']
                _cols = _get_table_columns_cached(st.session_state.connection_string, _tbl)
                excl_all[_rk] = {c[0] for c in (_cols or [])}
                nt_all[_rk] = len(_cols or [])
            st.session_state.gen_excluded_fields = excl_all
            st.session_state.gen_rel_n_total = nt_all
            st.session_state._pending_cfg_load = {
                'table_config': tc_new,
                'excluded_fields': excl_all,
            }
            st.rerun()
        
        # Список сохранённых конфигураций (кэш диска, см. _load_configs_for_graph)
        _fact_norm_for_cache = _pre_analyzer._normalize_table_name(_pre_selected_table_db)
        _pre_saved_configs = _load_configs_for_graph(
            str(_CONFIGS_DIR_PRE.resolve()),
            _pre_current_graph_hash,
            _fact_norm_for_cache,
            _pre_md,
            _pre_mdu,
        )

        if _pre_saved_configs:
            _pre_n_rels = len(_pre_relationships)
            _pre_hash_part = f" #{_pre_current_graph_hash[:8]}" if _pre_current_graph_hash else ""
            _pre_graph_spec = f"{_pre_human_gt} | ↓{_pre_md} ↑{_pre_mdu} | {_pre_n_rels} связей{_pre_hash_part}"
            with st.expander(f"📂 Сохранённые конфигурации для Графа связей {_pre_graph_spec} ({len(_pre_saved_configs)})", expanded=True):
                for i, cm in enumerate(_pre_saved_configs):
                    saved_at = cm.get('saved_at', '?')
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(saved_at)
                        saved_display = dt.strftime("%d.%m.%Y %H:%M")
                    except Exception:
                        saved_display = saved_at
        
                    human = cm.get('human_name') or cm.get('fact_table', '?')
                    sel_f = cm.get('selected_fields', '?')
                    tot_f = cm.get('total_fields', '?')
                    _cfg_title = (cm.get('name') or '').strip()
                    _cfg_vn = (cm.get('view_name') or '').strip()
                    _cfg_tags_list = cm.get('tags', []) or []
                    _cfg_tags_short = ' '.join(f'[{t}]' for t in _cfg_tags_list[:5])
                    if len(_cfg_tags_list) > 5:
                        _cfg_tags_short += '…'
                    _d_full = (cm.get('description') or '').strip()
                    if len(_d_full) > 60:
                        _desc_for_label = f" — {_d_full[:60]}…"
                    elif _d_full:
                        _desc_for_label = f" — {_d_full}"
                    else:
                        _desc_for_label = ""
                    label = (
                        f"{_cfg_title + ' — ' if _cfg_title else ''}"
                        f"{human}"
                        f"{f' | VIEW `{_cfg_vn}`' if _cfg_vn else ''} | "
                        f"↓{cm.get('max_depth', '?')} ↑{cm.get('max_depth_up', '?')} | "
                        f"таблиц: {cm.get('active_tables', '?')}/{cm.get('total_tables', '?')} | "
                        f"полей: {sel_f}/{tot_f} | "
                        f"{saved_display}"
                        f"{(' ' + _cfg_tags_short) if _cfg_tags_short else ''}"
                        f"{_desc_for_label}"
                    )

                    edit_key = f"_pre_cfg_edit_{i}"
                    col_load, col_edit, col_info, col_del = st.columns([4, 1, 1, 1])
                    with col_load:
                        if st.button(f"📌 {label}", key=f"pre_cfg_load_{i}", use_container_width=True):
                            with open(cm['filepath'], 'r', encoding='utf-8') as f:
                                full_data = json.load(f)
                            td_list_load = full_data.get('metadata', {}).get('tables_detail', [])
                            # Проверка path_from_root: у всех таблиц кроме root должен быть путь
                            _missing_path = [t.get('table', '?') for t in td_list_load if t.get('role') != 'root' and 'path_from_root' not in t]
                            if _missing_path:
                                st.error(
                                    f"⚠️ Конфигурация устарела: отсутствует path_from_root у таблиц {_missing_path[:5]}{'...' if len(_missing_path) > 5 else ''}. "
                                    "Удалите старые конфигурации и сохраните заново."
                                )
                            else:
                                loaded_tc = full_data.get('table_config', {})
                                loaded_excl = full_data.get('excluded_fields', {})
                                restored_excl = {}
                                for k, v in loaded_excl.items():
                                    restored_excl[k] = set(v) if isinstance(v, list) else v
                                st.session_state._pending_cfg_load = {
                                    'table_config': loaded_tc,
                                    'excluded_fields': restored_excl,
                                }
                                st.session_state.gen_table_config = loaded_tc
                                st.session_state.gen_excluded_fields = restored_excl
                                _apply_loaded_cfg_metadata_to_widgets(full_data)
                                st.success(f"✅ Конфигурация загружена: {cm.get('active_tables', '?')} таблиц, {sel_f} полей")
                            st.rerun()
                    with col_edit:
                        if st.button("✏️", key=f"pre_cfg_edit_btn_{i}", help="Редактировать метаданные (name, VIEW, описание, теги)"):
                            # Без st.rerun: Streamlit и так перезапустит скрипт; двойной rerun грузил всю страницу.
                            st.session_state[edit_key] = not st.session_state.get(edit_key, False)
                    with col_info:
                        detail_key = f"_pre_cfg_detail_{i}"
                        if st.button("📊", key=f"pre_cfg_info_{i}", help="Показать детали"):
                            st.session_state[detail_key] = not st.session_state.get(detail_key, False)
                    with col_del:
                        if st.button("🗑️", key=f"pre_cfg_del_{i}"):
                            try:
                                Path(cm['filepath']).unlink()
                            except Exception:
                                pass
                            _invalidate_project_config_metadata_caches()
                            st.rerun()

                    # Inline-редактирование только metadata (без table_config / excluded_fields)
                    if st.session_state.get(edit_key, False):
                        st.caption("**✏️ Метаданные этого JSON** (имя, VIEW, описание, теги)")
                        _fp_meta = cm['filepath']
                        with st.form(key=f"pre_cfg_meta_form_{i}"):
                            _fm_name = st.text_input(
                                "Название конфигурации",
                                value=str(cm.get('name') or ''),
                                key=f"pre_cfg_form_name_{i}",
                            )
                            _fm_vn = st.text_input(
                                "Имя VIEW",
                                value=str(cm.get('view_name') or ''),
                                key=f"pre_cfg_form_vn_{i}",
                            )
                            _fm_desc = st.text_area(
                                "Описание",
                                value=str(cm.get('description') or ''),
                                key=f"pre_cfg_form_desc_{i}",
                                height=72,
                            )
                            _tags_joined = (
                                ", ".join(cm.get("tags", []))
                                if isinstance(cm.get("tags"), list)
                                else str(cm.get("tags") or "")
                            )
                            _fm_tags = st.text_input(
                                "Теги через запятую",
                                value=_tags_joined,
                                key=f"pre_cfg_form_tags_{i}",
                            )
                            _sub_meta = st.form_submit_button("💾 Записать метаданные в файл")
                        if _sub_meta:
                            _vn_st = (_fm_vn or "").strip()
                            if _vn_st and not _cfg_view_name_chars_valid(_vn_st):
                                st.error("Имя VIEW: только буквы (Unicode), цифры и _. Исправьте и сохраните снова.")
                            else:
                                try:
                                    with open(_fp_meta, "r", encoding="utf-8") as _f:
                                        _data_m = json.load(_f)
                                    _md = _data_m.get("metadata", {}) or {}
                                    _md["name"] = (_fm_name or "").strip()
                                    _md["view_name"] = _vn_st
                                    _md["description"] = (_fm_desc or "").strip()
                                    _md["tags"] = _parse_cfg_tags_csv(_fm_tags or "")
                                    _data_m["metadata"] = _md
                                    with open(_fp_meta, "w", encoding="utf-8") as _f:
                                        json.dump(_data_m, _f, ensure_ascii=False, indent=2)
                                    st.session_state[edit_key] = False
                                    st.success("Метаданные обновлены.")
                                    _invalidate_project_config_metadata_caches()
                                    st.rerun()
                                except Exception as _e_m:
                                    st.error(f"Ошибка записи JSON: {_e_m}")

                    if st.session_state.get(detail_key, False):
                        # Загружаем полный файл — table_config в корне, tables_detail в metadata
                        _full_data = {}
                        try:
                            with open(cm['filepath'], 'r', encoding='utf-8') as _f:
                                _full_data = json.load(_f)
                        except Exception:
                            pass
                        _cfg_edges = cm.get('edges', []) or (_full_data.get('metadata', {}) or {}).get('edges', [])
                        td_list = cm.get('tables_detail', []) or (_full_data.get('metadata', {}) or {}).get('tables_detail', [])
                        st.caption("**Таблицы и выбранные поля:**")
                        for _td in td_list:
                            _sel_names = _td.get('selected_field_names', [])
                            if not _sel_names:
                                continue
                            _t_human = _td.get('human_name') or _td.get('table', '?')
                            _t_tbl = _td.get('table', '?')
                            st.markdown(f"- **{_t_human}** (`{_t_tbl}`): {', '.join(_sel_names)}")
                        st.markdown("---")
                        if _cfg_edges:
                            _root_table = cm.get('fact_table', '')
                            _root_human = cm.get('human_name') or _root_table
                            _root_td = next((t for t in td_list if t.get('role') == 'root'), None)
                            _root_sel = _root_td.get('selected_fields', '?') if _root_td else '?'
                            _root_tot = _root_td.get('total_fields', '?') if _root_td else '?'
                            st.markdown(f"⭐ **{_root_human}** (`{_root_table}`) — полей: {_root_sel}/{_root_tot}")
                            _children_by_source = {}
                            for _e in _cfg_edges:
                                _dir = _e.get('direction', 'forward')
                                _parent = _e['source'] if _dir == 'forward' else _e['target']
                                _children_by_source.setdefault(_parent, []).append(_e)
                            _visited_edges = set()
                            def _render_tree_pre(parent_table, indent_level):
                                for _e in _children_by_source.get(parent_table, []):
                                    _ek = _e.get('relationship_key', '')
                                    if _ek in _visited_edges:
                                        continue
                                    _visited_edges.add(_ek)
                                    _dir = _e.get('direction', 'forward')
                                    _dir_icon = "↑" if _dir == 'reverse' else "↓"
                                    _child = _e['target'] if _dir == 'forward' else _e['source']
                                    _child_human = (_e.get('target_human') if _dir == 'forward' else _e.get('source_human')) or _child
                                    _field = _e.get('field_name', '?')
                                    _jt = _e.get('join_type', '')
                                    _depth = _e.get('depth', '')
                                    _sel = _e.get('selected_fields', '?')
                                    _tot = _e.get('total_fields', '?')
                                    _indent = "&nbsp;" * (indent_level * 4)
                                    st.markdown(
                                        f"{_indent}{_dir_icon} --[`{_field}`]--> "
                                        f"**{_child_human}** (`{_child}`) "
                                        f"d={_depth} {_jt} — полей: {_sel}/{_tot}"
                                    )
                                    _render_tree_pre(_child, indent_level + 1)
                            _render_tree_pre(_root_table, 1)
                        elif td_list:
                            for td in td_list:
                                role_icon = "⭐" if td.get('role') == 'root' else ("↑" if td.get('role') == 'reverse' else "↓")
                                t_human = td.get('human_name') or td.get('table', '?')
                                t_sel = td.get('selected_fields', 0)
                                t_tot = td.get('total_fields', 0)
                                st.caption(f"{role_icon} **{t_human}** ({td.get('table', '?')}) — полей: {t_sel}/{t_tot}")
                        st.markdown("---")
        else:
            st.info("ℹ️ Нет сохранённых конфигураций. Настройте таблицы и поля ниже, затем сохраните конфигурацию.")
        
        # Также поддерживаем старые файлы config_*.json (обратная совместимость)
        _pre_old_configs = list(Path(config.DEFAULT_OUTPUT_DIR).glob("config_*.json")) if Path(config.DEFAULT_OUTPUT_DIR).exists() else []
        if _pre_old_configs:
            with st.expander(f"📁 Старые конфигурации ({len(_pre_old_configs)})", expanded=False):
                for oc in _pre_old_configs:
                    col_l, col_d = st.columns([5, 1])
                    with col_l:
                        if st.button(f"📄 {oc.name}", key=f"pre_old_cfg_{oc.name}", use_container_width=True):
                            with open(oc, 'r', encoding='utf-8') as f:
                                old_data = json.load(f)
                            st.session_state.gen_table_config = old_data.get('table_config', {})
                            st.session_state._pending_cfg_load = {
                                'table_config': old_data.get('table_config', {}),
                                'excluded_fields': {},
                            }
                            st.success(f"✅ Загружена старая конфигурация: `{oc.name}`")
                            st.rerun()
                    with col_d:
                        if st.button("🗑️", key=f"pre_old_del_{oc.name}"):
                            oc.unlink(missing_ok=True)
                            st.rerun()

    def _render_section9_fragment_inner():
        """
        Заголовок, поиск и тело секции 9. В st.fragment — взаимодействие ✏️/📊/поиск
        перезапускает только этот блок, без секции 10 и шагов 1–8.
        """
        st.header("9. 📂 Конфигурации проекта")
        _cfg_gs_raw = st.text_input(
            "🔍 Поиск по названию, описанию, тегам...",
            value="",
            key="_cfg_global_search",
            help="По всем cfg_*.json и sql_*.json без фильтра по текущему графу",
        )
        _cfg_gs = _cfg_gs_raw.strip().lower()
        if _cfg_gs:
            _render_cfg_sql_global_search(_cfg_gs)
        else:
            _render_section9_project_configs_body()

    # Без API fragment — полный rerun как раньше (старые версии Streamlit).
    _run_section9 = _render_section9_fragment_inner
    if _USE_FRAGMENT:
        _run_section9 = st.fragment(_render_section9_fragment_inner)
    _run_section9()

    st.markdown("---")

    # Применяем отложенную загрузку конфигурации ДО создания виджетов
    _pending = st.session_state.pop('_pending_cfg_load', None)
    if _pending is not None:
        _p_tc = _pending.get('table_config', {})
        _p_excl = _pending.get('excluded_fields', {})
        for _rk, _rv in _p_tc.items():
            st.session_state[f"gen_en_{_rk}"] = bool(_rv.get('enabled', False))
            st.session_state[f"gen_jt_{_rk}"] = str(_rv.get('join_type', 'INNER JOIN'))
        # Синхронизируем gen_f_* для полей — чтобы fragment не перезаписывал состояние
        _rels = st.session_state.gen_relationships_collected or []
        _rel_by_rk = {r['relationship_key']: r for r in _rels}
        _fact = st.session_state.gen_fact_table_db
        _root_key = f"__root__{_fact}"
        _keys_to_sync = {_root_key} | {r['relationship_key'] for r in _rels}
        _nt_pending = {}
        for _excl_key in _keys_to_sync:
            _excl_set = _p_excl.get(_excl_key, set())
            _excl_set = set(_excl_set) if isinstance(_excl_set, (list, set)) else set()
            if _excl_key.startswith('__root__'):
                _tbl = _excl_key.replace('__root__', '', 1)
                _cols = _get_table_columns_cached(st.session_state.connection_string, _tbl)
            else:
                _rel = _rel_by_rk.get(_excl_key)
                if not _rel:
                    continue
                _dir = _rel.get('direction', 'forward')
                _tbl = _rel['target_table'] if _dir == 'forward' else _rel['source_table']
                _cols = _get_table_columns_cached(st.session_state.connection_string, _tbl)
            _nt_pending[_excl_key] = len(_cols or [])
            for _cname, _, _ in (_cols or []):
                _fkey = f"gen_f_{_excl_key}_{_cname}"
                st.session_state[_fkey] = _cname not in _excl_set
        st.session_state.gen_rel_n_total = _nt_pending

    _render_config_section()

    relationships = st.session_state.gen_relationships_collected
    table_config = st.session_state.gen_table_config
    excluded_fields = st.session_state.gen_excluded_fields
    selected_table_db = st.session_state.gen_fact_table_db
    sp = st.session_state.gen_structure_parser

    # ═══════════════════════════════════════════════════════════════════════
    # СЕКЦИЯ 8: ВЫБРАННАЯ КОНФИГУРАЦИЯ
    # ═══════════════════════════════════════════════════════════════════════
    st.header("11. 💾 Выбранная конфигурация")

    _CONFIGS_DIR = Path(config.DEFAULT_OUTPUT_DIR) / "configs"

    # ─── Сохранение и визуализация ───────────────────────────────────────
    _col_save, _col_viz = st.columns(2)
    with _col_save:
        # Метаданные для JSON и для секции SQL (имя VIEW, описание, теги)
        _human_fact_save = sp.get_table_human_name(selected_table_db) if sp else None
        _auto_name = (_human_fact_save or selected_table_db).strip() or selected_table_db
        _base_vn = re.sub(
            r"[^\w]",
            "_",
            (_human_fact_save or selected_table_db).replace(" ", "_"),
            flags=re.UNICODE,
        )
        if not (_base_vn or "").strip("_"):
            _base_vn = re.sub(r"[^\w]", "_", selected_table_db, flags=re.UNICODE)
        _auto_vn = ("vw_" + _base_vn.lstrip("_"))[:120]
        if "_cfg_save_name" not in st.session_state:
            st.session_state["_cfg_save_name"] = _auto_name
        if "_cfg_save_view_name" not in st.session_state:
            st.session_state["_cfg_save_view_name"] = _auto_vn
        if "_cfg_save_description" not in st.session_state:
            st.session_state["_cfg_save_description"] = ""
        if "_cfg_save_tags" not in st.session_state:
            st.session_state["_cfg_save_tags"] = ""

        st.caption("Метаданные при сохранении cfg и при генерации SQL:")
        st.text_input("Название конфигурации", key="_cfg_save_name")
        _vn_widget = st.text_input(
            "Имя VIEW (буквы Unicode, цифры, _)",
            key="_cfg_save_view_name",
            help="Используется в CREATE VIEW и в имени файла cfg при непустом корректном значении",
        )
        st.text_area("Описание", key="_cfg_save_description", height=70)
        st.text_input("Теги через запятую", key="_cfg_save_tags", placeholder="отчёт, продажи")

        _vn_strip = (_vn_widget or "").strip()
        if _vn_strip and not _cfg_view_name_chars_valid(_vn_strip):
            st.warning("⚠️ Имя VIEW некорректно — сохранение cfg и генерация SQL с этим именем будут заблокированы.")

        if st.button("💾 Сохранить текущую конфигурацию", type="primary", key="gen_save_cfg"):
            from datetime import datetime
            _CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
            ts = datetime.now()
            safe_name = selected_table_db.replace('.', '_').replace(' ', '_').lstrip('_')

            # Исключена дублирующая пересинхронизация
            # excluded_fields уже корректно обновлен в _sync_excluded_for_rel

            # Конвертируем excluded_fields sets в lists для JSON
            excl_serializable = {}
            for k, v in excluded_fields.items():
                excl_serializable[k] = list(v) if isinstance(v, set) else v

            # Проверка дублей: сравниваем table_config и excluded_fields с существующими
            _is_duplicate = False
            if _CONFIGS_DIR.exists():
                for fp in _CONFIGS_DIR.glob("cfg_*.json"):
                    try:
                        with open(fp, 'r', encoding='utf-8') as f:
                            existing = json.load(f)
                        if existing.get('table_config') == table_config and existing.get('excluded_fields') == excl_serializable:
                            _is_duplicate = True
                            st.warning(f"⚠️ Конфигурация с аналогичными параметрами уже существует: `{fp.name}`. Дубль не сохранён.")
                            break
                    except Exception:
                        continue

            _save_name_m = st.session_state.get("_cfg_save_name", "").strip()
            _save_vn_m = st.session_state.get("_cfg_save_view_name", "").strip()
            _save_desc_m = st.session_state.get("_cfg_save_description", "").strip()
            _save_tags_m = _parse_cfg_tags_csv(st.session_state.get("_cfg_save_tags", ""))

            if not _is_duplicate and _save_vn_m and not _cfg_view_name_chars_valid(_save_vn_m):
                st.error("Исправьте имя VIEW или оставьте его пустым — сохранение отменено.")

            if not _is_duplicate and (not _save_vn_m or _cfg_view_name_chars_valid(_save_vn_m)):
                _rel_key_to_path_save = st.session_state.get('gen_rel_key_to_path') or {}
                _rk_to_rel_save = {r['relationship_key']: r for r in relationships}

                # Имя файла: из санитизированного view_name, иначе как раньше — по таблице
                if _save_vn_m:
                    _vn_for_fn = re.sub(r"[^\w]", "_", _save_vn_m, flags=re.UNICODE)
                    filename = f"cfg_{_vn_for_fn}_{ts.strftime('%Y%m%d_%H%M%S')}.json"
                else:
                    filename = f"cfg_{safe_name}_{ts.strftime('%Y%m%d_%H%M%S')}.json"

                # Собираем детали по каждой таблице, включая корневую и транзитные
                _total_fields = 0
                _selected_fields = 0
                _active_tables = 0
                _tables_detail = []
                _tables_seen = set()  # чтобы не дублировать транзитные

                # 1) Корневая таблица
                _root_key_save = f"__root__{selected_table_db}"
                root_cols_save = _get_table_columns_cached(st.session_state.connection_string, selected_table_db)
                if root_cols_save:
                    n_root = len(root_cols_save)
                    excl_root = len(excluded_fields.get(_root_key_save, set()))
                    sel_root = n_root - excl_root
                    _total_fields += n_root
                    _selected_fields += sel_root
                    _active_tables += 1
                    root_selected_names = [c[0] for c in root_cols_save if c[0] not in excluded_fields.get(_root_key_save, set())]
                    _tables_detail.append({
                        'table': selected_table_db,
                        'human_name': sp.get_table_human_name(selected_table_db) if sp else None,
                        'role': 'root',
                        'total_fields': n_root,
                        'selected_fields': sel_root,
                        'selected_field_names': root_selected_names,
                    })
                    _tables_seen.add(selected_table_db)

                # 2) Связанные таблицы: path_from_root, транзитные, рёбра из путей
                _edges = []
                _edges_rk_seen = set()
                for rel in relationships:
                    rk = rel['relationship_key']
                    tc = table_config.get(rk, {})
                    if not tc.get('enabled'):
                        continue
                    path_from_root = _rel_key_to_path_save.get(rk, [rk])
                    src = rel['source_table']
                    tgt = rel['target_table']
                    _dir = rel.get('direction', 'forward')
                    show_table = tgt if _dir == 'forward' else src
                    cols = _get_table_columns_cached(st.session_state.connection_string, show_table)
                    if not cols:
                        continue
                    n = len(cols)
                    excl = len(excluded_fields.get(rk, set()))
                    sel = n - excl
                    sel_names = [c[0] for c in cols if c[0] not in excluded_fields.get(rk, set())]

                    # Транзитные таблицы: все rk в path кроме последнего
                    for transit_rk in path_from_root[:-1]:
                        transit_rel = _rk_to_rel_save.get(transit_rk)
                        if not transit_rel:
                            continue
                        td = transit_rel.get('direction', 'forward')
                        transit_show = transit_rel['target_table'] if td == 'forward' else transit_rel['source_table']
                        if transit_show not in _tables_seen:
                            _tables_seen.add(transit_show)
                            transit_cols = _get_table_columns_cached(st.session_state.connection_string, transit_show)
                            n_transit = len(transit_cols) if transit_cols else 0
                            _total_fields += n_transit
                            _active_tables += 1
                            _tables_detail.append({
                                'table': transit_show,
                                'human_name': sp.get_table_human_name(transit_show) if sp else None,
                                'role': transit_rel.get('direction', 'forward'),
                                'relationship_key': transit_rk,
                                'source_field': transit_rel.get('source_field'),
                                'depth': transit_rel.get('depth', 0),
                                'join_type': table_config.get(transit_rk, {}).get('join_type', 'INNER JOIN'),
                                'total_fields': n_transit,
                                'selected_fields': 0,
                                'selected_field_names': [],
                                'path_from_root': _rel_key_to_path_save.get(transit_rk, [transit_rk]),
                            })
                            # Ребро для транзитной связи
                            if transit_rk not in _edges_rk_seen:
                                _edges_rk_seen.add(transit_rk)
                                tsrc = transit_rel['source_table']
                                ttgt = transit_rel['target_table']
                                _edges.append({
                                    'source': tsrc,
                                    'source_human': sp.get_table_human_name(tsrc) if sp else None,
                                    'target': ttgt,
                                    'target_human': sp.get_table_human_name(ttgt) if sp else None,
                                    'field_name': transit_rel.get('field_name'),
                                    'join_type': table_config.get(transit_rk, {}).get('join_type', 'INNER JOIN'),
                                    'direction': transit_rel.get('direction', 'forward'),
                                    'depth': transit_rel.get('depth', 0),
                                    'relationship_key': transit_rk,
                                    'selected_fields': 0,
                                    'total_fields': n_transit,
                                })

                    _total_fields += n
                    _selected_fields += sel
                    _active_tables += 1
                    _tables_detail.append({
                        'table': show_table,
                        'human_name': sp.get_table_human_name(show_table) if sp else None,
                        'role': rel.get('direction', 'forward'),
                        'relationship_key': rk,
                        'source_field': rel.get('source_field'),
                        'depth': rel.get('depth', 0),
                        'join_type': tc.get('join_type', 'INNER JOIN'),
                        'total_fields': n,
                        'selected_fields': sel,
                        'selected_field_names': sel_names,
                        'path_from_root': path_from_root,
                    })
                    # Рёбра из path_from_root (все рёбра пути)
                    for edge_rk in path_from_root:
                        if edge_rk in _edges_rk_seen:
                            continue
                        edge_rel = _rk_to_rel_save.get(edge_rk)
                        if not edge_rel:
                            continue
                        _edges_rk_seen.add(edge_rk)
                        esrc = edge_rel['source_table']
                        etgt = edge_rel['target_table']
                        edir = edge_rel.get('direction', 'forward')
                        eshow = etgt if edir == 'forward' else esrc
                        ecols = _get_table_columns_cached(st.session_state.connection_string, eshow)
                        en = len(ecols) if ecols else 0
                        esel = en - len(excluded_fields.get(edge_rk, set()))
                        _edges.append({
                            'source': esrc,
                            'source_human': sp.get_table_human_name(esrc) if sp else None,
                            'target': etgt,
                            'target_human': sp.get_table_human_name(etgt) if sp else None,
                            'field_name': edge_rel.get('field_name'),
                            'join_type': table_config.get(edge_rk, {}).get('join_type', 'INNER JOIN'),
                            'direction': edir,
                            'depth': edge_rel.get('depth', 0),
                            'relationship_key': edge_rk,
                            'selected_fields': esel,
                            'total_fields': en,
                        })

                config_data = {
                    'metadata': {
                        'name': _save_name_m,
                        'view_name': _save_vn_m,
                        'description': _save_desc_m,
                        'tags': _save_tags_m,
                        'fact_table': selected_table_db,
                        'human_name': sp.get_table_human_name(selected_table_db) if sp else None,
                        'max_depth': max_depth,
                        'max_depth_up': max_depth_up,
                        'fix_dates': fix_dates,
                        'naming_style': naming_style_code,
                        'saved_at': ts.isoformat(),
                        'active_tables': _active_tables,
                        'total_tables': len(table_config) + 1,
                        'selected_fields': _selected_fields,
                        'total_fields': _total_fields,
                        'tables_detail': _tables_detail,
                        'edges': _edges,
                        'graph_hash': st.session_state.get('gen_graph_hash'),
                        'graph_built_at': st.session_state.get('gen_graph_built_at'),
                        'relationship_count': len(relationships),
                    },
                    'table_config': table_config,
                    'excluded_fields': excl_serializable,
                }
                with open(_CONFIGS_DIR / filename, 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, ensure_ascii=False, indent=2)
                st.success(f"✅ Конфигурация сохранена: `{filename}`")
                _invalidate_project_config_metadata_caches()
    with _col_viz:
        if st.button("🖼️ Визуализировать конфигурацию", key="gen_viz_config"):
            with st.spinner("Отрисовка графа конфигурации..."):
                try:
                    from utils.guid_index_visualizer import render_relationship_graph
                    # Включённые + транзитные связи по фактическим путям включённых узлов.
                    # Строим локальную карту путей для секции 11 (она вне _render_config_section).
                    _an = st.session_state.gen_analyzer

                    def _build_viz_paths():
                        _children = {}
                        _vn = _an._normalize_table_name
                        for _rel in relationships:
                            _dir = _rel.get('direction', 'forward')
                            _parent = _rel['source_table'] if _dir == 'forward' else _rel['target_table']
                            _parent_norm = _vn(_parent)
                            for _pkey in (_parent, _parent_norm):
                                _lst = _children.setdefault(_pkey, [])
                                if _rel not in _lst:
                                    _lst.append(_rel)
                        for _k in _children:
                            _children[_k].sort(key=lambda _r: (0 if _r.get('direction') != 'reverse' else 1))

                        _norm_root = _vn(selected_table_db)
                        _ordered = []
                        _visited = set()
                        _rk_to_path = {}

                        def _dfs(_table, _parent_path, _table_path):
                            for _rel in _children.get(_table, []):
                                _rk = _rel['relationship_key']
                                if _rk in _visited:
                                    continue
                                _visited.add(_rk)
                                _path = _parent_path + [_rk]
                                _rk_to_path[_rk] = _path
                                _ordered.append(_rel)
                                _dir = _rel.get('direction', 'forward')
                                _child = _rel['target_table'] if _dir == 'forward' else _rel['source_table']
                                _child_norm = _vn(_child)
                                if _child in _table_path or _child_norm in _table_path:
                                    continue
                                _dfs(_child, _path, _table_path | {_child, _child_norm})

                        _roots = []
                        if _norm_root:
                            _roots.append(_norm_root)
                        if selected_table_db not in _roots:
                            _roots.append(selected_table_db)
                        for _root in _roots:
                            _dfs(_root, [], {_root, _vn(_root)})

                        # Не подмешиваем все relationships — иначе граф раздувается (план config_graph_viz_filter).

                        return _ordered, _rk_to_path

                    _viz_sorted_rels, _viz_rel_key_to_path = _build_viz_paths()

                    # Таблицы с хотя бы одним выбранным полем (корень + стороны включённых связей).
                    _vn_cfg = _an._normalize_table_name
                    _root_key_viz = f"__root__{selected_table_db}"
                    _root_cols_viz = _get_table_columns_cached(
                        st.session_state.connection_string, selected_table_db
                    )
                    _tables_with_selected = set()
                    if _root_cols_viz:
                        _nr = len(_root_cols_viz)
                        _xr = len(excluded_fields.get(_root_key_viz, set()))
                        if _nr - _xr > 0:
                            _tables_with_selected.add(selected_table_db)
                            _tables_with_selected.add(_vn_cfg(selected_table_db))

                    _leaf_rks = set()
                    for _rel in relationships:
                        _rk = _rel['relationship_key']
                        if not table_config.get(_rk, {}).get('enabled'):
                            continue
                        _d = _rel.get('direction', 'forward')
                        _ch = _rel['target_table'] if _d == 'forward' else _rel['source_table']
                        _cols_ch = _get_table_columns_cached(
                            st.session_state.connection_string, _ch
                        )
                        if not _cols_ch:
                            continue
                        _nch = len(_cols_ch)
                        _xch = len(excluded_fields.get(_rk, set()))
                        if _nch - _xch > 0:
                            _leaf_rks.add(_rk)
                            _tables_with_selected.add(_ch)
                            _tables_with_selected.add(_vn_cfg(_ch))

                    if not _tables_with_selected:
                        st.warning(
                            "Нет таблиц с выбранными полями — нечего показать на графе. "
                            "Включите хотя бы одно поле у корня или у присоединённой таблицы."
                        )
                    else:
                        # Объединение путей от корня до каждой «листвы» с выбранными полями (транзит по рёбрам пути).
                        _ui_paths_cfg = st.session_state.get('gen_rel_key_to_path') or {}
                        _viz_rk_order = []
                        _viz_rk_seen = set()
                        for _rk_leaf in sorted(_leaf_rks):
                            _path_use = _ui_paths_cfg.get(_rk_leaf) or _viz_rel_key_to_path.get(
                                _rk_leaf, [_rk_leaf]
                            )
                            for _prk in _path_use:
                                if _prk not in _viz_rk_seen:
                                    _viz_rk_seen.add(_prk)
                                    _viz_rk_order.append(_prk)
                        _viz_rks = set(_viz_rk_order)

                        _transit_rks = _viz_rks - _leaf_rks
                        _rk_to_rel_cfg = {r['relationship_key']: r for r in relationships}
                        _config_rels = [
                            _rk_to_rel_cfg[rk] for rk in _viz_rk_order if rk in _rk_to_rel_cfg
                        ]
                        viz_dir = Path(config.DEFAULT_OUTPUT_DIR) / "visualizations"
                        viz_dir.mkdir(parents=True, exist_ok=True)
                        from datetime import datetime as _dt_viz
                        _ts = _dt_viz.now().strftime('%Y%m%d_%H%M%S')
                        _safe = selected_table_db.replace('.', '_').replace(' ', '_').lstrip('_')
                        viz_path = viz_dir / f"config_{_safe}_{_ts}.jpg"
                        human_fact = sp.get_table_human_name(selected_table_db) if sp else None
                        cfg_title = f"Конфигурация: {human_fact or selected_table_db}"
                        # Подсчёт полей только для узлов графа (корень + таблицы из отобранных рёбер).
                        _root_key = f"__root__{selected_table_db}"
                        _root_cols = _get_table_columns_cached(
                            st.session_state.connection_string, selected_table_db
                        )
                        _node_counts = {}
                        if _root_cols:
                            _n_root = len(_root_cols)
                            _excl_root = len(excluded_fields.get(_root_key, set()))
                            _node_counts[selected_table_db] = (_n_root - _excl_root, _n_root)
                        for rel in _config_rels:
                            _dir = rel.get('direction', 'forward')
                            _tbl = rel['target_table'] if _dir == 'forward' else rel['source_table']
                            if _tbl not in _node_counts:
                                _cols = _get_table_columns_cached(
                                    st.session_state.connection_string, _tbl
                                )
                                if _cols:
                                    _n = len(_cols)
                                    if rel['relationship_key'] in _transit_rks:
                                        _node_counts[_tbl] = (0, _n)
                                    else:
                                        _excl = len(excluded_fields.get(rel['relationship_key'], set()))
                                        _node_counts[_tbl] = (_n - _excl, _n)
                        render_relationship_graph(
                            relationships=_config_rels,
                            fact_table=selected_table_db,
                            output_path=str(viz_path),
                            title=cfg_title,
                            structure_parser=sp,
                            dpi=150,
                            node_field_counts=_node_counts
                        )
                        st.session_state['_config_viz_path'] = str(viz_path)
                        st.success(f"✅ Граф конфигурации сохранён: `{viz_path.name}`")
                except Exception as e:
                    st.error(f"❌ Ошибка визуализации: {e}")

    _config_viz = st.session_state.get('_config_viz_path')
    if _config_viz and Path(_config_viz).exists():
        st.image(_config_viz, use_column_width=True)

    # Описание текущей конфигурации
    _active_count = sum(1 for rk, tc in table_config.items() if tc.get('enabled'))
    _total_excl = sum(len(v) if isinstance(v, set) else 0 for v in excluded_fields.values())
    st.info(
        f"📊 Активные связи: **{_active_count}** из {len(relationships)}"
        + (f" | Исключённых полей: **{_total_excl}**" if _total_excl else "")
    )

    st.markdown("---")

    # ═══════════════════════════════════════════════════════════════════════
    # СЕКЦИЯ 9: ГЕНЕРАЦИЯ SQL (Этап 5)
    # ═══════════════════════════════════════════════════════════════════════
    st.header("12. 🚀 Генерация SQL")

    col1, col2 = st.columns(2)
    with col1:
        output_format = st.radio(
            "Формат вывода:",
            ["CREATE VIEW", "SELECT", "Оба"],
            horizontal=True,
            key="gen_output_fmt"
        )
        fmt_map = {"CREATE VIEW": "view", "SELECT": "select", "Оба": "both"}
        output_format_code = fmt_map[output_format]
    with col2:
        human_fact = sp.get_table_human_name(selected_table_db)
        _sql_out_vn = (st.session_state.get('_cfg_save_view_name') or '').strip()
        if _sql_out_vn and _cfg_view_name_chars_valid(_sql_out_vn):
            default_filename = f"{_sql_out_vn}.sql".replace(' ', '_')
        else:
            default_filename = f"vw_{human_fact or selected_table_db.lstrip('_')}.sql".replace('.', '_').replace(' ', '_')
        output_file = st.text_input(
            "Файл вывода:",
            value=str(Path(config.DEFAULT_OUTPUT_DIR) / default_filename),
            key="gen_output_file"
        )

    _SQL_DIR = Path(config.DEFAULT_OUTPUT_DIR) / "sql"

    if st.button("🚀 Сгенерировать SQL", type="primary", key="gen_generate"):
        with st.spinner("Генерация SQL..."):
            try:
                rb = st.session_state.gen_relationship_builder
                if rb is None:
                    rb = RelationshipBuilder(analyzer)
                    st.session_state.gen_relationship_builder = rb
                vg = ViewGenerator(analyzer, rb, sp, fix_dates=fix_dates)

                _rels = st.session_state.gen_relationships_collected
                _tc = dict(st.session_state.gen_table_config)
                # Дополнительно читаем состояние чекбоксов (на случай если fragment не обновил table_config)
                for rel in _rels:
                    rk = rel['relationship_key']
                    ck = f"gen_en_{rk}"
                    if ck in st.session_state:
                        if rk not in _tc:
                            _tc[rk] = {'enabled': False, 'join_type': 'INNER JOIN'}
                        _tc[rk]['enabled'] = bool(st.session_state[ck])
                    jk = f"gen_jt_{rk}"
                    if jk in st.session_state:
                        if rk not in _tc:
                            _tc[rk] = {'enabled': False, 'join_type': 'INNER JOIN'}
                        _tc[rk]['join_type'] = str(st.session_state.get(jk, 'INNER JOIN'))
                _excl = dict(st.session_state.gen_excluded_fields)

                _md = st.session_state.get('gen_graph_max_depth') or st.session_state.get('gen_max_depth', 999)
                _mdu = st.session_state.get('gen_graph_max_depth_up') or st.session_state.get('gen_max_depth_up', 999)
                try:
                    _md = int(_md) if _md is not None else 999
                except (TypeError, ValueError):
                    _md = 999
                try:
                    _mdu = int(_mdu) if _mdu is not None else 999
                except (TypeError, ValueError):
                    _mdu = 999
                _paths = st.session_state.get('gen_rel_key_to_path') or {}
                _gen_vn = (st.session_state.get('_cfg_save_view_name') or '').strip()
                _gen_vn_arg = _gen_vn if _gen_vn and _cfg_view_name_chars_valid(_gen_vn) else None
                if _gen_vn and _gen_vn_arg is None:
                    st.error("Исправьте имя VIEW в секции 11 или очистите поле — генерация SQL отменена.")
                else:
                    sql = vg.generate_view_from_relationships(
                        fact_table=selected_table_db,
                        relationships=_rels,
                        table_config=_tc,
                        excluded_fields=_excl,
                        view_name=_gen_vn_arg,
                        output_format=output_format_code,
                        naming_style=naming_style_code,
                        max_depth_down=_md,
                        max_depth_up=_mdu,
                        paths_from_root=_paths if _paths else None
                    )

                    st.session_state.gen_generated_sql = sql

                    # Сохраняем output format и file в ui_state
                    _fmt_rev = {'CREATE VIEW': 'view', 'SELECT': 'select', 'Оба': 'both'}
                    _save_ui_state({
                        'gen_output_format': _fmt_rev.get(output_format, 'view'),
                        'gen_output_file': output_file,
                    })

                    # Сохраняем файл по указанному пути
                    out_path = Path(output_file)
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(out_path, 'w', encoding='utf-8') as f:
                        f.write(sql)

                    # Сохраняем в архив с метаданными
                    from datetime import datetime
                    _SQL_DIR.mkdir(parents=True, exist_ok=True)
                    ts = datetime.now()
                    safe_name = selected_table_db.replace('.', '_').replace(' ', '_').lstrip('_')
                    archive_name = f"sql_{safe_name}_{ts.strftime('%Y%m%d_%H%M%S')}"

                    # SQL файл
                    with open(_SQL_DIR / f"{archive_name}.sql", 'w', encoding='utf-8') as f:
                        f.write(sql)

                    # Метаданные (в т.ч. описание и теги из секции 11 — для сквозного поиска)
                    _active = sum(1 for v in _tc.values() if v.get('enabled'))
                    _sql_desc = (st.session_state.get('_cfg_save_description') or '').strip()
                    _sql_tags = _parse_cfg_tags_csv(st.session_state.get('_cfg_save_tags') or '')
                    meta = {
                        'fact_table': selected_table_db,
                        'human_name': sp.get_table_human_name(selected_table_db) if sp else None,
                        'format': output_format_code,
                        'naming_style': naming_style_code,
                        'max_depth': max_depth,
                        'max_depth_up': max_depth_up,
                        'fix_dates': fix_dates,
                        'active_tables': _active,
                        'total_tables': len(_tc),
                        'sql_lines': sql.count('\n') + 1,
                        'sql_size': len(sql),
                        'saved_at': ts.isoformat(),
                        'sql_file': f"{archive_name}.sql",
                        'description': _sql_desc,
                        'tags': _sql_tags,
                        'view_name': _gen_vn_arg,
                        'config_name': (st.session_state.get('_cfg_save_name') or '').strip() or None,
                    }
                    with open(_SQL_DIR / f"{archive_name}.json", 'w', encoding='utf-8') as f:
                        json.dump(meta, f, ensure_ascii=False, indent=2)

                    st.success(f"✅ SQL сгенерирован ({sql.count(chr(10))+1} строк) и сохранён в `{out_path.name}`")

            except Exception as e:
                st.error(f"❌ Ошибка: {str(e)}\n```\n{traceback.format_exc()}\n```")

    # ═══════════════════════════════════════════════════════════════════════
    # СЕКЦИЯ 10: РЕЗУЛЬТАТ SQL
    # ═══════════════════════════════════════════════════════════════════════
    if st.session_state.gen_generated_sql:
        st.header("13. 📋 Результат SQL")
        sql = st.session_state.gen_generated_sql

        # Показываем SQL в текстовом поле с возможностью копирования
        st.text_area(
            "SQL-запрос (можно выделить и скопировать):",
            value=sql,
            height=400,
            key="gen_sql_display"
        )

        col_dl, col_copy, col_info = st.columns([2, 2, 3])
        with col_dl:
            st.download_button(
                "📥 Скачать SQL",
                data=sql.encode('utf-8'),
                file_name=Path(output_file).name if output_file else "view.sql",
                mime="text/sql",
                key="gen_download"
            )
        with col_copy:
            st.caption(f"📊 Строк: {sql.count(chr(10))+1} | Размер: {len(sql):,} байт")
        with col_info:
            if output_file and Path(output_file).exists():
                st.info(f"💾 Сохранён: `{output_file}`")

    st.markdown("---")

    # ─── Сохранённые SQL ──────────────────────────────────────────────────
    saved_sqls = []
    if _SQL_DIR.exists():
        for fp in sorted(_SQL_DIR.glob("sql_*.json"), reverse=True):
            try:
                with open(fp, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
                meta['meta_path'] = str(fp)
                meta['sql_path'] = str(fp.parent / meta.get('sql_file', ''))
                saved_sqls.append(meta)
            except Exception:
                continue

    if saved_sqls:
        with st.expander(f"📂 Сохранённые SQL ({len(saved_sqls)})", expanded=False):
            for i, sm in enumerate(saved_sqls):
                saved_at = sm.get('saved_at', '?')
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(saved_at)
                    saved_display = dt.strftime("%d.%m.%Y %H:%M")
                except Exception:
                    saved_display = saved_at

                human = sm.get('human_name') or sm.get('fact_table', '?')
                fmt = sm.get('format', '?').upper()
                label = (
                    f"{human} | {fmt} | "
                    f"таблиц: {sm.get('active_tables', '?')}/{sm.get('total_tables', '?')} | "
                    f"{sm.get('sql_lines', '?')} строк | "
                    f"{saved_display}"
                )

                col_load, col_del = st.columns([5, 1])
                with col_load:
                    if st.button(f"📄 {label}", key=f"sql_load_{i}", use_container_width=True):
                        sql_file = sm.get('sql_path', '')
                        if sql_file and Path(sql_file).exists():
                            with open(sql_file, 'r', encoding='utf-8') as f:
                                loaded_sql = f.read()
                            st.session_state.gen_generated_sql = loaded_sql
                            st.success(f"✅ SQL загружен: {sm.get('sql_lines', '?')} строк")
                            st.rerun()
                        else:
                            st.error(f"❌ Файл не найден: `{sql_file}`")
                with col_del:
                    if st.button("🗑️", key=f"sql_del_{i}"):
                        try:
                            Path(sm['meta_path']).unlink(missing_ok=True)
                            sql_file = sm.get('sql_path', '')
                            if sql_file:
                                Path(sql_file).unlink(missing_ok=True)
                        except Exception:
                            pass
                        st.rerun()
