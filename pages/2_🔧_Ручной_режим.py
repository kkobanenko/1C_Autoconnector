#!/usr/bin/env python3
"""
Ручной режим: исходный интерфейс генерации SQL VIEW.
Полный контроль над параметрами.
"""

import streamlit as st
import io
from pathlib import Path
import traceback
import json

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from parsers.structure_parser import StructureParser
from db.structure_analyzer import StructureAnalyzer
from builders.relationship_builder import RelationshipBuilder
from generators.view_generator import ViewGenerator
from utils.db_connection import test_connection, get_connection_string_from_params
from utils.sidebar_context import render_context_sidebar
import config


st.title("🔧 Ручной режим")
st.caption("Исходный интерфейс с полным контролем над всеми параметрами генерации.")
st.markdown("---")

# Инициализация session state
if 'connection_string' not in st.session_state:
    st.session_state.connection_string = None
if 'connection_tested' not in st.session_state:
    st.session_state.connection_tested = False
if 'generated_sql' not in st.session_state:
    st.session_state.generated_sql = None
if 'relationships_collected' not in st.session_state:
    st.session_state.relationships_collected = None
if 'table_config' not in st.session_state:
    st.session_state.table_config = {}
if 'graph_built' not in st.session_state:
    st.session_state.graph_built = False
if 'analyzer' not in st.session_state:
    st.session_state.analyzer = None
if 'relationship_builder' not in st.session_state:
    st.session_state.relationship_builder = None
if 'structure_parser' not in st.session_state:
    st.session_state.structure_parser = None
if 'fact_table_db' not in st.session_state:
    st.session_state.fact_table_db = None


# Секция подключения к БД
st.header("🔌 Подключение к базе данных")

# Проверяем, есть ли credentials в переменных окружения
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
        help="По умолчанию используются credentials из файла .env / переменных окружения"
    )
else:
    cred_source = "✏️ Ввести вручную"
    st.info("ℹ️ Credentials не найдены в переменных окружения. Введите параметры подключения вручную.")

if cred_source == "🔒 Из переменных окружения (.env)":
    # Показываем параметры из окружения (только для чтения)
    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Host", value=_env_host, disabled=True, key="env_host_display")
        st.text_input("Database", value=_env_db, disabled=True, key="env_db_display")
    with col2:
        st.text_input("Username", value=_env_user, disabled=True, key="env_user_display")
        st.text_input("Password", value="••••••••", disabled=True, key="env_pass_display")
    st.caption("🔒 Credentials загружены из переменных окружения / файла `.env`")
    db_host, db_database, db_username, db_password = _env_host, _env_db, _env_user, _env_pass
else:
    # Ручной ввод
    col1, col2 = st.columns(2)
    with col1:
        db_host = st.text_input("Host", value=_env_host if _env_configured else "", key="manual_host")
        db_database = st.text_input("Database", value=_env_db if _env_configured else "", key="manual_db")
    with col2:
        db_username = st.text_input("Username", value=_env_user if _env_configured else "", key="manual_user")
        db_password = st.text_input("Password", value=_env_pass if _env_configured else "", type="password", key="manual_pass")

# Автоматическая проверка подключения
_cred_key = f"{db_host}|{db_database}|{db_username}|{db_password}"
if db_host and db_database and db_username and db_password:
    # Проверяем, изменились ли credentials с последней проверки
    if st.session_state.get('_last_cred_key') != _cred_key:
        connection_string = get_connection_string_from_params(
            db_host, db_database, db_username, db_password
        )
        with st.spinner("Проверка подключения к базе данных..."):
            success, message = test_connection(connection_string)
        if success:
            st.success(f"✅ {message}")
            st.session_state.connection_string = connection_string
            st.session_state.connection_tested = True
        else:
            st.error(f"❌ {message}")
            st.session_state.connection_string = None
            st.session_state.connection_tested = False
        st.session_state['_last_cred_key'] = _cred_key
    else:
        # Credentials не менялись — показываем сохранённый статус
        if st.session_state.connection_tested:
            st.success("✅ Подключение активно")
        else:
            st.error("❌ Подключение не удалось. Проверьте параметры.")
else:
    st.warning("⚠️ Заполните все поля для подключения к базе данных.")
    st.session_state.connection_tested = False

st.markdown("---")

# Секция параметров генерации
st.header("⚙️ Параметры генерации")

# Выбор файла структуры
structure_file_option = st.radio(
    "Выберите способ загрузки файла структуры:",
    ["Использовать файл по умолчанию", "Загрузить файл", "Указать путь"]
)

structure_file_path = None

if structure_file_option == "Использовать файл по умолчанию":
    structure_file_path = str(config.DEFAULT_STRUCTURE_FILE)
    st.info(f"Будет использован файл: {structure_file_path}")
elif structure_file_option == "Загрузить файл":
    uploaded_file = st.file_uploader("Загрузите файл структуры (.docx)", type=['docx'])
    if uploaded_file:
        # Сохраняем во временный файл
        temp_dir = Path(config.BASE_DIR) / "temp"
        temp_dir.mkdir(exist_ok=True)
        temp_file = temp_dir / uploaded_file.name
        with open(temp_file, 'wb') as f:
            f.write(uploaded_file.getbuffer())
        structure_file_path = str(temp_file)
        st.success(f"Файл загружен: {uploaded_file.name}")
else:
    structure_file_path = st.text_input(
        "Путь к файлу структуры",
        value=str(config.DEFAULT_STRUCTURE_FILE)
    )

# Поле ввода названия таблицы фактов

# Парсим структуру для получения человеческих названий
_manual_sp = None
if structure_file_path and Path(structure_file_path).exists():
    try:
        _manual_sp = StructureParser(structure_file_path)
        _manual_sp.parse()
        st.session_state.manual_structure_file_path = structure_file_path
        st.session_state.structure_parser = _manual_sp
    except Exception:
        _manual_sp = None
        st.session_state.pop("manual_structure_file_path", None)

# Получаем список таблиц из БД (если подключение установлено)
if st.session_state.connection_tested and st.session_state.connection_string:
    @st.cache_data(ttl=600)
    def _get_manual_table_list(_conn_str):
        a = StructureAnalyzer(_conn_str)
        try:
            all_t = a.get_all_tables()
            return sorted([t for t in all_t if not t.startswith('[') and '.' not in t])
        finally:
            a.close()

    _manual_tables = _get_manual_table_list(st.session_state.connection_string)

    def _classify_table_manual(name):
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

    manual_type_filter = st.radio(
        "Фильтр по типу таблицы:",
        ["Все", "Document", "AccumRg", "InfoRg", "Reference", "VT (табл. часть)", "Enum"],
        horizontal=True,
        key="manual_type_filter"
    )

    manual_search = st.text_input(
        "🔍 Поиск таблицы (по имени или техническому названию):",
        value="",
        key="manual_table_search",
        placeholder="Введите часть названия, например: Реализация или Document653"
    )

    manual_table_options = []
    manual_search_lower = manual_search.strip().lower()
    for t in _manual_tables:
        t_type = _classify_table_manual(t)
        if manual_type_filter != "Все" and t_type != manual_type_filter:
            continue
        human = _manual_sp.get_table_human_name(t) if _manual_sp else None
        label = f"{human} ({t})" if human else t
        if manual_search_lower and manual_search_lower not in label.lower():
            continue
        manual_table_options.append((label, t))

    if manual_table_options:
        selected_manual_label = st.selectbox(
            f"Выберите таблицу фактов ({len(manual_table_options)} найдено):",
            [opt[0] for opt in manual_table_options],
            key="manual_table_select"
        )
        fact_table = None
        for lbl, db_nm in manual_table_options:
            if lbl == selected_manual_label:
                fact_table = db_nm
                break
        if not fact_table:
            fact_table = ""
    else:
        st.warning("Ни одна таблица не найдена с текущим фильтром.")
        fact_table = ""
else:
    fact_table = st.text_input(
        "Название таблицы фактов",
        value="_Document653",
        help="Введите человеческое или техническое название таблицы"
    )

# Для sidebar и последующих секций — текущий выбор таблицы, даже без нажатия «Сгенерировать».
st.session_state.fact_table_db = fact_table.strip() if fact_table else None

render_context_sidebar("manual")

# Параметры генерации
col1, col2, col3 = st.columns(3)

with col1:
    max_depth = st.number_input(
        "Максимальный уровень рекурсии вниз",
        min_value=1,
        max_value=10,
        value=config.DEFAULT_MAX_DEPTH,
        help=(
            "Глубина прямых связей: сколько уровней таблиц-измерений подключать "
            "через binary(16) ссылки из таблицы фактов вниз по иерархии. "
            "Например, 1 = только прямые справочники; 2 = справочники и их справочники."
        )
    )

with col2:
    max_depth_up = st.number_input(
        "Максимальный уровень рекурсии вверх",
        min_value=0,
        max_value=5,
        value=1,
        help=(
            "Глубина обратных связей: сколько уровней таблиц, которые ссылаются "
            "на таблицу фактов снизу вверх, включать в запрос. "
            "0 = не включать обратные связи; 1 = прямые родители (например, заголовки документов)."
        )
    )

with col3:
    fix_dates = st.checkbox(
        "Исправлять искаженные даты",
        value=config.DEFAULT_FIX_DATES,
        help=(
            "1С хранит даты в полях datetime2(0). При выгрузке значения могут смещаться "
            "на 2000 лет вперёд (например, 3024 вместо 2024). "
            "Эта опция добавляет CASE-выражение: если год ≥ 3000 → вычитает 2000 лет."
        )
    )

# Секция выходного файла
st.markdown("---")
st.header("💾 Выходной файл")

output_file = st.text_input(
    "Путь к выходному SQL файлу",
    value=str(config.DEFAULT_OUTPUT_DIR / "view.sql"),
    help="Путь, куда будет сохранен сгенерированный SQL скрипт"
)

# Кнопка генерации
if st.button("🚀 Сгенерировать VIEW", type="primary"):
    # Проверки
    if not st.session_state.connection_tested:
        st.error("❌ Сначала проверьте подключение к базе данных!")
    elif not fact_table:
        st.error("❌ Укажите название таблицы фактов!")
    elif not structure_file_path or not Path(structure_file_path).exists():
        st.error(f"❌ Файл структуры не найден: {structure_file_path}")
    else:
        # Очищаем предыдущие результаты
        st.session_state.generated_sql = None
        
        # Создаем progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            # Этап 1: Загрузка структуры из .docx (0-20%)
            status_text.text("Этап 1/5: Загрузка структуры из .docx...")
            progress_bar.progress(10)
            
            structure_parser = StructureParser(structure_file_path)
            structure_data = structure_parser.parse()
            
            # Сохраняем результаты парсинга в JSON
            json_path = structure_parser.save_to_json()
            st.info(f"Результаты парсинга сохранены в: {json_path}")
            
            progress_bar.progress(20)
            
            # Этап 2: Подключение к БД и анализ структуры (20-40%)
            status_text.text("Этап 2/5: Подключение к БД и анализ структуры...")
            progress_bar.progress(30)
            
            analyzer = StructureAnalyzer(st.session_state.connection_string)
            analyzer.connect()
            
            progress_bar.progress(40)
            
            # Этап 3: Построение графа связей (40-70%)
            status_text.text("Этап 3/5: Построение индекса GUID и графа связей...")
            progress_bar.progress(50)
            
            relationship_builder = RelationshipBuilder(analyzer)
            # Определяем техническое имя таблицы для построения графа
            # Используем временный парсер для разрешения имени
            from parsers.structure_parser import StructureParser
            temp_parser = StructureParser(structure_file_path)
            temp_parser.parse()
            fact_table_db = None
            if fact_table.startswith('_'):
                fact_table_db = fact_table
            else:
                fact_table_db = temp_parser.get_table_db_name(fact_table) or fact_table
            # Строим граф только для нужной таблицы для ускорения
            relationship_builder.build_relationship_graph([fact_table_db])
            
            progress_bar.progress(70)
            
            # Сохраняем состояние для стадии выбора таблиц
            st.session_state.analyzer = analyzer
            st.session_state.relationship_builder = relationship_builder
            st.session_state.structure_parser = structure_parser
            st.session_state.fact_table_db = fact_table_db
            st.session_state.fact_table_input = fact_table
            st.session_state.max_depth_saved = max_depth
            st.session_state.max_depth_up_saved = max_depth_up
            st.session_state.fix_dates_saved = fix_dates
            st.session_state.output_file_saved = output_file
            st.session_state.graph_built = True
            
            # Этап 3.5: Сбор всех связей для отображения пользователю
            status_text.text("Этап 3.5/6: Сбор информации о связях...")
            progress_bar.progress(75)
            
            view_generator_temp = ViewGenerator(
                analyzer,
                relationship_builder,
                structure_parser,
                fix_dates=fix_dates
            )
            
            # Собираем прямые связи (вниз)
            relationships = view_generator_temp.collect_all_relationships(fact_table, max_depth=max_depth)
            
            # Собираем обратные связи (вверх), если max_depth_up > 0
            if max_depth_up > 0:
                reverse_rels = relationship_builder.find_reverse_relationships(fact_table_db, limit_guids=100)
                for rev_rel in reverse_rels:
                    # Добавляем direction для обратных связей
                    rev_rel['direction'] = 'reverse'
                    rev_rel['depth'] = 1
                    if 'source_alias' not in rev_rel:
                        rev_rel['source_alias'] = rev_rel['source_table'].lstrip('_')
                    if 'target_alias' not in rev_rel:
                        rev_rel['target_alias'] = rev_rel['target_table'].lstrip('_')
                    relationships.append(rev_rel)
            
            # Сохраняем данные в session_state ПЕРЕД st.stop()
            st.session_state.relationships_collected = relationships
            
            # Инициализируем конфигурацию по умолчанию (все связи включены, INNER JOIN — как в мастере)
            # Очищаем старую конфигурацию при новом построении графа
            st.session_state.table_config = {}
            
            # Этап 3.6: Проверка уникальных значений для фильтрации связей
            status_text.text("Этап 3.6/6: Проверка уникальных значений в полях...")
            progress_bar.progress(75)
            
            disabled_count = 0
            total_relationships = len(relationships)
            
            # Проверяем каждую связь на количество уникальных значений
            import sys
            enabled_count_temp = 0
            print(f"[INFO] Начинаем проверку {total_relationships} связей на уникальные значения...", file=sys.stderr)
            
            for idx, rel in enumerate(relationships):
                # Обновляем progress bar
                if total_relationships > 0:
                    progress_value = 75 + int((idx + 1) / total_relationships * 5)  # 75-80%
                    progress_bar.progress(progress_value)
                
                # Логируем проверяемую связь
                source_table = rel.get('source_table', 'N/A')
                field_name = rel.get('field_name', 'N/A')
                target_table = rel.get('target_table', 'N/A')
                depth = rel.get('depth', 0)
                print(f"[INFO] Проверка связи {idx + 1}/{total_relationships}: source_table='{source_table}', field_name='{field_name}', target_table='{target_table}', depth={depth}", file=sys.stderr)
                
                # Проверяем количество уникальных значений в поле связи (ленивый способ)
                try:
                    has_multiple_values = analyzer.has_at_least_two_distinct_values(
                        rel['source_table'],
                        rel['field_name']
                    )
                    
                    # Включаем связь только если найдено >= 2 уникальных значений
                    enabled = has_multiple_values
                    if enabled:
                        enabled_count_temp += 1
                        print(f"[INFO] Связь {idx + 1} ВКЛЮЧЕНА (найдено >= 2 уникальных значений)", file=sys.stderr)
                    else:
                        disabled_count += 1
                        print(f"[INFO] Связь {idx + 1} ОТКЛЮЧЕНА (найдено < 2 уникальных значений)", file=sys.stderr)
                except Exception as e:
                    # В случае ошибки отключаем связь (консервативный подход)
                    # Логируем ошибку для диагностики (временно)
                    print(f"[ERROR] Ошибка при проверке связи {idx + 1} ({source_table}.{field_name}): {type(e).__name__}: {e}", file=sys.stderr)
                    enabled = False
                    disabled_count += 1
                
                # Инициализируем конфигурацию связи
                st.session_state.table_config[rel['relationship_key']] = {
                    'enabled': enabled,
                    'join_type': 'INNER JOIN'
                }
            
            # Логируем общую статистику
            print(f"[INFO] Проверка завершена. Всего проверено: {total_relationships}, Включено: {enabled_count_temp}, Отключено: {disabled_count}", file=sys.stderr)
            
            progress_bar.progress(80)
            
            # Формируем сообщение о результатах
            if disabled_count > 0:
                status_text.text(
                    f"✅ Граф связей построен. Найдено {len(relationships)} связей. "
                    f"Отключено {disabled_count} связей с менее чем 2 уникальными значениями. "
                    f"Перейдите к разделу 'Выбор присоединяемых таблиц' для настройки."
                )
            else:
                status_text.text(
                    f"✅ Граф связей построен. Найдено {len(relationships)} связей. "
                    f"Перейдите к разделу 'Выбор присоединяемых таблиц' для настройки."
                )
            
            # Перезагружаем страницу, чтобы показать секцию выбора таблиц
            # После st.rerun() страница перезагрузится и покажет секцию выбора таблиц
            st.rerun()
            
        except Exception as e:
            progress_bar.progress(0)
            status_text.text(f"❌ Ошибка: {str(e)}")
            st.error(f"Ошибка при генерации:\n```\n{traceback.format_exc()}\n```")
            if 'analyzer' in locals():
                try:
                    analyzer.close()
                except:
                    pass

st.markdown("---")

# Секция поиска связей к таблице (информационная, только для просмотра)
if st.session_state.get('graph_built') and st.session_state.get('fact_table_db'):
    # Проверяем, собраны ли связи для отображения
    if 'all_relationships_for_display' not in st.session_state:
        # Собираем все связи (прямые и обратные) для отображения
        try:
            if (st.session_state.relationship_builder and 
                st.session_state.structure_parser and 
                st.session_state.fact_table_db):
                all_relationships = st.session_state.relationship_builder.collect_all_relationships_for_display(
                    st.session_state.fact_table_db,
                    st.session_state.structure_parser
                )
                st.session_state.all_relationships_for_display = all_relationships
        except Exception as e:
            st.session_state.all_relationships_for_display = []
    
    # Отображаем раздел поиска связей
    if st.session_state.get('all_relationships_for_display'):
        st.header("🔍 Поиск связей к таблице")
        st.info("Информация о всех связях (прямых и обратных) с базовой таблицей. Раздел только для просмотра.")
        
        all_rels = st.session_state.all_relationships_for_display
        
        # Разделяем на прямые и обратные связи
        forward_rels = [r for r in all_rels if r.get('direction') == 'forward']
        reverse_rels = [r for r in all_rels if r.get('direction') == 'reverse']
        
        st.write(f"**Найдено связей: {len(all_rels)}** (прямых: {len(forward_rels)}, обратных: {len(reverse_rels)})")
        
        # Функция для отображения связи только для просмотра
        def render_relationship_for_display(rel, indent_level=0, number_path=None):
            """Отображает связь только для просмотра (без интерактивных элементов)."""
            rel_key = rel['relationship_key']
            
            # Инициализируем путь нумерации если не передан
            if number_path is None:
                number_path = []
            
            # Получаем человеческие названия таблиц
            source_human = st.session_state.structure_parser.get_table_human_name(rel['source_table']) or rel['source_table']
            target_human = st.session_state.structure_parser.get_table_human_name(rel['target_table']) or rel['target_table']
            
            # Получаем человеческое название поля
            field_name = rel['field_name']
            field_human = None
            
            # Пробуем разные варианты имени поля
            variants_to_check = [field_name]
            
            # Вариант 1: Точное совпадение
            field_human = st.session_state.structure_parser.get_field_human_name(rel['source_table'], field_name)
            
            # Вариант 2: Без суффикса RRef или RRRef
            if not field_human:
                if field_name.endswith('RRRef'):
                    variants_to_check.append(field_name[:-6])
                    variants_to_check.append(field_name[:-5])
                elif field_name.endswith('RRef'):
                    variants_to_check.append(field_name[:-4])
                    if field_name.startswith('_'):
                        variants_to_check.append(field_name[1:-4])
            
            # Вариант 3: С/без подчеркивания
            if not field_human:
                if not field_name.startswith('_'):
                    variants_to_check.append('_' + field_name)
                    if field_name.endswith('RRef'):
                        variants_to_check.append('_' + field_name[:-4])
                else:
                    variants_to_check.append(field_name.lstrip('_'))
                    if field_name.endswith('RRef'):
                        variants_to_check.append(field_name.lstrip('_')[:-4])
            
            # Вариант 4: Без подчеркивания и без суффикса
            field_name_clean = field_name.lstrip('_')
            if field_name_clean.endswith('RRRef'):
                base_field = field_name_clean[:-6]
                variants_to_check.append(base_field)
                variants_to_check.append('_' + base_field)
            elif field_name_clean.endswith('RRef'):
                base_field = field_name_clean[:-4]
                variants_to_check.append(base_field)
                variants_to_check.append('_' + base_field)
            
            # Пробуем каждый вариант
            for variant in variants_to_check:
                if variant:
                    field_human = st.session_state.structure_parser.get_field_human_name(rel['source_table'], variant)
                    if field_human:
                        break
            
            if not field_human:
                field_human = field_name
            
            # Формируем строку нумерации
            number_str = '.'.join(map(str, number_path)) if number_path else ''
            
            indent = "  " * indent_level
            
            # Получаем техническое название PK целевой таблицы
            target_pk = "ID"  # По умолчанию
            try:
                if st.session_state.analyzer:
                    pk_columns = st.session_state.analyzer.get_primary_keys(rel['target_table'])
                    if pk_columns:
                        target_pk = pk_columns[0]  # Берем первый PK
            except:
                pass  # Если не удалось получить PK, используем значение по умолчанию
            
            # Формируем технические названия для отображения
            source_tech = f"{rel['source_table']}.{rel['field_name']}"
            target_tech = f"{rel['target_table']}.{target_pk}"
            
            # Строка 1: Нумерация и источник → цель с техническими названиями
            col1, col2 = st.columns([1, 11])
            with col1:
                if number_str:
                    st.markdown(f"**{number_str}**")
                else:
                    st.empty()
            with col2:
                if indent_level > 0:
                    st.markdown(f"{indent}└─ **{target_human}** ({target_tech})")
                else:
                    st.markdown(f"**{source_human}** ({source_tech}) → **{target_human}** ({target_tech})")
            
            # Строка 2: Поле
            col1, col2 = st.columns([1, 11])
            with col1:
                st.empty()
            with col2:
                if field_human != field_name:
                    st.caption(f"{indent}Поле: **{field_human}** (`{field_name}`)")
                else:
                    st.caption(f"{indent}Поле: `{field_name}`")
            
            # Строка 3: Алиас и таблица
            col1, col2 = st.columns([1, 11])
            with col1:
                st.empty()
            with col2:
                # Для обратных связей показываем информацию об исходной таблице
                if rel.get('direction') == 'reverse':
                    st.caption(f"{indent}Алиас источника: `{rel['source_alias']}` | Таблица источника: `{rel['source_table']}`")
                else:
                    st.caption(f"{indent}Алиас: `{rel['target_alias']}` | Таблица: `{rel['target_table']}`")
        
        # Отображаем прямые связи
        if forward_rels:
            st.subheader("Прямые связи (базовая таблица → другие таблицы)")
            for idx, rel in enumerate(forward_rels, start=1):
                render_relationship_for_display(rel, indent_level=0, number_path=[idx])
                if idx < len(forward_rels):
                    st.divider()
        
        # Отображаем обратные связи
        if reverse_rels:
            st.subheader("Обратные связи (другие таблицы → базовая таблица)")
            for idx, rel in enumerate(reverse_rels, start=1):
                render_relationship_for_display(rel, indent_level=0, number_path=[idx])
                if idx < len(reverse_rels):
                    st.divider()
        
        st.markdown("---")

# Секция выбора присоединяемых таблиц
# Проверяем наличие данных для отображения графа
# Добавляем отладочную информацию
debug_info = st.expander("🔍 Отладочная информация (для диагностики)")
with debug_info:
    st.write(f"graph_built: {st.session_state.get('graph_built')}")
    st.write(f"relationships_collected: {st.session_state.get('relationships_collected') is not None}")
    if st.session_state.get('relationships_collected'):
        st.write(f"Количество связей: {len(st.session_state.relationships_collected)}")
    else:
        st.write("relationships_collected is None или пустой")

if st.session_state.get('graph_built') and st.session_state.get('relationships_collected'):
    relationships = st.session_state.relationships_collected
    
    # Проверяем, что relationships не пустой
    if relationships and len(relationships) > 0:
        st.header("🔗 Выбор присоединяемых таблиц")
        st.info("Настройте, какие таблицы будут присоединены к представлению и какой тип JOIN использовать.")
        
        # Подсчитываем количество включенных и отключенных связей
        enabled_count = sum(
            1 for config in st.session_state.table_config.values()
            if config.get('enabled', True)
        )
        disabled_count = len(relationships) - enabled_count
        
        st.write(f"**Найдено связей: {len(relationships)} | Включено: {enabled_count} | Отключено: {disabled_count}**")
        
        # Функции для сохранения и загрузки настроек
        def save_table_config_to_file(file_path: str):
            """Сохраняет настройки таблиц в JSON файл.
            
            Формат сохраняемого файла:
            {
                "table_config": {
                    "relationship_key": {
                        "enabled": true/false,
                        "join_type": "INNER JOIN" | "LEFT JOIN" | "RIGHT JOIN" | "FULL OUTER JOIN"  # по умолчанию в UI — INNER
                    },
                    ...
                },
                "fact_table": "название таблицы фактов",
                "max_depth": 5,
                "fix_dates": true/false
            }
            """
            try:
                config_data = {
                    'table_config': st.session_state.table_config,
                    'fact_table': st.session_state.get('fact_table_input', ''),
                    'max_depth': st.session_state.get('max_depth_saved', 5),
                    'max_depth_up': st.session_state.get('max_depth_up_saved', 1),
                    'fix_dates': st.session_state.get('fix_dates_saved', True)
                }
                output_path = Path(file_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, ensure_ascii=False, indent=2)
                return True
            except Exception as e:
                st.error(f"Ошибка при сохранении настроек: {str(e)}")
                return False
        
        def load_table_config_from_file(uploaded_file=None, file_path: str = None) -> bool:
            """Загружает настройки таблиц из JSON файла.
            
            Args:
                uploaded_file: Загруженный файл через file_uploader (опционально)
                file_path: Путь к файлу на диске (опционально)
            
            Returns:
                True если загрузка успешна, False в противном случае
            """
            try:
                # Определяем источник данных
                if uploaded_file is not None:
                    # Читаем из загруженного файла
                    content = uploaded_file.read()
                    config_data = json.loads(content.decode('utf-8'))
                elif file_path:
                    # Читаем из файла на диске
                    file_path_obj = Path(file_path)
                    if not file_path_obj.exists():
                        st.error(f"Файл не найден: {file_path}")
                        return False
                    with open(file_path_obj, 'r', encoding='utf-8') as f:
                        config_data = json.load(f)
                else:
                    st.error("Не указан источник данных для загрузки")
                    return False
                
                # Проверяем структуру данных
                if 'table_config' not in config_data:
                    st.error("Неверный формат файла настроек: отсутствует 'table_config'")
                    return False
                
                # Загружаем настройки
                loaded_config = config_data['table_config']
                
                # Обновляем table_config в session_state
                # Важно: обновляем только те связи, которые есть в загруженном файле
                # Остальные связи остаются с текущими настройками
                for rel_key, config_item in loaded_config.items():
                    if rel_key in st.session_state.table_config:
                        # Обновляем существующую связь
                        st.session_state.table_config[rel_key].update(config_item)
                    else:
                        # Добавляем новую связь (если она появилась)
                        st.session_state.table_config[rel_key] = config_item
                    
                    checkbox_key = f"enabled_{rel_key}"
                    
                    # Обновляем состояние чекбокса
                    st.session_state[checkbox_key] = config_item.get('enabled', True)
                    
                    # Обновляем состояние selectbox для типа JOIN
                    join_type_key = f"join_type_{rel_key}"
                    join_type_value = config_item.get('join_type', 'INNER JOIN')
                    st.session_state[join_type_key] = join_type_value
                    
                    # Тип JOIN уже обновлен в table_config и в session_state для selectbox
                
                return True
            except json.JSONDecodeError as e:
                st.error(f"Ошибка при чтении JSON файла: {str(e)}")
                return False
            except Exception as e:
                st.error(f"Ошибка при загрузке настроек: {str(e)}")
                return False
        
        # UI для сохранения и загрузки настроек
        st.subheader("💾 Сохранение и загрузка настроек")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Сохранить текущие настройки:**")
            config_save_path = st.text_input(
                "Путь к файлу для сохранения настроек",
                value="/home/kobanenkokn/Curproject05_1cViews/config/table_config.json",
                key="config_save_path"
            )
            if st.button("💾 Сохранить настройки", type="secondary"):
                if save_table_config_to_file(config_save_path):
                    st.success(f"✅ Настройки успешно сохранены в файл: {config_save_path}")
        
        with col2:
            st.write("**Загрузить сохраненные настройки:**")
            
            # Вариант 1: Загрузка через file_uploader
            uploaded_config_file = st.file_uploader(
                "Выберите файл с настройками",
                type=['json'],
                key="config_uploader",
                help="Выберите JSON файл с ранее сохраненными настройками"
            )
            # Проверяем, что файл был загружен и еще не обработан
            if uploaded_config_file is not None:
                # Используем уникальный ключ для отслеживания загруженного файла
                file_id_key = f"config_file_id_{uploaded_config_file.file_id}"
                if file_id_key not in st.session_state or st.session_state[file_id_key] != uploaded_config_file.file_id:
                    if load_table_config_from_file(uploaded_file=uploaded_config_file):
                        st.session_state[file_id_key] = uploaded_config_file.file_id
                        st.success("✅ Настройки успешно загружены!")
                        st.rerun()  # Перезагружаем страницу для применения настроек
            
            # Вариант 2: Загрузка по пути к файлу
            st.write("**Или укажите путь к файлу:**")
            config_load_path = st.text_input(
                "Путь к файлу с настройками",
                value="/home/kobanenkokn/Curproject05_1cViews/config/table_config.json",
                key="config_load_path"
            )
            if st.button("📂 Загрузить настройки из файла", type="secondary"):
                if load_table_config_from_file(file_path=config_load_path):
                    st.success("✅ Настройки успешно загружены!")
                    st.rerun()  # Перезагружаем страницу для применения настроек
        
        st.divider()
        
        # Строим дерево связей: для каждой связи находим её дочерние связи
        # Ключ: relationship_key родителя, значение: список дочерних relationship_key
        children_map = {}
        root_relationships = []  # Связи первого уровня (depth=1)
        
        for rel in relationships:
            rel_key = rel['relationship_key']
            source_table = rel['source_table']
            source_alias = rel['source_alias']
            depth = rel['depth']
            
            # Если это связь первого уровня, добавляем в корневые
            if depth == 1:
                root_relationships.append(rel)
            
            # Находим дочерние связи (те, у которых source_table и source_alias совпадают с target_table и target_alias текущей связи)
            children = []
            for child_rel in relationships:
                if (child_rel['source_table'] == rel['target_table'] and 
                    child_rel['source_alias'] == rel['target_alias'] and
                    child_rel['depth'] == depth + 1):
                    children.append(child_rel)
            
            if children:
                children_map[rel_key] = children
        
        # Функция для рекурсивного отключения дочерних связей
        def disable_children(parent_key):
            """Отключает все дочерние связи рекурсивно."""
            if parent_key in children_map:
                for child_rel in children_map[parent_key]:
                    child_key = child_rel['relationship_key']
                    # Обновляем конфигурацию
                    if child_key in st.session_state.table_config:
                        st.session_state.table_config[child_key]['enabled'] = False
                    # Обновляем состояние чекбокса в session_state
                    checkbox_key = f"enabled_{child_key}"
                    if checkbox_key in st.session_state:
                        st.session_state[checkbox_key] = False
                    # Рекурсивно отключаем дочерние связи
                    disable_children(child_key)
        
        # Функция для рекурсивного отображения связей
        def render_relationship(rel, indent_level=0, number_path=None):
            """Рекурсивно отображает связь и её дочерние связи.
            
            Args:
                rel: Словарь с информацией о связи
                indent_level: Уровень вложенности для отступов
                number_path: Список номеров для многоуровневой нумерации (например, [1, 2, 3])
            """
            rel_key = rel['relationship_key']
            
            # Инициализируем путь нумерации если не передан
            if number_path is None:
                number_path = []
            
            # Получаем человеческие названия таблиц
            source_human = st.session_state.structure_parser.get_table_human_name(rel['source_table']) or rel['source_table']
            target_human = st.session_state.structure_parser.get_table_human_name(rel['target_table']) or rel['target_table']
            
            # Получаем человеческое название поля - пробуем разные варианты
            field_name = rel['field_name']
            field_human = None
            
            # Список вариантов для проверки
            variants_to_check = []
            
            # Вариант 1: Точное совпадение
            variants_to_check.append(field_name)
            
            # Вариант 2: Без суффикса RRef или RRRef
            if field_name.endswith('RRRef'):
                variants_to_check.append(field_name[:-6])  # Убираем 'RRRef' (6 символов)
                variants_to_check.append(field_name[:-5])   # Убираем 'RRef' (5 символов, если было опечатка)
            elif field_name.endswith('RRef'):
                variants_to_check.append(field_name[:-4])   # Убираем 'RRef' (4 символа) - это _Fld10028
                # Также пробуем вариант без подчеркивания и без RRef (важно!)
                if field_name.startswith('_'):
                    variants_to_check.append(field_name[1:-4])  # Убираем '_' в начале и 'RRef' в конце - это Fld10028
            
            # Вариант 3: С подчеркиванием в начале (если не было)
            if not field_name.startswith('_'):
                variants_to_check.append('_' + field_name)
                # Также пробуем с подчеркиванием и без RRef
                if field_name.endswith('RRef'):
                    variants_to_check.append('_' + field_name[:-4])  # Убираем 'RRef' (4 символа)
            else:
                variants_to_check.append(field_name.lstrip('_'))
                # Также пробуем без подчеркивания и без RRef
                if field_name.endswith('RRef'):
                    variants_to_check.append(field_name.lstrip('_')[:-4])  # Убираем 'RRef' (4 символа)
            
            # Вариант 4: Без подчеркивания и без суффикса (комбинация)
            # Это самый важный вариант - в структуре поля часто сохранены без подчеркивания и без RRef
            field_name_clean = field_name.lstrip('_')
            if field_name_clean.endswith('RRRef'):
                base_field = field_name_clean[:-6]
                variants_to_check.append(base_field)  # Без подчеркивания и без RRRef
                variants_to_check.append('_' + base_field)  # С подчеркиванием, без RRRef
            elif field_name_clean.endswith('RRef'):
                base_field = field_name_clean[:-4]  # Убираем 'RRef' (4 символа) - это Fld10028
                variants_to_check.append(base_field)  # Без подчеркивания и без RRef (важно! это Fld10028)
                variants_to_check.append('_' + base_field)  # С подчеркиванием, без RRef
            
            # Убираем дубликаты, сохраняя порядок
            seen = set()
            unique_variants = []
            for variant in variants_to_check:
                if variant and variant not in seen:
                    seen.add(variant)
                    unique_variants.append(variant)
            
            # Пробуем каждый вариант
            for variant in unique_variants:
                field_human = st.session_state.structure_parser.get_field_human_name(rel['source_table'], variant)
                if field_human:
                    break
            
            # Если человеческое название не найдено, используем техническое
            if not field_human:
                field_human = field_name
            
            # Формируем строку нумерации
            number_str = '.'.join(map(str, number_path)) if number_path else ''
            
            # Инициализируем конфигурацию если нужно
            if rel_key not in st.session_state.table_config:
                st.session_state.table_config[rel_key] = {
                    'enabled': True,
                    'join_type': 'INNER JOIN'
                }
            
            # Получаем текущее состояние из конфигурации
            config = st.session_state.table_config[rel_key]
            enabled = config.get('enabled', True)
            
            # Инициализируем ключ чекбокса в session_state из конфигурации
            checkbox_key = f"enabled_{rel_key}"
            if checkbox_key not in st.session_state:
                st.session_state[checkbox_key] = enabled
            else:
                # Синхронизируем состояние чекбокса с конфигурацией
                st.session_state[checkbox_key] = enabled
            
            # Проверяем, есть ли дочерние связи
            has_children = rel_key in children_map and len(children_map[rel_key]) > 0
            
            # Компактный дизайн: 3 строки
            indent = "  " * indent_level  # Отступ для вложенности
            
            # Получаем техническое название PK целевой таблицы
            target_pk = "ID"  # По умолчанию
            try:
                if st.session_state.analyzer:
                    pk_columns = st.session_state.analyzer.get_primary_keys(rel['target_table'])
                    if pk_columns:
                        target_pk = pk_columns[0]  # Берем первый PK
            except:
                pass  # Если не удалось получить PK, используем значение по умолчанию
            
            # Формируем технические названия для отображения
            source_tech = f"{rel['source_table']}.{rel['field_name']}"
            target_tech = f"{rel['target_table']}.{target_pk}"
            
            # Строка 1: Нумерация, чекбокс, источник → цель, тип JOIN
            col1, col2, col3, col4 = st.columns([1, 0.5, 5, 2])
            
            with col1:
                # Отображаем нумерацию
                if number_str:
                    st.markdown(f"**{number_str}**")
                else:
                    st.empty()
            
            with col2:
                # Чекбокс для включения/отключения связи
                # Используем on_change callback для обработки изменений
                checkbox_key = f"enabled_{rel_key}"
                
                # Инициализируем ключ чекбокса в session_state из конфигурации
                if checkbox_key not in st.session_state:
                    st.session_state[checkbox_key] = enabled
                else:
                    # Синхронизируем состояние чекбокса с конфигурацией
                    st.session_state[checkbox_key] = enabled
                
                def on_checkbox_change():
                    """Обработчик изменения чекбокса."""
                    current_value = st.session_state[checkbox_key]
                    st.session_state.table_config[rel_key]['enabled'] = current_value
                    # Если отключили, отключаем все дочерние связи
                    if not current_value:
                        disable_children(rel_key)
                
                st.checkbox(
                    "",
                    value=enabled,
                    key=checkbox_key,
                    label_visibility="collapsed",
                    on_change=on_checkbox_change
                )
            
            with col3:
                # Компактное отображение: источник → цель через поле с техническими названиями
                if indent_level > 0:
                    st.markdown(f"{indent}└─ **{target_human}**({target_tech})")
                else:
                    st.markdown(f"**{source_human}**({source_tech}) → **{target_human}**({target_tech})")
            
            with col4:
                # Выбор типа JOIN
                join_type_key = f"join_type_{rel_key}"
                # Получаем значение из session_state, если оно есть, иначе из config
                if join_type_key in st.session_state:
                    current_join_type = st.session_state[join_type_key]
                else:
                    current_join_type = config.get('join_type', 'INNER JOIN')
                    st.session_state[join_type_key] = current_join_type
                
                _manual_join_options = ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"]
                _mj_norm = str(current_join_type or "INNER JOIN").strip().upper()
                try:
                    _mj_idx = _manual_join_options.index(_mj_norm)
                except ValueError:
                    _mj_idx = 0
                join_type = st.selectbox(
                    "Тип JOIN",
                    _manual_join_options,
                    index=_mj_idx,
                    key=join_type_key,
                    disabled=not enabled,
                    label_visibility="collapsed",
                    help=(
                        "Как в мастере: для новых связей по умолчанию INNER JOIN. "
                        "FULL OUTER JOIN может сильно увеличить объём результата."
                    ),
                )
                if join_type == "FULL OUTER JOIN":
                    st.caption(
                        "Предупреждение: FULL OUTER JOIN на больших таблицах 1С может дать очень "
                        "большой результат и медленный запрос."
                    )
                # Обновляем конфигурацию при изменении
                st.session_state.table_config[rel_key]['join_type'] = join_type
            
            # Строка 2: Поле (человеческое и техническое название)
            col1, col2, col3 = st.columns([1, 0.5, 10.5])
            with col1:
                st.empty()  # Пустое место для выравнивания с нумерацией
            with col2:
                st.empty()  # Пустое место для выравнивания с чекбоксом
            with col3:
                # Всегда показываем человеческое название, если оно отличается от технического
                # Проверяем, что человеческое название действительно найдено (не равно техническому)
                if field_human and field_human != rel['field_name']:
                    st.caption(f"{indent}Поле: **{field_human}** (`{rel['field_name']}`)")
                else:
                    # Если человеческое название не найдено, показываем только техническое
                    st.caption(f"{indent}Поле: `{rel['field_name']}`")
            
            # Строка 3: Техническая информация (алиас и таблица)
            col1, col2, col3 = st.columns([1, 0.5, 10.5])
            with col1:
                st.empty()  # Пустое место для выравнивания с нумерацией
            with col2:
                st.empty()  # Пустое место для выравнивания с чекбоксом
            with col3:
                st.caption(f"{indent}Алиас: `{rel['target_alias']}` | Таблица: `{rel['target_table']}`")
            
            # Если есть дочерние связи и текущая связь включена, показываем их
            if has_children and enabled:
                # Подсчитываем количество включенных дочерних связей
                enabled_children_count = sum(
                    1 for child_rel in children_map[rel_key]
                    if st.session_state.table_config.get(child_rel['relationship_key'], {}).get('enabled', True)
                )
                
                # Используем ключ для отслеживания состояния раскрытия
                expand_key = f"expand_{rel_key}"
                if expand_key not in st.session_state:
                    st.session_state[expand_key] = False
                
                # Кнопка для раскрытия/сворачивания дочерних связей
                col1, col2, col3 = st.columns([1, 0.5, 10.5])
                with col1:
                    st.empty()  # Пустое место для выравнивания с нумерацией
                with col2:
                    st.empty()  # Пустое место для выравнивания с чекбоксом
                with col3:
                    expand_label = f"{indent}📂 {'▼' if st.session_state[expand_key] else '▶'} Показать дочерние связи ({enabled_children_count}/{len(children_map[rel_key])})"
                    if st.button(expand_label, key=f"btn_{rel_key}", use_container_width=False):
                        st.session_state[expand_key] = not st.session_state[expand_key]
                        st.rerun()
                
                # Показываем дочерние связи если раскрыто
                if st.session_state[expand_key]:
                    for idx, child_rel in enumerate(children_map[rel_key], start=1):
                        # Формируем новый путь нумерации: добавляем номер текущей дочерней связи
                        child_number_path = number_path + [idx]
                        render_relationship(child_rel, indent_level + 1, child_number_path)
            
            # Разделитель между связями
            if indent_level == 0:
                st.divider()
        
        # Отображаем только связи первого уровня
        st.subheader("Уровень 1")
        for idx, rel in enumerate(root_relationships, start=1):
            render_relationship(rel, indent_level=0, number_path=[idx])
        
        # Кнопка для продолжения генерации
        if st.button("🚀 Сгенерировать VIEW с учетом настроек", type="primary"):
            # Проверяем, что все необходимые данные сохранены
            if (st.session_state.analyzer and 
                st.session_state.relationship_builder and 
                st.session_state.structure_parser and 
                st.session_state.fact_table_db):
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    # Этап 4: Генерация SQL VIEW с учетом настроек (80-95%)
                    status_text.text("Этап 4/5: Генерация SQL VIEW с учетом настроек...")
                    progress_bar.progress(80)
                    
                    view_generator = ViewGenerator(
                        st.session_state.analyzer,
                        st.session_state.relationship_builder,
                        st.session_state.structure_parser,
                        fix_dates=st.session_state.fix_dates_saved
                    )
                    
                    # Используем сохраненные параметры
                    fact_table_input = st.session_state.fact_table_input
                    max_depth_use = st.session_state.max_depth_saved
                    fix_dates_use = st.session_state.fix_dates_saved
                    
                    sql = view_generator.generate_view(
                        fact_table_input, 
                        max_depth=max_depth_use,
                        table_config=st.session_state.table_config
                    )
                    
                    progress_bar.progress(95)
                    
                    # Этап 5: Завершение и сохранение (95-100%)
                    status_text.text("Этап 5/5: Сохранение результата...")
                    
                    # Сохраняем в файл
                    output_path = Path(st.session_state.output_file_saved)
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(output_path, 'w', encoding='utf-8') as f:
                        f.write(sql)
                    
                    # Сохраняем в session state
                    st.session_state.generated_sql = sql
                    
                    progress_bar.progress(100)
                    status_text.text("✅ Генерация завершена успешно!")
                    
                    # Закрываем соединение с БД
                    if st.session_state.analyzer:
                        try:
                            st.session_state.analyzer.close()
                        except:
                            pass
                    
                    st.success("✅ SQL VIEW успешно сгенерирован с учетом ваших настроек!")
                    st.rerun()
                    
                except Exception as e:
                    progress_bar.progress(0)
                    status_text.text(f"❌ Ошибка: {str(e)}")
                    st.error(f"Ошибка при генерации:\n```\n{traceback.format_exc()}\n```")

st.markdown("---")

# Секция результатов
st.header("📋 Результаты")

if st.session_state.get('generated_sql'):
    st.success("✅ SQL VIEW успешно сгенерирован!")
    
    # Предпросмотр SQL
    st.subheader("Предпросмотр SQL")
    st.code(st.session_state.generated_sql, language='sql')
    
    # Кнопка скачивания
    sql_bytes = st.session_state.generated_sql.encode('utf-8')
    st.download_button(
        label="📥 Скачать SQL файл",
        data=sql_bytes,
        file_name=Path(output_file).name if output_file else "view.sql",
        mime="text/sql"
    )
    
    # Информация о файле
    if output_file and Path(output_file).exists():
        file_size = Path(output_file).stat().st_size
        st.info(f"Файл сохранен: {output_file} ({file_size} байт)")
else:
    st.info("Результаты будут отображены здесь после генерации.")

# Секция логов (опционально)
with st.expander("📝 Логи процесса"):
    if st.session_state.get('generated_sql'):
        st.success("Генерация выполнена успешно")
        st.code(st.session_state.generated_sql[:500] + "..." if len(st.session_state.generated_sql) > 500 else st.session_state.generated_sql)
    else:
        st.info("Логи будут отображены после генерации")

