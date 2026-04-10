# План: метаданные конфигураций + сквозной поиск

**Основной файл реализации:** [pages/1_🚀_Генератор_VIEW.py](../pages/1_🚀_Генератор_VIEW.py)  
**Генератор SQL:** [generators/view_generator.py](../generators/view_generator.py) — **без изменений** (`view_name` уже в сигнатуре).

---

## TL;DR

- Метаданные при сохранении: **name**, **view_name**, **description**, **tags** → в `metadata` файла `cfg_*.json`.
- **view_name** → в `generate_view_from_relationships`, в имя файла конфига и дефолтное имя `.sql`.
- SQL-архив (`sql_*.json`): добавить **description**, **tags**.
- Секция 9: обновлённые подписи, восстановление полей в секции 11 при загрузке, кнопка **✏️** (inline-редактирование JSON).
- **Сквозной поиск** по всем `cfg_*.json` и `sql_*.json` без фильтра по текущему графу.

---

## Фаза A: поля ввода при сохранении (секция 11)

| Шаг | Действие |
|-----|----------|
| A1 | Перед кнопкой «💾 Сохранить текущую конфигурацию»: `text_input` name, view_name; `text_area` description; `text_input` tags (через запятую → `list[str]` lower/strip). Ключи: `_cfg_save_name`, `_cfg_save_view_name`, `_cfg_save_description`, `_cfg_save_tags`. Дефолты из human name таблицы фактов и авто-`vw_...`. |
| A2 | Валидация `view_name`: regex `^[\w]+$` с `re.UNICODE`; при невалидном — `st.warning` (решить: блокировать сохранение или только предупреждать). |
| A3 | В `config_data['metadata']` добавить `name`, `view_name`, `description`, `tags`; остальные поля metadata сохранить. |
| A4 | Имя файла: `cfg_{sanitized_view_name}_{timestamp}.json` с fallback на текущий `safe_name`. |

---

## Фаза B: генерация SQL (секция 12)

| Шаг | Действие |
|-----|----------|
| B1 | Передать в `generate_view_from_relationships` аргумент `view_name=` из `st.session_state.get('_cfg_save_view_name')` (пустая строка → `None`). |
| B2 | `default_filename` для вывода: если задан view_name → `{view_name}.sql`, иначе текущая логика. |
| B3 | В `meta` при записи `sql_*.json` добавить `description`, `tags` из session_state (аналогично парсингу тегов). |

---

## Фаза C: отображение и загрузка (секция 9)

| Шаг | Действие |
|-----|----------|
| C1 | Label кнопки «📌»: приоритет `metadata.name`, краткое описание, теги `[tag]`. |
| C2 | После загрузки конфига: заполнить `_cfg_save_name`, `_cfg_save_view_name`, `_cfg_save_description`, `_cfg_save_tags` из `full_data['metadata']`. |

---

## Фаза D: inline-редактирование

| Шаг | Действие |
|-----|----------|
| D1 | Колонки строки: `col_load`, `col_edit`, `col_info`, `col_del`. |
| D2 | Кнопка ✏️ переключает `st.session_state['_pre_cfg_edit_{i}']`. |
| D3 | При открытии — `st.form`: поля метаданных; submit → read/modify/write JSON, сброс флага, `rerun`. |

---

## Фаза E: сквозной поиск

| Шаг | Действие |
|-----|----------|
| E1 | Под заголовком секции 9: `st.text_input` поиска (отдельный key). |
| E2 | Если запрос непустой: сканировать все `configs/cfg_*.json` и `sql/sql_*.json`; подстрока в lower-case по полям из черновика; два expander’а (конфиги / SQL). |
| E3 | Загрузка конфига из результатов — та же логика, что у 📌, включая восстановление `_cfg_save_*`. |
| E4 | Для SQL: `download_button` по `sql_file` в каталоге `sql/`. |
| E5 | Если запрос пустой — **весь** существующий UI секции 9 в ветке `else` (большой рефакторинг отступов). |

**Замечание:** загрузка конфига с другого графа — как сейчас; при необходимости позже предупреждать по `graph_hash` в metadata.

---

## Обратная совместимость

1. Старые JSON без новых полей — пустые поля в UI, ✏️ позволяет дополнить.  
2. Старые SQL-meta без description/tags — поиск по `human_name` / fact.  
3. Не удалять и не переименовывать существующие ключи JSON.  

---

## Проверка

1. Сохранить конфиг с метаданными → в JSON есть `name`, `view_name`, `description`, `tags`.  
2. В списке секции 9 видны имя, описание, теги.  
3. Загрузить → секция 11 заполнена.  
4. SQL использует переданный `view_name`.  
5. SQL-archive JSON содержит `description` и `tags`.  
6. ✏️ обновляет файл на диске.  
7. Поиск находит по тегу/описанию; пустой поиск — прежний интерфейс.  
8. Старый конфиг без полей отображается корректно.  

---

## После реализации

- Кратко обновить [PRD.md](PRD.md) (формат конфигурации, секции 9/11).

---

## Чеклист задач

- [ ] Фаза A: виджеты, metadata, имя файла cfg  
- [ ] Фаза B: `view_name`, default_filename, meta SQL-архива  
- [ ] Фаза C: labels + восстановление `_cfg_save_*`  
- [ ] Фаза D: ✏️ + form  
- [ ] Фаза E: поиск + `else` для текущего UI  
- [ ] PRD  
