#!/usr/bin/env python3
"""
Эвристическая фильтрация полей таблиц.
Определяет, какие поля полезны для включения в итоговое представление.
"""

from dataclasses import dataclass
from typing import List, Optional, Set
from db.structure_analyzer import StructureAnalyzer
from parsers.structure_parser import StructureParser


@dataclass
class FieldInfo:
    """Информация о поле с решением о включении/исключении."""
    name: str               # Техническое имя поля
    human_name: str         # Человеческое имя (или техническое, если не найдено)
    data_type: str          # Тип данных
    full_type: str          # Полный тип данных
    include: bool           # Включать ли в SELECT
    reason: str             # Причина включения/исключения


class FieldFilter:
    """
    Фильтрует поля таблиц для включения в итоговое SQL-представление.
    
    Правила:
    - binary(16) → исключить (используются только для JOIN)
    - image, varbinary(max) → исключить (BLOB-данные)
    - Системные поля 1С → зависит от контекста
    - Для таблицы фактов: включать все содержательные поля
    - Для таблиц измерений: только «полезные» (Наименование, Код, реквизиты)
    """

    # Системные поля, которые ВСЕГДА исключаются
    ALWAYS_EXCLUDE: Set[str] = {
        '_Version', 'Version',
        '_Marked', 'Marked',
        '_PredefinedID', 'PredefinedID',
        '_Predefined', 'Predefined',
        '_Folder', 'Folder',
    }

    # Системные поля, полезные для таблицы ФАКТОВ
    FACT_USEFUL: Set[str] = {
        '_Date_Time', 'Date_Time',      # Дата документа
        '_Number', 'Number',            # Номер документа
        '_Posted', 'Posted',            # Проведён ли документ
        '_Description', 'Description',  # Наименование
    }

    # Системные поля, полезные для таблиц ИЗМЕРЕНИЙ (справочников)
    DIMENSION_USEFUL: Set[str] = {
        '_Description', 'Description',  # Наименование
        '_Code', 'Code',                # Код
    }

    def __init__(self, analyzer: StructureAnalyzer, structure_parser: Optional[StructureParser] = None):
        self.analyzer = analyzer
        self.structure_parser = structure_parser

    def filter_fields(
        self,
        table_name: str,
        is_fact_table: bool = False
    ) -> List[FieldInfo]:
        """
        Фильтрует поля таблицы и возвращает список с решениями.
        
        Args:
            table_name: Техническое имя таблицы
            is_fact_table: True если это таблица фактов (более либеральные правила)
            
        Returns:
            Список FieldInfo с решениями о включении каждого поля
        """
        columns = self.analyzer.get_table_columns(table_name)
        result: List[FieldInfo] = []

        for col in columns:
            name = col['name']
            data_type = col['data_type']
            full_type = col.get('full_type', data_type)
            max_length = col.get('max_length')

            # Определяем человеческое имя
            human_name = self._get_human_name(table_name, name)

            # Применяем правила фильтрации
            include, reason = self._decide(
                name, data_type, full_type, max_length,
                is_fact_table, human_name
            )

            result.append(FieldInfo(
                name=name,
                human_name=human_name,
                data_type=data_type,
                full_type=full_type,
                include=include,
                reason=reason
            ))

        return result

    def _decide(
        self,
        name: str,
        data_type: str,
        full_type: str,
        max_length: Optional[int],
        is_fact_table: bool,
        human_name: str
    ) -> tuple:
        """Принимает решение о включении/исключении поля. Возвращает (include, reason)."""

        # Правило F-03: binary(16) → исключить (для JOIN, не для SELECT)
        if data_type in ('binary', 'varbinary') and max_length == 16:
            return False, 'binary(16) — ссылочное поле (используется для JOIN)'

        # Правило F-05: BLOB-данные → исключить
        if data_type in ('image', 'text', 'ntext'):
            return False, f'{data_type} — BLOB-данные, не подходят для представления'
        if data_type in ('varbinary',) and (max_length == -1 or max_length is None or max_length > 16):
            return False, 'varbinary(max) — BLOB-данные'

        # Правило F-02: Системные поля → зависит от контекста
        clean_name = name.rstrip('RRef')
        if name in self.ALWAYS_EXCLUDE or clean_name in self.ALWAYS_EXCLUDE:
            return False, f'Системное поле 1С ({name})'

        # ID поле — первичный ключ, обычно не нужен в плоской таблице для фактов
        if name.upper() == 'ID':
            if is_fact_table:
                return False, 'ID таблицы фактов — первичный ключ, не нужен в плоском представлении'
            else:
                return False, 'ID измерения — первичный ключ'

        # Для таблицы фактов — включаем всё содержательное
        if is_fact_table:
            if name in self.FACT_USEFUL:
                return True, 'Системное поле, полезное для таблицы фактов'
            # Все остальные (числа, даты, строки, реквизиты) — включаем
            return True, 'Поле таблицы фактов'

        # Для таблиц измерений — строгий отбор
        # Полезные системные поля
        if name in self.DIMENSION_USEFUL:
            return True, 'Основное поле измерения (Наименование/Код)'

        # Поля с человеческим названием — скорее всего реквизиты, включаем
        if human_name != name:
            # Есть человеческое название из .docx → это реквизит, включаем
            return True, f'Реквизит измерения ({human_name})'

        # Числовые поля без человеческого имени — возможно метрики, включаем
        if data_type in ('numeric', 'decimal', 'float', 'real', 'money', 'smallmoney',
                         'int', 'bigint', 'smallint', 'tinyint'):
            return True, 'Числовое поле измерения'

        # Поля дат — включаем
        if data_type in ('datetime', 'datetime2', 'date', 'smalldatetime'):
            return True, 'Поле даты измерения'

        # Строковые поля Fld* без человеческого имени — исключаем
        if name.startswith('Fld') or name.startswith('_Fld'):
            return False, 'Поле без человеческого названия (неизвестного назначения)'

        # Всё остальное — включаем с нейтральной причиной
        return True, 'Прочее поле'

    def _get_human_name(self, table_name: str, field_name: str) -> str:
        """Получает человеческое имя поля из StructureParser."""
        if not self.structure_parser:
            return field_name

        # Пробуем разные варианты имени
        variants = [field_name]

        # Без RRef суффикса
        if field_name.endswith('RRef'):
            variants.append(field_name[:-4])
        if field_name.endswith('RRRef'):
            variants.append(field_name[:-5])

        # С/без подчёркивания
        if field_name.startswith('_'):
            variants.append(field_name[1:])
            if field_name.endswith('RRef'):
                variants.append(field_name[1:-4])
        else:
            variants.append('_' + field_name)

        for variant in variants:
            human = self.structure_parser.get_field_human_name(table_name, variant)
            if human:
                return human

        return field_name
