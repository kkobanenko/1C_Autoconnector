#!/usr/bin/env python3
"""
Оценка правомерности выбора таблицы в качестве таблицы фактов.
Анализирует таблицу по набору эвристических признаков и выдаёт рекомендацию.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from db.structure_analyzer import StructureAnalyzer
from parsers.structure_parser import StructureParser


@dataclass
class Warning:
    """Одно предупреждение/наблюдение по результатам оценки."""
    heuristic_id: str       # H-01..H-06
    severity: str           # 'positive' | 'neutral' | 'negative'
    message: str
    weight: int = 0         # Вес для подсчёта (положительный = за, отрицательный = против)


@dataclass
class AssessmentResult:
    """Результат оценки таблицы фактов."""
    score: str              # 'good' | 'maybe' | 'bad'
    score_label: str        # Человекочитаемая метка
    warnings: List[Warning] = field(default_factory=list)
    total_weight: int = 0


class FactTableAssessor:
    """
    Оценивает правомерность выбора таблицы в качестве таблицы фактов.
    
    Эвристики:
    - H-01: Тип таблицы (Document/AccumRg → +, Reference → −)
    - H-02: Наличие числовых/денежных полей
    - H-03: Наличие полей-дат
    - H-04: Количество строк
    - H-05: Наличие табличных частей VT (сильный отрицательный)
    - H-06: Количество внешних ссылок binary(16)
    - H-07: Наличие ссылок на Document (связь с документами)
    """

    def __init__(self, analyzer: StructureAnalyzer, structure_parser: Optional[StructureParser] = None):
        """
        Args:
            analyzer: Экземпляр StructureAnalyzer (должен быть подключён к БД)
            structure_parser: Экземпляр StructureParser (опционально, для человеческих имён)
        """
        self.analyzer = analyzer
        self.structure_parser = structure_parser

    def assess(self, table_name: str) -> AssessmentResult:
        """
        Оценивает таблицу по всем эвристикам.
        
        Args:
            table_name: Техническое имя таблицы (например, _Document653)
            
        Returns:
            AssessmentResult с оценкой и списком предупреждений
        """
        warnings: List[Warning] = []

        # H-01: Тип таблицы
        warnings.append(self._assess_table_type(table_name))

        # H-02: Числовые/денежные поля
        warnings.append(self._assess_numeric_fields(table_name))

        # H-03: Поля-даты
        warnings.append(self._assess_date_fields(table_name))

        # H-04: Количество строк
        warnings.append(self._assess_row_count(table_name))

        # H-05: Табличные части VT
        warnings.append(self._assess_vt_tables(table_name))

        # H-06: Внешние ссылки binary(16)
        warnings.append(self._assess_binary16_fields(table_name))

        # H-07: Ссылки на Document
        warnings.append(self._assess_document_references(table_name))

        # Подсчитываем итоговый вес
        total_weight = sum(w.weight for w in warnings)

        # Определяем итоговую оценку
        if total_weight >= 3:
            score = 'good'
            score_label = '✅ Хорошо подходит как таблица фактов'
        elif total_weight >= 0:
            score = 'maybe'
            score_label = '⚠️ Может подойти, но есть замечания'
        else:
            score = 'bad'
            score_label = '❌ Вероятно не подходит как таблица фактов'

        return AssessmentResult(
            score=score,
            score_label=score_label,
            warnings=warnings,
            total_weight=total_weight
        )

    def _assess_table_type(self, table_name: str) -> Warning:
        """H-01: Оценка по типу таблицы."""
        # Убираем префикс подчёркивания и квадратные скобки для анализа
        clean_name = table_name.strip('_').split('.')[-1].strip('[]')

        if clean_name.startswith('Document'):
            return Warning(
                heuristic_id='H-01',
                severity='positive',
                message=f'Таблица типа Document (документ) — классическая таблица фактов.',
                weight=2
            )
        elif clean_name.startswith('AccumRg') or clean_name.startswith('AccumReg'):
            return Warning(
                heuristic_id='H-01',
                severity='positive',
                message=f'Таблица типа AccumRg (регистр накопления) — содержит движения, хорошо подходит.',
                weight=3
            )
        elif clean_name.startswith('InfoRg') or clean_name.startswith('InfoReg'):
            return Warning(
                heuristic_id='H-01',
                severity='neutral',
                message=f'Таблица типа InfoRg (регистр сведений) — может содержать как факты, так и справочные данные.',
                weight=1
            )
        elif clean_name.startswith('Reference'):
            return Warning(
                heuristic_id='H-01',
                severity='negative',
                message=f'Таблица типа Reference (справочник) — обычно является таблицей измерений, а не фактов.',
                weight=-3
            )
        elif clean_name.startswith('Enum'):
            return Warning(
                heuristic_id='H-01',
                severity='negative',
                message=f'Таблица типа Enum (перечисление) — служебная таблица, не подходит как таблица фактов.',
                weight=-4
            )
        elif '_VT' in clean_name:
            return Warning(
                heuristic_id='H-01',
                severity='positive',
                message=f'Табличная часть (VT) — может хорошо подходить как детализированная таблица фактов.',
                weight=2
            )
        else:
            return Warning(
                heuristic_id='H-01',
                severity='neutral',
                message=f'Тип таблицы не определён по имени.',
                weight=0
            )

    def _assess_numeric_fields(self, table_name: str) -> Warning:
        """H-02: Наличие числовых/денежных полей."""
        try:
            columns = self.analyzer.get_table_columns(table_name)
            numeric_types = {'numeric', 'decimal', 'float', 'real', 'money', 'smallmoney', 'int', 'bigint', 'smallint', 'tinyint'}
            numeric_fields = [c for c in columns if c['data_type'] in numeric_types]

            # Исключаем системные поля (позицию и т.п.)
            meaningful_numeric = [c for c in numeric_fields
                                  if not c['name'].startswith('_') or 'Fld' in c['name']]

            if len(meaningful_numeric) >= 3:
                return Warning(
                    heuristic_id='H-02',
                    severity='positive',
                    message=f'Найдено {len(meaningful_numeric)} числовых полей (метрики/суммы).',
                    weight=2
                )
            elif len(meaningful_numeric) >= 1:
                return Warning(
                    heuristic_id='H-02',
                    severity='neutral',
                    message=f'Найдено {len(meaningful_numeric)} числовое(ых) поле(й).',
                    weight=1
                )
            else:
                return Warning(
                    heuristic_id='H-02',
                    severity='negative',
                    message='Числовые поля (метрики/суммы) не найдены.',
                    weight=-1
                )
        except Exception:
            return Warning(
                heuristic_id='H-02',
                severity='neutral',
                message='Не удалось проанализировать числовые поля.',
                weight=0
            )

    def _assess_date_fields(self, table_name: str) -> Warning:
        """H-03: Наличие полей-дат."""
        try:
            columns = self.analyzer.get_table_columns(table_name)
            date_types = {'datetime', 'datetime2', 'date', 'smalldatetime'}
            date_fields = [c for c in columns if c['data_type'] in date_types]

            if len(date_fields) >= 1:
                return Warning(
                    heuristic_id='H-03',
                    severity='positive',
                    message=f'Найдено {len(date_fields)} поле(й) с датами — временная ось фактов.',
                    weight=2
                )
            else:
                return Warning(
                    heuristic_id='H-03',
                    severity='negative',
                    message='Поля с датами не найдены.',
                    weight=-1
                )
        except Exception:
            return Warning(
                heuristic_id='H-03',
                severity='neutral',
                message='Не удалось проанализировать поля дат.',
                weight=0
            )

    def _assess_row_count(self, table_name: str) -> Warning:
        """H-04: Количество строк."""
        try:
            row_count = self.analyzer.get_table_row_count(table_name)

            if row_count >= 10000:
                return Warning(
                    heuristic_id='H-04',
                    severity='positive',
                    message=f'Таблица содержит {row_count:,} строк — характерно для таблицы фактов.',
                    weight=2
                )
            elif row_count >= 100:
                return Warning(
                    heuristic_id='H-04',
                    severity='neutral',
                    message=f'Таблица содержит {row_count:,} строк.',
                    weight=0
                )
            else:
                return Warning(
                    heuristic_id='H-04',
                    severity='negative',
                    message=f'Таблица содержит всего {row_count:,} строк — больше похоже на справочник.',
                    weight=-1
                )
        except Exception:
            return Warning(
                heuristic_id='H-04',
                severity='neutral',
                message='Не удалось определить количество строк.',
                weight=0
            )

    def _assess_vt_tables(self, table_name: str) -> Warning:
        """H-05: Наличие табличных частей VT (сильный отрицательный)."""
        try:
            vt_tables = self.analyzer.get_vt_tables(table_name)

            if vt_tables:
                vt_list = ', '.join(vt_tables[:5])
                suffix = f' и ещё {len(vt_tables) - 5}' if len(vt_tables) > 5 else ''
                return Warning(
                    heuristic_id='H-05',
                    severity='negative',
                    message=(
                        f'Найдено {len(vt_tables)} табличных частей: {vt_list}{suffix}. '
                        f'Таблица является заголовком документа. '
                        f'Рекомендуется вместо неё выбрать табличную часть как таблицу фактов.'
                    ),
                    weight=-4
                )
            else:
                return Warning(
                    heuristic_id='H-05',
                    severity='positive',
                    message='Табличные части (VT) не обнаружены.',
                    weight=1
                )
        except Exception:
            return Warning(
                heuristic_id='H-05',
                severity='neutral',
                message='Не удалось проверить наличие табличных частей.',
                weight=0
            )

    def _assess_binary16_fields(self, table_name: str) -> Warning:
        """H-06: Количество внешних ссылок binary(16)."""
        try:
            binary_fields = self.analyzer.get_binary16_fields(table_name)
            # Исключаем ID (PK) — он тоже binary(16), но не ссылка
            fk_fields = [f for f in binary_fields if f.upper() != 'ID']

            if len(fk_fields) >= 5:
                return Warning(
                    heuristic_id='H-06',
                    severity='positive',
                    message=f'Найдено {len(fk_fields)} ссылочных полей — факты обычно ссылаются на много измерений.',
                    weight=2
                )
            elif len(fk_fields) >= 2:
                return Warning(
                    heuristic_id='H-06',
                    severity='neutral',
                    message=f'Найдено {len(fk_fields)} ссылочных полей.',
                    weight=1
                )
            else:
                return Warning(
                    heuristic_id='H-06',
                    severity='neutral',
                    message=f'Мало ссылочных полей ({len(fk_fields)}).',
                    weight=0
                )
        except Exception:
            return Warning(
                heuristic_id='H-06',
                severity='neutral',
                message='Не удалось проанализировать ссылочные поля.',
                weight=0
            )

    def _assess_document_references(self, table_name: str) -> Warning:
        """H-07: Наличие ссылок на таблицы Document."""
        try:
            # Быстрая проверка: если GUID индекс ещё не построен — пропускаем,
            # чтобы не тратить минуты на его построение при оценке.
            if self.analyzer._guid_to_table_cache is None:
                return Warning(
                    heuristic_id='H-07',
                    severity='neutral',
                    message='GUID-индекс ещё не построен. Критерий будет доступен после построения графа связей.',
                    weight=0
                )

            binary_fields = self.analyzer.get_binary16_fields(table_name)
            fk_fields = [f for f in binary_fields
                         if f.upper() != 'ID' and not f.endswith('IDRRef')]

            # Ограничиваем количество проверяемых полей
            fk_fields = fk_fields[:8]

            document_refs = []
            self.analyzer.connect()
            schema, table = self.analyzer._parse_table_name(
                self.analyzer._normalize_table_name(table_name)
            )

            guid_index = self.analyzer._guid_to_table_cache

            for fld in fk_fields:
                try:
                    cursor = self.analyzer.conn.cursor()
                    cursor.execute(f"""
                        SELECT TOP 3 [{fld}]
                        FROM [{schema}].[{table}]
                        WHERE [{fld}] IS NOT NULL
                        AND [{fld}] != 0x00000000000000000000000000000000
                    """)
                    rows = cursor.fetchall()
                    cursor.close()

                    for row in rows:
                        guid_val = row[0]
                        if not guid_val:
                            continue
                        if isinstance(guid_val, (bytes, bytearray)) and len(guid_val) == 16:
                            target = guid_index.get(bytes(guid_val))
                            if target:
                                clean_target = target.strip('_').split('.')[-1].strip('[]')
                                if clean_target.startswith('Document'):
                                    human = (self.structure_parser.get_table_human_name(target)
                                             if self.structure_parser else None)
                                    ref_label = f"{human} ({target})" if human else target
                                    if ref_label not in document_refs:
                                        document_refs.append(ref_label)
                                    break
                except Exception:
                    continue

            if len(document_refs) >= 2:
                refs_str = ', '.join(document_refs[:5])
                return Warning(
                    heuristic_id='H-07',
                    severity='positive',
                    message=f'Найдены ссылки на {len(document_refs)} документов: {refs_str}. Характерно для таблицы фактов.',
                    weight=2
                )
            elif len(document_refs) == 1:
                return Warning(
                    heuristic_id='H-07',
                    severity='positive',
                    message=f'Найдена ссылка на документ: {document_refs[0]}.',
                    weight=1
                )
            else:
                return Warning(
                    heuristic_id='H-07',
                    severity='neutral',
                    message='Ссылки на документы не обнаружены.',
                    weight=0
                )
        except Exception:
            return Warning(
                heuristic_id='H-07',
                severity='neutral',
                message='Не удалось проверить ссылки на документы.',
                weight=0
            )
