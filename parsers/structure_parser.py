#!/usr/bin/env python3
"""
Парсер структуры данных из файла Структура.docx.
Извлекает маппинги между человеческими и техническими названиями таблиц и полей.
"""

import json
from pathlib import Path
from typing import Dict, Optional
from docx import Document


class StructureParser:
    """
    Класс для парсинга структуры данных из .docx файла.
    """
    
    def __init__(self, docx_path: str):
        """
        Инициализация парсера.
        
        Args:
            docx_path: Путь к файлу .docx со структурой
        """
        self.docx_path = docx_path
        self.table_mapping: Dict[str, str] = {}  # {human_name: db_name}
        self.reverse_table_mapping: Dict[str, str] = {}  # {db_name: human_name}
        self.field_mappings: Dict[str, Dict[str, str]] = {}  # {table_name: {field_name: human_name}}
        self.table_types: Dict[str, str] = {}  # {table_name: type}
        self.field_types: Dict[str, Dict[str, str]] = {}  # {table_name: {field_name: type}}
    
    def parse(self) -> Dict:
        """
        Парсит файл .docx и извлекает структуру данных.
        
        Returns:
            Словарь с результатами парсинга:
            {
                'table_mapping': {human_name: db_name},
                'reverse_table_mapping': {db_name: human_name},
                'field_mappings': {table_name: {field_name: human_name}},
                'table_types': {table_name: type},
                'field_types': {table_name: {field_name: type}}
            }
        """
        doc = Document(self.docx_path)
        current_table = None
        current_table_db = None
        
        for paragraph in doc.paragraphs:
            text = paragraph.text.strip()
            if not text:
                continue
            
            # Проверяем, является ли строка описанием таблицы
            # Формат: "Человеческое название => Техническое название, Тип"
            if '=>' in text:
                parts = text.split('=>')
                if len(parts) >= 2:
                    human_name = parts[0].strip()
                    right_part = parts[1].strip()
                    
                    # Разделяем техническое название и тип
                    if ',' in right_part:
                        db_name_part, table_type = right_part.rsplit(',', 1)
                        db_name = db_name_part.strip()
                        table_type = table_type.strip()
                    else:
                        db_name = right_part
                        table_type = "Основная"
                    
                    # Нормализуем имя таблицы
                    normalized_db_name = self._normalize_table_name(db_name)
                    
                    # Сохраняем маппинг
                    self.table_mapping[human_name] = normalized_db_name
                    self.reverse_table_mapping[normalized_db_name] = human_name
                    self.table_types[normalized_db_name] = table_type
                    
                    # Устанавливаем текущую таблицу
                    current_table = normalized_db_name
                    current_table_db = normalized_db_name
                    
                    # Инициализируем словарь полей для этой таблицы
                    if current_table not in self.field_mappings:
                        self.field_mappings[current_table] = {}
                    if current_table not in self.field_types:
                        self.field_types[current_table] = {}
            
            # Проверяем, является ли строка описанием поля
            # Формат: "ТехническоеПоле (Человеческое название)" или "ТехническоеПоле (Человеческое название, Тип)"
            elif current_table and '(' in text and ')' in text:
                # Извлекаем техническое имя поля и человеческое название
                field_part = text.split('(')[0].strip()
                human_part = text.split('(')[1].split(')')[0].strip()
                
                # Разделяем человеческое название и тип поля (если есть)
                if ',' in human_part:
                    human_name_field, field_type = human_part.rsplit(',', 1)
                    human_name_field = human_name_field.strip()
                    field_type = field_type.strip()
                else:
                    human_name_field = human_part
                    field_type = None
                
                # Сохраняем маппинг поля
                if current_table in self.field_mappings:
                    self.field_mappings[current_table][field_part] = human_name_field
                    if field_type:
                        self.field_types[current_table][field_part] = field_type
        
        return {
            'table_mapping': self.table_mapping,
            'reverse_table_mapping': self.reverse_table_mapping,
            'field_mappings': self.field_mappings,
            'table_types': self.table_types,
            'field_types': self.field_types
        }
    
    def _normalize_table_name(self, db_name: str) -> str:
        """
        Нормализует имя таблицы в БД.
        Добавляет подчеркивание в начало, если его нет.
        Обрабатывает табличные части (точки заменяются на подчеркивания).
        
        Args:
            db_name: Имя таблицы из документа
            
        Returns:
            Нормализованное имя таблицы
        """
        # Если имя содержит точку (например, Reference37172.VT37212), обрабатываем отдельно
        # Табличные части соединяются подчеркиванием: Document653.VT10121 -> _Document653_VT10121
        if '.' in db_name:
            parts = db_name.split('.')
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
        
        # Обычная таблица
        if not db_name.startswith('_'):
            return '_' + db_name
        return db_name
    
    def get_table_db_name(self, human_name: str) -> Optional[str]:
        """
        Получает техническое имя таблицы по человеческому названию.
        
        Args:
            human_name: Человеческое название таблицы
            
        Returns:
            Техническое имя таблицы или None если не найдено
        """
        return self.table_mapping.get(human_name)
    
    def get_table_human_name(self, db_name: str) -> Optional[str]:
        """
        Получает человеческое название таблицы по техническому имени.
        
        Args:
            db_name: Техническое имя таблицы
            
        Returns:
            Человеческое название таблицы или None если не найдено
        """
        # Пробуем найти как есть
        if db_name in self.reverse_table_mapping:
            return self.reverse_table_mapping[db_name]
        
        # Пробуем с нормализацией
        normalized = self._normalize_table_name(db_name)
        return self.reverse_table_mapping.get(normalized)
    
    def get_field_human_name(self, table_name: str, field_name: str) -> Optional[str]:
        """
        Получает человеческое название поля по техническому имени.
        
        Args:
            table_name: Техническое имя таблицы
            field_name: Техническое имя поля
            
        Returns:
            Человеческое название поля или None если не найдено
        """
        # Нормализуем имя таблицы
        normalized_table = self._normalize_table_name(table_name)
        
        # Пробуем найти в маппинге полей
        if normalized_table in self.field_mappings:
            # Пробуем точное совпадение
            if field_name in self.field_mappings[normalized_table]:
                return self.field_mappings[normalized_table][field_name]
            
            # Пробуем без подчеркивания в начале
            if field_name.startswith('_'):
                field_no_underscore = field_name.lstrip('_')
                if field_no_underscore in self.field_mappings[normalized_table]:
                    return self.field_mappings[normalized_table][field_no_underscore]
            
            # Пробуем с подчеркиванием в начале
            if not field_name.startswith('_'):
                field_with_underscore = '_' + field_name
                if field_with_underscore in self.field_mappings[normalized_table]:
                    return self.field_mappings[normalized_table][field_with_underscore]
            
            # Пробуем поиск по частичному совпадению (если поле заканчивается на RRef/RRRef)
            # Ищем поле без суффикса в структуре
            if field_name.endswith('RRRef'):
                base_field = field_name[:-6]  # Убираем 'RRRef'
                if base_field in self.field_mappings[normalized_table]:
                    return self.field_mappings[normalized_table][base_field]
                # Также пробуем без подчеркивания
                base_field_no_underscore = base_field.lstrip('_')
                if base_field_no_underscore in self.field_mappings[normalized_table]:
                    return self.field_mappings[normalized_table][base_field_no_underscore]
            elif field_name.endswith('RRef'):
                base_field = field_name[:-4]  # Убираем 'RRef' (4 символа)
                if base_field in self.field_mappings[normalized_table]:
                    return self.field_mappings[normalized_table][base_field]
                # Также пробуем без подчеркивания
                base_field_no_underscore = base_field.lstrip('_')
                if base_field_no_underscore in self.field_mappings[normalized_table]:
                    return self.field_mappings[normalized_table][base_field_no_underscore]
                # Также пробуем с подчеркиванием (если base_field без подчеркивания)
                if not base_field.startswith('_'):
                    base_field_with_underscore = '_' + base_field
                    if base_field_with_underscore in self.field_mappings[normalized_table]:
                        return self.field_mappings[normalized_table][base_field_with_underscore]
        
        return None
    
    def save_to_json(self, output_path: str = None) -> str:
        """
        Сохраняет результаты парсинга в JSON файл.
        
        Args:
            output_path: Путь к выходному JSON файлу. 
                        Если не указан, используется temp/structure_parsed.json
        
        Returns:
            Путь к сохраненному файлу
        """
        if output_path is None:
            # Используем директорию temp рядом с исходным файлом
            docx_path = Path(self.docx_path)
            temp_dir = docx_path.parent.parent / "temp"
            temp_dir.mkdir(exist_ok=True)
            output_path = str(temp_dir / "structure_parsed.json")
        
        # Подготавливаем данные для сохранения
        data = {
            'source_file': str(self.docx_path),
            'table_mapping': self.table_mapping,
            'reverse_table_mapping': self.reverse_table_mapping,
            'field_mappings': self.field_mappings,
            'table_types': self.table_types,
            'field_types': self.field_types,
            'statistics': {
                'tables_count': len(self.table_mapping),
                'tables_with_fields': len(self.field_mappings),
                'total_fields': sum(len(fields) for fields in self.field_mappings.values())
            }
        }
        
        # Сохраняем в JSON
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        return str(output_file)
















