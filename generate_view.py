#!/usr/bin/env python3
"""
Главный модуль для генерации SQL VIEW через CLI интерфейс.
"""

import argparse
import sys
from pathlib import Path

from parsers.structure_parser import StructureParser
from db.structure_analyzer import StructureAnalyzer
from builders.relationship_builder import RelationshipBuilder
from generators.view_generator import ViewGenerator
import config


def generate_view(
    fact_table: str,
    structure_file: str = None,
    output_file: str = None,
    max_depth: int = 5,
    fix_dates: bool = True,
    connection_string: str = None
) -> str:
    """
    Генерирует SQL VIEW для указанной таблицы.
    
    Args:
        fact_table: Имя таблицы фактов (человеческое или техническое)
        structure_file: Путь к файлу структуры .docx
        output_file: Путь к выходному SQL файлу
        max_depth: Максимальный уровень рекурсии
        fix_dates: Исправлять ли искаженные даты
        connection_string: Строка подключения к БД
        
    Returns:
        Сгенерированный SQL скрипт
    """
    # Определяем путь к файлу структуры
    if structure_file is None:
        structure_file = str(config.DEFAULT_STRUCTURE_FILE)
    
    # Проверяем существование файла структуры
    if not Path(structure_file).exists():
        raise FileNotFoundError(f"Файл структуры не найден: {structure_file}")
    
    print(f"Загрузка структуры из: {structure_file}")
    # Парсим структуру
    structure_parser = StructureParser(structure_file)
    structure_data = structure_parser.parse()
    print(f"Загружено таблиц: {len(structure_data['table_mapping'])}")
    
    # Сохраняем результаты парсинга в JSON
    json_path = structure_parser.save_to_json()
    print(f"Результаты парсинга сохранены в: {json_path}")
    
    print("Подключение к базе данных...")
    # Создаем анализатор структуры БД
    analyzer = StructureAnalyzer(connection_string)
    analyzer.connect()
    
    try:
        print("Анализ структуры БД...")
        # Определяем техническое имя таблицы
        fact_table_db = None
        if fact_table.startswith('_'):
            fact_table_db = fact_table
        else:
            fact_table_db = structure_parser.get_table_db_name(fact_table) or fact_table
        
        # Строим граф связей только для нужной таблицы для ускорения
        relationship_builder = RelationshipBuilder(analyzer)
        relationship_builder.build_relationship_graph([fact_table_db])
        print("Граф связей построен")
        
        print(f"Генерация VIEW для таблицы: {fact_table}")
        # Генерируем VIEW
        view_generator = ViewGenerator(
            analyzer,
            relationship_builder,
            structure_parser,
            fix_dates=fix_dates
        )
        
        sql = view_generator.generate_view(fact_table, max_depth=max_depth)
        
        # Сохраняем в файл если указан
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(sql)
            print(f"SQL скрипт сохранен в: {output_file}")
        else:
            print("\n=== Сгенерированный SQL ===\n")
            print(sql)
        
        return sql
    
    finally:
        analyzer.close()


def main():
    """Главная функция CLI."""
    parser = argparse.ArgumentParser(
        description='Генератор SQL VIEW для таблиц 1С с рекурсивными JOIN'
    )
    
    parser.add_argument(
        'fact_table',
        help='Название таблицы фактов (человеческое или техническое)'
    )
    
    parser.add_argument(
        '--structure-file',
        type=str,
        default=None,
        help=f'Путь к файлу структуры .docx (по умолчанию: {config.DEFAULT_STRUCTURE_FILE})'
    )
    
    parser.add_argument(
        '--output',
        '-o',
        type=str,
        default=None,
        help='Путь к выходному SQL файлу (если не указан, выводится в stdout)'
    )
    
    parser.add_argument(
        '--max-depth',
        type=int,
        default=5,
        help='Максимальный уровень рекурсии (по умолчанию: 5)'
    )
    
    parser.add_argument(
        '--no-fix-dates',
        action='store_true',
        help='Не исправлять искаженные даты'
    )
    
    parser.add_argument(
        '--connection-string',
        type=str,
        default=None,
        help='Строка подключения к БД (если не указана, используется из config)'
    )
    
    args = parser.parse_args()
    
    try:
        generate_view(
            fact_table=args.fact_table,
            structure_file=args.structure_file,
            output_file=args.output,
            max_depth=args.max_depth,
            fix_dates=not args.no_fix_dates,
            connection_string=args.connection_string
        )
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

