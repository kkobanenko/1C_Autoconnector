#!/usr/bin/env python3
"""
Скрипт для автоматического увеличения версии плагина.
Увеличивает патч-версию (1.0.0 -> 1.0.1) в config.py.
"""

import re
from pathlib import Path

def bump_version():
    """Увеличивает патч-версию в config.py."""
    config_path = Path(__file__).parent / "config.py"
    
    if not config_path.exists():
        print(f"❌ Файл {config_path} не найден!")
        return False
    
    # Читаем содержимое файла
    with open(config_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Ищем строку с версией
    version_pattern = r'VERSION\s*=\s*"(\d+)\.(\d+)\.(\d+)"'
    match = re.search(version_pattern, content)
    
    if not match:
        print("❌ Не найдена переменная VERSION в config.py!")
        return False
    
    # Извлекаем компоненты версии
    major = int(match.group(1))
    minor = int(match.group(2))
    patch = int(match.group(3))
    
    old_version = f"{major}.{minor}.{patch}"
    
    # Увеличиваем патч-версию (1.0.0 -> 1.0.1)
    patch += 1
    
    new_version = f"{major}.{minor}.{patch}"
    
    # Заменяем версию в содержимом
    new_content = re.sub(
        version_pattern,
        f'VERSION = "{new_version}"',
        content
    )
    
    # Записываем обратно
    with open(config_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"✅ Версия обновлена: {old_version} -> {new_version}")
    return True

if __name__ == "__main__":
    bump_version()

