#!/usr/bin/env python3
"""
Визуализация GUID-индекса и графа связей между таблицами.
- render_guid_index: горизонтальная столбчатая диаграмма GUID по таблицам
- render_relationship_graph: сетевой граф с узлами-таблицами и рёбрами-связями
"""

from typing import Dict, Optional
from pathlib import Path
from collections import Counter
from datetime import datetime

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


# Цветовая палитра по типам таблиц 1С
TYPE_COLORS = {
    'Document':  '#4FC3F7',  # голубой
    'Reference': '#81C784',  # зелёный
    'AccumRg':   '#FFB74D',  # оранжевый
    'InfoRg':    '#CE93D8',  # фиолетовый
    'Enum':      '#F06292',  # розовый
    'VT':        '#90A4AE',  # серый
    'Другое':    '#B0BEC5',  # светло-серый
}


def classify_table(name: str) -> str:
    """Определяет тип таблицы 1С по имени."""
    clean = name.lstrip('_')
    if '_VT' in clean:
        return 'VT'
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


def render_guid_index(
    guid_index: Dict[bytes, str],
    output_path: str,
    title: str = "GUID-индекс: распределение по таблицам",
    top_n: int = 40,
    structure_parser=None,
    metadata: Optional[Dict] = None,
    dpi: int = 150
) -> str:
    """
    Рисует визуализацию GUID-индекса и сохраняет в файл.
    
    Args:
        guid_index: Словарь {guid_bytes: table_name}
        output_path: Путь для сохранения (jpg/png)
        title: Заголовок диаграммы
        top_n: Показать топ N таблиц
        structure_parser: Парсер структуры для человеческих имён
        metadata: Метаданные индекса для отображения
        dpi: Разрешение
        
    Returns:
        Путь к сохранённому файлу
    """
    # 1. Считаем количество GUID на таблицу
    table_counts = Counter(guid_index.values())
    
    # 2. Сортируем по убыванию, берём top_n
    sorted_tables = table_counts.most_common(top_n)
    
    if not sorted_tables:
        raise ValueError("GUID-индекс пуст")
    
    # 3. Подготовка данных
    tables = [t for t, _ in reversed(sorted_tables)]  # снизу вверх
    counts = [c for _, c in reversed(sorted_tables)]
    types = [classify_table(t) for t in tables]
    colors = [TYPE_COLORS.get(tp, TYPE_COLORS['Другое']) for tp in types]
    
    # Человеческие имена
    labels = []
    for t in tables:
        human = None
        if structure_parser:
            try:
                human = structure_parser.get_table_human_name(t)
            except Exception:
                pass
        if human:
            labels.append(f"{human}\n({t})")
        else:
            labels.append(t)
    
    # 4. Статистика по типам
    type_counter = Counter()
    for table_name, count in table_counts.items():
        tp = classify_table(table_name)
        type_counter[tp] += count
    
    total_guids = sum(table_counts.values())
    total_tables = len(table_counts)
    
    # 5. Рисуем
    bar_height = 0.6
    fig_height = max(8, len(tables) * 0.35 + 3)
    fig_width = 16
    
    fig, (ax_main, ax_stats) = plt.subplots(
        1, 2,
        figsize=(fig_width, fig_height),
        gridspec_kw={'width_ratios': [4, 1]},
        facecolor='#1E1E2E'
    )
    
    # ── Основная диаграмма ──
    ax_main.set_facecolor('#1E1E2E')
    bars = ax_main.barh(
        range(len(tables)), counts,
        height=bar_height,
        color=colors,
        edgecolor='#2A2A3E',
        linewidth=0.5
    )
    
    # Подписи значений на барах
    max_count = max(counts) if counts else 1
    for i, (bar, count) in enumerate(zip(bars, counts)):
        x_pos = bar.get_width() + max_count * 0.01
        ax_main.text(
            x_pos, i, f" {count:,}",
            va='center', ha='left',
            color='#CDD6F4', fontsize=8, fontweight='bold'
        )
    
    ax_main.set_yticks(range(len(tables)))
    ax_main.set_yticklabels(labels, fontsize=7, color='#CDD6F4')
    ax_main.set_xlabel('Количество GUID', color='#CDD6F4', fontsize=10)
    ax_main.set_title(title, color='#CDD6F4', fontsize=14, fontweight='bold', pad=15)
    ax_main.tick_params(axis='x', colors='#6C7086', labelsize=8)
    ax_main.spines['top'].set_visible(False)
    ax_main.spines['right'].set_visible(False)
    ax_main.spines['bottom'].set_color('#45475A')
    ax_main.spines['left'].set_color('#45475A')
    ax_main.set_xlim(0, max_count * 1.15)
    
    # Сетка
    ax_main.xaxis.grid(True, linestyle='--', alpha=0.2, color='#6C7086')
    ax_main.set_axisbelow(True)
    
    # ── Панель статистики (справа) ──
    ax_stats.set_facecolor('#1E1E2E')
    ax_stats.axis('off')
    
    stats_y = 0.95
    dy = 0.045
    
    # Заголовок статистики
    ax_stats.text(0.1, stats_y, "📊 Статистика", fontsize=12,
                  fontweight='bold', color='#CDD6F4', transform=ax_stats.transAxes)
    stats_y -= dy * 1.5
    
    ax_stats.text(0.1, stats_y, f"Всего GUID: {total_guids:,}",
                  fontsize=9, color='#A6ADC8', transform=ax_stats.transAxes)
    stats_y -= dy
    ax_stats.text(0.1, stats_y, f"Таблиц: {total_tables}",
                  fontsize=9, color='#A6ADC8', transform=ax_stats.transAxes)
    stats_y -= dy
    
    if len(table_counts) > top_n:
        ax_stats.text(0.1, stats_y, f"Показаны: TOP {top_n}",
                      fontsize=9, color='#A6ADC8', transform=ax_stats.transAxes)
        stats_y -= dy
    
    # Метаданные
    if metadata:
        stats_y -= dy * 0.5
        ax_stats.text(0.1, stats_y, "─── Индекс ───",
                      fontsize=9, color='#6C7086', transform=ax_stats.transAxes)
        stats_y -= dy
        
        if metadata.get('host'):
            ax_stats.text(0.1, stats_y, f"Сервер: {metadata['host']}",
                          fontsize=8, color='#A6ADC8', transform=ax_stats.transAxes)
            stats_y -= dy
        if metadata.get('database'):
            ax_stats.text(0.1, stats_y, f"БД: {metadata['database']}",
                          fontsize=8, color='#A6ADC8', transform=ax_stats.transAxes)
            stats_y -= dy
        if metadata.get('built_at'):
            try:
                dt = datetime.fromisoformat(metadata['built_at'])
                built_str = dt.strftime("%d.%m.%Y %H:%M")
            except Exception:
                built_str = metadata['built_at']
            ax_stats.text(0.1, stats_y, f"Построен: {built_str}",
                          fontsize=8, color='#A6ADC8', transform=ax_stats.transAxes)
            stats_y -= dy
    
    # Распределение по типам
    stats_y -= dy * 0.5
    ax_stats.text(0.1, stats_y, "─── По типам ───",
                  fontsize=9, color='#6C7086', transform=ax_stats.transAxes)
    stats_y -= dy
    
    for tp in ['Document', 'Reference', 'AccumRg', 'InfoRg', 'Enum', 'VT', 'Другое']:
        cnt = type_counter.get(tp, 0)
        if cnt == 0:
            continue
        pct = cnt / total_guids * 100 if total_guids else 0
        color = TYPE_COLORS.get(tp, '#B0BEC5')
        ax_stats.text(0.1, stats_y, "■", fontsize=12, color=color,
                      transform=ax_stats.transAxes)
        ax_stats.text(0.18, stats_y, f"{tp}: {cnt:,} ({pct:.0f}%)",
                      fontsize=8, color='#A6ADC8', transform=ax_stats.transAxes)
        stats_y -= dy
    
    # Легенда типов (внизу основной диаграммы)
    legend_patches = [
        mpatches.Patch(color=c, label=t)
        for t, c in TYPE_COLORS.items()
        if type_counter.get(t, 0) > 0
    ]
    if legend_patches:
        ax_main.legend(
            handles=legend_patches, loc='lower right',
            fontsize=8, framealpha=0.3, facecolor='#313244',
            edgecolor='#45475A', labelcolor='#CDD6F4'
        )
    
    # Timestamp
    fig.text(0.99, 0.01, datetime.now().strftime("Сгенерировано: %d.%m.%Y %H:%M"),
             fontsize=7, color='#6C7086', ha='right', va='bottom')
    
    plt.tight_layout()
    
    # 6. Сохраняем
    output_path = str(output_path)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=dpi, bbox_inches='tight',
                facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close(fig)
    
    print(f"GUID-индекс визуализирован: {output_path}")
    return output_path


def render_relationship_graph(
    relationships: list,
    fact_table: str,
    output_path: str,
    title: str = "Граф связей",
    structure_parser=None,
    dpi: int = 150,
    node_field_counts: dict | None = None
) -> str:
    """
    Рисует граф связей между таблицами в виде сетевой диаграммы.
    
    Узлы = таблицы (цвет по типу, размер по числу связей).
    Рёбра = связи через binary(16) поля (подписаны именами полей).
    Таблица фактов выделена крупным размером и жирной рамкой.
    
    Args:
        relationships: Список связей из collect_all_relationships
        fact_table: Имя таблицы фактов (центральный узел)
        output_path: Путь для сохранения (jpg/png)
        title: Заголовок
        structure_parser: Парсер для человеческих имён
        dpi: Разрешение
        node_field_counts: Опционально {table_name: (selected, total)} — подпись полей в узле

    Returns:
        Путь к сохранённому файлу
    """
    import networkx as nx
    from matplotlib.patches import FancyArrowPatch
    import numpy as np
    
    # ─── 1. Строим граф ───────────────────────────────────────────────────
    G = nx.DiGraph()
    
    # Добавляем таблицу фактов
    G.add_node(fact_table, node_type='fact')
    
    edge_labels = {}
    for rel in relationships:
        src = rel.get('source_table', '')
        tgt = rel.get('target_table', '')
        field = rel.get('field_name', '')
        direction = rel.get('direction', 'forward')
        depth = rel.get('depth', 1)
        
        if not src or not tgt:
            continue
        
        G.add_node(src)
        G.add_node(tgt)
        
        if direction == 'reverse':
            G.add_edge(tgt, src, field=field, direction='reverse', depth=depth)
            edge_labels[(tgt, src)] = field
        else:
            G.add_edge(src, tgt, field=field, direction='forward', depth=depth)
            edge_labels[(src, tgt)] = field
    
    if len(G.nodes()) == 0:
        raise ValueError("Граф пуст — нет связей")
    
    # ─── 2. Расположение (layout) ─────────────────────────────────────────
    # Используем spring layout с фиксированной позицией факт-таблицы в центре
    fixed_pos = {fact_table: (0, 0)}
    
    try:
        pos = nx.spring_layout(
            G, k=2.5, iterations=80,
            pos=fixed_pos, fixed=[fact_table],
            seed=42
        )
    except Exception:
        pos = nx.spring_layout(G, k=2.5, seed=42)
    
    # ─── 3. Подготовка визуальных параметров ──────────────────────────────
    node_colors = []
    node_sizes = []
    node_edge_colors = []
    node_edge_widths = []
    labels = {}
    
    for node in G.nodes():
        tp = classify_table(node)
        color = TYPE_COLORS.get(tp, TYPE_COLORS['Другое'])
        
        # Человеческое имя
        human = None
        if structure_parser:
            try:
                human = structure_parser.get_table_human_name(node)
            except Exception:
                pass
        
        field_suffix = ""
        if node_field_counts and node in node_field_counts:
            sel, tot = node_field_counts[node]
            field_suffix = f" ({sel}/{tot})"

        if node == fact_table:
            labels[node] = f"★ {human or node}{field_suffix}"
            node_sizes.append(3000)
            node_edge_colors.append('#FFD700')  # золотая рамка
            node_edge_widths.append(3)
        else:
            short_name = human if human else node.lstrip('_')
            # Сокращаем длинные имена
            if len(short_name) > 20:
                short_name = short_name[:18] + '…'
            labels[node] = short_name + field_suffix
            deg = G.degree(node)
            node_sizes.append(800 + deg * 200)
            node_edge_colors.append('#45475A')
            node_edge_widths.append(1)
        
        node_colors.append(color)
    
    # ─── 4. Рисуем ────────────────────────────────────────────────────────
    n_nodes = len(G.nodes())
    fig_size = max(14, min(24, n_nodes * 0.8))
    fig, ax = plt.subplots(figsize=(fig_size, fig_size * 0.75), facecolor='#1E1E2E')
    ax.set_facecolor('#1E1E2E')
    
    # Рёбра
    forward_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get('direction') != 'reverse']
    reverse_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get('direction') == 'reverse']
    
    # Прямые связи — сплошная линия
    if forward_edges:
        nx.draw_networkx_edges(
            G, pos, edgelist=forward_edges, ax=ax,
            edge_color='#89B4FA', width=1.5,
            alpha=0.7, arrows=True,
            arrowsize=15, arrowstyle='-|>',
            connectionstyle='arc3,rad=0.1'
        )
    
    # Обратные связи — пунктирная линия
    if reverse_edges:
        nx.draw_networkx_edges(
            G, pos, edgelist=reverse_edges, ax=ax,
            edge_color='#F38BA8', width=1.5,
            alpha=0.6, arrows=True,
            arrowsize=15, arrowstyle='-|>',
            style='dashed',
            connectionstyle='arc3,rad=0.15'
        )
    
    # Узлы
    nx.draw_networkx_nodes(
        G, pos, ax=ax,
        node_color=node_colors,
        node_size=node_sizes,
        edgecolors=node_edge_colors,
        linewidths=node_edge_widths,
        alpha=0.9
    )
    
    # Подписи узлов
    nx.draw_networkx_labels(
        G, pos, labels, ax=ax,
        font_size=7, font_color='#CDD6F4',
        font_weight='bold'
    )
    
    # Подписи рёбер (имена полей)
    # Сокращаем длинные имена полей
    short_edge_labels = {}
    for (u, v), field in edge_labels.items():
        short = field
        if len(short) > 15:
            short = short[:13] + '…'
        short_edge_labels[(u, v)] = short
    
    nx.draw_networkx_edge_labels(
        G, pos, short_edge_labels, ax=ax,
        font_size=5, font_color='#6C7086',
        alpha=0.8, rotate=True
    )
    
    # ─── 5. Оформление ────────────────────────────────────────────────────
    ax.set_title(title, color='#CDD6F4', fontsize=16, fontweight='bold', pad=20)
    ax.axis('off')
    
    # Легенда
    legend_items = []
    used_types = set(classify_table(n) for n in G.nodes())
    for tp in ['Document', 'Reference', 'AccumRg', 'InfoRg', 'Enum', 'VT', 'Другое']:
        if tp in used_types:
            legend_items.append(
                mpatches.Patch(color=TYPE_COLORS[tp], label=tp)
            )
    # Добавляем элементы для прямых/обратных связей
    legend_items.append(plt.Line2D([0], [0], color='#89B4FA', linewidth=2, label='↓ Прямая связь'))
    legend_items.append(plt.Line2D([0], [0], color='#F38BA8', linewidth=2, linestyle='--', label='↑ Обратная связь'))
    legend_items.append(plt.Line2D([0], [0], marker='*', color='#FFD700', markersize=12,
                                    linestyle='', label='★ Таблица фактов'))
    
    ax.legend(
        handles=legend_items, loc='upper left',
        fontsize=8, framealpha=0.5, facecolor='#313244',
        edgecolor='#45475A', labelcolor='#CDD6F4'
    )
    
    # Статистика
    n_forward = len(forward_edges)
    n_reverse = len(reverse_edges)
    stats_text = f"Таблиц: {n_nodes} | Связей ↓: {n_forward} | Связей ↑: {n_reverse}"
    fig.text(0.5, 0.02, stats_text, fontsize=9, color='#A6ADC8', ha='center')
    fig.text(0.99, 0.01, datetime.now().strftime("Сгенерировано: %d.%m.%Y %H:%M"),
             fontsize=7, color='#6C7086', ha='right', va='bottom')
    
    plt.tight_layout()
    
    # ─── 6. Сохраняем ─────────────────────────────────────────────────────
    output_path = str(output_path)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=dpi, bbox_inches='tight',
                facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close(fig)
    
    print(f"Граф связей визуализирован: {output_path}")
    return output_path
