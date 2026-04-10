#!/usr/bin/env python3
"""
Накопление результатов массовой оценки таблиц на «фактотабличность».

Файл JSON (по умолчанию рядом с ui_state): merge по имени таблицы,
новая оценка заменяет предыдущую для той же таблицы.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import io

from analyzers.fact_table_assessor import AssessmentResult, FactTableAssessor, Warning

# Версия схемы файла (при смене формата увеличить и при необходимости мигрировать).
STORE_VERSION = 1

# Стабильный порядок эвристик для колонок отчёта.
HEURISTIC_IDS = [f"H-{i:02d}" for i in range(1, 8)]

# Ключ метрики в JSON → заголовок колонки в UI / XLSX (порядок отображения).
FACT_METRIC_COLUMNS: List[Tuple[str, str]] = [
    ("h01_table_class", "М: класс типа (H-01)"),
    ("h02_meaningful_numeric_count", "М: число числовых полей"),
    ("h03_date_field_count", "М: число полей дат"),
    ("h04_row_count", "М: число строк"),
    ("h05_vt_count", "М: число табл. частей (VT)"),
    ("h06_fk_ref_count", "М: ссылок binary(16)"),
    ("h07_guid_index_ready", "М: H-07 GUID-индекс"),
    ("h07_document_ref_count", "М: H-07 ссылок на документы"),
    ("h07_fk_fields_checked", "М: H-07 проверено полей FK"),
]

FACT_METRIC_DISPLAY_LABELS = [lbl for _k, lbl in FACT_METRIC_COLUMNS]


def _format_metric_value(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "да" if v else "нет"
    return v


def _apply_metrics_to_row(row: dict, metrics: Optional[dict]) -> None:
    """Добавляет в row колонки метрик из словаря result.metrics."""
    m = metrics if isinstance(metrics, dict) else {}
    for key, label in FACT_METRIC_COLUMNS:
        row[label] = _format_metric_value(m.get(key, ""))


def default_store_path(output_dir: Path) -> Path:
    """Путь к fact_assessments.json в каталоге вывода проекта."""
    return Path(output_dir) / "fact_assessments.json"


def warning_to_dict(w: Warning) -> dict:
    """Преобразует Warning в JSON-совместимый словарь."""
    return {
        "heuristic_id": w.heuristic_id,
        "severity": w.severity,
        "message": w.message,
        "weight": w.weight,
    }


def assessment_to_dict(result: AssessmentResult) -> dict:
    """Преобразует AssessmentResult в словарь для сохранения в JSON."""
    return {
        "score": result.score,
        "score_label": result.score_label,
        "total_weight": result.total_weight,
        "warnings": [warning_to_dict(w) for w in result.warnings],
        "metrics": dict(result.metrics or {}),
    }


def load_store(path: Path) -> dict:
    """
    Загружает накопленные оценки с диска.
    Возвращает словарь с ключами version, by_table (или пустую структуру).
    """
    try:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict) and "by_table" in raw:
                return raw
    except Exception as e:
        logging.warning("fact_assessment_store.load_store: %s", e)
    return {"version": STORE_VERSION, "by_table": {}}


def save_store(path: Path, data: dict) -> None:
    """Атомарно сохраняет store (mkdir родителя, UTF-8)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = dict(data)
    data["version"] = STORE_VERSION
    by_table = data.get("by_table")
    if not isinstance(by_table, dict):
        by_table = {}
    data["by_table"] = by_table
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def merge_updates(
    existing: dict,
    updates: Dict[str, dict],
    db_signature: Optional[str] = None,
) -> dict:
    """
    Возвращает новый store: копия existing, в by_table для каждого ключа из updates
    записана новая запись (остальные таблицы без изменений).

    updates: table_name -> запись вида success или error:
      успех: {"assessed_at": iso, "ok": True, "result": assessment_dict}
      ошибка: {"assessed_at": iso, "ok": False, "error_message": str}

    Если передан db_signature, он записывается в каждую обновлённую запись.
    """
    data = {
        "version": existing.get("version", STORE_VERSION),
        "by_table": dict(existing.get("by_table") or {}),
    }
    for tname, entry in updates.items():
        new_entry = dict(entry)
        if db_signature:
            new_entry["db_signature"] = db_signature
        data["by_table"][tname] = new_entry
    return data


def now_iso() -> str:
    """Текущее время в ISO UTC."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _weights_by_heuristic(warnings: List[dict]) -> Dict[str, int]:
    """Из списка предупреждений в JSON — словарь H-01 -> weight."""
    out = {hid: None for hid in HEURISTIC_IDS}
    for w in warnings or []:
        hid = w.get("heuristic_id")
        if hid in out:
            out[hid] = int(w.get("weight", 0))
    return out


def row_for_table(
    table_name: str,
    entry: dict,
    human_name_fn: Callable[[str], Optional[str]],
    is_favorite: bool,
) -> dict:
    """
    Одна строка для отображения / экспорта.

    human_name_fn — например lambda t: sp.get_table_human_name(t)
    """
    human = human_name_fn(table_name) or ""
    base = {
        "Таблица (техн.)": table_name,
        "Человекочитаемое имя": human,
        "Оценено (UTC)": entry.get("assessed_at", ""),
        "Избранное": bool(is_favorite),
    }
    if not entry.get("ok", False):
        base["Ошибка"] = entry.get("error_message", "Неизвестная ошибка")
        base["Итог"] = ""
        base["Итоговый вес"] = None
        base["Пояснение"] = ""
        for hid in HEURISTIC_IDS:
            base[hid] = None
        for _lab in FACT_METRIC_DISPLAY_LABELS:
            base[_lab] = ""
        return base

    res = entry.get("result") or {}
    warnings = res.get("warnings", [])
    wmap = _weights_by_heuristic(warnings)

    row = {
        **base,
        "Ошибка": "",
        "Итог": res.get("score", ""),
        "Итоговый вес": res.get("total_weight"),
        "Пояснение": res.get("score_label", ""),
    }
    for hid in HEURISTIC_IDS:
        v = wmap.get(hid)
        row[hid] = v if v is not None else ""
    _apply_metrics_to_row(row, res.get("metrics"))
    return row


def build_rows_from_store(
    store: dict,
    human_name_fn: Callable[[str], Optional[str]],
    favorites: Dict[str, Any],
    db_signature: Optional[str] = None,
    legacy_allowed_tables: Optional[set[str]] = None,
) -> List[dict]:
    """
    Список строк по ключам by_table (для DataFrame).

    Если db_signature задан, включаются:
    - записи с тем же db_signature;
    - legacy-записи без db_signature только если их таблица в legacy_allowed_tables
      (мягкая совместимость со старым форматом без межбазового «засорения»).
    """
    by_table = store.get("by_table") or {}
    rows: List[dict] = []
    for tname in sorted(by_table.keys(), key=lambda x: x.lower()):
        entry = by_table[tname]
        if db_signature:
            entry_sig = entry.get("db_signature")
            if entry_sig and entry_sig != db_signature:
                continue
            if not entry_sig and legacy_allowed_tables is not None and tname not in legacy_allowed_tables:
                continue
        is_fav = tname in favorites
        rows.append(row_for_table(tname, entry, human_name_fn, is_fav))
    return rows


def order_table_rows(rows: List[dict], preferred_columns: List[str]) -> List[dict]:
    """
    Возвращает новый список строк: в каждой записи ключи сначала в порядке preferred_columns,
    затем остальные по алфавиту — удобно для st.data_editor без pandas.
    """
    ordered: List[dict] = []
    for r in rows:
        out: dict = {}
        seen = set()
        for c in preferred_columns:
            if c in r:
                out[c] = r[c]
                seen.add(c)
        for k in sorted(kk for kk in r.keys() if kk not in seen):
            out[k] = r[k]
        ordered.append(out)
    return ordered


def assess_tables_bulk(
    assessor: FactTableAssessor,
    table_names: List[str],
    on_progress: Optional[Callable[[int, int, str], None]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> Dict[str, dict]:
    """
    Оценивает список таблиц; при ошибке на одной — пишет ok=False и продолжает.
    Если should_cancel() истинен перед очередной таблицей — прерывание, возврат частичного updates.

    on_progress(completed, total, caption) — перед строкой i: completed=i; после обработки: completed=i+1.
    """
    updates: Dict[str, dict] = {}
    total = len(table_names)
    for i, tname in enumerate(table_names):
        if should_cancel and should_cancel():
            break
        if on_progress:
            on_progress(i, total, f"Оценка: {tname}")
        ts = now_iso()
        try:
            result = assessor.assess(tname)
            updates[tname] = {
                "assessed_at": ts,
                "ok": True,
                "result": assessment_to_dict(result),
            }
        except Exception as e:
            logging.exception("Оценка таблицы %s", tname)
            updates[tname] = {
                "assessed_at": ts,
                "ok": False,
                "error_message": str(e),
            }
        if on_progress:
            on_progress(i + 1, total, tname)
    return updates


def export_to_xlsx_bytes(rows: List[dict]) -> bytes:
    """
    Строит XLSX в памяти из списка словарей (одинаковые ключи в строках).
    """
    try:
        from openpyxl import Workbook
    except ImportError as e:
        raise RuntimeError("Для экспорта установите openpyxl") from e

    wb = Workbook()
    ws = wb.active
    ws.title = "Оценка фактов"

    if not rows:
        ws.append(["Нет данных"])
        bio = io.BytesIO()
        wb.save(bio)
        return bio.getvalue()

    # Порядок колонок: фиксированные первыми, затем H-01..H-07
    preferred = [
        "Таблица (техн.)",
        "Человекочитаемое имя",
        "Оценено (UTC)",
        "Избранное",
        "Ошибка",
        "Итог",
        "Итоговый вес",
        "Пояснение",
    ] + HEURISTIC_IDS + FACT_METRIC_DISPLAY_LABELS

    keys_set = set()
    for r in rows:
        keys_set.update(r.keys())
    columns = [c for c in preferred if c in keys_set]
    columns.extend(sorted(k for k in keys_set if k not in columns))

    ws.append(columns)
    for r in rows:
        ws.append([r.get(c, "") for c in columns])

    bio = io.BytesIO()
    wb.save(bio)
    return bio.getvalue()
