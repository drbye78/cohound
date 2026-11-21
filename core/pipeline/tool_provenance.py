"""Публикация трассы локальных вызовов оценщика: ``outputs/tool_calls.csv``.

**Зачем.** Трасса живёт в ``cell_tool_calls`` (схема 3.28.0) и отвечает на вопрос «почему
вердикт такой»: какой инструмент модель позвала, с каким адресом, что получила и сколько
символов. Пока её не публикуют, оператор видит в отчёте только вердикт, а «инструмент не
звали» и «инструмент звали и он провалился» неотличимы — ровно тот класс, который план 28
§10.4 называет провалом опыта.

**Что это НЕ доказывает.** Вывод инструмента не является доказательством (I22): цитата
по-прежнему обязана быть подстрокой текста фрагмента, а ``source_id`` берётся только из
переданных фрагментов. Файл — провенанс исполнения, а не условие измерения: в ``run_id``
он не входит (``tool_profile`` — входит, трасса известна лишь после исполнения).

**Почему пустой файл не пишется.** Отсутствие вызовов и отсутствие трассы — разные факты,
но внутри пустого CSV их не различить. Поэтому «вызовов не было» публикуется не файлом, а
полем контракта ``tool_profile`` в манифесте: профиль без инструментов даёт стабильную
строку ``-;max_calls=0``, а включённый профиль с пустым файлом — видимый дефект проводки.
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any

__all__ = [
    "CSV_COLUMNS",
    "collect_tool_calls",
    "tool_calls_summary",
    "write_tool_calls_artifact",
]

#: Колонки файла. ``args_json`` — адрес запроса (``chunk_id`` и подобное), без него строка
#: «инструмент вызван» не отвечает, **что** он читал.
CSV_COLUMNS: tuple[str, ...] = (
    "product_id",
    "param_id",
    "seq",
    "tool",
    "ok",
    "chars",
    "reason",
    "args_json",
)


def collect_tool_calls(store: Any, run_id: str | None) -> list[dict[str, Any]]:
    """Строки трассы прогона в порядке ``(product_id, param_id, seq)``.

    Порядок задаёт store: трасса читается как последовательность ходов, а не как множество
    строк. Отсутствие ``run_id`` **или** store — пустой список, а не выдуманный вызов:
    отчёт умеет собираться без store (разбор готовых ячеек), и тогда трассу не читали —
    это не «вызовов не было».
    """
    if not run_id or store is None:
        return []
    rows: list[dict[str, Any]] = []
    for record in store.iter_tool_calls(run_id):
        entry = record.entry
        rows.append(
            {
                "product_id": record.product_id,
                "param_id": record.param_id,
                "seq": entry.seq,
                "tool": entry.name,
                "ok": int(entry.ok),
                "chars": entry.chars,
                "reason": entry.reason,
                "args_json": json.dumps(
                    dict(entry.arguments), ensure_ascii=False, sort_keys=True
                ),
            }
        )
    return rows


def write_tool_calls_artifact(
    store: Any, run_id: str | None, path: str | Path
) -> Path | None:
    """Пишет ``outputs/tool_calls.csv``; трассы нет → ``None``.

    Пустой файл не пишется: он выглядел бы как «посмотрели и вызовов не нашли» там, где
    профиль инструментов не включён вовсе. Различить эти два случая внутри пустого файла
    нечем — различие несёт ``tool_profile`` манифеста.
    """
    rows = collect_tool_calls(store, run_id)
    if not rows:
        return None
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(CSV_COLUMNS),
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {key: ("" if value is None else value) for key, value in row.items()}
            )
    return target


def tool_calls_summary(
    store: Any, run_id: str | None
) -> dict[str, Any] | None:
    """Сводка трассы: ``None`` — вызовов не записано (и это не ноль).

    Публикуются: число вызовов, число ячеек с вызовами, разбивка по инструментам и причины
    провалов. ``None`` вместо ``{"calls": 0}`` — потому что «ноль вызовов» и «трасса не
    велась» различает профиль, а не эта функция: сводка обязана не сливать их.
    """
    rows = collect_tool_calls(store, run_id)
    if not rows:
        return None
    by_tool: Counter[str] = Counter(row["tool"] for row in rows)
    failures: Counter[str] = Counter(
        row["reason"] or "без причины" for row in rows if not row["ok"]
    )
    cells = {(row["product_id"], row["param_id"]) for row in rows}
    return {
        "calls": len(rows),
        "cells_with_calls": len(cells),
        "calls_per_cell": len(rows) / len(cells) if cells else None,
        "by_tool": dict(sorted(by_tool.items())),
        "failures": dict(sorted(failures.items())),
        "chars_total": sum(int(row["chars"]) for row in rows),
    }


def human_line(summary: Mapping[str, Any] | None) -> str | None:
    """Одна строка оператору: ``None`` — вызовов не записано.

    Форма — «инструменты: N вызовов в M ячейках (по инструментам: …); провалов K».
    Нулевые части не печатаются: перечисление нулей врало бы о значимости.
    """
    if not summary:
        return None
    calls = int(summary.get("calls") or 0)
    cells = int(summary.get("cells_with_calls") or 0)
    if not calls:
        return None
    parts = [f"инструменты: {calls} вызовов в {cells} ячейках"]
    by_tool = summary.get("by_tool") or {}
    if by_tool:
        parts.append("по инструментам: " + ", ".join(f"{k}={v}" for k, v in by_tool.items()))
    failures = summary.get("failures") or {}
    if failures:
        parts.append("провалов " + ", ".join(f"{k}={v}" for k, v in failures.items()))
    return "; ".join(parts)
