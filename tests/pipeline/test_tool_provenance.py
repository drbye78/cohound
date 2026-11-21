"""Трасса вызовов как артефакт отчёта: ``outputs/tool_calls.csv`` (план 28 §10).

Проверяется то, что нельзя увидеть глазами в отчёте: пустая трасса не пишет файл и не
превращается в ноль; адрес запроса доезжает до файла; порядок строк — порядок ходов.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from core.config.run_config import load_run_config
from core.domain.tools import ToolTraceEntry
from core.pipeline.tool_provenance import (
    CSV_COLUMNS,
    collect_tool_calls,
    human_line,
    tool_calls_summary,
    write_tool_calls_artifact,
)
from core.ports.tools import ToolCallRecord
from core.store.sqlite_store import SqliteStore

RUN_ID = "r-tools"


@pytest.fixture()
def store(tmp_path: Path) -> SqliteStore:
    """Пустой прогон на текущей схеме: трасса требует строки ``runs`` (FK)."""
    store = SqliteStore(tmp_path / "cohound.db")
    cfg = load_run_config(Path(__file__).resolve().parents[2] / "packages/ru-pg-dbms/run.yaml")
    store.create_run(RUN_ID, cfg)
    yield store
    store.close()


def _record(
    product: str, param: str, seq: int, *, ok: bool = True, reason: str | None = None
) -> ToolCallRecord:
    return ToolCallRecord(
        product_id=product,
        param_id=param,
        entry=ToolTraceEntry(
            seq=seq,
            name="expand_to_section",
            arguments={"chunk_id": f"{product}:dv:0"},
            ok=ok,
            chars=120 if ok else 0,
            reason=reason,
        ),
    )


def test_empty_trace_writes_no_file_and_no_zero(store: SqliteStore, tmp_path: Path) -> None:
    """Нет вызовов → файла нет, сводки нет: «не звали» и «звали и провалились» не сливаются.

    Различие несёт ``tool_profile`` манифеста, а не пустой CSV: пустой файл читался бы как
    «посмотрели и вызовов не нашли» там, где профиль инструментов не включён вовсе.
    """
    path = tmp_path / "out" / "tool_calls.csv"
    assert write_tool_calls_artifact(store, RUN_ID, path) is None
    assert not path.exists()
    assert tool_calls_summary(store, RUN_ID) is None
    assert human_line(None) is None


def test_trace_is_published_with_address_and_order(store: SqliteStore, tmp_path: Path) -> None:
    """Строка файла несёт адрес запроса, а порядок — порядок ходов, не порядок вставки."""
    store.save_tool_calls(
        RUN_ID,
        [
            _record("p2", "b", 0),
            _record("p1", "a", 1, ok=False, reason="фрагмент не найден"),
            _record("p1", "a", 0),
        ],
    )
    path = write_tool_calls_artifact(store, RUN_ID, tmp_path / "tool_calls.csv")
    assert path is not None
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert tuple(rows[0].keys()) == CSV_COLUMNS
    assert [(r["product_id"], r["param_id"], r["seq"]) for r in rows] == [
        ("p1", "a", "0"),
        ("p1", "a", "1"),
        ("p2", "b", "0"),
    ]
    # Адрес запроса — часть аудита: без него «инструмент вызван» не отвечает, что он читал.
    assert json.loads(rows[1]["args_json"]) == {"chunk_id": "p1:dv:0"}
    assert rows[1]["ok"] == "0"
    assert rows[1]["reason"] == "фрагмент не найден"
    assert rows[0]["ok"] == "1"
    assert rows[0]["reason"] == ""


def test_summary_publishes_cost_not_only_verdict(store: SqliteStore) -> None:
    """Сводка: вызовы на ячейку, разбивка по инструментам, причины провалов."""
    store.save_tool_calls(
        RUN_ID,
        [
            _record("p1", "a", 0),
            _record("p1", "a", 1, ok=False, reason="не из таблицы"),
            _record("p2", "b", 0),
        ],
    )
    summary = tool_calls_summary(store, RUN_ID)
    assert summary is not None
    assert summary["calls"] == 3
    assert summary["cells_with_calls"] == 2
    assert summary["calls_per_cell"] == 1.5
    assert summary["by_tool"] == {"expand_to_section": 3}
    assert summary["failures"] == {"не из таблицы": 1}
    assert summary["chars_total"] == 240
    line = human_line(summary)
    assert line is not None
    assert "3 вызовов в 2 ячейках" in line
    assert "провалов не из таблицы=1" in line


def test_collect_is_empty_without_run_id(store: SqliteStore) -> None:
    """``run_id=None`` — пусто, а не выдуманная трасса чужого прогона."""
    assert collect_tool_calls(store, None) == []
