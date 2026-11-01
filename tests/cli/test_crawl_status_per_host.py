"""CLI: ``hound crawl status --per-host`` — метрика краула (17 §7.2, F8).

Проверяется граница: флаг печатает числа из durable-фактов (элементы + строка сеанса),
складывается с ``--per-url`` и **не** ломает его, нарушение вежливости видно и в ``--json``,
и в человекочитаемой форме, идущий сеанс помечен как незавершённый, пустой сеанс даёт
честные ``0``/``null``. Сеть не используется; данные категорийно-нейтральны (§1.3).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from core.cli.main import EXIT_OK, main
from core.domain import (
    CrawlItemStatus,
    CrawlSession,
    CrawlSessionItem,
    CrawlSessionStatus,
)
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

TS = "2026-01-01T00:00:00+00:00"
STARTED = "2026-01-01T00:00:00+00:00"
FINISHED = "2026-01-01T01:00:00+00:00"
SESSION_ID = "crawl:r1"


def _db(
    tmp_path: Path,
    *,
    items: list[CrawlSessionItem] | None = None,
    provenance: dict[str, Any] | None = None,
    started_at: str | None = STARTED,
    finished_at: str | None = FINISHED,
) -> Path:
    """БД с одним сеансом, его элементами и (опционально) провенансом."""
    db = tmp_path / "metric.db"
    with SqliteStore(db) as store:
        store.upsert_crawl_session(
            CrawlSession(
                session_id=SESSION_ID,
                run_id=None,  # standalone-сеанс: FK на runs не требуется
                profile="local",
                status=CrawlSessionStatus.COMPLETED
                if finished_at is not None
                else CrawlSessionStatus.RUNNING,
                created_at=TS,
                started_at=started_at,
                finished_at=finished_at,
                owner="other-host/4242/777",
                heartbeat_at=TS,
            )
        )
        for item in items or ():
            store.upsert_crawl_session_item(item)
        if provenance is not None:
            store.set_crawl_session_provenance(
                SESSION_ID, json.dumps(provenance, ensure_ascii=False, sort_keys=True)
            )
    return db


def _item(
    url: str,
    *,
    started: str | None,
    fetch_ms: int | None = None,
    status: CrawlItemStatus = CrawlItemStatus.FETCHED,
    error_kind: str | None = None,
) -> CrawlSessionItem:
    """Элемент сеанса с наблюдёнными метками попытки."""
    return CrawlSessionItem(
        session_id=SESSION_ID,
        product_id="p1",
        url=url,
        status=status,
        fetch_started_at=started,
        fetch_ms=fetch_ms,
        error_kind=error_kind,
    )


def _provenance(*roots: tuple[str, float]) -> dict[str, Any]:
    """Провенанс с объявленным ``rate_limit`` корней (durable-источник вежливости)."""
    return {
        "effective_limits": {
            "roots": [
                {"product_id": "p1", "root": root, "rate_limit": {"min_s": min_s, "max_s": min_s}}
                for root, min_s in roots
            ]
        }
    }


def _items() -> list[CrawlSessionItem]:
    """Два host'а: ``a.test`` — три попытки, ``b.test`` — одна упавшая."""
    return [
        _item("https://a.test/1", started="2026-01-01T00:00:00+00:00", fetch_ms=100),
        _item("https://a.test/2", started="2026-01-01T00:00:02+00:00", fetch_ms=200),
        _item("https://a.test/3", started="2026-01-01T00:00:04+00:00", fetch_ms=None),
        _item(
            "https://b.test/1",
            started="2026-01-01T00:00:01+00:00",
            status=CrawlItemStatus.FAILED,
            error_kind="source_unavailable",
        ),
    ]


def _argv(db: Path, *extra: str, json_mode: bool = True) -> list[str]:
    argv = ["crawl", "status", "--run-id", "r1", "--db", str(db)]
    if json_mode:
        argv.append("--json")
    return [*argv, *extra]


# --------------------------------------------------------------------------- #
# --per-host: числа в --json
# --------------------------------------------------------------------------- #


def test_per_host_publishes_metric_numbers(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--per-host`` печатает hosts/urls/wall, интервал, ratio, p50/p95 и backlog."""
    db = _db(
        tmp_path,
        items=_items(),
        provenance=_provenance(("https://a.test/", 1.0), ("https://b.test/", 1.0)),
    )

    assert main(_argv(db, "--per-host")) == EXIT_OK
    metric = json_out(capsys)["metric"]

    assert metric["hosts"] == 2
    assert metric["urls"] == 4
    assert metric["crawl_only_wall"]["seconds"] == 3600.0
    assert metric["crawl_only_wall"]["running"] is False
    # Очередь пуста → честный 0 и пустые строки очереди, а не «нет данных».
    assert metric["backlog_bytes"] == 0
    assert metric["queue_rows"] == {}
    assert metric["fetch_ms"] == {"p50": 100, "p95": 200, "n": 2}
    assert metric["fetch_ms_unknown"] == 2
    assert metric["requests_by_state"]["fetched"] == 3
    assert metric["requests_by_state"]["failed"] == 1
    assert metric["errors_by_kind"] == {"source_unavailable": 1}

    a = next(row for row in metric["per_host"] if row["host"] == "a.test")
    # Моменты 0, 2, 4 → интервалы 2 и 2 → минимум 2; объявлено 1.0 → ratio 2.0.
    assert a["observed_min_interval_s"] == 2.0
    assert a["declared_min_interval_s"] == 1.0
    assert a["ratio"] == 2.0
    assert a["politeness_violation"] is False
    assert a["fetch_ms"] == {"p50": 100, "p95": 200, "n": 2}
    assert a["fetch_ms_unknown"] == 1

    b = next(row for row in metric["per_host"] if row["host"] == "b.test")
    # Одна попытка → интервала нет: null (не 0).
    assert b["observed_min_interval_s"] is None
    assert b["ratio"] is None


def test_per_host_names_politeness_violation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``ratio < 1`` видно в ``--json`` (флаг) и в человекочитаемой форме (слово)."""
    db = _db(
        tmp_path,
        items=[
            _item("https://a.test/1", started="2026-01-01T00:00:00+00:00", fetch_ms=10),
            _item("https://a.test/2", started="2026-01-01T00:00:00.400000+00:00", fetch_ms=10),
        ],
        provenance=_provenance(("https://a.test/", 2.0)),
    )

    assert main(_argv(db, "--per-host")) == EXIT_OK
    row = json_out(capsys)["metric"]["per_host"][0]
    assert row["observed_min_interval_s"] == 0.4
    assert row["declared_min_interval_s"] == 2.0
    assert row["ratio"] == 0.2
    assert row["politeness_violation"] is True

    assert main(_argv(db, "--per-host", json_mode=False)) == EXIT_OK
    out = capsys.readouterr().out
    assert "politeness: a.test — НАРУШЕНИЕ" in out
    assert "ratio=0.200" in out


def test_per_host_human_table_is_aligned_and_named(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Человекочитаемая форма: шапка метрики и строка таблицы по host'у с числами."""
    db = _db(tmp_path, items=_items(), provenance=_provenance(("https://a.test/", 1.0)))

    assert main(_argv(db, "--per-host", json_mode=False)) == EXIT_OK
    out = capsys.readouterr().out
    assert "crawl_only_wall: 3600.000 s" in out
    assert "urls: 4, hosts: 2" in out
    assert "fetch_ms: p50=100 p95=200 n=2 unknown=2" in out
    assert "backlog: bytes=0 files=0 missing=0 queue_rows=—" in out
    assert "host" in out and "min_ivl_s" in out
    assert "a.test" in out and "2.000" in out
    # У host'а без объявленного значения ratio не считается — печатается прочерк, не ноль.
    assert "note: declared_min_interval_s: нет объявленного значения для host'ов: b.test" in out


def test_per_host_missing_declared_is_null_with_note(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Провенанс без ``rate_limit`` → ``declared_min_interval_s: null`` и заметка; не падение."""
    db = _db(tmp_path, items=_items(), provenance={})

    assert main(_argv(db, "--per-host")) == EXIT_OK
    metric = json_out(capsys)["metric"]
    assert all(row["declared_min_interval_s"] is None for row in metric["per_host"])
    assert all(row["ratio"] is None for row in metric["per_host"])
    assert any("не восстановим" in note for note in metric["notes"])


def test_running_session_wall_is_marked_unfinished(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Идущий сеанс: окно помечено ``running``, время — прошедшее, а не итог."""
    db = _db(
        tmp_path,
        items=_items(),
        provenance=_provenance(("https://a.test/", 1.0)),
        started_at="2020-01-01T00:00:00+00:00",
        finished_at=None,
    )

    assert main(_argv(db, "--per-host")) == EXIT_OK
    wall = json_out(capsys)["metric"]["crawl_only_wall"]
    assert wall["running"] is True
    assert wall["ended_at"] is None
    assert wall["seconds"] is not None and wall["seconds"] > 0

    assert main(_argv(db, "--per-host", json_mode=False)) == EXIT_OK
    assert "running — окно не завершено" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# Совместимость с --per-url
# --------------------------------------------------------------------------- #


def test_per_url_still_works_without_per_host(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--per-url`` в одиночку не меняется: есть ``items``, нет ``metric``."""
    db = _db(tmp_path, items=_items())

    assert main(_argv(db, "--per-url")) == EXIT_OK
    payload = json_out(capsys)
    assert len(payload["items"]) == 4
    assert "metric" not in payload


def test_per_url_and_per_host_compose(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Флаги складываются: один ответ несёт и построчный список, и агрегат."""
    db = _db(tmp_path, items=_items(), provenance=_provenance(("https://a.test/", 1.0)))

    assert main(_argv(db, "--per-url", "--per-host")) == EXIT_OK
    payload = json_out(capsys)
    assert len(payload["items"]) == 4
    assert payload["metric"]["urls"] == 4


def test_no_session_with_per_host_is_valid_empty_answer(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Сеанса нет → ``status: null`` и ``EXIT_OK``; метрики за несуществующий сеанс нет."""
    db = tmp_path / "empty.db"
    with SqliteStore(db):
        pass

    assert main(_argv(db, "--per-host")) == EXIT_OK
    payload = json_out(capsys)
    assert payload["status"] is None
    assert "metric" not in payload


def test_session_without_items_publishes_honest_zeros(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Сеанс без элементов: ``0``/``null`` в метрике, без падения."""
    db = _db(tmp_path, items=[], provenance=None)

    assert main(_argv(db, "--per-host")) == EXIT_OK
    metric = json_out(capsys)["metric"]
    assert metric["hosts"] == 0
    assert metric["urls"] == 0
    assert metric["per_host"] == []
    assert metric["backlog_bytes"] == 0
    assert metric["fetch_ms"] == {"p50": None, "p95": None, "n": 0}