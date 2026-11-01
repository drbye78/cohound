"""Метрика краула (17 §7.2, F8): формулы и агрегаты из durable-фактов сеанса.

Проверяются **числа**, а не форма вывода: интервал — минимум по соседним попыткам одного
host'а, перцентиль — nearest-rank (без интерполяции), ``ratio`` — против объявленного
минимума из провенанса, ``null`` — там, где измерять нечего. Нейтрально (§1.3): ``a.test``/
``b.test``/``p1``.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from core.domain import (
    CrawlItemStatus,
    CrawlSession,
    CrawlSessionItem,
    CrawlSessionStatus,
)
from core.pipeline.crawl_metric import (
    build_crawl_metric,
    declared_min_intervals,
    nearest_rank,
    observed_min_interval_s,
)

TS = "2026-01-01T00:00:00+00:00"
STARTED = "2026-01-01T00:00:00+00:00"
FINISHED = "2026-01-01T02:00:00+00:00"


def _session(
    *, started_at: str | None = STARTED, finished_at: str | None = FINISHED
) -> CrawlSession:
    """Строка сеанса с заданным окном (``finished_at=None`` — сеанс ещё идёт)."""
    return CrawlSession(
        session_id="crawl:r1",
        run_id="r1",
        profile="local",
        status=CrawlSessionStatus.COMPLETED
        if finished_at is not None
        else CrawlSessionStatus.RUNNING,
        created_at=TS,
        started_at=started_at,
        finished_at=finished_at,
    )


def _item(
    url: str,
    *,
    started: str | None,
    fetch_ms: int | None = None,
    status: CrawlItemStatus = CrawlItemStatus.FETCHED,
    error_kind: str | None = None,
) -> CrawlSessionItem:
    """Элемент сеанса: URL + наблюдённое начало попытки/длительность."""
    return CrawlSessionItem(
        session_id="crawl:r1",
        product_id="p1",
        url=url,
        status=status,
        fetch_started_at=started,
        fetch_ms=fetch_ms,
        error_kind=error_kind,
    )


def _provenance(*roots: tuple[str, float]) -> dict[str, Any]:
    """Провенанс с объявленным ``rate_limit.min_s`` у корней (``effective_limits.roots``)."""
    return {
        "effective_limits": {
            "roots": [
                {
                    "product_id": "p1",
                    "root": root,
                    "rate_limit": {"min_s": min_s, "max_s": min_s},
                }
                for root, min_s in roots
            ]
        }
    }


# --------------------------------------------------------------------------- #
# Формулы
# --------------------------------------------------------------------------- #


def test_nearest_rank_is_nearest_without_interpolation() -> None:
    """p50/p95 — наблюдённые значения по рангу ``ceil(p*n/100)``, не средние."""
    values = [1, 2, 3, 100]
    assert nearest_rank(values, 50) == 2  # rank=2: нижняя медиана, а не 2.5
    assert nearest_rank(values, 95) == 100  # rank=ceil(3.8)=4


def test_nearest_rank_large_n_has_no_float_drift() -> None:
    """``0.95*20`` в float дало бы 19.000…04 и ранг 20: целочисленная формула — 19."""
    values = list(range(1, 21))
    assert nearest_rank(values, 95) == 19
    assert nearest_rank(values, 50) == 10


def test_nearest_rank_empty_is_null_not_zero() -> None:
    """Пустая выборка → ``None``: ноль был бы выдуманным замером."""
    assert nearest_rank([], 50) is None
    assert nearest_rank([], 95) is None


def test_nearest_rank_single_value() -> None:
    """Один замер — и p50, и p95 равны ему (rank зажимается в выборку)."""
    assert nearest_rank([7], 50) == 7
    assert nearest_rank([7], 95) == 7


def test_observed_min_interval_is_min_of_sorted_neighbours() -> None:
    """Интервал — минимум по соседям **отсортированных** моментов, а не по порядку строк."""
    assert (
        observed_min_interval_s(
            [
                "2026-01-01T00:00:09+00:00",
                "2026-01-01T00:00:01+00:00",
                "2026-01-01T00:00:05+00:00",
            ]
        )
        == 4.0
    )


def test_observed_min_interval_needs_two_attempts() -> None:
    """Меньше двух моментов → ``None`` (не 0 и не «бесконечно»)."""
    assert observed_min_interval_s([]) is None
    assert observed_min_interval_s([None, None]) is None
    assert observed_min_interval_s(["2026-01-01T00:00:00+00:00"]) is None
    # ``None`` (resume/304/отказ до сети) не превращается в нулевой интервал.
    assert (
        observed_min_interval_s(
            ["2026-01-01T00:00:00+00:00", None, "2026-01-01T00:00:03+00:00"]
        )
        == 3.0
    )


def test_declared_intervals_read_provenance_per_host() -> None:
    """Объявленный минимум берётся из провенанса и адресуется по host'у корня."""
    declared = declared_min_intervals(
        _provenance(("https://a.test/docs", 1.0), ("https://b.test/", 2.5))
    )
    assert declared == {"a.test": (1.0,), "b.test": (2.5,)}


def test_declared_intervals_accept_nested_limits_shape() -> None:
    """Второй допустимый носитель — ``limits.rate_limit`` записи корня."""
    prov = {
        "effective_limits": {
            "roots": [{"root": "https://a.test/", "limits": {"rate_limit": {"min_s": 3.0}}}]
        }
    }
    assert declared_min_intervals(prov) == {"a.test": (3.0,)}


def test_declared_intervals_read_per_host_map() -> None:
    """F8: per-host карта ``rate_limits`` — основной носитель, host берётся её ключом."""
    prov = {
        "effective_limits": {
            "roots": [
                {
                    "root": "https://a.test/",
                    "rate_limits": {
                        "a.test": {"min_s": 1.0, "max_s": 2.0},
                        "cdn.a.test": {"min_s": 0.25, "max_s": 0.5},
                    },
                }
            ]
        }
    }
    assert declared_min_intervals(prov) == {"a.test": (1.0,), "cdn.a.test": (0.25,)}


def test_declared_intervals_conflict_across_roots_in_per_host_map() -> None:
    """Два корня объявили host'у разные минимумы → оба значения, решение — не здесь."""
    prov = {
        "effective_limits": {
            "roots": [
                {"root": "https://a.test/docs", "rate_limits": {"a.test": {"min_s": 1.0}}},
                {"root": "https://a.test/blog", "rate_limits": {"a.test": {"min_s": 5.0}}},
            ]
        }
    }
    assert declared_min_intervals(prov) == {"a.test": (1.0, 5.0)}


def test_declared_intervals_missing_is_empty_not_zero() -> None:
    """Нет объявленного значения → пусто (не 0.0): «неизвестно» ≠ «без троттлинга»."""
    assert declared_min_intervals(None) == {}
    assert declared_min_intervals({}) == {}
    assert declared_min_intervals({"effective_limits": {"roots": [{"root": "https://a.test/"}]}}) == {}


# --------------------------------------------------------------------------- #
# Агрегат по host'ам
# --------------------------------------------------------------------------- #


def _two_host_items() -> list[CrawlSessionItem]:
    """Два host'а: ``a.test`` — три попытки (часть без ``fetch_ms``), ``b.test`` — отказ."""
    return [
        _item("https://a.test/1", started="2026-01-01T00:00:00+00:00", fetch_ms=100),
        _item("https://a.test/2", started="2026-01-01T00:00:02+00:00", fetch_ms=200),
        # resume/304: ``fetch_ms`` не наблюдался — в выборку не входит, считается отдельно.
        _item("https://a.test/3", started="2026-01-01T00:00:05+00:00", fetch_ms=None),
        _item(
            "https://b.test/1",
            started="2026-01-01T00:00:01+00:00",
            status=CrawlItemStatus.FAILED,
            error_kind="source_unavailable",
        ),
    ]


def test_host_rows_publish_interval_ratio_percentiles_and_null() -> None:
    """Числа по host'ам: интервал, ratio против объявленного, p50/p95, ``null`` у одиночки."""
    metric = build_crawl_metric(
        session=_session(),
        items=_two_host_items(),
        provenance=_provenance(("https://a.test/", 1.0), ("https://b.test/", 1.0)),
    )

    assert metric["hosts"] == 2
    assert metric["urls"] == 4
    assert metric["crawl_only_wall"]["seconds"] == 7200.0
    assert metric["crawl_only_wall"]["running"] is False

    a = next(row for row in metric["per_host"] if row["host"] == "a.test")
    # Соседние моменты: 0, 2, 5 → интервалы 2 и 3 → минимум 2.
    assert a["observed_min_interval_s"] == 2.0
    assert a["declared_min_interval_s"] == 1.0
    assert a["ratio"] == 2.0
    assert a["politeness_violation"] is False
    assert a["requests"] == 3
    assert a["items"] == 3
    # fetch_ms: [100, 200], один None → он в выборке не участвует и считается отдельно.
    assert a["fetch_ms"] == {"p50": 100, "p95": 200, "n": 2}
    assert a["fetch_ms_unknown"] == 1

    b = next(row for row in metric["per_host"] if row["host"] == "b.test")
    # Одна попытка → интервала нет: None, а не 0 и не «бесконечно».
    assert b["observed_min_interval_s"] is None
    assert b["ratio"] is None
    assert b["politeness_violation"] is None
    assert b["fetch_ms"] == {"p50": None, "p95": None, "n": 0}
    assert b["fetch_ms_unknown"] == 1
    assert b["requests_by_state"]["failed"] == 1
    assert b["errors_by_kind"] == {"source_unavailable": 1}

    # Сеансовые агрегаты — по всем элементам, не суммой уже округлённых перцентилей.
    assert metric["fetch_ms"] == {"p50": 100, "p95": 200, "n": 2}
    assert metric["fetch_ms_unknown"] == 2
    assert metric["requests_by_state"] == {
        "pending": 0,
        "running": 0,
        "fetched": 3,
        "failed": 1,
        "cancelled": 0,
    }
    assert metric["errors_by_kind"] == {"source_unavailable": 1}


def test_politeness_violation_is_named_when_ratio_below_one() -> None:
    """``ratio < 1`` — нарушение вежливости, и оно названо флагом, а не спрятано."""
    items = [
        _item("https://a.test/1", started="2026-01-01T00:00:00+00:00", fetch_ms=10),
        _item("https://a.test/2", started="2026-01-01T00:00:00.500000+00:00", fetch_ms=10),
    ]
    metric = build_crawl_metric(
        session=_session(), items=items, provenance=_provenance(("https://a.test/", 2.0))
    )
    row = metric["per_host"][0]
    assert row["observed_min_interval_s"] == 0.5
    assert row["ratio"] == 0.25
    assert row["politeness_violation"] is True


def test_conflicting_declarations_yield_null_and_variants() -> None:
    """Корни одного host'а объявили разные минимумы → одного числа нет: ``None`` + варианты."""
    prov = {
        "effective_limits": {
            "roots": [
                {"root": "https://a.test/docs", "rate_limit": {"min_s": 1.0}},
                {"root": "https://a.test/blog", "rate_limit": {"min_s": 5.0}},
            ]
        }
    }
    metric = build_crawl_metric(
        session=_session(),
        items=[
            _item("https://a.test/1", started="2026-01-01T00:00:00+00:00", fetch_ms=1),
            _item("https://a.test/2", started="2026-01-01T00:00:03+00:00", fetch_ms=1),
        ],
        provenance=prov,
    )
    row = metric["per_host"][0]
    assert row["observed_min_interval_s"] == 3.0
    assert row["declared_min_interval_s"] is None
    assert row["ratio"] is None
    assert row["politeness_violation"] is None
    assert row["declared_min_interval_variants"] == [1.0, 5.0]
    assert any("разные минимумы" in note for note in metric["notes"])


def test_missing_declared_is_null_and_named_in_notes() -> None:
    """Минимум не восстановим → ``declared_min_interval_s: null`` и явная заметка."""
    metric = build_crawl_metric(
        session=_session(),
        items=[
            _item("https://a.test/1", started="2026-01-01T00:00:00+00:00", fetch_ms=1),
            _item("https://a.test/2", started="2026-01-01T00:00:02+00:00", fetch_ms=1),
        ],
        provenance={},
    )
    row = metric["per_host"][0]
    assert row["declared_min_interval_s"] is None
    assert row["ratio"] is None
    assert any("не восстановим" in note for note in metric["notes"])


def test_zero_declared_means_no_violation_possible() -> None:
    """``min_s=0`` — «без троттлинга»: отношения нет (``ratio: null``), нарушать нечего."""
    metric = build_crawl_metric(
        session=_session(),
        items=[
            _item("https://a.test/1", started="2026-01-01T00:00:00+00:00", fetch_ms=1),
            _item("https://a.test/2", started="2026-01-01T00:00:00.100000+00:00", fetch_ms=1),
        ],
        provenance=_provenance(("https://a.test/", 0.0)),
    )
    row = metric["per_host"][0]
    assert row["declared_min_interval_s"] == 0.0
    assert row["ratio"] is None
    assert row["politeness_violation"] is False


def test_running_session_wall_is_elapsed_and_marked() -> None:
    """Идущий сеанс: ``crawl_only_wall`` — прошедшее время с флагом ``running``, не итог."""
    metric = build_crawl_metric(
        session=_session(finished_at=None),
        items=[],
        provenance=None,
        now=_dt.datetime(2026, 1, 1, 0, 30, tzinfo=_dt.UTC),
    )
    assert metric["crawl_only_wall"] == {
        "seconds": 1800.0,
        "started_at": STARTED,
        "ended_at": None,
        "running": True,
    }


def test_empty_session_is_honest_zero_and_null() -> None:
    """Нет элементов/окна/очереди — честные ``0``/``null``, без падения."""
    metric = build_crawl_metric(
        session=_session(started_at=None, finished_at=None),
        items=[],
        provenance=None,
        backlog=None,
        now=_dt.datetime(2026, 1, 1, 0, 0, tzinfo=_dt.UTC),
    )
    assert metric["hosts"] == 0
    assert metric["urls"] == 0
    assert metric["crawl_only_wall"]["seconds"] is None  # окна не было — не 0 секунд
    assert metric["fetch_ms"] == {"p50": None, "p95": None, "n": 0}
    assert metric["fetch_ms_unknown"] == 0
    assert metric["backlog_bytes"] == 0
    assert metric["queue_rows"] == {}
    assert metric["per_host"] == []


def test_backlog_block_uses_backlog_report() -> None:
    """``backlog_bytes``/``queue_rows`` — из ``ExtractQueue.backlog(...)``, не второй счёт."""

    class _Report:
        """Минимальная форма :class:`BacklogReport` (тот же ``as_dict``)."""

        def as_dict(self) -> dict[str, Any]:
            return {
                "backlog_bytes": 4096,
                "backlog_files": 2,
                "backlog_missing": 1,
                "queue_rows": {"pending": 2},
                "free_bytes": None,
                "backlog_limit_bytes": None,
            }

    metric = build_crawl_metric(session=_session(), items=[], backlog=_Report())
    assert metric["backlog_bytes"] == 4096
    assert metric["backlog_files"] == 2
    assert metric["backlog_missing"] == 1
    assert metric["queue_rows"] == {"pending": 2}