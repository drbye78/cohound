"""17 §6 (F3): ``errors`` сортируются **при записи в артефакт**, а не при создании.

``errors`` наполняется по мере событий (сеть, пул воркеров, изоляция durable-записей),
поэтому два прогона с одинаковым результатом давали разный порядок строк, и артефакт
(``results[].errors`` прогона, payload ``crawl --json``) зависел от планировщика. Здесь
проверяется граница: сортируется **записываемая форма**, а одинаковые строки остаются
двумя (дедупликации нет — это два события, не одно).

Сверяется фактический артефакт (payload, который публикуется как ``run --json``), а не
промежуточный список фазы.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

from core.cli.context import _run_report_payload
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import build_corpus
from core.pipeline.runner import PhaseResult, RunReport
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore

FIXTURES = Path(__file__).parent / "fixtures"
TS = "2026-01-01T00:00:00+00:00"
ROOT_A = "https://docs.test/a"
ROOT_B = "https://docs.test/b"


# --------------------------------------------------------------------------- #
# Обвязка: нейтральный план + заглушки портов (без сети)
# --------------------------------------------------------------------------- #


def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan(run_id: str) -> RunPlan:
    return RunPlan(
        run_id=run_id,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=_catalog(),
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id="case",
            frozen_at=_dt.date(2026, 1, 1),
            entries=[
                CohortEntry(
                    product_id="p1",
                    role_overlap=1.0,
                    selection_reason="anchor",
                    anchor_relation=AnchorRelation.ANCHOR,
                )
            ],
        ),
        golden=[],
    )


def _make_run(store: SqliteStore, run_id: str) -> None:
    store.create_run(
        run_id,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )


class _FailingCrawler:
    """Краулер, падающий на каждом URL: каждый сбой = отдельное событие в ``errors``.

    Порядок вызовов (и, значит, порядок событий) равен порядку корней продукта: он задаётся
    входом ``urls``, а не планировщиком, — поэтому тест детерминирован.
    """

    version = "crawler-1"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        self.calls.append(url)
        raise RuntimeError(f"crawl failed: {url}")


class _Extractor:
    version = "extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:  # pragma: no cover - документов не появляется: все фетчи падают
        return ExtractedDoc(
            text=page.content.decode("utf-8", errors="replace"),
            content_hash="unused",
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    version = "chunker-1"

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:  # pragma: no cover
        return []


def _corpus_run(tmp_path: Path, run_id: str, roots: list[str]) -> tuple[Any, list[str]]:
    """Прогон фазы корпуса: падающие корни; возвращает ``PhaseResult`` и порядок событий."""
    store = SqliteStore(":memory:")
    _make_run(store, run_id)
    crawler = _FailingCrawler()
    try:
        result = build_corpus(
            _plan(run_id),
            crawler,
            _Extractor(),
            _Chunker(),
            store,
            urls={"p1": roots},
            raw_root=tmp_path,
            run_id=run_id,
            crawl_defaults=CrawlRunConfig(max_depth=0),
        )
        payload = _run_report_payload(
            run_id, RunReport(run_id=run_id, results=[result]), tmp_path / "cohound.db"
        )
    finally:
        store.close()
    return payload, crawler.calls


# --------------------------------------------------------------------------- #
# (1) Артефакт прогона: ``results[].errors`` — лексикографический порядок
# --------------------------------------------------------------------------- #


def test_run_payload_sorts_errors_instead_of_preserving_event_order(tmp_path: Path) -> None:
    """Один и тот же набор ошибок, пришедший в разном порядке, даёт один артефакт."""
    event_order = ["p1/b: второй", "p1/a: первый"]
    reversed_order = list(reversed(event_order))

    first = _run_report_payload(
        "run-errors",
        RunReport(
            run_id="run-errors",
            results=[PhaseResult(status="ok", phase="corpus", errors=list(event_order))],
        ),
        tmp_path / "cohound.db",
    )
    second = _run_report_payload(
        "run-errors",
        RunReport(
            run_id="run-errors",
            results=[
                PhaseResult(status="ok", phase="corpus", errors=list(reversed_order))
            ],
        ),
        tmp_path / "cohound.db",
    )

    # Публикуемая форма — по фазе и в агрегате.
    assert first["results"][0]["errors"] == sorted(event_order)
    assert first["errors"] == sorted(event_order)
    assert first["results"][0]["errors"] == second["results"][0]["errors"]
    assert first["errors"] == second["errors"]


def test_run_payload_keeps_duplicates(tmp_path: Path) -> None:
    """Дедупликации нет: две одинаковые строки — два события, а не одно."""
    payload = _run_report_payload(
        "run-dupes",
        RunReport(
            run_id="run-dupes",
            results=[
                PhaseResult(
                    status="ok",
                    phase="corpus",
                    errors=["p1/u: сбой", "p1/u: сбой", "p1/a: другой"],
                )
            ],
        ),
        tmp_path / "cohound.db",
    )

    assert payload["results"][0]["errors"] == [
        "p1/a: другой",
        "p1/u: сбой",
        "p1/u: сбой",
    ]


# --------------------------------------------------------------------------- #
# (2) Два настоящих прогона: события пришли в разном порядке — артефакт один
# --------------------------------------------------------------------------- #


def test_two_corpus_runs_with_swapped_root_order_publish_identical_errors(
    tmp_path: Path,
) -> None:
    """Порядок событий разный (проверено по порядку фетчей), публикуемый артефакт — один."""
    forward, calls_forward = _corpus_run(
        tmp_path / "forward", "run-errors-forward", [ROOT_A, ROOT_B]
    )
    backward, calls_backward = _corpus_run(
        tmp_path / "backward", "run-errors-backward", [ROOT_B, ROOT_A]
    )

    # События действительно пришли в разном порядке: сбой фиксируется сразу после фетча.
    assert calls_forward == [ROOT_A, ROOT_B]
    assert calls_backward == [ROOT_B, ROOT_A]
    assert len(forward["results"][0]["errors"]) == 2

    # Артефакт от порядка событий не зависит.
    assert forward["results"][0]["errors"] == backward["results"][0]["errors"]
    assert forward["errors"] == backward["errors"]
    assert forward["results"][0]["errors"] == sorted(forward["results"][0]["errors"])
    # Порядок — лексикографический по строке целиком, а не по порядку корней.
    assert ROOT_A in forward["results"][0]["errors"][0]