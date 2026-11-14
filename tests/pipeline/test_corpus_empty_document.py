"""Пустой документ — свойство артефакта, а не отказ парсера (``empty_document``).

Измерено 14.11.2026 на прогоне ``ru-pg-dbms-145ad66ca3f9``: ``Readme.md`` с
``content_hash`` пустой строки записывался классом ``parser_failed`` («парсер сломался»),
хотя разбирать было нечего. Класс неверен в двух смыслах: он врёт о приборе и делает
повтор осмысленным, хотя пустой файл не станет полным.

Проверяется цепочка: причина извлечения → класс строки очереди → покрытие → селектор
повтора. Тесты офлайн: SQLite в памяти, порты — структурные заглушки. Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cell,
    CellState,
    Cohort,
    CohortEntry,
    CrawlItemStatus,
    FailureKind,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
    SourceKind,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import build_corpus
from core.pipeline.crawl_session import default_session_id
from core.pipeline.evaluate import (
    RETRY_SKIP_NOT_RETRYABLE,
    RETRYABLE_FAILURE_KINDS,
    select_failed_cells,
)
from core.ports.types import (
    EMPTY_TEXT_REASON,
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

RUN_ID = "run-empty-doc"
FIXTURES = Path(__file__).parent / "fixtures"
TS = "2026-01-01T00:00:00+00:00"
URL = "https://docs.test/p1/empty.md"
URL_FAILED = "https://docs.test/p1/broken.html"


# --------------------------------------------------------------------------- #
# Заглушки портов (структурные, как в соседних тестах фазы)
# --------------------------------------------------------------------------- #


class _Crawler:
    """Краулер: ``url → bytes``; страница всегда отдаётся успешно."""

    version = "crawler-1"
    engine: str | None = None

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: object = None) -> RawPage:
        self.calls.append(url)
        return RawPage(url=url, content=self.pages[url], fetched_at=TS, lang="en")


class _Extractor:
    """Экстрактор: ``empty_urls`` — «текста нет», ``failure_urls`` — сбой разбора.

    Оба исхода возвращают пустой ``text`` и заполненный ``error`` (грациозная деградация
    адаптера), поэтому различить их можно только по причине — ровно так же, как на живом
    прогоне.
    """

    version = "extractor-1"

    def __init__(
        self,
        *,
        empty_urls: set[str] | None = None,
        failure_urls: set[str] | None = None,
    ) -> None:
        self.empty_urls = set(empty_urls or ())
        self.failure_urls = set(failure_urls or ())

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        if page.url in self.empty_urls:
            return ExtractedDoc(
                text="",
                content_hash=content_hash(""),
                extractor_version=self.version,
                error=EMPTY_TEXT_REASON,
            )
        if page.url in self.failure_urls:
            return ExtractedDoc(
                text="",
                content_hash=content_hash(""),
                extractor_version=self.version,
                error="trafilatura: извлечение не дало текста",
            )
        text = f"text for {page.url}"
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(text),
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    """Чанкер: один чанк на документ."""

    version = "chunker-1"

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        return [
            Chunk(
                chunk_id=f"{document_id}:0",
                chunk_no=0,
                text=doc.text,
                heading_path="h1",
                context_prefix="ctx",
                content_hash=doc.content_hash,
            )
        ]


def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a", "role_b"],
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


def _store(run_id: str = RUN_ID) -> SqliteStore:
    store = SqliteStore(":memory:")
    store.create_run(
        run_id,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    return store


def _build(
    tmp_path: Path,
    *,
    pages: dict[str, bytes],
    extractor: _Extractor,
) -> tuple[Any, SqliteStore]:
    store = _store()
    result = build_corpus(
        _plan(),
        _Crawler(pages),
        extractor,
        _Chunker(),
        store,
        urls={"p1": sorted(pages)},
        raw_root=tmp_path,
        run_id=RUN_ID,
        out_dir=tmp_path / "runs",
    )
    return result, store


# --------------------------------------------------------------------------- #
# Сквозной путь: извлечение → строка очереди → покрытие
# --------------------------------------------------------------------------- #


def test_empty_document_is_named_empty_document(tmp_path: Path) -> None:
    """«Текста нет» → ``empty_document`` в строке очереди и в покрытии (не ``parser_failed``)."""
    result, store = _build(
        tmp_path, pages={URL: b""}, extractor=_Extractor(empty_urls={URL})
    )
    try:
        items = list(store.iter_crawl_session_items(default_session_id(RUN_ID)))
        item = next(entry for entry in items if entry.url == URL)
        assert item.status is CrawlItemStatus.FAILED
        assert item.error_kind == FailureKind.EMPTY_DOCUMENT.value
        assert item.error == EMPTY_TEXT_REASON

        # Класс остаётся **видимым** в покрытии: документ добыт, но материал не наблюдён.
        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is False
        assert coverage.blocked == {FailureKind.EMPTY_DOCUMENT.value: 1}
        assert coverage.blocking_failures == {FailureKind.EMPTY_DOCUMENT.value: 1}

        # Причина отказа названа в канале фазы, а не проглочена.
        assert any("extract: " + EMPTY_TEXT_REASON in err for err in result.errors)
    finally:
        store.close()


def test_parser_failure_stays_parser_failed(tmp_path: Path) -> None:
    """Сбой разбора остаётся ``parser_failed``: классификация идёт по причине, а не «на всех».

    Различитель не должен превращать любой пустой текст в свойство артефакта: сломанный
    разбор — отказ прибора, и его повтор осмыслен.
    """
    _result, store = _build(
        tmp_path, pages={URL_FAILED: b"<html>"}, extractor=_Extractor(failure_urls={URL_FAILED})
    )
    try:
        items = list(store.iter_crawl_session_items(default_session_id(RUN_ID)))
        item = next(entry for entry in items if entry.url == URL_FAILED)
        assert item.error_kind == FailureKind.PARSER_FAILED.value
        assert item.error_kind != FailureKind.EMPTY_DOCUMENT.value
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# Селектор повтора: пустой документ терминален
# --------------------------------------------------------------------------- #


def _cell(param_id: str = "param_alpha") -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id="p1",
        param_id=param_id,
        state=CellState.FAILED,
        source_kind=SourceKind.DOCUMENTATION,
    )


def test_empty_document_is_not_retryable() -> None:
    """``hound retry`` не отбирает ячейку с ``empty_document``: повтор даст тот же исход."""
    assert FailureKind.EMPTY_DOCUMENT.value not in RETRYABLE_FAILURE_KINDS

    store = _store()
    try:
        cell = _cell()
        store.save_cell(cell)
        store.record_cell_failure(
            RUN_ID, "p1", "param_alpha", FailureKind.EMPTY_DOCUMENT.value
        )

        selection = select_failed_cells(store, RUN_ID)

        assert selection.retryable_cells == ()
        assert selection.failed_total == 1
        assert selection.skipped == (
            {
                "product_id": "p1",
                "param_id": "param_alpha",
                "reason": (
                    f"{RETRY_SKIP_NOT_RETRYABLE}:{FailureKind.EMPTY_DOCUMENT.value}"
                ),
                "failure_kind": FailureKind.EMPTY_DOCUMENT.value,
                "source_kind": "documentation",
                "state": "failed",
                "index_build_id": None,
            },
        )
    finally:
        store.close()
