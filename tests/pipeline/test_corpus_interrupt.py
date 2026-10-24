"""Прерывание продукт-цикла не теряет готовую работу предыдущих продуктов.

Требование владельца сформулировано прямо: **смерть на 2-м продукте из 3 не должна
уничтожать 1-й**. До правки Pass B (embed → физическая сборка → персистенция) шёл после
**всех** продуктов и держал чанки всех продуктов в RAM: четыре реальных прогона дали
``corpus_docs > 0`` (1550 / 1203 / 518 / 13) и ``corpus_chunks = 0`` — работа посчитана,
но в БД её нет. Персистенция документов (``_persist_docs`` в ``_finish_one``) была
per-product, чанков — нет.

Тест фиксирует новое поведение на жёстком прерывании (``BaseException``, как OOM/прерывание
оператора) посреди второго продукта из трёх:

* продукт 1 — ``corpus_chunks > 0``, строка ``corpus_coverage``, физическая партиция на
  диске;
* строки ``index_manifest`` нет — PK включает ``composition_id``, а он выводится только
  после **всех** продуктов (состав когорты); это документированное поведение, не потеря;
* продукты 2/3 не выдуманы (нет ни чанков, ни покрытия);
* сеанс краула закрыт терминально (``failed``), а не оставлен ``running`` навсегда.

Нейтральные фикстуры: ``p1/p2/p3``.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Callable
from pathlib import Path

import pytest

from core.adapters.index_npy import NpyIndexBuilder
from core.domain import (
    AnchorRelation,
    Cohort,
    CohortEntry,
    CrawlSessionStatus,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import build_corpus
from core.pipeline.crawl_session import default_session_id
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash
from core.util.index_partition import is_committed, read_manifest

try:  # необязательная зависимость (extra ``retrieval``)
    import numpy as _np  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - зависит от окружения
    _np = None  # type: ignore[assignment]

requires_numpy = pytest.mark.skipif(_np is None, reason="numpy не установлен (extra retrieval)")

RUN_ID = "run-interrupt"
SESSION_ID = default_session_id(RUN_ID)
PACKAGE = "case"
TS = "2026-01-01T00:00:00+00:00"

URL_P1 = "https://docs.test/p1/index"
URL_P2 = "https://docs.test/p2/index"
URL_P3 = "https://docs.test/p3/index"

# --------------------------------------------------------------------------- #
# Нейтральные фикстуры/заглушки портов
# --------------------------------------------------------------------------- #

def _plan(products: tuple[str, ...]) -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=list(products),
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=[],
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id=PACKAGE,
            frozen_at=_dt.date(2026, 1, 1),
            entries=[
                CohortEntry(
                    product_id=product_id,
                    role_overlap=1.0,
                    selection_reason="fixture",
                    anchor_relation=(
                        AnchorRelation.ANCHOR
                        if product_id == "p1"
                        else AnchorRelation.ANALOG
                    ),
                )
                for product_id in products
            ],
        ),
        golden=[],
    )

def _make_run(store: SqliteStore) -> None:
    """Строка прогона: FK ``crawl_sessions.run_id`` требует её для durable-краула."""
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id=PACKAGE,
            anchor="p1",
            cohort=PACKAGE,
            products=["p1", "p2", "p3"],
            package=PACKAGE,
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )

class _AbortCrawl(BaseException):
    """Жёсткое прерывание прогона (аналог OOM/прерывания): ``Exception``-изоляция его не ловит."""

class _AbortCrawler:
    """Краулер: ``url -> bytes``, на ``abort_url`` «убивает» прогон заданным исключением."""

    version = "crawler-1"

    def __init__(
        self,
        pages: dict[str, bytes],
        abort_url: str,
        abort: Callable[[str], BaseException],
    ) -> None:
        self.pages = dict(pages)
        self.abort_url = abort_url
        self.abort = abort
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        if url == self.abort_url:
            raise self.abort(url)
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
        )

class _Extractor:
    version = "extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        return ExtractedDoc(
            text=page.content.decode("utf-8", errors="replace"),
            content_hash=content_hash(page.content),
            extractor_version=self.version,
            page_count=1,
        )

class _Chunker:
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

class _Embedder:
    model_id = "embed-x"
    dim = 3

    def embed(self, texts):  # noqa: ANN001 — структурная заглушка порта
        return [[0.0, 0.1 * i, 0.2] for i in range(len(list(texts)))]

# --------------------------------------------------------------------------- #
# Acceptance: смерть на 2-м продукте из 3 не теряет 1-й
# --------------------------------------------------------------------------- #

@requires_numpy
@pytest.mark.parametrize(
    ("abort", "expected_status", "expected_error_fragment"),
    [
        (lambda url: _AbortCrawl(url), CrawlSessionStatus.INTERRUPTED, "_AbortCrawl"),
        (
            lambda _url: KeyboardInterrupt(),
            CrawlSessionStatus.INTERRUPTED,
            "KeyboardInterrupt",
        ),
    ],
    ids=["exception", "keyboard_interrupt"],
)
def test_death_at_second_product_keeps_first_product_work(
    tmp_path: Path,
    abort: Callable[[str], BaseException],
    expected_status: CrawlSessionStatus,
    expected_error_fragment: str,
) -> None:
    store = SqliteStore(":memory:")
    _make_run(store)
    index_root = tmp_path / "index_store"
    builder = NpyIndexBuilder(index_root, lexical_engine_version="tantivy-0.26.2")
    crawler = _AbortCrawler(
        {URL_P1: b"first product body", URL_P3: b"third product body"},
        abort_url=URL_P2,
        abort=abort,
    )
    try:
        with pytest.raises(type(abort(URL_P2))):
            build_corpus(
                _plan(("p1", "p2", "p3")),
                crawler,
                _Extractor(),
                _Chunker(),
                store,
                urls={"p1": [URL_P1], "p2": [URL_P2], "p3": [URL_P3]},
                raw_root=tmp_path / "raw",
                index_root=index_root,
                embedder=_Embedder(),
                index_builder=builder,
                package_id=PACKAGE,
                run_id=RUN_ID,
            )

        # 1. Чанки первого продукта персистентны (не только в RAM).
        chunk_rows = list(
            store.conn.execute(
                "SELECT chunk_id, product_id, build_id FROM corpus_chunks"
            )
        )
        assert [row["product_id"] for row in chunk_rows] == ["p1"]
        partition_build_id = chunk_rows[0]["build_id"]
        assert partition_build_id.startswith("pb_")

        # 2. Покрытие первого продукта записано на его границе (Ф0/O3).
        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.expected_sources == 1 and coverage.observed_sources == 1
        assert coverage.complete is True

        # 3. Физическая партиция первого продукта — на диске и зафиксирована
        #    («сначала диск, потом БД»: чанки появились только после build).
        assert is_committed(index_root, PACKAGE, partition_build_id, "p1")
        assert read_manifest(index_root, PACKAGE, partition_build_id, "p1")[
            "product_id"
        ] == "p1"

        # 4. Строки ``index_manifest`` нет: её PK включает ``composition_id``, а он
        #    выводится после **всех** продуктов (состав когорты). Документированное
        #    поведение: чанки и партиция переживают прерывание, реестр — нет.
        assert list(store.conn.execute("SELECT 1 FROM index_manifest")) == []

        # 5. Продукты 2/3 не выдуманы: ни чанков, ни строк покрытия.
        assert store.read_corpus_coverage(RUN_ID, "p2") is None
        assert store.read_corpus_coverage(RUN_ID, "p3") is None

        # 6. Сеанс краула закрыт терминально: убитый прогон виден как убитый, а не
        #    как «в полёте» (иначе ``hound crawl status`` врёт). Статус — из словаря:
        #    ``interrupted`` («исполнение прервано, тело не финализировано»), а не
        #    ``failed`` (сбой, который конвейер наблюдал) и не ``cancelled`` (снят
        #    оператором). Причина названа в ``error`` — тип исключения различает
        #    «оператор снял» и «фаза упала».
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is expected_status
        assert session.finished_at is not None
        assert expected_error_fragment in (session.error or "")
        # Прерывание действительно на втором продукте: третий не тронут.
        assert crawler.calls == [URL_P1, URL_P2]
    finally:
        store.close()


@requires_numpy
def test_normal_finish_is_not_overwritten_by_the_interrupt_handler(
    tmp_path: Path,
) -> None:
    """Штатный финал не переписывается обработчиком прерывания (регресс-защита).

    Обработчик срабатывает только когда сеанс **не** был финализирован штатно: иначе
    любой сбой после ``finalize_session`` (например, при записи провенанса композиции)
    переписывал бы легитимный ``completed`` на статус прерывания и врал о крауле,
    который на самом деле дошёл до конца.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    index_root = tmp_path / "index_store"
    try:
        result = build_corpus(
            _plan(("p1", "p2", "p3")),
            _AbortCrawler(
                {URL_P1: b"one", URL_P2: b"two", URL_P3: b"three"},
                abort_url="",
                abort=lambda url: _AbortCrawl(url),
            ),
            _Extractor(),
            _Chunker(),
            store,
            urls={"p1": [URL_P1], "p2": [URL_P2], "p3": [URL_P3]},
            raw_root=tmp_path / "raw",
            index_root=index_root,
            embedder=_Embedder(),
            index_builder=NpyIndexBuilder(
                index_root, lexical_engine_version="tantivy-0.26.2"
            ),
            package_id=PACKAGE,
            run_id=RUN_ID,
        )
        assert result.status == "ok"
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.COMPLETED
        # Все три продукта дошли до конца: чанки и реестр на месте.
        assert len(list(store.conn.execute("SELECT 1 FROM corpus_chunks"))) == 3
        assert len(list(store.conn.execute("SELECT 1 FROM index_manifest"))) == 3
    finally:
        store.close()
