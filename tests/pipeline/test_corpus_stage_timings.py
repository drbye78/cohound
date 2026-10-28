"""Декомпозиция времени фазы ``corpus`` по стадиям (наблюдаемость, **не** контракт).

``elapsed_seconds`` продукта говорит «сколько», но не «на что»: без бакетов вопрос «где
уходит время» не измеряется. Здесь проверяется **проводка** измерений, а не их величины:

* бакеты строки продукта неотрицательны, а ``overhead_s`` считается **формулой**
  (``elapsed − Σ бакетов``) и не зажимается в ноль — отрицательное значение означало бы
  дефект инструментирования и обязано быть видно, а не спрятано;
* инструментирование не меняет идентичность: ``execution_contract_digest`` и ``pb``/
  ``content_digest``/``index_build_id`` на фиксированном корпусе **побайтово** те же, что
  на HEAD до правки (пин ниже), и ни одно новое поле не попало в контракт;
* отказ ``index_builder.build`` не теряет бакет ``build_s``: время до исключения — тоже
  измеренный факт.

Фикстуры нейтральны (§1.3): ``p1``/``case``/``docs.test``, in-memory порты, без сети.
"""

from __future__ import annotations

import datetime as _dt
import json
import time
from pathlib import Path
from typing import Any

import pytest

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CorpusChunk,
    CrawlRunConfig,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import (
    _CorpusStageTimings,
    _embed_and_build,
    _stage_timings_aggregate,
    build_corpus,
)
from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS, execution_contract_digest
from core.pipeline.resources import ResourcesObservation, build_resources_block
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchPlan, RawPage
from core.util.hashing import content_hash
from tests.fakes.ports import FakeEmbedder, FakeIndexBuilder

RUN_ID = "run-stage-timings"
PACKAGE = "case"
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"

ROOT = "https://docs.test/"
CHILD_A = "https://docs.test/a"
CHILD_B = "https://docs.test/b"

#: Пин идентичности, снятый на HEAD **до** инструментирования (a0f933, тем же скриптом,
#: что и здесь: тот же план, те же страницы, те же фейковые порты). Инструментирование
#: наблюдаемости не вправе сдвинуть ни одно из значений: иначе это уже не наблюдение,
#: а смена условий измерения.
PINNED_PARTITION_BUILD_ID = "pb_1f3e739c2d35af4ef2162694"
PINNED_CONTENT_DIGEST = "761aa3da08cda85c38e68bdf"
PINNED_INDEX_BUILD_ID = "cb_a9ec7f9f8e9c85247ffce7eb"
PINNED_EXECUTION_CONTRACT_DIGEST = "c69774e1ba2ee321838838a9"

#: Поля, которые обязана нести строка продукта (``product_end``) и её Pass B-продолжение.
CRAWL_BUCKETS = ("fetch_wait_s", "extract_wait_s", "chunk_s")
INDEX_BUCKETS = ("embed_s", "build_s")


def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
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


class _Crawler:
    """Краулер ``url → bytes`` без сети."""

    version = "crawler-1"
    engine = "http"

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
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
    """Ровно два чанка на документ: нарезка детерминирована."""

    version = "chunker-1"

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        return [
            Chunk(
                chunk_id=f"{document_id}:{index}",
                chunk_no=index,
                text=f"{doc.text} #{index}",
                heading_path="h1",
                context_prefix="ctx",
                content_hash=doc.content_hash,
            )
            for index in range(2)
        ]


class _ExplodingBuilder:
    """Сборщик, падающий **после** работы: проверяет, что бакет не теряется при отказе."""

    vector_store = "fake"
    lexical_engine_version = "fake-bm25-1"

    def __init__(self, delay_s: float = 0.02) -> None:
        self.delay_s = delay_s
        self.commit_calls = 0

    def build(
        self,
        chunks: Any,
        vectors: Any,
        meta: Any,
    ) -> str:
        time.sleep(self.delay_s)
        raise RuntimeError("build refused")

    def commit(self) -> None:
        self.commit_calls += 1


def _pages() -> dict[str, bytes]:
    return {
        ROOT: b'<html><a href="/a">a</a><a href="/b">b</a></html>',
        CHILD_A: b"<html>a</html>",
        CHILD_B: b"<html>b</html>",
    }


def _chunks(count: int) -> list[CorpusChunk]:
    return [
        CorpusChunk(
            chunk_id=f"d{i}:0",
            doc_id=f"d{i}",
            product_id="p1",
            url=ROOT,
            heading_path="h1",
            chunk_no=0,
            text=f"chunk text {i}",
        )
        for i in range(count)
    ]


def _build(tmp_path: Path, **kwargs: Any) -> Any:
    pages = _pages()
    return build_corpus(
        _plan(),
        _Crawler(pages),
        _Extractor(),
        _Chunker(),
        None,
        urls={"p1": [ROOT]},
        raw_root=tmp_path / "raw",
        **kwargs,
    )


def _events(result: Any, kind: str) -> list[dict[str, Any]]:
    return [
        entry
        for entry in result.log_entries
        if isinstance(entry, dict) and entry.get("event") == kind
    ]


# --------------------------------------------------------------------------- #
# 1. Бакеты строки продукта: неотрицательны, остаток — по формуле
# --------------------------------------------------------------------------- #


def test_product_rows_carry_stage_buckets(tmp_path: Path) -> None:
    """``product_end`` несёт окно краула, ``product_index_end`` — окно Pass B.

    Эмбеддинг/сборка идут **после** ``product_end``, поэтому нулями там не выдаются:
    каждое окно публикует свои бакеты и свой остаток, а полная сумма видна в агрегате.
    """
    result = _build(
        tmp_path,
        embedder=FakeEmbedder(dim=4),
        index_builder=FakeIndexBuilder(),
        index_root=tmp_path / "index",
        package_id=PACKAGE,
        run_id=RUN_ID,
        snapshot_date="2026-01-01",
        crawl_defaults=CrawlRunConfig(max_depth=1),
    )
    assert result.status == "ok"

    (crawl,) = _events(result, "product_end")
    (index,) = _events(result, "product_index_end")

    for name in (*CRAWL_BUCKETS, "extract_workers", "overhead_s"):
        assert name in crawl, f"строка продукта обязана нести {name}"
    for name in (*INDEX_BUCKETS, "overhead_s", "total_elapsed_s", "index_elapsed_s"):
        assert name in index, f"строка Pass B обязана нести {name}"

    # Бакеты — времена: отрицательных не бывает.
    for event in (crawl, index):
        for name in (*CRAWL_BUCKETS, *INDEX_BUCKETS):
            if name in event:
                assert event[name] >= 0.0, f"{name} отрицателен: {event[name]}"
    # Окно Pass B не может быть отрицательным, а полное окно — короче окна краула.
    assert index["index_elapsed_s"] >= 0.0
    assert index["total_elapsed_s"] >= crawl["elapsed_seconds"]

    # Остаток — формула, а не константа: считаем её независимо от кода фазы.
    pass_a = crawl["fetch_wait_s"] + crawl["extract_wait_s"] + crawl["chunk_s"]
    assert crawl["overhead_s"] == pytest.approx(
        round(crawl["elapsed_seconds"] - pass_a, 3), abs=1e-3
    )
    full = pass_a + index["embed_s"] + index["build_s"]
    assert index["overhead_s"] == pytest.approx(
        round(index["total_elapsed_s"] - full, 3), abs=1e-3
    )


def test_stage_aggregate_is_the_sum_of_products(tmp_path: Path) -> None:
    """Агрегат фазы складывает бакеты сам: оператору не нужно складывать строки руками."""
    result = _build(
        tmp_path,
        embedder=FakeEmbedder(dim=4),
        index_builder=FakeIndexBuilder(),
        index_root=tmp_path / "index",
        package_id=PACKAGE,
        run_id=RUN_ID,
        snapshot_date="2026-01-01",
        crawl_defaults=CrawlRunConfig(max_depth=1),
    )
    observation = next(
        entry
        for entry in result.log_entries
        if isinstance(entry, ResourcesObservation)
    )
    aggregate = observation.stage_timings
    assert set(aggregate) == {"products", "total"}
    assert set(aggregate["products"]) == {"p1"}
    total = aggregate["total"]
    for name in (*CRAWL_BUCKETS, *INDEX_BUCKETS, "overhead_s", "elapsed_seconds"):
        assert name in total
    # Один продукт — агрегат равен его строке (округление то же, источник один).
    assert total["fetch_wait_s"] == pytest.approx(
        aggregate["products"]["p1"]["fetch_wait_s"], abs=1e-3
    )
    # Агрегат доезжает до блока ``metrics.json.resources`` (существующий канал).
    block = build_resources_block(observation=observation)
    assert block["stage_timings"] == aggregate


def test_overhead_is_formula_and_never_clamped() -> None:
    """Отрицательный остаток **пишется как есть**: это дефект инструментирования.

    ``max(0, …)`` спрятал бы двойной учёт бакета (или бакет, покрывающий чужое окно), а
    именно такой дефект и должен быть виден в машинном отчёте.
    """
    timings = _CorpusStageTimings(
        fetch_wait_s=2.0, extract_wait_s=1.0, chunk_s=0.5, embed_s=1.0, build_s=0.5
    )
    crawl = timings.crawl_fields(1.0)
    assert crawl["overhead_s"] == -2.5  # 1.0 − (2.0 + 1.0 + 0.5)
    assert timings.stage_sum() == 5.0
    index = timings.index_fields(3.0)
    assert index["overhead_s"] == -2.0  # 3.0 − 5.0
    aggregate = _stage_timings_aggregate([("p1", 1.0, timings)])
    assert aggregate["total"]["overhead_s"] == -4.0
    assert aggregate["products"]["p1"]["overhead_s"] == -4.0


def test_extract_wait_is_measured_through_the_pool(tmp_path: Path) -> None:
    """Ожидание ``future.result()`` в пуле экстракции попадает в бакет, а окно — рядом.

    Проверка именно **ожидания**: воркер спит на главном потоке не он, а ``_finish_extract``,
    поэтому ``extract_wait_s`` обязан быть > 0 при ``extract_workers=2``, а рядом лежит
    фактическое окно — без него «ожидание» читалось бы как работа.
    """
    from concurrent.futures import ThreadPoolExecutor

    from core.pipeline.corpus import ExtractionPool, _ExtractRequest

    def _slow_worker(request: _ExtractRequest) -> ExtractedDoc:
        time.sleep(0.02)
        content = Path(request.raw_path).read_bytes()
        return ExtractedDoc(
            text=content.decode("utf-8", errors="replace"),
            content_hash=content_hash(content),
            extractor_version="extractor-1",
            page_count=1,
        )

    pool = ExtractionPool(
        pool_factory=lambda workers, _level, **_kwargs: ThreadPoolExecutor(
            max_workers=workers
        ),
        worker=_slow_worker,
    )
    result = _build(
        tmp_path,
        extraction_pool=pool,
        crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=2),
    )
    (crawl,) = _events(result, "product_end")
    assert crawl["extract_workers"] == 2
    assert crawl["extract_wait_s"] > 0.0


def test_extract_window_is_the_widest_of_the_roots() -> None:
    """Окно рядом с ожиданием — максимум по корням: ожидание включает чужие задания."""
    timings = _CorpusStageTimings()
    assert timings.extract_workers is None  # inline-путь: окна нет, а не ноль
    timings.note_extract_window(2)
    timings.note_extract_window(1)
    assert timings.extract_workers == 2
    timings.reset()
    assert timings.extract_workers is None
    assert timings.stage_sum() == 0.0


# --------------------------------------------------------------------------- #
# 2. Инструментирование не меняет идентичность
# --------------------------------------------------------------------------- #


def test_stage_timings_do_not_change_run_identity(tmp_path: Path) -> None:
    """``pb``/``content_digest``/``index_build_id``/``execution_contract_digest`` — те же.

    Значения — пин, снятый на HEAD до инструментирования. Дайджест считается из
    **объявленных** входов, поэтому измерение исполнения не вправе его сдвинуть.
    """
    result = _build(
        tmp_path,
        embedder=FakeEmbedder(dim=4),
        index_builder=FakeIndexBuilder(),
        index_root=tmp_path / "index",
        package_id=PACKAGE,
        run_id=RUN_ID,
        snapshot_date="2026-01-01",
        crawl_defaults=CrawlRunConfig(max_depth=1),
    )
    build = next(
        entry for entry in result.log_entries if type(entry).__name__ == "IndexBuild"
    )
    (entry,) = build.entries
    assert entry.partition_build_id == PINNED_PARTITION_BUILD_ID
    assert entry.content_digest == PINNED_CONTENT_DIGEST
    assert entry.index_build_id == PINNED_INDEX_BUILD_ID

    contract = {name: f"v-{name}" for name in EXECUTION_CONTRACT_FIELDS}
    assert execution_contract_digest(contract) == PINNED_EXECUTION_CONTRACT_DIGEST


def test_no_stage_timing_field_enters_the_execution_contract() -> None:
    """Тайминги — наблюдаемость: ни одно поле бакетов в контракт не входит.

    Иначе измерение времени меняло бы ``run_id`` (и разрывало межцикловый diff).
    """
    for name in (
        *CRAWL_BUCKETS,
        *INDEX_BUCKETS,
        "overhead_s",
        "extract_workers",
        "stage_timings",
        "elapsed_seconds",
    ):
        assert name not in EXECUTION_CONTRACT_FIELDS


# --------------------------------------------------------------------------- #
# 3. Отказ сборки не теряет бакет
# --------------------------------------------------------------------------- #


def test_build_failure_still_reports_build_s() -> None:
    """``index_builder.build`` упал — ``build_s`` всё равно измерен (время до исключения)."""
    timings = _CorpusStageTimings()
    errors: list[str] = []
    ok = _embed_and_build(
        FakeEmbedder(dim=4),
        _ExplodingBuilder(delay_s=0.02),
        "p1",
        _chunks(3),
        package_id=PACKAGE,
        chunker_version="chunker-1",
        embed_model_id="fake-embedder-1",
        extractor_version="extractor-1",
        lexical_engine_version="fake-bm25-1",
        vector_store="fake",
        product_content_token="token",
        snapshot_date="2026-01-01",
        partition_build_id="pb_fixture",
        errors=errors,
        timings=timings,
    )
    assert ok is False
    assert errors and "index build" in errors[0]
    # Бакет не «пропал» вместе с исключением: 20 мс задержки видны в измерении.
    assert timings.build_s >= 0.015
    assert timings.embed_s > 0.0


def test_embed_failure_still_reports_embed_s() -> None:
    """Сбой эмбеддера — тот же принцип: время участка остаётся измеренным."""

    class _ExplodingEmbedder(FakeEmbedder):
        def embed(self, texts: Any) -> Any:
            time.sleep(0.02)
            raise RuntimeError("embed refused")

    timings = _CorpusStageTimings()
    errors: list[str] = []
    ok = _embed_and_build(
        _ExplodingEmbedder(dim=4),
        FakeIndexBuilder(),
        "p1",
        _chunks(2),
        package_id=PACKAGE,
        chunker_version="chunker-1",
        embed_model_id="fake-embedder-1",
        extractor_version="extractor-1",
        lexical_engine_version="fake-bm25-1",
        vector_store="fake",
        product_content_token="token",
        snapshot_date="2026-01-01",
        partition_build_id="pb_fixture",
        errors=errors,
        timings=timings,
    )
    assert ok is False
    assert timings.embed_s >= 0.015
    # Сборка не начиналась — её бакет остаётся нулевым, а не выдуманным.
    assert timings.build_s == 0.0
