"""F0: наблюдения ресурсов фазы корпуса — расписание, чанки на документ, пик воркера.

Здесь проверяется **проводка** измерений из фазы Ш5 наружу
(``docs/architecture/11-production-architecture-strategy.md`` §3.3/§4 F0), а не формулы
(они покрыты ``test_resources.py``):

* разрешённое окно экстракции и его источник (``derived``/``declared``) доезжают до
  ``ResourcesObservation``;
* распределение чанков на документ считается по **уже посчитанным** данным продукта
  (``ingest.docs``/``ingest.chunks``), без SQL на документ;
* пик RSS воркера читается из файла-стока (``{runs}/{run_id}/workers/{pid}.rss``) как
  максимум по содержимому — воркеры рециклятся, ``pid`` переиспользуется.

Порты подменяются детерминированными in-memory заглушками (без сети; I/O — только
``tmp_path`` для retention и стока). Данные нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
import os
from pathlib import Path
from typing import Any

from core.config.deploy import CrawlConfig
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
    RunConfig,
    RunMode,
)
from core.domain.enums import DocumentLevel
from core.pipeline import RunPlan
from core.pipeline.corpus import (
    ExtractionPool,
    _write_worker_peak_rss,
    build_corpus,
)
from core.pipeline.resources import ResourcesObservation
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchPlan,
    RawPage,
)
from core.util.hashing import content_hash

RUN_ID = "run-f0-observation"
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"

ROOT = "https://docs.test/"
CHILD_A = "https://docs.test/a"
CHILD_B = "https://docs.test/b"


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


def _create_run(store: Any) -> None:
    store.create_run(
        RUN_ID,
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


class _Crawler:
    """Краулер ``url → bytes`` без сети; фиксирует вызовы для детерминизма."""

    version = "crawler-1"
    engine = "http"

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
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
    """Ровно **два** чанка на документ: распределение проверяется по числу, а не по тексту."""

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


def _pages() -> dict[str, bytes]:
    return {
        ROOT: (
            b'<html><a href="/a">a</a><a href="/b">b</a></html>'
        ),
        CHILD_A: b"<html>a</html>",
        CHILD_B: b"<html>b</html>",
    }


def _build(
    tmp_path: Path, *, store: Any = None, **kwargs: Any
) -> Any:
    pages = _pages()
    return build_corpus(
        _plan(),
        _Crawler(pages),
        _Extractor(),
        _Chunker(),
        store,
        urls=kwargs.pop("urls", {"p1": [ROOT]}),
        raw_root=tmp_path,
        **kwargs,
    )


def _observation(result: Any) -> ResourcesObservation:
    entries = [
        entry
        for entry in result.log_entries
        if isinstance(entry, ResourcesObservation)
    ]
    assert len(entries) == 1, "фаза корпуса обязана опубликовать одно наблюдение"
    return entries[0]


# --------------------------------------------------------------------------- #
# Расписание экстракции и чанки на документ
# --------------------------------------------------------------------------- #

def test_observation_reports_derived_window_and_chunks_per_document(
    tmp_path: Path,
) -> None:
    """Окно из бюджета + распределение чанков по семействам — в наблюдении фазы.

    ``text`` (пик 855 MB) при бюджете 2000 MB помещает одного воркера; окно **выведено**,
    поэтому источник расписания — ``derived``. Три HTML-документа по два чанка: ``count``
    документов, ``max`` чанков и сумма обязаны совпасть с фактическим выходом фазы.
    """
    result = _build(
        tmp_path,
        crawl_defaults=CrawlRunConfig(max_depth=1),
        crawl_app=CrawlConfig(document_level=DocumentLevel.TEXT),
        derive_extract_workers=True,
        mem_budget_mb=2000,
    )
    assert result.status == "ok"
    observation = _observation(result)
    assert observation.scheduling_source == "derived"
    assert observation.extract_workers == 1
    assert observation.extract_max_tasks_per_child is None
    # Пик воркера не собирался: пула не было (окно 1 ⇒ inline-путь) — не ноль, а None.
    assert observation.workers_peak_rss_mb is None
    assert observation.documents == 3
    assert observation.chunks == 6
    assert observation.chunks_per_doc == {
        "html": {"count": 3, "p50": 2.0, "p90": 2.0, "max": 2.0}
    }


def test_observation_reports_declared_schedule_without_budget(tmp_path: Path) -> None:
    """Без бюджета окно не выводится: расписание объявленное (``declared``)."""
    result = _build(
        tmp_path,
        crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=3),
    )
    observation = _observation(result)
    assert observation.scheduling_source == "declared"
    assert observation.extract_workers == 3
    # Чанки на документ наблюдаются независимо от расписания.
    assert observation.documents == 3
    assert observation.chunks == 6


def test_observation_without_documents_is_empty_not_fake(tmp_path: Path) -> None:
    """Пустой корпус — ``documents=0``/пустое распределение, а не выдуманные семейства."""
    result = _build(tmp_path, urls={}, )
    observation = _observation(result)
    assert observation.documents == 0
    assert observation.chunks == 0
    assert observation.chunks_per_doc == {}
    assert observation.scheduling_source == "declared"


# --------------------------------------------------------------------------- #
# Пик RSS воркера: файл-сток
# --------------------------------------------------------------------------- #

def test_worker_peak_is_read_from_sink_files(tmp_path: Path) -> None:
    """Пик берётся как **максимум** по файлам стока, а не как последний записанный."""
    sink = tmp_path / "workers"
    sink.mkdir()
    (sink / "101.rss").write_text("900.0\n", encoding="utf-8")
    (sink / "102.rss").write_text("1500.5\n", encoding="utf-8")
    (sink / "103.rss").write_text("1200.0\n", encoding="utf-8")
    pool = ExtractionPool(
        pool_factory=lambda *_args, **_kwargs: None,
        worker=lambda request: ExtractedDoc(text="", content_hash=""),
        worker_rss_dir=str(sink),
    )
    assert pool.peak_worker_rss_mb() == 1500.5


def test_worker_peak_absent_is_none_everywhere(tmp_path: Path) -> None:
    """Нет каталога сточных записей / нет файлов — ``None``, не ноль.

    ``None`` здесь значит «пик воркера не измерялся» (inline-путь, ``split``, пустой пул).
    Ноль означал бы «измерен и равен нулю», чего не бывает.
    """
    empty = tmp_path / "empty"
    empty.mkdir()
    (empty / "broken.rss").write_text("not-a-number\n", encoding="utf-8")
    (empty / "negative.rss").write_text("-5\n", encoding="utf-8")
    assert (
        ExtractionPool(
            pool_factory=lambda *_a, **_k: None,
            worker=lambda request: ExtractedDoc(text="", content_hash=""),
            worker_rss_dir=str(empty),
        ).peak_worker_rss_mb()
        is None
    )
    assert (
        ExtractionPool(
            pool_factory=lambda *_a, **_k: None,
            worker=lambda request: ExtractedDoc(text="", content_hash=""),
            worker_rss_dir=str(tmp_path / "missing"),
        ).peak_worker_rss_mb()
        is None
    )
    assert (
        ExtractionPool(
            pool_factory=lambda *_a, **_k: None,
            worker=lambda request: ExtractedDoc(text="", content_hash=""),
        ).peak_worker_rss_mb()
        is None
    )


def test_worker_peak_sink_keeps_maximum_across_pid_reuse(tmp_path: Path) -> None:
    """Воркер пишет максимум: новое поколение с тем же ``pid`` не занижает пик.

    Рециклинг (``extract_max_tasks_per_child``) переиспользует ``pid``, поэтому файл
    стока — не «последнее значение», а максимум поколений; иначе метрика показывала бы
    пик случайного последнего воркера.
    """
    sink = tmp_path / "workers"
    sink.mkdir()
    target = sink / f"{os.getpid()}.rss"
    target.write_text("999999.0\n", encoding="utf-8")
    _write_worker_peak_rss(str(sink))
    assert float(target.read_text(encoding="utf-8").strip()) == 999999.0

    fresh = tmp_path / "fresh"
    _write_worker_peak_rss(str(fresh))
    written = (fresh / f"{os.getpid()}.rss").read_text(encoding="utf-8").strip()
    assert float(written) > 0
    # Запись доехала до пула-читателя без второго канала.
    assert ExtractionPool(
        pool_factory=lambda *_a, **_k: None,
        worker=lambda request: ExtractedDoc(text="", content_hash=""),
        worker_rss_dir=str(fresh),
    ).peak_worker_rss_mb() == float(written)


def test_corpus_observation_carries_worker_peak_from_pool_sink(tmp_path: Path) -> None:
    """Пик воркера из стока пула попадает в наблюдение фазы (``workers_peak_rss_mb``)."""
    sink = tmp_path / "workers"
    sink.mkdir()
    (sink / "201.rss").write_text("1953.0\n", encoding="utf-8")
    (sink / "202.rss").write_text("855.0\n", encoding="utf-8")
    pool = ExtractionPool(
        pool_factory=lambda *_a, **_k: None,
        worker=lambda request: ExtractedDoc(text="", content_hash=""),
        worker_rss_dir=str(sink),
    )
    result = _build(
        tmp_path / "run",
        extraction_pool=pool,
        crawl_defaults=CrawlRunConfig(max_depth=1),
    )
    observation = _observation(result)
    assert observation.workers_peak_rss_mb == 1953.0
    # Окно не выводилось и не объявлялось: расписание — объявленное «без окна».
    assert observation.extract_workers is None
    assert observation.scheduling_source == "declared"
