"""Ф1: батчевый эмбеддинг с прогрессом, heartbeat и границей сеанса краула.

Три дефекта наблюдения 2026-10-25 (``docs/reports/measurements/2026-10-25-run-stall-embedding.md``),
которые здесь закрываются тестами:

1. **Эмбеддинг одним вызовом.** ``embedder.embed([c.text for c in chunks])`` — участок без
   точек жизни: прогон простоял 24 минуты на 505 % CPU с неподвижным heartbeat, и «работает»
   было неотличимо от «повесился». Теперь эмбеддинг идёт внешними батчами
   (:data:`core.pipeline.corpus.EMBED_BATCH_CHUNKS`), на каждой границе — ``heartbeat.touch()``
   и снимок прогресса. Результат обязан остаться **побитово** тем же: порядок векторов равен
   порядку чанков (A3, детерминизм), поэтому это не ``execution_change``.
2. **Терминальный статус краула зависел от пост-краульной работы.** ``finalize_session``
   стоял после Pass B, и сеанс оставался ``running`` всё время эмбеддинга — ``stale_reason``
   врал о живом прогоне. Теперь сеанс закрывается на границе краула (после дренажа), а
   исключение **после** штатной финализации легитимный ``completed`` не переписывает.
3. **Пропускная способность.** Темп и остаток считаются по фактическим батчам текущего
   прогона (:class:`core.pipeline.corpus._EmbedThroughput`), а не по константе; в прогресс
   идут ``N/M``, прошедшее время, темп и ETA.

Нейтральные фикстуры: ``p1``, ``case``, ``fake-embedder``.
"""

from __future__ import annotations

import datetime as _dt
import math
import pathlib
from pathlib import Path
from typing import Any

import pytest

from core.domain import (
    AnchorRelation,
    Cohort,
    CohortEntry,
    CorpusChunk,
    CrawlSessionStatus,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import (
    EMBED_BATCH_CHUNKS,
    build_corpus,
    format_embed_progress,
)
from core.pipeline.corpus_index import (
    _embed_and_build,
    _embed_batched,
    _EmbedThroughput,
    _make_embed_reporter,
)
from core.pipeline.crawl_session import default_session_id
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash
from tests.fakes.ports import FakeEmbedder, FakeIndexBuilder

RUN_ID = "run-embed"
SESSION_ID = default_session_id(RUN_ID)
PACKAGE = "case"
TS = "2026-01-01T00:00:00+00:00"
URL_P1 = "https://docs.test/p1/index"

#: Текст документа длиннее любого порога «пустого тела»: режем его на много чанков.
BODY = b"x" * 400


# --------------------------------------------------------------------------- #
# Фикстуры/заглушки портов (нейтральные)
# --------------------------------------------------------------------------- #

def _chunks(count: int, *, product_id: str = "p1") -> list[CorpusChunk]:
    """``count`` различных чанков одного продукта (разный текст — нет dedup)."""
    return [
        CorpusChunk(
            chunk_id=f"d{i}:0",
            doc_id=f"d{i}",
            product_id=product_id,
            url=URL_P1,
            heading_path="h1",
            chunk_no=0,
            text=f"chunk text {i}",
        )
        for i in range(count)
    ]


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
            products=["p1"],
            package=PACKAGE,
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )


class _Crawler:
    version = "crawler-1"

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта, FetchPlan frozen
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
            text=_page_bytes(page).decode("utf-8", errors="replace"),
            content_hash=content_hash(_page_bytes(page)),
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    """Чанкер с объявленным числом чанков на документ (цена эмбеддинга — по чанкам)."""

    version = "chunker-1"

    def __init__(self, per_doc: int) -> None:
        self.per_doc = per_doc

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        return [
            Chunk(
                chunk_id=f"{document_id}:{i}",
                chunk_no=i,
                text=f"{doc.text} #{i}",
                heading_path="h1",
                context_prefix="ctx",
                content_hash=content_hash(f"{document_id}:{i}".encode()),
            )
            for i in range(self.per_doc)
        ]


class _SessionWatchEmbedder:
    """Эмбеддер, который на каждом вызове читает **durable** статус сеанса из стора.

    Так проверяется граница: к моменту начала эмбеддинга сеанс краула уже терминален,
    то есть ``stale_reason`` не может назвать эмбеддинг причиной протухания живого краула.
    """

    model_id = "fake-embedder"
    dim = 3

    def __init__(self, store: SqliteStore) -> None:
        self.store = store
        self.calls: list[int] = []
        self.statuses: list[CrawlSessionStatus | None] = []

    def embed(self, texts):  # noqa: ANN001 — структурная заглушка порта
        batch = list(texts)
        self.calls.append(len(batch))
        session = self.store.get_crawl_session(SESSION_ID)
        self.statuses.append(session.status if session is not None else None)
        return [[0.01 * i, 0.2, 0.3] for i in range(len(batch))]


class _AbortEmbedder:
    """Эмбеддер, падающий на первом батче заданным исключением."""

    model_id = "fake-embedder"
    dim = 3

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc
        self.calls = 0

    def embed(self, texts):  # noqa: ANN001 — структурная заглушка порта
        self.calls += 1
        raise self.exc


class _RecordingHeartbeat:
    """Стенд-in для :class:`~core.pipeline.liveness.Heartbeat`: считает точки жизни."""

    def __init__(self) -> None:
        self.touches = 0

    def touch(self, *, force: bool = False) -> bool:  # noqa: ARG002 — форма порта
        self.touches += 1
        return True


class _CountsStore:
    """Минимальный стор для колбэка: только агрегат сеанса."""

    def __init__(self, counts: dict[str, int]) -> None:
        self._counts = dict(counts)
        self.calls = 0

    def crawl_session_counts(self, session_id: str) -> dict[str, int]:  # noqa: ARG002
        self.calls += 1
        return dict(self._counts)


# --------------------------------------------------------------------------- #
# 1. Батчевый эмбеддинг: побитовое равенство одному вызову, порядок, память
# --------------------------------------------------------------------------- #

def test_batched_embedding_is_bitwise_equal_to_a_single_call() -> None:
    """Нарезка на батчи не меняет ни одного вектора и не меняет порядок (A3)."""
    total = EMBED_BATCH_CHUNKS * 2 + 5
    chunks = _chunks(total)
    embedder = FakeEmbedder(dim=4)
    marks: list[tuple[int, int, float]] = []

    def _mark(done: int, total_count: int, elapsed: float) -> None:
        marks.append((done, total_count, elapsed))

    # 16 §5.1: устройство счёта — обязательный keyword (дефолт объявленной оси).
    vectors = _embed_batched(embedder, chunks, embed_device="cpu", on_batch=_mark)

    # Побитовое равенство одному вызову на том же входе (тем же адаптером).
    assert vectors == FakeEmbedder(dim=4).embed([c.text for c in chunks])
    # Нарезка: два полных батча и остаток, порядок — по порядку чанков.
    assert [len(call) for call in embedder.calls] == [
        EMBED_BATCH_CHUNKS,
        EMBED_BATCH_CHUNKS,
        5,
    ]
    assert [c for call in embedder.calls for c in call] == [c.text for c in chunks]
    # «Сколько из скольких» монотонно и заканчивается полным счётом.
    assert [done for done, _total, _elapsed in marks] == [
        EMBED_BATCH_CHUNKS,
        EMBED_BATCH_CHUNKS * 2,
        total,
    ]
    assert all(tot == total for _done, tot, _elapsed in marks)


def test_embed_and_build_batches_and_passes_all_vectors_in_order() -> None:
    """``_embed_and_build`` режет эмбеддинг по батчам, но отдаёт сборщику все векторы."""
    total = EMBED_BATCH_CHUNKS + 3
    chunks = _chunks(total)
    embedder = FakeEmbedder(dim=4)
    builder = FakeIndexBuilder()
    errors: list[str] = []
    reported: list[int] = []

    ok = _embed_and_build(
        embedder,
        builder,
        "p1",
        chunks,
        package_id=PACKAGE,
        chunker_version="chunker-1",
        embed_model_id="fake-embedder",
        embed_device="cpu",
        extractor_version="extractor-1",
        lexical_engine_version="fake-bm25-1",
        vector_store="fake",
        product_content_token="token",
        snapshot_date="2026-01-01",
        partition_build_id="pb_fixture",
        errors=errors,
        on_batch=lambda done, _total, _elapsed: reported.append(done),
    )

    assert ok is True and errors == []
    # Границы батчей видны снаружи (единственная точка наблюдения Pass B).
    assert reported == [EMBED_BATCH_CHUNKS, total]
    # Векторы — в порядке чанков и побитово те же, что при одном вызове.
    assert builder.vectors_by_product["p1"] == FakeEmbedder(dim=4).embed(
        [c.text for c in chunks]
    )


# --------------------------------------------------------------------------- #
# 2. Прогресс и heartbeat на границе каждого батча
# --------------------------------------------------------------------------- #

def test_reporter_touches_heartbeat_and_emits_measured_progress() -> None:
    """Каждый батч даёт точку жизни и снимок прогресса с темпом и остатком."""
    heartbeat = _RecordingHeartbeat()
    store = _CountsStore({"fetched": 7, "pending": 0})
    snapshots: list[dict[str, int]] = []
    throughput = _EmbedThroughput()

    report = _make_embed_reporter(
        heartbeat=heartbeat,  # type: ignore[arg-type] — стенд-in формы Heartbeat
        progress=snapshots.append,
        store=store,
        session="ses",
        throughput=throughput,
    )
    total = EMBED_BATCH_CHUNKS * 3
    for step in (1, 2, 3):
        report(EMBED_BATCH_CHUNKS * step, total, float(step))

    # Признак жизни — на **каждой** границе, а не по расписанию дренажа.
    assert heartbeat.touches == 3
    assert len(snapshots) == 3
    assert store.calls == 3
    # Агрегат сеанса не подменён: эмбеддинг дополняет его, а не заменяет.
    assert snapshots[-1]["fetched"] == 7
    assert [snap["embed_chunks"] for snap in snapshots] == [
        EMBED_BATCH_CHUNKS,
        EMBED_BATCH_CHUNKS * 2,
        total,
    ]
    assert all(snap["embed_chunks_total"] == total for snap in snapshots)
    assert [snap["embed_elapsed_s"] for snap in snapshots] == [1, 2, 3]
    # Темп — измеренный (64 чанка/с здесь), остаток уменьшается.
    assert snapshots[0]["embed_chunks_per_min"] == EMBED_BATCH_CHUNKS * 60
    assert snapshots[0]["embed_eta_s"] > snapshots[-1]["embed_eta_s"]
    assert snapshots[-1]["embed_eta_s"] == 0


def test_reporter_measures_even_without_progress_callback() -> None:
    """Без ``--follow`` (нет колбэка) темп всё равно измеряется — он публикуется в логе."""
    heartbeat = _RecordingHeartbeat()
    throughput = _EmbedThroughput()
    report = _make_embed_reporter(
        heartbeat=heartbeat,  # type: ignore[arg-type] — стенд-in формы Heartbeat
        progress=None,
        store=_CountsStore({}),
        session="ses",
        throughput=throughput,
    )
    report(10, 100, 1.0)
    assert heartbeat.touches == 1
    assert throughput.rate == pytest.approx(10.0)


# --------------------------------------------------------------------------- #
# 3. Пропускная способность: сглаживание, ETA, человекочитаемая строка
# --------------------------------------------------------------------------- #

def test_throughput_smooths_batches_and_reports_remaining_time() -> None:
    """Темп — EMA по фактическим батчам; остаток — в секундах, из этого темпа."""
    throughput = _EmbedThroughput(alpha=0.3)
    assert throughput.rate is None
    assert throughput.eta_seconds(0, 100) is None

    assert throughput.observe(10, 1.0) == pytest.approx(10.0)  # первый замер — как есть
    # Второй батч вдвое медленнее: 10 чанков за 2 с = 5 чанк/с; EMA: 0.3·5 + 0.7·10.
    assert throughput.observe(20, 3.0) == pytest.approx(8.5)
    assert throughput.eta_seconds(20, 100) == round(80 / 8.5)

    # Замер без прироста не обнуляет темп (нулевой интервал — не остановка).
    assert throughput.observe(20, 3.0) == pytest.approx(8.5)
    # Работа закончена — остаток ноль, а не «нет данных».
    assert throughput.eta_seconds(100, 100) == 0


def test_progress_line_names_counts_rate_and_remaining_minutes() -> None:
    """Человекочитаемая форма: ``N/M чанков, X чанк/с, осталось ~Y мин``."""
    assert (
        format_embed_progress(64, 3000, 0.85, 3300)
        == "64/3000 чанков, 0.85 чанк/с, осталось ~55 мин"
    )
    # Завершённая работа: остаток не печатается (это шум, а не факт).
    assert format_embed_progress(3000, 3000, 0.85, 0) == "3000/3000 чанков, 0.85 чанк/с"
    # Темпа ещё нет (первый батч не закрыт) — числа не выдумываются.
    assert format_embed_progress(0, 10, None, None) == "0/10 чанков"
    # Меньше минуты — «~1 мин», а не «~0 мин».
    assert format_embed_progress(9, 10, 1.0, 30).endswith("осталось ~1 мин")


# --------------------------------------------------------------------------- #
# 4. Терминальный статус краула — граница краула, а не фазы корпуса
# --------------------------------------------------------------------------- #

def test_crawl_session_is_terminal_before_embedding_starts(tmp_path: Path) -> None:
    """Сеанс уже терминален в момент начала эмбеддинга (эмбеддинг — не работа краула)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    per_doc = EMBED_BATCH_CHUNKS + 6  # > одного батча: наблюдение обязано быть многократным
    embedder = _SessionWatchEmbedder(store)
    try:
        result = build_corpus(
            _plan(("p1",)),
            _Crawler({URL_P1: BODY}),
            _Extractor(),
            _Chunker(per_doc),
            store,
            urls={"p1": [URL_P1]},
            raw_root=tmp_path / "raw",
            index_root=tmp_path / "index_store",
            embedder=embedder,
            index_builder=FakeIndexBuilder(),
            package_id=PACKAGE,
            run_id=RUN_ID,
        )

        assert result.status == "ok"
        # Эмбеддинг действительно шёл батчами (а не одним вызовом).
        assert len(embedder.calls) == math.ceil(per_doc / EMBED_BATCH_CHUNKS)
        # ... и на каждом вызове durable-статус сеанса был уже терминальным.
        assert set(embedder.statuses) == {CrawlSessionStatus.COMPLETED}

        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.COMPLETED
        assert session.finished_at is not None
    finally:
        store.close()


def test_embedding_failure_does_not_leave_the_session_running(tmp_path: Path) -> None:
    """Сбой эмбеддинга (изолированный в ``errors``) не отменяет финал **краула**."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        result = build_corpus(
            _plan(("p1",)),
            _Crawler({URL_P1: BODY}),
            _Extractor(),
            _Chunker(4),
            store,
            urls={"p1": [URL_P1]},
            raw_root=tmp_path / "raw",
            index_root=tmp_path / "index_store",
            embedder=_AbortEmbedder(RuntimeError("embed boom")),
            index_builder=FakeIndexBuilder(),
            package_id=PACKAGE,
            run_id=RUN_ID,
        )
        assert result.status == "ok"
        assert any("index build" in err and "embed boom" in err for err in result.errors)
        # Ни одной строки corpus_chunks: «сначала диск, потом БД» (K2/§13).
        assert list(store.conn.execute("SELECT 1 FROM corpus_chunks")) == []
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        # Краул дошёл до конца — его статус про эмбеддинг ничего не утверждает.
        assert session.status is CrawlSessionStatus.COMPLETED
    finally:
        store.close()


def test_exception_after_finalization_keeps_the_legit_status(tmp_path: Path) -> None:
    """``KeyboardInterrupt`` на эмбеддинге не переписывает легитимный ``completed``."""
    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        with pytest.raises(KeyboardInterrupt):
            build_corpus(
                _plan(("p1",)),
                _Crawler({URL_P1: BODY}),
                _Extractor(),
                _Chunker(4),
                store,
                urls={"p1": [URL_P1]},
                raw_root=tmp_path / "raw",
                index_root=tmp_path / "index_store",
                embedder=_AbortEmbedder(KeyboardInterrupt()),
                index_builder=FakeIndexBuilder(),
                package_id=PACKAGE,
                run_id=RUN_ID,
            )
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.COMPLETED
        assert "phase interrupted" not in (session.error or "")
    finally:
        store.close()


def _page_bytes(page: Any) -> bytes:
    """Байты страницы: тело или retained-файл (потребитель очереди не держит тело в RAM).

    До F5 конверсия шла внутри краула, и ``RawPage.content`` страницы был непуст. Теперь
    документ конвертируется из строки очереди: ``content`` пуст, тело лежит по
    ``source_path`` (I19, ``corpus_raw/{hash}.bin``) — ровно так его получает воркер пула.
    Фейковый экстрактор обязан читать тот же носитель, что и адаптер, иначе он проверял бы
    не контракт порта, а прежний способ доставки байтов.
    """
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return pathlib.Path(path).read_bytes() if path else b""
