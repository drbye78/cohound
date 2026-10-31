"""F4 (17 §6): окно экстракции — свойство **корня**, а не общей очереди.

Зачем это существует. Очередь доводки экстракции — фазовая (C1): в ней лежат задания
нескольких корней. Пока окно считалось ``len(extract_pending)``, корень платил за чужое:
ждал полноты окна, которого не использовал, а ``popleft()`` забирал **чужое** задание, то
есть доводил чужой хвост. При полосах это сериализовало бы корни на пуле экстракции и
подменило бы окно памяти максимумом по корням.

Что проверяется. Счёт и выборка по ``item.root``: задания соседа не пополняют окно и не
изымаются, а порядок внутри корня остаётся FIFO. Пока полоса одна, в очереди лежат
задания только текущего корня, и новое условие совпадает с прежним — это тоже
утверждение (последний тест): перенос F4 не меняет поведение.
"""

from __future__ import annotations

from collections import deque
from concurrent.futures import Future
from pathlib import Path
from typing import Any

from core.domain import DocumentLevel, PdfPipeline
from core.pipeline import corpus_commit
from core.pipeline.corpus_commit import PendingExtract, RootDrain
from core.pipeline.corpus_index import _CorpusStageTimings
from core.pipeline.crawl_frontier import CrawlLimits, Frontier
from core.pipeline.extraction_runtime import _ExtractPools
from core.ports.types import FetchMode, FetchPlan, RawPage

#: Уровень/механизм PDF фазового пула: окно в ключе — своё у каждого корня.
_LEVEL = DocumentLevel.TEXT
_PDF = PdfPipeline.STRUCTURED


def _resolved(value: Any) -> Future[Any]:
    """Уже завершённый ``future``: ожидание пула в тесте не нужно."""
    future: Future[Any] = Future()
    future.set_result(value)
    return future


def _pending(root: RootDrain, url: str, *, log: list[str], window: int = 2) -> PendingExtract:
    """Задание доводки «в полёте» с записывающим коммитом (без реальной экстракции)."""

    def _commit(extracted: Any, error: str | None, kind: Any) -> None:
        log.append(url)

    return PendingExtract(
        pid="p1",
        url=url,
        depth=0,
        page=RawPage(
            url=url,
            content=b"",
            fetched_at="2026-01-01T00:00:00+00:00",
            fetch_mode=FetchMode.HTTP,
            lang=None,
            status_code=200,
            content_type="text/html",
            engine=None,
        ),
        raw_path=Path("/nowhere") / f"{abs(hash(url))}.bin",
        fetch_plan=FetchPlan(),
        freshly_fetched=True,
        future=_resolved("extracted"),
        root=root,
        commit=_commit,
        pool_key=(_LEVEL, _PDF, window),
    )


def _drain_for_window(
    queue: deque[PendingExtract],
    root: RootDrain,
    *,
    log: list[str],
    window: int = 2,
) -> None:
    """Один ход ``drain_for_window`` для корня — без драйвера (F4: шаги, не волна)."""
    corpus_commit.drain_for_window(
        extract_pending=queue,
        extract_window=window,
        frontier=Frontier("https://a.test/", CrawlLimits()),
        limits=CrawlLimits(),
        root_drain=root,
        pools=_ExtractPools(),
        errors=[],
        timings=_CorpusStageTimings(),
        extract_timeout=60.0,
        extract_finalizers=deque(),
    )


def test_counts_and_pops_belong_to_the_root_not_to_the_queue() -> None:
    """Счёт и изъятие — по ``item.root``: голова **общей** очереди не «своя» по факту."""
    log: list[str] = []
    root_a = RootDrain()
    root_b = RootDrain()
    a1 = _pending(root_a, "https://a.test/1", log=log)
    b1 = _pending(root_b, "https://b.test/1", log=log)
    a2 = _pending(root_a, "https://a.test/2", log=log)
    queue = deque([b1, a1, a2])

    assert (
        corpus_commit.pending_count_for_root(
            extract_pending=queue, root_drain=root_a
        )
        == 2
    )
    assert (
        corpus_commit.pending_count_for_root(
            extract_pending=queue, root_drain=root_b
        )
        == 1
    )

    # Голова очереди — чужое задание; корень A всё равно берёт своё, и первое — своё же.
    assert (
        corpus_commit.popleft_for_root(extract_pending=queue, root_drain=root_a)
        is a1
    )
    assert list(queue) == [b1, a2]
    assert (
        corpus_commit.popleft_for_root(extract_pending=queue, root_drain=root_b)
        is b1
    )


def test_window_of_one_root_is_not_filled_by_another_root() -> None:
    """Окно A не пополняется заданиями B: 1 < W = 2 ⇒ доводить нечего.

    Прежнее условие (``len(extract_pending) >= W``) здесь было бы истинно и довело бы
    **чужое** задание — то самое «окно по глобальной очереди», которое снимает F4.
    """
    log: list[str] = []
    root_a = RootDrain()
    root_b = RootDrain()
    queue = deque(
        [
            _pending(root_b, "https://b.test/1", log=log),
            _pending(root_a, "https://a.test/1", log=log),
            _pending(root_b, "https://b.test/2", log=log),
        ]
    )
    for item in queue:
        item.root.pending += 1

    _drain_for_window(queue, root_a, log=log)
    # Ни одно задание не доведено: своё окно A не полно, а чужого корень не берёт.
    assert log == []
    assert len(queue) == 3
    assert root_a.pending == 1
    assert root_b.pending == 2

    # Окно A полно — доводится **своё** старшее задание (FIFO корня), чужое остаётся.
    # Доводка идёт до ``own < W`` — это прежняя семантика окна, F4 её не меняет.
    queue.append(_pending(root_a, "https://a.test/2", log=log))
    root_a.pending += 1
    _drain_for_window(queue, root_a, log=log)
    assert log == ["https://a.test/1"]
    assert root_a.pending == 1
    assert [item.url for item in queue] == [
        "https://b.test/1",
        "https://b.test/2",
        "https://a.test/2",
    ]


def test_single_lane_keeps_the_previous_condition() -> None:
    """Пока полоса одна, прежнее условие и новое совпадают — перенос без смены поведения."""
    log: list[str] = []
    root = RootDrain()
    queue = deque(_pending(root, f"https://a.test/{i}", log=log) for i in range(4))
    assert (
        corpus_commit.pending_count_for_root(
            extract_pending=queue, root_drain=root
        )
        == len(queue)
    )
    _drain_for_window(queue, root, log=log)
    # W = 2: доводка идёт, пока ``own >= W``, то есть до ``own == W - 1`` — ровно то же,
    # что делало прежнее условие ``len(extract_pending) >= W`` при одном корне в очереди.
    assert log == [
        "https://a.test/0",
        "https://a.test/1",
        "https://a.test/2",
    ]
    assert [item.url for item in queue] == ["https://a.test/3"]
