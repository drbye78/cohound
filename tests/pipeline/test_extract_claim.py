"""F6 (17 §4.2/§4.3): аренда строки, heartbeat, восстановление и backpressure.

Проверяется то, что F6 добавляет к потребителю из F5:

1. **claim эксклюзивен** (§4.2 п.4): два потребителя на одной строке — один ``claimed``,
   второй получает ``False``/``lost`` и **не** считает работу своей; документ
   конвертируется ровно один раз.
2. **просроченная аренда возвращается видимо** (§4.2 п.3): ``pending`` + ``attempts += 1``,
   и каждый факт назван (кто держал, какой URL) — молчаливого воскрешения нет.
3. **исчерпание попыток терминально** (§4.2 п.5): строка становится ``failed`` с
   ``error_kind``; повтор — только явной командой.
4. **heartbeat идёт у долгого документа**: ожидание нарезано, живой потребитель
   продлевает аренду и не выглядит мёртвым.
5. **backpressure публикуется числами, а не настроением** (§4.3): ``backlog_bytes``/
   ``free_bytes``/``waited_s`` в записи ``crawl_backpressure``, сеанс ``partial``,
   причина ``extract_backlog_full``, и **без исключения**.
6. **замер попытки** (``fetch_started_at``/``fetch_ms``) заполняется производителем:
   §7.2 №1 (``observed_min_interval_s``) из операционных меток не восстанавливается.

Нейтральные данные (§1.3): ``p1``, ``docs.test``. Сеть не используется.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest

import tests.pipeline.test_extract_consumer as base
from core.domain import CrawlItemStatus, CrawlSessionStatus, ExtractQueueState
from core.pipeline import corpus_drain
from core.pipeline.corpus import IndexBuild
from core.pipeline.crawl_session import default_session_id
from core.pipeline.extract_queue import (
    EXTRACT_ATTEMPTS_ERROR_KIND,
    BackpressureEvent,
    ExtractQueue,
    wait_for_backlog,
)
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-extract-claim"
SESSION_ID = default_session_id(RUN_ID)
ROOT = base.ROOT
URLS = base.URLS


def _queue(store: SqliteStore) -> ExtractQueue:
    """Durable-очередь того же сеанса — тот же носитель, что у фазы."""
    return ExtractQueue(store=store, session_id=SESSION_ID)


def _build(tmp_path: Path, store: SqliteStore, **kwargs: Any):
    """Сборка корпуса с run_id этого модуля (сеанс — ``crawl:RUN_ID``)."""
    base.RUN_ID = RUN_ID
    base.SESSION_ID = SESSION_ID
    try:
        return base._build(tmp_path, store, **kwargs)
    finally:
        base.RUN_ID = "run-extract-consumer"
        base.SESSION_ID = default_session_id("run-extract-consumer")


def _crawl_only(tmp_path: Path) -> SqliteStore:
    """Краул без конверсии: очередь наполнена, строки ``pending``."""
    store = SqliteStore(":memory:")
    result = _build(tmp_path, store, crawl_only=True)
    assert result.errors == []
    return store


# --------------------------------------------------------------------------- #
# (1) claim эксклюзивен
# --------------------------------------------------------------------------- #


def test_claim_is_exclusive(tmp_path: Path) -> None:
    """Две аренды одной строки: вторая получает ``False`` и не пишет ``claimed_by``.

    ``expect=PENDING`` в ``UPDATE`` — не удобство, а единственный механизм, который
    делает claim атомарным на одно-писательном SQLite: без него два потребителя
    конвертировали бы один документ дважды и оба считали бы работу своей.
    """
    store = _crawl_only(tmp_path)
    try:
        queue = _queue(store)
        row = next(iter(queue.items("p1")))
        assert queue.claim(row, "worker-a") is True
        assert queue.claim(row, "worker-b") is False
        stored = {(item.url): item for item in queue.items("p1")}[row.url]
        assert stored.state is ExtractQueueState.CLAIMED
        assert stored.claimed_by == "worker-a"
        assert stored.claimed_at and stored.heartbeat_at
    finally:
        store.close()


def test_second_consumer_does_not_convert_claimed_rows(tmp_path: Path) -> None:
    """Чужая живая аренда — не работа второго потребителя: ``lost``, а не ``done``.

    Число ``lost`` публикуется: «строку забрал другой» — факт прогона, и без него
    пустой корпус второго потребителя читался бы как «нечего конвертировать».
    """
    store = _crawl_only(tmp_path)
    try:
        queue = _queue(store)
        for row in list(queue.items("p1")):
            assert queue.claim(row, "worker-a")
        # Второй потребитель видит только чужие аренды: ничего не берёт и не закрывает.
        report = _consume(store, worker="worker-b")
        assert report["lost"] == len(URLS) + 1  # 4 документа + корень
        assert report["done"] == 0
        rows = list(queue.items("p1"))
        assert {row.state for row in rows} == {ExtractQueueState.CLAIMED}
        assert {row.claimed_by for row in rows} == {"worker-a"}
    finally:
        store.close()


def _consume(store: SqliteStore, *, worker: str) -> dict[str, Any]:
    """Один вызов потребителя по тому же сеансу; отчёт — из ``log_entries`` фазы.

    Второй реализации нет: прогоняется та же фаза в режиме ``consume_only`` — то есть
    ровно то, что делает ``hound extract consume``.
    """
    base.RUN_ID = RUN_ID
    base.SESSION_ID = SESSION_ID
    try:
        result = base.build_corpus(
            base._plan(),
            base._Crawler({}),  # краулер не должен вызываться: режим без сети
            base._Extractor(),
            base._Chunker(),
            store,
            urls={"p1": []},
            raw_root=Path("/tmp"),  # noqa: S108 — корень не используется в consume-only
            run_id=RUN_ID,
            consume_only=True,
            crawl_defaults=base.CrawlRunConfig(max_depth=1),
            consume_worker=worker,
        )
    finally:
        base.RUN_ID = "run-extract-consumer"
        base.SESSION_ID = default_session_id("run-extract-consumer")
    records = [
        entry
        for entry in result.log_entries
        if isinstance(entry, dict) and entry.get("event") == "extract_consume"
    ]
    assert len(records) == 1, result.log_entries
    return dict(records[0])


# --------------------------------------------------------------------------- #
# (2) просроченная аренда и исчерпание попыток
# --------------------------------------------------------------------------- #


def test_stale_claim_is_reclaimed_with_visible_record(tmp_path: Path) -> None:
    """Просрочка возвращает строку в ``pending`` с ``attempts += 1`` и названным владельцем."""
    store = _crawl_only(tmp_path)
    try:
        queue = _queue(store)
        row = next(iter(queue.items("p1")))
        assert queue.claim(row, "dead-worker")
        reclaimed = queue.reclaim_stale(now="2030-01-01T00:00:00+00:00", lease_s=60)
        assert [record["url"] for record in reclaimed] == [row.url]
        assert reclaimed[0]["attempts"] == 1
        assert reclaimed[0]["claimed_by"] == "dead-worker"
        assert reclaimed[0]["exhausted"] is False
        stored = {item.url: item for item in list(queue.items("p1"))}[row.url]
        assert stored.state is ExtractQueueState.PENDING
        assert stored.attempts == 1
        assert stored.claimed_by is None
        assert "claim expired" in (stored.error or "")
    finally:
        store.close()


def test_reclaim_is_visible_in_consumer_report(tmp_path: Path) -> None:
    """Восстановление названо в отчёте потребителя: число, URL, кто держал аренду."""
    store = _crawl_only(tmp_path)
    try:
        queue = _queue(store)
        row = next(iter(queue.items("p1")))
        assert queue.claim(row, "dead-worker")
        # Часы не подменяются в фазе, поэтому просрочку создаём вручную: ``claimed_at``
        # в прошлом — тот же durable-факт, что оставил бы мёртвый воркер.
        with store.conn:
            store.conn.execute(
                "UPDATE extract_queue SET claimed_at = ?, heartbeat_at = ?"
                " WHERE session_id = ? AND product_id = ? AND url = ?",
                (
                    "2000-01-01T00:00:00+00:00",
                    "2000-01-01T00:00:00+00:00",
                    SESSION_ID,
                    "p1",
                    row.url,
                ),
            )
        report = _consume(store, worker="worker-b")
        assert report["reclaimed"] == 1
        assert report["reclaim_records"][0]["url"] == row.url
        # Строка конвертирована после восстановления — документ не потерян.
        stored = {item.url: item for item in list(queue.items("p1"))}[row.url]
        assert stored.state is ExtractQueueState.DONE
        assert stored.attempts == 1
    finally:
        store.close()


def test_exhausted_attempts_are_terminal(tmp_path: Path) -> None:
    """Исчерпание ``max_attempts`` — ``failed`` с классом сбоя, а не вечное воскрешение."""
    store = _crawl_only(tmp_path)
    try:
        queue = _queue(store)
        row = next(iter(queue.items("p1")))
        for _ in range(3):
            assert queue.claim(row, "dead-worker")
            reclaimed = queue.reclaim_stale(
                now="2030-01-01T00:00:00+00:00", lease_s=60, max_attempts=3
            )
            assert len(reclaimed) == 1
        stored = {item.url: item for item in list(queue.items("p1"))}[row.url]
        assert stored.state is ExtractQueueState.FAILED
        assert stored.error_kind == EXTRACT_ATTEMPTS_ERROR_KIND
        assert stored.attempts == 3
        # Терминальная строка не возвращается в работу: повтор — только явной командой.
        assert queue.reclaim_stale(now="2030-01-01T00:00:00+00:00", lease_s=60) == []
        assert stored.state is ExtractQueueState.FAILED
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (3) heartbeat у долгого документа
# --------------------------------------------------------------------------- #


def test_heartbeat_keeps_a_long_document_alive(tmp_path: Path) -> None:
    """Живой потребитель на долгом документе продлевает аренду, а не «умирает» молча.

    Один ``future.result(timeout=...)`` на весь документ означал бы молчание дольше
    порога аренды: просрочка вернула бы строку другому, и документ конвертировался бы
    повторно. Поэтому ожидание нарезано, и между срезами идёт heartbeat.
    """
    store = _crawl_only(tmp_path)
    created: list[base._ThreadPool] = []
    try:
        result = base.build_corpus(
            base._plan(),
            base._Crawler({}),
            base._Extractor(),
            base._Chunker(),
            store,
            urls={"p1": []},
            raw_root=tmp_path,
            run_id=RUN_ID,
            consume_only=True,
            crawl_defaults=base.CrawlRunConfig(max_depth=1, extract_workers=2),
            extraction_pool=base._pool(created, delays=(0.2, 0.2, 0.2, 0.2, 0.2)),
            consume_heartbeat_s=0.02,
        )
        records = [
            entry
            for entry in result.log_entries
            if isinstance(entry, dict) and entry.get("event") == "extract_consume"
        ]
        assert records, result.log_entries
        assert records[0]["heartbeats"] >= 1, records[0]
        assert records[0]["pending"] == 0
        assert all(
            row.state is ExtractQueueState.DONE for row in _queue(store).items()
        )
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (4) backpressure: числа, а не «стало тесно» (T8)
# --------------------------------------------------------------------------- #


def _retained_file(tmp_path: Path, size: int) -> Path:
    path = tmp_path / "p1" / "abc.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


def test_backlog_bytes_are_measured_from_files(tmp_path: Path) -> None:
    """``backlog_bytes`` — Σ размеров retained-файлов ``pending``+``claimed``, не оценка."""
    store = _crawl_only(tmp_path)
    try:
        queue = _queue(store)
        rows = list(queue.items("p1"))
        for row in rows:
            path = _retained_file(tmp_path, 300_000)
            with store.conn:
                store.conn.execute(
                    "UPDATE extract_queue SET raw_path = ? WHERE session_id = ?"
                    " AND product_id = ? AND url = ?",
                    (str(path), SESSION_ID, "p1", row.url),
                )
        report = queue.backlog(limit_mb=1, raw_root=tmp_path)
        assert report.bytes == 300_000 * len(rows)
        assert report.files == len(rows)
        assert report.missing == 0
        assert isinstance(report.free_bytes, int)
        # 5 × 300 KB = 1.5 MB против предела 1 MB — превышение наблюдаемо числом.
        assert report.over_limit is True
    finally:
        store.close()


def test_wait_for_backlog_gives_up_with_numbers(tmp_path: Path) -> None:
    """Ожидание исчерпано → ``gave_up`` с числами; исключения нет (T8, unit).

    ``sleep``/``clock`` инъецируются: тест проверяет **логику** ожидания, а не
    выдержку оператора. ``waited_s`` — наблюдённое значение, а не ноль по умолчанию.
    """
    store = _crawl_only(tmp_path)
    try:
        queue = _queue(store)
        for row in list(queue.items("p1")):
            path = _retained_file(tmp_path, 300_000)
            with store.conn:
                store.conn.execute(
                    "UPDATE extract_queue SET raw_path = ? WHERE session_id = ?"
                    " AND product_id = ? AND url = ?",
                    (str(path), SESSION_ID, "p1", row.url),
                )
        clock = iter([0.0, 5.0, 10.0, 15.0])
        event = wait_for_backlog(
            queue,
            limit_mb=1,
            raw_root=tmp_path,
            wait_s=10.0,
            poll_s=5.0,
            sleep=lambda seconds: None,
            clock=lambda: next(clock),
        )
        assert isinstance(event, BackpressureEvent)
        assert event.gave_up is True
        payload = event.as_dict()
        assert payload["event"] == "crawl_backpressure"
        assert payload["backlog_bytes"] == 300_000 * len(list(queue.items("p1")))
        assert isinstance(payload["free_bytes"], int)
        assert payload["waited_s"] > 0
        assert payload["gave_up"] is True
    finally:
        store.close()


def test_backpressure_is_visible_not_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T8: при достижении watermark — запись с числами, ``partial``, без исключения.

    Ожидание исполняется **настоящей** функцией (её логика покрыта выше), но с нулевым
    сроком терпения: тест доказывает не выдержку, а **последствие** — видимую запись
    ``crawl_backpressure`` с числами, усечение ``extract_backlog_full`` и ``partial``
    сеанс. Краул не вправе ни молча ждать, ни врать ``completed``.
    """
    real_wait = corpus_drain.wait_for_backlog

    def _impatient(queue: Any, **kwargs: Any) -> BackpressureEvent | None:
        return real_wait(
            queue, wait_s=0.0, poll_s=0.0, sleep=lambda _s: None, **kwargs
        )

    monkeypatch.setattr(corpus_drain, "wait_for_backlog", _impatient)
    store = SqliteStore(":memory:")
    try:
        result = _build(
            tmp_path,
            store,
            # Предел 1 МБ, а страницы больше: очередь не может уложиться в watermark.
            crawl_defaults=base.CrawlRunConfig(max_depth=1, extract_backlog_mb=1),
            pages={url: b"y" * (1024 * 1024 + 1) for url in (ROOT, *URLS)},
        )
        # Исключения нет: остановка приёма — исход прогона, а не сбой инструмента.
        assert result.status in ("ok", "failed")
        line = next(
            (text for text in result.errors if text.startswith("crawl_backpressure:")),
            None,
        )
        assert line is not None, result.errors
        assert "backlog_bytes=" in line
        assert "free_bytes=" in line
        assert "waited_s=" in line
        assert "gave_up=true" in line
        builder = next(
            entry for entry in result.log_entries if isinstance(entry, IndexBuild)
        )
        reasons = {
            record.get("truncated_reason")
            for record in builder.truncation
            if record.get("truncated")
        }
        assert "extract_backlog_full" in reasons, builder.truncation
        assert builder.truncated is True
        # Сеанс обязан закрыться ``partial``: остановленный приём — неполный корпус.
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_crawl_completes_while_consumer_is_slow(tmp_path: Path) -> None:
    """T7: краул заканчивается, не дожидаясь конверсии; очередь остаётся непустой.

    Замедление потребителя моделируется его **отсутствием**: ``crawl_only`` — это
    «потребителя нет вовсе», предельный случай «2 с/док». Утверждение проверяемое:
    фаза не вызывает экстрактор ни разу (``extractor.calls == []``), краул ``ok``, а
    очередь содержит ровно добытые документы в ``pending``. Именно это и означает
    «краул эффективен безотносительно конверсии» (17 §1).
    """
    store = SqliteStore(":memory:")
    extractor = base._Extractor()
    try:
        started = time.monotonic()
        result = _build(tmp_path, store, crawl_only=True, extractor=extractor)
        elapsed = time.monotonic() - started
        assert result.status == "ok"
        assert extractor.calls == []
        assert elapsed < 5.0  # краул не «ждёт» конверсию: её просто нет
        pending = list(_queue(store).items())
        assert len(pending) == len(URLS) + 1
        assert {row.state for row in pending} == {ExtractQueueState.PENDING}
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (5) замер попытки: fetch_started_at / fetch_ms
# --------------------------------------------------------------------------- #


def test_fetch_measurement_is_written_by_the_producer(tmp_path: Path) -> None:
    """``fetch_started_at``/``fetch_ms`` заполняются там, где идёт фетч (§7.2 №1).

    Пустое значение допустимо только там, где живого фетча **не было** (resume/отказ до
    сети). У ``FETCHED``-элемента после живого краула оба поля обязаны быть, иначе
    ``observed_min_interval_s`` не посчитать: ``first_seen``/``last_attempt`` —
    операционные метки, а не таймер.
    """
    store = SqliteStore(":memory:")
    try:
        _build(tmp_path, store, crawl_only=True)
        items = [
            item
            for item in store.iter_crawl_session_items(SESSION_ID)
            if item.status is CrawlItemStatus.FETCHED
        ]
        assert items, "живой краул обязан оставить FETCHED-элементы"
        for item in items:
            assert item.fetch_started_at, item.url
            assert item.fetch_ms is not None and item.fetch_ms >= 0, item.url
        # Resume: повторная пометка того же исхода **переносит** замер, а не обнуляет его.
        # Иначе после ``hound extract consume`` таймер фетча пропал бы, и §7.2 №1
        # считался бы только по живому прогону — то есть не считался бы вовсе.
        before = {item.url: item.fetch_ms for item in items}
        report = _consume(store, worker="worker-resume")
        assert report["done"] == len(URLS) + 1
        after = {
            item.url: item.fetch_ms
            for item in store.iter_crawl_session_items(SESSION_ID)
            if item.status is CrawlItemStatus.FETCHED
        }
        assert after == before
    finally:
        store.close()

# --------------------------------------------------------------------------- #
# (6) окно потребителя не меняет корпус
# --------------------------------------------------------------------------- #


def test_window_size_does_not_change_the_corpus(tmp_path: Path) -> None:
    """W=1 и W=4 дают один корпус: окно — расписание, а не условие измерения.

    Проверяется содержимое, а не раскладка: ``(url, content_hash)`` документов, набор
    чанков и ``partition_build_id``. Окно меняет **порядок завершения** заданий, но не
    набор документов — иначе ``extract_workers`` был бы осью контракта, а он провенанс.
    """
    corpora: list[tuple[list[tuple[str, str]], list[tuple[str, int]], list[str]]] = []
    for index, workers in enumerate((1, 4)):
        store = SqliteStore(":memory:")
        try:
            created: list[base._ThreadPool] = []
            result = _build(
                tmp_path / f"window-{index}",
                store,
                crawl_defaults=base.CrawlRunConfig(
                    max_depth=1, extract_workers=workers
                ),
                extraction_pool=base._pool(created),
            )
            assert result.errors == []
            docs = sorted(
                (doc.url, doc.content_hash) for doc in store.iter_corpus_docs(RUN_ID)
            )
            chunks = sorted(
                (str(row["url"]), int(row["chunk_no"]))
                for row in store.conn.execute(
                    "SELECT url, chunk_no FROM corpus_chunks"
                )
            )
            partitions = [
                entry.partition_build_id
                for entry in next(
                    entry
                    for entry in result.log_entries
                    if isinstance(entry, IndexBuild)
                ).entries
            ]
            corpora.append((docs, chunks, partitions))
        finally:
            store.close()
    assert corpora[0] == corpora[1]
    assert corpora[0][0]
