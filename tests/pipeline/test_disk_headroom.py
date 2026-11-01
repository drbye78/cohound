"""Жёсткий сторож свободного места носителя retention (решение владельца 01.11).

Что проверяется:

1. **Срабатывание.** ``free_bytes < reserve_bytes`` (размер носителя инъецируется, а не
   заполняется настоящий диск) даёт строку ``crawl_disk_headroom`` с числами, усечение
   ``disk_low_headroom`` и сеанс ``partial`` — **без исключения**: остановка приёма это
   исход прогона, а не сбой инструмента (I4).
2. **Молчание в норме.** Места достаточно — ни строки, ни усечения, прогон ``ok``.
3. **Порог — объявленный вход.** Резерв — ``crawl.max_artifact_bytes``; при том же
   свободном месте смена оси меняет исход. Константа «в коде» так себя не ведёт.
4. **Одна строка на прогон.** N отказанных волн (корней) дают **одну** запись, а число
   отказанных волн — счётчиком ``waves_blocked`` в ней же; приём при этом остановлен
   (после первой блокировки не добыто ни одного элемента).
5. **Неизвестное место названо.** ``free_bytes=None`` — сторож не применён
   (``applied=false``), и это видно в записи, а не выдано за «места хватает».

Нейтральные данные (§1.3): ``p1``, ``docs.test``. Сеть не используется.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import tests.pipeline.test_extract_consumer as base
from core.domain import CrawlRunConfig, CrawlSessionStatus
from core.pipeline import corpus_drain
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.crawl_session import default_session_id
from core.pipeline.extract_queue import (
    DISK_HEADROOM_EVENT,
    DISK_LOW_HEADROOM_REASON,
    DiskHeadroomGuard,
    DiskHeadroomReport,
    ExtractQueue,
    disk_headroom,
)
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-disk-headroom"
SESSION_ID = default_session_id(RUN_ID)
MIB = 1024 * 1024
ROOT = base.ROOT
PAGE = base.URLS[0]
BASE_RUN_ID = "run-extract-consumer"


# --------------------------------------------------------------------------- #
# Обвязка: полный прогон фазы корпуса с инъецированным размером носителя
# --------------------------------------------------------------------------- #


def _run(
    tmp_path: Path,
    store: SqliteStore,
    *,
    urls: tuple[str, ...] = (ROOT,),
    crawler: Any = None,
    pages: dict[str, bytes] | None = None,
    **kwargs: Any,
):
    """Сборка корпуса с ``run_id`` этого модуля; ``urls`` — авторские корни."""
    base.RUN_ID = RUN_ID
    base.SESSION_ID = SESSION_ID
    base._make_run(store)
    kwargs.setdefault("crawl_defaults", CrawlRunConfig(max_depth=1))
    try:
        return build_corpus(
            base._plan(),
            crawler if crawler is not None else base._Crawler(pages or base._pages()),
            base._Extractor(),
            base._Chunker(),
            store,
            urls={"p1": list(urls)},
            raw_root=tmp_path,
            run_id=RUN_ID,
            **kwargs,
        )
    finally:
        base.RUN_ID = BASE_RUN_ID
        base.SESSION_ID = default_session_id(BASE_RUN_ID)


def _builder(result: Any) -> IndexBuild:
    builds = [entry for entry in result.log_entries if isinstance(entry, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def _headroom_lines(result: Any) -> list[str]:
    return [text for text in result.errors if text.startswith(f"{DISK_HEADROOM_EVENT}:")]


def _truncation_reasons(result: Any) -> set[str]:
    return {
        str(record.get("truncated_reason"))
        for record in _builder(result).truncation
        if record.get("truncated")
    }


@pytest.fixture
def carrier(monkeypatch: pytest.MonkeyPatch):
    """Инъекция размера носителя retention: настоящий диск не заполняется.

    Подменяется **измеритель** (``_dir_free_bytes``), а не предикат: тест проверяет сторож
    на числе, которое объявил бы реальный носитель, а не на подменённом решении.
    """

    def _set(free: int | None) -> None:
        monkeypatch.setattr(
            "core.pipeline.extract_queue._dir_free_bytes", lambda _root: free
        )

    return _set


# --------------------------------------------------------------------------- #
# (1) Предикат: числа, а не «стало тесно»
# --------------------------------------------------------------------------- #


def test_predicate_blocks_when_free_is_below_announced_reserve() -> None:
    """Меньше резерва — блокировка; числа очереди наблюдаемы (пустая очередь = 0)."""
    report = disk_headroom(
        ExtractQueue(),
        reserve_bytes=250 * MIB,
        raw_root=None,
        free_bytes=100 * MIB,
    )
    assert isinstance(report, DiskHeadroomReport)
    assert report.blocked is True
    assert report.applied is True
    assert report.free_bytes == 100 * MIB
    assert report.reserve_bytes == 250 * MIB
    assert report.backlog_bytes == 0
    assert report.queue_rows == {}
    assert report.as_dict()["event"] == DISK_HEADROOM_EVENT


def test_predicate_is_silent_when_space_is_enough() -> None:
    """Свободного места не меньше резерва — сторож не срабатывает."""
    report = disk_headroom(
        ExtractQueue(),
        reserve_bytes=250 * MIB,
        raw_root=None,
        free_bytes=10 * 1024 * MIB,
    )
    assert report.blocked is False
    assert report.applied is True
    # Числа очереди не наблюдались — ``None``, а не ноль: ноль читался бы как факт.
    assert report.backlog_bytes is None
    assert report.queue_rows is None


def test_unknown_free_is_named_not_silent() -> None:
    """``free_bytes=None`` — сторож не применён, и это названо (``applied=false``)."""
    guard = DiskHeadroomGuard()
    report = guard.observe(ExtractQueue(), reserve_bytes=250 * MIB, raw_root=None)
    assert report.applied is False
    assert report.blocked is False
    assert guard.stopped is False
    notice = guard.notice()
    assert notice is not None
    assert notice.applied is False
    assert notice.waves_blocked == 0


def test_guard_refuses_every_later_wave_without_new_measurement() -> None:
    """После остановки каждая следующая волна отказана; замер уже не повторяется."""
    guard = DiskHeadroomGuard()
    queue = ExtractQueue()
    for _ in range(3):
        observed = guard.observe(
            queue, reserve_bytes=250 * MIB, raw_root=None, free_bytes=1
        )
        assert observed.blocked is True
    assert guard.stopped is True
    notice = guard.notice()
    assert notice is not None
    assert notice.blocked is True
    assert notice.waves_blocked == 3


# --------------------------------------------------------------------------- #
# (2) Полный прогон: остановка видима и не является исключением
# --------------------------------------------------------------------------- #


def test_low_headroom_stops_reception_visibly(
    tmp_path: Path, carrier: Any
) -> None:
    """Места меньше резерва — строка с числами, ``disk_low_headroom``, ``partial``, без сбоя."""
    carrier(1)  # 1 байт свободного места: меньше любого артефакта
    store = SqliteStore(":memory:")
    crawler = base._Crawler(base._pages())
    try:
        result = _run(tmp_path, store, crawler=crawler)
        # Исключения нет: остановка приёма — исход прогона, а не сбой (I4).
        assert result.status in ("ok", "failed")
        lines = _headroom_lines(result)
        assert len(lines) == 1, result.errors
        line = lines[0]
        assert "applied=true" in line
        assert "free_bytes=1" in line
        assert "reserve_bytes=" in line
        assert "backlog_bytes=" in line
        assert "blocked=true" in line
        assert "waves_blocked=1" in line
        # Приём остановлен **до** изъятия: ни одного сетевого вызова.
        assert crawler.calls == []
        assert _truncation_reasons(result) == {DISK_LOW_HEADROOM_REASON}
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_enough_headroom_leaves_run_untouched(
    tmp_path: Path, carrier: Any
) -> None:
    """Места достаточно — ни строки, ни усечения, прогон ``ok``."""
    carrier(64 * 1024 * MIB)
    store = SqliteStore(":memory:")
    crawler = base._Crawler(base._pages())
    try:
        result = _run(tmp_path, store, crawler=crawler)
        assert result.status == "ok"
        assert _headroom_lines(result) == []
        assert _truncation_reasons(result) == set()
        assert crawler.calls == [ROOT, *base.URLS]
        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.COMPLETED
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (3) Порог — объявленный вход, а не константа в коде
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("reserve_mib", [200, 400])
def test_threshold_follows_declared_axis(
    tmp_path: Path, carrier: Any, reserve_mib: int
) -> None:
    """Одно и то же свободное место (300 MiB), разная ось — разный исход.

    ``crawl.max_artifact_bytes`` объявлен: 300 MiB свободно при резерве 200 MiB — приём
    идёт; при резерве 400 MiB — останавливается. Значит порог читается из оси, а не
    спрятан константой: иначе оба прогона дали бы один исход.
    """
    carrier(300 * MIB)
    store = SqliteStore(":memory:")
    try:
        result = _run(
            tmp_path,
            store,
            crawl_defaults=CrawlRunConfig(
                max_depth=1, max_artifact_bytes=reserve_mib * MIB
            ),
        )
        assert bool(_headroom_lines(result)) is (reserve_mib > 300)
    finally:
        store.close()


def test_reserve_is_the_announced_artifact_size(
    tmp_path: Path, carrier: Any
) -> None:
    """``reserve_bytes`` в записи равен объявленному ``max_artifact_bytes`` оси."""
    carrier(1)
    store = SqliteStore(":memory:")
    try:
        result = _run(
            tmp_path,
            store,
            crawl_defaults=CrawlRunConfig(max_depth=1, max_artifact_bytes=7 * MIB),
        )
        line = _headroom_lines(result)[0]
        assert f"reserve_bytes={7 * MIB}" in line
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (4) Одна строка на прогон, счётчик волн в ней же
# --------------------------------------------------------------------------- #


def test_several_blocked_waves_give_one_line_with_counter(
    tmp_path: Path, carrier: Any
) -> None:
    """Два корня при нехватке места — одна строка, ``waves_blocked=2``, ноль добычи.

    Повторного «проверим и пошлём» нет: каждая следующая волна отказана тем же сторожем,
    поэтому число волн растёт в счётчике, а не в числе строк.
    """
    carrier(1)
    store = SqliteStore(":memory:")
    crawler = base._Crawler(base._pages())
    try:
        result = _run(tmp_path, store, urls=(ROOT, PAGE), crawler=crawler)
        lines = _headroom_lines(result)
        assert len(lines) == 1, result.errors
        assert "waves_blocked=2" in lines[0]
        # Приём остановлен: после первой блокировки не добыто ни одного элемента.
        assert crawler.calls == []
        assert list(store.iter_crawl_session_items(SESSION_ID)) == []
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (5) Строка фазы: стабильные ключи
# --------------------------------------------------------------------------- #


def test_line_is_rendered_by_phase_helper() -> None:
    """Строка фазы строится из записи: стабильные ключи, ``event`` — её маркер."""
    report = DiskHeadroomReport(
        free_bytes=5,
        reserve_bytes=10,
        backlog_bytes=123,
        queue_rows={"pending": 3, "done": 1},
        blocked=True,
        applied=True,
        waves_blocked=4,
    )
    line = corpus_drain.disk_headroom_line(report)
    assert line.startswith(f"{DISK_HEADROOM_EVENT}: ")
    for fragment in (
        "applied=true",
        "blocked=true",
        "free_bytes=5",
        "reserve_bytes=10",
        "backlog_bytes=123",
        "queue_rows=4",
        "waves_blocked=4",
    ):
        assert fragment in line