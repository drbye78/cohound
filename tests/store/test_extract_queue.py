"""Тесты durable-очереди конверсии ``extract_queue`` (17 §4.1, F2).

Проверяется ровно контракт очереди: идемпотентность записи (краул не перезаписывает
состояние потребителя), счётчики по состояниям и чтение в детерминированном порядке.
Плюс доменный инвариант модели: ``pending``/``claimed`` несут адрес retained-байтов и
его хеш — без них цепочка I19 рвётся до начала конверсии.

Все тесты на ``:memory:``-БД; нейтральны (§1.3): ``p1/p2``, ``docs.test``.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import ValidationError

from core.domain import (
    ExtractQueueItem,
    ExtractQueueState,
    RunConfig,
    RunMode,
)
from core.pipeline.crawl_session import default_session_id
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-extract-queue"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as s:
        s.create_run(
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
        yield s


def _item(url: str, **overrides: object) -> ExtractQueueItem:
    base: dict[str, object] = {
        "session_id": SESSION_ID,
        "product_id": "p1",
        "url": url,
        "content_hash": f"hash-{url}",
        "raw_path": f"corpus_raw/p1/{url.rsplit('/', 1)[-1]}.bin",
        "level": "full",
        "pdf_pipeline": "structured",
        "enqueued_at": TS,
    }
    base.update(overrides)
    return ExtractQueueItem(**base)


# --------------------------------------------------------------------------- #
# (1) идемпотентность записи
# --------------------------------------------------------------------------- #


def test_enqueue_is_idempotent_and_keeps_consumer_state(store: SqliteStore) -> None:
    """Повторная запись того же URL не дублирует строку и не сбрасывает состояние.

    Иначе краул, продолживший сеанс, вернул бы ``done`` в ``pending`` и заставил
    конверсию перечитать уже собранный документ (17 §4.2).
    """
    url = "https://docs.test/a"
    store.enqueue_extract_item(_item(url))
    store.enqueue_extract_item(_item(url, content_hash="hash-2", raw_path="other.bin"))

    rows = list(store.iter_extract_queue(SESSION_ID))
    assert len(rows) == 1
    # Первая запись побеждает: адрес факта принадлежит краулу и не переписывается.
    assert rows[0].content_hash == f"hash-{url}"
    assert rows[0].state is ExtractQueueState.PENDING

    # Состояние потребителя не затирается повторной записью краула.
    store.conn.execute(
        "UPDATE extract_queue SET state = ? WHERE session_id = ? AND url = ?",
        (ExtractQueueState.DONE.value, SESSION_ID, url),
    )
    store.enqueue_extract_item(_item(url))
    assert next(iter(store.iter_extract_queue(SESSION_ID))).state is ExtractQueueState.DONE


# --------------------------------------------------------------------------- #
# (2) счётчики по состояниям
# --------------------------------------------------------------------------- #


def test_count_extract_queue_reports_only_non_empty_states(
    store: SqliteStore,
) -> None:
    assert store.count_extract_queue(SESSION_ID) == {}

    store.enqueue_extract_item(_item("https://docs.test/a"))
    store.enqueue_extract_item(_item("https://docs.test/b"))
    store.conn.execute(
        "UPDATE extract_queue SET state = ? WHERE session_id = ? AND url = ?",
        (ExtractQueueState.CLAIMED.value, SESSION_ID, "https://docs.test/b"),
    )

    assert store.count_extract_queue(SESSION_ID) == {"pending": 1, "claimed": 1}
    # Чужая/несуществующая сессия — пусто, а не «всё, что есть».
    assert store.count_extract_queue("crawl:other") == {}


# --------------------------------------------------------------------------- #
# (3) чтение очереди
# --------------------------------------------------------------------------- #


def test_iter_extract_queue_is_deterministic_and_carries_conditions(
    store: SqliteStore,
) -> None:
    store.enqueue_extract_item(_item("https://docs.test/b"))
    store.enqueue_extract_item(
        _item("https://docs.test/a", level="layout", pdf_pipeline="text_layer")
    )

    rows = list(store.iter_extract_queue(SESSION_ID))

    assert [row.url for row in rows] == ["https://docs.test/a", "https://docs.test/b"]
    assert rows[0].level == "layout"
    assert rows[0].pdf_pipeline == "text_layer"
    assert rows[0].product_id == "p1"
    assert rows[0].attempts == 0
    assert rows[0].state_changed_at is None


# --------------------------------------------------------------------------- #
# (5) сброс сеанса снимает и очередь конверсии
# --------------------------------------------------------------------------- #


def test_clear_crawl_session_drops_queue(store: SqliteStore) -> None:
    """``--reset``/``restart`` — «этого краула больше нет»: очередь не переживает сброс.

    Иначе после сброса ``crawl status`` показывал бы ``pending`` за документы, которых в
    текущем сеансе никто не добывал, то есть выдавал бы прошлую попытку за текущую.
    """
    store.enqueue_extract_item(_item("https://docs.test/a"))

    store.clear_crawl_session(SESSION_ID)

    assert store.count_extract_queue(SESSION_ID) == {}
    assert list(store.iter_extract_queue(SESSION_ID)) == []


# --------------------------------------------------------------------------- #
# (6) доменный инвариант: pending/claimed несут адрес retained-байтов
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("state", [ExtractQueueState.PENDING, ExtractQueueState.CLAIMED])
def test_pending_without_raw_path_is_rejected(state: ExtractQueueState) -> None:
    with pytest.raises(ValidationError, match="raw_path"):
        _item("https://docs.test/a", state=state, raw_path="")


def test_pending_without_content_hash_is_rejected() -> None:
    with pytest.raises(ValidationError, match="content_hash"):
        _item("https://docs.test/a", content_hash="")


def test_terminal_state_does_not_require_address() -> None:
    """``done``/``skipped`` адрес не обязаны сохранять: это забота потребителя."""
    item = _item(
        "https://docs.test/a",
        state=ExtractQueueState.SKIPPED,
        raw_path="",
        content_hash="",
    )
    assert item.state is ExtractQueueState.SKIPPED


def test_enqueued_row_matches_retained_file_convention() -> None:
    """Адрес строки — та же форма, что пишет retention (``corpus_raw/{pid}/{hash}.bin``).

    Проверка нужна потому, что потребитель открывает файл по этому пути: расхождение
    формы сделало бы очередь нечитаемой, а не «неполной».
    """
    item = _item("https://docs.test/a", content_hash="abc123")
    assert Path(item.raw_path).name == "a.bin"
    assert item.raw_path.startswith("corpus_raw/p1/")