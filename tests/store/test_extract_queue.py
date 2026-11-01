"""Тесты durable-очереди конверсии ``extract_queue`` (17 §4.1, F2).

Проверяется ровно контракт очереди: идемпотентность записи (краул не перезаписывает
состояние потребителя), счётчики по состояниям и чтение в детерминированном порядке.
Плюс доменные инварианты модели: ``pending``/``claimed`` несут адрес retained-байтов
и его хеш, а также провенанс получения (``fetched_at``/``fetch_mode``/``extract_route``) —
без них цепочка I19 рвётся до начала конверсии, а условия измерения документа
перестают быть воспроизводимыми.

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
        # Провенанс получения обязателен для ``pending``/``claimed`` (валидатор модели):
        # очередь обещает воспроизводимость условий измерения, а не только файл.
        "fetched_at": TS,
        "fetch_mode": "http",
        "extract_route": "html",
        "enqueued_at": TS,
    }
    base.update(overrides)
    return ExtractQueueItem(**base)


# --------------------------------------------------------------------------- #
# (1) идемпотентность записи
# --------------------------------------------------------------------------- #


def test_enqueue_is_idempotent_and_keeps_consumer_state(store: SqliteStore) -> None:
    """Тот же факт не дублируется и не сбрасывает состояние потребителя.

    Иначе краул, продолживший сеанс, вернул бы ``done`` в ``pending`` и заставил
    конверсию перечитать уже собранный документ (17 §4.2).
    """
    url = "https://docs.test/a"
    store.enqueue_extract_item(_item(url))

    rows = list(store.iter_extract_queue(SESSION_ID))
    assert len(rows) == 1
    assert rows[0].content_hash == f"hash-{url}"
    assert rows[0].state is ExtractQueueState.PENDING

    # Состояние потребителя не затирается повторной записью того же факта.
    assert store.set_extract_state(
        SESSION_ID, "p1", url, ExtractQueueState.DONE, expect=ExtractQueueState.PENDING
    )
    store.enqueue_extract_item(_item(url))
    row = next(iter(store.iter_extract_queue(SESSION_ID)))
    assert row.state is ExtractQueueState.DONE
    assert row.content_hash == f"hash-{url}"


def test_enqueue_with_new_bytes_replaces_address_and_reopens_row(
    store: SqliteStore,
) -> None:
    """Другой ``content_hash`` того же URL — другой факт: адрес переписывается, строка снова ``pending``.

    Пересобрано в F5 (17 §4.1), причина названа: потребитель строит запрос конверсии
    **только из строки**, поэтому прежний адрес был бы единственным источником байтов.
    Оставленный адрес означал бы «новый документ добыт, но в корпус не попадёт», а
    ``state=done`` утверждал бы, что документ с этим ``content_hash`` в корпусе есть.
    Инвариант прежнего теста сохранён отдельно: тот же факт состояние не сбрасывает.
    """
    url = "https://docs.test/a"
    store.enqueue_extract_item(_item(url))
    assert store.set_extract_state(
        SESSION_ID, "p1", url, ExtractQueueState.DONE, expect=ExtractQueueState.PENDING
    )

    store.enqueue_extract_item(_item(url, content_hash="hash-2", raw_path="other.bin"))

    rows = list(store.iter_extract_queue(SESSION_ID))
    assert len(rows) == 1  # строка одна: ключ тот же
    assert rows[0].content_hash == "hash-2"
    assert rows[0].raw_path == "other.bin"
    # Работа новая: строка снова ожидает потребителя, след прошлого исполнения снят.
    assert rows[0].state is ExtractQueueState.PENDING
    assert rows[0].claimed_by is None
    assert rows[0].error_kind is None
    assert rows[0].error is None


# --------------------------------------------------------------------------- #
# (1b) условный переход состояния (контракт потребителя)
# --------------------------------------------------------------------------- #


def test_set_extract_state_moves_pending_to_done(store: SqliteStore) -> None:
    """``pending → done`` с ``expect=PENDING``: строка переходит, поля краула целы.

    Переход — единственный способ, которым потребитель объявляет работу сделанной
    (17 §4.1). Проверяется и то, чего он **не** делает: адрес retained-байтов и хеш
    остаются фактами краула, а не переписываются потребителем.
    """
    url = "https://docs.test/a"
    store.enqueue_extract_item(_item(url))

    assert store.set_extract_state(
        SESSION_ID, "p1", url, ExtractQueueState.DONE, expect=ExtractQueueState.PENDING
    )

    row = next(iter(store.iter_extract_queue(SESSION_ID)))
    assert row.state is ExtractQueueState.DONE
    assert row.state_changed_at is not None
    assert row.content_hash == f"hash-{url}"
    assert row.raw_path == "corpus_raw/p1/a.bin"
    assert row.error_kind is None and row.error is None


def test_set_extract_state_refuses_done_to_pending(store: SqliteStore) -> None:
    """Возврат ``done → pending`` отвергается: ``expect=PENDING`` не совпал.

    Без условия потребитель мог бы переоткрыть собранную работу (и повторно записать
    документ), то есть переписать факт, которым не владеет. Отказ виден вызывающему:
    ``False``, строка и метка времени не изменены.
    """
    url = "https://docs.test/a"
    store.enqueue_extract_item(_item(url))
    assert store.set_extract_state(
        SESSION_ID, "p1", url, ExtractQueueState.DONE, expect=ExtractQueueState.PENDING
    )
    done = next(iter(store.iter_extract_queue(SESSION_ID)))
    assert done.state is ExtractQueueState.DONE

    assert not store.set_extract_state(
        SESSION_ID,
        "p1",
        url,
        ExtractQueueState.PENDING,
        expect=ExtractQueueState.PENDING,
    )

    after = next(iter(store.iter_extract_queue(SESSION_ID)))
    assert after.state is ExtractQueueState.DONE
    assert after.state_changed_at == done.state_changed_at


def test_set_extract_state_expect_mismatch_leaves_row_untouched(
    store: SqliteStore,
) -> None:
    """Гонка двух потребителей: claim проигран — ``False`` и ни одного изменения.

    ``expect=CLAIMED`` на ``pending``-строке — это claim, который не состоялся.
    Проверяются все поля потребителя, а не только ``state``: частичная запись
    (например, ``claimed_by`` при отказе перехода) сделала бы строку «чужой».
    """
    url = "https://docs.test/a"
    store.enqueue_extract_item(_item(url))

    assert not store.set_extract_state(
        SESSION_ID,
        "p1",
        url,
        ExtractQueueState.CLAIMED,
        expect=ExtractQueueState.CLAIMED,
        claimed_by="worker-1",
        error_kind="parser_failed",
        error="should not be written",
    )

    row = next(iter(store.iter_extract_queue(SESSION_ID)))
    assert row.state is ExtractQueueState.PENDING
    assert row.state_changed_at is None
    assert row.claimed_by is None
    assert row.error_kind is None and row.error is None


# --------------------------------------------------------------------------- #
# (2) счётчики по состояниям
# --------------------------------------------------------------------------- #


def test_count_extract_queue_reports_only_non_empty_states(
    store: SqliteStore,
) -> None:
    assert store.count_extract_queue(SESSION_ID) == {}

    store.enqueue_extract_item(_item("https://docs.test/a"))
    store.enqueue_extract_item(_item("https://docs.test/b"))
    assert store.set_extract_state(
        SESSION_ID,
        "p1",
        "https://docs.test/b",
        ExtractQueueState.CLAIMED,
        expect=ExtractQueueState.PENDING,
        claimed_by="worker-1",
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


#: Провенанс получения, который потребитель обязан воспроизвести (``_ExtractRequest``,
#: ``core/pipeline/extraction_runtime.py``): ``{колонка: (имя поля модели, тип, значение)}``.
#: Список — данные теста, поэтому забытая в ``DDL``/INSERT/конвертере колонка роняет проверку.
FETCH_PROVENANCE: dict[str, tuple[str, str, object]] = {
    "fetched_at": ("fetched_at", "TEXT", "2026-01-01T00:00:03+00:00"),
    "fetch_mode": ("fetch_mode", "TEXT", "http"),
    "lang": ("lang", "TEXT", "en"),
    "status_code": ("status_code", "INTEGER", 200),
    "content_type": ("content_type", "TEXT", "text/html"),
    "engine": ("engine", "TEXT", "http"),
    "extract_route": ("extract_route", "TEXT", "html"),
}


def test_extract_queue_declares_fetch_provenance_columns(store: SqliteStore) -> None:
    """Свежая ``extract_queue`` объявляет все колонки провенанса получения.

    Без них потребитель не воспроизводит условия получения, выбирает маршрут заново и
    получает другой ``extractor_version``/``doc_version_id`` — другой корпус при тех же
    объявленных входах (I19, A3). Колонки nullable и без ``DEFAULT``: «не записано» —
    это ``NULL``, а не подобранное значение.
    """
    info = {
        row["name"]: row for row in store.conn.execute("PRAGMA table_info(extract_queue)")
    }
    for column, (field, declared_type, _) in FETCH_PROVENANCE.items():
        assert column in info, f"extract_queue.{column} не объявлена (поле {field})"
        assert info[column]["type"] == declared_type
        assert not info[column]["notnull"], f"{column} обязана быть nullable"
        assert info[column]["dflt_value"] is None, f"{column} не должна иметь DEFAULT"


def test_fetch_provenance_round_trips_through_queue(store: SqliteStore) -> None:
    """Провенанс получения переживает запись→чтение (INSERT и конвертер строки).

    Проверка ловит забытое поле в любую сторону: пропуск в ``INSERT`` даёт ``NULL`` при
    чтении, пропуск в конвертере — ``None`` в модели.
    """
    values = {field: value for field, _, value in FETCH_PROVENANCE.values()}
    store.enqueue_extract_item(_item("https://docs.test/provenance", **values))

    row = next(iter(store.iter_extract_queue(SESSION_ID)))
    for field, _, value in FETCH_PROVENANCE.values():
        assert getattr(row, field) == value, f"поле {field} потеряно на границе очереди"


def test_fetch_provenance_is_null_when_not_recorded(store: SqliteStore) -> None:
    """Строка без провенанса читается ``None`` — «не записано», а не выдуманный маршрут.

    Проверяется на **терминальном** состоянии: ``pending``/``claimed`` такой формы уже
    не допускают (валидатор ``_validate_pending_has_fetch_provenance``), потому что
    потребителю нечего воспроизводить. Для ``done``/``skipped`` NULL остаётся честным
    «не наблюдалось» и не подменяется догадкой.
    """
    store.enqueue_extract_item(
        _item(
            "https://docs.test/plain",
            state=ExtractQueueState.SKIPPED,
            fetched_at=None,
            fetch_mode=None,
            extract_route=None,
        )
    )

    row = next(iter(store.iter_extract_queue(SESSION_ID)))
    for field, _, _ in FETCH_PROVENANCE.values():
        assert getattr(row, field) is None


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


# --------------------------------------------------------------------------- #
# (7) доменный инвариант: pending/claimed несут провенанс получения
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("state", [ExtractQueueState.PENDING, ExtractQueueState.CLAIMED])
@pytest.mark.parametrize("field", ["fetched_at", "fetch_mode", "extract_route"])
def test_pending_without_fetch_provenance_is_rejected(
    state: ExtractQueueState, field: str
) -> None:
    """Без провенанса получения потребитель выведет условия сам — другой корпус (I19, A3).

    Сообщение обязано называть **чего именно** не хватает: иначе отказ неотличим от
    «что-то не так с моделью», а чинить нужно конкретную колонку производителя.
    """
    with pytest.raises(ValidationError, match=field):
        _item("https://docs.test/a", state=state, **{field: None})


def test_extract_route_outside_the_axis_is_rejected() -> None:
    """``extract_route`` — член ``ExtractionRoute``: свободная строка не пройдёт."""
    with pytest.raises(ValidationError, match="ExtractionRoute"):
        _item("https://docs.test/a", extract_route="guesswork")


def test_terminal_state_does_not_require_fetch_provenance() -> None:
    """Исход конверсии (``failed``) воспроизводимости не обещает — провенанс не обязателен."""
    item = _item(
        "https://docs.test/a",
        state=ExtractQueueState.FAILED,
        fetched_at=None,
        fetch_mode=None,
        extract_route="не ось",
        error_kind="parser_failed",
    )
    assert item.state is ExtractQueueState.FAILED


def test_enqueued_row_matches_retained_file_convention() -> None:
    """Адрес строки — та же форма, что пишет retention (``corpus_raw/{pid}/{hash}.bin``).

    Проверка нужна потому, что потребитель открывает файл по этому пути: расхождение
    формы сделало бы очередь нечитаемой, а не «неполной».
    """
    item = _item("https://docs.test/a", content_hash="abc123")
    assert Path(item.raw_path).name == "a.bin"
    assert item.raw_path.startswith("corpus_raw/p1/")