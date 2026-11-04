"""``container_state`` (STATE 10): durable-наблюдение контейнера (план 19 §E.7).

Зачем таблица: у контейнера нет версии документа (``document_version`` описывает
извлечённый текст, а членов контейнер не несёт), поэтому путь и хеш ретейнённых байтов
архива жили только в БД прогона. Проход без сети читает лишь ``state.db`` — и молча терял
членов контейнера (замер стенда: ``auto`` 9 документов/597 чанков, ``frozen`` 7/326).

Проверяется:

* round-trip записи/чтения и ``iter_container_state`` (порядок ``normalized_url``);
* upsert идемпотентен и **не** перезаписывает ``first_seen_*`` — «когда URL впервые
  оказался контейнером» остаётся фактом истории;
* ``forget_product`` удаляет строки продукта вместе с прочими таблицами состояния.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.ports.state import ContainerStateRow
from core.store.state_store import SqliteStateStore

PRODUCT = "p1"
URL = "https://docs.test/files/bundle.zip"
NORMALIZED = "https://docs.test/files/bundle.zip"


def _row(**overrides: object) -> ContainerStateRow:
    payload: dict[str, object] = {
        "product_id": PRODUCT,
        "url": URL,
        "normalized_url": NORMALIZED,
        "archive_content_hash": "a" * 64,
        "raw_path": "/tmp/raw/p1/" + "a" * 64 + ".bin",
        "format": "zip",
        "status": "descended",
        "first_seen_run_id": "run-1",
        "last_seen_run_id": "run-1",
        "first_seen_at": "2026-01-01T00:00:00Z",
        "last_seen_at": "2026-01-01T00:00:00Z",
    }
    payload.update(overrides)
    return ContainerStateRow(**payload)  # type: ignore[arg-type]


def test_record_and_read_round_trip() -> None:
    """Запись читается обратно полем в поле; ``iter`` — в порядке ``normalized_url``."""
    with SqliteStateStore(":memory:") as state:
        assert state.get_container_state(PRODUCT, NORMALIZED) is None

        state.record_container_state(_row())

        row = state.get_container_state(PRODUCT, NORMALIZED)
        assert row is not None
        assert row.product_id == PRODUCT
        assert row.url == URL
        assert row.normalized_url == NORMALIZED
        assert row.archive_content_hash == "a" * 64
        assert row.raw_path == "/tmp/raw/p1/" + "a" * 64 + ".bin"
        assert row.format == "zip"
        assert row.status == "descended"
        assert row.first_seen_run_id == "run-1"
        assert row.last_seen_run_id == "run-1"

        state.record_container_state(
            _row(url="https://docs.test/files/a.zip", normalized_url="https://docs.test/files/a.zip")
        )
        assert [item.normalized_url for item in state.iter_container_state(PRODUCT)] == [
            "https://docs.test/files/a.zip",
            NORMALIZED,
        ]


def test_upsert_keeps_first_seen_and_updates_last_seen() -> None:
    """Повторная запись обновляет наблюдение, но ``first_seen_*`` не сбрасывает."""
    with SqliteStateStore(":memory:") as state:
        state.record_container_state(_row())
        state.record_container_state(
            _row(
                archive_content_hash="b" * 64,
                raw_path="/tmp/raw/p1/" + "b" * 64 + ".bin",
                format=None,
                status="unreadable",
                last_seen_run_id="run-2",
                last_seen_at="2026-02-02T00:00:00Z",
                first_seen_run_id="run-2",
                first_seen_at="2026-02-02T00:00:00Z",
            )
        )

        row = state.get_container_state(PRODUCT, NORMALIZED)
        assert row is not None
        # Наблюдение последнего прогона — новое...
        assert row.archive_content_hash == "b" * 64
        assert row.status == "unreadable"
        assert row.last_seen_run_id == "run-2"
        assert row.last_seen_at == "2026-02-02T00:00:00Z"
        # ...формат, не названный повторно, не стирается (COALESCE-семантика)...
        assert row.format == "zip"
        # ...а первое — прежнее: история не переписывается.
        assert row.first_seen_run_id == "run-1"
        assert row.first_seen_at == "2026-01-01T00:00:00Z"
        # Строка одна: идемпотентность по (product_id, normalized_url).
        assert len(state.iter_container_state(PRODUCT)) == 1


def test_rows_are_product_scoped() -> None:
    """Строка одного продукта не видна другому (ключ включает ``product_id``)."""
    with SqliteStateStore(":memory:") as state:
        state.record_container_state(_row())
        assert state.get_container_state("p2", NORMALIZED) is None
        assert list(state.iter_container_state("p2")) == []


def test_invalid_status_is_rejected_by_schema() -> None:
    """``status`` ограничен объявленными исходами спуска (CHECK в DDL, не в коде)."""
    with (
        SqliteStateStore(":memory:") as state,
        pytest.raises(Exception),  # noqa: B017 — sqlite3.IntegrityError, класс не важен
    ):
        state.record_container_state(_row(status="failed"))


def test_forget_product_removes_container_rows(tmp_path: Path) -> None:
    """Забывание продукта снимает и строки ``container_state`` (это состояние, не провенанс)."""
    db_path = tmp_path / "state.db"
    with SqliteStateStore(db_path) as state:
        state.record_container_state(_row())
        result = state.forget_product(PRODUCT)
        assert result.found is True
        assert result.counts["container_state"] == 1
        assert state.get_container_state(PRODUCT, NORMALIZED) is None