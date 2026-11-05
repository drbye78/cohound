"""``corpus_artifact``: даты наблюдений артефактов в БД прогона (план 20 §F2).

Зачем носитель. Наблюдения живут в ``state.db`` (``capture``/``url_state``), а Ш9 и
пост-хок ``hound report`` открывают только ``runs/{run_id}/cohound.db``: без проекции
дата артефакта в отчёте либо выдумывалась бы, либо требовала второго источника. Здесь
проверяется сам носитель: состав таблицы (пин версии), замена набора продукта (resume не
плодит строк), ``NULL`` вместо пустой даты и пустой корпус без падения.

Тесты нейтральны (§1.3) и работают на ``:memory:``-БД, сеть не используется.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator

import pytest

from core.domain import RunConfig, RunMode
from core.ports.store import CorpusArtifactRow
from core.store.sqlite import SCHEMA_VERSION, connect
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-artifacts"


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as instance:
        instance.create_run(
            RUN_ID,
            RunConfig(
                case_id="case",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1", "p2"],
                package="pkg",
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
            ),
        )
        yield instance


def _row(**overrides: object) -> CorpusArtifactRow:
    base: dict[str, object] = {
        "run_id": RUN_ID,
        "product_id": "p1",
        "url_key": "https://docs.test/p1/a",
        "doc_version_id": "dv_1",
        "observed_at": "2026-01-01T00:00:00+00:00",
        "observed_how": "live",
        "capture_id": "cap_1",
        "root_url": "https://docs.test/p1/a",
    }
    base.update(overrides)
    return CorpusArtifactRow(**base)  # type: ignore[arg-type]


def test_schema_pins_the_table_shape() -> None:
    """Версия схемы и состав ``corpus_artifact`` запинены (как ``runs``/``corpus_docs``).

    3.19.0 — F2 плана ``20``: таблица объявлена в ``CREATE TABLE``; шимов ``ALTER TABLE``
    нет, поэтому файлы ≤ 3.18.0 отвергаются гейтом версии (:class:`SchemaVersionMismatch`),
    а не досоздаются при открытии.
    """
    assert SCHEMA_VERSION == "3.19.0"

    conn = connect(":memory:")
    try:
        columns = [r["name"] for r in conn.execute("PRAGMA table_info(corpus_artifact)")]
        # Состав — ровно эти колонки: датных четыре, и все NULL-able.
        assert columns == [
            "run_id",
            "product_id",
            "url_key",
            "doc_version_id",
            "observed_at",
            "observed_how",
            "capture_id",
            "root_url",
        ]
        info = {r["name"]: r for r in conn.execute("PRAGMA table_info(corpus_artifact)")}
        # Ключевые колонки — обязательные; датные — нет: NULL = «наблюдения нет».
        assert {name for name, row in info.items() if row["notnull"]} == {
            "run_id",
            "product_id",
            "url_key",
        }
        assert [name for name, row in info.items() if row["pk"]] == [
            "run_id",
            "product_id",
            "url_key",
        ]
        indexes = {
            r["name"] for r in conn.execute("PRAGMA index_list(corpus_artifact)")
        }
        assert "idx_corpus_artifact_product" in indexes
    finally:
        conn.close()


def test_replace_round_trips_and_is_scoped_per_product(store: SqliteStore) -> None:
    """Запись читается обратно как записана; продукты не видят чужих строк."""
    rows = [
        _row(),
        _row(
            url_key="https://docs.test/p1/b",
            doc_version_id="",
            observed_at=None,
            observed_how=None,
            capture_id=None,
            root_url=None,
        ),
        _row(product_id="p2", url_key="https://docs.test/p2/a"),
    ]
    store.replace_corpus_artifacts(RUN_ID, "p1", rows[:2])
    store.replace_corpus_artifacts(RUN_ID, "p2", rows[2:])

    assert list(store.iter_corpus_artifacts(RUN_ID, "p1")) == rows[:2]
    assert list(store.iter_corpus_artifacts(RUN_ID, "p2")) == rows[2:]
    assert list(store.iter_corpus_artifacts(RUN_ID)) == rows


def test_no_observation_is_null_never_empty_string(store: SqliteStore) -> None:
    """Артефакт без наблюдения — строка есть, датные поля ``NULL``.

    Документ в корпусе — факт; ``''`` вместо ``NULL`` читалось бы как дата, которой нет.
    """
    store.replace_corpus_artifacts(
        RUN_ID,
        "p1",
        [
            _row(
                observed_at=None,
                observed_how=None,
                capture_id=None,
                root_url=None,
            )
        ],
    )
    read = list(store.iter_corpus_artifacts(RUN_ID, "p1"))
    assert len(read) == 1
    assert read[0].observed_at is None
    assert read[0].observed_how is None
    assert read[0].capture_id is None
    assert read[0].root_url is None
    raw = store.conn.execute(
        "SELECT observed_at, observed_how, capture_id, root_url FROM corpus_artifact"
    ).fetchone()
    assert tuple(raw) == (None, None, None, None)


def test_repeated_replace_does_not_double_rows(store: SqliteStore) -> None:
    """Повторный проход (resume/``--reset``) даёт тот же набор, а не накопление."""
    rows = [_row(), _row(url_key="https://docs.test/p1/b")]
    for _ in range(3):
        store.replace_corpus_artifacts(RUN_ID, "p1", rows)
    assert list(store.iter_corpus_artifacts(RUN_ID, "p1")) == rows


def test_replace_drops_artifacts_that_left_the_corpus(store: SqliteStore) -> None:
    """Набор — функция корпуса: артефакт, выпавший из корпуса, исчезает из провенанса."""
    store.replace_corpus_artifacts(
        RUN_ID,
        "p1",
        [_row(), _row(url_key="https://docs.test/p1/b")],
    )
    store.replace_corpus_artifacts(RUN_ID, "p1", [_row(url_key="https://docs.test/p1/b")])
    assert [row.url_key for row in store.iter_corpus_artifacts(RUN_ID, "p1")] == [
        "https://docs.test/p1/b"
    ]


def test_empty_corpus_is_zero_rows_and_no_error(store: SqliteStore) -> None:
    """Пустой корпус продукта — ноль строк, без падения (не «дата не записана иначе»)."""
    store.replace_corpus_artifacts(RUN_ID, "p1", [])
    assert list(store.iter_corpus_artifacts(RUN_ID, "p1")) == []
    assert list(store.iter_corpus_artifacts(RUN_ID)) == []
    # Идемпотентно: повторная пустая замена — тот же факт.
    store.replace_corpus_artifacts(RUN_ID, "p1", [])
    assert list(store.iter_corpus_artifacts(RUN_ID, "p1")) == []

def test_evidence_refs_read_in_one_query(store: SqliteStore) -> None:
    """Ссылки evidence на артефакты — одним запросом на прогон (план 20 §7 F2).

    Отчёт сопоставляет ``doc_version_id``/``source_url`` evidence со строками
    ``corpus_artifact``; построчный читатель дал бы запрос на ячейку. Здесь пинится
    контракт читателя: ровно четыре поля, порядок вставки, отсутствующее значение —
    ``None``, а не ``''``.
    """
    from core.domain.enums import EvidenceType, SourceKind
    from core.domain.models import CellEvidence

    first = CellEvidence(
        evidence_id="ev-1",
        run_id=RUN_ID,
        product_id="p1",
        param_id="param_a",
        source_kind=SourceKind.DOCUMENTATION,
        evidence_type=EvidenceType.DIRECT,
        source_id="p1:dv_1:0",
        source_url="https://docs.test/p1/a",
        citation_quote="цитата",
        content_hash="hash",
        doc_version_id="dv_1",
    )
    second = CellEvidence(
        evidence_id="ev-2",
        run_id=RUN_ID,
        product_id="p1",
        param_id="param_a",
        source_kind=SourceKind.DOCUMENTATION,
        evidence_type=EvidenceType.DIRECT,
        source_id="p1:dv_2:1",
        source_url=None,
        citation_quote="цитата",
        content_hash="hash",
    )
    store.add_evidence(first)
    store.add_evidence(second)

    assert list(store.iter_cell_evidence_refs(RUN_ID)) == [
        {
            "product_id": "p1",
            "param_id": "param_a",
            "source_url": "https://docs.test/p1/a",
            "doc_version_id": "dv_1",
        },
        {
            "product_id": "p1",
            "param_id": "param_a",
            "source_url": None,
            "doc_version_id": None,
        },
    ]
