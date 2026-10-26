"""``cells.absence_rule`` и ``cells.validation_notes``: запись, чтение, legacy-строки.

Оба поля существовали как «поле есть — производителя нет»:

* ``absence_rule`` не имел колонки, и чтение жёстко подставляло ``explicit``: прочитанная
  ячейка заявляла правило, которого каталог не объявлял, а ``review.promote`` работает
  именно с прочитанными ячейками;
* ``validation_notes`` имели колонку и читателя, но ни одного писателя: причина отказа
  оценщика исчезала после прогона.

Здесь проверяется и обратная совместимость: прогон, записанный до появления колонки
(``NULL``), читается, не выдаётся за ``explicit`` и не ломает ``review.promote``.
"""

from __future__ import annotations

import datetime as _dt
import json
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from core.domain import (
    AbsenceRuleType,
    Cell,
    CellState,
    Focus,
    LLMEvidence,
    RankingPolicy,
    RunConfig,
    RunMode,
    Status,
)
from core.pipeline.review import promote
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-cells"


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1"],
        package="pkg-1",
        focus=Focus(modules=["m1"], params=[]),
        languages=["ru"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as s:
        s.create_run(RUN_ID, _cfg())
        yield s


def _cell(param_id: str = "param.a", **overrides: object) -> Cell:
    base: dict[str, object] = {
        "run_id": RUN_ID,
        "product_id": "p1",
        "param_id": param_id,
        "state": CellState.EVALUATED,
        "status": Status.OK,
        "evidence": [LLMEvidence(source_id="c1", quote="q")],
    }
    base.update(overrides)
    return Cell(**base)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# absence_rule
# ---------------------------------------------------------------------------

def test_absence_rule_round_trip(store: SqliteStore) -> None:
    """Правило параметра пишется в колонку и читается фактическим значением."""
    store.save_cell(
        _cell(absence_rule=AbsenceRuleType.DISTRIBUTION_ABSENCE)
    )

    cell = next(store.iter_cells(RUN_ID))
    assert cell.absence_rule is AbsenceRuleType.DISTRIBUTION_ABSENCE


def test_absence_rule_legacy_null_is_not_explicit(store: SqliteStore) -> None:
    """Строка без правила (``NULL``) читается как ``None``, а не выдуманный ``explicit``."""
    store.save_cell(_cell(absence_rule=None))
    store.conn.execute("UPDATE cells SET absence_rule = NULL")

    cell = next(store.iter_cells(RUN_ID))
    assert cell.absence_rule is None
    assert cell.absence_rule is not AbsenceRuleType.EXPLICIT


def test_absence_rule_unknown_value_reads_as_none(store: SqliteStore) -> None:
    """Неизвестное значение в колонке — ``None``: чтение прогона важнее строгости."""
    store.save_cell(_cell())
    store.conn.execute("UPDATE cells SET absence_rule = 'bogus'")

    assert next(store.iter_cells(RUN_ID)).absence_rule is None


def test_absence_rule_column_missing_in_read_only_db(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Read-only БД без колонки (старая версия) читается: ``absence_rule`` = ``None``.

    ``connect_read_only`` не применяет аддитивных миграций, поэтому проекция обязана
    пережить отсутствующую колонку — иначе чтение старого прогона падало бы целиком.
    """
    store.save_cell(_cell())
    path = tmp_path / "legacy.db"
    store.conn.execute("VACUUM INTO ?", (str(path),))
    store.conn.execute("ALTER TABLE cells DROP COLUMN absence_rule")
    path.unlink()
    store.conn.execute("VACUUM INTO ?", (str(path),))

    with SqliteStore(path, read_only=True) as legacy:
        columns = {
            row["name"] for row in legacy.conn.execute("PRAGMA table_info(cells)")
        }
        assert "absence_rule" not in columns
        cell = next(legacy.iter_cells(RUN_ID))
        assert cell.absence_rule is None


def test_absence_rule_does_not_resurrect_after_legacy_write(
    store: SqliteStore,
) -> None:
    """Перезапись legacy-ячейки не подставляет правило: ``None`` остаётся ``None``."""
    store.save_cell(_cell(absence_rule=None))
    store.conn.execute("UPDATE cells SET absence_rule = NULL")

    legacy = next(store.iter_cells(RUN_ID))
    store.save_cell(legacy.model_copy(update={"state": CellState.EVALUATED}))

    assert next(store.iter_cells(RUN_ID)).absence_rule is None


# ---------------------------------------------------------------------------
# validation_notes
# ---------------------------------------------------------------------------

def test_validation_notes_accumulate_across_saves(store: SqliteStore) -> None:
    """Две причины (две оценки ячейки при resume) копятся, а не затирают друг друга."""
    store.save_cell(_cell(validation_notes=["причина-1: ремонт"]))
    store.save_cell(_cell(validation_notes=["причина-2: отказ валидации"]))

    cell = next(store.iter_cells(RUN_ID))
    assert cell.validation_notes == [
        "причина-1: ремонт",
        "причина-2: отказ валидации",
    ]


def test_validation_notes_are_deduplicated(store: SqliteStore) -> None:
    """Повторная оценка с той же причиной не дублирует запись."""
    store.save_cell(_cell(validation_notes=["причина"]))
    store.save_cell(_cell(validation_notes=["причина"]))

    assert next(store.iter_cells(RUN_ID)).validation_notes == ["причина"]


def test_validation_notes_stored_as_json_list(store: SqliteStore) -> None:
    """Колонка несёт JSON-массив: список, а не склеенная строка."""
    store.save_cell(_cell(validation_notes=["a", "b"]))

    raw = store.conn.execute(
        "SELECT validation_notes FROM cells WHERE run_id = ?", (RUN_ID,)
    ).fetchone()[0]
    assert json.loads(raw) == ["a", "b"]


def test_validation_notes_absent_is_null(store: SqliteStore) -> None:
    """Нет заметок — ``NULL`` (а не пустой массив): «пусто» остаётся отличимым."""
    store.save_cell(_cell(validation_notes=[]))

    raw = store.conn.execute(
        "SELECT validation_notes FROM cells WHERE run_id = ?", (RUN_ID,)
    ).fetchone()[0]
    assert raw is None
    assert next(store.iter_cells(RUN_ID)).validation_notes == []


def test_validation_notes_legacy_plain_text_readable(store: SqliteStore) -> None:
    """Текст, записанный не JSON-массивом, читается как одна заметка, а не теряется."""
    store.save_cell(_cell())
    store.conn.execute(
        "UPDATE cells SET validation_notes = ? WHERE run_id = ?",
        ("ручная пометка", RUN_ID),
    )

    assert next(store.iter_cells(RUN_ID)).validation_notes == ["ручная пометка"]


def test_validation_notes_batch_accumulates(store: SqliteStore) -> None:
    """Батчевый путь (``save_cells_batch``) сливает так же, как одиночный."""
    store.save_cells_batch([_cell(validation_notes=["первая"])])
    store.save_cells_batch([_cell(validation_notes=["вторая"])])

    assert next(store.iter_cells(RUN_ID)).validation_notes == ["первая", "вторая"]


# ---------------------------------------------------------------------------
# review.promote на legacy-прогоне
# ---------------------------------------------------------------------------

def test_promote_works_on_legacy_row_without_absence_rule(store: SqliteStore) -> None:
    """``review.promote`` не падает на прочитанной legacy-ячейке (``absence_rule=NULL``)."""
    store.save_cell(
        Cell(
            run_id=RUN_ID,
            product_id="p1",
            param_id="param.prelim",
            state=CellState.PRELIMINARY,
            status=None,
            absence_rule=AbsenceRuleType.EXPLICIT,
        )
    )
    # Имитация строки прогона, записанного до появления колонки.
    store.conn.execute("UPDATE cells SET absence_rule = NULL")

    promoted = promote(
        store=store,
        run_id=RUN_ID,
        product_id="p1",
        param_id="param.prelim",
        actor="человек",
        validated_only=False,
    )

    assert promoted.state is CellState.EVALUATED
    assert promoted.reviewed_by == "человек"
    assert promoted.absence_rule is None
    stored = next(
        cell
        for cell in store.iter_cells(RUN_ID)
        if cell.param_id == "param.prelim"
    )
    assert stored.state is CellState.EVALUATED
    assert stored.absence_rule is None
