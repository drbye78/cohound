"""Пере-оценка устаревших ячеек (§13/K2): селектор + исполнение (``hound revalidate``).

Проверяется то, что нельзя проверить «на глаз» в отчёте:

* селектор отдаёт **ровно** помеченные ячейки (и ничего кроме), читая store, а не CSV;
* успешная пере-оценка снимает ``stale_reason`` и обновляет ``index_build_id`` до текущей
  сборки;
* неуспешная (``failed``, I4) пометку **сохраняет** — «свежесть» не выдаётся без вердикта;
* ячейки наследования/реестра пере-оценкой оценщика недостижимы (у них нет корпуса), и
  помечены stale они не бывают — это проверяется вызовом ``invalidate_affected``;
* повторный вызов — no-op с явным «пере-оценивать нечего»;
* ячейки вне stale не тронуты: ни статус, ни сборка, ни провенанс.

Работают на ``SqliteStore(":memory:")``; оценщик — скриптованный in-memory порт (I18).
Нейтральны к предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
from collections.abc import Iterator, Sequence
from pathlib import Path

import pytest

from core.cli.context import PLAN_INPUTS_SNAPSHOT_VERSION
from core.cli.errors import EXIT_OK
from core.cli.main import main
from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    Cell,
    CellState,
    Focus,
    Importance,
    LLMCellOutput,
    LLMEvidence,
    ParamType,
    RunConfig,
    RunMode,
    SourceKind,
    StaleReason,
    Status,
)
from core.pipeline import Ports
from core.pipeline.evaluate import (
    REVALIDATE_FAILED_PREFIX,
    REVALIDATE_NOTHING_TO_DO,
    REVALIDATE_SKIP_BUILD_MISMATCH,
    REVALIDATE_SKIP_NO_VERDICT,
    REVALIDATE_STATUS_NOOP,
    REVALIDATE_STATUS_REEVALUATED,
    STALE_SKIP_PARAM,
    STALE_SKIP_SOURCE_KIND,
    reevaluate_stale,
    select_stale_cells,
)
from core.pipeline.serp_loop import invalidate_affected
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.vector_index import RetrievedChunk
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-stale-1"
OLD_BUILD = "cb_old"
NEW_BUILD = "cb_new"
QUOTE = "Свойство Альфа доступно без дополнительной настройки."


# ---------------------------------------------------------------------------
# Данные и двойники
# ---------------------------------------------------------------------------


def _param(param_id: str = "param_alpha", title: str = "Параметр Альфа") -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=title,
        module="mod_a",
        group="grp_1",
        importance=Importance.BASIC,
        param_type=ParamType.FACT,
        rule="Подтвердить наличие свойства Альфа.",
        absence_rule=AbsenceRuleType.EXPLICIT,
        role_binding="role_a",
        concept_terms={"ru": ["свойство альфа"], "en": ["alpha feature"]},
        search_langs=["ru"],
    )


class _LexicalStub:
    """Лексический порт-заглушка: фиксированные чанки и провенанс сборки (R0)."""

    engine_version = "stub-1"

    def __init__(self, build_id: str, chunks: Sequence[RetrievedChunk]) -> None:
        self.build_id = build_id
        self.chunks = list(chunks)
        self.calls: list[str] = []

    def search(
        self,
        query: str,
        product_ids: list[str],
        top_k: int,
        lang: str | None = None,
    ) -> list[RetrievedChunk]:
        self.calls.append(query)
        scoped = [c for c in self.chunks if c.product_id in set(product_ids)]
        return scoped[:top_k]


class _ScriptedEvaluator:
    """Скриптованный ``EvaluatorPort`` (I18: без веб-тула); последний элемент повторяется."""

    def __init__(self, script: list[LLMCellOutput | Exception]) -> None:
        self.script = list(script)
        self.model_id = "fake-model"
        self.model_version = "1.0"
        self.seed = 0
        self.temperature = 0.0
        self.calls: list[LLMRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        item = self.script[min(len(self.calls) - 1, len(self.script) - 1)]
        if isinstance(item, Exception):
            raise item
        return LLMResponse(
            output=item, raw=item.model_dump_json(), prompt_hash="ph", decoder="fake"
        )


def _chunk(product_id: str = "p1", build_id: str = OLD_BUILD) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=f"chunk_{product_id}_1",
        product_id=product_id,
        text=QUOTE,
        content_hash="hash_1",
        heading_path="Раздел 1",
        build_id=build_id,
        score=1.0,
    )


def _ok_output() -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="Свойство Альфа подтверждено документацией.",
        evidence=[{"source_id": "chunk_p1_1", "quote": QUOTE}],
    )


def _cell(
    *,
    product_id: str = "p1",
    param_id: str = "param_alpha",
    state: CellState = CellState.EVALUATED,
    status: Status | None = Status.OK,
    source_kind: SourceKind = SourceKind.DOCUMENTATION,
    index_build_id: str | None = OLD_BUILD,
    stale: StaleReason | None = StaleReason.NEW_BUILD,
) -> Cell:
    evidence = (
        [LLMEvidence(source_id="chunk_p1_1", quote=QUOTE)]
        if status is Status.OK
        else []
    )
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=state,
        status=status,
        source_kind=source_kind,
        evidence=evidence,
        index_build_id=index_build_id,
        stale_reason=stale,
    )


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    """Прогон с текущей сборкой ``cb_new`` и набором ячеек разного происхождения."""
    with SqliteStore(":memory:") as s:
        s.create_run(
            RUN_ID,
            RunConfig(
                case_id="case-1",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1", "p2", "p3"],
                package="pkg-1",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
            ),
        )
        s.set_run_index_build_id(RUN_ID, NEW_BUILD)
        yield s


def _seed(store: SqliteStore, *cells: Cell) -> None:
    for cell in cells:
        store.save_cell(cell)


def _models(store: SqliteStore) -> dict[tuple[str, str], Cell]:
    return {(c.product_id, c.param_id): c for c in store.iter_cells(RUN_ID)}


# ---------------------------------------------------------------------------
# 1. Селектор: ровно помеченные ячейки
# ---------------------------------------------------------------------------


def test_selector_returns_exactly_marked_cells(store: SqliteStore) -> None:
    """Селектор отдаёт stale-ячейки и ничего кроме; свежие не попадают ни в одну часть."""
    _seed(
        store,
        _cell(param_id="param_alpha"),
        _cell(param_id="param_beta", stale=None),  # свежая
        _cell(
            product_id="p2",
            source_kind=SourceKind.REGISTRY,
            index_build_id=None,
        ),
        _cell(
            product_id="p3",
            source_kind=SourceKind.INHERITANCE,
            index_build_id=None,
        ),
    )

    selection = select_stale_cells(store, RUN_ID)

    assert {(c.product_id, c.param_id) for c in selection.reevaluable_cells} == {("p1", "param_alpha")}
    assert selection.total == 3  # «alpha» + registry(p2) + inheritance(p3)
    assert {row["reason"] for row in selection.skipped} == {
        f"{STALE_SKIP_SOURCE_KIND}:{SourceKind.REGISTRY.value}",
        f"{STALE_SKIP_SOURCE_KIND}:{SourceKind.INHERITANCE.value}",
    }
    # Свежая ячейка не появляется нигде: пере-оценка её не касается.
    assert all(c.param_id != "param_beta" for c in selection.reevaluable_cells)
    assert all(row["param_id"] != "param_beta" for row in selection.skipped)


def test_selector_filters_by_product_and_param(store: SqliteStore) -> None:
    """Фильтры ``product_ids``/``param_ids`` сужают селектор, не меняя семантику."""
    _seed(
        store,
        _cell(param_id="param_alpha"),
        _cell(param_id="param_beta"),
        _cell(product_id="p2"),
    )

    only_p2 = select_stale_cells(store, RUN_ID, product_ids=["p2"])
    only_beta = select_stale_cells(store, RUN_ID, param_ids=["param_beta"])

    assert {(c.product_id, c.param_id) for c in only_p2.reevaluable_cells} == {("p2", "param_alpha")}
    assert {(c.product_id, c.param_id) for c in only_beta.reevaluable_cells} == {("p1", "param_beta")}


# ---------------------------------------------------------------------------
# 2. Успех: пометка снята, сборка обновлена
# ---------------------------------------------------------------------------


def test_reevaluation_clears_mark_and_updates_build(store: SqliteStore) -> None:
    """Вердикт по текущей сборке → ``stale_reason=None`` и ``index_build_id=текущая``."""
    _seed(store, _cell())
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    report = reevaluate_stale(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    assert report.status == REVALIDATE_STATUS_REEVALUATED
    assert report.stale_total == 1
    assert [row["param_id"] for row in report.reevaluated] == ["param_alpha"]
    assert report.remaining == []
    assert len(evaluator.calls) == 1  # ровно затронутая ячейка, без «вся фаза»

    cell = _models(store)[("p1", "param_alpha")]
    assert cell.stale_reason is None
    assert cell.index_build_id == NEW_BUILD
    assert cell.state is CellState.EVALUATED
    assert cell.status is Status.OK
    # I1: вердикт наличия несёт evidence — оно лежит в ``cell_evidence`` (``Cell.evidence``
    # при чтении из store не восстанавливается по контракту: цитаты живут отдельно).
    direct = store.conn.execute(
        "SELECT COUNT(*) FROM cell_evidence"
        " WHERE run_id = ? AND product_id = ? AND param_id = ? AND evidence_type = 'direct'",
        (RUN_ID, "p1", "param_alpha"),
    ).fetchone()[0]
    assert direct >= 1


# ---------------------------------------------------------------------------
# 3. Провал: пометка сохранена
# ---------------------------------------------------------------------------


def test_failed_reevaluation_keeps_mark(store: SqliteStore) -> None:
    """``failed`` (I4) не снимает пометку: без вердикта ячейка не объявляется свежей."""
    _seed(store, _cell())
    evaluator = _ScriptedEvaluator([RuntimeError("оценщик недоступен")])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    report = reevaluate_stale(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    assert report.status == REVALIDATE_STATUS_NOOP
    assert report.reevaluated == []
    assert [row["reason"] for row in report.remaining] == [REVALIDATE_SKIP_NO_VERDICT]

    cell = _models(store)[("p1", "param_alpha")]
    assert cell.stale_reason is StaleReason.NEW_BUILD  # пометка НА МЕСТЕ
    assert cell.index_build_id == OLD_BUILD  # сборка не подменена
    assert cell.status is Status.OK  # старый вердикт не стёрт записью failed
    assert any(n.startswith(REVALIDATE_FAILED_PREFIX) for n in cell.validation_notes)
    assert any("evaluation_failed" in n for n in cell.validation_notes)


def test_build_mismatch_keeps_mark(store: SqliteStore) -> None:
    """Срез из **другой** сборки: вердикт получен, но не по текущему корпусу — stale."""
    _seed(store, _cell())
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(
        evaluator=evaluator, lexical_index=_LexicalStub("cb_other", [_chunk()])
    )

    report = reevaluate_stale(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    assert [row["reason"] for row in report.remaining] == [
        REVALIDATE_SKIP_BUILD_MISMATCH
    ]
    cell = _models(store)[("p1", "param_alpha")]
    assert cell.stale_reason is StaleReason.NEW_BUILD
    assert cell.index_build_id == OLD_BUILD


# ---------------------------------------------------------------------------
# 4. Не-documentation ячейки недостижимы (и не stale-ятся по сборке)
# ---------------------------------------------------------------------------


def test_invalidate_affected_never_marks_cells_without_build(store: SqliteStore) -> None:
    """Проверка чтением: у реестра/наследования нет привязки к сборке → они не stale.

    Это предпосылка селектора, а не его догадка: ``invalidate_affected`` отбирает ячейки
    по ``index_build_id == old_build_id`` (R15), а вердикты другого контура приходят с
    ``index_build_id=None`` — помечать их «устаревшими по сборке» нечем.
    """
    _seed(
        store,
        _cell(param_id="param_alpha", stale=None),  # документация, сборка OLD_BUILD
        _cell(
            product_id="p2",
            source_kind=SourceKind.REGISTRY,
            index_build_id=None,
            stale=None,
        ),
        _cell(
            product_id="p3",
            source_kind=SourceKind.INHERITANCE,
            index_build_id=None,
            stale=None,
        ),
    )

    invalidated = invalidate_affected(store, RUN_ID, OLD_BUILD, NEW_BUILD)

    assert {(c.product_id, c.param_id) for c in invalidated} == {("p1", "param_alpha")}
    cells = _models(store)
    assert cells[("p1", "param_alpha")].stale_reason is StaleReason.NEW_BUILD
    assert cells[("p2", "param_alpha")].stale_reason is None
    assert cells[("p3", "param_alpha")].stale_reason is None


def test_selector_skips_non_documentation_even_if_marked(store: SqliteStore) -> None:
    """Защита в глубину: даже помеченная registry/inheritance-ячейка не выбирается.

    Пометку такую никто не пишет (см. тест выше), но селектор обязан опираться на
    собственный предикат, а не на «раз помечено — значит корпус»: пере-оценка оценщиком
    подменила бы механизм получения вердикта (§2.2 п.7-8, I2b).
    """
    _seed(
        store,
        _cell(product_id="p2", source_kind=SourceKind.REGISTRY, index_build_id=None),
        _cell(product_id="p3", source_kind=SourceKind.INHERITANCE, index_build_id=None),
        _cell(param_id="param_alpha"),
    )
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    report = reevaluate_stale(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    assert len(evaluator.calls) == 1  # оценщик звали только для документации
    assert {(row["product_id"], row["param_id"]) for row in report.skipped} == {
        ("p2", "param_alpha"),
        ("p3", "param_alpha"),
    }
    cells = _models(store)
    assert cells[("p2", "param_alpha")].source_kind is SourceKind.REGISTRY
    assert cells[("p2", "param_alpha")].stale_reason is StaleReason.NEW_BUILD
    assert cells[("p3", "param_alpha")].index_build_id is None


def test_cell_state_without_verdict_is_not_reevaluable(store: SqliteStore) -> None:
    """``pending``/``failed``/``reviewed`` не пере-оцениваются: чужое решение не переписываем."""
    _seed(
        store,
        _cell(param_id="param_pending", state=CellState.PENDING, status=None),
        _cell(
            param_id="param_reviewed",
            state=CellState.REVIEWED,
            status=Status.OK,
        ),
    )

    selection = select_stale_cells(store, RUN_ID)

    assert selection.reevaluable_cells == ()
    assert {row["reason"] for row in selection.skipped} == {
        "state_not_reevaluable:pending",
        "state_not_reevaluable:reviewed",
    }


def test_param_outside_catalog_is_skipped(store: SqliteStore) -> None:
    """Ячейка без строки каталога не пере-оценивается: запросы и промпт не из чего построить."""
    _seed(store, _cell(param_id="param_ghost"))
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    report = reevaluate_stale(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    assert evaluator.calls == []
    assert [row["reason"] for row in report.skipped] == [STALE_SKIP_PARAM]
    assert _models(store)[("p1", "param_ghost")].stale_reason is StaleReason.NEW_BUILD


# ---------------------------------------------------------------------------
# 5. Идемпотентность: явное «пере-оценивать нечего»
# ---------------------------------------------------------------------------


def test_second_call_is_explicit_noop(store: SqliteStore) -> None:
    """Повтор без изменений: оценщик не зовётся, сообщение называет причину."""
    _seed(store, _cell())
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    first = reevaluate_stale(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )
    calls_after_first = len(evaluator.calls)
    second = reevaluate_stale(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    assert first.reevaluated and second.reevaluated == []
    assert second.status == REVALIDATE_STATUS_NOOP
    assert second.stale_total == 0
    assert second.message == REVALIDATE_NOTHING_TO_DO
    assert len(evaluator.calls) == calls_after_first  # работы не было


# ---------------------------------------------------------------------------
# 6. Ячейки вне stale не тронуты
# ---------------------------------------------------------------------------


def test_fresh_cells_are_untouched(store: SqliteStore) -> None:
    """Пере-оценка меняет только stale-ячейку: свежая остаётся байт-в-байт по полям."""
    _seed(
        store,
        _cell(param_id="param_alpha"),
        _cell(param_id="param_beta", stale=None, index_build_id=NEW_BUILD),
    )
    fresh_before = _models(store)[("p1", "param_beta")]
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    reevaluate_stale(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    fresh_after = _models(store)[("p1", "param_beta")]
    assert fresh_after.stale_reason is None
    assert fresh_after.index_build_id == fresh_before.index_build_id == NEW_BUILD
    assert fresh_after.status is fresh_before.status
    assert fresh_after.state is fresh_before.state
    assert fresh_after.validation_notes == fresh_before.validation_notes
    assert len(evaluator.calls) == 1


# ---------------------------------------------------------------------------
# Отказ без предпосылки — видимый, а не «тихо ноль»
# ---------------------------------------------------------------------------


def test_unknown_current_build_refuses(store: SqliteStore) -> None:
    """Без объявленной текущей сборки пере-оценка отказывается, а не выдумывает свежесть."""
    store.conn.execute("UPDATE runs SET index_build_id = NULL WHERE run_id = ?", (RUN_ID,))
    _seed(store, _cell())
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    report = reevaluate_stale(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    assert report.reevaluated == []
    assert [row["reason"] for row in report.remaining] == ["current_build_unknown"]
    assert evaluator.calls == []
    assert _models(store)[("p1", "param_alpha")].stale_reason is StaleReason.NEW_BUILD


def test_no_evaluator_refuses(store: SqliteStore) -> None:
    """Порт без оценщика — отказ с причиной, ячейки остаются stale."""
    _seed(store, _cell())
    ports = Ports(lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    report = reevaluate_stale(
        store=store, run_id=RUN_ID, params={"param_alpha": _param()}, ports=ports
    )

    assert [row["reason"] for row in report.remaining] == ["no_evaluator"]
    assert _models(store)[("p1", "param_alpha")].stale_reason is StaleReason.NEW_BUILD


# ---------------------------------------------------------------------------
# Точка входа оператора: ``hound revalidate`` (03 §D.6)
# ---------------------------------------------------------------------------


def _plan_payload() -> dict:
    """Минимальный снимок входов плана (``run_inputs``): каталог + когорта."""
    return {
        "snapshot_version": PLAN_INPUTS_SNAPSHOT_VERSION,
        "run_id": RUN_ID,
        "case_id": "case-1",
        "package_id": "pkg-1",
        "package_version": "1.0.0",
        "anchor": "p1",
        "products": ["p1"],
        "params": [_param().model_dump(mode="json")],
        "module_weights": {"mod_a": 1.0},
        "renormalized_weights": {"mod_a": 1.0},
        "cohort": {
            "case_id": "case-1",
            "frozen_at": "2026-01-01",
            "entries": [
                {"product_id": "p1", "role_overlap": 1.0, "anchor_relation": "anchor"}
            ],
        },
    }


def _db(tmp_path, *, cell: Cell | None = None) -> Path:
    """Файловая БД прогона: run + текущая сборка + снимок входов + одна ячейка."""
    db = tmp_path / "cohound.db"
    with SqliteStore(str(db)) as s:
        s.create_run(
            RUN_ID,
            RunConfig(
                case_id="case-1",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1"],
                package="pkg-1",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
            ),
        )
        s.set_run_index_build_id(RUN_ID, NEW_BUILD)
        s.save_plan_inputs(RUN_ID, payload=_plan_payload(), payload_sha256="0" * 64)
        s.save_cell(cell if cell is not None else _cell())
    return db


def test_cli_dry_run_reports_counts_without_evaluation(tmp_path, capsys) -> None:
    """``--dry-run`` показывает счётчики и не зовёт оценщика (селектор — дешёвый шаг)."""
    db = _db(tmp_path)
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    code = main(["revalidate", "--db", str(db), "--dry-run", "--json"], ports=ports)
    payload = json.loads(capsys.readouterr().out)

    assert code == EXIT_OK
    assert payload["status"] == "dry_run"
    assert payload["stale_total"] == 1
    assert payload["reevaluated"] == 0
    assert evaluator.calls == []


def test_cli_human_output_lists_counts(tmp_path, capsys) -> None:
    """Человекочитаемый вывод называет те же числа (оператор не обязан читать ``--json``)."""
    db = _db(tmp_path)

    code = main(["revalidate", "--db", str(db), "--dry-run"])
    out = capsys.readouterr().out

    assert code == EXIT_OK
    assert "stale_total: 1" in out
    assert "reevaluated: 0" in out
    assert "status: dry_run" in out


def test_cli_product_filter_is_applied(tmp_path, capsys) -> None:
    """``--product`` сужает селектор: чужой продукт даёт честный ноль stale."""
    db = _db(tmp_path)
    ports = Ports()

    matching = main(
        ["revalidate", "--db", str(db), "--dry-run", "--json", "--product", "p1"],
        ports=ports,
    )
    matching_payload = json.loads(capsys.readouterr().out)
    other = main(
        ["revalidate", "--db", str(db), "--dry-run", "--json", "--product", "pX"],
        ports=ports,
    )
    other_payload = json.loads(capsys.readouterr().out)

    assert matching == EXIT_OK and matching_payload["stale_total"] == 1
    assert other == EXIT_OK and other_payload["stale_total"] == 0


def test_cli_reevaluates_then_reports_noop(tmp_path, capsys) -> None:
    """Полный проход CLI: счётчики, снятая пометка и явный no-op на повторе."""
    db = _db(tmp_path)
    evaluator = _ScriptedEvaluator([_ok_output()])
    ports = Ports(evaluator=evaluator, lexical_index=_LexicalStub(NEW_BUILD, [_chunk()]))

    first = main(["revalidate", "--db", str(db), "--json"], ports=ports)
    first_payload = json.loads(capsys.readouterr().out)
    second = main(["revalidate", "--db", str(db), "--json"], ports=ports)
    second_payload = json.loads(capsys.readouterr().out)

    assert first == EXIT_OK and first_payload["status"] == "reevaluated"
    assert (first_payload["stale_total"], first_payload["reevaluated"]) == (1, 1)
    assert first_payload["remaining"] == 0
    assert second == EXIT_OK
    assert second_payload["status"] == REVALIDATE_STATUS_NOOP
    assert second_payload["message"] == REVALIDATE_NOTHING_TO_DO
    assert len(evaluator.calls) == 1  # повтор работы не сделал

