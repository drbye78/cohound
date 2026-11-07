"""CLI ``hound retry``: только сбойные ячейки, ``run_id`` не меняется (план 25).

Проверяется то, что нельзя проверить «на глаз»:

* ``--dry-run`` печатает счётчики и **не собирает порты** (шпион на ``build_adapters``):
  no-op не должен платить за индексы и оценщика;
* ``--json`` даёт разбираемый payload с теми же числами, что в полях отчёта;
* код возврата: частичный успех — ``EXIT_OK`` (оператор видит остаток), отказ — ``EXIT_ERROR``;
* модель оценщика берётся из объявленных ``llm_scenarios`` прогона, а не из флага/env.

Работают на ``SqliteStore`` в ``tmp_path``; оценщик — скриптованный in-memory порт (I18).
Нейтральны к предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

import pytest

from core.cli.errors import EXIT_ERROR, EXIT_OK
from core.cli.main import main
from core.domain import (
    AbsenceRuleType,
    AnchorRelation,
    CatalogRow,
    Cell,
    CellState,
    Cohort,
    CohortEntry,
    EndpointKind,
    Focus,
    Importance,
    LLMCellOutput,
    LLMScenario,
    ParamType,
    RunConfig,
    RunMode,
    ScenarioBinding,
    SourceKind,
    Status,
)
from core.pipeline import Ports
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.vector_index import RetrievedChunk
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-retry-cli"
BUILD = "cb_new"
QUOTE = "Свойство Альфа доступно без дополнительной настройки."


def _param(param_id: str) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=f"Параметр {param_id}",
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
    engine_version = "stub-1"

    def __init__(self, build_id: str, chunks: Sequence[RetrievedChunk]) -> None:
        self.build_id = build_id
        self.chunks = list(chunks)

    def search(
        self,
        query: str,
        product_ids: list[str],
        top_k: int,
        lang: str | None = None,
    ) -> list[RetrievedChunk]:
        return [c for c in self.chunks if c.product_id in set(product_ids)][:top_k]


class _ScriptedEvaluator:
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


def _chunk() -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id="chunk_p1_1",
        product_id="p1",
        text=QUOTE,
        content_hash="hash_1",
        heading_path="Раздел 1",
        build_id=BUILD,
        score=1.0,
    )


def _ok_output() -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="Свойство Альфа подтверждено документацией.",
        evidence=[{"source_id": "chunk_p1_1", "quote": QUOTE}],
    )


def _cell(param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id="p1",
        param_id=param_id,
        state=CellState.FAILED,
        status=None,
        source_kind=SourceKind.DOCUMENTATION,
        evidence=[],
        index_build_id=BUILD,
    )


def _plan_payload(*params: CatalogRow) -> dict[str, object]:
    cohort = Cohort(
        case_id="case-1",
        frozen_at=_dt.date(2026, 1, 1),
        entries=[
            CohortEntry(
                product_id="p1",
                role_overlap=1.0,
                anchor_relation=AnchorRelation.ANCHOR,
            )
        ],
    )
    return {
        "snapshot_version": 1,
        "run_id": RUN_ID,
        "params": [row.model_dump(mode="json") for row in params],
        "cohort": cohort.model_dump(mode="json"),
        "module_weights": {"mod_a": 1.0},
    }


def _make_db(tmp_path: Path, *, declare_model: bool, params: Sequence[str]) -> Path:
    db = tmp_path / "runs" / RUN_ID / "cohound.db"
    db.parent.mkdir(parents=True)
    with SqliteStore(db) as store:
        store.create_run(
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
        store.set_run_index_build_id(RUN_ID, BUILD)
        payload = _plan_payload(*(_param(p) for p in params))
        sha = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        store.save_plan_inputs(RUN_ID, payload=payload, payload_sha256=sha)
        for param_id in params:
            store.save_cell(_cell(param_id))
            store.record_cell_failure(RUN_ID, "p1", param_id, "evaluation_failed")
        if declare_model:
            store.save_llm_scenarios(
                [
                    ScenarioBinding(
                        run_id=RUN_ID,
                        scenario=LLMScenario.EVALUATOR,
                        model_id="fake-model",
                        endpoint_kind=EndpointKind.SELF_HOST,
                    )
                ]
            )
    return db


def _payload(capsys: pytest.CaptureFixture[str]) -> dict[str, object]:
    return json.loads(capsys.readouterr().out)


@pytest.fixture()
def spy(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Шпион на ``build_adapters``: список вызовов (пустой — порты не собирались)."""
    calls: list[str] = []

    def _spy(*args: object, **kwargs: object) -> Ports:
        calls.append("build_adapters")
        return Ports(
            evaluator=_ScriptedEvaluator([_ok_output()]),
            lexical_index=_LexicalStub(BUILD, [_chunk()]),
        )

    monkeypatch.setattr("core.cli.commands.retry.build_adapters", _spy)
    return calls


def test_dry_run_prints_counts_without_building_ports(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    spy: list[str],
) -> None:
    """``--dry-run``: селектор и счётчики, порты не собираются (шпион молчит)."""
    db = _make_db(tmp_path, declare_model=True, params=["param_alpha", "param_beta"])

    code = main(["retry", "--db", str(db), "--dry-run", "--json"])

    assert code == EXIT_OK
    payload = _payload(capsys)
    assert payload["status"] == "dry_run"
    assert payload["failed_total"] == 2
    assert payload["retryable"] == 2
    assert payload["retried"] == 0
    assert spy == []  # no-op не платит за индексы и оценщика
    assert [row["param_id"] for row in payload["selection"]["retryable_cells"]] == [
        "param_alpha",
        "param_beta",
    ]


def test_partial_success_returns_ok_with_numbers(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Одна ячейка получила вердикт, другая нет — ``EXIT_OK`` с числами (оператор повторит)."""
    db = _make_db(tmp_path, declare_model=True, params=["param_alpha", "param_beta"])
    evaluator = _ScriptedEvaluator([_ok_output(), RuntimeError("снова 503")])
    monkeypatch.setattr(
        "core.cli.commands.retry.build_adapters",
        lambda *args, **kwargs: Ports(
            evaluator=evaluator, lexical_index=_LexicalStub(BUILD, [_chunk()])
        ),
    )

    code = main(
        ["retry", "--db", str(db), "--json", "--max-retries", "0"]
    )

    assert code == EXIT_OK
    payload = _payload(capsys)
    assert payload["status"] == "retried"
    assert payload["retried"] == 2
    assert payload["recovered"] == 1
    assert payload["still_failed"] == 1
    # Журнал попыток — в файле прогона, по строке на попытку.
    lines = [
        json.loads(line)
        for line in (db.parent / "logs" / "retry.jsonl").read_text("utf-8").splitlines()
    ]
    assert [line["outcome"] for line in lines] == ["recovered", "no_verdict"]
    assert all(line["run_id"] == RUN_ID for line in lines)


def test_undeclared_evaluator_model_refuses(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    spy: list[str],
) -> None:
    """Модель не объявлена в ``llm_scenarios`` — отказ, порты не собираются, EXIT_ERROR."""
    db = _make_db(tmp_path, declare_model=False, params=["param_alpha"])

    code = main(["retry", "--db", str(db), "--json"])

    assert code == EXIT_ERROR
    payload = _payload(capsys)
    assert payload["status"] == "refused"
    assert "llm_scenarios" in payload["message"]
    assert spy == []
