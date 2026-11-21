"""CLI ``hound retry`` и профиль локальных инструментов: провенанс, а не ``deploy``.

План ``28`` §9: ``tool_profile`` — 37-е поле ``EXECUTION_CONTRACT_FIELDS``, то есть
**условие измерения** (⇒ ``run_id``). Отсюда правило команды ремонта: она исполняет
**объявленное** условие прогона, а не то, что сейчас записано в ``deploy.yaml``.
``deploy`` описывает, что объявлено на этом хосте; ``run_id`` — чем прогон мерили.

Проверяется:

* **дефолт — из провенанса** (``runs.tool_profile``). В ``deploy`` кладётся заведомо
  другой профиль: если бы команда читала его, исход был бы иным (отказ сборки вместо
  исполнения или наоборот);
* **профиль доезжает до исполнения**, а не только до объявления: ячейка получает набор
  инструментов, а трасса вызовов ложится в ``cell_tool_calls``;
* **флаг — явное переопределение оператора**, и расхождение с провенансом **публикуется**
  заметкой: смена условия не должна выглядеть продолжением прежних;
* **ось остаётся осью**: ``plan --tool-profile`` меняет ``run_id`` (тот же флаг, который
  принимает ``retry``), а сам ``retry`` ``run_id`` не переписывает — он чинит прогон.

Офлайн: сеть не используется, оценщик — скриптованный in-memory порт (I18).
Нейтрально к предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from core.cli.errors import EXIT_OK, EXIT_USAGE
from core.cli.main import main
from core.config.deploy import DeployConfig, ToolsDeployConfig
from core.domain import (
    AbsenceRuleType,
    AnchorRelation,
    CatalogRow,
    Cell,
    CellState,
    Cohort,
    CohortEntry,
    CorpusChunk,
    CorpusDoc,
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
from core.domain.tools import ToolInvocation
from core.pipeline import Ports
from core.ports.llm import LLMRequest, LLMResponse, ToolTurn, ToolTurnRequest
from core.ports.vector_index import RetrievedChunk
from core.store.sqlite_store import SqliteStore
from tests.helpers.packages import valid_package

RUN_ID = "run-retry-tool-profile"
BUILD = "cb_new"
DOC_ID = "p1:hash1"
CHUNK_ID = "p1:dv_1:0"
QUOTE = "Свойство Альфа доступно без дополнительной настройки."


# ---------------------------------------------------------------------------
# Прогон-заготовка
# ---------------------------------------------------------------------------


def _param(param_id: str = "param_alpha") -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title="Параметр Альфа",
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


def _chunk() -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=CHUNK_ID,
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
        evidence=[{"source_id": CHUNK_ID, "quote": QUOTE}],
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


class _PlainEvaluator:
    """Оценщик **без** ``turn``: при включённом профиле повтор обязан отказать."""

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0

    def __init__(self) -> None:
        self.calls: list[LLMRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        output = _ok_output()
        return LLMResponse(
            output=output, raw=output.model_dump_json(), prompt_hash="ph", decoder="fake"
        )


class _TurnEvaluator(_PlainEvaluator):
    """Оценщик с ``turn``: один вызов инструмента, затем вердикт.

    Признак второго хода — отрендеренный результат инструмента в промпте, а не счётчик
    объекта: у каждой ячейки свой диалог, и общий счётчик был бы ошибкой привязки.
    """

    _RESULT_MARKER = "=== ЛОКАЛЬНЫЙ ИНСТРУМЕНТ"
    decoder = "none"

    def __init__(self) -> None:
        super().__init__()
        self.requests: list[ToolTurnRequest] = []

    def turn(self, req: ToolTurnRequest) -> ToolTurn:
        self.requests.append(req)
        if self._RESULT_MARKER not in req.prompt:
            return ToolTurn(
                tool_calls=(
                    ToolInvocation(
                        name="expand_to_section", arguments={"chunk_id": CHUNK_ID}
                    ),
                ),
                raw="{}",
                finish_reason="tool_calls",
            )
        output = _ok_output()
        return ToolTurn(
            final=LLMResponse(
                output=output,
                raw=output.model_dump_json(),
                prompt_hash="ph-turn",
                decoder="none",
            ),
            raw=output.model_dump_json(),
            finish_reason="stop",
        )


def _cell(param_id: str = "param_alpha") -> Cell:
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


def _plan_payload(param: CatalogRow) -> dict[str, object]:
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
        "params": [param.model_dump(mode="json")],
        "cohort": cohort.model_dump(mode="json"),
        "module_weights": {"mod_a": 1.0},
    }


def _make_db(tmp_path: Path, *, tool_profile: str | None) -> Path:
    """Прогон с одной ``failed``-ячейкой и **объявленным** профилем инструментов.

    ``tool_profile=None`` — колонка не записана (манифест прошлой схемы/модульный вызов):
    это «провенанс не несёт», а не «инструментов нет», и команда обязана назвать разницу.
    """
    db = tmp_path / "runs" / RUN_ID / "cohound.db"
    db.parent.mkdir(parents=True)
    param = _param()
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
        if tool_profile is not None:
            # Тот же писатель, что у фазы (``UPDATE runs`` через ``persist_manifest``).
            manifest = store.read_manifest(RUN_ID)
            store.persist_manifest(
                RUN_ID, manifest.model_copy(update={"tool_profile": tool_profile})
            )
        store.set_run_index_build_id(RUN_ID, BUILD)
        payload = _plan_payload(param)
        sha = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        store.save_plan_inputs(RUN_ID, payload=payload, payload_sha256=sha)
        store.save_cell(_cell())
        store.record_cell_failure(RUN_ID, "p1", "param_alpha", "evaluation_failed")
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
        # Корпус: без него ``expand_to_section`` не нашёл бы адрес (ok=False), и тест
        # мерил бы отказ инструмента, а не его исполнение.
        store.upsert_corpus_doc(
            CorpusDoc(
                doc_id=DOC_ID,
                product_id="p1",
                url="https://docs.test/p1/d1",
                content_hash="hash1",
            ),
            doc_version_id="dv_1",
        )
        store.add_chunks_batch(
            [
                CorpusChunk(
                    chunk_id=CHUNK_ID,
                    doc_id=DOC_ID,
                    product_id="p1",
                    chunk_no=0,
                    text=QUOTE,
                    heading_path="Раздел 1",
                    content_hash="hash_1",
                )
            ]
        )
    return db


def _use_ports(monkeypatch: pytest.MonkeyPatch, evaluator: Any) -> None:
    """Порты повтора без индексов и сети: только оценщик и лексический двойник."""

    def _factory(*args: object, **kwargs: object) -> Ports:
        return Ports(evaluator=evaluator, lexical_index=_LexicalStub(BUILD, [_chunk()]))

    monkeypatch.setattr("core.cli.commands.retry.build_adapters", _factory)


def _use_deploy(monkeypatch: pytest.MonkeyPatch, profile: str, max_calls: int) -> None:
    """``deploy.yaml`` с **другим** профилем: он не должен попасть в исполнение.

    Ровно это и проверяется: если команда читает дефолт из развёртывания, исход меняется.
    """
    deploy = DeployConfig(
        profile="local", tools=ToolsDeployConfig(profile=profile, max_calls=max_calls)
    )
    monkeypatch.setattr(
        "core.cli.commands.retry.load_deploy_config", lambda *a, **k: deploy
    )


def _payload(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    return json.loads(capsys.readouterr().out)


def _traces(db: Path) -> list[Any]:
    with SqliteStore(db) as store:
        return list(store.iter_tool_calls(RUN_ID))


# ---------------------------------------------------------------------------
# 1. Дефолт — из провенанса прогона, а не из deploy
# ---------------------------------------------------------------------------


def test_retry_uses_declared_profile_and_runs_the_tools(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Прогон объявил инструменты ⇒ повтор их **исполняет**, хотя ``deploy`` молчит."""
    db = _make_db(tmp_path, tool_profile="expand_to_section;max_calls=2")
    evaluator = _TurnEvaluator()
    _use_ports(monkeypatch, evaluator)
    _use_deploy(monkeypatch, profile="none", max_calls=0)

    code = main(["retry", "--db", str(db), "--json"])

    assert code == EXIT_OK
    payload = _payload(capsys)
    assert payload["recovered"] == 1
    # Исполненное условие названо в payload — а не только подразумевается из run_id.
    assert payload["tool_profile"] == "expand_to_section;max_calls=2"
    # Транспорт исполнен: иначе ``turn`` не вызывался бы вовсе.
    assert evaluator.requests
    assert [spec.name for spec in evaluator.requests[0].allowed_tools] == [
        "expand_to_section"
    ]
    # Трасса — в БД: это признак, что условие исполнено, а не только объявлено.
    rows = _traces(db)
    assert [(r.param_id, r.entry.seq, r.entry.name, r.entry.ok) for r in rows] == [
        ("param_alpha", 1, "expand_to_section", True)
    ]


def test_retry_without_declared_profile_ignores_deploy_default(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Провенанс не несёт профиль ⇒ повтор идёт **без** инструментов, и это названо.

    ``deploy`` здесь объявляет ``t1``: если бы команда брала дефолт оттуда, исполнение
    упёрлось бы в отсутствие транспорта (``EXIT_USAGE``). Исход ``EXIT_OK`` и заметка —
    доказательство, что дефолт взят из провенанса.
    """
    db = _make_db(tmp_path, tool_profile=None)
    evaluator = _PlainEvaluator()
    _use_ports(monkeypatch, evaluator)
    _use_deploy(monkeypatch, profile="t1", max_calls=2)

    code = main(["retry", "--db", str(db), "--json"])

    assert code == EXIT_OK
    payload = _payload(capsys)
    assert payload["recovered"] == 1
    assert len(evaluator.calls) == 1  # путь без инструментов: ровно один ``complete``
    assert payload["tool_profile"] == "-;max_calls=0"
    assert _traces(db) == []
    assert any("не записал профиль инструментов" in note for note in payload["notes"])


# ---------------------------------------------------------------------------
# 2. Флаг — явное переопределение, и оно публикуется
# ---------------------------------------------------------------------------


def test_retry_flag_overrides_provenance_and_publishes_the_note(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--tool-profile`` исполняется, но расхождение с ``run_id`` названо заметкой.

    Молчаливая смена условия была бы ровно тем дефектом, ради которого профиль входит в
    контракт: вердикт описывал бы не тот ``run_id``, под которым лежит.
    """
    db = _make_db(tmp_path, tool_profile="-;max_calls=0")
    evaluator = _TurnEvaluator()
    _use_ports(monkeypatch, evaluator)
    _use_deploy(monkeypatch, profile="none", max_calls=0)

    code = main(
        [
            "retry",
            "--db",
            str(db),
            "--json",
            "--tool-profile",
            "t1",
            "--tool-max-calls",
            "2",
        ]
    )

    assert code == EXIT_OK
    payload = _payload(capsys)
    assert evaluator.requests  # профиль дошёл до исполнения
    assert payload["tool_profile"] == "expand_to_section;max_calls=2"
    assert [r.entry.name for r in _traces(db)] == ["expand_to_section"]
    note = next((n for n in payload["notes"] if "переопределён оператором" in n), None)
    assert note is not None
    assert "run_id" in note
    # ``retry`` чинит прогон и идентичность не переписывает.
    assert payload["run_id"] == RUN_ID


def test_retry_flag_without_transport_is_a_visible_refusal(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Профиль включён, а оценщик не умеет ``turn`` — ``EXIT_USAGE``, а не тихий откат.

    Это и есть доказательство, что флаг доходит до **исполнения**: тихий откат к пути без
    инструментов прошёл бы с ``EXIT_OK`` и записал вердикт под условием, которого не
    исполнял.
    """
    db = _make_db(tmp_path, tool_profile="-;max_calls=0")
    evaluator = _PlainEvaluator()
    _use_ports(monkeypatch, evaluator)
    _use_deploy(monkeypatch, profile="none", max_calls=0)

    code = main(
        [
            "retry",
            "--db",
            str(db),
            "--tool-profile",
            "t1",
            "--tool-max-calls",
            "2",
        ]
    )

    captured = capsys.readouterr()
    assert code == EXIT_USAGE
    assert "tool_caller" in captured.err
    # Ни вердикта, ни трассы: отказ произошёл до исполнения.
    assert _traces(db) == []
    assert [c.state for c in _iter_cells(db)] == [CellState.FAILED]


def _iter_cells(db: Path) -> list[Cell]:
    with SqliteStore(db) as store:
        return list(store.iter_cells(RUN_ID))


# ---------------------------------------------------------------------------
# 3. Ось остаётся осью: ``plan`` с тем же флагом меняет ``run_id``
# ---------------------------------------------------------------------------


def test_tool_profile_flag_changes_run_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--tool-profile`` меняет ``run_id`` прогона — то есть это условие измерения.

    Тест стоит здесь, а не только у ``plan``/``run``: ``retry`` обязан читать **ту же**
    строку, которую ось записала в контракт, иначе «профиль из провенанса» означал бы
    «профиль из строки, которую никто не пишет».
    """
    monkeypatch.setenv("COHOUND_HOME", str(tmp_path / "home"))
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    common = ["--run-yaml", str(package / "run.yaml"), "--package", str(package), "--json"]

    assert main(["run", *common, "--dry-run"]) == EXIT_OK
    base = _payload(capsys)["run_id"]

    flags = ["--tool-profile", "t1", "--tool-max-calls", "2"]
    assert main(["run", *common, "--dry-run", *flags]) == EXIT_OK
    with_tools = _payload(capsys)["run_id"]

    assert with_tools != base
    # Та же ось, тот же резолвер: ``plan`` печатает тот же id, что ``run``.
    assert main(["plan", *common, *flags]) == EXIT_OK
    planned = _payload(capsys)
    assert planned["run_id"] == with_tools
    assert planned["tool_profile"] == "expand_to_section;max_calls=2"
