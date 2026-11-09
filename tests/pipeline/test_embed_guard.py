"""O1 плана 26: гард «эмбеддер процесса = сборка партиции» в dense-плече.

Проверяется то, что нельзя увидеть «на глаз»:

* две **одинаково размерные** модели дают видимый отказ (:class:`EmbedderBuildMismatch`),
  а не молчаливый скор под чужим ``run_id`` — на пути фазы (``run --until evaluate``) и
  на пути повтора (``hound retry``);
* совпадающая модель не мешает поиску: dense-плечо реально отдаёт чанк в промпт;
* пустой ``model_id`` с любой стороны — **не** отказ (A3), но маркер
  ``embed_build_unverified`` в ``validation_notes``;
* профиль ``R0`` гард не применяет вовсе: dense-плечо не участвует в измерении.

Офлайн, на ``SqliteStore(":memory:")`` и поддельных портах (I18/I19 не ослабляются).
"""

from __future__ import annotations

import datetime as _dt
import json
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest

from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    Cell,
    CellState,
    CorpusCoverage,
    Focus,
    Importance,
    IndexManifestEntry,
    LLMCellOutput,
    ParamType,
    RunConfig,
    RunMode,
    SourceKind,
    Status,
)
from core.pipeline import Ports
from core.pipeline.evaluate import (
    EMBED_BUILD_UNVERIFIED_REASON,
    evaluate_phase,
    retry_failed_cells,
)
from core.pipeline.retrieve import EmbedderBuildMismatch, retrieval_params
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.vector_index import RetrievedChunk
from core.store.sqlite_store import SqliteStore
from tests.fakes.ports import FakeEmbedder, FakeLexicalIndex, FakeVectorIndex

RUN_ID = "run-embed-guard"
COMPOSITION = "cb_guard"
PARTITION = "pb_guard_p1"
QUOTE = "Свойство Альфа доступно без дополнительной настройки."


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


def _chunk(product_id: str = "p1") -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=f"chunk_{product_id}_1",
        product_id=product_id,
        text=QUOTE,
        content_hash="hash_1",
        heading_path="Раздел 1",
        build_id=PARTITION,
        score=1.0,
    )


class _RecordingEvaluator:
    """Оценщик, запоминающий промпты: проверка «dense-плечо реально дало чанк»."""

    model_id = "fake-evaluator"
    model_version = "1.0"
    seed = 0
    temperature = 0.0

    def __init__(self) -> None:
        self.prompts: list[str] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.prompts.append(req.prompt)
        output = LLMCellOutput(
            status=Status.ABSENT,
            justification="нет в срезе",
            evidence=[],
        )
        return LLMResponse(
            output=output,
            raw=output.model_dump_json(),
            prompt_hash="ph",
            decoder="fake",
        )


def _coverage() -> CorpusCoverage:
    """Полное покрытие: zero-source ветка фазы не перехватывает оценку."""
    return CorpusCoverage(
        product_id="p1",
        complete=True,
        expected_sources=1,
        observed_sources=1,
        computed_at="2026-11-09T00:00:00+00:00",
    )


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    with SqliteStore(":memory:") as s:
        s.create_run(
            RUN_ID,
            RunConfig(
                case_id="case-1",
                anchor="p1",
                cohort="cohort.yaml",
                products=["p1"],
                package="pkg-1",
                focus=Focus(),
                snapshot_date=_dt.date(2026, 11, 9),
                mode=RunMode.STRICT,
            ),
        )
        s.set_run_index_build_id(RUN_ID, COMPOSITION)
        s.save_corpus_coverage(RUN_ID, _coverage())
        yield s


def _manifest(store: SqliteStore, embed_model_id: str | None) -> None:
    store.save_index_manifest(
        IndexManifestEntry(
            index_build_id=COMPOSITION,
            partition_build_id=PARTITION,
            product_id="p1",
            embed_model_id=embed_model_id,
        )
    )


def _plan() -> SimpleNamespace:
    return SimpleNamespace(
        products=["p1"],
        params=[_param()],
        run_id=RUN_ID,
        snapshot_date=_dt.date(2026, 11, 9),
        cohort=SimpleNamespace(frozen_at=_dt.date(2026, 11, 9)),
    )


def _phase(store: SqliteStore, ports: Ports, *, profile: str) -> Any:
    return evaluate_phase(
        plan=_plan(),
        ports=ports,
        store=store,
        run_id=RUN_ID,
        profile=profile,
    )


def _ports(
    *,
    embed_model_id: str,
    build_id: str = COMPOSITION,
    chunk: RetrievedChunk | None = None,
) -> Ports:
    docs = [chunk or _chunk()]
    return Ports(
        evaluator=_RecordingEvaluator(),
        lexical_index=FakeLexicalIndex(docs=docs, build_id=build_id),
        vector_index=FakeVectorIndex(docs=docs, build_id=build_id),
        embedder=FakeEmbedder(model_id=embed_model_id),
    )


# ---------------------------------------------------------------------------
# Отказ: две модели одной размерности
# ---------------------------------------------------------------------------


def test_evaluate_phase_refuses_when_embedder_differs(store: SqliteStore) -> None:
    """Путь ``run --until evaluate``: расхождение id — видимый отказ, а не тихий скор."""
    _manifest(store, "other-model@rev2")
    with pytest.raises(EmbedderBuildMismatch) as exc:
        _phase(store, _ports(embed_model_id="model@rev1"), profile="R1")
    assert exc.value.query_model_id == "model@rev1"
    assert exc.value.build_model_id == "other-model@rev2"


def test_retry_refuses_when_embedder_differs(store: SqliteStore) -> None:
    """Путь ``hound retry``: та же сверка, тот же класс отказа."""
    _manifest(store, "other-model@rev2")
    store.save_cell(
        Cell(
            run_id=RUN_ID,
            product_id="p1",
            param_id="param_alpha",
            state=CellState.FAILED,
            status=None,
            source_kind=SourceKind.DOCUMENTATION,
            index_build_id=PARTITION,
            retrieval_profile="R1",
            retrieval_params_json=json.dumps(
                retrieval_params(profile="R1", cutoff_k=10, rrf_k=60),
                sort_keys=True,
            ),
        )
    )
    store.record_cell_failure(RUN_ID, "p1", "param_alpha", "evaluation_failed")
    with pytest.raises(EmbedderBuildMismatch):
        retry_failed_cells(
            store=store,
            run_id=RUN_ID,
            params={"param_alpha": _param()},
            ports=_ports(embed_model_id="model@rev1"),
        )


# ---------------------------------------------------------------------------
# Совпадение: поиск идёт
# ---------------------------------------------------------------------------


def test_matching_embedder_dense_returns_chunk(store: SqliteStore) -> None:
    """Совпадающая модель: dense-плечо реально отдаёт чанк (он виден в промпте)."""
    _manifest(store, "model@rev1")
    ports = _ports(embed_model_id="model@rev1")
    result = _phase(store, ports, profile="R1")

    assert result.status == "ok"
    assert result.cells and result.cells[0].state is CellState.EVALUATED
    evaluator = ports.evaluator
    assert isinstance(evaluator, _RecordingEvaluator)
    assert evaluator.prompts, "оценщик обязан быть вызван"
    assert QUOTE in evaluator.prompts[0], "dense-плечо не дало чанк в промпт"


# ---------------------------------------------------------------------------
# A3: пустой model_id — наблюдение, не отказ
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("query_model, build_model", [("", "model@rev1"), ("model@rev1", None)])
def test_empty_model_id_marks_unverified(
    store: SqliteStore, query_model: str, build_model: str | None
) -> None:
    """Пустая сторона — не отказ, но ячейки несут ``embed_build_unverified``."""
    _manifest(store, build_model)
    result = _phase(store, _ports(embed_model_id=query_model), profile="R1")

    assert result.status == "ok"
    assert result.cells
    assert all(
        EMBED_BUILD_UNVERIFIED_REASON in cell.validation_notes
        for cell in result.cells
    )
    persisted = {c.param_id: c for c in store.iter_cells(RUN_ID)}
    assert EMBED_BUILD_UNVERIFIED_REASON in persisted["param_alpha"].validation_notes


# ---------------------------------------------------------------------------
# R0: dense-плечо не используется — гард не применяется
# ---------------------------------------------------------------------------


def test_r0_ignores_embedder_mismatch(store: SqliteStore) -> None:
    """R0 не трогает эмбеддер: расхождение id не может быть отказом измерения."""
    _manifest(store, "other-model@rev2")
    ports = _ports(embed_model_id="model@rev1")
    result = _phase(store, ports, profile="R0")

    assert result.status == "ok"
    assert result.cells and result.cells[0].state is CellState.EVALUATED
    assert all(
        EMBED_BUILD_UNVERIFIED_REASON not in cell.validation_notes
        for cell in result.cells
    )
