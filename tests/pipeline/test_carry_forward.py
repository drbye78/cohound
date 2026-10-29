"""Перенос ячеек между прогонами (09 §10, схема ``cell-reuse-2``) — исполняется.

Проверяется то, что делает перенос **провенансом**, а не экономией: ячейка второго
прогона при неизменных условиях имеет вердикт и доказательства первого, помечена
``measurement_origin=carried`` с адресом источника, а при любом изменении условий
(набор документов, покрытие, вердикт ``❌`` без ``live|revalidated``, resume текущего
прогона) — измеряется заново.

Офлайн: SQLite в памяти (два «прогона» — две БД), оценщик — скрипт, ``state.db`` —
``SqliteStateStore``.

Нумерация ``T15–T20`` в 09 §15.1 занята другими приёмками (retired→re-add,
``canonical_name``, ``cutoff_K``, документ вне top-K, методология, модель, offline),
поэтому тесты волны 2 названы по §10.3/§11.4 — перенос ячеек.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    CorpusCoverage,
    DenialStrength,
    Focus,
    LLMCellOutput,
    MeasurementOrigin,
    RunConfig,
    RunMode,
    Status,
)
from core.pipeline.carry_forward import (
    carry_forward_cell,
    cell_reuse_conditions,
    evidence_digest,
)
from core.pipeline.corpus import _chunk_id
from core.pipeline.digests import (
    CELL_REUSE_SCHEMA,
    cell_reuse_key,
    retrieved_doc_set_digest,
)
from core.pipeline.eval_cache import EvalConditions, catalog_row_digest
from core.pipeline.evaluate import evaluate_phase
from core.pipeline.incremental import doc_version_id
from core.pipeline.report import collect_cell_origins
from core.pipeline.runner import Ports
from core.pipeline.state_sync import record_run_outcome
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.types import Chunk
from core.ports.vector_index import RetrievalResult, RetrievedChunk
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore

FIXTURES = Path(__file__).parent / "fixtures"
PRODUCT = "p1"
RUN_1 = "run_carry_1"
RUN_2 = "run_carry_2"

#: Продукт-партиция среза (``RetrievedChunk.build_id``): слагаемое ключа переноса.
PARTITION = "pb_fake_1"

#: Метка измерения: фиксирована, потому что в ключ не входит, а в строку индекса — да.
MEASURED_AT = "2026-10-30T00:00:00+00:00"


def _param(absence_rule: AbsenceRuleType | None = None) -> CatalogRow:
    """Строка каталога из общего фикстура (``absence_rule=explicit`` по умолчанию)."""
    data = json.loads((FIXTURES / "evaluate_inputs.json").read_text(encoding="utf-8"))
    param = CatalogRow(**data["param"])
    return (
        param
        if absence_rule is None
        else param.model_copy(update={"absence_rule": absence_rule})
    )


def _dv(doc_hash: str) -> str:
    """Версия документа в той форме, которую несёт ``chunk_id`` (16 §6)."""
    return doc_version_id(PRODUCT, doc_hash, "", "")


def _chunk(doc_hash: str, chunk_no: int, text: str) -> Chunk:
    """Чанк с настоящим ``chunk_id``: ``product:doc_version_id:chunk_no``."""
    return Chunk(
        chunk_id=_chunk_id(PRODUCT, _dv(doc_hash), chunk_no),
        chunk_no=chunk_no,
        text=text,
        heading_path="Раздел",
        context_prefix="контекст",
        content_hash=f"ch_{doc_hash}",
    )


def _source_id(doc_hash: str, chunk_no: int = 0) -> str:
    """``source_id`` в форме ``chunk_id`` прогона."""
    return _chunk_id(PRODUCT, _dv(doc_hash), chunk_no)


def _present(docs: list[tuple[str, str]]) -> LLMCellOutput:
    """``✅`` с прямыми цитатами из перечисленных документов."""
    return LLMCellOutput(
        status=Status.OK,
        justification="Вендор описывает свойство в документации.",
        evidence=[
            {"source_id": _source_id(doc_hash), "quote": text}
            for doc_hash, text in docs
        ],
    )


def _absent(docs: list[tuple[str, str]]) -> LLMCellOutput:
    """``❌`` с прямым отрицанием, опирающийся на перечисленные документы."""
    return LLMCellOutput(
        status=Status.ABSENT,
        justification="Вендор прямо отрицает поддержку свойства.",
        denial_strength="explicit",
        denial_reason="прямое отрицание в документации",
        evidence=[
            {"source_id": _source_id(doc_hash), "quote": text}
            for doc_hash, text in docs
        ],
    )


def _coverage(*, complete: bool = True) -> CorpusCoverage:
    return CorpusCoverage(
        product_id=PRODUCT,
        complete=complete,
        expected_sources=3,
        observed_sources=3 if complete else 2,
        computed_at="2026-10-01T00:00:00+00:00",
    )


class _ScriptedEvaluator:
    """Один ответ оценщика (без сети, I18); считает вызовы."""

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0
    decoder = "fake"
    endpoint_kind = "local"

    def __init__(self, out: LLMCellOutput) -> None:
        self.out = out
        self.calls: list[LLMRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        return LLMResponse(
            output=self.out,
            raw=self.out.model_dump_json(),
            prompt_hash="ph",
            decoder="fake",
        )


def _store(run_id: str) -> SqliteStore:
    """БД прогона с обязательной строкой ``runs``."""
    store = SqliteStore(":memory:")
    store.create_run(
        run_id,
        RunConfig(
            case_id="case",
            anchor=PRODUCT,
            cohort="case",
            products=[PRODUCT],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 10, 19),
            mode=RunMode.STRICT,
        ),
    )
    return store


def _conditions() -> EvalConditions:
    """Объявленные условия оценки (те же, что строит composition root из run.yaml)."""
    return EvalConditions(
        methodology_version="0.2.0",
        status_dict_version="2026-10-01",
        capability_policy="resolved-only-v1",
        unknown_policy="excluded",
        partial_score=0.5,
    )


def _plan() -> SimpleNamespace:
    return SimpleNamespace(
        products=[PRODUCT],
        params=[_param()],
        run_id=RUN_1,
        snapshot_date=_dt.date(2026, 10, 19),
        cohort=SimpleNamespace(frozen_at=_dt.date(2026, 10, 19)),
    )


def _retrieval(chunks: list[Chunk]) -> RetrievalResult:
    """Срез ретривала из готовых чанков — та же форма, что у настоящего ``retrieve``.

    ``build_id`` заполняется: он входит в ключ переноса (продукт-партиция, §10.1), и
    пустая строка сделала бы проверку слабее — «партиция не объявлена» вместо «та же».
    """
    return RetrievalResult(
        chunks=[
            RetrievedChunk(
                chunk_id=chunk.chunk_id,
                product_id=PRODUCT,
                text=chunk.text,
                content_hash=chunk.content_hash,
                heading_path=chunk.heading_path,
                build_id=PARTITION,
                score=1.0,
            )
            for chunk in chunks
        ],
        profile="R1",
        cutoff_K=12,
        build_id=PARTITION,
    )


def _run_phase(
    *,
    run_id: str,
    store: SqliteStore,
    evaluator: _ScriptedEvaluator,
    chunks: list[Chunk],
    coverage: CorpusCoverage,
    state: SqliteStateStore | None = None,
    previous_store: SqliteStore | None = None,
    previous_run_id: str | None = None,
    monkeypatch: pytest.MonkeyPatch | None = None,
):
    """Гоняет Ш6 с одним продуктом/параметром и заранее известным срезом (I17).

    Ретривал подменяется **на границе фазы** (:func:`core.pipeline.evaluate.retrieve_for_cell`):
    тест проверяет перенос, а не поиск. Всё остальное — настоящее: покрытие пишется в БД
    прогона, оценщик вызывается по-настоящему (скриптом), ячейки и evidence пишутся в
    store, строки индекса — в ``state.db``.
    """
    plan = _plan()
    plan.run_id = run_id
    store.save_corpus_coverage(run_id, coverage)
    if monkeypatch is not None:
        monkeypatch.setattr(
            "core.pipeline.evaluate.retrieve_for_cell",
            lambda *a, **kw: _retrieval(chunks),
        )
    return evaluate_phase(
        plan=plan,
        ports=Ports(evaluator=evaluator),
        store=store,
        run_id=run_id,
        state=state,
        previous_store=previous_store,
        previous_run_id=previous_run_id,
        eval_conditions=_conditions(),
    )


# ---------------------------------------------------------------------------
# Ключ: одинаковые условия → одинаковый ключ; набор документов порядко-независим
# ---------------------------------------------------------------------------


def test_retrieved_doc_set_digest_is_order_independent() -> None:
    """Набор документов ячейки не зависит от порядка выдачи (09 §10.1)."""
    a = _chunk("hash_a", 0, "текст А")
    b = _chunk("hash_b", 0, "текст Б")

    assert retrieved_doc_set_digest([a, b]) == retrieved_doc_set_digest([b, a])


def test_retrieved_doc_set_digest_ignores_rank_and_chunk_identity() -> None:
    """``score``/``rank``/``chunk_id`` в набор не входят: меняется выдача, не документы."""
    chunk = _chunk("hash_a", 0, "текст А")
    same_doc_other_chunk = _chunk("hash_a", 1, "другой фрагмент того же документа")
    reranked = Chunk(
        chunk_id=chunk.chunk_id,
        chunk_no=0,
        text=chunk.text,
        heading_path=chunk.heading_path,
        context_prefix=chunk.context_prefix,
        content_hash=chunk.content_hash,
    )

    assert retrieved_doc_set_digest([chunk]) == retrieved_doc_set_digest(
        [same_doc_other_chunk]
    )
    assert retrieved_doc_set_digest([chunk]) == retrieved_doc_set_digest([reranked])


def test_cell_reuse_key_is_stable_for_identical_conditions() -> None:
    """Один и тот же набор условий даёт один и тот же ключ (детерминизм, §S8)."""
    chunk = _chunk("hash_a", 0, "текст А")
    kwargs = dict(
        product_id=PRODUCT,
        param_id="param_alpha",
        catalog_row_digest=catalog_row_digest(_param()),
        prompt_template_digest="tmpl-1",
        model_id="fake-model",
        llm_scenario="evaluator",
        model_config_digest="cfg-1",
        retrieval_profile="R1",
        cutoff_k=12,
        retrieval_params_json='{"cutoff_K": 12, "profile": "R1"}',
        retrieved_doc_set_digest=retrieved_doc_set_digest([chunk]),
        coverage_digest="cov-1",
        methodology_version="0.2.0",
        status_dict_version="2026-10-01",
        scoring_policy_version="resolved-only-v1+excluded@abc",
        capability_policy="resolved-only-v1",
        unknown_policy="excluded",
        partial_score=0.5,
        attestation_digest="att-1",
        partition_build_id="pb-1",
    )

    assert cell_reuse_key(**kwargs) == cell_reuse_key(**kwargs)
    # Смена схемы ключа обязана менять ключ: иначе «2» не отличить от «1».
    assert CELL_REUSE_SCHEMA == "cell-reuse-2"


def test_cell_reuse_key_changes_with_any_condition() -> None:
    """Любое слагаемое меняет ключ — иначе перенос пережил бы смену условий."""
    base = dict(
        product_id=PRODUCT,
        param_id="param_alpha",
        catalog_row_digest="cat",
        prompt_template_digest="tmpl",
        model_id="model",
        llm_scenario="evaluator",
        model_config_digest="cfg",
        retrieval_profile="R1",
        cutoff_k=12,
        retrieval_params_json="params",
        retrieved_doc_set_digest="docs",
        coverage_digest="cov",
        methodology_version="0.2.0",
        status_dict_version="2026-10-01",
        scoring_policy_version="policy",
        capability_policy="cap",
        unknown_policy="unknown",
        partial_score=0.5,
        attestation_digest="att",
        partition_build_id="pb",
    )
    reference = cell_reuse_key(**base)
    for field, value in (
        ("catalog_row_digest", "cat2"),
        ("prompt_template_digest", "tmpl2"),
        ("model_id", "model2"),
        ("llm_scenario", "other"),
        ("model_config_digest", "cfg2"),
        ("retrieval_profile", "R2"),
        ("cutoff_k", 24),
        ("retrieval_params_json", "params2"),
        ("retrieved_doc_set_digest", "docs2"),
        ("coverage_digest", "cov2"),
        ("methodology_version", "0.3.0"),
        ("status_dict_version", None),
        ("scoring_policy_version", "policy2"),
        ("capability_policy", "cap2"),
        ("unknown_policy", "included"),
        ("partial_score", 0.25),
        ("attestation_digest", "att2"),
        ("partition_build_id", "pb2"),
    ):
        assert cell_reuse_key(**{**base, field: value}) != reference, field


# ---------------------------------------------------------------------------
# Перенос состоялся: вердикт, evidence и провенанс
# ---------------------------------------------------------------------------


def test_second_run_carries_cell_without_calling_evaluator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Неизменные условия → ячейка второго прогона перенесена, оценщик не вызван.

    Это и есть смысл механизма: вердикт берётся из прошлого цикла **с доказательствами**,
    а не пересчитывается моделью (09 §10.2).
    """
    docs = [("hash_a", "Свойство Альфа поддерживается в версии 2.")]
    chunks = [_chunk(doc_hash, 0, text) for doc_hash, text in docs]

    first_store = _store(RUN_1)
    first = _run_phase(
        run_id=RUN_1,
        store=first_store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=chunks,
        coverage=_coverage(),
        monkeypatch=monkeypatch,
    )
    assert first.status == "ok", first.errors
    assert len(first.cells) == 1

    state = SqliteStateStore(":memory:")
    # Первый прогон пишет индекс переноса (``observed``): без него второму нечего
    # сравнивать.
    _run_phase(
        run_id=RUN_1,
        store=first_store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=chunks,
        coverage=_coverage(),
        state=state,
        monkeypatch=monkeypatch,
    )
    index_rows = state.iter_cell_measurements(PRODUCT, run_id=RUN_1)
    assert len(index_rows) == 1
    assert index_rows[0].cell_reuse_key

    second_evaluator = _ScriptedEvaluator(_present(docs))
    second_store = _store(RUN_2)
    second = _run_phase(
        run_id=RUN_2,
        store=second_store,
        evaluator=second_evaluator,
        chunks=chunks,
        coverage=_coverage(),
        state=state,
        previous_store=first_store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )

    assert second.status == "ok", second.errors
    assert second_evaluator.calls == []  # вызов модели не потрачен
    carried = second.cells[0]
    assert carried.measurement_origin is MeasurementOrigin.CARRIED
    assert carried.carried_from_run_id == RUN_1
    assert carried.run_id == RUN_2
    assert carried.status is Status.OK
    # Доказательства перенесены вместе с вердиктом (I1/I22: ✅ без evidence не бывает).
    evidence = second_store.iter_cell_evidence(RUN_2, PRODUCT, "param_alpha")
    assert len(evidence) == 1
    assert evidence[0].citation_quote == docs[0][1]
    assert evidence[0].run_id == RUN_2
    assert evidence[0].evidence_id != ""
    # Счётчики фазы: ячейка не измерена сейчас, а перенесена.
    assert any(
        "cells_carried=1" in str(entry) for entry in second.log_entries
    ), second.log_entries


def test_carried_cell_writes_its_own_measurement_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Перенесённая ячейка получает строку индекса: цепочка не рвётся на третьем цикле."""
    docs = [("hash_a", "Свойство Альфа поддерживается в версии 2.")]
    chunks = [_chunk(doc_hash, 0, text) for doc_hash, text in docs]
    state = SqliteStateStore(":memory:")

    first_store = _store(RUN_1)
    _run_phase(
        run_id=RUN_1,
        store=first_store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=chunks,
        coverage=_coverage(),
        state=state,
        monkeypatch=monkeypatch,
    )
    second_store = _store(RUN_2)
    _run_phase(
        run_id=RUN_2,
        store=second_store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=chunks,
        coverage=_coverage(),
        state=state,
        previous_store=first_store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )

    rows = state.iter_cell_measurements(PRODUCT, run_id=RUN_2)
    assert len(rows) == 1
    assert rows[0].cell_reuse_key
    # Ключ переноса тот же, что у источника: иначе третий цикл не узнал бы ячейку.
    first_row = state.iter_cell_measurements(PRODUCT, run_id=RUN_1)[0]
    assert rows[0].cell_reuse_key == first_row.cell_reuse_key


# ---------------------------------------------------------------------------
# Перенос запрещён: каждое условие названо (09 §10.3)
# ---------------------------------------------------------------------------


def test_changed_document_set_prevents_carry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Другой набор документов (новый документ в top-K) → переоценка."""
    docs = [("hash_a", "Свойство Альфа поддерживается в версии 2.")]
    state = SqliteStateStore(":memory:")
    first_store = _store(RUN_1)
    _run_phase(
        run_id=RUN_1,
        store=first_store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=[_chunk("hash_a", 0, docs[0][1])],
        coverage=_coverage(),
        state=state,
        monkeypatch=monkeypatch,
    )

    changed_docs = [
        *docs,
        ("hash_b", "Свойство Альфа появилось в версии 2 и описано отдельно."),
    ]
    evaluator = _ScriptedEvaluator(_present(changed_docs))
    second_store = _store(RUN_2)
    second = _run_phase(
        run_id=RUN_2,
        store=second_store,
        evaluator=evaluator,
        chunks=[_chunk(doc_hash, 0, text) for doc_hash, text in changed_docs],
        coverage=_coverage(),
        state=state,
        previous_store=first_store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )

    assert len(evaluator.calls) == 1  # ячейка измерена заново
    assert second.cells[0].measurement_origin is MeasurementOrigin.OBSERVED


def test_new_document_outside_top_k_does_not_block_carry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Документ, не вошедший в top-K ячейки, перенос **не** запрещает (per-cell, §10.1).

    Это и есть смысл per-cell: инвалидация точная. Продукт получил новый документ, но
    retrieved-набор ячейки тот же — значит доказательства те же, и вердикт переносится.
    (Проверка «документ внутри top-K» — в `test_changed_document_set_prevents_carry`.)
    """
    docs = [("hash_a", "Свойство Альфа поддерживается в версии 2.")]
    state = SqliteStateStore(":memory:")
    first_store = _store(RUN_1)
    _run_phase(
        run_id=RUN_1,
        store=first_store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=[_chunk("hash_a", 0, docs[0][1])],
        coverage=_coverage(),
        state=state,
        monkeypatch=monkeypatch,
    )

    # Новый документ продукта: он есть в корпусе, но в срез ячейки не попал.
    second_store = _store(RUN_2)
    from core.domain import CorpusDoc, ReuseKind

    second_store.upsert_corpus_doc(
        CorpusDoc(
            doc_id=f"{PRODUCT}:ch_hash_new",
            product_id=PRODUCT,
            url="https://p1.test/new",
            reuse_kind=ReuseKind.LIVE,
        ),
        doc_version_id=_dv("hash_new"),
    )
    evaluator = _ScriptedEvaluator(_present(docs))
    second = _run_phase(
        run_id=RUN_2,
        store=second_store,
        evaluator=evaluator,
        chunks=[_chunk("hash_a", 0, docs[0][1])],
        coverage=_coverage(),
        state=state,
        previous_store=first_store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )

    assert evaluator.calls == []
    assert second.cells[0].measurement_origin is MeasurementOrigin.CARRIED
    assert second.cells[0].status is Status.OK


def test_incomplete_coverage_prevents_carry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Неполный корпус не переносит вердикт (I2c): «нет» не доказуемо неполнотой."""
    docs = [("hash_a", "Свойство Альфа не поддерживается в этой версии продукта.")]
    state = SqliteStateStore(":memory:")
    first_store = _store(RUN_1)
    _run_phase(
        run_id=RUN_1,
        store=first_store,
        evaluator=_ScriptedEvaluator(_absent(docs)),
        chunks=[_chunk("hash_a", 0, docs[0][1])],
        coverage=_coverage(),
        state=state,
        monkeypatch=monkeypatch,
    )

    evaluator = _ScriptedEvaluator(_absent(docs))
    second_store = _store(RUN_2)
    second = _run_phase(
        run_id=RUN_2,
        store=second_store,
        evaluator=evaluator,
        chunks=[_chunk("hash_a", 0, docs[0][1])],
        coverage=_coverage(complete=False),
        state=state,
        previous_store=first_store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )

    assert len(evaluator.calls) == 1
    assert second.cells[0].measurement_origin is MeasurementOrigin.OBSERVED


def test_resume_of_the_same_run_does_not_carry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resume текущего прогона не переносит против себя (``previous_run_id`` не он сам)."""
    docs = [("hash_a", "Свойство Альфа поддерживается в версии 2.")]
    state = SqliteStateStore(":memory:")
    store = _store(RUN_1)
    _run_phase(
        run_id=RUN_1,
        store=store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=[_chunk("hash_a", 0, docs[0][1])],
        coverage=_coverage(),
        state=state,
        monkeypatch=monkeypatch,
    )

    evaluator = _ScriptedEvaluator(_present(docs))
    again = _run_phase(
        run_id=RUN_1,
        store=store,
        evaluator=evaluator,
        chunks=[_chunk("hash_a", 0, docs[0][1])],
        coverage=_coverage(),
        state=state,
        previous_store=store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )

    assert len(evaluator.calls) == 1
    assert again.cells[0].measurement_origin is MeasurementOrigin.OBSERVED


def test_without_state_carry_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Нет ``state.db`` — нечего сравнивать; перенос выключен, а не «ключ не совпал»."""
    docs = [("hash_a", "Свойство Альфа поддерживается в версии 2.")]
    chunks = [_chunk("hash_a", 0, docs[0][1])]
    first_store = _store(RUN_1)
    _run_phase(
        run_id=RUN_1,
        store=first_store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=chunks,
        coverage=_coverage(),
        monkeypatch=monkeypatch,
    )

    evaluator = _ScriptedEvaluator(_present(docs))
    second = _run_phase(
        run_id=RUN_2,
        store=_store(RUN_2),
        evaluator=evaluator,
        chunks=chunks,
        coverage=_coverage(),
        previous_store=first_store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )

    assert len(evaluator.calls) == 1
    assert second.cells[0].measurement_origin is MeasurementOrigin.OBSERVED
    assert any("carry=—" in str(entry) for entry in second.log_entries)


def test_absent_verdict_is_not_carried_on_reused_corpus(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Перенесённый ``❌`` проходит **текущие** правила допуска (16 §8 п.4, I2c).

    Перенос — не обход инварианта: если в этом цикле документ взят из retained-байтов
    (``reused``), ``❌`` не публикуется и ячейка измеряется заново — ровно как измеренная.
    """
    docs = [("hash_a", "Свойство Альфа не поддерживается в этой версии продукта.")]
    state = SqliteStateStore(":memory:")
    first_store = _store(RUN_1)
    _run_phase(
        run_id=RUN_1,
        store=first_store,
        evaluator=_ScriptedEvaluator(_absent(docs)),
        chunks=[_chunk("hash_a", 0, docs[0][1])],
        coverage=_coverage(),
        state=state,
        monkeypatch=monkeypatch,
    )

    from core.domain import CorpusDoc, ReuseKind

    second_store = _store(RUN_2)
    second_store.upsert_corpus_doc(
        CorpusDoc(
            doc_id=f"{PRODUCT}:ch_hash_a",
            product_id=PRODUCT,
            url="https://p1.test/a",
            reuse_kind=ReuseKind.REUSED,
        ),
        doc_version_id=_dv("hash_a"),
    )
    evaluator = _ScriptedEvaluator(_absent(docs))
    second = _run_phase(
        run_id=RUN_2,
        store=second_store,
        evaluator=evaluator,
        chunks=[_chunk("hash_a", 0, docs[0][1])],
        coverage=_coverage(),
        state=state,
        previous_store=first_store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )

    assert len(evaluator.calls) == 1
    cell = second.cells[0]
    assert cell.measurement_origin is MeasurementOrigin.OBSERVED
    # И правило сработало ровно как на живой ячейке: ``❌`` понижен до ``❓``.
    assert cell.status is Status.UNKNOWN
    assert cell.evidence_scope is not None
    assert cell.evidence_scope.denial_strength is DenialStrength.NOT_STATED


def test_carry_forward_cell_refuses_without_previous_run() -> None:
    """``prev_run_id=None`` — переносить не из чего (первый прогон)."""
    conditions = cell_reuse_conditions(
        product_id=PRODUCT,
        conditions=_conditions(),
        scoring_policy_version="policy",
        # Ответ оценщика здесь не используется: проверяется отказ переноса по входу.
        evaluator=_ScriptedEvaluator(_absent([])),
        scope_acceptance=None,
    )
    assert (
        carry_forward_cell(
            _param(),
            product_id=PRODUCT,
            run_id=RUN_2,
            prev_run_id=None,
            state=SqliteStateStore(":memory:"),
            prev_store=_store(RUN_1),
            conditions=conditions,
            coverage=_coverage(),
            retrieved=[_chunk("hash_a", 0, "текст")],
        )
        is None
    )


# ---------------------------------------------------------------------------
# Счётчики: снимок и отчёт называют одно и то же число
# ---------------------------------------------------------------------------


def test_snapshot_counts_carried_cells(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``product_snapshot.cells_carried`` берётся из ``cells.measurement_origin``."""
    docs = [("hash_a", "Свойство Альфа поддерживается в версии 2.")]
    chunks = [_chunk("hash_a", 0, docs[0][1])]
    state = SqliteStateStore(":memory:")

    first_store = _store(RUN_1)
    _run_phase(
        run_id=RUN_1,
        store=first_store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=chunks,
        coverage=_coverage(),
        state=state,
        monkeypatch=monkeypatch,
    )
    second_store = _store(RUN_2)
    _run_phase(
        run_id=RUN_2,
        store=second_store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=chunks,
        coverage=_coverage(),
        state=state,
        previous_store=first_store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )
    record_run_outcome(
        state,
        run_id=RUN_2,
        package_id="pkg",
        cycle_id="c1",
        products=[PRODUCT],
        store=second_store,
        status="ok",
        started_at="2026-10-30T00:00:00+00:00",
        completed_at="2026-10-30T00:10:00+00:00",
        previous_run_id=RUN_1,
        evaluate_ok=True,
    )

    snapshot = state.last_snapshot(PRODUCT)
    assert snapshot is not None
    assert (snapshot.cells_total, snapshot.cells_measured) == (1, 1)
    assert snapshot.cells_carried == 1
    # Отчёт считает то же самое по записанным ячейкам — расхождения быть не может.
    origins = collect_cell_origins(second_store.iter_cells(RUN_2))
    assert origins == [
        {"product_id": PRODUCT, "cells_total": 1, "cells_carried": 1}
    ]


def test_first_run_writes_index_but_carries_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Первый прогон: индекс переноса пишется, переноса нет (``cells_carried = 0``)."""
    docs = [("hash_a", "Свойство Альфа поддерживается в версии 2.")]
    state = SqliteStateStore(":memory:")
    store = _store(RUN_1)

    result = _run_phase(
        run_id=RUN_1,
        store=store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=[_chunk("hash_a", 0, docs[0][1])],
        coverage=_coverage(),
        state=state,
        monkeypatch=monkeypatch,
    )

    assert result.status == "ok", result.errors
    assert result.cells[0].measurement_origin is MeasurementOrigin.OBSERVED
    rows = state.iter_cell_measurements(PRODUCT, run_id=RUN_1)
    assert len(rows) == 1
    # Ключ записан уже здесь: иначе второй прогон не смог бы ничего сравнить.
    assert rows[0].cell_reuse_key
    assert rows[0].evidence_digest
    assert collect_cell_origins(store.iter_cells(RUN_1))[0]["cells_carried"] == 0


def test_evidence_digest_is_comparable_between_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Отпечаток evidence не содержит ``run_id`` — иначе перенос был бы невозможен."""
    docs = [("hash_a", "Свойство Альфа поддерживается в версии 2.")]
    chunks = [_chunk("hash_a", 0, docs[0][1])]
    state = SqliteStateStore(":memory:")

    first_store = _store(RUN_1)
    _run_phase(
        run_id=RUN_1,
        store=first_store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=chunks,
        coverage=_coverage(),
        state=state,
        monkeypatch=monkeypatch,
    )
    second_store = _store(RUN_2)
    _run_phase(
        run_id=RUN_2,
        store=second_store,
        evaluator=_ScriptedEvaluator(_present(docs)),
        chunks=chunks,
        coverage=_coverage(),
        state=state,
        previous_store=first_store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )

    first_evidence = first_store.iter_cell_evidence(RUN_1, PRODUCT, "param_alpha")
    second_evidence = second_store.iter_cell_evidence(RUN_2, PRODUCT, "param_alpha")
    assert evidence_digest(first_evidence) == evidence_digest(second_evidence)
    assert first_evidence[0].evidence_id != second_evidence[0].evidence_id
