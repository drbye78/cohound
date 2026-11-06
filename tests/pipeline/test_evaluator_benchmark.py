"""Тесты benchmark моделей оценщика (A3, Ш6) — чистый контур, без сети и store.

Что доказывается:

* ``prepare_cases`` вызывает ретривал **один раз** на ячейку и замораживает срез;
  пропуски всегда названы (``no_chunks``/``catalog_missing``/…), настройки берутся из
  строки прогона;
* ``run_benchmark`` зовёт ``evaluate_cell(store=None)`` — измерение, а не прогон;
* метрики честные: validity, гистограмма ``failure_kind``, κ через существующий
  ``core.util.agreement`` (``None`` пропагируется с причиной), ``abstention``,
  ``fabricated_support`` (✅ с невалидируемыми цитатами — не успех), ``first_attempt_pass``,
  токены/вызовы, латентность, детерминизм по повторам, bootstrap-CI;
* вход одинаков для всех моделей (совпадение ``prompt_hash``/промптов), иначе — отказ.

Все модели — in-memory фейки за ``EvaluatorPort`` (I18: у порта нет инструментов).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from core.domain import (
    AbsenceRuleType,
    CatalogRow,
    DenialClaim,
    GoldenRow,
    Importance,
    LLMCellOutput,
    LLMEvidence,
    ParamType,
    SourceKind,
    Status,
)
from core.pipeline.evaluator_benchmark import (
    BENCHMARK_CONTRACT,
    PRECONDITION_GOLDEN_DECLINED,
    BenchmarkContractError,
    BenchmarkPreconditionError,
    CellCase,
    ModelUnderTest,
    prepare_cases,
    run_benchmark,
)
from core.pipeline.golden_status import GoldenStatus
from core.pipeline.retrieve import RetrievalResult
from core.ports.llm import LLMRequest, LLMResponse, TokenUsage
from core.ports.vector_index import RetrievedChunk

BUILD = "ib_bench"
RUN_ID = "run_bench"
PRODUCT = "p1"

TEXT_OK = "Функция Альфа доступна без дополнительной настройки."
TEXT_ABSENT = "Функция Альфа отсутствует в документации продукта."

# ---------------------------------------------------------------------------
# Фейковый оценщик (шов ``EvaluatorPort``, без сети)
# ---------------------------------------------------------------------------

class _FnEvaluator:
    """Оценщик-функция от промпта: тест играет модель, читая переданный промпт."""

    def __init__(
        self,
        fn,  # noqa: ANN001 — Callable[[str], LLMCellOutput | Exception]
        *,
        model_id: str = "fake",
        seed: int = 0,
        temperature: float = 0.0,
        usage: tuple[int, int, int] = (10, 5, 0),
        finish_reason: str | None = None,
    ) -> None:
        self._fn = fn
        self.model_id = model_id
        self.model_version = "1.0"
        self.decoder = "none"
        self.endpoint_kind = "api"
        self.seed = seed
        self.temperature = temperature
        self.usage = usage
        self.finish_reason = finish_reason
        self.prompts: list[str] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        self.prompts.append(req.prompt)
        result = self._fn(req.prompt)
        if isinstance(result, Exception):
            raise result
        return LLMResponse(
            output=result,
            raw=result.model_dump_json(),
            prompt_hash="ph",
            decoder="none",
            finish_reason=self.finish_reason,
            usage=TokenUsage(*self.usage),
        )

def _ok_output(quote: str = TEXT_OK) -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="Функция подтверждена документацией.",
        evidence=[LLMEvidence(source_id="c1", quote=quote)],
    )

def _absent_output() -> LLMCellOutput:
    """Идеальный ``❌``: класс объявлен и цитата приложена (иначе это ``❓``, не ``❌``).

    Вариант 2: модель, которая говорит «отсутствует» без прямого отрицания и цитаты,
    правильно получает ``❓`` — поэтому эталонная модель обязана объявить ``explicit``.
    """
    return LLMCellOutput(
        status=Status.ABSENT,
        justification="Документация прямо заявляет отсутствие функции.",
        evidence=[LLMEvidence(source_id="c1", quote=TEXT_ABSENT)],
        denial_strength=DenialClaim.EXPLICIT,
        denial_reason="прямое утверждение об отсутствии в документации",
    )

def _unknown_output() -> LLMCellOutput:
    return LLMCellOutput(status=Status.UNKNOWN, justification="Недостаточно данных.")

def _perfect(prompt: str) -> LLMCellOutput:
    """«Идеальная» модель: ✅ по маркеру ячейки, ❌ — где документация молчит."""
    if "CELL_ABSENT" in prompt:
        return _absent_output()
    return _ok_output()

# ---------------------------------------------------------------------------
# Данные ячеек
# ---------------------------------------------------------------------------

def _param(param_id: str, title: str) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=title,
        module="mod_a",
        group="grp_a",
        importance=Importance.BASIC,
        param_type=ParamType.FACT,
        rule="Подтвердить наличие функции.",
        absence_rule=AbsenceRuleType.EXPLICIT,
        role_binding="role_a",
        concept_terms={"ru": ["функция"], "en": ["feature"]},
        search_langs=["ru"],
    )

def _chunk(text: str, *, chunk_id: str = "c1") -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id=chunk_id,
        product_id=PRODUCT,
        text=text,
        content_hash=f"hash-{chunk_id}",
        heading_path="H",
        build_id=BUILD,
        score=1.0,
    )

def _case(
    param_id: str,
    title: str,
    expected: Status,
    *,
    text: str = TEXT_OK,
    product_id: str = PRODUCT,
    run_id: str = RUN_ID,
) -> CellCase:
    chunks = (_chunk(text),)
    retrieval = RetrievalResult(
        chunks=list(chunks), profile="R0", cutoff_K=10, build_id=BUILD
    )
    golden = GoldenRow(
        product_id=product_id,
        param_id=param_id,
        expected_status=expected,
        expected_evidence_kind=[SourceKind.DOCUMENTATION],
        case_class="fact",
    )
    return CellCase(
        param=_param(param_id, title),
        product_id=product_id,
        run_id=run_id,
        chunks=chunks,
        retrievals=retrieval,
        golden=golden,
        index_build_id=BUILD,
        rrf_k=60,
    )

def _two_cases() -> list[CellCase]:
    return [
        _case("param_ok", "CELL_OK", Status.OK),
        _case("param_absent", "CELL_ABSENT", Status.ABSENT, text=TEXT_ABSENT),
    ]

def _model(fn, *, model_id: str = "fake", seed: int = 0, temperature: float = 0.0):  # noqa: ANN001
    return ModelUnderTest(
        model_id=model_id,
        label=model_id,
        evaluator=_FnEvaluator(fn, model_id=model_id, seed=seed, temperature=temperature),
    )

def _fixed_clock(step: float = 0.25):
    """Детерминированные часы: каждый вызов — +``step`` секунд."""
    state = {"now": 0.0}

    def clock() -> float:
        state["now"] += step
        return state["now"]

    return clock

# ---------------------------------------------------------------------------
# Метрики: идеальная модель
# ---------------------------------------------------------------------------

def test_perfect_model_scores_are_honest() -> None:
    report = run_benchmark(_two_cases(), [_model(_perfect)], clock=_fixed_clock())
    model = report.models[0]
    assert model.validity_rate == pytest.approx(1.0)
    assert model.failure_kinds == {
        "validation_failed": 0,
        "evaluation_failed": 0,
        "none": 2,
        "harness_error": 0,
    }
    assert model.kappa == pytest.approx(1.0)
    assert model.abstention["golden_absent"] == 1
    assert model.abstention["answered_absent"] == 1
    assert model.abstention["absent_rate"] == pytest.approx(1.0)
    assert model.abstention["fabricated_support"] == 0
    assert model.first_attempt_pass == pytest.approx(1.0)
    assert model.tokens["prompt_total"] == 20
    assert model.tokens["completion_total"] == 10
    assert model.tokens["calls"] == 2
    assert model.tokens["calls_per_cell"] == pytest.approx(1.0)
    assert model.tokens["usage_reported_cells"] == 2
    # clock: два вызова evaluate_cell → по 0.25 с.
    assert model.latency_ms["mean"] == pytest.approx(250.0)
    assert model.determinism["measured"] is False
    payload = report.to_dict()
    assert payload["n_cells"] == 2
    assert payload["n_products"] == 1
    assert payload["contract"]["seed"] == 0
    assert payload["contract"]["max_retries"] == 2
    assert payload["contract"]["prompt_template"] == BENCHMARK_CONTRACT["prompt_template"]

def test_reasoning_tokens_and_finish_reason_are_measured() -> None:
    """Уровень усилия доказывается **расходом**, а не фактом приёма параметра.

    ``reasoning_tokens`` из ``usage`` агрегируются по ячейкам (среднее/медиана/макс), а
    ``finish_reason`` — по вызовам: без этого «уровень действует» осталось бы словами.
    """
    evaluator = _FnEvaluator(_perfect, usage=(10, 5, 12), finish_reason="stop")
    report = run_benchmark(
        _two_cases(),
        [ModelUnderTest(model_id="fake", label="fake", evaluator=evaluator)],
        clock=_fixed_clock(),
    )
    model = report.models[0]
    assert model.reasoning["mean"] == pytest.approx(12.0)
    assert model.reasoning["median"] == pytest.approx(12.0)
    assert model.reasoning["max"] == 12
    assert model.reasoning["cells_with_reasoning"] == 2
    assert model.finish_reason["counts"] == {"stop": 2}
    assert model.finish_reason["length_fraction"] == pytest.approx(0.0)
    assert model.invalid_json["calls"] == 0
    assert model.status_counts == {"✅": 1, "❌": 1}
    payload = report.to_dict()["models"][0]
    assert payload["cells"][0]["reasoning_tokens"] == 12
    assert payload["cells"][0]["finish_reasons"] == ["stop"]


def test_bootstrap_ci_brackets_point_estimate() -> None:
    report = run_benchmark(_two_cases(), [_model(_perfect)], clock=_fixed_clock())
    model = report.models[0]
    assert model.validity_ci.low is not None and model.validity_ci.high is not None
    assert model.validity_ci.low <= model.validity_rate <= model.validity_ci.high
    assert model.kappa_ci is not None
    assert model.kappa_ci.low is not None and model.kappa_ci.high is not None
    assert model.kappa_ci.low <= model.kappa <= model.kappa_ci.high

# ---------------------------------------------------------------------------
# Сдержанность и опасные режимы
# ---------------------------------------------------------------------------

def test_always_unknown_model_is_valid_but_disagrees() -> None:
    report = run_benchmark(_two_cases(), [_model(lambda _p: _unknown_output())])
    model = report.models[0]
    assert model.validity_rate == pytest.approx(1.0)
    assert {m.status for m in model.cells} == {Status.UNKNOWN.value}
    assert model.abstention["answered_absent"] == 0
    assert model.abstention["answered_unknown"] == 1
    assert model.abstention["absent_rate"] == pytest.approx(0.0)
    assert model.kappa == pytest.approx(0.0)

def test_invalid_json_is_evaluation_failed_with_retries() -> None:
    """Реальный путь разбора: невалидный JSON → ``evaluation_failed`` после повторов."""
    from core.adapters.llm_local import LocalEvaluator

    evaluator = LocalEvaluator(
        generate=lambda _prompt, _schema: "это не JSON",
        model_id="bad-json",
        base_url="http://bench.invalid",
    )
    report = run_benchmark(
        [_case("param_ok", "CELL_OK", Status.OK)],
        [ModelUnderTest(model_id="bad-json", evaluator=evaluator)],
    )
    model = report.models[0]
    assert model.validity_rate == pytest.approx(0.0)
    assert model.failure_kinds["evaluation_failed"] == 1
    assert model.tokens["calls"] == int(BENCHMARK_CONTRACT["max_retries"]) + 1
    assert model.first_attempt_pass == pytest.approx(0.0)
    assert model.kappa is None
    assert model.kappa_undefined_reason  # причина названа, число не выдумано
    # usage не сообщён — отчёт говорит это прямо, а не выдаёт 0 за измерение.
    assert model.tokens["usage_reported_cells"] == 0
    # Невалидный JSON назван отдельной статьёй: это путь отказа, а не «пустой ответ».
    assert model.invalid_json["calls"] == 3
    assert model.invalid_json["fraction"] == pytest.approx(1.0)
    assert any("usage" in limit for limit in report.limits)

def test_fabricated_support_is_counted_not_rewarded() -> None:
    """✅ с цитатой, которой нет в чанке: не успех, а ``fabricated_support``."""
    report = run_benchmark(
        [_case("param_ok", "CELL_OK", Status.OK)],
        [_model(lambda _p: _ok_output("Такой фразы в документе нет."))],
    )
    model = report.models[0]
    assert model.validity_rate == pytest.approx(0.0)
    assert model.failure_kinds["validation_failed"] == 1
    assert model.abstention["golden_ok"] == 1
    assert model.abstention["fabricated_support"] == 1
    assert model.abstention["fabricated_support_rate"] == pytest.approx(1.0)
    assert model.cells[0].status is None  # failed не несёт статус (I4)

def test_non_deterministic_model_is_measured() -> None:
    """repeats=2: статус меняется между повторами → determinism < 1 (первый класс)."""
    calls = {"n": 0}

    def flaky(_prompt: str) -> LLMCellOutput:
        calls["n"] += 1
        return _ok_output() if calls["n"] % 2 else _unknown_output()

    report = run_benchmark(
        [_case("param_ok", "CELL_OK", Status.OK)],
        [_model(flaky)],
        repeats=2,
    )
    determinism = report.models[0].determinism
    assert determinism["measured"] is True
    assert determinism["changed_cells"] == 1
    assert determinism["fraction_changed"] == pytest.approx(1.0)
    assert determinism["stable_fraction"] == pytest.approx(0.0)
    # первичные метрики — по первому повтору (не смешиваем n).
    assert report.models[0].n_cells == 1

# ---------------------------------------------------------------------------
# Вырожденное согласие и контракт
# ---------------------------------------------------------------------------

def test_degenerate_golden_propagates_kappa_none() -> None:
    cases = [
        _case("param_ok", "CELL_OK", Status.OK),
        _case("param_ok2", "CELL_OK_2", Status.OK),
    ]
    report = run_benchmark(cases, [_model(_perfect)])
    model = report.models[0]
    assert model.kappa is None
    assert model.kappa_undefined_reason
    assert model.kappa_ci is None
    assert report.to_dict()["models"][0]["agreement"]["kappa"] is None

def test_input_is_identical_for_all_models() -> None:
    models = [_model(_perfect, model_id="m1"), _model(_perfect, model_id="m2")]
    report = run_benchmark(_two_cases(), models)
    prompts = [model.evaluator.prompts for model in models]  # type: ignore[attr-defined]
    assert prompts[0] == prompts[1]
    assert report.input_digest
    assert len(report.input_digest) == 64

def test_store_is_none_no_db_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Измерение не персистится: ``evaluate_cell`` вызывается с ``store=None``."""
    import core.pipeline.evaluator_benchmark as module

    seen: list[Any] = []
    real = module.evaluate_cell

    def spy(**kwargs):  # noqa: ANN003
        seen.append(kwargs.get("store"))
        return real(**kwargs)

    monkeypatch.setattr(module, "evaluate_cell", spy)
    run_benchmark(_two_cases(), [_model(_perfect)])
    assert seen
    assert all(store is None for store in seen)

def test_contract_rejects_web_tool_and_nonzero_temperature() -> None:
    with pytest.raises(BenchmarkContractError):
        ModelUnderTest(model_id="m", evaluator=_FnEvaluator(_perfect), web_tool=True)
    with pytest.raises(BenchmarkContractError):
        run_benchmark(
            [_case("param_ok", "CELL_OK", Status.OK)],
            [_model(_perfect, temperature=0.7)],
        )
    with pytest.raises(ValueError):
        run_benchmark([], [_model(_perfect)])

def test_failure_kind_histogram_and_harness_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Сбой харнесса изолирован и назван, а не выдан за статус модели."""
    import core.pipeline.evaluator_benchmark as module

    def boom(**kwargs):  # noqa: ANN003
        raise RuntimeError("harness exploded")

    monkeypatch.setattr(module, "evaluate_cell", boom)
    report = run_benchmark([_case("param_ok", "CELL_OK", Status.OK)], [_model(_perfect)])
    model = report.models[0]
    assert model.failure_kinds["harness_error"] == 1
    assert model.cells[0].harness_error
    assert model.validity_rate == pytest.approx(0.0)

# ---------------------------------------------------------------------------
# prepare_cases: ретривал один раз, пропуски названы, настройки из прогона
# ---------------------------------------------------------------------------

class _FakeLexicalIndex:
    """BM25-подобный фейк: возвращает заранее заданные чанки продукта."""

    engine_version = "fake-bm25"

    def __init__(self, by_product: dict[str, list[RetrievedChunk]], build_id: str = BUILD) -> None:
        self.by_product = by_product
        self.build_id = build_id
        self.queries: list[str] = []

    def search(
        self,
        query: str,
        product_ids: list[str],
        top_k: int,
        lang: str | None = None,  # noqa: ARG002
    ) -> list[RetrievedChunk]:
        self.queries.append(query)
        found: list[RetrievedChunk] = []
        for product_id in product_ids:
            found.extend(self.by_product.get(product_id, []))
        return found[:top_k]

class _FakeStore:
    """Read-only стор прогона: манифест, сборка, продукты, чанки."""

    def __init__(
        self,
        *,
        products: dict[str, list[Any]],
        profile: str | None = "R0",
        cutoff: int | None = 10,
        params_json: str | None = '{"rrf_k": 60}',
        build_id: str = BUILD,
    ) -> None:
        self.products = products
        self.build_id = build_id
        self.manifest = SimpleNamespace(
            retrieval_profile=profile,
            retrieval_cutoff_K=cutoff,
            retrieval_params_json=params_json,
            index_build_id=build_id,
        )

    def read_manifest(self, run_id: str) -> Any:  # noqa: ARG002
        return self.manifest

    def read_run_index_ref(self, run_id: str) -> Any:  # noqa: ARG002
        return SimpleNamespace(index_build_id=self.build_id)

    def iter_index_manifest(self, index_build_id: str):  # noqa: ANN201, ARG002
        for product_id in self.products:
            yield SimpleNamespace(product_id=product_id, package_id="pkg")

    def iter_chunks(self, build_id: str, *, product_id: str | None = None):  # noqa: ANN201, ARG002
        yield from self.products.get(product_id or "", [])

def _golden(param_id: str, expected: Status, product_id: str = PRODUCT) -> GoldenRow:
    return GoldenRow(
        product_id=product_id,
        param_id=param_id,
        expected_status=expected,
        expected_evidence_kind=[SourceKind.DOCUMENTATION],
        case_class="fact",
    )

def test_prepare_cases_freezes_retrieval_and_reads_run_settings() -> None:
    chunk = _chunk(TEXT_OK)
    store = _FakeStore(products={PRODUCT: [chunk]})
    lexical = _FakeLexicalIndex({PRODUCT: [chunk]})
    prepared = prepare_cases(
        store=store,
        run_id=RUN_ID,
        golden=[_golden("param_ok", Status.OK)],
        catalog={"param_ok": _param("param_ok", "CELL_OK")},
        lexical_index=lexical,
    )
    assert len(prepared.cases) == 1
    case = prepared.cases[0]
    assert case.chunks == (chunk,)
    assert case.retrievals.build_id == BUILD
    assert prepared.retrieval_profile == "R0"
    assert prepared.retrieval_cutoff_k == 10
    assert prepared.rrf_k == 60
    assert prepared.skipped == {}
    # ретривал выполнен до моделей и только здесь; запросы — из каталога (S13).
    assert lexical.queries

def test_prepare_cases_names_skips_and_respects_limit() -> None:
    store = _FakeStore(products={PRODUCT: [_chunk(TEXT_OK)], "p2": []})
    lexical = _FakeLexicalIndex({PRODUCT: [_chunk(TEXT_OK)]})
    golden = [
        _golden("param_ok", Status.OK),
        _golden("param_missing", Status.OK),
        _golden("param_ok", Status.OK, product_id="p2"),
        _golden("param_ok", Status.OK, product_id="p9"),
    ]
    prepared = prepare_cases(
        store=store,
        run_id=RUN_ID,
        golden=golden,
        catalog={"param_ok": _param("param_ok", "CELL_OK")},
        lexical_index=lexical,
    )
    assert len(prepared.cases) == 1
    assert prepared.skipped == {
        f"{PRODUCT}/param_missing": "catalog_missing",
        "p2/param_ok": "no_chunks",
        "p9/param_ok": "product_not_in_build",
    }

    limited = prepare_cases(
        store=store,
        run_id=RUN_ID,
        golden=[_golden("param_ok", Status.OK)],
        catalog={"param_ok": _param("param_ok", "CELL_OK")},
        lexical_index=lexical,
        limit=1,
    )
    assert len(limited.cases) == 1

def test_prepare_cases_requires_preconditions() -> None:
    store = _FakeStore(products={PRODUCT: [_chunk(TEXT_OK)]})
    with pytest.raises(BenchmarkPreconditionError) as exc:
        prepare_cases(
            store=store,
            run_id=RUN_ID,
            golden=[_golden("param_ok", Status.OK)],
            catalog={"param_ok": _param("param_ok", "CELL_OK")},
            lexical_index=_FakeLexicalIndex({}),
            product_ids=[],
        )
    assert exc.value.reason == "no_products"

    with pytest.raises(BenchmarkPreconditionError) as exc2:
        prepare_cases(
            store=_FakeStore(products={PRODUCT: []}),
            run_id=RUN_ID,
            golden=[_golden("param_ok", Status.OK)],
            catalog={"param_ok": _param("param_ok", "CELL_OK")},
            lexical_index=_FakeLexicalIndex({}),
        )
    assert exc2.value.reason == "no_cases"
    assert "no_chunks" in str(exc2.value)

def test_prepare_cases_notes_unfixed_settings() -> None:
    chunk = _chunk(TEXT_OK)
    store = _FakeStore(
        products={PRODUCT: [chunk]}, profile=None, cutoff=None, params_json=None
    )
    prepared = prepare_cases(
        store=store,
        run_id=RUN_ID,
        golden=[_golden("param_ok", Status.OK)],
        catalog={"param_ok": _param("param_ok", "CELL_OK")},
        lexical_index=_FakeLexicalIndex({PRODUCT: [chunk]}),
    )
    assert prepared.retrieval_profile == "R0"
    assert prepared.retrieval_cutoff_k > 0
    assert prepared.rrf_k == 60
    assert any("не зафиксирован" in note for note in prepared.notes)

def test_report_limits_are_computed() -> None:
    report = run_benchmark(_two_cases(), [_model(_perfect)])
    text = " ".join(report.limits)
    assert "2 ячеек" in text
    assert "1 продукт" in text or "продукт" in text
    assert "детерминизм не измерен" in text
    assert "store=None" in text
    assert "failed" in text  # κ не вбирает failed-ячейки молча


# ---------------------------------------------------------------------------
# P6b: объявленный отказ эталона — главная причина, а не строка разложения
# ---------------------------------------------------------------------------


def _declined(reason: str = "размечен не на те продукты") -> GoldenStatus:
    return GoldenStatus(
        state="declined",
        declared=True,
        file_present=True,
        reason=reason,
        declined_by="analyst-name",
        declined_at="2026-10-27",
    )


def test_declined_golden_is_named_before_row_reasons() -> None:
    """``declined`` — причина отказа **эталона**, а ``product_not_in_build`` — разложение.

    До правки единственный инструмент, обязанный мерить точность оценщика, отвечал
    ``no_cases``: объявленный человеком отказ эталона растворялся в причинах уровня строк
    и не доходил до того, кто выбирает модель (A3).
    """
    chunk = _chunk(TEXT_OK)
    store = _FakeStore(products={PRODUCT: [chunk]})
    lexical = _FakeLexicalIndex({PRODUCT: [chunk]})
    golden = [
        _golden("param_ok", Status.OK),
        _golden("param_ok", Status.OK, product_id="p9"),
    ]
    with pytest.raises(BenchmarkPreconditionError) as exc:
        prepare_cases(
            store=store,
            run_id=RUN_ID,
            golden=golden,
            catalog={"param_ok": _param("param_ok", "CELL_OK")},
            lexical_index=lexical,
            golden_status=_declined(),
        )
    error = exc.value
    assert error.reason == PRECONDITION_GOLDEN_DECLINED
    message = str(error)
    # Автор, дата и причина — из объявления (не выдуманы и не потеряны).
    assert "analyst-name" in message
    assert "2026-10-27" in message
    assert "размечен не на те продукты" in message
    # Разложение по строкам сохранено — как второй уровень причины.
    assert error.details["row_skip_reasons"] == {
        "golden_declined": 1,
        "product_not_in_build": 1,
    }
    assert error.details["golden"]["declined_by"] == "analyst-name"
    # Ни одного ретривала: отозванный эталон не измеряется «понарошку».
    assert lexical.queries == []


def test_declined_golden_wins_even_when_rows_would_be_usable() -> None:
    """Отказ эталона не зависит от того, сошлись ли строки со сборкой."""
    chunk = _chunk(TEXT_OK)
    with pytest.raises(BenchmarkPreconditionError) as exc:
        prepare_cases(
            store=_FakeStore(products={PRODUCT: [chunk]}),
            run_id=RUN_ID,
            golden=[_golden("param_ok", Status.OK)],
            catalog={"param_ok": _param("param_ok", "CELL_OK")},
            lexical_index=_FakeLexicalIndex({PRODUCT: [chunk]}),
            golden_status=_declined(),
        )
    assert exc.value.reason == PRECONDITION_GOLDEN_DECLINED
    assert exc.value.details["row_skip_reasons"] == {"golden_declined": 1}


def test_present_golden_status_keeps_previous_behaviour() -> None:
    """``state: present`` — прежнее поведение: ячейки готовятся, причин отказа нет."""
    chunk = _chunk(TEXT_OK)
    lexical = _FakeLexicalIndex({PRODUCT: [chunk]})
    prepared = prepare_cases(
        store=_FakeStore(products={PRODUCT: [chunk]}),
        run_id=RUN_ID,
        golden=[_golden("param_ok", Status.OK)],
        catalog={"param_ok": _param("param_ok", "CELL_OK")},
        lexical_index=lexical,
        golden_status=GoldenStatus(
            state="present", declared=True, file_present=True
        ),
    )
    assert len(prepared.cases) == 1
    assert prepared.skipped == {}
    assert lexical.queries


def test_absent_golden_status_keeps_previous_behaviour() -> None:
    """Объявления нет (``golden_status=None``) — прежние причины уровня строк."""
    with pytest.raises(BenchmarkPreconditionError) as exc:
        prepare_cases(
            store=_FakeStore(products={PRODUCT: []}),
            run_id=RUN_ID,
            golden=[_golden("param_ok", Status.OK)],
            catalog={"param_ok": _param("param_ok", "CELL_OK")},
            lexical_index=_FakeLexicalIndex({}),
        )
    assert exc.value.reason == "no_cases"
    # Разложение по строкам теперь публикуется как ``details`` (второй уровень причины),
    # а не теряется: причина уровня набора — в ``message``.
    assert exc.value.details["row_skip_reasons"] == {"no_chunks": 1}
    assert exc.value.details["products_not_in_build"] == []
    assert "no_chunks" in str(exc.value)


# ---------------------------------------------------------------------------
# Два id-пространства: композиция прогона (cb_) vs продукт-партиция (pb_)
# ---------------------------------------------------------------------------

_COMPOSITION = "cb_bench_comp"
_PARTITION = "pb_bench_part"


class _PartitionStore:
    """Стор в **измеренной** форме: ``runs.index_build_id`` = ``cb_*``, чанки — под ``pb_*``.

    Ключевое свойство (и сторож регрессии): чанки читаются **только** по тому
    ``build_id``, который передан, поэтому запрос композицией возвращает пусто — ровно
    так ведёт себя ``SqliteStore.iter_chunks`` (``WHERE corpus_chunks.build_id = ?``).
    """

    def __init__(
        self,
        *,
        composition: str = _COMPOSITION,
        partitions: dict[str, str] | None = None,
        chunks: dict[tuple[str, str], list[Any]] | None = None,
    ) -> None:
        self.composition = composition
        #: ``product_id -> partition_build_id`` (как ``index_manifest``).
        self.partitions = partitions if partitions is not None else {PRODUCT: _PARTITION}
        self.chunks = chunks if chunks is not None else {}
        self.probed_build_ids: list[str] = []
        self.manifest_queries: list[str] = []

    def read_manifest(self, run_id: str) -> Any:  # noqa: ARG002
        return SimpleNamespace(
            retrieval_profile="R0",
            retrieval_cutoff_K=10,
            retrieval_params_json='{"rrf_k": 60}',
            index_build_id=self.composition,
        )

    def read_run_index_ref(self, run_id: str) -> Any:  # noqa: ARG002
        return SimpleNamespace(index_build_id=self.composition)

    def iter_index_manifest(self, index_build_id: str):  # noqa: ANN201
        self.manifest_queries.append(index_build_id)
        for product_id, partition in self.partitions.items():
            yield SimpleNamespace(
                product_id=product_id,
                package_id="pkg",
                index_build_id=self.composition,
                partition_build_id=partition,
            )

    def iter_chunks(self, build_id: str, *, product_id: str | None = None):  # noqa: ANN201
        self.probed_build_ids.append(build_id)
        yield from self.chunks.get((build_id, product_id or ""), [])


def test_prepare_cases_probes_partition_not_composition() -> None:
    """Регрессия измеренного дефекта: проба чанков идёт по ``pb_*``, а не по ``cb_*``.

    До правки ``_product_has_chunks`` получал композицию прогона, а
    ``corpus_chunks.build_id`` несёт партицию ⇒ все продукты выглядели пустыми и
    benchmark отвечал ``no_cases`` при живом корпусе (измерено 06.11.2026 на
    ``ru-pg-dbms-c2e7b860ac17``).
    """
    chunk = RetrievedChunk(
        chunk_id="c1",
        product_id=PRODUCT,
        text=TEXT_OK,
        content_hash="hash-c1",
        heading_path="H",
        build_id=_PARTITION,
        score=1.0,
    )
    store = _PartitionStore(chunks={(_PARTITION, PRODUCT): [chunk]})
    prepared = prepare_cases(
        store=store,
        run_id=RUN_ID,
        golden=[_golden("param_ok", Status.OK)],
        catalog={"param_ok": _param("param_ok", "CELL_OK")},
        lexical_index=_FakeLexicalIndex({PRODUCT: [chunk]}, build_id=_COMPOSITION),
    )
    assert len(prepared.cases) == 1
    assert prepared.skipped == {}
    # Проба — по партиции; композиция в ``corpus_chunks`` не встречается по построению.
    assert store.probed_build_ids == [_PARTITION]
    assert store.manifest_queries == [_COMPOSITION]
    # Провенанс ячейки остаётся композицией (I17/S5): партиция — уровень чанка.
    assert prepared.index_build_id == _COMPOSITION
    assert prepared.cases[0].index_build_id == _COMPOSITION
    assert prepared.cases[0].retrievals.build_id == _COMPOSITION


def test_product_without_manifest_row_is_product_not_in_build() -> None:
    """Продукт без строки в ``index_manifest`` — честный отказ, а не «попробуем cb_»."""
    chunk = RetrievedChunk(
        chunk_id="c1",
        product_id=PRODUCT,
        text=TEXT_OK,
        content_hash="hash-c1",
        heading_path="H",
        build_id=_PARTITION,
        score=1.0,
    )
    store = _PartitionStore(chunks={(_PARTITION, PRODUCT): [chunk]})
    with pytest.raises(BenchmarkPreconditionError) as exc:
        prepare_cases(
            store=store,
            run_id=RUN_ID,
            golden=[_golden("param_ok", Status.OK, product_id="p_absent")],
            catalog={"param_ok": _param("param_ok", "CELL_OK")},
            # Явный список продуктов не создаёт партицию: её даёт только манифест.
            product_ids=[PRODUCT, "p_absent"],
            lexical_index=_FakeLexicalIndex({PRODUCT: [chunk]}, build_id=_COMPOSITION),
        )
    assert exc.value.reason == "no_cases"
    assert exc.value.details["row_skip_reasons"] == {"product_not_in_build": 1}
    assert exc.value.details["products_not_in_build"] == ["p_absent"]
    assert "p_absent" in str(exc.value)
    # Ни одной пробы чанков у отсутствующего продукта: сверять не с чем.
    assert store.probed_build_ids == []
    assert exc.value.details["row_skips"] == {"p_absent/param_ok": "product_not_in_build"}


def test_no_cases_message_names_products_absent_from_build() -> None:
    """Верхняя причина называет **какие** продукты golden отсутствуют в сборке.

    ``details`` читает только ``--json``; тот, кто выбирает модель, видит ``message``.
    Общий ``no_cases`` отправлял искать поломку в чанках там, где продукта в сборке нет
    вовсе.
    """
    store = _PartitionStore()
    with pytest.raises(BenchmarkPreconditionError) as exc:
        prepare_cases(
            store=store,
            run_id=RUN_ID,
            golden=[
                _golden("param_ok", Status.OK, product_id="p8"),
                _golden("param_ok", Status.OK, product_id="p9"),
            ],
            catalog={"param_ok": _param("param_ok", "CELL_OK")},
            lexical_index=_FakeLexicalIndex({}),
        )
    error = exc.value
    message = str(error)
    assert error.reason == "no_cases"
    assert "ни один продукт golden не входит в сборку" in message
    assert _COMPOSITION in message
    assert "p8" in message
    assert "p9" in message
    assert error.details["products_not_in_build"] == ["p8", "p9"]
    assert error.details["row_skip_reasons"] == {"product_not_in_build": 2}
    assert error.details["build_id"] == _COMPOSITION


def test_build_mismatch_compares_with_composition_id() -> None:
    """I17 не потерян: результат, назвавший партицию вместо сборки, отвергается.

    ``RetrievalResult.build_id`` — провенанс **сборки** (композиция); порт, отдавший
    ``pb_*``, пришёл из другого пространства id, и ячейка не должна попасть в замер.
    """
    chunk = RetrievedChunk(
        chunk_id="c1",
        product_id=PRODUCT,
        text=TEXT_OK,
        content_hash="hash-c1",
        heading_path="H",
        build_id=_PARTITION,
        score=1.0,
    )
    store = _PartitionStore(chunks={(_PARTITION, PRODUCT): [chunk]})
    with pytest.raises(BenchmarkPreconditionError) as exc:
        prepare_cases(
            store=store,
            run_id=RUN_ID,
            golden=[_golden("param_ok", Status.OK)],
            catalog={"param_ok": _param("param_ok", "CELL_OK")},
            # Порт назвал партицию — это не сборка прогона (I17).
            lexical_index=_FakeLexicalIndex({PRODUCT: [chunk]}, build_id=_PARTITION),
        )
    assert exc.value.reason == "no_cases"
    assert exc.value.details["row_skip_reasons"] == {"build_mismatch": 1}
    assert _PARTITION in str(exc.value) and _COMPOSITION in str(exc.value)
