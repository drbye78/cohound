"""I2d: у ``❌`` есть **датированный носитель**, и дата публикуется (план 20 §11.2).

Правило заменяет решение 29.10.2026 «``❌`` требует ``live|revalidated`` в этом цикле»
(``_enforce_reuse_confirmed``, токен ``reuse_unconfirmed``). Смысл замены: ``❌`` — не
утверждение о сегодняшнем дне, а утверждение о **событии с датой**. Носитель:

* ``explicit``/``authoritative`` — запись наблюдения каждого цитируемого документа
  (``corpus_artifact.observed_at``); дата не обязана быть в этом цикле;
* ``bounded`` — снятие каждого объявленного корня с ``complete=1`` и датой;
  ``adopted``-снятие исчерпание не удостоверяет (предоставление — не обход);
* ``unobserved``/``not_stated``/``instrument_failed`` — правило не применяется.

Нарушение даёт ``❓`` (не ``failed``: I4) с токеном ``absence_undated`` в
``evidence_scope.reason`` **и** ``cells.validation_notes``, без протокола отсутствия
(``absence_evidence=[]``) и без даты (``cells.absence_as_of IS NULL``).
``absence_as_of`` = ``min`` дат носителей — «самое слабое звено».

Здесь же проверяется перенос: перенесённый ``❌`` обязан пройти тот же инвариант, иначе
перенос стал бы его обходом; гейт переноса читает ``coverage.complete``, который теперь
считается по снятиям корней.

Офлайн: SQLite в памяти, оценщик — скрипт (I18).
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest

from core.domain import (
    ABSENCE_UNDATED_REASON,
    AbsenceRuleType,
    CatalogRow,
    Cell,
    CellState,
    CorpusCoverage,
    CorpusDoc,
    DenialStrength,
    EvidenceScope,
    Focus,
    LLMCellOutput,
    RunConfig,
    RunMode,
    Status,
)
from core.domain.models import CorpusRootCoverage
from core.pipeline.carry_forward import cell_reuse_conditions
from core.pipeline.corpus_coverage import derive_corpus_coverage
from core.pipeline.corpus_index import _chunk_id
from core.pipeline.eval_cache import EvalConditions
from core.pipeline.evaluate import (
    ABSENCE_COVERAGE_UNSATISFIED_REASON,
    CorpusArtifactDates,
    _carried_rules_hold,
    _cited_observations,
    _enforce_dated_absence,
    evaluate_cell,
    observe_corpus_artifact_dates,
)
from core.pipeline.incremental import doc_version_id
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.store import CorpusArtifactRow
from core.ports.types import Chunk
from core.store.sqlite_store import SqliteStore

FIXTURES = Path(__file__).parent / "fixtures"
PRODUCT = "p1"
RUN_ID = "run_dated_1"
PREV_RUN = "run_dated_0"
ROOT = "https://docs.test/p1/"
TS = "2026-01-01T00:00:00+00:00"
D1 = "2026-01-02T00:00:00+00:00"
D2 = "2026-01-03T00:00:00+00:00"
D3 = "2026-01-04T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Общие помощники
# ---------------------------------------------------------------------------


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
    """``source_id`` в форме ``chunk_id`` прогона: второй сегмент — версия документа."""
    return _chunk_id(PRODUCT, _dv(doc_hash), chunk_no)


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


def _absent_uncited() -> LLMCellOutput:
    """``❌`` без цитаты: класс выбирает правило каталога (``bounded`` при подписи)."""
    return LLMCellOutput(
        status=Status.ABSENT,
        justification="Свойство не найдено в переданном срезе.",
        evidence=[],
    )


def _store(run_id: str = RUN_ID) -> SqliteStore:
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
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )
    return store


def _register_version(store: SqliteStore, doc_hash: str) -> None:
    """Строка корпуса + отображение ``doc_version_id → doc_id`` (вторая форма адреса)."""
    store.upsert_corpus_doc(
        CorpusDoc(
            doc_id=f"{PRODUCT}:{doc_hash}",
            product_id=PRODUCT,
            url=f"https://p1.test/{doc_hash}",
        ),
        doc_version_id=_dv(doc_hash),
    )


def _artifact(
    doc_hash: str,
    observed_at: str | None,
    *,
    doc_version_id: str | None = None,
) -> CorpusArtifactRow:
    """Строка ``corpus_artifact``: версия документа и дата наблюдения (``None`` — нет)."""
    return CorpusArtifactRow(
        run_id=RUN_ID,
        product_id=PRODUCT,
        url_key=f"https://p1.test/{doc_hash}",
        doc_version_id=_dv(doc_hash) if doc_version_id is None else doc_version_id,
        observed_at=observed_at,
        observed_how="live" if observed_at else None,
    )


def _coverage(
    *,
    complete: bool = True,
    roots: list[CorpusRootCoverage] | None = None,
) -> CorpusCoverage:
    """Покрытие продукта: полнота и корни задаются явно, счётчики — диагностика.

    ``frozen``-подобный случай (полнота по снятиям) не имеет элементов сеанса, поэтому
    при переданных корнях счётчики нулевые: ``complete=True`` опирается на ``roots``, а
    не на ``expected_sources > 0``.
    """
    if roots:
        expected = 0 if complete else 3
        observed = 0 if complete else 3
    else:
        expected = 3
        observed = 3 if complete else 2
    return CorpusCoverage(
        product_id=PRODUCT,
        complete=complete,
        roots=roots or [],
        expected_sources=expected,
        observed_sources=observed,
        computed_at=TS,
    )


def _root(
    *,
    complete: bool = True,
    observed_how: str = "live",
    observed_at: str | None = D1,
    root_url: str = ROOT,
) -> CorpusRootCoverage:
    return CorpusRootCoverage(
        root_url=root_url,
        capture_id="cap-1",
        complete=complete,
        observed_how=observed_how,
        observed_at=observed_at,
    )


class _ScriptedEvaluator:
    """Один ответ оценщика (без сети, I18)."""

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


def _evaluate(
    *,
    docs: list[tuple[str, str]],
    store: SqliteStore,
    coverage: CorpusCoverage | None = None,
    param: CatalogRow | None = None,
    output: LLMCellOutput | None = None,
    scope_acceptance: str | None = None,
):
    """Прогоняет ячейку через Ш6 с датами из ``corpus_artifact`` этого прогона."""
    chunks = [_chunk(doc_hash, 0, text) for doc_hash, text in docs]
    return evaluate_cell(
        param=param or _param(),
        product_id=PRODUCT,
        run_id=RUN_ID,
        evaluator=_ScriptedEvaluator(output or _absent(docs)),
        chunks=chunks,
        coverage=coverage if coverage is not None else _coverage(),
        scope_acceptance=scope_acceptance,
        artifact_dates=observe_corpus_artifact_dates(store, RUN_ID, PRODUCT),
        store=store,
    )


# ---------------------------------------------------------------------------
# explicit: носитель — запись наблюдения каждого цитируемого документа
# ---------------------------------------------------------------------------

DOC_A = "Свойство Альфа не поддерживается в этой версии продукта."
DOC_B = "В версии 2 свойство Альфа удалено."


def test_explicit_with_dated_document_publishes_absent_and_date() -> None:
    """Документ с датой наблюдения держит ``❌``; дата публикуется в ячейке и в БД."""
    store = _store()
    _register_version(store, "hash_a")
    store.replace_corpus_artifacts(RUN_ID, PRODUCT, [_artifact("hash_a", D1)])

    outcome = _evaluate(docs=[("hash_a", DOC_A)], store=store)

    cell = outcome.cell
    assert cell.status is Status.ABSENT
    assert cell.absence_as_of == D1
    assert cell.absence_evidence != []
    # Дата переживает прогон: отчёт читает ``cells.absence_as_of``, а не память процесса.
    saved = store.get_cell(RUN_ID, PRODUCT, "param_alpha")
    assert saved is not None and saved.absence_as_of == D1
    assert not any(
        ABSENCE_UNDATED_REASON in note for note in cell.validation_notes
    )


def test_explicit_without_recorded_date_demotes_to_unknown() -> None:
    """Строка артефакта есть, ``observed_at=None`` — даты носителя нет ⇒ ``❓``."""
    store = _store()
    _register_version(store, "hash_a")
    store.replace_corpus_artifacts(RUN_ID, PRODUCT, [_artifact("hash_a", None)])

    outcome = _evaluate(docs=[("hash_a", DOC_A)], store=store)

    cell = outcome.cell
    assert cell.status is Status.UNKNOWN
    assert outcome.failure_kind is None  # I4: процедурное условие, а не отказ прибора
    scope = cell.evidence_scope
    assert scope is not None and scope.denial_strength is DenialStrength.NOT_STATED
    assert (scope.reason or "").startswith(ABSENCE_UNDATED_REASON)
    assert "нет даты наблюдения документа" in (scope.reason or "")
    assert any(
        note.startswith(ABSENCE_UNDATED_REASON) and _dv("hash_a") in note
        for note in cell.validation_notes
    )
    # Протокол отсутствия не публикуется: «искали и не нашли» здесь не доказано.
    assert cell.absence_evidence == []
    assert cell.absence_as_of is None
    saved = store.get_cell(RUN_ID, PRODUCT, "param_alpha")
    assert saved is not None and saved.absence_as_of is None


def test_explicit_without_artifact_row_demotes_and_names_the_forms_searched() -> None:
    """Строки артефакта нет — причина называет обе формы адреса, которые искали."""
    store = _store()  # таблица есть, строк нет
    outcome = _evaluate(docs=[("hash_a", DOC_A)], store=store)

    cell = outcome.cell
    assert cell.status is Status.UNKNOWN
    reason = cell.evidence_scope.reason or ""
    assert reason.startswith(ABSENCE_UNDATED_REASON)
    assert "отсутствует в corpus_artifact" in reason
    assert f"doc_version_id={_dv('hash_a')}" in reason
    assert cell.absence_evidence == []
    assert cell.absence_as_of is None


def test_absence_as_of_is_the_earliest_carrier() -> None:
    """Два цитируемых документа разных дат ⇒ ``absence_as_of`` = более ранняя."""
    store = _store()
    for doc_hash in ("hash_a", "hash_b"):
        _register_version(store, doc_hash)
    store.replace_corpus_artifacts(
        RUN_ID,
        PRODUCT,
        [_artifact("hash_a", D3), _artifact("hash_b", D1)],
    )

    outcome = _evaluate(
        docs=[("hash_a", DOC_A), ("hash_b", DOC_B)], store=store
    )

    assert outcome.cell.status is Status.ABSENT
    # ``min``, а не ``max``: утверждение верно до самой ранней опоры.
    assert outcome.cell.absence_as_of == D1


def test_explicit_cited_observation_resolves_both_address_forms() -> None:
    """Адрес версии — основная форма, ``doc_id`` — вторая; обе дают дату."""
    store = _store()
    _register_version(store, "hash_a")
    store.replace_corpus_artifacts(RUN_ID, PRODUCT, [_artifact("hash_a", D1)])
    dates = observe_corpus_artifact_dates(store, RUN_ID, PRODUCT)
    assert dates is not None
    assert dates.by_doc_version == {_dv("hash_a"): D1}
    assert dates.by_doc == {f"{PRODUCT}:hash_a": D1}

    version_form, missing = _cited_observations(
        dates, [{"source_id": _source_id("hash_a")}]
    )
    assert version_form == [(f"{PRODUCT}:{_dv('hash_a')}", D1)]
    assert missing == []

    # Legacy-адрес (``content_hash`` во втором сегменте) — вторая форма индекса.
    legacy_form, missing = _cited_observations(
        dates, [{"source_id": f"{PRODUCT}:hash_a:0"}]
    )
    assert legacy_form == [(f"{PRODUCT}:hash_a", D1)]
    assert missing == []


# ---------------------------------------------------------------------------
# bounded: носитель — снятие каждого объявленного корня с complete=1
# ---------------------------------------------------------------------------

BOUNDED_QUOTE = "Свойство Альфа отсутствует в справочнике."


def test_bounded_with_complete_captures_publishes_absent_and_min_date() -> None:
    """Корни с ``complete=1`` и датами держат ``❌``; дата — ``min`` по корням."""
    store = _store()
    coverage = _coverage(
        complete=True,
        roots=[
            _root(observed_at=D3, root_url=ROOT),
            _root(observed_at=D2, root_url="https://docs.test/p1/guide/"),
        ],
    )
    outcome = _evaluate(
        docs=[("hash_a", BOUNDED_QUOTE)],
        output=_absent_uncited(),
        param=_param(AbsenceRuleType.DISTRIBUTION_ABSENCE),
        coverage=coverage,
        scope_acceptance="analyst",
        store=store,
    )

    cell = outcome.cell
    assert cell.status is Status.ABSENT
    assert cell.evidence_scope is not None
    assert cell.evidence_scope.denial_strength is DenialStrength.BOUNDED
    assert cell.absence_as_of == D2


def test_bounded_with_root_without_capture_stays_unknown_despite_acceptance() -> None:
    """``adopted``-снятие (``complete=False``) не удостоверяет исчерпание — даже с подписью.

    Это ужесточение §11.4 п.2: ``scope_acceptance`` закрывает I2c (риск принят), но не
    заменяет датированный носитель — у ``bounded`` без ``complete``-снятия его нет.
    """
    store = _store()
    coverage = _coverage(
        complete=False,
        roots=[
            _root(
                complete=False,
                observed_how="adopted",
                observed_at=D1,
                root_url=ROOT,
            )
        ],
    )
    outcome = _evaluate(
        docs=[("hash_a", BOUNDED_QUOTE)],
        output=_absent_uncited(),
        param=_param(AbsenceRuleType.DISTRIBUTION_ABSENCE),
        coverage=coverage,
        scope_acceptance="analyst",
        store=store,
    )

    cell = outcome.cell
    assert cell.status is Status.UNKNOWN
    scope = cell.evidence_scope
    assert scope is not None and scope.denial_strength is DenialStrength.NOT_STATED
    reason = scope.reason or ""
    # I2c пропустил (риск принят), снял именно I2d — и назвал, чего не хватило.
    assert ABSENCE_COVERAGE_UNSATISFIED_REASON not in reason
    assert reason.startswith(ABSENCE_UNDATED_REASON)
    assert "complete=1" in reason
    assert cell.absence_evidence == []
    assert cell.absence_as_of is None


def test_bounded_without_declared_roots_demotes() -> None:
    """Корней нет вовсе — исчерпание нечем удостоверить."""
    store = _store()
    outcome = _evaluate(
        docs=[("hash_a", BOUNDED_QUOTE)],
        output=_absent_uncited(),
        param=_param(AbsenceRuleType.DISTRIBUTION_ABSENCE),
        coverage=_coverage(complete=True),
        scope_acceptance="analyst",
        store=store,
    )

    assert outcome.cell.status is Status.UNKNOWN
    assert "нет объявленных корней" in (outcome.cell.evidence_scope.reason or "")


# ---------------------------------------------------------------------------
# Неприменимость: классы, которыми ``❌`` не публикуется
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "strength",
    [DenialStrength.UNOBSERVED, DenialStrength.NOT_STATED],
)
def test_rule_does_not_apply_to_unknown_classes(
    strength: DenialStrength,
) -> None:
    """``unobserved``/``not_stated`` — правило не применяется (поведение прежнее)."""
    scope = EvidenceScope(
        denial_strength=strength,
        reason="пространство не наблюдалось",
    )
    result = _enforce_dated_absence(
        out_status=Status.ABSENT,
        scope=scope,
        absence_evidence=[],
        param=_param(),
        product_id=PRODUCT,
        coverage=_coverage(),
        artifact_dates=CorpusArtifactDates(product_id=PRODUCT, by_doc_version={}),
    )
    assert result == (Status.ABSENT, scope, False, [], None)


def test_rule_is_inapplicable_without_coverage_or_reader() -> None:
    """``coverage=None`` либо нет читателя артефактов — вердикт не меняется (как I2c)."""
    scope = EvidenceScope(
        denial_strength=DenialStrength.EXPLICIT,
        reason="прямое отрицание",
        citation_quote="нет",
    )
    absent = [{"source_id": _source_id("hash_a")}]
    assert _enforce_dated_absence(
        out_status=Status.ABSENT,
        scope=scope,
        absence_evidence=absent,
        param=_param(),
        product_id=PRODUCT,
        coverage=None,
        artifact_dates=CorpusArtifactDates(product_id=PRODUCT, by_doc_version={}),
    ) == (Status.ABSENT, scope, False, [], None)
    assert _enforce_dated_absence(
        out_status=Status.ABSENT,
        scope=scope,
        absence_evidence=absent,
        param=_param(),
        product_id=PRODUCT,
        coverage=_coverage(),
        artifact_dates=None,
    ) == (Status.ABSENT, scope, False, [], None)
    assert observe_corpus_artifact_dates(None, RUN_ID, PRODUCT) is None
    # Store без читателя ``iter_corpus_artifacts`` — тоже «нечем читать».
    assert observe_corpus_artifact_dates(object(), RUN_ID, PRODUCT) is None


def test_i2c_demotion_leaves_no_second_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    """I2c сработал раньше ⇒ ячейка уже ``❓``, ``absence_undated`` не пишется."""
    import core.pipeline.evaluate as evaluate_module

    conflicting = EvidenceScope.model_construct(
        denial_strength=DenialStrength.BOUNDED,
        reason="конечный справочник объявлен каталогом",
        enumerated=True,
        scope_acceptance=None,
    )
    monkeypatch.setattr(
        evaluate_module, "_resolve_denial", lambda **_: (conflicting, [])
    )
    store = _store()
    outcome = _evaluate(
        docs=[("hash_a", DOC_A)],
        coverage=_coverage(complete=False),
        store=store,
    )

    cell = outcome.cell
    assert cell.status is Status.UNKNOWN
    reason = cell.evidence_scope.reason or ""
    assert ABSENCE_COVERAGE_UNSATISFIED_REASON in reason
    assert ABSENCE_UNDATED_REASON not in reason
    assert not any(
        note.startswith(ABSENCE_UNDATED_REASON) for note in cell.validation_notes
    )


# ---------------------------------------------------------------------------
# Перенос: тот же инвариант, тот же носитель текущего прогона
# ---------------------------------------------------------------------------


def _carried_cell(status: Status = Status.ABSENT) -> Cell:
    """Ячейка-предшественник: ``❌`` с цитатой на ``hash_a`` (как пишет Ш6)."""
    strength = (
        DenialStrength.EXPLICIT
        if status is Status.ABSENT
        else DenialStrength.NOT_STATED
    )
    return Cell(
        run_id=PREV_RUN,
        product_id=PRODUCT,
        param_id="param_alpha",
        state=CellState.EVALUATED,
        status=status,
        absence_evidence=[
            {
                "evidence_id": "ev-1",
                "source_id": _source_id("hash_a"),
                "citation_quote": DOC_A,
                "evidence_type": "direct",
            },
            {
                "evidence_id": "ev-absence",
                "source_id": None,
                "evidence_type": "absence",
            },
        ],
        evidence_scope=EvidenceScope(
            denial_strength=strength,
            reason="прямое отрицание" if strength is DenialStrength.EXPLICIT else "не знаю",
            citation_quote=DOC_A if strength is DenialStrength.EXPLICIT else None,
        ),
        absence_as_of=D1,
    )


def test_carried_absent_passes_with_dated_carrier() -> None:
    """Носитель в текущем прогоне есть — перенос ``❌`` проходит и получает здешнюю дату."""
    dates = CorpusArtifactDates(
        product_id=PRODUCT, by_doc_version={_dv("hash_a"): D2}
    )
    holds, as_of = _carried_rules_hold(
        _carried_cell(),
        param=_param(),
        product_id=PRODUCT,
        coverage=_coverage(roots=[_root()]),
        artifact_dates=dates,
    )
    assert holds is True
    assert as_of == D2


def test_carried_absent_is_refused_without_carrier() -> None:
    """Носителя нет — перенесённый ``❌`` не проходит: иначе перенос обходил бы I2d."""
    holds, as_of = _carried_rules_hold(
        _carried_cell(),
        param=_param(),
        product_id=PRODUCT,
        coverage=_coverage(roots=[_root()]),
        artifact_dates=CorpusArtifactDates(product_id=PRODUCT, by_doc_version={}),
    )
    assert holds is False
    assert as_of is None


def test_carried_unknown_is_not_checked() -> None:
    """``❓`` переносится без вопроса о носителе: I2d говорит только про ``❌``."""
    holds, as_of = _carried_rules_hold(
        _carried_cell(Status.UNKNOWN),
        param=_param(),
        product_id=PRODUCT,
        coverage=_coverage(),
        artifact_dates=None,
    )
    assert (holds, as_of) == (True, None)


class _SpyState:
    """``ProductStatePort``-двойник: запоминает, дошёл ли гейт до чтения измерения."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def get_cell_measurement(self, run_id: str, product_id: str, param_id: str):
        self.calls.append((run_id, product_id, param_id))
        return None


def _reuse_conditions():
    return cell_reuse_conditions(
        product_id=PRODUCT,
        conditions=EvalConditions(
            methodology_version="0.2.0",
            status_dict_version="2026-10-01",
            capability_policy="resolved-only-v1",
            unknown_policy="excluded",
            partial_score=0.5,
        ),
        scoring_policy_version="policy",
        evaluator=_ScriptedEvaluator(_absent([])),
        scope_acceptance=None,
    )


def test_carry_gate_reads_completeness_from_captures() -> None:
    """Гейт переноса — ``coverage.complete``; он теперь считается по снятиям корней.

    Код гейта не менялся: тест фиксирует, что перенос проходит на покрытии, полном **по
    снятиям** (элементов сеанса нет — ``frozen``-подобный проход), и отказывает, когда у
    корня нет ``complete``-снятия. Отказ виден по тому, что гейт не доходит до чтения
    строки измерения.
    """
    from core.pipeline.carry_forward import carry_forward_cell
    from core.ports.state import CaptureRow

    store = _store()
    state = _SpyState()
    conditions = _reuse_conditions()
    retrieved = [_chunk("hash_a", 0, DOC_A)]

    def _capture(*, complete: bool, observed_how: str) -> CaptureRow:
        return CaptureRow(
            capture_id="cap-1",
            product_id=PRODUCT,
            root_url=ROOT,
            started_at=TS,
            finished_at=D1,
            complete=complete,
            observed_how=observed_how,
        )

    full = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[_capture(complete=True, observed_how="live")],
    )
    assert full.complete is True

    carry_forward_cell(
        _param(),
        product_id=PRODUCT,
        run_id=RUN_ID,
        prev_run_id=PREV_RUN,
        state=state,  # type: ignore[arg-type] — двойник порта
        prev_store=store,
        conditions=conditions,
        coverage=full,
        retrieved=retrieved,
    )
    assert state.calls == [(PREV_RUN, PRODUCT, "param_alpha")]

    state.calls.clear()
    adopted_only = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[_capture(complete=False, observed_how="adopted")],
    )
    assert adopted_only.complete is False

    carry_forward_cell(
        _param(),
        product_id=PRODUCT,
        run_id=RUN_ID,
        prev_run_id=PREV_RUN,
        state=state,  # type: ignore[arg-type] — двойник порта
        prev_store=store,
        conditions=conditions,
        coverage=adopted_only,
        retrieved=retrieved,
    )
    assert state.calls == []


# ---------------------------------------------------------------------------
# Отчёт: ``absence_as_of`` публикуется рядом с ``evidence_as_of``
# ---------------------------------------------------------------------------


def _freshness_cell(*, absence_as_of: str | None) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id=PRODUCT,
        param_id="param_alpha",
        state=CellState.EVALUATED,
        status=Status.ABSENT,
        absence_as_of=absence_as_of,
        # ``absence_as_of`` — дата **опубликованного** ❌; ячейка без класса отрицания
        # была бы невалидной (I2), поэтому класс и цитата здесь настоящие.
        evidence_scope=EvidenceScope(
            denial_strength=DenialStrength.EXPLICIT,
            reason="прямое отрицание",
            citation_quote=DOC_A,
        ),
        absence_evidence=[
            {
                "evidence_id": "ev-1",
                "source_id": _source_id("hash_a"),
                "citation_quote": DOC_A,
                "evidence_type": "direct",
            }
        ],
    )


def test_zero_session_sources_with_captures_are_evaluated_not_failed() -> None:
    """``frozen``-подобный проход: элементов сеанса нет, но корпус удостоверён снятием.

    План 20 §11.6: без этого I2d был бы недостижим — фаза объявляла бы ячейки
    ``failed/no_sources`` раньше, чем дело доходит до дат (измерено 05.11.2026 на
    ``demo-absence``). ``expected_sources == 0`` само по себе перестало быть условием
    отказа: полноту решает снятие корня.
    """
    from core.pipeline.evaluate import evaluate_phase
    from core.pipeline.runner import Ports
    from tests.fakes.ports import FakeEvaluator
    from tests.pipeline.test_phases import RUN_ID as PHASE_RUN_ID
    from tests.pipeline.test_phases import _chunks_index, _plan, _store

    store = _store()
    store.save_corpus_coverage(PHASE_RUN_ID, _coverage(roots=[_root()]))

    result = evaluate_phase(
        plan=_plan(products=("p1",)),
        ports=Ports(
            evaluator=FakeEvaluator(
                default=LLMCellOutput(
                    status=Status.UNKNOWN, justification="не установлено"
                )
            ),
            lexical_index=_chunks_index(),
        ),
        store=store,
        run_id=PHASE_RUN_ID,
        profile="R0",
        cutoff_k=3,
    )

    cell = next(c for c in result.cells if c.product_id == "p1")
    assert cell.state is CellState.EVALUATED
    assert cell.status is Status.UNKNOWN


def test_cell_freshness_csv_publishes_absence_as_of(tmp_path: Path) -> None:
    """``absence_as_of`` — из ``cells.absence_as_of``; ``None`` — пустое поле.

    Дата не пересчитывается отчётом: конвейер уже выбрал носитель (I2d), и второй расчёт
    в отчёте разошёлся бы с вердиктом. Здесь же проверяется, что колонка стоит **рядом**
    с ``evidence_as_of`` и что существующая часть строки не изменилась.
    """
    import csv

    from core.pipeline.corpus_freshness import (
        build_corpus_freshness,
        write_cell_freshness_rows,
    )

    artifacts = [_artifact("hash_a", D1)]
    refs = [
        {"product_id": PRODUCT, "param_id": "param_alpha", "doc_version_id": _dv("hash_a")}
    ]
    fresh = build_corpus_freshness(
        run_id=RUN_ID,
        artifacts=artifacts,
        cells=[
            _freshness_cell(absence_as_of=D2),
            # Вторая ячейка без даты опоры: строка появляется из-за evidence, а поле пусто.
            Cell(
                run_id=RUN_ID,
                product_id=PRODUCT,
                param_id="param_beta",
                state=CellState.PENDING,
            ),
        ],
        evidence_refs=[
            *refs,
            {"product_id": PRODUCT, "param_id": "param_beta", "doc_version_id": _dv("hash_a")},
        ],
    )
    rows = {row["param_id"]: row for row in fresh.cell_rows}
    assert rows["param_alpha"]["evidence_as_of_from"] == D1
    assert rows["param_alpha"]["absence_as_of"] == D2
    assert rows["param_beta"]["absence_as_of"] is None

    path = write_cell_freshness_rows(fresh.cell_rows, tmp_path / "cell_freshness.csv")
    assert path is not None
    with path.open(encoding="utf-8", newline="") as handle:
        written = {row["param_id"]: row for row in csv.DictReader(handle)}
    assert written["param_alpha"]["absence_as_of"] == D2
    # ``None`` — пустое поле, не ``''``-значение и не выдуманная дата.
    assert written["param_beta"]["absence_as_of"] == ""
    # Прежние колонки не сдвинулись и не изменили значения.
    assert written["param_alpha"]["evidence_as_of_from"] == D1
    assert written["param_alpha"]["evidence_as_of_to"] == D1
    assert written["param_alpha"]["artifacts"] == "1"