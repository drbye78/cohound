"""Правило «``❌`` требует ``live|revalidated``» (16 §8 п.4, 09 §11.2) — исполняется.

Правило читает **происхождение наблюдения** (``corpus_docs.reuse_kind``) и имеет два
уровня, потому что у двух групп классов разный носитель утверждения:

* уровень **evidence** (``explicit``/``authoritative``) — «нет» держится на конкретном
  документе, поэтому проверяется **каждый** документ, на который опирается ``❌``;
* уровень **корпуса** (``bounded`` и прочие классы, дающие ``❌``) — «нет» выводится из
  корпуса, поэтому ``reused``/``stale``/``NULL``-документ в корпусе продукта снимает ``❌``.

Здесь фиксируется: ``reused``/``NULL`` снимают ``❌``, ``revalidated`` — нет; нарушение даёт
``❓`` (не ``failed``: I4) с машинной причиной ``reuse_unconfirmed`` в
``evidence_scope.reason`` и в ``cells.validation_notes``, а протокол отсутствия не
публикуется. Плюс — неприменимость правила там, где наблюдений нет, и приоритет I2c.

Офлайн: SQLite в памяти / заглушки, оценщик — скрипт.
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
    CellState,
    CorpusCoverage,
    CorpusDoc,
    DenialStrength,
    EvidenceScope,
    Focus,
    LLMCellOutput,
    ReuseKind,
    RunConfig,
    RunMode,
    Status,
    check_cell,
)
from core.pipeline.corpus_index import _chunk_id
from core.pipeline.evaluate import (
    ABSENCE_COVERAGE_UNSATISFIED_REASON,
    REUSE_UNCONFIRMED_REASON,
    CorpusReuseObservation,
    _enforce_reuse_confirmed,
    _evidence_level_reuse_violations,
    evaluate_cell,
    evaluate_phase,
    observe_corpus_reuse,
)
from core.pipeline.incremental import doc_version_id
from core.pipeline.runner import Ports
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.types import Chunk
from core.store.sqlite_store import SqliteStore

FIXTURES = Path(__file__).parent / "fixtures"
PRODUCT = "p1"
RUN_ID = "run_eval_1"


def _param(absence_rule: AbsenceRuleType | None = None) -> CatalogRow:
    """Строка каталога из общего фикстура (``absence_rule=explicit`` по умолчанию)."""
    data = json.loads((FIXTURES / "evaluate_inputs.json").read_text(encoding="utf-8"))
    param = CatalogRow(**data["param"])
    return param if absence_rule is None else param.model_copy(
        update={"absence_rule": absence_rule}
    )


def _dv(doc_hash: str) -> str:
    """Версия документа в той форме, которую реально несёт ``chunk_id`` (16 §6).

    Строится **ядром** (``incremental.doc_version_id``), а не руками: тест обязан
    адресовать документ тем же пространством идентификаторов, что и конвейер, иначе он
    проверяет формат, которого больше нет (``dv_`` из ``content_hash`` не выводится).
    """
    return doc_version_id(PRODUCT, doc_hash, "", "")


def _chunk(doc_hash: str, chunk_no: int, text: str) -> Chunk:
    """Чанк с **настоящим** ``chunk_id``: ``product:doc_version_id:chunk_no`` (16 §6).

    Строится той же функцией ядра (``corpus._chunk_id``), что и в конвейере: иначе
    правило не смогло бы восстановить документ из evidence, и тест проверял бы не тот
    путь (так и было: прежняя фикстура собирала до-граничный формат с ``content_hash``
    во втором сегменте).
    """
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


def _doc_ref(doc_hash: str) -> str:
    """Как документ называется в причине нарушения: ``product:doc_version_id``."""
    return f"{PRODUCT}:{_dv(doc_hash)}"


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


def _coverage(*, complete: bool = True) -> CorpusCoverage:
    return CorpusCoverage(
        product_id=PRODUCT,
        complete=complete,
        expected_sources=3,
        observed_sources=3 if complete else 2,
        computed_at="2026-01-01T00:00:00+00:00",
    )


def _reuse(**kinds: str) -> CorpusReuseObservation:
    """Наблюдение корпуса: ``doc_hash=<reuse_kind>``; ``none`` — ``NULL`` (не записано).

    Обе формы адреса заполняются из одной строки — так делает и ``observe_corpus_reuse``:
    ``by_doc`` (``product:content_hash``) и ``by_doc_version`` (``dv_...``) — это один
    документ в двух формах, а не два правила.
    """
    by_doc: dict[str, ReuseKind | None] = {}
    by_doc_version: dict[str, ReuseKind | None] = {}
    for doc_hash, kind in kinds.items():
        value = None if kind == "none" else ReuseKind(kind)
        by_doc[f"{PRODUCT}:{doc_hash}"] = value
        by_doc_version[_dv(doc_hash)] = value
    return CorpusReuseObservation(
        product_id=PRODUCT, by_doc=by_doc, by_doc_version=by_doc_version
    )


class _ScriptedEvaluator:
    """Один ответ оценщика (без сети, I18)."""

    model_id = "fake-model"
    model_version = "1.0"
    seed = 0
    temperature = 0.0
    decoder = "fake"

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


class _RecordingStore:
    """Приёмник записей (без SQLite): проверяется, что протокол отсутствия не пишется."""

    def __init__(self) -> None:
        self.cells: list = []
        self.evidence: list = []
        self.absence: list[dict] = []

    def save_cell(self, cell) -> None:  # noqa: ANN001
        self.cells.append(cell)

    def add_evidence_batch(self, evs) -> None:  # noqa: ANN001
        self.evidence.extend(evs)

    def add_absence_evidence(self, entries) -> None:  # noqa: ANN001
        self.absence.extend(entries)


def _absent_outcome(
    *,
    docs: list[tuple[str, str]],
    corpus_reuse: CorpusReuseObservation | None,
    coverage: CorpusCoverage | None = None,
    param: CatalogRow | None = None,
    scope_acceptance: str | None = None,
    output: LLMCellOutput | None = None,
):
    """Прогоняет ячейку через Ш6 с заданным наблюдением корпуса."""
    store = _RecordingStore()
    chunks = [_chunk(doc_hash, 0, text) for doc_hash, text in docs]
    outcome = evaluate_cell(
        param=param or _param(),
        product_id=PRODUCT,
        run_id=RUN_ID,
        evaluator=_ScriptedEvaluator(output or _absent(docs)),
        chunks=chunks,
        coverage=coverage if coverage is not None else _coverage(),
        scope_acceptance=scope_acceptance,
        corpus_reuse=corpus_reuse,
        store=store,
    )
    return outcome, store


# ---------------------------------------------------------------------------
# Evidence-уровень: `explicit` — проверяется документ, на который опирается `❌`
# ---------------------------------------------------------------------------

def test_reused_document_removes_absent() -> None:
    """Документ, взятый из retained-байтов (``reused``), ``❌`` не поддерживает."""
    docs = [("hash_a", "Свойство Альфа не поддерживается в этой версии продукта.")]
    outcome, store = _absent_outcome(
        docs=docs, corpus_reuse=_reuse(hash_a="reused")
    )

    cell = outcome.cell
    assert cell.state is CellState.EVALUATED
    assert cell.status is Status.UNKNOWN          # I12: ❓, а не «❌ без подтверждения»
    assert outcome.failure_kind is None           # I4: это не отказ прибора
    scope = cell.evidence_scope
    assert scope is not None
    assert scope.denial_strength is DenialStrength.NOT_STATED
    # Причина называет виновника: продукт/параметр, документ и что именно не так.
    assert REUSE_UNCONFIRMED_REASON in (scope.reason or "")
    assert f"{PRODUCT}/param_alpha" in (scope.reason or "")
    assert _doc_ref("hash_a") in (scope.reason or "")
    assert "reused" in (scope.reason or "")
    assert any(
        REUSE_UNCONFIRMED_REASON in note and _doc_ref("hash_a") in note
        for note in cell.validation_notes
    )
    # Протокол отсутствия не публикуется: «искали и не нашли» здесь не доказано.
    assert cell.absence_evidence == []
    assert store.absence == []
    assert check_cell(cell) == []


def test_revalidated_document_publishes_absent() -> None:
    """304 в этом цикле — сервер ответил: ``revalidated`` ``❌`` поддерживает."""
    docs = [("hash_a", "Свойство Альфа не поддерживается в этой версии продукта.")]
    outcome, store = _absent_outcome(
        docs=docs, corpus_reuse=_reuse(hash_a="revalidated")
    )

    assert outcome.cell.status is Status.ABSENT
    scope = outcome.cell.evidence_scope
    assert scope is not None and scope.denial_strength is DenialStrength.EXPLICIT
    assert not any(
        REUSE_UNCONFIRMED_REASON in note for note in outcome.cell.validation_notes
    )
    assert outcome.cell.absence_evidence != []
    assert store.absence != []
    assert check_cell(outcome.cell) == []


def test_legacy_null_reuse_kind_removes_absent() -> None:
    """``reuse_kind IS NULL`` — «наблюдение не записано» (legacy): ``❌`` не публикуется."""
    docs = [("hash_a", "Свойство Альфа не поддерживается в этой версии продукта.")]
    outcome, _ = _absent_outcome(docs=docs, corpus_reuse=_reuse(hash_a="none"))

    assert outcome.cell.status is Status.UNKNOWN
    assert "NULL" in (outcome.cell.evidence_scope.reason or "")
    assert outcome.cell.absence_evidence == []


def test_stale_document_removes_absent() -> None:
    """``stale`` (ревалидация пропущена по потолку) — тоже не подтверждение."""
    docs = [("hash_a", "Свойство Альфа не поддерживается в этой версии продукта.")]
    outcome, _ = _absent_outcome(docs=docs, corpus_reuse=_reuse(hash_a="stale"))

    assert outcome.cell.status is Status.UNKNOWN
    assert "stale" in (outcome.cell.evidence_scope.reason or "")


def test_evidence_level_checks_every_document() -> None:
    """Один ``reused`` среди трёх процитированных документов снимает ``❌``."""
    docs = [
        ("hash_a", "Свойство Альфа не поддерживается в этой версии продукта."),
        ("hash_b", "В версии 2 свойство Альфа удалено."),
        ("hash_c", "Свойство Альфа не входит в бесплатный план."),
    ]
    outcome, _ = _absent_outcome(
        docs=docs,
        corpus_reuse=_reuse(hash_a="live", hash_b="live", hash_c="reused"),
    )

    assert outcome.cell.status is Status.UNKNOWN
    reason = outcome.cell.evidence_scope.reason or ""
    assert REUSE_UNCONFIRMED_REASON in reason
    # Нарушитель назван, подтверждённые документы — нет.
    assert _doc_ref("hash_c") in reason
    assert _doc_ref("hash_a") not in reason
    assert _doc_ref("hash_b") not in reason


def test_evidence_level_requires_all_documents_confirmed() -> None:
    """Все три подтверждены — ``❌`` публикуется (правило не «запрещает всё»)."""
    docs = [
        ("hash_a", "Свойство Альфа не поддерживается в этой версии продукта."),
        ("hash_b", "В версии 2 свойство Альфа удалено."),
        ("hash_c", "Свойство Альфа не входит в бесплатный план."),
    ]
    outcome, _ = _absent_outcome(
        docs=docs,
        corpus_reuse=_reuse(hash_a="live", hash_b="revalidated", hash_c="live"),
    )

    assert outcome.cell.status is Status.ABSENT


def test_document_outside_product_corpus_is_not_confirmable() -> None:
    """Документ вне корпуса продукта подтвердить нечем — ``❌`` снимается."""
    docs = [("hash_a", "Свойство Альфа не поддерживается в этой версии продукта.")]
    outcome, _ = _absent_outcome(docs=docs, corpus_reuse=_reuse(hash_b="live"))

    assert outcome.cell.status is Status.UNKNOWN
    assert "отсутствует в corpus_docs" in (outcome.cell.evidence_scope.reason or "")

def test_document_version_form_is_resolved_without_content_hash_entry() -> None:
    """``source_id`` несёт версию документа (``dv_...``): документ находится по ней.

    Это и есть регрессия границы A: ``chunk_id`` адресует версию, а ``corpus_docs.doc_id``
    остаётся ``product:content_hash``, поэтому поиск только по ``doc_id`` не находил
    **ничего** и снимал каждый подтверждённый ``❌``.
    """
    doc_hash = "hash_a"
    corpus_reuse = CorpusReuseObservation(
        product_id=PRODUCT,
        # Строка корпуса известна только своей второй формой адреса.
        by_doc={f"{PRODUCT}:other": ReuseKind.REUSED},
        by_doc_version={_dv(doc_hash): ReuseKind.LIVE},
    )

    assert _evidence_level_reuse_violations(
        corpus_reuse, [{"source_id": _source_id(doc_hash)}]
    ) == []

def test_legacy_content_hash_chunk_id_still_confirmed() -> None:
    """Legacy-форма ``chunk_id`` (``content_hash`` во втором сегменте) продолжает работать.

    До границы A второй сегмент был хешем содержимого; такие ``source_id`` есть в
    сохранённых артефактах, и читаемость не должна зависеть от порядка форм поиска.
    """
    corpus_reuse = CorpusReuseObservation(
        product_id=PRODUCT,
        by_doc={f"{PRODUCT}:hash_a": ReuseKind.LIVE},
    )

    assert _evidence_level_reuse_violations(
        corpus_reuse, [{"source_id": f"{PRODUCT}:hash_a:0"}]
    ) == []

def test_missing_document_reason_names_the_forms_searched() -> None:
    """Причина называет, что именно искали: иначе оператор не знает, чего не хватило."""
    corpus_reuse = _reuse(hash_b="live")
    violations = _evidence_level_reuse_violations(
        corpus_reuse, [{"source_id": _source_id("hash_a")}]
    )

    assert len(violations) == 1
    assert _doc_ref("hash_a") in violations[0]
    assert f"doc_version_id={_dv('hash_a')}" in violations[0]
    assert "наблюдение не записано" in violations[0]


# ---------------------------------------------------------------------------
# Корпусный уровень: `bounded` выводит «нет» из корпуса — смотрится весь корпус
# ---------------------------------------------------------------------------

def test_corpus_level_checks_the_whole_corpus() -> None:
    """``bounded``: ``stale``-документ, на который ``❌`` не опирается, всё равно снимает его."""
    docs = [("hash_a", "Свойство Альфа отсутствует в справочнике.")]
    outcome, _ = _absent_outcome(
        docs=docs,
        output=_absent_uncited(),
        param=_param(AbsenceRuleType.DISTRIBUTION_ABSENCE),
        scope_acceptance="analyst",
        corpus_reuse=_reuse(hash_a="live", hash_b="stale", hash_c="live"),
    )

    cell = outcome.cell
    assert cell.status is Status.UNKNOWN
    scope = cell.evidence_scope
    assert scope is not None and scope.denial_strength is DenialStrength.NOT_STATED
    reason = scope.reason or ""
    assert REUSE_UNCONFIRMED_REASON in reason
    assert "уровень corpus" in reason
    # Причина называет корпус, счётчики по классам и пример документа.
    assert f"корпус продукта {PRODUCT}" in reason
    assert "1 из 3" in reason
    assert "stale=1" in reason
    assert f"{PRODUCT}:hash_b" in reason
    assert cell.absence_evidence == []


def test_corpus_level_confirmed_corpus_publishes_absent() -> None:
    """Весь корпус подтверждён — ``bounded`` публикует ``❌``."""
    docs = [("hash_a", "Свойство Альфа отсутствует в справочнике.")]
    outcome, _ = _absent_outcome(
        docs=docs,
        output=_absent_uncited(),
        param=_param(AbsenceRuleType.DISTRIBUTION_ABSENCE),
        scope_acceptance="analyst",
        corpus_reuse=_reuse(hash_a="live", hash_b="revalidated"),
    )

    assert outcome.cell.status is Status.ABSENT
    scope = outcome.cell.evidence_scope
    assert scope is not None and scope.denial_strength is DenialStrength.BOUNDED


def test_authoritative_denial_uses_evidence_level() -> None:
    """``authoritative`` — второй класс evidence-уровня: смотрится документ, а не корпус."""
    scope = EvidenceScope.model_construct(
        denial_strength=DenialStrength.AUTHORITATIVE,
        reason="авторитетная энумерация реестра",
        enumerated=True,
    )
    param = _param()
    absence = [
        {"source_id": _source_id("hash_a")},
        {"source_id": _source_id("hash_b")},
        {"source_id": None},  # протокол отсутствия — не документ
    ]
    status, demoted_scope, demoted, notes = _enforce_reuse_confirmed(
        out_status=Status.ABSENT,
        scope=scope,
        absence_evidence=absence,
        param=param,
        product_id=PRODUCT,
        coverage=_coverage(),
        corpus_reuse=_reuse(hash_a="live", hash_b="reused"),
    )

    assert (status, demoted) == (Status.UNKNOWN, True)
    assert demoted_scope is not None
    assert demoted_scope.denial_strength is DenialStrength.NOT_STATED
    assert "уровень evidence" in (demoted_scope.reason or "")
    assert _doc_ref("hash_b") in (demoted_scope.reason or "")
    assert any(_doc_ref("hash_b") in note for note in notes)


# ---------------------------------------------------------------------------
# Неприменимость: нет наблюдений — нечего проверять
# ---------------------------------------------------------------------------

def test_rule_is_inapplicable_without_coverage_record() -> None:
    """``coverage=None``: записи покрытия нет — правило неприменимо (как I2c)."""
    status, scope, demoted, notes = _enforce_reuse_confirmed(
        out_status=Status.ABSENT,
        scope=EvidenceScope(
            denial_strength=DenialStrength.EXPLICIT,
            reason="прямое отрицание",
            citation_quote="нет",
        ),
        absence_evidence=[{"source_id": _source_id("hash_a")}],
        param=_param(),
        product_id=PRODUCT,
        coverage=None,
        corpus_reuse=_reuse(hash_a="reused"),
    )

    assert (status, demoted, notes) == (Status.ABSENT, False, [])
    assert scope is not None and scope.denial_strength is DenialStrength.EXPLICIT


def test_rule_is_inapplicable_without_corpus_observation() -> None:
    """Наблюдения корпуса нет (нечем читать / корпус пуст) — вердикт не меняется."""
    docs = [("hash_a", "Свойство Альфа не поддерживается в этой версии продукта.")]
    outcome, _ = _absent_outcome(docs=docs, corpus_reuse=None)
    assert outcome.cell.status is Status.ABSENT

    empty = CorpusReuseObservation(product_id=PRODUCT, by_doc={})
    assert empty.is_applicable() is False
    status, _, demoted, _ = _enforce_reuse_confirmed(
        out_status=Status.ABSENT,
        scope=EvidenceScope(
            denial_strength=DenialStrength.EXPLICIT,
            reason="прямое отрицание",
            citation_quote="нет",
        ),
        absence_evidence=[{"source_id": _source_id("hash_a")}],
        param=_param(),
        product_id=PRODUCT,
        coverage=_coverage(),
        corpus_reuse=empty,
    )
    assert (status, demoted) == (Status.ABSENT, False)


def test_rule_does_not_apply_to_unknown_cells() -> None:
    """``❓`` не «понижается второй раз»: правило говорит только про ``❌``."""
    status, _, demoted, notes = _enforce_reuse_confirmed(
        out_status=Status.UNKNOWN,
        scope=None,
        absence_evidence=[],
        param=_param(),
        product_id=PRODUCT,
        coverage=_coverage(),
        corpus_reuse=_reuse(hash_a="reused"),
    )
    assert (status, demoted, notes) == (Status.UNKNOWN, False, [])


# ---------------------------------------------------------------------------
# Порядок с I2c: две проверки независимы, но вторая причина не добавляется
# ---------------------------------------------------------------------------

def test_i2c_demotion_leaves_no_second_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    """I2c сработал раньше ⇒ ячейка уже ``❓``, ``reuse_unconfirmed`` не пишется."""
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
    docs = [("hash_a", "Свойство Альфа не поддерживается в этой версии продукта.")]
    outcome, _ = _absent_outcome(
        docs=docs,
        corpus_reuse=_reuse(hash_a="reused"),
        coverage=_coverage(complete=False),
    )

    cell = outcome.cell
    assert cell.status is Status.UNKNOWN
    reason = cell.evidence_scope.reason or ""
    assert ABSENCE_COVERAGE_UNSATISFIED_REASON in reason
    assert REUSE_UNCONFIRMED_REASON not in reason
    assert not any(
        REUSE_UNCONFIRMED_REASON in note for note in cell.validation_notes
    )


# ---------------------------------------------------------------------------
# Наблюдение корпуса: один проход на продукт, а не на ячейку
# ---------------------------------------------------------------------------

def _store() -> SqliteStore:
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1", "p2"],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 10, 19),
            mode=RunMode.STRICT,
        ),
    )
    return store


def test_observe_corpus_reuse_reads_only_its_product() -> None:
    """Снимок фильтруется по продукту и сохраняет ``NULL`` как «не записано»."""
    store = _store()
    store.upsert_corpus_doc(
        CorpusDoc(
            doc_id="p1:hash_a",
            product_id="p1",
            url="https://p1.test/a",
            reuse_kind=ReuseKind.REVALIDATED,
        ),
        doc_version_id="dv_aaaa",
    )
    store.upsert_corpus_doc(
        CorpusDoc(
            doc_id="p1:hash_b",
            product_id="p1",
            url="https://p1.test/b",
            reuse_kind=ReuseKind.REUSED,
        )
    )
    store.upsert_corpus_doc(
        CorpusDoc(
            doc_id="p2:hash_c",
            product_id="p2",
            url="https://p2.test/c",
            reuse_kind=ReuseKind.LIVE,
        )
    )
    # Legacy-строка: писателя ``reuse_kind`` не было.
    store.upsert_corpus_doc(
        CorpusDoc(doc_id="p1:hash_d", product_id="p1", url="https://p1.test/d")
    )

    observation = observe_corpus_reuse(store, RUN_ID, "p1")

    assert observation is not None
    assert observation.is_applicable() is True
    assert observation.by_doc == {
        "p1:hash_a": ReuseKind.REVALIDATED,
        "p1:hash_b": ReuseKind.REUSED,
        "p1:hash_d": None,
    }
    # Вторая форма адреса (16 §6): версия документа → тот же ``reuse_kind``. Legacy-строка
    # без версии в индекс не попадает — иначе «не найдено» выдавалось бы за наблюдение.
    assert observation.by_doc_version == {"dv_aaaa": ReuseKind.REVALIDATED}
    assert observation.unconfirmed_docs() == ["p1:hash_b", "p1:hash_d"]


def test_observe_corpus_reuse_keeps_every_version_of_a_document() -> None:
    """Две версии одного документа — обе в индексе и обе подтверждают ``❌``.

    ``dv`` включает уровень извлечения, ``doc_id`` — нет, поэтому один текст на уровнях
    ``text`` и ``layout`` даёт **один** документ корпуса и **две** версии. Носитель «одна
    версия на строку» терял бы вторую, и evidence по ней получал бы ложное «наблюдение
    не записано» — снимая подтверждённый ``❌``.
    """
    store = _store()
    doc = CorpusDoc(
        doc_id="p1:same-text",
        product_id="p1",
        url="https://p1.test/same",
        reuse_kind=ReuseKind.LIVE,
    )
    store.upsert_corpus_doc(doc, doc_version_id="dv_text")
    store.upsert_corpus_doc(doc, doc_version_id="dv_layout")

    observation = observe_corpus_reuse(store, RUN_ID, "p1")

    assert observation is not None
    assert observation.by_doc_version == {
        "dv_text": ReuseKind.LIVE,
        "dv_layout": ReuseKind.LIVE,
    }
    for version in ("dv_text", "dv_layout"):
        assert _evidence_level_reuse_violations(
            observation, [{"source_id": f"p1:{version}:0"}]
        ) == []

def test_observe_corpus_reuse_without_reader_is_not_applicable() -> None:
    """Store без ``iter_corpus_docs`` — правило неприменимо, а не «всё подтверждено»."""
    assert observe_corpus_reuse(None, RUN_ID, PRODUCT) is None
    assert observe_corpus_reuse(_RecordingStore(), RUN_ID, PRODUCT) is None


def test_phase_reads_corpus_once_per_product(monkeypatch: pytest.MonkeyPatch) -> None:
    """Фаза читает ``corpus_docs`` **один раз на продукт**, а не на ячейку."""
    store = _store()
    store.upsert_corpus_doc(
        CorpusDoc(
            doc_id="p1:hash_a",
            product_id="p1",
            url="https://p1.test/a",
            reuse_kind=ReuseKind.LIVE,
        )
    )
    calls: list[str] = []
    original = SqliteStore.iter_corpus_docs

    def counting(self, run_id):  # noqa: ANN001 — обёртка над методом store
        calls.append(run_id)
        return original(self, run_id)

    monkeypatch.setattr(SqliteStore, "iter_corpus_docs", counting)

    class _Evaluator:
        model_id = "fake"

        def complete(self, request):  # noqa: ANN001 — не должен вызываться
            raise AssertionError("пустой срез не тратит вызов модели")

    params = [_param(), _param().model_copy(update={"param_id": "param_beta"})]
    plan = SimpleNamespace(
        products=["p1", "p2"],
        params=params,
        run_id=RUN_ID,
        snapshot_date=_dt.date(2026, 10, 19),
        cohort=SimpleNamespace(frozen_at=_dt.date(2026, 10, 19)),
    )
    evaluate_phase(
        plan=plan,
        ports=Ports(evaluator=_Evaluator()),
        store=store,
        run_id=RUN_ID,
    )

    # Два продукта × два параметра — но выборка корпуса одна на продукт.
    assert len(calls) == 2
