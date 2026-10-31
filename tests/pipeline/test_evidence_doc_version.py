"""O5: evidence-строка несёт адрес **версии документа** (09 §3.3, §10.2).

Прежде ``cell_evidence`` знала только ``content_hash`` — версию **текста**. Она не
различает уровень извлечения и версию экстрактора, поэтому две разные версии документа с
одинаковым текстом были неотличимы: перенос ячейки (``carry_forward``) подтверждал цитату
документом, которого ячейка в этом прогоне не видела (нарушение I17/I19 в части адреса).

Проверяется:

* писатель Ш6 берёт адрес из ``chunk_id`` ретривнутого чанка (``dv_...``, 16 §6), а не из
  ``content_hash``; колонка переживает запись/чтение;
* адрес, которого в адресе нет (legacy-форма ``product:content_hash:chunk_no``), не
  выдумывается: колонка ``NULL`` и **видимая** запись в ``cells.validation_notes``;
* проверка цепочки в ``carry_forward`` стала **строже**: адрес версии добавляется к
  требованию по ``content_hash``, поэтому перенос, разрешённый раньше, теперь запрещён —
  и ровно в случае смены версии документа при том же тексте;
* строки прошлых прогонов (колонки нет) читаются, и их перенос остаётся на прежней
  проверке по ``content_hash``: «не записано» ≠ «любая версия подходит» и ≠ отказ читать.

Офлайн: SQLite в памяти, оценщик — скрипт, ретривал подменяется на границе фазы.
"""

from __future__ import annotations

import datetime as _dt
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.domain import (
    CatalogRow,
    CellEvidence,
    CorpusCoverage,
    EvidenceType,
    Focus,
    LLMCellOutput,
    MeasurementOrigin,
    RunConfig,
    RunMode,
    SourceKind,
    Status,
)
from core.pipeline.carry_forward import _evidence_chain_valid
from core.pipeline.corpus_index import _chunk_id
from core.pipeline.eval_cache import EvalConditions
from core.pipeline.evaluate import (
    EVIDENCE_DOC_VERSION_UNRESOLVED_REASON,
    evaluate_phase,
)
from core.pipeline.incremental import doc_version_id
from core.pipeline.runner import Ports
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.types import Chunk
from core.ports.vector_index import RetrievalResult, RetrievedChunk
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore
from core.util.ids import doc_version_of, parse_source_id

FIXTURES = Path(__file__).parent / "fixtures"
PRODUCT = "p1"
RUN_1 = "run_dv_1"
RUN_2 = "run_dv_2"
PARTITION = "pb_fake_dv"

QUOTE = "Свойство Альфа поддерживается в версии 2."


# ---------------------------------------------------------------------------
# Вспомогательное: чанки, ответ оценщика, прогон фазы
# ---------------------------------------------------------------------------


def _param() -> CatalogRow:
    data = json.loads((FIXTURES / "evaluate_inputs.json").read_text(encoding="utf-8"))
    return CatalogRow(**data["param"])


def _dv(doc_hash: str, extractor: str = "", level: str = "") -> str:
    """Версия документа: ``dv_...`` от (продукт, хеш, версия экстрактора, уровень)."""
    return doc_version_id(PRODUCT, doc_hash, extractor, level)


def _chunk(doc_hash: str, *, extractor: str = "", level: str = "") -> Chunk:
    """Чанк с адресом версии документа (каноническая форма ``chunk_id``)."""
    return Chunk(
        chunk_id=_chunk_id(PRODUCT, _dv(doc_hash, extractor, level), 0),
        chunk_no=0,
        text=QUOTE,
        heading_path="Раздел",
        context_prefix="контекст",
        content_hash=f"ch_{doc_hash}",
    )


def _legacy_chunk(doc_hash: str) -> Chunk:
    """Чанк с **legacy**-адресом: ``product:content_hash:chunk_no`` (версии в нём нет)."""
    return Chunk(
        chunk_id=f"{PRODUCT}:{doc_hash}:0",
        chunk_no=0,
        text=QUOTE,
        heading_path="Раздел",
        context_prefix="контекст",
        content_hash=f"ch_{doc_hash}",
    )


def _present(chunk: Chunk) -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="Вендор описывает свойство в документации.",
        evidence=[{"source_id": chunk.chunk_id, "quote": QUOTE}],
    )


def _coverage() -> CorpusCoverage:
    return CorpusCoverage(
        product_id=PRODUCT,
        complete=True,
        expected_sources=3,
        observed_sources=3,
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
    return EvalConditions(
        methodology_version="0.2.0",
        status_dict_version="2026-10-01",
        capability_policy="resolved-only-v1",
        unknown_policy="excluded",
        partial_score=0.5,
    )


def _retrieval(chunks: list[Chunk]) -> RetrievalResult:
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
    state: SqliteStateStore | None = None,
    previous_store: SqliteStore | None = None,
    previous_run_id: str | None = None,
    monkeypatch: pytest.MonkeyPatch,
):
    plan = SimpleNamespace(
        products=[PRODUCT],
        params=[_param()],
        run_id=run_id,
        snapshot_date=_dt.date(2026, 10, 19),
        cohort=SimpleNamespace(frozen_at=_dt.date(2026, 10, 19)),
    )
    store.save_corpus_coverage(run_id, _coverage())
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
# Колонка: писатель Ш6, запись/чтение, схема
# ---------------------------------------------------------------------------


def test_evidence_row_carries_document_version_from_chunk_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Адрес версии берётся из ``chunk_id`` чанка-цитаты, а не из ``content_hash``."""
    chunk = _chunk("hash_a")
    expected = parse_source_id(chunk.chunk_id).doc_id
    assert expected.startswith("dv_")

    store = _store(RUN_1)
    result = _run_phase(
        run_id=RUN_1,
        store=store,
        evaluator=_ScriptedEvaluator(_present(chunk)),
        chunks=[chunk],
        monkeypatch=monkeypatch,
    )
    assert result.status == "ok", result.errors

    # Колонка БД: адрес версии, а не версия текста.
    row = store.conn.execute(
        "SELECT source_id, content_hash, doc_version_id FROM cell_evidence"
    ).fetchone()
    assert row["doc_version_id"] == expected
    assert row["content_hash"] == chunk.content_hash != expected

    # Читатель: тот же адрес доезжает через доменную модель (перенос работает по нему).
    evidence = store.iter_cell_evidence(RUN_1, PRODUCT, "param_alpha")
    assert [ev.doc_version_id for ev in evidence] == [expected]


def test_evidence_row_is_null_and_note_visible_without_version_address(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy-адрес версии не несёт → ``NULL`` и видимая причина, а не выдуманное значение."""
    chunk = _legacy_chunk("hash_a")

    store = _store(RUN_1)
    result = _run_phase(
        run_id=RUN_1,
        store=store,
        evaluator=_ScriptedEvaluator(_present(chunk)),
        chunks=[chunk],
        monkeypatch=monkeypatch,
    )
    assert result.status == "ok", result.errors

    row = store.conn.execute(
        "SELECT source_id, content_hash, doc_version_id FROM cell_evidence"
    ).fetchone()
    assert row["source_id"] == chunk.chunk_id
    assert row["content_hash"] == chunk.content_hash
    # Адрес версии не выдуман: в legacy-форме её нет.
    assert row["doc_version_id"] is None

    notes = result.cells[0].validation_notes
    assert any(EVIDENCE_DOC_VERSION_UNRESOLVED_REASON in note for note in notes), notes


def test_schema_declares_doc_version_id_column() -> None:
    """Колонка объявлена в ``DDL``; досоздания при открытии больше нет.

    Прежде тест утверждал «аддитивно, версия схемы не поднята» — то есть фиксировал шим,
    снесённый в волне S плана 17. Эквивалент на новом носителе: колонка есть в свежей схеме,
    и `ALTER TABLE`-шима нет (пропуск в `CREATE TABLE` не догоняется ничем).
    """
    from core.store import sqlite as sqlite_module
    from core.store.sqlite import connect

    conn = connect(":memory:")
    try:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(cell_evidence)")}
        assert "doc_version_id" in columns
    finally:
        conn.close()
    assert not hasattr(sqlite_module, "_ensure_columns")


def test_model_rejects_version_that_contradicts_the_address() -> None:
    """O5 — инвариант домена: объявленная версия обязана совпадать со вторым сегментом."""
    with pytest.raises(ValueError, match="O5"):
        CellEvidence(
            evidence_id="ev-1",
            run_id=RUN_1,
            product_id=PRODUCT,
            param_id="param_alpha",
            source_kind=SourceKind.DOCUMENTATION,
            evidence_type=EvidenceType.DIRECT,
            source_id=_chunk_id(PRODUCT, _dv("hash_a"), 0),
            citation_quote=QUOTE,
            content_hash="ch_hash_a",
            doc_version_id=_dv("hash_b"),
        )


def test_legacy_run_without_the_column_stays_readable(tmp_path: Path) -> None:
    """БД прошлой версии (колонки нет) читается: «не записано», а не сбой чтения."""
    path = tmp_path / "cohound.db"
    store = SqliteStore(str(path))
    store.create_run(
        RUN_1,
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
    store.add_evidence(
        CellEvidence(
            evidence_id="ev-1",
            run_id=RUN_1,
            product_id=PRODUCT,
            param_id="param_alpha",
            source_kind=SourceKind.DOCUMENTATION,
            evidence_type=EvidenceType.DIRECT,
            source_id=_chunk_id(PRODUCT, _dv("hash_a"), 0),
            citation_quote=QUOTE,
            content_hash="ch_hash_a",
            doc_version_id=_dv("hash_a"),
        )
    )
    store.close()

    raw = sqlite3.connect(path)
    raw.execute("ALTER TABLE cell_evidence DROP COLUMN doc_version_id")
    raw.commit()
    raw.close()

    legacy = SqliteStore(str(path), read_only=True)
    try:
        evidence = legacy.iter_cell_evidence(RUN_1, PRODUCT, "param_alpha")
        assert len(evidence) == 1
        assert evidence[0].doc_version_id is None
    finally:
        legacy.close()


# ---------------------------------------------------------------------------
# Читатель: проверка цепочки строже, а не слабее
# ---------------------------------------------------------------------------


def _evidence(chunk: Chunk, *, doc_version: str | None) -> CellEvidence:
    return CellEvidence(
        evidence_id="ev-1",
        run_id=RUN_1,
        product_id=PRODUCT,
        param_id="param_alpha",
        source_kind=SourceKind.DOCUMENTATION,
        evidence_type=EvidenceType.DIRECT,
        source_id=chunk.chunk_id,
        citation_quote=QUOTE,
        content_hash=chunk.content_hash,
        doc_version_id=doc_version,
    )


def test_chain_check_adds_version_to_the_hash_requirement() -> None:
    """Адрес версии — **дополнительное** условие: он не заменяет проверку по хешу."""
    cited = _chunk("hash_a")
    hashes = frozenset({cited.content_hash})
    version = doc_version_of(cited.chunk_id)
    assert version is not None

    # Версия в retrieved-наборе — цепочка цела.
    assert _evidence_chain_valid(
        [_evidence(cited, doc_version=version)],
        product_id=PRODUCT,
        retrieved_hashes=hashes,
        retrieved_doc_versions=frozenset({version}),
    )
    # Тот же текст, другая версия документа (другой уровень/экстрактор) — запрет.
    assert not _evidence_chain_valid(
        [_evidence(cited, doc_version=version)],
        product_id=PRODUCT,
        retrieved_hashes=hashes,
        retrieved_doc_versions=frozenset({_dv("hash_a", "docling-x", "full")}),
    )
    # Строка прошлого прогона без адреса — прежняя проверка по хешу (не слабее и не строже).
    assert _evidence_chain_valid(
        [_evidence(cited, doc_version=None)],
        product_id=PRODUCT,
        retrieved_hashes=hashes,
        retrieved_doc_versions=frozenset(),
    )
    # Хеш не в наборе — запрет по-прежнему (проверка не ослаблена новым полем).
    assert not _evidence_chain_valid(
        [_evidence(cited, doc_version=version)],
        product_id=PRODUCT,
        retrieved_hashes=frozenset({"другой_хеш"}),
        retrieved_doc_versions=frozenset({version}),
    )


def test_carry_is_refused_when_only_the_document_version_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Тот же текст в другой версии документа: раньше перенос, теперь переоценка.

    ``retrieved_doc_set_digest`` (слагаемое ключа) считается по ``content_hash``, поэтому
    ключ переноса в обоих прогонах совпадает: различие ловит **только** проверка адреса
    версии. Это и есть ужесточение O5 — перенос не разрешён там, где раньше был разрешён.
    """
    first_chunk = _chunk("hash_a")
    state = SqliteStateStore(":memory:")

    first_store = _store(RUN_1)
    _run_phase(
        run_id=RUN_1,
        store=first_store,
        evaluator=_ScriptedEvaluator(_present(first_chunk)),
        chunks=[first_chunk],
        state=state,
        monkeypatch=monkeypatch,
    )

    # Контроль: тот же адрес версии → перенос (иначе тест доказывал бы не то).
    same_store = _store(RUN_2)
    control = _ScriptedEvaluator(_present(first_chunk))
    control_phase = _run_phase(
        run_id=RUN_2,
        store=same_store,
        evaluator=control,
        chunks=[first_chunk],
        state=state,
        previous_store=first_store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )
    assert control.calls == []
    assert control_phase.cells[0].measurement_origin is MeasurementOrigin.CARRIED

    # Другая версия документа при том же тексте → переоценка.
    other_chunk = _chunk("hash_a", extractor="docling-x", level="full")
    assert other_chunk.content_hash == first_chunk.content_hash
    assert other_chunk.chunk_id != first_chunk.chunk_id

    second_store = _store(RUN_2)
    evaluator = _ScriptedEvaluator(_present(other_chunk))
    second = _run_phase(
        run_id=RUN_2,
        store=second_store,
        evaluator=evaluator,
        chunks=[other_chunk],
        state=state,
        previous_store=first_store,
        previous_run_id=RUN_1,
        monkeypatch=monkeypatch,
    )
    assert len(evaluator.calls) == 1
    assert second.cells[0].measurement_origin is MeasurementOrigin.OBSERVED
    # Новая строка несёт адрес новой версии, а не унаследованный.
    row = second_store.conn.execute(
        "SELECT doc_version_id FROM cell_evidence"
    ).fetchone()
    assert row["doc_version_id"] == doc_version_of(other_chunk.chunk_id)
