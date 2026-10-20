"""Тесты эшелона валидации Ш6 (T9.4, §7.1/§10/02 §C.2).

Проверяют: существование ``source_id`` (I10/I15), цитату-подстроку с нормализацией
пробелов (strict), построение ``CellEvidence`` с детерминированным ``evidence_id`` (S8)
и ``content_hash`` чанка (I19), резолв URL/hash через ``resolver``, аномалии §10.4
(заметки, не сбои), различие strict/preliminary и defensive-отказ N/A (I7).
Фикстуры категорийно-нейтральны (§1.3); порты/модели — in-memory.
"""

from __future__ import annotations

import json
from pathlib import Path

from core.domain import FailureKind, LLMCellOutput, LLMEvidence, Status
from core.domain.anchors import ResolvedSource
from core.pipeline.validate import (
    ValidationFailureKind,
    normalize_quote_text,
    validate_output,
)
from core.ports.types import Chunk
from core.ports.vector_index import RetrievedChunk

FIXTURES = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Фикстуры (нейтральные данные, §1.3)
# ---------------------------------------------------------------------------


def _data() -> dict:
    return json.loads((FIXTURES / "evaluate_inputs.json").read_text(encoding="utf-8"))


def _chunks(data: dict) -> dict[str, Chunk]:
    return {c["chunk_id"]: Chunk(**c) for c in data["chunks"]}


def _output(data: dict, key: str) -> LLMCellOutput:
    return LLMCellOutput(**data[key])


class _Resolver:
    """Фейковый ``SourceResolver``: словарь ``source_id -> ResolvedSource`` (без сети)."""

    def __init__(self, sources: dict[str, ResolvedSource]) -> None:
        self._sources = sources

    def resolve(self, source_id: str) -> ResolvedSource | None:
        return self._sources.get(source_id)


# ---------------------------------------------------------------------------
# (1) source_id существует (I10/I15)
# ---------------------------------------------------------------------------


def test_missing_source_id_fails() -> None:
    data = _data()
    result = validate_output(
        output=_output(data, "missing_source_output"),
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
    )
    assert result.ok is False
    assert result.failure_kind == "validation_failed"
    assert result.resolved_evidence == []
    assert any("I10/I15" in n for n in result.notes)


def test_failure_kind_matches_domain_failure_kind() -> None:
    assert (
        ValidationFailureKind.VALIDATION_FAILED.value
        == FailureKind.VALIDATION_FAILED.value
    )


# ---------------------------------------------------------------------------
# (2) цитата — подстрока чистого текста (§10; нормализация пробелов)
# ---------------------------------------------------------------------------


def test_quote_not_substring_fails() -> None:
    data = _data()
    result = validate_output(
        output=_output(data, "bad_quote_output"),
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
    )
    assert result.ok is False
    assert result.failure_kind == "validation_failed"
    assert any("I22" in n for n in result.notes)


def test_quote_whitespace_normalized_still_matches() -> None:
    data = _data()
    result = validate_output(
        output=_output(data, "whitespace_output"),
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
    )
    assert result.ok is True
    assert len(result.resolved_evidence) == 1
    # хранится исходная (ненормализованная) цитата модели
    assert result.resolved_evidence[0].citation_quote == "экспорт   данных\nв формате CSV"


def test_normalize_quote_text_contract() -> None:
    assert normalize_quote_text("  a\t b\n c  ") == "a b c"
    assert normalize_quote_text("") == ""


def test_empty_quote_fails() -> None:
    data = _data()
    output = LLMCellOutput(
        status=Status.OK,
        justification="голословно",
        evidence=[LLMEvidence(source_id="chunk_a2", quote="   ")],
    )
    result = validate_output(
        output=output,
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
    )
    assert result.ok is False
    assert any("I22" in n for n in result.notes)


def test_chunk_without_content_hash_fails() -> None:
    data = _data()
    chunks = _chunks(data)
    chunks["chunk_a2"] = Chunk(
        chunk_id="chunk_a2",
        chunk_no=1,
        text=chunks["chunk_a2"].text,
        heading_path="",
        context_prefix="",
        content_hash="",
    )
    result = validate_output(
        output=_output(data, "ok_output"),
        chunks=chunks,
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
    )
    assert result.ok is False
    assert any("I19" in n for n in result.notes)


# ---------------------------------------------------------------------------
# (3) CellEvidence: hash (I19), резолв (I10/I15), детерминизм id (S8)
# ---------------------------------------------------------------------------


def test_valid_output_builds_evidence_with_content_hash() -> None:
    data = _data()
    result = validate_output(
        output=_output(data, "ok_output"),
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
    )
    assert result.ok is True
    assert result.failure_kind is None
    assert len(result.resolved_evidence) == 1
    ev = result.resolved_evidence[0]
    assert ev.source_id == "chunk_a2"
    assert ev.content_hash == "hash_a2"  # I19
    assert ev.citation_quote == "Свойство Альфа доступно без дополнительной настройки."
    assert ev.run_id == data["run_id"]


def test_evidence_id_is_deterministic() -> None:
    data = _data()

    def _run():
        return validate_output(
            output=_output(data, "ok_output"),
            chunks=_chunks(data),
            product_id=data["product_id"],
            param_id=data["param"]["param_id"],
            run_id=data["run_id"],
        )

    first = _run()
    second = _run()
    assert first.resolved_evidence[0].evidence_id == second.resolved_evidence[0].evidence_id


def test_resolver_sets_url_and_build_id() -> None:
    data = _data()
    resolver = _Resolver(
        {
            "chunk_a2": ResolvedSource(
                chunk_id="chunk_a2",
                source_url="https://example.test/doc/a2",
                content_hash="hash_a2",
                build_id="build-1",
            )
        }
    )
    result = validate_output(
        output=_output(data, "ok_output"),
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
        resolver=resolver,
    )
    assert result.ok is True
    ev = result.resolved_evidence[0]
    assert ev.source_url == "https://example.test/doc/a2"
    assert ev.index_build_id == "build-1"


def test_resolver_unresolved_url_fails() -> None:
    data = _data()
    resolver = _Resolver({})  # ничего не резолвится
    result = validate_output(
        output=_output(data, "ok_output"),
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
        resolver=resolver,
    )
    assert result.ok is False
    assert result.failure_kind == "validation_failed"
    assert any("I10/I15" in n for n in result.notes)


def test_resolver_hash_mismatch_fails() -> None:
    data = _data()
    resolver = _Resolver(
        {
            "chunk_a2": ResolvedSource(
                chunk_id="chunk_a2",
                source_url="https://example.test/doc/a2",
                content_hash="other-hash",  # I19: расхождение
            )
        }
    )
    result = validate_output(
        output=_output(data, "ok_output"),
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
        resolver=resolver,
    )
    assert result.ok is False
    assert any("I19" in n for n in result.notes)


# ---------------------------------------------------------------------------
# (4) аномалии §10.4 — заметки, не сбои
# ---------------------------------------------------------------------------


def _retrieved(
    data: dict, chunk_id: str, score: float, build_id: str = "build-1"
) -> RetrievedChunk:
    src = next(c for c in data["chunks"] if c["chunk_id"] == chunk_id)
    return RetrievedChunk(
        chunk_id=chunk_id,
        product_id=data["product_id"],
        text=src["text"],
        content_hash=src["content_hash"],
        heading_path=src["heading_path"],
        build_id=build_id,
        score=score,
    )


def test_low_retrieval_score_anomaly_note_not_failure() -> None:
    data = _data()
    chunks = {"chunk_a2": _retrieved(data, "chunk_a2", 0.1)}
    result = validate_output(
        output=_output(data, "ok_output"),
        chunks=chunks,
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
        min_retrieval_score=0.5,
    )
    assert result.ok is True  # аномалия не валит ячейку
    assert any("§10.4" in n for n in result.notes)
    assert result.resolved_evidence[0].index_build_id == "build-1"


# ---------------------------------------------------------------------------
# (3b) partition_build_id evidence: партиция, не композиция (09 §8.1)
# ---------------------------------------------------------------------------


def test_evidence_partition_is_chunk_partition_not_composition() -> None:
    """``RetrievedChunk.build_id`` = ``pb_``, композиция — отдельно → evidence несёт ``pb_``."""
    data = _data()
    chunks = {"chunk_a2": _retrieved(data, "chunk_a2", 0.9, build_id="pb_p1")}
    result = validate_output(
        output=_output(data, "ok_output"),
        chunks=chunks,
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
        composition_id="cb_run",
    )
    assert result.ok is True
    assert result.resolved_evidence[0].partition_build_id == "pb_p1"


def test_evidence_partition_is_none_for_legacy_composition_build_id() -> None:
    """Legacy-строка с id композиции в ``chunk.build_id`` → ``NULL``, не композиция.

    Партиция никогда не равна композиции (разные формулы), поэтому равенство —
    надёжный признак «это не партиция»; backfill и выдумывание запрещены.
    """
    data = _data()
    chunks = {"chunk_a2": _retrieved(data, "chunk_a2", 0.9, build_id="cb_run")}
    result = validate_output(
        output=_output(data, "ok_output"),
        chunks=chunks,
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
        composition_id="cb_run",
    )
    assert result.ok is True
    assert result.resolved_evidence[0].partition_build_id is None
    assert result.resolved_evidence[0].partition_build_id != "cb_run"


def test_single_citation_ok_is_flagged_for_review() -> None:
    data = _data()
    result = validate_output(
        output=_output(data, "ok_output"),
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
    )
    assert result.ok is True
    assert any("единственную" in n for n in result.notes)


# ---------------------------------------------------------------------------
# strict vs preliminary (I13) и defensive N/A (I7)
# ---------------------------------------------------------------------------


def test_preliminary_failure_is_candidate_without_failure_kind() -> None:
    data = _data()
    result = validate_output(
        output=_output(data, "bad_quote_output"),
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
        strict=False,
    )
    assert result.ok is False
    assert result.failure_kind is None
    assert any("candidate" in n for n in result.notes)


def test_not_applicable_rejected_defensively() -> None:
    data = _data()
    output = LLMCellOutput.model_construct(
        status=Status.NOT_APPLICABLE,
        justification="не применимо",
        evidence=[],
    )
    result = validate_output(
        output=output,
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
    )
    assert result.ok is False
    assert any("I7" in n for n in result.notes)


def test_valid_output_no_anomaly_notes_for_partial() -> None:
    data = _data()
    output = LLMCellOutput(
        status=Status.PARTIAL,
        justification="частично подтверждено",
        evidence=[
            LLMEvidence(source_id="chunk_a2", quote="Свойство Альфа доступно")
        ],
    )
    result = validate_output(
        output=output,
        chunks=_chunks(data),
        product_id=data["product_id"],
        param_id=data["param"]["param_id"],
        run_id=data["run_id"],
    )
    assert result.ok is True
    assert result.notes == []  # ⚠️ не помечается аномалией единственной цитаты
