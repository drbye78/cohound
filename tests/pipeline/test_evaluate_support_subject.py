"""Ш6: замер «о предмете ли доказательство» — fabricated_support без смены вердикта.

Живой прогон дал ``✅``, у которых единственная цитата — строка шапки документа
(``Jatoba 18 Standard/Enterprise (18.02.2026, PostgreSQL 18)``): I22 формально исполнена
(цитата непуста и дословно есть в чанке), но предмета параметра в ней нет. Здесь
фиксируется поведение замера: он **не** меняет вердикт, а публикует величину и адресный
человеческий сигнал (``cells.validation_notes``, ``review_reason`` для ``✅``).
"""

from __future__ import annotations

import json
from pathlib import Path

from core.domain import (
    CatalogRow,
    CellState,
    LLMCellOutput,
    LLMEvidence,
    SourceKind,
    Status,
)
from core.pipeline.evaluate import (
    SUPPORT_NOT_ABOUT_SUBJECT,
    SUPPORT_STATE_NO_QUOTES,
    SUPPORT_STATE_SUPPORTED,
    SUPPORT_STATE_UNRELATED,
    evaluate_cell,
    support_overlap_counts,
    support_overlap_scan,
    support_subject_overlap,
)
from core.ports.llm import LLMRequest, LLMResponse
from core.ports.types import Chunk

FIXTURES = Path(__file__).parent / "fixtures"

#: Строка шапки документа — ровно та «цитата», которая дала 8 из 9 ложных ``✅`` живого
#: прогона: непуста, дословно присутствует в чанке, о предмете не говорит ничего.
HEADER_QUOTE = "Jatoba 18 Standard/Enterprise (18.02.2026, PostgreSQL 18)"


def _data() -> dict:
    return json.loads((FIXTURES / "evaluate_inputs.json").read_text(encoding="utf-8"))


def _param(data: dict) -> CatalogRow:
    return CatalogRow(**data["param"])


def _param_with(title: str, rule: str = "Подтвердить наличие свойства.") -> CatalogRow:
    data = _data()["param"]
    return CatalogRow(**{**data, "title": title, "rule": rule})


def _chunks(data: dict) -> list[Chunk]:
    return [Chunk(**c) for c in data["chunks"]]


def _chunks_with_header(data: dict) -> list[Chunk]:
    """Корпус, где единственный доказательный фрагмент — шапка документа."""
    return [
        *[Chunk(**c) for c in data["chunks"]],
        Chunk(
            chunk_id="chunk_header",
            chunk_no=9,
            text=HEADER_QUOTE,
            heading_path="",
            context_prefix="",
            content_hash="hash_header",
        ),
    ]


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


def _evaluate(out: LLMCellOutput, *, chunks: list[Chunk] | None = None):  # noqa: ANN202
    data = _data()
    return evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=_ScriptedEvaluator(out),
        chunks=chunks if chunks is not None else _chunks(data),
    )


def _ok(source_id: str, quote: str, **extra: object) -> LLMCellOutput:
    return LLMCellOutput(
        status=Status.OK,
        justification="Свойство подтверждено документацией.",
        evidence=[LLMEvidence(source_id=source_id, quote=quote)],
        **extra,  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Чистая функция: три состояния и морфология
# ---------------------------------------------------------------------------


def test_subject_stems_ignore_only_stopwords_and_short_words() -> None:
    """Стемы: первые 5 символов, служебные слова и короткие слова отброшены."""
    param = _param_with("Оптимизация запросов для БД")
    measured = support_subject_overlap(param, ["любая цитата"])
    assert measured is not None
    # «для» — стоп-слово, «БД» короче 4 букв, «запросов» → «запро».
    assert measured.subject_stems == frozenset({"оптим", "запро"})


def test_morphology_case_and_yo_are_normalized() -> None:
    """«Оптимизация» ~ «оптимизации», «SLRU» ~ «slru», «расчёт» ~ «расчет»."""
    assert support_subject_overlap(
        _param_with("Оптимизация запросов"), ["оптимизации запросов"]
    ) == support_subject_overlap(_param_with("Оптимизация запросов"), ["Оптимизация"])

    slru = support_subject_overlap(_param_with("Кэш SLRU"), ["slru кэш"])
    assert slru is not None
    assert slru.state == SUPPORT_STATE_SUPPORTED

    yo = support_subject_overlap(_param_with("Расчёт стоимости"), ["расчет стоимости"])
    assert yo is not None
    assert yo.state == SUPPORT_STATE_SUPPORTED


def test_empty_quotes_is_no_quotes_state() -> None:
    """Цитат нет — не наша аномалия (её ловит I22), состояние ``no_quotes``."""
    measured = support_subject_overlap(_param_with("Оптимизация запросов"), [])
    assert measured is not None
    assert measured.state == SUPPORT_STATE_NO_QUOTES
    assert measured.note() is None


def test_all_quotes_unrelated_is_unrelated_support() -> None:
    """Ни одна цитата не про предмет ⇒ ``unrelated_support`` с обеими цитатами в счёте."""
    measured = support_subject_overlap(
        _param_with("Оптимизация запросов"), [HEADER_QUOTE, "Экспорт данных в CSV."]
    )
    assert measured is not None
    assert measured.state == SUPPORT_STATE_UNRELATED
    assert (measured.quoted, measured.unrelated, measured.related) == (2, 2, 0)
    assert measured.note() == f"{SUPPORT_NOT_ABOUT_SUBJECT}: 2/2 цитат не упоминают предмет"


def test_one_related_quote_is_enough() -> None:
    """Хватит одной цитаты про предмет: состояние — факт пересечения, без порога доли."""
    measured = support_subject_overlap(
        _param_with("Оптимизация запросов"), [HEADER_QUOTE, "оптимизация запросов в ядре"]
    )
    assert measured is not None
    assert measured.state == SUPPORT_STATE_SUPPORTED
    assert (measured.quoted, measured.unrelated) == (2, 1)
    assert measured.note() is None


def test_unmeasurable_subject_is_none_not_supported() -> None:
    """Предмет без слов ≥4 букв — ``None``: «не измерено» не выдаётся за измеренное."""
    assert support_subject_overlap(_param_with("БД", rule="для"), [HEADER_QUOTE]) is None


# ---------------------------------------------------------------------------
# Сквозной путь: вердикт не меняется, сигнал появляется
# ---------------------------------------------------------------------------


def test_header_as_only_quote_keeps_ok_and_flags_review() -> None:
    """Шапка документа как единственная цитата: ``✅`` сохранён, но помечен на ревью."""
    outcome = _evaluate(
        _ok("chunk_header", HEADER_QUOTE), chunks=_chunks_with_header(_data())
    )

    assert outcome.cell.state is CellState.EVALUATED
    # Вердикт НЕ меняется: замер — величина, а не демотирование.
    assert outcome.cell.status is Status.OK
    assert len(outcome.cell.evidence) == 1

    assert outcome.support_overlap is not None
    assert outcome.support_overlap.state == SUPPORT_STATE_UNRELATED
    assert (outcome.support_overlap.quoted, outcome.support_overlap.unrelated) == (1, 1)

    notes = [
        n for n in outcome.cell.validation_notes if n.startswith(SUPPORT_NOT_ABOUT_SUBJECT)
    ]
    assert notes == [f"{SUPPORT_NOT_ABOUT_SUBJECT}: 1/1 цитат не упоминают предмет"]
    # Человеческий сигнал на ячейке (не автоматическое решение).
    assert outcome.cell.review_reason == notes[0]


def test_meaningful_quote_is_supported_and_silent() -> None:
    """Осмысленная цитата ⇒ ``supported``, заметок и причины ревью нет."""
    outcome = _evaluate(
        _ok("chunk_a2", "Свойство Альфа доступно без дополнительной настройки.")
    )

    assert outcome.cell.status is Status.OK
    assert outcome.support_overlap is not None
    assert outcome.support_overlap.state == SUPPORT_STATE_SUPPORTED
    assert not [
        n for n in outcome.cell.validation_notes if n.startswith(SUPPORT_NOT_ABOUT_SUBJECT)
    ]
    assert outcome.cell.review_reason is None


def test_partial_with_unrelated_quote_is_measured_but_not_review_reason() -> None:
    """``⚠️`` с посторонней цитатой тоже попадает в замер; ``review_reason`` — только ``✅``."""
    outcome = _evaluate(
        LLMCellOutput(
            status=Status.PARTIAL,
            justification="Частично подтверждено.",
            evidence=[LLMEvidence(source_id="chunk_header", quote=HEADER_QUOTE)],
        ),
        chunks=_chunks_with_header(_data()),
    )

    assert outcome.cell.status is Status.PARTIAL
    assert outcome.support_overlap is not None
    assert outcome.support_overlap.state == SUPPORT_STATE_UNRELATED
    assert [
        n for n in outcome.cell.validation_notes if n.startswith(SUPPORT_NOT_ABOUT_SUBJECT)
    ]
    assert outcome.cell.review_reason is None


def test_unrelated_support_does_not_displace_contradiction_reason() -> None:
    """Две причины ревью складываются: противоречие не теряется из-за второй."""
    outcome = _evaluate(
        _ok(
            "chunk_header",
            HEADER_QUOTE,
            denial_strength="explicit",
            denial_reason="вендор отрицает",
        ),
        chunks=_chunks_with_header(_data()),
    )

    reason = outcome.cell.review_reason or ""
    assert "denial_strength=explicit" in reason
    assert SUPPORT_NOT_ABOUT_SUBJECT in reason


def test_absence_verdict_is_not_measured() -> None:
    """У ``❓``/``❌`` цитата — наблюдение, а не поддержка: замер не применяется."""
    outcome = _evaluate(
        LLMCellOutput(
            status=Status.UNKNOWN,
            justification="Установить не удалось.",
        )
    )

    assert outcome.cell.status is Status.UNKNOWN
    assert outcome.support_overlap is None


def test_support_overlap_counts_aggregates_by_status() -> None:
    """Счёт по записям Ш6: читается из ``validation_notes`` и делится по статусам."""
    ok_cell = _evaluate(
        _ok("chunk_header", HEADER_QUOTE), chunks=_chunks_with_header(_data())
    ).cell
    clean_cell = _evaluate(
        _ok("chunk_a2", "Свойство Альфа доступно без дополнительной настройки.")
    ).cell
    partial_cell = _evaluate(
        LLMCellOutput(
            status=Status.PARTIAL,
            justification="Частично подтверждено.",
            evidence=[LLMEvidence(source_id="chunk_header", quote=HEADER_QUOTE)],
        ),
        chunks=_chunks_with_header(_data()),
    ).cell

    counts = support_overlap_counts([ok_cell, clean_cell, partial_cell])

    assert counts["cells_total"] == 3
    assert counts["cells_flagged"] == 2
    assert counts["by_status"] == {"⚠️": 1, "✅": 1}


def test_support_overlap_scan_covers_inherited_derived_quotes() -> None:
    """Замер по прогону видит и производное доказательство: записи Ш6 его не несут.

    Живой прогон дал 8 из 9 ложных ``✅`` именно на ячейках наследования
    (``source_kind=inheritance``): их единственное «доказательство» — ``derived``-цитата
    шапки документа. Счёт по ``validation_notes`` такие ячейки не видит, поэтому агрегат
    отчёта обязан идти через :func:`support_overlap_scan`.
    """
    data = _data()
    param = _param(data)
    ok_cell = _evaluate(
        _ok("chunk_header", HEADER_QUOTE), chunks=_chunks_with_header(data)
    ).cell
    inherited = ok_cell.model_copy(
        update={
            "source_kind": SourceKind.INHERITANCE,
            "validation_notes": [],
            "review_reason": None,
        }
    )
    quotes = {
        (inherited.product_id, inherited.param_id): [HEADER_QUOTE],
    }

    scan = support_overlap_scan([inherited], {param.param_id: param}, quotes)

    assert scan["cells_considered"] == 1
    assert scan["cells_flagged"] == 1
    assert scan["by_status"] == {"✅": 1}
    assert scan["cells"] == [
        {
            "product_id": str(inherited.product_id),
            "param_id": param.param_id,
            "status": "✅",
            "source_kind": "inheritance",
            "quoted": 1,
            "unrelated": 1,
            "subject_stems": ["альфа", "парам"],
        }
    ]
