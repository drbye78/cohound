"""Схема выхода оценщика (``LLMCellOutput``, §7): терпимость к полям отрицания.

Живая проба дала ``✅`` с ``denial_strength=explicit`` — прежний валидатор отвергал ответ
целиком, и ячейка по I4 становилась ``failed``/``status=None``: позитивный вердикт с
цитатой терялся из-за шумового поля. Здесь фиксируется новое правило: вердикт наличия не
оспаривается, а отбрасывание полей делает потребитель (``core.pipeline.evaluate``).
Конструктор при этом **не ослаблен**: I1/I7 на месте.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from core.domain import LLMCellOutput, LLMEvidence, Status


def _evidence() -> list[LLMEvidence]:
    return [LLMEvidence(source_id="c1", quote="цитата")]


def test_ok_with_noise_denial_strength_accepted() -> None:
    """``not_stated`` при ``✅`` — шум провайдера, ответ принимается."""
    out = LLMCellOutput(
        status=Status.OK,
        justification="свойство подтверждено",
        evidence=_evidence(),
        denial_strength="not_stated",
        denial_reason="не заявлено",
    )
    assert out.status is Status.OK
    assert len(out.evidence) == 1


def test_ok_with_explicit_denial_accepted() -> None:
    """``explicit`` при ``✅`` — противоречие, но вердикт наличия не отвергается.

    Разрешение противоречия — предмет ревью (см. ``evaluate._present_status_denial``),
    а не повод потерять доказанное наличие.
    """
    out = LLMCellOutput(
        status=Status.OK,
        justification="свойство подтверждено",
        evidence=_evidence(),
        denial_strength="explicit",
        denial_reason="вендор отрицает",
    )
    assert out.status is Status.OK
    assert out.denial_strength is not None


def test_partial_with_denial_reason_only_accepted() -> None:
    """``⚠️`` + только ``denial_reason`` (без класса) — тоже шум, а не отказ формы."""
    out = LLMCellOutput(
        status=Status.PARTIAL,
        justification="частично",
        evidence=_evidence(),
        denial_reason="похоже, ограничено",
    )
    assert out.status is Status.PARTIAL


def test_denial_fields_survive_for_audit() -> None:
    """Поля не стираются схемой: артефакт ``raw`` показывает, что прислала модель."""
    out = LLMCellOutput(
        status=Status.OK,
        justification="есть",
        evidence=_evidence(),
        denial_strength="explicit",
    )
    assert out.model_dump()["denial_strength"] == "explicit"


def test_ok_without_evidence_still_rejected() -> None:
    """I1 не ослаблен: ``✅`` без evidence отвергается."""
    with pytest.raises(ValidationError):
        LLMCellOutput(status=Status.OK, justification="x", evidence=[])


def test_na_still_rejected() -> None:
    """I7 не ослаблен: N/A на выходе LLM запрещён."""
    with pytest.raises(ValidationError):
        LLMCellOutput(status=Status.NOT_APPLICABLE, justification="x")


def test_absent_without_denial_fields_is_schema_valid() -> None:
    """``❌`` без полей отрицания схема принимает — вердикт решает не она.

    Это фактическое поведение кода (в ``_check`` нет правила «``❌`` требует поля»):
    недоказанное «нет» понижается до ``❓`` механизмом
    :func:`core.pipeline.evaluate._resolve_denial`, а не отказом формы. Жёсткий отказ
    здесь дал бы ``failed`` и потерял наблюдение — ровно тот дефект, который чинится для
    ``✅``. Тест фиксирует выбор явно, чтобы «ужесточение» не проехало незамеченным.
    """
    out = LLMCellOutput(status=Status.ABSENT, justification="не найдено", evidence=[])
    assert out.denial_strength is None


def test_absent_with_authoritative_denial_rejected() -> None:
    """Модель не вправе объявлять авторитет: ``authoritative`` не парсится."""
    with pytest.raises(ValidationError):
        LLMCellOutput(
            status=Status.ABSENT,
            justification="нет",
            denial_strength="authoritative",  # type: ignore[arg-type]
        )


def test_raw_json_from_live_probe_is_accepted() -> None:
    """Тот же разбор, что делает адаптер (``model_validate_json``), на форме живой пробы.

    Живая проба ``evaluator_v3`` дала ровно такой ответ (``✅`` + отрицание) и получила
    ``EvaluationError`` → ``failed``. Теперь этот JSON обязан разбираться: иначе ремонт
    существует только в конструкторе, а на живом пути по-прежнему теряет ячейку.
    """
    raw = (
        '{"status": "✅", "justification": "Свойство подтверждено документацией.",'
        ' "evidence": [{"source_id": "chunk_a2", "quote": "цитата"}],'
        ' "denial_strength": "explicit", "denial_reason": "шум провайдера"}'
    )
    out = LLMCellOutput.model_validate_json(raw)
    assert out.status is Status.OK
    assert out.denial_strength is not None
