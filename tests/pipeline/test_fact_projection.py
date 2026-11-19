"""Проекция вердиктов оценщика в факты (``core/pipeline/fact_projection.py``).

Проверки держат ровно то, ради чего проекция существует: правило «вердикт → факт»
механическое, отказы (нет цитаты / нет адреса / нет хеша) **видимы**, а не превращаются в
молчаливый ``present`` (I15/I22), и провенанс наблюдения называет прибор.

Ячейки собираются здесь, а не читаются из артефакта: проекция — функция от вердикта, и
источник вердикта (карта, recon, будущий прибор) её не касается. Часть ячеек строится
``model_construct``: проекция обязана пережить ячейку, нарушающую I1/I2 (историческая
строка, дефект оценщика) — именно такие случаи и порождают видимые отказы ниже.
"""

from __future__ import annotations

from typing import Any

from core.domain.enums import (
    CellState,
    FactReview,
    FactStatus,
    Importance,
    ParamType,
    Status,
)
from core.domain.models import CatalogRow, Cell, LLMEvidence
from core.pipeline.fact_projection import (
    FACT_PROJECTION_SCHEMA,
    NOTE_FAILED,
    NOTE_HASH_MISSING,
    NOTE_MISSING_CELL,
    NOTE_PARTIAL,
    NOTE_QUOTE_MISSING,
    NOTE_QUOTE_WITHOUT_URL,
    NOTE_UNKNOWN,
    facts_from_cells,
)

OBSERVED_BY = "llm:fake@rev1:terms+phrases"
BUILD_ID = "ref_postgresql_18_deadbeef"
OBSERVED_AT = "2026-11-19T12:00:00+00:00"
URL = "https://docs.example/18/jsonb.html"
HASH = "a" * 64


def _param(param_id: str) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=f"Title {param_id}",
        module="m",
        group="g",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule="rule text",
    )


def _cell(
    param_id: str,
    status: Status | None,
    *,
    evidence: list[LLMEvidence] | None = None,
    absence_as_of: str | None = None,
    state: CellState = CellState.EVALUATED,
) -> Cell:
    """Ячейка оценщика: ``model_construct`` — домен не пропустил бы часть этих состояний.

    ``✅`` без evidence нарушает I1, ``❌`` без ``EvidenceScope`` — I2; проекция обязана
    отвечать на них видимым отказом, а не падать, поэтому тест строит их напрямую.
    """
    return Cell.model_construct(
        run_id="recon:upstream-pg:18",
        product_id="upstream",
        param_id=param_id,
        state=state,
        status=status,
        evidence=list(evidence or []),
        absence_evidence=[],
        validation_notes=[],
        absence_as_of=absence_as_of,
    )


def _evidence(quote: str = "jsonb supports expression indexes") -> list[LLMEvidence]:
    return [LLMEvidence(source_id="c1", quote=quote)]


def _lookup(source_id: str) -> tuple[str | None, str | None]:
    """Чанк корпуса: адрес и хеш текста (как их отдаёт ``iter_reference_chunks``)."""
    return (URL, HASH) if source_id == "c1" else (None, None)


def _project(cells: dict[str, Cell], params: list[str]) -> list[Any]:
    return facts_from_cells(
        cells=cells,
        params=[_param(pid) for pid in params],
        baseline_id="upstream-pg",
        version="18",
        corpus_build_id=BUILD_ID,
        observed_at=OBSERVED_AT,
        observed_by=OBSERVED_BY,
        chunk_lookup=_lookup,
    )


def test_schema_is_declared() -> None:
    assert FACT_PROJECTION_SCHEMA == "fact-projection/1"


def test_ok_cell_becomes_present_with_quote_and_hash() -> None:
    """``✅`` → ``present``: цитата — из свидетельства, адрес и хеш — у чанка."""
    facts = _project({"p1": _cell("p1", Status.OK, evidence=_evidence())}, ["p1"])
    (fact,) = facts
    assert fact.status is FactStatus.PRESENT
    assert fact.quote == "jsonb supports expression indexes"
    assert fact.url == URL
    assert fact.content_hash == HASH
    assert fact.note is None
    assert fact.corpus_build_id == BUILD_ID


def test_partial_cell_becomes_present_with_note() -> None:
    """``⚠️`` → ``present``, но частичность названа в ``note``: это не то же, что ``✅``."""
    facts = _project({"p1": _cell("p1", Status.PARTIAL, evidence=_evidence())}, ["p1"])
    (fact,) = facts
    assert fact.status is FactStatus.PRESENT
    assert fact.note == NOTE_PARTIAL


def test_absent_cell_carries_the_date_of_absence() -> None:
    """``❌`` → ``absent`` с датой носителя в ``note`` (I2d: отсутствие датировано)."""
    facts = _project(
        {"p1": _cell("p1", Status.ABSENT, absence_as_of="2026-11-18")}, ["p1"]
    )
    (fact,) = facts
    assert fact.status is FactStatus.ABSENT
    assert fact.note == "llm_absent; as_of=2026-11-18"


def test_absent_cell_without_date_says_unknown() -> None:
    """Даты нет — ``as_of=unknown``, а не пустая пометка: «неизвестно» — тоже сведение."""
    facts = _project({"p1": _cell("p1", Status.ABSENT)}, ["p1"])
    (fact,) = facts
    assert fact.status is FactStatus.ABSENT
    assert fact.note == "llm_absent; as_of=unknown"


def test_unknown_cell_becomes_unclear() -> None:
    """``❓`` → ``unclear``: неизвестность не становится ни ``present``, ни ``absent`` (I12)."""
    facts = _project({"p1": _cell("p1", Status.UNKNOWN)}, ["p1"])
    (fact,) = facts
    assert fact.status is FactStatus.UNCLEAR
    assert fact.note == NOTE_UNKNOWN


def test_failed_cell_is_named_as_instrument_failure_not_as_unknown() -> None:
    """``failed`` (``status=null``, I4) → ``unclear`` **с отдельной** пометкой.

    «Прибор отказал» и «прибор посмотрел и не нашёл» — разные наблюдения, и сливать их
    значило бы выдать отказ инструмента за свойство документации. Обе стороны дают
    ``unclear`` (отсутствие вердикта — не отсутствие функции), но причина названа.
    """
    facts = _project({"p1": _cell("p1", None, state=CellState.FAILED)}, ["p1"])
    (fact,) = facts
    assert fact.status is FactStatus.UNCLEAR
    assert fact.note == NOTE_FAILED
    assert fact.note != NOTE_UNKNOWN


def test_cell_missing_from_the_set_becomes_unclear_with_note() -> None:
    """Параметра нет среди вердиктов — «вердикта нет», а не «документация молчит»."""
    facts = _project({}, ["p1"])
    (fact,) = facts
    assert fact.status is FactStatus.UNCLEAR
    assert fact.note == NOTE_MISSING_CELL


def test_ok_cell_without_quote_is_visible_unclear() -> None:
    """``✅`` без непустой цитаты: ``present`` без доказательства не публикуется (I22)."""
    for evidence in ([], [LLMEvidence(source_id="c1", quote="   ")]):
        facts = _project({"p1": _cell("p1", Status.OK, evidence=evidence)}, ["p1"])
        (fact,) = facts
        assert fact.status is FactStatus.UNCLEAR
        assert fact.note == NOTE_QUOTE_MISSING


def test_present_without_url_is_visible_unclear() -> None:
    """Цитата без адреса не резолвится (I15) — ``unclear``, а не молчаливый ``present``.

    Адрес пробуется дважды: у свидетельства и у самого чанка; если нет ни там, ни там —
    это названный отказ.
    """
    facts = facts_from_cells(
        cells={"p1": _cell("p1", Status.OK, evidence=_evidence())},
        params=[_param("p1")],
        baseline_id="upstream-pg",
        version="18",
        corpus_build_id=BUILD_ID,
        observed_at=OBSERVED_AT,
        observed_by=OBSERVED_BY,
        chunk_lookup=lambda _source_id: (None, HASH),
    )
    (fact,) = facts
    assert fact.status is FactStatus.UNCLEAR
    assert fact.note == NOTE_QUOTE_WITHOUT_URL
    assert fact.url is None


def test_present_without_hash_is_visible_unclear() -> None:
    """Хеша текста нет — цитата не привязана к тексту, значит наследовать нечем."""
    facts = facts_from_cells(
        cells={"p1": _cell("p1", Status.OK, evidence=_evidence())},
        params=[_param("p1")],
        baseline_id="upstream-pg",
        version="18",
        corpus_build_id=BUILD_ID,
        observed_at=OBSERVED_AT,
        observed_by=OBSERVED_BY,
        chunk_lookup=lambda _source_id: (URL, None),
    )
    (fact,) = facts
    assert fact.status is FactStatus.UNCLEAR
    assert fact.note == NOTE_HASH_MISSING


def test_url_falls_back_to_the_chunk_when_evidence_has_none() -> None:
    """Адрес свидетельства пуст, но чанк его знает — факт остаётся ``present``."""
    facts = facts_from_cells(
        cells={"p1": _cell("p1", Status.OK, evidence=_evidence())},
        params=[_param("p1")],
        baseline_id="upstream-pg",
        version="18",
        corpus_build_id=BUILD_ID,
        observed_at=OBSERVED_AT,
        observed_by=OBSERVED_BY,
        chunk_lookup=_lookup,
    )
    (fact,) = facts
    assert fact.status is FactStatus.PRESENT
    assert fact.url == URL


def test_observed_by_is_the_same_for_every_fact() -> None:
    """Провенанс у **всех** фактов проекции, включая ``absent``/``unclear``."""
    cells = {
        "p1": _cell("p1", Status.OK, evidence=_evidence()),
        "p2": _cell("p2", Status.ABSENT),
        "p3": _cell("p3", Status.UNKNOWN),
    }
    facts = _project(cells, ["p1", "p2", "p3", "p4"])
    assert {fact.observed_by for fact in facts} == {OBSERVED_BY}


def test_fact_order_is_the_catalog_order() -> None:
    """Порядок фактов — порядок ``params``: повторная проекция даёт тот же файл (R7)."""
    cells = {
        "p1": _cell("p1", Status.OK, evidence=_evidence()),
        "p2": _cell("p2", Status.UNKNOWN),
    }
    facts = _project(cells, ["p2", "p1"])
    assert [fact.param_id for fact in facts] == ["p2", "p1"]


def test_review_is_always_pending() -> None:
    """Прибор не подписывает за человека: ``review=pending`` у всех фактов."""
    cells = {
        "p1": _cell("p1", Status.OK, evidence=_evidence()),
        "p2": _cell("p2", Status.ABSENT),
    }
    facts = _project(cells, ["p1", "p2", "p3"])
    assert {fact.review for fact in facts} == {FactReview.PENDING}
    assert all(fact.reviewed_by is None for fact in facts)
