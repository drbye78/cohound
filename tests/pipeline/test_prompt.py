"""Тесты механической компиляции промпта оценщика (T9.1, §7.1/§5).

Проверяют: механическую сборку (rule/absence_rule/сателлиты/чанки), рендер
``[source: chunk_id]``, детерминированный порядок и порядко-независимый ``prompt_hash``,
``render_queries`` (S13), I18-гард (нет инструкций искать; работает только по чанкам,
без URL), запрет N/A (I7), идемпотентное сохранение ``prompts/{hash}.json`` и
JSON-раундтрип ``CompiledPrompt``. Фикстуры категорийно-нейтральны (§1.3).
"""

from __future__ import annotations

import json

import pytest

from core.domain.enums import (
    AbsenceRuleType,
    EvidenceState,
    Importance,
    Origin,
    ParamType,
    Relationship,
    SatelliteState,
)
from core.domain.models import CatalogRow, Satellite
from core.pipeline.prompt import (
    ABSENCE_GUIDANCE,
    TEMPLATE_VERSION,
    CompiledPrompt,
    compile_prompt,
    render_queries,
    save_prompt,
)
from core.ports.types import Chunk
from core.ports.vector_index import RetrievedChunk
from core.util.hashing import prompt_hash


# ---------------------------------------------------------------------------
# Фикстуры (нейтральные данные, §1.3)
# ---------------------------------------------------------------------------


def _param(
    *,
    role_binding: str | None = "role_x",
    absence_rule: AbsenceRuleType = AbsenceRuleType.EXPLICIT,
    concept_terms: dict[str, list[str]] | None = None,
) -> CatalogRow:
    return CatalogRow(
        param_id="param_a",
        title="Параметр A",
        module="m1",
        group="g1",
        importance=Importance.BASIC,
        param_type=ParamType.FACT,
        rule="Правило A",
        absence_rule=absence_rule,
        role_binding=role_binding,
        concept_terms=concept_terms or {},
    )


def _chunk(cid: str, text: str | None = None) -> Chunk:
    return Chunk(
        chunk_id=cid,
        chunk_no=0,
        text=text if text is not None else f"текст-{cid}",
        heading_path=f"H-{cid}",
        context_prefix=f"P-{cid}",
        content_hash=f"h-{cid}",
    )


def _sat(role: str, state: SatelliteState = SatelliteState.DRAFT) -> Satellite:
    return Satellite(role=role, state=state)


# ---------------------------------------------------------------------------
# Механическая сборка
# ---------------------------------------------------------------------------


def test_compiles_rule_absence_satellites_and_chunks() -> None:
    param = _param()
    sats = [_sat("role_x"), _sat("role_y")]
    compiled = compile_prompt(
        param=param,
        product_id="p1",
        chunks=[_chunk("c2"), _chunk("c1")],
        satellites=sats,
    )
    assert param.rule in compiled.text
    assert "explicit" in compiled.text
    assert ABSENCE_GUIDANCE[AbsenceRuleType.EXPLICIT] in compiled.text
    # релевантный сателлит присутствует, нерелевантный — отфильтрован
    assert "role=role_x" in compiled.text
    assert "role_y" not in compiled.text
    # чанки отрендерены
    assert "текст-c1" in compiled.text and "текст-c2" in compiled.text


def test_no_role_binding_includes_all_satellites() -> None:
    param = _param(role_binding=None)
    compiled = compile_prompt(
        param=param,
        product_id="p1",
        chunks=[_chunk("c1")],
        satellites=[_sat("role_x"), _sat("role_y")],
    )
    assert "role=role_x" in compiled.text
    assert "role=role_y" in compiled.text


def test_established_satellite_via_model_construct() -> None:
    # Полноценный approved-сателлит требует relationship_evidence и полей связи;
    # model_construct обходит валидатор, а компиляция обязана устоять (рендер контекста).
    approved = Satellite.model_construct(
        role="role_x",
        state=SatelliteState.APPROVED,
        component="comp1",
        origin=Origin.IN_HOUSE,
        relationship=Relationship.BUNDLED,
        evidence_state=EvidenceState.VERIFIED,
        relationship_evidence=None,
        rejected_candidates=[],
    )
    compiled = compile_prompt(
        param=_param(),
        product_id="p1",
        chunks=[_chunk("c1")],
        satellites=[approved],
    )
    assert "component=comp1" in compiled.text
    assert "origin=in_house" in compiled.text
    assert "relationship=bundled" in compiled.text
    assert "state=approved" in compiled.text


def test_absence_rule_override_changes_text_and_hash() -> None:
    param = _param(absence_rule=AbsenceRuleType.EXPLICIT)
    compiled = compile_prompt(
        param=param,
        product_id="p1",
        chunks=[_chunk("c1")],
        absence_rule=AbsenceRuleType.REGISTRY_ABSENCE,
    )
    assert "registry_absence" in compiled.text
    assert "авторитетная энумерация" in compiled.text
    # Скаляр охвата модели больше не предъявляется: вердикт по реестру ставит не она.
    assert "exhaustive" not in compiled.text
    assert compiled.prompt_hash == prompt_hash(
        TEMPLATE_VERSION, param.rule, "registry_absence", ["c1"]
    )


# ---------------------------------------------------------------------------
# Рендер чанков и детерминизм
# ---------------------------------------------------------------------------


def test_chunks_rendered_with_source_marker() -> None:
    compiled = compile_prompt(
        param=_param(),
        product_id="p1",
        chunks=[_chunk("c1", "тело один"), _chunk("c2", "тело два")],
    )
    assert "[source: c1]\nтело один" in compiled.text
    assert "[source: c2]\nтело два" in compiled.text


def test_chunks_ordered_by_chunk_id() -> None:
    compiled = compile_prompt(
        param=_param(),
        product_id="p1",
        chunks=[_chunk("c3"), _chunk("c1"), _chunk("c2")],
    )
    i1 = compiled.text.index("[source: c1]")
    i2 = compiled.text.index("[source: c2]")
    i3 = compiled.text.index("[source: c3]")
    assert i1 < i2 < i3
    assert compiled.chunk_ids == ["c1", "c2", "c3"]


def test_retrieved_chunk_is_accepted() -> None:
    rc = RetrievedChunk(
        chunk_id="c1",
        product_id="p1",
        text="векторный текст",
        content_hash="h1",
        heading_path="H",
        build_id="b1",
        score=0.9,
    )
    compiled = compile_prompt(param=_param(), product_id="p1", chunks=[rc])
    assert "[source: c1]\nвекторный текст" in compiled.text


def test_prompt_hash_order_independent() -> None:
    a = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c3"), _chunk("c1"), _chunk("c2")]
    )
    b = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c2"), _chunk("c3"), _chunk("c1")]
    )
    assert a.prompt_hash == b.prompt_hash
    assert a.text == b.text  # порядок нормализуется до рендера
    assert a.chunk_ids == b.chunk_ids == ["c1", "c2", "c3"]


def test_prompt_hash_matches_util_contract() -> None:
    param = _param()
    compiled = compile_prompt(
        param=param, product_id="p1", chunks=[_chunk("c2"), _chunk("c1")]
    )
    expected = prompt_hash(
        TEMPLATE_VERSION, param.rule, AbsenceRuleType.EXPLICIT.value, ["c1", "c2"]
    )
    assert compiled.prompt_hash == expected


def test_empty_chunks_still_compiles() -> None:
    param = _param()
    compiled = compile_prompt(param=param, product_id="p1", chunks=[])
    assert "(нет фрагментов)" in compiled.text
    assert compiled.prompt_hash == prompt_hash(
        TEMPLATE_VERSION, param.rule, AbsenceRuleType.EXPLICIT.value, []
    )


# ---------------------------------------------------------------------------
# render_queries (S13)
# ---------------------------------------------------------------------------


def test_render_queries_ru_then_en_and_truncates() -> None:
    param = _param(concept_terms={"en": ["e1", "e2"], "ru": ["r1", "r2"]})
    assert render_queries(param) == ["Параметр A", "r1", "r2"]
    assert render_queries(param, max_queries=10) == [
        "Параметр A",
        "r1",
        "r2",
        "e1",
        "e2",
    ]


def test_render_queries_package_order_within_language() -> None:
    param = _param(concept_terms={"ru": ["r2", "r1"]})
    assert render_queries(param, max_queries=10) == ["Параметр A", "r2", "r1"]


def test_render_queries_other_languages_after_ru_en() -> None:
    param = _param(concept_terms={"de": ["d1"], "ru": ["r1"]})
    assert render_queries(param, max_queries=10) == ["Параметр A", "r1", "d1"]


def test_render_queries_bounds() -> None:
    param = _param(concept_terms={"ru": ["r1"]})
    assert render_queries(param, max_queries=1) == ["Параметр A"]
    assert render_queries(param, max_queries=0) == []
    with pytest.raises(ValueError):
        render_queries(param, max_queries=-1)


def test_render_queries_collapses_duplicate_of_title() -> None:
    """Повтор заголовка в ``concept_terms`` не съедает слот бюджета ретривала.

    Замер по каталогу ``ru-pg-dbms``: автор кладёт полный заголовок параметра первым
    элементов ``concept_terms[lang]``, поэтому без схлопывания **119 из 119** параметров
    получали ``[title, title, ...]`` — один из трёх слотов уходил на повтор уже
    выполненного запроса.
    """
    param = _param(concept_terms={"ru": ["Параметр A", "r1", "r2"]})
    assert render_queries(param, max_queries=3) == ["Параметр A", "r1", "r2"]


def test_render_queries_collapses_duplicates_across_languages() -> None:
    """Один и тот же термин в ru и en — один запрос, а не два (регистр не различает)."""
    param = _param(concept_terms={"ru": ["xid"], "en": ["XID", "xid64"]})
    assert render_queries(param, max_queries=10) == ["Параметр A", "xid", "xid64"]


# ---------------------------------------------------------------------------
# I18 / I10 / I15 / I7 — гарантии текста промпта
# ---------------------------------------------------------------------------


def test_i18_no_external_lookup_instructions() -> None:
    compiled = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c1")]
    )
    low = compiled.text.lower()
    for token in ("search", "web", "tool"):
        assert token not in low, f"промпт не должен инструктировать искать: {token!r}"


def test_i18_works_only_from_chunks_and_no_urls() -> None:
    compiled = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c1")]
    )
    low = compiled.text.lower()
    assert "только" in low
    assert "url" in low
    assert "не генерируй" in low


def test_na_prohibited() -> None:
    compiled = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c1")]
    )
    assert "N/A" in compiled.text
    assert "недопустим" in compiled.text


def test_response_schema_reminder_present() -> None:
    compiled = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c1")]
    )
    for field in ("status", "justification", "evidence", "source_id", "quote"):
        assert field in compiled.text


# ---------------------------------------------------------------------------
# Сохранение и сериализация
# ---------------------------------------------------------------------------


def test_to_json_round_trips() -> None:
    compiled = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c2"), _chunk("c1")]
    )
    data = compiled.to_json()
    assert json.loads(json.dumps(data, ensure_ascii=False)) == data
    assert data["prompt_hash"] == compiled.prompt_hash
    assert data["template_version"] == TEMPLATE_VERSION
    assert data["cell_key"] == ["p1", "param_a"]
    assert data["chunk_ids"] == ["c1", "c2"]
    assert data["text"] == compiled.text


def test_save_prompt_writes_hash_named_file(tmp_path) -> None:
    compiled = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c1")]
    )
    path = save_prompt(compiled, tmp_path)
    assert path == tmp_path / f"{compiled.prompt_hash}.json"
    assert path.is_file()
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved == compiled.to_json()


def test_save_prompt_is_idempotent_and_immutable(tmp_path) -> None:
    compiled = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c1")]
    )
    path = save_prompt(compiled, tmp_path)
    path.write_text("SENTINEL", encoding="utf-8")
    again = save_prompt(compiled, tmp_path)
    assert again == path
    assert path.read_text(encoding="utf-8") == "SENTINEL"  # не перезаписан


def test_compiled_prompt_save_delegates(tmp_path) -> None:
    compiled = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c1")]
    )
    assert isinstance(compiled, CompiledPrompt)
    path = compiled.save(tmp_path)
    assert path.name == f"{compiled.prompt_hash}.json"


# ---------------------------------------------------------------------------
# Доступность ❌: адресат инструкции и версия шаблона (R1/R3)
# ---------------------------------------------------------------------------


def test_absent_is_a_legitimate_answer_only_with_direct_denial() -> None:
    """Шаблон не требует несуществующего поля и не подменяет незнание отсутствием.

    ``LLMCellOutput`` (``extra="forbid"``) не имеет полей про scope/absence-evidence,
    поэтому инструкция «``❌`` → absence-evidence» была **неисполнима**. Теперь правило
    честное и узкое: ``❌`` достижим только через объявленный ``explicit`` с цитатой,
    а релевантный-но-не-дословный фрагмент даёт ``❓``.
    """
    compiled = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c1")]
    )
    assert "absence-evidence" not in compiled.text
    assert "denial_strength" in compiled.text
    assert "непустой denial_reason" in compiled.text
    # v4: запрет выдуманного отсутствия назван как различение ВОПРОСА, а не как лозунг —
    # «❌ — заявление вендора, а не твой вывод из молчания».
    assert "не твой вывод" in compiled.text
    assert "Без такой цитаты ❌ запрещён" in compiled.text
    # v4: различение частичного утверждения от незнания — то, что v3 потерял (замер:
    # 15 ⚠️ → ❓ из 24), а v4 вернул на 9 из 15.
    assert "ЗАЯВЛЕНО ЧАСТИЧНО" in compiled.text
    assert "НЕ УСТАНОВЛЕНО" in compiled.text
    assert "КАК ВЫБИРАТЬ МЕЖДУ ⚠️ И ❓" in compiled.text


@pytest.mark.parametrize(
    "rule",
    [AbsenceRuleType.PARAMETER_REFERENCE, AbsenceRuleType.DISTRIBUTION_ABSENCE],
)
def test_absence_guidance_is_addressed_to_the_model_not_to_a_human(
    rule: AbsenceRuleType,
) -> None:
    """Гайд не предъявляет модели ни процедуру человека, ни несуществующее поле.

    Аттестация полноты — вопрос человека (I6/I7), а скаляр ``exhaustive`` модели
    недоступен: в форме ответа его нет. Гайд говорит только о том, что модель вправе
    заявить (``explicit``/``not_stated``), и называет человеческую роль как чужую.
    """
    guidance = ABSENCE_GUIDANCE[rule]
    assert "человек" in guidance
    assert "explicit" in guidance
    assert "source_scope" not in guidance
    assert "scope_attested_by" not in guidance
    # Конечный справочник не даёт модели права объявлять класс bounded.
    assert "bounded" in guidance

    compiled = compile_prompt(
        param=_param(absence_rule=rule),
        product_id="p1",
        chunks=[_chunk("c1")],
    )
    assert guidance in compiled.text


def test_template_version_is_v4_and_older_ones_are_preserved() -> None:
    """Смена текста шаблона — новая версия; старые файлы остаются провенансом прогонов."""
    from core.pipeline.prompt import TEMPLATES_DIR, _load_template

    assert TEMPLATE_VERSION == "evaluator_v4"
    v1 = TEMPLATES_DIR / "evaluator_v1.txt"
    v2 = TEMPLATES_DIR / "evaluator_v2.txt"
    v3 = TEMPLATES_DIR / "evaluator_v3.txt"
    v4 = TEMPLATES_DIR / "evaluator_v4.txt"
    assert v1.is_file(), "старый шаблон удалять нельзя: он провенанс прошлых прогонов"
    assert v2.is_file()
    assert v3.is_file()
    assert v4.is_file()
    assert _load_template("evaluator_v1") != _load_template(TEMPLATE_VERSION)
    # v1/v2 остаются байт-в-байт тем, чем были: hash прошлых прогонов не должен «поехать».
    assert "❌ → absence-evidence" in _load_template("evaluator_v1")
    assert "❌ — законный ответ" in _load_template("evaluator_v2")
