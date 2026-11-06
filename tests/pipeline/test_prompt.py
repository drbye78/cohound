"""Тесты механической компиляции промпта оценщика (T9.1, §7.1/§5).

Проверяют: механическую сборку (rule/absence_rule/сателлиты/чанки), рендер фрагмента
(структурная шапка ``[фрагмент N]``/``source_id``/``document``/``heading``/``context`` +
дословный текст), детерминированный порядок и порядко-независимый ``prompt_hash``,
``render_queries`` (S13), I18-гард (нет инструкций искать; работает только по чанкам,
без URL), запрет N/A (I7), правило языка цитаты и семантику пустого раздела ДАННЫЕ (v5),
идемпотентное сохранение ``prompts/{sha256(text)}.json`` и JSON-раундтрип ``CompiledPrompt``.
Фикстуры категорийно-нейтральны (§1.3).
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
from core.util.hashing import content_hash, prompt_hash

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


def _data_section(text: str) -> str:
    """Только раздел ДАННЫЕ: шапка шаблона тоже упоминает ``document:``/``context:``.

    Проверять отсутствие строки шапки надо в отрендеренных данных, а не во всём промпте:
    иначе проверка срабатывала бы на самом описании формата.
    """
    marker = "--- ДАННЫЕ ---"
    return text.split(marker, 1)[1] if marker in text else text


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


def test_chunks_rendered_with_structural_header() -> None:
    """C7: фрагмент несёт шапку (source_id/документ/заголовок) и дословный текст.

    Текст фрагмента обязан остаться **дословным** фрагментом промпта: цитатная сверка
    (I22, ``validate.py``) сравнивает цитату с чистым ``text`` чанка, и любая правка
    текста при рендере сделала бы законную цитату недоказуемой.
    """
    compiled = compile_prompt(
        param=_param(),
        product_id="p1",
        chunks=[_chunk("c1", "тело один"), _chunk("c2", "тело два")],
    )
    assert "[фрагмент 1]\nsource_id: c1\n" in compiled.text
    assert "[фрагмент 2]\nsource_id: c2\n" in compiled.text
    # Текст идёт после явной границы шапки и остаётся дословным.
    assert "текст:\nтело один" in compiled.text
    assert "текст:\nтело два" in compiled.text
    # Старый маркер ``[source: …]`` больше не печатается: шапка машинно-различима.
    assert "[source:" not in compiled.text


def test_chunk_header_shows_document_heading_and_context() -> None:
    """Документ и путь заголовков доходят до модели; контекст — отдельной строкой."""
    chunk = Chunk(
        chunk_id="prod:9f2ab1c3d4e5:7",
        chunk_no=7,
        text="Тело фрагмента.",
        heading_path="Администрирование > Резервное копирование",
        context_prefix="Администрирование — раздел про эксплуатацию.",
        content_hash="h7",
    )
    compiled = compile_prompt(param=_param(), product_id="prod", chunks=[chunk])
    assert "document: prod:9f2ab1c3d4e5\n" in compiled.text
    assert "heading: Администрирование > Резервное копирование\n" in compiled.text
    assert "context: Администрирование — раздел про эксплуатацию.\n" in compiled.text
    assert "текст:\nТело фрагмента." in compiled.text


def test_context_prefix_equal_to_heading_is_not_duplicated() -> None:
    """markdown-чанкер кладёт в ``context_prefix`` тот же путь: дубль не печатается."""
    chunk = Chunk(
        chunk_id="doc#1",
        chunk_no=1,
        text="Тело.",
        heading_path="A > B",
        context_prefix="A > B",
        content_hash="h",
    )
    compiled = compile_prompt(param=_param(), product_id="p1", chunks=[chunk])
    data = _data_section(compiled.text)
    assert "heading: A > B\n" in data
    assert "context:" not in data


def test_empty_heading_document_and_context_render_no_garbage() -> None:
    """Пустые поля шапки **не** дают строк: ни ``None``, ни ``[]``, ни пустых строк.

    Плоский текст не имеет заголовков (``heading_path=""``), а ``chunk_id`` может не
    нести номера фрагмента — тогда документ не выводится, а не выдумывается.
    """
    chunk = Chunk(
        chunk_id="doc",
        chunk_no=0,
        text="Плоский текст.",
        heading_path="",
        context_prefix="",
        content_hash="h",
    )
    compiled = compile_prompt(param=_param(), product_id="p1", chunks=[chunk])
    data = _data_section(compiled.text)
    assert "document:" not in data
    assert "heading:" not in data
    assert "context:" not in data
    assert "None" not in data
    assert "[]" not in data


def test_retrieved_chunk_carries_context_prefix_to_the_prompt() -> None:
    """``RetrievedChunk`` несёт ``context_prefix``: шапка таблицы видна оценщику.

    Порт получил поле в L1b-2 плана 18: строка таблицы без имён столбцов и продолжение
    листинга без языка — это фрагменты, которые оценщик не может прочитать по смыслу.
    ``text`` при этом остаётся дословным: контекст печатается отдельной строкой шапки
    (I22 сверяет цитату с ``text``, а не с шапкой).
    """
    rc = RetrievedChunk(
        chunk_id="prod:aa11:0",
        product_id="prod",
        text="| 1 | значение |",
        content_hash="h1",
        heading_path="Раздел",
        build_id="b1",
        score=0.9,
        context_prefix="Раздел\n| id | смысл |\n|---|---|",
    )
    compiled = compile_prompt(param=_param(), product_id="prod", chunks=[rc])
    data = _data_section(compiled.text)
    assert "source_id: prod:aa11:0\n" in data
    assert "document: prod:aa11\n" in data
    assert "heading: Раздел\n" in data
    assert "context: Раздел\n| id | смысл |\n|---|---|\n" in data
    assert "текст:\n| 1 | значение |" in data


def test_chunks_ordered_by_chunk_id() -> None:
    compiled = compile_prompt(
        param=_param(),
        product_id="p1",
        chunks=[_chunk("c3"), _chunk("c1"), _chunk("c2")],
    )
    i1 = compiled.text.index("source_id: c1")
    i2 = compiled.text.index("source_id: c2")
    i3 = compiled.text.index("source_id: c3")
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
    assert "source_id: c1\n" in compiled.text
    assert "текст:\nвекторный текст" in compiled.text


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


def test_save_prompt_writes_content_addressed_file(tmp_path) -> None:
    """Адрес файла — ``sha256(текста)``, а не ``prompt_hash`` §5.

    Формула §5 не покрывает product/title/текст чанков, поэтому два разных промпта
    законно делят ``prompt_hash``; адрес по hash терял второй промпт. ``prompt_hash``
    остаётся полем файла (см. ниже).
    """
    compiled = compile_prompt(
        param=_param(), product_id="p1", chunks=[_chunk("c1")]
    )
    path = save_prompt(compiled, tmp_path)
    assert path == tmp_path / f"{content_hash(compiled.text)}.json"
    assert path.is_file()
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved == compiled.to_json()
    assert saved["prompt_hash"] == compiled.prompt_hash


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
    assert path.name == f"{content_hash(compiled.text)}.json"


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


def test_template_version_is_v5_and_older_ones_are_preserved() -> None:
    """Смена текста шаблона — новая версия; старые файлы остаются провенансом прогонов."""
    from core.pipeline.prompt import TEMPLATES_DIR, _load_template

    assert TEMPLATE_VERSION == "evaluator_v5"
    v1 = TEMPLATES_DIR / "evaluator_v1.txt"
    v2 = TEMPLATES_DIR / "evaluator_v2.txt"
    v3 = TEMPLATES_DIR / "evaluator_v3.txt"
    v4 = TEMPLATES_DIR / "evaluator_v4.txt"
    v5 = TEMPLATES_DIR / "evaluator_v5.txt"
    assert v1.is_file(), "старый шаблон удалять нельзя: он провенанс прошлых прогонов"
    assert v2.is_file()
    assert v3.is_file()
    assert v4.is_file()
    assert v5.is_file()
    assert _load_template("evaluator_v1") != _load_template(TEMPLATE_VERSION)
    # v1/v2 остаются байт-в-байт тем, чем были: hash прошлых прогонов не должен «поехать».
    assert "❌ → absence-evidence" in _load_template("evaluator_v1")
    assert "❌ — законный ответ" in _load_template("evaluator_v2")
    # v4 не переписан: версии шаблонов — провенанс, а не рабочая площадка.
    assert "КАК ВЫБИРАТЬ МЕЖДУ ⚠️ И ❓" in _load_template("evaluator_v4")


# ---------------------------------------------------------------------------
# C7: шапка фрагмента не ломает цитатную сверку; шаблон описывает новый вход
# ---------------------------------------------------------------------------


def test_chunk_text_stays_a_verbatim_substring_of_the_prompt() -> None:
    """Цитата, взятая из чанка, остаётся подстрокой промпта (I22 не ломается рендером)."""
    text = "Резервное копирование   доступно\nтолько в полной версии."
    chunk = Chunk(
        chunk_id="prod:aa11:0",
        chunk_no=0,
        text=text,
        heading_path="Резервное копирование",
        context_prefix="Резервное копирование",
        content_hash="h",
    )
    compiled = compile_prompt(param=_param(), product_id="prod", chunks=[chunk])
    assert text in compiled.text


def test_template_explains_header_and_forbids_quoting_it() -> None:
    """Модель должна знать, что шапка — метаданные: иначе она процитирует ``heading:``."""
    compiled = compile_prompt(param=_param(), product_id="p1", chunks=[_chunk("c1")])
    assert "source_id:" in compiled.text
    assert "текст:" in compiled.text
    assert "ШАПКА — МЕТАДАННЫЕ" in compiled.text
    assert "цитировать нельзя" in compiled.text


def test_template_requires_quoting_in_the_fragment_language() -> None:
    """Правило языка цитаты: перевод цитаты = I22-провал = retry = ``failed``.

    Ни v1–v4 такого правила не содержали, а у нас русский вопрос над англоязычной
    документацией — ровно та ситуация, где модель склонна переводить.
    """
    compiled = compile_prompt(param=_param(), product_id="p1", chunks=[_chunk("c1")])
    assert "ЯЗЫК ЦИТАТЫ" in compiled.text
    assert "на языке фрагмента" in compiled.text
    assert "перевод" in compiled.text.lower()
    # Шаблон не разрешает перевод даже когда вопрос на другом языке.
    assert "не повод переводить" in compiled.text


def test_template_specifies_empty_data_section_semantics() -> None:
    """Пустой раздел ДАННЫЕ: семантика названа в шаблоне, а не только в коде.

    Код уже не вызывает модель на пустом срезе (``no_fragments``), но шаблон обязан
    задавать тот же ответ и для случая, когда пустой блок всё-таки дошёл до модели
    (например, ручной вызов) — иначе поведение зависело бы от того, кто зовёт.
    """
    compiled = compile_prompt(param=_param(), product_id="p1", chunks=[])
    assert "(нет фрагментов)" in compiled.text
    assert "ЕСЛИ РАЗДЕЛ ДАННЫЕ ПУСТ" in compiled.text
    assert "no_fragments" in compiled.text


@pytest.mark.parametrize("rule", list(AbsenceRuleType))
def test_rendered_guidance_has_no_escaped_quotes(rule: AbsenceRuleType) -> None:
    """Бэкслешей в guidance нет: печатается ``denial_strength="explicit"``.

    Проверка идёт по ``repr``/``ascii``, а не по выводу шелла: терминал искажает
    обратные слэши, и «на глаз» дефект выглядел исправленным.
    """
    assert '\\"' not in repr(ABSENCE_GUIDANCE[rule])
    assert '\\"' not in ascii(ABSENCE_GUIDANCE[rule])
    compiled = compile_prompt(
        param=_param(absence_rule=rule), product_id="p1", chunks=[_chunk("c1")]
    )
    assert '\\"' not in repr(compiled.text), rule
    assert 'denial_strength=\\"explicit\\"' not in ascii(compiled.text)


def test_prompt_template_identity_changes_with_v5(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``prompt_template`` = имя@hash условий → v5 даёт новую идентичность прогона.

    Это версионная граница ``execution_versions``: шаблон — условие измерения (§7.1/§7.4),
    поэтому его смена обязана дать новый ``run_id``, а не правку текущего прогона.
    Проверяется вызовом: та же формула под ``v4`` даёт другое значение.
    """
    from core.pipeline import prompt as prompt_module
    from core.pipeline.run_identity import _prompt_template_identity

    identity = _prompt_template_identity()
    assert identity.startswith("evaluator_v5@")
    monkeypatch.setattr(prompt_module, "TEMPLATE_VERSION", "evaluator_v4")
    assert _prompt_template_identity() != identity
    assert _prompt_template_identity().startswith("evaluator_v4@")

