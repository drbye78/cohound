"""Тесты локальных инструментов оценщика (:mod:`core.pipeline.local_tools`).

Проверяется контракт исполнения, а не «похожесть» ответа:

* неизвестное имя, аргументы не по схеме и неизвестный адрес дают ``ok=False`` с
  причиной — **никогда** исключение: модель обязана увидеть причину и продолжить;
* T1 возвращает ровно фрагменты раздела (а не соседей по ``chunk_no``), T2 называет
  сработавший маркер, T4 — шапку и номера строк, T7 — присутствующие и отсутствующие
  термины плюс ограничитель;
* обрезка по объявленному бюджету **видима** (``meta["truncated"]``, ``reason``, строка в
  тексте), а не молчалива;
* ``specs()`` отдаёт только инструменты профиля и в объявленном порядке.

Корпус — фейковый: инструменты проверяются на своих ответах, окно в корпус — отдельно
(``tests/pipeline/test_tool_corpus.py``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from core.domain.doc_tables import ExtractedTable, TableRef
from core.domain.enums import Importance, ParamType
from core.domain.models import CatalogRow
from core.domain.tools import ToolInvocation, ToolProfile
from core.pipeline import section_lexicon
from core.pipeline.local_tools import TOOL_CHAR_BUDGETS, build_local_tools
from core.pipeline.section_lexicon import UNKNOWN_ROLE, classify_heading, lexicon_report
from core.ports.tools import LocalToolPort
from core.ports.types import Chunk


def _chunk(
    chunk_id: str,
    chunk_no: int,
    *,
    heading: str = "",
    text: str = "",
    table_ref: TableRef | None = None,
) -> Chunk:
    return Chunk(
        chunk_id=chunk_id,
        chunk_no=chunk_no,
        text=text or f"текст фрагмента {chunk_no}",
        heading_path=heading,
        context_prefix="",
        content_hash=f"hash{chunk_no}",
        table_ref=table_ref,
    )


class _FakeCorpus:
    """``ToolCorpusPort``-заглушка: разделы и таблицы заданы тестом."""

    def __init__(
        self,
        chunks: Sequence[Chunk],
        *,
        sections: dict[str, Sequence[Chunk]] | None = None,
        tables: tuple[ExtractedTable, ...] | None = None,
    ) -> None:
        self._chunks = {chunk.chunk_id: chunk for chunk in chunks}
        self._sections = sections or {}
        self._tables = tables

    def chunk(self, chunk_id: str) -> Chunk | None:
        return self._chunks.get(chunk_id)

    def document_chunks(self, chunk_id: str) -> Sequence[Chunk]:
        return tuple(self._chunks.values())

    def section_chunks(self, chunk_id: str) -> Sequence[Chunk]:
        return self._sections.get(chunk_id, ())

    def document_tables(self, chunk_id: str) -> tuple[ExtractedTable, ...] | None:
        return self._tables


def _param(**overrides: Any) -> CatalogRow:
    payload: dict[str, Any] = {
        "param_id": "xlsx_001",
        "title": "Репликация",
        "module": "m",
        "group": "g",
        "importance": Importance.BASIC,
        "param_type": ParamType.FACT,
        "rule": "r",
    }
    payload.update(overrides)
    return CatalogRow(**payload)


def _tools(
    corpus: _FakeCorpus,
    *,
    names: tuple[str, ...] = (
        "expand_to_section",
        "section_role",
        "table_row",
        "subject_mention",
    ),
    param: CatalogRow | None = None,
):
    return build_local_tools(
        profile=ToolProfile(name="test", tools=names, max_calls=4),
        corpus=corpus,
        param=param or _param(),
    )


# -- профиль и схема вызова ---------------------------------------------- #


def test_specs_are_only_profile_tools_in_declared_order() -> None:
    """``specs()`` — подмножество профиля в порядке объявления, не в порядке профиля."""
    corpus = _FakeCorpus([_chunk("p:d:0", 0)])

    tools = _tools(corpus, names=("table_row", "expand_to_section"))

    assert [spec.name for spec in tools.specs()] == ["expand_to_section", "table_row"]
    assert tools.specs()[0].parameters["required"] == ["chunk_id"]


def test_empty_profile_has_no_specs() -> None:
    """Пустой профиль — объявленный путь без инструментов."""
    tools = build_local_tools(
        profile=ToolProfile(name="none", tools=(), max_calls=0),
        corpus=_FakeCorpus([]),
        param=_param(),
    )

    assert tools.specs() == ()


def test_unknown_tool_in_profile_is_refused_at_build() -> None:
    """Опечатка в профиле — отказ, а не молча выброшенное имя (профиль входит в run_id)."""
    with pytest.raises(ValueError, match="неизвестные инструменты"):
        build_local_tools(
            profile=ToolProfile(name="bad", tools=("expand_to_sektion",), max_calls=1),
            corpus=_FakeCorpus([]),
            param=_param(),
        )


def test_unknown_tool_name_is_a_reason_not_an_exception() -> None:
    """Неизвестное имя — ``ok=False`` с причиной."""
    tools = _tools(_FakeCorpus([]))

    outcome = tools.invoke(ToolInvocation(name="drop_database", arguments={}))

    assert outcome.ok is False
    assert "неизвестный инструмент" in (outcome.reason or "")


def test_tool_outside_profile_is_refused() -> None:
    """Инструмент, которого нет в профиле, не исполняется (иначе run_id врёт)."""
    tools = _tools(_FakeCorpus([]), names=("section_role",))

    outcome = tools.invoke(ToolInvocation(name="expand_to_section", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is False
    assert "не включён в профиль" in (outcome.reason or "")


@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"chunk_id": 1},
        {"chunk_id": "   "},
        {"chunk_id": "p:d:0", "extra": "лишнее"},
    ],
)
def test_arguments_outside_schema_are_refused(arguments: dict[str, Any]) -> None:
    """Аргументы не по схеме — ``ok=False`` с причиной, а не исключение."""
    tools = _tools(_FakeCorpus([_chunk("p:d:0", 0)]))

    outcome = tools.invoke(ToolInvocation(name="expand_to_section", arguments=arguments))

    assert outcome.ok is False
    assert "expand_to_section" in (outcome.reason or "")


# -- T1: раздел ----------------------------------------------------------- #


def test_expand_to_section_returns_exactly_the_section() -> None:
    """T1: фрагменты раздела, а не соседи по ``chunk_no``."""
    first = _chunk("p:d:0", 0, heading="Ограничения")
    neighbour = _chunk("p:d:1", 1, heading="Возможности")
    third = _chunk("p:d:2", 2, heading="Ограничения")
    corpus = _FakeCorpus(
        [first, neighbour, third], sections={"p:d:0": (first, third)}
    )
    tools = _tools(corpus)

    outcome = tools.invoke(ToolInvocation(name="expand_to_section", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is True
    assert "source_id: p:d:0" in outcome.content
    assert "source_id: p:d:2" in outcome.content
    assert "source_id: p:d:1" not in outcome.content
    assert "фрагментов в разделе: 2" in outcome.content
    assert outcome.meta["chunks"] == 2
    assert outcome.meta["truncated"] is False
    assert outcome.meta["complete"] is True


def test_expand_to_section_on_unknown_address_is_a_reason() -> None:
    """T1 на неизвестном адресе — ``ok=False`` с причиной."""
    tools = _tools(_FakeCorpus([_chunk("p:d:0", 0)]))

    outcome = tools.invoke(
        ToolInvocation(name="expand_to_section", arguments={"chunk_id": "p:d:99"})
    )

    assert outcome.ok is False
    assert "не найден" in (outcome.reason or "")


# -- T2: роль раздела ----------------------------------------------------- #


def test_section_role_names_the_marker() -> None:
    """T2 называет сработавший маркер, а не только класс."""
    chunk = _chunk("p:d:0", 0, heading="Ограничения")
    tools = _tools(_FakeCorpus([chunk]))

    outcome = tools.invoke(ToolInvocation(name="section_role", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is True
    assert outcome.meta["role"] == "limitation"
    assert outcome.meta["marker"] == "ограничен"
    assert "«ограничен»" in outcome.content


def test_section_role_without_marker_is_unknown() -> None:
    """Без маркеров — ``unknown`` и прочерк, а не выдуманная роль."""
    chunk = _chunk("p:d:0", 0, heading="Обзор")
    tools = _tools(_FakeCorpus([chunk]))

    outcome = tools.invoke(ToolInvocation(name="section_role", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is True
    assert outcome.meta["role"] == UNKNOWN_ROLE
    assert outcome.meta["marker"] == ""
    assert "unknown" in outcome.content


def test_section_role_uses_parent_heading_when_section_is_silent() -> None:
    """Родительский заголовок тоже объявляет роль; источник виден в ``matched_in``."""
    chunk = _chunk("p:d:0", 0, heading="Ограничения > Память")
    tools = _tools(_FakeCorpus([chunk]))

    outcome = tools.invoke(ToolInvocation(name="section_role", arguments={"chunk_id": "p:d:0"}))

    assert outcome.meta["role"] == "limitation"
    assert outcome.meta["matched_in"] == "path"


def test_section_role_refuses_on_empty_lexicon(monkeypatch: pytest.MonkeyPatch) -> None:
    """Пустой лексикон — отказ, а не ``unknown``: ``unknown`` значит «маркеров нет»."""
    monkeypatch.setattr(section_lexicon, "ROLE_MARKERS", {})
    chunk = _chunk("p:d:0", 0, heading="Ограничения")
    tools = _tools(_FakeCorpus([chunk]))

    outcome = tools.invoke(ToolInvocation(name="section_role", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is False
    assert "лексикон" in (outcome.reason or "")


def test_lexicon_declares_every_role() -> None:
    """Полнота лексикона видна отчётом: все объявленные роли имеют маркеры."""
    report = lexicon_report()

    assert report["complete"] is True
    assert report["missing"] == []
    assert report["undeclared"] == []
    assert all(report["markers"][role] > 0 for role in report["roles"])


def test_classify_heading_is_case_insensitive() -> None:
    """Сопоставление — без учёта регистра, ответ называет маркер словаря."""
    assert classify_heading("KNOWN ISSUES") == ("limitation", "known issues")
    assert classify_heading("Как настроить") == ("howto", "как ")


# -- T4: строка таблицы --------------------------------------------------- #


def _table() -> ExtractedTable:
    return ExtractedTable(
        table_id="t0",
        page_no=7,
        caption="Матрица версий",
        header=("Версия", "Поддержка"),
        rows=(("14", "да"), ("15", "нет"), ("16", "да")),
    )


def test_table_row_without_table_ref_is_a_reason() -> None:
    """Без адреса внутри таблицы — ``ok=False`` с причиной."""
    chunk = _chunk("p:d:0", 0, text="| 15 | нет |")
    tools = _tools(_FakeCorpus([chunk], tables=(_table(),)))

    outcome = tools.invoke(ToolInvocation(name="table_row", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is False
    assert "не из таблицы" in (outcome.reason or "")


def test_table_row_returns_header_and_numbered_rows() -> None:
    """С адресом — шапка столбцов и строки с номерами."""
    chunk = _chunk("p:d:0", 0, table_ref=TableRef("t0", 2, 3))
    tools = _tools(_FakeCorpus([chunk], tables=(_table(),)))

    outcome = tools.invoke(ToolInvocation(name="table_row", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is True
    assert "шапка: 1) Версия | 2) Поддержка" in outcome.content
    assert "строка 2: 15 | нет" in outcome.content
    assert "строка 3: 16 | да" in outcome.content
    assert "строка 1:" not in outcome.content
    assert outcome.meta["rows"] == 2


def test_table_row_with_header_only_fragment() -> None:
    """Фрагмент из одной шапки — честный ответ, а не пустая таблица."""
    chunk = _chunk("p:d:0", 0, table_ref=TableRef("t0"))
    tools = _tools(_FakeCorpus([chunk], tables=(_table(),)))

    outcome = tools.invoke(ToolInvocation(name="table_row", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is True
    assert "только шапку" in outcome.content
    assert outcome.meta["rows"] == 0


def test_table_row_with_missing_table_is_a_reason() -> None:
    """Таблицы нет в документе — ``ok=False`` с причиной и списком существующих."""
    chunk = _chunk("p:d:0", 0, table_ref=TableRef("t9", 1, 1))
    tools = _tools(_FakeCorpus([chunk], tables=(_table(),)))

    outcome = tools.invoke(ToolInvocation(name="table_row", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is False
    assert "t9" in (outcome.reason or "")


def test_table_row_with_unmeasured_tables_is_a_reason() -> None:
    """``None`` — читатель таблиц не даёт; это отдельная причина, не «таблиц нет»."""
    chunk = _chunk("p:d:0", 0, table_ref=TableRef("t0", 1, 1))
    tools = _tools(_FakeCorpus([chunk], tables=None))

    outcome = tools.invoke(ToolInvocation(name="table_row", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is False
    assert "не даёт" in (outcome.reason or "")


def test_table_row_out_of_range_is_a_reason() -> None:
    """Адрес строк вне таблицы — отказ с числами, а не тихое обрезание."""
    chunk = _chunk("p:d:0", 0, table_ref=TableRef("t0", 3, 9))
    tools = _tools(_FakeCorpus([chunk], tables=(_table(),)))

    outcome = tools.invoke(ToolInvocation(name="table_row", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is False
    assert "вне таблицы" in (outcome.reason or "")


# -- T7: субъект параметра ------------------------------------------------ #


def test_subject_mention_names_present_and_absent_terms_and_marker() -> None:
    """T7: что есть дословно, чего нет и какой ограничитель сработал."""
    param = _param(
        concept_terms={"ru": ["Репликация"], "en": ["replication"]},
        semantics="Репликация кластера между узлами",
    )
    tools = _tools(_FakeCorpus([]), param=param)

    outcome = tools.invoke(
        ToolInvocation(
            name="subject_mention",
            arguments={"quote": "Репликация доступна только на основном узле"},
        )
    )

    assert outcome.ok is True
    assert "«Репликация»" in outcome.content
    assert "«кластера»" in outcome.content
    assert "маркеры отрицания/ограничителя в цитате: «только»" in outcome.content
    assert outcome.meta["present"] == 1
    assert outcome.meta["markers"] == ["только"]


def test_subject_mention_without_markers_says_so() -> None:
    """Нет ограничителя — ответ говорит «—», а не молчит."""
    param = _param(concept_terms={"en": ["replication"]})
    tools = _tools(_FakeCorpus([]), param=param)

    outcome = tools.invoke(
        ToolInvocation(name="subject_mention", arguments={"quote": "Replication is available"})
    )

    assert outcome.ok is True
    assert "маркеры отрицания/ограничителя в цитате: —" in outcome.content
    assert outcome.meta["markers"] == []


def test_subject_mention_without_terms_says_so() -> None:
    """Параметр без терминов и семантики — явный ответ, а не пустой список."""
    tools = _tools(_FakeCorpus([]), param=_param())

    outcome = tools.invoke(
        ToolInvocation(name="subject_mention", arguments={"quote": "любая цитата"})
    )

    assert outcome.ok is True
    assert "нет объявленных терминов" in outcome.content
    assert outcome.meta["terms"] == 0


# -- бюджет ответа -------------------------------------------------------- #


def test_truncation_is_visible(monkeypatch: pytest.MonkeyPatch) -> None:
    """Обрезка по бюджету видна: ``meta["truncated"]``, ``total_chars``, ``reason``, текст."""
    monkeypatch.setitem(TOOL_CHAR_BUDGETS, "expand_to_section", 200)
    section = tuple(
        _chunk(f"p:d:{index}", index, heading="Раздел", text="слово " * 80)
        for index in range(5)
    )
    tools = _tools(_FakeCorpus(section, sections={"p:d:0": section}))

    outcome = tools.invoke(ToolInvocation(name="expand_to_section", arguments={"chunk_id": "p:d:0"}))

    assert outcome.ok is True
    assert outcome.meta["truncated"] is True
    assert outcome.meta["total_chars"] > 200
    assert len(outcome.content) == 200
    assert "обрезано" in outcome.content
    assert "бюджет" in (outcome.reason or "")

def test_tools_satisfy_port_contract() -> None:
    """Собранные инструменты структурно удовлетворяют ``LocalToolPort``."""
    assert isinstance(_tools(_FakeCorpus([])), LocalToolPort)
