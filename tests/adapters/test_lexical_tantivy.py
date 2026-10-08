"""Тесты ``TantivyLexicalIndex`` (W8/T8.3) — реализация ``LexicalIndexPort``.

Проверяются: реальный BM25-поиск на tantivy, RU-стемминг (запрос «экспорта» находит
«Экспорт»), EN-поле, продукт-скоупед (I17), детерминизм/tie-break (R7), identifier-профиль
(точное совпадение с учётом регистра), ngram-профиль (подстрочный матч фрагмента
идентификатора/слова, двойной профиль T8.3), провенанс ``RetrievedChunk``, контракт порта,
ленивый импорт/guard отсутствия зависимости и валидация входа. Фикстуры категорийно
нейтральны (§1.3). Тесты, требующие tantivy, помечены ``requires_tantivy`` и
пропускаются при отсутствии зависимости.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pytest

from core.adapters import lexical_tantivy
from core.adapters.lexical_tantivy import (
    LexicalChunk,
    LexicalIndexUnavailable,
    TantivyLexicalIndex,
    tantivy_available,
)
from core.ports.lexical_index import LexicalIndexPort

requires_tantivy = pytest.mark.skipif(
    not tantivy_available(), reason="tantivy не установлен"
)

PRODUCT_A = "p-alpha"
PRODUCT_B = "p-beta"

_RU_DOCS = [
    LexicalChunk(
        chunk_id="c1",
        product_id=PRODUCT_A,
        text="Экспорт данных во внешний файл",
        heading_path="Раздел > Экспорт",
        content_hash="hash-c1",
        build_id="build-1",
    ),
    LexicalChunk(
        chunk_id="c2",
        product_id=PRODUCT_A,
        text="перехват сетевого трафика",
        heading_path="Раздел > Сеть",
        content_hash="hash-c2",
        build_id="build-1",
    ),
    LexicalChunk(
        chunk_id="c3",
        product_id=PRODUCT_B,
        text="export of data to external file",
        heading_path="Section > Export",
        content_hash="hash-c3",
        build_id="build-1",
    ),
]


@dataclass
class _Record:
    """Объект-запись с атрибутами (эмуляция внешнего record-типа)."""

    chunk_id: str
    product_id: str
    text: str
    heading_path: str = ""
    content_hash: str = ""
    build_id: str = ""


def _index(records=_RU_DOCS, *, profile: str = "stemmed") -> TantivyLexicalIndex:
    index = TantivyLexicalIndex(profile=profile)
    index.index(list(records))
    return index


# ---------------------------------------------------------------------------
# Базовый контракт и провенанс
# ---------------------------------------------------------------------------


def test_tantivy_available_returns_bool() -> None:
    assert isinstance(tantivy_available(), bool)


@requires_tantivy
def test_search_returns_retrieved_chunks_with_provenance() -> None:
    with _index() as index:
        hits = index.search("экспорта", [PRODUCT_A], top_k=5)
    assert hits
    top = hits[0]
    assert top.chunk_id == "c1"
    assert top.product_id == PRODUCT_A
    assert top.text == "Экспорт данных во внешний файл"
    assert top.heading_path == "Раздел > Экспорт"
    assert top.content_hash == "hash-c1"
    assert top.build_id == "build-1"
    assert isinstance(top.score, float) and top.score > 0.0


@requires_tantivy
def test_russian_stemming_matches_inflected_form() -> None:
    with _index() as index:
        by_base = index.search("экспорт", [PRODUCT_A], top_k=5)
        by_inflected = index.search("экспорта", [PRODUCT_A], top_k=5)
    assert [c.chunk_id for c in by_base] == ["c1"]
    assert [c.chunk_id for c in by_inflected] == ["c1"]


@requires_tantivy
def test_english_query_matches_english_field() -> None:
    with _index() as index:
        hits = index.search("export", [PRODUCT_B], top_k=5)
    assert [c.chunk_id for c in hits] == ["c3"]


@requires_tantivy
def test_lang_selects_language_analyzer() -> None:
    """``lang`` выбирает анализатор языка: RU-стеммер для «экспорта», EN — для «exports».

    Это не языковой firewall: стеммер чужого скрипта может пропустить токен дословно,
    поэтому проверяется именно морфологическое сближение форм внутри своего языка.
    """
    with _index() as index:
        ru = index.search("экспорта", [PRODUCT_A], top_k=5, lang="ru")
        en = index.search("exports", [PRODUCT_B], top_k=5, lang="en")
    assert [c.chunk_id for c in ru] == ["c1"]
    assert [c.chunk_id for c in en] == ["c3"]


# ---------------------------------------------------------------------------
# Продукт-скоуп (I17)
# ---------------------------------------------------------------------------


@requires_tantivy
def test_product_scope_excludes_other_products() -> None:
    with _index() as index:
        scoped = index.search("export", [PRODUCT_B], top_k=10)
        not_scoped = index.search("export", [PRODUCT_A], top_k=10)
    assert [c.chunk_id for c in scoped] == ["c3"]
    assert not_scoped == []


@requires_tantivy
def test_empty_product_ids_return_nothing() -> None:
    with _index() as index:
        assert index.search("экспорт", [], top_k=10) == []


# ---------------------------------------------------------------------------
# top_k и детерминизм (R7)
# ---------------------------------------------------------------------------


@requires_tantivy
def test_top_k_limits_results() -> None:
    records = [
        LexicalChunk(chunk_id=f"t{i}", product_id=PRODUCT_A, text="экспорт данных")
        for i in range(3)
    ]
    with _index(records) as index:
        hits = index.search("экспорт", [PRODUCT_A], top_k=2)
    assert len(hits) == 2


@requires_tantivy
def test_tie_break_is_deterministic_by_chunk_id() -> None:
    records = [
        LexicalChunk(chunk_id="t9", product_id=PRODUCT_A, text="экспорт данных"),
        LexicalChunk(chunk_id="t1", product_id=PRODUCT_A, text="экспорт данных"),
    ]
    with _index(records) as index:
        hits = index.search("экспорт", [PRODUCT_A], top_k=5)
    assert [c.chunk_id for c in hits] == ["t1", "t9"]


@requires_tantivy
def test_same_query_twice_is_deterministic() -> None:
    with _index() as index:
        first = [c.chunk_id for c in index.search("экспорт", [PRODUCT_A, PRODUCT_B], top_k=5)]
        second = [c.chunk_id for c in index.search("экспорт", [PRODUCT_A, PRODUCT_B], top_k=5)]
    assert first == second
    assert first  # запрос действительно что-то нашёл


# ---------------------------------------------------------------------------
# Identifier-профиль
# ---------------------------------------------------------------------------


@requires_tantivy
def test_identifier_profile_is_case_sensitive() -> None:
    records = [
        LexicalChunk(chunk_id="id1", product_id=PRODUCT_A, text="see API-2024_X endpoint"),
        LexicalChunk(chunk_id="id2", product_id=PRODUCT_A, text="api-2024_x lowercase"),
    ]
    with _index(records, profile="identifier") as index:
        upper = index.search("API-2024_X", [PRODUCT_A], top_k=5)
        lower = index.search("api-2024_x", [PRODUCT_A], top_k=5)
    assert [c.chunk_id for c in upper] == ["id1"]
    assert [c.chunk_id for c in lower] == ["id2"]


@requires_tantivy
def test_search_identifier_overrides_stemmed_profile() -> None:
    records = [LexicalChunk("id1", PRODUCT_A, "see API-2024_X endpoint")]
    with _index(records, profile="stemmed") as index:
        hits = index.search_identifier("API-2024_X", [PRODUCT_A], top_k=5)
    assert [c.chunk_id for c in hits] == ["id1"]


@requires_tantivy
def test_raw_profile_alias_uses_identifier_field() -> None:
    records = [LexicalChunk("id1", PRODUCT_A, "see API-2024_X endpoint")]
    with _index(records, profile="raw") as index:
        hits = index.search("API-2024_X", [PRODUCT_A], top_k=5)
    assert [c.chunk_id for c in hits] == ["id1"]


# ---------------------------------------------------------------------------
# Формы входа и контракт порта
# ---------------------------------------------------------------------------


@requires_tantivy
def test_port_conformance() -> None:
    index = TantivyLexicalIndex()
    assert isinstance(index, LexicalIndexPort)
    assert index.engine_version.startswith("tantivy-")


@requires_tantivy
def test_mapping_and_object_records_accepted() -> None:
    records = [
        {
            "chunk_id": "m1",
            "product_id": PRODUCT_A,
            "text": "экспорт данных",
            "heading_path": "H",
            "content_hash": "hm1",
            "build_id": "build-1",
        },
        _Record("o1", PRODUCT_A, "экспорт файлов", heading_path="H2", content_hash="ho1"),
    ]
    with _index(records) as index:
        hits = index.search("экспорт", [PRODUCT_A], top_k=5)
    assert {c.chunk_id for c in hits} == {"m1", "o1"}


@requires_tantivy
def test_build_id_defaults_from_records() -> None:
    index = TantivyLexicalIndex()
    index.index([LexicalChunk("c1", PRODUCT_A, "экспорт", build_id="build-X")])
    assert index.build_id == "build-X"
    index.close()


@requires_tantivy
def test_build_id_argument_wins_over_records() -> None:
    index = TantivyLexicalIndex()
    index.index([LexicalChunk("c1", PRODUCT_A, "экспорт", build_id="build-X")], build_id="B")
    assert index.build_id == "B"
    index.close()


# ---------------------------------------------------------------------------
# Ошибочные входы
# ---------------------------------------------------------------------------


def test_invalid_profile_raises() -> None:
    with pytest.raises(ValueError):
        TantivyLexicalIndex(profile="fuzzy")


@requires_tantivy
def test_search_before_index_raises() -> None:
    with pytest.raises(LexicalIndexUnavailable):
        TantivyLexicalIndex().search("экспорт", [PRODUCT_A], top_k=5)


@requires_tantivy
def test_duplicate_chunk_id_raises() -> None:
    records = [
        LexicalChunk("dup", PRODUCT_A, "экспорт"),
        LexicalChunk("dup", PRODUCT_A, "экспорт"),
    ]
    with pytest.raises(ValueError):
        TantivyLexicalIndex().index(records)


def test_missing_field_raises() -> None:
    with pytest.raises(ValueError):
        TantivyLexicalIndex().index([{"chunk_id": "c1", "text": "экспорт"}])


# ---------------------------------------------------------------------------
# Ленивый импорт / guard отсутствия зависимости
# ---------------------------------------------------------------------------


def test_missing_dependency_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lexical_tantivy, "tantivy_available", lambda: False)
    index = TantivyLexicalIndex()
    assert index.available is False
    assert index.engine_version == "tantivy-unavailable"
    with pytest.raises(LexicalIndexUnavailable):
        index.index([{"chunk_id": "c1", "product_id": PRODUCT_A, "text": "экспорт"}])


@requires_tantivy
def test_engine_version_reports_native_version() -> None:
    assert TantivyLexicalIndex().engine_version != "tantivy-unknown"


# ---------------------------------------------------------------------------
# Ngram-профиль (T8.3, двойной профиль)
# ---------------------------------------------------------------------------

#: Записи с идентификатором (пунктуация/регистр) и обычным словом для ngram-проверок.
_NGRAM_DOCS = [
    LexicalChunk(
        chunk_id="n1",
        product_id=PRODUCT_A,
        text="see API-2024_X endpoint",
        heading_path="Section > API",
        content_hash="hash-n1",
        build_id="build-ng",
    ),
    LexicalChunk(
        chunk_id="n2",
        product_id=PRODUCT_A,
        text="перехват сетевого трафика",
        content_hash="hash-n2",
        build_id="build-ng",
    ),
    LexicalChunk(
        chunk_id="n3",
        product_id=PRODUCT_B,
        text="export of data to external file",
        content_hash="hash-n3",
        build_id="build-ng",
    ),
]


def test_profiles_include_ngram() -> None:
    assert "ngram" in lexical_tantivy._PROFILES


def test_ngram_bounds_are_sane() -> None:
    assert 1 <= lexical_tantivy._NGRAM_MIN <= lexical_tantivy._NGRAM_MAX


def test_ngram_profile_accepted_by_constructor() -> None:
    index = TantivyLexicalIndex(profile="ngram")
    try:
        assert index.profile == "ngram"
    finally:
        index.close()


def test_ngram_profile_normalizes_case_and_whitespace() -> None:
    index = TantivyLexicalIndex(profile="  NGRAM ")
    try:
        assert index.profile == "ngram"
    finally:
        index.close()


@pytest.mark.parametrize("bad", ["ngramm", "grams", "fuzzy", ""])
def test_unknown_profile_is_still_rejected(bad: str) -> None:
    with pytest.raises(ValueError):
        TantivyLexicalIndex(profile=bad)


def test_stemmed_remains_default_profile() -> None:
    index = TantivyLexicalIndex()
    try:
        assert index.profile == "stemmed"
    finally:
        index.close()


@requires_tantivy
def test_ngram_matches_partial_identifier() -> None:
    with _index(_NGRAM_DOCS, profile="ngram") as index:
        partial = index.search("i-202", [PRODUCT_A], top_k=5)
        exact = index.search("API-2024_X", [PRODUCT_A], top_k=5)
    assert [c.chunk_id for c in partial] == ["n1"]
    assert [c.chunk_id for c in exact] == ["n1"]


@requires_tantivy
def test_ngram_finds_substring_inside_token() -> None:
    with _index(_NGRAM_DOCS, profile="ngram") as index:
        hits = index.search("endpo", [PRODUCT_A], top_k=5)
    assert [c.chunk_id for c in hits] == ["n1"]


@requires_tantivy
def test_ngram_query_shorter_than_min_gram_is_empty() -> None:
    """Честная граница профиля: нет n-грамм короче ``_NGRAM_MIN`` → пустой результат."""
    with _index(_NGRAM_DOCS, profile="ngram") as index:
        assert index.search("xy", [PRODUCT_A], top_k=5) == []


@requires_tantivy
def test_search_ngram_overrides_stemmed_profile() -> None:
    with _index(_NGRAM_DOCS, profile="stemmed") as index:
        stemmed = index.search("i-202", [PRODUCT_A, PRODUCT_B], top_k=5)
        via_ngram = index.search_ngram("i-202", [PRODUCT_A, PRODUCT_B], top_k=5)
    assert stemmed == []  # стеммированный парсер не сближает подстроку
    assert [c.chunk_id for c in via_ngram] == ["n1"]


@requires_tantivy
def test_ngram_provenance_fields_preserved() -> None:
    with _index(_NGRAM_DOCS, profile="ngram") as index:
        hits = index.search("endpo", [PRODUCT_A], top_k=5)
    assert hits
    top = hits[0]
    assert top.chunk_id == "n1"
    assert top.product_id == PRODUCT_A
    assert top.text == "see API-2024_X endpoint"
    assert top.heading_path == "Section > API"
    assert top.content_hash == "hash-n1"
    assert top.build_id == "build-ng"
    assert isinstance(top.score, float) and top.score > 0.0


@requires_tantivy
def test_ngram_product_scope_excludes_other_products() -> None:
    with _index(_NGRAM_DOCS, profile="ngram") as index:
        other = index.search("endpo", [PRODUCT_B], top_k=10)
        own = index.search("endpo", [PRODUCT_A], top_k=10)
    assert other == []
    assert [c.chunk_id for c in own] == ["n1"]


@requires_tantivy
def test_ngram_empty_product_ids_return_nothing() -> None:
    with _index(_NGRAM_DOCS, profile="ngram") as index:
        assert index.search("endpo", [], top_k=10) == []


@requires_tantivy
def test_ngram_is_deterministic() -> None:
    with _index(_NGRAM_DOCS, profile="ngram") as index:
        first = [c.chunk_id for c in index.search("2024", [PRODUCT_A, PRODUCT_B], top_k=5)]
        second = [c.chunk_id for c in index.search("2024", [PRODUCT_A, PRODUCT_B], top_k=5)]
    assert first == second == ["n1"]


@requires_tantivy
def test_ngram_port_conformance() -> None:
    index = TantivyLexicalIndex(profile="ngram")
    try:
        assert isinstance(index, LexicalIndexPort)
        assert index.engine_version.startswith("tantivy-")
    finally:
        index.close()


def test_ngram_missing_dependency_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lexical_tantivy, "tantivy_available", lambda: False)
    index = TantivyLexicalIndex(profile="ngram")
    try:
        assert index.available is False
        with pytest.raises(LexicalIndexUnavailable):
            index.index(
                [{"chunk_id": "n1", "product_id": PRODUCT_A, "text": "endpoint"}]
            )
    finally:
        index.close()


# ---------------------------------------------------------------------------
# Нейтральность (§1.3)
# ---------------------------------------------------------------------------


def test_module_is_category_neutral() -> None:
    """Исходник адаптера не содержит предметных/вендорских терминов (§1.3)."""
    source = Path(lexical_tantivy.__file__).read_text(encoding="utf-8")
    forbidden = re.compile(r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE)
    assert forbidden.search(source) is None
