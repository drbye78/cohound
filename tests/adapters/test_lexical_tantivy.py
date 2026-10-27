"""Тесты ``TantivyLexicalIndex`` (W8/T8.3) — реализация ``LexicalIndexPort``.

Проверяются: реальный BM25-поиск на tantivy, RU-стемминг (запрос «экспорта» находит
«Экспорт»), EN-стемминг одним анализатором, продукт-скоупед (I17), детерминизм/tie-break
(R7), identifier- и ngram-профили как **отдельные сборки** (одно поле, анализатор по
профилю — C8), состав схемы (ровно одно текстовое поле), провенанс ``RetrievedChunk``,
контракт порта, ленивый импорт/guard отсутствия зависимости и валидация входа. Фикстуры
категорийно нейтральны (§1.3). Тесты, требующие tantivy, помечены ``requires_tantivy`` и
пропускаются при отсутствии зависимости.
"""

from __future__ import annotations

import json
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
def test_single_analyzer_stems_both_languages() -> None:
    """Один анализатор несёт оба стеммера: RU-формы сближает русский, EN — английский.

    До C8 это делали два поля (``body_stem_ru``/``body_stem_en``) с одним текстом и
    выбором поля по ``lang``. Теперь поле одно, и ``lang`` — ось **запроса** (контракт
    ``LexicalIndexPort``), а не ось индекса: выдача от него не зависит. Проверяется
    именно морфологическое сближение форм внутри своего алфавита (это не языковой
    firewall: стеммер чужого скрипта проходит токен дословно).
    """
    with _index() as index:
        ru = index.search("экспорта", [PRODUCT_A], top_k=5, lang="ru")
        en = index.search("exports", [PRODUCT_B], top_k=5, lang="en")
        ru_without_lang = index.search("экспорта", [PRODUCT_A], top_k=5)
    assert [c.chunk_id for c in ru] == ["c1"]
    assert [c.chunk_id for c in en] == ["c3"]
    assert [c.chunk_id for c in ru_without_lang] == [c.chunk_id for c in ru]


@requires_tantivy
def test_schema_has_exactly_one_text_field(tmp_path: Path) -> None:
    """C8: одно текстовое поле на сборку. Возврат к четырём — снова 4× анализа и объёма.

    Состав полей читается из ``meta.json`` собранного индекса: tantivy-py 0.26 не отдаёт
    список полей через API, а ``meta.json`` — фактическая схема сегментов, то есть
    измерение, а не чтение константы модуля.
    """
    path = tmp_path / "idx"
    with TantivyLexicalIndex(index_path=path) as index:
        index.index([LexicalChunk("c1", PRODUCT_A, "экспорт данных во внешний файл")])
    schema = json.loads((path / "meta.json").read_text(encoding="utf-8"))["schema"]
    assert [f["name"] for f in schema] == ["chunk_id", "product_id", "body"]
    body = schema[2]
    assert body["options"]["indexing"]["tokenizer"] == lexical_tantivy._STEM_TOKENIZER
    # Ни одного прежнего поля профиля: они и были четырьмя копиями одного текста.
    assert not {f["name"] for f in schema} & {
        "body_stem_ru",
        "body_stem_en",
        "body_identifier",
        "body_ngram",
    }


@requires_tantivy
def test_identifier_build_switches_the_field_analyzer() -> None:
    """Тот же единственный токенайзер-слот, но другой анализатор — ``whitespace``."""
    with _index([LexicalChunk("c1", PRODUCT_A, "see API-2024_X endpoint")], profile="identifier") as idx:
        assert idx._field_tokenizer() == "whitespace"


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


@requires_tantivy
def test_boundary_tie_does_not_depend_on_insertion_order() -> None:
    """Граница ``top_k`` не разрезает группу равных баллов (R7).

    Двадцать документов с **одинаковым** текстом дают одинаковый BM25-балл, поэтому
    позиция ``top_k`` попадает внутрь группы ничьих. Коллектор tantivy отдаёт ровно
    запрошенное число и при равенстве баллов выбирает по внутреннему порядку — он
    зависит от порядка вставки. Наблюдено на живом корпусе: два процесса на одной
    сборке вернули на позиции 9 разные чанки при балле 16.671051.

    Инвариант: выдача при фиксированном ``top_k`` не зависит от порядка вставки и
    совпадает с лексикографически младшими участниками ничьей.
    """
    records = [
        LexicalChunk(chunk_id=f"c{i:02d}", product_id=PRODUCT_A, text="экспорт данных")
        for i in range(20)
    ]
    with _index(records) as ascending:
        asc = [c.chunk_id for c in ascending.search("экспорт", [PRODUCT_A], top_k=5)]
    with _index(list(reversed(records))) as descending:
        desc = [c.chunk_id for c in descending.search("экспорт", [PRODUCT_A], top_k=5)]
    assert asc == desc == ["c00", "c01", "c02", "c03", "c04"]


@requires_tantivy
def test_boundary_tie_wider_than_headroom_is_still_deterministic() -> None:
    """Ничья шире стартового запаса расширяет окно, а не режется по воле движка.

    Сорок равных документов при ``top_k=5`` не помещаются в окно ``top_k + запас``,
    поэтому окно обязано вырасти; результат — тот же, что при узкой ничьей.
    """
    records = [
        LexicalChunk(chunk_id=f"d{i:02d}", product_id=PRODUCT_A, text="экспорт данных")
        for i in range(40)
    ]
    with _index(records) as ascending:
        asc = [c.chunk_id for c in ascending.search("экспорт", [PRODUCT_A], top_k=5)]
    with _index(list(reversed(records))) as descending:
        desc = [c.chunk_id for c in descending.search("экспорт", [PRODUCT_A], top_k=5)]
    assert asc == desc == ["d00", "d01", "d02", "d03", "d04"]


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
def test_identifier_requires_its_own_build() -> None:
    """Профиль — свойство сборки (C8): кросс-профильного поиска по индексу нет.

    Раньше ``search_identifier`` работал по стенмленному индексу, потому что в схеме
    лежали все четыре поля сразу. Одно поле — один анализ: точный идентификатор
    находится только индексом, построенным под ``identifier`` (там ``whitespace`` без
    lowercase), а стенмленный находит его лишь как стеммированный токен — то есть
    регистр и пунктуация не значимы. Это и есть оплаченная цена C8, и здесь она
    зафиксирована, чтобы возврат к «всем профилям сразу» был видимым решением.
    """
    records = [
        LexicalChunk("id1", PRODUCT_A, "see API-2024_X endpoint"),
        LexicalChunk("id2", PRODUCT_A, "see api-2024_x endpoint"),
    ]
    with _index(records, profile="identifier") as exact:
        assert [c.chunk_id for c in exact.search("API-2024_X", [PRODUCT_A], top_k=5)] == ["id1"]
    with _index(records, profile="stemmed") as stemmed:
        # стеммированный анализ регистр не различает — оба документа равноправны
        found = {c.chunk_id for c in stemmed.search("API-2024_X", [PRODUCT_A], top_k=5)}
    assert found == {"id1", "id2"}


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
def test_ngram_requires_its_own_build() -> None:
    """Подстрочный матч — только у ngram-сборки; стенмленная подстроку не сближает."""
    with _index(_NGRAM_DOCS, profile="stemmed") as stemmed:
        assert stemmed.search("i-202", [PRODUCT_A, PRODUCT_B], top_k=5) == []
    with _index(_NGRAM_DOCS, profile="ngram") as ngram:
        assert [c.chunk_id for c in ngram.search("i-202", [PRODUCT_A, PRODUCT_B], top_k=5)] == [
            "n1"
        ]


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


