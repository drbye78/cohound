"""Тесты правила «тот же документ» (``core.pipeline.doc_refs``).

Предмет — не качество эталона, а **единственность** правила: канонизация URL и уровни
совпадения ``exact`` → ``canonical`` → ``canonical_query`` живут в одном месте, чтобы
документный эталон и измерение recall@K резолвили один URL одинаково. Проверяются
границы канонизации (что снимается и что нет), хост-ключ, граница сегмента корня и
поведение ``resolve`` на всех уровнях, включая отсутствие документа.

БД минимальная: читается только ``corpus_docs`` (``product_id``/``url``/``doc_id``).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator

import pytest

from core.pipeline.doc_refs import DocIndex, canonical_url, host_key, root_matches

PRODUCT = "p1"
OTHER_PRODUCT = "p2"
URL_MAIN = "https://docs.test/tune"
DOC_MAIN = f"{PRODUCT}:main"
DOC_EN = f"{PRODUCT}:en"
DOC_RU = f"{PRODUCT}:ru"
DOC_OTHER = f"{OTHER_PRODUCT}:main"


# ---------------------------------------------------------------------------
# canonical_url
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        # Схема: http и https — одна форма (корпус пишет https, таблица иногда http).
        ("http://docs.test/a", "https://docs.test/a"),
        ("HTTPS://docs.test/a", "https://docs.test/a"),
        # Хост — регистронезависим, путь — нет.
        ("https://DOCS.Test/A", "https://docs.test/A"),
        # Порт по умолчанию снимается, нестандартный остаётся.
        ("https://docs.test:443/a", "https://docs.test/a"),
        ("http://docs.test:80/a", "https://docs.test/a"),
        ("https://docs.test:8080/a", "https://docs.test:8080/a"),
        # Фрагмент — якорь внутри страницы, не часть документа.
        ("https://docs.test/a#section-3", "https://docs.test/a"),
        # Повторные и хвостовой слэш.
        ("https://docs.test//a///b/", "https://docs.test/a/b"),
        # Percent-кодирование пути раскрывается.
        ("https://docs.test/a%20b", "https://docs.test/a b"),
        ("https://docs.test/%D1%82%D0%B5%D1%81%D1%82", "https://docs.test/тест"),
        # Пустой путь — корень, а не пустая строка.
        ("https://docs.test", "https://docs.test/"),
    ],
)
def test_canonical_url_drops_only_http_insignificant_parts(url: str, expected: str) -> None:
    assert canonical_url(url) == expected


def test_canonical_url_keeps_query_by_default() -> None:
    """Query по умолчанию — часть адреса: ``?lang=ru`` это другая редакция, не шум."""
    assert canonical_url(f"{URL_MAIN}?lang=ru") == f"{URL_MAIN}?lang=ru"


def test_canonical_url_drop_query_is_a_separate_weaker_level() -> None:
    """``drop_query`` — отдельный уровень, а не замена канонизации."""
    assert canonical_url(f"{URL_MAIN}?lang=ru", drop_query=True) == URL_MAIN
    # Фрагмент снимается и здесь: уровни не исключают друг друга.
    assert canonical_url(f"{URL_MAIN}?h=quote#x", drop_query=True) == URL_MAIN


# ---------------------------------------------------------------------------
# host_key
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://docs.test/a", "docs.test"),
        ("https://Docs.Test/a", "docs.test"),
        # ``www.`` и голый хост — одна форма.
        ("https://www.docs.test/a", "docs.test"),
        ("https://docs.test", "docs.test"),
    ],
)
def test_host_key_equates_www_and_bare_host(url: str, expected: str) -> None:
    assert host_key(url) == expected


# ---------------------------------------------------------------------------
# root_matches
# ---------------------------------------------------------------------------


def test_root_matches_path_prefix_on_segment_boundary() -> None:
    assert root_matches("https://docs.test/docs/guide/page", "https://docs.test/docs")
    assert root_matches("https://docs.test/docs/guide/page", "https://docs.test/docs/")


def test_root_matches_refuses_prefix_without_segment_boundary() -> None:
    """``/docsx`` не лежит под корнем ``/docs``: сравнение идёт по сегменту."""
    assert not root_matches("https://docs.test/docsx/page", "https://docs.test/docs")


def test_root_matches_does_not_inherit_subdomain() -> None:
    """Поддомен не наследует право обхода (AGENTS §6): хост сравнивается точно."""
    assert not root_matches("https://edu.docs.test/docs/a", "https://docs.test/docs")
    # А ``www.`` — та же форма, а не поддомен.
    assert root_matches("https://www.docs.test/docs/a", "https://docs.test/docs")


def test_root_matches_other_host_and_other_path() -> None:
    assert not root_matches("https://other.test/docs/a", "https://docs.test/docs")
    assert not root_matches("https://docs.test/guide/a", "https://docs.test/docs")


# ---------------------------------------------------------------------------
# DocIndex
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn() -> Iterator[sqlite3.Connection]:
    """Две редакции одного раздела у ``p1`` и один документ у ``p2``."""
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE corpus_docs (product_id TEXT, url TEXT, doc_id TEXT)")
    connection.executemany(
        "INSERT INTO corpus_docs (product_id, url, doc_id) VALUES (?, ?, ?)",
        [
            (PRODUCT, URL_MAIN, DOC_MAIN),
            (PRODUCT, f"{URL_MAIN}?lang=en", DOC_EN),
            (PRODUCT, f"{URL_MAIN}?lang=ru-en", DOC_RU),
            (OTHER_PRODUCT, URL_MAIN, DOC_OTHER),
        ],
    )
    connection.commit()
    try:
        yield connection
    finally:
        connection.close()


def test_resolve_exact_is_literal_url(conn: sqlite3.Connection) -> None:
    docs, level = DocIndex(conn).resolve(PRODUCT, URL_MAIN)
    assert (docs, level) == ([DOC_MAIN], "exact")


def test_resolve_canonical_ignores_fragment_and_trailing_slash(
    conn: sqlite3.Connection,
) -> None:
    index = DocIndex(conn)
    assert index.resolve(PRODUCT, f"{URL_MAIN}/#frag") == ([DOC_MAIN], "canonical")
    assert index.resolve(PRODUCT, "http://DOCS.test/tune") == ([DOC_MAIN], "canonical")


def test_resolve_canonical_query_returns_every_candidate(conn: sqlite3.Connection) -> None:
    """Две языковые редакции — факт корпуса: ``resolve`` не выбирает за вызывающего."""
    docs, level = DocIndex(conn).resolve(PRODUCT, f"{URL_MAIN}?lang=de")
    assert level == "canonical_query"
    assert docs == [DOC_MAIN, DOC_EN, DOC_RU]


def test_resolve_is_product_scoped(conn: sqlite3.Connection) -> None:
    """Тот же URL у другого продукта — другой документ (I17)."""
    index = DocIndex(conn)
    assert index.resolve(OTHER_PRODUCT, f"{URL_MAIN}?lang=en") == (
        [DOC_OTHER],
        "canonical_query",
    )
    assert index.resolve("p3", URL_MAIN) == ([], "")


def test_resolve_absent_url_has_no_level(conn: sqlite3.Connection) -> None:
    assert DocIndex(conn).resolve(PRODUCT, "https://docs.test/other") == ([], "")


def test_resolve_exact_wins_over_weaker_levels(conn: sqlite3.Connection) -> None:
    """Точный URL не размывается кандидатами более слабого уровня."""
    docs, level = DocIndex(conn).resolve(PRODUCT, f"{URL_MAIN}?lang=en")
    assert (docs, level) == ([DOC_EN], "exact")


def test_doc_url_and_doc_product(conn: sqlite3.Connection) -> None:
    index = DocIndex(conn)
    assert index.doc_url(DOC_EN) == f"{URL_MAIN}?lang=en"
    assert index.doc_product(DOC_EN) == PRODUCT
    assert index.doc_url("p1:missing") is None
    assert index.doc_product("p1:missing") is None


def test_empty_table_resolves_nothing() -> None:
    """Пустая таблица — не ошибка: документов нет, и это говорится пустым ответом."""
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute(
            "CREATE TABLE corpus_docs (product_id TEXT, url TEXT, doc_id TEXT)"
        )
        index = DocIndex(connection)
        assert index.resolve(PRODUCT, URL_MAIN) == ([], "")
        assert index.doc_url(DOC_MAIN) is None
        assert index.doc_product(DOC_MAIN) is None
    finally:
        connection.close()


def test_doc_index_does_not_close_the_callers_connection(
    conn: sqlite3.Connection,
) -> None:
    """Соединение чужое: после работы индекса владелец может им пользоваться."""
    index = DocIndex(conn)
    index.resolve(PRODUCT, URL_MAIN)
    assert conn.execute("SELECT count(*) FROM corpus_docs").fetchone()[0] == 4
