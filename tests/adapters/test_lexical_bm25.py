"""Тесты in-process BM25-адаптера (``LexicalIndexPort``, baseline R0).

Проверяется то, что делает адаптер пригодным как baseline и как test double retrieval-CI:
продукт-скоупед фильтрация (I17), детерминированный порядок при равных баллах (R7),
усечение по ``top_k`` и явный отказ до построения индекса. Эталон семантики —
``tests/selfcheck.py::_bm25``.
"""

from __future__ import annotations

import pytest

from core.adapters.lexical_bm25 import Bm25Index, LexicalDoc

_DOCS = [
    LexicalDoc("p1#0", "p1", "резервное копирование и восстановление базы"),
    LexicalDoc("p1#1", "p1", "разграничение прав доступа пользователей"),
    LexicalDoc("p2#0", "p2", "резервное копирование по расписанию"),
]


def test_bm25_is_product_scoped() -> None:
    """Запрос в партиции p1 не возвращает документов p2 (I17)."""
    index = Bm25Index()
    index.index(_DOCS)

    hits = index.search("резервное копирование", product_ids=["p1"], top_k=5)

    assert hits
    assert all(hit.product_id == "p1" for hit in hits)
    assert hits[0].chunk_id == "p1#0"


def test_bm25_order_is_deterministic() -> None:
    """Повторный запрос даёт тот же порядок; при равных баллах — tie-break по chunk_id."""
    index = Bm25Index()
    index.index(_DOCS)

    first = [hit.chunk_id for hit in index.search("резервное копирование", top_k=5)]
    second = [hit.chunk_id for hit in index.search("резервное копирование", top_k=5)]

    assert first == second

    # Одинаковый текст ⇒ одинаковый балл ⇒ порядок задаёт chunk_id, а не порядок входа.
    tie_text = "резервное копирование базы данных"
    forward = Bm25Index()
    forward.index([LexicalDoc("b#0", "p1", tie_text), LexicalDoc("a#0", "p1", tie_text)])
    backward = Bm25Index()
    backward.index([LexicalDoc("a#0", "p1", tie_text), LexicalDoc("b#0", "p1", tie_text)])

    assert [hit.chunk_id for hit in forward.search("резервное копирование", top_k=5)] == [
        "a#0",
        "b#0",
    ]
    assert [hit.chunk_id for hit in backward.search("резервное копирование", top_k=5)] == [
        "a#0",
        "b#0",
    ]


def test_bm25_top_k_truncates() -> None:
    """``top_k`` — жёсткая граница длины результата."""
    index = Bm25Index()
    index.index(_DOCS)

    assert len(index.search("резервное копирование", top_k=1)) == 1


def test_search_before_index_raises() -> None:
    """Поиск до ``index()`` — ``RuntimeError``, а не пустой результат."""
    with pytest.raises(RuntimeError, match="index"):
        Bm25Index().search("резервное копирование")


# ---------------------------------------------------------------------------
# CJK-биграммы (план 18, «Сопутствующая работа»)
# ---------------------------------------------------------------------------


def test_bm25_cjk_query_inside_unspaced_run_finds_the_document() -> None:
    """Точный кейс плана: «配置管理» и «备份功能» находят CJK-документ, EN — свой.

    До правки CJK-серия была одним токеном, и оба запроса давали 0 хитов. Тест держит
    три числа кейса одновременно: два CJK-запроса и английский контроль.
    """
    index = Bm25Index()
    index.index(
        [
            LexicalDoc("zh", "p", "数据库配置管理工具提供了备份功能。"),
            LexicalDoc("en", "p", "config management tool provides backup"),
        ]
    )

    assert [hit.chunk_id for hit in index.search("配置管理", top_k=5)] == ["zh"]
    assert [hit.chunk_id for hit in index.search("备份功能", top_k=5)] == ["zh"]
    assert [hit.chunk_id for hit in index.search("config management", top_k=5)] == ["en"]


def test_bm25_cjk_single_character_and_mixed_script() -> None:
    """Одиночный иероглиф индексируется; смешанный текст находится с обеих сторон."""
    index = Bm25Index()
    index.index(
        [
            LexicalDoc("one", "p", "配"),
            LexicalDoc("mix", "p", "PostgreSQL 配置管理"),
        ]
    )

    assert [hit.chunk_id for hit in index.search("配", top_k=5)] == ["one"]
    assert "mix" in {hit.chunk_id for hit in index.search("配置管理", top_k=5)}
    assert "mix" in {hit.chunk_id for hit in index.search("PostgreSQL", top_k=5)}


def test_bm25_engine_version_reflects_the_tokenization_change() -> None:
    """Версия правил токенизации поднята: индекс того же текста даёт другую выдачу."""
    assert Bm25Index.engine_version == "bm25-py-2"
