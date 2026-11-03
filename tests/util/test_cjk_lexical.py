"""Тесты ``core/util/cjk_lexical.py`` — сегментация CJK для лексического контура.

Проверяется контракт функции, а не «похожесть»: какие код-поинты считаются прогоном,
где проходят границы, сколько термов даёт прогон и что текст вне CJK копируется байт-в-байт.
Кейс плана 18 («配置管理» внутри «数据库配置管理工具…») проверяется на уровне токенов здесь
и на уровне поиска — в ``tests/adapters/test_lexical_tantivy.py`` /
``test_lexical_bm25.py``.
"""

from __future__ import annotations

from core.util.cjk_lexical import (
    CJK_RANGES,
    cjk_terms,
    is_cjk_char,
    iter_cjk_runs,
    segment_cjk,
)


def test_cjk_ranges_cover_all_four_scripts_and_skip_punctuation() -> None:
    """Han/хирагана/катакана/хангыль — прогоны; CJK-пунктуация и полноширина — нет."""
    for char in ["数", "据", "あ", "ア", "ー", "한", "글", "\U00020000"]:
        assert is_cjk_char(char), char
    for char in ["。", "！", "？", "；", "，", "、", "：", "・", "　", "Ａ", "1", "a", "Ж"]:
        assert not is_cjk_char(char), char


def test_iter_cjk_runs_reports_spans_of_maximal_runs() -> None:
    """Смещения — максимальные серии подряд идущих CJK, пунктуация их разрывает."""
    text = "PG 配置管理，数据库 PostgreSQL"
    assert list(iter_cjk_runs(text)) == [(3, 7), (8, 11)]
    assert list(iter_cjk_runs("no cjk here")) == []
    assert list(iter_cjk_runs("")) == []


def test_cjk_terms_bigrams_and_single_character() -> None:
    """Длина N ≥ 2 → N-1 перекрывающихся биграмм; длина 1 → сам символ; пусто → пусто."""
    assert cjk_terms("配置管理") == ["配置", "置管", "管理"]
    assert cjk_terms("配") == ["配"]
    assert cjk_terms("") == []


def test_segment_cjk_exact_index_view_of_the_plan_sentence() -> None:
    """Представление индекса — пин целиком, включая сохранённую пунктуацию.

    Пробелы только добавляются; ``。`` остаётся в строке (граница прогона), а не
    выбрасывается: ``segment_cjk`` — не нормализатор.
    """
    text = "数据库配置管理工具提供了备份功能。"
    assert segment_cjk(text) == (
        " 数据 据库 库配 配置 置管 管理 理工 工具 具提 提供 供了 了备 备份 份功 功能 。"
    )
    assert segment_cjk("PostgreSQL 配置管理") == "PostgreSQL  配置 置管 管理 "
    assert segment_cjk("配") == " 配 "


def test_segment_cjk_keeps_non_cjk_text_byte_identical() -> None:
    """Всё, что не CJK, копируется как есть, в исходном порядке (включая знаки)."""
    text = "Экспорт (v2.1) — файл; endpoint API-2024_X."
    assert segment_cjk(text) is text


def test_segment_cjk_is_deterministic() -> None:
    """Одинаковый вход — одинаковый выход (два вызова подряд)."""
    text = "配置管理 데이터베이스 データベース"
    assert segment_cjk(text) == segment_cjk(text)
    assert len(CJK_RANGES) >= 4