"""Тесты прибора кросс-языкового замера (`scripts/measure_cross_lingual_eval.py`).

Прибор отвечает числом на вопрос «извлекает ли оценщик тот же смысл из ZH-документа,
что и из EN», и ломается он тихо: κ получается правдоподобным при любом из дефектов
ниже. Поэтому каждый проверяется отдельно и **без сети, без LLM** — на внедрённых швах.

* **выбор параметров по ASCII-якорю** — параметр без латинского идентификатора в
  ``concept_terms.en`` обязан быть отброшен **с причиной**: молчаливый пропуск уменьшил бы
  знаменатель и выдал «выровняли всё» там, где часть каталога не выравнивается вообще.
* **выравнивание по якорю** — EN-чанк и ZH-чанк с одним якорем попадают в слайс, чанк без
  якоря — нет; ``max`` не считается якорем в ``max_connections`` (иначе слайс — случайный
  кусок, где встретилось слово).
* **κ и матрица переходов** — на заранее заданных статусах, включая ``None`` (ячейка без
  вердикта не пара и в κ не входит).
* **``✅→❓`` и ``✅→❌`` раздельно** — это разные дефекты (не понял перевод против
  «перевод сказал обратное»), и свёрнутый счётчик потерял бы именно второй.
* **пустой вход** — ``None``, не ``0``: деления на ноль быть не может, а выдуманное
  согласие хуже отсутствующего.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "measure_cross_lingual_eval.py"
    spec = importlib.util.spec_from_file_location("measure_cross_lingual_eval", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_cross_lingual_eval"] = module
    spec.loader.exec_module(module)
    return module


measure = _load_module()

CatalogRow = measure.CatalogRow
Chunk = measure.Chunk
IndexEntry = measure.IndexEntry


def _row(concept_terms: dict[str, list[str]], *, param_id: str = "p1") -> Any:
    """Строка каталога с заданными терминами (остальные поля — минимальные валидные)."""
    return CatalogRow(
        param_id=param_id,
        title="Параметр",
        module="core",
        group="g",
        importance="Вж",
        param_type="fact",
        rule="Правило.",
        absence_rule="explicit",
        role_binding="core_engine",
        concept_terms=concept_terms,
        search_langs=["ru", "en"],
    )


def _chunk(text: str, no: int, *, doc: str = "d") -> Any:
    """Чанк для выравнивания: адрес, номер и текст (остальное — как у чанкера)."""
    return Chunk(
        chunk_id=f"{doc}#{no}",
        chunk_no=no,
        text=text,
        heading_path="h",
        context_prefix="",
        content_hash="0" * 16,
    )


def _entry(name: str, path: str, *, fragment: bool = False) -> Any:
    """Запись предметного указателя."""
    return IndexEntry(name=name, path=path, has_fragment=fragment)


# --------------------------------------------------------------------------- #
# Выбор параметров по ASCII-якорю
# --------------------------------------------------------------------------- #


def test_anchor_chosen_when_en_term_is_an_ascii_identifier() -> None:
    """Термин-идентификатор с записью в указателе становится якорем параметра."""
    entries = [_entry("max_connections", "runtime-config-connection.html")]
    choice = measure.choose_anchor(_row({"en": ["max_connections"], "ru": ["соединения"]}), entries)
    assert choice.anchor == "max_connections"
    assert choice.paths == ("runtime-config-connection.html",)
    assert choice.reason is None


def test_param_without_ascii_term_is_dropped_with_reason() -> None:
    """Нет латинского идентификатора — отбраковка ``no_ascii_term``, а не тихий пропуск."""
    entries = [_entry("max_connections", "runtime-config-connection.html")]
    choice = measure.choose_anchor(_row({"en": [], "ru": ["Разработчик"]}), entries)
    assert choice.anchor is None
    assert choice.paths == ()
    assert choice.reason == "no_ascii_term"


def test_anchor_absent_from_index_is_dropped_with_reason() -> None:
    """Идентификатор есть, но указатель его не знает — причина ``anchor_not_in_index``."""
    entries = [_entry("wal", "wal.html")]
    choice = measure.choose_anchor(_row({"en": ["zabbix"]}), entries)
    assert choice.anchor is None
    assert choice.reason == "anchor_not_in_index"


def test_anchor_prefers_underscore_and_longer_term() -> None:
    """Ранг якоря: подчёркивание и длина, а не порядок в каталоге."""
    entries = [
        _entry("sql", "sql-syntax.html"),
        _entry("max_connections", "runtime-config-connection.html"),
    ]
    choice = measure.choose_anchor(_row({"en": ["sql", "max_connections"]}), entries)
    assert choice.anchor == "max_connections"


def test_anchor_paths_put_exact_entries_before_prefix_entries() -> None:
    """Точное имя указателя идёт раньше «имя + слово»; пути не дублируются."""
    entries = [
        _entry("wal level", "runtime-config-wal.html", fragment=True),
        _entry("wal", "wal.html"),
        _entry("wal level", "runtime-config-wal.html", fragment=True),
    ]
    assert measure.anchor_paths(entries, "wal") == (
        "wal.html",
        "runtime-config-wal.html",
    )


def test_anchor_matching_respects_ascii_token_boundaries() -> None:
    """``max`` не якорь в ``max_connections``: границы токена, а не подстрока."""
    pattern = measure.anchor_pattern("max")
    assert pattern.search("значение max равно 100") is not None
    assert pattern.search("max_connections = 100") is None


# --------------------------------------------------------------------------- #
# Выравнивание по якорю
# --------------------------------------------------------------------------- #


def test_alignment_takes_chunks_with_the_same_anchor_on_both_sides() -> None:
    """Один якорь — обе стороны получают свои чанки; чанк без якоря не попадает."""
    en = [
        _chunk("Connection settings\n\nmax_connections (integer)", 0),
        _chunk("Unrelated paragraph about indexes.", 1),
    ]
    zh = [
        _chunk("连接设置\n\nmax_connections (integer)", 0),
        _chunk("无关段落。", 1),
    ]
    assert [c.chunk_no for c in measure.anchor_chunks(en, "max_connections")] == [0]
    assert [c.chunk_no for c in measure.anchor_chunks(zh, "max_connections")] == [0]


def test_alignment_is_case_insensitive_for_uppercase_identifiers() -> None:
    """Имя команды в переводе остаётся латиницей: регистр не значим, сущность та же."""
    chunks = [_chunk("VACUUM FULL перестраивает таблицу.", 0)]
    assert measure.anchor_chunks(chunks, "vacuum")
    assert measure.anchor_chunks([_chunk("vakuum", 0)], "vacuum") == ()


def test_alignment_limits_slice_to_k_chunks() -> None:
    """Не больше K чанков на сторону: слайс не растёт с размером страницы."""
    chunks = [_chunk(f"WAL запись {no}", no) for no in range(5)]
    assert [c.chunk_no for c in measure.anchor_chunks(chunks, "wal", limit=3)] == [0, 1, 2]
    assert measure.anchor_chunks(chunks, "wal", limit=0) == ()


def test_alignment_absent_on_one_side_yields_empty_slice() -> None:
    """Якоря нет в ZH-тексте — сторона не выровнена (пустой слайс, не «похожий» чанк)."""
    zh = [_chunk("连接设置\n\n- 指定服务器用于监听的TCP/IP地址。", 0)]
    assert measure.anchor_chunks(zh, "max_connections") == ()


# --------------------------------------------------------------------------- #
# κ, матрица переходов, два вида ✅→
# --------------------------------------------------------------------------- #


def _record(en: str | None, zh: str | None, *, param_id: str = "p1", path: str = "a.html") -> Any:
    """Запись замера одного режима (для сводки)."""
    return {
        "param_id": param_id,
        "anchor": "x",
        "path": path,
        "aligned": {"status_en": en, "status_zh": zh},
        "retrieval": {"status_en": en, "status_zh": zh},
    }


def test_kappa_and_matrix_on_fixed_statuses() -> None:
    """κ считается существующей функцией, матрица — числами по парам."""
    records = [
        _record("✅", "✅"),
        _record("✅", "✅"),
        _record("❌", "❌"),
        _record("❓", "✅"),
    ]
    summary = measure.summarize(records, "aligned", bootstrap=50)
    assert summary["paired"] == 4
    assert summary["kappa"] is not None
    assert 0.0 <= summary["kappa"] <= 1.0
    matrix = {(row["en"], row["zh"]): row["n"] for row in summary["matrix"]}
    assert matrix == {("✅", "✅"): 2, ("❌", "❌"): 1, ("❓", "✅"): 1}


def test_ok_to_unknown_and_ok_to_absent_are_counted_separately() -> None:
    """``✅→❓`` (не понял) и ``✅→❌`` (сказал обратное) — разные дефекты, разные счётчики."""
    records = [
        _record("✅", "❓"),
        _record("✅", "❓"),
        _record("✅", "❌"),
        _record("✅", "✅"),
    ]
    summary = measure.summarize(records, "aligned", bootstrap=50)
    assert summary["ok_to_unknown"] == 2
    assert summary["ok_to_absent"] == 1


def test_records_without_verdict_are_not_pairs() -> None:
    """Ячейка без вердикта (``failed``/``no_fragments``) — не пара, но видна числом."""
    records = [_record("✅", "✅"), _record(None, "✅"), _record("✅", None)]
    summary = measure.summarize(records, "aligned", bootstrap=50)
    assert summary["paired"] == 1
    assert summary["unpaired"] == 2


def test_empty_input_gives_none_not_zero() -> None:
    """Пустой вход: κ ``None`` и пустая матрица — без деления на ноль и без выдумки."""
    summary = measure.summarize([], "aligned", bootstrap=50)
    assert summary["paired"] == 0
    assert summary["kappa"] is None
    assert summary["undefined_reason"]
    assert summary["matrix"] == []
    assert summary["ok_to_unknown"] == 0
    assert summary["ok_to_absent"] == 0
    assert summary["kappa_ci95"] == {"low": None, "high": None}


def test_bootstrap_returns_none_for_degenerate_pairs() -> None:
    """Вырожденные пары (одна метка на обеих сторонах) κ не определяют — интервала нет."""
    low, high = measure.bootstrap_kappa([("✅", "✅")] * 5, iterations=20)
    assert low is None
    assert high is None


def test_bootstrap_interval_brackets_the_point_estimate() -> None:
    """Интервал по парам накрывает точечную оценку (при разбросе меток)."""
    pairs = [("✅", "✅"), ("✅", "❓"), ("❌", "❌"), ("⚠️", "✅"), ("❓", "❓")] * 4
    low, high = measure.bootstrap_kappa(pairs, iterations=300)
    assert low is not None
    assert high is not None
    assert low <= high


def test_ratio_is_none_when_denominator_is_zero() -> None:
    """Отношение длин на нулевом знаменателе — ``None``, а не ``0``/``inf``."""
    assert measure._ratio(10, 0) is None
    assert measure._ratio(9_901, 25_121) == pytest.approx(0.394, abs=1e-3)


# --------------------------------------------------------------------------- #
# Разбор перечня страниц и предметного указателя (шов «где взять страницу»)
# --------------------------------------------------------------------------- #


def test_parse_sitemap_keeps_only_docs_18_pages() -> None:
    """Из sitemap берутся только страницы ``/docs/18/*.html``, без дублей."""
    xml = (
        "<urlset>"
        "<url><loc>https://www.postgresql.org/docs/18/wal.html</loc></url>"
        "<url><loc>https://www.postgresql.org/docs/18/wal.html</loc></url>"
        "<url><loc>https://www.postgresql.org/docs/17/wal.html</loc></url>"
        "<url><loc>https://www.postgresql.org/about/news.html</loc></url>"
        "</urlset>"
    )
    assert measure.parse_sitemap(xml) == ("https://www.postgresql.org/docs/18/wal.html",)


def test_parse_index_entries_reads_name_path_and_fragment() -> None:
    """Запись указателя: термин до запятой, путь и признак «ссылка на раздел»."""
    html = (
        '<dl><dt id="i1">max_connections configuration parameter, '
        '<a class="indexterm" href="runtime-config-connection.html#SETTINGS">Connection'
        "</a></dt>"
        '<dt id="i2">WAL, <a class="indexterm" href="wal.html">Write-Ahead Log</a></dt></dl>'
    )
    entries = measure.parse_index_entries(html)
    assert [(e.name, e.path, e.has_fragment) for e in entries] == [
        ("max_connections", "runtime-config-connection.html", True),
        ("wal", "wal.html", False),
    ]


def test_one_per_slice_keeps_first_record_of_each_group() -> None:
    """Контроль корреляции: одна запись на группу ``(якорь, путь)``."""
    records = [
        {"anchor": "oracle", "path": "plpgsql-porting.html", "param_id": "p1"},
        {"anchor": "oracle", "path": "plpgsql-porting.html", "param_id": "p2"},
        {"anchor": "wal", "path": "runtime-config-wal.html", "param_id": "p3"},
    ]
    kept = measure._one_per_slice(records)
    assert [record["param_id"] for record in kept] == ["p1", "p3"]
