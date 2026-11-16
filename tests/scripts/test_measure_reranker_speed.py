"""Пины замерных функций ``scripts/measure_reranker_speed.py`` (без torch, без сети).

Проверяются ровно те вычисления, на которых держатся публикуемые числа, — на синтетике,
где ответ известен заранее:

* детерминизм выбора пар (два вызова — один набор) и цикл запросов по документам;
* ``--limit`` ограничивает число пар, синтетические ZH-пары не выпадают первыми;
* обрезка по объявленному ``max_length``: длинный документ режется, а не падает;
* перцентили ``p50``/``p95``/``max`` на известном ряду;
* отказ при отсутствии БД и при пустом срезе корпуса.

Модели не грузятся: ни ``torch``, ни ``transformers``, ни ``sentence_transformers``
тестам не нужны — токенизатор подменяется счётчиком символов. Поэтому набор проходит в
окружении без весов и без GPU.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "measure_reranker_speed.py"


def _load_harness() -> object:
    spec = importlib.util.spec_from_file_location("_measure_reranker_speed", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


MEASURE = _load_harness()


class _CharTokenizer:
    """Подмена токенизатора: один символ — один токен (+2 спец-токена).

    Замена не «приближает» токенизацию модели, а делает предикат длины **известным**:
    тест проверяет логику обрезки, а не границы BPE конкретной модели.
    """

    def __call__(
        self,
        query: str,
        doc: str | None = None,
        *,
        add_special_tokens: bool = True,
        truncation: bool = False,
    ) -> dict[str, list[int]]:
        del truncation
        total = len(query) + len(doc or "")
        if add_special_tokens:
            total += 2
        return {"input_ids": list(range(total))}


# -- перцентили ---------------------------------------------------------------


def test_percentile_linear_interpolation() -> None:
    """``p50``/``p95`` — интерполяция по позиции ``q * (n-1)``, а не «соседнее» значение."""
    ordered = [1.0, 2.0, 3.0, 4.0]
    assert MEASURE.percentile(ordered, 0.5) == pytest.approx(2.5)  # type: ignore[attr-defined]
    assert MEASURE.percentile(ordered, 0.95) == pytest.approx(3.85)  # type: ignore[attr-defined]
    assert MEASURE.percentile(ordered, 0.0) == pytest.approx(1.0)  # type: ignore[attr-defined]
    assert MEASURE.percentile(ordered, 1.0) == pytest.approx(4.0)  # type: ignore[attr-defined]


def test_percentile_empty_is_error_not_zero() -> None:
    """Пустой ряд — ``ValueError``: ноль читался бы как измеренное значение."""
    with pytest.raises(ValueError):
        MEASURE.percentile([], 0.5)  # type: ignore[attr-defined]


def test_latency_stats_known_set() -> None:
    """``p50``/``p95``/``max`` на известном ряду (мс на пару по батчам)."""
    stats = MEASURE.latency_stats([10.0, 20.0, 30.0, 40.0])  # type: ignore[attr-defined]
    assert stats["p50"] == pytest.approx(25.0)
    assert stats["p95"] == pytest.approx(38.5)
    assert stats["max"] == pytest.approx(40.0)
    assert stats["mean"] == pytest.approx(25.0)
    with pytest.raises(ValueError):
        MEASURE.latency_stats([])  # type: ignore[attr-defined]


# -- выбор пар ----------------------------------------------------------------


def test_build_pairs_is_deterministic() -> None:
    """Два вызова на тех же входах дают один набор пар (сравнение чисел между запусками)."""
    queries = ["a", "b", "c"]
    docs = ["d0", "d1", "d2", "d3", "d4"]
    first = MEASURE.build_pairs(queries, docs)  # type: ignore[attr-defined]
    second = MEASURE.build_pairs(queries, docs)  # type: ignore[attr-defined]
    assert first == second
    # Запрос циклится по документам: каждый запрос встречается.
    assert [q for q, _ in first] == ["a", "b", "c", "a", "b"]
    assert [d for _, d in first] == docs


def test_build_pairs_zh_first_and_limit() -> None:
    """``limit`` режет набор, но синтетические ZH-пары остаются первыми."""
    zh = [("zh-q", "zh-d")]
    pairs = MEASURE.build_pairs(  # type: ignore[attr-defined]
        ["q"], ["d0", "d1", "d2"], zh_pairs=zh, limit=2
    )
    assert pairs == [("zh-q", "zh-d"), ("q", "d0")]
    assert len(MEASURE.build_pairs(["q"], ["d0", "d1"], limit=1)) == 1  # type: ignore[attr-defined]
    with pytest.raises(ValueError):
        MEASURE.build_pairs(["q"], ["d0"], limit=0)  # type: ignore[attr-defined]
    with pytest.raises(ValueError):
        MEASURE.build_pairs([], ["d0"])  # type: ignore[attr-defined]


# -- обрезка по max_length ----------------------------------------------------


def test_clip_pair_cuts_long_document() -> None:
    """Длинный документ режется до бюджета токенов, а не падает и не уходит как есть."""
    tokenizer = _CharTokenizer()
    query = "0123456789"  # 10 символов
    doc = "x" * 1000
    clipped_query, clipped_doc, before, after = MEASURE.clip_pair(  # type: ignore[attr-defined]
        (query, doc), tokenizer, 50
    )
    assert clipped_query == query
    assert before == 1000
    # Бюджет: 50 - 2 спец - 10 запроса = 38 символов документа.
    assert after == 38
    assert clipped_doc == "x" * 38
    assert MEASURE._token_count(tokenizer, clipped_query, clipped_doc) <= 50  # type: ignore[attr-defined]


def test_clip_pair_keeps_fitting_document_unchanged() -> None:
    """Документ, который влезает, возвращается без изменений (обрезка не «дрожит»)."""
    tokenizer = _CharTokenizer()
    query, doc = "q", "short"
    assert MEASURE.clip_pair((query, doc), tokenizer, 100) == (query, doc, 5, 5)  # type: ignore[attr-defined]


def test_clip_pair_query_alone_over_limit_drops_document() -> None:
    """Если запрос сам длиннее предела — документ не добавляется (запрос режет токенизатор)."""
    tokenizer = _CharTokenizer()
    query = "q" * 100
    _, clipped_doc, before, after = MEASURE.clip_pair((query, "doc"), tokenizer, 10)  # type: ignore[attr-defined]
    assert clipped_doc == ""
    assert (before, after) == (3, 0)


def test_clip_pairs_reports_evidence_of_clipping() -> None:
    """Статистика обрезки показывает, сколько пар реально укоротилось."""
    tokenizer = _CharTokenizer()
    pairs = [("q", "x" * 1000), ("q", "y" * 5)]
    clipped, stats = MEASURE.clip_pairs(pairs, tokenizer, 40)  # type: ignore[attr-defined]
    assert stats["pairs"] == 2
    assert stats["clipped_pairs"] == 1
    assert stats["max_length"] == 40
    assert stats["doc_chars_max_after"] == 37
    assert len(clipped[0][1]) == 37
    assert clipped[1][1] == "y" * 5


# -- вход: запросы и корпус ---------------------------------------------------


def test_load_queries_dedupes_and_skips_empty(tmp_path: Path) -> None:
    """Запросы — ``params[].title`` без дублей и пустых; порядок объявленный."""
    path = tmp_path / "plan_inputs.json"
    path.write_text(
        json.dumps(
            {
                "params": [
                    {"title": "Первый"},
                    {"title": "Первый"},
                    {"title": "  "},
                    {"title": "Второй"},
                    {"no_title": 1},
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    assert MEASURE.load_queries(path) == ["Первый", "Второй"]  # type: ignore[attr-defined]


def test_load_queries_missing_file(tmp_path: Path) -> None:
    """Нет снимка каталога — отказ, а не пустой набор запросов."""
    with pytest.raises(SystemExit):
        MEASURE.load_queries(tmp_path / "absent.json")  # type: ignore[attr-defined]


def _make_db(path: Path, product: str, texts: list[str]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE corpus_chunks ("
            "chunk_id TEXT PRIMARY KEY, product_id TEXT, text TEXT)"
        )
        conn.executemany(
            "INSERT INTO corpus_chunks (chunk_id, product_id, text) VALUES (?, ?, ?)",
            [(f"{product}:{i:03d}", product, t) for i, t in enumerate(texts)],
        )
        conn.commit()
    finally:
        conn.close()


def test_load_documents_stride_is_deterministic(tmp_path: Path) -> None:
    """Срез — каждый ``stride``-й чанк по ``chunk_id``: не «первые N» и не ``random``."""
    db = tmp_path / "cohound.db"
    _make_db(db, "prod", [f"text-{i}" for i in range(10)])
    first = MEASURE.load_documents(db, "prod", 3)  # type: ignore[attr-defined]
    second = MEASURE.load_documents(db, "prod", 3)  # type: ignore[attr-defined]
    assert first.chunk_ids == second.chunk_ids
    assert first.texts == second.texts
    assert first.rows_total == 10
    # offsets = (i * 10) // 3 → 0, 3, 6.
    assert first.chunk_ids == ["prod:000", "prod:003", "prod:006"]


def test_load_documents_empty_product_exits(tmp_path: Path) -> None:
    """Пустой продукт — видимый отказ (не пустая выборка с нулевыми числами)."""
    db = tmp_path / "cohound.db"
    _make_db(db, "prod", ["a"])
    with pytest.raises(SystemExit):
        MEASURE.load_documents(db, "other", 5)  # type: ignore[attr-defined]


def test_main_refuses_missing_db(tmp_path: Path) -> None:
    """Прибор не работает без БД прогона: отказ до загрузки моделей."""
    with pytest.raises(SystemExit):
        MEASURE.main(  # type: ignore[attr-defined]
            ["--db", str(tmp_path / "absent.db"), "--product", "prod"]
        )


def test_vram_oversubscribed_flag() -> None:
    """Пик аллокатора больше карты — флаг; неизвестная сторона — ``None``, не ``False``."""
    assert MEASURE.vram_oversubscribed(6 * 2**30, 4 * 2**30) is True  # type: ignore[attr-defined]
    assert MEASURE.vram_oversubscribed(3 * 2**30, 4 * 2**30) is False  # type: ignore[attr-defined]
    assert MEASURE.vram_oversubscribed(None, 4 * 2**30) is None  # type: ignore[attr-defined]
    assert MEASURE.vram_oversubscribed(3 * 2**30, None) is None  # type: ignore[attr-defined]


# -- режим питания: объявленный вход, а не вывод из признаков -----------------


def test_declared_power_mode_comes_only_from_the_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Признаки «батарея, сеть отключена» не переписывают объявленный режим.

    Режим — условие замера, а USB-C и комплектный БП на этом хосте неразличимы по sysfs.
    Тест подставляет наблюдаемое «от батареи» и объявляет ``ac-barrel``: если прибор
    начнёт выводить режим из признаков, тест упадёт.
    """
    monkeypatch.setattr(MEASURE, "_ac_online", lambda: False)  # type: ignore[attr-defined]
    monkeypatch.setattr(MEASURE, "_battery_status", lambda: "Discharging")  # type: ignore[attr-defined]
    args = type("Args", (), {"power_mode": "ac-barrel"})()
    assert MEASURE._declared_power_mode(args) == "ac-barrel"  # type: ignore[attr-defined]
    # И режим не подмешивается в снимок признаков: там только наблюдённое.
    snapshot = MEASURE._power_snapshot(with_gpu=False)  # type: ignore[attr-defined]
    assert snapshot["ac_online"] is False
    assert snapshot["battery_status"] == "Discharging"
    assert "power_mode" not in snapshot


def test_power_snapshot_unknown_fields_are_none_not_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Недоступный признак — ``None``; ноль остаётся нулём только если он измерен."""
    monkeypatch.setattr(MEASURE, "_read_text", lambda path: None)  # type: ignore[attr-defined]
    snapshot = MEASURE._power_snapshot(with_gpu=False)  # type: ignore[attr-defined]
    assert snapshot["ac_online"] is None
    assert snapshot["battery_status"] is None
    # При cpu карта не опрашивается вовсе, и поля — «не измерено».
    assert snapshot["sm_clock_mhz"] is None
    assert snapshot["sw_power_cap_us"] is None


def test_power_window_keeps_absolute_counter_pair() -> None:
    """Счётчик троттлинга публикуется парой «до/после», а не разницей."""
    window = MEASURE._power_window(  # type: ignore[attr-defined]
        {"sw_power_cap_us": 500, "ac_online": True},
        {"sw_power_cap_us": 900, "ac_online": True},
    )
    assert window["before"]["sw_power_cap_us"] == 500
    assert window["after"]["sw_power_cap_us"] == 900
    # «до тоже был не ноль» видно: разницы в окне нет вовсе.
    assert set(window) == {"before", "after"}


def test_parse_sw_power_cap_counter_not_flag() -> None:
    """Из вывода ``nvidia-smi -q -d PERFORMANCE`` берётся счётчик ``us``, а не флаг."""
    text = (
        "Clocks Event Reasons\n"
        "\tIdle                             : Not Active\n"
        "\tSW Power Cap                     : Active\n"
        "Clocks Event Reasons Counters\n"
        "\tSW Power Cap                     : 12345 us\n"
    )
    assert MEASURE._parse_sw_power_cap_us(text) == 12345  # type: ignore[attr-defined]
    assert MEASURE._parse_sw_power_cap_us("") is None  # type: ignore[attr-defined]
    assert MEASURE._parse_sw_power_cap_us("\tSW Power Cap : 0 us\n") == 0  # type: ignore[attr-defined]


def test_parse_gpu_metrics_na_is_none() -> None:
    """``[N/A]`` мобильной карты — ``None``, а не ``0``."""
    metrics = MEASURE._parse_gpu_metrics("[N/A], [N/A], 45")  # type: ignore[attr-defined]
    assert metrics["sm_clock_mhz"] is None
    assert metrics["power_draw_w"] is None
    assert metrics["temperature_c"] == 45


# -- длинные документы --------------------------------------------------------


def test_build_long_documents_reaches_target_and_is_deterministic() -> None:
    """Склейка набирает объявленный бюджет токенов и повторяема (текст корпусный)."""
    tokenizer = _CharTokenizer()
    docs = ["x" * 100, "y" * 100, "z" * 100]
    long_docs = MEASURE.build_long_documents(  # type: ignore[attr-defined]
        docs, tokenizer, "q", count=2, target_tokens=250
    )
    assert len(long_docs) == 2
    # 100 символов на чанк + "\n\n" ⇒ 202, 304 …: третий чанк уже перебирает бюджет.
    assert MEASURE._token_count(tokenizer, "q", long_docs[0]) >= 250  # type: ignore[attr-defined]
    assert long_docs[0].startswith("x" * 100)
    assert long_docs[1].startswith("y" * 100)
    again = MEASURE.build_long_documents(  # type: ignore[attr-defined]
        docs, tokenizer, "q", count=2, target_tokens=250
    )
    assert again == long_docs


def test_pair_source_labels_and_builds() -> None:
    """``doc_set`` называет набор, а сборка идёт тем же детерминированным правилом."""
    source = MEASURE.PairSource(  # type: ignore[attr-defined]
        queries=["q"], documents=["d0", "d1"], zh_pairs=[("zh", "d")]
    )
    assert source.doc_set == "corpus"
    assert source.build(_CharTokenizer()) == [("zh", "d"), ("q", "d0"), ("q", "d1")]
    long_source = MEASURE.PairSource(  # type: ignore[attr-defined]
        queries=["q"],
        documents=["a" * 50, "b" * 50],
        zh_pairs=[],
        long_count=2,
        long_target_tokens=60,
    )
    assert long_source.doc_set == "long:2@60"
    built = long_source.build(_CharTokenizer())
    assert len(built) == 2
    assert built[0][1].startswith("a" * 50) and "b" in built[0][1]
