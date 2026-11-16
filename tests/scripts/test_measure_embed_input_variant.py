"""Пины прибора ``scripts/measure_embed_input_variant.py`` (без БД, эмбеддера и GPU).

Прибор опубликовал число оси ``EMBED_INPUT_FIELDS`` (O19), и у него уже были прогоны —
поэтому тесты фиксируют не «покрытие», а те свойства, поломка которых даёт **тихо
неверное число**:

* **``_subset`` берёт равномерный шаг, а не первые N.** Первые 2000 чанков
  ``postgrespro`` покрывают 168 документов из 15 188 (замер 16.11.2026) — такой срез
  даёт ``hit@K = 1.000`` на всех вариантах, потому что чужих документов в матрице нет;
  префикс, выданный за срез, превращает дымовое число в число оси;
* **``_MatrixIndex._top`` ранжирует ключом ``(-балл, chunk_id)``, как адаптер**: иначе
  варианты сравнивались бы через другое ядро, и «эффект заголовка» включал бы разницу
  ранжирования. Отдельно — путь полного перебора при ``nan``/``inf``: ``np.partition``
  по нефинитным баллам даёт пустую выдачу, то есть тихий ноль вместо ответа;
* **нулевая строка нормируется без деления на ноль**: ``nan`` в балле — это не «плохой
  чанк», а сломанная метрика всей ячейки;
* **``smoke_only``** в отчёте: без него метрики среза читаются как метрики оси;
* **``_resolve_variants``** отказывает на неизвестном имени, дубле и пустом списке —
  иначе дубль сравнивает вариант сам с собой и печатает нулевую разницу как результат;
* **``_write_cells_csv``**: ``delta = базовый − второй`` и **пуст**, если один ранг
  ``None``; ``None``, записанный как ``0``, врёт в диффе ранжирования.
* **Условия замера публикуются раздельно**: ``chunks`` — сколько чанков измерено (после
  среза), ``partition_chunks`` — размер партиции (только у ``source == "partition"``).
  Слитые в одно число, они утверждают, что база мерилась на 206 931 чанке там, где
  измерено 3 000 (замер 16.11.2026, ``--limit-chunks 3000``). Рядом — инвариант: у всех
  вариантов отчёта ``chunks`` совпадает, иначе их числа несравнимы и отчёт отказывает.

Синтетика: матрицы ``numpy``, строки ``_Row`` и ``tmp_path`` для CSV. Стенд
``~/.cohound``, ``torch``/``cuda`` и БД не трогаются.
"""

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from core.ports.vector_index import VectorQuery

_SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "measure_embed_input_variant.py"
)


def _load_harness() -> Any:
    spec = importlib.util.spec_from_file_location("_measure_embed_input_variant", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


MEASURE = _load_harness()


def _row(chunk_id: str, *, text: str = "", doc_id: str = "doc") -> Any:
    """Строка чанка: прибору нужен минимум для входа эмбеддера и ранжирования."""
    return MEASURE._Row(
        chunk_id=chunk_id,
        doc_id=doc_id,
        product_id="product",
        text=text or f"text {chunk_id}",
        heading_path="",
        context_prefix="",
        content_hash=f"hash-{chunk_id}",
    )


def _index(chunk_ids: list[str], matrix: Any) -> Any:
    return MEASURE._MatrixIndex(
        [_row(chunk_id) for chunk_id in chunk_ids],
        matrix,
        build_id="variant:test",
        dim=int(np.asarray(matrix).shape[1]),
    )


# --------------------------------------------------------------------------- _subset


def test_subset_takes_even_step_not_prefix() -> None:
    """Префикс вместо шага: срез из 2000 чанков = 168 документов ⇒ hit@K 1.000 у всех."""
    order = [f"chunk-{index:05d}" for index in range(1000)]

    subset = MEASURE._subset(order, 100)

    assert len(subset) == 100
    assert subset[0] == order[0]
    # Главное утверждение: это не первые 100 чанков.
    assert subset[-1] != order[99]
    assert len(set(subset)) == len(subset)
    positions = [order.index(chunk_id) for chunk_id in subset]
    assert positions == sorted(positions)
    assert len(set(positions)) == len(positions)
    assert positions[-1] >= 900


@pytest.mark.parametrize("limit", [0, -5, 1000, 5000])
def test_subset_returns_everything_when_limit_is_not_a_cut(limit: int) -> None:
    """``limit=0``/``limit >= len`` — полный корпус, а не пустой список и не срез."""
    order = [f"chunk-{index:05d}" for index in range(1000)]

    assert MEASURE._subset(order, limit) == order


def test_subset_returns_a_copy() -> None:
    """Выдача не делит список с порядком корпуса: срез не имеет права править ``order``."""
    order = ["a", "b", "c"]

    subset = MEASURE._subset(order, 0)
    subset.append("d")

    assert order == ["a", "b", "c"]


# ------------------------------------------------------------------ _MatrixIndex._top


def test_top_ties_break_by_chunk_id() -> None:
    """Равный балл без tie-break: порядок вариантов зависел бы от порядка строк БД."""
    index = _index(["c", "a", "b"], [[1.0, 0.0], [1.0, 0.0], [1.0, 0.0]])

    hits = index.search(VectorQuery(product_ids=["product"], top_k=2, vector=[1.0, 0.0]))

    assert [hit.chunk_id for hit in hits] == ["a", "b"]
    assert hits[0].score == hits[1].score


def test_top_slices_to_top_k_and_keeps_descending_order() -> None:
    """Срез больше/меньше ``top_k``: лишние строки попадают в метрику как «найденные»."""
    index = _index(
        ["a", "b", "c", "d", "e"],
        [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0], [0.5, 0.5, 0.0], [0.0, 0.0, 0.5]],
    )

    two = index.search(VectorQuery(product_ids=["product"], top_k=2, vector=[1.0, 0.0, 0.0]))
    assert [hit.chunk_id for hit in two] == ["a", "d"]

    every = index.search(
        VectorQuery(product_ids=["product"], top_k=99, vector=[1.0, 0.0, 0.0])
    )
    assert len(every) == 5
    assert [hit.chunk_id for hit in every] == ["a", "d", "b", "c", "e"]
    scores = [hit.score for hit in every]
    assert scores == sorted(scores, reverse=True)


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_top_nonfinite_scores_scan_every_row(bad: float) -> None:
    """``np.partition`` по ``nan``/``inf`` даёт пустую выдачу: ноль вместо ответа."""
    index = _index(["a", "b", "c", "d", "e"], np.eye(5, 3, dtype=np.float32))
    scores = np.array([bad, 0.9, 0.8, 0.7, 0.6], dtype=np.float32)

    hits = index._top(scores, 2)

    assert len(hits) == 2
    assert len({hit.chunk_id for hit in hits}) == 2


def test_zero_row_scores_zero_not_nan() -> None:
    """Деление на нулевую норму: ``nan`` в балле ломает метрику всей ячейки."""
    index = _index(["zero", "a", "b"], [[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])

    hits = index.search(VectorQuery(product_ids=["product"], top_k=3, vector=[1.0, 0.0, 0.0]))

    assert len(hits) == 3
    assert all(np.isfinite(hit.score) for hit in hits)
    by_id = {hit.chunk_id: hit.score for hit in hits}
    assert by_id["zero"] == 0.0


# --------------------------------------------------------------------- smoke_only


def test_smoke_only_flag_marks_a_cut_corpus() -> None:
    """Срез корпуса без флага: метрики дымового прогона публикуются как число оси."""
    full_order = [f"chunk-{index:05d}" for index in range(100)]

    assert MEASURE._is_smoke_only(full_order[:10], full_order) is True
    assert MEASURE._is_smoke_only(full_order, full_order) is False


# --------------------------------------------------------------- _resolve_variants


def test_resolve_variants_rejects_unknown_name_with_the_allowed_list() -> None:
    """Опечатка в имени варианта: иначе мерился бы не тот вход, что назван в отчёте."""
    with pytest.raises(SystemExit) as excinfo:
        MEASURE._resolve_variants("text,headings")

    message = str(excinfo.value.code)
    assert "headings" in message
    for name in MEASURE.VARIANTS:
        assert name in message


def test_resolve_variants_rejects_duplicate() -> None:
    """Дубль: сравнение варианта с самим собой даёт нулевую разницу как результат."""
    with pytest.raises(SystemExit) as excinfo:
        MEASURE._resolve_variants("text,text")

    assert "дважды" in str(excinfo.value.code)


@pytest.mark.parametrize("raw", ["", "  ", ",", " , "])
def test_resolve_variants_rejects_empty(raw: str) -> None:
    """Пустой список: мерить нечего, а молчаливый ``[]`` даёт отчёт без вариантов."""
    with pytest.raises(SystemExit) as excinfo:
        MEASURE._resolve_variants(raw)

    assert "пуст" in str(excinfo.value.code)


# ---------------------------------------------------------------- _write_cells_csv


def test_write_cells_csv_delta_is_base_minus_other_and_blank_on_none(
    tmp_path: Path,
) -> None:
    """``None`` в ранге, ставший ``0``, и перевёрнутый знак — два способа соврать в диффе."""
    path = tmp_path / "cells.csv"
    cells = {
        "text": [
            {"cell": "p1", "rank": 3},
            {"cell": "p2", "rank": None},
            {"cell": "p3", "rank": 5},
        ],
        "heading": [
            {"cell": "p1", "rank": 5},
            {"cell": "p2", "rank": 2},
            {"cell": "p3", "rank": None},
        ],
    }

    MEASURE._write_cells_csv(path, ["text", "heading"], cells)

    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle))

    assert rows[0] == ["cell", "text", "heading", "delta"]
    assert rows[1] == ["p1", "3", "5", "-2"]
    # Хотя бы один ранг ``None`` ⇒ delta пуст, а не ``-2``/``0``.
    assert rows[2] == ["p2", "", "2", ""]
    assert rows[3] == ["p3", "5", "", ""]


# ------------------------------------- условия замера: срез и публикация (O19)


def _partition_stats(size: int) -> dict[str, Any]:
    """Статистика базового варианта ровно в той форме, что отдаёт ``_partition_matrix``."""
    return {
        "source": "partition",
        "partition_build_id": "pb_test",
        "composition_id": "cmp_test",
        "chunks": size,
        "partition_chunks": size,
        "seconds": 0.0,
        "chunks_per_s": None,
        "cache": None,
    }


def test_partition_slice_publishes_measured_chunks_not_partition_size() -> None:
    """Срез: ``chunks`` — 100 измеренных, ``partition_chunks`` — 1000 в сборке.

    Слитые в одно поле, они публикуют дымовое число как число оси.
    """
    partition_ids = [f"chunk-{index:05d}" for index in range(1000)]
    order = MEASURE._subset(partition_ids, 100)
    matrix = np.arange(1000 * 2, dtype=np.float32).reshape(1000, 2)
    stats = _partition_stats(1000)

    sliced_order, sliced_matrix = MEASURE._apply_partition_slice(
        order, partition_ids, matrix, stats
    )

    assert len(sliced_order) == 100
    assert stats["chunks"] == 100
    assert stats["partition_chunks"] == 1000
    # Порядок и матрица режутся вместе: строка ``i`` матрицы принадлежит ``sliced_order[i]``.
    positions = [partition_ids.index(chunk_id) for chunk_id in sliced_order]
    assert sliced_matrix.shape[0] == len(sliced_order)
    assert [int(row[0]) // 2 for row in sliced_matrix] == positions


def test_no_slice_publishes_equal_chunk_counts() -> None:
    """Без среза измерено всё: ``chunks == partition_chunks`` (их правит только срез)."""
    published = MEASURE._condition_fields(_partition_stats(206_931))

    assert published["chunks"] == published["partition_chunks"] == 206_931


def test_conditions_separate_measured_chunks_from_partition_size() -> None:
    """Публикация не склеивает два факта в одно поле (отчёт 16.11.2026: 206 931 / 3 000)."""
    stats = _partition_stats(206_931)
    stats["chunks"] = 3000

    published = MEASURE._condition_fields(stats)

    assert published["chunks"] == 3000
    assert published["partition_chunks"] == 206_931


def test_recomputed_variant_has_no_partition_chunks() -> None:
    """Пересчитанный вариант партицию не читал: поля ``partition_chunks`` у него нет."""
    published = MEASURE._condition_fields(
        {"source": "recomputed", "chunks": 3000, "seconds": 438.008, "chunks_per_s": 6.85}
    )

    assert published["chunks"] == 3000
    assert published["partition_chunks"] is None


def test_require_same_chunks_rejects_variants_measured_on_different_corpora() -> None:
    """База на 206 931 и вариант на 3 000: их числа несравнимы, отчёт обязан отказать."""
    reports = [
        {"variant": "text", "chunks": 206_931},
        {"variant": "heading", "chunks": 3000},
    ]

    with pytest.raises(SystemExit) as excinfo:
        MEASURE._require_same_chunks(reports)

    message = str(excinfo.value.code)
    assert "206931" in message
    assert "3000" in message
    assert "text" in message
    assert "heading" in message


def test_require_same_chunks_rejects_unknown_count() -> None:
    """``None`` (неизвестно, сколько измерено) — не «совпадает»: сравнивать нечем."""
    with pytest.raises(SystemExit):
        MEASURE._require_same_chunks(
            [{"variant": "text", "chunks": None}, {"variant": "heading", "chunks": 3000}]
        )


def test_require_same_chunks_accepts_equal_counts() -> None:
    """Одинаковое число измеренных чанков — условие сравнимости выполнено."""
    MEASURE._require_same_chunks(
        [{"variant": "text", "chunks": 3000}, {"variant": "heading", "chunks": 3000}]
    )


def test_partition_matrix_publishes_partition_size(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_partition_matrix`` публикует размер партиции отдельным полем, а не только ``chunks``.

    Проверяется сама функция, а не ``_condition_fields``: поле заполняется **здесь**, и
    удаление строки из неё оставляло бы ``partition_chunks = None`` у базового варианта —
    то есть размер сборки терялся бы молча, ровно тот дефект, ради которого поле заведено.
    """
    from types import SimpleNamespace

    vectors = np.zeros((3, 4), dtype=np.float32)
    data = SimpleNamespace(vectors=vectors, chunk_ids=["a", "b", "c"])
    view = SimpleNamespace(
        composition_id="comp-1", build_id_for=lambda product: f"pb_{product}"
    )

    calls: list[tuple[Any, ...]] = []

    def fake_partition(index_root: Any, package: str, build_id: str, product: str, **_: Any) -> Any:
        calls.append((package, build_id, product))
        return data

    monkeypatch.setattr("core.adapters.index_format.read_partition", fake_partition)
    monkeypatch.setattr("core.pipeline.composition.read_composition", lambda *a, **k: view)

    matrix, chunk_ids, stats = MEASURE._partition_matrix(Path("/nowhere"), "pkg", "comp-1", "prod")

    assert calls == [("pkg", "pb_prod", "prod")]
    assert chunk_ids == ["a", "b", "c"]
    assert matrix is vectors
    assert stats["chunks"] == 3
    assert stats["partition_chunks"] == 3
    assert stats["source"] == "partition"


def test_build_payload_rejects_incomparable_variants() -> None:
    """Путь сборки отчёта неотделим от проверки: несравнимые строки не выйдут наружу.

    Проверка стоит в ``_build_payload``, а не в ``main``, именно ради этого теста: вызов
    из ``main`` можно удалить, и отчёт вышел бы молча.
    """
    reports = [
        {"variant": "text", "chunks": 206_931},
        {"variant": "heading", "chunks": 3000},
    ]

    with pytest.raises(SystemExit):
        MEASURE._build_payload(reports, {"run_id": "r"}, peak_rss_bytes=None, peak_vram_bytes=None)


def test_build_payload_keeps_base_fields_and_variants() -> None:
    """Сборка не теряет поля условий и кладёт ``variants`` последним (порядок — часть формата)."""
    reports = [{"variant": "text", "chunks": 3000}, {"variant": "heading", "chunks": 3000}]

    payload = MEASURE._build_payload(
        reports, {"run_id": "r", "chunks_measured": 3000}, peak_rss_bytes=1, peak_vram_bytes=2
    )

    assert payload["run_id"] == "r"
    assert payload["chunks_measured"] == 3000
    assert payload["peak_rss_bytes"] == 1
    assert payload["peak_vram_bytes"] == 2
    assert payload["variants"] == reports
    assert list(payload)[-1] == "variants"


def test_load_average_published_as_measurement_condition() -> None:
    """Нагрузка хоста — часть условий, а не украшение отчёта.

    ``chunks_per_s`` на общем хосте без неё невоспроизводим: замер 16.11.2026 шёл при
    занятом хосте (рядом работали pytest и другой прибор), и без поля «6.85 чанк/с»
    читается как цена оси, а не как цена конкуренции за хост.
    """
    load = MEASURE._load_average()
    assert load is None or (len(load) == 3 and all(isinstance(v, float) for v in load))

    payload = MEASURE._build_payload(
        [{"variant": "text", "chunks": 3000}],
        {"run_id": "r"},
        peak_rss_bytes=1,
        peak_vram_bytes=None,
        load_average=[7.4, 6.5, 4.7],
    )
    assert payload["load_average"] == [7.4, 6.5, 4.7]
    # Ключ обязан быть в отчёте и когда счётчика нет: иначе поле «иногда есть» нельзя
    # отличить от «не измеряли» при разборе старых артефактов.
    payload = MEASURE._build_payload(
        [{"variant": "text", "chunks": 3000}],
        {"run_id": "r"},
        peak_rss_bytes=1,
        peak_vram_bytes=None,
    )
    assert "load_average" in payload
    assert payload["load_average"] is None
