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
