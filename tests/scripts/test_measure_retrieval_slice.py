"""Тесты диагностики среза (`scripts/measure_retrieval_slice.py`).

Прибор отвечает на вопрос «сколько `hit@K` стоит срез по чанкам вместо среза по K
документам». Ошибка здесь даёт правдоподобное число, поэтому проверяются не «функции
вызываются», а свойства, поломка которых меняет вывод:

- срез по документам обязан смотреть **всю** выдачу: если он унаследует срез по чанкам,
  второе число совпадёт с первым, и прибор отрапортует «цена среза = 0» — то есть ровно
  то, что он призван опровергнуть;
- чанк без записи в карте документов пропускается, как в
  :func:`core.pipeline.retrieval_docs.document_ids_for` — иначе прибор считал бы по
  другому правилу, чем метрика, и его числа были бы с ней несравнимы;
- порядок трёх чисел: `by_chunks ≤ by_documents ≤ anywhere` на любых данных. Нарушение
  означает, что срезы перепутаны местами.

Сеть, БД, эмбеддер и GPU не нужны: проверяется чистая функция и разбор аргументов.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль прибора по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "measure_retrieval_slice.py"
    spec = importlib.util.spec_from_file_location("measure_retrieval_slice", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["measure_retrieval_slice"] = module
    spec.loader.exec_module(module)
    return module


slice_mod = _load_module()


#: Карта, в которой чанки 0–9 принадлежат документам doc0…doc9, а чанк 10 — документу
#: doc0 (второй чанк того же документа: дедуп обязан его свернуть).
_CHUNK_DOC = {f"c{index:02d}": f"doc{index}" for index in range(10)}
_CHUNK_DOC["c10"] = "doc0"


def test_slice_by_documents_looks_past_the_chunk_cut() -> None:
    """Срез по документам видит документ, который по чанкам стоит за K.

    Это и есть предмет прибора. Выдача: 10 чанков, принадлежащих **двум** документам
    (по пять на каждый), и 11-й чанк — третьего документа. По чанкам третий документ
    за срезом; по документам он **третий**, то есть в первой десятке. Если бы второе
    число считалось по `chunk_ids[:k]`, прибор показывал бы «цена среза 0» на данных,
    где цена есть.
    """
    chunk_doc = {f"c{index:02d}": ("docA" if index < 5 else "docB") for index in range(10)}
    chunk_doc["c10"] = "docC"
    chunk_ids = [f"c{index:02d}" for index in range(11)]

    hits = slice_mod.slice_hits(chunk_ids, chunk_doc, {"docC"}, k=10)
    assert hits.by_chunks is False, "срез по 10 чанкам не должен видеть 11-й чанк"
    assert hits.by_documents is True, "по документам docC — третий, он в первой десятке"
    assert hits.anywhere is True

    # Тот же случай, но ожидание — документ, который виден обоими срезами: контроль,
    # что первое число не «всегда False».
    hits = slice_mod.slice_hits(chunk_ids, chunk_doc, {"docA"}, k=10)
    assert hits.by_chunks is True
    assert hits.by_documents is True


def test_by_documents_counts_documents_not_chunks() -> None:
    """Десять чанков одного документа — это один документ, а не десять.

    Ловушка, ради которой прибор написан: `document_ids_for(..., top_k=10)` даёт меньше
    десяти документов, если чанки одного документа повторяются. Проверка фиксирует, что
    второе число считается по дедуплицированному списку документов.
    """
    chunk_doc = {f"c{index:02d}": "same" for index in range(10)}
    chunk_doc["c10"] = "second"
    chunk_ids = [f"c{index:02d}" for index in range(11)]
    hits = slice_mod.slice_hits(chunk_ids, chunk_doc, {"second"}, k=10)
    assert hits.by_chunks is False  # c10 за срезом
    assert hits.by_documents is True  # документов всего два, оба в первой десятке


def test_chunk_without_document_is_skipped_like_the_metric() -> None:
    """Чанк без записи в карте пропускается: то же правило, что у метрики (I19).

    Если бы прибор «выдумывал» документ для такого чанка, он считал бы по другому
    правилу, чем `document_ids_for`, и его числа нельзя было бы сравнивать с
    опубликованными.
    """
    chunk_doc = {"c00": "doc0"}  # c01 отсутствует
    hits = slice_mod.slice_hits(["c01", "c00"], chunk_doc, {"doc0"}, k=10)
    assert hits.by_chunks is True
    assert hits.by_documents is True

    hits = slice_mod.slice_hits(["c01"], chunk_doc, {"doc0"}, k=10)
    assert hits.by_chunks is False
    assert hits.by_documents is False
    assert hits.anywhere is False


def test_empty_retrieval_hits_nothing() -> None:
    """Пустая выдача — три `False`, а не исключение: ячейка без хитов законна."""
    hits = slice_mod.slice_hits([], _CHUNK_DOC, {"doc0"}, k=10)
    assert (hits.by_chunks, hits.by_documents, hits.anywhere) == (False, False, False)


def test_expected_document_beyond_union_is_not_a_hit() -> None:
    """Ожидаемого документа нет в выдаче вовсе — все три `False` (не «нашлось хоть где-то»)."""
    hits = slice_mod.slice_hits(["c00", "c01"], _CHUNK_DOC, {"absent"}, k=10)
    assert (hits.by_chunks, hits.by_documents, hits.anywhere) == (False, False, False)


def test_multiple_expected_documents_are_a_set() -> None:
    """Набор ожиданий: попадание любого члена — попадание (у ячейки может быть несколько)."""
    hits = slice_mod.slice_hits(["c00", "c01"], _CHUNK_DOC, {"absent", "doc1"}, k=10)
    assert hits.by_chunks is True


@pytest.mark.parametrize("k", [1, 5, 10])
def test_three_numbers_are_ordered(k: int) -> None:
    """`by_chunks ≤ by_documents ≤ anywhere` при любом K: перепутанные срезы ловятся здесь."""
    chunk_ids = [f"c{index:02d}" for index in range(11)]
    chunk_doc = dict(_CHUNK_DOC)
    chunk_doc["c10"] = "late"
    for expected in ({"doc0"}, {"late"}, {"doc9"}, {"absent"}):
        hits = slice_mod.slice_hits(chunk_ids, chunk_doc, expected, k=k)
        assert int(hits.by_chunks) <= int(hits.by_documents) <= int(hits.anywhere)


def test_resolve_depths_keeps_order_and_drops_duplicates() -> None:
    """Порядок задан оператором; дубль — не второй прогон, а тот же."""
    assert slice_mod._resolve_depths("40,10,20,10") == (40, 10, 20)


@pytest.mark.parametrize("raw", ["", " ", ",", " , "])
def test_resolve_depths_rejects_empty(raw: str) -> None:
    """Пустой список — отказ, а не «ни одной глубины»: отчёт без строк выглядел бы как ноль."""
    with pytest.raises(SystemExit):
        slice_mod._resolve_depths(raw)


@pytest.mark.parametrize("raw", ["0", "-1", "10,-5"])
def test_resolve_depths_rejects_non_positive(raw: str) -> None:
    """Нулевая/отрицательная глубина — отказ с числом (в `retrieve` это `ValueError`)."""
    with pytest.raises(SystemExit):
        slice_mod._resolve_depths(raw)


def test_resolve_depths_rejects_garbage() -> None:
    """Нечисло — отказ, а не молчаливый пропуск: иначе глубина 10 подменилась бы дефолтом."""
    with pytest.raises(SystemExit):
        slice_mod._resolve_depths("10,десять")
