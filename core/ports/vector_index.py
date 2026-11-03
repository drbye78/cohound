"""Порт векторного индекса — ``VectorIndexPort`` (§3.2, I17).

Продукт-скоупед (I17): поиск не выходит за переданные ``product_ids`` (продукт + сателлиты).
``RetrievedChunk`` несёт ``content_hash`` и ``build_id`` — provenance единицы ретривала (R15).

**Батч запросов — контракт порта, а не деталь адаптера.** Косинус по партиции
memory-bound: матрица, прочитанная один раз на набор запросов, стоит в десятки раз
дешевле, чем то же чтение на каждый запрос (замер — doc 14 §3). Поэтому порт объявляет
``search_many``; ``pipeline`` зовёт :func:`batched_search` и не знает ни про файлы,
ни про формат, ни про ``numpy``. Реализация, которой батч нечего дать, получает цикл
по ``search`` — контракт один, цена разная и измеряемая.

Transitional-импорт типов — см. ``ports/types.py``/``ports/llm.py``. Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class VectorQuery:
    """Векторный запрос в пределах продукт-партиций (I17)."""

    product_ids: list[str]
    top_k: int
    text: str | None = None
    vector: list[float] | None = None


@dataclass(frozen=True)
class RetrievedChunk:
    """Чанк, возвращённый ретривалом, с полным провенансом (S4/S5).

    ``context_prefix`` — контекст продолжения: шапка таблицы с разделителем, info string
    fence, маркер цитаты. Он **не** входит в валидацию цитат (I22 сверяет ``text``), но
    без него оценщик видит строку таблицы без имён столбцов и продолжение листинга без
    языка. Поэтому поле доезжает до промпта, а не остаётся только в хранилище.
    """

    chunk_id: str
    product_id: str
    text: str
    content_hash: str
    heading_path: str
    build_id: str
    score: float
    context_prefix: str = ""


@dataclass(frozen=True)
class RetrievalResult:
    """Результат ретривала: fused top-K, профиль и сборка (I16/I19/I20)."""

    chunks: list[RetrievedChunk] = field(default_factory=list)
    profile: str = ""
    cutoff_K: int = 0
    build_id: str = ""


@runtime_checkable
class CompositionViewPort(Protocol):
    """Структурный контракт вида композиции партиций (09 §8.4).

    Конкретная реализация — :class:`core.pipeline.composition.CompositionView`.
    Порт живёт в ``ports``, чтобы ``adapters`` не импортировали ``pipeline``
    (матрица зависимостей 01 §1). Адаптеру нужны только эти три члена.
    """

    composition_id: str
    members: Mapping[str, str]

    def build_id_for(self, product_id: str) -> str: ...

    def product_ids(self) -> list[str]: ...


@runtime_checkable
class VectorIndexPort(Protocol):
    """Brute-force/ANN поиск по продукт-партиции. Дефолт — ``NpyVectorIndex`` (§3.2)."""

    build_id: str
    dim: int

    def search(self, q: VectorQuery) -> list[RetrievedChunk]: ...

    def search_many(
        self, queries: Sequence[VectorQuery]
    ) -> list[list[RetrievedChunk]]: ...

    def shard_ids(self, product_id: str) -> list[str]: ...


def batched_search(
    index: VectorIndexPort, queries: Sequence[VectorQuery]
) -> list[list[RetrievedChunk]]:
    """Батч-поиск через порт: ``search_many``, если реализовано, иначе цикл ``search``.

    Это дефолтная реализация батча **для контракта**: порт объявляет ``search_many``,
    но реализации, у которых нет своего батч-пути, не обязаны его писать — ``getattr``
    отдаёт им цикл. Поэтому расширение контракта не ломает ни одну существующую
    реализацию (структурная типизация), а ``pipeline`` получает одну точку вызова.

    Порядок результатов соответствует порядку ``queries``: ``result[i]`` — выдача
    ``queries[i]``. Смысл батча — прочитать партицию один раз на набор запросов; там,
    где реализация этого не умеет, вызовы эквивалентны поштучным.
    """
    impl = getattr(index, "search_many", None)
    if callable(impl):
        return [list(hits) for hits in impl(queries)]
    return [list(index.search(q)) for q in queries]
