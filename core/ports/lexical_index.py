"""Порт лексического индекса — ``LexicalIndexPort`` (§3.2, I16).

Симметрия профиля (I16): один и тот же движок/параметры для всех продуктов когорты.
Гибрид (RRF) собирается в ``pipeline/evaluate.py`` из этого порта и ``VectorIndexPort`` —
фьюжн не спрятан в адаптер, чтобы профиль R0/R1/R2 был явным и логируемым (02 §B.3).

Transitional-типы — см. ``ports/vector_index.py``. Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from core.ports.vector_index import RetrievedChunk


@runtime_checkable
class LexicalIndexPort(Protocol):
    """BM25-поиск по продукт-партициям. Дефолт — tantivy; baseline — in-process BM25.

    ``lang`` — ось **запроса**: язык запрашиваемого текста. Реализация вправе её не
    использовать (анализатор может нести оба языка сразу — так делает tantivy-адаптер
    после C8: одно поле, оба стеммера); порт не обещает языкового firewall и не
    требует разделения индекса по языкам.
    """

    engine_version: str

    def search(
        self,
        query: str,
        product_ids: list[str],
        top_k: int,
        lang: str | None = None,
    ) -> list[RetrievedChunk]: ...


@runtime_checkable
class LexicalDocLike(Protocol):
    """Минимум документа, который умеет индексировать :class:`LexicalSearcherPort`.

    Структурный, а не номинальный: ``pipeline`` не имеет права импортировать адаптер, а
    адаптеру не нужны ни адрес документа, ни его хеш — только идентичность, партиция и
    текст.
    """

    #: Свойства, а не поля: реализация вправе быть неизменяемой (frozen dataclass), и
    #: требование «переменная» отвергло бы её структурно (та же ловушка, что
    #: ``CompositionViewPort`` в ``vector_index``).
    @property
    def chunk_id(self) -> str: ...

    @property
    def product_id(self) -> str: ...

    @property
    def text(self) -> str: ...


@runtime_checkable
class LexicalSearcherPort(Protocol):
    """Индекс, который можно **собрать в процессе** и спросить по нему.

    Отличие от :class:`LexicalIndexPort` не косметическое: порт прогона описывает
    **сборку на диске** (tantivy, ``index_build_id``, партиции) и отдаёт
    :class:`RetrievedChunk` с провенансом сборки. Здесь предмет другой — чужой корпус,
    живущий вне прогона (reference-корпус одного продукта одной версии), и его индекс
    собирается на время вызова.

    Зачем отдельный протокол, а не ``Any``: карта обязана спрашивать лексику **через
    контракт**, иначе ``pipeline`` потянул бы адаптер и нарушил матрицу зависимостей
    (01 §1). Движок выбирает composition root, как и для прогона.
    """

    engine_version: str

    def index(self, docs: Sequence[LexicalDocLike]) -> None:
        """Строит индекс; порядок документов задаёт вызывающий (tie-break — ``chunk_id``)."""
        ...

    def search_scores(
        self,
        query: str,
        product_ids: Sequence[str],
        top_k: int,
    ) -> list[tuple[str, float]]:
        """Возвращает ``(chunk_id, score)`` по убыванию счёта (tie-break — ``chunk_id``)."""
        ...
