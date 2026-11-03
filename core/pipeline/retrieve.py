"""coHound — ретривал Ш6: явный RRF-фьюжн и ``RetrievedChunk`` с провенансом (T8.5, §3.2/§5).

Профиль гибридного поиска — часть ``RunConfig`` и **симметричен** для всех продуктов цикла
(I16); смена профиля/параметров = новая сборка (S1/§13). Фьюжн намеренно живёт в
``pipeline`` (а не спрятан в адаптере), чтобы профиль R0/R1/R2 был явным и логируемым
(01 §3.2, 02 §B.3).

- **R0** — только лексический (BM25).
- **R1** — только dense (векторный).
- **R2** — RRF-гибрид (лексический + dense).

Продукт-скоупед (I17): оба плеча получают ``product_ids``; ретривал не выходит за
партиции продукта и его сателлитов. Каждый ``RetrievedChunk`` несёт ``content_hash`` и
``build_id`` — provenance единицы доказательства (R15/S4/S5).

Tie-break детерминирован по ``chunk_id`` (R7). Модуль категорийно нейтрален (§1.3).

``RetrievalResult.build_id`` — сборка, к которой привязаны данные: у R1/R2 —
``vector_index.build_id`` (композиция, 09 §8.4), у R0 — ``lexical_index.build_id``
(заполняется засевом). ``engine_version`` в провенанс не попадает: это версия
движка, а не сборки.
"""

from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum

from core.ports.lexical_index import LexicalIndexPort
from core.ports.vector_index import (
    RetrievalResult,
    RetrievedChunk,
    VectorIndexPort,
    VectorQuery,
)

__all__ = [
    "PROFILE_DESCRIPTIONS",
    "RetrievalProfile",
    "retrieve",
    "rrf_fuse_chunks",
]


class RetrievalProfile(StrEnum):
    """Профиль ретривала (§B.3): R0=BM25 · R1=dense · R2=RRF-гибрид."""

    R0 = "R0"
    R1 = "R1"
    R2 = "R2"


#: Человекочитаемое описание профиля (для манифеста/логов).
PROFILE_DESCRIPTIONS: dict[RetrievalProfile, str] = {
    RetrievalProfile.R0: "только лексический BM25",
    RetrievalProfile.R1: "только dense (векторный)",
    RetrievalProfile.R2: "RRF-гибрид (BM25 + dense)",
}

#: RRF-константа по умолчанию (версионируется в ``retrieval_params_json``, S1).
DEFAULT_RRF_K = 60


def _required_index[T](
    value: T | None, what: str, profile: RetrievalProfile
) -> T:
    """Зависимость профиля, уже проверенная вызывающим: ``None`` здесь — нарушение контракта.

    Развилка по профилю (ниже) выбирает плечо по тому же признаку, по которому проверка
    выше требует порт, поэтому ветка видит не-``None``. Возврат значения вместо ``assert``
    держит сужение типа и под ``python -O``: молчаливая подмена пустым плечом невозможна.
    """
    if value is None:
        raise ValueError(f"профиль {profile.value} требует {what}")
    return value


def _as_profile(profile: RetrievalProfile | str) -> RetrievalProfile:
    """Нормализует профиль к :class:`RetrievalProfile` (принимает строку ``"R0"``/``"r0"``)."""
    if isinstance(profile, RetrievalProfile):
        return profile
    try:
        return RetrievalProfile(str(profile).upper())
    except ValueError as exc:
        raise ValueError(f"неизвестный профиль ретривала: {profile!r} (ожидается R0|R1|R2)") from exc


def _index_build_id(index: object) -> str:
    """Сборка, к которой привязан порт: его ``build_id`` (пусто — «неизвестно»).

    Не подменяется ``engine_version``: версия движка — свойство реализации, а не
    данных; записывать её в ``runs.index_build_id`` ячейки нельзя (S5/R15).
    """
    value = getattr(index, "build_id", "")
    return value if isinstance(value, str) else ""


def rrf_fuse_chunks(
    lexical: Sequence[RetrievedChunk],
    dense: Sequence[RetrievedChunk],
    *,
    rrf_k: int = DEFAULT_RRF_K,
    top_k: int | None = None,
) -> list[RetrievedChunk]:
    """RRF-слияние двух плеч с сохранением провенанса (R2).

    ``score = Σ 1/(rrf_k + rank)`` по источникам; чанк, присутствующий в обоих плечах,
    суммирует вклады. Дедуп — по ``chunk_id`` (один и тот же чанк из BM25 и dense
    сливается в одну запись). Порядок при равных score — по ``chunk_id`` (R7).

    Провенанс (``content_hash``/``build_id``/``text``/``heading_path``/``product_id``)
    берётся из первого встреченного источника; при расхождении предпочитается dense
    (векторный индекс хранит точный ``build_id`` сборки).
    """
    scores: dict[str, float] = {}
    provenance: dict[str, RetrievedChunk] = {}
    # Плечи в фиксированном порядке: сначала lexical, затем dense (детерминизм).
    for rank, chunk in enumerate(lexical, 1):
        scores[chunk.chunk_id] = scores.get(chunk.chunk_id, 0.0) + 1.0 / (rrf_k + rank)
        provenance.setdefault(chunk.chunk_id, chunk)
    for rank, chunk in enumerate(dense, 1):
        scores[chunk.chunk_id] = scores.get(chunk.chunk_id, 0.0) + 1.0 / (rrf_k + rank)
        # dense переопределяет провенанс: он несёт build_id сборки из векторного индекса.
        provenance[chunk.chunk_id] = chunk

    ordered = sorted(scores, key=lambda cid: (-scores[cid], cid))
    if top_k is not None:
        ordered = ordered[:top_k]
    return [
        RetrievedChunk(
            chunk_id=cid,
            product_id=provenance[cid].product_id,
            text=provenance[cid].text,
            content_hash=provenance[cid].content_hash,
            heading_path=provenance[cid].heading_path,
            build_id=provenance[cid].build_id,
            score=scores[cid],
            context_prefix=provenance[cid].context_prefix,
        )
        for cid in ordered
    ]


def retrieve(
    *,
    query_text: str,
    query_vector: Sequence[float] | None,
    product_ids: Sequence[str],
    profile: RetrievalProfile | str,
    cutoff_k: int,
    lexical_index: LexicalIndexPort | None = None,
    vector_index: VectorIndexPort | None = None,
    dense_hits: Sequence[RetrievedChunk] | None = None,
    rrf_k: int = DEFAULT_RRF_K,
    lang: str | None = None,
) -> RetrievalResult:
    """Единая точка гибридного ретривала по зафиксированному профилю (I16/I17/I20).

    :param query_text: текст запроса (для BM25).
    :param query_vector: вектор запроса (для dense); ``None`` недопустим для R1/R2,
        если dense-плечо не передано готовым (см. ``dense_hits``).
    :param product_ids: продукт-партиции запроса (I17) — ретривал не выходит за них.
    :param profile: ``R0`` | ``R1`` | ``R2``.
    :param cutoff_k: ``retrieval_cutoff_K`` — сколько чанков вернуть (I20).
    :param lexical_index / vector_index: инъецированные порты (ручной DI, A1).
    :param dense_hits: готовые результаты dense-плеча для **этого** запроса —
        продукт-скоупед top-K, посчитанные батчем (:func:`core.ports.vector_index.batched_search`).
        ``None`` — прежний путь: :func:`retrieve` сам зовёт ``vector_index.search``.
        Батч-путь не меняет семантику: те же ``cutoff_k`` и те же партиции ``product_ids``,
        просто чтение партиции и BLAS делятся между запросами одного продукта.
    :param rrf_k: RRF-константа (часть ``retrieval_params_json``, S1).
    :param lang: язык запроса (плечо BM25).

    :raises ValueError: неизвестный профиль; отсутствует нужный порт; для R1/R2 нет ни
        вектора, ни готового dense-плеча.
    """
    resolved = _as_profile(profile)
    products = list(product_ids)

    if resolved in (RetrievalProfile.R0, RetrievalProfile.R2) and lexical_index is None:
        raise ValueError(f"профиль {resolved.value} требует lexical_index")
    if resolved in (RetrievalProfile.R1, RetrievalProfile.R2):
        if vector_index is None:
            raise ValueError(f"профиль {resolved.value} требует vector_index")
        if query_vector is None and dense_hits is None:
            raise ValueError(f"профиль {resolved.value} требует query_vector (dense-плечо)")

    if resolved is RetrievalProfile.R0:
        lexical = _required_index(lexical_index, "lexical_index", resolved)
        chunks = list(lexical.search(query_text, products, top_k=cutoff_k, lang=lang))
        # Провенанс R0 — сборка (composition_id), а не ``engine_version``: движок
        # описывает реализацию BM25, а не данные. Источник тот же, что у dense-плеча:
        # атрибут ``build_id`` порта (``TantivyLexicalIndex`` заполняет его при засеве).
        build_id = _index_build_id(lexical)
    elif resolved is RetrievalProfile.R1:
        vectors = _required_index(vector_index, "vector_index", resolved)
        chunks = list(
            dense_hits
            if dense_hits is not None
            else vectors.search(
                VectorQuery(product_ids=products, top_k=cutoff_k, vector=list(query_vector or []))
            )
        )
        build_id = vectors.build_id
    else:  # R2 — RRF-гибрид обеих плеч.
        lexical = _required_index(lexical_index, "lexical_index", resolved)
        vectors = _required_index(vector_index, "vector_index", resolved)
        lexical_hits = lexical.search(query_text, products, top_k=cutoff_k, lang=lang)
        dense_leg = (
            list(dense_hits)
            if dense_hits is not None
            else vectors.search(
                VectorQuery(product_ids=products, top_k=cutoff_k, vector=list(query_vector or []))
            )
        )
        chunks = rrf_fuse_chunks(lexical_hits, dense_leg, rrf_k=rrf_k, top_k=cutoff_k)
        build_id = vectors.build_id

    # Детерминированный порядок + обрезка до K (I20). tie-break по chunk_id (R7).
    chunks.sort(key=lambda c: (-c.score, c.chunk_id))
    chunks = chunks[:cutoff_k]

    return RetrievalResult(
        chunks=chunks,
        profile=resolved.value,
        cutoff_K=cutoff_k,
        build_id=build_id or "",
    )
