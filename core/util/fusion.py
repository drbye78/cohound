"""Слияние результатов гибридного ретривала (RRF и др.).

Используется в Ш6 (§3.2, §5): ранжирование R0=BM25 / R1=dense / R2=RRF-гибрид.
RRF-параметры (``rrf_k``) — часть retrieval-профиля и версионируются
(§13, ADR-0013/0016, ``retrieval_params_json``).

Происхождение: адаптировано из проекта gag5 (`retrieval/fusion.py`, MIT License,
Copyright (c) 2024) — см. THIRD_PARTY_NOTICES.md. Изменения: убраны доменные веса
(graph/code/docs), ключ дедупа вынесен в параметр (coHound использует chunk_id),
добавлена типизация. Категорийно нейтрален (§1.3).
"""

from __future__ import annotations

import hashlib
from enum import StrEnum
from typing import Callable

# Результат ретривала: минимум — стабильный идентификатор и текстовое представление.
ResultDict = dict[str, object]


def _to_float(value: object) -> float:
    """Приводит значение score к float, устойчиво к None/строкам."""
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


class FusionMethod(StrEnum):
    RRF = "rrf"
    SCORE_NORMALIZED = "score_normalized"
    WEIGHTED = "weighted"
    COMBINED = "combined"


def _default_key(result: ResultDict, source: str) -> str:
    """Детерминированный ключ дедупа по идентичности чанка (без имени источника).

    Для гибрида R2 (BM25+dense по одному корпусу) один и тот же ``chunk_id``
    обязан сливаться в одну запись, поэтому источник в ключ НЕ входит.
    Не использует встроенный randomized ``hash()`` — воспроизводимость (R7).
    """
    doc_id = str(result.get("chunk_id", result.get("id", result.get("source_id", ""))))
    if doc_id:
        return doc_id
    content = str(result.get("content", ""))
    return hashlib.sha256(content[:200].encode()).hexdigest()[:24]


class ResultFusion:
    """Слияние результатов из нескольких источников ретривала."""

    def __init__(
        self,
        method: FusionMethod = FusionMethod.RRF,
        rrf_k: int = 60,
        weights: dict[str, float] | None = None,
        key_fn: Callable[[ResultDict, str], str] | None = None,
    ) -> None:
        self.method = method
        self.rrf_k = rrf_k
        self.weights = weights or {}
        self.key_fn = key_fn or _default_key

    def fuse(self, source_results: dict[str, list[ResultDict]]) -> list[ResultDict]:
        if self.method is FusionMethod.RRF:
            return self._rrf(source_results)
        if self.method is FusionMethod.SCORE_NORMALIZED:
            return self._score_normalized(source_results)
        if self.method is FusionMethod.WEIGHTED:
            return self._weighted(source_results)
        return self._combined(source_results)

    # ------------------------------------------------------------------ #

    def _fuse_template(
        self,
        source_results: dict[str, list[ResultDict]],
        score_fn: Callable[[ResultDict, str, int, int], tuple[float, float]],
    ) -> list[ResultDict]:
        fused_scores: dict[str, float] = {}
        metadata: dict[str, ResultDict] = {}

        for source, results in source_results.items():
            if not results:
                continue
            weight = self.weights.get(source, 1.0)
            for rank, result in enumerate(results, 1):
                key = self.key_fn(result, source)
                fused_score, raw_score = score_fn(result, source, rank, self.rrf_k)
                fused_scores[key] = fused_scores.get(key, 0.0) + fused_score * weight
                if key not in metadata:
                    metadata[key] = {
                        "content": result.get("content", ""),
                        "source": source,
                        "original_score": raw_score,
                        "rank": rank,
                    }

        order = sorted(fused_scores, key=lambda k: fused_scores[k], reverse=True)
        return [
            {
                "content": metadata[k]["content"],
                "source": metadata[k]["source"],
                "score": fused_scores[k],
                "original_score": metadata[k]["original_score"],
                "rank": metadata[k].get("rank", 0),
                "fused": True,
            }
            for k in order
        ]

    def _rrf(self, source_results: dict[str, list[ResultDict]]) -> list[ResultDict]:
        return self._fuse_template(
            source_results, lambda _r, _s, rank, k: (1.0 / (k + rank), 0.0)
        )

    def _score_normalized(self, source_results: dict[str, list[ResultDict]]) -> list[ResultDict]:
        ranges: dict[str, tuple[float, float]] = {}
        for source, results in source_results.items():
            if results:
                scores = [_to_float(r.get("score")) for r in results]
                lo = min(scores)
                ranges[source] = (lo, max(max(scores) - lo, 1e-9))
            else:
                ranges[source] = (0.0, 1.0)

        def score_fn(result: ResultDict, source: str, rank: int, k: int) -> tuple[float, float]:
            raw = _to_float(result.get("score"))
            lo, span = ranges.get(source, (0.0, 1.0))
            return (raw - lo) / span, raw

        return self._fuse_template(source_results, score_fn)

    def _weighted(self, source_results: dict[str, list[ResultDict]]) -> list[ResultDict]:
        def score_fn(result: ResultDict, source: str, rank: int, k: int) -> tuple[float, float]:
            raw = _to_float(result.get("score"))
            return raw, raw

        return self._fuse_template(source_results, score_fn)

    def _combined(self, source_results: dict[str, list[ResultDict]]) -> list[ResultDict]:
        rrf = self._rrf(source_results)
        norm = self._score_normalized(source_results)
        combined: dict[str, float] = {}
        metadata: dict[str, ResultDict] = {}
        for result in (*rrf, *norm):
            source = str(result.get("source", ""))
            key = self.key_fn(result, source)
            combined[key] = combined.get(key, 0.0) + _to_float(result.get("score"))
            metadata.setdefault(key, {"content": result.get("content", ""), "source": source})
        order = sorted(combined, key=lambda k: combined[k], reverse=True)
        return [
            {"content": metadata[k]["content"], "source": metadata[k]["source"],
             "score": combined[k], "fused": True}
            for k in order
        ]


def rrf_fuse(
    ranked_lists: list[list[ResultDict]],
    rrf_k: int = 60,
    source_names: list[str] | None = None,
) -> list[ResultDict]:
    """Удобная обёртка RRF для двух и более ранжированных списков.

    coHound-профиль R2: ``rrf_fuse([bm25_hits, dense_hits], rrf_k=...)``.
    """
    names = source_names or [f"s{i}" for i in range(len(ranked_lists))]
    source_results = {name: hits for name, hits in zip(names, ranked_lists, strict=False)}
    return ResultFusion(method=FusionMethod.RRF, rrf_k=rrf_k).fuse(source_results)
