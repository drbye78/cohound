"""Retrieval-golden: метрики качества ретривала (hit@K, MRR, recall@K).

Соответствует §11.2/§11.3 регламента (R9/R10, retrieval-golden) и I20
(cut-off метрики = pipeline ``retrieval_cutoff_K``). Используется для выбора
профиля R0/R1/R2 и как регрессионная сеть при смене эмбеддинга/чанкера.

Происхождение: метрики (hit@1/3/5, MRR) и eval-паттерн адаптированы из проекта gtm
(`scripts/kb_poc.py`, собственная разработка) — см. THIRD_PARTY_NOTICES.md.
Изменения: обобщено до K из конфигурации (I20), добавлен recall@K, типизация,
нейтральность (§1.3).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Sequence


@dataclass(frozen=True)
class GoldenQuery:
    """Один пример retrieval-golden: запрос → ожидаемые релевантные идентификаторы."""

    query: str
    expected_ids: frozenset[str]
    lang: str = "unknown"


@dataclass
class RetrievalReport:
    k: int
    queries: int
    hit_at_k: float
    recall_at_k: float
    mrr: float
    per_query_rank: list[int | None] = field(default_factory=list)

    def passes(self, min_recall: float) -> bool:
        return self.recall_at_k >= min_recall


# ``ranker(query, k)`` возвращает ранжированный список chunk_id (лучший первым).
Ranker = Callable[[str, int], Sequence[str]]


def evaluate_retrieval(
    queries: Sequence[GoldenQuery],
    ranker: Ranker,
    k: int,
) -> RetrievalReport:
    """Считает hit@K, recall@K и MRR по retrieval-golden при заданном cut-off K (I20)."""
    if k <= 0:
        raise ValueError("k должен быть > 0")
    if not queries:
        return RetrievalReport(k=k, queries=0, hit_at_k=0.0, recall_at_k=0.0, mrr=0.0)

    hit = 0
    recall_sum = 0.0
    rr_sum = 0.0
    ranks: list[int | None] = []

    for item in queries:
        ranked = list(ranker(item.query, k))[:k]
        rank = next((i for i, cid in enumerate(ranked, 1) if cid in item.expected_ids), None)
        ranks.append(rank)
        if rank is not None:
            hit += 1
            rr_sum += 1.0 / rank
        if item.expected_ids:
            found = sum(1 for cid in ranked if cid in item.expected_ids)
            recall_sum += found / len(item.expected_ids)

    n = len(queries)
    return RetrievalReport(
        k=k,
        queries=n,
        hit_at_k=hit / n,
        recall_at_k=recall_sum / n,
        mrr=rr_sum / n,
        per_query_rank=ranks,
    )
