"""BM25-лексический индекс — реализация ``LexicalIndexPort`` (baseline, без зависимостей).

Роль в coHound: лексическое плечо гибридного ретривала (§3.2, §5). В проде
лексический контур — tantivy (BM25, RU/EN + raw-identifier профиль); этот
in-process адаптер служит (а) baseline-референсом профиля **R0**, (б) test double
для retrieval-CI без контейнеров (§11.6), (в) запасным путём на M0.

Происхождение: адаптировано из проекта gtm (`scripts/kb_poc.py`, собственная
разработка) — см. THIRD_PARTY_NOTICES.md. Изменения: реализация ``LexicalIndexPort``,
продукт-скоупед фильтрация (I17), делегирование токенизации ``core.util.text_utils``,
детерминированный tie-break по ``chunk_id``. Нейтрален (§1.3).
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass

from core.util.text_utils import tokenize_lexical


@dataclass(frozen=True)
class LexicalDoc:
    """Индексируемый чанк: идентичность, текст, продукт-партиция."""

    chunk_id: str
    product_id: str
    text: str
    lang: str = "unknown"


@dataclass(frozen=True)
class LexicalHit:
    chunk_id: str
    product_id: str
    score: float


class Bm25Index:
    """In-process BM25 (k1=1.5, b=0.75) с фильтром по продукт-партициям (I17)."""

    engine_version = "bm25-py-1"

    def __init__(self, k1: float = 1.5, b: float = 0.75) -> None:
        self.k1 = k1
        self.b = b
        self._docs: list[LexicalDoc] = []
        self._tokens: list[list[str]] = []
        self._df: Counter[str] = Counter()
        self._avgdl: float = 0.0
        self._built = False

    # -- build ---------------------------------------------------------- #

    def index(self, docs: list[LexicalDoc]) -> None:
        self._docs = list(docs)
        self._tokens = [tokenize_lexical(d.text) for d in self._docs]
        self._df = Counter()
        total_len = 0
        for tokens in self._tokens:
            total_len += len(tokens)
            for term in set(tokens):
                self._df[term] += 1
        n = len(self._docs)
        self._avgdl = total_len / max(1, n)
        self._built = True

    # -- search (LexicalIndexPort.shape) -------------------------------- #

    def search(
        self,
        query: str,
        product_ids: list[str] | None = None,
        top_k: int = 8,
        lang: str | None = None,
    ) -> list[LexicalHit]:
        if not self._built:
            raise RuntimeError("Bm25Index: перед поиском вызовите index()")
        allowed = set(product_ids) if product_ids else None
        q_terms = tokenize_lexical(query)
        if not q_terms:
            return []

        n = len(self._docs)
        hits: list[LexicalHit] = []
        for i, doc in enumerate(self._docs):
            if allowed is not None and doc.product_id not in allowed:
                continue
            tf = Counter(self._tokens[i])
            dl = len(self._tokens[i])
            score = 0.0
            for term in q_terms:
                f = tf.get(term, 0)
                if not f:
                    continue
                df = self._df.get(term, 0)
                idf = math.log(1 + (n - df + 0.5) / (df + 0.5))
                denom = f + self.k1 * (1 - self.b + self.b * dl / (self._avgdl or 1.0))
                score += idf * (f * (self.k1 + 1)) / denom
            if score > 0:
                hits.append(LexicalHit(doc.chunk_id, doc.product_id, score))

        # Детерминированный порядок: по убыванию score, tie-break по chunk_id (R7).
        hits.sort(key=lambda h: (-h.score, h.chunk_id))
        return hits[:top_k]
