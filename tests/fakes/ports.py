"""Детерминированные in-memory заглушки портов для тестов конвейера (W4/T4.3).

Каждая заглушка структурно удовлетворяет соответствующему ``Protocol`` из ``core.ports``
(``isinstance``-проверка ``runtime_checkable``). Никакой сети/I/O: результаты скриптованы
и детерминированы, что делает сквозной прогон воспроизводимым (A3).

**I18.** У ``FakeEvaluator`` физически нет ``tools``/``web_search`` — интерфейс повторяет
``EvaluatorPort.complete`` без веб-поиска (оценщик работает только по переданным чанкам).
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import replace
from typing import Any, Callable

from core.domain import LLMCellOutput, RegistryFact
from core.ports.index_builder import IndexMeta
from core.ports.llm import LLMRequest, LLMResponse, TokenUsage
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchPlan, ProductRef, RawPage
from core.ports.vector_index import RetrievedChunk, VectorQuery

#: Фиксированная метка времени (детерминизм фейков).
FIXED_TS = "2026-01-01T00:00:00+00:00"

__all__ = [
    "FIXED_TS",
    "FakeEvaluator",
    "FakeSearchAdapter",
    "FakeCrawler",
    "FakeExtractor",
    "FakeChunker",
    "FakeRegistryFetcher",
    "FakeEmbedder",
    "FakeIndexBuilder",
    "FakeLexicalIndex",
    "FakeVectorIndex",
]


def _digest(data: bytes | str) -> str:
    """SHA-256 дайджест (детерминированный content_hash)."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _hash_vector(text: str, dim: int) -> list[float]:
    """Детерминированный вектор ``dim`` в ``[0, 1]``: по 4 байта SHA-256 на ось.

    Одинаковый текст → одинаковый вектор (воспроизводимость A3). Размерность любая
    (байты SHA-256 нарезаются по индексу оси, а не по смещению), модель/сеть не нужны.
    """
    values: list[float] = []
    for index in range(dim):
        digest = hashlib.sha256(f"{text}\x1f{index}".encode("utf-8")).digest()
        values.append(int.from_bytes(digest[:4], "big") / 0xFFFFFFFF)
    return values


def _cosine(a: Sequence[float], b: Sequence[float]) -> float:
    """Косинусная близость; при несовпадении размеров/нулевой норме — ``0.0``."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(y * y for y in b) ** 0.5
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


class FakeEvaluator:
    """Скриптованный оценщик: ``LLMCellOutput`` по ключу запроса (обычно — по промпту).

    Промпт компилируется под (product, param) в Ш6, поэтому ключ вывода удобно строить как
    ``f"{product_id}|{param_id}"`` через ``key`` (по умолчанию ключ — сам ``req.prompt``).
    """

    def __init__(
        self,
        outputs: dict[str, LLMCellOutput] | None = None,
        *,
        key: Callable[[LLMRequest], str] | None = None,
        default: LLMCellOutput | None = None,
    ) -> None:
        self.outputs = dict(outputs or {})
        self.key = key or (lambda req: req.prompt)
        self.default = default
        self.calls: list[LLMRequest] = []

    def complete(self, req: LLMRequest) -> LLMResponse:
        """Возвращает scripted-выход; без записи и без веб-тула (I18)."""
        self.calls.append(req)
        output = self.outputs.get(self.key(req), self.default)
        if output is None:
            raise KeyError(f"FakeEvaluator: нет scripted-выхода для {self.key(req)!r}")
        return LLMResponse(
            output=output,
            raw=output.model_dump_json(),
            prompt_hash=_digest(req.prompt),
            decoder="fake",
            usage=TokenUsage(),
        )


class FakeSearchAdapter:
    """Внешний поиск: URL+байты (I19), без превращения в evidence."""

    adapter_kind = "serp"
    provider = "fake"

    def __init__(
        self,
        results: dict[str, list[Any]] | None = None,
        pages: dict[str, RawPage] | None = None,
    ) -> None:
        self.results = dict(results or {})
        self.pages = dict(pages or {})

    def search(self, q: Any) -> list[Any]:
        """Возвращает скриптованные результаты по тексту запроса (пусто — если не задано)."""
        return list(self.results.get(q.text, []))

    def fetch(self, url: str) -> RawPage:
        """Дозагрузка выбранного URL; неизвестный — пустая страница (грациозно)."""
        page = self.pages.get(url)
        if page is not None:
            return page
        return RawPage(url=url, content=b"", fetched_at=FIXED_TS)


class FakeCrawler:
    """Краулер: страницы из словаря ``url -> RawPage``."""

    version = "fake-crawler-1"

    def __init__(self, pages: dict[str, RawPage] | None = None) -> None:
        self.pages = dict(pages or {})

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        """Возвращает заданную страницу; неизвестный URL — пустые байты."""
        page = self.pages.get(url)
        if page is not None:
            return page
        return RawPage(url=url, content=b"", fetched_at=FIXED_TS, fetch_mode=plan.fetch)


class FakeExtractor:
    """Экстрактор: текст = декодированные байты, ``content_hash`` — от байтов (I19)."""

    version = "fake-extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        """Нормализует ``RawPage`` в ``ExtractedDoc`` детерминированно."""
        text = page.content.decode("utf-8", errors="replace")
        return ExtractedDoc(
            text=text,
            content_hash=_digest(page.content),
            extractor_version=self.version,
            page_count=1,
        )


class FakeChunker:
    """Чанкер: один детерминированный чанк на документ."""

    version = "fake-chunker-1"

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        """Возвращает единственный чанк с чистым ``text`` (для валидации цитат)."""
        return [
            Chunk(
                chunk_id=f"{document_id}:0",
                chunk_no=0,
                text=doc.text,
                heading_path="",
                context_prefix="",
                content_hash=doc.content_hash,
            )
        ]


class FakeRegistryFetcher:
    """Реестровый фетчер: факты из словаря ``product_id -> [RegistryFact]``.

    R1: каждый факт несёт ``source_url``.
    """

    def __init__(
        self,
        registry_id: str = "fake-registry",
        run_id: str = "run-1",
        facts: dict[str, list[RegistryFact]] | None = None,
    ) -> None:
        self.registry_id = registry_id
        self.run_id = run_id
        self.facts = dict(facts or {})

    def fetch(self, product: ProductRef, scope: dict[str, Any]) -> list[RegistryFact]:
        """Возвращает заданные факты либо один дефолтный факт с ``source_url`` (R1)."""
        if product.product_id in self.facts:
            return list(self.facts[product.product_id])
        return [
            RegistryFact(
                run_id=self.run_id,
                product_id=product.product_id,
                registry_id=self.registry_id,
                fact_json='{"status": "present"}',
                source_url=f"https://example.test/registry/{product.product_id}",
                fetched_at=FIXED_TS,
            )
        ]


class FakeEmbedder:
    """Детерминированный in-memory эмбеддер (``EmbedderPort``), без сети/GPU.

    Вектор фиксированной размерности выводится из SHA-256 текста (:func:`_hash_vector`):
    одинаковый текст → одинаковый вектор, что делает ретривал воспроизводимым (A3).
    Это заглушка порта, а не локальная модель: ``model_id``/``dim`` объявлены, чтобы
    ``build_corpus`` записал их в ``index_manifest``.
    """

    available = True

    def __init__(self, dim: int = 4, model_id: str = "fake-embedder-1") -> None:
        if dim < 1:
            raise ValueError("dim обязан быть ≥ 1")
        self.dim = dim
        self.model_id = model_id
        self.calls: list[list[str]] = []

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Возвращает ``N × dim`` детерминированных векторов в порядке входа."""
        batch = [str(text) for text in texts]
        self.calls.append(batch)
        return [_hash_vector(text, self.dim) for text in batch]


class FakeIndexBuilder:
    """In-memory сборщик индекса (``IndexBuilderPort``): ``build`` стадирует, ``commit`` фиксирует.

    Контракт ``build_corpus``: ``build(...)`` вызывается **по каждой** продукт-партиции,
    а ``commit()`` — **один раз** в конце (см. ``core/pipeline/corpus.py``). Поэтому
    ``build`` копит записи в ``staged``, а ``commit`` переводит их в ``committed``.
    ``build`` возвращает ``build_id`` из ``meta.extra["index_build_id"]`` (его задаёт
    корпус) либо детерминированно выводит из ``IndexMeta``.
    """

    vector_store = "fake"
    lexical_engine_version = "fake-bm25-1"

    def __init__(self) -> None:
        self.meta_by_product: dict[str, IndexMeta] = {}
        self.chunks_by_product: dict[str, list[Chunk]] = {}
        self.vectors_by_product: dict[str, list[list[float]]] = {}
        self.build_ids: dict[str, str] = {}
        self.staged: list[str] = []
        self.committed: list[str] = []
        self.commit_calls = 0

    def build(
        self,
        chunks: Sequence[Chunk],
        vectors: Sequence[Sequence[float]],
        meta: IndexMeta,
    ) -> str:
        """Стадирует продукт-партицию и возвращает её ``build_id``."""
        build_id = str(meta.extra.get("index_build_id") or self._derive_build_id(meta))
        self.build_ids[meta.product_id] = build_id
        self.meta_by_product[meta.product_id] = meta
        self.chunks_by_product[meta.product_id] = list(chunks)
        self.vectors_by_product[meta.product_id] = [list(v) for v in vectors]
        self.staged.append(build_id)
        return build_id

    def commit(self) -> None:
        """Фиксирует все стадированные сборки (порядок сохранён)."""
        self.commit_calls += 1
        self.committed.extend(self.staged)
        self.staged.clear()

    @staticmethod
    def _derive_build_id(meta: IndexMeta) -> str:
        """Детерминированный ``build_id`` из метаданных (fallback без ``extra``)."""
        material = "\x1f".join(
            (
                meta.package,
                meta.chunker_version,
                meta.embed_model_id,
                meta.snapshot_date,
                meta.product_id,
            )
        )
        return "ib_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


class FakeLexicalIndex:
    """In-memory лексический индекс (``LexicalIndexPort``), продукт-скоупед (I17).

    Документы задаются готовыми ``RetrievedChunk``. Скор — число токенов запроса,
    встреченных в тексте чанка; при отсутствии совпадений берётся исходный ``score``
    чанка (чтобы «немой» запрос всё же вернул детерминированный топ). Порядок — по
    убыванию скора и ``chunk_id`` (R7), срез — ``top_k``.
    """

    def __init__(
        self,
        docs: Sequence[RetrievedChunk] | None = None,
        engine_version: str = "fake-bm25-1",
        build_id: str | None = None,
    ) -> None:
        self.docs = list(docs or [])
        self.engine_version = engine_version
        #: Сборка/композиция, к которой привязан индекс (провенанс R0, §5/R15).
        self.build_id = build_id
        self.calls: list[tuple[str, list[str], int]] = []

    def add(self, *chunks: RetrievedChunk) -> None:
        """Дописывает чанки в индекс (удобно для скриптового наполнения)."""
        self.docs.extend(chunks)

    def search(
        self,
        query: str,
        product_ids: list[str],
        top_k: int,
        lang: str | None = None,
    ) -> list[RetrievedChunk]:
        """Продукт-скоупед поиск (I17): фильтр по ``product_ids`` + срез ``top_k``."""
        self.calls.append((query, list(product_ids), top_k))
        allowed = set(product_ids)
        tokens = [tok for tok in str(query).casefold().split() if tok]
        hits: list[RetrievedChunk] = []
        for doc in self.docs:
            if doc.product_id not in allowed:
                continue
            overlap = float(sum(1 for tok in tokens if tok in doc.text.casefold()))
            score = overlap if overlap > 0.0 else float(doc.score)
            hits.append(replace(doc, score=score))
        hits.sort(key=lambda c: (-c.score, c.chunk_id))
        return hits[:top_k]


class FakeVectorIndex:
    """In-memory векторный индекс (``VectorIndexPort``), продукт-скоупед (I17).

    Вектор каждого чанка детерминированно выводится из ``chunk_id`` (как у
    :class:`FakeEmbedder`), поэтому близость воспроизводима. Скор — косинус вектора
    запроса и вектора чанка; без вектора запроса — ``0.0``.
    """

    def __init__(
        self,
        docs: Sequence[RetrievedChunk] | None = None,
        build_id: str = "ib_fake",
        dim: int = 4,
    ) -> None:
        self.docs = list(docs or [])
        self.build_id = build_id
        self.dim = dim
        self._vectors: dict[str, list[float]] = {
            doc.chunk_id: _hash_vector(doc.chunk_id, dim) for doc in self.docs
        }

    def search(self, q: VectorQuery) -> list[RetrievedChunk]:
        """Продукт-скоупед поиск (I17) по косинусу; срез — ``q.top_k`` (R7 tie-break)."""
        allowed = set(q.product_ids)
        query = [float(x) for x in (q.vector or [])]
        hits: list[RetrievedChunk] = []
        for doc in self.docs:
            if doc.product_id not in allowed:
                continue
            score = _cosine(query, self._vectors[doc.chunk_id])
            hits.append(replace(doc, score=score))
        hits.sort(key=lambda c: (-c.score, c.chunk_id))
        return hits[: q.top_k]

    def shard_ids(self, product_id: str) -> list[str]:
        """Идентификаторы чанков партиции (детерминированная заглушка шардов)."""
        return sorted(
            {doc.chunk_id for doc in self.docs if doc.product_id == product_id}
        )
