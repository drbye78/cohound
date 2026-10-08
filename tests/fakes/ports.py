"""Детерминированные in-memory заглушки портов для тестов конвейера (W4/T4.3).

Каждая заглушка структурно удовлетворяет соответствующему ``Protocol`` из ``core.ports``
(``isinstance``-проверка ``runtime_checkable``). Никакой сети/I/O: результаты скриптованы
и детерминированы, что делает сквозной прогон воспроизводимым (A3).

**I18.** У ``FakeEvaluator`` физически нет ``tools``/``web_search`` — интерфейс повторяет
``EvaluatorPort.complete`` без веб-поиска (оценщик работает только по переданным чанкам).
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable

from core.domain import LLMCellOutput, RegistryFact
from core.ports.llm import LLMRequest, LLMResponse, TokenUsage
from core.ports.types import Chunk, ExtractedDoc, ProductRef, RawPage, RenderMode

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
]


def _digest(data: bytes | str) -> str:
    """SHA-256 дайджест (детерминированный content_hash)."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


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

    def fetch(self, url: str, render: RenderMode = RenderMode.HTTP) -> RawPage:
        """Возвращает заданную страницу; неизвестный URL — пустые байты."""
        page = self.pages.get(url)
        if page is not None:
            return page
        return RawPage(url=url, content=b"", fetched_at=FIXED_TS, render_mode=render)


class FakeExtractor:
    """Экстрактор: текст = декодированные байты, ``content_hash`` — от байтов (I19)."""

    version = "fake-extractor-1"

    def extract(self, page: RawPage) -> ExtractedDoc:
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
