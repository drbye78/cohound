"""coHound — порты (L2): Protocol-контракты между доменом и адаптерами.

Структурная типизация (``typing.Protocol``), без наследования. Каждый порт — отдельный модуль
(архитектура 01 §3). Внешние системы (LLM, SERP, краул, индексы, реестры, store, DMZ-шов) —
за портами; ``pipeline`` зависит только от них.

Нейтральность (§1.3): в этом пакете нет предметных терминов.
"""

from __future__ import annotations

from core.ports.chunker import ChunkerPort
from core.ports.crawl_executor import (
    CrawlBatch,
    CrawlExecutorPort,
    CrawlProgress,
    CrawlResultItem,
    CrawlSubmitResult,
    CrawlWorkItem,
)
from core.ports.crawler import CrawlerPort
from core.ports.discovery_llm import (
    DiscoveryLLMPort,
    DiscoveryLLMRequest,
    DiscoveryLLMResponse,
)
from core.ports.embedder import EmbedderPort
from core.ports.extractor import ExtractorPort
from core.ports.gateway import ContentGateway
from core.ports.index_builder import IndexBuilderPort, IndexMeta
from core.ports.lexical_index import LexicalIndexPort
from core.ports.llm import EvaluatorPort, LLMRequest, LLMResponse, TokenUsage
from core.ports.poller import PollPort
from core.ports.registry import RegistryFact, RegistryFetcherPort
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult
from core.ports.state import (
    ChunkRow,
    DocumentVersionRow,
    ProductStatePort,
    ProductStateRow,
    RunLineageRow,
    SeedRow,
    SnapshotRow,
    UrlStateRow,
)
from core.ports.store import GateStore, RunIndexRef, StorePort
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    ProductRef,
    RawPage,
)
from core.ports.vector_index import (
    RetrievalResult,
    RetrievedChunk,
    VectorIndexPort,
    VectorQuery,
    batched_search,
)

__all__ = [
    "Chunk",
    "ChunkRow",
    "ChunkerPort",
    "ContentGateway",
    "CrawlBatch",
    "CrawlExecutorPort",
    "CrawlProgress",
    "CrawlResultItem",
    "CrawlSubmitResult",
    "CrawlWorkItem",
    "CrawlerPort",
    "DiscoveryLLMPort",
    "DiscoveryLLMRequest",
    "DiscoveryLLMResponse",
    "DocumentVersionRow",
    "EmbedderPort",
    "EvaluatorPort",
    "ExtractedDoc",
    "ExtractionRoute",
    "ExtractorPort",
    "FetchMode",
    "FetchPlan",
    "GateStore",
    "IndexBuilderPort",
    "IndexMeta",
    "LLMRequest",
    "LLMResponse",
    "LexicalIndexPort",
    "PollPort",
    "ProductRef",
    "ProductStatePort",
    "ProductStateRow",
    "RawPage",
    "RegistryFact",
    "RegistryFetcherPort",
    "RetrievalResult",
    "RetrievedChunk",
    "RunIndexRef",
    "RunLineageRow",
    "SearchAdapterPort",
    "SearchQuery",
    "SearchResult",
    "SeedRow",
    "SnapshotRow",
    "StorePort",
    "TokenUsage",
    "UrlStateRow",
    "VectorIndexPort",
    "VectorQuery",
    "batched_search",
]
