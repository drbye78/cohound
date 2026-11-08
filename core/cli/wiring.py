"""coHound — сборка зависимостей CLI (composition root Z0, ручной DI, A1; ``03`` §E.3).

Единственное место, где ядро соединяет ``pipeline`` с адаптерами: ``pipeline`` только
принимает порты как аргументы и **никогда** не инстанцирует адаптеры сам (``01`` §1, A2).
Никаких фреймворков — ручной DI (A1).

**Профили развёртывания (``07``).** ``local`` (дефолт) собирает реальные адаптеры
in-process. ``split`` (T13.4) собирает remote-прокси (``RemoteCrawler``/
``RemoteSearch``/``RemoteRegistry``/``RemoteExtractor``) поверх ``HttpContentGateway``
по секции ``edge.*`` deploy-конфига. ``edge/`` и любой DMZ-код **не импортируются**
(матрица слоёв ``01`` §1: ``cli`` не зависит от ``edge``). Certificate pinning
отсутствует намеренно (``07`` §8.1): доверие — через корпоративный CA.

**Состояние W5/W13.** Для ``local`` собираются in-process адаптеры, безопасные без
сети/тяжёлых зависимостей: чанкер и экстрактор (HTML + документы при наличии docling)
доступны всегда, тяжёлые опциональные (эмбеддер/индекс/лексический индекс) — только
при выполненной availability-предикате, иначе ``None`` (без исключений). Сетевые
адаптеры (краул/SERP) для ``local`` не инстанцируются: нет URL/учётных данных/запросов
в конфиге CLI — включаются явной инъекцией портов. Оценщик/реестр остаются ``None``,
пока вызывающий не передаст их (генератор/конфиг). Для ``split`` заполняются
remote-прокси веб-контура (``07`` §4); не-веб порты — по мере появления.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import importlib.util
import json
import os
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, Literal, cast

from core.adapters import index_format
from core.adapters.chunker_family import FamilyChunker
from core.adapters.container_any import container_for
from core.adapters.crawl_browser import BrowserCrawler
from core.adapters.crawl_http import HttpCrawler
from core.adapters.crawl_scrapy import ScrapyCrawler
from core.adapters.embed_local import LocalEmbedder
from core.adapters.embed_model2vec import LiteEmbedder, model2vec_available
from core.adapters.extract_document import DoclingDocumentExtractor
from core.adapters.extract_epub import EpubExtractor
from core.adapters.extract_html import HtmlExtractor
from core.adapters.extract_pdf_text import PdfiumDocumentExtractor
from core.adapters.extract_text import TextExtractor
from core.adapters.feed_poller import LocalFeedPoller, RemoteFeedPoller
from core.adapters.index_npy import NpyIndexBuilder, numpy_available
from core.adapters.lexical_tantivy import (
    LEXICAL_SCHEMA_VERSION,
    LexicalChunk,
    TantivyLexicalIndex,
    tantivy_available,
    tantivy_engine_version,
)
from core.adapters.llm_discovery import (
    DEFAULT_DECODER as DISCOVERY_DEFAULT_DECODER,
)
from core.adapters.llm_discovery import (
    DEFAULT_MAX_TOKENS as DISCOVERY_DEFAULT_MAX_TOKENS,
)
from core.adapters.llm_discovery import (
    DEFAULT_TIMEOUT as DISCOVERY_DEFAULT_TIMEOUT,
)
from core.adapters.llm_discovery import (
    DiscoveryLLM,
)
from core.adapters.llm_discovery import (
    GenerateFn as DiscoveryGenerateFn,
)
from core.adapters.llm_local import (
    DEFAULT_DECODER as EVALUATOR_DEFAULT_DECODER,
)
from core.adapters.llm_local import (
    DEFAULT_TIMEOUT as EVALUATOR_DEFAULT_TIMEOUT,
)
from core.adapters.llm_local import (
    EVALUATOR_DEFAULT_SEED,
    EVALUATOR_DEFAULT_TEMPERATURE,
    LocalEvaluator,
    ReasoningWireUnsupported,
    normalize_api_base,
    reasoning_wire,
)
from core.adapters.llm_local import (
    GenerateFn as EvaluatorGenerateFn,
)
from core.adapters.registry_generic import (
    GenericRegistryFetcher,
    RegistryConfig,
    RegistryFetchError,
)
from core.adapters.remote_crawl_executor import EdgeCrawlExecutor
from core.adapters.remote_crawler import RemoteCrawler
from core.adapters.remote_extractor import RemoteExtractor
from core.adapters.remote_gateway import (
    DEFAULT_SCHEMA_VERSION,
    HttpContentGateway,
    TransportFn,
)
from core.adapters.remote_registry import RemoteRegistry
from core.adapters.remote_search import RemoteSearch
from core.adapters.search_glm import GlmSearchAdapter
from core.adapters.search_yandex import YandexSearchAdapter
from core.adapters.vector_npy import ChunkLookup, NpyVectorIndex
from core.cli.errors import EXIT_USAGE, CliError
from core.config.crawl_sources import CrawlSourcesConfig
from core.config.deploy import (
    ENV_SERP,
    SPLIT_PROFILES,
    CrawlConfig,
    DeployConfig,
    EmbedConfig,
    LLMDeployConfig,
    LLMSlotConfig,
    is_split,
    load_deploy_config,
    parse_serp_switch,
    resolve_llm_slot,
    resolve_search_provider,
    resolve_serp_rounds,
)
from core.config.domain_profiles import DomainProfile
from core.domain.enums import (
    DocumentLevel,
    EmbedDevice,
    EmbedPrecision,
    PdfDevice,
    PdfPipeline,
    ReasoningEffort,
)
from core.domain.models import CrawlRunConfig, EmbedRunConfig
from core.domain.scoring import endpoint_host_notices
from core.pipeline import Ports, Runner
from core.pipeline.composition import (
    CompositionView,
    legacy_view,
    read_composition,
)
from core.pipeline.crawl_frontier import (
    resolve_crawl_limits,
    resolve_embed_device,
    resolve_embed_precision,
    resolve_pdf_device,
)
from core.pipeline.extraction_runtime import ExtractionPool
from core.pipeline.resources import (
    MEM_BUDGET_ENV,
    PARENT_FLOOR_MB,
    MemoryBudgetError,
    cuda_available,
    cuda_total_vram_mb,
    pdf_peak_mb,
    resolve_mem_budget_mb,
    vram_peak_mb,
    workers_from_budget,
)
from core.pipeline.runner import PHASES, Clock, PhaseFn
from core.ports.crawl_errors import CrawlError, CrawlNotFound
from core.ports.crawler import CrawlerPort
from core.ports.discovery_llm import DiscoveryLLMPort
from core.ports.embedder import EmbedderPort
from core.ports.extractor import DocumentExtractorPort, ExtractorPort
from core.ports.gateway import ContentGateway
from core.ports.lexical_index import LexicalIndexPort
from core.ports.llm import EvaluatorPort
from core.ports.poller import PollPort
from core.ports.registry import RegistryFetcherPort
from core.ports.search import SearchAdapterPort
from core.ports.store import StorePort
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    RawPage,
)
from core.ports.vector_index import VectorIndexPort
from core.store.sqlite_store import SqliteStore
from core.util.cache import ContentAddressedCache
from core.util.document_formats import (
    PDF_SUFFIX,
    document_content_type,
    document_suffix,
    is_epub,
    textual_family,
)
from core.util.hashing import short_hash
from core.util.logging import PhaseLogger
from core.util.redirects import follow_validated_redirects
from core.util.robots import RobotsGate
from core.util.sitemap import MAX_SITEMAP_ENTRIES

__all__ = [
    "DEFAULT_SCOUT_CRAWLER",
    "ENV_EMBED_LITE_MODEL",
    "ENV_EMBED_LITE_PATH",
    "ENV_EMBED_LITE_REVISION",
    "ENV_LLM_SLOT",
    "ENV_SCOUT_CRAWLER",
    "EVALUATOR_ENV_IGNORED",
    "LOCAL_PROFILE",
    "PHASES",
    "SPLIT_PROFILE",
    "SUPPORTED_PROFILES",
    "SUPPORTED_SCOUT_CRAWLERS",
    "SUPPORTED_SEARCH_ADAPTERS",
    "EmbedderBuildMismatch",
    "ExperimentTarget",
    "SearchResolutionError",
    "WiringError",
    "assert_embedder_matches_build",
    "build_adapters",
    "build_discovery_llm",
    "build_evaluator_llm",
    "build_extraction_pool",
    "build_gateway",
    "build_lite_embedder",
    "build_runner",
    "build_scout_crawler",
    "build_scout_crawlers",
    "build_search_adapter",
    "build_store",
    "build_vector_index",
    "build_worker_extractor",
    "ensure_reasoning_wire",
    "llm_env_notices",
    "llm_host_notices",
    "manifest_deploy_fields",
    "preflight_extraction_budget",
    "preflight_pdf_device",
    "require_pdf_device",
    "resolve_and_build_search_adapter",
    "resolve_evaluator_model_identity",
    "resolve_evaluator_reasoning_effort",
    "resolve_experiment_target",
    "resolve_extraction_budget",
    "run_db_path",
    "seed_lexical_index",
]

#: Профиль ``local`` — один процесс, реальные адаптеры in-process (дефолт, ``07``).
LOCAL_PROFILE = "local"
#: Профиль ``split`` — веб-адаптеры в DMZ, ядро ходит через ``ContentGateway`` (``07``).
SPLIT_PROFILE = "split"
#: Известные профили развёртывания, выбираемые CLI (``03`` §D.5/D.6).
SUPPORTED_PROFILES: tuple[str, ...] = (LOCAL_PROFILE, SPLIT_PROFILE)

#: Известные внешние поисковые адаптеры для ``hound discover`` (``03`` §D.6).
#: Провайдер — данные-конфигурация, а не предметная категория (§1.3).
#: Yandex — дефолт (async), GLM — альтернатива (§3.2).
SUPPORTED_SEARCH_ADAPTERS: tuple[str, ...] = (
    "yandex",
    "glm",
)

#: Префикс env-переменных LLM-разведки (``passport-scout``). Провайдер/модель — данные.
ENV_LLM_BASE_URL = "COHOUND_LLM_BASE_URL"
ENV_LLM_API_KEY = "COHOUND_LLM_API_KEY"
ENV_LLM_MODEL = "COHOUND_LLM_MODEL"
ENV_LLM_DECODER = "COHOUND_LLM_DECODER"
ENV_LLM_TIMEOUT = "COHOUND_LLM_TIMEOUT"
ENV_LLM_MAX_TOKENS = "COHOUND_LLM_MAX_TOKENS"
#: Env-переменная активного LLM-слота (``cheap``/``flagship``; ``--llm-slot`` переопределяет).
ENV_LLM_SLOT = "COHOUND_LLM_SLOT"

#: Дефолтный base_url OI-совместимого сервера LLM. Пусто → endpoint не сконфигурирован:
#: оператор обязан явно задать внутренний [OI]-совместимый API корпоративного периметра
#: (внешние хосты не используются).
DEFAULT_LLM_BASE_URL = ""
#: Дефолтная модель LLM-разведки — **последний** фолбэк (env ``COHOUND_LLM_MODEL`` >
#: поле слота > это значение). Модель разведки операционна (её выход — кандидаты/гипотезы
#: G2, не evidence), поэтому здесь допустим и env-фолбэк. Значение держится равным
#: ``config/deploy.yaml:llm.<slot>.model_id`` (пин в ``tests/config/test_deploy.py``).
DEFAULT_LLM_MODEL = "deepseek-v4.1-flash-1017:yellow"

#: Env-переопределения лёгкого оффлайн-эмбеддера (P4a; данные, не предметная категория):
#: HF-идентификатор модели, пин ревизии (sha/тег → входит в ``embed_model_id`` как ``repo@sha``,
#: граница ``execution_versions`` §13) и явный локальный каталог снапшота (оффлайн-инференс).
ENV_EMBED_LITE_MODEL = "COHOUND_EMBED_LITE_MODEL"
ENV_EMBED_LITE_REVISION = "COHOUND_EMBED_LITE_REVISION"
ENV_EMBED_LITE_PATH = "COHOUND_EMBED_LITE_PATH"


class WiringError(ValueError):
    """Ошибка сборки зависимостей: неизвестный или ещё не реализованный профиль."""


def build_lite_embedder() -> LiteEmbedder:
    """Собирает оффлайн-эмбеддер ``model2vec`` из env-данных (P4a, A3).

    Модель/ревизия/локальный путь — **данные** оператора (env, ``05`` §2.3), не предметная
    категория (§1.3): дефолты берёт сам адаптер. Ревизия попадает в ``model_id``
    (``repo@sha``) и, как следствие, в ``index_build_id``/манифест (§13, ``execution``-граница).
    Вызывающий обязан проверить ``available`` прежде чем использовать порт: отсутствие
    снапшота/зависимости не подменяется выдуманными векторами.
    """
    model_id = os.environ.get(ENV_EMBED_LITE_MODEL) or None
    revision = os.environ.get(ENV_EMBED_LITE_REVISION) or None
    model_path = os.environ.get(ENV_EMBED_LITE_PATH) or None
    kwargs: dict[str, Any] = {"revision": revision, "model_path": model_path}
    if model_id:
        kwargs["model_id"] = model_id
    return LiteEmbedder(**kwargs)


def _single_persisted_build(
    index_root: str | Path,
    package: str | None,
    build_id: str | None = None,
) -> str | None:
    """Разрешает сборку пакета детерминированно; ``None`` — выбор неоднозначен.

    Правила (09 §8.1–§8.2, после перехода на per-product партиции):

    1. явный ``build_id`` — приоритет (провенанс из манифеста прогона);
    2. ровно один ``_compositions/*.json`` — его ``composition_id`` (это
       ``runs.index_build_id``);
    3. композиций нет, но ровно один legacy-каталог сборки — он (раскладка до
       композиций: ``index_store/{package}/{build}/{product_id}/``);
    4. иначе — ``None``.

    «Несколько композиций и нет явного id» — **настоящая** неоднозначность: выбор
    произвольной сломал бы воспроизводимость (A3/§13), поэтому ``None``, а не догадка.
    Прежнее правило «>1 верхнеуровневых каталогов → ``None``» больше не годится: при
    per-product партициях несколько каталогов — нормальный случай, и dense-плечо
    молча исчезло бы.
    """
    if not package:
        return None
    if build_id:
        return build_id
    compositions = index_format.list_composition_ids(index_root, package)
    if len(compositions) == 1:
        return compositions[0]
    if compositions:
        return None
    builds = index_format.list_build_ids(index_root, package)
    return builds[0] if len(builds) == 1 else None


def _composition_view(
    index_root: str | Path, package: str, build_id: str
) -> CompositionView:
    """Вид композиции: манифест ``_compositions/{composition_id}.json`` или legacy-view.

    ``build_id`` здесь — уже разрешённый id (явный ``runs.index_build_id`` либо
    единственная композиция/legacy-сборка, см. :func:`_single_persisted_build`). Есть
    манифест → читаем его; нет (сборка до появления композиций) → :func:`legacy_view`
    (``pb := index_build_id``).
    """
    stored = read_composition(index_root, package, build_id)
    if stored is not None:
        return stored
    return legacy_view(
        build_id, index_format.list_partitions(index_root, package, build_id)
    )


def _build_dim(
    index_root: str | Path, package: str, view: CompositionView
) -> int | None:
    """Размерность вектора из манифеста первой продукт-партиции композиции (или ``None``)."""
    for product_id in view.product_ids():
        dim = index_format.read_manifest(
            index_root, package, view.build_id_for(product_id), product_id
        ).get("dim")
        if isinstance(dim, int) and dim > 0:
            return dim
    return None


def build_vector_index(
    *,
    index_root: str | Path,
    package: str | None,
    chunk_lookup: ChunkLookup | None,
    build_id: str | None = None,
) -> VectorIndexPort | None:
    """Собирает ``NpyVectorIndex`` (dense-плечо R1/R2), либо ``None`` — явное отсутствие.

    Порт строится только когда **все** предпосылки налицо: установлен ``numpy``, известен
    пакет, есть резолвер метаданных чанков (``chunk_lookup``) и разрешена зафиксированная
    сборка (явный ``build_id`` либо единственная сборка пакета). Иначе возвращается ``None``:
    отсутствие предпосылки — легитимный no-op (N3), который эксперимент фиксирует как
    ``skipped``/``no_embedder``, а не молчаливая дыра и не выдуманный индекс.

    ``dim`` берётся из манифеста сборки (авторитетный on-disk источник), поэтому индекс
    согласован с физическими векторами независимо от текущего эмбеддера. Продукт-скоуп
    (I17) обеспечивает сам ``NpyVectorIndex``.
    """
    if not numpy_available() or not package or chunk_lookup is None:
        return None
    resolved = _single_persisted_build(index_root, package, build_id)
    if resolved is None:
        return None
    view = _composition_view(index_root, package, resolved)
    dim = _build_dim(index_root, package, view)
    if dim is None:
        return None
    return NpyVectorIndex(index_root, package, resolved, dim, chunk_lookup, view=view)


def _lexical_engine_version() -> str:
    """Фактический лексический движок **и схема** прогона — для ``pb``/манифеста (§13:676).

    Лексическое плечо R0/R2 в прогоне обслуживает только ``TantivyLexicalIndex``
    (:func:`seed_lexical_index`); in-pipeline BM25 (``adapters.lexical_bm25``) —
    baseline/test double и в ретривал прогона не попадает. Без ``tantivy``
    :func:`seed_lexical_index` возвращает ``None``, и ``experiments`` помечает R0/R2
    как ``skipped`` («нет lexical_index»), поэтому честное значение — **пустая
    строка**: «движка нет», а не «версия неизвестна». Заполненное значение — версия
    движка, которая реально засеет лексический индекс этой сборки.
    """
    if not tantivy_available():
        return ""
    # Схема входит в строку **версии** намеренно: ``pb_``/``cb_`` — адрес сборки на
    # диске, и два разных лексических индекса не могут жить под одним адресом. Версия
    # движка одна и та же при смене схемы (движок тот же), поэтому без схемы ``pb``
    # различал бы движки, но не различал бы выдачу — тот же класс, что C9 про
    # разделители формулы. Форма — ``tantivy-<ver>+schema-<v>``: одна строка,
    # человекочитаемая в манифесте.
    return f"{tantivy_engine_version()}+schema-{LEXICAL_SCHEMA_VERSION}"


def seed_lexical_index(
    *,
    index_root: str | Path,
    package: str | None,
    store: StorePort | None,
    build_id: str | None = None,
) -> LexicalIndexPort | None:
    """Собирает лексический индекс (BM25, R0/R2) из персистентных чанков сборки.

    Лексическое плечо симметрично dense: ``TantivyLexicalIndex`` сам по себе непригоден
    (пустой индекс отвергает поиск), поэтому порт строится/засевается только при наличии
    ``tantivy``, ``store`` и разрешённой сборки. Чанки берутся продукт-партициями из
    ``store.iter_chunks(partition_build_id, product_id=…)`` (``partition_build_id`` —
    из вида композиции, 09 §8.4), поэтому поиск остаётся продукт-скоупед (I17).

    **Потоковый засев (A1/Д-3).** Записи не собираются в список: они идут генератором
    из ``iter_chunks`` прямо в ``index()``, а индекс не держит ни записей, ни их текста.
    Текст при поиске догружается из store пачкой — ``store.get_chunks`` передаётся как
    провайдер гидртации, поэтому адаптер не знает ни SQL, ни раскладки БД.

    **Два идентификатора (09 §8.1/§8.4).** ``build_id`` записи (``LexicalChunk``) —
    **партиция** продукта (``view.build_id_for(product_id)``), поэтому
    ``RetrievedChunk.build_id`` у R0-хита — ``pb_`` (физическая партиция, как и у dense).
    ``index.build_id`` — **композиция** (``composition_id``), провенанс R0/R2 и
    ``RetrievalResult.build_id``. Иначе — ``None`` (явное отсутствие; эксперимент
    пропускает R0/R2, N3). Карта ``продукт → партиция`` передаётся адаптеру как ``view``:
    после удаления зеркала у него нет записи, из которой можно было бы взять ``pb_``, а
    выводить его из ``chunk_id`` значило бы зависеть от формата идентификатора.

    **Время жизни индекса — прогон (C8, объявлено).** Здесь строится
    ``TantivyLexicalIndex()`` без ``index_path``, то есть индекс живёт во временном
    каталоге (``cohound-tantivy-*``) до ``close()``/GC и **не персистится** в
    ``index_store/``: каждый прогон пересобирает его из персистентных чанков сборки.
    Это решение, а не недосмотр — источник чанков уже персистентен, персист индекса
    потребовал бы версии схемы и миграции при смене профиля/движка, а замер стоимости
    сборки — 0.25 с на 9398 чанков (``adapters/lexical_tantivy.py``, докстринг модуля).
    """
    if not tantivy_available() or not package or store is None:
        return None
    resolved = _single_persisted_build(index_root, package, build_id)
    if resolved is None:
        return None
    view = _composition_view(index_root, package, resolved)
    # Adapter/port mismatch (``CompositionViewPort`` объявляет ``composition_id``/``members``
    # сеттеблами, а вид композиции — frozen-снимок с read-only атрибутами): статическая
    # несовместимость, поведение корректно — тот же класс, что ``lexical_index`` в
    # ``build_adapters``. Чинится изменением протокола на read-only свойства, а это
    # отдельная правка порта, не эта полоса.
    index = TantivyLexicalIndex(
        hydrate=store.get_chunks,
        view=view,
    )

    def _records() -> Iterable[LexicalChunk]:
        """Записи по одной: ни список чанков, ни их текст не живут вне записи в индекс."""
        for product_id in view.product_ids():
            # Чанки читаются из партиции продукта; id записи — партиция (``pb_``), id
            # индекса — композиция (``cb_``). Иначе R0-хит нёс бы ``cb_`` в
            # ``RetrievedChunk.build_id`` и evidence назвал бы композицию партицией.
            partition_build_id = view.build_id_for(product_id)
            for chunk in store.iter_chunks(partition_build_id, product_id=product_id):
                yield _lexical_record(chunk, product_id, partition_build_id)

    if not index.index(_records(), build_id=view.composition_id):
        # Пустая композиция — явное отсутствие лексического плеча (N3), а не пустой
        # индекс, на котором поиск падал бы с «index() не вызван».
        index.close()
        return None
    # ``LexicalIndexPort`` объявляет ``engine_version`` **записываемым** атрибутом, а
    # адаптер отдаёт его read-only свойством (``tantivy_engine_version()``): писать в
    # штамп движка потребителю нечего, поэтому сужение верно по смыслу, а не глушит
    # дефект. Настоящая правка — объявить свойство в порте (``core/ports``), отдельная
    # полоса; до неё приведение типа с инвариантом «read-only реализация writable порта».
    return cast(LexicalIndexPort, index)


def _lexical_record(chunk: Chunk, product_id: str, build_id: str) -> LexicalChunk:
    """``ports.types.Chunk`` + ``product_id`` (из партиции) → запись лексического индекса.

    ``build_id`` записи — **партиция** продукта (``pb_``), а не композиция. В индексе он
    **не хранится** (тело и провенанс приходят из store, A1/Д-3): записи живут только на
    время засева. Основной источник ``pb_`` у R0-хита — ``view``, переданный адаптеру;
    это значение работает как фолбэк для сборок без вида композиции (09 §8.4).
    """
    return LexicalChunk(
        chunk_id=chunk.chunk_id,
        product_id=product_id,
        text=chunk.text,
        heading_path=chunk.heading_path,
        content_hash=chunk.content_hash,
        build_id=build_id,
    )


@dataclass(frozen=True)
class ExperimentTarget:
    """Зафиксированный объект эксперимента ретривала (P4b): сборка, пакет, продукты, модель.

    Разрешается **из БД прогона** (:meth:`StorePort.read_run_index_ref` +
    :meth:`StorePort.iter_index_manifest`), а не сканированием ``index_store/``: сборка
    берётся из ``runs.index_build_id``, пакет — из записи ``index_manifest`` (авторитетно
    для раскладки сборки) с фолбэком на ``runs.package_id``. ``embed_model_id`` —
    согласованность «запросный эмбеддер ↔ физическая сборка» (NB1): два *одинаково
    размерных* разных эмбеддера дают бессмысленный скор, поэтому сверяется именно id.
    """

    package: str
    build_id: str
    product_ids: tuple[str, ...] = ()
    embed_model_id: str | None = None
    #: ``product_id → partition_build_id`` (09 §8.1). Пусто — раскладка legacy:
    #: все продукты читаются из ``build_id``.
    partition_by_product: Mapping[str, str] = field(default_factory=dict)


class EmbedderBuildMismatch(WiringError):
    """§13/I17 (NB1): запросный эмбеддер не совпадает со сборкой — эксперимент запрещён.

    Несёт оба идентификатора (запроса и сборки), чтобы ошибка называла расхождение явно.
    """

    def __init__(self, query_model_id: str, build_model_id: str) -> None:
        super().__init__(
            f"эмбеддер запроса ({query_model_id!r}) не совпадает с эмбеддером сборки "
            f"({build_model_id!r}): скор был бы бессмысленным (NB1/§13). "
            "Пересоберите индекс тем же эмбеддером либо смените эмбеддер запроса."
        )
        self.query_model_id = query_model_id
        self.build_model_id = build_model_id


def assert_embedder_matches_build(query_model_id: str, build_model_id: str) -> None:
    """Отказ при расхождении эмбеддера запроса и сборки (NB1; сравнение по id, не по dim).

    :raises EmbedderBuildMismatch: ``query_model_id != build_model_id`` — называет оба id.
    """
    if query_model_id != build_model_id:
        raise EmbedderBuildMismatch(query_model_id, build_model_id)


def _build_embed_model_id(
    index_root: str | Path,
    package: str,
    view: CompositionView,
    product_ids: tuple[str, ...],
) -> str | None:
    """``embed_model_id`` из ``manifest.json`` первой продукт-партиции композиции (или ``None``).

    On-disk манифест — авторитетный источник того, чем **физически** собраны векторы;
    партиция продукта читается по его ``partition_build_id`` (09 §8.4).
    """
    for product_id in product_ids:
        manifest = index_format.read_manifest(
            index_root, package, view.build_id_for(product_id), product_id
        )
        model_id = manifest.get("embed_model_id")
        if isinstance(model_id, str) and model_id:
            return model_id
    return None


def resolve_experiment_target(
    store: StorePort, run_id: str, *, index_root: str | Path = "index_store"
) -> ExperimentTarget | None:
    """Разрешает зафиксированную сборку прогона из БД (P4b, нейтрально).

    Источники (без сканирования каталогов, A1): ``runs.index_build_id`` даёт сборку;
    ``index_manifest`` этой сборки — пакет раскладки, продукт-партиции,
    ``partition_build_id`` каждой партиции (09 §8.1; legacy-NULL → ``index_build_id``)
    и (фолбэк) ``embed_model_id``; ``runs.package_id`` — фолбэк пакета, если запись
    ``index_manifest`` пакета не несёт. ``embed_model_id`` предпочитается из on-disk
    ``manifest.json`` партиции (авторитетный on-disk источник, §13).

    :returns: :class:`ExperimentTarget` или ``None`` — предпосылка отсутствует: нет строки
        прогона, не записан ``index_build_id`` или не разрешён пакет. Отсутствие явное
        (N3), значения не выдумываются.
    """
    ref = store.read_run_index_ref(run_id)
    if ref is None or not ref.index_build_id:
        return None
    build_id = ref.index_build_id
    entries = list(store.iter_index_manifest(build_id))
    package = next((entry.package_id for entry in entries if entry.package_id), None) or (
        ref.package_id
    )
    if not package:
        return None
    stored_view = read_composition(index_root, package, build_id)
    product_ids = tuple(sorted({entry.product_id for entry in entries}))
    if not product_ids:
        product_ids = tuple(
            stored_view.product_ids()
            if stored_view is not None
            else index_format.list_partitions(index_root, package, build_id)
        )
    view = stored_view or legacy_view(build_id, product_ids)
    # 09 §8.1: partition_build_id — из манифеста; legacy-строка (NULL) падает обратно
    # на index_build_id (MVP-4 шаг 1: pb := index_build_id). Продукт без строки
    # манифеста разрешается видом композиции.
    partition_by_product: dict[str, str] = {
        entry.product_id: entry.partition_build_id or entry.index_build_id
        for entry in entries
    }
    for product_id in product_ids:
        partition_by_product.setdefault(product_id, view.build_id_for(product_id))
    embed_model_id = _build_embed_model_id(index_root, package, view, product_ids)
    if embed_model_id is None:
        embed_model_id = next(
            (entry.embed_model_id for entry in entries if entry.embed_model_id), None
        )
    return ExperimentTarget(
        package=package,
        build_id=build_id,
        product_ids=product_ids,
        embed_model_id=embed_model_id,
        partition_by_product=partition_by_product,
    )


def run_db_path(runs_dir: str | Path, run_id: str) -> Path:
    """Путь к операционной БД прогона: ``runs/{run_id}/cohound.db`` (``03`` §A.2)."""
    return Path(runs_dir) / run_id / "cohound.db"


def build_store(
    db_path: str | Path = ":memory:",
    *,
    profile: str = LOCAL_PROFILE,
    read_only: bool = False,
) -> SqliteStore:
    """Собирает операционное хранилище прогона.

    Для обоих профилей ``StorePort`` живёт в ядре (доверенная зона ``core``, ``01`` §3.0),
    поэтому выбор профиля на хранилище не влияет — разделяются лишь веб-адаптеры
    (:func:`build_adapters`). Каталог БД создаётся при необходимости.

    ``read_only=True`` — путь **читателя** (§4.7 ``12-cli-architecture-review.md``):
    каталог и файл не создаются, ``DDL``/миграции не применяются, запись отвергается
    SQLite. Отсутствующий файл → :class:`~core.store.sqlite.StoreNotInitialized`
    (CLI: ``EXIT_NOT_FOUND``), а не «создали пустую БД». Гейт версии схемы сохраняется.

    :param db_path: файл БД или ``":memory:"``.
    :param profile: профиль развёртывания (проверяется на известность).
    :param read_only: открыть существующую БД только на чтение (без создания).
    :raises WiringError: неизвестный профиль.
    """
    if profile not in SUPPORTED_PROFILES:
        raise WiringError(f"неизвестный профиль развёртывания: {profile!r}")
    if read_only:
        return SqliteStore(db_path, read_only=True)
    if str(db_path) != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return SqliteStore(db_path)


def build_gateway(
    deploy: DeployConfig,
    *,
    transport: TransportFn | None = None,
    extra_headers: Mapping[str, str] | None = None,
) -> HttpContentGateway:
    """Собирает HTTP-транспорт ядро→edge по секции ``edge.*`` (``07`` §7).

    Соответствие полей ``deploy.edge`` параметрам :class:`HttpContentGateway`:

    * ``base_url`` → ``base_url`` (обязателен для split-профилей, ``07`` §7);
    * ``timeout_s``/``max_retries``/``max_response_bytes`` → одноимённые лимиты;
    * ``tls.ca`` → ``ca`` (доверие к корпоративному CA, **без pinning**, ``07`` §8.1);
    * ``tls.client_cert``/``tls.client_key`` → mTLS workload-identity;
    * ``auth: oidc`` — токен выдаёт корпоративный IDP в рантайме, в ``DeployConfig``
      его нет; при необходимости заголовок передаётся через ``extra_headers``.

    ``transport`` — тестовый шов ``(op, envelope) -> envelope`` (без сети); при
    ``None`` используется реальный HTTP (ленивый ``httpx``).

    :raises WiringError: не задан ``edge.base_url``.
    """
    edge = deploy.edge
    base_url = (edge.base_url or "").strip()
    if not base_url:
        raise WiringError(
            f"профиль {deploy.profile!r} требует непустой edge.base_url (07 §7)"
        )
    return HttpContentGateway(
        base_url=base_url,
        schema_version=DEFAULT_SCHEMA_VERSION,
        transport=transport,
        timeout=edge.timeout_s,
        max_retries=edge.max_retries,
        max_response_bytes=edge.max_response_bytes,
        ca=edge.tls.ca,
        client_cert=edge.tls.client_cert,
        client_key=edge.tls.client_key,
        extra_headers=extra_headers,
    )


def _module_installed(name: str) -> bool:
    """Установлен ли модуль (без импорта тяжёлого пакета)."""
    return importlib.util.find_spec(name) is not None


def build_search_adapter(
    adapter: str,
    *,
    cache: ContentAddressedCache | None = None,
    search_mode: str | None = None,
) -> SearchAdapterPort:
    """Собирает внешний поисковый адаптер по имени для ``hound discover`` (``03`` §D.6).

    Провайдер — данные-конфигурация (не предметная категория, §1.3). Реальный адаптер
    подтягивает ``httpx`` **лениво**; готовность проверяется свойством ``available`` у
    вызывающего (CLI). Отсутствие ``httpx``/учётных данных не подменяется выдуманными
    кандидатами (I19) — это решает :func:`core.cli.main.cmd_discover`.

    :param adapter: имя адаптера из :data:`SUPPORTED_SEARCH_ADAPTERS`.
    :param cache: глобальный content-addressed кэш (R7). Тот же объект, что получают
        краулеры: адаптер кэширует им ``fetch``, а pipeline — ответы поиска
        (``cache/serp``, :func:`core.pipeline.search_log.external_search_observed`).
        ``None`` — без кэша (``--from-scratch``).
    :param search_mode: объявленный режим запроса (``sync``/``async``, ``run.yaml:
        serp.mode``). ``None`` — не объявлено: адаптер возьмёт env/дефолт кода (путь
        разведки, чей выход — кандидаты, а не измерение).
    :raises WiringError: неизвестный адаптер.
    """
    if adapter == "yandex":
        return YandexSearchAdapter(cache=cache, search_mode=search_mode)
    if adapter == "glm":
        return GlmSearchAdapter(cache=cache)
    raise WiringError(
        f"неизвестный поисковый адаптер: {adapter!r}; "
        f"поддерживаются: {', '.join(SUPPORTED_SEARCH_ADAPTERS)}"
    )


def _llm_configured(adapter: Any) -> bool:
    """Сконфигурирован ли LLM-адаптер (готовность и непустые ``base_url``/``model_id``)."""
    return bool(getattr(adapter, "available", False)) and bool(
        getattr(adapter, "model_id", "") or getattr(adapter, "base_url", "")
    )


def _search_adapter_configured(adapter: Any) -> bool:
    """Готов ли реальный поисковый адаптер к вызовам (зависимость и учётные данные).

    Условие — **конъюнкция** (S9b): адаптер объявляет себя доступным (``available`` —
    его собственный вердикт по **всем** требуемым швам, см. ``search_yandex``/
    ``search_glm``) **и** учётные данные на месте. Прежняя редакция выходила из проверки
    по первому ``False`` — это то же ``and``, но записанное так, что «доступен» и «есть
    ключ» выглядели независимыми воротами; при одном внедрённом ``http_get`` гейт
    пропускал адаптер, который падал в ``search()``.

    Все известные адаптеры (``yandex``/``glm``) выставляют атрибут ``api_key``; без
    ключа такой адаптер не готов, и кандидаты не выдумываются (I19). Логика остаётся
    generic: если у адаптера нет атрибута ``api_key`` вовсе, достаточно доступности
    (``available``) — иначе он ошибочно блокировался бы. Для инъектированного
    тестового шва проверка не применяется (см. ``cmd_discover``).
    """
    available = bool(getattr(adapter, "available", True))
    # Атрибут ``api_key`` есть → он обязан быть непустым; нет атрибута → хватает доступности.
    has_key = not (
        hasattr(adapter, "api_key") and not getattr(adapter, "api_key", None)
    )
    return available and has_key


class SearchResolutionError(WiringError):
    """Ошибка разрешения/готовности поискового адаптера (composition root, ``03`` §D.6).

    Несёт код правила CLI в ``code``:

    * ``"search"`` — выбран неизвестный провайдер поиска (``EXIT_USAGE``);
    * ``"I19"`` — реальный адаптер недоступен (нет зависимости/учётных данных);
      вызывающий маппит его в ``EXIT_ERROR`` (``discover``) либо ``EXIT_USAGE`` (scout).

    Наследует :class:`WiringError`, поэтому старые обработчики (``except WiringError``)
    не ломаются.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def resolve_and_build_search_adapter(
    *,
    lang: str,
    explicit: str | None,
    injected: Any | None = None,
    no_search: bool = False,
    cache: ContentAddressedCache | None = None,
    search_mode: str | None = None,
) -> tuple[Any | None, str | None]:
    """Разрешает провайдера поиска и собирает адаптер (общий хелпер ``discover``/scout).

    Семантика (детерминированная, без LLM — I16/I21):

    * ``injected is not None`` — тестовый шов: резолв/валидация провайдера пропускаются,
      возвращается ``(injected, getattr(injected, "provider", "") or None)``;
    * ``no_search`` — поиск выключен: ``(None, None)``;
    * иначе — ``load_deploy_config(None)`` → :func:`resolve_search_provider` → проверка
      против :data:`SUPPORTED_SEARCH_ADAPTERS` → :func:`build_search_adapter` → проверка
      готовности (:func:`_search_adapter_configured`).

    :param cache: глобальный content-addressed кэш (R7), передаётся в адаптер; ``None``
        — без кэша (``--from-scratch``). Тот же объект обслуживает ``fetch`` адаптера и
        SERP-кэш pipeline: второй кэш на тот же корень заводить незачем.
    :param search_mode: объявленный режим запроса (``run.yaml: serp.mode``); ``None`` —
        не объявлено, адаптер возьмёт env/дефолт (разведка). Передаётся **явно** из
        контракта прогона: условие измерения не наследуется из окружения.
    :returns: ``(adapter, provider)`` — провайдер для провенанса (I23).
    :raises SearchResolutionError: неизвестный провайдер (``code="search"``) либо
        недоступный реальный адаптер (``code="I19"``).
    :raises WiringError: ошибка сборки адаптера (пробрасывается как есть).
    """
    if injected is not None:
        return injected, (getattr(injected, "provider", "") or None)
    if no_search:
        return None, None
    config = load_deploy_config(None)
    provider = resolve_search_provider(config.search, lang=lang, explicit=explicit)
    # Валидация против реестра адаптеров — в composition root (config-слой
    # провайдеро-нейтрален). Неизвестный провайдер → понятная ошибка (I19: не выдумываем).
    if provider not in SUPPORTED_SEARCH_ADAPTERS:
        raise SearchResolutionError(
            "search",
            f"неизвестный провайдер поиска {provider!r} "
            "(config/deploy.yaml search.adapter/by_language либо --search-adapter); "
            f"поддерживаются: {', '.join(SUPPORTED_SEARCH_ADAPTERS)}",
        )
    adapter = build_search_adapter(provider, cache=cache, search_mode=search_mode)
    if not _search_adapter_configured(adapter):
        raise SearchResolutionError(
            "I19",
            "поисковый адаптер недоступен: нет httpx либо учётных данных "
            "(yandex: YANDEX_SEARCH_API_KEY/YANDEX_FOLDER_ID; "
            "glm: GLM_SEARCH_API_KEY); "
            "кандидаты не выдумываются",
        )
    return adapter, provider


def _env_float(name: str, default: float) -> float:
    """Читает положительный float из env; нечисловое значение → :class:`WiringError`."""
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise WiringError(f"{name}: ожидалось число, получено {raw!r}") from exc
    if value <= 0:
        raise WiringError(f"{name}: значение обязано быть положительным, получено {raw!r}")
    return value


def _env_int(name: str, default: int) -> int:
    """Читает положительный int из env; нечисловое значение → :class:`WiringError`."""
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise WiringError(f"{name}: ожидалось целое, получено {raw!r}") from exc
    if value <= 0:
        raise WiringError(f"{name}: значение обязано быть положительным, получено {raw!r}")
    return value


def _resolve_llm_slot_and_config(
    deploy_llm: LLMDeployConfig | None,
    slot: str | None,
    run_slot: str | None = None,
) -> tuple[str, LLMSlotConfig | None]:
    """Резолвит активный LLM-слот и его конфиг (CLI > env > run.yaml > deploy).

    Приоритет имени слота (§7.4): явный ``slot`` (CLI ``--llm-slot``) > ``COHOUND_LLM_SLOT``
    (env) > ``run_slot`` (``run.yaml: llm_slot``) > ``deploy_llm.slot`` > дефолт
    (:data:`~core.config.deploy.DEFAULT_LLM_SLOT`). Первые три уровня передаются как
    ``explicit`` в :func:`core.config.deploy.resolve_llm_slot` (который сам валидирует и
    откатывается к ``deploy_llm.slot``/дефолту). Возвращает ``(slot, LLMSlotConfig|None)``.
    """
    explicit = (
        slot
        if slot is not None and str(slot).strip()
        else os.environ.get(ENV_LLM_SLOT) or run_slot
    )
    resolved = resolve_llm_slot(deploy_llm, explicit=explicit)
    config = getattr(deploy_llm, resolved) if deploy_llm is not None else None
    return resolved, config


#: Env-переменные, которые **не** участвуют в идентичности модели оценщика (§7.4/I21).
#: Они действуют только на LLM-разведку (:func:`build_discovery_llm`): оценщик берёт
#: ``model_id``/``decoder`` из объявленного входа — слота ``deploy.llm`` или ``--llm-model``.
EVALUATOR_ENV_IGNORED: tuple[str, ...] = (ENV_LLM_MODEL, ENV_LLM_DECODER)

#: Объявленный источник для каждой игнорируемой env-переменной (текст предупреждения).
_EVALUATOR_ENV_DECLARED_SOURCE: dict[str, str] = {
    ENV_LLM_MODEL: (
        "модель оценщика — объявленный вход (deploy.llm.{slot}.model_id или --llm-model)"
    ),
    ENV_LLM_DECODER: (
        "decoder оценщика — объявленный вход (deploy.llm.{slot}.decoder)"
    ),
}


def resolve_evaluator_reasoning_effort(
    *,
    slot_config: LLMSlotConfig | None,
    effort: ReasoningEffort | str | None = None,
    run_effort: ReasoningEffort | str | None = None,
    slot_name: str | None = None,
) -> ReasoningEffort | None:
    """Резолвит усилие рассуждения оценщика — **одна** формула для контракта и рантайма.

    Приоритет — только объявленные входы, ровно как у
    :func:`resolve_evaluator_model_identity`: ``--llm-reasoning-effort`` (CLI ``effort``) >
    ``run.yaml: llm_reasoning_effort`` (``run_effort``) > поле активного слота
    ``deploy.llm.<slot>.reasoning_effort``. Env здесь не читается: уровень входит в
    ``llm_scenarios`` → ``execution_contract_digest`` → ``run_id``, поэтому обязан быть
    функцией объявленных входов (иначе два хоста с одним ``run.yaml`` получили бы разный
    ``run_id`` и разные вердикты под одним именем прогона).

    Функция нужна **одна**: ею пользуются и ``_resolve_llm_scenarios`` (контракт), и
    :func:`build_evaluator_llm` (рантайм), и ``evaluate_cell`` (через атрибут адаптера).
    Вторая формула разошлась бы с первой — ровно тот разрыв, который уже был измерен на
    модели (``resolve_evaluator_model_identity``).

    ``None`` — уровень не объявлен ни на одном слое: параметр на провод не уходит, и это
    **отличимо** от объявленного ``off`` (``off`` — заявление «рассуждение не запрошено»,
    ``None`` — «оси нет»).

    Выразимость объявленного уровня на проводе семейства модели проверяет **адаптер**
    (:func:`core.adapters.llm_local.reasoning_wire`): там же, где уровень становится
    телом запроса. Значение, которое нельзя выразить, — отказ, а не тихий пропуск.

    :param slot_config: конфиг активного слота (``None`` — слотов нет).
    :param effort: явный CLI-уровень (``--llm-reasoning-effort``).
    :param run_effort: уровень run-слоя (``run.yaml: llm_reasoning_effort``).
    :param slot_name: имя активного слота (для текста ошибки).
    :raises CliError: значение вне перечисления (опечатка) — отказ с перечнем, а не
        молчаливое «не объявлено».
    """
    declared_cli = _coerce_reasoning_effort(effort, source="--llm-reasoning-effort")
    declared_run = _coerce_reasoning_effort(
        run_effort, source=f"run.yaml: llm_reasoning_effort (слот {slot_name or '<slot>'})"
    )
    slot_value = getattr(slot_config, "reasoning_effort", None)
    slot_effort = _coerce_reasoning_effort(
        slot_value,
        source=f"deploy.llm.{slot_name or '<slot>'}.reasoning_effort",
    )
    return declared_cli if declared_cli is not None else (
        declared_run if declared_run is not None else slot_effort
    )


def _coerce_reasoning_effort(
    value: ReasoningEffort | str | None, *, source: str
) -> ReasoningEffort | None:
    """Приводит объявленный уровень к enum; ``None``/пусто — «не объявлено».

    Строка, которой нет в перечислении, — ошибка входа: молча превратить её в ``None``
    значило бы объявить «оси нет» вместо «опечатка», и прогон пошёл бы без параметра,
    называя ``run_id`` без уровня.
    """
    if value is None:
        return None
    if isinstance(value, ReasoningEffort):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return ReasoningEffort(text.lower())
    except ValueError as exc:
        allowed = ", ".join(item.value for item in ReasoningEffort)
        raise CliError(
            f"{source}: недопустимый уровень рассуждения «{value}»; "
            f"ожидается один из {allowed}",
            code="llm",
            exit_code=EXIT_USAGE,
        ) from exc


def ensure_reasoning_wire(
    model_id: str, effort: ReasoningEffort | None
) -> None:
    """Проверяет, что объявленный уровень рассуждения выразим на проводе модели.

    Единственная точка отказа для «уровень объявлен, но отправить его модели нечем»:
    вызывается и runtime-сборкой оценщика (:func:`build_evaluator_llm`), и расчётом
    контракта (``_resolve_llm_scenarios``), поэтому ``plan`` и ``run`` отказывают
    одинаково — прогон не может начаться с уровнем, которого модель не получит.

    :raises CliError: уровень невыразим (``EXIT_USAGE``, код ``llm``).
    """
    try:
        reasoning_wire(model_id, effort)
    except ReasoningWireUnsupported as exc:
        raise CliError(str(exc), code="llm", exit_code=EXIT_USAGE) from exc


def resolve_evaluator_model_identity(
    *,
    slot_config: LLMSlotConfig | None,
    slot_name: str | None = None,
    model_id: str | None = None,
    decoder: str | None = None,
) -> tuple[str, str]:
    """Резолвит идентичность модели оценщика: ``(model_id, decoder)`` (§7.4/I21).

    Приоритет — **только объявленные** входы: явный аргумент (CLI ``--llm-model``) > поле
    активного слота ``deploy.llm.<slot>`` > дефолт модуля (``decoder``). Env
    ``COHOUND_LLM_MODEL``/``COHOUND_LLM_DECODER`` здесь **не читается**: идентичность модели
    входит в ``execution_contract_digest`` (``llm_scenarios`` → ``run_id``), поэтому она
    обязана быть функцией объявленных входов, а не окружения хоста (иначе манифест называл
    бы модель, которая не работала). Endpoint/ключ/таймаут — операционные и берутся из env
    как раньше (:func:`build_evaluator_llm`).

    **Никакого угадывания.** При активном endpoint отсутствие ``model_id`` и в аргументе, и
    в слоте — явная ошибка, а не молчаливый дефолт: дефолтная модель, которой оператор не
    объявлял, — та же ложная провенанс-запись, только тише.

    :param slot_config: конфиг активного слота (``None`` — слотов нет).
    :param slot_name: имя активного слота (для текста ошибки/предупреждений).
    :param model_id: явно объявленная модель (CLI ``--llm-model``).
    :param decoder: явно объявленный механизм декодирования (сейчас — только benchmark).
    :raises CliError: endpoint активен, но модель не объявлена ни аргументом, ни слотом.
    """
    slot_model = (getattr(slot_config, "model_id", None) or "").strip()
    slot_decoder = (getattr(slot_config, "decoder", None) or "").strip()
    declared_model = (model_id or "").strip()
    resolved_model = declared_model or slot_model
    if not resolved_model:
        raise CliError(
            "модель оценщика не объявлена: endpoint задан, но model_id отсутствует и в "
            f"слоте deploy.llm.{slot_name or '<slot>'}.model_id, и в --llm-model. "
            f"{ENV_LLM_MODEL} для оценщика не читается: идентичность модели — объявленный "
            "вход (I21/§7.4). Укажите model_id в deploy.yaml или передайте --llm-model",
            code="llm",
            exit_code=EXIT_USAGE,
        )
    resolved_decoder = (decoder or "").strip() or slot_decoder or EVALUATOR_DEFAULT_DECODER
    return resolved_model, resolved_decoder


def llm_env_notices(
    *,
    deploy_llm: LLMDeployConfig | None = None,
    slot: str | None = None,
    run_slot: str | None = None,
) -> tuple[str, ...]:
    """Видимые предупреждения о расхождении env-настроек LLM с объявленными осями (§7.4/I21).

    Оценщик поднимается **частично** из окружения: endpoint/ключ/таймаут — операционные
    данные хоста (env), а модель — объявленный вход. Отсюда два класса расхождений, которые
    нельзя проглатывать (эти переменные живут в runbook'ах операторов):

    * ``COHOUND_LLM_MODEL``/``COHOUND_LLM_DECODER`` заданы, но оценщиком **не читаются**
      (действуют только на LLM-разведку) — иначе смена env молча меняла бы поведение;
    * endpoint пришёл из env ``COHOUND_LLM_BASE_URL`` — оценщик работает вне
      ``execution_contract_digest`` (``llm_scenarios`` пуста либо объявляет другой endpoint),
      и это должно быть сказано, а не подразумеваться.

    Функция чистая (читает env и конфиг, ничего не пишет): её вызывают composition root для
    ``wiring.jsonl`` и команда — для payload прогона. Одно и то же сообщение на один вызов —
    «один раз», а не на каждую фазу.

    :param deploy_llm: секция ``llm`` deploy-конфига (слоты); ``None`` — нет слотов.
    :param slot: явный слот (``--llm-slot``); ``None`` → env/run.yaml/deploy-слот.
    :param run_slot: слот из ``run.yaml: llm_slot`` (§4.5).
    """
    resolved_slot, slot_config = _resolve_llm_slot_and_config(deploy_llm, slot, run_slot)
    notices: list[str] = []
    for env_name in EVALUATOR_ENV_IGNORED:
        env_value = (os.environ.get(env_name) or "").strip()
        if not env_value:
            continue
        declared_source = _EVALUATOR_ENV_DECLARED_SOURCE[env_name].format(
            slot=resolved_slot
        )
        notices.append(
            f"{env_name}={env_value} игнорируется оценщиком: {declared_source}; "
            "env-значение действует только на LLM-разведку"
        )
    env_base = (os.environ.get(ENV_LLM_BASE_URL) or "").strip()
    if env_base:
        slot_base = (getattr(slot_config, "base_url", None) or "").strip()
        if not slot_base:
            notices.append(
                f"оценщик поднят на endpoint из env {ENV_LLM_BASE_URL}: слот "
                f"{resolved_slot} неактивен (base_url пуст), поэтому llm_scenarios пуста — "
                "модель оценщика вне execution contract (run_id её не описывает)"
            )
        elif normalize_api_base(env_base) != normalize_api_base(slot_base):
            # Сравниваются канонические базы (``normalize_api_base``): хвост ``/v1`` —
            # тот же endpoint, и объявлять расхождение там, где его нет, значит приучать
            # не читать предупреждения. Молчание здесь — утверждение «endpoint тот же»,
            # а оно проверяемое: обе базы приведены одной функцией.
            notices.append(
                f"endpoint оценщика взят из env {ENV_LLM_BASE_URL} и переопределяет слот "
                f"{resolved_slot} (объявленный base_url={slot_base}): прогон идёт не на "
                "объявленном endpoint"
            )
    return tuple(notices)


def llm_host_notices(
    *,
    deploy_llm: LLMDeployConfig | None = None,
    hosts: Sequence[str] = (),
    slot: str | None = None,
    run_slot: str | None = None,
) -> tuple[dict[str, Any], ...]:
    """Машиночитаемые замечания о хостах endpoint'а оценщика (§10): объявленное исключение.

    Замечания делятся на два класса:

    * **объявленные** — из флага ``deploy.llm.rnd_endpoint`` и хостов ``llm_scenarios``:
      строится доменной функцией :func:`core.domain.scoring.endpoint_host_notices`
      (единственная формула — фаза Ш8 кладёт **те же** замечания в профиль публикуемости,
      поэтому CLI и манифест не могут разойтись);
    * **env-расхождение** — endpoint пришёл из ``COHOUND_LLM_BASE_URL`` и поэтому вне
      ``execution_contract_digest``. Текст **переиспользуется** из
      :func:`llm_env_notices` (второй формулировки нет): здесь он только получает код
      ``endpoint_from_env``, чтобы попадать в один машиночитаемый список.

    Порядок детерминирован: сначала объявленные (порядок проверок доменной функции),
    затем env-расхождение. Пустой список — замечаний нет.
    """
    declared = list(
        endpoint_host_notices(
            rnd_endpoint=getattr(deploy_llm, "rnd_endpoint", None), hosts=hosts
        )
    )
    env_texts = llm_env_notices(deploy_llm=deploy_llm, slot=slot, run_slot=run_slot)
    for text in env_texts:
        if ENV_LLM_BASE_URL not in text:
            continue
        declared.append(
            {
                "code": "endpoint_from_env",
                "level": "warning",
                "hosts": [],
                "message": text,
            }
        )
    return tuple(declared)


def serp_env_notices(
    *,
    deploy_search: Any = None,
    run_rounds: int | None = None,
    cli_rounds: Any = None,
) -> tuple[str, ...]:
    """Видимые предупреждения о переключателе SERP-контура из env (I3/§5).

    ``COHOUND_SERP`` — операционный аварийный троттл (образец — :func:`llm_env_notices`).
    Он **читается** (в отличие от ``COHOUND_LLM_MODEL``), потому что число раундов меняет
    корпус, а значит и ``run_id``; но именно поэтому расхождение env с объявленным входом
    обязано быть видимым: молчаливая подмена превратила бы «аварийное выключение» в
    невидимое изменение условий измерения.

    Сообщается: (а) значение env и его эффект; (б) если env перекрывает значение
    ``run.yaml``/``deploy.yaml`` — об этом говорится прямо; (в) если контур выключен
    env, а объявлен включённым — тоже.

    Функция чистая (env + конфиг), вызывается один раз composition root'ом; те же строки
    попадают в payload прогона и ``logs/wiring.jsonl``.
    """
    raw = os.environ.get(ENV_SERP)
    if raw is None or not str(raw).strip():
        return ()
    try:
        parse_serp_switch(raw, where=ENV_SERP)
    except ValueError as exc:  # невалидное значение — не «проигнорируем тихо»
        return (f"{ENV_SERP}={raw!r} не разбирается ({exc}); контур остаётся как объявлен",)
    effective, source = resolve_serp_rounds(
        deploy_search,
        cli=cli_rounds,
        env=raw,
        run_rounds=run_rounds,
    )
    if source != "env":
        declared = (
            "cli" if source == "cli" else "run.yaml/deploy.yaml"
        )
        return (
            f"{ENV_SERP}={raw.strip() or '∅'} не действует: раунды заданы уровнем "
            f"{declared} (эффективно {effective})",
        )
    declared_rounds = run_rounds
    if declared_rounds is None:
        declared_rounds = getattr(getattr(deploy_search, "serp", None), "rounds", None)
    if effective == 0:
        tail = (
            f"перекрывает объявленные run.yaml/deploy.yaml rounds={declared_rounds}"
            if declared_rounds
            else "контур был выключен и остаётся выключенным"
        )
        return (
            f"SERP-контур ВЫКЛЮЧЕН из env {ENV_SERP}={raw.strip()}: {tail}; "
            "значение входит в serp_rounds ⇒ run_id другого корпуса",
        )
    return (
        f"SERP-контур включён из env {ENV_SERP}={raw.strip()}: раундов {effective} "
        f"(объявлено run.yaml/deploy.yaml: {declared_rounds}); env перекрывает "
        "объявленный вход, значение входит в serp_rounds ⇒ run_id",
    )


def build_discovery_llm(
    *,
    base_url: str | None = None,
    api_key: str | None = None,
    model_id: str | None = None,
    decoder: str | None = None,
    timeout: float | None = None,
    max_tokens: int | None = None,
    generate: DiscoveryGenerateFn | None = None,
    slot: str | None = None,
    deploy_llm: LLMDeployConfig | None = None,
) -> DiscoveryLLMPort:
    """Собирает LLM-адаптер разведки паспорта (``03`` §E.3; слоты — :class:`LLMDeployConfig`).

    Источник конфигурации (по убыванию приоритета): явные аргументы (CLI-флаги) >
    env ``COHOUND_LLM_*`` > поля выбранного слота ``deploy_llm`` > дефолты модуля.
    Активный слот резолвится :func:`_resolve_llm_slot_and_config` (``slot`` >
    ``COHOUND_LLM_SLOT`` > ``deploy_llm.slot`` > ``cheap``). Ключ — единственный источник
    ``COHOUND_LLM_API_KEY`` (внешних фолбэков нет). Пустые ``base_url``/``model_id``
    допустимы: готовность проверяет вызывающий (``cmd_passport_scout``) и выдаёт
    понятную ошибку ``llm`` (:data:`EXIT_USAGE`), а не падает.

    **Асимметрия с оценщиком — намеренная (§7.4, I21).** Здесь env
    ``COHOUND_LLM_MODEL``/``COHOUND_LLM_DECODER`` по-прежнему переопределяют слот: выход
    разведки — кандидаты/гипотезы (G2, human-ревью), а не evidence, поэтому модель здесь —
    операционная настройка, а не условие измерения. У оценщика (Ш6) идентичность модели —
    **объявленный** вход: ``llm_scenarios`` входят в ``execution_contract_digest``, поэтому
    env там не читается (:func:`build_evaluator_llm`). Это не недосмотр: смешивать
    «кандидатов» и «доказательства» в одну политику значило бы позволить окружению
    переписывать провенанс вердиктов.

    :param slot: явный активный слот (``--llm-slot``); ``None`` → env/deploy-слот.
    :param deploy_llm: секция ``llm`` deploy-конфига (слоты моделей); ``None`` — нет.
    :param generate: тестовый/оффлайн-шов ``(prompt, schema) -> str`` (без сети).
    :raises WiringError: нечисловой/неположительный ``timeout``/``max_tokens`` из env.
    :raises ValueError: неизвестный слот (``--llm-slot``/``COHOUND_LLM_SLOT``).
    """
    _resolved_slot, slot_config = _resolve_llm_slot_and_config(deploy_llm, slot)
    slot_base = slot_config.base_url if slot_config is not None else None
    slot_model = slot_config.model_id if slot_config is not None else None
    slot_decoder = slot_config.decoder if slot_config is not None else None
    slot_timeout = (
        slot_config.timeout_s
        if slot_config is not None and slot_config.timeout_s is not None
        else DISCOVERY_DEFAULT_TIMEOUT
    )
    slot_max_tokens = (
        slot_config.max_tokens
        if slot_config is not None and slot_config.max_tokens is not None
        else DISCOVERY_DEFAULT_MAX_TOKENS
    )

    resolved_base = (
        base_url
        if base_url is not None
        else os.environ.get(ENV_LLM_BASE_URL) or slot_base or DEFAULT_LLM_BASE_URL
    )
    resolved_key = (
        api_key
        if api_key is not None
        else os.environ.get(ENV_LLM_API_KEY)
    )
    resolved_model = (
        model_id
        if model_id is not None
        else os.environ.get(ENV_LLM_MODEL) or slot_model or DEFAULT_LLM_MODEL
    )
    resolved_decoder = (
        decoder
        if decoder is not None
        else os.environ.get(ENV_LLM_DECODER) or slot_decoder or DISCOVERY_DEFAULT_DECODER
    )
    resolved_timeout = (
        timeout
        if timeout is not None
        else _env_float(ENV_LLM_TIMEOUT, slot_timeout)
    )
    resolved_max_tokens = (
        max_tokens
        if max_tokens is not None
        else _env_int(ENV_LLM_MAX_TOKENS, slot_max_tokens)
    )
    return DiscoveryLLM(
        base_url=resolved_base or "",
        api_key=(resolved_key or None),
        model_id=resolved_model or "",
        decoder=resolved_decoder or DISCOVERY_DEFAULT_DECODER,
        timeout=resolved_timeout,
        max_tokens=resolved_max_tokens,
        generate=generate,
    )


def build_evaluator_llm(
    *,
    deploy_llm: LLMDeployConfig | None = None,
    slot: str | None = None,
    generate: EvaluatorGenerateFn | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
    model_id: str | None = None,
    model_version: str | None = None,
    decoder: str | None = None,
    timeout: float | None = None,
    run_slot: str | None = None,
    seed: int | None = None,
    temperature: float | None = None,
    reasoning_effort: ReasoningEffort | str | None = None,
    run_reasoning_effort: ReasoningEffort | str | None = None,
    max_connections: int | None = None,
) -> EvaluatorPort | None:
    """Собирает runtime-оценщик из активного LLM-слота; ``None`` — слот не сконфигурирован.

    Оценщик (I18, без веб-тула) конструируется :class:`~core.adapters.llm_local.LocalEvaluator`
    из **активного слота** (``slot`` (CLI) > ``COHOUND_LLM_SLOT`` (env) > ``run_slot``
    (``run.yaml: llm_slot``) > ``deploy_llm.slot`` > ``cheap``, см.
    :func:`_resolve_llm_slot_and_config`).

    **Идентичность модели — объявленный вход, не env (§7.4/I21).** ``model_id``/``decoder``
    резолвятся :func:`resolve_evaluator_model_identity`: явный аргумент (``--llm-model``) >
    поле слота > дефолт модуля. Env ``COHOUND_LLM_MODEL``/``COHOUND_LLM_DECODER`` здесь
    **не читаются** (они действуют только на LLM-разведку, :func:`build_discovery_llm`):
    ``llm_scenarios`` входят в ``execution_contract_digest``, и манифест обязан называть ту
    модель, которая действительно работала. Модель не объявлена ни флагом, ни слотом при
    активном endpoint — явная ошибка (:class:`~core.cli.errors.CliError`, код ``llm``), а не
    тихий дефолт. О факте игнорирования env сообщает :func:`llm_env_notices`.

    **Операционные поля — из env, как раньше.** ``base_url``/``api_key``/``timeout`` —
    данные хоста (endpoint/секрет/лимит), а не условие измерения: приоритет явный аргумент >
    env > слот > дефолт.

    **Условие активности.** Если у выбранного слота (и env/аргументов) пустой ``base_url``
    — возвращается ``None``: оценщик не поднимается, поведение прогона прежнее (сценарий
    evaluator требует явного endpoint внутреннего [OI]-совместимого API корпоративного
    периметра). ``api_key`` — единственный источник ``COHOUND_LLM_API_KEY`` (внешних
    фолбэков нет).

    :param slot: явный активный слот (``--llm-slot``); ``None`` → env/run.yaml/deploy-слот.
    :param deploy_llm: секция ``llm`` deploy-конфига; ``None`` — нет слотов.
    :param run_slot: слот из ``run.yaml: llm_slot`` (§4.5); ниже env, выше ``deploy_llm.slot``.
    :param model_id: явно объявленная модель (CLI ``--llm-model``) — побеждает поле слота.
    :param generate: тестовый/оффлайн-шов ``(prompt, schema) -> str`` (без сети).
    :param seed: seed сэмплирования; ``None`` — дефолт адаптера (``0``). Benchmark оценщика
        передаёт ``0`` явно: детерминизм — условие измерения (A3), а не настройка.
    :param temperature: температура; ``None`` — дефолт адаптера (``0.0``, greedy).
    :param reasoning_effort: явный CLI-уровень рассуждения (``--llm-reasoning-effort``).
    :param run_reasoning_effort: уровень run-слоя (``run.yaml: llm_reasoning_effort``) —
        цепочка та же, что у модели (:func:`resolve_evaluator_reasoning_effort`), поэтому
        рантайм и ``llm_scenarios`` не могут разойтись.
    :param max_connections: потолок соединений HTTP-клиента оценщика — ось исполнения
        ``--llm-concurrency``: при N вызовах в полёте клиент обязан держать N соединений.
        ``None`` — дефолт ``httpx`` (``100``); в контракт/``run_id`` не входит (это время
        фазы, а не условие измерения).
    :raises ValueError: неизвестный слот (``--llm-slot``/``COHOUND_LLM_SLOT``/``run.yaml``).
    :raises CliError: endpoint активен, но модель не объявлена (ни слот, ни ``--llm-model``).
    """
    _resolved_slot, slot_config = _resolve_llm_slot_and_config(deploy_llm, slot, run_slot)
    slot_base = slot_config.base_url if slot_config is not None else None
    slot_version = slot_config.model_version if slot_config is not None else None
    slot_timeout = (
        slot_config.timeout_s
        if slot_config is not None and slot_config.timeout_s is not None
        else EVALUATOR_DEFAULT_TIMEOUT
    )

    resolved_base = (
        base_url
        if base_url is not None
        else os.environ.get(ENV_LLM_BASE_URL) or slot_base
    )
    if not (resolved_base and str(resolved_base).strip()):
        return None

    resolved_key = (
        api_key
        if api_key is not None
        else os.environ.get(ENV_LLM_API_KEY)
    )
    # §7.4/I21: модель/decoder — объявленный вход (аргумент > слот > дефолт), env не читается.
    resolved_model, resolved_decoder = resolve_evaluator_model_identity(
        slot_config=slot_config,
        slot_name=_resolved_slot,
        model_id=model_id,
        decoder=decoder,
    )
    # §7.4 (решение 06.11.2026): усилие рассуждения — объявленный вход той же цепочкой,
    # что модель (CLI > run.yaml > слот). Вторая формула разошлась бы с контрактом, и
    # манифест назвал бы уровень, с которым модель не работала.
    resolved_effort = resolve_evaluator_reasoning_effort(
        slot_config=slot_config,
        effort=reasoning_effort,
        run_effort=run_reasoning_effort,
        slot_name=_resolved_slot,
    )
    # Объявленный уровень обязан быть выразим на проводе **этой** модели: иначе прогон
    # пошёл бы без параметра, а ``llm_scenarios``/``run_id`` называли бы уровень. Это
    # ошибка входа (``EXIT_USAGE`` до старта), а не сбой исполнения одной ячейки.
    ensure_reasoning_wire(resolved_model, resolved_effort)
    resolved_version = (
        model_version if model_version is not None else slot_version
    )
    resolved_timeout = (
        timeout
        if timeout is not None
        else _env_float(ENV_LLM_TIMEOUT, slot_timeout)
    )
    return LocalEvaluator(
        generate=generate,
        model_id=resolved_model,
        model_version=resolved_version or "",
        decoder=resolved_decoder,
        base_url=str(resolved_base).strip(),
        api_key=(resolved_key or None),
        timeout=resolved_timeout,
        seed=EVALUATOR_DEFAULT_SEED if seed is None else seed,
        temperature=(
            EVALUATOR_DEFAULT_TEMPERATURE if temperature is None else temperature
        ),
        # V5: потолок генерации из слота. До правки поле читалось только
        # discovery-адаптером, а оценщик уходил без ``max_tokens`` — потолок задавал
        # сервер, и обрезание приходило как невалидный JSON. Одна константа на провод,
        # провенанс (``llm_scenarios.max_tokens``) и адаптер.
        max_tokens=(
            slot_config.max_tokens if slot_config is not None else None
        ),
        # Уровень рассуждения доезжает до провода: адаптер строит тело запроса по
        # семейству модели и **отказывает**, если уровень невыразим
        # (``reasoning_wire``) — тихий пропуск параметра был бы ложным провенансом.
        reasoning_effort=resolved_effort,
        # Ось исполнения Ш6: пул соединений одного клиента адаптера. Клиент один на
        # адаптер (``httpx.post`` создавал бы его на каждый вызов — лишнее TLS-рукопожатие
        # при параллельных вызовах).
        max_connections=max_connections,
    )


#: Env-переменная выбора краулера ``passport-scout``: ``http`` (дефолт) | ``scrapy`` | ``browser``.
ENV_SCOUT_CRAWLER = "COHOUND_SCOUT_CRAWLER"
#: Дефолтный (лёгкий) краулер scout — обычный HTTP без браузера (A1, MVP).
DEFAULT_SCOUT_CRAWLER = "http"
#: Поддерживаемые краулеры scout: лёгкий ``http``, браузерный ``scrapy`` и
#: ``browser`` (:class:`BrowserCrawler` — честный/hybrid браузерный рендер, SPA/JS).
SUPPORTED_SCOUT_CRAWLERS: tuple[str, ...] = ("http", "scrapy", "browser")


def _crawl_discovery_enabled(
    crawl: CrawlConfig,
    *,
    run_crawl: CrawlRunConfig | None = None,
    cli_max_depth: int | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
) -> bool:
    """Включает ли эффективная конфигурация обход ссылок (discovery) — N1/§10.

    Discovery включён, если хоть один слой (app ``deploy.yaml:crawl`` < run ``run.yaml`` <
    per-site ``domain_profiles``) просит ``max_depth > 0`` **или** у сайта задан ``sitemap``
    (его ``<loc>`` — дополнительные seeds). Явный ``crawl.robots`` (app-слой) переопределяет
    автодетект: ``True`` — принуждать robots даже без discovery, ``False`` — не принуждать
    даже при нём. Без discovery robots не трогается вовсе — прогон остаётся байт-в-байт
    прежним (никаких лишних fetch ``robots.txt``).
    """
    if crawl.robots is not None:
        return bool(crawl.robots)
    depths: list[int] = []
    if crawl.max_depth:
        depths.append(int(crawl.max_depth))
    # ``max_depth`` слоя ``run.yaml`` объявлен как ``int | None``: сужаем до проверки,
    # иначе ``int(None)`` отвергнут статически (а в рантайме недостижим — есть guard).
    run_max_depth = getattr(run_crawl, "max_depth", None) if run_crawl is not None else None
    if run_max_depth:
        depths.append(int(run_max_depth))
    if cli_max_depth is not None:
        depths.append(int(cli_max_depth))
    for profile in (domain_profiles or {}).values():
        depth = getattr(profile, "max_depth", None)
        if depth:
            depths.append(int(depth))
    if any(depth > 0 for depth in depths):
        return True
    return any(getattr(profile, "sitemap", None) for profile in (domain_profiles or {}).values())


def _robots_gate(http_crawler: CrawlerPort, *, user_agent: str | None) -> RobotsGate:
    """``RobotsGate`` поверх отдельного HTTP-краулера (фетч ``robots.txt``) — N1.

    Фетч ``robots.txt`` — ленивый (при первом ``allowed(host)``) и происходит **внутри**
    ``fetch`` страницы, который corpus уже пейсит единым владельцем (:class:`RateLimiter`,
    P2-3): отдельного пейсера не заводим (иначе было бы два владельца).

    **Три исхода чтения ленты, а не два** (:class:`RobotsGate`, RFC 9309 §2.3.1.4): прочитано
    (в т.ч. пусто) ⇒ разрешено всё; ``404``/``410`` ⇒ :class:`CrawlNotFound` пропускается
    наружу и гейт выводит ``not_found`` — тоже разрешено всё, это ответ origin; 5xx/сеть/
    таймаут ⇒ ``None`` ⇒ ``unavailable`` ⇒ origin **не обходится**. Каждому исходу свой
    ``refusal_reason`` у адаптера: ``robots_disallow`` против ``robots_unavailable``.

    Почему пропуск ``CrawlNotFound`` обязателен (замер 28.10, 12 хостов пакета):
    ``edu.postgrespro.ru``, ``docs.tantorlabs.ru`` и ``wiki.astralinux.ru`` отвечают ``404``
    на ``robots.txt``. С ```` except Exception: return None```` они получали ``unavailable``
    и выпадали из корпуса **целиком** — по комментариям пакета это ~800 + ~664 + ~68
    документов. Политика, которая на пустом месте отнимает три хоста, — не осторожность.
    """

    def fetch(host: str) -> str | None:
        try:
            page = http_crawler.fetch(
                f"https://{host}/robots.txt", FetchPlan(FetchMode.HTTP)
            )
        except CrawlNotFound:
            # ``404``/``410`` — **наблюдение** «ленты нет», а не отсутствие наблюдения.
            # Пропускаем исключение: :meth:`RobotsGate._read` распознаёт его по таксономии
            # (``http_status``/``failure_kind``) и даёт ``not_found`` — разрешено всё.
            # Глотать его здесь значило бы объявить недоступной ленту, про которую origin
            # прямо сказал, что её нет.
            raise
        except Exception:  # noqa: BLE001 — 5xx/сеть/таймаут = наблюдения нет
            return None
        # Пустой прочитанный ответ — факт («лента пуста, разрешено всё»), а не ``None``:
        # ``None`` означает «не смогли прочитать» и по RFC запрещает origin.
        raw = getattr(page, "content", b"") or b""
        return raw.decode("utf-8", errors="replace")

    return RobotsGate(fetch, user_agent=user_agent or "*")


#: Ярлык источника «движок задан слоем ``crawl_sources.yaml``» (наблюдаемость §D.6).
_ENGINE_SOURCE_CRAWL_SOURCES = "crawl_sources.engine"
#: Ярлык источника «движок выведен из ``fetch: browser`` доменного профиля».
_ENGINE_SOURCE_PROFILE = "profile.fetch"
#: Ярлык источника «дефолтный движок (env не задан)».
_ENGINE_SOURCE_DEFAULT = "default"


def _declared_engine_uses(
    domain_profiles: Mapping[str, DomainProfile] | None,
    crawl_sources: CrawlSourcesConfig | None,
) -> dict[str, dict[str, Any]]:
    """Источники, **запросившие** каждый движок (только для лога, не для выбора).

    Зеркало правила выбора активного краулера в pipeline/edge (``wtarget.engine`` →
    ``fetch: browser`` → env/дефолт), но здесь ничего не решается: функция собирает
    объяснение «почему движок попал в карту». Возвращает ``engine → {sources, details}``,
    где ``sources`` — ярлыки слоёв, ``details`` — адреса слоя (продукт/хост/дефолт).

    Env/дефолт сюда **не** входят: их добавляет вызывающая фабрика, потому что именно она
    различает «оператор задал env» и «применился дефолт».
    """
    uses: dict[str, dict[str, Any]] = {}

    def note(engine: str | None, source: str, detail: str | None) -> None:
        name = (engine or "").strip().lower()
        if not name:
            return
        entry = uses.setdefault(name, {"sources": [], "details": {}})
        if source not in entry["sources"]:
            entry["sources"].append(source)
        if detail:
            labels = entry["details"].setdefault(source, [])
            if detail not in labels:
                labels.append(detail)

    if crawl_sources is not None:
        note(crawl_sources.defaults.engine, _ENGINE_SOURCE_CRAWL_SOURCES, "defaults")
        for product_id in sorted(crawl_sources.products):
            note(
                crawl_sources.products[product_id].engine,
                _ENGINE_SOURCE_CRAWL_SOURCES,
                f"products[{product_id}]",
            )
        for product_id in sorted(crawl_sources.sites):
            for host in sorted(crawl_sources.sites[product_id]):
                note(
                    crawl_sources.sites[product_id][host].engine,
                    _ENGINE_SOURCE_CRAWL_SOURCES,
                    f"sites[{product_id}][{host}]",
                )
    # Пустой mapping вместо ``None``: индекс по ``domain_profiles[host]`` под ``None``
    # не индексируется статически (в рантайме ветка недостижима — цикл пуст).
    profiles = domain_profiles or {}
    for host in sorted(profiles):
        if profiles[host].fetch is FetchMode.BROWSER:
            note("browser", _ENGINE_SOURCE_PROFILE, host)
    return uses


def _engine_report_entry(uses: dict[str, dict[str, Any]], engine: str) -> dict[str, Any]:
    """Возвращает (создавая при необходимости) запись отчёта для движка."""
    return uses.setdefault(
        engine, {"sources": [], "details": {}, "built": True, "unavailable": None}
    )


def _format_engine_sources(entry: Mapping[str, Any]) -> str:
    """Форматирует источники решения для строки лога (без секретов)."""
    parts: list[str] = []
    details = entry.get("details") or {}
    for source in entry.get("sources") or ():
        labels = list(details.get(source) or ())
        if not labels:
            parts.append(str(source))
            continue
        shown = labels[:5]
        suffix = f", …+{len(labels) - len(shown)}" if len(labels) > len(shown) else ""
        parts.append(f"{source}[{', '.join(shown)}{suffix}]")
    return ", ".join(parts) or _ENGINE_SOURCE_DEFAULT


def _log_engine_selection(
    engine_log: PhaseLogger | None,
    uses: Mapping[str, Mapping[str, Any]],
) -> None:
    """Пишет одну строку на движок: эффективный движок и **почему** он выбран (§D.6).

    Движок — commodity-слой (A2) и не входит в ``execution_contract_digest``; без этой
    строки подмена ``scrapy`` → ``http`` не оставляет следа в прогоне. ``PhaseLogger`` —
    существующий хелпер ``runs/{run_id}/logs/{phase}.jsonl`` (``core.util.logging``);
    строки идут в ``logs/wiring.jsonl`` при сборке портов.
    """
    if engine_log is None:
        return
    for engine in sorted(uses):
        entry = uses[engine]
        message = (
            f"движок краула: {engine} (источник: {_format_engine_sources(entry)})"
        )
        ctx: dict[str, Any] = {
            "event": "engine_selection",
            "engine": engine,
            "sources": list(entry.get("sources") or ()),
            "scope": [
                label
                for labels in (entry.get("details") or {}).values()
                for label in labels
            ],
        }
        if entry.get("built", True):
            engine_log.info(message, **ctx)
        else:
            reason = entry.get("unavailable") or "нет доступного адаптера"
            engine_log.warn(f"{message}; движок НЕ собран: {reason}", **ctx)


def build_scout_crawler(
    *,
    deploy: DeployConfig | None = None,
    cache: ContentAddressedCache | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
    run_crawl: CrawlRunConfig | None = None,
    cli_max_depth: int | None = None,
    engine_log: PhaseLogger | None = None,
) -> CrawlerPort:
    """Собирает краулер для ``passport-scout`` (дефолт — лёгкий, A1/MVP).

    Обычный сайт = ``httpx`` (:class:`HttpCrawler`, ленивый импорт); браузерный рендер —
    опция для SPA/JS: ``scrapy`` (:class:`ScrapyCrawler`, Scrapy/playwright) или ``browser``
    (:class:`BrowserCrawler`, прямой Playwright, honest/hybrid). Выбор — через env
    :data:`ENV_SCOUT_CRAWLER` ∈ :data:`SUPPORTED_SCOUT_CRAWLERS`:

    * ``http`` (дефолт) — :class:`HttpCrawler`;
    * ``scrapy`` — :class:`ScrapyCrawler` (браузерный рендер через Scrapy/playwright);
    * ``browser`` — :class:`BrowserCrawler` (честный/hybrid браузерный рендер, SPA/JS).

    Для ``http`` применяется операционная политика :class:`~core.config.deploy.CrawlConfig`
    из ``deploy.yaml`` (``user_agent``/``timeout_s``/``follow_redirects``). Для ``browser``
    применяется ``deploy.crawl`` (``user_agent``) и ``deploy.crawl.browser``
    (:class:`~core.config.deploy.CrawlBrowserConfig`: ``cdp_url``/``user_data_dir``/
    ``headless``/``channel``/``timeout_ms``); декларативный ``mode`` не передаётся —
    фактическую ветку (CDP vs persistent-профиль) выводит сам адаптер по данным. ``deploy=None``
    → конфиг загружается канонически (:func:`load_deploy_config`); при профиле ``local``
    без файла значения совпадают с дефолтами адаптера — поведение не меняется
    (обратная совместимость). Недоступность зависимости даёт понятный ``CrawlError`` на
    фетче (адаптер не падает на конструкторе). Тесты инъектируют ``crawler`` в :func:`main`.

    **Исполнимость (D2).** Лёгкий ``HttpCrawler`` не умеет ``fetch: browser``. Проверка
    исполнимости **не** делается здесь: иначе ``hound run`` падал бы на сборке композиции
    даже для ``--until plan`` и для наборов URL, не задевающих browser-домены. Она
    URL-скоупедится в pipeline (:func:`core.pipeline.corpus.build_corpus`): browser-URL при
    HTTP-краулере пропускается с пофURL-ошибкой, не роняя фазу. Выбор
    ``http``/``scrapy``/``browser`` — только env :data:`ENV_SCOUT_CRAWLER`.

    **R7-кэш.** ``cache`` — глобальный content-addressed кэш (``core.util.cache``,
    ADR 0016), протягивается в краулер (у :class:`BrowserCrawler` кэшируется **только**
    hybrid-путь; honest-рендер всегда живой). План (``FetchPlan``) резолвится в pipeline
    (:func:`core.pipeline.fetch_plan.resolve_fetch_plan`), поэтому профили сюда не передаются.

    **Пейсинг — один источник.** Централизованный пейсинг живёт в corpus (полоса P2-3);
    адаптерный ``rate_limiter`` здесь **не** передаётся (``None``) — двойного сна нет.

    :param deploy: типизированный deploy-конфиг (политика ``crawl``); ``None`` → канон.
    :param cache: глобальный кэш внешних вызовов; ``None`` → без кэша.
    :param domain_profiles: зарезервирован для совместимости сигнатуры; исполнимость
        профилей проверяется URL-скоупедированно в pipeline (D2).
    :param run_crawl: ``crawl``-секция ``run.yaml`` (run-слой) — участвует в автодетекте
        discovery для robots (N1).
    :param cli_max_depth: ``--max-depth`` оператора — участвует в автодетекте discovery (N1).
    :param engine_log: лог наблюдаемости движка (``03`` §D.6); ``None`` → не пишем
        (тесты/вызовы без прогона). Одна строка: эффективный движок и источник решения.
    :raises WiringError: неизвестное значение ``COHOUND_SCOUT_CRAWLER``.
    """
    raw = os.environ.get(ENV_SCOUT_CRAWLER)
    name = (raw if raw is not None else DEFAULT_SCOUT_CRAWLER).strip().lower()
    if not name:
        name = DEFAULT_SCOUT_CRAWLER
    if name not in SUPPORTED_SCOUT_CRAWLERS:
        raise WiringError(
            f"{ENV_SCOUT_CRAWLER}: неизвестный краулер {name!r}; "
            f"поддерживаются: {', '.join(SUPPORTED_SCOUT_CRAWLERS)}"
        )
    env_value = (raw or "").strip().lower()
    source = (
        f"env:{ENV_SCOUT_CRAWLER}={env_value}" if env_value else _ENGINE_SOURCE_DEFAULT
    )
    _log_engine_selection(
        engine_log,
        {name: {"sources": [source], "details": {}, "built": True}},
    )
    if name == "http":
        config = deploy if deploy is not None else load_deploy_config(None)
        robots = _crawler_robots(
            config,
            run_crawl=run_crawl,
            cli_max_depth=cli_max_depth,
            domain_profiles=domain_profiles,
            cache=cache,
        )
        return HttpCrawler(**_http_crawler_kwargs(config), cache=cache, robots=robots)
    if name == "scrapy":
        # ScrapyCrawler уже выставляет ``ROBOTSTXT_OBEY`` в дефолтных настройках.
        config = deploy if deploy is not None else load_deploy_config(None)
        return ScrapyCrawler(
            cache=cache, max_response_bytes=config.crawl.max_response_bytes
        )
    if name == "browser":
        config = deploy if deploy is not None else load_deploy_config(None)
        robots = _crawler_robots(
            config,
            run_crawl=run_crawl,
            cli_max_depth=cli_max_depth,
            domain_profiles=domain_profiles,
            cache=cache,
        )
        return BrowserCrawler(
            **_browser_crawler_kwargs(config), cache=cache, robots=robots
        )
    # Недостижимо: имя сверено с ``SUPPORTED_SCOUT_CRAWLERS`` выше. Явный отказ, а не
    # «молча вернуть хоть что-то»: добавленное в кортеж имя без ветки сборки обязано
    # падать здесь, а не подменять объявленный движок другим.
    raise WiringError(
        f"{ENV_SCOUT_CRAWLER}: краулер {name!r} объявлен в "
        f"{SUPPORTED_SCOUT_CRAWLERS}, но ветки сборки у него нет"
    )


def _crawler_robots(
    config: DeployConfig,
    *,
    run_crawl: CrawlRunConfig | None,
    cli_max_depth: int | None,
    domain_profiles: Mapping[str, DomainProfile] | None,
    cache: ContentAddressedCache | None,
) -> RobotsGate | None:
    """``RobotsGate`` для адаптера при включённом discovery, иначе ``None`` (N1).

    Без discovery возвращается ``None``: адаптер не делает ни одного лишнего запроса
    ``robots.txt`` — прогон байт-в-байт прежний. При discovery gate строится поверх
    отдельного HTTP-краулера (без самого gate — рекурсии нет).
    """
    crawl = config.crawl
    if not _crawl_discovery_enabled(
        crawl,
        run_crawl=run_crawl,
        cli_max_depth=cli_max_depth,
        domain_profiles=domain_profiles,
    ):
        return None
    fetcher_crawler = HttpCrawler(**_http_crawler_kwargs(config), cache=cache)
    return _robots_gate(fetcher_crawler, user_agent=crawl.user_agent)


def _referenced_crawl_engines(
    domain_profiles: Mapping[str, DomainProfile] | None,
    crawl_sources: CrawlSourcesConfig | None,
) -> set[str]:
    """Движки, реально запрошенные конфигурацией (для ленивой сборки тяжёлых адаптеров).

    Источники признака: env :data:`ENV_SCOUT_CRAWLER` (явный выбор оператора), поля
    ``engine`` слоёв ``crawl_sources`` (``defaults``/``products``/``sites``) и наличие
    ``fetch: browser`` хотя бы в одном доменном профиле (нужен browser-capable движок).
    :func:`build_scout_crawlers` строит ``scrapy``/``browser`` только для упомянутых
    движков, чтобы не поднимать Playwright/Scrapy впустую (A1).
    """
    engines: set[str] = set()
    env = os.environ.get(ENV_SCOUT_CRAWLER)
    if env is not None and env.strip():
        engines.add(env.strip().lower())
    if crawl_sources is not None:
        sources = [crawl_sources.defaults]
        sources.extend(crawl_sources.products.values())
        for hosts in crawl_sources.sites.values():
            sources.extend(hosts.values())
        for source in sources:
            engine = getattr(source, "engine", None)
            if engine and str(engine).strip():
                engines.add(str(engine).strip().lower())
    if domain_profiles:
        for profile in domain_profiles.values():
            if getattr(profile, "fetch", None) is FetchMode.BROWSER:
                engines.add("browser")
    return engines


def build_scout_crawlers(
    *,
    deploy: DeployConfig | None = None,
    cache: ContentAddressedCache | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
    crawl_sources: CrawlSourcesConfig | None = None,
    run_crawl: CrawlRunConfig | None = None,
    cli_max_depth: int | None = None,
    engine_log: PhaseLogger | None = None,
) -> dict[str, CrawlerPort]:
    """Собирает карту ``engine → CrawlerPort`` для per-engine выбора краулера (L3a).

    Лёгкий ``http`` (:class:`HttpCrawler`) строится **всегда** (дефолтный движок).
    Тяжёлые ``scrapy``/``browser`` строятся **только** для движков, реально упомянутых
    конфигурацией (env :data:`ENV_SCOUT_CRAWLER`, поля ``engine`` в ``crawl_sources``,
    либо ``fetch: browser`` в ``domain_profiles``; см. :func:`_referenced_crawl_engines`).
    Это исключает ленивое поднятие Playwright/Scrapy, когда они не нужны (A1).

    Сборка каждого тяжёлого адаптера обёрнута в guard: недоступность зависимости
    (:class:`~core.ports.crawl_errors.CrawlError`/``CrawlUnavailable``), ``ImportError``
    или ошибка конфигурации (:class:`WiringError`) → движок **опускается**, а не валит всю
    сборку. Отсутствующий ``engine`` в карте — не ошибка: в
    :func:`core.pipeline.corpus.build_corpus` активным остаётся дефолтный ``crawler``.

    :param deploy: типизированный deploy-конфиг; ``None`` → :func:`load_deploy_config`.
    :param cache: глобальный content-addressed кэш (R7); ``None`` → без кэша.
    :param domain_profiles: доменные профили (``domain_profiles.yaml``); ``fetch: browser``
        помечает браузерный движок как запрошенный.
    :param crawl_sources: слои источников краула (``crawl_sources.yaml``); их ``engine``
        помечают соответствующие движки как запрошенные.
    :param run_crawl: ``crawl``-секция ``run.yaml`` (run-слой) — участвует в автодетекте
        discovery для robots (N1).
    :param cli_max_depth: ``--max-depth`` оператора — участвует в автодетекте discovery (N1).
    :param engine_log: лог наблюдаемости (``03`` §D.6): одна строка на движок с источником
        решения (``profile.fetch``/``crawl_sources.engine``/env/дефолт); ``None`` → не пишем.
    """
    config = deploy if deploy is not None else load_deploy_config(None)
    referenced = _referenced_crawl_engines(domain_profiles, crawl_sources)
    robots = _crawler_robots(
        config,
        run_crawl=run_crawl,
        cli_max_depth=cli_max_depth,
        domain_profiles=domain_profiles,
        cache=cache,
    )
    # Отчёт наблюдаемости: источники, запросившие движки (не влияет на сборку).
    uses = _declared_engine_uses(domain_profiles, crawl_sources)
    env_value = (os.environ.get(ENV_SCOUT_CRAWLER) or "").strip().lower()
    default_engine = env_value or DEFAULT_SCOUT_CRAWLER
    default_source = (
        f"env:{ENV_SCOUT_CRAWLER}={env_value}" if env_value else _ENGINE_SOURCE_DEFAULT
    )
    default_entry = _engine_report_entry(uses, default_engine)
    if default_source not in default_entry["sources"]:
        default_entry["sources"].insert(0, default_source)

    build_errors: dict[str, str] = {}
    crawlers: dict[str, CrawlerPort] = {
        "http": HttpCrawler(
            **_http_crawler_kwargs(config), cache=cache, robots=robots
        ),
    }
    if "scrapy" in referenced:
        try:
            crawlers["scrapy"] = ScrapyCrawler(
                cache=cache, max_response_bytes=config.crawl.max_response_bytes
            )
        except (WiringError, ImportError, CrawlError) as exc:
            build_errors["scrapy"] = f"{type(exc).__name__}: {exc}"
    if "browser" in referenced:
        try:
            crawlers["browser"] = BrowserCrawler(
                **_browser_crawler_kwargs(config), cache=cache, robots=robots
            )
        except (WiringError, ImportError, CrawlError) as exc:
            build_errors["browser"] = f"{type(exc).__name__}: {exc}"
    for engine, entry in uses.items():
        entry["built"] = engine in crawlers
        if not entry["built"]:
            entry["unavailable"] = build_errors.get(engine) or "нет доступного адаптера"
    _log_engine_selection(engine_log, uses)
    return crawlers


def _http_crawler_kwargs(deploy: DeployConfig) -> dict[str, Any]:
    """Параметры :class:`HttpCrawler` из ``deploy.crawl`` (дефолты адаптера при ``None``).

    Поля ``user_agent``/``timeout_s`` со значением ``None`` **не** передаются: адаптер
    подставит собственные дефолты (:data:`~core.adapters.crawl_http.DEFAULT_USER_AGENT`/
    ``DEFAULT_TIMEOUT``). ``follow_redirects`` передаётся всегда (дефолт ``True``).
    """
    crawl = deploy.crawl
    kwargs: dict[str, Any] = {"follow_redirects": crawl.follow_redirects}
    if crawl.user_agent is not None:
        kwargs["user_agent"] = crawl.user_agent
    if crawl.timeout_s is not None:
        kwargs["timeout"] = crawl.timeout_s
    # P0/OOM: app-дефолт потолка ответа; per-site/run значение приходит на FetchPlan.
    kwargs["max_response_bytes"] = crawl.max_response_bytes
    return kwargs


def _browser_crawler_kwargs(deploy: DeployConfig) -> dict[str, Any]:
    """Параметры :class:`BrowserCrawler` из ``deploy.crawl`` (дефолты адаптера при ``None``).

    Значения ``None`` в конфиге **не** передаются: адаптер подставит собственные дефолты
    (как в :func:`_http_crawler_kwargs`). Из ``deploy.crawl.browser``:

    * ``headless``/``timeout_ms`` — передаются всегда (у них конкретные дефолты в
      :class:`~core.config.deploy.CrawlBrowserConfig`);
    * ``cdp_url``/``user_data_dir``/``channel`` — только если не ``None``;
    * ``user_agent`` — из ``deploy.crawl.user_agent``, только если не ``None``.

    Декларативный ``browser.mode`` **не** передаётся: это селектор ветки подключения, а
    фактическую ветку (CDP vs persistent-профиль) выводит сам адаптер по ``cdp_url``/
    ``user_data_dir``. Пейсинг (``rate_limiter``) тоже не передаётся — его источник один
    (corpus, полоса P2-3).
    """
    crawl = deploy.crawl
    browser = crawl.browser
    kwargs: dict[str, Any] = {
        "headless": browser.headless,
        "timeout_ms": browser.timeout_ms,
    }
    if browser.cdp_url is not None:
        kwargs["cdp_url"] = browser.cdp_url
    if browser.user_data_dir is not None:
        kwargs["user_data_dir"] = browser.user_data_dir
    if browser.channel is not None:
        kwargs["channel"] = browser.channel
    if crawl.user_agent is not None:
        kwargs["user_agent"] = crawl.user_agent
    return kwargs


def _detect_document_filename(page: RawPage) -> str | None:
    """Имя файла с документным расширением, если формат страницы обрабатывает docling.

    Формат выводится единым классификатором :mod:`core.util.document_formats`: сначала
    суффикс пути URL (percent-декодированный), затем ``Content-Type``. Возврат ``None``
    означает, что страницу обрабатывает HTML-путь (trafilatura) — «нет документного
    сигнала ⇒ HTML». Используется только при маршруте ``AUTO``; ``HTML``/``DOCUMENT``
    задают путь явно (D2).
    """
    suffix = document_suffix(page.url)
    if suffix is None:
        suffix = document_content_type(page.content_type)
    return f"document{suffix}" if suffix is not None else None


def _extract_document(
    documents: DocumentExtractorPort, page: RawPage, filename: str
) -> ExtractedDoc:
    """Документ по пути retained-файла, иначе по байтам страницы (D2/I19).

    ``page.source_path`` задан — тело читает адаптер (``extract_path``), в RAM
    вызывающего его нет; иначе — прежний ``extract_bytes(page.content, filename)``.

    ``documents`` — документный адаптер (docling или PDF-механизм ``text_layer``): правило
    одно для обоих, потому что порт у них общий (``extract_path``/``extract_bytes``), а
    различие механизмов живёт в версии и в тексте, а не в обёртке.
    """
    if page.source_path:
        return documents.extract_path(page.source_path, filename_hint=filename)
    return documents.extract_bytes(page.content, filename)

class _LocalExtractor:
    """Композит локальной экстракции: текстовые форматы, EPUB, документы (docling), HTML.

    Реализует ``ExtractorPort`` структурно. Маршрут ``ExtractionRoute`` (D2) — явный
    приоритет, затем авто-детект формата (единый реестр
    :mod:`core.util.document_formats`):

    1. ``HTML`` — принудительно :class:`HtmlExtractor` (trafilatura);
    2. ``DOCUMENT`` — принудительно :class:`DoclingDocumentExtractor` (docling);
    3. ``AUTO``: текстовое семейство (суффикс, затем ``Content-Type``) →
       :class:`TextExtractor` (plain text/markdown — **без** trafilatura);
    4. EPUB (суффикс ``.epub``/``application/epub+zip``) → :class:`EpubExtractor`
       (stdlib-читатель OCF, главы — через тот же HTML-экстрактор);
    5. документный суффикс/тип → docling;
    6. иначе — HTML.

    Порядок 3/4 перед 5 осознан: текстовые суффиксы и ``.epub`` не входят в реестр
    docling (``DOCUMENT_SUFFIXES``), но их сигнал должен проверяться **до** фолбэка в
    HTML — именно этот фолбэк убивал ``.txt`` в ``parser_failed`` (22.10: 72 файла).
    Суффикс документа сильнее текстового ``Content-Type`` (``report.pdf`` как
    ``text/plain`` остаётся PDF) — правило реестра, не композита.

    Все адаптеры деградируют грациозно (``ExtractedDoc.error``), поэтому композит не
    бросает исключений на уровне извлечения.
    """

    #: B3: композит читает локальные байты (``page.source_path`` → ``extract_path``),
    #: поэтому пригоден для членов контейнеров (спуск идёт в ядре и в ``split``).
    supports_source_path = True

    def __init__(
        self,
        html: ExtractorPort,
        documents: DocumentExtractorPort | None,
        *,
        text: ExtractorPort | None = None,
        epub: ExtractorPort | None = None,
        pdf: DocumentExtractorPort | None = None,
        pdf_required: bool = False,
    ) -> None:
        """``text``/``epub``/``pdf`` — точки подмены (тесты/AB); по умолчанию реальные читатели.

        EPUB по умолчанию переиспользует **тот же** HTML-экстрактор: главы книги чистит
        trafilatura, второй HTML-путь не создаётся.

        ``pdf`` — PDF-механизм (P7): ``None`` — наследовать ``documents`` (дефолт
        ``structured``: PDF идёт в docling, поведение байт-в-байт прежнее). Отдельный
        адаптер (``text_layer``) перехватывает **только** PDF-суффикс, оставляя
        docling'у Office.

        ``pdf_required`` — PDF-механизм выбран явно оператором (``pdf_pipeline``): его
        недоступность больше не «фолбэк в HTML» (тихий переход на другой движок с тем же
        провенансом), а видимый отказ :class:`WiringError`. Дефолт ``False`` — доклинговый
        фолбэк без установленного docling сохраняется как сегодня.
        """
        self._html = html
        self._documents = documents
        self._text = text if text is not None else TextExtractor()
        self._epub = epub if epub is not None else EpubExtractor(html=html)
        self._pdf = pdf if pdf is not None else documents
        self._pdf_required = pdf_required
        html_version = str(getattr(self._html, "version", "") or "")
        # Версия композита: HTML-версия, дополненная документной при наличии docling и
        # PDF-механизмом (P7), если он — отдельный адаптер. Текстовый/EPUB-читатели сюда
        # **не** добавлены: это не ключ документа (его штампует сам адаптер —
        # ``text-plain-2``/``epub-2``), а объявленный стек, где каждый читатель назван
        # отдельно (:func:`core.cli.execution_axes.extractor_stack_version`).
        parts = [html_version]
        if documents is not None:
            parts.append(str(getattr(documents, "version", "")))
        if pdf is not None and pdf is not documents:
            parts.append(str(getattr(pdf, "version", "")))
        self.version = "+".join(part for part in parts if part)

    def _document_backend(
        self, filename: str | None
    ) -> DocumentExtractorPort | None:
        """Механизм для документного файла: PDF → выбранный ``pdf_pipeline``, иначе docling.

        ``None`` — обслуживать нечем (docling не установлен и PDF-механизм не выбран):
        вызывающий уходит в прежний HTML-путь. Для явно выбранного PDF-механизма
        (``pdf_required``) такой исход — :class:`WiringError`, а не фолбэк: иначе прогон
        объявил бы ``pdf_pipeline``, а извлекал другим движком.
        """
        if filename is not None and Path(filename).suffix.lower() == PDF_SUFFIX:
            if self._pdf is not None:
                return self._pdf
            if self._pdf_required:
                raise WiringError(
                    "crawl.pdf_pipeline выбран явно, но PDF-механизм не собран "
                    "(нет доступного адаптера): тихий переход на другой движок "
                    "сломал бы провенанс extractor_version"
                )
            return None
        return self._documents

    def _resolve_target(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> (
        tuple[Literal["html", "text", "epub"], ExtractorPort, None]
        | tuple[Literal["document"], DocumentExtractorPort, str]
    ):
        """Единственный диспетчер маршрута: ``(вид, адаптер, filename для документа)``.

        **Зачем отдельный метод, а не ветки внутри ``extract``.** Маршрут нужен в двух
        местах: чтобы извлечь документ и чтобы **заранее** назвать движок, который это
        сделает (ключ ``extract_cache``). Пока правило жило только внутри ``extract``,
        вызывающий мог лишь догадываться о движке — и догадывался неверно: брал
        композитную версию (``trafilatura-html-6+docling-doc-3:full/…``), тогда как
        документ штампуется версией **своего** адаптера (``trafilatura-html-6`` для
        страницы, ``docling-doc-3:full/…`` для PDF). Кэш от такого ключа не давал ни
        одного попадания: он был write-only. Один диспетчер исключает расхождение по
        построению, а не по дисциплине.

        **P7: PDF — отдельный механизм.** Внутри документной ветви PDF-суффикс уходит в
        выбранный ``pdf_pipeline`` (:meth:`_document_backend`), остальное — в docling.
        Поэтому версия для ключа ``extract_cache`` — версия реально работающего
        адаптера (``pdfium-text-2``, ``docling-doc-3:<level>/…`` для PDF либо
        ``docling-office-1:…`` для DOCX/PPTX/XLSX), а не композита.

        Порядок ветвей — контракт поведения (``.txt``/``.epub`` проверяются до фолбэка в
        HTML, см. докстринг класса): перестановка вернула бы ``.txt`` в ``parser_failed``.
        """
        if route is ExtractionRoute.HTML:
            return "html", self._html, None
        if route is ExtractionRoute.DOCUMENT:
            filename = _detect_document_filename(page)
            # Бессуффиксный принудительный документ — прежде ``document.pdf``; тем же
            # именем решается и механизм, иначе PDF-механизм не увидел бы свой вход.
            backend = self._document_backend(filename or "document.pdf")
            if backend is None:
                return "html", self._html, None
            return "document", backend, filename or "document.pdf"
        if textual_family(page.url, page.content_type) is not None:
            return "text", self._text, None
        if is_epub(page.url, page.content_type):
            return "epub", self._epub, None
        filename = _detect_document_filename(page)
        if filename is not None:
            backend = self._document_backend(filename)
            if backend is not None:
                return "document", backend, filename
        return "html", self._html, None

    def extractor_version_for(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> str:
        """Версия движка, который **будет** обслуживать эту страницу (ключ ``extract_cache``).

        Возвращает ровно ту строку, которую этот адаптер проштампует в
        ``ExtractedDoc.extractor_version``, — поэтому ключ пробы и ключ записи совпадают,
        и кэш попадает. Композитная ``.version`` для этого не годится: она описывает
        набор движков всего композита, а не движок конкретного документа.

        **Версия зависит от формата.** У docling две ветви провенанса: PDF штампуется
        уровнем и устройством (``docling-doc-3:<level>/<device>/…``), офисный документ —
        отдельной строкой без них (``docling-office-1:…``): у DOCX/PPTX/XLSX нет ни
        ``document_level``, ни ``pdf_device``. Поэтому адаптер, умеющий ответить по имени
        файла (:meth:`core.adapters.extract_document.DoclingDocumentExtractor.version_for`),
        спрашивается именно так. Иначе офисный документ получил бы ключ с условиями,
        которых не было, а запись под своей (офисной) версией никогда не находилась бы:
        кэш стал бы write-only — тот же дефект, что был до появления этого метода.

        Без docling (``off`` или пакет недоступен) документная ветвь недостижима, поэтому
        строка всегда принадлежит реально исполняемому адаптеру.
        """
        _, adapter, filename = self._resolve_target(page, route)
        version_for = getattr(adapter, "version_for", None)
        if filename is not None and callable(version_for):
            return str(version_for(filename) or "")
        return str(getattr(adapter, "version", "") or "")

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        """``RawPage`` → ``ExtractedDoc`` по маршруту ``route`` (D2).

        Документный путь: если страница несёт ``source_path`` (retained-файл, I19),
        извлечение идёт по пути (``extract_path``) — тело не копируется в RAM и не
        пишется во второй временный файл; иначе — прежний ``extract_bytes``.

        Диспетчеризация — :meth:`_resolve_target` (единственное место, где живёт правило
        маршрута), чтобы версия для ключа кэша не могла разойтись с фактическим движком.
        """
        kind, adapter, filename = self._resolve_target(page, route)
        if kind == "document":
            # ``adapter`` — документный адаптер по построению ``_resolve_target``: docling
            # (уровень/Office) либо PDF-механизм ``text_layer``. Оба обслуживаются одной
            # обёрткой — протокол ``extract_path``/``extract_bytes`` у них общий.
            # Приведение, а не проверка: ветку решает маршрут, а не форма объекта;
            # ``isinstance`` здесь сделал бы форму вторым решателем правила.
            return _extract_document(
                cast(DocumentExtractorPort, adapter), page, filename or "document.pdf"
            )
        # Обратный инвариант: всё, что не документ, обслуживает ``ExtractorPort``
        # (``_html``/``_text``/``_epub``) — композит маршрутизирует, читатель извлекает.
        return cast(ExtractorPort, adapter).extract(page, route)

    @property
    def options_provenance(self) -> dict[str, int | None]:
        """Провенанс замеренных ручек документной экстракции (R7/§13).

        Делегирует документному адаптеру: HTML-путь таких ручек не имеет, поэтому без
        docling — ``{}`` (значения не выдумываются, A3). Ключ — уровень извлечения,
        значение — ``queue_max_size`` (``None`` — у уровня ручки нет).
        """
        if self._documents is None:
            return {}
        return dict(getattr(self._documents, "options_provenance", {}) or {})


# --------------------------------------------------------------------------- #
# P4: пул экстракции (composition root) — модульные (picklable) воркер/инициализатор
# --------------------------------------------------------------------------- #


def build_worker_extractor(
    level: DocumentLevel = DocumentLevel.FULL,
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED,
    document_timeout: float | None = None,
    pdf_device: PdfDevice = PdfDevice.CPU,
) -> ExtractorPort:
    """Строит экстрактор **внутри** воркер-процесса (P4/P5/P7; picklable by reference, §7).

    Один экстрактор (HTML + текстовые форматы + EPUB + документы при наличии docling) на
    воркер — амортизирует загрузку весов между заданиями. Объекты не пересекают границу
    процессов: фабрика передаётся инициализатору ссылкой, вызов происходит в ребёнке.
    ``level`` (P5) задаёт уровень документной экстракции; ``off`` → docling **не**
    строится вовсе (документы пропускаются pipeline'ом, HTML обычен).

    **P7: ``pdf_pipeline``.** ``structured`` (дефолт) — прежнее поведение байт-в-байт:
    PDF обслуживает docling того же уровня. ``text_layer`` — PDF уходит в
    :class:`~core.adapters.extract_pdf_text.PdfiumDocumentExtractor`, а docling остаётся
    **только для Office** (docx/pptx/xlsx этот механизм не читает; уровень Office —
    прежний, ``pdf_pipeline`` его не касается). Отсутствие ``pypdfium2`` при явно
    выбранном ``text_layer`` — :class:`WiringError` (EXIT_USAGE), а не тихая подмена
    механизма: провенанс объявил бы одно, извлёк другое.

    Композит строится **всегда** (даже без docling и при ``off``): текстовый и EPUB-пути
    не зависят от docling, и возврат «голого» ``HtmlExtractor`` вернул бы ``.txt``/``.epub``
    в trafilatura — ровно тот дефект, из-за которого они умирали в ``parser_failed``.
    ``version``/поведение HTML при этом не меняются: ``_LocalExtractor(html, None).version``
    равен версии HTML-адаптера.

    **P8/F7: ``document_timeout``.** Явно объявленная ось ``crawl.document_timeout_s``
    доезжает до docling; ``None`` — ось не объявлена, и действует прежняя таблица уровней
    (:data:`core.adapters.extract_document._LEVEL_DOCUMENT_TIMEOUT_S`, включая ``full: None``).
    Подстановка дефолта оси вместо ``None`` запрещена: 1800 с — потолок **задачи** фазы,
    а не per-level таймаут конвертера, и подмена изменила бы поведение прогонов, которые
    ось не объявляли.

    **P9: ``pdf_device``.** Объявленное устройство моделей едет и в воркеры, и в
    inline-путь (эта же фабрика — ``extractor_factory`` портов). Оно входит в
    ``version`` экстрактора, то есть в ключ ``extract_cache`` и в провенанс документа.
    ``cuda`` при недоступном устройстве — :class:`WiringError` (EXIT_USAGE), а не тихий
    CPU: тихий фолбэк описал бы прогон устройством, на котором он не исполнялся (R5).
    Штатная проверка — до запуска (:func:`preflight_pdf_device`); здесь она повторяется
    потому, что фабрика исполняется и в воркер-процессе, который мог быть запущен другой
    конфигурацией.
    """
    html = HtmlExtractor()
    pipeline = PdfPipeline(pdf_pipeline)
    device = PdfDevice(pdf_device)
    require_pdf_device(device)
    pdf: DocumentExtractorPort | None = None
    documents: DocumentExtractorPort | None = None
    if level is not DocumentLevel.OFF:
        candidate = DoclingDocumentExtractor(
            level=level, document_timeout=document_timeout, pdf_device=device
        )
        documents = candidate if candidate.available else None
        if pipeline is PdfPipeline.TEXT_LAYER:
            text_layer = PdfiumDocumentExtractor()
            if not text_layer.available:
                raise WiringError(
                    "crawl.pdf_pipeline=text_layer требует pypdfium2: установите extras "
                    "'extract' (`uv pip install -e '.[extract]'`) либо объявите "
                    "pdf_pipeline=structured. Тихая подмена механизма запрещена — "
                    "extractor_version описал бы движок, которого не было"
                )
            pdf = text_layer
        else:
            pdf = documents
    return _LocalExtractor(
        html,
        documents,
        pdf=pdf,
        pdf_required=pipeline is PdfPipeline.TEXT_LAYER,
    )


def _extract_worker_init(
    extractor_factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort],
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED,
) -> None:
    """Инициализатор воркер-процесса пула экстракции (P4/P5/P7/§7): ставит его экстрактор.

    ``extractor_factory`` — module-level (picklable by reference); ``level`` — уровень корня,
    ``pdf_pipeline`` — механизм PDF (C1: пул — фазовый, ключ ``(уровень, механизм, окно)``).
    Глобал экстрактора живёт в ``core.pipeline.extraction_runtime`` (общий воркер
    :func:`core.pipeline.extraction_runtime.extract_worker`); фабрика передаётся и туда —
    защитный lazy-путь, если задание принесёт иную пару (уровень, механизм).
    """
    from core.pipeline.extraction_runtime import set_worker_extractor

    set_worker_extractor(
        extractor_factory(level, pdf_pipeline),
        level=level,
        factory=extractor_factory,
        pdf_pipeline=pdf_pipeline,
    )


def _local_extract_pool_factory(
    max_workers: int,
    level: DocumentLevel = DocumentLevel.FULL,
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED,
    *,
    max_tasks_per_child: int | None = None,
    pdf_device: PdfDevice | str = PdfDevice.CPU,
    extractor_factory: Callable[
        [DocumentLevel, PdfPipeline], ExtractorPort
    ] = build_worker_extractor,
) -> Any:
    """``ProcessPoolExecutor`` (forkserver) для локальной экстракции (P4/P5/P7/§7).

    ``forkserver`` (дефолт Python 3.14 на Linux) не форкает прогретый родитель (torch уже
    загружен в ядре при docling-экстракции) — безопасно; инициализатор строит экстрактор
    **нужного уровня и механизма** на воркер (``level``/``pdf_pipeline`` — свойства корня,
    P5/P7). Прецедент процессов — edge-планировщик (``core/edge/scheduler.py``).

    ``max_tasks_per_child`` (P4/OOM): после N заданий воркер штатно завершается, executor
    поднимает новый процесс и **повторно** зовёт ``initializer`` — экстрактор строится
    заново (``set_worker_extractor``), поэтому рабочий набор тяжёлого конвертера не
    накапливается. ``None`` — прежний долгоживущий воркер (байт-в-байт).

    ``extractor_factory`` — точка подмены для тестов рециклинга (по умолчанию — реальная
    фабрика composition root; никакого docling в тестах рециклинга не требуется).

    **P9: ``pdf_device``.** Устройство — свойство хоста, поэтому оно захватывается в
    ``partial`` фабрики (picklable: модульная функция + StrEnum-значение) и уезжает в
    инициализатор воркера. Оговорка: под ``cuda`` фабрика — уже ``partial``, поэтому
    подмена ``extractor_factory`` обязана принимать ``pdf_device``; тестовая подмена
    работает на дефолтном ``cpu`` и ``partial`` не строится.
    """
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor

    device = PdfDevice(pdf_device)
    if device is PdfDevice.CPU:
        init_factory: Callable[..., ExtractorPort] = extractor_factory
    else:
        # ``partial``, а не замыкание: инициализатор уезжает в другой процесс, и
        # замыкание там не разрешилось бы (pickle by reference). ``base`` — та же
        # фабрика, но с широкой подписью: ``partial`` добавляет kwarg ``pdf_device``,
        # которого в тесной ``Callable[[level, pipeline], …]`` нет.
        base: Callable[..., ExtractorPort] = extractor_factory
        init_factory = partial(base, pdf_device=device)

    return ProcessPoolExecutor(
        max_workers=max_workers,
        mp_context=multiprocessing.get_context("forkserver"),
        initializer=_extract_worker_init,
        initargs=(init_factory, level, pdf_pipeline),
        max_tasks_per_child=max_tasks_per_child,
    )


def build_extraction_pool(
    profile: str,
    *,
    extractor: ExtractorPort | None = None,
    max_tasks_per_child: int | None = None,
    worker_rss_dir: str | Path | None = None,
    pdf_device: PdfDevice | str = PdfDevice.CPU,
) -> ExtractionPool | None:
    """Собирает исполнитель экстракции по профилю развёртывания (P4/§7).

    * ``local`` — процессный пул (``ProcessPoolExecutor``/forkserver); воркер —
      module-level :func:`core.pipeline.extraction_runtime.extract_worker`, экстрактор
      строится инициализатором **в** процессе.
    * ``split`` — потоковый пул (``ThreadPoolExecutor``) поверх переданного
      ``extractor`` (``RemoteExtractor`` I/O-bound); шов (``/v1/*``) не меняется.
    * иначе/без экстрактора — ``None`` (фаза отработает ось ``extract_workers`` inline).

    ``max_tasks_per_child`` (P4/OOM) захватывается замыканием локальной фабрики как
    дефолт исполнения: pipeline резолвит ось per seed-root и, если она задана, передаёт
    значение аргументом (site > run > app). Ось действует только для ``local``: под
    ``split`` рециклинг не поддерживается (``supports_worker_recycling=False``).

    Сам пул создаётся **лениво** в ``build_corpus`` (C1: один на прогон, ключ
    ``(уровень, окно)``, только при ``W>1`` либо при заданном ``max_tasks_per_child``):
    ``None``/``1`` без оси ⇒ ``pool_factory`` не вызывается вовсе (поведение байт-в-байт).

    ``worker_rss_dir`` (F0) — каталог стока пика воркеров
    (``{runs}/{run_id}/workers``): воркер-процесс после задания пишет туда свой
    ``ru_maxrss``, родитель читает максимум (:meth:`ExtractionPool.peak_worker_rss_mb`).
    Для ``split`` остаётся ``None``: воркеры живут на edge, их пик не виден ядру, и
    выдумывать его нельзя.
    """
    from core.pipeline.extraction_runtime import extract_worker, run_extract

    if profile == LOCAL_PROFILE:
        default_recycle = max_tasks_per_child
        # P9: устройство моделей захватывается замыканием — это свойство хоста, а не
        # корня, поэтому per-root значения у него нет (единственная точка объявления —
        # ``deploy.yaml``/``run.yaml:crawl.pdf_device``). ``split`` его игнорирует: там
        # извлечение исполняет edge своим конфигом (R-S), как и ``document_level``.
        device = PdfDevice(pdf_device)

        def _local_factory(
            n: int,
            level: DocumentLevel = DocumentLevel.FULL,
            pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED,
            *,
            max_tasks_per_child: int | None = None,
        ) -> Any:
            # Per-root значение побеждает run-дефолт, захваченный замыканием.
            recycle = (
                default_recycle if max_tasks_per_child is None else max_tasks_per_child
            )
            return _local_extract_pool_factory(
                n,
                level,
                pdf_pipeline,
                max_tasks_per_child=recycle,
                pdf_device=device,
            )

        return ExtractionPool(
            pool_factory=_local_factory,
            worker=extract_worker,
            supports_worker_recycling=True,
            # ``ExtractionPool`` держит каталог строкой (внутри — ``Path(...)``), а
            # фабрика принимает ``str | Path``: приводим здесь, один раз.
            worker_rss_dir=(
                str(worker_rss_dir) if worker_rss_dir is not None else None
            ),
        )
    if profile == SPLIT_PROFILE and extractor is not None:
        def _remote_worker(request: Any) -> Any:
            return run_extract(extractor, request)

        # P5/P9: ось ``document_level``/``pdf_device`` здесь не действует — она уже
        # объявлена на собранном ``RemoteExtractor`` и едет в ``extract/submit``
        # (B3, 07 §8). ``pdf_pipeline`` под ``split`` не действует: механизм PDF
        # выбирает edge (R-S). Обе оси принимаются и игнорируются, чтобы подпись фабрики
        # совпадала с локальной.
        # F0: пик воркера не собирается — воркеры-threads живут в ядре, но их ``ru_maxrss``
        # это RSS всего процесса, а не воркера; подделывать число нельзя (``None``).
        def _thread_factory(n: int, *_: Any, **__: Any) -> Any:
            """Подпись локальной фабрики: оси корня (уровень/механизм) приняты и не действуют."""
            return _thread_pool(n)

        return ExtractionPool(
            pool_factory=_thread_factory,
            worker=_remote_worker,
            supports_worker_recycling=False,
            worker_rss_dir=None,
        )
    return None


def _thread_pool(max_workers: int) -> Any:
    """``ThreadPoolExecutor`` для потокового окна экстракции (split; P4/§7)."""
    from concurrent.futures import ThreadPoolExecutor

    return ThreadPoolExecutor(max_workers=max_workers)


def resolve_extraction_budget(
    profile: str,
    *,
    env: Mapping[str, str] | None = None,
    available_mb: int | None = None,
    injected_ports: bool = False,
) -> tuple[int | None, bool]:
    """Бюджет памяти прогона и флаг «окно экстракции выводится из бюджета» (F0/§3.3).

    Единственная точка чтения бюджета: ``COHOUND_MEM_BUDGET_MB`` > ``MemAvailable`` минус
    резерв (:func:`core.pipeline.resources.resolve_mem_budget_mb`). Дальше число едет в
    пайплайн **явным аргументом** (``mem_budget_mb``) — у чистого пайплайна нет права
    зависеть от окружения, иначе тесты и расписание стали бы функцией памяти хоста.

    Флаг ``derive_extract_workers`` — про то, кто исполняет экстракцию:

    * локальный экстрактор (процессный пул ``local`` или inline-путь в ядре) — ``True``:
      воркеры едят память этого хоста, значит их число выводится из его бюджета;
    * ``split`` — ``False``: воркеры живут на edge, у них собственный бюджет, и число
      воркеров ядра не должно описывать чужую машину.

    :param injected_ports: порты инъецированы извне (тестовый шов ``args.ports``), то есть
        работает **не** тот стек экстракции, чьи пики измерены (:data:`DOCLING_PEAK_MB`).
        Тогда вывод окна выключен: бюджет описывал бы docling, а исполнялся бы другой
        экстрактор — это было бы предвидение вместо замера (A3). Явно объявленная ось
        ``crawl.extract_workers`` работает и здесь.

    :returns: ``(budget_mb | None, derive_extract_workers)``.
    """
    derive = profile not in SPLIT_PROFILES and not injected_ports
    return resolve_mem_budget_mb(env=env, available_mb=available_mb), derive


def require_pdf_device(pdf_device: PdfDevice | str = PdfDevice.CPU) -> None:
    """``cuda`` без CUDA-устройства — отказ с числами, а не тихий CPU-фолбэк (P9/R5).

    Ось ``crawl.pdf_device`` — **объявленный** вход: прогон, объявивший ``cuda``,
    обязан исполниться на GPU либо не начаться. Тихая подстановка ``cpu`` была бы
    ложным провенансом (``extractor_version``/``extractor_stack`` назвали бы устройство,
    которого не было) — тот же класс, что чтение модели оценщика из env (§7.4).

    Сообщение несёт факты, по которым оператор может починить вход, а не «недоступно»:
    версию установленного ``torch`` (из метаданных дистрибутива — без импорта: импорт
    torch стоит сотни MB и был бы нужен только для отказа), факт ``cuda_available`` и
    объём карты (``None`` — устройства/драйвера нет).

    ``cpu`` проходит всегда: ось не требует устройства, а её отсутствие — штатный режим.
    """
    device = PdfDevice(pdf_device)
    if device is not PdfDevice.CUDA:
        return
    if cuda_available():
        return
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _dist_version

    try:
        torch_version = _dist_version("torch")
    except PackageNotFoundError:  # pragma: no cover — окружение без torch
        torch_version = "не установлен"
    total = cuda_total_vram_mb()
    raise WiringError(
        "crawl.pdf_device=cuda, но CUDA-устройство недоступно: "
        f"torch={torch_version}, cuda_available=False, "
        f"device_vram_mb={total if total is not None else 'нет устройства'}. "
        "Объявите crawl.pdf_device=cpu либо установите CUDA-колёса и драйвер "
        "(индекс cu130: `uv sync --frozen --extra extract-pdf`). Тихая подмена "
        "устройства запрещена: extractor_version/extractor_stack назвали бы GPU, "
        "на котором прогон не исполнялся"
    )


def require_embed_device(device: EmbedDevice | str = EmbedDevice.CPU) -> None:
    """``embed.device=cuda`` без CUDA-устройства — отказ с числами, а не тихий CPU.

    Зеркало :func:`require_pdf_device` для оси эмбеддера. Ось объявлена входом, значит
    прогон, объявивший ``cuda``, обязан посчитать векторы на GPU либо не начаться:
    тихая подстановка ``cpu`` дала бы CPU-векторы под ``run_id``, который объявляет
    ``cuda``, — ровно тот класс дефекта, ради которого ось вводится (``16`` §4.3).

    Сообщение называет ось ``embed.device`` и несёт факты для починки входа: версию
    установленного ``torch`` (из метаданных дистрибутива — без импорта, импорт стоил бы
    сотни MB ради одного отказа), ``cuda_available`` и объём карты (``None`` — устройства
    или драйвера нет). ``cpu`` проходит всегда: устройство не требуется.

    :raises WiringError: ``cuda`` при недоступном CUDA-устройстве (``EXIT_USAGE``).
    """
    resolved = EmbedDevice(device)
    if resolved is not EmbedDevice.CUDA:
        return
    if cuda_available():
        return
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _dist_version

    try:
        torch_version = _dist_version("torch")
    except PackageNotFoundError:  # pragma: no cover — окружение без torch
        torch_version = "не установлен"
    total = cuda_total_vram_mb()
    raise WiringError(
        "embed.device=cuda, но CUDA-устройство недоступно: "
        f"torch={torch_version}, cuda_available=False, "
        f"device_vram_mb={total if total is not None else 'нет устройства'}. "
        "Объявите embed.device=cpu либо установите CUDA-колёса и драйвер "
        "(индекс cu130: `uv sync --frozen --extra embed`). Тихая подмена устройства "
        "запрещена: embed_device/партиция/ключ embed_cache назвали бы GPU, на котором "
        "прогон не исполнялся"
    )


def preflight_pdf_device(
    *,
    deploy: DeployConfig | None = None,
    run_crawl: CrawlRunConfig | None = None,
) -> PdfDevice:
    """Резолвит объявленное устройство и **проверяет его исполнимость** до прогона.

    Одна точка входа для composition root: резолв — тот же
    :func:`core.pipeline.crawl_frontier.resolve_pdf_device`, что увидит фаза корпуса
    (``app < run``), поэтому отказ срабатывает ровно на том значении, которое
    исполнилось бы, а не на копии формулы.

    :raises WiringError: ``cuda`` при недоступном устройстве (``EXIT_USAGE`` до старта).
    """
    device = resolve_pdf_device(
        deploy.crawl if deploy is not None else CrawlConfig(), run_crawl
    )
    require_pdf_device(device)
    return device


def preflight_extraction_budget(
    *,
    roots: Iterable[str],
    deploy: DeployConfig | None,
    run_crawl: CrawlRunConfig | None,
    profiles: Mapping[str, DomainProfile],
    mem_budget_mb: int | None,
    derive_extract_workers: bool,
) -> tuple[MemoryBudgetError | None, tuple[str, ...]]:
    """Preflight §3.3: отказ начинать заведомо невозможную конфигурацию экстракции.

    Число воркеров выводится по объявленному бюджету
    (``floor((budget − parent_floor) / peak(level))``). **0 — валидный ответ**, и именно
    здесь он превращается в отказ ``EXIT_USAGE``: прогон, который заведомо не помещается,
    падает на первом документе (OOM-killer'ом, без трейсбека), а не на входе с числами.
    Это **не** потолок на работу: если воркеры помещаются, никакого лимита не вводится.

    Резолв — тот же :func:`core.pipeline.crawl_frontier.resolve_crawl_limits`, что и в
    фазе корпуса (``app < run < site``), поэтому уровень и окно берутся **разрешённые**, а
    не дефолтные: CLI-уточнение уровня уже лежит в ``run_crawl`` того же вида, что увидит
    фаза. Отказ срабатывает только когда окно **выведено** (``extract_workers_derived``):
    явно объявленная ось ``crawl.extract_workers`` на любом слое сильнее бюджета — тогда
    возвращается предупреждение с числами, а не отказ (воля оператора).

    :returns: ``(error | None, warnings)`` — ``error`` несёт числа
        (:meth:`MemoryBudgetError.describe`), ``warnings`` — тексты для ``errors``/логов.
    """
    error: MemoryBudgetError | None = None
    warnings: list[str] = []
    seen: set[tuple[int, str]] = set()
    seen_vram: set[tuple[str, str]] = set()
    for root in roots:
        try:
            limits = resolve_crawl_limits(
                str(root),
                app=(deploy.crawl if deploy is not None else CrawlConfig()),
                run=run_crawl,
                profiles=profiles,
                derive_extract_workers=derive_extract_workers,
                mem_budget_mb=mem_budget_mb,
            )
        except Exception as exc:  # noqa: BLE001 — отказ резолва осей не роняет preflight
            # Тишина здесь была бы потерей: непроверенный корень = конфигурация, которую
            # preflight «прошёл», не увидев. Отказ становится видимым предупреждением с
            # именем корня и первой строкой ошибки, затем проверка идёт по остальным.
            detail = (str(exc).splitlines() or [""])[0]
            warnings.append(
                f"crawl.extract_workers: оси корня {root} не разрешены "
                f"({type(exc).__name__}: {detail}) — бюджет для него не проверен"
            )
            continue
        level = limits.document_level
        pdf_pipeline = limits.pdf_pipeline
        if level is DocumentLevel.OFF and pdf_pipeline is not PdfPipeline.TEXT_LAYER:
            # Документной экстракции нет — воркеры не нужны; ноль здесь не «не помещается».
            continue
        # P9: окно под ``cuda`` ограничено ещё и видеопамятью — вместимость считается
        # тем же вызовом, что и в фазе корпуса (одна формула, не вторая копия).
        capacity = workers_from_budget(
            level,
            mem_budget_mb,
            pdf_pipeline=pdf_pipeline,
            pdf_device=limits.pdf_device,
        )
        # Неизмеренный VRAM-пик уровня — не «ноль» и не повод выдумать число: говорим
        # прямо, чем ограничено окно, чтобы отсутствие границы не выглядело гарантией.
        if (
            limits.pdf_device is PdfDevice.CUDA
            and vram_peak_mb(level, pdf_pipeline) is None
            and (level.value, pdf_pipeline.value) not in seen_vram
        ):
            seen_vram.add((level.value, pdf_pipeline.value))
            warnings.append(
                f"crawl.pdf_device=cuda: пик VRAM уровня {level.value} "
                f"({pdf_pipeline.value}) не измерен — окно ограничено памятью хоста и "
                f"CPU; VRAM-слагаемое не применяется (замер, а не предвидение: A3)"
            )
        if limits.extract_workers_derived:
            if limits.extract_workers == 0 and error is None:
                error = MemoryBudgetError(
                    budget_mb=mem_budget_mb,
                    level=level,
                    peak_mb=pdf_peak_mb(level, pdf_pipeline),
                    parent_floor_mb=PARENT_FLOOR_MB,
                    workers=0,
                )
            continue
        # Ось объявлена явно (app/run/site) — оператор сильнее бюджета, но расхождение
        # обязано быть видимым: иначе OOM выглядит как дефект прогона, а не как вход.
        if limits.extract_workers is None:
            continue
        key = (int(limits.extract_workers), level.value)
        if capacity >= int(limits.extract_workers) or key in seen:
            continue
        seen.add(key)
        warnings.append(
            "crawl.extract_workers: "
            f"{int(limits.extract_workers)} объявлено явно (app<run<site), но бюджет "
            f"{mem_budget_mb} MB и число CPU помещают {capacity} для level={level.value} "
            f"(peak={pdf_peak_mb(level, pdf_pipeline)} MB, "
            f"parent floor={PARENT_FLOOR_MB} MB) — "
            f"исполнение по воле оператора, бюджет ({MEM_BUDGET_ENV}) не учитывается"
        )
    return error, tuple(warnings)



def _build_local_ports(
    *,
    evaluator: EvaluatorPort | None,
    registry_fetchers: Mapping[str, RegistryFetcherPort] | None = None,
    registry_spec_digest: str | None = None,
    index_root: str | Path,
    package: str | None,
    crawler: CrawlerPort | None = None,
    crawlers: Mapping[str, CrawlerPort] | None = None,
    pollers: Mapping[str, PollPort] | None = None,
    store: StorePort | None = None,
    chunk_lookup: ChunkLookup | None = None,
    index_build_id: str | None = None,
    embedder: EmbedderPort | None = None,
    vector_index: VectorIndexPort | None = None,
    lexical_index: LexicalIndexPort | None = None,
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED,
    document_timeout: float | None = None,
    pdf_device: PdfDevice = PdfDevice.CPU,
    embed_device: EmbedDevice | str = EmbedDevice.CPU,
    embed_precision: EmbedPrecision | str = EmbedPrecision.FP32,
    embed_notices: list[str] | None = None,
) -> Ports:
    """Собирает in-process порты профиля ``local`` (без сети/тяжёлых зависимостей).

    Гарантия: никогда не бросает из-за отсутствия опциональной зависимости — недоступный
    тяжёлый адаптер остаётся ``None``. ``evaluator`` не создаётся сам:
    передаётся вызывающим (генератор/конфиг; I6/I7/I18 не автоматизируются).
    ``registry_fetchers``/``registry_spec_digest`` (E2) — готовые mapping фетчеров
    (``registry_id → RegistryFetcherPort``) и дайджест их спецификаций из composition root.
    ``crawler`` — опциональный HTTP-краулер корпуса, собранный composition root из
    ``deploy.crawl`` (:func:`build_scout_crawler`); ``None`` → не инстанцируется (прежнее
    поведение, сетевые адаптеры включаются явной инъекцией).
    ``pollers`` — карта ``kind → PollPort`` (Phase 5.1/D3: ``LocalFeedPoller`` для
    ``feed``/``channel``/``sitemap``), собранная из того же краулера; ``None`` → опрос выключен.

    **Эмбеддер (P4a).** Предпочитается оффлайн-путь ``LiteEmbedder`` (``model2vec``, без
    ``torch``), если он готов (зависимость + локальный снапшот модели); иначе — качественный
    ``LocalEmbedder`` (bge-m3) при наличии ``sentence-transformers``; иначе ``None``.
    Выдуманных векторов/подмены модели нет: недоступность — явный ``None`` (A3).

    **Устройство эмбеддера (``embed.device``).** ``embed_device`` — объявленная ось того же
    резолва ``app < run``, что у исполнения. ``cpu`` сохраняет прежний приоритет; ``cuda``
    исключает ``LiteEmbedder`` (NumPy/CPU by design) с видимой заметкой в ``embed_notices``
    и требует ``LocalEmbedder`` — недоступность стека отказ, а не CPU-фолбэк
    (:func:`_resolve_embedder`).

    **Точность эмбеддера (``embed.precision``).** ``embed_precision`` — вторая объявленная
    ось того же резолва. ``fp16`` исполняется ``torch.autocast``, поэтому тоже исключает
    ``LiteEmbedder`` и требует torch-путь; ``fp32`` — прежний приоритет. Точность, которую
    выбранный эмбеддер не исполнит, — отказ, а не тихий fp32 (:func:`_resolve_embedder`).

    **Dense- и лексическое плечи (P4a, I17).** ``vector_index``/``lexical_index`` строятся
    :func:`build_vector_index`/:func:`seed_lexical_index` только при наличии предпосылок
    (``numpy``/``tantivy``, ``chunk_lookup``/``store`` и разрешённая зафиксированная сборка
    ``index_build_id``). Отсутствие предпосылки — явный ``None`` (N3): эксперимент фиксирует
    это как ``skipped``, а не как молчаливую дыру или выдуманный индекс. Явно
    инъецированные порты имеют приоритет (ручной DI, A1).

    **PDF-механизм (P7).** ``pdf_pipeline`` выбирает адаптер PDF в композите:
    ``structured`` (дефолт) — прежний docling, ``text_layer`` — читатель текстового слоя.
    Недоступный явно выбранный механизм — :class:`WiringError` (EXIT_USAGE до прогона), а
    не фолбэк: ``extractor_version`` обязан описывать движок, который реально работал.

    **Таймаут документа (P8/F7).** ``document_timeout`` — значение явно объявленной оси
    ``crawl.document_timeout_s`` (``None`` — ось не объявлена: docling берёт per-level
    значение своей таблицы). Раньше ось не доезжала до конвертера вовсе: адаптер имел
    параметр, а конструктор его не передавал, поэтому объявленный потолок документа не
    действовал.

    **Устройство (P9).** ``pdf_device`` — объявленная ось ``crawl.pdf_device``: она
    доезжает до документарного адаптера (и в его ``version``), поэтому пробный
    экстрактор портов и Ш5 исполняются на том же устройстве, что объявлено. ``cuda``
    проверяется здесь же (:func:`require_pdf_device`): недоступное устройство — отказ
    до прогона, а не тихий CPU.
    """
    html = HtmlExtractor()
    require_pdf_device(pdf_device)
    documents = DoclingDocumentExtractor(
        document_timeout=document_timeout, pdf_device=pdf_device
    )
    # P7: PDF-механизм — часть композита, а не «настройка внутри docling». ``structured``
    # (дефолт) — тот же docling: поведение и ``version`` байт-в-байт прежние.
    pdf_adapter: DocumentExtractorPort | None = None
    if PdfPipeline(pdf_pipeline) is PdfPipeline.TEXT_LAYER:
        text_layer = PdfiumDocumentExtractor()
        if not text_layer.available:
            raise WiringError(
                "crawl.pdf_pipeline=text_layer требует pypdfium2: установите extras "
                "'extract' (`uv pip install -e '.[extract]'`) либо объявите "
                "pdf_pipeline=structured. Тихая подмена механизма запрещена — "
                "extractor_version описал бы движок, которого не было"
            )
        pdf_adapter = text_layer
    elif documents.available:
        pdf_adapter = documents
    # Композит строится всегда: без docling текстовые форматы и EPUB всё равно имеют
    # своих читателей, а документные суффиксы уходят в HTML-путь (как раньше —
    # ``_LocalExtractor(html, None)``).
    extractor: ExtractorPort = _LocalExtractor(
        html,
        documents if documents.available else None,
        pdf=pdf_adapter,
        pdf_required=PdfPipeline(pdf_pipeline) is PdfPipeline.TEXT_LAYER,
    )

    # Тяжёлые опциональные адаптеры: только при выполненной availability-предикате.
    # Устройство эмбеддера — **объявленная** ось (``embed.device``), а не свойство
    # адаптера: спрашивать у эмбеддера «на чём ты считаешь» и есть дефект ``14`` оговорки 5.
    resolved_embedder = (
        embedder
        if embedder is not None
        else _resolve_embedder(
            embed_device, precision=embed_precision, notices=embed_notices
        )
    )
    index_builder = (
        NpyIndexBuilder(
            index_root=index_root,
            package=package,
            # §13:676/09 §8.5 — версия лексического движка входит в ``pb`` и в
            # манифест партиции; передаём фактический движок этого прогона, а не
            # дефолт (иначе две сборки с разными движками неотличимы по id).
            lexical_engine_version=_lexical_engine_version(),
        )
        if numpy_available()
        else None
    )
    resolved_vector_index = (
        vector_index
        if vector_index is not None
        else build_vector_index(
            index_root=index_root,
            package=package,
            chunk_lookup=chunk_lookup,
            build_id=index_build_id,
        )
    )
    resolved_lexical_index = (
        lexical_index
        if lexical_index is not None
        else seed_lexical_index(
            index_root=index_root,
            package=package,
            store=store,
            build_id=index_build_id,
        )
    )

    return Ports(
        evaluator=evaluator,
        # E2: mapping реестровых фетчеров (registry_id → fetcher) из package-scoped
        # спецификаций + дайджест спецификаций (провенанс манифеста, §13).
        registry_fetchers=registry_fetchers,
        registry_spec_digest=registry_spec_digest,
        chunker=FamilyChunker(),
        extractor=extractor,
        embedder=resolved_embedder,
        index_builder=index_builder,
        # Adapter/port mismatch (engine_version: property vs mutable attr) — статическая
        # несовместимость протокола; runtime_checkable проверяет методы, поведение корректно.
        lexical_index=resolved_lexical_index,
        # Краулер корпуса: инстанцируется composition root'ом из ``deploy.crawl`` (R7/03
        # §D.5); без конфига остаётся ``None`` — прежнее поведение. SERP/discovery для
        # local включаются явной инъекцией портов (в CLI-конфиге нет запросов).
        crawler=crawler,
        # Lane A/L3a: карта ``engine → CrawlerPort`` для per-engine выбора в Ш5.
        crawlers=crawlers,
        # Phase 5.1: карта ``kind → PollPort`` для опроса фидов/каналов в Ш5.
        pollers=pollers,
        # B3: спуск в контейнеры — читатель по голове файла (stdlib zip / py7zr / rarfile).
        # Это не веб-эгресс: листинг и стриминг членов читают **локальный** ретейленный
        # файл, поэтому и в ``split`` спуск исполняется в ядре (архив живёт в corpus_raw).
        container=container_for,
        search=None,
        vector_index=resolved_vector_index,
    )


def _resolve_embedder(
    device: EmbedDevice | str = EmbedDevice.CPU,
    *,
    precision: EmbedPrecision | str = EmbedPrecision.FP32,
    notices: list[str] | None = None,
) -> EmbedderPort | None:
    """Выбирает эмбеддер по **объявленным** устройству и точности (P4a + ``embed.*``).

    Таблица ``16`` §4.3 — минимальная, backend остаётся недекларированным:

    * ``cpu`` — как сегодня: ``LiteEmbedder`` (``model2vec``, NumPy/CPU без ``torch``) →
      ``LocalEmbedder`` (bge-m3, ``sentence-transformers``) → ``None``. Обе реализации
      CPU-честны, поэтому приоритет не меняется;
    * ``cuda`` — ``LiteEmbedder`` **исключается** с видимой заметкой (``model2vec``
      NumPy/CPU by design и ось исполнить не может), берётся ``LocalEmbedder(device="cuda")``;
      недоступен стек — :class:`WiringError` (``EXIT_USAGE``), а не CPU-векторы.

    Тихого фолбэка нет ни в одну сторону: прогон, объявивший ``cuda``, не должен получить
    CPU-векторы под тем же ``run_id``.

    **Точность (``embed.precision``, решение владельца 06.11.2026).** ``fp32`` — прежний
    путь. ``fp16`` исполняется ``torch.autocast(device_type=<устройство>,
    dtype=float16)`` вокруг вызова модели (:class:`~core.adapters.embed_local.LocalEmbedder`),
    поэтому эмбеддер без torch (``LiteEmbedder``/``model2vec`` — NumPy/CPU by design)
    точность исполнить **не может** и исключается с видимой заметкой; недоступен
    torch-путь — :class:`WiringError`. Тихий fp32 запрещён: объявленная точность вошла бы
    в ``run_id``/``pb``/ключ ``embed_cache``, а считалась бы другая — ложный провенанс.

    Замер, на котором стоит ``fp16`` (``2026-11-06-embedding-precision-cpu.md``, cuda,
    bge-m3): 15.40/13.82 чанка/с против 6.02/4.99 у fp32, худший чанк ``1−cos = 8.3e-05``,
    top-10 0.998, детерминизм побитовый. Механизм на cpu исполним (torch.autocast), но
    собственных чисел у него нет — прогон их и публикует.

    :param device: объявленное устройство (:class:`EmbedDevice` или строка).
    :param precision: объявленная точность (:class:`EmbedPrecision` или строка).
    :param notices: список для видимых замечаний выбора (пишутся в журнал wiring).
    :raises WiringError: ``cuda`` без ``sentence-transformers``/``LocalEmbedder``;
        ``fp16`` без torch-пути (``sentence-transformers``).
    """
    resolved = EmbedDevice(device)
    resolved_precision = EmbedPrecision(precision)
    if resolved_precision is not EmbedPrecision.FP32:
        # Точность исполняется только через ``torch.autocast``: эмбеддер без torch её не
        # исполнит. Исключение видимое (журнал), а отсутствие пути — отказ, не fp32.
        if notices is not None and model2vec_available():
            notices.append(
                f"embed.precision={resolved_precision.value}: LiteEmbedder исключён — он "
                "NumPy/CPU by design и объявленную точность исполнить не может; выбран "
                "LocalEmbedder (bge-m3) с torch.autocast"
            )
        if not _module_installed("sentence_transformers"):
            raise WiringError(
                "embed.precision="
                f"{resolved_precision.value} требует sentence-transformers "
                "(LocalEmbedder + torch.autocast): установите extra 'embed' "
                "(`uv sync --frozen --extra embed`) либо объявите "
                "embed.precision=fp32. Тихий fp32 запрещён — прогон объявил точность, "
                "которая вошла бы в embed_precision/партицию/ключ embed_cache"
            )
        return LocalEmbedder(device=resolved.value, precision=resolved_precision.value)
    if resolved is EmbedDevice.CUDA:
        if notices is not None and model2vec_available():
            notices.append(
                "embed.device=cuda: LiteEmbedder исключён — он NumPy/CPU by design и ось "
                "исполнить не может; выбран LocalEmbedder (bge-m3) на cuda"
            )
        if not _module_installed("sentence_transformers"):
            raise WiringError(
                "embed.device=cuda требует sentence-transformers (LocalEmbedder): "
                "установите extra 'embed' (`uv pip install -e '.[embed]'`) либо объявите "
                "embed.device=cpu. Тихий фолбэк на CPU запрещён — прогон объявил cuda, "
                "и CPU-векторы разошлись бы с embed_device/партицией/ключом embed_cache"
            )
        return LocalEmbedder(device=resolved.value, precision=resolved_precision.value)
    if model2vec_available():
        try:
            lite = build_lite_embedder()
        except Exception:  # noqa: BLE001 — сборка портов не бросает (грациозная деградация)
            lite = None
        if lite is not None and lite.available:
            return lite
    if _module_installed("sentence_transformers"):
        return LocalEmbedder(device=resolved.value, precision=resolved_precision.value)
    # Нет ни одной ветви — это **видимое** состояние, а не молчаливая дыра: без эмбеддера
    # композиция не собирается, ``index_manifest``/``index_store`` не пишутся (реестр
    # утверждал бы «партиция на диске» без коммита), ``embed_model_id`` пуст, а R0
    # работает лексически. Пишется в журнал wiring; ``None`` сам по себе молчал бы.
    if notices is not None:
        notices.append(
            "эмбеддер не найден (ни model2vec, ни sentence-transformers): dense-ветвь не "
            "собрана — композиция и index_manifest не пишутся, embed_model_id пуст, R0 "
            "работает лексически; dense-оценка недоступна (`uv sync --extra embed-lite` "
            "или --extra embed)"
        )
    return None


def registry_specs_digest(specs: Mapping[str, RegistryConfig]) -> str | None:
    """Стабильный дайджест загруженных package-scoped спецификаций реестров (E2/§13).

    Канонические байты спецификации — детерминированная JSON-сериализация валидированного
    :class:`RegistryConfig` (``sort_keys=True``): смена **эффективного** содержания меняет
    дайджест, а комментарии/форматирование YAML — нет (не влияют на исполнение). Спецификации
    сортируются по ``registry_id`` (порядок не влияет). Пустой набор → ``None`` (реестры не
    сконфигурированы: поле манифеста не заполняется, нетронутые прогоны не меняются).
    """
    if not specs:
        return None
    parts = [
        f"{registry_id}\x1f"
        f"{json.dumps(specs[registry_id].model_dump(mode='json'), sort_keys=True, ensure_ascii=False)}"
        for registry_id in sorted(specs)
    ]
    return short_hash("\x1e".join(parts), 24)


class _RegistryHttpClient:
    """Минимальный HTTP-клиент реестрового адаптера (ленивый ``httpx``).

    Форма ``get(url, headers, timeout) -> bytes`` ожидается
    :class:`~core.adapters.registry_generic.GenericRegistryFetcher`. Сеть не хардкодится:
    composition root/тесты могут подменить клиент (шов ``registry_http_get``).
    """

    def get(
        self, url: str, headers: Mapping[str, str], timeout: float | None = None
    ) -> bytes:
        """GET реестрового URL с похоповой валидацией редиректов (R10).

        ``follow_redirects=False`` у каждого запроса: цикл ведёт
        :func:`core.util.redirects.follow_validated_redirects`, и каждый ``Location``
        проходит ``validate_url`` **до** выпуска запроса. Прежде клиент полагался на
        ``follow_redirects=True``: реестровая страница (внешний, недоверенный ответ) могла
        увести запрос на ``127.0.0.1``/``10.0.0.1``/metadata-эндпоинт, и SSRF-проверка
        видела бы уже готовый ответ. Отказ хопа — :class:`RegistryFetchError` с причиной
        «редирект на небезопасный адрес», чтобы вызывающий отличал SSRF-отказ от
        недоступности источника по тексту и типу.
        """
        httpx = self._import_http()

        def _fetch_hop(current: str) -> Any:
            return httpx.get(
                current,
                headers=dict(headers),
                timeout=timeout,
                follow_redirects=False,
            )

        try:
            response, _final_url = follow_validated_redirects(
                _fetch_hop,
                url,
                error=RegistryFetchError,
                label="HTTP-запрос к реестру",
            )
            response.raise_for_status()
        except RegistryFetchError:
            raise
        except Exception as exc:  # noqa: BLE001 — единая граница ошибки реестра
            raise RegistryFetchError(f"HTTP-запрос к реестру не удался: {url}") from exc
        return bytes(response.content)

    @staticmethod
    def _import_http() -> Any:
        """Лениво импортирует ``httpx``; при отсутствии — явная ошибка реестра."""
        if importlib.util.find_spec("httpx") is None:
            raise RegistryFetchError(
                "httpx не установлен: реальный HTTP-путь реестра недоступен"
            )
        import httpx

        return httpx


def _build_local_registry_fetchers(
    specs: Mapping[str, RegistryConfig],
    http_get: Any,
) -> dict[str, RegistryFetcherPort]:
    """Собирает ``{registry_id: GenericRegistryFetcher}`` (профиль ``local``).

    Единственное место конструирования локальных реестровых фетчеров (composition root Z0).
    Порядок ключей детерминирован (сортировка по ``registry_id``).
    """
    return {
        registry_id: GenericRegistryFetcher(specs[registry_id], http_get)
        for registry_id in sorted(specs)
    }


def build_adapters(
    *,
    profile: str = LOCAL_PROFILE,
    deploy: DeployConfig | None = None,
    transport: TransportFn | None = None,
    gateway: ContentGateway | None = None,
    extra_headers: Mapping[str, str] | None = None,
    evaluator: EvaluatorPort | None = None,
    registry_specs: Mapping[str, RegistryConfig] | None = None,
    registry_http_get: Any | None = None,
    index_root: str | Path = "index_store",
    package: str | None = None,
    cache: ContentAddressedCache | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
    crawl_sources: CrawlSourcesConfig | None = None,
    store: StorePort | None = None,
    chunk_lookup: ChunkLookup | None = None,
    index_build_id: str | None = None,
    vector_index: VectorIndexPort | None = None,
    lexical_index: LexicalIndexPort | None = None,
    embedder: EmbedderPort | None = None,
    run_crawl: CrawlRunConfig | None = None,
    run_embed: EmbedRunConfig | None = None,
    cli_max_depth: int | None = None,
    pdf_pipeline: PdfPipeline | None = None,
    engine_log: PhaseLogger | None = None,
) -> Ports:
    """Собирает порты внешних систем по профилю развёртывания (``03`` §E.3).

    * ``local`` — реальные адаптеры in-process: чанкер и экстрактор всегда, тяжёлые
      опциональные (эмбеддер/индекс/лексический индекс) — только при доступной
      зависимости, иначе ``None``. Сетевые адаптеры не собираются (см. комментарий в
      :func:`_build_local_ports`).
    * ``split`` — remote-прокси поверх :class:`HttpContentGateway` (``07`` §4): краулер,
      поиск, экстрактор и реестровый фетчер маршрутизируются в DMZ. Конфиг берётся из
      ``deploy`` или загружается из ``config/deploy.yaml``; профиль конфига обязан быть
      ``split``/``split-hardened``.

    :param deploy: типизированный deploy-конфиг; ``None`` → :func:`load_deploy_config`.
    :param transport: инъектируемый шов gateway (тесты без сокета).
    :param gateway: готовый ``ContentGateway`` (переопределяет сборку по конфигу).
    :param extra_headers: доп. заголовки шва (напр. OIDC bearer).
    :param evaluator: готовый оценщик (``local``); ``None`` → остаётся ``None`` (не
        конструируется автоматически: сценарий evaluator требует OI-совместимого endpoint
        корпоративного облака, см. :func:`build_evaluator_llm`).
    :param registry_specs: загруженные package-scoped спецификации реестров
        (``{registry_id: RegistryConfig}``, 03 §D.4) — из них собирается **mapping**
        ``Ports.registry_fetchers`` (``GenericRegistryFetcher`` для ``local``,
        ``RemoteRegistry`` для ``split``) и дайджест спецификаций. ``None``/пусто → реестры
        не сконфигурированы. Пустой реестр без спеки — не «молчаливая дыра»: загрузку и её
        ошибки поднимает composition root (``_resolve_ports``).
    :param registry_http_get: шов HTTP-клиента реестра (``local``; тесты без сети); ``None`` →
        реальный ленивый ``httpx`` (:class:`_RegistryHttpClient`).
    :param index_root: корень индексного хранилища для ``NpyIndexBuilder`` (``local``);
        composition root передаёт resolved ``paths.index_store`` (``03`` §A.1); дефолт
        ``"index_store"`` совпадает с дефолтом :class:`~core.config.paths.PathsConfig`.
    :param package: пакет по умолчанию для ``NpyIndexBuilder`` (``local``).
    :param cache: глобальный content-addressed кэш (R7, ADR 0016); протягивается в
        локальный краулер корпуса (``local``).
    :param domain_profiles: типизированные доменные профили (``domain_profiles.yaml``,
        ``03`` §D.2); используются локальным краулером для fail-fast проверки ``fetch: browser``
        (план резолвится в pipeline).
    :param run_crawl: ``crawl``-секция ``run.yaml`` (run-слой осей краула) — для автодетекта
        discovery при включении принуждения ``robots.txt`` (N1).
    :param run_embed: ``embed``-секция ``run.yaml`` (run-слой оси ``embed.device``) — резолв
        ``app < run`` тот же, что у исполнения
        (:func:`core.pipeline.crawl_frontier.resolve_embed_device`). ``None`` — run-слой не
        задан, действует ``deploy.embed.device`` (по умолчанию ``cpu``). Устройство не
        читается из окружения: иначе ``run_id`` перестал бы описывать место счёта векторов.
    :param cli_max_depth: ``--max-depth`` оператора — также участвует в автодетекте
        discovery (N1).
    :param pdf_pipeline: явный механизм PDF (операторский ``--pdf-pipeline``); ``None`` —
        резолв цепочкой app < run < site. Явное значение сильнее цепочки: ось объявляет
        механизм этого прогона, и пробный экстрактор портов обязан мерить тот же движок,
        что и корпус (тихая подмена механизма запрещена). Под ``split`` ось не действует:
        экстрактор — remote, конвертеры строит edge (R-S).
    :param engine_log: лог наблюдаемости выбора движка краула (``03`` §D.6); ``None`` →
        строки не пишутся (тесты/вызовы без прогона).
    :param crawl_sources: слои источников краула (``crawl_sources.yaml``, Lane A); их
        поля ``engine`` определяют, какие per-engine краулеры строит
        :func:`build_scout_crawlers` (ленивая сборка тяжёлых адаптеров).
    :param store: операционное хранилище прогона (``local``); ``None`` → недоступно. Нужно
        для засева лексического индекса из персистентных чанков (:func:`seed_lexical_index`,
        P4a/I17). Не открывается здесь: состав хранения — ответственность вызывающего.
    :param chunk_lookup: резолвер метаданных чанка (``Mapping``/callable) для
        ``NpyVectorIndex`` (P4a); ``None`` → dense-плечо остаётся ``None`` (явное отсутствие).
    :param index_build_id: явный идентификатор зафиксированной сборки (из манифеста прогона);
        ``None`` → берётся единственная сборка пакета, при неоднозначности — ``None``.
    :param vector_index / lexical_index: явно инъецированные порты ретривала (ручной DI, A1);
        приоритетнее автосборки (тесты/засеянные сборки).
    :param embedder: явно инъецированный эмбеддер (ручной DI, A1); приоритетнее авторезолва
        (:func:`_resolve_embedder`). Нужен команде эксперимента (P4b): детерминированный шов
        без загрузки модели.
    :raises WiringError: неизвестный профиль или split без валидного deploy-конфига.
    """
    if profile == LOCAL_PROFILE:
        # Краулер корпуса строится только при явном deploy-конфиге: политика crawl
        # (user_agent/timeout/follow_redirects) — данные оператора (03 D.5/D.6). Без
        # конфига — прежнее поведение (crawler=None; сетевые порты инъектируются).
        local_crawler = (
            build_scout_crawler(
                deploy=deploy,
                cache=cache,
                domain_profiles=domain_profiles,
                run_crawl=run_crawl,
                cli_max_depth=cli_max_depth,
            )
            if deploy is not None
            else None
        )
        # Lane A/L3a: карта per-engine краулеров для Ш5 (http всегда; scrapy/browser —
        # только при упоминании движка в crawl_sources/domain_profiles, лениво-guarded).
        crawlers = build_scout_crawlers(
            deploy=deploy,
            cache=cache,
            domain_profiles=domain_profiles,
            crawl_sources=crawl_sources,
            run_crawl=run_crawl,
            cli_max_depth=cli_max_depth,
            engine_log=engine_log,
        )
        # Phase 5.1: поллеры фидов/каналов (``kind → PollPort``) строятся из того же
        # локального краулера (фетч ленты — транспортом краула); без краулера (нет
        # deploy-конфига) опрос фидов выключен (``None``), как и прочие сетевые порты.
        pollers: dict[str, PollPort] | None = (
            {
                "feed": LocalFeedPoller(local_crawler),
                "channel": LocalFeedPoller(local_crawler),
                # D3: sitemap — тот же локальный транспорт; потолок записей — спековый
                # потолок одного sitemap (``MAX_SITEMAP_ENTRIES``), не дефолт feed/channel.
                "sitemap": LocalFeedPoller(
                    local_crawler, max_entries=MAX_SITEMAP_ENTRIES
                ),
            }
            if local_crawler is not None
            else None
        )
        # E2: mapping ``registry_id → GenericRegistryFetcher`` из package-scoped спецификаций
        # (единственная точка конструирования локальных фетчеров). Пусто → реестры не
        # сконфигурированы (прежнее поведение: волна пуста).
        resolved_specs = dict(registry_specs or {})
        local_registry_fetchers = _build_local_registry_fetchers(
            resolved_specs,
            registry_http_get if registry_http_get is not None else _RegistryHttpClient(),
        )
        # P7: PDF-механизм — та же цепочка app < run < site, что у остальных осей краула
        # (site-слой здесь недоступен: порты собираются до per-root резолва; корневой
        # механизм досоздаётся фабрикой в ``build_corpus``). Явный ``pdf_pipeline``
        # (операторский ``--pdf-pipeline``) сильнее цепочки: он объявляет механизм этого
        # прогона и обязан действовать и на пробном экстракторе, а не только в Ш5 —
        # иначе диагностика мерила бы другой движок, чем корпус.
        #
        # P8/F7: та же цепочка даёт ось ``document_timeout_s``. В адаптер идёт **только
        # объявленное** значение (``document_timeout_s_explicit``): дефолт оси (1800 с)
        # равен потолку задачи фазы, а не per-level таймауту конвертера, и его подстановка
        # изменила бы поведение прогонов, которые ось не объявляли.
        crawl_limits = resolve_crawl_limits(
            "",
            app=(deploy.crawl if deploy is not None else CrawlConfig()),
            run=run_crawl,
            profiles=None,
        )
        if pdf_pipeline is None:
            pdf_pipeline = crawl_limits.pdf_pipeline
        document_timeout = (
            crawl_limits.document_timeout_s
            if crawl_limits.document_timeout_s_explicit
            else None
        )
        # Ось ``embed.device`` — тем же резолвом ``app < run``, что увидит исполнение
        # (``resolve_embed_device``), а не копией формулы. Доступность CUDA проверяется
        # здесь же (:func:`require_embed_device`): недоступное устройство — отказ до
        # прогона, а не тихие CPU-векторы под объявленным ``cuda``.
        embed_device = resolve_embed_device(
            (deploy.embed if deploy is not None else EmbedConfig()),
            run_embed,
        )
        # Точность — соседняя объявленная ось того же резолва ``app < run``. Исполнимость
        # проверяет выбор адаптера (``_resolve_embedder``): torch-путь умеет fp16
        # (``autocast``), Lite — нет, и тогда это отказ, а не тихий fp32.
        embed_precision = resolve_embed_precision(
            (deploy.embed if deploy is not None else EmbedConfig()),
            run_embed,
        )
        require_embed_device(embed_device)
        embed_notices: list[str] = []
        ports = _build_local_ports(
            evaluator=evaluator,
            registry_fetchers=local_registry_fetchers or None,
            registry_spec_digest=registry_specs_digest(resolved_specs),
            index_root=index_root,
            package=package,
            crawler=local_crawler,
            crawlers=crawlers,
            pollers=pollers,
            store=store,
            chunk_lookup=chunk_lookup,
            index_build_id=index_build_id,
            vector_index=vector_index,
            lexical_index=lexical_index,
            embedder=embedder,
            pdf_pipeline=pdf_pipeline,
            document_timeout=document_timeout,
            # P9: устройство — из той же разрешённой оси (app < run), что и механизм:
            # пробный экстрактор обязан исполняться там же, где объявлено, иначе
            # ``extractor_version`` портов и Ш5 описывали бы разные устройства.
            pdf_device=crawl_limits.pdf_device,
            embed_device=embed_device,
            embed_precision=embed_precision,
            embed_notices=embed_notices,
        )
        # Выбор эмбеддера под объявленное устройство виден в журнале wiring (тот же
        # ``wiring.jsonl``, что и выбор движка краула): исключение Lite под ``cuda`` —
        # не тихое решение.
        if engine_log is not None:
            for notice in embed_notices:
                engine_log.warn(notice, event="embed_device_note")
        return ports
    if profile == SPLIT_PROFILE:
        config = deploy if deploy is not None else load_deploy_config(None)
        if not is_split(config):
            raise WiringError(
                "профиль split требует deploy-конфиг с profile split/split-hardened, "
                f"получено {config.profile!r} (07 §6/§7)"
            )
        content_gateway = gateway or build_gateway(
            config, transport=transport, extra_headers=extra_headers
        )
        # B1/B3: оси извлечения для шва — тот же резолв app < run, которым пользуется
        # локальный композит, чтобы `split` и `local` получали одно объявленное значение,
        # а не два «дефолта по месту».
        split_limits = resolve_crawl_limits(
            "",
            app=config.crawl,
            run=run_crawl,
            profiles=None,
        )
        # E2: mapping ``registry_id → RemoteRegistry`` из package-scoped спецификаций —
        # по одному remote-прокси на объявленный реестр (multi-registry на split).
        resolved_specs = dict(registry_specs or {})
        split_registry_fetchers = {
            spec_id: RemoteRegistry(content_gateway, registry_id=spec_id)
            for spec_id in sorted(resolved_specs)
        }
        # B1 (I18/07 §4–§5): в split ядро **не** держит и не выбирает локальные веб-краулеры —
        # иначе per-engine движок из ``crawl_sources`` заставил бы ядро фетчить страницы
        # in-process (прямой веб-эгресс из Z0, нарушение зонной модели). Весь веб идёт через
        # шов: ``crawlers=None`` → ``build_corpus`` для любого движка использует fallback
        # ``RemoteCrawler`` (шов транспорт-агностичен; per-engine маршрутизация на edge —
        # Phase 3).
        return Ports(
            crawler=RemoteCrawler(content_gateway),
            crawlers=None,
            # Phase 3.3: краул split исполняется через edge-планировщик (``CrawlExecutorPort``):
            # submit/status/collect по шву. ``crawler=RemoteCrawler`` оставлен для обратной
            # совместимости (non-executor путь); при наличии ``executor`` фаза его не использует.
            executor=EdgeCrawlExecutor(content_gateway, user_agent=config.crawl.user_agent),
            # Phase 5.1: опрос фидов/каналов делегируется DMZ-шву (op ``crawl/poll``); ядро
            # не фетчит ленты in-process (веб-эгресс только через шов, 07 §4–§5).
            pollers={
                "feed": RemoteFeedPoller(content_gateway),
                "channel": RemoteFeedPoller(content_gateway),
                # D3: развёртку sitemap исполняет DMZ (тот же шов ``crawl/poll``).
                "sitemap": RemoteFeedPoller(
                    content_gateway, max_entries=MAX_SITEMAP_ENTRIES
                ),
            },
            search=RemoteSearch(
                content_gateway,
                adapter_kind=(config.search.adapter or "remote"),
                provider="edge",
            ),
            # B3: спуск в контейнеры остаётся в ядре и в ``split``: байты архива лежат на
            # стороне ядра (``corpus_raw``), а читатель — локальный (листинг/стриминг
            # файла, не веб-эгресс). Член контейнера извлекается швом **по артефакту**
            # (``extract/upload`` + ``submit``): у него синтетический URL, но файл на диске
            # есть, поэтому ``extract_unavailable`` больше не повод для пропуска.
            container=container_for,
            extractor=RemoteExtractor(
                content_gateway,
                # B1/B3 (07 §8): объявленные оси извлечения едут в ``extract/submit``,
                # поэтому смена уровня/устройства меняет результат под ``split`` так же,
                # как под ``local``. Резолв — та же цепочка app < run, что у локального
                # композита (:func:`_build_local_ports`); per-root слой здесь не виден —
                # порты собираются до per-root резолва (то же ограничение, что у
                # ``pdf_pipeline`` выше).
                document_level=split_limits.document_level,
                pdf_device=split_limits.pdf_device,
                # Потолок ожидания извлечения — объявленная ось (её дефолт и есть прежний
                # зашитый 1800 с), а не второй таймер на стороне адаптера.
                document_timeout=split_limits.document_timeout_s,
            ),
            registry_fetchers=split_registry_fetchers or None,
            registry_spec_digest=registry_specs_digest(resolved_specs),
        )
    raise WiringError(f"неизвестный профиль развёртывания: {profile!r}")


def manifest_deploy_fields(
    deploy: DeployConfig, capabilities: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Поля деплой-провенанса манифеста прогона (R7; ``07`` §7).

    Возвращает три раздельных ключа, каждый из которых ложится в собственное поле
    :class:`~core.domain.models.RunManifest`:

    * ``deploy_profile`` — выбранный профиль зон (``local``/``split``/``split-hardened``);
    * ``edge_schema_version`` — версия контракта шва (``/v1/*``); **execution-class**
      провенанс (§13, не методологическая версия) — константа :data:`DEFAULT_SCHEMA_VERSION`;
    * ``edge_capabilities`` — версии edge-компонентов из ``/v1/capabilities``
      (если переданы; иначе пусто — значения не выдумываются).

    Деплой-провенанс течёт в манифест композицией: ``DeployConfig`` → ``_resolve_phases``
    → ``build_phases`` → фаза ``report`` → ``build_manifest`` → ``run_manifest.yaml``/
    ``SqliteStore.persist_manifest``. Хелпер даёт единый источник этих полей для
    композиции и диагностики шва.
    """
    return {
        "deploy_profile": deploy.profile,
        "edge_schema_version": DEFAULT_SCHEMA_VERSION,
        "edge_capabilities": dict(capabilities or {}),
    }


def build_runner(
    store: SqliteStore,
    *,
    profile: str = LOCAL_PROFILE,
    ports: Ports | None = None,
    phases: dict[str, PhaseFn] | None = None,
    clock: Clock | None = None,
) -> Runner:
    """Собирает :class:`Runner` с внедрёнными портами (ручной DI, A1).

    ``phases`` переопределяет реестр фаз (тесты/будущие W7–W11); по умолчанию действует
    реестр ``Runner`` — **только гейты** (реальные проверки). Не-гейтовые фазы обязан
    подставить composition root (``build_phases``): незарегистрированная фаза даёт
    ``failed`` («не зарегистрирована»), а не подделку ``ok``. Порты, если не переданы,
    собираются :func:`build_adapters` по профилю.

    :raises WiringError: неизвестный/нереализованный профиль (см. :func:`build_adapters`).
    """
    resolved_ports = ports if ports is not None else build_adapters(profile=profile)
    return Runner(store, ports=resolved_ports, clock=clock, phases=phases)
