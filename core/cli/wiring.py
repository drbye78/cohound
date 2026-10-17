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
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.adapters.chunker_markdown import MarkdownChunkerPort
from core.adapters.crawl_browser import BrowserCrawler
from core.adapters.crawl_http import HttpCrawler
from core.adapters.crawl_scrapy import CrawlError, ScrapyCrawler
from core.adapters.embed_local import LocalEmbedder
from core.adapters.embed_model2vec import LiteEmbedder, model2vec_available
from core.adapters.extract_document import DoclingDocumentExtractor
from core.adapters.extract_html import HtmlExtractor
from core.adapters.feed_poller import LocalFeedPoller, RemoteFeedPoller
from core.adapters.index_npy import NpyIndexBuilder, numpy_available
from core.adapters import index_format
from core.adapters.lexical_tantivy import (
    LexicalChunk,
    TantivyLexicalIndex,
    tantivy_available,
)
from core.adapters.vector_npy import ChunkLookup, NpyVectorIndex
from core.adapters.llm_discovery import (
    DEFAULT_DECODER as DISCOVERY_DEFAULT_DECODER,
    DEFAULT_MAX_TOKENS as DISCOVERY_DEFAULT_MAX_TOKENS,
    DEFAULT_TIMEOUT as DISCOVERY_DEFAULT_TIMEOUT,
    DiscoveryLLM,
    GenerateFn as DiscoveryGenerateFn,
)
from core.adapters.llm_local import (
    DEFAULT_DECODER as EVALUATOR_DEFAULT_DECODER,
    DEFAULT_MODEL_ID as EVALUATOR_DEFAULT_MODEL_ID,
    DEFAULT_TIMEOUT as EVALUATOR_DEFAULT_TIMEOUT,
    GenerateFn as EvaluatorGenerateFn,
    LocalEvaluator,
)
from core.adapters.remote_crawl_executor import EdgeCrawlExecutor
from core.adapters.remote_crawler import RemoteCrawler
from core.adapters.remote_extractor import RemoteExtractor
from core.adapters.remote_gateway import (
    DEFAULT_SCHEMA_VERSION,
    HttpContentGateway,
    TransportFn,
)
from core.adapters.registry_generic import (
    GenericRegistryFetcher,
    RegistryConfig,
    RegistryFetchError,
)
from core.adapters.remote_registry import RemoteRegistry
from core.adapters.remote_search import RemoteSearch
from core.adapters.search_glm import GlmSearchAdapter
from core.adapters.search_yandex import YandexSearchAdapter
from core.config.deploy import (
    DeployConfig,
    LLMDeployConfig,
    LLMSlotConfig,
    is_split,
    load_deploy_config,
    resolve_llm_slot,
    resolve_search_provider,
)
from core.config.domain_profiles import DomainProfile
from core.config.crawl_sources import CrawlSourcesConfig
from core.domain.models import CrawlRunConfig
from core.pipeline import Ports, Runner
from core.pipeline.runner import PHASES, Clock, PhaseFn
from core.ports.crawler import CrawlerPort
from core.ports.discovery_llm import DiscoveryLLMPort
from core.ports.embedder import EmbedderPort
from core.ports.extractor import ExtractorPort
from core.ports.gateway import ContentGateway
from core.ports.lexical_index import LexicalIndexPort
from core.ports.llm import EvaluatorPort
from core.ports.poller import PollPort
from core.ports.registry import RegistryFetcherPort
from core.ports.search import SearchAdapterPort
from core.ports.store import StorePort
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchMode, RawPage
from core.ports.types import FetchPlan
from core.ports.vector_index import VectorIndexPort
from core.store.backup import list_builds
from core.store.sqlite_store import SqliteStore
from core.util.cache import ContentAddressedCache
from core.util.hashing import short_hash
from core.util.robots import RobotsGate
from core.util.sitemap import MAX_SITEMAP_ENTRIES

__all__ = [
    "LOCAL_PROFILE",
    "SPLIT_PROFILE",
    "SUPPORTED_PROFILES",
    "SUPPORTED_SEARCH_ADAPTERS",
    "DEFAULT_REGISTRY_ID",
    "ENV_LLM_SLOT",
    "ENV_SCOUT_CRAWLER",
    "DEFAULT_SCOUT_CRAWLER",
    "SUPPORTED_SCOUT_CRAWLERS",
    "ENV_EMBED_LITE_MODEL",
    "ENV_EMBED_LITE_REVISION",
    "ENV_EMBED_LITE_PATH",
    "PHASES",
    "WiringError",
    "run_db_path",
    "build_store",
    "build_gateway",
    "build_search_adapter",
    "build_discovery_llm",
    "build_evaluator_llm",
    "build_lite_embedder",
    "build_vector_index",
    "seed_lexical_index",
    "ExperimentTarget",
    "resolve_experiment_target",
    "EmbedderBuildMismatch",
    "assert_embedder_matches_build",
    "build_scout_crawler",
    "build_scout_crawlers",
    "build_adapters",
    "build_runner",
    "manifest_deploy_fields",
    "SearchResolutionError",
    "resolve_and_build_search_adapter",
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

#: Идентификатор реестра по умолчанию для remote-прокси (``split``, ``RemoteRegistry``).
#: Рабочие спецификации реестров — **package-scoped** (``packages/{case_id}/registries/
#: {registry_id}.yaml``, перечислены в ``package.yaml: registries``, 03 §D.4); корневого
#: ``registries/`` больше нет. ``DeployConfig`` не несёт ``registry_fetchers``, поэтому
#: ``split``-прокси использует этот плейсхолдер-идентификатор, а конкретные id приходят
#: из манифеста пакета (данные, не предметная категория).
DEFAULT_REGISTRY_ID = "edge"

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
#: Дефолтная модель LLM-разведки (env-фолбэк; данные, не предметная категория).
DEFAULT_LLM_MODEL = "deepseek-v4.1-flash:cxbc"

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


def _single_persisted_build(index_root: str | Path, package: str | None) -> str | None:
    """Единственная зафиксированная сборка пакета или ``None`` (детерминированно).

    Переиспользует существующий перечислитель сборок ``backup.list_builds`` (единый
    носитель раскладки ``index_store/{package}/{build}/``). При **неоднозначности**
    (>1 сборки) возвращает ``None``: выбор произвольной сборки сломал бы воспроизводимость
    (A3/§13) — вызывающий обязан передать явный ``build_id`` (из манифеста прогона).
    """
    if not package:
        return None
    builds = list_builds(index_root, package)
    return builds[0][0] if len(builds) == 1 else None


def _build_dim(index_root: str | Path, package: str, build_id: str) -> int | None:
    """Размерность вектора из манифеста первой продукт-партиции сборки (или ``None``)."""
    for product_id in index_format.list_partitions(index_root, package, build_id):
        dim = index_format.read_manifest(index_root, package, build_id, product_id).get("dim")
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
    resolved = build_id or _single_persisted_build(index_root, package)
    if resolved is None:
        return None
    dim = _build_dim(index_root, package, resolved)
    if dim is None:
        return None
    return NpyVectorIndex(index_root, package, resolved, dim, chunk_lookup)


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
    ``store.iter_chunks(build_id, product_id=…)`` (``product_id`` известен из раскладки
    ``index_format.list_partitions``), поэтому поиск остаётся продукт-скоупед (I17).
    Иначе — ``None`` (явное отсутствие; эксперимент пропускает R0/R2, N3).
    """
    if not tantivy_available() or not package or store is None:
        return None
    resolved = build_id or _single_persisted_build(index_root, package)
    if resolved is None:
        return None
    records: list[LexicalChunk] = []
    for product_id in index_format.list_partitions(index_root, package, resolved):
        for chunk in store.iter_chunks(resolved, product_id=product_id):
            records.append(_lexical_record(chunk, product_id, resolved))
    if not records:
        return None
    index = TantivyLexicalIndex()
    index.index(records, build_id=resolved)
    return index


def _lexical_record(chunk: Chunk, product_id: str, build_id: str) -> LexicalChunk:
    """``ports.types.Chunk`` + ``product_id`` (из партиции) → запись лексического индекса."""
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
    index_root: str | Path, package: str, build_id: str, product_ids: tuple[str, ...]
) -> str | None:
    """``embed_model_id`` из ``manifest.json`` первой продукт-партиции сборки (или ``None``).

    On-disk манифест — авторитетный источник того, чем **физически** собраны векторы.
    """
    for product_id in product_ids:
        manifest = index_format.read_manifest(index_root, package, build_id, product_id)
        model_id = manifest.get("embed_model_id")
        if isinstance(model_id, str) and model_id:
            return model_id
    return None


def resolve_experiment_target(
    store: StorePort, run_id: str, *, index_root: str | Path = "index_store"
) -> ExperimentTarget | None:
    """Разрешает зафиксированную сборку прогона из БД (P4b, нейтрально).

    Источники (без сканирования каталогов, A1): ``runs.index_build_id`` даёт сборку;
    ``index_manifest`` этой сборки — пакет раскладки, продукт-партиции и (фолбэк)
    ``embed_model_id``; ``runs.package_id`` — фолбэк пакета, если запись
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
    product_ids = tuple(sorted({entry.product_id for entry in entries})) or tuple(
        index_format.list_partitions(index_root, package, build_id)
    )
    embed_model_id = _build_embed_model_id(index_root, package, build_id, product_ids)
    if embed_model_id is None:
        embed_model_id = next(
            (entry.embed_model_id for entry in entries if entry.embed_model_id), None
        )
    return ExperimentTarget(
        package=package,
        build_id=build_id,
        product_ids=product_ids,
        embed_model_id=embed_model_id,
    )


def run_db_path(runs_dir: str | Path, run_id: str) -> Path:
    """Путь к операционной БД прогона: ``runs/{run_id}/cohound.db`` (``03`` §A.2)."""
    return Path(runs_dir) / run_id / "cohound.db"


def build_store(
    db_path: str | Path = ":memory:", *, profile: str = LOCAL_PROFILE
) -> SqliteStore:
    """Собирает операционное хранилище прогона.

    Для обоих профилей ``StorePort`` живёт в ядре (доверенная зона ``core``, ``01`` §3.0),
    поэтому выбор профиля на хранилище не влияет — разделяются лишь веб-адаптеры
    (:func:`build_adapters`). Каталог БД создаётся при необходимости.

    :param db_path: файл БД или ``":memory:"``.
    :param profile: профиль развёртывания (проверяется на известность).
    :raises WiringError: неизвестный профиль.
    """
    if profile not in SUPPORTED_PROFILES:
        raise WiringError(f"неизвестный профиль развёртывания: {profile!r}")
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


def build_search_adapter(adapter: str) -> SearchAdapterPort:
    """Собирает внешний поисковый адаптер по имени для ``hound discover`` (``03`` §D.6).

    Провайдер — данные-конфигурация (не предметная категория, §1.3). Реальный адаптер
    подтягивает ``httpx`` **лениво**; готовность проверяется свойством ``available`` у
    вызывающего (CLI). Отсутствие ``httpx``/учётных данных не подменяется выдуманными
    кандидатами (I19) — это решает :func:`core.cli.main.cmd_discover`.

    :param adapter: имя адаптера из :data:`SUPPORTED_SEARCH_ADAPTERS`.
    :raises WiringError: неизвестный адаптер.
    """
    if adapter == "yandex":
        return YandexSearchAdapter()
    if adapter == "glm":
        return GlmSearchAdapter()
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

    Все известные адаптеры (``yandex``/``glm``) выставляют атрибут ``api_key``; без
    ключа такой адаптер не готов, и кандидаты не выдумываются (I19). Логика остаётся
    generic: если у адаптера нет атрибута ``api_key`` вовсе, достаточно доступности
    (``available``) — иначе он ошибочно блокировался бы. Для инъектированного
    тестового шва проверка не применяется (см. ``cmd_discover``).
    """
    if not bool(getattr(adapter, "available", True)):
        return False
    if hasattr(adapter, "api_key") and not getattr(adapter, "api_key", None):
        return False
    return True


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
) -> tuple[Any | None, str | None]:
    """Разрешает провайдера поиска и собирает адаптер (общий хелпер ``discover``/scout).

    Семантика (детерминированная, без LLM — I16/I21):

    * ``injected is not None`` — тестовый шов: резолв/валидация провайдера пропускаются,
      возвращается ``(injected, getattr(injected, "provider", "") or None)``;
    * ``no_search`` — поиск выключен: ``(None, None)``;
    * иначе — ``load_deploy_config(None)`` → :func:`resolve_search_provider` → проверка
      против :data:`SUPPORTED_SEARCH_ADAPTERS` → :func:`build_search_adapter` → проверка
      готовности (:func:`_search_adapter_configured`).

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
    adapter = build_search_adapter(provider)
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
) -> EvaluatorPort | None:
    """Собирает runtime-оценщик из активного LLM-слота; ``None`` — слот не сконфигурирован.

    Оценщик (I18, без веб-тула) конструируется :class:`~core.adapters.llm_local.LocalEvaluator`
    из **активного слота** (``slot`` (CLI) > ``COHOUND_LLM_SLOT`` (env) > ``run_slot``
    (``run.yaml: llm_slot``) > ``deploy_llm.slot`` > ``cheap``, см.
    :func:`_resolve_llm_slot_and_config`). Приоритет полей: явные аргументы > env
    ``COHOUND_LLM_*`` > поля слота > дефолты модуля ``adapters.llm_local``.

    **Условие активности.** Если у выбранного слота (и env/аргументов) пустой ``base_url``
    — возвращается ``None``: оценщик не поднимается, поведение прогона прежнее (сценарий
    evaluator требует явного endpoint внутреннего [OI]-совместимого API корпоративного
    периметра). ``api_key`` — единственный источник ``COHOUND_LLM_API_KEY`` (внешних
    фолбэков нет).

    :param slot: явный активный слот (``--llm-slot``); ``None`` → env/run.yaml/deploy-слот.
    :param deploy_llm: секция ``llm`` deploy-конфига; ``None`` — нет слотов.
    :param run_slot: слот из ``run.yaml: llm_slot`` (§4.5); ниже env, выше ``deploy_llm.slot``.
    :param generate: тестовый/оффлайн-шов ``(prompt, schema) -> str`` (без сети).
    :raises ValueError: неизвестный слот (``--llm-slot``/``COHOUND_LLM_SLOT``/``run.yaml``).
    """
    _resolved_slot, slot_config = _resolve_llm_slot_and_config(deploy_llm, slot, run_slot)
    slot_base = slot_config.base_url if slot_config is not None else None
    slot_model = slot_config.model_id if slot_config is not None else None
    slot_version = slot_config.model_version if slot_config is not None else None
    slot_decoder = slot_config.decoder if slot_config is not None else None
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
    resolved_model = (
        model_id
        if model_id is not None
        else os.environ.get(ENV_LLM_MODEL)
        or slot_model
        or EVALUATOR_DEFAULT_MODEL_ID
    )
    resolved_version = (
        model_version if model_version is not None else slot_version
    )
    resolved_decoder = (
        decoder
        if decoder is not None
        else os.environ.get(ENV_LLM_DECODER) or slot_decoder or EVALUATOR_DEFAULT_DECODER
    )
    resolved_timeout = (
        timeout
        if timeout is not None
        else _env_float(ENV_LLM_TIMEOUT, slot_timeout)
    )
    return LocalEvaluator(
        generate=generate,
        model_id=resolved_model or EVALUATOR_DEFAULT_MODEL_ID,
        model_version=resolved_version or "",
        decoder=resolved_decoder or EVALUATOR_DEFAULT_DECODER,
        base_url=str(resolved_base).strip(),
        api_key=(resolved_key or None),
        timeout=resolved_timeout,
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
    if run_crawl is not None and getattr(run_crawl, "max_depth", None):
        depths.append(int(run_crawl.max_depth))
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
    P2-3): отдельного пейсера не заводим (иначе было бы два владельца). Недоступная лента
    трактуется как «правил нет» (:class:`RobotsGate`), отказ ``Disallow`` несёт
    ``refusal_reason="robots_disallow"`` (адаптер, egress-слой).
    """

    def fetch(host: str) -> str | None:
        try:
            page = http_crawler.fetch(
                f"https://{host}/robots.txt", FetchPlan(FetchMode.HTTP)
            )
        except Exception:  # noqa: BLE001 — недоступный robots.txt = «правил нет»
            return None
        raw = getattr(page, "content", b"") or b""
        return raw.decode("utf-8", errors="replace") if raw else None

    return RobotsGate(fetch, user_agent=user_agent or "*")


def build_scout_crawler(
    *,
    deploy: DeployConfig | None = None,
    cache: ContentAddressedCache | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
    run_crawl: CrawlRunConfig | None = None,
    cli_max_depth: int | None = None,
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
    :raises WiringError: неизвестное значение ``COHOUND_SCOUT_CRAWLER``.
    """
    raw = os.environ.get(ENV_SCOUT_CRAWLER)
    name = (raw if raw is not None else DEFAULT_SCOUT_CRAWLER).strip().lower()
    if not name:
        name = DEFAULT_SCOUT_CRAWLER
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
        return ScrapyCrawler(cache=cache)
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
    raise WiringError(
        f"{ENV_SCOUT_CRAWLER}: неизвестный краулер {name!r}; "
        f"поддерживаются: {', '.join(SUPPORTED_SCOUT_CRAWLERS)}"
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
) -> dict[str, CrawlerPort]:
    """Собирает карту ``engine → CrawlerPort`` для per-engine выбора краулера (L3a).

    Лёгкий ``http`` (:class:`HttpCrawler`) строится **всегда** (дефолтный движок).
    Тяжёлые ``scrapy``/``browser`` строятся **только** для движков, реально упомянутых
    конфигурацией (env :data:`ENV_SCOUT_CRAWLER`, поля ``engine`` в ``crawl_sources``,
    либо ``fetch: browser`` в ``domain_profiles``; см. :func:`_referenced_crawl_engines`).
    Это исключает ленивое поднятие Playwright/Scrapy, когда они не нужны (A1).

    Сборка каждого тяжёлого адаптера обёрнута в guard: недоступность зависимости
    (:class:`~core.adapters.crawl_scrapy.CrawlError`/``CrawlUnavailable``), ``ImportError``
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

    crawlers: dict[str, CrawlerPort] = {
        "http": HttpCrawler(
            **_http_crawler_kwargs(config), cache=cache, robots=robots
        ),
    }
    if "scrapy" in referenced:
        try:
            crawlers["scrapy"] = ScrapyCrawler(cache=cache)
        except (WiringError, ImportError, CrawlError):
            pass
    if "browser" in referenced:
        try:
            crawlers["browser"] = BrowserCrawler(
                **_browser_crawler_kwargs(config), cache=cache, robots=robots
            )
        except (WiringError, ImportError, CrawlError):
            pass
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


#: Формат документа → расширение (роутинг композитного экстрактора на docling).
_DOC_SUFFIX_BY_CONTENT_TYPE: dict[str, str] = {
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
}


def _detect_document_filename(page: RawPage) -> str | None:
    """Имя файла с документным расширением, если формат страницы обрабатывает docling.

    Формат выводится из URL (суффикс пути), затем из ``content_type``. Возврат ``None``
    означает, что страницу обрабатывает HTML-путь (trafilatura). Используется только при
    маршруте ``AUTO``; ``HTML``/``DOCUMENT`` задают путь явно (D2).
    """
    suffix = Path(urlsplit(page.url).path).suffix.lower()
    if suffix and DoclingDocumentExtractor.supports(f"document{suffix}"):
        return f"document{suffix}"
    content_type = (page.content_type or "").split(";")[0].strip().lower()
    doc_suffix = _DOC_SUFFIX_BY_CONTENT_TYPE.get(content_type)
    return f"document{doc_suffix}" if doc_suffix is not None else None


class _LocalExtractor:
    """Композит локальной экстракции: документы (docling) поверх HTML (trafilatura).

    Реализует ``ExtractorPort`` структурно. Маршрут ``ExtractionRoute`` (D2):

    * ``HTML`` — принудительно :class:`HtmlExtractor` (trafilatura);
    * ``DOCUMENT`` — принудительно :class:`DoclingDocumentExtractor` (docling);
    * ``AUTO`` — авто-детект формата (URL/content-type): документ → docling, иначе HTML.

    Оба адаптера деградируют грациозно (``ExtractedDoc.error``), поэтому композит не
    бросает исключений на уровне извлечения.
    """

    def __init__(
        self, html: ExtractorPort, documents: DoclingDocumentExtractor | None
    ) -> None:
        self._html = html
        self._documents = documents
        html_version = str(getattr(self._html, "version", "") or "")
        # Версия экстрактора: HTML-версия, дополненная документной при наличии docling.
        self.version = (
            html_version
            if documents is None
            else f"{html_version}+{documents.version}"
        )

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        """``RawPage`` → ``ExtractedDoc`` по маршруту ``route`` (D2)."""
        if route is ExtractionRoute.HTML:
            return self._html.extract(page, route)
        if self._documents is None:
            return self._html.extract(page, route)
        if route is ExtractionRoute.DOCUMENT:
            filename = _detect_document_filename(page)
            return self._documents.extract_bytes(page.content, filename or "document.pdf")
        filename = _detect_document_filename(page)
        if filename is not None:
            return self._documents.extract_bytes(page.content, filename)
        return self._html.extract(page, route)


def _build_local_ports(
    *,
    evaluator: EvaluatorPort | None,
    registry: RegistryFetcherPort | None,
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
) -> Ports:
    """Собирает in-process порты профиля ``local`` (без сети/тяжёлых зависимостей).

    Гарантия: никогда не бросает из-за отсутствия опциональной зависимости — недоступный
    тяжёлый адаптер остаётся ``None``. ``evaluator``/``registry`` не создаются сами:
    передаются вызывающим (генератор/конфиг; I6/I7/I18 не автоматизируются).
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

    **Dense- и лексическое плечи (P4a, I17).** ``vector_index``/``lexical_index`` строятся
    :func:`build_vector_index`/:func:`seed_lexical_index` только при наличии предпосылок
    (``numpy``/``tantivy``, ``chunk_lookup``/``store`` и разрешённая зафиксированная сборка
    ``index_build_id``). Отсутствие предпосылки — явный ``None`` (N3): эксперимент фиксирует
    это как ``skipped``, а не как молчаливую дыру или выдуманный индекс. Явно
    инъецированные порты имеют приоритет (ручной DI, A1).
    """
    html = HtmlExtractor()
    documents = DoclingDocumentExtractor()
    # Без docling ``extractor`` — ровно ``HtmlExtractor``; при наличии docling
    # документные форматы (pdf/docx/pptx/xlsx) уходят в него через композит.
    extractor: ExtractorPort = (
        _LocalExtractor(html, documents) if documents.available else html
    )

    # Тяжёлые опциональные адаптеры: только при выполненной availability-предикате.
    resolved_embedder = embedder if embedder is not None else _resolve_embedder()
    index_builder = (
        NpyIndexBuilder(index_root=index_root, package=package)
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
        registry=registry,
        # E2: mapping реестровых фетчеров (registry_id → fetcher) из package-scoped
        # спецификаций + дайджест спецификаций (провенанс манифеста, §13).
        registry_fetchers=registry_fetchers,
        registry_spec_digest=registry_spec_digest,
        chunker=MarkdownChunkerPort(),
        extractor=extractor,
        embedder=resolved_embedder,
        index_builder=index_builder,
        # Adapter/port mismatch (engine_version: property vs mutable attr) — статическая
        # несовместимость протокола; runtime_checkable проверяет методы, поведение корректно.
        lexical_index=resolved_lexical_index,  # type: ignore[arg-type]
        # Краулер корпуса: инстанцируется composition root'ом из ``deploy.crawl`` (R7/03
        # §D.5); без конфига остаётся ``None`` — прежнее поведение. SERP/discovery для
        # local включаются явной инъекцией портов (в CLI-конфиге нет запросов).
        crawler=crawler,
        # Lane A/L3a: карта ``engine → CrawlerPort`` для per-engine выбора в Ш5.
        crawlers=crawlers,
        # Phase 5.1: карта ``kind → PollPort`` для опроса фидов/каналов в Ш5.
        pollers=pollers,
        search=None,
        vector_index=resolved_vector_index,
    )


def _resolve_embedder() -> EmbedderPort | None:
    """Выбирает эмбеддер: оффлайн ``LiteEmbedder`` → ``LocalEmbedder`` → ``None`` (P4a).

    Приоритет — лёгкий путь без ``torch`` (CI/fixation-friendly): он берётся, если
    ``model2vec`` установлен **и** локальный снапшот модели разрешается (оффлайн). Иначе
    сохраняется качественный ``LocalEmbedder`` (bge-m3) при наличии ``sentence-transformers``.
    Если недоступно ни то, ни другое — ``None``: адаптер **не** фабрикуется (I19/A3).
    """
    if model2vec_available():
        try:
            lite = build_lite_embedder()
        except Exception:  # noqa: BLE001 — сборка портов не бросает (грациозная деградация)
            lite = None
        if lite is not None and lite.available:
            return lite
    if _module_installed("sentence_transformers"):
        return LocalEmbedder()
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
        httpx = self._import_http()
        try:
            response = httpx.get(
                url, headers=dict(headers), timeout=timeout, follow_redirects=True
            )
            response.raise_for_status()
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
    registry_id: str = DEFAULT_REGISTRY_ID,
    extra_headers: Mapping[str, str] | None = None,
    evaluator: EvaluatorPort | None = None,
    registry: RegistryFetcherPort | None = None,
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
    cli_max_depth: int | None = None,
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
    :param registry_id: идентификатор реестра для ``RemoteRegistry`` (данные).
    :param extra_headers: доп. заголовки шва (напр. OIDC bearer).
    :param evaluator: готовый оценщик (``local``); ``None`` → остаётся ``None`` (не
        конструируется автоматически: сценарий evaluator требует OI-совместимого endpoint
        корпоративного облака, см. :func:`build_evaluator_llm`).
    :param registry: готовый реестровый фетчер (``local``); ``None`` → остаётся ``None``
        (legacy-поле; конфиг реестра — данные вызывающего).
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
    :param cli_max_depth: ``--max-depth`` оператора — также участвует в автодетекте
        discovery (N1).
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
        return _build_local_ports(
            evaluator=evaluator,
            registry=registry,
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
        )
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
            extractor=RemoteExtractor(content_gateway),
            registry=RemoteRegistry(content_gateway, registry_id=registry_id),
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
