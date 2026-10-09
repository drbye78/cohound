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
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.adapters.chunker_markdown import MarkdownChunkerPort
from core.adapters.crawl_http import HttpCrawler
from core.adapters.crawl_scrapy import ScrapyCrawler
from core.adapters.embed_local import LocalEmbedder
from core.adapters.extract_document import DoclingDocumentExtractor
from core.adapters.extract_html import HtmlExtractor
from core.adapters.index_npy import NpyIndexBuilder, numpy_available
from core.adapters.lexical_tantivy import TantivyLexicalIndex, tantivy_available
from core.adapters.llm_discovery import (
    DEFAULT_DECODER as DISCOVERY_DEFAULT_DECODER,
    DEFAULT_MAX_TOKENS as DISCOVERY_DEFAULT_MAX_TOKENS,
    DEFAULT_TIMEOUT as DISCOVERY_DEFAULT_TIMEOUT,
    DiscoveryLLM,
    GenerateFn as DiscoveryGenerateFn,
)
from core.adapters.remote_crawler import RemoteCrawler
from core.adapters.remote_extractor import RemoteExtractor
from core.adapters.remote_gateway import (
    DEFAULT_SCHEMA_VERSION,
    HttpContentGateway,
    TransportFn,
)
from core.adapters.remote_registry import RemoteRegistry
from core.adapters.remote_search import RemoteSearch
from core.adapters.search_duckduckgo import DuckDuckGoSearchAdapter
from core.adapters.search_glm import GlmSearchAdapter
from core.adapters.search_google import GoogleSearchAdapter
from core.adapters.search_openwebsearch import OpenWebSearchAdapter
from core.adapters.search_yandex import YandexSearchAdapter
from core.config.deploy import DeployConfig, is_split, load_deploy_config
from core.pipeline import Ports, Runner
from core.pipeline.runner import PHASES, Clock, PhaseFn
from core.ports.crawler import CrawlerPort
from core.ports.discovery_llm import DiscoveryLLMPort
from core.ports.extractor import ExtractorPort
from core.ports.gateway import ContentGateway
from core.ports.llm import EvaluatorPort
from core.ports.registry import RegistryFetcherPort
from core.ports.search import SearchAdapterPort
from core.ports.types import ExtractedDoc, RawPage
from core.store.sqlite_store import SqliteStore

__all__ = [
    "LOCAL_PROFILE",
    "SPLIT_PROFILE",
    "SUPPORTED_PROFILES",
    "SUPPORTED_SEARCH_ADAPTERS",
    "DEFAULT_REGISTRY_ID",
    "ENV_SCOUT_CRAWLER",
    "DEFAULT_SCOUT_CRAWLER",
    "SUPPORTED_SCOUT_CRAWLERS",
    "PHASES",
    "WiringError",
    "run_db_path",
    "build_store",
    "build_gateway",
    "build_search_adapter",
    "build_discovery_llm",
    "build_scout_crawler",
    "build_adapters",
    "build_runner",
    "manifest_deploy_fields",
]

#: Профиль ``local`` — один процесс, реальные адаптеры in-process (дефолт, ``07``).
LOCAL_PROFILE = "local"
#: Профиль ``split`` — веб-адаптеры в DMZ, ядро ходит через ``ContentGateway`` (``07``).
SPLIT_PROFILE = "split"
#: Известные профили развёртывания, выбираемые CLI (``03`` §D.5/D.6).
SUPPORTED_PROFILES: tuple[str, ...] = (LOCAL_PROFILE, SPLIT_PROFILE)

#: Известные внешние поисковые адаптеры для ``hound discover`` (``03`` §D.6).
#: Провайдер — данные-конфигурация, а не предметная категория (§1.3).
SUPPORTED_SEARCH_ADAPTERS: tuple[str, ...] = (
    "yandex",
    "glm",
    "google",
    "duckduckgo",
    "open-websearch",
)

#: Идентификатор реестра по умолчанию для remote-прокси (данные; ``deploy.yaml``
#: в модели ``DeployConfig`` не несёт ``registry_fetchers`` — см. ГЭП в
#: :func:`manifest_deploy_fields`).
DEFAULT_REGISTRY_ID = "edge"

#: Префикс env-переменных LLM-разведки (``passport-scout``). Провайдер/модель — данные.
ENV_LLM_BASE_URL = "COHOUND_LLM_BASE_URL"
ENV_LLM_API_KEY = "COHOUND_LLM_API_KEY"
ENV_LLM_MODEL = "COHOUND_LLM_MODEL"
ENV_LLM_DECODER = "COHOUND_LLM_DECODER"
ENV_LLM_TIMEOUT = "COHOUND_LLM_TIMEOUT"
ENV_LLM_MAX_TOKENS = "COHOUND_LLM_MAX_TOKENS"

#: Env-фолбэк ключа контура (уже присутствует в среде развёртывания).
ENV_PLUSVIBE_API_KEY = "PLUSVIBEAPI_API_KEY"

#: Дефолтный base_url openai-совместимого сервера LLM-разведки (env-фолбэк; данные).
DEFAULT_LLM_BASE_URL = "https://plusvibeapi.ru/v1"
#: Дефолтная модель LLM-разведки (env-фолбэк; данные, не предметная категория).
DEFAULT_LLM_MODEL = "deepseek-v4.1-flash:cxbc"


class WiringError(ValueError):
    """Ошибка сборки зависимостей: неизвестный или ещё не реализованный профиль."""


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
    if adapter == "google":
        return GoogleSearchAdapter()
    if adapter == "duckduckgo":
        return DuckDuckGoSearchAdapter()
    if adapter == "open-websearch":
        return OpenWebSearchAdapter()
    raise WiringError(
        f"неизвестный поисковый адаптер: {adapter!r}; "
        f"поддерживаются: {', '.join(SUPPORTED_SEARCH_ADAPTERS)}"
    )


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


def build_discovery_llm(
    *,
    base_url: str | None = None,
    api_key: str | None = None,
    model_id: str | None = None,
    decoder: str | None = None,
    timeout: float | None = None,
    max_tokens: int | None = None,
    generate: DiscoveryGenerateFn | None = None,
) -> DiscoveryLLMPort:
    """Собирает LLM-адаптер разведки паспорта из env с override CLI-флагами (``03`` §E.3).

    Источник конфигурации — env ``COHOUND_LLM_*`` (префикс задан здесь); явные
    аргументы (CLI-флаги) имеют приоритет. Фолбэки: ``COHOUND_LLM_API_KEY`` →
    ``PLUSVIBEAPI_API_KEY``; ``COHOUND_LLM_BASE_URL`` → :data:`DEFAULT_LLM_BASE_URL`;
    ``COHOUND_LLM_MODEL`` → :data:`DEFAULT_LLM_MODEL`; ``COHOUND_LLM_DECODER`` →
    ``json_object`` (подтверждён рабочим). Пустые ``base_url``/``model_id`` допустимы:
    готовность проверяет вызывающий (``cmd_passport_scout``) и выдаёт понятную ошибку
    ``llm`` (:data:`EXIT_USAGE`), а не падает.

    :param generate: тестовый/оффлайн-шов ``(prompt, schema) -> str`` (без сети).
    :raises WiringError: нечисловой/неположительный ``timeout``/``max_tokens`` из env.
    """
    resolved_base = (
        base_url
        if base_url is not None
        else os.environ.get(ENV_LLM_BASE_URL) or DEFAULT_LLM_BASE_URL
    )
    resolved_key = (
        api_key
        if api_key is not None
        else os.environ.get(ENV_LLM_API_KEY) or os.environ.get(ENV_PLUSVIBE_API_KEY)
    )
    resolved_model = (
        model_id
        if model_id is not None
        else os.environ.get(ENV_LLM_MODEL) or DEFAULT_LLM_MODEL
    )
    resolved_decoder = (
        decoder
        if decoder is not None
        else os.environ.get(ENV_LLM_DECODER, DISCOVERY_DEFAULT_DECODER)
    )
    resolved_timeout = (
        timeout
        if timeout is not None
        else _env_float(ENV_LLM_TIMEOUT, DISCOVERY_DEFAULT_TIMEOUT)
    )
    resolved_max_tokens = (
        max_tokens
        if max_tokens is not None
        else _env_int(ENV_LLM_MAX_TOKENS, DISCOVERY_DEFAULT_MAX_TOKENS)
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


#: Env-переменная выбора краулера ``passport-scout``: ``http`` (дефолт) | ``scrapy``.
ENV_SCOUT_CRAWLER = "COHOUND_SCOUT_CRAWLER"
#: Дефолтный (лёгкий) краулер scout — обычный HTTP без браузера (A1, MVP).
DEFAULT_SCOUT_CRAWLER = "http"
#: Поддерживаемые краулеры scout: лёгкий ``http`` и браузерный ``scrapy``.
SUPPORTED_SCOUT_CRAWLERS: tuple[str, ...] = ("http", "scrapy")


def build_scout_crawler() -> CrawlerPort:
    """Собирает краулер для ``passport-scout`` (дефолт — лёгкий, A1/MVP).

    Обычный сайт = ``httpx`` (:class:`HttpCrawler`, ленивый импорт); браузерный
    Scrapy/playwright — опция для SPA. Выбор — через env
    :data:`ENV_SCOUT_CRAWLER` ∈ :data:`SUPPORTED_SCOUT_CRAWLERS`:

    * ``http`` (дефолт) — :class:`HttpCrawler`;
    * ``scrapy`` — :class:`ScrapyCrawler` (прежнее поведение).

    Недоступность зависимости даёт понятный ``CrawlError`` на фетче (адаптер не
    падает на конструкторе). Тесты инъектируют ``crawler`` в :func:`main`.

    :raises WiringError: неизвестное значение ``COHOUND_SCOUT_CRAWLER``.
    """
    raw = os.environ.get(ENV_SCOUT_CRAWLER)
    name = (raw if raw is not None else DEFAULT_SCOUT_CRAWLER).strip().lower()
    if not name:
        name = DEFAULT_SCOUT_CRAWLER
    if name == "http":
        return HttpCrawler()
    if name == "scrapy":
        return ScrapyCrawler()
    raise WiringError(
        f"{ENV_SCOUT_CRAWLER}: неизвестный краулер {name!r}; "
        f"поддерживаются: {', '.join(SUPPORTED_SCOUT_CRAWLERS)}"
    )


#: Формат документа → расширение (роутинг композитного экстрактора на docling).
_DOC_SUFFIX_BY_CONTENT_TYPE: dict[str, str] = {
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
}


def _document_filename(page: RawPage) -> str | None:
    """Имя файла с документным расширением, если формат страницы обрабатывает docling.

    Формат выводится из URL (суффикс пути), затем из ``content_type``. Возврат ``None``
    означает, что страницу обрабатывает HTML-путь (trafilatura).
    """
    suffix = Path(urlsplit(page.url).path).suffix.lower()
    if suffix and DoclingDocumentExtractor.supports(f"document{suffix}"):
        return f"document{suffix}"
    content_type = (page.content_type or "").split(";")[0].strip().lower()
    doc_suffix = _DOC_SUFFIX_BY_CONTENT_TYPE.get(content_type)
    return f"document{doc_suffix}" if doc_suffix is not None else None


class _LocalExtractor:
    """Композит локальной экстракции: документы (docling) поверх HTML (trafilatura).

    Реализует ``ExtractorPort`` структурно. Документные форматы (pdf/docx/pptx/xlsx)
    уходят в :class:`DoclingDocumentExtractor`, если тот доступен; всё прочее — в
    :class:`HtmlExtractor`. Оба адаптера деградируют грациозно (``ExtractedDoc.error``),
    поэтому композит не бросает исключений на уровне извлечения.
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

    def extract(self, page: RawPage) -> ExtractedDoc:
        """``RawPage`` → ``ExtractedDoc``; документный формат → docling, иначе HTML."""
        if self._documents is not None:
            filename = _document_filename(page)
            if filename is not None:
                return self._documents.extract_bytes(page.content, filename)
        return self._html.extract(page)


def _build_local_ports(
    *,
    evaluator: EvaluatorPort | None,
    registry: RegistryFetcherPort | None,
    index_root: str | Path,
    package: str | None,
) -> Ports:
    """Собирает in-process порты профиля ``local`` (без сети/тяжёлых зависимостей).

    Гарантия: никогда не бросает из-за отсутствия опциональной зависимости — недоступный
    тяжёлый адаптер остаётся ``None``. ``evaluator``/``registry`` не создаются сами:
    передаются вызывающим (генератор/конфиг; I6/I7/I18 не автоматизируются).
    """
    html = HtmlExtractor()
    documents = DoclingDocumentExtractor()
    # Без docling ``extractor`` — ровно ``HtmlExtractor``; при наличии docling
    # документные форматы (pdf/docx/pptx/xlsx) уходят в него через композит.
    extractor: ExtractorPort = (
        _LocalExtractor(html, documents) if documents.available else html
    )

    # Тяжёлые опциональные адаптеры: только при выполненной availability-предикате.
    embedder = (
        LocalEmbedder() if _module_installed("sentence_transformers") else None
    )
    index_builder = (
        NpyIndexBuilder(index_root=index_root, package=package)
        if numpy_available()
        else None
    )
    lexical_index = TantivyLexicalIndex() if tantivy_available() else None

    return Ports(
        evaluator=evaluator,
        registry=registry,
        chunker=MarkdownChunkerPort(),
        extractor=extractor,
        embedder=embedder,
        index_builder=index_builder,
        # Adapter/port mismatch (engine_version: property vs mutable attr) — статическая
        # несовместимость протокола; runtime_checkable проверяет методы, поведение корректно.
        lexical_index=lexical_index,  # type: ignore[arg-type]
        # Сетевые адаптеры (crawl/SERP) для local не инстанцируются: в CLI-конфиге нет
        # ни URL-набора, ни учётных данных, ни discovery-запросов. Включаются явной
        # инъекцией портов вызывающим (composition root, W13/T13).
        crawler=None,
        search=None,
        vector_index=None,
    )


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
    index_root: str | Path = "index_store",
    package: str | None = None,
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
        конструируется автоматически: сценарий evaluator требует self-host backend).
    :param registry: готовый реестровый фетчер (``local``); ``None`` → остаётся ``None``
        (конфиг реестра — данные вызывающего).
    :param index_root: корень индексного хранилища для ``NpyIndexBuilder`` (``local``).
    :param package: пакет по умолчанию для ``NpyIndexBuilder`` (``local``).
    :raises WiringError: неизвестный профиль или split без валидного deploy-конфига.
    """
    if profile == LOCAL_PROFILE:
        return _build_local_ports(
            evaluator=evaluator,
            registry=registry,
            index_root=index_root,
            package=package,
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
        return Ports(
            crawler=RemoteCrawler(content_gateway),
            search=RemoteSearch(
                content_gateway,
                adapter_kind=(config.search.adapter or "remote"),
                provider="edge",
            ),
            extractor=RemoteExtractor(content_gateway),
            registry=RemoteRegistry(content_gateway, registry_id=registry_id),
        )
    raise WiringError(f"неизвестный профиль развёртывания: {profile!r}")


def manifest_deploy_fields(
    deploy: DeployConfig, capabilities: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Поля развёртывания для ``run_manifest.yaml`` (R7; ``07`` §7).

    Возвращает сведения, которые кладутся в ``RunManifest.edge_capabilities`` —
    единственное существующее поле манифеста для версий edge:

    * ``deploy_profile`` — выбранный профиль зон (``local``/``split``/``split-hardened``);
    * ``edge_schema_version`` — версия контракта шва (версионная граница §13);
    * версии edge-компонентов из ``/v1/capabilities`` (если переданы).

    **ГЭП (T13.4).** У доменного ``RunManifest`` (``core/domain/models.py``) нет
    отдельных полей ``deploy_profile`` и ``edge_schema_version``; ``SqliteStore.
    read_manifest`` не заполняет ``edge_capabilities``. Правка доменной модели/стора
    вне write-scope T13.4, поэтому поля возвращаются этим хелпером и складываются в
    ``edge_capabilities`` вызывающей стороной (манифест-писатель W10/T10.4).
    """
    fields: dict[str, Any] = {
        "deploy_profile": deploy.profile,
        "edge_schema_version": DEFAULT_SCHEMA_VERSION,
    }
    if capabilities:
        fields.update(dict(capabilities))
    return fields


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
    реестр ``Runner`` (echo-заглушки + реальные проверки гейтов). Порты, если не переданы,
    собираются :func:`build_adapters` по профилю.

    :raises WiringError: неизвестный/нереализованный профиль (см. :func:`build_adapters`).
    """
    resolved_ports = ports if ports is not None else build_adapters(profile=profile)
    return Runner(store, ports=resolved_ports, clock=clock, phases=phases)
