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
from core.adapters.llm_local import (
    DEFAULT_DECODER as EVALUATOR_DEFAULT_DECODER,
    DEFAULT_MODEL_ID as EVALUATOR_DEFAULT_MODEL_ID,
    DEFAULT_TIMEOUT as EVALUATOR_DEFAULT_TIMEOUT,
    GenerateFn as EvaluatorGenerateFn,
    LocalEvaluator,
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
from core.pipeline import Ports, Runner
from core.pipeline.runner import PHASES, Clock, PhaseFn
from core.ports.crawler import CrawlerPort
from core.ports.discovery_llm import DiscoveryLLMPort
from core.ports.extractor import ExtractorPort
from core.ports.gateway import ContentGateway
from core.ports.llm import EvaluatorPort
from core.ports.registry import RegistryFetcherPort
from core.ports.search import SearchAdapterPort
from core.ports.types import ExtractedDoc, ExtractionRoute, RawPage
from core.store.sqlite_store import SqliteStore
from core.util.cache import ContentAddressedCache

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
    "PHASES",
    "WiringError",
    "run_db_path",
    "build_store",
    "build_gateway",
    "build_search_adapter",
    "build_discovery_llm",
    "build_evaluator_llm",
    "build_scout_crawler",
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
#: Env-переменная активного LLM-слота (``cheap``/``flagship``; ``--llm-slot`` переопределяет).
ENV_LLM_SLOT = "COHOUND_LLM_SLOT"

#: Дефолтный base_url OI-совместимого сервера LLM. Пусто → endpoint не сконфигурирован:
#: оператор обязан явно задать внутренний [OI]-совместимый API корпоративного периметра
#: (внешние хосты не используются).
DEFAULT_LLM_BASE_URL = ""
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


#: Env-переменная выбора краулера ``passport-scout``: ``http`` (дефолт) | ``scrapy``.
ENV_SCOUT_CRAWLER = "COHOUND_SCOUT_CRAWLER"
#: Дефолтный (лёгкий) краулер scout — обычный HTTP без браузера (A1, MVP).
DEFAULT_SCOUT_CRAWLER = "http"
#: Поддерживаемые краулеры scout: лёгкий ``http`` и браузерный ``scrapy``.
SUPPORTED_SCOUT_CRAWLERS: tuple[str, ...] = ("http", "scrapy")


def build_scout_crawler(
    *,
    deploy: DeployConfig | None = None,
    cache: ContentAddressedCache | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
) -> CrawlerPort:
    """Собирает краулер для ``passport-scout`` (дефолт — лёгкий, A1/MVP).

    Обычный сайт = ``httpx`` (:class:`HttpCrawler`, ленивый импорт); браузерный
    Scrapy/playwright — опция для SPA. Выбор — через env
    :data:`ENV_SCOUT_CRAWLER` ∈ :data:`SUPPORTED_SCOUT_CRAWLERS`:

    * ``http`` (дефолт) — :class:`HttpCrawler`;
    * ``scrapy`` — :class:`ScrapyCrawler` (браузерный рендер).

    Для ``http`` применяется операционная политика :class:`~core.config.deploy.CrawlConfig`
    из ``deploy.yaml`` (``user_agent``/``timeout_s``/``follow_redirects``). ``deploy=None``
    → конфиг загружается канонически (:func:`load_deploy_config`); при профиле ``local``
    без файла значения совпадают с дефолтами :class:`HttpCrawler` — поведение не меняется
    (обратная совместимость). Недоступность зависимости даёт понятный ``CrawlError`` на
    фетче (адаптер не падает на конструкторе). Тесты инъектируют ``crawler`` в :func:`main`.

    **Исполнимость (D2).** Лёгкий ``HttpCrawler`` не умеет ``fetch: browser``. Проверка
    исполнимости **не** делается здесь: иначе ``hound run`` падал бы на сборке композиции
    даже для ``--until plan`` и для наборов URL, не задевающих browser-домены. Она
    URL-скоупедится в pipeline (:func:`core.pipeline.corpus.build_corpus`): browser-URL при
    HTTP-краулере пропускается с пофURL-ошибкой, не роняя фазу. Выбор ``http``/``scrapy`` —
    только env :data:`ENV_SCOUT_CRAWLER`.

    **R7-кэш.** ``cache`` — глобальный content-addressed кэш (``core.util.cache``,
    ADR 0016), протягивается в краулер. План (``FetchPlan``) резолвится в pipeline
    (:func:`core.pipeline.fetch_plan.resolve_fetch_plan`), поэтому профили сюда не передаются.

    :param deploy: типизированный deploy-конфиг (политика ``crawl``); ``None`` → канон.
    :param cache: глобальный кэш внешних вызовов; ``None`` → без кэша.
    :param domain_profiles: зарезервирован для совместимости сигнатуры; исполнимость
        профилей проверяется URL-скоупедированно в pipeline (D2).
    :raises WiringError: неизвестное значение ``COHOUND_SCOUT_CRAWLER``.
    """
    raw = os.environ.get(ENV_SCOUT_CRAWLER)
    name = (raw if raw is not None else DEFAULT_SCOUT_CRAWLER).strip().lower()
    if not name:
        name = DEFAULT_SCOUT_CRAWLER
    if name == "http":
        config = deploy if deploy is not None else load_deploy_config(None)
        return HttpCrawler(**_http_crawler_kwargs(config), cache=cache)
    if name == "scrapy":
        return ScrapyCrawler(cache=cache)
    raise WiringError(
        f"{ENV_SCOUT_CRAWLER}: неизвестный краулер {name!r}; "
        f"поддерживаются: {', '.join(SUPPORTED_SCOUT_CRAWLERS)}"
    )


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
    index_root: str | Path,
    package: str | None,
    crawler: CrawlerPort | None = None,
) -> Ports:
    """Собирает in-process порты профиля ``local`` (без сети/тяжёлых зависимостей).

    Гарантия: никогда не бросает из-за отсутствия опциональной зависимости — недоступный
    тяжёлый адаптер остаётся ``None``. ``evaluator``/``registry`` не создаются сами:
    передаются вызывающим (генератор/конфиг; I6/I7/I18 не автоматизируются).
    ``crawler`` — опциональный HTTP-краулер корпуса, собранный composition root из
    ``deploy.crawl`` (:func:`build_scout_crawler`); ``None`` → не инстанцируется (прежнее
    поведение, сетевые адаптеры включаются явной инъекцией).
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
        # Краулер корпуса: инстанцируется composition root'ом из ``deploy.crawl`` (R7/03
        # §D.5); без конфига остаётся ``None`` — прежнее поведение. SERP/discovery для
        # local включаются явной инъекцией портов (в CLI-конфиге нет запросов).
        crawler=crawler,
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
    cache: ContentAddressedCache | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
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
        (конфиг реестра — данные вызывающего).
    :param index_root: корень индексного хранилища для ``NpyIndexBuilder`` (``local``);
        composition root передаёт resolved ``paths.index_store`` (``03`` §A.1); дефолт
        ``"index_store"`` совпадает с дефолтом :class:`~core.config.paths.PathsConfig`.
    :param package: пакет по умолчанию для ``NpyIndexBuilder`` (``local``).
    :param cache: глобальный content-addressed кэш (R7, ADR 0016); протягивается в
        локальный краулер корпуса (``local``).
    :param domain_profiles: типизированные доменные профили (``domain_profiles.yaml``,
        ``03`` §D.2); используются локальным краулером для fail-fast проверки ``fetch: browser``
        (план резолвится в pipeline).
    :raises WiringError: неизвестный профиль или split без валидного deploy-конфига.
    """
    if profile == LOCAL_PROFILE:
        # Краулер корпуса строится только при явном deploy-конфиге: политика crawl
        # (user_agent/timeout/follow_redirects) — данные оператора (03 D.5/D.6). Без
        # конфига — прежнее поведение (crawler=None; сетевые порты инъектируются).
        local_crawler = (
            build_scout_crawler(
                deploy=deploy, cache=cache, domain_profiles=domain_profiles
            )
            if deploy is not None
            else None
        )
        return _build_local_ports(
            evaluator=evaluator,
            registry=registry,
            index_root=index_root,
            package=package,
            crawler=local_crawler,
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
