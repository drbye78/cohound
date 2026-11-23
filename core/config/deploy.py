"""coHound — загрузчик конфига развёртывания (``config/deploy.yaml``, 07 §7 / 03 D.5, W13/T13.4).

Развёртывание отделено от методологического прогона (``run.yaml``): этот файл выбирает
**профиль зон** (``local`` | ``split`` | ``split-hardened``) и адреса DMZ-шва (edge):
base_url, TLS, аутентификация входящих, лимиты, экспорт аудита, зона экстрактора,
бюджет/адаптер внешнего поиска и **конфиг LLM-слотов** (``llm:``, commodity/execution-слой
§7.4: ``cheap``/``flagship``, активный slot выбирается в рантайме). Модели обслуживаются
OI-совместимым API корпоративного облака — это **не** self-host инсталляция; vLLM/SGLang —
бэкенд облака, не наш стек.

Модуль только **парсит и типизирует** значения (пути, адреса, лимиты) в
:class:`DeployConfig`. Семантика зон/подстановки адаптеров — дело composition root
(``cli`` для Z0, ``edge`` для Z1); конвейер о зонах не знает (07 §4). Версии edge и
``edge_schema_version`` для манифеста R7 — **не** задача этого модуля.

Три секции описывают **условия измерения, которые объявлены на уровне развёртывания**,
а не зашиты в код исполнения: ``retrieval.profile`` (профиль ретривала, I16),
``lexical.profile`` (профиль анализатора лексического индекса) и ``extract.cache_max_bytes``
(операционный бюджет кэша экстракции). Первые две входят в контракт прогона
(``EXECUTION_CONTRACT_FIELDS``), третья — **нет** и входить не должна: бюджет кэша меняет
только цену повторного извлечения, а не состав корпуса и не вердикты.

Безопасность: загрузчик не логирует содержимое и не печатает секреты; ``client_key`` —
это **путь** к файлу ключа, а не сам ключ. Доверие к TLS — через корпоративный CA;
никакого certificate pinning (07 §8.1) — это осознанно отсутствует в модели.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров; строка
адаптера поиска — просто данные. Допустимые зависимости слоя ``config`` —
``domain``/``config``/``util`` (01 §1).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.domain.enums import (
    ArchiveDescent,
    Decoder,
    DocumentLevel,
    EmbedDevice,
    EmbedPrecision,
    PdfDevice,
    PdfPipeline,
    ReasoningEffort,
)
from core.domain.models import (
    DEFAULT_ARCHIVE_MAX_MEMBER_BYTES,
    DEFAULT_ARCHIVE_MAX_TOTAL_UNCOMPRESSED_BYTES,
    DEFAULT_ARCHIVE_MAX_UNCOMPRESSED_BYTES,
    DEFAULT_MAX_ARTIFACT_BYTES,
    DEFAULT_MAX_RESPONSE_BYTES,
)
from core.util.document_formats import (
    SUPPORTED_ARCHIVE_FORMATS,
    normalize_attachment_formats,
)
from core.util.rate_limit import normalize_host

__all__ = [
    "DEFAULT_LLM_MODEL_ID",
    "DEFAULT_LLM_SLOT",
    "ENV_NODE",
    "ENV_SERP",
    "LEXICAL_PROFILES",
    "LLM_SLOTS",
    "QUERY_LANGS",
    "RETRIEVAL_PROFILES",
    "SPLIT_HARDENED_WARNING",
    "AuditConfig",
    "AuthConfig",
    "CrawlBrowserConfig",
    "CrawlConfig",
    "CrawlShardConfig",
    "DeployConfig",
    "EdgeConfig",
    "EmbedConfig",
    "ExtractDeployConfig",
    "HostShard",
    "LLMDeployConfig",
    "LLMSlotConfig",
    "LexicalDeployConfig",
    "OIDCConfig",
    "RetrievalDeployConfig",
    "SearchDeployConfig",
    "SerpDeployConfig",
    "TLSConfig",
    "ToolsDeployConfig",
    "is_split",
    "load_deploy_config",
    "parse_serp_switch",
    "profile_notices",
    "resolve_host_shard",
    "resolve_llm_slot",
    "resolve_search_provider",
    "resolve_serp_rounds",
    "shard_notices",
]

# Канонические допустимые значения (ЗЕРКАЛО 07 §7 / 03 D.5). Не выносим в доменные
# enum'ы: это конфигурация развёртывания, а не доменная методология.
# D-8: ``split-hardened`` принимается ради совместимости конфига, но поведения не
# добавляет (эквивалент ``split``); видимое предупреждение — :func:`profile_notices`.
PROFILES: tuple[str, ...] = ("local", "split", "split-hardened")
SPLIT_PROFILES: frozenset[str] = frozenset({"split", "split-hardened"})
AUTH_KINDS: tuple[str, ...] = ("mtls", "oidc")
EXTRACTOR_ZONES: tuple[str, ...] = ("edge", "core", "separate")
AUDIT_SINKS: tuple[str, ...] = ("file", "syslog")
#: Режимы подключения браузерного краулера (07): внешний браузер по CDP либо
#: персистентный профиль, которым владеет Playwright.
CRAWL_BROWSER_MODES: tuple[str, ...] = ("cdp", "persistent")
#: Профили ретривала (I16) — **зеркало** ``core.pipeline.retrieve.RetrievalProfile``.
#: Зеркало, а не импорт: слой ``config`` не имеет права зависеть от ``pipeline``
#: (матрица 01 §1 гейтится ``tests/selfcheck.py::_import_graph``), а профиль ретривала
#: объявлен рядом с ретривалом. Расхождение ловит тест
#: ``tests/config/test_deploy.py::test_retrieval_profiles_mirror_the_engine_enum``.
RETRIEVAL_PROFILES: tuple[str, ...] = ("R0", "R1", "R2")
#: Язык запросов ретривала — **объявленная ось** (``query_lang`` → ``run_id``).
#:
#: Зачем ось, а не константа. До 19.11.2026 порядок языков был зашит
#: (``prompt._QUERY_LANG_ORDER = ("ru", "en")``) при бюджете ``max_queries = 3``, поэтому
#: в запрос попадали ``title`` и два **русских** термина, а английские не доходили
#: никогда. На английском reference-корпусе это решало исход отбора: замер 19.11.2026 —
#: ``present`` 13 (**``ru``**) / 37 (**``all``**) / 42 (**``en``**); на русской когорте
#: порядок обратный (hit@10 0.9211 / 0.8947 / 0.8509 на 114 ячейках эталона).
#: Язык корпуса — свойство **профиля**, а не кода: корпус когорты русский, корпус
#: reference-линии английский, поэтому значение объявляется, а не выводится.
#:
#: ``all`` — прежнее поведение (``ru`` → ``en`` → прочие языки): оно остаётся значением
#: по умолчанию **резолвера**, чтобы чистые вызовы не меняли результат молча, но живой
#: прогон объявляет язык явно. ``auto`` отсутствует намеренно (A3/R5): ось, выведенная из
#: догадки о корпусе, — не объявленный вход.
QUERY_LANGS: tuple[str, ...] = ("ru", "en", "all")
#: Профили анализатора лексического индекса — **зеркало** ``_PROFILES`` из
#: ``core.adapters.lexical_tantivy`` (та же причина: ``config`` не зависит от ``adapters``).
#: ``raw`` — псевдоним ``identifier``; в объявленном значении он нормализуется к
#: ``identifier``, чтобы одно поведение не имело двух имён в контракте.
LEXICAL_PROFILES: tuple[str, ...] = ("stemmed", "identifier", "raw", "ngram")

#: Именованные слоты моделей LLM (commodity/execution-слой §7.4). Активный slot
#: выбирается в рантайме (``--llm-slot`` / ``COHOUND_LLM_SLOT``), см. :func:`resolve_llm_slot`.
LLM_SLOTS: tuple[str, ...] = ("cheap", "flagship")
#: Безопасный дефолт активного слота (лёгкая модель).
DEFAULT_LLM_SLOT = "cheap"
#: Дефолтный ``model_id`` слотов (данные; обе слоты по умолчанию на одну модель).
#: Значение **провизионное**: это идентификатор, реально используемый сегодня, а не
#: результат измерения. Финальный выбор модели оценщика даёт
#: ``hound experiment evaluator-benchmark`` (A3); смена — граница ``execution_versions``
#: (``execution_change``, §13), а не ``methodology_version``.
DEFAULT_LLM_MODEL_ID = "deepseek-v4.1-flash-1017:yellow"

# Путь по умолчанию относительно рабочего каталога (раскладка A.1).
DEFAULT_DEPLOY_PATH: Path = Path("config/deploy.yaml")


# ---------------------------------------------------------------------------
# Низкоуровневые помощники
# ---------------------------------------------------------------------------


def _read_yaml(path: Path) -> Any:
    """Читает YAML-файл; ошибки I/O и синтаксиса приводит к ``ValueError``."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:  # noqa: BLE001 — понятная ошибка важнее типа
        raise ValueError(f"не удалось прочитать {path}: {exc}") from exc
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"некорректный YAML в {path}: {exc}") from exc


def _as_mapping(data: Any, where: str) -> dict[str, Any]:
    """Проверяет, что документ — непустой YAML-словарь."""
    if data is None:
        raise ValueError(f"{where}: пустой документ")
    if not isinstance(data, dict):
        raise ValueError(f"{where}: ожидался YAML-словарь, получено {type(data).__name__}")
    return data


# ---------------------------------------------------------------------------
# Модели конфига
# ---------------------------------------------------------------------------


class TLSConfig(BaseModel):
    """TLS-материалы шва (07 §7/§8.1).

    Все поля — **пути** к файлам (корпоративный CA, клиентский сертификат ядра, ключ).
    Certificate pinning сознательно не поддерживается: доверие строится на CA
    (в т.ч. TLS break-and-inspect корпоративных DLP/IDS), см. 07 §8.1.
    """

    model_config = ConfigDict(extra="forbid")

    ca: str | None = None
    client_cert: str | None = None
    client_key: str | None = None


class AuthConfig(BaseModel):
    """Аутентификация входящих на шве: mTLS workload-identity или OIDC/JWT (07 §8.2).

    В YAML допускается компактная скалярная форма ``auth: mtls`` (03 D.5) — она
    нормализуется в модель ``{kind: "mtls"}``.
    """

    model_config = ConfigDict(extra="forbid")

    kind: str

    @model_validator(mode="before")
    @classmethod
    def _coerce_scalar(cls, data: Any) -> Any:
        """Принимает скаляр ``mtls``/``oidc`` наравне со словарём ``{kind: …}``."""
        if isinstance(data, str):
            return {"kind": data}
        return data

    @field_validator("kind")
    @classmethod
    def _check_kind(cls, value: str) -> str:
        kind = value.strip()
        if kind not in AUTH_KINDS:
            raise ValueError(
                f"auth: недопустимый способ аутентификации «{value}»; "
                f"ожидается один из {list(AUTH_KINDS)}"
            )
        return kind


class OIDCConfig(BaseModel):
    """Параметры OIDC/JWT при ``auth: oidc`` (issuer/audience корпоративного IDP, 07 §7/§8.2)."""

    model_config = ConfigDict(extra="forbid")

    issuer: str
    audience: str


class AuditConfig(BaseModel):
    """Экспорт аудита шва в SIEM (07 §8.4). Механизм — файл или syslog."""

    model_config = ConfigDict(extra="forbid")

    sink: str | None = None
    target: str | None = None

    @field_validator("sink")
    @classmethod
    def _check_sink(cls, value: str | None) -> str | None:
        if value is None:
            return None
        sink = value.strip()
        if sink not in AUDIT_SINKS:
            raise ValueError(
                f"audit.sink: недопустимый сток «{value}»; ожидается один из {list(AUDIT_SINKS)}"
            )
        return sink


class EdgeConfig(BaseModel):
    """Адреса и параметры DMZ-шва (edge). Для профиля ``local`` игнорируется."""

    model_config = ConfigDict(extra="forbid")

    base_url: str | None = None
    tls: TLSConfig = Field(default_factory=TLSConfig)
    auth: AuthConfig | None = None
    oidc: OIDCConfig | None = None
    timeout_s: float = Field(default=30.0, gt=0)
    max_retries: int = Field(default=2, ge=0)
    max_response_bytes: int = Field(default=52_428_800, gt=0)
    audit: AuditConfig = Field(default_factory=AuditConfig)


class SerpDeployConfig(BaseModel):
    """Операционный дефолт SERP-контура (``deploy.search.serp``).

    Это **низший** уровень переключения (``CLI > env > run.yaml > deploy.yaml >
    default``): профиль развёртывания объявляет, работает ли контур на этой инсталляции
    по умолчанию. Значение ``0`` (дефолт) — контур выключен.

    Бюджета здесь **нет** намеренно: бюджет — объявленный вход прогона
    (``run.yaml: serp.budget``), потому что он определяет число ушедших запросов, то есть
    влияет на корпус. Два бюджетных словаря на одну вещь — источник тихого безлимита
    (прежняя секция ``search.budget`` ключевалась по языку, тогда как
    :class:`~core.pipeline.search_log.SearchBudget` ключуется по фазе, и не ограничивала
    ничего).
    """

    model_config = ConfigDict(extra="forbid")

    rounds: int = Field(default=0, ge=0)


class SearchDeployConfig(BaseModel):
    """Адаптер внешнего поиска (07 §7 / D.5).

    **Провайдер поиска.** ``adapter`` — провайдер **по умолчанию**; ``by_language`` —
    детерминированное переопределение по языку (география-прокси), например
    ``{ru: yandex, en: glm}``. Выбор из ``by_language`` — функция **от языка**,
    не от продукта, поэтому симметричный протокол I16 сохраняется. Имена провайдеров —
    свободные строки-данные: валидация против реестра (``SUPPORTED_SEARCH_ADAPTERS``)
    живёт в composition root (CLI), а не в config-слое (§1.3, 01 §1).

    **Симметрия I16** — методологическое правило конвейера; конфиг лишь парсит значения.
    Прежнее поле ``budget: {ru: 5, en: 5}`` **удалено**: оно ключевалось по языку, тогда
    как фактический потребитель (:class:`~core.pipeline.search_log.SearchBudget`)
    ключуется по фазе, и производственного вызова ``SearchBudget.from_config`` не было —
    то есть конфиг не ограничивал ничего. Единственный бюджет контура — объявленный вход
    прогона (``run.yaml: serp.budget``).
    """

    model_config = ConfigDict(extra="forbid")

    adapter: str | None = None
    by_language: dict[str, str] = Field(default_factory=dict)
    serp: SerpDeployConfig = Field(default_factory=SerpDeployConfig)

    @field_validator("by_language")
    @classmethod
    def _check_by_language(cls, value: dict[str, str]) -> dict[str, str]:
        """Ключи-языки и значения-провайдеры обязаны быть непустыми (данные-конфиг)."""
        for lang, provider in value.items():
            if not str(lang).strip():
                raise ValueError("search.by_language: пустой код языка")
            if not str(provider).strip():
                raise ValueError(
                    f"search.by_language[{lang}]: пустое имя провайдера поиска"
                )
        return value


class CrawlBrowserConfig(BaseModel):
    """Параметры браузерного краулинга корпуса (07).

    Операционный выбор оператора для браузерного краулера (SPA/JS-контент). ``mode`` —
    **декларативный** селектор ветки подключения:

    * ``"cdp"`` — подключение к **внешнему** процессу браузера по CDP (``cdp_url``);
      профиль/сессия принадлежат этому процессу; ``user_data_dir`` запрещён;
    * ``"persistent"`` — персистентный профиль (``user_data_dir``); ``cdp_url`` запрещён;
    * ``None`` — **не сконфигурировано** (плейсхолдер): перекрёстных требований нет.

    Фактическую ветку подключения выводит композиция/адаптер **по наличию**
    ``cdp_url``/``user_data_dir``; ``mode`` здесь лишь декларирует намерение и не
    является источником истины для рантайма (принятое проектное решение: адаптер
    ветвится по данным, а не по этой строке). Значения нормализуются (``strip``); пустые
    строки приводятся к ``None``. ``mode=None`` делает плейсхолдер **явным значением** —
    модель идемпотентна под ``model_dump()→model_validate()`` (persist/reload,
    ``model_copy``, JSON-схема), в отличие от неявного «поле не задавалось».
    ``channel`` — свободная строка-данные (канал установленного браузера), не предметная
    категория (§1.3).
    """

    model_config = ConfigDict(extra="forbid")

    mode: str | None = None
    cdp_url: str | None = None
    user_data_dir: str | None = None
    headless: bool = False
    channel: str | None = None
    timeout_ms: int = Field(default=30000, gt=0)

    @field_validator("mode")
    @classmethod
    def _check_mode(cls, value: str | None) -> str | None:
        """``None`` допустим (не сконфигурировано); иначе — из :data:`CRAWL_BROWSER_MODES`."""
        if value is None:
            return None
        mode = value.strip()
        if mode not in CRAWL_BROWSER_MODES:
            raise ValueError(
                f"crawl.browser.mode: недопустимый режим «{value}»; "
                f"ожидается один из {list(CRAWL_BROWSER_MODES)}"
            )
        return mode

    @field_validator("cdp_url", "user_data_dir", "channel")
    @classmethod
    def _empty_to_none(cls, value: str | None) -> str | None:
        """Пустые/пробельные строки нормализует в ``None`` (как соседние конфиги)."""
        if value is None:
            return None
        return value.strip() or None

    @model_validator(mode="after")
    def _check_mode_requirements(self) -> CrawlBrowserConfig:
        """Требования режима — явная функция от ``mode`` (идемпотентно под dump→validate).

        ``mode is None`` — плейсхолдер «не сконфигурировано»: требований нет. ``cdp``
        требует непустой ``cdp_url`` и запрещает ``user_data_dir``; ``persistent``
        требует непустой ``user_data_dir`` и запрещает ``cdp_url``.
        """
        if self.mode is None:
            return self
        if self.mode == "cdp":
            if not self.cdp_url:
                raise ValueError("crawl.browser: режим cdp требует непустой cdp_url")
            if self.user_data_dir is not None:
                raise ValueError(
                    "crawl.browser: режим cdp несовместим с user_data_dir (взаимоисключение)"
                )
        else:  # persistent (mode уже провалидирован против CRAWL_BROWSER_MODES)
            if not self.user_data_dir:
                raise ValueError(
                    "crawl.browser: режим persistent требует непустой user_data_dir"
                )
            if self.cdp_url is not None:
                raise ValueError(
                    "crawl.browser: режим persistent несовместим с cdp_url (взаимоисключение)"
                )
        return self


class CrawlShardConfig(BaseModel):
    """Один узел кластера краула и его **host'ы** (17 §2, F7 плана).

    Единица шардирования — **host**, и он же ключ вежливости
    (:func:`core.util.rate_limit.normalize_host`): если host'ы разъезжаются по узлам
    произвольно, два процесса получат по независимому бюджету на один сайт, и политика
    §10 нарушится без единой ошибки в коде. Поэтому каждый host принадлежит **ровно
    одному** узлу (дизъюнктность проверяет :class:`CrawlConfig`).

    Host'ы нормализуются **тем же правилом, что лимитер** (нижний регистр, без схемы,
    порта и завершающей точки): иначе ``Example.COM`` в шарде и ``example.com`` во
    frontier — разные ключи, и «дизъюнктно по построению» было бы ложью. ``node`` —
    операционный идентификатор процесса (не измерение): он попадает в провенанс сеанса,
    но **не** в ``run_id``.
    """

    model_config = ConfigDict(extra="forbid")

    node: str
    hosts: list[str] = Field(default_factory=list)

    @field_validator("node")
    @classmethod
    def _check_node(cls, value: str) -> str:
        """Идентификатор узла — непустая строка без пробелов по краям."""
        node = str(value).strip()
        if not node:
            raise ValueError("crawl.shards: пустой идентификатор узла (node)")
        return node

    @field_validator("hosts", mode="before")
    @classmethod
    def _check_hosts(cls, value: object) -> list[str]:
        """Нормализует host'ы правилом лимитера и отвергает пустые/повторные.

        Пустой шард (``hosts: []``) допустим как **объявление** «узел ничего не ведёт»:
        это видимая запись (``shard_notices``/провенанс), а не ошибка конфига. Пустая
        строка в списке — ошибка: она не host и молча ничего не значит.
        """
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, (list, tuple)):
            raise ValueError(
                "crawl.shards[].hosts: ожидался список host'ов"
            )
        hosts: list[str] = []
        for item in value:
            host = normalize_host(str(item))
            if not host:
                raise ValueError(
                    f"crawl.shards[].hosts: пустой host в {list(value)!r}"
                )
            if host in hosts:
                raise ValueError(
                    f"crawl.shards[].hosts: host «{host}» объявлен дважды в одном шарде"
                )
            hosts.append(host)
        return hosts


class CrawlConfig(BaseModel):
    """Операционная политика краулинга корпуса (03 D.5/D.6).

    Секция покрывает и лёгкий :class:`~core.adapters.crawl_http.HttpCrawler`
    (``user_agent``, ``timeout_s`` — таймаут запроса в секундах, ``follow_redirects``),
    и браузерный краулер (``browser``, см. :class:`CrawlBrowserConfig`) для SPA/JS.
    Значения **не** дублируют возможности адаптера, а лишь выносят их в конфиг оператора
    (A1). Поля со значением ``None`` означают «взять дефолт адаптера/инфраструктуры» —
    так сохраняется обратная совместимость профиля ``local`` без файла. Поле ``browser``
    по умолчанию — валидный плейсхолдер «браузерный краулинг не сконфигурирован».
    """

    model_config = ConfigDict(extra="forbid")

    user_agent: str | None = None
    timeout_s: float | None = Field(default=None, gt=0)
    follow_redirects: bool = True
    browser: CrawlBrowserConfig = Field(default_factory=CrawlBrowserConfig)
    # D1/D2: оси контроля краула (application default; ``deploy.yaml:crawl``). Разрешаются
    # цепочкой app < run (``run.yaml:crawl``) < site (``domain_profiles.yaml``) в
    # :func:`core.pipeline.crawl_frontier.resolve_crawl_limits`. ``max_depth`` дефолт 0 =
    # текущее поведение (только seeds); ``max_documents=0`` отвергается (пустой корпус —
    # конфиг-ошибка, не молчаливый ноль); ``exclude_*`` — glob (путь / basename).
    max_workers: int | None = Field(default=None, ge=1)
    # F1 (план 17): application-слой оси «сколько host'ов процесс краула ведёт
    # одновременно». ``None``/``1`` = сегодняшнее поведение. Per-site слоя у оси нет:
    # число полос — свойство узла, а не сайта (как у ``pdf_device``).
    host_concurrency: int | None = Field(default=None, ge=1)
    #: Сколько **браузерных движков** (по одному на поток) обслуживают браузерный фетч
    #: этого процесса. ``None``/``1`` — сегодняшнее поведение: один движок в одном потоке
    #: (Playwright sync-API не переносим между потоками, поэтому каждый движок живёт в
    #: своём потоке). Цепочка app < run < site: у этой оси есть per-site слой (в отличие
    #: от ``host_concurrency``). Объявленное значение входит в ``crawl_config_digest`` —
    #: параллелизм меняет нагрузку на хост и wall time, то есть условие измерения.
    browser_workers: int | None = Field(default=None, ge=1)
    # F6 (план 17, §4.3): application-слой watermark бэклога конверсии
    # (``crawl.extract_backlog_mb``, МБ retained-файлов очереди). ``None`` — предел не
    # объявлен. Per-site слоя нет: бэклог — свойство узла (диск и потребитель), а не
    # сайта, как у ``host_concurrency``.
    extract_backlog_mb: int | None = Field(default=None, gt=0)
    #: F7 (17 §2, «Эксклюзивность host'а — статическим шардом»): реестр узлов кластера
    #: краула — **объявленный конфиг развёртывания**, а не сервис. ``None`` — шардов нет
    #: (один процесс ведёт все host'ы, поведение байт-в-байт прежнее). Объявленная карта
    #: входит в ``crawl_config_digest`` (``CrawlLimits.as_dict``): она меняет состав
    #: корпуса, значит обязана менять ``run_id``. **Какой узел я** — операционный выбор
    #: (``--node``/``COHOUND_NODE``) и в дайджест **не** входит: это deployment
    #: provenance, как ``owner``/``host`` сеанса. В кластере все узлы объявляют одну и ту
    #: же карту, поэтому ``run_id`` у них один — один прогон, нарезанный на шарды.
    shards: list[CrawlShardConfig] | None = None
    # P4: отдельная ось CPU-параллелизма **экстракции** (пул процессов локального пути /
    # потоков split). ``None``/``1`` = сегодняшнее поведение (inline, байт-в-байт); не
    # смешивается с ``max_workers`` (осью сетевого fetch-пула). Резолв — та же цепочка
    # app < run < site. Память: ``W × ~2 GiB`` на процесс docling (см. 03 §D.5).
    extract_workers: int | None = Field(default=None, ge=1)
    # P4/OOM: application-дефолт рециклинга воркер-процессов локальной экстракции
    # (``ProcessPoolExecutor(max_tasks_per_child=N)``). ``None`` — ось не задана
    # (поведение байт-в-байт). Резолв — та же цепочка app < run < site.
    extract_max_tasks_per_child: int | None = Field(default=None, ge=1)
    # P0/OOM: потолок одного HTTP-ответа (и retained-файла на resume), байты. Дефолт
    # 20 MiB — тот же масштаб, что edge-лимит (``EdgeConfig.max_response_bytes``);
    # ``EdgeConfig`` не меняется. Ось операционная (``execution_change``), резолв —
    # app < run < site. Превышение — терминальный ``payload_too_large`` (без retention).
    max_response_bytes: int = Field(default=DEFAULT_MAX_RESPONSE_BYTES, gt=0)
    # P6/P0: потолок **потокового** допущенного артефакта (контейнер zip/7z/rar),
    # байты. Страница остаётся под ``max_response_bytes`` (in-memory потолок): артефакт
    # свыше него уходит в staging-файл, а не в RAM. Дефолт 250 MiB — объявленный
    # владельцем потолок; он покрывает измеренный корпусный контейнер (125.7 MB).
    # Ось операционная (``execution_change``, §13).
    max_artifact_bytes: int = Field(default=DEFAULT_MAX_ARTIFACT_BYTES, gt=0)
    # P6: заявленные форматы-контейнеры (класс «артефакт»). Незaявленный суффикс
    # (``.ova``/``.iso``/``.gz``/``.tar``) остаётся обычным бинарным артефактом. Сужение
    # набора (например только ``["zip"]``) — допустимая операционная правка.
    attachment_formats: tuple[str, ...] = SUPPORTED_ARCHIVE_FORMATS
    # P6: границы спуска в контейнер (исполняет следующий слой). Значения нужны уже
    # сейчас: они входят в провенанс/дайджест и определяют допуск артефакта.
    archive_max_member_bytes: int = Field(default=DEFAULT_ARCHIVE_MAX_MEMBER_BYTES, gt=0)
    #: Потолок **члена-контейнера** (вложенного архива), байты. Отдельная ось, а не та же
    #: ``archive_max_member_bytes``: та измерена по документам (заявленный максимум
    #: 25.1 MB) и отвечает на вопрос «влезет ли файл в извлечение», а до открытия
    #: контейнера его состав неизвестен (замер 15.11.2026: архив 132.2 MB → 57 PDF и ещё
    #: два уровня контейнеров). ``None`` — ось не задана: контейнер не ограничен **этим**
    #: потолком, а конечность держат ``archive_max_uncompressed_bytes`` (распакованный
    #: объём самого контейнера) и ``archive_max_total_uncompressed_bytes`` (корень).
    archive_max_container_bytes: int | None = Field(default=None, gt=0)
    archive_max_uncompressed_bytes: int = Field(
        default=DEFAULT_ARCHIVE_MAX_UNCOMPRESSED_BYTES, gt=0
    )
    archive_max_members: int | None = Field(default=None, gt=0)
    # B3: кумулятивный потолок распакованного объёма на **корень спуска** (все уровни
    # вложенности), байты. Спуск без потолка глубины обязан оставаться конечным: оси
    # ``archive_max_*`` ограничивают один контейнер, эта — весь корень. Операционная ось
    # (``execution_change``, §13).
    archive_max_total_uncompressed_bytes: int = Field(
        default=DEFAULT_ARCHIVE_MAX_TOTAL_UNCOMPRESSED_BYTES, gt=0
    )
    archive_descent: ArchiveDescent = ArchiveDescent.FULL
    max_depth: int = Field(default=0, ge=0)

    max_documents: int | None = Field(default=None, gt=0)
    #: Операторский потолок ревалидации известных URL **на продукт** за цикл
    #: (``crawl.revalidate_max_documents``). ``None`` — ось не задана: ревалидируются
    #: все известные URL (поведение байт-в-байт). Задание оси — видимое отложенное
    #: усечение (``revalidate_max_documents``) + ``stale`` в ``url_state``, не тихий дроп.
    revalidate_max_documents: int | None = Field(default=None, ge=1)
    exclude_paths: list[str] | None = None
    exclude_documents: list[str] | None = None
    # P5: уровень извлечения документных форматов (PDF/Office). Дефолт ``full`` =
    # сегодняшнее эффективное поведение (layout Heron + таблицы ACCURATE + авто-OCR); его
    # смена — граница класса ``execution_change`` (§13), не ``methodology_change``. Резолв —
    # та же цепочка app < run < site (:func:`core.pipeline.crawl_frontier.resolve_crawl_limits`),
    # per seed-root. ``off`` — документы не извлекаются (видимые ``document_skips``).
    document_level: DocumentLevel = DocumentLevel.FULL
    # P7: механизм извлечения PDF (``crawl.pdf_pipeline``). Это условие измерения состава
    # текста, а не удобство: ``structured`` (дефолт = сегодняшнее поведение) гоняет
    # layout-модели/таблицы/OCR через docling, ``text_layer`` читает только текстовый
    # слой. Разный механизм — разный текст документа, поэтому ось входит в
    # ``crawl_config_digest`` и меняет ``run_id`` (``execution_change``, §13).
    # ``crawl.document_level`` действует только при ``structured``.
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED
    # P9: устройство моделей извлечения PDF (``crawl.pdf_device``). ``auto`` не существует
    # намеренно (R5): объявленная ось обязана действовать, а «движок выбрал окружение» —
    # тот же класс, что чтение модели оценщика из env. Дефолт ``cpu`` = модели на CPU;
    # ``cuda`` требует доступного устройства (иначе EXIT_USAGE с числами, не фолбэк).
    # Устройство входит в ``extractor_version``/``extractor_stack`` (ключ ``extract_cache``
    # и ``run_id``), а в ``crawl_config_digest`` — нет: материал уже назван стеком, и
    # второе поле на тот же материал не дало бы различить расхождение прогонов.
    pdf_device: PdfDevice = PdfDevice.CPU
    # D1/§10 (N1): явное включение/выключение принуждения ``robots.txt`` в адаптерах
    # (egress-слой). ``None`` = auto: robots включается ровно тогда, когда включён обход
    # (любой эффективный ``max_depth > 0`` или сконфигурированный per-site ``sitemap``),
    # чтобы прогоны без discovery оставались байт-в-байт прежними (без fetch robots.txt).
    robots: bool | None = None
    # P8: таймаут **документа** (``crawl.document_timeout_s``) — потолок ожидания извлечения
    # одного документа/члена контейнера, секунды. ``None`` — ось не объявлена: действует
    # прежнее зашитое значение (1800 с, ``DEFAULT_DOCUMENT_TIMEOUT_S``), поведение и
    # ``run_id`` байт-в-байт. Именно поэтому поле ``None``, а не ``1800.0``: дефолт app-слоя
    # не должен делать ось «явной» и менять дайджест старых прогонов (§13).
    document_timeout_s: float | None = Field(default=None, gt=0)
    # D9 (план 26): лимиты **PDF-документа** на уровне узла (``crawl.pdf_max_pages`` /
    # ``crawl.pdf_max_extracted_chars``). ``None`` — лимита нет, и это **измеренное**
    # состояние корпуса, а не «безопасный потолок»: замер O31 (09.11.2026, 1 721 PDF /
    # 53 689 страниц) показывает, что любой кандидат потолка режет материал (1 000
    # страниц — 9.2 % страниц, 250 — 20.0 %, 1 M символов — 29.1 %). Поэтому дефолт здесь
    # именно ``None``: ось объявляет оператор, и объявленное значение входит в
    # ``crawl_config_digest``/``run_id`` (``execution_change`` §13).
    #
    # Цепочка резолва — app < run < **product** < site
    # (:func:`core.pipeline.crawl_frontier.resolve_crawl_limits`): продукт (данные пакета,
    # ``crawl_policy.yaml: product_axes``) специфичнее узла, корень — специфичнее продукта.
    # App-слой не «ещё один потолок»: без него объявленная на узле ось была бы недостижима
    # вовсе (``getattr(app, ...)`` возвращал бы ``None`` на любом конфиге).
    pdf_max_pages: int | None = Field(default=None, gt=0)
    pdf_max_extracted_chars: int | None = Field(default=None, gt=0)

    @field_validator("exclude_paths", "exclude_documents")
    @classmethod
    def _check_globs(cls, value: list[str] | None) -> list[str] | None:
        """Отвергает не-строки/пустые glob (D1/H: предсказуемый блэклист)."""
        if value is None:
            return None
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError(
                    "crawl.exclude_paths/exclude_documents: ожидались непустые строки-glob"
                )
        return value

    @field_validator("attachment_formats", mode="before")
    @classmethod
    def _check_attachment_formats(cls, value: object) -> tuple[str, ...]:
        """Нормализует заявленный набор контейнеров (нижний регистр, без точек).

        Канонизация и проверка против поддерживаемых форматов — в нейтральном
        :func:`core.util.document_formats.normalize_attachment_formats`: конфиг не должен
        молча обещать формат, для которого нет читателя.
        """
        if value is None:
            return SUPPORTED_ARCHIVE_FORMATS
        if isinstance(value, str):
            value = [value]
        # Форма входа не объявлена: YAML может дать список, кортеж или множество. Требуется
        # именно итерируемое, иначе ошибка будет названа здесь, а не ``TypeError`` из утилиты.
        if not isinstance(value, Iterable):
            raise ValueError(
                "crawl.attachment_formats: ожидался список форматов контейнеров, "
                f"получено {type(value).__name__}"
            )
        try:
            return normalize_attachment_formats(value)
        except ValueError as exc:
            raise ValueError(f"crawl.attachment_formats: {exc}") from exc

    @model_validator(mode="after")
    def _check_shards_disjoint(self) -> CrawlConfig:
        """Реестр шардов: уникальные узлы и **дизъюнктные** host'ы (T9, 17 §2).

        Пересечение host'ов между шардами — **отказ** (``EXIT_USAGE`` у CLI), а не
        предупреждение: два узла на одном host'е ломают политику вежливости §10 без
        единой ошибки в коде — каждый получит свой бюджет, и суммарная частота к сайту
        удвоится. Ошибка называет **и host'ы, и узлы**: «где именно» — часть сообщения,
        иначе оператор ищет пересечение руками.

        Дубликат ``node`` — тот же класс: два объявления одного узла означают, что карта
        описывает не то, что исполняется. Пустой шард ошибкой **не** является: это
        объявленное «узел ничего не ведёт», и он виден через :func:`shard_notices`.
        """
        if self.shards is None:
            return self
        owner: dict[str, str] = {}
        for shard in self.shards:
            if shard.node in owner:
                raise ValueError(
                    "shards: узел "
                    f"«{shard.node}» объявлен дважды (уникальность node обязательна: "
                    "иначе карта описывает не то, что исполняется)"
                )
            owner[shard.node] = shard.node
        seen: dict[str, str] = {}
        overlaps: dict[str, list[str]] = {}
        for shard in self.shards:
            for host in shard.hosts:
                previous = seen.get(host)
                if previous is not None and previous != shard.node:
                    overlaps.setdefault(host, sorted({previous, shard.node}))
                else:
                    seen[host] = shard.node
        if overlaps:
            detail = "; ".join(
                f"host «{host}» — узлы {', '.join(nodes)}"
                for host, nodes in sorted(overlaps.items())
            )
            raise ValueError(
                "shards: host'ы шардов пересекаются — два узла на одном host'е ломают "
                f"политику вежливости: {detail}. Каждый host принадлежит ровно одному "
                "узлу (17 §2)"
            )
        return self


class EmbedConfig(BaseModel):
    """Секция ``embed`` deploy-конфига: **какими весами** и где считается вектор.

    Три объявленные оси: ``model_id`` + ``revision`` (какие веса) и ``device`` /
    ``precision`` (где и с какой точностью счёт). Все входят в
    ``EXECUTION_CONTRACT_FIELDS`` (``embed_spec``/``embed_device``/``embed_precision``),
    в материал ``pb`` и в ключ ``embed_cache``, поэтому объявлены в конфиге, а не
    читаются у эмбеддера.

    **Модель объявлена потому, что она меняет векторы** (план 26 O6, решение владельца
    09.11.2026; 15.11.2026 — пин ревизии это HF **sha**, а не тег). До этого модель
    выбирал состав установленных extras и env ``COHOUND_EMBED_LITE_*``: на ``cpu`` тихо
    выходила одна модель (dim 256), на ``cuda`` — другая (dim 1024), то есть
    ``run_id``/``pb`` не описывали веса, которыми посчитаны векторы. Пара
    ``model_id@revision`` (свойство :attr:`embed_spec`) — объявленная ось; наблюдаемая
    строка ``embed_model_id`` остаётся провенансом (тот же приём, что
    ``extractor_stack``/``extractor_version``).

    Неполный пин — отказ: ``revision`` без ``model_id`` и ``model_id`` без ``revision``
    (неполная пара не описывает байты весов, а «какая-то» версия — это ложный
    провенанс). Оба ``None`` — законно: dense-ветвь **не объявлена**, и тогда эмбеддер
    не собирается вовсе (R0 работает лексически), а не «первый доступный адаптер».

    Намеренно **не** объявлены: ``backend``/``base_url`` (``14`` §6 п. 5) — остаются
    недекларированными до решения о появлении инстанса. Разница существенна: адрес
    инстанса — место, где лежат веса, а модель — сами веса.

    ``precision`` — решение владельца 06.11.2026 (fp16); за ним замер
    ``docs/reports/measurements/2026-11-06-embedding-precision-cpu.md``: 15.40/13.82
    чанка/с против 6.02/4.99 у fp32 (2.6–2.8×), худший чанк ``1−cos = 8.3e-05``,
    top-10 0.998, детерминизм побитовый. ``bf16``/``tf32`` в перечисление не входят —
    отвергнуты тем же замером (см. :class:`core.domain.enums.EmbedPrecision`).

    ``extra="forbid"``: ключ без модели отвергается, а не игнорируется молча — иначе
    ``deploy.yaml`` с ``embed: {backend: ...}`` создавал бы впечатление объявленного
    backend'а, которого никто не исполняет. Секции ``embed:`` в ``config/deploy.yaml``
    может не быть: дефолты ``cpu``/``fp32`` и есть отсутствие секции (модель при этом
    не объявлена — см. выше).
    """

    model_config = ConfigDict(extra="forbid")

    device: EmbedDevice = EmbedDevice.CPU
    #: Точность счёта вектора. Дефолт ``fp32`` — поведение до появления оси (байт-в-байт).
    #: Объявленная точность обязана доехать до провода (``torch.autocast``): эмбеддер,
    #: который её не исполняет (``model2vec``/NumPy), исключается, а при отсутствии
    #: torch-пути — ``WiringError``. Тихий fp32 запрещён: это ложный провенанс.
    precision: EmbedPrecision = EmbedPrecision.FP32
    #: HF-идентификатор модели (``repo``, данные — не предметная категория). ``None`` —
    #: dense-ветвь **не объявлена**: эмбеддер не собирается (модель выбрал бы хост, а
    #: ``embed_spec``/``run_id`` её не описал бы). Объявлено — собирается **ровно** это.
    model_id: str | None = None
    #: Пин ревизии весов — HF **sha**, не тег (решение владельца 15.11.2026): тег
    #: переезжает, и два прогона под одним ``embed_spec`` считали бы разные байты.
    #: Обязателен при объявленной модели и непуст при любом объявлении.
    revision: str | None = None

    @model_validator(mode="after")
    def _check_model_pin(self) -> EmbedConfig:
        """Пара ``model_id@revision`` объявляется целиком либо не объявляется вовсе.

        Неполная пара — отказ, а не догадка: ``model_id`` без ``revision`` описал бы
        «какую-то» версию весов, а ``revision`` без ``model_id`` вообще не адресует
        байты. Пустая строка — то же, что отсутствие значения, но объявленное молча:
        поэтому она тоже отказ. Пробельная строка — тот же случай: модель у этой секции
        не объявляет ``str_strip_whitespace``, поэтому значения нормализуются здесь
        (иначе ``" "`` доехало бы до загрузчика весов как идентификатор).
        """
        if self.model_id is not None:
            self.model_id = self.model_id.strip()
        if self.revision is not None:
            self.revision = self.revision.strip()
        if self.revision is not None and not self.revision:
            raise ValueError(
                "embed.revision пуст: пин ревизии объявляется непустым HF sha "
                "(D3, 15.11.2026 — sha, а не тег); убрать ключ или указать sha"
            )
        if self.model_id is not None and not self.model_id:
            raise ValueError(
                "embed.model_id пуст: объявить HF-идентификатор модели либо убрать ключ "
                "(пустая строка — не «модель по умолчанию», а необъявленная ось; "
                "ревизия пина — HF sha, не тег, D3 15.11.2026)"
            )
        if self.revision is not None and self.model_id is None:
            raise ValueError(
                "embed.revision объявлен без embed.model_id: ревизия без модели ничего не "
                "адресует — объявить embed.model_id вместе с embed.revision "
                "(sha, а не тег; D3, 15.11.2026)"
            )
        if self.model_id is not None and self.revision is None:
            raise ValueError(
                "embed.model_id объявлен без embed.revision: пин ревизии обязателен "
                "(решение владельца 09.11.2026) — иначе объявленная ось не описывает "
                "байты весов. Объявить embed.revision = HF sha (D3, 15.11.2026: sha, а не "
                "тег) либо убрать embed.model_id (тогда dense-ветвь не собирается)"
            )
        return self

    @property
    def embed_spec(self) -> str | None:
        """Объявленная пара ``model_id@revision`` либо ``None`` (dense не объявлен)."""
        if self.model_id is None or self.revision is None:
            return None
        return f"{self.model_id}@{self.revision}"


class LLMSlotConfig(BaseModel):
    """Конфиг одного именованного LLM-слота (commodity/execution-слой §7.4).

    Модели обслуживаются OI-совместимым API корпоративного облака (не self-host).
    Пустые поля означают «взять дефолт» (модуль ``adapters.llm_local`` / wiring); так
    сохраняется обратная совместимость и слот без ``base_url`` остаётся
    неактивным (оценщик не поднимается). Секрет (``api_key``) в YAML не хранится —
    только env.
    """

    model_config = ConfigDict(extra="forbid")

    base_url: str | None = None
    model_id: str | None = None
    model_version: str | None = None
    #: Механизм constrained decoding (:class:`core.domain.enums.Decoder`). Тип — enum, а
    #: не ``str``: до правки (V4) слот принимал любую строку, адаптер отправлял
    #: ``json_schema`` на всё, что ≠ ``none``, и провенанс объявлял запрошенную метку
    #: вместо исполненного механизма. Опечатка/регистр («JSoN_ObJeCt») теперь отказ, а не
    #: молчаливое включение схемы.
    decoder: Decoder | None = None
    timeout_s: float | None = Field(default=None, gt=0)
    max_tokens: int | None = Field(default=None, gt=0)
    #: §7.4 (решение владельца 06.11.2026): усилие рассуждения оценщика. ``None`` — не
    #: объявлено на этом слое (резолв идёт вверх по цепочке: ``--llm-reasoning-effort`` >
    #: ``run.yaml: llm_reasoning_effort`` > этот ключ), и параметра на провод не уходит.
    #: Входит в ``llm_scenarios`` → ``run_id``: рассуждение меняет и расход токенов, и
    #: текст ответа. Уровень, не выразимый на проводе объявленного семейства модели, —
    #: отказ (``EXIT_USAGE``), а не тихая отправка без параметра.
    reasoning_effort: ReasoningEffort | None = None


class LLMDeployConfig(BaseModel):
    """Слоты моделей LLM и активный slot (``llm:`` deploy-конфига; §7.4).

    Обе слоты по умолчанию пусты; ``slot`` — активный слот, переопределяемый
    ``--llm-slot``/``COHOUND_LLM_SLOT`` (см. :func:`resolve_llm_slot`).

    **``rnd_endpoint`` — объявление исключения, а не наблюдение (§10, блок «Исключение:
    стенд RnD»).** Правило «оценщик работает с [OI]-совместимым API внутри корпоративного
    периметра» — производственное. Стенд RnD намеренно ходит во внешний endpoint, и это
    исключение обязано быть **объявлено**, иначе внешний хост неотличим от ошибки
    конфигурации: определить «внешность» по имени хоста нельзя (корпоративные имена тоже
    публичные FQDN, split-horizon DNS — норма), а разрешать DNS ради провенанса значило бы
    зависеть от сети. Поэтому:

    * ``None`` (дефолт) — принадлежность периметру **не объявлена**. Если прогон при этом
      объявляет endpoint (``llm_scenarios`` непуста), это видно замечанием
      ``rnd_endpoint_not_declared``: карта не может быть названа производственной, потому
      что условия, которые это подтверждают, никем не заявлены;
    * ``True`` — объявлено: стенд вне корпоративного периметра, карта получает признак
      ``rnd_endpoint`` в профиле публикуемости и видимое предупреждение;
    * ``False`` — объявлено: endpoint внутри периметра (производственный контур).

    Поле **не** входит в ``EXECUTION_CONTRACT_FIELDS``: это декларация о месте исполнения,
    а не условие измерения, — ``run_id`` она не меняет.
    """

    model_config = ConfigDict(extra="forbid")

    slot: str = DEFAULT_LLM_SLOT
    cheap: LLMSlotConfig = Field(default_factory=LLMSlotConfig)
    flagship: LLMSlotConfig = Field(default_factory=LLMSlotConfig)
    #: См. докстринг класса: ``None`` — не объявлено, ``True`` — стенд вне периметра.
    rnd_endpoint: bool | None = None

    @field_validator("slot")
    @classmethod
    def _check_slot(cls, value: str) -> str:
        slot = value.strip()
        if slot not in LLM_SLOTS:
            raise ValueError(
                f"llm.slot: недопустимый слот «{value}»; ожидается один из {list(LLM_SLOTS)}"
            )
        return slot


class RetrievalDeployConfig(BaseModel):
    """Секция ``retrieval`` deploy-конфига: **объявленный дефолт** профиля ретривала.

    Закрывает D2 плана 26. Профиль ретривала — условие измерения (I16): он входит в
    ``EXECUTION_CONTRACT_FIELDS`` (``retrieval_profile``), значит в ``run_id``. До этой
    секции дефолт жил **в коде исполнения** (``evaluate.DEFAULT_RETRIEVAL_PROFILE``), а в
    контракт уходил ``None``: два одинаковых измерения («флаг не задан») описывались
    по-разному — ``null`` в ``run_id`` и ``R0`` в исполнении, — то есть ``run_id``
    утверждал одно, а вердикты получены другим прибором.

    Приоритет (объявленный, а не выведенный): ``--retrieval-profile`` >
    ``deploy.retrieval.profile`` > ``R0`` (дефолт поля). Резолв — один
    (:func:`core.cli.execution_axes.resolve_retrieval_profile`), поэтому контракт и
    исполнение не могут разойтись.

    Почему дефолт объявлен здесь, а не зашит в код: профиль симметричен для всех
    продуктов цикла (I16) и относится к условиям измерения конкретного развёртывания, а
    не к пакету. Смена значения — объявленная ось: новый ``run_id``
    (``execution_change``), а не тихая правка поведения.
    """

    model_config = ConfigDict(extra="forbid")

    #: ``R0`` | ``R1`` | ``R2``. Дефолт — ``R0``: лексическое плечо, единственное, что
    #: работает без объявленной модели эмбеддера (``embed_spec: null``).
    profile: str = "R0"

    #: Бюджет кэша партиций dense-индекса, МиБ. ``None`` — значение кода
    #: (:data:`core.adapters.vector_npy.DEFAULT_PARTITION_CACHE_BYTES`, 256).
    #:
    #: **Операционный бюджет, а не условие измерения** — поэтому его нет в
    #: ``EXECUTION_CONTRACT_FIELDS``, как и ``extract.cache_max_bytes``: он меняет цену
    #: поиска, а не состав корпуса и не вердикты.
    #:
    #: Почему параметр вообще объявлен, хотя замер 16.11.2026 (O43) показал, что дефолта
    #: хватает: он нужен там, где пере-чтение партиции действительно дорого. Тогда
    #: «сколько памяти отдать кэшу» — решение оператора с числом, а не константа в коде.
    #: На стенде ``postgrespro`` (206 931 × 1024 = **808 MiB** в fp32, то есть больше
    #: дефолтных 256 MiB) кэш для этой партиции не работает, но цена от этого не зависит:
    #: после устранения O43 сетка ``R0,R1`` стоит 4.0 с при 256 MiB, 6.8 с при 1024 MiB и
    #: 6.7 с при кэше 0. Отказ кэша по размеру виден как ``partition_cache_too_small``.
    partition_cache_mb: int | None = None

    #: Язык запросов ретривала: ``ru`` | ``en`` | ``all`` (см. :data:`QUERY_LANGS`).
    #:
    #: Условие измерения, а не удобство: от него зависит, какие термины каталога
    #: попадут в запрос, значит и набор доказательств. Входит в
    #: ``EXECUTION_CONTRACT_FIELDS`` (``query_lang``) ⇒ в ``run_id``; приоритет
    #: ``--query-lang`` > этот ключ > ``all`` (значение резолвера, сохраняющее прежнее
    #: поведение). ``auto`` нет намеренно: язык корпуса — свойство профиля, а не догадка
    #: исполнения.
    query_lang: str = "all"

    @field_validator("query_lang")
    @classmethod
    def _check_query_lang(cls, value: str) -> str:
        """Регистр не различаем; неизвестное значение — отказ с перечислением допустимых."""
        lang = str(value).strip().lower()
        if lang not in QUERY_LANGS:
            raise ValueError(
                f"retrieval.query_lang: недопустимый язык «{value}»; ожидается один из "
                f"{list(QUERY_LANGS)} (ось объявляется, а не выводится из корпуса)"
            )
        return lang

    @field_validator("partition_cache_mb")
    @classmethod
    def _check_partition_cache_mb(cls, value: int | None) -> int | None:
        """Отрицательный бюджет — отказ: ``0`` законно (кэш выключен), ``-1`` бессмысленно.

        ``0`` не превращается в ``None``: «кэш выключен» и «взять дефолт» — разные
        измерения цены, и тихое схлопывание одного в другое сделало бы замер
        невоспроизводимым.
        """
        if value is None:
            return None
        if value < 0:
            raise ValueError(
                f"retrieval.partition_cache_mb: бюджет обязан быть ≥ 0, получено {value!r}"
            )
        return value

    @field_validator("profile")
    @classmethod
    def _check_profile(cls, value: str) -> str:
        """Строка приводится к канону (``r0`` → ``R0``); неизвестное значение — отказ.

        Регистр не различаем намеренно: ``R0``/``r0`` — одно и то же условие измерения,
        и отвергать вторую запись значило бы наказывать за оформление. Отказ перечисляет
        допустимые значения: опечатка не должна доехать до исполнения как «другой
        профиль».
        """
        profile = str(value).strip().upper()
        if profile not in RETRIEVAL_PROFILES:
            raise ValueError(
                f"retrieval.profile: недопустимый профиль «{value}»; ожидается один из "
                f"{list(RETRIEVAL_PROFILES)} (I16: профиль ретривала — условие измерения)"
            )
        return profile


class LexicalDeployConfig(BaseModel):
    """Секция ``lexical`` deploy-конфига: **объявленный** профиль лексического анализа.

    Профиль анализатора — условие измерения, а не настройка удобства: ``stemmed``
    (lowercase + два стеммера + стоп-слова) и ``identifier`` (whitespace без lowercase и
    стемминга) дают разную выдачу. Замер O16 (15.11.2026, 253 306 чанков): ``stemmed``
    P@10 0.96 / hit 1.00, ``identifier`` 1.00 / 1.00 — различие видно в ранге первого
    попадания и в том, находит ли запрос ``min_wal_size``/``pg_upgrade``. Значит профиль
    обязан быть назван в ``lexical_spec`` (``EXECUTION_CONTRACT_FIELDS``), а не браться
    дефолтом адаптера в момент сборки индекса.

    Дефолт — ``stemmed``: выигрыш ``identifier`` на пилоте не доказан (O16), а цена
    ошибки — слепой recall (нет ни стемминга, ни lowercase). Смена профиля для
    профиля-кейса — объявленная ось (новый ``run_id``), не правка кода.

    ``raw`` — псевдоним ``identifier`` (см. ``adapters.lexical_tantivy``): нормализуется
    к ``identifier``, чтобы в контракте одно поведение не имело двух имён.
    """

    model_config = ConfigDict(extra="forbid")

    #: ``stemmed`` | ``identifier`` (``raw`` → ``identifier``) | ``ngram``.
    profile: str = "stemmed"

    @field_validator("profile")
    @classmethod
    def _check_profile(cls, value: str) -> str:
        """Приводит имя к канону; ``raw`` → ``identifier``; неизвестное — отказ."""
        profile = str(value).strip().lower()
        if profile not in LEXICAL_PROFILES:
            raise ValueError(
                f"lexical.profile: недопустимый профиль «{value}»; ожидается один из "
                f"{list(LEXICAL_PROFILES)} (``raw`` — псевдоним ``identifier``)"
            )
        return "identifier" if profile == "raw" else profile


class ToolsDeployConfig(BaseModel):
    """Секция ``tools`` deploy-конфига: **объявленный дефолт** профиля инструментов.

    Профиль локальных инструментов оценщика (план ``28`` §9) — условие измерения: он
    входит в ``EXECUTION_CONTRACT_FIELDS`` (``tool_profile``), значит в ``run_id``. Набор
    доступных вызовов меняет то, что модель может адресовать (раздел документа, строку
    таблицы), то есть набор доказательств и вердикт, — поэтому он объявлен, а не
    подставляется кодом исполнения.

    Приоритет (объявленный, а не выведенный): ``--tool-profile`` >
    ``deploy.tools.profile`` > ``none``. Резолв один
    (:func:`core.cli.execution_axes.resolve_tool_profile`), поэтому контракт и исполнение
    не могут разойтись — тот же довод, что у ``retrieval.profile`` (D2 плана 26).

    ``max_calls`` — **объявленный** бюджет вызовов на ячейку, а не предохранитель:
    «не ограничивать, а измерять» (A3) означает, что число объявляется и публикуется.
    Дефолт ``0`` согласован с ``profile: none``: инструментов нет — вызовов нет. Профиль
    с инструментами при нулевом бюджете отвергается (``ToolProfile``), а не исполняется
    как «профиль, который ничего не делает»: оператор обязан назвать число.
    """

    model_config = ConfigDict(extra="forbid")

    #: ``none`` | ``t1`` | ``t4`` | ``t7`` | список через ``+`` (``t1+t4+t7``).
    profile: str = "none"

    #: Бюджет вызовов инструментов на ячейку (``0`` — только при ``profile: none``).
    max_calls: int = 0

    @field_validator("max_calls")
    @classmethod
    def _check_max_calls(cls, value: int) -> int:
        """Отрицательный бюджет — отказ: бюджет объявляется числом, а не «сколько выйдет»."""
        if value < 0:
            raise ValueError(
                f"tools.max_calls: бюджет обязан быть ≥ 0, получено {value!r}"
            )
        return value


class ExtractDeployConfig(BaseModel):
    """Секция ``extract`` deploy-конфига: операционный бюджет кэша экстракции (D8).

    Решение владельца 15.11.2026: размер кэша экстракции — параметр конфигурации
    развёртывания; по умолчанию **не ограничен**; если задан — вытеснение (LRU по
    ``mtime``, ``ExtractionCache._evict_if_needed``) и повторное извлечение вытесненного
    происходят автоматически, без человека. Ключ кэша контент-адресный, поэтому
    вытеснение — обычный промах кэша, а не потеря доказательств.

    **Почему это НЕ входит в ``EXECUTION_CONTRACT_FIELDS``.** Бюджет кэша — свойство
    хоста, а не условие измерения: он меняет только то, что придётся пере-извлечь, но не
    состав корпуса и не вердикты (текст документа — функция байтов и объявленного стека
    извлечения, а не наличия записи в кэше). Внеси его в контракт — и переезд на диск
    другого размера давал бы новый ``run_id`` при том же измерении. Это записано здесь
    намеренно: следующий читатель не должен «дополнить» контракт этим полем.
    """

    model_config = ConfigDict(extra="forbid")

    #: Бюджет суммы ``*.json`` под ``paths.extract_cache``. ``None`` — без вытеснения
    #: (сегодняшнее поведение). ``0`` отвергается: это не «без кэша», а «вытеснять всё,
    #: включая только что записанное» — кэш, который никогда не попадает.
    cache_max_bytes: int | None = None

    @field_validator("cache_max_bytes")
    @classmethod
    def _check_budget(cls, value: int | None) -> int | None:
        """Бюджет либо не объявлен, либо положителен; ``0``/отрицательное — отказ."""
        if value is None:
            return None
        if value <= 0:
            raise ValueError(
                "extract.cache_max_bytes: "
                f"{value} — недопустимый бюджет. ``null`` означает «без вытеснения»; "
                "заданное число обязано быть положительным (ноль вытеснял бы всё, "
                "включая только что записанную запись, то есть кэш не попадал бы никогда)"
            )
        return value


class DeployConfig(BaseModel):
    """Профиль развёртывания и настройки зон (03 D.5).

    Для ``split``/``split-hardened`` обязателен ``edge.base_url``; для ``local``
    секция ``edge.*`` игнорируется. Секция ``crawl`` действует в обоих профилях
    (операционная политика HTTP-краулинга; 03 D.5/D.6). Секция ``llm`` несёт слоты
    моделей (commodity/execution-слой §7.4). Отсутствие файла даёт безопасный дефолт
    ``profile: local`` (см. :func:`load_deploy_config`).
    """

    model_config = ConfigDict(extra="forbid")

    profile: str = "local"
    edge: EdgeConfig = Field(default_factory=EdgeConfig)
    extractor_zone: str = "core"
    search: SearchDeployConfig = Field(default_factory=SearchDeployConfig)
    crawl: CrawlConfig = Field(default_factory=CrawlConfig)
    #: Устройство эмбеддера (ось ``embed.device``). Отсутствие секции = ``cpu``: дефолт
    #: объявлен здесь, а не выведен из окружения (torch выбирать устройство не должен).
    embed: EmbedConfig = Field(default_factory=EmbedConfig)
    llm: LLMDeployConfig = Field(default_factory=LLMDeployConfig)
    #: Объявленный дефолт профиля ретривала (I16/D2). Отсутствие секции = ``R0``; значение
    #: уходит **и** в контракт (``retrieval_profile``), **и** в исполнение — один резолв,
    #: поэтому «дефолт в коде» больше не расходится с ``run_id``.
    retrieval: RetrievalDeployConfig = Field(default_factory=RetrievalDeployConfig)
    #: Объявленный профиль лексического анализатора (``lexical_spec``). Отсутствие
    #: секции = ``stemmed`` — прежняя выдача байт-в-байт.
    lexical: LexicalDeployConfig = Field(default_factory=LexicalDeployConfig)
    #: Операционный бюджет кэша экстракции (D8). В контракт **не** входит: свойство хоста,
    #: а не условие измерения (см. :class:`ExtractDeployConfig`).
    extract: ExtractDeployConfig = Field(default_factory=ExtractDeployConfig)
    #: Объявленный дефолт профиля локальных инструментов оценщика (план ``28`` §9).
    #: Отсутствие секции = ``none`` (путь без инструментов — прежнее поведение).
    #: Значение уходит **и** в контракт (``tool_profile``), **и** в исполнение — один
    #: резолв (:func:`core.cli.execution_axes.resolve_tool_profile`), поэтому «дефолт в
    #: коде» не может разойтись с ``run_id``.
    tools: ToolsDeployConfig = Field(default_factory=ToolsDeployConfig)

    @field_validator("profile")
    @classmethod
    def _check_profile(cls, value: str) -> str:
        profile = value.strip()
        if profile not in PROFILES:
            raise ValueError(
                f"profile: недопустимый профиль «{value}»; ожидается один из {list(PROFILES)}"
            )
        return profile

    @field_validator("extractor_zone")
    @classmethod
    def _check_extractor_zone(cls, value: str) -> str:
        zone = value.strip()
        if zone not in EXTRACTOR_ZONES:
            raise ValueError(
                f"extractor_zone: недопустимая зона «{value}»; ожидается одна из {list(EXTRACTOR_ZONES)}"
            )
        return zone

    @model_validator(mode="after")
    def _require_edge_base_url_for_split(self) -> DeployConfig:
        """Для ``split``/``split-hardened`` шов обязан иметь ``edge.base_url``."""
        if self.profile in SPLIT_PROFILES and (
            self.edge.base_url is None or not self.edge.base_url.strip()
        ):
            raise ValueError(
                f"profile «{self.profile}» требует непустой edge.base_url "
                "(адрес DMZ-шва, 07 §7)"
            )
        return self


# ---------------------------------------------------------------------------
# Публичный загрузчик
# ---------------------------------------------------------------------------


def load_deploy_config(path: str | Path | None = None) -> DeployConfig:
    """Загружает ``config/deploy.yaml`` (03 D.5 / 07 §7) в :class:`DeployConfig`.

    ``path=None`` означает канонический путь ``config/deploy.yaml``. Если файла
    **нет** — возвращается безопасный дефолт ``profile: local`` (всё in-process),
    а не ошибка (A1: ``local`` — дефолт, DMZ не тянем без нужды).
    """
    target = DEFAULT_DEPLOY_PATH if path is None else Path(path)
    if not target.exists():
        return DeployConfig(profile="local")
    data = _as_mapping(_read_yaml(target), "deploy.yaml")
    return DeployConfig.model_validate(data)


def is_split(config: DeployConfig) -> bool:
    """``True`` для профилей ``split`` и ``split-hardened`` (зоны данных разделены)."""
    return config.profile in SPLIT_PROFILES


#: D-8: ``split-hardened`` **принимается** (совместимость конфига), но ничего не добавляет
#: сверх ``split``: нет ни Z2 PARSE, ни egress-прокси, ни TLS-инспекции. Молчаливое
#: принятие такого имени — ложное утверждение о безопасности, поэтому профиль обязан
#: громко предупреждать (payload, лог wiring, ``deploy-check``).
SPLIT_HARDENED_WARNING = (
    "profile «split-hardened» принят, но ничего не добавляет сверх «split»: Z2 PARSE, "
    "egress-прокси и TLS-инспекция не реализованы — зоны данных разделены ровно так же, "
    "как в «split». Хардненинга нет: используйте «split» (07 §7)"
)


def profile_notices(config: DeployConfig) -> tuple[str, ...]:
    """Видимые предупреждения о выбранном профиле развёртывания (D-8, 07 §7).

    Единственное сегодня — ``split-hardened``: имя обещает хардненинг, которого нет.
    Функция чистая (читает только конфиг), поэтому её зовут и composition root
    (payload/``wiring.jsonl``), и ``deploy-check`` — одна формулировка, один источник.
    """
    if config.profile == "split-hardened":
        return (SPLIT_HARDENED_WARNING,)
    return ()


# ---------------------------------------------------------------------------
# Шарды узлов краула (17 §2, F7)
# ---------------------------------------------------------------------------

#: Имя env-переменной с идентификатором узла краула (``--node`` её перекрывает).
#: Операционный выбор, а не измерение: узел записывается в провенанс сеанса, но в
#: ``run_id`` не входит (иначе один прогон, нарезанный на шарды, стал бы N измерениями).
ENV_NODE = "COHOUND_NODE"


@dataclass(frozen=True)
class HostShard:
    """Разрешённый шард **этого** узла: ``node`` + нормализованные host'ы.

    ``hosts`` — то, что узел имеет право вести; пустое множество означает «узел ничего
    не ведёт» и обязано быть видимым (:func:`shard_notices`), а не тихим простоем.
    Отдельный value-объект, а не пара аргументов: у него два потребителя (фильтр
    frontier'а и провенанс сеанса), и разъехаться им нельзя.
    """

    node: str
    hosts: frozenset[str] = frozenset()

    @property
    def empty(self) -> bool:
        """Узел объявлен без host'ов (видимый простой, а не ошибка конфига)."""
        return not self.hosts


def resolve_host_shard(
    shards: list[CrawlShardConfig] | None, node: str | None
) -> HostShard | None:
    """Выбирает шард узла из объявленной карты (``deploy.yaml: crawl.shards``).

    Правила (каждое — отказ или видимая запись, третьего нет):

    * карты нет (``None``) — шардов нет: ``None`` значит «весь мир мой», фильтра нет;
    * карта есть, узел **не выбран** — :class:`ValueError`: «взял все host'ы молча» было
      бы ложью о развёртывании, а кластер без выбора узла не исполняется;
    * узел выбран, но его нет в карте — :class:`ValueError`: опечатка в имени узла не
      должна молча превращаться в «все host'ы» (это дало бы два узла на одних host'ах);
    * узел найден — :class:`HostShard` (возможно пустой, см. :attr:`HostShard.empty`).

    :raises ValueError: карта объявлена, а узел не выбран или не объявлен в ней.
    """
    if shards is None:
        return None
    known = [shard.node for shard in shards]
    name = str(node or "").strip()
    if not name:
        raise ValueError(
            "shards: crawl.shards объявлены, но узел не выбран — укажите --node или "
            f"{ENV_NODE}; объявленные узлы: {', '.join(known) or '—'}"
        )
    for shard in shards:
        if shard.node == name:
            return HostShard(node=shard.node, hosts=frozenset(shard.hosts))
    raise ValueError(
        f"shards: узел «{name}» не объявлен в crawl.shards — объявленные узлы: "
        f"{', '.join(known) or '—'} (опечатка не должна означать «все host'ы»)"
    )


def shard_notices(shards: list[CrawlShardConfig] | None) -> tuple[str, ...]:
    """Видимые записи о **пустых** шардах объявленной карты (17 §2).

    Пустой шард — не ошибка конфига (узел может быть объявлен заранее), но и не тишина:
    «host'ов ноль и работы ноль» оператор обязан увидеть строкой, а не выяснять из
    отсутствия артефактов. Формулировка одна на все потребители (payload/лог/``errors``
    фазы) — как у :func:`profile_notices`.
    """
    if not shards:
        return ()
    return tuple(
        f"shards: узел «{shard.node}» объявлен без host'ов — этот узел не будет "
        "краулить (видимый простой, а не тихий: 17 §2)"
        for shard in shards
        if not shard.hosts
    )


# ---------------------------------------------------------------------------
# Резолвер провайдера поиска (I16/I21/I23)
# ---------------------------------------------------------------------------


def resolve_search_provider(
    search: SearchDeployConfig | None,
    *,
    lang: str = "ru",
    explicit: str | None = None,
    default: str = "yandex",
) -> str:
    """Детерминированный выбор провайдера веб-поиска (03 D.5).

    Приоритет (первый непустой побеждает):

    1. ``explicit`` — явный ``--search-adapter`` (CLI-флаг оператора);
    2. ``search.by_language[lang]`` — переопределение по языку (география-прокси);
    3. ``search.adapter`` — провайдер по умолчанию из конфига;
    4. ``default`` — безопасный дефолт (``yandex``).

    **I16 (симметрия протокола).** Функция зависит только от ``(lang, config)`` и
    **не** зависит от продукта: один и тот же язык даёт один и тот же провайдер для всех
    продуктов цикла — дифференциация по ``lang`` канонична (``SearchQuery.lang``,
    ``search_log.language``, бюджет по языкам), а не по продукту.

    **I21-дух (детерминизм).** Маршрут вычисляется по правилу, а не решается LLM.

    **§13 (версионная граница).** Смена таблицы провайдеров меняет протокол поиска и
    потому влияет на сопоставимость циклов; выбранный провайдер обязан быть зафиксирован
    в провенансе/``search_log`` (I23) и меняется как версионная граница.

    ``lang`` нормализуется к нижнему регистру; ключи ``by_language`` сравниваются
    ``casefold``-совпадением (детерминированно, регистронезависимо). Возврат — имя
    провайдера (строка-данные); валидация против реестра адаптеров — в composition root
    (CLI, ``wiring``), не здесь.

    :param search: секция ``search`` deploy-конфига либо ``None`` (нет конфига).
    :param lang: язык запроса (``ru``/``en``/…); по умолчанию ``ru``.
    :param explicit: явное переопределение (CLI-флаг); непустое побеждает всегда.
    :param default: безопасный дефолт при отсутствии конфига/совпадений.
    """
    if explicit is not None and str(explicit).strip():
        return str(explicit).strip()

    normalized = str(lang or "").strip().casefold()
    if search is not None and search.by_language:
        casefolded = {
            str(key).strip().casefold(): str(value).strip()
            for key, value in search.by_language.items()
        }
        matched = casefolded.get(normalized)
        if matched:
            return matched

    if search is not None and search.adapter is not None and str(search.adapter).strip():
        return str(search.adapter).strip()

    return default


# ---------------------------------------------------------------------------
# Резолвер активного LLM-слота (§7.4, commodity/execution-слой)
# ---------------------------------------------------------------------------


def resolve_llm_slot(
    llm: LLMDeployConfig | None,
    *,
    explicit: str | None = None,
    default: str = DEFAULT_LLM_SLOT,
) -> str:
    """Детерминированный выбор активного LLM-слота (03 D.5 / §7.4).

    Приоритет (первый непустой побеждает):

    1. ``explicit`` — явный ``--llm-slot``/``COHOUND_LLM_SLOT`` (CLI-флаг/среда оператора);
    2. ``llm.slot`` — активный слот из ``config/deploy.yaml``;
    3. ``default`` — безопасный дефолт (``cheap``).

    Значение нормализуется (``strip``) и валидируется против :data:`LLM_SLOTS`:
    неизвестный ``explicit``/``default`` → ``ValueError`` с понятным RU-сообщением
    (а не молчаливая подмена). Маршрут вычисляется по правилу, не LLM (дух I21).

    :param llm: секция ``llm`` deploy-конфига либо ``None`` (нет конфига).
    :param explicit: явное переопределение (CLI/env); непустое побеждает всегда.
    :param default: безопасный дефолт при отсутствии конфига/совпадений.
    :raises ValueError: выбрано неизвестное имя слота.
    """
    if explicit is not None and str(explicit).strip():
        slot = str(explicit).strip()
        if slot not in LLM_SLOTS:
            raise ValueError(
                f"llm-slot: недопустимый слот «{explicit}»; ожидается один из {list(LLM_SLOTS)}"
            )
        return slot

    if llm is not None and str(llm.slot).strip():
        slot = str(llm.slot).strip()
        if slot not in LLM_SLOTS:
            raise ValueError(
                f"llm.slot: недопустимый слот «{llm.slot}»; ожидается один из {list(LLM_SLOTS)}"
            )
        return slot

    if default not in LLM_SLOTS:
        raise ValueError(
            f"llm-slot default: недопустимый слот «{default}»; ожидается один из {list(LLM_SLOTS)}"
        )
    return default


# ---------------------------------------------------------------------------
# Резолвер переключателя SERP-контура (I3/§5)
# ---------------------------------------------------------------------------

#: Имя env-переключателя контура. Операционный аварийный троттл: ``0``/``off`` выключает
#: контур на инсталляции, целое ``N ≥ 1`` задаёт число раундов. Значение **входит** в
#: объявленную ось (``serp_rounds``), потому что раунды меняют корпус: скрытая подмена
#: дала бы прогон, у которого ``run_id`` описывает не тот контур, которым собран корпус.
ENV_SERP = "COHOUND_SERP"

#: Значения env/CLI, означающие «контур выключен».
_SERP_OFF_VALUES: frozenset[str] = frozenset({"0", "off", "false", "no", "выкл"})


def parse_serp_switch(raw: Any, *, where: str = ENV_SERP) -> int | None:
    """Разбирает значение переключателя контура: ``None`` — не задано, ``0`` — выключен.

    Принимаются только две формы: «выключено» (:data:`_SERP_OFF_VALUES`, регистр не важен)
    и целое ``N ≥ 0``. Иные значения — :class:`ValueError`: неоднозначный переключатель
    («вроде включено») не должен разрешаться молчаливым дефолтом.
    """
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise ValueError(f"{where}: ожидалось целое или off, получено {raw!r}")
    if isinstance(raw, int):
        if raw < 0:
            raise ValueError(f"{where}: раунды не могут быть отрицательными: {raw}")
        return raw
    text = str(raw).strip()
    if not text:
        return None
    if text.lower() in _SERP_OFF_VALUES:
        return 0
    try:
        value = int(text)
    except ValueError:
        raise ValueError(
            f"{where}: ожидалось целое число раундов или off, получено {text!r}"
        ) from None
    if value < 0:
        raise ValueError(f"{where}: раунды не могут быть отрицательными: {value}")
    return value


def resolve_serp_rounds(
    search: SearchDeployConfig | None,
    *,
    cli: Any = None,
    env: Any = None,
    run_rounds: int | None = None,
) -> tuple[int, str]:
    """Эффективное число раундов SERP-контура и **уровень**, который его задал.

    Приоритет: ``cli`` (``--serp-rounds``) > ``env`` (``COHOUND_SERP``) > ``run_rounds``
    (``run.yaml: serp.rounds``) > ``search.serp.rounds`` (``deploy.search.serp.rounds``)
    > ``0`` (контур выключен). Возвращаемый источник идёт в провенанс прогона: прогон
    обязан называть уровень, которым включён контур, иначе «почему поиск не работал»
    неотличимо от «поиск не объявлен».

    :raises ValueError: значение любого уровня не разбирается (не целое/отрицательное).
    """
    cli_rounds = parse_serp_switch(cli, where="--serp-rounds")
    if cli_rounds is not None:
        return cli_rounds, "cli"
    env_rounds = parse_serp_switch(env, where=ENV_SERP)
    if env_rounds is not None:
        return env_rounds, "env"
    if run_rounds is not None:
        return int(run_rounds), "run.yaml"
    deploy_rounds = getattr(getattr(search, "serp", None), "rounds", None)
    if deploy_rounds is not None:
        return int(deploy_rounds), "deploy.yaml"
    return 0, "default"


