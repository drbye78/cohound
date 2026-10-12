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

Безопасность: загрузчик не логирует содержимое и не печатает секреты; ``client_key`` —
это **путь** к файлу ключа, а не сам ключ. Доверие к TLS — через корпоративный CA;
никакого certificate pinning (07 §8.1) — это осознанно отсутствует в модели.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров; строка
адаптера поиска — просто данные. Допустимые зависимости слоя ``config`` —
``domain``/``config``/``util`` (01 §1).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

__all__ = [
    "TLSConfig",
    "AuthConfig",
    "OIDCConfig",
    "AuditConfig",
    "EdgeConfig",
    "SearchDeployConfig",
    "CrawlConfig",
    "CrawlBrowserConfig",
    "LLMSlotConfig",
    "LLMDeployConfig",
    "DeployConfig",
    "load_deploy_config",
    "is_split",
    "resolve_search_provider",
    "resolve_llm_slot",
    "LLM_SLOTS",
    "DEFAULT_LLM_SLOT",
    "DEFAULT_LLM_MODEL_ID",
]

# Канонические допустимые значения (ЗЕРКАЛО 07 §7 / 03 D.5). Не выносим в доменные
# enum'ы: это конфигурация развёртывания, а не доменная методология.
PROFILES: tuple[str, ...] = ("local", "split", "split-hardened")
SPLIT_PROFILES: frozenset[str] = frozenset({"split", "split-hardened"})
AUTH_KINDS: tuple[str, ...] = ("mtls", "oidc")
EXTRACTOR_ZONES: tuple[str, ...] = ("edge", "core", "separate")
AUDIT_SINKS: tuple[str, ...] = ("file", "syslog")
#: Режимы подключения браузерного краулера (07): внешний браузер по CDP либо
#: персистентный профиль, которым владеет Playwright.
CRAWL_BROWSER_MODES: tuple[str, ...] = ("cdp", "persistent")

#: Именованные слоты моделей LLM (commodity/execution-слой §7.4). Активный slot
#: выбирается в рантайме (``--llm-slot`` / ``COHOUND_LLM_SLOT``), см. :func:`resolve_llm_slot`.
LLM_SLOTS: tuple[str, ...] = ("cheap", "flagship")
#: Безопасный дефолт активного слота (лёгкая модель).
DEFAULT_LLM_SLOT = "cheap"
#: Дефолтный ``model_id`` слотов (данные; обе слоты по умолчанию на одну модель).
DEFAULT_LLM_MODEL_ID = "deepseek-v4.1-flash:cxbc"

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


class SearchDeployConfig(BaseModel):
    """Бюджет и адаптер внешнего поиска (07 §7 / D.5).

    Бюджет — целочисленные лимиты по языкам (``{ru: 5, en: 5}``); симметрия I16 —
    методологическое правило конвейера, здесь значение лишь парсится.

    **Провайдер поиска.** ``adapter`` — провайдер **по умолчанию**; ``by_language`` —
    детерминированное переопределение по языку (география-прокси), например
    ``{ru: yandex, en: glm}``. Выбор из ``by_language`` — функция **от языка**,
    не от продукта, поэтому симметричный протокол I16 сохраняется. Имена провайдеров —
    свободные строки-данные: валидация против реестра (``SUPPORTED_SEARCH_ADAPTERS``)
    живёт в composition root (CLI), а не в config-слое (§1.3, 01 §1).
    """

    model_config = ConfigDict(extra="forbid")

    budget: dict[str, int] = Field(default_factory=dict)
    adapter: str | None = None
    by_language: dict[str, str] = Field(default_factory=dict)

    @field_validator("budget")
    @classmethod
    def _check_budget(cls, value: dict[str, int]) -> dict[str, int]:
        for lang, limit in value.items():
            if not str(lang).strip():
                raise ValueError("search.budget: пустой код языка")
            if limit < 0:
                raise ValueError(f"search.budget[{lang}]: лимит не может быть отрицательным")
        return value

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
    decoder: str | None = None
    timeout_s: float | None = Field(default=None, gt=0)
    max_tokens: int | None = Field(default=None, gt=0)


class LLMDeployConfig(BaseModel):
    """Слоты моделей LLM и активный slot (``llm:`` deploy-конфига; §7.4).

    Обе слоты по умолчанию пусты; ``slot`` — активный слот, переопределяемый
    ``--llm-slot``/``COHOUND_LLM_SLOT`` (см. :func:`resolve_llm_slot`).
    """

    model_config = ConfigDict(extra="forbid")

    slot: str = DEFAULT_LLM_SLOT
    cheap: LLMSlotConfig = Field(default_factory=LLMSlotConfig)
    flagship: LLMSlotConfig = Field(default_factory=LLMSlotConfig)

    @field_validator("slot")
    @classmethod
    def _check_slot(cls, value: str) -> str:
        slot = value.strip()
        if slot not in LLM_SLOTS:
            raise ValueError(
                f"llm.slot: недопустимый слот «{value}»; ожидается один из {list(LLM_SLOTS)}"
            )
        return slot


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
    llm: LLMDeployConfig = Field(default_factory=LLMDeployConfig)

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
        if self.profile in SPLIT_PROFILES:
            if self.edge.base_url is None or not self.edge.base_url.strip():
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

