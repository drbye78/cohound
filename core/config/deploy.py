"""coHound — загрузчик конфига развёртывания (``config/deploy.yaml``, 07 §7 / 03 D.5, W13/T13.4).

Развёртывание отделено от методологического прогона (``run.yaml``): этот файл выбирает
**профиль зон** (``local`` | ``split`` | ``split-hardened``) и адреса DMZ-шва (edge):
base_url, TLS, аутентификация входящих, лимиты, экспорт аудита, зона экстрактора и
бюджет/адаптер внешнего поиска.

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
    "DeployConfig",
    "load_deploy_config",
    "is_split",
]

# Канонические допустимые значения (ЗЕРКАЛО 07 §7 / 03 D.5). Не выносим в доменные
# enum'ы: это конфигурация развёртывания, а не доменная методология.
PROFILES: tuple[str, ...] = ("local", "split", "split-hardened")
SPLIT_PROFILES: frozenset[str] = frozenset({"split", "split-hardened"})
AUTH_KINDS: tuple[str, ...] = ("mtls", "oidc")
EXTRACTOR_ZONES: tuple[str, ...] = ("edge", "core", "separate")
AUDIT_SINKS: tuple[str, ...] = ("file", "syslog")

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
    """

    model_config = ConfigDict(extra="forbid")

    budget: dict[str, int] = Field(default_factory=dict)
    adapter: str | None = None

    @field_validator("budget")
    @classmethod
    def _check_budget(cls, value: dict[str, int]) -> dict[str, int]:
        for lang, limit in value.items():
            if not str(lang).strip():
                raise ValueError("search.budget: пустой код языка")
            if limit < 0:
                raise ValueError(f"search.budget[{lang}]: лимит не может быть отрицательным")
        return value


class CrawlConfig(BaseModel):
    """Операционная политика HTTP-краулинга корпуса (03 D.5/D.6).

    Секция применима к лёгкому :class:`~core.adapters.crawl_http.HttpCrawler`
    (композиция — ``cli.wiring``): ``user_agent``, ``timeout_s`` (таймаут запроса, с) и
    ``follow_redirects``. Значения **не** дублируют возможности адаптера, а лишь
    выносят их в конфиг оператора (A1). Поля со значением ``None`` означают «взять
    дефолт адаптера» — так сохраняется обратная совместимость профиля ``local`` без файла.
    """

    model_config = ConfigDict(extra="forbid")

    user_agent: str | None = None
    timeout_s: float | None = Field(default=None, gt=0)
    follow_redirects: bool = True


class DeployConfig(BaseModel):
    """Профиль развёртывания и настройки зон (03 D.5).

    Для ``split``/``split-hardened`` обязателен ``edge.base_url``; для ``local``
    секция ``edge.*`` игнорируется. Секция ``crawl`` действует в обоих профилях
    (операционная политика HTTP-краулинга; 03 D.5/D.6). Отсутствие файла даёт
    безопасный дефолт ``profile: local`` (см. :func:`load_deploy_config`).
    """

    model_config = ConfigDict(extra="forbid")

    profile: str = "local"
    edge: EdgeConfig = Field(default_factory=EdgeConfig)
    extractor_zone: str = "core"
    search: SearchDeployConfig = Field(default_factory=SearchDeployConfig)
    crawl: CrawlConfig = Field(default_factory=CrawlConfig)

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
