"""coHound — загрузчик ``crawl_policy.yaml`` (план 19 §4: дефолты пакета).

Пакетный артефакт ``packages/{case_id}/crawl_policy.yaml`` необязателен и описывает
**дефолты** политики краула по трём слоям (приоритет от низкого к высокому):

* ``defaults`` — режим пакета для всех продуктов;
* ``products`` — режим на продукт (``product_id`` → :class:`CrawlPolicyEntry`);
* ``sites``    — режим на сайт продукта (``product_id`` → ``{host[+path] → entry}``).

Слой ``defaults`` — fallback: он применяется к любому объявленному корню, у которого нет
более конкретного совпадения. Ключ сайта — ``host`` или ``host/path`` без схемы URL.
Ключ без пути даёт область ``host``; с путём — ``root``. Маску внутри корня (``path``)
можно объявить явно полем ``scope_kind: path``: без него ``sites`` неотличим от корня.

Схема строгая (``extra="forbid"``): неизвестные поля отвергаются. Загрузчик только
читает файл; разрешение приоритетов и матчинг — :mod:`core.pipeline.crawl_policy`.
Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from core.domain.enums import CrawlPolicyMode, CrawlPolicyScopeKind

__all__ = [
    "CRAWL_POLICY_FILENAME",
    "CrawlPolicyConfig",
    "CrawlPolicyEntry",
    "load_crawl_policy",
    "parse_site_key",
]

#: Имя пакетного артефакта дефолтов политики краула.
CRAWL_POLICY_FILENAME = "crawl_policy.yaml"


class CrawlPolicyEntry(BaseModel):
    """Одна запись политики: режим, интервал и человеческая заметка.

    ``interval_hours`` обязателен и положителен только при ``mode='scheduled'``; при
    остальных режимах он запрещён — «интервал, который никто не читает» был бы ложной
    осью. ``scope_kind`` опционален и используется только в слое ``sites``: без него вид
    области выводится из формы ключа (``host`` → ``host``, ``host/path`` → ``root``).
    """

    model_config = ConfigDict(extra="forbid")

    mode: CrawlPolicyMode
    interval_hours: int | None = None
    note: str | None = None
    scope_kind: CrawlPolicyScopeKind | None = None

    @model_validator(mode="after")
    def _check_interval(self) -> CrawlPolicyEntry:
        if self.mode is CrawlPolicyMode.SCHEDULED:
            if self.interval_hours is None:
                raise ValueError("interval_hours обязателен при mode='scheduled'")
            if self.interval_hours <= 0:
                raise ValueError("interval_hours обязан быть положительным")
        elif self.interval_hours is not None:
            raise ValueError("interval_hours допустим только при mode='scheduled'")
        return self


class CrawlPolicyConfig(BaseModel):
    """Корень ``crawl_policy.yaml``: ``defaults`` → ``products`` → ``sites``."""

    model_config = ConfigDict(extra="forbid")

    defaults: CrawlPolicyEntry | None = None
    products: dict[str, CrawlPolicyEntry] = Field(default_factory=dict)
    sites: dict[str, dict[str, CrawlPolicyEntry]] = Field(default_factory=dict)

    @field_validator("sites")
    @classmethod
    def _check_site_keys(
        cls, value: dict[str, dict[str, CrawlPolicyEntry]]
    ) -> dict[str, dict[str, CrawlPolicyEntry]]:
        """Site-ключ обязан быть ``host[+path]`` без схемы URL; ``scope_kind`` — не product."""
        for product_id, hosts in value.items():
            for key, entry in hosts.items():
                if not key or not key.strip():
                    raise ValueError(f"sites[{product_id}]: пустой ключ сайта")
                if "://" in key:
                    raise ValueError(
                        f"sites[{product_id}]: ключ {key!r} содержит схему URL — "
                        "ожидается host[+path] (например example.com или example.com/docs)"
                    )
                if entry.scope_kind is CrawlPolicyScopeKind.PRODUCT:
                    raise ValueError(
                        f"sites[{product_id}][{key}]: scope_kind='product' несовместим "
                        "со слоем сайта"
                    )
        return value

    @model_validator(mode="after")
    def _check_layer_scope_kind(self) -> CrawlPolicyConfig:
        """``scope_kind`` осмыслен только в ``sites``; в верхних слоях он запрещён."""
        if self.defaults is not None and self.defaults.scope_kind is not None:
            raise ValueError("defaults: scope_kind не применим (вид области — product)")
        for product_id, entry in self.products.items():
            if entry.scope_kind is not None:
                raise ValueError(
                    f"products[{product_id}]: scope_kind не применим "
                    "(вид области — product)"
                )
        return self


def parse_site_key(key: str) -> tuple[str, str]:
    """``host[+path]`` → ``(host, path)``; путь нормализуется к ``/…`` без хвоста.

    Схема URL запрещена (проверяется загрузчиком); пустой путь означает область ``host``.
    """
    host, _, rest = key.strip().partition("/")
    if not host:
        raise ValueError(f"crawl_policy: пустой host в ключе {key!r}")
    path = ""
    if rest:
        path = "/" + rest.strip("/")
    return host.strip().lower(), path


def _read_yaml(path: str | Path) -> Any:
    """Читает YAML-файл; ошибки I/O и синтаксиса приводит к ``ValueError``."""
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:  # noqa: BLE001 — понятная ошибка важнее типа
        raise ValueError(f"не удалось прочитать {p}: {exc}") from exc
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"некорректный YAML в {p}: {exc}") from exc


def load_crawl_policy(path: str | Path) -> CrawlPolicyConfig:
    """Загружает ``crawl_policy.yaml`` в :class:`CrawlPolicyConfig`.

    Пустой/``None`` корень — конфиг по умолчанию (без слоёв). Иначе корень обязан быть
    словарём, проходящим строгую схему.

    :raises ValueError: нечитаемый/некорректный YAML; корень не словарь; схема не проходит
        валидацию (неизвестное поле, недопустимый режим/интервал, ключ сайта со схемой).
    """
    data = _read_yaml(path)
    if data is None:
        return CrawlPolicyConfig()
    if not isinstance(data, dict):
        raise ValueError(
            f"{CRAWL_POLICY_FILENAME}: ожидался YAML-словарь верхнего уровня, "
            f"получено {type(data).__name__}"
        )
    try:
        return CrawlPolicyConfig.model_validate(data)
    except ValidationError as exc:
        raise ValueError(
            f"{CRAWL_POLICY_FILENAME}: некорректная схема политики краула: {exc}"
        ) from exc