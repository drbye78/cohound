"""coHound — загрузчик ``crawl_sources.yaml`` (Lane A: source-routing contracts).

Пакетный артефакт ``packages/{case_id}/crawl_sources.yaml`` описывает источники краула по
слоям (приоритет от низкого к высокому в pipeline-резолвере):

* ``defaults`` — дефолтный источник пакета (fallback для всех продуктов);
* ``products`` — источник на продукт (``product_id`` → :class:`CrawlSource`);
* ``sites``    — источник на сайт продукта (``product_id`` → ``{host[+path] -> CrawlSource}``).

Каждый :class:`CrawlSource` несёт независимые оси получения/извлечения/стратегии, а также
строковые ``kind``/``engine`` (семантика источника и идентификатор движка транспорта —
строки-данные, не доменные enum) и свободный per-engine ``params`` (``dict[str, Any]``).
Строгая per-engine схема ``params`` — Phase 2/3, здесь не реализована: любые ключи
принимаются как есть (текущее поведение зафиксировано тестом).

Формат::

    defaults:        {kind: web, engine: http}
    products:
      p1:            {engine: scrapy, strategy: honest}
    sites:
      p1:
        example.com: {kind: feed, params: {selector: "item"}}

Схема строгая (``extra="forbid"``): неизвестные поля отвергаются. Нейтрален к предметной
категории (§1.3). Зависимости слоя ``config`` — ``domain``/``config``/``util``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.domain.models import RateLimitSpec

__all__ = [
    "CRAWL_SOURCES_FILENAME",
    "CrawlSource",
    "CrawlSourcesConfig",
    "load_crawl_sources",
]

#: Имя пакетного артефакта источников краула.
CRAWL_SOURCES_FILENAME = "crawl_sources.yaml"


class CrawlSource(BaseModel):
    """Источник краула: строковые ``kind``/``engine``, оси получения/извлечения и ``params``.

    ``kind`` — семантика источника (``web``/``feed``/``channel``/…); ``engine`` — идентификатор
    движка транспорта (``http``/``scrapy``/``browser``/…). Оба — свободные строки-данные,
    валидируются composition root, а не доменным enum.

    Оси ``fetch``/``extract``/``strategy``/``wait_for``/``rate_limit`` опциональны: ``None``
    означает «не задано на этом слое» — значение наследуется слоем ниже; ``params`` — свободный
    per-engine ``dict[str, Any]`` настроек (сливается по слоям, поздний ключ побеждает).
    Строгая схема по конкретному ``engine`` — Phase 2/3 (отложено): здесь принимаются любые
    ключи без валидации.
    """

    model_config = ConfigDict(extra="forbid")

    kind: str | None = None
    engine: str | None = None
    fetch: FetchMode | None = None
    extract: ExtractionRoute | None = None
    strategy: CrawlStrategy | None = None
    wait_for: str | None = None
    rate_limit: RateLimitSpec | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class CrawlSourcesConfig(BaseModel):
    """Корень ``crawl_sources.yaml``: слои ``defaults`` → ``products`` → ``sites``.

    ``sites`` вложен как ``product_id`` → ``{host[+path] -> CrawlSource}``; сопоставление
    ключей host/path — ответственность pipeline
    (:func:`core.pipeline.crawl_sources.resolve_crawl_target`), не загрузчика.
    """

    model_config = ConfigDict(extra="forbid")

    defaults: CrawlSource = Field(default_factory=CrawlSource)
    products: dict[str, CrawlSource] = Field(default_factory=dict)
    sites: dict[str, dict[str, CrawlSource]] = Field(default_factory=dict)

    @field_validator("sites")
    @classmethod
    def _check_site_keys(
        cls, value: dict[str, dict[str, CrawlSource]]
    ) -> dict[str, dict[str, CrawlSource]]:
        """Site-ключ обязан быть ``host[+path]`` без схемы URL (C4).

        URL-ключ (``https://example.com/x``) иначе молча даёт host=``https:`` при
        разборе в pipeline-резолвере; пустой ключ не имеет смысла. Сообщение — с именем
        продукта и ошибочным ключом.
        """
        for product_id, hosts in value.items():
            for key in hosts:
                if not key or not key.strip():
                    raise ValueError(f"sites[{product_id}]: пустой ключ сайта")
                if "://" in key:
                    raise ValueError(
                        f"sites[{product_id}]: ключ {key!r} содержит схему URL — "
                        f"ожидается host[+path] (например example.com или example.com/docs)"
                    )
        return value


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


def load_crawl_sources(path: str | Path) -> CrawlSourcesConfig:
    """Загружает ``crawl_sources.yaml`` (Lane A) в :class:`CrawlSourcesConfig`.

    Пустой/``None`` корень трактуется как конфиг по умолчанию (:class:`CrawlSourcesConfig` без
    слоёв). Иначе корень обязан быть YAML-словарём, который проходит строгую схему
    (``extra="forbid"``); неизвестные поля отвергаются.

    :raises ValueError: нечитаемый/некорректный YAML; корень не словарь; схема не проходит
        валидацию (неизвестное поле, недопустимое значение enum, не-mapping слой и т.п.).
    """
    data = _read_yaml(path)
    if data is None:
        return CrawlSourcesConfig()
    if not isinstance(data, dict):
        raise ValueError(
            f"{CRAWL_SOURCES_FILENAME}: ожидался YAML-словарь верхнего уровня, "
            f"получено {type(data).__name__}"
        )
    try:
        return CrawlSourcesConfig.model_validate(data)
    except ValidationError as exc:
        raise ValueError(
            f"{CRAWL_SOURCES_FILENAME}: некорректная схема источников краула: {exc}"
        ) from exc
