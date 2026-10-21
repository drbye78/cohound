"""coHound — загрузчик ``domain_profiles.yaml`` (D2, ``03`` §D.2).

Профиль домена описывает независимые оси получения/извлечения/стратегии контента:

* ``fetch``    — ``http`` | ``browser`` (HTTP-фетч или браузерный рендер JS);
* ``extract``  — ``auto`` | ``html`` | ``document`` (авто-детект по формату | HTML | документ);
* ``strategy`` — ``auto`` | ``honest`` | ``hybrid`` (как используется браузер, D4);
* ``wait_for`` — опциональное JS-условие готовности для браузерного рендера;
* ``rate_limit`` — опциональный диапазон uniform(min_s, max_s) между запросами;
* ``sitemap``  — опциональный URL публичного ``sitemap.xml`` (D3): его ``<loc>`` добавляются
  дополнительными depth-0 seeds (сам sitemap — не страница).

Формат::

    example.com:        {fetch: browser, extract: html}
    parent.test/path:   {fetch: browser, extract: document, strategy: hybrid}
    docs.example:       {sitemap: "https://docs.example/sitemap.xml"}

Ключ — ``host`` с опциональным ``/path``; сопоставление и приоритет профилей —
ответственность pipeline (:func:`core.pipeline.fetch_plan.resolve_fetch_plan`), не адаптеров.
Схема строгая (``extra="forbid"``): неизвестные поля (``render``/``index``/``spa``) отвергаются.

Нейтрален к предметной категории (§1.3). Зависимости слоя ``config`` — ``domain``/``config``/
``util``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from core.domain.enums import CrawlStrategy, DocumentLevel, ExtractionRoute, FetchMode
from core.domain.models import RateLimitSpec

__all__ = ["DomainProfile", "load_domain_profiles"]


class DomainProfile(BaseModel):
    """Профиль домена: оси ``fetch``/``extract``/``strategy`` + JS-ожидание и rate-limit (D2/D4).

    ``strategy``/``rate_limit`` опциональны: ``None`` → наследуется дефолт ``run.yaml``
    (секция ``crawl``); при её отсутствии — ``CrawlStrategy.AUTO`` без ограничения задержек.
    """

    model_config = ConfigDict(extra="forbid")

    fetch: FetchMode = FetchMode.HTTP
    extract: ExtractionRoute = ExtractionRoute.AUTO
    wait_for: str | None = None
    strategy: CrawlStrategy | None = None
    rate_limit: RateLimitSpec | None = None
    # D1/D2: per-site слой осей контроля краула (высший приоритет цепочки app < run < site).
    # ``None`` = «не задано на этом слое» (наследуется run/app default). Резолв —
    # :func:`core.pipeline.crawl_frontier.resolve_crawl_limits`. Схема остаётся строгой
    # (``extra="forbid"``): неизвестные поля отвергаются по-прежнему.
    max_workers: int | None = Field(default=None, ge=1)
    # P4: per-site override отдельной оси CPU-параллелизма экстракции (высший приоритет
    # цепочки app < run < site). ``None`` = не задано на этом слое (наследуется run/app).
    extract_workers: int | None = Field(default=None, ge=1)
    # P4/OOM: per-site override рециклинга воркеров локальной экстракции (высший приоритет
    # цепочки app < run < site). ``None`` = не задано на этом слое (наследуется run/app).
    extract_max_tasks_per_child: int | None = Field(default=None, ge=1)
    # P0/OOM: per-site потолок одного ответа/retained-файла (байты; высший приоритет
    # цепочки app < run < site). ``None`` = не задано на этом слое.
    max_response_bytes: int | None = Field(default=None, gt=0)
    max_depth: int | None = Field(default=None, ge=0)
    max_documents: int | None = Field(default=None, gt=0)
    # P-rev: per-site слой операторского потолка ревалидации известных URL на продукт.
    # ``None`` = не задано на этом слое (наследуется run/app). Ось применяется **к продукту**
    # целиком (все корни + orphan-бакеты), а не к отдельному корню.
    revalidate_max_documents: int | None = Field(default=None, ge=1)
    exclude_paths: list[str] | None = None
    exclude_documents: list[str] | None = None
    # P5: per-site слой уровня извлечения документных форматов (высший приоритет цепочки
    # app < run < site). ``None`` = не задано на этом слое (наследуется run/app). Это
    # операторский escape hatch от «слишком медленного PDF» — без правки кода.
    document_level: DocumentLevel | None = None
    # D3: per-site URL публичного ``sitemap.xml`` (опц.). ``None`` = не задан (поведение
    # неизменно). ``<loc>`` sitemap становятся **дополнительными** depth-0 seeds (не заменяют
    # seed-URL оператора); сам sitemap — не страница (не ретейнится/не ингестится, I19).
    # Развёртка ``sitemapindex`` — один уровень (потолки в ``core.util.sitemap``).
    sitemap: str | None = None

    @field_validator("exclude_paths", "exclude_documents")
    @classmethod
    def _check_globs(cls, value: list[str] | None) -> list[str] | None:
        """Отвергает не-строки/пустые glob (D1/H)."""
        if value is None:
            return None
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError(
                    "domain_profiles.exclude_paths/exclude_documents: "
                    "ожидались непустые строки-glob"
                )
        return value

    @field_validator("sitemap")
    @classmethod
    def _check_sitemap(cls, value: str | None) -> str | None:
        """Отвергает пустой/не-``http(s)`` URL sitemap (D3): ранняя ошибка конфигурации."""
        if value is None:
            return None
        text = value.strip()
        if not text:
            raise ValueError("domain_profiles.sitemap: ожидается непустой URL")
        if not text.lower().startswith(("http://", "https://")):
            raise ValueError(
                "domain_profiles.sitemap: ожидается http(s)-URL "
                "(например https://example.com/sitemap.xml)"
            )
        return text


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


def load_domain_profiles(path: str | Path) -> dict[str, DomainProfile]:
    """Загружает ``domain_profiles.yaml`` (D.2) в ``dict[str, DomainProfile]``.

    Корень — YAML-словарь, каждое значение — профиль домена (строгая схема
    :class:`DomainProfile`). Неизвестные поля профиля (напр. старые ``render``/``index``)
    отвергаются (``extra="forbid"``).

    :raises ValueError: нечитаемый/некорректный YAML; корень не словарь; профиль не словарь
        либо не проходит валидацию.
    """
    data = _read_yaml(path)
    if not isinstance(data, dict):
        raise ValueError(
            f"domain_profiles.yaml: ожидался YAML-словарь верхнего уровня, "
            f"получено {type(data).__name__}"
        )
    profiles: dict[str, DomainProfile] = {}
    for key, profile in data.items():
        if not isinstance(profile, dict):
            raise ValueError(
                f"domain_profiles.yaml[{key}]: ожидался словарь профиля, "
                f"получено {type(profile).__name__}"
            )
        try:
            profiles[str(key)] = DomainProfile.model_validate(profile)
        except ValidationError as exc:
            raise ValueError(
                f"domain_profiles.yaml[{key}]: некорректный профиль домена: {exc}"
            ) from exc
    return profiles
