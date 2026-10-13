"""coHound — резолвер цели краула по слоям источников (Lane A: source-routing contracts).

Чистая функция ``(product_id, url)`` → :class:`CrawlTarget`: собирает оси получения/извлечения/
стратегии из независимых слоёв ``crawl_sources.yaml`` (пакетный артефакт, D-серия), профилей
доменов и дефолтов прогона. Разрешение выполняется **в pipeline** (не в адаптерах), чтобы
краулер получал готовый value-объект.

Приоритет слоёв (сверху вниз — побеждает высший):

1. ``explicit`` (``--render-modes``) — переопределяет **только** ось ``fetch``;
2. site-слой: ``sources.sites[product_id]`` — лучший ``host[+path]``-ключ;
3. product-слой: ``sources.products[product_id]``;
4. ``domain_profiles`` (аргумент ``profiles``) — лучший host/path-профиль;
5. ``run.yaml:crawl`` (``run_default_strategy``/``run_default_rate_limit``);
6. ``sources.defaults`` — дефолт пакета;
7. дефолт адаптера/deploy (``fetch=HTTP``, ``extract=AUTO``, ``strategy=AUTO``,
   ``wait_for=None``, ``rate_limit=None``, ``engine=None``).

Каждая ось строится независимо: слои сканируются от низкого к высокому, берётся последнее
не-``None`` значение. ``engine``/``kind`` (строки-данные, не доменные enum) разрешаются из
слоёв defaults → product → site; ``params`` сливаются в том же порядке (поздний ключ побеждает)
и возвращаются read-only. Итоговый план нормализуется по инварианту осей: при ``fetch=HTTP``
``strategy`` приводится к ``AUTO``.

Нейтрален к предметной категории (§1.3): источники/домены — данные.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any, TypeVar

from core.config.crawl_sources import CrawlSource, CrawlSourcesConfig
from core.config.domain_profiles import DomainProfile
from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.domain.models import RateLimitSpec
from core.pipeline.fetch_plan import select_domain_profile, select_site_source
from core.ports.types import FetchPlan
from core.util.hashing import short_hash

__all__ = ["CrawlTarget", "crawl_sources_digest", "resolve_crawl_target"]

_T = TypeVar("_T")


@dataclass(frozen=True)
class CrawlTarget:
    """Разрешённая цель краула: план, идентификаторы движка/семантики и merged per-engine params.

    ``engine``/``kind`` — свободные строки-данные (валидируются composition root); ``params`` —
    read-only словарь, слитый по слоям ``defaults`` → ``product`` → ``site``.
    """

    plan: FetchPlan
    engine: str | None
    kind: str | None
    params: Mapping[str, Any]


def _last(*values: _T | None) -> _T | None:
    """Последнее не-``None`` значение; слои передаются от низкого приоритета к высокому."""
    result: _T | None = None
    for value in values:
        if value is not None:
            result = value
    return result


def crawl_sources_digest(sources: CrawlSourcesConfig) -> str:
    """Детерминированный отпечаток конфигурации ``crawl_sources.yaml`` (§4.4, Phase 2).

    Канонизация: ``model_dump(mode="json", exclude_none=True)`` → ``json.dumps`` с
    ``sort_keys=True``/``ensure_ascii=False`` (порядко-независимо, без ``None``-шумов) →
    :func:`core.util.hashing.short_hash`. Одинаковый смысл конфигурации → один отпечаток;
    любое содержательное изменение (движок/оси/``params``) → другой. Информационный маркер
    провенанса, а не версионная граница (§13).
    """
    canonical = json.dumps(
        sources.model_dump(mode="json", exclude_none=True),
        sort_keys=True,
        ensure_ascii=False,
    )
    return short_hash(canonical, 24)


def resolve_crawl_target(
    product_id: str,
    url: str,
    *,
    sources: CrawlSourcesConfig,
    profiles: Mapping[str, DomainProfile],
    explicit: Mapping[str, FetchMode] | None = None,
    run_default_strategy: CrawlStrategy = CrawlStrategy.AUTO,
    run_default_rate_limit: RateLimitSpec | None = None,
) -> CrawlTarget:
    """Разрешает :class:`CrawlTarget` для ``(product_id, url)`` по слоям источников.

    :param product_id: идентификатор продукта (ключ слоёв ``products``/``sites``).
    :param url: целевой URL.
    :param sources: конфиг ``crawl_sources.yaml`` (слои defaults/products/sites).
    :param profiles: профили доменов из ``domain_profiles.yaml`` (D.2).
    :param explicit: явные переопределения ``{url: FetchMode}`` (``--render-modes``); влияют
        **только** на ``fetch`` и побеждают все слои.
    :param run_default_strategy: дефолт оси стратегии из run.yaml ``crawl``.
    :param run_default_rate_limit: дефолт диапазона задержек из run.yaml ``crawl``.
    """
    defaults = sources.defaults
    product: CrawlSource | None = sources.products.get(product_id)
    site_map = sources.sites.get(product_id)
    site: CrawlSource | None = select_site_source(url, site_map) if site_map else None
    profile: DomainProfile | None = select_domain_profile(url, profiles)
    explicit_fetch = explicit.get(url) if explicit else None

    # Оси: низкий → высокий приоритет (последнее не-None побеждает).
    fetch = _last(
        FetchMode.HTTP,  # дефолт адаптера/deploy
        defaults.fetch,
        profile.fetch if profile is not None else None,
        product.fetch if product is not None else None,
        site.fetch if site is not None else None,
        explicit_fetch,
    ) or FetchMode.HTTP
    extract = _last(
        ExtractionRoute.AUTO,
        defaults.extract,
        profile.extract if profile is not None else None,
        product.extract if product is not None else None,
        site.extract if site is not None else None,
    ) or ExtractionRoute.AUTO
    strategy = _last(
        CrawlStrategy.AUTO,
        defaults.strategy,
        run_default_strategy,
        profile.strategy if profile is not None else None,
        product.strategy if product is not None else None,
        site.strategy if site is not None else None,
    ) or CrawlStrategy.AUTO
    wait_for = _last(
        defaults.wait_for,
        profile.wait_for if profile is not None else None,
        product.wait_for if product is not None else None,
        site.wait_for if site is not None else None,
    )
    rate_limit = _last(
        defaults.rate_limit,
        run_default_rate_limit,
        profile.rate_limit if profile is not None else None,
        product.rate_limit if product is not None else None,
        site.rate_limit if site is not None else None,
    )

    # Строки-данные: site > product > defaults (у профилей доменов engine/kind нет).
    engine = _last(
        defaults.engine,
        product.engine if product is not None else None,
        site.engine if site is not None else None,
    )
    kind = _last(
        defaults.kind,
        product.kind if product is not None else None,
        site.kind if site is not None else None,
    )

    # Слияние per-engine params: defaults → product → site (поздний ключ побеждает).
    merged: dict[str, Any] = {}
    merged.update(defaults.params)
    if product is not None:
        merged.update(product.params)
    if site is not None:
        merged.update(site.params)
    params: Mapping[str, Any] = MappingProxyType(merged)

    plan = FetchPlan(
        fetch,
        extract=extract,
        wait_for=wait_for,
        strategy=strategy,
        rate_limit=rate_limit,
        engine=engine,
    )

    # Нормализация осей (G1): при HTTP-фетче стратегия браузера бессмысленна → AUTO.
    if plan.fetch is FetchMode.HTTP:
        plan = replace(plan, strategy=CrawlStrategy.AUTO)

    return CrawlTarget(plan=plan, engine=engine, kind=kind, params=params)
