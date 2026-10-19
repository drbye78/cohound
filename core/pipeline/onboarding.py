"""coHound — onboarding-префлайт продукта (09 §5/§7.5, MVP-1).

Фаза ``onboarding`` стоит между ``plan`` и ``g1``: до краула проверяет, что у каждого
продукта когорты есть хотя бы один активный seed и он привязан к существующему профилю
домена. Неготовый продукт блокирует прогон (O1), а не приводит к «тихому пустому»
корпусу.

Источники seed'ов резолвятся **по продукту** (§7.5):

``--urls`` (полная замена) > ``sources/seeds.yaml[product]`` (даже если пусто)
> ``sources/corpus_urls.yaml[product]`` (legacy fallback с warning) > ``[]``.

MVP-1 не читает и не пишет durable ``state.db``: режим продукта всегда ``new``.
Модуль категорийно нейтрален (§1.3).
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.config.seeds import (
    CORPUS_URLS_RELPATH,
    SEEDS_RELPATH,
    load_corpus_urls,
    load_seeds,
    resolve_profile_key,
    validate_seed_url,
)
from core.domain.enums import (
    OnboardingState,
    ProductMode,
    ProductScope,
    RefreshPolicy,
    SeedRole,
    SeedSource,
    SeedState,
)
from core.domain.models import (
    ProductPlan,
    ProductPlanSet,
    ProductSeeds,
    SeedEntry,
    SeedsConfig,
)

__all__ = [
    "OnboardingPreflight",
    "effective_seeds",
    "load_effective_seeds",
    "preflight",
]

#: Причина блокировки: у продукта нет активных seed'ов.
REASON_NO_SEEDS = "no_seeds"
#: Причина блокировки: seed не привязан к существующему профилю домена.
REASON_PROFILE_UNBOUND = "profile_unbound"
#: Причина блокировки: seed невалиден (схема/SSRF/состояние).
REASON_SEED_INVALID = "seed_invalid"


@dataclass
class OnboardingPreflight:
    """Результат префлайта: планы продуктов + эффективный seed-набор.

    :param plans: планы каждого продукта когорты (детерминированный порядок).
    :param blocked: ``product_id`` неготовых продуктов (порядок планов).
    :param effective_urls: ``product_id → [normalized_url, ...]`` для фазы corpus.
    :param warnings: машинные предупреждения (legacy fallback, operator override).
    :param seeds: эффективный ``SeedsConfig`` (authoring-источник, не state).
    """

    plans: list[ProductPlan] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)
    effective_urls: dict[str, list[str]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    seeds: SeedsConfig = field(default_factory=SeedsConfig)

    @property
    def plan_set(self) -> ProductPlanSet:
        """Доменный набор планов для отчёта/dry-run."""
        return ProductPlanSet(products=list(self.plans), blocked=list(self.blocked))

    @property
    def ready(self) -> bool:
        """Все продукты готовы (нет заблокированных)."""
        return not self.blocked

    def reasons_for(self, product_id: str) -> list[str]:
        """Причины блокировки продукта (пусто — продукт готов)."""
        plan = self.plan_set.plan_for(product_id)
        return list(plan.reasons) if plan is not None else []


def _entry(
    *,
    url: str,
    role: SeedRole,
    source: SeedSource,
    source_ref: str | None,
    profiles: Mapping[str, Any] | None,
    warnings: list[str],
    today: _dt.date,
) -> SeedEntry | None:
    """Строит seed из URL/источника; невалидный URL → warning и ``None``."""
    try:
        normalized = validate_seed_url(url, warnings=warnings)
    except ValueError as exc:
        warnings.append(f"seed_invalid: {url}: {exc}")
        return None
    return SeedEntry(
        url=str(url).strip(),
        normalized_url=normalized,
        role=role,
        source=source,
        source_ref=source_ref,
        profile_key=resolve_profile_key(normalized, profiles),
        added_at=today,
        added_by="operator" if source is SeedSource.OPERATOR else "migration",
        enabled=True,
        state=SeedState.ENABLED,
    )


def load_effective_seeds(
    package_dir: str | Path,
    products: Iterable[str],
    *,
    profiles: Mapping[str, Any] | None = None,
    urls_override: Mapping[str, list[str]] | None = None,
    today: _dt.date | None = None,
) -> tuple[SeedsConfig, list[str]]:
    """Резолвит эффективный seed-набор по продуктам (§7.5).

    ``urls_override`` (``--urls``) **полностью** заменяет пакетный набор для всех
    продуктов: отсутствующий продукт получает пустой набор (и будет заблокирован).
    Иначе ``seeds.yaml[product]`` приоритетнее legacy ``corpus_urls.yaml[product]``;
    отсутствие продукта в обоих — пустой набор.

    Возвращает ``(config, warnings)``. Warnings машинные: ``seeds_legacy_fallback:<id>``,
    ``seeds_operator_override``.
    """
    snapshot = today or _dt.date.today()
    product_ids = sorted({str(pid) for pid in products})
    warnings: list[str] = []
    if urls_override is not None:
        warnings.append(
            "seeds_operator_override: --urls полностью заменяет seeds.yaml "
            "для всех продуктов"
        )
        configured: dict[str, ProductSeeds] = {}
        for product_id in product_ids:
            entries = [
                entry
                for url in urls_override.get(product_id, []) or []
                if (
                    entry := _entry(
                        url=str(url),
                        role=SeedRole.PRIMARY,
                        source=SeedSource.OPERATOR,
                        source_ref="--urls",
                        profiles=profiles,
                        warnings=warnings,
                        today=snapshot,
                    )
                )
                is not None
            ]
            configured[product_id] = ProductSeeds(
                state=(
                    OnboardingState.READY
                    if entries
                    else OnboardingState.PENDING_SEEDS
                ),
                seeds=entries,
            )
        return SeedsConfig(products=configured), warnings

    pkg = Path(package_dir)
    seeds_path = pkg / SEEDS_RELPATH
    config = load_seeds(seeds_path) if seeds_path.is_file() else SeedsConfig()
    legacy = load_corpus_urls(pkg / CORPUS_URLS_RELPATH)
    configured = dict(config.products)
    for product_id in product_ids:
        if product_id in configured:
            continue
        if product_id not in legacy:
            continue
        entries = [
            entry
            for url in legacy[product_id]
            if (
                entry := _entry(
                    url=str(url),
                    role=SeedRole.PRIMARY,
                    source=SeedSource.MIGRATION,
                    source_ref=CORPUS_URLS_RELPATH,
                    profiles=profiles,
                    warnings=warnings,
                    today=snapshot,
                )
            )
            is not None
        ]
        configured[product_id] = ProductSeeds(
            state=(
                OnboardingState.READY if entries else OnboardingState.PENDING_SEEDS
            ),
            seeds=entries,
        )
        warnings.append(
            f"seeds_legacy_fallback: {product_id} "
            f"(нет записи в {SEEDS_RELPATH}; использован {CORPUS_URLS_RELPATH})"
        )
    return SeedsConfig(schema=config.schema_, products=configured), warnings


def effective_seeds(
    config: SeedsConfig, product_id: str, *, profiles: Mapping[str, Any] | None
) -> list[SeedEntry]:
    """Активные seed'ы продукта с доразрешённым ``profile_key`` (для corpus/evaluate)."""
    rows: list[SeedEntry] = []
    for seed in config.enabled_for(product_id):
        key = seed.profile_key or resolve_profile_key(seed.url, profiles)
        rows.append(seed.model_copy(update={"profile_key": key}) if key else seed)
    return rows


def _reasons_for(
    product_id: str,
    product: ProductSeeds | None,
    profiles: Mapping[str, Any] | None,
) -> list[str]:
    """Причины неготовности продукта: ``no_seeds``/``profile_unbound``/``seed_invalid``."""
    reasons: list[str] = []
    if product is None or product.state is OnboardingState.PENDING_SEEDS:
        reasons.append(REASON_NO_SEEDS)
    if product is None:
        return reasons
    if product.state in (OnboardingState.BLOCKED, OnboardingState.RETIRED):
        reasons.append(product.state.value)
    active = product.enabled_for()
    if not active and REASON_NO_SEEDS not in reasons:
        reasons.append(REASON_NO_SEEDS)
    for seed in active:
        if seed.state is not SeedState.ENABLED:
            reasons.append(f"{REASON_SEED_INVALID}:{seed.normalized_url}")
            continue
        key = seed.profile_key or resolve_profile_key(seed.url, profiles)
        if not key or not profiles or key not in profiles:
            reasons.append(f"{REASON_PROFILE_UNBOUND}:{seed.normalized_url}")
    # Детерминированный порядок без дублей.
    return list(dict.fromkeys(reasons))


def preflight(
    package_dir: str | Path,
    products: Iterable[str],
    *,
    profiles: Mapping[str, Any] | None = None,
    urls_override: Mapping[str, list[str]] | None = None,
    today: _dt.date | None = None,
    scope: ProductScope = ProductScope.FULL,
) -> OnboardingPreflight:
    """Строит ``ProductPlanSet`` и блокирует неготовые продукты (O1).

    В MVP-1 режим продукта всегда ``new`` (durable state отсутствует). Продукт без
    активного seed'а или с несвязанным профилем попадает в ``blocked`` с причинами
    ``no_seeds``/``profile_unbound``/``seed_invalid``.
    """
    product_ids = sorted({str(pid) for pid in products})
    config, warnings = load_effective_seeds(
        package_dir,
        product_ids,
        profiles=profiles,
        urls_override=urls_override,
        today=today,
    )
    plans: list[ProductPlan] = []
    blocked: list[str] = []
    effective_urls: dict[str, list[str]] = {}
    for product_id in product_ids:
        product = config.products.get(product_id)
        reasons = _reasons_for(product_id, product, profiles)
        rows = effective_seeds(config, product_id, profiles=profiles) if product else []
        if reasons:
            plans.append(
                ProductPlan(
                    product_id=product_id,
                    mode=ProductMode.NEW,
                    onboarding_state=(
                        product.state
                        if product is not None
                        and product.state
                        in (OnboardingState.BLOCKED, OnboardingState.RETIRED)
                        else OnboardingState.PENDING_SEEDS
                    ),
                    seed_count=len(rows),
                    refresh_policy=RefreshPolicy.REVALIDATE,
                    scope=scope,
                    reasons=reasons,
                )
            )
            blocked.append(product_id)
            effective_urls[product_id] = []
            continue
        plans.append(
            ProductPlan(
                product_id=product_id,
                mode=ProductMode.NEW,
                onboarding_state=OnboardingState.READY,
                seed_count=len(rows),
                refresh_policy=RefreshPolicy.REFRESH,
                scope=scope,
                reasons=[],
            )
        )
        effective_urls[product_id] = [seed.normalized_url for seed in rows]
    return OnboardingPreflight(
        plans=plans,
        blocked=blocked,
        effective_urls=effective_urls,
        warnings=warnings,
        seeds=config,
    )
