"""coHound — onboarding-префлайт продукта (09 §5/§7.5, MVP-1).

Фаза ``onboarding`` стоит между ``plan`` и ``g1``: до краула проверяет, что у каждого
продукта когорты есть хотя бы один активный seed и он привязан к существующему профилю
домена. Неготовый продукт блокирует прогон (O1), а не приводит к «тихому пустому»
корпусу.

Источники seed'ов резолвятся **по продукту** (§7.5):

``--urls`` (полная замена) > ``sources/seeds.yaml[product]`` (даже если пусто)
> ``sources/corpus_urls.yaml[product]`` (legacy fallback с warning) > ``[]``.

MVP-1 не читает и не пишет durable ``state.db``: режим продукта всегда ``new``.
MVP-2a добавляет :func:`resolve_product_modes`: при переданном ``ProductStatePort``
режим продукта выводится из durable-рубежей (``last_corpus_run_id``/
``last_evaluated_run_id``) и последнего **полного** снимка (09 §5.1–§5.3). Без порта
(``state=None``) поведение прежнее: все готовые продукты — ``new``.
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
from core.ports.state import ProductStatePort, ProductStateRow

__all__ = [
    "OnboardingPreflight",
    "effective_seeds",
    "load_effective_seeds",
    "preflight",
    "resolve_product_modes",
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
    :param cycle_id: идентификатор цикла прогона (run identity — отдельный срез).
    :param previous_run_id: предыдущий прогон пакета (lineage §12.3).
    :param change_class: класс изменения (``methodology_change``/``execution_change``).
    """

    plans: list[ProductPlan] = field(default_factory=list)
    blocked: list[str] = field(default_factory=list)
    effective_urls: dict[str, list[str]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    seeds: SeedsConfig = field(default_factory=SeedsConfig)
    cycle_id: str | None = None
    previous_run_id: str | None = None
    change_class: str | None = None

    @property
    def plan_set(self) -> ProductPlanSet:
        """Доменный набор планов для отчёта/dry-run."""
        return ProductPlanSet(
            products=list(self.plans),
            blocked=list(self.blocked),
            cycle_id=self.cycle_id,
            previous_run_id=self.previous_run_id,
            change_class=self.change_class,
        )

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


def _mode_for(
    state: ProductStatePort | None,
    product_id: str,
    row: ProductStateRow | None,
) -> ProductMode:
    """Режим продукта по durable-состоянию (09 §5.1/§5.3).

    * нет строки state или ``last_corpus_run_id is None`` → ``new``;
    * ``re_added_at`` выставлен (вернули после ``retired``) → ``new``;
    * ``last_evaluated_run_id is None`` (corpus был, eval — нет) → ``full``;
    * последний **полный** снимок отсутствует или не ``complete`` → ``full``;
    * иначе → ``incremental``.

    ``state is None`` (durable-состояния нет) — поведение MVP-1: всегда ``new``.
    """
    if state is None or row is None or row.last_corpus_run_id is None:
        return ProductMode.NEW
    if row.re_added_at is not None:
        return ProductMode.NEW
    if row.last_evaluated_run_id is None:
        return ProductMode.FULL
    snapshot = state.last_snapshot(product_id, scope=ProductScope.FULL.value)
    if snapshot is None or snapshot.status != "complete":
        return ProductMode.FULL
    return ProductMode.INCREMENTAL


def resolve_product_modes(
    products: Iterable[str],
    seeds: SeedsConfig,
    *,
    profiles: Mapping[str, Any] | None = None,
    cycle_id: str | None = None,
    previous_run_id: str | None = None,
    change_class: str | None = None,
    scope: ProductScope = ProductScope.FULL,
    state: ProductStatePort | None = None,
) -> OnboardingPreflight:
    """Резолвит режим каждого продукта когорты (09 §5.3).

    Готовность (блокировка) определяется **авторским** seed-набором ``seeds``, а не
    state-строкой: отсутствие строки state — нормальный первый прогон продукта и не
    даёт ``no_seeds``. Режим же выводится из durable-рубежей (:func:`_mode_for`).

    :param products: ``product_id`` когорты (детерминированно сортируются).
    :param seeds: эффективный ``SeedsConfig`` (``sources/seeds.yaml``/fallback).
    :param profiles: доменные профили для привязки seed'ов.
    :param cycle_id: идентификатор цикла (run identity — отдельный срез).
    :param previous_run_id: предыдущий прогон пакета (lineage).
    :param change_class: класс изменения (``methodology_change``/``execution_change``).
    :param scope: ``full``|``product_only`` (09 §6.2/§13.6).
    :param state: durable-состояние продукта; ``None`` — все готовые продукты ``new``.
    """
    product_ids = sorted({str(pid) for pid in products})
    plans: list[ProductPlan] = []
    blocked: list[str] = []
    effective_urls: dict[str, list[str]] = {}
    for product_id in product_ids:
        product = seeds.products.get(product_id)
        reasons = _reasons_for(product_id, product, profiles)
        rows = effective_seeds(seeds, product_id, profiles=profiles)
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
        state_row = state.get_product_state(product_id) if state is not None else None
        mode = _mode_for(state, product_id, state_row)
        plans.append(
            ProductPlan(
                product_id=product_id,
                mode=mode,
                onboarding_state=OnboardingState.READY,
                seed_count=len(rows),
                refresh_policy=(
                    RefreshPolicy.REVALIDATE
                    if mode is ProductMode.INCREMENTAL
                    else RefreshPolicy.REFRESH
                ),
                scope=scope,
                reasons=[],
                last_completed_run_id=(
                    state_row.last_completed_run_id if state_row is not None else None
                ),
                last_corpus_run_id=(
                    state_row.last_corpus_run_id if state_row is not None else None
                ),
                last_evaluated_run_id=(
                    state_row.last_evaluated_run_id if state_row is not None else None
                ),
            )
        )
        effective_urls[product_id] = [seed.normalized_url for seed in rows]
    return OnboardingPreflight(
        plans=plans,
        blocked=blocked,
        effective_urls=effective_urls,
        seeds=seeds,
        cycle_id=cycle_id,
        previous_run_id=previous_run_id,
        change_class=change_class,
    )


def preflight(
    package_dir: str | Path,
    products: Iterable[str],
    *,
    profiles: Mapping[str, Any] | None = None,
    urls_override: Mapping[str, list[str]] | None = None,
    today: _dt.date | None = None,
    scope: ProductScope = ProductScope.FULL,
    state: ProductStatePort | None = None,
    cycle_id: str | None = None,
    previous_run_id: str | None = None,
    change_class: str | None = None,
) -> OnboardingPreflight:
    """Строит ``ProductPlanSet`` и блокирует неготовые продукты (O1).

    Продукт без активного seed'а или с несвязанным профилем попадает в ``blocked`` с
    причинами ``no_seeds``/``profile_unbound``/``seed_invalid``. Режим продукта
    резолвится :func:`resolve_product_modes` по переданному ``state`` (без него — ``new``).

    Синхронизация авторских seed'ов в ``state.seed_set`` — обязанность composition root
    (:meth:`ProductStatePort.reconcile_seed_set`), а не префлайта: файл — источник истины,
    таблица — операционный кэш.
    """
    product_ids = sorted({str(pid) for pid in products})
    config, warnings = load_effective_seeds(
        package_dir,
        product_ids,
        profiles=profiles,
        urls_override=urls_override,
        today=today,
    )
    result = resolve_product_modes(
        product_ids,
        config,
        profiles=profiles,
        cycle_id=cycle_id,
        previous_run_id=previous_run_id,
        change_class=change_class,
        scope=scope,
        state=state,
    )
    result.warnings = warnings
    return result
