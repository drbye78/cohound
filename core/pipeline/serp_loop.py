"""SERP-петля: append-only раунды сборок + инвалидация затронутых ячеек (K2, W11/T11.3).

Раунд SERP — это **не** мутация существующей индексной сборки, а **новая** сборка
(``R1-serp``, ``R2-serp``, …), полностью сохраняющая предыдущие (§13, K2). Модуль
оркестрирует один/несколько раундов:

```
run_serp_round:
  external_search (T11.2, I16/I19/I23)  → кандидаты SearchResult (НЕ evidence, I19)
  derive_round_build_id                 → новый index_build_id (≠ parent, §13)
  builder(ctx)                          → новая IndexBuild с явным новым id
  save_index_manifest (append-only)     → персистенция продукт-партиций (K1/I17)

run_serp_loop:
  R1..Rn, каждый раунд = новая сборка; после долива —
  invalidate_affected(...) → затронутые ячейки помечаются stale_reason='new_build' (K2)
```

**Иммутабельность (§13/K2).** Новый раунд обязан получить **новый** ``index_build_id``;
переиспользование ``parent_build_id`` запрещено (:class:`ImmutableBuildError`). Прошлые
сборки не мутируются: :func:`invalidate_affected` меняет только ``stale_reason`` ячейки,
оставляя её привязанной к своей (старой) сборке с явной пометкой устаревания — как и
предписывает §13 («либо новый ``index_build_id``, либо ячейка остаётся привязанной к своей
непереоценённой сборке»). Ячейка переоценивается на новой сборке отдельным (повторным Ш6).

**Гэпы хранилища (не правим store — фиксируем явно).** ``StorePort`` (``core/ports/store.py``)
предоставляет ``save_index_manifest`` (upsert по PK ``(index_build_id, product_id)``),
``iter_cells(run_id)`` и ``save_cells_batch``. Отсутствуют:

* ``save_index_build``/``read_index_build``/``iter_index_manifest`` — отдельной таблицы
  раундов/сборок нет; append-only обеспечивается уникальностью ``index_build_id``, а не
  методом store. Чтение раунда из store недоступно на уровне порта;
* ``iter_cells_by_build`` — затронутые ячейки отбираются полным ``iter_cells(run_id)`` с
  фильтром по провенансу ``Cell.index_build_id`` (R15);
* чтения ``cell_evidence`` нет — evidence-провенанс не используется, только ``Cell``.

**Формат-авторитет ``adapters/index_format.py``.** ``is_committed``/``ImmutableBuildError``
живут в адаптерах (``index_npy``), а ``pipeline`` по матрице зависимостей (01 §1) **не
импортирует** ``core.adapters.*``. Поэтому логическая иммутабельность (новый id ≠ прошлый)
обеспечивается здесь, а физическая — инжектируемым ``builder``; поднятый им
``ImmutableBuildError`` (по имени класса) транслируется в :class:`ImmutableBuildError`.
Если требуемого метода store нет — поднимается :class:`SerpLoopStoreGap`.

Модуль категорийно нейтрален (§1.3), детерминирован (без random/UUID), часы инжектируются.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from core.domain import (
    Cell,
    IndexManifestEntry,
    StaleReason,
    TerminationReason,
)
from core.pipeline.corpus import IndexBuild
from core.pipeline.idempotency import mark_stale, upsert_cells
from core.pipeline.search_log import SearchBudget, SearchEffort, external_search
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult
from core.ports.store import StorePort
from core.util.hashing import short_hash

__all__ = [
    "ImmutableBuildError",
    "SerpLoopError",
    "SerpLoopStoreGap",
    "SerpQuery",
    "RoundBuildContext",
    "SerpRound",
    "SerpLoopResult",
    "RoundBuilder",
    "default_round_builder",
    "derive_round_build_id",
    "derive_round_partition_id",
    "assert_build_id_new",
    "run_serp_round",
    "invalidate_affected",
    "run_serp_loop",
]

#: Разделитель полей material-строки ``index_build_id`` (unit separator — однозначная склейка).
_FIELD_SEP = "\x1f"
#: Разделитель групп внутри одной группы полей (кандидат ↔ ранг ↔ url).
_GROUP_SEP = "\x1e"


# --------------------------------------------------------------------------- #
# Ошибки
# --------------------------------------------------------------------------- #


class SerpLoopError(RuntimeError):
    """Базовая ошибка SERP-петли."""


class ImmutableBuildError(SerpLoopError):
    """Попытка переиспользовать/изменить уже зафиксированную сборку (K2/§13).

    Локальное зеркало семантики ``core.adapters.index_npy.ImmutableBuildError``: сам класс
    адаптерный и не импортируется из ``pipeline`` (матрица зависимостей 01 §1), поэтому
    :func:`run_serp_round` транслирует поднятый ``builder``-ом ``ImmutableBuildError``
    (по имени класса) в этот тип.
    """


class SerpLoopStoreGap(SerpLoopError):
    """Хранилище не предоставляет нужный метод (гэп ``StorePort``, см. docstring модуля)."""


# --------------------------------------------------------------------------- #
# Значения
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SerpQuery:
    """Поисковый запрос, привязанный к продукту (ключ бюджета — ``(phase, product_id)``, I16)."""

    product_id: str
    query: SearchQuery


@dataclass(frozen=True)
class RoundBuildContext:
    """Вход построения сборки раунда — seam композиции (W8): как собрать ``IndexBuild``.

    ``builder`` обязан вернуть ``IndexBuild`` с ``index_build_id == new_build_id`` (или,
    если id иной, он будет детерминированно перенормирован к ``new_build_id`` —
    «явный новый id» раунда главнее). Физическая запись партиций/чанков — ответственность
    ``builder`` (``IndexBuilderPort``), ``serp_loop`` пишет только ``index_manifest``.
    """

    new_build_id: str
    round_no: int
    package: str
    snapshot_date: str | None
    candidates_by_product: Mapping[str, list[SearchResult]]
    #: Сборка предыдущего раунда (``None`` для R1) — вход деривации per-product
    #: ``partition_build_id``. Не влияет на ``new_build_id`` (он уже выведен).
    parent_build_id: str | None = None


#: Seam построения сборки раунда (обёртка ``build_corpus`` или прямая ``IndexBuild``).
RoundBuilder = Callable[[RoundBuildContext], IndexBuild]


@dataclass
class SerpRound:
    """Один SERP-раунд: новая сборка + кандидаты + израсходованное усилие (K2).

    :param round_no: номер раунда (``R1`` = 1, …).
    :param build: **новая** ``IndexBuild`` раунда (иммутабельная сборка, §13).
    :param candidates: кандидаты ``SearchResult`` (только кандидаты, не evidence — I19).
    :param search_calls: фактически выполненные вызовы ``adapter.search`` (без отказов бюджета).
    :param termination_reason: терминация раунда (§5) или ``None`` — протокол продолжается.
    """

    round_no: int
    build: IndexBuild
    candidates: list[SearchResult] = field(default_factory=list)
    search_calls: int = 0
    termination_reason: TerminationReason | None = None


@dataclass
class SerpLoopResult:
    """Результат петли SERP — упорядоченный список раундов ``R1..Rn`` (append-only).

    Свойство :attr:`final_build` — сборка последнего раунда (или ``None`` для пустой петли);
    :meth:`to_manifest_json` — детерминированная JSON-сводка раундов для манифеста прогона.
    """

    rounds: list[SerpRound] = field(default_factory=list)

    @property
    def final_build(self) -> IndexBuild | None:
        """Сборка последнего раунда (``None``, если раундов нет)."""
        return self.rounds[-1].build if self.rounds else None

    def to_manifest_json(self) -> str:
        """Детерминированная JSON-сводка раундов (round_no, id, счётчики, кандидаты)."""
        payload = {
            "rounds": [
                {
                    "round_no": rnd.round_no,
                    "index_build_id": rnd.build.index_build_id,
                    "chunk_count": rnd.build.chunk_count,
                    "entry_count": len(rnd.build.entries),
                    "search_calls": rnd.search_calls,
                    "termination_reason": (
                        rnd.termination_reason.value
                        if rnd.termination_reason is not None
                        else None
                    ),
                    "candidates": [
                        {"url": c.url, "title": c.title, "rank": c.rank}
                        for c in rnd.candidates
                    ],
                }
                for rnd in self.rounds
            ],
            "final_build_id": (
                self.final_build.index_build_id if self.final_build is not None else None
            ),
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)


# --------------------------------------------------------------------------- #
# Вспомогательное
# --------------------------------------------------------------------------- #


def _as_phase(phase: Any) -> Any:
    """Фазовое значение без строгой валидации (для ``SearchEffort``; чужие строки допустимы)."""
    return getattr(phase, "value", phase)


def _require(store: Any, *names: str) -> None:
    """Проверяет наличие методов store; иначе — понятная :class:`SerpLoopStoreGap`."""
    missing = [name for name in names if not callable(getattr(store, name, None))]
    if missing:
        raise SerpLoopStoreGap(
            "StorePort не предоставляет методов: "
            + ", ".join(missing)
            + " — см. гэпы хранилища в docstring модуля (методы не добавляем)"
        )


def _candidates_token(candidates_by_product: Mapping[str, Sequence[SearchResult]]) -> str:
    """Детерминированный отпечаток кандидатов раунда (продукт ↔ ранг ↔ url)."""
    parts: list[str] = []
    for product_id in sorted(candidates_by_product):
        for result in candidates_by_product[product_id]:
            parts.append(
                _GROUP_SEP.join((str(product_id), str(result.rank), str(result.url)))
            )
    return _FIELD_SEP.join(parts)


def derive_round_build_id(
    parent_build_id: str | None,
    round_no: int,
    package: str,
    candidates_token: str,
) -> str:
    """Детерминированный ``composition_id`` раунда (``cb_``, без random/UUID, §13).

    Это ``runs.index_build_id`` раунда (09 §8.1): композиция per-product партиций,
    а не id одной физической сборки. Входы (склейка ``\\x1f``, порядок фиксирован):
    ``parent_build_id`` (пусто для R1), ``round_no``, ``package``, отпечаток кандидатов.
    Поскольку ``parent_build_id`` — композиция предыдущего раунда, идентификаторы
    образуют различающуюся цепочку ``R1 → R2 → …``: повторный раунд не переиспользует
    прошлую сборку (K2).
    """
    material = _FIELD_SEP.join(
        (
            parent_build_id or "",
            str(int(round_no)),
            str(package),
            candidates_token,
        )
    )
    return "cb_" + short_hash(material, 24)


def derive_round_partition_id(
    parent_build_id: str | None,
    round_no: int,
    package: str,
    candidates_token: str,
    product_id: str,
) -> str:
    """Детерминированный ``partition_build_id`` продукта в раунде (``pb_``, 09 §8.1).

    Раунд порождает **новые** партиции (append-only, K2): вход включает композицию
    родителя и номер раунда, поэтому повтор раунда не даёт тот же ``pb``. Продукт
    входит в material, поэтому партиции продуктов различимы даже при одинаковом
    наборе кандидатов.
    """
    material = _FIELD_SEP.join(
        (
            parent_build_id or "",
            str(int(round_no)),
            str(package),
            candidates_token,
            str(product_id),
        )
    )
    return "pb_" + short_hash(material, 24)


def assert_build_id_new(
    parent_build_id: str | None,
    new_build_id: str,
    *,
    existing_build_ids: Collection[str] | None = None,
) -> None:
    """Запрещает переиспользование/пустой ``index_build_id`` (K2/§13).

    :raises ImmutableBuildError: если ``new_build_id`` пуст, совпадает с ``parent_build_id``
        или уже присутствует в ``existing_build_ids``.
    """
    if not new_build_id:
        raise ImmutableBuildError("index_build_id раунда обязан быть непустым (K2/§13)")
    if parent_build_id is not None and new_build_id == parent_build_id:
        raise ImmutableBuildError(
            f"раунд переиспользует сборку {parent_build_id!r}: нужен новый index_build_id (K2/§13)"
        )
    if existing_build_ids is not None and new_build_id in set(existing_build_ids):
        raise ImmutableBuildError(
            f"сборка {new_build_id!r} уже зафиксирована: повторное использование запрещено (K2/§13)"
        )


def default_round_builder(ctx: RoundBuildContext) -> IndexBuild:
    """Сборка раунда по умолчанию: продукт-партиции из кандидатов (K1/I17).

    Создаёт по одной ``IndexManifestEntry`` на продукт с кандидатами, привязывая её к
    **явному** ``ctx.new_build_id``. Физический индекс/чанки не пишутся: их подключает
    ``builder`` композиции (обёртка ``build_corpus``/``IndexBuilderPort``).
    """
    entries: list[IndexManifestEntry] = []
    candidates_token = _candidates_token(ctx.candidates_by_product)
    for product_id in sorted(ctx.candidates_by_product):
        results = ctx.candidates_by_product[product_id]
        if not results:
            continue
        partition_build_id = derive_round_partition_id(
            ctx.parent_build_id,
            ctx.round_no,
            ctx.package,
            candidates_token,
            product_id,
        )
        entries.append(
            IndexManifestEntry(
                index_build_id=ctx.new_build_id,
                partition_build_id=partition_build_id,
                product_id=product_id,
                package_id=ctx.package,
                partition_key=f"{ctx.new_build_id}:{product_id}",
                snapshot_date=ctx.snapshot_date,
                store_path=(
                    f"index_store/{ctx.package}/{partition_build_id}/{product_id}/"
                ),
                created_at=ctx.snapshot_date,
                stats_json=json.dumps(
                    {"candidates": len(results)}, ensure_ascii=False, sort_keys=True
                ),
            )
        )
    return IndexBuild(
        index_build_id=ctx.new_build_id,
        entries=entries,
        docs=[],
        chunk_count=0,
    )


def _normalize_build(build: IndexBuild, new_build_id: str, package: str) -> IndexBuild:
    """Перенормирует сборку к явному ``new_build_id`` раунда (входную не мутирует).

    ``builder`` может вернуть сборку со своим производным id (обёртка ``build_corpus``);
    раунду важен **новый** id. Копируем ``IndexManifestEntry`` (``model_copy``) с новым
    ``index_build_id``/``partition_key``/``store_path``; физические артефакты — зона
    ``builder``. Если id уже совпадает — возвращаем сборку без изменений.
    """
    if build.index_build_id == new_build_id:
        return build
    entries = [
        entry.model_copy(
            update={
                "index_build_id": new_build_id,
                "partition_key": f"{new_build_id}:{entry.product_id}",
                # Физическая партиция адресуется ``pb``, а не композицией. ``builder``
                # мог вернуть per-product ``pb`` (``build_corpus``/default); если нет —
                # legacy-строка падает обратно на собственный id (не выдумываем новый).
                "partition_build_id": entry.partition_build_id
                or entry.index_build_id,
                "store_path": (
                    f"index_store/{package}/"
                    f"{entry.partition_build_id or entry.index_build_id}/"
                    f"{entry.product_id}/"
                ),
            }
        )
        for entry in build.entries
    ]
    return IndexBuild(
        index_build_id=new_build_id,
        entries=entries,
        docs=list(build.docs),
        chunk_count=build.chunk_count,
    )


def _call_builder(builder: RoundBuilder, ctx: RoundBuildContext) -> IndexBuild:
    """Вызывает ``builder``, транслируя его ``ImmutableBuildError`` в локальный тип (K2)."""
    try:
        return builder(ctx)
    except Exception as exc:  # noqa: BLE001 — диагностика важнее типа сбоя
        if type(exc).__name__ == "ImmutableBuildError":
            raise ImmutableBuildError(str(exc)) from exc
        raise


# --------------------------------------------------------------------------- #
# Один раунд
# --------------------------------------------------------------------------- #


def run_serp_round(
    *,
    adapter: SearchAdapterPort,
    queries_by_product: Mapping[str, Sequence[SearchQuery]],
    store: StorePort,
    run_id: str,
    effort: SearchEffort,
    budget: SearchBudget | None = None,
    round_no: int = 1,
    parent_build_id: str | None = None,
    package: str = "package",
    snapshot_date: str | None = None,
    builder: RoundBuilder | None = None,
    make_build_id: Callable[[str | None, int, str], str] | None = None,
    existing_build_ids: Collection[str] | None = None,
    clock: Callable[[], str] | None = None,
) -> SerpRound:
    """Выполняет **один** SERP-раунд и фиксирует новую сборку (K2, I16/I19/I23).

    Порядок: ``external_search`` по каждому ``(product_id, query)`` (T11.2; возвращает
    только кандидатов — I19) → детерминированный новый ``index_build_id`` →
    ``builder`` строит ``IndexBuild`` → ``save_index_manifest`` (append-only, K1/I17).

    :param queries_by_product: отображение ``product_id → [SearchQuery]``; продукты
        обходятся в алфавитном порядке (детерминизм). Пусто → пустой раунд.
    :param parent_build_id: сборка предыдущего раунда; новый id обязан от неё отличаться.
    :param builder: seam построения сборки; ``None`` → :func:`default_round_builder`.
    :param make_build_id: seam вывода id (для тестов/детерминизма); по умолчанию
        :func:`derive_round_build_id`.
    :param existing_build_ids: уже зафиксированные id (защита от переиспользования).

    :raises ImmutableBuildError: при попытке переиспользовать/изменить сборку (K2/§13).
    :raises SerpLoopStoreGap: если store не предоставляет ``iter_cells``-совместимых методов.
    :raises ValueError: при ``round_no < 1``, неверном ``queries_by_product`` или если
        ``builder`` вернул сборку, которую нельзя перенормировать к новому id.
    """
    if round_no < 1:
        raise ValueError(f"round_no обязан быть ≥ 1, получено {round_no}")
    if not isinstance(queries_by_product, Mapping):
        raise ValueError("queries_by_product обязан быть отображением product_id → [SearchQuery]")
    _require(store, "save_index_manifest")

    effective_budget = budget if budget is not None else effort.budget
    build_fn: RoundBuilder = builder or default_round_builder
    id_fn = make_build_id or (
        lambda parent, no, token: derive_round_build_id(parent, no, package, token)
    )

    candidates_by_product: dict[str, list[SearchResult]] = {}
    all_candidates: list[SearchResult] = []
    search_calls = 0
    budget_blocked = False

    for product_id in sorted(queries_by_product):
        key = str(product_id)
        candidates_by_product.setdefault(key, [])
        for query in queries_by_product[product_id]:
            phase = _as_phase(query.phase)
            if effort.remaining(phase, key) == 0:
                budget_blocked = True
            before = effort.used(phase, key)
            results = external_search(
                adapter,
                query,
                store=store,
                run_id=run_id,
                effort=effort,
                budget=effective_budget,
                product_id=key,
                clock=clock,
            )
            # ``search_calls`` — фактически выполненные вызовы адаптера (без отказов бюджета).
            search_calls += max(effort.used(phase, key) - before, 0)
            candidates_by_product[key].extend(results)
            all_candidates.extend(results)

    if all_candidates:
        termination: TerminationReason | None = None
    elif not queries_by_product or not any(queries_by_product.values()):
        termination = None
    elif budget_blocked:
        termination = TerminationReason.BUDGET_EXHAUSTED
    else:
        termination = TerminationReason.NOT_FOUND

    candidates_token = _candidates_token(candidates_by_product)
    new_build_id = id_fn(parent_build_id, round_no, candidates_token)
    assert_build_id_new(
        parent_build_id, new_build_id, existing_build_ids=existing_build_ids
    )

    ctx = RoundBuildContext(
        new_build_id=new_build_id,
        round_no=round_no,
        package=package,
        snapshot_date=snapshot_date,
        candidates_by_product=candidates_by_product,
        parent_build_id=parent_build_id,
    )
    build = _normalize_build(_call_builder(build_fn, ctx), new_build_id, package)
    if build.index_build_id != new_build_id:
        raise ValueError(
            f"builder вернул сборку {build.index_build_id!r}, ожидалось {new_build_id!r}"
        )

    # Append-only фиксация: тот же id не перезаписывается (см. assert_build_id_new).
    for entry in build.entries:
        store.save_index_manifest(entry)

    return SerpRound(
        round_no=round_no,
        build=build,
        candidates=all_candidates,
        search_calls=search_calls,
        termination_reason=termination,
    )


# --------------------------------------------------------------------------- #
# Инвалидация затронутых ячеек (K2)
# --------------------------------------------------------------------------- #


def invalidate_affected(
    store: StorePort,
    run_id: str,
    old_build_id: str,
    new_build_id: str,
    *,
    product_ids: Collection[str] | None = None,
    param_ids: Collection[str] | None = None,
) -> list[Cell]:
    """Помечает затронутые новой сборкой ячейки ``stale_reason='new_build'`` (K2/R15).

    Затронутые — ячейки прогона, чей провенанс ``Cell.index_build_id == old_build_id``
    (R15), опционально суженные ``product_ids``/``param_ids``. Каждая помечается через
    :func:`mark_stale` (копия, **не** мутация) и идемпотентно upsert-ится. ``index_build_id``
    ячейки **не** меняется: она остаётся привязанной к старой сборке с явной пометкой (§13);
    переоценка на новой сборке — отдельный повторный Ш6.

    Детерминированный порядок результата — ``(product_id, param_id)``. Уже помеченные
    ``new_build`` ячейки пропускаются (идемпотентность повторного вызова). Возвращаются
    инвалидированные ячейки.

    :raises ValueError: если ``old_build_id == new_build_id`` или id пуст.
    :raises SerpLoopStoreGap: если store не предоставляет ``iter_cells``/``save_cells_batch``.
    """
    if not old_build_id or not new_build_id:
        raise ValueError("old_build_id и new_build_id обязаны быть непустыми")
    if old_build_id == new_build_id:
        raise ValueError("old_build_id и new_build_id обязаны различаться (K2)")
    _require(store, "iter_cells", "save_cells_batch")

    products = set(product_ids) if product_ids is not None else None
    params = set(param_ids) if param_ids is not None else None

    affected: list[Cell] = []
    for cell in store.iter_cells(run_id):
        if cell.index_build_id != old_build_id:
            continue
        if products is not None and cell.product_id not in products:
            continue
        if params is not None and cell.param_id not in params:
            continue
        if cell.stale_reason is StaleReason.NEW_BUILD:
            continue  # уже помечена — повторный вызов идемпотентен
        affected.append(cell)

    affected.sort(key=lambda c: (c.product_id, c.param_id))
    invalidated = [mark_stale(cell, StaleReason.NEW_BUILD) for cell in affected]
    if invalidated:
        upsert_cells(store, invalidated)
    return invalidated


# --------------------------------------------------------------------------- #
# Петля раундов
# --------------------------------------------------------------------------- #


def run_serp_loop(
    *,
    adapter: SearchAdapterPort,
    queries_by_product: Mapping[str, Sequence[SearchQuery]],
    store: StorePort,
    run_id: str,
    effort: SearchEffort,
    rounds: int,
    budget: SearchBudget | None = None,
    start_round: int = 1,
    parent_build_id: str | None = None,
    package: str = "package",
    snapshot_date: str | None = None,
    builder: RoundBuilder | None = None,
    make_build_id: Callable[[str | None, int, str], str] | None = None,
    invalidate: bool = True,
    product_ids: Collection[str] | None = None,
    param_ids: Collection[str] | None = None,
    existing_build_ids: Collection[str] | None = None,
    clock: Callable[[], str] | None = None,
) -> SerpLoopResult:
    """Ограниченная петля SERP: ``R_start..R_{start+rounds-1}`` (append-only, K2).

    Каждый раунд порождает **новую** сборку (id цепочкой от предыдущей) и, если есть
    родительская сборка, инвалидирует затронутые ячейки (``stale_reason='new_build'``).
    Бюджет/терминация — через T11.2: раунд с ``termination_reason=BUDGET_EXHAUSTED``
    останавливает петлю (дальнейшие вызовы всё равно были бы отвергнуты бюджетом).

    :param rounds: число раундов (≥ 0); ``0`` → пустой результат.
    :param start_round: номер первого раунда (≥ 1); по умолчанию ``R1``.
    :param parent_build_id: сборка, от которой стартует цепочка (например ``R0-base``).
    :param existing_build_ids: уже зафиксированные id (защита append-only).
    :param invalidate: инвалидировать затронутые ячейки после долива раунда.
    """
    if rounds < 0:
        raise ValueError(f"rounds обязан быть ≥ 0, получено {rounds}")
    if start_round < 1:
        raise ValueError(f"start_round обязан быть ≥ 1, получено {start_round}")

    result = SerpLoopResult()
    current_parent = parent_build_id
    seen: set[str] = set(existing_build_ids or ())
    if current_parent is not None:
        seen.add(current_parent)

    for offset in range(rounds):
        round_no = start_round + offset
        rnd = run_serp_round(
            adapter=adapter,
            queries_by_product=queries_by_product,
            store=store,
            run_id=run_id,
            effort=effort,
            budget=budget,
            round_no=round_no,
            parent_build_id=current_parent,
            package=package,
            snapshot_date=snapshot_date,
            builder=builder,
            make_build_id=make_build_id,
            existing_build_ids=seen,
            clock=clock,
        )
        result.rounds.append(rnd)
        new_build_id = rnd.build.index_build_id
        if invalidate and current_parent is not None:
            invalidate_affected(
                store,
                run_id,
                current_parent,
                new_build_id,
                product_ids=product_ids,
                param_ids=param_ids,
            )
        seen.add(new_build_id)
        current_parent = new_build_id
        if rnd.termination_reason is TerminationReason.BUDGET_EXHAUSTED:
            break

    return result
