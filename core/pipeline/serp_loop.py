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
непереоценённой сборке»). Ячейка переоценивается на новой сборке выборочно:
селектор :func:`core.pipeline.evaluate.select_stale_cells` (и исполняющий его
``hound revalidate``) берёт **только** помеченные ячейки, а не пере-прогоняет Ш6 целиком.

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
from urllib.parse import unquote, urlsplit

from core.domain import (
    Cell,
    IndexManifestEntry,
    StaleReason,
    TerminationReason,
)
from core.domain.enums import SearchPhase
from core.pipeline.corpus import IndexBuild
from core.pipeline.crawl_frontier import (
    CrawlLimits,
    is_blacklisted,
    normalize_scope_host,
    root_path_prefix,
)
from core.pipeline.idempotency import mark_stale, upsert_cells
from core.pipeline.search_log import SearchBudget, SearchEffort, external_search
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult
from core.ports.store import StorePort
from core.util.hashing import short_hash
from core.util.security import validate_url

__all__ = [
    "ImmutableBuildError",
    "SerpLoopError",
    "SerpLoopStoreGap",
    "SerpQuery",
    "RoundBuildContext",
    "SerpRound",
    "SerpLoopResult",
    "SerpDiscovery",
    "SERP_QUERY_LIMIT",
    "RoundBuilder",
    "default_round_builder",
    "build_serp_queries",
    "declared_serp_hosts",
    "serp_host_in_scope",
    "serp_discover",
    "assign_serp_seeds",
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
#: Сколько кандидатов запрашивать у адаптера на один SERP-запрос (тот же масштаб, что
#: у ретривального выреза: внешний поиск отдаёт первые страницы, не весь выдачу).
SERP_QUERY_LIMIT = 10


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
# Производственный проход контура: запросы → поиск → допуск кандидатов (I3/§5)
# --------------------------------------------------------------------------- #


@dataclass
class SerpDiscovery:
    """Наблюдённый результат SERP-прохода (до долива в корпус).

    Это **наблюдение**, а не evidence (I19): принятые URL станут доказательствами только
    после ``corpus_raw`` → hash → index. Поля названы так, как они попадут в провенанс
    сеанса/манифеста, — второго словаря не заводим.

    :param rounds: объявленное число раундов (0 — контур выключен).
    :param executed_rounds: сколько раундов реально исполнено (ранняя терминация видна).
    :param source: уровень, которым включён контур (``cli``/``env``/``run.yaml``/…).
    :param budget_declared: объявленный бюджет **на продукт на раунд** (``None`` — выкл.).
    :param adapter: провайдер адаптера поиска (провенанс; ``None`` — адаптера не было).
    :param queries_by_product: выпущенные запросы (тексты, в порядке выпуска).
    :param accepted_by_product: принятые в корпус URL (новые, дедуплицированные).
    :param rejected_by_product: отказы по причинам (``ssrf``/``out_of_scope_host``/
        ``blacklisted``/``known``) — «сколько не пустили и почему», а не тихий фильтр.
    :param candidates_by_product: сколько кандидатов вернул адаптер (всего).
    :param calls_by_product: сколько вызовов адаптера состоялось (без отказов бюджета).
    :param exhausted_by_product: бюджет продукта исчерпан (факт, не догадка).
    :param termination_by_product: причина терминации по продукту (§5).
    :param unavailable_reason: почему контур не исполнен при объявленных раундах
        (``no_adapter``/``no_queries``) — видимый отказ, а не молчаливый пропуск.
    """

    rounds: int = 0
    executed_rounds: int = 0
    source: str = "default"
    budget_declared: int | None = None
    adapter: str | None = None
    queries_by_product: dict[str, list[str]] = field(default_factory=dict)
    accepted_by_product: dict[str, list[str]] = field(default_factory=dict)
    rejected_by_product: dict[str, dict[str, int]] = field(default_factory=dict)
    candidates_by_product: dict[str, int] = field(default_factory=dict)
    calls_by_product: dict[str, int] = field(default_factory=dict)
    exhausted_by_product: dict[str, bool] = field(default_factory=dict)
    termination_by_product: dict[str, str | None] = field(default_factory=dict)
    unavailable_reason: str | None = None

    @property
    def enabled(self) -> bool:
        """Объявлен ли контур (``rounds > 0``)."""
        return self.rounds > 0

    @property
    def accepted_total(self) -> int:
        """Сколько URL принято всего (сумма по продуктам)."""
        return sum(len(items) for items in self.accepted_by_product.values())

    @property
    def any_exhausted(self) -> bool:
        """Исчерпан ли бюджет хотя бы у одного продукта (видимая запись, не тишина)."""
        return any(self.exhausted_by_product.values())

    def to_observation(self) -> dict[str, Any]:
        """Провенанс контура для сеанса краула/манифеста (детерминированный JSON-вид)."""
        return {
            "enabled": self.enabled,
            "rounds": {
                "declared": int(self.rounds),
                "executed": int(self.executed_rounds),
                "source": self.source,
            },
            "budget": {
                "declared_per_round": self.budget_declared,
                "effective_per_product": (
                    None
                    if self.budget_declared is None
                    else int(self.budget_declared) * int(self.rounds)
                ),
                "used": {pid: int(n) for pid, n in sorted(self.calls_by_product.items())},
                "exhausted": {pid: bool(v) for pid, v in sorted(self.exhausted_by_product.items())},
            },
            "adapter": self.adapter,
            "unavailable_reason": self.unavailable_reason,
            "queries": {pid: list(qs) for pid, qs in sorted(self.queries_by_product.items())},
            "candidates": {pid: int(n) for pid, n in sorted(self.candidates_by_product.items())},
            "accepted": {pid: list(urls) for pid, urls in sorted(self.accepted_by_product.items())},
            "rejected": {
                pid: {reason: int(count) for reason, count in sorted(reasons.items())}
                for pid, reasons in sorted(self.rejected_by_product.items())
            },
            "termination": {
                pid: reason for pid, reason in sorted(self.termination_by_product.items())
            },
        }


def declared_serp_hosts(
    urls_by_product: Mapping[str, Sequence[str]],
    domain_profiles: Mapping[str, Any] | None = None,
) -> tuple[dict[str, frozenset[str]], frozenset[str]]:
    """Объявленные хосты: ``(own_hosts_by_product, declared_all)``.

    Зеркало правила scope фазы корпуса (``corpus`` §«scope — объявленные хосты»):
    объявленные хосты — все seed-корни прогона плюс ключи ``domain_profiles.yaml``;
    собственные хосты продукта — хосты его корней. Кандидат SERP допускается, если его
    хост принадлежит продукту (собственный хост либо **объявленный** поддомен корня) —
    чужой хост не «добирается» поиском (§13: поддомен вне объявления = execution_change).
    """
    profile_hosts = {
        normalize_scope_host(str(key).split("/", 1)[0])
        for key in (domain_profiles or {})
    }
    own: dict[str, frozenset[str]] = {}
    for product_id, urls in urls_by_product.items():
        own[str(product_id)] = frozenset(
            host
            for host in (
                normalize_scope_host(urlsplit(str(url)).hostname or "") for url in urls
            )
            if host
        )
    declared = frozenset(profile_hosts.union(*own.values()) - {""}) if own else frozenset(
        profile_hosts - {""}
    )
    return own, declared


def serp_host_in_scope(
    host: str, own_hosts: frozenset[str], declared: frozenset[str]
) -> bool:
    """Проходит ли хост scope продукта (собственный хост либо объявленный поддомен).

    Это **та же** семантика, что у ``Frontier.host_in_scope`` (``www.`` эквивалентен
    базовому хосту с обеих сторон), но без привязки к одному корню: продукт может иметь
    несколько seed-корней. Никакого наследования осей профиля здесь нет — только право
    продукта тратить свой бюджет.
    """
    norm = normalize_scope_host(host)
    if not norm:
        return False
    if norm in own_hosts:
        return True
    for root in own_hosts:
        if norm.endswith("." + root) and norm in declared:
            return True
    return False


def build_serp_queries(
    *,
    products: Sequence[str],
    params: Sequence[Any],
    product_names: Mapping[str, str] | None = None,
    langs: Sequence[str] = ("ru",),
    limit: int = SERP_QUERY_LIMIT,
) -> dict[str, list[SearchQuery]]:
    """Запросы контура: **каталог + имя продукта** (замер: имя продукта обязательно).

    Замер на остатке ``❓`` (63 ячейки): запросы без имени продукта попадали в целевой
    документ в 27 случаях из 63, с именем — в 45. ``render_queries`` (внутренний ретривал)
    отдаёт заголовок параметра и концепт-термины, но имени продукта не содержит, поэтому
    для **внешнего** поиска оно добавляется здесь: внешний поиск ищет по всему интернету,
    и «потоковая репликация» без бренда приносит чужие продукты.

    Порядок детерминирован: продукты — ``products`` как объявлено, параметры — порядок
    каталога, языки — объявленный порядок ``run.yaml: languages``. Дубли схлопываются по
    ``casefold`` (заголовок параметра может повторяться у разных строк).

    :param product_names: ``product_id → каноническое имя`` (из паспорта). Продукт без
        имени получает запрос из одного заголовка, и это видно в провенансе
        (``product_names`` в observation отсутствует) — значение не выдумывается.
    :param limit: ``SearchQuery.limit`` — сколько кандидатов просить у адаптера.
    """
    names = {str(pid): str(name) for pid, name in (product_names or {}).items() if name}
    language_list = [str(lang) for lang in (langs or ()) if str(lang).strip()] or ["ru"]
    result: dict[str, list[SearchQuery]] = {}
    for product_id in products:
        pid = str(product_id)
        name = names.get(pid)
        queries: list[SearchQuery] = []
        seen: set[tuple[str, str]] = set()
        for param in params:
            title = str(getattr(param, "title", "") or "").strip()
            if not title:
                continue
            text = f"{title} {name}".strip() if name else title
            # Языки параметра (`catalog.csv: search_langs`) сильнее объявленного набора
            # прогона: язык — свойство параметра, а `languages` — дефолт прогона.
            param_langs = [
                str(lang) for lang in (getattr(param, "search_langs", None) or ())
            ] or language_list
            for lang in param_langs:
                key = (text.casefold(), lang)
                if key in seen:
                    continue
                seen.add(key)
                queries.append(
                    SearchQuery(
                        text=text,
                        lang=lang,
                        limit=limit,
                        phase=SearchPhase.SERP.value,
                    )
                )
        result[pid] = queries
    return result


def _known_url_key(url: str) -> str:
    """Ключ дедупликации URL: без фрагмента, с нормализованным хостом и без ``/`` в конце."""
    parsed = urlsplit(str(url))
    host = normalize_scope_host(parsed.hostname or "")
    path = (parsed.path or "/").rstrip("/") or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{parsed.scheme.lower()}://{host}{path}{query}"


def _admit_candidate(
    result: SearchResult,
    *,
    own_hosts: frozenset[str],
    declared: frozenset[str],
    limits: CrawlLimits | None,
    seen: set[str],
) -> tuple[str | None, str | None]:
    """Допуск кандидата: ``(url|None, причина_отказа|None)``.

    Порядок проверок повторяет краул: SSRF → scope хоста (объявленные хосты) → блэклист →
    дедупликация. Path-prefix **не** применяется: найденный документ не обязан лежать под
    корнем seed'а (он не продолжает навигацию корня), но SSRF/host-scope/блэклист
    остаются обязательными (та же граница, что у терминального артефакта в ``Frontier``).
    """
    url = str(result.url or "").strip()
    if not url:
        return None, "empty"
    try:
        validate_url(url)
    except ValueError:
        return None, "ssrf"
    parsed = urlsplit(url)
    if not serp_host_in_scope(parsed.hostname or "", own_hosts, declared):
        return None, "out_of_scope_host"
    if limits is not None and is_blacklisted(url, limits):
        return None, "blacklisted"
    key = _known_url_key(url)
    if key in seen:
        return None, "known"
    seen.add(key)
    return url, None


def _host_equivalent(left: str, right: str) -> bool:
    """``www.``-эквивалентность хостов (``example.org`` ≡ ``www.example.org``)."""
    a = normalize_scope_host(left)
    b = normalize_scope_host(right)
    if not a or not b:
        return False
    return a == b or a.removeprefix("www.") == b.removeprefix("www.")


def _path_within(url_path: str, prefix: str) -> bool:
    """Путь укладывается в prefix по границе сегмента (``""`` — нет префикса)."""
    if not prefix:
        return True
    if not url_path:
        return False
    return url_path == prefix or url_path.startswith(prefix + "/")


def assign_serp_seeds(
    found_by_product: Mapping[str, Sequence[str]],
    roots_by_product: Mapping[str, Sequence[str]],
) -> dict[str, dict[str, list[str]]]:
    """Раскладывает находки SERP по **объявленным корням** продукта.

    Вопрос, на который отвечает функция: как найденный URL входит в корпус, не став
    при этом новым «авторским корнем»? Ответ: он ставится depth-0 seed'ом того корня,
    чей host и path-prefix его покрывают (при нескольких — самый длинный prefix).
    Найденный URL **не** становится отдельным корнем: иначе ``seeds_declared``,
    ``max_documents`` и «сколько корней у продукта» начали бы описывать результат
    поиска, то есть число корней зависело бы от ранжирования провайдера (A2/I16).

    Причины, по которым URL всё же может не покрываться ни одним корнем:

    * host объявлен (ключ ``domain_profiles.yaml`` или корень другого продукта), но
      собственного корня у продукта на нём нет → URL идёт на первый корень продукта;
    * URL на объявленном **поддомене** своего же корня → тот же случай.

    В обоих случаях бюджет платит указанный корень, и ``accepted_foreign_hosts``
    frontier'а фиксирует это как **объявленный чужой хост** — не тихий захват бюджета.

    :param found_by_product: принятые кандидаты (``SerpDiscovery.accepted_by_product``).
    :param roots_by_product: объявленные (авторские) корни: ``product_id → [url]``.
    :returns: ``{product_id: {root_url: [найденные url]}}`` (детерминированный порядок,
        пустые продукты/корни опущены).
    """
    assigned: dict[str, dict[str, list[str]]] = {}
    for product_id, found in found_by_product.items():
        pid = str(product_id)
        roots = [str(root) for root in (roots_by_product.get(pid) or ())]
        if not found or not roots:
            continue
        by_root: dict[str, list[str]] = {}
        for url in found:
            parsed = urlsplit(str(url))
            host = parsed.hostname or ""
            # Percent-декодирование — как в scope-предикате краула: иначе ``%2F`` обходил
            # бы границу сегмента при выборе корня.
            path = unquote(parsed.path or "")
            best_root: str | None = None
            best_prefix = -1
            for root in roots:
                root_host = urlsplit(root).hostname or ""
                prefix = root_path_prefix(root)
                if not _host_equivalent(host, root_host):
                    continue
                if not _path_within(path or "/", prefix):
                    continue
                if len(prefix) > best_prefix:
                    best_root = root
                    best_prefix = len(prefix)
            owner = best_root or roots[0]
            by_root.setdefault(owner, [])
            if str(url) not in by_root[owner]:
                by_root[owner].append(str(url))
        if by_root:
            assigned[pid] = {root: by_root[root] for root in sorted(by_root)}
    return assigned


def serp_discover(
    *,
    adapter: SearchAdapterPort | None,
    queries_by_product: Mapping[str, Sequence[SearchQuery]],
    store: Any,
    run_id: str,
    effort: SearchEffort,
    rounds: int,
    own_hosts_by_product: Mapping[str, frozenset[str]] | None = None,
    declared_hosts: frozenset[str] | None = None,
    known_urls_by_product: Mapping[str, Sequence[str]] | None = None,
    limits_by_product: Mapping[str, CrawlLimits] | None = None,
    source: str = "run.yaml",
    budget_declared: int | None = None,
    clock: Callable[[], str] | None = None,
) -> SerpDiscovery:
    """Исполняет SERP-проход: запросы → ``external_search`` → допуск кандидатов.

    **I19.** Возвращаются только URL-кандидаты: ни один не считается доказательством,
    пока не пройдёт ``corpus_raw`` → hash → index в фазе корпуса. Функция не читает
    корпус и не пишет чанков.

    **Бюджет.** Один бюджет на продукт на раунд (``SearchEffort`` с ключом
    ``(serp, product_id)`` внутри одного прогона): эффективный потолок — ``rounds ×
    budget``. Отказ бюджета виден (:class:`SerpDiscovery.exhausted_by_product`) и
    пишется в ``search_log`` причиной ``budget_exhausted`` — исчерпание не «тихое».

    **Ранняя терминация.** Раунд, не принёсший **ни одного нового** допущенного URL,
    останавливает проход: следующие раунды выпустили бы те же запросы по тому же
    провайдеру. Число исполненных раундов остаётся в наблюдении.

    **Видимый отказ вместо пропуска.** Адаптера нет или запросов нет при объявленных
    раундах — ``unavailable_reason`` заполняется, и фаза сообщает об этом; контур не
    изображает «исполнился и ничего не нашёл».

    :param known_urls_by_product: уже известные URL продукта (seed'ы + ранее найденное) —
        дедупликация, чтобы раунд не «нашёл» собственный корпус.
    :param limits_by_product: разрешённые оси краула продукта (источник блэклиста).
    """
    if rounds < 0:
        raise ValueError(f"rounds обязан быть ≥ 0, получено {rounds}")
    discovery = SerpDiscovery(
        rounds=int(rounds),
        source=str(source),
        budget_declared=None if budget_declared is None else int(budget_declared),
        adapter=getattr(adapter, "provider", None) if adapter is not None else None,
    )
    if rounds == 0:
        return discovery
    own_hosts_by_product = dict(own_hosts_by_product or {})
    declared = frozenset(declared_hosts or ())
    limits_by_product = dict(limits_by_product or {})
    seen_by_product: dict[str, set[str]] = {
        str(pid): {_known_url_key(url) for url in (known_urls_by_product or {}).get(pid, ())}
        for pid in queries_by_product
    }
    for pid in queries_by_product:
        discovery.accepted_by_product.setdefault(str(pid), [])

    if adapter is None:
        discovery.unavailable_reason = "no_adapter"
        discovery.termination_by_product = {
            str(pid): TerminationReason.ERROR.value for pid in queries_by_product
        }
        return discovery
    if not any(queries_by_product.values()):
        discovery.unavailable_reason = "no_queries"
        discovery.termination_by_product = {
            str(pid): TerminationReason.NOT_FOUND.value for pid in queries_by_product
        }
        return discovery

    for offset in range(int(rounds)):
        new_this_round = 0
        candidates_this_round = 0
        for product_id in sorted(queries_by_product):
            pid = str(product_id)
            queries = queries_by_product[product_id]
            own = frozenset(own_hosts_by_product.get(pid, frozenset()))
            seen = seen_by_product.setdefault(pid, set())
            for query in queries:
                before = effort.used(SearchPhase.SERP, pid)
                results = external_search(
                    adapter,
                    query,
                    store=store,
                    run_id=run_id,
                    effort=effort,
                    budget=effort.budget,
                    product_id=pid,
                    clock=clock,
                )
                used = effort.used(SearchPhase.SERP, pid) - before
                discovery.calls_by_product[pid] = (
                    discovery.calls_by_product.get(pid, 0) + max(used, 0)
                )
                if not results:
                    if effort.remaining(SearchPhase.SERP, pid) == 0:
                        discovery.exhausted_by_product[pid] = True
                    continue
                queries_log = discovery.queries_by_product.setdefault(pid, [])
                if query.text not in queries_log:
                    queries_log.append(query.text)
                discovery.candidates_by_product[pid] = (
                    discovery.candidates_by_product.get(pid, 0) + len(results)
                )
                candidates_this_round += len(results)
                rejects = discovery.rejected_by_product.setdefault(pid, {})
                for result in results:
                    accepted, reason = _admit_candidate(
                        result,
                        own_hosts=own,
                        declared=declared,
                        limits=limits_by_product.get(pid),
                        seen=seen,
                    )
                    if accepted is not None:
                        discovery.accepted_by_product[pid].append(accepted)
                        new_this_round += 1
                    elif reason is not None:
                        rejects[reason] = rejects.get(reason, 0) + 1
        discovery.executed_rounds = offset + 1
        if new_this_round == 0:
            # Дальнейшие раунды выпустили бы те же запросы: терминация видима.
            if candidates_this_round == 0:
                reason = TerminationReason.NOT_FOUND
            elif discovery.any_exhausted:
                reason = TerminationReason.BUDGET_EXHAUSTED
            else:
                reason = TerminationReason.RESULTS_EXHAUSTED
            discovery.termination_by_product = {
                pid: reason.value for pid in queries_by_product
            }
            break

    if not discovery.termination_by_product:
        discovery.termination_by_product = {
            pid: (
                TerminationReason.BUDGET_EXHAUSTED.value
                if discovery.exhausted_by_product.get(pid)
                else TerminationReason.RESULTS_EXHAUSTED.value
            )
            for pid in queries_by_product
        }
    return discovery


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

    Пометку снимает **не** эта функция: только выборочная пере-оценка
    (:func:`core.pipeline.evaluate.select_stale_cells` → ``hound revalidate``) и только
    при полученном вердикте по текущей сборке — иначе «свежесть» была бы утверждением
    без вердикта.

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
