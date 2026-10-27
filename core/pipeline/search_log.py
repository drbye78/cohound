"""Бюджет внешнего поиска и ``search_log`` — W11/T11.2 (§4.4/§5, I16/I19/I23).

Модуль даёт три вещи:

* :class:`SearchBudget` — фиксированные потолки внешних поисковых вызовов **по фазам**
  (``discovery``/``corpus``/``serp``/``scout``), разбираемые из
  ``run_config.search_effort_budget_json`` или обычного mapping (§5 «SERP-бюджет»,
  I23: бюджет фиксирован и залогирован для каждого продукта).
* :class:`SearchEffort` — счётчик израсходованного усилия **на (фаза, продукт)**.
  Ключ включает ``product_id``, поэтому исчерпание бюджета одним продуктом не
  «съедает» усилие другого: симметрия бюджета (I16).
* :func:`external_search` — ограниченный бюджетом одиночный вызов ``adapter.search``
  с записью в ``search_log``. Возвращает **только кандидатов** :class:`SearchResult`,
  никогда не evidence (I19): превращение внешнего контента в доказательство возможно
  лишь после Ш5 (``corpus_raw`` → extract → chunk → ``content_hash`` → index).
  :func:`external_search_observed` — та же работа, но с наблюдением вызова
  (:class:`SearchOutcome`: попадание в кэш, сбой адаптера, списание, **наблюдённое
  время вызова**) — им пользуется SERP-контур, чтобы отличить «не нашлось» от
  «сорвалось» (S8) и «ждали» от «быстро» (S4).
* **SERP-кэш (R7).** Единственная точка чтения/записи ``cache/serp/{query_hash}.json`` —
  :func:`external_search_observed`: ключ :func:`core.util.cache.serp_key` несёт текст
  запроса, провайдера, **язык** и ``limit``. Попадание отдаёт кандидатов без вызова
  адаптера и **без списания бюджета**; промах — вызов и запись удачного ответа.
  До этого кэш был объявлен в §11.7 и не реализован: ``serp_key`` не вызывался ни разу.

Протокол исчерпания §5 кодируется :func:`termination_for`
(``budget_exhausted``/``results_count``/``sufficient``/``error`` → ``TerminationReason``).

Модуль категорийно нейтрален (§1.3), новых зависимостей не добавляет. Часы для ``ts``
инжектируются (по умолчанию — UTC ISO), поэтому записи детерминированы.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from core.domain.enums import (
    AdapterKind,
    SearchKind,
    SearchPhase,
    TerminationReason,
)
from core.domain.models import SearchLogEntry
from core.ports.search import SearchAdapterPort, SearchQuery, SearchResult, SearchTiming
from core.util.cache import ContentAddressedCache, serp_key

__all__ = [
    "SearchBudget",
    "SearchEffort",
    "SearchOutcome",
    "external_search",
    "external_search_observed",
    "record_search",
    "termination_for",
]

#: Ключи mapping-бюджета, задающие «потолок по умолчанию» для фаз без явного лимита.
_DEFAULT_KEYS = frozenset({"default", "*", "all"})

#: Подкаталог SERP-кэша (``cache/serp/{query_hash}.json``, 03 §A.1, §11.7).
_SERP_CACHE_KIND = "serp"


def _utcnow() -> str:
    """Текущее время в UTC ISO-8601 (дефолтные часы для :func:`record_search`)."""
    return datetime.now(UTC).isoformat()


def _as_enum(enum_cls: type[Any], value: Any) -> Any:
    """Приводит значение к члену перечисления; ``None`` пропускает как есть."""
    if value is None or isinstance(value, enum_cls):
        return value
    return enum_cls(str(value))


def _phase_value(phase: SearchPhase | str) -> str:
    """Строковое значение фазы, устойчивое к «чужим» строкам (lenient-ключ бюджета)."""
    if isinstance(phase, SearchPhase):
        return phase.value
    try:
        return SearchPhase(str(phase)).value
    except ValueError:
        return str(phase)


def _require_phase(phase: SearchPhase | str) -> SearchPhase:
    """Строгая фаза для записи ``search_log`` (неизвестная строка — ошибка)."""
    if isinstance(phase, SearchPhase):
        return phase
    return SearchPhase(str(phase))


def _as_cap(value: Any, key: Any) -> int:
    """Неотрицательный целый потолок; не-число/отрицательное — ``ValueError``."""
    if isinstance(value, bool):
        raise ValueError(f"SearchBudget: потолок для {key!r} не может быть bool")
    if isinstance(value, int):
        cap = value
    else:
        try:
            cap = int(value)
        except (TypeError, ValueError):
            raise ValueError(
                f"SearchBudget: потолок для {key!r} не целое: {value!r}"
            ) from None
    if cap < 0:
        raise ValueError(f"SearchBudget: потолок для {key!r} отрицателен: {cap}")
    return cap


@dataclass(frozen=True)
class SearchBudget:
    """Потолки внешних поисковых вызовов по фазам (§5, I23).

    :param limits: карта ``{phase: cap}``; ключи — значения ``SearchPhase``.
    :param default: потолок для фаз без явного лимита (``None`` — без лимита).
    """

    limits: Mapping[str, int] = field(default_factory=dict)
    default: int | None = None

    @classmethod
    def from_config(cls, run_config: Any) -> SearchBudget:
        """Строит бюджет из ``run_config``.

        Принимает либо mapping напрямую, либо объект с атрибутом
        ``search_effort_budget_json`` (JSON-строка или mapping) — так бюджет читается
        как из ``RunManifest``, так и из любого конфига прогона. Отсутствие значения,
        ``None`` или пустая строка дают «безлимитный» бюджет.
        """
        if run_config is None:
            return cls()
        if isinstance(run_config, Mapping):
            return cls.from_mapping(run_config)
        return cls.from_mapping(getattr(run_config, "search_effort_budget_json", None))

    @classmethod
    def from_mapping(cls, raw: Any) -> SearchBudget:
        """Разбирает ``{phase: cap}`` из mapping / JSON-строки (``None`` → пусто)."""
        if raw is None:
            return cls()
        if isinstance(raw, (bytes, bytearray)):
            raw = bytes(raw).decode("utf-8")
        data: Any = raw
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return cls()
            data = json.loads(text)
        if not isinstance(data, Mapping):
            raise ValueError("SearchBudget: ожидается mapping или JSON-объект")

        limits: dict[str, int] = {}
        default: int | None = None
        for key, value in data.items():
            if value is None:
                continue
            cap = _as_cap(value, key)
            name = str(key).strip().lower()
            if name in _DEFAULT_KEYS:
                default = cap
            else:
                limits[name] = cap
        return cls(limits=limits, default=default)

    def limit_for(self, phase: SearchPhase | str) -> int | None:
        """Потолок для фазы; ``None`` — фаза без лимита (не ограничена)."""
        value = _phase_value(phase)
        if value in self.limits:
            return int(self.limits[value])
        return self.default


@dataclass(frozen=True)
class SearchEffort:
    """Счётчик внешних поисковых вызовов на ``(phase, product_id)`` (I16, I23).

    Бюджет применяется **на продукт**: собственные счётчики у каждого ``product_id``
    не пересекаются, поэтому исчерпание одним продуктом не лишает бюджета другой —
    симметрия поискового усилия (I16).

    **Frozen.** Счётчик передаётся между вызывающим и :func:`external_search`, и раньше
    функция присваивала ему ``budget`` на месте (``effort.budget = budget``) — то есть
    меняла чужой объект ради удобства проверки (S9a). Теперь бюджет счётчика неизменяем:
    вариант «другой бюджет, тот же журнал» даёт :meth:`with_budget`, а журнал
    (:attr:`_counts`) намеренно **общий** с исходным счётчиком, поэтому списания видны
    вызывающему.

    :param budget: потолки по фазам; пустой бюджет — без ограничений.
    """

    budget: SearchBudget = field(default_factory=SearchBudget)
    _counts: dict[tuple[str, str], int] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )

    def with_budget(self, budget: SearchBudget) -> SearchEffort:
        """Вид счётчика с другим бюджетом: тот же журнал расходов, другой потолок.

        Возвращается **новый** объект: бюджет исходного не меняется (S9a), но списания
        попадают в общий ``_counts``, поэтому ``used``/``remaining`` исходного счётчика
        остаются правдой.
        """
        view = SearchEffort(budget=budget)
        object.__setattr__(view, "_counts", self._counts)
        return view

    @property
    def counts(self) -> dict[tuple[str, str], int]:
        """Копия израсходованных значений ``{(phase, product_id): n}`` (детерминизм)."""
        return dict(self._counts)

    def used(self, phase: SearchPhase | str, product_id: str) -> int:
        """Сколько внешних вызовов израсходовано для ``(phase, product_id)``."""
        return self._counts.get((_phase_value(phase), str(product_id)), 0)

    def remaining(self, phase: SearchPhase | str, product_id: str) -> int | None:
        """Остаток бюджета; ``None`` — фаза не ограничена (безлимит)."""
        limit = self.budget.limit_for(phase)
        if limit is None:
            return None
        return max(limit - self.used(phase, product_id), 0)

    def charge(self, phase: SearchPhase | str, product_id: str, n: int = 1) -> bool:
        """Списывает ``n`` вызовов, если бюджет позволяет.

        :return: ``True`` — в пределах бюджета (счётчик увеличен);
            ``False`` — исчерпан (счётчик **не** меняется, вызов не состоялся).
        """
        if isinstance(n, bool) or not isinstance(n, int):
            raise ValueError(f"SearchEffort: n должно быть целым, получено {n!r}")
        if n < 0:
            raise ValueError(f"SearchEffort: n не может быть отрицательным: {n}")
        key = (_phase_value(phase), str(product_id))
        current = self._counts.get(key, 0)
        limit = self.budget.limit_for(phase)
        if limit is not None and current + n > limit:
            return False
        self._counts[key] = current + n
        return True


def record_search(
    store: Any,
    *,
    run_id: str,
    phase: SearchPhase | str,
    kind: SearchKind | str = SearchKind.EXTERNAL,
    provider: str | None = None,
    adapter_kind: AdapterKind | str | None = None,
    language: str | None = None,
    query: str | None = None,
    results_count: int | None = None,
    selected: int | None = None,
    termination_reason: TerminationReason | str | None = None,
    clock: Callable[[], str] | None = None,
    **scores: Any,
) -> SearchLogEntry:
    """Строит :class:`SearchLogEntry` и (при наличии ``store``) пишет ``search_log``.

    ``kind`` по умолчанию — ``SearchKind.EXTERNAL`` (внешний поиск; внутренний
    ретривал логируется с ``kind=SearchKind.RETRIEVAL``). Именованные ``**scores``
    (``lexical_score``/``dense_score``/``fused_rank``/``rerank_score``/
    ``retrieval_score``) переносятся в запись как есть. ``ts`` заполняется текущим
    UTC ISO; часы можно инжектировать через ``clock`` для детерминизма.

    ``search_log`` — единственное место логирования любых поисковых вызовов (§4.4, I23),
    включая tool-based.
    """
    entry = SearchLogEntry(
        run_id=run_id,
        phase=_require_phase(phase),
        kind=_as_enum(SearchKind, kind),
        provider=provider,
        adapter_kind=_as_enum(AdapterKind, adapter_kind),
        language=language,
        query=query,
        results_count=results_count,
        selected=selected,
        termination_reason=_as_enum(TerminationReason, termination_reason),
        ts=(clock or _utcnow)(),
        **scores,
    )
    if store is not None:
        store.log_search(entry)
    return entry


def termination_for(
    budget_exhausted: bool,
    results_count: int | None,
    sufficient: bool,
    error: BaseException | None,
) -> TerminationReason | None:
    """Отображение сигналов исчерпания в причину терминации (§5).

    Приоритет (сверху вниз):

    1. ``error`` задан → ``ERROR`` (сбой поискового вызова);
    2. ``sufficient`` → ``SUFFICIENT_EVIDENCE`` (доказательств достаточно);
    3. ``budget_exhausted`` → ``BUDGET_EXHAUSTED`` (потолок усилия исчерпан);
    4. ``results_count is None`` → ``None`` (не терминально, протокол продолжается);
    5. ``results_count == 0`` → ``NOT_FOUND`` (ничего не найдено);
    6. иначе → ``RESULTS_EXHAUSTED`` (доступные результаты исчерпаны).
    """
    if error is not None:
        return TerminationReason.ERROR
    if sufficient:
        return TerminationReason.SUFFICIENT_EVIDENCE
    if budget_exhausted:
        return TerminationReason.BUDGET_EXHAUSTED
    if results_count is None:
        return None
    if results_count <= 0:
        return TerminationReason.NOT_FOUND
    return TerminationReason.RESULTS_EXHAUSTED


@dataclass(frozen=True)
class SearchOutcome:
    """Наблюдение **одного** вызова внешнего поиска: результаты и факты о нём.

    Нужен вызывающему (``serp_discover``), чтобы отличить «ничего не нашлось» от
    «вызов сорвался» (S8): ``[]`` в обоих случаях одинаков, а ``search_log`` вызывающий
    не читает. Поля — наблюдение, а не решение: ``termination`` уже выведена по §5.

    :param results: кандидаты ответа (I19: не evidence), в порядке провайдера.
    :param termination: причина терминации этого вызова (``None`` — протокол продолжается).
    :param error: сбой адаптера (``None`` — вызов состоялся).
    :param from_cache: ответ взят из R7-кэша, живого вызова не было.
    :param charged: с бюджета списан вызов (при попадании в кэш — не списывается).
        ``True`` означает, что **живой вызов состоялся**, в том числе неуспешный: этим
        признаком ``serp_discover`` считает вызовы, не заглядывая в ключ бюджета.
    :param timing: наблюдённое время вызова — ожидание внешней системы отдельно от самих
        запросов (S4). Поле есть всегда: «ноль ожидания» — это факт, а не отсутствие
        факта; «не наблюдали» помечается ``timing.measured is False``.
    """

    results: tuple[SearchResult, ...] = ()
    termination: TerminationReason | None = None
    error: BaseException | None = None
    from_cache: bool = False
    charged: bool = False
    timing: SearchTiming = field(default_factory=SearchTiming)

    @property
    def found(self) -> bool:
        """Вернул ли вызов хотя бы одного кандидата."""
        return bool(self.results)


def _observed_timing(adapter: SearchAdapterPort) -> SearchTiming:
    """Тайминги вызова, которые адаптер наблюдает (S4); иначе — «не наблюдали».

    Читается **после** вызова: адаптер публикует время последнего ``search``
    (:attr:`last_timing`), а pipeline не знает ни провайдера, ни того, где тот спал.
    Адаптер без такого наблюдения (шов DMZ, ``remote_search``) даёт ``measured=False``:
    его нули означают «не наблюдали», а не «ждали ноль секунд».
    """
    observed = getattr(adapter, "last_timing", None)
    if isinstance(observed, SearchTiming):
        return observed
    return SearchTiming()


def _serp_cache_key(
    cache: ContentAddressedCache | None,
    adapter: SearchAdapterPort,
    query: SearchQuery,
) -> str | None:
    """Ключ SERP-кэша для вызова либо ``None`` (кэша нет / провайдер не объявлен).

    Кэшируем только при **объявленном** провайдере: провайдер — ось ключа, и без него
    две разные поисковые системы писали бы в одну запись (``serp_key`` различает их
    только по строке). Пустой после нормализации запрос не кэшируется — кэшировать нечего.
    """
    if cache is None:
        return None
    provider = getattr(adapter, "provider", None)
    if not isinstance(provider, str) or not provider.strip():
        return None
    try:
        return serp_key(
            query.text, provider, lang=query.lang, limit=int(query.limit)
        )
    except (TypeError, ValueError):
        return None


def _serp_cache_read(
    cache: ContentAddressedCache | None, key: str
) -> list[SearchResult] | None:
    """Кандидаты из SERP-кэша либо ``None`` при промахе/порче записи.

    Порча кэша не роняет прогон: битый или чужой по форме JSON — промах (тот же принцип,
    что у :meth:`ContentAddressedCache.get_meta`). Признак «сырые байты страницы» в записи
    невозможен: такие ответы не кэшируются (см. :func:`_serp_cache_write`).
    """
    if cache is None:
        return None
    try:
        payload = cache.get_json(_SERP_CACHE_KIND, key)
    except (OSError, ValueError):
        return None
    if not isinstance(payload, Mapping):
        return None
    raw_items = payload.get("results")
    if not isinstance(raw_items, list):
        return None
    results: list[SearchResult] = []
    for item in raw_items:
        if not isinstance(item, Mapping):
            return None
        url = item.get("url")
        if not isinstance(url, str) or not url:
            return None
        results.append(
            SearchResult(
                url=url,
                title=str(item.get("title") or ""),
                snippet=str(item.get("snippet") or ""),
                rank=int(item.get("rank") or 0),
            )
        )
    return results


def _serp_cache_write(
    cache: ContentAddressedCache | None,
    key: str,
    adapter: SearchAdapterPort,
    query: SearchQuery,
    results: Sequence[SearchResult],
) -> None:
    """Записывает удачный ответ в SERP-кэш (R7); ответы с байтами — не кэшируются.

    ``SearchResult.raw_bytes`` в JSON не сериализуемы и I19-специфичны для retention;
    если адаптер их вернул, запись пропускается (иначе попадание отдало бы ``None``
    вместо байтов — подмена, а не кэш).
    """
    if cache is None:
        return
    if any(getattr(result, "raw_bytes", None) is not None for result in results):
        return
    payload = {
        "query": str(query.text),
        "lang": str(query.lang),
        "limit": int(query.limit),
        "provider": str(getattr(adapter, "provider", "") or ""),
        "results": [
            {
                "url": str(result.url),
                "title": str(getattr(result, "title", "") or ""),
                "snippet": str(getattr(result, "snippet", "") or ""),
                "rank": int(getattr(result, "rank", 0) or 0),
            }
            for result in results
        ],
    }
    try:
        cache.put_json(_SERP_CACHE_KIND, key, payload)
    except OSError:  # запись кэша — удобство R7, а не условие прогона
        return


def external_search(
    adapter: SearchAdapterPort,
    query: SearchQuery,
    *,
    store: Any,
    run_id: str,
    effort: SearchEffort,
    budget: SearchBudget,
    product_id: str,
    cache: ContentAddressedCache | None = None,
    clock: Callable[[], str] | None = None,
) -> list[SearchResult]:
    """Ограниченный бюджетом одиночный внешний поиск с записью в ``search_log`` (I16/I19/I23).

    Тонкая обёртка над :func:`external_search_observed`: возвращает только кандидатов.

    :param cache: глобальный content-addressed кэш (R7); ``None`` — без кэша.
    """
    return list(
        external_search_observed(
            adapter,
            query,
            store=store,
            run_id=run_id,
            effort=effort,
            budget=budget,
            product_id=product_id,
            cache=cache,
            clock=clock,
        ).results
    )


def external_search_observed(
    adapter: SearchAdapterPort,
    query: SearchQuery,
    *,
    store: Any,
    run_id: str,
    effort: SearchEffort,
    budget: SearchBudget,
    product_id: str,
    cache: ContentAddressedCache | None = None,
    clock: Callable[[], str] | None = None,
) -> SearchOutcome:
    """Один внешний поиск: SERP-кэш (R7) → бюджет → вызов → ``search_log`` (§5).

    Порядок шагов (он важен):

    1. **Кэш.** Ключ :func:`core.util.cache.serp_key` — текст + провайдер + **язык** +
       ``limit``. Попадание → кандидаты из кэша, адаптер **не** вызывается и бюджет
       **не** списывается: иначе второй прогон отличался бы от первого по ``search_log``,
       а через него — по наблюдению и ``metrics`` (R7 обещает «без живого ре-фетча», а не
       «с другим счётом»).
    2. **Бюджет.** Исчерпан продукт-специфичный остаток → ``BUDGET_EXHAUSTED`` без вызова.
    3. **Вызов** ``adapter.search(query)``; расход списывается через
       :meth:`SearchEffort.charge` (в т.ч. неуспешный: сеть была потрачена, и именно
       неуспех чаще всего означает троттлинг — обнулять счётчик значило бы снимать
       единственный предохранитель; факт сбоя виден в :attr:`SearchOutcome.error` и в
       наблюдении контура).
    4. **Запись в кэш** — только удачный ответ (сбой в кэш не попадает, иначе R7
       зафиксировал бы поломку как результат).

    **S9a: подмена бюджета на месте.** Прежняя редакция присваивала ``effort.budget``
    объекту вызывающего. Теперь: ``budget``, переданный сюда, применяется только если
    ``effort`` своего лимита для фазы не несёт (:meth:`SearchEffort.with_budget` даёт
    **вид** с тем же журналом расходов). Наблюдённый факт: в SERP-пути эта ветка
    **не срабатывает** — ``serp_discover`` передаёт ``budget=effort.budget``, то есть
    счётчик уже несёт бюджет, и ``scoped is effort``. Ветка остаётся для вызова с
    «чужим» счётчиком; сегодня в дереве такого вызова нет (единственный потребитель —
    SERP-контур), поэтому ветка покрыта тестом, а не прогоном.

    Гарантия I19: возвращаются только кандидаты :class:`SearchResult`; функция не
    производит evidence и не обращается к хранилищу за доказательствами.

    :param effort: счётчик усилия на ``(phase, product_id)`` (ключ симметрии I16).
    :param budget: бюджет прогона; используется, если ``effort`` его не несёт.
    :param product_id: продукт, на который списывается усилие (I16).
    :param cache: глобальный content-addressed кэш (R7); ``None`` — без кэша.
    """
    phase = _require_phase(query.phase)
    provider = getattr(adapter, "provider", None)
    adapter_kind = getattr(adapter, "adapter_kind", None)

    cache_key = _serp_cache_key(cache, adapter, query)
    if cache_key is not None:
        cached = _serp_cache_read(cache, cache_key)
        if cached is not None:
            hit_termination = (
                termination_for(False, 0, False, None) if not cached else None
            )
            record_search(
                store,
                run_id=run_id,
                phase=phase,
                provider=provider,
                adapter_kind=adapter_kind,
                language=query.lang,
                query=query.text,
                results_count=len(cached),
                termination_reason=hit_termination,
                clock=clock,
            )
            return SearchOutcome(
                results=tuple(cached),
                termination=hit_termination,
                from_cache=True,
                # Живого вызова не было — времени не ушло: это наблюдённый ноль, а не
                # «не измеряли» (сеть в этом пути не задействована по построению).
                timing=SearchTiming(measured=True, from_cache=True),
            )

    # ``effort`` — носитель бюджета (charge не принимает бюджет отдельно). Если он пуст,
    # а явный ``budget`` задаёт лимит для фазы — берём **вид** с этим бюджетом, не меняя
    # исходный объект (S9a); журнал расходов общий, поэтому списания видны вызывающему.
    scoped = effort
    if (
        budget is not None
        and effort.budget.limit_for(phase) is None
        and budget.limit_for(phase) is not None
    ):
        scoped = effort.with_budget(budget)

    if scoped.remaining(phase, product_id) == 0:
        record_search(
            store,
            run_id=run_id,
            phase=phase,
            provider=provider,
            adapter_kind=adapter_kind,
            language=query.lang,
            query=query.text,
            results_count=0,
            termination_reason=TerminationReason.BUDGET_EXHAUSTED,
            clock=clock,
        )
        return SearchOutcome(
            termination=TerminationReason.BUDGET_EXHAUSTED,
            # Вызова не было — ожидания тоже: ноль наблюдаем по построению.
            timing=SearchTiming(measured=True),
        )

    error: BaseException | None = None
    results: list[SearchResult] = []
    try:
        results = list(adapter.search(query))
    except Exception as exc:  # noqa: BLE001 - сбой одного продукта не роняет остальные (P4)
        error = exc

    # Вызов (в т.ч. неуспешный) израсходовал усилие провайдера — списываем бюджет.
    charged = scoped.charge(phase, product_id)
    # S4: время вызова наблюдается **после** него, включая неуспешный — ожидание до
    # сбоя тоже стоимость прогона, и терять его значило бы показывать сбой бесплатным.
    timing = _observed_timing(adapter)

    if error is not None:
        termination: TerminationReason | None = termination_for(
            False, None, False, error
        )
    elif not results:
        termination = termination_for(False, 0, False, None)
    else:
        # Результаты есть — протокол продолжается; терминация фиксируется вызывающим.
        termination = None

    record_search(
        store,
        run_id=run_id,
        phase=phase,
        provider=provider,
        adapter_kind=adapter_kind,
        language=query.lang,
        query=query.text,
        results_count=(None if error is not None else len(results)),
        termination_reason=termination,
        clock=clock,
    )
    if error is None and cache_key is not None:
        _serp_cache_write(cache, cache_key, adapter, query, results)
    return SearchOutcome(
        results=tuple(results),
        termination=termination,
        error=error,
        charged=charged,
        timing=timing,
    )
