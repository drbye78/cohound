"""coHound — метрика краула (``17`` §7.2, фаза F8): числа из durable-фактов сеанса.

Модуль **не заводит носителя**: он читает строку сеанса (``crawl_sessions``) и его элементы
(``crawl_session_items``) и считает из них опубликованные величины §7.2. Носителя второго
нет намеренно — «снимок метрик» разошёлся бы с элементами при первом же ``resume``, а
метрика, которую нельзя пересчитать из durable-фактов, метрикой не является.

Что публикуется (§7.2, пункты 1–4):

1. ``hosts``, ``urls``, ``crawl_only_wall`` — из сеансовой строки
   (``finished_at - started_at``). Сеанс ещё идёт (``finished_at is None``) → печатается
   прошедшее время и флаг ``running``: незавершённое окно **не** выдаётся за итог.
2. На host — ``requests`` и **распределение** интервалов: ``observed_min_interval_s``,
   ``median_interval_s`` и число интервалов короче объявленного
   (``intervals_below_declared`` из ``intervals_total``, доля ``share_below_declared_pct``).
   Минимум сам по себе вердиктом не является: на 801 интервале два коротких дают
   ``ratio≈0.007`` при медиане 3.3 s, и «систематическое нарушение» из этого числа —
   чтение, которого в данных нет. Нарушением называется **факт** хотя бы одного интервала
   короче объявленного (``politeness_violation``), а его масштаб публикуется числом.
3. ``fetch_ms`` p50/p95 — на host и по сеансу; ``None`` (resume из retained-байтов, ``304``,
   отказ до сети) в выборку не входит, его число публикуется отдельно
   (``fetch_ms_unknown``). Пустая выборка → ``null``, а не ``0``.
4. ``backlog_bytes`` на конец — из ``ExtractQueue.backlog(...)`` (F6): второй способ считать
   те же байты не заводится, потому что он разошёлся бы с очередью при первом же удалении
   retained-файла.

Плюс ``requests_by_state`` и ``errors_by_kind``: отказ фетча — тоже результат краула, и
«сколько запросов и почему не доехало» обязано быть видно рядом с таймингами.

**Темп (прибор темпа, ``pacing``).** ``requests_per_s``, ``in_flight_max``,
``limiter_waits`` и ``limiter_waited_s`` берутся из блока ``pacing`` провенанса сеанса
(:mod:`core.pipeline.crawl_pacing`) — и **ниоткуда больше**: пик одновременных запросов и
суммарное ожидание лимитера нельзя восстановить из элементов сеанса, а второй носитель
разошёлся бы с первым. Там же — ``http_requests``/``foreign_hosts``: **фактические**
HTTP-запросы (лента ``robots.txt``, хопы редиректов, повторы загрузчика) и сколько из них
ушло не на host полосы. Сеанс без блока (старый или прерванный прогон) даёт ``null`` в
этих полях и заметку ``notes``: «не измеряли» — это факт, а ``0`` был бы выдуманным
измерением. ``requests_per_s`` — наблюдённая частота ``requests / окно`` (окно от первого
до последнего живого запроса origin'а, включая паузы лимитера).

**Формулы (воспроизводимы, без «примерно»).**

* ``observed_min_interval_s`` — минимум по разностям **соседних** моментов
  ``fetch_started_at`` одного host'а после сортировки по возрастанию. Меньше двух попыток →
  ``None``: интервала нет, и ни ``0`` (он означал бы «склеены»), ни «бесконечно» (он означал
  бы «не наблюдалось»).
* ``median_interval_s`` — медиана того же ряда интервалов по правилу nearest-rank (см. ниже):
  число, которое говорит, **как часто** хост видел запросы, тогда как минимум говорит лишь
  «был ли хоть один короткий». Оба публикуются рядом; одно без другого допускает неверное
  чтение в обе стороны.
* ``intervals_below_declared`` / ``share_below_declared_pct`` — сколько интервалов строго
  короче объявленного минимума и какая это доля. Считаются только при объявленном
  ``min_s > 0``: «троттлинга нет» — обещания нет, нарушать нечего (``null``, не ``0``).
* перцентиль — **nearest-rank**: для выборки ``v[0..n-1]`` (отсортирована по возрастанию) и
  целого ``p`` ранг ``= ceil(p*n/100)`` (1-based, целочисленно: ``(p*n + 99)//100``), ответ
  ``v[ранг-1]``. Никакой интерполяции и никакого усреднения соседей: значение — всегда один
  из наблюдённых замеров, поэтому число воспроизводимо на том же наборе. ``p50`` при чётном
  ``n`` — нижняя медиана (``n=4``, ``[1,2,3,100]`` → ``p50=2``), ``p95`` при ``n=4`` → ранг
  ``ceil(3.8)=4`` → максимум.
* ``ratio = observed_min_interval_s / declared_min_interval_s``; нарушение — ``ratio < 1``.

**Откуда объявленный минимум.** Из durable-провенанса сеанса, а **не** из текущего
``deploy.yaml``/``run.yaml`` (конфиг мог измениться после прогона). Основной носитель —
per-host карта записи корня: ``effective_limits.roots[*].rate_limits{host}.min_s``
(пишет драйвер из плана, которым фетчили, — см. ``Frontier.note_rate_limit``). Прежний
формат ``rate_limit``/``limits.rate_limit`` (один минимум на корень) читается как
совместимость: host тогда — host самого ``root``. Ключи host'ов нормализуются
:func:`core.util.rate_limit.normalize_host` — тем же правилом, что у лимитера. Если корни
одного host'а объявили **разные** минимумы, одного числа на host не существует:
``declared_min_interval_s`` = ``None``, а варианты публикуются
(``declared_min_interval_variants``) — подставить «среднее» значило бы выдумать обещание,
которого никто не давал. Минимум не восстановим → ``None`` и ничего не подставляется.

Нейтрален к предметной категории (§1.3): host'ы и числа, никаких названий продуктов.
"""

from __future__ import annotations

import datetime as _dt
import itertools
from collections.abc import Iterable, Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit

from core.domain import CrawlItemStatus
from core.util.rate_limit import normalize_host

__all__ = [
    "build_crawl_metric",
    "declared_min_intervals",
    "nearest_rank",
    "observed_intervals_s",
    "observed_min_interval_s",
]

#: Откуда берётся объявленная вежливость — единая строка для заметок payload'а.
#: Новый формат (F8) — per-host карта; прежний per-root ``rate_limit`` читается как
#: совместимость (см. :func:`declared_min_intervals`), поэтому в строке названы оба.
DECLARED_SOURCE = "effective_limits.roots[*].rate_limits{host}.min_s"


def _parse_iso(value: str | None) -> _dt.datetime | None:
    """ISO-8601 → ``datetime`` (naive трактуется как UTC); ``None``/мусор → ``None``.

    ``None`` здесь означает «момент не наблюдался», а не «нулевой момент»: вызывающий обязан
    различать эти случаи, иначе интервал посчитался бы от выдуманного нуля.
    """
    if not value:
        return None
    try:
        parsed = _dt.datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.UTC)
    return parsed


def _host_of(url: str) -> str:
    """Host строки — тот же ключ вежливости, что у лимитера (``normalize_host``)."""
    if not url:
        return ""
    try:
        name = urlsplit(url).hostname or ""
    except ValueError:  # неразбираемый URL — одна «безымянная» группа (как у лимитера)
        return ""
    return normalize_host(name)


def nearest_rank(values: Sequence[float], percentile: int) -> float | None:
    """Перцентиль по правилу nearest-rank (определение — в докстринге модуля).

    ``rank = ceil(p*n/100)`` (1-based, целочисленно), результат — ``v[rank-1]``
    отсортированной по возрастанию выборки. ``p`` — **целый** процент: дробные перцентили
    не нужны (§7.2 требует p50/p95), а вещественное ``0.95*n`` дало бы ``19.0000…04`` для
    ``n=20`` и подняло бы ранг на единицу — то есть молча другой ответ.

    Пустая выборка → ``None``: измерять нечего, и ``0`` здесь был бы выдуманным замером.
    ``p`` вне ``1..100`` зажимается в границы (``<=0`` → 1, ``>100`` → ``n``).
    """
    if not values:
        return None
    ordered = sorted(values)
    n = len(ordered)
    rank = (max(1, min(int(percentile), 100)) * n + 99) // 100
    return ordered[min(max(rank, 1), n) - 1]


def observed_intervals_s(timestamps: Iterable[str | None]) -> list[float]:
    """Ряд интервалов между соседними попытками (сек), по возрастанию моментов.

    Один ряд на все производные величины host'а (минимум, медиана, число коротких):
    второй проход по моментам разошёлся бы с первым на неразбираемом значении. Меньше
    двух наблюдаемых моментов → пустой список (интервалов нет).
    """
    moments = sorted(m for m in (_parse_iso(value) for value in timestamps) if m is not None)
    return [(later - earlier).total_seconds() for earlier, later in itertools.pairwise(moments)]


def observed_min_interval_s(timestamps: Iterable[str | None]) -> float | None:
    """Минимальный интервал между соседними попытками (сек) из ``fetch_started_at``.

    Моменты сортируются по возрастанию, берётся минимум по разностям соседей. Меньше двух
    наблюдаемых моментов → ``None`` (интервала нет). Неразбираемые значения пропускаются:
    они не «нулевой интервал», а отсутствие наблюдения.
    """
    intervals = observed_intervals_s(timestamps)
    if not intervals:
        return None
    return min(intervals)


def _min_s(spec: Any) -> float | None:
    """``min_s`` из объявленного ``rate_limit`` (mapping из провенанса либо объект)."""
    if spec is None:
        return None
    value = getattr(spec, "min_s", None)
    if value is None and isinstance(spec, Mapping):
        value = spec.get("min_s")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pacing_block(provenance: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Блок ``pacing`` из провенанса сеанса (темп); нет/не mapping → ``None``.

    Читается напрямую из провенанса, а не через :mod:`core.pipeline.crawl_pacing`: тот
    импортирует :func:`nearest_rank` отсюда, и обратный импорт дал бы цикл. Форма блока —
    его контракт; здесь только защита от чужого типа.
    """
    if not provenance:
        return None
    block = provenance.get("pacing")
    return dict(block) if isinstance(block, Mapping) and block else None


def declared_min_intervals(
    provenance: Mapping[str, Any] | None,
) -> dict[str, tuple[float, ...]]:
    """``{host: объявленные min_s}`` из durable-провенанса сеанса.

    Читаются записи корней ``effective_limits.roots[*]`` в порядке приоритета:

    1. ``rate_limits: {host: {min_s, max_s}}`` — **per-host** карта (F8): один корень
       штатно фетчит несколько host'ов с разными минимумами, поэтому host берётся
       ключом карты и нормализуется тем же правилом, что у лимитера;
    2. ``rate_limit``/``limits.rate_limit`` — прежний формат **per-root**: тогда host —
       host самого ``root``. Это только совместимость со старыми провенансами; на новом
       пишется карта, и подстановка «один минимум на корень» больше не нужна.

    Значения по host'у дедуплицируются и сортируются — форма стабильна. Host'ы без
    объявленного значения в словарь не попадают: «не объявлено» и «объявлено 0» — разные
    факты (ноль даёт ``ratio: null`` и «нарушать нечего», отсутствие — ``null`` и
    «неизмеримо»).

    Записи всех корней host'а собираются **списком**: если корни объявили разные
    минимумы, одного числа на host не существует, и решать за них здесь нельзя (см.
    :func:`build_crawl_metric`).
    """
    prov = provenance or {}
    limits = prov.get("effective_limits")
    roots = limits.get("roots") if isinstance(limits, Mapping) else None
    collected: dict[str, set[float]] = {}
    for record in roots or ():
        if not isinstance(record, Mapping):
            continue
        per_host = record.get("rate_limits")
        if isinstance(per_host, Mapping) and per_host:
            # F8: карта по host'ам — авторитетна для этой записи; корневой ``root``
            # здесь не адресат (у корня может быть несколько host'ов).
            for raw_host, spec in per_host.items():
                host = normalize_host(str(raw_host))
                value = _min_s(spec)
                if not host or value is None:
                    continue
                collected.setdefault(host, set()).add(value)
            continue
        host = _host_of(str(record.get("root") or ""))
        if not host:
            continue
        value = _min_s(record.get("rate_limit"))
        if value is None:
            inner = record.get("limits")
            if isinstance(inner, Mapping):
                value = _min_s(inner.get("rate_limit"))
        if value is None:
            continue
        collected.setdefault(host, set()).add(value)
    return {host: tuple(sorted(values)) for host, values in sorted(collected.items())}


def _fetch_ms_block(values: Sequence[int]) -> dict[str, Any]:
    """``{p50, p95, n}`` по наблюдённым ``fetch_ms`` (пустая выборка → ``null``)."""
    return {
        "p50": nearest_rank(values, 50),
        "p95": nearest_rank(values, 95),
        "n": len(values),
    }


def _status_counts(items: Sequence[Any]) -> dict[str, int]:
    """Число запросов по каждому статусу элемента (ключи — все значения enum)."""
    counts = {status.value: 0 for status in CrawlItemStatus}
    for item in items:
        status = getattr(item, "status", None)
        key = status.value if isinstance(status, CrawlItemStatus) else str(status)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _error_counts(items: Sequence[Any]) -> dict[str, int]:
    """Число отказов по классу (``error_kind``); пустые классы не печатаются."""
    counts: dict[str, int] = {}
    for item in items:
        kind = getattr(item, "error_kind", None)
        if not kind:
            continue
        key = str(kind)
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _host_row(
    host: str,
    items: Sequence[Any],
    variants: tuple[float, ...],
    pacing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Строка host'а: запросы, интервал, отношение к объявленному, ``fetch_ms``, темп.

    ``requests`` — число попыток с наблюдённым началом (``fetch_started_at``): именно из них
    считается интервал. ``items`` — все элементы host'а, включая те, что не дошли до сети
    (``fetch_started_at is None``): разница между двумя числами — видимая доля «не начались».

    ``pacing`` — строка host'а из блока ``pacing`` провенанса (:mod:`core.pipeline.crawl_pacing`);
    ``None`` — прибор не работал, и поля темпа остаются ``None`` (не нули).
    """
    timestamps = [getattr(item, "fetch_started_at", None) for item in items]
    intervals = observed_intervals_s(timestamps)
    observed = min(intervals) if intervals else None
    attempts = sum(1 for value in timestamps if value)
    fetch_values = [
        int(value)
        for value in (getattr(item, "fetch_ms", None) for item in items)
        if value is not None
    ]
    declared: float | None = None
    conflict = list(variants) if len(variants) > 1 else []
    if len(variants) == 1:
        declared = variants[0]
    ratio: float | None = None
    violation: bool | None = None
    below: int | None = None
    share: float | None = None
    if declared is not None and declared <= 0.0:
        # Объявлено «без троттлинга» (``min_s=0``): отношения не существует, нарушать
        # нечего — и при отсутствии наблюдения тоже (``false``, а не ``null``: это не
        # «неизмеримо», а «обещания нет»).
        violation = False
    elif observed is not None and declared is not None:
        ratio = observed / declared
        violation = ratio < 1.0
        # Масштаб нарушения: сколько интервалов короче обещанного. Пустой ряд интервалов
        # сюда не доходит (``observed`` был бы ``None``), поэтому деление определено.
        below = sum(1 for value in intervals if value < declared)
        if intervals:
            share = round(100.0 * below / len(intervals), 2)
    return {
        "host": host,
        "items": len(items),
        "requests": attempts,
        # Учёт фактических HTTP-запросов (лента ``robots.txt``, хопы редиректов, повторы):
        # ``http_requests`` — сколько запросов реально ушло, ``foreign_hosts`` — сколько из
        # них не на этот host. ``None``/пусто — прибор этого не наблюдал (не ноль).
        "http_requests": (pacing or {}).get("http_requests"),
        "foreign_hosts": dict((pacing or {}).get("foreign_hosts") or {}),
        # Темп — из наблюдённого блока; отсутствие блока = «не измеряли» (``None``).
        "requests_per_s": (pacing or {}).get("requests_per_s"),
        "in_flight_max": (pacing or {}).get("in_flight_max"),
        "limiter_waits": (pacing or {}).get("limiter_waits"),
        "limiter_waited_s": (pacing or {}).get("limiter_waited_s"),
        "observed_min_interval_s": observed,
        "median_interval_s": nearest_rank(intervals, 50),
        "intervals_total": len(intervals),
        "intervals_below_declared": below,
        "share_below_declared_pct": share,
        "declared_min_interval_s": declared,
        "declared_min_interval_variants": conflict,
        "ratio": ratio,
        "politeness_violation": violation,
        "fetch_ms": _fetch_ms_block(fetch_values),
        "fetch_ms_unknown": len(items) - len(fetch_values),
        "requests_by_state": _status_counts(items),
        "errors_by_kind": _error_counts(items),
    }


def _backlog_block(backlog: Any) -> dict[str, Any]:
    """``backlog_bytes``/``queue_rows`` из ``ExtractQueue.backlog(...)`` (17 §4.3, F6).

    Принимается результат ``backlog()`` (объект с ``as_dict``); ``None`` — очередь не
    читалась (сеанса нет) и тогда печатаются честные нули: пустая очередь и «неизвестно»
    различаются тем, что во втором случае сюда вообще не приходят.
    """
    data = backlog.as_dict() if backlog is not None else {}
    return {
        "backlog_bytes": int(data.get("backlog_bytes", 0)),
        "backlog_files": int(data.get("backlog_files", 0)),
        "backlog_missing": int(data.get("backlog_missing", 0)),
        "queue_rows": dict(sorted((data.get("queue_rows") or {}).items())),
    }


def build_crawl_metric(
    *,
    session: Any,
    items: Iterable[Any],
    provenance: Mapping[str, Any] | None = None,
    backlog: Any = None,
    now: _dt.datetime | None = None,
) -> dict[str, Any]:
    """Метрика краула (§7.2) из строки сеанса, его элементов и провенанса.

    ``now`` — только для идущего сеанса (``finished_at is None``): прошедшее время считается
    до него. Терминальный сеанс от ``now`` не зависит — итог окна берётся из ``finished_at``,
    поэтому повторный вызов даёт то же число.

    Формируемые ключи (стабильны, ``--json`` полагается на них):

    * ``hosts``/``urls``/``crawl_only_wall`` (``{seconds, started_at, ended_at, running}``);
    * ``requests_by_state``/``errors_by_kind``/``fetch_ms``/``fetch_ms_unknown``;
    * ``backlog_bytes``/``backlog_files``/``backlog_missing``/``queue_rows``;
    * ``per_host`` — строки :func:`_host_row` в лексикографическом порядке host'ов;
    * ``pacing`` — блок темпа из провенанса (``hosts``/``products``/``total``), ``None`` —
      прибор не работал;
    * ``declared_min_interval_source`` и ``notes`` — откуда взята объявленная вежливость и
      что из неё не восстановилось (пустой ``notes`` — всё восстановлено).
    """
    rows = list(items)
    declared = declared_min_intervals(provenance)
    pacing_block = _pacing_block(provenance)
    pacing_hosts = (pacing_block.get("hosts") or {}) if pacing_block else {}
    by_host: dict[str, list[Any]] = {}
    for item in rows:
        by_host.setdefault(_host_of(str(getattr(item, "url", "") or "")), []).append(item)

    started_at = getattr(session, "started_at", None) if session is not None else None
    finished_at = getattr(session, "finished_at", None) if session is not None else None
    started = _parse_iso(started_at)
    finished = _parse_iso(finished_at)
    running = finished is None
    ended = finished if finished is not None else (now or _dt.datetime.now(_dt.UTC))
    wall = (
        (ended - started).total_seconds()
        if started is not None and ended is not None
        else None
    )

    per_host = [
        _host_row(host, by_host[host], declared.get(host, ()), pacing_hosts.get(host))
        for host in sorted(by_host)
    ]
    fetch_values = [
        int(value)
        for value in (getattr(item, "fetch_ms", None) for item in rows)
        if value is not None
    ]

    notes: list[str] = []
    if not declared:
        notes.append(
            f"declared_min_interval_s: не восстановим из провенанса "
            f"({DECLARED_SOURCE} пуст) — ratio и politeness не считаются"
        )
    else:
        missing = sorted(host for host in by_host if host not in declared)
        if missing:
            notes.append(
                "declared_min_interval_s: нет объявленного значения для host'ов: "
                + ", ".join(missing)
            )
    conflicted = sorted(row["host"] for row in per_host if row["declared_min_interval_variants"])
    if conflicted:
        notes.append(
            "declared_min_interval_s: корни одного host'а объявили разные минимумы (варианты "
            "в declared_min_interval_variants): " + ", ".join(conflicted)
        )
    if not pacing_block:
        notes.append(
            "pacing: не измерялся (в провенансе сеанса нет блока pacing) — "
            "requests_per_s/in_flight_max/limiter_* не считаются"
        )

    metric: dict[str, Any] = {
        "hosts": len(by_host),
        "urls": len(rows),
        "crawl_only_wall": {
            "seconds": wall,
            "started_at": started_at,
            "ended_at": finished_at,
            # Незавершённое окно названо флагом: «сколько прошло» ≠ «сколько занял краул».
            "running": running,
        },
        "requests_by_state": _status_counts(rows),
        "errors_by_kind": _error_counts(rows),
        "fetch_ms": _fetch_ms_block(fetch_values),
        "fetch_ms_unknown": len(rows) - len(fetch_values),
        "declared_min_interval_source": DECLARED_SOURCE,
        "per_host": per_host,
        # Полный блок темпа (hosts/products/total) — тот же носитель, что в провенансе:
        # терминал печатает выборку, ``--json`` не теряет агрегат по продукту/прогону.
        "pacing": pacing_block,
        "notes": notes,
        **_backlog_block(backlog),
    }
    return metric