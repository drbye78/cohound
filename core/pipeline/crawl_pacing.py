"""coHound — прибор темпа краула: наблюдённая конкурентность и ожидание по origin.

Зачем отдельный прибор. Темп обхода задаёт **лимитер**, а не число потоков: воркеры
пула ждут резерв слота у :class:`~core.util.rate_limit.RateLimiter`, и «сколько
запросов в секунду даёт хост» до сих пор нельзя было прочитать ниоткуда — только
пересчитать руками из ``fetch_started_at`` элементов сеанса. Владелец (06.11.2026):
«не ограничивать, а измерять»; потолок без опубликованного числа — дефект. Здесь
число и снимается.

**Что именно наблюдается (и где единственная точка).** Per-fetch точка одна —
:func:`core.pipeline.corpus_ingest.local_fetch` (и её inline-двойник
``fetch_and_commit``): там известны и продукт, и origin, и длительность живого
вызова. Поэтому:

* :class:`CrawlPacing` держит под локом счётчик запросов **в полёте** на origin и его
  пик (high-water). Пик — измеренный факт конкретного прогона, а не ширина пула:
  браузерный маршрут сериализован, и его пик ``1`` — тоже факт (что и требуется
  увидеть, прежде чем решать вопрос о параллелизме браузера).
* Ожидание лимитера считает **сам лимитер** (:meth:`RateLimiter.wait_stats`): только
  он знает ``delay``/``remaining``. Здесь оно лишь сводится в тот же блок.

**Провенанс, не идентичность.** Блок ``pacing`` не входит ни в
``EXECUTION_CONTRACT_FIELDS``, ни в ``run_id``, ни в ``crawl_config_digest``: это
измерение исполнения, а не условие измерения. Носитель — провенанс сеанса краула
(``crawl_sessions.provenance_json``): его читают и ``metrics.json`` (через
``resources.crawl``), и ``hound crawl status --per-host``. Второго носителя нет —
счётчик, который нельзя пересчитать из durable-факта, метрикой не является.

**Формулы (воспроизводимы).**

* ``requests_per_s = requests / window_s``, где ``window_s`` — от **начала первого**
  живого запроса origin'а до **конца последнего** (монотонные часы). Окно включает
  паузы лимитера: именно оно и есть наблюдённая производительность хоста. Один
  запрос или вырожденное окно → ``None`` (делить не на что), а не ``0``/бесконечность.
* ``in_flight_max`` — максимум одновременных вызовов ``fetch`` на origin; для агрегата
  берётся **максимум по host'ам**, не сумма (сумма пиков не наблюдалась ни в один
  момент).
* ``limiter_waits``/``limiter_waited_s`` — кумулятив лимитера по origin (см.
  :meth:`RateLimiter.wait_stats`); у host'а без объявленной задержки это ``0`` — факт
  «ждать было нечего», а не «не измеряли».
* ``fetch_ms`` p50/p95 — nearest-rank по наблюдённым длительностям (та же функция,
  что у метрики краула: один ряд — одна формула).

Нейтрально к предметной категории (§1.3): host — строка из данных, никаких названий
продуктов и вендоров.
"""

from __future__ import annotations

import datetime as _dt
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from core.pipeline.crawl_metric import nearest_rank
from core.util.rate_limit import RateLimiter, normalize_host

__all__ = ["CrawlPacing", "pacing_from_provenance"]

#: Точность публикуемых дробей (запросы/с, секунды). Один знак — политика блока
#: ``resources`` — для темпа слишком груб: 0.05 req/s превратилось бы в 0.1, а окно
#: короче секунды — в ноль. Шесть знаков секунды = микросекунда, столько же, сколько
#: несут метки окна (``first_request_at``/``last_request_at`` в ISO с микросекундами):
#: окно и частота в артефакте обязаны быть пересчитываемы друг из друга.
PACING_DIGITS = 6


@dataclass
class _OriginState:
    """Наблюдённое состояние одного origin'а (запросы, полёт, окно, длительности)."""

    requests: int = 0
    in_flight: int = 0
    in_flight_max: int = 0
    fetch_ms: list[int] = field(default_factory=list)
    first_started: float | None = None
    last_finished: float | None = None
    first_wall: float | None = None
    last_wall: float | None = None


@dataclass
class _ProductState:
    """То же в разрезе продукта: продукт — граница коммита, не origin'а."""

    requests: int = 0
    fetch_ms: list[int] = field(default_factory=list)
    first_started: float | None = None
    last_finished: float | None = None
    first_wall: float | None = None
    last_wall: float | None = None


def _iso_utc(seconds: float | None) -> str | None:
    """Стенное время (``time.time``) → ISO-8601 UTC; ``None`` остаётся ``None``.

    Микросекунды сохраняются целочисленно: метка окна — провенанс, и округлять её до
    секунды значило бы выдумать окно, которого не было (окна бывают короче секунды).
    """
    if seconds is None:
        return None
    whole = int(seconds)
    micros = round((seconds - whole) * 1_000_000)
    if micros >= 1_000_000:  # округление переполнило секунду
        whole += 1
        micros -= 1_000_000
    return (
        _dt.datetime.fromtimestamp(whole, _dt.UTC)
        .replace(microsecond=micros)
        .isoformat()
    )


def _window_s(first: float | None, last: float | None) -> float | None:
    """Длительность окна в секундах; вырожденное/неполное окно → ``None``."""
    if first is None or last is None:
        return None
    value = last - first
    return value if value > 0.0 else None


def _rate(requests: int, window_s: float | None) -> float | None:
    """Наблюдённая частота: ``requests / window_s``; нечего делить → ``None``."""
    if window_s is None or window_s <= 0.0:
        return None
    return round(requests / window_s, PACING_DIGITS)


def _published_window(first: float | None, last: float | None) -> float | None:
    """Окно в секундах в **публикуемой** точности (частота считается из него же).

    Округление до публикации, а не после: иначе ``requests / window_s`` в артефакте не
    сходилось бы с ``requests_per_s`` на последнем знаке, и читатель получал бы два
    разных числа одного факта.
    """
    window = _window_s(first, last)
    return round(window, PACING_DIGITS) if window is not None else None


def _ms_block(values: list[int]) -> dict[str, Any]:
    """``{p50, p95, n}`` по наблюдённым длительностям (пустая выборка → ``null``)."""
    return {
        "p50": nearest_rank(values, 50),
        "p95": nearest_rank(values, 95),
        "n": len(values),
    }


def _earliest(first: float | None, candidate: float | None) -> float | None:
    """Минимум двух моментов (``None`` — отсутствие наблюдения, не ноль)."""
    if candidate is None:
        return first
    if first is None or candidate < first:
        return candidate
    return first


def _latest(last: float | None, candidate: float | None) -> float | None:
    """Максимум двух моментов (``None`` — отсутствие наблюдения, не ноль)."""
    if candidate is None:
        return last
    if last is None or candidate > last:
        return candidate
    return last


class CrawlPacing:
    """Наблюдатель темпа: запросы в полёте, пик, окно и длительности по origin.

    Один экземпляр на фазу корпуса (как :class:`~core.pipeline.corpus_state.CorpusCounters`):
    его мутируют **воркеры пула** (по одному вызову на живой фетч), а читает драйвер при
    финализации сеанса. Методы короткие и под локом — сама сеть под локом не идёт.

    :param limiter: владелец пейсинга; его кумулятивное ожидание входит в снимок.
        ``None`` — лимитера нет (тест/иной путь): ожидание публикуется нулём, и это факт
        «ждать было нечем».
    :param clock: монотонные часы (дефолт ``time.monotonic``) — окно и длительности.
    :param wall: стенное время (дефолт ``time.time``) — метки окна в ISO.
    """

    def __init__(
        self,
        *,
        limiter: RateLimiter | None = None,
        clock: Any = time.monotonic,
        wall: Any = time.time,
    ) -> None:
        self._limiter = limiter
        self._clock = clock
        self._wall = wall
        self._lock = threading.Lock()
        self._origins: dict[str, _OriginState] = {}
        self._products: dict[tuple[str, str], _ProductState] = {}

    @contextmanager
    def request(self, product_id: str, host: str) -> Iterator[None]:
        """Учесть один живой фетч на ``host``: вход/выход из полёта, окно, длительность.

        Контекст оборачивает **только** сетевой вызов (``fetch``), не ожидание лимитера:
        спящий поток к хосту не обращается, и считать его «в полёте» значило бы завысить
        наблюдённую конкурентность. Пик фиксируется на входе — так он не зависит от того,
        какой из потоков первым отпустит лок.
        """
        origin = normalize_host(host)
        started = self._clock()
        started_wall = self._wall()
        with self._lock:
            state = self._origins.setdefault(origin, _OriginState())
            product = self._products.setdefault((product_id, origin), _ProductState())
            state.requests += 1
            product.requests += 1
            state.in_flight += 1
            if state.in_flight > state.in_flight_max:
                state.in_flight_max = state.in_flight
            if state.first_started is None:
                state.first_started = started
                state.first_wall = started_wall
            if product.first_started is None:
                product.first_started = started
                product.first_wall = started_wall
        try:
            yield
        finally:
            finished = self._clock()
            finished_wall = self._wall()
            elapsed_ms = int((finished - started) * 1000)
            with self._lock:
                state.in_flight -= 1
                state.fetch_ms.append(elapsed_ms)
                product.fetch_ms.append(elapsed_ms)
                # Метки «последний» — максимум, а не последняя запись: поток мог
                # задержаться на захвате лока уже после замера.
                state.last_finished = _latest(state.last_finished, finished)
                if state.last_finished == finished:
                    state.last_wall = finished_wall
                product.last_finished = _latest(product.last_finished, finished)
                if product.last_finished == finished:
                    product.last_wall = finished_wall

    def snapshot(self) -> dict[str, Any]:
        """Снимок наблюдений: ``{hosts, products, total}`` (детерминированный порядок).

        Копирует накопленные ряды под локом: снимок уходит в провенанс сеанса и в
        ``metrics.json``, где мутация задним числом недопустима.
        """
        with self._lock:
            origins = {
                host: (
                    state.requests,
                    state.in_flight_max,
                    list(state.fetch_ms),
                    state.first_started,
                    state.last_finished,
                    state.first_wall,
                    state.last_wall,
                )
                for host, state in self._origins.items()
            }
            products = {
                key: (
                    state.requests,
                    list(state.fetch_ms),
                    state.first_started,
                    state.last_finished,
                    state.first_wall,
                    state.last_wall,
                )
                for key, state in self._products.items()
            }
        waits = self._limiter.wait_stats() if self._limiter is not None else {}

        hosts: dict[str, Any] = {}
        all_values: list[int] = []
        total_first: float | None = None
        total_last: float | None = None
        total_first_wall: float | None = None
        total_last_wall: float | None = None
        for host in sorted(origins):
            requests, peak, values, first, last, first_wall, last_wall = origins[host]
            all_values.extend(values)
            window = _published_window(first, last)
            wait = waits.get(host) or {"waits": 0, "waited_s": 0.0}
            hosts[host] = {
                "requests": requests,
                "requests_per_s": _rate(requests, window),
                "in_flight_max": peak,
                "limiter_waits": int(wait["waits"]),
                "limiter_waited_s": round(float(wait["waited_s"]), PACING_DIGITS),
                "fetch_ms": _ms_block(values),
                "window_s": window,
                "first_request_at": _iso_utc(first_wall),
                "last_request_at": _iso_utc(last_wall),
            }
            total_first = _earliest(total_first, first)
            total_last = _latest(total_last, last)
            total_first_wall = _earliest(total_first_wall, first_wall)
            total_last_wall = _latest(total_last_wall, last_wall)

        products_block: dict[str, dict[str, Any]] = {}
        products_values: dict[str, list[int]] = {}
        for product_id, host in sorted(products):
            (
                requests,
                values,
                first,
                last,
                first_wall,
                last_wall,
            ) = products[(product_id, host)]
            entry = products_block.setdefault(
                product_id,
                {
                    "requests": 0,
                    "hosts": [],
                    "first_started": None,
                    "last_finished": None,
                    "first_wall": None,
                    "last_wall": None,
                },
            )
            entry["requests"] += requests
            entry["hosts"].append(host)
            entry["first_started"] = _earliest(entry["first_started"], first)
            entry["last_finished"] = _latest(entry["last_finished"], last)
            entry["first_wall"] = _earliest(entry["first_wall"], first_wall)
            entry["last_wall"] = _latest(entry["last_wall"], last_wall)
            products_values.setdefault(product_id, []).extend(values)
        for product_id, entry in products_block.items():
            window = _published_window(
                entry.pop("first_started"), entry.pop("last_finished")
            )
            entry["hosts"] = sorted(set(entry.pop("hosts")))
            entry["requests"] = int(entry["requests"])
            entry["requests_per_s"] = _rate(entry["requests"], window)
            entry["fetch_ms"] = _ms_block(products_values.get(product_id, []))
            entry["window_s"] = window
            entry["first_request_at"] = _iso_utc(entry.pop("first_wall"))
            entry["last_request_at"] = _iso_utc(entry.pop("last_wall"))

        total_window = _published_window(total_first, total_last)
        total_requests = sum(row["requests"] for row in hosts.values())
        total = {
            "hosts": len(hosts),
            "products": len(products_block),
            "requests": total_requests,
            "requests_per_s": _rate(total_requests, total_window),
            # Максимум, не сумма: сумма пиков не наблюдалась ни в один момент.
            "in_flight_max": max(
                (row["in_flight_max"] for row in hosts.values()), default=0
            ),
            "limiter_waits": sum(row["limiter_waits"] for row in hosts.values()),
            "limiter_waited_s": round(
                sum(row["limiter_waited_s"] for row in hosts.values()), PACING_DIGITS
            ),
            "fetch_ms": _ms_block(all_values),
            "window_s": total_window,
            "first_request_at": _iso_utc(total_first_wall),
            "last_request_at": _iso_utc(total_last_wall),
        }
        return {
            "hosts": hosts,
            "products": {key: products_block[key] for key in sorted(products_block)},
            "total": total,
        }


def pacing_from_provenance(
    provenance: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Блок ``pacing`` из провенанса сеанса; нет/не mapping → ``None``.

    ``None`` значит «не измеряли» (старый сеанс, прерванный процесс), и это честнее
    нулей: ноль был бы утверждением, что запросов не было.
    """
    if not provenance:
        return None
    block = provenance.get("pacing")
    return dict(block) if isinstance(block, Mapping) and block else None