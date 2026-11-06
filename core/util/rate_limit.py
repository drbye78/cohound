"""coHound — пейсинг и backoff краула (политика, независимая от адаптеров).

Родовые примитивы вежливости к целевым сайтам, не зависящие от конкретного
краулера/транспорта (только stdlib). Здесь нет I/O кроме инъектируемого ``sleep``:
детерминизм в тестах достигается подстановкой ``random.Random(seed)`` и фейковых
``clock``/``sleep``.

* :func:`jittered_backoff` — Full Jitter (AWS) для экспоненциальных повторов;
* :class:`RateLimiter` — **межзапросный интервал по origin** (uniform-диапазон
  :class:`~core.domain.models.RateLimitSpec`), с внедряемыми ``sleep``/``clock``/``rng``.

**Гарантия и её граница (важно).** ``RateLimiter`` держит состояние **в процессе**:
два запроса к одному origin, сделанные из одного процесса (в т.ч. из разных потоков),
разнесены не меньше чем на выбранную задержку. Это не глобальный лимит: при пуле
**процессов** у каждого процесса свой экземпляр, и суммарная частота к origin остаётся
``процессы × 1/задержка``. Общий лимит требует разделяемого состояния (внешняя
координация/шов) — отдельная задача, здесь она не решается. В ``split``-режиме
единственный фетчер — процесс DMZ-планировщика, поэтому «в процессе» == «на origin».

Модуль категорийно-нейтрален (§1.3): нет названий продуктов/вендоров/предметных
терминов. Специфика сайтов — данные в ``packages/``.
"""

from __future__ import annotations

import datetime as _dt
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from core.domain.models import RateLimitSpec  # frozen value-объект: поля min_s/max_s

__all__ = ["RateLimiter", "jittered_backoff", "normalize_host"]

#: Порог, после которого ``RateLimiter`` вычищает заведомо инертные записи.
#: Это **не** сброс интервала: удаляются только записи, чей резерв уже в прошлом
#: (см. :meth:`RateLimiter._prune`), они не влияют ни на один будущий резерв.
_PRUNE_AT = 1024


def _uniform(rng: random.Random | None, low: float, high: float) -> float:
    """``uniform(low, high)`` через внедрённый ``rng`` или модуль ``random``.

    Модуль ``random`` предоставляет тот же интерфейс ``uniform``, что и
    :class:`random.Random`; приводим к ``Any``, чтобы не сужать тип модуля.
    """
    generator: Any = rng if rng is not None else random
    return generator.uniform(low, high)


def normalize_host(host: str) -> str:
    """Ключ origin для пейсинга: нижний регистр, без порта, без завершающей точки.

    Штатные вызывающие передают ``urlsplit(url).hostname.lower()`` — уже без схемы и
    порта (``core/pipeline/corpus.py``, ``core/edge/scheduler.py``). Нормализация здесь
    нужна, чтобы публичный ``wait`` не создавал разные ключи для ``Example.COM``,
    ``example.com`` и ``example.com:443``: для полизности это один сайт. Схему и порт
    лимитер не различает (они не передаются), поэтому «склеиваем» варианты в один
    origin — это ошибка в сторону **более медленного** пейсинга, безопасную для
    ``source_scope: exhaustive``.

    IPv6-литерал (``[::1]:443``) приводится к ``::1``; пустая строка остаётся пустой
    (неразбираемый URL — одна «безымянная» группа, консервативно).

    **Одна формула на два потребителя.** Функция публичная, потому что тем же правилом
    нормализуется **ключ шарда** (:mod:`core.config.deploy`): шард — множество host'ов, и
    если реестр нормализует ``Example.COM`` иначе, чем лимитер/frontier, «disjoint по
    построению» становится ложью (`docs/architecture/17-crawl-host-scheduler-plan.md` §2).
    Вторая копия правила разошлась бы молча — ровно тот класс, что «два ключа на один сайт».
    """
    value = (host or "").strip().lower()
    if not value:
        return ""
    if "://" in value:  # защита от вызывающего, передавшего полный URL
        value = (urlsplit(value).hostname or "").lower()
    if value.startswith("["):
        end = value.find("]")
        if end != -1:
            value = value[1:end]
    elif value.count(":") == 1:
        name, _, port = value.partition(":")
        if port.isdigit():
            value = name
    return value.rstrip(".")


def jittered_backoff(
    attempt: int,
    *,
    base_s: float = 0.5,
    cap_s: float = 30.0,
    rng: random.Random | None = None,
) -> float:
    """Full Jitter (AWS): ``uniform(0, min(cap_s, base_s * 2**attempt))``; ``attempt`` 0-based.

    Задержка перед ``attempt``-м повтором (``attempt=0`` — первый повтор). Верхняя
    граница растёт экспоненциально (``base_s * 2**attempt``), но не превышает ``cap_s``;
    фактическая задержка — равномерно в ``[0, ceiling]`` (анти-синхронизация стаи).

    :raises ValueError: ``attempt < 0``; ``base_s <= 0``; ``cap_s <= 0``.
    """
    if attempt < 0:
        raise ValueError(f"attempt обязан быть >= 0, получено {attempt!r}")
    if base_s <= 0:
        raise ValueError(f"base_s обязан быть > 0, получено {base_s!r}")
    if cap_s <= 0:
        raise ValueError(f"cap_s обязан быть > 0, получено {cap_s!r}")
    try:
        ceiling = base_s * (2**attempt)
    except OverflowError:  # гигантский attempt → потолок заведомо cap_s
        ceiling = cap_s
    return _uniform(rng, 0.0, min(cap_s, ceiling))


def _iso_utc_us(micros: int) -> str:
    """ISO-8601 (UTC) из **целых** микросекунд — без повторного округления.

    ``datetime.fromtimestamp(micros / 1e6)`` округлил бы ещё раз (float на эпохе ~1.7e9
    уже не точен до микросекунды), поэтому секунды и микросекунды разделяются целочисленно.
    """
    seconds, rest = divmod(micros, 1_000_000)
    return (
        _dt.datetime.fromtimestamp(seconds, _dt.UTC)
        .replace(microsecond=rest)
        .isoformat()
    )


@dataclass
class _WaitStat:
    """Кумулятивное ожидание одного origin: сколько раз ждали и сколько секунд.

    Ведётся лимитером под его же локом (там, где известны ``delay``/``remaining``), и
    только публикуется наружу. Политику паузы не меняет: это наблюдение, а не рычаг.
    """

    waits: int = 0
    waited_s: float = 0.0


class RateLimiter:
    """Межзапросный интервал **по origin** с резервированием времени (не сон под локом).

    Каждый вызов :meth:`wait` резервирует себе момент отправки запроса:

    1. коротко берётся лок;
    2. ``target = max(now, last[origin]) + delay`` (``delay`` сэмплируется из ``spec``),
       ``last[origin] = target``;
    3. лок **отпускается**, и только затем поток спит до ``target``.

    Почему так, а не «лок вокруг сна»: удерживая мьютекс на время сна, мы сериализовали
    бы **все** origin'ы — краулер схлопнулся бы в один запрос в полёте глобально, что
    хуже исходного дефекта (``N`` воркеров × ``1/delay`` на хост). Резервирование даёт
    корректный интервал для одного origin'а и параллельность для разных. **Не
    «упрощать»** обратно в сон под локом.

    ``spec is None`` → ``0.0`` без сна и без записи состояния. Иначе :meth:`wait`
    возвращает фактически проспанное число секунд (``target - now``; при конкуренции оно
    может превышать сэмплированную задержку — это и есть ожидание своей очереди к origin'у).

    **Разрешённый момент, а не момент пробуждения.** :meth:`wait_reserved` делает то же
    самое, но возвращает **момент резерва** в ISO-8601 (UTC). Это нужно там, где метка
    попытки пишется в провенанс: метка, снятая после пробуждения, несёт задержку передачи
    GIL, и интервал между двумя такими метками оказывался **короче** объявленного на
    25 мкс…9 мс (замер 02.11.2026: 3 интервала из 10 715 в пробе когорты «нарушали»
    вежливость, хотя ``time.sleep`` здесь не возвращается раньше запрошенного — min
    50.089 мс на 300 пробах). Проверка вежливости мерила планировщик потоков, а не наш
    пейсинг; резерв — это то, чем мы вправду управляем.

    **Метка ведётся в целых микросекундах отдельной цепью.** Метка — то, что читает
    метрика вежливости (`crawl_metric.observed_intervals_s`), а её носитель — ISO-8601 с
    микросекундным разрешением. Если считать ISO из двух чтений часов на вызов
    (``wall + (target - now)``), дрожание между чтениями (замер: 19 993…20 024 мкс при
    объявленных 20 000) и **квантование самой метки ±1 мкс** делают строгое
    ``observed < declared`` подбрасыванием монеты. Поэтому цепь метки строится в целых
    микросекундах: ``label = max(wall_us, last_label) + delay_us``. Тогда разность двух
    соседних меток равна ``delay`` **по построению**, а не «примерно»; шаг пейсинга —
    ровно тот, что объявлен, и никакого допуска в проверке не нужно.

    Состояние ограничено: при ``len(state) >= _PRUNE_AT`` удаляются записи с
    ``last <= now`` — их резерв уже в прошлом, поэтому ``max(now, last) == now`` и на
    любой будущий резерв они не влияют. Произвольного «потолка с обнулением» нет.

    :param sleep: функция сна ``(seconds) -> None`` (дефолт ``time.sleep``).
    :param rng: источник случайности (дефолт — модуль ``random``).
    :param clock: монотонные часы ``() -> float`` (дефолт ``time.monotonic``).
    """

    def __init__(
        self,
        *,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], float] = time.time,
    ) -> None:
        self._sleep = sleep
        self._rng = rng
        self._clock = clock
        #: Стенное время для :meth:`wait_reserved` (монотонные часы в ISO не переводятся).
        self._wall = wall
        #: Резерв отправки по origin: ``last[origin]`` — момент, раньше которого
        #: следующий запрос к этому origin'у уходить не должен.
        self._last: dict[str, float] = {}
        #: Та же цепь в **стенных** микросекундах (int): только для :meth:`wait_reserved`.
        #: Шаг этой цепи равен ``delay`` ровно, поэтому интервал между публикуемыми
        #: метками не несёт ни дрожания часов, ни квантования ISO.
        self._last_label: dict[str, int] = {}
        #: Наблюдённое ожидание по origin (см. :meth:`wait_stats`). Счётчики
        #: **кумулятивные**: ``_prune`` их не трогает — это итог, а не резерв.
        self._waits: dict[str, _WaitStat] = {}
        #: Лок защищает **только** чтение/запись ``_last`` и сэмплирование ``rng``
        #: (``random.Random`` не потокобезопасен). Сон — строго вне лока.
        self._lock = threading.Lock()

    def _prune(self, now: float, wall_us: int) -> None:
        """Удаляет записи с резервом в прошлом; вызывается под локом.

        ``last <= now`` ⇒ для будущего вызова ``max(now, last) == now``, т.е. запись
        инертна и её удаление не меняет ни один интервал. Записи в будущем (в т.ч.
        только что зарезервированная этим потоком) сохраняются. Цепь меток чистится по
        тому же правилу, но по **стенному** времени (``max(wall_us, label)``).
        """
        if len(self._last) < _PRUNE_AT and len(self._last_label) < _PRUNE_AT:
            return
        self._last = {key: target for key, target in self._last.items() if target > now}
        self._last_label = {
            key: label for key, label in self._last_label.items() if label > wall_us
        }

    def _reserve(self, host: str, spec: RateLimitSpec) -> tuple[float, int]:
        """Резервирует слот под локом: проспанные секунды и метку (стенные микросекунды)."""
        origin = normalize_host(host)
        with self._lock:
            now = self._clock()
            delay = _uniform(self._rng, spec.min_s, spec.max_s)
            target = max(now, self._last.get(origin, now)) + delay
            self._last[origin] = target
            # Метка — своя цепь в целых микросекундах, шаг которой равен ``delay`` ровно
            # (см. docstring класса). Округление одно, на входе в цепь.
            wall_us = round(self._wall() * 1_000_000)
            label_us = max(wall_us, self._last_label.get(origin, wall_us)) + round(
                delay * 1_000_000
            )
            self._last_label[origin] = label_us
            self._prune(now, wall_us)
            remaining = target - now
            # Наблюдение ожидания (не политика): сколько раз и сколько секунд этот origin
            # заставил ждать. ``remaining > 0`` — собственно пауза; резерв с нулевой
            # задержкой (``min_s = max_s = 0``) ожиданием не считается.
            if remaining > 0.0:
                stat = self._waits.setdefault(origin, _WaitStat())
                stat.waits += 1
                stat.waited_s += remaining
        return remaining, label_us

    def wait(self, host: str, spec: RateLimitSpec | None) -> float:
        """Резервирует слот для ``host`` и спит до него; возвращает проспанные секунды.

        ``spec is None`` → ``0.0`` без сна и без мутации состояния. Иначе:
        ``target = max(now, last[origin]) + uniform(spec.min_s, spec.max_s)``;
        ``last[origin] = target``; сон до ``target`` — **вне** лока.
        """
        if spec is None:
            return 0.0
        remaining, _label = self._reserve(host, spec)
        self._sleep(remaining)
        return remaining

    def wait_stats(self) -> dict[str, dict[str, float]]:
        """Наблюдённое ожидание по origin: ``{host: {waits, waited_s}}`` (кумулятивно).

        ``waits`` — сколько резервов **действительно ждали** (``remaining > 0``), а не
        сколько раз вызывали ``wait``: резерв без задержки (``min_s = max_s = 0``) паузой
        не является и в счёт не идёт. ``waited_s`` — сумма запланированных пауз: ровно
        то, что поток проспит до целевого момента (``time.sleep`` не возвращается раньше,
        но может позже — число остаётся нижней границей фактического простоя).
        Записи с нулём не появляются: «не ждали» и «ждали 0 секунд» — один факт.

        Это **провенанс исполнения**, а не условие измерения: в ``run_id``/дайджесты
        значение не входит. Потребитель — прибор темпа
        (:mod:`core.pipeline.crawl_pacing`).
        """
        with self._lock:
            return {
                origin: {"waits": stat.waits, "waited_s": stat.waited_s}
                for origin, stat in sorted(self._waits.items())
            }

    def wait_reserved(self, host: str, spec: RateLimitSpec | None) -> str:
        """Резервирует слот, спит до него и возвращает **разрешённый** момент (ISO-8601 UTC).

        Сон и резерв — тот же контракт, что у :meth:`wait`; разница в возвращаемом
        значении. Им пользуется провенанс попытки (``fetch_started_at``): метка должна
        означать «с этого момента запросу разрешено уйти», а не «когда поток проснулся» —
        иначе измеренный интервал между попытками несёт задержку передачи GIL, и проверка
        вежливости меряет планировщик потоков вместо нашего пейсинга. ``spec is None`` —
        пейсинга нет, разрешённый момент равен текущему.
        """
        if spec is None:
            return _iso_utc_us(round(self._wall() * 1_000_000))
        remaining, label_us = self._reserve(host, spec)
        self._sleep(remaining)
        return _iso_utc_us(label_us)
