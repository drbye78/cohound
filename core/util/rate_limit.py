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

import random
import threading
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

from core.domain.models import RateLimitSpec  # frozen value-объект: поля min_s/max_s

__all__ = ["RateLimiter", "jittered_backoff"]

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


def _normalize_host(host: str) -> str:
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

    ``spec is None`` → ``0.0`` без сна и без записи состояния. Иначе возвращается
    фактически проспанное число секунд (``target - now``; при конкуренции оно может
    превышать сэмплированную задержку — это и есть ожидание своей очереди к origin'у).

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
    ) -> None:
        self._sleep = sleep
        self._rng = rng
        self._clock = clock
        #: Резерв отправки по origin: ``last[origin]`` — момент, раньше которого
        #: следующий запрос к этому origin'у уходить не должен.
        self._last: dict[str, float] = {}
        #: Лок защищает **только** чтение/запись ``_last`` и сэмплирование ``rng``
        #: (``random.Random`` не потокобезопасен). Сон — строго вне лока.
        self._lock = threading.Lock()

    def _prune(self, now: float) -> None:
        """Удаляет записи с резервом в прошлом; вызывается под локом.

        ``last <= now`` ⇒ для будущего вызова ``max(now, last) == now``, т.е. запись
        инертна и её удаление не меняет ни один интервал. Записи в будущем (в т.ч.
        только что зарезервированная этим потоком) сохраняются.
        """
        if len(self._last) < _PRUNE_AT:
            return
        self._last = {key: target for key, target in self._last.items() if target > now}

    def wait(self, host: str, spec: RateLimitSpec | None) -> float:
        """Резервирует слот для ``host`` и спит до него; возвращает проспанные секунды.

        ``spec is None`` → ``0.0`` без сна и без мутации состояния. Иначе:
        ``target = max(now, last[origin]) + uniform(spec.min_s, spec.max_s)``;
        ``last[origin] = target``; сон до ``target`` — **вне** лока.
        """
        if spec is None:
            return 0.0
        origin = _normalize_host(host)
        with self._lock:
            now = self._clock()
            delay = _uniform(self._rng, spec.min_s, spec.max_s)
            target = max(now, self._last.get(origin, now)) + delay
            self._last[origin] = target
            self._prune(now)
        remaining = target - now
        self._sleep(remaining)
        return remaining
