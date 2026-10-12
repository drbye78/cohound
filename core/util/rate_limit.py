"""coHound — пейсинг и backoff краула (полизнозно независимы от адаптеров).

Родовые примитивы вежливости к целевым сайтам, не зависящие от конкретного
краулера/транспорта (только stdlib). Здесь нет I/O кроме инъектируемого ``sleep``:
детерминизм в тестах достигается подстановкой ``random.Random(seed)``.

* :func:`jittered_backoff` — Full Jitter (AWS) для экспоненциальных повторов;
* :class:`RateLimiter` — межзапросный пейсинг по :class:`~core.domain.models.RateLimitSpec`
  (uniform-диапазон), с внедряемыми ``sleep``/``rng``.

Модуль категорийно-нейтрален (§1.3): нет названий продуктов/вендоров/предметных
терминов. Специфика сайтов — данные в ``packages/``.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import Any

from core.domain.models import RateLimitSpec  # frozen value-объект: поля min_s/max_s

__all__ = ["RateLimiter", "jittered_backoff"]


def _uniform(rng: random.Random | None, low: float, high: float) -> float:
    """``uniform(low, high)`` через внедрённый ``rng`` или модуль ``random``.

    Модуль ``random`` предоставляет тот же интерфейс ``uniform``, что и
    :class:`random.Random`; приводим к ``Any``, чтобы не сужать тип модуля.
    """
    generator: Any = rng if rng is not None else random
    return generator.uniform(low, high)


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
    """Пейсинг politeness: ``sleep(uniform(spec.min_s, spec.max_s))`` на запрос.

    Инъектируем ``sleep`` (для тестов) и ``rng`` (детерминизм: ``random.Random(seed)``).
    Никаких глобальных состояний/синглтонов: per-host состояние, если понадобится
    централизованный пейсинг, живёт во владельце (см. полосу P2-3 — pipeline/corpus).

    :param sleep: функция сна ``(seconds) -> None`` (дефолт ``time.sleep``).
    :param rng: источник случайности (дефолт — модуль ``random``).
    """

    def __init__(
        self,
        *,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._sleep = sleep
        self._rng = rng

    def wait(self, host: str, spec: RateLimitSpec | None) -> float:
        """Спит случайную задержку для ``host``; ``spec is None`` → ``0.0`` без сна.

        Возвращает фактически проспанное число секунд. ``host`` принимается для
        API-совместимости будущего per-host пейсинга; текущая политика — единый
        uniform-диапазон ``spec`` (специфика сайтов — данные, не код).
        """
        if spec is None:
            return 0.0
        delay = _uniform(self._rng, spec.min_s, spec.max_s)
        self._sleep(delay)
        return delay
