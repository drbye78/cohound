"""Повторы HTTP-вызовов: bounded retry/backoff с уважением ``Retry-After`` (S6).

Модуль вынесен **из** поискового адаптера (``core/adapters/search_yandex.py``), потому что
политика повторов — общая для всех HTTP-адаптеров, а не провайдерская. До вынесения она
жила в одном адаптере, и второй (``search_glm.py``) на ``429``/``5xx`` падал с первой
попытки, причём бюджет прогона всё равно списывался (§3.4 S6 плана
``15-external-review-findings-plan.md``). Второй экземпляр той же политики разошёлся бы с
первым при первой же правке, поэтому она одна и параметризуется только «как звать» и
«чем сообщать».

Политика:

* повторяются транспортные сбои и статусы :data:`RETRYABLE_STATUSES` (``429``/``5xx``);
  прочие ``4xx`` (400/401/403/404/422) **не** повторяются: это ответ по существу;
* ``Retry-After`` (целые секунды или HTTP-date) уважается, но усечён
  :data:`MAX_RETRY_DELAY`;
* без ``Retry-After`` — exponential backoff с **аддитивным** jitter (до +50 % базы):
  диапазоны последовательных задержек не пересекаются, поэтому задержки растут
  монотонно (это проверяется тестом);
* попыток не больше :data:`MAX_RETRY_ATTEMPTS`; ожидание идёт через внедряемую ``sleep``,
  поэтому тесты не спят;
* в сообщение об ошибке попадают только статус/имя исключения и число попыток — ни
  заголовков, ни ключей (они могут содержать секреты).

Модуль категорийно нейтрален (§1.3), использует только stdlib; часа для ``Retry-After``
инжектируются (``now``), поэтому разбор детерминирован в тестах.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

__all__ = [
    "BASE_RETRY_DELAY",
    "MAX_RETRY_ATTEMPTS",
    "MAX_RETRY_DELAY",
    "RETRYABLE_STATUSES",
    "HttpRetryError",
    "backoff_delay",
    "parse_retry_after",
    "request_with_retry",
    "retry_after_header",
]

#: Максимальное число попыток реального HTTP-вызова (включая первую).
MAX_RETRY_ATTEMPTS = 5
#: Базовая задержка exponential backoff, с.
BASE_RETRY_DELAY = 1.0
#: Потолок задержки повтора, с (включая ``Retry-After``).
MAX_RETRY_DELAY = 60.0
#: Статусы, при которых реальный HTTP-вызов повторяется (``429`` — троттлинг провайдера).
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

class HttpRetryError(RuntimeError):
    """Повторы исчерпаны либо получен неретраибельный статус/сбой.

    Адаптеры транслируют его в свой тип ошибки (``SearchError``), поэтому вызывающий
    pipeline видит прежнюю семантику «вызов не удался», а не «странный тип из утилит».
    """

def retry_after_header(response: Any) -> str | None:
    """Достаёт ``Retry-After`` из заголовков ответа (регистронезависимо)."""
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    getter = getattr(headers, "get", None)
    if callable(getter):
        for name in ("Retry-After", "retry-after"):
            value = getter(name)
            if value is not None:
                return str(value)
        return None
    if isinstance(headers, Mapping):
        for key, value in headers.items():
            if str(key).lower() == "retry-after":
                return str(value)
    return None

def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """Разбирает ``Retry-After``: целые секунды или HTTP-date → секунды ожидания."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return max(0.0, float(int(text)))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError, OverflowError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    reference = now if now is not None else datetime.now(UTC)
    return max(0.0, (when - reference).total_seconds())

def backoff_delay(
    attempt: int,
    retry_after: str | None = None,
    *,
    base: float = BASE_RETRY_DELAY,
    cap: float = MAX_RETRY_DELAY,
    now: datetime | None = None,
) -> float:
    """Задержка перед повтором: ``Retry-After`` либо exponential backoff с jitter.

    ``attempt`` — номер только что завершившейся попытки (1-based). Jitter аддитивный
    (до +50 % базы), поэтому диапазоны последовательных задержек не пересекаются и растут.
    """
    parsed = parse_retry_after(retry_after, now=now)
    if parsed is not None:
        return min(cap, parsed)
    delay = base * (2 ** (attempt - 1))
    # S311: jitter повтора между клиентами одного провайдера, а не секрет. Здесь нужна
    # именно дешёвая не-криптографическая случайность; CSPRNG был бы бесполезен (не
    # скрывает ничего и не защищает от перебора) и дороже в горячем пути ретрая.
    jittered = delay * (1.0 + random.uniform(0.0, 0.5))  # noqa: S311 — jitter, не секрет
    return min(cap, jittered)

def request_with_retry(
    request: Callable[[], Any],
    *,
    label: str,
    sleep: Callable[[float], Any] = time.sleep,
    transport_error: Any = (),
    max_attempts: int = MAX_RETRY_ATTEMPTS,
) -> Any:
    """Выполняет HTTP-вызов с bounded retry/backoff.

    :param request: одна попытка вызова; возвращает объект-ответ с ``status_code``,
        ``content`` и (возможно) ``headers``.
    :param label: человекочитаемое имя вызова для сообщения об ошибке.
    :param sleep: внедряемое ожидание (в тестах — ``list.append``).
    :param transport_error: класс/кортеж транспортных исключений клиента (``httpx.
        TransportError``); пустой кортеж — только статусы считаются ретраибельными.
    :param max_attempts: сколько всего попыток делать (включая первую).
    :returns: ответ последней успешной попытки (объект как есть).
    :raises HttpRetryError: повторы исчерпаны либо ошибка неретраибельна.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            response = request()
        except transport_error as exc:
            if attempt < max_attempts:
                sleep(backoff_delay(attempt, None))
                continue
            raise HttpRetryError(
                f"{label} не удался после {attempt} попыток: {type(exc).__name__}"
            ) from exc
        except Exception as exc:  # noqa: BLE001 - унификация ошибок вызова
            raise HttpRetryError(f"{label} не удался: {type(exc).__name__}") from exc

        status = getattr(response, "status_code", None)
        if (
            isinstance(status, int)
            and status in RETRYABLE_STATUSES
            and attempt < max_attempts
        ):
            sleep(backoff_delay(attempt, retry_after_header(response)))
            continue
        if isinstance(status, int) and status >= 400:
            raise HttpRetryError(
                f"{label} не удался: HTTP {status} после {attempt} попыток"
            )
        return response
