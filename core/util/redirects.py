"""Похоповая валидация редиректов для путей, у которых свой HTTP-клиент (R10, §4 P3).

Зачем модуль. Транспорт краула (``core/adapters/crawl_http.py``) перестал полагаться на
``follow_redirects=True``: цикл ведёт адаптер, и каждый ``Location`` проходит
``validate_url`` **до** выпуска запроса. Но три пути держали собственный ``httpx``-клиент
и обходили транспорт: ``fetch`` поисковых адаптеров и HTTP-клиент реестров. У них
``follow_redirects=True`` означал, что публичная страница может отправить транспорт на
``127.0.0.1``/``10.0.0.1``/metadata-эндпоинт, а проверка (если она вообще была) увидела бы
уже готовый ответ. Политика одна — :func:`follow_validated_redirects`, а не три копии
цикла.

Почему шов — «получить один хоп», а не «клиент». Вызывающие обращаются к сети
по-разному: поисковые адаптеры уже оборачивают запрос в bounded retry
(:mod:`core.util.retry`), реестровый клиент — нет. Если бы модуль сам звал
``client.get``, то для адаптеров пришлось бы либо ретраить весь маршрут целиком (и терять
типизированный отказ хопа внутри общей ошибки ретрая), либо дублировать цикл. Поэтому
модуль просит только одно: «сходи на один URL и верни ответ», а решение о повторах
остаётся у вызывающего.

Поведение:

* каждый следующий хоп берётся у ``fetch_hop`` **только** после того, как ``Location``
  разрешён относительно текущего URL (:func:`urllib.parse.urljoin`) и прошёл
  ``validate_url``;
* хоп — только ``301/302/303/307/308`` **с** ``Location`` (набор ``httpx``); ``3xx`` без
  ``Location`` остаётся обычным ответом, как и у ``httpx``;
* небезопасная цель — **ошибка вызывающего**, чей тип передан параметром
  (``SearchError``/``RegistryFetchError``): запрос на неё не выпускается, её байты не
  читаются и не кэшируются;
* потолок хопов — ``max_redirects`` (дефолт 20, ровно дефолт ``httpx``); превышение —
  та же ошибка вызывающего (как ``TooManyRedirects``);
* **отказаться от следования нельзя**: это и есть предмет функции. Кому нужен ответ
  ``3xx`` «как есть», тот её просто не зовёт (так делает ``crawl_http`` при
  ``follow_redirects=False``) — выключенный по умолчанию режим внутри значил бы, что
  безопасность зависит от того, не забыл ли вызывающий её включить.

**Чего модуль не делает.** Пост-резолвной проверки адреса (DNS-rebinding) здесь нет — это
принятый риск S9g, записанный в :func:`core.util.security.validate_url`. Блокируются
**литеральные** приватные/петлевые адреса в ``Location``, а не имя, резолвящееся в
приватный IP. Тело промежуточного ответа читается вызывающим (один ``GET`` на хоп), как и
раньше — ``httpx`` с ``follow_redirects`` тоже читал его и отбрасывал.

Модуль категорийно нейтрален (§1.3): только stdlib и ``core.util.security``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin

from core.util.security import validate_url

__all__ = [
    "DEFAULT_MAX_REDIRECTS",
    "REDIRECT_STATUSES",
    "follow_validated_redirects",
]

#: Потолок числа редиректов на один фетч — ровно дефолт ``httpx``
#: (``httpx._config.DEFAULT_MAX_REDIRECTS``): ручной цикл не вводит нового поведения,
#: а переносит валидацию каждого хопа на транспорт.
DEFAULT_MAX_REDIRECTS = 20

#: Статусы, на которых делается следующий хоп (набор ``httpx``).
REDIRECT_STATUSES: frozenset[int] = frozenset({301, 302, 303, 307, 308})

def _location(response: Any) -> str | None:
    """Значение ``Location`` ответа либо ``None`` (устойчиво к dict-дублёрам в тестах)."""
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    getter = getattr(headers, "get", None)
    if not callable(getter):
        return None
    value = getter("Location")
    return None if value is None else str(value)

def follow_validated_redirects(
    fetch_hop: Callable[[str], Any],
    url: str,
    *,
    error: type[Exception],
    label: str,
    max_redirects: int = DEFAULT_MAX_REDIRECTS,
) -> tuple[Any, str]:
    """Проходит цепочку редиректов, валидируя каждый хоп; ``(ответ, финальный URL)``.

    :param fetch_hop: одна сетевая попытка: ``(url) -> ответ`` с ``status_code`` и
        ``headers``. Повторы/таймауты — внутри неё (это дело вызывающего).
    :param url: **запрошенный** URL; он же база для относительных ``Location``.
    :param error: тип ошибки вызывающего (``SearchError``/``RegistryFetchError``/…):
        отказ хопа и превышение потолка поднимаются им, а не нейтральным ``RuntimeError``, —
        вызывающий продолжает ловить один класс на все сбои транспорта, а причина
        («редирект на небезопасный адрес») остаётся в сообщении.
    :param label: имя вызова для сообщения об ошибке (URL не хардкодится в шапке текста).
    :param max_redirects: потолок хопов; превышение — ошибка вызывающего.
    :returns: ответ последнего хопа и URL, на котором он получен.
    :raises error: небезопасная цель ``Location`` или превышение числа хопов.
    """
    current = url
    hops = 0
    while True:
        response = fetch_hop(current)
        try:
            status = int(response.status_code)
        except (TypeError, ValueError):
            return response, current
        if status not in REDIRECT_STATUSES:
            return response, current
        location = _location(response)
        if not location:
            # ``3xx`` без ``Location`` — не хоп: остаётся обычным ответом (как у httpx).
            return response, current
        hops += 1
        if hops > max_redirects:
            raise error(f"{label}: превышено число редиректов ({max_redirects})")
        target = urljoin(current, location)
        try:
            validate_url(target)
        except ValueError as exc:
            raise error(
                f"{label}: редирект на небезопасный адрес отклонён SSRF-валидацией: {exc}"
            ) from exc
        current = target
