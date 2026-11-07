"""Учёт **фактических** HTTP-запросов попытки фетча — провенанс, не идентичность.

Зачем отдельный счётчик. Прибор темпа (:mod:`core.pipeline.crawl_pacing`) считает
**попытки фетча** (``requests``): сколько раз вызывающий попросил страницу. Хост при этом
получает больше запросов — ``robots.txt``, каждый хоп редиректа, повтор после повтора
загрузчика. Измерено 07.11.2026 на панели C (``docs.tantorlabs.ru``): опубликовано
≈1.57 запроса/с, фактически ≈3.1 — на каждый документ уходила ещё и лента
(``301`` → чужой хост → ``404``), а счётчик её не видел. Число, которое нельзя проверить,
метрикой не является.

**Где учитывается.** Там, где запрос действительно выпускается, — на транспорте:

* путь ``http`` — :meth:`core.adapters.crawl_http.HttpCrawler._get_real` отмечает каждый
  хоп перед ``httpx.stream``; лента ``robots.txt`` идёт через тот же ``fetch`` и потому
  считается;
* путь ``scrapy`` — downloader-middleware (:mod:`core.adapters.crawl_scrapy`) отмечает
  каждый запрос, дошедший до загрузчика, включая ленту и хопы редиректов;
* прочие HTTP-клиенты с собственным циклом редиректов
  (:func:`core.util.redirects.follow_validated_redirects`) отмечают каждый хоп.

**Область наблюдения — попытка фетча**, а не процесс: :func:`attempt` кладёт счётчик в
``threading.local()``, поэтому пул воркеров не смешивает попытки, а вложенность
восстанавливает предыдущий счётчик (обёртка вокруг обёртки не теряет внешнюю).
Scrapy-путь считает в потоке реактора, а не в потоке фетча, поэтому адаптер забирает
текущий счётчик (:func:`current_attempt`) и передаёт его движку явно — счётчик
потокобезопасен (:class:`HttpAudit` держит лок).

**Атрибуция по фактическому хосту** запроса, а не по хосту полосы: редирект на объявленный
чужой хост — это запрос к чужому хосту, и он виден в ``foreign_hosts``.

**Отсутствие наблюдения — ``None``/пустая карта, а не ноль.** Счётчик, который никто не
трогал (кэш-попадание, внедрённый шов, браузерный путь), публикует ``http_requests: None``:
«не измеряли» и «измерено 0 запросов» — разные факты.

Блок не входит ни в ``run_id``, ни в ``crawl_config_digest``, ни в
``EXECUTION_CONTRACT_FIELDS``: это измерение исполнения, а не условие измерения.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlsplit

__all__ = ["HttpAudit", "attempt", "current_attempt", "note_request"]

#: Счётчик текущей попытки фетча в этом потоке (``None`` — попытки нет).
_local = threading.local()


def _host_of(url: str) -> str:
    """Host запроса для атрибуции (нижний регистр; неразбираемый URL — пустая строка)."""
    try:
        return (urlsplit(str(url)).hostname or "").lower()
    except (TypeError, ValueError):
        return ""


class HttpAudit:
    """Счётчик запросов одной попытки: сколько ушло и на какие хосты.

    Мутируется из потока, который реально выпускает запрос (для Scrapy — поток реактора),
    читается в конце попытки потоком фетча; поэтому все операции под локом.
    """

    __slots__ = ("_hosts", "_lane_host", "_lock", "_observed", "_requests")

    def __init__(self, lane_host: str | None = None) -> None:
        #: Хост полосы: запросы «не на него» публикуются как ``foreign_hosts``.
        self._lane_host = (lane_host or "").lower() or None
        self._lock = threading.Lock()
        self._requests = 0
        self._hosts: dict[str, int] = {}
        self._observed = False

    def __copy__(self) -> HttpAudit:
        """Копия — **тот же** счётчик: это handle попытки, а не значение."""
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> HttpAudit:
        """``deepcopy`` — тоже тот же счётчик.

        Обязательно, а не косметика: ``scrapy.settings.Settings.copy()`` (его зовёт
        ``Crawler.__init__``) — это ``copy.deepcopy``. Без этого метода копирование упало
        бы на ``threading.Lock``, а если бы и удалось — middleware считала бы в **копию**,
        и опубликованное ``http_requests`` было бы нулём при живых запросах.
        """
        return self

    def note(self, url: str) -> None:
        """Отметить один HTTP-запрос к ``url`` (вызывается **перед** выпуском запроса)."""
        host = _host_of(url)
        with self._lock:
            self._observed = True
            self._requests += 1
            if host:
                self._hosts[host] = self._hosts.get(host, 0) + 1

    def snapshot(self) -> dict[str, Any]:
        """Публикуемая форма: ``{http_requests, foreign_hosts}``.

        ``http_requests`` — число запросов, ушедших с хоста полосы и на чужие хосты вместе;
        ``foreign_hosts`` — ``{host: n}`` только по чужим (своих среди них нет: иначе карта
        дублировала бы счётчик и читалась как «чужих много»). Ничего не отмечено →
        ``http_requests: None`` (не ноль) и пустая карта.
        """
        with self._lock:
            if not self._observed:
                return {"http_requests": None, "foreign_hosts": {}}
            foreign = {
                host: count
                for host, count in sorted(self._hosts.items())
                if self._lane_host is not None and host != self._lane_host
            }
            return {"http_requests": self._requests, "foreign_hosts": foreign}


@contextmanager
def attempt(lane_host: str | None = None) -> Iterator[HttpAudit]:
    """Открывает попытку фетча на хосте ``lane_host``; восстанавливает предыдущую на выходе."""
    audit = HttpAudit(lane_host)
    previous: HttpAudit | None = getattr(_local, "current", None)
    _local.current = audit
    try:
        yield audit
    finally:
        _local.current = previous


def current_attempt() -> HttpAudit | None:
    """Счётчик текущей попытки этого потока (``None`` — попытки нет, учёт не ведётся)."""
    audit: HttpAudit | None = getattr(_local, "current", None)
    return audit


def note_request(url: str) -> None:
    """Отметить запрос в текущей попытке; вне попытки — no-op (учёт не запрошен)."""
    audit = current_attempt()
    if audit is not None:
        audit.note(url)