"""Порт опроса фида/канала — ``PollPort`` (Phase 5.1; §3.2, Ш5).

Опрос **публичной** ленты/канала документов даёт список целевых URL-кандидатов
(:class:`~core.util.feed.FeedEntry`). Это отдельный порт от :class:`CrawlerPort`
(``ports/crawler.py``): краулер отдаёт сырые байты страницы (retention, I19), а
поллер — уже разобранные ссылки ленты. Не смешивать: ``CrawlerPort`` —
``@runtime_checkable``, и добавление ``poll`` в него сломало бы ``isinstance``-проверки
адаптеров/раннера.

**Границы доверия.** Поллер лишь **производит кандидатов** — не evidence (I19). SSRF-проверка
(``core.util.security.validate_url``) выполняется на входе (URL ленты) и на каждой записи;
окончательная постановка в корпус и повторная проверка — на границе corpus enqueue (Ш5).
Секреты (токены доступа к каналу) передаются только через ``token_env`` (env/deploy), **не**
инлайном в ``params``.

**Профиль ``split``.** Реализация исполняется в DMZ; ядро вызывает её через
``ContentGateway`` (op ``crawl/poll``). Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from core.ports.types import FetchPlan
from core.util.feed import FeedEntry

__all__ = ["PollPort"]


@runtime_checkable
class PollPort(Protocol):
    """Опрос фида/канала: ``kind`` выбирает парсер, ``params`` — ограничения выборки.

    ``kind`` — ``"feed"`` (XML: RSS/Atom/RDF) либо ``"channel"`` (JSON/JSON-lines/список
    URL). ``params`` — ограничения (``limit``/``since``/``token_env``); валидируются
    :meth:`validate_params` **до** опроса. ``plan`` — оси получения ленты (HTTP/браузер),
    резолвятся в pipeline и передаются готовыми (как в :class:`~core.ports.crawler.CrawlerPort`).
    """

    version: str

    def poll(
        self,
        url: str,
        *,
        kind: str,
        params: Mapping[str, Any],
        plan: FetchPlan,
    ) -> list[FeedEntry]: ...

    def validate_params(self, kind: str, params: Mapping[str, Any]) -> list[str]: ...
