"""Порт краулера — ``CrawlerPort`` (§3.2, Ш5).

Возвращает **сырые байты** (retention, I19). План получения (``FetchPlan``: HTTP/браузер и
JS-ожидание) резолвится в pipeline и передаётся сюда готовым — адаптер лишь исполняет его.
Robots-этика — ответственность адаптера (``adapters/crawl_scrapy.py``). В профиле ``split``
исполняется в DMZ; ядро использует remote-proxy (``ports/gateway.py``). Нейтрален (§1.3).

**Условный запрос (conditional fetch).** Если план несёт ``if_none_match``/
``if_modified_since`` (валидаторы предыдущего получения), адаптер обязан отправить их
заголовками ``If-None-Match``/``If-Modified-Since`` и **не** отдавать закэшированное тело
вместо проверки. Ответ ``304`` означает «закэшированное представление актуально»: адаптер
возвращает ``RawPage(not_modified=True, status_code=304, content=<байты кэша>)`` — тело
**никогда** не пустое и не сфабрикованное; отсутствие байтов в кэше — ошибка. При ``200``
адаптер возвращает новое тело и заполненные ``etag``/``last_modified``. Транспорт, не
видящий заголовков ответа (честный браузерный рендер), валидаторы не заявляет.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from core.ports.types import FetchPlan, RawPage


@runtime_checkable
class CrawlerPort(Protocol):
    """Загрузка страниц. ``plan.fetch`` выбирает HTTP или браузерный рендер (JS)."""

    version: str

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage: ...
