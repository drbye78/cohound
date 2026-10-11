"""Remote-прокси краулера — ``RemoteCrawler`` (W13/T13.3; 07 §4).

Реализация ``CrawlerPort`` в ядре для профиля ``split``: реальный Scrapy-адаптер
исполняется в DMZ, а ядро вызывает его через ``ContentGateway``. Контракт порта
**не меняется** (07 §4) — прокси drop-in для ``adapters/crawl_scrapy.py``.

I19: edge возвращает сырые байты (``raw.bytes_b64``/``raw.content``), ядро
декодирует и **пересчитывает** ``content_hash`` (при наличии заявленного),
не доверяя edge (07 §5). Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping

from core.adapters.remote_gateway import (
    RemoteContentError,
    raw_page_from_response,
)
from core.ports.gateway import ContentGateway
from core.ports.types import FetchPlan, RawPage

__all__ = ["RemoteCrawler"]


class RemoteCrawler:
    """``CrawlerPort`` через DMZ-шов. Drop-in для реального краулера (07 §4)."""

    #: Браузерный рендер исполняется на edge (DMZ) — прокси поддерживает ``fetch: browser``.
    supports_browser = True

    def __init__(self, gateway: ContentGateway, *, version: str = "remote-crawler-1") -> None:
        self._gateway = gateway
        self.version = version

    @property
    def available(self) -> bool:
        """Доступен ли транспорт шва (``httpx`` или внедрённый ``transport``)."""
        return bool(getattr(self._gateway, "available", True))

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        """``/v1/fetch`` → ``RawPage`` (сырые байты, retention).

        План сериализуется целиком (``fetch``/``extract``/``wait_for``): edge исполняет
        обе оси, а прокси потребляет сырые байты. Нейтрален (§1.3).
        """
        payload = {
            "url": url,
            "fetch": plan.fetch.value,
            "lang": None,
            "extract": plan.extract.value,
            "wait_for": plan.wait_for,
        }
        response = self._gateway.call("fetch", payload)
        if not isinstance(response, Mapping):
            raise RemoteContentError("fetch: payload ответа edge не является отображением")
        return raw_page_from_response(response, url=url, fetch=plan.fetch)
