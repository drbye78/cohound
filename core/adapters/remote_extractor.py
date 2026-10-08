"""Remote-прокси экстрактора — ``RemoteExtractor`` (W13/T13.3; 07 §4).

Реализация ``ExtractorPort`` в ядре для профиля ``split``: разбор недоверенных
байтов (docling/trafilatura) исполняется в DMZ, ядро вызывает его через
``ContentGateway``. Контракт порта **не меняется** (07 §4).

07 §3 определяет ``POST /v1/fetch`` с флагом ``extract: bool``: при
``extract=True`` edge выполняет fetch+extract+sanitize и возвращает
``text: {content, hash, extractor_version}``. Ядро **пересчитывает**
``content_hash`` очищенного текста (I19), не доверяя edge (07 §5).
Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping

from core.adapters.remote_gateway import (
    RemoteContentError,
    extracted_doc_from_response,
)
from core.ports.gateway import ContentGateway
from core.ports.types import ExtractedDoc, RawPage, RenderMode

__all__ = ["RemoteExtractor"]


class RemoteExtractor:
    """``ExtractorPort`` через DMZ-шов. Drop-in для реального экстрактора (07 §4)."""

    def __init__(self, gateway: ContentGateway, *, version: str = "remote-extractor-1") -> None:
        self._gateway = gateway
        self.version = version

    @property
    def available(self) -> bool:
        """Доступен ли транспорт шва (``httpx`` или внедрённый ``transport``)."""
        return bool(getattr(self._gateway, "available", True))

    def extract(self, page: RawPage) -> ExtractedDoc:
        """``/v1/fetch`` с ``extract=True`` → ``ExtractedDoc`` (повторный хэш, I19).

        Байты повторно не передаются: edge сам выполняет fetch по ``url``
        (форма протокола 07 §3). ``url``/``render``/``lang`` берутся из ``page``.
        """
        render = RenderMode(page.render_mode)
        payload = {
            "url": page.url,
            "render": render.value,
            "lang": page.lang,
            "extract": True,
        }
        response = self._gateway.call("fetch", payload)
        if not isinstance(response, Mapping):
            raise RemoteContentError("fetch(extract): payload ответа edge не отображение")
        return extracted_doc_from_response(response, version=self.version)
