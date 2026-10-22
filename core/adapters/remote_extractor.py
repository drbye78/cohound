"""Remote-прокси экстрактора — ``RemoteExtractor`` (W13/T13.3; 07 §4).

Реализация ``ExtractorPort`` в ядре для профиля ``split``: разбор недоверенных
байтов (docling/trafilatura) исполняется в DMZ, ядро вызывает его через
``ContentGateway``. Контракт порта **не меняется** (07 §4).

07 §3 определяет ``POST /v1/fetch`` с осями ``fetch`` (http|browser) и
``extract`` (auto|html|document): edge выполняет fetch+extract+sanitize и возвращает
``text: {content, hash, extractor_version}``. Ядро **пересчитывает**
``content_hash`` очищенного текста (I19), не доверяя edge (07 §5).

**Семейство формата.** Публикуемый payload шва не несёт ``ExtractedDoc.family``
(расширять контракт ради поля чанкера не требуется): прокси выводит его тем же реестром,
которым руководствовался edge — ``textual_family(page.url, page.content_type)``. Правило
одно, поэтому решение о чанкере в ``split`` совпадает с ``local``; EPUB/HTML/docling
остаются ``None`` (markdown-совместимое семейство).
Нейтрален (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from core.adapters.remote_gateway import (
    RemoteContentError,
    extracted_doc_from_response,
)
from core.ports.gateway import ContentGateway
from core.ports.types import ExtractedDoc, ExtractionRoute, FetchMode, RawPage
from core.util.document_formats import textual_family

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

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        """``/v1/fetch`` → ``ExtractedDoc`` (повторный хэш, I19).

        Байты повторно не передаются: edge сам выполняет fetch по ``url``
        (форма протокола 07 §3). ``url``/``fetch``/``lang`` берутся из ``page``; маршрут
        извлечения — из аргумента ``route`` (резолвится в pipeline). ``wait_for`` не
        переносится: ``RawPage`` его не несёт.
        """
        fetch = FetchMode(page.fetch_mode)
        payload = {
            "url": page.url,
            "fetch": fetch.value,
            "lang": page.lang,
            "extract": route.value,
            "wait_for": None,
        }
        response = self._gateway.call("fetch", payload)
        if not isinstance(response, Mapping):
            raise RemoteContentError("fetch(extract): payload ответа edge не отображение")
        doc = extracted_doc_from_response(response, version=self.version)
        family = textual_family(page.url, page.content_type)
        if family is None or doc.error is not None:
            return doc
        return replace(doc, family=family)
