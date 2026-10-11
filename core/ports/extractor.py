"""Порт экстрактора — ``ExtractorPort`` (§3.2, Ш5).

HTML/PDF/Office → нормализованный markdown-текст. ``extractor_version`` фиксируется для
re-extract без ре-фетча (A/B экстракторов, retention). Санитайзинг и цитатная валидность —
на границе (02 §C.2). В профиле ``split`` экстракция недоверенного контента исполняется в DMZ.

Контракт: ``extract(page, route)`` принимает ``RawPage`` и маршрут извлечения
(``ExtractionRoute``: авто по формату | принудительный HTML | документ). Маршрут резолвится
в pipeline в составе ``FetchPlan``. Реализация ``adapters/extract_document.py`` имеет
``extract_bytes(content, filename)`` — обёртка приводит в composition root. Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from core.ports.types import ExtractedDoc, ExtractionRoute, RawPage


@runtime_checkable
class ExtractorPort(Protocol):
    """Нормализация сырого документа в текст/markdown с версией экстрактора."""

    version: str

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc: ...
