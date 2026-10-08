"""Порт экстрактора — ``ExtractorPort`` (§3.2, Ш5).

HTML/PDF/Office → нормализованный markdown-текст. ``extractor_version`` фиксируется для
re-extract без ре-фетча (A/B экстракторов, retention). Санитайзинг и цитатная валидность —
на границе (02 §C.2). В профиле ``split`` экстракция недоверенного контента исполняется в DMZ.

Контракт: ``extract(page)`` принимает ``RawPage`` (формат выводится из url/content-type).
Реализация ``adapters/extract_document.py`` имеет ``extract_bytes(content, filename)`` —
обёртка приводится в T7.2. Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from core.ports.types import ExtractedDoc, RawPage


@runtime_checkable
class ExtractorPort(Protocol):
    """Нормализация сырого документа в текст/markdown с версией экстрактора."""

    version: str

    def extract(self, page: RawPage) -> ExtractedDoc: ...
