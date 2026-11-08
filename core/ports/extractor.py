"""Порт экстрактора — ``ExtractorPort`` (§3.2, Ш5).

HTML/PDF/Office → нормализованный markdown-текст. ``extractor_version`` фиксируется для
re-extract без ре-фетча (A/B экстракторов, retention). Санитайзинг и цитатная валидность —
на границе (02 §C.2). В профиле ``split`` экстракция недоверенного контента исполняется в DMZ.

Контракт: ``extract(page, route)`` принимает ``RawPage`` и маршрут извлечения
(``ExtractionRoute``: авто по формату | принудительный HTML | документ). Маршрут резолвится
в pipeline в составе ``FetchPlan``. Реализация ``adapters/extract_document.py`` имеет
``extract_bytes(content, filename)`` и ``extract_path(path, filename_hint=...)`` — обёртка
приводит в composition root. ``page.source_path`` (аддитивное поле ``RawPage``) позволяет
реализации читать retained-файл вместо ``content``: байты документа не обязаны проходить
через RAM вызывающего. Нейтрален (§1.3).
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from core.ports.types import ExtractedDoc, ExtractionRoute, RawPage


@runtime_checkable
class ExtractorPort(Protocol):
    """Нормализация сырого документа в текст/markdown с версией экстрактора."""

    version: str

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc: ...


@runtime_checkable
class DocumentExtractorPort(Protocol):
    """Документный адаптер (docling / ``text_layer``) — **второй** вход экстракции.

    Отдельный порт, а не ``ExtractorPort``, потому что это разные предметы:
    ``ExtractorPort.extract(page, route)`` — маршрутизация ``RawPage`` (её делает
    композит в composition root), а документный адаптер получает уже выбранный файл —
    путь retained-файла (``extract_path``) либо байты (``extract_bytes``). Прежде
    композит аннотировал документный адаптер как ``ExtractorPort``, и mypy честно
    возражал: ``ExtractorPort`` не имеет ни ``extract_path``, ни ``extract_bytes``.
    Аннотация была ложью об интерфейсе, а не «шумом типов».

    ``version`` — read-only: реализации выводят штамп из уровня/устройства/сборки
    движка (``docling-doc-3:<level>/<device>/…``, ``pdfium-text-2``), записывать его
    потребителю нечего.

    ``version_for(filename)`` намеренно **не** входит в порт: офисный и PDF-пути
    штампуются разными строками, и метод есть только у docling. Композит спрашивает
    его через ``getattr`` и падает обратно на ``version`` — отсутствие метода это
    «один штамп на весь адаптер», а не ошибка.
    """

    @property
    def version(self) -> str: ...

    def extract_bytes(
        self, content: bytes, filename: str = "document.pdf"
    ) -> ExtractedDoc: ...

    def extract_path(
        self, path: str | Path, *, filename_hint: str | None = None
    ) -> ExtractedDoc: ...
