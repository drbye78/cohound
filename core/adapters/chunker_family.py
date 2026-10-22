"""Диспетчер чанкеров по семейству формата — композит ``ChunkerPort``.

Текстовые форматы — первый класс (решение владельца): у plain text своя единица смысла
(абзац), у markdown/HTML/EPUB — заголовок. Поэтому в composition root строится **один**
порт-чанкер, который выбирает реализацию по ``ExtractedDoc.family``:

* ``"text"`` → :class:`core.adapters.chunker_text.TextChunker` (абзацы/предложения);
* ``"markdown"``/``"epub"``/``None`` → :class:`core.adapters.chunker_markdown.MarkdownChunker`
  (заголовки; ``None`` — HTML и docling, оба отдают markdown).

**``chunker_version`` — композит, а не версия одного чанкера.** Поле
``chunker_version`` входит в ``EXECUTION_CONTRACT_FIELDS``
(:mod:`core.pipeline.digests`) и в ``partition_build_id``/``index_build_id``: с появлением
второго чанкера набор чанков определяется **парой** правил, поэтому объявленная версия —
``"md-heading-2+text-para-1"`` (:func:`composite_chunker_version`). Это версионная
граница класса ``execution_versions`` (``execution_change``, §13): меняется ``run_id``,
методология (``METHODOLOGY_VERSION``) — нет.

Нейтрален (§1.3).
"""

from __future__ import annotations

from core.adapters.chunker_markdown import MarkdownChunker, MarkdownChunkerPort
from core.adapters.chunker_text import TextChunker, TextChunkerPort
from core.ports.types import Chunk, ExtractedDoc
from core.util.document_formats import FAMILY_TEXT

__all__ = ["FamilyChunker", "composite_chunker_version"]


def composite_chunker_version() -> str:
    """Объявленная ``chunker_version``: версия markdown-чанкера + версия текстового.

    Единая формула для декларации (:mod:`core.cli.execution_axes`) и для самого композита
    (:attr:`FamilyChunker.version`): два разных источника разошлись бы с фактической
    нарезкой и сломали бы воспроизводимость контракта (R7 §11.7).
    """
    return f"{MarkdownChunker.version}+{TextChunker.version}"


class FamilyChunker:
    """``ChunkerPort``-композит: семейство ``text`` → абзацный чанкер, иначе markdown.

    Реализует порт структурно. ``markdown``/``text`` — точки подмены (тесты, AB); по
    умолчанию собираются порт-обёртки, поэтому сигнатура ``chunk(doc, document_id)``
    одна.
    """

    def __init__(
        self,
        markdown: MarkdownChunkerPort | None = None,
        text: TextChunkerPort | None = None,
    ) -> None:
        self._markdown = markdown if markdown is not None else MarkdownChunkerPort()
        self._text = text if text is not None else TextChunkerPort()

    @property
    def version(self) -> str:
        """Композитная версия правил нарезки (:func:`composite_chunker_version`)."""
        return composite_chunker_version()

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        """``ExtractedDoc`` → чанки тем чанкером, чьё семейство указал экстрактор."""
        chunker = (
            self._text
            if getattr(doc, "family", None) == FAMILY_TEXT
            else self._markdown
        )
        return chunker.chunk(doc, document_id)
