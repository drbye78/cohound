"""Markdown-чанкер по структурным заголовкам — реализация ``ChunkerPort``.

Делит документ на секции по заголовкам markdown, сохраняя иерархический путь
(``heading_path``) и ``context_prefix``. Соответствует §3.2/§5 регламента
(«чанкер с heading_path»), порт — ``core/ports/chunker.py`` (архитектура 01 §3.5).

Происхождение: адаптировано из проекта gag5 (`ingestion/structural_chunker.py`,
MIT License, Copyright (c) 2024) — см. THIRD_PARTY_NOTICES.md. Изменения: собственная
модель ``Chunk`` (chunk_no/heading_path/context_prefix/content_hash), детерминированные
id и hash, удалён хвост ``ingestion.chunker``, добавлена типизация. Нейтрален (§1.3).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from core.ports.types import Chunk
from core.ports.types import ExtractedDoc

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$")


@dataclass
class _Section:
    heading: str
    level: int
    content: str
    start_line: int
    end_line: int
    parent_headings: list[str] = field(default_factory=list)

    @property
    def heading_path(self) -> str:
        return " > ".join([*self.parent_headings, self.heading]) if self.heading else ""


def _sha256_16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


class MarkdownChunker:
    """Реализация ``ChunkerPort``: markdown → секционные чанки с heading_path."""

    version = "md-heading-1"

    def __init__(
        self,
        chunk_size: int = 1500,
        chunk_overlap: int = 200,
        min_chunk_size: int = 1,
    ) -> None:
        if chunk_overlap >= chunk_size:
            raise ValueError("chunk_overlap должен быть меньше chunk_size")
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size

    # -- public --------------------------------------------------------- #

    def chunk(self, document_id: str, text: str) -> list[Chunk]:
        sections = self._extract_sections(text)
        chunks: list[Chunk] = []
        for section in sections:
            path = section.heading_path
            if len(section.content) <= self.chunk_size:
                pieces = [section.content]
            else:
                pieces = self._split_with_overlap(section.content)
            for piece in pieces:
                piece = piece.strip()
                if len(piece) < self.min_chunk_size:
                    continue
                no = len(chunks)
                chunks.append(
                    Chunk(
                        chunk_id=self._chunk_id(document_id, no),
                        chunk_no=no,
                        text=piece,
                        heading_path=path,
                        context_prefix=path,
                        content_hash=_sha256_16(piece),
                    )
                )
        return chunks

    # -- internals ------------------------------------------------------ #

    @staticmethod
    def _chunk_id(document_id: str, no: int) -> str:
        return f"{document_id}#{no}"

    def _extract_sections(self, text: str) -> list[_Section]:
        lines = text.split("\n")
        sections: list[_Section] = []
        headings: list[str] = []
        buffer: list[str] = []
        start_line = 1

        def flush(end_line: int) -> None:
            if not buffer and not headings:
                return
            heading = headings[-1] if headings else ""
            sections.append(
                _Section(
                    heading=heading,
                    level=len(headings),
                    content="\n".join(buffer).strip(),
                    start_line=start_line,
                    end_line=end_line,
                    parent_headings=headings[:-1] if len(headings) > 1 else [],
                )
            )

        for line_no, line in enumerate(lines, 1):
            match = _HEADING_RE.match(line)
            if match:
                flush(line_no - 1)
                buffer = []
                level = len(match.group(1))
                title = match.group(2).strip()
                headings = headings[: level - 1]
                headings.append(title)
                start_line = line_no + 1
            else:
                buffer.append(line)
        flush(len(lines))

        if not sections:
            sections.append(
                _Section(
                    heading="", level=0, content=text.strip(),
                    start_line=1, end_line=len(lines),
                )
            )
        return sections

    def _split_with_overlap(self, content: str) -> list[str]:
        pieces: list[str] = []
        pos = 0
        length = len(content)
        while pos < length:
            end = min(pos + self.chunk_size, length)
            if end < length:
                newline = content.rfind("\n", pos, end)
                if newline > pos:
                    end = newline
            piece = content[pos:end].strip()
            if piece:
                pieces.append(piece)
            next_pos = end - self.chunk_overlap
            pos = next_pos if next_pos > pos else end
        return pieces


class MarkdownChunkerPort(MarkdownChunker):
    """Обёртка ``MarkdownChunker`` под контракт ``ChunkerPort`` (T7.3).

    Порт (``core/ports/chunker.py``) объявляет ``chunk(doc: ExtractedDoc, document_id)``,
    тогда как базовый :class:`MarkdownChunker` принимает ``chunk(document_id, text)``
    и используется в selfcheck. Обёртка переопределяет ``chunk`` под порт-сигнатуру и
    оставляет базовый метод доступным как :meth:`chunk_text` — так адаптер реализует
    ``ChunkerPort`` структурно, не ломая существующие вызовы.

    ``document_id`` — стабильный id документа (например, ``source_id``/``doc_id``);
    ``chunk_id`` детерминирован (``f"{document_id}#{no}"``), что сохраняет привязку
    чанка к сборке и идемпотентность (I19/S5).
    """

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:  # type: ignore[override]
        """Порт-путь: ``ExtractedDoc`` + ``document_id`` → секционные чанки с heading_path."""
        return self.chunk_text(document_id, doc.text)

    def chunk_text(self, document_id: str, text: str) -> list[Chunk]:
        """Чанкинг по markdown-тексту (сохранённый базовый вход)."""
        return super().chunk(document_id, text)


