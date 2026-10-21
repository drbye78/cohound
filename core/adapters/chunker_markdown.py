"""Markdown-чанкер по структурным заголовкам — реализация ``ChunkerPort``.

Делит документ на секции по заголовкам markdown, сохраняя иерархический путь
(``heading_path``) и ``context_prefix``. Соответствует §3.2/§5 регламента
(«чанкер с heading_path»), порт — ``core/ports/chunker.py`` (архитектура 01 §3.5).

Распознавание заголовков (``md-heading-2``):

* **ATX** — ``# … ######`` в начале строки (как раньше);
* **fence-aware** — внутри ограждённого блока кода (````` ``` `````/``~~~``, маркер
  длиной ≥3) строки вида ``# comment`` заголовками **не** считаются, поэтому пример
  кода не обрезает стек заголовков и не подменяет ``heading_path``. Закрывает блок
  только тот же символ не короче открывающего и без info-string (CommonMark);
  незакрытый до конца документа fence — не ошибка, остаток остаётся кодом;
* **setext** — ``Title`` + строка-подчёркивание только из ``=`` (уровень 1) или
  ``-`` (уровень 2). Консервативно: подчёркивание срабатывает лишь под строкой,
  которая является обычным текстом абзаца. Не срабатывает под пустой строкой
  (``---`` как тематический разрыв), заголовком, fence, элементом списка/цитаты,
  строкой с отступом ≥4 пробелов, HTML/таблицей, тематическим разрывом или другим
  подчёркиванием. Многострочный абзац перед подчёркиванием **не** склеивается:
  заголовком становится только непосредственно предшествующая строка (ложный
  заголовок хуже пропущенного).
* **YAML-front-matter** — блок ``---`` … ``---``/``...`` в самом начале документа
  (если тело содержит ``:``) пропускается без распознавания заголовков: иначе
  ``# yaml comment`` стал бы ATX-заголовком, а закрывающий ``---`` — setext-заголовком.
  Без тела-отображения документ трактуется по CommonMark (``---`` — разрыв/setext).

Происхождение: адаптировано из проекта gag5 (`ingestion/structural_chunker.py`,
MIT License, Copyright (c) 2024). Изменения: собственная
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
# Открывающий fence: до 3 пробелов отступа, маркер ` или ~ длиной ≥3, info-string.
_FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
# Setext-подчёркивание: только `=` или только `-` (плюс хвостовые пробелы).
_SETEXT_RE = re.compile(r"^ {0,3}(=+|-+)[ \t]*$")
# Тематический разрыв (`---`, `***`, `___`) — не текст и не setext-заголовок.
_THEMATIC_BREAK_RE = re.compile(
    r"^ {0,3}(?:(?:\*[ \t]*){3,}|(?:-[ \t]*){3,}|(?:_[ \t]*){3,})$"
)
# Строки, которые не являются обычным текстом абзаца (консервативный отказ setext):
# отступ кода, цитата, список, HTML-блок, строка таблицы.
_NON_PARAGRAPH_RE = re.compile(r"^(?: {4,}|\t|>|[-*+][ \t]|\d{1,9}[.)][ \t]|<|\|)")


def _fence_open(line: str) -> tuple[str, int] | None:
    """Возвращает ``(символ, длина)`` открывающего fence либо ``None``.

    Info-string backtick-фенса не может содержать обратную кавычку (CommonMark) —
    такая строка трактуется как обычный текст (консервативно: лучше не открыть блок,
    чем ошибочно спрятать в нём заголовки).
    """
    match = _FENCE_RE.match(line)
    if not match:
        return None
    marker = match.group(1)
    if marker[0] == "`" and "`" in match.group(2):
        return None
    return marker[0], len(marker)


def _fence_close(line: str, char: str, length: int) -> bool:
    """Закрывает ли строка открытый fence: тот же символ, длина ≥, без info-string."""
    match = _FENCE_RE.match(line)
    if not match:
        return False
    marker = match.group(1)
    return marker[0] == char and len(marker) >= length and not match.group(2).strip()


def _is_setext_text(line: str) -> bool:
    """Может ли строка быть текстом setext-заголовка (консервативно)."""
    if not line.strip():
        return False
    if _HEADING_RE.match(line) or _SETEXT_RE.match(line):
        return False
    if _FENCE_RE.match(line) or _THEMATIC_BREAK_RE.match(line):
        return False
    return not _NON_PARAGRAPH_RE.match(line)


def _front_matter_lines(lines: list[str]) -> int:
    """Сколько строк в начале документа занимает YAML-front-matter (0 — нет блока).

    Консервативно: блок распознаётся только если первая строка — ровно ``---``,
    ниже есть закрывающий ``---``/``...`` **и** тело блока содержит YAML-отображение
    (``:``). Иначе документ трактуется как обычный markdown — это сохраняет
    CommonMark-поведение ``---`` (тематический разрыв / setext). Внутри front-matter
    заголовки не распознаются: иначе ``# yaml comment`` стал бы ATX-заголовком,
    а закрывающий ``---`` — setext-заголовком.
    """
    if not lines or lines[0].strip() != "---":
        return 0
    for index in range(1, len(lines)):
        if lines[index].strip() in ("---", "..."):
            if any(":" in line for line in lines[1:index]):
                return index + 1
            return 0
    return 0


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
    """Реализация ``ChunkerPort``: markdown → секционные чанки с heading_path.

    ``version`` входит в ``partition_build_id``/ключи кэша и в
    ``execution_contract_digest``: смена распознавания заголовков (fence-aware +
    setext) меняет chunk-set, поэтому версия поднята ``md-heading-1`` → ``md-heading-2``.
    """

    version = "md-heading-2"

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
        fence: tuple[str, int] | None = None
        front_matter = _front_matter_lines(lines)

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

        def open_heading(title: str, level: int, line_no: int) -> None:
            """Сбрасывает стек заголовков под ``level`` и начинает новую секцию."""
            nonlocal buffer, headings, start_line
            buffer = []
            headings = headings[: level - 1]
            headings.append(title)
            start_line = line_no + 1

        for line_no, line in enumerate(lines, 1):
            if line_no <= front_matter:
                buffer.append(line)
                continue

            if fence is not None:
                # Внутри блока кода: заголовки не распознаются, строки не теряются.
                if _fence_close(line, *fence):
                    fence = None
                buffer.append(line)
                continue

            opened = _fence_open(line)
            if opened is not None:
                fence = opened
                buffer.append(line)
                continue

            match = _HEADING_RE.match(line)
            if match:
                flush(line_no - 1)
                open_heading(match.group(2).strip(), len(match.group(1)), line_no)
                continue

            setext = _SETEXT_RE.match(line)
            if setext and buffer and _is_setext_text(buffer[-1]):
                # Строка-текст становится заголовком; контент секции кончается перед ней.
                title = buffer.pop().strip()
                flush(line_no - 2)
                level = 1 if setext.group(1)[0] == "=" else 2
                open_heading(title, level, line_no)
                continue

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


