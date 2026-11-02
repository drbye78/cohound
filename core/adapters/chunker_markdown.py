"""Markdown-чанкер по структурным заголовкам — реализация ``ChunkerPort``.

Делит документ на секции по заголовкам markdown, сохраняя иерархический путь
(``heading_path``) и ``context_prefix``. Соответствует §3.2/§5 регламента
(«чанкер с heading_path»), порт — ``core/ports/chunker.py`` (архитектура 01 §3.5).

Распознавание заголовков (``md-heading-4``):

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
* **относительные уровни** — уровни ATX трактуются от самого мелкого уровня заголовка,
  реально присутствующего в документе (``root_level``; setext вносит свои 1/2), а не
  абсолютно: документ без ``#`` не получает вложенность из ничего. Внешние извлекатели
  отдают ``##`` как верхний уровень (trafilatura — если первый заголовок страницы был h2;
  docling — для DOCX/XLSX), и абсолютная трактовка превращала соседние ``##`` в родителя
  и ребёнка. Документ, начинающийся с ``#``, режется байт-в-байт как раньше.

**Границы куска.** Кусок кончается на границе строки, иначе слова
(в обоих случаях — только во второй половине окна, чтобы перевод строки у самого начала
не ужимал кусок почти до нуля), и только при отсутствии границы (длинный токен без
пробелов) — жёстким разрезом по ``chunk_size``. Начало следующего куска — ``end -
chunk_overlap`` — сдвигается **вперёд** к границе слова: кусок не начинается обрывком
слова. Отсюда следствие, которое важно не путать с обещанием: фактическое перекрытие
**≤** ``chunk_overlap`` (снап двигает начало вперёд, ``strip`` снимает краевые пробелы),
«ровно 200» не гарантируется никогда — 200 это верхняя граница, а не измерение.

**Порог фрагмента.** Кусок короче :data:`MIN_CHUNK_SIZE` в индекс не попадает: иначе
односимвольный остаток таблицы мог бы стать ``source_id`` цитаты. Число — из замера, не
«на глаз» (см. константу).

Происхождение: адаптировано из проекта gag5 (`ingestion/structural_chunker.py`,
MIT License, Copyright (c) 2024). Изменения: собственная
модель ``Chunk`` (chunk_no/heading_path/context_prefix/content_hash), детерминированные
id и hash, удалён хвост ``ingestion.chunker``, добавлена типизация. Нейтрален (§1.3).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from core.ports.types import Chunk, ExtractedDoc

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

#: Минимальная длина чанка (символы): кусок короче в индекс не попадает.
#:
#: Замер на реальном корпусе (``runs/ru-pg-dbms-3edf2b06097c/cohound.db``, 9398 чанков,
#: ``md-heading-2``, дефолтная геометрия 1500/200): распределение длин p1=36, p5=110,
#: p50=725, p90=1434; чанков короче 20 символов — 71 (0.76 %), и это почти исключительно
#: обломки разметки: ``|``, номера страниц (``203``), колонтитулы (``Листов 81``),
#: заглушки (``<!-- image -->``, 14). «Фразовых» (≥3 слова длиной ≥3) среди них 1 из 71;
#: в следующей полосе 20–40 символов — уже 27 из 35 (``Версия для работы в Docker``,
#: ``apt-get remove jatoba18-jcs``). Разрыв на 20 символах и есть граница «мусор ↔
#: короткая настоящая строка», поэтому порог стоит именно там: выше него начинают
#: теряться настоящие однострочные утверждения. Доказательств порог не отнимает:
#: в том же прогоне кратчайший чанк, ставший evidence, — 178 символов (в чанки корпуса
#: резолвятся 130 из 187 evidence-строк; остальные — на уровне URL), и ни один документ
#: не теряет все свои чанки (0 из 56 — и при 20, и при 40 символах).
#:
#: Порог — антифрагментная защита, а не ось геометрии: он задаётся для дефолтного
#: ``chunk_size`` и **не** масштабируется сам. При малой ``chunk_size`` его надо объявлять
#: явно, иначе он выбросит целые короткие куски (``min_chunk_size < chunk_size`` —
#: проверяется в конструкторе).
MIN_CHUNK_SIZE = 20


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

    Границы куска и порог фрагмента меняют нарезку так же, как распознавание заголовков,
    поэтому их правка — версионная граница ``execution_versions`` (§13): версию поднимает
    владелец правки вместе с пере-прогоном, а не адаптер «на месте». Пока версия не
    поднята, ``chunker_version`` описывает прежнюю нарезку — это известно и объявлено.
    """

    #: ``md-heading-4`` (было ``md-heading-3``): ATX-уровни трактуются **относительно**
    #: самого мелкого уровня документа (``root_level``), а не абсолютно. Меняются
    #: ``heading_path`` и границы секций ⇒ другой chunk-set (``context_prefix`` тоже),
    #: то есть условия измерения: без подъёма версии ``chunker_version`` описывал бы
    #: прежнюю нарезку — ложь в контракте. Класс — ``execution_versions`` (§13).
    #: ``md-heading-3`` (было ``md-heading-2``) — граница куска и порог фрагмента.
    version = "md-heading-4"

    def __init__(
        self,
        chunk_size: int = 1500,
        chunk_overlap: int = 200,
        min_chunk_size: int = MIN_CHUNK_SIZE,
    ) -> None:
        if chunk_overlap < 0:
            raise ValueError("chunk_overlap не может быть отрицательным")
        if chunk_overlap >= chunk_size:
            raise ValueError("chunk_overlap должен быть меньше chunk_size")
        if min_chunk_size < 0:
            raise ValueError("min_chunk_size не может быть отрицательным")
        if min_chunk_size >= chunk_size:
            raise ValueError("min_chunk_size должен быть меньше chunk_size")
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
        # Самый мелкий уровень заголовка (ATX или setext), встреченного **до** текущего
        # (``None`` — ни одного). Уровни трактуются относительно него, а не абсолютно:
        # документ, у которого нет ``#``, не получает вложенность из ничего
        # (см. ``open_heading``).
        root_level: int | None = None

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
            """Сбрасывает стек заголовков под **эффективный** уровень и открывает секцию.

            **Уровень относителен самого мелкого заголовка документа** (``root_level``).
            Абсолютный уровень markdown (``#`` = 1, ``##`` = 2…) описывает разметку
            источника, а не структуру документа: trafilatura отдаёт ``##``, если первый
            заголовок страницы — h2, docling — ``##`` для заголовков DOCX/XLSX, и в обоих
            случаях ``#`` в тексте может отсутствовать вовсе. Абсолютная трактовка давала
            ложную вложенность: два соседних ``##`` (``РАЗДЕЛ 1`` / ``РАЗДЕЛ 2``) давали
            путь ``РАЗДЕЛ 1 > РАЗДЕЛ 2``, то есть второй раздел объявлялся частью первого,
            а лист книги — частью предыдущего листа. ``heading_path`` — привязка цитаты к
            разделу (evidence), поэтому это была ложь в провенансе, а не косметика.

            ``root_level`` — минимум уровней встреченных заголовков (setext вносит свои
            1/2, как и раньше). Порядок уровней при вычитании константы сохраняется,
            поэтому документ, начинающийся с ``#``, режется как прежде.
            """
            nonlocal buffer, headings, start_line, root_level
            if root_level is None or level < root_level:
                # Заголовок мельче текущего корня понижает корень: новые заголовки
                # считаются от него. Уже выданные секции не переписываются — текст,
                # попавший в корпус, не меняется задним числом.
                root_level = level
            depth = max(1, level - root_level + 1)
            buffer = []
            headings = headings[: depth - 1]
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

    def _cut_point(self, content: str, pos: int, end: int) -> int:
        """Конец куска: граница строки, иначе любого пробела, иначе жёсткий разрез.

        Граница ищется только во **второй половине** окна: иначе перевод строки у самого
        ``pos`` ужал бы кусок почти до нуля (``newline > pos`` пропускал ``pos + 1``), и
        «перекрытие» перестало бы быть перекрытием.
        """
        floor = pos + max(self.chunk_size // 2, 1)
        newline = content.rfind("\n", floor, end)
        if newline > pos:
            return newline
        for index in range(end - 1, floor - 1, -1):
            if content[index].isspace():
                return index
        return end  # границы нет (длинный неразрывный токен) — режем жёстко

    @staticmethod
    def _start_point(content: str, pos: int, end: int) -> int:
        """Начало следующего куска — на границе слова (слово не рвётся).

        ``pos`` — это ``end - chunk_overlap``, произвольная позиция внутри слова.
        Сдвиг вперёд к первому пробельному символу (пробел, перевод строки, табуляция)
        делает фактическое перекрытие ≤ ``chunk_overlap`` и никогда не начинает кусок
        с обрывка слова; начало строки — тоже граница слова. Если границы до ``end``
        нет — остаётся жёсткий разрез (иначе кусок стал бы пустым, а цикл не сдвинулся).
        """
        if pos <= 0:
            return pos
        if content[pos - 1].isspace() or content[pos].isspace():
            return pos
        for index in range(pos, end):
            if content[index].isspace():
                return index + 1
        return pos

    def _split_with_overlap(self, content: str) -> list[str]:
        """Режет секцию на куски ≤ ``chunk_size`` с перекрытием и границами слов.

        Инварианты: длина куска ≤ ``chunk_size``; кусок не начинается и не кончается
        обрывком слова, кроме случая длинного токена без пробелов/переводов строк;
        перекрытие соседних кусков ≤ ``chunk_overlap`` (см. докстринг модуля).
        """
        pieces: list[str] = []
        pos = 0
        length = len(content)
        while pos < length:
            end = min(pos + self.chunk_size, length)
            if end < length:
                end = self._cut_point(content, pos, end)
            piece = content[pos:end].strip()
            if piece:
                pieces.append(piece)
            next_pos = end - self.chunk_overlap
            if next_pos <= pos or next_pos >= end:
                pos = end  # перекрытия не осталось — идём строго вперёд
            else:
                pos = self._start_point(content, next_pos, end)
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


