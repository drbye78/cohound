"""Markdown-чанкер по структуре документа — реализация ``ChunkerPort``.

Делит документ на куски **по разобранной структуре** (``core.adapters.md_ast``), сохраняя
``heading_path`` и ``context_prefix``. Соответствует §3.2/§5 регламента («чанкер с
heading_path»), порт — ``core/ports/chunker.py`` (архитектура 01 §3.5).

Версия ``md-heading-5`` — переход с построчного конечного автомата на AST. Что изменилось
и почему (замеры и разбор — ``docs/architecture/18-markdown-chunker-plan.md``):

* **Куски — точные срезы источника.** ``chunk.text`` больше не ``strip()``: конкатенация
  текстов всех чанков равна нормализованному входу байт-в-байт (P1). Иначе нельзя ни
  проверить, что ничего не потеряно, ни гарантировать I22 (цитата — подстрока текста).
* **Перекрытия нет** (``chunk_overlap = 0``). Перекрытие несовместимо с P1 и удваивало
  evidence-шум: соседние чанки делили абзац и шапку таблицы.
* **Строка заголовка входит в текст своего чанка.** Раньше она жила только в
  ``heading_path`` и из ``text`` пропадала — тоже нарушение P1.
* **Границы — по конструкции, а не по номеру символа.** Минимальная единица (строка
  таблицы, элемент списка, строка кода, fence, blockquote, сноска) не рвётся; внутри
  абзаца рез допускается только в позициях, посчитанных сканером по сырому срезу
  (код-спаны, ссылки, inline-html, формулы, графемы — ``md_ast.safe_cut_offsets``).
* **Продолжение несёт контекст.** Чанк, начавшийся внутри таблицы, получает шапку с
  разделителем и выравниванием, continuation fence — открывающую строку с info string
  (P9/P10); промпт печатает ``context``, когда он отличается от ``heading_path``.
* **Относительные уровни сохранены.** ATX-уровни трактуются от самого мелкого уровня,
  реально присутствующего в документе: trafilatura отдаёт ``##``, если первый заголовок
  страницы был h2, docling — ``##`` для DOCX/XLSX, и абсолютная трактовка превращала
  соседние ``##`` в родителя и ребёнка. Документ, начинающийся с ``#``, режется как
  прежде: ``heading_path`` — привязка цитаты к разделу, поэтому ложная вложенность была
  бы ложью в провенансе.

**Профиль диалекта — свойство источника.** ``profile`` объявлен в конструкторе и входит
в ``chunker_version``: ``gfm`` для страниц и офисных документов (trafilatura/docling не
пишут Pandoc-разметку), ``pandoc`` — только там, где диалект заявлен, потому что
``dollarmath`` в профиле страниц читал бы ``$$`` как формулу, тогда как в документации это
dollar-quoting SQL.

**Порог фрагмента (C6) больше не выбрасывает текст.** Кусок короче
:data:`MIN_CHUNK_SIZE` — это либо хвост разреза (приклеивается к предыдущему куску той же
секции), либо самостоятельно объявленная единица документа (однострочная секция). Второе
остаётся чанком: P1 запрещает терять символы, а «мелкий мусор в индексе» — гипотеза,
которую надо измерить, а не вычеркнуть текст. Число таких чанков публикует
``scripts/measure_md_chunking.py``.

Происхождение: адаптировано из проекта gag5 (`ingestion/structural_chunker.py`,
MIT License, Copyright (c) 2024). Изменения: собственная модель ``Chunk``
(chunk_no/heading_path/context_prefix/content_hash), детерминированные id и hash, удалён
хвост ``ingestion.chunker``, добавлена типизация. Нейтрален (§1.3).
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _dist_version

from core.adapters.md_ast import (
    LineIndex,
    MdBlock,
    MdDocument,
    is_cut_boundary,
    parse_markdown,
    safe_cut_offsets,
)
from core.ports.types import Chunk, ExtractedDoc
from core.util.text_utils import SENTENCE_END_CHARS

#: Атрибуты заголовка Pandoc (``{#id .cls}``): в ``heading_path`` не входят (P11).
_HEADING_ATTRS_RE = re.compile(r"\s*\{(?:[#.][^{}]*)\}\s*$")

#: Минимальная длина куска (символы). Порог **не выбрасывает** текст: хвост разреза
#: приклеивается к предыдущему куску той же секции, а объявленная единица документа
#: (однострочная секция) остаётся чанком — иначе ломается P1.
#:
#: Число 20 — из замера ``md-heading-2`` (9398 чанков): мусор (``|``, номера страниц,
#: колонтитулы, ``<!-- image -->``) лежит ниже 20 символов, а настоящие однострочные
#: утверждения начинаются сразу выше. При ``md-heading-5`` порог действует только на
#: хвосты разреза, поэтому от него больше не зависит, попадёт ли короткая секция в индекс.
MIN_CHUNK_SIZE = 20

#: Версия спецификации CommonMark, которую реализует движок разбора.
COMMONMARK_SPEC_VERSION = "commonmark-0.31.2"


def _engine_version() -> str:
    """Версия движка грамматики **из метаданных пакета**, а не из константы.

    Обновление зависимости меняет правила разбора, поэтому ``chunker_version`` обязан это
    показать: иначе два прогона с разными движками получили бы один ``run_id``. Это тот же
    класс, что ``extractor_stack`` (AGENTS §9): версия библиотеки — часть условий измерения.
    Неизвестная версия пишется как ``unknown``, а не выдумывается.
    """

    def one(name: str) -> str:
        try:
            return _dist_version(name)
        except PackageNotFoundError:
            return "unknown"

    return f"mdit-{one('markdown-it-py')}/plugins-{one('mdit-py-plugins')}"


@dataclass(frozen=True, slots=True)
class _Section:
    """Секция документа: непрерывный диапазон символов и её ``heading_path``."""

    start: int
    end: int
    heading_path: str


@dataclass(frozen=True, slots=True)
class _Cut:
    """Разрешённая позиция разреза: смещение, структурность, контекст продолжения.

    ``structural`` различает два класса границ. Структурная граница — между блоками,
    строками таблицы, элементами списка, детьми контейнера: она не рвёт минимальную
    единицу (P2). Внутренняя граница — разрез внутри абзаца или строки кода: допустим
    только когда структурной границы в окне нет, то есть конструкция сама длиннее
    бюджета (§3 плана: единственный разрешённый компромисс).

    ``sentence`` — разрез сразу после конца предложения: внутри абзаца такие позиции
    предпочтительнее любого пробела, иначе чанк кончается посреди фразы и цитата
    обрывается на полуслове.
    """

    offset: int
    structural: bool
    context: str | None = None
    sentence: bool = False


def _sha256_16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


class MarkdownChunker:
    """Реализация ``ChunkerPort``: markdown → структурные чанки с ``heading_path``.

    ``version`` входит в ``partition_build_id``/ключи кэша и в
    ``execution_contract_digest``: смена правил нарезки меняет chunk-set, поэтому версия
    поднята ``md-heading-4`` → ``md-heading-5`` (класс ``execution_versions``, §13).
    """

    #: Версия правил нарезки — ** стек**, а не метка файла: грамматика, движок разбора и
    #: политика нарезки меняются независимо, и ``run_id`` обязан различить любое из трёх.
    #: ``md-heading-4`` → ``md-heading-5`` был переходом с построчного автомата на AST
    #: (другой chunk-set: другие границы, другой ``text``, другой ``context_prefix``).
    #: Какой именно профиль грамматики применён — объявляет ``chunking_spec``
    #: (``md_ast.PROFILES``; страницы и офисные документы идут в ``gfm``).
    version = f"md-ast-1:{COMMONMARK_SPEC_VERSION}+gfm+pandoc/{_engine_version()}/policy-1"

    #: Объявленный (и снятый) размер перекрытия: контракт продолжает его публиковать
    #: (:func:`core.adapters.chunker_family.composite_chunking_spec`), а значение 0 —
    #: само решение, а не забытый параметр.
    chunk_overlap = 0

    def __init__(
        self,
        chunk_size: int = 1500,
        min_chunk_size: int = MIN_CHUNK_SIZE,
        profile: str = "gfm",
    ) -> None:
        if min_chunk_size < 0:
            raise ValueError("min_chunk_size не может быть отрицательным")
        if min_chunk_size >= chunk_size:
            raise ValueError("min_chunk_size должен быть меньше chunk_size")
        self.chunk_size = chunk_size
        self.min_chunk_size = min_chunk_size
        self.profile = profile

    # -- public --------------------------------------------------------- #

    def chunk(self, document_id: str, text: str) -> list[Chunk]:
        """Режет markdown на структурные куски; ``text`` куска — точный срез (P1).

        Инвариант проверяем: ``"".join(c.text for c in chunks)`` равно
        ``parse_markdown(text).text`` (нормализованному входу). Документ без единого
        непробельного символа даёт ноль чанков — терять в нём нечего.
        """
        document = parse_markdown(text, self.profile)
        index = LineIndex(document.text)
        chunks: list[Chunk] = []
        for section in self._sections(document):
            for start, end, context in self._pieces(document, section, index):
                body = document.text[start:end]
                if not body.strip():
                    continue
                no = len(chunks)
                chunks.append(
                    Chunk(
                        chunk_id=self._chunk_id(document_id, no),
                        chunk_no=no,
                        text=body,
                        heading_path=section.heading_path,
                        context_prefix=self._context_prefix(section.heading_path, context),
                        content_hash=_sha256_16(body),
                    )
                )
        return chunks

    # -- секции --------------------------------------------------------- #

    def _sections(self, document: MdDocument) -> list[_Section]:
        """Режет документ на секции по заголовкам верхнего уровня.

        Уровни относительны самого мелкого встреченного заголовка (``root_level``), как
        в ``md-heading-4``: уже выданные секции не переписываются задним числом, поэтому
        текст, попавший в корпус, не меняется от того, что дальше встретился ``#``.
        """
        length = len(document.text)
        sections: list[_Section] = []
        stack: list[tuple[int, str]] = []
        root_level: int | None = None
        start = 0
        path = ""

        for block in document.blocks:
            if block.kind != "heading":
                continue
            if block.span.start_char > start:
                sections.append(_Section(start, block.span.start_char, path))
            level = block.level or 1
            if root_level is None or level < root_level:
                root_level = level
            depth = max(1, level - root_level + 1)
            stack = stack[: depth - 1]
            stack.append((level, _heading_title(block)))
            path = " > ".join(title for _, title in stack if title)
            start = block.span.start_char

        if start < length:
            sections.append(_Section(start, length, path))
        return [section for section in sections if section.end > section.start]

    # -- границы разреза ------------------------------------------------ #

    def _pieces(
        self, document: MdDocument, section: _Section, index: LineIndex
    ) -> list[tuple[int, int, str | None]]:
        """Куски секции: ``(начало, конец, контекст продолжения)`` в символах.

        Куски покрывают секцию без пропусков и наложений, поэтому конкатенация текстов
        равна секции дословно (P1). Конец куска — самая крупная разрешённая граница,
        помещающаяся в бюджет; если границ нет вовсе (конструкция длиннее бюджета),
        остаётся жёсткий разрез — компромисс §3 плана, и он виден по ``context``.
        """
        source = document.text
        cuts = self._cuts(document, section, index)
        # Кусок короче половины бюджета — не экономия, а потеря: заголовок, отрезанный
        # от своей первой фразы, перестаёт быть её контекстом. Поэтому граница,
        # стоящая ближе половины окна, кандидатом не считается (кроме случая, когда
        # секция целиком влезает в бюджет — тогда режется она сама, а не её край).
        floor = min(self.chunk_size // 2, self.chunk_size - self.min_chunk_size)
        pieces: list[tuple[int, int, str | None]] = []
        position = section.start
        # Контекст **начала** текущего куска: он приходит от разреза, которым этот кусок
        # открыт, и переживает последний (короткий) кусок секции — иначе хвост fence или
        # таблицы терял бы маркер и шапку ровно там, где они нужнее всего.
        start_context: str | None = None

        while position < section.end:
            if section.end - position <= self.chunk_size:
                pieces.append((position, section.end, start_context))
                break
            limit = position + self.chunk_size
            window = [cut for cut in cuts if position + floor <= cut.offset <= limit]
            structural = [cut for cut in window if cut.structural]
            sentences = [cut for cut in window if cut.sentence]
            chosen = max(structural or sentences or window, key=lambda cut: cut.offset, default=None)
            if chosen is not None:
                end, context = chosen.offset, chosen.context
            else:
                end, context = self._hard_cut(source, position, limit), None
            pieces.append((position, end, start_context))
            position, start_context = end, context

        return self._absorb_tail(source, pieces)

    def _absorb_tail(
        self, source: str, pieces: list[tuple[int, int, str | None]]
    ) -> list[tuple[int, int, str | None]]:
        """Приклеивает хвост короче порога к предыдущему куску той же секции.

        Текст не выбрасывается (P1), а обломок разметки не становится отдельным
        ``source_id`` цитаты. Бюджет при этом может быть превышен на ``min_chunk_size``
        символов — это объявленный компромисс, а не незамеченный выход за границу.
        """
        if len(pieces) < 2:
            return pieces
        start, end, _ = pieces[-1]
        if len(source[start:end].strip()) >= self.min_chunk_size:
            return pieces
        previous_start, _, previous_context = pieces[-2]
        return [*pieces[:-2], (previous_start, end, previous_context)]

    def _cuts(self, document: MdDocument, section: _Section, index: LineIndex) -> list[_Cut]:
        """Все разрешённые границы внутри секции, кроме её собственного начала."""
        cuts: list[_Cut] = []
        for block in document.blocks:
            if not section.start <= block.span.start_char < section.end:
                continue
            if block.span.start_char > section.start:
                cuts.append(_Cut(block.span.start_char, structural=True))
            cuts.extend(self._block_cuts(block, document.text, index))
        return cuts

    def _block_cuts(
        self, block: MdBlock, source: str, index: LineIndex, prefix: str = ""
    ) -> list[_Cut]:
        """Границы внутри блока: атомарные конструкции не режутся вовсе.

        ``prefix`` — контекст вложенности (``> `` для цитаты, ``::: {.cls}`` для div):
        кусок, начавшийся внутри контейнера, обязан нести его маркер, иначе одиночный
        абзац цитаты читается как обычный текст.
        """
        if block.kind in ("heading", "math", "footnote_def", "reference", "front_matter"):
            return []
        if block.kind == "paragraph":
            return [
                _Cut(
                    offset,
                    structural=False,
                    context=prefix or None,
                    sentence=_ends_sentence(source, offset),
                )
                for offset in safe_cut_offsets(
                    source,
                    block.span.start_char,
                    block.span.end_char,
                    math=self.profile == "pandoc",
                )
            ]
        if block.kind in ("fence", "code", "html"):
            context = self._fence_opening(block, source) if block.kind == "fence" else None
            if prefix:
                context = f"{prefix}{context}" if context else prefix
            return [
                _Cut(index.offset(line), structural=False, context=context)
                for line in range(block.span.start_line + 1, block.span.end_line)
            ]
        if block.kind == "table":
            return self._table_cuts(block, source, index, prefix=prefix)

        inner = self._child_prefix(block, source, prefix)
        cuts: list[_Cut] = []
        for child in block.children:
            if child.span.start_char > block.span.start_char:
                cuts.append(_Cut(child.span.start_char, structural=True, context=inner or None))
            cuts.extend(self._block_cuts(child, source, index, prefix=inner))
        return cuts

    @staticmethod
    def _child_prefix(block: MdBlock, source: str, prefix: str) -> str:
        """Контекст вложенности для детей контейнера (цитата, div, сноска)."""
        if block.kind == "blockquote":
            return f"{prefix}> " if prefix else "> "
        if block.kind == "container":
            return prefix or MarkdownChunker._fence_opening(block, source)
        return prefix

    @staticmethod
    def _table_cuts(
        block: MdBlock, source: str, index: LineIndex, prefix: str = ""
    ) -> list[_Cut]:
        """Резать таблицу можно только между строками; продолжение несёт шапку (P9)."""
        header_rows = int(block.info.get("header_rows") or 1)
        header_end = min(index.offset(block.span.start_line + header_rows + 1), block.span.end_char)
        header = source[block.span.start_char : header_end].rstrip("\n")
        context = f"{prefix}{header}" if prefix else header
        rows = [item for item in block.walk() if item.kind == "table_row"]
        return [
            _Cut(row.span.start_char, structural=True, context=context)
            for row in rows[header_rows:]
            if block.span.start_char < row.span.start_char < block.span.end_char
        ]

    @staticmethod
    def _fence_opening(block: MdBlock, source: str) -> str:
        """Открывающая строка fence: маркер и info string (P10)."""
        start = block.span.start_char
        newline = source.find("\n", start, block.span.end_char)
        return source[start : newline if newline != -1 else block.span.end_char].rstrip("\r")

    def _hard_cut(self, source: str, position: int, limit: int) -> int:
        """Жёсткий разрез, когда безопасных границ нет: не рвёт графемный кластер."""
        end = min(limit, len(source))
        while end > position + 1 and not is_cut_boundary(source, end):
            end -= 1
        return end

    @staticmethod
    def _context_prefix(heading_path: str, context: str | None) -> str:
        """Контекст продолжения: путь заголовков плюс шапка таблицы / fence (P9/P10)."""
        if not context:
            return heading_path
        return f"{heading_path}\n{context}" if heading_path else context

    # -- прочее --------------------------------------------------------- #

    @staticmethod
    def _chunk_id(document_id: str, no: int) -> str:
        return f"{document_id}#{no}"


def _ends_sentence(source: str, offset: int) -> bool:
    """Кончается ли текст перед ``offset`` концом предложения.

    Разрез стоит **перед** первым символом следующего слова, поэтому знак конца
    предложения обычно отделён от него пробелом: ``"фраза. Следующая"`` → позиция указывает
    на ``С``, а ``.`` — на два символа левее. Проверять только ``source[offset - 1]``
    значило бы не найти ни одного конца предложения.
    """
    index = offset - 1
    while index >= 0 and source[index].isspace():
        index -= 1
    return index >= 0 and source[index] in SENTENCE_END_CHARS


def _heading_title(block: MdBlock) -> str:
    """Текст заголовка для ``heading_path`` — без атрибутов разметки (P11)."""
    title = str(block.info.get("text") or "").strip()
    return _HEADING_ATTRS_RE.sub("", title).strip()


class MarkdownChunkerPort(MarkdownChunker):
    """Обёртка ``MarkdownChunker`` под контракт ``ChunkerPort`` (T7.3).

    Порт (``core/ports/chunker.py``) объявляет ``chunk(doc: ExtractedDoc, document_id)``,
    тогда как базовый :class:`MarkdownChunker` принимает ``chunk(document_id, text)``
    и используется selfcheck. Обёртка переопределяет ``chunk`` под порт-сигнатуру и
    оставляет базовый метод доступным как :meth:`chunk_text` — так адаптер реализует
    ``ChunkerPort`` структурно, не ломая существующие вызовы.

    ``document_id`` — стабильный id документа (например, ``source_id``/``doc_id``);
    ``chunk_id`` детерминирован (``f"{document_id}#{no}"``), что сохраняет привязку
    чанка к сборке и идемпотентность (I19/S5).
    """

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:  # type: ignore[override]
        """Порт-путь: ``ExtractedDoc`` + ``document_id`` → структурные чанки."""
        return self.chunk_text(document_id, doc.text)

    def chunk_text(self, document_id: str, text: str) -> list[Chunk]:
        """Чанкинг по markdown-тексту (сохранённый базовый вход)."""
        return super().chunk(document_id, text)