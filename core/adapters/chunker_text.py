"""Plain-text чанкер по абзацам — реализация ``ChunkerPort`` для семейства ``text``.

Зачем отдельный чанкер, если markdown-чанкер режет и plain text: ``MarkdownChunker``
делит по заголовкам, а у плоского текста их нет — остаётся один «раздел», который режется
по размеру, и разрез может прийтись на середину абзаца/предложения. Для текста (``.txt``/
``.log``/``.sql``/… — семейство :data:`core.util.document_formats.FAMILY_TEXT`) граница
абзаца и есть естественная единица смысла, поэтому:

* текст делится на блоки по пустым строкам (абзац = блок);
* блоки **сливаются** до ``chunk_size``; блок, который помещается, не режется никогда;
* блок больше ``chunk_size`` режется по границам предложений (``.``/``!``/``?``/``;`` и
  многоточие — а также полноширинные ``。！？；``, которые **не требуют пробела** после
  себя, иначе в китайском тексте не находится ни одной границы), затем по границам
  клауз (``，``/``、``/``：``), и только клауза, сама превышающая ``chunk_size``, режется
  жёстко — по безопасной границе символа, а не по номеру байта.

``heading_path`` — всегда ``""``: заголовков в плоском тексте нет, и выдумывать их нельзя
(anchor-blind/честность структуры). ``context_prefix`` — первое предложение первого абзаца
чанка, ограниченное :data:`CONTEXT_PREFIX_MAX` символами. Контекст идёт в промпт оценщика
шапкой фрагмента (``context:``, ``core.pipeline.prompt``) и **в эмбеддинг не входит**:
эмбеддится чистый ``text``, поэтому правка контекста не меняет ``index_build_id``.
Retrieval-путь поле больше не теряет (L1b-2a плана 18): ``RetrievedChunk`` его несёт.

``version`` = ``text-para-1``: входит в ``chunker_version`` → ``partition_build_id``/
``index_build_id``, поэтому смена правил нарезки обязана поднять версию (§13).
Порог фрагмента (:data:`MIN_CHUNK_SIZE`) — такая же смена нарезки: её версионная граница
``execution_versions``, и версию поднимает владелец правки вместе с пере-прогоном, а не
адаптер «на месте». Пока она не поднята, ``chunker_version`` описывает прежний набор чанков.

Нейтрален (§1.3).
"""

from __future__ import annotations

import hashlib
import re

from core.ports.types import Chunk, ExtractedDoc
from core.util.text_utils import hard_cut

__all__ = [
    "CONTEXT_PREFIX_MAX",
    "MIN_CHUNK_SIZE",
    "TEXT_CHUNKER_VERSION",
    "TextChunker",
    "TextChunkerPort",
]

#: Версия правил нарезки (провенанс/``chunker_version``).
#: ``text-para-3`` (было ``text-para-2``): границы предложений понимают полноширинные
#: ``。！？；`` (в CJK пробела после терминатора нет), добавлен второй уровень — клаузы
#: (``，、：``), а жёсткий разрез не рвёт графемный кластер. Меняет chunk-set ⇒
#: ``execution_versions`` (§13), не методология.
#: ``text-para-2`` (было ``text-para-1``): введён порог фрагмента (1 → 20).
TEXT_CHUNKER_VERSION = "text-para-3"

#: Минимальная длина чанка (символы): кусок короче в индекс не попадает.
#:
#: То же значение и то же обоснование, что у markdown-чанкера (замер распределения длин
#: на реальном корпусе — :data:`core.adapters.chunker_markdown.MIN_CHUNK_SIZE`): кусок
#: короче порога — обломок разметки, а не утверждение, и как ``source_id`` цитаты он
#: опасен (I22). Порог не масштабируется под ``chunk_size`` сам: при малой геометрии
#: его объявляют явно (``min_chunk_size < chunk_size`` — проверяется конструктором).
MIN_CHUNK_SIZE = 20

#: Потолок ``context_prefix`` (символы): контекст — шапка фрагмента в промпте оценщика
#: (в эмбеддинг он не входит), а не второй абзац; значение совпадает по масштабу с
#: заголовочным путём markdown-чанкера.
CONTEXT_PREFIX_MAX = 200

#: Граница абзаца — строка, состоящая только из пробелов (plain text из CMS часто несёт
#: хвостовые пробелы в «пустых» строках).
_BLOCK_SEP_RE = re.compile(r"\n[ \t]*\n(?:[ \t]*\n)*")

#: Граница предложения. Западная — терминатор + пробел (``…`` и ``;`` включены осознанно:
#: многоточие — тот же терминатор, а перечисление тоже смысловая граница). Полноширинная —
#: терминатор **без** обязательного пробела: в китайском/японском тексте его нет, и
#: правило «терминатор + пробел» не находит там ни одной границы (замер: 2 000 иероглифов
#: резались жёстко посреди фразы). Закрывающая кавычка сразу после терминатора относится к
#: предложению и остаётся в нём.
_SENTENCE_SEP_RE = re.compile(
    r"(?<=[.!?;…])\s+"                # западный терминатор + пробел
    r"|(?<=[。！？；])(?![」』”》])"     # полноширинный терминатор, кавычка — не за ним
    r"|(?<=[。！？；][」』”》])\s*"      # терминатор + закрывающая кавычка
)

#: Граница клаузы — второй уровень, когда предложение само больше бюджета:
#: полноширинные ``，``/``、``/``：`` и западные запятая/двоеточие с пробелом.
_CLAUSE_SEP_RE = re.compile(r"(?<=[，、：])\s*|(?<=[,:])\s+")

#: Схлопывание пробелов в ``context_prefix``.
_WHITESPACE_RE = re.compile(r"\s+")


def _sha256_16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _blocks(text: str) -> list[str]:
    """Абзацы текста (непустые, без краевых пробелов), в исходном порядке."""
    return [block.strip() for block in _BLOCK_SEP_RE.split(text) if block.strip()]


def _split_keep(text: str, pattern: re.Pattern[str]) -> list[tuple[str, str]]:
    """Куски с разделителем, который их завершал: ``(кусок, разделитель)``.

    Разделитель сохраняется, потому что он **часть текста**: у западной границы это
    пробел, у полноширинной — ничего. Склейка ``кусок + разделитель + кусок`` возвращает
    вход без потерь; альтернатива («склеивать всегда пробелом») дописывала бы в китайский
    текст пробелы, которых в нём не было, и меняла бы ``content_hash`` относительно
    источника.
    """
    pieces: list[tuple[str, str]] = []
    position = 0
    for match in pattern.finditer(text):
        if match.start() < position:
            continue
        piece = text[position : match.start()]
        if piece.strip():
            pieces.append((piece.strip(), text[match.start() : match.end()]))
        position = match.end()
    tail = text[position:]
    if tail.strip():
        pieces.append((tail.strip(), ""))
    return pieces


def _sentences(text: str) -> list[tuple[str, str]]:
    """Предложения блока с разделителем после каждого (непустые, без краевых пробелов)."""
    return _split_keep(text, _SENTENCE_SEP_RE)


def _clauses(text: str) -> list[tuple[str, str]]:
    """Клаузы предложения с разделителем — второй уровень, когда предложение не влезло."""
    return _split_keep(text, _CLAUSE_SEP_RE)


def _hard_split(text: str, size: int) -> list[str]:
    """Жёсткий разрез по ``size``: пробел во второй половине окна, иначе безопасная граница.

    ``hard_cut`` не рвёт графемный кластер и не начинает кусок с закрывающей пунктуации:
    иначе цитата открывалась бы знаком, который относится к предыдущей фразе.
    """
    pieces: list[str] = []
    position = 0
    length = len(text)
    while position < length:
        end = min(position + size, length)
        if end < length:
            space = text.rfind(" ", position + size // 2, end)
            end = space if space > position else hard_cut(text, position, end)
        piece = text[position:end].strip()
        if piece:
            pieces.append(piece)
        position = max(end, position + 1)
    return pieces


def _split_block(block: str, size: int) -> list[str]:
    """Блок больше ``size``: сначала границы предложений, потом жёсткий разрез.

    Предложение, которое само не помещается, режется жёстко — иначе один «бесконечный»
    абзац (лог без точек) вернул бы чанк сверх ``chunk_size``.
    """
    pieces: list[str] = []
    current = ""
    pending_sep = ""
    for sentence, sep in _sentences(block):
        if len(sentence) > size:
            if current:
                pieces.append(current)
            current, pending_sep = "", ""
            pieces.extend(_split_sentence(sentence, size))
            continue
        candidate = f"{current}{pending_sep}{sentence}" if current else sentence
        if current and len(candidate) > size:
            pieces.append(current)
            candidate = sentence
        current = candidate
        pending_sep = sep
    if current:
        pieces.append(current)
    return pieces


def _split_sentence(sentence: str, size: int) -> list[str]:
    """Предложение больше бюджета: сначала клаузы, потом жёсткий разрез.

    Клауза — это уже осмысленная единица (перечисление, придаточное), поэтому она
    предпочтительнее разреза по номеру символа; каждая клауза, которая сама не влезает,
    режется жёстко.
    """
    pieces: list[str] = []
    current = ""
    pending_sep = ""
    for clause, sep in _clauses(sentence):
        if len(clause) > size:
            if current:
                pieces.append(current)
            current, pending_sep = "", ""
            pieces.extend(_hard_split(clause, size))
            continue
        candidate = f"{current}{pending_sep}{clause}" if current else clause
        if current and len(candidate) > size:
            pieces.append(current)
            candidate = clause
        current = candidate
        pending_sep = sep
    if current:
        pieces.append(current)
    return pieces or [sentence]


def _context_prefix(text: str) -> str:
    """Первое предложение первого абзаца чанка, схлопнутое и ограниченное по длине."""
    first_block = text.split("\n", 1)[0].strip()
    sentences = _sentences(first_block)
    prefix = sentences[0][0] if sentences else first_block
    prefix = _WHITESPACE_RE.sub(" ", prefix).strip()
    if len(prefix) > CONTEXT_PREFIX_MAX:
        prefix = prefix[:CONTEXT_PREFIX_MAX].rstrip()
    return prefix


class TextChunker:
    """Абзацный чанкер plain text (``text-para-1``).

    Сигнатура базовая — ``chunk(document_id, text)`` (как у
    :class:`core.adapters.chunker_markdown.MarkdownChunker`); порт-путь с ``ExtractedDoc``
    даёт :class:`TextChunkerPort`. Перекрытия (overlap) нет осознанно: дубли абзацев
    удваивают evidence-шум, а смысловая граница абзаца и так не рвётся.

    **Смещений в документе нет — это объявленная граница, а не забывчивость.** Куски
    здесь обрезаются ``.strip()`` и склеиваются из предложений, поэтому ``text`` не
    является точным срезом входа, и ``char_start``/``char_end`` остаются ``None``.
    Точные срезы для плоского текста — отдельная версия правил (``text-para-4``), а не
    побочный эффект провенанса.
    """

    version = TEXT_CHUNKER_VERSION

    def __init__(
        self,
        chunk_size: int = 1500,
        min_chunk_size: int = MIN_CHUNK_SIZE,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size должен быть положительным")
        if min_chunk_size < 0:
            raise ValueError("min_chunk_size не может быть отрицательным")
        if min_chunk_size >= chunk_size:
            raise ValueError("min_chunk_size должен быть меньше chunk_size")
        self.chunk_size = chunk_size
        self.min_chunk_size = min_chunk_size

    def chunk(self, document_id: str, text: str) -> list[Chunk]:
        """Текст → абзацные чанки (``heading_path=""``, детерминированные id/hash)."""
        pieces: list[str] = []
        buffer: list[str] = []
        length = 0

        def flush() -> None:
            nonlocal buffer, length
            if buffer:
                pieces.append("\n\n".join(buffer))
                buffer, length = [], 0

        for block in _blocks(text):
            if len(block) > self.chunk_size:
                flush()
                pieces.extend(_split_block(block, self.chunk_size))
                continue
            extra = len(block) + (2 if buffer else 0)
            if buffer and length + extra > self.chunk_size:
                flush()
                extra = len(block)
            buffer.append(block)
            length += extra
        flush()

        chunks: list[Chunk] = []
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
                    heading_path="",
                    context_prefix=_context_prefix(piece),
                    content_hash=_sha256_16(piece),
                )
            )
        return chunks

    @staticmethod
    def _chunk_id(document_id: str, no: int) -> str:
        return f"{document_id}#{no}"


class TextChunkerPort(TextChunker):
    """Обёртка ``TextChunker`` под контракт ``ChunkerPort`` (``chunk(doc, document_id)``).

    Зеркалит :class:`core.adapters.chunker_markdown.MarkdownChunkerPort`: базовый вход
    ``chunk(document_id, text)`` доступен как :meth:`chunk_text`, порт-вход принимает
    ``ExtractedDoc``. ``document_id`` — стабильный id документа, поэтому ``chunk_id``
    детерминирован (``f"{document_id}#{no}"``).
    """

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:  # type: ignore[override]
        """Порт-путь: ``ExtractedDoc`` + ``document_id`` → абзацные чанки."""
        return self.chunk_text(document_id, doc.text)

    def chunk_text(self, document_id: str, text: str) -> list[Chunk]:
        """Чанкинг по plain-тексту (сохранённый базовый вход)."""
        return super().chunk(document_id, text)
