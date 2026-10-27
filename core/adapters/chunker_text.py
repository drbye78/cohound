"""Plain-text чанкер по абзацам — реализация ``ChunkerPort`` для семейства ``text``.

Зачем отдельный чанкер, если markdown-чанкер режет и plain text: ``MarkdownChunker``
делит по заголовкам, а у плоского текста их нет — остаётся один «раздел», который режется
по размеру, и разрез может прийтись на середину абзаца/предложения. Для текста (``.txt``/
``.log``/``.sql``/… — семейство :data:`core.util.document_formats.FAMILY_TEXT`) граница
абзаца и есть естественная единица смысла, поэтому:

* текст делится на блоки по пустым строкам (абзац = блок);
* блоки **сливаются** до ``chunk_size``; блок, который помещается, не режется никогда;
* блок больше ``chunk_size`` режется по границам предложений (``.``/``!``/``?``/``;`` и
  многоточие, за которыми пробел), и только предложение, само превышающее ``chunk_size``,
  режется жёстко (по возможности по пробелу, а не по букве).

``heading_path`` — всегда ``""``: заголовков в плоском тексте нет, и выдумывать их нельзя
(anchor-blind/честность структуры). ``context_prefix`` — первое предложение первого абзаца
чанка, ограниченное :data:`CONTEXT_PREFIX_MAX` символами. Контекст идёт в промпт оценщика
шапкой фрагмента (``context:``, ``core.pipeline.prompt``) и **в эмбеддинг не входит**:
эмбеддится чистый ``text``, поэтому правка контекста не меняет ``index_build_id``. На
retrieval-пути поле пока теряется: ``RetrievedChunk`` его не несёт, и до оценщика строка
``context:`` доходит только там, где передан ``Chunk``.

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

__all__ = [
    "CONTEXT_PREFIX_MAX",
    "MIN_CHUNK_SIZE",
    "TEXT_CHUNKER_VERSION",
    "TextChunker",
    "TextChunkerPort",
]

#: Версия правил нарезки (провенанс/``chunker_version``).
#: ``text-para-2`` (было ``text-para-1``): введён порог фрагмента (1 → 20). Порог меняет
#: набор чанков, то есть условие измерения: ``execution_versions`` (§13), не методология.
TEXT_CHUNKER_VERSION = "text-para-2"

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

#: Граница предложения: терминатор + пробел/перевод строки. ``…`` — тот же терминатор для
#: русских текстов; ``;`` включён осознанно (владелец: перечисление — тоже смысловая
#: граница, а склейка длинного перечня в один кусок хуже для цитат).
_SENTENCE_SEP_RE = re.compile(r"(?<=[.!?;…])\s+")

#: Схлопывание пробелов в ``context_prefix``.
_WHITESPACE_RE = re.compile(r"\s+")


def _sha256_16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _blocks(text: str) -> list[str]:
    """Абзацы текста (непустые, без краевых пробелов), в исходном порядке."""
    return [block.strip() for block in _BLOCK_SEP_RE.split(text) if block.strip()]


def _sentences(text: str) -> list[str]:
    """Предложения блока (непустые, без краевых пробелов)."""
    return [piece.strip() for piece in _SENTENCE_SEP_RE.split(text) if piece.strip()]


def _hard_split(text: str, size: int) -> list[str]:
    """Жёсткий разрез по ``size`` с предпочтением пробела во второй половине окна."""
    pieces: list[str] = []
    position = 0
    length = len(text)
    while position < length:
        end = min(position + size, length)
        if end < length:
            space = text.rfind(" ", position + size // 2, end)
            if space > position:
                end = space
        piece = text[position:end].strip()
        if piece:
            pieces.append(piece)
        position = end
    return pieces


def _split_block(block: str, size: int) -> list[str]:
    """Блок больше ``size``: сначала границы предложений, потом жёсткий разрез.

    Предложение, которое само не помещается, режется жёстко — иначе один «бесконечный»
    абзац (лог без точек) вернул бы чанк сверх ``chunk_size``.
    """
    pieces: list[str] = []
    buffer: list[str] = []
    length = 0
    for sentence in _sentences(block):
        if len(sentence) > size:
            if buffer:
                pieces.append(" ".join(buffer))
                buffer, length = [], 0
            pieces.extend(_hard_split(sentence, size))
            continue
        extra = len(sentence) + (1 if buffer else 0)
        if buffer and length + extra > size:
            pieces.append(" ".join(buffer))
            buffer, length = [], 0
        buffer.append(sentence)
        length += len(sentence) + (1 if len(buffer) > 1 else 0)
    if buffer:
        pieces.append(" ".join(buffer))
    return pieces


def _context_prefix(text: str) -> str:
    """Первое предложение первого абзаца чанка, схлопнутое и ограниченное по длине."""
    first_block = text.split("\n", 1)[0].strip()
    sentences = _sentences(first_block)
    prefix = sentences[0] if sentences else first_block
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
