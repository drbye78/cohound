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

**Куски — точные срезы нормализованного входа (P1/P4).** Как и у markdown-чанкера,
``chunk.text`` — это ``normalized[char_start:char_end]`` без ``strip()`` и без склейки
через пробел: конкатенация текстов всех чанков равна нормализованному входу
байт-в-байт, а разделитель между кусками (пробел/перевод строки) остаётся в
**предыдущем** куске. Нормализация здесь минимальна и объявлена: BOM снимается,
``CRLF``/``CR`` приводятся к ``LF`` — смещения относятся **к нормализованному тексту**,
ровно как у ``MarkdownChunker`` (``md_ast.normalize_text``). Документ без единого
непробельного символа даёт ноль чанков: терять в нём нечего.

``heading_path`` — всегда ``""``: заголовков в плоском тексте нет, и выдумывать их нельзя
(anchor-blind/честность структуры). ``context_prefix`` — первое предложение первого абзаца
чанка, ограниченное :data:`CONTEXT_PREFIX_MAX` символами. Контекст идёт в промпт оценщика
шапкой фрагмента (``context:``, ``core.pipeline.prompt``) и **в эмбеддинг не входит**:
эмбеддится чистый ``text``, поэтому правка контекста не меняет ``index_build_id``.
Retrieval-путь поле больше не теряет (L1b-2a плана 18): ``RetrievedChunk`` его несёт.

``version`` = ``text-para-4``: входит в ``chunker_version`` → ``partition_build_id``/
``index_build_id``, поэтому смена правил нарезки обязана поднять версию (§13). Здесь она
поднята тем же актом (`text-para-3` → `text-para-4`) и объявлена как `execution_change`:
хранимый текст и вход эмбеддера/промпта меняются, методология — нет.

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
#:
#: ``text-para-4`` (было ``text-para-3``): кусок — **точный срез** нормализованного
#: входа, а не строка, собранная из ``.strip()``-фрагментов. ``char_start``/``char_end``
#: заполнены тем же срезом (``text == normalized[start:end]``), конкатенация текстов
#: всех чанков равна входу (P1). Набор **границ** разреза по возможности не менялся:
#: блоки, предложения и клаузы считаются теми же правилами, а новая позиция границы —
#: это прежняя позиция начала следующего куска (замер: 185 из 194 документов корпуса
#: сохранили границы; см. ``docs/reports/measurements/2026-11-04-text-chunker-exact-slices.md``).
#: Сдвинуться граница могла только там, где точный срез это вынудил:
#:
#: * разделитель между блоками берётся из источника (а не нормализуется в ``\n\n``) и
#:   входит в **предыдущий** кусок — поэтому краевые пробелы больше не срезаются, и
#:   кусок может превысить бюджет ровно на сохранённые пробелы (``over_budget`` замера);
#: * кусок короче :data:`MIN_CHUNK_SIZE` не выбрасывается, а приклеивается к
#:   предыдущему (или, если он первый, к следующему) — P1 запрещает терять символы;
#: * в жёстком разрезе пробел остаётся в предыдущем куске (``space + 1``), а не
#:   отбрасывается, как раньше, — иначе разделитель оказался бы в следующем.
#:
#: Меняется хранимый текст (краевые пробелы больше не срезаются) и вход
#: эмбеддера/промпта для семейства ``text`` ⇒ ``execution_versions`` (§13), не
#: методология.
#:
#: ``text-para-3`` (было ``text-para-2``): границы предложений понимают полноширинные
#: ``。！？；`` (в CJK пробела после терминатора нет), добавлен второй уровень — клаузы
#: (``，、：``), а жёсткий разрез не рвёт графемный кластер. Меняет chunk-set ⇒
#: ``execution_versions`` (§13), не методология.
#: ``text-para-2`` (было ``text-para-1``): введён порог фрагмента (1 → 20).
TEXT_CHUNKER_VERSION = "text-para-4"

#: Минимальная длина чанка (символы): кусок короче в индекс не попадает.
#:
#: То же значение и то же обоснование, что у markdown-чанкера (замер распределения длин
#: на реальном корпусе — :data:`core.adapters.chunker_markdown.MIN_CHUNK_SIZE`): кусок
#: короче порога — обломок разметки, а не утверждение, и как ``source_id`` цитаты он
#: опасен (I22). Порог не масштабируется под ``chunk_size`` сам: при малой геометрии
#: его объявляют явно (``min_chunk_size < chunk_size`` — проверяется конструктором).
#:
#: **Порог не выбрасывает текст (P1).** Хвост короче порога приклеивается к предыдущему
#: куску, а документ, который целиком короче порога, остаётся одним чанком: удалить его
#: значило бы потерять вход. Такие «tiny» чанки публикует харнесс замера.
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


def _normalize(text: str) -> str:
    """Вход, к которому относятся смещения: без BOM, переводы строк — ``LF``.

    Это ровно та же нормализация, что у markdown-чанкера
    (:func:`core.adapters.md_ast.normalize_text`), но без разбора: ``char_start``/
    ``char_end`` обязаны относиться к тому тексту, из которого взят срез. Полная
    нормализация (управляющие символы, пустые строки, края) уже сделана читателем
    (``text-plain-2``); здесь — защита от входа, пришедшего мимо него.
    """
    text = text.removeprefix("\ufeff")
    if "\r" in text:
        text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text


def _content_span(text: str, start: int, end: int) -> tuple[int, int] | None:
    """Срез без краевых пробелов: ``(начало, конец)`` или ``None`` для пробельного."""
    piece = text[start:end]
    left = len(piece) - len(piece.lstrip())
    right = len(piece.rstrip())
    if right <= left:
        return None
    return start + left, start + right


def _content_bounds(text: str, start: int, end: int) -> tuple[int, int]:
    """Границы содержимого внутри среза: краевые пробелы исключены."""
    piece = text[start:end]
    left = start + len(piece) - len(piece.lstrip())
    right = start + len(piece.rstrip())
    return left, right


def _block_spans(text: str) -> list[tuple[int, int]]:
    """Абзацы текста как **смещения** в нём (без краевых пробелов), в исходном порядке.

    Смещения, а не строки: кусок обязан быть точным срезом, поэтому блок нельзя
    «пересобрать» из ``block.strip()`` — потерялся бы разделитель между блоками.
    """
    spans: list[tuple[int, int]] = []
    position = 0
    for match in _BLOCK_SEP_RE.finditer(text):
        span = _content_span(text, position, match.start())
        if span is not None:
            spans.append(span)
        position = match.end()
    span = _content_span(text, position, len(text))
    if span is not None:
        spans.append(span)
    return spans


def _split_spans(
    text: str, start: int, end: int, pattern: re.Pattern[str]
) -> list[tuple[int, int]]:
    """Куски ``[start, end)`` по границам ``pattern`` — смещения, а не строки.

    Границы предложения/клаузы **потребляются** (пробел после западного терминатора,
    пробелы вокруг полноширинного), поэтому следующий кусок начинается сразу за
    разделителем: разделитель остаётся в предыдущем куске, и склейка возвращает вход.
    Если между границами нет ничего, кроме пробелов, кусок из них не создаётся —
    граница просто не ставится (иначе появился бы «пустой чанк»).
    """
    spans: list[tuple[int, int]] = []
    position = start
    for match in pattern.finditer(text, start, end):
        if match.start() < position:
            continue
        if text[position : match.start()].strip():
            spans.append((position, match.end()))
            position = match.end()
            continue
        # Пробельный промежуток: разделитель принадлежит предыдущему куску.
        if spans:
            spans[-1] = (spans[-1][0], match.end())
        position = match.end()
    if position < end:
        if text[position:end].strip():
            spans.append((position, end))
        elif spans:
            spans[-1] = (spans[-1][0], end)
    return spans


class TextChunker:
    """Абзацный чанкер plain text (``text-para-4``).

    Сигнатура базовая — ``chunk_text(document_id, text)`` (как у
    :class:`core.adapters.chunker_markdown.MarkdownChunker`); порт-путь с ``ExtractedDoc``
    даёт :class:`TextChunkerPort`. Перекрытия (overlap) нет осознанно: дубли абзацев
    удваивают evidence-шум, а смысловая граница абзаца и так не рвётся.

    **Куски — точные срезы.** ``text`` чанка равен ``normalized[char_start:char_end]``,
    где ``normalized`` — вход после :func:`_normalize`; ``char_start``/``char_end``
    заполнены всегда. Конкатенация текстов всех чанков равна нормализованному входу
    (P1), поэтому разделитель между кусками остаётся в предыдущем куске, а не
    переписывается пробелом.
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

    def chunk_text(self, document_id: str, text: str) -> list[Chunk]:
        """Текст → абзацные чанки (``heading_path=""``, точные срезы, детерминизм)."""
        source = _normalize(text)
        if not source.strip():
            return []
        spans = self._spans(source)
        chunks: list[Chunk] = []
        for start, end in spans:
            body = source[start:end]
            if not body.strip():
                continue
            no = len(chunks)
            chunks.append(
                Chunk(
                    chunk_id=self._chunk_id(document_id, no),
                    chunk_no=no,
                    text=body,
                    heading_path="",
                    context_prefix=_context_prefix(body),
                    content_hash=_sha256_16(body),
                    char_start=start,
                    char_end=end,
                )
            )
        return chunks

    # -- границы ------------------------------------------------------- #

    def _spans(self, source: str) -> list[tuple[int, int]]:
        """Непрерывные куски, покрывающие ``source`` от 0 до ``len(source)``.

        Строятся как список **начал** кусков: начало первого — 0, конец последнего —
        длина входа, поэтому ведущие/хвостовые переводы строк попадают в крайние куски,
        а не теряются (P1). Затем куски короче порога приклеиваются к соседу.
        """
        pieces = self._pieces(source)
        if not pieces:
            return []
        starts = [0, *(span_start for span_start, _ in pieces[1:])]
        ends = [*(span_start for span_start, _ in pieces[1:]), len(source)]
        return _absorb_short(source, list(zip(starts, ends, strict=True)), self.min_chunk_size)

    def _pieces(self, source: str) -> list[tuple[int, int]]:
        """Куски по абзацам и бюджету — до склейки коротких и до покрытия краёв.

        Слияние блоков считается **прежней арифметикой** (длина содержимого + 2 на
        разделитель), а не длиной фактического среза: иначе границы поехали бы там, где
        разделитель в источнике не ровно ``\\n\\n`` (двойной перевод строки, пробелы в
        «пустой» строке). Точный срез вынуждает только хранимый текст — разделитель
        берётся из источника и может сделать кусок длиннее бюджета на свою длину; это
        видно как ``over_budget`` замера, а не как сдвиг границы.
        """
        pieces: list[tuple[int, int]] = []
        buffer_start: int | None = None
        buffer_end = 0
        length = 0
        for start, end in _block_spans(source):
            block_len = end - start
            if block_len > self.chunk_size:
                if buffer_start is not None:
                    pieces.append((buffer_start, buffer_end))
                    buffer_start, length = None, 0
                pieces.extend(self._split_block(source, start, end))
                continue
            extra = block_len + (2 if buffer_start is not None else 0)
            if buffer_start is not None and length + extra > self.chunk_size:
                pieces.append((buffer_start, buffer_end))
                buffer_start, buffer_end, length = start, end, block_len
                continue
            if buffer_start is None:
                buffer_start, buffer_end, length = start, end, block_len
            else:
                buffer_end = end
                length += extra
        if buffer_start is not None:
            pieces.append((buffer_start, buffer_end))
        return pieces

    def _split_block(self, source: str, start: int, end: int) -> list[tuple[int, int]]:
        """Блок больше ``size``: сначала границы предложений, потом жёсткий разрез.

        Предложение, которое само не помещается, режется жёстко — иначе один «бесконечный»
        абзац (лог без точек) вернул бы чанк сверх ``chunk_size``. Бюджет считается по
        **содержимому** предложения (краевые пробелы и разделитель не входят): прежние
        правила добавляли разделитель только вместе со следующим предложением, и иначе
        граница сдвинулась бы на длину разделителя без нужды.
        """
        pieces: list[tuple[int, int]] = []
        current_start: int | None = None
        current_end = 0
        for sentence_start, sentence_end in _split_spans(
            source, start, end, _SENTENCE_SEP_RE
        ):
            content_start, content_end = _content_bounds(
                source, sentence_start, sentence_end
            )
            if content_end - content_start > self.chunk_size:
                if current_start is not None:
                    pieces.append((current_start, current_end))
                    current_start = None
                pieces.extend(self._split_sentence(source, sentence_start, sentence_end))
                continue
            if current_start is None:
                current_start, current_end = content_start, content_end
            elif content_end - current_start > self.chunk_size:
                pieces.append((current_start, current_end))
                current_start, current_end = content_start, content_end
            else:
                current_end = content_end
        if current_start is not None:
            pieces.append((current_start, current_end))
        return pieces or [(start, end)]

    def _split_sentence(self, source: str, start: int, end: int) -> list[tuple[int, int]]:
        """Предложение больше бюджета: сначала клаузы, потом жёсткий разрез."""
        pieces: list[tuple[int, int]] = []
        current_start: int | None = None
        current_end = 0
        for clause_start, clause_end in _split_spans(source, start, end, _CLAUSE_SEP_RE):
            content_start, content_end = _content_bounds(source, clause_start, clause_end)
            if content_end - content_start > self.chunk_size:
                if current_start is not None:
                    pieces.append((current_start, current_end))
                    current_start = None
                pieces.extend(self._hard_split(source, clause_start, clause_end))
                continue
            if current_start is None:
                current_start, current_end = content_start, content_end
            elif content_end - current_start > self.chunk_size:
                pieces.append((current_start, current_end))
                current_start, current_end = content_start, content_end
            else:
                current_end = content_end
        if current_start is not None:
            pieces.append((current_start, current_end))
        return pieces or [(start, end)]

    def _hard_split(self, source: str, start: int, end: int) -> list[tuple[int, int]]:
        """Жёсткий разрез: пробел во второй половине окна, иначе безопасная граница.

        ``hard_cut`` не рвёт графемный кластер и не начинает кусок с закрывающей
        пунктуации: иначе цитата открывалась бы знаком, который относится к предыдущей
        фразе. Пробел остаётся в предыдущем куске (``space + 1``), поэтому следующий
        кусок начинается со слова, а не с разделителя. Кусок, состоящий только из
        пробелов, не создаётся: такой разрез не ставится вовсе, и остаток уходит одним
        куском (объявленный компромисс, видимый по ``over_budget``).
        """
        pieces: list[tuple[int, int]] = []
        position = start
        while end - position > self.chunk_size:
            limit = position + self.chunk_size
            space = source.rfind(" ", position + self.chunk_size // 2, limit)
            if space > position:
                cut = space + 1
            else:
                cut = hard_cut(source, position, limit)
                if cut <= position:
                    cut = position + 1
            if not source[position:cut].strip():
                cut = end
            pieces.append((position, cut))
            position = cut
        if position < end:
            pieces.append((position, end))
        return pieces

    @staticmethod
    def _chunk_id(document_id: str, no: int) -> str:
        return f"{document_id}#{no}"


def _absorb_short(
    source: str, spans: list[tuple[int, int]], min_size: int
) -> list[tuple[int, int]]:
    """Приклеивает куски короче порога к соседу — текст не выбрасывается (P1).

    Хвост короче порога — обломок разреза, а не утверждение, и как отдельный
    ``source_id`` цитаты он опасен (I22). Но удалить его нельзя: P1 требует, чтобы
    склейка возвращала вход. Поэтому короткий кусок приклеивается к **предыдущему**
    (границы после него не сдвигаются), а если он первый — к следующему: у первого
    нет предыдущего, и это единственный вынужденный сдвиг. Документ, целиком короче
    порога, остаётся одним куском.
    """
    if min_size <= 0:
        return spans
    result: list[tuple[int, int]] = []
    carry: int | None = None
    for start, end in spans:
        if carry is not None:
            start, carry = carry, None
        if len(source[start:end].strip()) < min_size:
            if result:
                result[-1] = (result[-1][0], end)
            else:
                carry = start
            continue
        result.append((start, end))
    if carry is not None:
        result.append((carry, spans[-1][1]))
    return result


def _context_prefix(text: str) -> str:
    """Первое предложение первого абзаца чанка, схлопнутое и ограниченное по длине."""
    first_block = text.strip().split("\n", 1)[0].strip()
    match = _SENTENCE_SEP_RE.search(first_block)
    prefix = first_block[: match.start()] if match else first_block
    prefix = _WHITESPACE_RE.sub(" ", prefix).strip()
    if len(prefix) > CONTEXT_PREFIX_MAX:
        prefix = prefix[:CONTEXT_PREFIX_MAX].rstrip()
    return prefix


class TextChunkerPort(TextChunker):
    """Обёртка ``TextChunker`` под контракт ``ChunkerPort`` (``chunk(doc, document_id)``).

    Зеркалит :class:`core.adapters.chunker_markdown.MarkdownChunkerPort`. Два входа не
    спорят о подписи: базовый называется :meth:`chunk_text` (``document_id``, ``text``),
    порт-вход — ``chunk`` (``ExtractedDoc``, ``document_id``), поэтому переопределения
    с другой сигнатурой здесь нет. ``document_id`` — стабильный id документа, поэтому
    ``chunk_id`` детерминирован (``f"{document_id}#{no}"``).
    """

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        """Порт-путь: ``ExtractedDoc`` + ``document_id`` → абзацные чанки."""
        return self.chunk_text(document_id, doc.text)