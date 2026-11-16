"""Структурные блоки документа: адрес фрагмента, а не только смещение (O36 фаза 2).

Зачем отдельный носитель. До этой правки документ описывался двумя вещами: **текстом** и
(после фазы 1) картой страниц. Этого хватает цитате, но не хватает структуре: «этот абзац —
пункт такого-то списка», «этот текст собран моделью из раскладки, а не прочитан слоем» —
сказать было нечем. Блок добавляет ровно три факта, которых не было: **вид** фрагмента
(:data:`BLOCK_KINDS`), **порядок чтения** и **происхождение текста** (:data:`TEXT_SOURCES`).

**Признак ``read|reconstructed`` — главное в этом модуле.** Механизм ``text_layer`` читает
текстовый слой как есть: текст страницы прочитан (:data:`TEXT_SOURCE_READ`). Механизм
``structured`` реконструирует раскладку моделью: даже границы страниц там — результат
работы движка (:data:`TEXT_SOURCE_RECONSTRUCTED`). Без этого признака «реконструкция»
неотличима от факта, и цитата, собранная моделью, выглядела бы дословной (A3).

Почему ``block_id`` — нумерация, а не хеш. ``b0``/``b1``/… по позиции в документе
детерминированы при пере-извлечении того же текста тем же механизмом, а хеш содержимого
менялся бы от любой правки блока — то есть адрес «переезжал» бы вместе с данными, и
«блок b3 в прошлом цикле» перестала бы значить что-либо. Идентичность здесь — место в
документе, а не содержимое (то же решение, что у ``table_id``).

Почему у чанка **нет** колонки ``block_id``. Ровно по той же причине, по которой нет
``page_number`` (:mod:`core.domain.page_map`): копию обязан поддерживать каждый путь записи
чанка — основной ингест, члены контейнеров, переиспользование, перенос состояния,
пере-нарезка. Один забытый путь даёт молча **неверный** адрес, и отличить его от верного
нечем. Поэтому ``block_id`` чанка — функция от ``(blocks документа, char_start чанка)``
(:func:`block_id_for`), а публикуется она там же, где страница
(:mod:`core.pipeline.page_publication`).

``None`` и пустой кортеж — **разные факты**, и смешивать их нельзя (A3):

* ``None`` — читатель блоков **не даёт вовсе** (HTML, текст/markdown, EPUB, Office,
  шов ``split``, docling — см. докстринг ``extract_document``);
* ``()`` — читатель блоков смотрел и блоков не дал (документ без содержимого).

Различие то же, что у таблиц (:class:`core.domain.doc_tables.ExtractedTable`), и оно же
делает честной фразу «блоков нет» вместо «мы их не искали».

Инвариант (:func:`check_blocks`): адреса уникальны и совпадают с позицией, отрезки
полуоткрытые и лежат внутри текста, порядок строго возрастает, родитель существует и идёт
**раньше** ребёнка. Нарушение — отказ модели, а не «странный блок»: по ложной записи
цитата получила бы адрес, которого не было.

Нейтрален (§1.3): здесь нет предметных терминов — только виды фрагментов и числа.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

__all__ = [
    "BLOCK_KINDS",
    "BLOCK_KIND_CODE",
    "BLOCK_KIND_HEADING",
    "BLOCK_KIND_IMAGE",
    "BLOCK_KIND_LIST",
    "BLOCK_KIND_OTHER",
    "BLOCK_KIND_PAGE",
    "BLOCK_KIND_PARAGRAPH",
    "BLOCK_KIND_TABLE",
    "TEXT_SOURCES",
    "TEXT_SOURCE_READ",
    "TEXT_SOURCE_RECONSTRUCTED",
    "BlockList",
    "ExtractedBlock",
    "block_id_for",
    "blocks_from_json",
    "blocks_to_json",
    "check_blocks",
]

#: Страница целиком: граница, которую знает **любой** постраничный читатель, даже если об
#: абзацах он не сообщает ничего (``text_layer``/pdfium). Это честный минимум структуры.
BLOCK_KIND_PAGE = "page"
#: Заголовок раздела.
BLOCK_KIND_HEADING = "heading"
#: Абзац обычного текста.
BLOCK_KIND_PARAGRAPH = "paragraph"
#: Таблица (объектом она живёт отдельно — :class:`core.domain.doc_tables.ExtractedTable`).
BLOCK_KIND_TABLE = "table"
#: Блок кода / дословный фрагмент.
BLOCK_KIND_CODE = "code"
#: Список (нумерованный или маркированный) как целое.
BLOCK_KIND_LIST = "list"
#: Изображение (текста в нём нет; блок адресует место).
BLOCK_KIND_IMAGE = "image"
#: Вид, который читатель различает, но не относит ни к одному из объявленных.
BLOCK_KIND_OTHER = "other"

#: Объявленный набор видов блока. Новый вид — это новое значение здесь, а не строка в
#: адаптере: набор обязан быть закрытым, иначе «вид» перестаёт быть проверяемым фактом.
BLOCK_KINDS: tuple[str, ...] = (
    BLOCK_KIND_PAGE,
    BLOCK_KIND_HEADING,
    BLOCK_KIND_PARAGRAPH,
    BLOCK_KIND_TABLE,
    BLOCK_KIND_CODE,
    BLOCK_KIND_LIST,
    BLOCK_KIND_IMAGE,
    BLOCK_KIND_OTHER,
)

#: Текст блока **прочитан** как есть (текстовый слой файла либо разметка страницы).
TEXT_SOURCE_READ = "read"
#: Текст блока **собран** моделью из раскладки (реконструкция, а не чтение).
TEXT_SOURCE_RECONSTRUCTED = "reconstructed"

#: Объявленный набор происхождений текста — ровно два, и третий не заводится:
#: «прочитано», «реконструировано» и ничего между ними. Промежуточное значение
#: («прочитано, но поправлено») было бы утверждением, которое читатель не делает.
TEXT_SOURCES: tuple[str, ...] = (TEXT_SOURCE_READ, TEXT_SOURCE_RECONSTRUCTED)


@dataclass(frozen=True)
class ExtractedBlock:
    """Фрагмент документа так, как его разметил читатель (O36 фаза 2 плана ``26``).

    ``block_id`` — адрес в документе: ``"b0"``, ``"b1"``, … по позиции в списке. Нумерация,
    а не хеш содержимого: адрес не должен переезжать от правки текста (см. докстринг
    модуля).

    ``kind`` — вид фрагмента из :data:`BLOCK_KINDS`.

    ``page_no`` — страница (**1-based**), к которой читатель относит блок; ``None`` —
    читатель страницу не дал. ``0`` не бывает: «страница 0» — не факт, а выдумка.

    ``char_start``/``char_end`` — **полуоткрытый** отрезок ``[char_start, char_end)`` в
    пространстве **чанкера** (текст документа без ведущего BOM — тот же контракт, что у
    :attr:`core.ports.types.ExtractedDoc.page_map`). ``char_start == char_end`` — блок
    прочитан, текста в нём нет (пустая страница, изображение без подписи): это факт, а не
    разрыв.

    ``bbox`` — рамка фрагмента на странице ``(left, top, right, bottom)`` в координатах
    читателя; ``None`` — читатель координат не даёт (pdfium не даёт, HTML не даёт).
    Выдумывать координаты нельзя: они выглядят проверяемыми, но не проверяемы.

    ``parent_id`` — ``block_id`` объемлющего блока (``None`` — верхний уровень). Родитель
    обязан идти в документе **раньше** ребёнка: иначе адрес родителя ещё не существует в
    момент, когда он нужен.

    ``order`` — порядок **чтения** (0-based, строго возрастает). Это не позиция в файле:
    у двухколоночной вёрстки они расходятся, и именно поэтому поле нужно отдельно.

    ``text_source`` — :data:`TEXT_SOURCE_READ` либо :data:`TEXT_SOURCE_RECONSTRUCTED`
    (см. докстринг модуля: без этого признака реконструкция неотличима от факта).
    """

    block_id: str
    kind: str
    char_start: int
    char_end: int
    order: int
    text_source: str
    page_no: int | None = None
    bbox: tuple[float, float, float, float] | None = None
    parent_id: str | None = None


#: Список блоков документа: ``()`` — читатель смотрел и не дал блоков, ``None`` — не даёт
#: вовсе. Контракт различия — в :attr:`core.ports.types.ExtractedDoc.blocks`.
BlockList = tuple[ExtractedBlock, ...]


def check_blocks(
    blocks: BlockList, text_length: int | None, *, label: str = "ExtractedDoc"
) -> None:
    """Проверяет инвариант списка блоков (см. :class:`ExtractedBlock`).

    Блоки — провенанс, а не оценка: адрес уникален и совпадает с позицией, отрезок не
    вывернут и лежит внутри ``text``, порядок строго возрастает, родитель существует и
    идёт раньше ребёнка, вид и происхождение — из объявленных наборов. Нарушение — не
    «странный блок», а ложная запись: по ней цитата получила бы адрес фрагмента, которого
    не было, и отличить это от настоящего адреса было бы нечем.

    ``text_length`` — длина текста, внутри которого обязаны лежать отрезки; ``None`` —
    длина неизвестна (читаем из БД, где текста нет), и тогда проверяются только структура
    и порядок. ``label`` — префикс сообщения: у одного инварианта два носителя
    (``ExtractedDoc`` и строка ``corpus_docs``), и сообщение обязано называть тот, который
    действительно нарушен. Один валидатор на два носителя намеренно: две копии разошлись
    бы молча.

    Пересечение отрезков **не** запрещено: вложенность (раздел внутри страницы, абзац
    внутри раздела) — это и есть структура, ради которой блоки заведены.

    :raises ValueError: список блоков нарушает инвариант.
    """
    seen: dict[str, ExtractedBlock] = {}
    previous_order: int | None = None
    for index, block in enumerate(blocks):
        if not isinstance(block, ExtractedBlock):
            raise ValueError(
                f"{label}: элемент blocks не является ExtractedBlock: {block!r}"
            )
        expected_id = f"b{index}"
        if block.block_id != expected_id:
            raise ValueError(
                f"{label}: block_id {block.block_id!r} не совпадает с позицией в списке "
                f"— адрес обязан быть {expected_id!r} (детерминированная нумерация)"
            )
        if block.block_id in seen:
            raise ValueError(f"{label}: block_id {block.block_id!r} повторяется")
        if block.kind not in BLOCK_KINDS:
            raise ValueError(
                f"{label}: неизвестный вид блока {block.kind!r} — объявлены "
                f"{list(BLOCK_KINDS)}"
            )
        if block.text_source not in TEXT_SOURCES:
            raise ValueError(
                f"{label}: неизвестное происхождение текста {block.text_source!r} — "
                f"объявлены {list(TEXT_SOURCES)}"
            )
        if block.page_no is not None and (
            isinstance(block.page_no, bool)
            or not isinstance(block.page_no, int)
            or block.page_no < 1
        ):
            raise ValueError(
                f"{label}: page_no обязан быть >= 1 или None — нумерация страниц "
                f"1-based, получено {block.page_no!r} ({block.block_id})"
            )
        for name, value in (
            ("char_start", block.char_start),
            ("char_end", block.char_end),
            ("order", block.order),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(
                    f"{label}: {name} обязан быть int, получено {value!r} "
                    f"({block.block_id})"
                )
        if block.char_start < 0 or block.char_end < block.char_start:
            raise ValueError(
                f"{label}: блок с отрицательным или вывернутым интервалом — нужен "
                f"0 <= char_start <= char_end: {block.block_id} "
                f"({block.char_start}, {block.char_end})"
            )
        if text_length is not None and block.char_end > text_length:
            raise ValueError(
                f"{label}: блок выходит за текст — char_end {block.char_end} > "
                f"len(text) {text_length}: {block.block_id}"
            )
        if block.order < 0:
            raise ValueError(
                f"{label}: order отрицателен — порядок чтения 0-based: {block.block_id}"
            )
        if previous_order is not None and block.order <= previous_order:
            raise ValueError(
                f"{label}: order не возрастает — {block.block_id} идёт с {block.order} "
                f"после {previous_order}; порядок чтения обязан быть строго монотонным"
            )
        if block.bbox is not None:
            box = block.bbox
            if not isinstance(box, tuple) or len(box) != 4:
                raise ValueError(
                    f"{label}: bbox обязан быть четвёркой (left, top, right, bottom) "
                    f"или None: {block.block_id}"
                )
            if any(
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                for value in box
            ):
                raise ValueError(
                    f"{label}: bbox содержит не конечное число — координаты обязаны быть "
                    f"конечными (NaN/бесконечность не позиция): {block.block_id}"
                )
        if block.parent_id is not None:
            parent = seen.get(block.parent_id)
            if parent is None:
                raise ValueError(
                    f"{label}: parent_id {block.parent_id!r} ссылается на блок, которого "
                    f"нет либо который идёт позже — родитель обязан существовать и "
                    f"стоять раньше ребёнка ({block.block_id})"
                )
        seen[block.block_id] = block
        previous_order = block.order


def block_id_for(char_start: int | None, blocks: BlockList | None) -> str | None:
    """``block_id`` блока, содержащего смещение ``char_start`` (``None`` — назвать нельзя).

    Правило — **то же**, что у номера страницы (:func:`core.domain.page_map.page_number_for`),
    и это не совпадение: обе функции отвечают на один вопрос «какому отрезку принадлежит
    смещение», поэтому второе правило было бы вторым ответом на один вопрос.

    * смещение внутри отрезка ``[char_start, char_end)`` — этот блок;
    * смещение в **разрыве** между блоками — **следующий** блок: разделитель принадлежит
      тому блоку, который начинается после него;
    * смещение **до** первого блока — первый блок, **за** последним — последний: текст вне
      списка не должен получать «нет блока» там, где блок очевиден;
    * **пустой отрезок** (``char_start == char_end``) не содержит ни одного смещения,
      поэтому смещение на его позиции уходит **следующему** блоку (а если следующего нет —
      предыдущему);
    * ``None`` на входе (нет смещения либо читатель блоков не дал) → ``None``. Пустой
      список читается как отсутствующий: «блоков не названо» — это не «ноль блоков».

    Реализация — одно сравнение с ``char_end``: отрезки идут в порядке чтения, поэтому
    первое ``char_start < char_end`` и есть ответ (пустой блок условие не выполняет и
    пропускается сам — ровно по правилу выше).

    Вложенность разрешается порядком: родитель стоит раньше ребёнка, поэтому смещение
    внутри обоих называет **родителя** — самый общий блок, который его содержит. Для
    плоского списка (``text_layer``) это ровно «свой» блок.
    """
    if char_start is None or not blocks:
        return None
    last_id: str | None = None
    for block in blocks:
        if char_start < block.char_end:
            return block.block_id
        last_id = block.block_id
    return last_id


def blocks_to_json(blocks: BlockList | None) -> str | None:
    """Блоки → текст колонки ``corpus_docs.blocks``; «читатель не даёт» → ``None``.

    ``None`` пишется как ``NULL``, пустой список — как ``[]``: это **разные** факты
    («не даёт» против «смотрел и не дал»), и свести их к одному значило бы потерять ровно
    тот ответ, ради которого колонка заведена. Формат — JSON-список объектов: он читается
    глазами в ``litecli`` и не требует схемы.
    """
    if blocks is None:
        return None
    return json.dumps(
        [
            {
                "block_id": block.block_id,
                "kind": block.kind,
                "char_start": block.char_start,
                "char_end": block.char_end,
                "order": block.order,
                "text_source": block.text_source,
                "page_no": block.page_no,
                "bbox": None if block.bbox is None else list(block.bbox),
                "parent_id": block.parent_id,
            }
            for block in blocks
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _required_str(item: Mapping[str, Any], field: str) -> str:
    """Обязательное строковое поле записи; отсутствие/иной тип — порча (``ValueError``)."""
    value = item.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"поле {field!r}: ожидалась непустая строка")
    return value


def _optional_str(item: Mapping[str, Any], field: str) -> str | None:
    """Необязательное строковое поле: ``None`` — не задано, иной тип — порча."""
    value = item.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"поле {field!r}: ожидалась строка или null")
    return value


def _required_int(item: Mapping[str, Any], field: str) -> int:
    """Обязательное целое поле записи; ``bool`` — не целое (иначе ``True`` станет 1)."""
    value = item.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"поле {field!r}: ожидалось целое")
    return value


def _optional_int(item: Mapping[str, Any], field: str) -> int | None:
    """Необязательное целое поле: ``None`` — не задано, иной тип — порча."""
    value = item.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"поле {field!r}: ожидалось целое или null")
    return value


def blocks_from_json(raw: Any) -> BlockList | None:
    """Текст колонки ``corpus_docs.blocks`` → блоки; всё непонятное → ``None``.

    Порча колонки читается как «читатель блоков не даёт», а **не** как исключение:
    ``report`` обязан строиться и на повреждённой БД, а блок, собранный из испорченной
    записи, был бы догадкой (A3). Поэтому здесь нет ни одного ``raise``: не список, не
    объект, нарушенный инвариант (:func:`check_blocks` с ``text_length=None``) — всё
    ``None``.

    Пустой список — **не** порча: ``[]`` означает «читатель смотрел и блоков не дал», и он
    возвращается пустым кортежем, а не ``None``.
    """
    if not isinstance(raw, str) or not raw:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, list):
        return None
    blocks: list[ExtractedBlock] = []
    for item in payload:
        if not isinstance(item, dict):
            return None
        bbox_raw = item.get("bbox")
        bbox: tuple[float, float, float, float] | None
        if bbox_raw is None:
            bbox = None
        elif isinstance(bbox_raw, (list, tuple)) and len(bbox_raw) == 4:
            bbox = tuple(float(value) for value in bbox_raw)  # type: ignore[assignment]
        else:
            return None
        try:
            blocks.append(
                ExtractedBlock(
                    block_id=_required_str(item, "block_id"),
                    kind=_required_str(item, "kind"),
                    char_start=_required_int(item, "char_start"),
                    char_end=_required_int(item, "char_end"),
                    order=_required_int(item, "order"),
                    text_source=_required_str(item, "text_source"),
                    page_no=_optional_int(item, "page_no"),
                    bbox=bbox,
                    parent_id=_optional_str(item, "parent_id"),
                )
            )
        except (TypeError, ValueError):
            return None
    result: BlockList = tuple(blocks)
    try:
        check_blocks(result, None, label="corpus_docs")
    except ValueError:
        return None
    return result
