"""Карта страниц: какой отрезок текста принадлежит какой странице документа.

Зачем это отдельный модуль. Номер страницы — свойство **документа** (читатель знает, где
кончилась страница), а нужен он **чанку** (цитата обязана назвать страницу). Хранится
только первое: :attr:`~core.ports.types.ExtractedDoc.page_map` — упорядоченный набор
полуоткрытых отрезков ``(page_no, char_start, char_end)`` в тексте документа. Номер
страницы чанка — **функция** от этой карты и ``char_start`` (:func:`page_number_for`), а
не колонка.

Почему функция, а не колонка ``corpus_chunks.page_number``. Колонка — это копия,
которую обязан поддерживать **каждый** путь записи чанка: основной ингест, члены
контейнеров, переиспользование, перенос состояния, пере-нарезка. Один забытый путь даёт
молча **неверный** номер, и отличить его от верного нечем — ровно тот класс отказа, ради
устранения которого карта и заведена. Функция же не может «отстать»: она считается из
тех же двух чисел, что уже лежат в БД.

Почему смещения — в пространстве **чанкера**. ``char_start`` чанка адресует текст
документа ровно в том виде, в каком он ушёл в нарезку
(:func:`~core.util.sanitize.finalize_extracted_text` снимает ведущий BOM). Если бы карта
хранила смещения «как их отдал читатель», документ с BOM сдвинул бы **каждую** страницу
на единицу — молча и на всём документе. Поэтому читатель сам сдвигает карту в
пространство чанкера (см. ``core/adapters/extract_pdf_text.py``), и обе стороны говорят
об одном тексте.

``None`` — читатель карту **не дал** (сегодня так делают все механизмы, кроме
``text_layer``). Это не «страниц нет» и не «одна страница»: выдуманный номер хуже
отсутствующего (A3), поэтому отсутствие называется отсутствием.

Нейтрален (§1.3): здесь нет предметных терминов — только числа и интервалы.
"""

from __future__ import annotations

import json
from typing import Any

__all__ = [
    "PageMap",
    "check_page_map",
    "page_map_from_json",
    "page_map_to_json",
    "page_number_for",
]

#: Карта страниц: ``(page_no 1-based, char_start, char_end)``, полуоткрытые отрезки,
#: отсортированы по номеру страницы и не пересекаются. Контракт координат — тот же, что
#: у :attr:`~core.ports.types.ExtractedDoc.page_map` (пространство чанкера, без BOM).
PageMap = tuple[tuple[int, int, int], ...]


def check_page_map(
    page_map: PageMap, text_length: int | None, *, label: str = "ExtractedDoc"
) -> None:
    """Проверяет инвариант карты страниц (см. :attr:`ExtractedDoc.page_map`).

    Карта — провенанс, а не оценка: сегменты обязаны идти по возрастанию номера страницы,
    не пересекаться, лежать внутри ``text`` и не быть вывернутыми (``end < start``).
    Нарушение — не «странная карта», а ложная запись: по ней чанк получил бы **номер
    страницы**, которого не было, и отличить это от настоящего номера было бы нечем.

    ``text_length`` — длина текста, внутри которого обязаны лежать отрезки; ``None`` —
    длина неизвестна (читаем из БД, где текста нет), и тогда проверяется только структура
    и порядок. ``label`` — префикс сообщения: у одного и того же инварианта два носителя
    (``ExtractedDoc`` и строка ``corpus_docs``), и сообщение обязано называть тот, который
    действительно нарушен. Один валидатор на два носителя намеренно: две копии разошлись
    бы молча.

    :raises ValueError: карта нарушает инвариант.
    """
    previous_page = 0
    previous_end = 0
    for entry in page_map:
        if not isinstance(entry, tuple) or len(entry) != 3:
            raise ValueError(
                f"{label}: элемент page_map не является тройкой "
                f"(page_no, char_start, char_end): {entry!r}"
            )
        page_no, start, end = entry
        if any(isinstance(value, bool) or not isinstance(value, int) for value in entry):
            raise ValueError(
                f"{label}: page_map содержит не целое число — смещения и номер "
                f"страницы обязаны быть int: {entry!r}"
            )
        if page_no < 1:
            raise ValueError(
                f"{label}: page_map с номером страницы < 1 — нумерация страниц "
                f"1-based: {entry!r}"
            )
        if start < 0 or end < start:
            raise ValueError(
                f"{label}: page_map с отрицательным или вывернутым интервалом — "
                f"нужен 0 <= char_start <= char_end: {entry!r}"
            )
        if text_length is not None and end > text_length:
            raise ValueError(
                f"{label}: page_map выходит за текст — char_end "
                f"{end} > len(text) {text_length}: {entry!r}"
            )
        if page_no <= previous_page or start < previous_end:
            raise ValueError(
                f"{label}: page_map не отсортирована или сегменты пересекаются — "
                f"страница {page_no} начинается в {start} после {previous_end}"
            )
        previous_page, previous_end = page_no, end


def page_number_for(char_start: int | None, page_map: PageMap | None) -> int | None:
    """Номер страницы для смещения ``char_start`` (``None`` — назвать нельзя).

    Правило ровно одно и то же для всех потребителей:

    * смещение внутри отрезка ``[char_start, char_end)`` — страница этого отрезка;
    * смещение в **разрыве** (``\\n\\n`` между страницами) — **следующая** страница:
      разделитель принадлежит той странице, которая начинается после него;
    * смещение **до** первого отрезка — первая страница, **за** последним — последняя:
      текст вне карты не должен получать «нет страницы» там, где страница очевидна;
    * **пустой отрезок** (``start == end``) — прочитанная страница без текста: он не
      содержит ни одного смещения, поэтому ``char_start``, равный его позиции, уходит
      **следующему** отрезку (а если следующего нет — предыдущему, то есть последней
      странице); смещение **до** его позиции принадлежит ему — как и любой разрыв;
    * ``None`` на входе (нет смещения либо читатель не дал карту) → ``None``. Пустая
      карта читается как отсутствующая: «страниц не названо» — это не «ноль страниц».

    Реализация — одно сравнение с ``char_end``: отрезки отсортированы и не пересекаются,
    поэтому первое ``char_start < char_end`` и есть ответ (для пустого отрезка условие
    ложно, и он пропускается сам — ровно по правилу выше).
    """
    if char_start is None or not page_map:
        return None
    last_page: int | None = None
    for page_no, _start, end in page_map:
        if char_start < end:
            return page_no
        last_page = page_no
    return last_page


def page_map_to_json(page_map: PageMap | None) -> str | None:
    """Карта → текст колонки ``corpus_docs.page_map``; отсутствие карты → ``None``.

    ``None`` (и пустая карта) пишутся как ``NULL``, а **не** как ``''``: пустая строка
    читалась бы как «карта есть и она пуста», то есть как утверждение о страницах.
    Формат — JSON-список троек ``[page_no, start, end]``: он читается глазами в
    ``litecli`` и не требует схемы.
    """
    if not page_map:
        return None
    return json.dumps(
        [[page_no, start, end] for page_no, start, end in page_map],
        separators=(",", ":"),
    )


def page_map_from_json(raw: Any) -> PageMap | None:
    """Текст колонки ``corpus_docs.page_map`` → карта; всё непонятное → ``None``.

    Порча колонки читается как «карты нет», а **не** как исключение: ``report`` обязан
    строиться и на повреждённой БД, а номер страницы по испорченной карте был бы
    догадкой (A3). Поэтому здесь нет ни одного ``raise``: не список, не тройки, не
    целые, вывернутый интервал, нарушенный порядок — всё ``None``.

    Длина текста при чтении неизвестна (в строке корпуса текста нет), поэтому проверяется
    структура и порядок (:func:`check_page_map` с ``text_length=None``); верхнюю границу
    держит валидатор :class:`~core.domain.models.CorpusDoc` по ``extracted_chars``.
    """
    if not isinstance(raw, str) or not raw:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, list) or not payload:
        return None
    entries: list[tuple[int, int, int]] = []
    for item in payload:
        if not isinstance(item, (list, tuple)) or len(item) != 3:
            return None
        entries.append((item[0], item[1], item[2]))
    result: PageMap = tuple(entries)
    try:
        check_page_map(result, None, label="corpus_docs")
    except ValueError:
        return None
    return result
