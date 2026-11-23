"""Класс **работы** конверсии: из строки очереди — в цену документа (F0).

**Зачем отдельный модуль.** До этого ширина окна потребителя была одна на все документы
продукта, и цена работы выводилась из **объявленного уровня PDF-корня**, а не из
фактического артефакта: HTML-страница (медиана 0.21 s и 90 MB) стояла в одном FIFO с PDF
``layout_tables`` (7.2 s и 3679 MB пика), поэтому 11.9 ядра из 12 простаивали, а рециклинг
воркера каждые 25 задач перезагружал модели docling перед следующим PDF. Замер —
``docs/reports/measurements/2026-11-07-work-class-peaks.md``.

**Класс — цена, а не методология.** Он не входит в ``run_id`` (как и ``extract_workers``:
число одновременных исполнителей — расписание, а не условие измерения) и не меняет ни
статусов I1–I23, ни состава корпуса. Он решает ровно одно: в какую полосу расписания
попадёт строка и каким окном эта полоса ограничена.

**Порядок правил неслучаен** (он же таблица теста ``test_work_class.py``): контейнер
сильнее всего (архив — не документ, его байты уходят в спуск), затем PDF (механизм задан
осью ``crawl.pdf_pipeline``), затем Office (docling без уровня/устройства). Остальные
исходы — ``model_free``: фолбэк композита при неизвестном типе — HTML
(``core/cli/wiring.py:build_worker_extractor``), поэтому «неизвестный ``Content-Type``»,
текстовое семейство и EPUB стоят одинаково — без моделей.

**Классификация по объявленному типу, а не по байтам.** Функция чистая (без I/O) и
вызывается до извлечения; магия байтов здесь не читается — её решает адаптер. Расхождение
возможно только при противоречии «суффикс против ``Content-Type``» (``notes.txt``,
отданный как ``application/pdf``): тогда класс берётся по более **тяжёлому** сигналу, то
есть окно выходит меньше, а не больше — направление ошибки «на одного воркера меньше».

Нейтрален (§1.3): форматы и MIME — данные, не вендоры.
"""

from __future__ import annotations

from core.domain.enums import DocumentLevel, PdfPipeline, WorkClass
from core.util.document_formats import (
    ATTACHMENT_SUFFIXES,
    OFFICE_SUFFIXES,
    PDF_SUFFIX,
    document_content_type,
    document_suffix,
    is_attachment_content_type,
    normalize_mime,
    path_suffix,
)

__all__ = ["work_class_for"]

#: MIME контейнеров **сверх** реестра: ``application/x-tar``. Класс работы отвечает на
#: вопрос «сколько стоит байт», а не «допущен ли он»: допуск решает
#: ``crawl.attachment_formats``, и tar в объявленный набор попасть не может
#: (:data:`core.util.document_formats.SUPPORTED_ARCHIVE_FORMATS` его не содержит).
#: Поэтому лишняя строка здесь не расширяет вселенную корпуса — она только честно
#: называет цену архива, у которого нет читателя.
_EXTRA_CONTAINER_MIME: frozenset[str] = frozenset(
    {"application/x-tar", "application/tar"}
)


def work_class_for(
    *,
    content_type: str | None,
    url: str,
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline,
) -> WorkClass:
    """Класс работы строки очереди: цена документа по объявленному типу.

    :param content_type: ``Content-Type`` ответа (``None`` — заголовка нет: так приходят
        строки прошлых исполнений и суффикс-less эндпоинты).
    :param url: адрес документа; у члена контейнера — ``{archive}!/{member}``, и суффикс
        берётся по члену (это и есть его формат).
    :param level: объявленный уровень документной экстракции (``crawl.document_level``).
        На **класс** не влияет: он задаёт множитель цены внутри класса
        (``DOCLING_PEAK_MB[level]``), а не класс.
    :param pdf_pipeline: объявленный механизм PDF (``crawl.pdf_pipeline``): ``text_layer``
        — pdfium без моделей, ``structured`` — docling.

    Порядок правил (он же тест-таблица):

    1. контейнер (MIME ``zip``/``7z``/``rar``/``x-tar`` или суффикс ``.zip``/``.7z``/``.rar``)
       → :attr:`WorkClass.CONTAINER`;
    2. ``application/pdf`` или суффикс ``.pdf`` → :attr:`WorkClass.PDF_TEXT_LAYER` при
       ``pdf_pipeline == text_layer``, иначе :attr:`WorkClass.PDF_STRUCTURED`;
    3. офисный суффикс/тип (``docx``/``pptx``/``xlsx``) → :attr:`WorkClass.OFFICE`;
    4. всё остальное → :attr:`WorkClass.MODEL_FREE`: HTML и XHTML (страница), пустой или
       неизвестный тип (фолбэк композита — HTML), текстовое семейство
       (``.txt``/``.log``/``.sql``/``.rst``/markdown, ``text/plain``, ``text/markdown``),
       EPUB (``.epub``/``application/epub+zip``). Эти четыре исхода брифа сходятся к одному
       классу намеренно: цена у них одна — читатели без моделей, — и разные метки только
       раздробили бы полосу без выигрыша в памяти.
    """
    del level  # уровень меняет множитель цены внутри класса, а не класс
    mime = normalize_mime(content_type)
    suffix = path_suffix(url)
    # 1. Контейнер: байты уходят в спуск, извлекают его члены — своим классом.
    if (
        mime in _EXTRA_CONTAINER_MIME
        or is_attachment_content_type(content_type)
        or suffix in ATTACHMENT_SUFFIXES
    ):
        return WorkClass.CONTAINER
    # 2. PDF: механизм объявлен осью, поэтому класс выбирает она, а не суффикс.
    #    ``document_content_type`` отдаёт суффикс по MIME — один источник с
    #    ``document_suffix``, вторая таблица не заводится.
    document = document_suffix(url) or document_content_type(content_type)
    if document == PDF_SUFFIX:
        return (
            WorkClass.PDF_TEXT_LAYER
            if pdf_pipeline is PdfPipeline.TEXT_LAYER
            else WorkClass.PDF_STRUCTURED
        )
    # 3. Office: docling, но без уровня и устройства — своя цена.
    if document in OFFICE_SUFFIXES:
        return WorkClass.OFFICE
    # 4. HTML / текст / EPUB / неизвестный тип — читатели без моделей. Отдельного списка
    #    HTML-сигналов здесь нет намеренно: фолбэк композита — HTML, поэтому «страница» и
    #    «неизвестный тип» стоят одинаково, и различающая ветка не изменила бы ни класс,
    #    ни окно, зато завела бы вторую таблицу форматов рядом с реестром.
    return WorkClass.MODEL_FREE
