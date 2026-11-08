"""Экстрактор документов (docling → markdown) — реализация ``ExtractorPort``.

Поддерживает PDF и Office Open XML (docx/pptx/xlsx) единообразно через docling:
- PDF — layout-aware конвертация (таблицы/фич-матрицы, §3.2; блокер PDF-теста M0);
- DOCX/PPTX/XLSX — Office-бэкенды docling (python-docx/python-pptx/openpyxl);
  для этих форматов не требуется torch/layout-модели (``SimplePipeline``).

**P5 — уровень извлечения.** Качество PDF-экстракции задаётся осью
``crawl.document_level`` (:class:`~core.domain.enums.DocumentLevel`), а не «случайным»
дефолтом пустого ``DocumentConverter()``. Уровень входит в ``version`` экстрактора
(``docling-doc-3:<level>/<device>/docling-<build>/<policy>/<finalize>``) и тем самым — в
``index_build_id`` иммутабельной сборки (§13): смена уровня не может молча дать ту же сборку.

**Офисная ветвь провенанса.** DOCX/PPTX/XLSX читает ``SimplePipeline``: ни уровня
(``document_level`` — лестница PDF-конвейера), ни устройства (``pdf_device`` — место
вычисления моделей) у него нет. Поэтому офисный документ штампуется отдельной строкой
(``docling-office-1:<policy>/<build>/<finalize>``,
:func:`docling_office_version`), а версия конкретного файла берётся :meth:`version_for`.
До 02.11.2026 DOCX объявлялся как ``docling-doc-3:layout_tables/cpu/…`` — условие
измерения, которого не было.

**Сборка движка в версии экстрактора (Д-8).** ``<build>`` — версия установленной
библиотеки ``docling``, полученная :func:`docling_engine_build` (единственный источник;
``execution_axes.extractor_stack_version`` берёт то же число тем же вызовом). Без неё
ключ ``extract_cache`` (``(content_hash, extractor_version, level)``) не отличал бы
текст, произведённый другой сборкой парсера, и прогон объявлял бы один движок, а
переиспользовал текст другого. Цена — после апгрейда docling внутри диапазона
``>=2.135,<3`` кэш один раз промахивается, а ``pb``/``corpus_docs.extractor_version``
меняются: они описывают **фактически прочитанный** текст, а не предполагаемый движок.

* ``full`` (**дефолт**) — сегодняшнее эффективное поведение: layout Heron + таблицы
  TableFormer ``ACCURATE`` + ``do_cell_matching=True`` + авто-OCR (``OcrAutoOptions``),
  ``images_scale=1.0``; энричменты выключены. Это дефолты ``DocumentConverter()``, кроме
  замеренной ручки памяти ``queue_max_size=1`` (см. :meth:`DoclingDocumentExtractor._pdf_options`:
  −44% peak RSS при байт-в-байт идентичном markdown).
* ``layout_tables`` — layout + ``TableFormer FAST`` + cell-matching, **OCR выключен**.
* ``layout`` — только layout-модель (без таблиц, без OCR).
* ``text`` — моделе-независимый ``NativePdfPipeline`` (текст + координаты; без reading
  order/заголовков/таблиц/текста сканов).
* ``off`` — документы **не извлекаются** (пропуск делает pipeline до вызова адаптера);
  прямой вызов возвращает честную ошибку, а не молчаливый пустой результат.

**Исправление документации (P5).** Прежний адаптер объявлял ``__init__(use_ocr=False)`` и
**никогда не читал** параметр, поэтому проект ошибочно заявлял «OCR выключен», тогда как
docling гонял RapidOCR. Теперь параметр удалён, а поведение (OCR включён при ``full``)
описано честно.

**OCR при ``full`` проверяется, а не предполагается.** docling выбирает OCR-движок
лениво (``OcrAutoModel``), и при несовместимой установке ``rapidocr`` все кандидаты
отвергаются: конвертер молча работает без OCR, а текст помечается ``full``. Поэтому
``_build_converter`` для ``full`` строит auto-OCR для опций уровня и требует непустой
``_engine``; иначе — :class:`OcrEngineUnavailableError` (фаза фиксирует
``parser_failed``). Уровни ``layout``/``layout_tables``/``text`` OCR выключают явно и
проверку не выполняют.

Происхождение идеи graceful-degradation (ошибка вместо падения) и способа вызова
``DocumentConverter.export_to_markdown()``: проект gag5 (`documents/parse.py`, MIT,
Copyright (c) 2024). Реализация написана заново (чистая, без доменных хвостов и мёртвых
веток gag5); расширена на Office-форматы. Нейтрален (§1.3).

Зависимость ``docling`` — необязательная и тяжёлая; импортируется лениво. Для офисных
форматов достаточно лёгкого extras ``docling-slim[format-office]`` (без torch).

**Ленивый импорт docling (замер 22.10).** ``docling`` тянет torch + transformers:
``import core.adapters.extract_document`` стоил **403 MB RSS** на процесс, а ``import
core.cli.wiring`` — **426 MB**. При этом в реальном прогоне 1038 из 1203 документов —
HTML (86%), и docling им не нужен вовсе: пул воркеров платил 403 MB за конвейер,
который 86% работы не использует. Поэтому символы docling разрешаются **при первом
фактическом извлечении** (:func:`_resolve_docling`), а импорт адаптера стоит ~38 MB
(``import core.cli.wiring`` — 77 MB вместо 426 MB; ``core.edge.__main__`` — 60 MB
вместо 410 MB). После разрешения поведение, текст и ``extractor_version`` — байт-в-байт
прежние.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Protocol

from core.domain.enums import DocumentLevel, PdfDevice
from core.ports.types import ExtractedDoc
from core.util.document_formats import DOCUMENT_SUFFIXES, PDF_SUFFIX
from core.util.sanitize import finalize_extracted_text

#: docling-символы как модульные атрибуты (публичная поверхность адаптера).
#:
#: ``None`` — «ещё не разрешено» (импорт при первом извлечении, :func:`_resolve_docling`);
#: непустое значение — «разрешено»: либо импортом, либо **инъекцией** (тесты подменяют
#: имена через ``monkeypatch.setattr``). Имена обязаны оставаться настоящими атрибутами
#: модуля: ``monkeypatch.setattr(extract_document, "DocumentConverter", fake)`` иначе
#: сломался бы, а module-level ``__getattr__`` вернул бы фейк лишь однажды.
_DOCLING_AVAILABLE: bool | None = None

#: Есть ли пакет ``docling`` в окружении (``find_spec``, без импорта). ``None`` — не смотрели.
_DOCLING_PRESENT: bool | None = None

DocumentConverter: Any = None
InputFormat: Any = None
PdfFormatOption: Any = None
NativePdfFormatOption: Any = None
PdfPipelineOptions: Any = None
NativePdfPipelineOptions: Any = None
TableStructureOptions: Any = None
TableFormerMode: Any = None
OcrAutoOptions: Any = None
#: P9: ``AcceleratorOptions`` — не входит в :data:`_DOCLING_SYMBOLS` (это не часть
#: контракта инъекции: тесты подменяют конвертер/опции, а устройство проверяют отдельно),
#: но заполняется тем же импортом. ``None`` — docling не импортирован (или подменён):
#: тогда устройство объявить нечем, и это видно предупреждением, а не тишиной.
AcceleratorOptions: Any = None

#: Имена, которые :func:`_resolve_docling` заполняет из docling. Порядок не важен.
_DOCLING_SYMBOLS: tuple[str, ...] = (
    "DocumentConverter",
    "InputFormat",
    "PdfFormatOption",
    "NativePdfFormatOption",
    "PdfPipelineOptions",
    "NativePdfPipelineOptions",
    "TableStructureOptions",
    "TableFormerMode",
    "OcrAutoOptions",
)

#: Семейство docling-адаптера (без per-level суффикса ``:level``). **Единственное**
#: объявление: ``core.cli.execution_axes`` берёт его отсюда (импорт адаптера уже есть в
#: composition root через ``wiring``, поэтому второй копии строки не нужно — копия
#: разошлась бы с реальностью молча). Уровень извлечения — отдельная операционная ось
#: ``crawl.document_level``, уже входящая в ``crawl_config_digest``.
DOCLING_ADAPTER_FAMILY = "docling-doc-3"

#: Семейство **офисной** ветви docling (docx/pptx/xlsx) — отдельное от PDF.
#:
#: Почему отдельное, а не «то же с другим уровнем». Офисный документ идёт через
#: ``SimplePipeline``: у него **нет** ни оси ``crawl.document_level`` (нет layout-модели,
#: таблиц-моделей и OCR — структуру даёт сам файл), ни оси ``crawl.pdf_device``
#: (моделей, которым нужен тензорный ускоритель, нет вовсе). Штамповать офисному
#: документу ``docling-doc-3:layout_tables/cpu/…`` значило бы **объявить условие
#: измерения, которого не было**: уровень ``layout_tables`` обещает восстановление
#: структуры моделями, а устройство — место их вычисления. Провенанс не вправе называть
#: исполнение, которого не было (A3), поэтому у офисной ветви своё семейство и своя
#: формула (:func:`docling_office_version`).
DOCLING_OFFICE_ADAPTER_FAMILY = "docling-office-1"

#: Префикс «сборки движка» в идентификаторах версии. Один на ``extractor_version``
#: (ключ ``extract_cache``/``pb``) и на ``extractor_stack`` (``run_id``).
_DOCLING_BUILD_PREFIX = "docling-"

#: Политика экспорта markdown — **слагаемое идентичности** текста, а не оформление.
#:
#: Замер 2026-11-01 (`docs/reports/measurements/2026-11-01-pdf-identifier-loss.md`):
#: ``export_to_markdown()`` по умолчанию экранирует подчёркивания (``_`` → ``\_``), и на
#: живом PDF это 1470 вхождений, из которых 26 уникальных имён параметров пришли в корпус
#: искажёнными (``plantuner.disable\_index``). Текст документа меняется ⇒ обязан меняться
#: ключ ``extract_cache`` и объявленный ``extractor_stack``, иначе два разных корпуса жили
#: бы под одним ``run_id`` (Д-8: ``run_id`` не вправе называть движок, которого не было).
#:
#: Значение читают **оба** носителя: :func:`docling_extractor_version` (провенанс + ключ
#: кэша) и :func:`core.cli.execution_axes.extractor_stack_version` (объявленное поле
#: контракта). Одна константа — потому что две независимые формулы разошлись бы молча.
DOCLING_MARKDOWN_POLICY = "md-plain-underscore"

#: Политика **очистки текста** на границе экстракции — отдельное слагаемое идентичности.
#:
#: Зачем отдельно от :data:`DOCLING_MARKDOWN_POLICY`. Экспортное экранирование
#: (``escape_underscores``) и пост-очистка текста (:func:`~core.util.sanitize.
#: finalize_extracted_text`: управляющие символы + нейтрализация каркаса промпт-инъекций)
#: — разные ручки с разными причинами. Слить их в одну строку значило бы, что смена одной
#: выглядит как смена другой, и разбор замера не смог бы назвать виновника.
#:
#: Сегмент входит в версию **обеих** ветвей (PDF и Office): очистка меняет ``content_hash``
#: ровно так же, как экспорт, поэтому текст, произведённый прежней очисткой, не должен
#: находиться под новым ключом ``extract_cache`` (02.11.2026: очистка не применялась на
#: документном пути вовсе — до правки версии отличать было нечего).
DOCLING_TEXT_FINALIZE = "finalize-1"

#: Имена дистрибутивов, под которыми ставится одна и та же библиотека: полный ``docling``
#: (с torch) и лёгкий ``docling-slim`` (тот же код без torch; 05-dependency-stack).
_DOCLING_DISTRIBUTIONS: tuple[str, ...] = ("docling", "docling-slim")

#: Имя статуса docling «сконвертирована только часть документа». Сравнивается по имени, а
#: не по объекту ``ConversionStatus``: адаптер не импортирует docling на уровне модуля
#: (ленивый импорт), а повторы этого перечисления в собственный enum были бы дублем
#: семантики чужой библиотеки.
#:
#: Почему это отдельная ветка, а не «всё, что не SUCCESS». ``PARTIAL_SUCCESS`` несёт
#: **готовый текст** части страниц (типичный живой случай: 665-страничный PDF, где OCR
#: падает на части страниц, или документ, не уложившийся в ``document_timeout``). Отбросив
#: его целиком, адаптер терял прочитанное: документ исчезал из корпуса, а ячейка получала
#: «материала нет» при существующем доказательстве. ``FAILURE``/``SKIPPED``/пустой текст —
#: по-прежнему ``_error`` (текста нет, частичного успеха без текста не бывает).
PARTIAL_SUCCESS_STATUS = "PARTIAL_SUCCESS"

#: Уровни, которые обещают **структуру целого** документа (layout-модель/таблицы/OCR).
#: Уровень ``text`` (``NativePdfPipeline``) структуры не обещает: там частичность означает
#: только «часть страниц не прочитана», и утверждать иное значило бы приписать уровню
#: обещание, которого он не давал.
_STRUCTURED_LEVELS: frozenset[DocumentLevel] = frozenset(
    {DocumentLevel.LAYOUT, DocumentLevel.LAYOUT_TABLES, DocumentLevel.FULL}
)


def _dist_version(dist: str) -> str | None:
    """Версия дистрибутива ``dist`` либо ``None``, если метаданные не отвечают.

    ``None`` — значение, а не тишина: вызывающий переходит к следующему имени
    дистрибутива и в итоге честно сообщает ``"unknown"`` (версия не выдумывается, A3).
    """
    try:
        return importlib.metadata.version(dist)
    except importlib.metadata.PackageNotFoundError:
        return None
    except Exception:  # noqa: BLE001 — битые метаданные не роняют резолв версии
        return None


def docling_library_version() -> str:
    """Версия установленной библиотеки ``docling`` — **единственный** источник (Д-8).

    Читаются метаданные дистрибутива (``importlib.metadata``), а не ``import docling``:
    импорт тянет torch/модели и недопустим ни на старте прогона, ни в воркере ради
    строки версии. Проверяются оба имени дистрибутива — на slim-установке версия иначе
    молча терялась бы. Отсутствие пакета — честный ``"unknown"``: версия не выдумывается
    (A3), и значение попадает в версию экстрактора как есть, а не подменяется
    правдоподобным дефолтом.
    """
    for dist in _DOCLING_DISTRIBUTIONS:
        version = _dist_version(dist)
        if version is not None:
            return version
    return "unknown"


def docling_engine_build() -> str:
    """Сборка движка: ``docling-<version установленной библиотеки>`` (Д-8).

    Единственная формулировка «сборки»: и ``extractor_version`` (ключ ``extract_cache``
    и слагаемое ``pb``), и ``extractor_stack`` (слагаемое ``run_id``) собираются из этого
    вызова. Две независимые формулы разошлись бы — а расхождение как раз и означает
    «``run_id`` называет движок, которого не было».
    """
    return f"{_DOCLING_BUILD_PREFIX}{docling_library_version()}"


def docling_extractor_version(
    level: DocumentLevel | str, pdf_device: PdfDevice | str = PdfDevice.CPU
) -> str:
    """``extractor_version`` **PDF**-документа:
    ``docling-doc-3:<level>/<device>/docling-<build>/<policy>/<finalize>``.

    Пример: ``docling-doc-3:full/cuda/docling-2.135.0/md-plain-underscore/finalize-1``.
    Уровень кодирует ось ``crawl.document_level`` (P5), устройство — ``crawl.pdf_device``
    (P9), сборка — фактически установленную библиотеку, политика — экспорт
    (:data:`DOCLING_MARKDOWN_POLICY`), финализация — очистку текста
    (:data:`DOCLING_TEXT_FINALIZE`). Все слагаемые входят в ключ ``extract_cache``
    (``(content_hash, extractor_version, level)``): после апгрейда docling, смены
    устройства, политики экспорта **или очистки** старый текст получается только под
    **старым** ключом и потому не находится.

    Почему устройство обязательно в ключе (Д-8 в новом виде). Текст документа не
    бит-в-бит независим от устройства: layout/таблицы считаются тензорными ядрами, и
    численные различия меняют разметку на границах. Без сегмента устройства CUDA-прогон
    нашёл бы в кэше CPU-текст под тем же ключом — то есть ``stage=extract`` вернул бы
    текст, которого объявленное устройство не производило. Это ровно тот класс, ради
    которого в ключе лежит сборка движка.

    **Офисные документы этой версией не штампуются** (у них нет ни уровня, ни
    устройства): см. :func:`docling_office_version`.

    ``pdf_device`` принимает и ``str`` (значение оси из конфига) — нормализуется через
    :class:`PdfDevice`, поэтому два пути не могут дать разные строки для одного входа.
    """
    device = PdfDevice(pdf_device)
    return (
        f"{DOCLING_ADAPTER_FAMILY}:{DocumentLevel(level).value}/"
        f"{device.value}/{docling_engine_build()}/{DOCLING_MARKDOWN_POLICY}/"
        f"{DOCLING_TEXT_FINALIZE}"
    )


def docling_office_version() -> str:
    """``extractor_version`` **офисного** документа: ``docling-office-1:<policy>/<build>/<finalize>``.

    Пример: ``docling-office-1:md-plain-underscore/docling-2.135.0/finalize-1``.

    **Почему в строке нет уровня и устройства.** DOCX/PPTX/XLSX читает офисный бэкенд
    docling (python-docx/python-pptx/openpyxl) через ``SimplePipeline``: структуру
    (заголовки, таблицы, списки) отдаёт сам файл, моделей нет. Следовательно:

    * ``crawl.document_level`` (``off|text|layout|layout_tables|full``) — лестница
      качества **PDF**-конвейера: layout-модель, таблицы TableFormer, OCR. Ни одна из
      этих ступеней офисного документа не касается, и повторять их в штампе значило бы
      объявить измерение, которого не проводилось (живой дефект: до 02.11.2026 DOCX
      штамповался как ``docling-doc-3:layout_tables/cpu/…``);
    * ``crawl.pdf_device`` — место вычисления моделей. Моделей нет, считать нечего:
      сегмент ``cpu``/``cuda`` описывал бы устройство, которое не участвовало.

    Остаются слагаемые, которые действительно влияют на текст: сборка движка
    (:func:`docling_engine_build` — версия установленной библиотеки), политика экспорта
    (:data:`DOCLING_MARKDOWN_POLICY`) и финализация (:data:`DOCLING_TEXT_FINALIZE`).
    Семейство ``docling-office-1`` само по себе — версия схемы штампа: если офисный путь
    когда-нибудь обзаведётся своими осями, штамм сменит семейство, а не притворится PDF.

    Функция не принимает аргументов намеренно: осей нет, а «необязательный» аргумент
    уровня приглашал бы передать его и тем вернуть ложь в провенанс.
    """
    return (
        f"{DOCLING_OFFICE_ADAPTER_FAMILY}:{DOCLING_MARKDOWN_POLICY}/"
        f"{docling_engine_build()}/{DOCLING_TEXT_FINALIZE}"
    )


class _DoclingResult(Protocol):
    """Поверхность результата конвертации, которую берёт адаптер.

    docling — опциональный extra: его типы нельзя импортировать на уровне модуля, а
    ``object`` запрещал единственное обращение ``result.document``. Протокол объявляет
    ровно то, что используется, и проверяется структурно.
    """

    @property
    def document(self) -> Any: ...

    #: Статус конвертации (``ConversionStatus`` docling). ``Any``: у значения читается
    #: ``.name`` (или берётся ``str``), а сам enum принадлежит опциональной библиотеке.
    @property
    def status(self) -> Any: ...


class _DoclingConverter(Protocol):
    """Поверхность ``DocumentConverter``: один метод ``convert(path)``.

    Остальное (``status``, ``input.page_count``, ``pages``) читается через ``getattr``
    в :func:`_document_text`/:func:`_page_count` — там ``object`` и ``getattr`` уместны,
    потому что поле может отсутствовать у другой версии docling.
    """

    def convert(self, path: str) -> _DoclingResult: ...


class _DoclingDocument(Protocol):
    """Поверхность ``DoclingDocument``, которую берёт адаптер: экспорт в markdown.

    Объявлена ровно одна операция: ``export_to_markdown`` с аргументами-политиками
    (``escape_underscores``, ``page_no``). Полная подпись docling здесь не повторяется —
    она принадлежит опциональной библиотеке и менялась между версиями; ``**kwargs``
    фиксирует вызов, а не чужой контракт, и адаптер передаёт только те два аргумента,
    которые объявлены его собственными политиками.

    ``getattr``-поля (``pages``/``groups``) здесь не объявлены намеренно: они читаются
    через ``getattr`` в :func:`_sheet_sections`, потому что у разных версий docling их
    может не быть вовсе.
    """

    #: Только те два аргумента, которыми адаптер управляет через свои политики
    #: (:data:`DOCLING_MARKDOWN_POLICY` и постраничный экспорт листов книги).
    def export_to_markdown(
        self, *, escape_underscores: bool = True, page_no: int | None = None
    ) -> str: ...


def _document_text(document: _DoclingDocument) -> str:
    """Текст документа: markdown **через объявленные политики** — экспорт и очистку.

    **Экспорт.** Экранирование подчёркиваний снято (``escape_underscores=False``): иначе
    ``_`` в именах параметров приходит как ``\\_`` и доходит до чанков и ``citation_quote``
    (I22 требует непустую цитату, а точное сопоставление термина с цитатой на ``\\_``
    ломается). Замер и разбор — :data:`DOCLING_MARKDOWN_POLICY`.

    ``escape_html`` намеренно оставлен дефолтным: его искажения (``&amp;``) на замере дали
    два токена, отдельного решения по ним нет, а менять поведение «заодно» — это менять
    текст корпуса без измерения.

    **Очистка** (:func:`~core.util.sanitize.finalize_extracted_text`,
    :data:`DOCLING_TEXT_FINALIZE`) — та же единая граница, что у HTML/текстового путей:
    управляющие символы снимаются, каркас промпт-инъекций нейтрализуется. До 02.11.2026
    документный путь её не вызывал, и один и тот же текст доходил до оценщика помеченным
    из ``.txt`` и дословным из ``.docx`` (замер 02.11.2026).

    Обе точки извлечения (полный успех и ``PARTIAL_SUCCESS``) ходят **сюда**: политики
    экспорта и очистки обязаны быть одни, иначе частичный документ описан другим текстом,
    чем целый, и ``content_hash`` у них несоизмерим.

    **Имена листов книги (XLSX).** docling кладёт имя листа в ``doc.groups``
    (``GroupLabel.SHEET``, по группе на лист), но в markdown оно **не попадает**:
    у таблицы нет ни caption, ни SectionHeader. Поэтому имя листа — знание, которое
    адаптер обязан либо сохранить, либо не выдумывать. Сохраняется оно как заголовок
    ``## <лист>`` перед содержимым листа (:func:`_sheet_markdown`): так оно входит в
    ``heading_path`` чанкера, то есть становится видимой частью корпуса, а не теряется.
    Раскладка берётся экспортом **по странице** (``page_no=``: у XLSX страница = лист,
    у PPTX — слайд), а не нарезкой готового markdown по строкам: строковая хирургия
    разошлась бы с раскладкой движка при первом же изменении рендера. Если раскладку
    сопоставить не удалось, имена остаются видимой записью ``> sheet: <имя>`` в тексте
    и предупреждением в журнале — догадка вместо факта запрещена (A3).
    """
    markdown = document.export_to_markdown(escape_underscores=False)
    sections = _sheet_sections(document)
    if sections is not None:
        layered = _sheet_markdown(document, sections, markdown)
        if layered is not None:
            return finalize_extracted_text(layered)
        # Раскладка по листам не сошлась с целым текстом (или движок перестал понимать
        # ``page_no``): содержимое отдаём целиком, а имена листов — видимой записью, а не
        # догадкой о том, какой кусок какому листу принадлежит.
        _log.warning(
            "xlsx: имена листов не разложены по разделам (%s) — записаны в текст как "
            "«> sheet:» без привязки к фрагментам",
            ", ".join(name for _, name in sections),
        )
        listing = "\n".join(f"> sheet: {name}" for _, name in sections)
        return finalize_extracted_text(f"{listing}\n\n{markdown}")
    return finalize_extracted_text(markdown)


#: Имя ``GroupLabel``, которым офисный бэкенд помечает группу листа книги (XLSX).
_SHEET_GROUP_LABEL = "sheet"


def _sheet_sections(document: object) -> list[tuple[int, str]] | None:
    """Листы книги как ``[(номер страницы, имя)]`` либо ``None`` (не книга/не сошлось).

    Контракт с docling 2.135 (замер 02.11.2026): группа листа — ``GroupItem`` с
    ``label=GroupLabel.SHEET`` и ``name``; **порядок групп совпадает с порядком страниц**
    (лист — страница: ``doc.pages`` = ``{1: …, 2: …}``). Условия, при которых имена
    берутся: лист = страница (количество групп равно количеству страниц) и у каждого листа
    есть непустое имя. Иначе ``None`` — имена не раздаются догадкой по позиции.
    """
    pages = getattr(document, "pages", None)
    groups = getattr(document, "groups", None)
    if not pages or not groups:
        return None
    sheets = [
        group
        for group in groups
        if getattr(getattr(group, "label", None), "value", None) == _SHEET_GROUP_LABEL
    ]
    if not sheets or len(sheets) != len(pages):
        return None
    page_numbers = sorted(pages)
    sections: list[tuple[int, str]] = []
    for page_no, group in zip(page_numbers, sheets, strict=True):
        name = " ".join(str(getattr(group, "name", "") or "").split())
        if not name:
            return None
        sections.append((page_no, name))
    return sections


def _sheet_markdown(
    document: _DoclingDocument, sections: list[tuple[int, str]], whole: str
) -> str | None:
    """Markdown книги с заголовком ``## <лист>`` на каждый лист; ``None`` — не сошлось.

    Каждый лист экспортируется **своей страницей** (``page_no=``) — это тот же
    ``export_to_markdown`` docling, а не разбор готового текста. Затем проверяется, что
    посимвольный состав (без пробелов) совпадает с экспортом целиком: если постраничный
    экспорт что-то потерял (элемент без ``prov``/страницы), ``None`` — вызывающий отдаст
    целый текст с видимой записью имён. Потерять содержание ради заголовков нельзя:
    корпус — доказательство (I1/I19), а имя листа — провенанс.
    """
    try:
        bodies = [
            str(
                document.export_to_markdown(
                    escape_underscores=False, page_no=page_no
                )
            )
            for page_no, _name in sections
        ]
    except Exception:  # noqa: BLE001 — раскладка по страницам не обязана поддерживаться
        return None
    if "".join("".join(body.split()) for body in bodies) != "".join(whole.split()):
        return None
    return "\n\n".join(
        f"## {name}\n\n{body.strip()}" if body.strip() else f"## {name}"
        for (_page_no, name), body in zip(sections, bodies, strict=True)
    )


def _resolve_docling() -> bool:
    """Разрешает docling-символы лениво; ``True`` — docling доступен.

    Порядок проверок важен — он и есть контракт с тестами
    (``tests/adapters/test_extract_document.py`` подменяет ``DocumentConverter``,
    ``InputFormat``, ``PdfFormatOption`` и ``_DOCLING_AVAILABLE``):

    1. **Инъекция.** Любое из имён :data:`_DOCLING_SYMBOLS` уже не ``None`` в
       ``globals()`` — импорта **нет**, используется подставленное значение. Проверка
       не имеет побочных эффектов (не пишет ``_DOCLING_AVAILABLE``): иначе после
       ``monkeypatch``-отката флага в модуле осталось бы «разрешено», и следующий тест
       увидел бы ``available=False`` при рабочем docling.
    2. **Уже решено.** ``_DOCLING_AVAILABLE`` не ``None`` (импорт состоялся или его
       результат подставлен явно) — повторного импорта нет.
    3. **Импорт** (единственное место, знающее путь к docling). ``ImportError`` →
       ``_DOCLING_AVAILABLE = False``, символы остаются ``None``; :attr:`available`
       тогда ``False``, а извлечение отдаёт честную ошибку «docling не установлен».
    """
    global _DOCLING_AVAILABLE

    module_globals = globals()
    if any(module_globals[name] is not None for name in _DOCLING_SYMBOLS):
        return True
    if _DOCLING_AVAILABLE is not None:
        return _DOCLING_AVAILABLE

    try:  # тяжёлая необязательная зависимость: torch/transformers внутри
        import docling  # noqa: F401
        from docling.datamodel.base_models import (
            InputFormat as _InputFormat,
        )
        from docling.datamodel.pipeline_options import (
            AcceleratorOptions as _AcceleratorOptions,
        )
        from docling.datamodel.pipeline_options import (
            NativePdfPipelineOptions as _NativePdfPipelineOptions,
        )
        from docling.datamodel.pipeline_options import (
            OcrAutoOptions as _OcrAutoOptions,
        )
        from docling.datamodel.pipeline_options import (
            PdfPipelineOptions as _PdfPipelineOptions,
        )
        from docling.datamodel.pipeline_options import (
            TableFormerMode as _TableFormerMode,
        )
        from docling.datamodel.pipeline_options import (
            TableStructureOptions as _TableStructureOptions,
        )
        from docling.document_converter import (
            DocumentConverter as _DocumentConverter,
        )
        from docling.document_converter import (
            NativePdfFormatOption as _NativePdfFormatOption,
        )
        from docling.document_converter import (
            PdfFormatOption as _PdfFormatOption,
        )
    except ImportError:  # pragma: no cover - зависит от окружения
        _DOCLING_AVAILABLE = False
        return False

    module_globals.update(
        {
            "DocumentConverter": _DocumentConverter,
            "InputFormat": _InputFormat,
            "PdfFormatOption": _PdfFormatOption,
            "NativePdfFormatOption": _NativePdfFormatOption,
            "PdfPipelineOptions": _PdfPipelineOptions,
            "NativePdfPipelineOptions": _NativePdfPipelineOptions,
            "TableStructureOptions": _TableStructureOptions,
            "TableFormerMode": _TableFormerMode,
            "OcrAutoOptions": _OcrAutoOptions,
            "AcceleratorOptions": _AcceleratorOptions,
        }
    )
    _DOCLING_AVAILABLE = True
    return True


def _docling_present() -> bool:
    """Установлен ли ``docling`` — проверка **без импорта** (``find_spec``, torch не грузится).

    Зачем отдельная проба. ``available`` спрашивают **до** работы — на старте каждого
    воркера пула (``core.cli.wiring.build_worker_extractor``) и в
    ``build_extractor``, — чтобы решить, включать ли документный экстрактор (это же
    решение входит в композитную строку ``version``). Импорт там стоил бы 403 MB на
    воркер, который 86% корпуса обрабатывает HTML-путём, то есть ровно то, что эта
    ленивость убирает.

    Найденный, но сломанный docling (несовместимые extras) ловится при **первом
    извлечении**: :func:`_resolve_docling` вернёт ``False``, :meth:`_precheck` отдаст
    прежнюю честную ошибку «docling не установлен», а :attr:`available` после этого
    сойдётся к ``False`` (см. свойство) — решение «включать ли docling» самокорректируется
    по факту, а не по догадке.
    """
    global _DOCLING_PRESENT

    if _DOCLING_PRESENT is None:
        try:
            _DOCLING_PRESENT = importlib.util.find_spec("docling") is not None
        except (ImportError, ValueError):  # pragma: no cover - сломанный __spec__
            _DOCLING_PRESENT = False
    return _DOCLING_PRESENT


# §3.2: расширяемый реестр поддерживаемых форматов (единый источник — нейтральный
# ``core.util.document_formats``, чтобы pipeline мог детектировать документы, не импортируя
# адаптеры). PDF — layout-aware (может требовать layout/OCR-модели); Office — лёгкий путь.
SUPPORTED_FORMATS: dict[str, str] = {
    suffix: ("layout" if suffix == ".pdf" else "office") for suffix in DOCUMENT_SUFFIXES
}

_log = logging.getLogger(__name__)


class OcrEngineUnavailableError(RuntimeError):
    """``document_level=full`` требует рабочий OCR, но docling его не нашёл.

    Поднимается из :meth:`DoclingDocumentExtractor._build_converter`: молчаливое
    извлечение без OCR, помеченное как ``full``, — ложь о качестве текста. Ошибка
    доходит до фазы как ``parser_failed`` (видимая запись ``errors``), а не превращается
    в пустой/неполный текст.
    """


class _OcrProbeUnsupported(RuntimeError):
    """Внутренний API docling не позволяет проверить OCR (переименование/новая версия).

    Не ошибка OCR: вызывающий деградирует в warning, а не падает (version-tolerant).
    """


def _probe_ocr_engine(pdf_options: object) -> object | None:
    """Строит auto-OCR для PDF-опций уровня и возвращает выбранный движок.

    ``None`` — docling отверг всех кандидатов (``OcrAutoModel._engine is None``): OCR
    фактически недоступен. Проверяется именно тот выбор, который сделает пайплайн
    (``OcrAutoModel``), но **без** инициализации тяжёлых layout/table-моделей: для
    офисного документа цена проверки не должна быть ценой PDF-конвейера.

    Если docling переименует внутренности (``OcrAutoModel``/``_engine``), поднимается
    :class:`_OcrProbeUnsupported` — это деградация проверки, не отказ извлечения.
    """
    try:
        from docling.models.stages.ocr.auto_ocr_model import (
            OcrAutoModel,
        )
    except ImportError as exc:  # pragma: no cover - зависит от версии docling
        raise _OcrProbeUnsupported(f"нет OcrAutoModel: {exc}") from exc

    ocr_options = getattr(pdf_options, "ocr_options", None)
    accelerator_options = getattr(pdf_options, "accelerator_options", None)
    if ocr_options is None or accelerator_options is None:
        raise _OcrProbeUnsupported("у PDF-опций нет ocr_options/accelerator_options")

    artifacts_path = getattr(pdf_options, "artifacts_path", None)
    if artifacts_path is None:
        # Как в BasePipeline: неявный путь артефактов — из настроек docling.
        # ``Any`` — намеренно: объект принадлежит внешней библиотеке, известен только в
        # этой ветке (пакет — опциональный extra), а нужен из него один атрибут.
        docling_settings: Any = None
        try:
            from docling.datamodel.settings import (
                settings as docling_settings,
            )
        except ImportError:  # pragma: no cover - зависит от версии docling
            docling_settings = None
        artifacts_path = getattr(docling_settings, "artifacts_path", None)

    try:
        auto = OcrAutoModel(
            enabled=True,
            artifacts_path=artifacts_path,
            options=ocr_options,
            accelerator_options=accelerator_options,
        )
    except (TypeError, AttributeError) as exc:  # pragma: no cover - смена API docling
        raise _OcrProbeUnsupported(f"OcrAutoModel несовместим: {exc}") from exc
    return getattr(auto, "_engine", auto)


#: §6 (P5): щедрый per-level ``document_timeout`` (секунды). ``full`` — ``None``
#: (без таймаута = сегодняшнее поведение; иначе можно было бы потерять документ, который
#: сегодня успешно извлекается). ``text``/``layout`` короче, ``layout_tables`` длиннее.
_LEVEL_DOCUMENT_TIMEOUT_S: dict[DocumentLevel, float | None] = {
    DocumentLevel.OFF: None,
    DocumentLevel.TEXT: 900.0,
    DocumentLevel.LAYOUT: 1800.0,
    DocumentLevel.LAYOUT_TABLES: 2700.0,
    DocumentLevel.FULL: None,
}

#: ``queue_max_size`` PDF-пайплайнов docling (``StandardPdfPipeline``) — **замеренная**
#: ручка памяти, а не дефолт библиотеки (дефолт 2.135 — 100 страниц в полёте).
#: См. :meth:`DoclingDocumentExtractor._pdf_options`: −44% peak RSS при идентичном
#: markdown; ``execution_change``, ``extractor_version`` не меняется (кэш экстракции
#: остаётся валидным).
PDF_QUEUE_MAX_SIZE = 1

#: Число CPU-потоков инференса (``AcceleratorOptions.num_threads``), объявленное **явно**.
#:
#: Значение равно дефолту docling 2.135 — проверено вызовом, а не по памяти:
#: ``AcceleratorOptions()`` → ``num_threads=4``. Пинится не ради «настройки», а чтобы
#: убрать **env-зависимость**: ``AcceleratorOptions`` — ``BaseSettings`` с префиксом
#: ``DOCLING_``, поэтому без явного аргумента число потоков приходит из окружения
#: (``DOCLING_NUM_THREADS``/``OMP_NUM_THREADS``). Проверено вызовом:
#: ``DOCLING_NUM_THREADS=11 OMP_NUM_THREADS=11`` даёт **11** у ``AcceleratorOptions()`` и
#: **4** у ``AcceleratorOptions(num_threads=4)`` — явный аргумент приоритетнее env.
#:
#: Это тот же класс дефекта, что P9 (R5): параметр исполнения не должен выбираться
#: окружением. Иначе два хоста с одним ``run.yaml`` извлекают по-разному, а
#: ``extractor_version`` этого не различает — в ключ ``extract_cache`` входит устройство
#: (:func:`docling_extractor_version`), но не потоки.
#:
#: **Влияние на скорость не измерено.** Утверждения «больше потоков быстрее/медленнее» на
#: этом стеке здесь нет ни в одну сторону: подбор значения по времени — отдельный замер,
#: а не предвидение (A3).
ACCELERATOR_NUM_THREADS = 4

#: Эффективное значение замеренной ручки по уровням PDF-пайплайна: ``None`` — ручки у
#: пайплайна уровня нет (``text`` — ``NativePdfPipelineOptions``, другой механизм).
_PDF_QUEUE_MAX_SIZE_BY_LEVEL: dict[str, int | None] = {
    DocumentLevel.LAYOUT.value: PDF_QUEUE_MAX_SIZE,
    DocumentLevel.LAYOUT_TABLES.value: PDF_QUEUE_MAX_SIZE,
    DocumentLevel.FULL.value: PDF_QUEUE_MAX_SIZE,
    DocumentLevel.TEXT.value: None,
}


def pdf_queue_provenance() -> dict[str, int | None]:
    """``queue_max_size`` по уровням извлечения — для провенанса манифеста (R7/§13).

    Ручка **не** входит в ``extractor_version`` и не является границей
    ``EXECUTION_CONTRACT_FIELDS``, но её значение обязано быть видно в манифесте: без него
    замер (и его −44% RSS) невоспроизводим. ``None`` — у уровня ручки нет (``text``).
    """
    return dict(_PDF_QUEUE_MAX_SIZE_BY_LEVEL)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def _stage_by_reference(source: Path, target: Path) -> None:
    """Кладёт ``target`` с нужным суффиксом, **не копируя тело** документа.

    docling выводит формат из суффикса пути, а retained-файл называется
    ``{sha256}.bin`` (I19), поэтому конвертеру нужен путь с настоящим расширением.
    Порядок попыток — от самой дешёвой к самой дорогой:

    1. ``os.link`` — жёсткая ссылка: те же inode/байты, ноль места, ноль копирования.
       Работает, когда ``tmp`` и ``corpus_raw`` на одной ФС (частый случай);
    2. ``os.symlink`` — если ``tmp`` на другой ФС (``EXDEV``): байты тоже не копируются,
       docling открывает файл по ссылке;
    3. ``shutil.copyfile`` — последний резерв (ФС без ссылок): одна копия на диске,
       но не в RAM (``copyfile`` не поднимает тело в память).

    Файл-цель живёт только на время конвертации (временный каталог вызывающего).
    """
    try:
        os.link(source, target)
        return
    except OSError:
        pass
    try:
        os.symlink(source.resolve(), target)
        return
    except OSError:
        pass
    shutil.copyfile(source, target)


def _page_count(result: object) -> int | None:
    """Число страниц **документа**, а не число конвертированных страниц.

    ``len(result.pages)`` — это страницы, попавшие в результат: сегодня это весь документ,
    но при любом будущем ``page_range`` (частичная конвертация) счётчик стал бы ложным
    (например, 1 вместо 665), и ``page_count`` перестал бы описывать документ. Docling
    хранит полный счётчик в ``result.input.page_count`` (``InputDocument``), поэтому
    берём его. Fallback — ``len(result.pages)``: у ``InputDocument`` поле имеет дефолт
    ``0`` (и в старых версиях/для не-PDF бэкендов может быть незаполненным), а ``0``
    страниц у успешно извлечённого документа — заведомо неверный счётчик.

    **Ноль — не измерение** (замер 02.11.2026). Офисный DOCX-бэкенд не вызывает
    ``add_page``: у него ``len(doc.pages) == 0`` и ``input.page_count == 0``, то есть
    «страниц нет» — не факт о документе, а отсутствие понятия страницы у формата.
    Возвращать в этом случае ``0`` значило бы объявить замер, которого не было: отчёт
    показал бы «0 страниц» у прочитанного документа. Поэтому «ни одного наблюдения» →
    :data:`None` («не измерено»), а не ноль. У XLSX/PPTX счётчик реальный: лист/слайд —
    страница (``input.page_count`` = 2/1 на замере), у PDF — число страниц PDF.
    """
    input_doc = getattr(result, "input", None)
    total = getattr(input_doc, "page_count", None)
    if isinstance(total, int) and not isinstance(total, bool) and total > 0:
        return total
    pages = getattr(result, "pages", None)
    if pages:
        return len(pages)
    return None


def _converted_page_count(result: object) -> int | None:
    """Сколько страниц **реально попало** в результат (``len(result.pages)``).

    Другой вопрос, чем у :func:`_page_count`: там — размер документа, здесь — объём
    проделанной работы. Пара нужна, чтобы «частично» было числом (``3 из 665``), а не
    прилагательным: без него человек не отличит «не прочиталась одна страница из семи» от
    «не прочиталось 662 из 665».
    """
    pages = getattr(result, "pages", None)
    if pages is None:
        return None
    try:
        return len(pages)
    except TypeError:  # pragma: no cover — бэкенд отдал не коллекцию
        return None


class DoclingDocumentExtractor:
    """Документ (PDF/DOCX/PPTX/XLSX) → markdown через docling. Реализация ``ExtractorPort``.

    ``level`` задаёт уровень извлечения **PDF** (P5), ``pdf_device`` — устройство моделей
    PDF (P9). ``version`` — инстансный и кодирует уровень, устройство, сборку движка,
    политику экспорта и очистку текста
    (``docling-doc-3:<level>/<device>/docling-<build>/<policy>/<finalize>``,
    :func:`docling_extractor_version`), чтобы смена уровня, устройства или апгрейд docling
    была видна и в ``index_build_id``, и в ключе ``extract_cache``. Классовый ``version`` —
    дефолт ``full``/``cpu`` (обратная совместимость по атрибуту).

    **Офисный документ ``version`` не использует** (см. :meth:`version_for`): у DOCX/PPTX/
    XLSX нет ни уровня, ни устройства — их штамп строит :func:`docling_office_version`.
    Поэтому провенанс конкретного файла берётся :meth:`version_for`, а ``version`` остаётся
    штампом PDF-ветви.
    """

    #: Классовый дефолт = уровень ``full``, вычисленный при импорте
    #: (совпадает с ``DoclingDocumentExtractor().version``). Инстанс считает версию заново:
    #: тест, подменивший сборку движка, обязан увидеть новый ключ, а не импортный.
    version = docling_extractor_version(DocumentLevel.FULL)

    #: B3: адаптер извлекает документ по **пути** к локальному файлу (``page.source_path``),
    #: поэтому пригоден для членов контейнеров: спуск отдаёт временный файл, а не буфер.
    supports_source_path = True

    def __init__(
        self,
        level: DocumentLevel = DocumentLevel.FULL,
        *,
        document_timeout: float | None = None,
        pdf_device: PdfDevice | str = PdfDevice.CPU,
    ) -> None:
        self._level = DocumentLevel(level)
        # P9: объявленное устройство моделей. Подставляется в ``accelerator_options``
        # конвертера (``auto`` в docling не используется: ось объявлена, а не выведена).
        self._pdf_device = PdfDevice(pdf_device)
        self.version = docling_extractor_version(self._level, self._pdf_device)
        # §6: явный таймаут оператора приоритетен; иначе — щедрый per-level дефолт.
        self._document_timeout = document_timeout
        self._converter = None
        #: OCR проверен (``full``): повторные PDF не пере-строят auto-OCR.
        self._ocr_checked = False

    def version_for(self, filename: str) -> str:
        """Версия **фактического** документа: PDF и Office штампуются по-разному.

        ``self.version`` описывает PDF-ветвь (уровень + устройство). Офисный документ идёт
        через ``SimplePipeline`` без уровня и без моделей, поэтому его версия — отдельная
        (:func:`docling_office_version`). Выбор — по суффиксу файла, тому же, по которому
        docling выводит формат: другого входа у решения нет, а догадка о формате по
        содержимому была бы вторым, расходящимся с docling классификатором.

        Метод нужен composition root'у: ключ ``extract_cache`` спрашивается **до**
        извлечения (``_LocalExtractor.extractor_version_for``), и он обязан совпасть со
        штампом адаптера. Без него офисный документ получал бы PDF-ключ с уровнем и
        устройством, которых не было (и кэш был бы write-only).

        :param filename: имя файла с расширением (``document.docx``/``report.pdf``).
        """
        return self._version_for_suffix(Path(filename).suffix.lower())

    def _version_for_suffix(self, suffix: str) -> str:
        """Версия по суффиксу: только ``.pdf`` — PDF-ветвь, остальное — Office."""
        return self.version if suffix == PDF_SUFFIX else docling_office_version()

    @property
    def level(self) -> DocumentLevel:
        """Уровень извлечения этого экстрактора (P5)."""
        return self._level

    @property
    def options_provenance(self) -> dict[str, int | None]:
        """Замеренные ручки извлечения по уровням — провенанс манифеста (R7/§13).

        Сейчас это ``queue_max_size`` (:data:`PDF_QUEUE_MAX_SIZE`): значение обязано быть
        видно в манифесте прогона, иначе замер невоспроизводим. Не входит в
        ``extractor_version``, поэтому кэш экстракции от него не зависит.
        """
        return pdf_queue_provenance()

    @property
    def available(self) -> bool:
        """docling пригоден — проверка **без импорта** (цена ответа не 403 MB).

        Пока фактического разрешения не было — «пакет установлен» (:func:`_docling_present`);
        после первого извлечения — уже измеренный результат (:func:`_resolve_docling`),
        чтобы найденный, но сломанный docling не считался доступным до конца процесса.
        Спрашивается на старте воркера пула, поэтому импорт здесь недопустим.
        """
        if _DOCLING_AVAILABLE is not None:
            return _DOCLING_AVAILABLE and DocumentConverter is not None
        return _docling_present()

    @staticmethod
    def supports(filename: str) -> bool:
        return Path(filename).suffix.lower() in SUPPORTED_FORMATS

    def _pdf_options(self) -> object:
        """Строит PDF-опции docling для своего уровня (P5).

        Возвращает ``PipelineOptions`` (уровни ``layout``/``layout_tables``/``full``) либо
        объект нативного пайплайна (``text``). ``off`` сюда не доходит штатно.

        **``queue_max_size=1`` (замер, не предвидение).** Главный драйвер памяти
        ``StandardPdfPipeline`` — не «весь PDF», а число страниц **в полёте**: продюсер плюс
        шесть очередей по ``queue_max_size`` (дефолт docling 2.135 — **100**). Замер уровня
        ``layout_tables``: синтетика 68 стр. — 3160 MB peak RSS при дефолте против
        **1775 MB** при ``queue_max_size=1`` (**−44%**), время на страницу 1.72 → 1.60 s.
        Реальный PDF 665 стр. (A/B на одном документе): дефолт — 4174 MB / 1.46 s/стр.,
        ``queue_max_size=1`` — **2126 MB** (**−49%**) / **1.30 s/стр.** (не медленнее).
        Markdown при этом **байт-в-байт тот же**: md5 совпал и на 81-, и на 665-стр.
        документах; статус docling — ``SUCCESS``, страниц 665/665 (частичных нет).

        Класс изменения — ``execution_change`` (§13), **не** ``methodology_change``:
        ``extractor_version`` (``docling-doc-3:<level>/<device>/docling-<build>``) не
        включает эту ручку и от замера не меняется, поэтому записи ``extract_cache/``
        остаются валидными (замер не инвалидирует извлечённый корпус). ``golden-full``
        не требуется.

        **P9: устройство и потоки.** ``accelerator_options`` выставляются **всегда** для
        структурированных уровней: ``device`` — объявленная ось ``crawl.pdf_device``,
        ``num_threads`` — :data:`ACCELERATOR_NUM_THREADS` (равен дефолту библиотеки, но
        передан явно — иначе значение приходит из env ``DOCLING_NUM_THREADS``/
        ``OMP_NUM_THREADS``). Дефолт docling ``device="auto"`` не используется: он выбирает
        устройство по окружению, а не по объявленному входу (R5), и GPU-хост молча
        исполнял бы прогон не там, где заявлено. Устройство, в отличие от этой ручки,
        **входит** в ``extractor_version`` — текст документа от него зависит.

        Ветка ``text`` (``NativePdfPipelineOptions``) намеренно **без** этой ручки: у
        нативного пайплайна другой механизм и своя память (899 MB), поля ``queue_max_size``
        у него нет вовсе; моделей, которым нужно устройство, он не грузит.
        """
        # Символы docling разрешаются здесь же: до этого вызова они могли быть ``None``
        # (ленивый импорт). Повторный вызов — no-op (см. :func:`_resolve_docling`).
        _resolve_docling()
        timeout = (
            self._document_timeout
            if self._document_timeout is not None
            else _LEVEL_DOCUMENT_TIMEOUT_S.get(self._level)
        )
        if self._level is DocumentLevel.TEXT:
            # Нативный пайплайн моделей не грузит: устройство и потоки ему не адресованы
            # (pdfium/нативный парсер исполняется в вызывающем процессе на CPU).
            return NativePdfPipelineOptions(document_timeout=timeout)
        # P9: устройство объявлено, а не выведено. Без этого docling берёт
        # ``AcceleratorOptions()`` с ``device="auto"``, то есть решает сам — ровно то,
        # что R5 запрещает: провенанс назвал бы устройство, которого вход не объявлял.
        #
        # ``accelerator_options`` добавляется в kwargs **только** когда объект есть:
        # у docling это поле не принимает ``None`` (Pydantic отвергнет), а подменённые
        # символы без docling дают именно ``None``.
        common: dict[str, Any] = {
            "document_timeout": timeout,
            "queue_max_size": PDF_QUEUE_MAX_SIZE,
        }
        accelerator = self._accelerator_options()
        if accelerator is not None:
            common["accelerator_options"] = accelerator
        if self._level is DocumentLevel.LAYOUT:
            return PdfPipelineOptions(
                do_table_structure=False,
                do_ocr=False,
                **common,
            )
        if self._level is DocumentLevel.LAYOUT_TABLES:
            return PdfPipelineOptions(
                do_table_structure=True,
                table_structure_options=TableStructureOptions(
                    do_cell_matching=True, mode=TableFormerMode.FAST
                ),
                do_ocr=False,
                **common,
            )
        # ``full`` — байт-в-байт дефолты docling (OCR включён, таблицы ACCURATE), кроме
        # ``queue_max_size`` (см. docstring: замер −44% RSS, markdown идентичен).
        return PdfPipelineOptions(**common)

    def _accelerator_options(self) -> object:
        """``AcceleratorOptions`` с **объявленным** устройством (P9).

        ``device`` — значение оси ``crawl.pdf_device`` (``cpu``/``cuda``), ``num_threads``
        — :data:`ACCELERATOR_NUM_THREADS` (дефолт библиотеки, переданный явно). Оба
        значения объявлены здесь, а не взяты из дефолтов библиотеки: ``device="auto"`` —
        это выбор окружения, а не входа, а число потоков как дефолт ``BaseSettings``
        читается из env (``DOCLING_NUM_THREADS``/``OMP_NUM_THREADS``).

        ``AcceleratorOptions`` не входит в контракт инъекции docling-символов
        (:data:`_DOCLING_SYMBOLS`), поэтому в окружении с подменёнными символами он может
        быть ``None``. Тогда объявить устройство нечем: это **отказ** для ``cuda`` (нельзя
        исполнить объявленную ось) и видимое предупреждение для ``cpu`` — тихая подмена
        устройства запрещена тем же правилом, что тихий CPU-фолбэк.
        """
        if AcceleratorOptions is None:  # pragma: no cover — только подменённые символы
            if self._pdf_device is PdfDevice.CUDA:
                raise RuntimeError(
                    "crawl.pdf_device=cuda: AcceleratorOptions недоступен "
                    "(docling не импортирован или символы подменены) — объявленное "
                    "устройство невозможно исполнить"
                )
            _log.warning(
                "document extract: AcceleratorOptions недоступен — устройство "
                "моделей не объявлено (ожидалось %s)",
                self._pdf_device.value,
            )
            return None
        return AcceleratorOptions(
            device=self._pdf_device.value, num_threads=ACCELERATOR_NUM_THREADS
        )

    def _build_converter(self, *, pdf: bool = True):  # noqa: ANN202 - тип docling необязателен
        """Собирает ``DocumentConverter`` с per-level PDF-опциями (Office — дефолт docling).

        ``pdf=False`` — конвертер строится для офисного документа: OCR (и его проверка)
        не нужны, а в окружении ``extract`` (Office без torch/OCR) их может не быть.
        Для PDF при ``full`` здесь же **проверяется** рабочий OCR-движок: уровень обещает
        OCR, поэтому его отсутствие — явная ошибка (:class:`OcrEngineUnavailableError`),
        а не тихое извлечение без OCR. Уровни ``layout``/``layout_tables``/``text`` OCR
        выключают осознанно — проверка для них не выполняется.
        """
        _resolve_docling()
        if self._level is DocumentLevel.OFF:
            return DocumentConverter()
        pdf_options = self._pdf_options()
        if self._level is DocumentLevel.TEXT:
            format_options = {
                InputFormat.PDF: NativePdfFormatOption(pipeline_options=pdf_options)
            }
        else:
            format_options = {
                InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_options)
            }
        converter = DocumentConverter(format_options=format_options)
        if self._level is DocumentLevel.FULL and pdf:
            self._require_working_ocr(pdf_options)
        return converter

    def _require_working_ocr(self, pdf_options: object) -> None:
        """``full`` обязан иметь рабочий OCR; иначе — отказ, а не текст «как full»."""
        if self._ocr_checked:
            return
        try:
            engine = _probe_ocr_engine(pdf_options)
        except _OcrProbeUnsupported as exc:
            # docling переименовал внутренности: проверка невозможна. Это не повод
            # ломать извлечение — фиксируем предупреждением (version-tolerant).
            _log.warning(
                "document_level=full: не удалось проверить OCR-движок docling (%s) — "
                "проверка пропущена",
                exc,
            )
            self._ocr_checked = True
            return
        if engine is None:
            raise OcrEngineUnavailableError(
                "document_level=full требует OCR, но docling не нашёл ни одного "
                "OCR-движка: установленный rapidocr/onnxruntime несовместим "
                "(docling 2.135 импортирует PP_OCRV6_LANGS, удалённый в rapidocr "
                "3.10.0; нужен rapidocr>=3.9.1,<3.10). Извлечение остановлено, чтобы "
                "не выдать текст без OCR за уровень full."
            )
        self._ocr_checked = True

    def extract_bytes(self, content: bytes, filename: str = "document.pdf") -> ExtractedDoc:
        """Извлекает markdown из байтов документа (retention-единица — байты, §3.2).

        ``filename`` определяет формат (расширение). Неподдерживаемое расширение →
        ``ExtractedDoc`` с ``error`` (без исключения).

        Исключение: ``full`` без рабочего OCR-движка — :class:`OcrEngineUnavailableError`
        (ошибка конфигурации, а не грациозная деградация). Фаза переводит её в
        ``parser_failed``; текст без OCR никогда не помечается уровнем ``full``.

        Байты пишутся во временный файл (docling читает путь). Если байты уже лежат
        retained-файлом, дешевле :meth:`extract_path`: он не копирует тело.
        """
        suffix = Path(filename).suffix.lower() or ".pdf"
        error = self._precheck(suffix)
        if error is not None:
            return error
        # Штамп провенанса — версия **фактической** ветви (PDF или Office): офисному
        # документу нельзя приписать уровень/устройство PDF-конвейера (см. ``version_for``).
        version = self._version_for_suffix(suffix)
        # Сборка конвертера — вне ``try``: ``OcrEngineUnavailableError`` обязан дойти до
        # фазы как ошибка конфигурации, а не превратиться в ``docling: ...``.
        converter = self._converter or self._build_converter(pdf=suffix == ".pdf")
        self._converter = converter
        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            return self._convert(
                converter, tmp_path, version=version, office=suffix != PDF_SUFFIX
            )
        except Exception as exc:  # noqa: BLE001 - грациозная деградация на уровне адаптера
            return self._error(f"docling: {exc}", version)
        finally:
            if tmp_path is not None:
                Path(tmp_path).unlink(missing_ok=True)

    def extract_path(
        self, path: str | Path, *, filename_hint: str | None = None
    ) -> ExtractedDoc:
        """Извлекает markdown из retained-файла, **не поднимая тело в RAM**.

        ``filename_hint`` задаёт формат (расширение): retained-файл называется
        ``{sha256}.bin`` (I19), а docling выводит формат из суффикса пути, поэтому
        файл подставляется конвертеру по ссылке с правильным суффиксом
        (:func:`_stage_by_reference`: hard link → symlink → копия). Ни один из
        первых двух вариантов не копирует байты, так что документ на сотни МБ не
        проходит через RSS воркера и не удваивается на диске.

        Семантика извлечения та же, что у :meth:`extract_bytes`: тот же уровень
        (``document_level``), та же OCR-проверка, те же строки ошибок, тот же
        ``ExtractedDoc`` и та же версия — :meth:`version_for` (PDF и Office штампуются
        разными строками).
        """
        source = Path(path)
        suffix = (
            Path(filename_hint).suffix.lower()
            if filename_hint
            else source.suffix.lower()
        ) or ".pdf"
        error = self._precheck(suffix)
        if error is not None:
            return error
        version = self._version_for_suffix(suffix)
        if not source.is_file():
            # Видимая ошибка, а не падение: файл мог исчезнуть между retention и извлечением.
            return self._error(f"retained-файл не найден: {source}", version)
        converter = self._converter or self._build_converter(pdf=suffix == ".pdf")
        self._converter = converter
        try:
            with tempfile.TemporaryDirectory(prefix="cohound-extract-") as staging:
                staged = Path(staging) / f"document{suffix}"
                _stage_by_reference(source, staged)
                return self._convert(
                    converter, str(staged), version=version, office=suffix != PDF_SUFFIX
                )
        except Exception as exc:  # noqa: BLE001 - грациозная деградация на уровне адаптера
            return self._error(f"docling: {exc}", version)

    def _precheck(self, suffix: str) -> ExtractedDoc | None:
        """Общие проверки обоих входов; ``None`` — можно конвертировать.

        Формат, уровень ``off``, наличие docling и OCR-проверка для ``full`` PDF —
        ровно те же и в том же порядке, что были в ``extract_bytes``: входы
        (байты/путь) не меняют семантику извлечения.
        """
        if suffix not in SUPPORTED_FORMATS:
            # Провал формата — не ветвь провенанса: обработчика нет вовсе, поэтому штамп
            # остаётся инстансным (PDF-версия описывает сам адаптер, а не документ).
            return self._error(f"формат не поддерживается: {suffix}")
        if self._level is DocumentLevel.OFF:
            # ``off`` — решение pipeline пропустить документ, а не задача адаптера; прямой
            # вызов честно ошибается (никогда не пустой текст «молча»).
            return self._error(
                "document_level=off: документы не извлекаются",
                self._version_for_suffix(suffix),
            )
        # Ленивый импорт docling: ``available`` отвечает по ``find_spec`` (без импорта),
        # фактическое разрешение символов — здесь, на первом реальном документе. Строка
        # ошибки прежняя: «пакет есть, но не импортируется» и «пакета нет» для вызывающего
        # неразличимы — извлекать нечем в обоих случаях.
        if not _resolve_docling():
            return self._error(
                "docling не установлен", self._version_for_suffix(suffix)
            )
        # ``full`` обещает OCR именно для PDF; офисный документ OCR не использует, поэтому
        # проверка выполняется только на PDF-пути (окружение ``extract`` без OCR остаётся
        # рабочим). Ошибка — до конвертации: никакой текст без OCR не помечается ``full``.
        if suffix == PDF_SUFFIX and self._level is DocumentLevel.FULL:
            self._require_working_ocr(self._pdf_options())
        return None

    def _convert(
        self,
        converter: _DoclingConverter,
        path: str,
        *,
        version: str | None = None,
        office: bool = False,
    ) -> ExtractedDoc:
        """Общий хвост обоих входов: ``convert`` → markdown → ``ExtractedDoc``.

        Статус конвертации разбирается на три исхода, а не на два:

        * ``SUCCESS`` — полный текст, ``partial=False`` (прежнее поведение);
        * ``PARTIAL_SUCCESS`` — текст **части** документа, ``partial=True`` и причина
          (:meth:`_partial`). Пустой текст при этом статусе — не частичный успех, а
          провал: ``_error``;
        * любой другой статус — ``_error`` (прежняя строка ошибки).

        Любой сбой конвертации — грациозная деградация ``docling: <причина>`` (прежняя
        строка ошибки), а не исключение.

        ``version`` — штамп **фактической** ветви (PDF или Office): его считает вызывающий
        вход (:meth:`extract_bytes`/`extract_path`) по суффиксу, потому что вывести ветвь
        из результата docling нельзя — формат знает вход, а не результат.
        ``office`` — та же ветвь в булевом виде: причина частичности не должна называть
        уровень, которого у офисного пути нет.
        """
        stamp = self.version if version is None else version
        try:
            result = converter.convert(path)
            status = getattr(result.status, "name", str(result.status))
            if status == PARTIAL_SUCCESS_STATUS:
                doc = self._partial(result, status, version=stamp, office=office)
                if doc.error is None:
                    # Операционная видимость: артефакт прогона признак частичности пока не
                    # несёт (его место — ``corpus_docs``/манифест), поэтому факт «документ
                    # прочитан не целиком» обязан быть виден хотя бы в журнале, а не
                    # растворяться в числе документов.
                    _log.warning("частичное извлечение %s: %s", path, doc.partial_reason)
                return doc
            if status != "SUCCESS":
                return self._error(f"docling: статус конвертации {status}", stamp)
            text = _document_text(result.document)
            pages = _page_count(result)
            return ExtractedDoc(
                text=text,
                content_hash=_sha256(text),
                extractor_version=stamp,
                page_count=pages,
            )
        except Exception as exc:  # noqa: BLE001 - грациозная деградация на уровне адаптера
            return self._error(f"docling: {exc}", stamp)

    def _partial(
        self, result: _DoclingResult, status: str, *, version: str | None = None, office: bool = False
    ) -> ExtractedDoc:
        """``PARTIAL_SUCCESS`` → документ с текстом **части** и видимой причиной.

        Частичность не выдаётся за полный успех по двум признакам сразу:

        * ``partial=True`` — машинный признак для вызывающего (сегодня его читает только
          журнал: персистенция в ``corpus_docs``/манифест — отдельная правка ядра, не
          адаптера);
        * ``partial_reason`` называет **статус**, **сколько страниц получено из скольких**
          и **объявленный уровень** — ``layout``/``layout_tables``/``full`` обещают
          структуру целого документа, поэтому их частичный результат не вправе читаться
          как «full», не будучи таковым (``extractor_version`` при этом не меняется: он
          описывает движок и ось уровня, а не объём проделанной работы).

        Пустой текст при частичном статусе — это не частичный успех: страницы, которых нет
        в результате, не дали ничего. Тогда — прежняя строка ``_error``, чтобы «прочитали
        частично» не стало способом вернуть пустой документ в корпус.

        ``office=True`` — офисный документ: у него нет уровня (``SimplePipeline``), поэтому
        причина частичности не называет ни уровня, ни обещания структуры. Иначе DOCX
        получил бы в провенанс ``full``-обещание, которого никто не давал.
        """
        text = _document_text(result.document)
        if not text.strip():
            return self._error(
                f"docling: статус конвертации {status} без текста",
                self.version if version is None else version,
            )
        total = _page_count(result)
        converted = _converted_page_count(result)
        pages_note = ""
        if converted is not None and total is not None:
            pages_note = f", получено страниц {converted} из {total}"
        elif converted is not None:
            pages_note = f", получено страниц {converted}"
        if office:
            # Офисный документ идёт без уровня и без моделей: приписывать ему обещание
            # структуры «уровня full» значило бы вернуть в провенанс ту самую ложь, ради
            # которой у Office отдельная версия (:func:`docling_office_version`).
            reason = (
                f"docling: статус конвертации {status}{pages_note}; "
                f"офисный документ прочитан не целиком"
            )
        else:
            # Формулировка зависит от уровня: ``layout``/``layout_tables``/``full`` обещают
            # **структуру целого** документа (заголовки, таблицы, reading order), поэтому их
            # частичный результат не вправе читаться как обещанное; ``text`` структуры не
            # обещает вовсе — там частичность означает только «часть страниц не прочитана».
            claim = (
                "обещана структура целого документа — получен текст части"
                if self._level in _STRUCTURED_LEVELS
                else "получен текст части документа"
            )
            reason = (
                f"docling: статус конвертации {status}{pages_note}; "
                f"уровень {self._level.value}: {claim}"
            )
        return ExtractedDoc(
            text=text,
            content_hash=_sha256(text),
            extractor_version=self.version if version is None else version,
            page_count=total,
            partial=True,
            partial_reason=reason,
            pages_converted=converted,
        )

    def _error(self, message: str, version: str | None = None) -> ExtractedDoc:
        """Грациозная деградация: пустой текст, хеш пустой строки, ``error``.

        ``version`` — штамп фактической ветви (PDF/Office); по умолчанию PDF-версия
        инстанса (прямые вызовы и тесты).
        """
        return ExtractedDoc(
            text="",
            content_hash=_sha256(""),
            extractor_version=self.version if version is None else version,
            error=message,
        )


# Обратная совместимость с прежним именем (PDF-only вызовы).
DoclingPdfExtractor = DoclingDocumentExtractor
