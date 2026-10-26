"""Экстрактор документов (docling → markdown) — реализация ``ExtractorPort``.

Поддерживает PDF и Office Open XML (docx/pptx/xlsx) единообразно через docling:
- PDF — layout-aware конвертация (таблицы/фич-матрицы, §3.2; блокер PDF-теста M0);
- DOCX/PPTX/XLSX — Office-бэкенды docling (python-docx/python-pptx/openpyxl);
  для этих форматов не требуется torch/layout-модели (``SimplePipeline``).

**P5 — уровень извлечения.** Качество PDF-экстракции задаётся осью
``crawl.document_level`` (:class:`~core.domain.enums.DocumentLevel`), а не «случайным»
дефолтом пустого ``DocumentConverter()``. Уровень входит в ``version`` экстрактора
(``docling-doc-3:<level>/docling-<build>``) и тем самым — в ``index_build_id``
иммутабельной сборки (§13): смена уровня не может молча дать ту же сборку.

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
from typing import Any

from core.domain.enums import DocumentLevel
from core.ports.types import ExtractedDoc
from core.util.document_formats import DOCUMENT_SUFFIXES

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

#: Префикс «сборки движка» в идентификаторах версии. Один на ``extractor_version``
#: (ключ ``extract_cache``/``pb``) и на ``extractor_stack`` (``run_id``).
_DOCLING_BUILD_PREFIX = "docling-"

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
        try:
            return importlib.metadata.version(dist)
        except importlib.metadata.PackageNotFoundError:
            continue
        except Exception:  # noqa: BLE001 — метаданные не роняют резолв версии
            continue
    return "unknown"


def docling_engine_build() -> str:
    """Сборка движка: ``docling-<version установленной библиотеки>`` (Д-8).

    Единственная формулировка «сборки»: и ``extractor_version`` (ключ ``extract_cache``
    и слагаемое ``pb``), и ``extractor_stack`` (слагаемое ``run_id``) собираются из этого
    вызова. Две независимые формулы разошлись бы — а расхождение как раз и означает
    «``run_id`` называет движок, которого не было».
    """
    return f"{_DOCLING_BUILD_PREFIX}{docling_library_version()}"


def docling_extractor_version(level: DocumentLevel | str) -> str:
    """``extractor_version`` документа: ``docling-doc-3:<level>/docling-<build>``.

    Пример: ``docling-doc-3:full/docling-2.135.0``. Уровень кодирует ось
    ``crawl.document_level`` (P5), сборка — фактически установленную библиотеку. Оба
    слагаемых входят в ключ ``extract_cache``: после апгрейда docling старый текст
    получается только под **старым** ключом и потому не находится.
    """
    return (
        f"{DOCLING_ADAPTER_FAMILY}:{DocumentLevel(level).value}/{docling_engine_build()}"
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
        import docling  # noqa: F401  # type: ignore[import-not-found]
        from docling.datamodel.base_models import (  # type: ignore[import-not-found]
            InputFormat as _InputFormat,
        )
        from docling.datamodel.pipeline_options import (  # type: ignore[import-not-found]
            NativePdfPipelineOptions as _NativePdfPipelineOptions,
        )
        from docling.datamodel.pipeline_options import (  # type: ignore[import-not-found]
            OcrAutoOptions as _OcrAutoOptions,
        )
        from docling.datamodel.pipeline_options import (  # type: ignore[import-not-found]
            PdfPipelineOptions as _PdfPipelineOptions,
        )
        from docling.datamodel.pipeline_options import (  # type: ignore[import-not-found]
            TableFormerMode as _TableFormerMode,
        )
        from docling.datamodel.pipeline_options import (  # type: ignore[import-not-found]
            TableStructureOptions as _TableStructureOptions,
        )
        from docling.document_converter import (  # type: ignore[import-not-found]
            DocumentConverter as _DocumentConverter,
        )
        from docling.document_converter import (  # type: ignore[import-not-found]
            NativePdfFormatOption as _NativePdfFormatOption,
        )
        from docling.document_converter import (  # type: ignore[import-not-found]
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
        from docling.models.stages.ocr.auto_ocr_model import (  # type: ignore[import-not-found]
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
        try:
            from docling.datamodel.settings import settings  # type: ignore[import-not-found]
        except ImportError:  # pragma: no cover - зависит от версии docling
            settings = None
        artifacts_path = getattr(settings, "artifacts_path", None)

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
    """
    input_doc = getattr(result, "input", None)
    total = getattr(input_doc, "page_count", None)
    if isinstance(total, int) and not isinstance(total, bool) and total > 0:
        return total
    pages = getattr(result, "pages", None)
    return len(pages) if pages is not None else None


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

    ``level`` задаёт уровень извлечения PDF (P5). ``version`` — инстансный и кодирует
    уровень и сборку движка (``docling-doc-3:<level>/docling-<build>``,
    :func:`docling_extractor_version`), чтобы смена уровня или апгрейд docling была видна
    и в ``index_build_id``, и в ключе ``extract_cache``. Классовый ``version`` — дефолт
    ``full`` (обратная совместимость по атрибуту).
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
    ) -> None:
        self._level = DocumentLevel(level)
        self.version = docling_extractor_version(self._level)
        # §6: явный таймаут оператора приоритетен; иначе — щедрый per-level дефолт.
        self._document_timeout = document_timeout
        self._converter = None
        #: OCR проверен (``full``): повторные PDF не пере-строят auto-OCR.
        self._ocr_checked = False

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
        ``extractor_version`` (``docling-doc-3:<level>/docling-<build>``) не включает эту
        ручку и от замера не меняется, поэтому записи ``extract_cache/`` остаются
        валидными (замер не инвалидирует извлечённый корпус). ``golden-full`` не требуется.

        Ветка ``text`` (``NativePdfPipelineOptions``) намеренно **без** этой ручки: у
        нативного пайплайна другой механизм и своя память (899 MB), поля ``queue_max_size``
        у него нет вовсе.
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
            return NativePdfPipelineOptions(document_timeout=timeout)
        if self._level is DocumentLevel.LAYOUT:
            return PdfPipelineOptions(
                do_table_structure=False,
                do_ocr=False,
                document_timeout=timeout,
                queue_max_size=PDF_QUEUE_MAX_SIZE,
            )
        if self._level is DocumentLevel.LAYOUT_TABLES:
            return PdfPipelineOptions(
                do_table_structure=True,
                table_structure_options=TableStructureOptions(
                    do_cell_matching=True, mode=TableFormerMode.FAST
                ),
                do_ocr=False,
                document_timeout=timeout,
                queue_max_size=PDF_QUEUE_MAX_SIZE,
            )
        # ``full`` — байт-в-байт дефолты docling (OCR включён, таблицы ACCURATE), кроме
        # ``queue_max_size`` (см. docstring: замер −44% RSS, markdown идентичен).
        return PdfPipelineOptions(
            document_timeout=timeout, queue_max_size=PDF_QUEUE_MAX_SIZE
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
        # Сборка конвертера — вне ``try``: ``OcrEngineUnavailableError`` обязан дойти до
        # фазы как ошибка конфигурации, а не превратиться в ``docling: ...``.
        converter = self._converter or self._build_converter(pdf=suffix == ".pdf")
        self._converter = converter
        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            return self._convert(converter, tmp_path)
        except Exception as exc:  # noqa: BLE001 - грациозная деградация на уровне адаптера
            return self._error(f"docling: {exc}")
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
        ``ExtractedDoc`` и тот же ``version``.
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
        if not source.is_file():
            # Видимая ошибка, а не падение: файл мог исчезнуть между retention и извлечением.
            return self._error(f"retained-файл не найден: {source}")
        converter = self._converter or self._build_converter(pdf=suffix == ".pdf")
        self._converter = converter
        try:
            with tempfile.TemporaryDirectory(prefix="cohound-extract-") as staging:
                staged = Path(staging) / f"document{suffix}"
                _stage_by_reference(source, staged)
                return self._convert(converter, str(staged))
        except Exception as exc:  # noqa: BLE001 - грациозная деградация на уровне адаптера
            return self._error(f"docling: {exc}")

    def _precheck(self, suffix: str) -> ExtractedDoc | None:
        """Общие проверки обоих входов; ``None`` — можно конвертировать.

        Формат, уровень ``off``, наличие docling и OCR-проверка для ``full`` PDF —
        ровно те же и в том же порядке, что были в ``extract_bytes``: входы
        (байты/путь) не меняют семантику извлечения.
        """
        if suffix not in SUPPORTED_FORMATS:
            return self._error(f"формат не поддерживается: {suffix}")
        if self._level is DocumentLevel.OFF:
            # ``off`` — решение pipeline пропустить документ, а не задача адаптера; прямой
            # вызов честно ошибается (никогда не пустой текст «молча»).
            return self._error("document_level=off: документы не извлекаются")
        # Ленивый импорт docling: ``available`` отвечает по ``find_spec`` (без импорта),
        # фактическое разрешение символов — здесь, на первом реальном документе. Строка
        # ошибки прежняя: «пакет есть, но не импортируется» и «пакета нет» для вызывающего
        # неразличимы — извлекать нечем в обоих случаях.
        if not _resolve_docling():
            return self._error("docling не установлен")
        # ``full`` обещает OCR именно для PDF; офисный документ OCR не использует, поэтому
        # проверка выполняется только на PDF-пути (окружение ``extract`` без OCR остаётся
        # рабочим). Ошибка — до конвертации: никакой текст без OCR не помечается ``full``.
        if suffix == ".pdf" and self._level is DocumentLevel.FULL:
            self._require_working_ocr(self._pdf_options())
        return None

    def _convert(self, converter: object, path: str) -> ExtractedDoc:
        """Общий хвост обоих входов: ``convert`` → markdown → ``ExtractedDoc``.

        Статус конвертации разбирается на три исхода, а не на два:

        * ``SUCCESS`` — полный текст, ``partial=False`` (прежнее поведение);
        * ``PARTIAL_SUCCESS`` — текст **части** документа, ``partial=True`` и причина
          (:meth:`_partial`). Пустой текст при этом статусе — не частичный успех, а
          провал: ``_error``;
        * любой другой статус — ``_error`` (прежняя строка ошибки).

        Любой сбой конвертации — грациозная деградация ``docling: <причина>`` (прежняя
        строка ошибки), а не исключение.
        """
        try:
            result = converter.convert(path)
            status = getattr(result.status, "name", str(result.status))
            if status == PARTIAL_SUCCESS_STATUS:
                doc = self._partial(result, status)
                if doc.error is None:
                    # Операционная видимость: артефакт прогона признак частичности пока не
                    # несёт (его место — ``corpus_docs``/манифест), поэтому факт «документ
                    # прочитан не целиком» обязан быть виден хотя бы в журнале, а не
                    # растворяться в числе документов.
                    _log.warning("частичное извлечение %s: %s", path, doc.partial_reason)
                return doc
            if status != "SUCCESS":
                return self._error(f"docling: статус конвертации {status}")
            text = result.document.export_to_markdown()
            pages = _page_count(result)
            return ExtractedDoc(
                text=text,
                content_hash=_sha256(text),
                extractor_version=self.version,
                page_count=pages,
            )
        except Exception as exc:  # noqa: BLE001 - грациозная деградация на уровне адаптера
            return self._error(f"docling: {exc}")

    def _partial(self, result: object, status: str) -> ExtractedDoc:
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
        """
        text = result.document.export_to_markdown()
        if not text.strip():
            return self._error(f"docling: статус конвертации {status} без текста")
        total = _page_count(result)
        converted = _converted_page_count(result)
        pages_note = ""
        if converted is not None and total is not None:
            pages_note = f", получено страниц {converted} из {total}"
        elif converted is not None:
            pages_note = f", получено страниц {converted}"
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
            extractor_version=self.version,
            page_count=total,
            partial=True,
            partial_reason=reason,
        )

    def _error(self, message: str) -> ExtractedDoc:
        return ExtractedDoc(
            text="",
            content_hash=_sha256(""),
            extractor_version=self.version,
            error=message,
        )


# Обратная совместимость с прежним именем (PDF-only вызовы).
DoclingPdfExtractor = DoclingDocumentExtractor
