"""Экстрактор документов (docling → markdown) — реализация ``ExtractorPort``.

Поддерживает PDF и Office Open XML (docx/pptx/xlsx) единообразно через docling:
- PDF — layout-aware конвертация (таблицы/фич-матрицы, §3.2; блокер PDF-теста M0);
- DOCX/PPTX/XLSX — Office-бэкенды docling (python-docx/python-pptx/openpyxl);
  для этих форматов не требуется torch/layout-модели (``SimplePipeline``).

**P5 — уровень извлечения.** Качество PDF-экстракции задаётся осью
``crawl.document_level`` (:class:`~core.domain.enums.DocumentLevel`), а не «случайным»
дефолтом пустого ``DocumentConverter()``. Уровень входит в ``version`` экстрактора
(``docling-doc-3:<level>``) и тем самым — в ``index_build_id`` иммутабельной сборки (§13):
смена уровня не может молча дать ту же сборку.

* ``full`` (**дефолт**) — сегодняшнее эффективное поведение: layout Heron + таблицы
  TableFormer ``ACCURATE`` + ``do_cell_matching=True`` + авто-OCR (``OcrAutoOptions``),
  ``images_scale=1.0``; энричменты выключены. Это **байт-в-байт** опции дефолтного
  ``DocumentConverter()`` (ранее адаптер строил его без опций).
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

Происхождение идеи graceful-degradation (ошибка вместо падения) и способа вызова
``DocumentConverter.export_to_markdown()``: проект gag5 (`documents/parse.py`, MIT,
Copyright (c) 2024). Реализация написана заново (чистая, без доменных хвостов и мёртвых
веток gag5); расширена на Office-форматы. Нейтрален (§1.3).

Зависимость ``docling`` — необязательная и тяжёлая; импортируется лениво. Для офисных
форматов достаточно лёгкого extras ``docling-slim[format-office]`` (без torch).
"""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path

from core.domain.enums import DocumentLevel
from core.ports.types import ExtractedDoc
from core.util.document_formats import DOCUMENT_SUFFIXES

try:  # тяжёлая необязательная зависимость
    import docling  # noqa: F401  # type: ignore[import-not-found]

    _DOCLING_AVAILABLE = True
except ImportError:  # pragma: no cover - зависит от окружения
    _DOCLING_AVAILABLE = False

try:
    from docling.datamodel.base_models import InputFormat  # type: ignore[import-not-found]
    from docling.datamodel.pipeline_options import (  # type: ignore[import-not-found]
        NativePdfPipelineOptions,
        OcrAutoOptions,
        PdfPipelineOptions,
        TableFormerMode,
        TableStructureOptions,
    )
    from docling.document_converter import (  # type: ignore[import-not-found]
        DocumentConverter,
        NativePdfFormatOption,
        PdfFormatOption,
    )
except ImportError:  # pragma: no cover
    DocumentConverter = None  # type: ignore[assignment,misc]
    InputFormat = None  # type: ignore[assignment,misc]
    PdfFormatOption = None  # type: ignore[assignment,misc]
    NativePdfFormatOption = None  # type: ignore[assignment,misc]
    PdfPipelineOptions = None  # type: ignore[assignment,misc]
    NativePdfPipelineOptions = None  # type: ignore[assignment,misc]
    TableStructureOptions = None  # type: ignore[assignment,misc]
    TableFormerMode = None  # type: ignore[assignment,misc]
    OcrAutoOptions = None  # type: ignore[assignment,misc]

# §3.2: расширяемый реестр поддерживаемых форматов (единый источник — нейтральный
# ``core.util.document_formats``, чтобы pipeline мог детектировать документы, не импортируя
# адаптеры). PDF — layout-aware (может требовать layout/OCR-модели); Office — лёгкий путь.
SUPPORTED_FORMATS: dict[str, str] = {
    suffix: ("layout" if suffix == ".pdf" else "office") for suffix in DOCUMENT_SUFFIXES
}

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


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class DoclingDocumentExtractor:
    """Документ (PDF/DOCX/PPTX/XLSX) → markdown через docling. Реализация ``ExtractorPort``.

    ``level`` задаёт уровень извлечения PDF (P5). ``version`` — инстансный и кодирует
    уровень (``docling-doc-3:<level>``), чтобы смена уровня была видна в ``index_build_id``.
    Классовый ``version`` — дефолт ``full`` (обратная совместимость по атрибуту).
    """

    #: Классовый дефолт = уровень ``full`` (совпадает с ``DoclingDocumentExtractor().version``).
    version = "docling-doc-3:full"

    def __init__(
        self,
        level: DocumentLevel = DocumentLevel.FULL,
        *,
        document_timeout: float | None = None,
    ) -> None:
        self._level = DocumentLevel(level)
        self.version = f"docling-doc-3:{self._level.value}"
        # §6: явный таймаут оператора приоритетен; иначе — щедрый per-level дефолт.
        self._document_timeout = document_timeout
        self._converter = None

    @property
    def level(self) -> DocumentLevel:
        """Уровень извлечения этого экстрактора (P5)."""
        return self._level

    @property
    def available(self) -> bool:
        return _DOCLING_AVAILABLE and DocumentConverter is not None

    @staticmethod
    def supports(filename: str) -> bool:
        return Path(filename).suffix.lower() in SUPPORTED_FORMATS

    def _pdf_options(self) -> object:
        """Строит PDF-опции docling для своего уровня (P5).

        Возвращает ``PipelineOptions`` (уровни ``layout``/``layout_tables``/``full``) либо
        объект нативного пайплайна (``text``). ``off`` сюда не доходит штатно.
        """
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
            )
        if self._level is DocumentLevel.LAYOUT_TABLES:
            return PdfPipelineOptions(
                do_table_structure=True,
                table_structure_options=TableStructureOptions(
                    do_cell_matching=True, mode=TableFormerMode.FAST
                ),
                do_ocr=False,
                document_timeout=timeout,
            )
        # ``full`` — байт-в-байт дефолты docling (OCR включён, таблицы ACCURATE).
        return PdfPipelineOptions(document_timeout=timeout)

    def _build_converter(self):  # noqa: ANN202 - тип docling необязателен
        """Собирает ``DocumentConverter`` с per-level PDF-опциями (Office — дефолт docling)."""
        if self._level is DocumentLevel.OFF:
            return DocumentConverter()
        if self._level is DocumentLevel.TEXT:
            format_options = {
                InputFormat.PDF: NativePdfFormatOption(
                    pipeline_options=self._pdf_options()
                )
            }
        else:
            format_options = {
                InputFormat.PDF: PdfFormatOption(pipeline_options=self._pdf_options())
            }
        return DocumentConverter(format_options=format_options)

    def extract_bytes(self, content: bytes, filename: str = "document.pdf") -> ExtractedDoc:
        """Извлекает markdown из байтов документа (retention-единица — байты, §3.2).

        ``filename`` определяет формат (расширение). Неподдерживаемое расширение →
        ``ExtractedDoc`` с ``error`` (без исключения).
        """
        suffix = Path(filename).suffix.lower() or ".pdf"
        if suffix not in SUPPORTED_FORMATS:
            return self._error(f"формат не поддерживается: {suffix}")
        if self._level is DocumentLevel.OFF:
            # ``off`` — решение pipeline пропустить документ, а не задача адаптера; прямой
            # вызов честно ошибается (никогда не пустой текст «молча»).
            return self._error("document_level=off: документы не извлекаются")
        if not self.available:
            return self._error("docling не установлен")

        converter = self._converter or self._build_converter()
        self._converter = converter
        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            result = converter.convert(tmp_path)
            status = getattr(result.status, "name", str(result.status))
            if status != "SUCCESS":
                return self._error(f"docling: статус конвертации {status}")
            text = result.document.export_to_markdown()
            pages = len(result.pages) if hasattr(result, "pages") else None
            return ExtractedDoc(
                text=text,
                content_hash=_sha256(text),
                extractor_version=self.version,
                page_count=pages,
            )
        except Exception as exc:  # noqa: BLE001 - грациозная деградация на уровне адаптера
            return self._error(f"docling: {exc}")
        finally:
            if tmp_path is not None:
                Path(tmp_path).unlink(missing_ok=True)

    def _error(self, message: str) -> ExtractedDoc:
        return ExtractedDoc(
            text="",
            content_hash=_sha256(""),
            extractor_version=self.version,
            error=message,
        )


# Обратная совместимость с прежним именем (PDF-only вызовы).
DoclingPdfExtractor = DoclingDocumentExtractor
