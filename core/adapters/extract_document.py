"""Экстрактор документов (docling → markdown) — реализация ``ExtractorPort``.

Поддерживает PDF и Office Open XML (docx/pptx/xlsx) единообразно через docling:
- PDF — layout-aware конвертация (таблицы/фич-матрицы, §3.2; блокер PDF-теста M0);
- DOCX/PPTX/XLSX — Office-бэкенды docling (python-docx/python-pptx/openpyxl);
  для этих форматов не требуется torch/layout-модели (``SimplePipeline``).

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

from core.ports.types import ExtractedDoc

try:  # тяжёлая необязательная зависимость
    import docling  # noqa: F401  # type: ignore[import-not-found]

    _DOCLING_AVAILABLE = True
except ImportError:  # pragma: no cover - зависит от окружения
    _DOCLING_AVAILABLE = False

try:
    from docling.document_converter import DocumentConverter  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    DocumentConverter = None  # type: ignore[assignment,misc]

# §3.2: расширяемый реестр поддерживаемых форматов.
# PDF — layout-aware (может требовать layout/OCR-модели); Office — лёгкий путь.
SUPPORTED_FORMATS: dict[str, str] = {
    ".pdf": "layout",
    ".docx": "office",
    ".pptx": "office",
    ".xlsx": "office",
}


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class DoclingDocumentExtractor:
    """Документ (PDF/DOCX/PPTX/XLSX) → markdown через docling. Реализация ``ExtractorPort``."""

    version = "docling-doc-2"

    def __init__(self, use_ocr: bool = False) -> None:
        self._use_ocr = use_ocr
        self._converter = None

    @property
    def available(self) -> bool:
        return _DOCLING_AVAILABLE and DocumentConverter is not None

    @staticmethod
    def supports(filename: str) -> bool:
        return Path(filename).suffix.lower() in SUPPORTED_FORMATS

    def extract_bytes(self, content: bytes, filename: str = "document.pdf") -> ExtractedDoc:
        """Извлекает markdown из байтов документа (retention-единица — байты, §3.2).

        ``filename`` определяет формат (расширение). Неподдерживаемое расширение →
        ``ExtractedDoc`` с ``error`` (без исключения).
        """
        suffix = Path(filename).suffix.lower() or ".pdf"
        if suffix not in SUPPORTED_FORMATS:
            return self._error(f"формат не поддерживается: {suffix}")
        if not self.available:
            return self._error("docling не установлен")

        converter = self._converter or DocumentConverter()  # type: ignore[operator,misc]
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
