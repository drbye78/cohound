"""PDF-экстрактор (docling → markdown) — реализация ``ExtractorPort`` для PDF.

Соответствует §3.2/§5 регламента (экстракция PDF через docling; блокер PDF-теста M0).
docling выбран осознанно: сохраняет таблицы/layout (критично для фич-матриц вендоров).

Происхождение идеи graceful-degradation (docling → ошибка, а не падение) и способа
вызова ``DocumentConverter.export_to_markdown()``: проект gag5 (`documents/parse.py`,
MIT, Copyright (c) 2024). Реализация написана заново (чистая, без доменных хвостов и
мёртвых веток gag5). См. THIRD_PARTY_NOTICES.md. Нейтрален (§1.3).

Зависимость ``docling`` — необязательная и тяжёлая; импортируется лениво.
"""

from __future__ import annotations

import hashlib
import tempfile
from dataclasses import dataclass
from pathlib import Path

try:  # тяжёлая необязательная зависимость
    import docling  # noqa: F401  # type: ignore[import-not-found]

    _DOCLING_AVAILABLE = True
except ImportError:  # pragma: no cover - зависит от окружения
    _DOCLING_AVAILABLE = False

try:
    from docling.document_converter import DocumentConverter  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    DocumentConverter = None  # type: ignore[assignment,misc]


@dataclass(frozen=True)
class ExtractedDoc:
    """Нормализованный документ после экстракции (§3.5, ``ExtractedDoc``)."""

    text: str
    content_hash: str
    extractor_version: str
    page_count: int | None = None
    error: str | None = None


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class DoclingPdfExtractor:
    """PDF → markdown через docling. Реализация ``ExtractorPort`` (PDF)."""

    version = "docling-pdf-1"

    def __init__(self, use_ocr: bool = False) -> None:
        self._use_ocr = use_ocr
        self._converter = None

    @property
    def available(self) -> bool:
        return _DOCLING_AVAILABLE and DocumentConverter is not None

    def extract_bytes(self, content: bytes) -> ExtractedDoc:
        """Извлекает markdown из байтов PDF (retention-единица — сырые байты, §3.2)."""
        if not self.available:
            return ExtractedDoc(
                text="", content_hash=_sha256(""), extractor_version=self.version,
                error="docling не установлен",
            )
        converter = self._converter or DocumentConverter()  # type: ignore[operator,misc]
        self._converter = converter
        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            result = converter.convert(tmp_path)
            status = getattr(result.status, "name", str(result.status))
            if status != "SUCCESS":
                return ExtractedDoc(
                    text="", content_hash=_sha256(""), extractor_version=self.version,
                    error=f"docling: статус конвертации {status}",
                )
            text = result.document.export_to_markdown()
            pages = len(result.pages) if hasattr(result, "pages") else None
            return ExtractedDoc(
                text=text, content_hash=_sha256(text), extractor_version=self.version,
                page_count=pages,
            )
        except Exception as exc:  # noqa: BLE001 - грациозная деградация на уровне адаптера
            return ExtractedDoc(
                text="", content_hash=_sha256(""), extractor_version=self.version,
                error=f"docling: {exc}",
            )
        finally:
            if tmp_path is not None:
                Path(tmp_path).unlink(missing_ok=True)
