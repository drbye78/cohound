"""coHound — нейтральный классификатор **документных** форматов (P5).

Единый источник истины для «это документный формат?» на двух осях:

* **суффикс** — по basename/path URL (pre-fetch-детект, без сети);
* **Content-Type** — по заголовку ответа (post-fetch-детект; суффикс-less URL).

Модуль вынесен в ``core/util`` (а не живёт в адаптере docling), потому что решение о
пропуске документов на уровне ``crawl.document_level=off`` принимает **pipeline**, которому
запрещено импортировать адаптеры (A1/слой-матрица). Сам реестр форматов категорийно-нейтрален
(расширения файлов и MIME-типы — данные, не вендоры; §1.3).

Поддерживаемые форматы совпадают с реестром экстрактора документов
(``core.adapters.extract_document.SUPPORTED_FORMATS``): PDF и Office Open XML.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from urllib.parse import unquote, urlsplit

__all__ = [
    "BINARY_SNIFF_BYTES",
    "DOCUMENT_CONTENT_TYPES",
    "DOCUMENT_SUFFIXES",
    "document_content_type",
    "document_suffix",
    "is_textual_content_type",
    "unsupported_binary_reason",
]

#: Сколько первых байт достаточно для бинарного sniff (NUL/HTML-маркер).
BINARY_SNIFF_BYTES = 8192
#: Порог доли control-символов (без tab/newline/CR/FF) в UTF-8 тексте.
_CONTROL_RATIO_MAX = 0.30
#: Текстовые MIME, которые считаются страницей/данными, а не бинарным артефактом.
_TEXT_CONTENT_TYPES: frozenset[str] = frozenset(
    {
        "application/xhtml+xml",
        "application/json",
        "application/ld+json",
        "application/xml",
        "application/x-xml",
        "application/rss+xml",
        "application/atom+xml",
        "application/javascript",
        "application/x-javascript",
        "application/svg+xml",
    }
)

#: Расширения документных форматов, поддерживаемых экстрактором (нижний регистр).
DOCUMENT_SUFFIXES: tuple[str, ...] = (".pdf", ".docx", ".pptx", ".xlsx")

#: MIME-тип → документный суффикс. Только типы, реально поддерживаемые экстрактором:
#: PDF и Office Open XML (docx/pptx/xlsx). Прочие (напр. старый бинарный MS Office,
#: ``application/octet-stream``) намеренно **не** классифицируются — иначе ``off``
#: пропускал бы не-документный контент под видом документа.
DOCUMENT_CONTENT_TYPES: dict[str, str] = {
    "application/pdf": ".pdf",
    "application/x-pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
}


def document_suffix(url_or_name: str) -> str | None:
    """Документный суффикс из basename URL/имени файла или ``None`` (pre-fetch-детект).

    Путь URL percent-декодируется (как scope-сравнение в ``crawl_frontier``), query/fragment
    отбрасываются: ``/download/report.pdf?x=1`` → ``.pdf``. Не-документный/без-суффикса →
    ``None`` (тогда тип узнаётся только после фетча, по ``Content-Type``).
    """
    path = urlsplit(url_or_name).path or url_or_name
    path = unquote(path)
    suffix = PurePosixPath(path).suffix.lower()
    return suffix if suffix in DOCUMENT_SUFFIXES else None


def document_content_type(content_type: str | None) -> str | None:
    """Документный суффикс по заголовку ``Content-Type`` или ``None`` (post-fetch-детект).

    Параметры ``;charset=...`` отбрасываются, регистр приводится к нижнему. Не-документный
    либо отсутствующий тип → ``None``.
    """
    mime = _mime(content_type)
    return DOCUMENT_CONTENT_TYPES.get(mime)


def _mime(content_type: str | None) -> str:
    """Нормализованный MIME без параметров (нижний регистр; пусто — ``""``)."""
    if not content_type:
        return ""
    return content_type.split(";", 1)[0].strip().lower()


def is_textual_content_type(content_type: str | None) -> bool:
    """Текстовый MIME (``text/*``, xhtml/json/xml-семейство) или ``False``."""
    mime = _mime(content_type)
    return bool(
        mime
        and (
            mime.startswith("text/")
            or mime in _TEXT_CONTENT_TYPES
            or mime.endswith("+xml")
            or mime.endswith("+json")
        )
    )


def _html_sniff(head: bytes) -> bool:
    """HTML-маркер в первых байтах (регистронезависимо, без полного декодирования)."""
    lowered = head.lstrip()[:512].lower()
    return lowered.startswith(b"<!doctype html") or b"<html" in lowered


def _pdf_magic(data: bytes) -> bool:
    """PDF-магия в начале (допускаем BOM/пробелы перед ``%PDF-``)."""
    return data.lstrip()[:1024].startswith(b"%PDF-")


def _zip_magic(data: bytes) -> bool:
    """ZIP-магия (OOXML — это ZIP; конкретный формат решает экстрактор)."""
    return data[:4] == b"PK\x03\x04"


def _control_ratio(text: str) -> float:
    """Доля control-символов (кроме обычного whitespace) в декодированном тексте."""
    if not text:
        return 0.0
    controls = sum(
        1 for ch in text if ord(ch) < 32 and ch not in "\t\n\r\f"
    )
    return controls / len(text)


def unsupported_binary_reason(
    content: bytes,
    *,
    url: str | None = None,
    content_type: str | None = None,
) -> str | None:
    """Причина «бинарный артефакт — не документ» либо ``None`` (payload разрешён).

    Порядок (осознанный, чтобы OVA/архивы не доходили до экстрактора):

    1. **Документный allowlist** (суффикс/MIME из этого модуля) — разрешаем: магию
       конкретного формата решает экстрактор;
    2. **Магия** ``%PDF-``/ZIP — разрешаем (суффикс-less ``document.pdf``);
    3. **NUL в первых 8 KiB** — бинарный артефакт (даже при ``text/plain``);
    4. **text/* / xhtml / json / xml / HTML-sniff** — разрешаем (если NUL не найден);
    5. иначе — UTF-8 + control-ratio; провал любого признака → бинарный.

    Возвращает человекочитаемую причину; решение о классе сбоя
    (``FailureKind.UNSUPPORTED_BINARY``) принимает pipeline.
    """
    if not content:
        return None
    if document_suffix(url or ""):
        return None
    if _mime(content_type) in DOCUMENT_CONTENT_TYPES:
        return None
    head = content[:BINARY_SNIFF_BYTES]
    if _pdf_magic(head) or _zip_magic(content):
        return None
    if b"\x00" in head:
        return "binary payload: NUL в первых 8192 байт"
    if is_textual_content_type(content_type) or _html_sniff(head):
        return None
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return "binary payload: не UTF-8"
    ratio = _control_ratio(text)
    if ratio > _CONTROL_RATIO_MAX:
        return (
            "binary payload: доля control-символов "
            f"{ratio:.0%} > {_CONTROL_RATIO_MAX:.0%}"
        )
    return None
