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
    "DOCUMENT_CONTENT_TYPES",
    "DOCUMENT_SUFFIXES",
    "document_content_type",
    "document_suffix",
]

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
    if not content_type:
        return None
    mime = content_type.split(";", 1)[0].strip().lower()
    return DOCUMENT_CONTENT_TYPES.get(mime)
