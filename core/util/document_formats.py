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

**Контейнеры (архивы) — отдельный класс допуска (P6).** ``zip``/``7z``/``rar`` — не
документы, а **артефакты-контейнеры**, из которых спуск достаёт документы-члены. Заявленный
набор приходит из конфига (``crawl.attachment_formats``, дефолт ``("zip", "7z", "rar")``):
незaявленный контейнер (``.ova``/``.iso``/``.gz``/``.tar``) остаётся обычным бинарным
артефактом и отсекается как прежде. OOXML (``.docx``/``.pptx``/``.xlsx``) — это тоже ZIP,
но **документ**, а не контейнер: различаем по маркеру ``[Content_Types].xml``.

**Терминальный артефакт — надкласс допуска (P6b).** Контейнер **и** документ — оба
**терминальны**: их байты не продолжают навигацию (документ — лист для экстрактора,
контейнер — вход для спуска). Поэтому у них общий допуск: файловый приём под потолком
``crawl.max_artifact_bytes`` и обход path-prefix scope (host/SSRF/блэклист — по-прежнему
обязательны). Единый предикат — :func:`is_terminal_artifact`.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import PurePosixPath
from urllib.parse import unquote, urlsplit

__all__ = [
    "ATTACHMENT_CONTENT_TYPES",
    "ATTACHMENT_SUFFIXES",
    "BINARY_SNIFF_BYTES",
    "DOCUMENT_CONTENT_TYPES",
    "DOCUMENT_SUFFIXES",
    "SUPPORTED_ARCHIVE_FORMATS",
    "classify_archive_bytes",
    "document_content_type",
    "document_suffix",
    "is_attachment_content_type",
    "is_attachment_url",
    "is_terminal_artifact",
    "is_terminal_artifact_content_type",
    "is_terminal_artifact_url",
    "is_textual_content_type",
    "normalize_attachment_formats",
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

#: Форматы контейнеров, для которых у coHound есть читатель (дефолт оси
#: ``crawl.attachment_formats``). Порядок — канонический (для ``as_dict``/дайджеста).
SUPPORTED_ARCHIVE_FORMATS: tuple[str, ...] = ("zip", "7z", "rar")

#: Суффиксы контейнеров — объединение всех поддерживаемых форматов. Допуск по URL
#: **всегда** сужается заявленным набором (:func:`is_attachment_url`); константа нужна
#: только как дефолт «все поддерживаемые».
ATTACHMENT_SUFFIXES: tuple[str, ...] = tuple(f".{fmt}" for fmt in SUPPORTED_ARCHIVE_FORMATS)

#: MIME-тип → формат контейнера. ``application/octet-stream`` намеренно отсутствует:
#: общий бинарный тип не доказывает контейнер (тогда класс решает магия байтов).
ATTACHMENT_CONTENT_TYPES: dict[str, str] = {
    "application/zip": "zip",
    "application/x-zip": "zip",
    "application/x-zip-compressed": "zip",
    "application/x-7z-compressed": "7z",
    "application/x-7z": "7z",
    "application/x-rar": "rar",
    "application/x-rar-compressed": "rar",
    "application/vnd.rar": "rar",
}

#: Магия контейнеров: первый префикс — основной, остальные — варианты (пустой ZIP и RAR5).
_ARCHIVE_MAGIC: dict[str, tuple[bytes, ...]] = {
    "zip": (b"PK\x03\x04", b"PK\x05\x06"),
    "7z": (b"7z\xbc\xaf\x27\x1c",),
    "rar": (b"Rar!\x1a\x07",),
}

#: Маркер OOXML внутри ZIP: наличие означает документ, а не контейнер.
_OOXML_MARKER = b"[Content_Types].xml"


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


# --------------------------------------------------------------------------- #
# Контейнеры (архивы): класс допуска «артефакт» (P6)
# --------------------------------------------------------------------------- #


def normalize_attachment_formats(
    formats: Iterable[str] | None = None,
) -> tuple[str, ...]:
    """Канонический набор заявленных форматов-контейнеров (нижний регистр, без точек).

    ``None`` → все поддерживаемые (:data:`SUPPORTED_ARCHIVE_FORMATS`). Пустой набор
    допустим (контейнеры не заявлены — артефактов нет). Неподдерживаемый формат —
    ``ValueError``: конфиг не должен молча обещать несуществующий читатель.
    """
    if formats is None:
        return SUPPORTED_ARCHIVE_FORMATS
    normalized: list[str] = []
    for raw in formats:
        value = str(raw).strip().lower().lstrip(".")
        if not value:
            raise ValueError("attachment_formats: пустой формат контейнера")
        if value not in SUPPORTED_ARCHIVE_FORMATS:
            raise ValueError(
                f"attachment_formats: неподдерживаемый формат «{raw}»; "
                f"ожидается один из {list(SUPPORTED_ARCHIVE_FORMATS)}"
            )
        if value not in normalized:
            normalized.append(value)
    return tuple(normalized)


def is_attachment_url(
    url: str, formats: Iterable[str] | None = None
) -> bool:
    """Контейнер ли ``url`` по суффиксу basename (pre-fetch-детект).

    Учитывается **только** заявленный набор ``formats`` (:func:`normalize_attachment_formats`):
    ``.ova``/``.iso``/``.gz``/``.tar`` при дефолтном наборе — не артефакты.
    """
    declared = normalize_attachment_formats(formats)
    if not declared:
        return False
    path = urlsplit(url).path or url
    suffix = PurePosixPath(unquote(path)).suffix.lower()
    return bool(suffix) and suffix.lstrip(".") in declared


def is_attachment_content_type(
    content_type: str | None, formats: Iterable[str] | None = None
) -> bool:
    """Контейнер ли ответ по ``Content-Type`` (post-fetch-детект; суффикс-less URL).

    Общий бинарный тип (``application/octet-stream``) контейнером **не** считается:
    его класс решает магия байтов. Не заявленный формат → ``False``.
    """
    declared = normalize_attachment_formats(formats)
    fmt = ATTACHMENT_CONTENT_TYPES.get(_mime(content_type))
    return fmt is not None and fmt in declared


# --------------------------------------------------------------------------- #
# Терминальный артефакт: контейнер (P6) ИЛИ документ — общий класс допуска (P6b)
# --------------------------------------------------------------------------- #


def is_terminal_artifact_url(
    url: str, formats: Iterable[str] | None = None
) -> bool:
    """Терминальный артефакт ли ``url`` по суффиксу basename (pre-fetch-детект).

    «Терминальный» = ответ **не продолжает навигацию**: документ (``pdf``/``docx``/
    ``pptx``/``xlsx`` — лист для экстрактора) либо контейнер из заявленного набора
    (``zip``/``7z``/``rar`` — вход для спуска). Поэтому path-prefix scope к такому URL
    **не применяется** (он не «страница раздела»); host scope и SSRF-валидация
    применяются **всегда**.

    ``.rtf`` намеренно **не** входит в набор: у реестра экстрактора
    (``SUPPORTED_FORMATS`` = pdf/docx/pptx/xlsx) нет читателя RTF, поэтому допуск дал бы
    блокирующий ``parser_failed`` и мог бы заблокировать ``corpus_coverage.complete``.
    RTF ждёт своей полосы (``striprtf``), а не «заодно с документами».
    """
    return bool(document_suffix(url)) or is_attachment_url(url, formats)


def is_terminal_artifact_content_type(
    content_type: str | None, formats: Iterable[str] | None = None
) -> bool:
    """Терминальный артефакт ли ответ по ``Content-Type`` (post-fetch-детект).

    Тот же класс, что :func:`is_terminal_artifact_url`, но для суффикс-less URL
    (``/download?id=7``): документный MIME либо MIME заявленного контейнера. Общий
    бинарный тип (``application/octet-stream``) классом не является — его решает магия
    байтов.
    """
    return document_content_type(content_type) is not None or is_attachment_content_type(
        content_type, formats
    )


def is_terminal_artifact(
    url: str | None = None,
    content_type: str | None = None,
    attachment_formats: Iterable[str] | None = None,
) -> bool:
    """Единый предикат класса «терминальный артефакт» (суффикс **или** ``Content-Type``).

    «Терминальный» = ответ **не продолжает навигацию** (документ или контейнер), поэтому
    path-prefix scope к нему не применяется; host scope и SSRF-валидация — всегда.
    Контейнерная часть сужается заявленным ``attachment_formats``; документная — нет
    (документный allowlist совпадает с реестром экстрактора и от конфига не зависит).

    ``.rtf`` не допускается (см. :func:`is_terminal_artifact_url`): экстрактора нет.
    """
    if url is not None and is_terminal_artifact_url(url, attachment_formats):
        return True
    return is_terminal_artifact_content_type(content_type, attachment_formats)


def _container_magic(head: bytes) -> str | None:
    """Формат контейнера по магии первых байт (``zip``/``7z``/``rar``) либо ``None``."""
    for fmt, magics in _ARCHIVE_MAGIC.items():
        if any(head.startswith(magic) for magic in magics):
            return fmt
    return None


def classify_archive_bytes(head: bytes) -> str | None:
    """Формат контейнера по голове байтов: ``"zip"``/``"7z"``/``"rar"`` либо ``None``.

    ZIP-магия (``PK\\x03\\x04``/``PK\\x05\\x06``) **и** отсутствие маркера OOXML
    ``[Content_Types].xml``: OOXML — документ (его разбирает экстрактор), а не контейнер
    для спуска. ``7z``/RAR4/RAR5 определяются по собственной магии (OOXML-маркер к ним
    неприменим). Прочие форматы (tar/OVA/ISO/gzip) → ``None``: они не контейнеры допуска.

    Смотрится **только** голова (:data:`BINARY_SNIFF_BYTES`): вызывающему можно передать
    и целое тело — поиск маркера не пойдёт по членам контейнера (иначе zip с docx-членом
    ложно считался бы документом).
    """
    head = head[:BINARY_SNIFF_BYTES]
    fmt = _container_magic(head[:8])
    if fmt is None:
        return None
    if fmt == "zip" and _OOXML_MARKER in head:
        return None
    return fmt


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
    attachment_formats: Iterable[str] | None = None,
) -> str | None:
    """Причина «бинарный артефакт — не документ» либо ``None`` (payload разрешён).

    Порядок (осознанный, чтобы OVA/образы не доходили до экстрактора):

    1. **Документный allowlist** (суффикс/MIME из этого модуля) — разрешаем: магию
       конкретного формата решает экстрактор;
    2. **Магия** ``%PDF-``/ZIP — разрешаем (суффикс-less ``document.pdf``);
    2b. **Магия заявленного контейнера** (``zip``/``7z``/``rar`` из
       ``attachment_formats``) — разрешаем: это допущенный артефакт, а не бинарный мусор
       (решение о спуске принимает pipeline). Незаявленный контейнер сюда не попадает и
       отсекается ниже как бинарный;
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
    declared = normalize_attachment_formats(attachment_formats)
    if declared and _container_magic(head[:8]) in declared:
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
