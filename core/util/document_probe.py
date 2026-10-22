"""Классификация артефакта по голове файла — без рендеринга и без экстракции (B3).

Слой ``util``: чистая логика над файлом/байтами, без ``ports``/``adapters`` (матрица 01 §1).
Модуль отвечает на вопрос «что это за артефакт и сколько в нём страниц», **не** разбирая
содержимое:

* :func:`classify_artifact` — класс (``container``/``document``/``page``/``binary``) и формат;
* :func:`pdf_page_count` — число страниц PDF **по шапке/структуре файла** (``/Type /Pages``
  + ``/Count``), без рендеринга и без docling: рендер — работа экстрактора, а не зонда.

Почему счётчик страниц — по шапке: проба классифицирует артефакт **до** извлечения
(``archive_descent=plan`` вообще ничего не извлекает), и «сколько страниц в PDF» нужно
знать без запуска тяжёлого конвертера. Это **оценка по индексу файла**, а не гарантия:
PDF с перекрёстными ссылками в сжатых объектных потоках может не показать ``/Type /Pages``
открытым текстом — тогда функция честно возвращает ``None`` («шапка не сообщает»), а не
выдуманное число (A3).

Нейтрален (§1.3).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from core.util.document_formats import (
    BINARY_SNIFF_BYTES,
    DOCUMENT_SUFFIXES,
    classify_archive_bytes,
    document_suffix,
    is_textual_content_type,
)

__all__ = [
    "ArtifactProbe",
    "CLASS_BINARY",
    "CLASS_CONTAINER",
    "CLASS_DOCUMENT",
    "CLASS_PAGE",
    "PDF_MAGIC",
    "classify_artifact",
    "pdf_page_count",
    "read_head",
]

#: Классы артефакта: чем он станет для конвейера.
CLASS_CONTAINER = "container"
CLASS_DOCUMENT = "document"
CLASS_PAGE = "page"
CLASS_BINARY = "binary"

#: Магия PDF (перенос строки/пробелы после версии допускаются).
PDF_MAGIC = b"%PDF-"

#: Читаемый блок файла при сканировании шапки (1 MiB): счёт страниц не требует RAM.
_SCAN_CHUNK = 1024 * 1024

#: Перекрытие между блоками: токен ``/Type /Page`` или ``/Count N`` может лечь на границу.
_SCAN_OVERLAP = 256

#: ``/Type /Page`` — объект страницы. ``/Pages`` (дерево) отвергается явно: иначе
#: документ из 1 страницы давал бы 2 «страницы» (дерево + лист).
_PAGE_OBJECT_RE = re.compile(rb"/Type\s*/Page(?![A-Za-z])")

#: ``/Type /Pages ... /Count N`` — заявленное число листьев в дереве страниц. Между
#: ``/Type`` и ``/Count`` в одном словаре — не больше ~200 байт; окно ограничено, чтобы
#: не склеить ``/Pages`` одного объекта с ``/Count`` другого.
_PAGES_COUNT_RE = re.compile(rb"/Type\s*/Pages\b[^\n\r]{0,200}?/Count\s+(\d+)")

#: Суффиксы, которые спуск/экстрактор считают текстовой разметкой (маршрут ``auto``).
_TEXT_SUFFIXES: frozenset[str] = frozenset(
    {".md", ".markdown", ".txt", ".html", ".htm", ".xml", ".json", ".csv"}
)


@dataclass(frozen=True)
class ArtifactProbe:
    """Наблюдение класса артефакта (вход спуска/экстракции, провенанс прогона).

    ``kind`` — один из ``CLASS_*``; ``format`` — формат по байтам/суффиксу (``pdf``,
    ``zip``, ``docx``, ``md``…); ``size`` — фактический размер файла; ``page_count`` —
    число страниц PDF (``None`` — не PDF либо шапка его не сообщает). ``declared_format``
    — формат, заявленный URL/``Content-Type`` (может расходиться с байтами: суффикс врёт).
    """

    kind: str
    format: str | None
    suffix: str | None
    size: int
    page_count: int | None = None
    declared_format: str | None = None

    @property
    def is_container(self) -> bool:
        """Контейнер (``zip``/``7z``/``rar``) — вход спуска, а не экстрактора."""
        return self.kind == CLASS_CONTAINER

    def as_dict(self) -> dict[str, object]:
        """JSON-совместимое представление (стабильные ключи — провенанс)."""
        return {
            "kind": self.kind,
            "format": self.format,
            "suffix": self.suffix,
            "size": self.size,
            "page_count": self.page_count,
            "declared_format": self.declared_format,
        }


def read_head(path: str | Path, limit: int = BINARY_SNIFF_BYTES) -> bytes:
    """Первые ``limit`` байт файла (``b""``, если файл не читается).

    Голова, а не тело: классификация контейнера/документа решается магией байтов, а
    листинг архива — отдельный (порт контейнера читает только индекс).
    """
    try:
        with Path(path).open("rb") as handle:
            return handle.read(max(0, int(limit)))
    except OSError:
        return b""


def _declared_format(
    suffix: str | None, content_type: str | None
) -> str | None:
    """Формат, **заявленный** URL-суффиксом или ``Content-Type`` (не байтами)."""
    if suffix:
        return suffix.lstrip(".")
    media = (content_type or "").split(";")[0].strip().lower()
    if media == "application/pdf":
        return "pdf"
    if media in ("text/markdown", "text/x-markdown"):
        return "md"
    if media in ("text/plain",):
        return "txt"
    if media in ("text/html", "application/xhtml+xml"):
        return "html"
    if media == "application/zip":
        return "zip"
    return None


def pdf_page_count(path: str | Path) -> int | None:
    """Число страниц PDF по структуре файла (``None`` — не PDF или шапка не сообщает).

    Два независимых признака, оба из открытого текста файла:

    1. ``/Type /Pages ... /Count N`` — **заявленное** число листьев дерева страниц
       (авторитетно, когда видно);
    2. число объектов ``/Type /Page`` — фактическое число листьев, видимых в файле.

    Возвращается максимум из двух: PDF, у которого дерево объявляет 665 страниц, но
    часть листьев в сжатых объектных потоках, обязан дать 665, а не «сколько увидели».
    Файл читается блоками по 1 MiB с перекрытием 256 байт — токен на границе блоков не
    теряется, а RAM не зависит от размера документа.
    """
    declared = 0
    objects = 0
    seen_magic = False
    tail = b""
    try:
        with Path(path).open("rb") as handle:
            while True:
                chunk = handle.read(_SCAN_CHUNK)
                if not chunk:
                    break
                if not seen_magic:
                    seen_magic = chunk.lstrip()[: len(PDF_MAGIC)] == PDF_MAGIC
                    if not seen_magic:
                        return None
                window = tail + chunk
                objects += len(_PAGE_OBJECT_RE.findall(window))
                for match in _PAGES_COUNT_RE.finditer(window):
                    try:
                        value = int(match.group(1))
                    except ValueError:  # pragma: no cover - regex гарантирует цифры
                        continue
                    declared = max(declared, value)
                tail = window[-_SCAN_OVERLAP:]
    except OSError:
        return None
    if not seen_magic:
        return None
    best = max(declared, objects)
    return best if best > 0 else None


def classify_artifact(
    path: str | Path,
    *,
    url: str | None = None,
    content_type: str | None = None,
    head: bytes | None = None,
) -> ArtifactProbe:
    """Класс/формат артефакта по **байтам** (голова) с оглядкой на заявленный формат.

    Порядок решений:

    1. магия контейнера (``zip``/``7z``/``rar``) **и** заявленный формат не документ —
       ``container``. OOXML (``docx``/``pptx``/``xlsx``) тоже ZIP, но
       ``classify_archive_bytes`` отличает его по ``[Content_Types].xml``; кроме того,
       заявленный документный суффикс/``Content-Type`` оставляет файл документом даже с
       байтами обычного zip — его судьбу решает экстрактор, а не спуск;
    2. магия PDF либо документный суффикс/``Content-Type`` — ``document`` (+``page_count``
       для PDF);
    3. текстовая разметка (суффикс/``Content-Type``) — ``page``;
    4. иначе — ``binary``.

    ``head`` можно передать заранее прочитанным (тот же файл уже в памяти вызывающего).
    """
    path = Path(path)
    head_bytes = read_head(path) if head is None else head
    suffix = document_suffix(url or "") if url else path.suffix.lower() or None
    declared = _declared_format(suffix, content_type)
    try:
        size = path.stat().st_size
    except OSError:
        size = 0

    document_suffixes = frozenset(DOCUMENT_SUFFIXES)
    declared_document = (
        suffix in document_suffixes
        or declared in {item.lstrip(".") for item in document_suffixes}
    )
    archive_format = classify_archive_bytes(head_bytes)
    # Контейнер — только когда **заявленный** формат не документ: ``.docx`` с байтами
    # обычного zip остаётся документом (маршрут экстрактора, как и до спуска). Иначе
    # «zip без ``[Content_Types].xml``» отбирал бы у экстрактора документ, который он
    # умеет разбирать, и подменял бы ``parser_failed`` спуском по чужому классу.
    if archive_format is not None and not declared_document:
        return ArtifactProbe(
            kind=CLASS_CONTAINER,
            format=archive_format,
            suffix=suffix,
            size=size,
            declared_format=declared,
        )
    if head_bytes.lstrip()[: len(PDF_MAGIC)] == PDF_MAGIC:
        return ArtifactProbe(
            kind=CLASS_DOCUMENT,
            format="pdf",
            suffix=suffix,
            size=size,
            page_count=pdf_page_count(path),
            declared_format=declared,
        )
    if declared_document:
        return ArtifactProbe(
            kind=CLASS_DOCUMENT,
            format=(suffix or f".{declared}").lstrip("."),
            suffix=suffix,
            size=size,
            declared_format=declared,
        )
    if suffix in _TEXT_SUFFIXES or is_textual_content_type(content_type):
        return ArtifactProbe(
            kind=CLASS_PAGE,
            format=(suffix or declared or "").lstrip(".") or None,
            suffix=suffix,
            size=size,
            declared_format=declared,
        )
    return ArtifactProbe(
        kind=CLASS_BINARY,
        format=declared,
        suffix=suffix,
        size=size,
        declared_format=declared,
    )
