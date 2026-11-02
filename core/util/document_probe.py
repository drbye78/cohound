"""Классификация артефакта по голове файла — без рендеринга и без экстракции (B3).

Слой ``util``: чистая логика над файлом/байтами, без ``ports``/``adapters`` (матрица 01 §1).
Модуль отвечает на вопрос «что это за артефакт и сколько в нём страниц», **не** разбирая
содержимое:

* :func:`classify_artifact` — класс (``container``/``document``/``page``/``binary``) и формат;
  OOXML узнаётся по **центральному каталогу** zip (:func:`ooxml_content_types`), а не по
  первым 8 KiB: иначе большой ``.docx`` без документарного суффикса классифицировался бы
  как контейнер и спуск отдал бы «пусто, ошибок нет» (F8);
* :func:`pdf_page_count` — число страниц PDF по **окнам** файла (шапка + хвост, потолок
  :data:`_SCAN_HEAD_BYTES`/:data:`_SCAN_TAIL_BYTES` на сторону): ``/Type /Pages`` +
  ``/Count`` из шапки/хвоста, без рендеринга и без docling. Полный файл читается только
  если он целиком укладывается в эти окна.

Почему счётчик страниц — по структуре, а не рендером: проба классифицирует артефакт
**до** извлечения (``archive_descent=plan`` вообще ничего не извлекает), и «сколько страниц
в PDF» нужно знать без запуска тяжёлого конвертера. Это **оценка по индексу файла**, а не
гарантия: PDF с перекрёстными ссылками в сжатых объектных потоках может не показать
``/Type /Pages`` открытым текстом — тогда функция честно возвращает ``None`` («не
сообщает»), а не выдуманное число (A3).

Нейтрален (§1.3).
"""

from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from core.util.document_formats import (
    BINARY_SNIFF_BYTES,
    DOCUMENT_SUFFIXES,
    OOXML_MARKER_NAME,
    TEXTUAL_SUFFIXES,
    classify_archive_bytes,
    document_suffix,
    is_textual_content_type,
)

__all__ = [
    "CLASS_BINARY",
    "CLASS_CONTAINER",
    "CLASS_DOCUMENT",
    "CLASS_PAGE",
    "PDF_MAGIC",
    "ArtifactProbe",
    "classify_artifact",
    "ooxml_content_types",
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

#: Читаемый блок файла при сканировании шапки/хвоста (1 MiB): счёт страниц не требует RAM.
_SCAN_CHUNK = 1024 * 1024

#: Потолок чтения **с начала** файла (1 MiB). Стоимость замера (чтение + два
#: regex-прохода): ≈1.4–1.7 мс на MiB, поэтому окно ограничивает цену пробы на документ
#: (~3 мс на оба окна), а не растёт линейно с размером файла: PDF на 128 MiB читался
#: целиком и стоил **177 мс** (замер: 1.3/9.1/29.3/176.8 мс на 1/8/32/128 MiB).
_SCAN_HEAD_BYTES = 1024 * 1024

#: Потолок чтения **с конца** файла (1 MiB): хвост несёт xref/trailer, а объект
#: ``/Type /Pages`` часто пишется последним — без хвоста заявленное ``/Count`` терялось бы.
_SCAN_TAIL_BYTES = 1024 * 1024

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
#:
#: Реестр чанкеров (:data:`core.util.document_formats.TEXTUAL_SUFFIXES`) берётся **целиком**:
#: собственный список здесь расходился с ним и записывал ``.mdx``/``.qmd``/``.rst``/``.log``/
#: ``.sql`` в ``binary`` — семейство для маршрутизации оставалось верным (там решает
#: ``textual_family``), а наблюдение артефакта лгало. Дополнительно — форматы страниц и
#: данных, которые чанкер как разметку не читает, но и бинарём не являются.
_TEXT_SUFFIXES: frozenset[str] = frozenset(TEXTUAL_SUFFIXES) | frozenset(
    {".html", ".htm", ".xml", ".json", ".csv"}
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


def _scan_text(handle: BinaryIO, start: int, length: int) -> tuple[int, int]:
    """``(declared, objects)`` по окну ``[start, start+length)`` файла.

    Окно читается блоками по :data:`_SCAN_CHUNK` с перекрытием :data:`_SCAN_OVERLAP`:
    токен на границе блоков не теряется, а RAM не зависит ни от окна, ни от файла.
    """
    declared = 0
    objects = 0
    tail = b""
    handle.seek(start)
    remaining = length
    while remaining > 0:
        chunk = handle.read(min(_SCAN_CHUNK, remaining))
        if not chunk:
            break
        remaining -= len(chunk)
        window = tail + chunk
        objects += len(_PAGE_OBJECT_RE.findall(window))
        for match in _PAGES_COUNT_RE.finditer(window):
            try:
                value = int(match.group(1))
            except ValueError:  # pragma: no cover - regex гарантирует цифры
                continue
            declared = max(declared, value)
        tail = window[-_SCAN_OVERLAP:]
    return declared, objects


def _pdf_windows(size: int) -> tuple[list[tuple[int, int]], bool]:
    """Окна чтения для файла размера ``size`` и признак «покрыт целиком».

    Файл целиком читается, только если он укладывается в шапку+хвост (тогда счётчик
    точен). Иначе читаются две ограниченные стороны — и результат может быть только
    **заявленным** ``/Count``, а не числом увиденных объектов (см. :func:`pdf_page_count`).
    """
    if size <= _SCAN_HEAD_BYTES + _SCAN_TAIL_BYTES:
        return [(0, size)], True
    head = _SCAN_HEAD_BYTES
    return [(0, head), (size - _SCAN_TAIL_BYTES, _SCAN_TAIL_BYTES)], False


def ooxml_content_types(path: str | Path) -> bool | None:
    """Есть ли запись ``[Content_Types].xml`` в **центральном каталоге** zip.

    ``True``/``False`` — каталог прочитан, ответ определён; ``None`` — файл не читается
    как zip (тогда класс решает не эта функция, а вызывающий: ``None`` — не «нет
    маркера»).

    Читается только центральный каталог (хвост файла), тела членов не читаются: цена —
    O(число записей), а не O(размер архива). Именно это делает распознавание OOXML
    независимым от смещения заголовка записи: у большого ``.docx`` маркер лежит далеко за
    первыми :data:`BINARY_SNIFF_BYTES` байтами (F8: замерено на 8258/9078/20078 —
    голова его не видит, и спуск отдавал «пусто, ошибок нет»).
    """
    try:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
    except (RecursionError, MemoryError):
        raise
    except Exception:  # noqa: BLE001 — «не читается как zip» — валидный ответ пробы
        return None
    for name in names:
        if name.replace("\\", "/").lstrip("./") == OOXML_MARKER_NAME:
            return True
    return False


def pdf_page_count(path: str | Path) -> int | None:
    """Число страниц PDF по структуре файла (``None`` — не PDF или число не сообщено).

    Читаются **два окна**: шапка (:data:`_SCAN_HEAD_BYTES`) и хвост
    (:data:`_SCAN_TAIL_BYTES`). Хвост нужен потому, что xref/trailer и объект
    ``/Type /Pages`` часто пишутся последними; шапка — потому что часть генераторов
    кладёт дерево страниц в начало. Файл целиком читается **только** если он целиком
    укладывается в эти окна (``size <= 2 MiB``) — тогда счётчик точен.

    Два независимых признака, оба из открытого текста файла:

    1. ``/Type /Pages ... /Count N`` — **заявленное** число листьев дерева страниц
       (авторитетно, когда видно);
    2. число объектов ``/Type /Page`` — фактическое число листьев, видимых в окнах.

    Возвращается максимум из двух, **если файл прочитан целиком**. Для файла больше окон
    возвращается только ``declared``: частичный пересчёт объектов занизил бы число, а
    выдуманное число хуже отсутствия (A3) — «увидели 6 листьев из 665» это не ответ.
    Ничего не найдено → ``None``.

    **Стоимость (замер, 3.14):** ≈1.4–1.7 мс на MiB прочитанного (чтение + два
    regex-прохода). Прежняя форма читала файл целиком, поэтому цена росла линейно с
    размером: 1/8/32/128 MiB → 1.3/9.1/29.3/**176.8** мс на артефакт. Теперь читается не
    больше двух окон (2 MiB) — 2.0–3.4 мс на тех же файлах, то есть цена пробы перестала
    зависеть от размера документа (>50× на 128 MiB).
    """
    try:
        size = Path(path).stat().st_size
    except OSError:
        return None
    if size <= 0:
        return None
    windows, complete = _pdf_windows(size)
    declared = 0
    objects = 0
    try:
        with Path(path).open("rb") as handle:
            # Магия — в самом начале (BOM/пробелы перед ``%PDF-`` допускаются): не PDF
            # считается без сканирования окон, цена отказа — 1 KiB чтения.
            probe = handle.read(min(len(PDF_MAGIC) + 1024, size)).lstrip()
            if not probe.startswith(PDF_MAGIC):
                return None
            for start, length in windows:
                part_declared, part_objects = _scan_text(handle, start, length)
                declared = max(declared, part_declared)
                objects += part_objects
    except OSError:
        return None
    best = max(declared, objects) if complete else declared
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

    1. ZIP-магия в голове **и** запись ``[Content_Types].xml`` в центральном каталоге —
        ``document`` (OOXML: ``docx``/``pptx``/``xlsx``; голова его не всегда видит —
        маркер ищется по каталогу, см. :func:`ooxml_content_types`). Заявленный
        документный суффикс/``Content-Type`` оставляет файл документом даже с байтами
        обычного zip: его судьбу решает экстрактор, а не спуск;
    2. магия контейнера (``zip``/``7z``/``rar``) **и** заявленный формат не документ —
       ``container``;
    3. магия PDF либо документный суффикс/``Content-Type`` — ``document`` (+``page_count``
       для PDF);
    4. текстовая разметка (суффикс/``Content-Type``) — ``page``;
    5. иначе — ``binary``.

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
    # OOXML за пределами головы: маркер ищется по центральному каталогу zip (F8). Без
    # этого большой ``.docx`` без документарного суффикса классифицировался как
    # ``container``, спуск не принимал ни одного члена (члены — ``word/document.xml``) и
    # наблюдение выходило ``descended`` с ``members_accepted=0`` и **пустыми** ``errors``:
    # документ молча терялся. Здесь он становится документом — маршрут экстрактора, а его
    # отказ виден как ``parser_failed``.
    if not declared_document and head_bytes[:2] == b"PK" and ooxml_content_types(path):
        return ArtifactProbe(
            kind=CLASS_DOCUMENT,
            format="ooxml",
            suffix=suffix,
            size=size,
            declared_format=declared,
        )
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
