"""coHound — нейтральные «головные» сканеры форматов для шага ``probe`` (срез C2).

Модуль **не выполняет экстракцию** и не ходит в сеть/ФС: он разбирает уже прочитанные
байты и отвечает на технические вопросы о допущенном артефакте (I19: источник — только
ретенированные байты):

* PDF — сколько страниц заявлено в заголовке (без рендеринга и без docling);
* контейнеры ``zip``/``7z``/``rar`` — какие члены перечислены (только **имена**, без
  распаковки и без чтения членов);
* формат — по магии байтов и/или суффиксу/MIME (единый реестр
  :mod:`core.util.document_formats`).

Разбор «по заголовку» намеренно best-effort: сжатые object-stream'ы PDF или
зашифрованные/нестандартные контейнеры дают ``None``/пустой состав — это честный
результат, а не выдуманное число. Вызывающий обязан сохранить диагностику (``note``).

Нейтрален (§1.3): расширения/MIME — данные, не вендоры.
"""

from __future__ import annotations

import io
import struct
import zipfile

from core.util.document_formats import (
    BINARY_SNIFF_BYTES,
    classify_archive_bytes,
    document_content_type,
    document_suffix,
)

__all__ = [
    "MAX_MEMBER_LIST",
    "archive_format",
    "detect_format",
    "list_container_members",
    "pdf_page_count",
    "rar_members",
    "sevenzip_members",
    "zip_members",
]

#: Потолок членов, попадающих в провенанс (защита от гигантских оглавлений).
MAX_MEMBER_LIST = 10_000

#: Сигнатуры RAR4/RAR5 (первая — RAR4, вторая — RAR5).
_RAR4_MAGIC = b"Rar!\x1a\x07\x00"
_RAR5_MAGIC = b"Rar!\x1a\x07\x01\x00"

#: Маркеры страниц PDF. ``/Type /Page`` (не ``/Pages``) — объект страницы; пробелы и
#: переводы строк между токенами допустимы.
_PDF_PAGE_RE = None  # ленивая компиляция ниже (модуль без импорта re на горячем пути)


def _pdf_page_re():
    """Ленивая компиляция регекса страницы PDF (``re`` импортируется один раз)."""
    global _PDF_PAGE_RE
    if _PDF_PAGE_RE is None:
        import re

        _PDF_PAGE_RE = re.compile(rb"/Type\s*/Page(?![A-Za-z])")
    return _PDF_PAGE_RE


def pdf_page_count(data: bytes) -> int | None:
    """Число страниц PDF по заголовку (``/Type /Page``) либо ``None``.

    Это **не** полный парсер: сжатые object-stream'ы (PDF 1.5+) могут скрывать объекты
    страниц, тогда честно возвращается ``None``. Число не выдумывается.
    """
    if not data:
        return None
    head = data[:BINARY_SNIFF_BYTES]
    if b"%PDF-" not in head.lstrip()[:1024]:
        return None
    count = len(_pdf_page_re().findall(data))
    return count or None


def archive_format(head: bytes) -> str | None:
    """Формат контейнера по голове байтов (``zip``/``7z``/``rar``) либо ``None``.

    Переиспользует единый классификатор :func:`core.util.document_formats.classify_archive_bytes`
    (OOXML при этом документ, а не контейнер).
    """
    return classify_archive_bytes(head[:BINARY_SNIFF_BYTES])


def detect_format(
    head: bytes, url: str | None = None, content_type: str | None = None
) -> str | None:
    """Технический формат артефакта: ``pdf``/``zip``/``7z``/``rar``/office/``html``.

    Порядок: магия байтов (контейнер/PDF) → документный суффикс/MIME → ``html`` для
    остального. ``None`` допустим, если даже ``html`` не подтверждён пустой головой.
    """
    container = archive_format(head)
    if container is not None:
        return container
    stripped = head.lstrip()[:1024]
    if stripped.startswith(b"%PDF-"):
        return "pdf"
    suffix = document_suffix(url or "") or document_content_type(content_type)
    if suffix:
        return suffix.lstrip(".").lower()
    if not head:
        return None
    return "html"


def zip_members(data: bytes) -> list[str] | None:
    """Имена членов ZIP (central directory; OOXML тоже ZIP) либо ``None`` при сбое.

    Члены **не** распаковываются: ``zipfile`` читает только оглавление.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            names = archive.namelist()
    except (zipfile.BadZipFile, OSError, ValueError):
        return None
    return names[:MAX_MEMBER_LIST]


def _read_vint(data: bytes, pos: int) -> tuple[int, int]:
    """Читает RAR5 vint; возвращает ``(value, next_pos)``."""
    value = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ValueError("vint: конец данных")
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7
        if shift > 63:
            raise ValueError("vint: переполнение")


def _rar5_members(data: bytes) -> list[str]:
    """Перечисляет имена членов RAR5 (заголовки блоков, без распаковки данных)."""
    pos = len(_RAR5_MAGIC)
    members: list[str] = []
    while pos + 4 <= len(data) and len(members) < MAX_MEMBER_LIST:
        # CRC32 (4) + header_size (vint) + header data.
        pos += 4
        header_size, pos = _read_vint(data, pos)
        header_start = pos
        if header_size <= 0 or header_start + header_size > len(data):
            raise ValueError("rar5: некорректный размер заголовка")
        header = data[header_start : header_start + header_size]
        pos = header_start + header_size

        hpos = 0
        header_type, hpos = _read_vint(header, hpos)
        flags, hpos = _read_vint(header, hpos)
        extra_size = 0
        data_size = 0
        if flags & 0x0001:
            extra_size, hpos = _read_vint(header, hpos)
        if flags & 0x0002:
            data_size, hpos = _read_vint(header, hpos)
        # Тело заголовка файла: file_flags, unpacked_size, attributes, [mtime], [crc],
        # compression_info, host_os, name_size, name.
        if header_type == 2:  # file header
            file_flags, hpos = _read_vint(header, hpos)
            _unpacked, hpos = _read_vint(header, hpos)
            _attributes, hpos = _read_vint(header, hpos)
            if file_flags & 0x0002:  # mtime присутствует (4 байта)
                hpos += 4
            if file_flags & 0x0004:  # data CRC32 присутствует
                hpos += 4
            _compression, hpos = _read_vint(header, hpos)
            _host_os, hpos = _read_vint(header, hpos)
            name_size, hpos = _read_vint(header, hpos)
            if hpos + name_size > len(header):
                raise ValueError("rar5: имя выходит за заголовок")
            name = header[hpos : hpos + name_size].decode("utf-8", "replace")
            if name and not (file_flags & 0x0001):  # не каталог
                members.append(name)
        pos += data_size + extra_size
        if header_type == 5:  # end of archive
            break
    return members


def _rar4_members(data: bytes) -> list[str]:
    """Перечисляет имена членов RAR4 (блочные заголовки, без распаковки данных)."""
    pos = len(_RAR4_MAGIC)
    members: list[str] = []
    while pos + 7 <= len(data) and len(members) < MAX_MEMBER_LIST:
        _crc, head_type, flags, head_size = struct.unpack_from("<HBHH", data, pos)
        if head_size < 7 or pos + head_size > len(data):
            raise ValueError("rar4: некорректный размер заголовка")
        header = data[pos : pos + head_size]
        if head_type == 0x74:  # file header
            if head_size < 32:
                raise ValueError("rar4: короткий file header")
            pack_size, _unp_size = struct.unpack_from("<II", header, 7)
            offset = 32
            if flags & 0x0100:  # LHD_LARGE: high pack/unp sizes
                offset += 8
            name_size = struct.unpack_from("<H", header, 26)[0]
            name = header[offset : offset + name_size].decode("utf-8", "replace")
            if name and not (flags & 0x00E0):  # не каталог (LHD_WINDOWMASK не пусто)
                members.append(name)
            # Данные члена следуют за заголовком, если размер не включён в заголовок.
            data_size = 0 if flags & 0x8000 else pack_size
            pos += head_size + data_size
        else:
            pos += head_size
        if head_type == 0x7B:  # end of archive
            break
    return members


def rar_members(data: bytes) -> list[str] | None:
    """Имена членов RAR4/RAR5 либо ``None`` (не RAR/повреждён/не разобрано)."""
    try:
        if data.startswith(_RAR5_MAGIC):
            return _rar5_members(data)
        if data.startswith(_RAR4_MAGIC):
            return _rar4_members(data)
    except (ValueError, IndexError, struct.error):
        return None
    return None


def sevenzip_members(data: bytes) -> list[str] | None:
    """Имена членов 7z **не читаются** без распаковки заголовка (LZMA) → ``None``.

    Возврат ``None`` — честный ответ «состав не установлен», а не «контейнер пуст».
    """
    return None


def list_container_members(data: bytes, fmt: str) -> list[str] | None:
    """Перечисляет членов контейнера заявленного формата (имена, без извлечения)."""
    if fmt == "zip":
        return zip_members(data)
    if fmt == "rar":
        return rar_members(data)
    if fmt == "7z":
        return sevenzip_members(data)
    return None
