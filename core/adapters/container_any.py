"""Диспетчер контейнеров: формат по голове байтов → адаптер (спуск, B2).

Класс ответа уже решён **до** этого модуля: ``core.util.document_formats.classify_archive_bytes``
отличает контейнер от документа (OOXML — тоже ZIP, но документ: его разбирает
экстрактор, а не спуск). Здесь — только выбор читателя по формату и чтение головы файла.

Почему по **голове**, а не по суффиксу: суффикс врёт (``.zip`` с PDF внутри, ``.rar``
переименованный в ``.bin``), а магия байтов — то, что реально определяет читателя.
Голова — 8 KiB (``BINARY_SNIFF_BYTES``): этого достаточно для магии всех трёх форматов и
для маркера OOXML, и это не «чтение архива».

Неизвестный формат → ``None`` (не исключение) плюс :func:`unknown_reason` — видимая
формулировка для пропуска спуска. Известный формат с отсутствующим бэкендом → адаптер
(он честно отдаст ``unreadable="backend_unavailable: ..."``): «не умею читать» и «это не
контейнер» — разные факты, и путать их нельзя.

Нейтрален (§1.3).
"""

from __future__ import annotations

from pathlib import Path

from core.adapters.container_7z import SevenZipContainer
from core.adapters.container_rar import RarContainer
from core.adapters.container_zip import ZipContainer
from core.ports.container import ContainerPort
from core.util.document_formats import BINARY_SNIFF_BYTES, classify_archive_bytes

__all__ = [
    "SNIFF_BYTES",
    "classify_archive_bytes",
    "container_for",
    "container_for_head",
    "container_formats",
    "sniff_head",
    "unavailable_reason",
    "unknown_reason",
]

#: Сколько первых байт файла достаточно для выбора читателя (8 KiB).
SNIFF_BYTES = BINARY_SNIFF_BYTES

#: Синглтон-читатели по формату (адаптеры без состояния; листинг — чистая функция файла).
_ADAPTERS: dict[str, ContainerPort] = {
    "zip": ZipContainer(),
    "7z": SevenZipContainer(),
    "rar": RarContainer(),
}

#: Маркер OOXML внутри ZIP: такой файл — документ, а не контейнер для спуска.
_OOXML_MARKER = b"[Content_Types].xml"


def container_formats() -> tuple[str, ...]:
    """Форматы, для которых есть читатель (порядок канонический)."""
    return tuple(_ADAPTERS)


def sniff_head(path: str | Path) -> bytes:
    """Первые :data:`SNIFF_BYTES` байт файла (пустая строка — файл не читается)."""
    try:
        with Path(path).open("rb") as handle:
            return handle.read(SNIFF_BYTES)
    except OSError:
        return b""


def container_for_head(head: bytes) -> ContainerPort | None:
    """Читатель по голове байтов либо ``None`` (не контейнер допуска)."""
    fmt = classify_archive_bytes(head)
    if fmt is None:
        return None
    return _ADAPTERS.get(fmt)


def container_for(path: str | Path) -> ContainerPort | None:
    """Читатель по первым 8 KiB файла либо ``None`` (не контейнер / файл не читается)."""
    return container_for_head(sniff_head(path))


def unknown_reason(head: bytes) -> str:
    """Видимая причина «контейнера нет» (для пропуска спуска, не для исключения).

    OOXML отделён от «просто не контейнер» намеренно: ``docx``/``pptx``/``xlsx`` — ZIP
    по магии, и без этой развилки причина «не распознан как контейнер» выглядела бы
    ошибкой распознавания вместо правильного маршрута (документ → экстрактор).
    """
    if head.startswith(b"PK") and _OOXML_MARKER in head:
        return "not_a_container: OOXML — документ, а не контейнер (маршрут экстрактора)"
    return "not_a_container: голова не распознана как zip/7z/rar"


def unavailable_reason(port: ContainerPort) -> str | None:
    """Причина недоступности читателя либо ``None`` (читатель доступен).

    ``ContainerPort`` не требует метода причины (контракт минимален), поэтому адаптеры
    отдают его как расширение; читатель без такого метода считается доступным.
    """
    probe = getattr(port, "unavailable_reason", None)
    if callable(probe):
        return probe()
    return None if port.available() else "backend_unavailable: читатель недоступен"
