"""7z-контейнер на ``py7zr`` — реализация ``ContainerPort`` (спуск, B2).

**Опциональный бэкенд.** ``py7zr`` живёт в extra ``extract-archives`` и в базовый набор
не входит (LGPL-исключение, ``docs/architecture/05-dependency-stack.md`` §2.4). Поэтому
импорт — **ленивый и охраняемый**: модуль импортируется и конструируется без пакета.
Отсутствие деградирует **видимо**:

* ``available()`` → ``False``;
* ``list_members`` → ``ContainerListing(unreadable="backend_unavailable: py7zr не
  установлен (extra extract-archives)")`` — не пустой состав и не исключение;
* ``open_member`` → ``ContainerUnavailable`` с той же причиной (потока нет по
  определению, поэтому здесь исключение — единственный честный ответ).

**Листинг — из заголовка 7z** (инвариант (a)): ``SevenZipFile.list()`` разбирает
заголовок, тела членов не читаются.

**Солидность — только если бэкенд её сообщает.** ``solid`` берётся из
``archiveinfo()`` **как есть**; если бэкенд поля не отдаёт или отдаёт не ``bool`` —
``None`` («не знаю»), а не догадка. Угадывание солидности сломало бы прогноз стоимости
спуска: в солидном архиве чтение одного члена стоит распаковки всего блока.

**Потоковое чтение — через временный файл (честная граница).** У ``py7zr`` нет
инкрементального читателя члена: ``read(targets=...)`` отдаёт ``BytesIO`` (член целиком
в RAM), поэтому адаптер извлекает **один** член во временный каталог и отдаёт его
чанками. RAM остаётся ограниченной (инвариант (b)), но распакованный член проходит
через диск — и в солидном архиве py7zr всё равно распаковывает блок целиком. Оба факта
зафиксированы в ``docs/architecture/09-product-onboarding-incremental-runs.md`` §6.7;
потолки (``archive_max_*``) остаются политикой, а не свойством библиотеки.

Нейтрален (§1.3).
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterator, Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

from core.ports.container import (
    ContainerListing,
    ContainerMember,
    ContainerUnavailable,
)

__all__ = ["CHUNK_SIZE", "BACKEND", "SevenZipContainer", "backend_module", "unavailable_reason"]

#: Имя опционального пакета-бэкенда.
BACKEND = "py7zr"

#: Видимая причина отсутствия бэкенда (одна формулировка на все три метода).
_REASON = "backend_unavailable: py7zr не установлен (extra extract-archives)"

#: Размер чанка потокового чтения члена (1 MiB).
CHUNK_SIZE = 1024 * 1024


def backend_module() -> ModuleType | None:
    """Модуль ``py7zr`` либо ``None`` (пакет не установлен).

    Отдельная функция, а не константа модуля: тесты подменяют её фейком, чтобы
    проверить форму успешного пути без установки LGPL-пакета.
    """
    try:
        import py7zr  # noqa: PLC0415 — ленивый импорт опционального бэкенда
    except ImportError:
        return None
    return py7zr


def unavailable_reason() -> str | None:
    """Причина недоступности бэкенда либо ``None`` (бэкенд есть)."""
    return None if backend_module() is not None else _REASON


def _field(entry: Any, name: str, default: Any = None) -> Any:
    """Поле записи индекса: ``py7zr`` отдаёт ``dict``, но принимаем и объект."""
    if isinstance(entry, Mapping):
        return entry.get(name, default)
    return getattr(entry, name, default)


def _archive_info(archive: Any) -> Any:
    """``archiveinfo()`` бэкенда либо ``None`` (бэкенд его не отдаёт / он упал)."""
    probe = getattr(archive, "archiveinfo", None)
    if not callable(probe):
        return None
    try:
        return probe()
    except Exception:  # noqa: BLE001 — провенанс не имеет права ронять листинг
        return None


def _solid(info: Any) -> bool | None:
    """Флаг солидности из индекса: ``bool`` либо ``None`` («бэкенд не сообщает»)."""
    raw = _field(info, "solid", None)
    return raw if isinstance(raw, bool) else None


def _encrypted(archive: Any, info: Any) -> bool:
    """Требует ли архив пароля: собственный предикат бэкенда, иначе — метод шифра."""
    probe = getattr(archive, "needs_password", None)
    if callable(probe):
        try:
            return bool(probe())
        except Exception:  # noqa: BLE001 — предикат бэкенда может упасть на битом файле
            pass
    methods = _field(info, "method_names", None) or ()
    return any("AES" in str(method).upper() for method in methods)


def _member(entry: Any) -> ContainerMember:
    """Запись индекса → :class:`ContainerMember` (размеры — заявленные)."""
    name = str(_field(entry, "filename", ""))
    size = _field(entry, "uncompressed", 0)
    compressed = _field(entry, "compressed", None)
    crc = _field(entry, "crc32", None)
    is_dir = _field(entry, "is_directory", None)
    return ContainerMember(
        path=name,
        size=int(size or 0),
        compressed_size=None if compressed is None else int(compressed),
        crc32=None if crc is None else int(crc),
        is_dir=bool(is_dir) if is_dir is not None else name.endswith("/"),
    )


def _contained(workdir: Path, member_path: str) -> Path:
    """Путь извлечённого члена внутри ``workdir`` (страховка от выхода за каталог)."""
    target = (workdir / member_path).resolve()
    if not target.is_relative_to(workdir.resolve()):
        raise ContainerUnavailable(f"member_path_rejected: {member_path}")
    return target


class SevenZipContainer:
    """``ContainerPort`` для ``7z`` (``py7zr``; при отсутствии — видимая деградация)."""

    @property
    def version(self) -> str:
        """Провенанс: ``py7zr-<ver>`` либо ``py7zr-unavailable`` (стабильно и непусто)."""
        module = backend_module()
        if module is None:
            return "py7zr-unavailable"
        raw = str(getattr(module, "__version__", "")).strip()
        return f"py7zr-{raw}" if raw else "py7zr-unknown"

    def available(self) -> bool:
        """Установлен ли ``py7zr`` (без исключений)."""
        return backend_module() is not None

    def unavailable_reason(self) -> str | None:
        """Причина недоступности бэкенда либо ``None``."""
        return unavailable_reason()

    def list_members(self, path: str) -> ContainerListing:
        """Индекс из заголовка 7z; битый архив → ``unreadable``, не исключение."""
        module = backend_module()
        if module is None:
            return ContainerListing(
                format="7z",
                members=(),
                solid=None,
                encrypted=False,
                truncated=False,
                unreadable=_REASON,
            )
        try:
            with module.SevenZipFile(path, "r") as archive:
                entries = list(archive.list())
                info = _archive_info(archive)
                solid = _solid(info)
                encrypted = _encrypted(archive, info)
        except Exception as exc:  # noqa: BLE001 — контракт порта: причина, не исключение
            return ContainerListing(
                format="7z",
                members=(),
                solid=None,
                encrypted=False,
                truncated=False,
                unreadable=f"{type(exc).__name__}: {exc}",
            )
        return ContainerListing(
            format="7z",
            members=tuple(_member(entry) for entry in entries),
            solid=solid,
            encrypted=encrypted,
            truncated=False,
            unreadable=None,
        )

    def open_member(self, path: str, member_path: str) -> Iterator[bytes]:
        """Поток тела члена по 1 MiB (через временный файл, см. модульный докстринг)."""
        return self._iter_member(path, member_path)

    def _iter_member(self, path: str, member_path: str) -> Iterator[bytes]:
        """Извлечь **один** член во временный каталог и отдать чанками."""
        module = backend_module()
        if module is None:
            raise ContainerUnavailable(_REASON)
        with tempfile.TemporaryDirectory(prefix="cohound-7z-") as workdir:
            with module.SevenZipFile(path, "r") as archive:
                archive.extract(path=workdir, targets=[member_path])
            target = _contained(Path(workdir), member_path)
            if not target.is_file():
                raise ContainerUnavailable(f"member_not_extracted: {member_path}")
            with target.open("rb") as handle:
                while True:
                    chunk = handle.read(CHUNK_SIZE)
                    if not chunk:
                        return
                    yield chunk
