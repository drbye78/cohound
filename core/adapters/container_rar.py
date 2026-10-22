"""RAR-контейнер на ``rarfile`` + внешняя утилита — реализация ``ContainerPort`` (B2).

**Два условия, а не одно.** ``rarfile`` — только *разбор* RAR4/RAR5; сжатые члены он
распаковывает **внешней утилитой** (``bsdtar``/``unrar``/``7z``). Поэтому
:meth:`RarContainer.available` честно сообщает **конъюнкцию**: пакет установлен **и**
утилита найдена. Причины различаются, чтобы пропуск был диагностируемым:

* нет пакета → ``backend_unavailable: rarfile не установлен (extra extract-archives)``;
* пакет есть, утилиты нет → ``backend_unavailable: нет внешней утилиты (bsdtar/unrar/7z)``.

Оговорка (честно): ``rarfile`` умеет читать **stored**-члены и без утилиты, но
порт-уровневый ответ «доступен» относился бы тогда лишь к части архивов. Порт
показывает консервативную правду: без утилиты спуск в RAR не заявляется.

**Листинг — из заголовка RAR** (инвариант (a)): ``infolist()`` разбирает заголовки,
тела членов не читаются. **Солидность — ``None``**: ``rarfile`` флаг сплошного блока не
отдаёт, а догадка исказила бы прогноз стоимости (в солидном RAR чтение члена стоит
распаковки блока).

**Чтение — потоком** (инвариант (b)): ``RarFile.open`` отдаёт файлоподобный объект
поверх процесса утилиты, поэтому чанк 1 MiB не требует члена в памяти.

Нейтрален (§1.3).
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator, Mapping
from types import ModuleType
from typing import Any

from core.ports.container import (
    ContainerListing,
    ContainerMember,
    ContainerUnavailable,
)

__all__ = [
    "BACKEND",
    "CHUNK_SIZE",
    "RarContainer",
    "backend_module",
    "external_tool",
    "unavailable_reason",
]

#: Имя опционального пакета-бэкенда.
BACKEND = "rarfile"

#: Внешние утилиты, которыми ``rarfile`` распаковывает сжатые члены (порядок — приоритет).
TOOL_NAMES: tuple[str, ...] = ("bsdtar", "unrar", "7z", "7zz")

#: Видимые причины недоступности (одна формулировка на все три метода).
_REASON_PACKAGE = "backend_unavailable: rarfile не установлен (extra extract-archives)"
_REASON_TOOL = "backend_unavailable: нет внешней утилиты (bsdtar/unrar/7z)"

#: Размер чанка потокового чтения члена (1 MiB).
CHUNK_SIZE = 1024 * 1024


def backend_module() -> ModuleType | None:
    """Модуль ``rarfile`` либо ``None`` (пакет не установлен).

    Отдельная функция, а не константа: тесты подменяют её фейком, чтобы проверить форму
    успешного пути без установки пакета.
    """
    try:
        import rarfile  # noqa: PLC0415 — ленивый импорт опционального бэкенда
    except ImportError:
        return None
    return rarfile


def external_tool() -> str | None:
    """Путь к внешней утилите (``bsdtar``/``unrar``/``7z``) либо ``None``.

    Сначала ``PATH`` (детерминированный порядок :data:`TOOL_NAMES`), затем — предикат
    самого ``rarfile`` (``tool_setup``), который учитывает и настроенный в окружении
    инструмент; в этом случае имя утилиты неизвестно и отдаётся ``"external"``.
    """
    for name in TOOL_NAMES:
        found = shutil.which(name)
        if found:
            return found
    module = backend_module()
    probe = getattr(module, "tool_setup", None) if module is not None else None
    if not callable(probe):
        return None
    try:
        result = probe()
    except Exception:  # noqa: BLE001 — предикат бэкенда не имеет права ронять проверку
        return None
    setup = result[0] if isinstance(result, tuple) and result else result
    return "external" if setup else None


def unavailable_reason() -> str | None:
    """Причина недоступности RAR-читателя либо ``None`` (пакет **и** утилита есть)."""
    if backend_module() is None:
        return _REASON_PACKAGE
    if external_tool() is None:
        return _REASON_TOOL
    return None


def _unavailable_listing(reason: str) -> ContainerListing:
    """Листинг «прочитать нельзя» с видимой причиной (состав пуст, формат известен)."""
    return ContainerListing(
        format="rar",
        members=(),
        solid=None,
        encrypted=False,
        truncated=False,
        unreadable=reason,
    )


def _field(entry: Any, name: str, default: Any = None) -> Any:
    """Поле записи индекса: ``rarfile`` отдаёт объект, но принимаем и ``dict``."""
    if isinstance(entry, Mapping):
        return entry.get(name, default)
    return getattr(entry, name, default)


def _is_dir(entry: Any) -> bool:
    """Запись-каталог (``rarfile`` отдаёт метод ``is_dir()``)."""
    probe = getattr(entry, "is_dir", None)
    if callable(probe):
        try:
            return bool(probe())
        except Exception:  # noqa: BLE001 — битый заголовок не должен ронять листинг
            pass
    return str(_field(entry, "filename", "")).endswith("/")


def _member(entry: Any) -> ContainerMember:
    """Запись индекса → :class:`ContainerMember` (размеры — заявленные)."""
    size = _field(entry, "file_size", 0)
    compressed = _field(entry, "compress_size", None)
    crc = _field(entry, "CRC", None)
    return ContainerMember(
        path=str(_field(entry, "filename", "")),
        size=int(size or 0),
        compressed_size=None if compressed is None else int(compressed),
        crc32=None if crc is None else int(crc),
        is_dir=_is_dir(entry),
    )


def _encrypted(archive: Any, infos: list[Any]) -> bool:
    """Требует ли архив пароля: предикат архива, иначе — предикаты членов."""
    probe = getattr(archive, "needs_password", None)
    if callable(probe):
        try:
            return bool(probe())
        except Exception:  # noqa: BLE001 — битый заголовок не должен ронять листинг
            pass
    for info in infos:
        member_probe = getattr(info, "needs_password", None)
        if callable(member_probe):
            try:
                if member_probe():
                    return True
            except Exception:  # noqa: BLE001
                continue
    return False


class RarContainer:
    """``ContainerPort`` для ``rar`` (``rarfile`` + внешняя утилита)."""

    @property
    def version(self) -> str:
        """Провенанс: ``rarfile-<ver>+<tool>`` либо ``rarfile-unavailable``."""
        module = backend_module()
        if module is None:
            return "rarfile-unavailable"
        raw = str(getattr(module, "__version__", "")).strip() or "unknown"
        tool = external_tool()
        if tool is None:
            return f"rarfile-{raw}+no-tool"
        return f"rarfile-{raw}+{tool.rsplit('/', 1)[-1]}"

    def available(self) -> bool:
        """Пакет **и** внешняя утилита (без исключений)."""
        return unavailable_reason() is None

    def unavailable_reason(self) -> str | None:
        """Причина недоступности либо ``None``."""
        return unavailable_reason()

    def list_members(self, path: str) -> ContainerListing:
        """Индекс из заголовков RAR; битый архив → ``unreadable``, не исключение."""
        reason = unavailable_reason()
        if reason is not None:
            return _unavailable_listing(reason)
        module = backend_module()
        if module is None:  # pragma: no cover — недостижимо: причина уже проверена
            return _unavailable_listing(_REASON_PACKAGE)
        try:
            with module.RarFile(path) as archive:
                infos = list(archive.infolist())
                encrypted = _encrypted(archive, infos)
        except Exception as exc:  # noqa: BLE001 — контракт порта: причина, не исключение
            return ContainerListing(
                format="rar",
                members=(),
                solid=None,
                encrypted=False,
                truncated=False,
                unreadable=f"{type(exc).__name__}: {exc}",
            )
        return ContainerListing(
            format="rar",
            members=tuple(_member(info) for info in infos),
            # rarfile флаг сплошного блока не отдаёт: «не знаю», а не догадка.
            solid=None,
            encrypted=encrypted,
            truncated=False,
            unreadable=None,
        )

    def open_member(self, path: str, member_path: str) -> Iterator[bytes]:
        """Поток тела члена по 1 MiB (``rarfile`` читает из процесса утилиты)."""
        return self._iter_member(path, member_path)

    def _iter_member(self, path: str, member_path: str) -> Iterator[bytes]:
        """Генератор чанков; пароль/битый член — исключение бэкенда (видимый отказ)."""
        reason = unavailable_reason()
        if reason is not None:
            raise ContainerUnavailable(reason)
        module = backend_module()
        if module is None:  # pragma: no cover — недостижимо: причина уже проверена
            raise ContainerUnavailable(_REASON_PACKAGE)
        with module.RarFile(path) as archive, archive.open(member_path) as handle:
            while True:
                chunk = handle.read(CHUNK_SIZE)
                if not chunk:
                    return
                yield chunk
