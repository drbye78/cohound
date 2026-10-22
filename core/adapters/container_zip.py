"""ZIP-контейнер на stdlib ``zipfile`` — реализация ``ContainerPort`` (спуск, B2).

Почему stdlib, а не ``pyzipper``: обычный zip (deflate/stored) читается ``zipfile``
полностью, внешних бинарей не нужно, лицензия — PSF. ``pyzipper`` заявлен в extra
``extract-archives`` только ради **AES**-zip (WinZip AE-1/AE-2); до его подключения
AES-члены деградируют **видимо** — ``open_member`` поднимает ``RuntimeError`` от
``zipfile`` («encrypted, password required»), а ``list_members`` честно ставит
``encrypted=True`` (флаг ``0x1`` в общем бите).

**Листинг — только central directory** (инвариант (a)): ``infolist()`` разбирает индекс
архива, тела членов не читаются. Размеры — заявленные из индекса.

**Чтение — потоком** (инвариант (b)): ``ZipFile.open`` отдаёт файлоподобный объект
поверх потока архива, поэтому чанк 1 MiB не требует ни члена, ни архива в памяти.
ZIP **несолиден по построению**: каждый член — независимый deflate-поток, поэтому
``solid=False`` — факт формата, а не догадка.

Bomb-guard здесь **нет** (и не должно быть): ``zipfile`` не ограничивает распаковку,
потолки задаёт политика (``core.util.container_policy``: заявленный размер как первый
гейт + ``bounded_iter`` как жёсткий байтовый потолок на чтении).

Нейтрален (§1.3).
"""

from __future__ import annotations

import sys
import zipfile
from collections.abc import Iterator

from core.ports.container import ContainerListing, ContainerMember

__all__ = ["CHUNK_SIZE", "ZipContainer"]

#: Размер чанка потокового чтения члена (1 MiB): достаточно крупно для I/O, достаточно
#: мелко, чтобы пик памяти не зависел от размера члена.
CHUNK_SIZE = 1024 * 1024

#: Бит «член зашифрован» в general purpose flag (ZipCrypto и AES alike).
_ENCRYPTED_FLAG = 0x1

#: Ошибки, которые ``zipfile`` поднимает на битом/неподдерживаемом контейнере.
#: ``BadZipFile`` — не ZIP/повреждён; ``LargeZipFile`` — нужен ZIP64-путь записи
#: (на чтении почти не встречается, но контракт «не бросать» держим общим).
_LISTING_ERRORS: tuple[type[BaseException], ...] = (
    zipfile.BadZipFile,
    zipfile.LargeZipFile,
    EOFError,
    OSError,
)


class ZipContainer:
    """``ContainerPort`` для ``zip`` (stdlib ``zipfile``; всегда доступен)."""

    @property
    def version(self) -> str:
        """Провенанс: версия stdlib-читателя (``zipfile-3.14``)."""
        return f"zipfile-{sys.version_info.major}.{sys.version_info.minor}"

    def available(self) -> bool:
        """ZIP читается всегда: ``zipfile`` — часть stdlib."""
        return True

    def unavailable_reason(self) -> str | None:
        """Причина недоступности: у stdlib-читателя её нет."""
        return None

    def list_members(self, path: str) -> ContainerListing:
        """Индекс из central directory; битый архив → ``unreadable``, не исключение."""
        try:
            with zipfile.ZipFile(path) as archive:
                infos = archive.infolist()
        except _LISTING_ERRORS as exc:
            return ContainerListing(
                format="zip",
                members=(),
                solid=False,
                encrypted=False,
                truncated=False,
                unreadable=str(exc) or exc.__class__.__name__,
            )
        members = tuple(
            ContainerMember(
                path=info.filename,
                size=int(info.file_size),
                compressed_size=int(info.compress_size),
                crc32=int(info.CRC),
                is_dir=info.is_dir(),
            )
            for info in infos
        )
        return ContainerListing(
            format="zip",
            members=members,
            # ZIP несолиден по построению — это факт формата, а не «бэкенд не умеет».
            solid=False,
            encrypted=any(info.flag_bits & _ENCRYPTED_FLAG for info in infos),
            truncated=False,
            unreadable=None,
        )

    def open_member(self, path: str, member_path: str) -> Iterator[bytes]:
        """Поток тела члена по 1 MiB.

        Шифрованный член поднимет ``RuntimeError`` от ``zipfile`` (AES без ``pyzipper``
        не читается) — это видимый отказ, а не пустой поток: тихая подмена пустотой
        сделала бы «не прочитали» неотличимым от «прочитали пустое».
        """
        return self._iter_member(path, member_path)

    def _iter_member(self, path: str, member_path: str) -> Iterator[bytes]:
        """Генератор чанков: архив открывается на время итерации, не на всё чтение."""
        with zipfile.ZipFile(path) as archive, archive.open(member_path) as handle:
            while True:
                chunk = handle.read(CHUNK_SIZE)
                if not chunk:
                    return
                yield chunk
