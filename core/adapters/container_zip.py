"""ZIP-контейнер на stdlib ``zipfile`` — реализация ``ContainerPort`` (спуск, B2).

Почему stdlib, а не ``pyzipper``: обычный zip (deflate/stored) читается ``zipfile``
полностью, внешних бинарей не нужно, лицензия — PSF. ``pyzipper`` не входит ни в один
extra: **AES**-zip (WinZip AE-1/AE-2) сознательно не поддержан (CVE-2026-44722 к тому же
запретил бы старые версии). AES-члены деградируют **видимо** — ``open_member`` поднимает ``RuntimeError`` от
``zipfile`` («encrypted, password required»), а ``list_members`` честно ставит
``ContainerEncryption.ENCRYPTED`` (флаг ``0x1`` в общем бите). Предикат у ZIP
**определённый**: флаг есть в central directory у каждого члена, поэтому незнание
(``.UNKNOWN``) у zip возможно только там, где индекс вообще не прочитан.

**Листинг — только central directory** (инвариант (a)): ``infolist()`` разбирает индекс
архива, тела членов не читаются. Размеры — заявленные из индекса.

**Чтение — потоком** (инвариант (b)): ``ZipFile.open`` отдаёт файлоподобный объект
поверх потока архива, поэтому чанк 1 MiB не требует ни члена, ни архива в памяти.
ZIP **несолиден по построению**: каждый член — независимый deflate-поток, поэтому
``solid=False`` — факт формата, а не догадка.

**Один ``ZipFile`` на архив, а не на члена** (S2/план 23). ``ZipFile`` разбирает central
directory в конструкторе, поэтому прежняя форма (открыть архив внутри генератора каждого
члена) читала индекс заново на каждого члена — O(n²) по числу членов, +60 % на распаковке
архива (замер: 80 мс против 50 мс). Теперь открытый архив живёт в адаптере под счётчиком
читателей: генератор члена берёт ссылку и возвращает её в ``finally`` (в том числе на
брошенной итерации — ``GeneratorExit``).

**Жизненный цикл — конец обхода, а не конец члена.** Закрывать архив в ``finally`` каждого
члена значило бы открывать его на каждого члена, то есть сохранять прежний O(n²); сигнала
«архив закончился» в порте нет. Поэтому закрытие детерминировано двумя точками: открытие
**другого** архива вытесняет простаивающие хендлы (архив со счётчиком 0 — его больше никто
не читает прямо сейчас), а ``close()`` закрывает всё в конце спуска (его зовёт
``ArchiveDescentWalker.descend``). Активный хендл не вытесняется и не закрывается под живым
читателем — за это отвечает счётчик, а не догадка о жизненном цикле. Вытеснение нужно и
для конечности дескрипторов: цепочка из 5000 различных вложенных архивов иначе упёрлась бы
в ``EMFILE``.

Bomb-guard здесь **нет** (и не должно быть): ``zipfile`` не ограничивает распаковку,
потолки задаёт политика (``core.util.container_policy``: заявленный размер как первый
гейт + ``bounded_iter`` как жёсткий байтовый потолок на чтении).

Нейтрален (§1.3).
"""

from __future__ import annotations

import sys
import threading
import zipfile
from collections.abc import Iterator

from core.domain.enums import ContainerEncryption
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

    def __init__(self) -> None:
        #: Открытые архивы: путь → ``(ZipFile, число активных читателей)``. Экземпляр
        #: диспетчера контейнеров — синглтон процесса (``container_any._ADAPTERS``),
        #: поэтому один открытый архив обслуживает всех членов этого архива.
        self._open: dict[str, tuple[zipfile.ZipFile, int]] = {}
        #: Замок счётчика: порт не запрещает читать членов из разных потоков.
        self._lock = threading.Lock()

    def close(self) -> None:
        """Закрывает открытые архивы — конец обхода (зовёт ``ArchiveDescentWalker``).

        Это единственная детерминированная точка закрытия: сигнала «архив закончился» в
        порте нет, поэтому переоткрытие на каждого члена и закрытие после каждого члена
        вернули бы ровно тот O(n²), который здесь устраняется. Хендл, ещё удерживаемый
        живым генератором, тоже закрывается: конец спуска означает, что читать дальше
        некому (``open_member`` после этого открывает архив заново).
        """
        with self._lock:
            entries = list(self._open.values())
            self._open.clear()
        for archive, _readers in entries:
            archive.close()

    def _acquire(self, path: str) -> zipfile.ZipFile:
        """Открытый архив пути (открывается при первом читателе, +1 к счётчику).

        Новый архив вытесняет **простаивающие** хендлы (счётчик 0): так дескрипторы не
        копятся по числу архивов, посещённых за спуск (цепочка вложенных архивов длиной
        5000 не упирается в лимит ``EMFILE``). Активные хендлы не трогаются.
        """
        evicted: list[zipfile.ZipFile] = []
        with self._lock:
            entry = self._open.get(path)
            if entry is not None:
                archive, readers = entry
                self._open[path] = (archive, readers + 1)
                return archive
            for other_path, (other, readers) in list(self._open.items()):
                if readers == 0:
                    del self._open[other_path]
                    evicted.append(other)
            archive = zipfile.ZipFile(path)
            self._open[path] = (archive, 1)
        for other in evicted:
            other.close()
        return archive

    def _release(self, path: str) -> None:
        """Возврат ссылки читателя. Архив **не** закрывается: следующий член возьмёт тот же хендл.

        Закрытие здесь означало бы одно открытие на члена, то есть прежний дефект. Закрывает
        ``close()`` в конце обхода (или вытеснение при открытии другого архива).
        """
        with self._lock:
            entry = self._open.get(path)
            if entry is None:  # pragma: no cover — парный acquire гарантирован
                return
            archive, readers = entry
            self._open[path] = (archive, max(0, readers - 1))

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
                # Индекс не прочитан ⇒ предикат шифрования не спрашивался: «не знаю», а
                # не «не зашифрован» (иначе отказ чтения останется без причины).
                encryption=ContainerEncryption.UNKNOWN,
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
            encryption=(
                ContainerEncryption.ENCRYPTED
                if any(info.flag_bits & _ENCRYPTED_FLAG for info in infos)
                else ContainerEncryption.NOT_ENCRYPTED
            ),
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
        """Генератор чанков: архив берётся из общего хендла, ссылка возвращается в ``finally``.

        ``ZipFile.open`` поднимает исключение на шифрованном члене — оно уходит наружу
        видимым отказом, а ссылка на архив всё равно возвращается ``finally``, поэтому
        неудачное чтение не оставляет счётчик задранным (иначе хендл никогда не вытеснится).
        """
        archive = self._acquire(path)
        try:
            handle = archive.open(member_path)
            try:
                while True:
                    chunk = handle.read(CHUNK_SIZE)
                    if not chunk:
                        return
                    yield chunk
            finally:
                handle.close()
        finally:
            # Исполняется и на исчерпании, и на ``GeneratorExit`` (брошенный/закрытый
            # генератор): архив не остаётся открытым после обхода.
            self._release(path)
