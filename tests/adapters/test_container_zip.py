"""ZIP-контейнер на stdlib: ``core/adapters/container_zip.py`` (срез B2).

Архивы строятся **здесь же** stdlib-``zipfile`` (сети и внешних бэкендов нет). Проверяется
ровно то, ради чего порт существует:

* листинг совпадает с индексом (имена, заявленные размеры, CRC) и **не** извлекает:
  архив с испорченным телом члена листается, а чтение этого члена честно падает;
* чтение — поток чанков (несколько чанков на член > 2 MiB, пик памяти не растёт до
  размера члена);
* шифрование видно флагом (``encrypted=True``), а чтение шифрованного члена отказывает
  видимо, не отдавая пустоту;
* битый/отсутствующий файл → ``unreadable``, без исключения;
* OOXML (``docx``-подобный zip) классифицируется как **документ**, а не контейнер.
"""

from __future__ import annotations

import tracemalloc
import zipfile
from pathlib import Path

import pytest

from core.adapters.container_any import (
    container_for,
    container_for_head,
    unavailable_reason,
    unknown_reason,
)
from core.adapters.container_zip import CHUNK_SIZE, ZipContainer
from core.util.document_formats import classify_archive_bytes

_PAYLOAD = b"coHound container payload\n"
_LARGE = (b"0123456789abcdef" * 65536) * 3  # 3 MiB ровно, повторяемый шаблон


def _make_zip(
    path: Path,
    members: dict[str, bytes],
    *,
    compression: int = zipfile.ZIP_STORED,
    dirs: tuple[str, ...] = (),
) -> Path:
    """Собрать zip с заданными членами (порядок — как в ``dict``)."""
    with zipfile.ZipFile(path, "w", compression=compression) as archive:
        for name in dirs:
            archive.writestr(zipfile.ZipInfo(name), b"")
        for name, data in members.items():
            archive.writestr(name, data)
    return path


def _set_encrypted_flag(path: Path) -> int:
    """Выставить бит «зашифрован» в central directory (stdlib писать шифрованный не умеет).

    Правка только индекса: ``infolist()`` читает флаги из central directory, тела членов
    остаются как были. Возвращает число поправленных записей.
    """
    data = bytearray(path.read_bytes())
    signature = b"PK\x01\x02"
    offset = 0
    patched = 0
    while True:
        offset = data.find(signature, offset)
        if offset < 0:
            path.write_bytes(bytes(data))
            return patched
        flags = int.from_bytes(data[offset + 8 : offset + 10], "little") | 0x1
        data[offset + 8 : offset + 10] = flags.to_bytes(2, "little")
        patched += 1
        offset += len(signature)


# --------------------------------------------------------------------------- #
# Инвариант (a): листинг читает индекс, а не тела
# --------------------------------------------------------------------------- #


def test_listing_matches_index(tmp_path: Path) -> None:
    """Имена, заявленные размеры и CRC — из индекса контейнера."""
    path = _make_zip(
        tmp_path / "a.zip",
        {"docs/a.pdf": _PAYLOAD, "notes.txt": b"x" * 100},
        dirs=("docs/",),
    )
    listing = ZipContainer().list_members(str(path))
    assert listing.format == "zip"
    assert listing.unreadable is None
    assert listing.encrypted is False
    assert listing.truncated is False
    # ZIP несолиден по построению — факт формата, а не «не знаю».
    assert listing.solid is False
    by_name = {member.path: member for member in listing.members}
    assert set(by_name) == {"docs/", "docs/a.pdf", "notes.txt"}
    assert by_name["docs/"].is_dir is True
    assert by_name["docs/a.pdf"].is_dir is False
    assert by_name["docs/a.pdf"].size == len(_PAYLOAD)
    assert by_name["notes.txt"].size == 100
    assert by_name["docs/a.pdf"].crc32 == zipfile.crc32(_PAYLOAD)
    assert by_name["docs/a.pdf"].compressed_size == len(_PAYLOAD)  # stored


def test_listing_does_not_read_member_bodies(tmp_path: Path) -> None:
    """Испорченное тело члена не мешает листингу — доказательство «индекс, не извлечение»."""
    path = _make_zip(tmp_path / "corrupt-body.zip", {"big.bin": _LARGE})
    data = bytearray(path.read_bytes())
    middle = len(data) // 2
    data[middle : middle + 1024] = b"\xff" * 1024
    path.write_bytes(bytes(data))

    listing = ZipContainer().list_members(str(path))
    assert listing.unreadable is None
    assert [member.path for member in listing.members] == ["big.bin"]
    assert listing.members[0].size == len(_LARGE)

    # А тело при чтении — действительно битое (CRC не сходится): видимый отказ, не тишина.
    with pytest.raises(zipfile.BadZipFile):
        b"".join(ZipContainer().open_member(str(path), "big.bin"))


def test_listing_empty_zip(tmp_path: Path) -> None:
    """Пустой архив — валидный контейнер без членов."""
    path = _make_zip(tmp_path / "empty.zip", {})
    listing = ZipContainer().list_members(str(path))
    assert listing.members == ()
    assert listing.unreadable is None


# --------------------------------------------------------------------------- #
# Инвариант (b): чтение потоком
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("compression", [zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED])
def test_streaming_read_yields_exact_bytes(tmp_path: Path, compression: int) -> None:
    """Поток собирается в ровно те байты, что записаны (stored и deflate)."""
    path = _make_zip(
        tmp_path / "read.zip", {"docs/a.pdf": _PAYLOAD}, compression=compression
    )
    chunks = list(ZipContainer().open_member(str(path), "docs/a.pdf"))
    assert b"".join(chunks) == _PAYLOAD


def test_streaming_read_is_chunked_and_memory_bounded(tmp_path: Path) -> None:
    """Член > 2 MiB читается несколькими чанками; пик памяти не достигает его размера."""
    path = _make_zip(tmp_path / "large.zip", {"big.bin": _LARGE})

    tracemalloc.start()
    try:
        total = 0
        chunks = 0
        biggest = 0
        for chunk in ZipContainer().open_member(str(path), "big.bin"):
            total += len(chunk)
            chunks += 1
            biggest = max(biggest, len(chunk))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert total == len(_LARGE)
    assert chunks > 1, "член обязан приходить чанками, а не целиком"
    assert biggest <= CHUNK_SIZE
    assert peak < len(_LARGE), "пик памяти не должен достигать размера члена"


def test_streaming_read_unknown_member_raises(tmp_path: Path) -> None:
    """Несуществующий член — ``KeyError``, а не пустой поток."""
    path = _make_zip(tmp_path / "one.zip", {"a.txt": b"x"})
    with pytest.raises(KeyError):
        list(ZipContainer().open_member(str(path), "missing.txt"))


# --------------------------------------------------------------------------- #
# Шифрование
# --------------------------------------------------------------------------- #


def test_encrypted_flag_is_visible(tmp_path: Path) -> None:
    """Флаг ``0x1`` в индексе → ``encrypted=True`` (ZipCrypto и AES alike)."""
    path = _make_zip(tmp_path / "secret.zip", {"a.pdf": _PAYLOAD})
    assert _set_encrypted_flag(path) == 1
    listing = ZipContainer().list_members(str(path))
    assert listing.encrypted is True
    assert listing.unreadable is None
    assert [member.path for member in listing.members] == ["a.pdf"]


def test_encrypted_member_read_fails_visibly(tmp_path: Path) -> None:
    """Чтение шифрованного члена — отказ, а не пустой документ."""
    path = _make_zip(tmp_path / "secret.zip", {"a.pdf": _PAYLOAD})
    _set_encrypted_flag(path)
    with pytest.raises(RuntimeError):
        list(ZipContainer().open_member(str(path), "a.pdf"))


# --------------------------------------------------------------------------- #
# Нечитаемый контейнер — причина, не исключение
# --------------------------------------------------------------------------- #


def test_corrupt_zip_is_unreadable_not_raised(tmp_path: Path) -> None:
    """Мусор с ZIP-магией: ``unreadable`` с текстом ошибки, состав пуст."""
    path = tmp_path / "broken.zip"
    path.write_bytes(b"PK\x03\x04" + b"not a zip at all" * 10)
    listing = ZipContainer().list_members(str(path))
    assert listing.format == "zip"
    assert listing.members == ()
    assert listing.unreadable
    assert listing.solid is False


def test_missing_file_is_unreadable(tmp_path: Path) -> None:
    """Отсутствующий файл — тоже ``unreadable`` (порт не бросает)."""
    listing = ZipContainer().list_members(str(tmp_path / "nope.zip"))
    assert listing.members == ()
    assert listing.unreadable


def test_zip_container_is_always_available() -> None:
    """stdlib-читатель доступен всегда; версия — провенанс, а не пустая строка."""
    container = ZipContainer()
    assert container.available() is True
    assert container.unavailable_reason() is None
    assert container.version.startswith("zipfile-")


# --------------------------------------------------------------------------- #
# Диспетчер: контейнер vs документ
# --------------------------------------------------------------------------- #


def test_container_for_detects_zip(tmp_path: Path) -> None:
    """Реальный zip → читатель ZIP."""
    path = _make_zip(tmp_path / "a.zip", {"a.pdf": _PAYLOAD})
    container = container_for(path)
    assert container is not None
    assert container.version.startswith("zipfile-")


def test_ooxml_zip_is_document_not_container(tmp_path: Path) -> None:
    """``docx``-подобный zip (маркер ``[Content_Types].xml``) — документ, не контейнер."""
    path = _make_zip(
        tmp_path / "report.docx",
        {
            "[Content_Types].xml": b"<Types/>",
            "word/document.xml": b"<document/>",
        },
    )
    head = path.read_bytes()[:8192]
    assert classify_archive_bytes(head) is None
    assert container_for(path) is None
    assert container_for_head(head) is None
    assert "OOXML" in unknown_reason(head)


def test_plain_zip_head_is_container() -> None:
    """Обычная ZIP-магия без OOXML-маркера — контейнер."""
    assert classify_archive_bytes(b"PK\x03\x04" + b"\x00" * 64) == "zip"
    assert "not_a_container" in unknown_reason(b"\x00" * 64)


def test_container_for_unreadable_path_is_none(tmp_path: Path) -> None:
    """Нечитаемый файл не притворяется контейнером (``None``, причина — у вызывающего)."""
    assert container_for(tmp_path / "missing.bin") is None


def test_unavailable_reason_of_available_reader_is_none(tmp_path: Path) -> None:
    """Доступный читатель не имеет причины недоступности (диспетчер не врёт)."""
    path = _make_zip(tmp_path / "a.zip", {"a.pdf": _PAYLOAD})
    container = container_for(path)
    assert container is not None
    assert unavailable_reason(container) is None


def test_unavailable_reason_of_foreign_reader_is_generic() -> None:
    """Читатель без метода причины: ``available()`` False → видимая общая причина."""

    class _Blind:
        version = "blind-0"

        def available(self) -> bool:
            return False

        def list_members(self, path: str):  # pragma: no cover — не вызывается
            raise AssertionError

        def open_member(self, path: str, member_path: str):  # pragma: no cover
            raise AssertionError

    reason = unavailable_reason(_Blind())  # type: ignore[arg-type]
    assert reason is not None
    assert reason.startswith("backend_unavailable")
