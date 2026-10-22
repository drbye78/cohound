"""RAR-контейнер: ``core/adapters/container_rar.py`` (срез B2).

``rarfile`` в окружении **нет**, зато внешние утилиты (``unrar``/``7z``) есть — поэтому
проверяются **обе** половины условия доступности:

* нет пакета → ``available() is False`` и **точная** причина про пакет;
* пакет есть (фейк), утилиты нет → ``available() is False`` и **точная** причина про
  утилиту (``shutil.which`` подменяется на «ничего не найдено»);
* пакет и утилита есть → успешный путь: состав, ``solid=None`` (``rarfile`` флаг не
  отдаёт — не догадываемся), ``encrypted``, потоковое чтение.

Пакет не устанавливается: ``rarfile`` подменяется фейком.
"""

from __future__ import annotations

import shutil
from types import SimpleNamespace

import pytest

from core.adapters import container_rar
from core.adapters.container_rar import RarContainer
from core.ports.container import ContainerUnavailable

_REASON_PACKAGE = "backend_unavailable: rarfile не установлен (extra extract-archives)"
_REASON_TOOL = "backend_unavailable: нет внешней утилиты (bsdtar/unrar/7z)"


def _no_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    """Смоделировать «утилиты нет» (``PATH`` не содержит ни одной)."""
    monkeypatch.setattr(shutil, "which", lambda name: None)


def _tool(monkeypatch: pytest.MonkeyPatch, found: str = "/usr/bin/bsdtar") -> None:
    """Смоделировать найденную внешнюю утилиту."""

    def which(name: str) -> str | None:
        return found if name == "bsdtar" else None

    monkeypatch.setattr(shutil, "which", which)


def _install_fake(monkeypatch: pytest.MonkeyPatch, module: object) -> None:
    """Подменить бэкенд-модуль (пакет не устанавливаем)."""
    monkeypatch.setattr(container_rar, "backend_module", lambda: module)


# --------------------------------------------------------------------------- #
# Реальное состояние окружения: пакета нет
# --------------------------------------------------------------------------- #


def test_backend_absent_is_visible() -> None:
    """Без ``rarfile``: ``available() is False`` и версия-заглушка провенанса."""
    container = RarContainer()
    assert container.available() is False
    assert container.version == "rarfile-unavailable"
    assert container.unavailable_reason() == _REASON_PACKAGE


def test_backend_absent_listing_reason_is_exact() -> None:
    """Листинг объясняет отсутствие пакета, а не отдаёт пустой архив."""
    listing = RarContainer().list_members("/nonexistent/a.rar")
    assert listing.format == "rar"
    assert listing.members == ()
    assert listing.unreadable == _REASON_PACKAGE
    # rarfile солидность не сообщает — и без бэкенда тоже «не знаю».
    assert listing.solid is None
    assert listing.encrypted is False


def test_backend_absent_open_member_raises_typed() -> None:
    """Потока нет → ``ContainerUnavailable`` с той же причиной."""
    with pytest.raises(ContainerUnavailable) as excinfo:
        list(RarContainer().open_member("/nonexistent/a.rar", "a.pdf"))
    assert excinfo.value.reason == _REASON_PACKAGE


# --------------------------------------------------------------------------- #
# Пакет есть, утилиты нет
# --------------------------------------------------------------------------- #


def test_package_without_tool_is_not_available(monkeypatch: pytest.MonkeyPatch) -> None:
    """``rarfile`` без внешней утилиты: консервативная правда — недоступен."""
    _install_fake(monkeypatch, SimpleNamespace(__version__="9.9"))
    _no_tool(monkeypatch)
    container = RarContainer()
    assert container.available() is False
    assert container.unavailable_reason() == _REASON_TOOL
    assert container.version == "rarfile-9.9+no-tool"
    assert container.list_members("a.rar").unreadable == _REASON_TOOL


def test_tool_setup_predicate_is_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    """Утилита, настроенная в окружении (``rarfile.tool_setup``), считается найденной."""
    module = SimpleNamespace(__version__="9.9", tool_setup=lambda: (True, None))
    _install_fake(monkeypatch, module)
    _no_tool(monkeypatch)
    container = RarContainer()
    assert container.available() is True
    assert container.unavailable_reason() is None
    assert container.version == "rarfile-9.9+external"


# --------------------------------------------------------------------------- #
# Форма успешного пути (фейковый бэкенд + фейковая утилита)
# --------------------------------------------------------------------------- #


class _FakeInfo:
    """Минимальная форма ``rarfile.RarInfo``."""

    def __init__(
        self,
        filename: str,
        file_size: int,
        compress_size: int,
        crc: int | None,
        *,
        directory: bool = False,
        encrypted: bool = False,
    ) -> None:
        self.filename = filename
        self.file_size = file_size
        self.compress_size = compress_size
        self.CRC = crc
        self._directory = directory
        self._encrypted = encrypted

    def is_dir(self) -> bool:
        return self._directory

    def needs_password(self) -> bool:
        return self._encrypted


class _FakeRarFile:
    """Минимальная форма ``rarfile.RarFile``: infolist/open/needs_password."""

    entries: list[_FakeInfo] = []
    payloads: dict[str, bytes] = {}
    password: bool = False
    listing_error: Exception | None = None

    def __init__(self, path: str) -> None:
        self.path = path

    def __enter__(self) -> "_FakeRarFile":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def infolist(self) -> list[_FakeInfo]:
        if self.listing_error is not None:
            raise self.listing_error
        return list(self.entries)

    def needs_password(self) -> bool:
        return self.password

    def open(self, name: str):
        return _FakeStream(self.payloads[name])


class _FakeStream:
    """Файлоподобный объект члена (контекстный менеджер + ``read``)."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    def __enter__(self) -> "_FakeStream":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            chunk = self._data[self._pos :]
            self._pos = len(self._data)
            return chunk
        chunk = self._data[self._pos : self._pos + size]
        self._pos += len(chunk)
        return chunk


def _fake_module(**overrides: object) -> object:
    """Фейковый модуль ``rarfile``."""
    cls = type("FakeRarFile", (_FakeRarFile,), dict(overrides))
    return SimpleNamespace(RarFile=cls, __version__="9.9")


def test_listing_from_header(monkeypatch: pytest.MonkeyPatch) -> None:
    """Состав и заявленные размеры — из заголовков; ``solid`` — ``None``, не догадка."""
    _install_fake(
        monkeypatch,
        _fake_module(
            entries=[
                _FakeInfo("docs/a.pdf", 100, 40, 123),
                _FakeInfo("docs/", 0, 0, None, directory=True),
            ]
        ),
    )
    _tool(monkeypatch)
    container = RarContainer()
    assert container.available() is True
    assert container.version == "rarfile-9.9+bsdtar"
    listing = container.list_members("a.rar")
    assert listing.unreadable is None
    assert listing.solid is None
    assert listing.encrypted is False
    assert [
        (m.path, m.size, m.compressed_size, m.crc32, m.is_dir) for m in listing.members
    ] == [
        ("docs/a.pdf", 100, 40, 123, False),
        ("docs/", 0, 0, None, True),
    ]


def test_listing_accepts_mapping_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Запись индекса может быть ``dict``: каталог виден по хвостовому ``/``."""
    _install_fake(
        monkeypatch,
        _fake_module(
            entries=[
                {"filename": "docs/", "file_size": 0, "compress_size": 0, "CRC": None},
                {"filename": "a.pdf", "file_size": 7, "compress_size": 5, "CRC": 42},
            ]
        ),
    )
    _tool(monkeypatch)
    listing = RarContainer().list_members("a.rar")
    assert [(m.path, m.size, m.compressed_size, m.crc32, m.is_dir) for m in listing.members] == [
        ("docs/", 0, 0, None, True),
        ("a.pdf", 7, 5, 42, False),
    ]


def test_encrypted_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    """Пароль требуется → ``encrypted=True``."""
    _install_fake(monkeypatch, _fake_module(entries=[], password=True))
    _tool(monkeypatch)
    assert RarContainer().list_members("a.rar").encrypted is True


def test_encrypted_member_without_archive_predicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Если предиката архива нет, шифрование видно по члену."""
    module = _fake_module(
        entries=[_FakeInfo("a.pdf", 10, 10, 1, encrypted=True)],
        needs_password=None,
    )
    _install_fake(monkeypatch, module)
    _tool(monkeypatch)
    assert RarContainer().list_members("a.rar").encrypted is True


def test_listing_error_becomes_unreadable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Битый архив: причина в ``unreadable``, исключения нет."""
    _install_fake(monkeypatch, _fake_module(listing_error=ValueError("bad rar")))
    _tool(monkeypatch)
    listing = RarContainer().list_members("a.rar")
    assert listing.members == ()
    assert listing.unreadable == "ValueError: bad rar"


def test_open_member_streams_exact_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Чтение члена — поток чанков, байты ровно те."""
    payload = b"rar member payload" * 100_000  # ~1.7 MiB → несколько чанков
    _install_fake(monkeypatch, _fake_module(payloads={"docs/a.pdf": payload}))
    _tool(monkeypatch)
    chunks = list(RarContainer().open_member("a.rar", "docs/a.pdf"))
    assert b"".join(chunks) == payload
    assert len(chunks) > 1


def test_open_member_without_tool_is_typed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Пакет есть, утилиты нет → ``ContainerUnavailable`` с причиной про утилиту."""
    _install_fake(monkeypatch, _fake_module(payloads={"a.pdf": b"x"}))
    _no_tool(monkeypatch)
    with pytest.raises(ContainerUnavailable) as excinfo:
        list(RarContainer().open_member("a.rar", "a.pdf"))
    assert excinfo.value.reason == _REASON_TOOL
