"""7z-контейнер: ``core/adapters/container_7z.py`` (срез B2).

``py7zr`` в окружении **нет** (extra ``extract-archives``, LGPL-исключение) — и это
проверяется как штатное состояние: ``available() is False``, листинг несёт **точную**
видимую причину, ``open_member`` отказывает типизированно. Форма успешного пути
проверяется на фейковом бэкенде (пакет не устанавливается): состав, ``solid`` из индекса
(а не догадка), ``encrypted``, потоковое чтение через временный файл.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from core.adapters import container_7z
from core.adapters.container_7z import SevenZipContainer
from core.ports.container import ContainerUnavailable

_ABSENT = "backend_unavailable: py7zr не установлен (extra extract-archives)"


def _install_fake(monkeypatch: pytest.MonkeyPatch, module: object) -> None:
    """Подменить бэкенд-модуль (пакет не устанавливаем)."""
    monkeypatch.setattr(container_7z, "backend_module", lambda: module)


# --------------------------------------------------------------------------- #
# Реальное состояние окружения: бэкенда нет
# --------------------------------------------------------------------------- #


def test_backend_absent_is_visible() -> None:
    """Без ``py7zr``: ``available() is False`` и версия-заглушка провенанса."""
    container = SevenZipContainer()
    assert container.available() is False
    assert container.version == "py7zr-unavailable"
    assert container.unavailable_reason() == _ABSENT


def test_backend_absent_listing_reason_is_exact() -> None:
    """Листинг объясняет отсутствие бэкенда, а не отдаёт пустой архив."""
    listing = SevenZipContainer().list_members("/nonexistent/a.7z")
    assert listing.format == "7z"
    assert listing.members == ()
    assert listing.unreadable == _ABSENT
    assert listing.solid is None
    assert listing.encrypted is False
    assert listing.truncated is False


def test_backend_absent_open_member_raises_typed() -> None:
    """Потока нет по определению → ``ContainerUnavailable`` с той же причиной."""
    with pytest.raises(ContainerUnavailable) as excinfo:
        list(SevenZipContainer().open_member("/nonexistent/a.7z", "a.pdf"))
    assert excinfo.value.reason == _ABSENT


# --------------------------------------------------------------------------- #
# Форма успешного пути (фейковый бэкенд)
# --------------------------------------------------------------------------- #


class _FakeSevenZipFile:
    """Минимальная форма ``py7zr.SevenZipFile``: list/archiveinfo/extract."""

    entries: list[dict] = []
    info: object = SimpleNamespace(solid=True, method_names=["LZMA2"])
    password: bool = False
    payloads: dict[str, bytes] = {}
    listing_error: Exception | None = None

    def __init__(self, path: str, mode: str = "r") -> None:
        self.path = path

    def __enter__(self) -> "_FakeSevenZipFile":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def list(self) -> list[dict]:
        if self.listing_error is not None:
            raise self.listing_error
        return list(self.entries)

    def archiveinfo(self) -> object:
        return self.info

    def needs_password(self) -> bool:
        return self.password

    def extract(self, path: str | None = None, targets: list[str] | None = None) -> None:
        assert path is not None
        for target in targets or ():
            data = self.payloads.get(target)
            if data is None or ".." in Path(target).parts:
                continue  # выходящий путь фейк не пишет: его отвергает сам адаптер
            destination = Path(path) / target
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(data)


def _fake_module(**overrides: object) -> object:
    """Фейковый модуль ``py7zr`` (класс настраивается через атрибуты)."""
    cls = type("FakeSevenZipFile", (_FakeSevenZipFile,), dict(overrides))
    return SimpleNamespace(SevenZipFile=cls, __version__="9.9.9")


def test_listing_from_header(monkeypatch: pytest.MonkeyPatch) -> None:
    """Состав и заявленные размеры — из индекса; ``solid`` — из флага архива."""
    _install_fake(
        monkeypatch,
        _fake_module(
            entries=[
                {
                    "filename": "docs/a.pdf",
                    "uncompressed": 100,
                    "compressed": 40,
                    "crc32": 123,
                    "is_directory": False,
                },
                {
                    "filename": "docs/",
                    "uncompressed": 0,
                    "compressed": 0,
                    "crc32": None,
                    "is_directory": True,
                },
            ]
        ),
    )
    container = SevenZipContainer()
    assert container.available() is True
    assert container.version == "py7zr-9.9.9"
    listing = container.list_members("a.7z")
    assert listing.unreadable is None
    assert listing.solid is True
    assert listing.encrypted is False
    assert [
        (m.path, m.size, m.compressed_size, m.crc32, m.is_dir) for m in listing.members
    ] == [
        ("docs/a.pdf", 100, 40, 123, False),
        ("docs/", 0, 0, None, True),
    ]


def test_solid_is_not_guessed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Бэкенд не сообщает солидность → ``None``, а не догадка."""
    _install_fake(
        monkeypatch,
        _fake_module(entries=[{"filename": "a.pdf", "uncompressed": 1}], info=object()),
    )
    assert SevenZipContainer().list_members("a.7z").solid is None


def test_archiveinfo_absent_is_not_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Бэкенд без ``archiveinfo`` листается, солидность — ``None``."""
    _install_fake(
        monkeypatch,
        _fake_module(
            entries=[{"filename": "a.pdf", "uncompressed": 1}],
            archiveinfo=None,
        ),
    )
    listing = SevenZipContainer().list_members("a.7z")
    assert listing.unreadable is None
    assert listing.solid is None
    assert [m.path for m in listing.members] == ["a.pdf"]


def test_solid_non_bool_is_not_coerced(monkeypatch: pytest.MonkeyPatch) -> None:
    """Не-``bool`` в поле солидности не превращается в «правду»."""
    _install_fake(
        monkeypatch,
        _fake_module(entries=[], info=SimpleNamespace(solid="yes")),
    )
    assert SevenZipContainer().list_members("a.7z").solid is None


def test_encrypted_from_backend_predicate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Пароль требуется → ``encrypted=True``."""
    _install_fake(monkeypatch, _fake_module(entries=[], password=True))
    assert SevenZipContainer().list_members("a.7z").encrypted is True


def test_encrypted_from_cipher_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """Без предиката шифр ``7zAES`` в методах всё равно виден."""
    module = _fake_module(
        entries=[],
        needs_password=None,  # бэкенд предиката не отдаёт
        info=SimpleNamespace(solid=False, method_names=["7zAES", "LZMA2"]),
    )
    _install_fake(monkeypatch, module)
    assert SevenZipContainer().list_members("a.7z").encrypted is True


def test_listing_error_becomes_unreadable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Битый архив: причина в ``unreadable``, исключения нет."""
    _install_fake(
        monkeypatch,
        _fake_module(listing_error=ValueError("bad header")),
    )
    listing = SevenZipContainer().list_members("a.7z")
    assert listing.members == ()
    assert listing.unreadable == "ValueError: bad header"


def test_open_member_streams_exact_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Чтение члена отдаёт ровно его байты (через временный файл, чанками)."""
    payload = b"7z member payload" * 100_000  # ~1.7 MiB → несколько чанков
    _install_fake(monkeypatch, _fake_module(payloads={"docs/a.pdf": payload}))
    chunks = list(SevenZipContainer().open_member("a.7z", "docs/a.pdf"))
    assert b"".join(chunks) == payload
    assert len(chunks) > 1


def test_open_member_not_extracted_is_typed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Бэкенд «не извлёк» член — видимый типизированный отказ, не пустой поток."""
    _install_fake(monkeypatch, _fake_module(payloads={}))
    with pytest.raises(ContainerUnavailable) as excinfo:
        list(SevenZipContainer().open_member("a.7z", "docs/a.pdf"))
    assert excinfo.value.reason == "member_not_extracted: docs/a.pdf"


def test_open_member_rejects_traversal_target(monkeypatch: pytest.MonkeyPatch) -> None:
    """Путь члена, выходящий из временного каталога, отвергается до чтения."""
    _install_fake(monkeypatch, _fake_module(payloads={"../escape.pdf": b"x"}))
    with pytest.raises(ContainerUnavailable) as excinfo:
        list(SevenZipContainer().open_member("a.7z", "../escape.pdf"))
    assert excinfo.value.reason.startswith("member_path_rejected")
