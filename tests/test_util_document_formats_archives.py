"""Класс «артефакт-контейнер»: ``core/util/document_formats.py`` (P6).

Проверяются: канонизация заявленного набора (``normalize_attachment_formats``),
детект по URL-суффиксу и ``Content-Type``, различение контейнера и OOXML-документа по
магии байтов (``classify_archive_bytes``) и допуск заявленного контейнера в
``unsupported_binary_reason``. Данные нейтральны (§1.3): никаких реальных архивов —
только магия и маркеры.
"""

from __future__ import annotations

import pytest

from core.util.document_formats import (
    BINARY_SNIFF_BYTES,
    SUPPORTED_ARCHIVE_FORMATS,
    classify_archive_bytes,
    is_attachment_content_type,
    is_attachment_url,
    normalize_attachment_formats,
    unsupported_binary_reason,
)

#: Магия ZIP (пустой и с членом) и OOXML-маркер.
_ZIP_LOCAL = b"PK\x03\x04"
_ZIP_EMPTY = b"PK\x05\x06"
_SEVEN_Z = b"7z\xbc\xaf\x27\x1c"
_RAR4 = b"Rar!\x1a\x07\x00"
_RAR5 = b"Rar!\x1a\x07\x01\x00"
_OOXML = b"[Content_Types].xml"


# ---------------------------------------------------------------------------
# normalize_attachment_formats
# ---------------------------------------------------------------------------


def test_normalize_defaults_to_all_supported() -> None:
    assert normalize_attachment_formats(None) == SUPPORTED_ARCHIVE_FORMATS
    assert normalize_attachment_formats() == SUPPORTED_ARCHIVE_FORMATS


def test_normalize_is_case_and_dot_insensitive_and_dedups() -> None:
    assert normalize_attachment_formats([".ZIP", "Zip", "7z"]) == ("zip", "7z")


def test_normalize_keeps_declared_order() -> None:
    assert normalize_attachment_formats(["rar", "zip"]) == ("rar", "zip")


def test_normalize_empty_set_means_no_containers() -> None:
    assert normalize_attachment_formats([]) == ()


def test_normalize_drops_unknown_by_rejecting() -> None:
    """Неизвестный формат не «теряется молча»: конфиг не обещает несуществующий читатель."""
    with pytest.raises(ValueError):
        normalize_attachment_formats(["zip", "gz"])
    with pytest.raises(ValueError):
        normalize_attachment_formats(["ova"])
    with pytest.raises(ValueError):
        normalize_attachment_formats(["."])


def test_normalize_rejects_empty_member() -> None:
    with pytest.raises(ValueError):
        normalize_attachment_formats(["zip", "  "])


# ---------------------------------------------------------------------------
# is_attachment_url / is_attachment_content_type
# ---------------------------------------------------------------------------


def test_attachment_url_matches_declared_suffix_only() -> None:
    assert is_attachment_url("https://x.test/files/release.zip")
    assert is_attachment_url("https://x.test/files/release.ZIP")
    assert is_attachment_url("https://x.test/a.7z?token=1")
    # Незаявленный контейнер — не артефакт (остаётся бинарным артефактом).
    assert not is_attachment_url("https://x.test/disk.ova")
    assert not is_attachment_url("https://x.test/disk.iso")
    assert not is_attachment_url("https://x.test/page.html")


def test_attachment_url_respects_declared_subset() -> None:
    assert is_attachment_url("https://x.test/a.zip", ["zip"])
    assert not is_attachment_url("https://x.test/a.7z", ["zip"])
    assert not is_attachment_url("https://x.test/a.zip", [])


def test_attachment_content_type_matches_declared_subset() -> None:
    assert is_attachment_content_type("application/zip", ["zip"])
    assert is_attachment_content_type("application/zip; charset=binary")
    assert is_attachment_content_type("application/x-7z-compressed")
    assert is_attachment_content_type("application/vnd.rar")
    # Общий бинарный тип не доказывает контейнер (класс решает магия байтов).
    assert not is_attachment_content_type("application/octet-stream")
    assert not is_attachment_content_type("text/html")
    assert not is_attachment_content_type(None)
    assert not is_attachment_content_type("application/zip", ["7z"])


# ---------------------------------------------------------------------------
# classify_archive_bytes
# ---------------------------------------------------------------------------


def test_classify_zip_container() -> None:
    assert classify_archive_bytes(_ZIP_LOCAL + b"\x14\x00\x00\x00rest") == "zip"
    assert classify_archive_bytes(_ZIP_EMPTY + b"\x00" * 18) == "zip"


def test_classify_zip_with_ooxml_marker_is_document() -> None:
    """OOXML — тоже ZIP, но это документ: маркер ``[Content_Types].xml`` в голове."""
    body = _ZIP_LOCAL + b"\x14\x00\x00\x00" + _OOXML + b"..." + b"\x00" * 64
    assert classify_archive_bytes(body) is None


def test_classify_zip_with_docx_member_beyond_head_is_container() -> None:
    """Маркер вне головы (docx-член внутри контейнера) не делает контейнер документом."""
    body = _ZIP_LOCAL + b"\x00" * BINARY_SNIFF_BYTES + _OOXML
    assert classify_archive_bytes(body) == "zip"


def test_classify_7z_and_rar_magic() -> None:
    assert classify_archive_bytes(_SEVEN_Z + b"\x00" * 16) == "7z"
    assert classify_archive_bytes(_RAR4 + b"\x00" * 16) == "rar"
    assert classify_archive_bytes(_RAR5 + b"\x00" * 16) == "rar"


def test_classify_non_container_is_none() -> None:
    assert classify_archive_bytes(b"%PDF-1.7\n") is None
    assert classify_archive_bytes(b"<html>page</html>") is None
    assert classify_archive_bytes(b"\x1f\x8b\x08gzip") is None  # gzip — не контейнер допуска
    assert classify_archive_bytes(b"") is None


# ---------------------------------------------------------------------------
# unsupported_binary_reason: допуск заявленного контейнера
# ---------------------------------------------------------------------------


def test_declared_container_is_admitted_but_undeclared_is_binary() -> None:
    zip_body = _ZIP_LOCAL + b"\x00" * 64
    assert unsupported_binary_reason(zip_body, attachment_formats=["zip"]) is None
    assert unsupported_binary_reason(zip_body) is None  # ZIP-магия допускалась и раньше
    seven = _SEVEN_Z + b"\x00" * 64
    assert unsupported_binary_reason(seven, attachment_formats=["7z"]) is None
    # Набор не заявлен вовсе — контейнер остаётся обычным бинарным артефактом (NUL).
    assert unsupported_binary_reason(seven, attachment_formats=[]) is not None
    # Заявлен только zip: 7z не допущен и отсекается как бинарный.
    assert unsupported_binary_reason(seven, attachment_formats=["zip"]) is not None


def test_declared_container_still_rejects_garbage_bytes() -> None:
    """Класс по суффиксу не отменяет проверку байтов: NUL-мусор остаётся бинарным."""
    assert unsupported_binary_reason(
        b"\x00garbage", url="https://x.test/a.zip", attachment_formats=["zip"]
    ) is not None
