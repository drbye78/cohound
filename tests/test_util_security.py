"""Тесты ``core/util/security.py``: SSRF-валидация URL и санитизация имён.

Проверяется то, на чём держится scope краула (§10) и защита пути retention:
схема, литеральные приватные/metadata-адреса, allowlist домена, path traversal в именах
и стабильные slug-идентификаторы. Эталон семантики — ``tests/selfcheck.py::_util_security``.

Что проверка **не** обещает (S9g/P3): post-resolve сверки адреса нет, поэтому имя,
указывающее на приватный IP, эту валидацию проходит — закрывает её фетчер, не эта функция.
"""

from __future__ import annotations

import pytest

from core.util.security import sanitize_filename, sanitize_slug, validate_url


def test_validate_url_allows_public() -> None:
    """Публичный HTTP(S)-URL возвращается без изменений."""
    assert validate_url("https://example.com/a") == "https://example.com/a"


def test_validate_url_blocks_loopback_linklocal_and_bad_scheme() -> None:
    """Петлевой IP, link-local/metadata и не-HTTP(S) схема отвергаются."""
    for bad in (
        "http://127.0.0.1/x",
        "http://169.254.169.254/latest",
        "ftp://example.com/file",
    ):
        with pytest.raises(ValueError):
            validate_url(bad)


def test_validate_url_enforces_allowed_domain() -> None:
    """При ``allowed_domain`` чужой хост отвергается, свой (и его поддомен) проходит."""
    with pytest.raises(ValueError, match="не принадлежит"):
        validate_url("https://evil.com/docs", allowed_domain="example.com")
    assert (
        validate_url("https://example.com/docs", allowed_domain="example.com")
        == "https://example.com/docs"
    )
    assert (
        validate_url("https://docs.example.com/x", allowed_domain="example.com")
        == "https://docs.example.com/x"
    )


def test_sanitize_filename_strips_traversal() -> None:
    """Разделители и ведущие точки снимаются: имя не выходит за свой каталог."""
    sanitized = sanitize_filename("../../etc/passwd")

    assert ".." not in sanitized
    assert "/" not in sanitized
    assert sanitized == "etcpasswd"


def test_sanitize_slug_normalizes() -> None:
    """Всё вне ``[a-zA-Z0-9_-]`` → ``_``; повторы схлопываются, края обрезаются.

    Регистр **не** приводится к нижнему: ``sanitize_slug`` — санитайзер идентификатора,
    а не slugifier (это расхождение с распространённым ожиданием «lowercase через дефис»
    зафиксировано тестом явно, чтобы смена поведения была видимой).
    """
    assert sanitize_slug("Platform V Pangolin!") == "Platform_V_Pangolin"
    assert sanitize_slug("  Mixed  Case/with..dots  ") == "Mixed_Case_with_dots"
