"""Тесты ``core/util/security.py``: SSRF-валидация URL и санитизация имён.

Проверяется то, на чём держится scope краула (§10) и защита пути retention:
схема, литеральные non-global адреса (включая альтернативные нотации IPv4 и
IPv4-mapped IPv6), allowlist домена, post-resolve проверка через
``resolve_and_validate``, path traversal в именах и стабильные slug-идентификаторы.
Эталон семантики — ``tests/selfcheck.py::_util_security``.

Отказы различаются типами (оба — подклассы ``ValueError``): ``SsrfError`` — отказ
политики (постоянный), ``ResolutionError`` — сбой резолва (временный). Смешение
записало бы DNS-сбой как «небезопасный URL».

Что проверка **не** обещает (S9g/P3): ``validate_url`` не резолвит DNS и не пинует
адрес, поэтому между проверкой и соединением остаётся окно TOCTOU/DNS-rebinding.
``resolve_and_validate`` сужает его, но не устраняет.
"""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import Any

import pytest

from core.util.security import (
    ResolutionError,
    SsrfError,
    resolve_and_validate,
    sanitize_filename,
    sanitize_slug,
    validate_url,
)

# Литеральные адреса, которые обязана отвергать SSRF-политика. Список группирован:
# unspecified, loopback, link-local/metadata, private, CGNAT, unique-local, multicast,
# reserved/documentation и альтернативные нотации IPv4.
_BAD_LITERAL_URLS = [
    # unspecified
    "http://0.0.0.0/",
    "http://[::]/",
    # loopback, включая IPv4-mapped
    "http://127.0.0.1/",
    "http://[::1]/",
    "http://[::ffff:127.0.0.1]/",
    # link-local и облачный metadata-эндпоинт (в т.ч. mapped)
    "http://169.254.169.254/latest/meta-data/",
    "http://[fe80::1]/",
    "http://[::ffff:169.254.169.254]/",
    # private
    "http://10.1.2.3/",
    "http://172.16.0.1/",
    "http://192.168.0.1/",
    # CGNAT
    "http://100.64.0.1/",
    # unique-local
    "http://[fc00::1]/",
    # multicast: в Python 3.14 ``is_global`` у multicast равен True — нужна отдельная ветка
    "http://224.0.0.1/",
    "http://[ff02::1]/",
    # reserved / documentation
    "http://240.0.0.1/",
    "http://[2001:db8::1]/",
    # альтернативные нотации IPv4 (``ipaddress`` их не разбирает)
    "http://2130706433/",
    "http://0x7f000001/",
    "http://0177.0.0.1/",
    "http://127.1/",
    "http://0x7f.1/",
    "http://0/",
    # завершающая точка FQDN: getaddrinfo резолвит такой хост как литерал
    "http://127.0.0.1./",
    "http://2130706433./",
    # percent-encoding hostname
    "http://127%2e0%2e0%2e1/",
    # IPv6-формы со ВСТРОЕННЫМ IPv4: ``is_global`` у них True (ревью 09.11.2026).
    # NAT64 well-known: в IPv6-only сети с DNS64 транслируется в вложенный IPv4.
    "http://[64:ff9b::7f00:1]/",
    "http://[64:ff9b::a9fe:a9fe]/",
    # deprecated IPv4-compatible (RFC 4291 §2.5.5.1)
    "http://[::7f00:1]/",
    "http://[::c0a8:101]/",
    # IPv4-translated (RFC 2765): доступ к вложенному адресу даёт последние 32 бита
    "http://[::ffff:0:7f00:1]/",
]

_PUBLIC_URLS = [
    "http://93.184.216.34/",
    "https://example.com/",
    "https://example.com/a",
    # NAT64 с ПУБЛИЧНЫМ вложенным адресом — законная цель в NAT64-сети
    "http://[64:ff9b::808:808]/",
]


def _resolver(*addresses: str) -> Callable[[str], list[tuple[Any, ...]]]:
    """Резолвер-заглушка: ``getaddrinfo``-подобные записи для заданных адресов."""

    def resolve(hostname: str) -> list[tuple[Any, ...]]:
        return [
            (
                socket.AF_INET6 if ":" in address else socket.AF_INET,
                socket.SOCK_STREAM,
                6,
                "",
                (address, 0),
            )
            for address in addresses
        ]

    return resolve


@pytest.mark.parametrize("url", _PUBLIC_URLS)
def test_validate_url_allows_public(url: str) -> None:
    """Публичный HTTP(S)-URL возвращается без изменений."""
    assert validate_url(url) == url


def test_validate_url_blocks_loopback_linklocal_and_bad_scheme() -> None:
    """Петлевой IP, link-local/metadata и не-HTTP(S) схема отвергаются."""
    for bad in (
        "http://127.0.0.1/x",
        "http://169.254.169.254/latest",
        "ftp://example.com/file",
    ):
        with pytest.raises(SsrfError):
            validate_url(bad)


@pytest.mark.parametrize("url", _BAD_LITERAL_URLS)
def test_validate_url_blocks_non_global_literal_addresses(url: str) -> None:
    """Любой литеральный non-global адрес отвергается, в какой бы форме он ни был записан."""
    with pytest.raises(SsrfError):
        validate_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://169.254.169.254/latest/meta-data/",
        "http://[::ffff:169.254.169.254]/",
        # вложенный metadata через NAT64/IPv4-compatible: сообщение тоже обязано быть явным
        "http://[64:ff9b::a9fe:a9fe]/",
        "http://[::a9fe:a9fe]/",
    ],
)
def test_validate_url_metadata_has_explicit_message(url: str) -> None:
    """Metadata-эндпоинт назван явно, а не тонет в общем «приватный IP»."""
    with pytest.raises(SsrfError, match="metadata"):
        validate_url(url)


@pytest.mark.parametrize("url", ["http://[::1/", "http://[:::1]/", "http://[/"])
def test_validate_url_broken_url_is_typed_policy_refusal(url: str) -> None:
    """Битый IPv6-литерал — ``SsrfError``, а не голый ``ValueError`` из ``urlparse``.

    Иначе транспорт получает исключение вне таксономии краула, и постоянный мусорный URL
    классифицируется как временная недоступность источника.
    """
    with pytest.raises(SsrfError):
        validate_url(url)


def test_validate_url_does_not_resolve_names() -> None:
    """``validate_url`` чистая: имя не резолвится и потому проходит.

    Это не дыра в самой функции, а её контракт: имя закрывает
    :func:`resolve_and_validate` (post-resolve проверка, S9g).
    """
    assert validate_url("http://internal.example/") == "http://internal.example/"


def test_validate_url_allows_fqdn_with_trailing_dot() -> None:
    """Завершающая точка снимается только для распознавания литерала, не для DNS-имени."""
    assert validate_url("http://example.com./docs") == "http://example.com./docs"


def test_validate_url_enforces_allowed_domain() -> None:
    """При ``allowed_domain`` чужой хост отвергается, свой (и его поддомен) проходит."""
    with pytest.raises(SsrfError, match="не принадлежит"):
        validate_url("https://evil.com/docs", allowed_domain="example.com")
    assert (
        validate_url("https://example.com/docs", allowed_domain="example.com")
        == "https://example.com/docs"
    )
    assert (
        validate_url("https://docs.example.com/x", allowed_domain="example.com")
        == "https://docs.example.com/x"
    )


def test_resolve_and_validate_rejects_name_resolving_to_loopback() -> None:
    """Имя, резолвящееся в петлевой адрес, отвергается на post-resolve шаге."""
    with pytest.raises(SsrfError, match="SSRF"):
        resolve_and_validate("http://internal.example/", resolver=_resolver("127.0.0.1"))


def test_resolve_and_validate_rejects_localhost_by_name() -> None:
    """``localhost`` закрывается резолвом: имя → ``127.0.0.1`` (или ``::1``) — отказ."""
    with pytest.raises(SsrfError, match="SSRF"):
        resolve_and_validate("http://localhost/", resolver=_resolver("127.0.0.1", "::1"))


def test_resolve_and_validate_allows_public_resolution() -> None:
    """Имя, резолвящееся в публичный адрес, возвращается без изменений."""
    url = "https://docs.example/"
    assert resolve_and_validate(url, resolver=_resolver("93.184.216.34")) == url


def test_resolve_and_validate_allows_public_ipv6_resolution() -> None:
    """Публичный IPv6-адрес в ответе резолвера принимается."""
    url = "https://docs.example/"
    assert (
        resolve_and_validate(url, resolver=_resolver("2606:2800:220:1:248:1893:25c8:1946"))
        == url
    )


def test_resolve_and_validate_rejects_mixed_resolution() -> None:
    """Смешанный ответ (публичный + приватный) отвергается целиком."""
    with pytest.raises(SsrfError, match="SSRF"):
        resolve_and_validate(
            "http://split.example/", resolver=_resolver("93.184.216.34", "127.0.0.1")
        )


def test_resolve_and_validate_rejects_metadata_resolution() -> None:
    """Ответ резолвера на metadata-эндпоинт даёт явное сообщение."""
    with pytest.raises(SsrfError, match="metadata"):
        resolve_and_validate("http://meta.example/", resolver=_resolver("169.254.169.254"))


def test_resolve_and_validate_rejects_ipv4_mapped_resolution() -> None:
    """IPv4-mapped IPv6 в ответе резолвера разворачивается и отвергается."""
    with pytest.raises(SsrfError, match="SSRF"):
        resolve_and_validate(
            "http://mapped.example/", resolver=_resolver("::ffff:127.0.0.1")
        )


def test_resolve_and_validate_rejects_multicast_resolution() -> None:
    """Multicast в ответе резолвера отвергается отдельной веткой."""
    with pytest.raises(SsrfError, match="multicast"):
        resolve_and_validate("http://mcast.example/", resolver=_resolver("224.0.0.1"))


def test_resolve_and_validate_fails_closed_on_empty_resolution() -> None:
    """Пустой ответ резолвера — отказ, а не «нет адресов, значит можно»."""
    with pytest.raises(ResolutionError, match="не вернул адресов"):
        resolve_and_validate("http://empty.example/", resolver=_resolver())


def test_resolve_and_validate_fails_closed_on_resolver_error() -> None:
    """Сбой резолва — ``ValueError`` (отказ), не проброс сетевой ошибки наружу."""

    def broken(hostname: str) -> list[tuple[Any, ...]]:
        raise OSError("DNS недоступен")

    with pytest.raises(ResolutionError, match="не удалось разрешить"):
        resolve_and_validate("http://down.example/", resolver=broken)


def test_resolve_and_validate_keeps_pre_checks() -> None:
    """Проверки ``validate_url`` (схема, allowlist) наследуются без ослабления."""
    with pytest.raises(SsrfError, match="схема"):
        resolve_and_validate("ftp://example.com/", resolver=_resolver("93.184.216.34"))
    with pytest.raises(SsrfError, match="не принадлежит"):
        resolve_and_validate(
            "https://evil.com/",
            resolver=_resolver("93.184.216.34"),
            allowed_domain="example.com",
        )


def test_resolve_and_validate_rejects_literal_private_without_resolver() -> None:
    """Литеральный приватный адрес отвергается ещё до обращения к резолверу."""

    def must_not_run(hostname: str) -> list[tuple[Any, ...]]:
        raise AssertionError(f"резолвер не должен вызываться для литерала: {hostname}")

    with pytest.raises(SsrfError, match="SSRF"):
        resolve_and_validate("http://10.0.0.1/", resolver=must_not_run)


def test_refusal_classes_are_value_errors_but_disjoint() -> None:
    """Оба класса — ``ValueError``, но не подклассы друг друга.

    ``ValueError`` сохранён базой: существующие вызывающие (``except ValueError``) не
    ломаются. Различие при этом смысловое и обязано быть: отказ политики — постоянный,
    сбой резолва — временный; смешение записало бы DNS-сбой как «небезопасный URL».
    """
    assert issubclass(SsrfError, ValueError)
    assert issubclass(ResolutionError, ValueError)
    assert not issubclass(ResolutionError, SsrfError)
    assert not issubclass(SsrfError, ResolutionError)


def test_resolve_and_validate_malformed_entry_is_resolution_error() -> None:
    """Неразборчивая запись резолвера — ``ResolutionError`` (сбой прибора, не политика)."""

    def broken(hostname: str) -> list[tuple[Any, ...]]:
        return [("only-one-field",)]

    with pytest.raises(ResolutionError, match="некорректную запись"):
        resolve_and_validate("http://weird.example/", resolver=broken)


def test_resolve_and_validate_unparsable_address_is_resolution_error() -> None:
    """Адрес, который не разбирается ``ipaddress``, — тоже ``ResolutionError``."""

    def broken(hostname: str) -> list[tuple[Any, ...]]:
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("not-an-ip", 0))]

    with pytest.raises(ResolutionError, match="неразборчивый адрес"):
        resolve_and_validate("http://garbled.example/", resolver=broken)


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
