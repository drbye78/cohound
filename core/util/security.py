"""Безопасность сетевого сбора: SSRF-защита URL и санитизация имён файлов.

Используется краулером/фетчерами (Ш5, §3.2, §10): валидация URL перед запросом,
защита пути retention (`corpus_raw/{product_id}/`), санитизация идентификаторов.

Происхождение: адаптировано из проекта gag5 (`core/security.py`, MIT License,
Copyright (c) 2024). Изменения: удалена функция под
графовую БД (Cypher), добавлена типизация, сохранена нейтральность (§1.3).
"""

from __future__ import annotations

import ipaddress
import re
import socket
from collections.abc import Callable, Iterable
from typing import Any
from urllib.parse import unquote, urlparse

_IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
_AddrInfo = tuple[int, int, int, str, tuple[Any, ...]]
# Резолвер в форме ``socket.getaddrinfo``: имя → записи getaddrinfo-подобных кортежей.
_Resolver = Callable[[str], Iterable[_AddrInfo]]

_METADATA_IP = ipaddress.IPv4Address("169.254.169.254")

#: Префиксы IPv6, **встраивающие** IPv4-адрес: ``is_global`` у них ``True``, поэтому
#: одной проверки глобальности мало (ревью 09.11.2026: NAT64 ``64:ff9b::7f00:1``
#: транслируется в ``127.0.0.1`` в IPv6-only сети с DNS64).
#: * ``64:ff9b::/96`` — well-known NAT64 (RFC 6052);
#: * ``::/96`` — deprecated IPv4-compatible (RFC 4291 §2.5.5.1); ``::ffff:0:0/96``
#:   (IPv4-mapped) и ``::ffff:0:0:0/96`` (IPv4-translated, RFC 2765) обрабатываются
#:   отдельными ветками, потому что у них есть штатный доступ к вложенному адресу.
_NAT64_WELL_KNOWN = ipaddress.IPv6Network("64:ff9b::/96")
_IPV4_COMPATIBLE = ipaddress.IPv6Network("::/96")
#: IPv4-translated (RFC 2765 SIIT): последние 32 бита — IPv4, вложенного доступа из
#: stdlib нет (``ipv4_translated`` в 3.14 отсутствует), поэтому читаем байты сами.
_IPV4_TRANSLATED = ipaddress.IPv6Network("::ffff:0:0:0/96")


class SsrfError(ValueError):
    """URL **отклонён политикой** SSRF: небезопасная/недопустимая цель.

    Это постоянный отказ: повтор запроса к тому же URL смысла не имеет (литеральный
    приватный адрес, не-HTTP(S) схема, чужой домен при ``allowed_domain``). ``ValueError``
    остаётся базой намеренно — существующие вызывающие ловят его и продолжают работать.
    """


class ResolutionError(ValueError):
    """**Сбой резолва**: DNS не ответил, вернул пустой или неразборчивый ответ.

    Это временный сбой прибора, а не отказ политики: адрес не признан небезопасным,
    его просто не удалось установить (fail closed). Транспорт обязан различать эти два
    исхода и классифицировать сбой резолва как транзиентный, а не как «небезопасный URL».
    """


def _parse_literal_ip(hostname: str) -> _IPAddress | None:
    """Возвращает литеральный IP из hostname или ``None``, если это DNS-имя.

    ``ipaddress`` понимает только каноническую запись, поэтому hostname проверяется в
    нескольких нормализациях (разбор строки, без сети):

    * альтернативные нотации IPv4 (``2130706433``, ``0x7f000001``, ``0177.0.0.1``,
      ``127.1``) — через ``socket.inet_aton``, который читает их как одну 32-битную
      величину;
    * завершающая точка FQDN (``127.0.0.1.``) — ``getaddrinfo`` такой хост резолвит как
      литерал, поэтому она снимается перед разбором;
    * percent-encoding (``127%2e0%2e0%2e1``) — раскрывается ``unquote``: транспорты
      раскодируют hostname по-разному, и литерал не должен проходить из-за записи.
    """
    candidates = [hostname, unquote(hostname)]
    candidates += [candidate[:-1] for candidate in candidates if candidate.endswith(".")]
    for candidate in dict.fromkeys(candidates):
        try:
            return ipaddress.ip_address(candidate)
        except ValueError:
            pass
        try:
            return ipaddress.IPv4Address(socket.inet_aton(candidate))
        except (OSError, ValueError):
            pass
    return None


def _embedded_ipv4(ip: _IPAddress) -> ipaddress.IPv4Address | None:
    """IPv4-адрес, **встроенный** в IPv6-форму, либо ``None``.

    Зачем: ``is_global`` у этих форм ``True``, поэтому без разворота адрес проходит
    allow-list. Развёртываются четыре формы (ни один вложенный адрес не переживает
    «только глобальный»): IPv4-mapped ``::ffff:0:0/96``, IPv4-translated
    ``::ffff:0:0:0/96``, well-known NAT64 ``64:ff9b::/96`` и deprecated
    IPv4-compatible ``::/96``.
    """
    if not isinstance(ip, ipaddress.IPv6Address):
        return None
    if ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    if ip in _IPV4_TRANSLATED or ip in _NAT64_WELL_KNOWN or ip in _IPV4_COMPATIBLE:
        return ipaddress.IPv4Address(ip.packed[-4:])
    return None


def _reject_non_public(ip: _IPAddress, *, hostname: str) -> None:
    """Отклоняет адрес, не являющийся глобально маршрутизируемым (SSRF-политика).

    Политика разрешающая, а не запрещающая: пропускается только глобально
    маршрутизируемый адрес. Явно проверяются:

    * IPv6-формы со **встроенным** IPv4-адресом — ``is_global`` у них ``True``, поэтому
      одной проверки глобальности мало: IPv4-mapped (``::ffff:127.0.0.1``),
      IPv4-translated (``::ffff:0:7f00:1``), NAT64 ``64:ff9b::/96`` (в IPv6-only сети с
      DNS64 транслируется в вложенный IPv4) и deprecated IPv4-compatible ``::/96``.
      Вложенный адрес проверяется сам, а не только внешний IPv6;
    * metadata-эндпоинт ``169.254.169.254`` — отдельное сообщение (и во вложенной форме);
    * multicast — отдельная ветка: в Python 3.14 ``is_global`` у multicast равен
      ``True`` (``224.0.0.1``/``ff02::1``), поэтому одного ``is_global`` недостаточно.

    Остаточная граница названа, а не забыта: local-use NAT64 ``64:ff9b:1::/48``
    (RFC 8215) вложенный IPv4 не отдаёт без знания длины префикса площадки, а Teredo
    прячет адрес XOR-ом; для обеих форм ``is_global`` в Python 3.14 равен ``False``, то
    есть они отвергаются общим правилом. Пиннинга адреса нет (S9g).
    """
    inner = _embedded_ipv4(ip)
    if inner is not None:
        if inner == _METADATA_IP:
            raise SsrfError(
                "доступ к metadata-эндпоинту 169.254.169.254 заблокирован "
                f"(хост '{hostname}', IPv6-форма {ip})"
            )
        if inner.is_multicast or not inner.is_global:
            raise SsrfError(
                f"URL указывает на IPv6-адрес '{ip}' (хост '{hostname}') "
                f"со встроенным не-глобальным IPv4 '{inner}' (SSRF)"
            )
        return
    if ip == _METADATA_IP:
        raise SsrfError(
            f"доступ к metadata-эндпоинту 169.254.169.254 заблокирован (хост '{hostname}')"
        )
    if ip.is_multicast:
        raise SsrfError(f"URL указывает на multicast-адрес '{ip}' (хост '{hostname}', SSRF)")
    if not ip.is_global:
        raise SsrfError(
            f"URL указывает на не-глобальный/внутренний IP '{ip}' (хост '{hostname}', SSRF)"
        )


def validate_url(url: str, allowed_domain: str | None = None) -> str:
    """Проверяет URL и блокирует SSRF-цели, возвращая его же при успехе.

    Отклоняет: не-HTTP(S) схемы; любой литеральный IP, не являющийся глобально
    маршрутизируемым (unspecified ``0.0.0.0``/``::``, loopback, link-local ``fe80::/10``
    и ``169.254.0.0/16``, unique-local ``fc00::/7``, private, CGNAT ``100.64.0.0/10``,
    multicast, reserved); metadata-эндпоинт (отдельное сообщение); хост вне
    ``allowed_domain`` (если задан).

    Закрыто также и то, что ``ipaddress`` сам не разбирает: альтернативные нотации IPv4
    (``2130706433``, ``0x7f000001``, ``0177.0.0.1``, ``127.1``) нормализуются через
    ``socket.inet_aton``, IPv6-формы со встроенным IPv4 (``::ffff:127.0.0.1``,
    ``::ffff:169.254.169.254``, NAT64 ``64:ff9b::7f00:1``, IPv4-compatible ``::7f00:1``)
    разворачиваются и проверяются по вложенному IPv4, а
    завершающая точка FQDN (``127.0.0.1.``) снимается, а percent-encoding
    (``127%2e0%2e0%2e1``) раскрывается перед разбором — иначе литерал прошёл бы
    как «DNS-имя».

    Граница, названная честно: набор неканонических IPv4-форм определяет libc
    (``inet_aton``) и он платформозависим. В проде это подстраховано вторым слоем —
    :func:`resolve_and_validate` резолвит имя через тот же ``getaddrinfo``, который
    прочитает форму так же, как транспорт, и отклонит вложенный адрес.

    **Битый URL — отказ политики, не исключение парсера.** ``urlparse`` поднимает
    ``ValueError`` на некорректном IPv6-литерале (``http://[::1/``); он переводится в
    :class:`SsrfError`, иначе транспорт получил бы голый ``ValueError`` вне таксономии
    краула и записал бы постоянный мусорный URL как временную недоступность.

    Функция **чистая**: DNS не резолвится, сеть не используется. Поэтому DNS-имя,
    указывающее на приватный адрес (в том числе ``localhost``), здесь **не** выявляется —
    это делает :func:`resolve_and_validate` (post-resolve проверка каждого адреса).

    **Остаточный риск S9g (не закрыт): TOCTOU/DNS-rebinding.** Между проверкой адреса
    и фактическим соединением ответ DNS может измениться, а пиннинг адреса (соединение
    строго с проверенным IP) не реализуется. Post-resolve проверка сужает окно, но не
    устраняет его; полное закрытие требует транспорта с пиннингом, что дорого в
    сопровождении и меряется отдельно. Редиректы валидируются похопово (R10): уход на
    приватный/metadata-адрес через ``Location`` закрыт, через DNS-имя — только в связке
    с :func:`resolve_and_validate`.
    """
    try:
        parsed = urlparse(url)
    except ValueError as exc:
        # ``urlparse`` поднимает ``ValueError`` на битом IPv6-литерале: это отказ URL,
        # а не сбой парсера вызывающего — таксономия краула должна видеть ``SsrfError``.
        raise SsrfError(f"некорректный URL {url!r}: {exc}") from exc
    if parsed.scheme not in ("http", "https"):
        raise SsrfError(f"недопустимая схема URL '{parsed.scheme}': только http/https")

    hostname = parsed.hostname
    if not hostname:
        raise SsrfError("URL должен содержать корректный hostname")

    if (
        allowed_domain is not None
        and hostname != allowed_domain
        and not hostname.endswith(f".{allowed_domain}")
    ):
        raise SsrfError(
            f"hostname '{hostname}' не принадлежит разрешённому домену '{allowed_domain}'"
        )

    literal = _parse_literal_ip(hostname)
    if literal is not None:
        _reject_non_public(literal, hostname=hostname)
    return url


def _default_resolver(hostname: str) -> Iterable[_AddrInfo]:
    """Резолвит имя штатным ``socket.getaddrinfo`` (единственное место с DNS)."""
    return socket.getaddrinfo(hostname, None)


def resolve_and_validate(
    url: str,
    *,
    resolver: _Resolver | None = None,
    allowed_domain: str | None = None,
) -> str:
    """Проверяет URL и **дополнительно** резолвит хост, отвергая не-глобальные адреса.

    ``validate_url`` работает без сети и видит только литеральные IP: DNS-имя,
    указывающее на ``127.0.0.1`` (или ``localhost``), проходит его. Этот helper
    закрывает такой случай — резолвит ``hostname`` и требует, чтобы **каждый**
    полученный адрес был глобально маршрутизируемым. Смешанный ответ
    (публичный + приватный) отвергается целиком: одного «плохого» адреса достаточно.

    ``resolver`` инъектируемый: принимает ``hostname`` и возвращает итерируемое
    ``getaddrinfo``-подобных кортежей ``(family, type, proto, canonname, sockaddr)``,
    где ``sockaddr[0]`` — адрес. По умолчанию — ``socket.getaddrinfo`` (реальная сеть);
    тесты передают заглушку и в DNS не ходят.

    Возвращает исходный ``url`` при успехе. Типы отказов различаются, чтобы транспорт
    не выдавал временный сбой прибора за постоянный отказ URL:

    * :class:`SsrfError` — отказ SSRF-политики (небезопасная цель, в т.ч. на pre-check);
    * :class:`ResolutionError` — резолвер не ответил, вернул пустой или неразборчивый
      ответ (fail closed).

    Остаточный риск S9g: между этой проверкой и соединением адрес может измениться
    (TOCTOU/DNS-rebinding); пиннинг адреса не реализуется.
    """
    validate_url(url, allowed_domain=allowed_domain)
    hostname = urlparse(url).hostname
    if not hostname:  # недостижимо после validate_url; страховка для типизации
        raise SsrfError("URL должен содержать корректный hostname")

    resolve = resolver if resolver is not None else _default_resolver
    try:
        infos = list(resolve(hostname))
    except (OSError, UnicodeError) as exc:
        raise ResolutionError(f"не удалось разрешить хост '{hostname}': {exc}") from exc
    if not infos:
        raise ResolutionError(f"резолвер не вернул адресов для хоста '{hostname}'")

    for info in infos:
        try:
            raw = str(info[4][0])
        except (IndexError, TypeError) as exc:
            raise ResolutionError(
                f"резолвер вернул некорректную запись для хоста '{hostname}': {info!r}"
            ) from exc
        try:
            ip = ipaddress.ip_address(raw)
        except ValueError as exc:
            raise ResolutionError(
                f"резолвер вернул неразборчивый адрес '{raw}' для хоста '{hostname}'"
            ) from exc
        _reject_non_public(ip, hostname=hostname)
    return url


def sanitize_filename(name: str) -> str:
    """Санитизирует имя файла от path traversal (разделители, null-байты, ведущие точки)."""
    if not name:
        raise ValueError("имя файла не может быть пустым")
    sanitized = name.replace("\x00", "").replace("/", "").replace("\\", "").lstrip(".")
    if not sanitized:
        raise ValueError("имя файла пустое после санитизации")
    return sanitized


def sanitize_slug(name: str, max_length: int = 64) -> str:
    """Приводит строку к безопасному slug для идентификаторов (product_id, doc_id).

    Разрешены только [a-z0-9_-]; остальное → '_' (якоря данных §2.4 — стабильные slug).
    """
    safe = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    safe = re.sub(r"_+", "_", safe).strip("_")
    if len(safe) > max_length:
        safe = safe[:max_length].rstrip("_")
    return safe or "node"
