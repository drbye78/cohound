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
from urllib.parse import urlparse

_PRIVATE_NETWORKS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
)

_METADATA_IP = ipaddress.ip_address("169.254.169.254")


def validate_url(url: str, allowed_domain: str | None = None) -> str:
    """Проверяет URL и блокирует SSRF-цели, возвращая его же при успехе.

    Отклоняет: не-HTTP(S) схемы; приватные/петлевые/link-local литеральные IP;
    облачный metadata-эндпоинт; хост вне ``allowed_domain`` (если задан).

    Важно: блокируются только **литеральные** IP; DNS-rebinding на этапе разбора
    не выявляется (требуется проверка адреса после резолва — на стороне фетчера).
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"недопустимая схема URL '{parsed.scheme}': только http/https")

    hostname = parsed.hostname
    if not hostname:
        raise ValueError("URL должен содержать корректный hostname")

    if allowed_domain is not None:
        if hostname != allowed_domain and not hostname.endswith(f".{allowed_domain}"):
            raise ValueError(
                f"hostname '{hostname}' не принадлежит разрешённому домену '{allowed_domain}'"
            )

    try:
        ip = ipaddress.ip_address(hostname)
    except ValueError:
        return url  # DNS-имя, не литеральный IP
    if ip == _METADATA_IP:
        raise ValueError("доступ к metadata-эндпоинту 169.254.169.254 заблокирован")
    for network in _PRIVATE_NETWORKS:
        if ip in network:
            raise ValueError(f"URL указывает на приватный/внутренний IP '{ip}' (SSRF)")
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
