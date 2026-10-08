"""Хеширование контента и промптов (I19, §10).

``content_hash`` — SHA-256 в полном hex (64 символа); им фиксируется внешний контент
на пути ``corpus_raw → extract → chunk → content_hash → index`` (I19). ``prompt_hash``
детерминированно связывает ячейку с версией шаблона и входными чанками (§5): смена
шаблона → новый hash → инвалидация ячеек (§13).

Модуль категорийно-нейтрален (§1.3), зависимостей на ядро не имеет.
"""

from __future__ import annotations

import hashlib

# Разделитель полей — unit separator: не встречается в версиях/правилах/chunk_id
# и делает склейку однозначной.
_FIELD_SEP = "\x1f"

__all__ = ["content_hash", "prompt_hash", "short_hash"]


def content_hash(data: bytes | str) -> str:
    """SHA-256 контента в полном hex (64 символа).

    Строки кодируются UTF-8 — ``content_hash("ж")`` и ``content_hash("ж".encode())``
    совпадают. Детерминирован для одинаковых байтов.
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def short_hash(data: bytes | str, n: int = 24) -> str:
    """Первые ``n`` hex-символов SHA-256 (стиль ``derived_evidence_id``, S8).

    По умолчанию ``n=24``. ``n`` не может быть отрицательным (``ValueError``).
    Если ``n`` больше длины дайджеста, возвращается полный дайджест.
    """
    if n < 0:
        raise ValueError("n не может быть отрицательным")
    return content_hash(data)[:n]


def prompt_hash(
    template_version: str, rule: str, absence_rule: str, chunk_ids: list[str]
) -> str:
    """Детерминированный хэш промпта (§5): версия шаблона + правило + чанки.

    ``chunk_ids`` сортируются, поэтому порядок входа не влияет на результат
    (prompt_hash инвариантен к перестановке чанков). Склейка — фиксированным
    разделителем. SHA-256 в полном hex.
    """
    parts = [template_version, rule, absence_rule, *sorted(chunk_ids)]
    raw = _FIELD_SEP.join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
