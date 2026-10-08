"""Резолв ``source_id`` → URL и сверка ``content_hash`` (I10/I15/I19).

Инварианты I10/I15: URL для evidence не генерируется LLM, а **детерминированно**
резолвится системой из ``source_id`` (без сети). I19: резолв обязан сверить
``content_hash`` evidence с хэшем чанка — при расхождении ``source_id`` невалиден и
не принимается молча.

``SourceResolver`` — reader-протокол, **объявленный внутри ``domain/``** (не в
``ports/``), поэтому матрица зависимостей L0 ↛ L2 соблюдается (01 §2.5); реализация
приходит из ``store/`` (T3.4). Модуль — чистый Python без I/O, нейтрален (§1.3).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from core.domain.models import CellEvidence

__all__ = [
    "ResolvedSource",
    "SourceResolver",
    "resolve_source_url",
    "verify_evidence_hash",
]


@dataclass(frozen=True)
class ResolvedSource:
    """Результат резолва ``source_id``: чанк, его URL, хэш и (опц.) сборка."""

    chunk_id: str
    source_url: str | None
    content_hash: str | None
    build_id: str | None = None


class SourceResolver(Protocol):
    """Reader-протокол резолва ``source_id`` (структурная типизация).

    Реализуется индексом/хранилищем (``store/``, T3.4): lookup по ``source_id``
    без сети. Возвращает :class:`ResolvedSource` или ``None``, если ``source_id``
    неизвестен.
    """

    def resolve(self, source_id: str) -> ResolvedSource | None: ...


def resolve_source_url(
    source_id: str,
    resolver: SourceResolver,
    expected_hash: str | None = None,
) -> str | None:
    """Детерминированно резолвит ``source_id`` в URL (I10/I15), без сети.

    Если ``source_id`` не найден — ``None``. Если задан ``expected_hash`` (I19) и
    ``content_hash`` резолва ему не равен (в т.ч. отсутствует) — ``None``: расхождение
    инвалидирует ``source_id``, молчаливое принятие запрещено.
    """
    resolved = resolver.resolve(source_id)
    if resolved is None:
        return None
    if expected_hash is not None and resolved.content_hash != expected_hash:
        return None
    return resolved.source_url


def verify_evidence_hash(ev: CellEvidence, resolver: SourceResolver) -> bool:
    """Проверяет ``content_hash`` evidence против резолва (I19).

    Требуется, чтобы ``source_id`` резолвился, а ``content_hash`` evidence и чанка
    были непустыми **и равными**. Дополнительно: если URL задан и в evidence, и в
    резолве — они обязаны совпадать. Иначе — ``False``.
    """
    resolved = resolver.resolve(ev.source_id)
    if resolved is None:
        return False
    if not ev.content_hash or not resolved.content_hash:
        return False
    if ev.content_hash != resolved.content_hash:
        return False
    if ev.source_url and resolved.source_url and ev.source_url != resolved.source_url:
        return False
    return True
