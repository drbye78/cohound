"""Тесты конструктора ``CellEvidence`` (§2.3: I10/I15, I19, R1).

Отличие от ``tests/domain/test_invariants.py``: там проверяется **функция**
``check_evidence`` на объекте, собранном через ``model_construct`` (в обход валидатора).
Здесь проверяется сам ``model_validator(mode="after")`` — то есть отказ происходит уже
на конструировании, и нарушающий объект нельзя создать обычным путём.

Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Any

import pytest

from core.domain.enums import EvidenceType, SourceKind
from core.domain.models import CellEvidence

_RUN = "run-1"
_PRODUCT = "prod-1"
_PARAM = "param-1"


def _evidence(**kw: Any) -> CellEvidence:
    """Валидный по умолчанию evidence; ``kw`` переопределяет отдельные поля."""
    base: dict[str, Any] = {
        "evidence_id": "ev",
        "run_id": _RUN,
        "product_id": _PRODUCT,
        "param_id": _PARAM,
        "source_kind": SourceKind.DOCUMENTATION,
        "evidence_type": EvidenceType.DIRECT,
        "source_id": "chunk_1",
        "content_hash": "h1",
    }
    base.update(kw)
    return CellEvidence(**base)


def test_corpus_evidence_without_content_hash_is_rejected() -> None:
    """Корпусный evidence без hash не конструируется (I19)."""
    with pytest.raises(ValueError, match="I19"):
        CellEvidence(
            evidence_id="ev",
            run_id=_RUN,
            product_id=_PRODUCT,
            param_id=_PARAM,
            source_kind=SourceKind.DOCUMENTATION,
            evidence_type=EvidenceType.DIRECT,
            source_id="c1",
        )
    # Контроль: тот же объект с hash — законен.
    assert _evidence(content_hash="h1").content_hash == "h1"


def test_registry_evidence_requires_source_url() -> None:
    """Registry evidence без резолва не конструируется (R1)."""
    with pytest.raises(ValueError, match="R1"):
        _evidence(source_kind=SourceKind.REGISTRY, content_hash=None)
    # Контроль: с source_url — законен.
    ev = _evidence(source_kind=SourceKind.REGISTRY, source_url="https://r.example/x")
    assert ev.source_url == "https://r.example/x"


def test_evidence_without_source_id_is_rejected() -> None:
    """Evidence без ``source_id`` не конструируется (I10/I15)."""
    with pytest.raises(ValueError, match="I10/I15"):
        _evidence(source_id="")
