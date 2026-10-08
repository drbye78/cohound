"""Тесты резолва ``source_id`` → URL и сверки ``content_hash`` (I10/I15/I19, W2/T2.4).

Используется минимальный fake-резолвер: структурная реализация ``SourceResolver``
без наследования (Protocol).
"""

from __future__ import annotations

from core.domain.anchors import (
    ResolvedSource,
    SourceResolver,
    resolve_source_url,
    verify_evidence_hash,
)
from core.domain.enums import EvidenceType, SourceKind
from core.domain.models import CellEvidence

_HASH_A = "a" * 64
_HASH_B = "b" * 64
_URL = "https://docs.example.com/page"


class FakeResolver:
    """Структурная реализация ``SourceResolver`` (без наследования)."""

    def __init__(self, mapping: dict[str, ResolvedSource]) -> None:
        self._mapping = mapping
        self.calls: list[str] = []

    def resolve(self, source_id: str) -> ResolvedSource | None:
        self.calls.append(source_id)
        return self._mapping.get(source_id)


def _evidence(
    source_id: str,
    *,
    content_hash: str | None,
    source_url: str | None = None,
    source_kind: SourceKind = SourceKind.DOCUMENTATION,
) -> CellEvidence:
    return CellEvidence(
        evidence_id="ev1",
        run_id="r",
        product_id="p",
        param_id="x",
        source_kind=source_kind,
        evidence_type=EvidenceType.DIRECT,
        source_id=source_id,
        source_url=source_url,
        content_hash=content_hash,
    )


def test_fake_resolver_is_structural() -> None:
    """Fake-класс удовлетворяет протоколу структурно (без наследования)."""
    resolver: SourceResolver = FakeResolver({})
    assert resolver.resolve("missing") is None


# --- resolve_source_url (I10/I15) -------------------------------------------


def test_resolve_happy_path() -> None:
    resolver = FakeResolver({"p:doc:0": ResolvedSource("doc#0", _URL, _HASH_A, "b1")})
    assert resolve_source_url("p:doc:0", resolver) == _URL
    assert resolver.calls == ["p:doc:0"]


def test_resolve_unknown_returns_none() -> None:
    assert resolve_source_url("p:doc:0", FakeResolver({})) is None


def test_resolve_expected_hash_match_returns_url() -> None:
    resolver = FakeResolver({"p:doc:0": ResolvedSource("doc#0", _URL, _HASH_A)})
    assert resolve_source_url("p:doc:0", resolver, expected_hash=_HASH_A) == _URL


# --- hash mismatch → None (I19) ---------------------------------------------


def test_resolve_hash_mismatch_returns_none() -> None:
    resolver = FakeResolver({"p:doc:0": ResolvedSource("doc#0", _URL, _HASH_A)})
    assert resolve_source_url("p:doc:0", resolver, expected_hash=_HASH_B) is None


def test_resolve_hash_absent_in_resolved_returns_none() -> None:
    resolver = FakeResolver({"p:doc:0": ResolvedSource("doc#0", _URL, None)})
    assert resolve_source_url("p:doc:0", resolver, expected_hash=_HASH_A) is None


# --- verify_evidence_hash (I19) ---------------------------------------------


def test_verify_true_when_hash_and_url_match() -> None:
    resolver = FakeResolver({"p:doc:0": ResolvedSource("doc#0", _URL, _HASH_A, "b1")})
    ev = _evidence("p:doc:0", content_hash=_HASH_A, source_url=_URL)
    assert verify_evidence_hash(ev, resolver) is True


def test_verify_true_when_urls_absent() -> None:
    resolver = FakeResolver({"p:doc:0": ResolvedSource("doc#0", None, _HASH_A)})
    ev = _evidence("p:doc:0", content_hash=_HASH_A)
    assert verify_evidence_hash(ev, resolver) is True


def test_verify_false_on_hash_mismatch() -> None:
    resolver = FakeResolver({"p:doc:0": ResolvedSource("doc#0", _URL, _HASH_A)})
    ev = _evidence("p:doc:0", content_hash=_HASH_B, source_url=_URL)
    assert verify_evidence_hash(ev, resolver) is False


def test_verify_false_when_evidence_hash_missing() -> None:
    # manual-evidence может не нести content_hash — но тогда I19 не подтверждён.
    resolver = FakeResolver({"p:doc:0": ResolvedSource("doc#0", _URL, _HASH_A)})
    ev = _evidence("p:doc:0", content_hash=None, source_kind=SourceKind.MANUAL)
    assert verify_evidence_hash(ev, resolver) is False


def test_verify_false_when_resolved_hash_missing() -> None:
    resolver = FakeResolver({"p:doc:0": ResolvedSource("doc#0", _URL, None)})
    ev = _evidence("p:doc:0", content_hash=_HASH_A, source_url=_URL)
    assert verify_evidence_hash(ev, resolver) is False


def test_verify_false_when_source_unresolved() -> None:
    ev = _evidence("p:doc:0", content_hash=_HASH_A, source_url=_URL)
    assert verify_evidence_hash(ev, FakeResolver({})) is False


def test_verify_false_when_url_mismatch() -> None:
    resolver = FakeResolver({"p:doc:0": ResolvedSource("doc#0", "https://other.example/x", _HASH_A)})
    ev = _evidence("p:doc:0", content_hash=_HASH_A, source_url=_URL)
    assert verify_evidence_hash(ev, resolver) is False
