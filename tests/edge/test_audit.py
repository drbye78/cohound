"""Тесты аудита шва → SIEM — ``core/edge/audit.py`` (W13/T13.9; 07 §8.4–§8.5).

Проверяется: одна JSON-строка на событие; корреляция по ``request_id``;
детерминированные часы; обязательные SIEM-поля; явная классификация контента
(метка оператора + хук переопределения); sink-и в файле и в памяти. Фикстуры
категорийно нейтральны (§1.3).
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path

import pytest

import core.edge.audit as audit_module
from core.edge.audit import (
    AUDIT_STATUS_ALLOW,
    AUDIT_STATUS_DENY,
    AUDIT_STATUS_ERROR,
    AUDIT_STATUSES,
    CLASSIFICATION_DEFAULT,
    CLASSIFICATION_INTERNAL,
    AuditEvent,
    AuditSink,
    Classification,
    FileAuditSink,
    MemoryAuditSink,
    audit_call,
    classify,
)
from core.edge.auth import Principal

_FIXED_TS = "2026-01-01T00:00:00+00:00"
_REQUIRED_FIELDS = {
    "ts",
    "request_id",
    "principal",
    "op",
    "status",
    "classification",
    "remote",
}


def _clock() -> str:
    """Детерминированные часы аудита."""
    return _FIXED_TS


def _read_lines(path: Path) -> list[dict[str, object]]:
    """Читает JSON-lines и возвращает разобранные события."""
    text = path.read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# FileAuditSink
# ---------------------------------------------------------------------------


def test_file_sink_writes_one_json_line_per_event(tmp_path: Path) -> None:
    path = tmp_path / "audit" / "edge.jsonl"
    sink = FileAuditSink(path, clock=_clock)
    audit_call(
        sink,
        request_id="req-1",
        principal=Principal(subject="svc-core"),
        op="fetch",
        status=AUDIT_STATUS_ALLOW,
    )
    audit_call(
        sink,
        request_id="req-2",
        principal=None,
        op="health",
        status=AUDIT_STATUS_ALLOW,
    )
    records = _read_lines(path)
    assert len(records) == 2
    assert [record["request_id"] for record in records] == ["req-1", "req-2"]


def test_file_sink_appends_across_instances(tmp_path: Path) -> None:
    path = tmp_path / "edge.jsonl"
    for index in range(3):
        FileAuditSink(path).emit(
            AuditEvent(
                ts=_FIXED_TS,
                request_id=f"req-{index}",
                principal=None,
                op="health",
                status=AUDIT_STATUS_ALLOW,
                classification=CLASSIFICATION_INTERNAL,
            )
        )
    assert len(_read_lines(path)) == 3


def test_file_sink_stamps_empty_ts_with_injected_clock(tmp_path: Path) -> None:
    path = tmp_path / "edge.jsonl"
    sink = FileAuditSink(path, clock=_clock)
    sink.emit(
        AuditEvent(
            ts="",
            request_id="req-x",
            principal=None,
            op="health",
            status=AUDIT_STATUS_ALLOW,
            classification=CLASSIFICATION_INTERNAL,
        )
    )
    assert _read_lines(path)[0]["ts"] == _FIXED_TS


def test_file_sink_output_is_utf8_not_escaped(tmp_path: Path) -> None:
    """Кириллица сохраняется как есть (``ensure_ascii=False``)."""
    path = tmp_path / "edge.jsonl"
    FileAuditSink(path).emit(
        AuditEvent(
            ts=_FIXED_TS,
            request_id="req-1",
            principal="служба",
            op="search",
            status=AUDIT_STATUS_ALLOW,
            classification=CLASSIFICATION_INTERNAL,
        )
    )
    raw = path.read_text(encoding="utf-8")
    assert "служба" in raw
    assert "\\u" not in raw


# ---------------------------------------------------------------------------
# MemoryAuditSink и удобный вызов
# ---------------------------------------------------------------------------


def test_memory_sink_collects_events() -> None:
    sink = MemoryAuditSink()
    event = audit_call(
        sink,
        request_id="req-1",
        principal="svc-core",
        op="search",
        status=AUDIT_STATUS_DENY,
        classification=CLASSIFICATION_INTERNAL,
        clock=_clock,
    )
    assert sink.events == [event]


def test_audit_call_uses_deterministic_clock() -> None:
    sink = MemoryAuditSink()
    event = audit_call(
        sink,
        request_id="req-1",
        principal=None,
        op="health",
        status=AUDIT_STATUS_ALLOW,
        clock=_clock,
    )
    assert event.ts == _FIXED_TS


def test_audit_call_requires_request_id() -> None:
    sink = MemoryAuditSink()
    with pytest.raises(ValueError):
        audit_call(
            sink,
            request_id="",
            principal=None,
            op="health",
            status=AUDIT_STATUS_ALLOW,
            clock=_clock,
        )


def test_audit_call_rejects_unknown_status() -> None:
    sink = MemoryAuditSink()
    with pytest.raises(ValueError):
        audit_call(
            sink,
            request_id="req-1",
            principal=None,
            op="health",
            status="maybe",
            clock=_clock,
        )


# ---------------------------------------------------------------------------
# Принципал / атрибуция
# ---------------------------------------------------------------------------


def test_audit_event_extracts_subject_from_principal_object() -> None:
    event = audit_call(
        MemoryAuditSink(),
        request_id="req-1",
        principal=Principal(subject="svc-core", method="mtls"),
        op="fetch",
        status=AUDIT_STATUS_ALLOW,
        clock=_clock,
    )
    assert event.principal == "svc-core"


def test_audit_event_accepts_plain_subject_string() -> None:
    event = audit_call(
        MemoryAuditSink(),
        request_id="req-1",
        principal="svc-raw",
        op="fetch",
        status=AUDIT_STATUS_ALLOW,
        clock=_clock,
    )
    assert event.principal == "svc-raw"


def test_audit_event_allows_anonymous() -> None:
    event = audit_call(
        MemoryAuditSink(),
        request_id="req-1",
        principal=None,
        op="health",
        status=AUDIT_STATUS_ALLOW,
        clock=_clock,
    )
    assert event.principal is None


# ---------------------------------------------------------------------------
# SIEM-форма и корреляция
# ---------------------------------------------------------------------------


def test_siem_shape_has_required_fields(tmp_path: Path) -> None:
    path = tmp_path / "edge.jsonl"
    sink = FileAuditSink(path, clock=_clock)
    audit_call(
        sink,
        request_id="req-correlate",
        principal=Principal(subject="svc-core"),
        op="registry",
        status=AUDIT_STATUS_ERROR,
        remote="10.0.0.7:51000",
    )
    record = _read_lines(path)[0]
    assert set(record) == _REQUIRED_FIELDS
    assert record["request_id"] == "req-correlate"
    assert record["op"] == "registry"
    assert record["status"] == AUDIT_STATUS_ERROR
    assert record["classification"] == CLASSIFICATION_DEFAULT
    assert record["remote"] == "10.0.0.7:51000"


def test_siem_events_correlate_by_request_id(tmp_path: Path) -> None:
    path = tmp_path / "edge.jsonl"
    sink = FileAuditSink(path, clock=_clock)
    for status in (AUDIT_STATUS_ALLOW, AUDIT_STATUS_DENY):
        audit_call(
            sink,
            request_id="req-42",
            principal="svc-core",
            op="fetch",
            status=status,
        )
    records = _read_lines(path)
    assert {record["request_id"] for record in records} == {"req-42"}
    assert {record["status"] for record in records} == {
        AUDIT_STATUS_ALLOW,
        AUDIT_STATUS_DENY,
    }


def test_to_json_is_deterministic_sorted() -> None:
    event = AuditEvent(
        ts=_FIXED_TS,
        request_id="req-1",
        principal="svc",
        op="fetch",
        status=AUDIT_STATUS_ALLOW,
        classification=CLASSIFICATION_INTERNAL,
        remote=None,
    )
    encoded = event.to_json()
    assert encoded == json.dumps(
        event.to_dict(), ensure_ascii=False, sort_keys=True
    )
    # Ключи отсортированы лексикографически.
    assert encoded.index('"classification"') < encoded.index('"op"')


def test_audit_statuses_exact() -> None:
    assert AUDIT_STATUSES == frozenset({"allow", "deny", "error"})


# ---------------------------------------------------------------------------
# Классификация (07 §8.5)
# ---------------------------------------------------------------------------


def test_classify_defaults_to_neutral_internal() -> None:
    assert classify("fetch", {"url": "https://x.test"}) == CLASSIFICATION_INTERNAL


def test_classify_uses_explicit_override_hook() -> None:
    def hook(op: str, payload: Mapping[str, object]) -> str:
        return "untrusted-public" if op == "fetch" else "internal"

    assert classify("fetch", {}, override=hook) == "untrusted-public"
    assert classify("search", {}, override=hook) == "internal"


def test_classification_object_applies_label_and_hook() -> None:
    classification = Classification(label="internal", override=lambda op, payload: "custom")
    assert str(classification) == "internal"
    assert classification.for_call("fetch", {}) == "custom"


def test_classification_rejects_empty_label() -> None:
    with pytest.raises(ValueError):
        Classification(label="   ")


def test_audit_call_accepts_classification_object() -> None:
    event = audit_call(
        MemoryAuditSink(),
        request_id="req-1",
        principal=None,
        op="fetch",
        status=AUDIT_STATUS_ALLOW,
        classification=Classification(label="untrusted-public"),
        clock=_clock,
    )
    assert event.classification == "untrusted-public"


# ---------------------------------------------------------------------------
# Протокол и нейтральность
# ---------------------------------------------------------------------------


def test_sinks_satisfy_protocol() -> None:
    assert isinstance(MemoryAuditSink(), AuditSink)
    assert isinstance(FileAuditSink("unused.jsonl"), AuditSink)


def test_audit_event_is_frozen() -> None:
    event = AuditEvent(
        ts=_FIXED_TS,
        request_id="req-1",
        principal=None,
        op="health",
        status=AUDIT_STATUS_ALLOW,
        classification=CLASSIFICATION_INTERNAL,
    )
    with pytest.raises(Exception):
        event.op = "other"  # type: ignore[misc]


_FORBIDDEN = re.compile(
    r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE
)


def test_source_is_neutral() -> None:
    assert audit_module.__file__ is not None
    source = Path(audit_module.__file__).read_text(encoding="utf-8")
    assert not _FORBIDDEN.search(source)
