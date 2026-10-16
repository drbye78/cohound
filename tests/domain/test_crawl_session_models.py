"""Модели сеанса краула (Lane B): конструкция, ``extra="forbid"``, членство в перечислениях."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from core.domain import (
    CrawlItemStatus,
    CrawlSession,
    CrawlSessionItem,
    CrawlSessionStatus,
)


def _session(**overrides: object) -> CrawlSession:
    base: dict[str, object] = {
        "session_id": "s1",
        "run_id": "r1",
        "profile": "local",
        "status": CrawlSessionStatus.CREATED,
        "created_at": "2026-01-01T00:00:00Z",
    }
    base.update(overrides)
    return CrawlSession(**base)


def _item(**overrides: object) -> CrawlSessionItem:
    base: dict[str, object] = {
        "session_id": "s1",
        "product_id": "p1",
        "url": "https://docs.test/p1",
        "status": CrawlItemStatus.PENDING,
    }
    base.update(overrides)
    return CrawlSessionItem(**base)


def test_session_full_round_trip() -> None:
    session = _session(
        profile="split",
        status=CrawlSessionStatus.PARTIAL,
        started_at="t1",
        finished_at="t2",
        total=3,
        pending=0,
        running=0,
        fetched=2,
        failed=1,
        cancelled=0,
        error="1 url failed",
        options_json='{"from_scratch": true}',
        provenance_json='{"engine": "local"}',
    )
    assert session.status is CrawlSessionStatus.PARTIAL
    assert session.fetched == 2 and session.failed == 1
    assert session.options_json == '{"from_scratch": true}'


def test_session_run_id_optional_defaults_none() -> None:
    """C1: ``run_id`` — soft-link; ``None`` (standalone-сеанс) допустим."""
    base = {
        "session_id": "s1",
        "profile": "local",
        "status": CrawlSessionStatus.CREATED,
        "created_at": "2026-01-01T00:00:00Z",
    }
    assert CrawlSession(**base).run_id is None
    assert _session(run_id=None).run_id is None
    assert _session(run_id="r1").run_id == "r1"


def test_session_profile_is_free_string() -> None:
    """C5: ``profile`` не валидируется доменом (допустимый набор — composition root)."""
    # Домен L0 не импортирует cli (матрица слоёв): значение — свободная строка.
    assert _session(profile="split-hardened").profile == "split-hardened"
    with pytest.raises(ValidationError):
        _session(profile=123)


def test_item_full_round_trip() -> None:
    item = _item(
        status=CrawlItemStatus.FETCHED,
        attempts=2,
        engine="scrapy",
        kind="doc",
        fetch_mode="browser",
        extract_route="auto",
        strategy="hybrid",
        http_status=200,
        content_type="text/html",
        lang="en",
        content_hash="h1",
        raw_path="/tmp/h1.bin",
        first_seen="t0",
        last_attempt="t1",
    )
    assert item.status is CrawlItemStatus.FETCHED
    assert item.attempts == 2
    assert item.http_status == 200
    assert item.content_hash == "h1"
    assert item.raw_path == "/tmp/h1.bin"
