"""Синхронизация доменных перечислений со словарём §4.1 (W1/T1.5, S3)."""

from __future__ import annotations

from pathlib import Path

from core.domain import (
    CapabilityPolicy,
    CrawlStrategy,
    Gate,
    RankingPolicy,
    SearchKind,
    SkipReason,
    StaleReason,
    Status,
    UnknownPolicy,
    assert_dictionary_integrity,
)

DICT = Path(__file__).resolve().parent.parent.parent / "docs" / "status-dictionary.yaml"


def test_dictionary_integrity() -> None:
    assert_dictionary_integrity(DICT)


def test_status_symbols_are_dictionary_keys() -> None:
    from core.domain import load_dictionary

    data = load_dictionary(DICT)
    symbols = {meta["symbol"] for meta in data["statuses"].values()}
    assert symbols == {s.value for s in Status}


def test_new_enums_match_dictionary() -> None:
    from core.domain import load_dictionary

    data = load_dictionary(DICT)
    assert set(data["ranking_policy"]) == {v.value for v in RankingPolicy}
    assert set(data["capability_policy"]) == {v.value for v in CapabilityPolicy}
    assert set(data["unknown_policy"]) == {v.value for v in UnknownPolicy}
    assert set(data["gate"]) == {v.value for v in Gate}
    assert set(data["search_kind"]) == {v.value for v in SearchKind}
    assert set(data["stale_reason"]) == {v.value for v in StaleReason}
    assert set(data["crawl_strategy"]) == {v.value for v in CrawlStrategy}
    assert set(data["skip_reason"]) == {v.value for v in SkipReason}
