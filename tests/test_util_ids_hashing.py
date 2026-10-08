"""Тесты ``core/util/ids.py`` и ``core/util/hashing.py`` (W2/T2.4).

Покрывают round-trip ``source_id`` ↔ ``SourceRef``, малформед-вход, slug-валидацию,
детерминизм ``content_hash`` и порядко-независимость ``prompt_hash``.
"""

from __future__ import annotations

import hashlib

import pytest

from core.util.hashing import content_hash, prompt_hash, short_hash
from core.util.ids import SourceRef, parse_source_id, source_id, validate_slug

# --- source_id ---------------------------------------------------------------


def test_source_id_format_without_build() -> None:
    assert source_id("prod", "doc", 0) == "prod:doc:0"
    assert source_id("prod", "doc", 123) == "prod:doc:123"


def test_source_id_format_with_build() -> None:
    assert source_id("prod", "doc", 7, "build_1") == "prod:doc:7:build_1"


def test_source_id_empty_build_is_not_appended() -> None:
    assert source_id("prod", "doc", 7, "") == "prod:doc:7"


# --- parse_source_id round-trip ---------------------------------------------


def test_parse_round_trip_without_build() -> None:
    ref = parse_source_id(source_id("prod", "doc", 3))
    assert ref == SourceRef("prod", "doc", 3, None)


def test_parse_round_trip_with_build() -> None:
    ref = parse_source_id(source_id("prod", "doc", 3, "b_9"))
    assert ref == SourceRef("prod", "doc", 3, "b_9")
    assert ref.build_id == "b_9"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "a:b",
        "a:b:c:d:e",
        "a:b:x",
        "a:b:-1",
        "a::0",
        ":b:0",
        "a:b:0:",
        "a:b: 0",
    ],
)
def test_parse_malformed_raises(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_source_id(bad)


# --- validate_slug -----------------------------------------------------------


@pytest.mark.parametrize("good", ["prod", "prod_1", "a", "0", "p_0_x"])
def test_validate_slug_accepts(good: str) -> None:
    assert validate_slug(good) == good


@pytest.mark.parametrize("bad", ["", "Prod", "prod-1", "prod 1", "прод", "prod.x", "PROD"])
def test_validate_slug_rejects(bad: str) -> None:
    with pytest.raises(ValueError):
        validate_slug(bad)


# --- content_hash ------------------------------------------------------------


def test_content_hash_deterministic_and_full_hex() -> None:
    h1 = content_hash(b"hello world")
    h2 = content_hash(b"hello world")
    assert h1 == h2
    assert len(h1) == 64
    assert h1 == hashlib.sha256(b"hello world").hexdigest()


def test_content_hash_str_and_bytes_agree_utf8() -> None:
    assert content_hash("ёж") == content_hash("ёж".encode("utf-8"))


def test_content_hash_known_vector() -> None:
    assert content_hash(b"abc") == (
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


# --- prompt_hash -------------------------------------------------------------


def test_prompt_hash_order_independent() -> None:
    a = prompt_hash("v1", "rule", "explicit", ["c5", "c1", "c3"])
    b = prompt_hash("v1", "rule", "explicit", ["c3", "c5", "c1"])
    c = prompt_hash("v1", "rule", "explicit", ["c1", "c3", "c5"])
    assert a == b == c


def test_prompt_hash_deterministic_across_calls() -> None:
    assert prompt_hash("v1", "rule", "explicit", ["c1", "c2"]) == prompt_hash(
        "v1", "rule", "explicit", ["c2", "c1"]
    )


def test_prompt_hash_sensitive_to_fields() -> None:
    base = prompt_hash("v1", "rule", "explicit", ["c1"])
    assert prompt_hash("v2", "rule", "explicit", ["c1"]) != base
    assert prompt_hash("v1", "other", "explicit", ["c1"]) != base
    assert prompt_hash("v1", "rule", "parameter_reference", ["c1"]) != base
    assert prompt_hash("v1", "rule", "explicit", ["c2"]) != base


def test_prompt_hash_full_hex() -> None:
    assert len(prompt_hash("v1", "rule", "explicit", [])) == 64


# --- short_hash --------------------------------------------------------------


def test_short_hash_prefix_and_default_length() -> None:
    digest = content_hash(b"abc")
    assert short_hash(b"abc") == digest[:24]
    assert len(short_hash(b"abc")) == 24


def test_short_hash_custom_n() -> None:
    assert short_hash(b"abc", 8) == content_hash(b"abc")[:8]


def test_short_hash_n_zero_empty() -> None:
    assert short_hash(b"abc", 0) == ""


def test_short_hash_n_over_digest_returns_full() -> None:
    assert short_hash(b"abc", 100) == content_hash(b"abc")


def test_short_hash_negative_raises() -> None:
    with pytest.raises(ValueError):
        short_hash(b"abc", -1)
