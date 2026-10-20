"""Тесты композиции партиций (09 §8.1–§8.4): id, legacy-shim, иммутабельный манифест.

Проверяются детерминизм/порядко-независимость ``derive_composition_id``, семантика
``legacy_view`` (одна сборка = одна композиция), round-trip манифеста и запрет правки
на месте. Синтетика категорийно нейтральна (§1.3).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.pipeline.composition import (
    CompositionView,
    ImmutableCompositionError,
    composition_path,
    derive_composition_id,
    legacy_view,
    read_composition,
    write_composition,
)

PKG = "pkg-a"
CREATED = "2026-10-20T00:00:00Z"


def _view() -> CompositionView:
    return CompositionView(
        composition_id="cb_test",
        members={"p-alpha": "pb_alpha", "p-beta": "pb_beta"},
    )


# ---------------------------------------------------------------------------
# derive_composition_id
# ---------------------------------------------------------------------------


def test_derive_is_deterministic() -> None:
    members = {"p1": "pb1", "p2": "pb2"}
    digests = {"p1": "d1", "p2": "d2"}
    assert derive_composition_id(PKG, members, digests) == derive_composition_id(
        PKG, members, digests
    )


def test_derive_is_order_independent() -> None:
    forward = {"p1": "pb1", "p2": "pb2", "p3": "pb3"}
    backward = {"p3": "pb3", "p1": "pb1", "p2": "pb2"}
    assert derive_composition_id(PKG, forward) == derive_composition_id(PKG, backward)


def test_derive_sensitive_to_package_member_partition_and_digest() -> None:
    base = {"p1": "pb1", "p2": "pb2"}
    baseline = derive_composition_id(PKG, base, {"p1": "d1"})
    assert derive_composition_id("pkg-b", base, {"p1": "d1"}) != baseline
    assert derive_composition_id(PKG, {"p1": "pb1"}, {"p1": "d1"}) != baseline
    assert derive_composition_id(PKG, {"p1": "pbX", "p2": "pb2"}, {"p1": "d1"}) != baseline
    assert derive_composition_id(PKG, base, {"p1": "dX"}) != baseline


def test_derive_has_cb_prefix_and_is_short() -> None:
    value = derive_composition_id(PKG, {"p1": "pb1"})
    assert value.startswith("cb_")
    assert len(value) > len("cb_")


def test_derive_empty_composition_is_stable() -> None:
    assert derive_composition_id(PKG, {}) == derive_composition_id(PKG, {})


# ---------------------------------------------------------------------------
# CompositionView / legacy_view
# ---------------------------------------------------------------------------


def test_legacy_view_maps_every_product_to_one_build() -> None:
    view = legacy_view("ib_legacy", ["p2", "p1"])
    assert view.composition_id == "ib_legacy"
    assert view.build_id_for("p1") == "ib_legacy"
    assert view.build_id_for("p2") == "ib_legacy"
    assert view.product_ids() == ["p1", "p2"]  # отсортировано
    assert not view.is_empty


def test_legacy_view_empty() -> None:
    view = legacy_view("ib_legacy", [])
    assert view.is_empty
    assert view.product_ids() == []


def test_build_id_for_stranger_raises_keyerror() -> None:
    view = legacy_view("ib_legacy", ["p1"])
    with pytest.raises(KeyError, match="stranger"):
        view.build_id_for("stranger")


def test_view_members_are_immutable_snapshot() -> None:
    source = {"p1": "pb1"}
    view = CompositionView(composition_id="cb_x", members=source)
    source["p1"] = "changed"
    assert view.build_id_for("p1") == "pb1"


# ---------------------------------------------------------------------------
# write/read манифеста
# ---------------------------------------------------------------------------


def test_write_read_round_trip(tmp_path: Path) -> None:
    view = _view()
    path = write_composition(
        tmp_path, PKG, view, created_at=CREATED, digests={"p-alpha": "d-alpha"}
    )
    assert path == composition_path(tmp_path, PKG, "cb_test")
    stored = read_composition(tmp_path, PKG, "cb_test")
    assert stored == view


def test_missing_composition_is_none(tmp_path: Path) -> None:
    assert read_composition(tmp_path, PKG, "cb_absent") is None


def test_identical_rewrite_is_noop(tmp_path: Path) -> None:
    view = _view()
    path = write_composition(tmp_path, PKG, view, created_at=CREATED)
    before = path.read_bytes()
    before_mtime = path.stat().st_mtime_ns

    again = write_composition(
        tmp_path, PKG, view, created_at="2026-10-21T00:00:00Z"
    )

    assert again == path
    assert path.read_bytes() == before
    assert path.stat().st_mtime_ns == before_mtime


def test_changed_members_are_rejected(tmp_path: Path) -> None:
    write_composition(tmp_path, PKG, _view(), created_at=CREATED)
    changed = CompositionView(
        composition_id="cb_test",
        members={"p-alpha": "pb_other", "p-beta": "pb_beta"},
    )
    with pytest.raises(ImmutableCompositionError):
        write_composition(tmp_path, PKG, changed, created_at=CREATED)


def test_changed_digest_is_rejected(tmp_path: Path) -> None:
    view = _view()
    write_composition(tmp_path, PKG, view, created_at=CREATED, digests={"p-alpha": "d1"})
    with pytest.raises(ImmutableCompositionError):
        write_composition(
            tmp_path, PKG, view, created_at=CREATED, digests={"p-alpha": "d2"}
        )


def test_malformed_json_raises_value_error(tmp_path: Path) -> None:
    path = composition_path(tmp_path, PKG, "cb_bad")
    path.parent.mkdir(parents=True)
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(ValueError, match="не читается как JSON"):
        read_composition(tmp_path, PKG, "cb_bad")


def test_wrong_schema_raises_value_error(tmp_path: Path) -> None:
    path = composition_path(tmp_path, PKG, "cb_bad")
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps({"schema": "other/1", "composition_id": "cb_bad", "members": []}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="схема"):
        read_composition(tmp_path, PKG, "cb_bad")


def test_duplicate_product_raises_value_error(tmp_path: Path) -> None:
    path = composition_path(tmp_path, PKG, "cb_dup")
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "schema": "composition/1",
                "composition_id": "cb_dup",
                "package": PKG,
                "members": [
                    {"product_id": "p1", "partition_build_id": "pb1"},
                    {"product_id": "p1", "partition_build_id": "pb2"},
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="дубликат"):
        read_composition(tmp_path, PKG, "cb_dup")
