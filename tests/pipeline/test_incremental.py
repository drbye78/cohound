"""Тесты планировщика инкрементального корпуса (09 §6.3/§9/§10.1).

Офлайн: поддельный ``ProductStatePort`` (только ``iter_url_state``), без сети и без БД.
Проверяются матрица политик, дедупликация discovery, неприкосновенность корней,
детерминированный порядок, классификация переиспользования и стабильность ключей.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable

import pytest

from core.domain.enums import IncrementalKind, RefreshPolicy, ReuseKind
from core.pipeline.incremental import (
    IncrementalItem,
    chunk_row_id,
    classify_reuse,
    doc_set_digest,
    doc_version_id,
    plan_incremental,
)
from core.ports.state import DocumentVersionRow, UrlStateRow


class FakeState:
    """Минимальный ``ProductStatePort``: планировщику нужен только ``iter_url_state``."""

    def __init__(self, rows: Iterable[UrlStateRow] = ()) -> None:
        self._rows = list(rows)

    def iter_url_state(self, product_id: str) -> list[UrlStateRow]:
        return [row for row in self._rows if row.product_id == product_id]


def _url(
    normalized_url: str,
    *,
    product_id: str = "p1",
    url: str | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
    doc_version_id: str | None = None,
) -> UrlStateRow:
    return UrlStateRow(
        product_id=product_id,
        url=url or normalized_url,
        normalized_url=normalized_url,
        etag=etag,
        last_modified=last_modified,
        doc_version_id=doc_version_id,
    )


def _doc(
    *,
    doc_version_id: str = "dv_1",
    content_hash: str = "h1",
    extractor_version: str = "ext-1",
    level: str = "html",
    status: str = "active",
    product_id: str = "p1",
) -> DocumentVersionRow:
    return DocumentVersionRow(
        doc_version_id=doc_version_id,
        doc_id=f"{product_id}:{content_hash}",
        product_id=product_id,
        url="https://p1.test/a",
        content_hash=content_hash,
        extractor_version=extractor_version,
        level=level,
        first_seen_run_id="run-1",
        last_seen_run_id="run-1",
        first_seen_at="2026-01-01T00:00:00Z",
        last_seen_at="2026-01-01T00:00:00Z",
        status=status,
    )


ROOTS = ["https://p1.test/", "https://p1.test/docs"]


def test_roots_are_always_fetched_in_input_order() -> None:
    for policy in RefreshPolicy:
        plan = plan_incremental(
            product_id="p1", roots=ROOTS, state=FakeState(), policy=policy
        )
        assert [(item.normalized_url, item.kind) for item in plan] == [
            ("https://p1.test/", IncrementalKind.FETCH),
            ("https://p1.test/docs", IncrementalKind.FETCH),
        ]
        assert [item.root_url for item in plan] == [
            "https://p1.test/",
            "https://p1.test/docs",
        ]
        assert all(item.depth == 0 for item in plan)


def test_offline_turns_known_urls_into_reuse_and_suppresses_discovery() -> None:
    state = FakeState([_url("https://p1.test/a", doc_version_id="dv_a")])

    plan = plan_incremental(
        product_id="p1",
        roots=["https://p1.test/"],
        state=state,
        policy=RefreshPolicy.OFFLINE,
        known_new_urls=["https://p1.test/new"],
    )

    assert [(item.normalized_url, item.kind) for item in plan] == [
        ("https://p1.test/", IncrementalKind.FETCH),
        ("https://p1.test/a", IncrementalKind.REUSE),
    ]
    assert plan[1].doc_version_id == "dv_a"


@pytest.mark.parametrize(
    "policy", [RefreshPolicy.AUTO, RefreshPolicy.REVALIDATE, RefreshPolicy.REFRESH]
)
def test_known_urls_are_revalidated_with_validators(policy: RefreshPolicy) -> None:
    state = FakeState(
        [
            _url(
                "https://p1.test/a",
                etag='"v1"',
                last_modified="Wed, 01 Jan 2026 00:00:00 GMT",
                doc_version_id="dv_a",
            )
        ]
    )

    plan = plan_incremental(
        product_id="p1", roots=["https://p1.test/"], state=state, policy=policy
    )

    item = plan[-1]
    assert item.kind is IncrementalKind.REVALIDATE
    assert item.etag == '"v1"'
    assert item.last_modified == "Wed, 01 Jan 2026 00:00:00 GMT"
    assert item.doc_version_id == "dv_a"


def test_revalidate_without_validators_is_still_revalidate() -> None:
    state = FakeState([_url("https://p1.test/a")])

    plan = plan_incremental(
        product_id="p1",
        roots=["https://p1.test/"],
        state=state,
        policy=RefreshPolicy.AUTO,
    )

    item = plan[-1]
    assert item.kind is IncrementalKind.REVALIDATE
    assert item.etag is None
    assert item.last_modified is None
    assert item.doc_version_id is None


def test_known_new_urls_are_fetch_and_deduped_against_roots_and_state() -> None:
    state = FakeState([_url("https://p1.test/known")])

    plan = plan_incremental(
        product_id="p1",
        roots=["https://p1.test/"],
        state=state,
        policy=RefreshPolicy.AUTO,
        known_new_urls=[
            "https://p1.test/",
            "https://p1.test/known",
            "https://p1.test/new-b",
            "https://p1.test/new-a",
            "https://p1.test/new-a",
        ],
    )

    assert [(item.normalized_url, item.kind) for item in plan] == [
        ("https://p1.test/", IncrementalKind.FETCH),
        ("https://p1.test/new-b", IncrementalKind.FETCH),
        ("https://p1.test/new-a", IncrementalKind.FETCH),
        # URL из known_new_urls уже был в state — остаётся revalidate, не fetch.
        ("https://p1.test/known", IncrementalKind.REVALIDATE),
    ]
    assert plan[1].root_url is None


def test_exclude_urls_drop_discovery_and_state_but_not_roots() -> None:
    state = FakeState(
        [_url("https://p1.test/excluded"), _url("https://p1.test/kept")]
    )

    plan = plan_incremental(
        product_id="p1",
        roots=["https://p1.test/excluded"],
        state=state,
        policy=RefreshPolicy.AUTO,
        known_new_urls=["https://p1.test/excluded", "https://p1.test/new"],
        exclude_urls=["https://p1.test/excluded"],
    )

    assert [item.normalized_url for item in plan] == [
        "https://p1.test/excluded",  # корень неприкосновенен
        "https://p1.test/new",
        "https://p1.test/kept",
    ]


def test_state_rows_are_sorted_and_normalized_urls_collapse_roots() -> None:
    state = FakeState(
        [
            _url("https://p1.test/c"),
            _url("https://p1.test/a"),
            _url("https://p1.test/b"),
        ]
    )

    plan = plan_incremental(
        product_id="p1",
        roots=["https://P1.TEST/", "https://p1.test:443/"],
        state=state,
        policy=RefreshPolicy.AUTO,
    )

    assert [item.normalized_url for item in plan] == [
        "https://p1.test/",
        "https://p1.test/a",
        "https://p1.test/b",
        "https://p1.test/c",
    ]


def test_state_is_scoped_to_product() -> None:
    state = FakeState(
        [
            _url("https://p1.test/a"),
            _url("https://p2.test/a", product_id="p2"),
        ]
    )

    plan = plan_incremental(
        product_id="p1",
        roots=["https://p1.test/"],
        state=state,
        policy=RefreshPolicy.AUTO,
    )

    assert [item.normalized_url for item in plan] == [
        "https://p1.test/",
        "https://p1.test/a",
    ]


def test_incremental_item_is_frozen() -> None:
    item = IncrementalItem(
        url="https://p1.test/",
        normalized_url="https://p1.test/",
        root_url="https://p1.test/",
        depth=0,
        kind=IncrementalKind.FETCH,
    )
    with pytest.raises(Exception):
        item.url = "https://p1.test/other"  # type: ignore[misc]


# -- classify_reuse ------------------------------------------------------ #
@pytest.mark.parametrize(
    ("not_modified", "content_hash", "has_prior", "has_doc", "expected"),
    [
        (True, None, False, False, ReuseKind.REVALIDATED),
        (True, "h1", True, True, ReuseKind.REVALIDATED),
        (False, "h1", True, True, ReuseKind.REVALIDATED),
        (False, "h2", True, True, ReuseKind.LIVE),
        (False, "h1", True, False, ReuseKind.LIVE),
        (False, "h1", False, False, ReuseKind.LIVE),
        (False, None, True, True, ReuseKind.LIVE),
    ],
)
def test_classify_reuse_matrix(
    not_modified: bool,
    content_hash: str | None,
    has_prior: bool,
    has_doc: bool,
    expected: ReuseKind,
) -> None:
    prior = _url("https://p1.test/a") if has_prior else None
    prior_doc = _doc() if has_doc else None

    assert (
        classify_reuse(
            prior=prior,
            prior_doc=prior_doc,
            not_modified=not_modified,
            content_hash=content_hash,
        )
        is expected
    )


# -- идентификаторы ------------------------------------------------------ #
def test_doc_version_id_is_stable() -> None:
    first = doc_version_id("p1", "h1", "ext-1", "html")
    second = doc_version_id("p1", "h1", "ext-1", "html")
    assert first == second
    assert first.startswith("dv_")
    assert len(first) == len("dv_") + 24


@pytest.mark.parametrize(
    "other",
    [
        ("p2", "h1", "ext-1", "html"),
        ("p1", "h2", "ext-1", "html"),
        ("p1", "h1", "ext-2", "html"),
        ("p1", "h1", "ext-1", "pdf"),
    ],
)
def test_doc_version_id_is_sensitive_to_each_key_element(
    other: tuple[str, str, str, str],
) -> None:
    assert doc_version_id(*other) != doc_version_id("p1", "h1", "ext-1", "html")


def test_chunk_row_id_is_distinct_from_run_level_chunk_id() -> None:
    durable = chunk_row_id("dv_1", "chk-1", 0)
    run_level = "p1:h1:0"
    assert durable != run_level
    assert ":" not in durable
    # Стабилен и чувствителен к версии чанкера и номеру.
    assert durable == chunk_row_id("dv_1", "chk-1", 0)
    assert durable != chunk_row_id("dv_1", "chk-2", 0)
    assert durable != chunk_row_id("dv_1", "chk-1", 1)
    assert durable != chunk_row_id("dv_2", "chk-1", 0)


def test_doc_set_digest_is_stable_and_order_insensitive() -> None:
    rows = [
        _doc(doc_version_id="dv_a", content_hash="ha"),
        _doc(doc_version_id="dv_b", content_hash="hb"),
    ]

    assert doc_set_digest(rows) == doc_set_digest(list(reversed(rows)))
    assert doc_set_digest(rows) == doc_set_digest(rows)
    assert doc_set_digest([]) != ""
    assert doc_set_digest([]) == doc_set_digest([])


def test_doc_set_digest_ignores_non_active_and_mutable_metadata() -> None:
    active = _doc(doc_version_id="dv_a", content_hash="ha")
    superseded = _doc(
        doc_version_id="dv_b", content_hash="hb", status="superseded"
    )
    evicted = _doc(doc_version_id="dv_c", content_hash="hc", status="evicted")

    assert doc_set_digest([active]) == doc_set_digest([active, superseded, evicted])

    # last_seen_* — факт наблюдения, а не состав doc-set.
    same_content = dataclasses.replace(
        active,
        last_seen_run_id="run-9",
        last_seen_at="2026-12-31T00:00:00Z",
        raw_path="/tmp/raw",
    )
    assert doc_set_digest([same_content]) == doc_set_digest([active])

    # Смена любого ключевого элемента меняет дайджест.
    assert doc_set_digest([_doc(doc_version_id="dv_a", content_hash="hx")]) != (
        doc_set_digest([active])
    )
