"""Тесты ``SessionItemIndex``: LRU поверх точечных чтений стора (R1).

Фейк-стор считает вызовы: главное утверждение здесь — сколько раз индекс сходил
в хранилище, а не только что он вернул. Нейтральны (§1.3).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from core.domain import CrawlItemStatus, CrawlSessionItem
from core.pipeline.session_items import SessionItemIndex


def _item(url: str, product_id: str = "p1", **overrides: Any) -> CrawlSessionItem:
    base: dict[str, Any] = {
        "session_id": "s1",
        "product_id": product_id,
        "url": url,
        "status": CrawlItemStatus.PENDING,
    }
    base.update(overrides)
    return CrawlSessionItem(**base)


class FakeStore:
    """Минимальный стор: точечное чтение + итератор, оба считают вызовы."""

    def __init__(self, items: list[CrawlSessionItem]) -> None:
        self._rows = {(i.product_id, i.url): i for i in items}
        self.get_calls: list[tuple[str, str, str]] = []
        self.iter_calls: list[str] = []

    def get_crawl_session_item(
        self, session_id: str, product_id: str, url: str
    ) -> CrawlSessionItem | None:
        self.get_calls.append((session_id, product_id, url))
        return self._rows.get((product_id, url))

    def iter_crawl_session_items(
        self, session_id: str, *, product_id: str | None = None
    ) -> Iterator[CrawlSessionItem]:
        self.iter_calls.append(session_id)
        for (pid, _url), item in sorted(self._rows.items()):
            if product_id is not None and pid != product_id:
                continue
            yield item


@pytest.fixture()
def store() -> FakeStore:
    return FakeStore([_item("http://a/1"), _item("http://a/2")])


def test_hit_after_miss_reads_store_once(store: FakeStore) -> None:
    index = SessionItemIndex(store, "s1")
    first = index.get(("p1", "http://a/1"))
    second = index.get(("p1", "http://a/1"))
    assert first is not None
    assert second is first
    assert len(store.get_calls) == 1


def test_missing_url_is_not_read_twice(store: FakeStore) -> None:
    index = SessionItemIndex(store, "s1")
    assert index.get(("p1", "http://nope/")) is None
    assert index.get(("p1", "http://nope/")) is None
    assert ("p1", "http://nope/") not in index
    assert len(store.get_calls) == 1


def test_none_session_never_touches_store(store: FakeStore) -> None:
    index = SessionItemIndex(store, None)
    assert index.get(("p1", "http://a/1")) is None
    index[("p1", "http://a/1")] = _item("http://a/1")
    assert index.get(("p1", "http://a/1")) is not None
    assert len(index) == 0
    assert list(index) == []
    assert list(index.values()) == []
    assert list(index.items()) == []
    assert store.get_calls == []
    assert store.iter_calls == []


def test_setitem_is_memory_only_but_visible(store: FakeStore) -> None:
    index = SessionItemIndex(store, "s1")
    item = _item("http://new/1")
    index[("p1", "http://new/1")] = item
    assert store.get_calls == []
    assert index[("p1", "http://new/1")] is item
    assert store.get_calls == []


def test_eviction_by_capacity_rereads_store(store: FakeStore) -> None:
    index = SessionItemIndex(store, "s1", capacity=2)
    index.get(("p1", "http://a/1"))
    index.get(("p1", "http://a/2"))
    index.get(("p1", "http://a/3"))  # промах стора + вытеснение самого старого
    assert len(store.get_calls) == 3
    index.get(("p1", "http://a/1"))  # вытеснен — снова в стор
    assert len(store.get_calls) == 4
    # второй ключ остался в LRU
    index.get(("p1", "http://a/3"))
    assert len(store.get_calls) == 4


def test_values_and_items_use_one_iter_call(store: FakeStore) -> None:
    index = SessionItemIndex(store, "s1")
    values = list(index.values())
    assert [v.url for v in values] == ["http://a/1", "http://a/2"]
    assert store.get_calls == []
    assert len(store.iter_calls) == 1

    pairs = list(index.items())
    assert [k for k, _ in pairs] == [("p1", "http://a/1"), ("p1", "http://a/2")]
    assert store.get_calls == []
    assert len(store.iter_calls) == 2


def test_len_and_contains_agree_with_store(store: FakeStore) -> None:
    index = SessionItemIndex(store, "s1")
    assert len(index) == 2
    assert ("p1", "http://a/1") in index
    assert ("p1", "http://a/2") in index
    assert ("p2", "http://a/1") not in index
    assert ("p1", "http://nope/") not in index
    assert len(list(index)) == 2


def test_getitem_raises_on_missing(store: FakeStore) -> None:
    index = SessionItemIndex(store, "s1")
    with pytest.raises(KeyError):
        index[("p1", "http://nope/")]


def test_delitem_raises_when_row_absent(store: FakeStore) -> None:
    index = SessionItemIndex(store, "s1")
    index.get(("p1", "http://a/1"))
    with pytest.raises(KeyError):
        del index[("p1", "http://nope/")]


def test_capacity_must_be_positive(store: FakeStore) -> None:
    with pytest.raises(ValueError):
        SessionItemIndex(store, "s1", capacity=0)
