"""Work-set известных URL: режим продукта не отменяет политику без сети (план 19 §P5).

Дефект, который эти тесты держат закрытым: ``plan_incremental_items`` возвращал ``[]``
для любого продукта не в режиме ``incremental``. Под сетью это верно (BFS/sitemap сами
найдут документы), но под ``offline``/``frozen`` сети нет вовсе, поэтому проход видел
только авторские корни и **молча** терял всё, что найдено прошлым краулом. Измерено на
стенде ``ru-pg-dbms`` (04.11.2026): ``frozen`` собрал 2 документа из 9, причём на
``:memory:``-стенде приёмочные тесты P3 этого не видели — там продукт был объявлен
``incremental``, а живой прогон шёл в режиме ``full``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pytest

from core.domain.enums import IncrementalKind, ProductMode, RefreshPolicy
from core.pipeline.corpus_ingest import plan_incremental_items
from core.ports.state import UrlStateRow

ROOT = "https://p1.test/docs/"
KNOWN = "https://p1.test/docs/guide.pdf"


class _FakeState:
    """Минимальный ``ProductStatePort``: планировщику нужен только ``iter_url_state``."""

    def __init__(self, rows: Iterable[UrlStateRow] = ()) -> None:
        self._rows = list(rows)

    def iter_url_state(self, product_id: str) -> list[UrlStateRow]:
        return [row for row in self._rows if row.product_id == product_id]


def _state() -> _FakeState:
    return _FakeState(
        [
            UrlStateRow(
                product_id="p1",
                url=ROOT,
                normalized_url=ROOT,
                doc_version_id="dv_root",
            ),
            UrlStateRow(
                product_id="p1",
                url=KNOWN,
                normalized_url=KNOWN,
                doc_version_id="dv_known",
            ),
        ]
    )


def _items(
    *, mode: ProductMode, policy: RefreshPolicy
) -> list[Any]:
    return plan_incremental_items(
        "p1",
        [ROOT],
        state=_state(),
        policy=policy,
        product_modes={"p1": mode.value},
        planned_validators={},
        errors=[],
    )


@pytest.mark.parametrize("policy", [RefreshPolicy.OFFLINE, RefreshPolicy.FROZEN])
def test_no_network_work_set_includes_known_urls_even_in_full_mode(
    policy: RefreshPolicy,
) -> None:
    """``full`` + политика без сети: известный URL — в work-set, а не только корень."""
    items = _items(mode=ProductMode.FULL, policy=policy)

    assert [item.normalized_url for item in items] == [KNOWN]
    assert items[0].kind is IncrementalKind.REUSE
    assert items[0].root_url is None, "авторский корень обрабатывается прежним путём"


def test_network_work_set_is_not_extended_for_full_mode() -> None:
    """Под сетью прежнее поведение не меняется: ``full`` — BFS/sitemap, не ``url_state``."""
    assert _items(mode=ProductMode.FULL, policy=RefreshPolicy.AUTO) == []


def test_network_work_set_keeps_revalidation_for_incremental_mode() -> None:
    """``incremental`` под ``auto`` по-прежнему ревалидирует известные URL."""
    items = _items(mode=ProductMode.INCREMENTAL, policy=RefreshPolicy.AUTO)

    assert [item.normalized_url for item in items] == [KNOWN]
    assert items[0].kind is IncrementalKind.REVALIDATE