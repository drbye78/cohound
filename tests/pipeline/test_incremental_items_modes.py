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
from core.pipeline.crawl_frontier import CrawlLimits, Frontier
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


def _items(*, mode: ProductMode, policy: RefreshPolicy) -> list[Any]:
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


# --------------------------------------------------------------------------- #
# Объявленное пространство: url_state хранит и выведенные корни (23.11.2026)
# --------------------------------------------------------------------------- #

RETIRED = "https://edu.p1.test/docs/old.html"


def _state_with_retired_root() -> _FakeState:
    """Строка ``url_state``, чей корень выведен из пакета, но запись осталась."""
    return _FakeState(
        [
            UrlStateRow(
                product_id="p1",
                url=KNOWN,
                normalized_url=KNOWN,
                doc_version_id="dv_known",
            ),
            UrlStateRow(
                product_id="p1",
                url=RETIRED,
                normalized_url=RETIRED,
                doc_version_id="dv_retired",
            ),
        ]
    )


def test_no_network_work_set_drops_urls_outside_declared_space() -> None:
    """Под политикой без сети набор ограничен **объявленным** пространством.

    Дефект: под сетью URL вне scope отсекает ``Frontier``, а под ``offline``/``frozen``
    Frontier не вызывается вовсе — и материал выведенного корня возвращался в корпус.
    Измерено 23.11.2026: 163 документа `edu.postgrespro.ru` вошли в прогон
    `ru-pg-dbms-902c0b71c419`, тогда как предыдущий их не содержал.
    """
    dropped: list[tuple[str, str]] = []
    items = plan_incremental_items(
        "p1",
        [ROOT],
        state=_state_with_retired_root(),
        policy=RefreshPolicy.OFFLINE,
        product_modes={"p1": ProductMode.FULL.value},
        planned_validators={},
        errors=[],
        scopes=[(ROOT, Frontier(ROOT, CrawlLimits()))],
        dropped=dropped,
    )

    assert [item.normalized_url for item in items] == [KNOWN]
    assert dropped == [(RETIRED, "out_of_declared_space")]


def test_declared_space_filter_is_noop_for_network_policies() -> None:
    """Под сетью фильтр не применяется: там пространство держит ``Frontier``."""
    items = plan_incremental_items(
        "p1",
        [ROOT],
        state=_state_with_retired_root(),
        policy=RefreshPolicy.AUTO,
        product_modes={"p1": ProductMode.INCREMENTAL.value},
        planned_validators={},
        errors=[],
        scopes=[(ROOT, Frontier(ROOT, CrawlLimits()))],
    )

    assert {item.normalized_url for item in items} == {KNOWN, RETIRED}

# --------------------------------------------------------------------------- #
# Блэклист оператора — часть объявленного пространства (27.11.2026)
# --------------------------------------------------------------------------- #

PARAMETRIC = "https://p1.test/docs/guide?lang=en"

def _state_with_parametric() -> _FakeState:
    """Строка ``url_state`` с параметрической query (``?lang=``) рядом с «голым» адресом."""
    return _FakeState(
        [
            UrlStateRow(
                product_id="p1",
                url=KNOWN,
                normalized_url=KNOWN,
                doc_version_id="dv_known",
            ),
            UrlStateRow(
                product_id="p1",
                url=PARAMETRIC,
                normalized_url=PARAMETRIC,
                doc_version_id="dv_param",
            ),
        ]
    )

def test_no_network_work_set_applies_operator_blacklist() -> None:
    """Под политикой без сети блэклист применяется так же, как под сетью.

    Дефект-класс: ``exclude_queries``/``exclude_paths`` входят в ``crawl_config_digest``,
    то есть объявлены частью пространства, но фильтр прохода без сети проверял только
    ``scope_admission`` (host + path-prefix + SSRF). Объявленное правило «параметрический
    URL — не материал продукта» держалось бы только под сетью, а ``url_state`` возвращал бы
    такие адреса в корпус при ``offline``/``frozen``. Измерено 27.11.2026 на стенде:
    23 228 адресов с query из 81 575 в ``url_state``.
    """
    dropped: list[tuple[str, str]] = []
    items = plan_incremental_items(
        "p1",
        [ROOT],
        state=_state_with_parametric(),
        policy=RefreshPolicy.OFFLINE,
        product_modes={"p1": ProductMode.FULL.value},
        planned_validators={},
        errors=[],
        scopes=[(ROOT, Frontier(ROOT, CrawlLimits(exclude_queries=("*",))))],
        dropped=dropped,
    )

    assert [item.normalized_url for item in items] == [KNOWN]
    assert dropped == [(PARAMETRIC, "blacklisted")]

def test_blacklist_filter_does_not_touch_urls_without_query() -> None:
    """``["*"]`` не матчит URL без query: «голые» адреса остаются в наборе."""
    dropped: list[tuple[str, str]] = []
    items = plan_incremental_items(
        "p1",
        [ROOT],
        state=_state_with_parametric(),
        policy=RefreshPolicy.OFFLINE,
        product_modes={"p1": ProductMode.FULL.value},
        planned_validators={},
        errors=[],
        scopes=[
            (
                ROOT,
                Frontier(
                    ROOT,
                    CrawlLimits(exclude_queries=("*",), exclude_paths=("/order/*",)),
                ),
            )
        ],
        dropped=dropped,
    )

    assert [item.normalized_url for item in items] == [KNOWN]
    assert dropped == [(PARAMETRIC, "blacklisted")]

