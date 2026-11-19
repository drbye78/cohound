"""Кандидаты recon для оценщика (``core/pipeline/recon_candidates.py``).

Предмет — объединение **двух** наборов кандидатов, и проверки держат именно его свойства:
ни один набор не является надмножеством другого (замер 19.11.2026: 49/53 и 28/53 против
53/53 вместе), поэтому объединение обязано сохранять оба, дедуплицировать по чанку и
отдавать детерминированный порядок (он же порядок входа оценщика, R7).
"""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from core.domain.enums import Importance, ParamType
from core.domain.models import CatalogRow
from core.pipeline.recon_candidates import CANDIDATE_POLICY, recon_candidates

PRODUCT = "upstream"


class _JournalIndex:
    """Фейковый индекс: заранее объявленная выдача + журнал запросов.

    Журнал нужен потому, что часть предмета — **что именно** спросили у индекса
    (язык терминов доходит до обоих наборов), а счёт этого не показывает.
    """

    engine_version = "fake-bm25"

    def __init__(self, hits: dict[str, list[tuple[str, float]]]) -> None:
        self.hits = hits
        self.queries: list[tuple[str, list[str], int]] = []

    def index(self, docs: Sequence[object]) -> None:  # pragma: no cover — порт
        raise AssertionError("кандидаты строятся по готовому индексу")

    def search_scores(
        self,
        query: str,
        product_ids: Sequence[str] | None = None,
        top_k: int = 8,
    ) -> list[tuple[str, float]]:
        self.queries.append((query, list(product_ids or []), top_k))
        return list(self.hits.get(query, []))[:top_k]


def _param(
    *,
    param_id: str = "p1",
    title: str = "Backup",
    terms: dict[str, list[str]] | None = None,
) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=title,
        module="m",
        group="g",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule="rule text",
        concept_terms=terms if terms is not None else {"en": ["backup", "restore"]},
    )


def test_policy_name_is_declared() -> None:
    """Имя правила входит в ``observed_by`` факта — оно часть провенанса."""
    assert CANDIDATE_POLICY == "terms+phrases"


def test_union_keeps_chunks_found_by_only_one_set() -> None:
    """Предмет: словесный набор находит X, фразовый — Y, и оба остаются кандидатами.

    Это и есть замер: 49/53 у слов, 28/53 у фраз, 53/53 у объединения. Тест держит
    свойство, а не число: если один набор когда-нибудь станет надмножеством, тест
    покажет, что цена объединения изменилась.
    """
    param = _param()
    index = _JournalIndex(
        {
            # словесный запрос (``" ".join(parameter_terms)``) находит только X
            "backup restore": [("x", 9.0)],
            # фразовые запросы (``render_queries``) находят только Y
            "Backup": [("y", 5.0)],
        }
    )
    pairs = recon_candidates(param, index=index, product_id=PRODUCT)
    assert [chunk_id for chunk_id, _score in pairs] == ["x", "y"]


def test_chunk_found_by_both_sets_appears_once_with_max_score() -> None:
    """Дедуп по ``chunk_id``: один чанк — одна строка, со **максимальным** счётом."""
    param = _param()
    index = _JournalIndex(
        {
            "backup restore": [("shared", 3.0), ("terms-only", 1.0)],
            "Backup": [("shared", 7.5)],
        }
    )
    pairs = recon_candidates(param, index=index, product_id=PRODUCT)
    assert [chunk_id for chunk_id, _score in pairs] == ["shared", "terms-only"]
    assert dict(pairs)["shared"] == 7.5


def test_order_is_deterministic() -> None:
    """Повторный вызов на том же индексе даёт тот же список: он же порядок промпта (R7)."""
    param = _param()
    index = _JournalIndex(
        {
            "backup restore": [("b", 2.0), ("a", 2.0), ("c", 1.0)],
            "Backup": [("d", 2.0)],
        }
    )
    first = recon_candidates(param, index=index, product_id=PRODUCT)
    second = recon_candidates(param, index=index, product_id=PRODUCT)
    assert first == second
    # Равные счета разрешаются ``chunk_id`` — иначе порядок зависел бы от выдачи индекса.
    assert [chunk_id for chunk_id, _score in first] == ["a", "b", "d", "c"]


def test_parameter_without_terms_yields_nothing() -> None:
    """Ни терминов, ни заголовка — пустой список кандидатов, без исключения.

    «Нечего спросить» — наблюдение, а не сбой: параметр без терминов не должен ронять
    разведку, он остаётся без кандидатов (и ячейка получит ``no_fragments``).
    """
    param = _param(title="", terms={})
    index = _JournalIndex({})
    assert recon_candidates(param, index=index, product_id=PRODUCT) == []
    # Запросов не было вовсе: пустая строка в индексе — не поиск, а шум в журнале.
    assert index.queries == []


def test_query_lang_reaches_both_sets() -> None:
    """``query_lang=en`` доходит до обоих наборов: русских терминов в запросах нет.

    Проверяется по журналу, а не по счёту: счёт не отличает «спросили не то» от
    «спросили то, а индекс ответил пусто».
    """
    param = _param(
        title="Backup",
        terms={"ru": ["резервное копирование"], "en": ["backup", "restore"]},
    )
    index = _JournalIndex({})
    recon_candidates(param, index=index, product_id=PRODUCT, query_lang="en")
    assert index.queries, "ни одного запроса — наборы не отработали"
    joined = " | ".join(query for query, _pids, _k in index.queries)
    assert "резервное" not in joined
    assert "backup" in joined.lower()
    # Оба набора спросили объявленным языком: словесный — склейкой терминов, фразовый —
    # запросами ``render_queries`` (заголовок + термины языка).
    assert any("restore" in query.lower() for query, _pids, _k in index.queries)


def test_search_is_product_scoped() -> None:
    """Кандидаты спрашиваются с ``product_ids``: чужой продукт в корпус не попадает (I17)."""
    param = _param()
    index = _JournalIndex({"backup restore": [("x", 1.0)]})
    recon_candidates(param, index=index, product_id=PRODUCT)
    assert all(pids == [PRODUCT] for _query, pids, _k in index.queries)


@pytest.mark.parametrize("lang", ["ru", "en", "all"])
def test_terms_and_phrases_depths_are_declared(lang: str) -> None:
    """Глубины наборов — параметры вызова, а не константы внутри: цена видна из журнала."""
    param = _param()
    index = _JournalIndex({})
    recon_candidates(
        param,
        index=index,
        product_id=PRODUCT,
        terms_top_k=7,
        phrases_top_k=3,
        query_lang=lang,
    )
    depths = {top_k for _query, _pids, top_k in index.queries}
    assert depths <= {7, 3}
