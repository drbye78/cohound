"""Тесты прибора ``scripts/measure_recon_instruments.py``.

Прибор отвечает на вопрос «чей механизм отбора доносит место, где утверждение нашлось», и
цена ошибки здесь — вывод об архитектуре. Поэтому проверяются свойства, поломка которых
даёт **правдоподобное, но неверное** число:

* наборы не сливаются: чанк, найденный только словесным запросом, не засчитывается
  фразовому (иначе «фразовый покрыл всё» появилось бы из чужой выдачи);
* объединение **дедуплицирует** чанк: размер — не сумма размеров наборов;
* известная цитата, которой нет ни в одном наборе, называется, а не исчезает;
* в пары МакНемара идут только факты, снятые оценщиком: иначе фразовый гейт сравнивался
  бы сам с собой;
* перенос на версии считается по объявленной целевой версии, а не по любой.

Живой корпус не нужен: индекс и артефакты подменяются, проверяются чистые функции.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "scripts"))

import measure_recon_instruments as measure  # noqa: E402

from core.domain.enums import FactReview, FactStatus, Importance, ParamType  # noqa: E402
from core.domain.models import BaselineFact, CatalogRow  # noqa: E402
from core.pipeline.reference_corpus import parameter_terms  # noqa: E402


class _FakeIndex:
    """Индекс по точному тексту запроса: тесту важно, **какой** набор что нашёл."""

    def __init__(self, by_query: dict[str, list[tuple[str, float]]]) -> None:
        self.by_query = by_query
        self.queries: list[str] = []

    def search_scores(
        self, query: str, product_ids: list[str] | None = None, top_k: int = 10
    ) -> list[tuple[str, float]]:
        self.queries.append(query)
        return list(self.by_query.get(query, []))[:top_k]


def _param(param_id: str = "p1", *, title: str = "Alpha feature") -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=title,
        module="module_core",
        group="core_capabilities",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule="rule text",
        concept_terms={"en": ["alpha"]},
    )


def _index(param: CatalogRow, *, terms: list[tuple[str, float]], phrases: dict[str, list[tuple[str, float]]]) -> _FakeIndex:
    """Индекс, отвечающий по известным запросам параметра (термины и каждый запрос Ш6)."""
    by_query: dict[str, list[tuple[str, float]]] = {
        " ".join(parameter_terms(param, query_lang="en")): terms
    }
    by_query.update(phrases)
    return _FakeIndex(by_query)


def _fact(
    param_id: str,
    *,
    version: str = "18",
    status: FactStatus = FactStatus.PRESENT,
    observed_by: str = "llm:fake@rev1:terms+phrases:evaluator_v6",
    quote: str | None = "alpha is supported",
) -> BaselineFact:
    return BaselineFact(
        baseline_id="upstream-pg",
        param_id=param_id,
        version=version,
        status=status,
        url="https://d/doc" if status is FactStatus.PRESENT else None,
        quote=quote if status is FactStatus.PRESENT else None,
        content_hash="a" * 64 if status is FactStatus.PRESENT else None,
        observed_at="2026-11-20T00:00:00+00:00",
        corpus_build_id="ref_postgresql_18_deadbeef",
        review=FactReview.PENDING,
        observed_by=observed_by,
    )


def _coverage(param: CatalogRow, index: _FakeIndex, texts: dict[str, str], quotes: dict[str, list[str]]):
    return measure.candidate_coverage(
        catalog=[param],
        index=index,
        texts=texts,
        product_id="reference",
        quotes=quotes,
        query_lang="en",
    )


def test_a_chunk_found_only_by_terms_does_not_count_for_phrases() -> None:
    """Иначе «фразовый покрыл всё» появилось бы из чужой выдачи."""
    param = _param()
    index = _index(param, terms=[("c-term", 1.0)], phrases={})
    payload = _coverage(param, index, {"c-term": "alpha is supported"}, {"p1": ["alpha is supported"]})
    row = payload["rows"][0]
    assert row["terms"] is True
    assert row["phrases"] is False
    assert row["union"] is True
    assert payload["counts"]["unique_to_terms"] == 1
    assert payload["counts"]["unique_to_phrases"] == 0


def test_union_deduplicates_a_chunk_found_by_both_sets() -> None:
    """Размер объединения — не сумма наборов: один чанк — одна строка кандидатов."""
    param = _param()
    index = _index(param, terms=[("c-both", 1.0)], phrases={"Alpha feature": [("c-both", 2.0)]})
    payload = _coverage(param, index, {"c-both": "alpha is supported"}, {"p1": ["alpha is supported"]})
    row = payload["rows"][0]
    assert row["n_terms"] == 1
    assert row["n_phrases"] == 1
    assert row["n_union"] == 1


def test_a_known_quote_missing_from_every_set_is_named() -> None:
    """Цитата, до которой не донёс никто, называется, а не исчезает из счёта."""
    param = _param()
    index = _index(param, terms=[("c-other", 1.0)], phrases={})
    payload = _coverage(param, index, {"c-other": "unrelated"}, {"p1": ["alpha is supported"]})
    assert payload["counts"]["missed_by_union"] == ["p1"]
    assert payload["counts"]["coverage"]["union"] == 0.0


def test_agreement_counts_only_facts_taken_by_the_evaluator() -> None:
    """Фразовый гейт не попадает в пары: иначе он сравнивался бы сам с собой."""
    facts = {
        "p1": _fact("p1"),
        "p2": _fact("p2", status=FactStatus.UNCLEAR, observed_by="phrase-1"),
    }
    cells = {"p2": {"status": "✅"}}
    payload = measure.instrument_agreement(
        catalog=[_param("p1"), _param("p2")], facts=facts, cells=cells
    )
    assert payload["available"] is True
    assert payload["instruments"]["llm"] == 1
    assert payload["instruments"]["phrase"] == 1
    # p2 снят фразовым гейтом — в пары не входит, поэтому «только карта» = 0.
    # Одна дискордантная пара даёт p=1.0: точный критерий не объявляет значимость там,
    # где различие — одна ячейка.
    assert payload["llm_vs_map"] == {
        "llm_only": 1,
        "map_only": 0,
        "both": 0,
        "neither": 0,
        "mcnemar_p": 1.0,
    }


def test_agreement_is_unavailable_without_evaluator_facts() -> None:
    """Файл, написанный фразовым гейтом, не даёт пары — и прибор это говорит, а не молчит."""
    facts = {"p1": _fact("p1", observed_by="phrase-1")}
    payload = measure.instrument_agreement(catalog=[_param()], facts=facts, cells={})
    assert payload["available"] is False
    assert "observed_by" in payload["reason"]


def test_cross_version_counts_declared_target_only() -> None:
    """Параметр, найденный только на 17-й, не попадает в счёт целевой 18-й."""
    facts = [
        _fact("p1", version="18"),
        _fact("p1", version="17"),
        _fact("p2", version="17"),
        _fact("p3", version="18", status=FactStatus.UNCLEAR),
    ]
    payload = measure.cross_version(
        catalog=[_param("p1"), _param("p2"), _param("p3")],
        facts=facts,
        versions=["18", "17"],
        target="18",
    )
    assert payload["present_at_target"] == 1
    assert payload["only_at_target"] == 0
    assert payload["at_two_or_more"] == 1
    assert payload["rows"][0]["versions"] == ["17", "18"]
    assert payload["distribution"] == {2: 1}
