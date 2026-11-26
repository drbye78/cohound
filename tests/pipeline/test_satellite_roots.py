"""Вывод корней сателлита из наблюдений: числа, неоднозначность и границы.

Пины держат четыре свойства, каждое из которых иначе теряется молча:

1. **Порог отделяет раздел от меню** — префикс с покрытием ниже `MIN_ROOT_LINKS` не
   становится корнем, но попадает в `rejected` с числом (видимость, а не тишина).
2. **Избыточный ребёнок схлопывается** — если префикс-родитель покрывает столько же
   ссылок, ребёнок не предлагается вторым корнем (иначе один материал объявлялся бы
   дважды, и бюджет делился бы между двумя адресами одного дерева).
3. **Неоднозначность не разрешается прибором** — два корня минимальной глубины с равным
   покрытием дают `ambiguous`, и `roots_by_role` такую роль пропускает: выбор — решение
   человека.
4. **Карта сайта усиливает и дополняет** — префикс, известный только по `<loc>`,
   становится кандидатом с `evidence=("sitemap",)`, а совпавший с ссылками несёт оба
   свидетельства.
"""

from __future__ import annotations

from core.pipeline.satellite_roots import (
    MIN_ROOT_LINKS,
    derive_roots,
    roots_by_role,
)

HOST = "sat.example"


def _urls(prefix: str, count: int, *, host: str = HOST) -> list[str]:
    return [f"https://{host}/{prefix}/page-{index}.html" for index in range(count)]


def _plan(**kwargs: object):
    base = {
        "product_id": "demo",
        "role": "backup",
        "component": "Backup",
        "seed_url": f"https://{HOST}/",
        "final_url": f"https://{HOST}/",
    }
    base.update(kwargs)
    return derive_roots(**base)  # type: ignore[arg-type]


def test_prefix_below_threshold_is_rejected_with_count() -> None:
    plan = _plan(links=[f"https://{HOST}/about", f"https://{HOST}/contacts"])
    assert plan.candidates == []
    rejected = [item for item in plan.rejected if item["reason"] == "below_min_links"]
    assert rejected and rejected[0]["min_links"] == MIN_ROOT_LINKS


def test_section_becomes_root_with_link_evidence() -> None:
    plan = _plan(links=_urls("docs", 5))
    assert [item.key for item in plan.candidates] == [f"{HOST}/docs"]
    assert plan.candidates[0].documents == 5
    assert plan.candidates[0].evidence == ("links",)
    assert plan.recommended() is not None
    assert not plan.ambiguous


def test_specific_prefix_wins_over_equal_coverage_parent() -> None:
    # Одна и та же пятёрка ссылок даёт префиксы /docs, /docs/v1 и /docs/v1/... —
    # корнем остаётся самый точный: родитель с тем же покрытием захватил бы соседние
    # разделы, которых в наблюдении нет.
    plan = _plan(links=_urls("docs/v1", 5))
    assert [item.key for item in plan.candidates] == [f"{HOST}/docs/v1"]


def test_equal_depth_equal_coverage_is_ambiguous_and_not_applied() -> None:
    links = _urls("docs", 4) + _urls("kb", 4)
    plan = _plan(links=links)
    assert {item.key for item in plan.candidates} == {f"{HOST}/docs", f"{HOST}/kb"}
    assert plan.ambiguous
    assert plan.recommended() is None
    assert roots_by_role([plan]) == {}


def test_sitemap_adds_candidate_and_strengthens_existing() -> None:
    links = _urls("docs", 4)
    sitemap = _urls("docs", 4) + _urls("api", 6)
    plan = _plan(links=links, sitemap_urls=sitemap)
    by_key = {item.key: item for item in plan.candidates}
    assert set(by_key) == {f"{HOST}/docs", f"{HOST}/api"}
    assert by_key[f"{HOST}/docs"].evidence == ("links", "sitemap")
    assert by_key[f"{HOST}/api"].evidence == ("sitemap",)


def test_external_host_links_are_visible_not_counted() -> None:
    links = [*_urls("docs", 4), "https://other.example/x", "https://other.example/y"]
    plan = _plan(links=links)
    assert [item.key for item in plan.candidates] == [f"{HOST}/docs"]
    assert any(item["reason"] == "external_host" for item in plan.rejected)


def test_redirect_to_other_host_moves_the_root() -> None:
    plan = _plan(
        seed_url=f"https://{HOST}/",
        final_url="https://portal.example/",
        links=[f"https://portal.example/docs/page-{i}.html" for i in range(4)],
    )
    assert [item.key for item in plan.candidates] == ["portal.example/docs"]
    assert any(note.startswith("redirect_host") for note in plan.notes)


def test_no_links_is_named_not_invented() -> None:
    plan = _plan(links=[], sitemap_urls=[])
    assert plan.candidates == []
    assert "no_links" in plan.notes


def test_robots_disallow_is_a_note_not_a_rejection() -> None:
    plan = _plan(links=_urls("docs", 4), robots_allows=False)
    assert [item.key for item in plan.candidates] == [f"{HOST}/docs"]
    assert "robots_disallow_seed" in plan.notes
