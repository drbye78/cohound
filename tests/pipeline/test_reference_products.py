"""План 27 §3–§13: reference-продукты профиля — роли, факты, ревью, refresh, отчёт.

Проверки держат ровно те свойства, ради которых подсистема существует:

* роль reference-продукта **вычисляется** из паспортов, а не объявляется полем;
* ``review`` — переключатель набора унаследованных ячеек, и без ``accepted`` ячейки нет;
* ``refresh`` не пишет ``removed`` и не трогает ``baselines.csv``;
* отчёт считает наследование тем же кодом, что Ш4 (второй формулы нет).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.domain.enums import (
    FactReview,
    FactStatus,
    Importance,
    InheritanceMode,
    ParamType,
)
from core.domain.models import (
    BaselineFact,
    BaselineRow,
    CatalogRow,
    Citation,
    Passport,
    ReferenceProduct,
    ReferenceProducts,
    ReferenceProductVersion,
    Upstream,
)
from core.pipeline.reference_products import (
    DIFF_ADDED,
    DIFF_CHANGED,
    DIFF_REMOVED,
    DIFF_UNCHANGED,
    DIFF_UNCLEAR,
    apply_refresh,
    baseline_roles,
    fact_rows,
    inheritance_report,
    load_package_facts,
    refresh_diff,
    review_facts,
    write_facts,
)

FACT_HEADER = (
    "baseline_id,param_id,version,status,url,quote,content_hash,observed_at,"
    "corpus_build_id,review,reviewed_by,note\n"
)


def _package(tmp_path: Path) -> Path:
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    (package / "baselines.csv").write_text(
        "baseline_id,param_id,min_version,inheritance_mode,note\n", encoding="utf-8"
    )
    return package


def _param(param_id: str) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=f"Title {param_id}",
        module="m",
        group="g",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule="rule text",
    )


def _passport(product_id: str, *baselines: str) -> Passport:
    return Passport(
        product_id=product_id,
        canonical_name=f"Product {product_id}",
        vendor="Vendor",
        upstreams=[
            Upstream(
                baseline_id=baseline_id,
                version="18",
                evidence=Citation(url="https://u.test", quote="q"),
            )
            for baseline_id in baselines
        ],
        jurisdiction="RU",
        snapshot_date=__import__("datetime").date(2026, 1, 1),
    )


def _fact(
    param_id: str = "p1",
    *,
    baseline_id: str = "upstream-pg",
    version: str = "18",
    status: FactStatus = FactStatus.PRESENT,
    review: FactReview = FactReview.ACCEPTED,
    quote: str = "upstream documents the mechanism",
    digest: str = "a" * 64,
) -> BaselineFact:
    return BaselineFact(
        baseline_id=baseline_id,
        param_id=param_id,
        version=version,
        status=status,
        url="https://upstream.test/doc",
        quote=quote if status is FactStatus.PRESENT else None,
        content_hash=digest if status is FactStatus.PRESENT else None,
        review=review,
        reviewed_by="roger" if review is not FactReview.PENDING else None,
    )


# ---------------------------------------------------------------------------
# Роли: вычисляются, а не объявляются (§3.1)
# ---------------------------------------------------------------------------


def test_role_is_ancestor_when_a_cohort_product_declares_the_line() -> None:
    declaration = ReferenceProducts(
        schema="reference_products/1",
        products=[
            ReferenceProduct(
                baseline_id="upstream-pg",
                product_id="postgresql",
                canonical_name="PostgreSQL",
                versions=[ReferenceProductVersion(version="18", docs_roots=["https://d"])],
                current_version="18",
            ),
            ReferenceProduct(
                baseline_id="upstream-mysql",
                product_id="mysql",
                canonical_name="MySQL",
                versions=[ReferenceProductVersion(version="8.4", docs_roots=["https://m"])],
                current_version="8.4",
            ),
        ],
    )
    passports = {"alpha": _passport("alpha", "upstream-pg"), "beta": _passport("beta")}
    roles = baseline_roles(declaration, passports, cohort_products=["alpha", "beta"])
    assert roles["upstream-pg"]["role"] == "ancestor"
    assert roles["upstream-pg"]["ancestors_of"] == ["alpha"]
    # Параллель — не «нет данных»: это измеренный факт «никто в когорте не объявил линию».
    assert roles["upstream-mysql"]["role"] == "parallel"
    assert roles["upstream-mysql"]["ancestors_of"] == []


def test_roles_ignore_products_outside_the_cohort() -> None:
    """Паспорт чужого пакета не делает линию предком этого (§3.1)."""
    declaration = ReferenceProducts(
        schema="reference_products/1",
        products=[
            ReferenceProduct(
                baseline_id="upstream-pg",
                product_id="postgresql",
                canonical_name="PostgreSQL",
                versions=[ReferenceProductVersion(version="18", docs_roots=["https://d"])],
                current_version="18",
            )
        ],
    )
    passports = {"stranger": _passport("stranger", "upstream-pg")}
    roles = baseline_roles(declaration, passports, cohort_products=["alpha"])
    assert roles["upstream-pg"]["role"] == "parallel"


def test_roles_empty_without_declaration() -> None:
    assert baseline_roles(None, {"alpha": _passport("alpha", "x")}) == {}


# ---------------------------------------------------------------------------
# Факты: фильтры и ревью (§3.3, §5)
# ---------------------------------------------------------------------------


def test_fact_filters_compare_version_as_written() -> None:
    """«18» и «18.4.1» в файле — разные записи; приведение к мажору скрыло бы дефект."""
    facts = [_fact("p1", version="18"), _fact("p2", version="18.4.1")]
    assert [f.param_id for f in fact_rows(facts, version="18")] == ["p1"]
    assert [f.param_id for f in fact_rows(facts, version="18.4.1")] == ["p2"]
    assert len(fact_rows(facts, review=FactReview.ACCEPTED)) == 2
    assert fact_rows(facts, review=FactReview.PENDING) == []


def test_review_accept_writes_fact_and_apply_log(tmp_path: Path) -> None:
    package = _package(tmp_path)
    write_facts(package, [_fact("p1", review=FactReview.PENDING)])
    payload = review_facts(package, baseline_id="upstream-pg", actor="roger", accept=True)
    assert payload["changed"] == [
        {"param_id": "p1", "version": "18", "from": "pending", "to": "accepted"}
    ]
    stored = load_package_facts(package)
    assert stored[0].review is FactReview.ACCEPTED
    assert stored[0].reviewed_by == "roger"
    log = (package / ".authoring" / "apply_log.jsonl").read_text(encoding="utf-8")
    entry = json.loads(log.splitlines()[-1])
    assert entry["op"] == "baseline-review"
    assert entry["actor"] == "roger"


def test_review_dry_run_writes_nothing(tmp_path: Path) -> None:
    package = _package(tmp_path)
    write_facts(package, [_fact("p1", review=FactReview.PENDING)])
    payload = review_facts(
        package, baseline_id="upstream-pg", actor="roger", accept=True, dry_run=True
    )
    assert payload["changed"], "dry-run обязан показать, что записал бы"
    assert load_package_facts(package)[0].review is FactReview.PENDING
    assert not (package / ".authoring" / "apply_log.jsonl").exists()


def test_review_requires_actor(tmp_path: Path) -> None:
    package = _package(tmp_path)
    write_facts(package, [_fact("p1", review=FactReview.PENDING)])
    with pytest.raises(ValueError, match="actor"):
        review_facts(package, baseline_id="upstream-pg", actor="  ", accept=True)


def test_review_clear_returns_to_pending(tmp_path: Path) -> None:
    package = _package(tmp_path)
    write_facts(package, [_fact("p1")])
    review_facts(package, baseline_id="upstream-pg", actor="roger", accept=None)
    stored = load_package_facts(package)[0]
    assert stored.review is FactReview.PENDING
    assert stored.reviewed_by is None


# ---------------------------------------------------------------------------
# Refresh (§13)
# ---------------------------------------------------------------------------


def test_refresh_diff_classifies_all_five_classes() -> None:
    previous = [
        _fact("added", version="17", status=FactStatus.ABSENT),
        _fact("removed", version="17"),
        _fact("changed", version="17", quote="old text"),
        _fact("same", version="17"),
        _fact("unclear_now", version="17"),
    ]
    current = [
        _fact("added", version="18"),
        _fact("removed", version="18", status=FactStatus.ABSENT),
        _fact("changed", version="18", quote="new text"),
        _fact("same", version="18"),
        _fact("unclear_now", version="18", status=FactStatus.UNCLEAR),
    ]
    diff = refresh_diff(previous, current)
    assert [item["param_id"] for item in diff[DIFF_ADDED]] == ["added"]
    assert [item["param_id"] for item in diff[DIFF_REMOVED]] == ["removed"]
    assert [item["param_id"] for item in diff[DIFF_CHANGED]] == ["changed"]
    assert [item["param_id"] for item in diff[DIFF_UNCHANGED]] == ["same"]
    assert [item["param_id"] for item in diff[DIFF_UNCLEAR]] == ["unclear_now"]


def test_refresh_never_writes_removed_and_never_touches_baselines(tmp_path: Path) -> None:
    package = _package(tmp_path)
    baselines_before = (package / "baselines.csv").read_text(encoding="utf-8")
    write_facts(
        package,
        [_fact("gone", version="17"), _fact("kept", version="17", quote="old")],
    )
    current = [
        _fact("gone", version="18", status=FactStatus.ABSENT),
        _fact("kept", version="18", quote="new"),
    ]
    payload = apply_refresh(
        package,
        baseline_id="upstream-pg",
        version="18",
        actor="roger",
        current_facts=current,
    )
    assert [item["param_id"] for item in payload["diff"][DIFF_REMOVED]] == ["gone"]
    stored = {(fact.param_id, fact.version): fact for fact in load_package_facts(package)}
    # Регрессия не записана: новая версия «gone» отсутствует, старая осталась историей.
    assert ("gone", "18") not in stored
    assert ("gone", "17") in stored
    # `changed` применён.
    assert stored[("kept", "18")].quote == "new"
    assert (package / "baselines.csv").read_text(encoding="utf-8") == baselines_before
    assert payload["proposed_min_version"] == []


def test_refresh_proposes_min_version_for_added_only(tmp_path: Path) -> None:
    package = _package(tmp_path)
    write_facts(package, [_fact("old", version="17")])
    payload = apply_refresh(
        package,
        baseline_id="upstream-pg",
        version="18",
        actor="roger",
        current_facts=[_fact("old", version="18"), _fact("new", version="18")],
        dry_run=True,
    )
    assert payload["proposed_min_version"] == ["new"]


# ---------------------------------------------------------------------------
# Отчёт: то же наследование, что в Ш4 (§10)
# ---------------------------------------------------------------------------


def test_report_inherits_from_the_first_ancestor_that_has_an_accepted_fact() -> None:
    """Архетип B: победил второй предок, причина по первому опубликована (§3.2)."""
    passports = {"alpha": _passport("alpha", "upstream-fork", "upstream-pg")}
    baselines = [
        BaselineRow(
            baseline_id="upstream-fork",
            param_id="p1",
            inheritance_mode=InheritanceMode.DIRECT,
        ),
        BaselineRow(
            baseline_id="upstream-pg",
            param_id="p1",
            inheritance_mode=InheritanceMode.DIRECT,
        ),
    ]
    facts = [_fact("p1", baseline_id="upstream-pg")]
    report = inheritance_report(
        baselines=baselines,
        products=["alpha"],
        passports=passports,
        facts=facts,
        params=[_param("p1")],
    )
    assert report["inherited_cells"] == 1
    product = report["products"]["alpha"]
    assert product["cells_emitted"] == 1
    assert product["not_inherited"] == []
    assert product["baseline_ids"] == ["upstream-fork", "upstream-pg"]


def test_report_publishes_reasons_when_nothing_is_inherited() -> None:
    passports = {"alpha": _passport("alpha", "upstream-pg")}
    baselines = [
        BaselineRow(
            baseline_id="upstream-pg",
            param_id="p1",
            inheritance_mode=InheritanceMode.DIRECT,
        )
    ]
    report = inheritance_report(
        baselines=baselines,
        products=["alpha"],
        passports=passports,
        facts=[_fact("p1", review=FactReview.PENDING)],
        params=[_param("p1")],
    )
    assert report["inherited_cells"] == 0
    product = report["products"]["alpha"]
    assert product["reason_counts"] == {"baseline_fact_pending": 1}
    assert product["not_inherited"][0]["param_id"] == "p1"


def test_facts_csv_round_trip_keeps_all_columns(tmp_path: Path) -> None:
    package = _package(tmp_path)
    write_facts(package, [_fact("p1"), _fact("p2", status=FactStatus.ABSENT)])
    text = (package / "sources" / "baseline_facts.csv").read_text(encoding="utf-8")
    assert text.splitlines()[0] == FACT_HEADER.strip()
    restored = load_package_facts(package)
    assert [fact.param_id for fact in restored] == ["p1", "p2"]
    assert restored[1].status is FactStatus.ABSENT
