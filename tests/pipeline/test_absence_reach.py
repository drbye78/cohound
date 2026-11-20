"""Тесты достижимости эталонных ``❌`` (:mod:`core.pipeline.absence_reach`).

Модуль отвечает на вопрос «сколько эталона вообще достижимо», и цена ошибки — вывод об
архитектуре: неверный потолок превратил бы «методология запрещает вердикт» в «система не
понимает» (или наоборот). Поэтому проверяются свойства, поломка которых даёт
**правдоподобное, но неверное** число:

* ``registry_absence`` достижим без носителя (I2b: реестр — авторитетная энумерация);
* ``bounded`` достижим только при снятии **каждого** корня — частичное исчерпание не
  считается (правило берётся из домена, а не переписывается здесь);
* ``adopted``-снятие ``bounded`` не удостоверяет;
* правило ``explicit`` не приравнивается ни к достижимому, ни к недостижимому: оно вне
  потолка, иначе объявило бы недостижимым неизмеренное;
* ``❓`` на недостижимом ``❌`` не считается ошибкой системы, а ``✅`` на нём — считается;
* ``carriers_from_coverage`` берёт причину из записи прогона, а не догадывается.

Живой корпус не нужен: проверяются чистые функции на синтетических данных.
"""

from __future__ import annotations

from core.domain.enums import (
    AbsenceRuleType,
    Importance,
    ParamType,
    SourceKind,
    Status,
)
from core.domain.models import (
    CatalogRow,
    CorpusCoverage,
    CorpusRootCoverage,
    GoldenRow,
)
from core.pipeline.absence_reach import (
    CARRIER_MISSING,
    POLICY_SKIPPED,
    CellVerdict,
    ProductCarrier,
    agreement_with_ceiling,
    carriers_from_coverage,
    classify_reachability,
)


def _param(param_id: str, rule: AbsenceRuleType) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=param_id,
        module="m",
        group="g",
        importance=Importance.BASIC,
        param_type=ParamType.FACT,
        rule="r",
        absence_rule=rule,
    )


def _golden(product: str, param: str, status: Status) -> GoldenRow:
    return GoldenRow(
        product_id=product,
        param_id=param,
        expected_status=status,
        expected_evidence_kind=[SourceKind.DOCUMENTATION],
        case_class="class_ok",
    )


def _root(url: str, *, complete: bool, how: str | None = "live", matched=True):
    return CorpusRootCoverage(
        root_url=url,
        capture_id="cap" if complete else None,
        complete=complete,
        observed_how=how if complete else None,
        observed_at="2026-11-07T00:00:00+00:00" if complete else None,
        policy_digest="d",
        policy_matched=matched,
    )


def test_registry_absence_is_reachable_without_carrier() -> None:
    """I2b: реестр авторитетен — снятие корней и подпись не нужны."""
    catalog = {"p": _param("p", AbsenceRuleType.REGISTRY_ABSENCE)}
    reach = classify_reachability(
        [_golden("prod", "p", Status.ABSENT)], catalog, carriers={}
    )
    assert reach.absent_reachable == 1
    assert reach.absent_unreachable == 0


def test_bounded_needs_every_root_captured() -> None:
    """Частичное исчерпание — не носитель: корень без complete=1 отменяет ``bounded``."""
    catalog = {"p": _param("p", AbsenceRuleType.DISTRIBUTION_ABSENCE)}
    carrier = ProductCarrier(
        complete=False,
        roots=(
            _root("https://a/", complete=True),
            _root("https://b/", complete=False),
        ),
        skipped_by_policy=False,
    )
    reach = classify_reachability(
        [_golden("prod", "p", Status.ABSENT)], catalog, {"prod": carrier}
    )
    assert reach.absent_unreachable == 1
    assert reach.absent_unreachable_by_reason == {CARRIER_MISSING: 1}


def test_bounded_reachable_when_all_roots_captured() -> None:
    """Все корни исчерпаны снятием — ``bounded`` достижим, потолок 1.0."""
    catalog = {"p": _param("p", AbsenceRuleType.PARAMETER_REFERENCE)}
    carrier = ProductCarrier(
        complete=True,
        roots=(_root("https://a/", complete=True), _root("https://b/", complete=True)),
        skipped_by_policy=False,
    )
    reach = classify_reachability(
        [_golden("prod", "p", Status.ABSENT)], catalog, {"prod": carrier}
    )
    assert reach.absent_reachable == 1
    assert reach.ceiling == 1.0


def test_adopted_capture_does_not_attest_bounded() -> None:
    """Предоставление — не обход: ``adopted`` исчерпание не удостоверяет."""
    catalog = {"p": _param("p", AbsenceRuleType.DISTRIBUTION_ABSENCE)}
    carrier = ProductCarrier(
        complete=False,
        roots=(_root("https://a/", complete=True, how="adopted"),),
        skipped_by_policy=False,
    )
    reach = classify_reachability(
        [_golden("prod", "p", Status.ABSENT)], catalog, {"prod": carrier}
    )
    assert reach.absent_unreachable == 1


def test_policy_skipped_is_named_separately_from_missing_carrier() -> None:
    """«Не смотрели по решению» и «смотрели, но не исчерпали» — разные причины."""
    catalog = {"p": _param("p", AbsenceRuleType.DISTRIBUTION_ABSENCE)}
    carrier = ProductCarrier(
        complete=False, roots=(), skipped_by_policy=True
    )
    reach = classify_reachability(
        [_golden("prod", "p", Status.ABSENT)], catalog, {"prod": carrier}
    )
    assert reach.absent_unreachable_by_reason == {POLICY_SKIPPED: 1}


def test_explicit_rule_stays_out_of_the_ceiling() -> None:
    """Достижимость ``explicit`` решает цитата ячейки — до вердикта невычислима."""
    catalog = {"p": _param("p", AbsenceRuleType.EXPLICIT)}
    reach = classify_reachability(
        [_golden("prod", "p", Status.ABSENT)], catalog, carriers={}
    )
    assert reach.absent_unknown == 1
    assert reach.absent_unreachable == 0
    # Измеримого знаменателя нет: единственная ячейка — ``unknown``. ``None``, а не 1.0 и
    # не 0.0: потолок не измерен, и число выдало бы мнение за замер (A3).
    assert reach.ceiling is None


def test_non_absent_golden_does_not_enter_the_ceiling() -> None:
    """``✅``/``⚠️`` не имеют вопроса о носителе и не разбавляют потолок."""
    catalog = {
        "p": _param("p", AbsenceRuleType.DISTRIBUTION_ABSENCE),
        "q": _param("q", AbsenceRuleType.DISTRIBUTION_ABSENCE),
    }
    reach = classify_reachability(
        [
            _golden("prod", "p", Status.ABSENT),
            _golden("prod", "q", Status.OK),
            _golden("prod", "q", Status.PARTIAL),
        ],
        catalog,
        carriers={},
    )
    assert reach.cells == 3
    assert reach.absent_total == 1
    assert reach.absent_unreachable == 1
    # Не-``❌`` ячейки достижимы по определению (носитель им не нужен) — они в числителе.
    assert reach.ceiling == 2 / 3


def test_unknown_status_on_unreachable_is_not_a_system_error() -> None:
    """``❓`` там, где ``❌`` недостижим, — исход методологии, а не промах прибора."""
    catalog = {"p": _param("p", AbsenceRuleType.DISTRIBUTION_ABSENCE)}
    golden = [_golden("prod", "p", Status.ABSENT)]
    reach = classify_reachability(golden, catalog, carriers={})
    result = agreement_with_ceiling(
        golden, {("prod", "p"): CellVerdict(status=Status.UNKNOWN)}, reach, catalog
    )
    assert result["not_attributable"] == 1
    assert result["matches"] == 0
    assert result["attestable_denominator"] == 0


def test_presence_claim_on_unreachable_absent_is_still_an_error() -> None:
    """``✅`` там, где эталон говорит об отсутствии, методологией не объясняется."""
    catalog = {"p": _param("p", AbsenceRuleType.DISTRIBUTION_ABSENCE)}
    golden = [_golden("prod", "p", Status.ABSENT)]
    reach = classify_reachability(golden, catalog, carriers={})
    result = agreement_with_ceiling(
        golden, {("prod", "p"): CellVerdict(status=Status.OK)}, reach, catalog
    )
    assert result["claims_presence_where_golden_absent"] == 1
    assert result["not_attributable"] == 0
    assert result["matches"] == 0


def test_agreement_counts_exact_matches_only() -> None:
    """Согласие — точное равенство статусов; ``⚠️`` вместо ``❌`` совпадением не является."""
    catalog = {
        "p": _param("p", AbsenceRuleType.REGISTRY_ABSENCE),
        "q": _param("q", AbsenceRuleType.REGISTRY_ABSENCE),
    }
    golden = [
        _golden("prod", "p", Status.OK),
        _golden("prod", "q", Status.ABSENT),
    ]
    result = agreement_with_ceiling(
        golden,
        {
            ("prod", "p"): CellVerdict(status=Status.OK),
            ("prod", "q"): CellVerdict(status=Status.PARTIAL),
        },
        classify_reachability(golden, catalog, carriers={}),
        catalog,
    )
    assert result["matches"] == 1
    assert result["agreement"] == 0.5


def test_carriers_from_coverage_reads_reason_and_roots() -> None:
    """Причина недостижимости берётся из записи прогона, а не выводится заново."""
    coverage = CorpusCoverage(
        product_id="prod",
        complete=False,
        roots=[_root("https://a/", complete=False)],
        computed_at="2026-11-07T00:00:00+00:00",
        reason="crawl_policy_skipped",
    )
    carriers = carriers_from_coverage([coverage])
    carrier = carriers["prod"]
    assert carrier.complete is False
    assert carrier.skipped_by_policy is True
    assert len(carrier.roots) == 1
    assert carrier.bounded_ok() is False

    # Иная причина — не «не смотрели по решению»: различие обязано сохраняться.
    other = CorpusCoverage(
        product_id="other",
        complete=False,
        computed_at="2026-11-07T00:00:00+00:00",
        reason="no_seeds",
    )
    assert carriers_from_coverage([other])["other"].skipped_by_policy is False
