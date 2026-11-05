"""Тесты производителей evidence-механизмов (§5): протокол исчерпания и аттестация охвата.

До этих механизмов у обоих была только «принимающая» сторона:

* **I3** — ``ProtocolExhaustion``/таблица/``save_exhaustion`` существовали, но ни одна фаза
  не писала строку, поэтому каждый strict-``❓`` отчёта попадал в аномалии как «нет
  протокола» (``collect_i3_anomalies(cells, None)``);
* **I2b/I2c** — ``evaluate_phase`` принимал ``scope_attested_by``, но ни один вызывающий его
  не передавал, и ``❌`` при неполном корпусе был недостижим в принципе.

Проверяется главное свойство обоих: **протокол называет только тот контур, который реально
исполнился**, а подпись аттестации — per product, потому что покрытие per product. Неполный
протокол остаётся аномалией (``❓`` не меняет статус, I12) — это наблюдение, а не дефект,
который следует заглушить.

Фикстуры категорийно нейтральны (§1.3); сеть не используется.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
import yaml

from core.domain import (
    REQUIRED_EXHAUSTION_CONTOURS,
    Cell,
    CellState,
    CorpusCoverage,
    DenialStrength,
    EvidenceScope,
    LLMCellOutput,
    ScopeAttestation,
    Status,
    TerminationReason,
)
from core.domain.enums import AbsenceRuleType
from core.domain.models import CorpusRootCoverage
from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS
from core.pipeline.evaluate import evaluate_phase
from core.pipeline.exhaustion import (
    build_protocols,
    contours_run,
    protocol_map,
)
from core.pipeline.normalize import load_scope_attestations
from core.pipeline.package_admin import (
    PackageAdminError,
    attest_scope,
    get_scope_attestations,
)
from core.pipeline.phases import _observed_exhaustion_facts
from core.pipeline.report import collect_i3_anomalies
from core.pipeline.runner import Ports, RunContext
from tests.fakes.ports import FakeEvaluator
from tests.helpers.packages import valid_package
from tests.pipeline.test_phases import (
    RUN_ID,
    _chunks_index,
    _inputs,
    _plan,
    _store,
)

FIXED_TS = "2026-01-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# I3: протокол исчерпания — только наблюдённые контуры
# ---------------------------------------------------------------------------


def _unknown_cell(product_id: str = "p1", param_id: str = "param_a") -> Cell:
    """``❓`` нового образца: несёт причину незнания — это и есть его обоснование (I3)."""
    return Cell(
        run_id=RUN_ID,
        product_id=product_id,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.UNKNOWN,
        evidence_scope=EvidenceScope(
            denial_strength=DenialStrength.NOT_STATED,
            reason="материал релевантен, дословного утверждения нет",
        ),
    )


def test_contours_run_names_only_observed_contours() -> None:
    """Контур попадает в протокол только если передан как наблюдённый факт."""
    facts = contours_run(["p1", "p2"], corpus=["p1"], registry=["p2"])
    assert facts["p1"] == frozenset({"corpus"})
    assert facts["p2"] == frozenset({"registry"})


def test_contours_run_ignores_products_outside_plan() -> None:
    """Протокол существует только для продуктов плана: посторонний факт отбрасывается."""
    facts = contours_run(["p1"], corpus=["p1", "ghost"])
    assert set(facts) == {"p1"}


def test_build_protocols_covers_only_unknown_cells() -> None:
    """Строка протокола — только для ``❓``; ✅ не получает протокола исчерпания."""
    ok = _unknown_cell().model_copy(update={"status": Status.OK})
    rows = build_protocols(
        [ok, _unknown_cell()],
        run_id=RUN_ID,
        contours_run=contours_run(["p1"], corpus=["p1"]),
    )
    assert [(row.product_id, row.param_id) for row in rows] == [("p1", "param_a")]
    assert rows[0].contours == ["corpus"]


def test_unknown_without_reason_is_reported_as_anomaly() -> None:
    """Аномалия I3 — ``❓``, не называющий причину незнания (Вариант 2)."""
    # Объект есть, но причины нет: домен такую ячейку не соберёт, а из store она
    # прийти может (повреждённая/частичная запись) — отчёт обязан назвать это аномалией.
    cell = _unknown_cell().model_copy(
        update={
            "evidence_scope": EvidenceScope.model_construct(
                denial_strength=DenialStrength.NOT_STATED, reason=None
            )
        }
    )
    rows = build_protocols(
        [cell], run_id=RUN_ID, contours_run=contours_run(["p1"], corpus=["p1"])
    )
    anomalies = collect_i3_anomalies([cell], protocol_map(rows))
    assert len(anomalies) == 1
    reason = anomalies[0]["reason"]
    assert "reason" in reason
    # Прежнее требование перечня контуров снято: протокол — провенанс поиска.
    assert "контур" not in reason


def test_partial_protocol_is_not_an_anomaly() -> None:
    """Неполный протокол не делает ``❓`` аномальным: причина живёт в ячейке.

    Раньше здесь требовались все четыре контура, и параметр, который нельзя доказать
    поиском, не мог получить честное ``❓`` без SERP-контура. Теперь ``❓`` обоснован
    классом ``not_stated`` + причиной, а протокол остаётся записью о том, что исполнялось.
    """
    cell = _unknown_cell()
    rows = build_protocols(
        [cell], run_id=RUN_ID, contours_run=contours_run(["p1"], corpus=["p1"])
    )
    assert rows[0].contours == ["corpus"]
    assert collect_i3_anomalies([cell], protocol_map(rows)) == []


def test_full_protocol_produces_no_anomaly() -> None:
    """Все четыре контура пройдены ⇒ ``❓`` обоснован, аномалии нет."""
    cell = _unknown_cell()
    rows = build_protocols(
        [cell],
        run_id=RUN_ID,
        contours_run=contours_run(
            ["p1"], corpus=["p1"], serp=["p1"], registry=["p1"], inheritance=["p1"]
        ),
    )
    assert sorted(rows[0].contours) == sorted(REQUIRED_EXHAUSTION_CONTOURS)
    assert collect_i3_anomalies([cell], protocol_map(rows)) == []


def test_reviewed_unknown_is_not_an_anomaly() -> None:
    """I3 — дизъюнкция: человеческая подпись заменяет полный протокол (I3-OR)."""
    reviewed = _unknown_cell().model_copy(update={"reviewed_by": "expert-1"})
    assert collect_i3_anomalies([reviewed], None) == []


def test_sufficient_evidence_is_rejected_for_unknown() -> None:
    """``sufficient_evidence`` значит «нашли достаточно» — с ``❓`` он противоречив."""
    with pytest.raises(ValueError, match="sufficient_evidence"):
        build_protocols(
            [_unknown_cell()],
            run_id=RUN_ID,
            contours_run=contours_run(["p1"], corpus=["p1"]),
            termination_by_product={"p1": TerminationReason.SUFFICIENT_EVIDENCE},
        )


def test_budget_exhausted_requires_observed_flag() -> None:
    """Бюджет считается исчерпанным только по наблюдённому признаку, не по «остатку 0»."""
    implicit = build_protocols(
        [_unknown_cell()],
        run_id=RUN_ID,
        contours_run=contours_run(["p1"], corpus=["p1"]),
        budget={"p1": {"used": 3}},
    )
    assert implicit[0].termination_reason is not TerminationReason.BUDGET_EXHAUSTED
    explicit = build_protocols(
        [_unknown_cell()],
        run_id=RUN_ID,
        contours_run=contours_run(["p1"], corpus=["p1"]),
        budget={"p1": {"used": 3, "exhausted": True}},
    )
    assert explicit[0].termination_reason is TerminationReason.BUDGET_EXHAUSTED


def test_no_contours_run_is_not_found_and_stays_empty() -> None:
    """Ни один контур не исполнился ⇒ ``not_found`` и пустой ``contours`` — честная запись."""
    rows = build_protocols(
        [_unknown_cell()], run_id=RUN_ID, contours_run=contours_run(["p1"])
    )
    assert rows[0].contours == []
    assert rows[0].termination_reason is TerminationReason.NOT_FOUND


def test_protocols_are_deterministically_ordered() -> None:
    """Порядок строк и контуров детерминирован (R7): повтор даёт те же байты."""
    cells = [_unknown_cell("p2"), _unknown_cell("p1"), _unknown_cell("p1", "param_b")]
    facts = contours_run(["p1", "p2"], corpus=["p2", "p1"], registry=["p1"])
    first = build_protocols(cells, run_id=RUN_ID, contours_run=facts)
    second = build_protocols(
        list(reversed(cells)), run_id=RUN_ID, contours_run=facts
    )
    assert [row.model_dump() for row in first] == [row.model_dump() for row in second]
    assert [(row.product_id, row.param_id) for row in first] == [
        ("p1", "param_a"),
        ("p1", "param_b"),
        ("p2", "param_a"),
    ]


# ---------------------------------------------------------------------------
# I3: сквозная запись из фазы оценки и чтение отчётом
# ---------------------------------------------------------------------------


def _coverage(product_id: str, *, complete: bool = True) -> CorpusCoverage:
    return CorpusCoverage(
        product_id=product_id,
        complete=complete,
        expected_sources=1,
        observed_sources=1,
        computed_at=FIXED_TS,
    )


def test_evaluate_persists_protocol_for_unknown_cell() -> None:
    """``❓`` от оценщика оставляет durable-строку ``search_exhaustion`` (было 0 строк)."""
    store = _store()
    store.save_corpus_coverage(RUN_ID, _coverage("p1"))
    from core.pipeline.exhaustion import ExhaustionFacts

    result = evaluate_phase(
        plan=_plan(products=("p1",)),
        ports=Ports(
            evaluator=FakeEvaluator(
                default=LLMCellOutput(
                    status=Status.UNKNOWN, justification="Недостаточно данных."
                )
            ),
            lexical_index=_chunks_index(),
        ),
        store=store,
        run_id=RUN_ID,
        profile="R0",
        cutoff_k=3,
        exhaustion_facts=ExhaustionFacts(
            contours_run=contours_run(["p1"], corpus=["p1"]),
            yielded_by_product={"p1": True},
        ),
    )
    assert result.status == "ok"
    rows = list(store.iter_exhaustions(RUN_ID))
    assert [(row.product_id, row.param_id) for row in rows] == [("p1", "param_a")]
    # Контур корпуса наблюдён; SERP-контура в прогоне нет — он честно отсутствует.
    assert rows[0].contours == ["corpus"]
    assert rows[0].termination_reason is TerminationReason.RESULTS_EXHAUSTED


def test_evaluate_without_facts_writes_no_protocol() -> None:
    """Без переданных фактов Ш6 не догадывается: строк не появляется (прежнее поведение)."""
    store = _store()
    store.save_corpus_coverage(RUN_ID, _coverage("p1"))
    evaluate_phase(
        plan=_plan(products=("p1",)),
        ports=Ports(
            evaluator=FakeEvaluator(
                default=LLMCellOutput(
                    status=Status.UNKNOWN, justification="Недостаточно данных."
                )
            ),
            lexical_index=_chunks_index(),
        ),
        store=store,
        run_id=RUN_ID,
        profile="R0",
        cutoff_k=3,
    )
    assert list(store.iter_exhaustions(RUN_ID)) == []


def test_coverage_row_without_observation_is_not_a_ran_contour() -> None:
    """Строка покрытия без наблюдений (``observed_sources=0``) не доказывает корпус.

    ``coverage_missing``/``no_seeds`` пишутся и для продукта, который не краулили вовсе.
    Засчитать по ним контур ``corpus`` значило бы выдать отсутствие работы за исчерпание —
    ровно то, что модуль обязуется не делать.
    """
    store = _store()
    store.save_corpus_coverage(
        RUN_ID,
        CorpusCoverage(
            product_id="p1",
            complete=False,
            expected_sources=0,
            observed_sources=0,
            computed_at=FIXED_TS,
            reason="coverage_missing",
        ),
    )
    ctx = RunContext(run_id=RUN_ID, store=store, ports=Ports())
    facts = _observed_exhaustion_facts(ctx, _plan(products=("p1",)), _inputs(), {})
    assert facts.contours_run["p1"] == frozenset()
    rows = build_protocols(
        [_unknown_cell()], run_id=RUN_ID, contours_run=facts.contours_run
    )
    assert rows[0].contours == []
    assert rows[0].termination_reason is TerminationReason.NOT_FOUND


def test_observed_coverage_row_is_a_ran_corpus_contour() -> None:
    """А вот наблюдённый корпус (``observed_sources > 0``) — состоявшийся контур."""
    store = _store()
    store.save_corpus_coverage(RUN_ID, _coverage("p1"))
    ctx = RunContext(run_id=RUN_ID, store=store, ports=Ports())
    facts = _observed_exhaustion_facts(ctx, _plan(products=("p1",)), _inputs(), {})
    assert facts.contours_run["p1"] == frozenset({"corpus"})


# ---------------------------------------------------------------------------
# I2b/I2c: аттестация охвата per product
# ---------------------------------------------------------------------------


def _write_attestations(package: Path, entries: list[dict]) -> None:
    path = package / "scope_attestation.yaml"
    path.write_text(
        yaml.safe_dump({"attestations": entries}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def test_load_scope_attestations_missing_file_is_empty(tmp_path: Path) -> None:
    """Отсутствие файла — не ошибка: механизм опционален."""
    assert load_scope_attestations(valid_package(tmp_path, scope_attestation=False)) == {}


def test_load_scope_attestations_procedure_only_is_empty(tmp_path: Path) -> None:
    """Файл с одной процедурой (исторический формат) читается как «подписей нет»."""
    assert load_scope_attestations(valid_package(tmp_path)) == {}


def test_load_scope_attestations_reads_entries(tmp_path: Path) -> None:
    package = valid_package(tmp_path)
    _write_attestations(
        package,
        [{"product_id": "p1", "attested_by": "expert-1", "snapshot_date": "2026-01-01"}],
    )
    loaded = load_scope_attestations(package)
    assert set(loaded) == {"p1"}
    assert isinstance(loaded["p1"], ScopeAttestation)
    assert loaded["p1"].attested_by == "expert-1"


def test_load_scope_attestations_rejects_duplicate_product(tmp_path: Path) -> None:
    package = valid_package(tmp_path)
    _write_attestations(
        package,
        [
            {"product_id": "p1", "attested_by": "expert-1"},
            {"product_id": "p1", "attested_by": "expert-2"},
        ],
    )
    with pytest.raises(ValueError, match="дубликат"):
        load_scope_attestations(package)


def test_load_scope_attestations_rejects_unknown_product(tmp_path: Path) -> None:
    """Подпись продукта вне когорты — ошибка: аттестация не может быть «ни про кого»."""
    package = valid_package(tmp_path)
    _write_attestations(package, [{"product_id": "ghost", "attested_by": "expert-1"}])
    with pytest.raises(ValueError, match="неизвестный product_id"):
        load_scope_attestations(package)


def test_load_scope_attestations_rejects_empty_attested_by(tmp_path: Path) -> None:
    """Аттестация человеческая: пустой подписант запрещён."""
    package = valid_package(tmp_path)
    _write_attestations(package, [{"product_id": "p1", "attested_by": ""}])
    with pytest.raises(ValueError, match="attested_by"):
        load_scope_attestations(package)


def test_load_scope_attestations_rejects_non_list(tmp_path: Path) -> None:
    package = valid_package(tmp_path)
    (package / "scope_attestation.yaml").write_text(
        "attestations: {product_id: p1}\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="списком"):
        load_scope_attestations(package)


def test_attest_scope_writes_entry_idempotently(tmp_path: Path) -> None:
    """Подпись пишется детерминированно; повтор с теми же полями не меняет файл."""
    package = valid_package(tmp_path)
    attest_scope(
        package,
        "p1",
        actor="expert-1",
        notes="сверено",
        snapshot_date="2026-01-01",
        attested_at=FIXED_TS,
    )
    first = (package / "scope_attestation.yaml").read_bytes()
    loaded = get_scope_attestations(package)
    assert loaded["p1"].attested_by == "expert-1"
    assert loaded["p1"].attested_at == FIXED_TS

    attest_scope(
        package,
        "p1",
        actor="expert-1",
        notes="сверено",
        snapshot_date="2026-01-01",
    )
    assert (package / "scope_attestation.yaml").read_bytes() == first


def test_attest_scope_preserves_procedure(tmp_path: Path) -> None:
    """Добавление подписи не теряет процедуру аттестации (исторический ключ)."""
    package = valid_package(tmp_path)
    before = yaml.safe_load((package / "scope_attestation.yaml").read_text(encoding="utf-8"))
    attest_scope(package, "p1", actor="expert-1", attested_at=FIXED_TS)
    after = yaml.safe_load((package / "scope_attestation.yaml").read_text(encoding="utf-8"))
    assert after["procedure"] == before["procedure"]
    assert [item["product_id"] for item in after["attestations"]] == ["p1"]


def test_attest_scope_rejects_unknown_product_and_empty_actor(tmp_path: Path) -> None:
    package = valid_package(tmp_path)
    with pytest.raises(PackageAdminError):
        attest_scope(package, "ghost", actor="expert-1")
    with pytest.raises(PackageAdminError):
        attest_scope(package, "p1", actor="   ")


def test_scope_attestations_do_not_change_run_identity() -> None:
    """Подпись авторизует ``❌`` из тех же измерений — это не условие измерения (§13).

    Тот же класс, что ``reviewed_by`` и политика гейтов: смена подписанта не должна
    давать новый ``run_id`` и рвать межцикловый diff.
    """
    assert not [field for field in EXECUTION_CONTRACT_FIELDS if "scope" in field]
    assert not [field for field in EXECUTION_CONTRACT_FIELDS if "attest" in field]


def _absent_evaluator() -> FakeEvaluator:
    return FakeEvaluator(
        default=LLMCellOutput(status=Status.ABSENT, justification="Не найдено.")
    )


def _distribution_plan(products: tuple[str, ...] = ("p1",)):  # noqa: ANN202
    """План с конечным справочником: только для таких правил достижим ``bounded``."""
    plan = _plan(products=products)
    params = [
        row.model_copy(update={"absence_rule": AbsenceRuleType.DISTRIBUTION_ABSENCE})
        for row in plan.params
    ]
    return dataclasses.replace(plan, params=params)


def test_undeduced_absence_becomes_unknown_not_published() -> None:
    """Вариант 2: ``❌`` без объявленного класса не публикуется — остаётся ``❓``.

    Неполный охват при этом не превращает честное «не установлено» в ``pending``-
    кандидата: ячейка остаётся на карте, а причина (класс + текст) видна читателю.
    """
    store = _store()
    store.save_corpus_coverage(RUN_ID, _coverage("p1", complete=False))
    result = evaluate_phase(
        plan=_plan(products=("p1",)),
        ports=Ports(evaluator=_absent_evaluator(), lexical_index=_chunks_index()),
        store=store,
        run_id=RUN_ID,
        profile="R0",
        cutoff_k=3,
    )
    assert result.status == "ok"
    cell = next(c for c in result.cells if c.product_id == "p1")
    assert cell.state is CellState.EVALUATED
    assert cell.status is Status.UNKNOWN
    assert cell.evidence_scope is not None
    assert cell.evidence_scope.denial_strength is DenialStrength.UNOBSERVED
    assert "corpus_incomplete" in (cell.evidence_scope.reason or "")
    assert cell.absence_evidence == []  # протокол отсутствия — только для ❌


def test_scope_acceptance_mapping_is_looked_up_per_product() -> None:
    """Принятие риска для p2 не публикует ``❌`` p1: покрытие per product."""
    store = _store()
    store.save_corpus_coverage(RUN_ID, _coverage("p1", complete=False))
    other = evaluate_phase(
        plan=_distribution_plan(),
        ports=Ports(evaluator=_absent_evaluator(), lexical_index=_chunks_index()),
        store=store,
        run_id=RUN_ID,
        profile="R0",
        cutoff_k=3,
        scope_acceptance={"p2": "expert-2"},
    )
    cell = next(c for c in other.cells if c.product_id == "p1")
    assert cell.state is CellState.EVALUATED
    assert cell.status is Status.UNKNOWN
    assert cell.evidence_scope is not None
    # Подпись чужого продукта не применилась: риска за p1 никто не принимал.
    assert cell.evidence_scope.scope_acceptance is None
    assert "absence_unattested" in (cell.evidence_scope.reason or "")
    assert cell.evidence_scope.denial_strength is DenialStrength.UNOBSERVED


def test_scope_acceptance_mapping_publishes_bounded_absent_for_accepted_product() -> None:
    """Принятый риск делает конечный справочник ``bounded`` ⇒ ``❌``, подпись видна.

    Полнота — по снятию корня (I2d, план 20 §11.2): у ``bounded`` без ``complete``-снятия
    датированного носителя нет, поэтому подпись риска **одна** ``❌`` больше не держит
    (ужесточение §11.4 п.2 закреплено в ``test_absence_dated.py``). Здесь проверяется
    **отображение** подписи в вердикт и протокол, поэтому носитель задан явно.
    """
    store = _store()
    store.save_corpus_coverage(
        RUN_ID,
        CorpusCoverage(
            product_id="p1",
            complete=True,
            roots=[
                CorpusRootCoverage(
                    root_url="https://p1.test/",
                    capture_id="cap-1",
                    complete=True,
                    observed_how="live",
                    observed_at=FIXED_TS,
                )
            ],
            expected_sources=0,
            observed_sources=0,
            computed_at=FIXED_TS,
        ),
    )
    result = evaluate_phase(
        plan=_distribution_plan(),
        ports=Ports(evaluator=_absent_evaluator(), lexical_index=_chunks_index()),
        store=store,
        run_id=RUN_ID,
        profile="R0",
        cutoff_k=3,
        scope_acceptance={"p1": "expert-1"},
    )
    cell = next(c for c in result.cells if c.product_id == "p1")
    assert cell.state is CellState.EVALUATED
    assert cell.status is Status.ABSENT
    assert cell.evidence_scope is not None
    assert cell.evidence_scope.denial_strength is DenialStrength.BOUNDED
    assert cell.evidence_scope.scope_acceptance == "expert-1"
    assert any(
        entry.get("scope_acceptance") == "expert-1"
        for entry in cell.absence_evidence
    )
