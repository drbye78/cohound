"""План 27 §13: предложение строк политики наследования из наблюдений.

Держится ровно то, ради чего модуль существует:

* ``min_version`` — **первый объявленный** мажор с подписанным фактом, а не алфавитный;
* ``conditional`` выводится только из карты **той же** версии (цитата не о предмете);
* прибор не подписывает за человека: ``pending`` публикуется, но не предлагается;
* ``--apply`` пишет только по заполненному ``decision`` и только ``accept``;
* строки другой линии и незаполненная разметка — отказ, а не догадка.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.domain.enums import FactReview, FactStatus, Importance, ParamType
from core.domain.models import BaselineFact, BaselineRow, CatalogRow
from core.pipeline import package_admin as pa
from core.pipeline.reference_products import load_package_facts, write_facts
from core.pipeline.reference_propose import (
    CANDIDATE_COLUMNS,
    REASON_EXISTING,
    REASON_FACT_PRESENT_ACCEPTED,
    REASON_MAP_SUPPORT_UNRELATED,
    REASON_NEEDS_SIGNATURE,
    REASON_NO_EVIDENCE,
    SUPPORT_NOT_ABOUT_SUBJECT,
    apply_candidates,
    candidates_path,
    load_candidates,
    propose_baselines,
    write_candidates,
)

REFERENCE_YAML = """\
schema: reference_products/1
products:
  - baseline_id: upstream-pg
    product_id: postgresql
    canonical_name: PostgreSQL
    versions:
      - version: "18"
        docs_roots: ["https://d/18/"]
      - version: "17"
        docs_roots: ["https://d/17/"]
      - version: "9"
        docs_roots: ["https://d/9/"]
    current_version: "18"
"""

CATALOG_CSV = (
    "param_id,title,module,group,importance,param_type,rule,absence_rule,"
    "role_binding,concept_terms,search_langs\n"
    'p1,JSONB indexing,module_core,core_capabilities,Кр,fact,"supports jsonb storage",'
    "explicit,role_primary,,ru|en\n"
    'p2,Operators,module_core,core_capabilities,Вж,fact,"supports operators",'
    "explicit,role_primary,,ru|en\n"
)


def _package(tmp_path: Path) -> Path:
    """Валидный пакет: ``package.yaml`` нужен писателю ``add_baseline``, а не тесту."""
    pa.create_package("case_x", "B", dir=tmp_path)
    package = tmp_path / "case_x"
    (package / "sources").mkdir(exist_ok=True)
    (package / "sources" / "reference_products.yaml").write_text(
        REFERENCE_YAML, encoding="utf-8"
    )
    (package / "catalog.csv").write_text(CATALOG_CSV, encoding="utf-8")
    return package


def _param(param_id: str) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=f"Title {param_id}",
        module="module_core",
        group="core_capabilities",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule="rule text",
    )


def _fact(
    param_id: str = "p1",
    *,
    version: str = "18",
    status: FactStatus = FactStatus.PRESENT,
    review: FactReview = FactReview.ACCEPTED,
) -> BaselineFact:
    return BaselineFact(
        baseline_id="upstream-pg",
        param_id=param_id,
        version=version,
        status=status,
        url="https://d/doc" if status is FactStatus.PRESENT else None,
        quote="upstream documents it" if status is FactStatus.PRESENT else None,
        content_hash="a" * 64 if status is FactStatus.PRESENT else None,
        observed_at="2026-11-19T00:00:00+00:00",
        corpus_build_id="ref_postgresql_18_deadbeef",
        review=review,
        reviewed_by="roger" if review is not FactReview.PENDING else None,
    )


def _row(proposal: dict, param_id: str) -> dict:
    return next(row for row in proposal["rows"] if row["param_id"] == param_id)


def _propose(
    facts: list[BaselineFact],
    *,
    versions: tuple[str, ...] = ("18", "17", "9"),
    baselines: list[BaselineRow] | None = None,
    map_cells: list[dict] | None = None,
    map_version: str | None = None,
) -> dict:
    return propose_baselines(
        baseline_id="upstream-pg",
        facts=facts,
        versions=list(versions),
        catalog=[_param("p1"), _param("p2")],
        baselines=baselines or [],
        map_cells=map_cells,
        map_version=map_version,
    )


# ---------------------------------------------------------------------------
# Правило: direct / conditional
# ---------------------------------------------------------------------------


def test_min_version_is_the_first_declared_major_not_the_alphabet() -> None:
    """``18`` объявлена раньше ``9`` — побеждает декларация, а не сортировка строк."""
    proposal = _propose([_fact("p1", version="9"), _fact("p1", version="18")])
    row = _row(proposal, "p1")
    assert row["min_version"] == "18"
    assert row["inheritance_mode"] == "direct"
    assert row["reason"] == REASON_FACT_PRESENT_ACCEPTED
    assert row["decision"] == ""
    assert row["evidence_versions"] == "18|9"


def test_fact_written_with_a_minor_version_matches_the_declared_major() -> None:
    """Факт ``18.4.1`` подтверждает объявленную ``18`` (сравнение по мажору)."""
    row = _row(_propose([_fact("p1", version="18.4.1")]), "p1")
    assert row["min_version"] == "18"
    assert row["inheritance_mode"] == "direct"


def test_map_support_not_about_subject_makes_the_row_conditional() -> None:
    cells = [{"param_id": "p1", "review_reason": f"{SUPPORT_NOT_ABOUT_SUBJECT}: quote about backups"}]
    row = _row(_propose([_fact("p1")], map_cells=cells, map_version="18"), "p1")
    assert row["inheritance_mode"] == "conditional"
    assert row["reason"] == REASON_MAP_SUPPORT_UNRELATED
    assert row["map_review_reason"].startswith(SUPPORT_NOT_ABOUT_SUBJECT)


def test_map_of_another_version_does_not_downgrade_the_row() -> None:
    """Карта ``17`` не судит факт ``18``: измерение одной версии не переносится."""
    cells = [{"param_id": "p1", "review_reason": SUPPORT_NOT_ABOUT_SUBJECT}]
    proposal = _propose([_fact("p1", version="18")], map_cells=cells, map_version="17")
    row = _row(proposal, "p1")
    assert row["inheritance_mode"] == "direct"
    assert row["reason"] == REASON_FACT_PRESENT_ACCEPTED
    assert proposal["map_applied"] is True
    assert proposal["map_version"] == "17"


def test_map_without_a_version_is_not_applied() -> None:
    """Карта без названной версии не применяется — и это видно (``map_applied``)."""
    cells = [{"param_id": "p1", "review_reason": SUPPORT_NOT_ABOUT_SUBJECT}]
    proposal = _propose([_fact("p1")], map_cells=cells, map_version=None)
    assert proposal["map_applied"] is False
    assert _row(proposal, "p1")["inheritance_mode"] == "direct"


def test_map_without_the_reason_keeps_the_row_direct() -> None:
    cells = [{"param_id": "p1", "review_reason": None}]
    row = _row(_propose([_fact("p1")], map_cells=cells, map_version="18"), "p1")
    assert row["inheritance_mode"] == "direct"


# ---------------------------------------------------------------------------
# Что не предлагается
# ---------------------------------------------------------------------------


def test_pending_fact_needs_a_signature_and_is_not_proposed() -> None:
    proposal = _propose([_fact("p1", review=FactReview.PENDING)])
    row = _row(proposal, "p1")
    assert row["reason"] == REASON_NEEDS_SIGNATURE
    assert row["decision"] == "skip"
    assert row["min_version"] == ""
    assert proposal["needs_signature"] == ["p1"]
    assert proposal["proposed"] == []


def test_unclear_and_absent_give_no_evidence() -> None:
    proposal = _propose(
        [
            _fact("p1", status=FactStatus.UNCLEAR, review=FactReview.PENDING),
            _fact("p2", status=FactStatus.ABSENT, review=FactReview.PENDING),
        ]
    )
    assert proposal["no_evidence"] == ["p1", "p2"]
    assert proposal["proposed"] == []
    assert _row(proposal, "p1")["decision"] == "skip"
    assert _row(proposal, "p2")["reason"] == REASON_NO_EVIDENCE


def test_parameter_without_any_fact_gives_no_evidence() -> None:
    proposal = _propose([_fact("p1")])
    assert proposal["no_evidence"] == ["p2"]
    assert _row(proposal, "p2")["evidence_versions"] == ""


def test_existing_row_is_marked_and_not_reproposed() -> None:
    existing = [
        BaselineRow(
            baseline_id="upstream-pg",
            param_id="p1",
            min_version="17",
            inheritance_mode="conditional",
        )
    ]
    proposal = _propose([_fact("p1")], baselines=existing)
    row = _row(proposal, "p1")
    assert row["decision"] == "existing"
    assert row["reason"] == REASON_EXISTING
    # Показано, что стоит в файле, а не то, что предлагалось бы.
    assert (row["min_version"], row["inheritance_mode"]) == ("17", "conditional")
    assert proposal["proposed"] == []
    assert proposal["existing"] == ["p1"]


def test_proposal_is_deterministic() -> None:
    facts = [_fact("p1"), _fact("p2", version="17")]
    assert _propose(facts)["rows"] == _propose(facts)["rows"]


def test_support_token_matches_evaluator() -> None:
    """Токен ячейки пишет оценщик; модуль предложения его только читает."""
    from core.pipeline.evaluate import SUPPORT_NOT_ABOUT_SUBJECT as evaluator_token

    assert SUPPORT_NOT_ABOUT_SUBJECT == evaluator_token


# ---------------------------------------------------------------------------
# Артефакт предложения
# ---------------------------------------------------------------------------


def test_candidate_file_has_canonical_columns_and_empty_decision(tmp_path: Path) -> None:
    package = _package(tmp_path)
    proposal = _propose([_fact("p1")])
    path = write_candidates(package, "upstream-pg", proposal["rows"])
    assert path == candidates_path(package, "upstream-pg")
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == ",".join(CANDIDATE_COLUMNS)
    restored = load_candidates(path)
    # Порядок строк — порядок каталога, а не алфавит slug'ов.
    assert [row["param_id"] for row in restored] == ["p1", "p2"]
    assert restored[0]["decision"] == ""
    assert restored[0]["min_version"] == "18"
    assert restored[1]["decision"] == "skip"


def test_load_candidates_refuses_a_foreign_header(tmp_path: Path) -> None:
    path = tmp_path / "foreign.csv"
    path.write_text("param_id,decision\np1,accept\n", encoding="utf-8")
    with pytest.raises(ValueError, match="заголовок"):
        load_candidates(path)


def test_load_candidates_names_the_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="hound baseline propose"):
        load_candidates(candidates_path(tmp_path / "pkg", "upstream-pg"))


# ---------------------------------------------------------------------------
# --apply
# ---------------------------------------------------------------------------


def _candidate_file(package: Path, decisions: dict[str, str]) -> Path:
    """Артефакт с заполненными решениями (``{param_id: decision}``)."""
    proposal = _propose([_fact("p1"), _fact("p2", version="17")])
    rows = []
    for row in proposal["rows"]:
        rows.append({**row, "decision": decisions.get(row["param_id"], "skip")})
    return write_candidates(package, "upstream-pg", rows)


def test_apply_refuses_unfilled_decision_and_names_the_file(tmp_path: Path) -> None:
    package = _package(tmp_path)
    write_candidates(package, "upstream-pg", _propose([_fact("p1"), _fact("p2")])["rows"])
    with pytest.raises(ValueError) as exc:
        apply_candidates(package, baseline_id="upstream-pg", actor="roger")
    message = str(exc.value)
    assert "p1" in message and "p2" in message
    assert str(candidates_path(package, "upstream-pg")) in message
    assert not (package / "baselines.csv").read_text(encoding="utf-8").count("upstream-pg")


def test_apply_writes_only_accepted_rows(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _candidate_file(package, {"p1": "accept", "p2": "reject"})
    payload = apply_candidates(package, baseline_id="upstream-pg", actor="roger")
    assert [item["param_id"] for item in payload["added"]] == ["p1"]
    rows = (package / "baselines.csv").read_text(encoding="utf-8").splitlines()
    assert rows[1:] == ["upstream-pg,p1,18,direct,"]
    log = (package / ".authoring" / "apply_log.jsonl").read_text(encoding="utf-8")
    assert json.loads(log.splitlines()[-1])["actor"] == "roger"
    assert payload["counts"] == {"rows": 2, "accepted": 1, "added": 1, "skipped": 1}


def test_apply_skips_rows_marked_existing(tmp_path: Path) -> None:
    """``existing`` не трогается: строка уже в файле, политику не переписываем."""
    package = _package(tmp_path)
    pa.add_baseline(package, "upstream-pg", "p1", min_version="17", actor="roger")
    before = (package / "baselines.csv").read_text(encoding="utf-8")
    proposal = _propose(
        [_fact("p1")],
        baselines=[
            BaselineRow(
                baseline_id="upstream-pg", param_id="p1", min_version="17",
                inheritance_mode="direct",
            )
        ],
    )
    write_candidates(
        package, "upstream-pg",
        [{**row, "decision": "skip"} for row in proposal["rows"]],
    )
    payload = apply_candidates(package, baseline_id="upstream-pg", actor="roger")
    assert payload["added"] == []
    assert (package / "baselines.csv").read_text(encoding="utf-8") == before


def test_apply_dry_run_writes_nothing(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _candidate_file(package, {"p1": "accept", "p2": "accept"})
    log_path = package / ".authoring" / "apply_log.jsonl"
    log_before = log_path.read_text(encoding="utf-8")
    payload = apply_candidates(
        package, baseline_id="upstream-pg", actor="roger", dry_run=True
    )
    assert [item["param_id"] for item in payload["added"]] == ["p1", "p2"]
    assert (package / "baselines.csv").read_text(encoding="utf-8").splitlines() == [
        "baseline_id,param_id,min_version,inheritance_mode,note"
    ]
    # Журнал не пополнился: dry-run не оставляет следа правки.
    assert log_path.read_text(encoding="utf-8") == log_before


def test_apply_refuses_accept_without_min_version(tmp_path: Path) -> None:
    """Неверный пол молча пере- или недо-наследует: пустой ``min_version`` — отказ."""
    package = _package(tmp_path)
    proposal = _propose([_fact("p1")])
    rows = [
        {**row, "decision": "accept", "min_version": ""}
        if row["param_id"] == "p1"
        else {**row, "decision": "skip"}
        for row in proposal["rows"]
    ]
    write_candidates(package, "upstream-pg", rows)
    with pytest.raises(ValueError, match="min_version"):
        apply_candidates(package, baseline_id="upstream-pg", actor="roger")


def test_apply_refuses_rows_of_another_line(tmp_path: Path) -> None:
    package = _package(tmp_path)
    proposal = _propose([_fact("p1")])
    rows = [{**row, "decision": "skip"} for row in proposal["rows"]]
    rows[0] = {**rows[0], "baseline_id": "upstream-other", "decision": "accept"}
    write_candidates(package, "upstream-pg", rows)
    with pytest.raises(ValueError, match="другой линии"):
        apply_candidates(package, baseline_id="upstream-pg", actor="roger")


def test_apply_requires_actor(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _candidate_file(package, {"p1": "accept"})
    with pytest.raises(ValueError, match="actor"):
        apply_candidates(package, baseline_id="upstream-pg", actor="  ")


def test_facts_written_to_the_package_feed_the_proposal(tmp_path: Path) -> None:
    """Сквозная связка: ``write_facts`` → ``propose_baselines`` (без ручной сборки)."""
    package = _package(tmp_path)
    write_facts(package, [_fact("p1")])
    proposal = propose_baselines(
        baseline_id="upstream-pg",
        facts=load_package_facts(package),
        versions=["18", "17", "9"],
        catalog=[_param("p1"), _param("p2")],
    )
    assert proposal["proposed"] == ["p1"]
    assert proposal["counts"]["no_evidence"] == 1
