"""Тесты гейта полноты авторских полей (A3, фаза P3).

Проверяют, что незаполненные обязательные поля выражаются явно (``None``/пусто), а не
правдоподобным текстом-заглушкой: код-эмиттеры кандидатов не выдумывают ``definition``/
``selection_reason``, линтер помечает их уровнем ``incomplete`` (не ``blocking``/
``warning``), а прогон неполного пакета блокируется.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.domain.enums import AnchorRelation
from core.domain.models import CohortEntry, RoleDef
from core.pipeline import package_admin as pa
from core.pipeline import package_discovery as pd
from core.pipeline.lint import check_unfilled_authoring, lint_run_inputs
from core.pipeline.normalize import load_inputs
from core.pipeline.package_io import load_artifacts, prospective_lint

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_PACKAGE = ROOT / "packages" / "_template"


def _run(capsys, argv: list[str]) -> int:
    code = main(argv)
    capsys.readouterr()
    return code


# ---------------------------------------------------------------------------
# Доменные модели выражают «не заполнено» через None
# ---------------------------------------------------------------------------


def test_domain_models_allow_unfilled_authoring_fields() -> None:
    role = RoleDef(role_id="role_x")
    assert role.definition is None
    entry = CohortEntry(
        product_id="p1", role_overlap=1.0, anchor_relation=AnchorRelation.ANCHOR
    )
    assert entry.selection_reason is None


# ---------------------------------------------------------------------------
# Линтер: incomplete — отдельный уровень, не blocking/warning
# ---------------------------------------------------------------------------


def test_unfilled_authoring_is_incomplete_not_blocking() -> None:
    template_inputs = load_inputs(TEMPLATE_PACKAGE / "run.yaml", TEMPLATE_PACKAGE)
    messages = check_unfilled_authoring(template_inputs)
    assert messages and all(m.startswith("A3:") for m in messages)
    report = lint_run_inputs(template_inputs)
    assert report.blocking == []
    assert report.warnings == []
    assert report.incomplete  # неполнота видна отдельно
    assert report.ok is True  # создание/правка не блокируются
    assert report.complete is False
    assert report.ready is False  # прогон запрещён


def test_template_package_is_incomplete_but_lint_clean() -> None:
    artifacts = load_artifacts(TEMPLATE_PACKAGE)
    report = prospective_lint(artifacts)
    assert report.blocking == []
    assert report.warnings == []
    assert report.incomplete


def test_filled_authoring_passes() -> None:
    template_inputs = load_inputs(TEMPLATE_PACKAGE / "run.yaml", TEMPLATE_PACKAGE)
    roles = [
        role.model_copy(update={"definition": f"роль {role.role_id}"})
        for role in template_inputs.package.roles
    ]
    entries = [
        entry.model_copy(update={"selection_reason": f"обоснование {entry.product_id}"})
        for entry in template_inputs.cohort.entries
    ]
    filled = dataclasses.replace(
        template_inputs,
        package=template_inputs.package.model_copy(update={"roles": roles}),
        cohort=template_inputs.cohort.model_copy(update={"entries": entries}),
    )
    report = lint_run_inputs(filled)
    assert report.incomplete == []
    assert report.ready is True


# ---------------------------------------------------------------------------
# Код-эмиттеры кандидатов не выдумывают поля
# ---------------------------------------------------------------------------


def test_candidate_suggestions_do_not_fabricate_fields() -> None:
    roles = pd.suggest_roles({"roles": [{"role_id": "role_new"}]})
    assert roles and "definition" not in roles[0].payload
    cohort = pd.suggest_cohort_members({"cohort_members": [{"product_id": "prod_new"}]})
    assert cohort and "selection_reason" not in cohort[0].payload
    for candidate in [*roles, *cohort]:
        assert "TODO" not in str(candidate.payload)


def test_candidate_suggestions_keep_provided_fields() -> None:
    roles = pd.suggest_roles(
        {"roles": [{"role_id": "role_new", "definition": "описание из профиля"}]}
    )
    assert roles[0].payload["definition"] == "описание из профиля"
    cohort = pd.suggest_cohort_members(
        {"cohort_members": [{"product_id": "prod_new", "selection_reason": "из профиля"}]}
    )
    assert cohort[0].payload["selection_reason"] == "из профиля"


# ---------------------------------------------------------------------------
# CLI: неполный пакет не допускается к плану
# ---------------------------------------------------------------------------


def test_cli_plan_blocked_on_incomplete_package(capsys, tmp_path: Path) -> None:
    pa.create_package("case_p3", "B", dir=tmp_path)
    package = tmp_path / "case_p3"
    code = _run(
        capsys,
        ["plan", "--run-yaml", str(package / "run.yaml"), "--package", str(package)],
    )
    assert code == EXIT_USAGE  # LintFailure: неполнота не проходит молча


def test_cli_plan_ok_after_authoring_filled(capsys, tmp_path: Path) -> None:
    pa.create_package("case_p3_ok", "B", dir=tmp_path)
    package = tmp_path / "case_p3_ok"
    for role_id in ("role_primary", "role_secondary"):
        pa.update_role(package, role_id, definition=f"роль {role_id}")
    artifacts = load_artifacts(package)
    assert artifacts.cohort is not None
    entries = [
        entry.model_copy(update={"selection_reason": f"обоснование {entry.product_id}"})
        for entry in artifacts.cohort.entries
    ]
    pa.set_cohort(package, entries, actor="t")
    assert _run(
        capsys,
        ["plan", "--run-yaml", str(package / "run.yaml"), "--package", str(package)],
    ) == EXIT_OK
