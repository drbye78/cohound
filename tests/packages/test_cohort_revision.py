"""Ревизия состава когорты: ``run.yaml: cohort_ref`` + ``frozen_at`` (§2.1 п. 4 док. 09).

Проверяется закрытие дефекта «``add-product`` расширяет когорту, но не пишет ревизию»:
фактическая смена состава двигает ревизию (``cohort-{case_id}-{frozen_at}-r{n}``), поэтому
``cohort_digest`` (а с ним ``run_id``) отличает «когорту расширили, объявив ревизию» от
«в ``cohort.yaml`` дописали строку». Обратная сторона — идемпотентность: повтор с тем же
составом ревизию не двигает, иначе каждый ``--apply`` давал бы новый ``run_id``.

Формула ревизии — чистая функция :func:`core.pipeline.package_admin.next_cohort_ref`
(время внутри не читается: дата приходит аргументом).
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import yaml

from core.domain.enums import OnboardingState
from core.domain.models import Cohort
from core.pipeline.digests import cohort_digest
from core.pipeline.package_add_product import AddProductPlan, apply_add_product
from core.pipeline.package_admin import (
    _patch_run_cohort_ref,
    next_cohort_ref,
    update_cohort,
)
from tests.helpers.packages import valid_package

PRODUCT_ID = "acme_db"
PRODUCT_NAME = "Acme DB"
#: Дата онбординга отличается от ``frozen_at`` фикстуры (2026-06-01) — иначе обновление
#: даты фиксации состава было бы неотличимо от его отсутствия.
ONBOARDING = _dt.date(2026, 7, 15)
EXPECTED_REF = "cohort-case_x-2026-07-15-r2"


def _plan() -> AddProductPlan:
    """Минимальный план добавления продукта (без seed'ов — как в ``pending_seeds``).

    Профили/seeds намеренно пусты: тест про ревизию когорты, а не про онбординг сайта.
    Когорта пишется и при нуле валидных seed'ов (09 §2.2), поэтому путь остаётся живым.
    """
    return AddProductPlan(
        product_name=PRODUCT_NAME,
        product_id=PRODUCT_ID,
        resolved_url="https://acme.test/",
        url_source="explicit",
        cohort_entry={
            "product_id": PRODUCT_ID,
            "role_overlap": 0.5,
            "selection_reason": "аналог (тест)",
            "anchor_relation": "analog",
        },
        selection_reason="аналог (тест)",
        onboarding_state=OnboardingState.PENDING_SEEDS,
        onboarding_missing=["no_seeds"],
        today=ONBOARDING,
    )


def _run_yaml(package: Path) -> dict:
    return yaml.safe_load((package / "run.yaml").read_text(encoding="utf-8"))


def _cohort(package: Path) -> Cohort:
    return Cohort.model_validate(
        yaml.safe_load((package / "cohort.yaml").read_text(encoding="utf-8"))
    )


def _digest(package: Path) -> str:
    """``cohort_digest`` по фактическим файлам пакета (как это делает ``run``)."""
    run = _run_yaml(package)
    return cohort_digest(
        _cohort(package), case_id=run["case_id"], cohort_ref=run.get("cohort_ref")
    )


def _artifact(package: Path) -> dict:
    path = package / "sources" / "add_product" / f"{ONBOARDING.isoformat()}-{PRODUCT_ID}.json"
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# apply_add_product: ревизия пишется
# ---------------------------------------------------------------------------


def test_add_product_apply_writes_cohort_revision(tmp_path: Path) -> None:
    """``--apply``: ``cohort_ref`` с суффиксом ``-r2``, ``frozen_at`` = дата онбординга."""
    package = valid_package(tmp_path)
    assert _run_yaml(package).get("cohort_ref") is None, "исходный набор — без ревизии"
    before_digest = _digest(package)

    applied = apply_add_product(package, _plan(), actor="tester")

    assert _run_yaml(package)["cohort_ref"] == EXPECTED_REF
    assert applied["cohort_ref"] == EXPECTED_REF
    cohort = _cohort(package)
    assert cohort.frozen_at == ONBOARDING, "дата фиксации состава = дата онбординга"
    assert PRODUCT_ID in {entry.product_id for entry in cohort.entries}
    # Провенанс ревизии виден в идентичности прогона: дайджест до ≠ после.
    assert _digest(package) != before_digest


def test_add_product_apply_records_revision_in_review_artifact(tmp_path: Path) -> None:
    """Ревизия видна в review-артефакте, а не только в ``run.yaml``."""
    package = valid_package(tmp_path)
    apply_add_product(package, _plan(), actor="tester")

    artifact = _artifact(package)
    assert artifact["cohort_ref"] == EXPECTED_REF
    assert artifact["apply"][-1]["cohort_ref"] == EXPECTED_REF
    assert artifact["apply"][-1]["cohort"]["changed"] is True


def test_add_product_reapply_keeps_revision_and_run_yaml_bytes(tmp_path: Path) -> None:
    """Повторный ``--apply`` того же продукта — no-op: ревизия и ``run.yaml`` не меняются."""
    package = valid_package(tmp_path)
    apply_add_product(package, _plan(), actor="tester")
    run_bytes = (package / "run.yaml").read_bytes()

    second = apply_add_product(package, _plan(), actor="tester")

    assert second["cohort"]["noop"] is True
    assert second["cohort_ref"] == EXPECTED_REF
    assert _run_yaml(package)["cohort_ref"] == EXPECTED_REF
    assert (package / "run.yaml").read_bytes() == run_bytes


# ---------------------------------------------------------------------------
# update_cohort: ручной путь (``hound package cohort update|set``)
# ---------------------------------------------------------------------------


def _entries(*extra: str) -> list[dict]:
    rows = [
        {
            "product_id": "p1",
            "role_overlap": 1.0,
            "selection_reason": "якорь",
            "anchor_relation": "anchor",
        },
        {
            "product_id": "p2",
            "role_overlap": 0.5,
            "selection_reason": "аналог",
            "anchor_relation": "analog",
        },
    ]
    rows.extend(
        {
            "product_id": product_id,
            "role_overlap": 0.4,
            "selection_reason": "аналог",
            "anchor_relation": "analog",
        }
        for product_id in extra
    )
    return rows


def test_manual_cohort_update_moves_revision_and_keeps_run_comments(tmp_path: Path) -> None:
    """Ручная смена состава двигает ревизию точечно: комментарии ``run.yaml`` целы."""
    package = valid_package(tmp_path)
    result = update_cohort(
        package, {"entries": _entries("p3")}, actor="tester", frozen_at="2026-08-01"
    )

    assert "run" in result.changed
    assert any("§13" in warning for warning in result.warnings)
    text = (package / "run.yaml").read_text(encoding="utf-8")
    lines = text.splitlines()
    anchor_index = lines.index("cohort: cohort_case_x")
    assert lines[anchor_index + 1] == "cohort_ref: cohort-case_x-2026-08-01-r2"
    assert _cohort(package).frozen_at == _dt.date(2026, 8, 1)


def test_update_cohort_same_entries_with_new_frozen_at_keeps_revision(tmp_path: Path) -> None:
    """Смена только ``frozen_at`` — не новая когорта: ревизия не двигается."""
    package = valid_package(tmp_path)
    update_cohort(package, {"entries": _entries("p3")}, actor="tester", frozen_at="2026-08-01")
    revision = _run_yaml(package)["cohort_ref"]
    same_entries = [entry.model_dump(mode="python") for entry in _cohort(package).entries]

    update_cohort(
        package, {"entries": same_entries, "frozen_at": "2030-01-01"}, actor="tester"
    )

    assert _run_yaml(package)["cohort_ref"] == revision
    assert _cohort(package).frozen_at == _dt.date(2030, 1, 1), "дата фиксации применяется"


def test_update_cohort_revision_counts_up(tmp_path: Path) -> None:
    """Вторая смена состава — ``r3``: номер ревизии не сбрасывается."""
    package = valid_package(tmp_path)
    update_cohort(package, {"entries": _entries("p3")}, actor="tester", frozen_at="2026-08-01")
    update_cohort(
        package, {"entries": _entries("p3", "p4")}, actor="tester", frozen_at="2026-08-02"
    )

    assert _run_yaml(package)["cohort_ref"] == "cohort-case_x-2026-08-02-r3"


# ---------------------------------------------------------------------------
# Формула ревизии (чистая функция)
# ---------------------------------------------------------------------------


def test_next_cohort_ref_formula() -> None:
    """Без суффикса → ``r2`` (исходный набор = первая ревизия без суффикса)."""
    assert next_cohort_ref(None, case_id="case_x", frozen_at="2026-07-15") == EXPECTED_REF
    assert next_cohort_ref("", case_id="case_x", frozen_at=ONBOARDING) == EXPECTED_REF
    assert (
        next_cohort_ref("cohort_other", case_id="case_x", frozen_at=ONBOARDING)
        == EXPECTED_REF
    )


def test_next_cohort_ref_increments_existing_suffix() -> None:
    """``-r2`` → ``-r3`` (двузначный номер не сбивает разбор)."""
    assert (
        next_cohort_ref("cohort-case_x-2026-07-15-r2", case_id="case_x", frozen_at="2026-07-15")
        == "cohort-case_x-2026-07-15-r3"
    )
    assert (
        next_cohort_ref("cohort-case_x-2026-07-15-r9", case_id="case_x", frozen_at="2026-07-15")
        == "cohort-case_x-2026-07-15-r10"
    )


def test_next_cohort_ref_is_deterministic() -> None:
    """Формула — функция аргументов: время внутри не читается."""
    first = next_cohort_ref(None, case_id="case_x", frozen_at=ONBOARDING)
    second = next_cohort_ref(None, case_id="case_x", frozen_at=ONBOARDING)
    assert first == second


# ---------------------------------------------------------------------------
# Точечный патч текста run.yaml
# ---------------------------------------------------------------------------


def test_patch_run_cohort_ref_is_surgical_and_idempotent() -> None:
    """Патч меняет только значение ``cohort_ref``: комментарии и соседние строки целы."""
    raw = (
        "# комментарий верхнего уровня\n"
        "case_id: case_x\n"
        "cohort: cohort_case_x   # имя файла когорты\n"
        "cohort_ref: cohort-case_x-2026-06-01-r2  # r2\n"
        "products: [p1]\n"
    )
    patched = _patch_run_cohort_ref(raw, "cohort-case_x-2026-08-01-r3")
    assert patched == raw.replace("2026-06-01-r2", "2026-08-01-r3")
    assert _patch_run_cohort_ref(patched, "cohort-case_x-2026-08-01-r3") == patched
    # ``None`` не снимает ревизию: операция когорты не вправе терять провенанс.
    assert _patch_run_cohort_ref(raw, None) == raw


def test_patch_run_cohort_ref_inserts_after_cohort_line() -> None:
    """Ключа нет → строка вставляется сразу после ``cohort:`` (рядом с именем файла)."""
    raw = "case_id: case_x\ncohort: cohort_case_x\nproducts: [p1]\n"
    patched = _patch_run_cohort_ref(raw, "cohort-case_x-2026-07-15-r2")
    assert patched.splitlines()[2] == "cohort_ref: cohort-case_x-2026-07-15-r2"
