"""Слагаемые ``run_id`` на манифесте: ``cohort_digest``/``seeds_digest`` (09 §12.5/§13).

Прежде это были **единственные** два поля ``EXECUTION_CONTRACT_FIELDS``, которых не было
ни в таблице ``runs``, ни на :class:`RunManifest`. Поэтому при расхождении контракта
``hound diff`` не мог назвать ось и печатал «предел диагностики» вместо причины.

Здесь проверяется главное свойство: значение на манифесте — **проекция того же
объявленного входа**, из которого выведен ``run_id``, а не второй расчёт. Доказательство
строится не сравнением с константой, а обратным ходом: ``run_id``, выведенный **из полей
манифеста**, обязан совпасть с ``run_id`` прогона.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from core.config.seeds import load_seeds
from core.domain import RunManifest, RunMode
from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS, cohort_digest, seeds_digest
from core.pipeline.plan import derive_run_id, plan
from core.pipeline.report import (
    build_manifest,
    declared_identity_digests,
    diff_boundary_violations,
)
from core.pipeline.run_identity import build_execution_contract
from core.store.sqlite_store import SqliteStore

FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
RUN_YAML = FIXTURES / "run.yaml"


def _inputs():
    """Нормализованные входы фикстурного пакета (категорийно-нейтральные, §1.3)."""
    from core.pipeline.normalize import load_inputs

    return load_inputs(RUN_YAML, FIXTURES)


def _identity(inputs, *, urls=None):
    """``(cohort_digest, seeds_digest, contract, run_id)`` — как у composition root."""
    cohort, seeds = declared_identity_digests(
        inputs, package_dir=FIXTURES, urls=urls
    )
    contract = build_execution_contract(inputs, cohort_digest=cohort, seeds_digest=seeds)
    run_id = derive_run_id(
        inputs,
        cycle_id=inputs.run.snapshot_date.isoformat(),
        cohort_digest=cohort,
        seeds_digest=seeds,
        execution_contract_digest=contract,
    )
    return cohort, seeds, contract, run_id


def _manifest(inputs, *, urls=None) -> RunManifest:
    """Манифест прогона на фикстурах (с дайджестами идентичности)."""
    cohort, seeds, contract, run_id = _identity(inputs, urls=urls)
    return build_manifest(
        inputs=inputs,
        plan=plan(inputs, run_id=run_id),
        cohort_digest=cohort,
        seeds_digest=seeds,
        execution_contract_digest=contract,
        cycle_id=inputs.run.snapshot_date.isoformat(),
    )


# --------------------------------------------------------------------------- #
# 1. Манифест = проекция входа, из которого выведен run_id
# --------------------------------------------------------------------------- #


def test_manifest_digests_are_the_run_id_material() -> None:
    """``run_id``, выведенный **из полей манифеста**, совпадает с ``run_id`` прогона.

    Это и есть доказательство честности: манифест несёт ровно те строки, которые вошли
    в материал идентичности. Тест ловит и подмену значения (другой дайджест), и второй
    расчёт «похожим способом» — обратный ход вернул бы иной ``run_id``.
    """
    inputs = _inputs()
    cohort, seeds, contract, run_id = _identity(inputs)
    manifest = _manifest(inputs)

    assert manifest.run_id == run_id
    assert manifest.cohort_digest == cohort
    assert manifest.seeds_digest == seeds
    assert derive_run_id(
        inputs,
        cycle_id=manifest.cycle_id,
        cohort_digest=manifest.cohort_digest,
        seeds_digest=manifest.seeds_digest,
        execution_contract_digest=contract,
    ) == run_id


def test_declared_identity_digests_use_the_same_functions_and_material() -> None:
    """Хелпер манифеста считает **те же** величины, что composition root для ``run_id``.

    Материал когорты — ``inputs.cohort`` + ``case_id``/``cohort_ref`` объявленного
    ``run.yaml``; seed-набор — эффективный (``load_effective_seeds``). Второй формулы нет.
    """
    inputs = _inputs()
    cohort, seeds = declared_identity_digests(inputs, package_dir=FIXTURES)

    assert cohort == cohort_digest(
        inputs.cohort,
        case_id=inputs.run.case_id,
        cohort_ref=inputs.run.cohort_ref,
    )
    assert seeds == seeds_digest(load_seeds(FIXTURES / "sources" / "seeds.yaml"))


def test_operator_urls_override_moves_both_digest_and_run_id() -> None:
    """``--urls`` полностью заменяет seed-набор — и манифест обязан назвать **тот же** набор.

    Операторский корпус меняет ``seeds_digest``, а значит и ``run_id``. Если бы манифест
    считал пакетный набор, он разошёлся бы с идентичностью — ровно та ложь, которую
    запрещает A3.
    """
    inputs = _inputs()
    base_cohort, base_seeds, _base_contract, base_run_id = _identity(inputs)
    urls = {"p1": ["https://example.test/root"]}
    cohort, seeds, contract, run_id = _identity(inputs, urls=urls)

    assert cohort == base_cohort  # когорта не менялась
    assert seeds != base_seeds
    assert run_id != base_run_id

    manifest = _manifest(inputs, urls=urls)
    assert (manifest.cohort_digest, manifest.seeds_digest) == (cohort, seeds)
    assert derive_run_id(
        inputs,
        cycle_id=manifest.cycle_id,
        cohort_digest=manifest.cohort_digest,
        seeds_digest=manifest.seeds_digest,
        execution_contract_digest=contract,
    ) == run_id


def test_declared_cohort_revision_moves_manifest_digest_with_run_id() -> None:
    """Смена объявленной ревизии когорты двигает ``run_id`` **и** поле манифеста вместе."""
    inputs = _inputs()
    base = _manifest(inputs)
    revised_run = inputs.run.model_copy(update={"cohort_ref": "fixture-r2"})
    revised = _manifest(dataclasses.replace(inputs, run=revised_run))

    assert revised.cohort_digest != base.cohort_digest
    assert revised.seeds_digest == base.seeds_digest
    assert revised.run_id != base.run_id


def test_missing_package_dir_does_not_fabricate_seeds_digest() -> None:
    """Без каталога пакета и без ``--urls`` seed-набор не из чего разрешить → ``None``.

    Дайджест пустого набора — **другой** факт (пустой набор даёт стабильный хеш), поэтому
    подставлять его нельзя: отсутствие данных не выдаётся за измерение.
    """
    inputs = _inputs()
    cohort, seeds = declared_identity_digests(inputs)
    assert cohort == cohort_digest(
        inputs.cohort,
        case_id=inputs.run.case_id,
        cohort_ref=inputs.run.cohort_ref,
    )
    assert seeds is None


# --------------------------------------------------------------------------- #
# 2. Персистенция: колонки ``runs.cohort_digest``/``runs.seeds_digest``
# --------------------------------------------------------------------------- #


def _store_manifest(**overrides: object) -> RunManifest:
    values: dict[str, object] = {
        "run_id": "run-1",
        "case_id": "case-1",
        "created_at": "2026-01-01T00:00:00Z",
        "mode": RunMode.STRICT,
        "methodology_version": "0.2.0",
        "package_version": "pkg-1",
        "catalog_version": "cat-1",
        "status_dict_version": "sd-1",
        "scoring_policy_version": "sp-1",
        "anchor_product_id": "p1",
    }
    values.update(overrides)
    return RunManifest(**values)  # type: ignore[arg-type]


def _cfg():
    import datetime as _dt

    from core.domain import Focus, RankingPolicy, RunConfig

    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1", "p2"],
        package="pkg-1",
        focus=Focus(modules=["m1"], params=[]),
        languages=["ru"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def test_persist_manifest_roundtrips_identity_digests() -> None:
    """``persist_manifest``→``read_manifest`` несут оба дайджеста (иначе diff их не увидит)."""
    with SqliteStore(":memory:") as store:
        store.create_run("run-1", _cfg())
        assert store.read_manifest("run-1").cohort_digest is None

        store.persist_manifest(
            "run-1", _store_manifest(cohort_digest="cd_1", seeds_digest="sd_1")
        )
        read = store.read_manifest("run-1")
        assert read.cohort_digest == "cd_1"
        assert read.seeds_digest == "sd_1"


def test_runs_table_has_identity_digest_columns() -> None:
    """Колонки аддитивные: ``SCHEMA_VERSION`` не поднимается, старая БД получает их."""
    from core.store.sqlite import SCHEMA_VERSION, connect

    conn = connect(":memory:")
    try:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(runs)")}
        assert {"cohort_digest", "seeds_digest"} <= columns
    finally:
        conn.close()
    # Подъём версии отверг бы гейтом (11 §2.6) все существующие прогоны ради метки,
    # которая ничего не добавляет к читаемости аддитивной колонки.
    assert SCHEMA_VERSION == "3.13.0"


# --------------------------------------------------------------------------- #
# 3. Диагностика diff: ось названа, предела больше нет
# --------------------------------------------------------------------------- #


def _pair(**overrides: object) -> tuple[RunManifest, RunManifest]:
    """Пара манифестов с разными контрактами и разными переопределёнными полями."""
    prev = _store_manifest(execution_contract_digest="ec_1", **overrides)
    changed = {
        key: ("cd_2" if key == "cohort_digest" else value)
        for key, value in overrides.items()
    }
    curr = _store_manifest(execution_contract_digest="ec_2", **changed)
    return prev, curr


def test_diff_names_cohort_digest_instead_of_diagnostic_limit() -> None:
    """Разные ``cohort_digest`` — названная ось, а не «предел диагностики»."""
    prev, curr = _pair(cohort_digest="cd_1")
    reasons = diff_boundary_violations(prev, curr)

    assert reasons[0] == "execution_change: execution_contract_digest: ec_1 → ec_2"
    assert (
        "execution_change: execution_contract_digest: cohort_digest: 'cd_1' → 'cd_2'"
        in reasons
    )
    limit = [reason for reason in reasons if "не восстановимо" in reason]
    assert all("cohort_digest" not in line for line in limit)
    assert all("seeds_digest" not in line for line in limit)


def test_diff_names_seeds_digest_instead_of_diagnostic_limit() -> None:
    """То же для ``seeds_digest`` — набор корней назван, а не объявлен невыводимым."""
    prev = _store_manifest(execution_contract_digest="ec_1", seeds_digest="sd_1")
    curr = _store_manifest(execution_contract_digest="ec_2", seeds_digest="sd_2")
    reasons = diff_boundary_violations(prev, curr)

    assert (
        "execution_change: execution_contract_digest: seeds_digest: 'sd_1' → 'sd_2'"
        in reasons
    )
    assert all(
        "seeds_digest" not in line for line in reasons if "не восстановимо" in line
    )


def test_diff_omits_limit_line_when_nothing_is_unavailable(monkeypatch) -> None:
    """Список недоступных пуст ⇒ строки о пределе нет вовсе (предела больше нет).

    Перечень контракта сужается до двух полей намеренно: у настоящего набора остаются
    оси, которых на манифесте нет (``extractor_stack`` и др.), и там строка о пределе
    уместна. Проверяется именно ветка «предела нет» — она не должна печатать лишнее.
    """
    import core.pipeline.report as report

    monkeypatch.setattr(
        report, "EXECUTION_CONTRACT_FIELDS", ("cohort_digest", "seeds_digest")
    )
    prev = _store_manifest(execution_contract_digest="ec_1", cohort_digest="cd", seeds_digest="sd")
    curr = _store_manifest(execution_contract_digest="ec_2", cohort_digest="cd", seeds_digest="sd")

    assert diff_boundary_violations(prev, curr) == [
        "execution_change: execution_contract_digest: ec_1 → ec_2"
    ]


def test_diff_does_not_invent_change_for_legacy_row_without_digest() -> None:
    """Строка прошлой схемы (дайджеста нет) — «не записано», а не «изменилось».

    Современный прогон пишет оба дайджеста всегда, поэтому ``None`` здесь означает
    отсутствие записи. Назвать это сменой когорты значило бы утверждать то, чего нет в
    данных (A3): поле уходит в перечень недоступных.
    """
    prev = _store_manifest(execution_contract_digest="ec_1")  # дайджестов нет
    curr = _store_manifest(
        execution_contract_digest="ec_2", cohort_digest="cd_2", seeds_digest="sd_2"
    )
    reasons = diff_boundary_violations(prev, curr)

    assert not any("cohort_digest: None" in reason for reason in reasons)
    assert not any("seeds_digest: None" in reason for reason in reasons)
    limit = [reason for reason in reasons if "не восстановимо" in reason]
    assert limit and "cohort_digest" in limit[0] and "seeds_digest" in limit[0]


def test_contract_fields_still_list_identity_digests() -> None:
    """Дайджесты остаются полями контракта: смена любого из них — ``execution_change``."""
    assert {"cohort_digest", "seeds_digest"} <= set(EXECUTION_CONTRACT_FIELDS)


@pytest.mark.parametrize("field", ["cohort_digest", "seeds_digest"])
def test_manifest_field_is_declared_optional(field: str) -> None:
    """Поле объявлено с дефолтом ``None`` — старая строка читается, догадка не вставляется."""
    assert RunManifest.model_fields[field].default is None
