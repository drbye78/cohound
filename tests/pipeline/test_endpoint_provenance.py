"""Проводка признака стенда RnD (§10) до профиля, манифеста и артефакта.

Проверяется ровно то, что нельзя проверить в домене: признак **доходит** до трёх выходов
(``ScoreBundle.publishability`` → ``run_manifest.publishability`` → ``outputs/publishability.yaml``)
и **переживает пост-хок пересборку** (``hound report``): декларация о месте исполнения не
выводится из ячеек, поэтому её единственный durable-носитель — ``runs.publishability_json``,
и без ранней записи пересборка молча снимала бы признак стенда с уже опубликованной карты.

``run_id`` при этом не меняется: место исполнения — декларация, а не условие измерения.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from core.domain.enums import (
    CellState,
    EndpointKind,
    Importance,
    LLMScenario,
    RunMode,
    Status,
)
from core.domain.models import (
    CatalogRow,
    Cell,
    Focus,
    LLMEvidence,
    RunConfig,
    ScenarioBinding,
)
from core.domain.scoring import evidence_coverage
from core.pipeline.compute import compute
from core.pipeline.report import build_manifest, write_publishability_artifact
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-endpoint-provenance"
PRODUCT = "prod.a"
HOST = "llm.stand.test"


def _row(param_id: str, importance: Importance = Importance.BASIC) -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title=param_id,
        module="mod.one",
        group="grp",
        importance=importance,
        param_type="fact",
        rule="правило",
    )


def _cell(param_id: str) -> Cell:
    return Cell(
        run_id=RUN_ID,
        product_id=PRODUCT,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.OK,
        evidence=[LLMEvidence(source_id="src.1", quote="цитата")],
    )


def _binding() -> ScenarioBinding:
    return ScenarioBinding(
        run_id=RUN_ID,
        scenario=LLMScenario.EVALUATOR,
        model_id="model.test",
        endpoint_kind=EndpointKind.API,
        decoder="json_object",
        endpoint_host=HOST,
    )


@pytest.fixture()
def store():
    """Прогон с одной объявленной строкой ``llm_scenarios`` (хост endpoint'а — durable)."""
    with SqliteStore(":memory:") as opened:
        opened.create_run(
            RUN_ID,
            RunConfig(
                case_id="case-1",
                anchor=PRODUCT,
                cohort="cohort.yaml",
                products=[PRODUCT],
                package="pkg-1",
                focus=Focus(),
                languages=["en"],
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
            ),
        )
        opened.save_llm_scenarios([_binding()])
        opened.save_cell(_cell("p001"))
        yield opened


def _bundle(store: SqliteStore, **kwargs: object):
    catalog = {"p001": _row("p001")}
    return compute(
        store=store,
        run_id=RUN_ID,
        catalog=catalog,
        module_weights={},
        universe={(PRODUCT, "p001")},
        **kwargs,  # type: ignore[arg-type]
    ), catalog


def test_hosts_come_from_durable_llm_scenarios(store: SqliteStore) -> None:
    """Хост endpoint'а читается из ``llm_scenarios``, а не угадывается из живого конфига."""
    bundle, _ = _bundle(store)
    profile = bundle.publishability

    assert profile is not None
    assert profile.llm_hosts == [HOST]


def test_declaration_reaches_manifest_and_artifact(
    store: SqliteStore, tmp_path: Path
) -> None:
    """Объявленное исключение видно в профиле, манифесте и ``outputs/publishability.yaml``."""
    bundle, catalog = _bundle(store, rnd_endpoint=True)
    profile = bundle.publishability
    assert profile is not None and profile.rnd_endpoint is True

    inputs = SimpleNamespace(
        run=SimpleNamespace(
            case_id="case-1",
            mode=RunMode.STRICT,
            methodology_version="0.1.0",
            cohort="cohort.yaml",
            ranking_policy=None,
        ),
        package=None,
    )
    plan = SimpleNamespace(
        run_id=RUN_ID,
        anchor=PRODUCT,
        modules=["mod.one"],
        methodology_version="0.1.0",
        catalog_version="1.0.0",
        cohort_ref="cohort.yaml",
        params=list(catalog.values()),
    )
    manifest = build_manifest(
        inputs=inputs,
        plan=plan,
        bundle=bundle,
        created_at="2026-01-01T00:00:00Z",
    )
    assert manifest.publishability["rnd_endpoint"] is True
    assert manifest.publishability["llm_hosts"] == [HOST]

    path = write_publishability_artifact(
        profile, tmp_path / "publishability.yaml", run_id=RUN_ID
    )
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert document["rnd_endpoint"] is True
    assert document["llm_hosts"] == [HOST]
    assert document["llm_host_notices"][0]["code"] == "rnd_endpoint_declared"


def test_post_hoc_recompute_keeps_declaration(store: SqliteStore) -> None:
    """Пост-хок пересборка (``hound report``) восстанавливает декларацию из durable-записи.

    Без ``save_publishability`` пересборка профиля из ячеек теряла бы признак стенда, и
    уже опубликованная карта перезаписывалась бы как производственная.
    """
    first, _ = _bundle(store, rnd_endpoint=True)
    profile = first.publishability
    assert profile is not None
    store.save_publishability(RUN_ID, profile.to_dict())

    recomputed, _ = _bundle(store)  # без явного флага: как это делает ``hound report``

    assert recomputed.publishability is not None
    assert recomputed.publishability.rnd_endpoint is True
    assert recomputed.publishability.rnd_endpoint_declared is True


def test_undeclared_host_is_reported_in_profile(store: SqliteStore) -> None:
    """Без объявления прогон заявляет производственный контур — это видно замечанием."""
    bundle, _ = _bundle(store)
    profile = bundle.publishability

    assert profile is not None
    assert profile.rnd_endpoint is False
    assert profile.rnd_endpoint_declared is False
    assert [notice["code"] for notice in profile.llm_host_notices] == [
        "rnd_endpoint_not_declared"
    ]


def test_declaration_does_not_change_class(store: SqliteStore) -> None:
    """Класс публикуемости не зависит от места исполнения (пин на «не демотирует»)."""
    declared, _ = _bundle(store, rnd_endpoint=True)
    undeclared, _ = _bundle(store, rnd_endpoint=None)
    internal, _ = _bundle(store, rnd_endpoint=False)

    classes = {
        bundle.publishability.klass
        for bundle in (declared, undeclared, internal)
        if bundle.publishability is not None
    }
    assert len(classes) == 1
    assert "rnd_endpoint" not in (declared.publishability.caveats or [])  # type: ignore[union-attr]


def test_coverage_is_untouched_by_the_flag(store: SqliteStore) -> None:
    """Признак не трогает измерение: Coverage/знаменатель одинаковы во всех состояниях."""
    coverages = {
        (bundle.coverage.value, bundle.coverage.denominator)
        for bundle in (
            _bundle(store, rnd_endpoint=True)[0],
            _bundle(store, rnd_endpoint=False)[0],
        )
    }
    assert len(coverages) == 1
    # И совпадает с доменным расчётом по тем же ячейкам (второй формулы нет).
    catalog = {"p001": _row("p001")}
    direct = evidence_coverage([_cell("p001")], catalog, {}, universe={(PRODUCT, "p001")})
    assert coverages == {(direct.value, direct.denominator)}
