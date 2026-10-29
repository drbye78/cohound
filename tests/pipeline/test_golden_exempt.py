"""Тесты waiver'а эталона по продукту (``exempt_products``, §11.6).

Предмет: ``golden.csv`` — таблица когорты, и «у продукта нет ни одной golden-строки»
было тем же молчанием, что и отсутствие файла: нельзя отличить «эталон для продукта ещё
не размечали» от «строки потеряли». Требование «эталон обязателен» до этого жило надписью:
``golden_required_for`` вычислялся, но ни одна фаза, гейт и ``report`` его не исполняли.

Проверяется:

* разбор ``exempt_products`` в ``sources/golden_status.yaml``: без поля, корректный
  waiver, waiver без ``product_id``/``reason``/``declared_by``, дубликат, неизвестный
  ключ записи, waiver при ``declined``, не-список, не-дата;
* waiver **не** входит в ``cohort_digest``: иначе объявление «эталона для продукта нет»
  меняло бы ``run_id`` и рвало межцикловый diff;
* линтер: продукт без golden и без waiver → ``warning`` с именем продукта (уровень
  ``warning``, а не блок: эталон не входит в ``run_id`` и не влияет на вердикты);
  waiver снимает предупреждение; waiver для покрытого/отсутствующего продукта →
  ``warning``; при ``declined`` правило покрытия не применяется (сверки нет вовсе);
* отчёт: аномалия ``golden_exempt_product`` попадает в ``Artifacts`` и строкой в
  операторский CSV, называя продукт и причину.

Данные категорийно-нейтральны (§1.3).
"""

from __future__ import annotations

import csv
import datetime as _dt
import json
import shutil
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from core.domain.enums import AnchorRelation, EndpointKind, LLMScenario, RunMode
from core.domain.models import (
    CatalogRow,
    Cohort,
    CohortEntry,
    RunConfig,
    RunManifest,
    ScenarioBinding,
)
from core.pipeline.compute import compute
from core.pipeline.digests import cohort_digest
from core.pipeline.golden_status import (
    STATE_DECLINED,
    STATE_PRESENT,
    GoldenProductExemption,
    GoldenStatus,
    golden_status_path,
    read_golden_status,
)
from core.pipeline.lint import (
    check_golden_coverage,
    check_golden_exempt_sync,
    lint_run_inputs,
)
from core.pipeline.normalize import RunInputs, load_inputs
from core.pipeline.report import collect_golden_exempt_anomalies, write_reports
from core.store.sqlite_store import SqliteStore

FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
RUN_YAML = FIXTURES / "run.yaml"
REPORT_CATALOG = Path(__file__).resolve().parent / "fixtures" / "report_catalog.json"

RUN_ID = "run-golden-exempt"
PRODUCT_A = "prod.a"
PRODUCT_B = "prod.b"

#: Артефакты, которых достаточно для ``load_inputs``.
_PACKAGE_FILES = (
    "package.yaml",
    "catalog.csv",
    "baselines.csv",
    "cohort.yaml",
    "domain_profiles.yaml",
)

_HEADER = "schema: golden_status/1\n"

#: Готовая запись waiver'а (нейтральные значения).
_WAIVER = (
    "exempt_products:\n"
    "  - product_id: p2\n"
    '    reason: "эталон для продукта ещё не размечали"\n'
    "    declared_by: analyst\n"
    "    declared_at: '2026-02-01'\n"
)


def _package(tmp_path: Path, *, golden: bool = True) -> Path:
    """Копия фикстур в ``tmp_path`` (когорта — ``p1``/``p2``, golden по обоим)."""
    for name in _PACKAGE_FILES:
        shutil.copy(FIXTURES / name, tmp_path / name)
    if golden:
        shutil.copy(FIXTURES / "golden.csv", tmp_path / "golden.csv")
    return tmp_path


def _declare(package: Path, text: str) -> Path:
    """Пишет объявление в ``{package}/sources/golden_status.yaml``."""
    path = golden_status_path(package)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _inputs() -> RunInputs:
    """Нормализованные входы на фикстурах (эталон покрывает всю когорту)."""
    return replace(
        load_inputs(RUN_YAML, FIXTURES),
        scope_attestation_procedure="ручная аттестация полноты источника (тест)",
    )


def _waiver(product_id: str = "p2", reason: str = "эталона нет") -> GoldenProductExemption:
    """Запись waiver'а для линтер/отчёт-тестов."""
    return GoldenProductExemption(
        product_id=product_id, reason=reason, declared_by="analyst"
    )


def _status(*exempt: GoldenProductExemption, state: str = STATE_PRESENT) -> GoldenStatus:
    """Объявленное состояние эталона с заданными waiver'ами."""
    return GoldenStatus(
        state=state,  # type: ignore[arg-type] — значения заданы тестом
        declared=True,
        file_present=True,
        exempt_products=exempt,
    )


# ---------------------------------------------------------------------------
# Разбор golden_status.yaml: exempt_products
# ---------------------------------------------------------------------------


def test_no_field_means_no_waivers(tmp_path: Path) -> None:
    """Обратная совместимость: файлы без ``exempt_products`` разбираются как раньше."""
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: present\n")
    status = read_golden_status(package)
    assert status.exempt_products == ()
    assert status.exempt_ids == frozenset()


def test_waiver_is_parsed(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: present\n" + _WAIVER)
    status = read_golden_status(package)
    assert status.state == STATE_PRESENT
    assert status.exempt_ids == {"p2"}
    (entry,) = status.exempt_products
    assert entry.product_id == "p2"
    assert entry.reason == "эталон для продукта ещё не размечали"
    assert entry.declared_by == "analyst"
    assert entry.declared_at == "2026-02-01"
    # Представление для JSON: waiver виден вызывающему, а не только разборщику.
    assert status.as_dict()["exempt_products"] == [
        {
            "product_id": "p2",
            "reason": "эталон для продукта ещё не размечали",
            "declared_by": "analyst",
            "declared_at": "2026-02-01",
        }
    ]


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(
            '  - reason: "причина"\n    declared_by: analyst\n', id="no-product"
        ),
        pytest.param(
            '  - product_id: p2\n    declared_by: analyst\n', id="no-reason"
        ),
        pytest.param('  - product_id: p2\n    reason: "причина"\n', id="no-author"),
        pytest.param(
            '  - product_id: p2\n    reason: "  "\n    declared_by: "  "\n',
            id="blank-values",
        ),
    ],
)
def test_waiver_requires_product_reason_and_author(tmp_path: Path, entry: str) -> None:
    """Тихий waiver запрещён: без причины и автора это пропуск, а не решение."""
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: present\nexempt_products:\n" + entry)
    with pytest.raises(ValueError) as excinfo:
        read_golden_status(package)
    message = str(excinfo.value)
    assert message.startswith("golden_status.yaml:")
    assert "тихий waiver" in message


def test_duplicate_waiver_product_is_error(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(
        package,
        _HEADER
        + "state: present\nexempt_products:\n"
        + '  - product_id: p2\n    reason: "первая причина"\n    declared_by: analyst\n'
        + '  - product_id: p2\n    reason: "вторая причина"\n    declared_by: analyst\n',
    )
    with pytest.raises(ValueError, match="объявлен дважды"):
        read_golden_status(package)


def test_unknown_key_in_waiver_is_error(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(
        package,
        _HEADER
        + "state: present\nexempt_products:\n"
        + '  - product_id: p2\n    reason: "причина"\n    declared_by: analyst\n'
        + "    skip_comparison: true\n",
    )
    with pytest.raises(ValueError, match="неизвестные ключи"):
        read_golden_status(package)


def test_waiver_is_allowed_at_declined(tmp_path: Path) -> None:
    """Поле осмысленно при любом ``state``: продукт может быть исключён и при ``declined``."""
    package = _package(tmp_path)
    _declare(
        package,
        _HEADER
        + "state: declined\n"
        + 'reason: "разметка непригодна"\n'
        + "declined_by: analyst\n"
        + _WAIVER,
    )
    status = read_golden_status(package)
    assert status.state == STATE_DECLINED
    assert status.exempt_ids == {"p2"}


def test_exempt_products_must_be_list(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: present\nexempt_products: p2\n")
    with pytest.raises(ValueError, match="обязан быть списком"):
        read_golden_status(package)


def test_waiver_entry_must_be_mapping(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(package, _HEADER + "state: present\nexempt_products:\n  - p2\n")
    with pytest.raises(ValueError, match="обязан быть словарём"):
        read_golden_status(package)


def test_waiver_bad_declared_at_is_error(tmp_path: Path) -> None:
    package = _package(tmp_path)
    _declare(
        package,
        _HEADER
        + "state: present\nexempt_products:\n"
        + '  - product_id: p2\n    reason: "причина"\n    declared_by: analyst\n'
        + "    declared_at: '01.02.2026'\n",
    )
    with pytest.raises(ValueError, match="ISO-8601"):
        read_golden_status(package)


# ---------------------------------------------------------------------------
# waiver не входит в идентичность прогона
# ---------------------------------------------------------------------------


def _cohort() -> Cohort:
    """Когорта с одним якорем (валидная по домену)."""
    return Cohort(
        case_id="case_x",
        frozen_at=_dt.date(2026, 6, 1),
        entries=[
            CohortEntry(
                product_id="p1",
                role_overlap=1.0,
                selection_reason="якорь",
                anchor_relation=AnchorRelation.ANCHOR,
            ),
            CohortEntry(
                product_id="p2",
                role_overlap=0.5,
                selection_reason="аналог",
                anchor_relation=AnchorRelation.ANALOG,
            ),
        ],
    )


def test_cohort_digest_ignores_waivers() -> None:
    """Объявление «эталона для продукта нет» не меняет ``run_id`` (иначе рвётся diff).

    Дайджест считается по составу когорты; waiver живёт в объявлении эталона и в него не
    попадает — иначе объявление об инструменте проверки переписывало бы идентичность
    измерения, и межцикловый diff помечался бы ``execution_change`` без смены условий.
    """
    plain = cohort_digest(_cohort(), case_id="case_x")
    # Дайджест — функция только когорты: waiver'ы ему не передаются вовсе.
    with_waivers = _status(_waiver("p2"), _waiver("p1"))
    assert with_waivers.exempt_ids == {"p1", "p2"}
    assert cohort_digest(_cohort(), case_id="case_x") == plain
    # Контроль: изменение состава когорты дайджест меняет (тест не пустой).
    other = _cohort().model_copy(update={"frozen_at": _dt.date(2026, 6, 2)})
    assert cohort_digest(other, case_id="case_x") != plain


# ---------------------------------------------------------------------------
# Линтер: покрытие golden по продуктам когорты
# ---------------------------------------------------------------------------


def _inputs_with_uncovered_product() -> RunInputs:
    """Входы, где продукт ``p3`` в когорте, но golden-строк для него нет.

    Оба тира эталона при этом на месте: иначе «нет покрытия» смешалось бы с «нет тира»
    и правило ``golden-presence`` шумело бы не о том.
    """
    inputs = _inputs()
    cohort = inputs.cohort.model_copy(
        update={
            "entries": [
                *inputs.cohort.entries,
                CohortEntry(
                    product_id="p3",
                    role_overlap=0.3,
                    selection_reason="аналог",
                    anchor_relation=AnchorRelation.ANALOG,
                ),
            ]
        }
    )
    run = inputs.run.model_copy(update={"products": [*inputs.run.products, "p3"]})
    return replace(inputs, cohort=cohort, run=run)


def test_product_without_golden_and_without_waiver_is_warning() -> None:
    """Требование «эталон покрывает когорту» видно, но не запрещает измерение.

    Уровень — ``warning`` (решение владельца 29.10.2026): ``golden.csv`` не входит в
    ``execution_contract_digest`` и не влияет на вердикты (§11.3), поэтому гейт на нём
    объявлял бы проверочный инструмент условием замера — и запрещал бы первый замер
    продукта, эталон для которого аннотируют **по** его результату.
    """
    inputs = _inputs_with_uncovered_product()
    messages = check_golden_coverage(inputs)
    assert len(messages) == 1
    assert "p3" in messages[0]
    assert "sources/golden_status.yaml" in messages[0]

    report = lint_run_inputs(inputs)
    assert report.warnings == messages
    assert report.incomplete == []
    assert report.blocking == []
    assert report.ready is True, "неполнота эталона не запрещает прогон"


def test_waiver_clears_coverage_warning() -> None:
    """Объявленный waiver — решение, а не недочёт: прогон не блокируется и не шумит."""
    inputs = replace(
        _inputs_with_uncovered_product(), golden_status=_status(_waiver("p3"))
    )
    assert check_golden_coverage(inputs) == []
    report = lint_run_inputs(inputs)
    assert report.incomplete == []
    assert report.warnings == []
    assert report.ready is True


def test_waiver_for_covered_product_warns() -> None:
    """Эталон появился, waiver не сняли — устаревшее объявление видно warning'ом."""
    inputs = replace(_inputs(), golden_status=_status(_waiver("p2")))
    messages = check_golden_exempt_sync(inputs)
    assert len(messages) == 1
    assert "p2" in messages[0]
    report = lint_run_inputs(inputs)
    assert report.warnings == messages
    assert report.ready is True, "сверка идёт — это не блокировка"


def test_waiver_for_product_outside_cohort_warns() -> None:
    inputs = replace(_inputs(), golden_status=_status(_waiver("p_ghost")))
    messages = check_golden_exempt_sync(inputs)
    assert len(messages) == 1
    assert "p_ghost" in messages[0]
    assert "нет в когорте" in messages[0]
    assert lint_run_inputs(inputs).ready is True


def test_coverage_rule_not_applicable_at_declined() -> None:
    """``declined`` — сверки нет вовсе: требовать покрытие не по чему."""
    inputs = replace(
        _inputs(),
        golden=[],
        golden_status=GoldenStatus(
            state=STATE_DECLINED,
            declared=True,
            file_present=True,
            reason="разметка непригодна",
            declined_by="analyst",
        ),
    )
    assert check_golden_coverage(inputs) == []
    assert check_golden_exempt_sync(inputs) == []
    assert lint_run_inputs(inputs).incomplete == []


def test_clean_fixture_has_no_golden_messages() -> None:
    """Эталон покрывает когорту целиком — новых сообщений нет."""
    assert check_golden_coverage(_inputs()) == []
    assert check_golden_exempt_sync(_inputs()) == []


# ---------------------------------------------------------------------------
# Отчёт: аномалия golden_exempt_product
# ---------------------------------------------------------------------------


def _catalog() -> dict[str, CatalogRow]:
    rows = json.loads(REPORT_CATALOG.read_text(encoding="utf-8"))
    return {row["param_id"]: CatalogRow(**row) for row in rows}


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor=PRODUCT_A,
        cohort="cohort.yaml",
        products=[PRODUCT_A, PRODUCT_B],
        package="pkg-1",
        languages=["en"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )


@pytest.fixture()
def store() -> Iterator[SqliteStore]:
    """Хранилище с созданным прогоном (FK cells → runs включён)."""
    with SqliteStore(":memory:") as sqlite_store:
        sqlite_store.create_run(RUN_ID, _cfg())
        yield sqlite_store


def _manifest() -> RunManifest:
    data: dict[str, Any] = {
        "run_id": RUN_ID,
        "case_id": "case-1",
        "created_at": "2026-01-01T00:00:00+00:00",
        "mode": RunMode.STRICT,
        "methodology_version": "2.0.0",
        "package_version": "1.0.0",
        "catalog_version": "1.0.0",
        "status_dict_version": "1.0.0",
        "scoring_policy_version": "1.0.0",
        "anchor_product_id": PRODUCT_A,
        "cohort_ref": "cohort-1",
        "llm_scenarios": [
            ScenarioBinding(
                run_id=RUN_ID,
                scenario=LLMScenario.EVALUATOR,
                model_id="model.1",
                model_version="1",
                endpoint_kind=EndpointKind.SELF_HOST,
                web_tool=False,
            )
        ],
        "snapshot_date": "2026-01-01",
    }
    return RunManifest(**data)


def _read_csv(path: Path | None) -> list[dict[str, str]]:
    assert path is not None
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_collect_anomaly_names_product_and_reason() -> None:
    """Аномалия называет продукт и причину из объявления, а не «есть waiver»."""
    anomalies = collect_golden_exempt_anomalies(
        _status(_waiver("p2", reason="эталон не размечали"))
    )
    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly["product_id"] == "p2"
    assert anomaly["param_id"] == ""
    assert "эталон не размечали" in anomaly["reason"]
    assert "analyst" in anomaly["reason"]
    assert anomaly["declared_by"] == "analyst"
    # Объявления не читали / waiver'ов нет — пусто, а не выдуманная запись.
    assert collect_golden_exempt_anomalies(None) == []
    assert collect_golden_exempt_anomalies(_status()) == []


def test_anomaly_is_in_artifacts_and_operator_csv(
    store: SqliteStore, tmp_path: Path
) -> None:
    """«Эталона для продукта нет» видно в артефактах прогона, а не только в пакете."""
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    artifacts = write_reports(
        bundle=bundle,
        cells=[],
        catalog=_catalog(),
        manifest=_manifest(),
        out_dir=tmp_path,
        run_id=RUN_ID,
        golden_status=_status(_waiver("p2", reason="эталон не размечали")),
    )

    assert artifacts.golden_exempt_products
    assert artifacts.golden_exempt_products[0]["product_id"] == "p2"
    # Аномалия — наблюдение (I12), поэтому она в общем списке аномалий прогона.
    assert any(
        anomaly.get("product_id") == "p2" for anomaly in artifacts.i3_anomalies
    )

    rows = _read_csv(artifacts.nda_questions)
    exempt_rows = [
        row for row in rows if row.get("anomaly_kind") == "golden_exempt_product"
    ]
    assert len(exempt_rows) == 1
    assert exempt_rows[0]["product_id"] == "p2"
    assert "эталон не размечали" in exempt_rows[0]["i3_anomaly"]


def test_write_reports_finds_waiver_in_package_without_explicit_argument(
    store: SqliteStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Производитель аномалии работает и без явного аргумента: объявление берётся из пакета.

    Waiver не входит в ``run_id`` и не персистится в БД прогона, поэтому единственный
    источник для отчёта — авторский вход пакета; каталог находится по ``manifest.case_id``
    через объявленную раскладку (``COHOUND_PACKAGES_DIR``), а не догадкой от CWD.
    """
    packages = tmp_path / "packages"
    package = packages / "case-1"
    _declare(
        package,
        _HEADER + "state: absent\n" + _WAIVER,
    )
    monkeypatch.setenv("COHOUND_PACKAGES_DIR", str(packages))

    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    artifacts = write_reports(
        bundle=bundle,
        cells=[],
        catalog=_catalog(),
        manifest=_manifest(),
        out_dir=tmp_path / "runs",
        run_id=RUN_ID,
    )

    assert [item["product_id"] for item in artifacts.golden_exempt_products] == ["p2"]
    rows = _read_csv(artifacts.nda_questions)
    assert any(
        row.get("anomaly_kind") == "golden_exempt_product" for row in rows
    ), "waiver из пакета обязан дойти до операторского CSV"


def test_write_reports_without_package_has_no_waivers(
    store: SqliteStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Пакета нет по объявленному пути — waiver'ов не показываем и отчёт не роняем."""
    monkeypatch.setenv("COHOUND_PACKAGES_DIR", str(tmp_path / "absent-packages"))
    bundle = compute(store=store, run_id=RUN_ID, catalog=_catalog(), module_weights={})
    artifacts = write_reports(
        bundle=bundle,
        cells=[],
        catalog=_catalog(),
        manifest=_manifest(),
        out_dir=tmp_path / "runs",
        run_id=RUN_ID,
    )
    assert artifacts.golden_exempt_products == []
