"""Тесты низкоуровневого I/O артефактов пакета (итерация I1, ``package_io``).

Проверяют: атомарную детерминированную запись (YAML/CSV), path-конфайнмент,
round-trip чтения/записи, условные колонки ``deprecated``/``replaced_by`` (§13),
проспективный линт, предикат новых blocking и append-only журнал мутаций.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.config.crawl_sources import CrawlSource, CrawlSourcesConfig
from core.config.run_config import load_catalog, load_run_config
from core.domain.models import (
    CatalogRow,
    CrawlRunConfig,
    ModuleDef,
    PackageManifest,
    RunConfig,
)
from core.domain.enums import (
    Archetype,
    CrawlStrategy,
    ExtractionRoute,
    FetchMode,
    Importance,
    ParamType,
)
from core.pipeline.lint import LintReport
from core.pipeline.package_io import (
    ARTIFACT_FILES,
    PackageArtifacts,
    append_apply_log,
    assert_no_new_blocking,
    atomic_write_csv,
    atomic_write_text,
    build_run_inputs,
    delete_artifact,
    get_artifact,
    list_artifacts,
    load_artifacts,
    matches,
    patch_run_projection,
    prospective_lint,
    save_artifact,
    scan_packages,
    validate_case_id,
)

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = ROOT / "packages" / "_template"


def _row(**overrides: object) -> CatalogRow:
    base: dict[str, object] = {
        "param_id": "param.x",
        "title": "X",
        "module": "module_core",
        "group": "core",
        "importance": Importance.BASIC,
        "param_type": ParamType.FACT,
        "rule": "route",
    }
    base.update(overrides)
    return CatalogRow.model_validate(base)


# ---------------------------------------------------------------------------
# Атомарная запись / детерминизм
# ---------------------------------------------------------------------------


def test_atomic_write_text_is_deterministic(tmp_path: Path) -> None:
    path = tmp_path / "out.txt"
    atomic_write_text(path, "строка\n")
    atomic_write_text(path, "строка\n")
    assert path.read_text(encoding="utf-8") == "строка\n"
    assert not list(tmp_path.glob(".*.tmp")), "временные файлы не должны оставаться"


def test_atomic_write_csv_uses_lf_and_creates_parent(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "out.csv"
    atomic_write_csv(path, ["a", "b"], [["1", "2"], ["3", None]])
    raw = path.read_bytes()
    assert b"\r\n" not in raw
    assert raw == "a,b\n1,2\n3,\n".encode("utf-8")


def test_validate_case_id_accepts_slug_and_rejects_bad() -> None:
    assert validate_case_id("demo_pkg") == "demo_pkg"
    for bad in ("", "Demo", "a-b", "../x", "a/b", "a b"):
        with pytest.raises(ValueError):
            validate_case_id(bad)


# ---------------------------------------------------------------------------
# Round-trip артефактов
# ---------------------------------------------------------------------------


def test_load_artifacts_reads_template(tmp_path: Path) -> None:
    artifacts = load_artifacts(TEMPLATE)
    assert artifacts.manifest is not None
    assert artifacts.cohort is not None
    assert artifacts.run is not None
    assert artifacts.catalog
    assert artifacts.scope_attestation


def test_load_artifacts_missing_dir_returns_empty(tmp_path: Path) -> None:
    artifacts = load_artifacts(tmp_path / "nope")
    assert artifacts == PackageArtifacts()
    assert artifacts.manifest is None
    assert artifacts.catalog == []


def test_save_and_reload_round_trip(tmp_path: Path) -> None:
    source = load_artifacts(TEMPLATE)
    for name in ("package", "catalog", "baselines", "cohort", "golden", "run"):
        save_artifact(tmp_path, name, {
            "package": source.manifest,
            "catalog": source.catalog,
            "baselines": source.baselines,
            "cohort": source.cohort,
            "golden": source.golden,
            "run": source.run,
        }[name])
    reloaded = load_artifacts(tmp_path)
    assert reloaded.manifest == source.manifest
    assert reloaded.cohort == source.cohort
    assert reloaded.run == source.run
    assert reloaded.catalog == sorted(source.catalog, key=lambda r: r.param_id)
    assert reloaded.golden == sorted(
        source.golden, key=lambda r: (r.product_id, r.param_id, r.tier.value, r.case_class)
    )


def test_save_run_omits_none_and_keeps_explicit_crawl_axes(tmp_path: Path) -> None:
    """Полная запись run.yaml: null-поля не эмитятся, явные оси crawl сохраняются."""
    source = load_artifacts(TEMPLATE).run
    assert source is not None
    run = source.model_copy(
        update={"llm_slot": None, "crawl": CrawlRunConfig(max_response_bytes=123, max_depth=2)}
    )

    path = save_artifact(tmp_path, "run", run)
    text = path.read_text(encoding="utf-8")

    assert "max_response_bytes: 123" in text
    assert "max_response_bytes: null" not in text
    assert "llm_slot" not in text
    assert load_run_config(path).crawl == CrawlRunConfig(max_response_bytes=123, max_depth=2)


#: run.yaml с комментариями и операционной секцией crawl (стиль живого пакета).
_RUN_WITH_COMMENTS = (
    "# run: комментарий верхнего уровня\n"
    "case_id: demo\n"
    "anchor: alpha          # якорь когорты\n"
    "cohort: cohort_demo\n"
    "products:\n"
    "  - alpha\n"
    "  - beta\n"
    "package: demo\n"
    "snapshot_date: '2026-01-01'\n"
    "mode: strict\n"
    "crawl:\n"
    "  # операционная секция\n"
    "  max_depth: 3\n"
)


def test_patch_run_projection_preserves_comments_and_is_idempotent(tmp_path: Path) -> None:
    """Точечный патч меняет только anchor/products и идемпотентен."""
    run = RunConfig.model_validate(
        {
            "case_id": "demo",
            "anchor": "beta",
            "cohort": "cohort_demo",
            "products": ["beta", "gamma"],
            "package": "demo",
            "snapshot_date": "2026-01-01",
            "mode": "strict",
        }
    )

    once = patch_run_projection(_RUN_WITH_COMMENTS, run)
    twice = patch_run_projection(once, run)
    assert once == twice, "патч обязан быть идемпотентным"

    assert "# run: комментарий верхнего уровня" in once
    assert "# якорь когорты" in once
    assert "# операционная секция" in once
    assert "anchor: beta          # якорь когорты" in once
    assert "products:\n  - beta\n  - gamma\n" in once
    assert "- alpha" not in once
    assert once.endswith("  # операционная секция\n  max_depth: 3\n")

    path = tmp_path / "run.yaml"
    path.write_text(once, encoding="utf-8")
    loaded = load_run_config(path)
    assert loaded.anchor == "beta"
    assert loaded.products == ["beta", "gamma"]
    assert loaded.crawl is not None and loaded.crawl.max_depth == 3


def test_patch_run_projection_appends_missing_products_block() -> None:
    """Если ``products:`` нет — блок добавляется в конец, остальной текст не тронут."""
    run = RunConfig.model_validate(
        {
            "case_id": "demo",
            "anchor": "beta",
            "cohort": "cohort_demo",
            "products": ["beta", "gamma"],
            "package": "demo",
            "snapshot_date": "2026-01-01",
            "mode": "strict",
        }
    )

    patched = patch_run_projection("case_id: demo\nanchor: alpha\npackage: demo\n", run)

    assert patched == (
        "case_id: demo\nanchor: beta\npackage: demo\nproducts:\n- beta\n- gamma\n"
    )


def test_optional_monitoring_columns_only_when_needed(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    rows = [_row()]
    save_artifact(plain, "catalog", rows)
    header = (plain / "catalog.csv").read_text(encoding="utf-8").splitlines()[0]
    assert "deprecated" not in header and "replaced_by" not in header

    flagged = tmp_path / "flagged"
    save_artifact(flagged, "catalog", [_row(deprecated=True, replaced_by="param.y")])
    reloaded = load_catalog(flagged / "catalog.csv")
    assert reloaded[0].deprecated is True
    assert reloaded[0].replaced_by == "param.y"


def test_csv_without_optional_columns_is_backward_compatible(tmp_path: Path) -> None:
    path = tmp_path / "catalog.csv"
    path.write_text(
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs\n"
        "param.a,T,module_core,g,Баз,fact,rule,explicit,,\"{\"\"ru\"\": [\"\"a\"\"]}\",ru\n",
        encoding="utf-8",
    )
    rows = load_catalog(path)
    assert rows[0].deprecated is False
    assert rows[0].replaced_by is None


def test_save_artifact_unknown_name(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        save_artifact(tmp_path, "nope", {})


def test_save_artifact_wrong_type(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        save_artifact(tmp_path, "package", {"not": "a manifest"})


def test_save_artifact_scope_and_profiles(tmp_path: Path) -> None:
    save_artifact(tmp_path, "scope_attestation", "полная проверка")
    save_artifact(tmp_path, "domain_profiles", {"dom": {"fetch": "http", "extract": "html"}})
    reloaded = load_artifacts(tmp_path)
    assert reloaded.scope_attestation == "полная проверка"
    assert reloaded.domain_profiles["dom"].fetch is FetchMode.HTTP
    assert reloaded.domain_profiles["dom"].extract is ExtractionRoute.HTML


# ---------------------------------------------------------------------------
# crawl_sources.yaml (Lane A): чтение/запись артефакта источников краула
# ---------------------------------------------------------------------------


def _crawl_sources() -> CrawlSourcesConfig:
    """Нейтральный конфиг источников: defaults + product + site-слой."""
    return CrawlSourcesConfig(
        defaults=CrawlSource(kind="web", engine="http"),
        products={"p1": CrawlSource(engine="scrapy", strategy=CrawlStrategy.HONEST)},
        sites={
            "p1": {
                "example.com": CrawlSource(
                    kind="feed", params={"selector": "item"}
                )
            }
        },
    )


def test_load_artifacts_without_crawl_sources_defaults(tmp_path: Path) -> None:
    # Отсутствующий crawl_sources.yaml → пустой конфиг по умолчанию.
    artifacts = load_artifacts(tmp_path)
    assert artifacts.crawl_sources == CrawlSourcesConfig()


def test_save_and_reload_crawl_sources_round_trip(tmp_path: Path) -> None:
    cfg = _crawl_sources()
    save_artifact(tmp_path, "crawl_sources", cfg)
    assert (tmp_path / "crawl_sources.yaml").is_file()
    reloaded = load_artifacts(tmp_path)
    assert reloaded.crawl_sources == cfg


def test_save_artifact_crawl_sources_wrong_type(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        save_artifact(tmp_path, "crawl_sources", {"not": "a config"})


def test_list_artifacts_crawl_sources_count(tmp_path: Path) -> None:
    save_artifact(tmp_path, "crawl_sources", _crawl_sources())
    items = {item["name"]: item for item in list_artifacts(tmp_path)}
    # Счётчик — число явных источников: 1 продукт + 1 site-запись.
    assert items["crawl_sources"]["present"] is True
    assert items["crawl_sources"]["count"] == 2
    assert items["crawl_sources"]["size"] > 0


def test_get_artifact_crawl_sources_structured(tmp_path: Path) -> None:
    save_artifact(tmp_path, "crawl_sources", _crawl_sources())
    view, path = get_artifact(tmp_path, "crawl_sources")
    assert path == tmp_path / "crawl_sources.yaml"
    assert isinstance(view, dict)
    assert set(view) == {"defaults", "products", "sites"}
    assert view["products"]["p1"]["engine"] == "scrapy"
    raw, _ = get_artifact(tmp_path, "crawl_sources", raw=True)
    assert isinstance(raw, str) and raw


def test_delete_crawl_sources_resets_to_default(tmp_path: Path) -> None:
    save_artifact(tmp_path, "crawl_sources", _crawl_sources())
    assert delete_artifact(tmp_path, "crawl_sources") is True
    assert not (tmp_path / "crawl_sources.yaml").exists()
    assert load_artifacts(tmp_path).crawl_sources == CrawlSourcesConfig()
    assert delete_artifact(tmp_path, "crawl_sources") is False


# ---------------------------------------------------------------------------
# Проспективный линт
# ---------------------------------------------------------------------------


def test_prospective_lint_on_template_is_clean() -> None:
    report = prospective_lint(load_artifacts(TEMPLATE))
    assert report.blocking == []
    assert report.warnings == []


def test_prospective_lint_synthesizes_partial_state() -> None:
    report = prospective_lint(PackageArtifacts())
    assert report.blocking == []
    assert any("golden" in warning for warning in report.warnings)


def test_build_run_inputs_synthesizes_missing_run() -> None:
    inputs = build_run_inputs(PackageArtifacts())
    assert inputs.run.products == ["anchor"]
    assert inputs.run.anchor == "anchor"
    assert inputs.package.case_id == "package"


def test_assert_no_new_blocking_detects_delta() -> None:
    before = LintReport(blocking=["a"], warnings=[])
    after = LintReport(blocking=["a", "b"], warnings=[])
    assert assert_no_new_blocking(before, after) == ["b"]
    assert assert_no_new_blocking(after, before) == []


def test_build_run_inputs_uses_manifest_case_id() -> None:
    artifacts = load_artifacts(TEMPLATE)
    inputs = build_run_inputs(artifacts)
    assert inputs.package.case_id == "template"


# ---------------------------------------------------------------------------
# Read-only обзор/чтение артефактов: list_artifacts / get_artifact
# ---------------------------------------------------------------------------


def test_list_artifacts_reports_presence_size_and_count() -> None:
    template = load_artifacts(TEMPLATE)
    assert template.run is not None
    items = list_artifacts(TEMPLATE)
    assert [item["name"] for item in items] == list(ARTIFACT_FILES)
    by_name = {item["name"]: item for item in items}
    # Шаблон не содержит crawl_sources.yaml — он опционален; прочие на месте.
    assert all(
        item["present"] for item in items if item["name"] != "crawl_sources"
    )
    assert by_name["crawl_sources"]["present"] is False
    assert by_name["crawl_sources"]["count"] is None
    assert by_name["package"]["count"] == 2  # два модуля шаблона
    assert by_name["catalog"]["count"] == len(template.catalog)
    assert by_name["catalog"]["size"] > 0
    assert by_name["scope_attestation"]["count"] == 1
    assert by_name["run"]["count"] == len(template.run.products)


def test_list_artifacts_absent_and_broken_package(tmp_path: Path) -> None:
    # Отсутствующий пакет: все артефакты absent, count None.
    items = list_artifacts(tmp_path / "nope")
    assert all(item["present"] is False for item in items)
    assert all(item["count"] is None for item in items)
    assert all(item["size"] == 0 for item in items)

    # Битый catalog.csv не валит обзор: состав читается по файловой системе.
    broken = tmp_path / "broken"
    save_artifact(broken, "package", load_artifacts(TEMPLATE).manifest)
    (broken / "catalog.csv").write_text("bad,header\n1,2\n", encoding="utf-8")
    items = list_artifacts(broken)
    by_name = {item["name"]: item for item in items}
    assert by_name["package"]["present"] is True
    assert by_name["catalog"]["present"] is True
    assert by_name["catalog"]["count"] is None  # разбор не удался — счётчик не выдуман
    assert by_name["catalog"]["size"] > 0


def test_get_artifact_structured_views() -> None:
    manifest, path = get_artifact(TEMPLATE, "package")
    assert path == TEMPLATE / "package.yaml"
    assert manifest["case_id"] == "template"
    assert len(manifest["modules"]) == 2

    catalog, _ = get_artifact(TEMPLATE, "catalog")
    assert isinstance(catalog, list) and catalog[0]["param_id"]

    profiles, _ = get_artifact(TEMPLATE, "domain_profiles")
    assert isinstance(profiles, dict)

    procedure, _ = get_artifact(TEMPLATE, "scope_attestation")
    assert isinstance(procedure, str) and procedure


def test_get_artifact_raw_returns_text() -> None:
    data, path = get_artifact(TEMPLATE, "run", raw=True)
    assert isinstance(data, str)
    assert data == (TEMPLATE / "run.yaml").read_text(encoding="utf-8")
    assert path == TEMPLATE / "run.yaml"


def test_get_artifact_absent_and_unknown(tmp_path: Path) -> None:
    data, path = get_artifact(tmp_path / "nope", "scope_attestation")
    assert data is None
    assert path == tmp_path / "nope" / "scope_attestation.yaml"
    raw, _ = get_artifact(tmp_path / "nope", "scope_attestation", raw=True)
    assert raw is None
    with pytest.raises(ValueError):
        get_artifact(TEMPLATE, "nope")


# ---------------------------------------------------------------------------
# Журнал авторских мутаций
# ---------------------------------------------------------------------------


def test_append_apply_log_is_append_only_and_sorted(tmp_path: Path) -> None:
    log = append_apply_log(tmp_path, {"op": "param-add", "actor": "a1", "changed": ["catalog"]})
    append_apply_log(tmp_path, {"op": "module-add", "actor": "a2", "changed": ["package"]})
    assert log == tmp_path / ".authoring" / "apply_log.jsonl"
    lines = log.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["op"] == "param-add"
    assert json.loads(lines[1])["actor"] == "a2"
    # Ключи отсортированы (детерминированный JSON).
    assert lines[0].startswith('{"actor"')


# ---------------------------------------------------------------------------
# Поиск/фильтрация и скан каталога пакетов
# ---------------------------------------------------------------------------


def test_matches_is_case_insensitive_unicode_and_empty_query() -> None:
    assert matches("Backup", "backup") is True
    assert matches("Ключ", "ключ") is True
    assert matches("ёлка", "елка") is True  # ё→е
    assert matches(None, "x") is False
    assert matches("abc", None) is True
    assert matches("abc", "") is True
    assert matches("abc", "zzz") is False


def _write_min_package(entry: Path, case_id: str, *, version: str = "1.0.0") -> Path:
    """Пишет минимальный валидный ``package.yaml`` (без остальных артефактов)."""
    entry.mkdir(parents=True, exist_ok=True)
    manifest = PackageManifest(
        case_id=case_id,
        package_version=version,
        archetype=Archetype.B,
        modules=[ModuleDef(module_id="core", weight=100.0)],
        roles=[],
        baselines=[],
        baseline_participant=None,
    )
    save_artifact(entry, "package", manifest)
    return entry


def test_scan_packages_summarizes_and_orders(tmp_path: Path) -> None:
    _write_min_package(tmp_path / "case_b", "case_b")
    _write_min_package(tmp_path / "case_a", "case_a")
    summaries = scan_packages(tmp_path)
    assert [s.case_id for s in summaries] == ["case_a", "case_b"]
    first = summaries[0].as_dict()
    assert first["modules"] == 1 and first["catalog"] == 0
    assert first["ok"] is True and first["error"] is None


def test_scan_packages_excludes_template_by_default(tmp_path: Path) -> None:
    _write_min_package(tmp_path / "_template", "template")
    _write_min_package(tmp_path / "case_a", "case_a")
    assert [s.case_id for s in scan_packages(tmp_path)] == ["case_a"]
    included = scan_packages(tmp_path, include_template=True)
    assert {s.case_id for s in included} == {"case_a", "template"}


def test_scan_packages_skips_dirs_without_manifest_and_reports_errors(tmp_path: Path) -> None:
    _write_min_package(tmp_path / "case_ok", "case_ok")
    (tmp_path / "not_a_package").mkdir()
    broken = tmp_path / "case_broken"
    broken.mkdir()
    (broken / "package.yaml").write_text("modules: []\n", encoding="utf-8")  # невалиден
    summaries = scan_packages(tmp_path)
    by_id = {s.case_id: s for s in summaries}
    assert "not_a_package" not in by_id
    assert by_id["case_ok"].ok is True
    assert by_id["case_broken"].ok is False
    assert by_id["case_broken"].error


def test_scan_packages_filters_by_query_and_missing_dir(tmp_path: Path) -> None:
    _write_min_package(tmp_path / "case_alpha", "case_alpha")
    _write_min_package(tmp_path / "case_beta", "case_beta")
    assert [s.case_id for s in scan_packages(tmp_path, query="ALPHA")] == ["case_alpha"]
    assert [s.case_id for s in scan_packages(tmp_path, query="beta")] == ["case_beta"]
    assert scan_packages(tmp_path / "nope") == []
