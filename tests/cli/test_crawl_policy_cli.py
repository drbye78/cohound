"""CLI ``hound crawl policy`` и влияние политики на ``plan`` (план 19 P1).

Проверяется: append-only запись под локом пакета, идемпотентность, ``--dry-run`` без
записи, эффективный режим с источником в ``list`` и разные ``run_id`` у двух политик.
Сеть не участвует: P1 политику только разрешает и печатает.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.config.paths import resolve_paths
from core.domain.enums import CrawlPolicyMode, CrawlPolicyScopeKind
from core.ports.state import CrawlPolicyRow
from core.store.state_sqlite import state_db_path
from core.store.state_store import SqliteStateStore

pytestmark = pytest.mark.usefixtures("isolated_state_dir")

FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
RUN_YAML = FIXTURES / "run.yaml"


def _package(tmp_path: Path, name: str = "policy_pkg") -> Path:
    """Минимальный пакет из фикстур + чистая процедура аттестации охвата."""
    package = tmp_path / name
    package.mkdir()
    for filename in ("package.yaml", "catalog.csv", "baselines.csv", "cohort.yaml"):
        shutil.copy(FIXTURES / filename, package / filename)
    (package / "sources").mkdir()
    shutil.copy(FIXTURES / "sources" / "seeds.yaml", package / "sources" / "seeds.yaml")
    (package / "scope_attestation.yaml").write_text(
        "procedure: ручная аттестация полноты источника (тест)\n", encoding="utf-8"
    )
    return package


def _db_path(package: Path) -> Path:
    return state_db_path(resolve_paths().state, package.name)


def _run(capsys: pytest.CaptureFixture[str], argv: list[str]) -> tuple[int, str, str]:
    code = main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _policy_argv(package: Path, *rest: str) -> list[str]:
    return ["crawl", "policy", *rest, "--package", str(package), "--json"]


def _set(
    capsys: pytest.CaptureFixture[str],
    package: Path,
    *,
    mode: str,
    scope: str = "product",
    ident: str = "p1",
    extra: list[str] | None = None,
) -> tuple[int, dict[str, object], str]:
    code, out, err = _run(
        capsys,
        _policy_argv(
            package,
            "set",
            "--actor",
            "roger",
            "--scope",
            scope,
            "--id",
            ident,
            "--mode",
            mode,
            *(extra or ()),
        ),
    )
    payload = json.loads(out) if out.strip() else {}
    return code, payload, err


def test_set_dry_run_writes_nothing_then_real_write_then_dry_run_noop(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _package(tmp_path)
    db_path = _db_path(package)

    code, payload, _ = _set(
        capsys, package, mode="network_denied", extra=["--dry-run"]
    )
    assert code == EXIT_OK
    assert payload["dry_run"] is True
    assert payload["created"] is True
    assert not db_path.exists(), "dry-run не должен создавать state.db"
    assert any("dry-run" in warning for warning in payload["warnings"])

    code, payload, _ = _set(capsys, package, mode="network_denied")
    assert code == EXIT_OK and payload["created"] is True
    ro = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        count_before = ro.execute("SELECT COUNT(*) FROM crawl_policy").fetchone()[0]
        version_before = ro.execute(
            "SELECT value FROM schema_meta WHERE key='schema_version'"
        ).fetchone()[0]
    finally:
        ro.close()
    assert count_before == 1

    code, payload, _ = _set(
        capsys, package, mode="auto", extra=["--dry-run"]
    )
    assert code == EXIT_OK and payload["dry_run"] is True
    ro = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        assert ro.execute("SELECT COUNT(*) FROM crawl_policy").fetchone()[0] == count_before
        assert (
            ro.execute(
                "SELECT value FROM schema_meta WHERE key='schema_version'"
            ).fetchone()[0]
            == version_before
        )
    finally:
        ro.close()


def test_set_is_idempotent_and_mode_change_is_append_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _package(tmp_path)
    code, first, _ = _set(capsys, package, mode="network_denied")
    assert code == EXIT_OK and first["created"] is True
    code, second, _ = _set(capsys, package, mode="network_denied")
    assert code == EXIT_OK and second["created"] is False
    assert second["policy_id"] == first["policy_id"]

    code, third, _ = _set(capsys, package, mode="auto")
    assert code == EXIT_OK and third["created"] is True
    assert third["superseded"] == [first["policy_id"]]

    store = SqliteStateStore(_db_path(package))
    try:
        active = store.list_crawl_policy()
        assert [row.mode for row in active] == [CrawlPolicyMode.AUTO]
        assert len(store.list_crawl_policy(include_superseded=True)) == 2
    finally:
        store.close()


def test_list_shows_effective_mode_and_source(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _package(tmp_path)
    _set(capsys, package, mode="network_denied")
    code, out, _ = _run(capsys, _policy_argv(package, "list"))
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["digest"]
    decision = next(
        item for item in payload["decisions"] if item["product_id"] == "p1"
    )
    assert decision["mode"] == "network_denied"
    assert decision["source"] == "operator"
    assert payload["records"][0]["actor"] == "roger"


def test_clear_dry_run_then_clear(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _package(tmp_path)
    _set(capsys, package, mode="network_denied")
    code, out, _ = _run(
        capsys, _policy_argv(package, "clear", "--actor", "roger", "--dry-run")
    )
    assert code == EXIT_OK and json.loads(out)["count"] == 1
    store = SqliteStateStore(_db_path(package))
    try:
        assert len(store.list_crawl_policy()) == 1, "dry-run ничего не отозвал"
    finally:
        store.close()

    code, out, _ = _run(capsys, _policy_argv(package, "clear", "--actor", "roger"))
    assert code == EXIT_OK and json.loads(out)["count"] == 1
    store = SqliteStateStore(_db_path(package))
    try:
        assert store.list_crawl_policy() == []
        assert len(store.list_crawl_policy(include_superseded=True)) == 1
    finally:
        store.close()


def test_plan_two_policies_give_different_run_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _package(tmp_path)
    code, out, err = _run(
        capsys,
        ["plan", "--run-yaml", str(RUN_YAML), "--package", str(package), "--json"],
    )
    assert code == EXIT_OK, err
    baseline = json.loads(out)

    (package / "crawl_policy.yaml").write_text(
        "products:\n  p1: {mode: network_denied}\n", encoding="utf-8"
    )
    code, out, err = _run(
        capsys,
        ["plan", "--run-yaml", str(RUN_YAML), "--package", str(package), "--json"],
    )
    assert code == EXIT_OK, err
    changed = json.loads(out)

    assert baseline["run_id"] != changed["run_id"]
    assert baseline["crawl_policy"]["digest"] != changed["crawl_policy"]["digest"]
    decision = next(
        item for item in changed["crawl_policy"]["decisions"] if item["product_id"] == "p1"
    )
    assert decision["decision"] == "network_denied"
    assert decision["source"] == "package"


def test_plan_prints_resolution_without_executing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _package(tmp_path)
    (package / "crawl_policy.yaml").write_text(
        "sites:\n  p1:\n    p1.test: {mode: on_request}\n", encoding="utf-8"
    )
    code, out, err = _run(
        capsys, ["plan", "--run-yaml", str(RUN_YAML), "--package", str(package)]
    )
    assert code == EXIT_OK, err
    assert "crawl policy: digest=" in out
    assert "mode=on_request" in out
    # Несовпавшая строка видна токеном, а не тонет в тишине.
    assert "crawl_policy_unmatched: 0" in out


def test_plan_conflict_is_exit_usage_with_both_records(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    package = _package(tmp_path)
    db_path = _db_path(package)
    db_path.parent.mkdir(parents=True)
    store = SqliteStateStore(db_path)
    try:
        store.record_crawl_policy(
            CrawlPolicyRow(
                product_id="p1",
                scope_kind=CrawlPolicyScopeKind.PRODUCT,
                mode=CrawlPolicyMode.AUTO,
                actor="alice",
                created_at="2026-11-01T00:00:00Z",
            )
        )
        store.record_crawl_policy(
            CrawlPolicyRow(
                product_id="p1",
                scope_kind=CrawlPolicyScopeKind.PRODUCT,
                mode=CrawlPolicyMode.NETWORK_DENIED,
                actor="bob",
                created_at="2026-11-02T00:00:00Z",
            )
        )
    finally:
        store.close()

    code, out, _err = _run(
        capsys,
        ["plan", "--run-yaml", str(RUN_YAML), "--package", str(package), "--json"],
    )
    assert code == EXIT_USAGE
    payload = json.loads(out)
    assert payload["error"] == "crawl_policy_conflict"
    message = payload["message"]
    assert "crawl_policy_conflict" in message
    assert "policy_id=1" in message and "policy_id=2" in message
    assert "auto" in message and "network_denied" in message


def test_unknown_refresh_policy_is_an_error_not_auto() -> None:
    from core.pipeline.corpus import _as_refresh_policy

    with pytest.raises(ValueError, match="неизвестная политика"):
        _as_refresh_policy("bogus")