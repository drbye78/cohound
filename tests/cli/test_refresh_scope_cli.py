"""``--refresh-scope`` (план 19 §5/P2): флаг прогона, а не отметка в БД.

Требования, каждое проверено вызовом:

* флаг объявлен **одинаково** у ``plan``/``run``/``crawl run`` (паритет): он входит в
  ``crawl_policy_digest``, поэтому разъехавшиеся формы дали бы разные ``run_id`` при одном
  объявленном входе;
* ``plan`` **печатает** запрос и его резолв, но не исполняет: сеть в тесте не участвует;
* ``--refresh-scope`` меняет решение (``reuse`` → ``refresh``) и **``run_id``**, а его
  отсутствие — нет: два прогона без флага дают тот же идентификатор;
* неразбираемая форма — ``EXIT_USAGE``, а не «запрос не совпал молча».

Тест держит ``state.db`` пакета: ``scheduled`` разрешается по часам, поэтому «свежее»
наблюдение кладётся явной строкой ``url_state`` (интервал 100 часов — не due).
"""

from __future__ import annotations

import datetime as _dt
import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.cli.parser import build_parser
from core.config.paths import resolve_paths
from core.ports.state import UrlStateRow
from core.store.state_sqlite import state_db_path
from core.store.state_store import SqliteStateStore

pytestmark = pytest.mark.usefixtures("isolated_state_dir")

FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
RUN_YAML = FIXTURES / "run.yaml"


def _package(tmp_path: Path, name: str = "scope_pkg") -> Path:
    """Минимальный пакет из фикстур + политика ``scheduled`` на host продукта ``p1``."""
    package = tmp_path / name
    package.mkdir()
    for filename in ("package.yaml", "catalog.csv", "baselines.csv", "cohort.yaml"):
        shutil.copy(FIXTURES / filename, package / filename)
    (package / "sources").mkdir()
    shutil.copy(FIXTURES / "sources" / "seeds.yaml", package / "sources" / "seeds.yaml")
    (package / "scope_attestation.yaml").write_text(
        "procedure: ручная аттестация полноты источника (тест)\n", encoding="utf-8"
    )
    (package / "crawl_policy.yaml").write_text(
        "sites:\n  p1:\n    p1.test: {mode: scheduled, interval_hours: 100}\n",
        encoding="utf-8",
    )
    return package


def _db_path(package: Path) -> Path:
    return state_db_path(resolve_paths().state, package.name)


def _seed_fresh_observation(package: Path) -> None:
    """Свежее наблюдение корня: ``scheduled`` с интервалом 100 ч — не due (reuse)."""
    db_path = _db_path(package)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteStateStore(db_path)
    try:
        store.upsert_url_state(
            UrlStateRow(
                product_id="p1",
                url="https://p1.test/",
                normalized_url="https://p1.test/",
                last_http_status=200,
                last_checked_at=_dt.datetime.now(_dt.UTC).isoformat(),
                last_changed_at=_dt.datetime.now(_dt.UTC).isoformat(),
                reuse_kind="live",
            )
        )
    finally:
        store.close()


def _plan(capsys: pytest.CaptureFixture[str], package: Path, *extra: str) -> Any:
    code = main(
        [
            "plan",
            "--run-yaml",
            str(RUN_YAML),
            "--package",
            str(package),
            "--json",
            *extra,
        ]
    )
    captured = capsys.readouterr()
    return code, json.loads(captured.out.strip().splitlines()[-1]), captured.err


def _subparser_args(command: str) -> dict[str, Any]:
    """Аргументы подкоманды из собранного парсера (по ``dest``)."""
    parser = build_parser()
    sub = next(
        action
        for action in parser._subparsers._group_actions  # type: ignore[union-attr]
        if hasattr(action, "choices") and action.choices
    )
    return {action.dest: action for action in sub.choices[command]._actions}


def _crawl_run_args() -> dict[str, Any]:
    """Аргументы ``crawl run``: ``crawl`` — вложенный парсер, не плоский ``dest``."""
    parser = build_parser()
    sub = next(
        action
        for action in parser._subparsers._group_actions  # type: ignore[union-attr]
        if hasattr(action, "choices") and action.choices
    )
    crawl = sub.choices["crawl"]
    nested = next(
        action
        for action in crawl._subparsers._group_actions  # type: ignore[union-attr]
        if hasattr(action, "choices") and action.choices
    )
    return {action.dest: action for action in nested.choices["run"]._actions}


def test_refresh_scope_flag_parity_plan_run_crawl() -> None:
    """Один флаг — три команды: ``plan``/``run``/``crawl run`` (тот же dest/дефолт)."""
    for name, args in (
        ("plan", _subparser_args("plan")),
        ("run", _subparser_args("run")),
        ("crawl run", _crawl_run_args()),
    ):
        assert "refresh_scope" in args, f"{name} не принимает --refresh-scope"
        assert args["refresh_scope"].default is None
        # Повторяемый: append, иначе второй запрос молча заменил бы первый.
        assert args["refresh_scope"].__class__.__name__ == "_AppendAction"


def test_refresh_scope_changes_run_id_and_absence_does_not(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Запрос меняет решение и ``run_id``; без запроса прогон детерминирован."""
    package = _package(tmp_path)
    _seed_fresh_observation(package)

    code, first, err = _plan(capsys, package)
    assert code == EXIT_OK, err
    code, second, err = _plan(capsys, package)
    assert code == EXIT_OK, err
    assert first["run_id"] == second["run_id"]

    decision = next(
        item for item in first["crawl_policy"]["decisions"] if item["product_id"] == "p1"
    )
    # Не due и запроса не было: обновление ждёт запроса, сеть запрещена.
    assert decision["decision"] == "reuse"
    assert decision["network_mode"] == "deny"
    assert decision["corpus_source"] == "chunks"
    assert decision["reason"] == "crawl_on_request_not_requested"

    code, requested, err = _plan(capsys, package, "--refresh-scope", "p1.test")
    assert code == EXIT_OK, err
    assert requested["crawl_policy"]["requested"] == ["p1.test"]
    decision = next(
        item
        for item in requested["crawl_policy"]["decisions"]
        if item["product_id"] == "p1"
    )
    assert decision["decision"] == "refresh"
    assert decision["network_mode"] == "fetch"
    assert decision["requested"] is True
    assert requested["run_id"] != first["run_id"]


def test_refresh_scope_unmatched_request_is_visible(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Запрос вне объявленных корней виден и идентичность не меняет."""
    package = _package(tmp_path)
    _seed_fresh_observation(package)
    code, plain, err = _plan(capsys, package)
    assert code == EXIT_OK, err
    code, stray, err = _plan(capsys, package, "--refresh-scope", "nowhere.test")
    assert code == EXIT_OK, err
    assert stray["crawl_policy"]["unmatched_requested"] == ["nowhere.test"]
    assert stray["run_id"] == plain["run_id"]


def test_bad_refresh_scope_is_exit_usage(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Неразбираемая форма — отказ использования с названным флагом."""
    package = _package(tmp_path)
    code, payload, _ = _plan(capsys, package, "--refresh-scope", "https://")
    assert code == EXIT_USAGE
    assert "refresh-scope" in json.dumps(payload, ensure_ascii=False)