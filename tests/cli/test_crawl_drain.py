"""Тесты ``hound crawl drain`` — докачка усечённых корней и правка пределов.

Сеть не нужна: проход краула подменяется фейком, который пишет **свой** прогон с
провенансом. Так проверяется то, что дороже всего ошибить молча, — лестница глубины
(объявленная глубина следует за **пройденной**, а не за прежним объявлением) и правило
«профиль правится только по докачанному корню».
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
from pathlib import Path

import pytest

from core.cli.commands import crawl_drain
from core.cli.errors import CliError
from core.domain import (
    CrawlSession,
    CrawlSessionStatus,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.pipeline.crawl_session import default_session_id
from core.pipeline.drain_plan import TruncatedRoot
from core.store.sqlite_store import SqliteStore

_PACKAGE_PROFILES = """\
# Шапка пакета: обоснование замеров.
a.test/docs:
  max_depth: 8
  max_documents: 100  # замер: 95 документов
b.test:
  max_depth: 2
  max_documents: 50
"""


# --------------------------------------------------------------------------- #
# Фикстуры: прогон с провенансом
# --------------------------------------------------------------------------- #


def _run_cfg() -> RunConfig:
    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1"],
        package="pkg-1",
        focus=Focus(modules=["m1"], params=["param.a"]),
        languages=["ru"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _write_run(runs_dir: Path, run_id: str, roots: list[dict[str, object]]) -> None:
    """Создаёт ``runs/{run_id}/cohound.db`` с сеансом, чей провенанс несёт ``roots``."""
    db_path = runs_dir / run_id / "cohound.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with SqliteStore(db_path) as store:
        store.create_run(run_id, _run_cfg())
        session_id = default_session_id(run_id)
        store.upsert_crawl_session(
            CrawlSession(
                session_id=session_id,
                run_id=run_id,
                profile="local",
                status=CrawlSessionStatus.RUNNING,
                created_at="2026-01-01T00:00:00Z",
                started_at=f"2026-01-0{len(run_id) % 9 + 1}T00:00:00Z",
            )
        )
        store.set_crawl_session_provenance(
            session_id,
            json.dumps(
                {
                    "status": "partial",
                    "truncated": True,
                    "effective_limits": {"roots": roots},
                }
            ),
        )


def _root_record(
    product_id: str,
    url: str,
    *,
    reason: str | None,
    truncated: bool = True,
    pages_fetched: int = 100,
    replayed: int = 0,
    max_depth: int | None = 8,
    max_documents: int | None = 100,
    queue_remaining: int | None = 5,
) -> dict[str, object]:
    return {
        "product_id": product_id,
        "root": url,
        "truncated": truncated,
        "truncated_reason": reason,
        "pages_fetched": pages_fetched,
        "replayed": replayed,
        "queue_remaining": queue_remaining,
        "limits": {"max_depth": max_depth, "max_documents": max_documents},
    }


@pytest.fixture()
def package(tmp_path: Path) -> Path:
    pkg = tmp_path / "pkg"
    (pkg / "sources").mkdir(parents=True)
    (pkg / "domain_profiles.yaml").write_text(_PACKAGE_PROFILES, encoding="utf-8")
    (pkg / "run.yaml").write_text("case_id: case-1\n", encoding="utf-8")
    return pkg


def _args(pkg: Path, runs_dir: Path, **overrides: object) -> argparse.Namespace:
    base: dict[str, object] = {
        "package": str(pkg),
        "run_yaml": str(pkg / "run.yaml"),
        "urls": None,
        "from_run": None,
        "product": None,
        "root": None,
        "margin_percent": 10,
        "depth_step": 1,
        "max_passes": 3,
        "list_only": False,
        "apply": False,
        "actor": None,
        "runs_dir": str(runs_dir),
        "json": False,
        "profile": "local",
        "env_file": None,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


# --------------------------------------------------------------------------- #
# Чистые помощники
# --------------------------------------------------------------------------- #


def test_profile_keys_follow_file_order(package: Path) -> None:
    assert crawl_drain._profile_keys(package / "domain_profiles.yaml") == [
        "a.test/docs",
        "b.test",
    ]


def test_profile_keys_rejects_non_mapping(tmp_path: Path) -> None:
    bad = tmp_path / "domain_profiles.yaml"
    bad.write_text("- a\n- b\n", encoding="utf-8")
    with pytest.raises(CliError):
        crawl_drain._profile_keys(bad)


def _truncated(key_url: str = "https://a.test/docs/") -> TruncatedRoot:
    return TruncatedRoot(
        product_id="p1",
        root_url=key_url,
        reason="max_documents",
        pages_fetched=100,
        queue_remaining=5,
        max_depth=8,
        max_documents=100,
    )


def test_select_all_by_default() -> None:
    roots = [_truncated(), _truncated("https://b.test/")]
    assert crawl_drain._select(roots, products=None, keys=None) == roots


def test_select_by_product_and_key() -> None:
    roots = [_truncated(), _truncated("https://b.test/")]
    assert [r.key for r in crawl_drain._select(roots, products=None, keys=["b.test"])] == [
        "b.test"
    ]
    assert crawl_drain._select(roots, products=["nope"], keys=None) == []


def test_select_rejects_unknown_key_instead_of_empty_selection() -> None:
    with pytest.raises(CliError):
        crawl_drain._select([_truncated()], products=None, keys=["nope.test"])


def test_pass_argv_always_names_the_runs_root(package: Path, tmp_path: Path) -> None:
    # Дочерний Namespace собирается парсером без ``args.paths``: опущенный
    # ``--runs-dir`` увёл бы проход в литерал ``runs``, то есть в чужой каталог.
    argv = crawl_drain._pass_argv(
        _args(package, tmp_path / "nowhere"), keys=["a.test"], depth=None,
        runs_root=tmp_path / "resolved",
    )
    assert argv[argv.index("--runs-dir") + 1] == str(tmp_path / "resolved")
    assert "--crawl-only" in argv and argv[argv.index("--until") + 1] == "corpus"
    assert argv[argv.index("--refresh") + 1] == "drain"
    assert argv[-2:] == ["--drain-root", "a.test"]


def test_pass_argv_adds_depth_only_when_declared(package: Path, tmp_path: Path) -> None:
    args = _args(package, tmp_path)
    assert "--drain-max-depth" not in crawl_drain._pass_argv(
        args, keys=["a.test"], depth=None, runs_root=tmp_path
    )
    argv = crawl_drain._pass_argv(args, keys=["a.test"], depth=9, runs_root=tmp_path)
    assert argv[argv.index("--drain-max-depth") + 1] == "9"


def test_not_drainable_note_names_the_way_out() -> None:
    # Отказ без выхода — это отказ без ответа: у каждой причины назван другой механизм.
    assert "crawl policy clear" in crawl_drain._not_drainable_note("crawl_policy_skipped")
    assert "artifacts fetch" in crawl_drain._not_drainable_note("blocked")
    assert "диск" in crawl_drain._not_drainable_note("disk_low_headroom")
    assert "не записана" in crawl_drain._not_drainable_note(None)
    assert "не про потолок" in crawl_drain._not_drainable_note("something_new")


def test_group_by_depth_none_first_then_ascending() -> None:
    pending = {
        ("p1", "https://a.test/docs/"): 9,
        ("p1", "https://b.test/"): None,
        ("p1", "https://c.test/"): 10,
    }
    groups = crawl_drain._group_by_depth(pending)
    assert [depth for depth, _ in groups] == [None, 9, 10]
    assert groups[0][1] == [("p1", "https://b.test/")]


# --------------------------------------------------------------------------- #
# Тело команды
# --------------------------------------------------------------------------- #


def test_apply_requires_actor(package: Path, tmp_path: Path) -> None:
    with pytest.raises(CliError) as exc:
        crawl_drain.run(_args(package, tmp_path / "runs", apply=True))
    assert exc.value.exit_code != 0


def test_missing_profiles_is_usage_error(tmp_path: Path) -> None:
    pkg = tmp_path / "empty"
    pkg.mkdir()
    with pytest.raises(CliError):
        crawl_drain.run(_args(pkg, tmp_path / "runs"))


def test_no_runs_is_usage_error(package: Path, tmp_path: Path) -> None:
    with pytest.raises(CliError):
        crawl_drain.run(_args(package, tmp_path / "runs"))


def test_not_drainable_reason_is_named_and_file_untouched(
    package: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    runs = tmp_path / "runs"
    _write_run(
        runs,
        "run-a",
        [
            _root_record(
                "p1", "https://a.test/docs/", reason="round_budget", pages_fetched=7
            )
        ],
    )
    before = (package / "domain_profiles.yaml").read_text(encoding="utf-8")
    assert crawl_drain.run(_args(package, runs, from_run="run-a")) == 0
    assert (package / "domain_profiles.yaml").read_text(encoding="utf-8") == before
    out = capsys.readouterr().out
    assert "not_drainable:round_budget" in out
    assert "докачивать нечего" in out


def test_list_only_never_runs_a_pass(
    package: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    runs = tmp_path / "runs"
    _write_run(
        runs,
        "run-a",
        [
            _root_record("p1", "https://a.test/docs/", reason="max_documents"),
            _root_record("p1", "https://b.test/", reason="disk_low_headroom"),
        ],
    )
    before = (package / "domain_profiles.yaml").read_text(encoding="utf-8")

    def boom(*_a: object, **_k: object):
        raise AssertionError("--list не должен исполнять проход краула")

    monkeypatch.setattr(crawl_drain, "_execute_pass", boom)
    assert crawl_drain.run(_args(package, runs, from_run="run-a", list_only=True)) == 0
    out = capsys.readouterr().out
    assert "усечённых корней: 2; докачиваемых: 1" in out
    # Сводка по причинам называет и число, и что с этим делать.
    assert "причина disk_low_headroom: 1" in out
    assert "освободите место" in out
    assert "докачать p1/a.test/docs" in out
    assert (package / "domain_profiles.yaml").read_text(encoding="utf-8") == before


def test_drain_then_apply_writes_measured_plus_margin(
    package: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs = tmp_path / "runs"
    _write_run(
        runs, "run-a", [_root_record("p1", "https://a.test/docs/", reason="max_documents")]
    )

    def fake_pass(args: argparse.Namespace, *, keys: list[str], depth: int | None, runs_root: Path):
        # Докачали: корень исчерпан, 1 000 документов, 900 из них — из байтов.
        _write_run(
            runs,
            "run-pass",
            [
                _root_record(
                    "p1",
                    "https://a.test/docs/",
                    reason=None,
                    truncated=False,
                    pages_fetched=100,
                    replayed=900,
                    max_documents=None,
                )
            ],
        )
        return 0, {"run_id": "run-pass", "session_status": "completed"}

    monkeypatch.setattr(crawl_drain, "_execute_pass", fake_pass)
    code = crawl_drain.run(
        _args(package, runs, from_run="run-a", apply=True, actor="roger")
    )
    assert code == 0
    data = (package / "domain_profiles.yaml").read_text(encoding="utf-8")
    # 1 000 измерено → ceil(1 000 × 1.1) = 1 100; шапка и комментарий сохранены.
    assert "max_documents: 1100  # замер: 95 документов" in data
    assert data.startswith("# Шапка пакета: обоснование замеров.")
    assert "max_documents: 50" in data  # соседний корень не тронут
    artifact = package / "sources" / "crawl_drain"
    (written,) = list(artifact.iterdir())
    payload = json.loads(written.read_text(encoding="utf-8"))
    assert payload["actor"] == "roger"
    assert payload["margin_percent"] == 10
    assert payload["updates"][0]["measured"] == 1000
    assert payload["updates"][0]["max_documents"] == 1100


def test_depth_ladder_declares_walked_depth_not_previous_plus_step(
    package: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs = tmp_path / "runs"
    _write_run(
        runs,
        "run-a",
        [
            _root_record(
                "p1", "https://a.test/docs/", reason="max_depth", max_documents=None
            )
        ],
    )
    seen: list[int | None] = []

    def fake_pass(args: argparse.Namespace, *, keys: list[str], depth: int | None, runs_root: Path):
        seen.append(depth)
        # Первый проход на 9 — всё ещё усечён глубиной; второй на 10 — исчерпан.
        exhausted = depth == 10
        _write_run(
            runs,
            f"run-pass-{depth}",
            [
                _root_record(
                    "p1",
                    "https://a.test/docs/",
                    reason=None if exhausted else "max_depth",
                    truncated=not exhausted,
                    pages_fetched=10,
                    replayed=140,
                    max_depth=depth,
                    max_documents=None,
                )
            ],
        )
        return 0, {"run_id": f"run-pass-{depth}", "session_status": "partial"}

    monkeypatch.setattr(crawl_drain, "_execute_pass", fake_pass)
    assert (
        crawl_drain.run(
            _args(package, runs, from_run="run-a", apply=True, actor="roger")
        )
        == 0
    )
    assert seen == [9, 10]
    data = (package / "domain_profiles.yaml").read_text(encoding="utf-8")
    # Объявлена ПРОЙДЕННАЯ глубина 10, а не «прежний 8 + шаг» = 9 (иначе новое усечение).
    assert "max_depth: 10" in data
    assert "max_depth: 2" in data  # соседний корень не тронут


def test_ladder_exhausted_leaves_profile_untouched(
    package: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs = tmp_path / "runs"
    _write_run(
        runs,
        "run-a",
        [
            _root_record(
                "p1", "https://a.test/docs/", reason="max_depth", max_documents=None
            )
        ],
    )
    before = (package / "domain_profiles.yaml").read_text(encoding="utf-8")

    def fake_pass(args: argparse.Namespace, *, keys: list[str], depth: int | None, runs_root: Path):
        _write_run(
            runs,
            f"run-pass-{depth}",
            [
                _root_record(
                    "p1",
                    "https://a.test/docs/",
                    reason="max_depth",
                    max_depth=depth,
                    max_documents=None,
                )
            ],
        )
        return 0, {"run_id": f"run-pass-{depth}", "session_status": "partial"}

    monkeypatch.setattr(crawl_drain, "_execute_pass", fake_pass)
    assert (
        crawl_drain.run(
            _args(package, runs, from_run="run-a", max_passes=1, apply=True, actor="x")
        )
        == 0
    )
    # Корень не докачан: предел по недокачанному наблюдению не объявляется.
    assert (package / "domain_profiles.yaml").read_text(encoding="utf-8") == before
    assert not (package / "sources" / "crawl_drain").exists()


def test_failed_pass_is_named_and_not_declared(
    package: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runs = tmp_path / "runs"
    _write_run(
        runs, "run-a", [_root_record("p1", "https://a.test/docs/", reason="max_documents")]
    )
    before = (package / "domain_profiles.yaml").read_text(encoding="utf-8")

    def fake_pass(args: argparse.Namespace, *, keys: list[str], depth: int | None, runs_root: Path):
        return 1, {"run_id": "run-broken", "session_status": "failed"}

    monkeypatch.setattr(crawl_drain, "_execute_pass", fake_pass)
    assert crawl_drain.run(_args(package, runs, from_run="run-a")) == 0
    assert (package / "domain_profiles.yaml").read_text(encoding="utf-8") == before


def test_json_output_carries_passes_and_remaining(
    package: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    runs = tmp_path / "runs"
    _write_run(
        runs,
        "run-a",
        [
            _root_record("p1", "https://a.test/docs/", reason="max_documents"),
            _root_record("p1", "https://b.test/", reason="disk_low_headroom"),
        ],
    )

    def fake_pass(args: argparse.Namespace, *, keys: list[str], depth: int | None, runs_root: Path):
        _write_run(
            runs,
            "run-pass",
            [
                _root_record(
                    "p1",
                    "https://a.test/docs/",
                    reason=None,
                    truncated=False,
                    pages_fetched=5,
                    replayed=5,
                    max_documents=None,
                )
            ],
        )
        return 0, {"run_id": "run-pass", "session_status": "completed"}

    monkeypatch.setattr(crawl_drain, "_execute_pass", fake_pass)
    crawl_drain.run(_args(package, runs, from_run="run-a", json=True))
    payload = json.loads(capsys.readouterr().out)
    assert payload["passes"][0]["run_id"] == "run-pass"
    assert [item["reason"] for item in payload["remaining"]] == ["not_drainable:disk_low_headroom"]
    assert payload["drained"][0]["documents"] == 10
