"""Тесты ручек конвейера корпуса: ``--render-modes``, ``--cutoff-k`` и ``crawl:``.

Проверяют проводку ``hound run``:

* ``--render-modes FILE`` — JSON/YAML ``{url: "http"|"browser"}`` парсится в
  ``{url: RenderMode}`` и доходит до реестра фаз (Ш5); невалидный режим/форма/отсутствие
  файла → ``EXIT_USAGE`` (код ``render-modes``);
* ``--cutoff-k K`` — зафиксированный ``retrieval_cutoff_K`` (I20) доходит до фаз;
  неположительное/нечисловое значение отвергается ``argparse`` (``EXIT_USAGE``);
* секция ``crawl:`` из ``deploy.yaml`` применяется к ``HttpCrawler``
  (:func:`build_scout_crawler`/``build_adapters``); без конфига — дефолты адаптера.

Данные категорийно-нейтральны (§1.3); ``phases_factory`` — тестовый шов composition
root (прогон без внешних систем).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.adapters.crawl_http import HttpCrawler
from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.cli.wiring import (
    LOCAL_PROFILE,
    ENV_SCOUT_CRAWLER,
    build_adapters,
    build_scout_crawler,
)
from core.config.deploy import CrawlConfig, DeployConfig
from core.ports.types import RenderMode
from tests.helpers.packages import capturing_factory, run_argv, valid_package


# ---------------------------------------------------------------------------
# --render-modes
# ---------------------------------------------------------------------------


def test_render_modes_json_propagates(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    modes = tmp_path / "render.json"
    modes.write_text(
        json.dumps(
            {"https://docs.example/p1": "browser", "https://docs.example/p2": "http"}
        ),
        encoding="utf-8",
    )

    captured, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="knobs-1", db=db, until="plan"), "--render-modes", str(modes)], phases_factory=factory
    )
    assert code == EXIT_OK
    assert captured["render_modes"] == {
        "https://docs.example/p1": RenderMode.BROWSER,
        "https://docs.example/p2": RenderMode.HTTP,
    }


def test_render_modes_yaml_propagates(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    modes = tmp_path / "render.yaml"
    modes.write_text("https://docs.example/p1: browser\n", encoding="utf-8")

    captured, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="knobs-1", db=db, until="plan"), "--render-modes", str(modes)], phases_factory=factory
    )
    assert code == EXIT_OK
    assert captured["render_modes"] == {"https://docs.example/p1": RenderMode.BROWSER}


def test_render_modes_default_empty(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"

    captured, factory = capturing_factory()
    code = main(run_argv(package, run_id="knobs-1", db=db, until="plan"), phases_factory=factory)
    assert code == EXIT_OK
    assert captured["render_modes"] == {}


def test_render_modes_invalid_value_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    modes = tmp_path / "render.json"
    modes.write_text('{"https://docs.example/p1": "webkit"}', encoding="utf-8")

    captured, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="knobs-1", db=db, until="plan"), "--render-modes", str(modes)], phases_factory=factory
    )
    assert code == EXIT_USAGE
    out = capsys.readouterr()
    assert "render-modes" in out.out + out.err
    assert captured == {}  # фазы не собирались


def test_render_modes_missing_file_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"

    _, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="knobs-1", db=db, until="plan"), "--render-modes", str(tmp_path / "absent.json")],
        phases_factory=factory,
    )
    assert code == EXIT_USAGE
    out = capsys.readouterr()
    assert "render-modes" in out.out + out.err


@pytest.mark.parametrize("payload", ['["https://a"]', '{"https://a": 1}'])
def test_render_modes_bad_shape_usage_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    payload: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    modes = tmp_path / "render.json"
    modes.write_text(payload, encoding="utf-8")

    _, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="knobs-1", db=db, until="plan"), "--render-modes", str(modes)], phases_factory=factory
    )
    assert code == EXIT_USAGE
    out = capsys.readouterr()
    assert "render-modes" in out.out + out.err


def test_render_modes_dry_run_validates_and_succeeds(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Живая проверка: ``--dry-run`` с флагами не падает и печатает режимы."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    modes = tmp_path / "render.json"
    modes.write_text('{"https://docs.example/p1": "browser"}', encoding="utf-8")

    code = main(
        [
            *run_argv(package, run_id="knobs-1", db=db, until="plan"),
            "--dry-run",
            "--render-modes",
            str(modes),
            "--cutoff-k",
            "7",
        ]
    )
    assert code == EXIT_OK
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["dry_run"] is True
    assert payload["render_modes"] == {"https://docs.example/p1": "browser"}


def test_render_modes_dry_run_invalid_value_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    modes = tmp_path / "render.json"
    modes.write_text('{"https://docs.example/p1": "webkit"}', encoding="utf-8")

    code = main(
        [*run_argv(package, run_id="knobs-1", db=db, until="plan"), "--dry-run", "--render-modes", str(modes)]
    )
    assert code == EXIT_USAGE


# ---------------------------------------------------------------------------
# --cutoff-k
# ---------------------------------------------------------------------------


def test_cutoff_k_propagates(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"

    captured, factory = capturing_factory()
    code = main(
        [*run_argv(package, run_id="knobs-1", db=db, until="plan"), "--cutoff-k", "5"], phases_factory=factory
    )
    assert code == EXIT_OK
    assert captured["cutoff_k"] == 5


def test_cutoff_k_default_pipeline_value(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Без флага значение ``None`` → фаза оценки применяет ``DEFAULT_RETRIEVAL_CUTOFF_K``."""
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"

    captured, factory = capturing_factory()
    code = main(run_argv(package, run_id="knobs-1", db=db, until="plan"), phases_factory=factory)
    assert code == EXIT_OK
    assert captured["cutoff_k"] is None


@pytest.mark.parametrize("bad", ["0", "-3", "abc", "1.5"])
def test_cutoff_k_non_positive_rejected(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    bad: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"

    code = main([*run_argv(package, run_id="knobs-1", db=db, until="plan"), "--cutoff-k", bad])
    assert code == EXIT_USAGE


# ---------------------------------------------------------------------------
# crawl policy (deploy.yaml) → HttpCrawler
# ---------------------------------------------------------------------------


def test_crawl_policy_applied_by_scout_builder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ENV_SCOUT_CRAWLER, raising=False)
    cfg = DeployConfig(
        profile="local",
        crawl=CrawlConfig(
            user_agent="cohound-bot/2.0", timeout_s=12.5, follow_redirects=False
        ),
    )
    crawler = build_scout_crawler(deploy=cfg)
    assert isinstance(crawler, HttpCrawler)
    assert crawler.user_agent == "cohound-bot/2.0"
    assert crawler.timeout == 12.5
    assert crawler.follow_redirects is False


def test_scout_builder_defaults_without_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Профиль ``local`` без файла → дефолты ``HttpCrawler`` (обратная совместимость)."""
    monkeypatch.delenv(ENV_SCOUT_CRAWLER, raising=False)
    crawler = build_scout_crawler(deploy=DeployConfig(profile="local"))
    assert isinstance(crawler, HttpCrawler)
    assert crawler.user_agent == HttpCrawler().user_agent
    assert crawler.timeout == HttpCrawler().timeout
    assert crawler.follow_redirects is True


def test_local_adapters_build_crawler_from_deploy() -> None:
    cfg = DeployConfig(profile="local", crawl=CrawlConfig(user_agent="ua/9", timeout_s=9.0))
    ports = build_adapters(profile=LOCAL_PROFILE, deploy=cfg)
    assert isinstance(ports.crawler, HttpCrawler)
    assert ports.crawler.user_agent == "ua/9"
    assert ports.crawler.timeout == 9.0


def test_local_adapters_without_deploy_keep_crawler_none() -> None:
    ports = build_adapters(profile=LOCAL_PROFILE)
    assert ports.crawler is None
