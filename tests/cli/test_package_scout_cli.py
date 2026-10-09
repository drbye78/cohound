"""Тесты CLI-команды ``hound package scout`` (``03`` §E.1).

Без сети/ключей: ``discovery_llm``/``crawler``/``extractor`` инъектируются в :func:`main`.
Покрываются: отсутствие LLM-конфигурации → ``EXIT_USAGE`` (``llm``), недоступный поиск →
``EXIT_ERROR`` (``I19``), ``--dry-run`` ничего не пишет, happy-path пишет кандидатов +
provenance (apply-совместимо), частичный ``ok=False`` всё равно пишет (OQ-3), битый
``--package``/``--only`` → ``EXIT_USAGE``.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from core.adapters.llm_discovery import DiscoveryLLM
from core.cli.main import EXIT_ERROR, EXIT_OK, EXIT_USAGE, main
from core.pipeline.package_candidates import loads_candidates
from core.ports.types import RawPage, RenderMode
from tests.fakes.ports import FakeCrawler, FakeExtractor

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_PACKAGE = ROOT / "packages" / "_template"
FIXTURES = Path(__file__).resolve().parent.parent / "pipeline" / "fixtures" / "package_scout"
URL = "https://docs.test/alpha"
PAGE_TEXT = (
    "Alpha Product documentation.\n"
    "The Alpha Product is built on the Open Core engine, an open-source upstream.\n"
    "Vendor: ACME.\n"
    "The Alpha Product provides Backup and Restore with verified snapshots.\n"
    "It supports Query Planner hints for parallel queries.\n"
)
FIXED_TS = "2026-01-01T00:00:00+00:00"

_ENV_NAMES = (
    "COHOUND_LLM_BASE_URL",
    "COHOUND_LLM_API_KEY",
    "COHOUND_LLM_MODEL",
    "COHOUND_LLM_DECODER",
    "COHOUND_LLM_TIMEOUT",
    "COHOUND_LLM_MAX_TOKENS",
    "YANDEX_SEARCH_API_KEY",
    "YANDEX_FOLDER_ID",
    "GLM_SEARCH_API_KEY",
    "GOOGLE_API_KEY",
    "GOOGLE_CSE_ID",
)


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def _fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _llm(*, fail_catalog: bool = False) -> DiscoveryLLM:
    landscape = _fixture("landscape.json")
    catalog = _fixture("catalog.json")

    def generate(prompt: str, schema: type) -> str:
        if schema.__name__ == "ScoutLandscapeHypothesis":
            return json.dumps(landscape, ensure_ascii=False)
        if fail_catalog:
            raise RuntimeError("no backend")
        return json.dumps(catalog, ensure_ascii=False)

    return DiscoveryLLM(generate=generate, model_id="m1", decoder="none")


def _crawler() -> FakeCrawler:
    page = RawPage(
        url=URL,
        content=PAGE_TEXT.encode("utf-8"),
        fetched_at=FIXED_TS,
        render_mode=RenderMode.HTTP,
    )
    return FakeCrawler({URL: page})


def _package(tmp_path: Path) -> Path:
    dest = tmp_path / "package"
    shutil.copytree(TEMPLATE_PACKAGE, dest)
    return dest


def _json_out(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])


def _base_argv(package: Path, out: Path, *extra: str, dry_run: bool = False) -> list[str]:
    argv = [
        "package",
        "scout",
        "--package",
        str(package),
        "--subject-name",
        "Alpha Product",
        "--out",
        str(out),
        "--no-search",
        "--url",
        URL,
    ]
    if dry_run:
        argv.append("--dry-run")
    argv.extend(extra)
    return argv


# ---------------------------------------------------------------------------
# Конфигурация / ошибки входа
# ---------------------------------------------------------------------------


def test_missing_llm_config_is_usage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
    package = _package(tmp_path)
    code = main(
        _base_argv(package, tmp_path / "out", "--dry-run"),
        discovery_llm=DiscoveryLLM(base_url="", model_id=""),
        crawler=_crawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_USAGE
    assert "llm" in capsys.readouterr().err


def test_missing_package_is_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(
        _base_argv(tmp_path / "nope", tmp_path / "out", "--dry-run"),
        discovery_llm=_llm(),
        crawler=_crawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_USAGE
    assert "package" in capsys.readouterr().err


def test_invalid_only_is_usage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
    package = _package(tmp_path)
    code = main(
        _base_argv(package, tmp_path / "out", "--only", "bogus"),
        discovery_llm=_llm(),
        crawler=_crawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_USAGE
    assert "only" in capsys.readouterr().err


def test_search_without_keys_is_error_not_fabrication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
    package = _package(tmp_path)
    argv = [
        "package",
        "scout",
        "--package",
        str(package),
        "--subject-name",
        "Alpha Product",
        "--out",
        str(tmp_path / "out"),
        "--json",
    ]
    code = main(argv, discovery_llm=_llm(), crawler=_crawler(), extractor=FakeExtractor())
    assert code == EXIT_ERROR
    payload = _json_out(capsys)
    assert payload["error"] == "I19"


# ---------------------------------------------------------------------------
# dry-run / happy-path
# ---------------------------------------------------------------------------


def test_dry_run_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
    package = _package(tmp_path)
    out = tmp_path / "out"
    code = main(
        _base_argv(package, out, "--json", dry_run=True),
        discovery_llm=_llm(),
        crawler=_crawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload["dry_run"] is True
    assert "candidates_path" not in payload
    assert not out.exists()


def test_happy_path_writes_candidates_and_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
    package = _package(tmp_path)
    out = tmp_path / "out"
    code = main(
        _base_argv(package, out, "--json"),
        discovery_llm=_llm(),
        crawler=_crawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload["ok"] is True
    assert payload["candidates"] > 0
    candidates_path = Path(payload["candidates_path"])
    provenance_path = Path(payload["provenance_path"])
    assert candidates_path.is_file() and provenance_path.is_file()
    parsed = loads_candidates(candidates_path.read_text(encoding="utf-8"))
    assert all(c.needs_human for c in parsed)
    sidecar = json.loads(provenance_path.read_text(encoding="utf-8"))
    assert sidecar["fetched"][0]["url"] == URL
    # Пакет не изменён (scout пишет только в sources/scout, OQ-4/I19).
    assert (package / "package.yaml").is_file()
    assert not (package / "catalog.csv").read_text(encoding="utf-8").count("backup_and_restore")


def test_partial_failure_still_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
    package = _package(tmp_path)
    out = tmp_path / "out"
    code = main(
        _base_argv(package, out, "--json"),
        discovery_llm=_llm(fail_catalog=True),
        crawler=_crawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_ERROR
    payload = _json_out(capsys)
    assert payload["ok"] is False
    # OQ-3: частичные кандидаты записаны несмотря на ok=False.
    assert Path(payload["candidates_path"]).is_file()
    assert payload["errors"]


def test_only_catalog_runs_without_landscape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
    package = _package(tmp_path)
    out = tmp_path / "out"
    code = main(
        _base_argv(package, out, "--json", "--only", "catalog", dry_run=True),
        discovery_llm=_llm(),
        crawler=_crawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload["ok"] is True


def test_human_output_does_not_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
    package = _package(tmp_path)
    code = main(
        _base_argv(package, tmp_path / "out", dry_run=True),
        discovery_llm=_llm(),
        crawler=_crawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "subject_name: Alpha Product" in out
    assert "ok: yes" in out
