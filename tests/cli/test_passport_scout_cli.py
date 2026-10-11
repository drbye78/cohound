"""Тесты CLI-команды ``passport-scout`` (M1, ``03`` §D.6/E.1).

Без сети/ключей: ``discovery_llm``/``crawler``/``extractor`` инъектируются в
:func:`main`. Покрываются: отсутствие LLM-конфигурации → ``EXIT_USAGE`` (код ``llm``),
``--dry-run`` ничего не пишет, happy-path пишет YAML+sidecar, ``--json``-форма,
``--package`` c ролями (и битым пакетом → ``EXIT_USAGE``).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.adapters.llm_discovery import DiscoveryLLM
from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.ports.types import FetchMode, FetchPlan, RawPage
from tests.fakes.ports import FakeExtractor
from tests.helpers.output import json_out
from tests.helpers.scout import clear_env, fake_crawler

URL = "https://docs.test/alpha"
PAGE_TEXT = "The Alpha Product supports Feature X natively."
FIXTURES = Path(__file__).resolve().parent.parent / "pipeline" / "fixtures" / "passport_scout"
PACKAGE_FIXTURE = Path(__file__).resolve().parent.parent / "config" / "fixtures"

_ENV_NAMES = (
    "COHOUND_LLM_BASE_URL",
    "COHOUND_LLM_API_KEY",
    "COHOUND_LLM_MODEL",
    "COHOUND_LLM_DECODER",
    "COHOUND_LLM_TIMEOUT",
    "COHOUND_LLM_MAX_TOKENS",
)


def _hypothesis() -> dict:
    return json.loads((FIXTURES / "hypothesis.json").read_text(encoding="utf-8"))


def _llm() -> DiscoveryLLM:
    payload = json.dumps(_hypothesis(), ensure_ascii=False)
    return DiscoveryLLM(generate=lambda p, s: payload, model_id="m1")


# ---------------------------------------------------------------------------
# Конфигурация LLM
# ---------------------------------------------------------------------------


def test_missing_llm_config_is_usage_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Явно пустой адаптер (нет base_url/model/шва) → понятная ошибка ``llm``."""
    clear_env(monkeypatch, _ENV_NAMES)
    unconfigured = DiscoveryLLM(base_url="", model_id="")
    code = main(
        ["passport-scout", "--product-name", "X", "--url", URL],
        discovery_llm=unconfigured,
    )
    assert code == EXIT_USAGE
    err = capsys.readouterr().err
    assert "llm" in err


def test_wiring_defaults_and_env_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Без env: base_url пуст, model/decoder — дефолты; env задаёт base_url и ключ."""
    clear_env(monkeypatch, _ENV_NAMES)
    from core.cli.wiring import build_discovery_llm

    llm = build_discovery_llm()
    assert llm.base_url == ""
    assert llm.model_id == "deepseek-v4.1-flash:cxbc"
    assert llm.decoder == "json_object"
    assert llm.api_key is None

    monkeypatch.setenv("COHOUND_LLM_BASE_URL", "https://llm.internal/v1")
    monkeypatch.setenv("COHOUND_LLM_API_KEY", "primary-key")
    configured = build_discovery_llm()
    assert configured.base_url == "https://llm.internal/v1"
    assert configured.api_key == "primary-key"


def test_llm_flags_override_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI-флаги переопределяют env при сборке адаптера (без сети)."""
    clear_env(monkeypatch, _ENV_NAMES)
    monkeypatch.setenv("COHOUND_LLM_BASE_URL", "http://env.test/v1")
    from core.cli.wiring import build_discovery_llm

    llm = build_discovery_llm(base_url="http://x.test/v1", model_id="m")
    assert llm.base_url == "http://x.test/v1"
    assert llm.model_id == "m"


# ---------------------------------------------------------------------------
# dry-run / happy-path
# ---------------------------------------------------------------------------


def test_dry_run_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    clear_env(monkeypatch, _ENV_NAMES)
    out = tmp_path / "render"
    code = main(
        [
            "passport-scout",
            "--product-name",
            "Alpha Product",
            "--url",
            URL,
            "--passports-dir",
            str(out),
            "--dry-run",
            "--json",
        ],
        discovery_llm=_llm(),
        crawler=fake_crawler(URL, PAGE_TEXT),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["dry_run"] is True
    assert "passport_path" not in payload
    assert not out.exists()


def test_happy_path_writes_yaml_and_sidecar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    clear_env(monkeypatch, _ENV_NAMES)
    out = tmp_path / "render"
    code = main(
        [
            "passport-scout",
            "--product-name",
            "Alpha Product",
            "--url",
            URL,
            "--passports-dir",
            str(out),
            "--json",
        ],
        discovery_llm=_llm(),
        crawler=fake_crawler(URL, PAGE_TEXT),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["product_id"] == "Alpha_Product"
    yaml_path = out / "Alpha_Product" / "2026-06-01.yaml"
    sidecar_path = out / "Alpha_Product" / "2026-06-01.candidates.json"
    # Дата снимка — сегодня (today не задан в CLI), поэтому ищем любой *.yaml.
    yamls = list((out / "Alpha_Product").glob("*.yaml"))
    sidecars = list((out / "Alpha_Product").glob("*.candidates.json"))
    assert len(yamls) == 1 and len(sidecars) == 1
    assert yamls[0].read_text(encoding="utf-8")
    assert json.loads(sidecars[0].read_text(encoding="utf-8"))["fetched"][0]["url"] == URL
    # payload содержит пути
    assert Path(payload["passport_path"]) == yamls[0]
    assert Path(payload["candidates_path"]) == sidecars[0]


def test_human_output_does_not_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    clear_env(monkeypatch, _ENV_NAMES)
    code = main(
        [
            "passport-scout",
            "--product-name",
            "Alpha Product",
            "--url",
            URL,
            "--passports-dir",
            str(tmp_path / "render"),
            "--dry-run",
        ],
        discovery_llm=_llm(),
        crawler=fake_crawler(URL, PAGE_TEXT),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "product_id: Alpha_Product" in out
    assert "ok: yes" in out


# ---------------------------------------------------------------------------
# --package / роли
# ---------------------------------------------------------------------------


def test_package_roles_loaded_from_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    clear_env(monkeypatch, _ENV_NAMES)
    code = main(
        [
            "passport-scout",
            "--product-name",
            "Alpha Product",
            "--url",
            URL,
            "--package",
            str(PACKAGE_FIXTURE),
            "--passports-dir",
            str(tmp_path / "render"),
            "--dry-run",
            "--json",
        ],
        discovery_llm=_llm(),
        crawler=fake_crawler(URL, PAGE_TEXT),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_OK
    assert json_out(capsys)["ok"] is True


def test_broken_package_is_usage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    clear_env(monkeypatch, _ENV_NAMES)
    bad = tmp_path / "bad_pkg"
    bad.mkdir()
    (bad / "package.yaml").write_text("case_id: [", encoding="utf-8")
    code = main(
        [
            "passport-scout",
            "--product-name",
            "Alpha Product",
            "--url",
            URL,
            "--package",
            str(bad),
            "--dry-run",
        ],
        discovery_llm=_llm(),
        crawler=fake_crawler(URL, PAGE_TEXT),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_USAGE
    assert "package" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Ошибки сбора: черновик не публикуется
# ---------------------------------------------------------------------------


def test_errors_do_not_write_draft(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    clear_env(monkeypatch, _ENV_NAMES)

    class _BoomCrawler:
        version = "boom"

        def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
            raise RuntimeError("network down")

    from core.cli.main import EXIT_ERROR

    out = tmp_path / "render"
    code = main(
        [
            "passport-scout",
            "--product-name",
            "Alpha Product",
            "--url",
            URL,
            "--passports-dir",
            str(out),
            "--json",
        ],
        discovery_llm=_llm(),
        crawler=_BoomCrawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_ERROR
    payload = json_out(capsys)
    assert payload["ok"] is False
    assert payload["errors"]
    assert not out.exists()
