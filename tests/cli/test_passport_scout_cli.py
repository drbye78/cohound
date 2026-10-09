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
from core.ports.types import RawPage, RenderMode
from tests.fakes.ports import FakeCrawler, FakeExtractor

URL = "https://docs.test/alpha"
PAGE_TEXT = "The Alpha Product supports Feature X natively."
FIXED_TS = "2026-01-01T00:00:00+00:00"
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


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def _hypothesis() -> dict:
    return json.loads((FIXTURES / "hypothesis.json").read_text(encoding="utf-8"))


def _llm() -> DiscoveryLLM:
    payload = json.dumps(_hypothesis(), ensure_ascii=False)
    return DiscoveryLLM(generate=lambda p, s: payload, model_id="m1")


def _crawler() -> FakeCrawler:
    page = RawPage(
        url=URL,
        content=PAGE_TEXT.encode("utf-8"),
        fetched_at=FIXED_TS,
        render_mode=RenderMode.HTTP,
    )
    return FakeCrawler({URL: page})


def _json_out(capsys: pytest.CaptureFixture[str]) -> dict:
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])


# ---------------------------------------------------------------------------
# Конфигурация LLM
# ---------------------------------------------------------------------------


def test_missing_llm_config_is_usage_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Явно пустой адаптер (нет base_url/model/шва) → понятная ошибка ``llm``."""
    _clear_env(monkeypatch)
    unconfigured = DiscoveryLLM(base_url="", model_id="")
    code = main(
        ["passport-scout", "--product-name", "X", "--url", URL],
        discovery_llm=unconfigured,
    )
    assert code == EXIT_USAGE
    err = capsys.readouterr().err
    assert "llm" in err


def test_wiring_defaults_and_api_key_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """env-фолбэки: base_url/model/decoder по умолчанию; ключ COHOUND_LLM_API_KEY → PLUSVIBE."""
    _clear_env(monkeypatch)
    monkeypatch.setenv("PLUSVIBEAPI_API_KEY", "fallback-key")
    from core.cli.wiring import build_discovery_llm

    llm = build_discovery_llm()
    assert llm.base_url == "https://plusvibeapi.ru/v1"
    assert llm.model_id == "deepseek-v4.1-flash:cxbc"
    assert llm.decoder == "json_object"
    assert llm.api_key == "fallback-key"

    monkeypatch.setenv("COHOUND_LLM_API_KEY", "primary-key")
    assert build_discovery_llm().api_key == "primary-key"


def test_llm_flags_override_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI-флаги переопределяют env при сборке адаптера (без сети)."""
    _clear_env(monkeypatch)
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
    _clear_env(monkeypatch)
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
        crawler=_crawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_OK
    payload = _json_out(capsys)
    assert payload["dry_run"] is True
    assert "passport_path" not in payload
    assert not out.exists()


def test_happy_path_writes_yaml_and_sidecar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
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
        crawler=_crawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_OK
    payload = _json_out(capsys)
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
    _clear_env(monkeypatch)
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
        crawler=_crawler(),
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
    _clear_env(monkeypatch)
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
        crawler=_crawler(),
        extractor=FakeExtractor(),
    )
    assert code == EXIT_OK
    assert _json_out(capsys)["ok"] is True


def test_broken_package_is_usage_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
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
        crawler=_crawler(),
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
    _clear_env(monkeypatch)

    class _BoomCrawler:
        version = "boom"

        def fetch(self, url: str, render: RenderMode = RenderMode.HTTP) -> RawPage:
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
    payload = _json_out(capsys)
    assert payload["ok"] is False
    assert payload["errors"]
    assert not out.exists()
