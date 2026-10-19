"""Тесты CLI ``hound package add-product`` (офлайн: порты/LLM/поиск инъецируются).

Покрыты: plan-only по умолчанию (ничего не пишется), SERP выполняется и при ``--url``
(стартовый + связанный сайт в плане), ``--apply --actor`` пишет когорту/run.yaml/профили/
артефакт, отказы без ``--actor``/``--reason`` и неоднозначный URL → ``EXIT_USAGE``.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

import yaml

from core.adapters.llm_discovery import DiscoveryLLM
from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.cli.wiring import SearchResolutionError
from core.pipeline.crawl_probe import RobotsGate
from core.pipeline.crawl_probe_driver import ProbePorts
from core.ports.search import SearchResult
from tests.fakes.crawl_probe import ScriptedHttpCrawler, page
from tests.fakes.ports import FakeExtractor, FakeSearchAdapter
from tests.helpers.packages import valid_package

PRODUCT = "Acme Product"
START = "https://acme.test/"
RELATED = "https://acme-docs.test/"
TEXT = "Acme Product documentation text. " * 30
_QUERIES = [
    f'"{PRODUCT}" официальный сайт',
    f'"{PRODUCT}" документация',
    f'"{PRODUCT}" docs',
]


class _RecordingSearch(FakeSearchAdapter):
    """FakeSearchAdapter с журналом вызовов (SERP не отключается ``--url``)."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.calls: list[Any] = []

    def search(self, q):
        self.calls.append(q)
        return super().search(q)


def _llm() -> DiscoveryLLM:
    payload = json.dumps(
        {
            "canonical_name": PRODUCT,
            "vendor": "Acme",
            "doc_domains": ["acme.test"],
            "satellites": [],
        },
        ensure_ascii=False,
    )
    return DiscoveryLLM(
        generate=lambda prompt, schema: payload, model_id="fake-llm", decoder="none"
    )


def _page(url: str):
    return page(url, content=f"<html><body><p>{TEXT}</p></body></html>".encode())


def _crawler(*urls: str) -> ScriptedHttpCrawler:
    return ScriptedHttpCrawler({url: _page(url) for url in urls})


def _ports() -> ProbePorts:
    return ProbePorts(
        http=ScriptedHttpCrawler({START: _page(START)}),
        browser=None,
        extractor=FakeExtractor(),
        robots=RobotsGate(lambda _host: ""),
        sleep=lambda _seconds: None,
    )


def _serp(*urls: str) -> _RecordingSearch:
    results = [SearchResult(url=url, title=url, rank=index) for index, url in enumerate(urls)]
    mapping = {query: results for query in _QUERIES}
    mapping[PRODUCT] = results
    return _RecordingSearch(results=mapping)


def _argv(pkg: Path, *extra: str) -> list[str]:
    return ["package", "add-product", "--package", str(pkg), PRODUCT, "--json", *extra]


def _run(capsys, argv: list[str], *, search=None, crawler=None):
    code = main(
        argv,
        discovery_llm=_llm(),
        crawler=crawler or _crawler(START, RELATED),
        extractor=FakeExtractor(),
        search_adapter=search,
        crawl_probe_ports=_ports(),
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip().splitlines()[-1]) if captured.out.strip() else {}
    return code, payload


def _snapshot(pkg: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(pkg)): path.read_bytes()
        for path in sorted(pkg.rglob("*"))
        if path.is_file()
    }


def test_dry_run_writes_nothing_and_uses_serp_with_url(capsys, tmp_path: Path) -> None:
    """Plan-only: SERP всё равно выполняется, стартовый и связанный сайты в плане."""
    pkg = valid_package(tmp_path)
    before = _snapshot(pkg)
    search = _serp(RELATED)
    code, payload = _run(capsys, _argv(pkg, "--url", START), search=search)

    assert code == EXIT_OK
    assert payload["dry_run"] is True
    assert payload["resolved_url"] == START
    assert payload["url_source"] == "explicit"
    keys = {site["key"] for site in payload["sites"]}
    assert "acme.test" in keys
    assert "acme-docs.test" in keys
    assert len(search.calls) == len(_QUERIES) + 1  # discover всегда + scout
    assert _snapshot(pkg) == before


def test_apply_writes_cohort_run_profiles_and_artifact(capsys, tmp_path: Path) -> None:
    """``--apply --actor --reason`` пишет когорту, run.yaml, профили и артефакт."""
    pkg = valid_package(tmp_path)
    code, payload = _run(
        capsys,
        _argv(pkg, "--url", START, "--apply", "--actor", "tester", "--reason", "аналог"),
        search=_serp(),
    )

    assert code == EXIT_OK
    assert payload["applied"] is True
    assert payload["cohort"]["changed"] is True

    cohort = yaml.safe_load((pkg / "cohort.yaml").read_text(encoding="utf-8"))
    assert any(entry["product_id"] == payload["product_id"] for entry in cohort["entries"])
    run = yaml.safe_load((pkg / "run.yaml").read_text(encoding="utf-8"))
    assert payload["product_id"] in run["products"]
    profiles = yaml.safe_load((pkg / "domain_profiles.yaml").read_text(encoding="utf-8"))
    assert "acme.test" in profiles
    artifact = pkg / "sources" / "add_product" / (
        f"{_dt.date.today().isoformat()}-{payload['product_id']}.json"
    )
    assert artifact.is_file()
    # MVP-1: add-product не пишет операционный state.db.
    assert not (tmp_path / "state").exists()


def test_apply_without_actor_is_usage_and_writes_nothing(capsys, tmp_path: Path) -> None:
    """``--apply`` без ``--actor`` → ``EXIT_USAGE`` до любых записей."""
    pkg = valid_package(tmp_path)
    before = _snapshot(pkg)
    code, payload = _run(
        capsys,
        _argv(pkg, "--url", START, "--apply", "--reason", "аналог"),
        search=_serp(),
    )
    assert code == EXIT_USAGE
    assert payload["ok"] is False
    assert _snapshot(pkg) == before


def test_apply_without_reason_is_usage_and_writes_nothing(capsys, tmp_path: Path) -> None:
    """Нет ``--reason`` и нет scout-обоснования → ``EXIT_USAGE``, пакет не изменён."""
    pkg = valid_package(tmp_path)
    before = _snapshot(pkg)
    code, payload = _run(
        capsys,
        _argv(pkg, "--url", START, "--apply", "--actor", "tester"),
        search=_serp(),
    )
    assert code == EXIT_USAGE
    assert payload["ok"] is False
    assert _snapshot(pkg) == before


def test_ambiguous_url_is_usage_and_writes_nothing(capsys, tmp_path: Path) -> None:
    """Без ``--url`` два равных SERP-кандидата → ``EXIT_USAGE`` с кандидатами."""
    pkg = valid_package(tmp_path)
    before = _snapshot(pkg)
    code, payload = _run(
        capsys,
        _argv(pkg),
        search=_serp("https://one.test/", "https://two.test/"),
    )
    assert code == EXIT_USAGE
    assert payload["ok"] is False
    assert str(pkg) not in payload.get("message", "")
    assert _snapshot(pkg) == before


def test_search_failure_reason_reaches_warning_with_url(
    capsys, tmp_path: Path, monkeypatch
) -> None:
    """Сбой резолва поиска при ``--url`` виден в warning (не только generic-строка)."""

    def _failing_resolver(**_kwargs):
        raise SearchResolutionError("search", "HTTP 429 quota exceeded")

    monkeypatch.setattr(
        "core.cli.wiring.resolve_and_build_search_adapter", _failing_resolver
    )
    pkg = valid_package(tmp_path)
    code, payload = _run(capsys, _argv(pkg, "--url", START))
    assert code == EXIT_OK
    assert any(
        "SERP недоступен" in warning and "429" in warning
        for warning in payload.get("warnings", [])
    )


def test_zero_seeds_apply_writes_pending_and_exits_blocked(
    capsys, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ноль валидных seed'ов: cohort + ``pending_seeds``, ``EXIT_BLOCKED`` (09 §2.2)."""
    from core.cli.main import EXIT_BLOCKED
    from core.cli.package import commands as package_commands
    from core.domain.enums import OnboardingState

    original = package_commands.plan_add_product

    def _no_seeds(*args, **kwargs):
        plan = original(*args, **kwargs)
        plan.seeds = []
        plan.onboarding_state = OnboardingState.PENDING_SEEDS
        plan.onboarding_missing = ["no_seeds"]
        return plan

    monkeypatch.setattr(package_commands, "plan_add_product", _no_seeds)

    pkg = valid_package(tmp_path)
    code, payload = _run(
        capsys,
        _argv(pkg, "--url", START, "--apply", "--actor", "tester", "--reason", "аналог"),
        search=_serp(),
    )

    assert code == EXIT_BLOCKED
    assert payload["ok"] is False
    assert payload["onboarding_state"] == "pending_seeds"
    assert payload["onboarding_missing"] == ["no_seeds"]
    assert "no_seeds" in payload["needs_human"]
    cohort = yaml.safe_load((pkg / "cohort.yaml").read_text(encoding="utf-8"))
    assert any(entry["product_id"] == payload["product_id"] for entry in cohort["entries"])
    seeds = yaml.safe_load((pkg / "sources" / "seeds.yaml").read_text(encoding="utf-8"))
    assert seeds["products"][payload["product_id"]] == {
        "state": "pending_seeds",
        "seeds": [],
    }


def test_dry_run_reports_seeds_and_empty_written_files(capsys, tmp_path: Path) -> None:
    """Dry-run JSON несёт seeds/onboarding_state/onboarding_missing и пустой written_files."""
    pkg = valid_package(tmp_path)
    code, payload = _run(capsys, _argv(pkg, "--url", START), search=_serp(RELATED))

    assert code == EXIT_OK
    assert payload["dry_run"] is True
    assert payload["written_files"] == []
    assert payload["onboarding_state"] == "ready"
    assert payload["onboarding_missing"] == []
    assert {seed["normalized_url"] for seed in payload["seeds"]} == {
        START,
        RELATED,
    }
