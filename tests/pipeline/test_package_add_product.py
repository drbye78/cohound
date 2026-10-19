"""Тесты планирования/применения ``hound package add-product`` (офлайн, фейки).

Покрыты: разрешение URL из SERP (один сильный / неоднозначность / подсказка ``--url``),
вывод ключей (корпоративный root + path, выделенный хост), честный unresolved-профиль,
бюджеты sitemap, применение (когорта + run.yaml + профили + артефакт), отказы
без ``--actor``/``--reason``, ничего не пишется в plan-only, generator drift.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from core.adapters.llm_discovery import DiscoveryLLM
from core.config.domain_profiles import load_domain_profiles
from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.pipeline.crawl_probe import (
    ProbeConfidence,
    ProbeKeyStatus,
    RobotsGate,
    Verdict,
)
from core.pipeline.crawl_probe_driver import ProbePorts
from core.pipeline.package_add_product import (
    AddProductError,
    DiscoveredSite,
    _derive_sites,
    _profile_for_verdict,
    _sitemap_budget,
    apply_add_product,
    plan_add_product,
    write_domain_profiles_with_new_keys,
)
from core.ports.search import SearchResult
from tests.fakes.crawl_probe import ScriptedHttpCrawler, page
from tests.fakes.ports import FakeExtractor, FakeSearchAdapter
from tests.helpers.packages import valid_package

PRODUCT = "Acme Product"
START = "https://acme.test/"
DOCS = "https://acme-docs.test/"
LONG_TEXT = "Acme Product documentation text. " * 30

_QUERIES = [
    f'"{PRODUCT}" официальный сайт',
    f'"{PRODUCT}" документация',
    f'"{PRODUCT}" docs',
]


def _hypothesis() -> dict:
    """Минимальная гипотеза паспорта (без сателлитов: обоснование не выдумывается)."""
    return {
        "canonical_name": PRODUCT,
        "vendor": "Acme",
        "doc_domains": ["acme.test"],
        "satellites": [],
    }


def _llm() -> DiscoveryLLM:
    payload = json.dumps(_hypothesis(), ensure_ascii=False)
    return DiscoveryLLM(
        generate=lambda prompt, schema: payload, model_id="fake-llm", decoder="none"
    )


def _crawler(*urls: str) -> ScriptedHttpCrawler:
    return ScriptedHttpCrawler({url: page(url, content=_html(LONG_TEXT).encode()) for url in urls})


def _html(text: str) -> str:
    return f"<html><body><p>{text}</p></body></html>"


def _ports(http: ScriptedHttpCrawler | None = None, *, browser=None) -> ProbePorts:
    return ProbePorts(
        http=http or ScriptedHttpCrawler(default=page(START, content=_html(LONG_TEXT).encode())),
        browser=browser,
        extractor=FakeExtractor(),
        robots=RobotsGate(lambda _host: ""),
        sleep=lambda _seconds: None,
    )


class _RecordingSearch(FakeSearchAdapter):
    """FakeSearchAdapter с учётом вызовов (проверка «SERP всегда»)."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.calls: list[Any] = []

    def search(self, q):
        self.calls.append(q)
        return super().search(q)


def _serp(*urls: str) -> _RecordingSearch:
    results = [SearchResult(url=url, title=url, rank=index) for index, url in enumerate(urls)]
    mapping = {query: results for query in _QUERIES}
    mapping[PRODUCT] = results
    return _RecordingSearch(results=mapping)


def _plan(pkg: Path, *, url: str | None = START, search=None, crawler=None, ports=None):
    return plan_add_product(
        pkg,
        PRODUCT,
        url=url,
        reason="аналог (тест)",
        discovery_llm=_llm(),
        crawler=crawler or _crawler(*[value for value in (START, DOCS) if value]),
        extractor=FakeExtractor(),
        search=search,
        probe_ports=ports or _ports(),
        today="2026-06-01",
    )


# ---------------------------------------------------------------------------
# URL resolution
# ---------------------------------------------------------------------------


def test_explicit_url_does_not_skip_serp(tmp_path: Path) -> None:
    """Подсказка ``--url`` не отключает SERP и не сужает набор сайтов."""
    pkg = valid_package(tmp_path)
    search = _serp(DOCS)
    plan = _plan(pkg, url=START, search=search)
    assert plan.resolved_url == START
    assert plan.url_source == "explicit"
    assert len(search.calls) == len(_QUERIES) + 1  # discover + scout
    keys = {site.key for site in plan.sites}
    assert "acme.test" in keys  # стартовый (выделенный) хост
    assert "acme-docs.test" in keys  # связанный сайт из SERP


def test_no_url_one_strong_candidate_resolves(tmp_path: Path) -> None:
    """Без ``--url`` один сильный кандидат разрешается детерминированно."""
    pkg = valid_package(tmp_path)
    plan = _plan(pkg, url=None, search=_serp(START))
    assert plan.resolved_url == START
    assert plan.url_source == "serp"
    assert any("SERP" in item for item in plan.needs_human)


def test_no_url_ambiguous_candidates_is_usage_error(tmp_path: Path) -> None:
    """Равный скор двух кандидатов → usage-ошибка с кандидатами (без записи)."""
    pkg = valid_package(tmp_path)
    before = _snapshot(pkg)
    with pytest.raises(AddProductError) as excinfo:
        _plan(pkg, url=None, search=_serp("https://one.test/", "https://two.test/"))
    assert excinfo.value.code == "usage"
    assert len(excinfo.value.candidates) == 2
    assert _snapshot(pkg) == before


def test_explicit_url_identifies_among_synonyms(tmp_path: Path) -> None:
    """Подсказка ``--url`` снимает неоднозначность синонимичных имён."""
    pkg = valid_package(tmp_path)
    plan = _plan(
        pkg,
        url=START,
        search=_serp("https://unrelated.test/", "https://acme.test/"),
        crawler=_crawler(START, "https://unrelated.test/"),
    )
    assert plan.resolved_url == START


def test_external_serp_hosts_stay_candidates_not_sites(tmp_path: Path) -> None:
    """Реселлер вне вендорского домена — ``external_host`` в кандидатах, не в sites."""
    pkg = valid_package(tmp_path)
    vendor_sub = "https://web.acme.test/products/acme"
    reseller = "https://granit-concern.ru/kvant"
    plan = _plan(
        pkg,
        url=START,
        search=_serp(vendor_sub, reseller),
        crawler=_crawler(START, vendor_sub, reseller),
    )
    hosts = {site.key.split("/", 1)[0] for site in plan.sites}
    assert hosts <= {"acme.test", "web.acme.test"}
    assert "granit-concern.ru" not in hosts
    external = [
        item for item in plan.url_candidates if item.get("reason") == "external_host"
    ]
    assert any("granit-concern.ru" in item["url"] for item in external)


def test_plan_transliterates_cyrillic_product_id(tmp_path: Path) -> None:
    """«Квант-Гибрид» → ``kvant-gibrid`` в product_id и когорте (не ``-``)."""
    pkg = valid_package(tmp_path)
    plan = plan_add_product(
        pkg,
        "Квант-Гибрид",
        url=START,
        reason="аналог (тест)",
        discovery_llm=_llm(),
        crawler=_crawler(START),
        extractor=FakeExtractor(),
        search=None,
        search_warning="SERP недоступен (SearchResolutionError: HTTP 429)",
        probe_ports=_ports(),
        today="2026-06-01",
    )
    assert plan.product_id == "kvant-gibrid"
    assert plan.cohort_entry["product_id"] == "kvant-gibrid"
    assert any("429" in warning for warning in plan.warnings)


# ---------------------------------------------------------------------------
# Key derivation
# ---------------------------------------------------------------------------


def test_corporate_root_and_product_path_keys() -> None:
    """Корпоративный root + продуктовый path → оба ключа (root помечен)."""
    sites = _derive_sites(
        ["https://corp.test/", "https://corp.test/products/acme"],
        product_name=PRODUCT,
    )
    by_key = {site.key: site for site in sites}
    assert by_key["corp.test"].is_root is True
    assert by_key["corp.test/products/acme"].is_root is False


def test_dedicated_product_host_is_not_root() -> None:
    """Выделенный продуктовый хост — обычный crawl root, НЕ I17 host-root."""
    sites = _derive_sites(["https://acme.test/docs"], product_name=PRODUCT)
    assert [site.key for site in sites] == ["acme.test"]
    assert sites[0].is_root is False


def test_ftdata_dedicated_host_gets_normal_budget() -> None:
    """Регрессия ``ftdata.ru``: выделенный хост без sitemap → 8/200, не 0/10."""
    site = DiscoveredSite(
        key="ftdata.ru", urls=["https://ftdata.ru/"], reason="dedicated", is_root=False
    )
    budget = _sitemap_budget(site, ports=_ports())
    assert budget["max_depth"] == 8
    assert budget["max_documents"] == 200
    assert budget["sitemap_url"] is None


def test_seed_path_scopes_product_and_not_sibling_products() -> None:
    """Seed-путь ``/products/qhb`` — граница сегмента: ``qhb-sert``/``qss`` не матчатся."""
    seed = "https://quantom.info/products/qhb/"
    sites = _derive_sites(
        [
            seed,
            "https://quantom.info/products/qhb/docs/",
            "https://quantom.info/products/qhb-sert/",
            "https://quantom.info/products/qss/",
        ],
        product_name="Квант-Гибрид",
        seed_url=seed,
    )
    by_key = {site.key: site for site in sites}
    assert by_key["quantom.info/products/qhb"].urls == [
        seed,
        "https://quantom.info/products/qhb/docs/",
    ]
    assert "quantom.info/products/qhb-sert" not in by_key
    assert "quantom.info/products/qss" not in by_key


def test_seed_path_sibling_host_reuses_product_prefix() -> None:
    """Sibling-поддомен с тем же ``products/qhb`` → продуктовый ключ, не ``products``."""
    seed = "https://quantom.info/products/qhb/"
    sites = _derive_sites(
        [
            seed,
            "https://web.quantom.info/products/qhb/",
            "https://web.quantom.info/products/qhb-sert/",
            "https://web.quantom.info/products/qss/",
        ],
        product_name="Квант-Гибрид",
        seed_url=seed,
    )
    keys = {site.key for site in sites}
    assert "web.quantom.info/products/qhb" in keys
    assert "web.quantom.info/products" not in keys
    assert "web.quantom.info/products/qhb-sert" not in keys
    assert "web.quantom.info/products/qss" not in keys


def test_docs_host_uses_first_non_generic_segment() -> None:
    """Док-хост: ``/qhb/std-1/...`` → ``repo.quantom.info/qhb`` (не глубокая страница)."""
    sites = _derive_sites(
        ["https://repo.quantom.info/qhb/std-1/doc/1.5.3/ru/x.html"],
        product_name="Квант-Гибрид",
        seed_url="https://quantom.info/products/qhb/",
    )
    assert [site.key for site in sites] == ["repo.quantom.info/qhb"]


# ---------------------------------------------------------------------------
# Probe / budgets
# ---------------------------------------------------------------------------


def test_unresolved_probe_defaults_to_http_auto_with_warning(tmp_path: Path) -> None:
    """Неразрешённый ключ — ``http/auto`` + warning; browser/wait_for не выдумываются."""
    pkg = valid_package(tmp_path)
    http = ScriptedHttpCrawler(default=Exception("offline"))
    plan = _plan(
        pkg,
        crawler=_crawler(START),
        ports=_ports(http, browser=None),
    )
    profile = plan.profiles["acme.test"]
    assert profile["fetch"] == FetchMode.HTTP.value
    assert profile["extract"] == ExtractionRoute.AUTO.value
    assert "strategy" not in profile
    assert "wait_for" not in profile
    assert any("key unresolved" in warning for warning in plan.warnings)


def test_sitemap_present_budget(tmp_path: Path) -> None:
    """Sitemap с in-scope URL → max_depth 2 и max_documents от измеренного count."""
    site = DiscoveredSite(
        key="docs.test/guide", urls=["https://docs.test/guide"], reason="path"
    )
    xml = (
        "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"
        "<url><loc>https://docs.test/guide/a</loc></url>"
        "<url><loc>https://docs.test/guide/b</loc></url>"
        "<url><loc>https://docs.test/other</loc></url>"
        "</urlset>"
    )
    http = ScriptedHttpCrawler(
        {
            "https://docs.test/robots.txt": page(
                "https://docs.test/robots.txt",
                content=b"Sitemap: https://docs.test/sitemap.xml\n",
                content_type="text/plain",
            ),
            "https://docs.test/sitemap.xml": page(
                "https://docs.test/sitemap.xml",
                content=xml.encode(),
                content_type="application/xml",
            ),
        }
    )
    budget = _sitemap_budget(site, ports=_ports(http))
    assert budget["sitemap_found"] is True
    assert budget["sitemap_source_url"] == "https://docs.test/sitemap.xml"
    # Path-ключ: sitemap не эмитится (host-wide sitemap даёт мало in-scope locs).
    assert budget["sitemap_url"] is None
    assert budget["in_scope_count"] == 2
    assert budget["max_depth"] == 2
    assert budget["max_documents"] == 50  # max(50, min(1000, 2 * 2))


def test_sitemap_in_scope_uses_seed_product_prefix() -> None:
    """Sitemap-зонд: in-scope только product-префикс; ``qhb-sert``/``qss`` отсечены."""
    site = DiscoveredSite(
        key="quantom.info/products/qhb",
        urls=["https://quantom.info/products/qhb/"],
        reason="path",
        is_root=False,
    )
    xml = (
        "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"
        "<url><loc>https://quantom.info/products/qhb/</loc></url>"
        "<url><loc>https://quantom.info/products/qhb/docs/</loc></url>"
        "<url><loc>https://quantom.info/products/qhb-sert/</loc></url>"
        "<url><loc>https://quantom.info/products/qss/</loc></url>"
        "</urlset>"
    )
    http = ScriptedHttpCrawler(
        {
            "https://quantom.info/robots.txt": page(
                "https://quantom.info/robots.txt",
                content=b"Sitemap: https://quantom.info/sitemap.xml\n",
                content_type="text/plain",
            ),
            "https://quantom.info/sitemap.xml": page(
                "https://quantom.info/sitemap.xml",
                content=xml.encode(),
                content_type="application/xml",
            ),
        }
    )
    budget = _sitemap_budget(site, ports=_ports(http))
    assert budget["in_scope_count"] == 2
    assert budget["in_scope_urls"] == [
        "https://quantom.info/products/qhb/",
        "https://quantom.info/products/qhb/docs/",
    ]
    assert budget["sitemap_found"] is True
    assert budget["sitemap"] is False  # path-ключ: в профиль не эмитится


def test_path_key_with_host_sitemap_has_no_sitemap_field() -> None:
    """Path-ключ: sitemap-счётчики в бюджете, но ``sitemap:`` в профиль не пишется."""
    site = DiscoveredSite(
        key="corp.test/products/acme",
        urls=["https://corp.test/products/acme"],
        reason="path",
        is_root=False,
    )
    xml = (
        "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"
        "<url><loc>https://corp.test/products/acme/a</loc></url>"
        "<url><loc>https://corp.test/products/acme/b</loc></url>"
        "<url><loc>https://corp.test/products/acme/c</loc></url>"
        "</urlset>"
    )
    http = ScriptedHttpCrawler(
        {
            "https://corp.test/robots.txt": page(
                "https://corp.test/robots.txt",
                content=b"Sitemap: https://corp.test/sitemap.xml\n",
                content_type="text/plain",
            ),
            "https://corp.test/sitemap.xml": page(
                "https://corp.test/sitemap.xml",
                content=xml.encode(),
                content_type="application/xml",
            ),
        }
    )
    budget = _sitemap_budget(site, ports=_ports(http))
    assert budget["sitemap_found"] is True
    assert budget["in_scope_count"] == 3
    assert budget["sitemap_url"] is None
    assert budget["sitemap"] is False
    verdict = Verdict(
        key=site.key,
        status=ProbeKeyStatus.RESOLVED,
        confidence=ProbeConfidence.HIGH,
        fetch=FetchMode.HTTP,
    )
    profile = _profile_for_verdict(site.key, verdict, budget)
    assert "sitemap" not in profile
    assert profile["max_depth"] == 2
    assert profile["max_documents"] == 50


def test_dedicated_host_with_sitemap_emits_it() -> None:
    """Выделенный хост: host-wide sitemap безопасен и попадает в профиль."""
    site = DiscoveredSite(
        key="corp.test", urls=["https://corp.test/"], reason="dedicated", is_root=False
    )
    xml = (
        "<urlset xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>"
        "<url><loc>https://corp.test/a</loc></url>"
        "<url><loc>https://corp.test/b</loc></url>"
        "</urlset>"
    )
    http = ScriptedHttpCrawler(
        {
            "https://corp.test/robots.txt": page(
                "https://corp.test/robots.txt",
                content=b"Sitemap: https://corp.test/sitemap.xml\n",
                content_type="text/plain",
            ),
            "https://corp.test/sitemap.xml": page(
                "https://corp.test/sitemap.xml",
                content=xml.encode(),
                content_type="application/xml",
            ),
        }
    )
    budget = _sitemap_budget(site, ports=_ports(http))
    assert budget["sitemap_url"] == "https://corp.test/sitemap.xml"
    assert budget["max_depth"] == 2
    assert budget["max_documents"] == 50


def test_no_sitemap_budget(tmp_path: Path) -> None:
    """Нет sitemap → path-ключ получает max_depth 8 / max_documents 200."""
    site = DiscoveredSite(
        key="docs.test/guide", urls=["https://docs.test/guide"], reason="path"
    )
    http = ScriptedHttpCrawler(
        {
            "https://docs.test/robots.txt": page(
                "https://docs.test/robots.txt", content=b"", content_type="text/plain"
            )
        }
    )
    budget = _sitemap_budget(site, ports=_ports(http))
    assert budget["sitemap_found"] is False
    assert budget["max_depth"] == 8
    assert budget["max_documents"] == 200
    assert budget["fetches"] == 1


def test_host_root_budget_is_zero(tmp_path: Path) -> None:
    """Host-root ключ: max_depth 0 / max_documents 10 (I17 trap)."""
    site = DiscoveredSite(key="corp.test", urls=["https://corp.test/"], reason="root", is_root=True)
    budget = _sitemap_budget(site, ports=_ports())
    assert budget["max_depth"] == 0
    assert budget["max_documents"] == 10


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


def test_apply_writes_cohort_run_profiles_and_artifact(tmp_path: Path) -> None:
    """Apply: когорта + run.yaml products + новый ключ профилей + review-артефакт."""
    pkg = valid_package(tmp_path)
    plan = _plan(pkg, crawler=_crawler(START), ports=_ports())
    result = apply_add_product(pkg, plan, actor="tester")

    cohort = yaml.safe_load((pkg / "cohort.yaml").read_text(encoding="utf-8"))
    assert any(entry["product_id"] == plan.product_id for entry in cohort["entries"])
    run = yaml.safe_load((pkg / "run.yaml").read_text(encoding="utf-8"))
    assert plan.product_id in run["products"]

    profiles = load_domain_profiles(pkg / "domain_profiles.yaml")
    assert "acme.test" in profiles
    artifact = pkg / "sources" / "add_product" / "2026-06-01.json"
    assert artifact.is_file()
    assert str(artifact) in result["written_files"]


def test_apply_merges_existing_key_and_appends_new(tmp_path: Path) -> None:
    """Существующий ключ — только измеренные оси; новый — полный блок; шапка цела."""
    path = tmp_path / "domain_profiles.yaml"
    path.write_text(
        "# header\n# second line\n\nalpha.test:\n  fetch: http\n  extract: html\n",
        encoding="utf-8",
    )
    verdicts = [
        Verdict(
            key="alpha.test",
            status=ProbeKeyStatus.RESOLVED,
            confidence=ProbeConfidence.HIGH,
            fetch=FetchMode.BROWSER,
            strategy=CrawlStrategy.HONEST,
            extract=ExtractionRoute.DOCUMENT,
        )
    ]
    changed, new_keys = write_domain_profiles_with_new_keys(
        path,
        verdicts,
        {
            "beta.test": {
                "fetch": "http",
                "extract": "auto",
                "max_depth": 8,
                "max_documents": 200,
            }
        },
    )
    assert changed is True
    assert new_keys == ["beta.test"]
    text = path.read_text(encoding="utf-8")
    assert text.startswith("# header\n# second line\n")
    profiles = load_domain_profiles(path)
    assert profiles["alpha.test"].fetch is FetchMode.BROWSER
    assert profiles["alpha.test"].extract is ExtractionRoute.DOCUMENT
    assert profiles["beta.test"].max_documents == 200


def test_apply_without_actor_or_reason_writes_nothing(tmp_path: Path) -> None:
    """Нет ``--actor``/``--reason`` → usage, пакет не изменён."""
    pkg = valid_package(tmp_path)
    plan = _plan(pkg, crawler=_crawler(START), ports=_ports())
    before = _snapshot(pkg)
    with pytest.raises(AddProductError):
        apply_add_product(pkg, plan, actor="")
    # reason отсутствует у плана без scout-обоснования
    plan.selection_reason = None
    with pytest.raises(AddProductError) as excinfo:
        apply_add_product(pkg, plan, actor="tester")
    assert excinfo.value.code == "usage"
    assert _snapshot(pkg) == before


def test_plan_only_writes_nothing(tmp_path: Path) -> None:
    """plan-only (без apply) не меняет пакет."""
    pkg = valid_package(tmp_path)
    before = _snapshot(pkg)
    _plan(pkg, crawler=_crawler(START), ports=_ports())
    assert _snapshot(pkg) == before


def test_generator_drift_warning(tmp_path: Path) -> None:
    """``sources/build_package.py`` → warning, но план строится."""
    pkg = valid_package(tmp_path)
    (pkg / "sources").mkdir()
    (pkg / "sources" / "build_package.py").write_text("# generator\n", encoding="utf-8")
    plan = _plan(pkg, crawler=_crawler(START), ports=_ports())
    assert any("generator_drift" in warning for warning in plan.warnings)


def _snapshot(pkg: Path) -> dict[str, bytes]:
    """Снимок всех файлов пакета (проверка «ничего не записано»)."""
    return {
        str(path.relative_to(pkg)): path.read_bytes()
        for path in sorted(pkg.rglob("*"))
        if path.is_file()
    }
