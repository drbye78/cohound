"""Тесты чистого ядра и драйвера зонда осей краула (``package crawl-probe``).

Без сети: краулеры/экстрактор инъектируются. Покрыты: предикат пригодности (``None`` —
неизвестно, не сбой; короткий текст — непригодно), лестница http → hybrid → honest,
дизамбигуатор по ``fetch_mode``, честный ``unresolved`` (в т.ч. browser_unavailable),
URL-сорсинг, правка файла с сохранением шапки и неизмеренных полей, зеркало словаря.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from core.adapters.crawl_browser import BrowserCrawler, CrawlUnavailable
from core.adapters.crawl_scrapy import CrawlError
from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.pipeline.crawl_probe import (
    MIN_USABLE_TEXT,
    Observation,
    ProbeConfidence,
    ProbeKeyStatus,
    ProbeReason,
    ProbeSource,
    ProbeTier,
    Verdict,
    detect_extract_route,
    group_urls_by_key,
    is_usable,
    pick_urls,
    render_profiles_with_verdicts,
    split_header,
    synthesize_url,
)
from core.pipeline.crawl_probe_driver import ProbePorts, run_probe
from tests.fakes.crawl_probe import (
    FakeBackend,
    RecordingHttpGet,
    ScriptedBrowserCrawler,
    ScriptedHttpCrawler,
    html_page,
    page,
)
from tests.fakes.ports import FakeCrawler, FakeExtractor

URL = "https://example.test/page"
KEY = "example.test"


class _AllowRobots:
    """Робот-политика, разрешающая всё (тесты)."""

    def allowed(self, url: str) -> bool:  # noqa: ARG002
        """Всегда ``True``."""
        return True


def _ports(
    http: ScriptedHttpCrawler,
    browser: ScriptedBrowserCrawler | object | None = None,
    *,
    robots: object | None = None,
) -> ProbePorts:
    """Собирает порты драйвера с фейковым экстрактором и без реального сна."""
    return ProbePorts(
        http=http,
        browser=browser,
        extractor=FakeExtractor(),
        robots=robots if robots is not None else _AllowRobots(),
        sleep=lambda _seconds: None,
    )


def _run(http, browser=None, *, key=KEY, url=URL, source=ProbeSource.URLS, **kwargs):
    """Прогоняет зонд по одному ключу/URL и возвращает вердикт."""
    ports = kwargs.pop("ports", None) or _ports(http, browser)
    verdicts = run_probe(
        [key], candidates={key: (source, [url])}, ports=ports, **kwargs
    )
    return verdicts[0]


# ---------------------------------------------------------------------------
# Предикат пригодности
# ---------------------------------------------------------------------------


def test_status_code_none_is_not_failure() -> None:
    """``status_code is None`` — неизвестно, а не сбой (при извлекаемом тексте)."""
    observation = Observation(
        tier=ProbeTier.HTTP,
        url=URL,
        source=ProbeSource.URLS,
        status_code=None,
        content_type=None,
        content_bytes=1000,
        text_chars=MIN_USABLE_TEXT,
    )
    assert is_usable(observation) is True


def test_short_extracted_text_is_unusable() -> None:
    """Короткий **извлечённый** текст — непригодно (сырые байты не считаются)."""
    observation = Observation(
        tier=ProbeTier.HTTP,
        url=URL,
        source=ProbeSource.URLS,
        status_code=200,
        content_type="text/html",
        content_bytes=5000,
        text_chars=MIN_USABLE_TEXT - 1,
    )
    assert is_usable(observation) is False


def test_document_content_type_is_usable() -> None:
    """Документ по ``content-type`` проходит предикат (extract предложит ``document``)."""
    observation = Observation(
        tier=ProbeTier.HTTP,
        url="https://example.test/doc",
        source=ProbeSource.URLS,
        status_code=200,
        content_type="application/pdf",
        content_bytes=1000,
        text_chars=MIN_USABLE_TEXT,
    )
    assert is_usable(observation) is True
    assert (
        detect_extract_route("https://example.test/doc", "application/pdf")
        is ExtractionRoute.DOCUMENT
    )


# ---------------------------------------------------------------------------
# Лестница тиров
# ---------------------------------------------------------------------------


def test_http_200_html_never_calls_browser() -> None:
    """Tier 1 пригоден → ``fetch: http`` и браузер не вызывается вовсе."""
    http = FakeCrawler({URL: page(URL)})
    browser = ScriptedBrowserCrawler()
    verdict = _run(http, browser)
    assert verdict.resolved
    assert verdict.fetch is FetchMode.HTTP
    assert verdict.strategy is None  # на http-ключе strategy не пишется
    assert verdict.extract is None  # без документа — не определено (пишем только document)
    assert browser.calls == []


def test_transient_http_and_browser_failure_is_unresolved_not_browser() -> None:
    """⚠ Транзиентный сбой HTTP + сбой браузера ⇒ unresolved, НЕ browser."""
    http = ScriptedHttpCrawler({URL: CrawlError("transient")})
    browser = ScriptedBrowserCrawler(default=CrawlError("no browser"))
    verdict = _run(http, browser)
    assert verdict.status.value == "unresolved"
    assert verdict.fetch is None
    assert ProbeReason.BOTH_TIERS_FAILED.value in verdict.reasons
    assert len(http.calls) == 3  # первая попытка + 2 повтора (bounded-retry)
    assert "transient" in (verdict.detail or "")


def test_http_403_escalates_and_honest_render_wins() -> None:
    """401/403 → эскалация; честный рендер даёт ``{fetch: browser, strategy: honest}``."""
    http = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    browser = ScriptedBrowserCrawler(
        honest={URL: page(URL, fetch_mode=FetchMode.BROWSER, status_code=None)}
    )
    verdict = _run(http, browser)
    assert verdict.resolved
    assert verdict.fetch is FetchMode.BROWSER
    assert verdict.strategy is CrawlStrategy.HONEST
    assert verdict.rate_limit_hint is None  # 403 — антибот, не сигнал частоты


def test_hybrid_bridge_http_gives_hybrid_strategy() -> None:
    """Исход гибридного тира ``fetch_mode=HTTP`` (cookie-мост) ⇒ ``strategy: hybrid``."""
    http = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    browser = ScriptedBrowserCrawler(
        hybrid={URL: page(URL, fetch_mode=FetchMode.HTTP, status_code=200)}
    )
    verdict = _run(http, browser)
    assert verdict.resolved
    assert verdict.fetch is FetchMode.BROWSER
    assert verdict.strategy is CrawlStrategy.HYBRID


def test_hybrid_fallback_to_render_gives_honest_strategy() -> None:
    """Гибрид, откатившийся к рендеру (``fetch_mode=BROWSER``) ⇒ ``strategy: honest``."""
    http = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    browser = ScriptedBrowserCrawler(
        hybrid={URL: page(URL, fetch_mode=FetchMode.BROWSER, status_code=None)}
    )
    verdict = _run(http, browser)
    assert verdict.resolved
    assert verdict.fetch is FetchMode.BROWSER
    assert verdict.strategy is CrawlStrategy.HONEST


def test_browser_unavailable_is_unresolved() -> None:
    """Недоступный браузер при эскалации ⇒ unresolved ``browser_unavailable`` (не http)."""
    http = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    browser = ScriptedBrowserCrawler(
        default=CrawlUnavailable("playwright missing")
    )
    verdict = _run(http, browser)
    assert not verdict.resolved
    assert ProbeReason.BROWSER_UNAVAILABLE.value in verdict.reasons
    assert verdict.fetch is None


def test_tier1_raised_marks_medium_confidence() -> None:
    """Tier 1 упал (не вернул страницу) → остаётся маловероятный ложный browser ⇒ medium."""
    http = ScriptedHttpCrawler({URL: CrawlError("transient")})
    browser = ScriptedBrowserCrawler(
        hybrid={URL: page(URL, fetch_mode=FetchMode.HTTP, status_code=200)}
    )
    verdict = _run(http, browser)
    assert verdict.resolved
    assert verdict.confidence is ProbeConfidence.MEDIUM


def test_tier1_returned_bad_page_gives_high_confidence() -> None:
    """Tier 1 вернул страницу (401) — определённый антибот-сигнал ⇒ high."""
    http = ScriptedHttpCrawler({URL: page(URL, status_code=401)})
    browser = ScriptedBrowserCrawler(
        hybrid={URL: page(URL, fetch_mode=FetchMode.HTTP, status_code=200)}
    )
    verdict = _run(http, browser)
    assert verdict.confidence is ProbeConfidence.HIGH


def test_observed_429_sets_rate_limit_hint_but_403_does_not() -> None:
    """Наблюдённый 429 → консервативный ``rate_limit`` hint; 403 — нет."""
    http_429 = ScriptedHttpCrawler({URL: page(URL, status_code=429)})
    http_403 = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    browser = ScriptedBrowserCrawler(default=CrawlError("no browser"))
    v429 = _run(http_429, browser)
    v403 = _run(http_403, browser)
    assert v429.rate_limit_hint is not None
    assert v403.rate_limit_hint is None


def test_unprobeable_key_is_reported_unresolved() -> None:
    """⚠ Ключ без источника URL честно попадает в unresolved (не дефолтится)."""
    http = ScriptedHttpCrawler({URL: page(URL)})
    verdicts = run_probe(
        ["a.test", "b.test"], candidates={"a.test": (ProbeSource.URLS, [URL])},
        ports=_ports(http),
    )
    by_key = {verdict.key: verdict for verdict in verdicts}
    assert set(by_key) == {"a.test", "b.test"}
    assert by_key["b.test"].resolved is False
    assert ProbeReason.NO_URL.value in by_key["b.test"].reasons


def test_robots_disallowed_is_unresolved() -> None:
    """Запрет ``robots.txt`` → unresolved ``robots_disallowed`` (URL не фетчится)."""
    class _Deny:
        def allowed(self, url: str) -> bool:  # noqa: ARG002
            return False

    http = ScriptedHttpCrawler({URL: page(URL)})
    verdict = _run(http, ports=_ports(http, robots=_Deny()))
    assert not verdict.resolved
    assert ProbeReason.ROBOTS_DISALLOWED.value in verdict.reasons
    assert http.calls == []


def test_no_browser_stops_after_tier1() -> None:
    """``no_browser`` останавливает лестницу после Tier 1 (браузер не вызывается)."""
    http = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    browser = ScriptedBrowserCrawler(default=html_page())
    verdict = _run(http, browser, no_browser=True)
    assert not verdict.resolved
    assert ProbeReason.BROWSER_DISABLED.value in verdict.reasons
    assert browser.calls == []


def test_wait_for_hint_on_stub_render() -> None:
    """Заглушка честного рендера (короткий HTML) → ``wait_for_recommended`` + needs_confirmation."""
    http = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    stub = page(URL, fetch_mode=FetchMode.BROWSER, status_code=None, content=b"<html>wait</html>")
    browser = ScriptedBrowserCrawler(honest={URL: stub})
    verdict = _run(http, browser)
    assert verdict.wait_for_recommended is True
    assert verdict.needs_confirmation is True


def test_multiple_urls_prefer_resolved_and_merge_unresolved() -> None:
    """Несколько URL: разрешённый побеждает; при полном провале причины объединяются."""
    ok = "https://example.test/a"
    bad = "https://example.test/b"
    http = ScriptedHttpCrawler({ok: page(ok), bad: CrawlError("transient-bad")})
    ports = _ports(http, ScriptedBrowserCrawler(default=CrawlError("no browser")))
    mixed = run_probe(
        [KEY], candidates={KEY: (ProbeSource.URLS, [bad, ok])}, ports=ports,
        max_urls_per_key=2,
    )[0]
    assert mixed.resolved
    assert mixed.url == ok

    http_all_bad = ScriptedHttpCrawler(default=CrawlError("transient"))
    ports = _ports(http_all_bad, ScriptedBrowserCrawler(default=CrawlError("no browser")))
    merged = run_probe(
        [KEY], candidates={KEY: (ProbeSource.URLS, [bad, ok])}, ports=ports,
        max_urls_per_key=2,
    )[0]
    assert not merged.resolved
    assert ProbeReason.BOTH_TIERS_FAILED.value in merged.reasons
    assert len(merged.urls) == 2
    assert merged.detail and merged.detail.count("transient") >= 2


def test_max_urls_per_key_caps_to_shortest_path() -> None:
    """``max_urls_per_key=1`` зондирует только кратчайший URL."""
    long_url = "https://example.test/deep/path"
    short_url = "https://example.test/"
    http = ScriptedHttpCrawler({short_url: page(short_url), long_url: page(long_url)})
    verdict = run_probe(
        [KEY],
        candidates={KEY: (ProbeSource.URLS, [long_url, short_url])},
        ports=_ports(http),
        max_urls_per_key=1,
    )[0]
    assert verdict.resolved
    assert http.calls == [short_url]


def test_real_browser_crawler_hybrid_bridge_feeds_verdict() -> None:
    """Реальный ``BrowserCrawler`` (cookie-мост, fake швы) → ``strategy: hybrid``."""
    backend = FakeBackend(content=b"<html>" + b"x" * 3000 + b"</html>")
    http_get = RecordingHttpGet([(200, b"<html>" + b"y" * 3000 + b"</html>", {"Content-Type": "text/html"})])
    browser = BrowserCrawler(backend=backend, http_get=http_get, cache=None)
    http = ScriptedHttpCrawler({URL: page(URL, status_code=401)})
    verdict = _run(http, browser)
    assert verdict.resolved
    assert verdict.strategy is CrawlStrategy.HYBRID
    assert verdict.fetch is FetchMode.BROWSER


# ---------------------------------------------------------------------------
# URL-сорсинг / правка файла
# ---------------------------------------------------------------------------


def test_pick_urls_is_deterministic() -> None:
    """Отбор URL: кратчайший путь, затем лексикографически; дубликаты схлопнуты."""
    urls = ["https://e.test/b/x", "https://e.test/a", "https://e.test/a", "https://e.test/"]
    assert pick_urls(urls, 3) == ["https://e.test/", "https://e.test/a", "https://e.test/b/x"]
    assert pick_urls(urls, 1) == ["https://e.test/"]
    with pytest.raises(ValueError):
        pick_urls(urls, 0)


def test_synthesize_url_shape() -> None:
    """Синтез URL из ключа ``host[+path]``."""
    assert synthesize_url("example.test") == "https://example.test/"
    assert synthesize_url("example.test/docs/") == "https://example.test/docs/"


def test_group_urls_by_key_reuses_matcher() -> None:
    """Группировка URL по ключам через существующий host/path-матчер."""
    grouped = group_urls_by_key(
        ["https://example.test/docs/a", "https://example.test/other", "https://x.test/1"],
        ["example.test", "example.test/docs", "x.test"],
    )
    assert grouped["example.test/docs"] == ["https://example.test/docs/a"]
    assert grouped["example.test"] == ["https://example.test/other"]
    assert grouped["x.test"] == ["https://x.test/1"]


def test_render_preserves_header_and_untouched_fields(tmp_path: Path) -> None:
    """Правка сохраняет шапку байт-в-байт и неизмеренные поля; пишет только измеренное."""
    raw = (
        "# Обоснование (живые замеры) — шапка.\n"
        "# вторая строка\n"
        "\n"
        "example.test:\n"
        "  fetch: browser\n"
        "  extract: html\n"
        "  strategy: honest\n"
        "  wait_for: \"() => document.body && 1\"\n"
        "  rate_limit:\n"
        "    min_s: 1.0\n"
        "    max_s: 2.0\n"
    )
    http = ScriptedHttpCrawler({URL: page(URL)})
    verdict = _run(http)
    rendered = render_profiles_with_verdicts(raw, [verdict])
    header, _ = split_header(raw)
    assert rendered.startswith(header)
    assert "Обоснование (живые замеры)" in rendered
    assert "wait_for" in rendered  # существующий wait_for сохранён
    assert "rate_limit" in rendered
    assert "strategy" not in rendered  # на http-ключе strategy снимается
    assert "version" not in rendered
    # Результат обязан читаться строгим загрузчиком.
    from core.config.domain_profiles import load_domain_profiles

    path = tmp_path / "domain_profiles.yaml"
    path.write_text(rendered, encoding="utf-8")
    loaded = load_domain_profiles(path)
    assert loaded["example.test"].fetch is FetchMode.HTTP


# ---------------------------------------------------------------------------
# Правка файла: экономия diff (no-op, дословность неизменённых, порядок полей)
# ---------------------------------------------------------------------------


def _verdict(
    key: str,
    url: str,
    *,
    fetch: FetchMode,
    strategy: CrawlStrategy | None = None,
    extract: ExtractionRoute | None = None,
) -> Verdict:
    """Собирает разрешённый вердикт вручную (правка файла без прогона зонда)."""
    return Verdict(
        key=key,
        status=ProbeKeyStatus.RESOLVED,
        confidence=ProbeConfidence.HIGH,
        source=ProbeSource.URLS,
        url=url,
        fetch=fetch,
        strategy=strategy,
        extract=extract,
    )


def test_render_noop_when_values_match_returns_raw() -> None:
    """⚠ Вердикты, совпадающие с файлом, ⇒ исходный текст байт-в-байт (нулевой diff)."""
    raw = (
        "# шапка\n"
        "example.test:\n"
        "  fetch: http\n"
        "  extract: html\n"
    )
    verdict = _verdict("example.test", "https://example.test/", fetch=FetchMode.HTTP)
    assert render_profiles_with_verdicts(raw, [verdict]) == raw


def test_render_changes_only_measured_key_and_keeps_others_verbatim() -> None:
    """⚠ Меняется один ключ; неизменённый профиль сохраняется дословно (кавычки целы)."""
    raw = (
        "# шапка\n"
        "alpha.test:\n"
        "  fetch: http\n"
        "  extract: html\n"
        "beta.test:\n"
        "  fetch: browser\n"
        "  extract: html\n"
        "  strategy: honest\n"
        '  wait_for: "() => document.body && 1"\n'
    )
    alpha = _verdict(
        "alpha.test", "https://alpha.test/", fetch=FetchMode.BROWSER,
        strategy=CrawlStrategy.HONEST,
    )
    beta = _verdict(
        "beta.test", "https://beta.test/", fetch=FetchMode.BROWSER,
        strategy=CrawlStrategy.HONEST,
    )
    rendered = render_profiles_with_verdicts(raw, [alpha, beta])
    header, _ = split_header(raw)
    assert rendered != raw
    assert rendered.startswith(header)
    # Неизменённый beta — дословно (ручная шапка/кавычки wait_for не трогаются).
    assert (
        "beta.test:\n"
        "  fetch: browser\n"
        "  extract: html\n"
        "  strategy: honest\n"
        '  wait_for: "() => document.body && 1"\n'
    ) in rendered
    # Изменённый alpha — только измеренные оси, порядок модели.
    assert (
        "alpha.test:\n"
        "  fetch: browser\n"
        "  extract: html\n"
        "  strategy: honest\n"
    ) in rendered


def test_render_keeps_model_field_order_not_alphabetical() -> None:
    """⚠ Переписанный профиль держит порядок модели (fetch → extract → strategy)."""
    raw = "g.test:\n  fetch: http\n  extract: html\n"
    verdict = _verdict(
        "g.test", "https://g.test/", fetch=FetchMode.BROWSER, strategy=CrawlStrategy.HONEST
    )
    rendered = render_profiles_with_verdicts(raw, [verdict])
    block = rendered.split("g.test:\n", 1)[1]
    assert block.index("fetch:") < block.index("extract:")
    assert block.index("extract:") < block.index("strategy:")
    assert "g.test:\n  extract:" not in rendered  # не алфавитный порядок


def test_reported_extract_is_null_without_document_evidence() -> None:
    """⚠ Отчёт ``extract`` без документа — ``None`` (не мнимое «auto»/«html»)."""
    verdict = _run(FakeCrawler({URL: page(URL)}))
    assert verdict.extract is None
    assert verdict.as_dict()["extract"] is None


def test_reported_extract_is_document_on_document_evidence() -> None:
    """Положительное свидетельство формата ⇒ отчёт ``extract: document``."""
    doc_url = "https://example.test/report.pdf"
    http = FakeCrawler({doc_url: page(doc_url, content_type="application/pdf")})
    verdict = _run(http, url=doc_url)
    assert verdict.extract is ExtractionRoute.DOCUMENT
    assert verdict.as_dict()["extract"] == "document"
