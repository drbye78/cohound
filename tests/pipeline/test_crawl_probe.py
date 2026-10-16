"""Тесты чистого ядра и драйвера зонда осей краула (``package crawl-probe``).

Без сети: краулеры/экстрактор инъектируются. Покрыты: предикат пригодности (``None`` —
неизвестно, не сбой; короткий текст — непригодно), лестница http → hybrid → honest,
дизамбигуатор по ``fetch_mode``, честный ``unresolved`` (в т.ч. browser_unavailable),
URL-сорсинг, правка файла с сохранением шапки и неизмеренных полей, зеркало словаря.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from core.adapters.crawl_browser import BrowserCrawler, CrawlUnavailable
from core.adapters.crawl_scrapy import CrawlError
from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.ports.types import RawPage
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
    SilentExtractor,
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


def _silent_ports(
    http: ScriptedHttpCrawler, browser: ScriptedBrowserCrawler | None = None
) -> ProbePorts:
    """Порты с экстрактором, воспроизводящим сбой извлечения (0 символов при байтах > 0)."""
    return ProbePorts(
        http=http,
        browser=browser,
        extractor=SilentExtractor(),
        robots=_AllowRobots(),
        sleep=lambda _seconds: None,
    )


#: Тело страницы, дошедшей до нас целиком (транспорт сработал), но не поддавшейся экстракции.
_BIG_BODY = b"<html>" + b"c" * 5000 + b"</html>"

#: Тело реальной 260-байтной заглушки JS-челленджа (форма живого замера).
_STUB_BODY = b"<html>" + b"s" * 247 + b"</html>"


def test_all_tiers_reach_content_but_extract_nothing_is_extraction_empty() -> None:
    """⚠ Транспорт сработал во всех тирах, экстракция дала 0 символов ⇒ ``extraction_empty``.

    Это сбой **экстрактора**, а не краулера: ``both_tiers_failed`` здесь был бы ложью.
    Ось ``fetch`` остаётся неопределённой (``null``) — пригодность мереется извлечением.
    """
    http = ScriptedHttpCrawler({URL: page(URL, content=_BIG_BODY)})
    browser = ScriptedBrowserCrawler(
        default=page(
            URL, fetch_mode=FetchMode.BROWSER, status_code=None, content=_BIG_BODY
        )
    )
    verdict = _run(http, browser, ports=_silent_ports(http, browser))
    assert not verdict.resolved
    assert verdict.fetch is None  # не угадываем http
    assert verdict.reasons == (ProbeReason.EXTRACTION_EMPTY.value,)
    assert ProbeReason.BOTH_TIERS_FAILED.value not in verdict.reasons
    assert "экстрактор" in (verdict.detail or "")
    # Наблюдения несут доказательство: транспорт дошёл (байты есть), текста нет.
    assert verdict.observations
    assert all(o.raised is False and o.unavailable is False for o in verdict.observations)
    assert all(o.content_bytes > 0 for o in verdict.observations)
    assert all(o.text_chars == 0 for o in verdict.observations)
    first = verdict.as_dict()["observations"][0]
    assert first["content_bytes"] > 0 and first["text_chars"] == 0
    assert first["error"]  # диагностика экстрактора сохранена в артефакте


def test_http_401_raise_with_browser_stub_reports_both_reasons() -> None:
    """⚠ Форма живого замера: http упал (401), браузерные тиры дошли, но дали 0 символов.

    Решение: **обе** причины. Транспортный сбой Tier 1 (антибот ``401``) — реальный барьер,
    и называть ситуацию только «сбоем экстракции» значило бы спрятать его; но и ``0``
    извлечённых символов при непустом ответе — отдельный сигнал. ``detail`` разводит тиры.
    """
    assert len(_STUB_BODY) == 260  # форма живого наблюдения
    http = ScriptedHttpCrawler({URL: CrawlError("HTTP 401 Unauthorized")})
    browser = ScriptedBrowserCrawler(
        default=page(
            URL, fetch_mode=FetchMode.BROWSER, status_code=None, content=_STUB_BODY
        )
    )
    verdict = _run(http, browser, ports=_silent_ports(http, browser))
    assert verdict.reasons == (
        ProbeReason.BOTH_TIERS_FAILED.value,
        ProbeReason.EXTRACTION_EMPTY.value,
    )
    assert verdict.fetch is None
    detail = verdict.detail or ""
    assert "http: транспортный сбой" in detail
    assert "HTTP 401" in detail  # назван именно Tier 1
    assert "hybrid: транспорт дошёл" in detail
    assert "экстракт" in detail
    assert verdict.wait_for_recommended is True  # заглушка рендера — мотив wait_for
    assert verdict.needs_confirmation is True


def test_http_extracts_zero_while_browser_tiers_raise_reports_both_reasons() -> None:
    """Обратное расхождение: http дошёл, но извлёк 0; браузерные тиры упали ⇒ обе причины."""
    http = ScriptedHttpCrawler({URL: page(URL, content=_BIG_BODY)})
    browser = ScriptedBrowserCrawler(default=CrawlError("render failed"))
    verdict = _run(http, browser, ports=_silent_ports(http, browser))
    assert verdict.reasons == (
        ProbeReason.BOTH_TIERS_FAILED.value,
        ProbeReason.EXTRACTION_EMPTY.value,
    )
    detail = verdict.detail or ""
    assert "http: транспорт дошёл" in detail
    assert "honest: транспортный сбой" in detail


def test_all_tiers_raised_stays_both_tiers_failed_without_extraction_reason() -> None:
    """Чистый транспортный сбой (все тиры raise) ⇒ только ``both_tiers_failed`` (не extraction)."""
    http = ScriptedHttpCrawler({URL: CrawlError("timeout")})
    browser = ScriptedBrowserCrawler(default=CrawlError("render failed"))
    verdict = _run(http, browser)
    assert verdict.reasons == (ProbeReason.BOTH_TIERS_FAILED.value,)
    assert ProbeReason.EXTRACTION_EMPTY.value not in verdict.reasons
    detail = verdict.detail or ""
    assert "timeout" in detail and "render failed" in detail


def test_small_nonzero_text_stays_both_tiers_failed() -> None:
    """Малый **ненулевой** текст — не ``extraction_empty`` (консервативно ``both_tiers_failed``)."""
    http = ScriptedHttpCrawler({URL: page(URL, content=_STUB_BODY)})
    browser = ScriptedBrowserCrawler(default=CrawlError("render failed"))
    ports = ProbePorts(
        http=http,
        browser=browser,
        extractor=SilentExtractor(text="тридцать символов текста"),
        robots=_AllowRobots(),
        sleep=lambda _seconds: None,
    )
    verdict = _run(http, browser, ports=ports)
    assert verdict.reasons == (ProbeReason.BOTH_TIERS_FAILED.value,)


def test_crawl_probe_reason_mirrors_status_dictionary() -> None:
    """⚠ Словарь — источник истины: ``crawl_probe_reason`` зеркалит ``ProbeReason``."""
    from core.domain.dictionary import load_dictionary

    dictionary = Path(__file__).resolve().parents[2] / "docs" / "status-dictionary.yaml"
    data = load_dictionary(dictionary)
    assert set(data["crawl_probe_reason"]) == {reason.value for reason in ProbeReason}


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


# ---------------------------------------------------------------------------
# Авторское ``wait_for`` ключа: прокидывается, виден в артефакте, не синтезируется
# ---------------------------------------------------------------------------


def test_authored_wait_for_reaches_both_browser_tiers_and_is_surfaced() -> None:
    """⚠ Авторское ``wait_for`` уходит в plan обоих браузерных тиров и видно в наблюдении.

    Лестница исчерпана до honest: hybrid рендерит заглушку, honest даёт контент. Оба плана
    обязаны нести **авторское** условие, а наблюдение — ``wait_for_applied=True``.
    """
    stub = page(
        URL, fetch_mode=FetchMode.BROWSER, status_code=None, content=b"<html>wait</html>"
    )
    usable = page(URL, fetch_mode=FetchMode.BROWSER, status_code=None)
    http = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    browser = ScriptedBrowserCrawler(hybrid={URL: stub}, honest={URL: usable})
    authored = "() => document.body && document.body.innerText.length > 1"
    verdict = _run(http, browser, wait_for_by_key={KEY: authored})

    assert [plan.strategy for plan in browser.plan_calls] == [
        CrawlStrategy.HYBRID,
        CrawlStrategy.HONEST,
    ]
    assert all(plan.wait_for == authored for plan in browser.plan_calls)
    browser_obs = [o for o in verdict.observations if o.is_browser]
    assert browser_obs and all(o.wait_for_applied for o in browser_obs)
    assert all(o.as_dict()["wait_for_applied"] for o in browser_obs)
    # Пригодный контент **с** условием ключа — подтверждение, а не «условие не нужно».
    assert verdict.resolved
    assert verdict.wait_for_confirmed is True
    assert verdict.as_dict()["wait_for_confirmed"] is True


def test_absent_authored_wait_for_probes_with_none_and_no_synthesis() -> None:
    """⚠ Ключ без авторского ``wait_for`` зондируется с ``wait_for=None`` (синтеза нет)."""
    http = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    browser = ScriptedBrowserCrawler(
        honest={URL: page(URL, fetch_mode=FetchMode.BROWSER, status_code=None)}
    )
    verdict = _run(http, browser)  # ``wait_for_by_key`` не задан

    assert browser.plan_calls and all(plan.wait_for is None for plan in browser.plan_calls)
    assert all(not o.wait_for_applied for o in verdict.observations)
    assert verdict.wait_for_confirmed is False
    assert verdict.wait_for_recommended is False


class _WaitForGatedBrowser:
    """Браузерный тир, отдающий контент **только** при применённом ``wait_for``."""

    version = "fake-gated-browser-1"
    engine = "browser"
    supports_browser = True

    def __init__(self, content: bytes) -> None:
        self._content = content
        self.plans: list[Any] = []

    def fetch(self, url: str, plan: Any = None) -> RawPage:
        """Контент при ``plan.wait_for``; иначе — короткая заглушка (непригодно)."""
        self.plans.append(plan)
        if getattr(plan, "wait_for", None):
            return page(
                url, fetch_mode=FetchMode.BROWSER, status_code=None, content=self._content
            )
        return page(
            url,
            fetch_mode=FetchMode.BROWSER,
            status_code=None,
            content=b"<html>stub</html>",
        )


def test_wait_for_gated_browser_resolves_only_with_authored_wait_for() -> None:
    """⚠ Тир даёт контент только с ``wait_for``: авторское условие разрешает ключ.

    Без авторского условия тот же тир отдаёт заглушку → честный ``unresolved`` с
    ``wait_for_recommended``; синтезированное условие не появляется ни в одном плане.
    """
    body = b"<html><body><p>" + ("content " * 200).encode("utf-8") + b"</p></body></html>"
    http = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    resolved = _run(
        http, _WaitForGatedBrowser(body), wait_for_by_key={KEY: "() => ready"}
    )
    assert resolved.resolved
    assert resolved.fetch is FetchMode.BROWSER
    assert resolved.strategy is CrawlStrategy.HONEST
    assert resolved.wait_for_confirmed is True

    http2 = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    gated = _WaitForGatedBrowser(body)
    stub = _run(http2, gated)
    assert not stub.resolved
    assert stub.wait_for_recommended is True
    assert stub.needs_confirmation is True
    # Ни один план не несёт синтезированного условия.
    assert gated.plans and all(getattr(plan, "wait_for", None) is None for plan in gated.plans)


def test_stub_with_applied_wait_for_is_marked_and_not_recommended() -> None:
    """⚠ Заглушка **с** применённым ``wait_for``: ``detail`` помечен, рекомендации нет.

    Условие ключа уже применено, и рендер всё равно остался заглушкой: рекомендовать
    добавить ``wait_for`` (он есть) неинформативно, но факт применения обязан быть виден.
    """
    http = ScriptedHttpCrawler({URL: page(URL, status_code=403)})
    stub = page(
        URL, fetch_mode=FetchMode.BROWSER, status_code=None, content=b"<html>wait</html>"
    )
    browser = ScriptedBrowserCrawler(hybrid={URL: stub}, honest={URL: stub})
    verdict = _run(http, browser, wait_for_by_key={KEY: "() => document.body && 1"})

    assert not verdict.resolved
    assert "[wait_for применён]" in (verdict.detail or "")
    assert all(o.wait_for_applied for o in verdict.observations if o.is_browser)
    assert verdict.wait_for_recommended is False
