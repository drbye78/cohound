"""Пейсящиеся повторы фетча: каждая попытка берёт **свой** слот лимитера.

Дефект (измерен 07.11.2026 на панели в деградации апстрима): ``RETRY_TIMES: 2`` у
Scrapy повторял ``5xx`` **внутри** ``active.fetch``, то есть внутри одного резерва
лимитера. На хост уходило до трёх запросов без интервала, ``attempts`` элемента
оставался ``1``, и повтор был виден только косвенно — через ``http_requests``
(``docs/reports/measurements/2026-11-07-crawl-rate-tantor.md`` §7.6).

Проверяется:

1. успех с первой попытки — один ``active.fetch`` и один ``wait_reserved``;
2. ``500`` → ``200`` — два вызова и **два** резерва, элемент получает страницу,
   ``retries = 1``;
3. ``404`` — терминальный ответ, повтор не делается;
4. исчерпание повторов — исход ``CrawlError``, а не тихое «получилось»;
5. обе ветки (пул ``local_fetch`` и inline ``fetch_and_commit``) зовут **один**
   помощник: тест падает, если ретрай появится только в одной.

Данные категорийно-нейтральны (§1.3): ``a.test``/``p1``.
"""

from __future__ import annotations

from typing import Any

import pytest

from core.domain import RateLimitSpec
from core.pipeline.corpus_ingest import (
    FETCH_RETRY_TIMES,
    LocalFetch,
    fetch_and_commit,
    local_fetch,
)
from core.pipeline.crawl_pacing import CrawlPacing
from core.ports.crawl_errors import CrawlError, CrawlNotFound
from core.ports.types import FetchMode, FetchPlan, RawPage
from core.util.http_audit import note_request
from core.util.rate_limit import RateLimiter

HOST = "a.test"
URL = f"https://{HOST}/page"
TS = "2026-01-01T00:00:00+00:00"


def _page() -> RawPage:
    """Успешный ответ транспорта."""
    return RawPage(url=URL, content=b"<html>ok</html>", fetched_at=TS)


class _ScriptedCrawler:
    """Краулер по сценарию: отдаёт исходы по очереди и считает вызовы.

    Каждый вызов отмечает запрос в ``http_audit`` — как настоящий транспорт. Иначе
    ``http_requests`` прибора не отличил бы повтор от одного запроса.
    """

    supports_browser = True
    engine = "http"

    def __init__(self, *outcomes: Any) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0

    def fetch(self, url: str, plan: FetchPlan) -> RawPage:
        self.calls += 1
        note_request(url)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _CountingLimiter(RateLimiter):
    """Боевой лимитер с фейковым сном: считает **резервы** (слоты origin'а)."""

    def __init__(self) -> None:
        self.reserves: list[str] = []
        super().__init__()

    def _reserve(self, host: str, spec: RateLimitSpec) -> tuple[float, int]:
        self.reserves.append(host)
        return super()._reserve(host, spec)


def _desc(crawler: _ScriptedCrawler) -> LocalFetch:
    """Дескриптор живого фетча (пул и inline различаются только точкой входа)."""
    return LocalFetch(
        pid="p1",
        wu=URL,
        depth=0,
        fetch_plan=FetchPlan(
            fetch=FetchMode.HTTP, rate_limit=RateLimitSpec(min_s=0.0, max_s=0.0)
        ),
        active=crawler,
        engine_value="http",
        prior_item=None,
        resume=None,
        browser_blocked=False,
        poolable=True,
        host=HOST,
        scope_host=HOST,
        enforce_redirect_scope=False,
        terminal=False,
        reject=None,
    )


def _inline(desc: LocalFetch, limiter: RateLimiter, pacing: CrawlPacing) -> Any:
    """Inline-путь с заглушкой коммита: возвращает исход, переданный коммиту."""
    captured: list[dict[str, Any]] = []

    def commit_local(desc: LocalFetch, **kwargs: Any) -> None:
        captured.append(kwargs)

    fetch_and_commit(desc, limiter=limiter, commit_local=commit_local, pacing=pacing)
    assert len(captured) == 1
    return captured[0]


def test_success_on_first_attempt_takes_exactly_one_slot() -> None:
    """Успех с первой попытки: один ``fetch``, один резерв, повторов нет."""
    crawler = _ScriptedCrawler(_page())
    limiter = _CountingLimiter()
    pacing = CrawlPacing()

    page = local_fetch(_desc(crawler), limiter=limiter, pacing=pacing)

    assert page.content == b"<html>ok</html>"
    assert crawler.calls == 1
    assert limiter.reserves == [HOST]
    host = pacing.snapshot()["hosts"][HOST]
    assert host["retries"] == 0
    assert host["requests"] == 1


def test_500_then_200_retries_with_a_new_limiter_slot() -> None:
    """``500`` → ``200``: два резерва, страница получена, повтор виден в приборе."""
    crawler = _ScriptedCrawler(
        CrawlError("HTTP 500", http_status=500),
        _page(),
    )
    limiter = _CountingLimiter()
    pacing = CrawlPacing()

    outcome = _inline(_desc(crawler), limiter, pacing)

    assert crawler.calls == 2
    # Каждая попытка — свой слот: ровно это и было дефектом движка.
    assert limiter.reserves == [HOST, HOST]
    assert outcome["page"].content == b"<html>ok</html>"
    host = pacing.snapshot()["hosts"][HOST]
    assert host["retries"] == 1
    assert host["requests"] == 2
    assert host["http_requests"] == 2


def test_404_is_terminal_and_not_retried() -> None:
    """``404`` — определённый ответ: повтор адресовался бы тому, что уже ответило."""
    crawler = _ScriptedCrawler(CrawlNotFound("HTTP 404", http_status=404), _page())
    limiter = _CountingLimiter()
    pacing = CrawlPacing()

    with pytest.raises(CrawlNotFound):
        local_fetch(_desc(crawler), limiter=limiter, pacing=pacing)

    assert crawler.calls == 1
    assert limiter.reserves == [HOST]
    assert pacing.snapshot()["hosts"][HOST]["retries"] == 0


def test_transient_exception_is_retried_up_to_the_declared_number() -> None:
    """Сетевой сбой (``CrawlError`` без статуса) повторяется ``1 + FETCH_RETRY_TIMES``."""
    crawler = _ScriptedCrawler(
        *[CrawlError("обрыв") for _ in range(1 + FETCH_RETRY_TIMES)]
    )
    limiter = _CountingLimiter()
    pacing = CrawlPacing()

    with pytest.raises(CrawlError):
        local_fetch(_desc(crawler), limiter=limiter, pacing=pacing)

    assert crawler.calls == 1 + FETCH_RETRY_TIMES
    assert limiter.reserves == [HOST] * (1 + FETCH_RETRY_TIMES)
    assert pacing.snapshot()["hosts"][HOST]["retries"] == FETCH_RETRY_TIMES


@pytest.mark.parametrize("branch", ["pool", "inline"])
def test_both_fetch_branches_share_the_same_retry_helper(branch: str) -> None:
    """Ретрай обязан быть в **обеих** ветках: пул и inline зовут один помощник.

    Тест падает, если повтор появится только в одной из них (ровно так дефект и
    выжил бы: успешный путь зелёный, повтор — вне пейсинга).
    """
    crawler = _ScriptedCrawler(
        CrawlError("HTTP 503", http_status=503),
        _page(),
    )
    limiter = _CountingLimiter()
    pacing = CrawlPacing()
    desc = _desc(crawler)

    if branch == "pool":
        page = local_fetch(desc, limiter=limiter, pacing=pacing)
        assert page.content == b"<html>ok</html>"
    else:
        outcome = _inline(desc, limiter, pacing)
        assert outcome["page"].content == b"<html>ok</html>"

    assert crawler.calls == 2
    assert limiter.reserves == [HOST, HOST]
    assert pacing.snapshot()["hosts"][HOST]["retries"] == 1