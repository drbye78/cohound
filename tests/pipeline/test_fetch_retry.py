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
   помощник: тест падает, если ретрай появится только в одной;
6. бэкофф между попытками (:func:`core.util.fetch_retry.fetch_with_paced_retries`) —
   пауза equal-jitter от экспоненты, её нет перед первой и после последней попытки,
   её нет на терминальном исходе, и она выдерживается даже при ``rate_limit = None``
   (ровно тот случай, где раньше три запроса уходили подряд).

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
from core.util.fetch_retry import (
    FETCH_BACKOFF_BASE_S,
    FETCH_BACKOFF_FACTOR,
    FetchAttemptStats,
    fetch_with_paced_retries,
)
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


def test_local_path_uses_the_shared_retry_helper() -> None:
    """Локальный путь зовёт **тот же** цикл и константу, что edge-планировщик.

    Повтор вынесен из локального модуля в :mod:`core.util.fetch_retry` именно затем,
    чтобы у ``split`` не появилось второй реализации правила. Тест падает, если здесь
    снова заведётся своя копия цикла или своё число попыток.
    """
    from core.pipeline import corpus_ingest
    from core.util import fetch_retry

    assert corpus_ingest.fetch_with_paced_retries is fetch_retry.fetch_with_paced_retries
    assert corpus_ingest.FETCH_RETRY_TIMES is fetch_retry.FETCH_RETRY_TIMES


# --- бэкофф между попытками -------------------------------------------------------
#
# Пауза — equal-jitter от экспоненты по номеру провалившейся попытки: при
# ``rng() = 0.5`` это середина диапазона, то есть ``0.75 * delay``. Сон подменён
# списком событий: тест проверяет арифметику и **порядок** (пауза строго между
# попытками), а не терпение раннера.


def _series(
    *outcomes: Any,
    rate_limit: RateLimitSpec | None = None,
) -> tuple[list[tuple[str, Any]], FetchAttemptStats, BaseException | None]:
    """Прогон помощника на сценарии исходов: события ``("fetch", n)``/``("sleep", s)``.

    ``rng`` зафиксирован на ``0.5`` — джиттер становится детерминированной серединой
    диапазона. Возвращает события, статистику и поднятое исключение (если было).
    """
    events: list[tuple[str, Any]] = []
    queue = list(outcomes)
    calls = 0

    def fetch() -> Any:
        nonlocal calls
        calls += 1
        events.append(("fetch", calls))
        outcome = queue.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def sleep(seconds: float) -> None:
        events.append(("sleep", seconds))

    stats = FetchAttemptStats()
    error: BaseException | None = None
    try:
        fetch_with_paced_retries(
            fetch,
            host=HOST,
            rate_limit=rate_limit,
            # Лимитер без настоящего сна: пейсинг здесь не предмет проверки.
            limiter=RateLimiter(sleep=lambda _s: None),
            is_transient=lambda exc: not isinstance(exc, CrawlNotFound),
            stats=stats,
            sleep=sleep,
            rng=lambda: 0.5,
        )
    except BaseException as exc:  # noqa: BLE001 — исход сценария, а не ошибка теста
        error = exc
    return events, stats, error


def test_backoff_is_equal_jitter_of_the_exponential() -> None:
    """Попытка 1 → 0.75 c, попытка 2 → 1.5 c: ``delay/2 + 0.5 * delay/2`` при ``rng=0.5``."""
    events, _stats, error = _series(
        CrawlError("HTTP 503"), CrawlError("HTTP 503"), _page()
    )

    assert error is None
    pauses = [value for kind, value in events if kind == "sleep"]
    # delay = BASE * FACTOR ** (n - 1), pause = 0.75 * delay при rng = 0.5.
    expected = [
        FETCH_BACKOFF_BASE_S * FETCH_BACKOFF_FACTOR**0 * 0.75,
        FETCH_BACKOFF_BASE_S * FETCH_BACKOFF_FACTOR**1 * 0.75,
    ]
    assert pauses == pytest.approx(expected)
    assert pauses == pytest.approx([0.75, 1.5])


def test_no_pause_before_the_first_or_after_the_last_attempt() -> None:
    """Три попытки — ровно две паузы, и обе строго **между** вызовами фетча."""
    events, _stats, error = _series(
        CrawlError("HTTP 503"), CrawlError("HTTP 503"), _page()
    )

    assert error is None
    kinds = [kind for kind, _value in events]
    # Пауза перед первой попыткой и после последней — потерянное время без адресата.
    assert kinds == ["fetch", "sleep", "fetch", "sleep", "fetch"]
    assert kinds.count("sleep") == FETCH_RETRY_TIMES


def test_no_pause_on_a_terminal_outcome() -> None:
    """``404`` терминален: ``is_transient`` вернул ``False`` ⇒ ``sleep`` не звали ни разу."""
    events, stats, error = _series(CrawlNotFound("HTTP 404", http_status=404), _page())

    assert isinstance(error, CrawlNotFound)
    assert [kind for kind, _value in events] == ["fetch"]
    assert stats.retries == 0
    assert stats.backoff_ms == 0


def test_backoff_ms_records_the_slept_sum() -> None:
    """``backoff_ms`` — фактически проспанное (0.75 + 1.5 c = 2250 мс), целое число."""
    _events, stats, error = _series(
        CrawlError("HTTP 503"), CrawlError("HTTP 503"), _page()
    )

    assert error is None
    assert stats.backoff_ms == 2250
    assert isinstance(stats.backoff_ms, int)


def test_backoff_happens_even_without_rate_limiting() -> None:
    """``rate_limit = None`` (нулевой пейсинг) — именно тот случай, что был дефектом.

    Слот лимитера паузы не даёт вовсе, поэтому без бэкоффа три попытки уходили к
    ответившему ``503`` серверу подряд.
    """
    events, stats, error = _series(CrawlError("HTTP 503"), _page(), rate_limit=None)

    assert error is None
    pauses = [value for kind, value in events if kind == "sleep"]
    assert pauses == pytest.approx([0.75])
    assert stats.backoff_ms == 750
