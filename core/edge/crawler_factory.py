"""coHound — модульная фабрика краулеров edge-слоя (Phase 3.4/B1).

Factories here are **module-level and picklable by reference** so they can be passed to
``ProcessPoolExecutor(initializer=..., initargs=(crawler_factory, ...))``: ``scrapy`` и
``browser`` исполняются в отдельных процессах, а живой объект-адаптер через границу
процессов не передаётся — воркер строит **собственный** экземпляр по имени движка.

**Почему отдельный модуль (B1).** Launcher запускается как ``python -m core.edge``, то
есть ``core/edge/__main__.py`` исполняется под именем ``__main__``. Определённая в нём
фабрика получала бы ``__module__ == "__main__"``, и pickle-lookup в воркер-процессе не
нашёл бы её (``BrokenProcessPool``). Вынесение фабрики в импортируемый модуль
``core.edge.crawler_factory`` делает ``__module__`` стабильным и picklable.

Модуль категорийно нейтрален (§1.3); зависит только от ``adapters``/``ports`` (edge-слой).

**Robots (§10, A2b).** Edge строит политику ``robots.txt`` сам (:func:`build_robots_gate`,
зеркало ``core.cli.wiring._robots_gate``) и отдаёт её краулерам через ``crawler_factory``:
адаптеры гейтят фетч по ``self._robots is not None and plan.robots`` (политика приходит с
ядра пер-элемент). ``plan.robots=False`` (дефолт) ⇒ ни одного fetch ``robots.txt`` — дефолтные
прогоны байт-идентичны.
"""

from __future__ import annotations

from core.adapters.crawl_browser import BrowserCrawler
from core.adapters.crawl_http import HttpCrawler
from core.adapters.crawl_scrapy import ScrapyCrawler
from core.ports.crawler import CrawlerPort
from core.ports.types import FetchMode, FetchPlan
from core.util.robots import RobotsGate

__all__ = ["crawler_factory", "build_robots_gate"]


def build_robots_gate(user_agent: str | None) -> RobotsGate:
    """``RobotsGate`` поверх отдельного HTTP-краулера (фетч ``robots.txt``) — зеркало N1.

    Фетч ``robots.txt`` ленивый (при первом ``allowed(host)``) и идёт **внутри** ``fetch``
    страницы, который планировщик уже пейсит (:class:`~core.util.rate_limit.RateLimiter`):
    отдельного пейсера не заводим. Недоступная лента трактуется как «правил нет»
    (:class:`RobotsGate`), отказ ``Disallow`` несёт ``refusal_reason="robots_disallow"``
    (адаптер, egress-слой). ``user_agent`` — операционная настройка с ядра
    (``deploy.yaml:crawl.user_agent``); ``None``/пустое ⇒ ``"*"``.

    Module-level и picklable by reference (как :func:`crawler_factory`): воркер-процесс
    строит политику **сам** (``RobotsGate`` держит замыкание-фетчер и не picklable).
    """
    user_agent = (user_agent or "").strip() or "*"

    def fetch(host: str) -> str | None:
        try:
            page = HttpCrawler().fetch(f"https://{host}/robots.txt", FetchPlan(FetchMode.HTTP))
        except Exception:  # noqa: BLE001 — недоступный robots.txt = «правил нет»
            return None
        raw = getattr(page, "content", b"") or b""
        return raw.decode("utf-8", errors="replace") if raw else None

    return RobotsGate(fetch, user_agent=user_agent)


def crawler_factory(
    engine: str, *, robots: RobotsGate | None = None
) -> CrawlerPort:
    """Собирает **свежий** краулер движка (обычно внутри воркер-процесса).

    Module-level и picklable by reference: планировщик передаёт эту функцию в
    инициализатор ``ProcessPoolExecutor``, и экземпляр строится **в самом процессе** —
    адаптеры не picklable, а Playwright не форк-безопасен, поэтому живой объект не
    пересекает границу процессов.

    ``http``/``scrapy``/``browser`` → соответствующий адаптер (регистронезависимо, со
    strip); неизвестный движок → ``ValueError`` (честный сбой воркера, без тихой
    подмены движка).

    :param robots: опциональная per-host политика ``robots.txt`` (§10). Передаётся в
        ``HttpCrawler``/``BrowserCrawler``; ``ScrapyCrawler`` уже выставляет
        ``ROBOTSTXT_OBEY`` и политику не принимает. ``None`` (дефолт) → адаптер без гейта
        (``plan.robots`` инертен; лишних fetch ``robots.txt`` нет).
    """
    key = str(engine).strip().lower()
    if key == "http":
        return HttpCrawler(robots=robots)
    if key == "scrapy":
        return ScrapyCrawler()
    if key == "browser":
        return BrowserCrawler(robots=robots)
    raise ValueError(f"неизвестный движок краула: {engine!r}")
