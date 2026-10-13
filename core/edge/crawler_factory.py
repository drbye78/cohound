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
"""

from __future__ import annotations

from core.adapters.crawl_browser import BrowserCrawler
from core.adapters.crawl_http import HttpCrawler
from core.adapters.crawl_scrapy import ScrapyCrawler
from core.ports.crawler import CrawlerPort

__all__ = ["crawler_factory"]


def crawler_factory(engine: str) -> CrawlerPort:
    """Собирает **свежий** краулер движка (обычно внутри воркер-процесса).

    Module-level и picklable by reference: планировщик передаёт эту функцию в
    инициализатор ``ProcessPoolExecutor``, и экземпляр строится **в самом процессе** —
    адаптеры не picklable, а Playwright не форк-безопасен, поэтому живой объект не
    пересекает границу процессов.

    ``http``/``scrapy``/``browser`` → соответствующий адаптер (регистронезависимо, со
    strip); неизвестный движок → ``ValueError`` (честный сбой воркера, без тихой
    подмены движка).
    """
    key = str(engine).strip().lower()
    if key == "http":
        return HttpCrawler()
    if key == "scrapy":
        return ScrapyCrawler()
    if key == "browser":
        return BrowserCrawler()
    raise ValueError(f"неизвестный движок краула: {engine!r}")
