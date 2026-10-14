"""coHound — composition root edge-узла (``python -m core.edge serve``).

Второй composition root (07 §4): собирает **реальные** веб-адаптеры из ``core.adapters.*``
и публикует их по протоколу ``/v1/*`` (:class:`core.edge.server.EdgeGateway`). Здесь нет
доступа к ``store``/``domain``/``cli`` — edge отвечает на запросы ядра, обратного
инициирования нет (I18/I19; 07 §3/§5).

Что собирается:

* **краулеры** — ``http`` всегда; ``scrapy``/``browser`` «лениво» из карты движков
  (``EdgeRuntimeConfig.workers``, положительное число слотов), каждая сборка под guard:
  недоступность зависимости не валит узел, движок просто опускается. Активный движок
  выбирается по оси ``fetch`` запроса (:class:`_EngineCrawler`);
* **экстрактор** — композит HTML (trafilatura) поверх документов (docling) по маршруту
  ``ExtractionRoute`` (edge — веб-зона, документы — часть её контента);
* **чанкер** — ``MarkdownChunkerPort`` (строится как часть корня; нарезка в MVP остаётся
  в ядре — I19, шов отдаёт сырые байты/текст);
* **планировщик** — :class:`core.edge.scheduler.EdgeScheduler` (Phase 3.2/3.4a):
  ``http`` исполняется in-process (потоки), ``scrapy``/``browser`` — в отдельных
  процессах через module-level :func:`crawler_factory` (адаптеры не picklable, Playwright
  не форк-безопасен). Планировщик инъецируется в :func:`serve` (``scheduler=``): сервер
  запускает его до обслуживания и останавливает в ``finally``.
* **поллер фидов** — :class:`core.edge.poller.FeedPollService` (Wave 5.1) поверх того же
  краулер-диспетчера: обслуживает ``crawl/poll`` (фетчит **только каталоговый документ**
  ленты/канала, возвращает ссылки-записи; контент записей — через обычный ``fetch``/
  ``crawl/collect``). Инъецируется в ``EdgeGateway`` (``poller=``).

Флаги: ``serve [--config FILE] [--host H] [--port P] [--check]``. ``--check`` собирает
конфиг+адаптеры+gateway+планировщик и выходит без привязки сокета **и** без старта
воркеров (smoke: misconfig всплывает до запуска).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import signal
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.adapters.chunker_markdown import MarkdownChunkerPort
from core.adapters.crawl_browser import BrowserCrawler
from core.adapters.crawl_http import HttpCrawler
from core.adapters.crawl_scrapy import CrawlError, ScrapyCrawler
from core.adapters.extract_document import DoclingDocumentExtractor
from core.adapters.extract_html import HtmlExtractor
from core.edge.config import EdgeRuntimeConfig, load_edge_runtime_config
from core.edge.crawler_factory import crawler_factory
from core.edge.poller import FeedPollService
from core.edge.scheduler import EdgeScheduler
from core.edge.server import EdgeGateway, serve
from core.ports.chunker import ChunkerPort
from core.ports.crawler import CrawlerPort
from core.ports.extractor import ExtractorPort
from core.ports.types import ExtractionRoute, FetchMode, FetchPlan, RawPage

__all__ = ["main", "crawler_factory"]

#: Коды возврата edge-лаунчера (edge — отдельный composition root, CLI-ядро не импортирует).
EXIT_OK = 0
#: Непредвиденная ошибка сборки/исполнения.
EXIT_ERROR = 1
#: Ошибка входа/конфигурации (неизвестная подкоманда, невалидный конфиг).
EXIT_USAGE = 2

#: Форматы документов docling → расширение для детекции маршрута (зеркало 03 §D.2).
_DOC_SUFFIX_BY_CONTENT_TYPE: dict[str, str] = {
    "application/pdf": ".pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
}


# ---------------------------------------------------------------------------
# Экстрактор: композит HTML (trafilatura) + документы (docling)
# ---------------------------------------------------------------------------


def _detect_document_filename(page: RawPage) -> str | None:
    """Имя документа с расширением, если формат обрабатывает docling (иначе ``None``).

    Формат выводится из URL (суффикс пути), затем из ``content_type``. ``None`` — страницу
    обрабатывает HTML-путь. Используется только при маршруте ``AUTO`` (07 §3).
    """
    suffix = Path(urlsplit(page.url).path).suffix.lower()
    if suffix and DoclingDocumentExtractor.supports(f"document{suffix}"):
        return f"document{suffix}"
    content_type = (page.content_type or "").split(";")[0].strip().lower()
    doc_suffix = _DOC_SUFFIX_BY_CONTENT_TYPE.get(content_type)
    return f"document{doc_suffix}" if doc_suffix is not None else None


class _EdgeExtractor:
    """Композит edge-экстракции: документы (docling) поверх HTML (trafilatura).

    Реализует ``ExtractorPort`` структурно. Маршрут ``ExtractionRoute``: ``HTML`` —
    принудительно HTML; ``DOCUMENT`` — принудительно docling; ``AUTO`` — авто-детект
    формата (URL/content-type). Отсутствие docling деградирует к чистому HTML.
    """

    def __init__(self) -> None:
        self._html = HtmlExtractor()
        self._documents = DoclingDocumentExtractor()
        if not self._documents.available:
            self._documents = None  # type: ignore[assignment]
        self.version = (
            self._html.version
            if self._documents is None
            else f"{self._html.version}+{self._documents.version}"
        )

    @property
    def SUPPORTED_FORMATS(self) -> dict[str, str]:
        """Поддерживаемые форматы: HTML + (при docling) документные (для capabilities)."""
        formats = {"html": "html"}
        if self._documents is not None:
            declared = getattr(self._documents, "SUPPORTED_FORMATS", None)
            if isinstance(declared, dict):
                formats.update(declared)
        return formats

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> Any:
        """``RawPage`` → ``ExtractedDoc`` по ``route`` (грациозная деградация)."""
        if route is ExtractionRoute.HTML or self._documents is None:
            return self._html.extract(page, route)
        if route is ExtractionRoute.DOCUMENT:
            filename = _detect_document_filename(page) or "document.pdf"
            return self._documents.extract_bytes(page.content, filename)
        filename = _detect_document_filename(page)
        if filename is not None:
            return self._documents.extract_bytes(page.content, filename)
        return self._html.extract(page, route)


# ---------------------------------------------------------------------------
# Краулеры: карта движков + диспетчер по оси fetch
# ---------------------------------------------------------------------------


def _build_crawlers(config: EdgeRuntimeConfig) -> dict[str, CrawlerPort]:
    """Собирает карту ``engine → CrawlerPort`` (``http`` всегда; тяжёлые — по ``workers``).

    Тяжёлые движки (``scrapy``/``browser``) строятся только если в ``config.workers`` для
    них задано положительное число слотов; каждая сборка под guard — недоступность
    зависимости (``ImportError``/``CrawlError``) не валит узел, движок опускается.
    """
    crawlers: dict[str, CrawlerPort] = {"http": HttpCrawler()}
    for engine, factory in (("scrapy", ScrapyCrawler), ("browser", BrowserCrawler)):
        if int(config.workers.get(engine, 0)) <= 0:
            continue
        try:
            crawlers[engine] = factory()
        except (ImportError, CrawlError):
            continue
    return crawlers


class _EngineCrawler:
    """Диспетчер по оси ``fetch``: ``BROWSER`` → browser/scrapy, иначе ``http``.

    ``EdgeGateway`` принимает один ``CrawlerPort``; карта движков скрыта здесь. При
    запросе браузерного рендера выбирается browser-capable движок (``browser``, затем
    ``scrapy``); если тяжёлых нет — дефолтный ``http`` (best-effort, без падения).
    """

    def __init__(self, crawlers: dict[str, CrawlerPort], default: str = "http") -> None:
        self._crawlers = dict(crawlers)
        if default in self._crawlers:
            self._default = default
        elif self._crawlers:
            self._default = next(iter(self._crawlers))
        else:  # pragma: no cover — ``_build_crawlers`` всегда даёт ``http``
            self._default = default
        # Данные-атрибуты (а не property): контракт ``CrawlerPort.version`` — поле-строка.
        self.version = str(
            getattr(self._crawlers.get(self._default), "version", "") or ""
        )
        self.engine = self._default

    def _pick(self, plan: FetchPlan) -> str:
        """Движок по оси ``fetch``: ``BROWSER`` → browser/scrapy, иначе дефолтный ``http``."""
        if getattr(plan, "fetch", FetchMode.HTTP) is FetchMode.BROWSER:
            for engine in ("browser", "scrapy"):
                if engine in self._crawlers:
                    return engine
        return self._default

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        """Делегирует выбранному движку (SSRF/деградация — ответственность адаптера)."""
        crawler = self._crawlers.get(self._pick(plan)) or self._crawlers[self._default]
        return crawler.fetch(url, plan)


# ---------------------------------------------------------------------------
# Сборка composition root
# ---------------------------------------------------------------------------


def _build_gateway(
    config: EdgeRuntimeConfig,
) -> tuple[EdgeGateway, dict[str, CrawlerPort], ChunkerPort]:
    """Собирает адаптеры edge и ``EdgeGateway`` (ручной DI, A1).

    Возвращает ``(gateway, crawlers, chunker)``: ``crawlers``/``chunker`` — для статуса
    запуска (чанкер пока не протянут в шов: нарезка в ядре, I19); gateway публикует
    краулер-диспетчер (:class:`_EngineCrawler`) и композит-экстрактор.

    Диспетчер строится **один раз** и передаётся и в ``EdgeGateway``, и в
    :class:`FeedPollService` (поллер фетчит каталоговый документ тем же диспетчером,
    что и ``fetch``): единая точка выбора движка по оси ``fetch``.
    """
    crawlers = _build_crawlers(config)
    dispatcher = _EngineCrawler(crawlers)
    extractor: ExtractorPort = _EdgeExtractor()
    chunker: ChunkerPort = MarkdownChunkerPort()
    gateway = EdgeGateway(
        dispatcher,
        extractor,
        schema_version=config.schema_version,
        poller=FeedPollService(dispatcher),
    )
    return gateway, crawlers, chunker


def _build_scheduler(
    config: EdgeRuntimeConfig, crawlers: dict[str, CrawlerPort]
) -> EdgeScheduler:
    """Собирает ``EdgeScheduler``: ``http`` — in-process, ``scrapy``/``browser`` — процессы.

    В карту in-process попадает **только** ``http`` (живой адаптер уже собран
    ``_build_gateway``); ``scrapy``/``browser`` исполняются в процессах через
    :func:`core.edge.crawler_factory.crawler_factory` — и лишь если для движка задано
    положительное число слотов (фильтрация внутри планировщика: ``config.workers[engine] > 0``).
    Фабрика вынесена в отдельный импортируемый модуль (B1): при ``python -m core.edge``
    локальная фабрика имела бы ``__module__ == "__main__"`` и не была бы picklable для
    ``ProcessPoolExecutor``. Конструирование не стартует воркеры и не трогает диск
    (``start()``/БД — лениво), поэтому безопасно и для ``--check``.
    """
    in_process: dict[str, CrawlerPort] = {}
    http = crawlers.get("http")
    if http is not None:
        in_process["http"] = http
    return EdgeScheduler(config, in_process, crawler_factory=crawler_factory)


def _raise_keyboard_interrupt(signum: int, frame: Any) -> None:
    """Обработчик сигнала: ``SIGTERM`` → ``KeyboardInterrupt`` (graceful-останов)."""
    raise KeyboardInterrupt


@contextmanager
def _sigterm_graceful() -> Iterator[None]:
    """На время сервера переводит ``SIGTERM`` в ``KeyboardInterrupt`` (Phase 3.4a).

    Позволяет :func:`serve` развернуть ``finally`` (``scheduler.stop()``) при остановке
    узла штатным ``SIGTERM``. Если установка обработчика невозможна (не главный поток /
    среда без ``SIGTERM``) — тихо пропускается (поведение по умолчанию). Прежний
    обработчик восстанавливается на выходе.
    """
    previous: Any = None
    installed = False
    if hasattr(signal, "SIGTERM"):
        try:
            previous = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
            installed = True
        except (ValueError, OSError, RuntimeError):  # pragma: no cover - не главный поток
            installed = False
    try:
        yield
    finally:
        if installed:
            signal.signal(signal.SIGTERM, previous)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    """Собирает argparse-парсер edge-лаунчера (stdlib, без фреймворков)."""
    parser = argparse.ArgumentParser(
        prog="python -m core.edge",
        description="coHound edge-узел (Z1/DMZ): публикация веб-адаптеров по /v1/*.",
    )
    sub = parser.add_subparsers(dest="command", metavar="КОМАНДА")

    p_serve = sub.add_parser("serve", help="запустить gateway-сервер edge (блокирующе)")
    p_serve.add_argument(
        "--config",
        default=None,
        metavar="FILE",
        help="путь к edge.yaml (runtime-конфиг edge); отсутствие → env/дефолты",
    )
    p_serve.add_argument("--host", default=None, help="переопределить host прослушивания")
    p_serve.add_argument("--port", type=int, default=None, help="переопределить порт прослушивания")
    p_serve.add_argument(
        "--check",
        action="store_true",
        help="собрать конфиг+адаптеры+gateway и выйти без привязки сокета (smoke)",
    )
    return parser


def _system_exit_code(exc: SystemExit) -> int:
    """Нормализует код ``SystemExit`` argparse к целому (``--help`` → 0)."""
    if exc.code is None:
        return EXIT_OK
    if isinstance(exc.code, int):
        return exc.code
    return EXIT_ERROR


def main(argv: list[str] | None = None) -> int:
    """Точка входа edge-узла. Возвращает код выхода (``0`` — успех).

    ``serve`` собирает runtime-конфиг, адаптеры и планировщик краула, печатает стартовую
    строку и запускает блокирующий ``serve(..., scheduler=scheduler)``: сервер стартует
    планировщик до обслуживания и останавливает в ``finally`` (Phase 3.4a). ``SIGTERM``/
    ``KeyboardInterrupt`` трактуются как штатная остановка (код ``0``); ``scheduler.stop()``
    при этом вызывается ровно один раз — внутри ``serve``, не здесь.
    ``--check`` собирает всё (включая объект планировщика) и выходит без сокета и без старта
    воркеров.
    """
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return _system_exit_code(exc)

    if getattr(args, "command", None) != "serve":
        parser.print_help()
        return EXIT_USAGE

    try:
        config = load_edge_runtime_config(args.config)
    except ValueError as exc:
        sys.stderr.write(f"edge: {exc}\n")
        return EXIT_USAGE

    host = args.host if args.host is not None else config.host
    port = args.port if args.port is not None else config.port

    gateway, crawlers, _chunker = _build_gateway(config)
    scheduler = _build_scheduler(config, crawlers)
    engines = sorted(crawlers)

    if args.check:
        sys.stdout.write(
            "edge check ok: "
            f"schema={config.schema_version} engines={engines} "
            f"extractor={gateway.capabilities()['extractor_version']} "
            f"state_dir={config.state_dir} blob_dir={config.resolved_blob_dir} "
            f"scheduler=ready poller=ready\n"
        )
        scheduler.close()  # воркеры не стартовали; соединение с БД не открывалось
        return EXIT_OK

    sys.stdout.write(
        f"edge serve: http://{host}:{port} schema={config.schema_version} "
        f"engines={engines}\n"
    )
    sys.stdout.flush()
    try:
        with _sigterm_graceful():
            serve(host, port, gateway, scheduler=scheduler)
    except KeyboardInterrupt:
        # Штатная остановка: serve() уже вызвал scheduler.stop() в finally — не дублируем.
        return EXIT_OK
    finally:
        scheduler.close()
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover — точка входа модуля
    raise SystemExit(main())
