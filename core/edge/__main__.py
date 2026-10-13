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
  в ядре — I19, шов отдаёт сырые байты/текст).

Флаги: ``serve [--config FILE] [--host H] [--port P] [--check]``. ``--check`` собирает
конфиг+адаптеры+gateway и выходит без привязки сокета (smoke).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import sys
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
from core.edge.server import EdgeGateway, serve
from core.ports.chunker import ChunkerPort
from core.ports.crawler import CrawlerPort
from core.ports.extractor import ExtractorPort
from core.ports.types import ExtractionRoute, FetchMode, FetchPlan, RawPage

__all__ = ["main"]

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
    краулер-диспетчер и композит-экстрактор.
    """
    crawlers = _build_crawlers(config)
    extractor: ExtractorPort = _EdgeExtractor()
    chunker: ChunkerPort = MarkdownChunkerPort()
    gateway = EdgeGateway(
        _EngineCrawler(crawlers),
        extractor,
        schema_version=config.schema_version,
    )
    return gateway, crawlers, chunker


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

    ``serve`` собирает runtime-конфиг и адаптеры, печатает стартовую строку и запускает
    блокирующий ``serve(..., scheduler=None)`` (хук планировщика — Wave 3.2).
    ``--check`` собирает всё и выходит без сокета.
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
    engines = sorted(crawlers)

    if args.check:
        sys.stdout.write(
            "edge check ok: "
            f"schema={config.schema_version} engines={engines} "
            f"extractor={gateway.capabilities()['extractor_version']} "
            f"state_dir={config.state_dir} blob_dir={config.resolved_blob_dir}\n"
        )
        return EXIT_OK

    sys.stdout.write(
        f"edge serve: http://{host}:{port} schema={config.schema_version} "
        f"engines={engines}\n"
    )
    sys.stdout.flush()
    serve(host, port, gateway, scheduler=None)
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover — точка входа модуля
    raise SystemExit(main())
