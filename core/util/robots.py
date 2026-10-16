"""coHound — per-host политика ``robots.txt`` (чистый stdlib; D1, §10).

Единая реализация робот-этики: разбор ``robots.txt`` через ``urllib.robotparser`` с
**инъектируемым** фетчем текста ленты (в core нет сетевого I/O — в реальном пути ленту
приносит egress-слой/DMZ, а не ядро). Класс промотирован сюда из
``core.pipeline.crawl_probe`` (единственная реализация; запрос-зонд импортирует его отсюда).

**Семантика.** Недоступная лента (``None``/исключение) трактуется как «правил нет» (как 404
в штатной семантике robots) — это **дефолт-разрешение**, а не молчаливый обход. ``allowed``
возвращает ``False`` только при явном ``Disallow`` для нашего агента. Сбой разбора не роняет
краул (``True``).

``crawl_delay`` отдаёт объявленную агенту задержку (сек), если она есть: адаптеры могут
использовать её как **floor** (не как источник пейсинга — им владеет corpus, P2-3);
явный ``rate_limit`` всегда важнее.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from collections.abc import Callable
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

__all__ = ["RobotsGate"]


class RobotsGate:
    """Per-host политика ``robots.txt`` (разбор ``urllib.robotparser``, инъектируемый фетч).

    ``fetch(host)`` → текст ``robots.txt`` либо ``None`` (недоступен). Недоступная лента
    трактуется как «правил нет» (как 404 в штатной семантике robots). Проверка — только
    запрет: ``allowed`` возвращает ``False`` лишь при явном ``Disallow``.
    """

    def __init__(
        self, fetch: Callable[[str], str | None], *, user_agent: str = "*"
    ) -> None:
        self._fetch = fetch
        self._user_agent = user_agent
        self._parsers: dict[str, RobotFileParser] = {}

    def _parser(self, host: str) -> RobotFileParser:
        parser = self._parsers.get(host)
        if parser is not None:
            return parser
        text: str | None
        try:
            text = self._fetch(host)
        except Exception:  # noqa: BLE001 — недоступный robots.txt = «правил нет»
            text = None
        parser = RobotFileParser()
        parser.parse((text or "").splitlines())
        self._parsers[host] = parser
        return parser

    def allowed(self, url: str) -> bool:
        """Разрешён ли ``url`` для нашего агента (``True`` при отсутствии запрета)."""
        host = (urlsplit(url).hostname or "").lower()
        if not host:
            return True
        try:
            return bool(self._parser(host).can_fetch(self._user_agent, url))
        except Exception:  # noqa: BLE001 — не роняем зонд из-за разбора robots
            return True

    def crawl_delay(self, url: str) -> float | None:
        """Объявленная агенту задержка (сек) из ``Crawl-delay``; ``None`` — не задана.

        Возвращается как **floor** для пейсинга: адаптеры применяют её лишь при отсутствии
        явного ``rate_limit`` (явный лимит всегда важнее). Недоступная лента/сбой разбора →
        ``None``.
        """
        host = (urlsplit(url).hostname or "").lower()
        if not host:
            return None
        try:
            value = self._parser(host).crawl_delay(self._user_agent)
        except Exception:  # noqa: BLE001 — сбой разбора robots не роняет краул
            return None
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
