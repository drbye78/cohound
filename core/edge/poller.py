"""coHound — edge-сервис опроса фидов/каналов (``crawl/poll``; Wave 5.1).

Сервис **чистый по отношению к хранилищу**: не пишет в БД/индексы, не является
evidence. Он фетчит **только каталоговый документ** фида/канала (через инъецированный
``CrawlerPort``) и возвращает **ссылки** на записи (URL+метаданные) — сырые байты
записей здесь не добываются (их фетчит очередь краула отдельно, I19).

Границы слоя — edge: допускаются ``ports``/``domain``/``util``; **никогда**
``store``/``pipeline``/``cli``. Опора — stdlib + ``util``. Нейтрален (§1.3).

Безопасность. URL самого каталога и **каждый** URL записи проходят ``validate_url``
(SSRF) перед возвратом; сервис не фетчит записи. Пустые/небезопасные URL записей
отбрасываются. Разбор XML — через ``core.util.feed.parse_feed`` / ``core.util.sitemap``
(defusedxml); для sitemap edge сам разворачивает ``sitemapindex`` на один уровень (D3).
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from typing import Any

from core.edge.server import EdgeError
from core.ports.crawler import CrawlerPort
from core.ports.types import (
    CrawlStrategy,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    RawPage,
)
from core.util.feed import (
    FeedEntry,
    FeedParseError,
    entry_not_before,
    parse_channel_document,
    parse_feed,
    parse_instant,
)
from core.util.security import validate_url
from core.util.sitemap import (
    MAX_SITEMAP_ENTRIES,
    SitemapDocument,
    SitemapParseError,
    decompress_if_gzip,
    expand_sitemap,
    parse_sitemap,
)

__all__ = ["FeedPollService"]

#: Виды каталогового документа, поддерживаемые опросом (fixed contract): фид, канал, sitemap.
_KIND_FEED = "feed"
_KIND_CHANNEL = "channel"
_KIND_SITEMAP = "sitemap"
SUPPORTED_KINDS: frozenset[str] = frozenset({_KIND_FEED, _KIND_CHANNEL, _KIND_SITEMAP})

#: Допустимые значения осей фетча/экстракции/стратегии (совпадают со seam-контрактом).
_FETCH_VALUES = frozenset(mode.value for mode in FetchMode)
_EXTRACT_VALUES = frozenset(route.value for route in ExtractionRoute)
_STRATEGY_VALUES = frozenset(strategy.value for strategy in CrawlStrategy)


class FeedPollService:
    """Опрос фида/канала: фетч каталогового документа → список ссылок на записи.

    :param crawler: инъецированный ``CrawlerPort`` (в edge — реальный веб-адаптер),
        которым фетчится **каталоговый документ**; записи не фетчатся.
    :param poll_concurrency: потолок одновременных опросов (семафор); исчерпание →
        ``EdgeError("poll busy")`` (неблокирующий захват).
    :param max_feed_bytes: потолок размера каталогового документа (байт); излишек
        усекается, и при последующем сбое разбора выдаётся понятная ошибка размера.
    :param max_entries: жёсткий потолок числа записей в ответе (``params.limit`` не
        может его превысить).
    :param clock: часы ``() -> str`` (зарезервировано; детерминизм тестов).
    """

    def __init__(
        self,
        crawler: CrawlerPort,
        *,
        poll_concurrency: int = 4,
        max_feed_bytes: int = 2_000_000,
        max_entries: int = 500,
        clock: Callable[[], str] | None = None,
    ) -> None:
        self._crawler = crawler
        self._max_feed_bytes = max(1, int(max_feed_bytes))
        self._max_entries = max(1, int(max_entries))
        self._clock = clock
        self._semaphore = threading.Semaphore(max(1, int(poll_concurrency)))

    # -- публичный контракт -----------------------------------------------------

    def poll(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Опрашивает фид/канал и возвращает ``{entries, count, truncated}``.

        Операция определена как POST ``/v1/crawl/poll``. Только **чтение**: каталоговый
        документ фетчится, из него извлекаются ссылки; ничего не подаётся в очередь и не
        персистится.

        :raises EdgeError: занятый семафор (``poll busy``); небезопасный URL фида;
            сбой фетча адаптера; небезопасный/некорректный документ; превышение размера.
        """
        url = _clean(getattr(request, "get", lambda *_: None)("url"))
        if not url:
            raise EdgeError("crawl/poll: поле url обязательно")
        kind = _clean(request.get("kind")).lower()
        if kind not in SUPPORTED_KINDS:
            raise EdgeError(
                f"crawl/poll: поле kind должно быть одним из {sorted(SUPPORTED_KINDS)}"
            )
        params = request.get("params")
        params = dict(params) if isinstance(params, Mapping) else {}
        plan = self._build_plan(request)

        # SSRF каталога — до фетча (сервис возвращает недоверенные ссылки, но сам ходит строго
        # по валидному публичному http(s)).
        self._validate(url, kind)

        if not self._semaphore.acquire(blocking=False):
            raise EdgeError("poll busy")
        try:
            page = self._fetch(url, plan)
        finally:
            self._semaphore.release()

        content = bytes(getattr(page, "content", b"") or b"")
        content_type = getattr(page, "content_type", None)
        if kind == _KIND_SITEMAP:
            return self._poll_sitemap(url, content, plan, params, content_type)

        was_truncated = len(content) > self._max_feed_bytes
        if was_truncated:
            content = content[: self._max_feed_bytes]

        raw_entries = self._parse(kind, content, content_type, url, was_truncated)
        return self._finalize(raw_entries, params, was_truncated)

    # -- фетч/разбор ------------------------------------------------------------

    def _build_plan(self, request: Mapping[str, Any]) -> FetchPlan:
        """Собирает ``FetchPlan`` из осей запроса (мягкая деградация к дефолтам)."""
        fetch = _coerce_enum(FetchMode, request.get("fetch"), FetchMode.HTTP, _FETCH_VALUES)
        extract = _coerce_enum(
            ExtractionRoute, request.get("extract"), ExtractionRoute.AUTO, _EXTRACT_VALUES
        )
        strategy = _coerce_enum(
            CrawlStrategy, request.get("strategy"), CrawlStrategy.AUTO, _STRATEGY_VALUES
        )
        wait_for = _clean(request.get("wait_for")) or None
        return FetchPlan(fetch=fetch, extract=extract, strategy=strategy, wait_for=wait_for)

    def _fetch(self, url: str, plan: FetchPlan) -> RawPage:
        """Фетчит каталоговый документ инъецированным краулером (без записи)."""
        try:
            page = self._crawler.fetch(url, plan)
        except EdgeError:
            raise
        except Exception as exc:  # noqa: BLE001 - грациозная граница адаптера
            raise EdgeError(f"crawl/poll: fetch: {exc}") from exc
        if not isinstance(page, RawPage):
            raise EdgeError("crawl/poll: краулер вернул не RawPage")
        return page

    def _parse(
        self,
        kind: str,
        content: bytes,
        content_type: Any,
        url: str,
        was_truncated: bool,
    ) -> list[FeedEntry]:
        """Разбирает документ в записи (feed — XML, channel — JSON/список URL)."""
        try:
            if kind == _KIND_FEED:
                return parse_feed(content, content_type=_mime(content_type), base_url=url)
            return parse_channel_document(
                content, content_type=_mime(content_type), base_url=url
            )
        except FeedParseError as exc:
            raise self._size_error(exc, was_truncated) from exc

    def _poll_sitemap(
        self,
        url: str,
        content: bytes,
        plan: FetchPlan,
        params: Mapping[str, Any],
        content_type: Any,
    ) -> dict[str, Any]:
        """Разбирает sitemap и (для ``sitemapindex``) разворачивает **один** уровень (D3).

        Сам sitemap — не страница (не ретейнится/не ингестится). Дочерние sitemap фетчатся
        тем же краулером (robots/politeness — политика транспорта); каждый дочерний URL
        проходит ``validate_url`` (SSRF) **до** фетча. Потолки — ``MAX_CHILD_SITEMAPS``/
        ``MAX_SITEMAP_ENTRIES`` (см. ``core.util.sitemap``); вложенный индекс → ``EdgeError``.
        Ссылки ``<loc>`` — кандидаты (не evidence, I19): байты страниц здесь не добываются.
        """
        data = decompress_if_gzip(content)
        was_truncated = len(data) > self._max_feed_bytes
        if was_truncated:
            data = data[: self._max_feed_bytes]

        try:
            root = parse_sitemap(data, content_type=_mime(content_type), base_url=url)
        except SitemapParseError as exc:
            raise self._size_error(exc, was_truncated) from exc

        def fetch_child(child_url: str) -> SitemapDocument:
            try:
                validate_url(child_url)
            except ValueError as exc:
                raise EdgeError(
                    f"crawl/poll: небезопасный URL дочернего sitemap: {exc}"
                ) from exc
            page = self._fetch(child_url, plan)
            child_bytes = decompress_if_gzip(bytes(getattr(page, "content", b"") or b""))
            if len(child_bytes) > self._max_feed_bytes:
                child_bytes = child_bytes[: self._max_feed_bytes]
            try:
                return parse_sitemap(
                    child_bytes,
                    content_type=_mime(getattr(page, "content_type", None)),
                    base_url=child_url,
                )
            except SitemapParseError as exc:
                raise EdgeError(f"crawl/poll: дочерний sitemap: {exc}") from exc

        try:
            entries, expand_truncated = expand_sitemap(root, fetch_child)
        except SitemapParseError as exc:
            raise EdgeError(f"crawl/poll: {exc}") from exc

        return self._finalize(
            entries,
            params,
            was_truncated or expand_truncated,
            max_entries=MAX_SITEMAP_ENTRIES,
        )

    def _size_error(self, exc: Exception, was_truncated: bool) -> EdgeError:
        """Ошибка разбора: при усечении по ``max_feed_bytes`` — понятное сообщение о размере."""
        if was_truncated:
            return EdgeError(
                f"crawl/poll: документ превысил лимит {self._max_feed_bytes} байт "
                "и был усечён до неразбираемого состояния"
            )
        return EdgeError(f"crawl/poll: parse: {exc}")

    # -- нормализация/caps ------------------------------------------------------

    def _finalize(
        self,
        entries: list[FeedEntry],
        params: Mapping[str, Any],
        input_truncated: bool,
        max_entries: int | None = None,
    ) -> dict[str, Any]:
        """Применяет caps (``since``/``limit``), SSRF-фильтр записей, собирает ответ.

        ``input_truncated`` — документ был усечён по ``max_feed_bytes``/потолкам sitemap до
        разбора; признак ``truncated`` ответа объединяет его с усечением по лимиту записей.
        ``max_entries`` — жёсткий потолок ответа (по умолчанию ``self._max_entries``; для
        sitemap — ``MAX_SITEMAP_ENTRIES``).
        """
        since = _clean(params.get("since")) or None
        threshold = parse_instant(since) if since is not None else None
        requested_limit = _opt_int(params.get("limit"))
        effective_limit = self._max_entries if max_entries is None else max_entries
        if requested_limit is not None and requested_limit > 0:
            effective_limit = min(requested_limit, effective_limit)

        selected: list[FeedEntry] = []
        truncated = bool(input_truncated)
        for entry in entries:
            # N1: семантика ``since`` общая с локальным поллером (разбор метки, не строковое
            # сравнение); неизвестная/неразборчивая дата записи — консервативно сохраняется.
            if threshold is not None and not entry_not_before(entry, threshold):
                continue
            entry_url = _clean(entry.url)
            if not entry_url:
                continue
            try:
                validate_url(entry_url)
            except ValueError:
                # Небезопасный (SSRF)/неподдерживаемый URL записи — пропускаем, не роняя опрос.
                continue
            if len(selected) >= effective_limit:
                truncated = True
                break
            selected.append(entry)

        return {
            "entries": [_entry_dict(entry) for entry in selected],
            "count": len(selected),
            "truncated": truncated,
        }

    def _validate(self, url: str, what: str) -> None:
        """SSRF-валидация URL (фида); нарушение → ``EdgeError``."""
        try:
            validate_url(url)
        except ValueError as exc:
            raise EdgeError(f"crawl/poll: небезопасный URL {what}: {exc}") from exc


# ---------------------------------------------------------------------------
# Мелкие нормализаторы
# ---------------------------------------------------------------------------


def _entry_dict(entry: FeedEntry) -> dict[str, Any]:
    """Ссылка на запись в форме seam-контракта (URL+метаданные, без байтов)."""
    return {
        "url": entry.url,
        "guid": entry.guid,
        "published_at": entry.published_at,
        "title": entry.title,
    }


def _clean(value: Any) -> str:
    """Строковое значение без управляющих символов и обрамляющих пробелов."""
    if value is None:
        return ""
    return str(value).strip()


def _mime(content_type: Any) -> str | None:
    """MIME без параметров (``text/xml; charset=…`` → ``text/xml``); ``None`` → ``None``."""
    if content_type is None:
        return None
    text = str(content_type).split(";", 1)[0].strip()
    return text or None


def _opt_int(value: Any) -> int | None:
    """``None``/нечисло → ``None``; иначе ``int``."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_enum(
    enum_cls: Any, value: Any, default: Any, allowed: frozenset[str]
) -> Any:
    """Приводит строку к элементу enum; неизвестное/пустое → ``default``."""
    if value in (None, ""):
        return default
    key = str(value).strip().lower()
    if key not in allowed:
        return default
    return enum_cls(key)
