"""Опрос фидов/каналов/sitemap — ``LocalFeedPoller`` и ``RemoteFeedPoller`` (Phase 5.1/D3; §3.2, Ш5).

Реализации :class:`~core.ports.poller.PollPort`: получают публичный каталоговый документ
(лента/канал/sitemap) и возвращают список ссылок-кандидатов
:class:`~core.util.feed.FeedEntry` (не evidence, I19).

* :class:`LocalFeedPoller` — in-process (профиль ``local``): фетчит каталог локальным
  ``CrawlerPort`` (сырые байты), разбирает ``core.util.feed``/``core.util.sitemap`` по
  ``kind`` и применяет ограничения ``params``.
* :class:`RemoteFeedPoller` — прокси поверх :class:`~core.ports.gateway.ContentGateway`
  (профиль ``split``): DMZ исполняет опрос (op ``crawl/poll``), ядро валидирует форму
  ответа и **повторно** проверяет URL записей (SSRF; edge не доверяем, 07 §5).

**``kind`` и sitemap (D3).** ``feed`` — XML RSS/Atom/RDF; ``channel`` — JSON/JSON-lines/
список URL; ``sitemap`` — ``sitemap.xml``/``sitemap.xml.gz`` (urlset; ``sitemapindex``
разворачивается ровно на один уровень). Для ``sitemap`` в ``split`` DMZ исполняет и
развёртку индекса, ядру возвращаются уже плоские ссылки (I19).

**Границы доверия.** Поллер производит кандидатов, а не evidence (I19): SSRF-проверка
(``core.util.security.validate_url``) выполняется на входе (URL ленты) и на каждой записи.
Небезопасные записи **отбрасываются** (одна враждебная ссылка в публичной ленте не должна
отменять весь опрос); постановка в корпус и финальная проверка — на границе corpus enqueue.

**Секреты.** Токены доступа к каналу передаются только через ``token_env`` (env/deploy);
инлайн-ключи вида ``token``/``api_key``/``authorization`` отвергаются :meth:`validate_params`.
Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeGuard

from core.adapters.remote_gateway import RemoteContentError
from core.ports.crawler import CrawlerPort
from core.ports.gateway import ContentGateway
from core.ports.types import FetchPlan
from core.util.feed import (
    FeedEntry,
    entry_not_before,
    parse_channel_document,
    parse_feed,
    parse_instant,
)
from core.util.security import validate_url
from core.util.sitemap import (
    SitemapDocument,
    decompress_if_gzip,
    expand_sitemap,
    parse_sitemap,
)

__all__ = ["LocalFeedPoller", "RemoteFeedPoller"]

#: Поддерживаемые виды опроса: XML-фид, «канальный» документ и sitemap (D3).
_SUPPORTED_KINDS: frozenset[str] = frozenset({"feed", "channel", "sitemap"})

#: Фрагменты имени параметра, выдающие инлайн-секрет. ``token_env`` — легитимен и
#: исключается явно (секрет берётся из окружения, а не передаётся значением).
_SECRET_KEY_FRAGMENTS: tuple[str, ...] = (
    "token",
    "secret",
    "password",
    "passwd",
    "api_key",
    "apikey",
    "authorization",
    "auth",
)
#: Точные имена секрет-подобных ключей (не покрытые фрагментами).
_SECRET_KEY_NAMES: frozenset[str] = frozenset({"bearer", "credentials", "credential"})


# ---------------------------------------------------------------------------
# Хелперы значений
# ---------------------------------------------------------------------------


def _is_int(value: Any) -> TypeGuard[int]:
    """``int``, но не ``bool`` (``bool`` — подкласс ``int``)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _opt_str(value: Any) -> str | None:
    """Непустая строка либо ``None`` (необязательные метаданные записи)."""
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _is_secret_key(key: str) -> bool:
    """Является ли имя параметра секрет-подобным (инлайн недопустим; см. ``token_env``)."""
    name = key.strip().lower()
    if name == "token_env":
        return False
    if name in _SECRET_KEY_NAMES:
        return True
    return any(fragment in name for fragment in _SECRET_KEY_FRAGMENTS)


def _params_messages(kind: str, params: Mapping[str, Any]) -> list[str]:
    """Общая проверка ``kind``/``params`` для обоих поллеров (см. контракт ``PollPort``)."""
    messages: list[str] = []
    if kind not in _SUPPORTED_KINDS:
        messages.append(
            f"kind={kind!r} недопустим: ожидается один из {sorted(_SUPPORTED_KINDS)}"
        )

    if not isinstance(params, Mapping):
        messages.append("params обязан быть отображением")
        return messages

    for key in params:
        if not isinstance(key, str):
            messages.append(f"ключ параметра {key!r} обязан быть строкой")
            continue
        if _is_secret_key(key):
            messages.append(
                f"params.{key}: секреты передаются только через token_env (env/deploy), "
                "не инлайном"
            )

    limit = params.get("limit")
    if limit is not None and not (_is_int(limit) and limit > 0):
        messages.append(f"params.limit={limit!r} недопустим: ожидается целое > 0")

    since = params.get("since")
    if since is not None:
        if not isinstance(since, str) or parse_instant(since) is None:
            messages.append(
                f"params.since={since!r} недопустим: ожидается ISO-8601/RFC822-метка времени"
            )

    return messages


def _apply_params(
    entries: list[FeedEntry], params: Mapping[str, Any], max_entries: int
) -> list[FeedEntry]:
    """Применяет caps ``params``: ``limit`` (≤ ``max_entries``) и фильтр ``since``."""
    cap: int = int(max_entries)
    limit = params.get("limit")
    if _is_int(limit):
        effective = int(limit)
        if effective > 0:
            cap = min(cap, effective) if cap > 0 else effective
    if cap > 0:
        entries = entries[:cap]

    since = params.get("since")
    if since:
        threshold = parse_instant(str(since))
        if threshold is not None:
            entries = [entry for entry in entries if entry_not_before(entry, threshold)]
    return entries


def _safe_entries(entries: list[FeedEntry]) -> list[FeedEntry]:
    """Отбрасывает записи с небезопасным URL (SSRF; edge/ленте не доверяем — 07 §5)."""
    safe: list[FeedEntry] = []
    for entry in entries:
        try:
            validate_url(entry.url)
        except ValueError:
            continue
        safe.append(entry)
    return safe


# ---------------------------------------------------------------------------
# Local
# ---------------------------------------------------------------------------


class LocalFeedPoller:
    """``PollPort`` in-process: локальный краулер + ``core.util.feed``/``sitemap`` (``local``).

    :param crawler: краулер для получения байтов каталогового документа (:class:`CrawlerPort`).
    :param max_feed_bytes: потолок размера байтов документа (обрезка перед разбором; для
        sitemap применяется к распакованному gzip; защита ядра от раздутого документа).
        ``<= 0`` — без потолка.
    :param max_entries: потолок числа записей-кандидатов (обрезка результата). ``<= 0`` —
        без потолка.
    """

    def __init__(
        self,
        crawler: CrawlerPort,
        *,
        max_feed_bytes: int = 2_000_000,
        max_entries: int = 500,
        version: str = "local-feed-poller-1",
    ) -> None:
        self._crawler = crawler
        self._max_feed_bytes = int(max_feed_bytes)
        self._max_entries = int(max_entries)
        self.version = version

    def validate_params(self, kind: str, params: Mapping[str, Any]) -> list[str]:
        """Проверяет ``kind``/``params`` (пустой список = ок)."""
        return _params_messages(kind, params)

    def poll(
        self,
        url: str,
        *,
        kind: str,
        params: Mapping[str, Any],
        plan: FetchPlan,
    ) -> list[FeedEntry]:
        """Фетчит каталог, разбирает по ``kind`` и применяет caps ``params``.

        :raises ValueError: небезопасный/недопустимый URL каталога (SSRF) либо неизвестный
            ``kind``.
        :raises FeedParseError: небезопасный/некорректный фид/канал.
        :raises SitemapParseError: небезопасный/некорректный sitemap (в т.ч. gzip-бомба,
            вложенный ``sitemapindex``).
        """
        validate_url(url)
        page = self._crawler.fetch(url, plan)
        content = page.content
        content_type = getattr(page, "content_type", None)

        if kind == "sitemap":
            # gzip распаковывается ДО байтового потолка (иначе усечение ломает поток).
            entries = self._poll_sitemap(url, content, plan)
        else:
            if self._max_feed_bytes > 0 and len(content) > self._max_feed_bytes:
                content = content[: self._max_feed_bytes]

            if kind == "feed":
                entries = parse_feed(content, content_type=content_type, base_url=url)
            elif kind == "channel":
                entries = parse_channel_document(content, content_type=content_type, base_url=url)
            else:
                raise ValueError(
                    f"неизвестный kind опроса {kind!r}: ожидается один из {sorted(_SUPPORTED_KINDS)}"
                )

        entries = _apply_params(entries, params, self._max_entries)
        return _safe_entries(entries)

    def _bounded_sitemap_bytes(self, content: bytes) -> bytes:
        """Распаковывает gzip (по magic) и применяет потолок ``max_feed_bytes`` к результату."""
        data = decompress_if_gzip(content)
        if self._max_feed_bytes > 0 and len(data) > self._max_feed_bytes:
            data = data[: self._max_feed_bytes]
        return data

    def _poll_sitemap(self, url: str, content: bytes, plan: FetchPlan) -> list[FeedEntry]:
        """Разбирает sitemap и (для ``sitemapindex``) разворачивает **один** уровень (D3).

        Сам sitemap — не страница (не ретейнится/не ингестится). Дочерние sitemap фетчатся
        тем же краулером (robots/politeness — политика транспорта); каждый дочерний URL
        проходит ``validate_url`` (SSRF) **до** фетча. Потолки развёртки —
        ``MAX_CHILD_SITEMAPS``/``MAX_SITEMAP_ENTRIES``; вложенный индекс — отказ.
        """
        root = parse_sitemap(self._bounded_sitemap_bytes(content), base_url=url)

        def fetch_child(child_url: str) -> SitemapDocument:
            validate_url(child_url)
            page = self._crawler.fetch(child_url, plan)
            return parse_sitemap(
                self._bounded_sitemap_bytes(page.content),
                content_type=getattr(page, "content_type", None),
                base_url=child_url,
            )

        entries, _truncated = expand_sitemap(root, fetch_child)
        return entries


# ---------------------------------------------------------------------------
# Remote
# ---------------------------------------------------------------------------


class RemoteFeedPoller:
    """``PollPort`` поверх DMZ-шва (op ``crawl/poll``; профиль ``split``).

    Виды ``feed``/``channel``/``sitemap`` исполняет DMZ (веб-эгресс только через шов, 07 §4):
    для ``sitemap`` edge разворачивает (при необходимости) ``sitemapindex`` и возвращает
    плоские ссылки; ядро валидирует форму ответа и повторно проверяет URL записей (SSRF).

    :param gateway: ``ContentGateway`` — транспорт ядро→edge (07 §3).
    :param max_entries: потолок числа записей-кандидатов (обрезка результата). ``<= 0`` —
        без потолка.
    :param version: версия адаптера (провенанс/манифест).
    """

    def __init__(
        self,
        gateway: ContentGateway,
        *,
        max_entries: int = 500,
        version: str = "remote-feed-poller-1",
    ) -> None:
        self._gateway = gateway
        self._max_entries = int(max_entries)
        self.version = version

    def validate_params(self, kind: str, params: Mapping[str, Any]) -> list[str]:
        """Проверяет ``kind``/``params`` (пустой список = ок)."""
        return _params_messages(kind, params)

    def poll(
        self,
        url: str,
        *,
        kind: str,
        params: Mapping[str, Any],
        plan: FetchPlan,
    ) -> list[FeedEntry]:
        """``POST /v1/crawl/poll`` → список записей (кандидаты, I19).

        Оси получения сериализуются как в :class:`~core.adapters.remote_crawler.RemoteCrawler`
        (``fetch``/``extract``/``strategy``/``wait_for``). Форма ответа
        (``entries``/``count``/``truncated``) валидируется минимально (в стиле
        :mod:`core.adapters.gateway_validate`; ``core.edge`` не импортируется).

        :raises RemoteContentError: не-``Mapping`` ответ, неверная форма ответа,
            небезопасный URL ленты либо структурно некорректная запись.
        """
        try:
            validate_url(url)
        except ValueError as exc:
            raise RemoteContentError(f"crawl/poll: небезопасный url ленты ({exc})") from exc

        payload = {
            "url": url,
            "kind": kind,
            "params": dict(params or {}),
            "fetch": plan.fetch.value,
            "extract": plan.extract.value,
            "strategy": plan.strategy.value,
            "wait_for": plan.wait_for,
        }
        response = self._gateway.call("crawl/poll", payload)
        if not isinstance(response, Mapping):
            raise RemoteContentError(
                "crawl/poll: payload ответа edge не является отображением"
            )

        raw_entries = response.get("entries")
        if not isinstance(raw_entries, list):
            raise RemoteContentError("crawl/poll: поле entries должно быть списком")
        count = response.get("count")
        if count is not None and not _is_int(count):
            raise RemoteContentError("crawl/poll: поле count должно быть целым числом")
        truncated = response.get("truncated")
        if truncated is not None and not isinstance(truncated, bool):
            raise RemoteContentError("crawl/poll: поле truncated должно быть булевым")

        entries = [self._entry(item) for item in raw_entries]
        if self._max_entries > 0 and len(entries) > self._max_entries:
            entries = entries[: self._max_entries]
        return _safe_entries(entries)

    @staticmethod
    def _entry(item: Any) -> FeedEntry:
        """Собирает ``FeedEntry`` из элемента ``entries`` ответа ``crawl/poll``."""
        if not isinstance(item, Mapping):
            raise RemoteContentError("crawl/poll.entries: элемент должен быть объектом")
        url = item.get("url")
        if not isinstance(url, str) or not url:
            raise RemoteContentError("crawl/poll.entries: url — непустая строка")
        return FeedEntry(
            url=url,
            guid=_opt_str(item.get("guid")),
            published_at=_opt_str(item.get("published_at")),
            title=_opt_str(item.get("title")),
        )
