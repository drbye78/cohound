"""Порт исполнителя краула — ``CrawlExecutorPort`` (Phase 3.3; 07 §4, 02 §B.1).

Единый контракт **исполнения** краула для профиля ``split``: ядро формулирует набор
работ (``CrawlWorkItem``) и получает прогресс/результаты, а **где** это исполняется
(in-process локальный цикл или edge-планировщик DMZ) — деталь адаптера. Локальный путь
(``build_corpus`` с ``crawler``/``crawlers``) порт не использует: он остаётся прежним.

Граница I19: порт возвращает **сырые байты** результата (``CrawlResultItem.raw_bytes``),
а не evidence. Ядро само пишет retention и **пересчитывает** ``content_hash`` (§5);
хэшу исполняющей стороны (edge) ядро не доверяет.

Словарь (requests/responses шва ``/v1/crawl/*``) — Phase 3.1; здесь только нейтральные
транспорт-агностичные value-объекты. Порты могут импортировать домен (оси краула —
доменные enum'ы). Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.domain.models import RateLimitSpec

__all__ = [
    "ACTIVE_JOB_STATUSES",
    "CrawlBatch",
    "CrawlExecutorPort",
    "CrawlProgress",
    "CrawlResultItem",
    "CrawlSubmitResult",
    "CrawlWorkItem",
]

#: Статусы задания, считающиеся **не**-терминальными (опрос продолжается). Всё прочее —
#: терминальный исход (``completed``/``partial``/``cancelled``/``failed``). Общий контракт
#: вызывающего и адаптеров: ``collect`` имеет смысл после терминального статуса.
ACTIVE_JOB_STATUSES = frozenset({"pending", "running", "queued", "submitted"})


@dataclass(frozen=True)
class CrawlWorkItem:
    """Одна единица работы краула: URL продукта + разрешённые оси получения/извлечения.

    Оси уже **разрешены** в pipeline (:func:`core.pipeline.crawl_sources.resolve_crawl_target`)
    и передаются готовыми: исполнитель (edge) их лишь исполняет. ``engine``/``kind`` —
    свободные строки-данные (движок транспорта и вид источника), ``None`` — не задано
    на слое источников.

    ``robots`` — флаг egress-вежливости (§10): включать ли проверку ``robots.txt`` для
    этого элемента (политика, не методология). ``rate_limit`` — **резолвнутый per-item**
    диапазон задержек egress (:class:`~core.domain.models.RateLimitSpec`); ``None`` ⇒ без
    пейсинга (дефолт сохраняет байт-идентичность прогонов).

    ``if_none_match``/``if_modified_since`` — валидаторы условного повторного получения
    (additive): ``None`` ⇒ безусловный GET, байт-идентичный прежнему поведению.
    """

    product_id: str
    url: str
    engine: str | None = None
    kind: str | None = None
    fetch: FetchMode = FetchMode.HTTP
    extract: ExtractionRoute = ExtractionRoute.AUTO
    strategy: CrawlStrategy = CrawlStrategy.AUTO
    wait_for: str | None = None
    lang: str | None = None
    robots: bool = False
    rate_limit: RateLimitSpec | None = None
    if_none_match: str | None = None
    if_modified_since: str | None = None


@dataclass(frozen=True)
class CrawlProgress:
    """Снимок прогресса задания краула (агрегат счётчиков очереди).

    ``status`` — строковый статус задания исполнителя (``pending``/``running``/
    ``completed``/``partial``/``cancelled``/``failed``; терминальность определяет
    вызывающий). ``engines``/``kinds`` — фактически задействованные движки/виды (провенанс).
    """

    job_id: str
    status: str
    total: int = 0
    pending: int = 0
    running: int = 0
    fetched: int = 0
    failed: int = 0
    cancelled: int = 0
    engines: list[str] = field(default_factory=list)
    kinds: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CrawlResultItem:
    """Результат по одному URL: провенанс попытки + (best-effort) сырые байты.

    ``raw_bytes`` — сырые байты, декодированные из шва (retention делает **ядро**);
    ``None`` означает «байтов нет» (сбой источника/извлечения — смотреть ``error``).
    ``content_hash`` — хэш, **пересчитанный ядром** по декодированным байтам (I19):
    заявленному исполнителем значению ядро не доверяет. ``engine``/``kind`` — фактически
    применённый транспорт и вид источника (провенанс элемента сеанса). ``final_url`` —
    финальный URL после редиректов (``None`` ⇒ трактуется как ``url``); ядро берёт его
    как ``RawPage.url`` (база ссылок, redirect-scope), ключ элемента сеанса остаётся
    запрошенным ``url``.

    ``etag``/``last_modified``/``not_modified`` — валидаторы ответа и признак ``304``
    (additive): ``not_modified=True`` означает, что ``raw_bytes`` — **закэшированное**
    представление, подтверждённое сервером; пустых байтов в этом случае не бывает.
    """

    product_id: str
    url: str
    status: str
    engine: str | None = None
    kind: str | None = None
    http_status: int | None = None
    content_type: str | None = None
    fetched_at: str | None = None
    content_hash: str | None = None
    raw_bytes: bytes | None = None
    error: str | None = None
    final_url: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    not_modified: bool = False


@dataclass(frozen=True)
class CrawlBatch:
    """Страница результатов сбора (``crawl/collect``): ``items`` + курсор пагинации.

    ``done`` — последняя страница; ``next_cursor`` — курсор следующей (``None``/``done``
    завершают обход). Порядок ``items`` — как отдал исполнитель.
    """

    items: list[CrawlResultItem] = field(default_factory=list)
    next_cursor: str | None = None
    done: bool = True


@dataclass(frozen=True)
class CrawlSubmitResult:
    """Результат подачи набора работ (back-pressure, Phase 3 B2/C2).

    ``accepted=False`` — исполнитель отклонил подачу (напр. перегрузка/back-pressure):
    ядро обязано подождать ``retry_after_s`` (если задано) и повторить подачу. При отказе
    ``job_id`` — пустая строка; иначе — handle созданного (или ранее созданного по тому же
    ключу идемпотентности) задания.
    """

    job_id: str
    accepted: bool = True
    retry_after_s: float | None = None


@runtime_checkable
class CrawlExecutorPort(Protocol):
    """Исполнение краула (``split``): submit → status → collect (+ cancel).

    Контракт транспорт-агностичен: локальный планировщик и edge-планировщик DMZ
    взаимозаменяемы. ``submit`` идемпотентен по ``idempotency_key`` — **по набору работ**,
    не по сеансу (повторная подача того же набора не создаёт дублей; изменившийся набор —
    новое задание). ``submit`` возвращает :class:`CrawlSubmitResult` (в т.ч. ``accepted`` —
    сигнал back-pressure). ``collect`` отдаёт **сырые байты** (I19).
    """

    version: str

    def submit(
        self,
        items: list[CrawlWorkItem],
        *,
        session_id: str,
        run_id: str,
        profile: str,
        idempotency_key: str,
    ) -> CrawlSubmitResult:
        """Подаёт набор работ; возвращает :class:`CrawlSubmitResult` (handle + back-pressure)."""
        ...

    def status(self, handle: str) -> CrawlProgress:
        """Текущий прогресс задания ``handle``."""
        ...

    def cancel(self, handle: str) -> CrawlProgress:
        """Запрашивает отмену задания ``handle``; возвращает его прогресс."""
        ...

    def collect(
        self, handle: str, *, cursor: str | None = None, limit: int | None = None
    ) -> CrawlBatch:
        """Забирает страницу результатов задания ``handle`` (``cursor``/``limit``)."""
        ...
