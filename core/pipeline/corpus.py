"""Сборка корпуса — Ш5 (02 §B.1): retention → extract → dedup → chunk → index_manifest.

Фаза Ш5 превращает внешний контент (краул доменных профилей / SERP) в проиндексированные
продукт-партиции. Путь контента фиксирован и воспроизводим (I19):

```
url ── CrawlerPort.fetch ──▶ RawPage ── retention в corpus_raw/{product_id}/{hash}.bin
    ── ExtractorPort.extract ──▶ ExtractedDoc
    ── dedup(content_hash) ──▶ ChunkerPort.chunk(doc, document_id)
    ── content_hash(chunk.text) ──▶ CorpusChunk (build_id = index_build_id)
    ── (опц.) EmbedderPort.embed + IndexBuilderPort.build/commit ──▶ IndexManifestEntry
```

**I19.** Любой внешний контент становится evidence только после
``corpus_raw → extract → chunk → content_hash → index``. Фаза обеспечивает это структурно:
retention пишется **до** экстракции; каждый индексируемый чанк обязан нести непустой
``content_hash`` (иначе отклоняется и фиксируется в ``errors``); ``build_id`` чанка и
``index_manifest`` привязывают контент к иммутабельной сборке.

**I17 (продукт-скоупед).** Дедуп ведётся **в пределах продукта**, а не глобально: идентичная
перепечатка внутри одного продукта индексируется один раз (``повтор перепечаток не
индексируется``, §B.1), но тот же текст, отданный разными продуктами, сохраняется в обеих
продукт-партициях — иначе ретривал продукта B не увидел бы собственный документ. Один
``IndexManifestEntry`` — на продукт-партицию.

**Sitemap (D3).** Per-site ``DomainProfile.sitemap`` разворачивается через ``PollPort``
(``kind="sitemap"``; local in-process ‖ split через шов ``crawl/poll``) ровно на один уровень:
``<loc>`` становятся **дополнительными depth-0 seeds** того же seed-root, что и seed
оператора (``sitemapindex`` разворачивается один уровень; глубже — отказ). Сам sitemap — не
страница: не ретейнится/не ингестится (I19). ``<loc>`` проходят ``validate_url`` (SSRF) и
далее тот же блэклист/``max_documents``/``max_depth``, что и прочие seeds.

**Иммутабельность.** ``index_build_id`` — **контент-адресуемый** отпечаток батча сборки:
``short_hash`` от (``_BUILD_ID_SCHEMA`` + пакет/params + ``snapshot_date`` + ``chunker_version``
+ ``embed_model_id`` + ``extractor_version`` + ``lexical_engine_version`` + ``vector_store`` +
``content_token``). ``content_token`` — детерминированный (порядко-независимый) отпечаток
содержимого **всех** продукт-партиций (sorted ``(chunk_id, content_hash)``). Поэтому разные
корпуса не могут получить один id (same-id/different-content коллизия), а те же входы и то же
содержимое — один. Любое изменение входов/контента даёт новую сборку; старая не мутируется
(§B.1/§13, K2). ``retrieval_profile``/``retrieval_cutoff_K``/``retrieval_params_json`` **не**
входят в id: ретривал-конфиг не меняет чанкинг/векторы.

**Два прохода (один вызов, без ре-краула).** Pass A (ingest) по каждому продукту выполняет
краул → retention (I19) → экстракцию → дедуп(``content_hash``) → чанкинг и копит
``_ProductIngest`` в памяти; ``corpus_docs`` персистятся сразу (retention-провенанс не зависит
от сборки), а ``build_id`` чанков пока ``None``. После Pass A финализируется ``index_build_id``
(по всем чанкам + декларированным входам). Pass B (index) проставляет ``build_id``, строит
``IndexManifestEntry`` и вызывает ``_embed_and_build``; при успехе персистятся ``corpus_chunks``
+ ``index_manifest`` («сначала диск, потом БД»). Никакого round/parent-чейнинга.

**Durable-краул (D3/L1a).** При наличии ``store`` и ``run_id`` фаза ведёт **сеанс краула**
(``crawl_sessions`` + ``crawl_session_items``; ядро-владелец сеанса, §4.4): per-URL элемент
``CrawlItemStatus`` + провенанс попытки. Сеанс открывается через
:mod:`core.pipeline.crawl_session` (get-or-create + recovery-on-open), идемпотентно на прогон
(``session_id = f"crawl:{run_id}"``). URL с уже сохранёнными байтами **не фетчится повторно** —
``RawPage`` реконструируется из retained ``corpus_raw`` (I19: raw — источник) и ре-извлекается
детерминированно. ``store is None`` или ``run_id is None`` → поведение без durable-краула
(счётчики ``crawl_*`` — нули, строк сеанса нет). Все записи сеанса изолированы (сбой →
``errors``, фаза остаётся ``ok``).

**Порядок «сначала диск, потом БД».** Физическая продукт-партиция авторитетна. Пока
``index_builder.build`` не завершился успешно, фаза **не** пишет ``corpus_chunks``/
``index_manifest`` в БД: иначе реестр заявлял бы сборку, которой нет (или не совпадает с
иммутабельной партицией на диске). Если ``build`` отказал (в т.ч. ``ImmutableBuildError``
при попытке изменить уже зафиксированную сборку, K2/§13), ``corpus_docs`` всё равно
персистятся (retention), но ``corpus_chunks``/``index_manifest`` остаются неизменными, а
отказ попадает в ``errors`` с указанием, что изменение корпуса требует нового
``run_id``/раунда. Это зеркалит порядок в ``serp_loop`` (build → ``save_index_manifest``).

**Внедрение зависимостей.** Адаптеры (``core/adapters/*``) здесь не импортируются: фаза
работает только через порты (A1). ``embedder``/``index_builder`` опциональны — если они не
переданы, фаза строит корпус и ``index_manifest``, а эмбеддинг/физическую сборку индекса
подключает W8 (composition root). При их наличии фаза вызывает ``embed``/``build``/``commit``
best-effort (сбой изолируется).

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import datetime as _dt
import json
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.config.crawl_sources import CrawlSourcesConfig
from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain import (
    CorpusChunk,
    CorpusDoc,
    CrawlItemStatus,
    CrawlRunConfig,
    CrawlSessionItem,
    CrawlStrategy,
    DocumentLevel,
    FailureKind,
    IndexManifestEntry,
)
from core.pipeline.crawl_frontier import Frontier, resolve_crawl_limits
from core.pipeline.crawl_session import (
    default_session_id,
    finalize_session,
    open_session,
    prior_items,
)
from core.pipeline.crawl_sources import (
    CrawlTarget,
    crawl_sources_digest,
    resolve_crawl_target,
)
from core.pipeline.fetch_plan import _host_matches, select_domain_profile
from core.pipeline.runner import PhaseResult
from core.ports.chunker import ChunkerPort
from core.ports.crawl_executor import (
    ACTIVE_JOB_STATUSES,
    CrawlExecutorPort,
    CrawlResultItem,
    CrawlWorkItem,
)
from core.ports.crawler import CrawlerPort
from core.ports.embedder import EmbedderPort
from core.ports.extractor import ExtractorPort
from core.ports.index_builder import IndexBuilderPort, IndexMeta
from core.ports.poller import PollPort
from core.ports.types import Chunk as PortChunk
from core.ports.types import (
    ExtractedDoc,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    RawPage,
)
from core.util.document_formats import document_content_type, document_suffix
from core.util.hashing import content_hash, short_hash
from core.util.html_links import extract_links
from core.util.rate_limit import RateLimiter
from core.util.security import validate_url

__all__ = ["ExtractionPool", "IndexBuild", "build_corpus"]

#: Разделитель полей material-строки ``index_build_id`` (unit separator — однозначная склейка).
_FIELD_SEP = "\x1f"

#: Схема формулы ``index_build_id`` (P2): входит в material, поэтому будущая ревизия формулы
#: всегда даёт новые id (старые сборки не переиспользуются).
_BUILD_ID_SCHEMA = "corpus-build-2"

#: Phase 3 B2: максимум попыток подачи работ исполнителю при back-pressure
#: (``accepted=False``). После исчерпания — изолированный ``FAILED`` (``source_unavailable``),
#: а не тихий провал всего прогона.
_SUBMIT_MAX_ATTEMPTS = 5

#: Phase 4: ``error_kind`` терминального ``FAILED`` при исчерпании ``--max-attempts``.
#: Свободная строка-данные (``CrawlSessionItem.error_kind`` — не доменный enum): отличает
#: «попытки кончились» от сетевого/браузерного сбоя.
_MAX_ATTEMPTS_ERROR_KIND = "attempts_exhausted"

#: Phase 5.2: семантика источника-каталога (свободная строка-данные, не доменный enum).
#: URL с таким ``target.kind`` — не контент-страница, а лента/канал: он **поллится** через
#: ``Ports.pollers[kind]``, а полученные ссылки-записи добываются обычным путём. Один уровень
#: развёртки: записи повторно не расширяются.
_DIRECTORY_KINDS: frozenset[str] = frozenset({"feed", "channel"})

#: D3: ``kind`` опроса sitemap (``DomainProfile.sitemap``) — свободная строка-данные. Sitemap
#: не входит в ``_DIRECTORY_KINDS``: это per-site опция профиля домена, а не источник
#: ``crawl_sources.yaml``. Разворачивается через ``pollers["sitemap"]`` (local ‖ split).
_SITEMAP_KIND = "sitemap"

#: P3-A2: размер волны (раунда) executor-транспорта — число узлов фронтьера на один
#: ``_submit_collect``. **Не** ``CrawlLimits.max_workers``: ``max_workers`` — ось локального
#: пула и остаётся локальной (у одной оси не может быть двух смыслов). 64 ≪ дефолтного
#: ``queue_limit`` edge, поэтому превысить back-pressure может только размер одной волны.
_EXECUTOR_WAVE_ITEMS = 64

#: P3-A2 (§1(iii)): предохранитель от неограниченного BFS по общему edge — максимум раундов
#: (волн) на один seed-root. ``max_depth`` не ограничивает **число** страниц, поэтому без
#: предохранителя неограниченный BFS был бы самонаведённым DoS. При исчерпании — усечение
#: ``round_budget`` + явная запись в ``errors`` + сеанс ``PARTIAL`` (никогда не тихая
#: остановка). У локального пути аналога нет (остаточное расхождение R3).
_EXECUTOR_MAX_WAVES_PER_ROOT = 1000

#: P4: предохранитель от зависшего воркера экстракции — потолок ожидания ``future.result``
#: (НЕ SLA: заведомо щедрый дефолт; конфигурируется аддитивным kwarg ``extract_task_timeout_s``
#: зеркально ``executor_sleep``). По таймауту пул reap'ится (``terminate_workers``) и остаток
#: корня идёт inline — никогда не тихо.
_EXTRACT_TASK_TIMEOUT_S = 1800.0

#: P4: ограниченное число пересборок мёртвого пула экстракции (краш/OOM/таймаут). После
#: исчерпания — inline-fallback остатка корня с явной записью в ``errors`` (без авто-ретрая).
_EXTRACT_MAX_POOL_REBUILDS = 2

#: P4: сентинел «экстракция не была отложена» для :func:`build_corpus`-интернала
#: ``_commit_outcome`` (inline-путь вызывает ``_ingest_one``; пул передаёт готовый результат).
_INLINE = object()


@dataclass(frozen=True)
class _ExtractRequest:
    """P4: payload одного задания экстракции — **только примитивы** (picklable by construction).

    Байты страницы **не** передаются: retention (I19) уже положил их в ``raw_path``; воркер
    перечитывает ровно эти байты и реконструирует тот же ``RawPage`` перед ``extractor.extract``.
    ``route`` — ось извлечения, ``fetch_mode`` — ось получения (обе — ``StrEnum``-примитивы).
    """

    raw_path: str
    url: str
    fetched_at: str
    fetch_mode: FetchMode
    lang: str | None
    status_code: int | None
    content_type: str | None
    engine: str | None
    route: ExtractionRoute
    #: P5: уровень извлечения документных форматов (``crawl.document_level``). Едет в payload
    #: для самодостаточности задания и корректности под будущие режимы; по построению пул —
    #: per-seed-root (один уровень на пул), поэтому воркер кэширует конвертер по уровню.
    level: DocumentLevel = DocumentLevel.FULL


@dataclass(frozen=True)
class ExtractionPool:
    """P4: инъекция исполнителя экстракции из composition root (A1, ``pipeline ↛ adapters``).

    :param pool_factory: фабрика ``(int, DocumentLevel) → Executor`` (размер =
        ``limits.extract_workers``; уровень — per-seed-root, P5); ``local`` —
        ``ProcessPoolExecutor`` (forkserver, инициализатор строит экстрактор нужного уровня
        на воркер), ``split`` — ``ThreadPoolExecutor`` (``RemoteExtractor`` I/O-bound; ось
        ``document_level`` под ``split`` не действует — R-S).
    :param worker: рабочая функция ``_ExtractRequest → ExtractedDoc``. Для процессов —
        module-level (picklable by reference); для потоков допустим bound/closure.
    """

    pool_factory: Callable[..., Executor]
    worker: Callable[[_ExtractRequest], ExtractedDoc]


#: P4: глобальный экстрактор воркер-процесса. Устанавливается инициализатором пула
#: (composition root строит адаптер **внутри** процесса — объекты не пересекают границу).
_WORKER_EXTRACTOR: ExtractorPort | None = None
#: P5: уровень глобального экстрактора воркера (``None`` — «подходит для любого», split).
_WORKER_EXTRACTOR_LEVEL: DocumentLevel | None = None
#: P5: фабрика per-level экстрактора в воркере (защитный lazy-путь, если уровень задания
#: не совпал с инициализированным). Передаётся ссылкой из composition root.
_WORKER_EXTRACTOR_FACTORY: Callable[[DocumentLevel], ExtractorPort] | None = None


def set_worker_extractor(
    extractor: ExtractorPort | None,
    *,
    level: DocumentLevel | None = None,
    factory: Callable[[DocumentLevel], ExtractorPort] | None = None,
) -> None:
    """Устанавливает экстрактор воркер-процесса (зовётся ``initializer`` пула, P4/P5).

    ``level`` — уровень установленного экстрактора (``None`` — универсальный, напр. split);
    ``factory`` — фабрика per-level, если задание принесло иной уровень (защитный путь).
    """
    global _WORKER_EXTRACTOR, _WORKER_EXTRACTOR_LEVEL, _WORKER_EXTRACTOR_FACTORY
    _WORKER_EXTRACTOR = extractor
    _WORKER_EXTRACTOR_LEVEL = level
    _WORKER_EXTRACTOR_FACTORY = factory


def _worker_extractor_for(level: DocumentLevel) -> ExtractorPort | None:
    """Экстрактор воркера для ``level`` (кэш размера 1; per-root пул — P5).

    Если инициализированный экстрактор подходит (уровень совпал или универсален) — он и
    используется. Иначе (защитный путь) строится фабрикой и кэшируется. Кэш по построению
    размера 1 не является рабочим (пул per seed-root).
    """
    if _WORKER_EXTRACTOR is not None and (
        _WORKER_EXTRACTOR_LEVEL is None or _WORKER_EXTRACTOR_LEVEL == level
    ):
        return _WORKER_EXTRACTOR
    if _WORKER_EXTRACTOR_FACTORY is not None:
        set_worker_extractor(
            _WORKER_EXTRACTOR_FACTORY(level), level=level, factory=_WORKER_EXTRACTOR_FACTORY
        )
        return _WORKER_EXTRACTOR
    return _WORKER_EXTRACTOR


def _run_extract(extractor: ExtractorPort, request: _ExtractRequest) -> ExtractedDoc:
    """Перечитывает retained-байты (I19) и вызывает ``extractor.extract`` на том же ``RawPage``.

    Общий шаг модульного воркер-процесса (:func:`_extract_worker`) и потокового воркера
    (``split``): источник байтов — retained-файл, а не буфер родителя (I19).
    """
    content = Path(request.raw_path).read_bytes()
    page = RawPage(
        url=request.url,
        content=content,
        fetched_at=request.fetched_at,
        fetch_mode=request.fetch_mode,
        lang=request.lang,
        status_code=request.status_code,
        content_type=request.content_type,
        engine=request.engine,
    )
    return extractor.extract(page, request.route)


def _extract_worker(request: _ExtractRequest) -> ExtractedDoc:
    """Module-level воркер экстракции для процессов (picklable by reference, P4/§7).

    Использует экстрактор, построенный инициализатором в этом процессе; ``store``/``session``
    воркеру недоступны by construction (payload — только примитивы).
    """
    if _WORKER_EXTRACTOR is None:  # pragma: no cover — защита от неверной сборки
        return ExtractedDoc(
            text="",
            content_hash="",
            extractor_version="",
            error="extraction worker без экстрактора (initializer не отработал)",
        )
    extractor = _worker_extractor_for(request.level)
    if extractor is None:  # pragma: no cover — защита от неверной сборки
        return ExtractedDoc(
            text="",
            content_hash="",
            extractor_version="",
            error="extraction worker без экстрактора для уровня",
        )
    return _run_extract(extractor, request)


@dataclass
class IndexBuild:
    """Результат сборки корпуса (Ш5): сборка + продукт-партиции + retention-документы.

    :param index_build_id: детерминированный идентификатор иммутабельной сборки (§B.1/§13).
    :param entries: по одной ``IndexManifestEntry`` на продукт-партицию (I17).
    :param docs: уникальные ``CorpusDoc`` (retention + provenance экстракции, §3.2).
    :param chunk_count: суммарное число проиндексированных чанков.
    :param crawl_pending: L1a-наблюдаемость: элементы сеанса в очереди (``PENDING``/``RUNNING``).
    :param crawl_fetched: L1a: элементы ``FETCHED`` (сырые байты сохранены = «ready» для
        материализации).
    :param crawl_done: L1a: зеркало ``crawl_fetched`` (элемент ``FETCHED`` = готов к
        извлечению; отдельного ``done`` на уровне элемента нет — сеанс трекает краул, не
        экстракцию). Поле сохранено для обратной совместимости наблюдаемости.
    :param crawl_failed: L1a: терминально сбойные элементы (``FAILED``; ``CANCELLED`` —
        если у модели нет отдельного поля — складывается сюда только для счётчика).
    """

    index_build_id: str
    entries: list[IndexManifestEntry] = field(default_factory=list)
    docs: list[CorpusDoc] = field(default_factory=list)
    chunk_count: int = 0
    crawl_pending: int = 0
    crawl_fetched: int = 0
    crawl_done: int = 0
    crawl_failed: int = 0
    #: D1/G: корпус был **политико-усечён** (``max_documents``/``max_depth``) — краул не
    #: исчерпал домен. Машинно-читаемый флаг на результате фазы (log_entry); связанные
    #: записи — ``truncation``. Политико-усечённый корпус не «полон» (см. статус сеанса).
    truncated: bool = False
    #: D1/G: детализация усечения по ``(product, seed-root)``: эффективные лимиты, число
    #: добытых страниц, причина (``max_documents``/``max_depth``), зачёркнутые блэклистом URL.
    truncation: list[dict[str, Any]] = field(default_factory=list)
    #: P5: видимые пропуски документов уровнем ``off`` (решение **никогда** не тихо):
    #: ``{product_id, url, reason: url_suffix|content_type, level}``. Питает канал
    #: усечения/аномалии (``source_scope: exhaustive`` поверх «прореженного» продукта).
    document_skips: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class _ProductIngest:
    """Промежуточный результат Pass A (ingest) по одному продукту.

    Копит ``docs`` (retention + провенанс экстракции) и ``chunks`` **до** фиксации
    ``index_build_id``. ``build_id`` чанков остаётся ``None``; ``corpus_chunks``/
    ``index_manifest`` не пишутся до успешного физического ``build`` (Pass B).
    """

    product_id: str
    docs: list[CorpusDoc] = field(default_factory=list)
    chunks: list[CorpusChunk] = field(default_factory=list)


@dataclass
class _LocalFetch:
    """D4: дескриптор отложенного сетевого фетча локального пути.

    Расщепляет ``_process_local`` на три фазы: **pre-fetch** (главный поток — resume,
    browser-guard, решение о пейсинге/ёмкости), **fetch** (пул — только
    ``limiter.wait`` + ``active.fetch``) и **post-fetch** (главный поток — redirect-scope
    и retention). Дескриптор несёт всё нужное пост-фазе и не содержит мутаций.

    ``resume``/``browser_blocked`` вычисляются пре-фазой (чисто), а коммитятся
    (соответственно ``_resume_page`` или ``FAILED``-``_mark``) пост-фазой в FIFO-порядке
    отправки (детерминизм победителя дедупа и ``errors``).

    ``poolable`` — идёт ли живой фетч в пул: ``True`` только для не-браузерного живого
    фетча (resume/browser-guard/``FetchMode.BROWSER`` — всегда inline на главном потоке).
    """

    pid: str
    wu: str
    depth: int | None
    fetch_plan: FetchPlan
    active: Any
    engine_value: str | None
    prior_item: CrawlSessionItem | None
    resume: tuple[bytes, Path] | None
    browser_blocked: bool
    poolable: bool
    host: str
    scope_host: str | None
    enforce_redirect_scope: bool


@dataclass
class _WaveEntry:
    """D4: элемент волны дренажа frontier.

    Порядок элементов — порядок отправки (FIFO). ``kind``:
    ``"blacklisted"`` | ``"attempts"`` (мутации коммитятся пост-фазой) | ``"fetch"``
    (несёт ``desc`` и, если фетч реально нужен, ``future`` пула).
    """

    pid: str
    wu: str
    depth: int | None
    kind: str
    desc: _LocalFetch | None = None
    future: Any = None
    prior_item: CrawlSessionItem | None = None


@dataclass
class _PendingExtract:
    """P4: отложенная экстракция (главный поток, FIFO-очередь доводки).

    Несёт всё, что нужно финишу: реконструированный ``RawPage``, ``raw_path`` (перечитывается
    воркером, I19), ``fetch_plan`` (ось ``extract``), флаг свежести (для ``note_fetched`` и
    учёта ёмкости с in-flight) и ``future`` пула. Мутаций состояния сеанса/корпуса здесь нет.
    """

    pid: str
    url: str
    depth: int | None
    page: RawPage
    raw_path: Path
    fetch_plan: FetchPlan
    freshly_fetched: bool
    future: Any


# --------------------------------------------------------------------------- #
# Детерминированные выводы (версии/идентификаторы)
# --------------------------------------------------------------------------- #


def _as_text(value: Any) -> Any:
    """Приводит enum-значение (``StrEnum``) к строке ``.value``; прочее — как есть."""
    return getattr(value, "value", value)


def _looks_like_html(page: RawPage) -> bool:
    """Похожа ли страница на HTML (для извлечения ссылок при ``max_depth > 0``, D1).

    Если ``content_type`` известен — по нему (``html``/``xml``); иначе — эвристика по телу
    (маркер ``<html``/``<!doctype`` или наличие ``<a `` в начале). Не-HTML (PDF/бинарники)
    не парсится на ссылки.
    """
    content_type = (page.content_type or "").lower()
    if content_type:
        return "html" in content_type or "xml" in content_type
    head = page.content.lstrip()[:256].lower()
    return head.startswith(b"<html") or head.startswith(b"<!doctype") or b"<a " in head


def _now_iso() -> str:
    """Текущее время в ISO-8601 (UTC) — для провенанса элементов сеанса (L1a)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _work_set_idempotency_key(work: Sequence[CrawlWorkItem]) -> str:
    """Ключ идемпотентности по **набору работ** попытки (Phase 3 B2, детерминирован).

    Ключ выводится из содержимого ``work`` (``product_id``/``url`` + применённые оси), а
    не из ``session_id``: resume пропускает уже добытые URL, поэтому набор работ
    сокращается → ключ меняется → edge создаёт **новое** задание для оставшихся/сбойных
    URL (иначе он вернул бы старое терминальное задание и молча потерял новые работы).
    Повторная подача идентичного набора даёт тот же ключ (идемпотентно). Идемпотентность —
    **по набору работ**, не по сеансу.

    Канонизация: ``sorted`` по ``(product_id, url)`` (уникальны в наборе) →
    ``json.dumps(sort_keys=True, ensure_ascii=False)`` → :func:`short_hash`.
    """
    rows = sorted(
        (
            (
                w.product_id,
                w.url,
                w.engine,
                _as_text(w.fetch),
                _as_text(w.strategy),
                w.wait_for,
            )
            for w in work
        ),
        key=lambda row: (row[0], row[1]),
    )
    material = json.dumps(rows, sort_keys=True, ensure_ascii=False)
    return short_hash(material, 24)


def _resolve_package_id(plan: Any, package_id: str | None) -> str:
    """Идентификатор пакета корпуса для ``index_build_id``/``index_manifest``.

    Приоритет: явный ``package_id`` → ``plan.package`` → ``plan.case_id`` →
    ``plan.cohort.case_id`` → ``plan.run_id`` (forward-compat: ``RunPlan`` сегодня несёт
    ``run_id``/``cohort``, список пакетов — нет).
    """
    if package_id:
        return str(package_id)
    for attr in ("package", "case_id"):
        value = getattr(plan, attr, None)
        if value:
            return str(value)
    cohort = getattr(plan, "cohort", None)
    case_id = getattr(cohort, "case_id", None)
    if case_id:
        return str(case_id)
    return str(getattr(plan, "run_id", "") or "")


def _resolve_snapshot_date(plan: Any, snapshot_date: Any) -> str | None:
    """``snapshot_date`` для сборки: явный → ``plan.snapshot_date`` → ``cohort.frozen_at``."""
    if snapshot_date is not None:
        return str(snapshot_date)
    value = getattr(plan, "snapshot_date", None)
    if value is not None:
        return str(value)
    cohort = getattr(plan, "cohort", None)
    frozen = getattr(cohort, "frozen_at", None)
    return str(frozen) if frozen is not None else None


def _params_token(plan: Any) -> str:
    """Детерминированный отпечаток выбранного каталога (``param_id`` через запятую)."""
    rows = getattr(plan, "params", None) or []
    return ",".join(sorted(str(getattr(row, "param_id", row)) for row in rows))


def _derive_index_build_id(
    *,
    package_id: str,
    params_token: str,
    snapshot_date: str | None,
    chunker_version: str,
    embed_model_id: str,
    extractor_version: str,
    lexical_engine_version: str,
    vector_store: str,
    content_token: str,
) -> str:
    """Детерминированный контент-адресуемый ``index_build_id`` (без random/UUID, §B.1/§13).

    Точные входы (склеены ``\\x1f``, порядок фиксирован):

    0. ``_BUILD_ID_SCHEMA`` — версия формулы (P2);
    1. ``package_id`` — пакет корпуса (производный от ``plan`` либо явный);
    2. ``params_token`` — отпечаток выбранного каталога (``param_id``);
    3. ``snapshot_date`` (пустая строка, если нет);
    4. ``chunker_version``;
    5. ``embed_model_id`` (пусто, если эмбеддер не подключён — W8);
    6. ``extractor_version`` — композитная версия экстрактора (см. :func:`_extractor_token`);
    7. ``lexical_engine_version``;
    8. ``vector_store``;
    9. ``content_token`` — отпечаток содержимого батча (см. :func:`_content_token`).

    ``retrieval_*`` намеренно **не** входят: ретривал-конфиг не меняет чанкинг/векторы.
    """
    material = _FIELD_SEP.join(
        (
            _BUILD_ID_SCHEMA,
            package_id,
            params_token,
            snapshot_date or "",
            chunker_version,
            embed_model_id,
            extractor_version,
            lexical_engine_version,
            vector_store,
            content_token,
        )
    )
    return "ib_" + short_hash(material, 24)


def _product_content_token(chunks: Sequence[CorpusChunk]) -> str:
    """Отпечаток содержимого продукт-партиции: sorted ``(chunk_id, content_hash)``.

    Порядко-независим (сортировка пар) и чувствителен к любому изменению контента. Пары
    склеиваются ``_FIELD_SEP`` (``chunk_id`` не содержит разделителя) — однозначно.
    """
    material = _FIELD_SEP.join(
        f"{chunk_id}{_FIELD_SEP}{chunk_hash}"
        for chunk_id, chunk_hash in sorted(
            (c.chunk_id, c.content_hash or "") for c in chunks
        )
    )
    return short_hash(material, 24)


def _content_token(per_product: Mapping[str, Sequence[CorpusChunk]]) -> str:
    """Батч-отпечаток: product-delimited per-product токены (sorted ``product_id``).

    Один ``index_build_id`` описывает **весь** батч продуктов, поэтому отпечаток склеивает
    отпечатки продукт-партиций в детерминированном порядке продуктов. Пустой батч даёт
    детерминированный отпечаток пустой строки.
    """
    parts = [
        f"{product_id}{_FIELD_SEP}{_product_content_token(per_product[product_id])}"
        for product_id in sorted(per_product)
    ]
    return short_hash(_FIELD_SEP.join(parts), 24)


def _extractor_token(per_product: Mapping[str, Sequence[CorpusDoc]]) -> str:
    """Композитная ``extractor_version`` из фактически извлечённых документов батча.

    Собираются непустые ``CorpusDoc.extractor_version`` по всем продуктам, дедуплицируются и
    сортируются (порядко-независимо) и склеиваются через запятую. При единогласии версий даёт
    одну строку; при расхождении (например, разные extract-маршруты) — детерминированный
    композит. Документы без версии игнорируются; если версий нет вовсе — пустая строка.
    """
    versions = sorted(
        {
            doc.extractor_version
            for docs in per_product.values()
            for doc in docs
            if doc.extractor_version
        }
    )
    return ",".join(versions)


def _chunk_id(product_id: str, doc_hash: str, chunk_no: int) -> str:
    """Детерминированный глобально-уникальный ``chunk_id``.

    ``document_id = f"{product_id}:{doc_hash}"`` уже уникален, поэтому
    ``f"{product_id}:{doc_hash}:{chunk_no}"`` уникален глобально и стабилен. Идентификатор
    строится ядром (а не берётся у чанкера), чтобы гарантировать уникальность независимо
    от адаптера и воспроизводимость ``source_id``-резолва.
    """
    return f"{product_id}:{doc_hash}:{chunk_no}"


# --------------------------------------------------------------------------- #
# Персистенция (изолированная: сбой store не роняет фазу)
# --------------------------------------------------------------------------- #


def _persist_docs(
    store: Any,
    docs: list[CorpusDoc],
    errors: list[str],
) -> None:
    """Пишет ``corpus_docs`` (retention + provenance экстракции, §3.2).

    Вызывается **всегда** — даже если физическая сборка индекса отказала: сырые байты и
    провенанс экстракции не зависят от иммутабельности индексной сборки (I19). Каждая
    запись обёрнута в try/except: сбой персистенции фиксируется в ``errors`` и не
    прерывает обработку остальных продуктов (контракт изоляции фаз, ``runner.py``).
    """
    if store is None:
        return
    for doc in docs:
        try:
            store.upsert_corpus_doc(doc)
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(f"upsert_corpus_doc({doc.doc_id}): {type(exc).__name__}: {exc}")


def _persist_chunks_and_manifest(
    store: Any,
    chunks: list[CorpusChunk],
    entry: IndexManifestEntry | None,
    errors: list[str],
) -> None:
    """Пишет ``corpus_chunks`` → ``index_manifest`` (порядок: FK ``doc_id``).

    Вызывается **только** когда физическая продукт-партиция реально зафиксирована на диске
    («disk first, DB second»): реестр не должен заявлять сборку, которой нет (K2/§13).
    Каждая запись обёрнута в try/except: сбой фиксируется в ``errors`` и не прерывает
    обработку остальных продуктов (контракт изоляции фаз, ``runner.py``).
    """
    if store is None:
        return
    if chunks:
        try:
            store.add_chunks_batch(chunks)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"add_chunks_batch: {type(exc).__name__}: {exc}")
    if entry is not None:
        try:
            store.save_index_manifest(entry)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"save_index_manifest({entry.product_id}): {type(exc).__name__}: {exc}")


def _embed_and_build(
    embedder: EmbedderPort,
    index_builder: IndexBuilderPort,
    product_id: str,
    chunks: list[CorpusChunk],
    *,
    package_id: str,
    chunker_version: str,
    embed_model_id: str,
    snapshot_date: str | None,
    index_build_id: str,
    errors: list[str],
) -> bool:
    """Best-effort эмбеддинг и физическая сборка индекса продукт-партиции (K3, §B.1).

    ``pipeline`` не работает с файлами индекса напрямую: за ``.npy``-шарды/tantivy отвечает
    ``IndexBuilderPort``. Сбой изолируется в ``errors``.

    :returns: ``True`` — если ``index_builder.build`` вернулся без исключения (партиция
        физически зафиксирована); ``False`` — при **любом** исключении (в т.ч.
        ``ImmutableBuildError`` при попытке изменить уже зафиксированную сборку, K2/§13).
        Вызывающий использует результат как гейт для записи ``corpus_chunks``/
        ``index_manifest`` («disk first, DB second»).
    """
    try:
        port_chunks = [
            PortChunk(
                chunk_id=c.chunk_id,
                chunk_no=c.chunk_no or 0,
                text=c.text,
                heading_path=c.heading_path or "",
                context_prefix=c.context_prefix or "",
                content_hash=c.content_hash or "",
            )
            for c in chunks
        ]
        vectors = embedder.embed([c.text for c in chunks])
        meta = IndexMeta(
            package=package_id,
            chunker_version=chunker_version,
            embed_model_id=embed_model_id,
            snapshot_date=snapshot_date or "",
            product_id=product_id,
            extra={"index_build_id": index_build_id},
        )
        index_builder.build(port_chunks, vectors, meta)
        return True
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        errors.append(f"{product_id}: index build: {type(exc).__name__}: {exc}")
        return False


# --------------------------------------------------------------------------- #
# Durable-краул (L1a): сеанс краула — аудит/очередь/resume per-URL
# --------------------------------------------------------------------------- #


def _resume_source(
    item: CrawlSessionItem | None,
) -> tuple[bytes, Path] | None:
    """Возвращает retained-байты для resume, если элемент сеанса позволяет НЕ фетчить (D3).

    Пригодны элементы ``FETCHED`` (сырые байты уже сохранены) и ``FAILED`` с сохранённым
    ``raw_path`` (re-extract из тех же байтов без сети). ``PENDING``/``RUNNING``/отсутствие
    записи, отсутствие ``raw_path`` или файла на диске → ``None`` (обычный путь фетча).

    **Верификация (D3).** Если в записи задан ``content_hash``, а хеш прочитанных байтов
    не совпадает, файл считается непригодным (повреждён/подменён) → ``None``: резюм не
    должен молча давать другой ``index_build_id`` и самопротиворечивую аудит-строку.
    I19: retained raw — источник; экстракция детерминирована.
    """
    if item is None:
        return None
    if item.status not in (CrawlItemStatus.FETCHED, CrawlItemStatus.FAILED):
        return None
    if not item.raw_path:
        return None
    path = Path(item.raw_path)
    if not path.is_file():
        return None
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if item.content_hash and content_hash(data) != item.content_hash:
        return None
    return data, path


def _resume_page(
    item: CrawlSessionItem, url: str, content: bytes, path: Path, fetch_plan: FetchPlan
) -> RawPage:
    """Реконструирует ``RawPage`` из retained-байтов и элемента сеанса (без сети, D3).

    ``fetched_at`` — из ``last_attempt`` записи, иначе mtime файла (UTC). ``fetch_mode`` —
    из записи (если распознаётся, иначе ``fetch_plan.fetch``). ``content_type``/``lang``/
    ``status_code`` — из провенанса записи (``lang`` персистится, иначе свежий и
    возобновлённый прогоны расходились бы по ``corpus_docs.lang``/``corpus_chunks.lang``).
    """
    fetched_at = item.last_attempt
    if not fetched_at:
        try:
            fetched_at = _dt.datetime.fromtimestamp(
                path.stat().st_mtime, tz=_dt.timezone.utc
            ).isoformat()
        except OSError:
            fetched_at = ""
    fetch_mode = fetch_plan.fetch
    if item.fetch_mode:
        try:
            fetch_mode = FetchMode(item.fetch_mode)
        except ValueError:
            fetch_mode = fetch_plan.fetch
    return RawPage(
        url=url,
        content=content,
        fetched_at=fetched_at or "",
        fetch_mode=fetch_mode,
        lang=item.lang,
        status_code=item.http_status,
        content_type=item.content_type,
    )


# --------------------------------------------------------------------------- #
# Основная фаза
# --------------------------------------------------------------------------- #


def build_corpus(
    plan: Any,
    crawler: CrawlerPort,
    extractor: ExtractorPort,
    chunker: ChunkerPort,
    store: Any = None,
    *,
    urls: Mapping[str, list[str]] | None = None,
    snapshot_date: Any = None,
    chunker_version: str | None = None,
    raw_root: str | Path = "corpus_raw",
    embedder: EmbedderPort | None = None,
    index_builder: IndexBuilderPort | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
    explicit_fetch: Mapping[str, FetchMode] | None = None,
    crawl_defaults: CrawlRunConfig | None = None,
    rate_limiter: RateLimiter | None = None,
    package_id: str | None = None,
    run_id: str | None = None,
    crawl_sources: CrawlSourcesConfig | None = None,
    crawlers: Mapping[str, CrawlerPort] | None = None,
    pollers: Mapping[str, PollPort] | None = None,
    session_id: str | None = None,
    executor: CrawlExecutorPort | None = None,
    executor_poll_interval: float = 0.25,
    executor_sleep: Callable[[float], None] = time.sleep,
    executor_wave_items: int | None = None,
    pool_factory: Callable[[int], Executor] | None = None,
    extraction_pool: ExtractionPool | None = None,
    extractor_factory: Callable[[DocumentLevel], ExtractorPort] | None = None,
    extract_task_timeout_s: float | None = None,
    max_attempts: int | None = None,
    progress: Callable[[dict[str, int]], None] | None = None,
    crawl_app: CrawlConfig | None = None,
) -> PhaseResult:
    """Ш5: собирает корпус из ``plan × urls`` и возвращает ``PhaseResult``.

    :param plan: ``RunPlan`` (продукты, каталог). Список URL ``RunPlan`` не несёт — он
        передаётся явно через ``urls`` (тонкий вход, см. ниже).
    :param crawler: ``CrawlerPort`` — сырые байты страниц.
    :param extractor: ``ExtractorPort`` — нормализация в текст.
    :param chunker: ``ChunkerPort`` — нарезка с ``heading_path``/``context_prefix``.
    :param store: опциональный ``StorePort`` (``upsert_corpus_doc``/``add_chunks_batch``/
        ``save_index_manifest``).
    :param urls: отображение ``product_id → [url, ...]``. ``RunPlan`` не содержит URL, а
        паспортные ``doc_domains`` живут вне плана; поэтому источники передаются явно.
        Если ``urls`` не задан, фаза не собирает ничего и завершается ``ok`` (пустая сборка).
        Обрабатываются только URL продуктов плана (при пустом ``plan.products`` — ключи
        ``urls`` в детерминированном порядке).
    :param snapshot_date: окно снимка; ``None`` → ``plan.snapshot_date``/``cohort.frozen_at``.
    :param chunker_version: версия чанкера; ``None`` → ``chunker.version``.
    :param raw_root: корень retention (``corpus_raw/{product_id}/{raw_hash}.bin``); дефолт
        ``"corpus_raw"`` совпадает с :class:`~core.config.paths.PathsConfig`; composition
        root передаёт resolved ``paths.corpus_raw`` (``03`` §A.1).
    :param embedder: опциональный ``EmbedderPort`` (W8); при наличии — батч-эмбеддинг.
    :param index_builder: опциональный ``IndexBuilderPort`` (W8); при наличии — build/commit.
    :param domain_profiles: типизированные доменные профили (D.2) для резолва ``FetchPlan``,
        осей контроля краула (D1/D2) и per-site ``sitemap`` (D3).
    :param explicit_fetch: явные переопределения оси ``fetch`` по URL (``--render-modes``);
        побеждают профиль, но только по оси ``fetch`` (``extract``/``wait_for`` — из профиля).
    :param crawl_defaults: дефолты секции ``crawl`` run.yaml (D4): ось ``strategy`` и
        диапазон задержек ``rate_limit``. ``None`` → ``CrawlStrategy.AUTO`` без ограничения;
        профиль домена, задавший соответствующее поле, его переопределяет. Проводка
        используется краулером (P2/P3); здесь только резолвится в ``FetchPlan``.
    :param rate_limiter: владелец politeness-пейсинга (**единственный** — corpus). Перед
        каждым **живым** фетчем вызывается ``wait(host, fetch_plan.rate_limit)``;
        ``spec is None`` → без сна. ``None`` → строится :class:`RateLimiter` с реальным
        ``sleep``; тесты инжектят ``RateLimiter(sleep=lambda s: None, rng=random.Random(...))``.
        Путь ``resume`` (re-извлечение из retained raw) пейсингом не облагается: живого
        сетевого вызова нет.
    :param package_id: явный пакет корпуса; ``None`` → производный от ``plan``.
    :param run_id: идентификатор прогона для durable-краула (D3/L1a). При наличии ``store`` и
        ``run_id`` фаза ведёт **сеанс краула** (``crawl_sessions``/``crawl_session_items``,
        аудит/очередь/resume): URL с сохранёнными байтами НЕ фетчится повторно, а
        ре-извлекается из retained raw. ``None`` (или ``store is None``) → поведение без
        durable-краула (счётчики ``crawl_*`` — нули).
    :param crawl_sources: опциональный ``CrawlSourcesConfig`` (Lane A) — слои источников
        ``defaults``/``products``/``sites``; резолвится в ``FetchPlan``/``engine``/``kind``
        через :func:`core.pipeline.crawl_sources.resolve_crawl_target`. ``None`` → пустой
        конфиг (поведение как у одиночного профиля домена).
    :param crawlers: опциональная карта ``engine → CrawlerPort``: активный краулер выбирается
        по ``target.engine`` (fallback — ``crawler``). Позволяет per-engine адаптеры без
        изменения сигнатуры ``crawler``.
    :param pollers: Phase 5.2/D3 — карта ``kind → PollPort`` для источников-каталогов
        (``feed``/``channel``) и sitemap (``sitemap``): ``kind`` направления резолвится в
        pipeline, каталог/sitemap поллится, entries — ссылки; контент добывается обычным
        путём. ``None`` → источники-каталоги не разворачиваются (URL с директорийным ``kind``
        помечается ``FAILED`` ``source_unavailable``), а per-site ``sitemap`` фиксирует ошибку.
    :param session_id: явный идентификатор сеанса краула; ``None`` →
        ``f"crawl:{run_id}"`` (идемпотентно на прогон). Игнорируется без ``store``/``run_id``.
    :param executor: опциональный исполнитель краула (``CrawlExecutorPort``, Phase 3.3). При
        наличии краул исполняется **через него** (``split``/edge-планировщик): работы
        подаются batch-волнами, результаты забираются пагинацией, а retention/экстракция/
        дедуп/нарезка и сеансовое трекинг — общие с локальным путём (``_ingest_one``).
        ``crawler``/``crawlers`` при этом не используются. **P3-A2:** ``executor`` — лишь
        подключаемый **batch-транспорт** того же drain-алгоритма, что и локальный пул
        (``_drain_root``): BFS-раунды, ``Frontier``, блэклисты, ``max_depth``/``max_documents``
        и redirect-scope — на стороне ядра, одинаково для обоих профилей. ``None`` →
        локальный per-URL путь (пул/последовательный).
    :param executor_poll_interval: пауза между опросами прогресса задания исполнителя, с
        (только при ``executor``); уже терминальное задание не пейсится.
    :param executor_sleep: хук сна опроса (по умолчанию ``time.sleep``); тесты инжектят
        no-op, чтобы не платить реальное время.
    :param executor_wave_items: P3-A2 — размер волны executor-транспорта (число узлов
        фронтьера на один ``_submit_collect``); ``None`` → :data:`_EXECUTOR_WAVE_ITEMS`.
        Инжектируемый параметр для детерминированных тестов (зеркалит ``pool_factory``);
        **не** ось локального пула.
    :param pool_factory: D4 — фабрика ограниченного пула ``Callable[[int], Executor]`` для
        **сетевого fetch** локального пути; ``int`` — размер волны (``CrawlLimits.max_workers``
        корня). ``None`` → stdlib :class:`concurrent.futures.ThreadPoolExecutor`. Инъекция
        нужна только детерминированным тестам (зеркалит ``executor_sleep``). Пул создаётся
        лишь при ``max_workers > 1``: ``max_workers in (None, 1)`` — прежний однопоточный путь
        без пула. **Инъецируемые** ``sleep``/``rng`` лимитера (при заданном ``rate_limiter``)
        должны быть потокобезопасны — pacing зовётся из fetch-воркеров.
    :param extraction_pool: P4 — инъекция исполнителя **экстракции** (:class:`ExtractionPool`:
        фабрика пула + рабочий вызов). Отдельная ось ``CrawlLimits.extract_workers``
        (``app < run < site``): ``None``/``1`` ⇒ пул не создаётся и экстракция inline
        (поведение байт-в-байт). Пул — per seed-root, лениво, с ``shutdown``; окно ``W``
        (не более ``W`` экстракций «в полёте»), доводка строго FIFO. ``None`` при заданной
        оси ``>1`` — явная запись в ``errors`` + inline-fallback (не тихо). Воркер получает
        только :class:`_ExtractRequest` (примитивы + ``raw_path``); байты не пиклятся (I19).
    :param extractor_factory: P5 — фабрика per-level экстрактора ``Callable[[DocumentLevel],
        ExtractorPort]``, переданная ссылкой из composition root (слой-матрица: pipeline не
        импортирует адаптеры). Применяется к **inline**-пути (``extract_workers`` None/``1``):
        для уровня корня, отличного от ``full``, строится экстрактор этого уровня; ``full``
        переиспользует переданный ``extractor`` (дефолт — без лишней загрузки весов docling).
        ``None`` → уровень на inline-пути не действует (поведение как прежде; пул-путь всё
        равно получает уровень через ``extraction_pool.pool_factory``).
    :param extract_task_timeout_s: P4 — потолок ожидания ``future.result`` одного задания
        экстракции (guard от зависшего воркера, не SLA). ``None`` →
        :data:`_EXTRACT_TASK_TIMEOUT_S`. По таймауту пул reap'ится, in-flight → ``FAILED``,
        пул пересобирается (bounded) либо inline-fallback; авто-ретрая нет.
    :param max_attempts: Phase 4 — потолок попыток по ``(product_id, url)``. URL с уже
        сохранённым ``CrawlSessionItem.attempts >= max_attempts`` **не** фетчится и **не**
        подаётся исполнителю: он помечается терминальным ``FAILED`` с ``error_kind =
        "attempts_exhausted"`` и понятной ошибкой (не тихий дроп). ``None`` → без потолка.
    :param progress: Phase 4 — опциональный колбэк прогресса: вызывается со снимком
        ``crawl_session_counts`` (``{status: count}``) по ходу фазы (для ``--follow``).
        Ошибки колбэка изолируются и не роняют фазу. ``None`` → без прогресс-вывода.
    :param crawl_app: D1/D2 — application-слой осей контроля краула (``deploy.yaml:crawl``,
        :class:`~core.config.deploy.CrawlConfig`): низший приоритет цепочки
        ``app < run < site`` для ``max_workers``/``max_depth``/``max_documents``/
        ``exclude_paths``/``exclude_documents`` (:func:`core.pipeline.crawl_frontier.resolve_crawl_limits`).
        ``None`` → дефолт :class:`CrawlConfig` (``max_depth=0``, без лимитов/блэклистов —
        поведение неизменно).

    Возвращает ``PhaseResult`` со ``status='ok'`` (даже при изолированных ошибках),
    ``cells=[]`` (Ш5 не производит ячеек) и ``log_entries=[IndexBuild]`` (сборка + счётчики).

    :raises ValueError: если ``urls`` задан, но не является отображением.
    """
    if urls is not None and not isinstance(urls, Mapping):
        raise ValueError("urls обязан быть отображением product_id → список URL")

    url_map: dict[str, list[str]] = {}
    if urls:
        for pid, values in urls.items():
            url_map[str(pid)] = sorted({str(u) for u in (values or [])})

    declared = sorted({str(p) for p in (getattr(plan, "products", None) or [])})
    products = declared if declared else sorted(url_map)

    package = _resolve_package_id(plan, package_id)
    snapshot = _resolve_snapshot_date(plan, snapshot_date)
    resolved_chunker_version = (
        str(chunker_version)
        if chunker_version is not None
        else str(getattr(chunker, "version", "") or "")
    )
    embed_model_id = str(getattr(embedder, "model_id", "") or "")
    lexical_engine_version = str(getattr(index_builder, "lexical_engine_version", "") or "")
    vector_store = str(getattr(index_builder, "vector_store", "") or "")

    errors: list[str] = []
    duplicate_count = 0
    raw_root_path = Path(raw_root)
    #: P5: видимые пропуски документов уровнем ``off`` (никогда не тихо) — агрегат по всем
    #: корням/продуктам прогона; кладётся в ``IndexBuild.document_skips``.
    document_skips: list[dict[str, Any]] = []

    # D4: дефолты оси стратегии/задержек из run.yaml ``crawl`` (профиль домена их переопределяет).
    crawl_strategy_default = (
        crawl_defaults.strategy if crawl_defaults is not None else CrawlStrategy.AUTO
    )
    crawl_rate_default = crawl_defaults.rate_limit if crawl_defaults is not None else None

    # P2-3: единый владелец politeness-пейсинга — corpus (адаптерный ``rate_limiter`` — None).
    limiter = rate_limiter if rate_limiter is not None else RateLimiter()

    # P3-A2: размер волны executor-транспорта. Читаем модульную константу в момент вызова
    # (тесты могут её подменить) — но инъекция через параметр приоритетнее.
    wave_items = (
        executor_wave_items
        if executor_wave_items is not None
        else _EXECUTOR_WAVE_ITEMS
    )

    # P4: потолок ожидания задания экстракции (guard от зависшего воркера). Инъекция
    # приоритетнее модульной константы (тесты дают крошечный таймаут).
    _extract_timeout = (
        extract_task_timeout_s
        if extract_task_timeout_s is not None
        else _EXTRACT_TASK_TIMEOUT_S
    )

    # L1a: сеанс краула (durable намерение + агрегат прогресса, §4.4). Идемпотентен на
    # прогон: ``session_id`` по умолчанию ``f"crawl:{run_id}"``. Без ``store``/``run_id``
    # durable-краул выключен (строк сеанса нет, счётчики ``crawl_*`` — нули).
    session: str | None = None
    if store is not None and run_id is not None:
        session = session_id if session_id is not None else default_session_id(run_id)
        try:
            open_session(store, session_id=session, run_id=run_id)
        except Exception as exc:  # noqa: BLE001 — resume best-effort, не роняет фазу
            errors.append(f"open_crawl_session: {type(exc).__name__}: {exc}")
            session = None

    # L1a: априорные элементы сеанса (аудит/очередь/resume). Ключ — ``(product_id, url)``:
    # PK ``crawl_session_items`` = ``(session_id, product_id, url)``, поэтому один URL под
    # разными продуктами имеет независимые записи (I17/§4.4: партиция и её retained raw
    # принадлежат продукту).
    items: dict[tuple[str, str], CrawlSessionItem] = {}
    if session is not None:
        try:
            items = prior_items(store, session)
        except Exception as exc:  # noqa: BLE001 — resume best-effort, не роняет фазу
            errors.append(f"iter_crawl_session_items: {type(exc).__name__}: {exc}")
            items = {}

    # L1a: разрешённые цели краула (``engine``/``kind``) по ``(product_id, url)`` — провенанс
    # элемента сеанса.
    targets: dict[tuple[str, str], Any] = {}
    #: Phase 2.1/S1: фактически применённый движок (имя транспорта адаптера) по
    #: ``(product_id, url)``; ``None`` → откат на ``target.engine``. Отдельная карта, чтобы
    #: **не мутировать** замороженный ``CrawlTarget``.
    applied_engines: dict[tuple[str, str], str | None] = {}

    def _mark(pid: str, u: str, status: CrawlItemStatus, **fields: Any) -> None:
        """Помечает исход URL и (при активном сеансе) персистит элемент краула (L1a).

        Пишет **полную** строку ``crawl_session_items``: поля предыдущего элемента
        переносятся (carry-forward), переданные не-``None`` поля перекрывают. Счётчики
        сеанса поддерживаются стором транзакционно. Сбой изолируется в ``errors``; без
        сеанса durable-краул выключен — строка не пишется (наблюдаемость идёт из
        ``IndexBuild``-счётчиков, которые при отсутствии сеанса нулевые).

        Phase 2.1/S1: ``engine`` — **применённый** транспорт (``applied_engines``), а не
        запрошенный ``target.engine``; откат на ``target.engine`` при отсутствии записи.
        """
        if session is None:
            return
        key = (pid, u)
        base = items.get(key)
        target = targets.get(key)
        applied = applied_engines.get(key)
        engine_value = applied if applied is not None else (
            target.engine if target is not None else None
        )
        now = _now_iso()
        attempts = base.attempts if base is not None else 0
        if status in (CrawlItemStatus.FETCHED, CrawlItemStatus.FAILED) and (
            base is None or base.status is not status
        ):
            attempts += 1
        record: dict[str, Any] = {
            "session_id": session,
            "product_id": pid,
            "url": u,
            "status": status,
            "attempts": attempts,
            "depth": base.depth if base is not None else None,
            "engine": engine_value,
            "kind": target.kind if target is not None else None,
            "fetch_mode": base.fetch_mode if base is not None else None,
            "extract_route": base.extract_route if base is not None else None,
            "strategy": base.strategy if base is not None else None,
            "http_status": base.http_status if base is not None else None,
            "content_type": base.content_type if base is not None else None,
            "lang": base.lang if base is not None else None,
            "content_hash": base.content_hash if base is not None else None,
            "raw_path": base.raw_path if base is not None else None,
            "error_kind": base.error_kind if base is not None else None,
            "error": base.error if base is not None else None,
            "first_seen": (base.first_seen if base is not None and base.first_seen else now),
            "last_attempt": now,
        }
        for name, value in fields.items():
            if value is not None:
                record[name] = value
        try:
            item = CrawlSessionItem(**record)
            store.upsert_crawl_session_item(item)
            items[key] = item
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(
                f"{pid}/{u}: crawl_session_item {_as_text(status)}: "
                f"{type(exc).__name__}: {exc}"
            )

    def _retain(
        product_id: str,
        url: str,
        page: RawPage,
        fetch_plan: FetchPlan,
        *,
        depth: int | None = None,
    ) -> Path | None:
        """Retention сырых байтов (I19) + durable-элемент ``FETCHED``; ``None`` при сбое.

        Байты пишутся в ``raw_root/{product_id}/{raw_hash}.bin`` **до** экстракции; при
        удаче элемент сеанса фиксируется ``FETCHED`` (provenance фетча). Сбой сохранения =
        недоступность источника по этому URL: строка помечается ``FAILED``
        (``source_unavailable``), чтобы очередь/счётчики были правдивы; возвращается ``None``.
        Общий шаг локального и executor путей (без дублирования).
        """
        raw_hash = content_hash(page.content)
        try:
            raw_path = raw_root_path / product_id / f"{raw_hash}.bin"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_bytes(page.content)
        except Exception as exc:  # noqa: BLE001
            reason = f"{type(exc).__name__}: {exc}"
            errors.append(f"{product_id}/{url}: retention: {reason}")
            # L1a: сбой сохранения байтов = недоступность источника по этому URL;
            # помечаем FAILED, чтобы очередь/счётчики были правдивы (не ``pending``).
            _mark(
                product_id,
                url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                error=f"retention: {reason}",
                depth=depth,
            )
            return None
        # L1a: байты сохранены — durable-элемент сеанса (provenance фетча).
        _mark(
            product_id,
            url,
            CrawlItemStatus.FETCHED,
            content_hash=raw_hash,
            raw_path=str(raw_path),
            http_status=page.status_code,
            content_type=page.content_type,
            lang=page.lang,
            fetch_mode=_as_text(fetch_plan.fetch),
            extract_route=_as_text(fetch_plan.extract),
            strategy=_as_text(fetch_plan.strategy),
            depth=depth,
        )
        return raw_path

    def _ingest_one(
        product_id: str,
        url: str,
        page: RawPage,
        fetch_plan: FetchPlan,
        raw_path: Path,
        ingest: _ProductIngest,
        seen_docs: dict[str, str],
        seen_chunk_ids: set[str],
        extractor_obj: ExtractorPort,
    ) -> None:
        """Inline-экстракция → доводка одного URL (путь без пула, ``None``/``1``).

        Тело экстракции (``extractor_obj.extract``) вынесено в :func:`_finish_one`; здесь —
        только inline-вызов и перевод исключения в ту же ошибку ``parser_failed``, что и
        пул-путь (P4: единая семантика отказа). ``extractor_obj`` — экстрактор уровня
        корня (P5; ``full`` переиспользует переданный порт).
        """
        try:
            extracted = extractor_obj.extract(page, fetch_plan.extract)
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            _finish_one(
                product_id,
                url,
                page,
                fetch_plan,
                raw_path,
                None,
                ingest,
                seen_docs,
                seen_chunk_ids,
                extract_error=f"{type(exc).__name__}: {exc}",
            )
            return
        _finish_one(
            product_id,
            url,
            page,
            fetch_plan,
            raw_path,
            extracted,
            ingest,
            seen_docs,
            seen_chunk_ids,
        )

    def _finish_one(
        product_id: str,
        url: str,
        page: RawPage,
        fetch_plan: FetchPlan,
        raw_path: Path,
        extracted: Any,
        ingest: _ProductIngest,
        seen_docs: dict[str, str],
        seen_chunk_ids: set[str],
        *,
        extract_error: str | None = None,
    ) -> None:
        """Доводка одного URL после экстракции: дедуп → нарезка → ``_mark`` (главный поток).

        P4: граница расщепления проходит **механически** по ``extracted`` — результат
        экстракции приходит либо из inline-вызова, либо из ``future.result()`` пула. Сырые
        байты уже сохранены (``raw_path``); шаги изолируют ошибки в ``errors`` и фиксируют
        терминальный статус элемента сеанса через ``_mark`` (FIFO на главном потоке).
        """
        nonlocal duplicate_count
        # 3. Экстракция (грациозная деградация адаптера через .error).
        if extract_error is not None:
            errors.append(f"{product_id}/{url}: extract: {extract_error}")
            _mark(
                product_id,
                url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.PARSER_FAILED.value,
                error=extract_error,
                raw_path=str(raw_path),
            )
            return
        if extracted is None or getattr(extracted, "error", None):
            reason = getattr(extracted, "error", None) or "пустой ExtractedDoc"
            errors.append(f"{product_id}/{url}: extract: {reason}")
            _mark(
                product_id,
                url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.PARSER_FAILED.value,
                error=reason,
                raw_path=str(raw_path),
            )
            return

        doc_hash = str(getattr(extracted, "content_hash", "") or "")
        if not doc_hash:
            errors.append(f"I19: {product_id}/{url}: ExtractedDoc без content_hash — отклонён")
            _mark(
                product_id,
                url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.PARSER_FAILED.value,
                error="ExtractedDoc без content_hash",
                raw_path=str(raw_path),
            )
            return

        # 4. Дедуп по content_hash (в пределах продукта).
        if doc_hash in seen_docs:
            duplicate_count += 1
            # Байты сохранены и контент уже проиндексирован близнецом в этой партиции.
            _mark(product_id, url, CrawlItemStatus.FETCHED, raw_path=str(raw_path))
            return
        seen_docs[doc_hash] = url

        document_id = f"{product_id}:{doc_hash}"
        doc = CorpusDoc(
            doc_id=document_id,
            product_id=product_id,
            url=page.url,
            fetched_at=page.fetched_at,
            content_hash=doc_hash,
            raw_path=str(raw_path),
            extractor_version=getattr(extracted, "extractor_version", None),
            render_mode=_as_text(page.fetch_mode),
            lang=page.lang,
        )
        ingest.docs.append(doc)

        # 5. Чанкер.
        try:
            chunks = chunker.chunk(extracted, document_id)
        except Exception as exc:  # noqa: BLE001
            # Сбой нарезки — не отказ источника/парсера: raw сохранён (FETCHED),
            # повторный resume сможет ре-извлечь без ре-фетча.
            errors.append(f"{product_id}/{url}: chunk: {type(exc).__name__}: {exc}")
            return

        for chunk in sorted(chunks or [], key=lambda c: getattr(c, "chunk_no", 0)):
            provided_hash = str(getattr(chunk, "content_hash", "") or "").strip()
            text = getattr(chunk, "text", "") or ""
            # I19: у каждого индексируемого чанка обязан быть непустой hash.
            if not provided_hash:
                errors.append(f"I19: {document_id}: чанк без content_hash отклонён")
                continue
            if not text.strip():
                errors.append(f"I19: {document_id}: чанк с пустым текстом отклонён")
                continue

            chunk_no = int(getattr(chunk, "chunk_no", 0))
            cid = _chunk_id(product_id, doc_hash, chunk_no)
            if cid in seen_chunk_ids:
                errors.append(f"{document_id}: дублирующийся chunk_id {cid!r} отклонён")
                continue
            seen_chunk_ids.add(cid)

            ingest.chunks.append(
                CorpusChunk(
                    chunk_id=cid,
                    doc_id=document_id,
                    product_id=product_id,
                    url=page.url,
                    heading_path=getattr(chunk, "heading_path", "") or "",
                    lang=page.lang,
                    chunk_no=chunk_no,
                    text=text,
                    context_prefix=getattr(chunk, "context_prefix", "") or "",
                    # Авторитетный хеш — от чистого текста (I19/§10).
                    content_hash=content_hash(text),
                    # build_id проставляется в Pass B после фиксации id сборки.
                    build_id=None,
                    embed_model_id=embed_model_id or None,
                    embedded_at=None,
                )
            )

        # L1a: документ успешно добыт и нарезан → элемент сеанса терминален ``FETCHED``
        # (сеанс трекает краул: сырые байты сохранены = «ready» для материализации).
        _mark(product_id, url, CrawlItemStatus.FETCHED, raw_path=str(raw_path))

    def _submit_collect(work: list[CrawlWorkItem]) -> dict[tuple[str, str], CrawlResultItem]:
        """Phase 3.3/5.3: подача набора работ исполнителю → ожидание → сбор результатов.

        Единая машинерия одной «волны» (submit с back-pressure → опрос статуса → сбор
        пагинацией); ошибки шва изолируются в ``errors``. Пустой набор → ``{}`` без обращения
        к исполнителю. ``idempotency_key`` — по набору работ **этой** волны (разные волны ⇒
        разные ключи ⇒ разные задания), ``session_id`` стабилен на прогон.

        **P3-A2 (§3): бисекция back-pressure.** Если после :data:`_SUBMIT_MAX_ATTEMPTS`
        попыток волна всё ещё отвергнута (``accepted=false``), она делится **пополам** и
        каждая половина обрабатывается как своя волна (свой ``idempotency_key`` — он выводится
        из набора работ); рекурсия до одиночных элементов. Одиночный элемент, всё ещё
        отвергнутый, возвращается синтетическим ``CrawlResultItem(status="failed",
        error=...)`` — вызывающий честно помечает его per-URL ``FAILED`` ``source_unavailable``
        (без молчаливого дропа), сеанс финализируется ``PARTIAL``. Отвергнутые ``submit`` не
        создают заданий, поэтому повтор после деления безопасен.
        """
        assert executor is not None
        if not work:
            return {}

        # B2: ``session_id`` стабилен на прогон, а ``idempotency_key`` — по набору работ этой
        # волны (resume/развёртка каталога меняют набор → новый ключ → новое задание).
        session_key = session or default_session_id(str(run_id or ""))
        idempotency_key = _work_set_idempotency_key(work)
        profile = getattr(executor, "profile", None) or "split"

        # C2: подача с повтором при back-pressure (``accepted=False``), ограниченно.
        handle = ""
        accepted = False
        for attempt in range(_SUBMIT_MAX_ATTEMPTS):
            try:
                submission = executor.submit(
                    work,
                    session_id=session_key,
                    run_id=str(run_id or ""),
                    profile=profile,
                    idempotency_key=idempotency_key,
                )
            except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                errors.append(f"executor.submit: {type(exc).__name__}: {exc}")
                return {}
            if submission.accepted:
                handle = submission.job_id
                accepted = True
                break
            # Back-pressure: подождать (сначала ``retry_after_s``, иначе интервал опроса).
            if attempt + 1 < _SUBMIT_MAX_ATTEMPTS:
                executor_sleep(
                    submission.retry_after_s
                    if submission.retry_after_s is not None
                    else executor_poll_interval
                )

        if not accepted:
            # §3: back-pressure не снят. Одиночный элемент — честный per-URL ``FAILED``;
            # иначе — бисекция волны (рекурсия до одиночных).
            if len(work) == 1:
                only = work[0]
                reason = (
                    "executor.submit: back-pressure не снят после "
                    f"{_SUBMIT_MAX_ATTEMPTS} попыток (accepted=false)"
                )
                errors.append(f"{only.product_id}/{only.url}: {reason}")
                return {
                    (only.product_id, only.url): CrawlResultItem(
                        product_id=only.product_id,
                        url=only.url,
                        status="failed",
                        engine=only.engine,
                        kind=only.kind,
                        error=reason,
                    )
                }
            mid = len(work) // 2
            errors.append(
                "executor.submit: back-pressure не снят после "
                f"{_SUBMIT_MAX_ATTEMPTS} попыток (accepted=false) для волны из "
                f"{len(work)} работ — бисекция пополам"
            )
            left = _submit_collect(work[:mid])
            right = _submit_collect(work[mid:])
            left.update(right)
            return left

        # Опрос до терминального статуса (уже терминальное задание не пейсится).
        try:
            progress = executor.status(handle)
            while progress.status in ACTIVE_JOB_STATUSES:
                executor_sleep(executor_poll_interval)
                progress = executor.status(handle)
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            errors.append(f"executor: {type(exc).__name__}: {exc}")
            return {}

        # Сбор результатов пагинацией (``next_cursor``/``done``).
        collected: dict[tuple[str, str], CrawlResultItem] = {}
        cursor: str | None = None
        try:
            while True:
                batch = executor.collect(handle, cursor=cursor)
                for result in batch.items:
                    collected[(result.product_id, result.url)] = result
                if batch.done or not batch.next_cursor:
                    break
                cursor = batch.next_cursor
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            errors.append(f"executor.collect: {type(exc).__name__}: {exc}")
        return collected

    def _work_item(
        pid: str, url: str, target: Any, *, robots: bool = False
    ) -> CrawlWorkItem:
        """Собирает работу волны из резолвнутой цели (оси ``FetchPlan`` + провенанс).

        P3-A2/A2c: ``rate_limit`` переносится per-item — ровно тот **резолвнутый** лимит
        (``target.plan.rate_limit``), что локально лежит в ``desc.fetch_plan.rate_limit``
        после иерархии источников; pacing исполняет edge (§11). ``None`` — без пейсинга.

        A2b/§6(b): ``robots`` — egress-политика, вычисленная ядром на **корень**; edge
        строит краулеры «голыми», поэтому флаг обязан ехать с работы (иначе split-discovery
        не применяет robots и расходится с local по контенту).
        """
        plan_i = target.plan
        return CrawlWorkItem(
            product_id=pid,
            url=url,
            engine=target.engine,
            kind=target.kind,
            fetch=plan_i.fetch,
            extract=plan_i.extract,
            strategy=plan_i.strategy,
            wait_for=plan_i.wait_for,
            lang=None,
            rate_limit=plan_i.rate_limit,
            robots=robots,
        )

    # --- Pass A (ingest): crawl → retention → extract → dedup → chunk ---------- #
    # Векторы/физический индекс и персистенция corpus_chunks/index_manifest не выполняются:
    # сначала собирается содержимое всего батча, затем финализируется index_build_id.
    #
    # Phase 3.3: две ветки исполнения краула — локальная (``crawler``/``crawlers``, прежнее
    # поведение) и через ``executor`` (``split``/edge-планировщик). Общая часть (retention/
    # экстракция/дедуп/нарезка/сеансовый трекинг) вынесена в ``_retain``/``_ingest_one`` —
    # обе ветки используют её, не дублируя логику.
    ingests: list[_ProductIngest] = []

    def _emit_progress() -> None:
        """Phase 4: снимок прогресса сеанса для колбэка ``progress`` (``--follow``).

        Читает ``crawl_session_counts`` (единственный владелец — стор); ошибки колбэка/
        чтения изолируются (наблюдаемость не роняет фазу). Без активного сеанса — no-op.
        """
        if progress is None or session is None:
            return
        try:
            progress(dict(store.crawl_session_counts(session)))
        except Exception:  # noqa: BLE001 — прогресс информационен
            pass

    _emit_progress()

    def _expand_directory(pid: str, url: str, target: Any) -> list[str] | None:
        """Phase 5.2: разворачивает источник-каталог (``feed``/``channel``) в список ссылок.

        Возвращает ``None``, если ``target.kind`` — **не** директорийный (URL обрабатывается
        как обычная страница). Иначе поллит каталог через ``pollers[kind]`` и возвращает
        URL-записи (кандидаты, не evidence — I19): сам каталог **не** ретейнится и не даёт
        ``CorpusDoc``/чанков. Любой сбой каталога (нет поллера / плохие ``params`` / ошибка
        поллинга) — терминальный ``FAILED`` ``source_unavailable`` (сеанс финализируется
        ``PARTIAL``, а не «тихо пусто»). Записи ре-валидируются SSRF-политикой и дедуплицируются
        с сохранением порядка поллера; один уровень развёртки (записи повторно не расширяются).
        """
        if target.kind not in _DIRECTORY_KINDS:
            return None

        poller = (pollers or {}).get(target.kind)
        if poller is None:
            reason = (
                f"{pid}/{url}: источник-каталог kind={target.kind}, "
                "но поллер не сконфигурирован"
            )
            errors.append(reason)
            _mark(
                pid,
                url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                error=reason,
            )
            _emit_progress()
            return []

        problems = poller.validate_params(target.kind, target.params)
        if problems:
            reason = (
                f"{pid}/{url}: источник-каталог kind={target.kind}: "
                + "; ".join(problems)
            )
            errors.append(reason)
            _mark(
                pid,
                url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                error=reason,
            )
            _emit_progress()
            return []

        try:
            entries = poller.poll(
                url, kind=target.kind, params=target.params, plan=target.plan
            )
        except Exception as exc:  # noqa: BLE001 — изоляция каталога (не роняет фазу)
            reason = f"{pid}/{url}: poll: {type(exc).__name__}: {exc}"
            errors.append(reason)
            _mark(
                pid,
                url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                error=reason,
            )
            _emit_progress()
            return []

        # Каталог добыт: элемент ``FETCHED`` (raw НЕ сохраняется — это не контент/evidence,
        # только ссылки; resume такого элемента не даёт — ``_resume_source`` требует ``raw_path``).
        applied_engines[(pid, url)] = target.engine
        _mark(pid, url, CrawlItemStatus.FETCHED)
        _emit_progress()

        entry_urls: list[str] = []
        # N2: самокаталог — URL каталога уже добыт выше (kind/engine записи унаследуют
        # его); запись с URL == каталогу (self-listing фид) не фетчится повторно, не
        # ретейнится и не перезаписывает ``targets[(pid, url)]``.
        seen: set[str] = {url}
        for entry in entries:
            entry_url = str(getattr(entry, "url", "") or "")
            if not entry_url:
                continue
            try:
                validate_url(entry_url)
            except Exception as exc:  # noqa: BLE001 — SSRF-политика на каждой записи
                errors.append(
                    f"{pid}/{entry_url}: entry URL отклонён SSRF-политикой: {exc}"
                )
                continue
            if entry_url in seen:
                continue
            seen.add(entry_url)
            entry_urls.append(entry_url)
        return entry_urls

    # D3: sitemap разворачивается через ``PollPort(kind="sitemap")`` (local in-process ‖
    # split через шов ``crawl/poll``) и кэшируется по ``(product, sitemap_url)``: сайт
    # опрашивается один раз, даже если у него несколько seed-URL.
    sitemap_cache: dict[tuple[str, str], list[str]] = {}
    #: ``<loc>``, уже прикреплённые к какому-либо seed-root продукта (дедуп между seeds).
    sitemap_seed_used: set[tuple[str, str]] = set()

    def _sitemap_url_for(seed_url: str) -> str | None:
        """URL sitemap из лучшего per-site профиля seed-URL (``DomainProfile.sitemap``)."""
        profile = select_domain_profile(seed_url, domain_profiles or {})
        if profile is None:
            return None
        value = getattr(profile, "sitemap", None)
        return str(value) if value else None

    def _expand_sitemap(pid: str, seed_url: str) -> list[str]:
        """D3: разворачивает per-site ``sitemap`` в дополнительные depth-0 seed-URLs.

        Возвращает ``<loc>`` (после SSRF-проверки) или ``[]``, если у сайта нет sitemap. Сам
        sitemap — не страница: не ретейнится/не ингестится (элемент сеанса ``FETCHED`` без
        ``raw_path``, I19). Сбой/отсутствие поллера изолируется в ``errors`` (seed оператора
        сохраняется). Кэш по ``(product, sitemap_url)`` — один опрос на сайт.
        """
        sitemap_url = _sitemap_url_for(seed_url)
        if not sitemap_url:
            return []
        key = (pid, sitemap_url)
        cached = sitemap_cache.get(key)
        if cached is not None:
            return list(cached)

        locs: list[str] = []
        poller = (pollers or {}).get(_SITEMAP_KIND)
        if poller is None:
            reason = (
                f"{pid}/{sitemap_url}: sitemap задан профилем домена, "
                "но поллер sitemap не сконфигурирован"
            )
            errors.append(reason)
            _mark(
                pid,
                sitemap_url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                error=reason,
            )
            _emit_progress()
            sitemap_cache[key] = locs
            return locs

        problems = poller.validate_params(_SITEMAP_KIND, {})
        if problems:
            reason = f"{pid}/{sitemap_url}: sitemap: " + "; ".join(problems)
            errors.append(reason)
            _mark(
                pid,
                sitemap_url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                error=reason,
            )
            _emit_progress()
            sitemap_cache[key] = locs
            return locs

        # Провенанс: sitemap — не страница, собственный ``kind="sitemap"`` (строка-данные).
        starget = resolve_crawl_target(
            pid,
            sitemap_url,
            sources=crawl_sources or CrawlSourcesConfig(),
            profiles=domain_profiles or {},
            explicit=explicit_fetch,
            run_default_strategy=crawl_strategy_default,
            run_default_rate_limit=crawl_rate_default,
        )
        targets[(pid, sitemap_url)] = CrawlTarget(
            plan=starget.plan, engine=starget.engine, kind=_SITEMAP_KIND, params={}
        )
        try:
            entries = poller.poll(
                sitemap_url, kind=_SITEMAP_KIND, params={}, plan=starget.plan
            )
        except Exception as exc:  # noqa: BLE001 — изоляция sitemap (не роняет фазу)
            reason = f"{pid}/{sitemap_url}: sitemap poll: {type(exc).__name__}: {exc}"
            errors.append(reason)
            _mark(
                pid,
                sitemap_url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                error=reason,
            )
            _emit_progress()
            sitemap_cache[key] = locs
            return locs

        applied_engines[(pid, sitemap_url)] = starget.engine
        _mark(pid, sitemap_url, CrawlItemStatus.FETCHED)  # без raw: не контент (I19)
        _emit_progress()

        # ``<loc>`` — кандидаты: SSRF-перепроверка до становления seed (не ``evidence``).
        seen: set[str] = {sitemap_url}
        for entry in entries:
            loc = str(getattr(entry, "url", "") or "")
            if not loc or loc in seen:
                continue
            try:
                validate_url(loc)
            except Exception as exc:  # noqa: BLE001 — SSRF-политика на каждом <loc>
                errors.append(f"{pid}/{loc}: sitemap <loc> отклонён SSRF-политикой: {exc}")
                continue
            seen.add(loc)
            locs.append(loc)
        sitemap_cache[key] = locs
        return list(locs)

    def _fresh_sitemap_seeds(pid: str, seed_url: str) -> tuple[str, ...]:
        """``<loc>`` sitemap, ещё не прикреплённые ни к одному seed-root продукта (дедуп).

        Один и тот же sitemap у нескольких seed-URL даёт один набор ``<loc>``; прикреплять
        их к каждому корню — двойной фетч/бюджет, поэтому свежие seeds отдаются один раз.
        """
        fresh: list[str] = []
        for loc in _expand_sitemap(pid, seed_url):
            key = (pid, loc)
            if key in sitemap_seed_used:
                continue
            sitemap_seed_used.add(key)
            fresh.append(loc)
        return tuple(fresh)

    def _local_prefetch(
        pid: str,
        wu: str,
        wtarget: Any,
        *,
        depth: int | None = None,
        scope_host: str | None = None,
        enforce_redirect_scope: bool = False,
        robots: bool = False,
    ) -> _LocalFetch:
        """D4 pre-fetch (главный поток): resume/browser-guard/решение о пейсинге.

        **Чистая** фаза: не пишет ни в ``errors``, ни в сеанс, ни в ``applied_engines``.
        Все мутации отложены в :func:`_commit_local` (FIFO-порядок коммита) — это позволяет
        исполнять саму сеть в пуле, а исходы коммитить детерминированно на главном потоке.
        Активный краулер выбирается по ``wtarget.engine`` (fallback — ``crawler``).

        A2b/§6(b): ``robots`` проставляется в ``plan.robots`` (``dataclasses.replace``) —
        адаптер применяет ``robots.txt``-гейт только при ``self._robots is not None and
        plan.robots``; без discovery/принуждения флаг False (никаких лишних ``robots.txt``).
        """
        fetch_plan = (
            wtarget.plan
            if getattr(wtarget.plan, "robots", False) == robots
            else replace(wtarget.plan, robots=robots)
        )
        prior_item = items.get((pid, wu))
        resume = _resume_source(prior_item) if prior_item is not None else None
        # Движок: явный ``wtarget.engine`` (из ``crawl_sources``) приоритетен; при его
        # отсутствии ключ выводится из оси профиля домена (``fetch: browser`` → ``"browser"``).
        # Зеркало edge ``scheduler._resolve_engine`` и ``wiring._referenced_crawl_engines``
        # (последний кладёт ``browser`` в карту именно по ``domain_profiles.fetch``).
        # Литерал ``"browser"`` совпадает с ключом карты в ``wiring`` (импорт CLI-константы
        # нарушил бы слой ``pipeline ↛ cli``). Если карта не содержит ``"browser"``
        # (зависимость недоступна) — прежний фолбэк на ``crawler``, и честная
        # URL-скоупедированная ошибка D2 ниже срабатывает как раньше.
        engine_key = wtarget.engine or (
            "browser" if fetch_plan.fetch is FetchMode.BROWSER else None
        )
        active = ((crawlers or {}).get(engine_key) if engine_key else None) or crawler
        browser_blocked = (
            getattr(active, "supports_browser", True) is False
            and fetch_plan.fetch is FetchMode.BROWSER
        )
        # D4/e: в пул идёт ТОЛЬКО живой не-браузерный фетч. Браузерные элементы (даже при
        # ``supports_browser`` краулере) — никогда: ``BrowserCrawler`` водит единственную
        # ``_page`` без лока. Resume живого фетча не делает вовсе.
        poolable = (
            resume is None
            and not browser_blocked
            and fetch_plan.fetch is not FetchMode.BROWSER
        )
        host = (urlsplit(wu).hostname or "").lower()
        return _LocalFetch(
            pid=pid,
            wu=wu,
            depth=depth,
            fetch_plan=fetch_plan,
            active=active,
            engine_value=getattr(active, "engine", None) or wtarget.engine,
            prior_item=prior_item,
            resume=resume,
            browser_blocked=browser_blocked,
            poolable=poolable,
            host=host,
            scope_host=scope_host,
            enforce_redirect_scope=enforce_redirect_scope,
        )

    def _local_fetch(desc: _LocalFetch) -> RawPage:
        """D4 fetch-фаза (**единственный** вызов, идущий в пул): pacing + ``active.fetch``.

        Никакой мутации состояния сеанса здесь нет — только живой сетевой вызов.
        ``limiter.wait`` остаётся per-request (``RateLimiter`` стателесс); при инъекции
        ``sleep``/``rng`` они должны быть **потокобезопасны**, т.к. воркеры пула зовут pacing
        конкурентно.
        """
        limiter.wait(desc.host, desc.fetch_plan.rate_limit)
        return desc.active.fetch(desc.wu, desc.fetch_plan)

    def _fetch_and_commit(
        desc: _LocalFetch,
    ) -> tuple[RawPage, Path, FetchPlan, bool] | None:
        """Inline-фетч на главном потоке + коммит (последовательный путь и browser-режим D4).

        Используется, когда фетч **не** уходит в пул: ``max_workers in (None, 1)`` либо
        browser-элемент (``BrowserCrawler`` не потокобезопасен). ``limiter.wait`` — вне
        ``try`` (как в прежнем ``_process_local``): его сбой — не сбой источника.
        """
        limiter.wait(desc.host, desc.fetch_plan.rate_limit)
        try:
            page = desc.active.fetch(desc.wu, desc.fetch_plan)
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            return _commit_local(desc, exc=exc)
        return _commit_local(desc, page=page)

    def _commit_local(
        desc: _LocalFetch,
        *,
        page: RawPage | None = None,
        exc: Exception | None = None,
    ) -> tuple[RawPage, Path, FetchPlan, bool] | None:
        """D4 post-fetch (главный поток): мутации исхода в FIFO-порядке отправки.

        Возвращает ``(page, raw_path, fetch_plan, freshly_fetched)`` для последующей
        ``_ingest_one``; ``None`` — исход пропущен/сбой (терминальный ``FAILED`` уже
        проставлен ``_mark``), вызывающий делает ``continue``. ``freshly_fetched=False`` —
        resume из retained raw (живого фетча не было; ``max_documents`` его не считает).

        ``depth`` — глубина URL в BFS-frontier (персистится в элементе сеанса). ``scope_host``
        — host seed-root, ``enforce_redirect_scope`` — включён ли жёсткий redirect-scope
        (B4/D1): проверка финального URL применяется **только** при ``max_depth > 0``
        (discovery). На глубине 0 оператор явно запросил URL — финальная страница
        ретейнится как прежде (восстановленное поведение до D1). При discovery проверка
        **симметрична** (``_host_matches(final, root) or _host_matches(root, final)``):
        www↔apex / родительский редирект не теряется, а настоящий уход из семейства
        поднимает явный ``redirect_out_of_scope`` (не молчаливый дроп).

        **Детектируемость браузерного redirect-escape (A3).** ``BrowserCrawler`` теперь несёт в
        ``RawPage.url`` **финальный** (пост-редиректный) URL навигации — так же, как Scrapy и httpx,
        поэтому проверка ниже покрывает и браузерный путь. Исключение — кэш-хит браузера
        (``BrowserCrawler._page_from_cache``): content-addressed кэш хранит только тело по ключу
        запрошенного URL, восстановить финальный URL из него нельзя, поэтому для кэшированных
        страниц браузерный escape по-прежнему недетектируем (задокументировано там же).
        """
        pid, wu = desc.pid, desc.wu
        fetch_plan = desc.fetch_plan

        # S1/Phase 2.1: применяемый движок — первая мутация исхода (ориентир для ``_mark``),
        # как и прежде; всегда до resume-guard/бrowser-guard.
        applied_engines[(pid, wu)] = desc.engine_value

        if desc.resume is not None:
            assert desc.prior_item is not None  # resume возможен только при наличии элемента
            content, raw_path = desc.resume
            page = _resume_page(desc.prior_item, wu, content, raw_path, fetch_plan)
            # Счётчик отражает статус возобновляемого элемента (FETCHED — retryable,
            # FAILED — re-extract), а не ``pending``: он уже персистится в сеансе, и при
            # последующем сбое chunker'а URL будет посчитан как ``fetched``, а не потерян.
            return page, raw_path, fetch_plan, False

        # D2: браузерный рендер требует поддерживающего краулера. Проверка URL-скоупедирована
        # (а не на сборке композиции): фаза не падает, пропускается только этот URL.
        if desc.browser_blocked:
            reason = (
                f"{pid}/{wu}: требует браузерный рендер, но краулер "
                f"{type(desc.active).__name__} не поддерживает browser; "
                f"задайте COHOUND_SCOUT_CRAWLER=scrapy или browser"
            )
            errors.append(reason)
            # B2: пропущенный URL — неразрешённый исход, а не «не записанный pending».
            # При активном сеансе фиксируем элемент ``FAILED`` (source_unavailable),
            # чтобы сеанс финализировался ``PARTIAL`` (а не ложно ``COMPLETED``), а URL
            # оставался видимым/возобновляемым. Без сеанса — прежнее поведение (строки нет).
            _mark(
                pid,
                wu,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                error=reason,
                depth=desc.depth,
            )
            return None

        # 1. Краул: сбой/пустой RawPage — изоляция по контракту фазы.
        if exc is not None:
            reason = f"{type(exc).__name__}: {exc}"
            # D1/§10: robots-отказ несёт собственный ``refusal_reason`` (не сбой источника).
            error_kind = (
                getattr(exc, "refusal_reason", None)
                or FailureKind.SOURCE_UNAVAILABLE.value
            )
            errors.append(f"{pid}/{wu}: crawl: {reason}")
            _mark(
                pid,
                wu,
                CrawlItemStatus.FAILED,
                error_kind=error_kind,
                error=reason,
                depth=desc.depth,
            )
            return None
        if page is None:
            errors.append(f"{pid}/{wu}: crawl: пустой RawPage")
            _mark(
                pid,
                wu,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                error="пустой RawPage",
                depth=desc.depth,
            )
            return None

        # D1/B4: redirect-escape. При discovery (``max_depth > 0``) финальный URL (после
        # HTTP-редиректов) обязан остаться в host-scope корня — иначе страницу не ретейним
        # и не расширяем frontier (не «вытаскиваем» чужой контент под видом in-site).
        # На глубине 0 проверка выключена (оператор явно запросил URL) — прежнее поведение.
        if desc.scope_host and desc.enforce_redirect_scope:
            final_host = (urlsplit(page.url).hostname or "").lower()
            # Симметрия: www→apex и apex→www — одно семейство; сравнение «final в root»
            # в одну сторону теряло бы легитимный redirect www↔apex.
            if final_host and not (
                _host_matches(final_host, desc.scope_host)
                or _host_matches(desc.scope_host, final_host)
            ):
                reason = (
                    f"redirect вышел из scope: {page.url!r} "
                    f"(root host {desc.scope_host!r})"
                )
                errors.append(f"{pid}/{wu}: {reason}")
                _mark(
                    pid,
                    wu,
                    CrawlItemStatus.FAILED,
                    error_kind="redirect_out_of_scope",
                    error=reason,
                    depth=desc.depth,
                )
                return None

        # 2. Retention (I19): сырые байты пишутся ДО экстракции (общий шаг).
        raw_path = _retain(pid, wu, page, fetch_plan, depth=desc.depth)
        if raw_path is None:
            return None
        return page, raw_path, fetch_plan, True

    def _process_local(
        pid: str,
        wu: str,
        wtarget: Any,
        *,
        depth: int | None = None,
        scope_host: str | None = None,
        enforce_redirect_scope: bool = False,
        robots: bool = False,
    ) -> tuple[RawPage, Path, FetchPlan, bool] | None:
        """Локальный per-URL путь (последовательный): pre-fetch → fetch → post-fetch (D4).

        Точка входа для ``max_workers in (None, 1)`` — прежний однопоточный путь (пул не
        создаётся). Расщепление на фазы — задел под волновой пул (см.
        :func:`_local_prefetch`/:func:`_local_fetch`/:func:`_commit_local`); здесь фетч
        исполняется inline на главном потоке.
        """
        desc = _local_prefetch(
            pid,
            wu,
            wtarget,
            depth=depth,
            scope_host=scope_host,
            enforce_redirect_scope=enforce_redirect_scope,
            robots=robots,
        )
        if desc.resume is not None or desc.browser_blocked:
            return _commit_local(desc)

        # P2-3: politeness-пейсинг — единый владелец corpus (R7-независимый). Применяется
        # **только** к живому фетчу: путь ``resume`` (retained raw, выше) не пейсится.
        return _fetch_and_commit(desc)

    # D1/G: accumulators честности усечения. ``truncation_records`` — детализация по
    # ``(product, seed-root)``; флаг/продукты уходят в провенанс сеанса и ``IndexBuild``.
    truncation_records: list[dict[str, Any]] = []
    truncated_products: set[str] = set()
    pages_fetched_total = 0

    for product_id in products:
        ingest = _ProductIngest(product_id=product_id)
        # I17: дедуп в пределах продукт-партиции (см. docstring модуля).
        seen_docs: dict[str, str] = {}
        seen_chunk_ids: set[str] = set()

        def _drain_root(
            pid: str,
            root_url: str,
            root_target: Any,
            extra_seeds: Sequence[str] = (),
        ) -> Frontier:
            """D1/D3: BFS-drain одного seed-root (scope/блэклист/лимиты/глубина).

            Seeds — глубина 0; ссылка со страницы глубины ``d`` → ``d+1``. ``extra_seeds`` —
            дополнительные depth-0 seeds этого корня (D3: ``<loc>`` sitemap); их
            проверяет тот же блэклист/бюджет ``max_documents``, что и seed оператора.
            Ставка в очередь гейтится scope (host+path-граница) и блэклистом; ``max_documents``
            — жёсткий стоп (не исключение): останавливает постановку/обход этого корня.
            Возвращает frontier с провенансом усечения (``fetched``/``truncated``/
            ``truncated_reason``).
            """
            limits = resolve_crawl_limits(
                root_url,
                app=crawl_app or CrawlConfig(),
                run=crawl_defaults,
                profiles=domain_profiles or {},
            )
            frontier = Frontier(root_url, limits)
            # P5: уровень извлечения документов — свойство корня (per seed-root; один конвертер
            # на пул). ``full`` переиспользует переданный экстрактор (дефолт — без лишней
            # загрузки весов); иной уровень строится фабрикой, если она передана.
            level = limits.document_level
            root_extractor: ExtractorPort = extractor
            if level is not DocumentLevel.FULL and extractor_factory is not None:
                try:
                    root_extractor = extractor_factory(level)
                except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                    errors.append(
                        f"{pid}/{root_url}: extractor_factory({level.value}): "
                        f"{type(exc).__name__}: {exc} — уровень понижен до переданного "
                        "экстрактора"
                    )
                    root_extractor = extractor

            def _record_document_skip(url: str, reason: str) -> None:
                """P5: видимая запись пропуска документа (frontier + агрегат ``IndexBuild``)."""
                frontier.note_document_skip(url, reason)
                document_skips.append(
                    {
                        "product_id": pid,
                        "url": url,
                        "reason": reason,
                        "level": level.value,
                    }
                )

            def _skip_document_prefetch(work_url: str) -> bool:
                """P5: pre-fetch-пропуск документа при ``off`` (суффикс URL).

                ``True`` — URL пропущен (не фетчится, не ретейнится, не входит в
                ``max_documents``; запись видна в ``document_skips``). Блэклист побеждает
                (проверяется раньше). Content-type-детект суффикс-less URL — post-fetch путь.
                """
                if level is not DocumentLevel.OFF:
                    return False
                if document_suffix(work_url) is None:
                    return False
                _record_document_skip(work_url, "url_suffix")
                return True

            # P4: окно экстракции — **per seed-root**, лениво. ``None``/``1`` ⇒ пул не
            # создаётся (экстракция inline, байт-в-байт). ``W`` всегда конечный ``int ≥ 2``.
            extract_window = (
                int(limits.extract_workers)
                if limits.extract_workers is not None and int(limits.extract_workers) > 1
                else None
            )
            if extract_window is not None and extraction_pool is None:
                # Ось задана, но пул не сконфигурирован — не тихо: явная запись + inline.
                errors.append(
                    f"{pid}/{root_url}: crawl.extract_workers={extract_window} задан, "
                    "но пул экстракции не сконфигурирован — экстракция inline"
                )
            #: Исполнять ли экстракцию в пуле (снимается inline-fallback'ом при отказе).
            defer_extract = extract_window is not None and extraction_pool is not None
            extract_executor: Executor | None = None
            extract_rebuilds = 0
            pending: deque[_PendingExtract] = deque()
            # A2b/§6(b)/§10: egress-политика robots — **условна от discovery** и
            # вычисляется ядром **на корень** (edge строит краулеры «голыми»). Явный
            # ``crawl.robots`` (app-слой) переопределяет автодетект; иначе robots включён
            # ровно при обходе ссылок (``max_depth > 0``) или наличии sitemap у сайта корня.
            # Без discovery — ``False`` (никаких лишних fetch ``robots.txt``, байт-в-байт).
            if crawl_app is not None and crawl_app.robots is not None:
                robots_enabled = bool(crawl_app.robots)
            else:
                robots_enabled = (
                    limits.max_depth > 0 or _sitemap_url_for(root_url) is not None
                )
            # B2 (resume-бюджет): уже израсходованные страницы этого корня считаются из
            # durable-элементов сеанса (status FETCHED/FAILED с реально доступным retained
            # raw), чей URL в scope корня. Иначе повторный прогон того же session_id начал
            # бы ``fetched`` с нуля, заново обошёл resumed-страницу, превысил max_documents
            # и ложно финализировался COMPLETED. Считаем только resume-пригодные элементы
            # (fresh-фетч этого прогона инкрементит счётчик сам), чтобы не задвоить бюджет.
            consumed = sum(
                1
                for (ipid, iurl), item in items.items()
                if ipid == pid and frontier.in_scope(iurl)
                and _resume_source(item) is not None
            )
            frontier.note_consumed(consumed)
            seed_status = frontier.add_seed(root_url)
            if seed_status == "blacklisted":
                errors.append(
                    f"{pid}/{root_url}: seed зачёркнут блэклистом "
                    "(exclude_paths/exclude_documents) — не enqueued"
                )
                _emit_progress()
                return frontier
            if seed_status == "truncated":
                errors.append(
                    f"{pid}/{root_url}: seed не поставлен — достигнут max_documents"
                )
                _emit_progress()
                return frontier

            # D3: дополнительные depth-0 seeds (``<loc>`` sitemap) — под тем же блэклистом и
            # бюджетом, что и seed оператора (``add_seed`` возвращает статус).
            for extra in extra_seeds:
                extra_status = frontier.add_seed(extra)
                if extra_status == "blacklisted":
                    errors.append(
                        f"{pid}/{extra}: sitemap-seed зачёркнут блэклистом "
                        "(exclude_paths/exclude_documents) — не enqueued"
                    )
                elif extra_status == "truncated":
                    errors.append(
                        f"{pid}/{extra}: sitemap-seed не поставлен — достигнут max_documents"
                    )

            def _resolve_target(pid: str, url: str) -> Any:
                """Разрешает ``CrawlTarget`` URL (кэш ``targets``), как в обоих путях."""
                target = targets.get((pid, url))
                if target is None:
                    target = resolve_crawl_target(
                        pid,
                        url,
                        sources=crawl_sources or CrawlSourcesConfig(),
                        profiles=domain_profiles or {},
                        explicit=explicit_fetch,
                        run_default_strategy=crawl_strategy_default,
                        run_default_rate_limit=crawl_rate_default,
                    )
                    targets[(pid, url)] = target
                return target

            def _commit_outcome(
                pid: str,
                work_url: str,
                depth: int | None,
                outcome: tuple[RawPage, Path, FetchPlan, bool] | None,
                *,
                extracted: Any = _INLINE,
                extract_error: str | None = None,
            ) -> None:
                """Общий пост-ингест дренажа: retention-счётчик, доводка, BFS-расширение.

                Вызывается на главном потоке в FIFO-порядке; ``note_fetched`` — только здесь
                (жёсткий стоп ``max_documents`` не может быть превышен из воркеров пула).

                P4: ``extracted`` — результат экстракции от пула (``_INLINE`` ⇒ inline через
                ``_ingest_one``; иначе ``_finish_one`` с готовым ``ExtractedDoc``/``None`` и
                ``extract_error``). Граница расщепления — механическая (по ``extractor.extract``).
                """
                if outcome is None:
                    return  # отказ/сбой — frontier от него не расширяется
                page, raw_path, fetch_plan, freshly_fetched = outcome
                if freshly_fetched:
                    frontier.note_fetched()
                # P5: post-fetch-пропуск документа при ``off`` (тип узнан только по Content-Type;
                # фетч был ⇒ страница уже учтена в ``max_documents``, элемент сеанса — FETCHED
                # из retention; ``FAILED`` не ставим — skip ≠ сбой, §2/§11.3). Байты ретейнены.
                if (
                    level is DocumentLevel.OFF
                    and document_content_type(page.content_type) is not None
                ):
                    _record_document_skip(work_url, "content_type")
                    _emit_progress()
                    return
                if extracted is _INLINE:
                    _ingest_one(
                        pid,
                        work_url,
                        page,
                        fetch_plan,
                        raw_path,
                        ingest,
                        seen_docs,
                        seen_chunk_ids,
                        root_extractor,
                    )
                else:
                    _finish_one(
                        pid,
                        work_url,
                        page,
                        fetch_plan,
                        raw_path,
                        extracted,
                        ingest,
                        seen_docs,
                        seen_chunk_ids,
                        extract_error=extract_error,
                    )
                _emit_progress()
                # D1: извлечение in-site ссылок и следующий уровень BFS. При max_depth=0
                # извлечение не выполняется вовсе (байт-в-байт прежнее поведение).
                if limits.max_depth > 0 and _looks_like_html(page):
                    frontier.offer(extract_links(page.content, page.url), depth)

            def _make_extract_request(
                page: RawPage, raw_path: Path, fetch_plan: FetchPlan
            ) -> _ExtractRequest:
                """Строит picklable payload экстракции (примитивы + ``raw_path``; без байтов)."""
                return _ExtractRequest(
                    raw_path=str(raw_path),
                    url=page.url,
                    fetched_at=page.fetched_at,
                    fetch_mode=page.fetch_mode,
                    lang=page.lang,
                    status_code=page.status_code,
                    content_type=page.content_type,
                    engine=page.engine,
                    route=fetch_plan.extract,
                    level=level,
                )

            def _inflight_fresh() -> int:
                """Свеже-добытые страницы «в полёте» (учёт ёмкости с окном, P4/§3)."""
                return sum(1 for item in pending if item.freshly_fetched)

            def _has_fetch_capacity() -> bool:
                """Ёмкость корня с учётом in-flight: ``max_documents − fetched − inflight``."""
                if limits.max_documents is None:
                    return True
                return limits.max_documents - frontier.fetched - _inflight_fresh() > 0

            def _discard_extract_executor() -> None:
                nonlocal extract_executor
                executor_obj = extract_executor
                if executor_obj is None:
                    return
                try:
                    executor_obj.shutdown(wait=False)
                except Exception:  # noqa: BLE001 — reaping best-effort
                    pass
                extract_executor = None

            def _recover_extract_pool() -> None:
                """Отравленный пул: discard + bounded rebuild либо inline-fallback (§4).

                Повторное использование мёртвого пула запрещено; авто-ретрая нет.
                """
                nonlocal extract_rebuilds, defer_extract
                _discard_extract_executor()
                if extract_rebuilds < _EXTRACT_MAX_POOL_REBUILDS:
                    extract_rebuilds += 1
                else:
                    defer_extract = False
                    errors.append(
                        f"{pid}/{root_url}: пул экстракции исчерпал пересборки "
                        f"({_EXTRACT_MAX_POOL_REBUILDS}) — остаток корня inline"
                    )

            def _fail_extract(item: _PendingExtract, reason: str) -> None:
                """Явный терминальный ``FAILED parser_failed`` через ``_commit_outcome``.

                Страница уже ретейнена (``raw_path`` записан в submit-фазе), поэтому свежая
                считается в бюджете ``max_documents`` (``note_fetched``) — как в inline-пути
                (там ``note_fetched`` вызывается до экстракции). Иначе после отказов пул мог бы
                добрать замены сверх потолка.

                P4/§11.6: отказ доводится **единственным** упорядоченным путём —
                ``_commit_outcome`` с ``extracted=None``/``extract_error=reason`` (тот же
                ``note_fetched`` + дедуп/чанкер + ``_mark`` + ``errors`` + ``offer`` исходящих
                ссылок + ``_emit_progress``), а не изолированной ветвью. Иначе краш/таймаут
                терял бы BFS-выходы упавшей страницы (inline-путь их всегда предлагает), а
                ``note_fetched`` вызывался бы в обход ``_commit_outcome`` (§12.5).
                """
                _commit_outcome(
                    item.pid,
                    item.url,
                    item.depth,
                    (item.page, item.raw_path, item.fetch_plan, item.freshly_fetched),
                    extracted=None,
                    extract_error=reason,
                )

            def _abort_inflight(reason: str) -> None:
                """Помечает все in-flight (кроме уже снятого текущего) ``FAILED``."""
                while pending:
                    _fail_extract(pending.popleft(), reason)

            def _terminate_extract_executor() -> None:
                """Reaping зависшего пула (``terminate_workers`` доступен в Python 3.14)."""
                executor_obj = extract_executor
                if executor_obj is None:
                    return
                terminate = getattr(executor_obj, "terminate_workers", None)
                if terminate is not None:
                    try:
                        terminate()
                    except Exception:  # noqa: BLE001 — reaping best-effort
                        pass

            def _finish_pending(item: _PendingExtract) -> None:
                """Доводит oldest pending: ``future.result`` → ``_commit_outcome`` (FIFO).

                P4/§4: ``BrokenProcessPool`` — воркер умер (краш/OOM): все in-flight →
                ``FAILED``, пул discard+rebuild (bounded) либо inline-fallback. Таймаут —
                reap пула + то же. Никакого авто-ретрая.
                """
                try:
                    extracted = item.future.result(timeout=_extract_timeout)
                except TimeoutError:
                    _terminate_extract_executor()
                    _fail_extract(item, "extraction timeout")
                    _abort_inflight("extraction pool terminated after timeout")
                    _recover_extract_pool()
                    return
                except BrokenProcessPool:
                    _fail_extract(item, "extraction worker died")
                    _abort_inflight("extraction worker died")
                    _recover_extract_pool()
                    return
                except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                    _commit_outcome(
                        item.pid,
                        item.url,
                        item.depth,
                        (item.page, item.raw_path, item.fetch_plan, item.freshly_fetched),
                        extracted=None,
                        extract_error=f"{type(exc).__name__}: {exc}",
                    )
                    return
                _commit_outcome(
                    item.pid,
                    item.url,
                    item.depth,
                    (item.page, item.raw_path, item.fetch_plan, item.freshly_fetched),
                    extracted=extracted,
                )

            def _drain_for_window() -> None:
                """Доводит oldest, пока окно полно или ёмкость исчерпана (с in-flight)."""
                while pending and (
                    len(pending) >= extract_window or not _has_fetch_capacity()
                ):
                    _finish_pending(pending.popleft())

            def _commit_or_defer(
                pid_arg: str,
                url_arg: str,
                depth_arg: int | None,
                outcome: tuple[RawPage, Path, FetchPlan, bool] | None,
            ) -> None:
                """P4 submit/finish-шов: пул активен ⇒ отложить экстракцию в FIFO-очередь.

                Иначе — немедленный :func:`_commit_outcome` (inline, байт-в-байт). Отправка
                идёт с главного потока в порядке узлов (FIFO); окно не превышается инлайн-дренажем.
                """
                nonlocal extract_executor, defer_extract
                if outcome is None:
                    return
                if not defer_extract:
                    _commit_outcome(pid_arg, url_arg, depth_arg, outcome)
                    return
                page, raw_path, fetch_plan, freshly_fetched = outcome
                if extract_executor is None:
                    try:
                        extract_executor = extraction_pool.pool_factory(  # type: ignore[union-attr]
                            extract_window, level
                        )
                    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                        errors.append(
                            f"{pid_arg}/{url_arg}: extract pool: "
                            f"{type(exc).__name__}: {exc} — inline"
                        )
                        defer_extract = False
                        _commit_outcome(pid_arg, url_arg, depth_arg, outcome)
                        return
                try:
                    future = extract_executor.submit(
                        extraction_pool.worker,  # type: ignore[union-attr]
                        _make_extract_request(page, raw_path, fetch_plan),
                    )
                except Exception as exc:  # noqa: BLE001 — непиклящийся вход/сбой submit
                    errors.append(
                        f"{pid_arg}/{url_arg}: extract submit: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    _commit_outcome(pid_arg, url_arg, depth_arg, outcome)
                    return
                pending.append(
                    _PendingExtract(
                        pid_arg,
                        url_arg,
                        depth_arg,
                        page,
                        raw_path,
                        fetch_plan,
                        freshly_fetched,
                        future,
                    )
                )
                # Окно: не более ``W`` экстракций «в полёте» (доводка FIFO).
                while len(pending) >= extract_window:
                    _finish_pending(pending.popleft())

            def _commit_wave(entries: list[_WaveEntry]) -> None:
                """Коммитит исходы волны на главном потоке в FIFO-порядке отправки (D4).

                Порядок коммита = порядок отправки: детерминированы ``errors``, ``_mark``,
                победитель дедупа (``seen_docs[doc_hash] = url``) и ``offer`` — единственные
                порядко-чувствительные артефакты (идентичность корпуса content-addressed).
                """
                for entry in entries:
                    if entry.kind == "blacklisted":
                        # D1/H: блэклист применяется и перед фетчем (в дополнение к постановке).
                        errors.append(
                            f"{entry.pid}/{entry.wu}: зачёркнут блэклистом перед фетчем — пропущен"
                        )
                        continue
                    if entry.kind == "attempts":
                        prior_item = entry.prior_item
                        assert prior_item is not None
                        reason = (
                            f"max_attempts exhausted "
                            f"(attempts={prior_item.attempts} >= {max_attempts})"
                        )
                        errors.append(f"{entry.pid}/{entry.wu}: {reason}")
                        _mark(
                            entry.pid,
                            entry.wu,
                            CrawlItemStatus.FAILED,
                            error_kind=_MAX_ATTEMPTS_ERROR_KIND,
                            error=reason,
                            depth=entry.depth,
                        )
                        _emit_progress()
                        continue
                    desc = entry.desc
                    assert desc is not None
                    if entry.future is not None:
                        try:
                            page = entry.future.result()
                        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                            outcome = _commit_local(desc, exc=exc)
                        else:
                            outcome = _commit_local(desc, page=page)
                    elif desc.resume is not None or desc.browser_blocked:
                        # resume/browser-guard: живого фетча нет, пул не задействован.
                        outcome = _commit_local(desc)
                    else:
                        # browser-режим (``supports_browser`` краулер): в пул не идёт —
                        # фетч inline на главном потоке (``_fetch_and_commit``), как прежде.
                        outcome = _fetch_and_commit(desc)
                    _commit_or_defer(entry.pid, entry.wu, entry.depth, outcome)

            # D4: ограниченный пул ТОЛЬКО на сетевой fetch локального пути. Пул создаётся
            # лишь при ``max_workers > 1``: ``None``/``1`` — прежний однопоточный путь без
            # пула (байт-в-байт). В пул идут лишь work-items с живым HTTP-фетчем; браузерные
            # элементы — никогда (см. ``_local_prefetch``).
            max_workers = limits.max_workers
            pool: Executor | None = None
            # P3-A2: executor — подключаемый batch-транспорт; локальный пул при нём не нужен.
            if (
                executor is None
                and max_workers is not None
                and int(max_workers) > 1
            ):
                pool = (
                    pool_factory(int(max_workers))
                    if pool_factory is not None
                    else ThreadPoolExecutor(max_workers=int(max_workers))
                )
            # P3-A2 (§1(iii)): счётчик раундов executor-транспорта на этот корень.
            waves = 0
            try:
                while True:
                    # P4: доводим окно экстракции (окно полно и/или ёмкость исчерпана с
                    # учётом in-flight) до постановки/выхода — иначе пере-удержание.
                    if pending:
                        _drain_for_window()
                    if pending and len(frontier) == 0:
                        # Очередь корня пуста, но доводка in-flight может добавить ссылки
                        # (``offer``/BFS на finish) — доводим всё, прежде чем счесть исчерпание.
                        while pending:
                            _finish_pending(pending.popleft())
                    if not _has_fetch_capacity() and not pending:
                        # D3: seeds sitemap ставятся пачкой; ``max_documents`` обязан
                        # ограничивать и их (не только обнаруженные ссылки). Без лимита —
                        # всегда есть ёмкость. Проверка — в голове цикла на главном потоке.
                        frontier.mark_truncated("max_documents")
                        break
                    if executor is not None:
                        # --- P3-A2: executor-волна (раунд). Один раунд = один submit; --- #
                        # раунды строго последовательны (набор работы N+1 зависит от ссылок N).
                        if waves >= _EXECUTOR_MAX_WAVES_PER_ROOT:
                            # Предохранитель (§1(iii)): неограниченный BFS по общему edge —
                            # самонаведённый DoS. Никогда не останавливаться молча: усечение
                            # ``round_budget`` + явная ошибка + сеанс ``PARTIAL``.
                            frontier.mark_truncated("round_budget")
                            errors.append(
                                f"{pid}/{root_url}: предохранитель раундов исчерпан "
                                f"({_EXECUTOR_MAX_WAVES_PER_ROOT} волн) — обход остановлен "
                                "(round_budget)"
                            )
                            _emit_progress()
                            break
                        waves += 1
                        remaining = (
                            None
                            if limits.max_documents is None
                            else limits.max_documents
                            - frontier.fetched
                            - _inflight_fresh()
                        )
                        wave_size = (
                            wave_items if remaining is None else min(wave_items, remaining)
                        )
                        wave_size = min(int(wave_size), len(frontier))
                        if wave_size <= 0:
                            break
                        entries: list[_WaveEntry] = []
                        work: list[CrawlWorkItem] = []
                        for _ in range(wave_size):
                            node = frontier.next()
                            if node is None:
                                break
                            work_url, depth = node
                            if frontier.blacklisted(work_url):
                                # D1/H: блэклист применяется и перед фетчем.
                                entries.append(
                                    _WaveEntry(pid, work_url, depth, "blacklisted")
                                )
                                continue
                            if _skip_document_prefetch(work_url):
                                # P5: документ при ``off`` — pre-fetch-пропуск (виден).
                                _emit_progress()
                                continue
                            prior_item = items.get((pid, work_url))
                            if (
                                max_attempts is not None
                                and prior_item is not None
                                and prior_item.attempts >= max_attempts
                            ):
                                entries.append(
                                    _WaveEntry(
                                        pid,
                                        work_url,
                                        depth,
                                        "attempts",
                                        prior_item=prior_item,
                                    )
                                )
                                continue
                            work_target = _resolve_target(pid, work_url)
                            resume = (
                                _resume_source(prior_item)
                                if prior_item is not None
                                else None
                            )
                            # Дескриптор коммита строится вручную (не ``_local_prefetch``):
                            # browser-guard/``active`` — оси локального краулера, а здесь
                            # байты доставляет исполнитель; ``browser_blocked`` всегда False.
                            desc = _LocalFetch(
                                pid=pid,
                                wu=work_url,
                                depth=depth,
                                fetch_plan=work_target.plan,
                                active=None,
                                engine_value=work_target.engine,
                                prior_item=prior_item,
                                resume=resume,
                                browser_blocked=False,
                                poolable=False,
                                host=(urlsplit(work_url).hostname or "").lower(),
                                scope_host=frontier.root_host,
                                enforce_redirect_scope=limits.max_depth > 0,
                            )
                            entries.append(
                                _WaveEntry(pid, work_url, depth, "fetch", desc=desc)
                            )
                            if resume is None:
                                # Только живой фетч подаётся исполнителю; resume минует submit.
                                work.append(
                                    _work_item(
                                        pid, work_url, work_target, robots=robots_enabled
                                    )
                                )
                        collected = _submit_collect(work)
                        for entry in entries:
                            if entry.kind == "blacklisted":
                                errors.append(
                                    f"{entry.pid}/{entry.wu}: зачёркнут блэклистом "
                                    "перед фетчем — пропущен"
                                )
                                continue
                            if entry.kind == "attempts":
                                prior_item = entry.prior_item
                                assert prior_item is not None
                                reason = (
                                    f"max_attempts exhausted "
                                    f"(attempts={prior_item.attempts} >= {max_attempts})"
                                )
                                errors.append(f"{entry.pid}/{entry.wu}: {reason}")
                                _mark(
                                    entry.pid,
                                    entry.wu,
                                    CrawlItemStatus.FAILED,
                                    error_kind=_MAX_ATTEMPTS_ERROR_KIND,
                                    error=reason,
                                    depth=entry.depth,
                                )
                                _emit_progress()
                                continue
                            desc = entry.desc
                            assert desc is not None
                            if desc.resume is not None:
                                # Resume: живого фетча нет — коммит локально без submit.
                                outcome = _commit_local(desc)
                                _commit_or_defer(entry.pid, entry.wu, entry.depth, outcome)
                                continue
                            result = collected.get((entry.pid, entry.wu))
                            if result is None:
                                errors.append(
                                    f"{entry.pid}/{entry.wu}: executor: результат не получен"
                                )
                                _mark(
                                    entry.pid,
                                    entry.wu,
                                    CrawlItemStatus.FAILED,
                                    error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                                    error="executor: результат не получен",
                                    depth=entry.depth,
                                )
                                _emit_progress()
                                continue
                            if result.error or result.raw_bytes is None:
                                reason = result.error or "executor: пустой raw_bytes"
                                errors.append(f"{entry.pid}/{entry.wu}: executor: {reason}")
                                _mark(
                                    entry.pid,
                                    entry.wu,
                                    CrawlItemStatus.FAILED,
                                    error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                                    error=reason,
                                    depth=entry.depth,
                                )
                                _emit_progress()
                                continue
                            # S1: применяемый движок — фактический транспорт из результата шва.
                            desc.engine_value = result.engine or desc.engine_value
                            page = RawPage(
                                url=result.final_url or result.url,
                                content=result.raw_bytes,
                                fetched_at=result.fetched_at or "",
                                fetch_mode=desc.fetch_plan.fetch,
                                lang=None,
                                status_code=result.http_status,
                                content_type=result.content_type,
                                engine=result.engine,
                            )
                            outcome = _commit_local(desc, page=page)
                            _commit_or_defer(entry.pid, entry.wu, entry.depth, outcome)
                        continue
                    if pool is None:
                        # --- Последовательный путь (max_workers None/1): как прежде. --- #
                        node = frontier.next()
                        if node is None:
                            break
                        work_url, depth = node
                        # D1/H: блэклист применяется и перед фетчем (в дополнение к постановке).
                        if frontier.blacklisted(work_url):
                            errors.append(
                                f"{pid}/{work_url}: зачёркнут блэклистом перед фетчем — пропущен"
                            )
                            continue
                        if _skip_document_prefetch(work_url):
                            # P5: документ при ``off`` — pre-fetch-пропуск (виден; не в max_documents).
                            _emit_progress()
                            continue
                        prior_item = items.get((pid, work_url))
                        if (
                            max_attempts is not None
                            and prior_item is not None
                            and prior_item.attempts >= max_attempts
                        ):
                            reason = (
                                f"max_attempts exhausted "
                                f"(attempts={prior_item.attempts} >= {max_attempts})"
                            )
                            errors.append(f"{pid}/{work_url}: {reason}")
                            _mark(
                                pid,
                                work_url,
                                CrawlItemStatus.FAILED,
                                error_kind=_MAX_ATTEMPTS_ERROR_KIND,
                                error=reason,
                                depth=depth,
                            )
                            _emit_progress()
                            continue

                        work_target = _resolve_target(pid, work_url)
                        outcome = _process_local(
                            pid,
                            work_url,
                            work_target,
                            depth=depth,
                            scope_host=frontier.root_host,
                            # B4: жёсткий redirect-scope — только при discovery (max_depth > 0);
                            # на глубине 0 финальная страница ретейнится как прежде.
                            enforce_redirect_scope=limits.max_depth > 0,
                            robots=robots_enabled,
                        )
                        _commit_or_defer(pid, work_url, depth, outcome)
                        continue

                    # --- D4: волна fetch-only. Размер волны = min(K, остаток ёмкости, длина
                    # очереди): пере-удержание ``max_documents`` структурно невозможно.
                    # Диспатч (prefetch + submit) и коммит — на главном потоке; off-thread
                    # идёт только ``limiter.wait`` + ``active.fetch``. --- #
                    remaining = (
                        None
                        if limits.max_documents is None
                        else limits.max_documents
                        - frontier.fetched
                        - _inflight_fresh()
                    )
                    wave_size = (
                        max_workers if remaining is None else min(max_workers, remaining)
                    )
                    wave_size = min(int(wave_size), len(frontier))
                    if wave_size <= 0:
                        break
                    entries: list[_WaveEntry] = []
                    for _ in range(wave_size):
                        node = frontier.next()
                        if node is None:
                            break
                        work_url, depth = node
                        if frontier.blacklisted(work_url):
                            entries.append(
                                _WaveEntry(pid, work_url, depth, "blacklisted")
                            )
                            continue
                        if _skip_document_prefetch(work_url):
                            # P5: документ при ``off`` — pre-fetch-пропуск (виден).
                            _emit_progress()
                            continue
                        prior_item = items.get((pid, work_url))
                        if (
                            max_attempts is not None
                            and prior_item is not None
                            and prior_item.attempts >= max_attempts
                        ):
                            entries.append(
                                _WaveEntry(
                                    pid, work_url, depth, "attempts", prior_item=prior_item
                                )
                            )
                            continue
                        work_target = _resolve_target(pid, work_url)
                        desc = _local_prefetch(
                            pid,
                            work_url,
                            work_target,
                            depth=depth,
                            scope_host=frontier.root_host,
                            enforce_redirect_scope=limits.max_depth > 0,
                            robots=robots_enabled,
                        )
                        # D2/e: browser-элементы НИКОГДА не пулятся (``BrowserCrawler`` водит
                        # единственную ``_page`` без лока) — идут inline на главном потоке;
                        # resume-элементы тоже минуют пул (живого фетча нет; ``poolable``).
                        future = None
                        if desc.poolable:
                            assert pool is not None
                            future = pool.submit(_local_fetch, desc)
                        entries.append(
                            _WaveEntry(
                                pid, work_url, depth, "fetch", desc=desc, future=future
                            )
                        )
                    _commit_wave(entries)
                # P4: pending обязан доводиться до конца корня (весь дренаж, FIFO).
                while pending:
                    _finish_pending(pending.popleft())
            finally:
                if pool is not None:
                    pool.shutdown(wait=True)
                _discard_extract_executor()
            return frontier

        # P3-A2: **однопутевой** product-цикл. Развёртка каталогов/sitemap делается один раз
        # до drain (общий блок построения корней); транспорт выбирается по ``executor is None``.
        # Порядок expand → seed → drain на корень совпадает с локальным и является порядком
        # ingest (interleaving-трюк ``work_by_product`` больше не нужен).
        for url in url_map.get(product_id, []):
            prior_item = items.get((product_id, url))

            # Phase 4: потолок попыток (``--max-attempts``). URL, уже исчерпавший попытки,
            # не фетчится/не разворачивается: терминальный ``FAILED`` с явным ``error_kind``
            # (не тихий дроп) → сеанс финализируется ``PARTIAL``.
            if (
                max_attempts is not None
                and prior_item is not None
                and prior_item.attempts >= max_attempts
            ):
                reason = (
                    f"max_attempts exhausted "
                    f"(attempts={prior_item.attempts} >= {max_attempts})"
                )
                errors.append(f"{product_id}/{url}: {reason}")
                _mark(
                    product_id,
                    url,
                    CrawlItemStatus.FAILED,
                    error_kind=_MAX_ATTEMPTS_ERROR_KIND,
                    error=reason,
                )
                _emit_progress()
                continue

            # D2/Lane A: единая цель краула резолвится в pipeline и передаётся и краулеру,
            # и экстрактору (``plan`` + идентификаторы ``engine``/``kind`` из слоёв источников).
            target = resolve_crawl_target(
                product_id,
                url,
                sources=crawl_sources or CrawlSourcesConfig(),
                profiles=domain_profiles or {},
                explicit=explicit_fetch,
                run_default_strategy=crawl_strategy_default,
                run_default_rate_limit=crawl_rate_default,
            )
            targets[(product_id, url)] = target

            # Phase 5.2: источник-каталог (feed/channel) разворачивается в ссылки-записи;
            # каждая запись — **отдельный seed-root** глубины 0 (один уровень развёртки,
            # без ре-расширения). Обычный URL — единственный корень.
            entry_urls = _expand_directory(product_id, url, target)
            if entry_urls is None:
                # D3: seed оператора + ``<loc>`` его per-site sitemap как дополнительные
                # depth-0 seeds этого же корня (аугментация, не замена). Без sitemap —
                # пусто (поведение неизменно).
                roots: list[tuple[str, Any, tuple[str, ...]]] = [
                    (url, target, _fresh_sitemap_seeds(product_id, url))
                ]
            else:
                roots = []
                for entry_url in entry_urls:
                    etarget = resolve_crawl_target(
                        product_id,
                        entry_url,
                        sources=crawl_sources or CrawlSourcesConfig(),
                        profiles=domain_profiles or {},
                        explicit=explicit_fetch,
                        run_default_strategy=crawl_strategy_default,
                        run_default_rate_limit=crawl_rate_default,
                    )
                    # Провенанс записи = ``kind`` каталога-источника + собственный engine
                    # URL-записи (схему не меняем — только ``CrawlTarget``).
                    entry_target = CrawlTarget(
                        plan=etarget.plan,
                        engine=etarget.engine,
                        kind=target.kind,
                        params=etarget.params,
                    )
                    targets[(product_id, entry_url)] = entry_target
                    roots.append((entry_url, entry_target, ()))

            # D1: BFS-frontier по каждому seed-root (``max_depth=0`` → только seeds,
            # поведение неизменно). Провенанс усечения копится для сеанса/``IndexBuild``.
            for root_url, root_target, extra_seeds in roots:
                frontier = _drain_root(
                    product_id, root_url, root_target, extra_seeds
                )
                pages_fetched_total += frontier.fetched
                if frontier.truncated:
                    truncated_products.add(product_id)
                # P5: пропуск документов «прореживает» корпус продукта по построению — он
                # питает тот же канал аномалии, что и усечение (защита от false ❌ I2/I2b:
                # `source_scope: exhaustive` поверх «прореженного» продукта — аномалия
                # для человека). Причина — не ``max_*``, а ``document_level=off``.
                if frontier.skipped_documents:
                    truncated_products.add(product_id)
                truncation_records.append(
                    {
                        "product_id": product_id,
                        "root": root_url,
                        "pages_fetched": frontier.fetched,
                        "truncated": frontier.truncated,
                        "truncated_reason": frontier.truncated_reason,
                        "limits": frontier.limits.as_dict(),
                        "skipped_blacklisted": list(frontier.skipped_blacklisted),
                        # P5: видимые пропуски документов (``url_suffix``/``content_type``).
                        "skipped_documents": [dict(rec) for rec in frontier.skipped_documents],
                    }
                )

        # Retention-провенанс не зависит от индексной сборки: docs персистятся в Pass A.
        _persist_docs(store, ingest.docs, errors)
        ingests.append(ingest)

    _emit_progress()

    # --- Финализация index_build_id (по батчу: декларированные входы + контент) --- #
    # extractor_version: композит из фактически извлечённых документов; fallback — объявленная
    # версия экстрактора-порта (если ExtractedDoc не нёс версии).
    extractor_version_token = _extractor_token(
        {ingest.product_id: ingest.docs for ingest in ingests}
    ) or str(getattr(extractor, "version", "") or "")
    content_token = _content_token(
        {ingest.product_id: ingest.chunks for ingest in ingests}
    )
    index_build_id = _derive_index_build_id(
        package_id=package,
        params_token=_params_token(plan),
        snapshot_date=snapshot,
        chunker_version=resolved_chunker_version,
        embed_model_id=embed_model_id,
        extractor_version=extractor_version_token,
        lexical_engine_version=lexical_engine_version,
        vector_store=vector_store,
        content_token=content_token,
    )

    # --- Pass B (index): build_id → manifest → physical build → persist --------- #
    all_docs: list[CorpusDoc] = []
    entries: list[IndexManifestEntry] = []
    chunk_count = 0

    for ingest in ingests:
        product_id = ingest.product_id
        product_chunks = ingest.chunks
        entry: IndexManifestEntry | None = None

        if product_chunks:
            chunk_count += len(product_chunks)
            # build_id и extractor_version чанка — по финализированной сборке (Pass B).
            doc_versions = {doc.doc_id: doc.extractor_version for doc in ingest.docs}
            for chunk in product_chunks:
                chunk.build_id = index_build_id
                chunk.extractor_version = doc_versions.get(chunk.doc_id)

            entry = IndexManifestEntry(
                index_build_id=index_build_id,
                product_id=product_id,
                package_id=package,
                partition_key=f"{index_build_id}:{product_id}",
                snapshot_date=snapshot,
                chunker_version=resolved_chunker_version or None,
                extractor_version=extractor_version_token or None,
                embed_model_id=embed_model_id or None,
                embed_dim=getattr(embedder, "dim", None),
                lexical_engine_version=lexical_engine_version or None,
                vector_store=vector_store or None,
                content_digest=_product_content_token(product_chunks),
                store_path=f"index_store/{package}/{index_build_id}/{product_id}/",
                created_at=snapshot,
                stats_json=json.dumps(
                    {
                        "chunks": len(product_chunks),
                        "docs": len({c.doc_id for c in product_chunks}),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
            entries.append(entry)

        # «Disk first, DB second»: физическая сборка авторитетна. Пока build не завершился
        # успешно, corpus_chunks/index_manifest не пишутся (K2/§13); entry остаётся в entries
        # для провенанса/log (IndexBuild).
        physical_ok = True
        if entry is not None and embedder is not None and index_builder is not None:
            physical_ok = _embed_and_build(
                embedder,
                index_builder,
                product_id,
                product_chunks,
                package_id=package,
                chunker_version=resolved_chunker_version,
                embed_model_id=embed_model_id,
                snapshot_date=snapshot,
                index_build_id=index_build_id,
                errors=errors,
            )

        all_docs.extend(ingest.docs)
        if physical_ok:
            _persist_chunks_and_manifest(store, product_chunks, entry, errors)
        else:
            errors.append(
                f"{product_id}: физическая сборка индекса не зафиксирована — "
                "corpus_chunks/index_manifest не записаны (K2/§13); "
                "изменение корпуса требует нового run_id/раунда"
            )

    if entries and embedder is not None and index_builder is not None:
        try:
            index_builder.commit()
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            errors.append(f"index commit: {type(exc).__name__}: {exc}")

    # L1a: финализация сеанса (``COMPLETED`` без сбоев, иначе ``PARTIAL``) — единственный
    # вызов на фазу. ``finished_at`` фиксирует момент завершения.
    if session is not None:
        try:
            # Phase 2/2.1: провенанс сеанса — движки/виды источников из **персистенных
            # элементов** (``items.values()``: фактически применённый ``engine`` и ``kind``
            # элемента) + дайджест конфигурации источников. ``targets`` для агрегата не
            # годится: там лишь запрошенный движок (может быть ``None``).
            engines = sorted(
                {str(i.engine) for i in items.values() if i.engine}
            )
            kinds = sorted(
                {str(i.kind) for i in items.values() if i.kind}
            )
            digest = (
                crawl_sources_digest(crawl_sources)
                if crawl_sources is not None
                else None
            )
            finalize_session(
                store,
                session,
                finished_at=_now_iso(),
                engines=engines,
                kinds=kinds,
                crawl_sources_digest=digest,
                # D1/G: политико-усечённый краул не рапортуется ``COMPLETED`` (→ ``PARTIAL``);
                # эффективные лимиты/число добытых страниц/усечённые продукты — в провенанс.
                truncated=bool(truncated_products),
                effective_limits={"roots": truncation_records},
                pages_fetched=pages_fetched_total,
                truncated_products=sorted(truncated_products),
            )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(f"finalize_crawl_session({session}): {type(exc).__name__}: {exc}")

    # L1a/C2: счётчики durable-краула (наблюдаемость) — целиком из элементов сеанса. Без
    # сеанса (``store``/``run_id`` не заданы) — нули. ``crawl_done`` — зеркало ``crawl_fetched``
    # (элемент ``FETCHED`` = «ready»); отдельных ``done``/``cancelled`` у элемента нет, поэтому
    # ``CANCELLED`` складывается в ``crawl_failed`` только для счётчика.
    crawl_pending = crawl_fetched = crawl_failed = 0
    if session is not None:
        try:
            counts = store.crawl_session_counts(session)
            crawl_pending = counts.get(CrawlItemStatus.PENDING.value, 0) + counts.get(
                CrawlItemStatus.RUNNING.value, 0
            )
            crawl_fetched = counts.get(CrawlItemStatus.FETCHED.value, 0)
            crawl_failed = counts.get(CrawlItemStatus.FAILED.value, 0) + counts.get(
                CrawlItemStatus.CANCELLED.value, 0
            )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(f"crawl_session_counts: {type(exc).__name__}: {exc}")

    build = IndexBuild(
        index_build_id=index_build_id,
        entries=entries,
        docs=all_docs,
        chunk_count=chunk_count,
        crawl_pending=crawl_pending,
        crawl_fetched=crawl_fetched,
        crawl_done=crawl_fetched,
        crawl_failed=crawl_failed,
        truncated=bool(truncated_products),
        truncation=truncation_records,
        document_skips=list(document_skips),
    )
    # Примечание: индексная сборка «возвращается» в log_entries (PhaseResult — контракт фазы;
    # W8 читает IndexBuild отсюда). Счётчики — в полях IndexBuild; дубликаты — в errors.
    return PhaseResult(status="ok", phase="corpus", cells=[], log_entries=[build], errors=errors)
