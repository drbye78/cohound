"""Допуск и разбор одного URL: retention, переиспользование, локальный фетч, развёртка.

Часть декомпозиции ``corpus.py`` (план ``docs/reports/2026-10-30-large-modules-refactoring-plan.md``,
L5 шаг 2). Из тела :func:`core.pipeline.corpus._build_corpus` вынесены замыкания **среднего
слоя**, у которых все зависимости — **уровня фазы**: порт состояния, стор, сеанс краула,
счётчики, ошибки, оси краула, пул экстракции. Всё, что зависит от конкретного корня
(``frontier``, ``limits``, ``root_extractor``, окно экстракции), осталось в теле: это
шаг 3 (корневой цикл/``finish_one``).

**Почему это не «модуль про ингест», а «допуск и разбор».** Одно назначение — превратить
страницу или объявленный файл в durable-запись: решить, можно ли её брать (SSRF/scope/
блэклист/потолки), где лежат её байты (retention, I19), что уже есть в ``url_state``
(валидаторы, переиспользование версии), и как её развернуть (каталог, sitemap, спуск
остаётся в теле). Локальный путь фетча (pre-fetch → fetch → post-fetch commit) входит сюда
же: это тот же разбор одного URL, только с сетью посередине, и его пост-фаза —
единственный writer ``url_state``/элемента сеанса.

**Почему параметры явные.** Раньше это были свободные переменные замыканий: сигнатура
молчала о том, что функция пишет в сеанс и читает состояние. Теперь каждая зависимость
названа. Там, где набор аргументов — константа фазы (``mark``/``retain``/``validators``),
вызывающий связывает их один раз через :func:`functools.partial`; здесь же функции,
которые зовут друг друга, получают уже связанный вызываемый объект параметром
(``mark``/``retain``/``finish_one``/``resolve_target``) — так модуль не знает, **откуда**
берётся сеанс, и обратного импорта ``corpus`` не возникает.

**Порядок записи не меняется.** Тела перенесены дословно. Оракул —
``tests/pipeline/test_corpus_write_order.py`` (последовательность SQL-операторов).
"""

from __future__ import annotations

import contextlib
import datetime as _dt
import errno
import hashlib
import json
import os
import shutil
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import Executor
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.config.crawl_sources import CrawlSourcesConfig
from core.config.domain_profiles import DomainProfile
from core.config.seeds import normalize_seed_url
from core.domain import (
    ArchiveDescentStatus,
    CorpusChunk,
    CorpusDoc,
    CrawlItemStatus,
    CrawlSessionItem,
    CrawlStrategy,
    DocumentLevel,
    ExtractionRoute,
    FailureKind,
    IncrementalKind,
    PdfPipeline,
    ProductMode,
    RefreshPolicy,
    ReuseKind,
)
from core.pipeline.artifact_probe import probe_artifact
from core.pipeline.corpus_index import _chunk_id, _CorpusStageTimings
from core.pipeline.corpus_persist import (
    CLEAR_SESSION_FIELD,
    DocReuse,
    _now_iso,
    as_product_mode,
    canonical_failure_kind,
    count_doc,
    find_doc_version,
    manual_host_scope,
    mark_session,
    reuse_plan,
    sitemap_url_for,
    supersede_document,
    write_document_state,
)
from core.pipeline.corpus_state import CorpusCounters
from core.pipeline.crawl_frontier import (
    SCOPE_ARTIFACT_OUTSIDE_PREFIX,
    SCOPE_OUT,
    CrawlLimits,
    Frontier,
    is_blacklisted,
)
from core.pipeline.crawl_pacing import CrawlPacing
from core.pipeline.crawl_session import default_session_id
from core.pipeline.crawl_sources import CrawlTarget, resolve_crawl_target
from core.pipeline.extract_cache import ExtractionCache
from core.pipeline.extraction_runtime import (
    _RESUME_READ_CHUNK,
    ExtractionPool,
    _as_text,
    _cache_extractor_version,
    _ExtractRequest,
    _hash_file,
    _page_with_source_path,
)
from core.pipeline.fetch_plan import _host_matches
from core.pipeline.incremental import (
    IncrementalItem,
    classify_reuse,
    doc_version_id,
    plan_incremental,
)
from core.pipeline.liveness import Heartbeat
from core.pipeline.manual_artifacts import (
    ORIGIN_CRAWL,
    ORIGIN_MANUAL,
    ManualArtifact,
)
from core.pipeline.refresh_modes import dispatch_refresh
from core.ports.chunker import ChunkerPort
from core.ports.container import ContainerPort
from core.ports.crawl_errors import is_transient_fetch_error
from core.ports.crawl_executor import (
    ACTIVE_JOB_STATUSES,
    CrawlExecutorPort,
    CrawlResultItem,
    CrawlWorkItem,
)
from core.ports.crawler import CrawlerPort
from core.ports.extractor import ExtractorPort
from core.ports.poller import PollPort
from core.ports.state import (
    CapturedDocRow,
    ContainerStateRow,
    DocumentVersionRow,
    ProductStatePort,
    UrlStateRow,
)
from core.ports.types import FetchMode, FetchPlan, RawPage
from core.util.document_formats import (
    BINARY_SNIFF_BYTES,
    document_suffix,
    is_terminal_artifact,
    unsupported_binary_reason,
)
from core.util.document_probe import CLASS_DOCUMENT

# ``FETCH_RETRY_TIMES`` реэкспортируется намеренно (``X as X``): ``corpus_ingest.
# FETCH_RETRY_TIMES`` — публичный адрес локального пути для его тестов, но значение у
# него одно на оба пути фетча (edge берёт ту же константу из ``core.util.fetch_retry``).
from core.util.fetch_retry import FETCH_RETRY_TIMES as FETCH_RETRY_TIMES
from core.util.fetch_retry import FetchAttemptStats, fetch_with_paced_retries
from core.util.hashing import content_hash, short_hash
from core.util.rate_limit import RateLimiter
from core.util.security import validate_url

#: Подкаталог ``raw_root`` для staging-файлов допущенных артефактов: тот же ФС, что
#: и ``corpus_raw``, поэтому публикация — ``os.replace`` (переезд, не копия).
_STAGING_DIRNAME = "_staging"


#: Phase 3 B2: максимум попыток подачи работ исполнителю при back-pressure
#: (``accepted=False``). После исчерпания — изолированный ``FAILED`` (``source_unavailable``),
#: а не тихий провал всего прогона.
_SUBMIT_MAX_ATTEMPTS = 5


#: Phase 5.2: семантика источника-каталога (свободная строка-данные, не доменный enum).
#: URL с таким ``target.kind`` — не контент-страница, а лента/канал: он **поллится** через
#: ``Ports.pollers[kind]``, а полученные ссылки-записи добываются обычным путём. Один уровень
#: развёртки: записи повторно не расширяются.
DIRECTORY_KINDS: frozenset[str] = frozenset({"feed", "channel"})


#: D3: ``kind`` опроса sitemap (``DomainProfile.sitemap``) — свободная строка-данные. Sitemap
#: не входит в ``DIRECTORY_KINDS``: это per-site опция профиля домена, а не источник
#: ``crawl_sources.yaml``. Разворачивается через ``pollers["sitemap"]`` (local ‖ split).
_SITEMAP_KIND = "sitemap"


#: Классы отказа ``<loc>`` sitemap от scope корня (F6): ``host`` — sitemap рекламирует
#: другой host (сигнал чужого сайта/SSRF), ``path`` — host тот же, но путь вне префикса
#: корня (host-wide sitemap при path-scoped seed — **нормальная** конфигурация). Классы
#: считаются и репортятся раздельно: их смешение скрывало бы host-сигнал в массе path-отказов.
_SITEMAP_SKIP_HOST = "host"


_SITEMAP_SKIP_PATH = "path"


#: ``Content-Type`` члена по суффиксу — для экстрактора и провенанса (данные, не эвристика).
MEMBER_CONTENT_TYPES: dict[str, str] = {
    ".pdf": "application/pdf",
    ".docx": (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ),
    ".pptx": (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    ),
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".md": "text/markdown",
    ".markdown": "text/markdown",
    ".txt": "text/plain",
    ".html": "text/html",
    ".htm": "text/html",
}


def _state_url_key(url: str) -> str | None:
    """Ключ ``url_state`` для URL: канонизация §7.2 (:func:`normalize_seed_url`).

    Единственный источник истины нормализации URL — ``core.config.seeds``: и seed-набор,
    и планировщик (``incremental._normalize_url``), и этот ключ обязаны совпадать, иначе
    строка состояния не нашлась бы по только что наблюдённому URL.

    :returns: нормализованный URL либо ``None`` (URL не нормализуется — состояние по нему
        не ведётся; вызывающий решает, писать ли видимую ошибку).
    """
    try:
        return normalize_seed_url(str(url or ""))
    except ValueError:
        return None


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


_TERMINAL_FAILURE_KINDS: frozenset[FailureKind] = frozenset(
    {
        FailureKind.NOT_FOUND,
        FailureKind.PAYLOAD_TOO_LARGE,
        FailureKind.UNSUPPORTED_BINARY,
    }
)


def scope_owner(url: str, scopes: Sequence[tuple[str, Frontier]]) -> str | None:
    """Корень-владелец URL по scope-инференсу (09 §6.3, «Владение known-URL»).

    Побеждает **самый специфичный** ``root_path_prefix`` (длина пути), при равенстве —
    авторский порядок (``scopes`` идёт в авторском порядке, строгое ``>`` оставляет
    первого). ``None`` — URL вне scope всех корней: он не принадлежит никому, и тратить
    бюджет чужого корня на него нельзя.

    Функция — **единственный** носитель этого предиката: им пользуются и группировка
    ревалидации (``group_revalidation_items``), и реконструкция израсходованного бюджета
    на resume (корневой цикл). Два разных правила («кто первый» vs «кто специфичнее»)
    расходились бы ровно на тех URL, где это дороже всего: измерено, что host-корень с
    префиксом ``/`` забирал зарядку у docs-корня, и потолок умножался на число перезапусков.
    """
    best: str | None = None
    best_len = -1
    for root, probe in scopes:
        if not probe.in_scope(url):
            continue
        if len(probe.path_prefix) > best_len:
            best = root
            best_len = len(probe.path_prefix)
    return best


def _url_origin(url: str) -> str:
    """Origin URL (``scheme://host[:port]``) — ключ orphan-бакета ревалидации.

    ``url_state`` не хранит seed-происхождение; для известного URL вне scope всех
    авторских корней единственная устойчивая группировка — origin (продукт + origin).
    """
    parts = urlsplit(url)
    return f"{(parts.scheme or '').lower()}://{parts.netloc.lower()}"


def _sitemap_skip_class(loc: str, root_url: str) -> str:
    """Класс отказа ``<loc>`` sitemap от scope корня (F6): ``host`` либо ``path``.

    ``host`` — host ``<loc>`` не совпадает с host корня (sitemap рекламирует другой сайт —
    сигнал чужого host/SSRF). ``path`` — host тот же, но путь вне path-префикса корня
    (в т.ч. traversal-guard ``..``): host-wide sitemap при path-scoped seed, т.е. штатная
    конфигурация. Вызывается только для loc, уже отвергнутых ``Frontier.in_scope`` (SSRF-
    отклонённые loc до этого не доходят — у них своя построчная запись в ``expand_sitemap``).
    """
    root_host = (urlsplit(root_url).hostname or "").lower()
    host = (urlsplit(loc).hostname or "").lower()
    if not host or not _host_matches(host, root_host):
        return _SITEMAP_SKIP_HOST
    return _SITEMAP_SKIP_PATH


def looks_like_html(page: RawPage) -> bool:
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


@dataclass(frozen=True)
class RetainedReject:
    """Retained raw не пригоден по границе исполнения (P0): терминально, без экстракции."""

    error_kind: FailureKind
    reason: str
    path: Path


@dataclass(frozen=True)
class RetainedRaw:
    """Retained-файл, пригодный для resume (D3) без обязательного чтения в RAM.

    ``data is None`` — файл **не** буферизован (допущенный артефакт-контейнер): байты
    читает только тот, кому они реально нужны (спуск/экстрактор), а path остаётся
    источником истины I19. ``head`` — первые :data:`BINARY_SNIFF_BYTES` байт (post-fetch
    классификация без чтения файла); у буферизованных страниц ``head is None``.
    """

    path: Path
    content_hash: str
    size: int
    head: bytes | None = None
    data: bytes | None = None


def _publish_staged(staged: Path, target: Path) -> None:
    """Публикует staging-файл как retained: ``os.replace`` (переезд, не копия).

    Staging-каталог создаётся ядром под ``raw_root``, поэтому штатно файлы на одном ФС и
    ``os.replace`` атомарен. Если адаптер всё же дал файл с другого ФС (``EXDEV``) —
    потоковая копия ядром (``shutil.copyfile``: sendfile/``copy_file_range``, без чтения
    тела в RAM) и удаление staging: байты не теряются, но и RAM не растёт.
    """
    try:
        os.replace(staged, target)
        return
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
    shutil.copyfile(staged, target)
    staged.unlink(missing_ok=True)


def artifact_is_terminal(
    url: str | None,
    content_type: str | None,
    attachment_formats: Sequence[str] | None,
) -> bool:
    """Класс «терминальный артефакт» — контейнер **или** документ (P6b).

    Единый класс допуска/потолка: такой ответ не продолжает навигацию, поэтому вправе
    быть больше страничного ``max_response_bytes`` и удерживаться под
    ``max_artifact_bytes`` (``pdf``/``docx``/``pptx``/``xlsx`` и ``zip``/``7z``/``rar``).
    Документная часть от ``attachment_formats`` не зависит (совпадает с реестром
    экстрактора); ``.rtf`` не допускается — читателя нет.
    """
    return is_terminal_artifact(url or None, content_type, attachment_formats)


def resume_retained(
    item: CrawlSessionItem | None,
    *,
    max_bytes: int | None = None,
    artifact_max_bytes: int | None = None,
    attachment_formats: Sequence[str] | None = None,
) -> RetainedRaw | RetainedReject | None:
    """Возвращает retained-источник для resume, если элемент сеанса позволяет НЕ фетчить (D3).

    Пригодны элементы ``FETCHED`` (сырые байты уже сохранены) и ``FAILED`` с сохранённым
    ``raw_path`` (re-extract из тех же байтов без сети). ``PENDING``/``RUNNING``/отсутствие
    записи, отсутствие ``raw_path`` или файла на диске → ``None`` (обычный путь фетча).

    **P0/P6b: потолок по классу.** Размер проверяется по ``stat`` **до** чтения. Класс
    определяется по метаданным записи (URL-суффикс/``Content-Type``, заявленный набор
    ``attachment_formats``): допущенный **терминальный артефакт** — документ
    (``pdf``/``docx``/``pptx``/``xlsx``) **или** контейнер (``zip``/``7z``/``rar``) —
    ограничен ``artifact_max_bytes``, страница — ``max_bytes``. Так retained-PDF/артефакт в
    125 MB резюмится, а страница того же размера по-прежнему терминально отвергается
    ``payload_too_large``.

    **Память.** Страницы (≤ ``max_bytes``) буферизуются как прежде — путь байт-в-байт
    прежний. Артефакт **не** буферизуется: возвращается :class:`RetainedRaw` с путём,
    потоковым хешем и первыми 8 KiB (``head``) — файл целиком в RAM не попадает.

    **Верификация (D3).** Если в записи задан ``content_hash``, а потоковый хеш файла
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
    artifact = artifact_is_terminal(item.url, item.content_type, attachment_formats)
    cap = artifact_max_bytes if artifact and artifact_max_bytes is not None else max_bytes
    try:
        size = path.stat().st_size
    except OSError:
        return None
    if cap is not None and size > cap:
        return RetainedReject(
            FailureKind.PAYLOAD_TOO_LARGE,
            f"retained {size} байт > лимита {cap} (payload_too_large)",
            path,
        )
    hasher = hashlib.sha256()
    head = bytearray()
    buffer: bytearray | None = None if artifact else bytearray()
    read = 0
    try:
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(_RESUME_READ_CHUNK)
                if not chunk:
                    break
                read += len(chunk)
                if cap is not None and read > cap:
                    # Файл вырос после stat: режем до буферизации остатка.
                    return RetainedReject(
                        FailureKind.PAYLOAD_TOO_LARGE,
                        f"retained превысил лимит {cap} при чтении "
                        "(payload_too_large)",
                        path,
                    )
                hasher.update(chunk)
                if len(head) < BINARY_SNIFF_BYTES:
                    head.extend(chunk[: BINARY_SNIFF_BYTES - len(head)])
                if buffer is not None:
                    buffer.extend(chunk)
    except OSError:
        return None
    digest = hasher.hexdigest()
    if item.content_hash and digest != item.content_hash:
        return None
    snapshot = bytes(head)
    buffered = None if buffer is None else bytes(buffer)
    binary_reason = unsupported_binary_reason(
        buffered if buffered is not None else snapshot,
        url=item.url,
        content_type=item.content_type,
        attachment_formats=attachment_formats,
    )
    if binary_reason is not None:
        return RetainedReject(
            FailureKind.UNSUPPORTED_BINARY,
            f"retained: {binary_reason}",
            path,
        )
    return RetainedRaw(
        path=path,
        content_hash=digest,
        size=read,
        head=None if buffered is not None else snapshot,
        data=buffered,
    )


def resume_page(
    item: CrawlSessionItem,
    url: str,
    retained: RetainedRaw,
    fetch_plan: FetchPlan,
    validators: tuple[str | None, str | None] | None = None,
) -> RawPage:
    """Реконструирует ``RawPage`` из retained-источника и элемента сеанса (без сети, D3).

    ``retained.data is not None`` — обычная страница: ``content`` — буферизованные байты
    (байт-в-байт прежнее поведение). ``data is None`` — артефакт-контейнер: ``content``
    пуст, ``staged_path`` указывает на retained-файл, ``head`` — первые 8 KiB; файл в RAM
    не читается.

    ``fetched_at`` — из ``last_attempt`` записи, иначе mtime файла (UTC). ``fetch_mode`` —
    из записи (если распознаётся, иначе ``fetch_plan.fetch``). ``content_type``/``lang``/
    ``status_code`` — из провенанса записи (``lang`` персистится, иначе свежий и
    возобновлённый прогоны расходились бы по ``corpus_docs.lang``/``corpus_chunks.lang``).

    **Валидаторы (09 §6.3).** ``CrawlSessionItem`` не хранит ``ETag``/``Last-Modified``
    (колонок нет — сеанс трекает краул, а не условную валидацию), поэтому валидаторы
    приходят аргументом из ``url_state`` (durable-источник) и переносятся в ``RawPage``,
    чтобы запись состояния после resume не потеряла их. ``not_modified`` всегда ``False``:
    resume перечитывает retained-байты, серверного подтверждения ``304`` не было.
    """
    fetched_at = item.last_attempt
    if not fetched_at:
        try:
            fetched_at = _dt.datetime.fromtimestamp(
                retained.path.stat().st_mtime, tz=_dt.UTC
            ).isoformat()
        except OSError:
            fetched_at = ""
    fetch_mode = fetch_plan.fetch
    if item.fetch_mode:
        try:
            fetch_mode = FetchMode(item.fetch_mode)
        except ValueError:
            fetch_mode = fetch_plan.fetch
    etag, last_modified = validators if validators is not None else (None, None)
    buffered = retained.data is not None
    return RawPage(
        url=url,
        content=retained.data or b"",
        fetched_at=fetched_at or "",
        fetch_mode=fetch_mode,
        lang=item.lang,
        status_code=item.http_status,
        content_type=item.content_type,
        etag=etag,
        last_modified=last_modified,
        staged_path=None if buffered else str(retained.path),
        head=None if buffered else retained.head,
    )


def terminal_failure_kind(item: CrawlSessionItem | None) -> FailureKind | None:
    """Терминальный класс ``FAILED``-элемента (``not_found``/``payload_too_large``/
    ``unsupported_binary``) либо ``None``.

    Это не транзиентные сбои: origin уже дал определённый ответ/исполнение упёрлось в
    осознанную границу (потолок байтов, бинарный не-документ). Такой элемент **не**
    пере-фетчивается при resume и **не** превращается в ``attempts_exhausted``.

    Терминальность — только для ``FAILED``: ``FETCHED`` с этим классом невозможен, а
    ``pending``/``running`` не несут окончательного ответа.
    """
    if item is None or item.status is not CrawlItemStatus.FAILED or not item.error_kind:
        return None
    try:
        kind = FailureKind(item.error_kind)
    except ValueError:
        return None
    return kind if kind in _TERMINAL_FAILURE_KINDS else None


def _terminal_not_found(item: CrawlSessionItem | None) -> bool:
    """Совместимость: терминален ли элемент определённым ответом «ресурса нет»."""
    return terminal_failure_kind(item) is FailureKind.NOT_FOUND


@dataclass
class ProductIngest:
    """Промежуточный результат Pass A (ingest) по одному продукту.

    Копит ``docs`` (retention + провенанс экстракции) и ``chunks`` — но **только до
    границы продукта**. Там чанки персистятся (Pass B, «сначала диск, потом БД»),
    документы уже персистентны (``corpus_docs`` пишется инкрементально), и обе коллекции
    освобождаются: пик памяти фазы должен определяться **одним** продуктом, а не их суммой.

    После освобождения остаётся сводка — ``chunk_count``/``distinct_docs``. Финальному
    циклу манифеста этого достаточно: ``partition_build_id``/``content_digest``/
    ``extractor_version`` лежат в ``product_plans``, а сами данные — в БД.

    Граница честности: копление — **память, а не durability**. Персистенция на границе
    продукта переживает падение; освобождение ниже её не заменяет, а снимает удержание.
    """

    product_id: str
    docs: list[CorpusDoc] = field(default_factory=list)
    chunks: list[CorpusChunk] = field(default_factory=list)
    #: Нормализованные ключи URL, которые **этот прогон наблюдал** (живой ответ
    #: ``200``/``304``), включая те, что схлопнулись по ``doc_hash`` и потому не дошли
    #: до ``docs``. Носитель нужен переиспользованию запрещённого корня (E.6): строка
    #: корпуса адресуется **URL**, и переиспользованная копия не имеет права встать на
    #: место наблюдения только потому, что наблюдение оказалось алиасом близнеца
    #: (правило 2 оркестратора, 05.11.2026). ``reused`` (retained-байты) сюда не входит:
    #: сети не было — наблюдения нет.
    observed_urls: set[str] = field(default_factory=set)
    #: Сколько чанков продукта записано (нужно финальному циклу манифеста для ``stats_json``).
    chunk_count: int = 0
    #: Сколько **разных** документов эти чанки покрывают (``stats_json.docs``). Документ
    #: без чанков сюда не входит — это и есть прежняя семантика поля.
    distinct_docs: int = 0


@dataclass
class LocalFetch:
    """D4: дескриптор отложенного сетевого фетча локального пути.

    Расщепляет ``process_local`` на три фазы: **pre-fetch** (главный поток — resume,
    browser-guard, решение о пейсинге/ёмкости), **fetch** (пул — только
    ``limiter.wait`` + ``active.fetch``) и **post-fetch** (главный поток — redirect-scope
    и retention). Дескриптор несёт всё нужное пост-фазе и не содержит мутаций.

    ``resume``/``browser_blocked`` вычисляются пре-фазой (чисто), а коммитятся
    (соответственно ``resume_page`` или ``FAILED``-``mark``) пост-фазой в FIFO-порядке
    отправки (детерминизм победителя дедупа и ``errors``).

    ``poolable`` — идёт ли живой фетч в пул: ``True`` только для не-браузерного живого
    фетча (resume/browser-guard/терминальный ``not_found``/``FetchMode.BROWSER`` — всегда
    inline на главном потоке).

    ``terminal`` — элемент уже получил определённый ответ «ресурса нет» (``not_found``):
    живой фетч не выполняется, пост-фаза лишь подтверждает терминальный исход.
    """

    pid: str
    wu: str
    depth: int | None
    fetch_plan: FetchPlan
    active: Any
    engine_value: str | None
    prior_item: CrawlSessionItem | None
    resume: RetainedRaw | None
    browser_blocked: bool
    poolable: bool
    host: str
    scope_host: str | None
    enforce_redirect_scope: bool
    terminal: bool = False
    #: P0: retained raw отклонён по размеру/бинарности — терминальный исход без экстракции.
    reject: RetainedReject | None = None
    #: P6: класс-aware потолок артефакта-контейнера и заявленный набор форматов (нужны
    #: пост-фазе для допуска/размера; ``None``/пусто — наследовать поведение страницы).
    max_artifact_bytes: int | None = None
    attachment_formats: tuple[str, ...] | None = None
    #: 17 §6 (F6): замер попытки — момент начала живого фетча и его длительность. Пишет
    #: fetch-фаза (``local_fetch``/``fetch_and_commit``), читает коммит: интервал между
    #: запросами одного host'а из длительностей не восстанавливается (§7.2 №1), поэтому
    #: момент начала — отдельная величина. ``None`` — живого фетча не было (resume/reject/
    #: терминальный класс): «не наблюдалось», а не «ноль».
    fetch_started_at: str | None = None
    fetch_ms: int | None = None
    #: Снятие корня, в чьём обходе получен этот URL (план 20 §3). Нужен только писателю
    #: ``url_state``: наблюдение артефакта обязано называть снятие, а вычислить его из
    #: URL нельзя (один URL может принадлежать разным корням разных прогонов).
    #: ``None`` — прогона нет либо корень не снимается (synthetic-бакет).
    capture_id: str | None = None


@dataclass(frozen=True)
class SitemapSeeds:
    """D3: развёртка sitemap корня — свежие ``<loc>``-seeds + отвергнутые scope.

    :param fresh: ``<loc>`` в scope корня, ещё не прикреплённые ни к одному корню продукта.
    :param skipped: агрегированный **провенанс** отказов scope: не более одной записи на
        класс (``host``/``path``) на корень — ``{"reason", "count", "examples", "message"}``.
        Отказ scope — не ``errors`` (F10): host-wide sitemap при path-scoped seed отвергает
        большинство ``<loc>`` штатно, и канал сбоев не должен выглядеть деградировавшим.
    """

    fresh: tuple[str, ...] = ()
    skipped: tuple[dict[str, Any], ...] = ()


# --------------------------------------------------------------------------- #
# Разбор одного URL: переиспользование версии, воронка исходов, retention
# --------------------------------------------------------------------------- #


def validators(
    pid: str,
    url: str,
    *,
    state: ProductStatePort | None,
    policy: RefreshPolicy,
    planned_validators: Mapping[tuple[str, str], tuple[str | None, str | None]],
    errors: list[str],
) -> tuple[str | None, str | None]:
    """Валидаторы условного GET для URL (09 §6.3): ``(etag, last_modified)``.

    Источник — ``url_state`` (durable), а не сеанс краула: валидаторы переживают
    прогоны. Отправляются только при политике ``auto``/``revalidate``: ``refresh``
    обязан получить свежий ответ (безусловный GET), ``offline`` сеть не трогает.
    Плановые значения (из ``plan_incremental``) приоритетнее повторного чтения строки —
    это тот же источник, прочитанный один раз на продукт.

    Сбой чтения состояния изолируется в ``errors``: отсутствие валидатора деградирует
    запрос до безусловного GET (безопасно), а не роняет фазу.
    """
    if state is None:
        return None, None
    # План 19 §P3: отправка валидаторов — факт таблицы исполнения, а не набор
    # `policy in (...)`-условий: `refresh` не отправляет (нужен свежий ответ),
    # `offline`/`frozen` не отправляют (сети нет).
    if not dispatch_refresh(policy).validators:
        return None, None
    key = _state_url_key(url)
    if key is None:
        return None, None
    planned = planned_validators.get((pid, key))
    if planned is not None:
        return planned
    try:
        row = state.get_url_state(pid, key)
    except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
        errors.append(
            f"state: get_url_state({pid}/{key}): {type(exc).__name__}: {exc}"
        )
        return None, None
    if row is None:
        return None, None
    return row.etag, row.last_modified


def reuse_not_modified(
    pid: str,
    url: str,
    *,
    state: ProductStatePort | None,
    errors: list[str],
    chunker_version: str,
) -> DocReuse | None:
    """Переиспользование по ``304`` — **до** экстракции (её вызов не нужен).

    ``content_hash`` документа известен из ``url_state.doc_version_id``: сервер
    подтвердил, что представление не изменилось. Нет версии/чанков — ``None``
    (вызывающий идёт обычным путём экстракции).
    """
    if state is None:
        return None
    key = _state_url_key(url)
    if key is None:
        return None
    try:
        prior = state.get_url_state(pid, key)
        prior_doc = (
            state.get_document_version(prior.doc_version_id)
            if prior is not None and prior.doc_version_id
            else None
        )
    except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
        errors.append(
            f"{pid}/{url}: state: reuse(304): {type(exc).__name__}: {exc}"
        )
        return None
    if classify_reuse(
        prior=prior, prior_doc=prior_doc, not_modified=True, content_hash=None
    ) is not ReuseKind.REVALIDATED:
        return None
    return reuse_plan(
        pid,
        url,
        prior_doc,
        state=state,
        errors=errors,
        chunker_version=chunker_version,
    )


def reuse_for_page(
    pid: str,
    url: str,
    page: RawPage,
    *,
    state: ProductStatePort | None,
    errors: list[str],
    chunker_version: str,
) -> DocReuse | None:
    """Переиспользование по ``304`` с видимой записью, если версии нет.

    ``304`` без известной версии (или с отвергнутым chunk-set) — не повод молчать:
    сервер подтвердил «не изменилось», а переиспользовать нечего, поэтому прогон
    идёт обычной экстракцией, а факт фиксируется в ``errors``. Конкретная причина
    отказа уже записана :func:`reuse_not_modified` — дублирующая запись не нужна.
    """
    if not page.not_modified:
        return None
    before = len(errors)
    reuse = reuse_not_modified(
            pid, url, state=state, errors=errors, chunker_version=chunker_version
        )
    if reuse is None and len(errors) == before:
        errors.append(
            f"{pid}/{url}: 304 без переиспользуемой версии документа — "
            "обычная экстракция"
        )
    return reuse


def reuse_same_hash(
    pid: str,
    url: str,
    doc_hash: str,
    extractor_token: str,
    level_text: str,
    *,
    state: ProductStatePort | None,
    errors: list[str],
    chunker_version: str,
) -> DocReuse | None:
    """Переиспользование при ``200`` с прежним ``content_hash`` (после экстракции)."""
    if state is None:
        return None
    key = _state_url_key(url)
    if key is None:
        return None
    try:
        prior = state.get_url_state(pid, key)
    except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
        errors.append(
            f"{pid}/{url}: state: get_url_state: {type(exc).__name__}: {exc}"
        )
        return None
    prior_doc = find_doc_version(
        pid, doc_hash, extractor_token, level_text, state=state, errors=errors
    )
    if classify_reuse(
        prior=prior, prior_doc=prior_doc, not_modified=False, content_hash=doc_hash
    ) is not ReuseKind.REVALIDATED:
        return None
    return reuse_plan(
        pid,
        url,
        prior_doc,
        state=state,
        errors=errors,
        chunker_version=chunker_version,
    )


#: Виды наблюдения снятия корня (``capture.observed_how``, план 20 §3). CHECK живёт в
#: DDL ``capture``; словарь статусов значения не дублирует (это провенанс, не статус).
CAPTURE_OBSERVED_LIVE = "live"
CAPTURE_OBSERVED_REVALIDATED = "revalidated"
CAPTURE_OBSERVED_ADOPTED = "adopted"


def capture_id_for(product_id: str, root_url: str, run_id: str | None) -> str:
    """Детерминированный ``capture_id`` снятия: ``(product_id, root_url, run_id)``.

    Детерминизм — требование, а не удобство: повторный вход в фазу (resume/``--reset``)
    обязан обновить то же снятие, а не создать второе наблюдение того же обхода. Формула
    не входит в ``run_id``: снятие — провенанс прогона, а не его объявленный вход.
    """
    return short_hash(
        "\x1f".join((str(product_id), str(root_url), str(run_id or ""))), 24
    )


def record_url_state(
    pid: str,
    url: str,
    status: CrawlItemStatus,
    fields: Mapping[str, Any],
    extra: Mapping[str, Any] | None,
    *,
    state: ProductStatePort | None,
    errors: list[str],
    observation: Mapping[str, Any] | None = None,
) -> None:
    """Пишет ``url_state`` по исходу одного URL (09 §6.3) + наблюдение артефакта.

    Вызывается из :func:`mark` — единственной воронки терминальных исходов, поэтому
    покрывает **каждый** URL независимо от режима продукта (субстрат наполняется уже
    на первом прогоне). Правила:

    * ``FETCHED`` + новый/изменившийся документ → новый ``doc_version_id``,
      ``reuse_kind=live``, счётчик сбоев сброшен;
    * ``FETCHED`` + ``304``/тот же ``content_hash`` → ``doc_version_id`` сохраняется,
      ``reuse_kind=revalidated``;
    * ``not_found`` → ``doc_version_id=NULL``, ``last_error_kind=not_found``, прежняя
      версия документа — ``superseded``;
    * прочие сбои (``robots_disallow``/``parser_failed``/``extraction_timeout``/
      ``attempts_exhausted``/…) → ``last_error_kind``, ``consecutive_failures += 1``,
      прежняя версия сохраняется.

    ``observation`` (план 20 §3) — факт наблюдения **версии-артефакта**
    (``observed_at``/``observed_how``/``capture_id``/``observed_actor``). Он пишется
    только для ``FETCHED``: отказ не является наблюдённой версией, и его
    ``last_checked_at``/``consecutive_failures`` пишутся как прежде. ``None`` в любой из
    четырёх колонок означает «не передано» и сохраняет прежнее значение (COALESCE в
    upsert): ``frozen``/``offline``/``stale`` не обнуляют наблюдение живого прогона.

    ``FETCHED`` без ``raw_path`` (каталог/sitemap — не контент, I19) в состояние не
    попадает: иначе следующий инкрементальный план принял бы их за документы.
    Любой сбой записи изолируется в ``errors`` (никогда не роняет фазу).
    """
    if state is None:
        return
    if status is CrawlItemStatus.FETCHED and not fields.get("raw_path"):
        return
    key = _state_url_key(url)
    if key is None:
        errors.append(
            f"{pid}/{url}: state: URL не нормализуется — url_state не записан"
        )
        return
    try:
        prior = state.get_url_state(pid, key)
        base = (
            prior
            if prior is not None
            else UrlStateRow(product_id=pid, url=url, normalized_url=key)
        )
        now = _now_iso()
        current: dict[str, Any] = {
            "doc_version_id": base.doc_version_id,
            "reuse_kind": base.reuse_kind,
            "etag": base.etag,
            "last_modified": base.last_modified,
            "last_http_status": base.last_http_status,
            "last_changed_at": base.last_changed_at,
        }
        # Не-``None`` значения исхода перекрывают прежние (COALESCE-семантика):
        # сервер, переставший отдавать ETag, не обязан стирать валидатор.
        for name, value in (extra or {}).items():
            if value is not None:
                current[name] = value
        failures = base.consecutive_failures
        error_kind = base.last_error_kind
        if status is CrawlItemStatus.FETCHED:
            failures = 0
            error_kind = None
        else:
            error_kind = canonical_failure_kind(fields.get("error_kind"))
            if error_kind == FailureKind.NOT_FOUND.value:
                supersede_document(
                    pid, url, base.doc_version_id, state=state, errors=errors
                )
                current["doc_version_id"] = None
            else:
                failures += 1
        # Наблюдение — только состоявшееся получение версии. Наблюдение прежней версии
        # отказом не стирается: ``None`` в колонке означает «писатель не передал», и
        # upsert сохраняет прежнее значение (COALESCE).
        received = observation if status is CrawlItemStatus.FETCHED else None
        state.upsert_url_state(
            UrlStateRow(
                product_id=pid,
                url=url,
                normalized_url=key,
                doc_version_id=current["doc_version_id"],
                etag=current["etag"],
                last_modified=current["last_modified"],
                last_http_status=current["last_http_status"],
                last_checked_at=now,
                last_changed_at=current["last_changed_at"],
                next_check_at=base.next_check_at,
                consecutive_failures=failures,
                last_error_kind=error_kind,
                reuse_kind=current["reuse_kind"],
                seed_id=base.seed_id,
                observed_at=None if received is None else received.get("observed_at"),
                observed_how=(
                    None if received is None else received.get("observed_how")
                ),
                capture_id=None if received is None else received.get("capture_id"),
                observed_actor=(
                    None if received is None else received.get("observed_actor")
                ),
            )
        )
    except Exception as exc:  # noqa: BLE001 — изоляция state-записи
        errors.append(
            f"state: url_state({pid}/{url}): {type(exc).__name__}: {exc}"
        )


def mark(
    pid: str,
    u: str,
    status: CrawlItemStatus,
    *,
    session: str | None,
    store: Any,
    items: dict[tuple[str, str], CrawlSessionItem],
    targets: dict[tuple[str, str], Any],
    applied_engines: dict[tuple[str, str], str | None],
    errors: list[str],
    heartbeat: Heartbeat,
    state: ProductStatePort | None,
    **fields: Any,
) -> None:
    """Помечает исход URL: сеанс краула (L1a) + durable ``url_state`` (09 §6.3).

    Сеанс — прежнее поведение (:func:`mark_session`); ``url_state`` пишется при
    наличии ``state`` для **каждого** исхода URL. ``state_fields`` — поля исхода
    документа (``doc_version_id``/``reuse_kind``/валидаторы/``http_status``), которые
    нельзя пронести через ``CrawlSessionItem`` (у него нет таких колонок).
    """
    state_fields = fields.pop("state_fields", None)
    # Наблюдение артефакта (план 20 §3) — не поля элемента сеанса: вынимаются здесь,
    # иначе ``mark_session`` получил бы неизвестные колонки.
    observation = {
        "observed_at": fields.pop("observed_at", None),
        "observed_how": fields.pop("observed_how", None),
        "capture_id": fields.pop("capture_id", None),
        "observed_actor": fields.pop("observed_actor", None),
    }
    mark_session(
        pid,
        u,
        status,
        session=session,
        store=store,
        items=items,
        targets=targets,
        applied_engines=applied_engines,
        errors=errors,
        heartbeat=heartbeat,
        **fields,
    )
    record_url_state(
        pid,
        u,
        status,
        fields,
        state_fields,
        state=state,
        errors=errors,
        observation=observation,
    )


def retain(
    product_id: str,
    url: str,
    page: RawPage,
    fetch_plan: FetchPlan,
    *,
    mark: Callable[..., None],
    errors: list[str],
    raw_root_path: Path,
    depth: int | None = None,
    origin: str = ORIGIN_CRAWL,
    fetch_started_at: str | None = None,
    fetch_ms: int | None = None,
    capture_id: str | None = None,
) -> Path | None:
    """Retention сырых байтов (I19) + durable-элемент ``FETCHED``; ``None`` при сбое.

    Байты пишутся в ``raw_root/{product_id}/{raw_hash}.bin`` **до** экстракции; при
    удаче элемент сеанса фиксируется ``FETCHED`` (provenance фетча). Сбой сохранения =
    недоступность источника по этому URL: строка помечается ``FAILED``
    (``source_unavailable``), чтобы очередь/счётчики были правдивы; возвращается ``None``.
    Общий шаг локального и executor путей (без дублирования).

    **Файловый staging (P6).** ``page.staged_path`` — тело уже лежит файлом у
    адаптера: хеш считается **потоково** по файлу (I19: адаптерному хешу не доверяем —
    его и нет), публикация — :func:`_publish_staged` (переезд в том же ФС без копии и
    без чтения в RAM). ``staged_path is None`` — прежний путь ``write_bytes``
    байт-в-байт.

    **Ручной артефакт (§3.3).** ``origin != 'crawl'`` — байты предъявил человек: цепочка
    та же (хеш считается здесь же, публикация та же), но элемент сеанса **не**
    перезаписывается. Отказ краула (``failed`` + ``robots_disallow``) остаётся фактом
    аудита; наблюдённость материала видна отдельно — ``corpus_docs.origin='manual'``.
    Поэтому ``mark`` здесь не вызывается ни при успехе, ни при сбое: иначе карта
    выглядела бы так, будто сайт прочитан.
    """
    staged = Path(page.staged_path) if page.staged_path else None
    manual = origin != ORIGIN_CRAWL
    raw_path: Path | None = None
    try:
        raw_hash = _hash_file(staged) if staged is not None else content_hash(page.content)
        raw_path = raw_root_path / product_id / f"{raw_hash}.bin"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if staged is not None:
            _publish_staged(staged, raw_path)
        else:
            raw_path.write_bytes(page.content)
    except Exception as exc:  # noqa: BLE001
        if staged is not None:
            # Сбой удаления staging-файла не меняет исход: retention уже провален и
            # его причина ниже попадёт в ``errors`` (и в элемент сеанса). Осиротевший
            # staging-файл — цена меньше, чем подмена причины отказа.
            with contextlib.suppress(OSError):
                staged.unlink(missing_ok=True)
        reason = f"{type(exc).__name__}: {exc}"
        errors.append(f"{product_id}/{url}: retention: {reason}")
        if manual:
            return None
        # L1a: сбой сохранения байтов = недоступность источника по этому URL;
        # помечаем FAILED, чтобы очередь/счётчики были правдивы (не ``pending``).
        mark(
            product_id,
            url,
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
            error=f"retention: {reason}",
            depth=depth,
        )
        return None
    if manual:
        return raw_path
    # L1a: байты сохранены — durable-элемент сеанса (provenance фетча).
    mark(
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
        # F5 (17 §4.1): валидаторы условного GET — провенанс **получения**, поэтому
        # пишутся здесь, а не в конверсии: строка ``extract_queue`` их не несёт (у неё
        # нет таких колонок), а потребитель собирает ``RawPage`` из строки. Оставить их
        # только в ``finish_one`` значило бы потерять ``etag``/``last_modified`` на каждом
        # документе — и следующий инкрементальный план пошёл бы без условного GET, то есть
        # без 304 (``09`` §6.3), молча.
        # 17 §6 (F6): замер попытки — часть факта получения, а не отдельная запись.
        # ``None`` — живого фетча не было (resume использовать не может: он сюда не
        # доходит), значит это «не наблюдалось по построению».
        fetch_started_at=fetch_started_at,
        fetch_ms=fetch_ms,
        # План 20 §3: байты добыты **сейчас** — это и есть наблюдение артефакта.
        # ``304`` — сервер подтвердил версию (момент подтверждения — сейчас); живой
        # ответ — момент ответа сервера (``page.fetched_at``), а не момент записи.
        # ``resume`` сюда не доходит (retained-байты коммитятся до retention) и
        # наблюдения не создаёт.
        observed_at=_now_iso() if page.not_modified else page.fetched_at,
        observed_how=(
            CAPTURE_OBSERVED_REVALIDATED
            if page.not_modified
            else CAPTURE_OBSERVED_LIVE
        ),
        capture_id=capture_id,
        state_fields={
            "etag": page.etag,
            "last_modified": page.last_modified,
        },
    )
    return raw_path


def ingest_one(
    product_id: str,
    url: str,
    page: RawPage,
    fetch_plan: FetchPlan,
    raw_path: Path,
    ingest: ProductIngest,
    seen_docs: dict[str, str],
    seen_chunk_ids: set[str],
    extractor_obj: ExtractorPort,
    level: DocumentLevel,
    *,
    finish_one: Callable[..., str | None],
    extract_cache: ExtractionCache | None,
    reuse_kind: ReuseKind,
    origin: str = ORIGIN_CRAWL,
) -> str | None:
    """Inline-экстракция → доводка одного URL (путь без пула, ``None``/``1``).

    Тело экстракции (``extractor_obj.extract``) вынесено в :func:`finish_one`; здесь —
    только inline-вызов и перевод исключения в ту же ошибку ``parser_failed``, что и
    пул-путь (P4: единая семантика отказа). ``extractor_obj`` — экстрактор уровня
    корня (P5; ``full`` переиспользует переданный порт). ``level`` — уровень корня:
    нужен как ключ кэша экстракции (Ф1.2) и не выводится из ``extractor_obj``, потому
    что ``full`` переиспользует переданный порт.

    :returns: ``doc_id`` созданного/найденного документа корпуса либо ``None`` — исход
        не дал документа (сбой экстракции/нарезки). Нужен ручной подаче
        (13-manual-artifacts §3.3): только дошедший до корпуса артефакт закрывает
        пробел покрытия, а не «объявленный файл».
    :param reuse_kind: происхождение наблюдения этого URL **в этом прогоне**
        (16 §3.3) — вычислено вызывающим по пути страницы (``resume_page`` ⇒
        ``reused``, 304 ⇒ ``revalidated``, живой fetch ⇒ ``live``), а не здесь:
        у ``finish_one`` нет разницы «resume vs живой fetch», а угадывать её из
        ``page`` нельзя (у resume ``not_modified`` всегда ``False``).
    """
    try:
        level_text = _as_text(level)
        # I19: ключ — хеш тех же байтов, что retention положил в ``raw_path``.
        # ``raw_path.stem`` и есть этот хеш (``retain`` называет файл ``{hash}.bin``),
        # поэтому для страницы ключ байт-идентичен ``content_hash(page.content)``, а для
        # staging-артефакта (``page.content`` пуст) ключ не вырождается в хеш пустоты.
        cache_key = raw_path.stem
        probe_page = _page_with_source_path(page, raw_path)
        extracted = (
            None
            if extract_cache is None
            else extract_cache.get(
                cache_key,
                _cache_extractor_version(extractor_obj, probe_page, fetch_plan.extract),
                level_text,
            )
        )
        if extracted is None:
            extracted = extractor_obj.extract(probe_page, fetch_plan.extract)
            if extract_cache is not None:
                extract_cache.put(
                    extracted, level=level_text, content_hash=cache_key
                )
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        finish_one(
            product_id,
            url,
            page,
            fetch_plan,
            raw_path,
            None,
            ingest,
            seen_docs,
            seen_chunk_ids,
            extractor_obj,
            level,
            extract_error=f"{type(exc).__name__}: {exc}",
            reuse_kind=reuse_kind,
            origin=origin,
        )
        return None
    return finish_one(
        product_id,
        url,
        page,
        fetch_plan,
        raw_path,
        extracted,
        ingest,
        seen_docs,
        seen_chunk_ids,
        extractor_obj,
        level,
        reuse_kind=reuse_kind,
        origin=origin,
    )


# --------------------------------------------------------------------------- #
# L5 шаг 3: доводка одного документа (было замыканием ``_build_corpus``)
# --------------------------------------------------------------------------- #


#: Причина частичности, когда экстрактор её не назвал. Не выдумка причины, а **факт её
#: отсутствия**: частичный документ обязан остаться в корпусе (текст реален) и обязан
#: быть помечен, поэтому «причина не сообщена» честнее и тихого ``partial=False``, и
#: падения на инварианте ``CorpusDoc``, и придуманного объяснения.
PARTIAL_REASON_UNSTATED = "извлечение частичное: причина экстрактором не сообщена"


def _partial_fields(extracted: Any) -> dict[str, Any]:
    """Поля частичности ``ExtractedDoc`` → ``CorpusDoc`` (общая точка обоих путей).

    Один маппинг на страницу и на члена контейнера: иначе «пул vs inline» или «страница
    vs член архива» могли бы разойтись в признаке, а это признак, ради которого всё и
    делается. ``None``/нечисло остаются ``None`` — «не измерено» не превращается в ноль.
    """
    partial = bool(getattr(extracted, "partial", False))
    reason = str(getattr(extracted, "partial_reason", None) or "").strip() or None
    page_count = getattr(extracted, "page_count", None)
    pages_converted = getattr(extracted, "pages_converted", None)
    text = getattr(extracted, "text", None)
    return {
        "partial": partial,
        "partial_reason": (reason or PARTIAL_REASON_UNSTATED) if partial else None,
        "page_count": (
            page_count
            if isinstance(page_count, int) and not isinstance(page_count, bool)
            else None
        ),
        "pages_converted": (
            pages_converted
            if isinstance(pages_converted, int) and not isinstance(pages_converted, bool)
            else None
        ),
        "extracted_chars": len(text) if isinstance(text, str) else None,
    }


def _reused_partial_fields(doc_version: Any) -> tuple[dict[str, Any], bool]:
    """Поля частичности **из durable-записи** версии: ``(поля CorpusDoc, признак известен)``.

    Переиспользование версии (``304``: экстракции не было) не вправе ответить на вопрос
    «прочитан документ целиком?» из самого факта переиспользования. До v5 ответа не было
    вовсе, и ``partial=False`` означал «в этом прогоне не наблюдался» — то есть усечённый
    текст молча объявлялся полным, а ``CorpusCoverage`` объявляло покрытие полным на
    нём (I2/I2c). Теперь признак лежит в ``document_version`` рядом с версией, к которой
    он относится, и переиспользование его **восстанавливает**.

    :returns: ``(поля, известен)``. ``известен=False`` — строка записана версией, которая
        признак не сохраняла (``extract_partial IS NULL``). Тогда поля не утверждают
        ничего (``partial=False`` — вынужденное булево модели, а не измерение), и
        вызывающий обязан **сказать**, что признак неизвестен, а не выдать это за
        «полное».
    """
    raw = getattr(doc_version, "extract_partial", None)
    if raw is None:
        return (
            {
                "partial": False,
                "partial_reason": None,
                "page_count": None,
                "pages_converted": None,
                "extracted_chars": None,
            },
            False,
        )
    partial = bool(raw)
    reason = (
        str(getattr(doc_version, "extract_partial_reason", None) or "").strip() or None
    )
    page_count = getattr(doc_version, "extract_page_count", None)
    pages_converted = getattr(doc_version, "extract_pages_converted", None)
    return (
        {
            "partial": partial,
            # Частичный документ обязан назвать причину (инвариант ``CorpusDoc``); причина
            # из записи — это причина, которую **сообщил прибор**, а не пересказ.
            "partial_reason": (reason or PARTIAL_REASON_UNSTATED) if partial else None,
            "page_count": (
                page_count
                if isinstance(page_count, int) and not isinstance(page_count, bool)
                else None
            ),
            "pages_converted": (
                pages_converted
                if isinstance(pages_converted, int)
                and not isinstance(pages_converted, bool)
                and partial
                else None
            ),
            "extracted_chars": None,
        },
        True,
    )


def persist_docs(
    store: Any,
    docs: list[CorpusDoc],
    errors: list[str],
    *,
    origin: str = ORIGIN_CRAWL,
    versions_by_doc: Mapping[str, str] | None = None,
) -> None:
    """Пишет ``corpus_docs`` (retention + provenance экстракции, §3.2).

    **Единственная** точка записи ``corpus_docs``. В Pass A вызывается инкрементально —
    сразу после ``ingest.docs.append`` для каждого полностью сформированного документа
    (см. ``_finish_one``), поэтому при жёстком убийстве теряется максимум документ
    «в полёте». Retention-провенанс не зависит от иммутабельности индексной сборки (I19),
    поэтому запись не откладывается до Pass B.

    Каждая запись обёрнута в try/except: сбой одного ``upsert_corpus_doc`` фиксируется в
    ``errors`` и не прерывает ни обработку остальных документов, ни краул (контракт
    изоляции фаз, ``runner.py``).

    ``origin`` (13-manual-artifacts §3.3) — происхождение **байтов**: ``crawl`` (дефолт,
    вызов байт-в-байт прежний — лишний kwarg не передаётся) либо ``manual``. Отдельный
    аргумент, а не поле ``CorpusDoc``: доменная модель не получает смысла, которого у неё
    нет, а ``corpus_docs.origin`` — провенанс записи байтов, а не документа.

    ``versions_by_doc`` (16 §6) — ``doc_id → doc_version_id``: адрес версии, которую несёт
    ``chunk_id``. Тоже отдельный аргумент и по той же причине: версия — не свойство строки
    корпуса (их у документа бывает несколько — ``dv`` включает уровень извлечения), а
    производное отображение ``corpus_doc_version``, которое пишет хранилище.
    """
    if store is None:
        return
    for doc in docs:
        version = None if versions_by_doc is None else versions_by_doc.get(doc.doc_id)
        try:
            if origin == ORIGIN_CRAWL:
                store.upsert_corpus_doc(doc, doc_version_id=version)
            else:
                store.upsert_corpus_doc(doc, origin=origin, doc_version_id=version)
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(f"upsert_corpus_doc({doc.doc_id}): {type(exc).__name__}: {exc}")


def _accept_chunk_candidates(
    product_id: str,
    document_id: str,
    dv_id: str,
    candidates: Iterable[tuple[int, str, str, str, str, int | None, int | None]],
    *,
    ingest: ProductIngest,
    seen_chunk_ids: set[str],
    errors: list[str],
    url: str,
    lang: str | None,
    embed_model_id: str,
) -> list[tuple[int, str, str, str, int | None, int | None]]:
    """Допускает чанк-кандидаты в корпус — **единственная** точка I19-гейта чанка.

    Кандидат приходит ``(chunk_no, text, heading_path, context_prefix, provided_hash,
    char_start, char_end)``: так выглядит и результат чанкера, и переиспользованный
    chunk-set. Один гейт на оба пути — иначе «нарезка» и «переиспользование» разошлись бы
    в проверках, а переиспользование как раз и есть место, где текст не пере-наблюдается.

    Смещения проходят насквозь: гейт их не считает и не выводит из текста — он лишь
    обязан не терять провенанс, объявленный чанкером (``None``/``None`` у абзацного
    чанкера — законное значение, а не «нули»).

    Отклонение видно (``errors``) и не тихо: чанк без ``content_hash``, чанк с пустым
    текстом и дублирующийся ``chunk_id`` не попадают в корпус, но и не исчезают молча.

    :returns: принятые ``(chunk_no, text, heading_path, context_prefix, char_start,
        char_end)`` — вход durable-записи ``chunk_meta``/``chunk_store`` (09 §4.2/§9).
    """
    accepted: list[tuple[int, str, str, str, int | None, int | None]] = []
    for (
        chunk_no,
        text,
        heading_path,
        context_prefix,
        provided_hash,
        char_start,
        char_end,
    ) in candidates:
        # I19: у каждого индексируемого чанка обязан быть непустой hash.
        if not provided_hash:
            errors.append(f"I19: {document_id}: чанк без content_hash отклонён")
            continue
        if not text.strip():
            errors.append(f"I19: {document_id}: чанк с пустым текстом отклонён")
            continue

        cid = _chunk_id(product_id, dv_id, chunk_no)
        if cid in seen_chunk_ids:
            errors.append(f"{document_id}: дублирующийся chunk_id {cid!r} отклонён")
            continue
        seen_chunk_ids.add(cid)

        ingest.chunks.append(
            CorpusChunk(
                chunk_id=cid,
                doc_id=document_id,
                product_id=product_id,
                url=url,
                heading_path=heading_path,
                lang=lang,
                chunk_no=chunk_no,
                text=text,
                context_prefix=context_prefix,
                # L1b-2b: смещения куска — провенанс из чанкера (или из
                # переиспользованного chunk-set), а не вычисленное здесь значение.
                char_start=char_start,
                char_end=char_end,
                # Авторитетный хеш — от чистого текста (I19/§10).
                content_hash=content_hash(text),
                # build_id проставляется в Pass B после фиксации id сборки.
                build_id=None,
                embed_model_id=embed_model_id or None,
                embedded_at=None,
            )
        )
        accepted.append(
            (chunk_no, text, heading_path, context_prefix, char_start, char_end)
        )
    return accepted


def finish_one(
    product_id: str,
    url: str,
    page: RawPage,
    fetch_plan: FetchPlan,
    raw_path: Path,
    extracted: Any,
    ingest: ProductIngest,
    seen_docs: dict[str, str],
    seen_chunk_ids: set[str],
    extractor_obj: ExtractorPort,
    level: DocumentLevel,
    *,
    mark_item: Callable[..., None],
    errors: list[str],
    store: Any,
    chunker: ChunkerPort,
    timings_for: Callable[[str], _CorpusStageTimings],
    state: ProductStatePort | None,
    duplicates: dict[str, int],
    doc_counters: dict[str, dict[str, int]],
    partial_unknown_products: dict[str, list[str]],
    partiality_changes: list[dict[str, Any]],
    reuse_same_hash: Callable[..., DocReuse | None],
    persist_docs: Callable[..., None],
    chunker_version: str,
    run_id: str | None,
    embed_model_id: str,
    reuse_kind: ReuseKind,
    extract_error: str | None = None,
    extract_failure_kind: FailureKind | None = None,
    reuse: DocReuse | None = None,
    origin: str = ORIGIN_CRAWL,
) -> str | None:
    """Доводка одного URL после экстракции: дедуп → нарезка → ``mark_item`` (главный поток).

    P4: граница расщепления проходит **механически** по ``extracted`` — результат
    экстракции приходит либо из inline-вызова, либо из ``future.result()`` пула. Сырые
    байты уже сохранены (``raw_path``); шаги изолируют ошибки в ``errors`` и фиксируют
    терминальный статус элемента сеанса через ``mark_item`` (FIFO на главном потоке).

    **Переиспользование (09 §6.3/§9).** ``reuse`` — готовая версия документа (путь
    ``304``, экстракция не вызывалась); ``None`` — обычная экстракция. При ``200`` с
    прежним ``content_hash`` версия ищется здесь же (:func:`reuse_same_hash`): чанкер
    не вызывается, chunk-set берётся из ``chunk_store`` после проверки I19
    (:func:`core.pipeline.corpus_persist.load_reused_chunks`). Run-level ``chunk_id``
    адресует версию документа (``doc_version_id``, 16 §6), поэтому идентификаторы
    индекса от переиспользования не меняются.

    ``reuse_kind`` — происхождение наблюдения (16 §3.3), приходит **аргументом**:
    ``reused`` (байты из retained, ``resume_page``), ``revalidated`` (304 либо 200 с
    прежним ``content_hash``) либо ``live`` (живой fetch). Выводить его из ``page``
    здесь нельзя: у resume-страницы ``not_modified`` всегда ``False``, и «живой 200»
    стал бы ложной провенанс-записью. ``200``-совпадение хеша повышает ``live`` до
    ``revalidated`` (сервер ответил, контент тот же), но ``reused`` не понижается и не
    повышается: сети не было, сильнейшего наблюдения не появилось.

    ``extract_failure_kind`` — класс отказа извлечения: ``None`` ⇒ ``parser_failed``
    (прежнее отображение), таймаут задания пула приходит как ``extraction_timeout``.

    **Ручной артефакт (§3.3).** ``origin != 'crawl'`` — элемент сеанса не пишется:
    отказ краула (``robots_disallow``) остаётся фактом аудита, а не превращается в
    ``fetched``. Меняется только писатель ``corpus_docs`` (``origin='manual'``).
    :returns: ``doc_id``, если документ попал в корпус, иначе ``None`` — по этому
        признаку ручная подача закрывает пробел покрытия.
    """
    manual = origin != ORIGIN_CRAWL

    def mark(pid: str, u: str, status: CrawlItemStatus, **fields: Any) -> None:
        """``mark_item`` для краула; для ручного артефакта — no-op (аудит не перезаписывается)."""
        if manual:
            return
        mark_item(pid, u, status, **fields)

    if reuse is not None and extracted is not None and getattr(extracted, "error", None):
        # Экстрактор текущего прогона сообщил сбой: переиспользование скрыло бы его,
        # а сбой парсера — это сигнал о конвейере, а не повод «взять старое».
        reuse = None
    extractor_token = str(getattr(extractor_obj, "version", "") or "")
    level_text = _as_text(level)
    doc_extractor_version = extractor_token or None

    # 3. Экстракция (грациозная деградация адаптера через .error).
    if reuse is None:
        if extract_error is not None:
            errors.append(f"{product_id}/{url}: extract: {extract_error}")
            mark(
                product_id,
                url,
                CrawlItemStatus.FAILED,
                error_kind=(
                    extract_failure_kind or FailureKind.PARSER_FAILED
                ).value,
                error=extract_error,
                raw_path=str(raw_path),
            )
            return None
        if extracted is None or getattr(extracted, "error", None):
            reason = getattr(extracted, "error", None) or "пустой ExtractedDoc"
            errors.append(f"{product_id}/{url}: extract: {reason}")
            mark(
                product_id,
                url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.PARSER_FAILED.value,
                error=reason,
                raw_path=str(raw_path),
            )
            return None

        doc_hash = str(getattr(extracted, "content_hash", "") or "")
        if not doc_hash:
            errors.append(
                f"I19: {product_id}/{url}: ExtractedDoc без content_hash — отклонён"
            )
            mark(
                product_id,
                url,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.PARSER_FAILED.value,
                error="ExtractedDoc без content_hash",
                raw_path=str(raw_path),
            )
            return None
        doc_extractor_version = getattr(extracted, "extractor_version", None)
        extractor_token = str(doc_extractor_version or "") or extractor_token
    else:
        doc_hash = reuse.doc_version.content_hash
        doc_extractor_version = reuse.doc_version.extractor_version or None
        extractor_token = (
            reuse.doc_version.extractor_version or extractor_token
        )
        level_text = reuse.doc_version.level or level_text

    # 4. Дедуп по content_hash (в пределах продукта).
    document_id = f"{product_id}:{doc_hash}"
    # E.6 (правило 2): наблюдение регистрируется **до** дедупа. Алиас по ``doc_hash``
    # выходит из функции, не доходя до ``ingest.docs``, и без этой записи
    # переиспользование запрещённого корня вставило бы для того же URL строку
    # ``reused``/``http_status=NULL`` со старым носителем — ложный провенанс (наблюдение
    # было) и устаревший текст рядом с наблюдённым (I19). Запись идёт после проверки
    # хеша: документ без ``content_hash`` не наблюдался как документ, и его durable-копия
    # вправе остаться в корпусе (иначе состав корня сузился бы молча).
    if not manual and reuse_kind in (ReuseKind.LIVE, ReuseKind.REVALIDATED):
        observed_key = _state_url_key(page.url)
        if observed_key is not None:
            ingest.observed_urls.add(observed_key)
    if doc_hash in seen_docs:
        # Наблюдаемость: сколько документов схлопнулось по продукту. Класс исхода
        # не меняется (алиас получает ``FETCHED`` и версию близнеца) — это счётчик,
        # а не ошибка.
        duplicates[product_id] = duplicates.get(product_id, 0) + 1
        # Байты сохранены и контент уже проиндексирован близнецом в этой партиции.
        # Алиас получает ту же версию документа (если она уже записана), чтобы
        # следующий инкрементальный план знал, что контент URL уже в корпусе.
        found = find_doc_version(
            product_id,
            doc_hash,
            extractor_token,
            level_text,
            state=state,
            errors=errors,
        )
        mark(
            product_id,
            url,
            CrawlItemStatus.FETCHED,
            raw_path=str(raw_path),
            # 17 §6 (F3): алиас знает ключ дедупа (он пришёл с тем же ``doc_hash``) —
            # носитель канонического адреса заполняется и для него: иначе ``min(url)``
            # выбирался бы среди тех URL, что успели стать документом, а не среди всех.
            doc_hash=doc_hash,
            state_fields={
                "doc_version_id": (
                    found.doc_version_id if found is not None else None
                ),
                "etag": page.etag,
                "last_modified": page.last_modified,
                "last_http_status": page.status_code,
            },
        )
        return document_id
    seen_docs[doc_hash] = url

    # 16 §6: адрес версии документа — тот же ключ §4.2, что уходит в
    # ``document_version`` ниже (``content_hash`` + версия экстрактора + уровень).
    # Считается **до** нарезки: ``chunk_id`` адресует версию, а не голый хеш
    # (иначе ``layout``- и ``text``-документы одного текста давали один ``chunk_id``).
    dv_id = doc_version_id(product_id, doc_hash, extractor_token, level_text)

    # 5. Переиспользование версии (200 с прежним content_hash) либо чанкер.
    # Разрешается **до** создания ``CorpusDoc``: происхождение наблюдения
    # (``reuse_kind``) — поле той же строки (16 §3.3), а «200 с прежним хешем» —
    # это ``revalidated``, а не ``live`` (09 §6.3 ``classify``).
    active_reuse = reuse
    if active_reuse is None:
        active_reuse = reuse_same_hash(
            product_id, url, doc_hash, extractor_token, level_text
        )
    if active_reuse is not None and reuse_kind is ReuseKind.LIVE:
        # Сервер ответил (200), контент тот же ⇒ ревалидация. ``reused`` (retained-
        # байты без сети) не повышается: сильнейшего наблюдения не появилось.
        reuse_kind = ReuseKind.REVALIDATED

    # Частичность — **измеренное** свойство, а не вывод из текста: «прочитано 7 страниц
    # из 2763» сообщает прибор (``ExtractedDoc.partial``), а markdown-строка об этом
    # молчит. Два пути, и оба обязаны назвать одно и то же:
    # * извлечение было (inline/пул/кэш) — признак из ``ExtractedDoc``;
    # * переиспользование (``304``) — извлечения не было, поэтому признак берётся из
    #   durable-записи версии (``document_version.extract_partial``). До v5 его там не
    #   было, и усечённый текст объявлялся полным; теперь ``partial_known`` различает
    #   «полное» и «не измерено», и второе не выдаётся за первое.
    if reuse is None:
        partial_fields = _partial_fields(extracted)
        partial_known = True
    else:
        partial_fields, partial_known = _reused_partial_fields(reuse.doc_version)
        if not partial_known:
            # «Не измерено» называется явно: иначе этот документ в отчёте неотличим от
            # прочитанного целиком, а ``CorpusCoverage`` считает полноту по признаку.
            partial_unknown_products.setdefault(product_id, []).append(page.url)
    doc = CorpusDoc(
        doc_id=document_id,
        product_id=product_id,
        url=page.url,
        fetched_at=page.fetched_at,
        content_hash=doc_hash,
        raw_path=str(raw_path),
        extractor_version=doc_extractor_version,
        render_mode=_as_text(page.fetch_mode),
        lang=page.lang,
        # 16 §3.3: происхождение наблюдения **этого** прогона. ``http_status`` —
        # ответ сервера в этом цикле. У retained-байтов сервер не отвечал, поэтому
        # ``NULL``, даже если ``RawPage.status_code`` несёт прежний ответ (он нужен
        # ``url_state.last_http_status`` — факт прошлого цикла, не этого).
        http_status=(
            None if reuse_kind is ReuseKind.REUSED else page.status_code
        ),
        reuse_kind=reuse_kind,
        **partial_fields,
    )
    ingest.docs.append(doc)
    # Инкрементальная персистенция retention-провенанса: ``doc`` полностью сформирован
    # выше (``doc_id``/``content_hash``/``raw_path``/``extractor_version``/``lang``/
    # ``render_mode``), а сырые байты уже на диске (retention выполнен до экстракции).
    # Поэтому пишем строку сразу, а не в конце продукта: жёсткое убийство (OOM/SIGKILL)
    # теряет максимум документ «в полёте» (ещё извлекаемый), а не весь накопленный
    # продукт. ``persist_docs`` изолирует сбой записи в ``errors`` — краул продолжается.
    persist_docs(
        store, [doc], errors, origin=origin, versions_by_doc={document_id: dv_id}
    )

    #: ``(chunk_no, text, heading_path, context_prefix, provided_hash, char_start,
    #: char_end)`` — общая форма для чанкера и переиспользованного набора (I19-проверки
    #: ниже одинаковы). Смещения входят в кандидат, потому что это провенанс куска:
    #: без них штатный путь ингеста писал бы ``NULL`` в ``corpus_chunks.char_start``
    #: (``ingest_member_doc`` — не единственный производитель чанка).
    candidates: list[tuple[int, str, str, str, str, int | None, int | None]] = []
    if active_reuse is not None:
        candidates = [
            (
                chunk.chunk_no,
                chunk.text,
                chunk.heading_path,
                chunk.context_prefix,
                chunk.content_hash,
                chunk.char_start,
                chunk.char_end,
            )
            for chunk in active_reuse.chunks
        ]
    else:
        try:
            _chunk_started = time.perf_counter()
            try:
                chunks = chunker.chunk(extracted, document_id)
            finally:
                # ``chunk_s`` — работа главного потока; считается и при отказе нарезки.
                timings_for(product_id).chunk_s += (
                    time.perf_counter() - _chunk_started
                )
        except Exception as exc:  # noqa: BLE001
            # Сбой нарезки — не отказ источника/парсера: raw сохранён (FETCHED),
            # повторный resume сможет ре-извлечь без ре-фетча.
            errors.append(f"{product_id}/{url}: chunk: {type(exc).__name__}: {exc}")
            return None
        candidates = [
            (
                int(getattr(chunk, "chunk_no", 0)),
                getattr(chunk, "text", "") or "",
                getattr(chunk, "heading_path", "") or "",
                getattr(chunk, "context_prefix", "") or "",
                str(getattr(chunk, "content_hash", "") or "").strip(),
                # L1b-2b: смещения — как их объявил чанкер; ``None`` у абзацного
                # чанкера означает «кусок не срез», а не «начало в нуле».
                getattr(chunk, "char_start", None),
                getattr(chunk, "char_end", None),
            )
            for chunk in sorted(
                chunks or [], key=lambda c: getattr(c, "chunk_no", 0)
            )
        ]

    accepted: list[tuple[int, str, str, str, int | None, int | None]] = (
        _accept_chunk_candidates(
            product_id,
            document_id,
            dv_id,
            candidates,
            ingest=ingest,
            seen_chunk_ids=seen_chunk_ids,
            errors=errors,
            url=page.url,
            lang=page.lang,
            embed_model_id=embed_model_id,
        )
    )

    # 09 §6.3/§9: версия документа + chunk-set в durable-субстрате. Идентификатор
    # (``dv_id``) выведен выше — тем же ключом §4.2, которым адресованы ``chunk_id``.
    write_document_state(
        product_id,
        url,
        page,
        dv_id=dv_id,
        doc_hash=doc_hash,
        extractor_token=extractor_token,
        level_text=level_text,
        raw_path=raw_path,
        chunks=accepted,
        partial_fields=partial_fields,
        partial_known=partial_known,
        state=state,
        errors=errors,
        partiality_changes=partiality_changes,
        chunker_version=chunker_version,
        run_id=run_id,
    )
    reused = active_reuse is not None
    count_doc(
        product_id, reused=reused, state=state, doc_counters=doc_counters
    )

    # L1a: документ успешно добыт и нарезан → элемент сеанса терминален ``FETCHED``
    # (сеанс трекает краул: сырые байты сохранены = «ready» для материализации).
    # Для ручного артефакта ``mark`` — no-op: отказ краула остаётся в аудите (§3.3).
    mark(
        product_id,
        url,
        CrawlItemStatus.FETCHED,
        raw_path=str(raw_path),
        # 17 §6 (F3): хеш **извлечённого текста** — ключ, по которому канонический адрес
        # документа выбирается запросом (``min(url)`` среди равных), а не порядком готовности.
        doc_hash=doc_hash,
        state_fields={
            "doc_version_id": dv_id,
            "reuse_kind": (
                ReuseKind.REVALIDATED.value if reused else ReuseKind.LIVE.value
            ),
            "etag": page.etag,
            "last_modified": page.last_modified,
            "last_http_status": page.status_code,
            "last_changed_at": None if reused else _now_iso(),
        },
    )
    return document_id


def feed_manual_artifact(
    product_id: str,
    entry: ManualArtifact,
    ingest: ProductIngest,
    seen_docs: dict[str, str],
    seen_chunk_ids: set[str],
    *,
    ingest_one: Callable[..., str | None],
    retain: Callable[..., Path | None],
    limits_for: Callable[[str], CrawlLimits],
    resolve_target: Callable[[str, str], Any],
    url_map: Mapping[str, list[str]],
    domain_profiles: Mapping[str, DomainProfile] | None,
    extractor: ExtractorPort,
    extractor_factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort] | None,
    container_for: Callable[[str | Path], ContainerPort | None] | None,
    artifact_probes: list[dict[str, Any]],
    errors: list[str],
) -> tuple[str | None, Path | None]:
    """Один объявленный файл: допуск → retention → экстракция → корпус (I19).

    **Чего здесь нет.** Нет второго пути байтов: файл оператора читается и отдаётся
    ``RawPage``, дальше работает :func:`retain` — единственный writer сырых байтов,
    который сам хеширует их и кладёт в ``corpus_raw``. Файл оператора **не
    перемещается и не изменяется**: ``_publish_staged`` (переезд staging-файла) здесь
    не используется вовсе, поэтому предоставленный файл остаётся там, где его положил
    человек, и следующий прогон перечитает его же.

    **Краулер обходится, границы — нет** (§3.3): живой фетч не выполняется (robots
    снова отказал бы), но SSRF-валидация, scope объявленных хостов продукта и
    блэклист остаются обязательными, а потолок размера артефакта берётся тот же, что
    у краулера (не новый лимит, а уже объявленный ``crawl.max_artifact_bytes``).

    **Отказ не стирается.** Элемент сеанса этим путём не перезаписывается
    (``origin=ORIGIN_MANUAL`` отключает ``mark``): ``failed`` + ``robots_disallow``
    остаётся в ``crawl_session_items``, а наблюдённость материала видна отдельно —
    ``corpus_docs.origin = 'manual'``.

    :returns: ``(doc_id, raw_path)``: ``doc_id`` — документ попал в корпус (только
        такой артефакт закрывает пробел покрытия), ``raw_path`` — retained-байты
        (``None`` — байты не сохранены).
    """
    from core.pipeline.serp_loop import serp_host_in_scope

    url = entry.url
    where = f"{product_id}/{url}: manual_artifact"
    # 1. SSRF — та же единственная проверка URL, что у всего прогона. Дублируется
    #    намеренно: загрузчик проверяет декларацию, фаза — допуск байтов.
    try:
        validate_url(url)
    except ValueError as exc:
        errors.append(f"{where}: URL не проходит SSRF-проверку ({exc}) — не допущен")
        return None, None
    # 2. Scope — только объявленные хосты продукта (поддомен — если объявлен).
    own_hosts, declared_hosts = manual_host_scope(
        product_id, url_map=url_map, domain_profiles=domain_profiles
    )
    if not serp_host_in_scope(
        urlsplit(url).hostname or "", own_hosts, declared_hosts
    ):
        errors.append(
            f"{where}: хост вне объявленных хостов продукта — не допущен "
            "(предоставление не расширяет scope краула)"
        )
        return None, None
    # 3. Пределы — те же объявленные оси, что у краула (app < run < site).
    limits = limits_for(url)
    if is_blacklisted(url, limits):
        errors.append(f"{where}: URL в блэклисте — не допущен")
        return None, None
    if entry.size > limits.max_artifact_bytes:
        # Не новый потолок: ровно та граница, на которой краулер отказал бы этому же
        # файлу (``payload_too_large``). Молчаливая обрезка запрещена.
        errors.append(
            f"{where}: {entry.size} Б > crawl.max_artifact_bytes="
            f"{limits.max_artifact_bytes} — не допущен "
            f"({FailureKind.PAYLOAD_TOO_LARGE.value}, как у краула)"
        )
        return None, None
    target = resolve_target(product_id, url)
    fetch_plan = target.plan
    level = limits.document_level
    extractor_obj: ExtractorPort = extractor
    if extractor_factory is not None and (
        level is not DocumentLevel.FULL
        or limits.pdf_pipeline is not PdfPipeline.STRUCTURED
    ):
        # Зеркало пути корня (P5): уровень/механизм — свойство настройки, а не
        # «какой экстрактор случайно передали». Отказ фабрики — видимая деградация.
        try:
            extractor_obj = extractor_factory(level, limits.pdf_pipeline)
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            errors.append(
                f"{where}: extractor_factory({level.value}, "
                f"{limits.pdf_pipeline.value}): {type(exc).__name__}: {exc} — "
                "понижен до переданного экстрактора"
            )
            extractor_obj = extractor
    # 4. Байты оператора → RawPage. Файл только читается (аудит сохраняет его на
    #    месте): retained-копию пишет ``retain``, и именно её хеш станет evidence.
    try:
        body = entry.path.read_bytes()
    except OSError as exc:
        errors.append(f"{where}: объявленный файл не читается: {exc}")
        return None, None
    page = RawPage(
        url=url,
        content=body,
        fetched_at=_now_iso(),
        # Получения не было: ``render_mode='http'`` был бы ложной провенанс-записью,
        # поэтому NULL + ``origin='manual'`` (два факта вместо одного неверного).
        fetch_mode=None,
        content_type=MEMBER_CONTENT_TYPES.get(document_suffix(url) or ""),
        engine=ORIGIN_MANUAL,
    )
    raw_path = retain(product_id, url, page, fetch_plan, origin=ORIGIN_MANUAL)
    if raw_path is None:
        return None, None
    # 5. Контейнер — маршрут **спуска**, а не экстрактора; спуск живёт внутри дренажа
    #    корня (пулы/окна/провенанс). Ручная подача его не исполняет, поэтому говорит
    #    это вслух: тихая попытка «скормить zip docling-у» дала бы невнятный
    #    ``parser_failed`` вместо честного «предоставьте документы членами».
    try:
        probe = probe_artifact(
            raw_path,
            product_id=product_id,
            url=url,
            content_type=page.content_type,
            container_for=container_for,
        )
        artifact_probes.append(probe.as_dict())
        if probe.is_container:
            errors.append(
                f"{where}: контейнер ({probe.artifact.format}) — спуск для ручной "
                "подачи не подключён; предоставьте документы членами архива "
                "(спуск исполняется только краулом)"
            )
            return None, raw_path
    except Exception as exc:  # noqa: BLE001 — зонд не роняет фазу
        errors.append(
            f"{where}: зонд артефакта: {type(exc).__name__}: {exc} — "
            "артефакт подан экстрактору как есть"
        )
    doc_id = ingest_one(
        product_id,
        url,
        page,
        fetch_plan,
        raw_path,
        ingest,
        seen_docs,
        seen_chunk_ids,
        extractor_obj,
        level,
        # 16 §3.3: байты оператора пришли **не от сервера** — «живым наблюдением»
        # такой документ не является. ``live`` здесь был бы ложной провенанс-записью,
        # а ``stale`` (пропуск по потолку ревалидации) — не тот механизм. Сеть не
        # трогалась ⇒ ``reused``; последствия для публикации ``❌`` считает правило
        # §11.2 там, где оно исполняется (``evaluate``), а не писатель строки.
        reuse_kind=ReuseKind.REUSED,
        origin=ORIGIN_MANUAL,
    )
    return doc_id, raw_path


def submit_collect(
    work: list[CrawlWorkItem],
    *,
    executor: CrawlExecutorPort | None,
    errors: list[str],
    session: str | None,
    run_id: str | None,
    executor_poll_interval: float,
    executor_sleep: Callable[[float], None],
) -> dict[tuple[str, str], CrawlResultItem]:
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
    if executor is None:
        # Внутренний инвариант: волны подаёт только транспортный путь (``split``).
        # Явная проверка, а не ``assert``: под ``-O`` assert исчез, и функция упала бы
        # позже на ``executor.submit`` с неинформативным ``AttributeError``.
        raise AssertionError(
            "submit_collect вызван без исполнителя (локальный краул волны не подаёт)"
        )
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
        left = submit_collect(
            work[:mid],
            executor=executor,
            errors=errors,
            session=session,
            run_id=run_id,
            executor_poll_interval=executor_poll_interval,
            executor_sleep=executor_sleep,
        )
        right = submit_collect(
            work[mid:],
            executor=executor,
            errors=errors,
            session=session,
            run_id=run_id,
            executor_poll_interval=executor_poll_interval,
            executor_sleep=executor_sleep,
        )
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


def work_item(
    pid: str,
    url: str,
    target: Any,
    *,
    validators: Callable[[str, str], tuple[str | None, str | None]],
    robots: bool = False,
) -> CrawlWorkItem:
    """Собирает работу волны из резолвнутой цели (оси ``FetchPlan`` + провенанс).

    P3-A2/A2c: ``rate_limit`` переносится per-item — ровно тот **резолвнутый** лимит
    (``target.plan.rate_limit``), что локально лежит в ``desc.fetch_plan.rate_limit``
    после иерархии источников; pacing исполняет edge (§11). ``None`` — без пейсинга.

    A2b/§6(b): ``robots`` — egress-политика, вычисленная ядром на **корень**; edge
    строит краулеры «голыми», поэтому флаг обязан ехать с работы (иначе split-discovery
    не применяет robots и расходится с local по контенту).

    ``if_none_match``/``if_modified_since`` (09 §6.3) — валидаторы условного GET из
    ``url_state``: edge отправляет ``304`` тем же контрактом, что и локальный путь, и
    обязан получить их в работе (иначе split-профиль не экономит трафик). ``None`` —
    прежний безусловный GET (байт-идентичное поведение).
    """
    plan_i = target.plan
    if_none_match, if_modified_since = validators(pid, url)
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
        if_none_match=if_none_match,
        if_modified_since=if_modified_since,
    )


def expand_directory(
    pid: str,
    url: str,
    target: Any,
    *,
    mark: Callable[..., None],
    emit: Callable[[], None],
    errors: list[str],
    pollers: Mapping[str, PollPort] | None,
    applied_engines: dict[tuple[str, str], str | None],
) -> list[str] | None:
    """Phase 5.2: разворачивает источник-каталог (``feed``/``channel``) в список ссылок.

    Возвращает ``None``, если ``target.kind`` — **не** директорийный (URL обрабатывается
    как обычная страница). Иначе поллит каталог через ``pollers[kind]`` и возвращает
    URL-записи (кандидаты, не evidence — I19): сам каталог **не** ретейнится и не даёт
    ``CorpusDoc``/чанков. Любой сбой каталога (нет поллера / плохие ``params`` / ошибка
    поллинга) — терминальный ``FAILED`` ``source_unavailable`` (сеанс финализируется
    ``PARTIAL``, а не «тихо пусто»). Записи ре-валидируются SSRF-политикой и дедуплицируются
    с сохранением порядка поллера; один уровень развёртки (записи повторно не расширяются).
    """
    if target.kind not in DIRECTORY_KINDS:
        return None

    poller = (pollers or {}).get(target.kind)
    if poller is None:
        reason = (
            f"{pid}/{url}: источник-каталог kind={target.kind}, "
            "но поллер не сконфигурирован"
        )
        errors.append(reason)
        mark(
            pid,
            url,
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
            error=reason,
        )
        emit()
        return []

    problems = poller.validate_params(target.kind, target.params)
    if problems:
        reason = (
            f"{pid}/{url}: источник-каталог kind={target.kind}: "
            + "; ".join(problems)
        )
        errors.append(reason)
        mark(
            pid,
            url,
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
            error=reason,
        )
        emit()
        return []

    try:
        entries = poller.poll(
            url, kind=target.kind, params=target.params, plan=target.plan
        )
    except Exception as exc:  # noqa: BLE001 — изоляция каталога (не роняет фазу)
        reason = f"{pid}/{url}: poll: {type(exc).__name__}: {exc}"
        errors.append(reason)
        mark(
            pid,
            url,
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
            error=reason,
        )
        emit()
        return []

    # Каталог добыт: элемент ``FETCHED`` (raw НЕ сохраняется — это не контент/evidence,
    # только ссылки; resume такого элемента не даёт — ``resume_retained`` требует ``raw_path``).
    applied_engines[(pid, url)] = target.engine
    mark(pid, url, CrawlItemStatus.FETCHED)
    emit()

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


def expand_sitemap(
    pid: str,
    seed_url: str,
    *,
    mark: Callable[..., None],
    emit: Callable[[], None],
    errors: list[str],
    pollers: Mapping[str, PollPort] | None,
    sitemap_cache: dict[tuple[str, str], list[str]],
    targets: dict[tuple[str, str], Any],
    applied_engines: dict[tuple[str, str], str | None],
    domain_profiles: Mapping[str, DomainProfile] | None,
    resolve_target: Callable[[str, str], Any],
) -> list[str]:
    """D3: разворачивает per-site ``sitemap`` в дополнительные depth-0 seed-URLs.

    Возвращает ``<loc>`` (после SSRF-проверки) или ``[]``, если у сайта нет sitemap. Сам
    sitemap — не страница: не ретейнится/не ингестится (элемент сеанса ``FETCHED`` без
    ``raw_path``, I19). Сбой/отсутствие поллера изолируется в ``errors`` (seed оператора
    сохраняется). Кэш по ``(product, sitemap_url)`` — один опрос на сайт.
    """
    sitemap_url = sitemap_url_for(seed_url, domain_profiles=domain_profiles)
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
        mark(
            pid,
            sitemap_url,
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
            error=reason,
        )
        emit()
        sitemap_cache[key] = locs
        return locs

    problems = poller.validate_params(_SITEMAP_KIND, {})
    if problems:
        reason = f"{pid}/{sitemap_url}: sitemap: " + "; ".join(problems)
        errors.append(reason)
        mark(
            pid,
            sitemap_url,
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
            error=reason,
        )
        emit()
        sitemap_cache[key] = locs
        return locs

    # Провенанс: sitemap — не страница, собственный ``kind="sitemap"`` (строка-данные).
    starget = resolve_target(pid, sitemap_url)
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
        mark(
            pid,
            sitemap_url,
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
            error=reason,
        )
        emit()
        sitemap_cache[key] = locs
        return locs

    applied_engines[(pid, sitemap_url)] = starget.engine
    mark(pid, sitemap_url, CrawlItemStatus.FETCHED)  # без raw: не контент (I19)
    emit()

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


def fresh_sitemap_seeds(
    pid: str,
    seed_url: str,
    frontier: Frontier,
    *,
    expand_sitemap: Callable[[str, str], list[str]],
    sitemap_seed_used: set[tuple[str, str]],
) -> SitemapSeeds:
    """``<loc>`` sitemap, ещё не прикреплённые ни к одному seed-root продукта (дедуп).

    Один и тот же sitemap у нескольких seed-URL даёт один набор ``<loc>``; прикреплять
    их к каждому корню — двойной фетч/бюджет, поэтому свежие seeds отдаются один раз.

    ``<loc>`` — **подчинённые** seeds корня, а не задающие scope: loc проходит тот же
    scope-допуск (host + path-граница + traversal-guard), что и обнаруженная ссылка,
    **кроме** классового исключения терминального артефакта (P6b): ``<loc>`` на
    ``.pdf``/``.docx``/``.pptx``/``.xlsx`` или заявленный контейнер вне path-prefix
    **ставится в очередь**, а не считается ``_SITEMAP_SKIP_PATH`` (host scope, SSRF и
    traversal-guard сохраняются; обход видим через
    :meth:`Frontier.note_artifact_bypass`). Scope-чек исполняется **до** пометки
    ``(pid, loc)`` использованным, поэтому loc, отвергнутый этим корнем, остаётся
    доступным другому корню продукта. Кэш опроса sitemap (``(pid, sitemap_url)``)
    остаётся нефильтрованным.

    Пометка ``(pid, loc)`` **не** делается здесь (F12): loc может быть отвергнут уже
    после scope — блэклистом профиля корня (``exclude_paths``/``exclude_documents``) или
    ``max_documents`` при постановке в очередь. Такой loc обязан остаться доступным
    другому корню продукта, поэтому ``sitemap_seed_used`` пополняется в
    корневом цикле только по факту приёма в очередь (``add_seed`` → ``queued``/
    ``dup``).

    Отказ scope — **провенанс**, не ``errors`` (F10): host-wide sitemap при path-scoped
    seed отвергает большинство ``<loc>`` штатно. Отказы агрегируются по классу
    (:func:`_sitemap_skip_class`): ``host`` (sitemap рекламирует другой host) отдельно
    от ``path`` (тот же host, путь вне префикса) — иначе host-сигнал тонул бы в массе
    path-отказов. На корень — не более одной записи на класс (не на loc).
    """
    fresh: list[str] = []
    rejected: dict[str, list[str]] = {_SITEMAP_SKIP_HOST: [], _SITEMAP_SKIP_PATH: []}
    for loc in expand_sitemap(pid, seed_url):
        admission = frontier.scope_admission(loc)
        if admission == SCOPE_OUT:
            rejected[_sitemap_skip_class(loc, frontier.root_url)].append(loc)
            continue
        if admission == SCOPE_ARTIFACT_OUTSIDE_PREFIX and not frontier.blacklisted(
            loc
        ):
            # P6b: терминальный артефакт вне path-prefix — enqueued, но обход обязан
            # быть видимым. Блэклист решается позже (``add_seed``, F12); здесь только
            # не пишем в провенанс то, что заведомо будет отброшено.
            frontier.note_artifact_bypass(loc)
        key = (pid, loc)
        if key in sitemap_seed_used:
            continue
        fresh.append(loc)
    skipped: list[dict[str, Any]] = []
    for reason, label in (
        (_SITEMAP_SKIP_HOST, "чужой host"),
        (_SITEMAP_SKIP_PATH, "path вне префикса корня"),
    ):
        locs = rejected[reason]
        if not locs:
            continue
        # Сотни loc на один корень (host-wide sitemap при узком seed) — одна запись:
        # построчные затопили бы провенанс. Примеры — первые три (счётчик — точный).
        preview = ", ".join(locs[:3])
        if len(locs) > 3:
            preview += f" и ещё {len(locs) - 3}"
        skipped.append(
            {
                "reason": reason,
                "count": len(locs),
                "examples": list(locs[:3]),
                "message": (
                    f"{pid}/{seed_url}: sitemap: {len(locs)} <loc> вне scope корня "
                    f"{frontier.root_url} ({label}) — не enqueued (например: {preview})"
                ),
            }
        )
    return SitemapSeeds(fresh=tuple(fresh), skipped=tuple(skipped))


def local_prefetch(
    pid: str,
    wu: str,
    wtarget: Any,
    *,
    validators: Callable[[str, str], tuple[str | None, str | None]],
    crawler: CrawlerPort,
    crawlers: Mapping[str, CrawlerPort] | None,
    items: Mapping[tuple[str, str], CrawlSessionItem],
    raw_root_path: Path,
    depth: int | None = None,
    scope_host: str | None = None,
    enforce_redirect_scope: bool = False,
    robots: bool = False,
    max_response_bytes: int,
    artifact_max_bytes: int | None = None,
    attachment_formats: Sequence[str] | None = None,
    capture_id: str | None = None,
) -> LocalFetch:
    """D4 pre-fetch (главный поток): resume/browser-guard/решение о пейсинге.

    **Чистая** фаза: не пишет ни в сеанс, ни в ``applied_engines``. Единственное
    исключение — сбой чтения ``url_state`` (09 §6.3) фиксируется в ``errors``: он
    деградирует запрос до безусловного GET, а не роняет фазу. Все прочие мутации
    отложены в :func:`commit_local` (FIFO-порядок коммита) — это позволяет
    исполнять саму сеть в пуле, а исходы коммитить детерминированно на главном потоке.
    Активный краулер выбирается по ``wtarget.engine`` (fallback — ``crawler``).

    A2b/§6(b): ``robots`` проставляется в ``plan.robots`` (``dataclasses.replace``) —
    адаптер применяет ``robots.txt``-гейт только при ``self._robots is not None and
    plan.robots``; без discovery/принуждения флаг False (никаких лишних ``robots.txt``).

    **Условный GET (09 §6.3).** При политике ``auto``/``revalidate`` валидаторы из
    ``url_state`` переносятся в ``FetchPlan`` (``dataclasses.replace`` — план не
    мутируется): ``304`` экономит трафик, а трафик-нейтральный ``None`` сохраняет
    прежний безусловный GET байт-в-байт.
    """
    fetch_plan = (
        wtarget.plan
        if getattr(wtarget.plan, "robots", False) == robots
        else replace(wtarget.plan, robots=robots)
    )
    # P0: per-site/run потолок ответа едет на плане (адаптер применяет его к стриму).
    fetch_plan = replace(fetch_plan, max_response_bytes=max_response_bytes)
    # P6: допуск артефакта-контейнера. Страница остаётся под ``max_response_bytes``;
    # артефакт класса «контейнер» (суффикс/``Content-Type``) вправе уйти в staging-файл
    # под ``raw_root`` и ограничен ``max_artifact_bytes``. Класс решает адаптер по
    # заявленному набору: незаявленный контейнер получает прежний ``payload_too_large``.
    if artifact_max_bytes is not None:
        fetch_plan = replace(
            fetch_plan,
            staging_dir=str(raw_root_path / _STAGING_DIRNAME),
            max_stream_bytes=int(artifact_max_bytes),
            attachment_formats=(
                None if attachment_formats is None else tuple(attachment_formats)
            ),
        )
    # 09 §6.3: валидаторы условного GET (только auto/revalidate; refresh — безусловный).
    if_none_match, if_modified_since = validators(pid, wu)
    if if_none_match is not None or if_modified_since is not None:
        fetch_plan = replace(
            fetch_plan,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
        )
    prior_item = items.get((pid, wu))
    resume_source = (
        resume_retained(
            prior_item,
            max_bytes=max_response_bytes,
            artifact_max_bytes=artifact_max_bytes,
            attachment_formats=attachment_formats,
        )
        if prior_item is not None
        else None
    )
    reject = (
        resume_source
        if isinstance(resume_source, RetainedReject)
        else None
    )
    resume = resume_source if isinstance(resume_source, RetainedRaw) else None
    # Терминальный класс (not_found/payload_too_large/unsupported_binary): живой
    # фетч не нужен и вреден (см. helper).
    terminal = terminal_failure_kind(prior_item) is not None
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
        and reject is None
        and not terminal
        and not browser_blocked
        and fetch_plan.fetch is not FetchMode.BROWSER
    )
    host = (urlsplit(wu).hostname or "").lower()
    return LocalFetch(
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
        terminal=terminal,
        reject=reject,
        max_artifact_bytes=artifact_max_bytes,
        attachment_formats=(
            None if attachment_formats is None else tuple(attachment_formats)
        ),
        # План 20 §3: снятие корня едет с дескриптором до писателя ``url_state`` —
        # другой точки, где известны и корень, и исход этого URL, нет.
        capture_id=capture_id,
    )


# Повторы живого фетча — **одно** правило на все пути краула: константа
# (:data:`core.util.fetch_retry.FETCH_RETRY_TIMES`), цикл
# (:func:`core.util.fetch_retry.fetch_with_paced_retries`) и классификатор отказа
# (:func:`core.ports.crawl_errors.is_transient_fetch_error`) живут в своих слоях.
# ``FETCH_RETRY_TIMES`` здесь только **реэкспортируется** (импорт выше): это публичный
# адрес локального пути, своей копии числа у него нет — второй литерал означал бы два
# разных правила повтора у двух путей фетча.
def _fetch_with_paced_retries(
    desc: LocalFetch,
    *,
    limiter: RateLimiter,
    pacing: CrawlPacing | None = None,
) -> RawPage:
    """Живой фетч с повторами, **каждый из которых берёт свой слот** лимитера.

    Один помощник на оба пути (пуловой :func:`local_fetch` и inline
    :func:`fetch_and_commit`): ретрай, живущий только в одном из них, — тот же дефект,
    что был у движка, только незаметнее. Сам цикл живёт в
    :func:`core.util.fetch_retry.fetch_with_paced_retries` — его же зовёт
    edge-планировщик под ``split``, поэтому повторы у двух профилей исполнения одни.

    Поведение (всё это делает общий помощник, здесь — только привязка к дескриптору):

    * **пауза между попытками** — не изобретённая экспонента, а сам ``RateLimiter``:
      ``wait_reserved`` перед каждой попыткой резервирует новый слот origin'а и спит до
      него. ``RawPage`` заголовков не несёт, поэтому ``Retry-After`` здесь недоступен;
      объявленный интервал и есть вежливая пауза (если он нулевой — паузы нет, это
      объявленный факт, а не забывчивость).
    * ``fetch_started_at`` — метка **последнего** резерва (разрешённый момент, а не момент
      пробуждения), как и раньше: интервал вежливости меряется по резервам.
    * ``fetch_ms`` — полная длительность работы (все попытки и паузы между ними), без
      первого ожидания резерва: у одной удачной попытки семантика прежняя («время вокруг
      живого вызова»).
    * ``pacing.request`` оборачивает **каждую** попытку отдельно: спящий в лимитере поток
      к хосту не обращается и «в полёте» не считается; фактический HTTP-учёт каждой
      попытки складывается в тот же origin.
    * Повтор считается в приборе (``pacing.retry``): «сколько запросов было повтором»
      видно и без вычитания ``http_requests - items``.
    """
    stats = FetchAttemptStats()
    try:
        return fetch_with_paced_retries(
            lambda: desc.active.fetch(desc.wu, desc.fetch_plan),
            host=desc.host,
            rate_limit=desc.fetch_plan.rate_limit,
            limiter=limiter,
            is_transient=is_transient_fetch_error,
            stats=stats,
            attempt_scope=(
                (lambda: pacing.request(desc.pid, desc.host))
                if pacing is not None
                else None
            ),
            on_retry=(
                (lambda: pacing.retry(desc.pid, desc.host))
                if pacing is not None
                else None
            ),
        )
    finally:
        # Тот же контракт, что раньше: живой фетч оставил след, только если резерв был.
        if stats.started_at is not None:
            desc.fetch_started_at = stats.started_at
            desc.fetch_ms = stats.fetch_ms


def local_fetch(
    desc: LocalFetch,
    *,
    limiter: RateLimiter,
    pacing: CrawlPacing | None = None,
) -> RawPage:
    """D4 fetch-фаза (**единственный** вызов, идущий в пул): paced-ретраи + ``active.fetch``.

    Никакой мутации состояния сеанса здесь нет — только живой сетевой вызов.
    ``limiter.wait_reserved`` даёт интервал **по origin** и одновременно метку попытки:
    воркеры пула зовут pacing конкурентно, и ``RateLimiter`` резервирует слот на origin
    под локом, а спит уже вне него — иначе один хост блокировал бы все остальные.
    Гарантия — внутри процесса; при пуле процессов состояние не разделяется (см.
    ``util/rate_limit``).

    **Резерв — один на попытку, не один на элемент работы.** Успешный фетч по-прежнему
    делает ровно один резерв; повтор (5xx/408/429/сетевой сбой) — новый резерв, и это
    исправление дефекта 07.11.2026: ретраи загрузчика Scrapy исполнялись внутри одной
    попытки и пейсинга не видели. Дополнительный ``limiter.wait`` перед резервом — это
    **второй** независимый слот того же origin'а, то есть удвоенный объявленный интервал
    (``rate_limit: {min_s: 1, max_s: 5}`` превращался в 2–10 с при среднем 6 с).
    Оба пути (:func:`local_fetch` и :func:`fetch_and_commit`) зовут один помощник
    :func:`_fetch_with_paced_retries`, поэтому пейсят одинаково.
    """
    return _fetch_with_paced_retries(desc, limiter=limiter, pacing=pacing)


def fetch_and_commit(
    desc: LocalFetch,
    *,
    limiter: RateLimiter,
    commit_local: Callable[..., tuple[RawPage, Path, FetchPlan, bool] | None],
    pacing: CrawlPacing | None = None,
) -> tuple[RawPage, Path, FetchPlan, bool] | None:
    """Inline-фетч на главном потоке + коммит (последовательный путь и browser-режим D4).

    Используется, когда фетч **не** уходит в пул: ``max_workers in (None, 1)`` либо
    browser-элемент (``BrowserCrawler`` не потокобезопасен). Метка ``fetch_started_at``
    и повторы — тот же помощник, что у пулового пути (:func:`_fetch_with_paced_retries`),
    поэтому успешный фетч делает ровно один резерв, а повтор — ещё один.
    """
    try:
        page = _fetch_with_paced_retries(desc, limiter=limiter, pacing=pacing)
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        return commit_local(desc, exc=exc)
    return commit_local(desc, page=page)


def commit_local(
    desc: LocalFetch,
    *,
    mark: Callable[..., None],
    retain: Callable[..., Path | None],
    validators: Callable[[str, str], tuple[str | None, str | None]],
    errors: list[str],
    applied_engines: dict[tuple[str, str], str | None],
    page: RawPage | None = None,
    exc: Exception | None = None,
) -> tuple[RawPage, Path, FetchPlan, bool] | None:
    """D4 post-fetch (главный поток): мутации исхода в FIFO-порядке отправки.

    Возвращает ``(page, raw_path, fetch_plan, freshly_fetched)`` для последующей
    ``ingest_one``; ``None`` — исход пропущен/сбой (терминальный ``FAILED`` уже
    проставлен ``mark``), вызывающий делает ``continue``. ``freshly_fetched=False`` —
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

    # S1/Phase 2.1: применяемый движок — первая мутация исхода (ориентир для ``mark``),
    # как и прежде; всегда до resume-guard/бrowser-guard.
    applied_engines[(pid, wu)] = desc.engine_value

    # Терминальный класс (not_found/payload_too_large/unsupported_binary): определённый
    # ответ уже получен либо исполнение упёрлось в осознанную границу; живой фетч не
    # выполняется. Элемент остаётся ``FAILED`` (класс сбоя, не статус), ``attempts`` не
    # растёт (``mark`` инкрементит только при смене статуса), поэтому исход не
    # превращается в ``attempts_exhausted``.
    if desc.terminal:
        prior = desc.prior_item
        kind = terminal_failure_kind(prior) or FailureKind.NOT_FOUND
        mark(
            pid,
            wu,
            CrawlItemStatus.FAILED,
            error_kind=kind.value,
            error=(
                prior.error
                if prior is not None and prior.error
                else f"{kind.value}: терминально, без повтора"
            ),
            depth=desc.depth,
        )
        return None

    # P0: retained raw отклонён по размеру/бинарности — терминально, без экстракции.
    # Для бинарного артефакта сессия не должна ссылаться на байты: чистим raw_path/
    # content_hash и удаляем retained-файл (он не evidence). Для oversize — та же
    # очистка (на диске остаётся, но сессия больше на него не опирается).
    if desc.reject is not None:
        reject = desc.reject
        # Сбой удаления файла не меняет исход: отказ уже терминальный и виден в
        # ``errors`` + элементе сеанса (``raw_path``/``content_hash`` очищены, то есть
        # байты вне evidence). Неудалённый файл — вопрос retention, не вердикта.
        with contextlib.suppress(OSError):
            reject.path.unlink(missing_ok=True)
        errors.append(f"{pid}/{wu}: {reject.error_kind.value}: {reject.reason}")
        mark(
            pid,
            wu,
            CrawlItemStatus.FAILED,
            error_kind=reject.error_kind.value,
            error=reject.reason,
            raw_path=CLEAR_SESSION_FIELD,
            content_hash=CLEAR_SESSION_FIELD,
            depth=desc.depth,
        )
        return None

    if desc.resume is not None:
        if desc.prior_item is None:
            # Инвариант сборки ``LocalFetch``: resume существует только у элемента,
            # который уже был в сеансе (иначе возобновлять нечего).
            raise AssertionError(
                f"{pid}/{wu}: resume без prior_item — LocalFetch собран неверно"
            )
        retained = desc.resume
        # 09 §6.3: валидаторы сеанс не хранит — они приходят из ``url_state`` (durable),
        # чтобы запись состояния после resume не потеряла условную валидацию.
        page = resume_page(
            desc.prior_item, wu, retained, fetch_plan, validators(pid, wu)
        )
        # Байты уже ретейнены (``retained.path`` — источник истины I19): повторной
        # публикации/копии нет. Артефакт-контейнер в RAM не читался — ``content`` пуст,
        # ``page.staged_path`` указывает на retained-файл.
        raw_path = retained.path
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
        mark(
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
        # Класс исхода несёт сам адаптер: ``failure_kind`` (404/410 → ``not_found``)
        # либо ``refusal_reason`` (robots-отказ, D1/§10). Нет атрибута — честный
        # дефолт ``source_unavailable``. ``mark`` канонизирует значение.
        error_kind = (
            getattr(exc, "failure_kind", None)
            or getattr(exc, "refusal_reason", None)
            or FailureKind.SOURCE_UNAVAILABLE.value
        )
        errors.append(f"{pid}/{wu}: crawl: {reason}")
        # Наблюдаемость (не классификация): фактический HTTP-статус, если адаптер его
        # видел (429/403/503/404 …). ``error_kind`` остаётся прежним — таксономия
        # ``FailureKind`` этим срезом не меняется; статус лишь делает отказ различимым
        # (транзиентный rate-limit vs политический запрет). Нет статуса — ``None``,
        # не выдумываем (carry-forward в ``mark_session`` не перекрывает прежнее).
        mark(
            pid,
            wu,
            CrawlItemStatus.FAILED,
            error_kind=error_kind,
            error=reason,
            depth=desc.depth,
            http_status=getattr(exc, "http_status", None),
            # 17 §6 (F6): попытка состоялась (сеть) — её время записано; отказ не делает
            # замер несуществующим, иначе p50/p95 теряли бы все неудачные запросы.
            fetch_started_at=desc.fetch_started_at,
            fetch_ms=desc.fetch_ms,
        )
        return None
    if page is None:
        errors.append(f"{pid}/{wu}: crawl: пустой RawPage")
        mark(
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
            mark(
                pid,
                wu,
                CrawlItemStatus.FAILED,
                error_kind="redirect_out_of_scope",
                error=reason,
                depth=desc.depth,
            )
            return None

    # P0: границы исполнения ДО retention/экстракции. Сначала размер (OVA/образ
    # должен стать payload_too_large, а не binary-sniff), затем бинарный не-документ.
    #
    # **Потолок класс-aware (P6b).** Терминальный артефакт (документ или заявленный
    # контейнер) меряется ``max_artifact_bytes``, страница — ``max_response_bytes``.
    # Без этого транспорт, поднявший свой потолок до artifact-уровня, всё равно
    # умирал бы здесь на страничном: живой прогон 22.10 — 125 727 643-байтный zip
    # приезжал целиком (scrapy не имеет staging-пути) и получал
    # ``payload_too_large: … > лимита 20971520`` уже после успешной загрузки.
    artifact = artifact_is_terminal(
        page.url or wu, page.content_type, desc.attachment_formats
    )
    artifact_cap = (
        fetch_plan.max_stream_bytes
        if fetch_plan.max_stream_bytes is not None
        else desc.max_artifact_bytes
    )
    cap = (
        artifact_cap
        if artifact and artifact_cap is not None
        else fetch_plan.max_response_bytes
    )
    if cap is not None and len(page.content) > cap:
        reason = (
            f"payload_too_large: {len(page.content)} байт > лимита {cap}"
        )
        errors.append(f"{pid}/{wu}: {reason}")
        mark(
            pid,
            wu,
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.PAYLOAD_TOO_LARGE.value,
            error=reason,
            raw_path=CLEAR_SESSION_FIELD,
            content_hash=CLEAR_SESSION_FIELD,
            depth=desc.depth,
        )
        return None
    binary_reason = unsupported_binary_reason(
        page.content or page.head or b"",
        url=page.url or wu,
        content_type=page.content_type,
        attachment_formats=desc.attachment_formats,
    )
    if binary_reason is not None:
        reason = f"unsupported_binary: {binary_reason}"
        errors.append(f"{pid}/{wu}: {reason}")
        mark(
            pid,
            wu,
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.UNSUPPORTED_BINARY.value,
            error=reason,
            raw_path=CLEAR_SESSION_FIELD,
            content_hash=CLEAR_SESSION_FIELD,
            depth=desc.depth,
        )
        return None

    # 2. Retention (I19): сырые байты пишутся ДО экстракции (общий шаг). Замер попытки
    # (F6) едет тем же вызовом: ``FETCHED``-элемент обязан нести момент начала и
    # длительность, иначе §7.2 №1 (`observed_min_interval_s`) не посчитать.
    # Отдельное имя от ``raw_path`` выше: там — путь **уже** ретейненных байтов
    # (resume, всегда ``Path``), здесь — результат retention (``Path | None``). Это две
    # разные величины, и общее имя делало ``None`` несовместимым с ``Path``.
    retained_path = retain(
        pid,
        wu,
        page,
        fetch_plan,
        depth=desc.depth,
        fetch_started_at=desc.fetch_started_at,
        fetch_ms=desc.fetch_ms,
        capture_id=desc.capture_id,
    )
    if retained_path is None:
        return None
    return page, retained_path, fetch_plan, True


def process_local(
    pid: str,
    wu: str,
    wtarget: Any,
    *,
    local_prefetch: Callable[..., LocalFetch],
    commit_local: Callable[..., tuple[RawPage, Path, FetchPlan, bool] | None],
    fetch_and_commit: Callable[..., tuple[RawPage, Path, FetchPlan, bool] | None],
    depth: int | None = None,
    scope_host: str | None = None,
    enforce_redirect_scope: bool = False,
    robots: bool = False,
    max_response_bytes: int,
    artifact_max_bytes: int | None = None,
    attachment_formats: Sequence[str] | None = None,
    capture_id: str | None = None,
) -> tuple[RawPage, Path, FetchPlan, bool] | None:
    """Локальный per-URL путь (последовательный): pre-fetch → fetch → post-fetch (D4).

    Точка входа для ``max_workers in (None, 1)`` — прежний однопоточный путь (пул не
    создаётся). Расщепление на фазы — задел под волновой пул (см.
    :func:`local_prefetch`/:func:`local_fetch`/:func:`commit_local`); здесь фетч
    исполняется inline на главном потоке.
    """
    desc = local_prefetch(
        pid,
        wu,
        wtarget,
        depth=depth,
        scope_host=scope_host,
        enforce_redirect_scope=enforce_redirect_scope,
        robots=robots,
        max_response_bytes=max_response_bytes,
        artifact_max_bytes=artifact_max_bytes,
        attachment_formats=attachment_formats,
        capture_id=capture_id,
    )
    if (
        desc.resume is not None
        or desc.reject is not None
        or desc.browser_blocked
        or desc.terminal
    ):
        return commit_local(desc)

    # P2-3: politeness-пейсинг — единый владелец corpus (R7-независимый). Применяется
    # **только** к живому фетчу: путь ``resume`` (retained raw, выше) не пейсится.
    return fetch_and_commit(desc)


def plan_incremental_items(
    product_id: str,
    roots: Sequence[str],
    *,
    state: ProductStatePort | None,
    policy: RefreshPolicy,
    product_modes: Mapping[str, str] | None,
    planned_validators: dict[tuple[str, str], tuple[str | None, str | None]],
    errors: list[str],
) -> list[IncrementalItem]:
    """Дополнительный work-set инкрементального продукта (09 §6.3, MVP-3b).

    Возвращает только **не**-авторские элементы: авторские корни обрабатывает прежний
    путь (каталоги/sitemap), а планировщик лишь подтверждает их ``fetch``. Порядок —
    планировщика (``url_state`` отсортирован по ``normalized_url``).

    ``exclude_urls`` не передаётся: набор исключений резолвится per-root
    (``resolve_crawl_limits``: ``app < run < site``), поэтому дешёвого продуктового
    набора нет; ``Frontier`` всё равно применяет блэклист к каждому seed и оставляет
    видимую запись, а не тихий пропуск.

    **``reuse`` (политика ``offline``).** Элемент ``reuse`` доходит до фазы как есть:
    ``corpus`` исполняет его отдельным проходом без сети
    (:func:`offline_reuse_product`) и **не** понижает до ``revalidate``. Понижение
    означало бы, что политика «без сети» ходит в сеть: ``revalidate`` без валидаторов —
    это безусловный GET. Авторские корни сюда не входят, но и они под ``offline``
    разрешаются в состоянии, а не в сети — решает вызывающий (он знает корни).

    ``new``/``full`` план **под сетью** не расширяют (прежний BFS/sitemap обнаружит сам).
    Под политикой без сети (``offline``/``frozen``) дело обратное: BFS-discovery не
    бывает вовсе, поэтому единственный источник известных URL — ``url_state``, и режим
    продукта (онбординг, полнота снимка) не должен его отменять. Иначе проход без сети
    видит только авторские корни и **молча** теряет всё, что найдено прошлым краулом
    (измерено на стенде плана 19 §P5: 2 документа из 9).

    Ремонт провалов (``--only-failed``) идёт **не** этим путём: он не зависит от режима
    продукта и не выводится из ``url_state`` целиком — см. :func:`plan_repair_items`.
    """
    if state is None:
        return []
    no_network = not dispatch_refresh(policy).network
    if (
        not no_network
        and as_product_mode((product_modes or {}).get(product_id))
        is not ProductMode.INCREMENTAL
    ):
        return []
    try:
        planned = plan_incremental(
            product_id=product_id,
            roots=list(roots),
            state=state,
            policy=policy,
            exclude_urls=(),
        )
    except Exception as exc:  # noqa: BLE001 — изоляция планировщика
        errors.append(
            f"{product_id}: plan_incremental: {type(exc).__name__}: {exc} — "
            "работа только по авторским корням"
        )
        return []
    author = {_state_url_key(url) for url in roots}
    extra: list[IncrementalItem] = []
    for item in planned:
        if item.root_url is not None and item.normalized_url in author:
            continue  # авторский корень — прежний путь (каталог/sitemap)
        if item.kind is IncrementalKind.REVALIDATE:
            # Валидаторы едут и в локальный ``FetchPlan``, и в ``CrawlWorkItem``.
            planned_validators[(product_id, item.normalized_url)] = (
                item.etag,
                item.last_modified,
            )
        extra.append(item)
    return extra


def _host_declared(url: str, scopes: Sequence[tuple[str, Frontier]]) -> bool:
    """Объявлен ли host URL хотя бы одним корнем продукта (C: объявленные хосты).

    Предикат — ``Frontier.host_in_scope`` (тот же, что у краула), а не ``in_scope``:
    path-prefix здесь не проверяется, потому что ремонтный URL — **известный** лист,
    а не точка продолжения навигации; принадлежность «своему» префиксу решает
    группировка (:func:`group_revalidation_items`), а host — право вообще тратить
    бюджет продукта.
    """
    host = (urlsplit(url).hostname or "").strip().lower()
    if not host:
        return False
    return any(probe.host_in_scope(host) for _root, probe in scopes)


def plan_repair_items(
    product_id: str,
    urls: Sequence[str],
    *,
    state: ProductStatePort | None,
    policy: RefreshPolicy,
    planned_validators: dict[tuple[str, str], tuple[str | None, str | None]],
    errors: list[str],
    scopes: Sequence[tuple[str, Frontier]] = (),
) -> tuple[list[IncrementalItem], list[tuple[str, str]]]:
    """Work-set **ремонта** проваленных URL (``--only-failed``, межпрогонный носитель).

    Вход — уже разрешённое множество провалов (``core.cli.context._only_failed_work_set``:
    ``FAILED``-элементы текущего сеанса, иначе строки ``url_state`` с
    ``consecutive_failures > 0``). Здесь оно превращается в элементы плана, а не в seed'ы:
    ремонт — это повторная проверка **известных** URL, а не обход.

    Почему отдельная функция, а не ветка :func:`plan_incremental_items`:

    * ремонт **не зависит от режима продукта**. ``plan_incremental_items`` возвращает ``[]``
      для ``new``/``full`` — это верно для инкрементального плана (полный обход — прежний
      путь), но провал прошлого цикла существует и у продукта, который сейчас ``new``/``full``
      (например, когорта расширилась, а прежние провалы никуда не делись);
    * и наоборот: :func:`plan_incremental` для такого продукта вернул бы корни **и все**
      строки ``url_state`` — то есть ремонт превратился бы в полный краул. Поэтому набор
      берётся **ровно** из переданного множества, а не выводится из состояния целиком.

    Семантика элемента: ``kind=revalidate``, ``depth=0``, ``root_url=None`` — известный
    лист. Он встаёт в дренаж через ``Frontier.add_revalidate_seed`` (вне бюджета
    ``max_documents``), поэтому не расширяет BFS и не тратит discovery-бюджет корня;
    группировку по корням делает вызывающий (:func:`group_revalidation_items`), так что
    авторские корни под ``--only-failed`` **не обходятся**.

    Валидаторы (``ETag``/``Last-Modified``) и ``doc_version_id`` переносятся из
    ``url_state``, если строка есть: ремонт — условный GET, а не безусловный.

    **``not_found`` в наборе не появляется.** ``record_url_state`` увеличивает
    ``consecutive_failures`` только на транзиентных сбоях; ``not_found`` (404) — терминальное
    наблюдение, он лишь обнуляет ``doc_version_id`` (версия ``superseded``). Ретраить 404
    значит выдавать терминальное наблюдение за сбой, который стоит повторять; строка с
    ``last_error_kind=not_found`` поэтому исключается, даже если счётчик остался от прежних
    транзиентных сбоев (иначе такой URL попадал бы в каждый ремонт бесконечно: счётчик на
    404 не растёт и не сбрасывается).

    :param urls: разрешённое множество провалов (порядок сохраняется, дубли схлопываются).
    :param state: durable-состояние продукта; ``None`` — ремонт без валидаторов (видимо).
    :param policy: политика наблюдения прогона. ``offline`` — элемент тоже ``reuse``:
        ремонт остаётся повторной проверкой **известных** URL, но проверяет он состояние,
        а не сервер (сети в этой политике нет — понижение до ``revalidate`` означало бы
        безусловный GET).
    :param planned_validators: общий словарь прогона ``(product, normalized) → (etag, lm)``.
    :param scopes: ``(root, Frontier)`` продукта — только для проверки объявленных хостов.
    :returns: ``(элементы, отброшенные)``; отброшенные — пары ``(url, причина)``, чтобы
        вызывающий опубликовал число и причину, а не потерял URL молча.
    """
    rows: dict[str, UrlStateRow] = {}
    if state is not None:
        try:
            rows = {
                row.normalized_url: row
                for row in state.iter_url_state(product_id)
            }
        except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
            errors.append(
                f"{product_id}: repair: state.iter_url_state: {type(exc).__name__}: "
                f"{exc} — ремонт без валидаторов"
            )

    items: list[IncrementalItem] = []
    dropped: list[tuple[str, str]] = []
    seen: set[str] = set()
    # План 19 §P3: вид элемента и отправка валидаторов — из общей таблицы исполнения
    # политики, а не из `is OFFLINE`-ветки (иначе `frozen` молча пошёл бы в сеть).
    execution = dispatch_refresh(policy)
    for raw in urls:
        text = str(raw)
        normalized = _state_url_key(text)
        if normalized is None:
            errors.append(
                f"{product_id}/{text}: repair: URL не нормализуется — не поставлен"
            )
            dropped.append((text, "not_normalizable"))
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        row = rows.get(normalized)
        if row is not None and row.last_error_kind == FailureKind.NOT_FOUND.value:
            # Терминальное наблюдение (404), а не транзиентный сбой: не ремонтируется.
            dropped.append((text, "not_found_terminal"))
            continue
        if not _host_declared(text, scopes):
            dropped.append((text, "out_of_declared_hosts"))
            continue
        if execution.validators:
            # Валидаторы условного GET: ремонт — повторная проверка известного URL.
            planned_validators[(product_id, normalized)] = (
                row.etag if row is not None else None,
                row.last_modified if row is not None else None,
            )
        items.append(
            IncrementalItem(
                url=text,
                normalized_url=normalized,
                # Происхождение в ``url_state`` не сохранено, а ремонт корней не обходит.
                root_url=None,
                depth=0,
                kind=execution.known_kind,
                etag=row.etag if row is not None else None,
                last_modified=row.last_modified if row is not None else None,
                doc_version_id=row.doc_version_id if row is not None else None,
            )
        )
    return items, dropped


#: Тексты оффлайн-исходов (09 §6.3, ``refresh_policy=offline``). Токены вынесены в
#: константы, потому что их читают оператор и тесты: смена формулировки не должна
#: незаметно менять смысл («не наблюдали» ≠ «нечего переиспользовать» ≠ «ошибка фазы»).
OFFLINE_UNKNOWN_ROOT_TEXT = (
    "offline: URL отсутствует в durable-состоянии (url_state) — оффлайн-наблюдение "
    "невозможно: сети в этой политике нет, а переиспользовать нечего. Прогоните "
    "--refresh auto/revalidate, чтобы получить документ"
)
OFFLINE_NO_VERSION_TEXT = (
    "offline: переиспользовать нечего — у URL нет активной версии документа; "
    "документ не вошёл в корпус (сеть не трогается)"
)


@dataclass(frozen=True)
class OfflineReuseReport:
    """Итог оффлайн-прохода продукта: что переиспользовано и что не удалось (09 §6.3).

    Отчёт существует, потому что «переиспользовано N», «пересобрано из байтов M» и
    «пересобрать нечем J» — разные факты, и ни один не должен выглядеть как другой.
    Счётчики публикуются строкой продукта и в событии ``product_end``; в ``errors``
    идут только **отказы** (``no_version``/``no_bytes``), а пересборка — факт, не
    ошибка, поэтому оффлайн-прогон нельзя спутать ни с полным, ни с провалившимся.

    :param reused: документов вошло в корпус из durable-субстрата.
    :param chunks: чанков переиспользовано (сумма по принятым документам).
    :param rebuilt: документов **пересобрано** из сохранённых байтов текущим
        конвейером (chunk-set отвергнут, байты на месте).
    :param rebuilt_chunks: чанков принято из пересобранных документов (сумма).
    :param rebuilt_seconds: суммарное время пересборки (экстракция + нарезка).
    :param no_bytes: URL, у которых chunk-set не годится **и** сохранённых байтов нет:
        пересобрать нечем (отдельно от ``no_version``: там нет версии, здесь нет файла).
    :param no_version: URL известны состоянию, но активной версии документа нет
        (в том числе ``superseded``): пересобирать не из чего.
    :param unknown_roots: авторские корни, которых нет в ``url_state``: оффлайн их
        наблюдать нечем, и сети для этого прогона не существует.
    :param urls: URL, для которых сеть не трогалась (порядок прохода).
    :param skipped_present: URL, которые проход **не** тронул, потому что в этом прогоне
        у них **уже есть строка корпуса** (``ingest.docs``): живое наблюдение этого
        прогона или документ, добавленный предыдущим проходом переиспользования. Это не
        «переиспользовано» и не «нечего переиспользовать», а третий факт: копия в
        состоянии есть, но строка корпуса уже занята — и вторая строка на тот же URL
        была бы либо ложным провенансом (``reused``/``NULL`` при состоявшемся 200), либо
        устаревшей версией того же URL рядом с наблюдённой (I19).
    """

    reused: int = 0
    chunks: int = 0
    #: Документов **пересобрано** из сохранённых байтов текущим конвейером: chunk-set
    #: отвергнут (чужая версия чанкера, пустой набор, I19-расхождение текста), но байты
    #: на месте. Это не «переиспользовано» (экстрактор и чанкер вызывались) и не ошибка
    #: (сеть не трогалась): третий факт, названный токеном ``reuse_bytes_rebuild``.
    rebuilt: int = 0
    #: Чанков принято из пересобранных документов (сумма).
    rebuilt_chunks: int = 0
    #: Суммарное время пересборки (экстракция + нарезка), секунды. Медиана не нужна:
    #: публикуется **цена** прохода, а не распределение одной выборки.
    rebuilt_seconds: float = 0.0
    #: URL, у которых chunk-set не годится **и** сохранённых байтов нет: пересобрать
    #: нечем. Отдельно от ``no_version``: там нет версии, здесь нет файла байтов.
    no_bytes: tuple[str, ...] = ()
    no_version: tuple[str, ...] = ()
    unknown_roots: tuple[str, ...] = ()
    urls: tuple[str, ...] = ()
    #: URL, пропущенные, потому что строка корпуса этого прогона уже есть (E.6):
    #: живое наблюдение побеждает копию по построению, а второй проход не повторяет
    #: работу первого (иначе ``duplicates`` считал бы схлопывание, которого не было).
    skipped_present: tuple[str, ...] = ()
    #: Durable-группы дедупа, которые использовал проход (план 19 §P3): пары
    #: ``(doc_hash, url)``. Пишутся в запись снятия, чтобы следующий ``frozen`` выбрал
    #: тот же канонический адрес, не обращаясь к ``crawl_session_items``.
    capture_docs: tuple[CapturedDocRow, ...] = ()
    #: Метка политики в операторской строке: ``offline`` (прогон целиком) ‖
    #: ``crawl_policy`` (корень, которому политика запретила сеть). Один проход —
    #: два повода; читатель обязан их различать, не сравнивая корпуса.
    label: str = "offline"

    def note(self, product_id: str) -> str:
        """Операторская строка продукта: проход и его цена в наблюдении."""
        # Хвост про пропуск добавляется **только когда он есть**: строка ``offline``
        # без живых наблюдений не должна обрастать нулевым фактом.
        skipped = (
            ""
            if not self.skipped_present
            else f", уже в корпусе этого прогона {len(self.skipped_present)} "
            "(не переиспользуется)"
        )
        return (
            f"corpus: {self.label} {product_id} — переиспользовано документов "
            f"{self.reused} (чанков {self.chunks}), пересобрано из байтов "
            f"{self.rebuilt} (чанков {self.rebuilt_chunks}), переиспользовать нечего "
            f"{len(self.no_version)}, байтов нет {len(self.no_bytes)}, "
            f"корней вне состояния {len(self.unknown_roots)}"
            f"{skipped}; "
            f"reused={self.reused} rebuilt={self.rebuilt} "
            f"chunks={self.chunks + self.rebuilt_chunks} no_bytes={len(self.no_bytes)}; "
            "сеть не трогалась, наблюдений этого прогона нет "
            "(❌ — только с датой носителя, I2d)"
            f"{self._rebuild_token()}"
        )

    def _rebuild_token(self) -> str:
        """Машинный токен пересборки с числами — только когда пересборка была.

        Токен **не** идёт в ``errors``: пересборка — не ошибка, а факт («сеть не
        трогалась, документ вошёл в корпус»). Ноль пересобранных документов не
        публикуется: нулевой факт читался бы как «пересобирали и не вышло».
        """
        if not self.rebuilt:
            return ""
        return (
            f"; {REUSE_BYTES_REBUILD}:{self.rebuilt} — документов пересобрано из "
            f"сохранённых байтов (чанков {self.rebuilt_chunks}, "
            f"{self.rebuilt_seconds:.1f} с)"
        )


#: Токен видимого факта: документ **пересобран** из сохранённых байтов текущим
#: конвейером (экстрактор → чанкер) вместо переиспользования чужого chunk-set. Это не
#: ошибка: сеть не трогалась, а документ вошёл в корпус.
REUSE_BYTES_REBUILD = "reuse_bytes_rebuild"

#: Токен видимой причины: chunk-set не годится, а сохранённых байтов нет (нет файла
#: retention либо его хеш не совпал с записанным) — пересобрать нечем.
REUSE_BYTES_MISSING = "reuse_bytes_missing"

#: Токен видимого факта: пересборка дала **другой** ``content_hash``, чем записан в
#: ``document_version``. Это не «контент изменился» (сети не было) — сменился движок
#: извлечения; класс изменения — ``content_change``, и документ остаётся в корпусе.
REUSE_BYTES_REEXTRACT = "reuse_bytes_reextract"

#: Пояснение к :data:`REUSE_BYTES_MISSING` (операторский канал).
REUSE_BYTES_MISSING_TEXT = (
    "offline: chunk-set не годится для переиспользования, а сохранённых сырых байтов "
    "нет (нет файла retention либо его хеш не совпал с записанным) — пересобрать "
    "нечем, документ не вошёл в корпус (сеть не трогается)"
)


@dataclass(frozen=True)
class RebuiltDoc:
    """Документ, пересобранный из сохранённых байтов **текущим** конвейером.

    Один носитель результата для двух проходов без сети: ``frozen_source=bytes``
    (пере-извлечение по оси прогона) и фолбэк переиспользования (chunk-set отвергнут).
    Пока результат был словарём в теле ``frozen``, второе плечо пришлось бы писать
    второй копией, и копии разошлись бы молча.

    :param url: URL, которому принадлежат байты (адрес версии в состоянии).
    :param key: нормализованный ключ ``url_state``.
    :param doc_hash: ``content_hash`` **текущей** экстракции — идентичность документа.
    :param raw_path: файл сохранённых байтов (провенанс, не копия содержимого).
    :param extractor_token: ``extractor_version`` фактического движка (штамп адаптера).
    :param level_text: объявленный уровень извлечения (условие измерения).
    :param partial_fields: поля частичности извлечения (измерены сейчас, не из записи).
    :param chunks: чанки текущего чанкера в порядке ``chunk_no``.
    :param from_cache: ``ExtractedDoc`` взят из ``extract_cache`` (экстрактор не звался).
    :param extracted: экстрактор вызывался (даже если затем отказал).
    :param content_changed: ``doc_hash`` разошёлся с durable-версией (дрейф извлечения).
    :param seconds: время попытки (экстракция + нарезка).
    """

    url: str
    key: str
    doc_hash: str
    raw_path: Path
    extractor_token: str
    level_text: str
    partial_fields: dict[str, Any]
    chunks: list[Any]
    from_cache: bool = False
    extracted: bool = False
    content_changed: bool = False
    seconds: float = 0.0


@dataclass(frozen=True)
class BytesRebuildOutcome:
    """Итог попытки пересборки одного документа из сохранённых байтов.

    Три исхода различимы по построению: ``missing_bytes`` (пересобирать нечем),
    ``failed`` (байты есть, конвейер отказал — причина в ``errors``) и ``doc``
    (документ пересобран). Молчаливого «ничего не произошло» нет.
    """

    doc: RebuiltDoc | None = None
    missing_bytes: bool = False
    failed: bool = False
    cache_hit: bool = False
    extracted: bool = False
    seconds: float = 0.0


def _verified_raw_key(
    product_id: str, url: str, raw_path: Path, *, label: str, errors: list[str]
) -> str | None:
    """Ключ ``extract_cache`` из **содержимого** файла либо ``None`` (байтов нет).

    Retention называет файл ``{sha256 сырых байтов}.bin``, поэтому ``raw_path.stem`` —
    ожидаемый ключ, а :func:`_hash_file` — фактический. Расхождение имени и содержимого
    означает «сохранённых байтов нет» (файл подменён/битый), а не «контент изменился».
    """
    if not raw_path.is_file():
        return None
    try:
        raw_key = _hash_file(raw_path)
    except OSError as exc:
        errors.append(
            f"{product_id}/{url}: {label}: чтение retained-байтов {raw_path}: "
            f"{type(exc).__name__}: {exc}"
        )
        return None
    return raw_key if raw_key == raw_path.stem else None


def _level_extractor(
    *,
    product_id: str,
    url: str,
    label: str,
    errors: list[str],
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline,
    extractor: ExtractorPort,
    extractor_factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort]
    | None,
    level_extractors: dict[tuple[DocumentLevel, PdfPipeline], ExtractorPort],
) -> ExtractorPort:
    """Экстрактор **объявленного** уровня URL — тот же резолвер, что у живого пути.

    Экземпляр строится один раз на ключ ``(уровень, механизм)`` — как пул живого
    пути: построение docling заново на каждый документ стоило бы загрузки моделей по
    числу PDF (замер: 5 перезагрузок подряд), а не по числу условий. Уровень
    композиции (``full`` + ``structured``) — переданный экстрактор, без фабрики.
    Отказ фабрики виден в ``errors`` и не подменяет уровень молча.
    """
    if extractor_factory is None or (
        level is DocumentLevel.FULL and pdf_pipeline is PdfPipeline.STRUCTURED
    ):
        return extractor
    extractor_key = (level, pdf_pipeline)
    cached = level_extractors.get(extractor_key)
    if cached is not None:
        return cached
    try:
        built = extractor_factory(level, pdf_pipeline)
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        errors.append(
            f"{product_id}/{url}: {label}: extractor_factory({level.value}, "
            f"{pdf_pipeline.value}): {type(exc).__name__}: {exc} — "
            "понижен до переданного экстрактора"
        )
        return extractor
    level_extractors[extractor_key] = built
    return built


def rebuild_from_stored_bytes(
    product_id: str,
    url: str,
    *,
    raw_path: Path | None,
    prior: UrlStateRow | None,
    prior_doc: DocumentVersionRow | None,
    label: str,
    errors: list[str],
    extractor: ExtractorPort,
    chunker: ChunkerPort,
    extract_cache: ExtractionCache | None,
    fetch_plan_for: Callable[[str], FetchPlan],
    limits_for: Callable[[str], Any],
    extractor_factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort]
    | None = None,
    level_extractors: dict[tuple[DocumentLevel, PdfPipeline], ExtractorPort]
    | None = None,
) -> BytesRebuildOutcome:
    """Сохранённые байты → **текущий** экстрактор → **текущий** чанкер. Ноль запросов.

    Общий шаг двух проходов без сети: ``frozen_source=bytes`` (ось прогона) и фолбэк
    переиспользования, когда chunk-set отвергнут (чужая версия чанкера, пустой набор,
    I19-расхождение текста). Второй копии этого пути нет намеренно: копии расходятся
    молча, а цена расхождения — «тот же файл, другой текст» под тем же провенансом.

    **Ключ ``extract_cache`` — хеш сырых байтов** (``_hash_file`` = ``raw_path.stem``),
    а не версия чанкера: повторный проход в том же окружении обязан попадать в кэш и не
    платить конверсию дважды. Промах кэша — не ошибка.

    **Уровень и механизм PDF — условие измерения**, а не свойство «какой экстрактор
    случайно передали»: экстрактор выбирается per-URL через ``extractor_factory`` (тот
    же резолвер, что у живого пути). Иначе ``extractor_version`` назвал бы условие,
    которого в корпусе не было, а ``extract_cache`` получил бы второй ключ.

    **Маршрут — по байтам файла**, а не по URL/``Content-Type``: у retained-файла
    суффикс ``.bin``, заголовка ответа в этом прогоне нет. Иначе PDF по бессуффиксному
    URL ушёл бы в HTML-ветку и дал ложный дрейф.

    ``content_changed`` не отказ: смена текста при том же файле — ``content_change``
    (движок извлечения — провенанс, а не ось контракта), и решает вызывающий: ``frozen``
    называет это дрейфом измерения и отклоняет, фолбэк переиспользования принимает с
    отдельным видимым токеном.

    :returns: :class:`BytesRebuildOutcome`; ``doc`` заполнен только при успехе.
    """
    if raw_path is None:
        return BytesRebuildOutcome(missing_bytes=True)
    raw_key = _verified_raw_key(product_id, url, raw_path, label=label, errors=errors)
    if raw_key is None:
        return BytesRebuildOutcome(missing_bytes=True)

    started = time.perf_counter()
    plan = fetch_plan_for(url)
    limits = limits_for(url)
    level = limits.document_level
    level_text = _as_text(level)
    extractor_obj = _level_extractor(
        product_id=product_id,
        url=url,
        label=label,
        errors=errors,
        level=level,
        pdf_pipeline=limits.pdf_pipeline,
        extractor=extractor,
        extractor_factory=extractor_factory,
        level_extractors={} if level_extractors is None else level_extractors,
    )
    try:
        probe = probe_artifact(str(raw_path), product_id=product_id, url=url)
        route = (
            ExtractionRoute.DOCUMENT
            if probe.artifact.kind == CLASS_DOCUMENT
            else ExtractionRoute.AUTO
        )
    except Exception as exc:  # noqa: BLE001 — зонд не должен ронять продукт
        errors.append(
            f"{product_id}/{url}: {label}: probe: {type(exc).__name__}: {exc} — "
            "маршрут по URL (байты не классифицированы)"
        )
        route = ExtractionRoute.AUTO
    # ``fetched_at`` — **прежнее** наблюдение байтов, не «сейчас»: нового получения не
    # было. ``status_code``/``render_mode``/``lang`` не выдумываются.
    page = RawPage(
        url=url,
        content=b"",
        source_path=str(raw_path),
        fetched_at=str(
            (prior.last_checked_at if prior is not None else None)
            or (prior_doc.first_seen_at if prior_doc is not None else None)
            or ""
        ),
        fetch_mode=plan.fetch,
        status_code=None,
        content_type=None,
        engine=None,
    )
    extracted = None
    cache_hit = False
    if extract_cache is not None:
        version_token = _cache_extractor_version(extractor_obj, page, route)
        extracted = extract_cache.get(raw_key, version_token, level_text)
        if extracted is not None:
            cache_hit = True
    extracted_now = False
    if extracted is None:
        try:
            extracted = extractor_obj.extract(page, route)
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            errors.append(
                f"{product_id}/{url}: {label}: extract: {type(exc).__name__}: {exc}"
            )
            return BytesRebuildOutcome(
                failed=True,
                extracted=True,
                seconds=time.perf_counter() - started,
            )
        extracted_now = True
        if extract_cache is not None:
            extract_cache.put(extracted, level=level_text, content_hash=raw_key)
    if getattr(extracted, "error", None):
        errors.append(f"{product_id}/{url}: {label}: extract: {extracted.error}")
        return BytesRebuildOutcome(
            failed=True,
            extracted=extracted_now,
            cache_hit=cache_hit,
            seconds=time.perf_counter() - started,
        )
    doc_hash = str(getattr(extracted, "content_hash", "") or "")
    if not doc_hash:
        errors.append(
            f"I19: {product_id}/{url}: ExtractedDoc без content_hash — отклонён"
        )
        return BytesRebuildOutcome(
            failed=True,
            extracted=extracted_now,
            cache_hit=cache_hit,
            seconds=time.perf_counter() - started,
        )
    try:
        chunks = chunker.chunk(extracted, f"{product_id}:{doc_hash}")
    except Exception as exc:  # noqa: BLE001
        errors.append(
            f"{product_id}/{url}: {label}: chunk: {type(exc).__name__}: {exc}"
        )
        return BytesRebuildOutcome(
            failed=True,
            extracted=extracted_now,
            cache_hit=cache_hit,
            seconds=time.perf_counter() - started,
        )
    return BytesRebuildOutcome(
        doc=RebuiltDoc(
            url=url,
            key=_state_url_key(url) or url,
            doc_hash=doc_hash,
            raw_path=raw_path,
            extractor_token=str(getattr(extracted, "extractor_version", None) or ""),
            level_text=str(level_text or ""),
            partial_fields=_partial_fields(extracted),
            chunks=sorted(
                chunks or [], key=lambda chunk: getattr(chunk, "chunk_no", 0)
            ),
            from_cache=cache_hit,
            extracted=extracted_now,
            content_changed=bool(
                prior_doc is not None and doc_hash != prior_doc.content_hash
            ),
            seconds=time.perf_counter() - started,
        ),
        cache_hit=cache_hit,
        extracted=extracted_now,
        seconds=time.perf_counter() - started,
    )


def _persist_rebuilt_doc(
    product_id: str,
    canonical_url: str,
    rebuilt: RebuiltDoc,
    *,
    state: ProductStatePort | None,
    errors: list[str],
    embed_model_id: str,
    store: Any,
    ingest: ProductIngest,
    seen_docs: dict[str, str],
    seen_chunk_ids: set[str],
    duplicates: dict[str, int],
    doc_counters: dict[str, dict[str, int]],
    persist_docs: Callable[..., None],
) -> int | None:
    """Пишет пересобранный документ в корпус; ``None`` — тот же ``doc_hash`` уже вошёл.

    Единственная точка записи пересобранного документа: ``frozen`` и фолбэк
    переиспользования идут сюда, поэтому «что считается документом корпуса» у них не
    может разойтись. ``reuse_kind=REUSED``: байты в этом прогоне не получались, и
    провенанс — прежнее наблюдение (I2d), а не ответ сервера. ``url_state`` не
    переписывается: наблюдения не было.

    :returns: число принятых чанков либо ``None`` (дедуп по ``doc_hash``).
    """
    doc_hash = rebuilt.doc_hash
    if doc_hash in seen_docs:
        duplicates[product_id] = duplicates.get(product_id, 0) + 1
        return None
    seen_docs[doc_hash] = canonical_url
    document_id = f"{product_id}:{doc_hash}"
    dv_id = doc_version_id(
        product_id, doc_hash, rebuilt.extractor_token, rebuilt.level_text
    )
    doc = CorpusDoc(
        doc_id=document_id,
        product_id=product_id,
        url=canonical_url,
        # Получения не было: ``fetched_at``/``render_mode``/``lang`` — NULL, а не
        # «сейчас»/«http»/дефолт. Провенанс записи — ``reuse_kind=reused``.
        fetched_at=None,
        content_hash=doc_hash,
        raw_path=str(rebuilt.raw_path),
        extractor_version=rebuilt.extractor_token or None,
        render_mode=None,
        lang=None,
        http_status=None,
        reuse_kind=ReuseKind.REUSED,
        **rebuilt.partial_fields,
    )
    ingest.docs.append(doc)
    persist_docs(store, [doc], errors, versions_by_doc={document_id: dv_id})
    accepted = _accept_chunk_candidates(
        product_id,
        document_id,
        dv_id,
        [
            (
                int(getattr(chunk, "chunk_no", 0)),
                getattr(chunk, "text", "") or "",
                getattr(chunk, "heading_path", "") or "",
                getattr(chunk, "context_prefix", "") or "",
                str(getattr(chunk, "content_hash", "") or "").strip(),
                getattr(chunk, "char_start", None),
                getattr(chunk, "char_end", None),
            )
            for chunk in rebuilt.chunks
        ],
        ingest=ingest,
        seen_chunk_ids=seen_chunk_ids,
        errors=errors,
        url=canonical_url,
        lang=None,
        embed_model_id=embed_model_id,
    )
    count_doc(product_id, reused=True, state=state, doc_counters=doc_counters)
    return len(accepted)



def offline_reuse_product(
    product_id: str,
    roots: Sequence[str],
    items: Sequence[IncrementalItem],
    *,
    state: ProductStatePort | None,
    errors: list[str],
    chunker_version: str,
    embed_model_id: str,
    store: Any,
    ingest: ProductIngest,
    seen_docs: dict[str, str],
    seen_chunk_ids: set[str],
    duplicates: dict[str, int],
    doc_counters: dict[str, dict[str, int]],
    partial_unknown_products: dict[str, list[str]],
    persist_docs: Callable[..., None],
    extractor: ExtractorPort,
    chunker: ChunkerPort,
    extract_cache: ExtractionCache | None,
    fetch_plan_for: Callable[[str], FetchPlan],
    limits_for: Callable[[str], Any],
    extractor_factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort]
    | None = None,
    label: str = "offline",
    skip_urls: Sequence[str] = (),
) -> OfflineReuseReport:
    """Оффлайн-проход продукта: корпус из durable-субстрата, **ноль сетевых вызовов**.

    Политика ``offline`` (``--refresh offline``) означает «наблюдать нечем и не будем»:
    документ и его chunk-set берутся из ``state.db`` (``url_state`` →
    ``document_version`` → ``chunk_meta``/``chunk_store``), экстрактор и чанкер **не
    вызываются**, HTTP-статус — ``NULL``, ``reuse_kind=reused``. Это тот же
    переиспользованный набор, что у ``304`` (:func:`reuse_not_modified`), но без самого
    ``304``: сервер не спрашивался.

    **Почему отдельный проход, а не дренаж с ревалидационными seed'ами.** Дренаж — это
    сеть по построению: seed-фетч, sitemap/feed-развёртка, BFS. Оффлайн-элемент не имеет
    ни одного из этих шагов, поэтому идти через ``root_lifecycle`` значило бы провести
    «без сети» через код, единственное назначение которого — сеть. Здесь нет ни
    ``frontier``, ни ``limits``, ни ``FetchPlan``: нет и места, где сеть могла бы
    случиться.

    **Авторский корень.** Корень, известный ``url_state``, переиспользуется как любой
    другой URL: «без сети» не может начинаться с GET корня. Корень, которого в состоянии
    нет, оффлайн наблюдать нечем — это видимая ошибка с продуктом и URL
    (:data:`OFFLINE_UNKNOWN_ROOT_TEXT`), а не тихий пропуск и не поход в сеть: пустой
    корпус без причины читался бы как «у продукта ничего нет».

    **Известный URL без пригодной версии.** Нет ``document_version`` (или она
    ``superseded``) — пересобирать не из чего: документ в корпус не попадает, причина
    называется :data:`OFFLINE_NO_VERSION_TEXT`, счётчик ``no_version``. Chunk-set
    отвергнут ``reuse_plan`` (несовпадение версии чанкера, текст не сошёлся с
    ``content_hash``, пустой набор) — причина записывается самим ``reuse_plan``, а
    следствие зависит от **байтов**:

    * файл сохранённых байтов на месте и его хеш совпал с именем — документ
      **пересобирается** текущим конвейером (:func:`rebuild_from_stored_bytes`:
      per-URL экстрактор → текущий чанкер) и входит в корпус; это факт
      ``rebuilt``/``reuse_bytes_rebuild``, а **не** ошибка: сеть не трогалась, а
      chunk-set чужой версии по-прежнему не годится как есть (I19 не ослаблен);
    * байтов нет (``raw_path`` пуст, файла нет, хеш не совпал) — пересобрать нечем:
      :data:`REUSE_BYTES_MISSING_TEXT`, счётчик ``no_bytes`` и ``errors``.

    Три исхода различимы потому, что это три разных факта: «переиспользовано»,
    «пересобрано» и «пересобрать нечем». Первый не зовёт приборов, второй зовёт, третий
    не может. Молчаливого пропуска нет ни в одном.

    **Пересборка — не наблюдение.** ``url_state`` не переписывается (наблюдения не
    было), а датированный носитель ``❌`` — прежнее durable-наблюдение URL
    (``corpus_artifact.observed_at`` из ``url_state``, I2d). Если текущий экстрактор дал
    **другой** ``content_hash``, это ``content_change`` (движок — провенанс, не ось
    контракта): документ остаётся в корпусе, а дрейф назван
    :data:`REUSE_BYTES_REEXTRACT`. ``frozen`` тот же дрейф отклоняет: там он означает
    расхождение измерения, здесь — что пересборка дала новый текст того же файла.

    **Чего здесь нет намеренно.** Нет записи в ``url_state``: наблюдения не было, и
    ``reuse_kind``/``last_http_status`` прошлого цикла переписывать нельзя — иначе
    следующий прогон прочитал бы «reused» как последнюю классификацию этого URL. Нет
    элемента сеанса краула: ``crawl_session_items`` — учёт фетчей, а фетчей не было.
    Отсюда следствие для покрытия: ``observed_sources`` остаётся нулём, потому что
    наблюдений **в этом прогоне** нет, и ``complete`` не может стать ``True``
    (``derive_corpus_coverage``: условие «ни одного наблюдения»). Переиспользованный
    документ честно называется переиспользованным, а не наблюдённым.

    :returns: :class:`OfflineReuseReport` — что переиспользовано и что не удалось.

    ``label`` называет **повод** прохода: ``offline`` — политика наблюдения прогона
    целиком; ``crawl_policy`` — корень, которому политика краула запретила сеть
    (``network_denied`` ‖ ``on_request`` без запроса). Проход один и тот же («продукт →
    корень»), различается провенанс: переиспользование по политике не означает, что
    оператор просил ``offline``.

    **``skip_urls`` — строка корпуса одна на URL (E.6, решение оркестратора 05.11.2026).**
    Идентичность корпусной строки для этого вопроса — **URL**, а не ``doc_hash``: если
    URL в этом прогоне уже имеет строку корпуса (живое наблюдение 200/304, ручной
    артефакт или документ предыдущего прохода переиспользования), его копия не входит в
    работу вовсе. Иначе получалось бы одно из двух: при том же тексте — строка
    оставалась бы ``reused``/``http_status=NULL``, то есть врала бы о провенансе
    (наблюдение было); при изменённом — рядом с наблюдённой встала бы устаревшая версия
    того же URL как кандидат в доказательства (I19). Пропуск называется отдельным
    счётчиком ``skipped_present``, а не выдаётся за «переиспользовано 0»: это третий
    факт, а не отсутствие копии.
    """
    report_reused = 0
    report_chunks = 0
    report_rebuilt = 0
    report_rebuilt_chunks = 0
    report_rebuilt_seconds = 0.0
    no_version: list[str] = []
    no_bytes: list[str] = []
    unknown_roots: list[str] = []
    touched: list[str] = []
    skipped_present: list[str] = []
    seen_urls: set[str] = set()
    capture_pairs: list[tuple[str, str, str]] = []
    #: Экстракторы по ключу ``(уровень, механизм PDF)`` — один на условие прохода
    #: (тот же резолвер, что у ``frozen`` и живого пути).
    level_extractors: dict[tuple[DocumentLevel, PdfPipeline], ExtractorPort] = {}
    #: Пропуск — по нормализованному ключу ``url_state``: тот же, которым проход
    #: адресует строку состояния. Ненормализуемый вход молча не пропускается: он
    #: попадёт в собственную видимую ошибку ниже.
    skip_keys = {
        key for url in skip_urls if (key := _state_url_key(url)) is not None
    }

    # Порядок: авторские корни (порядок входа) → известные URL (порядок планировщика).
    # Корни идут первыми, потому что они — авторское решение: если корень недоступен
    # оффлайн, это первое, что должен увидеть оператор.
    ordered: list[str] = [*roots, *(item.url for item in items)]

    def _reason(text: str) -> str:
        """Причина с меткой повода: ``offline`` (прогон) ‖ ``crawl_policy`` (корень).

        Один проход — два повода; текст ошибки обязан называть тот, который случился,
        иначе оператор искал бы «--refresh offline» там, где сеть запретил режим корня.
        """
        return text if label == "offline" else text.replace("offline:", f"{label}:", 1)

    for url in ordered:
        key = _state_url_key(url)
        if key is None:
            errors.append(
                f"{product_id}/{url}: {label}: URL не нормализуется — "
                "переиспользование невозможно (состояние по нему не ведётся)"
            )
            continue
        if key in seen_urls:
            continue
        if key in skip_keys:
            # Строка корпуса этого прогона уже есть (живое наблюдение или документ
            # предыдущего прохода): вторая строка на тот же URL не появляется.
            skipped_present.append(url)
            continue
        seen_urls.add(key)
        touched.append(url)

        # Версия берётся из durable-субстрата **напрямую** (:func:`reuse_plan`), а не через
        # :func:`reuse_not_modified`: у того классификация ``304``-специфична
        # (``classify_reuse(not_modified=True)``), а здесь сервер не отвечал вовсе —
        # провенанс не должен утверждать контакт, которого не было. Сам переиспользуемый
        # набор (документ + верифицированный chunk-set) — тот же примитив.
        prior = _prior_url_state(product_id, key, url, state=state, errors=errors)
        if prior is None:
            unknown_roots.append(url)
            errors.append(f"{product_id}/{url}: {_reason(OFFLINE_UNKNOWN_ROOT_TEXT)}")
            continue
        prior_doc = _prior_document_version(
            prior, product_id, url, state=state, errors=errors
        )
        if prior_doc is None or prior_doc.status != _DOC_STATUS_ACTIVE:
            # Нет версии (либо она вытеснена живым прогоном): пересобирать не из чего,
            # а воскрешать ``superseded`` значило бы вернуть в корпус вытесненное.
            no_version.append(url)
            errors.append(f"{product_id}/{url}: {_reason(OFFLINE_NO_VERSION_TEXT)}")
            continue
        #: Граница ошибок ``reuse_plan``: его причина («повторная нарезка») — объяснение
        #: отказа, а не сбой прогона. Успешная пересборка делает её неактуальной и не
        #: оставляет в ``errors`` (пересборка — не ошибка); при отказе причина остаётся.
        errors_before_reuse = len(errors)
        reuse = reuse_plan(
            product_id,
            url,
            prior_doc,
            state=state,
            errors=errors,
            chunker_version=chunker_version,
        )
        if reuse is None:
            # Второе плечо: chunk-set не годится (версия чанкера/пустой набор/I19), но
            # сохранённые байты могут дать документ **текущим** конвейером. Причина
            # отказа уже записана ``reuse_plan``/``load_reused_chunks`` — здесь
            # называется следствие для корпуса, и оно зависит от байтов.
            raw_path = Path(prior_doc.raw_path) if prior_doc.raw_path else None
            outcome = rebuild_from_stored_bytes(
                product_id,
                url,
                raw_path=raw_path,
                prior=prior,
                prior_doc=prior_doc,
                label=label,
                errors=errors,
                extractor=extractor,
                chunker=chunker,
                extract_cache=extract_cache,
                fetch_plan_for=fetch_plan_for,
                limits_for=limits_for,
                extractor_factory=extractor_factory,
                level_extractors=level_extractors,
            )
            if outcome.missing_bytes:
                # Пересобрать нечем: файла байтов нет либо его хеш не совпал с именем.
                no_bytes.append(url)
                errors.append(
                    f"{product_id}/{url}: {_reason(REUSE_BYTES_MISSING_TEXT)}"
                )
                continue
            if outcome.doc is None:
                # Байты есть, конвейер отказал — причина уже в ``errors``; документа нет.
                continue
            rebuilt_doc = outcome.doc
            # Пересборка состоялась: причина отказа ``reuse_plan`` больше не описывает
            # исход документа. Снимаем **только** её срез — ошибки конвейера,
            # персистенции и чанк-гейта ниже остаются.
            del errors[errors_before_reuse:]
            if rebuilt_doc.content_changed:
                # Тот же файл, другой текст: сменился движок извлечения, а не контент
                # (сети не было). Класс изменения — ``content_change``; документ
                # остаётся в корпусе, но факт назван, а не спрятан.
                errors.append(
                    f"{product_id}/{url}: {REUSE_BYTES_REEXTRACT}: content_hash "
                    f"пересборки {rebuilt_doc.doc_hash} != записанного "
                    f"{prior_doc.content_hash} — тот же файл, другой текст текущего "
                    "движка (наблюдения не было)"
                )
            report_rebuilt_seconds += outcome.seconds
            # Durable-группа: алиас тоже участвует в дедупе (как у переиспользования).
            capture_pairs.append((rebuilt_doc.doc_hash, url, key))
            rebuilt_accepted = _persist_rebuilt_doc(
                product_id,
                url,
                rebuilt_doc,
                state=state,
                errors=errors,
                embed_model_id=embed_model_id,
                store=store,
                ingest=ingest,
                seen_docs=seen_docs,
                seen_chunk_ids=seen_chunk_ids,
                duplicates=duplicates,
                doc_counters=doc_counters,
                persist_docs=persist_docs,
            )
            if rebuilt_accepted is None:
                # Тот же ``doc_hash`` уже вошёл (дедуп): строк не плодим.
                continue
            report_rebuilt += 1
            report_rebuilt_chunks += rebuilt_accepted
            continue

        doc_hash = str(reuse.doc_version.content_hash or "")
        if not doc_hash:
            # I19: версия без хеша не адресует ни документ, ни чанки — переиспользовать
            # её значило бы выдумать идентичность контента.
            errors.append(
                f"I19: {product_id}/{url}: durable-версия без content_hash — "
                f"переиспользование отклонено ({label})"
            )
            no_version.append(url)
            continue
        document_id = f"{product_id}:{doc_hash}"
        # Durable-группа: алиас тоже участвует в дедупе, и запись снятия обязана знать,
        # что этот URL — тот же документ (иначе ``frozen`` выбрал бы адрес наугад).
        capture_pairs.append((doc_hash, url, key))
        if doc_hash in seen_docs:
            duplicates[product_id] = duplicates.get(product_id, 0) + 1
            continue
        seen_docs[doc_hash] = url

        extractor_token = str(reuse.doc_version.extractor_version or "")
        level_text = str(reuse.doc_version.level or "")
        dv_id = doc_version_id(product_id, doc_hash, extractor_token, level_text)
        partial_fields, partial_known = _reused_partial_fields(reuse.doc_version)
        if not partial_known:
            # v5: признак частичности в версии не записан — «не измерено», и это
            # называется отдельно (строка продукта), а не выдаётся за «прочитано всё».
            partial_unknown_products.setdefault(product_id, []).append(url)
        doc = CorpusDoc(
            doc_id=document_id,
            product_id=product_id,
            url=url,
            # Получения не было: ``fetched_at``/``render_mode``/``lang`` — NULL, а не
            # «сейчас»/«http»/дефолт. Провенанс записи — ``reuse_kind=reused``.
            fetched_at=None,
            content_hash=doc_hash,
            # Путь retained-байтов — из durable-версии: это байты, чей хеш и есть
            # идентичность документа (I19). Новых байтов оффлайн не появляется.
            raw_path=reuse.doc_version.raw_path,
            extractor_version=extractor_token or None,
            render_mode=None,
            lang=None,
            http_status=None,
            reuse_kind=ReuseKind.REUSED,
            **partial_fields,
        )
        ingest.docs.append(doc)
        persist_docs(store, [doc], errors, versions_by_doc={document_id: dv_id})
        accepted = _accept_chunk_candidates(
            product_id,
            document_id,
            dv_id,
            [
                (
                    chunk.chunk_no,
                    chunk.text,
                    chunk.heading_path,
                    chunk.context_prefix,
                    chunk.content_hash,
                    chunk.char_start,
                    chunk.char_end,
                )
                for chunk in reuse.chunks
            ],
            ingest=ingest,
            seen_chunk_ids=seen_chunk_ids,
            errors=errors,
            url=url,
            lang=None,
            embed_model_id=embed_model_id,
        )
        count_doc(product_id, reused=True, state=state, doc_counters=doc_counters)
        report_reused += 1
        report_chunks += len(accepted)

    if no_bytes:
        # Машинный токен с числом — в тот же канал видимых причин, что у ``frozen``:
        # «сколько пересобрать нечем» важнее «есть ли причина», а ``failed`` здесь нет
        # (I4): прогон состоялся, корпус просто неполон.
        errors.append(
            f"{product_id}: {REUSE_BYTES_MISSING}:{len(no_bytes)} — сохранённых байтов "
            f"нет у {len(no_bytes)} известных URL, пересобрать нечем ({label})"
        )
    return OfflineReuseReport(
        reused=report_reused,
        chunks=report_chunks,
        rebuilt=report_rebuilt,
        rebuilt_chunks=report_rebuilt_chunks,
        rebuilt_seconds=report_rebuilt_seconds,
        no_bytes=tuple(no_bytes),
        no_version=tuple(no_version),
        unknown_roots=tuple(unknown_roots),
        urls=tuple(touched),
        skipped_present=tuple(skipped_present),
        capture_docs=tuple(
            CapturedDocRow(
                doc_hash=doc_hash,
                url=url,
                normalized_url=key,
                reuse_kind=ReuseKind.REUSED.value,
            )
            for doc_hash, url, key in capture_pairs
        ),
        label=label,
    )


#: Статус версии документа, которую ``frozen`` вправе пере-извлечь: только ``active``.
#: ``superseded`` — версия, которую живой прогон заменил; пере-извлекать её значило бы
#: вернуть в корпус то, что уже вытеснено (та же дисциплина, что у ``offline``).
_DOC_STATUS_ACTIVE = "active"

#: Токен видимой причины: у известного URL нет сохранённых байтов (или они не те, что
#: записаны версией). С числом — ``stored_bytes_missing:{n}``: «сколько» важнее «есть ли».
FROZEN_STORED_BYTES_MISSING = "stored_bytes_missing"

#: Токен видимой причины: авторский корень не наблюдался (нет строки ``url_state``).
#: Тот же факт, что у ``offline``, но названный в провенансе ``frozen``.
FROZEN_UNKNOWN_ROOTS = "unknown_roots"

#: Токен дрейфа пере-извлечения: текущий движок дал **другой** ``content_hash`` на тех же
#: байтах, чем записан в ``document_version``. Это не «контент изменился» (сети не было) —
#: иначе новый ``doc_hash`` дал бы ложный ``document_id``, ложный дедуп и другой
#: ``doc_set_digest``, а причина выглядела бы наблюдением.
FROZEN_REEXTRACT_MISMATCH = "frozen_reextract_mismatch"

#: Токены воспроизведения **контейнеров** (план 19 §E.7). У контейнера нет версии
#: документа, поэтому проход без сети находил его в ``no_version`` и молча терял членов.
#: ``frozen_container_replayed:{n}`` — сколько контейнеров воспроизведено из сохранённых
#: байтов; ``frozen_container_unreadable`` — байты есть, но индекс не прочитан;
#: ``frozen_container_empty`` — спуск исполнен, а в корпус не дошёл ни один член. Все три —
#: видимые причины/факты, а не ``failed`` (I4): прогон состоялся, корпус назван.
FROZEN_CONTAINER_REPLAYED = "frozen_container_replayed"
FROZEN_CONTAINER_UNREADABLE = "frozen_container_unreadable"
FROZEN_CONTAINER_EMPTY = "frozen_container_empty"

#: Пояснения к причинам ``frozen`` (операторский канал). Токен отдельно, текст отдельно:
#: тесты и оператор читают токен, а формулировка не должна незаметно менять смысл.
FROZEN_UNKNOWN_ROOT_TEXT = (
    "frozen: URL отсутствует в durable-состоянии (url_state) — сохранённых байтов нет, "
    "а сети в этой политике нет. Прогоните --refresh auto/revalidate, чтобы получить "
    "документ"
)
FROZEN_NO_VERSION_TEXT = (
    "frozen: у URL нет активной версии документа — неизвестно, какие байты сохранены; "
    "документ не вошёл в корпус (сеть не трогается)"
)
FROZEN_STORED_BYTES_MISSING_TEXT = (
    "frozen: сохранённые сырые байты недоступны (нет файла retention либо его хеш не "
    "совпал с записанным content_hash) — пере-извлечь нечего (сеть не трогается)"
)


@dataclass(frozen=True)
class FrozenBytesReport:
    """Итог ``frozen_source=bytes``: что пере-извлечено и что не удалось (план 19 §P3).

    Все три «не удалось» — **видимые** причины, а не ``failed`` (I4): прогон состоялся,
    корпус просто неполон, и это называется токеном причины.

    :param docs: документов вошло в корпус.
    :param chunks: чанков принято (сумма по документам).
    :param reextracted: документов, для которых экстрактор **вызывался** (без кэша).
    :param cache_hits: документов, взятых из ``extract_cache`` (повторный ``frozen`` в том
        же окружении обязан попадать в кэш — ключ там хеш сырых байтов, I19).
    :param unknown_roots: URL/корни, которых нет в ``url_state``.
    :param stored_bytes_missing: известные URL без пригодных сохранённых байтов.
    :param no_version: известные URL без активной версии документа.
    :param reextract_mismatch: URL, чей пере-извлечённый ``content_hash`` не совпал с
        записанным (дрейф пере-извлечения, а не смена контента).
    :param groups: durable-группы дедупа, которые проход использовал:
        ``(doc_hash, (url, ...))`` — кто был алиасом одного документа. Пишутся в запись
        снятия, чтобы следующий ``frozen`` выбрал тот же канонический адрес без
        ``crawl_session_items``.
    :param containers: сколько контейнеров воспроизведено из сохранённых байтов (их
        члены входят в ``docs``/``chunks``, а не считаются отдельным корпусом).
    :param container_docs: документов пришло **из членов** контейнеров.
    :param container_chunks: чанков пришло из членов контейнеров.
    :param container_unreadable: URL контейнеров, чьи сохранённые байты не прочитаны.
    :param container_empty: URL контейнеров, чей спуск не дал ни одного документа.
    :param urls: URL, для которых сеть не трогалась (порядок прохода).
    """

    docs: int = 0
    chunks: int = 0
    reextracted: int = 0
    cache_hits: int = 0
    unknown_roots: tuple[str, ...] = ()
    stored_bytes_missing: tuple[str, ...] = ()
    no_version: tuple[str, ...] = ()
    reextract_mismatch: tuple[str, ...] = ()
    containers: int = 0
    container_docs: int = 0
    container_chunks: int = 0
    container_unreadable: tuple[str, ...] = ()
    container_empty: tuple[str, ...] = ()
    groups: tuple[tuple[str, tuple[str, ...]], ...] = ()
    urls: tuple[str, ...] = ()
    #: Плоские durable-группы для записи снятия: ``(doc_hash, url, normalized_url)``.
    #: ``groups`` — вложенное представление для оператора; здесь — адреса строк таблицы.
    capture_docs: tuple[CapturedDocRow, ...] = ()
    label: str = "frozen"

    @property
    def stored_bytes_missing_token(self) -> str:
        """Машинный токен причины с числом: ``stored_bytes_missing:{n}``."""
        return f"{FROZEN_STORED_BYTES_MISSING}:{len(self.stored_bytes_missing)}"

    def note(self, product_id: str) -> str:
        """Операторская строка продукта: что снято и чего не хватило."""
        return (
            f"corpus: {self.label} {product_id} — пере-извлечено документов {self.docs} "
            f"(чанков {self.chunks}, экстракций {self.reextracted}, из кэша "
            f"{self.cache_hits}), контейнеров {self.containers} "
            f"(из них документов {self.container_docs}, чанков {self.container_chunks}, "
            f"не прочитано {len(self.container_unreadable)}, пусто "
            f"{len(self.container_empty)}), нет байтов {len(self.stored_bytes_missing)}, "
            f"корней вне состояния {len(self.unknown_roots)}, без версии "
            f"{len(self.no_version)}, дрейф пере-извлечения "
            f"{len(self.reextract_mismatch)}; "
            "сеть не трогалась, наблюдений этого прогона нет "
            "(❌ — только с датой носителя, I2d)"
        )


def frozen_bytes_product(
    product_id: str,
    roots: Sequence[str],
    items: Sequence[IncrementalItem],
    *,
    state: ProductStatePort | None,
    errors: list[str],
    chunker_version: str,
    embed_model_id: str,
    store: Any,
    ingest: ProductIngest,
    seen_docs: dict[str, str],
    seen_chunk_ids: set[str],
    duplicates: dict[str, int],
    doc_counters: dict[str, dict[str, int]],
    partial_unknown_products: dict[str, list[str]],
    persist_docs: Callable[..., None],
    extractor: ExtractorPort,
    chunker: ChunkerPort,
    extract_cache: ExtractionCache | None,
    fetch_plan_for: Callable[[str], FetchPlan],
    limits_for: Callable[[str], Any],
    extractor_factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort]
    | None = None,
    container_replay: Callable[..., Any] | None = None,
    label: str = "frozen",
) -> FrozenBytesReport:
    """``frozen_source=bytes``: корпус из ``corpus_raw`` → **текущая** экстракция (P3).

    Чем отличается от ``offline`` (тот же ноль запросов): ``offline`` берёт готовый
    chunk-set из ``state.db`` и отказывает при смене версии чанкера; здесь берутся
    сохранённые **байты** и проходят текущий конвейер (экстрактор → чанкер). Поэтому
    ``frozen`` измеряет новый стек на старом корпусе, а ``offline`` — нет.

    **Ключ ``extract_cache`` — хеш сырых байтов** (``_hash_file`` = ``raw_path.stem``):
    ключ не зависит от версии чанкера, поэтому повторный ``frozen`` в том же окружении
    попадает в кэш и не платит экстракцией дважды. Промах кэша — не ошибка: текущий
    экстрактор вызывается, а расхождение его ``content_hash`` с записанным в
    ``document_version`` называется :data:`FROZEN_REEXTRACT_MISMATCH` (дрейф
    пере-извлечения), а **не** «контент изменился»: сети не было, наблюдения не было.

    **Контейнеры воспроизводятся вместе с членами (план 19 §E.7).** У контейнера нет
    версии документа (``document_version`` описывает извлечённый текст), поэтому URL
    архива попадал в ``no_version``, а его члены приходили только из живого
    ``ArchiveDescentWalker``. Теперь путь и хеш ретейнённых байтов durable
    (``container_state``, STATE 10), и проход без сети исполняет **тот же** спуск из тех
    же байтов: ``defer_member=None``/``window=1`` (пула и очереди здесь нет), провенанс —
    в списки фазы и store, ``crawl_session_items`` не пишутся. Члены входят в ``docs``/
    ``chunks`` отчёта: это документы корпуса, а не отдельная категория.

    **Durable-состояние не переписывается.** ``url_state``/``document_version`` не
    трогаются: ``frozen`` не наблюдал сервер, и объявить пере-извлечённую версию
    «последней» значило бы дать следующему живому прогону переиспользовать версию,
    которую он не видел (та же дисциплина, что у ``offline``). Элементы
    ``crawl_session_items`` тоже не пишутся — поэтому дедуп опирается на durable-группы
    прошлой записи снятия, а не на ``min(url)`` по элементам сеанса.

    **Уровень — условие измерения.** Экстрактор выбирается per-URL через
    ``extractor_factory(level, pdf_pipeline)`` (тот же резолвер, что у живого пути):
    иначе пере-извлечение идёт уровнем композиции, ``extractor_version`` называет
    условие, которого в корпусе не было, а ``extract_cache`` получает второй ключ —
    «дрейф» там, где движок был тот же, а настройка другая.

    **Порядок и адреса.** Все кандидаты известны до экстракции; документы группируются по
    ``content_hash``, канонический адрес группы — ``min(url)`` по всем URL группы (включая
    алиасы прошлого снятия). Поэтому результат не зависит от порядка готовности и от
    ``host_concurrency``: проход последователен, а адрес выбирается запросом, а не гонкой.

    :returns: :class:`FrozenBytesReport` — документы, чанки, причины и группы дедупа.
    """
    #: Накопленные кандидаты: один на URL, ещё без адреса документа.
    collected: list[RebuiltDoc] = []
    unknown_roots: list[str] = []
    stored_bytes_missing: list[str] = []
    no_version: list[str] = []
    reextract_mismatch: list[str] = []
    cache_hits = 0
    reextracted = 0
    touched: list[str] = []
    #: Экстракторы по ключу ``(уровень, механизм PDF)`` — один на условие прохода.
    level_extractors: dict[tuple[DocumentLevel, PdfPipeline], ExtractorPort] = {}
    seen_urls: set[str] = set()
    #: План 19 §E.7: контейнеры, воспроизведённые из сохранённых байтов, и их вклад в
    #: корпус. Члены входят в ``docs``/``chunks`` отчёта — это документы корпуса, а не
    #: отдельная категория.
    containers_replayed = 0
    container_docs = 0
    container_chunks = 0
    container_unreadable: list[str] = []
    container_empty: list[str] = []

    #: Durable-группы прошлого снятия: ``doc_hash → urls``. Нужны ровно за тем, чтобы
    #: следующий ``frozen`` видел, кто был алиасом одного документа
    #: (``crawl_session_items`` им не пишется).
    prior_urls_by_hash: dict[str, list[str]] = {}
    if state is not None:
        try:
            prior_capture = state.last_corpus_capture(product_id)
        except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
            errors.append(
                f"{product_id}: frozen: last_corpus_capture: {type(exc).__name__}: {exc}"
            )
            prior_capture = None
        for captured_doc in () if prior_capture is None else prior_capture.docs:
            prior_urls_by_hash.setdefault(captured_doc.doc_hash, []).append(
                captured_doc.url
            )

    def _reason(text: str) -> str:
        """Причина с меткой прохода: текст ``offline``-семейства получает свою метку."""
        return text if label == "offline" else text.replace("offline:", f"{label}:", 1)

    def _extractor_at(
        level: DocumentLevel, pdf_pipeline: PdfPipeline, url: str
    ) -> ExtractorPort:
        """Экстрактор объявленного уровня URL — общий резолвер с ``reuse``-фолбэком."""
        return _level_extractor(
            product_id=product_id,
            url=url,
            label=label,
            errors=errors,
            level=level,
            pdf_pipeline=pdf_pipeline,
            extractor=extractor,
            extractor_factory=extractor_factory,
            level_extractors=level_extractors,
        )

    def _replay_container(
        container: ContainerStateRow, prior: UrlStateRow | None, url: str
    ) -> None:
        """Воспроизвести контейнер из сохранённых байтов (план 19 §E.7), inline.

        Контейнер не имеет версии документа, поэтому до STATE 10 проход без сети видел
        его только в ``no_version`` и молча терял членов (замер стенда: ``auto`` 9
        документов/597 чанков, ``frozen`` 7/326 — не хватало 2 членов/271 чанка). Теперь
        путь и хеш байтов durable (``container_state``), и спуск исполняется из **тех же**
        байтов тем же обходом, что у живого пути: сеть не нужна, ``crawl_session_items``
        не пишутся, провенанс идёт в списки фазы и в store.

        Верификация — от содержимого файла (``_hash_file`` против записанного хеша
        архива): подменённый/битый файл — ``stored_bytes_missing``, а не «контейнер без
        членов». Нечитаемый индекс — токен ``frozen_container_unreadable``; исполненный
        спуск без единого документа — ``frozen_container_empty``. Оба — видимые причины.
        """
        nonlocal containers_replayed, container_docs, container_chunks
        raw_path = Path(container.raw_path) if container.raw_path else None
        raw_key: str | None = None
        if raw_path is not None and raw_path.is_file():
            try:
                raw_key = _hash_file(raw_path)
            except OSError as exc:
                errors.append(
                    f"{product_id}/{url}: {label}: чтение retained-байтов контейнера "
                    f"{raw_path}: {type(exc).__name__}: {exc}"
                )
            else:
                if raw_key != container.archive_content_hash:
                    raw_key = None
        if raw_path is None or raw_key is None:
            stored_bytes_missing.append(url)
            errors.append(
                f"{product_id}/{url}: {_reason(FROZEN_STORED_BYTES_MISSING_TEXT)}"
            )
            return
        limits = limits_for(url)
        level = limits.document_level
        extractor_obj = _extractor_at(level, limits.pdf_pipeline, url)
        # ``fetched_at`` — прежнее наблюдение байтов: нового получения не было.
        fetched_at = str(
            (prior.last_checked_at if prior is not None else None)
            or container.last_seen_at
            or ""
        )
        if container_replay is None:
            container_unreadable.append(url)
            errors.append(
                f"{product_id}/{url}: {FROZEN_CONTAINER_UNREADABLE}: спуск в контейнер не "
                "подключён фазой — члены сохранённых байтов не воспроизведены"
            )
            return
        try:
            outcome = container_replay(
                url,
                raw_path,
                level=level,
                pdf_pipeline=limits.pdf_pipeline,
                extractor=extractor_obj,
                fetched_at=fetched_at,
                limits=limits,
            )
        except Exception as exc:  # noqa: BLE001 — отказ спуска не роняет продукт
            container_unreadable.append(url)
            errors.append(
                f"{product_id}/{url}: {FROZEN_CONTAINER_UNREADABLE}: спуск из сохранённых "
                f"байтов отказал: {type(exc).__name__}: {exc}"
            )
            return
        status = str(getattr(outcome, "status", "") or "")
        if status == ArchiveDescentStatus.UNREADABLE.value:
            container_unreadable.append(url)
            errors.append(
                f"{product_id}/{url}: {FROZEN_CONTAINER_UNREADABLE}: сохранённые байты "
                f"контейнера не прочитаны ({getattr(outcome, 'reason', None)}) — члены не "
                "воспроизведены"
            )
            return
        containers_replayed += 1
        outcome_docs = int(getattr(outcome, "docs", 0) or 0)
        container_docs += outcome_docs
        container_chunks += int(getattr(outcome, "chunks", 0) or 0)
        if status == ArchiveDescentStatus.PLANNED.value:
            # Объявленная ось ``archive_descent=plan`` — не деградация, но и не тишина:
            # членов в корпусе нет именно поэтому.
            errors.append(
                f"{product_id}/{url}: {label}: спуск объявлен листингом "
                "(archive_descent=plan) — члены контейнера не воспроизведены"
            )
            return
        if outcome_docs == 0:
            container_empty.append(url)
            errors.append(
                f"{product_id}/{url}: {FROZEN_CONTAINER_EMPTY}: спуск исполнен "
                f"({status}), но ни один член не дошёл до корпуса "
                f"(строк членов {int(getattr(outcome, 'members', 0) or 0)})"
            )

    # Порядок: авторские корни (авторский порядок) → известные URL (порядок планировщика).
    for url in [*roots, *(item.url for item in items)]:
        key = _state_url_key(url)
        if key is None:
            errors.append(
                f"{product_id}/{url}: {label}: URL не нормализуется — пере-извлечение "
                "невозможно (состояние по нему не ведётся)"
            )
            continue
        if key in seen_urls:
            continue
        seen_urls.add(key)
        touched.append(url)

        prior = _prior_url_state(product_id, key, url, state=state, errors=errors)
        # План 19 §E.7: контейнер — **не** документ (версии документа у него нет), поэтому
        # проверка идёт до ветки ``no_version``: иначе сохранённые члены архива навсегда
        # оставались бы «URL без версии». Сеть не нужна: путь и хеш байтов durable.
        container = _prior_container_state(
            product_id, key, url, state=state, errors=errors
        )
        if container is not None:
            _replay_container(container, prior, url)
            continue
        if prior is None:
            unknown_roots.append(url)
            errors.append(f"{product_id}/{url}: {_reason(FROZEN_UNKNOWN_ROOT_TEXT)}")
            continue
        prior_doc = _prior_document_version(
            prior, product_id, url, state=state, errors=errors
        )
        if prior_doc is None or prior_doc.status != _DOC_STATUS_ACTIVE:
            no_version.append(url)
            errors.append(f"{product_id}/{url}: {_reason(FROZEN_NO_VERSION_TEXT)}")
            continue

        raw_path = Path(prior_doc.raw_path) if prior_doc.raw_path else None
        outcome = rebuild_from_stored_bytes(
            product_id,
            url,
            raw_path=raw_path,
            prior=prior,
            prior_doc=prior_doc,
            label=label,
            errors=errors,
            extractor=extractor,
            chunker=chunker,
            extract_cache=extract_cache,
            fetch_plan_for=fetch_plan_for,
            limits_for=limits_for,
            extractor_factory=extractor_factory,
            level_extractors=level_extractors,
        )
        if outcome.cache_hit:
            cache_hits += 1
        if outcome.extracted:
            reextracted += 1
        if outcome.missing_bytes:
            # I19: верификация от **содержимого** файла, а не от имени. Retention
            # называет файл ``{sha256 сырых байтов}.bin``, поэтому ``raw_path.stem`` —
            # ожидаемый ключ, а ``_hash_file`` — фактический. ``document_version.content_hash``
            # здесь **не** ключ: это хеш извлечённого текста, а не байтов. Расхождение
            # имени и содержимого означает «сохранённых байтов нет» (файл подменён/битый),
            # а не «контент изменился».
            stored_bytes_missing.append(url)
            errors.append(
                f"{product_id}/{url}: {_reason(FROZEN_STORED_BYTES_MISSING_TEXT)}"
            )
            continue
        if outcome.doc is None:
            # Байты есть, конвейер отказал — причина уже в ``errors``.
            continue
        rebuilt = outcome.doc
        if rebuilt.content_changed:
            # Дрейф пере-извлечения: тот же файл, другой текст. Не «контент изменился»
            # (наблюдения не было), поэтому причина — свой токен.
            reextract_mismatch.append(url)
            errors.append(
                f"{product_id}/{url}: {FROZEN_REEXTRACT_MISMATCH}: content_hash "
                f"пере-извлечения {rebuilt.doc_hash} != записанного "
                f"{prior_doc.content_hash} — тот же файл, другой текст текущего "
                "движка (смена контента не наблюдалась)"
            )
            continue
        collected.append(rebuilt)

    # --- дедуп и канонический адрес ------------------------------------------- #
    # Группа — ``content_hash``: одинаковый текст двух URL — один документ. Группа
    # расширяется алиасами прошлого снятия (durable), а адрес выбирается ``min(url)``:
    # порядок прохода и host_concurrency на адрес не влияют.
    by_hash: dict[str, list[RebuiltDoc]] = {}
    for candidate in collected:
        by_hash.setdefault(candidate.doc_hash, []).append(candidate)

    report_docs = 0
    report_chunks = 0
    groups: list[tuple[str, tuple[str, ...]]] = []
    capture_doc_map: dict[str, CapturedDocRow] = {}
    for doc_hash in sorted(by_hash):
        candidates = by_hash[doc_hash]
        urls = {candidate.url for candidate in candidates}
        urls.update(prior_urls_by_hash.get(doc_hash, ()))
        canonical_url = min(urls)
        groups.append((doc_hash, tuple(sorted(urls))))
        for group_url in sorted(urls):
            group_key = _state_url_key(group_url)
            if group_key is None:
                continue
            capture_doc_map[group_url] = CapturedDocRow(
                doc_hash=doc_hash,
                url=group_url,
                normalized_url=group_key,
                reuse_kind=ReuseKind.REUSED.value,
            )

        chosen = min(candidates, key=lambda candidate: candidate.url)
        accepted = _persist_rebuilt_doc(
            product_id,
            canonical_url,
            chosen,
            state=state,
            errors=errors,
            embed_model_id=embed_model_id,
            store=store,
            ingest=ingest,
            seen_docs=seen_docs,
            seen_chunk_ids=seen_chunk_ids,
            duplicates=duplicates,
            doc_counters=doc_counters,
            persist_docs=persist_docs,
        )
        if accepted is None:
            continue
        report_docs += 1
        report_chunks += accepted

    if stored_bytes_missing:
        # Машинный токен с числом — в тот же канал видимых причин, а не только в отчёт:
        # «сколько байтов не нашлось» важнее «есть ли причина», а ``failed`` здесь нет
        # (I4): прогон состоялся, корпус просто неполон.
        errors.append(
            f"{product_id}: {FROZEN_STORED_BYTES_MISSING}:"
            f"{len(stored_bytes_missing)} — сохранённых байтов нет у "
            f"{len(stored_bytes_missing)} известных URL ({label})"
        )
    if containers_replayed:
        # Тот же канал, но факт **положительный**: «контейнеры воспроизведены» — это
        # ответ на вопрос «почему членов в корпусе столько же, сколько в живом прогоне».
        errors.append(
            f"{product_id}: {FROZEN_CONTAINER_REPLAYED}:{containers_replayed} — "
            f"контейнеров воспроизведено из сохранённых байтов (документов "
            f"{container_docs}, чанков {container_chunks}) ({label})"
        )

    return FrozenBytesReport(
        docs=report_docs + container_docs,
        chunks=report_chunks + container_chunks,
        reextracted=reextracted,
        cache_hits=cache_hits,
        unknown_roots=tuple(unknown_roots),
        stored_bytes_missing=tuple(stored_bytes_missing),
        no_version=tuple(no_version),
        reextract_mismatch=tuple(reextract_mismatch),
        containers=containers_replayed,
        container_docs=container_docs,
        container_chunks=container_chunks,
        container_unreadable=tuple(container_unreadable),
        container_empty=tuple(container_empty),
        groups=tuple(groups),
        urls=tuple(touched),
        capture_docs=tuple(capture_doc_map[key] for key in sorted(capture_doc_map)),
        label=label,
    )


def _prior_container_state(
    product_id: str,
    key: str,
    url: str,
    *,
    state: ProductStatePort | None,
    errors: list[str],
) -> ContainerStateRow | None:
    """Durable-наблюдение контейнера по ключу URL (план 19 §E.7, STATE 10).

    ``None`` — URL не был контейнером (обычный документ либо неизвестный URL): решение
    «это контейнер» принимает запись, а не повторный зонд байтов. Зонд потребовал бы
    читателя контейнеров **до** выбора маршрута, то есть второй путь распознавания рядом
    с живым (A1: один носитель факта).

    Отказ чтения изолируется в ``errors`` и означает «записи нет»: прогон продолжится
    обычным путём документа, а причина останется видимой.
    """
    if state is None:
        return None
    try:
        return state.get_container_state(product_id, key)
    except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
        errors.append(
            f"{product_id}/{url}: state: get_container_state: "
            f"{type(exc).__name__}: {exc}"
        )
        return None


def _prior_url_state(
    product_id: str,
    key: str,
    url: str,
    *,
    state: ProductStatePort | None,
    errors: list[str],
) -> UrlStateRow | None:
    """Строка ``url_state`` **только для диагностики** оффлайн-исхода (не решение).

    Решение принимает :func:`reuse_not_modified`; здесь нужен лишь ответ на вопрос
    «URL вообще известен состоянию?» — от него зависит текст ошибки: «корня нет в
    состоянии» и «версии переиспользовать нечего» ведут оператора в разные места.
    """
    if state is None:
        return None
    try:
        return state.get_url_state(product_id, key)
    except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
        errors.append(
            f"{product_id}/{url}: offline: state: get_url_state: "
            f"{type(exc).__name__}: {exc}"
        )
        return None


def _prior_document_version(
    prior: UrlStateRow,
    product_id: str,
    url: str,
    *,
    state: ProductStatePort | None,
    errors: list[str],
) -> DocumentVersionRow | None:
    """Версия документа, на которую указывает ``url_state.doc_version_id`` (или ``None``).

    ``None`` — либо версии у URL ещё не было, либо строка ``document_version`` пропала/
    стала ``superseded``: ``reuse_plan`` откажет в обоих случаях, и вызывающий назовёт
    это одной причиной («переиспользовать нечего»), а не выдумает различие, которого
    в субстрате нет.
    """
    if state is None or not prior.doc_version_id:
        return None
    try:
        return state.get_document_version(prior.doc_version_id)
    except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
        errors.append(
            f"{product_id}/{url}: offline: state: get_document_version"
            f"({prior.doc_version_id}): {type(exc).__name__}: {exc}"
        )
        return None


def group_revalidation_items(
    product_id: str,
    roots: Sequence[str],
    items: Sequence[IncrementalItem],
    *,
    scope_hosts_by_product: Mapping[str, frozenset[str]],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Группирует известные URL по авторским корням (scope inference, каждый прогон).

    ``url_state.seed_id`` вне этого среза: происхождение считается заново по
    ``Frontier.in_scope``. Побеждает **самый специфичный** ``root_path_prefix``
    (длина пути), при равенстве — авторский порядок. URL вне scope всех корней —
    orphan (``origin -> urls``): один synthetic-бакет на origin, ревалидация без
    записи усечения.

    Scope-предикат — тот же, что у дренажа (C: объявленные хосты), поэтому известный
    URL необъявленного поддомена не приписывается чужому корню: он становится orphan
    и **виден** в ``revalidation_orphans``, а не молча добывается под чужим бюджетом.
    """
    product_scope_hosts = scope_hosts_by_product.get(product_id, frozenset())
    scopes = [
        (root, Frontier(root, CrawlLimits(), scope_hosts=product_scope_hosts))
        for root in roots
    ]
    groups: dict[str, list[str]] = {root: [] for root in roots}
    orphans: dict[str, list[str]] = {}
    for item in items:
        best_root = scope_owner(item.url, scopes)
        if best_root is None:
            orphans.setdefault(_url_origin(item.url), []).append(item.url)
        else:
            groups[best_root].append(item.url)
    return groups, orphans


def durable_urls_by_owner(
    product_id: str,
    roots: Sequence[str],
    *,
    state: ProductStatePort | None,
    scope_hosts_by_product: Mapping[str, frozenset[str]],
    errors: list[str],
) -> dict[str, list[str]]:
    """Durable-документы продукта, разложенные по **корню-владельцу** (E.6 плана 19).

    Зачем это, если есть :func:`group_revalidation_items`. Группы ревалидации — это
    work-set **сетевого** обхода: вне режима ``incremental`` планировщик их не строит
    (``plan_incremental_items`` под сетью возвращает ``[]`` для ``new``/``full``). Для
    корня, которому политика запретила сеть, work-set другой по определению — это
    **durable-субстрат**, ровно как у прохода без сети: там ``url_state`` и есть работа
    (AGENTS §6.1; измерено 04.11.2026 — roots-only потерял 7 документов из 9). Брать
    ревалидационную группу значило бы тихо терять документы корня в ``full``/``new``.

    Владелец — тот же предикат, что у группировки ревалидации (:func:`scope_owner`:
    самый специфичный ``root_path_prefix``), поэтому найденный URL не может «переехать»
    под другой корень между двумя ответами. URL вне scope всех авторских корней
    (orphan) сюда не входят: они не принадлежат ни одному корню продукта.

    :returns: ``{root_url: [url, ...], ...}`` — порядок ``url_state`` (сортировка по
        ``normalized_url``), чтобы проход был воспроизводим. Корень без документов в
        словаре отсутствует.
    """
    if state is None or not roots:
        return {}
    product_scope_hosts = scope_hosts_by_product.get(product_id, frozenset())
    scopes = [
        (root, Frontier(root, CrawlLimits(), scope_hosts=product_scope_hosts))
        for root in roots
    ]
    owned: dict[str, list[str]] = {}
    try:
        rows = sorted(state.iter_url_state(product_id), key=lambda row: row.normalized_url)
    except Exception as exc:  # noqa: BLE001 — изоляция чтения состояния
        errors.append(
            f"{product_id}: durable_urls_by_owner: {type(exc).__name__}: {exc} — "
            "переиспользование корня пойдёт по авторскому адресу"
        )
        return {}
    for row in rows:
        # Адрес — тот, которым строка адресована состоянию (``row.url``): им же
        # пользуется планировщик (``_item_from_url_state``), и он же станет
        # ``corpus_docs.url`` переиспользованной копии.
        owner = scope_owner(row.url, scopes)
        if owner is None:
            continue
        owned.setdefault(owner, []).append(row.url)
    return owned


def resolve_product_revalidate_cap(
    roots: Sequence[str], *, limits_for: Callable[[str], CrawlLimits]
) -> int | None:
    """Разрешает ось ``crawl.revalidate_max_documents`` **на продукт**.

    Ось живёт в :class:`CrawlLimits` и резолвится тем же
    :func:`resolve_crawl_limits` (app < run < site), что и остальные оси, но
    применяется к продукту целиком, а не к отдельному корню. У ``resolve_crawl_limits``
    вход — URL (per-site слой), а у продукта URL-идентичности нет: представителем
    продукта служит первый авторский корень (стабильный author/sorted порядок). Это
    единственное место, где «per product» требует такого представителя; при нескольких
    per-site значениях для одного продукта действует значение первого корня.
    """
    if not roots:
        return None
    return limits_for(roots[0]).revalidate_max_documents


def apply_revalidation_cap(
    product_id: str,
    groups: Mapping[str, Sequence[str]],
    orphans: Mapping[str, Sequence[str]],
    items: Sequence[IncrementalItem],
    cap: int,
    *,
    state: ProductStatePort | None,
    errors: list[str],
    limits_for: Callable[[str], CrawlLimits],
    counters: CorpusCounters,
) -> tuple[
    dict[str, list[str]],
    dict[str, list[str]],
    dict[str, int],
    dict[str, int],
    dict[str, int],
    dict[str, int],
    int,
    int,
    bool,
]:
    """Отбирает ``cap`` **исполнимых** известных URL продукта.

    Сначала — eligibility-фильтр по resolved-лимитам владельца (root key для
    root-группы, origin для orphan-бакета): ``exclude_paths``/``exclude_documents``
    и pre-fetch-пропуск документа при ``document_level=off``. Иначе неисполнимый URL
    навсегда занимал бы голову NULLs-first/oldest-first очереди: drain отверг бы его
    без обновления ``url_state``, и потолок тратил бы слот на него каждый цикл.

    Затем — порядок ``last_checked_at`` **NULLs first**, ``normalized_url``. Стабильное
    «первые N» навсегда бы заморило хвост; «сначала старые» ротирует автоматически:
    отложенные сохраняют старый ``last_checked_at`` и на следующем цикле оказываются
    первыми. Отложенные получают ``reuse_kind=stale`` в ``url_state``; неисполнимые
    не откладываются, а попадают в ``skipped``.

    Возвращает ``(selected_groups, selected_orphans, deferred_by_root,
    deferred_by_origin, skipped_by_root, skipped_by_origin, deferred_total,
    skipped_total, cap_fired)``. Инвариант учёта:
    ``selected + deferred + skipped == len(known)`` по продукту.
    Сбой чтения состояния — явная ошибка и ось игнорируется целиком.
    """
    flat_groups = {key: list(urls) for key, urls in groups.items()}
    flat_orphans = {key: list(urls) for key, urls in orphans.items()}
    if state is None:
        # Потолок выбирает из **известных** URL, а их список живёт в состоянии: без
        # носителя выбирать не из чего. Прежний путь падал здесь ``AttributeError`` и
        # выдавал его за сбой чтения состояния — ложная причина. Ось не применена, и это
        # видимая запись: все известные URL идут в работу.
        errors.append(
            f"{product_id}: revalidate_max_documents: состояния нет (state=None) — "
            "ось игнорируется (все известные URL)"
        )
        return flat_groups, flat_orphans, {}, {}, {}, {}, 0, 0, False
    try:
        rows = {
            row.normalized_url: row for row in state.iter_url_state(product_id)
        }
    except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
        errors.append(
            f"{product_id}: revalidate_max_documents: state.iter_url_state: "
            f"{type(exc).__name__}: {exc} — ось игнорируется (все известные URL)"
        )
        return flat_groups, flat_orphans, {}, {}, {}, {}, 0, 0, False

    normalized = {item.url: item.normalized_url for item in items}

    def _normalized(url: str) -> str:
        key = normalized.get(url) or _state_url_key(url)
        return key or url

    limits_cache: dict[str, CrawlLimits] = {}

    def _limits_for(owner: str) -> CrawlLimits:
        limits = limits_cache.get(owner)
        if limits is None:
            limits = limits_for(owner)
            note_scheduling(limits, counters=counters)
            limits_cache[owner] = limits
        return limits

    def _ineligible(url: str, owner: str) -> bool:
        limits = _limits_for(owner)
        if is_blacklisted(url, limits):
            return True
        return (
            limits.document_level is DocumentLevel.OFF
            and document_suffix(url) is not None
        )

    entries: list[tuple[str, str, str]] = [
        (url, "root", key)
        for key, urls in flat_groups.items()
        for url in urls
    ] + [
        (url, "orphan", key)
        for key, urls in flat_orphans.items()
        for url in urls
    ]

    skipped_by_root = {key: 0 for key in flat_groups}
    skipped_by_origin = {key: 0 for key in flat_orphans}
    eligible: list[tuple[str, str, str]] = []
    for entry in entries:
        url, kind, owner = entry
        if not _ineligible(url, owner):
            eligible.append(entry)
            continue
        target = skipped_by_root if kind == "root" else skipped_by_origin
        target[owner] += 1

    def _order(entry: tuple[str, str, str]) -> tuple[bool, str, str, str]:
        url = entry[0]
        key = _normalized(url)
        row = rows.get(key)
        checked = row.last_checked_at if row is not None else None
        # ``False`` (NULL) сортируется раньше ``True`` — NULLs first.
        return (checked is not None, checked or "", key, url)

    ordered = sorted(eligible, key=_order)
    selected = ordered[:cap]
    deferred = ordered[cap:]
    selected_urls = {url for url, _kind, _key in selected}
    deferred_urls = {url for url, _kind, _key in deferred}

    selected_groups = {
        key: [url for url in urls if url in selected_urls]
        for key, urls in flat_groups.items()
    }
    selected_orphans = {
        key: [url for url in urls if url in selected_urls]
        for key, urls in flat_orphans.items()
    }
    deferred_by_root = {
        key: sum(1 for url in urls if url in deferred_urls)
        for key, urls in flat_groups.items()
    }
    deferred_by_origin = {
        key: sum(1 for url in urls if url in deferred_urls)
        for key, urls in flat_orphans.items()
    }

    # ``stale`` — durable-сигнал следующему циклу: отложенный URL просрочен, а не
    # «не существует». Значение enum берём из домена; нового перечисления не вводим.
    for url, _kind, _key in deferred:
        key = _normalized(url)
        row = rows.get(key)
        if row is None or row.reuse_kind == ReuseKind.STALE.value:
            continue
        try:
            state.upsert_url_state(
                replace(row, reuse_kind=ReuseKind.STALE.value)
            )
        except Exception as exc:  # noqa: BLE001 — изоляция state-записи
            errors.append(
                f"state: url_state({product_id}/{key}): stale: "
                f"{type(exc).__name__}: {exc}"
            )
    return (
        selected_groups,
        selected_orphans,
        deferred_by_root,
        deferred_by_origin,
        skipped_by_root,
        skipped_by_origin,
        len(deferred),
        sum(skipped_by_root.values()) + sum(skipped_by_origin.values()),
        bool(deferred),
    )


def note_scheduling(limits: CrawlLimits, *, counters: CorpusCounters) -> None:
    """Запоминает разрешённое окно экстракции корня (F0, наблюдаемость расписания).

    Максимум по корням, а не последнее значение: пул — фазовый по ключу
    ``(уровень, окно)``, и фактическая ширина параллелизма фазы — самая широкая из
    использованных. ``derived`` — «хотя бы на одном корне» (§2.10): тогда источник
    расписания в метриках — бюджет памяти, а не воля оператора.
    """
    if limits.extract_workers is not None:
        candidate = int(limits.extract_workers)
        if (
            counters.observed_extract_workers is None
            or candidate > counters.observed_extract_workers
        ):
            counters.observed_extract_workers = candidate
    if limits.extract_max_tasks_per_child is not None:
        candidate_tasks = int(limits.extract_max_tasks_per_child)
        if (
            counters.observed_extract_max_tasks is None
            or candidate_tasks > counters.observed_extract_max_tasks
        ):
            counters.observed_extract_max_tasks = candidate_tasks
    if limits.extract_workers_derived:
        counters.observed_extract_derived = True


def owner_root(
    url: str,
    *,
    owner_scopes: Sequence[tuple[str, Frontier]],
    owner_cache: dict[str, str | None],
) -> str | None:
    """Корень-владелец URL (``None`` — вне scope всех авторских корней продукта)."""
    if url not in owner_cache:
        owner_cache[url] = scope_owner(url, owner_scopes)
    return owner_cache[url]


def resolve_target(
    target_pid: str,
    target_url: str,
    *,
    targets: dict[tuple[str, str], Any],
    crawl_sources: CrawlSourcesConfig | None,
    domain_profiles: Mapping[str, DomainProfile] | None,
    explicit_fetch: Mapping[str, FetchMode] | None,
    crawl_strategy_default: CrawlStrategy,
    crawl_rate_default: Any,
) -> Any:
    """Разрешает ``CrawlTarget`` URL (кэш ``targets``), как в обоих путях.

    ``target_pid``/``target_url`` вместо ``pid``/``url``: последние —
    переменные циклов внешней области, и совпадение имён делало бы
    привязку неотличимой от поздней (B023).
    """
    target = targets.get((target_pid, target_url))
    if target is None:
        target = resolve_crawl_target(
            target_pid,
            target_url,
            sources=crawl_sources or CrawlSourcesConfig(),
            profiles=domain_profiles or {},
            explicit=explicit_fetch,
            run_default_strategy=crawl_strategy_default,
            run_default_rate_limit=crawl_rate_default,
        )
        targets[(target_pid, target_url)] = target
    return target


def new_extract_pool(
    *,
    extraction_pool: ExtractionPool,
    extract_window: int | None,
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline,
    recycle_tasks: int | None,
) -> Executor:
    """Фабрика пула ключа ``(level, window)`` — одна на прогон (C1/L1).

    Поднята в область корня (F4a плана ``17``): пул нужен **двум**
    потребителям — страницам и членам контейнеров (B3/P4). ``pools.executor``
    кэширует исполнителя по ключу, поэтому тело исполняется один раз.

    ``extraction_pool`` обязателен: фабрика строит пул **через** него, и «пула нет»
    означает inline-путь, до этой функции не доходящий. Опциональный параметр обещал бы
    ветку, которой нет и не должно быть (inline исполняется выше, в потребителе).
    """
    if recycle_tasks is not None and extraction_pool.supports_worker_recycling:
        # P4/OOM: счётчик заданий на процесс — воркер пересоздаётся
        # (initializer `_extract_worker_init` строит экстрактор заново).
        return extraction_pool.pool_factory(
            extract_window, level, pdf_pipeline=pdf_pipeline,
            max_tasks_per_child=recycle_tasks,
        )
    # Счётчик рециклинга не задан — вызов без него (остальное то же).
    return extraction_pool.pool_factory(
        extract_window, level, pdf_pipeline=pdf_pipeline
    )


def make_extract_request(
    page: RawPage,
    raw_path: Path,
    fetch_plan: FetchPlan,
    *,
    extract_cache: ExtractionCache | None,
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline,
    worker_rss_dir: str | None,
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
        pdf_pipeline=pdf_pipeline,
        cache_root=(
            str(extract_cache.root) if extract_cache is not None else None
        ),
        cache_max_bytes=(
            extract_cache.max_bytes if extract_cache is not None else None
        ),
        # F0: сток пика памяти воркера (``None`` — пик не собирается).
        rss_dir=worker_rss_dir,
    )


