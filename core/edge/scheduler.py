"""coHound — планировщик очереди краула edge (Z1/DMZ; Phase 3.2, 07 §3–§5).

Edge-локальный исполнитель заданий краула: **собственная** SQLite-БД
(``{state_dir}/edge_jobs.db``), content-addressed блоб-хранилище
(``{resolved_blob_dir}/{job_id}/{content_hash}.bin``) и пул воркеров. Планировщик
инъецируется в :class:`core.edge.server.EdgeGateway` (``scheduler=``) и реализует
зафиксированный в Wave 3.1 интерфейс:

* ``submit(payload) -> {job_id, accepted, total, session_id}`` — идемпотентная
  постановка задания (ключ ``(run_id, session_id, idempotency_key)``) с back-pressure
  по ``queue_limit``;
* ``status(job_id) -> {job_id, status, total, pending, running, fetched, failed,
  cancelled, engines, kinds}``;
* ``cancel(job_id) -> {job_id, cancelled}``;
* ``collect(job_id, cursor, limit) -> {items, next_cursor, done}``.

**Модель исполнения (решение владельца).** ``http`` исполняется **потоками** в
процессе edge; ``scrapy``/``browser`` — **процессами** (Playwright не форк-безопасен,
адаптеры не picklable): воркер-процесс строит собственный экземпляр краулера через
инъецируемый ``crawler_factory(engine) -> CrawlerPort``. Процесс только фетчит и
возвращает ``RawPage``; запись блоба/БД делает родитель (единственная точка доступа к
SQLite), поэтому соединение не пересекает границу процессов.

**Границы слоя.** Модуль — edge, импортирует ``adapters``/``config``/``ports``/``util``;
**никогда** ``store``/``pipeline``/``cli``. I18/I19: edge возвращает сырые байты, не
evidence; ядро пересчитывает хэш. Опора — только stdlib (``sqlite3``/``threading``/
``concurrent.futures``/``multiprocessing``/``base64``/``hashlib``). Нейтрален (§1.3).

Recovery-on-start зеркалит ``core.pipeline``-семантику: застрявшие ``running``-элементы
возвращаются в ``pending``, счётчики пересчитываются (прерванный процесс не теряет
намерения краула).

**§10 / pacing (A2c).** Под ``split`` pacing исполняется на стороне edge **per executing
context**: один стателесс :class:`~core.util.rate_limit.RateLimiter` в процессе планировщика
для потоковых движков и по одному в каждом воркер-процессе для ``scrapy``/``browser``. Это
**не** глобальный per-host rate-гейт: при ``workers>1`` параллельные фетчи одного хоста не
ограничены сверху, а кросс-процессный pacing не разделяет состояние. Полное соблюдение
``rate_limit`` как глобального per-host лимита **не заявляется**. Локальный путь имеет тот же
характер при ``max_workers>1`` (лимитер стателесс, ``sleep(uniform(min,max))`` на запрос),
поэтому расхождение ``local``/``split`` по контенту отсутствует; тайминг не нормируется ни в
одном профиле. ``rate_limit=None`` (дефолт) ⇒ ноль снов (дефолтные прогоны байт-идентичны).

**Robots (§10, A2b).** Edge — стателесс-фетчер: политику приносит ядро пер-элемент
(``plan.robots``), а политику ``robots.txt`` edge строит сам (:func:`build_robots_gate`) и
отдаёт краулерам. Фетч ``robots.txt`` происходит только когда ``plan.robots`` истинно.
"""

from __future__ import annotations

import base64
import json
import multiprocessing
import os
import sqlite3
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.edge.config import EdgeRuntimeConfig
from core.edge.crawler_factory import build_robots_gate
from core.ports.crawler import CrawlerPort
from core.ports.types import (
    CrawlStrategy,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    RateLimitSpec,
)
from core.util.hashing import content_hash
from core.util.rate_limit import RateLimiter
from core.util.robots import RobotsGate
from core.util.security import validate_url

__all__ = [
    "EdgeScheduler",
    "ITEM_PENDING",
    "ITEM_RUNNING",
    "ITEM_FETCHED",
    "ITEM_FAILED",
    "ITEM_CANCELLED",
    "ITEM_TERMINAL",
    "JOB_CREATED",
    "JOB_RUNNING",
    "JOB_COMPLETED",
    "JOB_PARTIAL",
    "JOB_CANCELLED",
    "JOB_FAILED",
    "JOB_TERMINAL",
]

# ---------------------------------------------------------------------------
# Статусы (строки-данные; значения совпадают с доменными CrawlItemStatus/
# CrawlSessionStatus, но edge их не импортирует как enum — это операционная таблица)
# ---------------------------------------------------------------------------

#: Элемент в очереди, ещё не фетчился.
ITEM_PENDING = "pending"
#: Элемент в обработке воркером (при старте восстанавливается в ``pending``).
ITEM_RUNNING = "running"
#: Сырые байты сохранены в блоб-хранилище (retention).
ITEM_FETCHED = "fetched"
#: Терминальный сбой фетча элемента.
ITEM_FAILED = "failed"
#: Элемент снят с работы (операторская отмена).
ITEM_CANCELLED = "cancelled"

#: Терминальные статусы элемента.
ITEM_TERMINAL: frozenset[str] = frozenset({ITEM_FETCHED, ITEM_FAILED, ITEM_CANCELLED})

#: Задание объявлено, работа не начата.
JOB_CREATED = "created"
#: Задание исполняется.
JOB_RUNNING = "running"
#: Все элементы успешно добыты.
JOB_COMPLETED = "completed"
#: Часть элементов упала/отменена, тело краула добыто.
JOB_PARTIAL = "partial"
#: Задание снято оператором/системой.
JOB_CANCELLED = "cancelled"
#: Терминальный сбой задания.
JOB_FAILED = "failed"

#: Терминальные статусы задания.
JOB_TERMINAL: frozenset[str] = frozenset(
    {JOB_COMPLETED, JOB_PARTIAL, JOB_CANCELLED, JOB_FAILED}
)

#: Placeholder-строка и параметры фильтра «задание нетерминально» для SQL (C4).
#: Используется в recovery/claim/stop, чтобы элементы терминальных заданий не оживали.
_TERMINAL_PLACEHOLDERS = ",".join("?" for _ in JOB_TERMINAL)
_TERMINAL_PARAMS: tuple[str, ...] = tuple(sorted(JOB_TERMINAL))


def _non_terminal_job_filter() -> str:
    """SQL-фрагмент «элемент принадлежит нетерминальному заданию» (C4).

    Подставляется в ``UPDATE``/``SELECT`` по ``edge_job_items``; параметры —
    :data:`_TERMINAL_PARAMS`. Так recovery/claim/stop никогда не трогают элементы
    заданий в терминальном статусе (``completed|partial|cancelled|failed``).
    """
    return (
        "job_id IN (SELECT job_id FROM edge_jobs"
        f" WHERE status NOT IN ({_TERMINAL_PLACEHOLDERS}))"
    )

#: Движки, исполняемые в отдельных процессах (тяжёлые/не форк-безопасные).
_PROCESS_ENGINES: tuple[str, ...] = ("scrapy", "browser")

#: Дефолтный размер страницы ``collect`` (число элементов).
_DEFAULT_COLLECT_LIMIT = 100
#: Бюджет декодированных байтов на страницу ``collect`` (bounded-память).
_COLLECT_MAX_BYTES = 8 * 1024 * 1024

#: DDL edge-локальной БД (идемпотентно; PRAGMA выставляются отдельно).
_SCHEMA = """
CREATE TABLE IF NOT EXISTS edge_jobs (
    job_id          TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    status          TEXT NOT NULL,
    total           INTEGER NOT NULL DEFAULT 0,
    pending         INTEGER NOT NULL DEFAULT 0,
    running         INTEGER NOT NULL DEFAULT 0,
    fetched         INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    cancelled       INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    error           TEXT,
    UNIQUE (run_id, session_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS edge_job_items (
    job_id       TEXT NOT NULL,
    product_id   TEXT NOT NULL,
    url          TEXT NOT NULL,
    status       TEXT NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    engine       TEXT,
    kind         TEXT,
    fetch        TEXT,
    extract      TEXT,
    strategy     TEXT,
    wait_for     TEXT,
    lang         TEXT,
    http_status  INTEGER,
    content_type TEXT,
    fetched_at   TEXT,
    content_hash TEXT,
    blob_ref     TEXT,
    error        TEXT,
    robots       INTEGER,
    final_url    TEXT,
    rate_limit   TEXT,
    PRIMARY KEY (job_id, product_id, url)
);

CREATE INDEX IF NOT EXISTS idx_edge_job_items_job_status
    ON edge_job_items (job_id, status);
"""

#: Аддитивные колонки ``edge_job_items`` (имя, тип), добавляемые идемпотентной миграцией
#: (§6a/§11). ``CREATE TABLE IF NOT EXISTS`` НЕ расширяет существующую ``edge_jobs.db`` —
#: колонки доливаются через ``ALTER TABLE`` (``_connect`` → ``_migrate``).
_ITEM_MIGRATED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("robots", "INTEGER"),
    ("final_url", "TEXT"),
    ("rate_limit", "TEXT"),
)


def _utc_now_iso() -> str:
    """Текущий UTC-таймстемп ISO-8601 (дефолтные часы планировщика)."""
    return datetime.now(timezone.utc).isoformat()


def _default_id_factory() -> str:
    """Дефолтный ``job_id`` — uuid4 hex."""
    return uuid.uuid4().hex


def _default_process_context() -> Any:
    """Контекст ``multiprocessing`` для процесс-движков (``forkserver`` → ``spawn``).

    ``forkserver`` — дефолт Python 3.14 на Linux и безопасен для Playwright (не
    форкает прогретый родитель); ``spawn`` — переносимый фолбэк.
    """
    for method in ("forkserver", "spawn"):
        try:
            return multiprocessing.get_context(method)
        except ValueError:  # pragma: no cover - платформо-зависимо
            continue
    return multiprocessing.get_context()  # pragma: no cover


# ---------------------------------------------------------------------------
# Воркер-функции (module-level: должны быть picklable для процессов)
# ---------------------------------------------------------------------------

#: Фабрика краулеров, установленная инициализатором воркер-процесса.
_WORKER_FACTORY: Callable[..., CrawlerPort] | None = None
#: Построенные краулеры воркер-процесса (по движку; переиспользуются между задачами).
_WORKER_CRAWLERS: dict[str, CrawlerPort] = {}
#: Пейсер воркер-процесса (A2c): один на процесс, строится инициализатором (дефолт
#: :class:`RateLimiter`). ``None`` — до инициализации.
_WORKER_LIMITER: RateLimiter | None = None
#: Политика ``robots.txt`` воркер-процесса (§10): строится инициализатором из
#: ``user_agent`` запроса. ``None`` — до инициализации.
_WORKER_ROBOTS: RobotsGate | None = None


def _process_worker_init(
    crawler_factory: Callable[..., CrawlerPort],
    engines: Sequence[str],
    limiter_factory: Callable[[], RateLimiter] = RateLimiter,
    robots_user_agent: str | None = None,
) -> None:
    """Инициализатор воркер-процесса: сохраняет фабрику + строит пейсер/robots-политику.

    Каждый процесс строит **собственный** экземпляр краулера — Playwright/адаптеры
    не пересекают границу процессов. Пейсер (A2c) и robots-политика (§10) также строятся
    **в процессе** (объекты не picklable через границу): ``limiter_factory`` picklable by
    reference (дефолт :class:`RateLimiter`), ``robots_user_agent`` — строка с ядра.
    """
    global _WORKER_FACTORY, _WORKER_LIMITER, _WORKER_ROBOTS
    _WORKER_FACTORY = crawler_factory
    _WORKER_CRAWLERS.clear()
    _WORKER_LIMITER = limiter_factory()
    _WORKER_ROBOTS = build_robots_gate(robots_user_agent)


def _worker_crawler(engine: str) -> CrawlerPort:
    """Лениво строит (и кэширует) краулер движка внутри воркер-процесса.

    Robots-политика воркера передаётся в фабрику: ``plan.robots=False`` (дефолт) делает
    её инертной — лишних fetch ``robots.txt`` нет.
    """
    crawler = _WORKER_CRAWLERS.get(engine)
    if crawler is None:
        if _WORKER_FACTORY is None:  # pragma: no cover - защита от неверной сборки
            raise RuntimeError("воркер-процесс без crawler_factory")
        crawler = _WORKER_FACTORY(engine, robots=_WORKER_ROBOTS)
        _WORKER_CRAWLERS[engine] = crawler
    return crawler


def _worker_limiter() -> RateLimiter:
    """Пейсер воркер-процесса (ленивая сборка дефолтом, если инициализатор не позвали)."""
    global _WORKER_LIMITER
    if _WORKER_LIMITER is None:  # pragma: no cover - защита от неверной сборки
        _WORKER_LIMITER = RateLimiter()
    return _WORKER_LIMITER


def _fetch_host(url: str) -> str:
    """Хост запрошенного URL для пейсинга (зеркало ``corpus``: ``hostname`` в нижнем регистре)."""
    return (urlsplit(url).hostname or "").lower()


def _process_fetch(engine: str, url: str, plan: FetchPlan) -> dict[str, Any]:
    """Воркер-процесс: пейсит, строит краулер и фетчит; возвращает picklable-результат.

    Исключения **не** пробрасываются через границу процессов (их pickle/traceback
    хрупок) — сводятся к строке ``error``. ``limiter.wait`` — до фетча (A2c, §11.2);
    ``plan.rate_limit=None`` ⇒ без сна.
    """
    _worker_limiter().wait(_fetch_host(url), plan.rate_limit)
    try:
        page = _worker_crawler(engine).fetch(url, plan)
    except BaseException as exc:  # noqa: BLE001 - сбой воркера → строковый результат
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"ok": True, "page": page}


def _thread_fetch(
    crawler: CrawlerPort, engine: str, url: str, plan: FetchPlan, limiter: RateLimiter
) -> dict[str, Any]:
    """Воркер-поток (in-process): пейсит, фетчит инъецированным краулером; та же форма.

    ``limiter.wait`` — до фетча (A2c, §11.2, зеркало ``corpus._local_fetch``); при
    ``plan.rate_limit=None`` :meth:`RateLimiter.wait` возвращает ``0.0`` **без** ``sleep``.
    """
    limiter.wait(_fetch_host(url), plan.rate_limit)
    try:
        page = crawler.fetch(url, plan)
    except BaseException as exc:  # noqa: BLE001 - единый контракт результата
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"ok": True, "page": page}


def _bool_int(value: Any, default: int = 0) -> int:
    """Мягкое приведение к ``int`` (нечисло → ``default``)."""
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _pool_workers(pool: Any) -> list[Any]:
    """Возвращает воркер-потоки/процессы пула для ограниченного join (C5b).

    ``ThreadPoolExecutor`` хранит потоки в ``_threads``, ``ProcessPoolExecutor`` —
    процессы в ``_processes`` (dict ``pid -> Process``). Атрибуты — приватные детали
    stdlib, но стабильные; при их отсутствии возвращается пустой список (остановка
    тогда опирается на таймауты адаптеров).
    """
    workers: list[Any] = []
    threads = getattr(pool, "_threads", None)
    if threads:
        workers.extend(list(threads))
    processes = getattr(pool, "_processes", None)
    if processes:
        workers.extend(list(processes.values()) if isinstance(processes, Mapping) else list(processes))
    return workers


# ---------------------------------------------------------------------------
# Планировщик
# ---------------------------------------------------------------------------


class EdgeScheduler:
    """Edge-локальный планировщик очереди краула (Phase 3.2).

    :param config: runtime-конфигурация edge (``state_dir``/``workers``/``queue_limit``/
        ``resolved_blob_dir``).
    :param crawlers: отображение ``engine → CrawlerPort`` для **in-process** (потоковых)
        движков (обычно ``http``). Живые объекты здесь; в процессы не передаются.
    :param crawler_factory: фабрика ``engine → CrawlerPort`` для **процессных** движков
        (``scrapy``/``browser``); вызывается **внутри** воркер-процесса. Должна быть
        picklable (module-level функция) — иначе процесс-пул не поднимется.
    :param clock: часы ``() -> ISO-8601`` (детерминизм в тестах).
    :param id_factory: генератор ``job_id`` (детерминизм в тестах).
    :param poll_interval: интервал ожидания координатора, сек (резерв под фоновое ожидание).
    :param process_context: контекст ``multiprocessing`` (по умолчанию ``forkserver``/``spawn``).
    :param limiter: пейсер потоковых движков (A2c, §11.2) — один стателесс
        :class:`~core.util.rate_limit.RateLimiter` на процесс планировщика; ``None`` →
        ``RateLimiter()``. Инжектируем в тестах (стаб ``sleep``/``rng``).
    :param limiter_factory: picklable-фабрика пейсера **воркер-процесса** (``scrapy``/
        ``browser``); ``None`` → :class:`RateLimiter`. Вызывается внутри процесса.
    :param robots_factory: фабрика политики ``robots.txt`` (§10) ``(user_agent) -> RobotsGate``;
        ``None`` → :func:`core.edge.crawler_factory.build_robots_gate`.
    """

    def __init__(
        self,
        config: EdgeRuntimeConfig,
        crawlers: Mapping[str, CrawlerPort],
        *,
        crawler_factory: Callable[[str], CrawlerPort] | None = None,
        clock: Callable[[], str] | None = None,
        id_factory: Callable[[], str] | None = None,
        poll_interval: float = 0.05,
        process_context: Any = None,
        limiter: RateLimiter | None = None,
        limiter_factory: Callable[[], RateLimiter] | None = None,
        robots_factory: Callable[[str | None], RobotsGate] | None = None,
    ) -> None:
        self._config = config
        self._crawlers: dict[str, CrawlerPort] = dict(crawlers or {})
        self._crawler_factory = crawler_factory
        self._clock = clock or _utc_now_iso
        self._id_factory = id_factory or _default_id_factory
        self._poll = max(0.001, float(poll_interval))
        # A2c: пейсер потоковых движков (стателесс) + фабрика пейсера воркер-процесса.
        self._limiter = limiter if limiter is not None else RateLimiter()
        self._limiter_factory = limiter_factory or RateLimiter
        # §10: фабрика robots-политики; сама политика строится лениво из ``user_agent`` submit.
        self._robots_factory = robots_factory or build_robots_gate
        self._robots_user_agent: str | None = None
        self._robots_gate: RobotsGate | None = None

        self._state_dir = Path(config.state_dir)
        self._blob_dir = Path(config.resolved_blob_dir)
        self._db_path = self._state_dir / "edge_jobs.db"

        # In-process (потоковые) и процессные движки.
        self._thread_engines: set[str] = set(self._crawlers)
        self._process_engines: tuple[str, ...] = tuple(
            engine
            for engine in _PROCESS_ENGINES
            if int(config.workers.get(engine, 0)) > 0 and crawler_factory is not None
        )

        self._process_context = process_context
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._thread_pool: ThreadPoolExecutor | None = None
        self._process_pool: ProcessPoolExecutor | None = None
        self._thread_slots = 0
        self._process_slots = 0
        self._inflight = 0
        self._started = False
        self._stopped = False
        #: Флаг реэнтрантности ``_dispatch`` (C5a): done-callback может сработать
        #: синхронно из ``_submit_row`` и вызвать ``_dispatch`` повторно; флаг
        #: гарантирует единственного активного диспетчера (без рекурсии).
        self._dispatching = False

    # -- жизненный цикл --------------------------------------------------------

    def start(self) -> None:
        """Идемпотентно: recovery-on-start, сборка пулов, диспетчеризация pending."""
        with self._lock:
            if self._started:
                return
            conn = self._connect()
            self._recover(conn)
            self._build_pools()
            self._started = True
        self._dispatch()

    def stop(self, timeout: float | None = None) -> None:
        """Graceful-останов: сигнал → слив пулов → ``running→pending`` → join.

        После остановки публичные чтения (``status``/``collect``) продолжают работать.
        :param timeout: бюджет ожидания слива, сек (best-effort; ``None`` — без лимита).
        """
        with self._lock:
            if self._stopped:
                return
            self._stopped = True
            thread_pool, process_pool = self._thread_pool, self._process_pool
        self._drain(thread_pool, process_pool, timeout)
        with self._lock:
            conn = self._conn
            if conn is not None:
                with conn:
                    conn.execute(
                        "UPDATE edge_job_items SET status = ? WHERE status = ?"
                        f" AND {_non_terminal_job_filter()}",
                        (ITEM_PENDING, ITEM_RUNNING, *_TERMINAL_PARAMS),
                    )
                rows = conn.execute(
                    "SELECT job_id FROM edge_jobs WHERE status NOT IN "
                    f"({_TERMINAL_PLACEHOLDERS})",
                    _TERMINAL_PARAMS,
                ).fetchall()
                for row in rows:
                    self._recompute_job(conn, str(row["job_id"]))
            self._thread_pool = None
            self._process_pool = None
            self._inflight = 0

    def close(self) -> None:
        """Закрывает соединение с БД (для аккуратной уборки ресурсов в тестах)."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> "EdgeScheduler":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.stop()
        self.close()

    # -- БД --------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        """Лениво открывает edge-локальную БД и создаёт схему (идемпотентно)."""
        with self._lock:
            if self._conn is None:
                self._state_dir.mkdir(parents=True, exist_ok=True)
                conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA busy_timeout = 5000")
                conn.execute("PRAGMA synchronous = NORMAL")
                conn.executescript(_SCHEMA)
                self._migrate(conn)
                conn.commit()
                self._conn = conn
            return self._conn

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        """Идемпотентно доливает аддитивные колонки ``edge_job_items`` (§6a/§11).

        ``CREATE TABLE IF NOT EXISTS`` НЕ добавляет колонку в **существующую**
        ``edge_jobs.db`` — поэтому недостающие колонки (``robots``/``final_url``/
        ``rate_limit``) доливаются через ``ALTER TABLE ... ADD COLUMN``. Повторный вызов
        — no-op (колонка уже есть); существующие строки не трогаются. Имена колонок —
        литералы модуля (не из данных), SQL-инъекция невозможна.
        """
        existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(edge_job_items)")}
        for column, column_type in _ITEM_MIGRATED_COLUMNS:
            if column not in existing:
                conn.execute(f"ALTER TABLE edge_job_items ADD COLUMN {column} {column_type}")

    def _get_job(self, conn: sqlite3.Connection, job_id: str) -> sqlite3.Row:
        """Строка задания; неизвестное задание → ``ValueError`` (edge честно ошибается)."""
        row = conn.execute(
            "SELECT * FROM edge_jobs WHERE job_id = ?", (str(job_id),)
        ).fetchone()
        if row is None:
            raise ValueError(f"неизвестное задание краула: {job_id!r}")
        return row

    # -- submit ----------------------------------------------------------------

    def submit(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Идемпотентная постановка задания краула (back-pressure по ``queue_limit``).

        Возвращает ``{job_id, accepted, total, session_id}``. При исчерпании потолка
        очереди — ``accepted=False``, ``job_id=""``, ``total=0`` (без исключения).
        Повтор с тем же ``(run_id, session_id, idempotency_key)`` возвращает прежний
        ``job_id`` (без дублей элементов). Каждый URL проходит SSRF-проверку
        (:func:`core.util.security.validate_url`) до постановки.
        """
        request = dict(payload or {})
        session_id = str(request.get("session_id") or "")
        run_id = str(request.get("run_id") or "")
        idempotency_key = str(request.get("idempotency_key") or "")
        for field, value in (
            ("run_id", run_id),
            ("session_id", session_id),
            ("idempotency_key", idempotency_key),
        ):
            if not value:
                raise ValueError(f"crawl/submit: обязательное поле {field} — непустая строка")
        items = self._normalize_items(request.get("items"))
        conn = self._connect()
        # §10: robots-политика строится из request-level ``user_agent`` (дефолт ``"*"``) и
        # вешается на краулеры до диспатча; ``plan.robots=False`` делает её инертной.
        self._configure_robots(_opt_str(request.get("user_agent")))

        with self._lock:
            existing = conn.execute(
                "SELECT job_id, total, session_id FROM edge_jobs"
                " WHERE run_id = ? AND session_id = ? AND idempotency_key = ?",
                (run_id, session_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                return {
                    "job_id": str(existing["job_id"]),
                    "accepted": True,
                    "total": int(existing["total"]),
                    "session_id": str(existing["session_id"]),
                }

            outstanding = conn.execute(
                "SELECT COUNT(*) AS n FROM edge_job_items i"
                " JOIN edge_jobs j ON j.job_id = i.job_id"
                " WHERE j.status NOT IN ('completed','partial','cancelled','failed')"
            ).fetchone()["n"]
            if int(outstanding) >= int(self._config.queue_limit):
                return {
                    "job_id": "",
                    "accepted": False,
                    "total": 0,
                    "session_id": session_id,
                }

            job_id = str(self._id_factory())
            now = self._clock()
            with conn:
                conn.execute(
                    "INSERT INTO edge_jobs (job_id, run_id, session_id, idempotency_key,"
                    " status, total, pending, running, fetched, failed, cancelled, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?)",
                    (
                        job_id,
                        run_id,
                        session_id,
                        idempotency_key,
                        JOB_CREATED,
                        len(items),
                        len(items),
                        now,
                    ),
                )
                conn.executemany(
                    "INSERT INTO edge_job_items (job_id, product_id, url, status, engine,"
                    " kind, fetch, extract, strategy, wait_for, lang, robots, rate_limit)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            job_id,
                            item["product_id"],
                            item["url"],
                            ITEM_PENDING,
                            item["engine"],
                            item["kind"],
                            item["fetch"],
                            item["extract"],
                            item["strategy"],
                            item["wait_for"],
                            item["lang"],
                            int(item["robots"]),
                            _rate_limit_json(item["rate_limit"]),
                        )
                        for item in items
                    ],
                )

        self.start()
        self._dispatch()
        return {
            "job_id": job_id,
            "accepted": True,
            "total": len(items),
            "session_id": session_id,
        }

    def _normalize_items(self, raw_items: Any) -> list[dict[str, Any]]:
        """Валидирует элементы (SSRF), резолвит движок, готовит строки для вставки."""
        if not isinstance(raw_items, (list, tuple)) or not raw_items:
            raise ValueError("crawl/submit: поле items — непустой список")
        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(raw_items):
            if not isinstance(item, Mapping):
                raise ValueError(f"crawl/submit: элемент items[{index}] должен быть объектом")
            product_id = str(item.get("product_id") or "")
            url = str(item.get("url") or "")
            if not product_id or not url:
                raise ValueError(
                    f"crawl/submit: элемент items[{index}] требует product_id и url"
                )
            validate_url(url)  # SSRF (ValueError при нарушении)
            engine = self._resolve_engine(item.get("engine"), item.get("fetch"))
            normalized.append(
                {
                    "product_id": product_id,
                    "url": url,
                    "engine": engine,
                    "kind": _opt_str(item.get("kind")),
                    "fetch": _opt_str(item.get("fetch")),
                    "extract": _opt_str(item.get("extract")),
                    "strategy": _opt_str(item.get("strategy")),
                    "wait_for": _opt_str(item.get("wait_for")),
                    "lang": _opt_str(item.get("lang")),
                    # §10/§11.1: additive-оси терпимо (malformed → False/None, без падения submit).
                    "robots": _opt_bool(item.get("robots")),
                    "rate_limit": _opt_rate_limit(item.get("rate_limit")),
                }
            )
        return normalized

    def _available_engines(self) -> set[str]:
        """Движки, доступные на этом edge-узле (потоковые + процессные)."""
        return set(self._thread_engines) | set(self._process_engines)

    def _resolve_engine(self, explicit: Any, fetch_axis: Any) -> str:
        """Резолвит движок элемента: явный ``engine`` → ось ``fetch`` → дефолт ``http``.

        Best-effort фолбэк для browser-запросов (``browser`` → ``scrapy`` → ``http``,
        зеркало ``_EngineCrawler``). Явно неизвестный движок сохраняется как есть —
        честный сбой в воркере, а не тихая подмена.
        """
        available = self._available_engines()
        if explicit not in (None, ""):
            requested = str(explicit).strip().lower()
        elif str(fetch_axis or "").strip().lower() == "browser":
            requested = "browser"
        else:
            requested = "http"
        if requested in available:
            return requested
        if requested in ("browser", "scrapy"):
            for engine in ("browser", "scrapy", "http"):
                if engine in available:
                    return engine
        return requested

    def _configure_robots(self, user_agent: str | None) -> None:
        """Строит robots-политику из request-level ``user_agent`` и вешает её на краулеры (§10).

        Политика ``robots.txt`` приходит с ядра **условно от discovery** (пер-элемент
        ``plan.robots``), но сам ``RobotsGate`` edge строит сам — поверх отдельного
        HTTP-краулера (фетч ``robots.txt`` ленивый, только при ``plan.robots``). Гейт
        вешается на in-process краулеры; процессные получают его из ``user_agent`` в
        инициализаторе воркера. Смена ``user_agent`` пересобирает политику (кеш парсеров
        привязан к агенту).
        """
        with self._lock:
            if self._robots_gate is not None and self._robots_user_agent == user_agent:
                return
            self._robots_user_agent = user_agent
            gate = self._robots_factory(user_agent)
            self._robots_gate = gate
            for crawler in self._crawlers.values():
                _attach_robots(crawler, gate)

    # -- status / cancel / collect --------------------------------------------

    def status(self, job_id: str) -> dict[str, Any]:
        """Прогресс задания: счётчики и наблюдаемые ``engines``/``kinds``."""
        conn = self._connect()
        with self._lock:
            job = self._get_job(conn, job_id)
            engines = [
                str(row["engine"])
                for row in conn.execute(
                    "SELECT DISTINCT engine FROM edge_job_items"
                    " WHERE job_id = ? AND engine IS NOT NULL ORDER BY engine",
                    (str(job_id),),
                )
            ]
            kinds = [
                str(row["kind"])
                for row in conn.execute(
                    "SELECT DISTINCT kind FROM edge_job_items"
                    " WHERE job_id = ? AND kind IS NOT NULL ORDER BY kind",
                    (str(job_id),),
                )
            ]
        return {
            "job_id": str(job_id),
            "status": str(job["status"]),
            "total": int(job["total"]),
            "pending": int(job["pending"]),
            "running": int(job["running"]),
            "fetched": int(job["fetched"]),
            "failed": int(job["failed"]),
            "cancelled": int(job["cancelled"]),
            "engines": engines,
            "kinds": kinds,
        }

    def cancel(self, job_id: str) -> dict[str, Any]:
        """Отменяет задание: ``pending``/``running`` элементы → ``cancelled``."""
        conn = self._connect()
        with self._lock:
            self._get_job(conn, job_id)
            with conn:
                cursor = conn.execute(
                    "UPDATE edge_job_items SET status = ?"
                    " WHERE job_id = ? AND status IN (?, ?)",
                    (ITEM_CANCELLED, str(job_id), ITEM_PENDING, ITEM_RUNNING),
                )
                cancelled = int(cursor.rowcount or 0)
                conn.execute(
                    "UPDATE edge_jobs SET status = ?, finished_at = ? WHERE job_id = ?",
                    (JOB_CANCELLED, self._clock(), str(job_id)),
                )
            self._recompute_job(conn, str(job_id))
        return {"job_id": str(job_id), "cancelled": cancelled}

    def collect(
        self, job_id: str, cursor: Any = None, limit: Any = None
    ) -> dict[str, Any]:
        """Страница результатов задания (контент, не evidence — I19).

        Порядок стабилен по ``(product_id, url)``; ``cursor`` — непрозрачный маркер
        (десятичное смещение). Блоб читается в ``raw.bytes_b64``; размер страницы
        ограничен ``limit`` и бюджетом ``_COLLECT_MAX_BYTES``. Идемпотентно при
        неизменном наборе. ``done`` — когда все элементы терминальны.
        """
        conn = self._connect()
        with self._lock:
            self._get_job(conn, job_id)
            rows = conn.execute(
                "SELECT * FROM edge_job_items WHERE job_id = ?"
                " ORDER BY product_id, url",
                (str(job_id),),
            ).fetchall()
            start = _parse_cursor(cursor)
            if start > len(rows):
                start = len(rows)
            page_limit = max(1, _bool_int(limit, _DEFAULT_COLLECT_LIMIT) or _DEFAULT_COLLECT_LIMIT)

            items: list[dict[str, Any]] = []
            total_bytes = 0
            index = start
            while index < len(rows) and len(items) < page_limit:
                item = self._collect_item(rows[index])
                size = int(item["raw"]["size"]) if item["raw"] is not None else 0
                if items and total_bytes + size > _COLLECT_MAX_BYTES:
                    break
                items.append(item)
                total_bytes += size
                index += 1

            next_cursor = str(index) if index < len(rows) else None
            done = all(str(row["status"]) in ITEM_TERMINAL for row in rows)
        return {"items": items, "next_cursor": next_cursor, "done": done}

    def _collect_item(self, row: sqlite3.Row) -> dict[str, Any]:
        """Нормализует элемент в форму ответа ``crawl/collect`` (raw из блоба)."""
        raw: dict[str, Any] | None = None
        blob_ref = row["blob_ref"]
        if str(row["status"]) == ITEM_FETCHED and blob_ref:
            data = self._read_blob(str(blob_ref))
            if data is not None:
                raw = {
                    "hash": str(row["content_hash"] or content_hash(data)),
                    "size": len(data),
                    "bytes_b64": base64.b64encode(data).decode("ascii"),
                }
        return {
            "product_id": str(row["product_id"]),
            "url": str(row["url"]),
            "status": str(row["status"]),
            "engine": _opt_str(row["engine"]),
            "kind": _opt_str(row["kind"]),
            "http_status": row["http_status"],
            "content_type": _opt_str(row["content_type"]),
            "fetched_at": _opt_str(row["fetched_at"]),
            "content_hash": _opt_str(row["content_hash"]),
            "final_url": _opt_str(row["final_url"]),
            "raw": raw,
            "error": _opt_str(row["error"]),
        }

    # -- пулы воркеров ---------------------------------------------------------

    def _build_pools(self) -> None:
        """Собирает потоковый и (при наличии) процессный пулы из ``config.workers``."""
        thread_slots = sum(
            max(0, int(self._config.workers.get(engine, 0)))
            for engine in self._thread_engines
        )
        self._thread_slots = max(1, thread_slots)
        self._thread_pool = ThreadPoolExecutor(
            max_workers=self._thread_slots, thread_name_prefix="edge-crawl"
        )
        self._process_slots = 0
        self._process_pool = None
        if self._process_engines and self._crawler_factory is not None:
            self._process_slots = max(
                1,
                sum(
                    max(0, int(self._config.workers.get(engine, 0)))
                    for engine in self._process_engines
                ),
            )
            context = self._process_context or _default_process_context()
            self._process_pool = ProcessPoolExecutor(
                max_workers=self._process_slots,
                mp_context=context,
                initializer=_process_worker_init,
                initargs=(
                    self._crawler_factory,
                    self._process_engines,
                    self._limiter_factory,
                    self._robots_user_agent,
                ),
            )

    def _max_inflight(self) -> int:
        """Потолок одновременных фетчей (потоки + процессы)."""
        return max(1, self._thread_slots + self._process_slots)

    def _drain(
        self,
        thread_pool: ThreadPoolExecutor | None,
        process_pool: ProcessPoolExecutor | None,
        timeout: float | None,
    ) -> None:
        """Сливает пулы: снимает не начатые задачи и ограниченно ждёт запущенные (C5b).

        * ``cancel_futures=True`` снимает ещё не начатые фьючеры;
        * при ``timeout is None`` — ждём завершения запущенных воркеров без лимита
          (``shutdown(wait=True)``);
        * при заданном ``timeout`` — **ограниченный** join воркер-потоков/процессов по
          дедлайну; если кто-то не завершился, пишем предупреждение в ``stderr`` и
          продолжаем остановку (не висим молча). Жёсткий дедлайн самого фетча — на
          стороне адаптеров (их таймауты), это лишь следствие.
        """
        pools = [pool for pool in (thread_pool, process_pool) if pool is not None]
        if timeout is None:
            for pool in pools:
                try:
                    pool.shutdown(wait=True, cancel_futures=True)
                except Exception:  # noqa: BLE001 - останов обязан быть graceful
                    pass
            return

        deadline = time.monotonic() + max(0.0, float(timeout))
        for pool in pools:
            try:
                pool.shutdown(wait=False, cancel_futures=True)
            except Exception:  # noqa: BLE001
                pass
        alive: list[Any] = []
        for pool in pools:
            for worker in _pool_workers(pool):
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    try:
                        worker.join(remaining)
                    except Exception:  # noqa: BLE001
                        pass
                if worker.is_alive():
                    alive.append(worker)
        if alive:
            sys.stderr.write(
                f"edge: scheduler.stop(): {len(alive)} воркер(ов) не завершились за "
                f"{float(timeout)} с; продолжаю остановку "
                "(жёсткий дедлайн фетча — на стороне адаптеров)\n"
            )

    # -- recovery --------------------------------------------------------------

    def _recover(self, conn: sqlite3.Connection) -> None:
        """Recovery-on-start: ``running``-элементы → ``pending`` + пересчёт счётчиков.

        Зеркалит ``store.recover_crawl_session``: прерванный процесс не теряет
        намерения краула. Идемпотентно.

        **Скоуп — только нетерминальные задания (C4).** Элементы заданий в терминальном
        статусе не «оживают»: крах между claim и cancel не должен воскрешать элементы
        отменённого/проваленного задания.
        """
        with self._lock:
            with conn:
                conn.execute(
                    "UPDATE edge_job_items SET status = ? WHERE status = ?"
                    f" AND {_non_terminal_job_filter()}",
                    (ITEM_PENDING, ITEM_RUNNING, *_TERMINAL_PARAMS),
                )
            rows = conn.execute(
                "SELECT job_id FROM edge_jobs WHERE status NOT IN "
                f"({_TERMINAL_PLACEHOLDERS})",
                _TERMINAL_PARAMS,
            ).fetchall()
            for row in rows:
                self._recompute_job(conn, str(row["job_id"]))

    # -- диспетчеризация -------------------------------------------------------

    def _dispatch(self) -> None:
        """Заполняет свободные слоты: атомарный claim ``pending`` → submit в пул.

        **Без реэнтрантности (C5a).** Done-callback ``_on_done`` может сработать синхронно
        прямо из ``_submit_row`` (future уже готов) и вызвать ``_dispatch`` повторно.
        Флаг :attr:`_dispatching` делает активным ровно одного диспетчера: реэнтрантный
        вызов сразу выходит, а внешний цикл продолжает добирать слоты (``_inflight`` уже
        уменьшен callback-ом). Это исключает рекурсию/двойной claim.
        """
        if not self._started:
            self.start()
            return
        with self._lock:
            if self._stopped or self._dispatching:
                return
            self._dispatching = True
            try:
                while self._inflight < self._max_inflight():
                    row = self._claim_pending()
                    if row is None:
                        break
                    self._inflight += 1
                    self._submit_row(row)
            finally:
                self._dispatching = False

    def _claim_pending(self) -> sqlite3.Row | None:
        """Атомарно берёт один ``pending``-элемент (под ``self._lock``: без гонок).

        Скоуп — только нетерминальные задания (C4): элементы терминального задания
        (``completed|partial|cancelled|failed``) никогда не клеймятся.
        """
        conn = self._conn
        assert conn is not None
        row = conn.execute(
            "SELECT * FROM edge_job_items WHERE status = ?"
            f" AND {_non_terminal_job_filter()}"
            " ORDER BY job_id, product_id, url LIMIT 1",
            (ITEM_PENDING, *_TERMINAL_PARAMS),
        ).fetchone()
        if row is None:
            return None
        with conn:
            cursor = conn.execute(
                "UPDATE edge_job_items SET status = ?, attempts = attempts + 1"
                " WHERE job_id = ? AND product_id = ? AND url = ? AND status = ?",
                (
                    ITEM_RUNNING,
                    row["job_id"],
                    row["product_id"],
                    row["url"],
                    ITEM_PENDING,
                ),
            )
            if (cursor.rowcount or 0) == 0:  # pragma: no cover - под lock недостижимо
                return None
            conn.execute(
                "UPDATE edge_jobs SET running = running + 1, pending = pending - 1"
                " WHERE job_id = ?",
                (row["job_id"],),
            )
            conn.execute(
                "UPDATE edge_jobs SET status = ?, started_at = COALESCE(started_at, ?)"
                " WHERE job_id = ? AND status = ?",
                (JOB_RUNNING, self._clock(), row["job_id"], JOB_CREATED),
            )
        return row

    def _submit_row(self, row: sqlite3.Row) -> None:
        """Отправляет элемент в подходящий пул (поток для in-process, процесс иначе)."""
        engine = str(row["engine"] or "http")
        plan = self._plan_for(row)
        process_pool = self._process_pool
        if engine in self._process_engines and process_pool is not None:
            future = process_pool.submit(_process_fetch, engine, str(row["url"]), plan)
        else:
            crawler = self._crawlers.get(engine)
            thread_pool = self._thread_pool
            if crawler is None or thread_pool is None:
                self._finish_failed(
                    row, f"движок {engine!r} не сконфигурирован на этом edge-узле"
                )
                # Слот не занят future → освобождаем его и пересчитываем задание.
                self._inflight = max(0, self._inflight - 1)
                self._recompute_job(self._conn, str(row["job_id"]))
                return
            future = thread_pool.submit(
                _thread_fetch, crawler, engine, str(row["url"]), plan, self._limiter
            )
        future.add_done_callback(lambda fut, r=row: self._on_done(r, fut))

    def _plan_for(self, row: sqlite3.Row) -> FetchPlan:
        """Строит ``FetchPlan`` из осей элемента (мягкая деградация к дефолтам).

        ``robots`` (§10) и ``rate_limit`` (A2c, §11.2) переносятся с ядра пер-элемент;
        отсутствие/``NULL`` ⇒ ``False``/``None`` (дефолтные прогоны байт-идентичны).
        """
        return FetchPlan(
            fetch=_coerce_enum(FetchMode, row["fetch"], FetchMode.HTTP),
            extract=_coerce_enum(ExtractionRoute, row["extract"], ExtractionRoute.AUTO),
            strategy=_coerce_enum(CrawlStrategy, row["strategy"], CrawlStrategy.AUTO),
            wait_for=_opt_str(row["wait_for"]),
            engine=str(row["engine"]) if row["engine"] else None,
            robots=bool(_bool_int(row["robots"])),
            rate_limit=_rate_limit_from_json(row["rate_limit"]),
        )

    def _on_done(self, row: sqlite3.Row, future: Any) -> None:
        """Callback воркера: строка результата → блоб/БД → пересчёт → добирание очереди."""
        try:
            result = future.result()
        except BaseException as exc:  # noqa: BLE001 - сбой future → failed-элемент
            result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        with self._lock:
            self._inflight = max(0, self._inflight - 1)
            if isinstance(result, Mapping) and result.get("ok"):
                self._finish_fetched(row, result.get("page"))
            else:
                error = result.get("error") if isinstance(result, Mapping) else "неизвестный сбой"
                self._finish_failed(row, str(error or "неизвестный сбой"))
            self._recompute_job(self._conn, str(row["job_id"]))
        if not self._stopped:
            self._dispatch()

    def _finish_fetched(self, row: sqlite3.Row, page: Any) -> None:
        """Записывает блоб и метаданные успешного фетча (только если элемент ``running``).

        **Блоб пишется после подтверждённого перехода (C3).** Сначала выполняется
        guard-``UPDATE ... WHERE status = RUNNING``; если он затронул 0 строк (элемент
        отменён/снят, пока воркер фетчил), блоб **не** пишется — иначе он остался бы
        сиротой. Content-addressed дедуп сохраняется: ``_write_blob`` идемпотентен по
        ``{job_id}/{hash}.bin``.
        """
        if page is None:
            self._finish_failed(row, "воркер вернул пустой результат")
            return
        content = bytes(getattr(page, "content", b"") or b"")
        digest = content_hash(content)
        blob_ref = self._blob_ref(str(row["job_id"]), digest)
        engine = _opt_str(getattr(page, "engine", None)) or _opt_str(row["engine"])
        fetched_at = _opt_str(getattr(page, "fetched_at", None)) or self._clock()
        status_code = _opt_int(getattr(page, "status_code", None))
        content_type = _opt_str(getattr(page, "content_type", None))
        # §6a: финальный URL после редиректов (``RawPage.url``) — ядру нужен для базы
        # ссылок и redirect-scope; ключ элемента сеанса остаётся запрошенным ``url``.
        final_url = _opt_str(getattr(page, "url", None))
        conn = self._conn
        assert conn is not None
        with conn:
            cursor = conn.execute(
                "UPDATE edge_job_items SET status = ?, engine = ?, http_status = ?,"
                " content_type = ?, fetched_at = ?, content_hash = ?, blob_ref = ?,"
                " final_url = ?, error = NULL"
                " WHERE job_id = ? AND product_id = ? AND url = ? AND status = ?",
                (
                    ITEM_FETCHED,
                    engine,
                    status_code,
                    content_type,
                    fetched_at,
                    digest,
                    blob_ref,
                    final_url,
                    row["job_id"],
                    row["product_id"],
                    row["url"],
                    ITEM_RUNNING,
                ),
            )
        if (cursor.rowcount or 0) == 0:
            # Элемент больше не ``running`` (отменён/восстановлен) — перехода не было,
            # блоб не создаём: сирота не возникает (C3).
            return
        self._write_blob(str(row["job_id"]), content)

    def _finish_failed(self, row: sqlite3.Row, error: str) -> None:
        """Помечает элемент ``failed`` c текстом ошибки (только если он ``running``)."""
        conn = self._conn
        if conn is None:  # pragma: no cover - защита от раннего сбоя
            return
        with conn:
            conn.execute(
                "UPDATE edge_job_items SET status = ?, error = ?"
                " WHERE job_id = ? AND product_id = ? AND url = ? AND status = ?",
                (
                    ITEM_FAILED,
                    error,
                    row["job_id"],
                    row["product_id"],
                    row["url"],
                    ITEM_RUNNING,
                ),
            )

    def _recompute_job(self, conn: sqlite3.Connection | None, job_id: str) -> None:
        """Пересчитывает счётчики задания и его статус (терминал → ``finished_at``)."""
        if conn is None:  # pragma: no cover - защита
            return
        counts = {status: 0 for status in (ITEM_PENDING, ITEM_RUNNING, ITEM_FETCHED, ITEM_FAILED, ITEM_CANCELLED)}
        for row in conn.execute(
            "SELECT status, COUNT(*) AS n FROM edge_job_items WHERE job_id = ? GROUP BY status",
            (job_id,),
        ):
            counts[str(row["status"])] = int(row["n"])
        total = sum(counts.values())
        job = conn.execute(
            "SELECT status, started_at FROM edge_jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        if job is None:  # pragma: no cover - защита
            return

        current = str(job["status"])
        terminal_known = current in JOB_TERMINAL
        if current == JOB_CANCELLED:
            status = JOB_CANCELLED
        elif counts[ITEM_PENDING] or counts[ITEM_RUNNING]:
            status = JOB_RUNNING if job["started_at"] else JOB_CREATED
        else:
            defects = counts[ITEM_FAILED] + counts[ITEM_CANCELLED]
            status = JOB_COMPLETED if defects == 0 else JOB_PARTIAL

        with conn:
            conn.execute(
                "UPDATE edge_jobs SET status = ?, total = ?, pending = ?, running = ?,"
                " fetched = ?, failed = ?, cancelled = ? WHERE job_id = ?",
                (
                    status,
                    total,
                    counts[ITEM_PENDING],
                    counts[ITEM_RUNNING],
                    counts[ITEM_FETCHED],
                    counts[ITEM_FAILED],
                    counts[ITEM_CANCELLED],
                    job_id,
                ),
            )
            if status in JOB_TERMINAL and not terminal_known:
                conn.execute(
                    "UPDATE edge_jobs SET finished_at = ? WHERE job_id = ?",
                    (self._clock(), job_id),
                )

    # -- блоб-хранилище (content-addressed) ------------------------------------

    @staticmethod
    def _blob_ref(job_id: str, digest: str) -> str:
        """Относительная ссылка блоба ``{job_id}/{hash}.bin`` (единый формат)."""
        return f"{job_id}/{digest}.bin"

    def _write_blob(self, job_id: str, content: bytes) -> tuple[str, str]:
        """Пишет байты в ``{blob_dir}/{job_id}/{hash}.bin`` (атомарно); ``(hash, ref)``."""
        digest = content_hash(content)
        ref = self._blob_ref(job_id, digest)
        path = self._blob_dir / ref
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
            tmp.write_bytes(content)
            os.replace(tmp, path)
        return digest, ref

    def _read_blob(self, blob_ref: str) -> bytes | None:
        """Читает блоб по относительной ссылке (защита от traversal); ``None`` при сбое."""
        try:
            base = self._blob_dir.resolve()
            path = (self._blob_dir / blob_ref).resolve()
        except OSError:  # pragma: no cover - защита
            return None
        if path != base and base not in path.parents:
            return None
        try:
            return path.read_bytes()
        except OSError:
            return None


# ---------------------------------------------------------------------------
# Мелкие нормализаторы
# ---------------------------------------------------------------------------


def _opt_str(value: Any) -> str | None:
    """``None`` → ``None``; иначе строка (пустая → ``None``)."""
    if value is None:
        return None
    text = str(value)
    return text or None


def _opt_int(value: Any) -> int | None:
    """``None`` → ``None``; иначе ``int`` (нечисло → ``None``)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _opt_float(value: Any) -> float | None:
    """``None`` → ``None``; иначе ``float`` (``bool``/нечисло/``inf``/``nan`` → ``None``)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in (float("inf"), float("-inf")):  # nan/inf
        return None
    return number


def _opt_bool(value: Any) -> bool:
    """Терпимый ``bool``: истинно только ``True``; иное (в т.ч. malformed) → ``False``."""
    return value is True


def _opt_rate_limit(value: Any) -> RateLimitSpec | None:
    """Терпимый разбор ``rate_limit`` (§11.1): ``{min_s,max_s}`` → ``RateLimitSpec``.

    Отсутствие/``null``/malformed (не-объект, не-числа, отрицательные, ``max_s < min_s``)
    → ``None`` (без пейсинга). Никогда не бросает — submit не падает из-за поля политики.
    """
    if not isinstance(value, Mapping):
        return None
    min_s = _opt_float(value.get("min_s"))
    max_s = _opt_float(value.get("max_s"))
    if min_s is None or max_s is None or min_s < 0.0 or max_s < min_s:
        return None
    try:
        return RateLimitSpec(min_s=min_s, max_s=max_s)
    except ValueError:
        return None


def _rate_limit_json(spec: RateLimitSpec | None) -> str | None:
    """``RateLimitSpec`` → JSON ``{"min_s","max_s"}`` для хранения; ``None`` → ``NULL``."""
    if spec is None:
        return None
    return json.dumps({"min_s": float(spec.min_s), "max_s": float(spec.max_s)})


def _rate_limit_from_json(value: Any) -> RateLimitSpec | None:
    """Обратный разбор ``rate_limit`` из БД (терпимо: ``NULL``/битый JSON → ``None``)."""
    if value in (None, ""):
        return None
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return None
    return _opt_rate_limit(decoded)


def _attach_robots(crawler: Any, gate: RobotsGate) -> None:
    """Вешает robots-политику на адаптер (§10).

    Адаптеры читают политику из ``self._robots`` (``HttpCrawler``/``BrowserCrawler``), но
    публичного сеттера нет, а in-process краулеры построены composition root'ом **до**
    того, как edge узнал ``user_agent`` из submit-запроса. Поэтому политика вешается
    присваиванием — это ровно тот атрибут, по которому адаптеры гейтят
    (``self._robots is not None and plan.robots``). ``plan.robots=False`` делает её
    инертной (никаких лишних fetch ``robots.txt``).
    """
    try:
        crawler._robots = gate  # noqa: SLF001 - документированный seam адаптеров (§10)
    except (AttributeError, TypeError):  # pragma: no cover - защита от нестандартных краулеров
        pass


def _coerce_enum(enum_cls: Any, value: Any, default: Any) -> Any:
    """Приводит строку к элементу enum; неизвестное/пустое → ``default``."""
    if value in (None, ""):
        return default
    try:
        return enum_cls(str(value))
    except ValueError:
        return default


def _parse_cursor(cursor: Any) -> int:
    """Непрозрачный маркер ``collect`` → смещение (нечисло/``None`` → 0; не отрицательно)."""
    if cursor in (None, ""):
        return 0
    try:
        return max(0, int(str(cursor)))
    except (TypeError, ValueError):
        return 0
