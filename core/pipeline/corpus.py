"""Сборка корпуса — Ш5 (02 §B.1): retention → extract → dedup → chunk → index_manifest.

Фаза Ш5 превращает внешний контент (краул доменных профилей / SERP) в проиндексированные
продукт-партиции. Путь контента фиксирован и воспроизводим (I19):

```
url ── CrawlerPort.fetch ──▶ RawPage ── retention в corpus_raw/{product_id}/{hash}.bin
    ── ExtractorPort.extract ──▶ ExtractedDoc
    ── dedup(content_hash) ──▶ ChunkerPort.chunk(doc, document_id)
    ── content_hash(chunk.text) ──▶ CorpusChunk (build_id = partition_build_id)
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
страница: не ретейнится/не ингестится (I19). ``<loc>`` — **подчинённые** seeds: проходят
``validate_url`` (SSRF) и scope корня (``Frontier.in_scope``: host + path-граница +
traversal-guard), затем тот же блэклист/``max_documents``/``max_depth``, что и прочие seeds.
Вне scope корня loc не ставится (I17: host-wide sitemap при path-scoped seed не заливает
чужие продукты) и попадает в **провенанс усечения** корня
(``truncation_records[*].skipped_out_of_scope``), а не в ``errors``: при path-scoped seed
это нормальная конфигурация, а не сбой. Классы отказа разделены: ``host`` — sitemap
рекламирует другой host (сигнал чужого сайта/SSRF), ``path`` — тот же host, но путь вне
префикса корня.

**Иммутабельность и два id (09 §8.1).** ``partition_build_id`` (``pb_``) — контент-адресуемый
отпечаток **одной** продукт-партиции: ``short_hash`` от (``_PARTITION_SCHEMA`` + пакет +
``product_id`` + ``chunker_version`` + ``embed_model_id`` + ``extractor_version`` +
``lexical_engine_version`` + ``vector_store`` + ``product_content_token``). ``product_content_token``
— порядко-независимый отпечаток содержимого партиции (sorted ``(chunk_id, content_hash)``).
``composition_id`` (``cb_``) — отпечаток состава когорты и версий/дайджестов её партиций
(:func:`core.pipeline.composition.derive_composition_id`); это ``runs.index_build_id``.
``params_token`` (каталог/фокус) и ``snapshot_date`` в ``pb`` **не входят**: они не меняют
чанки/векторы (снимок — провенанс). Поэтому разные корпуса не могут получить один ``pb``
(same-id/different-content коллизия), а те же входы и то же содержимое — один. Любое изменение
входов/контента даёт новую сборку; старая не мутируется (§B.1/§13, K2).
``retrieval_profile``/``retrieval_cutoff_K``/``retrieval_params_json`` **не** входят ни в один
id: ретривал-конфиг не меняет чанкинг/векторы.

**Два прохода (один вызов, без ре-краула).** Pass A (ingest) по каждому продукту выполняет
краул → retention (I19) → экстракцию → дедуп(``content_hash``) → чанкинг и копит
``_ProductIngest`` в памяти; ``corpus_docs`` персистятся **инкрементально** — сразу при
коммите каждого документа (retention-провенанс не зависит от сборки), поэтому жёсткое
убийство теряет максимум документ «в полёте», а не весь продукт; ``build_id`` чанков пока
``None``. После Pass A выводятся per-product ``partition_build_id`` и ``composition_id``
(``runs.index_build_id``). Pass B (index) проставляет ``build_id = pb``, строит
``IndexManifestEntry`` и либо переиспользует уже зафиксированную партицию (ноль эмбеддингов,
09 §8.5), либо вызывает ``_embed_and_build``; при успехе персистятся ``corpus_chunks`` +
``index_manifest`` («сначала диск, потом БД»), а после фиксации **всех** членов пишется
иммутабельная композиция ``_compositions/{cid}.json``. Никакого round/parent-чейнинга.

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
при попытке изменить уже зафиксированную сборку, K2/§13), ``corpus_docs`` уже
персистенчены инкрементально (retention), но ``corpus_chunks``/``index_manifest`` остаются
неизменными, а
отказ попадает в ``errors`` с указанием, что изменение корпуса требует нового
``run_id``/раунда. Это зеркалит порядок в ``serp_loop`` (build → ``save_index_manifest``).

**Внедрение зависимостей.** Адаптеры (``core/adapters/*``) здесь не импортируются: фаза
работает только через порты (A1). ``embedder``/``index_builder`` опциональны — если они не
переданы, фаза строит корпус и ``index_manifest``, а эмбеддинг/физическую сборку индекса
подключает W8 (composition root). При их наличии фаза вызывает ``embed``/``build``/``commit``
best-effort (сбой изолируется).

**Инкрементальный субстрат (09 §6.3/§9, MVP-3b).** При переданном ``state``
(:class:`~core.ports.state.ProductStatePort`) фаза дополнительно ведёт durable-состояние
URL/документов/чанков:

* ``url_state`` читается/пишется **для каждого продукта** независимо от режима — субстрат
  наполняется уже на первом прогоне; режим ``incremental`` дополнительно расширяет work-set
  известными URL (``plan_incremental``) — новым/полным продуктам BFS/sitemap остаётся
  прежним;
* политика ``auto``/``revalidate`` добавляет валидаторы условного GET
  (``If-None-Match``/``If-Modified-Since``) в ``FetchPlan`` (локальный путь) и
  ``CrawlWorkItem`` (исполнитель), поэтому ``304`` не тратит трафик;
* ответ ``304`` или ``200`` с прежним ``content_hash`` переиспользует
  ``document_version`` и её chunk-set из ``chunk_store`` без вызова экстрактора/чанкера;
  каждый переиспользованный чанк проверяется по I19 (``content_hash(text) == row.content_hash``),
  при любом расхождении — обычная экстракция + нарезка и видимая запись в ``errors``;
* все сбои state-записи изолируются в ``errors`` (контракт :func:`_persist_docs`): фаза не
  падает и не меняет код возврата.

``state is None`` — прежнее поведение байт-в-байт (никаких чтений/записей субстрата,
пустой ``IndexBuild.doc_counters``, тот же ``index_build_id``).

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import datetime as _dt
import errno
import hashlib
import json
import os
import shutil
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.config.crawl_sources import CrawlSourcesConfig
from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.config.seeds import normalize_seed_url
from core.domain import (
    BLOCKING_FAILURE_KINDS,
    CorpusChunk,
    CorpusCoverage,
    CorpusDoc,
    CrawlItemStatus,
    CrawlRunConfig,
    CrawlSessionItem,
    CrawlStrategy,
    DocumentLevel,
    FailureKind,
    IncrementalKind,
    IndexManifestEntry,
    ProductMode,
    RefreshPolicy,
    ReuseKind,
)
from core.pipeline.crawl_frontier import (
    CrawlLimits,
    Frontier,
    is_blacklisted,
    resolve_crawl_limits,
)
from core.pipeline.crawl_session import (
    default_session_id,
    finalize_session,
    open_session,
    prior_items,
)
from core.pipeline.composition import (
    CompositionView,
    derive_composition_id,
    write_composition,
)
from core.pipeline.crawl_sources import (
    CrawlTarget,
    crawl_sources_digest,
    resolve_crawl_target,
)
from core.pipeline.extract_cache import ExtractionCache
from core.pipeline.fetch_plan import _host_matches, select_domain_profile
from core.pipeline.incremental import (
    IncrementalItem,
    chunk_row_id,
    classify_reuse,
    doc_version_id,
    plan_incremental,
)
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
from core.ports.state import (
    ChunkRow,
    DocumentVersionRow,
    IndexCompositionRow,
    IndexPartitionRow,
    ProductStatePort,
    UrlStateRow,
)
from core.ports.types import Chunk as PortChunk
from core.ports.types import (
    ExtractedDoc,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    RawPage,
)
from core.util.document_formats import (
    BINARY_SNIFF_BYTES,
    classify_archive_bytes,
    document_content_type,
    document_suffix,
    is_attachment_content_type,
    is_attachment_url,
    unsupported_binary_reason,
)
from core.util.hashing import content_hash, short_hash
from core.util.index_partition import build_root as _partition_root
from core.util.index_partition import is_committed as _partition_is_committed
from core.util.index_partition import read_manifest as _read_partition_manifest
from core.util.html_links import extract_links
from core.util.rate_limit import RateLimiter
from core.util.security import validate_url

__all__ = ["ExtractionPool", "IndexBuild", "build_corpus", "derive_corpus_coverage"]

#: Разделитель полей material-строки ``index_build_id`` (unit separator — однозначная склейка).
_FIELD_SEP = "\x1f"

#: Схема формулы ``partition_build_id`` (09 §8.1). Входит в material, поэтому ревизия
#: формулы всегда даёт новые id: старые партиции не переиспользуются молча.
_PARTITION_SCHEMA = "partition-1"

#: P6: подкаталог ``raw_root`` для staging-файлов допущенных артефактов. Тот же ФС, что
#: и ``corpus_raw``, поэтому публикация — ``os.replace`` (переезд, не копия).
_STAGING_DIRNAME = "_staging"

#: Phase 3 B2: максимум попыток подачи работ исполнителю при back-pressure
#: (``accepted=False``). После исчерпания — изолированный ``FAILED`` (``source_unavailable``),
#: а не тихий провал всего прогона.
_SUBMIT_MAX_ATTEMPTS = 5

#: Phase 4/Ф0: ``error_kind`` терминального ``FAILED`` при исчерпании ``--max-attempts``.
#: Канонический класс сбоя (``FailureKind.ATTEMPTS_EXHAUSTED``): отличает «попытки
#: кончились» от сетевого/браузерного сбоя, не выходя за словарь §4.1.
_MAX_ATTEMPTS_ERROR_KIND = FailureKind.ATTEMPTS_EXHAUSTED.value

#: Phase 5.2: семантика источника-каталога (свободная строка-данные, не доменный enum).
#: URL с таким ``target.kind`` — не контент-страница, а лента/канал: он **поллится** через
#: ``Ports.pollers[kind]``, а полученные ссылки-записи добываются обычным путём. Один уровень
#: развёртки: записи повторно не расширяются.
_DIRECTORY_KINDS: frozenset[str] = frozenset({"feed", "channel"})

#: D3: ``kind`` опроса sitemap (``DomainProfile.sitemap``) — свободная строка-данные. Sitemap
#: не входит в ``_DIRECTORY_KINDS``: это per-site опция профиля домена, а не источник
#: ``crawl_sources.yaml``. Разворачивается через ``pollers["sitemap"]`` (local ‖ split).
_SITEMAP_KIND = "sitemap"

#: Классы отказа ``<loc>`` sitemap от scope корня (F6): ``host`` — sitemap рекламирует
#: другой host (сигнал чужого сайта/SSRF), ``path`` — host тот же, но путь вне префикса
#: корня (host-wide sitemap при path-scoped seed — **нормальная** конфигурация). Классы
#: считаются и репортятся раздельно: их смешение скрывало бы host-сигнал в массе path-отказов.
_SITEMAP_SKIP_HOST = "host"
_SITEMAP_SKIP_PATH = "path"

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

#: P4/C1: предохранитель от зависшего задания экстракции — потолок ожидания
#: ``future.result`` **одного артефакта** (НЕ SLA: заведомо щедрый дефолт; конфигурируется
#: аддитивным kwarg ``extract_task_timeout_s``). По таймауту терминально помечается ровно
#: этот артефакт (``extraction_timeout``), пул продолжает работу; пересборка — только если
#: пул не дал ни одного завершения за ``2 × timeout`` (см. :class:`_ExtractPools`).
_EXTRACT_TASK_TIMEOUT_S = 1800.0

#: P4/C1: ограниченное число пересборок мёртвого пула экстракции (краш/OOM/двойной таймаут
#: без единого завершения). После исчерпания — остаток прогона терминально ``FAILED`` с явной
#: записью в ``errors`` (без авто-ретрая и без inline-экстракции в родителе).
_EXTRACT_MAX_POOL_REBUILDS = 2

#: C1: сколько подряд идущих таймаутов **без единого завершения** считаются мёртвым пулом
#: (каждый таймаут — полное окно ожидания ``timeout``, поэтому два подряд = ``2 × timeout``).
_EXTRACT_DEAD_POOL_TIMEOUTS = 2

#: P4: сентинел «экстракция не была отложена» для :func:`build_corpus`-интернала
#: ``_commit_outcome`` (inline-путь вызывает ``_ingest_one``; пул передаёт готовый результат).
_INLINE = object()

#: P0/OOM: явно очистить carry-forward поле элемента сеанса (``_mark`` пропускает ``None``).
_CLEAR = object()

#: P0: размер порции потокового хеширования retained-байтов (resume).
_RESUME_READ_CHUNK = 1024 * 1024

#: Статус активной версии документа (``document_version.status``, 09 §4.2).
_DOC_ACTIVE = "active"
#: Статус вытесненной версии: документ больше не наблюдается по URL (``404``/``410``, 09 §6.3).
_DOC_SUPERSEDED = "superseded"


@dataclass(frozen=True)
class _ReusedChunk:
    """Переиспользованный чанк документа: текст уже верифицирован по I19.

    ``content_hash`` — hash **чистого текста** из ``chunk_store``; он сверен с
    ``chunk_meta.content_hash`` при загрузке (:func:`_load_reused_chunks`), поэтому
    неверифицированный текст сюда не попадает.
    """

    chunk_no: int
    text: str
    content_hash: str
    heading_path: str
    context_prefix: str


@dataclass(frozen=True)
class _DocReuse:
    """Готовая к переиспользованию версия документа + её проверенный chunk-set.

    :param doc_version: строка ``document_version`` (``active``), чей контент совпал.
    :param chunks: чанки в порядке ``chunk_no``; непусты (пустой набор → не reuse).
    """

    doc_version: DocumentVersionRow
    chunks: tuple[_ReusedChunk, ...]


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


def _as_refresh_policy(value: str) -> RefreshPolicy:
    """Приводит строку политики наблюдения к :class:`RefreshPolicy` (дефолт ``auto``)."""
    try:
        return RefreshPolicy(value)
    except ValueError:
        return RefreshPolicy.AUTO


def _as_product_mode(value: str | None) -> ProductMode:
    """Режим продукта по строке плана онбординга; отсутствие/неизвестное — ``new``.

    ``new`` — честный дефолт: продукт без записи в ``product_modes`` ведёт себя как
    первый прогон (полный BFS), а не как инкрементальный.
    """
    if value is None:
        return ProductMode.NEW
    try:
        return ProductMode(value)
    except ValueError:
        return ProductMode.NEW


def _load_reused_chunks(
    state: ProductStatePort,
    doc_version: DocumentVersionRow,
    *,
    chunker_version: str,
    where: str,
    errors: list[str],
) -> tuple[_ReusedChunk, ...] | None:
    """Читает и **верифицирует** chunk-set версии документа из durable-субстрата (I19).

    Переиспользовать можно только текст, который доказуемо тот же, что был проиндексирован:
    для каждого ``chunk_meta`` берётся ``chunk_store.text`` по ``content_hash`` и
    пересчитывается ``content_hash(text)``. Любое расхождение (нет текста, хеш не сошёлся,
    пустой набор) — отказ от переиспользования с видимой записью в ``errors``: вызывающий
    идёт обычным путём экстракции/нарезки. Молчаливая подмена текста запрещена.

    ``chunker_version`` обязан совпасть с версией прогона: иначе границы чанков другой
    версии чанкера молча смешались бы с текущей сборкой (durable ``chunk_meta`` различает
    версии, ``document_version`` — нет).

    :returns: верифицированные чанки в порядке ``chunk_no`` либо ``None`` (не переиспользуем).
    """
    doc_version_id_ = doc_version.doc_version_id
    if doc_version.chunker_version and doc_version.chunker_version != chunker_version:
        errors.append(
            f"{where}: reuse: версия чанкера {doc_version.chunker_version!r} != "
            f"{chunker_version!r} — повторная нарезка"
        )
        return None
    try:
        rows = list(state.iter_doc_chunks(doc_version_id_))
    except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
        errors.append(
            f"{where}: reuse: iter_doc_chunks({doc_version_id_}): "
            f"{type(exc).__name__}: {exc} — повторная нарезка"
        )
        return None
    if not rows:
        errors.append(
            f"{where}: reuse: пустой chunk-set {doc_version_id_!r} — повторная нарезка"
        )
        return None
    reused: list[_ReusedChunk] = []
    for row in rows:
        try:
            text = state.get_chunk_text(row.content_hash)
        except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
            errors.append(
                f"{where}: reuse: get_chunk_text({row.content_hash}): "
                f"{type(exc).__name__}: {exc} — повторная нарезка"
            )
            return None
        if text is None:
            errors.append(
                f"{where}: reuse: текст чанка {row.chunk_id!r} не сохранён "
                "(chunk_store) — повторная нарезка"
            )
            return None
        if content_hash(text) != row.content_hash:
            errors.append(
                f"{where}: reuse: I19: текст чанка {row.chunk_id!r} не совпадает с "
                "content_hash — повторная нарезка (неверифицированный текст не "
                "переиспользуется)"
            )
            return None
        reused.append(
            _ReusedChunk(
                chunk_no=int(row.chunk_no),
                text=text,
                content_hash=row.content_hash,
                heading_path=row.heading_path or "",
                context_prefix=row.context_prefix or "",
            )
        )
    return tuple(reused)


@dataclass(frozen=True)
class _RetainedReject:
    """Retained raw не пригоден по границе исполнения (P0): терминально, без экстракции."""

    error_kind: FailureKind
    reason: str
    path: Path


def _canonical_failure_kind(reason: str | None) -> str:
    """Единый маппинг любого ``error_kind``-значения в канонический :class:`FailureKind`.

    Все записи ``crawl_session_items`` идут через :func:`_mark`, который вызывает этот
    helper: ad-hoc-литералы (``redirect_out_of_scope``) и ``refusal_reason`` адаптеров
    (``robots_disallow``) канонизируются в одном месте, а не в каждом write-site.
    Неизвестное значение не выдумывает класс: это ``source_unavailable`` (честный дефолт).
    """
    if not reason:
        return FailureKind.SOURCE_UNAVAILABLE.value
    try:
        return FailureKind(reason).value
    except ValueError:
        return FailureKind.SOURCE_UNAVAILABLE.value


def derive_corpus_coverage(
    *,
    product_id: str,
    items: Iterable[CrawlSessionItem],
    truncated: bool,
    computed_at: str,
) -> CorpusCoverage:
    """Строит :class:`CorpusCoverage` продукта из персистентных элементов сеанса (Ф0).

    Детерминированно: ``expected_sources`` — число элементов, ``observed_sources`` —
    дошедшие до ``FETCHED``, ``blocked`` — счётчики ``FAILED`` по ``error_kind`` (аудит).

    **Порог неполноты — три независимых условия.**

    1. *Нечитаемый origin.* Origin (``netloc`` URL) считается **нечитаемым**, только
       если из него не получено ни одного ``FETCHED`` элемента и при этом был хотя бы
       один недочёт (любой не-``FETCHED`` исход, кроме ``not_found``). Полностью тёмный
       сайт не может доказать отсутствие никогда.
    2. *In-scope блокирующий сбой.* Любой ``FAILED`` элемент с ``error_kind`` из
       :data:`core.domain.enums.BLOCKING_FAILURE_KINDS` (``robots_disallow``,
       ``parser_failed``, ``extraction_timeout``, ``attempts_exhausted``,
       ``dlp_blocked``, ``quarantined``) делает покрытие неполным **по построению**,
       даже если origin в целом читаем: часть материала продукта осталась ненаблюдённой,
       и ``❌`` на таком корпусе не является доказательством отсутствия. Причина
       блокировки видна в ``blocked``/``partial_blocks`` и в производном
       :attr:`CorpusCoverage.blocking_failures`.
    3. *Ни одного наблюдения.* ``observed_sources == 0`` при ``expected_sources > 0``
       делает покрытие неполным **по построению**: корпус, в котором не прочитано ни
       одного документа, не может доказывать отсутствие. ``not_found`` на каждом URL —
       наблюдение отсутствия *этих URL*, а не доказательство, что у продукта нет
       возможности; полностью ``pending``/``running`` сеанс — отсутствие наблюдения
       вовсе. ``reason`` при этом остаётся ``None``: неполноту объясняют
       ``expected_sources``/``observed_sources``/``blocked``/``unreadable_origins``.

    ``complete = (not truncated) and (not unreadable_origins) and (no blocking failure) and (observed_sources > 0)``.

    Явно **не** блокируют ``complete``:

    * ``not_found`` (HTTP 404/410) — разрешённое наблюдение отсутствия: origin ответил,
      что ресурса нет; остаётся в ``blocked`` как аудит. Сам по себе класс не блокирует,
      но корпус, где **все** URL вернули ``not_found``, неполон по условию (3);
    * ``source_unavailable`` — транзиентная недоступность читаемого origin'а; полностью
      тёмный origin по-прежнему ловится условием (1);
    * ``redirect_out_of_scope`` — уход за границу охвата по определению;
    * сбои конвейера оценки (``search_failed``/``evaluation_failed``/
      ``validation_failed``) — не наблюдения корпуса.

    ``partial_blocks`` фиксирует не-``not_found`` сбои каждого origin'а: человек видит
    частичные отказы, но они больше не «тихие» — блокирующий класс означает неполноту.

    **Человеческий escape hatch.** ``scope_attested_by`` остаётся единственным способом
    опубликовать ``❌`` на неполном корпусе: человек осознанно принимает риск, что
    ненаблюдённая часть сайта могла бы содержать параметр. Машинный ``❌`` в этом случае
    понижается до ``pending``-кандидата (I2c/I6/I7).
    """
    expected = 0
    observed = 0
    blocked: dict[str, int] = {}
    fetched_by_origin: dict[str, int] = {}
    #: origin → число «недостач» (не-FETCHED исходов, кроме ``not_found``).
    losses_by_origin: dict[str, int] = {}
    partial_blocks: dict[str, dict[str, int]] = {}
    for item in items:
        expected += 1
        origin = (
            (urlsplit(item.url).netloc or "").lower()
            or item.url.strip().lower()
            or "unknown"
        )
        if item.status is CrawlItemStatus.FETCHED:
            observed += 1
            fetched_by_origin[origin] = fetched_by_origin.get(origin, 0) + 1
            continue
        kind = item.error_kind if item.status is CrawlItemStatus.FAILED else None
        if kind:
            blocked[kind] = blocked.get(kind, 0) + 1
        if kind == FailureKind.NOT_FOUND.value:
            # Разрешённое наблюдение отсутствия: не потеря и не блокировка покрытия.
            continue
        losses_by_origin[origin] = losses_by_origin.get(origin, 0) + 1
        if kind:
            counts = partial_blocks.setdefault(origin, {})
            counts[kind] = counts.get(kind, 0) + 1

    unreadable_origins = sorted(
        origin for origin in losses_by_origin if fetched_by_origin.get(origin, 0) == 0
    )
    blocking_values = {kind.value for kind in BLOCKING_FAILURE_KINDS}
    blocking_failures = any(kind in blocking_values for kind in blocked)
    return CorpusCoverage(
        product_id=product_id,
        complete=(not truncated)
        and not unreadable_origins
        and not blocking_failures
        and observed > 0,
        expected_sources=expected,
        observed_sources=observed,
        blocked=dict(sorted(blocked.items())),
        unreadable_origins=unreadable_origins,
        partial_blocks={
            origin: dict(sorted(partial_blocks[origin].items()))
            for origin in sorted(partial_blocks)
        },
        truncated=truncated,
        computed_at=computed_at,
    )


def _persist_corpus_coverage(
    store: Any,
    run_id: str,
    session_id: str,
    truncated_products: set[str],
    computed_at: str,
    *,
    products: Iterable[str] | None = None,
) -> None:
    """Вычисляет и персистит покрытие **каждого** продукта плана (Ф0, O3).

    Продукт без элементов сеанса (нулевой seed-набор / preflight обойдён) получает
    строку ``expected_sources=0, complete=False, reason=no_seeds`` — отсутствие
    покрытия не должно читаться как «неизвестно» и не должно давать тихий ``ok``.
    Продукт с элементами, но без единого ``FETCHED`` (все ``not_found``/``pending``),
    получает ``complete=False`` из :func:`derive_corpus_coverage` с ``reason=None``:
    неполноту объясняют счётчики, а не новая причина.
    """
    by_product: dict[str, list[CrawlSessionItem]] = {}
    for item in store.iter_crawl_session_items(session_id):
        by_product.setdefault(item.product_id, []).append(item)
    planned = {str(product_id) for product_id in (products or ())}
    for product_id in sorted(planned | set(by_product)):
        items = by_product.get(product_id, [])
        if not items:
            coverage = CorpusCoverage(
                product_id=product_id,
                complete=False,
                expected_sources=0,
                observed_sources=0,
                computed_at=computed_at,
                reason="no_seeds",
            )
        else:
            coverage = derive_corpus_coverage(
                product_id=product_id,
                items=items,
                truncated=product_id in truncated_products,
                computed_at=computed_at,
            )
        store.save_corpus_coverage(run_id, coverage)


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
    #: для самодостаточности задания и корректности под будущие режимы; C1: пул — фазовый,
    #: ключ ``(уровень, окно)``, поэтому воркер кэширует конвертер по уровню.
    level: DocumentLevel = DocumentLevel.FULL
    #: Ф1.2: корень и бюджет межпрогонного кэша экстракции. Едут **примитивами** — сам
    #: объект кэша не пиклится (payload остаётся picklable by construction); воркер
    #: строит экземпляр из этих полей. ``None`` → кэш в воркере выключен.
    cache_root: str | None = None
    cache_max_bytes: int | None = None


@dataclass(frozen=True)
class ExtractionPool:
    """P4: инъекция исполнителя экстракции из composition root (A1, ``pipeline ↛ adapters``).

    :param pool_factory: фабрика ``(int, DocumentLevel) → Executor`` (размер =
        ``limits.extract_workers``; уровень — свойство корня, C1: пул — фазовый по ключу
        ``(уровень, окно)``); ``local`` —
        ``ProcessPoolExecutor`` (forkserver, инициализатор строит экстрактор нужного уровня
        на воркер), ``split`` — ``ThreadPoolExecutor`` (``RemoteExtractor`` I/O-bound; ось
        ``document_level`` под ``split`` не действует — R-S).
    :param worker: рабочая функция ``_ExtractRequest → ExtractedDoc``. Для процессов —
        module-level (picklable by reference); для потоков допустим bound/closure.
    :param supports_worker_recycling: профиль умеет рециклинг воркеров по счётчику заданий
        (``ProcessPoolExecutor(max_tasks_per_child=N)``). ``False`` (``split``/потоки) —
        ось ``extract_max_tasks_per_child`` не действует (не тихо: запись в ``errors``).
    """

    pool_factory: Callable[..., Executor]
    worker: Callable[[_ExtractRequest], ExtractedDoc]
    supports_worker_recycling: bool = False


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
    """Экстрактор воркера для ``level`` (кэш размера 1; C1: пул — фазовый по уровню).

    Если инициализированный экстрактор подходит (уровень совпал или универсален) — он и
    используется. Иначе (защитный путь) строится фабрикой и кэшируется. Кэш по построению
    размера 1 не является рабочим: инициализатор пула строит экстрактор нужного уровня.
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


def _extract_cache_key(raw_path: str | Path) -> str:
    """Ключ кэша экстракции для retained-файла: ``{content_hash}.bin`` → хеш байтов.

    ``_retain`` называет retained-файл ``{sha256(bytes)}.bin``, поэтому ``stem`` — ровно
    ``content_hash`` тех байтов, которые прочитает воркер. Родительская проба (C1/L4) и
    проба воркера (:func:`_run_extract`) обязаны давать один ключ: это контракт раскладки
    retention (I19), а не случайное совпадение.
    """
    return Path(raw_path).stem


def _run_extract(extractor: ExtractorPort, request: _ExtractRequest) -> ExtractedDoc:
    """Перечитывает retained-байты (I19) и вызывает ``extractor.extract`` на том же ``RawPage``.

    Общий шаг модульного воркер-процесса (:func:`_extract_worker`) и потокового воркера
    (``split``): источник байтов — retained-файл, а не буфер родителя (I19).

    Ключ кэша считается из **фактически прочитанных** байтов (``content_hash(content)``) и
    совпадает с родительским :func:`_extract_cache_key` (``raw_path.stem``) — I19 требует
    верификации от байтов, а не от имени файла.
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
    # Ф1.2: кэш экстракции. Ключ — хеш байтов, **только что прочитанных из retained-файла**
    # (I19): хит принимается лишь при совпадении ключа конверта с этими байтами, поэтому
    # кэш не может подсунуть неверифицированный контент. Пропускается ровно вызов
    # экстрактора — чтение и хеширование retained-байтов остаются.
    level_text = _as_text(request.level)
    key = content_hash(content)
    cache = (
        ExtractionCache(request.cache_root, max_bytes=request.cache_max_bytes)
        if request.cache_root
        else None
    )
    if cache is not None:
        cached = cache.get(key, getattr(extractor, "version", ""), level_text)
        if cached is not None:
            return cached
    extracted = extractor.extract(page, request.route)
    if cache is not None:
        cache.put(extracted, level=level_text, content_hash=key)
    return extracted


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
class _ExtractPoolSlot:
    """Состояние одного ключа фазового пула экстракции (C1/L1)."""

    executor: Executor | None = None
    #: Число выполненных пересборок (bounded :data:`_EXTRACT_MAX_POOL_REBUILDS`).
    rebuilds: int = 0
    #: Пул исчерпал пересборки: остаток прогона по ключу — терминальный отказ, без inline.
    abandoned: bool = False
    #: Подряд идущие таймауты без единого завершения: ``2`` = ``2 × timeout`` без результата.
    stalled_timeouts: int = 0


class _ExtractPools:
    """C1/L1: фазовый реестр пулов экстракции — один пул на прогон, а не на seed-root.

    Ключ — ``(DocumentLevel, window)``: уровень документной экстракции и окно (число
    заданий «в полёте»). Пул переиспользуется **всеми** корнями и продуктами фазы, поэтому
    forkserver и загрузка весов конвертера платятся один раз, а не на каждый корень.
    ``shutdown(wait=True, cancel_futures=True)`` выполняется ровно один раз — в ``finally``
    фазы (:func:`build_corpus`), а не на корень.

    Реестр также несёт фазовое состояние отказов **на ключ**: счётчик пересборок и признак
    «исчерпал пересборки». Исчерпание не воскресает на следующем корне того же ключа
    (иначе бюджет пересборок обходился бы числом корней).

    Таймауты (:meth:`note_timeout`) считаются подряд идущими, пока пул не завершил ни одного
    задания: каждое ожидание длится ``timeout``, поэтому ``2`` подряд = ``2 × timeout`` без
    единого завершения — только тогда пул считается мёртвым и пересобирается (bounded).
    Одиночный зависший артефакт убивает только себя (``extraction_timeout``), не пул.
    """

    def __init__(self, *, max_rebuilds: int = _EXTRACT_MAX_POOL_REBUILDS) -> None:
        self._max_rebuilds = max_rebuilds
        self._slots: dict[tuple[DocumentLevel, int], _ExtractPoolSlot] = {}

    def slot(self, key: tuple[DocumentLevel, int]) -> _ExtractPoolSlot:
        """Состояние ключа (создаётся лениво; I/O нет)."""
        slot = self._slots.get(key)
        if slot is None:
            slot = _ExtractPoolSlot()
            self._slots[key] = slot
        return slot

    def executor(
        self, key: tuple[DocumentLevel, int], factory: Callable[[], Executor]
    ) -> Executor:
        """Пул ключа, создаваемый ``factory`` при первом обращении (лениво)."""
        slot = self.slot(key)
        if slot.executor is None:
            slot.executor = factory()
        return slot.executor

    def note_completion(self, key: tuple[DocumentLevel, int]) -> None:
        """Задание завершилось (любым исходом): пул жив, серия таймаутов сброшена."""
        self.slot(key).stalled_timeouts = 0

    def note_timeout(self, key: tuple[DocumentLevel, int]) -> bool:
        """Учесть таймаут артефакта; ``True`` — пул мёртв (``2 × timeout`` без завершений)."""
        slot = self.slot(key)
        slot.stalled_timeouts += 1
        return slot.stalled_timeouts >= _EXTRACT_DEAD_POOL_TIMEOUTS

    def terminate(self, key: tuple[DocumentLevel, int]) -> None:
        """Reaping зависших воркеров (``terminate_workers``, Python 3.14; best-effort)."""
        executor = self.slot(key).executor
        terminate = getattr(executor, "terminate_workers", None)
        if terminate is None:
            return
        try:
            terminate()
        except Exception:  # noqa: BLE001 — reaping best-effort
            pass

    def discard(self, key: tuple[DocumentLevel, int]) -> None:
        """Закрывает пул ключа синхронно и отменяет запланированное (P0/OOM)."""
        slot = self.slot(key)
        executor = slot.executor
        slot.executor = None
        if executor is None:
            return
        try:
            # P0/OOM: дождаться реального завершения воркеров и отменить запланированное.
            # ``wait=False`` оставлял бы старые процессы живыми поверх следующего корня
            # (двойной рабочий набор → OOM).
            executor.shutdown(wait=True, cancel_futures=True)
        except TypeError:  # фейки/старые исполнители без cancel_futures
            try:
                executor.shutdown(wait=True)
            except Exception:  # noqa: BLE001 — reaping best-effort
                pass
        except Exception:  # noqa: BLE001 — reaping best-effort
            pass

    def rebuild(self, key: tuple[DocumentLevel, int]) -> bool:
        """Discard + bounded rebuild; ``False`` — пересборки исчерпаны (ключ abandon'ен)."""
        self.discard(key)
        slot = self.slot(key)
        slot.stalled_timeouts = 0
        if slot.rebuilds >= self._max_rebuilds:
            slot.abandoned = True
            return False
        slot.rebuilds += 1
        return True

    def abandoned(self, key: tuple[DocumentLevel, int]) -> bool:
        """Исчерпал ли ключ пересборки (остаток прогона — терминальный отказ)."""
        return self.slot(key).abandoned

    def shutdown(self) -> None:
        """Единственная точка ``shutdown(wait=True, cancel_futures=True)`` фазы."""
        for key in list(self._slots):
            self.discard(key)


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
    :param doc_counters: честные счётчики документов по продукту (09 §6.3):
        ``{"total": n, "new": n, "reused": n}``. ``new`` — документ прошёл
        экстракцию+нарезку в этом прогоне; ``reused`` — версия переиспользована из
        durable-субстрата (304/тот же ``content_hash``). Пустая карта — legacy-путь
        (``state is None``): счётчики не выдумываются.
    :param duplicates: **наблюдаемость** дедупа по ``content_hash`` внутри продукта:
        ``{"product_id": n}`` — сколько документов схлопнулось (алиасы/перепечатки).
        Это не ошибка и не смена статуса: дедуп легитимен (I17 — партиция продукта),
        но должен быть виден, иначе цепочка «25 версий-алиасов → 1 документ» теряется
        молча. Пустая карта — схлопываний не было.
    :param duplicate_count: сумма ``duplicates`` (аддитивный счётчик для операторских
        сводок). **Не входит** в ``index_build_id``: это наблюдение, а не состав
        композиции (пин — ``tests/pipeline/test_corpus.py``).
    :param partitions_reused: сколько продукт-партиций переиспользовано с диска без
        вызова ``embedder.embed`` (09 §8.5). Ноль — ни одна партиция не переиспользована
        (первый прогон/нет ``index_root``); значение честное, не оценка.
    :param revalidation_orphans: наблюдение (не ``errors``, не смена статуса I12):
        известные URL инкрементального продукта, не попадающие ни в один авторский корень.
        Ключ ``product_id:origin`` → ``{"selected": N, "fetched": M}``; один synthetic-бакет
        на (продукт, origin). При операторском потолке ``revalidate_max_documents``
        добавляются ``"deferred": K`` (отложенные) и ``"skipped": S`` (неисполнимые);
        leftover-группа каталога пишется под ``product_id:catalog:{url}`` с
        ``"via": "catalog"``. Бакеты **не** пишутся в ``truncation`` — иначе синтетическая
        причина демотировала бы ``❌`` без вины авторского корня.
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
    #: добытых страниц, причина (``max_documents``/``max_depth``/``round_budget``, а для
    #: операторского потолка ревалидации — ``revalidate_max_documents``), зачёркнутые
    #: блэклистом URL и аддитивный ``revalidation``-счётчик.
    truncation: list[dict[str, Any]] = field(default_factory=list)
    #: P5: видимые пропуски документов уровнем ``off`` (решение **никогда** не тихо):
    #: ``{product_id, url, reason: url_suffix|content_type, level}``. Питает канал
    #: усечения/аномалии (``source_scope: exhaustive`` поверх «прореженного» продукта).
    document_skips: list[dict[str, Any]] = field(default_factory=list)
    #: 09 §6.3: известные URL вне scope всех авторских корней (author-scope drift):
    #: ``"product_id:origin"`` → ``{"selected": N, "fetched": M}`` (+ ``deferred``/
    #: ``skipped`` при потолке). Каталоговый leftover — ``product_id:catalog:{url}`` с
    #: ``"via": "catalog"``. Наблюдение, не усечение и не ошибка.
    revalidation_orphans: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: 09 §6.3: счётчики документов по продукту (``total``/``new``/``reused``). Пусто —
    #: durable-субстрата нет (legacy), счётчики не фабрикуются.
    doc_counters: dict[str, dict[str, int]] = field(default_factory=dict)
    #: Наблюдаемость дедупа по ``content_hash`` **внутри продукта**: ``{"product_id": n}``
    #: — сколько документов схлопнулось. Не ошибка и не статус: дедуп легитимен (I17),
    #: но должен быть виден (иначе алиасы версий исчезают молча).
    duplicates: dict[str, int] = field(default_factory=dict)
    #: Сумма ``duplicates``. Аддитивное поле наблюдаемости; в ``index_build_id`` **не**
    #: входит (композиция зависит только от состава когорты и дайджестов партиций).
    duplicate_count: int = 0
    #: 09 §8.5: число продукт-партиций, переиспользованных без эмбеддинга.
    partitions_reused: int = 0


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
    resume: _RetainedRaw | None
    browser_blocked: bool
    poolable: bool
    host: str
    scope_host: str | None
    enforce_redirect_scope: bool
    terminal: bool = False
    #: P0: retained raw отклонён по размеру/бинарности — терминальный исход без экстракции.
    reject: _RetainedReject | None = None
    #: P6: класс-aware потолок артефакта-контейнера и заявленный набор форматов (нужны
    #: пост-фазе для допуска/размера; ``None``/пусто — наследовать поведение страницы).
    max_artifact_bytes: int | None = None
    attachment_formats: tuple[str, ...] | None = None


@dataclass
class _WaveEntry:
    """D4: элемент волны дренажа frontier.

    Порядок элементов — порядок отправки (FIFO). ``kind``:
    ``"blacklisted"`` | ``"attempts"`` | ``"terminal_not_found"`` (мутации коммитятся
    пост-фазой; терминальный ``not_found`` не подаётся в работу) | ``"fetch"``
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
class _RootDrain:
    """C1: хвост экстракции одного seed-root в **фазовой** очереди доводки.

    Пул и FIFO-очередь экстракции живут в области фазы (:func:`build_corpus`), поэтому
    задание может быть доведено уже после возврата своего корня. ``pending`` — число
    недоведённых заданий корня; ``finalize`` — замыкание вызывающего, которое считает
    провенанс корня (``revalidation``/``truncation``) **после** доводки хвоста; оно
    вызывается ровно один раз (:func:`_finalize_ready_roots`), в порядке корней.
    """

    pending: int = 0
    finalize: Callable[[], None] | None = None
    finalized: bool = False


@dataclass
class _PendingExtract:
    """P4: отложенная экстракция (главный поток, FIFO-очередь доводки).

    Несёт всё, что нужно финишу: реконструированный ``RawPage``, ``raw_path`` (перечитывается
    воркером, I19), ``fetch_plan`` (ось ``extract``), флаг свежести (для ``note_fetched`` и
    учёта ёмкости с in-flight) и ``future`` пула. Мутаций состояния сеанса/корпуса здесь нет.

    C1: очередь — **фазовая**, поэтому задание несёт ``root`` (свой корень: фильтр ёмкости,
    учёт хвоста) и ``commit`` — замыкание ``_commit_outcome`` именно своего корня. Это
    позволяет доводить хвост корня A во время фетча корня B без потери провенанса.
    """

    pid: str
    url: str
    depth: int | None
    page: RawPage
    raw_path: Path
    fetch_plan: FetchPlan
    freshly_fetched: bool
    future: Any
    #: C1: корень задания (хвост/финализация провенанса).
    root: _RootDrain
    #: C1: доводка исхода — ``(extracted, extract_error, failure_kind) → None``.
    commit: Callable[[Any, str | None, FailureKind | None], None]
    #: C1: ключ фазового пула ``(level, window)`` — учёт завершений/таймаутов и пересборка.
    pool_key: tuple[DocumentLevel, int]


@dataclass(frozen=True)
class _SitemapSeeds:
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
    отклонённые loc до этого не доходят — у них своя построчная запись в ``_expand_sitemap``).
    """
    root_host = (urlsplit(root_url).hostname or "").lower()
    host = (urlsplit(loc).hostname or "").lower()
    if not host or not _host_matches(host, root_host):
        return _SITEMAP_SKIP_HOST
    return _SITEMAP_SKIP_PATH


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

    Приоритет: явный ``package_id`` (его резолвит composition root/``phases.resolve_package_id``:
    основная идентичность — ``run.yaml:package``, а не ``run_id``) → ``plan.package`` →
    ``plan.case_id`` → ``plan.cohort.case_id`` → ``plan.run_id``.

    ``run_id`` — только последний резерв (прямой вызов ``build_corpus`` без пакета): он
    делает партиции run-scoped и исключает переиспользование между прогонами (09 §8.2),
    поэтому не выбирается, пока известно имя пакета.
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


def _derive_partition_build_id(
    *,
    package_id: str,
    product_id: str,
    chunker_version: str,
    embed_model_id: str,
    extractor_version: str,
    lexical_engine_version: str,
    vector_store: str,
    product_content_token: str,
) -> str:
    """Детерминированный ``partition_build_id`` одной продукт-партиции (09 §8.1).

    Входы (склеены ``\\x1f``, порядок фиксирован):

    0. ``_PARTITION_SCHEMA`` — версия формулы;
    1. ``package_id``;
    2. ``product_id`` — партиция продукт-скоупед (I17);
    3. ``chunker_version``;
    4. ``embed_model_id`` (пусто, если эмбеддер не подключён);
    5. ``extractor_version`` — композит версий экстрактора **этого** продукта;
    6. ``lexical_engine_version``;
    7. ``vector_store``;
    8. ``product_content_token`` — отпечаток содержимого партиции.

    **Чего здесь нет и почему.** ``params_token`` (каталог/фокус) и ``snapshot_date``
    намеренно исключены: каталог/фокус не меняют чанки/векторы, а снимок — провенанс,
    не содержимое. Это позволяет переиспользовать партицию при смене фокуса/каталога
    (09 §8.1). ``retrieval_*`` тоже не входят: ретривал-конфиг не меняет чанкинг/векторы.

    Ревизия формулы (``_PARTITION_SCHEMA``) всегда даёт новые id: старые партиции
    остаются на диске иммутабельно, но новой композицией не переиспользуются.
    """
    material = _FIELD_SEP.join(
        (
            _PARTITION_SCHEMA,
            package_id,
            product_id,
            chunker_version,
            embed_model_id,
            extractor_version,
            lexical_engine_version,
            vector_store,
            product_content_token,
        )
    )
    return "pb_" + short_hash(material, 24)


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


def _product_extractor_token(docs: Sequence[CorpusDoc]) -> str:
    """Композитная ``extractor_version`` **одного** продукта (09 §8.1).

    Собираются непустые ``CorpusDoc.extractor_version`` документов продукта,
    дедуплицируются и сортируются (порядко-независимо), склеиваются через запятую.
    При единогласии версий даёт одну строку; при расхождении (разные extract-маршруты) —
    детерминированный композит. Документы без версии игнорируются; если версий нет —
    пустая строка (вызывающий подставляет объявленную версию порта).
    """
    versions = sorted(
        {doc.extractor_version for doc in docs if doc.extractor_version}
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

    **Единственная** точка записи ``corpus_docs``. В Pass A вызывается инкрементально —
    сразу после ``ingest.docs.append`` для каждого полностью сформированного документа
    (см. ``_finish_one``), поэтому при жёстком убийстве теряется максимум документ
    «в полёте». Retention-провенанс не зависит от иммутабельности индексной сборки (I19),
    поэтому запись не откладывается до Pass B.

    Каждая запись обёрнута в try/except: сбой одного ``upsert_corpus_doc`` фиксируется в
    ``errors`` и не прерывает ни обработку остальных документов, ни краул (контракт
    изоляции фаз, ``runner.py``).
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
) -> bool:
    """Пишет ``corpus_chunks`` → ``index_manifest`` (порядок: FK ``doc_id``).

    Вызывается **только** когда физическая продукт-партиция реально зафиксирована на диске
    («disk first, DB second»): реестр не должен заявлять сборку, которой нет (K2/§13).
    Каждая запись обёрнута в try/except: сбой фиксируется в ``errors`` и не прерывает
    обработку остальных продуктов (контракт изоляции фаз, ``runner.py``).

    :returns: ``True`` — строки записаны (или store отсутствует); ``False`` — хотя бы одна
        запись не удалась. Композиция пишется только при ``True`` для **всех** членов:
        иначе она ссылалась бы на партицию, которой нет в реестре прогона.
    """
    if store is None:
        return True
    ok = True
    if chunks:
        try:
            store.add_chunks_batch(chunks)
        except Exception as exc:  # noqa: BLE001
            ok = False
            errors.append(f"add_chunks_batch: {type(exc).__name__}: {exc}")
    if entry is not None:
        try:
            store.save_index_manifest(entry)
        except Exception as exc:  # noqa: BLE001
            ok = False
            errors.append(f"save_index_manifest({entry.product_id}): {type(exc).__name__}: {exc}")
    return ok


def _register_composition_state(
    state: ProductStatePort,
    *,
    package_id: str,
    composition_id: str,
    entries: Sequence[IndexManifestEntry],
    created_at: str,
    run_id: str | None,
    errors: list[str],
) -> None:
    """Регистрирует партиции и композицию в durable-состоянии (09 §8.1–§8.2).

    Вызывается **после** того, как манифест композиции физически записан на диск и все
    строки ``index_manifest``/``corpus_chunks`` персистентны («сначала диск, потом БД»).
    Каждая запись изолирована в ``errors`` — сбой реестра не отменяет уже зафиксированную
    сборку и не роняет фазу (контракт изоляции state-записей).
    """
    for entry in entries:
        if not entry.partition_build_id:
            continue
        try:
            state.register_index_partition(
                IndexPartitionRow(
                    package_id=package_id,
                    product_id=entry.product_id,
                    partition_build_id=entry.partition_build_id,
                    content_digest=entry.content_digest or "",
                    chunker_version=entry.chunker_version or "",
                    embed_model_id=entry.embed_model_id or "",
                    extractor_version=entry.extractor_version or "",
                    lexical_engine_version=entry.lexical_engine_version or "",
                    vector_store=entry.vector_store or "",
                    store_path=entry.store_path or "",
                    chunks=_entry_chunks(entry),
                    dim=entry.embed_dim,
                    created_at=created_at,
                    last_used_run_id=run_id,
                )
            )
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            errors.append(
                f"register_index_partition({entry.product_id}, "
                f"{entry.partition_build_id}): {type(exc).__name__}: {exc}"
            )
    try:
        state.save_index_composition(
            IndexCompositionRow(
                composition_id=composition_id,
                package_id=package_id,
                members=tuple(
                    (
                        entry.product_id,
                        entry.partition_build_id or "",
                        entry.content_digest or "",
                    )
                    for entry in entries
                ),
                created_at=created_at,
            )
        )
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        errors.append(
            f"save_index_composition({composition_id}): {type(exc).__name__}: {exc}"
        )


def _entry_chunks(entry: IndexManifestEntry) -> int:
    """Число чанков партиции из ``stats_json`` (0 при отсутствии/порче)."""
    try:
        payload = json.loads(entry.stats_json or "{}")
    except (TypeError, json.JSONDecodeError):
        return 0
    value = payload.get("chunks") if isinstance(payload, dict) else None
    return int(value) if isinstance(value, int) else 0


def _embed_and_build(
    embedder: EmbedderPort,
    index_builder: IndexBuilderPort,
    product_id: str,
    chunks: list[CorpusChunk],
    *,
    package_id: str,
    chunker_version: str,
    embed_model_id: str,
    extractor_version: str,
    lexical_engine_version: str,
    vector_store: str,
    product_content_token: str,
    snapshot_date: str | None,
    partition_build_id: str,
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
            extra={
                # 09 §8.2: физическая сборка адресуется **партицией**, не композицией.
                "partition_build_id": partition_build_id,
                # Контентный отпечаток партиции: сверяется вторым, физическим контролем
                # переиспользования (09 §8.5); id уже хеширует его, но диск проверяется
                # отдельно.
                "content_digest": product_content_token,
                # Версии производства/формата — для второго, физического контроля
                # переиспользования (09 §8.5): они уже свернуты в ``pb``, но манифест
                # на диске проверяется отдельно от id.
                "extractor_version": extractor_version,
                "lexical_engine_version": lexical_engine_version,
                "vector_store": vector_store,
            },
        )
        index_builder.build(port_chunks, vectors, meta)
        return True
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        errors.append(f"{product_id}: index build: {type(exc).__name__}: {exc}")
        return False


def _partition_reuse_reason(
    *,
    index_root: str | Path | None,
    package_id: str,
    partition_build_id: str,
    product_id: str,
    product_content_token: str,
    chunker_version: str,
    embed_model_id: str,
    extractor_version: str,
    lexical_engine_version: str,
    vector_store: str,
) -> str | None:
    """Причина, по которой партицию **нельзя** переиспользовать (``None`` — можно).

    Гейт переиспользования (09 §8.5) — два независимых уровня:

    1. **id-уровень.** ``partition_build_id`` уже хеширует все версии производства и
       ``product_content_token``; совпал id — совпали входы by construction.
    2. **физический уровень (эта проверка).** Партиция реально зафиксирована на диске
       (``is_committed``), а её ``manifest.json`` несёт тот же ``content_digest`` и те же
       ``chunker_version``/``embed_model_id``/``extractor_version``/
       ``lexical_engine_version``/``vector_store``. Это защита от коллизии id, порчи и
       подмены артефакта: id — не доказательство целостности диска.

    Возвращает человекочитаемую причину отказа (для ``errors``) либо ``None``.
    Отсутствие каталога при первом прогоне — **не** ошибка (возвращается ``None`` только
    при пригодной партиции; вызывающий отличает «нет каталога» от «каталог неполон»).
    """
    if index_root is None:
        return "index_root не задан"
    if not _partition_is_committed(
        index_root, package_id, partition_build_id, product_id
    ):
        return "партиция не зафиксирована на диске"
    manifest = _read_partition_manifest(
        index_root, package_id, partition_build_id, product_id
    )
    expected = {
        "content_digest": product_content_token,
        "chunker_version": chunker_version,
        "embed_model_id": embed_model_id,
        "extractor_version": extractor_version,
        "lexical_engine_version": lexical_engine_version,
        "vector_store": vector_store,
    }
    for name, value in expected.items():
        actual = manifest.get(name)
        if str(actual if actual is not None else "") != str(value):
            return (
                f"манифест партиции расходится по {name}: "
                f"{actual!r} != {value!r}"
            )
    return None


# --------------------------------------------------------------------------- #
# Durable-краул (L1a): сеанс краула — аудит/очередь/resume per-URL
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _RetainedRaw:
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


def _hash_file(path: Path) -> str:
    """SHA-256 файла потоково (1 MiB): файл не читается целиком ради хеша."""
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_RESUME_READ_CHUNK), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


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


def _is_attachment(
    url: str | None,
    content_type: str | None,
    attachment_formats: Sequence[str] | None,
) -> bool:
    """Класс «артефакт-контейнер» по URL-суффиксу либо ``Content-Type`` (P6).

    ``attachment_formats=None`` — дефолт всех поддерживаемых форматов (так резолвится
    конфиг); пустой набор — контейнеров не заявлено, класс не определяется.
    """
    if is_attachment_url(url or "", attachment_formats):
        return True
    return is_attachment_content_type(content_type, attachment_formats)


def _resume_source(
    item: CrawlSessionItem | None,
    *,
    max_bytes: int | None = None,
    artifact_max_bytes: int | None = None,
    attachment_formats: Sequence[str] | None = None,
) -> _RetainedRaw | _RetainedReject | None:
    """Возвращает retained-источник для resume, если элемент сеанса позволяет НЕ фетчить (D3).

    Пригодны элементы ``FETCHED`` (сырые байты уже сохранены) и ``FAILED`` с сохранённым
    ``raw_path`` (re-extract из тех же байтов без сети). ``PENDING``/``RUNNING``/отсутствие
    записи, отсутствие ``raw_path`` или файла на диске → ``None`` (обычный путь фетча).

    **P0/P6: потолок по классу.** Размер проверяется по ``stat`` **до** чтения. Класс
    определяется по метаданным записи (URL-суффикс/``Content-Type``, заявленный набор
    ``attachment_formats``): допущенный артефакт-контейнер ограничен
    ``artifact_max_bytes``, страница — ``max_bytes``. Так retained-артефакт в 125 MB
    резюмится, а страница того же размера по-прежнему терминально отвергается
    ``payload_too_large``.

    **Память.** Страницы (≤ ``max_bytes``) буферизуются как прежде — путь байт-в-байт
    прежний. Артефакт **не** буферизуется: возвращается :class:`_RetainedRaw` с путём,
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
    artifact = _is_attachment(item.url, item.content_type, attachment_formats)
    cap = artifact_max_bytes if artifact and artifact_max_bytes is not None else max_bytes
    try:
        size = path.stat().st_size
    except OSError:
        return None
    if cap is not None and size > cap:
        return _RetainedReject(
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
                    return _RetainedReject(
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
        return _RetainedReject(
            FailureKind.UNSUPPORTED_BINARY,
            f"retained: {binary_reason}",
            path,
        )
    return _RetainedRaw(
        path=path,
        content_hash=digest,
        size=read,
        head=None if buffered is not None else snapshot,
        data=buffered,
    )


#: Терминальные классы: повторный фетч/экстракция не дадут нового знания (P0/I4).
_TERMINAL_FAILURE_KINDS: frozenset[FailureKind] = frozenset(
    {
        FailureKind.NOT_FOUND,
        FailureKind.PAYLOAD_TOO_LARGE,
        FailureKind.UNSUPPORTED_BINARY,
    }
)


def _terminal_failure_kind(item: CrawlSessionItem | None) -> FailureKind | None:
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
    return _terminal_failure_kind(item) is FailureKind.NOT_FOUND


def _resume_page(
    item: CrawlSessionItem,
    url: str,
    retained: _RetainedRaw,
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
                retained.path.stat().st_mtime, tz=_dt.timezone.utc
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


# --------------------------------------------------------------------------- #
# Основная фаза
# --------------------------------------------------------------------------- #


def _build_corpus(
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
    index_root: str | Path | None = None,
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
    extract_cache: ExtractionCache | None = None,
    extract_task_timeout_s: float | None = None,
    max_attempts: int | None = None,
    progress: Callable[[dict[str, int]], None] | None = None,
    crawl_app: CrawlConfig | None = None,
    state: ProductStatePort | None = None,
    product_modes: Mapping[str, str] | None = None,
    refresh_policy: str = "auto",
    pools: _ExtractPools,
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
    :param index_root: корень ``index_store/`` для (а) проверки/переиспользования уже
        зафиксированной продукт-партиции (09 §8.5) и (б) записи иммутабельного манифеста
        композиции ``_compositions/{composition_id}.json`` (09 §8.2). ``None`` — легаси-путь
        без чтения/записи индекса: переиспользования нет, композиция не пишется (поведение
        байт-идентично до-MVP-4). Composition root передаёт resolved ``paths.index_store``
        (``03`` §A.1), совпадающий с корнем ``IndexBuilderPort``.
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
        (поведение байт-в-байт). C1/L1: пул — **фазовый** (один на прогон, ключ
        ``(DocumentLevel, window)``), лениво создаётся при первом задании и переиспользуется
        всеми корнями/продуктами; ``shutdown(wait=True, cancel_futures=True)`` — один раз в
        ``finally`` фазы (:func:`build_corpus`), не на корень. Окно ``W`` (не более ``W``
        экстракций «в полёте»), доводка строго FIFO. ``None`` при заданной оси ``>1`` —
        явная запись в ``errors`` + inline-fallback (не тихо). Воркер получает только
        :class:`_ExtractRequest` (примитивы + ``raw_path``); байты не пиклятся (I19).
        OOM-ось ``crawl.extract_max_tasks_per_child=N`` (только процессный пул) включает пул
        и при ``extract_workers == 1`` и пересоздаёт воркер после N заданий
        (``max_tasks_per_child``); ``None`` — прежнее поведение.
    :param extractor_factory: P5 — фабрика per-level экстрактора ``Callable[[DocumentLevel],
        ExtractorPort]``, переданная ссылкой из composition root (слой-матрица: pipeline не
        импортирует адаптеры). Применяется к **inline**-пути (``extract_workers`` None/``1``):
        для уровня корня, отличного от ``full``, строится экстрактор этого уровня; ``full``
        переиспользует переданный ``extractor`` (дефолт — без лишней загрузки весов docling).
        ``None`` → уровень на inline-пути не действует (поведение как прежде; пул-путь всё
        равно получает уровень через ``extraction_pool.pool_factory``).
    :param extract_cache: Ф1.2 — межпрогонный кэш экстракции (:class:`ExtractionCache`).
        ``None`` → кэш выключен, поведение байт-в-байт прежнее. Ключ — хеш **ретенированных**
        байтов (I19), поэтому хит не может подсунуть неверифицированный контент: хеш
        пересчитывается из тех же байтов, которые прочитал бы экстрактор. C1/L4: на пулевом
        пути проба выполняется **в родителе** до submit (хит не занимает слот воркера и не
        оплачивает инициализатор); проба в воркере остаётся защитой, ключ у них один.
    :param extract_task_timeout_s: P4/C1 — потолок ожидания ``future.result`` **одного**
        задания экстракции (guard от зависшего артефакта, не SLA). ``None`` →
        :data:`_EXTRACT_TASK_TIMEOUT_S`. По таймауту терминально помечается только это
        задание (``extraction_timeout``), пул продолжает работу; пересборка — только если
        пул не дал ни одного завершения за ``2 × timeout`` (bounded, затем терминальный
        отказ); авто-ретрая нет.
    :param max_attempts: Phase 4 — потолок попыток по ``(product_id, url)``. URL с уже
        сохранённым ``CrawlSessionItem.attempts >= max_attempts`` **не** фетчится и **не**
        подаётся исполнителю: он помечается терминальным ``FAILED`` с ``error_kind =
        "attempts_exhausted"`` и понятной ошибкой (не тихий дроп). ``None`` → без потолка.
        Терминальный ``not_found`` (404/410) этому потолку не подлежит: определённый
        ответ не повторяется и не переклассифицируется в ``attempts_exhausted``.
    :param progress: Phase 4 — опциональный колбэк прогресса: вызывается со снимком
        ``crawl_session_counts`` (``{status: count}``) по ходу фазы (для ``--follow``).
        Ошибки колбэка изолируются и не роняют фазу. ``None`` → без прогресс-вывода.
    :param crawl_app: D1/D2 — application-слой осей контроля краула (``deploy.yaml:crawl``,
        :class:`~core.config.deploy.CrawlConfig`): низший приоритет цепочки
        ``app < run < site`` для ``max_workers``/``max_depth``/``max_documents``/
        ``exclude_paths``/``exclude_documents`` (:func:`core.pipeline.crawl_frontier.resolve_crawl_limits`).
        ``None`` → дефолт :class:`CrawlConfig` (``max_depth=0``, без лимитов/блэклистов —
        поведение неизменно).
    :param state: durable per-product состояние (``ProductStatePort``, 09 §4.1/§4.2).
        ``None`` → прежнее поведение **байт-в-байт**: субстрат не читается и не пишется,
        ``IndexBuild.doc_counters`` пуст, ``index_build_id`` тот же. При наличии — состояние
        URL/документов/чанков читается и пишется для **каждого** продукта независимо от
        режима (субстрат наполняется на первом прогоне), а политика наблюдения добавляет
        валидаторы условного GET и переиспользование документов.
    :param product_modes: ``product_id → "new"|"full"|"incremental"`` (значения
        :class:`~core.domain.enums.ProductMode`). Отсутствующий продукт трактуется как
        ``new``. ``incremental`` расширяет work-set известными URL
        (:func:`core.pipeline.incremental.plan_incremental`), ``new``/``full`` сохраняют
        прежнюю развёртку BFS/sitemap. Игнорируется при ``state is None``.
    :param refresh_policy: политика наблюдения (значение :class:`RefreshPolicy`):
        ``auto``/``revalidate`` — условный GET с валидаторами; ``refresh`` — безусловный
        GET (валидаторы не отправляются); ``offline`` — известные URL без сети, но
        переиспользование ``reuse`` в этом срезе **не реализовано** (видимая ошибка на
        продукт, элемент обрабатывается как ``revalidate``).

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
    # Объём корпуса = продукты **с эффективными seed'ами**, а не весь план: при
    # ``--only-product``/``--only-failed`` ``urls`` уже сужен фильтром, и запись
    # zero-source покрытия для продуктов вне области прогона была бы ложной (09 §6.2).
    # Пустой ``urls`` (не задан) → весь план, как прежде.
    products = sorted(url_map) if url_map else declared

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
    #: Наблюдаемость дедупа по ``content_hash`` внутри продукта: ``{"product_id": n}``.
    #: Считается по продукту (``seen_docs`` — per-product), поэтому и карта продуктовая;
    #: суммарный ``duplicate_count`` — сумма. Дедуп легитимен, но не должен быть невидим.
    duplicates: dict[str, int] = {}
    raw_root_path = Path(raw_root)
    #: P5: видимые пропуски документов уровнем ``off`` (никогда не тихо) — агрегат по всем
    #: корням/продуктам прогона; кладётся в ``IndexBuild.document_skips``.
    document_skips: list[dict[str, Any]] = []
    #: 09 §6.3: известные URL вне scope авторских корней — наблюдение по (продукт, origin).
    revalidation_orphans: dict[str, dict[str, Any]] = {}
    revalidation_orphan_total = 0
    #: 09 §6.3: честные счётчики документов по продукту (``total``/``new``/``reused``).
    #: Заполняются только при наличии durable-субстрата (``state``), иначе карта пуста.
    doc_counters: dict[str, dict[str, int]] = {}

    # 09 §6.3/§9 (MVP-3b): политика наблюдения и режимы продуктов. ``state is None`` —
    # legacy-путь: все три аргумента игнорируются целиком (поведение байт-в-байт).
    policy = _as_refresh_policy(refresh_policy)

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

    # ------------------------------------------------------------------ #
    # 09 §6.3/§9: durable-состояние URL/документов/чанков (MVP-3b)
    # ------------------------------------------------------------------ #
    #: Валидаторы условного GET из инкрементального work-set:
    #: ``(product_id, normalized_url) → (If-None-Match, If-Modified-Since)``.
    planned_validators: dict[tuple[str, str], tuple[str | None, str | None]] = {}

    def _validators(pid: str, url: str) -> tuple[str | None, str | None]:
        """Валидаторы условного GET для URL (09 §6.3): ``(etag, last_modified)``.

        Источник — ``url_state`` (durable), а не сеанс краула: валидаторы переживают
        прогоны. Отправляются только при политике ``auto``/``revalidate``: ``refresh``
        обязан получить свежий ответ (безусловный GET), ``offline`` сеть не трогает.
        Плановые значения (из ``plan_incremental``) приоритетнее повторного чтения строки —
        это тот же источник, прочитанный один раз на продукт.

        Сбой чтения состояния изолируется в ``errors``: отсутствие валидатора деградирует
        запрос до безусловного GET (безопасно), а не роняет фазу.
        """
        if state is None or policy in (RefreshPolicy.REFRESH, RefreshPolicy.OFFLINE):
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

    def _count_doc(pid: str, *, reused: bool) -> None:
        """Считает принятый документ прогона (``total``/``new``/``reused``, 09 §6.3).

        Считается **документ**, а не URL: повтор/алиас (дедуп ``seen_docs``) не создаёт
        записи в корпусе и сюда не попадает.
        """
        if state is None:
            return
        bucket = doc_counters.setdefault(pid, {"total": 0, "new": 0, "reused": 0})
        bucket["total"] += 1
        bucket["reused" if reused else "new"] += 1

    def _find_doc_version(
        pid: str, doc_hash: str, extractor_token: str, level_text: str
    ) -> DocumentVersionRow | None:
        """Версия документа по ключу §4.2 (``content_hash``+``extractor_version``+``level``)."""
        if state is None:
            return None
        try:
            return state.find_document_version(
                pid,
                content_hash=doc_hash,
                extractor_version=extractor_token,
                level=level_text,
            )
        except Exception as exc:  # noqa: BLE001 — изоляция state-чтения
            errors.append(
                f"state: find_document_version({pid}/{doc_hash}): "
                f"{type(exc).__name__}: {exc}"
            )
            return None

    def _reuse_plan(
        pid: str, url: str, doc_version: DocumentVersionRow | None
    ) -> _DocReuse | None:
        """Проверенный chunk-set версии документа либо ``None`` (не переиспользуем)."""
        if state is None or doc_version is None:
            return None
        if doc_version.status != _DOC_ACTIVE:
            return None
        chunks = _load_reused_chunks(
            state,
            doc_version,
            chunker_version=resolved_chunker_version,
            where=f"{pid}/{url}",
            errors=errors,
        )
        if chunks is None:
            return None
        return _DocReuse(doc_version=doc_version, chunks=chunks)

    def _reuse_not_modified(pid: str, url: str) -> _DocReuse | None:
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
        return _reuse_plan(pid, url, prior_doc)

    def _reuse_for_page(pid: str, url: str, page: RawPage) -> _DocReuse | None:
        """Переиспользование по ``304`` с видимой записью, если версии нет.

        ``304`` без известной версии (или с отвергнутым chunk-set) — не повод молчать:
        сервер подтвердил «не изменилось», а переиспользовать нечего, поэтому прогон
        идёт обычной экстракцией, а факт фиксируется в ``errors``. Конкретная причина
        отказа уже записана :func:`_reuse_not_modified` — дублирующая запись не нужна.
        """
        if not page.not_modified:
            return None
        before = len(errors)
        reuse = _reuse_not_modified(pid, url)
        if reuse is None and len(errors) == before:
            errors.append(
                f"{pid}/{url}: 304 без переиспользуемой версии документа — "
                "обычная экстракция"
            )
        return reuse

    def _reuse_same_hash(
        pid: str, url: str, doc_hash: str, extractor_token: str, level_text: str
    ) -> _DocReuse | None:
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
        prior_doc = _find_doc_version(pid, doc_hash, extractor_token, level_text)
        if classify_reuse(
            prior=prior, prior_doc=prior_doc, not_modified=False, content_hash=doc_hash
        ) is not ReuseKind.REVALIDATED:
            return None
        return _reuse_plan(pid, url, prior_doc)

    def _supersede_document(pid: str, url: str, dv_id: str | None) -> None:
        """Помечает прежнюю версию ``superseded`` (09 §6.3: ``404``/``410`` по URL)."""
        if state is None or not dv_id:
            return
        try:
            row = state.get_document_version(dv_id)
            if row is None or row.status != _DOC_ACTIVE:
                return
            state.upsert_document_version(replace(row, status=_DOC_SUPERSEDED))
        except Exception as exc:  # noqa: BLE001 — изоляция state-записи
            errors.append(
                f"{pid}/{url}: state: supersede({dv_id}): "
                f"{type(exc).__name__}: {exc}"
            )

    def _record_url_state(
        pid: str,
        url: str,
        status: CrawlItemStatus,
        fields: Mapping[str, Any],
        extra: Mapping[str, Any] | None,
    ) -> None:
        """Пишет ``url_state`` по исходу одного URL (09 §6.3).

        Вызывается из :func:`_mark` — единственной воронки терминальных исходов, поэтому
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
                error_kind = _canonical_failure_kind(fields.get("error_kind"))
                if error_kind == FailureKind.NOT_FOUND.value:
                    _supersede_document(pid, url, base.doc_version_id)
                    current["doc_version_id"] = None
                else:
                    failures += 1
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
                )
            )
        except Exception as exc:  # noqa: BLE001 — изоляция state-записи
            errors.append(
                f"state: url_state({pid}/{url}): {type(exc).__name__}: {exc}"
            )

    def _write_document_state(
        pid: str,
        url: str,
        page: RawPage,
        *,
        dv_id: str,
        doc_hash: str,
        extractor_token: str,
        level_text: str,
        raw_path: Path,
        chunks: Sequence[tuple[int, str, str, str]],
    ) -> None:
        """Пишет версию документа и её chunk-set в durable-субстрат (09 §4.2/§9).

        Порядок — по FK: ``document_version`` → ``chunk_store`` → ``chunk_meta``.
        ``chunk_meta`` адресуется durable-ключом :func:`chunk_row_id` (не run-level
        ``chunk_id``) и различает версию чанкера. ``chunk_store``-ключ — hash **фактически
        записываемого текста** (I19): испорченная строка под старым хешем не «заражает»
        новую запись.

        ``first_seen_*`` существующей версии не затирается: иначе повторное наблюдение
        того же документа выглядело бы как новый (``docs_new``/``docs_reused`` в
        :func:`core.pipeline.state_sync.record_run_outcome`).

        :param chunks: ``(chunk_no, text, heading_path, context_prefix)`` принятых чанков.
        """
        if state is None:
            return
        try:
            now = _now_iso()
            existing = state.get_document_version(dv_id)
            run_token = str(run_id or "")
            rows: list[tuple[ChunkRow, str]] = []
            for chunk_no, text, heading_path, context_prefix in chunks:
                row = ChunkRow(
                    chunk_id=chunk_row_id(dv_id, resolved_chunker_version, chunk_no),
                    doc_version_id=dv_id,
                    product_id=pid,
                    chunk_no=chunk_no,
                    content_hash=content_hash(text),
                    chunker_version=resolved_chunker_version,
                    heading_path=heading_path or None,
                    context_prefix=context_prefix or None,
                    lang=page.lang,
                )
                rows.append((row, text))
            digest = short_hash(
                _FIELD_SEP.join(sorted(row.chunk_id for row, _text in rows)), 24
            )
            state.upsert_document_version(
                DocumentVersionRow(
                    doc_version_id=dv_id,
                    doc_id=f"{pid}:{doc_hash}",
                    product_id=pid,
                    url=page.url,
                    content_hash=doc_hash,
                    extractor_version=extractor_token,
                    level=level_text,
                    raw_path=str(raw_path),
                    chunker_version=resolved_chunker_version,
                    chunk_ids_digest=digest,
                    first_seen_run_id=(
                        existing.first_seen_run_id if existing is not None else run_token
                    ),
                    last_seen_run_id=run_token,
                    first_seen_at=(
                        existing.first_seen_at if existing is not None else now
                    ),
                    last_seen_at=now,
                    status=_DOC_ACTIVE,
                )
            )
            for row, text in rows:
                state.put_chunk_text(row.content_hash, text, lang=page.lang)
            state.replace_doc_chunks(dv_id, [row for row, _text in rows])
        except Exception as exc:  # noqa: BLE001 — изоляция state-записи
            errors.append(
                f"state: document_version({pid}/{url}): {type(exc).__name__}: {exc}"
            )

    def _mark(pid: str, u: str, status: CrawlItemStatus, **fields: Any) -> None:
        """Помечает исход URL: сеанс краула (L1a) + durable ``url_state`` (09 §6.3).

        Сеанс — прежнее поведение (:func:`_mark_session`); ``url_state`` пишется при
        наличии ``state`` для **каждого** исхода URL. ``state_fields`` — поля исхода
        документа (``doc_version_id``/``reuse_kind``/валидаторы/``http_status``), которые
        нельзя пронести через ``CrawlSessionItem`` (у него нет таких колонок).
        """
        state_fields = fields.pop("state_fields", None)
        _mark_session(pid, u, status, **fields)
        _record_url_state(pid, u, status, fields, state_fields)

    def _mark_session(pid: str, u: str, status: CrawlItemStatus, **fields: Any) -> None:
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
            if value is _CLEAR:
                record[name] = None
                continue
            if value is not None:
                if name == "error_kind":
                    # Ф0: единственная воронка записи ``crawl_session_items`` — здесь
                    # канонизируем класс сбоя, чтобы persisted-значение всегда было
                    # членом FailureKind (или source_unavailable для неизвестного).
                    value = _canonical_failure_kind(value)
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

        **Файловый staging (P6).** ``page.staged_path`` — тело уже лежит файлом у
        адаптера: хеш считается **потоково** по файлу (I19: адаптерному хешу не доверяем —
        его и нет), публикация — :func:`_publish_staged` (переезд в том же ФС без копии и
        без чтения в RAM). ``staged_path is None`` — прежний путь ``write_bytes``
        байт-в-байт.
        """
        staged = Path(page.staged_path) if page.staged_path else None
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
                try:
                    staged.unlink(missing_ok=True)
                except OSError:
                    pass
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
        level: DocumentLevel,
    ) -> None:
        """Inline-экстракция → доводка одного URL (путь без пула, ``None``/``1``).

        Тело экстракции (``extractor_obj.extract``) вынесено в :func:`_finish_one`; здесь —
        только inline-вызов и перевод исключения в ту же ошибку ``parser_failed``, что и
        пул-путь (P4: единая семантика отказа). ``extractor_obj`` — экстрактор уровня
        корня (P5; ``full`` переиспользует переданный порт). ``level`` — уровень корня:
        нужен как ключ кэша экстракции (Ф1.2) и не выводится из ``extractor_obj``, потому
        что ``full`` переиспользует переданный порт.
        """
        try:
            level_text = _as_text(level)
            # I19: ключ — хеш тех же байтов, что retention положил в ``raw_path``.
            # ``raw_path.stem`` и есть этот хеш (``_retain`` называет файл ``{hash}.bin``),
            # поэтому для страницы ключ байт-идентичен ``content_hash(page.content)``, а для
            # staging-артефакта (``page.content`` пуст) ключ не вырождается в хеш пустоты.
            cache_key = raw_path.stem
            extracted = (
                None
                if extract_cache is None
                else extract_cache.get(
                    cache_key, getattr(extractor_obj, "version", ""), level_text
                )
            )
            if extracted is None:
                extracted = extractor_obj.extract(page, fetch_plan.extract)
                if extract_cache is not None:
                    extract_cache.put(
                        extracted, level=level_text, content_hash=cache_key
                    )
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
                extractor_obj,
                level,
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
            extractor_obj,
            level,
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
        extractor_obj: ExtractorPort,
        level: DocumentLevel,
        *,
        extract_error: str | None = None,
        extract_failure_kind: FailureKind | None = None,
        reuse: _DocReuse | None = None,
    ) -> None:
        """Доводка одного URL после экстракции: дедуп → нарезка → ``_mark`` (главный поток).

        P4: граница расщепления проходит **механически** по ``extracted`` — результат
        экстракции приходит либо из inline-вызова, либо из ``future.result()`` пула. Сырые
        байты уже сохранены (``raw_path``); шаги изолируют ошибки в ``errors`` и фиксируют
        терминальный статус элемента сеанса через ``_mark`` (FIFO на главном потоке).

        **Переиспользование (09 §6.3/§9).** ``reuse`` — готовая версия документа (путь
        ``304``, экстракция не вызывалась); ``None`` — обычная экстракция. При ``200`` с
        прежним ``content_hash`` версия ищется здесь же (:func:`_reuse_same_hash`): чанкер
        не вызывается, chunk-set берётся из ``chunk_store`` после проверки I19
        (:func:`_load_reused_chunks`). Run-level ``chunk_id`` остаётся прежним
        (:func:`_chunk_id`) — идентификаторы индекса не меняются.

        ``extract_failure_kind`` — класс отказа извлечения: ``None`` ⇒ ``parser_failed``
        (прежнее отображение), таймаут задания пула приходит как ``extraction_timeout``.
        """
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
                _mark(
                    product_id,
                    url,
                    CrawlItemStatus.FAILED,
                    error_kind=(
                        extract_failure_kind or FailureKind.PARSER_FAILED
                    ).value,
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
                errors.append(
                    f"I19: {product_id}/{url}: ExtractedDoc без content_hash — отклонён"
                )
                _mark(
                    product_id,
                    url,
                    CrawlItemStatus.FAILED,
                    error_kind=FailureKind.PARSER_FAILED.value,
                    error="ExtractedDoc без content_hash",
                    raw_path=str(raw_path),
                )
                return
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
        if doc_hash in seen_docs:
            # Наблюдаемость: сколько документов схлопнулось по продукту. Класс исхода
            # не меняется (алиас получает ``FETCHED`` и версию близнеца) — это счётчик,
            # а не ошибка.
            duplicates[product_id] = duplicates.get(product_id, 0) + 1
            # Байты сохранены и контент уже проиндексирован близнецом в этой партиции.
            # Алиас получает ту же версию документа (если она уже записана), чтобы
            # следующий инкрементальный план знал, что контент URL уже в корпусе.
            found = _find_doc_version(
                product_id, doc_hash, extractor_token, level_text
            )
            _mark(
                product_id,
                url,
                CrawlItemStatus.FETCHED,
                raw_path=str(raw_path),
                state_fields={
                    "doc_version_id": (
                        found.doc_version_id if found is not None else None
                    ),
                    "etag": page.etag,
                    "last_modified": page.last_modified,
                    "last_http_status": page.status_code,
                },
            )
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
            extractor_version=doc_extractor_version,
            render_mode=_as_text(page.fetch_mode),
            lang=page.lang,
        )
        ingest.docs.append(doc)
        # Инкрементальная персистенция retention-провенанса: ``doc`` полностью сформирован
        # выше (``doc_id``/``content_hash``/``raw_path``/``extractor_version``/``lang``/
        # ``render_mode``), а сырые байты уже на диске (retention выполнен до экстракции).
        # Поэтому пишем строку сразу, а не в конце продукта: жёсткое убийство (OOM/SIGKILL)
        # теряет максимум документ «в полёте» (ещё извлекаемый), а не весь накопленный
        # продукт. ``_persist_docs`` изолирует сбой записи в ``errors`` — краул продолжается.
        _persist_docs(store, [doc], errors)

        # 5. Переиспользование версии (200 с прежним content_hash) либо чанкер.
        active_reuse = reuse
        if active_reuse is None:
            active_reuse = _reuse_same_hash(
                product_id, url, doc_hash, extractor_token, level_text
            )
        #: ``(chunk_no, text, heading_path, context_prefix, provided_hash)`` — общая форма
        #: для чанкера и переиспользованного набора (I19-проверки ниже одинаковы).
        candidates: list[tuple[int, str, str, str, str]] = []
        if active_reuse is not None:
            candidates = [
                (
                    chunk.chunk_no,
                    chunk.text,
                    chunk.heading_path,
                    chunk.context_prefix,
                    chunk.content_hash,
                )
                for chunk in active_reuse.chunks
            ]
        else:
            try:
                chunks = chunker.chunk(extracted, document_id)
            except Exception as exc:  # noqa: BLE001
                # Сбой нарезки — не отказ источника/парсера: raw сохранён (FETCHED),
                # повторный resume сможет ре-извлечь без ре-фетча.
                errors.append(f"{product_id}/{url}: chunk: {type(exc).__name__}: {exc}")
                return
            candidates = [
                (
                    int(getattr(chunk, "chunk_no", 0)),
                    getattr(chunk, "text", "") or "",
                    getattr(chunk, "heading_path", "") or "",
                    getattr(chunk, "context_prefix", "") or "",
                    str(getattr(chunk, "content_hash", "") or "").strip(),
                )
                for chunk in sorted(
                    chunks or [], key=lambda c: getattr(c, "chunk_no", 0)
                )
            ]

        accepted: list[tuple[int, str, str, str]] = []
        for chunk_no, text, heading_path, context_prefix, provided_hash in candidates:
            # I19: у каждого индексируемого чанка обязан быть непустой hash.
            if not provided_hash:
                errors.append(f"I19: {document_id}: чанк без content_hash отклонён")
                continue
            if not text.strip():
                errors.append(f"I19: {document_id}: чанк с пустым текстом отклонён")
                continue

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
                    heading_path=heading_path,
                    lang=page.lang,
                    chunk_no=chunk_no,
                    text=text,
                    context_prefix=context_prefix,
                    # Авторитетный хеш — от чистого текста (I19/§10).
                    content_hash=content_hash(text),
                    # build_id проставляется в Pass B после фиксации id сборки.
                    build_id=None,
                    embed_model_id=embed_model_id or None,
                    embedded_at=None,
                )
            )
            accepted.append((chunk_no, text, heading_path, context_prefix))

        # 09 §6.3/§9: версия документа + chunk-set в durable-субстрате. Идентификатор
        # выводится детерминированно (ключ §4.2), поэтому он известен и при сбое записи —
        # ``url_state`` всё равно сошлётся на правильную версию.
        dv_id = doc_version_id(product_id, doc_hash, extractor_token, level_text)
        _write_document_state(
            product_id,
            url,
            page,
            dv_id=dv_id,
            doc_hash=doc_hash,
            extractor_token=extractor_token,
            level_text=level_text,
            raw_path=raw_path,
            chunks=accepted,
        )
        reused = active_reuse is not None
        _count_doc(product_id, reused=reused)

        # L1a: документ успешно добыт и нарезан → элемент сеанса терминален ``FETCHED``
        # (сеанс трекает краул: сырые байты сохранены = «ready» для материализации).
        _mark(
            product_id,
            url,
            CrawlItemStatus.FETCHED,
            raw_path=str(raw_path),
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

        ``if_none_match``/``if_modified_since`` (09 §6.3) — валидаторы условного GET из
        ``url_state``: edge отправляет ``304`` тем же контрактом, что и локальный путь, и
        обязан получить их в работе (иначе split-профиль не экономит трафик). ``None`` —
        прежний безусловный GET (байт-идентичное поведение).
        """
        plan_i = target.plan
        if_none_match, if_modified_since = _validators(pid, url)
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

    # --- C1/L1: фазовый пул и FIFO-очередь экстракции ------------------------- #
    # Пул создаётся лениво на ключ ``(level, window)`` и живёт до ``finally`` фазы
    # (обёртка :func:`build_corpus`), а не на seed-root. Очередь доводки — тоже фазовая:
    # хвост корня, не способный расширить свой frontier, доводится во время фетча
    # следующего корня, а не блокирует его.
    extract_pending: deque[_PendingExtract] = deque()
    #: Корни с недоведённым хвостом — в порядке дренажа (порядок провенанса сохраняется).
    extract_finalizers: deque[_RootDrain] = deque()

    def _finalize_ready_roots() -> None:
        """Вызывает финализаторы корней, чьи хвосты доведены, — строго в порядке корней."""
        while extract_finalizers and extract_finalizers[0].pending == 0:
            drain = extract_finalizers.popleft()
            finalize = drain.finalize
            if finalize is not None and not drain.finalized:
                drain.finalized = True
                finalize()

    def _recover_extract_pool(
        pool_key: tuple[DocumentLevel, int], label: str
    ) -> None:
        """Отравленный пул: discard + bounded rebuild; исчерпание — терминальный отказ.

        Повторное использование мёртвого пула запрещено; авто-ретрая нет. Inline после
        исчерпания не включается: на memory-constrained хосте docling в родителе — худший
        режим, а тихая деградация скрыла бы дефект.
        """
        if pools.rebuild(pool_key):
            return
        errors.append(
            f"{label}: пул экстракции {pool_key[0].value}/W={pool_key[1]} исчерпал "
            f"пересборки ({_EXTRACT_MAX_POOL_REBUILDS}) — остаток прогона терминально "
            "FAILED (parser_failed), без inline-экстракции"
        )

    def _abort_extract_pending(reason: str) -> None:
        """Помечает все in-flight задания (все корни) ``FAILED`` — пул мёртв.

        Хвосты уже отложенных корней тоже принадлежат этому пулу, поэтому отказ получают
        все: исход видим, а не потерян вместе с пересборкой.
        """
        while extract_pending:
            item = extract_pending.popleft()
            item.commit(None, reason, None)
            item.root.pending -= 1
        _finalize_ready_roots()

    def _finish_extract(item: _PendingExtract) -> None:
        """Доводит oldest задание фазовой очереди: ``future.result`` → доводка корня (FIFO).

        P4/C1: ``BrokenProcessPool`` — воркер умер (краш/OOM): все in-flight → ``FAILED``,
        пул discard+rebuild (bounded) либо терминальный отказ. Таймаут — **per-artifact**:
        терминально помечается только это задание (``extraction_timeout``), пул продолжает
        работу; пересборка — только если пул не дал ни одного завершения за ``2 × timeout``
        (:meth:`_ExtractPools.note_timeout`).
        """
        try:
            extracted = item.future.result(timeout=_extract_timeout)
        except TimeoutError:
            # C1: зависший артефакт инвалидируется точечно; пул (и его здоровые воркеры)
            # продолжает работу. ``cancel`` на уже исполняемом future — no-op.
            cancel = getattr(item.future, "cancel", None)
            if cancel is not None:
                try:
                    cancel()
                except Exception:  # noqa: BLE001 — инвалидация best-effort
                    pass
            item.commit(None, "extraction timeout", FailureKind.EXTRACTION_TIMEOUT)
            if pools.note_timeout(item.pool_key):
                # ``2 × timeout`` без единого завершения: пул мёртв — reap + bounded rebuild.
                pools.terminate(item.pool_key)
                _abort_extract_pending("extraction pool terminated after timeout")
                _recover_extract_pool(item.pool_key, f"{item.pid}/{item.url}")
        except BrokenProcessPool:
            item.commit(None, "extraction worker died", None)
            _abort_extract_pending("extraction worker died")
            _recover_extract_pool(item.pool_key, f"{item.pid}/{item.url}")
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            item.commit(None, f"{type(exc).__name__}: {exc}", None)
        else:
            pools.note_completion(item.pool_key)
            item.commit(extracted, None, None)
        finally:
            item.root.pending -= 1
            _finalize_ready_roots()

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

    def _fresh_sitemap_seeds(
        pid: str, seed_url: str, frontier: Frontier
    ) -> _SitemapSeeds:
        """``<loc>`` sitemap, ещё не прикреплённые ни к одному seed-root продукта (дедуп).

        Один и тот же sitemap у нескольких seed-URL даёт один набор ``<loc>``; прикреплять
        их к каждому корню — двойной фетч/бюджет, поэтому свежие seeds отдаются один раз.

        ``<loc>`` — **подчинённые** seeds корня, а не задающие scope: loc проходит тот же
        ``frontier.in_scope`` (host + path-граница + traversal-guard), что и обнаруженная
        ссылка. Scope-чек исполняется **до** пометки ``(pid, loc)`` использованным, поэтому
        loc, отвергнутый этим корнем, остаётся доступным другому корню продукта. Кэш опроса
        sitemap (``(pid, sitemap_url)``) остаётся нефильтрованным.

        Пометка ``(pid, loc)`` **не** делается здесь (F12): loc может быть отвергнут уже
        после scope — блэклистом профиля корня (``exclude_paths``/``exclude_documents``) или
        ``max_documents`` при постановке в очередь. Такой loc обязан остаться доступным
        другому корню продукта, поэтому ``sitemap_seed_used`` пополняется в
        :func:`_drain_root` только по факту приёма в очередь (``add_seed`` → ``queued``/
        ``dup``).

        Отказ scope — **провенанс**, не ``errors`` (F10): host-wide sitemap при path-scoped
        seed отвергает большинство ``<loc>`` штатно. Отказы агрегируются по классу
        (:func:`_sitemap_skip_class`): ``host`` (sitemap рекламирует другой host) отдельно
        от ``path`` (тот же host, путь вне префикса) — иначе host-сигнал тонул бы в массе
        path-отказов. На корень — не более одной записи на класс (не на loc).
        """
        fresh: list[str] = []
        rejected: dict[str, list[str]] = {_SITEMAP_SKIP_HOST: [], _SITEMAP_SKIP_PATH: []}
        for loc in _expand_sitemap(pid, seed_url):
            if not frontier.in_scope(loc):
                rejected[_sitemap_skip_class(loc, frontier.root_url)].append(loc)
                continue
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
        return _SitemapSeeds(fresh=tuple(fresh), skipped=tuple(skipped))

    def _local_prefetch(
        pid: str,
        wu: str,
        wtarget: Any,
        *,
        depth: int | None = None,
        scope_host: str | None = None,
        enforce_redirect_scope: bool = False,
        robots: bool = False,
        max_response_bytes: int,
        artifact_max_bytes: int | None = None,
        attachment_formats: Sequence[str] | None = None,
    ) -> _LocalFetch:
        """D4 pre-fetch (главный поток): resume/browser-guard/решение о пейсинге.

        **Чистая** фаза: не пишет ни в сеанс, ни в ``applied_engines``. Единственное
        исключение — сбой чтения ``url_state`` (09 §6.3) фиксируется в ``errors``: он
        деградирует запрос до безусловного GET, а не роняет фазу. Все прочие мутации
        отложены в :func:`_commit_local` (FIFO-порядок коммита) — это позволяет
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
        if_none_match, if_modified_since = _validators(pid, wu)
        if if_none_match is not None or if_modified_since is not None:
            fetch_plan = replace(
                fetch_plan,
                if_none_match=if_none_match,
                if_modified_since=if_modified_since,
            )
        prior_item = items.get((pid, wu))
        resume_source = (
            _resume_source(
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
            if isinstance(resume_source, _RetainedReject)
            else None
        )
        resume = resume_source if isinstance(resume_source, _RetainedRaw) else None
        # Терминальный класс (not_found/payload_too_large/unsupported_binary): живой
        # фетч не нужен и вреден (см. helper).
        terminal = _terminal_failure_kind(prior_item) is not None
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
            terminal=terminal,
            reject=reject,
            max_artifact_bytes=artifact_max_bytes,
            attachment_formats=(
                None if attachment_formats is None else tuple(attachment_formats)
            ),
        )

    def _local_fetch(desc: _LocalFetch) -> RawPage:
        """D4 fetch-фаза (**единственный** вызов, идущий в пул): pacing + ``active.fetch``.

        Никакой мутации состояния сеанса здесь нет — только живой сетевой вызов.
        ``limiter.wait`` даёт интервал **по origin**: воркеры пула зовут pacing
        конкурентно, и ``RateLimiter`` резервирует слот на origin под локом, а спит уже
        вне него — иначе один хост блокировал бы все остальные. Гарантия — внутри
        процесса; при пуле процессов состояние не разделяется (см. ``util/rate_limit``).
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

        # Терминальный класс (not_found/payload_too_large/unsupported_binary): определённый
        # ответ уже получен либо исполнение упёрлось в осознанную границу; живой фетч не
        # выполняется. Элемент остаётся ``FAILED`` (класс сбоя, не статус), ``attempts`` не
        # растёт (``_mark`` инкрементит только при смене статуса), поэтому исход не
        # превращается в ``attempts_exhausted``.
        if desc.terminal:
            prior = desc.prior_item
            kind = _terminal_failure_kind(prior) or FailureKind.NOT_FOUND
            _mark(
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
            try:
                reject.path.unlink(missing_ok=True)
            except OSError:
                pass
            errors.append(f"{pid}/{wu}: {reject.error_kind.value}: {reject.reason}")
            _mark(
                pid,
                wu,
                CrawlItemStatus.FAILED,
                error_kind=reject.error_kind.value,
                error=reject.reason,
                raw_path=_CLEAR,
                content_hash=_CLEAR,
                depth=desc.depth,
            )
            return None

        if desc.resume is not None:
            assert desc.prior_item is not None  # resume возможен только при наличии элемента
            retained = desc.resume
            # 09 §6.3: валидаторы сеанс не хранит — они приходят из ``url_state`` (durable),
            # чтобы запись состояния после resume не потеряла условную валидацию.
            page = _resume_page(
                desc.prior_item, wu, retained, fetch_plan, _validators(pid, wu)
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
            # Класс исхода несёт сам адаптер: ``failure_kind`` (404/410 → ``not_found``)
            # либо ``refusal_reason`` (robots-отказ, D1/§10). Нет атрибута — честный
            # дефолт ``source_unavailable``. ``_mark`` канонизирует значение.
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
            # не выдумываем (carry-forward в ``_mark_session`` не перекрывает прежнее).
            _mark(
                pid,
                wu,
                CrawlItemStatus.FAILED,
                error_kind=error_kind,
                error=reason,
                depth=desc.depth,
                http_status=getattr(exc, "http_status", None),
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

        # P0: границы исполнения ДО retention/экстракции. Сначала размер (OVA/образ
        # должен стать payload_too_large, а не binary-sniff), затем бинарный не-документ.
        cap = fetch_plan.max_response_bytes
        if cap is not None and len(page.content) > cap:
            reason = (
                f"payload_too_large: {len(page.content)} байт > лимита {cap}"
            )
            errors.append(f"{pid}/{wu}: {reason}")
            _mark(
                pid,
                wu,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.PAYLOAD_TOO_LARGE.value,
                error=reason,
                raw_path=_CLEAR,
                content_hash=_CLEAR,
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
            _mark(
                pid,
                wu,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.UNSUPPORTED_BINARY.value,
                error=reason,
                raw_path=_CLEAR,
                content_hash=_CLEAR,
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
        max_response_bytes: int,
        artifact_max_bytes: int | None = None,
        attachment_formats: Sequence[str] | None = None,
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
            max_response_bytes=max_response_bytes,
            artifact_max_bytes=artifact_max_bytes,
            attachment_formats=attachment_formats,
        )
        if (
            desc.resume is not None
            or desc.reject is not None
            or desc.browser_blocked
            or desc.terminal
        ):
            return _commit_local(desc)

        # P2-3: politeness-пейсинг — единый владелец corpus (R7-независимый). Применяется
        # **только** к живому фетчу: путь ``resume`` (retained raw, выше) не пейсится.
        return _fetch_and_commit(desc)

    # D1/G: accumulators честности усечения. ``truncation_records`` — детализация по
    # ``(product, seed-root)``; флаг/продукты уходят в провенанс сеанса и ``IndexBuild``.
    truncation_records: list[dict[str, Any]] = []
    truncated_products: set[str] = set()
    pages_fetched_total = 0
    #: S1: фактически добытые ревалидационные страницы (discovery-счётчик отдельно).
    revalidation_fetched_total = 0

    def _record_root_outcome(
        product_id: str,
        root_url: str,
        frontier: Frontier,
        sitemap_skips: Sequence[Mapping[str, Any]],
        *,
        revalidation: Mapping[str, Any] | None = None,
    ) -> None:
        """Провенанс одного seed-root после drain: усечение/пропуски (D1/G, P5, D3/F10).

        Общий шаг авторских корней и известных URL инкрементального work-set: порядок
        записей и их содержимое не зависят от того, откуда пришёл корень.

        ``revalidation`` — аддитивная наблюдаемость ревалидации **в любом режиме**:
        ``{"selected": N, "fetched": M}`` (enqueued и фактически добытые) и, если
        операторский потолок отложил/отсёк URL, ``"deferred": K``/``"skipped": S``.
        ``None`` — у корня не было известных URL: ключ не появляется, payload
        ``state is None`` байт-в-байт прежний.
        """
        nonlocal pages_fetched_total, revalidation_fetched_total
        pages_fetched_total += frontier.fetched
        revalidation_fetched_total += frontier.revalidation_fetched
        if frontier.truncated:
            truncated_products.add(product_id)
        # P5: пропуск документов «прореживает» корпус продукта по построению — он
        # питает тот же канал аномалии, что и усечение (защита от false ❌ I2/I2b:
        # `source_scope: exhaustive` поверх «прореженного» продукта — аномалия
        # для человека). Причина — не ``max_*``, а ``document_level=off``.
        if frontier.skipped_documents:
            truncated_products.add(product_id)
        record: dict[str, Any] = {
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
        # D3/F10: отказы scope у ``<loc>`` sitemap — провенанс, не ``errors``.
        # Ключ появляется только при наличии отказов: без sitemap запись байт-в-байт
        # прежняя. Классы ``host``/``path`` раздельны (F6).
        if sitemap_skips:
            record["skipped_out_of_scope"] = [dict(skip) for skip in sitemap_skips]
        if revalidation is not None:
            record["revalidation"] = dict(revalidation)
        truncation_records.append(record)

    def _plan_incremental_items(
        product_id: str, roots: Sequence[str]
    ) -> list[IncrementalItem]:
        """Дополнительный work-set инкрементального продукта (09 §6.3, MVP-3b).

        Возвращает только **не**-авторские элементы: авторские корни обрабатывает прежний
        путь (каталоги/sitemap), а планировщик лишь подтверждает их ``fetch``. Порядок —
        планировщика (``url_state`` отсортирован по ``normalized_url``).

        ``exclude_urls`` не передаётся: набор исключений резолвится per-root
        (``resolve_crawl_limits``: ``app < run < site``), поэтому дешёвого продуктового
        набора нет; ``Frontier`` всё равно применяет блэклист к каждому seed и оставляет
        видимую запись, а не тихий пропуск.

        ``reuse`` (политика ``offline``) в этом срезе **не реализован**: элемент честно
        деградирует до ``revalidate`` с одной видимой ошибкой на продукт — молчаливая
        смена политики запрещена. ``new``/``full`` план не расширяют (прежний BFS/sitemap).
        """
        if state is None:
            return []
        if (
            _as_product_mode((product_modes or {}).get(product_id))
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
        reuse_reported = False
        for item in planned:
            if item.root_url is not None and item.normalized_url in author:
                continue  # авторский корень — прежний путь (каталог/sitemap)
            if item.kind is IncrementalKind.REUSE:
                if not reuse_reported:
                    errors.append(f"offline reuse не реализован в corpus: {product_id}")
                    reuse_reported = True
                item = replace(item, kind=IncrementalKind.REVALIDATE)
            if item.kind is IncrementalKind.REVALIDATE:
                # Валидаторы едут и в локальный ``FetchPlan``, и в ``CrawlWorkItem``.
                planned_validators[(product_id, item.normalized_url)] = (
                    item.etag,
                    item.last_modified,
                )
            extra.append(item)
        return extra

    def _group_revalidation_items(
        roots: Sequence[str], items: Sequence[IncrementalItem]
    ) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
        """Группирует известные URL по авторским корням (scope inference, каждый прогон).

        ``url_state.seed_id`` вне этого среза: происхождение считается заново по
        ``Frontier.in_scope``. Побеждает **самый специфичный** ``root_path_prefix``
        (длина пути), при равенстве — авторский порядок. URL вне scope всех корней —
        orphan (``origin -> urls``): один synthetic-бакет на origin, ревалидация без
        записи усечения.
        """
        scopes = [(root, Frontier(root, CrawlLimits())) for root in roots]
        groups: dict[str, list[str]] = {root: [] for root in roots}
        orphans: dict[str, list[str]] = {}
        for item in items:
            best_root: str | None = None
            best_len = -1
            for root, probe in scopes:
                if not probe.in_scope(item.url):
                    continue
                if len(probe.path_prefix) > best_len:
                    best_root = root
                    best_len = len(probe.path_prefix)
            if best_root is None:
                orphans.setdefault(_url_origin(item.url), []).append(item.url)
            else:
                groups[best_root].append(item.url)
        return groups, orphans

    def _resolve_product_revalidate_cap(roots: Sequence[str]) -> int | None:
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
        return resolve_crawl_limits(
            roots[0],
            app=crawl_app or CrawlConfig(),
            run=crawl_defaults,
            profiles=domain_profiles or {},
        ).revalidate_max_documents

    def _apply_revalidation_cap(
        product_id: str,
        groups: Mapping[str, Sequence[str]],
        orphans: Mapping[str, Sequence[str]],
        items: Sequence[IncrementalItem],
        cap: int,
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
        try:
            rows = {
                row.normalized_url: row
                for row in state.iter_url_state(product_id)  # type: ignore[union-attr]
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
                limits = resolve_crawl_limits(
                    owner,
                    app=crawl_app or CrawlConfig(),
                    run=crawl_defaults,
                    profiles=domain_profiles or {},
                )
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
                state.upsert_url_state(  # type: ignore[union-attr]
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

    for product_id in products:
        ingest = _ProductIngest(product_id=product_id)
        # I17: дедуп в пределах продукт-партиции (см. docstring модуля).
        seen_docs: dict[str, str] = {}
        seen_chunk_ids: set[str] = set()
        #: F13: durable-URL, уже заряженные в resume-бюджет **продукта** (первый корень-
        #: претендент побеждает). URL, лежащий в scope нескольких корней, не должен
        #: расходовать ``max_documents`` каждого из них — иначе бюджет исчерпывается досрочно.
        resume_counted: set[str] = set()
        product_roots = url_map.get(product_id, [])
        #: 09 §6.3: известные URL продукта (``revalidate``) — после авторских корней.
        incremental_items = _plan_incremental_items(product_id, product_roots)
        #: 09 §6.3: группировка **всех** известных URL по авторским корням (scope
        #: inference) до применения операторского потолка. ``state is None``/
        #: не-incremental ⇒ пусто: grouping — no-op (байт-в-байт).
        if incremental_items:
            all_revalidation_groups, all_orphan_buckets = (
                _group_revalidation_items(product_roots, incremental_items)
            )
        else:
            all_revalidation_groups, all_orphan_buckets = {}, {}

        #: Ops-ось «не ограничивать, а измерять» (default-off): потолок ревалидации
        #: **на продукт**. Отсутствие оси — все известные URL, без записи ``stale`` и
        #: без усечения; задание — видимый defer хвоста. ``split`` — transport, который
        #: не исполняет локальную политику отбора: не притворяемся, что ось применена.
        revalidate_cap = _resolve_product_revalidate_cap(product_roots)
        cap_applies = bool(
            revalidate_cap is not None
            and incremental_items
            and len(incremental_items) > revalidate_cap
        )
        # S3: split-ошибка — только когда потолку действительно есть что ограничивать.
        if cap_applies and executor is not None:
            errors.append(
                f"{product_id}: crawl.revalidate_max_documents={revalidate_cap} "
                "поддерживается только локальным краулом (local) — ось игнорируется "
                "(split)"
            )
            revalidate_cap = None
            cap_applies = False
        if cap_applies:
            (
                revalidation_groups,
                orphan_buckets,
                deferred_by_root,
                deferred_by_origin,
                skipped_by_root,
                skipped_by_origin,
                revalidation_deferred_total,
                revalidation_skipped_total,
                revalidation_cap_fired,
            ) = _apply_revalidation_cap(
                product_id,
                all_revalidation_groups,
                all_orphan_buckets,
                incremental_items,
                int(revalidate_cap),  # type: ignore[arg-type]
            )
        else:
            revalidation_groups = {
                key: list(urls) for key, urls in all_revalidation_groups.items()
            }
            orphan_buckets = {
                key: list(urls) for key, urls in all_orphan_buckets.items()
            }
            deferred_by_root = {}
            deferred_by_origin = {}
            skipped_by_root = {}
            skipped_by_origin = {}
            revalidation_deferred_total = 0
            revalidation_skipped_total = 0
            revalidation_cap_fired = False
        #: **Все** известные URL продукта (включая отложенные cap'ом) — вне resume-
        #: бюджета ``max_documents`` (B2/F13): отложенный URL не фетчится в этом цикле,
        #: но и не должен заряжать discovery-бюджет при resume. Скан resume ниже
        #: исключает их до подсчёта.
        revalidation_url_set = {
            url
            for urls in (
                *all_revalidation_groups.values(),
                *all_orphan_buckets.values(),
            )
            for url in urls
        }

        def _drain_root(
            pid: str,
            root_url: str,
            root_target: Any,
            *,
            sitemap_seeds: bool = False,
            revalidate: Sequence[str] = (),
            seed: bool = True,
            defer_tail: bool = True,
        ) -> tuple[Frontier, list[dict[str, Any]], _RootDrain]:
            """D1/D3: BFS-drain одного seed-root (scope/блэклист/лимиты/глубина).

            Seeds — глубина 0; ссылка со страницы глубины ``d`` → ``d+1``. ``sitemap_seeds``
            — развернуть per-site sitemap корня в дополнительные depth-0 seeds (D3); они
            **подчинены** scope корня (``frontier.in_scope``), проходят тот же блэклист/
            бюджет ``max_documents``, что и seed оператора.
            ``revalidate`` — известные URL этого корня (scope inference): ставятся
            **после** авторского seed и **до** BFS отдельным путём
            (:meth:`Frontier.add_revalidate_seed`), поэтому не расходуют ``max_documents``,
            не расширяют BFS (листья) и дедуплицируются ``_seen`` с переоткрытием ссылкой.
            ``seed=False`` — synthetic-бакет (orphan): авторского seed нет, только
            ревалидация; усечение такого бакета не пишется вызывающим.
            Ставка в очередь гейтится scope (host+path-граница) и блэклистом; ``max_documents``
            — жёсткий стоп (не исключение): останавливает постановку/обход этого корня.
            Возвращает frontier с провенансом усечения (``fetched``/``truncated``/
            ``truncated_reason``), агрегированный провенанс отказов scope у ``<loc>``
            (пусто без sitemap/без отказов) и :class:`_RootDrain` — хвост экстракции корня
            в фазовой очереди (C1). ``defer_tail=False`` (synthetic-бакеты ревалидации,
            вызывающий читает счётчики frontier сразу) — хвост доводится здесь же.
            """
            limits = resolve_crawl_limits(
                root_url,
                app=crawl_app or CrawlConfig(),
                run=crawl_defaults,
                profiles=domain_profiles or {},
            )
            frontier = Frontier(root_url, limits)
            # D3/дефект B (I17): ``<loc>`` sitemap — кандидаты **этого** корня; scope-фильтр
            # применяется до дедупа ``(pid, loc)``. Дедуп-пометка — не здесь, а по факту
            # приёма в очередь (F12, см. цикл ``extra_seeds`` ниже). Без sitemap
            # ``_expand_sitemap`` отдаёт ``[]`` без обращения к поллеру — путь не исполняется
            # (поведение байт-в-байт).
            sitemap = (
                _fresh_sitemap_seeds(pid, root_url, frontier)
                if sitemap_seeds
                else _SitemapSeeds()
            )
            extra_seeds = sitemap.fresh
            # P5: уровень извлечения документов — свойство корня (C1: ключ фазового пула
            # включает уровень — один конвертер на ключ). ``full`` переиспользует переданный
            # экстрактор (дефолт — без лишней загрузки весов); иной уровень строится фабрикой,
            # если она передана.
            level = limits.document_level
            root_extractor: ExtractorPort = extractor
            # ``full`` сюда не попадает: переданный экстрактор и есть уровень ``full``,
            # поэтому «понижение до переданного» для него невозможно. Отказ OCR на
            # ``full`` приходит из адаптера на построении конвертера и становится
            # ``parser_failed`` (видимая запись ``errors`` через ``_finish_one``/
            # ``_finish_extract``), а не тихой подменой уровня.
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

            # P4/C1: окно экстракции резолвится **на корень** (лимиты корня), а **пул** —
            # фазовый: ключ ``(level, window)`` в реестре фазы (C1/L1). ``None``/``1`` ⇒ пул
            # не создаётся (экстракция inline, байт-в-байт). ``W`` всегда конечный ``int ≥ 2``.
            extract_workers_axis = (
                int(limits.extract_workers)
                if limits.extract_workers is not None and int(limits.extract_workers) > 1
                else None
            )
            # P4/OOM: рециклинг воркеров (``crawl.extract_max_tasks_per_child``). Ось имеет
            # смысл только для процессного пула (local): при её наличии экстракция идёт
            # пулом даже при ``extract_workers == 1``, окно — 1 (родитель не копит pending),
            # а воркер пересоздаётся после N заданий, освобождая рабочий набор конвертера.
            recycle_tasks = limits.extract_max_tasks_per_child
            can_recycle = bool(
                recycle_tasks is not None
                and extraction_pool is not None
                and extraction_pool.supports_worker_recycling
            )
            extract_window = (
                extract_workers_axis
                if extract_workers_axis is not None
                else (1 if can_recycle else None)
            )
            if extract_workers_axis is not None and extraction_pool is None:
                # Ось задана, но пул не сконфигурирован — не тихо: явная запись + inline.
                errors.append(
                    f"{pid}/{root_url}: crawl.extract_workers={extract_window} задан, "
                    "но пул экстракции не сконфигурирован — экстракция inline"
                )
            if recycle_tasks is not None and extraction_pool is None:
                errors.append(
                    f"{pid}/{root_url}: crawl.extract_max_tasks_per_child="
                    f"{recycle_tasks} задан, но пул экстракции не сконфигурирован — "
                    "экстракция inline"
                )
            elif (
                recycle_tasks is not None
                and not extraction_pool.supports_worker_recycling  # type: ignore[union-attr]
            ):
                # ``split`` (потоковый пул) не пересоздаёт воркеров — не молчим.
                errors.append(
                    f"{pid}/{root_url}: crawl.extract_max_tasks_per_child="
                    f"{recycle_tasks} поддерживается только процессным пулом (local) — "
                    "ось игнорируется"
                )
            #: C1: ключ фазового пула — уровень документа + окно экстракции. Пул один на
            #: прогон и переиспользуется всеми корнями/продуктами (forkserver и веса
            #: конвертера платятся один раз); ``shutdown`` — один раз в ``finally`` фазы.
            pool_key: tuple[DocumentLevel, int] | None = (
                (level, extract_window) if extract_window is not None else None
            )
            #: Исполнять ли экстракцию в пуле (снимается inline-fallback'ом при отказе).
            defer_extract = extract_window is not None and extraction_pool is not None
            #: C1: хвост этого корня в фазовой очереди доводки (счётчик + финализатор).
            root_drain = _RootDrain()
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
            #
            # F13: бюджет — **продукта**, а не корня. При нескольких корнях на host URL
            # в scope двух корней иначе заряжался бы обоим, и ``max_documents`` исчерпывался
            # бы досрочно на resume. Первый корень-претендент помечает URL в
            # ``resume_counted``; последующие корни его не пересчитывают (fresh-путь
            # неизменен: ``note_fetched`` — только на реально добытую страницу).
            consumed = 0
            for (ipid, iurl), item in items.items():
                if ipid != pid or iurl in resume_counted:
                    continue
                # Известные URL ревалидации — вне бюджета discovery: иначе resumed
                # сессия заряжала бы ими ``max_documents``, seed стал бы ``truncated``
                # и продукт ложно ``complete=False`` (регрессия B2/F13 уровнем выше).
                if iurl in revalidation_url_set:
                    continue
                if not frontier.in_scope(iurl):
                    continue
                counted = _resume_source(
                    item,
                    max_bytes=limits.max_response_bytes,
                    artifact_max_bytes=limits.max_artifact_bytes,
                    attachment_formats=limits.attachment_formats,
                )
                if not isinstance(counted, _RetainedRaw):
                    # Нет пригодного resume (oversize/binary/несовпадение) — не бюджет.
                    continue
                consumed += 1
                resume_counted.add(iurl)
            frontier.note_consumed(consumed)
            seed_status = frontier.add_seed(root_url) if seed else "synthetic"
            seed_blacklisted = seed_status == "blacklisted"
            seed_truncated = seed_status == "truncated"
            if seed_blacklisted:
                errors.append(
                    f"{pid}/{root_url}: seed зачёркнут блэклистом "
                    "(exclude_paths/exclude_documents) — не enqueued"
                )
            if seed_truncated:
                errors.append(
                    f"{pid}/{root_url}: seed не поставлен — достигнут max_documents"
                )

            # 09 §6.3: известные URL — depth-0 листья **после** авторского seed и **до**
            # BFS. Enqueue до BFS позволяет ``frontier._seen`` схлопнуть известный URL,
            # переоткрытый ссылкой (иначе безусловный второй GET — потеря 304-экономии).
            # Путь вне бюджета discovery (``add_revalidate_seed``): ревалидация не является
            # «документом, который добавляет обход». Ранние возвраты ниже не должны
            # терять группу: blacklisted/truncated корень всё равно ревалидирует известные URL.
            revalidation_queued = 0
            for known in revalidate:
                if frontier.add_revalidate_seed(known) == "queued":
                    revalidation_queued += 1
            if (seed_blacklisted or seed_truncated) and revalidation_queued == 0:
                _emit_progress()
                return frontier, list(sitemap.skipped), root_drain

            # D3: дополнительные depth-0 seeds (``<loc>`` sitemap) — под тем же блэклистом и
            # бюджетом, что и seed оператора (``add_seed`` возвращает статус). Scope-фильтр
            # живёт **ровно в одном месте** — ``_fresh_sitemap_seeds`` (тот же ``frontier``):
            # ``extra_seeds`` приходят только оттуда, поэтому повторный scope-чек здесь был бы
            # недостижимой защитой (F7 — удалён; тест на удаление фильтра есть).
            #
            # F12: ``(pid, loc)`` помечается использованным **только при приёме в очередь**
            # (``queued``/``dup`` — loc уже у этого корня). Отказ блэклистом/бюджетом не
            # расходует loc: другой корень того же продукта ещё вправе его взять. Инвариант
            # «один loc — не более одного фетча на продукт» держится именно этой пометкой.
            for extra in (() if (seed_blacklisted or seed_truncated) else extra_seeds):
                extra_status = frontier.add_seed(extra)
                if extra_status in ("queued", "dup"):
                    sitemap_seed_used.add((pid, extra))
                elif extra_status == "blacklisted":
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
                extract_failure_kind: FailureKind | None = None,
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
                # 09 §6.3: ревалидация известного URL — depth-0 лист и **вне** бюджета
                # discovery. ``pages_fetched``/``truncated`` продолжают означать ровно
                # «документы, которые добавил обход этого корня».
                is_revalidation = frontier.is_revalidation(work_url)
                if freshly_fetched:
                    if is_revalidation:
                        # S1: ревалидационный трафик не смешивается с discovery-счётчиком
                        # ``fetched``/``pages_fetched``, но обязан быть видимым.
                        frontier.revalidation_fetched += 1
                    else:
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
                # P6: допущенный артефакт-контейнер ретейнён, но спуск в контейнеры —
                # следующий слой (``ContainerPort``/``corpus_members``). Экстрактор на
                # пустом ``content`` (staging) дал бы ложный ``parser_failed``: контейнер —
                # не документ. Поэтому исход — видимый skip: элемент остаётся ``FETCHED``
                # (байты — evidence I19), причина — в ``errors``. Класс определяется
                # **байтами** (``classify_archive_bytes``: OOXML — документ, не контейнер)
                # и заявленным набором осей; до спуска ни один байт контейнера не читается
                # в RAM сверх головы (``head``/первые 8 KiB).
                if (
                    _is_attachment(
                        page.url or work_url,
                        page.content_type,
                        fetch_plan.attachment_formats,
                    )
                    and classify_archive_bytes(page.content or page.head or b"")
                    is not None
                ):
                    errors.append(
                        f"{pid}/{work_url}: attachment_skip: контейнер ретейнён, "
                        "спуск не исполняется (текущий слой)"
                    )
                    _emit_progress()
                    return
                if extracted is _INLINE:
                    # 09 §6.3/§9: ``304`` переиспользует версию документа **до**
                    # экстракции (экстрактор/чанкер не вызываются). Неудача reuse
                    # (нет версии/чанков, I19-расхождение) — видимая запись уже сделана,
                    # дальше обычный путь экстракции.
                    reuse = _reuse_for_page(pid, work_url, page)
                    if reuse is not None:
                        _finish_one(
                            pid,
                            work_url,
                            page,
                            fetch_plan,
                            raw_path,
                            None,
                            ingest,
                            seen_docs,
                            seen_chunk_ids,
                            root_extractor,
                            level,
                            reuse=reuse,
                        )
                    else:
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
                            level,
                        )
                else:
                    # 09 §6.3: пул уже извлёк документ (экстракция не отменяется — она
                    # оплачена), но при ``304`` нарезка переиспользуется: чанкер не зовётся.
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
                        root_extractor,
                        level,
                        extract_error=extract_error,
                        extract_failure_kind=extract_failure_kind,
                        reuse=(
                            _reuse_for_page(pid, work_url, page)
                            if extract_error is None
                            else None
                        ),
                    )
                _emit_progress()
                # D1: извлечение in-site ссылок и следующий уровень BFS. При max_depth=0
                # извлечение не выполняется вовсе (байт-в-байт прежнее поведение).
                # 09 §6.3: ревалидация — лист; её ссылки не расширяют discovery (иначе
                # глубокая страница, пере-добытая на depth 0, меняла бы coverage/стоимость).
                if (
                    not is_revalidation
                    and limits.max_depth > 0
                    and _looks_like_html(page)
                ):
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
                    cache_root=(
                        str(extract_cache.root) if extract_cache is not None else None
                    ),
                    cache_max_bytes=(
                        extract_cache.max_bytes if extract_cache is not None else None
                    ),
                )

            def _inflight_fresh() -> int:
                """Свеже-добытые страницы **этого корня** «в полёте» (ёмкость с окном, P4/§3).

                Очередь экстракции — фазовая (C1), поэтому фильтр по ``item.root``: хвосты
                других корней не отнимают ёмкость у этого. Ревалидация исключена: она не
                расходует ``max_documents`` и не должна отнимать ёмкость у discovery.
                """
                return sum(
                    1
                    for item in extract_pending
                    if item.root is root_drain
                    and item.freshly_fetched
                    and not frontier.is_revalidation(item.url)
                )

            def _has_fetch_capacity() -> bool:
                """Ёмкость корня с учётом in-flight: ``max_documents − fetched − inflight``."""
                if limits.max_documents is None:
                    return True
                return limits.max_documents - frontier.fetched - _inflight_fresh() > 0

            def _tail_may_extend_frontier() -> bool:
                """Может ли хвост **этого** корня расширить его frontier (BFS-ссылки)?

                ``_commit_outcome`` предлагает исходящие ссылки только для не-ревалидационной
                HTML-страницы при ``max_depth > 0``. Если ни одно задание хвоста этого не
                может, доводка безопасна после возврата корня: BFS от её момента не зависит,
                а фетч следующего корня не ждёт экстракции (C1). Иначе хвост доводится здесь
                же — прежний порядок байт-в-байт.
                """
                if limits.max_depth <= 0:
                    return False
                for item in extract_pending:
                    if item.root is not root_drain:
                        continue
                    if frontier.is_revalidation(item.url):
                        continue
                    if _looks_like_html(item.page):
                        return True
                return False

            def _drain_for_window() -> None:
                """Доводит oldest, пока окно полно или ёмкость исчерпана (с in-flight)."""
                while extract_pending and (
                    len(extract_pending) >= extract_window or not _has_fetch_capacity()
                ):
                    _finish_extract(extract_pending.popleft())

            def _make_commit(
                pid_arg: str,
                url_arg: str,
                depth_arg: int | None,
                outcome: tuple[RawPage, Path, FetchPlan, bool],
            ) -> Callable[[Any, str | None, FailureKind | None], None]:
                """Замыкание доводки исхода своего корня (C1: очередь доводки — фазовая)."""

                def _commit(
                    extracted: Any,
                    extract_error: str | None,
                    failure_kind: FailureKind | None,
                ) -> None:
                    _commit_outcome(
                        pid_arg,
                        url_arg,
                        depth_arg,
                        outcome,
                        extracted=extracted,
                        extract_error=extract_error,
                        extract_failure_kind=failure_kind,
                    )

                return _commit

            def _commit_or_defer(
                pid_arg: str,
                url_arg: str,
                depth_arg: int | None,
                outcome: tuple[RawPage, Path, FetchPlan, bool] | None,
            ) -> None:
                """P4 submit/finish-шов: пул активен ⇒ отложить экстракцию в фазовую очередь.

                Иначе — немедленный :func:`_commit_outcome` (inline, байт-в-байт). Отправка
                идёт с главного потока в порядке узлов (FIFO); окно не превышается инлайн-дренажем.

                C1/L4: проба ``extract_cache`` — **в родителе**, до submit. Хит идёт путём
                «готовый текст» (тот же ``_finish_one``, что у результата пула), не занимая
                слот воркера и не оплачивая инициализатор. Проба в воркере остаётся защитой;
                ключ родителя и воркера — один и тот же хеш ретенированных байтов.
                """
                nonlocal defer_extract
                if outcome is None:
                    return
                page, raw_path, fetch_plan, freshly_fetched = outcome
                if defer_extract and extract_cache is not None:
                    # C1/L4: проба — в родителе, до submit и до проверки исчерпания пула:
                    # готовый текст не требует экстракции вовсе. Ключ — ``raw_path.stem``
                    # (тот же хеш, что воркер считает из прочитанных байтов).
                    cached = extract_cache.get(
                        _extract_cache_key(raw_path),
                        str(getattr(root_extractor, "version", "") or ""),
                        _as_text(level),
                    )
                    if cached is not None:
                        _commit_outcome(
                            pid_arg, url_arg, depth_arg, outcome, extracted=cached
                        )
                        return
                if pool_key is not None and pools.abandoned(pool_key):
                    # Пул исчерпал пересборки: никакой inline-экстракции. Страница
                    # ретейнена, но исход терминален и видим (parser_failed).
                    _commit_outcome(
                        pid_arg,
                        url_arg,
                        depth_arg,
                        outcome,
                        extracted=None,
                        extract_error=(
                            "extraction pool exhausted after "
                            f"{_EXTRACT_MAX_POOL_REBUILDS} rebuilds — inline disabled"
                        ),
                    )
                    return
                if not defer_extract:
                    _commit_outcome(pid_arg, url_arg, depth_arg, outcome)
                    return

                def _new_pool() -> Executor:
                    if (
                        recycle_tasks is not None
                        and extraction_pool.supports_worker_recycling  # type: ignore[union-attr]
                    ):
                        # P4/OOM: счётчик заданий на процесс — воркер пересоздаётся
                        # (initializer `_extract_worker_init` строит экстрактор заново).
                        return extraction_pool.pool_factory(  # type: ignore[union-attr]
                            extract_window, level, max_tasks_per_child=recycle_tasks
                        )
                    # Ось не задана — прежний вызов (поведение байт-в-байт).
                    return extraction_pool.pool_factory(  # type: ignore[union-attr]
                        extract_window, level
                    )

                assert pool_key is not None  # defer_extract ⇒ окно и ключ заданы
                try:
                    executor = pools.executor(pool_key, _new_pool)
                except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                    errors.append(
                        f"{pid_arg}/{url_arg}: extract pool: "
                        f"{type(exc).__name__}: {exc} — inline"
                    )
                    defer_extract = False
                    _commit_outcome(pid_arg, url_arg, depth_arg, outcome)
                    return
                try:
                    future = executor.submit(
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
                root_drain.pending += 1
                extract_pending.append(
                    _PendingExtract(
                        pid=pid_arg,
                        url=url_arg,
                        depth=depth_arg,
                        page=page,
                        raw_path=raw_path,
                        fetch_plan=fetch_plan,
                        freshly_fetched=freshly_fetched,
                        future=future,
                        root=root_drain,
                        commit=_make_commit(pid_arg, url_arg, depth_arg, outcome),
                        pool_key=pool_key,
                    )
                )
                # Окно: не более ``W`` экстракций «в полёте» (доводка FIFO).
                while len(extract_pending) >= extract_window:
                    _finish_extract(extract_pending.popleft())

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
                    elif (
                        desc.resume is not None
                        or desc.reject is not None
                        or desc.browser_blocked
                        or desc.terminal
                    ):
                        # resume/reject/browser-guard/терминальный класс: живого фетча нет.
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
                    # C1: окно — фазовой очереди (пул общий), поэтому нужен лишь при
                    # активном пуле этого корня.
                    if extract_pending and extract_window is not None:
                        _drain_for_window()
                    if root_drain.pending and len(frontier) == 0:
                        # Очередь корня пуста, но доводка in-flight может добавить ссылки
                        # (``offer``/BFS на finish) — доводим всё, прежде чем счесть исчерпание.
                        # C1: если хвост BFS расширить не может, он откладывается на фазу —
                        # фетч следующего корня не ждёт экстракции.
                        if _tail_may_extend_frontier():
                            while root_drain.pending:
                                _finish_extract(extract_pending.popleft())
                        else:
                            break
                    if not _has_fetch_capacity() and not root_drain.pending:
                        # D3: seeds sitemap ставятся пачкой; ``max_documents`` обязан
                        # ограничивать и их (не только обнаруженные ссылки). Без лимита —
                        # всегда есть ёмкость. Проверка — в голове цикла на главном потоке.
                        # 09 §6.3: ревалидация известных URL бюджет не расходует; если
                        # голова очереди — такая группа, дренаж продолжается (бюджет
                        # исчерпан только для discovery).
                        if not frontier.next_is_revalidation():
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
                        remaining = (
                            None
                            if limits.max_documents is None
                            else limits.max_documents
                            - frontier.fetched
                            - _inflight_fresh()
                        )
                        # M2: всегда впускаем непрерывный revalidation-блок **плюс**
                        # discovery-room. Иначе при 0 < remaining < wave_items волна
                        # берёт лишь remaining свободных URL, fetched не растёт, и
                        # предохранитель раундов срабатывает на хвосте ревалидации.
                        reval_n = min(
                            frontier.revalidation_head_count(wave_items),
                            wave_items // 2,
                        )
                        room = (
                            wave_items
                            if remaining is None
                            else min(wave_items, max(0, remaining))
                        )
                        wave_size = min(
                            len(frontier),
                            wave_items,
                            max(reval_n, reval_n + room),
                        )
                        if wave_size <= 0:
                            break
                        entries: list[_WaveEntry] = []
                        work: list[CrawlWorkItem] = []
                        wave_has_discovery = False
                        for _ in range(wave_size):
                            node = frontier.next()
                            if node is None:
                                break
                            work_url, depth = node
                            if not frontier.is_revalidation(work_url):
                                wave_has_discovery = True
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
                            if _terminal_failure_kind(prior_item) is not None:
                                # Определённый ответ «ресурса нет» уже получен: в работу
                                # не подаём (иначе 404 стоил бы запросов на каждом цикле).
                                entries.append(
                                    _WaveEntry(
                                        pid,
                                        work_url,
                                        depth,
                                        "terminal_not_found",
                                        prior_item=prior_item,
                                    )
                                )
                                continue
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
                            resume_source = (
                                _resume_source(
                                    prior_item,
                                    max_bytes=limits.max_response_bytes,
                                    artifact_max_bytes=limits.max_artifact_bytes,
                                    attachment_formats=limits.attachment_formats,
                                )
                                if prior_item is not None
                                else None
                            )
                            reject = (
                                resume_source
                                if isinstance(resume_source, _RetainedReject)
                                else None
                            )
                            resume = (
                                resume_source
                                if isinstance(resume_source, _RetainedRaw)
                                else None
                            )
                            # Дескриптор коммита строится вручную (не ``_local_prefetch``):
                            # browser-guard/``active`` — оси локального краулера, а здесь
                            # байты доставляет исполнитель; ``browser_blocked`` всегда False.
                            desc = _LocalFetch(
                                pid=pid,
                                wu=work_url,
                                depth=depth,
                                fetch_plan=replace(
                                    work_target.plan,
                                    max_response_bytes=limits.max_response_bytes,
                                ),
                                active=None,
                                engine_value=work_target.engine,
                                prior_item=prior_item,
                                resume=resume,
                                browser_blocked=False,
                                poolable=False,
                                host=(urlsplit(work_url).hostname or "").lower(),
                                scope_host=frontier.root_host,
                                enforce_redirect_scope=limits.max_depth > 0,
                                reject=reject,
                            )
                            entries.append(
                                _WaveEntry(pid, work_url, depth, "fetch", desc=desc)
                            )
                            if resume is None and reject is None:
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
                            if entry.kind == "terminal_not_found":
                                # Терминальный класс уже получен: повторного фетча не было,
                                # элемент остаётся ``FAILED`` со своим ``error_kind``.
                                prior_item = entry.prior_item
                                kind = (
                                    _terminal_failure_kind(prior_item)
                                    or FailureKind.NOT_FOUND
                                )
                                _mark(
                                    entry.pid,
                                    entry.wu,
                                    CrawlItemStatus.FAILED,
                                    error_kind=kind.value,
                                    error=(
                                        prior_item.error
                                        if prior_item is not None and prior_item.error
                                        else f"{kind.value}: терминально, без повтора"
                                    ),
                                    depth=entry.depth,
                                )
                                _emit_progress()
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
                            if desc.resume is not None or desc.reject is not None:
                                # Resume/reject: живого фетча нет — коммит локально без submit.
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
                                # Наблюдаемость (тот же дефект, что и на локальном пути):
                                # шов уже несёт ``http_status`` отказа — не теряем его.
                                # Класс отказа не меняется (``source_unavailable``).
                                _mark(
                                    entry.pid,
                                    entry.wu,
                                    CrawlItemStatus.FAILED,
                                    error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                                    error=reason,
                                    depth=entry.depth,
                                    http_status=result.http_status,
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
                                # 09 §6.3: валидаторы ответа и признак 304 переносятся в
                                # ``RawPage`` (раньше молча терялись — реальный дефект):
                                # без них ``url_state`` не сохранит ETag/Last-Modified, а
                                # ``304`` не распознается как переиспользование.
                                etag=result.etag,
                                last_modified=result.last_modified,
                                not_modified=result.not_modified,
                            )
                            outcome = _commit_local(desc, page=page)
                            _commit_or_defer(entry.pid, entry.wu, entry.depth, outcome)
                        # M2: предохранитель — BFS-guard, поэтому чисто
                        # ревалидационная волна его не расходует.
                        if wave_has_discovery:
                            waves += 1
                        continue
                    if pool is None:
                        # --- Последовательный путь (max_workers None/1): как прежде. --- #
                        node = frontier.next()
                        if node is None:
                            break
                        work_url, depth = node
                        # 09 §6.3: бюджет discovery исчерпан, а из очереди пришёл не
                        # ревалидационный лист — останавливаем обход (ревалидация
                        # обрабатывается отдельно и бюджет не расходует).
                        if not _has_fetch_capacity() and not frontier.is_revalidation(
                            work_url
                        ):
                            frontier.mark_truncated("max_documents")
                            break
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
                            and _terminal_failure_kind(prior_item) is None
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
                            max_response_bytes=limits.max_response_bytes,
                            artifact_max_bytes=limits.max_artifact_bytes,
                            attachment_formats=limits.attachment_formats,
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
                    # M2: та же формула, что в executor-ветке: непрерывный
                    # revalidation-блок + discovery-room, без роста batch.
                    reval_n = min(
                        frontier.revalidation_head_count(max_workers),
                        int(max_workers) // 2,
                    )
                    room = (
                        int(max_workers)
                        if remaining is None
                        else min(int(max_workers), max(0, remaining))
                    )
                    wave_size = min(
                        len(frontier),
                        int(max_workers),
                        max(reval_n, reval_n + room),
                    )
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
                            and _terminal_failure_kind(prior_item) is None
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
                            max_response_bytes=limits.max_response_bytes,
                            artifact_max_bytes=limits.max_artifact_bytes,
                            attachment_formats=limits.attachment_formats,
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
                # P4/C1: хвост корня доводится здесь, только если он **может** расширить
                # frontier (BFS-ссылки). Иначе он остаётся в фазовой очереди: пул общий,
                # и фетч следующего корня не ждёт экстракции (провенанс корня посчитает
                # финализатор — после доводки хвоста).
                if root_drain.pending and (
                    not defer_tail or _tail_may_extend_frontier()
                ):
                    while root_drain.pending:
                        _finish_extract(extract_pending.popleft())
            finally:
                # C1: пул экстракции — фазовый (``shutdown`` в ``finally`` фазы), здесь
                # закрывается только локальный пул фетча этого корня.
                if pool is not None:
                    pool.shutdown(wait=True)
            return frontier, list(sitemap.skipped), root_drain

        # P3-A2: **однопутевой** product-цикл. Развёртка каталогов/sitemap делается один раз
        # до drain (общий блок построения корней); транспорт выбирается по ``executor is None``.
        # Порядок expand → seed → drain на корень совпадает с локальным и является порядком
        # ingest (interleaving-трюк ``work_by_product`` больше не нужен).
        #: Корни, реально дренированные в этом продукте (для leftover-групп каталогов).
        drained_roots: set[str] = set()
        for url in url_map.get(product_id, []):
            prior_item = items.get((product_id, url))

            # Phase 4: потолок попыток (``--max-attempts``). URL, уже исчерпавший попытки,
            # не фетчится/не разворачивается: терминальный ``FAILED`` с явным ``error_kind``
            # (не тихий дроп) → сеанс финализируется ``PARTIAL``.
            if (
                max_attempts is not None
                and prior_item is not None
                and prior_item.attempts >= max_attempts
                and _terminal_failure_kind(prior_item) is None
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
                # D3: seed оператора; его per-site sitemap разворачивается в **подчинённые**
                # depth-0 seeds того же корня уже внутри ``_drain_root`` — там есть frontier,
                # чей scope фильтрует ``<loc>`` (дефект B/I17). Без sitemap — путь не
                # исполняется (поведение неизменно).
                roots: list[tuple[str, Any, bool]] = [(url, target, True)]
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
                    # Запись каталога — самостоятельный корень без sitemap-аугментации
                    # (развёртка sitemap — свойство seed оператора, не записей каталога).
                    roots.append((entry_url, entry_target, False))

            # D1: BFS-frontier по каждому seed-root (``max_depth=0`` → только seeds,
            # поведение неизменно). Провенанс усечения копится для сеанса/``IndexBuild``.
            # 09 §6.3: известные URL этого корня — в его же дренаж (после seed, до BFS),
            # а не отдельными frontier'ами: одна запись усечения на корень, один бюджет.
            for root_url, root_target, sitemap_seeds in roots:
                drained_roots.add(root_url)
                known_urls = revalidation_groups.get(root_url, ())
                frontier, sitemap_skips, root_drain = _drain_root(
                    product_id,
                    root_url,
                    root_target,
                    sitemap_seeds=sitemap_seeds,
                    revalidate=known_urls,
                )

                def _finalize_root(
                    _pid: str = product_id,
                    _root_url: str = root_url,
                    _frontier: Frontier = frontier,
                    _sitemap_skips: list[dict[str, Any]] = sitemap_skips,
                    _known_urls: Sequence[str] = known_urls,
                ) -> None:
                    """Провенанс корня после доводки его экстракционного хвоста (C1).

                    Счётчики frontier (``fetched``/``revalidation_fetched``/усечение)
                    меняются доводкой отложенных заданий, поэтому запись откладывается
                    до момента, когда хвост корня доведён (:func:`_finalize_ready_roots`).
                    Порядок записей — порядок корней (FIFO финализаторов).
                    """
                    #: Наблюдаемость в любом режиме: ключ есть ровно у корня, который имел
                    #: известные URL (даже если cap отложил все — ``selected=0``). У корня
                    #: без известных URL ключа нет: ``state is None`` остаётся байт-в-байт.
                    revalidation_info: dict[str, Any] | None = None
                    if all_revalidation_groups.get(_root_url):
                        revalidation_info = {
                            "selected": sum(
                                1
                                for url in _known_urls
                                if _frontier.is_revalidation(url)
                            ),
                            # S1: фактическая ревалидация, не «выбрано»; discovery-счётчик
                            # ``pages_fetched`` не смешивается с этим полем.
                            "fetched": _frontier.revalidation_fetched,
                        }
                        if revalidation_cap_fired:
                            revalidation_info["deferred"] = deferred_by_root.get(
                                _root_url, 0
                            )
                        if skipped_by_root.get(_root_url):
                            revalidation_info["skipped"] = skipped_by_root[_root_url]
                    _record_root_outcome(
                        _pid,
                        _root_url,
                        _frontier,
                        _sitemap_skips,
                        revalidation=revalidation_info,
                    )

                if root_drain.pending:
                    # C1: хвост отложен в фазовую очередь — провенанс посчитается, когда
                    # он будет доведён (фетч следующего корня уже не ждёт экстракции).
                    root_drain.finalize = _finalize_root
                    extract_finalizers.append(root_drain)
                else:
                    _finalize_root()

        # Каталог (feed/channel) разворачивается в **другие** корни, поэтому группа,
        # привязанная к авторскому URL каталога, может не совпасть ни с одним drain'ом.
        # Не теряем ревалидацию: синтетический бакет без seed (без записи усечения).
        for group_root, known_urls in revalidation_groups.items():
            pre_cap_urls = all_revalidation_groups.get(group_root) or ()
            if group_root in drained_roots or not pre_cap_urls:
                continue
            frontier: Frontier | None = None
            if known_urls:
                # C1: synthetic-бакет — вызывающий читает счётчики frontier сразу,
                # поэтому хвост доводится здесь же (``defer_tail=False``).
                frontier, _skips, _drain = _drain_root(
                    product_id,
                    group_root,
                    None,
                    sitemap_seeds=False,
                    revalidate=known_urls,
                    seed=False,
                    defer_tail=False,
                )
            # S2: leftover-группа каталога — тоже видимый бакет, а не потерянный frontier.
            info: dict[str, Any] = {
                "selected": (
                    sum(1 for url in known_urls if frontier.is_revalidation(url))
                    if frontier is not None
                    else 0
                ),
                "fetched": frontier.revalidation_fetched if frontier is not None else 0,
                "via": "catalog",
            }
            if revalidation_cap_fired:
                info["deferred"] = deferred_by_root.get(group_root, 0)
            if skipped_by_root.get(group_root):
                info["skipped"] = skipped_by_root[group_root]
            if frontier is not None and frontier.skipped_blacklisted:
                info["skipped_blacklisted"] = list(frontier.skipped_blacklisted)
            revalidation_orphans[f"{product_id}:catalog:{group_root}"] = info
            if frontier is not None:
                revalidation_fetched_total += frontier.revalidation_fetched

        # 09 §6.3: известные URL вне scope **всех** авторских корней (author-scope
        # drift: корень удалён/ключ пути сужен). Один synthetic-бакет на (продукт,
        # origin): depth-0 листья, только ревалидация. В ``truncation`` **не** пишется
        # (иначе synthetic-причина демотировала бы ``❌`` без вины корня); наблюдение —
        # ``IndexBuild.revalidation_orphans`` + заметка фазы. I12: статус не меняется.
        for origin, all_known_urls in all_orphan_buckets.items():
            selected_urls = orphan_buckets.get(origin, ())
            frontier: Frontier | None = None
            if selected_urls:
                # C1: synthetic-бакет — хвост доводится здесь же (``defer_tail=False``).
                frontier, _skips, _drain = _drain_root(
                    product_id,
                    origin,
                    None,
                    sitemap_seeds=False,
                    revalidate=selected_urls,
                    seed=False,
                    defer_tail=False,
                )
            info: dict[str, Any] = {
                "selected": (
                    sum(1 for url in selected_urls if frontier.is_revalidation(url))
                    if frontier is not None
                    else 0
                ),
                "fetched": frontier.revalidation_fetched if frontier is not None else 0,
            }
            if revalidation_cap_fired:
                # Отложенные orphan-URL — не «потеряны»: их число видно рядом с selected.
                info["deferred"] = deferred_by_origin.get(origin, 0)
            if skipped_by_origin.get(origin):
                info["skipped"] = skipped_by_origin[origin]
            if frontier is not None and frontier.skipped_blacklisted:
                info["skipped_blacklisted"] = list(frontier.skipped_blacklisted)
            revalidation_orphans[f"{product_id}:{origin}"] = info
            revalidation_orphan_total += len(all_known_urls)
            if frontier is not None:
                revalidation_fetched_total += frontier.revalidation_fetched
            # Targets известных URL резолвятся лениво в ``_drain_root``
            # (``_resolve_target``) — предварительная резолюция не нужна.

        # C1: хвосты экстракции продукта обязаны быть доведены до расчёта провенанса
        # (``truncation_records``/``pages_fetched_total``/финализаторы корней). Пул —
        # фазовый, поэтому здесь же доводятся хвосты всех корней продукта (FIFO).
        while extract_pending:
            _finish_extract(extract_pending.popleft())
        _finalize_ready_roots()

        # Ops-потолок ревалидации — **продуктовое** усечение через существующий канал
        # (``truncated`` + ``truncated_reason``), без новой coverage/freshness-оси.
        # Precedence: crawl-потолки (``max_documents``/``max_depth``/…) не перекрываются —
        # причина ревалидации ставится только если ни одна запись продукта уже не
        # усечена. Сама причина живёт в первой записи корня (product-level факт), а
        # аддитивный ``revalidation``-счётчик остаётся на каждом корне.
        if revalidation_deferred_total > 0:
            truncated_products.add(product_id)
            product_records = [
                record
                for record in truncation_records
                if record.get("product_id") == product_id
            ]
            already_crawl_truncated = any(
                record.get("truncated")
                and record.get("truncated_reason") != "revalidate_max_documents"
                for record in product_records
            )
            if not already_crawl_truncated:
                if product_records:
                    target = next(
                        (
                            record
                            for record in product_records
                            if (record.get("revalidation") or {}).get("deferred")
                        ),
                        product_records[0],
                    )
                    target["truncated"] = True
                    target["truncated_reason"] = "revalidate_max_documents"
                else:
                    # Нет ни одного авторского корня (только orphan-бакеты) — иначе
                    # продуктовый факт потерял бы канал причины в ``truncation_summary``.
                    truncation_records.append(
                        {
                            "product_id": product_id,
                            "root": None,
                            "pages_fetched": 0,
                            "truncated": True,
                            "truncated_reason": "revalidate_max_documents",
                            "limits": {},
                            "skipped_blacklisted": [],
                            "skipped_documents": [],
                        }
                    )
        # Retention-провенанс не зависит от индексной сборки и уже персистенчен
        # инкрементально: единственная точка ``ingest.docs.append`` — ``_finish_one``,
        # где ``_persist_docs`` вызывается сразу после коммита документа. Финального
        # «дампа» продукта здесь нет намеренно: он лишь повторно писал бы те же строки
        # (``INSERT OR REPLACE`` по PK) и удваивал бы транзакции без выигрыша в durability.
        ingests.append(ingest)

    _emit_progress()

    # --- Планирование партиций и композиции (09 §8.1) ------------------------- #
    # extractor_version: per-product композит из фактически извлечённых документов;
    # fallback — объявленная версия экстрактора-порта (если ExtractedDoc не нёс версии).
    declared_extractor_version = str(getattr(extractor, "version", "") or "")
    #: ``product_id -> {partition_build_id, content_digest, extractor_version}``.
    product_plans: dict[str, dict[str, str]] = {}
    members: dict[str, str] = {}
    digests: dict[str, str] = {}
    for ingest in ingests:
        if not ingest.chunks:
            continue
        product_token = _product_content_token(ingest.chunks)
        extractor_token = (
            _product_extractor_token(ingest.docs) or declared_extractor_version
        )
        partition_build_id = _derive_partition_build_id(
            package_id=package,
            product_id=ingest.product_id,
            chunker_version=resolved_chunker_version,
            embed_model_id=embed_model_id,
            extractor_version=extractor_token,
            lexical_engine_version=lexical_engine_version,
            vector_store=vector_store,
            product_content_token=product_token,
        )
        product_plans[ingest.product_id] = {
            "partition_build_id": partition_build_id,
            "content_digest": product_token,
            "extractor_version": extractor_token,
        }
        members[ingest.product_id] = partition_build_id
        digests[ingest.product_id] = product_token

    # ``composition_id`` — единственный ``runs.index_build_id`` (09 §8.1). Он зависит
    # только от состава когорты и версий/дайджестов партиций; ``params_token``/
    # ``snapshot_date``/``retrieval_*`` не входят.
    index_build_id = derive_composition_id(package, members, digests)

    # --- Pass B (index): partition_id → manifest → build/reuse → persist ------- #
    all_docs: list[CorpusDoc] = []
    entries: list[IndexManifestEntry] = []
    chunk_count = 0
    partitions_reused = 0
    all_physical_ok = True

    for ingest in ingests:
        product_id = ingest.product_id
        product_chunks = ingest.chunks
        entry: IndexManifestEntry | None = None
        physical_ok = True

        if product_chunks:
            chunk_count += len(product_chunks)
            plan = product_plans[product_id]
            partition_build_id = plan["partition_build_id"]
            product_token = plan["content_digest"]
            extractor_token = plan["extractor_version"]
            # build_id и extractor_version чанка — по партиции (Pass B). ``build_id``
            # чанка = ``pb``: ``iter_chunks(pb, product_id=…)`` остаётся рабочим, а I17
            # держится структурно (``pb`` хеширует product_id + его content token).
            doc_versions = {doc.doc_id: doc.extractor_version for doc in ingest.docs}
            for chunk in product_chunks:
                chunk.build_id = partition_build_id
                chunk.extractor_version = doc_versions.get(chunk.doc_id)

            entry = IndexManifestEntry(
                # ``index_build_id`` — композиция (``runs.index_build_id``);
                # ``partition_build_id`` — физическая продукт-партиция (09 §8.1).
                index_build_id=index_build_id,
                partition_build_id=partition_build_id,
                product_id=product_id,
                package_id=package,
                partition_key=f"{index_build_id}:{product_id}",
                snapshot_date=snapshot,
                chunker_version=resolved_chunker_version or None,
                extractor_version=extractor_token or None,
                embed_model_id=embed_model_id or None,
                embed_dim=getattr(embedder, "dim", None),
                lexical_engine_version=lexical_engine_version or None,
                vector_store=vector_store or None,
                content_digest=product_token,
                # Информационно; builder сам собирает путь из id, который ему передали.
                store_path=f"index_store/{package}/{partition_build_id}/{product_id}/",
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

            # 09 §8.5: переиспользование партиции — реальный CPU-win (ноль эмбеддингов).
            # Гейт: id-уровень (``pb``) + физическая проверка диска/манифеста.
            if embedder is not None and index_builder is not None:
                reuse_reason: str | None = "index_root не задан"
                if index_root is not None:
                    reuse_reason = _partition_reuse_reason(
                        index_root=index_root,
                        package_id=package,
                        partition_build_id=partition_build_id,
                        product_id=product_id,
                        product_content_token=product_token,
                        chunker_version=resolved_chunker_version,
                        embed_model_id=embed_model_id,
                        extractor_version=extractor_token,
                        lexical_engine_version=lexical_engine_version,
                        vector_store=vector_store,
                    )
                if reuse_reason is None:
                    partitions_reused += 1
                else:
                    # «Нет каталога» — нормальный первый прогон, не ошибка. Существующий,
                    # но непригодный каталог — видимая причина пересборки (не тихо).
                    partition_path = (
                        _partition_root(
                            index_root, package, partition_build_id, product_id
                        )
                        if index_root is not None
                        else None
                    )
                    if partition_path is not None and partition_path.exists():
                        errors.append(
                            f"{product_id}: партиция {partition_build_id} не "
                            f"переиспользована ({reuse_reason}) — пересборка"
                        )
                    physical_ok = _embed_and_build(
                        embedder,
                        index_builder,
                        product_id,
                        product_chunks,
                        package_id=package,
                        chunker_version=resolved_chunker_version,
                        embed_model_id=embed_model_id,
                        extractor_version=extractor_token,
                        lexical_engine_version=lexical_engine_version,
                        vector_store=vector_store,
                        product_content_token=product_token,
                        snapshot_date=snapshot,
                        partition_build_id=partition_build_id,
                        errors=errors,
                    )

        all_docs.extend(ingest.docs)
        # «Disk first, DB second»: физическая сборка авторитетна. Пока build не завершился
        # успешно, corpus_chunks/index_manifest не пишутся (K2/§13); entry остаётся в entries
        # для провенанса/log (IndexBuild).
        if physical_ok:
            persisted_ok = _persist_chunks_and_manifest(
                store, product_chunks, entry, errors
            )
            if not persisted_ok:
                all_physical_ok = False
        else:
            all_physical_ok = False
            errors.append(
                f"{product_id}: физическая сборка индекса не зафиксирована — "
                "corpus_chunks/index_manifest не записаны (K2/§13); "
                "изменение корпуса требует нового run_id/раунда"
            )

    if entries and embedder is not None and index_builder is not None:
        commit_ok = True
        try:
            index_builder.commit()
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            commit_ok = False
            errors.append(f"index commit: {type(exc).__name__}: {exc}")

        # 09 §8.2/§8.6: композиция пишется **после** фиксации всех партиций и их
        # DB-строк. Частичный сбой члена/коммита ⇒ композиции нет (иначе она ссылалась
        # бы на несуществующую партицию). Иммутабельность: другой состав при том же id —
        # ``ImmutableCompositionError`` в ``errors``, не падение фазы.
        if index_root is not None and all_physical_ok and commit_ok:
            created_at = str(snapshot) if snapshot else _now_iso()
            try:
                write_composition(
                    index_root,
                    package,
                    CompositionView(
                        composition_id=index_build_id, members=members
                    ),
                    created_at=created_at,
                    digests=digests,
                )
            except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                errors.append(
                    f"composition({index_build_id}): {type(exc).__name__}: {exc}"
                )
            else:
                if state is not None:
                    _register_composition_state(
                        state,
                        package_id=package,
                        composition_id=index_build_id,
                        entries=entries,
                        created_at=created_at,
                        run_id=run_id,
                        errors=errors,
                    )

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
                revalidation_fetched=revalidation_fetched_total,
                truncated_products=sorted(truncated_products),
            )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(f"finalize_crawl_session({session}): {type(exc).__name__}: {exc}")

        # Ф0: покрытие корпуса продукта — первоклассный факт для гейта ❌ (I2c).
        # Производно от **персистентных** элементов сеанса и записей об усечении; пишется
        # после финализации, чтобы провенанс/усечение уже были зафиксированы.
        try:
            _persist_corpus_coverage(
                store,
                run_id,
                session,
                truncated_products,
                _now_iso(),
                products=products,
            )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(
                f"corpus_coverage({session}): {type(exc).__name__}: {exc}"
            )

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
        revalidation_orphans=dict(revalidation_orphans),
        doc_counters=dict(doc_counters),
        duplicates=dict(duplicates),
        duplicate_count=sum(duplicates.values()),
        partitions_reused=partitions_reused,
    )
    # Примечание: индексная сборка «возвращается» в log_entries (PhaseResult — контракт фазы;
    # W8 читает IndexBuild отсюда). Счётчики — в полях IndexBuild (``doc_counters``,
    # ``duplicates``/``duplicate_count``); операторская строка о дедупе — note фазы
    # (``phases._duplicate_notes`` → ``results[].notes``), не ``errors``: схлопывание
    # одинакового контента легитимно.
    log_entries: list[Any] = [build]
    if revalidation_orphan_total:
        # Наблюдение, не ошибка и не смена статуса (I12): авторский scope разошёлся с
        # тем, что реально накопилось в ``url_state``.
        log_entries.append(
            f"corpus: {revalidation_orphan_total} known URLs are outside every authored "
            "root — author-scope drift (revalidated as orphans; observation only)"
        )
    return PhaseResult(
        status="ok", phase="corpus", cells=[], log_entries=log_entries, errors=errors
    )


def build_corpus(*args: Any, **kwargs: Any) -> PhaseResult:
    """Ш5 (публичная точка фазы): ``_build_corpus`` + жизненный цикл фазовых пулов.

    C1/L1: пул экстракции — **один на прогон** (ключ ``(DocumentLevel, window)`` в
    :class:`_ExtractPools`), поэтому ``shutdown(wait=True, cancel_futures=True)``
    выполняется ровно один раз — здесь, в ``finally`` фазы, а не на каждый seed-root.
    Это единственное отличие обёртки от :func:`_build_corpus`; аргументы и поведение
    фазы — прежние (см. docstring :func:`_build_corpus`).
    """
    pools = _ExtractPools(max_rebuilds=_EXTRACT_MAX_POOL_REBUILDS)
    try:
        return _build_corpus(*args, pools=pools, **kwargs)
    finally:
        pools.shutdown()

