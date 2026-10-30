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

**Порядок корней и scope — операторские, не лексикографические (C, ``execution_change``
§13).** Корни продукта дренируются в **авторском** порядке (``corpus_urls.yaml``/
``seeds.yaml``, дедуп — первое вхождение), а не в ``sorted()``: живой прогон показал цену
лексикографического порядка — корень корпоративного портала (800 страниц, ~2 часа)
отсортировался перед корнем документации продукта, съел бюджет продукта и не дал
docs-корню (высшая ценность для карты возможностей) ни одного своего элемента; прогон
умер, docs так и не начался. Правило: **корень, который идёт последним, — корень, который
может не выполниться**. Scope корня — объявленные хосты: необъявленный поддомен хоста
корня вне scope (``Frontier.host_in_scope``), а отвергнутые поддомены и принятые «чужие»
хосты считаются в провенансе корня
(``truncation_records[*].out_of_scope_subdomains``/``accepted_foreign_hosts``). Меняются
условия измерения (``crawl_config_digest``/``seeds_digest``/``run_id``), но не правила
методологии: ``METHODOLOGY_VERSION`` не меняется.

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
``ProductIngest`` в памяти **только на время своего продукта**; ``corpus_docs`` персистятся
**инкрементально** — сразу при
коммите каждого документа (retention-провенанс не зависит от сборки), поэтому жёсткое
убийство теряет максимум документ «в полёте», а не весь продукт; ``build_id`` чанков пока
``None``. Pass B (index) идёт **на границе каждого продукта**, сразу после его Pass A:
выводятся ``partition_build_id`` (входы — только чанки/документы этого продукта, ``pb``
продукт-скоупед, I17), ``build_id = pb`` проставляется чанкам, партиция либо
переиспользуется (ноль эмбеддингов, 09 §8.5), либо собирается ``_embed_and_build``; при
успехе **сразу** персистятся ``corpus_chunks`` («сначала диск, потом БД»). Иначе смерть на
N-м продукте уничтожала бы посчитанную, но не персистентную работу предыдущих. Там же, на
границе продукта, ``ProductIngest`` **освобождает** ``docs``/``chunks``: после персистенции
единственный носитель продукта — БД, а в памяти остаётся сводка (``chunk_count``/
``distinct_docs``), которой достаточно пост-циклу. Поэтому пик памяти фазы определяется
**одним** продуктом, а не их суммой (Д-2). Только
после **всех** продуктов (ибо он зависит от состава когорты) выводится ``composition_id``
(``runs.index_build_id``), пишутся строки ``index_manifest`` (их PK включает композицию) и
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
иммутабельной партицией на диске). Запись разделена по времени: ``corpus_chunks`` — сразу
на границе продукта (после успешного ``build``/переиспользования его партиции),
``index_manifest`` — после всех продуктов (PK строки включает ``composition_id``), и только
для партиций, физически зафиксированных. Если ``build`` отказал (в т.ч.
``ImmutableBuildError`` при попытке изменить уже зафиксированную сборку, K2/§13),
``corpus_docs`` уже персистенчены инкрементально (retention), но ``corpus_chunks``/
``index_manifest`` по этому продукту остаются неизменными, а
отказ попадает в ``errors`` с указанием, что изменение корпуса требует нового
``run_id``/раунда. Это зеркалит порядок в ``serp_loop`` (build → ``save_index_manifest``).

**Внедрение зависимостей.** Адаптеры (``core/adapters/*``) здесь не импортируются: фаза
работает только через порты (A1). ``embedder``/``index_builder`` опциональны — если они не
переданы, фаза строит корпус и ``index_manifest``, а эмбеддинг/физическую сборку индекса
подключает W8 (composition root). При их наличии фаза вызывает ``embed``/``build``/``commit``
best-effort (сбой изолируется).

**Кэш векторов (опция).** ``embed_cache`` — межпрогонный кэш **почанковых** векторов
(:mod:`core.pipeline.embed_cache`), ключ — ``(content_hash текста чанка, embed_model_id)``.
Он существует потому, что переиспользование индекса возможно только целой продукт-партицией:
смена ``extractor_version``/``chunker_version`` или добавление документа даёт новый ``pb`` и
пересчитывает эмбеддинг всего продукта, хотя текст большинства чанков не изменился. Кэш не
входит ни в ``pb``, ни в манифест партиции: он меняет **стоимость** сборки, а не условия
измерения (A3). Наблюдаемость — ``embed_cached``/``embed_hit_ratio_pct`` в прогрессе и
«из кэша N (X%)» в итоговой строке продукта: без числа «сколько сэкономил кэш» фича
недоказуема. ``embed_cache is None`` — прежнее поведение байт-в-байт.

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

import contextlib
import datetime as _dt
import json
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.config.crawl_sources import CrawlSourcesConfig
from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain import (
    ArchiveDescentStatus,
    ArchiveObservation,
    CorpusChunk,
    CorpusCoverage,
    CorpusDoc,
    CorpusMember,
    CrawlItemStatus,
    CrawlRunConfig,
    CrawlSessionItem,
    CrawlSessionStatus,
    CrawlStrategy,
    DocumentLevel,
    EmbedDevice,
    FailureKind,
    IndexManifestEntry,
    PdfPipeline,
    RefreshPolicy,
    ReuseKind,
    RunProductStatus,
    RunScope,
)
from core.pipeline import corpus_commit, corpus_ingest
from core.pipeline.archive_descent import (
    SKIP_EXTRACT_FAILED,
    SKIP_EXTRACT_UNAVAILABLE,
    ArchiveDescentWalker,
    DescentBudget,
    MemberDeferral,
    MemberOutcome,
    MemberTask,
)
from core.pipeline.artifact_probe import probe_artifact
from core.pipeline.artifacts_gap import write_artifacts_gap
from core.pipeline.composition import (
    CompositionView,
    derive_composition_id,
    write_composition,
)

# L5 (шаг 1): durable-субстрат тела ``_build_corpus`` вынесен в свой модуль. Имена здесь
# **публичные** — это межмодульный контракт, а не приватность, пробитая наружу: приватное
# имя через границу модуля было дефектом в L1 (``wiring.py``). Модуль — лист (он не
# импортирует ``corpus``), поэтому обратного импорта нет.
# L5 (шаг 2): допуск/разбор одного URL (``corpus_ingest``) и фиксация исхода корня
# (``corpus_commit``) вынесены в свои модули. Имена через границу — **публичные**: приватное
# имя через границу модуля — дефект (L1, ``wiring.py``). Оба модуля — листья (``corpus`` они
# не импортируют), поэтому цикла нет: ``corpus_commit`` импортирует ``corpus_ingest``, но не
# наоборот.
from core.pipeline.corpus_commit import PendingExtract, PhaseLifecycle, RootDrain

# L2: расчёт/персистенция покрытия и статуса продукта вынесены в свой модуль, но
# **реэкспортируются здесь**. Причина та же, что у L1: тесты и ``selfcheck`` адресуют эти
# имена по ``core.pipeline.corpus``, а существующие тесты в этой волне не редактируются
# (§0.3 плана декомпозиции). ``noqa: F401`` — осознанно: имена обязаны резолвиться здесь.
from core.pipeline.corpus_coverage import (  # noqa: F401
    ZERO_SOURCE_ALL_SEEDS_REJECTED,
    ZERO_SOURCE_NO_SEEDS,
    ZERO_SOURCE_NO_URLS_ENTRY,
    _final_product_status,
    _persist_corpus_coverage,
    _product_notes,
    derive_corpus_coverage,
)

# L3: идентичность сборки и эмбеддинг продукт-партиции вынесены в свой модуль и
# **реэкспортируются здесь** — по той же причине (§0.3): тесты адресуют эти имена по
# ``core.pipeline.corpus``, а существующие тесты в этой волне не редактируются.
from core.pipeline.corpus_index import (  # noqa: F401
    _EMBED_RATE_ALPHA,
    _FIELD_SEP,
    _PARTITION_SCHEMA,
    EMBED_BATCH_CHUNKS,
    _chunk_id,
    _CorpusStageTimings,
    _derive_partition_build_id,
    _embed_and_build,
    _embed_batched,
    _EmbedCensus,
    _EmbedThroughput,
    _make_embed_reporter,
    _partition_reuse_reason,
    _product_content_token,
    _product_extractor_token,
    format_embed_progress,
)

#: Реэкспорт для существующих тестов (§0.3 плана): ``test_corpus.py`` берёт эти два имени
#: по ``core.pipeline.corpus``. Приватность сохранена намеренно — это те же приватные имена
#: модуля-владельца, а не новый контракт; ``noqa: F401`` — имена обязаны резолвиться здесь.
from core.pipeline.corpus_ingest import (  # noqa: F401
    _SUBMIT_MAX_ATTEMPTS,
    DIRECTORY_KINDS,
    MEMBER_CONTENT_TYPES,
    LocalFetch,
    ProductIngest,
    RetainedRaw,
    RetainedReject,
    SitemapSeeds,
    _work_set_idempotency_key,
    artifact_is_terminal,
    looks_like_html,
    resume_retained,
    terminal_failure_kind,
)
from core.pipeline.corpus_persist import (
    DocReuse,
    count_doc,
    emit_product,
    emit_progress,
    find_doc_version,
    persist_manual_audit,
    save_run_product,
    write_document_state,
)

# L5 (шаг 1): мутабельное состояние фазы, которое раньше текло через ``nonlocal``.
# Отдельный модуль — чтобы следующие шаги (``corpus_ingest``/``corpus_commit``) могли
# аннотировать им свои функции, не создавая цикла с ``corpus``.
from core.pipeline.corpus_state import CorpusCounters
from core.pipeline.crawl_frontier import (
    SCOPE_OUT,
    CrawlLimits,
    Frontier,
    normalize_scope_host,
    resolve_crawl_limits,
)
from core.pipeline.crawl_session import (
    default_session_id,
    open_session,
    prior_items,
    read_provenance,
)
from core.pipeline.crawl_sources import (
    CrawlTarget,
    resolve_crawl_target,
)
from core.pipeline.embed_cache import EmbedCache
from core.pipeline.extract_cache import ExtractionCache

# L1: runtime экстракции вынесен в свой модуль, но **реэкспортируется здесь**. Причина —
# не удобство: 46 операторов импорта в 29 тестовых файлах адресуют эти имена по
# ``core.pipeline.corpus``, а существующие тесты в этой волне не редактируются (§0.3
# плана декомпозиции). Импорт помечен ``noqa: F401`` осознанно: имена, которые внутри
# этого модуля больше не вызываются, всё равно обязаны здесь резолвиться — иначе тест,
# адресующий ``core.pipeline.corpus._extract_worker``, перестал бы импортироваться.
from core.pipeline.extraction_runtime import (  # noqa: F401
    _EXTRACT_DEAD_POOL_TIMEOUTS,
    _EXTRACT_MAX_POOL_REBUILDS,
    _RESUME_READ_CHUNK,
    ExtractionPool,
    _as_text,
    _cache_extractor_version,
    _extract_cache_key,
    _extract_worker,
    _ExtractPoolKey,
    _ExtractPools,
    _ExtractPoolSlot,
    _ExtractRequest,
    _hash_file,
    _page_with_source_path,
    _run_extract,
    _trim_worker_arenas,
    _worker_extractor_for,
    _worker_peak_rss_mb,
    _write_worker_peak_rss,
    set_worker_extractor,
)
from core.pipeline.incremental import (
    doc_version_id,
)
from core.pipeline.index_build import IndexBuild
from core.pipeline.liveness import Heartbeat
from core.pipeline.manual_artifacts import (
    ORIGIN_CRAWL,
    ManualArtifacts,
)
from core.pipeline.resources import ResourcesObservation, summarize_chunks_per_doc
from core.pipeline.runner import PhaseResult
from core.ports.chunker import ChunkerPort
from core.ports.container import ContainerPort
from core.ports.crawl_executor import (
    CrawlExecutorPort,
    CrawlWorkItem,
)
from core.ports.crawler import CrawlerPort
from core.ports.embedder import EmbedderPort
from core.ports.extractor import ExtractorPort
from core.ports.index_builder import IndexBuilderPort
from core.ports.poller import PollPort
from core.ports.state import (
    IndexCompositionRow,
    IndexPartitionRow,
    ProductStatePort,
)
from core.ports.types import (
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    RawPage,
)
from core.util.document_formats import (
    DOCUMENT_SUFFIXES,
    document_content_type,
    document_suffix,
)
from core.util.hashing import content_hash
from core.util.html_links import extract_links
from core.util.index_partition import build_root as _partition_root
from core.util.rate_limit import RateLimiter

#: Алиас для ``selfcheck``: он адресует набор директорных kind-ов по ``corpus`` (§0.3
#: плана декомпозиции); сам набор живёт в ``corpus_ingest``.
_DIRECTORY_KINDS = DIRECTORY_KINDS

__all__ = ["ExtractionPool", "IndexBuild", "build_corpus", "derive_corpus_coverage"]


#: Phase 3 B2: максимум попыток подачи работ исполнителю при back-pressure
#: (``accepted=False``). После исчерпания — изолированный ``FAILED`` (``source_unavailable``),
#: а не тихий провал всего прогона.

#: Phase 4/Ф0: ``error_kind`` терминального ``FAILED`` при исчерпании ``--max-attempts``.
#: Канонический класс сбоя (``FailureKind.ATTEMPTS_EXHAUSTED``): отличает «попытки
#: кончились» от сетевого/браузерного сбоя, не выходя за словарь §4.1.
_MAX_ATTEMPTS_ERROR_KIND = FailureKind.ATTEMPTS_EXHAUSTED.value

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

#: P4: сентинел «экстракция не была отложена» для :func:`build_corpus`-интернала
#: ``_commit_outcome`` (inline-путь вызывает ``_ingest_one``; пул передаёт готовый результат).
_INLINE = object()


def _as_refresh_policy(value: str) -> RefreshPolicy:
    """Приводит строку политики наблюдения к :class:`RefreshPolicy` (дефолт ``auto``)."""
    try:
        return RefreshPolicy(value)
    except ValueError:
        return RefreshPolicy.AUTO


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
    desc: LocalFetch | None = None
    future: Any = None
    prior_item: CrawlSessionItem | None = None


# --------------------------------------------------------------------------- #
# Детерминированные выводы (версии/идентификаторы)
# --------------------------------------------------------------------------- #


def _ordered_unique(values: Iterable[Any]) -> list[str]:
    """Дедуп с сохранением **авторского** порядка (первое вхождение побеждает).

    Порядок seed-корней — операторский (``corpus_urls.yaml``/``seeds.yaml``), а не
    лексикографический: корень, который идёт последним, может не выполниться вовсе
    (execution_change §13, см. :func:`_build_corpus`).
    """
    seen: set[str] = set()
    out: list[str] = []
    for value in values or ():
        url = str(value)
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out

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


def _now_iso() -> str:
    """Текущее время в ISO-8601 (UTC) — для провенанса элементов сеанса (L1a)."""
    return _dt.datetime.now(_dt.UTC).isoformat()


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


# --------------------------------------------------------------------------- #
# Персистенция (изолированная: сбой store не роняет фазу)
# --------------------------------------------------------------------------- #


def _persist_docs(
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


def _persist_observation_sync(
    store: Any,
    errors: list[str],
) -> Callable[[ArchiveObservation], None] | None:
    """Write-through наблюдения спуска: синк для ``ArchiveDescentWalker`` (F1).

    Возвращает ``None``, если стора нет (прежнее поведение спуска: только список фазы).
    Наблюдение пишется **сразу** за ``_record``, поэтому SIGKILL/OOM посреди контейнера
    теряет максимум контейнер «в полёте», а не весь спуск: раньше наблюдения копились в
    памяти до конца фазы. Идемпотентность уже есть — ``observation_id`` детерминирован
    (``run|product|root|hash|depth``), повторный спуск перекрывает строку, а не плодит её.

    Отказ стора изолируется в ``errors`` (по образцу :func:`_persist_docs`): падение
    фазы хуже потерянной строки провенанса, но тихая потеря — недопустима.
    """
    if store is None:
        return None

    def _sync(observation: ArchiveObservation) -> None:
        try:
            store.save_archive_observation(observation)
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(
                f"save_archive_observation({observation.observation_id}): "
                f"{type(exc).__name__}: {exc}"
            )

    return _sync


def _persist_member_sync(
    store: Any,
    errors: list[str],
) -> Callable[[CorpusMember], None] | None:
    """Write-through строки ``corpus_members``: синк для ``ArchiveDescentWalker`` (F1).

    Строка пишется **после** доводки члена (``_settle``/``_emit_skipped_member``), то есть
    когда ``doc_id``/``extracted_content_hash`` известны или пропуск назван: раньше строки
    копились до конца фазы, и жёсткое убийство уносило их все. Идентичность —
    ``(archive_content_hash, member_path, product_id)``, поэтому повторный спуск делает
    UPSERT, а не дубль; отсюда же идемпотентность финальной пачки (reconciliation-pass).
    """
    if store is None:
        return None

    def _sync(row: CorpusMember) -> None:
        try:
            store.save_corpus_members([row])
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(
                f"save_corpus_members({row.archive_content_hash}/{row.member_path}): "
                f"{type(exc).__name__}: {exc}"
            )

    return _sync


def _persist_chunks(
    store: Any,
    chunks: list[CorpusChunk],
    errors: list[str],
) -> bool:
    """Пишет ``corpus_chunks`` **одного** продукта (FK ``doc_id`` — документы уже в БД).

    Вызывается **только** когда физическая продукт-партиция этого продукта реально
    зафиксирована на диске («disk first, DB second»): реестр не должен заявлять сборку,
    которой нет (K2/§13). Вызывается **сразу** на границе продукта, а не после всех
    продуктов: иначе смерть на N-м продукте уничтожала бы посчитанные, но не
    персистентные чанки предыдущих.

    Сбой обёрнут в try/except: фиксируется в ``errors`` и не прерывает обработку
    остальных продуктов (контракт изоляции фаз, ``runner.py``).

    :returns: ``True`` — строки записаны (или store отсутствует); ``False`` — запись
        не удалась. Композиция пишется только при ``True`` для **всех** членов: иначе она
        ссылалась бы на партицию, которой нет в реестре прогона.
    """
    if store is None:
        return True
    if not chunks:
        return True
    try:
        store.add_chunks_batch(chunks)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"add_chunks_batch: {type(exc).__name__}: {exc}")
        return False
    return True


def _persist_manifest(
    store: Any,
    entries: Sequence[IndexManifestEntry],
    errors: list[str],
) -> bool:
    """Пишет ``index_manifest`` — строки продуктов, чьи партиции уже на диске.

    Отдельно от :func:`_persist_chunks`, потому что PK ``index_manifest`` включает
    ``index_build_id`` (композицию когорты), а она выводится только после **всех**
    продуктов. Порядок «disk first, DB second» (K2/§13) сохраняется: вызывается после
    того, как чанки продуктов персистентны и их партиции физически зафиксированы;
    вызывающий передаёт **только** те записи, для которых ``build``/переиспользование
    отчитались успехом.

    :returns: ``True`` — строки записаны (или store отсутствует); ``False`` — хотя бы
        одна запись не удалась (композиция тогда не пишется).
    """
    if store is None:
        return True
    ok = True
    for entry in entries:
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


def _stage_timings_aggregate(
    rows: Sequence[tuple[str, float, _CorpusStageTimings]],
) -> dict[str, Any]:
    """Агрегат стадийных бакетов фазы: по продуктам и суммой (складывать руками не нужно).

    :param rows: ``(product_id, elapsed_s продукта целиком, бакеты)`` в порядке продуктов.
    :returns: ``{"products": {...}, "total": {...}}``; ``overhead_s`` — по той же формуле
        ``Σ elapsed − Σ бакетов`` и **без** зажима в ноль (отрицательное значение — дефект,
        который обязан быть виден в машинном отчёте).
    """
    products: dict[str, Any] = {}
    elapsed_total = 0.0
    names = ("fetch_wait_s", "extract_wait_s", "chunk_s", "embed_s", "build_s")
    buckets = dict.fromkeys(names, 0.0)
    workers: int | None = None
    for product_id, elapsed_s, timings in rows:
        elapsed_total += elapsed_s
        for name in names:
            buckets[name] += getattr(timings, name)
        if timings.extract_workers is not None and (
            workers is None or timings.extract_workers > workers
        ):
            workers = timings.extract_workers
        products[product_id] = {
            "elapsed_seconds": round(elapsed_s, 3),
            **{name: round(getattr(timings, name), 3) for name in names},
            "extract_workers": timings.extract_workers,
            "overhead_s": round(elapsed_s - timings.stage_sum(), 3),
        }
    total = {
        "elapsed_seconds": round(elapsed_total, 3),
        **{name: round(buckets[name], 3) for name in names},
        "extract_workers": workers,
        "overhead_s": round(elapsed_total - sum(buckets.values()), 3),
    }
    return {"products": products, "total": total}


# --------------------------------------------------------------------------- #
# Durable-краул (L1a): сеанс краула — аудит/очередь/resume per-URL
# --------------------------------------------------------------------------- #


#: Терминальные классы: повторный фетч/экстракция не дадут нового знания (P0/I4).


# --------------------------------------------------------------------------- #
# Основная фаза
# --------------------------------------------------------------------------- #


#: Маршрут извлечения члена по суффиксу: документы — ``document``, разметка — ``auto``
#: (``auto`` сам роутит HTML/markdown; ``html`` намеренно не форсируется — A1).
_DOCUMENT_SUFFIX_SET: frozenset[str] = frozenset(DOCUMENT_SUFFIXES)

#: ``Content-Type`` члена по суффиксу — для экстрактора и провенанса (данные, не эвристика).


def _member_route(suffix: str) -> ExtractionRoute:
    """Маршрут извлечения члена по **его собственному** суффиксу (не по суффиксу архива)."""
    return (
        ExtractionRoute.DOCUMENT
        if suffix.lower() in _DOCUMENT_SUFFIX_SET
        else ExtractionRoute.AUTO
    )


def _member_extract_request(
    task: MemberTask,
    *,
    fetched_at: str,
    lang: str | None,
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED,
    rss_dir: str | None = None,
) -> _ExtractRequest:
    """B3/P4: payload экстракции члена — только примитивы, байты не передаются (I19).

    Зеркало :func:`_make_extract_request` для страницы: источник байтов — **путь** к
    временному файлу члена (воркер читает его сам), маршрут — по суффиксу члена,
    ``content_type`` — по суффиксу (данные, не эвристика). Межпрогонный кэш экстракции
    членам не передаётся: имена временных файлов (``member-N.bin``) не
    content-addressed, и кэш не должен зависеть от их нумерации; член при этом всё равно
    хешируется воркером из фактически прочитанных байтов.
    """
    suffix = task.suffix.lower()
    return _ExtractRequest(
        raw_path=task.temp_path,
        url=task.member_url,
        fetched_at=fetched_at,
        fetch_mode=FetchMode.HTTP,
        lang=lang,
        status_code=None,
        content_type=MEMBER_CONTENT_TYPES.get(suffix),
        engine=None,
        route=_member_route(suffix),
        level=level,
        pdf_pipeline=pdf_pipeline,
        rss_dir=rss_dir,
    )


def _member_extract_unavailable(extractor: ExtractorPort) -> MemberOutcome | None:
    """``split``: экстрактор шва работает по URL — локальные байты он не принимает.

    Капитуляция явная, а не тихая: член контейнера не извлечён, причина видна в
    ``corpus_members.skip_reason``/``errors``. Проверка живёт **в родителе** (у воркера
    пула нет этого признака — он получает уже готовый payload), поэтому и inline-путь, и
    пул спрашивают одно и то же место.
    """
    if getattr(extractor, "supports_source_path", False):
        return None
    return MemberOutcome(
        skip_reason=SKIP_EXTRACT_UNAVAILABLE,
        detail=(
            "экстрактор не принимает локальные байты (split: DMZ-путь по URL) — "
            "член контейнера не извлечён; нужен шов извлечения по источнику"
        ),
    )


def _ingest_member_doc(
    task: MemberTask,
    *,
    extractor: ExtractorPort,
    chunker: ChunkerPort,
    ingest: ProductIngest,
    seen_docs: dict[str, str],
    seen_chunk_ids: set[str],
    errors: list[str],
    store: Any,
    level: DocumentLevel,
    fetched_at: str,
    lang: str | None,
    artifact_probes: list[dict[str, Any]],
    extracted: Any = _INLINE,
    timings: _CorpusStageTimings | None = None,
    reuse_kind: ReuseKind,
) -> MemberOutcome:
    """Член контейнера → документ корпуса (``CorpusDoc`` + чанки) или видимый пропуск.

    **Байты члена не ретейнятся** (§6.7): ``task.temp_path`` — временный файл, который
    спуск удаляет сразу после возврата. ``CorpusDoc.raw_path`` поэтому указывает на
    **архив** (сырьё I19), а не на член: член ре-выводится из архива тем же
    ``open_member``. ``CorpusDoc.url`` — синтетический ``{archive_url}!/{member_path}``.

    **Маршрут** — по суффиксу самого члена (pdf → ``document``, md/html → ``auto``), а не
    по маршруту архива: у контейнера маршрута извлечения нет.

    **``split``.** Экстрактор шва (``RemoteExtractor``) работает по URL и локальные байты
    не принимает. Такой член не извлекается **молча**: возвращается видимый пропуск
    ``extract_unavailable`` с причиной (капитуляция явная, не тихая).

    Дедуп — по ``content_hash`` в пределах продукта (I17), как у страниц: повтор
    перепечатки внутри архива не индексируется дважды, но член остаётся в
    ``corpus_members`` со ссылкой на уже существующий ``doc_id``.

    ``extracted`` — результат извлечения, если он пришёл из пула (``_INLINE`` ⇒
    извлекаем здесь, inline-путь байт-в-байт). Родительская часть (зонд, документ,
    нарезка, дедуп, строки) одна и та же для обоих путей — поэтому «пул vs inline» не
    может разойтись в наблюдаемых артефактах.

    ``timings`` — бакеты стадий продукта (наблюдаемость): сюда идёт ``chunk_s`` нарезки
    члена. ``None`` — не измерять (юнит-путь).
    """
    unavailable = _member_extract_unavailable(extractor)
    if unavailable is not None:
        return unavailable
    suffix = task.suffix.lower()
    # Зонд члена — та же классификация, что у артефакта верхнего уровня (класс/формат/
    # размер/``page_count`` PDF): «сколько страниц в PDF-члене» видно без извлечения.
    artifact_probes.append(
        probe_artifact(
            task.temp_path,
            product_id=task.product_id,
            url=task.member_url,
            content_type=MEMBER_CONTENT_TYPES.get(suffix),
            container_for=None,
        ).as_dict()
    )
    page = RawPage(
        url=task.member_url,
        content=b"",
        fetched_at=fetched_at,
        fetch_mode=FetchMode.HTTP,
        content_type=MEMBER_CONTENT_TYPES.get(suffix),
        lang=lang,
        source_path=task.temp_path,
    )
    if extracted is _INLINE:
        # Inline-путь (нет пула): извлечение здесь, на главном потоке.
        try:
            extracted = extractor.extract(page, _member_route(suffix))
        except Exception as exc:  # noqa: BLE001 — отказ члена изолируется (контракт фазы)
            return MemberOutcome(
                skip_reason=SKIP_EXTRACT_FAILED,
                detail=f"{type(exc).__name__}: {exc}",
            )
    if extracted is None or getattr(extracted, "error", None):
        return MemberOutcome(
            skip_reason=SKIP_EXTRACT_FAILED,
            detail=str(getattr(extracted, "error", None) or "пустой ExtractedDoc"),
        )
    doc_hash = str(getattr(extracted, "content_hash", "") or "")
    if not doc_hash:
        return MemberOutcome(
            skip_reason=SKIP_EXTRACT_FAILED,
            detail="I19: ExtractedDoc без content_hash — отклонён",
        )
    document_id = f"{task.product_id}:{doc_hash}"
    if doc_hash in seen_docs:
        # Тот же текст уже в партиции: член остаётся в corpus_members, но нового
        # документа/чанков не появляется (I17: повтор перепечатки не индексируется).
        # Признак частичности здесь **не** заявляется: документа этим ингестом не
        # создавалось, а признак принадлежит строке близнеца (``corpus_docs.partial``) —
        # второй заявкой о том же тексте мы бы спорили с ней.
        return MemberOutcome(doc_id=document_id, content_hash=doc_hash)
    seen_docs[doc_hash] = task.member_url

    # 16 §6: адрес версии документа — та же формула, что у страниц
    # (``content_hash`` + версия экстрактора + уровень). Durable-версии у члена
    # контейнера нет (его байты не ретейнятся, §6.7), но ``chunk_id`` обязан быть
    # функцией **содержимого и уровня**, а не пути получения: иначе один и тот же
    # текст внутри архива и на странице адресовался бы по-разному.
    member_extractor_token = (
        str(getattr(extracted, "extractor_version", None) or "")
        or str(getattr(extractor, "version", "") or "")
    )
    member_dv_id = doc_version_id(
        task.product_id, doc_hash, member_extractor_token, _as_text(level)
    )

    doc = CorpusDoc(
        doc_id=document_id,
        product_id=task.product_id,
        url=task.member_url,
        fetched_at=fetched_at,
        content_hash=doc_hash,
        # Архив — сырьё (I19); байты члена отдельно не ретейнятся (§6.7).
        raw_path=task.archive_path,
        extractor_version=str(getattr(extracted, "extractor_version", None) or "") or None,
        render_mode=_as_text(FetchMode.HTTP),
        lang=lang,
        # 16 §3.3: происхождение наблюдения члена — происхождение **контейнера**: своих
        # байтов из сети у члена нет, а ``NULL`` здесь значил бы «наблюдение не записано»
        # и навсегда запретил бы ``❌`` по свидетельству из архива. ``http_status`` не
        # наследуется: сервер по адресу члена в этом цикле не отвечал (``NULL`` — ровно
        # этот факт), а ответ контейнера — факт о другом URL.
        reuse_kind=reuse_kind,
        # Член контейнера — тот же путь извлечения, что и страница: признак частичности
        # обязан пережить его так же (иначе «прочитан 1 член из 47» молчит внутри архива).
        **_partial_fields(extracted),
    )
    ingest.docs.append(doc)
    _persist_docs(store, [doc], errors, versions_by_doc={document_id: member_dv_id})

    # Тот же признак — в исходе члена: документ корпуса (`corpus_docs.partial`) — само
    # свидетельство, а `MemberOutcome.partial` делает его **машинным**. Бинарный исход
    # «ингестнут/пропущен» без этого выдавал бы «прочитал часть» за полное чтение.
    member_partial = bool(getattr(extracted, "partial", False))
    member_partial_reason = (
        str(getattr(extracted, "partial_reason", None) or "").strip() or None
    )

    try:
        _chunk_started = time.perf_counter()
        try:
            chunks = chunker.chunk(extracted, document_id)
        finally:
            # ``chunk_s`` — работа главного потока; считается и при отказе нарезки.
            if timings is not None:
                timings.chunk_s += time.perf_counter() - _chunk_started
    except Exception as exc:  # noqa: BLE001 — сбой нарезки не отменяет документ
        errors.append(
            f"{task.product_id}/{task.member_url}: chunk: {type(exc).__name__}: {exc}"
        )
        return MemberOutcome(
            doc_id=document_id,
            content_hash=doc_hash,
            partial=member_partial,
            partial_reason=member_partial_reason,
        )

    for chunk in sorted(chunks or [], key=lambda c: getattr(c, "chunk_no", 0)):
        chunk_no = int(getattr(chunk, "chunk_no", 0))
        text = getattr(chunk, "text", "") or ""
        provided_hash = str(getattr(chunk, "content_hash", "") or "").strip()
        # I19: у каждого индексируемого чанка обязан быть непустой hash (как у страниц).
        if not provided_hash:
            errors.append(f"I19: {document_id}: чанк без content_hash отклонён")
            continue
        if not text.strip():
            errors.append(f"I19: {document_id}: чанк с пустым текстом отклонён")
            continue
        cid = _chunk_id(task.product_id, member_dv_id, chunk_no)
        if cid in seen_chunk_ids:
            errors.append(f"{document_id}: дублирующийся chunk_id {cid!r} отклонён")
            continue
        seen_chunk_ids.add(cid)
        ingest.chunks.append(
            CorpusChunk(
                chunk_id=cid,
                doc_id=document_id,
                product_id=task.product_id,
                url=task.member_url,
                heading_path=getattr(chunk, "heading_path", "") or "",
                lang=lang,
                chunk_no=chunk_no,
                text=text,
                context_prefix=getattr(chunk, "context_prefix", "") or "",
                content_hash=content_hash(text),
                build_id=None,
                embed_model_id=None,
                embedded_at=None,
            )
        )
    return MemberOutcome(
        doc_id=document_id,
        content_hash=doc_hash,
        partial=member_partial,
        partial_reason=member_partial_reason,
    )


def _serp_disabled_observation(
    source: str | None, mode: str | None = None
) -> dict[str, Any]:
    """Провенанс **выключенного** контура: «выключено, источник <уровень>».

    Отсутствие записи неотличимо от «контур есть, но не сработал», поэтому выключенный
    контур тоже пишет наблюдение — с уровнем, который его выключил (``run.yaml``/
    ``deploy.yaml``/``default``/``env``). Значения не выдумываются: бюджет не показывается,
    если контур выключен.

    **Форма совпадает с включённым контуром.** Набор ключей — тот же, что у
    :meth:`core.pipeline.serp_loop.SerpDiscovery.to_observation`, и это не косметика:
    читатель провенанса обязан отличать «поля нет, потому что контур выключен» от «поле не
    собрали». Разные формы делали это различие невыразимым — сравнение двух прогонов по
    ``yield``/``errors`` падало бы на выключенном контуре в ``KeyError`` или, хуже, читалось
    бы как «замер отсутствует» вместо «вызовов не было».

    Что именно означают нули: при ``enabled=False`` **ни один** вызов не сделан, поэтому
    нулевые счётчики (``used``/``charged``/``errors``/``budget_refused``) — измеренный факт,
    а не подстановка. Пустые отображения ``query_outcomes``/``yield``/``timing`` значат
    «ни одного продукта с вызовами» — ровно то же, что у включённого контура без запросов.
    ``measured`` живёт внутри строки ``timing`` на продукт и здесь не появляется по той же
    причине: строк нет, потому что не было вызова. ``declared_per_round``/
    ``effective_per_product`` остаются ``None``: объявленного бюджета у выключенного контура
    не существует, а ``0`` объявил бы бюджет, которого никто не задавал.
    """
    return {
        "enabled": False,
        "rounds": {
            "declared": 0,
            "executed": 0,
            "not_executed": 0,
            "not_executed_reason": None,
            "source": str(source or "default"),
        },
        "budget": {
            "declared_per_round": None,
            "effective_per_product": None,
            "used": {},
            "charged": {},
            "from_cache": {},
            "exhausted": {},
        },
        "adapter": None,
        # Объявленный режим виден и при выключенном контуре: ``run_id`` описывает его
        # всегда (ось в контракте), а ``used=None`` — «адаптера не было», не «async».
        "mode": {"declared": mode, "used": None},
        "unavailable_reason": None,
        "queries": {},
        # S7: исходов запросов нет, потому что запросов не было. Пустое отображение
        # говорит именно это; отсутствие ключа не говорило ничего.
        "query_outcomes": {},
        # Доходность и сбои — те же ключи, что у включённого контура (S8): оператор
        # сравнивает прогоны по ним, и выключенный контур не должен выпадать из сравнения.
        "yield": {},
        "candidates": {},
        "accepted": {},
        "rejected": {},
        "errors": {},
        "timing": {},
        "termination": {},
    }


def _run_serp_contour(
    *,
    plan: Any,
    url_map: Mapping[str, list[str]],
    search: Any,
    serp_rounds: int,
    serp_budget: int | None,
    serp_source: str | None,
    serp_mode: str | None,
    serp_langs: Sequence[str] | None,
    product_names: Mapping[str, str] | None,
    domain_profiles: Mapping[str, DomainProfile] | None,
    crawl_app: Any,
    crawl_defaults: Any,
    store: Any,
    run_id: str | None,
    errors: list[str],
) -> tuple[dict[str, Any], dict[str, dict[str, list[str]]]]:
    """Исполняет SERP-контур **до** дренажа: поиск → допуск → привязка к корням.

    Почему до дренажа, а не «построить корпус, потом долить». Ячейки прогона создаёт Ш6
    (``evaluate``), то есть **после** корпуса: на момент второго прохода внутри Ш5 старых
    ячеек не существует, и «пересборка + инвалидация» дала бы вторую полную экстракцию и
    вторую нумерацию партиций без нового знания. Один проход по объединённому набору URL
    даёт ровно тот же состав корпуса (старые + найденные документы) и тот же
    ``index_build_id``, но платит за него один раз; I19 при этом сохраняется полностью —
    найденный URL попадает в корпус обычным путём (retention → hash → index), а не
    подсовывается как evidence.

    Инвалидация затронутых ячеек (:func:`core.pipeline.serp_loop.invalidate_affected`)
    остаётся в механизме раундов (``run_serp_loop``) для случая, когда ячейки уже
    существуют (выборочная пере-оценка/повторный раунд в том же прогоне) — здесь она
    неприменима по той же причине: инвалидировать нечего.

    Возвращает ``(observation, seeds_by_product)``: второе — найденные URL, разложенные
    по объявленным корням (:func:`core.pipeline.serp_loop.assign_serp_seeds`). Найденный
    URL входит depth-0 seed'ом **своего** корня, а не новым корнем: иначе
    ``seeds_declared``/``max_documents``/«число корней продукта» описывали бы результат
    поиска, то есть зависели бы от ранжирования провайдера (A2/I16). Наблюдение пишется
    в провенанс сеанса.

    :raises ValueError: ``rounds > 0`` без бюджета (безлимит запрещён выше по стеку; здесь
        — защита от прямого вызова модуля).
    """
    # Локальный импорт: ``serp_loop`` импортирует ``IndexBuild`` из этого модуля, поэтому
    # модульный импорт дал бы цикл (та же причина, что у ``_worker_extractor_for``).
    from core.domain.enums import SearchPhase
    from core.pipeline.search_log import SearchBudget, SearchEffort
    from core.pipeline.serp_loop import (
        assign_serp_seeds,
        build_serp_queries,
        declared_serp_hosts,
        serp_discover,
    )

    if serp_rounds <= 0:
        return _serp_disabled_observation(serp_source, serp_mode), {}
    if serp_budget is None or int(serp_budget) < 1:
        raise ValueError(
            "SERP-контур: rounds > 0 требует объявленного серп-бюджета "
            "(run.yaml: serp.budget) — безлимит внешнего поиска запрещён"
        )

    products = sorted(str(pid) for pid in url_map)
    own_hosts, declared_hosts = declared_serp_hosts(url_map, domain_profiles)
    param_rows = list(getattr(plan, "params", None) or [])
    langs = [str(lang) for lang in (serp_langs or ()) if str(lang).strip()]
    queries = build_serp_queries(
        products=products,
        params=param_rows,
        product_names=product_names,
        langs=langs or ["ru"],
    )
    # Блэклист — из **разрешённых** осей продукта (app < run < site): тот же предикат, что
    # у краула, иначе поиск протащил бы в корпус то, что краул зачёркивает.
    limits_by_product: dict[str, CrawlLimits] = {}
    for product_id in products:
        roots = url_map.get(product_id) or []
        base_url = str(roots[0]) if roots else ""
        if not base_url:
            continue
        try:
            limits_by_product[product_id] = resolve_crawl_limits(
                base_url,
                app=crawl_app if crawl_app is not None else CrawlConfig(),
                run=crawl_defaults,
                profiles=domain_profiles or {},
            )
        except Exception as exc:  # noqa: BLE001 — оси краула вторичны к поиску
            errors.append(
                f"serp: resolve_crawl_limits({product_id}): {type(exc).__name__}: {exc}"
            )

    effort = SearchEffort(
        budget=SearchBudget(limits={SearchPhase.SERP.value: int(serp_budget) * int(serp_rounds)})
    )
    discovery = serp_discover(
        adapter=search,
        queries_by_product=queries,
        store=store,
        run_id=str(run_id or ""),
        effort=effort,
        rounds=int(serp_rounds),
        own_hosts_by_product=own_hosts,
        declared_hosts=declared_hosts,
        known_urls_by_product=url_map,
        limits_by_product=limits_by_product,
        source=str(serp_source or "run.yaml"),
        budget_declared=int(serp_budget),
        # Режим — из объявленной оси (контракт ``serp_mode``), а не из env: он
        # определяет бэкенд, значит и состав находок, и записывается в наблюдение.
        mode=serp_mode,
    )
    observation = discovery.to_observation()
    if discovery.unavailable_reason == "no_adapter":
        # Объявленный контур без адаптера — видимый отказ, а не молчаливый пропуск:
        # в ``split`` адаптер приходит швом, в ``local`` его подключает оператор.
        errors.append(
            "serp: объявлены раунды, но адаптер внешнего поиска не подключён "
            "(local: поисковый адаптер не собран; split: шов /v1/search) — "
            "контур не исполнен"
        )
    elif discovery.unavailable_reason == "no_queries":
        errors.append("serp: объявлены раунды, но запросов нет (пустой каталог/языки)")

    # Находки раскладываем по **авторским** корням: url_map не расширяется — иначе
    # найденный документ стал бы корнем и принёс бы с собой собственный бюджет.
    seeds_by_product = assign_serp_seeds(discovery.accepted_by_product, url_map)
    return observation, seeds_by_product


def _serp_note(observation: Mapping[str, Any]) -> str:
    """Операторская строка о состоянии SERP-контура (наблюдение, не ошибка).

    Выключенный контур тоже печатается: «выключено, источник X» — это факт прогона, а не
    отсутствие факта. У включённого видны раунды, бюджет, число принятых URL и причины
    отказов — то есть **доходность** контура (замер: 189 запросов → 10 новых URL на
    объявленном хосте), а не только его факт.

    Время ожидания внешней системы (S4) называется, только если оно **наблюдено** и
    непусто: «ждали 0 с» — не новость, а вот «полтора часа ушло в паузы внешней
    async-операции» оператор иначе не увидел бы. Машинно-читаемое число живёт в
    наблюдении (``timing``), строка — лишь его краткий пересказ.

    Сбои вызовов (S8) называются **отдельно** от отказов допуска: «принято URL 0, отказов
    0» при отказавшем провайдере читалось бы как «поиск ничего не нашёл», хотя искали и не
    получили ответа. Строка печатается только по непустому счётчику — исправному контуру
    она ничего не добавляет. Доходность (принятых URL на обслуженный запрос) печатается
    **только при непустом знаменателе**: ноль обслуженных запросов не даёт «доли 0», он не
    даёт доли вовсе.
    """
    rounds = observation.get("rounds") or {}
    budget = observation.get("budget") or {}
    if not observation.get("enabled"):
        return (
            "serp: контур выключен "
            f"(источник: {rounds.get('source', 'default')}) — вызовов внешнего поиска нет"
        )
    accepted = observation.get("accepted") or {}
    accepted_total = sum(len(urls) for urls in accepted.values())
    used = budget.get("used") or {}
    used_total = sum(int(n) for n in used.values())
    rejected = observation.get("rejected") or {}
    rejected_total = sum(
        int(count) for reasons in rejected.values() for count in reasons.values()
    )
    note = (
        f"serp: раундов {rounds.get('executed', 0)}/{rounds.get('declared', 0)} "
        f"(источник: {rounds.get('source', 'run.yaml')}), запросов {used_total}, "
        f"принято URL {accepted_total}, отказов {rejected_total} "
        f"(бюджет на продукт: {budget.get('effective_per_product')})"
    )
    # S8: сбой вызова — **отдельная** строка счёта, а не «ничего не нашлось». Прогон, в
    # котором провайдер отказал на каждом запросе, иначе читался бы оператором ровно как
    # «поиск ничего не нашёл»: он видел бы только «принято URL 0». Печатается по непустому
    # счётчику: у исправного контура эта строка — шум.
    errors_total = sum(int(n) for n in (observation.get("errors") or {}).values())
    if errors_total:
        note += f"; сбоев {errors_total}"
    # Доходность — числитель и знаменатель рядом, чтобы доля не вычислялась «на глаз» из
    # разных блоков. Только при непустом знаменателе: ``served == 0`` не даёт «доли 0», он
    # не даёт доли вовсе (``None`` в наблюдении — отсутствие замера, а не нулевой выход).
    served_total = sum(
        int(row.get("served") or 0)
        for row in (observation.get("yield") or {}).values()
        if isinstance(row, Mapping)
    )
    if served_total:
        note += (
            f"; доходность {accepted_total / served_total:.3f} принятых URL "
            f"на обслуженный запрос ({accepted_total}/{served_total})"
        )
    if observation.get("unavailable_reason"):
        note += f"; контур не исполнен: {observation['unavailable_reason']}"
    exhausted = [
        pid for pid, flag in (budget.get("exhausted") or {}).items() if flag
    ]
    if exhausted:
        note += f"; бюджет исчерпан: {', '.join(sorted(exhausted))}"
    # S4: стоимость ожидания внешней системы. Печатается только наблюдённое и непустое:
    # «ждали 0 с» — не факт для оператора, а вот «сколько прогона ушло в паузы» — факт.
    timing = observation.get("timing") or {}
    if isinstance(timing, Mapping):
        wait = sum(
            float(row.get("wait_seconds") or 0)
            for row in timing.values()
            if isinstance(row, Mapping) and row.get("measured")
        )
        request = sum(
            float(row.get("request_seconds") or 0)
            for row in timing.values()
            if isinstance(row, Mapping) and row.get("measured")
        )
        if wait > 0:
            note += (
                f"; ожидание внешней системы {wait:.1f} с, запросы {request:.1f} с"
            )
    # «Нашли» ≠ «вошло в корпус»: принятый кандидат может быть зачёркнут блэклистом или
    # не поместиться в ``max_documents`` корня — это отдельный факт и он называется.
    ingest = observation.get("ingest") or {}
    if ingest:
        enqueued = sum(int(row.get("enqueued", 0)) for row in ingest.values())
        dropped = sum(len(row.get("blacklisted", ())) for row in ingest.values())
        truncated = sum(len(row.get("truncated", ())) for row in ingest.values())
        note += f"; в очередь корпуса {enqueued}"
        if dropped:
            note += f", зачёркнуто блэклистом {dropped}"
        if truncated:
            note += f", не поместилось в max_documents {truncated}"
    return note


def _persist_serp_provenance(
    store: Any, session_id: str | None, observation: Mapping[str, Any], errors: list[str]
) -> None:
    """Дописывает наблюдение SERP-контура в провенанс сеанса краула (durable).

    Источник — провенанс **сеанса**, а не отдельная таблица: контур расширяет тот же
    краул (его документы добыты в этом же сеансе), а ``StorePort`` не даёт отдельного
    места под факты поиска. Запись аддитивна (ключ ``serp``), поэтому старые читатели
    провенанса не ломаются.

    Идемпотентность: повтор (resume/report) перезаписывает ключ тем же детерминированным
    значением. Сбой записи изолируется в ``errors`` — провенанс не роняет фазу.
    """
    if store is None or session_id is None:
        return
    try:
        session = store.get_crawl_session(session_id)
        if session is None:
            return
        existing = read_provenance(session)
        existing["serp"] = dict(observation)
        store.set_crawl_session_provenance(
            session_id,
            json.dumps(existing, ensure_ascii=False, sort_keys=True),
        )
    except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
        errors.append(f"serp: set_crawl_session_provenance: {type(exc).__name__}: {exc}")


def _invalidate_previous_build_cells(
    store: Any, run_id: str | None, index_build_id: str, errors: list[str]
) -> list[str]:
    """Помечает ``stale_reason='new_build'`` ячейки прогона, привязанные к прошлой сборке.

    Зачем это здесь, а не в Ш6: сборка создаётся в Ш5, и только она знает, какая
    сборка теперь текущая. «Ячейка описывает ушедшую сборку» — факт, который фиксирует
    фаза корпуса; расчёт (:mod:`core.pipeline.compute`) уже умеет исключать такие ячейки,
    отчёт — называть их очередью пере-оценки, а выборочная пере-оценка исполняется
    селектором :func:`core.pipeline.evaluate.select_stale_cells` (``hound revalidate``):
    он берёт **только** помеченные ячейки, поэтому за незатронутое платить не нужно.

    Идемпотентно (``invalidate_affected`` пропускает уже помеченные) и безопасно при
    отсутствии ячеек: пустой список — no-op. Сбой изолируется в ``errors`` — провенансная
    пометка не должна ронять уже собранный корпус.

    :returns: строки-наблюдения для ``log_entries`` (не ошибки: пометка — это факт, а
        не сбой фазы).
    """
    if store is None or not run_id or not index_build_id:
        return []
    try:
        previous = {
            cell.index_build_id
            for cell in store.iter_cells(str(run_id))
            if cell.index_build_id and cell.index_build_id != index_build_id
        }
    except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее пометки
        errors.append(f"stale(new_build): iter_cells: {type(exc).__name__}: {exc}")
        return []
    if not previous:
        return []
    from core.pipeline.serp_loop import invalidate_affected

    notes: list[str] = []

    for old_build_id in sorted(previous):
        try:
            invalidated = invalidate_affected(
                store, str(run_id), old_build_id, index_build_id
            )
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            errors.append(
                f"stale(new_build): invalidate_affected({old_build_id}): "
                f"{type(exc).__name__}: {exc}"
            )
            continue
        if invalidated:
            notes.append(
                f"corpus: {len(invalidated)} ячеек прошлой сборки ({old_build_id}) "
                "помечены устаревшими (new_build) — требуют пере-оценки: "
                f"hound revalidate --run-id {run_id}"
            )
    return notes


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
    out_dir: str | Path | None = None,
    index_root: str | Path | None = None,
    embedder: EmbedderPort | None = None,
    index_builder: IndexBuilderPort | None = None,
    domain_profiles: Mapping[str, DomainProfile] | None = None,
    explicit_fetch: Mapping[str, FetchMode] | None = None,
    crawl_defaults: CrawlRunConfig | None = None,
    embed_device: str = EmbedDevice.CPU.value,
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
    extractor_factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort] | None = None,
    extract_cache: ExtractionCache | None = None,
    embed_cache: EmbedCache | None = None,
    extract_task_timeout_s: float | None = None,
    max_attempts: int | None = None,
    progress: Callable[[dict[str, int]], None] | None = None,
    on_product: Callable[[str, str], None] | None = None,
    crawl_app: CrawlConfig | None = None,
    state: ProductStatePort | None = None,
    product_modes: Mapping[str, str] | None = None,
    refresh_policy: str = "auto",
    container_for: Callable[[str | Path], ContainerPort | None] | None = None,
    pools: _ExtractPools,
    lifecycle: PhaseLifecycle | None = None,
    mem_budget_mb: int | None = None,
    derive_extract_workers: bool = False,
    search: Any = None,
    serp_rounds: int = 0,
    serp_budget: int | None = None,
    serp_source: str | None = None,
    serp_mode: str | None = None,
    serp_langs: Sequence[str] | None = None,
    product_names: Mapping[str, str] | None = None,
    manual_artifacts: ManualArtifacts | None = None,
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
    :param out_dir: корень прогонов для отчёта ``artifacts_gap.yaml``
        (``{out_dir}/{run_id}/outputs/``, 13-manual-artifacts §4). ``None`` — отчёт не
        пишется: у прямого вызова модуля нет определённого каталога прогонов, а
        CWD-дефолт сделал бы артефакт общим для всех вызовов процесса (та же причина,
        что у выключенных кэшей). Composition root передаёт resolved ``paths.runs``.
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
    :param embed_device: устройство счёта векторов — **объявленная ось** прогона
        (``embed.device``, 16 §4.1). Composition root резолвит её
        (:func:`core.pipeline.crawl_frontier.resolve_embed_device`) и передаёт фазе
        аргументом; фаза не читает env и не спрашивает адаптер (``EmbedderPort`` об
        устройстве не знает — и это правильно: место счёта не свойство порта). Значение
        входит в материал ``pb``, в манифест партиции, в ключ ``embed_cache`` и в проверку
        переиспользования; дефолт ``cpu`` — объявленный дефолт оси, а не «что выберет
        движок» (``auto`` не существует).
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
    :param extractor_factory: P5/P7 — фабрика экстрактора
        ``Callable[[DocumentLevel, PdfPipeline], ExtractorPort]``, переданная ссылкой из
        composition root (слой-матрица: pipeline не импортирует адаптеры). Применяется к
        **inline**-пути (``extract_workers`` None/``1``): для пары, отличной от
        ``(full, structured)``, строится экстрактор этой пары; ``(full, structured)``
        переиспользует переданный ``extractor`` (дефолт — без лишней загрузки весов docling).
        ``None`` → оси на inline-пути не действуют (поведение как прежде; пул-путь всё
        равно получает их через ``extraction_pool.pool_factory``).
    :param extract_cache: Ф1.2 — межпрогонный кэш экстракции (:class:`ExtractionCache`).
        ``None`` → кэш выключен, поведение байт-в-байт прежнее. Ключ — хеш **ретенированных**
        байтов (I19), поэтому хит не может подсунуть неверифицированный контент: хеш
        пересчитывается из тех же байтов, которые прочитал бы экстрактор. C1/L4: на пулевом
        пути проба выполняется **в родителе** до submit (хит не занимает слот воркера и не
        оплачивает инициализатор); проба в воркере остаётся защитой, ключ у них один.
    :param embed_cache: межпрогонный кэш **векторов** (:class:`~core.pipeline.embed_cache.EmbedCache`).
        ``None`` → кэша нет, поведение байт-в-байт прежнее. Ключ — ``(content_hash текста
        чанка, embed_model_id)``, поэтому кэш переиспользует вектор там, где текст совпал, и
        не зависит от ``extractor_version``/``chunker_version``: смена механизма извлечения
        или добавление документа пересчитывает только реально изменившийся текст, а не всю
        продукт-партицию. ``pb``/манифест партиции от кэша **не** меняются: кэш сокращает
        стоимость эмбеддинга, а не условия измерения (A3).
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
    :param on_product: Phase 4 — опциональный колбэк **границ продукта**
        (``("start"|"end", product_id)``): вызывается **живо** по ходу фазы — ``start``
        перед первым дренажом продукта, ``end`` после его тела (включая нулевой выход).
        Закрывает разрыв «продукт ещё в очереди vs продукт потерян»: продукт-цикл строго
        последователен, и дренаж одного продукта может идти часами, а ``log_entries``
        материализуются только в конце фазы. Ошибки колбэка изолируются и не роняют
        фазу (наблюдаемость вторична). ``None`` → без вывода (поведение неизменно).
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
    :param container_for: B3 — инъекция читателя контейнеров
        (``Callable[[path], ContainerPort | None]``; composition root передаёт
        ``core.adapters.container_any.container_for``). ``None`` → спуск не исполняется, но
        **видимо**: каждый допущенный контейнер получает запись ``archive_observations``
        со статусом ``unreadable`` и причиной ``container_reader_unavailable`` (тихий
        пропуск запрещён).
    :param mem_budget_mb: F0 — бюджет памяти прогона, MB, **явный вход** от composition root
        (:func:`core.pipeline.resources.resolve_mem_budget_mb`). ``None`` → бюджета нет, окно
        экстракции из него не выводится. Фаза окружение не читает: иначе тесты и расписание
        стали бы функцией памяти хоста.
    :param derive_extract_workers: F0 — выводить окно экстракции из бюджета, когда ось
        ``crawl.extract_workers`` не объявлена ни на одном слое. ``True`` только для
        локального экстрактора (процессный пул/inline в ядре); под ``split`` воркеры живут
        на edge со своим бюджетом, поэтому флаг приходит ``False`` — его решает composition
        root, а не фаза.
    :param search: адаптер внешнего поиска (``SearchAdapterPort``) — исполняет SERP-контур.
        ``None`` при объявленных раундах — видимый отказ (``errors``), а не молчаливый
        пропуск: в ``split`` адаптер приходит швом, в ``local`` его подключает оператор.
    :param manual_artifacts: объявленные ручные артефакты пакета
        (``sources/manual_artifacts.yaml``, 13-manual-artifacts §3) — байты, которые не
        взял краулер. Подаются **тем же** путём I19 (``RawPage`` → :func:`_retain` →
        экстракция → чанкер → индекс): второго пути байтов в evidence не появляется.
        Отказ краула при этом остаётся в ``crawl_session_items``, а наблюдённость
        материала видна отдельно — ``corpus_docs.origin`` со значением ``manual``.
        ``None`` — подач нет (поведение байт-в-байт прежнее).
    :param serp_rounds: раунды SERP-контура (0 — выключен). Вход — объявленная ось
        (``serp_rounds`` контракта), а не чтение конфига внутри фазы.
    :param serp_budget: потолок запросов **на продукт на раунд** (``run.yaml:
        serp.budget``). Обязан быть объявлен при ``serp_rounds > 0``.
    :param serp_source: уровень, которым включён контур (``cli``/``env``/``run.yaml``/
        ``deploy.yaml``/``default``) — попадает в провенанс, чтобы «почему поиск не
        работал» было отличимо от «поиск не объявлен».
    :param serp_langs: языки запросов по умолчанию (``run.yaml: languages``); язык
        параметра (``catalog.csv: search_langs``) сильнее.
    :param product_names: ``product_id → каноническое имя`` (из паспорта) для запросов
        «каталог + имя продукта» (замер: с именем 45/63 попаданий против 27/63 без него).
        Продукт без имени ищется по заголовку параметра, и это видно в провенансе.

    Возвращает ``PhaseResult`` со ``status='ok'`` (даже при изолированных ошибках),
    ``cells=[]`` (Ш5 не производит ячеек) и ``log_entries=[IndexBuild, ResourcesObservation]``
    (сборка + счётчики; F0 — наблюдённые ресурсные факты фазы: расписание экстракции,
    чанки на документ, пик воркеров).

    :raises ValueError: если ``urls`` задан, но не является отображением.
    """
    if urls is not None and not isinstance(urls, Mapping):
        raise ValueError("urls обязан быть отображением product_id → список URL")

    url_map: dict[str, list[str]] = {}
    if urls:
        for pid, values in urls.items():
            # C/порядок (execution_change §13): **авторский** порядок корней, не
            # лексикографический. `sorted()` ставил `https://host/` перед
            # `https://host/docs/`, и живой прогон 22.10 показал цену: 800-страничный
            # корпоративный портал отработал 2 часа, а docs-корень (высшая ценность для
            # карты возможностей) не дренировался вовсе — упавший прогон не дошёл до него.
            # Дедуп — первое вхождение (стабильно для повторной генерации/паспортов).
            url_map[str(pid)] = _ordered_unique(values or [])

    # C (execution_change): scope корня = **объявленные** хосты. Объявленные — ключи
    # `domain_profiles.yaml` (пакет) и все seed-корни прогона; хост, объявленный
    # собственным корнем продукта, из «поддомен-допуска» этого продукта исключается:
    # у него свой корень и свой бюджет (иначе учебный поддомен продолжал бы тратить
    # бюджет корня-предка, хотя объявлен отдельным корнем).
    scope_hosts_by_product: dict[str, frozenset[str]] = {}
    if url_map:
        all_root_hosts = {
            normalize_scope_host(urlsplit(str(url)).hostname or "")
            for product_urls in url_map.values()
            for url in product_urls
        }
        declared_profile_hosts = {
            normalize_scope_host(str(key).split("/", 1)[0])
            for key in (domain_profiles or {})
        }
        declared_hosts = (all_root_hosts | declared_profile_hosts) - {""}
        for product_id, product_urls in url_map.items():
            own_hosts = {
                normalize_scope_host(urlsplit(str(url)).hostname or "")
                for url in product_urls
            }
            scope_hosts_by_product[product_id] = frozenset(
                declared_hosts - own_hosts
            )

    declared = sorted({str(p) for p in (getattr(plan, "products", None) or [])})

    errors: list[str] = []

    # --- SERP-контур (I3/§5): поиск → допуск → расширение объявленного seed-набора --- #
    # Контур исполняется **внутри** фазы корпуса (не отдельной фазой ``PHASES``) и до
    # дренажа: найденные URL входят в тот же набор, что и авторские, поэтому проходят
    # обычный путь retention → hash → index (I19). Почему не «после сборки» — см.
    # докстринг :func:`_run_serp_contour`.
    serp_observation: dict[str, Any] = _serp_disabled_observation(serp_source, serp_mode)
    #: Найденные SERP документы, разложенные по авторским корням: ``product → root → urls``.
    #: Второй проход по ним — обычный дренаж тех же корней (глубина 0), не «долив в индекс».
    serp_seeds_by_product: dict[str, dict[str, list[str]]] = {}
    #: Судьба находок по продуктам (``queued``/``dup``/``blacklisted``/``truncated``).
    serp_ingest_by_product: dict[str, dict[str, Any]] = {}
    if serp_rounds:
        try:
            serp_observation, serp_seeds_by_product = _run_serp_contour(
                plan=plan,
                url_map=url_map,
                search=search,
                serp_rounds=int(serp_rounds),
                serp_budget=serp_budget,
                serp_source=serp_source,
                serp_mode=serp_mode,
                serp_langs=serp_langs,
                product_names=product_names,
                domain_profiles=domain_profiles,
                crawl_app=crawl_app,
                crawl_defaults=crawl_defaults,
                store=store,
                run_id=run_id,
                errors=errors,
            )
        except ValueError:
            # Безлимит/битый конфиг — не «частичный успех»: отказ обязан дойти до вызывающего.
            raise
        except Exception as exc:  # noqa: BLE001 — контур не роняет объявленную фазу
            errors.append(f"serp: проход не исполнен: {type(exc).__name__}: {exc}")

    # Объём корпуса = продукты **с эффективными seed'ами**, а не весь план: при
    # ``--only-product``/``--only-failed`` ``urls`` уже сужен фильтром, и запись
    # zero-source покрытия для продуктов вне области прогона была бы ложной (09 §6.2).
    # Пустой ``urls`` (не задан) → весь план, как прежде. SERP-находки объём **не**
    # расширяют: они не корни, а seeds уже объявленных корней — продукт без авторского
    # seed'а поиском не «заводится» (иначе число продуктов зависело бы от ранжирования).
    products = sorted(url_map) if url_map else declared

    #: 16 §3.1: объём прогона — провенанс, а не материал идентичности (в ``run_id`` он
    #: входит через ``plan.run_scope``, ``plan.py:94``). Нужен строке ``run_product``
    #: (16 §3.2): без него полное измерение и срез ``--only-product`` неотличимы.
    #: Дефолт берётся у перечисления (:class:`~core.domain.enums.RunScope`), а не
    #: литералом; само значение остаётся строкой намеренно — ``product:<id>`` членом
    #: ``RunScope`` не является (``run_identity.resolve_run_scope``: ``PRODUCT_ONLY`` —
    #: категория, а не готовый префикс).
    run_scope = str(getattr(plan, "run_scope", None) or RunScope.FULL.value)

    #: Продукты, для которых строка ``run_product`` уже написана (граница продукта).
    #: Нужен концу фазы: он закрывает **остальные** объявленные продукты и не перетирает
    #: измеренные числа уже закрытых — второй расчёт той же величины разошёлся бы с первым.
    run_products_written: set[str] = set()

    package = _resolve_package_id(plan, package_id)
    snapshot = _resolve_snapshot_date(plan, snapshot_date)
    resolved_chunker_version = (
        str(chunker_version)
        if chunker_version is not None
        else str(getattr(chunker, "version", "") or "")
    )
    embed_model_id = str(getattr(embedder, "model_id", "") or "")
    # Провенанс движков берётся у **сборщика**: composition root передаёт в
    # ``NpyIndexBuilder`` фактический лексический движок прогона (``tantivy-<ver>``,
    # §13:676). Пустая строка здесь — не «неизвестная версия», а «лексического плеча
    # в прогоне нет» (без tantivy R0/R2 пропускаются, ``experiments`` → ``skipped``)
    # либо инъектированный duck-typed сборщик без атрибута (тесты/DI). Hard-fail на
    # пустом значении сломал бы легитимную dense-only сборку (N3), поэтому это не
    # ошибка прогона; ``getattr`` оставлен именно для duck-typed случая.
    lexical_engine_version = str(getattr(index_builder, "lexical_engine_version", "") or "")
    vector_store = str(getattr(index_builder, "vector_store", "") or "")

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
    #: Прерванная фаза обязана закрыть сеанс терминально (см. ``PhaseLifecycle``):
    #: адрес сеанса известен только здесь, а ``finally`` — в обёртке :func:`build_corpus`.
    if lifecycle is not None:
        lifecycle.store = store
        lifecycle.session_id = session

    #: F1/Д-6: признак жизни сеанса (``heartbeat_at``/``owner``) — точечно, с троттлингом
    #: (:class:`core.pipeline.liveness.Heartbeat`). Без сеанса объект пуст, вызовы — no-op,
    #: поэтому воронки не обрастают ветками «а если durable-краул выключен».
    heartbeat = Heartbeat(store, session)

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

    #: v5: ``product_id → [url]`` документов, у которых переиспользуется версия, а признак
    #: частичности в ней **не записан** (``document_version.extract_partial IS NULL``:
    #: строка создана до v5). Это не «полное» и не «частичное» — это «не измерено», и в
    #: отчёте оно называется отдельно, чтобы молчание не читалось как полнота.
    partial_unknown_products: dict[str, list[str]] = {}
    #: v5: смены частичности при записи версии (``было частично → стало полное`` и обратно).
    #: Смена — факт наблюдения, а не ошибка: он попадает в строку продукта, а не в ``errors``.
    partiality_changes: list[dict[str, Any]] = []

    # ------------------------------------------------------------------ #
    # L5 шаг 2: связывание константных аргументов фазы (``functools.partial``)
    # ------------------------------------------------------------------ #
    # Замыкания среднего слоя вынесены в ``corpus_ingest``/``corpus_commit`` и получают
    # зависимости **параметрами** — сигнатура называет, что функция читает и куда пишет.
    # Здесь эти зависимости связываются один раз: ``partial`` только фиксирует аргументы
    # (не замыкание и не состояние, ничего не мутирует), поэтому глубоко вложенные места
    # не повторяют по восемь одинаковых ``store=``/``session=`` и не вылезают за 100
    # символов. Имена алиасов — прежние: точки вызова не меняются.
    emit = partial(
        emit_progress,
        store=store,
        session=session,
        progress=progress,
        heartbeat=heartbeat,
    )
    #: Резолв ``CrawlTarget`` — те же пять осей у всех путей добычи (5 точек вызова).
    _crawl_target = partial(
        resolve_crawl_target,
        sources=crawl_sources or CrawlSourcesConfig(),
        profiles=domain_profiles or {},
        explicit=explicit_fetch,
        run_default_strategy=crawl_strategy_default,
        run_default_rate_limit=crawl_rate_default,
    )
    #: Резолв осей краула ``app < run < site``: одна проводимость бюджета на фазу
    #: (F0: ``derive_extract_workers``/``mem_budget_mb`` едут тем же путём).
    _limits_for_url = partial(
        resolve_crawl_limits,
        app=crawl_app or CrawlConfig(),
        run=crawl_defaults,
        profiles=domain_profiles or {},
        derive_extract_workers=derive_extract_workers,
        mem_budget_mb=mem_budget_mb,
    )
    _validators = partial(
        corpus_ingest.validators,
        state=state,
        policy=policy,
        planned_validators=planned_validators,
        errors=errors,
    )
    _reuse_for_page = partial(
        corpus_ingest.reuse_for_page,
        state=state,
        errors=errors,
        chunker_version=resolved_chunker_version,
    )
    _reuse_same_hash = partial(
        corpus_ingest.reuse_same_hash,
        state=state,
        errors=errors,
        chunker_version=resolved_chunker_version,
    )
    _mark = partial(
        corpus_ingest.mark,
        session=session,
        store=store,
        items=items,
        targets=targets,
        applied_engines=applied_engines,
        errors=errors,
        heartbeat=heartbeat,
        state=state,
    )
    _retain = partial(
        corpus_ingest.retain,
        mark=_mark,
        errors=errors,
        raw_root_path=raw_root_path,
    )
    _commit_local = partial(
        corpus_ingest.commit_local,
        mark=_mark,
        retain=_retain,
        validators=_validators,
        errors=errors,
        applied_engines=applied_engines,
    )
    _local_fetch = partial(corpus_ingest.local_fetch, limiter=limiter)
    _fetch_and_commit = partial(
        corpus_ingest.fetch_and_commit,
        limiter=limiter,
        commit_local=_commit_local,
    )
    _local_prefetch = partial(
        corpus_ingest.local_prefetch,
        validators=_validators,
        crawler=crawler,
        crawlers=crawlers,
        items=items,
        raw_root_path=raw_root_path,
    )
    _process_local = partial(
        corpus_ingest.process_local,
        local_prefetch=_local_prefetch,
        commit_local=_commit_local,
        fetch_and_commit=_fetch_and_commit,
    )
    _submit_collect = partial(
        corpus_ingest.submit_collect,
        executor=executor,
        errors=errors,
        session=session,
        run_id=run_id,
        executor_poll_interval=executor_poll_interval,
        executor_sleep=executor_sleep,
    )
    _work_item = partial(corpus_ingest.work_item, validators=_validators)
    _resolve_target = partial(
        corpus_ingest.resolve_target,
        targets=targets,
        crawl_sources=crawl_sources,
        domain_profiles=domain_profiles,
        explicit_fetch=explicit_fetch,
        crawl_strategy_default=crawl_strategy_default,
        crawl_rate_default=crawl_rate_default,
    )
    _plan_incremental_items = partial(
        corpus_ingest.plan_incremental_items,
        state=state,
        policy=policy,
        product_modes=product_modes,
        planned_validators=planned_validators,
        errors=errors,
    )
    _group_revalidation_items = partial(
        corpus_ingest.group_revalidation_items,
        scope_hosts_by_product=scope_hosts_by_product,
    )
    _resolve_product_revalidate_cap = partial(
        corpus_ingest.resolve_product_revalidate_cap,
        limits_for=_limits_for_url,
    )
    _expand_directory = partial(
        corpus_ingest.expand_directory,
        mark=_mark,
        emit=emit,
        errors=errors,
        pollers=pollers,
        applied_engines=applied_engines,
    )

    def _finish_one(
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
        reuse_kind: ReuseKind,
        extract_error: str | None = None,
        extract_failure_kind: FailureKind | None = None,
        reuse: DocReuse | None = None,
        origin: str = ORIGIN_CRAWL,
    ) -> str | None:
        """Доводка одного URL после экстракции: дедуп → нарезка → ``_mark`` (главный поток).

        P4: граница расщепления проходит **механически** по ``extracted`` — результат
        экстракции приходит либо из inline-вызова, либо из ``future.result()`` пула. Сырые
        байты уже сохранены (``raw_path``); шаги изолируют ошибки в ``errors`` и фиксируют
        терминальный статус элемента сеанса через ``_mark`` (FIFO на главном потоке).

        **Переиспользование (09 §6.3/§9).** ``reuse`` — готовая версия документа (путь
        ``304``, экстракция не вызывалась); ``None`` — обычная экстракция. При ``200`` с
        прежним ``content_hash`` версия ищется здесь же (:func:`_reuse_same_hash`): чанкер
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
            """``_mark`` для краула; для ручного артефакта — no-op (аудит не перезаписывается)."""
            if manual:
                return
            _mark(pid, u, status, **fields)

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
            active_reuse = _reuse_same_hash(
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
        # продукт. ``_persist_docs`` изолирует сбой записи в ``errors`` — краул продолжается.
        _persist_docs(
            store, [doc], errors, origin=origin, versions_by_doc={document_id: dv_id}
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
                _chunk_started = time.perf_counter()
                try:
                    chunks = chunker.chunk(extracted, document_id)
                finally:
                    # ``chunk_s`` — работа главного потока; считается и при отказе нарезки.
                    timings.chunk_s += time.perf_counter() - _chunk_started
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
            chunker_version=resolved_chunker_version,
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

    # -- 13-manual-artifacts §3: подача объявленных файлов оператора ---------- #

    # --- Pass A (ingest): crawl → retention → extract → dedup → chunk ---------- #
    # Векторы/физический индекс и персистенция corpus_chunks здесь не выполняются:
    # сначала собирается содержимое продукта, затем (на его границе) планируется
    # партиция и фиксируется Pass B. ``index_build_id`` (композиция когорты) —
    # единственное, что выводится после **всех** продуктов.
    #
    # Phase 3.3: две ветки исполнения краула — локальная (``crawler``/``crawlers``, прежнее
    # поведение) и через ``executor`` (``split``/edge-планировщик). Общая часть (retention/
    # экстракция/дедуп/нарезка/сеансовый трекинг) вынесена в ``_retain``/``_ingest_one`` —
    # обе ветки используют её, не дублируя логику.
    ingests: list[ProductIngest] = []

    emit()

    # --- C1/L1: фазовый пул и FIFO-очередь экстракции ------------------------- #
    # Пул создаётся лениво на ключ ``(level, window)`` и живёт до ``finally`` фазы
    # (обёртка :func:`build_corpus`), а не на seed-root. Очередь доводки — тоже фазовая:
    # хвост корня, не способный расширить свой frontier, доводится во время фетча
    # следующего корня, а не блокирует его.
    extract_pending: deque[PendingExtract] = deque()
    #: Корни с недоведённым хвостом — в порядке дренажа (порядок провенанса сохраняется).
    extract_finalizers: deque[RootDrain] = deque()
    #: Бакеты стадий **текущего** продукта (наблюдаемость, не контракт). Объявлены здесь, а
    #: не в цикле продуктов, потому что closures ниже (``_submit_collect``/``_finish_extract``)
    #: видят переменную ячейкой; цикл обнуляет тот же объект (:meth:`_CorpusStageTimings.reset`).
    timings = _CorpusStageTimings()
    _finish_extract = partial(
        corpus_commit.finish_extract,
        pools=pools,
        errors=errors,
        timings=timings,
        extract_timeout=_extract_timeout,
        extract_pending=extract_pending,
        extract_finalizers=extract_finalizers,
    )
    _finalize_ready_roots = partial(
        corpus_commit.finalize_ready_roots,
        extract_finalizers=extract_finalizers,
    )
    _recover_extract_pool = partial(
        corpus_commit.recover_extract_pool,
        pools=pools,
        errors=errors,
    )
    _abort_extract_pending = partial(
        corpus_commit.abort_extract_pending,
        extract_pending=extract_pending,
        extract_finalizers=extract_finalizers,
    )


    # D3: sitemap разворачивается через ``PollPort(kind="sitemap")`` (local in-process ‖
    # split через шов ``crawl/poll``) и кэшируется по ``(product, sitemap_url)``: сайт
    # опрашивается один раз, даже если у него несколько seed-URL.
    sitemap_cache: dict[tuple[str, str], list[str]] = {}
    #: ``<loc>``, уже прикреплённые к какому-либо seed-root продукта (дедуп между seeds).
    sitemap_seed_used: set[tuple[str, str]] = set()
    _expand_sitemap = partial(
        corpus_ingest.expand_sitemap,
        mark=_mark,
        emit=emit,
        errors=errors,
        pollers=pollers,
        sitemap_cache=sitemap_cache,
        targets=targets,
        applied_engines=applied_engines,
        domain_profiles=domain_profiles,
        resolve_target=_crawl_target,
    )
    _fresh_sitemap_seeds = partial(
        corpus_ingest.fresh_sitemap_seeds,
        expand_sitemap=_expand_sitemap,
        sitemap_seed_used=sitemap_seed_used,
    )


    # D1/G: accumulators честности усечения. ``truncation_records`` — детализация по
    # ``(product, seed-root)``; флаг/продукты уходят в провенанс сеанса и ``IndexBuild``.
    truncation_records: list[dict[str, Any]] = []
    truncated_products: set[str] = set()
    # L5: мутабельное состояние фазы — ровно те имена, что раньше текли через
    # ``nonlocal`` (накопители и флаги шага; смысл каждого поля — в
    # :class:`core.pipeline.corpus_state.CorpusCounters`). Один экземпляр на вызов:
    # замыкания читают ссылку и мутируют поля, поэтому ``nonlocal`` в теле не осталось.
    counters = CorpusCounters()
    _note_scheduling = partial(corpus_ingest.note_scheduling, counters=counters)
    _apply_revalidation_cap = partial(
        corpus_ingest.apply_revalidation_cap,
        state=state,
        errors=errors,
        limits_for=_limits_for_url,
        counters=counters,
    )
    _record_root_outcome = partial(
        corpus_commit.record_root_outcome,
        counters=counters,
        truncated_products=truncated_products,
        truncation_records=truncation_records,
        store=store,
        session=session,
        errors=errors,
    )
    _finalize_crawl_session = partial(
        corpus_commit.finalize_crawl_session,
        session=session,
        counters=counters,
        items=items,
        store=store,
        crawl_sources=crawl_sources,
        truncated_products=truncated_products,
        truncation_records=truncation_records,
        errors=errors,
        lifecycle=lifecycle,
    )


    #: B3: наблюдения спуска (прогноз + исход) и члены контейнеров — провенанс фазы.
    #: Списки нужны ``PhaseResult`` и финальному reconciliation-pass; durable-запись идёт
    #: **write-through** (синки волкера, F1), а не только пачкой в конце фазы.
    archive_observations: list[ArchiveObservation] = []
    corpus_member_rows: list[CorpusMember] = []
    #: B3: зонд артефактов (класс/формат/размер/``page_count``/листинг) — до экстракции.
    artifact_probes: list[dict[str, Any]] = []
    _ingest_one = partial(
        corpus_ingest.ingest_one,
        finish_one=_finish_one,
        extract_cache=extract_cache,
    )
    _feed_manual_artifact = partial(
        corpus_ingest.feed_manual_artifact,
        ingest_one=_ingest_one,
        retain=_retain,
        limits_for=_limits_for_url,
        resolve_target=_crawl_target,
        url_map=url_map,
        domain_profiles=domain_profiles,
        extractor=extractor,
        extractor_factory=extractor_factory,
        container_for=container_for,
        artifact_probes=artifact_probes,
        errors=errors,
    )


    #: Границы продукта в наблюдаемости (§B.1, аддитивно к фазовым start/end).
    #:
    #: Без них «продукт ещё в очереди» неотличим от «продукт потерян»: продукт-цикл
    #: строго последователен, и дренаж одного продукта может идти часами. Записи —
    #: **структурные** (dict) в ``PhaseResult.log_entries``: ``product_start`` до первого
    #: корня (``roots``/``seeds_declared``) и ``product_end`` после доводки хвостов
    #: (``roots``/``seeds_enqueued``/``session_items``/``fetched``/``failed``/
    #: ``elapsed_seconds``/``reason``/``partial_docs``). ``product_end`` пишется и при
    #: нулевом выходе — именно этот случай раньше был немым. Одна пара на продукт
    #: (не на URL).
    product_events: list[dict[str, Any]] = []

    #: Наблюдаемость эмбеддинга: измеренная пропускная способность — по строке на продукт
    #: (``N/M чанков, X чанк/с``). Это наблюдение о **цене** работы, а не о её составе:
    #: в ``run_id`` не входит (не измерение карты, A3), но оператор его видит.
    embed_notes: list[str] = []
    #: Причина zero-source покрытия по продукту (``None`` — причина не названа счётчиками).
    #: Заполняется на ``product_end`` и передаётся в :func:`_persist_corpus_coverage`.
    zero_source_reasons: dict[str, str | None] = {}
    #: Advisory-очередь «что осталось человеку» (``PhaseResult.pending``): zero-source
    #: продукты видны в конце фазы корпуса — **до** дорогой Ш6, а не только в отчёте.
    zero_source_pending: list[str] = []

    # --- Планирование партиций: аккумуляторы на весь прогон (09 §8.1) --------- #
    #: extractor_version: per-product композит из фактически извлечённых документов;
    #: fallback — объявленная версия экстрактора-порта (если ExtractedDoc не нёс версии).
    declared_extractor_version = str(getattr(extractor, "version", "") or "")
    #: ``product_id -> {partition_build_id, content_digest, extractor_version}``.
    #: Заполняется **на границе каждого продукта** (Pass B в цикле), потому что входы —
    #: только чанки/документы этого продукта (``pb`` продукт-скоупед, I17).
    product_plans: dict[str, dict[str, str]] = {}
    #: Члены/дайджесты композиции накапливаются по продуктам: ``composition_id`` зависит
    #: от **всего** состава когорты и выводится после всех продуктов.
    members: dict[str, str] = {}
    digests: dict[str, str] = {}
    #: Pass B-аккумуляторы (провенанс ``IndexBuild``); чанки и партиции фиксируются
    #: per-product, здесь — только то, что видит вызывающий по завершении фазы.
    #: Документы **не** накапливаются: их носитель — ``corpus_docs`` (Д-2/A4), а фазе нужен
    #: только счётчик прошедших документов.
    doc_count = 0
    chunk_count = 0
    partitions_reused = 0
    all_physical_ok = True
    #: ``product_id -> True``, если партиция продукта физически зафиксирована
    #: (``build``/переиспользование). Строка ``index_manifest`` пишется только для них.
    physically_ok: dict[str, bool] = {}

    # --- F0: наблюдаемость памяти и расписания (§4 F0) ----------------------- #
    #: Сток пика памяти воркеров приходит **готовым** от composition root
    #: (``{runs}/{run_id}/workers/{pid}.rss``): фаза не решает, собирается ли пик —
    #: ``None`` у thread-пула (``split``) и у пула без прогона. Фаза только доставляет
    #: путь в payload (:attr:`_ExtractRequest.rss_dir`) и читает максимум в конце.
    worker_rss_dir: str | None = (
        extraction_pool.worker_rss_dir if extraction_pool is not None else None
    )
    #: ``(url, chunk_count)`` по **каждому** документу сборки — вход распределения чанков
    #: на документ (:func:`core.pipeline.resources.summarize_chunks_per_doc`). Набирается
    #: на границе продукта из уже посчитанных ``ingest.docs``/``ingest.chunks``: никаких
    #: дополнительных SQL-запросов на документ.
    observed_documents: list[tuple[str, int]] = []

    #: 13-manual-artifacts §3.3: ``product_id → [url]``, чей материал **предъявлен**
    #: объявленным артефактом и дошёл до корпуса. Это вход ``derive_corpus_coverage``:
    #: отказ краула по такому URL остаётся аудитом, но перестаёт делать покрытие
    #: неполным. Объявляется до цикла — нужен и на границе продукта, и в конце фазы.
    manual_urls_by_product: dict[str, list[str]] = {}
    #: I2/I2c: ``product_id → [url]`` документов, прочитанных **не целиком**
    #: (``CorpusDoc.partial``). Объявляется до цикла по той же причине, что и
    #: ``manual_urls_by_product``: нужен и на границе продукта, и в конце фазы — строка
    #: покрытия перезаписывается дважды, и оба раза причина ``partial_extraction``
    #: обязана быть на месте. Накопитель, а не чтение БД: документы продукта уже в
    #: памяти, а ``corpus_docs`` не несёт ``run_id`` (общая БД — объединение корпусов).
    partial_urls_by_product: dict[str, list[str]] = {}
    #: Наблюдённые бакеты стадий по продуктам: ``(product_id, elapsed всего, бакеты)``.
    #: Копятся до конца фазы — агрегат публикуется рядом с наблюдением ресурсов
    #: (``metrics.json.resources.stage_timings``), а не только строками продуктов.
    stage_timings_rows: list[tuple[str, float, _CorpusStageTimings]] = []
    for product_id in products:
        product_started = time.perf_counter()
        #: Бакеты стадий этого продукта: Pass A (краул+нарезка) и Pass B (эмбеддинг+сборка).
        #: Объект один на фазу и обнуляется здесь — closures фазы видят его ячейкой, и
        #: переприсваивание внутри цикла было бы захватом переменной цикла.
        timings.reset()
        ingest = ProductIngest(product_id=product_id)
        # I17: дедуп в пределах продукт-партиции (см. docstring модуля).
        seen_docs: dict[str, str] = {}
        seen_chunk_ids: set[str] = set()
        #: F13: durable-URL, уже заряженные в resume-бюджет **продукта** (первый корень-
        #: претендент побеждает). URL, лежащий в scope нескольких корней, не должен
        #: расходовать ``max_documents`` каждого из них — иначе бюджет исчерпывается досрочно.
        resume_counted: set[str] = set()
        product_roots = url_map.get(product_id, [])
        #: 09 §6.3 (дефект 1): владелец известного URL считается **один раз на продукт** —
        #: тем же предикатом, что у группировки ревалидации (:func:`scope_owner`): побеждает
        #: самый специфичный ``root_path_prefix``, при равенстве — авторский порядок. Иначе
        #: реконструкция бюджета на resume отдавала бы зарядку первому корню-претенденту:
        #: host-корень (prefix ``/``) забирал её у docs-корня, потолок умножался на число
        #: перезапусков (измерено: 1610 ``fetched`` при потолке 800 = ``2 × 800``).
        owner_scopes = [
            (
                root,
                Frontier(
                    root,
                    CrawlLimits(),
                    scope_hosts=scope_hosts_by_product.get(product_id, frozenset()),
                ),
            )
            for root in product_roots
        ]
        owner_cache: dict[str, str | None] = {}

        # L5 шаг 2: ``owner_root`` вынесен в ``corpus_ingest``; ``partial`` привязывает
        # per-продуктное состояние при **создании** (а не поздним связыванием), поэтому
        # B023-дефолты больше не нужны — привязка видна в точке определения.
        _owner_root = partial(  # noqa: B023 — ``partial`` связывает значения при создании
            corpus_ingest.owner_root,
            owner_scopes=owner_scopes,
            owner_cache=owner_cache,
        )

        #: I3/§5: наблюдённая судьба SERP-находок этого продукта (``queued``/``dup``/
        #: ``blacklisted``/``truncated``). Пишется в провенанс контура ниже: «поиск нашёл»
        #: и «находка вошла в корпус» — разные факты, и второй нельзя выводить из первого.
        serp_ingest: dict[str, Any] = {
            "enqueued": 0,
            "dup": 0,
            "blacklisted": [],
            "truncated": [],
        }
        #: Находки, разложенные по корням этого продукта (:func:`assign_serp_seeds`).
        serp_seeds_by_root = dict(serp_seeds_by_product.get(str(product_id), {}))
        #: Граница продукта (start): до первого дренажа — «продукт начат», а не «потерян».
        product_events.append(
            {
                "event": "product_start",
                "product_id": product_id,
                "roots": len(product_roots),
                "seeds_declared": len(product_roots),
            }
        )
        #: Сколько seed'ов фактически поставлено в очередь (авторские + sitemap). Растёт
        #: в :func:`_drain_root`; нужен для честной причины zero-source. Сбрасывается на
        #: каждом продукте — поле общего состояния, а не локальная переменная.
        counters.product_seeds_enqueued = 0
        #: Живая граница продукта: до первого дренажа (продукт может идти часами).
        emit_product("start", product_id, on_product=on_product)
        # 16 §3.2: граница продукта — строка ``run_product`` в статусе ``running``. Пишется
        # до Pass A, потому что «продукт начат» и «продукт потерян» должны различаться
        # запросом, а не догадкой; финальный статус перезапишет её после Pass B.
        save_run_product(
            product_id,
            store=store,
            run_id=run_id,
            run_scope=run_scope,
            product_modes=product_modes,
            run_products_written=run_products_written,
            errors=errors,
            status=RunProductStatus.RUNNING,
        )
        #: 09 §6.3: известные URL продукта (``revalidate``) — после авторских корней.
        incremental_items = _plan_incremental_items(product_id, product_roots)
        #: 09 §6.3: группировка **всех** известных URL по авторским корням (scope
        #: inference) до применения операторского потолка. ``state is None``/
        #: не-incremental ⇒ пусто: grouping — no-op (байт-в-байт).
        if incremental_items:
            all_revalidation_groups, all_orphan_buckets = (
                _group_revalidation_items(
                    product_id, product_roots, incremental_items
                )
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
                # skipped_total: контракт возвращает, но потребителя нет — деталь
                # считается по ``skipped_by_root``/``skipped_by_origin`` (провенанс корня).
                _revalidation_skipped_total,
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
            serp_seeds: Sequence[str] = (),
            seed: bool = True,
            defer_tail: bool = True,
            # B023/F13: per-продуктное состояние привязывается при **определении** функции
            # (дефолт вычисляется в объемлющей области). ``_drain_root`` переопределяется
            # на каждой итерации цикла продуктов, а эти значения пересоздаются там же:
            # без привязки тело читало бы их поздним связыванием, то есть зависело бы от
            # порядка вызовов — а не от продукта, которому корень принадлежит.
            ingest: ProductIngest = ingest,
            seen_docs: dict[str, str] = seen_docs,
            seen_chunk_ids: set[str] = seen_chunk_ids,
            serp_ingest: dict[str, Any] = serp_ingest,
            resume_counted: set[str] = resume_counted,
            revalidation_url_set: set[str] = revalidation_url_set,
        ) -> tuple[Frontier, list[dict[str, Any]], RootDrain]:
            """D1/D3: BFS-drain одного seed-root (scope/блэклист/лимиты/глубина).

            Seeds — глубина 0; ссылка со страницы глубины ``d`` → ``d+1``. ``sitemap_seeds``
            — развернуть per-site sitemap корня в дополнительные depth-0 seeds (D3); они
            **подчинены** scope корня (``frontier.in_scope``), проходят тот же блэклист/
            бюджет ``max_documents``, что и seed оператора.
            ``revalidate`` — известные URL этого корня (scope inference): ставятся
            **после** авторского seed и **до** BFS отдельным путём
            (:meth:`Frontier.add_revalidate_seed`), поэтому не расходуют ``max_documents``,
            не расширяют BFS (листья) и дедуплицируются ``_seen`` с переоткрытием ссылкой.
            ``serp_seeds`` — документы, найденные внешним поиском (I3/§5): ставятся
            depth-0 seed'ами **этого** корня через :meth:`Frontier.add_seed`, то есть
            расходуют его ``max_documents`` и проходят блэклист, но не становятся новыми
            корнями. Так «доход» поиска платит из бюджета уже объявленного корня
            (нет умножения бюджета числом находок) и ``seeds_declared`` остаётся
            авторским.
            ``seed=False`` — synthetic-бакет (orphan): авторского seed нет, только
            ревалидация; усечение такого бакета не пишется вызывающим.
            Ставка в очередь гейтится scope (объявленный host + path-граница, C) и
            блэклистом; ``max_documents`` — жёсткий стоп (не исключение): останавливает
            постановку/обход этого корня.
            Возвращает frontier с провенансом усечения (``fetched``/``truncated``/
            ``truncated_reason``), агрегированный провенанс отказов scope у ``<loc>``
            (пусто без sitemap/без отказов) и :class:`RootDrain` — хвост экстракции корня
            в фазовой очереди (C1). ``defer_tail=False`` (synthetic-бакеты ревалидации,
            вызывающий читает счётчики frontier сразу) — хвост доводится здесь же.
            """
            limits = _limits_for_url(root_url)
            _note_scheduling(limits)
            frontier = Frontier(
                root_url, limits, scope_hosts=scope_hosts_by_product.get(pid, frozenset())
            )
            # D3/дефект B (I17): ``<loc>`` sitemap — кандидаты **этого** корня; scope-фильтр
            # применяется до дедупа ``(pid, loc)``. Дедуп-пометка — не здесь, а по факту
            # приёма в очередь (F12, см. цикл ``extra_seeds`` ниже). Без sitemap
            # ``_expand_sitemap`` отдаёт ``[]`` без обращения к поллеру — путь не исполняется
            # (поведение байт-в-байт).
            sitemap = (
                _fresh_sitemap_seeds(pid, root_url, frontier)
                if sitemap_seeds
                else SitemapSeeds()
            )
            extra_seeds = sitemap.fresh
            # P5: уровень извлечения документов — свойство корня (C1: ключ фазового пула
            # включает уровень — один конвертер на ключ). ``full`` переиспользует переданный
            # экстрактор (дефолт — без лишней загрузки весов); иной уровень строится фабрикой,
            # если она передана.
            level = limits.document_level
            # P7: механизм PDF — тоже свойство корня (ключ пула включает и его).
            pdf_pipeline = limits.pdf_pipeline
            root_extractor: ExtractorPort = extractor
            # Композит по умолчанию — ``full`` + ``structured``; переданный экстрактор и есть
            # эта пара, поэтому для неё «понижение до переданного» невозможно. Отказ OCR на
            # ``full`` приходит из адаптера на построении конвертера и становится
            # ``parser_failed`` (видимая запись ``errors`` через ``_finish_one``/
            # ``_finish_extract``), а не тихой подменой уровня.
            if extractor_factory is not None and (
                level is not DocumentLevel.FULL
                or pdf_pipeline is not PdfPipeline.STRUCTURED
            ):
                try:
                    root_extractor = extractor_factory(level, pdf_pipeline)
                except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                    errors.append(
                        f"{pid}/{root_url}: extractor_factory({level.value}, "
                        f"{pdf_pipeline.value}): {type(exc).__name__}: {exc} — "
                        "понижен до переданного экстрактора"
                    )
                    root_extractor = extractor

            def _record_document_skip(skip_url: str, reason: str) -> None:
                """P5: видимая запись пропуска документа (frontier + агрегат ``IndexBuild``).

                Параметр назван ``skip_url``, а не ``url``, намеренно: ``url`` —
                переменная цикла внешней области, и совпадение имён делало бы связывание
                неотличимым от позднего (B023).
                """
                frontier.note_document_skip(skip_url, reason)
                document_skips.append(
                    {
                        "product_id": pid,
                        "url": skip_url,
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
            #: C1/P7: ключ фазового пула — уровень документа + механизм PDF + окно
            #: экстракции. Пул один на прогон и переиспользуется всеми корнями/продуктами
            #: с тем же ключом (forkserver и веса конвертера платятся один раз);
            #: ``shutdown`` — один раз в ``finally`` фазы.
            #: Наблюдаемость: фактическое окно уходит в строку продукта рядом с
            #: ``extract_wait_s`` — иначе «ожидание» при окне > 1 читалось бы как работа.
            timings.note_extract_window(extract_window)
            pool_key: _ExtractPoolKey | None = (
                (level, pdf_pipeline, extract_window)
                if extract_window is not None
                else None
            )
            #: Исполнять ли экстракцию в пуле (снимается inline-fallback'ом при отказе).
            counters.defer_extract = (
                extract_window is not None and extraction_pool is not None
            )
            #: C1: хвост этого корня в фазовой очереди доводки (счётчик + финализатор).
            root_drain = RootDrain()
            # L5 шаг 2: ``partial`` привязывает состояние **этого** корня (окно, уровень,
            # механизм PDF, frontier) — то, что остаётся в ``_drain_root`` до шага 3.
            _new_extract_pool = partial(
                corpus_ingest.new_extract_pool,
                extraction_pool=extraction_pool,
                extract_window=extract_window,
                level=level,
                pdf_pipeline=pdf_pipeline,
                recycle_tasks=recycle_tasks,
            )
            _make_extract_request = partial(
                corpus_ingest.make_extract_request,
                extract_cache=extract_cache,
                level=level,
                pdf_pipeline=pdf_pipeline,
                worker_rss_dir=worker_rss_dir,
            )
            _inflight_fresh = partial(
                corpus_commit.inflight_fresh,
                extract_pending=extract_pending,
                frontier=frontier,
                root_drain=root_drain,
            )
            _has_fetch_capacity = partial(
                corpus_commit.has_fetch_capacity,
                frontier=frontier,
                limits=limits,
                extract_pending=extract_pending,
                root_drain=root_drain,
            )
            _tail_may_extend_frontier = partial(
                corpus_commit.tail_may_extend_frontier,
                extract_pending=extract_pending,
                frontier=frontier,
                limits=limits,
                root_drain=root_drain,
            )
            _drain_for_window = partial(
                corpus_commit.drain_for_window,
                extract_pending=extract_pending,
                extract_window=extract_window,
                frontier=frontier,
                limits=limits,
                root_drain=root_drain,
                pools=pools,
                errors=errors,
                timings=timings,
                extract_timeout=_extract_timeout,
                extract_finalizers=extract_finalizers,
            )
            # A2b/§6(b)/§10: egress-политика robots — **условна от discovery** и
            # вычисляется ядром **на корень** (edge строит краулеры «голыми»). Значение
            # резолвится **одной** формулой в :func:`resolve_crawl_limits` (app-слой
            # ``crawl.robots`` переопределяет автодетект; иначе robots включён ровно при
            # обходе ссылок ``max_depth > 0`` или наличии sitemap у сайта корня) и там же
            # входит в ``crawl_config_digest``: поведение и идентичность не расходятся.
            # Без discovery — ``False`` (никаких лишних fetch ``robots.txt``, байт-в-байт).
            robots_enabled = limits.robots
            # B2 (resume-бюджет): уже израсходованные страницы этого корня считаются из
            # durable-элементов сеанса (status FETCHED/FAILED с реально доступным retained
            # raw), чей URL в scope корня. Иначе повторный прогон того же session_id начал
            # бы ``fetched`` с нуля, заново обошёл resumed-страницу, превысил max_documents
            # и ложно финализировался COMPLETED. Считаем только resume-пригодные элементы
            # (fresh-фетч этого прогона инкрементит счётчик сам), чтобы не задвоить бюджет.
            #
            # F13: бюджет — **продукта**, а не корня. При нескольких корнях на host URL
            # в scope двух корней иначе заряжался бы обоим, и ``max_documents`` исчерпывался
            # бы досрочно на resume. Заряжает **владелец** — самый специфичный
            # ``root_path_prefix`` (``_owner_root``, тот же предикат, что у группировки
            # ревалидации), а не первый корень-претендент: иначе host-корень с префиксом
            # ``/`` забирал зарядку у docs-корня и потолок умножался на число перезапусков.
            # ``resume_counted`` остаётся продукт-wide страховкой от двойной зарядки.
            consumed = 0
            #: Заряженные этим корнем URL: каждый обязан быть и **дренируемым** (см. ниже).
            charged_urls: list[str] = []
            for (ipid, iurl), item in items.items():
                if ipid != pid or iurl in resume_counted:
                    continue
                # Известные URL ревалидации — вне бюджета discovery: иначе resumed
                # сессия заряжала бы ими ``max_documents``, seed стал бы ``truncated``
                # и продукт ложно ``complete=False`` (регрессия B2/F13 уровнем выше).
                if iurl in revalidation_url_set:
                    continue
                # B023-исключение: ``_owner_root`` пересоздаётся на каждом продукте
                # вместе с ``_drain_root`` (обе привязки — в одной итерации цикла), и
                # дренаж продукта завершается до следующей итерации, поэтому поздняя
                # привязка ячейки не подменяет владельца URL.
                owner = _owner_root(iurl)  # noqa: B023 — см. выше
                if owner is not None:
                    if owner != root_url:
                        # Владелец другой (более специфичный) корень — он и зарядит.
                        continue
                elif frontier.scope_admission(iurl) == SCOPE_OUT:
                    # URL не принадлежит ни одному авторскому корню (например, запись
                    # каталога feed/channel): прежний путь — корень, чей scope его
                    # принимает, заряжает сам, а ``resume_counted`` не даёт задвоить.
                    #
                    # P6b: терминальный артефакт вне path-prefix этим корнем тоже
                    # допускается, поэтому его resumed-элемент обязан заряжать бюджет —
                    # иначе повторный прогон тихо добывал бы лишнюю страницу сверх
                    # ``max_documents``.
                    continue
                counted = resume_retained(
                    item,
                    max_bytes=limits.max_response_bytes,
                    artifact_max_bytes=limits.max_artifact_bytes,
                    attachment_formats=limits.attachment_formats,
                )
                if not isinstance(counted, RetainedRaw):
                    # Нет пригодного resume (oversize/binary/несовпадение) — не бюджет.
                    continue
                consumed += 1
                resume_counted.add(iurl)
                charged_urls.append(iurl)
            frontier.note_consumed(consumed)
            seed_status = frontier.add_seed(root_url) if seed else "synthetic"
            if seed_status == "queued":
                counters.product_seeds_enqueued += 1
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
            # 09 §6.3 (дефект 3): **заряженный URL обязан быть и дренируемым.** Зарядка
            # бюджета (``consumed``) и работа расцеплены не должны быть: зарядка без
            # постановки в очередь платит бюджетом за работу, которой не будет. Поэтому
            # каждый заряженный элемент ставится depth-0 листом тем же путём, что и
            # ревалидация — **независимо от режима продукта**: предусловие набора «есть
            # durable-элементы для дренажа», а не «продукт инкрементальный». Измерено на
            # продукте, чей корень заряжался ровно в потолок 600/600 при 509 документах и
            # 25 чанках, тогда как 4935 чанков лежали в durable-субстрате: ``add_seed``
            # возвращал ``truncated``, дренаж выходил до обхода, и корпус терялся не сетью.
            #
            # Бюджет повторно **не** тратится: ``add_revalidate_seed`` ёмкость не проверяет,
            # а ``_commit_outcome`` считает такой fetch в ``revalidation_fetched``, не в
            # ``note_fetched`` (то есть ``max_documents`` не двигается).
            charged_queued = 0
            for known in charged_urls:
                if frontier.add_revalidate_seed(known) == "queued":
                    charged_queued += 1
            # Ранний выход — только если дренировать действительно нечего: заряженные URL
            # (``charged_queued``) считаются наравне с ревалидацией известных URL, иначе
            # усечённый корень терял бы уже добытое дерево.
            if (
                (seed_blacklisted or seed_truncated)
                and revalidation_queued == 0
                and charged_queued == 0
            ):
                emit()
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
                    if extra_status == "queued":
                        counters.product_seeds_enqueued += 1
                elif extra_status == "blacklisted":
                    errors.append(
                        f"{pid}/{extra}: sitemap-seed зачёркнут блэклистом "
                        "(exclude_paths/exclude_documents) — не enqueued"
                    )
                elif extra_status == "truncated":
                    errors.append(
                        f"{pid}/{extra}: sitemap-seed не поставлен — достигнут max_documents"
                    )

            # I3/§5: документы, найденные SERP-контуром, — depth-0 seeds **этого** корня.
            # Тот же путь, что у sitemap-`<loc>` (блэклист + ``max_documents`` + дедуп
            # ``_seen``), поэтому бюджет продукта не умножается на число находок, а
            # отказ виден (блэклист/потолок) — молчаливого дропа нет. Scope/SSRF хоста
            # уже проверены при допуске кандидата (``serp_discover``), path-prefix здесь
            # не применяется — как у терминальных артефактов: найденный документ не
            # продолжает навигацию корня, поэтому «раздел» к нему не относится.
            for found in serp_seeds:
                found_status = frontier.add_seed(found)
                if found_status == "queued":
                    counters.product_seeds_enqueued += 1
                    serp_ingest["enqueued"] += 1
                elif found_status == "dup":
                    serp_ingest["dup"] += 1
                elif found_status == "blacklisted":
                    serp_ingest["blacklisted"].append(found)
                    errors.append(
                        f"{pid}/{found}: serp-находка зачёркнута блэклистом "
                        "(exclude_paths/exclude_documents) — не enqueued"
                    )
                elif found_status == "truncated":
                    serp_ingest["truncated"].append(found)
                    errors.append(
                        f"{pid}/{found}: serp-находка не поставлена — достигнут max_documents"
                    )

            def _descend_container(
                pid: str,
                work_url: str,
                page: RawPage,
                raw_path: Path,
                *,
                reuse_kind: ReuseKind,
            ) -> None:
                """B3: спуск в ретейнённый контейнер (``plan`` — только листинг и прогноз).

                Байты архива уже в ``corpus_raw`` (I19); спуск читает **индекс** и стримит
                принятых членов по одному во временный файл. Каждый контейнер получает
                запись ``archive_observations`` — и пустой тоже: «спуск ничего не дал» не
                имеет права быть тише, чем «спуск дал десять документов».

                **Нечитаемый контейнер — блокирующий класс покрытия.** Если индекс не
                прочитан (битый файл, нет бэкенда, читатель не подключён), содержимое
                осталось ненаблюдённым, поэтому элемент помечается ``FAILED`` с
                существующим классом ``parser_failed`` (новый ``failure_kind`` не вводится,
                §2.3): ``❌`` на таком корпусе не был бы доказательством отсутствия.
                ``raw_path`` сохраняется — следующий прогон повторит спуск из тех же байтов.
                """
                archive_url = page.url or work_url
                #: Идентичность архива — хеш ретейненных байтов; ``_retain`` называет файл
                #: ``{sha256}.bin``, поэтому ``stem`` и есть этот хеш (I19).
                archive_hash = raw_path.stem

                # B3/P4: извлечение членов — в **тот же** фазовый пул, что и страницы.
                # Раньше 47 PDF внутри zip конвертировались в родителе по одному (вся
                # стоимость спуска на одном потоке, воркеры простаивали). Член дороже
                # страницы, поэтому окно упреждения = ``extract_window``: новая ось **не**
                # вводится намеренно — она попала бы в ``CrawlLimits.as_dict()`` →
                # ``crawl_control_digest`` → ``EXECUTION_CONTRACT_FIELDS`` и форкнула бы
                # ``run_id`` ради чистого рычага пропускной способности (§13: это не смена
                # условий измерения).
                defer_member: MemberDeferral | None = None
                if (
                    counters.defer_extract
                    and extraction_pool is not None
                    and pool_key is not None
                ):
                    #: Пул этого спуска признан мёртвым (``2 × timeout`` без завершений):
                    #: остаток членов — видимый пропуск, а не 47 ожиданий по таймауту.
                    counters.member_pool_dead = False

                    def _submit_member(task: MemberTask) -> Any:
                        """Submit извлечения члена; ``MemberOutcome`` — немедленный исход.

                        ``split`` проверяется **здесь**, в родителе: у воркера нет признака
                        ``supports_source_path``, и без этой проверки шов получил бы локальный
                        путь вместо URL (капитуляция была бы невидимой).
                        """
                        unavailable = _member_extract_unavailable(root_extractor)
                        if unavailable is not None:
                            return unavailable
                        if counters.member_pool_dead:
                            return MemberOutcome(
                                skip_reason=SKIP_EXTRACT_FAILED,
                                detail=(
                                    "пул экстракции не завершил ни одного задания за "
                                    f"{_EXTRACT_DEAD_POOL_TIMEOUTS} × таймаут — член не "
                                    "извлечён (inline после отказа пула запрещён)"
                                ),
                            )
                        if pools.abandoned(pool_key):
                            # Как у страниц: пул исчерпал пересборки — inline запрещён
                            # (docling в родителе на memory-constrained хосте = худший режим).
                            return MemberOutcome(
                                skip_reason=SKIP_EXTRACT_FAILED,
                                detail=(
                                    "extraction pool exhausted after "
                                    f"{_EXTRACT_MAX_POOL_REBUILDS} rebuilds — inline disabled"
                                ),
                            )
                        executor = pools.executor(pool_key, _new_extract_pool)
                        return executor.submit(
                            extraction_pool.worker,
                            _member_extract_request(
                                task,
                                fetched_at=page.fetched_at,
                                lang=page.lang,
                                level=level,
                                pdf_pipeline=pdf_pipeline,
                                rss_dir=worker_rss_dir,
                            ),
                        )

                    def _resolve_member(handle: Any, task: MemberTask) -> MemberOutcome:
                        """FIFO-доводка задания члена: future → ``_ingest_member_doc``.

                        Пересборкой мёртвого пула занимается **страничный** путь
                        (:func:`_finish_extract`): спуск исполняется внутри ``item.commit``,
                        где ``root.pending`` ещё включает сам архив, и рекурсивная доводка
                        той же фазовой очереди занизила бы/задвоила счётчики корней.
                        """
                        try:
                            _wait_started = time.perf_counter()
                            try:
                                extracted = handle.result(timeout=_extract_timeout)
                            finally:
                                # Тот же бакет, что и у страничной доводки: ожидание пула
                                # экстракции на главном потоке (член контейнера).
                                timings.extract_wait_s += time.perf_counter() - _wait_started
                        except TimeoutError:
                            counters.member_pool_dead = pools.note_timeout(pool_key)
                            return MemberOutcome(
                                skip_reason=SKIP_EXTRACT_FAILED,
                                detail=f"extraction timeout ({_extract_timeout} s)",
                            )
                        except BrokenProcessPool:
                            counters.member_pool_dead = True
                            return MemberOutcome(
                                skip_reason=SKIP_EXTRACT_FAILED,
                                detail="extraction worker died",
                            )
                        except Exception as exc:  # noqa: BLE001 — изоляция по контракту
                            return MemberOutcome(
                                skip_reason=SKIP_EXTRACT_FAILED,
                                detail=f"{type(exc).__name__}: {exc}",
                            )
                        # Операционная наблюдаемость: член — такое же доведённое задание
                        # пула, поэтому он входит в ``pools.completed`` (строка «документов
                        # через пул» в конце фазы) и в сброс серии таймаутов.
                        pools.note_completion(pool_key)
                        return _ingest_member_doc(
                            task,
                            extractor=root_extractor,
                            chunker=chunker,
                            ingest=ingest,
                            seen_docs=seen_docs,
                            seen_chunk_ids=seen_chunk_ids,
                            errors=errors,
                            store=store,
                            level=level,
                            fetched_at=page.fetched_at,
                            lang=page.lang,
                            artifact_probes=artifact_probes,
                            extracted=extracted,
                            timings=timings,
                            reuse_kind=reuse_kind,
                        )

                    defer_member = MemberDeferral(
                        submit=_submit_member, resolve=_resolve_member
                    )

                walker = ArchiveDescentWalker(
                    product_id=pid,
                    root_url=archive_url,
                    descent=limits.archive_descent,
                    budget=DescentBudget(
                        max_member_bytes=limits.archive_max_member_bytes,
                        max_uncompressed_bytes=limits.archive_max_uncompressed_bytes,
                        max_members=limits.archive_max_members,
                        max_total_uncompressed_bytes=(
                            limits.archive_max_total_uncompressed_bytes
                        ),
                    ),
                    container_for=container_for,
                    ingest_member=lambda task: _ingest_member_doc(
                        task,
                        extractor=root_extractor,
                        chunker=chunker,
                        ingest=ingest,
                        seen_docs=seen_docs,
                        seen_chunk_ids=seen_chunk_ids,
                        errors=errors,
                        store=store,
                        level=level,
                        fetched_at=page.fetched_at,
                        lang=page.lang,
                        artifact_probes=artifact_probes,
                        timings=timings,
                        reuse_kind=reuse_kind,
                    ),
                    observations=archive_observations,
                    members=corpus_member_rows,
                    errors=errors,
                    run_id=run_id,
                    created_at=_now_iso(),
                    defer_member=defer_member,
                    #: Окно упреждения = окно экстракции корня (``extract_workers``);
                    #: без пула окно не используется (inline-путь байт-в-байт).
                    window=extract_window or 1,
                    #: Write-through (F1): провенанс спуска durable на каждом шаге, а не в
                    #: конце фазы. Отказ стора изолируется в ``errors`` синком, поэтому
                    #: спуск продолжается (провенанс важнее, падение фазы — хуже).
                    persist_observation=_persist_observation_sync(store, errors),
                    persist_member=_persist_member_sync(store, errors),
                )
                observation = walker.descend(
                    archive_path=raw_path,
                    archive_url=archive_url,
                    archive_content_hash=archive_hash,
                )
                if observation.status is ArchiveDescentStatus.UNREADABLE:
                    reason = f"archive_unreadable: {observation.reason}"
                    _mark(
                        pid,
                        work_url,
                        CrawlItemStatus.FAILED,
                        error_kind=FailureKind.PARSER_FAILED.value,
                        error=reason,
                        raw_path=str(raw_path),
                    )

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
                # 16 §3.3: происхождение наблюдения берётся **из пути страницы этого
                # прогона**, а не из ``url_state`` (там последняя классификация, в том
                # числе из прошлого цикла). ``freshly_fetched=False`` — единственный путь
                # без сети: ``resume_page`` (retained-байты, ``not_modified`` у него
                # всегда ``False``). ``304`` — сервер ответил, версия не изменилась.
                reuse_kind = (
                    ReuseKind.REUSED
                    if not freshly_fetched
                    else (
                        ReuseKind.REVALIDATED
                        if page.not_modified
                        else ReuseKind.LIVE
                    )
                )
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
                    emit()
                    return
                # B3: допущенный терминальный артефакт (документ **или** контейнер) проходит
                # **зонд** до экстракции: класс/формат/размер по байтам, ``page_count`` PDF
                # по структуре файла, листинг контейнера — по индексу (central directory).
                # Зонд — классификация, а не извлечение: тела членов он не читает.
                if artifact_is_terminal(
                    page.url or work_url,
                    page.content_type,
                    fetch_plan.attachment_formats,
                ):
                    probe = probe_artifact(
                        raw_path,
                        product_id=pid,
                        url=page.url or work_url,
                        content_type=page.content_type,
                        container_for=container_for,
                    )
                    artifact_probes.append(probe.as_dict())
                    if probe.is_container:
                        # Контейнер — не документ: вместо экстракции исполняется спуск
                        # (``archive_descent=plan`` — только листинг и прогноз).
                        _descend_container(pid, work_url, page, raw_path, reuse_kind=reuse_kind)
                        emit()
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
                            reuse_kind=reuse_kind,
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
                            reuse_kind=reuse_kind,
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
                        reuse_kind=reuse_kind,
                        reuse=(
                            _reuse_for_page(pid, work_url, page)
                            if extract_error is None
                            else None
                        ),
                    )
                emit()
                # D1: извлечение in-site ссылок и следующий уровень BFS. При max_depth=0
                # извлечение не выполняется вовсе (байт-в-байт прежнее поведение).
                # 09 §6.3: ревалидация — лист; её ссылки не расширяют discovery (иначе
                # глубокая страница, пере-добытая на depth 0, меняла бы coverage/стоимость).
                if (
                    not is_revalidation
                    and limits.max_depth > 0
                    and looks_like_html(page)
                ):
                    frontier.offer(extract_links(page.content, page.url), depth)

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
                if outcome is None:
                    return
                page, raw_path, fetch_plan, freshly_fetched = outcome
                if counters.defer_extract and extract_cache is not None:
                    # C1/L4: проба — в родителе, до submit и до проверки исчерпания пула:
                    # готовый текст не требует экстракции вовсе. Ключ — ``raw_path.stem``
                    # (тот же хеш, что воркер считает из прочитанных байтов).
                    cached = extract_cache.get(
                        _extract_cache_key(raw_path),
                        _cache_extractor_version(
                            root_extractor,
                            _page_with_source_path(page, raw_path),
                            fetch_plan.extract,
                        ),
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
                if not counters.defer_extract:
                    _commit_outcome(pid_arg, url_arg, depth_arg, outcome)
                    return

                if pool_key is None:
                    # Инвариант ветки: ``counters.defer_extract`` истинно только при окне,
                    # а окно и есть ключ пула. Проверка явная — под ``-O`` assert исчез бы
                    # и ключ ушёл бы в ``pools.executor`` как ``None``.
                    raise AssertionError(
                        f"{pid_arg}/{url_arg}: defer_extract без ключа пула "
                        "(окно экстракции не задано)"
                    )
                try:
                    executor = pools.executor(pool_key, _new_extract_pool)
                except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                    errors.append(
                        f"{pid_arg}/{url_arg}: extract pool: "
                        f"{type(exc).__name__}: {exc} — inline"
                    )
                    counters.defer_extract = False
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
                    PendingExtract(
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
                        if prior_item is None:
                            # Инвариант сборки ``_WaveEntry(kind="attempts")``: запись
                            # создаётся только по существующему элементу сеанса.
                            raise AssertionError(
                                f"{entry.pid}/{entry.wu}: attempts-запись без prior_item"
                            )
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
                        emit()
                        continue
                    desc = entry.desc
                    if desc is None:
                        # Инвариант ``_WaveEntry``: не-attempts/не-блэклист запись несёт
                        # либо desc, либо future (и то и другое заполняется сборкой).
                        raise AssertionError(
                            f"{entry.pid}/{entry.wu}: запись волны без desc"
                        )
                    if entry.future is not None:
                        _fetch_started = time.perf_counter()
                        try:
                            page = entry.future.result()
                        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                            # ``fetch_wait_s`` — **ожидание** пула ``_local_fetch``; коммит
                            # (``_commit_local``) остаётся работой главного потока и в бакет
                            # не входит.
                            timings.fetch_wait_s += time.perf_counter() - _fetch_started
                            outcome = _commit_local(desc, exc=exc)
                        else:
                            timings.fetch_wait_s += time.perf_counter() - _fetch_started
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
                        # Здесь ``fetch_wait_s`` — **работа**, а не ожидание: живого пула нет,
                        # фетч (и коммит) исполняются на этом же потоке. Имя бакета общее с
                        # пуловым путём намеренно: «участок фетча» — одна стадия продукта.
                        _fetch_started = time.perf_counter()
                        try:
                            outcome = _fetch_and_commit(desc)
                        finally:
                            timings.fetch_wait_s += time.perf_counter() - _fetch_started
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
                    # D3: seeds sitemap ставятся пачкой; ``max_documents`` обязан
                    # ограничивать и их (не только обнаруженные ссылки). Без лимита —
                    # всегда есть ёмкость. Проверка — в голове цикла на главном потоке.
                    # 09 §6.3: ревалидация известных URL бюджет не расходует; если
                    # голова очереди — такая группа, дренаж продолжается (бюджет
                    # исчерпан только для discovery).
                    if (
                        not _has_fetch_capacity()
                        and not root_drain.pending
                        and not frontier.next_is_revalidation()
                    ):
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
                            emit()
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
                                emit()
                                continue
                            prior_item = items.get((pid, work_url))
                            if terminal_failure_kind(prior_item) is not None:
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
                                resume_retained(
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
                                if isinstance(resume_source, RetainedReject)
                                else None
                            )
                            resume = (
                                resume_source
                                if isinstance(resume_source, RetainedRaw)
                                else None
                            )
                            # Дескриптор коммита строится вручную (не ``_local_prefetch``):
                            # browser-guard/``active`` — оси локального краулера, а здесь
                            # байты доставляет исполнитель; ``browser_blocked`` всегда False.
                            desc = LocalFetch(
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
                        # ``fetch_wait_s`` split-пути: ``_submit_collect`` — это подача +
                        # опрос статуса + сбор результатов, то есть **ожидание** исполнителя
                        # (работу делает он, а не главный поток). Тот же бакет, что и у
                        # локального пула: «участок фетча» — одна стадия продукта.
                        _fetch_started = time.perf_counter()
                        try:
                            collected = _submit_collect(work)
                        finally:
                            timings.fetch_wait_s += time.perf_counter() - _fetch_started
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
                                    terminal_failure_kind(prior_item)
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
                                emit()
                                continue
                            if entry.kind == "attempts":
                                prior_item = entry.prior_item
                                if prior_item is None:
                                    # См. инвариант ``_WaveEntry(kind="attempts")`` выше:
                                    # запись создаётся только по существующему элементу.
                                    raise AssertionError(
                                        f"{entry.pid}/{entry.wu}: attempts-запись "
                                        "без prior_item"
                                    )
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
                                emit()
                                continue
                            desc = entry.desc
                            if desc is None:
                                # Инвариант ``_WaveEntry``: не-attempts/не-блэклист запись
                                # несёт desc — иначе коммитить нечего.
                                raise AssertionError(
                                    f"{entry.pid}/{entry.wu}: запись волны без desc"
                                )
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
                                emit()
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
                                emit()
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
                            emit()
                            continue
                        prior_item = items.get((pid, work_url))
                        if (
                            max_attempts is not None
                            and prior_item is not None
                            and prior_item.attempts >= max_attempts
                            and terminal_failure_kind(prior_item) is None
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
                            emit()
                            continue

                        work_target = _resolve_target(pid, work_url)
                        # Последовательный путь (``max_workers in (None, 1)``): фетч inline на
                        # главном потоке, поэтому ``fetch_wait_s`` здесь — **работа**, а не
                        # ожидание (как и inline-ветка волны). Бакет общий: «участок фетча» —
                        # одна стадия продукта.
                        _fetch_started = time.perf_counter()
                        try:
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
                        finally:
                            timings.fetch_wait_s += time.perf_counter() - _fetch_started
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
                            emit()
                            continue
                        prior_item = items.get((pid, work_url))
                        if (
                            max_attempts is not None
                            and prior_item is not None
                            and prior_item.attempts >= max_attempts
                            and terminal_failure_kind(prior_item) is None
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
                            if pool is None:
                                # Инвариант ветки: ``poolable`` выставляется только когда
                                # создан пул фетча этого корня (``executor is None`` путь
                                # пула не имеет вовсе).
                                raise AssertionError(
                                    f"{desc.pid}/{desc.wu}: poolable без пула фетча"
                                )
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
                and terminal_failure_kind(prior_item) is None
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
                emit()
                continue

            # D2/Lane A: единая цель краула резолвится в pipeline и передаётся и краулеру,
            # и экстрактору (``plan`` + идентификаторы ``engine``/``kind`` из слоёв источников).
            target = _crawl_target(product_id, url)
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
                    etarget = _crawl_target(product_id, entry_url)
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
                    # SERP-находки этого корня: depth-0 seeds под тем же блэклистом
                    # и ``max_documents``, что и авторские/`<loc>` seeds.
                    serp_seeds=serp_seeds_by_root.get(root_url, ()),
                )

                # L5 шаг 2: ``finalize_root`` вынесен в ``corpus_commit``; ``partial``
                # привязывает провенанс корня при создании финализатора — он живёт в
                # ``extract_finalizers`` и вызывается **позже** (после доводки хвоста),
                # то есть когда цикл продукта уже ушёл вперёд.
                _finalize_root = partial(
                    corpus_commit.finalize_root,
                    pid=product_id,
                    root_url=root_url,
                    frontier=frontier,
                    sitemap_skips=sitemap_skips,
                    known_urls=known_urls,
                    all_revalidation_groups=all_revalidation_groups,
                    cap_fired=revalidation_cap_fired,
                    deferred_by_root=deferred_by_root,
                    skipped_by_root=skipped_by_root,
                    counters=counters,
                    truncated_products=truncated_products,
                    truncation_records=truncation_records,
                    store=store,
                    session=session,
                    errors=errors,
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
            if frontier is not None and frontier.artifacts_outside_prefix_count:
                # P6b: видимость обхода path-prefix — рядом с ``skipped_blacklisted``.
                info["artifacts_outside_prefix"] = {
                    "count": frontier.artifacts_outside_prefix_count,
                    "examples": list(frontier.artifacts_outside_prefix),
                }
            revalidation_orphans[f"{product_id}:catalog:{group_root}"] = info
            if frontier is not None:
                counters.revalidation_fetched_total += frontier.revalidation_fetched

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
            if frontier is not None and frontier.artifacts_outside_prefix_count:
                # P6b: видимость обхода path-prefix — рядом с ``skipped_blacklisted``.
                info["artifacts_outside_prefix"] = {
                    "count": frontier.artifacts_outside_prefix_count,
                    "examples": list(frontier.artifacts_outside_prefix),
                }
            revalidation_orphans[f"{product_id}:{origin}"] = info
            revalidation_orphan_total += len(all_known_urls)
            if frontier is not None:
                counters.revalidation_fetched_total += frontier.revalidation_fetched
            # Targets известных URL резолвятся лениво в ``_drain_root``
            # (``_resolve_target``) — предварительная резолюция не нужна.

        # C1: хвосты экстракции продукта обязаны быть доведены до расчёта провенанса
        # (``truncation_records``/``counters.pages_fetched_total``/финализаторы корней). Пул —
        # фазовый, поэтому здесь же доводятся хвосты всех корней продукта (FIFO).
        while extract_pending:
            _finish_extract(extract_pending.popleft())
        _finalize_ready_roots()
        # -- 13-manual-artifacts §3: подача объявленных файлов оператора ---------- #
        # Порядок важен: **после** дренажа (отказы краула уже в элементах сеанса —
        # иначе аннотировать нечего) и **до** Pass B (документы входят в ту же сборку,
        # I19). Очередь и счётчики сеанса подача не трогает, поэтому ``fetched``
        # продукта по-прежнему означает «прочитано краулом».
        manual_entries = (
            manual_artifacts.for_product(product_id)
            if manual_artifacts is not None
            else ()
        )
        manual_resolved: list[str] = []
        for entry in manual_entries:
            doc_id, raw_path = _feed_manual_artifact(
                product_id, entry, ingest, seen_docs, seen_chunk_ids
            )
            if doc_id is not None:
                manual_resolved.append(entry.url)
            persist_manual_audit(
                entry,
                doc_id=doc_id,
                raw_path=raw_path,
                store=store,
                run_id=run_id,
                errors=errors,
            )
        if manual_resolved:
            manual_urls_by_product[product_id] = manual_resolved

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
        #: Граница продукта (end) — **всегда**, включая нулевой выход (раньше этот случай
        #: был немым). Счётчики — из персистентных элементов сеанса продукта; без сеанса
        #: (durable-краул выключен) ``session_items``/``fetched``/``failed`` — нули, а
        #: выход виден по ``docs`` (фактически собранные документы).
        product_items = (
            [
                item
                for (item_pid, _item_url), item in items.items()
                if item_pid == product_id
            ]
            if session is not None
            else []
        )
        product_fetched = sum(
            1 for item in product_items if item.status is CrawlItemStatus.FETCHED
        )
        product_failed = sum(
            1
            for item in product_items
            if item.status in (CrawlItemStatus.FAILED, CrawlItemStatus.CANCELLED)
        )
        product_reason: str | None = None
        # Zero-source — утверждение о **сеансе**: без сеанса «нет элементов» не значит
        # «нет источников» (счётчики просто не ведутся), поэтому причина не выдумывается.
        # ``and not manual_resolved``: продукт, чей материал предъявлен объявленным
        # артефактом, без источников **не** остался — иначе ошибка утверждала бы
        # пустой корпус при непустом.
        if session is not None and not product_items and not manual_resolved:
            # Истинная причина вместо единой константы ``no_seeds``: пустой seed-набор,
            # полностью отклонённые seed'ы и отсутствие записи в ``urls`` — разные пути.
            if product_id not in url_map:
                product_reason = ZERO_SOURCE_NO_URLS_ENTRY
            elif not product_roots:
                product_reason = ZERO_SOURCE_NO_SEEDS
            elif counters.product_seeds_enqueued == 0:
                product_reason = ZERO_SOURCE_ALL_SEEDS_REJECTED
            # Иначе seed'ы поставлены, но исходов нет — причину называют ``errors``
            # (сбои исполнителя/экстракции); выдумывать четвёртую причину не нужно.
            zero_source_reasons[product_id] = product_reason
            zero_source_pending.append(
                f"{product_id}: корпус без источников "
                f"({product_reason or 'no session items'}) — добавьте seed'ы "
                "(hound package seeds add) или проверьте отображение urls; "
                "оценка отметит все ячейки failed, а не ❌"
            )
            # Сегодня zero-source продукт не даёт ни одной записи ``errors`` — путь немой.
            errors.append(
                f"{product_id}: zero-source corpus "
                f"({product_reason or 'no session items'}): "
                f"roots={len(product_roots)}, "
                f"seeds_enqueued={counters.product_seeds_enqueued} — ячейки будут failed, не ❌"
            )
        # I2/I2c: документы, прочитанные **не целиком**. Признак уже в ``CorpusDoc`` —
        # здесь он только считается, чтобы попасть в две наблюдаемые точки: строку
        # продукта (``results[].notes``) и строку покрытия (``blocked`` с причиной
        # ``partial_extraction``). Считать из ``ingest.docs`` (память продукта), а не из
        # БД: ``corpus_docs`` не несёт ``run_id``, и в общей БД выборка была бы
        # объединением корпусов.
        product_partial = [doc.url for doc in ingest.docs if doc.partial]
        if product_partial:
            partial_urls_by_product[product_id] = product_partial
        # Наблюдаемость: окно Pass A закрывается **здесь** — ``elapsed_seconds`` считается
        # один раз, чтобы строка и остаток бакетов говорили об одном и том же интервале.
        product_crawl_elapsed = time.perf_counter() - product_started
        #: 16 §3.2: число документов продукта — снимается **до** освобождения
        #: ``ingest.docs`` на границе Pass B (дальше их носитель — ``corpus_docs``).
        #: Одна величина на две записи: строка ``product_end`` и ``run_product.docs_total``.
        product_docs_total = len(ingest.docs)
        product_events.append(
            {
                "event": "product_end",
                "product_id": product_id,
                "roots": len(drained_roots),
                "seeds_declared": len(product_roots),
                "seeds_enqueued": counters.product_seeds_enqueued,
                "session_items": len(product_items),
                "fetched": product_fetched,
                "failed": product_failed,
                "docs": product_docs_total,
                "partial_docs": len(product_partial),
                # v5: «признак частичности неизвестен» — отдельный факт, а не «полное».
                # Строка версии могла быть записана до v5 (признак не сохранялся), и
                # переиспользование такой версии не вправе молчать об этом.
                "partial_unknown": len(partial_unknown_products.get(product_id, [])),
                # v5: смены частичности (было частично → стало полное и обратно): тот же
                # ключ дал другой объём работы. Не ошибка, но обязано быть видно.
                "partiality_changes": [
                    {
                        "url": change["url"],
                        "was": change["was"],
                        "now": change["now"],
                    }
                    for change in partiality_changes
                    if change["product_id"] == product_id
                ],
                # 13-manual-artifacts §3.3: «объявлено» и «дошло до корпуса» —
                # разные факты, и разницу нельзя выводить один из другого.
                "manual_declared": len(manual_entries),
                "manual_resolved": len(manual_resolved),
                "elapsed_seconds": round(product_crawl_elapsed, 3),
                # Наблюдаемость стадий (**не** контракт): ожидание фетча/экстракции и работа
                # нарезки на главном потоке + остаток этого же окна. ``embed_s``/``build_s``
                # здесь нулями не выдаются: Pass B идёт после ``product_end`` и публикуется
                # событием ``product_index_end``.
                **timings.crawl_fields(product_crawl_elapsed),
                "reason": product_reason,
            }
        )
        #: Живая граница продукта: тело завершено — в том числе при нулевом выходе
        #: (именно этот случай раньше был немым до конца фазы).
        emit_product("end", product_id, on_product=on_product)

        # Наблюдаемость: последний продукт — конец **краула**. Сеанс закрывается здесь,
        # до Pass B этого продукта: эмбеддинг/сборка индекса — работа фазы, и терминальный
        # статус краула не должен от неё зависеть (иначе ``stale_reason`` врёт). Продукты идут
        # строго последовательно (Pass A каждого предшествует его Pass B), поэтому дренаж
        # всех продуктов закончен ровно на границе последнего.
        if product_id == products[-1]:
            _finalize_crawl_session()

        # --- Pass B (index) **этого** продукта (09 §8.1) ---------------------- #
        # Партиция планируется, эмбеддится, физически собирается и персистится сразу
        # после Pass A продукта, а не после всех продуктов. Иначе смерть на N-м
        # продукте уничтожала готовую работу предыдущих (чанки посчитаны — в БД их
        # нет). Все входы планирования — ``ingest.chunks``/``ingest.docs`` **только
        # этого** продукта (``pb`` продукт-скоупед, I17), поэтому перенос в цикл не
        # меняет ни один ``pb``; ``composition_id`` по-прежнему выводится после всех
        # продуктов (он зависит от состава когорты).
        product_chunks = ingest.chunks
        # F0: распределение чанков на документ — из **уже посчитанных** данных продукта
        # (``ingest.docs``/``ingest.chunks``). Никаких SQL-запросов на документ: чанки и
        # документы в памяти именно на этой границе, дальше они уходят в персистенцию.
        product_chunk_counts: dict[str, int] = {}
        for chunk in product_chunks:
            product_chunk_counts[chunk.doc_id] = (
                product_chunk_counts.get(chunk.doc_id, 0) + 1
            )
        for doc in ingest.docs:
            observed_documents.append(
                (doc.url, product_chunk_counts.get(doc.doc_id, 0))
            )
        physical_ok = True
        if product_chunks:
            chunk_count += len(product_chunks)
            product_token = _product_content_token(product_chunks)
            extractor_token = (
                _product_extractor_token(ingest.docs) or declared_extractor_version
            )
            partition_build_id = _derive_partition_build_id(
                package_id=package,
                product_id=product_id,
                chunker_version=resolved_chunker_version,
                embed_model_id=embed_model_id,
                # 16 §5.1: место счёта — часть условий сборки партиции.
                embed_device=embed_device,
                extractor_version=extractor_token,
                lexical_engine_version=lexical_engine_version,
                vector_store=vector_store,
                product_content_token=product_token,
            )
            product_plans[product_id] = {
                "partition_build_id": partition_build_id,
                "content_digest": product_token,
                "extractor_version": extractor_token,
            }
            members[product_id] = partition_build_id
            digests[product_id] = product_token
            # build_id и extractor_version чанка — по партиции (Pass B). ``build_id``
            # чанка = ``pb``: ``iter_chunks(pb, product_id=…)`` остаётся рабочим, а I17
            # держится структурно (``pb`` хеширует product_id + его content token).
            doc_versions = {doc.doc_id: doc.extractor_version for doc in ingest.docs}
            for chunk in product_chunks:
                chunk.build_id = partition_build_id
                chunk.extractor_version = doc_versions.get(chunk.doc_id)

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
                        embed_device=embed_device,
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
                    # Наблюдаемость: эмбеддинг продукта — единственный участок фазы, где
                    # нет дренажа, а значит и нет прежних точек жизни. Темп измеряется
                    # здесь, чтобы «сколько осталось» следовало из факта прогона.
                    throughput = _EmbedThroughput()
                    # Счётчики кэша — только при подключённом кэше: без него публиковать
                    # «из кэша 0» значило бы заявлять механизм, которого нет.
                    census = _EmbedCensus() if embed_cache is not None else None
                    physical_ok = _embed_and_build(
                        embedder,
                        index_builder,
                        product_id,
                        product_chunks,
                        package_id=package,
                        chunker_version=resolved_chunker_version,
                        embed_model_id=embed_model_id,
                        embed_device=embed_device,
                        extractor_version=extractor_token,
                        lexical_engine_version=lexical_engine_version,
                        vector_store=vector_store,
                        product_content_token=product_token,
                        snapshot_date=snapshot,
                        partition_build_id=partition_build_id,
                        errors=errors,
                        on_batch=_make_embed_reporter(
                            heartbeat=heartbeat,
                            progress=progress,
                            store=store,
                            session=session,
                            throughput=throughput,
                            census=census,
                        ),
                        cache=embed_cache,
                        census=census,
                        timings=timings,
                    )
                    # Пропускная способность и доля кэша — опубликованные числа, а не
                    # впечатление: оператор видит, во что обошёлся эмбеддинг продукта,
                    # сколько чанков пришло готовыми и может сверить темп с прошлым
                    # прогоном/другой моделью (A3).
                    embed_line = format_embed_progress(
                        len(product_chunks),
                        len(product_chunks),
                        throughput.rate,
                        0,
                        cached=None if census is None else census.cached,
                    )
                    embed_notes.append(
                        f"corpus: эмбеддинг {product_id}: {embed_line} "
                        f"за {throughput.elapsed:.0f} s"
                    )

            # «Disk first, DB second»: физическая сборка авторитетна. Пока build этого
            # продукта не завершился успешно, ``corpus_chunks`` не пишутся (K2/§13).
            # ``index_manifest`` пишется позже (его PK включает композицию) — и только
            # для продуктов с ``physically_ok=True``.
            if physical_ok:
                if not _persist_chunks(store, product_chunks, errors):
                    all_physical_ok = False
            else:
                all_physical_ok = False
                errors.append(
                    f"{product_id}: физическая сборка индекса не зафиксирована — "
                    "corpus_chunks/index_manifest не записаны (K2/§13); "
                    "изменение корпуса требует нового run_id/раунда"
                )
        physically_ok[product_id] = physical_ok
        # A3/Д-2: граница продукта — конец удержания продукта в памяти. Всё, что
        # считается из ``ingest.docs``/``ingest.chunks`` (распределение чанков на документ,
        # продукт-токен, ``pb``, версии чанков, признак ``partial``), уже посчитано выше,
        # а чанки записаны (или отказ виден в ``errors``). Пост-циклу нужна сводка —
        # документы и чанки дальше существуют только в БД. ``product_chunks`` отсоединяется
        # от объекта, а затем и локальная ссылка снимается: иначе список чанков прожил бы
        # до следующей итерации, то есть ровно тот пик, который снимается.
        doc_count += len(ingest.docs)
        ingest.chunk_count = len(product_chunks)
        ingest.distinct_docs = len(product_chunk_counts)
        ingest.docs = []
        ingest.chunks = []
        del product_chunks
        del product_chunk_counts

        # Наблюдаемость: окно Pass B этого продукта закрыто. Пять бакетов вместе с
        # ``elapsed_seconds`` Pass A дают полное окно продукта, и остаток
        # (``overhead_s`` = всё окно − Σ пяти бакетов) считается по формуле, а не зажимается
        # в ноль: отрицательное значение — видимый дефект инструментирования.
        product_total_elapsed = time.perf_counter() - product_started
        stage_timings_rows.append((product_id, product_total_elapsed, replace(timings)))
        product_events.append(
            {
                "event": "product_index_end",
                "product_id": product_id,
                "index_elapsed_s": round(
                    product_total_elapsed - product_crawl_elapsed, 3
                ),
                "total_elapsed_s": round(product_total_elapsed, 3),
                "extract_workers": timings.extract_workers,
                **timings.index_fields(product_total_elapsed),
            }
        )

        # Ф0/O3: покрытие продукта пишется на его границе, а не только в конце фазы.
        # Элементы сеанса продукта **финальны** (продукт-цикл строго последователен),
        # поэтому строка честна уже здесь: продукт, дошедший до ``product_end``,
        # сохраняет покрытие, даже если следующий продукт убьёт прогон. Конец фазы
        # перезаписывает строки (``INSERT OR REPLACE``) и остаётся авторитетным — он же
        # закрывает продукты, вообще не вошедшие в цикл (zero-source/coverage_missing).
        product_coverage: CorpusCoverage | None = None
        if session is not None:
            try:
                written_coverage = _persist_corpus_coverage(
                    store,
                    run_id,
                    session,
                    truncated_products,
                    _now_iso(),
                    products=[product_id],
                    reasons=zero_source_reasons,
                    resolved_manually=manual_urls_by_product,
                    partial_docs=partial_urls_by_product,
                )
                # 16 §3.2: ``coverage_complete`` строки продукта — та же величина, что
                # легла в ``corpus_coverage``, а не второй её расчёт.
                product_coverage = written_coverage.get(product_id)
            except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
                errors.append(
                    f"corpus_coverage({product_id}): {type(exc).__name__}: {exc}"
                )

        # 16 §3.2: финальная строка продукта — после Pass B (там же, где ``corpus_coverage``).
        # Числа — уже посчитанные фазой величины: документы (``product_docs_total`` и
        # ``doc_counters``), полнота покрытия (``CorpusCoverage``), статус — из
        # :func:`_final_product_status`. ``cells_total``/``cells_pending`` остаются нулями:
        # на границе ``corpus`` ячеек ещё нет (их считает Ш6), и переписывать колонку из
        # другой фазы нельзя — она означает «на границе corpus столько».
        _product_counters = doc_counters.get(product_id) or {}
        _docs_reused = int(_product_counters.get("reused", 0))
        save_run_product(
            product_id,
            store=store,
            run_id=run_id,
            run_scope=run_scope,
            product_modes=product_modes,
            run_products_written=run_products_written,
            errors=errors,
            status=_final_product_status(
                zero_source_reason=product_reason,
                physical_ok=physical_ok,
                coverage=product_coverage,
            ),
            coverage_complete=(
                None if product_coverage is None else product_coverage.complete
            ),
            docs_total=product_docs_total,
            # «Новое» = всё, что не переиспользовано: члены контейнеров и ручные
            # артефакты проходят экстракцию в этом прогоне, и отдельного счётчика
            # «reused» у них нет (``count_doc`` вызывается только для страниц).
            docs_new=max(0, product_docs_total - _docs_reused),
            docs_reused=_docs_reused,
            error_kind=product_reason,
        )

        # Retention-провенанс не зависит от индексной сборки и уже персистенчен
        # инкрементально: единственная точка ``ingest.docs.append`` — ``_finish_one``,
        # где ``_persist_docs`` вызывается сразу после коммита документа. Финального
        # «дампа» продукта здесь нет намеренно: он лишь повторно писал бы те же строки
        # (``INSERT OR REPLACE`` по PK) и удваивал бы транзакции без выигрыша в durability.
        ingests.append(ingest)
        if serp_seeds_by_root:
            # Наблюдение контура: «нашли» и «вошло в корпус» — разные факты; второй
            # нельзя выводить из первого (блэклист/потолок могут его отменить).
            serp_ingest_by_product[str(product_id)] = {
                "enqueued": int(serp_ingest["enqueued"]),
                "dup": int(serp_ingest["dup"]),
                "blacklisted": list(serp_ingest["blacklisted"]),
                "truncated": list(serp_ingest["truncated"]),
            }

    # Продукт плана **без записи** в непустом ``urls``: в цикл он не попал (``products``
    # выводится из ``url_map``), поэтому молча не краулится и получает строку покрытия
    # только как ``coverage_missing`` в Ш6. Наблюдаемость называет причину здесь, до
    # оценки. Coverage-строку не пишем намеренно: ``--only-product``/``--only-failed``
    # сужают ``urls``, и zero-source запись для продукта вне области прогона была бы
    # ложной (09 §6.2).
    if url_map:
        for missing_id in sorted(set(declared) - set(url_map)):
            product_events.append(
                {
                    "event": "product_skipped",
                    "product_id": missing_id,
                    "roots": 0,
                    "seeds_declared": 0,
                    "seeds_enqueued": 0,
                    "session_items": 0,
                    "fetched": 0,
                    "failed": 0,
                    "elapsed_seconds": 0.0,
                    "reason": ZERO_SOURCE_NO_URLS_ENTRY,
                }
            )

    # Наблюдаемость: сеанс краула уже закрыт на границе краула — на ``product_end`` последнего
    # продукта (``_finalize_crawl_session`` идемпотентен). Здесь вызов нужен только для
    # пустого ``products``: фазы без продуктов, где дренажа нет вовсе. Пост-краульная
    # работа (манифест, композиция) ниже на терминальный статус уже не влияет.
    _finalize_crawl_session()

    # Ф0: покрытие корпуса продукта — первоклассный факт для гейта ❌ (I2c).
    # Производно от **персистентных** элементов сеанса и записей об усечении; пишется
    # после финализации, чтобы провенанс/усечение уже были зафиксированы. Это факт о
    # корпусе, а не о статусе сеанса, поэтому остаётся на границе фазы (пишется поверх
    # per-product строк и закрывает продукты, вообще не вошедшие в цикл).
    if session is not None:
        try:
            end_coverage = _persist_corpus_coverage(
                store,
                run_id,
                session,
                truncated_products,
                _now_iso(),
                products=products,
                reasons=zero_source_reasons,
                resolved_manually=manual_urls_by_product,
                # I2/I2c: та же карта, что на границе продукта — конец фазы
                # перезаписывает строки покрытия и обязан сохранить причину.
                partial_docs=partial_urls_by_product,
            )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(
                f"corpus_coverage({session}): {type(exc).__name__}: {exc}"
            )
        else:
            # 16 §3.2: «переписывается в конце фазы» — здесь закрываются продукты, **не
            # вошедшие в цикл** (нет записи ``urls``, срез ``--only-product``): у них не
            # было границы, и без этой строки «продукт не начинался» неотличим от
            # «продукта не было в прогоне». Строка, написанная на границе, не трогается:
            # она несёт измеренные числа, а здесь их взять неоткуда. Статус — ``blocked``
            # при названной причине zero-source, иначе ``pending`` (фаза закончилась, не
            # начав продукт; ``failed`` был бы утверждением о сбое, которого не было).
            for pid in sorted(set(declared) | set(products)):
                if pid in run_products_written:
                    continue
                # Причина zero-source считается циклом только для вошедших в него
                # продуктов; здесь её отсутствие у объявленного продукта означает ровно
                # одно — корпусного входа нет (``urls`` его не назвал).
                zero_reason = zero_source_reasons.get(pid)
                if zero_reason is None and pid not in url_map:
                    zero_reason = ZERO_SOURCE_NO_URLS_ENTRY
                coverage = end_coverage.get(pid)
                save_run_product(
                    pid,
                    store=store,
                    run_id=run_id,
                    run_scope=run_scope,
                    product_modes=product_modes,
                    run_products_written=run_products_written,
                    errors=errors,
                    status=(
                        RunProductStatus.BLOCKED
                        if zero_reason is not None
                        else RunProductStatus.PENDING
                    ),
                    coverage_complete=(
                        None if coverage is None else coverage.complete
                    ),
                    error_kind=zero_reason,
                )

    # §4 (13-manual-artifacts): список **недоступных** артефактов — заполняемый вход
    # следующего прогона, а не справка. Ставится здесь, на границе фазы: оператор
    # останавливается на ``--until corpus`` и обязан увидеть пробел до дорогой Ш6, а не
    # после ``report`` (до него ещё ``evaluate``). Источник — персистентные элементы
    # сеанса, поэтому повтор (фаза ``report``) даёт тот же файл.
    #
    # Best-effort: отчёт вторичен к фазе, его сбой уходит в ``errors`` (видимо), но не
    # роняет фазу — как и сбой покрытия выше. ``out_dir is None`` — прямой вызов модуля
    # без определённого каталога прогонов: писать некуда, артефакт не создаётся.
    gap_line: str | None = None
    if out_dir is not None and store is not None and run_id is not None:
        try:
            gap_report = write_artifacts_gap(
                store, run_id=run_id, out_dir=out_dir, session_id=session
            )
        except Exception as exc:  # noqa: BLE001 — отчёт не роняет фазу
            errors.append(f"artifacts_gap: {type(exc).__name__}: {exc}")
        else:
            gap_line = f"corpus: {gap_report.human_line()}"

    # Дальше — пост-краульная работа фазы (манифест, композиция). Терминальный статус
    # сеанса выше уже зафиксирован и от неё не зависит.
    emit()

    # --- Манифест и композиция (09 §8.1) ------------------------------------- #
    # Партиции уже физически зафиксированы, а их чанки персистентны (Pass B шёл в
    # цикле продуктов). Здесь остаётся только то, что зависит от **всего** состава
    # когорты: ``composition_id``, строки ``index_manifest`` (их PK включает
    # композицию) и иммутабельный файл композиции.
    #
    # ``composition_id`` — единственный ``runs.index_build_id`` (09 §8.1). Он зависит
    # только от состава когорты и версий/дайджестов партиций; ``params_token``/
    # ``snapshot_date``/``retrieval_*`` не входят.
    index_build_id = derive_composition_id(package, members, digests)

    entries: list[IndexManifestEntry] = []
    #: Записи, чьи партиции физически зафиксированы: только они идут в реестр прогона.
    manifest_entries: list[IndexManifestEntry] = []
    for ingest in ingests:
        if not ingest.chunk_count:
            continue
        product_id = ingest.product_id
        plan = product_plans[product_id]
        partition_build_id = plan["partition_build_id"]
        product_token = plan["content_digest"]
        extractor_token = plan["extractor_version"]

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
                    # Сводка продукта, снятая на его границе (A3): сами чанки к этому
                    # моменту давно в БД, и в памяти их нет.
                    "chunks": ingest.chunk_count,
                    "docs": ingest.distinct_docs,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
        # ``entry`` остаётся в ``entries`` и при отказе сборки: это провенанс/log
        # (``IndexBuild``). В реестр (``index_manifest``) идёт только зафиксированная
        # партиция — «disk first, DB second» (K2/§13).
        entries.append(entry)
        if physically_ok.get(product_id, False):
            manifest_entries.append(entry)

    # Строки манифеста — после того, как композиция известна (PK включает
    # ``index_build_id``) и партиции уже на диске; чанки персистентны ещё с границы
    # продукта. Сбой записи снимает композицию (она не должна ссылаться на партицию,
    # которой нет в реестре прогона).
    if not _persist_manifest(store, manifest_entries, errors):
        all_physical_ok = False

    # K2/§13: ячейки, привязанные к **прошлой** сборке этого же прогона, помечаются
    # устаревшими. Живой случай — повтор фазы на существующем прогоне (``--reset``/
    # ``--from-scratch``) или повторный раунд: вердикт относится к ушедшей сборке, и
    # публиковать его как измерение текущей нельзя. Сами ячейки не удаляются и статус
    # не меняется (I12): помеченные исключаются из расчёта и видны в отчёте как
    # «требует пере-оценки». При обычном прогоне ячеек ещё нет — вызов no-op.
    stale_notes = _invalidate_previous_build_cells(
        store, run_id, index_build_id, errors
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

    # --- B3: reconciliation-pass спуска в контейнеры (прогноз/исход + члены) --- #
    # **Страховка, а не единственная запись (F1).** Durable-запись уже произошла
    # write-through'ом: ``persist_observation``/``persist_member`` пишут строку сразу за
    # её появлением в волкере, поэтому SIGKILL/OOM теряет максимум контейнер (наблюдение)
    # или член (строка) «в полёте». Здесь — идемпотентный повторный upsert: он добирает
    # то, что не дошло (отказ стора на горячем пути виден в ``errors`` и лечится здесь),
    # и дёшев, потому что ключи детерминированы (``observation_id``) либо уже UPSERT
    # (``corpus_members`` по ``(archive_content_hash, member_path, product_id)``).
    # Сбой записи изолируется в ``errors`` (контракт фазы): провенанс важнее, но падение
    # фазы — хуже.
    if store is not None and run_id is not None and archive_observations:
        try:
            for observation in archive_observations:
                store.save_archive_observation(observation)
        except Exception as exc:  # noqa: BLE001 — изоляция персистенции
            errors.append(f"archive_observations: {type(exc).__name__}: {exc}")
        try:
            store.save_corpus_members(corpus_member_rows)
        except Exception as exc:  # noqa: BLE001 — изоляция персистенции
            errors.append(f"corpus_members: {type(exc).__name__}: {exc}")

    # SERP-контур: наблюдение в durable-провенанс сеанса (ключ ``serp``) — источник фактов
    # для I3-протокола (``phases._observed_exhaustion_facts``) и для ``search_effort``
    # манифеста. Пишется **после** финализации сеанса: ``finalize_session`` перезаписывает
    # ``provenance_json`` целиком.
    if serp_observation.get("enabled") and serp_seeds_by_product:
        serp_observation["ingest"] = {
            product: serp_ingest_by_product.get(product, {"enqueued": 0})
            for product in sorted(serp_seeds_by_product)
        }
    _persist_serp_provenance(store, session, serp_observation, errors)

    build = IndexBuild(
        index_build_id=index_build_id,
        entries=entries,
        doc_count=doc_count,
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
    #: Границы продуктов (start/end/skipped) идут рядом со сборкой: структурные dict-записи
    #: контракта фазы (``PhaseResult.log_entries``), одна пара на продукт. Машинный
    #: потребитель берёт dict, оператор — **строку** ниже: ``results[].notes`` пропускает
    #: только ``str``, и без строки продукт-скоуп остался бы в поле, которое никто не читает
    #: (``log_entries`` не сериализуется ни в один файл). Строка — одна на продукт, поэтому
    #: заметки растут линейно по продуктам, а не по URL.
    #:
    #: Строки появляются **только при активном сеансе**: границы продукта — утверждение о
    #: durable-сеансе (``session_items``/``fetched``/``failed``), а без сеанса счётчиков не
    #: существует и заметка была бы выдуманной. Это же условие держит инвариант «``state is
    #: None`` — прежняя форма фазы» (``test_state_none_keeps_previous_truncation_shape``):
    #: прямой вызов без стора не приобретает новых строк.
    #: Строка оператору нужна, когда контур что-то делал или был объявлен и не исполнен;
    #: выключенный контур виден структурной записью (``enabled: false`` + источник).
    serp_note_needed = bool(serp_observation.get("enabled")) or bool(
        serp_observation.get("unavailable_reason")
    )
    log_entries: list[Any] = [
        build,
        *product_events,
        *(_product_notes(product_events) if session is not None else []),
        # 13-manual-artifacts §4: «N документов недоступно: …» — одной строкой, чтобы
        # оператор узнал о пробеле из вывода фазы, а не из чтения файла.
        *([gap_line] if gap_line else []),
        *embed_notes,
        # I3/§5: наблюдение SERP-контура — структурная запись (машинному потребителю) и
        # строка (оператору). Пишется и при выключенном контуре: «выключено, источник X»
        # отличимо от «контур был, но ничего не дал».
        serp_observation,
        # Строка — только когда контур действительно исполнялся (или объявлен, но не
        # исполнен): выключенный контур уже назван структурной записью выше, а тихий
        # прогон не должен обрастать декоративными строками (их отсутствие — часть
        # контракта наблюдаемости: `test_state_none_keeps_previous_truncation_shape`).
        *([_serp_note(serp_observation)] if serp_note_needed else []),
        # K2/§13: пометка ячеек прошлой сборки — наблюдение фазы (очередь пере-оценки),
        # а не ошибка: корпус собран, вердикты прошлой сборки опубликованы не будут.
        *stale_notes,
    ]
    # B3: провенанс спуска доступен из результата фазы (и из run DB — таблицы
    # ``archive_observations``/``corpus_members``). Запись есть даже при нулевом выходе.
    for observation in archive_observations:
        log_entries.append(observation.model_dump(mode="json"))
        log_entries.append(
            "archive: "
            f"{observation.archive_url} (depth={observation.depth}): "
            f"{observation.status.value}, "
            f"members={observation.outcome.get('members_accepted', 0)}/"
            f"{observation.outcome.get('members_total', 0)}, "
            f"ingested={observation.outcome.get('members_ingested', 0)}, "
            f"skipped={observation.outcome.get('members_skipped', 0)}"
            + (
                f", stopped={observation.outcome['stopped_reason']}"
                if observation.outcome.get("stopped_reason")
                else ""
            )
        )
    # B3: зонд артефактов (класс/формат/размер/``page_count``/листинг) — до экстракции.
    log_entries.extend(artifact_probes)
    if revalidation_orphan_total:
        # Наблюдение, не ошибка и не смена статуса (I12): авторский scope разошёлся с
        # тем, что реально накопилось в ``url_state``.
        log_entries.append(
            f"corpus: {revalidation_orphan_total} known URLs are outside every authored "
            "root — author-scope drift (revalidated as orphans; observation only)"
        )
    if pools.completed:
        # Операционный рычаг памяти виден оператору (не «магия»): воркер-процесс после
        # каждого документа возвращает ОС свободные арены glibc (``malloc_trim(0)``,
        # ``core/util/memory.py``). Один trim на доведённое задание, поэтому счётчик фазы
        # — это и число trim-вызовов в воркерах. Не ``errors``: ужатие heap — норма,
        # не сбой; ``run_id``/контракт от рычага не зависят.
        log_entries.append(
            "corpus: экстракция — trim арен (malloc_trim) в воркере после каждого "
            f"документа; документов через пул: {pools.completed}"
        )
    # F0: наблюдённые ресурсные факты фазы — расписание экстракции, чанки на документ и
    # пик воркеров пула (если он собирался). Пики RSS родителя по фазам меряет
    # наблюдаемость (:mod:`core.pipeline.observability`), здесь их нет по построению.
    log_entries.append(
        ResourcesObservation(
            chunks_per_doc=summarize_chunks_per_doc(observed_documents),
            documents=len(observed_documents),
            chunks=chunk_count,
            extract_workers=counters.observed_extract_workers,
            extract_max_tasks_per_child=counters.observed_extract_max_tasks,
            scheduling_source=(
                "derived" if counters.observed_extract_derived else "declared"
            ),
            workers_peak_rss_mb=(
                extraction_pool.peak_worker_rss_mb()
                if extraction_pool is not None and worker_rss_dir
                else None
            ),
            # Наблюдаемость стадий (не контракт): бакеты по продуктам и их сумма. Без
            # агрегата «где уходит время» пришлось бы складывать строки продуктов руками.
            stage_timings=_stage_timings_aggregate(stage_timings_rows),
        )
    )
    return PhaseResult(
        status="ok",
        phase="corpus",
        cells=[],
        log_entries=log_entries,
        errors=errors,
        # Advisory (не влияет на status/гейты/resume): zero-source продукты — «что осталось
        # человеку» уже на выходе Ш5, до дорогой Ш6. Видно в ``hound run`` (``pending:``),
        # ``--json`` (``results[].pending``) и в ``runs/{run_id}/logs/corpus.jsonl``
        # (запись конца фазы несёт ``pending``/``pending_items``).
        pending=zero_source_pending,
    )


def _finalize_interrupted_session(
    lifecycle: PhaseLifecycle, exc: BaseException
) -> None:
    """Закрывает сеанс краула терминальным статусом после прерывания фазы.

    **Почему не** :func:`core.pipeline.crawl_session.finalize_session`. Он выводит статус
    из ``store.crawl_session_counts``: ``completed``, если нет failed/cancelled, иначе
    ``partial``. Для прерванного прогона оба ответа — утверждения о **теле** краула,
    которых никто не проверял: убитый на середине прогон обычно имеет ``pending > 0``,
    и ``partial`` («часть краула добыта» — значение, которым уже помечается политическое
    усечение и частичный отказ) описал бы его неверно, а при нуле добытого — прямо
    ложно. Кроме того ``finalize_session`` пишет ``provenance_json`` целиком: он заявил
    бы нормальную финализацию с ``pages_fetched=None``/``engines=[]``/``truncated=false``,
    то есть выдумал бы провенанс о прогоне, который не дошёл до конца.

    Поэтому ставится **терминальный статус из словаря**
    (``docs/status-dictionary.yaml: crawl_session_status``) без записи провенанса —
    ``interrupted``: «исполнение прервано, тело краула не финализировано». Это ровно тот
    случай, для которого значение введено (``10`` §8.2): факт прерывания известен
    обработчику размотки, а ``partial`` занят усечением и ``failed`` — заявлением о сбое,
    которого конвейер не наблюдал. Различить «оператор снял» и «фаза упала» статус не
    обязан — причина остаётся в ``error`` (тип исключения), а ``cancel`` оператора
    оставляет свой след в ``error``.

    Сбой самой записи не поднимается: обработчик не имеет права подменить исходное
    исключение (оно и есть причина прерывания).
    """
    # Сбой записи не поднимается (см. докстринг): исходное исключение важнее. След не
    # теряется — незакрытый ``running`` увидит :mod:`core.pipeline.liveness` при открытии
    # сеанса и переведёт в ``interrupted`` со ``stale_reason`` (в ``crawl status/list``).
    with contextlib.suppress(Exception):
        lifecycle.store.update_crawl_session_status(
            lifecycle.session_id,
            CrawlSessionStatus.INTERRUPTED,
            finished_at=_now_iso(),
            error=f"phase interrupted: corpus did not complete ({type(exc).__name__})",
        )


def build_corpus(*args: Any, **kwargs: Any) -> PhaseResult:
    """Ш5 (публичная точка фазы): ``_build_corpus`` + жизненный цикл фазовых пулов.

    C1/L1: пул экстракции — **один на прогон** (ключ ``(DocumentLevel, window)`` в
    :class:`_ExtractPools`), поэтому ``shutdown(wait=True, cancel_futures=True)``
    выполняется ровно один раз — здесь, в ``finally`` фазы, а не на каждый seed-root.

    Здесь же закрывается durable-сеанс краула, если фаза **не дошла** до штатного
    финала: иначе убитый прогон навсегда оставлял бы ``status='running'``, и
    ``hound crawl status`` показывал бы «в полёте» мёртвый краул. Штатный путь не
    меняется: сеанс финализирует сама ``_build_corpus`` (``completed``/``partial``),
    и ``lifecycle.completed`` снимает вмешательство. Это единственные две отличия
    обёртки от :func:`_build_corpus`; аргументы и поведение фазы — прежние (см.
    docstring :func:`_build_corpus`).
    """
    pools = _ExtractPools(max_rebuilds=_EXTRACT_MAX_POOL_REBUILDS)
    lifecycle = PhaseLifecycle()
    try:
        return _build_corpus(*args, pools=pools, lifecycle=lifecycle, **kwargs)
    except BaseException as exc:
        # Прерывание (исключение/``KeyboardInterrupt``/``SystemExit``): исключение
        # перехватывается только чтобы узнать его тип (см.
        # :func:`_finalize_interrupted_session`) и сразу переподнимается — фаза не
        # «проглатывает» сбой.
        if lifecycle.session_id is not None and not lifecycle.completed:
            _finalize_interrupted_session(lifecycle, exc)
        raise
    finally:
        pools.shutdown()

