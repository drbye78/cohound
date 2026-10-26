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
``_ProductIngest`` в памяти; ``corpus_docs`` персистятся **инкрементально** — сразу при
коммите каждого документа (retention-провенанс не зависит от сборки), поэтому жёсткое
убийство теряет максимум документ «в полёте», а не весь продукт; ``build_id`` чанков пока
``None``. Pass B (index) идёт **на границе каждого продукта**, сразу после его Pass A:
выводятся ``partition_build_id`` (входы — только чанки/документы этого продукта, ``pb``
продукт-скоупед, I17), ``build_id = pb`` проставляется чанкам, партиция либо
переиспользуется (ноль эмбеддингов, 09 §8.5), либо собирается ``_embed_and_build``; при
успехе **сразу** персистятся ``corpus_chunks`` («сначала диск, потом БД»). Иначе смерть на
N-м продукте уничтожала бы посчитанную, но не персистентную работу предыдущих. Только
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

import datetime as _dt
import errno
import hashlib
import json
import operator
import os
import shutil
import time
import uuid
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
    FailureKind,
    IncrementalKind,
    IndexManifestEntry,
    PdfPipeline,
    ProductMode,
    RefreshPolicy,
    ReuseKind,
)
from core.pipeline.crawl_frontier import (
    SCOPE_ARTIFACT_OUTSIDE_PREFIX,
    SCOPE_OUT,
    CrawlLimits,
    Frontier,
    is_blacklisted,
    normalize_scope_host,
    resolve_crawl_limits,
)
from core.pipeline.archive_descent import (
    ArchiveDescentWalker,
    DescentBudget,
    MemberDeferral,
    MemberOutcome,
    MemberTask,
    SKIP_EXTRACT_FAILED,
    SKIP_EXTRACT_UNAVAILABLE,
)
from core.pipeline.artifact_probe import probe_artifact
from core.pipeline.artifacts_gap import write_artifacts_gap
from core.pipeline.crawl_session import (
    default_session_id,
    finalize_session,
    open_session,
    prior_items,
    read_provenance,
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
from core.pipeline.embed_cache import EmbedCache
from core.pipeline.extract_cache import ExtractionCache
from core.pipeline.fetch_plan import _host_matches, resolve_fetch_plan, select_domain_profile
from core.pipeline.incremental import (
    IncrementalItem,
    chunk_row_id,
    classify_reuse,
    doc_version_id,
    plan_incremental,
)
from core.pipeline.liveness import Heartbeat
from core.pipeline.manual_artifacts import (
    ORIGIN_CRAWL,
    ORIGIN_MANUAL,
    ManualArtifact,
    ManualArtifacts,
    artifact_record,
)
from core.pipeline.resources import ResourcesObservation, summarize_chunks_per_doc
from core.pipeline.runner import PhaseResult
from core.ports.chunker import ChunkerPort
from core.ports.container import ContainerPort
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
from core.util import memory as _memory
from core.util.document_formats import (
    BINARY_SNIFF_BYTES,
    DOCUMENT_SUFFIXES,
    document_content_type,
    document_suffix,
    is_terminal_artifact,
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

#: Наблюдаемость эмбеддинга: число чанков в одном батче **на уровне фазы**
#: (Pass B, :func:`_embed_and_build`).
#:
#: Почему внешний батч, если :meth:`EmbedderPort.embed` режет вход сам: внутренняя нарезка
#: адаптера — его деталь, и между её батчами у фазы нет точки наблюдения. Один вызов на весь
#: продукт — это участок без признака жизни: 2026-10-25 прогон простоял 24 минуты на 505 % CPU
#: с неподвижным heartbeat, и «работает» было неотличимо от «повесился». Внешний батч даёт
#: границу, на которой фаза пишет heartbeat и счётчик прогресса.
#:
#: Величина — компромисс двух ограничений:
#:
#: * **Наблюдаемость.** Дефолтный ``chunk_size`` — 1500 символов; замер этого хоста
#:   (``docs/reports/measurements/2026-10-25-run-stall-embedding.md``) — 1173 мс/чанк. 64 чанка
#:   = окно ~75 с между точками жизни. Порог stale сеанса — 600 с, то есть запас восьмикратный,
#:   и оператор с ``--follow`` видит движение раз в батч, а не «никогда».
#: * **Память.** Тексты батча материализуются по мере обхода (список ссылок на
#:   ``chunk.text`` не строится на весь продукт), векторы батча добавляются к накопителю.
#:   Накопитель векторов всё равно полон: партиция собирается одним вызовом
#:   ``IndexBuilderPort.build(chunks, vectors, meta)``, который требует все векторы сразу —
#:   это контракт порта, а не выбор корпуса (пик памяти ограничивается там, не здесь).
#:
#: Кратность 32 (внутренний батч ``LocalEmbedder``) держит нарезку совпадающей с внутренней,
#: поэтому результат побитово тот же, что у одного вызова (детерминизм, A3).
EMBED_BATCH_CHUNKS = 64

#: Наблюдаемость эмбеддинга: вес нового замера в экспоненциальном сглаживании темпа эмбеддинга
#: (:class:`_EmbedThroughput`). 0.3 — темп реагирует на смену длины чанков за 3–4 батча, но
#: не скачет от одного медленного батча (первый батч включает ленивую загрузку модели).
_EMBED_RATE_ALPHA = 0.3

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
    manual_urls: Iterable[str] = (),
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

    **Ручной артефакт закрывает пробел, а не отказ (13-manual-artifacts §3.3).**
    ``manual_urls`` — URL, чей материал **предъявлен** объявленным артефактом
    (``corpus_docs.origin=manual``). Такой блокирующий ``FAILED`` остаётся в ``blocked`` и
    ``partial_blocks`` (аудит: краулу отказано), но больше не делает покрытие неполным и не
    делает origin «нечитаемым»: материал наблюдён, хоть и другим путём. Их число —
    :attr:`CorpusCoverage.resolved_manually`. Заменяется не факт, а его последствие для
    полноты: карта не должна выглядеть так, будто краул прочитал сайт.

    **Человеческий escape hatch.** ``scope_attested_by`` остаётся единственным способом
    опубликовать ``❌`` на неполном корпусе: человек осознанно принимает риск, что
    ненаблюдённая часть сайта могла бы содержать параметр. Машинный ``❌`` в этом случае
    понижается до ``pending``-кандидата (I2c/I6/I7). Где материал предъявлен, подпись риска
    не нужна — нужен документ.
    """
    expected = 0
    observed = 0
    blocked: dict[str, int] = {}
    fetched_by_origin: dict[str, int] = {}
    #: origin → число «недостач» (не-FETCHED исходов, кроме ``not_found``).
    losses_by_origin: dict[str, int] = {}
    partial_blocks: dict[str, dict[str, int]] = {}
    resolved_urls = {str(url) for url in manual_urls}
    resolved_manually = 0
    blocking_values = {kind.value for kind in BLOCKING_FAILURE_KINDS}
    blocking_failures = False
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
        # Ручной артефакт (§3.3): отказ **остаётся** аудитом (``blocked`` уже посчитан
        # выше, ``partial_blocks`` — здесь), но перестаёт быть потерей: материал наблюдён
        # другим путём, поэтому origin не «нечитаем», а покрытие не «неполно».
        if str(item.url) in resolved_urls:
            if kind:
                counts = partial_blocks.setdefault(origin, {})
                counts[kind] = counts.get(kind, 0) + 1
                # Счётчик называет **закрытые пробелы**, поэтому считает блокирующие
                # классы: неблокирующий отказ (``source_unavailable``/``not_found``/
                # ``redirect_out_of_scope``) пробела не открывал, и приписывать его
                # подаче значило бы завышать её вклад.
                if kind in blocking_values:
                    resolved_manually += 1
            continue
        losses_by_origin[origin] = losses_by_origin.get(origin, 0) + 1
        if kind:
            counts = partial_blocks.setdefault(origin, {})
            counts[kind] = counts.get(kind, 0) + 1
            if kind in blocking_values:
                blocking_failures = True

    unreadable_origins = sorted(
        origin for origin in losses_by_origin if fetched_by_origin.get(origin, 0) == 0
    )
    # 13-manual-artifacts §3.3: «ни одного наблюдения» — не только про краул. Материал,
    # предъявленный человеком, наблюдён; ``observed_sources`` при этом остаётся счётчиком
    # **краула** (строка не должна утверждать, что сайт прочитан), а ручная часть названа
    # отдельно — ``resolved_manually``.
    observed_any = observed > 0 or resolved_manually > 0
    return CorpusCoverage(
        product_id=product_id,
        complete=(not truncated)
        and not unreadable_origins
        and not blocking_failures
        and observed_any,
        expected_sources=expected,
        observed_sources=observed,
        blocked=dict(sorted(blocked.items())),
        unreadable_origins=unreadable_origins,
        partial_blocks={
            origin: dict(sorted(partial_blocks[origin].items()))
            for origin in sorted(partial_blocks)
        },
        truncated=truncated,
        resolved_manually=resolved_manually,
        computed_at=computed_at,
    )


#: Машинные причины zero-source покрытия (``CorpusCoverage.reason``).
#:
#: ``reason`` — **наблюдательное** поле (свободная строка в модели и колонка TEXT в БД),
#: не перечисление ``docs/status-dictionary.yaml``: расширение значений не меняет смысл
#: правил покрытия и не является методологической границей (§13). ``failure_kind`` ячеек
#: остаётся ``no_sources`` (:func:`core.pipeline.evaluate.evaluate_phase`), поэтому
#: словарь классов сбоя не расширяется.
ZERO_SOURCE_NO_SEEDS = "no_seeds"
"""Список seed'ов продукта пуст (ключ ``urls`` есть, значений нет) либо ``urls`` не задан."""
ZERO_SOURCE_ALL_SEEDS_REJECTED = "all_seeds_rejected"
"""Seed'ы были, но ни один не поставлен в очередь: блэклист/``max_documents``/пустая
развёртка источника-каталога. Конкретный URL называет per-seed запись ``errors``."""
ZERO_SOURCE_NO_URLS_ENTRY = "no_urls_entry"
"""У продукта плана нет записи в отображении ``urls`` вовсе."""


def _product_notes(events: Sequence[Mapping[str, Any]]) -> list[str]:
    """Строковые заметки фазы по границам продуктов (операторский канал).

    ``log_entries`` фазы **не сериализуется** ни в один файл: CLI превращает в
    ``results[].notes`` только ``str``-записи. Поэтому структурные dict-события
    (``product_start``/``product_end``/``product_skipped``) сами по себе оператору
    невидимы — здесь они дублируются одной строкой на продукт. Это не ``errors``:
    продукт с бюджетом ``N`` и нулём отказов — норма, а не сбой.

    Строка ``product_end`` называет ровно те величины, которых раньше не было видно
    (сколько корней/seed'ов/элементов/документов и сколько это заняло), поэтому
    «продукт съел весь бюджет» и «продукт почти ничего не дал» перестают выглядеть
    одинаково в отчёте. ``product_skipped`` — единственный след продукта плана, вообще
    не попавшего в цикл (``--only-product``/``--only-failed``/пустой ``urls``).

    :param events: структурные записи границ продукта (порядок сохраняется).
    :returns: заметки в порядке событий; ``product_start`` не дублируется (граница
        «начат» нужна живьём, в отчёте она бесполезна).
    """
    notes: list[str] = []
    for event in events:
        kind = event.get("event")
        if kind == "product_skipped":
            notes.append(
                f"corpus: продукт {event.get('product_id')} пропущен — нет записи в "
                "urls (фильтр --only-product/--only-failed или пустой seed-набор)"
            )
        elif kind == "product_end":
            reason = event.get("reason")
            tail = f", причина {reason}" if reason else ""
            notes.append(
                f"corpus: продукт {event.get('product_id')} — "
                f"корней {event.get('roots')}, seed'ов поставлено "
                f"{event.get('seeds_enqueued')} из {event.get('seeds_declared')}, "
                f"элементов {event.get('session_items')} "
                f"(fetched {event.get('fetched')}, failed {event.get('failed')}), "
                f"документов {event.get('docs')}, "
                f"{event.get('elapsed_seconds')} с{tail}"
            )
    return notes

def _persist_corpus_coverage(
    store: Any,
    run_id: str,
    session_id: str,
    truncated_products: set[str],
    computed_at: str,
    *,
    products: Iterable[str] | None = None,
    reasons: Mapping[str, str | None] | None = None,
    resolved_manually: Mapping[str, Iterable[str]] | None = None,
) -> None:
    """Вычисляет и персистит покрытие **каждого** продукта плана (Ф0, O3).

    Продукт без элементов сеанса (нулевой seed-набор / preflight обойдён / все seed'ы
    отклонены) получает строку ``expected_sources=0, complete=False`` и **истинную**
    причину в ``reason`` — отсутствие покрытия не должно читаться как «неизвестно» и не
    должно давать тихий ``ok``. Значения — :data:`ZERO_SOURCE_NO_SEEDS`,
    :data:`ZERO_SOURCE_ALL_SEEDS_REJECTED`, :data:`ZERO_SOURCE_NO_URLS_ENTRY`;
    вызывающий (:func:`_build_corpus`) знает фактический путь, поэтому передаёт карту
    ``reasons`` (продукт → причина). Без карты (прямой вызов/старые тесты) — прежнее
    ``no_seeds``: значение по умолчанию не придумывает новую причину.
    Продукт с элементами, но без единого ``FETCHED`` (все ``not_found``/``pending``),
    получает ``complete=False`` из :func:`derive_corpus_coverage` с ``reason=None``:
    неполноту объясняют счётчики, а не новая причина.

    ``resolved_manually`` — ``product_id → [url]``, чей материал предъявлен объявленным
    ручным артефактом (13-manual-artifacts §3.3): отказ остаётся аудитом, но перестаёт
    делать покрытие неполным. Значение по умолчанию — пусто (поведение прежнее).
    """
    by_product: dict[str, list[CrawlSessionItem]] = {}
    for item in store.iter_crawl_session_items(session_id):
        by_product.setdefault(item.product_id, []).append(item)
    planned = {str(product_id) for product_id in (products or ())}
    known_reasons = dict(reasons or {})
    for product_id in sorted(planned | set(by_product)):
        items = by_product.get(product_id, [])
        if not items:
            coverage = CorpusCoverage(
                product_id=product_id,
                complete=False,
                expected_sources=0,
                observed_sources=0,
                computed_at=computed_at,
                # Ключ, присутствующий со значением ``None``, — честное «причина
                # счётчиками не названа»; отсутствие ключа — прежний ``no_seeds``.
                reason=known_reasons.get(product_id, ZERO_SOURCE_NO_SEEDS),
            )
        else:
            coverage = derive_corpus_coverage(
                product_id=product_id,
                items=items,
                truncated=product_id in truncated_products,
                computed_at=computed_at,
                # 13-manual-artifacts §3.3: URL, чей материал предъявлен артефактом.
                manual_urls=(resolved_manually or {}).get(product_id, ()),
            )
        store.save_corpus_coverage(run_id, coverage)


#: Ключ фазового пула экстракции (C1/P7): ``(уровень документов, механизм PDF, окно)``.
#: Обе оси — свойства корня и определяют, каким движком будет извлечён документ, поэтому
#: ключ обязан их различать: переиспользовать пул другого механизма значило бы извлечь
#: текст не тем движком, чем объявлено (провенанс ``extractor_version``).
_ExtractPoolKey = tuple[DocumentLevel, PdfPipeline, int]


@dataclass(frozen=True)
class _ExtractRequest:
    """P4: payload одного задания экстракции — **только примитивы** (picklable by construction).

    Байты страницы **не** передаются: retention (I19) уже положил их в ``raw_path``; воркер
    перечитывает этот файл **потоково** (только для хеша кэша) и реконструирует ``RawPage``
    с ``source_path`` и пустым ``content`` — тело документа не поднимается в RAM воркера.
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
    #: ключ ``(уровень, механизм PDF, окно)``, поэтому воркер кэширует конвертер по ключу.
    level: DocumentLevel = DocumentLevel.FULL
    #: P7: механизм извлечения PDF (``crawl.pdf_pipeline``). Едет в payload по той же
    #: причине, что и уровень: воркер обязан обслужить задание **своим** механизмом, а не
    #: тем, что случайно построил инициализатор другого пула.
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED
    #: Ф1.2: корень и бюджет межпрогонного кэша экстракции. Едут **примитивами** — сам
    #: объект кэша не пиклится (payload остаётся picklable by construction); воркер
    #: строит экземпляр из этих полей. ``None`` → кэш в воркере выключен.
    cache_root: str | None = None
    cache_max_bytes: int | None = None
    #: F0: каталог стока пика памяти воркера (``{runs}/{run_id}/workers``). Воркер пишет
    #: туда свой ``ru_maxrss``; ``None`` — пик не собирается (thread-пул/inline). Поле
    #: едет в payload, а не в результат: контракт ``_ExtractRequest → ExtractedDoc``
    #: остаётся портовым, наблюдаемость не попадает в документ.
    rss_dir: str | None = None


@dataclass(frozen=True)
class ExtractionPool:
    """P4: инъекция исполнителя экстракции из composition root (A1, ``pipeline ↛ adapters``).

    :param pool_factory: фабрика ``(int, DocumentLevel, *, PdfPipeline) → Executor``
        (размер = ``limits.extract_workers``; уровень и механизм PDF — свойства корня, C1:
        пул — фазовый по ключу ``(уровень, механизм, окно)``); ``local`` —
        ``ProcessPoolExecutor`` (forkserver, инициализатор строит экстрактор нужного уровня
        и механизма на воркер), ``split`` — ``ThreadPoolExecutor`` (``RemoteExtractor``
        I/O-bound; оси ``document_level``/``pdf_pipeline`` под ``split`` не действуют — R-S).
    :param worker: рабочая функция ``_ExtractRequest → ExtractedDoc``. Для процессов —
        module-level (picklable by reference); для потоков допустим bound/closure.
    :param supports_worker_recycling: профиль умеет рециклинг воркеров по счётчику заданий
        (``ProcessPoolExecutor(max_tasks_per_child=N)``). ``False`` (``split``/потоки) —
        ось ``extract_max_tasks_per_child`` не действует (не тихо: запись в ``errors``).
    :param worker_rss_dir: F0 — каталог стока пика памяти воркеров
        (``{runs}/{run_id}/workers``). Каждый воркер-процесс после задания пишет туда свой
        ``ru_maxrss`` (файл на ``pid``), родитель читает максимум
        (:meth:`peak_worker_rss_mb`). ``None`` — пик не собирается (``split``: воркеры на
        edge; потоковый пул: ``ru_maxrss`` потока — это RSS процесса, а не воркера).

    **Файл-сток, а не канал.** Пик воркера собирается файлом на ``pid`` в каталоге прогона,
    а не через ``Manager``/очередь/поле результата:

    * контракт ``_ExtractRequest → ExtractedDoc`` остаётся прежним — документ не обязан
      нести наблюдаемость, а ``ExtractedDoc`` не получает поля, которого нет у порта;
    * ``Manager`` поднимает лишний процесс и канал (память ровно там, где её считают), а
      очередь требует слива после каждого задания — то есть работу на каждом документе;
    * воркеры **рециклятся** (``extract_max_tasks_per_child``), поэтому файл на ``pid``
      перезаписывается новым процессом с тем же ``pid``: берётся максимум по всем файлам,
      то есть по всем поколениям воркеров;
    * запись атомарна (``tmp`` + ``replace``) — родитель не прочитает половину числа.

    Сток пишется в ``finally`` задания: пик нужен и у задания, упавшего на извлечении
    (OOM-подобный отказ — ровно тот случай, ради которого метрика и собирается).
    """

    pool_factory: Callable[..., Executor]
    worker: Callable[[_ExtractRequest], ExtractedDoc]
    supports_worker_recycling: bool = False
    worker_rss_dir: str | None = None

    def peak_worker_rss_mb(self) -> float | None:
        """Максимум ``ru_maxrss`` по воркерам пула, MB; ``None`` — не собирали/не было.

        Читает файлы сточных записей (:attr:`worker_rss_dir`); максимум берётся по **всем**
        файлам, потому что ``pid`` переиспользуется при рециклинге воркеров, а имя файла —
        это ``pid`` поколения, а не поколение. Нечитаемый/битый файл пропускается: метрика
        не выдумывается и не роняет прогон. ``None`` возвращается и при пустом каталоге
        (пул не создавался либо заданий не было) — «не измеряли» ≠ «ноль».
        """
        if not self.worker_rss_dir:
            return None
        try:
            paths = list(Path(self.worker_rss_dir).glob("*.rss"))
        except OSError:  # pragma: no cover — каталог недоступен
            return None
        peak: float | None = None
        for path in paths:
            try:
                value = float(path.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):  # pragma: no cover — битая запись
                continue
            # Неположительный RSS — не факт, а мусор в стоке (отрицательного пика памяти
            # не бывает). Он отбрасывается ровно как нечитаемая запись: метрика не
            # выдумывается и не может оказаться отрицательной.
            if value <= 0:
                continue
            if peak is None or value > peak:
                peak = value
        return peak


#: P4: глобальный экстрактор воркер-процесса. Устанавливается инициализатором пула
#: (composition root строит адаптер **внутри** процесса — объекты не пересекают границу).
_WORKER_EXTRACTOR: ExtractorPort | None = None
#: P5: уровень глобального экстрактора воркера (``None`` — «подходит для любого», split).
_WORKER_EXTRACTOR_LEVEL: DocumentLevel | None = None
#: P7: механизм извлечения PDF глобального экстрактора воркера (``None`` — «любой», split).
_WORKER_EXTRACTOR_PIPELINE: PdfPipeline | None = None
#: P5/P7: фабрика экстрактора ``(level, pdf_pipeline)`` в воркере (защитный lazy-путь, если
#: пара задания не совпала с инициализированной). Передаётся ссылкой из composition root.
_WORKER_EXTRACTOR_FACTORY: (
    Callable[[DocumentLevel, PdfPipeline], ExtractorPort] | None
) = None


def set_worker_extractor(
    extractor: ExtractorPort | None,
    *,
    level: DocumentLevel | None = None,
    factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort] | None = None,
    pdf_pipeline: PdfPipeline | None = None,
) -> None:
    """Устанавливает экстрактор воркер-процесса (зовётся ``initializer`` пула, P4/P5/P7).

    ``level``/``pdf_pipeline`` — оси установленного экстрактора (``None`` — универсальный,
    напр. split); ``factory`` — фабрика ``(level, pdf_pipeline)``, если задание принесло
    иную пару (защитный путь).
    """
    global _WORKER_EXTRACTOR, _WORKER_EXTRACTOR_LEVEL, _WORKER_EXTRACTOR_PIPELINE
    global _WORKER_EXTRACTOR_FACTORY
    _WORKER_EXTRACTOR = extractor
    _WORKER_EXTRACTOR_LEVEL = level
    _WORKER_EXTRACTOR_PIPELINE = pdf_pipeline
    _WORKER_EXTRACTOR_FACTORY = factory


def _worker_extractor_for(
    level: DocumentLevel, pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED
) -> ExtractorPort | None:
    """Экстрактор воркера для пары ``(level, pdf_pipeline)`` (кэш размера 1; C1).

    Если инициализированный экстрактор подходит (обе оси совпали или универсальны) — он и
    используется. Иначе (защитный путь) строится фабрикой и кэшируется: пара «другой
    механизм PDF» — не та же самая настройка, и обслуживать её чужой экстрактор значило бы
    извлечь текст не тем движком, чем объявлено.
    """
    if _WORKER_EXTRACTOR is not None and (
        _WORKER_EXTRACTOR_LEVEL is None or _WORKER_EXTRACTOR_LEVEL == level
    ) and (
        _WORKER_EXTRACTOR_PIPELINE is None
        or _WORKER_EXTRACTOR_PIPELINE == pdf_pipeline
    ):
        return _WORKER_EXTRACTOR
    if _WORKER_EXTRACTOR_FACTORY is not None:
        set_worker_extractor(
            _WORKER_EXTRACTOR_FACTORY(level, pdf_pipeline),
            level=level,
            factory=_WORKER_EXTRACTOR_FACTORY,
            pdf_pipeline=pdf_pipeline,
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


def _page_with_source_path(page: RawPage, raw_path: Path) -> RawPage:
    """``RawPage`` с путём к retained-файлу — когда тело пришло staging-путём.

    Документ (``pdf``/office), допущенный как терминальный артефакт (P6b), приезжает с
    пустым ``content`` и файлом на диске: тело не буферизуется, чтобы не держать 100 MiB в
    RAM. Экстрактору нужен источник байтов, поэтому inline-путь (без пула экстракции)
    сообщает ему **путь** к тому же retained-файлу (``source_path``, I19) — так же, как это
    делает воркер пула (:func:`_run_extract`). Байты в RAM родителя при этом **не**
    читаются: читает адаптер, и только он (docling открывает файл сам).

    Страница (``content`` непуст) возвращается как есть — путь байт-в-байт прежний.
    Отсутствующий/нечитаемый файл — тоже ``page`` без изменений: ошибку честно поднимет
    экстрактор, а не этот хелпер.
    """
    if page.content:
        return page
    if not Path(raw_path).is_file():
        return page
    return replace(page, source_path=str(raw_path))


def _cache_extractor_version(
    extractor: ExtractorPort, page: RawPage, route: ExtractionRoute
) -> str:
    """Версия движка для ключа ``extract_cache`` — **фактического** движка страницы.

    Кэш пишется с ``ExtractedDoc.extractor_version`` (его штампует сам адаптер: страница —
    ``trafilatura-html-2``, PDF — ``docling-doc-3:full/docling-<build>``), поэтому проба
    обязана спрашивать ту же величину. Композитная ``.version`` для этого не годится: она
    описывает набор движков композита (``trafilatura-html-2+docling-doc-3:…``) и не
    совпадает со штампом **ни одного** документа — кэш от такого ключа не давал попаданий
    вовсе (write-only).

    Композит отвечает через :meth:`core.cli.wiring._LocalExtractor.extractor_version_for`:
    маршрут резолвится там же, где исполняется извлечение, поэтому ключ пробы и ключ записи
    не могут разойтись. Экстрактор без такого метода (простые адаптеры, тестовые фейки,
    ``RemoteExtractor``) штампует ровно ``.version`` — правило то же, диспетчера нет.

    Ошибка резолвера **не** глушится: тихий откат на композитную версию вернул бы ровно тот
    дефект, ради которого этот помощник существует (кэш, который никогда не попадает).
    """
    resolver = getattr(extractor, "extractor_version_for", None)
    if callable(resolver):
        return str(resolver(page, route) or "")
    return str(getattr(extractor, "version", "") or "")


def _run_extract(extractor: ExtractorPort, request: _ExtractRequest) -> ExtractedDoc:
    """Перечитывает retained-байты (I19) и вызывает ``extractor.extract`` на том же ``RawPage``.

    Общий шаг модульного воркер-процесса (:func:`_extract_worker`) и потокового воркера
    (``split``): источник байтов — retained-файл, а не буфер родителя (I19).

    **Байты не поднимаются в RAM.** Retained-файл читается потоково (1 MiB) только ради
    ключа кэша; сам ``RawPage`` несёт ``source_path`` и пустой ``content`` — документ
    (десятки/сотни МБ) не копируется в RSS воркера лишь для того, чтобы адаптер записал
    его во временный файл и прочитал обратно. Экстрактор, который не умеет читать путь,
    работает по ``content`` как прежде (поле аддитивное, дефолт ``None``).

    Ключ кэша считается из **фактически прочитанных** байтов (потоковый ``sha256`` файла) и
    совпадает с родительским :func:`_extract_cache_key` (``raw_path.stem``) — I19 требует
    верификации от байтов, а не от имени файла. Потоковый хеш даёт ровно тот же ключ:
    это хеш тех же байтов файла.
    """
    # I19: верификация от содержимого файла, а не от его имени; чтение — потоковое,
    # поэтому размер документа не влияет на RSS воркера.
    key = _hash_file(Path(request.raw_path))
    page = RawPage(
        url=request.url,
        content=b"",
        source_path=str(request.raw_path),
        fetched_at=request.fetched_at,
        fetch_mode=request.fetch_mode,
        lang=request.lang,
        status_code=request.status_code,
        content_type=request.content_type,
        engine=request.engine,
    )
    # Ф1.2: кэш экстракции. Ключ — хеш байтов retained-файла (I19): хит принимается лишь
    # при совпадении ключа конверта с этими байтами, поэтому кэш не может подсунуть
    # неверифицированный контент. Пропускается ровно вызов экстрактора — хеширование
    # retained-байтов остаётся.
    level_text = _as_text(request.level)
    cache = (
        ExtractionCache(request.cache_root, max_bytes=request.cache_max_bytes)
        if request.cache_root
        else None
    )
    if cache is not None:
        # Ключ — версия того движка, который обслужит **эту** страницу (штамп адаптера),
        # иначе проба не совпадёт с записью и кэш останется write-only.
        cached = cache.get(
            key, _cache_extractor_version(extractor, page, request.route), level_text
        )
        if cached is not None:
            return cached
    extracted = extractor.extract(page, request.route)
    if cache is not None:
        cache.put(extracted, level=level_text, content_hash=key)
    return extracted


def _trim_worker_arenas() -> bool:
    """Отдаёт ОС свободные арены glibc после документа (операционный рычаг памяти).

    Зовётся **в воркер-процессе** (:func:`_extract_worker`) после того, как результат
    извлечения получен, и **до** возврата payload родителю: родитель получает документ,
    когда heap воркера уже ужат. Адаптер об этом не знает — память процесса не его дело.

    Замер (22.10, ``layout_tables``, один процесс): после 81-стр. PDF RSS 1679 → 1084 MB
    (≈600 MB ОС), второй trim позже 1406 → 1240 MB; без trim 4 мелких PDF поднимали
    процесс с 1679 до 2052 MB (арены, а не объективная потребность). Подробности —
    :mod:`core.util.memory`.

    **Никогда не поднимает.** Гигиена памяти не вправе ломать извлечение: сбой хука —
    ``False`` и запись документа, а не отказ задания.
    """
    try:
        return bool(_memory.release_free_arenas())
    except Exception:  # noqa: BLE001 — наблюдаемость/память не ломают экстракцию
        return False


def _worker_peak_rss_mb() -> float | None:
    """Пик RSS **текущего** процесса, MB (``resource.getrusage``); ``None`` — не прочитать.

    ``ru_maxrss`` на Linux — KiB и **монотонен** в пределах процесса, поэтому значение
    после задания и есть пик воркера на этот момент (включая память конвертера).
    Не-POSIX окружение/сбой — ``None``: метрика не выдумывается.
    """
    try:
        import resource

        kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (ImportError, OSError, ValueError):  # pragma: no cover — не-POSIX
        return None
    if kib <= 0:  # pragma: no cover — вырожденное значение
        return None
    return round(float(kib) / 1024.0, 1)


def _write_worker_peak_rss(directory: str | None) -> None:
    """Воркер: пишет свой пик RSS в сток ``{directory}/{pid}.rss`` (F0).

    Файл, а не результат документа: контракт ``_ExtractRequest → ExtractedDoc`` не должен
    нести наблюдаемость, а ``Manager``/очередь стоили бы канал и слив на каждом задании
    (см. :class:`ExtractionPool`). ``pid`` переиспользуется при рециклинге воркеров,
    поэтому в файле хранится **максимум**: новое поколение дописывает только большее.
    Запись атомарна (``tmp`` + ``replace``), чтобы родитель не прочитал половину числа.

    Сбой стока — не ошибка прогона: наблюдаемость вторична к экстракции (как
    :func:`_trim_worker_arenas`).
    """
    if not directory:
        return
    peak = _worker_peak_rss_mb()
    if peak is None:
        return
    try:
        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        target = root / f"{os.getpid()}.rss"
        try:
            previous = float(target.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            previous = 0.0
        if peak <= previous:
            return
        temporary = target.with_suffix(f".{os.getpid()}.tmp")
        temporary.write_text(f"{peak}\n", encoding="utf-8")
        os.replace(temporary, target)
    except OSError:  # pragma: no cover — сток недоступен
        return


def _extract_worker(request: _ExtractRequest) -> ExtractedDoc:
    """Module-level воркер экстракции для процессов (picklable by reference, P4/§7).

    Использует экстрактор, построенный инициализатором в этом процессе; ``store``/``session``
    воркеру недоступны by construction (payload — только примитивы).

    После каждого документа (и до возврата результата родителю) свободные арены glibc
    возвращаются ОС — ``malloc_trim(0)``, :func:`_trim_worker_arenas`. Без этого RSS
    воркера растёт храповиком: ``max_tasks_per_child`` рециклинг лишь откладывает OOM,
    а не отменяет его. Рычаг **операционный**: результат байт-в-байт тот же, ``run_id``
    не меняется, ``execution_change`` не нужен (§13).

    F0: в том же ``finally`` пик RSS процесса уходит в файл-сток
    (:func:`_write_worker_peak_rss`) — наблюдаемость памяти воркера без изменения
    контракта результата. Пишется и на отказе задания: OOM-подобный отказ — главный
    случай, ради которого метрика собирается.
    """
    if _WORKER_EXTRACTOR is None:  # pragma: no cover — защита от неверной сборки
        return ExtractedDoc(
            text="",
            content_hash="",
            extractor_version="",
            error="extraction worker без экстрактора (initializer не отработал)",
        )
    extractor = _worker_extractor_for(request.level, request.pdf_pipeline)
    if extractor is None:  # pragma: no cover — защита от неверной сборки
        return ExtractedDoc(
            text="",
            content_hash="",
            extractor_version="",
            error="extraction worker без экстрактора для уровня/механизма",
        )
    # ``finally``: арены возвращаются и на честном отказе извлечения (исключение
    # поднимается дальше как прежде — пул сам решает судьбу задания).
    try:
        return _run_extract(extractor, request)
    finally:
        _trim_worker_arenas()
        _write_worker_peak_rss(request.rss_dir)


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
        self._slots: dict[_ExtractPoolKey, _ExtractPoolSlot] = {}
        #: Наблюдаемость операционного рычага памяти: сколько заданий воркеры пула
        #: довели до результата. В воркере на каждое такое задание приходится ровно один
        #: ``malloc_trim(0)`` (:func:`_extract_worker`), поэтому счётчик фазы — честная
        #: нижняя граница числа trim-вызовов, видимая оператору (обратного канала из
        #: дочернего процесса нет).
        self.completed = 0

    def slot(self, key: _ExtractPoolKey) -> _ExtractPoolSlot:
        """Состояние ключа (создаётся лениво; I/O нет)."""
        slot = self._slots.get(key)
        if slot is None:
            slot = _ExtractPoolSlot()
            self._slots[key] = slot
        return slot

    def executor(
        self, key: _ExtractPoolKey, factory: Callable[[], Executor]
    ) -> Executor:
        """Пул ключа, создаваемый ``factory`` при первом обращении (лениво)."""
        slot = self.slot(key)
        if slot.executor is None:
            slot.executor = factory()
        return slot.executor

    def note_completion(self, key: _ExtractPoolKey) -> None:
        """Задание завершилось (любым исходом): пул жив, серия таймаутов сброшена."""
        self.slot(key).stalled_timeouts = 0
        self.completed += 1

    def note_timeout(self, key: _ExtractPoolKey) -> bool:
        """Учесть таймаут артефакта; ``True`` — пул мёртв (``2 × timeout`` без завершений)."""
        slot = self.slot(key)
        slot.stalled_timeouts += 1
        return slot.stalled_timeouts >= _EXTRACT_DEAD_POOL_TIMEOUTS

    def terminate(self, key: _ExtractPoolKey) -> None:
        """Reaping зависших воркеров (``terminate_workers``, Python 3.14; best-effort)."""
        executor = self.slot(key).executor
        terminate = getattr(executor, "terminate_workers", None)
        if terminate is None:
            return
        try:
            terminate()
        except Exception:  # noqa: BLE001 — reaping best-effort
            pass

    def discard(self, key: _ExtractPoolKey) -> None:
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

    def rebuild(self, key: _ExtractPoolKey) -> bool:
        """Discard + bounded rebuild; ``False`` — пересборки исчерпаны (ключ abandon'ен)."""
        self.discard(key)
        slot = self.slot(key)
        slot.stalled_timeouts = 0
        if slot.rebuilds >= self._max_rebuilds:
            slot.abandoned = True
            return False
        slot.rebuilds += 1
        return True

    def abandoned(self, key: _ExtractPoolKey) -> bool:
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

    Копит ``docs`` (retention + провенанс экстракции) и ``chunks``. ``chunks``
    персистятся на границе продукта (Pass B, «сначала диск, потом БД»); ``docs`` живут
    до конца фазы, потому что ``PhaseResult`` их возвращает.

    Граница честности: копление — **память, а не durability**. Персистенция на границе
    продукта переживает падение, но чанки всех продуктов всё равно остаются в RAM до
    конца фазы (их держит этот объект), поэтому пик памяти переносом записи не снижается.
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
    pool_key: _ExtractPoolKey


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
    6. ``lexical_engine_version`` — фактический лексический движок прогона
       (``tantivy-<ver>``; пусто — лексического плеча нет). Берётся у сборщика,
       которого настраивает composition root (``wiring``), а не у ``getattr``-дефолта:
       иначе ``pb`` не различал бы движки (§13:676);
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
    *,
    origin: str = ORIGIN_CRAWL,
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
    """
    if store is None:
        return
    for doc in docs:
        try:
            if origin == ORIGIN_CRAWL:
                store.upsert_corpus_doc(doc)
            else:
                store.upsert_corpus_doc(doc, origin=origin)
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


@dataclass
class _EmbedCensus:
    """Счётчики работы эмбеддинга одного продукта: из кэша vs посчитано сейчас.

    Живёт в ``build_corpus`` (per продукт), наполняется :func:`_embed_batched` и
    читается наблюдаемостью (:func:`_make_embed_reporter`, итоговая строка продукта).
    Разделение принципиально для доказательности кэша: «сколько чанков эмбеддилось» и
    «сколько взято готовым» — разные величины, и только их пара отвечает на вопрос
    «сколько сэкономил кэш».

    ``processed = cached + embedded`` — сколько чанков продукта уже обслужено (это и есть
    прогресс ``embed_chunks``). ``hit_ratio_pct`` — целые проценты: наблюдаемость печатает
    целые, а не дробь, которую нечем сверить.
    """

    #: Чанки, вектор которых взят из кэша (эмбеддер не вызывался).
    cached: int = 0
    #: Чанки, вектор которых посчитан в этом прогоне (промахи кэша).
    embedded: int = 0

    @property
    def processed(self) -> int:
        """Сколько чанков уже обслужено (хиты + свежие)."""
        return self.cached + self.embedded

    @property
    def hit_ratio_pct(self) -> int:
        """Доля попаданий в процентах (0, если работы ещё не было)."""
        total = self.processed
        if total <= 0:
            return 0
        return int(round(100.0 * self.cached / total))


@dataclass
class _EmbedThroughput:
    """Наблюдаемый темп батчевого эмбеддинга: EMA по батчам одного продукта.

    **Почему измерение, а не константа (A3).** Цена чанка зависит от длины текста, модели и
    CPU: замер этого хоста даёт 374 мс на 500 символов, 1173 мс на 1500 и 1916 мс на 3000.
    Предсказывать её константой значило бы объявить то, что измеряется; поэтому остаток
    оценивается по фактическому темпу **текущего** прогона.

    Сглаживание (``alpha`` = :data:`_EMBED_RATE_ALPHA`) нужно, чтобы первый батч — с ленивой
    загрузкой модели — не определял оценку на весь продукт, а один медленный батч не дёргал
    её на порядок.
    """

    #: Вес нового замера (0 < alpha ≤ 1).
    alpha: float = _EMBED_RATE_ALPHA
    #: Сглаженный темп, чанков/с (``None`` — замеров ещё не было).
    rate: float | None = None
    #: Сколько чанков уже эмбеддировано (по последнему замеру).
    done: int = 0
    #: Прошедшее время эмбеддинга на последнем замере, с.
    elapsed: float = 0.0

    def observe(self, done: int, elapsed: float) -> float | None:
        """Учитывает границу батча и возвращает сглаженный темп (чанков/с).

        Замер без прироста (нулевой интервал/повтор того же счётчика) темпа не меняет:
        делить на него нельзя, а подставлять ноль значило бы объявить остановку.
        """
        delta_n = done - self.done
        delta_t = elapsed - self.elapsed
        self.done = done
        self.elapsed = elapsed
        if delta_n <= 0 or delta_t <= 0:
            return self.rate
        sample = delta_n / delta_t
        self.rate = (
            sample
            if self.rate is None
            else self.alpha * sample + (1.0 - self.alpha) * self.rate
        )
        return self.rate

    def eta_seconds(self, done: int, total: int) -> int | None:
        """Оценка остатка, с (``None`` — темпа ещё нет; ``0`` — работа закончена)."""
        if done >= total:
            return 0
        if self.rate is None or self.rate <= 0:
            return None
        return int(round((total - done) / self.rate))


def format_embed_progress(
    done: int,
    total: int,
    rate: float | None,
    eta_s: int | None,
    *,
    cached: int | None = None,
) -> str:
    """Человекочитаемая строка эмбеддинга: ``N/M чанков, X чанк/с, осталось ~Y мин``.

    Остаток печатается только пока работа не закончена (``done < total``): «осталось ~0 мин»
    на завершённом продукте — шум, а не факт. Темп печатается с двумя знаками: при 1173 мс на
    чанк это 0.85 чанк/с, и округление до целого скрыло бы разницу между моделями и длинами
    текста — то есть ровно то, ради чего число публикуется.

    :param cached: сколько из ``done`` чанков взято из кэша векторов; ``None`` — кэша нет
        (строка остаётся прежней, байт-в-байт). При заданном числе публикуется и доля:
        без неё «сколько сэкономил кэш» не читается из строки.
    """
    parts = [f"{done}/{total} чанков"]
    if cached is not None:
        share = int(round(100.0 * cached / done)) if done > 0 else 0
        parts.append(f"из кэша {cached} ({share}%)")
    if rate is not None and rate > 0:
        parts.append(f"{rate:.2f} чанк/с")
    if eta_s is not None and done < total:
        # Минуты — операторская единица для «десятков минут/часов»; меньше минуты честнее
        # назвать минутой, чем нулём.
        parts.append(f"осталось ~{max(1, round(eta_s / 60.0))} мин")
    return ", ".join(parts)


def _make_embed_reporter(
    *,
    heartbeat: Heartbeat,
    progress: Callable[[dict[str, int]], None] | None,
    store: Any,
    session: str | None,
    throughput: _EmbedThroughput,
    census: _EmbedCensus | None = None,
) -> Callable[[int, int, float], None]:
    """Колбэк границы батча эмбеддинга: признак жизни + прогресс с темпом и остатком.

    Эмбеддинг идёт **после** дренажа продукта, где :func:`_emit_progress` уже не вызывается.
    До правки единственный вызов ``embed`` на весь продукт не оставлял в этом участке ни
    одной точки жизни: сеанс 2026-10-25 оставался ``running`` с heartbeat, замершим 1280 с,
    при 505 % CPU — работа выглядела как мёртвый сеанс.

    Каналы — существующие (``heartbeat`` и ``progress`` ``--follow``), новых не вводится.
    Ключи ``embed_*`` **дополняют** агрегат сеанса (``pending``/``fetched``/…), а не заменяют
    его: на границе эмбеддинга краул уже финализирован, и оба факта должны быть видны
    одновременно. Все значения — целые (контракт ``progress``): ``embed_chunks``/``embed_chunks_total``
    — «сколько из скольких», ``embed_elapsed_s`` — прошедшее время эмбеддинга,
    ``embed_chunks_per_min`` — фактический темп прогона, ``embed_eta_s`` — оценка остатка.
    При подключённом кэше векторов (:class:`~core.pipeline.embed_cache.EmbedCache`) к ним
    добавляются ``embed_cached`` — сколько чанков взято готовыми — и
    ``embed_hit_ratio_pct`` — их доля. Человекочитаемая форма тех же величин —
    :func:`format_embed_progress`.

    **``embed_chunks`` — обработанные чанки, ``embed_chunks_per_min`` — темп эмбеддинга.**
    Прогресс обязан считать хиты (иначе «из кэша 3000 из 3000» выглядело бы как стоящий на
    нуле продукт), а темп — только фактически посчитанные: это та же величина, что публикуют
    замеры пропускной способности, и она сравнивается между прогонами и моделями (A3). Оценка
    остатка берётся из неё и потому при большой доле хитов слегка завышена — в безопасную
    сторону («работает дольше, чем окажется»), а не в сторону мнимого зависания.

    Ошибки чтения стора/колбэка изолируются: прогресс информационен и не роняет фазу
    (наблюдаемость не измерение).
    """
    counts: dict[str, int] = {}

    def _report(done: int, total: int, elapsed: float) -> None:
        # Точка жизни — до любых ранних выходов: она не зависит от того, включён ли --follow.
        heartbeat.touch()
        # Наблюдение темпа — даже без колбэка: накопленный темп публикуется в итоговой
        # строке фазы (ценой «сколько это заняло»), а не только в живом прогрессе.
        # Без кэша (``census is None``) обработанные чанки и есть эмбеддированные.
        embedded = done if census is None else census.embedded
        rate = throughput.observe(embedded, elapsed)
        eta = throughput.eta_seconds(done, total)
        if progress is None:
            return
        if session is not None:
            try:
                counts.update(dict(store.crawl_session_counts(session)))
            except Exception:  # noqa: BLE001 — прогресс информационен
                pass
        counts["embed_chunks"] = done
        counts["embed_chunks_total"] = total
        counts["embed_elapsed_s"] = int(elapsed)
        if census is not None:
            counts["embed_cached"] = census.cached
            counts["embed_hit_ratio_pct"] = census.hit_ratio_pct
        if rate is not None and rate > 0:
            counts["embed_chunks_per_min"] = int(round(rate * 60.0))
        if eta is not None:
            counts["embed_eta_s"] = int(eta)
        try:
            progress(dict(counts))
        except Exception:  # noqa: BLE001 — прогресс информационен
            pass

    return _report


def _embed_batched(
    embedder: EmbedderPort,
    chunks: Sequence[CorpusChunk],
    *,
    on_batch: Callable[[int, int, float], None] | None = None,
    batch_size: int = EMBED_BATCH_CHUNKS,
    cache: EmbedCache | None = None,
    embed_model_id: str = "",
    census: _EmbedCensus | None = None,
) -> list[list[float]]:
    """Эмбеддинг чанков внешними батчами; порядок входа = порядок выхода (детерминизм, A3).

    Один батч — один вызов ``embed`` и одна точка наблюдения (:data:`EMBED_BATCH_CHUNKS`).
    Тексты собираются **по батчу**, а не списком на весь продукт: ``[c.text for c in chunks]``
    держал рядом с полным накопителем векторов ещё и полный список текстов.

    Возвращается полный список векторов: партицию собирает ``IndexBuilderPort.build(chunks,
    vectors, meta)`` целиком, поэтому пик «все векторы» задан контрактом порта, а не циклом.
    Батч лишь ограничивает **второй** материализуемый список (тексты) и даёт наблюдение.

    **Кэш векторов (опция).** При ``cache`` каждый чанк батча сначала спрашивается у кэша
    по ключу ``(content_hash текста, embed_model_id)``; эмбеддер получает только промахи,
    свежие векторы сразу записываются в кэш, а хиты подставляются в **свои** позиции. Поэтому
    порядок входа = порядок выхода и при смешанном батче (инвариант держится структурой:
    слоты батча индексируются позицией чанка, а не порядком ответа эмбеддера). ``cache=None``
    — прежнее поведение байт-в-байт: один вызов ``embed`` на весь батч.

    Кэш — оптимизация, а потому его сбой не должен стоить прогона: ``get``/``put`` устроены
    как промах/no-op на любой порче файла. Ошибка самого эмбеддера по-прежнему пробрасывается
    (её изолирует вызывающий): тихо подставить нули значило бы выдать выдуманный вектор за
    измеренный.

    :param embed_model_id: идентификатор модели — часть ключа кэша. Берётся у объявленного
        входа прогона (``EmbedderPort.model_id``), а не выдумывается здесь: кэш обязан
        смешивать векторы только одной модели.
    :param census: счётчики per продукт (хиты/посчитанные); ``None`` — не считать.
    """
    total = len(chunks)
    vectors: list[list[float]] = []
    started = time.monotonic()
    # Размерность — часть проверки хитов: вектор другой размерности несовместим с моделью.
    # ``operator.index`` принимает и numpy-целые, иначе проверка молча выключилась бы.
    try:
        declared_dim = operator.index(getattr(embedder, "dim", None))
    except (TypeError, ValueError):
        declared_dim = 0
    expected_dim = declared_dim if declared_dim > 0 else None
    for start in range(0, total, batch_size):
        batch = chunks[start : start + batch_size]
        if cache is None:
            vectors.extend(embedder.embed([c.text for c in batch]))
            if census is not None:
                census.embedded += len(batch)
        else:
            slots: list[list[float] | None] = [None] * len(batch)
            pending: list[int] = []
            cached_now = 0
            for position, chunk in enumerate(batch):
                hit = cache.get(
                    chunk.content_hash or "", embed_model_id, dim=expected_dim
                )
                if hit is None:
                    pending.append(position)
                    continue
                slots[position] = hit
                cached_now += 1
            if pending:
                fresh = embedder.embed([batch[i].text for i in pending])
                if len(fresh) != len(pending):
                    raise ValueError(
                        "эмбеддер вернул "
                        f"{len(fresh)} векторов на {len(pending)} чанков"
                    )
                for offset, position in enumerate(pending):
                    vector = fresh[offset]
                    slots[position] = vector
                    # Пишем ровно то, что вернула модель: квантизация в fp16 — забота
                    # кэша, и она тождественна той, что делает сборщик индекса.
                    cache.put(
                        batch[position].content_hash or "",
                        embed_model_id,
                        vector,
                        dim=expected_dim,
                    )
            if census is not None:
                census.cached += cached_now
                census.embedded += len(pending)
            for value in slots:
                if value is None:  # pragma: no cover — позиция заполняется всегда
                    raise ValueError("батч эмбеддинга оставил чанк без вектора")
                vectors.append(value)
        if on_batch is not None:
            on_batch(start + len(batch), total, time.monotonic() - started)
    return vectors


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
    on_batch: Callable[[int, int, float], None] | None = None,
    cache: EmbedCache | None = None,
    census: _EmbedCensus | None = None,
) -> bool:
    """Best-effort эмбеддинг и физическая сборка индекса продукт-партиции (K3, §B.1).

    ``pipeline`` не работает с файлами индекса напрямую: за ``.npy``-шарды/tantivy отвечает
    ``IndexBuilderPort``. Сбой изолируется в ``errors``.

    **Эмбеддинг — внешними батчами** (:data:`EMBED_BATCH_CHUNKS`), а не одним вызовом на весь
    продукт: между батчами фаза получает точку наблюдения (``on_batch`` → heartbeat +
    прогресс). Тексты батча материализуются по мере обхода; накопитель векторов полон по
    контракту ``IndexBuilderPort.build``. Порядок векторов совпадает с порядком чанков, а
    кратность внешнего батча внутреннему держит результат побитово равным одному вызову
    (детерминизм, A3).

    :param cache: кэш векторов (:class:`~core.pipeline.embed_cache.EmbedCache`); ``None`` —
        без кэша, поведение прежнее. Кэш не меняет ни ``pb``, ни манифест партиции: он
        сокращает **стоимость** эмбеддинга, а не условия измерения.
    :param census: счётчики «из кэша / посчитано» этого продукта (наполняется эмбеддингом).
    :param on_batch: ``(done, total, elapsed_s)`` — граница батча; ``None`` — без наблюдения
        (юнит-путь). Ошибка колбэка не должна выдавать себя за сбой сборки: наблюдение
        изолируется вызывающим.
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
        vectors = _embed_batched(
            embedder,
            chunks,
            on_batch=on_batch,
            cache=cache,
            embed_model_id=embed_model_id,
            census=census,
        )
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


def _is_terminal_artifact(
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

    **P0/P6b: потолок по классу.** Размер проверяется по ``stat`` **до** чтения. Класс
    определяется по метаданным записи (URL-суффикс/``Content-Type``, заявленный набор
    ``attachment_formats``): допущенный **терминальный артефакт** — документ
    (``pdf``/``docx``/``pptx``/``xlsx``) **или** контейнер (``zip``/``7z``/``rar``) —
    ограничен ``artifact_max_bytes``, страница — ``max_bytes``. Так retained-PDF/артефакт в
    125 MB резюмится, а страница того же размера по-прежнему терминально отвергается
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
    artifact = _is_terminal_artifact(item.url, item.content_type, attachment_formats)
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


#: Маршрут извлечения члена по суффиксу: документы — ``document``, разметка — ``auto``
#: (``auto`` сам роутит HTML/markdown; ``html`` намеренно не форсируется — A1).
_DOCUMENT_SUFFIX_SET: frozenset[str] = frozenset(DOCUMENT_SUFFIXES)

#: ``Content-Type`` члена по суффиксу — для экстрактора и провенанса (данные, не эвристика).
_MEMBER_CONTENT_TYPES: dict[str, str] = {
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
        content_type=_MEMBER_CONTENT_TYPES.get(suffix),
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
    ingest: _ProductIngest,
    seen_docs: dict[str, str],
    seen_chunk_ids: set[str],
    errors: list[str],
    store: Any,
    level: DocumentLevel,
    fetched_at: str,
    lang: str | None,
    artifact_probes: list[dict[str, Any]],
    extracted: Any = _INLINE,
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
            content_type=_MEMBER_CONTENT_TYPES.get(suffix),
            container_for=None,
        ).as_dict()
    )
    page = RawPage(
        url=task.member_url,
        content=b"",
        fetched_at=fetched_at,
        fetch_mode=FetchMode.HTTP,
        content_type=_MEMBER_CONTENT_TYPES.get(suffix),
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
        return MemberOutcome(doc_id=document_id, content_hash=doc_hash)
    seen_docs[doc_hash] = task.member_url

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
    )
    ingest.docs.append(doc)
    _persist_docs(store, [doc], errors)

    try:
        chunks = chunker.chunk(extracted, document_id)
    except Exception as exc:  # noqa: BLE001 — сбой нарезки не отменяет документ
        errors.append(
            f"{task.product_id}/{task.member_url}: chunk: {type(exc).__name__}: {exc}"
        )
        return MemberOutcome(doc_id=document_id, content_hash=doc_hash)

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
        cid = _chunk_id(task.product_id, doc_hash, chunk_no)
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
    return MemberOutcome(doc_id=document_id, content_hash=doc_hash)


@dataclass
class _PhaseLifecycle:
    """Адрес durable-сеанса краула для обработчика прерывания в обёртке фазы.

    ``_build_corpus`` открывает сеанс внутри себя, а обработчик прерывания принадлежит
    обёртке :func:`build_corpus` (там же единственный ``pools.shutdown``). Без этого
    адреса прерванная фаза оставляла бы сеанс в ``running`` навсегда — ровно дефект
    «фантомных краулов в полёте» в ``hound crawl status``.

    ``completed`` ставится **только** после штатной финализации сеанса
    (:func:`core.pipeline.crawl_session.finalize_session`): любое исключение (включая
    ``KeyboardInterrupt``/``BaseException``) оставляет ``False``, и обёртка закрывает
    сеанс терминально (:func:`_finalize_interrupted_session`). Обратное тоже важно:
    исключение **после** успешной финализации не переписывает легитимный
    ``completed``/``partial`` на статус прерывания.

    Граница честности: ``SIGKILL``/OOM-killer не перехватывается ни одним Python-кодом,
    поэтому после него сеанс остаётся ``running``; это лечится recovery-on-open
    (:func:`core.pipeline.crawl_session.open_session`) при следующем запуске, а не
    обработчиком прерывания.
    """

    store: Any = None
    session_id: str | None = None
    completed: bool = False


def _serp_disabled_observation(source: str | None) -> dict[str, Any]:
    """Провенанс **выключенного** контура: «выключено, источник <уровень>».

    Отсутствие записи неотличимо от «контур есть, но не сработал», поэтому выключенный
    контур тоже пишет наблюдение — с уровнем, который его выключил (``run.yaml``/
    ``deploy.yaml``/``default``/``env``). Значения не выдумываются: бюджет не показывается,
    если контур выключен.
    """
    return {
        "enabled": False,
        "rounds": {"declared": 0, "executed": 0, "source": str(source or "default")},
        "budget": {"declared_per_round": None, "effective_per_product": None, "used": {}, "exhausted": {}},
        "adapter": None,
        "unavailable_reason": None,
        "queries": {},
        "candidates": {},
        "accepted": {},
        "rejected": {},
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
        return _serp_disabled_observation(serp_source), {}
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
    if observation.get("unavailable_reason"):
        note += f"; контур не исполнен: {observation['unavailable_reason']}"
    exhausted = [
        pid for pid, flag in (budget.get("exhausted") or {}).items() if flag
    ]
    if exhausted:
        note += f"; бюджет исчерпан: {', '.join(sorted(exhausted))}"
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
    сборка теперь текущая. ``evaluate`` не умеет выборочной пере-оценки (нет селектора
    затронутых ячеек), поэтому «ячейка описывает ушедшую сборку» — факт, который
    фиксирует фаза корпуса; расчёт (:mod:`core.pipeline.compute`) уже умеет исключать
    такие ячейки, а отчёт — называть их очередью пере-оценки.

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
                "помечены устаревшими (new_build) — требуют пере-оценки"
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
    lifecycle: _PhaseLifecycle | None = None,
    mem_budget_mb: int | None = None,
    derive_extract_workers: bool = False,
    search: Any = None,
    serp_rounds: int = 0,
    serp_budget: int | None = None,
    serp_source: str | None = None,
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
    serp_observation: dict[str, Any] = _serp_disabled_observation(serp_source)
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
    #: Прерванная фаза обязана закрыть сеанс терминально (см. ``_PhaseLifecycle``):
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
            # F1/Д-6: каждая запись исхода URL — честный признак жизни процесса.
            heartbeat.touch()
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
        origin: str = ORIGIN_CRAWL,
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
        Поэтому ``_mark`` здесь не вызывается ни при успехе, ни при сбое: иначе карта
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
                try:
                    staged.unlink(missing_ok=True)
                except OSError:
                    pass
            reason = f"{type(exc).__name__}: {exc}"
            errors.append(f"{product_id}/{url}: retention: {reason}")
            if manual:
                return None
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
        if manual:
            return raw_path
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
        *,
        origin: str = ORIGIN_CRAWL,
    ) -> str | None:
        """Inline-экстракция → доводка одного URL (путь без пула, ``None``/``1``).

        Тело экстракции (``extractor_obj.extract``) вынесено в :func:`_finish_one`; здесь —
        только inline-вызов и перевод исключения в ту же ошибку ``parser_failed``, что и
        пул-путь (P4: единая семантика отказа). ``extractor_obj`` — экстрактор уровня
        корня (P5; ``full`` переиспользует переданный порт). ``level`` — уровень корня:
        нужен как ключ кэша экстракции (Ф1.2) и не выводится из ``extractor_obj``, потому
        что ``full`` переиспользует переданный порт.

        :returns: ``doc_id`` созданного/найденного документа корпуса либо ``None`` — исход
            не дал документа (сбой экстракции/нарезки). Нужен ручной подаче
            (13-manual-artifacts §3.3): только дошедший до корпуса артефакт закрывает
            пробел покрытия, а не «объявленный файл».
        """
        try:
            level_text = _as_text(level)
            # I19: ключ — хеш тех же байтов, что retention положил в ``raw_path``.
            # ``raw_path.stem`` и есть этот хеш (``_retain`` называет файл ``{hash}.bin``),
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
                origin=origin,
            )
            return None
        return _finish_one(
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
            origin=origin,
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
        (:func:`_load_reused_chunks`). Run-level ``chunk_id`` остаётся прежним
        (:func:`_chunk_id`) — идентификаторы индекса не меняются.

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
            found = _find_doc_version(
                product_id, doc_hash, extractor_token, level_text
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
        _persist_docs(store, [doc], errors, origin=origin)

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
    def _manual_host_scope(product_id: str) -> tuple[frozenset[str], frozenset[str]]:
        """``(own_hosts, declared_hosts)`` продукта — та же семантика, что у scope краула.

        Собственные хосты продукта (его seed-корни) и **все** объявленные хосты прогона
        (корни + ключи ``domain_profiles.yaml``). Ручной артефакт не расширяет scope:
        поддомен допускается только объявленным, чужой хост — нет. Иначе «предоставление»
        стало бы способом дописать корпус чужим хостом — то есть execution_change,
        проведённым мимо ``run_id``.
        """
        from core.pipeline.serp_loop import declared_serp_hosts

        own_by_product, declared = declared_serp_hosts(
            {product_id: url_map.get(product_id, [])}, domain_profiles
        )
        return own_by_product.get(product_id, frozenset()), declared

    def _persist_manual_audit(
        entry: ManualArtifact, *, doc_id: str | None, raw_path: Path | None
    ) -> None:
        """Аудит предоставления (§3.3): акт человека, а не «повторная попытка фетча».

        Пишется и при неудаче (``doc_id``/``raw_path`` — ``None``): «объявлено, но не
        дошло» — факт, который нельзя терять, иначе провал подачи выглядел бы как её
        отсутствие. Отказ краула по этому URL живёт отдельно (``crawl_session_items``)
        и этим методом не трогается.
        """
        if store is None:
            return
        try:
            store.save_manual_artifact(
                artifact_record(
                    entry,
                    run_id=str(run_id or ""),
                    doc_id=doc_id,
                    raw_path=None if raw_path is None else str(raw_path),
                    created_at=_now_iso(),
                )
            )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(
                f"{entry.product_id}/{entry.url}: manual_artifact audit: "
                f"{type(exc).__name__}: {exc}"
            )

    def _feed_manual_artifact(
        product_id: str,
        entry: ManualArtifact,
        ingest: _ProductIngest,
        seen_docs: dict[str, str],
        seen_chunk_ids: set[str],
    ) -> tuple[str | None, Path | None]:
        """Один объявленный файл: допуск → retention → экстракция → корпус (I19).

        **Чего здесь нет.** Нет второго пути байтов: файл оператора читается и отдаётся
        ``RawPage``, дальше работает :func:`_retain` — единственный writer сырых байтов,
        который сам хеширует их и кладёт в ``corpus_raw``. Файл оператора **не
        перемещается и не изменяется**: ``_publish_staged`` (переезд staging-файла) здесь
        не используется вовсе, поэтому предоставленный файл остаётся там, где его положил
        человек, и следующий прогон перечитает его же.

        **Краулер обходится, границы — нет** (§3.3): живой фетч не выполняется (robots
        снова отказал бы), но SSRF-валидация, scope объявленных хостов продукта и
        блэклист остаются обязательными, а потолок размера артефакта берётся тот же, что
        у краулера (не новый лимит, а уже объявленный ``crawl.max_artifact_bytes``).

        **Отказ не стирается.** Элемент сеанса этим путём не перезаписывается
        (``origin=ORIGIN_MANUAL`` отключает ``_mark``): ``failed`` + ``robots_disallow``
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
        own_hosts, declared_hosts = _manual_host_scope(product_id)
        if not serp_host_in_scope(
            urlsplit(url).hostname or "", own_hosts, declared_hosts
        ):
            errors.append(
                f"{where}: хост вне объявленных хостов продукта — не допущен "
                "(предоставление не расширяет scope краула)"
            )
            return None, None
        # 3. Пределы — те же объявленные оси, что у краула (app < run < site).
        limits = resolve_crawl_limits(
            url,
            app=crawl_app or CrawlConfig(),
            run=crawl_defaults,
            profiles=domain_profiles or {},
            derive_extract_workers=derive_extract_workers,
            mem_budget_mb=mem_budget_mb,
        )
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
        target = resolve_crawl_target(
            product_id,
            url,
            sources=crawl_sources or CrawlSourcesConfig(),
            profiles=domain_profiles or {},
            explicit=explicit_fetch,
            run_default_strategy=crawl_strategy_default,
            run_default_rate_limit=crawl_rate_default,
        )
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
        #    месте): retained-копию пишет ``_retain``, и именно её хеш станет evidence.
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
            fetch_mode=None,  # type: ignore[arg-type]
            content_type=_MEMBER_CONTENT_TYPES.get(document_suffix(url) or ""),
            engine=ORIGIN_MANUAL,
        )
        raw_path = _retain(product_id, url, page, fetch_plan, origin=ORIGIN_MANUAL)
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
        doc_id = _ingest_one(
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
            origin=ORIGIN_MANUAL,
        )
        return doc_id, raw_path

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
    # Векторы/физический индекс и персистенция corpus_chunks здесь не выполняются:
    # сначала собирается содержимое продукта, затем (на его границе) планируется
    # партиция и фиксируется Pass B. ``index_build_id`` (композиция когорты) —
    # единственное, что выводится после **всех** продуктов.
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

        F1/Д-6: здесь же — признак жизни сеанса (``heartbeat.touch``). Точка выбрана
        намеренно: она стоит на каждой границе шага дренажа (фетч/экстракция/расширение
        frontier), поэтому её покрытие не зависит от того, включён ли ``--follow``.
        """
        heartbeat.touch()
        if progress is None or session is None:
            return
        try:
            progress(dict(store.crawl_session_counts(session)))
        except Exception:  # noqa: BLE001 — прогресс информационен
            pass

    def _emit_product(phase: str, product_id: str) -> None:
        """Живая граница продукта для ``on_product`` (``--follow``).

        В отличие от ``log_entries`` (материализуются в конце фазы), этот канал
        срабатывает **во время** дренажа: оператор видит, что продукт начат, а не
        гадает «потерян или ещё в очереди». Ошибки колбэка изолируются.
        """
        if on_product is None:
            return
        try:
            on_product(phase, product_id)
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
        pool_key: _ExtractPoolKey, label: str
    ) -> None:
        """Отравленный пул: discard + bounded rebuild; исчерпание — терминальный отказ.

        Повторное использование мёртвого пула запрещено; авто-ретрая нет. Inline после
        исчерпания не включается: на memory-constrained хосте docling в родителе — худший
        режим, а тихая деградация скрыла бы дефект.
        """
        if pools.rebuild(pool_key):
            return
        errors.append(
            f"{label}: пул экстракции {pool_key[0].value}/{pool_key[1].value}"
            f"/W={pool_key[2]} исчерпал "
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
        #
        # **Потолок класс-aware (P6b).** Терминальный артефакт (документ или заявленный
        # контейнер) меряется ``max_artifact_bytes``, страница — ``max_response_bytes``.
        # Без этого транспорт, поднявший свой потолок до artifact-уровня, всё равно
        # умирал бы здесь на страничном: живой прогон 22.10 — 125 727 643-байтный zip
        # приезжал целиком (scrapy не имеет staging-пути) и получал
        # ``payload_too_large: … > лимита 20971520`` уже после успешной загрузки.
        artifact = _is_terminal_artifact(
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

    def _persist_partial_crawl_provenance() -> None:
        """Пишет наблюдение scope/усечения в ``crawl_sessions.provenance_json`` **по корню**.

        Почему не только в ``finalize_session``: живой прогон 22.10 умер через ~3 часа, и
        ``provenance_json`` остался ``{}`` — вся картина поддоменов (кто сколько забрал)
        пропала вместе с процессом, хотя именно она объясняла дефект. Поэтому провенанс
        корня персистится сразу после его drain, а не в конце фазы: упавший прогон всё
        равно отвечает числом «сколько дали поддомены».

        Форма совместима с финализацией (``effective_limits.roots`` — тот же канал, что
        читает :func:`core.pipeline.crawl_session.truncation_summary`);
        ``finalize_session`` позже перезаписывает его **полным** набором корней.
        ``partial: true``/``status: "running"`` помечают незавершённый сеанс, чтобы
        частичный провенанс не выглядел финальным.
        """
        if store is None or session is None:
            return
        try:
            store.set_crawl_session_provenance(
                session,
                json.dumps(
                    {
                        "partial": True,
                        "status": "running",
                        "effective_limits": {"roots": truncation_records},
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость не валит фазу
            errors.append(f"crawl_provenance(partial): {type(exc).__name__}: {exc}")

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
        # P6b: обход path-prefix терминальными артефактами (документ/заявленный контейнер
        # вне префикса корня) — счётчик + ограниченные примеры. Ключ появляется только при
        # наличии обхода: запись корня без артефактов остаётся байт-в-байт прежней.
        if frontier.artifacts_outside_prefix_count:
            record["artifacts_outside_prefix"] = {
                "count": frontier.artifacts_outside_prefix_count,
                "examples": list(frontier.artifacts_outside_prefix),
            }
        # C (execution_change): видимость поддоменов. ``out_of_scope_subdomains`` —
        # отвергнутые поддомены хоста корня (объявленных хостов не хватило): точный
        # счётчик по хостам + ограниченные примеры. ``accepted_foreign_hosts`` — сколько
        # принятых элементов корня пришло **не** с его собственного хоста (объявленный
        # поддомен, у которого нет своего корня). Вместе они отвечают числом на вопрос
        # «сколько дали поддомены» — и в живом прогоне, и в посмертном разборе.
        # Ключи появляются только при наличии данных: запись корня без поддоменов
        # остаётся байт-в-байт прежней.
        if frontier.out_of_scope_subdomains:
            record["out_of_scope_subdomains"] = {
                "count": sum(frontier.out_of_scope_subdomains.values()),
                "by_host": dict(sorted(frontier.out_of_scope_subdomains.items())),
                "examples": {
                    host: list(urls)
                    for host, urls in sorted(
                        frontier.out_of_scope_subdomain_examples.items()
                    )
                },
            }
        if frontier.accepted_foreign_hosts:
            record["accepted_foreign_hosts"] = dict(
                sorted(frontier.accepted_foreign_hosts.items())
            )
        # D3/F10: отказы scope у ``<loc>`` sitemap — провенанс, не ``errors``.
        # Ключ появляется только при наличии отказов: без sitemap запись байт-в-байт
        # прежняя. Классы ``host``/``path`` раздельны (F6).
        if sitemap_skips:
            record["skipped_out_of_scope"] = [dict(skip) for skip in sitemap_skips]
        if revalidation is not None:
            record["revalidation"] = dict(revalidation)
        truncation_records.append(record)
        # Наблюдаемость не должна умирать вместе с процессом: провенанс корня
        # персистится сразу (см. ``_persist_partial_crawl_provenance``).
        _persist_partial_crawl_provenance()

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
        product_id: str, roots: Sequence[str], items: Sequence[IncrementalItem]
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
            # F0: та же проводимость бюджета, что у остальных резолвов фазы (значение
            # здесь не используется — читается только ``revalidate_max_documents``).
            derive_extract_workers=derive_extract_workers,
            mem_budget_mb=mem_budget_mb,
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
                    derive_extract_workers=derive_extract_workers,
                    mem_budget_mb=mem_budget_mb,
                )
                _note_scheduling(limits)
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

    #: B3: наблюдения спуска (прогноз + исход) и члены контейнеров — провенанс фазы.
    #: Списки нужны ``PhaseResult`` и финальному reconciliation-pass; durable-запись идёт
    #: **write-through** (синки волкера, F1), а не только пачкой в конце фазы.
    archive_observations: list[ArchiveObservation] = []
    corpus_member_rows: list[CorpusMember] = []
    #: B3: зонд артефактов (класс/формат/размер/``page_count``/листинг) — до экстракции.
    artifact_probes: list[dict[str, Any]] = []

    #: Границы продукта в наблюдаемости (§B.1, аддитивно к фазовым start/end).
    #:
    #: Без них «продукт ещё в очереди» неотличим от «продукт потерян»: продукт-цикл
    #: строго последователен, и дренаж одного продукта может идти часами. Записи —
    #: **структурные** (dict) в ``PhaseResult.log_entries``: ``product_start`` до первого
    #: корня (``roots``/``seeds_declared``) и ``product_end`` после доводки хвостов
    #: (``roots``/``seeds_enqueued``/``session_items``/``fetched``/``failed``/
    #: ``elapsed_seconds``/``reason``). ``product_end`` пишется и при нулевом выходе —
    #: именно этот случай раньше был немым. Одна пара на продукт (не на URL).
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
    all_docs: list[CorpusDoc] = []
    chunk_count = 0
    partitions_reused = 0
    all_physical_ok = True
    #: ``product_id -> True``, если партиция продукта физически зафиксирована
    #: (``build``/переиспользование). Строка ``index_manifest`` пишется только для них.
    physically_ok: dict[str, bool] = {}

    #: Наблюдаемость: сеанс краула закрыт (финализация — одна на фазу, см. ``_finalize_crawl_session``).
    session_finalized = False

    def _finalize_crawl_session() -> None:
        """L1a: финализация сеанса (``COMPLETED`` без сбоев, иначе ``PARTIAL``).

        **Граница — краул, а не фаза.** Сеанс закрывается, когда дренированы все продукты
        (последний ``product_end``), а не когда закончилась фаза корпуса: эмбеддинг и сборка
        индекса — работа фазы, к сеансу краула отношения не имеющая. Пока
        ``finalize_session`` стоял после Pass B, сеанс висел ``running`` всё время
        эмбеддинга и ``stale_reason`` (heartbeat, порог 600 с) называл ложную причину у
        живого прогона: 2026-10-25 после ``crawl product end`` сеанс «протух» на 1280 с при
        505 % CPU.

        Инвариант :class:`_PhaseLifecycle` сохранён: ``completed`` ставится **только** после
        штатной финализации. Исключение до неё оставляет ``False``, и обёртка закрывает
        сеанс терминально (:func:`_finalize_interrupted_session`); исключение после неё
        (эмбеддинг/сборка/композиция) легитимный ``completed``/``partial`` не переписывает.
        """
        nonlocal session_finalized
        if session is None or session_finalized:
            return
        session_finalized = True
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
            errors.append(f"finalize_session({session}): {type(exc).__name__}: {exc}")
        else:
            # Штатный финал: обёртке больше нечего закрывать (иначе ``finally``
            # перезаписал бы ``completed``/``partial`` на терминальный сбой).
            if lifecycle is not None:
                lifecycle.completed = True

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
    #: Наблюдённое расписание экстракции: максимум разрешённого окна по корням, максимум
    #: рециклинга и признак «окно выведено из бюджета хотя бы на одном корне».
    observed_extract_workers: int | None = None
    observed_extract_max_tasks: int | None = None
    observed_extract_derived = False

    def _note_scheduling(limits: CrawlLimits) -> None:
        """Запоминает разрешённое окно экстракции корня (F0, наблюдаемость расписания).

        Максимум по корням, а не последнее значение: пул — фазовый по ключу
        ``(уровень, окно)``, и фактическая ширина параллелизма фазы — самая широкая из
        использованных. ``derived`` — «хотя бы на одном корне» (§2.10): тогда источник
        расписания в метриках — бюджет памяти, а не воля оператора.
        """
        nonlocal observed_extract_workers, observed_extract_max_tasks
        nonlocal observed_extract_derived
        if limits.extract_workers is not None:
            candidate = int(limits.extract_workers)
            if observed_extract_workers is None or candidate > observed_extract_workers:
                observed_extract_workers = candidate
        if limits.extract_max_tasks_per_child is not None:
            candidate_tasks = int(limits.extract_max_tasks_per_child)
            if (
                observed_extract_max_tasks is None
                or candidate_tasks > observed_extract_max_tasks
            ):
                observed_extract_max_tasks = candidate_tasks
        if limits.extract_workers_derived:
            observed_extract_derived = True

    #: 13-manual-artifacts §3.3: ``product_id → [url]``, чей материал **предъявлен**
    #: объявленным артефактом и дошёл до корпуса. Это вход ``derive_corpus_coverage``:
    #: отказ краула по такому URL остаётся аудитом, но перестаёт делать покрытие
    #: неполным. Объявляется до цикла — нужен и на границе продукта, и в конце фазы.
    manual_urls_by_product: dict[str, list[str]] = {}
    for product_id in products:
        product_started = time.perf_counter()
        ingest = _ProductIngest(product_id=product_id)
        # I17: дедуп в пределах продукт-партиции (см. docstring модуля).
        seen_docs: dict[str, str] = {}
        seen_chunk_ids: set[str] = set()
        #: F13: durable-URL, уже заряженные в resume-бюджет **продукта** (первый корень-
        #: претендент побеждает). URL, лежащий в scope нескольких корней, не должен
        #: расходовать ``max_documents`` каждого из них — иначе бюджет исчерпывается досрочно.
        resume_counted: set[str] = set()
        product_roots = url_map.get(product_id, [])
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
        #: в :func:`_drain_root`; нужен для честной причины zero-source.
        product_seeds_enqueued = 0
        #: Живая граница продукта: до первого дренажа (продукт может идти часами).
        _emit_product("start", product_id)
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
            serp_seeds: Sequence[str] = (),
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
            (пусто без sitemap/без отказов) и :class:`_RootDrain` — хвост экстракции корня
            в фазовой очереди (C1). ``defer_tail=False`` (synthetic-бакеты ревалидации,
            вызывающий читает счётчики frontier сразу) — хвост доводится здесь же.
            """
            nonlocal product_seeds_enqueued
            limits = resolve_crawl_limits(
                root_url,
                app=crawl_app or CrawlConfig(),
                run=crawl_defaults,
                profiles=domain_profiles or {},
                derive_extract_workers=derive_extract_workers,
                mem_budget_mb=mem_budget_mb,
            )
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
                else _SitemapSeeds()
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
            #: C1/P7: ключ фазового пула — уровень документа + механизм PDF + окно
            #: экстракции. Пул один на прогон и переиспользуется всеми корнями/продуктами
            #: с тем же ключом (forkserver и веса конвертера платятся один раз);
            #: ``shutdown`` — один раз в ``finally`` фазы.
            pool_key: _ExtractPoolKey | None = (
                (level, pdf_pipeline, extract_window)
                if extract_window is not None
                else None
            )
            #: Исполнять ли экстракцию в пуле (снимается inline-fallback'ом при отказе).
            defer_extract = extract_window is not None and extraction_pool is not None
            #: C1: хвост этого корня в фазовой очереди доводки (счётчик + финализатор).
            root_drain = _RootDrain()
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
                if frontier.scope_admission(iurl) == SCOPE_OUT:
                    # P6b: терминальный артефакт вне path-prefix этим корнем тоже
                    # допускается, поэтому его resumed-элемент обязан заряжать бюджет —
                    # иначе повторный прогон тихо добывал бы лишнюю страницу сверх
                    # ``max_documents``.
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
            if seed_status == "queued":
                product_seeds_enqueued += 1
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
                    if extra_status == "queued":
                        product_seeds_enqueued += 1
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
                    product_seeds_enqueued += 1
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

            def _new_extract_pool() -> Executor:
                """Фабрика пула ключа ``(level, window)`` — одна на прогон (C1/L1).

                Поднята из ``_commit_or_defer`` в область корня: пул нужен **двум**
                потребителям — страницам и членам контейнеров (B3/P4). ``pools.executor``
                кэширует исполнителя по ключу, поэтому тело исполняется один раз.
                """
                if (
                    recycle_tasks is not None
                    and extraction_pool.supports_worker_recycling  # type: ignore[union-attr]
                ):
                    # P4/OOM: счётчик заданий на процесс — воркер пересоздаётся
                    # (initializer `_extract_worker_init` строит экстрактор заново).
                    return extraction_pool.pool_factory(  # type: ignore[union-attr]
                        extract_window, level, pdf_pipeline=pdf_pipeline,
                        max_tasks_per_child=recycle_tasks,
                    )
                # Счётчик рециклинга не задан — вызов без него (остальное то же).
                return extraction_pool.pool_factory(  # type: ignore[union-attr]
                    extract_window, level, pdf_pipeline=pdf_pipeline
                )

            def _descend_container(
                pid: str,
                work_url: str,
                page: RawPage,
                raw_path: Path,
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
                if defer_extract and extraction_pool is not None and pool_key is not None:
                    #: Пул этого спуска признан мёртвым (``2 × timeout`` без завершений):
                    #: остаток членов — видимый пропуск, а не 47 ожиданий по таймауту.
                    member_pool_dead = False

                    def _submit_member(task: MemberTask) -> Any:
                        """Submit извлечения члена; ``MemberOutcome`` — немедленный исход.

                        ``split`` проверяется **здесь**, в родителе: у воркера нет признака
                        ``supports_source_path``, и без этой проверки шов получил бы локальный
                        путь вместо URL (капитуляция была бы невидимой).
                        """
                        nonlocal member_pool_dead
                        unavailable = _member_extract_unavailable(root_extractor)
                        if unavailable is not None:
                            return unavailable
                        if member_pool_dead:
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
                        nonlocal member_pool_dead
                        try:
                            extracted = handle.result(timeout=_extract_timeout)
                        except TimeoutError:
                            member_pool_dead = pools.note_timeout(pool_key)
                            return MemberOutcome(
                                skip_reason=SKIP_EXTRACT_FAILED,
                                detail=f"extraction timeout ({_extract_timeout} s)",
                            )
                        except BrokenProcessPool:
                            member_pool_dead = True
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
                # B3: допущенный терминальный артефакт (документ **или** контейнер) проходит
                # **зонд** до экстракции: класс/формат/размер по байтам, ``page_count`` PDF
                # по структуре файла, листинг контейнера — по индексу (central directory).
                # Зонд — классификация, а не извлечение: тела членов он не читает.
                if _is_terminal_artifact(
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
                        _descend_container(pid, work_url, page, raw_path)
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
                if not defer_extract:
                    _commit_outcome(pid_arg, url_arg, depth_arg, outcome)
                    return

                assert pool_key is not None  # defer_extract ⇒ окно и ключ заданы
                try:
                    executor = pools.executor(pool_key, _new_extract_pool)
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
                    # SERP-находки этого корня: depth-0 seeds под тем же блэклистом
                    # и ``max_documents``, что и авторские/`<loc>` seeds.
                    serp_seeds=serp_seeds_by_root.get(root_url, ()),
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
            if frontier is not None and frontier.artifacts_outside_prefix_count:
                # P6b: видимость обхода path-prefix — рядом с ``skipped_blacklisted``.
                info["artifacts_outside_prefix"] = {
                    "count": frontier.artifacts_outside_prefix_count,
                    "examples": list(frontier.artifacts_outside_prefix),
                }
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
            if frontier is not None and frontier.artifacts_outside_prefix_count:
                # P6b: видимость обхода path-prefix — рядом с ``skipped_blacklisted``.
                info["artifacts_outside_prefix"] = {
                    "count": frontier.artifacts_outside_prefix_count,
                    "examples": list(frontier.artifacts_outside_prefix),
                }
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
            _persist_manual_audit(entry, doc_id=doc_id, raw_path=raw_path)
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
            elif product_seeds_enqueued == 0:
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
                f"seeds_enqueued={product_seeds_enqueued} — ячейки будут failed, не ❌"
            )
        product_events.append(
            {
                "event": "product_end",
                "product_id": product_id,
                "roots": len(drained_roots),
                "seeds_declared": len(product_roots),
                "seeds_enqueued": product_seeds_enqueued,
                "session_items": len(product_items),
                "fetched": product_fetched,
                "failed": product_failed,
                "docs": len(ingest.docs),
                # 13-manual-artifacts §3.3: «объявлено» и «дошло до корпуса» —
                # разные факты, и разницу нельзя выводить один из другого.
                "manual_declared": len(manual_entries),
                "manual_resolved": len(manual_resolved),
                "elapsed_seconds": round(time.perf_counter() - product_started, 3),
                "reason": product_reason,
            }
        )
        #: Живая граница продукта: тело завершено — в том числе при нулевом выходе
        #: (именно этот случай раньше был немым до конца фазы).
        _emit_product("end", product_id)

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
        all_docs.extend(ingest.docs)

        # Ф0/O3: покрытие продукта пишется на его границе, а не только в конце фазы.
        # Элементы сеанса продукта **финальны** (продукт-цикл строго последователен),
        # поэтому строка честна уже здесь: продукт, дошедший до ``product_end``,
        # сохраняет покрытие, даже если следующий продукт убьёт прогон. Конец фазы
        # перезаписывает строки (``INSERT OR REPLACE``) и остаётся авторитетным — он же
        # закрывает продукты, вообще не вошедшие в цикл (zero-source/coverage_missing).
        if session is not None:
            try:
                _persist_corpus_coverage(
                    store,
                    run_id,
                    session,
                    truncated_products,
                    _now_iso(),
                    products=[product_id],
                    reasons=zero_source_reasons,
                    resolved_manually=manual_urls_by_product,
                )
            except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
                errors.append(
                    f"corpus_coverage({product_id}): {type(exc).__name__}: {exc}"
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
            _persist_corpus_coverage(
                store,
                run_id,
                session,
                truncated_products,
                _now_iso(),
                products=products,
                reasons=zero_source_reasons,
                resolved_manually=manual_urls_by_product,
            )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(
                f"corpus_coverage({session}): {type(exc).__name__}: {exc}"
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
    _emit_progress()

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
        if not ingest.chunks:
            continue
        product_id = ingest.product_id
        product_chunks = ingest.chunks
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
                    "chunks": len(product_chunks),
                    "docs": len({c.doc_id for c in product_chunks}),
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
            extract_workers=observed_extract_workers,
            extract_max_tasks_per_child=observed_extract_max_tasks,
            scheduling_source=(
                "derived" if observed_extract_derived else "declared"
            ),
            workers_peak_rss_mb=(
                extraction_pool.peak_worker_rss_mb()
                if extraction_pool is not None and worker_rss_dir
                else None
            ),
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
    lifecycle: _PhaseLifecycle, exc: BaseException
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
    try:
        lifecycle.store.update_crawl_session_status(
            lifecycle.session_id,
            CrawlSessionStatus.INTERRUPTED,
            finished_at=_now_iso(),
            error=f"phase interrupted: corpus did not complete ({type(exc).__name__})",
        )
    except Exception:  # noqa: BLE001 — не маскируем исходное исключение
        pass


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
    lifecycle = _PhaseLifecycle()
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

