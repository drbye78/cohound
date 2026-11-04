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
  падает и не меняет код возврата;
* политика ``offline`` — **ноль сетевых вызовов** (ни фетча, ни условного GET, ни
  sitemap/feed-развёртки, ни внешнего поиска): корпус собирается из того же субстрата
  отдельным проходом, ``reuse_kind=reused``, экстрактор/чанкер не вызываются, а
  переиспользование остаётся **не** наблюдением этого прогона (``observed_sources=0``).

``state is None`` — прежнее поведение байт-в-байт (никаких чтений/записей субстрата,
пустой ``IndexBuild.doc_counters``, тот же ``index_build_id``).

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import contextlib
import datetime as _dt
import json
import time
from collections.abc import (
    Callable,
    Generator,
    Iterable,
    Iterator,
    Mapping,
    Sequence,
)
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from functools import partial
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.config.crawl_sources import CrawlSourcesConfig
from core.config.deploy import CrawlConfig, resolve_host_shard
from core.config.domain_profiles import DomainProfile
from core.domain import (
    ArchiveObservation,
    CorpusChunk,
    CorpusCoverage,
    CorpusDoc,
    CorpusMember,
    CrawlCorpusSource,
    CrawlItemStatus,
    CrawlNetworkMode,
    CrawlRunConfig,
    CrawlSessionItem,
    CrawlSessionStatus,
    CrawlStrategy,
    DocumentLevel,
    EmbedDevice,
    IndexManifestEntry,
    PdfPipeline,
    RefreshPolicy,
    RunProductStatus,
    RunScope,
)
from core.pipeline import (
    corpus_commit,
    corpus_drain,
    corpus_driver,
    corpus_ingest,
    extract_consume,
)
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
from core.pipeline.corpus_commit import PhaseLifecycle

# L2: расчёт/персистенция покрытия и статуса продукта вынесены в свой модуль. Здесь
# остаются имена, которые этот модуль читает сам, и публичный ``derive_corpus_coverage``
# (``__all__``; тесты и ``selfcheck`` адресуют его по ``corpus`` — ``noqa: F401``).
from core.pipeline.corpus_coverage import (  # noqa: F401
    HOST_OUT_OF_SHARD_REASON,
    ONLY_FAILED_REPAIR_REASON,
    ZERO_SOURCE_ALL_SEEDS_REJECTED,
    ZERO_SOURCE_NO_SEEDS,
    ZERO_SOURCE_NO_URLS_ENTRY,
    _final_product_status,
    _persist_corpus_coverage,
    _product_notes,
    derive_corpus_coverage,
)

#: L5 шаг 3: класс терминального отказа попыток вынесен в ``corpus_drain``.
from core.pipeline.corpus_drain import _MAX_ATTEMPTS_ERROR_KIND  # noqa: F401

# L3: идентичность сборки и эмбеддинг продукт-партиции вынесены в свой модуль.
# ``EMBED_BATCH_CHUNKS`` остаётся реэкспортом: это **публичное** имя модуля-владельца, и
# тесты адресуют его по ``corpus`` (``noqa: F401`` — внутри этого модуля оно не читается).
from core.pipeline.corpus_index import (  # noqa: F401
    EMBED_BATCH_CHUNKS,
    _CorpusStageTimings,
    _derive_partition_build_id,
    _embed_and_build,
    _EmbedCensus,
    _EmbedThroughput,
    _make_embed_reporter,
    _partition_reuse_reason,
    _product_content_token,
    _product_extractor_token,
    format_embed_progress,
)

#: Из ``corpus_ingest`` этот модуль читает ``ProductIngest``/``terminal_failure_kind`` сам;
#: ``PARTIAL_REASON_UNSTATED`` адресуют тесты по ``corpus`` (``noqa: F401``).
from core.pipeline.corpus_ingest import (  # noqa: F401
    PARTIAL_REASON_UNSTATED,
    ProductIngest,
    terminal_failure_kind,
)
from core.pipeline.corpus_ingest import frozen_bytes_product as _frozen_bytes_product
from core.pipeline.corpus_ingest import offline_reuse_product as _offline_reuse_product
from core.pipeline.corpus_ingest import persist_docs as _persist_docs
from core.pipeline.corpus_persist import (
    emit_product,
    emit_progress,
    persist_manual_audit,
    save_run_product,
)

# L5 (шаг 1): мутабельное состояние фазы, которое раньше текло через ``nonlocal``.
# Отдельный модуль — чтобы следующие шаги (``corpus_ingest``/``corpus_commit``) могли
# аннотировать им свои функции, не создавая цикла с ``corpus``.
from core.pipeline.corpus_state import CorpusCounters
from core.pipeline.crawl_frontier import (
    CrawlLimits,
    Frontier,
    normalize_scope_host,
    resolve_crawl_limits,
)
from core.pipeline.crawl_policy import (
    CRAWL_POLICY_SKIPPED_REASON,
    CrawlPolicyResolution,
    PolicyExecution,
)
from core.pipeline.crawl_session import (
    default_session_id,
    open_session,
    prior_items,
    read_provenance,
    truncation_summary,
)
from core.pipeline.crawl_sources import (
    CrawlTarget,
    resolve_crawl_target,
)
from core.pipeline.digests import (
    corpus_capture_digest,
    corpus_doc_set_digest,
    coverage_digest,
)
from core.pipeline.embed_cache import EmbedCache
from core.pipeline.extract_cache import ExtractionCache

#: F5 (17 §4.1): конверсия — работа потребителя очереди, а не краула. Доводка члена
#: контейнера живёт в ``extract_consume``; алиас — не удобство:
#: ``tests/pipeline/test_corpus_descent.py`` подменяет ``corpus._ingest_member_doc``, и
#: вызов обязан идти через атрибут **этого** модуля (параметр ``ingest_member_doc``
#: читается из глобала в точке создания носителя).
from core.pipeline.extract_consume import ingest_member_doc as _ingest_member_doc  # noqa: F401
from core.pipeline.extract_queue import (
    EXTRACT_HEARTBEAT_S,
    DiskHeadroomGuard,
    ExtractQueue,
)

# L1: runtime экстракции вынесен в свой модуль. Здесь остаются имена, которые этот модуль
# читает сам (``_EXTRACT_MAX_POOL_REBUILDS``, ``_ExtractPools``, ``ExtractionPool``), и
# публичный ``set_worker_extractor``: его адресуют тесты по ``corpus`` (``noqa: F401``).
from core.pipeline.extraction_runtime import (  # noqa: F401
    _EXTRACT_MAX_POOL_REBUILDS,
    ExtractionPool,
    _ExtractPools,
    set_worker_extractor,
)
from core.pipeline.incremental import IncrementalItem
from core.pipeline.index_build import IndexBuild
from core.pipeline.liveness import Heartbeat
from core.pipeline.manual_artifacts import (
    ManualArtifacts,
)
from core.pipeline.refresh_modes import dispatch_refresh
from core.pipeline.resources import ResourcesObservation, summarize_chunks_per_doc
from core.pipeline.runner import PhaseResult, PhaseStatus, sorted_error_lines
from core.ports.chunker import ChunkerPort
from core.ports.container import ContainerPort
from core.ports.crawl_executor import (
    CrawlExecutorPort,
)
from core.ports.crawler import CrawlerPort
from core.ports.embedder import EmbedderPort
from core.ports.extractor import ExtractorPort
from core.ports.index_builder import IndexBuilderPort
from core.ports.poller import PollPort
from core.ports.state import (
    CorpusCaptureRow,
    IndexCompositionRow,
    IndexPartitionRow,
    ProductStatePort,
)
from core.ports.types import (
    FetchMode,
    FetchPlan,
)
from core.util.index_partition import build_root as _partition_root
from core.util.rate_limit import RateLimiter, normalize_host

__all__ = [
    "ExtractionPool",
    "IndexBuild",
    "build_corpus",
    "canonicalize_doc_urls",
    "derive_corpus_coverage",
]


#: Phase 3 B2: максимум попыток подачи работ исполнителю при back-pressure
#: (``accepted=False``). После исчерпания — изолированный ``FAILED`` (``source_unavailable``),
#: а не тихий провал всего прогона.

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

#: Причина нулевого наблюдения корпуса при политике ``offline`` (``--refresh offline``).
#:
#: Это **не** ``no_seeds``: seed'ы есть, но сети в этой политике нет — документы берутся
#: из durable-субстрата (``reuse_kind=reused``), а наблюдением это не является.
#: ``observed_sources`` поэтому остаётся нулём, ``complete`` — ``False``: оффлайн-прогон
#: не вправе выдавать себя за полный корпус. Токен наблюдательный (колонка ``reason`` —
#: свободная строка), но названный: молчаливый ``no_seeds`` утверждал бы, что источников
#: не было, а они были.
COVERAGE_OFFLINE_NO_OBSERVATION = "offline_no_network_observation"

#: То же для ``frozen`` (план 19 §P3): корпус собран пере-извлечением сохранённых байтов,
#: но наблюдений **этого** прогона нет (сети не было). Отдельный токен, потому что
#: «заморожен корпус» и «оператор просил offline» — разные провенансы, и выводить один
#: из другого читатель не должен.
COVERAGE_FROZEN_NO_OBSERVATION = "frozen_no_network_observation"

#: Причина неисполненных раундов SERP-контура при ``offline``: поиск — тоже сеть.
_SERP_OFFLINE_REASON = "offline_refresh_policy"

#: То же для ``frozen`` (план 19 §P3): ноль запросов — значит и внешний поиск не
#: исполняется. Отдельный токен, потому что «заморожен корпус из байтов» — не «оператор
#: просил offline»: читатель провенанса не должен выводить одно из другого.
_SERP_FROZEN_REASON = "frozen_refresh_policy"


def _as_refresh_policy(value: str) -> RefreshPolicy:
    """Приводит строку политики наблюдения к :class:`RefreshPolicy`.

    Неизвестное значение — **ошибка**, не тихий ``auto``: опечатка в политике иначе
    включала бы сеть (AGENTS §2, план 19 §6.5). Значение проверяется на входе; сюда
    попадает уже разрешённая строка, поэтому ``ValueError`` — сигнал дефекта проводки,
    а не пользовательского ввода.
    """
    try:
        return RefreshPolicy(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in RefreshPolicy)
        raise ValueError(
            f"неизвестная политика обновления {value!r}: допустимо {allowed}"
        ) from exc


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


CANONICAL_URL_CHANGED_NOTE = (
    "corpus: {product_id}: канонический адрес {count} документов переопределён "
    "минимумом url среди равных doc_hash (дедуп не зависит от порядка наблюдения)"
)


def canonicalize_doc_urls(
    product_id: str,
    *,
    items: Mapping[tuple[str, str], CrawlSessionItem],
    store: Any,
    run_id: str | None,
    errors: list[str],
    docs: Sequence[CorpusDoc] = (),
) -> int:
    """Согласует ``corpus_docs.url`` с каноническим адресом: ``min(url)`` среди равных ``doc_hash``.

    17 §6 (F3): «победитель контент-дедупа — запрос, а не гонка». Кто первый доехал до
    ``ingest_one``, тот и записал документ (``seen_docs``), а при нескольких полосах
    «первый» — это момент готовности страницы, то есть состав корпуса зависел бы от
    планировщика. Поэтому после персиста документов продукта адрес **согласуется**
    запросом: ``(product_id, doc_hash) → {url}``, победитель — :func:`min` множества.

    **Носитель.** Группы берутся из ``crawl_session_items.doc_hash`` (durable: переживает
    resume и повторный ингест), а не из памяти фазы: словарь ``seen_docs`` начинается
    пустым при реюзе/``--only-failed``, и «победитель» снова стал бы случайным.
    ``corpus_docs.content_hash`` для группировки не годится — это тот же ключ, но
    ``crawl_session_items.content_hash`` хранит хеш **сырых байтов** (I19), а дедуп идёт по
    хешу **извлечённого текста**: разные величины, и по байтам алиасы не собрать.

    **Границы (и почему проход ничего не портит).**

    * документ с единственным URL — группа из одного элемента, ничего не меняется;
    * строка истории без ``doc_hash`` (БД прошлой схемы) в группу **не входит**: «неизвестно»
      не выдаётся за минимум. Если текущий адрес документа не входит в известную группу,
      документ **пропускается**: значит часть адресов этого текста неизвестна, и минимум по
      неполному множеству был бы подменой;
    * реюз версии документа из прошлого прогона (``304``/retained-байты/offline) не мешает:
      алиас этого прогона получает ``doc_hash`` при ингесте, поэтому попадает в группу
      этого прогона.

    Меняется **только** поле ``url`` строки корпуса: ``doc_id``, ``content_hash``, версия,
    retained-байты и признак частичности — идентичность и провенанс документа, а не адрес.
    Счётчик ``duplicates``/семантика алиасов не затрагиваются.

    Изменение артефакта **видимо**: вызывающий пишет число изменённых адресов в лог фазы
    (:data:`CANONICAL_URL_CHANGED_NOTE`), а не подменяет адрес молча.

    Проход идемпотентен и не зависит от порядка: повторный вызов на согласованном корпусе
    возвращает ``0`` (условие ``url <> ?`` в сторе), а :func:`min` по множеству не зависит
    от порядка обхода.

    ``docs`` — документы продукта в памяти фазы (``ingest.docs``): адрес переписывается и
    там, иначе ``product_partial``/``observed_documents`` назвали бы документ по адресу,
    которого в ``corpus_docs`` уже нет. Необязателен: согласование durable-строки от него
    не зависит.

    :returns: число документов, чей канонический адрес был переписан.
    """
    if store is None or run_id is None:
        return 0
    groups: dict[str, set[str]] = {}
    for (item_product_id, item_url), item in items.items():
        if item_product_id != product_id or not item.doc_hash:
            continue
        groups.setdefault(item.doc_hash, set()).add(item_url)
    # (а) документ с единственным URL согласовывать не с чем, поэтому пары фильтруются
    # **до** чтения корпуса: прогон без дедупа не приобретает ни одного лишнего запроса
    # (наблюдаемость важна и в обратную сторону — молчаливая цена чтения тоже видна).
    contested = {
        doc_hash: urls for doc_hash, urls in groups.items() if len(urls) > 1
    }
    if not contested:
        return 0
    changed = 0
    try:
        urls_by_doc = {
            doc.doc_id: doc.url
            for doc in store.iter_corpus_docs(run_id)
            if doc.product_id == product_id
        }
        for doc_hash in sorted(contested):
            urls = contested[doc_hash]
            doc_id = f"{product_id}:{doc_hash}"
            current = urls_by_doc.get(doc_id)
            if current is None or current not in urls:
                # (б) текущий адрес не принадлежит известной группе: часть адресов этого
                # текста не записана (старая БД), и минимум по неполному множеству был бы
                # выдуманным. Пропуск виден в счётчике ``changed``, а не маскируется.
                continue
            canonical = min(urls)
            if canonical == current:
                continue
            if store.set_corpus_doc_url(doc_id, canonical):
                changed += 1
                # Память продукта говорит то же, что durable-строка: иначе строка продукта
                # и покрытие назвали бы документ по адресу, которого в корпусе уже нет.
                for doc in docs:
                    if doc.doc_id == doc_id:
                        doc.url = canonical
    except Exception as exc:  # noqa: BLE001 — изоляция персистенции (контракт фазы)
        errors.append(
            f"canonical_url({product_id}): {type(exc).__name__}: {exc}"
        )
    return changed


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
class ProductContext:
    """Контекст продукта когорты: всё, что живёт от планирования до закрытия (F9).

    **Зачем объект.** Продукт был итерацией цикла: его состояние (накопитель ингеста,
    дедуп-множества, счётчик seed'ов, бакеты стадий, ревалидационные группы) рождалось и
    умирало внутри одной итерации. Когортный дренаж (F9 плана 17) разрывает итерацию на
    две половины: **планирование** (без сети, все продукты сразу) и **закрытие** (хвост
    продукта в момент завершения его последнего корня). Между ними состояние обязано
    жить, и владельцем его полей становится продукт, а не переменная цикла: иначе
    поздний хвост читал бы чужой продукт (ровно тот дефект, от которого защищали
    B023-дефолты, когда половин было две).

    **Почему не frozen.** Поля-контейнеры мутируются корнями продукта (это общее
    состояние, а не копия), а поля-значения (результаты предпрохода, ``started_at``)
    заполняются по ходу: объект живёт дольше одного шага. Границу владения держит не
    ``frozen``, а то, что предпроход пишет поля **один раз**, до первого корня.

    **Что здесь продуктовое, а что фазовое.** Фазовое (склад, сеанс, ошибки, очередь
    конверсии, очереди экстракции) остаётся в фазовых носителях и не копируется; здесь
    только то, у чего цена ошибки — атрибуция: чьи это документы, чей бюджет, чьё время.
    """

    #: Идентификатор продукта (`param_id`-независимый slug продукта).
    product_id: str
    #: Авторские корни продукта (``url_map[product_id]``) в авторском порядке.
    roots: Sequence[str]
    #: Корни, которые продукт реально обойдёт (пусто у ``offline``/ремонта/``consume_only``).
    author_urls: Sequence[str]
    #: Накопитель продукта (документы/чанки/счётчики ингеста).
    ingest: ProductIngest
    #: I17: дедуп ``content_hash → doc_id`` в пределах продукт-партиции.
    seen_docs: dict[str, str]
    #: I17: дедуп ``chunk_id`` в пределах продукт-партиции.
    seen_chunk_ids: set[str]
    #: F13: durable-URL, уже заряженные в resume-бюджет **продукта**.
    resume_counted: set[str]
    #: 09 §6.3: ``(корень, frontier)`` — владелец известного URL считается один раз.
    owner_scopes: list[tuple[str, Any]]
    #: Кэш владельца URL (самый специфичный корень-претендент).
    owner_cache: dict[str, str | None]
    #: I3/§5: наблюдённая судьба SERP-находок продукта.
    serp_ingest: dict[str, Any]
    #: Находки, разложенные по корням продукта :func:`assign_serp_seeds`.
    serp_seeds_by_root: Mapping[str, Sequence[str]]
    #: Корни, реально дренированные в этом продукте (leftover-группы каталогов).
    drained_roots: set[str]
    #: Сколько seed'ов поставлено в очередь **этого** продукта (носитель — в скоупе).
    seeds: corpus_drain.SeedTally
    #: Бакеты стадий продукта (наблюдаемость; носитель — этот продукт, а не фаза).
    timings: _CorpusStageTimings
    #: Продуктовый скоуп дренажа (разделяется корнями продукта, не копируется).
    scope: corpus_drain.ProductDrainScope
    #: Продуктовый скоуп потребителя очереди (тот же накопитель, те же дедуп-множества).
    consume: extract_consume.ProductScope
    #: Фазовый скоуп потребителя с бакетами **этого** продукта (копия ссылок, не состояния).
    consume_scope: extract_consume.ConsumerScope
    #: Момент границы продукта (``start``), ``None`` — продукт ещё не открыт.
    started_at: float | None = None
    #: 09 §6.3: итог оффлайн-прохода по продукту (``None`` — политика не ``offline``
    #: и не ``frozen_source=chunks``).
    offline_report: Any = None
    #: План 19 §P3: итог ``frozen_source=bytes`` (``None`` — иная политика/источник).
    #: Нужен хвосту: запись снятия (``corpus_capture``) пишется после покрытия, а группы
    #: дедупа и причины приходят из этого прохода.
    frozen_report: Any = None
    #: Группы ревалидации: до применения операторского потолка и после.
    all_revalidation_groups: Mapping[str, Sequence[str]] = field(default_factory=dict)
    revalidation_groups: Mapping[str, Sequence[str]] = field(default_factory=dict)
    all_orphan_buckets: Mapping[str, Sequence[str]] = field(default_factory=dict)
    orphan_buckets: Mapping[str, Sequence[str]] = field(default_factory=dict)
    #: Отложенные/пропущенные потолком ревалидации URL — по корню и по origin.
    deferred_by_root: Mapping[str, int] = field(default_factory=dict)
    deferred_by_origin: Mapping[str, int] = field(default_factory=dict)
    skipped_by_root: Mapping[str, int] = field(default_factory=dict)
    skipped_by_origin: Mapping[str, int] = field(default_factory=dict)
    #: Наблюдения потолка ревалидации (факт, а не ошибка).
    revalidation_deferred_total: int = 0
    revalidation_cap_fired: bool = False
    #: Объявленная ось полос продукта (``crawl.host_concurrency``).
    lane_axis: int | None = None
    #: Frontiers synthetic-бакетов продукта, дождавшиеся хвоста. Ключ — ``(вид, адрес)``:
    #: адрес один не различает catalog-leftover и orphan-бакет, а совпадение адресов
    #: подменило бы frontier одного наблюдения другим. Synthetic-корни идут тем же потоком
    #: планов (F9), поэтому их итог приходит в ``on_root_done`` раньше хвоста, а
    #: использовать его можно только там.
    synthetic: dict[tuple[str, str], Frontier] = field(default_factory=dict)
    #: Планированный work-set продукта (инкремент ‖ ремонт). Нужен оффлайн-проходу: он
    #: исполняется на **границе** продукта, а не в предпроходе, — иначе документы всех
    #: продуктов когорты лежали бы в памяти одновременно (``offline_reuse_product``
    #: наполняет ``ingest.docs``), и пик памяти рос бы с числом продуктов, а не с числом
    #: полос.
    incremental_items: Sequence[IncrementalItem] = field(default_factory=tuple)
    #: Хвост продукта уже исполнен (конверсия → Pass B → покрытие → строка продукта).
    #: Нужен одному решению: «краул когорты закончен» — это «закрыты **все** продукты»,
    #: а не «закрыт последний по порядку»: при объявленной оси последний по порядку
    #: продукт может закрыться, пока корни соседа ещё в полёте.
    closed: bool = False
    #: План 19 §P2: корни продукта, которые политика запретила (сеть) — строки для
    #: ``results[].notes``. Список — доказательство, что пропуск был **решением**, а не
    #: «пустым корпусом»: продукт остаётся на карте (I4), покрытие делает неполным.
    policy_skips: list[str] = field(default_factory=list)


#: Вид synthetic-бакета ревалидации: leftover-группа каталога (feed/channel раскрылся в
#: **другие** корни, и привязка к авторскому URL каталога не совпала ни с одним drain'ом)
#: ‖ orphan-бакет (известные URL продукта вне scope всех авторских корней, 09 §6.3).
#: Вид входит в ключ наблюдения: один и тот же адрес может встретиться в обоих.
_SYNTHETIC_CATALOG = "catalog"
_SYNTHETIC_ORPHAN = "orphan"


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
    tail_total = 0.0
    for product_id, elapsed_s, timings in rows:
        elapsed_total += elapsed_s
        tail_total += timings.product_tail_s
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
            # F9 плана 17: сколько главный поток провёл в хвосте продукта (выдача новых
            # волн на это время стояла). Публикуется **рядом**, а не бакетом: интервал
            # пересекается с Pass B, и включение в сумму сделало бы остаток окна ложным.
            "product_tail_s": round(timings.product_tail_s, 3),
            "overhead_s": round(elapsed_s - timings.stage_sum(), 3),
        }
    total = {
        "elapsed_seconds": round(elapsed_total, 3),
        **{name: round(buckets[name], 3) for name in names},
        "extract_workers": workers,
        "product_tail_s": round(tail_total, 3),
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
    products: Sequence[str] | None = None,
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
    # ``products`` — сужение по политике краула (план 19 §P2): продукт с полностью
    # запрещённой сетью не спрашивается вовсе. ``None`` — все продукты ``url_map``
    # (поведение прежнее).
    products = sorted(str(pid) for pid in (products if products is not None else url_map))
    if not products:
        return _serp_disabled_observation(serp_source, serp_mode), {}
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
        # Причина неисполнения называется, когда она есть: «контур выключен» без неё
        # читалось бы как решение оператора, а ``offline`` — политика прогона, при
        # которой внешний поиск не трогается наравне с краулом.
        reason = rounds.get("not_executed_reason")
        return (
            "serp: контур выключен "
            f"(источник: {rounds.get('source', 'default')}"
            + (f", причина: {reason}" if reason else "")
            + ") — вызовов внешнего поиска нет"
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
    frozen_source: str | None = None,
    seeds_digest: str | None = None,
    crawl_policy: CrawlPolicyResolution | None = None,
    only_failed_repairs: Mapping[str, list[str]] | None = None,
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
    crawl_only: bool = False,
    consume_only: bool = False,
    consume_worker: str | None = None,
    consume_heartbeat_s: float | None = None,
    node_id: str | None = None,
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
        (``root_lifecycle``): BFS-раунды, ``Frontier``, блэклисты, ``max_depth``/``max_documents``
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
        GET (валидаторы не отправляются); ``offline`` — **ноль сетевых вызовов**: ни
        фетча, ни условного GET, ни sitemap/feed-развёртки, ни внешнего поиска. Корпус
        собирается из durable-субстрата отдельным проходом
        (:func:`core.pipeline.corpus_ingest.offline_reuse_product`), документы получают
        ``reuse_kind=reused`` и ``http_status=NULL``, экстрактор и чанкер не вызываются.
        Авторский корень, известный ``url_state``, тоже переиспользуется; корень, которого
        в состоянии нет, — видимая ошибка (сеть в этой политике отсутствует). Покрытие
        такого прогона неполно по построению: наблюдений **этого** прогона нет
        (``observed_sources=0``, причина ``offline_no_network_observation``).
    :param crawl_policy: разрешённая политика краула (:class:`CrawlPolicyResolution`,
        план 19 §P2) — **тот же** объект, что дал ``crawl_policy_digest`` для ``run_id``.
        Гейт стоит в начале ``_author_root_plans``, то есть **до** резолва цели,
        развёртки каталога (feed/channel — сеть) и sitemap, и до сборки synthetic-корней.
        ``deny``-корень не делает ни одного запроса; корпус ``chunks``-корня берётся тем
        же generalized-проходом, что ``offline`` («продукт → корень»). Каждый пропуск
        делает покрытие неполным и назван токеном ``crawl_policy_skipped``.
        ``None`` — политики нет: каждый корень ``fetch``+``network`` (поведение прежнее).
    :param only_failed_repairs: ``product_id → [url, ...]`` — work-set **ремонта**
        (``--only-failed``, межпрогонный носитель ``url_state``; резолвится composition
        root'ом: ``core.cli.context._only_failed_work_set``). Для названного продукта
        (а) авторские корни **не дренятся** — ремонт не полный обход, поэтому BFS и
        sitemap не исполняются, а объявленные корни остаются только пространством
        scope-инференса; (б) work-set строится из переданного множества
        (:func:`core.pipeline.corpus_ingest.plan_repair_items`) **независимо** от
        ``product_modes``, и каждый элемент идёт depth-0 листом через
        ``Frontier.add_revalidate_seed`` (вне бюджета ``max_documents``). ``None``/пустой
        словарь ⇒ поведение прежнее, байт-в-байт.
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
    :param crawl_only: F2 (17 §4) — **краул без конверсии**. Frontier, фетч, retention и
        элемент сеанса ``FETCHED`` исполняются как обычно, а каждый добытый документ
        становится строкой durable-очереди ``extract_queue``; зонд артефакта, спуск в
        контейнеры, экстракция, нарезка, дедуп и запись ``corpus_docs``/``corpus_chunks``
        **не** выполняются. Смысл: «краул выполнен» (элементы терминальны, frontier
        исчерпан, усечение записано) перестаёт означать «корпус собран» — сборка
        становится работой потребителя очереди, и время краула измеряется без чужой
        стоимости. Счётчики ``max_documents``/``pages_fetched``/усечения те же, что в
        полном прогоне: это тот же краул, а не другой. Требует активного сеанса
        (``store`` + ``run_id``) и **несовместим** с ``extraction_pool`` — извлечение в
        режиме «без извлечения» было бы молчаливым противоречием (:raises ValueError).
    :param consume_only: F6 (17 §6) — **только конверсия**, без краула: та же фаза, но
        сетевые/дренажные шаги не исполняются, а ``extract_consume.consume`` читает
        durable-очередь ``extract_queue`` уже завершённого сеанса. Это вход
        ``hound extract consume`` и **не второй потребитель**: логика конверсии — та же
        функция ``consume``, отличается только тем, что корни не дренируются. Усечение
        (``truncated``) восстанавливается из durable-провенанса сеанса, а не выводится
        заново: прогон без краула не вправе объявить усечённый корпус полным (иначе
        ``coverage.complete=True`` и ложный ``❌`` по I2/I2b). Сеанс здесь **не**
        финализируется — его закрывает краул.
    :param node_id: F7 (17 §2) — **какой узел я**: имя из объявленной карты
        ``deploy.yaml: crawl.shards`` (выбор оператора ``--node``/``COHOUND_NODE``,
        резолвится composition root; фаза окружение не читает). При объявленной карте
        crawler берёт **только** host'ы своего шарда, отказ чужому host'у виден счётчиком
        ``host_out_of_shard``. Шардированный узел видит **часть** корпуса, поэтому сеанс
        закрывается ``partial`` с названной причиной, а покрытие продуктов, чьи host'ы
        покрыты не полностью, — не ``complete``: иначе непокрытый host читался бы как
        ``exhaustive`` и ``❌`` публиковался бы из неполного корпуса (I2b/I2c).
        ``None`` — узел не выбран (при объявленной карте это отказ ещё на входе CLI, в
        фазе — «шардов нет»).

    Возвращает ``PhaseResult`` со ``status='ok'`` (даже при изолированных ошибках),
    ``cells=[]`` (Ш5 не производит ячеек) и ``log_entries=[IndexBuild, ResourcesObservation]``
    (сборка + счётчики; F0 — наблюдённые ресурсные факты фазы: расписание экстракции,
    чанки на документ, пик воркеров).

    :raises ValueError: если ``urls`` задан, но не является отображением; если
        ``crawl_only`` запрошен вместе с ``extraction_pool`` или без сеанса.
    """
    if urls is not None and not isinstance(urls, Mapping):
        raise ValueError("urls обязан быть отображением product_id → список URL")
    # 17 §4 (F2): режим «без извлечения» несовместим с исполнителем извлечения. Молча
    # проигнорировать пул нельзя: прогон объявил бы краул без конверсии, а исполнял
    # конверсию (или наоборот) — и это было бы неотличимо в артефактах.
    if consume_only and crawl_only:
        raise ValueError(
            "consume_only и crawl_only взаимоисключающи: первый конвертирует очередь "
            "завершённого сеанса, второй эту очередь только наполняет"
        )
    if crawl_only and extraction_pool is not None:
        raise ValueError(
            "crawl_only несовместим с extraction_pool: режим «краул без конверсии» "
            "не извлекает документы, а пул экстракции существует ровно для извлечения"
        )

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

    # ------------------------------------------------------------------ #
    # F7 (17 §2): шард узла — фильтр «какие host'ы мои» и честность корпуса
    # ------------------------------------------------------------------ #
    # Карта шардов объявлена в ``deploy.yaml: crawl.shards``; **какой узел я** —
    # операционный выбор (резолвится composition root'ом), поэтому сюда приходит уже
    # значение, а не окружение. Фильтр не подменяет scope (``Frontier.host_in_scope``):
    # он спрашивает «моё ли это железо», а scope — «принадлежит ли URL этому корню».
    #
    # В режиме ``consume_only`` краула нет вовсе, поэтому фильтр шарда не применяется и
    # узел не требуется: очередь уже durable, а конверсия права на host не спрашивает.
    # Честность при этом **не** теряется — она восстанавливается из провенанса сеанса
    # (``truncated_products``/``shard.filtered_products``) ниже, а не выводится заново.
    declared_shards = None if consume_only else getattr(crawl_app, "shards", None)
    host_shard = resolve_host_shard(declared_shards, node_id)
    shard_hosts = None if host_shard is None else host_shard.hosts
    #: Продукты, чьи **объявленные** host'ы покрыты шардом не полностью: их корпус этим
    #: узлом заведомо не собран целиком (часть host'ов принадлежит другому узлу).
    #: Вычисляется из объявленного входа (seed-корни + объявленные поддомены), то есть
    #: до фетча — поэтому «шардированный узел видит часть корпуса» честно читается даже
    #: тогда, когда чужой host так и не встретился в ссылках.
    shard_filtered_products: set[str] = set()
    shard_block: dict[str, Any] | None = None
    if host_shard is not None:
        for product_id, product_urls in url_map.items():
            product_declared = {
                normalize_host(urlsplit(str(url)).hostname or "")
                for url in product_urls
            } | {
                normalize_host(host)
                for host in scope_hosts_by_product.get(product_id, frozenset())
            }
            if product_declared - set(shard_hosts or ()):
                shard_filtered_products.add(product_id)
        shard_block = {
            "node": host_shard.node,
            "hosts": sorted(shard_hosts or ()),
            "declared_nodes": sorted(
                str(getattr(shard, "node", "")) for shard in (declared_shards or ())
            ),
            "filtered_products": sorted(shard_filtered_products),
            # ``filtered`` — «этот узел видит не весь объявленный корпус». Тогда сеанс
            # закрывается ``partial``, а покрытие продукта — не ``complete``: иначе
            # непокрытый host читался бы как ``exhaustive``, и ``❌`` получил бы право
            # на публикацию по I2b/I2c (вердикт из неполного корпуса — недопустим).
            "filtered": bool(shard_filtered_products),
            "reason": HOST_OUT_OF_SHARD_REASON if shard_filtered_products else None,
        }
        if host_shard.empty:
            # Пустой шард — объявленное «узел ничего не ведёт». Не ошибка конфига, но и
            # не тишина: строка видна в артефакте фазы (``errors``), а не только в логе.
            errors.append(
                f"shards: узел «{host_shard.node}» объявлен без host'ов — этот узел "
                "не будет краулить (видимый простой, а не тихий: 17 §2)"
            )
    elif node_id:
        # Обратный случай: узел задан, карты нет. Это не ошибка (env может стоять на
        # хосте целиком), но и не «взял все host'ы молча» — факт назван строкой.
        errors.append(
            f"shards: узел «{node_id}» задан, но crawl.shards не объявлены — "
            "фильтра по шарду нет, краул ведёт все host'ы"
        )

    # 09 §6.3/§9 (MVP-3b): политика наблюдения и режимы продуктов. ``state is None`` —
    # legacy-путь: все три аргумента игнорируются целиком (поведение байт-в-байт).
    # Резолвится **до** SERP-контура и дренажа: ``offline``/``frozen`` — «наблюдать
    # нечем и не будем», то есть ни фетча, ни условного GET, ни sitemap/feed-развёртки,
    # ни внешнего поиска. Корпус в этом случае собирается из durable-субстрата: ``offline``
    # и ``frozen_source=chunks`` — из чанков
    # (:func:`core.pipeline.corpus_ingest.offline_reuse_product`), ``frozen_source=bytes`` —
    # пере-извлечением сохранённых байтов
    # (:func:`core.pipeline.corpus_ingest.frozen_bytes_product`).
    #
    # План 19 §P3: решения берутся из **одной** таблицы (:func:`dispatch_refresh`), а не
    # из `is OFFLINE`-веток: новое значение политики без строки таблицы падает, а не
    # получает молча сетевое поведение.
    policy = _as_refresh_policy(refresh_policy)
    execution = dispatch_refresh(policy, frozen_source=frozen_source)
    offline = policy is RefreshPolicy.OFFLINE
    frozen = policy is RefreshPolicy.FROZEN
    #: Ни одного сетевого вызова: и `offline`, и `frozen`. Имя говорит **факт**, а не
    #: значение политики: от него зависят SERP-контур, авторские корни и покрытие.
    no_network = not execution.network
    #: Откуда корпус при отсутствии сети: `chunks` (offline) либо ось frozen.
    corpus_source_no_network = execution.corpus_source

    def _policy_execution(product_id: str, url: str) -> PolicyExecution:
        """Исполнение политики для адреса (``fetch``+``network`` без политики).

        Одна точка чтения: и SERP-фильтр, и авторские корни, и synthetic-бакеты
        спрашивают **эту** функцию, а не пересобирают решение по-своему. Без политики —
        ``fetch``+``network`` без области: «запретить по умолчанию» было бы решением,
        которого оператор не принимал.

        ``frozen`` перекрывает политику **всех** корней (план 19 §5: сеть запрещена всем;
        ``auto`` и ``on_request`` обрабатываются одинаково). Различие с ``network_denied``
        — в провенансе, а не в запросах; здесь оно намеренно не различается: политика
        поверхностей корней уже посчитана и в дайджест вошла, а запрет сети — условие
        этого прогона.
        """
        if frozen:
            return PolicyExecution(
                network_mode=CrawlNetworkMode.DENY,
                corpus_source=corpus_source_no_network,
            )
        if crawl_policy is None:
            return PolicyExecution()
        return crawl_policy.execution_for(product_id, url)

    def _policy_denied_products() -> set[str]:
        """Продукты, у которых **все** авторские корни запрещены сетью.

        Нужен SERP-контуру: поиск — сетевой вызов **продукта**, и продукт, у которого
        сеть запрещена целиком, не спрашивается вовсе. Продукт с хотя бы одним живым
        корнем остаётся в контуре (находки разложатся в его корни и будут отфильтрованы
        по корню ниже).
        """
        denied: set[str] = set()
        for pid, roots in url_map.items():
            if roots and all(
                _policy_execution(str(pid), str(root)).denied for root in roots
            ):
                denied.add(str(pid))
        return denied

    # --- SERP-контур (I3/§5): поиск → допуск → расширение объявленного seed-набора --- #
    # Контур исполняется **внутри** фазы корпуса (не отдельной фазой ``PHASES``) и до
    # дренажа: найденные URL входят в тот же набор, что и авторские, поэтому проходят
    # обычный путь retention → hash → index (I19). Почему не «после сборки» — см.
    # докстринг :func:`_run_serp_contour`.
    serp_observation: dict[str, Any] = _serp_disabled_observation(serp_source, serp_mode)
    #: Найденные SERP документы, разложенные по авторским корням: ``product → root → urls``.
    #: Второй проход по ним — обычный дренаж тех же корней (глубина 0), не «долив в индекс».
    serp_seeds_by_product: dict[str, dict[str, list[str]]] = {}
    #: Судьба находок по продуктам (``enqueued``/``dup``/``blacklisted``).
    serp_ingest_by_product: dict[str, dict[str, Any]] = {}
    #: 09 §6.3: ``offline`` — ноль сетевых вызовов, и внешний поиск тоже сеть. Контур не
    #: исполняется, но и не молчит: наблюдение называет причину (``not_executed_reason``),
    #: иначе «контур выключен» читалось бы как решение оператора, а не как политика прогона.
    #: План 19 §P2: продукты, которым политика **разрешила** сеть. ``None`` — политики
    #: нет: в контуре участвуют все продукты (поведение прежнее). Продукт, у которого
    #: запрещены все авторские корни, не спрашивается вовсе: поиск — сетевой вызов
    #: **продукта**, и «запрещённый» не должен платить за соседа.
    serp_denied_products = (
        _policy_denied_products() if crawl_policy is not None else set()
    )
    serp_allowed_products: list[str] | None = (
        [pid for pid in sorted(url_map) if pid not in serp_denied_products]
        if crawl_policy is not None
        else None
    )
    #: План 19 §P2: строки о пропусках по политике (операторский канал). Живут рядом с
    #: ``offline_notes``, потому что оба называют «почему корпуса этого прогона нет/мало».
    policy_notes: list[str] = []

    if no_network and serp_rounds:
        serp_observation["rounds"]["declared"] = int(serp_rounds)
        serp_observation["rounds"]["not_executed"] = int(serp_rounds)
        # Причина — политика прогона: ``offline`` и ``frozen`` различаются провенансом,
        # а не наличием запросов, и «почему контур не исполнен» обязано называть ту,
        # которая была объявлена.
        serp_observation["rounds"]["not_executed_reason"] = (
            _SERP_OFFLINE_REASON if offline else _SERP_FROZEN_REASON
        )
    elif serp_rounds and consume_only:
        # F6: конверсия чужой очереди — сетевой контур поиска не её работа. Объявленные
        # раунды называются неисполненными: «не исполнено» ≠ «выключено оператором».
        serp_observation["rounds"]["declared"] = int(serp_rounds)
        serp_observation["rounds"]["not_executed"] = int(serp_rounds)
        serp_observation["rounds"]["not_executed_reason"] = "consumer_only"
    elif serp_rounds and serp_allowed_products is not None and not serp_allowed_products:
        # План 19 §P2: политика запретила сеть **всем** продуктам прогона — внешний
        # поиск не исполняется. Причина — собственный токен: «контур выключен политикой»,
        # а не «``offline``» и не «оператор выключил».
        serp_observation["rounds"]["declared"] = int(serp_rounds)
        serp_observation["rounds"]["not_executed"] = int(serp_rounds)
        serp_observation["rounds"]["not_executed_reason"] = CRAWL_POLICY_SKIPPED_REASON
    elif serp_rounds:
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
                # План 19 §P2: спрашиваются только продукты с разрешённой сетью.
                products=serp_allowed_products,
            )
        except ValueError:
            # Безлимит/битый конфиг — не «частичный успех»: отказ обязан дойти до вызывающего.
            raise
        except Exception as exc:  # noqa: BLE001 — контур не роняет объявленную фазу
            errors.append(f"serp: проход не исполнен: {type(exc).__name__}: {exc}")
        # План 19 §P2: находки фильтруются **по корню**: поиск продукта мог отработать
        # (у него есть живой корень), но найденный URL принадлежит запрещённому корню —
        # ставить его seed'ом значило бы сделать запрос под запретом.
        dropped_serp = 0
        for pid in list(serp_seeds_by_product):
            by_root = serp_seeds_by_product[pid]
            for root_url in list(by_root):
                if _policy_execution(pid, root_url).denied:
                    dropped_serp += len(by_root.pop(root_url))
            if not by_root:
                serp_seeds_by_product.pop(pid)
        if dropped_serp:
            policy_notes.append(
                f"corpus: crawl_policy — находок SERP в запрещённых корнях "
                f"отброшено {dropped_serp} (сеть корня запрещена политикой)"
            )
        if serp_denied_products:
            policy_notes.append(
                "corpus: crawl_policy — внешний поиск не исполнялся для продуктов "
                f"с запрещённой сетью: {', '.join(sorted(serp_denied_products))}"
            )

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
    #: 17 §6 (F3): сколько документов продукта получили канонический адрес ``min(url)``
    #: вместо «первого дошедшего». Обязано быть видно оператору (строка лога), потому что
    #: это смена адреса в артефакте, а не косметика.
    canonical_url_changes: dict[str, int] = {}
    raw_root_path = Path(raw_root)
    #: P5: видимые пропуски документов уровнем ``off`` (никогда не тихо) — агрегат по всем
    #: корням/продуктам прогона; кладётся в ``IndexBuild.document_skips``.
    document_skips: list[dict[str, Any]] = []
    #: 09 §6.3: известные URL вне scope авторских корней — наблюдение по (продукт, origin).
    revalidation_orphans: dict[str, dict[str, Any]] = {}
    #: 09 §6.3: честные счётчики документов по продукту (``total``/``new``/``reused``).
    #: Заполняются только при наличии durable-субстрата (``state``), иначе карта пуста.
    doc_counters: dict[str, dict[str, int]] = {}

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
    # 17 §4 (F2): «краул без конверсии» без сеанса — противоречие в самом входе:
    # durable-очередь адресуется ``session_id``, и без него добытый документ некуда
    # положить. ValueError, а не тихий краул без публикации: последний выглядел бы как
    # успешный краул с пустой очередью.
    if crawl_only and session is None:
        raise ValueError(
            "crawl_only требует активного сеанса краула (store + run_id): "
            "без session_id durable-очередь extract_queue не адресуема"
        )
    # F6 (17 §6): конверсия чужой очереди без адреса сеанса — противоречие входа:
    # читать нечего, и «0 документов» выглядело бы как пустой сеанс, а не как ошибка.
    if consume_only and session is None:
        raise ValueError(
            "consume_only требует активного сеанса краула (store + run_id): "
            "durable-очередь extract_queue адресуется только session_id"
        )
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

    #: ``--only-failed``: строки учёта ремонта по продукту — «поставлено в work-set /
    #: отброшено и почему». Строки попадают в ``log_entries`` (→ ``results[].notes``):
    #: отброшенный URL не исчезает молча, а назван числом с причиной.
    repair_notes: list[str] = []
    #: ``--only-failed``: ``product_id → токен причины`` — продукты, которые этот прогон
    #: по построению не наблюдал целиком (ремонт). Покрытие такого продукта не ``complete``
    #: даже при всех ``FETCHED``: иначе ремонт одного URL объявлял бы сайт прочитанным.
    coverage_incomplete: dict[str, str] = {}

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
    # Ремонт провалов (``--only-failed``) — отдельный планировщик: он не зависит от
    # режима продукта и не выводится из ``url_state`` целиком (иначе ``new``/``full``
    # получил бы полный краул вместо ремонта).
    _plan_repair_items = partial(
        corpus_ingest.plan_repair_items,
        state=state,
        policy=policy,
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

    # --- F5 (17 §4.1): очередь конверсии ------------------------------------- #
    # Краул больше не конвертирует: документ становится строкой очереди, а зонд/спуск/
    # извлечение/нарезка — работа :func:`core.pipeline.extract_consume.consume` на границе
    # продукта (после дренажа всех его корней, до Pass B). При активном сеансе очередь
    # durable (``extract_queue``), без сеанса — в памяти процесса; носитель один и тот же.
    conversion_queue = ExtractQueue(store=store, session_id=session)
    #: Пул экстракции создаётся лениво на ключ ``(level, pdf_pipeline, window)`` и живёт до
    #: ``finally`` фазы (обёртка :func:`build_corpus`), а не на seed-root.
    #: Бакеты стадий — **по продукту** (F9 плана 17): когортный дренаж ведёт корни
    #: нескольких продуктов одновременно, и общий носитель приписал бы ожидание одного
    #: продукта строке другого. Резолвер (``_timings_for``) — единственная точка, где
    #: продукт превращается в его бакеты; корень без продукта получает ``_loose_timings``
    #: и в публикацию фазовых строк не попадает.
    product_timings: dict[str, _CorpusStageTimings] = {}
    _loose_timings = _CorpusStageTimings()

    def _timings_for(product_id: str | None) -> _CorpusStageTimings:
        """Бакеты стадий продукта; корень вне когорты — общий носитель без публикации.

        ``None``/неизвестный продукт не поднимают исключение намеренно: резолвер
        спрашивается драйвером **до** ``begin`` (бакеты нужны уже при открытии полосы),
        а бакет — наблюдаемость: отказ из-за неё уронил бы измерение. Плана без продукта
        фаза не строит — дефект плана назовёт ``_begin_root``.
        """
        if product_id is None:
            return _loose_timings
        return product_timings.get(str(product_id), _loose_timings)


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
    # F7 (17 §2): продукт, чьи объявленные host'ы покрыты шардом не полностью, этим
    # узлом **не** собран целиком. Это известный заранее (из объявленного входа) факт, а
    # не вывод из усечения: он входит в тот же канал честности, что ``max_documents``/
    # ``document_level`` — иначе ``coverage`` объявил бы полный охват на части корпуса.
    shard_reason_products: set[str] = set(shard_filtered_products)
    if shard_filtered_products:
        truncated_products |= shard_filtered_products
    #: F6 (17 §6): конверсия не краулит, поэтому собственного усечения у неё нет.
    #: Усечение — durable-факт **сеанса** (``provenance_json``: ``truncated``/
    #: ``truncated_products``/``effective_limits.roots``). Восстанавливаем его, а не
    #: выводим заново: иначе прогон без краула объявил бы усечённый корпус полным
    #: (``coverage.complete=True``), и ``❌`` на нём стал бы ложным (I2/I2b).
    if consume_only and store is not None and session is not None:
        try:
            prior_session = store.get_crawl_session(session)
            prior_truncation = (
                truncation_summary(read_provenance(prior_session))
                if prior_session is not None
                else {}
            )
            if prior_truncation.get("truncated"):
                truncated_products.update(
                    str(pid)
                    for pid in prior_truncation.get("truncated_products") or ()
                )
                roots = (prior_truncation.get("effective_limits") or {}).get(
                    "roots"
                ) or ()
                truncation_records.extend(
                    dict(record) for record in roots if isinstance(record, Mapping)
                )
            # F7 (17 §2): причина неполноты по шарду — durable-факт **сеанса**, а не
            # вывод этой фазы: конверсия не краулит и не знает, какие host'ы были на
            # другом узле. Восстанавливаем названный токен (`host_out_of_shard`), иначе
            # покрытие продукта осталось бы `complete=False` без причины и читалось бы
            # как «объясняется счётчиками» — то есть как обычное усечение.
            prior_shard = (
                read_provenance(prior_session).get("shard")
                if prior_session is not None
                else None
            )
            if isinstance(prior_shard, Mapping):
                shard_reason_products.update(
                    str(pid) for pid in prior_shard.get("filtered_products") or ()
                )
        except Exception as exc:  # noqa: BLE001 — наблюдаемость best-effort
            errors.append(
                "consume_only: провенанс усечения сеанса не прочитан: "
                f"{type(exc).__name__}: {exc}"
            )
    #: F6 (17 §6): отчёты потребителя по продуктам — машинный факт вызова ``consume``
    #: (``done/failed/skipped/claimed/reclaimed/heartbeats/pending/backlog``). Публикуются
    #: в ``log_entries``: без них ``hound extract consume`` печатал бы догадку, а не то,
    #: что действительно произошло с очередью.
    consumer_reports: list[Any] = []
    #: F5: продукты, у которых очередь конверсии осталась незакрытой. Это не «изолированная
    #: ошибка»: корпус продукта не полон, и фаза обязана вернуть ``failed`` (иначе
    #: ``evaluate`` пошёл бы по неполному предмету, а гейтовый ``subject_digest`` был бы
    #: снят с незрелого корпуса).
    corpus_not_converted: set[str] = set()
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
        # F7 (17 §2): шард узла — провенанс развёртывания. ``None`` у прогонов без
        # объявленной карты, поэтому их провенанс/статус не меняется.
        shard=shard_block,
    )


    #: B3: наблюдения спуска (прогноз + исход) и члены контейнеров — провенанс фазы.
    #: Списки нужны ``PhaseResult`` и финальному reconciliation-pass; durable-запись идёт
    #: **write-through** (синки волкера, F1), а не только пачкой в конце фазы.
    archive_observations: list[ArchiveObservation] = []
    corpus_member_rows: list[CorpusMember] = []
    #: B3: зонд артефактов (класс/формат/размер/``page_count``/листинг) — до экстракции.
    artifact_probes: list[dict[str, Any]] = []
    # L5 шаг 3: ``finish_one`` (доводка одного документа) вынесен в ``corpus_ingest``;
    # зависимости фазы связываются один раз здесь. ``persist_docs`` читается из глобала
    # модуля в момент вызова ``_build_corpus`` — тест ``test_corpus_phase_boundary``
    # подменяет ``corpus._persist_docs``, и подмена обязана дойти до писателя.
    _finish_one = partial(
        corpus_ingest.finish_one,
        mark_item=_mark,
        errors=errors,
        store=store,
        chunker=chunker,
        timings_for=_timings_for,
        state=state,
        duplicates=duplicates,
        doc_counters=doc_counters,
        partial_unknown_products=partial_unknown_products,
        partiality_changes=partiality_changes,
        reuse_same_hash=_reuse_same_hash,
        persist_docs=_persist_docs,
        chunker_version=resolved_chunker_version,
        run_id=run_id,
        embed_model_id=embed_model_id,
    )
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
    #: 09 §6.3: строки оффлайн-прохода (``--refresh offline``) — по одной на продукт.
    #: Оператор обязан видеть, что корпус собран **без наблюдения**: сколько документов
    #: переиспользовано и скольким URL переиспользовать было нечего.
    offline_notes: list[str] = []
    #: Причина zero-source покрытия по продукту (``None`` — причина не названа счётчиками).
    #: Заполняется на ``product_end`` и передаётся в :func:`_persist_corpus_coverage`.
    zero_source_reasons: dict[str, str | None] = {}
    #: 09 §6.3: ``product_id → причина`` для **всех** продуктов оффлайн-прогона. Отдельная
    #: карта, потому что ``zero_source_reasons`` управляет статусом (``blocked``), а здесь
    #: речь о честности покрытия: наблюдений не было, даже если документы переиспользованы.
    offline_reasons: dict[str, str] = {}
    #: План 19 §P2: ``product_id → crawl_policy_skipped`` для продуктов, у которых **нет
    #: элементов сеанса** (все корни запрещены политикой). Отдельная карта от
    #: ``incomplete_reasons``: та применяется, когда элементы есть, и иначе продукт без
    #: единого элемента назвался бы ``no_seeds`` — «seed'ов нет» вместо «сеть запрещена».
    policy_zero_reasons: dict[str, str] = {}
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
    #: 17 §6 (F4b): наблюдённые факты о полосах — по строке на продукт. Публикуются
    #: строкой, а не ``errors``: «ось объявлена, а host'ов меньше», «под executor
    #: чередование не действует» — это факты прогона, а не сбои. Строки появляются
    #: **только когда ось объявлена**: прогон, её не объявивший, не обрастает ими.
    lane_notes: list[str] = []

    # L5 шаг 3: дренаж одного корня вынесен в ``corpus_drain``, чередование корней
    # продукта — в ``corpus_driver`` (F4b). Зависимости приходят корню **носителями по
    # владению** (S2): фазовый скоуп и операции фазы создаются здесь один раз,
    # продуктовый — на границе продукта. Так точка вызова называет владельца состояния, а
    # не перечисляет 60 kwargs трёх разных природ.
    #: Состояние и оси фазы (общее для всех продуктов и корней).
    #: Жёсткий сторож свободного места — один на фазу (решение владельца 01.11):
    #: свободное место принадлежит носителю фазы, поэтому и остановка приёма фазовая.
    #: Строку ``crawl_disk_headroom`` публикуем **после** цикла продуктов: счётчик
    #: отказанных волн тогда итоговый, а запись одна.
    _disk_guard = DiskHeadroomGuard()
    _phase_scope = corpus_drain.PhaseDrainScope(
        errors=errors,
        store=store,
        run_id=run_id,
        chunker=chunker,
        container_for=container_for,
        items=items,
        max_attempts=max_attempts,
        wave_items=wave_items,
        # Читается из глобала модуля в точке создания носителя (до цикла продуктов):
        # тест ``test_split_round_budget_valve_truncates`` подменяет его атрибутом
        # ``corpus``, и подмена обязана попасть в носитель.
        max_waves=_EXECUTOR_MAX_WAVES_PER_ROOT,
        executor=executor,
        counters=counters,
        timings_for=_timings_for,
        scope_hosts_by_product=scope_hosts_by_product,
        sitemap_seed_used=sitemap_seed_used,
        document_skips=document_skips,
        queue=conversion_queue,
        session_id=session,
        # F6 (§4.3): корень retention — для публикуемого ``free_bytes`` рядом с
        # ``backlog_bytes``. Предел берётся из тех же ``CrawlLimits``, что у корня.
        raw_root=str(raw_root),
        # Жёсткий сторож свободного места: тот же фазовый носитель, что у очереди.
        disk_guard=_disk_guard,
        # F7 (17 §2): шард узла — host'ы, которые этот процесс имеет право вести, и его
        # имя для провенанса. ``None``/``None`` — карты не объявлено (поведение прежнее).
        shard_hosts=shard_hosts,
        node_id=node_id,
    )
    #: Замкнутые операции фазы (их сигнатуры держат этот модуль и тесты — носитель их
    #: не переписывает, только называет владельца).
    _phase_operations = corpus_drain.PhaseDrainOperations(
        emit=emit,
        mark=_mark,
        commit_local=_commit_local,
        local_prefetch=_local_prefetch,
        process_local=_process_local,
        submit_collect=_submit_collect,
        work_item=_work_item,
        resolve_target=_resolve_target,
        fresh_sitemap_seeds=_fresh_sitemap_seeds,
        limits_for=_limits_for_url,
    )
    #: F5: фазовый носитель потребителя очереди. Пул/кэш/экстрактор — те же объекты, что
    #: у фазы: второй экземпляр конвертера означал бы второй ``extractor_version``.
    _consume_scope = extract_consume.ConsumerScope(
        errors=errors,
        store=store,
        run_id=run_id,
        chunker=chunker,
        container_for=container_for,
        extractor=extractor,
        extractor_factory=extractor_factory,
        extraction_pool=extraction_pool,
        extract_cache=extract_cache,
        pools=pools,
        extract_timeout=_extract_timeout,
        worker_rss_dir=worker_rss_dir,
        counters=counters,
        # F9: бакеты продукта резолвятся по ``product_id`` на границе продукта
        # (:func:`dataclasses.replace` в предпроходе) — общий носитель фазы приписал бы
        # ожидание экстракции одного продукта строке другого.
        timings=_loose_timings,
        artifact_probes=artifact_probes,
        archive_observations=archive_observations,
        corpus_member_rows=corpus_member_rows,
        limits_for=_limits_for_url,
        # F6 (§4.3): потребитель публикует ``backlog_bytes``/``free_bytes`` числом, но
        # не ждёт: потребление — это и есть уменьшение бэклога.
        raw_root=str(raw_root),
        # F6 (§4.2): кто мы для аренды и как часто продлеваем её. ``None`` — имя
        # выводится из процесса/потока, период — опубликованная константа; явные
        # значения нужны внешнему потребителю и тестам (не более того).
        worker_id=consume_worker,
        heartbeat_s=(
            EXTRACT_HEARTBEAT_S if consume_heartbeat_s is None else consume_heartbeat_s
        ),
        # План 19 §E.7: состояние продукта — для write-through наблюдения контейнера
        # (``container_state``). Без него ``frozen`` не найдёт байтов архива в состоянии.
        state=state,
    )
    _consume_operations = extract_consume.ConsumerOperations(
        mark=_mark,
        reuse_for_page=_reuse_for_page,
        ingest_one=_ingest_one,
        finish_one=_finish_one,
        ingest_member_doc=_ingest_member_doc,
        persist_docs=_persist_docs,
        note_scheduling=_note_scheduling,
    )

    def _begin_root(
        plan: corpus_drain.RootPlan,
        *,
        fetch_pool: Executor | None = None,
        browser_pool: Executor | None = None,
    ) -> Generator[corpus_drain.WavePlan, corpus_drain.WaveOutcomes, Any]:
        """``begin=`` драйвера: генератор корня с носителями **его продукта** (F9).

        Продукт берётся из плана (``plan.product_id``), а не из ``partial``-а вокруг вызова:
        один поток планов ведёт корни всей когорты, поэтому «чей это корень» обязан быть в
        самом значении — иначе закрытие одного продукта подсунуло бы другому чужой скоуп.

        Пулы фетча создаёт **драйвер** на полосу (``lane_width``), поэтому фазовый
        носитель копируется на корень через :func:`dataclasses.replace`: контейнеры
        внутри копии остаются общими (копия — не новое состояние), меняются только две
        ссылки на пулы.
        """
        product_id = plan.product_id
        ctx = contexts.get(str(product_id)) if product_id is not None else None
        if ctx is None:
            # Корень без продукта в когортном потоке — дефект плана, а не режим: у него
            # нет ни продуктового скоупа, ни счётчика seed'ов, ни места в отчёте.
            raise AssertionError(
                f"{plan.root_url}: план без продукта в когортном дренаже"
            )
        return corpus_drain.root_lifecycle(
            plan,
            product=ctx.scope,
            phase=replace(
                _phase_scope, fetch_pool=fetch_pool, browser_pool=browser_pool
            ),
            operations=_phase_operations,
        )

    def _lane_width(root: corpus_drain.RootPlan) -> int | None:
        """Ширина пула фетча полосы для корня — та же формула, что у самого корня.

        Оси резолвятся у **корня** (``app < run < site``), поэтому драйвер не заводит
        второй источник: он спрашивает то же разрешение, что и ``root_lifecycle``.
        """
        return corpus_drain.lane_width(_limits_for_url(root.root_url), executor=executor)

    def _reserve_root_record(
        product_id: str, root_url: str, *, truncation_records: list[dict[str, Any]]
    ) -> int:
        """Занять слот записи провенанса корня в ``truncation_records`` (F4b).

        Заглушка остаётся в списке только при обрыве прогона до финализации корня — и
        тогда она **честно** называет корень, у которого исхода нет, вместо тихого
        пропуска в артефакте. На штатном пути слот перезаписывается записью целиком
        (:func:`core.pipeline.corpus_commit.record_root_outcome`).
        """
        truncation_records.append(
            {"product_id": product_id, "root": root_url, "planned": True}
        )
        return len(truncation_records) - 1

    def _root_done(outcome: corpus_driver.RootOutcome) -> None:
        """Исход завершённого корня: провенанс в **свой** слот ‖ сбор synthetic-бакета.

        Слот занят при планировании корня (``RootPlan.record_slot``), то есть в
        авторском порядке, поэтому ``truncation_records`` — артефакт манифеста
        (``effective_limits.roots``) — не зависит от порядка завершения корней. При
        чередовании порядок завершения другой, и без слота порядок объявления читался
        бы как порядок работы. Прецедент — ``sorted_error_lines`` (F3): артефакт
        приводится к авторскому порядку, а не к наблюдаемому.

        **Продукт берётся из плана.** Один поток планов ведёт корни всей когорты, поэтому
        продуктовые значения (группы ревалидации, потолок, слоты) приходят из контекста,
        названного планом, — ``partial`` вокруг вызова был бы читателем переменной цикла.

        **Synthetic-бакет отдаёт только frontier.** У него нет авторского seed'а, поэтому
        записи усечения он не пишет (``finalize_root`` не вызывается — как и раньше, когда
        его дренировали отдельным вызовом); но ``revalidation_fetched`` он вносит в
        фазовый счётчик и отдаёт frontier хвосту продукта — тот строит по нему наблюдение
        ``revalidation_orphans``. Без этого переноса число ревалидации потерялось бы вместе
        с полосой.
        """
        plan = outcome.plan
        product_id = plan.product_id
        ctx = contexts.get(str(product_id)) if product_id is not None else None
        if ctx is None:
            raise AssertionError(f"{plan.root_url}: исход корня без продукта")
        if not plan.seed:
            # Ключ наблюдения — ``(вид, адрес)``: адрес сам по себе не различает
            # catalog-leftover и orphan (см. ``ProductContext.synthetic``).
            kind = plan.synthetic_kind or "synthetic"
            ctx.synthetic[(kind, plan.root_url)] = outcome.frontier
            counters.revalidation_fetched_total += outcome.frontier.revalidation_fetched
            return
        corpus_commit.finalize_root(
            ctx.product_id,
            plan.root_url,
            outcome.frontier,
            outcome.sitemap_skips,
            plan.known_urls,
            all_revalidation_groups=ctx.all_revalidation_groups,
            cap_fired=ctx.revalidation_cap_fired,
            deferred_by_root=ctx.deferred_by_root,
            skipped_by_root=ctx.skipped_by_root,
            counters=counters,
            truncated_products=truncated_products,
            truncation_records=truncation_records,
            store=store,
            session=session,
            errors=errors,
            record_slot=plan.record_slot,
        )

    def _policy_skip(
        product_id: str,
        root_url: str,
        execution: PolicyExecution,
        *,
        known_urls: Sequence[str] = (),
        author_seed: bool = True,
    ) -> None:
        """Видимый пропуск корня политикой (план 19 §P2): провенанс, покрытие, субстрат.

        Зовётся **из гейта планов** — до резолва цели, развёртки каталога (feed/channel)
        и sitemap, — поэтому «ни одного запроса» здесь не обещание, а следствие места
        вызова. Пропуск делает продукт неполным (``crawl_policy_skipped`` в
        ``truncated_products`` и ``incomplete_reasons``) и **не** идёт через zero-source:
        продукт остаётся на карте (I4), а не превращается в ``BLOCKED``
        («несостоявшееся измерение» — это другой случай).

        ``chunks``-корень берёт корпус тем же обобщённым проходом, что ``offline``
        («продукт → корень»), а не вторым путём: ``url_state``/``document_version`` там
        не переписываются, поэтому следующий живой прогон не примет переиспользованное
        за наблюдённое.
        """
        reason = CRAWL_POLICY_SKIPPED_REASON
        reasons = [reason, *([execution.reason] if execution.reason else [])]
        if author_seed:
            # Synthetic-бакет не пишет запись усечения (у него нет авторского seed'а —
            # как и раньше: его дренаж не финализируется). Покрытие при этом не молчит.
            corpus_commit.record_policy_skip(
                product_id=product_id,
                root_url=root_url,
                scope=execution.scope,
                reasons=reasons,
                network_mode=execution.network_mode.value,
                corpus_source=execution.corpus_source.value,
                truncation_records=truncation_records,
            )
        truncated_products.add(product_id)
        coverage_incomplete.setdefault(product_id, reason)
        policy_zero_reasons.setdefault(product_id, reason)
        ctx = contexts.get(str(product_id))
        detail = ", ".join(reasons)
        scope_text = execution.scope or "policy"
        if ctx is not None:
            ctx.policy_skips.append(f"{root_url} — {detail} ({scope_text})")
        policy_notes.append(
            f"corpus: crawl_policy {product_id} {root_url} — сеть запрещена "
            f"({detail}; область {scope_text}); корпус: "
            f"{execution.corpus_source.value}"
        )
        if execution.corpus_source is not CrawlCorpusSource.CHUNKS or ctx is None:
            return
        report = _offline_reuse_product(
            product_id,
            [*([root_url] if author_seed else []), *known_urls],
            [],
            state=state,
            errors=errors,
            chunker_version=resolved_chunker_version,
            embed_model_id=embed_model_id,
            store=store,
            ingest=ctx.ingest,
            seen_docs=ctx.seen_docs,
            seen_chunk_ids=ctx.seen_chunk_ids,
            duplicates=duplicates,
            doc_counters=doc_counters,
            partial_unknown_products=partial_unknown_products,
            persist_docs=_persist_docs,
            label="crawl_policy",
        )
        policy_notes.append(report.note(product_id))

    def _synthetic_root_plans(
        ctx: ProductContext,
    ) -> Iterator[corpus_drain.RootPlan]:
        """Synthetic-бакеты ревалидации продукта — тем же потоком планов, что авторские.

        Зачем в поток, а не одиночным дренированием в хвосте. Хвост продукта исполняется
        **на главном потоке**, и одиночный дренаж внутри него открыл бы второй реестр
        права на host'ы: волна бакета могла бы уйти на host, который уже держит полоса
        другого продукта, — то есть нарушила бы «право на host» ровно там, где его
        вводили. В потоке планов claim общий, а итог бакета приходит в ``_root_done``
        раньше, чем хвост его прочитает.

        Состав и порядок прежние: leftover-группы каталогов (в порядке групп), затем
        orphan-бакеты (в порядке origin'ов). Пустая группа в поток не попадает — как и
        раньше, когда пустую группу не дренировали, а наблюдение писали с нулями.
        """
        for group_root, known_urls in ctx.revalidation_groups.items():
            if group_root in ctx.drained_roots:
                continue
            if not ctx.all_revalidation_groups.get(group_root):
                continue
            if not known_urls:
                continue
            # План 19 §P2: synthetic-бакет ревалидации — сеть. Политика проверяется до
            # постановки плана в поток, то есть до первого вызова.
            bucket_execution = _policy_execution(ctx.product_id, group_root)
            if bucket_execution.denied:
                _policy_skip(
                    ctx.product_id,
                    group_root,
                    bucket_execution,
                    known_urls=known_urls,
                    author_seed=False,
                )
                continue
            yield corpus_drain.RootPlan(
                root_url=group_root,
                target=None,
                sitemap_seeds=False,
                known_urls=known_urls,
                seed=False,
                product_id=ctx.product_id,
                synthetic_kind=_SYNTHETIC_CATALOG,
                network_mode=bucket_execution.network_mode,
                corpus_source=bucket_execution.corpus_source,
                policy_scope=bucket_execution.scope,
            )
        for origin, selected_urls in ctx.orphan_buckets.items():
            if not selected_urls:
                continue
            bucket_execution = _policy_execution(ctx.product_id, origin)
            if bucket_execution.denied:
                _policy_skip(
                    ctx.product_id,
                    origin,
                    bucket_execution,
                    known_urls=selected_urls,
                    author_seed=False,
                )
                continue
            yield corpus_drain.RootPlan(
                root_url=origin,
                target=None,
                sitemap_seeds=False,
                known_urls=selected_urls,
                seed=False,
                product_id=ctx.product_id,
                synthetic_kind=_SYNTHETIC_ORPHAN,
                network_mode=bucket_execution.network_mode,
                corpus_source=bucket_execution.corpus_source,
                policy_scope=bucket_execution.scope,
            )


    def _author_root_plans(
        product_id: str,
        *,
        author_urls: Sequence[str],
        items: Mapping[tuple[str, str], CrawlSessionItem],
        max_attempts: int | None,
        errors: list[str],
        emit: Callable[[], None],
        mark: Callable[..., None],
        crawl_target: Callable[..., Any],
        expand_directory: Callable[..., list[str] | None],
        targets: dict[tuple[str, str], Any],
        revalidation_groups: Mapping[str, Sequence[str]],
        serp_seeds_by_root: Mapping[str, Sequence[str]],
        drained_roots: set[str],
        truncation_records: list[dict[str, Any]],
    ) -> Iterator[corpus_drain.RootPlan]:
        """Корни продукта в авторском порядке — ленивый генератор драйвера полос (F4b).

        **Ленивый, а не список.** Тело исполняется по одному корню, в момент допуска
        драйвером: резолв цели, развёртка каталога и ``max_attempts`` — это побочные
        эффекты (``errors``/``mark``/``emit``), и при ``host_concurrency`` ``None``/``1``
        их порядок прежний: корень N+1 не планируется, пока корень N не дренирован.

        **Слот провенанса — здесь**, в авторском порядке планирования:
        ``truncation_records`` (артефакт ``effective_limits.roots``) поэтому не зависит от
        порядка завершения корней.

        **Почему параметры, а не замыкание.** Генератор объявлен один раз на фазу;
        продуктные значения приходят из итерации продукта, и захват переменных цикла
        сделал бы ленивый генератор читателем чужого продукта.
        """
        for url in author_urls:
            # --- План 19 §P2: ГЕЙТ ПОЛИТИКИ — выше первого сетевого вызова. -------- #
            # Здесь **до** резолва цели (``crawl_target``), развёртки каталога
            # (``expand_directory``: feed/channel — сеть) и до постановки sitemap-корня
            # (развёртка sitemap — тоже сеть, ниже в ``root_lifecycle``). Гейт во
            # фронтьере недостаточен: он исполняется **после** этих вызовов, то есть
            # запрет сети нарушался бы ещё до фронтьера.
            execution = _policy_execution(product_id, url)
            if execution.denied:
                _policy_skip(
                    product_id,
                    url,
                    execution,
                    known_urls=revalidation_groups.get(url, ()),
                )
                continue
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
                mark(
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
            target = crawl_target(product_id, url)
            targets[(product_id, url)] = target

            # Phase 5.2: источник-каталог (feed/channel) разворачивается в ссылки-записи;
            # каждая запись — **отдельный seed-root** глубины 0 (один уровень развёртки,
            # без ре-расширения). Обычный URL — единственный корень.
            entry_urls = expand_directory(product_id, url, target)
            if entry_urls is None:
                # D3: seed оператора; его per-site sitemap разворачивается в **подчинённые**
                # depth-0 seeds того же корня уже внутри ``root_lifecycle`` — там есть frontier,
                # чей scope фильтрует ``<loc>`` (дефект B/I17). Без sitemap — путь не
                # исполняется (поведение неизменно).
                roots: list[corpus_drain.RootPlan] = [
                    corpus_drain.RootPlan(
                        root_url=url,
                        target=target,
                        sitemap_seeds=True,
                        known_urls=revalidation_groups.get(url, ()),
                        serp_seeds=serp_seeds_by_root.get(url, ()),
                        product_id=product_id,
                        # План 19 §P2: решение политики едет **значением** корня: драйвер
                        # и дренаж видят его без повторного разрешения, а ``deny`` туда
                        # попасть не может — гейт выше.
                        network_mode=execution.network_mode,
                        corpus_source=execution.corpus_source,
                        policy_scope=execution.scope,
                    )
                ]
            else:
                roots = []
                for entry_url in entry_urls:
                    etarget = crawl_target(product_id, entry_url)
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
                    roots.append(
                        corpus_drain.RootPlan(
                            root_url=entry_url,
                            target=entry_target,
                            sitemap_seeds=False,
                            known_urls=revalidation_groups.get(entry_url, ()),
                            serp_seeds=serp_seeds_by_root.get(entry_url, ()),
                            product_id=product_id,
                            # Запись каталога наследует решение **источника**: политика
                            # назвала корень-каталог, а записи — его раскрытие, а не
                            # самостоятельные объявленные корни.
                            network_mode=execution.network_mode,
                            corpus_source=execution.corpus_source,
                            policy_scope=execution.scope,
                        )
                    )

            # D1: marks корней этого URL для leftover-групп каталогов и — слоты провенанса.
            # F4: корень описан значением (``RootPlan``), а не пятью аргументами вызова —
            # драйверу полос (F4b) нужен сам корень, а порядок остаётся авторским.
            for root in roots:
                drained_roots.add(root.root_url)
                yield replace(
                    root,
                    record_slot=_reserve_root_record(
                        product_id,
                        root.root_url,
                        truncation_records=truncation_records,
                    ),
                )


    # --- Когорта: предпроход без сети ---------------------------------------- #
    # F9 плана 17: планирование продуктов отделено от их закрытия. Предпроход не трогает
    # сеть (планирование инкрементальных/ремонтных элементов, владельцы URL, ось полос,
    # группы ревалидации, SERP-находки), поэтому его можно исполнить для **всех** продуктов
    # до первого корня: когортный драйвер обязан иметь носитель каждого продукта уже
    # наготове, иначе корень второго продукта некуда положить.
    contexts: dict[str, ProductContext] = {}
    for product_id in products:
        #: Бакеты стадий этого продукта: Pass A (краул+нарезка) и Pass B (эмбеддинг+сборка).
        #: Носитель — продуктовый (``ProductContext.timings``): когортный дренаж ведёт
        #: корни нескольких продуктов одновременно, поэтому общий объект приписал бы
        #: ожидание одного продукта строке другого.
        product_timings[product_id] = _CorpusStageTimings()
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

        #: I3/§5: наблюдённая судьба SERP-находок этого продукта (``enqueued``/``dup``/
        #: ``blacklisted``). Пишется в провенанс контура ниже: «поиск нашёл» и «находка
        #: вошла в корпус» — разные факты, и второй нельзя выводить из первого. Ключа
        #: ``truncated`` здесь нет: усечения на постановке не бывает (A2/F3 — бюджет
        #: решается на изъятии), а «поставлена и не изъята» видно числом
        #: ``queue_remaining`` в провенансе корня.
        serp_ingest: dict[str, Any] = {
            "enqueued": 0,
            "dup": 0,
            "blacklisted": [],
        }
        #: Находки, разложенные по корням этого продукта (:func:`assign_serp_seeds`).
        serp_seeds_by_root = dict(serp_seeds_by_product.get(str(product_id), {}))
        #: Сколько seed'ов фактически поставлено в очередь (авторские + sitemap + SERP).
        #: Растёт в :func:`root_lifecycle`; нужен честной причине zero-source. Носитель —
        #: продуктовый (``SeedTally``): общий фазовый счётчик приписал бы посадки одного
        #: продукта другому, когда продукты идут одновременно.
        seeds = corpus_drain.SeedTally()
        #: ``--only-failed``: разрешённое множество ремонта для этого продукта
        #: (``None``/пусто — обычный путь). Ремонт — **не** вопрос режима продукта, поэтому
        #: он выбирает планировщик ниже, а не гейт ``ProductMode`` в
        #: :func:`core.pipeline.corpus_ingest.plan_incremental_items`.
        repair_urls = list((only_failed_repairs or {}).get(product_id) or ())
        #: 09 §6.3: известные URL продукта (``revalidate``) — после авторских корней.
        if repair_urls:
            # Ремонт провалов: набор берётся **ровно** из разрешённого множества и
            # независимо от режима; ``plan_incremental`` сюда не зовётся — для ``new``/
            # ``full`` он вернул бы корни и весь ``url_state``, то есть полный краул.
            incremental_items, repair_dropped = _plan_repair_items(
                product_id, repair_urls, scopes=owner_scopes
            )
            dropped_by_reason: dict[str, int] = {}
            for _url, reason in repair_dropped:
                dropped_by_reason[reason] = dropped_by_reason.get(reason, 0) + 1
            repair_notes.append(
                f"corpus: {product_id}: ремонт --only-failed — в work-set "
                f"{len(incremental_items)} из {len(repair_urls)} URL "
                f"(depth-0 известные URL, авторские корни не обходятся); отброшено "
                f"{len(repair_dropped)}"
                + (
                    " ("
                    + ", ".join(
                        f"{reason}={count}"
                        for reason, count in sorted(dropped_by_reason.items())
                    )
                    + ")"
                    if dropped_by_reason
                    else ""
                )
            )
        else:
            incremental_items = _plan_incremental_items(product_id, product_roots)
        if repair_urls:
            # Честность покрытия (I2/I2c): ремонт наблюдал только проваленные URL —
            # «complete» на нём означало бы «сайт прочитан», а не «ремонт выполнен».
            coverage_incomplete[product_id] = ONLY_FAILED_REPAIR_REASON
        if product_id in shard_filtered_products:
            # F7 (17 §2): узел видит не все объявленные host'ы продукта. Причина сильнее
            # ремонта: она про **развёртывание**, а не про объём этого прогона, и верна
            # всегда, пока шард не покрывает продукт целиком.
            coverage_incomplete[product_id] = HOST_OUT_OF_SHARD_REASON
        elif product_id in shard_reason_products:
            # consume_only: причина восстановлена из провенанса сеанса (см. выше) — та
            # же честность без краула в этом исполнении.
            coverage_incomplete[product_id] = HOST_OUT_OF_SHARD_REASON
        #: 09 §6.3: ``offline`` — корпус из durable-субстрата **вместо** дренажа. Отдельный
        #: проход, а не дренаж с ревалидационными seed'ами: у оффлайн-элемента нет ни
        #: фетча, ни sitemap/feed-развёртки, ни BFS, поэтому проход не заходит в код,
        #: единственное назначение которого — сеть. Корни продукта разрешаются в
        #: состоянии (корень, которого там нет, — видимая ошибка, а не GET).
        #:
        #: **Исполняется не здесь, а на границе продукта** (``_open_product``): проход
        #: наполняет ``ingest.docs``, а продуктовый носитель обязан жить до своего
        #: закрытия. В предпроходе (он идёт для **всех** продуктов до первого корня) это
        #: удержало бы документы всей когорты сразу, и пик памяти рос бы с числом
        #: продуктов вместо числа полос.
        #: 09 §6.3: группировка **всех** известных URL по авторским корням (scope
        #: inference) до применения операторского потолка. ``state is None``/
        #: не-incremental/``offline`` ⇒ пусто: grouping — no-op (байт-в-байт). Под
        #: ``offline`` группировка не нужна: она адресует ревалидационные бакеты дренажа,
        #: а дренажа в этой политике нет — известные URL уже разрешены выше.
        if incremental_items and not no_network:
            all_revalidation_groups, all_orphan_buckets = (
                _group_revalidation_items(
                    product_id, product_roots, incremental_items
                )
            )
        else:
            all_revalidation_groups, all_orphan_buckets = {}, {}
        if consume_only:
            # F6 (17 §6): ревалидация известных URL — сетевой вызов. Конверсия чужой
            # очереди не наблюдает ничего: бакеты не строятся, иначе они дренировались
            # бы (в том числе orphan-бакеты, куда при пустом списке корней попало бы всё).
            all_revalidation_groups, all_orphan_buckets = {}, {}

        #: Ops-ось «не ограничивать, а измерять» (default-off): потолок ревалидации
        #: **на продукт**. Отсутствие оси — все известные URL, без записи ``stale`` и
        #: без усечения; задание — видимый defer хвоста. ``split`` — transport, который
        #: не исполняет локальную политику отбора: не притворяемся, что ось применена.
        revalidate_cap = _resolve_product_revalidate_cap(product_roots)
        # План 19 §P5: под политикой без сети ревалидации нет вовсе — потолок нечего
        # ограничивать, а его применение усекло бы work-set переиспользования и
        # записало бы defer хвоста, которого не было.
        cap_applies = bool(
            revalidate_cap is not None
            and incremental_items
            and len(incremental_items) > revalidate_cap
            and not no_network
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

        # L5 шаг 3 / S2: per-продуктное состояние связывается здесь, при создании
        # носителя. Раньше это были B023-дефолты замыкания: значения пересоздаются на
        # каждой итерации цикла, и привязка в точке определения была единственным
        # способом не прочитать чужой продукт. Носитель продуктового скоупа — **общий**
        # для всех корней продукта, поэтому корни получают ссылку на один объект, а не
        # копию (копия вернула бы тихий баг: корень обновляет копию, сосед не видит).
        _product_scope = corpus_drain.ProductDrainScope(
            pid=product_id,
            ingest=ingest,
            seen_docs=seen_docs,
            seen_chunk_ids=seen_chunk_ids,
            serp_ingest=serp_ingest,
            resume_counted=resume_counted,
            revalidation_url_set=revalidation_url_set,
            owner_root=_owner_root,
            seeds=seeds,
        )
        #: F5: тот же накопитель и те же дедуп-множества — но для потребителя очереди.
        #: Два объекта с разными списками означали бы «краул писал в один, конверсия в
        #: другой», то есть пустой корпус при непустой очереди.
        _consume_product = extract_consume.ProductScope(
            pid=product_id,
            ingest=ingest,
            seen_docs=seen_docs,
            seen_chunk_ids=seen_chunk_ids,
        )

        #: Корни, реально дренированные в этом продукте (для leftover-групп каталогов).
        #: Заполняется в авторском планировании, читается synthetic-планами и хвостом.
        drained_roots: set[str] = set()

        # 17 §6 (F4b): число полос — **объявленный** вход продукта. Ось описывает процесс
        # краула, а не сайт, поэтому берётся у первого авторского корня той же цепочкой
        # ``app < run < site``, что и остальные оси краула (второго источника нет).
        lane_axis: int | None = (
            _limits_for_url(product_roots[0]).host_concurrency
            if product_roots
            else None
        )
        if lane_axis is not None and int(lane_axis) > 1:
            lane_values = {
                _limits_for_url(url).host_concurrency for url in product_roots
            }
            declared_hosts = {
                normalize_scope_host(urlsplit(url).hostname or "")
                for url in product_roots
            }
            if executor is not None:
                # Не ошибка и не деградация: под ``executor`` волну собирает шов одним
                # блокирующим вызовом, промежуточной точки «план волны» там нет.
                lane_notes.append(
                    f"crawl: продукт {product_id}: host_concurrency={lane_axis} объявлен, "
                    "но корни идут через executor — чередование не действует "
                    "(сбор волны — один блокирующий вызов шва)"
                )
            elif len(declared_hosts) < int(lane_axis):
                # «не ограничивать, а измерять»: ось не усекает прогон и не выдумывает
                # полосы — факт публикуется числом.
                lane_notes.append(
                    f"crawl: продукт {product_id}: host_concurrency={lane_axis} объявлен, "
                    f"host'ов в продукте {len(declared_hosts)} — одновременно полос не "
                    f"больше {len(declared_hosts)}"
                )
            if len(lane_values) > 1:
                lane_notes.append(
                    f"crawl: продукт {product_id}: host_concurrency объявлен по-разному "
                    f"для корней продукта {sorted(v for v in lane_values if v is not None)}"
                    f" — полос {lane_axis} (значение первого корня)"
                )

        #: F9 плана 17: продуктовый контекст. Он живёт дольше итерации: планирование
        #: (здесь) и закрытие (хвост, когда завершён последний корень продукта) — две
        #: половины одного продукта, и состояние между ними принадлежит продукту.
        contexts[str(product_id)] = ProductContext(
            product_id=str(product_id),
            roots=product_roots,
            author_urls=(
                () if (repair_urls or no_network or consume_only) else url_map.get(product_id, [])
            ),
            ingest=ingest,
            seen_docs=seen_docs,
            seen_chunk_ids=seen_chunk_ids,
            resume_counted=resume_counted,
            owner_scopes=owner_scopes,
            owner_cache=owner_cache,
            serp_ingest=serp_ingest,
            serp_seeds_by_root=serp_seeds_by_root,
            drained_roots=drained_roots,
            seeds=seeds,
            timings=product_timings[str(product_id)],
            scope=_product_scope,
            consume=_consume_product,
            #: Тот же фазовый носитель потребителя, но с бакетами **этого** продукта:
            #: конверсия идёт в хвосте продукта и мерит его ожидание экстракции.
            consume_scope=replace(
                _consume_scope, timings=product_timings[str(product_id)]
            ),
            incremental_items=incremental_items,
            all_revalidation_groups=all_revalidation_groups,
            revalidation_groups=revalidation_groups,
            all_orphan_buckets=all_orphan_buckets,
            orphan_buckets=orphan_buckets,
            deferred_by_root=deferred_by_root,
            deferred_by_origin=deferred_by_origin,
            skipped_by_root=skipped_by_root,
            skipped_by_origin=skipped_by_origin,
            revalidation_deferred_total=revalidation_deferred_total,
            revalidation_cap_fired=revalidation_cap_fired,
            lane_axis=lane_axis,
        )

    # --- Когортный дренаж: один поток планов на всю когорту ------------------ #
    def _open_product(ctx: ProductContext) -> None:
        """Граница продукта (start): «продукт начат», а не «потерян» (16 §3.2).

        Вызывается из потока планов в момент, когда драйвер доходит до первого корня
        продукта, — то есть при ``host_concurrency`` ``None``/``1`` ровно там же, где
        раньше стояла граница итерации: продукт N+1 не начинается, пока не закрыт N.
        При объявленной оси продукты открываются по мере хода очереди, и это видимо:
        ``start``/``end`` перестают быть строго вложенными парами (см. контракт
        ``test_run_product_is_written_at_the_product_boundary``).

        Строка ``run_product`` в статусе ``running`` пишется **до** Pass A: «продукт
        начат» и «продукт потерян» должны различаться запросом, а не догадкой; финальный
        статус перезапишет её после Pass B.
        """
        ctx.started_at = time.perf_counter()
        product_events.append(
            {
                "event": "product_start",
                "product_id": ctx.product_id,
                "roots": len(ctx.roots),
                "seeds_declared": len(ctx.roots),
            }
        )
        #: Живая граница продукта: до первого дренажа (продукт может идти часами).
        emit_product("start", ctx.product_id, on_product=on_product)
        save_run_product(
            ctx.product_id,
            store=store,
            run_id=run_id,
            run_scope=run_scope,
            product_modes=product_modes,
            run_products_written=run_products_written,
            errors=errors,
            status=RunProductStatus.RUNNING,
        )
        # 09 §6.3/план 19 §P3: корпус без сети — работа **границы продукта**, а не
        # предпрохода. Проход наполняет ``ingest.docs`` и персистит документы: держать
        # его результат до закрытия продукта — это и есть продуктовый носитель, а не
        # общий склад. Порядок прежний: после ``start`` и строки ``running``, до работы
        # над корпусом. Источник выбирает **таблица** политики (``execution.corpus_source``),
        # а не ``is OFFLINE``: ``frozen_source=chunks`` — тот же проход переиспользования,
        # ``frozen_source=bytes`` — пере-извлечение сохранённых байтов.
        if no_network:
            pass_label = "frozen" if frozen else "offline"
            if corpus_source_no_network is CrawlCorpusSource.CHUNKS:
                ctx.offline_report = _offline_reuse_product(
                    ctx.product_id,
                    ctx.roots,
                    ctx.incremental_items,
                    state=state,
                    errors=errors,
                    chunker_version=resolved_chunker_version,
                    embed_model_id=embed_model_id,
                    store=store,
                    ingest=ctx.ingest,
                    seen_docs=ctx.seen_docs,
                    seen_chunk_ids=ctx.seen_chunk_ids,
                    duplicates=duplicates,
                    doc_counters=doc_counters,
                    partial_unknown_products=partial_unknown_products,
                    persist_docs=_persist_docs,
                    label=pass_label,
                )
                offline_notes.append(ctx.offline_report.note(ctx.product_id))
            else:

                def _frozen_fetch_plan(url: str) -> FetchPlan:
                    """План получения URL — тот же резолвер, что у живого дренажа.

                    Нужен только для ``FetchPlan.fetch``/``extract`` (ось
                    ``frozen_source=bytes`` не ходит в сеть): план описывает, **чем**
                    извлекать, а не куда идти.
                    """
                    return _crawl_target(ctx.product_id, url).plan

                def _frozen_container_replay(
                    url: str,
                    raw_path: Path,
                    *,
                    level: DocumentLevel,
                    pdf_pipeline: PdfPipeline,
                    extractor: ExtractorPort,
                    fetched_at: str,
                    limits: Any,
                ) -> Any:
                    """Спуск в контейнер из сохранённых байтов (план 19 §E.7).

                    Живой путь делает это в потребителе очереди (:func:`_descend_container`
                    → пул ‖ inline). Здесь ни очереди, ни пула, ни сеансовых меток: прогон
                    ничего не наблюдал. Поэтому — inline-спуск тем же
                    :class:`ArchiveDescentWalker` и тем же per-URL экстрактором, а
                    провенанс идёт в **продуктовый** скоуп потребителя (те же списки
                    фазы, тот же store, та же запись ``container_state``).
                    """
                    return extract_consume.descend_frozen_container(
                        product_id=ctx.product_id,
                        archive_url=url,
                        archive_path=Path(raw_path),
                        archive_content_hash=Path(raw_path).stem,
                        level=level,
                        pdf_pipeline=pdf_pipeline,
                        extractor=extractor,
                        limits=limits,
                        fetched_at=fetched_at,
                        persist_docs=_persist_docs,
                        scope=ctx.consume_scope,
                        product=ctx.consume,
                    )

                ctx.frozen_report = _frozen_bytes_product(
                    ctx.product_id,
                    ctx.roots,
                    ctx.incremental_items,
                    state=state,
                    errors=errors,
                    chunker_version=resolved_chunker_version,
                    embed_model_id=embed_model_id,
                    store=store,
                    ingest=ctx.ingest,
                    seen_docs=ctx.seen_docs,
                    seen_chunk_ids=ctx.seen_chunk_ids,
                    duplicates=duplicates,
                    doc_counters=doc_counters,
                    partial_unknown_products=partial_unknown_products,
                    persist_docs=_persist_docs,
                    extractor=extractor,
                    chunker=chunker,
                    extract_cache=extract_cache,
                    fetch_plan_for=_frozen_fetch_plan,
                    limits_for=_limits_for_url,
                    # Тот же per-level резолвер, что у живого пути и ручных артефактов:
                    # `frozen` обязан извлекать объявленным уровнем, иначе он штампует
                    # условие, которого измерение не объявляло (см. corpus_ingest).
                    extractor_factory=extractor_factory,
                    # План 19 §E.7: члены контейнеров — из сохранённых байтов, без сети.
                    container_replay=_frozen_container_replay,
                    label=pass_label,
                )
                offline_notes.append(ctx.frozen_report.note(ctx.product_id))

    def _cohort_root_plans() -> Iterator[
        corpus_drain.RootPlan | corpus_driver.ProductBoundary
    ]:
        """Корни всей когорты: продукты в авторском порядке, граница — маркер (F9).

        **Ленивый.** Тело исполняется по одному продукту и по одному корню, в момент
        допуска драйвером: резолв цели, развёртка каталога и ``max_attempts`` — побочные
        эффекты (``errors``/``mark``/``emit``), и при ``host_concurrency`` ``None``/``1``
        их порядок прежний — корень N+1 не планируется, пока корень N не дренирован.

        **Граница продукта — маркер, а не «смена pid».** Продукт без корней и последний
        продукт потока иначе не закрылись бы вовсе: смена ``product_id`` у них не
        наступает. Маркер называет факт прямо, и хвост продукта исполняется драйвером
        на главном потоке ровно тогда, когда он обязан был бы исполниться.

        **``consume_only`` и ``offline``** дают продукт без корней (``author_urls`` пуст),
        но с маркером: хвост (конверсия чужой очереди ‖ переиспользование субстрата,
        Pass B, покрытие) обязан исполниться, как и прежде.
        """
        for product_id in products:
            ctx = contexts[str(product_id)]
            _open_product(ctx)
            yield from _author_root_plans(
                ctx.product_id,
                author_urls=ctx.author_urls,
                items=items,
                max_attempts=max_attempts,
                errors=errors,
                emit=emit,
                mark=_mark,
                crawl_target=_crawl_target,
                expand_directory=_expand_directory,
                targets=targets,
                revalidation_groups=ctx.revalidation_groups,
                serp_seeds_by_root=ctx.serp_seeds_by_root,
                drained_roots=ctx.drained_roots,
                truncation_records=truncation_records,
            )
            yield from _synthetic_root_plans(ctx)
            yield corpus_driver.ProductBoundary(ctx.product_id)

    def _crawl_finished() -> bool:
        """Краул когорты закончен: **все** объявленные продукты закрыты.

        Не «закрыт последний по авторскому порядку»: при объявленной оси
        ``crawl.host_concurrency`` продукты закрываются в порядке готовности, и
        последний по порядку может закрыться первым. «Краул закончен» — утверждение о
        полном наборе продуктов, поэтому и спрашивается о нём.
        """
        return all(ctx.closed for ctx in contexts.values())

    def _close_product(product_id: str) -> None:
        """Хвост продукта на главном потоке: конверсия → Pass B → покрытие → строка.

        Что здесь и почему именно здесь. Хвост исполняет драйвер, когда завершён
        последний корень продукта (``corpus_driver._close_product``): до этого момента
        ни один шаг хвоста не наблюдал бы полного набора документов продукта. Порядок
        шагов прежний (F5): конверсия очереди → подача объявленных артефактов → потолок
        ревалидации → ``product_end`` → Pass B → ``corpus_coverage`` → финальная строка
        ``run_product``.

        **Цена компромисса.** Пока исполняется хвост, драйвер не выдаёт новых волн:
        отправленные доживают в пулах. Так пик памяти ограничен числом полос, а не
        числом продуктов в полёте; цена видна в ``product_tail_s`` (её пишет драйвер).

        **Имена ниже — поля контекста продукта**, привязанные один раз в начале тела:
        хвост — это хвост **одного** продукта, и повторять ``ctx.`` в 500 строках значило
        бы прятать это за шумом. Владелец имени назван здесь, а не у каждого чтения.
        """
        ctx = contexts[str(product_id)]
        #: Продукт закрывается ровно один раз (гарантия драйвера: по счётчику корней).
        #: Отметка нужна вопросу «краул когорты закончен?» — см. ``_crawl_finished``.
        ctx.closed = True
        #: Граница продукта (start); ``None`` — продукт открыт не был (обрыв до него).
        product_started = (
            ctx.started_at if ctx.started_at is not None else time.perf_counter()
        )
        ingest = ctx.ingest
        seen_docs = ctx.seen_docs
        seen_chunk_ids = ctx.seen_chunk_ids
        product_roots = ctx.roots
        drained_roots = ctx.drained_roots
        all_revalidation_groups = ctx.all_revalidation_groups
        revalidation_groups = ctx.revalidation_groups
        orphan_buckets = ctx.orphan_buckets
        all_orphan_buckets = ctx.all_orphan_buckets
        deferred_by_root = ctx.deferred_by_root
        deferred_by_origin = ctx.deferred_by_origin
        skipped_by_root = ctx.skipped_by_root
        skipped_by_origin = ctx.skipped_by_origin
        revalidation_cap_fired = ctx.revalidation_cap_fired
        revalidation_deferred_total = ctx.revalidation_deferred_total
        offline_report = ctx.offline_report
        serp_ingest = ctx.serp_ingest
        serp_seeds_by_root = ctx.serp_seeds_by_root
        _consume_product = ctx.consume
        #: Бакеты стадий **этого** продукта (ожидание фетча/экстракции, нарезка, Pass B).
        timings = ctx.timings

        # Каталог (feed/channel) разворачивается в **другие** корни, поэтому группа,
        # привязанная к авторскому URL каталога, может не совпасть ни с одним drain'ом.
        # Не теряем ревалидацию: синтетический бакет без seed (без записи усечения).
        # Бакет уже дренирован полосой (``_synthetic_root_plans``): здесь только его
        # наблюдение. Пустой бакет в поток не попадал, поэтому ``frontier is None`` — это
        # «дренажа не было», а не «дренаж потерялся».
        for group_root, known_urls in revalidation_groups.items():
            pre_cap_urls = all_revalidation_groups.get(group_root) or ()
            if group_root in drained_roots or not pre_cap_urls:
                continue
            frontier: Frontier | None = ctx.synthetic.get(
                (_SYNTHETIC_CATALOG, group_root)
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

        # 09 §6.3: известные URL вне scope **всех** авторских корней (author-scope
        # drift: корень удалён/ключ пути сужен). Один synthetic-бакет на (продукт,
        # origin): depth-0 листья, только ревалидация. В ``truncation`` **не** пишется
        # (иначе synthetic-причина демотировала бы ``❌`` без вины корня); наблюдение —
        # ``IndexBuild.revalidation_orphans`` + заметка фазы. I12: статус не меняется.
        for origin, all_known_urls in all_orphan_buckets.items():
            selected_urls = orphan_buckets.get(origin, ())
            # Бакет уже дренирован полосой (``_synthetic_root_plans``): наблюдение — здесь.
            frontier: Frontier | None = ctx.synthetic.get((_SYNTHETIC_ORPHAN, origin))
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
            counters.revalidation_orphan_total += len(all_known_urls)
            # Targets известных URL резолвятся лениво в корневом цикле
            # (``_resolve_target``) — предварительная резолюция не нужна.

        # F5 (17 §4.1): конверсия продукта — работа потребителя очереди, и она идёт
        # **после** дренажа всех корней продукта (краул и конверсия разведены по
        # владельцу) и **до** Pass B (документы входят в ту же сборку, I19). Продукт
        # закрывается целиком: незакрытая строка очереди означает недостроенный корпус, и
        # фаза не вправе объявить ``ok`` (иначе ``evaluate`` пошёл бы по неполному
        # предмету).
        conversion = (
            None
            if crawl_only
            else extract_consume.consume(
                session,
                [product_id],
                queue=conversion_queue,
                # Бакеты стадий — **этого** продукта: конверсия идёт в его хвосте, и её
                # ожидание экстракции принадлежит его строке, а не соседнему продукту.
                scope=ctx.consume_scope,
                operations=_consume_operations,
                products={product_id: _consume_product},
            )
        )
        if conversion is not None:
            consumer_reports.append(conversion)
        if conversion is not None and conversion.pending:
            errors.append(
                f"{product_id}: конверсия очереди не завершена — строк без терминального "
                f"состояния {conversion.pending} (done={conversion.done}, "
                f"failed={conversion.failed}); корпус продукта не полон"
            )
            corpus_not_converted.add(product_id)
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
        if no_network:
            # 09 §6.3/план 19 §P3: прогон **не наблюдал ничего** — ни одного сетевого
            # вызова. Это не ``no_seeds`` (seed'ы есть) и не «продукт пуст» (документы
            # могут быть переиспользованы/пере-извлечены из durable-субстрата). Причина
            # называется в строке покрытия всегда: ``observed_sources`` — счётчик
            # наблюдений **этого** прогона, и он нулевой, поэтому ``complete`` не может
            # стать ``True``. ``blocked`` ставится только когда материала не осталось:
            # «материал есть, охват неполон» — это ``partial``, а не ``blocked``.
            no_network_label = "frozen" if frozen else "offline"
            no_network_reason = (
                COVERAGE_FROZEN_NO_OBSERVATION
                if frozen
                else COVERAGE_OFFLINE_NO_OBSERVATION
            )
            offline_reasons[product_id] = no_network_reason
            no_version_count = 0
            unknown_roots_count = 0
            missing_bytes_count = 0
            if ctx.frozen_report is not None:
                no_version_count = len(ctx.frozen_report.no_version)
                unknown_roots_count = len(ctx.frozen_report.unknown_roots)
                missing_bytes_count = len(ctx.frozen_report.stored_bytes_missing)
            elif offline_report is not None:
                no_version_count = len(offline_report.no_version)
                unknown_roots_count = len(offline_report.unknown_roots)
            if not ingest.docs and not manual_resolved:
                product_reason = no_network_reason
                zero_source_pending.append(
                    f"{product_id}: {no_network_label} — материала нет и сеть не "
                    "трогалась; "
                    "корпус пуст, оценка отметит все ячейки failed, а не ❌. "
                    "Нужен прогон с --refresh auto/revalidate"
                )
                errors.append(
                    f"{product_id}: {no_network_label} corpus без единого документа "
                    f"({no_network_reason}): roots={len(product_roots)}, "
                    f"docs=0, no_version={no_version_count}, "
                    f"unknown_roots={unknown_roots_count}, "
                    f"stored_bytes_missing={missing_bytes_count} — "
                    "ячейки будут failed, не ❌"
                )
        # Zero-source — утверждение о **сеансе**: без сеанса «нет элементов» не значит
        # «нет источников» (счётчики просто не ведутся), поэтому причина не выдумывается.
        # ``and not manual_resolved``: продукт, чей материал предъявлен объявленным
        # артефактом, без источников **не** остался — иначе ошибка утверждала бы
        # пустой корпус при непустом.
        elif (
            session is not None
            and not product_items
            and not manual_resolved
            # План 19 §P2: пустой сеанс из-за политики — не zero-source. Здесь
            # ``BLOCKED`` означал бы «измерение не состоялось», а политический пропуск —
            # решение с известной причиной: продукт остаётся на карте (I4), покрытие
            # неполно и названо токеном ``crawl_policy_skipped``.
            and not ctx.policy_skips
        ):
            # Истинная причина вместо единой константы ``no_seeds``: пустой seed-набор,
            # полностью отклонённые seed'ы и отсутствие записи в ``urls`` — разные пути.
            if product_id not in url_map:
                product_reason = ZERO_SOURCE_NO_URLS_ENTRY
            elif not product_roots:
                product_reason = ZERO_SOURCE_NO_SEEDS
            elif ctx.seeds.enqueued == 0:
                product_reason = (
                    # F7: ни один seed не поставлен **из-за шарда** — причина называется
                    # своим токеном, а не общим ``all_seeds_rejected`` (материал есть, он
                    # на другом узле).
                    HOST_OUT_OF_SHARD_REASON
                    if product_id in shard_filtered_products
                    else ZERO_SOURCE_ALL_SEEDS_REJECTED
                )
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
                f"seeds_enqueued={ctx.seeds.enqueued} — ячейки будут failed, не ❌"
            )
        # 17 §6 (F3): канонический адрес документа — **запрос**, а не гонка. После персиста
        # документов продукта адрес согласуется по durable-носителю
        # (``crawl_session_items.doc_hash``): победитель дедупа — ``min(url)``, поэтому
        # состав корпуса не зависит от того, какая полоса доехала первой. Проход
        # идемпотентен; число переписанных адресов идёт в лог фазы (видимая смена
        # артефакта), а не в ``errors``: это не сбой, а исправление порядка.
        canonical_changed = canonicalize_doc_urls(
            product_id,
            items=items,
            store=store,
            run_id=run_id,
            errors=errors,
            docs=ingest.docs,
        )
        if canonical_changed:
            canonical_url_changes[product_id] = canonical_changed
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
                "seeds_enqueued": ctx.seeds.enqueued,
                "session_items": len(product_items),
                "fetched": product_fetched,
                "failed": product_failed,
                "docs": product_docs_total,
                # 17 §6 (F3): сколько адресов документов согласовано запросом
                # (``min(url)`` среди равных ``doc_hash``). Машинный канал к тому же
                # числу, что в строке лога: «артефакт изменился» видно запросом.
                "canonical_url_changes": canonical_changed,
                "partial_docs": len(product_partial),
                # v5: «признак частичности неизвестен» — отдельный факт, а не «полное».
                # Строка версии могла быть записана до v5 (признак не сохранялся), и
                # переиспользование такой версии не вправе молчать об этом.
                "partial_unknown": len(partial_unknown_products.get(product_id, [])),
                # План 19 §P2: корни, пропущенные политикой (сеть запрещена). Поле есть
                # всегда: пустой список — «политика никого не запретила», а не «не считали».
                "policy_skips": list(ctx.policy_skips),
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
                # 09 §6.3: оффлайн-проход — отдельные поля, а не пересказ ``fetched``/
                # ``docs``: «взято из состояния» и «наблюдено в этом прогоне» — разные
                # факты, и первое не становится вторым от того, что документ попал в корпус.
                "offline": (
                    None
                    if offline_report is None
                    else {
                        "label": offline_report.label,
                        "reused_docs": offline_report.reused,
                        "reused_chunks": offline_report.chunks,
                        "no_version": list(offline_report.no_version),
                        "unknown_roots": list(offline_report.unknown_roots),
                    }
                ),
                # План 19 §P3: пере-извлечение сохранённых байтов — отдельный факт, а не
                # пересказ ``offline``: «из состояния» и «из тех же байтов текущим стеком»
                # дают разный текст корпуса и разные причины пропуска.
                "frozen": (
                    None
                    if ctx.frozen_report is None
                    else {
                        "label": ctx.frozen_report.label,
                        "docs": ctx.frozen_report.docs,
                        "chunks": ctx.frozen_report.chunks,
                        "reextracted": ctx.frozen_report.reextracted,
                        "cache_hits": ctx.frozen_report.cache_hits,
                        "no_version": list(ctx.frozen_report.no_version),
                        "unknown_roots": list(ctx.frozen_report.unknown_roots),
                        "stored_bytes_missing": list(
                            ctx.frozen_report.stored_bytes_missing
                        ),
                        "reextract_mismatch": list(
                            ctx.frozen_report.reextract_mismatch
                        ),
                        # План 19 §E.7: контейнеры и их вклад в корпус — числа, а не
                        # пересказ: члены входят в ``docs``/``chunks`` этого же события.
                        "containers": ctx.frozen_report.containers,
                        "container_docs": ctx.frozen_report.container_docs,
                        "container_chunks": ctx.frozen_report.container_chunks,
                        "container_unreadable": list(
                            ctx.frozen_report.container_unreadable
                        ),
                        "container_empty": list(ctx.frozen_report.container_empty),
                    }
                ),
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

        # Наблюдаемость: здесь закрывается **краул**, а не «последний продукт». Сеанс
        # закрывается до Pass B: эмбеддинг/сборка индекса — работа фазы, и терминальный
        # статус краула не должен от неё зависеть (иначе ``stale_reason`` врёт).
        # Условие — «закрыты **все** продукты когорты», а не «это последний по порядку»:
        # при объявленной оси ``crawl.host_concurrency`` последний по порядку продукт может
        # закрыться, пока корни соседа ещё в полёте, и терминальный статус сеанса описал бы
        # незавершённый краул. При ``None``/``1`` эти условия совпадают, поэтому порядок
        # записи прежний байт-в-байт. Если краул ещё не закончен, сеанс закроет вызов сразу
        # после прохода когорты (:func:`corpus_commit.finalize_crawl_session` идемпотентен).
        if not consume_only and _crawl_finished():
            # F6: сеанс закрывает краул, а не конверсия чужой очереди. Повторная
            # финализация перезаписала бы ``pages_fetched`` нулём счётчиков этой фазы —
            # то есть сделала бы провенанс краула ложным.
            _finalize_crawl_session()

        # --- Pass B (index) **этого** продукта (09 §8.1) ---------------------- #
        # Партиция планируется, эмбеддится, физически собирается и персистится сразу
        # после Pass A продукта, а не после всех продуктов. Иначе смерть на N-м
        # продукте уничтожала готовую работу предыдущих (чанки посчитаны — в БД их
        # нет). Все входы планирования — ``ingest.chunks``/``ingest.docs`` **только
        # этого** продукта (``pb`` продукт-скоупед, I17), поэтому перенос в цикл не
        # меняет ни один ``pb``; ``composition_id`` по-прежнему выводится после всех
        # продуктов (он зависит от состава когорты).
        # F5: вход Pass B приводится к каноническому порядку ``(product_id, url,
        # chunk_no)``. Очередь отдаёт строки в этом порядке, но **расписание** доводки
        # оконное: чанки попадают в ``ingest.chunks`` по мере того, как их документы
        # доходят. Порядок обязан быть функцией **содержимого**, иначе два прогона с
        # одинаковым составом дали бы разный порядок сборки, и сравнение артефактов
        # показывало бы расписание (та же причина, что у ``canonicalize_doc_urls`` F3).
        product_chunks = sorted(
            ingest.chunks,
            key=lambda chunk: (chunk.product_id, chunk.url, chunk.chunk_no),
        )
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
            counters.chunk_count += len(product_chunks)
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
                    counters.partitions_reused += 1
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
                    counters.all_physical_ok = False
            else:
                counters.all_physical_ok = False
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
        # План 19 §P3: отпечаток doc-set **этого** прогона считается до освобождения
        # продукта из памяти. ``document_version`` в state пишется только живым путём:
        # ``frozen`` его не пишет, поэтому state-субстрат описал бы не этот корпус.
        frozen_doc_set_digest = (
            corpus_doc_set_digest(ingest.docs) if frozen else None
        )
        counters.doc_count += len(ingest.docs)
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
        # Элементы сеанса **этого** продукта финальны (его корни завершены — драйвер
        # зовёт хвост только после последнего корня), поэтому строка честна уже здесь:
        # продукт, дошедший до ``product_end``, сохраняет покрытие, даже если следующий
        # продукт убьёт прогон. Конец фазы
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
                    reasons={
                        **zero_source_reasons,
                        **offline_reasons,
                        # План 19 §P2: у продукта не было ни одного элемента, потому
                        # что политика запретила сеть, — причина называется ею.
                        **policy_zero_reasons,
                    },
                    resolved_manually=manual_urls_by_product,
                    partial_docs=partial_urls_by_product,
                    # ``--only-failed``: ремонт не наблюдал продукт целиком.
                    incomplete_reasons=coverage_incomplete,
                )
                # 16 §3.2: ``coverage_complete`` строки продукта — та же величина, что
                # легла в ``corpus_coverage``, а не второй её расчёт.
                product_coverage = written_coverage.get(product_id)
            except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
                errors.append(
                    f"corpus_coverage({product_id}): {type(exc).__name__}: {exc}"
                )

        # План 19 §P3: запись снятия корпуса — после покрытия (оно часть записи) и
        # **только** для ``frozen``: снятие описывает «что зафиксировано без сети».
        # ``url_state.last_checked_at`` этой роли не годится — он пишется при любом
        # исходе, включая отказ. Группы дедупа берутся из прохода, который их
        # использовал; ``crawl_session_items`` ``frozen`` не пишет, поэтому это
        # единственный durable-носитель канонического адреса.
        if frozen and state is not None and run_id and frozen_doc_set_digest is not None:
            capture_report = ctx.frozen_report or offline_report
            try:
                state.record_corpus_capture(
                    CorpusCaptureRow(
                        product_id=product_id,
                        run_id=run_id,
                        captured_at=_now_iso(),
                        capture_digest=corpus_capture_digest(
                            roots=product_roots,
                            urls=(
                                () if capture_report is None else capture_report.urls
                            ),
                            seeds_digest=seeds_digest or "",
                        ),
                        seeds_digest=seeds_digest or "",
                        source=corpus_source_no_network.value,
                        coverage_complete=(
                            False
                            if product_coverage is None
                            else bool(product_coverage.complete)
                        ),
                        coverage_digest=coverage_digest(product_coverage),
                        doc_set_digest=frozen_doc_set_digest,
                        docs=(
                            ()
                            if capture_report is None
                            else capture_report.capture_docs
                        ),
                    )
                )
            except Exception as exc:  # noqa: BLE001 — снятие вторично к корпусу
                errors.append(
                    f"{product_id}: frozen: corpus_capture: "
                    f"{type(exc).__name__}: {exc}"
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
            }

    # --- Один вызов драйвера на всю когорту (F9 плана 17) -------------------- #
    # Продукт — граница **коммита**, а не граница взаимного исключения: поток планов
    # содержит корни всех продуктов, право на host'ы — один реестр на когорту, а хвост
    # продукта исполняется драйвером, когда завершён его последний корень. До F9 вызов
    # стоял внутри цикла продуктов, и волна не могла содержать корни двух продуктов —
    # окна ``fetch_started_at`` продуктов не пересекались ни при какой оси
    # ``crawl.host_concurrency`` (измерено: продукты когорты шли строго последовательно).
    #
    # Потолок полос когорты — максимум объявленных продуктовых значений (``None`` — ось не
    # объявлена, одна полоса). Потолок **продукта** приходит из ``lane_limit_for``: ось
    # объявлена на корень (``app < run < site``), и продукт со строгим объявлением не
    # получает полос «за компанию» с соседом.
    _cohort_lane_axis = max(
        (int(ctx.lane_axis or 1) for ctx in contexts.values()), default=1
    )
    # Владелец браузерного движка — **один поток на фазу**. Playwright sync-API привязан к
    # потоку, создавшему движок: контекст, открытый в потоке одной полосы, ломается в
    # следующей (``cannot switch to a different thread``), а движок при этом общий на
    # когорту (``ports.crawler``). Поэтому пул создаётся здесь, отдаётся драйверу как
    # общий и закрывается здесь же: движок закрывается **на своём потоке**, затем поток
    # останавливается. Иначе браузерный процесс висел бы до конца фазы (на хосте 6.9 GiB
    # это удерживаемые ~300 MB). Фаза без браузерного движка пул не заводит вовсе: потоку
    # нечего было бы владеть, а браузерный фетч без движка невозможен.
    _browsers = _browser_engine_crawlers([crawler, *(crawlers or {}).values()])
    browser_pool = (
        ThreadPoolExecutor(max_workers=1, thread_name_prefix="cohound-browser")
        if _browsers
        else None
    )
    try:
        corpus_driver.drive_roots(
            _cohort_root_plans(),
            host_concurrency=None if _cohort_lane_axis <= 1 else _cohort_lane_axis,
            begin=_begin_root,
            lane_width=_lane_width,
            local_fetch=_local_fetch,
            timings_for=_timings_for,
            errors=errors,
            pool_factory=pool_factory,
            browser_pool=browser_pool,
            notes=lane_notes,
            on_root_done=_root_done,
            on_product_done=_close_product,
            lane_limit_for=lambda plan: contexts[str(plan.product_id)].lane_axis or 1,
            # Teardown: durable-факт «документ добыт» не отбрасывается — прерывание краула
            # называет, сколько строк очереди конверсии осталось ``pending``.
            abort=partial(
                _note_interrupted_queue, queue=conversion_queue, errors=errors
            ),
            label=" когорты",
        )
    finally:
        if browser_pool is not None:
            _shutdown_browser_owner(
                browser_pool, _browsers, errors=errors, label=" когорты"
            )

    # Конец прохода когорты: если краул закончился не в хвосте продукта (при объявленной
    # оси последний по авторскому порядку продукт закрывается раньше соседей), сеанс
    # закрывается здесь — до оставшейся работы фазы (состав когорты, композиция). Этот же
    # вызов закрывает сеанс у **пустой когорты** (``products == []``): дренажа нет, а
    # сеанс, открытый фазой, обязан получить терминальный статус. Идемпотентно: в обычном
    # случае ``session_finalized`` уже стоит из хвоста.
    if not consume_only:
        _finalize_crawl_session()

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

    # Наблюдаемость: сеанс краула закрыт выше — в хвосте последнего закрытого продукта
    # либо (у пустой когорты) концом прохода когорты. Пост-краульная работа (манифест,
    # композиция) ниже на терминальный статус уже не влияет.
    # F6: конверсия чужой очереди сеанс не закрывает (см. границу продукта).

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
                reasons={
                        **zero_source_reasons,
                        **offline_reasons,
                        # План 19 §P2: у продукта не было ни одного элемента, потому
                        # что политика запретила сеть, — причина называется ею.
                        **policy_zero_reasons,
                    },
                resolved_manually=manual_urls_by_product,
                # I2/I2c: та же карта, что на границе продукта — конец фазы
                # перезаписывает строки покрытия и обязан сохранить причину.
                partial_docs=partial_urls_by_product,
                # Тот же набор продуктов-ремонтов: перезапись не должна вернуть им
                # ``complete``, которого они не заслужили.
                incomplete_reasons=coverage_incomplete,
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

    # Жёсткий сторож свободного места (решение владельца 01.11): **одна** строка на прогон
    # с числами. Публикуется здесь, а не в голове волны, по двум причинам: `waves_blocked`
    # обязан быть итоговым (он растёт с каждой отказанной волной), а вторая такая же строка
    # на каждый корень была бы шумом вместо наблюдения. Строки нет, когда приём не
    # останавливался и место измерялось штатно: «места хватает» — не событие.
    _disk_notice = _disk_guard.notice()
    if _disk_notice is not None:
        errors.append(corpus_drain.disk_headroom_line(_disk_notice))

    # Строки манифеста — после того, как композиция известна (PK включает
    # ``index_build_id``) и партиции уже на диске; чанки персистентны ещё с границы
    # продукта. Сбой записи снимает композицию (она не должна ссылаться на партицию,
    # которой нет в реестре прогона).
    if not _persist_manifest(store, manifest_entries, errors):
        counters.all_physical_ok = False

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
        if index_root is not None and counters.all_physical_ok and commit_ok:
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
        doc_count=counters.doc_count,
        chunk_count=counters.chunk_count,
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
        partitions_reused=counters.partitions_reused,
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
        # 09 §6.3: оффлайн-проход — по строке на продукт. Строка нужна оператору: без неё
        # «корпус собрался» читалось бы как «сайт прочитан», хотя сети не было вовсе.
        *offline_notes,
        # План 19 §P2: пропуски по политике краула — по строке на корень. Без неё
        # «корень не дал документов» читалось бы как сбой, хотя сеть запретил режим.
        *policy_notes,
        # 17 §6 (F4b): факты о полосах — по строке на продукт, и только когда ось
        # ``crawl.host_concurrency`` объявлена. Тихая замена «полос N» на «работали
        # последовательно» была бы ровно тем дефектом, который ось обязана называть.
        *lane_notes,
        # 17 §6 (F3): смена канонического адреса документа — по строке на продукт, где это
        # случилось. Строка обязательна: адрес попадает в отчёт/цитаты, и тихая подмена
        # одного адреса другим была бы невидимым изменением артефакта.
        *(
            CANONICAL_URL_CHANGED_NOTE.format(product_id=note_product, count=count)
            for note_product, count in sorted(canonical_url_changes.items())
        ),
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
        # ``--only-failed``: учёт ремонта (work-set / отброшено с причиной) — одна строка
        # на продукт, чтобы ни один провал не исчез молча.
        *repair_notes,
    ]
    # F6 (17 §6): отчёт потребителя — по записи на продукт. Печатаемые CLI числа берутся
    # отсюда, а не считаются второй раз в команде: «сколько взято в аренду» и «сколько
    # восстановлено после просрочки» знает только сам проход по очереди.
    for report in consumer_reports:
        log_entries.append(
            {
                "event": "extract_consume",
                "product_id": report.product_id,
                "done": report.done,
                "failed": report.failed,
                "already": report.already,
                "lost": report.lost,
                "claimed": report.claimed,
                "reclaimed": report.reclaimed,
                "heartbeats": report.heartbeats,
                "reclaim_records": list(report.reclaim_records),
                "pending": report.pending,
                "docs": report.docs,
                "backlog": dict(report.backlog),
            }
        )
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
    if counters.revalidation_orphan_total:
        # Наблюдение, не ошибка и не смена статуса (I12): авторский scope разошёлся с
        # тем, что реально накопилось в ``url_state``.
        log_entries.append(
            f"corpus: {counters.revalidation_orphan_total} known URLs are outside every authored "
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
            chunks=counters.chunk_count,
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
    # F5: незакрытая очередь конверсии — это не «изолированная ошибка», а неполный
    # корпус: ``evaluate`` по такому предмету дал бы ложные ``❌``. ``failed`` останавливает
    # прогон (``Runner``), и это намеренно: «не прошло» — валидный исход.
    corpus_status: PhaseStatus = "failed" if corpus_not_converted else "ok"
    return PhaseResult(
        status=corpus_status,
        phase="corpus",
        cells=[],
        log_entries=log_entries,
        # 17 §6 (F3): публикуемая форма ``errors`` — лексикографический порядок, а не
        # порядок событий: иначе артефакт (``results[].errors``) зависел бы от планировщика
        # (сеть/пул воркеров дают разный порядок при одинаковом результате). Накопленный
        # список до этой точки хронологический; дубликаты сохраняются (два события — две
        # строки), поэтому ``sorted_error_lines`` не дедуплицирует.
        errors=sorted_error_lines(errors),
        # Advisory (не влияет на status/гейты/resume): zero-source продукты — «что осталось
        # человеку» уже на выходе Ш5, до дорогой Ш6. Видно в ``hound run`` (``pending:``),
        # ``--json`` (``results[].pending``) и в ``runs/{run_id}/logs/corpus.jsonl``
        # (запись конца фазы несёт ``pending``/``pending_items``).
        pending=zero_source_pending,
    )


def _note_interrupted_queue(
    reason: str, *, queue: ExtractQueue, errors: list[str]
) -> None:
    """Teardown прерванного краула: назвать, сколько строк конверсии осталось ``pending``.

    **Почему не «отбросить очередь».** Строка очереди — durable-факт «документ добыт»
    (I19: байты уже в ``corpus_raw``), а не эфемерное задание. Прерывание прогона его не
    отменяет: следующий запуск доработает строку по тому же адресу. Поэтому teardown
    обязан **назвать число**, а не притвориться, что работы не было: иначе «краул
    прерван» читалось бы как «конверсия закончена».

    Собственных ошибок хук не пишет: причина прерывания уже названа драйвером
    (``crawl-драйвер: … — прервано корней без исхода N``), а число — факт очереди,
    который публикуется здесь же.
    """
    pending = queue.pending()
    if pending:
        errors.append(
            f"extract_queue: crawl прерван ({reason}); строк очереди конверсии без "
            f"терминального состояния: {pending} — следующий запуск доработает их "
            "по тому же адресу"
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


#: Ключ движка браузерного рендера. Тот же литерал, по которому pipeline маршрутизирует
#: ``fetch: browser`` (``corpus_ingest.local_prefetch``) и который кладёт в карту
#: composition root. Импорт CLI-константы нарушил бы слой ``pipeline ↛ cli``.
_BROWSER_ENGINE = "browser"


def _browser_engine_crawlers(crawlers: Iterable[Any]) -> list[Any]:
    """Краулеры, владеющие браузерным движком **в этом процессе**.

    Признак — тот же литерал ``engine``, по которому pipeline маршрутизирует
    ``fetch: browser``. Транспорты без него (``remote`` — рендер на edge) и без ``close``
    (внедрённые заглушки) сюда не попадают: закрывать в браузерном потоке нечего.
    """
    seen: dict[int, Any] = {}
    for active in crawlers:
        if active is None:
            continue
        if getattr(active, "engine", None) != _BROWSER_ENGINE:
            continue
        if not callable(getattr(active, "close", None)):
            continue
        seen.setdefault(id(active), active)
    return list(seen.values())


def _shutdown_browser_owner(
    pool: ThreadPoolExecutor,
    targets: list[Any],
    *,
    errors: list[str],
    label: str = "",
) -> None:
    """Закрыть браузерный движок **на его потоке** и остановить поток фазы.

    Зачем отдельный шаг, а не ``crawler.close()`` на главном потоке. Playwright sync-API
    привязан к потоку, создавшему движок: закрытие из чужого потока — не «почти работает»,
    а отказ (``cannot switch to a different thread``), который ``BrowserCrawler.close``
    гасит best-effort. Тогда браузерный процесс и временный профиль остались бы
    незакрытыми до конца процесса — на 6.9 GiB хоста это удерживаемые сотни мегабайт на
    всю оставшуюся фазу.

    Поэтому закрытие подаётся **в тот же пул**, который исполнял браузерные фетчи: там
    поток-владелец, и там движок закрывается штатно. ``close`` идемпотентен, поэтому
    повторный вызов безопасен. Ошибки закрытия не поднимаются: они называются в
    ``errors`` — teardown не имеет права подменить исход фазы, но и молчать о незакрытом
    движке не должен.

    ``targets`` — уже отобранные :func:`_browser_engine_crawlers`: чужие транспорты из
    этого пула не трогаются (у Scrapy своё владение потоком).
    """
    def _close_engines() -> list[str]:
        """Исполнить закрытие на потоке-владельце; вернуть строки сбоев (не бросать)."""
        failures: list[str] = []
        for active in targets:
            try:
                active.close()
            except Exception as exc:  # noqa: BLE001 — teardown не подменяет исход
                failures.append(f"{type(exc).__name__}: {exc}")
        return failures

    try:
        failures = pool.submit(_close_engines).result()
    except Exception as exc:  # noqa: BLE001 — пул мог не запустить поток
        errors.append(
            f"crawl{label}: закрытие браузерного движка: {type(exc).__name__}: {exc}"
        )
    else:
        errors.extend(f"crawl{label}: close(браузер): {failure}" for failure in failures)
    finally:
        pool.shutdown(wait=True)
