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
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import Executor
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.config.crawl_sources import CrawlSourcesConfig
from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain import (
    ArchiveObservation,
    CorpusChunk,
    CorpusCoverage,
    CorpusMember,
    CrawlItemStatus,
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
from core.pipeline import corpus_commit, corpus_drain, corpus_ingest
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

# L2: расчёт/персистенция покрытия и статуса продукта вынесены в свой модуль. Здесь
# остаются имена, которые этот модуль читает сам, и публичный ``derive_corpus_coverage``
# (``__all__``; тесты и ``selfcheck`` адресуют его по ``corpus`` — ``noqa: F401``).
from core.pipeline.corpus_coverage import (  # noqa: F401
    ONLY_FAILED_REPAIR_REASON,
    ZERO_SOURCE_ALL_SEEDS_REJECTED,
    ZERO_SOURCE_NO_SEEDS,
    ZERO_SOURCE_NO_URLS_ENTRY,
    _final_product_status,
    _persist_corpus_coverage,
    _product_notes,
    derive_corpus_coverage,
)

#: L5 шаг 3: доводка члена контейнера и класс терминального отказа попыток вынесены в
#: ``corpus_drain``. Алиасы — не удобство: ``tests/pipeline/test_corpus_descent.py``
#: подменяет ``corpus._ingest_member_doc``, и вызов обязан идти через атрибут **этого**
#: модуля (параметр ``ingest_member_doc`` читается из глобала в точке вызова).
from core.pipeline.corpus_drain import _MAX_ATTEMPTS_ERROR_KIND  # noqa: F401
from core.pipeline.corpus_drain import (  # noqa: F401
    ingest_member_doc as _ingest_member_doc,
)

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

# L1: runtime экстракции вынесен в свой модуль. Здесь остаются имена, которые этот модуль
# читает сам (``_EXTRACT_MAX_POOL_REBUILDS``, ``_ExtractPools``, ``ExtractionPool``), и
# публичный ``set_worker_extractor``: его адресуют тесты по ``corpus`` (``noqa: F401``).
from core.pipeline.extraction_runtime import (  # noqa: F401
    _EXTRACT_MAX_POOL_REBUILDS,
    ExtractionPool,
    _ExtractPools,
    set_worker_extractor,
)
from core.pipeline.index_build import IndexBuild
from core.pipeline.liveness import Heartbeat
from core.pipeline.manual_artifacts import (
    ManualArtifacts,
)
from core.pipeline.resources import ResourcesObservation, summarize_chunks_per_doc
from core.pipeline.runner import PhaseResult
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
    IndexCompositionRow,
    IndexPartitionRow,
    ProductStatePort,
)
from core.ports.types import (
    FetchMode,
)
from core.util.index_partition import build_root as _partition_root
from core.util.rate_limit import RateLimiter

__all__ = ["ExtractionPool", "IndexBuild", "build_corpus", "derive_corpus_coverage"]


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

#: Причина неисполненных раундов SERP-контура при ``offline``: поиск — тоже сеть.
_SERP_OFFLINE_REASON = "offline_refresh_policy"


def _as_refresh_policy(value: str) -> RefreshPolicy:
    """Приводит строку политики наблюдения к :class:`RefreshPolicy` (дефолт ``auto``)."""
    try:
        return RefreshPolicy(value)
    except ValueError:
        return RefreshPolicy.AUTO


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
        GET (валидаторы не отправляются); ``offline`` — **ноль сетевых вызовов**: ни
        фетча, ни условного GET, ни sitemap/feed-развёртки, ни внешнего поиска. Корпус
        собирается из durable-субстрата отдельным проходом
        (:func:`core.pipeline.corpus_ingest.offline_reuse_product`), документы получают
        ``reuse_kind=reused`` и ``http_status=NULL``, экстрактор и чанкер не вызываются.
        Авторский корень, известный ``url_state``, тоже переиспользуется; корень, которого
        в состоянии нет, — видимая ошибка (сеть в этой политике отсутствует). Покрытие
        такого прогона неполно по построению: наблюдений **этого** прогона нет
        (``observed_sources=0``, причина ``offline_no_network_observation``).
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

    # 09 §6.3/§9 (MVP-3b): политика наблюдения и режимы продуктов. ``state is None`` —
    # legacy-путь: все три аргумента игнорируются целиком (поведение байт-в-байт).
    # Резолвится **до** SERP-контура и дренажа: ``offline`` — «наблюдать нечем и не
    # будем», то есть ни фетча, ни условного GET, ни sitemap/feed-развёртки, ни внешнего
    # поиска. Корпус в этом случае собирается из durable-субстрата
    # (:func:`core.pipeline.corpus_ingest.offline_reuse_product`).
    policy = _as_refresh_policy(refresh_policy)
    offline = policy is RefreshPolicy.OFFLINE

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
    #: 09 §6.3: ``offline`` — ноль сетевых вызовов, и внешний поиск тоже сеть. Контур не
    #: исполняется, но и не молчит: наблюдение называет причину (``not_executed_reason``),
    #: иначе «контур выключен» читалось бы как решение оператора, а не как политика прогона.
    if policy is RefreshPolicy.OFFLINE and serp_rounds:
        serp_observation["rounds"]["declared"] = int(serp_rounds)
        serp_observation["rounds"]["not_executed"] = int(serp_rounds)
        serp_observation["rounds"]["not_executed_reason"] = _SERP_OFFLINE_REASON
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
        timings=timings,
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

    # L5 шаг 3: ``drain_root`` (дренаж одного корня) вынесен в ``corpus_drain``. Фазовые
    # зависимости связываются здесь один раз (``partial`` не замыкание и не состояние —
    # он только фиксирует аргументы). Per-продуктные связываются на границе продукта:
    # они пересоздаются на каждой итерации, и позднее связывание было бы дефектом (B023),
    # а не удобством.
    _drain_root_phase = partial(
        corpus_drain.drain_root,
        errors=errors,
        store=store,
        run_id=run_id,
        chunker=chunker,
        container_for=container_for,
        items=items,
        max_attempts=max_attempts,
        wave_items=wave_items,
        # Читается из глобала модуля в точке вызова: тест
        # ``test_split_round_budget_valve_truncates`` подменяет его атрибутом ``corpus``.
        max_waves=_EXECUTOR_MAX_WAVES_PER_ROOT,
        worker_rss_dir=worker_rss_dir,
        extraction_pool=extraction_pool,
        extract_cache=extract_cache,
        extractor=extractor,
        extractor_factory=extractor_factory,
        executor=executor,
        pool_factory=pool_factory,
        pools=pools,
        extract_pending=extract_pending,
        extract_finalizers=extract_finalizers,
        extract_timeout=_extract_timeout,
        counters=counters,
        timings=timings,
        scope_hosts_by_product=scope_hosts_by_product,
        sitemap_seed_used=sitemap_seed_used,
        document_skips=document_skips,
        artifact_probes=artifact_probes,
        archive_observations=archive_observations,
        corpus_member_rows=corpus_member_rows,
        emit=emit,
        mark=_mark,
        commit_local=_commit_local,
        local_fetch=_local_fetch,
        fetch_and_commit=_fetch_and_commit,
        local_prefetch=_local_prefetch,
        process_local=_process_local,
        submit_collect=_submit_collect,
        work_item=_work_item,
        resolve_target=_resolve_target,
        reuse_for_page=_reuse_for_page,
        ingest_one=_ingest_one,
        finish_one=_finish_one,
        finish_extract=_finish_extract,
        fresh_sitemap_seeds=_fresh_sitemap_seeds,
        limits_for=_limits_for_url,
        note_scheduling=_note_scheduling,
        ingest_member_doc=_ingest_member_doc,
        persist_docs=_persist_docs,
    )
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
        #: 09 §6.3: ``offline`` — корпус из durable-субстрата **до** дренажа и вместо него.
        #: Отдельный проход, а не дренаж с ревалидационными seed'ами: у оффлайн-элемента
        #: нет ни фетча, ни sitemap/feed-развёртки, ни BFS, поэтому проход не заходит в
        #: код, единственное назначение которого — сеть. Корни продукта разрешаются в
        #: состоянии (корень, которого там нет, — видимая ошибка, а не GET).
        offline_report = None
        if offline:
            offline_report = _offline_reuse_product(
                product_id,
                product_roots,
                incremental_items,
                state=state,
                errors=errors,
                chunker_version=resolved_chunker_version,
                embed_model_id=embed_model_id,
                store=store,
                ingest=ingest,
                seen_docs=seen_docs,
                seen_chunk_ids=seen_chunk_ids,
                duplicates=duplicates,
                doc_counters=doc_counters,
                partial_unknown_products=partial_unknown_products,
                persist_docs=_persist_docs,
            )
            offline_notes.append(offline_report.note(product_id))
        #: 09 §6.3: группировка **всех** известных URL по авторским корням (scope
        #: inference) до применения операторского потолка. ``state is None``/
        #: не-incremental/``offline`` ⇒ пусто: grouping — no-op (байт-в-байт). Под
        #: ``offline`` группировка не нужна: она адресует ревалидационные бакеты дренажа,
        #: а дренажа в этой политике нет — известные URL уже разрешены выше.
        if incremental_items and not offline:
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

        # L5 шаг 3: per-продуктное состояние ``drain_root`` связывается здесь, при
        # создании ``partial``. Раньше это были B023-дефолты замыкания: значения
        # пересоздаются на каждой итерации цикла, и привязка в точке определения была
        # единственным способом не прочитать чужой продукт.
        _drain_root = partial(
            _drain_root_phase,
            ingest=ingest,
            seen_docs=seen_docs,
            seen_chunk_ids=seen_chunk_ids,
            serp_ingest=serp_ingest,
            resume_counted=resume_counted,
            revalidation_url_set=revalidation_url_set,
            owner_root=_owner_root,
        )


        # P3-A2: **однопутевой** product-цикл. Развёртка каталогов/sitemap делается один раз
        # до drain (общий блок построения корней); транспорт выбирается по ``executor is None``.
        # Порядок expand → seed → drain на корень совпадает с локальным и является порядком
        # ingest (interleaving-трюк ``work_by_product`` больше не нужен).
        #: Корни, реально дренированные в этом продукте (для leftover-групп каталогов).
        drained_roots: set[str] = set()
        # Ремонт (``--only-failed``) — **не** обход: авторские корни не дренятся, их роль
        # здесь — только пространство scope-инференса для группировки известных URL.
        # Работа идёт через leftover-группы/orphan-бакеты ниже (``seed=False`` ⇒
        # ``add_revalidate_seed``, depth-0 листья вне бюджета discovery).
        # ``offline`` — дренажа нет вовсе: корпус уже собран из durable-субстрата, а дренаж
        # начинается с seed-фетча, то есть с сети.
        for url in (() if (repair_urls or offline) else url_map.get(product_id, [])):
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
        if offline:
            # 09 §6.3: оффлайн-прогон **не наблюдал ничего** — ни одного сетевого вызова.
            # Это не ``no_seeds`` (seed'ы есть) и не «продукт пуст» (документы могут быть
            # переиспользованы из durable-субстрата). Причина называется в строке покрытия
            # всегда: ``observed_sources`` — счётчик наблюдений **этого** прогона, и он
            # нулевой, поэтому ``complete`` не может стать ``True`` (условие «ни одного
            # наблюдения» в :func:`derive_corpus_coverage`). ``blocked`` ставится только
            # когда переиспользовать было нечего: «материал есть, охват неполон» — это
            # ``partial``, а не ``blocked``.
            offline_reasons[product_id] = COVERAGE_OFFLINE_NO_OBSERVATION
            if not ingest.docs and not manual_resolved:
                product_reason = COVERAGE_OFFLINE_NO_OBSERVATION
                zero_source_pending.append(
                    f"{product_id}: offline — переиспользовать нечего и сеть не "
                    "трогалась; корпус пуст, оценка отметит все ячейки failed, а не ❌. "
                    "Нужен прогон с --refresh auto/revalidate"
                )
                errors.append(
                    f"{product_id}: offline corpus без единого документа "
                    f"({COVERAGE_OFFLINE_NO_OBSERVATION}): "
                    f"roots={len(product_roots)}, "
                    f"reused=0, no_version="
                    f"{len(offline_report.no_version) if offline_report else 0}, "
                    f"unknown_roots="
                    f"{len(offline_report.unknown_roots) if offline_report else 0} — "
                    "ячейки будут failed, не ❌"
                )
        # Zero-source — утверждение о **сеансе**: без сеанса «нет элементов» не значит
        # «нет источников» (счётчики просто не ведутся), поэтому причина не выдумывается.
        # ``and not manual_resolved``: продукт, чей материал предъявлен объявленным
        # артефактом, без источников **не** остался — иначе ошибка утверждала бы
        # пустой корпус при непустом.
        elif session is not None and not product_items and not manual_resolved:
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
                # 09 §6.3: оффлайн-проход — отдельные поля, а не пересказ ``fetched``/
                # ``docs``: «взято из состояния» и «наблюдено в этом прогоне» — разные
                # факты, и первое не становится вторым от того, что документ попал в корпус.
                "offline": (
                    None
                    if offline_report is None
                    else {
                        "reused_docs": offline_report.reused,
                        "reused_chunks": offline_report.chunks,
                        "no_version": list(offline_report.no_version),
                        "unknown_roots": list(offline_report.unknown_roots),
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
                    reasons={**zero_source_reasons, **offline_reasons},
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
                reasons={**zero_source_reasons, **offline_reasons},
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
        # 09 §6.3: оффлайн-проход — по строке на продукт. Строка нужна оператору: без неё
        # «корпус собрался» читалось бы как «сайт прочитан», хотя сети не было вовсе.
        *offline_notes,
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

