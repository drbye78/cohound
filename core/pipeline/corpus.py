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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.config.crawl_sources import CrawlSourcesConfig
from core.config.domain_profiles import DomainProfile
from core.domain import (
    CorpusChunk,
    CorpusDoc,
    CrawlItemStatus,
    CrawlRunConfig,
    CrawlSessionItem,
    CrawlStrategy,
    FailureKind,
    IndexManifestEntry,
)
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
from core.ports.types import FetchMode, FetchPlan, RawPage
from core.util.hashing import content_hash, short_hash
from core.util.rate_limit import RateLimiter
from core.util.security import validate_url

__all__ = ["IndexBuild", "build_corpus"]

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


# --------------------------------------------------------------------------- #
# Детерминированные выводы (версии/идентификаторы)
# --------------------------------------------------------------------------- #


def _as_text(value: Any) -> Any:
    """Приводит enum-значение (``StrEnum``) к строке ``.value``; прочее — как есть."""
    return getattr(value, "value", value)


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
    max_attempts: int | None = None,
    progress: Callable[[dict[str, int]], None] | None = None,
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
    :param domain_profiles: типизированные доменные профили (D.2) для резолва ``FetchPlan``.
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
    :param pollers: Phase 5.2 — карта ``kind → PollPort`` для источников-каталогов
        (``feed``/``channel``): ``kind`` направления резолвится в pipeline, каталог поллится,
        entries — ссылки; контент добывается обычным путём. ``None`` → источники-каталоги не
        разворачиваются (URL с директорийным ``kind`` помечается ``FAILED`` ``source_unavailable``).
    :param session_id: явный идентификатор сеанса краула; ``None`` →
        ``f"crawl:{run_id}"`` (идемпотентно на прогон). Игнорируется без ``store``/``run_id``.
    :param executor: опциональный исполнитель краула (``CrawlExecutorPort``, Phase 3.3). При
        наличии краул исполняется **через него** (``split``/edge-планировщик): работы
        подаются одним batch-запросом, результаты забираются пагинацией, а retention/
        экстракция/дедуп/нарезка и сеансовое трекинг — общие с локальным путём
        (``_ingest_one``). ``crawler``/``crawlers`` при этом не используются. ``None`` →
        прежний локальный per-URL путь.
    :param executor_poll_interval: пауза между опросами прогресса задания исполнителя, с
        (только при ``executor``); уже терминальное задание не пейсится.
    :param executor_sleep: хук сна опроса (по умолчанию ``time.sleep``); тесты инжектят
        no-op, чтобы не платить реальное время.
    :param max_attempts: Phase 4 — потолок попыток по ``(product_id, url)``. URL с уже
        сохранённым ``CrawlSessionItem.attempts >= max_attempts`` **не** фетчится и **не**
        подаётся исполнителю: он помечается терминальным ``FAILED`` с ``error_kind =
        "attempts_exhausted"`` и понятной ошибкой (не тихий дроп). ``None`` → без потолка.
    :param progress: Phase 4 — опциональный колбэк прогресса: вызывается со снимком
        ``crawl_session_counts`` (``{status: count}``) по ходу фазы (для ``--follow``).
        Ошибки колбэка изолируются и не роняют фазу. ``None`` → без прогресс-вывода.

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

    # D4: дефолты оси стратегии/задержек из run.yaml ``crawl`` (профиль домена их переопределяет).
    crawl_strategy_default = (
        crawl_defaults.strategy if crawl_defaults is not None else CrawlStrategy.AUTO
    )
    crawl_rate_default = crawl_defaults.rate_limit if crawl_defaults is not None else None

    # P2-3: единый владелец politeness-пейсинга — corpus (адаптерный ``rate_limiter`` — None).
    limiter = rate_limiter if rate_limiter is not None else RateLimiter()

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
        product_id: str, url: str, page: RawPage, fetch_plan: FetchPlan
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
    ) -> None:
        """Экстракция → дедуп → нарезка одного URL (общая часть локального/executor путей).

        Сырые байты уже сохранены (``raw_path``); шаги изолируют ошибки в ``errors`` и
        фиксируют терминальный статус элемента сеанса через ``_mark``. Логика идентична для
        обоих путей корпуса — вынесена, чтобы не дублировать её.
        """
        nonlocal duplicate_count
        # 3. Экстракция (грациозная деградация адаптера через .error).
        try:
            extracted = extractor.extract(page, fetch_plan.extract)
        except Exception as exc:  # noqa: BLE001
            reason = f"{type(exc).__name__}: {exc}"
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
            # Back-pressure не снят: изолированный сбой подачи (не тихий провал прогона);
            # работы помечаются ``source_unavailable`` при ingest (результатов нет).
            errors.append(
                "executor.submit: back-pressure не снят после "
                f"{_SUBMIT_MAX_ATTEMPTS} попыток (accepted=false)"
            )
            return {}

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

    def _work_item(pid: str, url: str, target: Any) -> CrawlWorkItem:
        """Собирает работу волны из резолвнутой цели (оси ``FetchPlan`` + провенанс)."""
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
        )

    def _run_executor() -> list[CrawlWorkItem]:
        """Phase 3.3/5.3: волна 1 — top-level НЕ-директорные работы через ``executor``.

        Резолвит цель каждого top-level URL и кладёт её в ``targets``/``applied_engines`` (как
        локальный путь). Источники-каталоги (``kind ∈ _DIRECTORY_KINDS``) сюда не попадают:
        их разворачивает :func:`_run_executor_waves` (волна 2). Уже resum'ащиеся элементы
        (retained raw на диске) и исчерпавшие попытки (``--max-attempts``) НЕ подаются —
        ingest/re-mark идёт в основном цикле Pass A. Возвращает список работ волны 1.
        """
        assert executor is not None
        work: list[CrawlWorkItem] = []
        for product_id in products:
            for url in url_map.get(product_id, []):
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
                applied_engines[(product_id, url)] = target.engine
                if target.kind in _DIRECTORY_KINDS:
                    continue  # каталог — волна 2 (разворачивается ``_run_executor_waves``)
                prior_item = items.get((product_id, url))
                if _resume_source(prior_item) is not None:
                    continue  # resume без подачи: ingest ре-извлечёт из retained raw
                # Phase 4: исчерпавшие попытки URL не подаются исполнителю — их помечает
                # терминальным ``FAILED`` основной цикл (``max_attempts``).
                if (
                    max_attempts is not None
                    and prior_item is not None
                    and prior_item.attempts >= max_attempts
                ):
                    continue
                work.append(_work_item(product_id, url, target))
        return work

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

    def _process_local(
        pid: str, wu: str, wtarget: Any
    ) -> tuple[RawPage, Path, FetchPlan] | None:
        """Локальный per-URL путь: resume/браузер-guard/пейсинг/фетч/retention (Ш5).

        Общая часть локального пути для обычных страниц и записей каталога. Возвращает
        ``(page, raw_path, fetch_plan)`` для последующей ``_ingest_one``; ``None`` — исход
        пропущен/сбой (терминальный ``FAILED`` уже проставлен ``_mark``), вызывающий делает
        ``continue``. Активный краулер выбирается по ``wtarget.engine`` (fallback — ``crawler``).
        """
        fetch_plan = wtarget.plan
        prior_item = items.get((pid, wu))
        resume = _resume_source(prior_item) if prior_item is not None else None

        active = ((crawlers or {}).get(wtarget.engine) if wtarget.engine else None) or crawler
        applied_engines[(pid, wu)] = getattr(active, "engine", None) or wtarget.engine

        if resume is not None:
            assert prior_item is not None  # resume возможен только при наличии элемента
            content, raw_path = resume
            page = _resume_page(prior_item, wu, content, raw_path, fetch_plan)
            # Счётчик отражает статус возобновляемого элемента (FETCHED — retryable,
            # FAILED — re-extract), а не ``pending``: он уже персистится в сеансе, и при
            # последующем сбое chunker'а URL будет посчитан как ``fetched``, а не потерян.
            return page, raw_path, fetch_plan

        # D2: браузерный рендер требует поддерживающего краулера. Проверка URL-скоупедирована
        # (а не на сборке композиции): фаза не падает, пропускается только этот URL.
        if (
            getattr(active, "supports_browser", True) is False
            and fetch_plan.fetch is FetchMode.BROWSER
        ):
            reason = (
                f"{pid}/{wu}: требует браузерный рендер, но краулер "
                f"{type(active).__name__} не поддерживает browser; "
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
            )
            return None

        # P2-3: politeness-пейсинг — единый владелец corpus (R7-независимый; стратегия/лимит
        # берутся из уже резолвнутого ``FetchPlan``). Применяется **только** к живому фетчу:
        # путь ``resume`` (retained raw, выше) не пейсится.
        host = (urlsplit(wu).hostname or "").lower()
        limiter.wait(host, fetch_plan.rate_limit)

        # 1. Краул.
        try:
            page = active.fetch(wu, fetch_plan)
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            reason = f"{type(exc).__name__}: {exc}"
            errors.append(f"{pid}/{wu}: crawl: {reason}")
            _mark(
                pid,
                wu,
                CrawlItemStatus.FAILED,
                error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                error=reason,
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
            )
            return None

        # 2. Retention (I19): сырые байты пишутся ДО экстракции (общий шаг).
        raw_path = _retain(pid, wu, page, fetch_plan)
        if raw_path is None:
            return None
        return page, raw_path, fetch_plan

    def _run_executor_waves() -> tuple[
        dict[tuple[str, str], CrawlResultItem], dict[str, list[str]]
    ]:
        """Phase 5.3: исполнение краула через ``executor`` волнами (профиль ``split``).

        Волна 1 — top-level НЕ-директорные URL (:func:`_run_executor`); затем опрос каталогов
        (``feed``/``channel``) через ``pollers[kind]`` (:func:`_expand_directory`); затем
        **волна 2** — записи каталогов, отдельным ``submit`` с собственным
        ``idempotency_key``. Каталог не ретейнится и не ингестится (I19); записи добываются
        исполнителем и проходят штатный rehash. Один уровень развёртки (записи не
        ре-расширяются). Возвращает ``(collected, work_by_product)``: карту результатов по
        ``(product_id, url)`` и упорядоченный (как в локальном пути) список ingest-URL
        по продукту для Pass A.
        """
        assert executor is not None

        # --- Волна 1: top-level НЕ-директорные работы (отдельный submit) --- #
        wave1 = _run_executor()
        collected: dict[tuple[str, str], CrawlResultItem] = dict(_submit_collect(wave1))

        # --- Опрос каталогов + волна 2: записи каталогов --- #
        wave2: list[CrawlWorkItem] = []
        # Порядок ingest в Pass A зеркалит локальный: не-директорный URL даёт себя, каталог —
        # свои записи (в порядке поллера) на своём месте.
        work_by_product: dict[str, list[str]] = {pid: [] for pid in products}
        for product_id in products:
            for url in url_map.get(product_id, []):
                target = targets[(product_id, url)]
                if target.kind not in _DIRECTORY_KINDS:
                    work_by_product[product_id].append(url)
                    continue

                # Phase 4: исчерпавший попытки каталог не поллится — терминальный ``FAILED``
                # (сеанс финализируется ``PARTIAL``); в Pass A каталог не входит.
                prior_item = items.get((product_id, url))
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

                # ``_expand_directory`` помечает каталог ``FETCHED`` (без raw) либо
                # ``FAILED source_unavailable`` (нет поллера/плохие params/ошибка опроса).
                entry_urls = _expand_directory(product_id, url, target) or []
                for entry_url in entry_urls:
                    work_by_product[product_id].append(entry_url)

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

                    e_prior = items.get((product_id, entry_url))
                    if _resume_source(e_prior) is not None:
                        continue  # resume без подачи: ingest ре-извлечёт из retained raw
                    if (
                        max_attempts is not None
                        and e_prior is not None
                        and e_prior.attempts >= max_attempts
                    ):
                        continue  # исчерпано: пометит терминальным ``FAILED`` Pass A
                    wave2.append(_work_item(product_id, entry_url, entry_target))

        # --- Волна 2: отдельный submit с собственным ``idempotency_key`` --- #
        collected.update(_submit_collect(wave2))
        return collected, work_by_product

    # Phase 5.3: в executor-режиме состав/порядок ingest-URL задаёт волновая развёртка;
    # локальный путь по-прежнему идёт по ``url_map``.
    collected: dict[tuple[str, str], CrawlResultItem] = {}
    work_by_product: dict[str, list[str]] = {}
    if executor is not None:
        collected, work_by_product = _run_executor_waves()

    for product_id in products:
        ingest = _ProductIngest(product_id=product_id)
        # I17: дедуп в пределах продукт-партиции (см. docstring модуля).
        seen_docs: dict[str, str] = {}
        seen_chunk_ids: set[str] = set()

        # Phase 5.3: в executor-режиме итерируем по волновому набору (каталоги уже опрошены,
        # их URL в наборе нет; записи — на месте каталогов). Локальный путь — по ``url_map``.
        for url in (
            url_map.get(product_id, [])
            if executor is None
            else work_by_product.get(product_id, [])
        ):
            # L1a: resume — если по ``(product_id, url)`` есть элемент сеанса с сохранёнными
            # байтами, НЕ фетчим: ре-извлекаем из retained raw (I19: raw — источник).
            prior_item = items.get((product_id, url))
            resume = _resume_source(prior_item) if prior_item is not None else None

            # Phase 4: потолок попыток (``--max-attempts``). URL, уже исчерпавший попытки,
            # не фетчится и не подаётся исполнителю: терминальный ``FAILED`` с явным
            # ``error_kind`` (не тихий дроп) → сеанс финализируется ``PARTIAL``.
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

            if executor is None:
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
                # каждая добывается обычным путём (один уровень развёртки, без ре-расширения).
                entry_urls = _expand_directory(product_id, url, target)
                if entry_urls is None:
                    work: list[tuple[str, Any]] = [(url, target)]
                else:
                    work = []
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
                        work.append((entry_url, entry_target))

                for work_url, work_target in work:
                    outcome = _process_local(product_id, work_url, work_target)
                    if outcome is None:
                        continue
                    page, raw_path, fetch_plan = outcome
                    _ingest_one(
                        product_id,
                        work_url,
                        page,
                        fetch_plan,
                        raw_path,
                        ingest,
                        seen_docs,
                        seen_chunk_ids,
                    )
                    _emit_progress()
                continue

            # Phase 3.3: executor-ветка — цель уже разрешена при подаче работ; результат
            # берётся из сбора (или resum'ится из retained raw без подачи).
            target = targets[(product_id, url)]
            fetch_plan = target.plan

            if resume is not None:
                assert prior_item is not None  # resume возможен только при наличии элемента
                content, raw_path = resume
                page = _resume_page(prior_item, url, content, raw_path, fetch_plan)
            else:
                result = collected.get((product_id, url))
                if result is None:
                    errors.append(f"{product_id}/{url}: executor: результат не получен")
                    _mark(
                        product_id,
                        url,
                        CrawlItemStatus.FAILED,
                        error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                        error="executor: результат не получен",
                    )
                    continue
                if result.error or result.raw_bytes is None:
                    reason = result.error or "executor: пустой raw_bytes"
                    errors.append(f"{product_id}/{url}: executor: {reason}")
                    _mark(
                        product_id,
                        url,
                        CrawlItemStatus.FAILED,
                        error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                        error=reason,
                    )
                    continue
                # S1: применяемый движок — фактический транспорт из результата шва.
                applied_engines[(product_id, url)] = result.engine or target.engine
                page = RawPage(
                    url=result.url,
                    content=result.raw_bytes,
                    fetched_at=result.fetched_at or "",
                    fetch_mode=fetch_plan.fetch,
                    lang=None,
                    status_code=result.http_status,
                    content_type=result.content_type,
                )
                # 2. Retention (I19) — общий шаг с локальным путём.
                raw_path = _retain(product_id, url, page, fetch_plan)
                if raw_path is None:
                    continue

            # 3-5. Экстракция/дедуп/нарезка — общая часть обоих путей (``_ingest_one``).
            _ingest_one(
                product_id,
                url,
                page,
                fetch_plan,
                raw_path,
                ingest,
                seen_docs,
                seen_chunk_ids,
            )
            _emit_progress()

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
    )
    # Примечание: индексная сборка «возвращается» в log_entries (PhaseResult — контракт фазы;
    # W8 читает IndexBuild отсюда). Счётчики — в полях IndexBuild; дубликаты — в errors.
    return PhaseResult(status="ok", phase="corpus", cells=[], log_entries=[build], errors=errors)
