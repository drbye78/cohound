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

**Durable-краул (D3).** При наличии ``store`` и ``run_id`` фаза ведёт ``crawl_attempts``
(аудит/очередь/resume): per-URL статус ``FetchStatus`` + провенанс попытки. URL с уже
сохранёнными байтами **не фетчится повторно** — ``RawPage`` реконструируется из retained
``corpus_raw`` (I19: raw — источник) и ре-извлекается детерминированно. ``store is None`` или
``run_id is None`` → поведение без durable-краула. Все записи краула изолированы (сбой →
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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.config.domain_profiles import DomainProfile
from core.domain import (
    CorpusChunk,
    CorpusDoc,
    CrawlAttempt,
    FailureKind,
    FetchStatus,
    IndexManifestEntry,
)
from core.pipeline.fetch_plan import resolve_fetch_plan
from core.pipeline.runner import PhaseResult
from core.ports.chunker import ChunkerPort
from core.ports.crawler import CrawlerPort
from core.ports.embedder import EmbedderPort
from core.ports.extractor import ExtractorPort
from core.ports.index_builder import IndexBuilderPort, IndexMeta
from core.ports.types import Chunk as PortChunk
from core.ports.types import FetchMode, FetchPlan, RawPage
from core.util.hashing import content_hash, short_hash

__all__ = ["IndexBuild", "build_corpus"]

#: Разделитель полей material-строки ``index_build_id`` (unit separator — однозначная склейка).
_FIELD_SEP = "\x1f"

#: Схема формулы ``index_build_id`` (P2): входит в material, поэтому будущая ревизия формулы
#: всегда даёт новые id (старые сборки не переиспользуются).
_BUILD_ID_SCHEMA = "corpus-build-2"


@dataclass
class IndexBuild:
    """Результат сборки корпуса (Ш5): сборка + продукт-партиции + retention-документы.

    :param index_build_id: детерминированный идентификатор иммутабельной сборки (§B.1/§13).
    :param entries: по одной ``IndexManifestEntry`` на продукт-партицию (I17).
    :param docs: уникальные ``CorpusDoc`` (retention + provenance экстракции, §3.2).
    :param chunk_count: суммарное число проиндексированных чанков.
    :param crawl_pending: D3-наблюдаемость: URL в очереди (не фетчились).
    :param crawl_fetched: D3: сырые байты сохранены, экстракция не завершена.
    :param crawl_done: D3: fetched+extracted+chunked успешно.
    :param crawl_failed: D3: терминально после попыток.
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
# Durable-краул (D3): аудит/очередь/resume per-URL
# --------------------------------------------------------------------------- #


def _record_crawl(
    store: Any,
    run_id: str | None,
    product_id: str,
    url: str,
    status: FetchStatus,
    errors: list[str],
    **fields: Any,
) -> None:
    """Изолированно пишет переход статуса краула в ``crawl_attempts`` (D3).

    Сбой store фиксируется в ``errors`` и не прерывает фазу (контракт изоляции фаз,
    ``runner.py``). ``store is None`` или ``run_id is None`` → no-op: ровно поведение без
    durable-краула (обратная совместимость).
    """
    if store is None or run_id is None:
        return
    try:
        store.mark_crawl_status(run_id, product_id, url, status, **fields)
    except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
        errors.append(
            f"{product_id}/{url}: crawl_attempts {_as_text(status)}: "
            f"{type(exc).__name__}: {exc}"
        )


def _resume_source(
    attempt: CrawlAttempt | None,
) -> tuple[bytes, Path] | None:
    """Возвращает retained-байты для resume, если запись краула позволяет НЕ фетчить (D3).

    Пригодны записи ``FETCHED``/``DONE`` (сырые байты уже сохранены) и ``FAILED`` с
    сохранённым ``raw_path`` (re-extract из тех же байтов без сети). ``PENDING``/отсутствие
    записи, отсутствие ``raw_path`` или файла на диске → ``None`` (обычный путь фетча).

    **Верификация (D3).** Если в записи задан ``content_hash``, а хеш прочитанных байтов
    не совпадает, файл считается непригодным (повреждён/подменён) → ``None``: резюм не
    должен молча давать другой ``index_build_id`` и самопротиворечивую аудит-строку.
    I19: retained raw — источник; экстракция детерминирована.
    """
    if attempt is None:
        return None
    if attempt.status not in (FetchStatus.FETCHED, FetchStatus.DONE, FetchStatus.FAILED):
        return None
    if not attempt.raw_path:
        return None
    path = Path(attempt.raw_path)
    if not path.is_file():
        return None
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if attempt.content_hash and content_hash(data) != attempt.content_hash:
        return None
    return data, path


def _resume_page(
    attempt: CrawlAttempt, url: str, content: bytes, path: Path, fetch_plan: FetchPlan
) -> RawPage:
    """Реконструирует ``RawPage`` из retained-байтов и записи краула (без сети, D3).

    ``fetched_at`` — из ``last_attempt`` записи, иначе mtime файла (UTC). ``fetch_mode`` —
    из записи (если распознаётся, иначе ``fetch_plan.fetch``). ``content_type``/``lang``/
    ``status_code`` — из провенанса записи (``lang`` персистится с D3, иначе свежий и
    возобновлённый прогоны расходились бы по ``corpus_docs.lang``/``corpus_chunks.lang``).
    """
    fetched_at = attempt.last_attempt
    if not fetched_at:
        try:
            fetched_at = _dt.datetime.fromtimestamp(
                path.stat().st_mtime, tz=_dt.timezone.utc
            ).isoformat()
        except OSError:
            fetched_at = ""
    fetch_mode = fetch_plan.fetch
    if attempt.fetch_mode:
        try:
            fetch_mode = FetchMode(attempt.fetch_mode)
        except ValueError:
            fetch_mode = fetch_plan.fetch
    return RawPage(
        url=url,
        content=content,
        fetched_at=fetched_at or "",
        fetch_mode=fetch_mode,
        lang=attempt.lang,
        status_code=attempt.http_status,
        content_type=attempt.content_type,
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
    package_id: str | None = None,
    run_id: str | None = None,
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
    :param package_id: явный пакет корпуса; ``None`` → производный от ``plan``.
    :param run_id: идентификатор прогона для durable-краула (D3). При наличии ``store`` и
        ``run_id`` фаза ведёт ``crawl_attempts`` (аудит/очередь/resume): URL с сохранёнными
        байтами НЕ фетчится повторно, а ре-извлекается из retained raw. ``None`` (или
        ``store is None``) → поведение без durable-краула.

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

    # D3: prior durable-состояние краула (аудит/очередь/resume). Ключ — ``(product_id, url)``:
    # PK таблицы ``crawl_attempts`` = ``(run_id, product_id, url)``, поэтому один и тот же URL
    # под разными продуктами имеет независимые записи (иначе строки затирали бы друг друга —
    # I17/§4.4: партиция и её retained raw принадлежат продукту).
    prior: dict[tuple[str, str], CrawlAttempt] = {}
    if store is not None and run_id is not None:
        try:
            prior = {
                (attempt.product_id, attempt.url): attempt
                for attempt in store.iter_crawl_attempts(run_id)
            }
        except Exception as exc:  # noqa: BLE001 — resume best-effort, не роняет фазу
            errors.append(f"iter_crawl_attempts: {type(exc).__name__}: {exc}")
            prior = {}

    # D3: наблюдаемые финальные статусы per-URL (для счётчиков IndexBuild).
    crawl_status: dict[tuple[str, str], FetchStatus] = {}
    for pid in products:
        for u in url_map.get(pid, []):
            crawl_status[(pid, u)] = FetchStatus.PENDING

    def _mark(pid: str, u: str, status: FetchStatus, **fields: Any) -> None:
        """Помечает статус URL и (при store+run_id) персистит его в ``crawl_attempts``."""
        crawl_status[(pid, u)] = status
        _record_crawl(store, run_id, pid, u, status, errors, **fields)

    # --- Pass A (ingest): crawl → retention → extract → dedup → chunk ---------- #
    # Векторы/физический индекс и персистенция corpus_chunks/index_manifest не выполняются:
    # сначала собирается содержимое всего батча, затем финализируется index_build_id.
    ingests: list[_ProductIngest] = []
    for product_id in products:
        ingest = _ProductIngest(product_id=product_id)
        # I17: дедуп в пределах продукт-партиции (см. docstring модуля).
        seen_docs: dict[str, str] = {}
        seen_chunk_ids: set[str] = set()

        for url in url_map.get(product_id, []):
            # D2: единый план резолвится в pipeline и передаётся и краулеру, и экстрактору.
            fetch_plan = resolve_fetch_plan(
                url, domain_profiles or {}, explicit_fetch
            )

            # D3: resume — если по ``(product_id, url)`` есть durable-запись с сохранёнными
            # байтами, НЕ фетчим: ре-извлекаем из retained raw (I19: raw — источник).
            attempt = prior.get((product_id, url))
            resume = _resume_source(attempt) if attempt is not None else None

            if resume is not None:
                assert attempt is not None  # resume возможен только при наличии записи
                content, raw_path = resume
                raw_hash = attempt.content_hash or content_hash(content)
                page = _resume_page(attempt, url, content, raw_path, fetch_plan)
                # D3: счётчик отражает статус возобновляемой записи (FETCHED — retryable,
                # DONE — успех), а не ``pending``; при последующем сбое chunker'а URL будет
                # посчитан как ``fetched``, а не потерян.
                crawl_status[(product_id, url)] = attempt.status
            else:
                # D2: браузерный рендер требует поддерживающего краулера. Проверка
                # URL-скоупедирована (а не на сборке композиции): фаза не падает, пропускается
                # только этот URL. Резолв проверяется — явный override на ``http`` снимает ошибку.
                if (
                    getattr(crawler, "supports_browser", True) is False
                    and fetch_plan.fetch is FetchMode.BROWSER
                ):
                    errors.append(
                        f"{product_id}/{url}: требует браузерный рендер, но краулер "
                        f"{type(crawler).__name__} не поддерживает browser; "
                        f"задайте COHOUND_SCOUT_CRAWLER=scrapy"
                    )
                    continue

                # 1. Краул.
                try:
                    page = crawler.fetch(url, fetch_plan)
                except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                    reason = f"{type(exc).__name__}: {exc}"
                    errors.append(f"{product_id}/{url}: crawl: {reason}")
                    _mark(
                        product_id,
                        url,
                        FetchStatus.FAILED,
                        error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                        error=reason,
                    )
                    continue
                if page is None:
                    errors.append(f"{product_id}/{url}: crawl: пустой RawPage")
                    _mark(
                        product_id,
                        url,
                        FetchStatus.FAILED,
                        error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                        error="пустой RawPage",
                    )
                    continue

                # 2. Retention (I19): сырые байты пишутся ДО экстракции.
                raw_hash = content_hash(page.content)
                try:
                    raw_path = raw_root_path / product_id / f"{raw_hash}.bin"
                    raw_path.parent.mkdir(parents=True, exist_ok=True)
                    raw_path.write_bytes(page.content)
                except Exception as exc:  # noqa: BLE001
                    reason = f"{type(exc).__name__}: {exc}"
                    errors.append(f"{product_id}/{url}: retention: {reason}")
                    # D3: сбой сохранения байтов = недоступность источника по этому URL;
                    # помечаем FAILED, чтобы очередь/счётчики были правдивы (не ``pending``).
                    _mark(
                        product_id,
                        url,
                        FetchStatus.FAILED,
                        error_kind=FailureKind.SOURCE_UNAVAILABLE.value,
                        error=f"retention: {reason}",
                    )
                    continue
                # D3: байты сохранены — durable-след попытки (provenance фетча).
                _mark(
                    product_id,
                    url,
                    FetchStatus.FETCHED,
                    content_hash=raw_hash,
                    raw_path=str(raw_path),
                    http_status=page.status_code,
                    content_type=page.content_type,
                    lang=page.lang,
                    fetch_mode=_as_text(fetch_plan.fetch),
                    extract_route=_as_text(fetch_plan.extract),
                )

            # 3. Экстракция (грациозная деградация адаптера через .error).
            try:
                extracted = extractor.extract(page, fetch_plan.extract)
            except Exception as exc:  # noqa: BLE001
                reason = f"{type(exc).__name__}: {exc}"
                errors.append(f"{product_id}/{url}: extract: {reason}")
                _mark(
                    product_id,
                    url,
                    FetchStatus.FAILED,
                    error_kind=FailureKind.PARSER_FAILED.value,
                    error=reason,
                    raw_path=str(raw_path),
                )
                continue
            if extracted is None or getattr(extracted, "error", None):
                reason = getattr(extracted, "error", None) or "пустой ExtractedDoc"
                errors.append(f"{product_id}/{url}: extract: {reason}")
                _mark(
                    product_id,
                    url,
                    FetchStatus.FAILED,
                    error_kind=FailureKind.PARSER_FAILED.value,
                    error=reason,
                    raw_path=str(raw_path),
                )
                continue

            doc_hash = str(getattr(extracted, "content_hash", "") or "")
            if not doc_hash:
                errors.append(f"I19: {product_id}/{url}: ExtractedDoc без content_hash — отклонён")
                _mark(
                    product_id,
                    url,
                    FetchStatus.FAILED,
                    error_kind=FailureKind.PARSER_FAILED.value,
                    error="ExtractedDoc без content_hash",
                    raw_path=str(raw_path),
                )
                continue

            # 4. Дедуп по content_hash (в пределах продукта).
            if doc_hash in seen_docs:
                duplicate_count += 1
                # Байты сохранены и контент уже проиндексирован близнецом в этой партиции.
                _mark(product_id, url, FetchStatus.DONE, raw_path=str(raw_path))
                continue
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
                continue

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

            # D3: документ успешно извлечён и нарезан → терминальный DONE.
            _mark(product_id, url, FetchStatus.DONE, raw_path=str(raw_path))

        # Retention-провенанс не зависит от индексной сборки: docs персистятся в Pass A.
        _persist_docs(store, ingest.docs, errors)
        ingests.append(ingest)

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

    # D3: счётчики durable-краула (наблюдаемость). Статус per-URL — финальный исход попытки.
    crawl_counts = {status.value: 0 for status in FetchStatus}
    for status in crawl_status.values():
        crawl_counts[status.value] = crawl_counts.get(status.value, 0) + 1

    build = IndexBuild(
        index_build_id=index_build_id,
        entries=entries,
        docs=all_docs,
        chunk_count=chunk_count,
        crawl_pending=crawl_counts[FetchStatus.PENDING.value],
        crawl_fetched=crawl_counts[FetchStatus.FETCHED.value],
        crawl_done=crawl_counts[FetchStatus.DONE.value],
        crawl_failed=crawl_counts[FetchStatus.FAILED.value],
    )
    # Примечание: индексная сборка «возвращается» в log_entries (PhaseResult — контракт фазы;
    # W8 читает IndexBuild отсюда). Счётчики — в полях IndexBuild; дубликаты — в errors.
    return PhaseResult(status="ok", phase="corpus", cells=[], log_entries=[build], errors=errors)
