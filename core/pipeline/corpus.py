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

**Иммутабельность.** ``index_build_id`` = ``short_hash`` от (пакет/params + ``chunker_version`` +
``embed_model_id`` + ``snapshot_date``). Любое изменение входов даёт новую сборку; старая не
мутируется (§B.1/§13, K2).

**Внедрение зависимостей.** Адаптеры (``core/adapters/*``) здесь не импортируются: фаза
работает только через порты (A1). ``embedder``/``index_builder`` опциональны — если они не
переданы, фаза строит корпус и ``index_manifest``, а эмбеддинг/физическую сборку индекса
подключает W8 (composition root). При их наличии фаза вызывает ``embed``/``build``/``commit``
best-effort (сбой изолируется).

Нейтрален (§1.3): нет названий продуктов/вендоров/предметных терминов.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.domain import CorpusChunk, CorpusDoc, IndexManifestEntry
from core.pipeline.runner import PhaseResult
from core.ports.chunker import ChunkerPort
from core.ports.crawler import CrawlerPort
from core.ports.embedder import EmbedderPort
from core.ports.extractor import ExtractorPort
from core.ports.index_builder import IndexBuilderPort, IndexMeta
from core.ports.types import Chunk as PortChunk
from core.ports.types import RenderMode
from core.util.hashing import content_hash, short_hash

__all__ = ["IndexBuild", "build_corpus"]

#: Разделитель полей material-строки ``index_build_id`` (unit separator — однозначная склейка).
_FIELD_SEP = "\x1f"


@dataclass
class IndexBuild:
    """Результат сборки корпуса (Ш5): сборка + продукт-партиции + retention-документы.

    :param index_build_id: детерминированный идентификатор иммутабельной сборки (§B.1/§13).
    :param entries: по одной ``IndexManifestEntry`` на продукт-партицию (I17).
    :param docs: уникальные ``CorpusDoc`` (retention + provenance экстракции, §3.2).
    :param chunk_count: суммарное число проиндексированных чанков.
    """

    index_build_id: str
    entries: list[IndexManifestEntry] = field(default_factory=list)
    docs: list[CorpusDoc] = field(default_factory=list)
    chunk_count: int = 0


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
    package_id: str,
    params_token: str,
    chunker_version: str,
    embed_model_id: str,
    snapshot_date: str | None,
) -> str:
    """Детерминированный ``index_build_id`` (без random/UUID, §B.1/§13).

    Точные входы (склеены ``\\x1f``, порядок фиксирован):

    1. ``package_id`` — пакет корпуса (производный от ``plan`` либо явный);
    2. ``params_token`` — отпечаток выбранного каталога (``param_id``);
    3. ``chunker_version``;
    4. ``embed_model_id`` (пусто, если эмбеддер не подключён — W8);
    5. ``snapshot_date``.
    """
    material = _FIELD_SEP.join(
        (package_id, params_token, chunker_version, embed_model_id, snapshot_date or "")
    )
    return "ib_" + short_hash(material, 24)


def _chunk_id(product_id: str, doc_hash: str, chunk_no: int) -> str:
    """Детерминированный глобально-уникальный ``chunk_id``.

    ``document_id = f"{product_id}:{doc_hash}"`` уже уникален, поэтому
    ``f"{product_id}:{doc_hash}:{chunk_no}"`` уникален глобально и стабилен. Идентификатор
    строится ядром (а не берётся у чанкера), чтобы гарантировать уникальность независимо
    от адаптера и воспроизводимость ``source_id``-резолва.
    """
    return f"{product_id}:{doc_hash}:{chunk_no}"


def _render_for(render_modes: Mapping[str, RenderMode] | None, url: str) -> RenderMode:
    """Режим рендера для URL (по умолчанию HTTP; profile — ответственность адаптера)."""
    if render_modes:
        value = render_modes.get(url)
        if value is not None:
            return value
    return RenderMode.HTTP


# --------------------------------------------------------------------------- #
# Персистенция (изолированная: сбой store не роняет фазу)
# --------------------------------------------------------------------------- #


def _persist_corpus(
    store: Any,
    docs: list[CorpusDoc],
    chunks: list[CorpusChunk],
    entry: IndexManifestEntry | None,
    errors: list[str],
) -> None:
    """Пишет документы → чанки → запись манифеста (порядок важен: FK ``doc_id``).

    Каждая запись обёрнута в try/except: сбой персистенции фиксируется в ``errors`` и не
    прерывает обработку остальных продуктов (контракт изоляции фаз, ``runner.py``).
    """
    if store is None:
        return
    for doc in docs:
        try:
            store.upsert_corpus_doc(doc)
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(f"upsert_corpus_doc({doc.doc_id}): {type(exc).__name__}: {exc}")
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
) -> None:
    """Best-effort эмбеддинг и физическая сборка индекса продукт-партиции (K3, §B.1).

    ``pipeline`` не работает с файлами индекса напрямую: за ``.npy``-шарды/tantivy отвечает
    ``IndexBuilderPort``. Сбой изолируется в ``errors``.
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
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        errors.append(f"{product_id}: index build: {type(exc).__name__}: {exc}")


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
    render_modes: Mapping[str, RenderMode] | None = None,
    package_id: str | None = None,
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
    :param render_modes: необязательный ``{url: RenderMode}``; по умолчанию HTTP.
    :param package_id: явный пакет корпуса; ``None`` → производный от ``plan``.

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
    index_build_id = _derive_index_build_id(
        package,
        _params_token(plan),
        resolved_chunker_version,
        embed_model_id,
        snapshot,
    )

    errors: list[str] = []
    all_docs: list[CorpusDoc] = []
    entries: list[IndexManifestEntry] = []
    chunk_count = 0
    duplicate_count = 0
    raw_root_path = Path(raw_root)

    for product_id in products:
        product_docs: list[CorpusDoc] = []
        product_chunks: list[CorpusChunk] = []
        # I17: дедуп в пределах продукт-партиции (см. docstring модуля).
        seen_docs: dict[str, str] = {}
        seen_chunk_ids: set[str] = set()

        for url in url_map.get(product_id, []):
            render = _render_for(render_modes, url)

            # 1. Краул.
            try:
                page = crawler.fetch(url, render)
            except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
                errors.append(f"{product_id}/{url}: crawl: {type(exc).__name__}: {exc}")
                continue
            if page is None:
                errors.append(f"{product_id}/{url}: crawl: пустой RawPage")
                continue

            # 2. Retention (I19): сырые байты пишутся ДО экстракции.
            raw_hash = content_hash(page.content)
            try:
                raw_path = raw_root_path / product_id / f"{raw_hash}.bin"
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_bytes(page.content)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{product_id}/{url}: retention: {type(exc).__name__}: {exc}")
                continue

            # 3. Экстракция (грациозная деградация адаптера через .error).
            try:
                extracted = extractor.extract(page)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{product_id}/{url}: extract: {type(exc).__name__}: {exc}")
                continue
            if extracted is None or getattr(extracted, "error", None):
                reason = getattr(extracted, "error", None) or "пустой ExtractedDoc"
                errors.append(f"{product_id}/{url}: extract: {reason}")
                continue

            doc_hash = str(getattr(extracted, "content_hash", "") or "")
            if not doc_hash:
                errors.append(f"I19: {product_id}/{url}: ExtractedDoc без content_hash — отклонён")
                continue

            # 4. Дедуп по content_hash (в пределах продукта).
            if doc_hash in seen_docs:
                duplicate_count += 1
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
                render_mode=_as_text(page.render_mode),
                lang=page.lang,
            )
            product_docs.append(doc)

            # 5. Чанкер.
            try:
                chunks = chunker.chunk(extracted, document_id)
            except Exception as exc:  # noqa: BLE001
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

                product_chunks.append(
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
                        build_id=index_build_id,
                        embed_model_id=embed_model_id or None,
                        embedded_at=None,
                    )
                )

        entry: IndexManifestEntry | None = None
        if product_chunks:
            chunk_count += len(product_chunks)
            entry = IndexManifestEntry(
                index_build_id=index_build_id,
                product_id=product_id,
                package_id=package,
                partition_key=f"{index_build_id}:{product_id}",
                snapshot_date=snapshot,
                chunker_version=resolved_chunker_version or None,
                embed_model_id=embed_model_id or None,
                embed_dim=getattr(embedder, "dim", None),
                lexical_engine_version=getattr(index_builder, "lexical_engine_version", None),
                vector_store=getattr(index_builder, "vector_store", None),
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
            if embedder is not None and index_builder is not None:
                _embed_and_build(
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

        all_docs.extend(product_docs)
        _persist_corpus(store, product_docs, product_chunks, entry, errors)

    if entries and embedder is not None and index_builder is not None:
        try:
            index_builder.commit()
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            errors.append(f"index commit: {type(exc).__name__}: {exc}")

    build = IndexBuild(
        index_build_id=index_build_id,
        entries=entries,
        docs=all_docs,
        chunk_count=chunk_count,
    )
    # Примечание: индексная сборка «возвращается» в log_entries (PhaseResult — контракт фазы;
    # W8 читает IndexBuild отсюда). Счётчики — в полях IndexBuild; дубликаты — в errors.
    return PhaseResult(status="ok", phase="corpus", cells=[], log_entries=[build], errors=errors)
