"""Потребитель очереди: документ из ``extract_queue`` → корпус (17 §6, F5).

**Что здесь.** Единственная функция конверсии :func:`consume`. Краул больше не конвертирует:
он доводит frontier и пишет durable-факт «документ добыт» (:mod:`core.pipeline.extract_queue`),
а эта функция читает очередь продукта **синхронно до конца продукта** и делает всё, что
раньше делал inline-путь: зонд терминального артефакта, спуск в контейнер, переиспользование
версии (``304``), извлечение (пул ‖ inline), нарезку, дедуп и запись документов.

**Почему единица работы — ``(session, product)``, а не документ.** Пассы разделены по
владельцу: Pass A (краул + конверсия) наполняет продуктовый накопитель, Pass B
(эмбеддинг + сборка партиции) исполняется на границе продукта в ``corpus``. Потребитель,
работающий на документ, не умеет «закончить продукт», а два потребителя одного продукта не
соберут одну партицию (``pb`` продукт-скоупед, I17).

**Чего здесь нет и почему.** Здесь нет frontier, сеансовых меток и BFS: расширение ссылок и
``note_fetched`` — работа fetch-коммита краула. Это не косметика: ``note_fetched`` в
коммите конверсии означал бы ``truncated=false`` при реальном усечении, то есть ложный
``complete`` и ложный ``❌`` (I2/I2b).

**Провенанс берётся из строки, а не выводится заново.** :class:`_ExtractRequest`
собирается из полей строки (``raw_path``/``url``/``fetched_at``/``fetch_mode``/``lang``/
``status_code``/``content_type``/``engine``/``extract_route``/``level``/``pdf_pipeline``) плюс
двух фазовых величин, которых в строке нет по природе (корень/бюджет кэша извлечения и
каталог стока пика памяти воркеров). Вывести маршрут из конфига значило бы получить другой
``extractor_version``/``doc_version_id`` — то есть другой корпус при тех же объявленных
входах (I19, A3).

``reuse_kind`` (16 §3.3) тоже выводится из строки, и это **точно**, а не приблизительно:
строка создаётся первым наблюдением URL в сеансе (``enqueue_extract_item``:
``ON CONFLICT DO NOTHING``), а первое наблюдение не может быть resume'ом — resume требует
уже добытых retained-байтов того же сеанса, у которых строка уже есть. Поэтому ``304`` —
``revalidated`` (байты из кэша, сервер подтвердил неизменность), иначе ``live``.

**Тело документа не поднимается в родителе.** ``RawPage`` несёт ``source_path`` и пустой
``content`` — ровно то, что получает воркер пула
(:func:`core.pipeline.extraction_runtime._run_extract`). Один носитель тела на оба пути
(пул и inline) — иначе «пул ‖ inline» были бы двумя разными контрактами извлечения.
"""

from __future__ import annotations

import contextlib
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, wait
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field, replace
from functools import partial
from pathlib import Path
from typing import Any

from core.domain import (
    ArchiveDescentStatus,
    ArchiveObservation,
    CorpusChunk,
    CorpusDoc,
    CorpusMember,
    CrawlItemStatus,
    DocumentLevel,
    ExtractQueueItem,
    ExtractQueueState,
    FailureKind,
    PdfPipeline,
    ReuseKind,
    WorkClass,
)
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
from core.pipeline.corpus_index import _chunk_id, _CorpusStageTimings
from core.pipeline.corpus_ingest import (
    MEMBER_CONTENT_TYPES,
    ProductIngest,
    _partial_fields,
    _state_url_key,
    artifact_is_terminal,
    new_extract_pool,
    note_budget_accounting,
)
from core.pipeline.corpus_persist import DocReuse, _now_iso
from core.pipeline.corpus_state import CorpusCounters
from core.pipeline.crawl_frontier import CrawlLimits
from core.pipeline.extract_cache import ExtractionCache
from core.pipeline.extract_queue import (
    EXTRACT_CLAIM_LEASE_S,
    EXTRACT_HEARTBEAT_S,
    EXTRACT_MAX_ATTEMPTS,
    TERMINAL_QUEUE_STATES,
    ExtractQueue,
)
from core.pipeline.extraction_runtime import (
    _EXTRACT_DEAD_POOL_TIMEOUTS,
    _EXTRACT_MAX_POOL_REBUILDS,
    ExtractionPool,
    _as_text,
    _cache_extractor_version,
    _extract_cache_key,
    _ExtractPoolKey,
    _ExtractPools,
    _ExtractRequest,
)
from core.pipeline.incremental import doc_version_id
from core.pipeline.resources import (
    BUDGET_ACCOUNTED,
    BUDGET_UNACCOUNTED,
    PARENT_FLOOR_MB,
    work_class_peak_mb,
    workers_for_class,
)
from core.pipeline.work_class import work_class_for
from core.ports.chunker import ChunkerPort
from core.ports.container import ContainerPort
from core.ports.extractor import ExtractorPort
from core.ports.state import ContainerStateRow, ProductStatePort
from core.ports.types import ExtractionRoute, FetchMode, FetchPlan, RawPage
from core.util.document_formats import DOCUMENT_SUFFIXES
from core.util.hashing import content_hash

#: Сентинел «извлечения ещё не было» для члена контейнера: отличает «не извлекали» от
#: «извлечение вернуло ``None``». Второй сентинел означал бы второй путь.
_INLINE = object()

#: Маршрут извлечения члена по суффиксу: документы — ``document``, разметка — ``auto``
#: (``auto`` сам роутит HTML/markdown; ``html`` намеренно не форсируется — A1).
_DOCUMENT_SUFFIX_SET: frozenset[str] = frozenset(DOCUMENT_SUFFIXES)


def _member_route(suffix: str) -> ExtractionRoute:
    """Маршрут извлечения члена по **его собственному** суффиксу (не по суффиксу архива)."""
    return (
        ExtractionRoute.DOCUMENT
        if suffix.lower() in _DOCUMENT_SUFFIX_SET
        else ExtractionRoute.AUTO
    )


def persist_observation_sync(
    store: Any,
    errors: list[str],
) -> Callable[[ArchiveObservation], None] | None:
    """Write-through наблюдения спуска: синк для ``ArchiveDescentWalker`` (F1).

    Возвращает ``None``, если стора нет (прежнее поведение спуска: только список фазы).
    Наблюдение пишется **сразу** за ``_record``, поэтому SIGKILL/OOM посреди контейнера
    теряет максимум контейнер «в полёте», а не весь спуск. Идемпотентность уже есть —
    ``observation_id`` детерминирован, повторный спуск перекрывает строку.

    Отказ стора изолируется в ``errors``: падение конверсии хуже потерянной строки
    провенанса, но тихая потеря недопустима.
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


def persist_member_sync(
    store: Any,
    errors: list[str],
) -> Callable[[CorpusMember], None] | None:
    """Write-through строки ``corpus_members``: синк для ``ArchiveDescentWalker`` (F1).

    Строка пишется **после** доводки члена, когда ``doc_id``/``extracted_content_hash``
    известны или пропуск назван. Идентичность — ``(archive_content_hash, member_path,
    product_id)``, поэтому повторный спуск делает UPSERT, а не дубль.
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


def write_container_state(
    product_id: str,
    url: str,
    raw_path: Path,
    observation: ArchiveObservation,
    *,
    archive_format: str | None,
    state: ProductStatePort | None,
    errors: list[str],
    run_id: str | None,
) -> None:
    """Durable-наблюдение контейнера в ``state.db`` (план 19 §E.7, STATE 10).

    Зачем: контейнер не имеет версии документа, поэтому путь и хеш его ретейнённых байтов
    прежде жили только в БД прогона (``archive_observations``/``corpus_members``). Проход
    без сети читает лишь ``state.db`` и воспроизводил верхнеуровневые документы, **молча**
    теряя членов (замер стенда: ``auto`` 9 документов/597 чанков, ``frozen`` 7/326).

    Пишется **нормализованный** URL (ключ, по которому ``frozen`` ищет строку) и хеш
    ретейнённых байтов — ``raw_path.stem``: retention называет файл ``{sha256}.bin``,
    поэтому имя и есть идентичность байтов (I19), а не хеш извлечённого текста.

    Отказ записи изолируется в ``errors``: спуск уже состоялся, и его провенанс в списках
    фазы не должен пропасть из-за недоступного состояния. ``state is None`` — прогон без
    durable-состояния (прежнее поведение, ничего не пишется).
    """
    if state is None:
        return
    key = _state_url_key(url)
    if key is None:
        errors.append(
            f"{product_id}/{url}: state: URL не нормализуется — container_state не записан"
        )
        return
    now = _now_iso()
    seen_run = run_id or ""
    try:
        state.record_container_state(
            ContainerStateRow(
                product_id=product_id,
                url=url,
                normalized_url=key,
                archive_content_hash=raw_path.stem,
                # Абсолютный путь: проход без сети может идти из другого CWD и в другом
                # процессе — относительный путь retention там не разрешился бы.
                raw_path=str(Path(raw_path).resolve()),
                format=archive_format,
                status=observation.status.value,
                first_seen_run_id=seen_run,
                last_seen_run_id=seen_run,
                first_seen_at=now,
                last_seen_at=now,
            )
        )
    except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
        errors.append(
            f"{product_id}/{url}: state: record_container_state: "
            f"{type(exc).__name__}: {exc}"
        )


def member_extract_request(
    task: MemberTask,
    *,
    fetched_at: str,
    lang: str | None,
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline = PdfPipeline.STRUCTURED,
    rss_dir: str | None = None,
    cache_root: str | None = None,
    cache_max_bytes: int | None = None,
) -> _ExtractRequest:
    """B3/P4: payload экстракции члена — только примитивы, байты не передаются (I19).

    Источник байтов — **путь** к временному файлу члена (воркер читает его сам), маршрут —
    по суффиксу члена, ``content_type`` — по суффиксу (данные, не эвристика).

    **Ключ кэша — явный** (S2/план 23): ``cache_key=task.content_hash`` — хеш **байтов
    члена**, посчитанный спуском в том же потоке, которым байты писались в temp-файл.
    Выводимая форма (``stem`` файла) для члена неверна по построению: ``member-N.bin`` —
    номер, одинаковый по смыслу для всех членов архива, поэтому все они делили бы одну
    запись кэша и получали бы друг чужой текст. Имя temp-файла в ключ не входит и после
    этого: повторные байты находят одну запись, даже если пришли из разных архивов и
    названы разными номерами.
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
        cache_root=cache_root,
        cache_max_bytes=cache_max_bytes,
        cache_key=task.content_hash,
        rss_dir=rss_dir,
    )


def member_extract_unavailable(extractor: ExtractorPort) -> MemberOutcome | None:
    """``split``: экстрактор шва работает по URL — локальные байты он не принимает.

    Капитуляция явная, а не тихая: член контейнера не извлечён, причина видна в
    ``corpus_members.skip_reason``/``errors``. Проверка живёт **в родителе** (у воркера
    пула нет этого признака — он получает уже готовый payload).
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


def ingest_member_doc(
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
    extract_cache: ExtractionCache | None = None,
    timings: _CorpusStageTimings | None = None,
    reuse_kind: ReuseKind,
    persist_docs: Callable[..., None],
) -> MemberOutcome:
    """Член контейнера → документ корпуса (``CorpusDoc`` + чанки) или видимый пропуск.

    **Байты члена не ретейнятся** (§6.7): ``task.temp_path`` — временный файл, который
    спуск удаляет сразу после возврата. ``CorpusDoc.raw_path`` поэтому указывает на
    **архив** (сырьё I19), а не на член.

    **Маршрут** — по суффиксу самого члена (pdf → ``document``, md/html → ``auto``).

    **Кэш (S2/план 23).** Ключ — хеш **байтов члена** (``task.content_hash``, посчитан
    спуском), а не номер временного файла: одинаковые байты в разных архивах делят одну
    запись, разные члены одного архива — никогда. ``extract_cache=None`` — прежнее
    поведение (кэш не спрашивается).

    **``split``.** Экстрактор шва (``RemoteExtractor``) работает по URL и локальные байты
    не принимает; такой член не извлекается молча, а возвращает видимый пропуск
    ``extract_unavailable`` с причиной.

    Дедуп — по ``content_hash`` в пределах продукта (I17), как у страниц.

    ``extracted`` — результат извлечения, если он пришёл из пула (``_INLINE`` ⇒
    извлекаем здесь). Родительская часть (зонд, документ, нарезка, дедуп, строки) одна и
    та же для обоих путей — поэтому «пул vs inline» не может разойтись в артефактах.
    """
    unavailable = member_extract_unavailable(extractor)
    if unavailable is not None:
        return unavailable
    suffix = task.suffix.lower()
    # Зонд члена — та же классификация, что у артефакта верхнего уровня: «сколько страниц
    # в PDF-члене» видно без извлечения.
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
    route = _member_route(suffix)
    #: S2/план 23: ключ кэша члена — хеш **его байтов**, посчитанный спуском в том же
    #: потоке, которым байты писались в temp-файл (I19). Номер ``member-N.bin`` в ключ не
    #: входит: иначе все члены архива делили бы одну запись.
    cache_key = task.content_hash or None
    if extracted is _INLINE:
        # Inline-путь (нет пула): извлечение здесь, на главном потоке. Кэш спрашивается и
        # здесь: проход без сети (``frozen``) исполняет членов inline, и повторные байты
        # не должны конвертироваться второй раз только из-за отсутствия пула.
        cached = None
        if extract_cache is not None and cache_key:
            # Версия движка — фактического для этого маршрута: иначе проба не совпадёт с
            # записью и кэш останется write-only (та же причина, что у страниц).
            cached = extract_cache.get(
                cache_key,
                _cache_extractor_version(extractor, page, route),
                _as_text(level),
            )
        if cached is not None:
            extracted = cached
        else:
            try:
                extracted = extractor.extract(page, route)
            except Exception as exc:  # noqa: BLE001 — отказ члена изолируется (контракт фазы)
                return MemberOutcome(
                    skip_reason=SKIP_EXTRACT_FAILED,
                    detail=f"{type(exc).__name__}: {exc}",
                )
            if extract_cache is not None and cache_key:
                # ``put`` сам не сохраняет сбой/пустоту/частичность — здесь только ключ.
                extract_cache.put(
                    extracted, level=_as_text(level), content_hash=cache_key
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
        # создавалось, а признак принадлежит строке близнеца (``corpus_docs.partial``).
        return MemberOutcome(doc_id=document_id, content_hash=doc_hash)
    seen_docs[doc_hash] = task.member_url

    # 16 §6: адрес версии документа — та же формула, что у страниц.
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
        # 16 §3.3: происхождение наблюдения члена — происхождение **контейнера**.
        reuse_kind=reuse_kind,
        # Член — тот же путь извлечения, что и страница: признак частичности обязан
        # пережить его так же (иначе «прочитан 1 член из 47» молчит внутри архива).
        **_partial_fields(extracted),
    )
    ingest.docs.append(doc)
    persist_docs(store, [doc], errors, versions_by_doc={document_id: member_dv_id})

    # Тот же признак — в исходе члена: ``MemberOutcome.partial`` делает его машинным.
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
                # L1b-2b: провенанс куска — смещения в нормализованном тексте документа.
                # Переносятся как есть; ``None`` у абзацного чанкера — объявленная граница,
                # а не потеря: ``CorpusChunk`` принимает либо пару, либо ``None``/``None``.
                char_start=getattr(chunk, "char_start", None),
                char_end=getattr(chunk, "char_end", None),
            )
        )
    return MemberOutcome(
        doc_id=document_id,
        content_hash=doc_hash,
        partial=member_partial,
        partial_reason=member_partial_reason,
    )


# --------------------------------------------------------------------------- #
# Носители: фазовый скоуп потребителя и звенья продуктов
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ConsumerScope:
    """Фазовое состояние потребителя: порты, оси, каналы видимости.

    Один объект на фазу (как :class:`core.pipeline.corpus_drain.PhaseDrainScope` на стороне
    краула). Поля — ссылки на мутируемые контейнеры (``errors``, накопители наблюдений) и
    неизменяемые оси/порты: ``frozen`` запрещает подменить поле, но не запрещает писать в
    содержимое — ровно то разделение, которое нужно.

    **Чего здесь нет.** Здесь нет frontier, сеансовых меток и очереди: первое — краул,
    второе — владение :class:`ExtractQueue` (передаётся аргументом функции, чтобы
    потребитель нельзя было случайно направить в чужой сеанс).
    """

    #: Канал видимости фазы: каждая деградация обязана быть названа здесь.
    errors: list[str]
    #: Склад фазы.
    store: Any
    #: Идентификатор прогона для durable-записей провенанса (``None`` — прогона нет).
    run_id: str | None
    #: Чанкер фазы (версия входит в ``pb``).
    chunker: ChunkerPort
    #: Читатель контейнеров фазы (``None`` — бэкенда нет, спуск не открывается).
    container_for: Callable[[str | Path], ContainerPort | None] | None
    #: Экстрактор по умолчанию (композит ``full`` + ``structured``).
    extractor: ExtractorPort
    #: Фабрика экстрактора под уровень/механизм строки (``None`` — только переданный).
    extractor_factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort] | None
    #: Фазовый пул экстракции (``None`` — экстракция inline).
    extraction_pool: ExtractionPool | None
    #: Межпрогонный кэш извлечённого текста (ключ — хеш ретенированных байтов).
    extract_cache: ExtractionCache | None
    #: Реестр фазовых пулов экстракции (ключ — уровень + механизм + окно).
    pools: _ExtractPools
    #: Потолок одного ожидания пула, секунды (``deep``-таймаут).
    extract_timeout: float
    #: Каталог стока пика памяти воркеров (``None`` — пик не собирается).
    worker_rss_dir: str | None
    #: Накопители фазы (наблюдённые объёмы, а не лимиты).
    counters: CorpusCounters
    #: Бакеты стадий продукта: ожидание экстракции, нарезка.
    timings: _CorpusStageTimings
    #: B3: пробы терминальных артефактов (класс/формат/размер/``page_count``).
    artifact_probes: list[dict[str, Any]]
    #: B3: наблюдения спуска в контейнеры (в том числе пустые — молчания нет).
    archive_observations: list[ArchiveObservation]
    #: B3: члены контейнеров, дошедшие до корпуса.
    corpus_member_rows: list[CorpusMember]
    #: Оси краула по URL (``app < run < site``): окно экстракции, бюджеты спуска,
    #: ``attachment_formats``. Та же функция, что у корня, — второго источника нет.
    limits_for: Callable[[str], CrawlLimits]
    #: Уже названные диагностики окна (ключи — причина, не URL): ось, заданная без пула
    #: или с неподходящим пулом, — факт **фазы**, а не каждого документа. Без дедупа
    #: одна и та же строка повторилась бы на каждой строке очереди, и ``errors`` перестал
    #: бы читаться (тот же принцип, что у ``sorted_error_lines``: назвать, а не залить).
    window_notes: set[str] = field(default_factory=set)
    #: F6: кто мы для аренды строки. Один потребитель — один идентификатор; он пишется в
    #: ``claimed_by`` и по нему же проверяется, что аренда ещё наша. ``None`` — выводится
    #: из процесса/потока (два потребителя в одном процессе обязаны различаться).
    worker_id: str | None = None
    #: F6: порог аренды (секунды) и период heartbeat. Инъецируемы, чтобы тест просрочки и
    #: heartbeat не платил реальным временем; дефолты — опубликованные константы.
    claim_lease_s: float = EXTRACT_CLAIM_LEASE_S
    heartbeat_s: float = EXTRACT_HEARTBEAT_S
    #: F6: сколько аренд строка переживает до терминального ``failed`` (§4.2 п.5).
    max_claim_attempts: int = EXTRACT_MAX_ATTEMPTS
    #: F6: корень retention — для публикуемого ``free_bytes`` рядом с ``backlog_bytes``
    #: (§4.3). ``None`` — носитель неизвестен, и число не выдумывается. Предел здесь не
    #: хранится: он объявленная ось и берётся из тех же ``CrawlLimits``, что у краула
    #: (:func:`_backlog_limit_mb`) — второго носителя одного значения нет.
    raw_root: str | None = None
    #: Durable-состояние продукта (``state.db``): write-through наблюдения контейнера
    #: (план 19 §E.7). ``None`` — прогон без состояния: провенанс остаётся только в списках
    #: фазы, а проход без сети контейнер не воспроизведёт (видимая причина у него есть).
    state: ProductStatePort | None = None
    #: F0: бюджет памяти прогона, MB — **явный вход** composition root
    #: (:func:`core.pipeline.resources.resolve_mem_budget_mb`), а не чтение окружения:
    #: чистый пайплайн не имеет права зависеть от памяти хоста. По нему выводится окно
    #: **каждого класса работы** (:func:`_row_window`); ``None`` — бюджета нет, окна классов
    #: не выводятся и причина публикуется. В ``run_id`` не входит: расписание — не условие
    #: измерения (иначе ``run_id`` стал бы функцией хоста).
    mem_budget_mb: int | None = None


@dataclass(frozen=True)
class ConsumerOperations:
    """Замкнутые операции фазы: **чем** потребитель действует.

    Все поля — callable'ы, связанные один раз в ``corpus._build_corpus``. Их сигнатуры
    держат ``corpus.py`` и тесты (в том числе подменой атрибутов модуля ``corpus``),
    поэтому носитель их не переписывает: он называет владельца, а не форму вызова.
    """

    #: Отметка элемента сеанса (``CrawlItemStatus`` + класс сбоя).
    mark: Callable[..., None]
    #: Переиспользование версии документа (``304``): чанки без экстракции.
    reuse_for_page: Callable[..., DocReuse | None]
    #: Ингест одного документа (extract → chunk → запись), inline-путь.
    ingest_one: Callable[..., str | None]
    #: Фиксация готового извлечения (пул/кэш/переиспользование) — без экстракции.
    finish_one: Callable[..., str | None]
    #: Ингест члена контейнера (спуск B3), адрес записи — продукт.
    ingest_member_doc: Callable[..., MemberOutcome]
    #: Durable-персистенция документов продукта (инкрементально, I19).
    persist_docs: Callable[..., None]
    #: Наблюдение расписания экстракции (окно → строка продукта/метрики). Ключевые
    #: аргументы ``work_class``/``window`` обязательны: одно число больше не описывает
    #: расписание — у каждой полосы своё окно, и наблюдаемость обязана это назвать.
    note_scheduling: Callable[..., None]


@dataclass(frozen=True)
class ProductScope:
    """Продуктовый скоуп потребителя: накопитель и дедуп-множества продукта.

    Корни продукта делят это состояние с потребителем, поэтому объект **один**: копия
    вернула бы тихий дефект «корень обновил копию, сосед не увидел». Поля — ссылки.
    """

    #: Идентификатор продукта.
    pid: str
    #: Накопитель продукта (документы/чанки/счётчики ингеста).
    ingest: ProductIngest
    #: I17: ``content_hash → url`` в пределах продукт-партиции.
    seen_docs: dict[str, str]
    #: I17: ``chunk_id`` в пределах продукт-партиции.
    seen_chunk_ids: set[str]


@dataclass
class ConsumerReport:
    """Исход конверсии одного продукта за вызов потребителя.

    ``pending`` — строки, оставшиеся незакрытыми (``pending``/``claimed``). Именно этим
    числом фаза отвечает на вопрос «корпус продукта собран?»: ``0`` — да, ``>0`` — нет, и
    тогда фаза **не** возвращает ``ok`` (иначе ``evaluate`` пошёл бы по неполному корпусу,
    а гейтовый ``subject_digest`` был бы снят с незрелого предмета).
    """

    product_id: str
    #: Строк, доведённых до ``done`` (документ или видимый пропуск).
    done: int = 0
    #: Строк, закрытых как ``failed`` (конверсия отказала, класс в ``error_kind``).
    failed: int = 0
    #: Строк, которые оказались уже терминальными (повторный вызов — no-op).
    already: int = 0
    #: Строк, не перешедших состояние (забрал другой потребитель/закрыли).
    lost: int = 0
    #: F6: строк, взятых в аренду этим потребителем.
    claimed: int = 0
    #: F6: просроченных аренд, возвращённых в ``pending`` перед проходом (§4.2 п.3).
    reclaimed: int = 0
    #: F6: сколько раз продлевалась аренда (живой потребитель на долгом документе).
    heartbeats: int = 0
    #: F6: факты восстановления аренд — «кто держал, какой URL, сколько раз» (§4.2 п.3:
    #: молчаливое воскрешение запрещено).
    reclaim_records: list[dict[str, Any]] = field(default_factory=list)
    #: F6/§4.3: наблюдённый backlog в конце прохода (числа, не оценка).
    backlog: dict[str, Any] = field(default_factory=dict)
    #: Строк, оставшихся незакрытыми после вызова.
    pending: int = 0
    #: Фактическое окно экстракции продукта — **максимум по классам** (``None`` — все
    #: строки шли inline). Поле сохранено ради совместимости отчёта; разбивка — рядом.
    extract_window: int | None = None
    #: Окно по классам работы (``{work_class: window}``) — фактическая ширина каждой полосы.
    #: Класс, которого нет в словаре, либо не встречался, либо шёл inline (окна у него нет:
    #: «не измеряли» ≠ «ноль», та же конвенция, что у ``extract_window``).
    extract_window_by_class: dict[str, int] = field(default_factory=dict)
    #: F0: наблюдённый максимум суммы пиков заданий «в полёте», MB (``None`` — учёт не
    #: вёлся: бюджета нет). Сумма ограничена бюджетом **по построению**, поэтому пик,
    #: упёршийся в бюджет при непустой очереди, — это ответ «узел загружен».
    in_flight_mb_peak: int | None = None
    #: F0: сколько **стартов** отложено из-за бюджета, по классам (события, не строки).
    class_skipped_for_budget: dict[str, int] = field(default_factory=dict)
    #: F0: ``accounted`` | ``class_budget_unaccounted`` | ``None`` (учёт не применялся).
    budget_accounting: str | None = None
    #: Документов, дошедших до корпуса этим вызовом.
    docs: int = 0
    #: Отдельные факты прогона (не ошибки): напр. «строка уже закрыта».
    notes: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Строка очереди → задание экстракции / страница / план
# --------------------------------------------------------------------------- #


def _enum_or(enum_cls: Any, value: Any, default: Any) -> Any:
    """Значение enum из колонки строки очереди; ``default`` — если колонка пуста.

    Пустая колонка — не «нейтрально», а нарушение контракта очереди (S3): для
    ``pending``/``claimed`` валидатор модели требует непустых ``fetch_mode``/
    ``extract_route``. Здесь ``default`` — путь для строк прежнего производителя, которые
    потребитель всё равно не конвертирует.
    """
    if value is None or value == "":
        return default
    try:
        return enum_cls(value)
    except ValueError:
        return default


def _row_page(item: ExtractQueueItem, reuse_kind: ReuseKind) -> RawPage:
    """``RawPage`` из строки очереди: тело — по ``source_path``, а не в RAM родителя.

    ``content`` пуст, ``source_path`` — retained-файл (``corpus_raw/{hash}.bin``, I19).
    Это ровно тот носитель, который получает воркер пула: один контракт извлечения на
    оба пути (пул ‖ inline).

    ``not_modified`` — из происхождения наблюдения **этого** исполнения: только
    ``revalidated`` означает, что сервер в этом цикле подтвердил неизменность и
    переиспользование версии (``reuse_for_page``) вправе не звать экстрактор. Брать его
    из ``status_code`` строки нельзя: у строки, добытой в прошлом исполнении, там
    прежний ``200`` (а ``304`` — только если сервер ответил 304 именно тогда).
    """
    return RawPage(
        url=item.url,
        content=b"",
        source_path=item.raw_path,
        fetched_at=item.fetched_at or "",
        fetch_mode=_enum_or(FetchMode, item.fetch_mode, FetchMode.HTTP),
        lang=item.lang,
        status_code=item.status_code,
        content_type=item.content_type,
        engine=item.engine,
        not_modified=reuse_kind is ReuseKind.REVALIDATED,
    )


def _row_request(item: ExtractQueueItem, scope: ConsumerScope) -> _ExtractRequest:
    """``_ExtractRequest`` из строки очереди + фазовых величин, которых в строке нет.

    Поля строки (все — условия измерения документа): ``raw_path``/``url``/``fetched_at``/
    ``fetch_mode``/``lang``/``status_code``/``content_type``/``engine``/``extract_route``/
    ``level``/``pdf_pipeline``. Фазовые добавки: ``cache_root``/``cache_max_bytes`` (корень
    и бюджет межпрогонного кэша — свойство прогона, а не документа) и ``rss_dir`` (сток
    пика памяти воркера).
    """
    return _ExtractRequest(
        raw_path=item.raw_path,
        url=item.url,
        fetched_at=item.fetched_at or "",
        fetch_mode=_enum_or(FetchMode, item.fetch_mode, FetchMode.HTTP),
        lang=item.lang,
        status_code=item.status_code,
        content_type=item.content_type,
        engine=item.engine,
        route=_enum_or(ExtractionRoute, item.extract_route, ExtractionRoute.AUTO),
        level=_enum_or(DocumentLevel, item.level, DocumentLevel.FULL),
        pdf_pipeline=_enum_or(PdfPipeline, item.pdf_pipeline, PdfPipeline.STRUCTURED),
        cache_root=(
            str(scope.extract_cache.root) if scope.extract_cache is not None else None
        ),
        cache_max_bytes=(
            scope.extract_cache.max_bytes if scope.extract_cache is not None else None
        ),
        rss_dir=scope.worker_rss_dir,
    )


def _row_plan(item: ExtractQueueItem, limits: CrawlLimits) -> FetchPlan:
    """``FetchPlan`` из строки: маршрут извлечения и объявленные форматы артефактов.

    Нужен только конверсионной ветке (класс ответа и маршрут) — сети за ним нет.
    ``attachment_formats`` — ось корня: она решает, что считается контейнером, поэтому
    берётся из тех же ``CrawlLimits``, что у краула.
    """
    return FetchPlan(
        fetch=_enum_or(FetchMode, item.fetch_mode, FetchMode.HTTP),
        extract=_enum_or(ExtractionRoute, item.extract_route, ExtractionRoute.AUTO),
        attachment_formats=limits.attachment_formats,
    )


def _reuse_kind_for(item: ExtractQueueItem) -> ReuseKind:
    """Запасное правило происхождения, когда краул в этом исполнении URL не наблюдал.

    Строка, оставшаяся от прошлого исполнения (прерванный прогон), конвертируется в
    этом: происхождения **этого** цикла у неё нет (``put`` её не видел), поэтому годится
    только факт строки — ``304`` ⇒ ``revalidated``, иначе ``live``. Основной путь —
    :meth:`core.pipeline.extract_queue.ExtractQueue.cycle_reuse_kind`: краул знает точное
    происхождение и передаёт его в очередь.
    """
    return ReuseKind.REVALIDATED if item.status_code == 304 else ReuseKind.LIVE


# --------------------------------------------------------------------------- #
# Окно экстракции, пул, извлечение
# --------------------------------------------------------------------------- #


def _row_window(
    limits: CrawlLimits,
    scope: ConsumerScope,
    label: str,
    *,
    work_class: WorkClass,
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline,
) -> int | None:
    """Окно экстракции строки **внутри её класса работы**: ось > бюджет класса > inline.

    Три уровня, по убыванию силы входа:

    1. **объявленная** ось ``crawl.extract_workers`` (``> 1``) — воля оператора, сильнее
       бюджета: она задана явно и её цена ему известна. Выведенная ось
       (``extract_workers_derived``) сюда **не** входит: это не воля оператора, а прежняя
       единая оценка из бюджета по пику **PDF-уровня**
       (:func:`core.pipeline.crawl_frontier.resolve_crawl_limits` →
       :func:`core.pipeline.resources.workers_from_budget`). Принять её за окно класса
       значило бы вернуть ровно тот дефект, ради которого полосы разделены: HTML получал бы
       окно, посчитанное по цене docling (2 вместо 12 при бюджете 8000 MB);
    2. окно **класса**, выведенное из бюджета памяти
       (:func:`core.pipeline.resources.workers_for_class`): HTML-страница стоит 317 MB,
       PDF ``layout_tables`` — 3679 MB, и одно окно на оба класса либо морило HTML в
       очереди, либо обещало память, которой нет;
    3. ``None`` — экстракция inline (пула нет либо окно не объявлено и не выводится).

    **Рециклинг воркеров окно больше не задаёт.** До этого ``extract_max_tasks_per_child``
    принудительно возвращал окно 1 — склейка, унаследованная от inline-пути, где «один
    воркер» и «одно задание в полёте» были одним фактом. Это разные вещи: рециклинг — про
    память процесса (после N заданий воркер штатно завершается), окно — про число
    одновременных заданий. Склейка снята; рециклинг по-прежнему включает пул при окне 1
    (иначе ось ``extract_max_tasks_per_child`` перестала бы действовать вовсе).

    Ось и окно — провенанс, а не измерение: они не входят в ``CrawlLimits.as_dict()``,
    поэтому их значения не могут сдвинуть ``run_id``; публикуются они в
    ``metrics.json.resources.scheduling`` (:func:`core.pipeline.corpus_ingest.
    note_scheduling`) и ``extract_workers_by_class``.

    Несовпадение оси и пула — **не тихо** (:func:`_note_window_diagnostics`), как и
    неизмеренный пик класса или его невместимость: причина называется один раз на класс.
    """
    axis = (
        int(limits.extract_workers)
        if (
            limits.extract_workers is not None
            and int(limits.extract_workers) > 1
            and not limits.extract_workers_derived
        )
        else None
    )
    _note_window_diagnostics(limits, axis, scope, label)
    if axis is not None:
        return axis
    if limits.extract_workers_derived and scope.extraction_pool is not None:
        workers, reason = workers_for_class(
            work_class,
            level,
            pdf_pipeline,
            scope.mem_budget_mb,
            pdf_device=limits.pdf_device,
        )
        if reason is not None:
            # Пик класса не измерен либо бюджета нет: окно 1 и **видимая** причина.
            # Молчаливая единица выглядела бы как замер, а ноль оставил бы строки
            # класса без исхода вовсе.
            _note_window_once(
                scope,
                f"{reason}:{work_class.value}",
                f"{label}: класс {work_class.value}: {reason} — окно 1 "
                f"(бюджет {scope.mem_budget_mb} MB, пик класса не измерен: "
                "выдуманное число запрещено, A3)",
            )
            return 1
        if workers > 1:
            return workers
        if workers == 0:
            # Не помещается по бюджету: preflight отказывает такому прогону, но если он
            # всё же идёт (ось выведена в обход), причина обязана быть названной, а окно —
            # 1, как и сказано в сообщении: молчаливый ``None`` увёл бы класс в inline,
            # то есть в родителя, где тот же пик тратится без изоляции процесса.
            _note_window_once(
                scope,
                f"work_class_budget_zero:{work_class.value}",
                f"{label}: класс {work_class.value} не помещается в бюджет "
                f"{scope.mem_budget_mb} MB — окно 1",
            )
            return 1
    recycle = limits.extract_max_tasks_per_child
    if (
        recycle is not None
        and scope.extraction_pool is not None
        and scope.extraction_pool.supports_worker_recycling
    ):
        # Рециклинг включает пул (иначе ось не действовала бы), но окно остаётся 1:
        # он про память процесса, а не про число одновременных заданий.
        return 1
    return None


def _note_window_diagnostics(
    limits: CrawlLimits,
    axis: int | None,
    scope: ConsumerScope,
    label: str,
) -> None:
    """Назвать несовпадение объявленной оси экстракции и фазового пула (один раз на причину).

    Три случая, каждый — потеря объявленного измерения, если промолчать: ось задана без
    пула (экстракция пойдёт inline, то есть не с той параллельностью), рециклинг задан без
    пула, рециклинг задан на пуле, который его не умеет (``split`` — потоковый пул).
    """
    if axis is not None and scope.extraction_pool is None:
        _note_window_once(
            scope,
            "extract_workers_no_pool",
            f"{label}: crawl.extract_workers={axis} задан, но пул экстракции "
            "не сконфигурирован — экстракция inline",
        )
    recycle = limits.extract_max_tasks_per_child
    if recycle is None:
        return
    if scope.extraction_pool is None:
        _note_window_once(
            scope,
            "recycle_no_pool",
            f"{label}: crawl.extract_max_tasks_per_child={recycle} задан, но пул "
            "экстракции не сконфигурирован — экстракция inline",
        )
    elif not scope.extraction_pool.supports_worker_recycling:
        _note_window_once(
            scope,
            "recycle_not_supported",
            f"{label}: crawl.extract_max_tasks_per_child={recycle} поддерживается "
            "только процессным пулом (local) — ось игнорируется",
        )


def _note_window_once(scope: ConsumerScope, key: str, message: str) -> None:
    """Пишет диагностику окна один раз на причину (лейбл — первой строки)."""
    if key in scope.window_notes:
        return
    scope.window_notes.add(key)
    scope.errors.append(message)


def recover_extract_pool(
    pool_key: _ExtractPoolKey,
    label: str,
    *,
    pools: _ExtractPools,
    errors: list[str],
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


def _extractor_for(
    scope: ConsumerScope,
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline,
    *,
    cache: dict[tuple[DocumentLevel, PdfPipeline], ExtractorPort],
) -> ExtractorPort:
    """Экстрактор под уровень/механизм строки: ``full``+``structured`` — переданный.

    Иной уровень строится фабрикой (загрузка весов — часть конверсии); отказ фабрики
    виден в ``errors`` и не подменяет уровень молча (иначе ``extractor_version`` назвал бы
    движок, которого не было).
    """
    key = (level, pdf_pipeline)
    cached = cache.get(key)
    if cached is not None:
        return cached
    factory = scope.extractor_factory
    chosen = scope.extractor
    if factory is not None and (
        level is not DocumentLevel.FULL or pdf_pipeline is not PdfPipeline.STRUCTURED
    ):
        try:
            chosen = factory(level, pdf_pipeline)
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            scope.errors.append(
                f"extractor_factory({level.value}, {pdf_pipeline.value}): "
                f"{type(exc).__name__}: {exc} — понижен до переданного экстрактора"
            )
            chosen = scope.extractor
    cache[key] = chosen
    return chosen


@dataclass
class _RowJob:
    """Одна строка очереди в работе: всё, что нужно довести её до исхода.

    ``future is None`` ⇒ извлечение не отправлено в пул (inline-путь уже исполнен, либо
    пул отказал). ``done`` ⇒ результат уже записан (контейнер/переиспользование/inline).
    """

    item: ExtractQueueItem
    page: RawPage
    raw_path: Path
    fetch_plan: FetchPlan
    level: DocumentLevel
    pdf_pipeline: PdfPipeline
    extractor: ExtractorPort
    reuse_kind: ReuseKind
    #: Полоса расписания, в которой идёт строка (цена работы: пик RAM/VRAM, модели).
    work_class: WorkClass
    #: Запрошенный краулом адрес: ``mark``/``url_state`` адресуются им, тогда как
    #: ``item.url``/``page.url`` — финальный адрес документа (редирект).
    work_url: str = ""
    reuse: DocReuse | None = None
    pool_key: _ExtractPoolKey | None = None
    future: Any = None
    #: Готовый текст из ``extract_cache``: доводится **не** на старте строки, а в порядке
    #: очереди (:meth:`_BandRun.settle`). Хит кэша — запись корпуса, поэтому он не имеет
    #: права обгонять документы, которые очередь поставила раньше.
    cached: Any = None
    #: Строка уже закрыта: доведена (:meth:`_BandRun._settle`) либо снята как остаток
    #: мёртвой полосы (:func:`_abort_pending`). Флаг нужен потому, что снятая строка
    #: остаётся в глобальном порядке, и курсор доводки дошёл бы до неё повторно —
    #: второй исход по той же строке очереди был бы двойным счётом и второй записью.
    settled: bool = False
    #: Сколько MB этот слот держит в общем счётчике памяти (``_BandRun.in_flight_mb``).
    #: ``None`` — резерва нет: бюджет не объявлен, строка шла inline/из кэша либо это
    #: контейнер (спуск потоковый, слот пула не занят). Ненулевое значение — резерв
    #: возвращается **ровно один раз** (:meth:`_BandRun._release`), иначе бюджет «утечёт».
    peak_mb: int | None = None
    #: Класс с **неизмеренным** пиком держит не мегабайты, а право быть единственным:
    #: выдумать его пик нельзя (A3), поэтому он идёт один на узел (окно 1 уже это
    #: обещает, здесь — то же самое между классами).
    hold_single: bool = False
    #: Потолок задания (``started_at + extract_timeout``), секунды ``perf_counter``.
    #: Считается **на старте**, а не в момент ожидания: иначе зависший воркер, чьё
    #: завершение никто не ждёт (полоса занята другими), не истёк бы никогда.
    deadline: float | None = None
    error: str | None = None
    error_kind: FailureKind | None = None
    done: bool = False
    doc_id: str | None = None
    errors_before: int = 0
    #: F6: строка взята **этим** потребителем в аренду (``pending → claimed``). ``False`` —
    #: повтор терминальной строки (Pass B обязан получить документ заново): такая строка
    #: закрывается тем же состоянием, а не через claim.
    claimed: bool = False


def _conversion_failure(
    new_errors: Sequence[str], product_id: str, url: str
) -> str | None:
    """Причина отказа конверсии из свежих строк ``errors`` (``None`` — отказа не было).

    Зачем не сигнатура: ``ingest_one``/``finish_one`` — операции фазы (тесты подменяют их
    атрибутом модуля ``corpus``), и их контракт — «вернуть ``doc_id`` либо ``None``», а
    причина отказа уже записана в ``errors`` теми же двумя путями. Читать её оттуда честнее,
    чем завести второй канал, который с первым разошёлся бы.
    """
    prefix = f"{product_id}/{url}:"
    for line in reversed(list(new_errors)):
        if line.startswith(prefix) or line.startswith(f"I19: {product_id}/{url}"):
            return line
    return None


def _descend_container(
    pid: str,
    work_url: str,
    page: RawPage,
    raw_path: Path,
    *,
    reuse_kind: ReuseKind,
    scope: ConsumerScope,
    operations: ConsumerOperations,
    product: ProductScope,
    extractor: ExtractorPort,
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline,
    window: int | None,
    limits: CrawlLimits,
    archive_format: str | None = None,
) -> str | None:
    """B3: спуск в ретейнённый контейнер (``plan`` — только листинг и прогноз).

    Байты архива уже в ``corpus_raw`` (I19); спуск читает **индекс** и стримит принятых
    членов по одному во временный файл. Каждый контейнер получает запись
    ``archive_observations`` — и пустой тоже: «спуск ничего не дал» не имеет права быть
    тише, чем «спуск дал десять документов».

    **Нечитаемый контейнер — блокирующий класс покрытия.** Если индекс не прочитан (битый
    файл, нет бэкенда, читатель не подключён), содержимое осталось ненаблюдённым, поэтому
    элемент помечается ``FAILED`` с существующим классом ``parser_failed``: ``❌`` на таком
    корпусе не был бы доказательством отсутствия. ``raw_path`` сохраняется — следующий
    прогон повторит спуск из тех же байтов.

    :returns: причина отказа (``None`` — спуск исполнен, даже если он дал пустой список).
    """
    archive_url = page.url or work_url
    #: Идентичность архива — хеш ретейненных байтов; ``_retain`` называет файл
    #: ``{sha256}.bin``, поэтому ``stem`` и есть этот хеш (I19).
    archive_hash = raw_path.stem
    # ``persist_docs`` приходит операцией фазы (тесты подменяют его атрибутом модуля
    # ``corpus``): связываем один раз, чтобы две точки вызова не повторяли kwarg.
    ingest_member = partial(
        operations.ingest_member_doc, persist_docs=operations.persist_docs
    )
    pool_key: _ExtractPoolKey | None = (
        (level, pdf_pipeline, window) if window is not None else None
    )
    #: S2/план 23: корень и бюджет кэша — те же, что у фазы. Едут в payload члена
    #: примитивами; ключ задания — хеш байтов члена (``member_extract_request``).
    member_cache_root = (
        str(scope.extract_cache.root) if scope.extract_cache is not None else None
    )
    member_cache_max_bytes = (
        scope.extract_cache.max_bytes if scope.extract_cache is not None else None
    )
    defer_member: MemberDeferral | None = None
    #: Пул этого спуска признан мёртвым (``2 × timeout`` без завершений): остаток членов —
    #: видимый пропуск, а не 47 ожиданий по таймауту.
    state = {"dead": False}
    if (
        window is not None
        and scope.extraction_pool is not None
        and pool_key is not None
    ):
        # Сужение типа: поле носителя — ``ExtractionPool | None``, а вложенные функции
        # ниже читают пул уже **точно** (mypy не сужает атрибуты через границу функции).
        member_pool = scope.extraction_pool
        new_pool = partial(
            new_extract_pool,
            extraction_pool=scope.extraction_pool,
            extract_window=window,
            level=level,
            pdf_pipeline=pdf_pipeline,
            recycle_tasks=limits.extract_max_tasks_per_child,
        )

        def _submit_member(task: MemberTask) -> Any:
            """Submit извлечения члена; ``MemberOutcome`` — немедленный исход.

            ``split`` проверяется **здесь**, в родителе: у воркера нет признака
            ``supports_source_path``, и без этой проверки шов получил бы локальный путь
            вместо URL (капитуляция была бы невидимой).
            """
            unavailable = member_extract_unavailable(extractor)
            if unavailable is not None:
                return unavailable
            if state["dead"]:
                return MemberOutcome(
                    skip_reason=SKIP_EXTRACT_FAILED,
                    detail=(
                        "пул экстракции не завершил ни одного задания за "
                        f"{_EXTRACT_DEAD_POOL_TIMEOUTS} × таймаут — член не "
                        "извлечён (inline после отказа пула запрещён)"
                    ),
                )
            if scope.pools.abandoned(pool_key):
                # Как у страниц: пул исчерпал пересборки — inline запрещён
                # (docling в родителе на memory-constrained хосте = худший режим).
                return MemberOutcome(
                    skip_reason=SKIP_EXTRACT_FAILED,
                    detail=(
                        "extraction pool exhausted after "
                        f"{_EXTRACT_MAX_POOL_REBUILDS} rebuilds — inline disabled"
                    ),
                )
            executor = scope.pools.executor(pool_key, new_pool)
            return executor.submit(
                member_pool.worker,
                member_extract_request(
                    task,
                    fetched_at=page.fetched_at,
                    lang=page.lang,
                    level=level,
                    pdf_pipeline=pdf_pipeline,
                    rss_dir=scope.worker_rss_dir,
                    cache_root=member_cache_root,
                    cache_max_bytes=member_cache_max_bytes,
                ),
            )

        def _resolve_member(handle: Any, task: MemberTask) -> MemberOutcome:
            """FIFO-доводка задания члена: future → ``ingest_member_doc``."""
            try:
                _wait_started = time.perf_counter()
                try:
                    extracted = handle.result(timeout=scope.extract_timeout)
                finally:
                    # Тот же бакет, что и у страничной доводки: ожидание пула
                    # экстракции на главном потоке (член контейнера).
                    scope.timings.extract_wait_s += (
                        time.perf_counter() - _wait_started
                    )
            except TimeoutError:
                state["dead"] = scope.pools.note_timeout(pool_key)
                return MemberOutcome(
                    skip_reason=SKIP_EXTRACT_FAILED,
                    detail=f"extraction timeout ({scope.extract_timeout} s)",
                )
            except BrokenProcessPool:
                state["dead"] = True
                return MemberOutcome(
                    skip_reason=SKIP_EXTRACT_FAILED,
                    detail="extraction worker died",
                )
            except Exception as exc:  # noqa: BLE001 — изоляция по контракту
                return MemberOutcome(
                    skip_reason=SKIP_EXTRACT_FAILED,
                    detail=f"{type(exc).__name__}: {exc}",
                )
            # Операционная наблюдаемость: член — такое же доведённое задание пула,
            # поэтому он входит в ``pools.completed`` и в сброс серии таймаутов.
            scope.pools.note_completion(pool_key)
            return ingest_member(
                task,
                extractor=extractor,
                chunker=scope.chunker,
                ingest=product.ingest,
                seen_docs=product.seen_docs,
                seen_chunk_ids=product.seen_chunk_ids,
                errors=scope.errors,
                store=scope.store,
                level=level,
                fetched_at=page.fetched_at,
                lang=page.lang,
                artifact_probes=scope.artifact_probes,
                extracted=extracted,
                timings=scope.timings,
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
        container_for=scope.container_for,
        ingest_member=lambda task: ingest_member(
            task,
            extractor=extractor,
            chunker=scope.chunker,
            ingest=product.ingest,
            seen_docs=product.seen_docs,
            seen_chunk_ids=product.seen_chunk_ids,
            errors=scope.errors,
            store=scope.store,
            level=level,
            fetched_at=page.fetched_at,
            lang=page.lang,
            artifact_probes=scope.artifact_probes,
            # S2/план 23: inline-член спрашивает кэш по хешу своих байтов, как и воркер.
            extract_cache=scope.extract_cache,
            timings=scope.timings,
            reuse_kind=reuse_kind,
        ),
        observations=scope.archive_observations,
        members=scope.corpus_member_rows,
        errors=scope.errors,
        run_id=scope.run_id,
        created_at=_now_iso(),
        defer_member=defer_member,
        #: Окно упреждения = окно экстракции строки; без пула окно не используется.
        window=window or 1,
        #: Write-through (F1): провенанс спуска durable на каждом шаге, а не в конце
        #: фазы. Отказ стора изолируется в ``errors`` синком, поэтому спуск продолжается.
        persist_observation=persist_observation_sync(scope.store, scope.errors),
        persist_member=persist_member_sync(scope.store, scope.errors),
    )
    observation = walker.descend(
        archive_path=raw_path,
        archive_url=archive_url,
        archive_content_hash=archive_hash,
    )
    # План 19 §E.7: наблюдение контейнера durable — не только в списках фазы. Без строки
    # проход без сети не знает ни пути байтов, ни их хеша (версии документа у контейнера
    # нет) и молча теряет членов. Пишется и для ``UNREADABLE``: «контейнер не открылся» —
    # тоже наблюдение, и следующий прогон обязан видеть, что URL был контейнером.
    write_container_state(
        pid,
        archive_url,
        raw_path,
        observation,
        archive_format=archive_format,
        state=scope.state,
        errors=scope.errors,
        run_id=scope.run_id,
    )
    if observation.status is ArchiveDescentStatus.UNREADABLE:
        reason = f"archive_unreadable: {observation.reason}"
        operations.mark(
            pid,
            work_url,
            CrawlItemStatus.FAILED,
            error_kind=FailureKind.PARSER_FAILED.value,
            error=reason,
            raw_path=str(raw_path),
        )
        return reason
    return None


@dataclass(frozen=True)
class FrozenDescentReport:
    """Итог inline-спуска контейнера в проходе **без сети** (план 19 §E.7).

    ``status`` — :class:`~core.domain.enums.ArchiveDescentStatus` текущего спуска
    (значение строкой: он же пишется в ``container_state``). ``docs``/``chunks`` —
    сколько членов вошло в корпус и сколько чанков принято; ``members`` — сколько строк
    ``corpus_members`` добавилось (ингестнутые **и** видимо пропущенные). Пустой спуск
    (``docs == 0``) — не ошибка прибора, но и не тишина: вызывающий называет его токеном.
    """

    status: str
    docs: int = 0
    chunks: int = 0
    members: int = 0
    reason: str | None = None


def descend_frozen_container(
    *,
    product_id: str,
    archive_url: str,
    archive_path: Path,
    archive_content_hash: str,
    level: DocumentLevel,
    pdf_pipeline: PdfPipeline,
    extractor: ExtractorPort,
    limits: CrawlLimits,
    fetched_at: str,
    persist_docs: Callable[..., None],
    scope: ConsumerScope,
    product: ProductScope,
) -> FrozenDescentReport:
    """Спуск в контейнер из **сохранённых байтов** в проходе без сети (план 19 §E.7).

    Живой путь разворачивает контейнер из очереди конверсии (:func:`_descend_container`):
    там есть пул экстракции, окно упреждения и метки сеанса. Здесь ни того, ни другого, ни
    третьего нет по существу: прогон не наблюдал ничего и не пишет ``crawl_session_items``
    (``frozen`` — пере-извлечение прежних байтов, а не фетч). Поэтому:

    * **inline** — ``defer_member=None``, ``window=1``: извлечение члена идёт на главном
      потоке. Пул в этом проходе всё равно был бы объявлен относительно очереди, которой
      нет; число членов контейнера ограничено осью ``archive_max_members``, и это цена
      воспроизведения, а не деградация.
    * **провенанс — тот же, что у живого пути**: наблюдения и строки членов идут в
      списки фазы и в store (write-through синками). Без них «контейнер развернулся» было
      бы утверждением, а не записью.
    * **``fetched_at`` — прежнее наблюдение байтов**: нового получения не было, и
      ``now`` здесь был бы ложной провенанс-меткой.
    * **``lang=None``**: язык члена не выводится из архива; атрибуция «язык = язык
      архива» была бы догадкой (A3).
    * **``reuse_kind=REUSED``**: байты получены из состояния, а не с сервера.

    Маршрут и уровень — **объявленные** (per-URL ``extractor_factory(level, pdf_pipeline)``
    у вызывающего): иначе ``extractor_version`` назвал бы условие, которого в корпусе не
    было (I21/A3). ``extract_cache`` спрашивается по хешу байтов **члена** (считан на
    спуске, S2/план 23): повторный проход без сети над неизменёнными архивами не
    конвертирует те же члены второй раз.

    :returns: :class:`FrozenDescentReport` — статус и измеренные приросты корпуса.
    """
    docs_before = len(product.ingest.docs)
    chunks_before = len(product.ingest.chunks)
    members_before = len(scope.corpus_member_rows)
    ingest_member = partial(ingest_member_doc, persist_docs=persist_docs)
    walker = ArchiveDescentWalker(
        product_id=product_id,
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
        container_for=scope.container_for,
        ingest_member=lambda task: ingest_member(
            task,
            extractor=extractor,
            chunker=scope.chunker,
            ingest=product.ingest,
            seen_docs=product.seen_docs,
            seen_chunk_ids=product.seen_chunk_ids,
            errors=scope.errors,
            store=scope.store,
            level=level,
            fetched_at=fetched_at,
            lang=None,
            artifact_probes=scope.artifact_probes,
            # S2/план 23: проход без сети извлекает членов inline — кэш по хешу байтов
            # члена работает и здесь (иначе повторный ``frozen`` конвертировал бы всё).
            extract_cache=scope.extract_cache,
            timings=scope.timings,
            reuse_kind=ReuseKind.REUSED,
        ),
        observations=scope.archive_observations,
        members=scope.corpus_member_rows,
        errors=scope.errors,
        run_id=scope.run_id,
        created_at=_now_iso(),
        # Ни пула, ни окна: доводить член отложенно некому и незачем.
        defer_member=None,
        window=1,
        persist_observation=persist_observation_sync(scope.store, scope.errors),
        persist_member=persist_member_sync(scope.store, scope.errors),
    )
    observation = walker.descend(
        archive_path=archive_path,
        archive_url=archive_url,
        archive_content_hash=archive_content_hash,
    )
    write_container_state(
        product_id,
        archive_url,
        archive_path,
        observation,
        archive_format=None,
        state=scope.state,
        errors=scope.errors,
        run_id=scope.run_id,
    )
    return FrozenDescentReport(
        status=observation.status.value,
        docs=len(product.ingest.docs) - docs_before,
        chunks=len(product.ingest.chunks) - chunks_before,
        members=len(scope.corpus_member_rows) - members_before,
        reason=observation.reason,
    )


def _start_row(
    item: ExtractQueueItem,
    *,
    scope: ConsumerScope,
    operations: ConsumerOperations,
    product: ProductScope,
    extractor: ExtractorPort,
    window: int | None,
    reuse_kind: ReuseKind,
    work_url: str,
    work_class: WorkClass,
    claimed: bool = False,
) -> _RowJob:
    """Начать конверсию строки: зонд → спуск → переиспользование → отправка в пул.

    Порядок шагов — прежний inline-порядок (``_commit_outcome`` до F5), потому что он
    наблюдаем: зонд артефакта → спуск/контейнер → переиспользование (``304``) → извлечение
    → доводка. Меняется только владелец кода (краул → потребитель).

    Извлечение **отправляется**, а не доводится здесь: окно ``extract_workers`` означает
    несколько заданий «в полёте», поэтому доводка идёт FIFO отдельным шагом
    (:func:`_settle_row`). Inline-путь (пул не сконфигурирован) доводит сразу.

    ``work_url`` — запрошенный краулом адрес (ключ аудита сеанса и ``url_state``):
    ``item.url`` — финальный адрес документа (редирект), и подменять им адрес работы
    нельзя (иначе ``mark`` создаст второй элемент сеанса, а валидаторы лягут под адрес,
    которого план не спросит).
    """
    pid = item.product_id
    limits = scope.limits_for(item.url)
    level = _enum_or(DocumentLevel, item.level, DocumentLevel.FULL)
    pdf_pipeline = _enum_or(PdfPipeline, item.pdf_pipeline, PdfPipeline.STRUCTURED)
    page = _row_page(item, reuse_kind)
    raw_path = Path(item.raw_path)
    job = _RowJob(
        item=item,
        page=page,
        raw_path=raw_path,
        fetch_plan=_row_plan(item, limits),
        level=level,
        pdf_pipeline=pdf_pipeline,
        extractor=extractor,
        reuse_kind=reuse_kind,
        work_class=work_class,
        work_url=work_url,
        errors_before=len(scope.errors),
        claimed=claimed,
    )
    # B3: допущенный терминальный артефакт (документ **или** контейнер) проходит
    # **зонд** до экстракции: класс/формат/размер по байтам, ``page_count`` PDF по
    # структуре файла, листинг контейнера — по индексу (central directory).
    if artifact_is_terminal(
        page.url or item.url,
        page.content_type,
        job.fetch_plan.attachment_formats,
    ):
        probe = probe_artifact(
            raw_path,
            product_id=pid,
            url=page.url or item.url,
            content_type=page.content_type,
            container_for=scope.container_for,
        )
        scope.artifact_probes.append(probe.as_dict())
        if probe.is_container:
            # Контейнер — не документ: вместо экстракции исполняется спуск
            # (``archive_descent=plan`` — только листинг и прогноз).
            job.error = _descend_container(
                pid,
                work_url,
                page,
                raw_path,
                reuse_kind=job.reuse_kind,
                scope=scope,
                operations=operations,
                product=product,
                extractor=extractor,
                level=level,
                pdf_pipeline=pdf_pipeline,
                window=window,
                limits=limits,
                archive_format=probe.artifact.format,
            )
            job.done = True
            return job
    # 09 §6.3: ``304`` переиспользует версию документа **до** экстракции (экстрактор/
    # чанкер не вызываются). Неудача reuse (нет версии/чанков, I19-расхождение) — видимая
    # запись уже сделана, дальше обычный путь экстракции.
    job.reuse = operations.reuse_for_page(pid, work_url, page)
    if window is None or scope.extraction_pool is None:
        # Inline: извлечение на главном потоке (``ingest_one`` сам зовёт экстрактор и
        # ловит его отказ в тот же ``parser_failed``, что и пул).
        _finish_inline_row(job, scope=scope, operations=operations, product=product)
        return job
    job.pool_key = (level, pdf_pipeline, window)
    if scope.pools.abandoned(job.pool_key):
        # Пул исчерпал пересборки: никакой inline-экстракции. Документ ретейнён, но
        # исход терминален и видим (parser_failed).
        job.error = (
            "extraction pool exhausted after "
            f"{_EXTRACT_MAX_POOL_REBUILDS} rebuilds — inline disabled"
        )
        job.error_kind = FailureKind.PARSER_FAILED
        return job
    # C1/L4: проба ``extract_cache`` — **в родителе**, до submit. Хит идёт путём «готовый
    # текст», не занимая слот воркера и не оплачивая инициализатор. Ключ родителя и
    # воркера — один и тот же хеш ретенированных байтов (``raw_path.stem``).
    if scope.extract_cache is not None:
        cached = scope.extract_cache.get(
            _extract_cache_key(raw_path),
            _cache_extractor_version(
                extractor, _page_with_raw_path(page, raw_path), job.fetch_plan.extract
            ),
            _as_text(level),
        )
        if cached is not None:
            # Хит кэша — **не** запись здесь: ``finish_one`` пишет документ и чанки в
            # порядке вставки, а порядок вставки входит в ``vector_digest`` партиции.
            # Старт строки идёт по полосе (то есть может обогнать очередь), поэтому текст
            # отдаётся доводке (:meth:`_BandRun.settle`), а она идёт порядком очереди.
            job.cached = cached
            return job
    try:
        executor = scope.pools.executor(job.pool_key, _new_pool_for(job, scope))
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        scope.errors.append(
            f"{pid}/{item.url}: extract pool: {type(exc).__name__}: {exc} — inline"
        )
        _finish_inline_row(job, scope=scope, operations=operations, product=product)
        return job
    # Потолок задания считается **на старте**: доводка может ждать очереди (полоса занята
    # другим классом), и зависший воркер обязан истечь по часам, а не по моменту, когда
    # до его строки дошла очередь.
    job.deadline = time.perf_counter() + scope.extract_timeout
    try:
        job.future = executor.submit(
            scope.extraction_pool.worker,  # type: ignore[union-attr]
            _row_request(item, scope),
        )
    except Exception as exc:  # noqa: BLE001 — непиклящийся вход/сбой submit
        scope.errors.append(
            f"{pid}/{item.url}: extract submit: {type(exc).__name__}: {exc} — inline"
        )
        _finish_inline_row(job, scope=scope, operations=operations, product=product)
    return job


def _page_with_raw_path(page: RawPage, raw_path: Path) -> RawPage:
    """``RawPage`` с ``source_path`` = retained-файл (тело не копируется).

    ``RawPage`` — замороженный dataclass (порт, не Pydantic-модель), поэтому копия
    строится ``dataclasses.replace``.
    """
    return replace(page, source_path=str(raw_path), content=b"")


def _new_pool_for(job: _RowJob, scope: ConsumerScope) -> Any:
    """Фабрика пула ключа ``(уровень, механизм PDF, окно)`` для этого задания."""
    window = job.pool_key[2] if job.pool_key is not None else None
    return partial(
        new_extract_pool,
        extraction_pool=scope.extraction_pool,
        extract_window=window,
        level=job.level,
        pdf_pipeline=job.pdf_pipeline,
        recycle_tasks=scope.limits_for(job.item.url).extract_max_tasks_per_child,
    )


def _finish_inline_row(
    job: _RowJob,
    *,
    scope: ConsumerScope,
    operations: ConsumerOperations,
    product: ProductScope,
) -> None:
    """Довести строку inline и **записать** её исход в задание.

    Inline-путь не проходит через :func:`_settle_row` (пула нет), а исход обязан быть
    таким же: ``finish_one`` сообщает отказ **строкой** ``errors`` и возвращает ``None``.
    Без переноса этой строки в ``job.error`` строка закрылась бы ``done`` без документа,
    и корпус объявил бы себя полным — ровно тот тихий пропуск, ради которого заведён
    ``state=failed`` с ``error_kind``. Срез ``errors`` берётся **после** служебной строки
    деградации пула, иначе «перешли на inline» читалось бы как отказ документа.
    """
    before = len(scope.errors)
    job.doc_id = _finish_inline(
        job, scope=scope, operations=operations, product=product
    )
    job.done = True
    failure = _conversion_failure(
        scope.errors[before:], job.item.product_id, job.work_url
    )
    if failure is not None:
        job.error = failure
        job.error_kind = FailureKind.PARSER_FAILED


def _finish_inline(
    job: _RowJob,
    *,
    scope: ConsumerScope,
    operations: ConsumerOperations,
    product: ProductScope,
) -> str | None:
    """Inline-доводка строки: экстракция на главном потоке (пул не участвует).

    ``304`` переиспользует версию документа **до** экстракции: экстрактор не зовётся,
    доводка идёт путём «готовый текст» (:func:`finish_one` с ``extracted=None``) —
    ровно как в пул-пути (:func:`_finish_extracted`). Вызов ``ingest_one`` здесь
    оплатил бы извлечение, которое сервер уже отменил своим ``304``.
    """
    if job.reuse is not None:
        return operations.finish_one(
            job.item.product_id,
            job.work_url,
            job.page,
            job.fetch_plan,
            job.raw_path,
            None,
            product.ingest,
            product.seen_docs,
            product.seen_chunk_ids,
            job.extractor,
            job.level,
            reuse_kind=job.reuse_kind,
            reuse=job.reuse,
        )
    return operations.ingest_one(
        job.item.product_id,
        job.work_url,
        job.page,
        job.fetch_plan,
        job.raw_path,
        product.ingest,
        product.seen_docs,
        product.seen_chunk_ids,
        job.extractor,
        job.level,
        reuse_kind=job.reuse_kind,
    )


def _finish_extracted(
    job: _RowJob,
    extracted: Any,
    *,
    scope: ConsumerScope,
    operations: ConsumerOperations,
    product: ProductScope,
    extract_error: str | None = None,
    failure_kind: FailureKind | None = None,
) -> str | None:
    """Доводка готового извлечения (пул/кэш/304): нарезка без повторного извлечения."""
    reuse = job.reuse if extract_error is None else None
    return operations.finish_one(
        job.item.product_id,
        job.work_url,
        job.page,
        job.fetch_plan,
        job.raw_path,
        extracted,
        product.ingest,
        product.seen_docs,
        product.seen_chunk_ids,
        job.extractor,
        job.level,
        extract_error=extract_error,
        extract_failure_kind=failure_kind,
        reuse_kind=job.reuse_kind,
        reuse=reuse,
    )


@dataclass
class _Lease:
    """Аренда строки глазами потребителя (F6): очередь, воркер и счётчик heartbeat.

    Счётчик нужен отчёту: «потребитель работал долго и продлевал аренду» — факт, без
    которого нельзя отличить живой долгий документ от воскрешённого мёртвого.
    """

    queue: ExtractQueue
    worker: str
    heartbeats: int = 0

    def touch(self, item: ExtractQueueItem) -> bool:
        """Продлить аренду; ``False`` — она уже не наша (вернули/закрыли)."""
        if not self.queue.heartbeat(item, self.worker):
            return False
        self.heartbeats += 1
        return True


def _default_worker_id() -> str:
    """Идентификатор воркера по умолчанию: ``host/pid/поток``.

    Два потребителя в одном процессе обязаны различаться — иначе проверка «аренда ещё
    наша» (``expect_claimed_by``) не отличила бы их, и один закрывал бы работу другого.
    Форма — та же, что у владельца сеанса (:func:`core.pipeline.liveness.owner_id`), а не
    своя: ``claimed_by`` — тоже метка «кто именно», и разный формат в двух журналах
    мешал бы читать их вместе. Разделитель ``/`` — тот же, поэтому метка разбирается
    общим кодом (и нормируется в пинах порядка записи).
    """
    import os
    import socket
    import threading

    return "/".join(
        (
            socket.gethostname() or "unknown",
            str(os.getpid()),
            str(threading.get_ident()),
        )
    )


def _await_extraction(
    job: _RowJob,
    *,
    scope: ConsumerScope,
    lease: _Lease | None,
) -> Any:
    """Дождаться извлечения, **продлевая аренду** на каждом срезе ожидания (§4.2 п.3).

    Один ``future.result(timeout=extract_timeout)`` на долгом документе означал бы молчание
    дольше порога аренды: живой потребитель был бы признан мёртвым, и документ пошёл бы в
    конвертацию повторно. Поэтому ожидание нарезано, и между срезами идёт heartbeat.

    ``TimeoutError`` поднимается только когда истёк **общий** потолок документа
    (``extract_timeout``); таймаут среза — не исход, а повод продлить аренду.
    """
    # Потолок — от старта строки (:attr:`_RowJob.deadline`), а не от входа в ожидание:
    # доводка идёт порядком очереди, поэтому между стартом и первым ``result`` может
    # пройти всё окно другой полосы, и отсчёт «сейчас + timeout» простил бы зависание.
    deadline = (
        job.deadline
        if job.deadline is not None
        else time.perf_counter() + scope.extract_timeout
    )
    slice_s = scope.heartbeat_s if scope.heartbeat_s > 0 else scope.extract_timeout
    while True:
        remaining = deadline - time.perf_counter()
        if remaining <= 0:
            raise TimeoutError()
        try:
            return job.future.result(timeout=min(remaining, slice_s))  # type: ignore[union-attr]
        except TimeoutError:
            if job.claimed and lease is not None:
                lease.touch(job.item)
            continue


def _settle_row(
    job: _RowJob,
    *,
    scope: ConsumerScope,
    operations: ConsumerOperations,
    product: ProductScope,
    lease: _Lease | None = None,
) -> tuple[_RowJob, str | None, str | None, bool]:
    """Довести начатую строку: ``future.result`` → ``finish_one``.

    :returns: ``(doc_id, причина отказа, пул признан мёртвым)``. Мёртвый пул — сигнал
        вызывающему: остаток очереди этого ключа получает видимый отказ, а не ожидание
        по таймауту.

    ``BrokenProcessPool`` — воркер умер (краш/OOM): пул discard+rebuild (bounded) либо
    терминальный отказ. Таймаут — **per-artifact**: терминально помечается только это
    задание (``extraction_timeout``), пул продолжает работу; пересборка — только если пул
    не дал ни одного завершения за ``2 × timeout`` (:meth:`_ExtractPools.note_timeout`).

    Наблюдаемость: время ``future.result()`` (включая таймаут и смерть воркера) идёт в
    ``timings.extract_wait_s`` — это **ожидание** главного потока, а не работа.
    """
    if job.done:
        return job, job.doc_id, job.error, False
    if job.cached is not None:
        # Хит ``extract_cache``: экстракции не было, доводка обычная (нарезка + запись).
        # Записывается **здесь**, а не на старте строки, потому что порядок вставки входит
        # в ``vector_digest`` партиции, а старт полосы порядок очереди не гарантирует.
        doc_id = _finish_extracted(
            job, job.cached, scope=scope, operations=operations, product=product
        )
        return job, doc_id, _conversion_failure(
            scope.errors[job.errors_before :], job.item.product_id, job.item.url
        ), False
    if job.future is None:
        # Начатая строка без отправки: пул исчерпал пересборки. Inline запрещён.
        doc_id = _finish_extracted(
            job,
            None,
            scope=scope,
            operations=operations,
            product=product,
            extract_error=job.error or "extraction pool exhausted",
            failure_kind=job.error_kind or FailureKind.PARSER_FAILED,
        )
        return job, doc_id, job.error, False
    try:
        _wait_started = time.perf_counter()
        try:
            extracted = _await_extraction(job, scope=scope, lease=lease)
        finally:
            # ``extract_wait_s`` — **ожидание** экстракции на главном потоке (в т.ч. при
            # таймауте/сбое воркера). При окне > 1 включает ожидание чужих заданий:
            # рядом публикуется фактическое окно (``extract_workers``).
            scope.timings.extract_wait_s += time.perf_counter() - _wait_started
    except TimeoutError:
        # Зависший артефакт инвалидируется точечно; пул (и его здоровые воркеры)
        # продолжает работу. ``cancel`` на уже исполняемом future — no-op.
        cancel = getattr(job.future, "cancel", None)
        if cancel is not None:
            with contextlib.suppress(Exception):
                cancel()
        doc_id = _finish_extracted(
            job,
            None,
            scope=scope,
            operations=operations,
            product=product,
            extract_error="extraction timeout",
            failure_kind=FailureKind.EXTRACTION_TIMEOUT,
        )
        dead = scope.pools.note_timeout(job.pool_key)  # type: ignore[arg-type]
        if dead:
            # ``2 × timeout`` без единого завершения: пул мёртв — reap + bounded rebuild.
            scope.pools.terminate(job.pool_key)  # type: ignore[arg-type]
            recover_extract_pool(
                job.pool_key,  # type: ignore[arg-type]
                f"{job.item.product_id}/{job.item.url}",
                pools=scope.pools,
                errors=scope.errors,
            )
        return job, doc_id, "extraction timeout", dead
    except BrokenProcessPool:
        doc_id = _finish_extracted(
            job,
            None,
            scope=scope,
            operations=operations,
            product=product,
            extract_error="extraction worker died",
            failure_kind=None,
        )
        recover_extract_pool(
            job.pool_key,  # type: ignore[arg-type]
            f"{job.item.product_id}/{job.item.url}",
            pools=scope.pools,
            errors=scope.errors,
        )
        return job, doc_id, "extraction worker died", True
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        error = f"{type(exc).__name__}: {exc}"
        doc_id = _finish_extracted(
            job,
            None,
            scope=scope,
            operations=operations,
            product=product,
            extract_error=error,
            failure_kind=None,
        )
        return job, doc_id, error, False
    scope.pools.note_completion(job.pool_key)  # type: ignore[arg-type]
    # 09 §6.3: пул уже извлёк документ (экстракция не отменяется — она оплачена), но при
    # ``304`` нарезка переиспользуется: чанкер не зовётся.
    doc_id = _finish_extracted(
        job, extracted, scope=scope, operations=operations, product=product
    )
    return job, doc_id, _conversion_failure(
        scope.errors[job.errors_before :], job.item.product_id, job.item.url
    ), False


# --------------------------------------------------------------------------- #
# Полосы расписания: класс работы → свой курсор, своё окно, свой пул
# --------------------------------------------------------------------------- #


@dataclass
class _RowSlot:
    """Место строки в **глобальном** порядке очереди — порядке доводки.

    Задание появляется в слоте, когда до строки дошла её полоса: аренда берётся на старте,
    а не на планировании. Взять аренду заранее значило бы, что прерванный прогон оставит
    после себя ``attempts`` по строкам, работы по которым не начиналось, и после
    ``max_claim_attempts`` они стали бы терминальными без единой попытки.
    """

    index: int
    item: ExtractQueueItem
    job: _RowJob | None = None
    #: Строка закрыта **без задания**: её забрал другой потребитель (``lost``). Курсор
    #: доводки обязан пройти мимо — иначе голова очереди навсегда осталась бы пустой.
    closed: bool = False


@dataclass
class _Band:
    """Полоса расписания: один класс работы — свой курсор, своё окно, свой пул.

    Пул адресуется ключом ``(уровень, механизм PDF, окно)`` (:class:`_ExtractPoolKey`),
    поэтому разные окна **уже** дают разные пулы: ключ менять не нужно, и полосы не делят
    исполнителя. Общий у них только процесс-родитель и бюджет памяти.
    """

    work_class: WorkClass
    #: Индексы строк полосы в глобальном порядке (возрастают: порядок очереди внутри класса).
    slots: list[int] = field(default_factory=list)
    cursor: int = 0
    #: Окно полосы — максимум разрешённого по её строкам; ``None`` — строки идут inline.
    window: int | None = None
    #: Задания «в полёте» этой полосы (только с future: доведённые на старте сюда не
    #: попадают — они уже готовы, и держать их незачем).
    pending: deque[_RowJob] = field(default_factory=deque)
    #: Строка полосы пишет корпус **на старте** (спуск в контейнер либо inline-путь), то
    #: есть способна обогнать порядок очереди. Такая полоса стартует только на голове
    #: глобального порядка: ``corpus_chunks`` пишется в порядке вставки, и он входит в
    #: ``vector_digest`` партиции (``core/util/index_partition.py``).
    head_only: bool = False

    @property
    def capacity(self) -> int:
        """Сколько заданий полосы держать в полёте (inline — одно: оно идёт на потоке)."""
        return 1 if self.window is None else max(1, int(self.window))


def _row_ready(job: _RowJob) -> bool:
    """Готово ли задание к доводке — **неблокирующая** проверка.

    ``future.done()`` вместо ``result()``: доводка головы не имеет права ждать хвост другой
    полосы (иначе HTML снова встанет за PDF). Не-``concurrent.futures.Future`` (чужой или
    тестовый исполнитель) готовности не сообщает — такая строка доводится прежним
    блокирующим ``result`` (``_settle_row``), и поведение остаётся байт-в-байт прежним.

    Истёкший потолок задания тоже считается готовностью: ``_await_extraction`` немедленно
    поднимет ``TimeoutError``, и строка закроется ``extraction_timeout`` — без этого
    зависший воркер, чьё завершение никто не ждёт, не истёк бы никогда.
    """
    if job.done or job.future is None or job.cached is not None:
        return True
    if not isinstance(job.future, Future):
        return True
    if job.future.done():
        return True
    return job.deadline is not None and time.perf_counter() >= job.deadline


@dataclass
class _BandRun:
    """Один проход потребителя по продукту: полосы, глобальный порядок, счётчики.

    **Почему глобальный порядок доводки, а не «кто первый ответил».** ``_content_digest``
    партиции склеивает ``chunk_ids``/``content_hash`` **в порядке вставки**, поэтому запись
    документов обязана идти порядком очереди при любой ширине полос — иначе два прогона
    одного набора строк дали бы разный ``vector_digest`` при одинаковых ``doc_id`` (это
    проверяется тестом ``test_window_one_and_window_n_give_the_same_corpus_identity``). Полосы
    распараллеливают **извлечение**, а доводку (нарезка/запись) сериализуют в каноническом
    порядке: работа дорогая, запись дешёвая.

    Счётчики ``done``/``failed``/``lost``/``claimed`` — те же, что в :class:`ConsumerReport`;
    ``windows`` — фактическое окно по классам (``{work_class: window}``).
    """

    queue: ExtractQueue
    scope: ConsumerScope
    operations: ConsumerOperations
    product: ProductScope
    product_id: str
    worker: str
    lease: _Lease
    extractors: dict[tuple[DocumentLevel, PdfPipeline], ExtractorPort] = field(
        default_factory=dict
    )
    order: list[_RowSlot] = field(default_factory=list)
    bands: dict[WorkClass, _Band] = field(default_factory=dict)
    done: int = 0
    failed: int = 0
    lost: int = 0
    claimed: int = 0
    windows: dict[str, int] = field(default_factory=dict)
    #: F0: сумма пиков **стартовавших** заданий, MB. Окно класса — верхняя граница
    #: («сколько класс вправе держать»), а этот счётчик — фактическое ограничение: пики
    #: складываются, и 11 HTML-страниц по 317 MB плюс PDF на 3679 MB не помещаются в бюджет
    #: 4 GB, хотя каждое окно по отдельности «поместилось». Это **учёт**, а не потолок:
    #: слагаемые — измеренные пики (:func:`work_class_peak_mb`) и объявленный бюджет
    #: (:attr:`ConsumerScope.mem_budget_mb`), выдуманных чисел здесь нет.
    in_flight_mb: int = 0
    #: F0: заданий класса с неизмеренным пиком в полёте (0/1) — такой класс идёт один.
    unmeasured_in_flight: int = 0
    #: F0: наблюдённый максимум :attr:`in_flight_mb` за проход (публикуется в метриках).
    in_flight_peak_mb: int = 0
    #: F0: сколько **стартов** отложено из-за бюджета, по классам. Счётчик событий, а не
    #: строк: одна строка может быть отложена несколько раз, пока сосед не освободит
    #: память. Растёт при непустой очереди ⇒ упор в бюджет **узла**, а не в ширину полосы.
    skipped_for_budget: dict[str, int] = field(default_factory=dict)
    #: F0: ``accounted`` | ``class_budget_unaccounted`` | ``None`` (учёт не применялся).
    budget_accounting: str | None = None

    # -- планирование ------------------------------------------------------- #

    def add(self, item: ExtractQueueItem) -> None:
        """Поставить строку в глобальный порядок и в полосу её класса."""
        slot = _RowSlot(index=len(self.order), item=item)
        self.order.append(slot)
        work_class = work_class_for(
            content_type=item.content_type,
            url=item.url,
            level=_enum_or(DocumentLevel, item.level, DocumentLevel.FULL),
            pdf_pipeline=_enum_or(
                PdfPipeline, item.pdf_pipeline, PdfPipeline.STRUCTURED
            ),
        )
        band = self.bands.get(work_class)
        if band is None:
            band = _Band(work_class=work_class)
            self.bands[work_class] = band
            if work_class is WorkClass.CONTAINER:
                # Спуск в контейнер пишет членов на старте строки (``_descend_container``):
                # его полоса обязана идти строго порядком очереди.
                band.head_only = True
        band.slots.append(slot.index)

    def ordered_bands(self) -> list[_Band]:
        """Полосы в каноническом порядке классов (детерминизм подачи, а не порядок словаря)."""
        return [self.bands[c] for c in WorkClass if c in self.bands]

    # -- цикл --------------------------------------------------------------- #

    def run(self) -> None:
        """Подавать, ждать и доводить, пока очередь продукта не закрыта.

        Один общий цикл ожидания на все полосы: ``wait(..., FIRST_COMPLETED)`` возвращается
        по первому завершению любого класса, поэтому готовая HTML-строка не ждёт PDF.
        """
        cursor = 0
        bands = self.ordered_bands()
        while cursor < len(self.order):
            started = self._feed(cursor, bands)
            advanced = self._commit(cursor)
            if advanced != cursor:
                cursor = advanced
                continue
            if started:
                continue
            if not self._in_flight(bands):
                # Ни подать, ни довести нечего — этого не должно быть (голова очереди
                # всегда стартуема), но молчаливое зависание недопустимо: называем и
                # выходим, оставив строки незакрытыми (``pending > 0`` — видимый сигнал).
                self.scope.errors.append(
                    f"{self.product_id}: расписание конверсии не двигается на строке "
                    f"{self.order[cursor].item.url} — полосы пусты, исхода нет"
                )
                break
            self._await_ready(bands)

    def _feed(self, cursor: int, bands: list[_Band]) -> bool:
        """Подать строки в полосы, пока у каждой есть место в окне. ``True`` — подали хоть одну."""
        started = False
        for band in bands:
            while band.cursor < len(band.slots) and len(band.pending) < band.capacity:
                slot = self.order[band.slots[band.cursor]]
                limits = self.scope.limits_for(slot.item.url)
                level = _enum_or(DocumentLevel, slot.item.level, DocumentLevel.FULL)
                pdf_pipeline = _enum_or(
                    PdfPipeline, slot.item.pdf_pipeline, PdfPipeline.STRUCTURED
                )
                window = _row_window(
                    limits,
                    self.scope,
                    f"{self.product_id}/{slot.item.url}",
                    work_class=band.work_class,
                    level=level,
                    pdf_pipeline=pdf_pipeline,
                )
                if window is None or self.scope.extraction_pool is None:
                    # Inline-строка пишет корпус **на старте** (``_start_row`` доводит её
                    # сразу), поэтому полоса обязана стартовать её только на голове
                    # глобального порядка: иначе запись обгонит очередь, а
                    # ``vector_digest`` партиции склеивает чанки в порядке вставки.
                    band.head_only = True
                if band.head_only and (slot.index != cursor or band.pending):
                    break
                admitted, reserve_mb, hold_single = self._admit(
                    band, level, pdf_pipeline
                )
                if not admitted:
                    # Бюджет занят: строка ждёт, курсор полосы не двигается. Выходим из
                    # **этой** полосы, а не из цикла подачи: у соседнего класса память
                    # может быть свободна, и он обязан стартовать (иначе PDF ждал бы HTML
                    # и наоборот — тот самый FIFO, ради которого полосы и разделены).
                    self.skipped_for_budget[band.work_class.value] = (
                        self.skipped_for_budget.get(band.work_class.value, 0) + 1
                    )
                    break
                band.cursor += 1
                self._begin(
                    slot,
                    band,
                    limits=limits,
                    window=window,
                    level=level,
                    pdf_pipeline=pdf_pipeline,
                    reserve_mb=reserve_mb,
                    hold_single=hold_single,
                )
                started = True
        return started

    def _begin(
        self,
        slot: _RowSlot,
        band: _Band,
        *,
        limits: CrawlLimits,
        window: int | None,
        level: DocumentLevel,
        pdf_pipeline: PdfPipeline,
        reserve_mb: int | None = None,
        hold_single: bool = False,
    ) -> None:
        """Взять аренду и начать строку (claim → окно → извлечение → отправка).

        ``reserve_mb``/``hold_single`` — решение :meth:`_admit` о памяти. Резерв
        засчитывается **после** :func:`_start_row` и только если строка действительно
        заняла слот пула (``job.future is not None``): inline-строка и хит кэша память не
        держат, и авансовый резерв «утёк» бы навсегда.
        """
        item = slot.item
        was_claimed = False
        if item.state is ExtractQueueState.PENDING:
            if not self.queue.claim(item, self.worker):
                self.lost += 1
                slot.closed = True
                return
            self.claimed += 1
            was_claimed = True
        elif item.state is ExtractQueueState.CLAIMED and item.claimed_by != self.worker:
            # Чужая живая аренда: не трогаем. Просроченную вернёт ``reclaim_stale``
            # следующего прохода — видимо, а не по-тихому.
            self.lost += 1
            slot.closed = True
            return
        # Наблюдаемость расписания: ось объявлена, а окно — провенанс (не входит в
        # ``CrawlLimits.as_dict()``), поэтому публикуется, а не входит в ``run_id``.
        self.operations.note_scheduling(
            limits, work_class=band.work_class, window=window
        )
        if window is not None:
            band.window = window if band.window is None else max(band.window, window)
            self.windows[band.work_class.value] = max(
                self.windows.get(band.work_class.value, 0), window
            )
            self.scope.timings.note_extract_window(window, band.work_class)
        extractor = _extractor_for(self.scope, level, pdf_pipeline, cache=self.extractors)
        job = _start_row(
            item,
            scope=self.scope,
            operations=self.operations,
            product=self.product,
            extractor=extractor,
            window=window,
            # Происхождение наблюдения — факт цикла от краула; строка даёт лишь запасное
            # правило для остатка прошлого исполнения.
            reuse_kind=(self.queue.cycle_reuse_kind(item) or _reuse_kind_for(item)),
            work_url=self.queue.work_url(item),
            work_class=band.work_class,
            claimed=was_claimed,
        )
        slot.job = job
        if job.future is not None:
            job.peak_mb = reserve_mb
            job.hold_single = hold_single
            if reserve_mb is not None:
                self.in_flight_mb += reserve_mb
                self.in_flight_peak_mb = max(self.in_flight_peak_mb, self.in_flight_mb)
            elif hold_single:
                self.unmeasured_in_flight += 1
            band.pending.append(job)

    def _commit(self, cursor: int) -> int:
        """Довести готовые строки **в порядке очереди**; вернуть новый курсор."""
        while cursor < len(self.order):
            slot = self.order[cursor]
            if slot.closed:
                # Строку забрал другой потребитель: работы нет, курсор идёт дальше.
                cursor += 1
                continue
            job = slot.job
            if job is None or not _row_ready(job):
                break
            if not job.settled:
                self._settle(job)
            cursor += 1
        return cursor

    def _settle(self, job: _RowJob) -> None:
        """Довести одну строку: результат → запись → состояние строки очереди."""
        job.settled = True
        # Память возвращается **до** записи: задание уже завершилось (иначе доводка бы не
        # дошла), и держать его пик в счётчике значило бы морить соседей на время нарезки
        # и записи — работы, которая память воркера не занимает.
        self._release(job)
        band = self.bands[job.work_class]
        settled, doc_id, error, dead_pool = _settle_row(
            job,
            scope=self.scope,
            operations=self.operations,
            product=self.product,
            lease=self.lease,
        )
        self.done, self.failed, self.lost = _record_outcome(
            self.queue,
            settled,
            doc_id,
            error,
            done=self.done,
            failed=self.failed,
            lost=self.lost,
            worker=self.worker,
        )
        self._drop(band, job)
        if dead_pool:
            self._abort_band(band, error or "extraction pool terminated")

    def _drop(self, band: _Band, job: _RowJob) -> None:
        """Убрать задание из «в полёте» полосы (доводка идёт порядком старта внутри полосы)."""
        if band.pending and band.pending[0] is job:
            band.pending.popleft()
            return
        # Порядок внутри полосы — порядок её курсора, поэтому сюда попадать не должны;
        # но потерять задание нельзя, иначе место в окне останется занятым навсегда.
        band.pending = deque(item for item in band.pending if item is not job)

    def _admit(
        self, band: _Band, level: DocumentLevel, pdf_pipeline: PdfPipeline
    ) -> tuple[bool, int | None, bool]:
        """Допустить ли ещё одну единицу класса: ``(можно, резерв MB, «идёт один»)``.

        **Почему учёт, а не потолок.** Окно класса (:func:`_row_window`) отвечает на вопрос
        «сколько заданий класс вправе держать в полёте» и считается так, будто класс в
        корпусе один. Пики же складываются: при бюджете 4500 MB ``model_free`` получает
        12 × 317 = 3804 MB, а ``pdf_structured/layout_tables`` — 1 × 3679 MB, итого
        **7483 MB** при доступных 4000 MB. Ни одно слагаемое здесь не выдумано: 317 и
        3679 — измеренные пики (``docs/reports/measurements/2026-11-07-work-classes-peaks.md``,
        ``2026-10-24-docling-peaks-real-corpus.md``), бюджет — ``MemAvailable −
        PARENT_FLOOR_MB`` из ``run.yaml``/окружения. Поэтому ограничение — арифметика на
        этих числах, а не «безопасный» лимит: **отказ** здесь означал бы, что стоимость не
        измерена, а измерена она как раз и есть.

        Правила, по убыванию силы:

        1. бюджета нет (``ConsumerScope.mem_budget_mb is None``) — учёт невозможен, окна
           классов работают как прежде, а факт публикуется токеном
           :data:`~core.pipeline.resources.BUDGET_UNACCOUNTED`: «мы не считали» не имеет
           права выглядеть как «всё поместилось»;
        2. пик класса **не измерен** (``office``/``container``) — класс идёт один на узел:
           подставить чужой пик значило бы выдать замер другого стека за свой (A3);
        3. ``in_flight_mb + peak <= budget − PARENT_FLOOR_MB`` — можно;
        4. иначе — **не стартуем**, даём шанс другой полосе; строка вернётся, когда сосед
           освободит память (:meth:`_release`). Ничего не теряется: курсор полосы не
           двигается, аренда не берётся;
        5. исключение из (4): если **ничего** не в полёте, а единица не помещается и одна
           (пик больше всего бюджета), старт разрешается. Иначе прогон встал бы намертво
           на строке, которую preflight обязан был отвергнуть
           (:func:`core.cli.wiring.preflight_extraction_budget`), — а «встать» хуже, чем
           видимое превышение, названное в ``errors`` из :func:`_row_window`.

        Класс с неизмеренным пиком (правило 2) не складывается с соседями ни в одну
        сторону: пока он в полёте, не стартует никто, и наоборот — поэтому проверка
        ``unmeasured_in_flight`` стоит и в правиле 3, а не только в правиле 2.

        Резерв засчитывается не здесь, а в :meth:`_begin` **после** старта: строка может не
        дойти до пула (inline, хит ``extract_cache``, исчерпанный пул), и авансовый резерв
        утёк бы навсегда.
        """
        budget = self.scope.mem_budget_mb
        if budget is None:
            if self.budget_accounting is None:
                self.budget_accounting = BUDGET_UNACCOUNTED
            return True, None, False
        self.budget_accounting = BUDGET_ACCOUNTED
        peak, _reason = work_class_peak_mb(band.work_class, level, pdf_pipeline)
        if peak is None:
            if self.in_flight_mb == 0 and self.unmeasured_in_flight == 0:
                return True, None, True
            return False, None, False
        room = int(budget) - PARENT_FLOOR_MB
        if self.in_flight_mb + peak <= room and self.unmeasured_in_flight == 0:
            return True, peak, False
        if self.in_flight_mb == 0 and self.unmeasured_in_flight == 0:
            return True, peak, False
        return False, peak, False

    def _release(self, job: _RowJob) -> None:
        """Вернуть резерв строки в общий счётчик памяти — **ровно один раз**.

        Идемпотентность держится на самом задании (``peak_mb``/``hold_single`` гасятся при
        возврате), а не на дисциплине вызовов: путей закрытия строки три (успех, отказ
        конверсии, остаток мёртвой полосы), и забытый возврат на любом из них превратил бы
        бюджет в утекающий счётчик — прогон встал бы на середине, когда очередь ещё не
        пуста, и выглядело бы это как зависание, а не как дефект учёта.
        """
        if job.peak_mb is not None:
            self.in_flight_mb = max(0, self.in_flight_mb - job.peak_mb)
            job.peak_mb = None
        elif job.hold_single:
            self.unmeasured_in_flight = max(0, self.unmeasured_in_flight - 1)
            job.hold_single = False

    def _abort_band(self, band: _Band, reason: str) -> None:
        """Мёртвый пул **одной** полосы: её остаток — видимый отказ, остальные живут.

        Отказ инструмента на этом прогоне — не «работа не начата»: строка, оставленная
        ``pending``, не дала бы фазе объявить корпус собранным, и прогон остановился бы без
        исхода по этим документам. Запись отказов обгоняет порядок очереди, и это безопасно:
        ``finish_one`` при ``extract_error`` документа не пишет (только ``mark`` элемента
        сеанса), поэтому состав и порядок корпуса не меняются.
        """
        pending = band.pending
        band.pending = deque()
        # Остаток полосы закрывается терминально — его резервы обязаны вернуться в общий
        # счётчик, иначе бюджет «утечёт» и соседний класс встанет на середине прогона.
        for job in pending:
            self._release(job)
        self.done, self.failed, self.lost = _abort_pending(
            pending,
            reason,
            queue=self.queue,
            scope=self.scope,
            operations=self.operations,
            product=self.product,
            done=self.done,
            failed=self.failed,
            lost=self.lost,
            worker=self.worker,
        )

    @staticmethod
    def _in_flight(bands: list[_Band]) -> bool:
        return any(job.future is not None for band in bands for job in band.pending)

    def _await_ready(self, bands: list[_Band]) -> None:
        """Ждать **первого** завершения по всем полосам, продлевая аренды между срезами.

        ``wait(FIRST_COMPLETED)`` вместо ``result()`` одной строки: иначе полоса, чья голова
        медленная, держала бы главный поток и готовая работа соседней полосы не забиралась
        бы (ровно тот дефект, ради которого полосы и разделены). Срез не длиннее heartbeat:
        молчание дольше порога аренды означало бы «потребитель мёртв» и повторную конвертацию
        живого документа. Время ожидания идёт в ``extract_wait_s`` — это **ожидание**
        главного потока, а не работа.
        """
        futures = [
            job.future for band in bands for job in band.pending if job.future is not None
        ]
        real = [future for future in futures if isinstance(future, Future)]
        started = time.perf_counter()
        try:
            if real:
                wait(real, timeout=self._slice_s(bands), return_when=FIRST_COMPLETED)
        finally:
            self.scope.timings.extract_wait_s += time.perf_counter() - started
        for band in bands:
            for job in band.pending:
                if job.claimed:
                    self.lease.touch(job.item)

    def _slice_s(self, bands: list[_Band]) -> float:
        """Длина среза ожидания: не больше heartbeat и не больше ближайшего потолка."""
        heartbeat = (
            self.scope.heartbeat_s
            if self.scope.heartbeat_s > 0
            else self.scope.extract_timeout
        )
        now = time.perf_counter()
        deadlines = [
            job.deadline - now
            for band in bands
            for job in band.pending
            if job.deadline is not None
        ]
        if not deadlines:
            return heartbeat
        return max(0.01, min(heartbeat, min(deadlines)))


# --------------------------------------------------------------------------- #
# Единственная точка входа потребителя
# --------------------------------------------------------------------------- #


def consume(
    session_id: str | None,
    product_ids: Sequence[str],
    *,
    queue: ExtractQueue,
    scope: ConsumerScope,
    operations: ConsumerOperations,
    products: Mapping[str, ProductScope],
) -> ConsumerReport:
    """Конвертирует очередь продуктов до конца (F5: единственный hand-off).

    :param session_id: адрес сеанса (``None`` — durable-краул выключен, очередь в памяти).
    :param product_ids: продукты в авторском порядке; каждый закрывается целиком до
        перехода к следующему (``pb`` продукт-скоупед, I17).
    :param queue: очередь фактов «документ добыт» (:class:`ExtractQueue`).
    :param scope: фазовый носитель потребителя.
    :param operations: замкнутые операции фазы.
    :param products: продуктовые скоупы (накопитель и дедуп-множества).
    :returns: сводка; ``pending > 0`` означает, что продукт **не** собран, и фаза не
        вправе объявить ``ok``.

    **Состояние строки** меняется дважды: аренда ``pending → claimed`` (F6, §4.2 п.4) перед
    работой и ``claimed → done|failed`` после конверсии — с ``expect=CLAIMED`` и
    ``expect_claimed_by=<этот воркер>``. Чужая или просроченная аренда закрыть строку не даёт
    (``False`` → ``lost``), иначе ``done`` описывал бы работу, которую делает кто-то другой.
    Повторный вызов видит ``done`` и ничего не делает (идемпотентность). Аренда, чей владелец
    замолчал, возвращается в ``pending`` в голове прохода (``reclaim_stale``) с
    ``attempts += 1`` и видимой записью: «взял и умер» не теряет документ и не требует
    ручного вмешательства.

    **Работа идёт полосами по классу** (:class:`_BandRun`): у каждого класса
    (:class:`~core.domain.enums.WorkClass` — цена документа, а не методология) свой курсор,
    своё окно (:func:`_row_window`) и свой пул — пул адресуется ключом ``(уровень, механизм
    PDF, окно)``, поэтому разные окна уже дают разные пулы. Готовность заданий определяется
    неблокирующе (``wait(FIRST_COMPLETED)`` по всем полосам сразу), поэтому HTML-страница не
    ждёт PDF.

    **Запись — порядком очереди, при любой ширине полос.** ``_content_digest`` партиции
    склеивает ``chunk_ids``/``content_hash`` в порядке вставки, поэтому доводка идёт
    глобальным курсором: полосы распараллеливают **извлечение**, а запись сериализуют
    каноническим порядком. Состав корпуса (``doc_id``/``chunk_id``/``content_hash``) от
    ширины не зависит — это пин, а не следствие.
    """
    reports: list[ConsumerReport] = []
    #: F6/§4.3: строки, по которым берётся объявленный предел бэклога (ось у них одна).
    scope_items: list[ExtractQueueItem] = []
    worker = scope.worker_id or _default_worker_id()
    for product_id in product_ids:
        product = products[product_id]
        rows = list(queue.items(product_id))
        done = failed = already = lost = 0
        claimed = 0
        #: F6 (§4.2 п.3): просроченные аренды возвращаются в ``pending`` **до** чтения
        #: списка и видимо: кто держал, какой URL, сколько раз. Мёртвый воркер не должен
        #: оставлять документ вне корпуса, а живой — конвертировать его дважды.
        stale = queue.reclaim_stale(
            lease_s=scope.claim_lease_s,
            max_attempts=scope.max_claim_attempts,
        )
        for record in stale:
            scope.errors.append(
                "extract_claim_reclaimed: "
                f"{record['product_id']}/{record['url']}: attempts={record['attempts']}"
                f" last_owner={record['claimed_by'] or '—'}"
                f" exhausted={bool(record['exhausted'])}"
            )
        if stale:
            rows = list(queue.items(product_id))
        scope_items.extend(rows)
        docs_before = len(product.ingest.docs)
        #: F6: аренда глазами этого прохода — продлевается между срезами ожидания.
        lease = _Lease(queue=queue, worker=worker)
        run = _BandRun(
            queue=queue,
            scope=scope,
            operations=operations,
            product=product,
            product_id=product_id,
            worker=worker,
            lease=lease,
        )
        for item in rows:
            # Терминальное состояние строки — факт **прошлого** исполнения этой же
            # очереди (resume/``--reset`` того же ``run_id``), а не «уже сделано в этой
            # фазе»: новый Pass B обязан получить документы заново, иначе партиция
            # собралась бы пустой. Повторный же вызов потребителя в одной фазе виден по
            # ``was_converted`` — вот он и есть идемпотентность.
            if queue.was_converted(item):
                already += 1
                continue
            if item.state is ExtractQueueState.SKIPPED:
                # ``skipped`` — «конвертировать сознательно нечего» (краул закрыл строку
                # на терминальном отказе источника либо пропуск по политике). Работы по
                # такой строке нет; ``failed`` сюда не входит — сбой парсера повторяемый,
                # и resume обязан попробовать снова (09 §6.3).
                already += 1
                continue
            run.add(item)
        # Полосы по классу работы: HTML не ждёт PDF, у каждой своё окно и свой пул.
        run.run()
        done, failed, lost = run.done, run.failed, run.lost
        claimed = run.claimed
        left = queue.pending(product_id)
        report = ConsumerReport(
            product_id=product_id,
            done=done,
            failed=failed,
            already=already,
            lost=lost,
            claimed=claimed,
            reclaimed=len(stale),
            heartbeats=lease.heartbeats,
            reclaim_records=list(stale),
            pending=left,
            # Поле совместимости: максимум по классам (прежний смысл «ширина, на которой
            # шла работа»). Разбивка по классам — рядом и она же в провенансе.
            extract_window=max(run.windows.values()) if run.windows else None,
            extract_window_by_class=dict(run.windows),
            # F0: учёт памяти «в полёте». Пик публикуется только когда учёт вёлся:
            # ``0`` при необъявленном бюджете читалось бы как «ничего не занимали».
            budget_accounting=run.budget_accounting,
            in_flight_mb_peak=(
                run.in_flight_peak_mb
                if run.budget_accounting == BUDGET_ACCOUNTED
                else None
            ),
            class_skipped_for_budget=dict(run.skipped_for_budget),
            docs=len(product.ingest.docs) - docs_before,
        )
        # Учёт агрегируется на счётчиках фазы сразу: пик — максимумом по продуктам,
        # отложенные старты — суммой (оба факта относятся к бюджету **узла**).
        note_budget_accounting(
            scope.counters,
            accounting=run.budget_accounting,
            in_flight_mb_peak=(
                run.in_flight_peak_mb
                if run.budget_accounting == BUDGET_ACCOUNTED
                else None
            ),
            skipped=run.skipped_for_budget,
        )
        if left:
            report.notes.append(
                f"{product_id}: очередь конверсии не закрыта — строк без терминального "
                f"состояния {left}; корпус продукта не полон"
            )
        reports.append(report)
    merged = _merge_reports(reports)
    # F6/§4.3: backlog публикуется числом всегда (даже без объявленного предела): «сколько
    # байтов ждёт конверсии» — факт прогона, а не только повод ждать.
    merged.backlog = queue.backlog(
        limit_mb=_backlog_limit_mb(scope, scope_items), raw_root=scope.raw_root
    ).as_dict()
    if session_id is not None and merged.pending:
        scope.errors.append(
            f"extract_consume({session_id}): незакрытых строк очереди "
            f"{merged.pending} — конверсия прервана, корпус не полон"
        )
    return merged


def _record_outcome(
    queue: ExtractQueue,
    job: _RowJob,
    doc_id: str | None,
    error: str | None,
    *,
    done: int,
    failed: int,
    lost: int,
    worker: str | None = None,
) -> tuple[int, int, int]:
    """Перевести строку в терминальное состояние и посчитать исход.

    ``error`` — причина отказа конверсии; ``None`` — строка закрыта как ``done`` даже
    если документа не появилось (артефакт-контейнер, видимый пропуск): строка очереди
    обработана, а пропуск назван в ``corpus_members``/элементе сеанса.

    Возврат ``False`` от условного перехода — не «наша» работа: строку закрыл другой
    потребитель (или она уже была терминальной), поэтому она считается ``lost``, а не
    ``done``/``failed``.
    """
    del doc_id  # исход документа уже записан операцией фазы; здесь — только состояние
    #: F6: строка, взятая в аренду **этим** потребителем, закрывается с ``expect=CLAIMED``
    #: и проверкой владельца: просроченную аренду могли вернуть другому, и «закрытие задним
    #: числом» объявило бы ``done`` о чужой работе. Строка, оставшаяся терминальной с
    #: прошлого исполнения, переписывается с ``expect=`` тем же состоянием: это тот же факт
    #: о том же документе, а не гонка за работу (иначе Pass B получил бы пустую партицию).
    expect: ExtractQueueState | None
    if job.claimed:
        expect = ExtractQueueState.CLAIMED
        expect_claimed_by: str | None = worker
    else:
        expect = job.item.state if job.item.state in TERMINAL_QUEUE_STATES else None
        expect_claimed_by = None
    if error is not None:
        if queue.set_state(
            job.item,
            ExtractQueueState.FAILED,
            expect=expect,
            expect_claimed_by=expect_claimed_by,
            error_kind=(job.error_kind or FailureKind.PARSER_FAILED).value,
            error=error,
        ):
            queue.mark_converted(job.item)
            return done, failed + 1, lost
        return done, failed, lost + 1
    if queue.set_state(
        job.item,
        ExtractQueueState.DONE,
        expect=expect,
        expect_claimed_by=expect_claimed_by,
    ):
        queue.mark_converted(job.item)
        return done + 1, failed, lost
    return done, failed, lost + 1


def _abort_pending(
    pending: deque[_RowJob],
    reason: str,
    *,
    queue: ExtractQueue,
    scope: ConsumerScope,
    operations: ConsumerOperations,
    product: ProductScope,
    done: int,
    failed: int,
    lost: int,
    worker: str | None = None,
) -> tuple[int, int, int]:
    """Остаток «в полёте» — терминальный отказ, **не** ``pending``: пул мёртв, ждать нечего.

    Почему ``failed``, а не «оставить ``pending`` до следующего запуска». Мёртвый пул —
    отказ **инструмента** на этом прогоне, а не «работа не начата»: строка, оставленная
    ``pending``, не даёт фазе объявить корпус собранным (барьер ``pending=0``), и прогон
    остановился бы на ``corpus`` **без** исхода по этим документам. Поэтому исход
    называется здесь же (``parser_failed`` + причина, плюс ``mark`` элемента сеанса), и
    документ попадает в знаменатель покрытия как ``failed`` (I5) — «не измерено» с
    причиной, а не «не дошло до измерения».

    Прежнее поведение (до F5 плана ``17``) было ровно таким: прерывание коммитило все
    in-flight задания как ``FAILED`` — «не измерено» с причиной, а не тихое исчезновение.
    """
    while pending:
        job = pending.popleft()
        # Строка закрыта здесь и навсегда: она остаётся в глобальном порядке доводки, и
        # курсор дошёл бы до неё повторно (второй исход — двойной счёт и вторая запись).
        job.settled = True
        job.done = True
        doc_id = _finish_extracted(
            job,
            None,
            scope=scope,
            operations=operations,
            product=product,
            extract_error=reason,
            failure_kind=FailureKind.PARSER_FAILED,
        )
        done, failed, lost = _record_outcome(
            queue,
            job,
            doc_id,
            reason,
            done=done,
            failed=failed,
            lost=lost,
            worker=worker,
        )
    return done, failed, lost


def _backlog_limit_mb(
    scope: ConsumerScope, items: Sequence[ExtractQueueItem]
) -> int | None:
    """Объявленный предел бэклога (``crawl.extract_backlog_mb``) по осям строк (§4.3).

    Ось резолвится той же цепочкой app < run, что у краула, поэтому значение одинаково
    для всех строк прогона; перебор нужен лишь чтобы не заводить второй носитель
    (``ConsumerScope`` не хранит ось, которую уже умеет резолвить). ``None`` — предел не
    объявлен: число ``backlog_bytes`` публикуется, остановки приёма у потребителя нет.
    """
    for item in items:
        value = scope.limits_for(item.url).extract_backlog_mb
        if value is not None:
            return int(value)
    return None


def _merge_reports(reports: Sequence[ConsumerReport]) -> ConsumerReport:
    """Сводка по продуктам в одну запись (аддитивные счётчики, ``pending`` — сумма)."""
    if len(reports) == 1:
        return reports[0]
    merged = ConsumerReport(product_id=",".join(r.product_id for r in reports))
    for report in reports:
        merged.done += report.done
        merged.failed += report.failed
        merged.already += report.already
        merged.lost += report.lost
        merged.claimed += report.claimed
        merged.reclaimed += report.reclaimed
        merged.heartbeats += report.heartbeats
        merged.reclaim_records.extend(report.reclaim_records)
        merged.pending += report.pending
        merged.docs += report.docs
        if report.extract_window is not None and (
            merged.extract_window is None
            or report.extract_window > merged.extract_window
        ):
            merged.extract_window = report.extract_window
        for work_class, window in report.extract_window_by_class.items():
            merged.extract_window_by_class[work_class] = max(
                merged.extract_window_by_class.get(work_class, 0), window
            )
        if report.in_flight_mb_peak is not None:
            merged.in_flight_mb_peak = max(
                merged.in_flight_mb_peak or 0, report.in_flight_mb_peak
            )
        for work_class, skipped in report.class_skipped_for_budget.items():
            merged.class_skipped_for_budget[work_class] = (
                merged.class_skipped_for_budget.get(work_class, 0) + skipped
            )
        if report.budget_accounting == BUDGET_UNACCOUNTED:
            # Токен сильнее частичного учёта: продукт без бюджета не должен маскироваться
            # соседом, у которого бюджет был.
            merged.budget_accounting = BUDGET_UNACCOUNTED
        elif report.budget_accounting is not None and merged.budget_accounting is None:
            merged.budget_accounting = report.budget_accounting
        merged.notes.extend(report.notes)
    return merged


__all__ = [
    "ConsumerOperations",
    "ConsumerReport",
    "ConsumerScope",
    "ProductScope",
    "consume",
    "ingest_member_doc",
    "member_extract_request",
    "member_extract_unavailable",
    "persist_member_sync",
    "persist_observation_sync",
    "recover_extract_pool",
]