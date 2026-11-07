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
    """Член контейнера не извлечён: экстрактор не принимает локальные байты.

    Члену нельзя сходить по URL — его адрес синтетический, а файл на диске есть; поэтому
    извлечение возможно, только если экстрактор умеет читать ``page.source_path``. Шов
    умеет (``RemoteExtractor.supports_source_path``, ``ae0f933``), локальные читатели
    контейнеров — тоже; экстрактор без признака даёт капитуляцию явную, а не тихую: член
    не извлечён, причина видна в ``corpus_members.skip_reason``/``errors``. Проверка живёт
    **в родителе** (у воркера пула нет этого признака — он получает уже готовый payload).
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

    **``split``.** Экстрактор шва принимает **локальные байты** (``supports_source_path``,
    ``RemoteExtractor``) и извлекает член по артефакту через ``extract/submit``
    (``ae0f933``), поэтому член контейнера становится документом корпуса и под ``split``.
    Экстрактор, который байтов не принимает, даёт видимый пропуск ``extract_unavailable``
    с причиной, а не молчание (:func:`member_extract_unavailable`).

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
    #: Наблюдение расписания экстракции (окно → строка продукта/метрики).
    note_scheduling: Callable[[CrawlLimits], None]


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
    #: Фактическое окно экстракции продукта (``None`` — экстракция inline).
    extract_window: int | None = None
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


def _row_window(limits: CrawlLimits, scope: ConsumerScope, label: str) -> int | None:
    """Окно экстракции одной строки: ось ``extract_workers`` либо рециклинг.

    Ровно прежняя формула (``root_lifecycle`` до F5), но применённая к **строке**: окно —
    свойство объявленных осей того URL, а не очереди. ``None`` — экстракция inline.

    Ось — провенанс, а не измерение: она не входит в ``CrawlLimits.as_dict()``, поэтому
    её значение не может сдвинуть ``run_id``; публикуется она в
    ``metrics.json.resources.scheduling`` (:func:`core.pipeline.corpus_ingest.
    note_scheduling`).

    Несовпадение оси и пула — **не тихо** (:func:`_note_window_diagnostics`): раньше эту
    проверку делал корень, а окно теперь резолвится здесь, на строке.
    """
    axis = (
        int(limits.extract_workers)
        if limits.extract_workers is not None and int(limits.extract_workers) > 1
        else None
    )
    _note_window_diagnostics(limits, axis, scope, label)
    if axis is not None:
        return axis
    recycle = limits.extract_max_tasks_per_child
    if (
        recycle is not None
        and scope.extraction_pool is not None
        and scope.extraction_pool.supports_worker_recycling
    ):
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
    #: Запрошенный краулом адрес: ``mark``/``url_state`` адресуются им, тогда как
    #: ``item.url``/``page.url`` — финальный адрес документа (редирект).
    work_url: str = ""
    reuse: DocReuse | None = None
    pool_key: _ExtractPoolKey | None = None
    future: Any = None
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
            job.doc_id = _finish_extracted(
                job, cached, scope=scope, operations=operations, product=product
            )
            job.done = True
            return job
    try:
        executor = scope.pools.executor(job.pool_key, _new_pool_for(job, scope))
    except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
        scope.errors.append(
            f"{pid}/{item.url}: extract pool: {type(exc).__name__}: {exc} — inline"
        )
        _finish_inline_row(job, scope=scope, operations=operations, product=product)
        return job
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
    deadline = time.perf_counter() + scope.extract_timeout
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

    **Окно экстракции** резолвится по строке (:func:`_row_window`), а пул — фазовый по ключу
    ``(уровень, механизм PDF, окно)``. Ожидание — FIFO: ``future.result`` вызывается в
    порядке строк, поэтому порядок записи документов не зависит от того, какой воркер
    ответил первым.
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
        extractors: dict[tuple[DocumentLevel, PdfPipeline], ExtractorPort] = {}
        #: Ожидающие доводки строки **этого** продукта в порядке постановки (FIFO).
        pending: deque[_RowJob] = deque()
        window_seen: int | None = None
        dead_pool = False
        #: F6: аренда глазами этого прохода — продлевается между срезами ожидания.
        lease = _Lease(queue=queue, worker=worker)
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
            # F6 (§4.2 п.4): claim — условный переход ``pending → claimed``. ``False``
            # значит «строку уже взял/закрыл другой потребитель»: это **не** ошибка и не
            # наша работа — идём дальше, документ не конвертируется дважды.
            was_claimed = False
            if item.state is ExtractQueueState.PENDING:
                if not queue.claim(item, worker):
                    lost += 1
                    continue
                claimed += 1
                was_claimed = True
            elif item.state is ExtractQueueState.CLAIMED:
                if item.claimed_by != worker:
                    # Чужая живая аренда: не трогаем. Если она просрочена, её вернёт
                    # ``reclaim_stale`` следующего прохода — видимо, а не по-тихому.
                    lost += 1
                    continue
            limits = scope.limits_for(item.url)
            # Наблюдаемость расписания: ось объявлена, а окно — провенанс (не входит в
            # ``CrawlLimits.as_dict()``), поэтому публикуется, а не входит в ``run_id``.
            operations.note_scheduling(limits)
            window = _row_window(limits, scope, f"{product_id}/{item.url}")
            scope.timings.note_extract_window(window)
            if window is not None and (window_seen is None or window > window_seen):
                window_seen = window
            level = _enum_or(DocumentLevel, item.level, DocumentLevel.FULL)
            pdf_pipeline = _enum_or(
                PdfPipeline, item.pdf_pipeline, PdfPipeline.STRUCTURED
            )
            extractor = _extractor_for(
                scope, level, pdf_pipeline, cache=extractors
            )
            job = _start_row(
                item,
                scope=scope,
                operations=operations,
                product=product,
                extractor=extractor,
                window=window,
                # Происхождение наблюдения — факт цикла от краула; строка даёт лишь
                # запасное правило для остатка прошлого исполнения.
                reuse_kind=(
                    queue.cycle_reuse_kind(item) or _reuse_kind_for(item)
                ),
                work_url=queue.work_url(item),
                claimed=was_claimed,
            )
            pending.append(job)
            # Окно: не более ``W`` экстракций этого продукта «в полёте» (доводка FIFO).
            while window is not None and len(pending) >= window:
                settled, doc_id, error, dead_pool = _settle_row(
                    pending.popleft(),
                    scope=scope,
                    operations=operations,
                    product=product,
                    lease=lease,
                )
                done, failed, lost = _record_outcome(
                    queue,
                    settled,
                    doc_id,
                    error,
                    done=done,
                    failed=failed,
                    lost=lost,
                    worker=worker,
                )
                if dead_pool:
                    done, failed, lost = _abort_pending(
                        pending,
                        error or "extraction pool terminated",
                        queue=queue,
                        scope=scope,
                        operations=operations,
                        product=product,
                        done=done,
                        failed=failed,
                        lost=lost,
                        worker=worker,
                    )
        while pending:
            settled, doc_id, error, dead_pool = _settle_row(
                pending.popleft(),
                scope=scope,
                operations=operations,
                product=product,
                lease=lease,
            )
            done, failed, lost = _record_outcome(
                queue,
                settled,
                doc_id,
                error,
                done=done,
                failed=failed,
                lost=lost,
                worker=worker,
            )
            if dead_pool:
                done, failed, lost = _abort_pending(
                    pending,
                    error or "extraction pool terminated",
                    queue=queue,
                    scope=scope,
                    operations=operations,
                    product=product,
                    done=done,
                    failed=failed,
                    lost=lost,
                    worker=worker,
                )
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
            extract_window=window_seen,
            docs=len(product.ingest.docs) - docs_before,
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