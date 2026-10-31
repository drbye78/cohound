"""Дренаж одного seed-root: окно фетча/экстракции, frontier, спуск в контейнеры.

Часть декомпозиции ``corpus.py`` (план ``docs/reports/2026-10-30-large-modules-refactoring-plan.md``,
L5 шаг 3). Здесь — то, что делает работу **одного корня** от постановки seed'а до фиксации
исхода: BFS-раунды ``Frontier`` (локальный пул ‖ ``executor``-волны), окно экстракции,
спуск в ретейненный контейнер и коммит исходов на главном потоке.

**Почему отдельный модуль.** ``_drain_root`` был самым крупным замыканием тела
(1 476 строк) и читал 17 имён охватывающей области, включая ``frontier``/``limits``/
окно экстракции. Вынесен **целиком** и **дословно**: тело перенесено по AST-диапазону,
изменились только заголовок и имена вызовов.

**Почему носители, а не 60 параметров.** Всё, что раньше было свободной переменной
замыкания, по-прежнему приходит аргументом, но не поштучно: корень получает три носителя,
разделённые **по владению** — :class:`ProductDrainScope` (продуктовое состояние, общее для
всех корней продукта), :class:`PhaseDrainScope` (состояние и оси фазы) и
:class:`PhaseDrainOperations` (замкнутые операции фазы). Плоский список из 60 kwargs трёх
разных природ не отвечал на вопрос «что корень читает и что меняет»: имя носителя называет
владельца, имя поля — конкретное состояние. Носители — ``frozen`` dataclass'ы со
**ссылками** на мутируемые контейнеры: ``frozen`` запрещает подменить поле, а не изменить
содержимое, и это ровно то, что нужно (копия контейнера вернула бы тихий баг: корень
обновил бы копию, а сосед не увидел). Зависимости уровня фазы связываются один раз через
:func:`functools.partial` в точке вызова (``corpus._build_corpus``), per-продуктные —
на границе продукта.

**Границы.** Модуль импортирует ``corpus_ingest``/``corpus_commit``/``corpus_persist``,
но не ``corpus``: обратный импорт дал бы цикл. ``ingest_member_doc`` и ``persist_docs``
приходят параметрами, а не импортом, по той же причине, по которой ``finish_one``
приходит параметром: тесты подменяют их атрибутами модуля ``corpus``, и подмена обязана
доходить до писателя.

**Шаги, а не волна целиком (F4).** Волновой цикл вынесен в генератор
:func:`root_steps`, который выдаёт **план** волны (:class:`WavePlan`) и получает
:class:`WaveOutcomes`; :func:`root_lifecycle` — генератор корня целиком (подготовка +
шаги). Так исполнено чередование корней (F4b): полоса — это **право на host**, а не
объект, поэтому кооперативные шаги не требуют ни потоков-полос, ни мутаций вне главного
потока. Отправку волны и ожидание исполняет драйвер
(:mod:`core.pipeline.corpus_driver`), потому что допуск волны решается до отправки.

**Порядок записи не меняется.** Оракул — ``tests/pipeline/test_corpus_write_order.py``.
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Generator, Mapping, Sequence
from concurrent.futures import Executor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from core.domain import (
    ArchiveDescentStatus,
    ArchiveObservation,
    CorpusChunk,
    CorpusDoc,
    CorpusMember,
    CrawlItemStatus,
    CrawlSessionItem,
    DocumentLevel,
    ExtractQueueItem,
    FailureKind,
    PdfPipeline,
    ReuseKind,
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
from core.pipeline.corpus_commit import (
    PendingExtract,
    RootDrain,
    popleft_for_root,
)
from core.pipeline.corpus_index import _chunk_id, _CorpusStageTimings
from core.pipeline.corpus_ingest import (
    MEMBER_CONTENT_TYPES,
    LocalFetch,
    ProductIngest,
    RetainedRaw,
    RetainedReject,
    SitemapSeeds,
    _partial_fields,
    artifact_is_terminal,
    looks_like_html,
    resume_retained,
    terminal_failure_kind,
)
from core.pipeline.corpus_persist import DocReuse, _now_iso
from core.pipeline.corpus_state import CorpusCounters
from core.pipeline.crawl_frontier import SCOPE_OUT, CrawlLimits, Frontier
from core.pipeline.extract_cache import ExtractionCache
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
    _page_with_source_path,
)
from core.pipeline.incremental import doc_version_id
from core.ports.chunker import ChunkerPort
from core.ports.container import ContainerPort
from core.ports.crawl_executor import CrawlExecutorPort, CrawlWorkItem
from core.ports.extractor import ExtractorPort
from core.ports.types import ExtractionRoute, FetchMode, FetchPlan, RawPage
from core.util.document_formats import (
    DOCUMENT_SUFFIXES,
    document_content_type,
    document_suffix,
)
from core.util.hashing import content_hash
from core.util.html_links import extract_links

#: Phase 4/Ф0: ``error_kind`` терминального ``FAILED`` при исчерпании ``--max-attempts``.
#: Канонический класс сбоя (``FailureKind.ATTEMPTS_EXHAUSTED``): отличает «попытки
#: кончились» от сетевого/браузерного сбоя, не выходя за словарь §4.1.
_MAX_ATTEMPTS_ERROR_KIND = FailureKind.ATTEMPTS_EXHAUSTED.value

#: P4: сентинел «экстракция не была отложена» для :func:`build_corpus`-интернала
#: ``_commit_outcome`` (inline-путь вызывает ``ingest_one``; пул передаёт готовый результат).
_INLINE = object()


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


def persist_observation_sync(
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


def persist_member_sync(
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


def member_extract_request(
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


def member_extract_unavailable(extractor: ExtractorPort) -> MemberOutcome | None:
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
    timings: _CorpusStageTimings | None = None,
    reuse_kind: ReuseKind,
    persist_docs: Callable[..., None],
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
    unavailable = member_extract_unavailable(extractor)
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
    persist_docs(store, [doc], errors, versions_by_doc={document_id: member_dv_id})

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


@dataclass(frozen=True)
class RootPlan:
    """Один seed-root продукта: адрес, цель краула и чем его аугментировать (F4).

    Зачем отдельный тип. Дренаж корня принимает пять значений на корень; драйверу полос
    (F4b) они нужны как **значение**, иначе будущая функция драйвера стала бы
    70-параметровой — то есть перестала бы называть, что она читает. ``known_urls`` и
    ``serp_seeds`` зависят от корня, поэтому резолвятся в точке построения плана
    (``corpus._build_corpus``), а не внутри дренажа: корень их только потребляет.
    """

    root_url: str
    target: Any
    sitemap_seeds: bool
    #: Известные URL этого корня (ревалидация, 09 §6.3) — depth-0 листья вне бюджета.
    known_urls: Sequence[str] = ()
    #: Документы, найденные SERP-контуром (I3/§5): depth-0 seeds **этого** корня.
    serp_seeds: Sequence[str] = ()
    #: ``False`` — synthetic-бакет (orphan): авторского seed нет, только ревалидация.
    seed: bool = True
    #: ``False`` — хвост экстракции доводится здесь же (вызывающий читает счётчики
    #: frontier сразу); ``True`` — откладывается в фазовую очередь (C1).
    defer_tail: bool = True
    #: Слот записи провенанса в ``truncation_records`` (F4b). Занимается **в момент
    #: планирования**, то есть в авторском порядке корней, а не в порядке их завершения:
    #: при чередовании корней порядок завершения другой, а артефакт манифеста
    #: (``effective_limits.roots``) обязан остаться авторским. ``None`` — корень, у
    #: которого слота нет (synthetic-бакеты): запись добавляется в конец.
    record_slot: int | None = None


@dataclass(frozen=True)
class WaveRequest:
    """Задания волны, готовые к отправке, и маршрут её не-poolable части.

    ``entries`` — записи в порядке отправки (FIFO). ``non_poolable`` сообщает, что в
    волне есть живой фетч не-poolable маршрута (browser): он исполняется **отдельным
    пулом из одного потока**, а не на потоке драйвера, — иначе один браузерный фетч
    остановил бы все полосы (F4).
    """

    entries: list[_WaveEntry]
    non_poolable: bool = False


@dataclass(frozen=True)
class WavePlan:
    """План волны: записи, право на host'ы и маршрут — **до** отправки (F4b).

    Драйвер полос решает допуск волны **до** того, как она уйдёт в пул, поэтому
    генератор отдаёт план, а не отправленную волну: отправка и ожидание — работа
    драйвера (``corpus_driver``), коммит исходов — по-прежнему этого корня.

    ``hosts`` — hosts записей с живым фетчем (``LocalFetch.host``, тот же ключ, что у
    ``limiter.wait``, — см. :func:`corpus_ingest.local_fetch`): это ровно то право,
    которое драйвер резервирует за корнем на время волны. Пустое множество (волна без
    живого фетча: блэклист/исчерпанные попытки/resume) не пересекается ни с чем и
    допускается всегда.

    ``non_poolable`` — в волне есть живой фетч не-poolable маршрута (browser): ему нужен
    пул из **одного** потока (см. :func:`_dispatch_wave`), и драйвер обязан его дать.
    """

    entries: list[_WaveEntry]
    hosts: frozenset[str] = frozenset()
    non_poolable: bool = False


def _wave_plan(entries: list[_WaveEntry]) -> WavePlan:
    """Право волны на host'ы + маршрут — по готовым записям (F4b).

    ``hosts`` берутся **из дескрипторов** (``desc.host``), а не из URL записи: ключ права
    обязан совпадать с ключом вежливости (``limiter.wait(desc.host, …)``), иначе полосы
    пейсили бы один сайт двумя независимыми бюджетами.

    ``non_poolable`` повторяет условие ветки :func:`_dispatch_wave` (живой фетч вне пула
    фетча): resume/reject/browser-guard/терминальный класс живого фетча не делают.
    Расхождение с :func:`_dispatch_wave` — это не тихая ошибка: там сработает
    ``AssertionError`` «не-poolable маршрут без своего пула».
    """
    hosts = {
        entry.desc.host
        for entry in entries
        if entry.kind == "fetch" and entry.desc is not None and entry.desc.host
    }
    non_poolable = any(
        entry.kind == "fetch"
        and entry.desc is not None
        and not entry.desc.poolable
        and entry.desc.resume is None
        and entry.desc.reject is None
        and not entry.desc.browser_blocked
        and not entry.desc.terminal
        for entry in entries
    )
    return WavePlan(
        entries=entries, hosts=frozenset(hosts), non_poolable=non_poolable
    )


@dataclass(frozen=True)
class _WaveFetch:
    """Исход одного фетча: страница **или** сбой (ровно одно из двух)."""

    page: RawPage | None = None
    exc: Exception | None = None


@dataclass(frozen=True)
class WaveOutcomes:
    """Исходы отправленной волны: по элементу на запись, в том же порядке.

    ``fetch[i] is None`` — живого фетча у записи не было (блэклист, исчерпанные попытки,
    resume/reject/browser-guard/терминальный класс). Длина списка равна длине
    ``WaveRequest.entries``: коммит идёт по парам и порядок не восстанавливает.
    """

    fetch: list[_WaveFetch | None]


def _dispatch_wave(
    entries: list[_WaveEntry],
    *,
    pool: Executor | None,
    non_poolable_pool: Executor | None,
    local_fetch: Callable[[LocalFetch], RawPage],
) -> WaveRequest:
    """Отправка волны: живой фетч — в пул, браузерный — в пул из **одного** потока.

    Мутаций состояния сеанса/корпуса здесь нет (как и в прежнем ``_commit_wave``):
    отправляется только ``local_fetch`` — чистый сетевой вызов. Разделение с
    :func:`_commit_wave_results` — по владению: отправка и коммит остаются на главном
    потоке корня, ожидание — работа драйвера.

    Не-poolable маршрут (браузерный фетч) идёт в **отдельный однопоточный** пул. Прежний
    путь исполнял его на потоке драйвера, и при полосах один браузерный фетч остановил бы
    все полосы. Поток-владелец при этом сохранён: ``BrowserCrawler`` водит единственную
    ``_page`` без лока, поэтому у маршрута ровно один поток — он и есть владелец. Коммит
    исхода остаётся на главном потоке (T5: мутации пишет только драйвер).
    """
    non_poolable = False
    for entry in entries:
        if entry.kind != "fetch":
            continue
        desc = entry.desc
        if desc is None:
            # Инвариант ``_WaveEntry``: не-attempts/не-блэклист запись несёт ``desc``.
            raise AssertionError(f"{entry.pid}/{entry.wu}: запись волны без desc")
        if desc.poolable:
            if pool is None:
                # Инвариант ветки: ``poolable`` выставляется только при созданном пуле.
                raise AssertionError(
                    f"{desc.pid}/{desc.wu}: poolable без пула фетча"
                )
            entry.future = pool.submit(local_fetch, desc)
        elif (
            desc.resume is not None
            or desc.reject is not None
            or desc.browser_blocked
            or desc.terminal
        ):
            # resume/reject/browser-guard/терминальный класс: живого фетча нет.
            continue
        else:
            if non_poolable_pool is None:
                # Инвариант ветки: волна живёт только при созданном пуле фетча, а он
                # создаётся вместе с пулом не-poolable маршрута.
                raise AssertionError(
                    f"{desc.pid}/{desc.wu}: не-poolable маршрут без своего пула"
                )
            entry.future = non_poolable_pool.submit(local_fetch, desc)
            non_poolable = True
    return WaveRequest(entries=entries, non_poolable=non_poolable)


def _await_wave(
    request: WaveRequest,
    *,
    timings: _CorpusStageTimings,
) -> WaveOutcomes:
    """Дождаться исходов волны в порядке отправки.

    ``fetch_wait_s`` — **ожидание** пула: работу делает он, коммит
    (``_commit_wave_results``) остаётся работой главного потока и в бакет не входит.
    Бакет пополняется здесь, а не в коммите, потому что ожидание — единственное, что
    делает драйвер; порядок накопления — порядок элементов, то есть прежний.
    """
    fetch: list[_WaveFetch | None] = [None] * len(request.entries)
    for index, entry in enumerate(request.entries):
        if entry.future is None:
            continue
        _fetch_started = time.perf_counter()
        try:
            page = entry.future.result()
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            timings.fetch_wait_s += time.perf_counter() - _fetch_started
            fetch[index] = _WaveFetch(exc=exc)
        else:
            timings.fetch_wait_s += time.perf_counter() - _fetch_started
            fetch[index] = _WaveFetch(page=page)
    return WaveOutcomes(fetch=fetch)


def _commit_wave_results(
    entries: list[_WaveEntry],
    outcomes: WaveOutcomes,
    *,
    commit_local: Callable[..., Any],
    commit_or_defer: Callable[..., None],
    mark: Callable[..., None],
    emit: Callable[[], None],
    errors: list[str],
    max_attempts: int | None,
) -> None:
    """Коммитит исходы волны на главном потоке в FIFO-порядке отправки (D4).

    Порядок коммита = порядок отправки: детерминированы ``errors``, ``mark``,
    победитель дедупа (``seen_docs[doc_hash] = url``) и ``offer`` — единственные
    порядко-чувствительные артефакты (идентичность корпуса content-addressed).

    Вторая половина прежнего ``_commit_wave`` (F4): ожидание исходов ушло в
    :func:`_await_wave`, отправка — в :func:`_dispatch_wave`, а **порядок применения**
    остался здесь и остаётся инвариантом: ``errors``, ``mark`` и дедуп-победитель видят
    ровно прежнюю последовательность.
    """
    for index, entry in enumerate(entries):
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
            mark(
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
        fetched = outcomes.fetch[index]
        if fetched is not None:
            # Исход фетча пришёл от пула (ожидание и бакет ``fetch_wait_s`` — в
            # ``_await_wave``): живой фетч есть ровно у записей, у которых был
            # ``future``, поэтому ветка «нет исхода» ниже — инвариант, а не режим.
            outcome = (
                commit_local(desc, exc=fetched.exc)
                if fetched.exc is not None
                else commit_local(desc, page=fetched.page)
            )
        elif (
            desc.resume is not None
            or desc.reject is not None
            or desc.browser_blocked
            or desc.terminal
        ):
            # resume/reject/browser-guard/терминальный класс: живого фетча нет.
            outcome = commit_local(desc)
        else:
            # Инвариант ``_dispatch_wave``: живой фетч всегда отправлен (в пул или в
            # пул не-poolable маршрута), поэтому «живого фетча нет» без исхода —
            # ошибка сборки волны, а не исход.
            raise AssertionError(
                f"{entry.pid}/{entry.wu}: волна без исхода фетча"
            )
        commit_or_defer(entry.pid, entry.wu, entry.depth, outcome)


def root_steps(
    pid: str,
    root_url: str,
    frontier: Frontier,
    *,
    root_drain: RootDrain,
    limits: CrawlLimits,
    items: Mapping[tuple[str, str], CrawlSessionItem],
    errors: list[str],
    emit: Callable[[], None],
    mark: Callable[..., None],
    commit_local: Callable[..., Any],
    local_prefetch: Callable[..., LocalFetch],
    process_local: Callable[..., Any],
    submit_collect: Callable[..., Mapping[tuple[str, str], Any]],
    work_item: Callable[..., CrawlWorkItem],
    resolve_target: Callable[..., Any],
    finish_extract: Callable[[PendingExtract], None],
    extract_pending: deque[PendingExtract],
    extract_window: int | None,
    max_attempts: int | None,
    max_waves: int,
    wave_items: int,
    robots_enabled: bool,
    timings: _CorpusStageTimings,
    executor: CrawlExecutorPort | None,
    pool: Executor | None,
    max_workers: int | None,
    defer_tail: bool,
    sitemap_skips: Sequence[dict[str, Any]],
    _drain_for_window: Callable[[], None],
    _has_fetch_capacity: Callable[[], bool],
    _inflight_fresh: Callable[[], int],
    _tail_may_extend_frontier: Callable[[], bool],
    _skip_document_prefetch: Callable[[str], bool],
    _commit_or_defer: Callable[..., None],
) -> Generator[
    WavePlan, WaveOutcomes, tuple[Frontier, list[dict[str, Any]], RootDrain]
]:
    """Шаги дренажа одного seed-root: волновой цикл, frontier, коммит исходов (F4).

    Тело волнового цикла прежнего ``drain_root``, вынесенное **дословно**; изменилось
    только то, что относится к отправке волны: ``_commit_wave`` разделён на
    :func:`_dispatch_wave` (отправка), :func:`_await_wave` (ожидание) и
    :func:`_commit_wave_results` (FIFO-коммит), а между отправкой и коммитом стоит
    ``yield``. С F4b генератор отдаёт **план** волны (:class:`WavePlan`): допуск волны
    (право на host'ы) решается до отправки, поэтому отправку и ожидание исполняет
    драйвер, а коммит остаётся здесь.

    **Зачем генератор.** Полоса при чередовании корней — не объект, а **право на host**:
    один корень штатно фетчит несколько host'ов, поэтому носителем состояния остаётся
    корень, а «полоса» выражается правом на host. Тотальная мутация сеанса и корпуса
    допустима только на главном потоке (T5), поэтому параллельность здесь —
    кооперативные шаги, а не потоки-полосы: генератор выдаёт план волны и получает
    исходы, драйвер решает, когда их ждать. Пока драйвер гоняет один корень до конца
    (``host_concurrency`` None/1), поведение прежнее байт-в-байт.

    **Порядок — инвариант.** Внутри корня коммит идёт FIFO-порядком отправки: от него
    зависят победитель дедупа, порядок ``errors`` и порядок записи корпуса. Оракул —
    ``tests/pipeline/test_corpus_write_order.py``.

    **Пул — параметр, а не владение.** ``pool`` принадлежит полосе: при чередовании
    корней одновременно живут не более ``host_concurrency`` пулов фетча (и столько же
    пулов браузерного маршрута), а не по пулу на корень. ``pool is None`` — прежний
    последовательный путь (``max_workers`` ``None``/``1``), байт-в-байт.
    """
    waves = 0
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
                    finish_extract(
                        popleft_for_root(
                            extract_pending=extract_pending, root_drain=root_drain
                        )
                    )
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
            if waves >= max_waves:
                # Предохранитель (§1(iii)): неограниченный BFS по общему edge —
                # самонаведённый DoS. Никогда не останавливаться молча: усечение
                # ``round_budget`` + явная ошибка + сеанс ``PARTIAL``.
                frontier.mark_truncated("round_budget")
                errors.append(
                    f"{pid}/{root_url}: предохранитель раундов исчерпан "
                    f"({max_waves} волн) — обход остановлен "
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
                # A2 (F3): нулевая волна при непустой очереди — это остановка
                # по бюджету изъятия. Она обязана быть видимой: без пометки
                # усечение было бы тихим (осталось N узлов, которых не изымаем).
                if len(frontier) and not frontier.has_capacity:
                    frontier.mark_truncated("max_documents")
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
                work_target = resolve_target(pid, work_url)
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
                # Дескриптор коммита строится вручную (не ``local_prefetch``):
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
                        work_item(
                            pid, work_url, work_target, robots=robots_enabled
                        )
                    )
            # ``fetch_wait_s`` split-пути: ``submit_collect`` — это подача +
            # опрос статуса + сбор результатов, то есть **ожидание** исполнителя
            # (работу делает он, а не главный поток). Тот же бакет, что и у
            # локального пула: «участок фетча» — одна стадия продукта.
            _fetch_started = time.perf_counter()
            try:
                collected = submit_collect(work)
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
                    mark(
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
                    mark(
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
                    outcome = commit_local(desc)
                    _commit_or_defer(entry.pid, entry.wu, entry.depth, outcome)
                    continue
                result = collected.get((entry.pid, entry.wu))
                if result is None:
                    errors.append(
                        f"{entry.pid}/{entry.wu}: executor: результат не получен"
                    )
                    mark(
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
                    mark(
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
                outcome = commit_local(desc, page=page)
                _commit_or_defer(entry.pid, entry.wu, entry.depth, outcome)
            # M2: предохранитель — BFS-guard, поэтому чисто
            # ревалидационная волна его не расходует.
            if wave_has_discovery:
                waves += 1
            continue
        if pool is None:
            # --- Последовательный путь (max_workers None/1): как прежде. --- #
            # A2 (F3): бюджет изъятия проверяется **до** изъятия — узел,
            # снятый с очереди и отвергнутый по ёмкости, выпал бы из счёта
            # «в очереди осталось N». ``peek`` не изымает и о бюджете не решает.
            head = frontier.peek()
            if head is None:
                break
            work_url, depth = head
            # 09 §6.3: бюджет discovery исчерпан (с учётом окна экстракции), а
            # голова очереди — не ревалидационный лист: останавливаем обход
            # (ревалидация обрабатывается отдельно и бюджет не расходует).
            if not _has_fetch_capacity() and not frontier.is_revalidation(
                work_url
            ):
                frontier.mark_truncated("max_documents")
                break
            if frontier.next() is None:
                # Ёмкость (``fetched``) исчерпана самим ``next``: он уже поставил
                # усечение ``max_documents``.
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
                mark(
                    pid,
                    work_url,
                    CrawlItemStatus.FAILED,
                    error_kind=_MAX_ATTEMPTS_ERROR_KIND,
                    error=reason,
                    depth=depth,
                )
                emit()
                continue

            work_target = resolve_target(pid, work_url)
            # Последовательный путь (``max_workers in (None, 1)``): фетч inline на
            # главном потоке, поэтому ``fetch_wait_s`` здесь — **работа**, а не
            # ожидание (как и inline-ветка волны). Бакет общий: «участок фетча» —
            # одна стадия продукта.
            _fetch_started = time.perf_counter()
            try:
                outcome = process_local(
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
            # A2 (F3): та же видимость остановки по бюджету изъятия, что в
            # executor-ветке: непустая очередь + исчерпанный ``fetched``.
            if len(frontier) and not frontier.has_capacity:
                frontier.mark_truncated("max_documents")
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
            work_target = resolve_target(pid, work_url)
            desc = local_prefetch(
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
            # единственную ``_page`` без лока) — у них свой маршрут с одним потоком
            # (см. ``_dispatch_wave``); resume-элементы тоже минуют пул (живого
            # фетча нет; ``poolable``).
            if desc.poolable and pool is None:
                # Инвариант ветки: ``poolable`` выставляется только когда создан
                # пул фетча этого корня (``executor is None`` путь пула не имеет вовсе).
                raise AssertionError(
                    f"{desc.pid}/{desc.wu}: poolable без пула фетча"
                )
            entries.append(
                _WaveEntry(pid, work_url, depth, "fetch", desc=desc)
            )
        # Точка плана волны (F4b): корень отдаёт готовые записи **до** отправки, драйвер
        # решает допуск (право на host'ы), отправляет и ждёт. Коммит ниже остаётся
        # FIFO-порядком этого корня и исполняется на главном потоке сразу после того, как
        # драйвер вернёт исходы.
        outcomes = yield _wave_plan(entries)
        _commit_wave_results(
            entries,
            outcomes,
            commit_local=commit_local,
            commit_or_defer=_commit_or_defer,
            mark=mark,
            emit=emit,
            errors=errors,
            max_attempts=max_attempts,
        )
    # P4/C1: хвост корня доводится здесь, только если он **может** расширить
    # frontier (BFS-ссылки). Иначе он остаётся в фазовой очереди: пул общий,
    # и фетч следующего корня не ждёт экстракции (провенанс корня посчитает
    # финализатор — после доводки хвоста).
    if root_drain.pending and (
        not defer_tail or _tail_may_extend_frontier()
    ):
        while root_drain.pending:
            finish_extract(
                popleft_for_root(
                    extract_pending=extract_pending, root_drain=root_drain
                )
            )
    return frontier, list(sitemap_skips), root_drain


def lane_width(limits: CrawlLimits, *, executor: CrawlExecutorPort | None) -> int | None:
    """Ширина пула фетча полосы: ``max_workers`` или ``None`` (живого пула нет).

    Одна формула на два читателя — драйвер полос (создаёт пул) и сам корень (выбирает
    ветку «волна/последовательно»): второй копии правила быть не должно, иначе полоса
    получила бы пул, которого корень не ждёт, или наоборот.

    ``None`` возвращается в трёх случаях, и все три — не деградация: ``executor``
    (split: работу делает шов, ``submit_collect`` — один блокирующий вызов волны) и
    ``max_workers`` ``None``/``1`` (прежний последовательный путь без пула).
    """
    if executor is not None:
        return None
    if limits.max_workers is None or int(limits.max_workers) <= 1:
        return None
    return int(limits.max_workers)


@dataclass(frozen=True)
class ProductDrainScope:
    """Продуктовый скоуп дренажа: состояние, живущее весь продукт и общее корням.

    Один объект на продукт. Здесь то, что не принадлежит ни одному корню: накопитель
    ингеста, дедуп-множества продукт-партиции (I17), судьба SERP-находок, множество
    уже заряженных в resume-бюджет URL, все известные URL ревалидации и предикат
    владельца URL. Корни одного продукта **делят** это состояние, поэтому корень не
    вправе получить копию.

    Почему ``frozen``. Поля — ссылки на мутируемые контейнеры; ``frozen`` запрещает
    подменить поле (корень не подсунет свой словарь вместо продуктового), но не
    запрещает писать в содержимое — а писать он обязан именно в общее: копия вернула
    бы тихий дефект «корень обновил копию, сосед не увидел».

    Владелец — фаза сборки корпуса: создаётся на границе продукта
    (``corpus._build_corpus``), живёт до конца продукта.
    """

    #: Идентификатор продукта: единственное место, где он теперь назван в дренаже.
    pid: str
    #: Накопитель продукта (документы/чанки/счётчики ингеста).
    ingest: ProductIngest
    #: I17: ``content_hash → doc_id`` в пределах продукт-партиции.
    seen_docs: dict[str, str]
    #: I17: ``chunk_id`` в пределах продукт-партиции.
    seen_chunk_ids: set[str]
    #: Наблюдённая судьба SERP-находок (``enqueued``/``dup``/``blacklisted``).
    serp_ingest: dict[str, Any]
    #: F13: durable-URL, уже заряженные в resume-бюджет **продукта**.
    resume_counted: set[str]
    #: Известные URL ревалидации: вне бюджета discovery (B2/F13).
    revalidation_url_set: set[str]
    #: Самый специфичный корень-владелец URL (тот же предикат, что у группировки
    #: ревалидации); ``None`` — URL не принадлежит ни одному авторскому корню.
    owner_root: Callable[[str], str | None]


@dataclass(frozen=True)
class PhaseDrainScope:
    """Фазовый скоуп дренажа: состояние и оси одной фазы сборки корпуса.

    Один объект на фазу (не на продукт и не на корень): каналы видимости
    (``errors``), store и идентификатор прогона, порты (чанкер, экстрактор, контейнеры),
    фазовые очереди экстракции/финализаторов и пул экстракции, накопители наблюдений
    (пропуски документов, пробы артефактов, наблюдения архивов, члены контейнеров),
    оси фазы (``max_attempts``, размер волны, предохранитель раундов, таймаут) и режим
    «краул без конверсии» с адресом durable-очереди.

    Почему ``frozen``. Поля — ссылки на мутируемые контейнеры и на неизменяемые оси:
    подменять ось в полёте нельзя (она — условие измерения), а писать в общие списки
    ``errors``/наблюдений обязательно. ``frozen`` различает эти два случая.

    ``fetch_pool``/``browser_pool`` — единственные поля **полосы**: их создаёт драйвер
    на полосу (``lane_width``) и передаёт при допуске корня, поэтому на корень фазовый
    носитель копируется через :func:`dataclasses.replace`. Копия — не новое состояние:
    все контейнеры внутри неё общие, меняются только эти две ссылки.
    """

    #: Канал видимости фазы: каждая деградация обязана быть названа здесь.
    errors: list[str]
    #: Склад фазы (документы/чанки/очередь экстракции/сеанс).
    store: Any
    #: Идентификатор прогона для durable-записей провенанса (``None`` — прогона нет).
    run_id: str | None
    #: Чанкер фазы (версия входит в ``pb``).
    chunker: ChunkerPort
    #: Читатель контейнеров фазы (``None`` — бэкенда нет, спуск не открывается).
    container_for: Callable[[str | Path], ContainerPort | None] | None
    #: Durable-элементы сеанса: resume-бюджет и адрес документа (I19).
    items: Mapping[tuple[str, str], CrawlSessionItem]
    #: ``--max-attempts``: терминальный ``FAILED`` по исчерпанию попыток.
    max_attempts: int | None
    #: Размер волны фетча в узлах (ось executor-транспорта).
    wave_items: int
    #: Предохранитель раундов BFS (``round_budget``).
    max_waves: int
    #: Каталог стока пика памяти воркеров (``None`` — пик не собирается).
    worker_rss_dir: str | None
    #: Фазовый пул экстракции (``None`` — экстракция inline).
    extraction_pool: ExtractionPool | None
    #: Межпрогонный кэш извлечённого текста (ключ — хеш ретенированных байтов).
    extract_cache: ExtractionCache | None
    #: Экстрактор по умолчанию (композит ``full`` + ``structured``).
    extractor: ExtractorPort
    #: Фабрика экстрактора под уровень/механизм корня (``None`` — только переданный).
    extractor_factory: Callable[[DocumentLevel, PdfPipeline], ExtractorPort] | None
    #: Исполнитель краула (split: работу делает шов, локального пула нет).
    executor: CrawlExecutorPort | None
    #: Реестр фазовых пулов экстракции (ключ — уровень + механизм + окно).
    pools: _ExtractPools
    #: Фазовая FIFO-очередь отложенной экстракции (общая для корней).
    extract_pending: deque[PendingExtract]
    #: Хвосты корней, чей провенанс считается после доводки (C1).
    extract_finalizers: deque[RootDrain]
    #: Потолок одного ожидания пула, секунды (``deep``-таймаут корня).
    extract_timeout: float
    #: Накопители фазы (наблюдённые объёмы, а не лимиты).
    counters: CorpusCounters
    #: Бакеты стадий продукта: ожидание фетча/экстракции, нарезка, эмбеддинг.
    timings: _CorpusStageTimings
    #: Объявленные host'ы по продуктам (scope корня, §6).
    scope_hosts_by_product: Mapping[str, frozenset[str]]
    #: F12: ``(pid, loc)`` sitemap, уже принятые в очередь: один loc — один фетч.
    sitemap_seed_used: set[tuple[str, str]]
    #: P5: видимые пропуски документов (``off``-уровень) для агрегата сборки.
    document_skips: list[dict[str, Any]]
    #: B3: пробы терминальных артефактов (класс/формат/размер/``page_count``).
    artifact_probes: list[dict[str, Any]]
    #: B3: наблюдения спуска в контейнеры (в том числе пустые — молчания нет).
    archive_observations: list[ArchiveObservation]
    #: B3: члены контейнеров, дошедшие до корпуса.
    corpus_member_rows: list[CorpusMember]
    #: F2 (17 §4): режим «краул без конверсии» — durable-факт вместо сборки.
    crawl_only: bool
    #: F2: адрес durable-очереди ``extract_queue`` (без него режим невозможен).
    session_id: str | None
    #: F4b: пул фетча **полосы** (создаёт драйвер, см. :func:`lane_width`).
    fetch_pool: Executor | None = None
    #: F4b: пул не-poolable маршрута (браузер) — один поток на полосу.
    browser_pool: Executor | None = None


@dataclass(frozen=True)
class PhaseDrainOperations:
    """Замкнутые операции фазы: **чем** корень действует, а не что он читает/меняет.

    Все поля — callable'ы, связанные один раз на фазу в ``corpus._build_corpus``
    (``partial`` и замыкания над сеансом, складом и наблюдением фазы). Корень их только
    вызывает: подменить операцию нельзя (``frozen``), и это важно — операция несёт
    адресацию записи (какой продукт, какой сеанс), а не параметр корня.

    **Сигнатуры операций не меняются.** Их держат ``corpus.py`` и тесты (в том числе
    подменой атрибутов модуля ``corpus``), поэтому носитель их не переписывает: он
    называет владельца, а не форму вызова.

    Владелец — фаза: носитель создаётся один раз, до цикла продуктов, и переиспользуется
    всеми продуктами и корнями.
    """

    #: Наблюдаемость: границы продукта/строки прогресса (мутация — на главном потоке).
    emit: Callable[[], None]
    #: Отметка элемента сеанса (``CrawlItemStatus`` + класс сбоя).
    mark: Callable[..., None]
    #: Коммит исхода на главном потоке (retention + доводка + BFS).
    commit_local: Callable[..., Any]
    #: Локальный фетч (единственная функция, исполняемая в потоке пула).
    local_prefetch: Callable[..., LocalFetch]
    #: Доводка одного локального элемента (запись страницы в сеанс).
    process_local: Callable[..., Any]
    #: Волна через шов (split): подача + опрос + сбор результатов.
    submit_collect: Callable[..., Mapping[tuple[str, str], Any]]
    #: Рабочий элемент исполнителя из разрешённой цели.
    work_item: Callable[..., CrawlWorkItem]
    #: Резолв цели краула (движок/вид из слоёв источников).
    resolve_target: Callable[..., Any]
    #: Переиспользование версии документа (``304``): чанки без экстракции.
    reuse_for_page: Callable[..., DocReuse | None]
    #: Ингест одного документа (extract → chunk → запись), inline-путь.
    ingest_one: Callable[..., str | None]
    #: Фиксация готового извлечения (пул/кэш/переиспользование) — без экстракции.
    finish_one: Callable[..., str | None]
    #: Доводка одного отложенного задания фазовой очереди.
    finish_extract: Callable[[PendingExtract], None]
    #: D3: развёртка per-site sitemap корня в подчинённые depth-0 seeds.
    fresh_sitemap_seeds: Callable[..., SitemapSeeds]
    #: Оси краула корня (``app < run < site``) — единственная формула резолва.
    limits_for: Callable[[str], CrawlLimits]
    #: Наблюдение расписания экстракции (окно → строка продукта/метрики).
    note_scheduling: Callable[[CrawlLimits], None]
    #: Ингест члена контейнера (спуск B3), адрес записи — продукт.
    ingest_member_doc: Callable[..., MemberOutcome]
    #: Durable-персистенция документов продукта (инкрементально, I19).
    persist_docs: Callable[..., None]


def root_lifecycle(
    root: RootPlan,
    *,
    product: ProductDrainScope,
    phase: PhaseDrainScope,
    operations: PhaseDrainOperations,
) -> Generator[
    WavePlan, WaveOutcomes, tuple[Frontier, list[dict[str, Any]], RootDrain]
]:
    """D1/D3: BFS-drain одного seed-root (scope/блэклист/лимиты/глубина).

    Seeds — глубина 0; ссылка со страницы глубины ``d`` → ``d+1``. ``root.sitemap_seeds``
    — развернуть per-site sitemap корня в дополнительные depth-0 seeds (D3); они
    **подчинены** scope корня (``frontier.in_scope``), проходят тот же блэклист/
    бюджет ``max_documents``, что и seed оператора.
    ``root.known_urls`` — известные URL этого корня (scope inference): ставятся
    **после** авторского seed и **до** BFS отдельным путём
    (:meth:`Frontier.add_revalidate_seed`), поэтому не расходуют ``max_documents``,
    не расширяют BFS (листья) и дедуплицируются ``_seen`` с переоткрытием ссылкой.
    ``root.serp_seeds`` — документы, найденные внешним поиском (I3/§5): ставятся
    depth-0 seed'ами **этого** корня через :meth:`Frontier.add_seed`, то есть
    расходуют его ``max_documents`` и проходят блэклист, но не становятся новыми
    корнями. Так «доход» поиска платит из бюджета уже объявленного корня
    (нет умножения бюджета числом находок) и ``seeds_declared`` остаётся
    авторским.
    ``root.seed=False`` — synthetic-бакет (orphan): авторского seed нет, только
    ревалидация; усечение такого бакета не пишется вызывающим.
    Ставка в очередь гейтится scope (объявленный host + path-граница, C) и
    блэклистом; ``max_documents`` — жёсткий стоп (не исключение): останавливает
    постановку/обход этого корня.
    Возвращает frontier с провенансом усечения (``fetched``/``truncated``/
    ``truncated_reason``), агрегированный провенанс отказов scope у ``<loc>``
    (пусто без sitemap/без отказов) и :class:`RootDrain` — хвост экстракции корня
    в фазовой очереди (C1). ``root.defer_tail=False`` (synthetic-бакеты ревалидации,
    вызывающий читает счётчики frontier сразу) — хвост доводится здесь же.

    **Носители — по владению.** ``product`` (:class:`ProductDrainScope`) — общее
    состояние продукта, корни его делят, а не копируют. ``phase``
    (:class:`PhaseDrainScope`) — состояние и оси фазы, включая адрес durable-очереди и
    режим ``crawl_only``. ``operations`` (:class:`PhaseDrainOperations`) — замкнутые
    операции фазы (их сигнатуры держат ``corpus.py`` и тесты). Тело читает и пишет
    через эти три имени, поэтому «кто владеет этим полем» отвечает точка доступа, а не
    память читателя.

    ``phase.crawl_only`` (17 §4, F2) — режим «краул без конверсии»: добытый документ
    записывается durable-фактом в ``extract_queue``, а зонд артефакта, спуск в
    контейнеры, ``ingest_one``/``finish_one`` и нарезка **не** выполняются. Так
    «краул выполнен» перестаёт быть синонимом «корпус собран»: сборка — работа
    потребителя очереди. Счётчики (``max_documents``/``pages_fetched``/усечение) и
    расширение BFS-ссылок остаются теми же — иначе режим был бы другим краулом, а не
    тем же без конверсии. ``phase.session_id`` — адрес очереди; без него режим
    невозможен (факту некуда лечь).

    **F4b (17 §6): генератор шагов, а не функция.** Подготовка
    (limits/frontier/seeds/зарядка бюджета) исполняется при первом ``next`` — то есть в
    момент допуска корня драйвером, а не при создании объекта. Дальше корень выдаёт
    :class:`WavePlan` и получает :class:`WaveOutcomes`; **отправку** волны и **ожидание**
    исполняет драйвер (:mod:`core.pipeline.corpus_driver`), потому что допуск волны
    (право на host'ы) решается до отправки. Коммит исходов остаётся здесь и идёт
    FIFO-порядком отправки.

    ``phase.fetch_pool``/``phase.browser_pool`` — пулы **полосы**, их создаёт драйвер
    (одна пара на активную полосу, не на корень) и передаёт при допуске корня, из-за
    чего фазовый носитель копируется на корень через :func:`dataclasses.replace`. Их
    отсутствие при объявленном ``max_workers`` — ``AssertionError``: молчаливый
    inline-фетч на главном потоке схлопнул бы полосы.
    """
    root_url = root.root_url
    sitemap_seeds = root.sitemap_seeds
    revalidate = root.known_urls
    serp_seeds = root.serp_seeds
    seed = root.seed
    defer_tail = root.defer_tail
    # ``persist_docs`` приходит операцией фазы (тесты подменяют его атрибутом модуля
    # ``corpus``): связываем один раз, чтобы три точки вызова не повторяли kwarg.
    ingest_member = partial(
        operations.ingest_member_doc, persist_docs=operations.persist_docs
    )
    limits = operations.limits_for(root_url)
    operations.note_scheduling(limits)
    frontier = Frontier(
        root_url,
        limits,
        scope_hosts=phase.scope_hosts_by_product.get(product.pid, frozenset()),
    )
    # D3/дефект B (I17): ``<loc>`` sitemap — кандидаты **этого** корня; scope-фильтр
    # применяется до дедупа ``(pid, loc)``. Дедуп-пометка — не здесь, а по факту
    # приёма в очередь (F12, см. цикл ``extra_seeds`` ниже). Без sitemap
    # ``_expand_sitemap`` отдаёт ``[]`` без обращения к поллеру — путь не исполняется
    # (поведение байт-в-байт).
    sitemap = (
        operations.fresh_sitemap_seeds(product.pid, root_url, frontier)
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
    root_extractor: ExtractorPort = phase.extractor
    # Композит по умолчанию — ``full`` + ``structured``; переданный экстрактор и есть
    # эта пара, поэтому для неё «понижение до переданного» невозможно. Отказ OCR на
    # ``full`` приходит из адаптера на построении конвертера и становится
    # ``parser_failed`` (видимая запись ``errors`` через ``finish_one``/
    # ``finish_extract``), а не тихой подменой уровня.
    # 17 §4 (F2): в режиме «краул без конверсии» конвертер не строится вовсе — его
    # построение и есть конверсия (загрузка весов). Экстрактор, таким образом, остаётся
    # переданным и не вызывается: ``ingest_one``/``finish_one`` этот режим не исполняет.
    if not phase.crawl_only and phase.extractor_factory is not None and (
        level is not DocumentLevel.FULL
        or pdf_pipeline is not PdfPipeline.STRUCTURED
    ):
        try:
            root_extractor = phase.extractor_factory(level, pdf_pipeline)
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            phase.errors.append(
                f"{product.pid}/{root_url}: extractor_factory({level.value}, "
                f"{pdf_pipeline.value}): {type(exc).__name__}: {exc} — "
                "понижен до переданного экстрактора"
            )
            root_extractor = phase.extractor

    def _record_document_skip(skip_url: str, reason: str) -> None:
        """P5: видимая запись пропуска документа (frontier + агрегат ``IndexBuild``).

        Параметр назван ``skip_url``, а не ``url``, намеренно: ``url`` —
        переменная цикла внешней области, и совпадение имён делало бы связывание
        неотличимым от позднего (B023).
        """
        frontier.note_document_skip(skip_url, reason)
        phase.document_skips.append(
            {
                "product_id": product.pid,
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
        and phase.extraction_pool is not None
        and phase.extraction_pool.supports_worker_recycling
    )
    extract_window = (
        extract_workers_axis
        if extract_workers_axis is not None
        else (1 if can_recycle else None)
    )
    if extract_workers_axis is not None and phase.extraction_pool is None:
        # Ось задана, но пул не сконфигурирован — не тихо: явная запись + inline.
        phase.errors.append(
            f"{product.pid}/{root_url}: crawl.extract_workers={extract_window} задан, "
            "но пул экстракции не сконфигурирован — экстракция inline"
        )
    if recycle_tasks is not None and phase.extraction_pool is None:
        phase.errors.append(
            f"{product.pid}/{root_url}: crawl.extract_max_tasks_per_child="
            f"{recycle_tasks} задан, но пул экстракции не сконфигурирован — "
            "экстракция inline"
        )
    elif (
        recycle_tasks is not None
        and not phase.extraction_pool.supports_worker_recycling  # type: ignore[union-attr]
    ):
        # ``split`` (потоковый пул) не пересоздаёт воркеров — не молчим.
        phase.errors.append(
            f"{product.pid}/{root_url}: crawl.extract_max_tasks_per_child="
            f"{recycle_tasks} поддерживается только процессным пулом (local) — "
            "ось игнорируется"
        )
    #: C1/P7: ключ фазового пула — уровень документа + механизм PDF + окно
    #: экстракции. Пул один на прогон и переиспользуется всеми корнями/продуктами
    #: с тем же ключом (forkserver и веса конвертера платятся один раз);
    #: ``shutdown`` — один раз в ``finally`` фазы.
    #: Наблюдаемость: фактическое окно уходит в строку продукта рядом с
    #: ``extract_wait_s`` — иначе «ожидание» при окне > 1 читалось бы как работа.
    phase.timings.note_extract_window(extract_window)
    pool_key: _ExtractPoolKey | None = (
        (level, pdf_pipeline, extract_window)
        if extract_window is not None
        else None
    )
    #: Исполнять ли экстракцию в пуле (снимается inline-fallback'ом при отказе).
    #: **Локальный для корня** флаг шага, а не поле продуктового ``counters``: при
    #: чередовании корней корень без пула выставил бы ``False`` соседу, и тот
    #: конвертировал бы inline на драйвере — конкурентность схлопнулась бы без единой
    #: ошибки в артефактах.
    defer_extract = (
        extract_window is not None and phase.extraction_pool is not None
    )
    #: Пул экстракции текущего спуска признан мёртвым (``2 × timeout`` без
    #: завершений). Тоже флаг корня и по той же причине: общий объект выключал бы
    #: членов соседнего корня.
    member_pool_dead = False
    #: C1: хвост этого корня в фазовой очереди доводки (счётчик + финализатор).
    root_drain = RootDrain()
    # L5 шаг 2: ``partial`` привязывает состояние **этого** корня (окно, уровень,
    # механизм PDF, frontier) — то, что остаётся в ``_drain_root`` до шага 3.
    _new_extract_pool = partial(
        corpus_ingest.new_extract_pool,
        extraction_pool=phase.extraction_pool,
        extract_window=extract_window,
        level=level,
        pdf_pipeline=pdf_pipeline,
        recycle_tasks=recycle_tasks,
    )
    _make_extract_request = partial(
        corpus_ingest.make_extract_request,
        extract_cache=phase.extract_cache,
        level=level,
        pdf_pipeline=pdf_pipeline,
        worker_rss_dir=phase.worker_rss_dir,
    )
    _inflight_fresh = partial(
        corpus_commit.inflight_fresh,
        extract_pending=phase.extract_pending,
        frontier=frontier,
        root_drain=root_drain,
    )
    _has_fetch_capacity = partial(
        corpus_commit.has_fetch_capacity,
        frontier=frontier,
        limits=limits,
        extract_pending=phase.extract_pending,
        root_drain=root_drain,
    )
    _tail_may_extend_frontier = partial(
        corpus_commit.tail_may_extend_frontier,
        extract_pending=phase.extract_pending,
        frontier=frontier,
        limits=limits,
        root_drain=root_drain,
    )
    _drain_for_window = partial(
        corpus_commit.drain_for_window,
        extract_pending=phase.extract_pending,
        extract_window=extract_window,
        frontier=frontier,
        limits=limits,
        root_drain=root_drain,
        pools=phase.pools,
        errors=phase.errors,
        timings=phase.timings,
        extract_timeout=phase.extract_timeout,
        extract_finalizers=phase.extract_finalizers,
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
    # ``root_path_prefix`` (``owner_root``, тот же предикат, что у группировки
    # ревалидации), а не первый корень-претендент: иначе host-корень с префиксом
    # ``/`` забирал зарядку у docs-корня и потолок умножался на число перезапусков.
    # ``resume_counted`` остаётся продукт-wide страховкой от двойной зарядки.
    consumed = 0
    #: Заряженные этим корнем URL: каждый обязан быть и **дренируемым** (см. ниже).
    charged_urls: list[str] = []
    for (ipid, iurl), item in phase.items.items():
        if ipid != product.pid or iurl in product.resume_counted:
            continue
        # Известные URL ревалидации — вне бюджета discovery: иначе resumed
        # сессия заряжала бы ими ``max_documents``, seed стал бы ``truncated``
        # и продукт ложно ``complete=False`` (регрессия B2/F13 уровнем выше).
        if iurl in product.revalidation_url_set:
            continue
        # B023-исключение: ``owner_root`` пересоздаётся на каждом продукте
        # вместе с ``_drain_root`` (обе привязки — в одной итерации цикла), и
        # дренаж продукта завершается до следующей итерации, поэтому поздняя
        # привязка ячейки не подменяет владельца URL.
        owner = product.owner_root(iurl)  # noqa: B023 — см. выше
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
        product.resume_counted.add(iurl)
        charged_urls.append(iurl)
    frontier.note_consumed(consumed)
    charged_set = set(charged_urls)
    if seed and not frontier.has_capacity and root_url in charged_set:
        # A2 (F3 плана 17): бюджет закрыт resume-зарядкой ⇒ как discovery seed его
        # никто не изымет (``next`` откажет). Но байты корня уже ретенированы и
        # зарядили бюджет — это ровно ревалидация известного URL, а её путь бюджет не
        # расходует. Иначе страница корня выпала бы из корпуса (дефект 3, 09 §6.3).
        seed_status = frontier.add_revalidate_seed(root_url)
    else:
        seed_status = frontier.add_seed(root_url) if seed else "synthetic"
    if seed_status == "queued":
        phase.counters.product_seeds_enqueued += 1
    seed_blacklisted = seed_status == "blacklisted"
    if seed_blacklisted:
        phase.errors.append(
            f"{product.pid}/{root_url}: seed зачёркнут блэклистом "
            "(exclude_paths/exclude_documents) — не enqueued"
        )
    # A2 (F3 плана 17): бюджет ``max_documents`` решается **на изъятии**, поэтому
    # ``add_seed`` его больше не проверяет и статуса ``truncated`` не возвращает.
    # Но корень, чей бюджет уже израсходован (resume-зарядка ``note_consumed``), не
    # изымет **ничего**: ``next`` откажет на первом же узле. Это надо назвать сейчас,
    # потому что sitemap/SERP-находки расходуют **общий на продукт** loc-пул
    # (``sitemap_seed_used``): постановка их корнем, который за ними не сойдёт,
    # «съела» бы loc у другого корня того же продукта (F12).
    budget_closed = not frontier.has_capacity
    if budget_closed:
        frontier.mark_truncated("max_documents")
        phase.errors.append(
            f"{product.pid}/{root_url}: discovery не изымается — достигнут max_documents "
            "(усечение на изъятии; ревалидация известных URL бюджет не расходует)"
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
        (seed_blacklisted or budget_closed)
        and revalidation_queued == 0
        and charged_queued == 0
    ):
        operations.emit()
        return frontier, list(sitemap.skipped), root_drain

    # D3: дополнительные depth-0 seeds (``<loc>`` sitemap) — под тем же блэклистом и
    # бюджетом, что и seed оператора (``add_seed`` возвращает статус). Scope-фильтр
    # живёт **ровно в одном месте** — ``fresh_sitemap_seeds`` (тот же ``frontier``):
    # ``extra_seeds`` приходят только оттуда, поэтому повторный scope-чек здесь был бы
    # недостижимой защитой (F7 — удалён; тест на удаление фильтра есть).
    #
    # F12: ``(pid, loc)`` помечается использованным **только при приёме в очередь**
    # (``queued``/``dup`` — loc уже у этого корня). Отказ блэклистом/бюджетом не
    # расходует loc: другой корень того же продукта ещё вправе его взять. Инвариант
    # «один loc — не более одного фетча на продукт» держится именно этой пометкой.
    for extra in (() if (seed_blacklisted or budget_closed) else extra_seeds):
        extra_status = frontier.add_seed(extra)
        if extra_status in ("queued", "dup"):
            phase.sitemap_seed_used.add((product.pid, extra))
            if extra_status == "queued":
                phase.counters.product_seeds_enqueued += 1
        elif extra_status == "blacklisted":
            phase.errors.append(
                f"{product.pid}/{extra}: sitemap-seed зачёркнут блэклистом "
                "(exclude_paths/exclude_documents) — не enqueued"
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
            phase.counters.product_seeds_enqueued += 1
            product.serp_ingest["enqueued"] += 1
        elif found_status == "dup":
            product.serp_ingest["dup"] += 1
        elif found_status == "blacklisted":
            product.serp_ingest["blacklisted"].append(found)
            phase.errors.append(
                f"{product.pid}/{found}: serp-находка зачёркнута блэклистом "
                "(exclude_paths/exclude_documents) — не enqueued"
            )
        # A2 (F3): отказа по ``max_documents`` на постановке больше нет — находка
        # ставится в очередь, а бюджет решается на изъятии. Поэтому усечения на
        # постановке не бывает вовсе, и ключа ``truncated`` в наблюдении нет:
        # «поставлена и не изъята» видно числом ``queue_remaining`` в провенансе
        # корня. Мёртвый ключ (объявлен, но никем не наполнялся) удалён, а не
        # оставлен пустым: пустой список читался бы как наблюдённый факт
        # «усечения не было».

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
        # ``member_pool_dead`` — флаг **этого** корня: объявлен на его уровне, чтобы
        # при чередовании корней мёртвый пул одного не выключал членов соседа.
        nonlocal member_pool_dead
        if (
            defer_extract
            and phase.extraction_pool is not None
            and pool_key is not None
        ):
            # Сужение типа: поле носителя — ``ExtractionPool | None``, а вложенное
            # замыкание ниже читает пул уже **точно** (mypy не сужает атрибуты через
            # границу функции). Локальная ссылка — то же состояние, не копия.
            member_pool = phase.extraction_pool
            #: Пул этого спуска признан мёртвым (``2 × timeout`` без завершений):
            #: остаток членов — видимый пропуск, а не 47 ожиданий по таймауту.
            member_pool_dead = False

            def _submit_member(task: MemberTask) -> Any:
                """Submit извлечения члена; ``MemberOutcome`` — немедленный исход.

                ``split`` проверяется **здесь**, в родителе: у воркера нет признака
                ``supports_source_path``, и без этой проверки шов получил бы локальный
                путь вместо URL (капитуляция была бы невидимой).
                """
                unavailable = member_extract_unavailable(root_extractor)
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
                if phase.pools.abandoned(pool_key):
                    # Как у страниц: пул исчерпал пересборки — inline запрещён
                    # (docling в родителе на memory-constrained хосте = худший режим).
                    return MemberOutcome(
                        skip_reason=SKIP_EXTRACT_FAILED,
                        detail=(
                            "extraction pool exhausted after "
                            f"{_EXTRACT_MAX_POOL_REBUILDS} rebuilds — inline disabled"
                        ),
                    )
                executor = phase.pools.executor(pool_key, _new_extract_pool)
                return executor.submit(
                    member_pool.worker,
                    member_extract_request(
                        task,
                        fetched_at=page.fetched_at,
                        lang=page.lang,
                        level=level,
                        pdf_pipeline=pdf_pipeline,
                        rss_dir=phase.worker_rss_dir,
                    ),
                )

            def _resolve_member(handle: Any, task: MemberTask) -> MemberOutcome:
                """FIFO-доводка задания члена: future → ``ingest_member_doc``.

                Пересборкой мёртвого пула занимается **страничный** путь
                (:func:`finish_extract`): спуск исполняется внутри ``item.commit``,
                где ``root.pending`` ещё включает сам архив, и рекурсивная доводка
                той же фазовой очереди занизила бы/задвоила счётчики корней.

                ``member_pool_dead`` — флаг **этого** корня (см. ``_descend_container``),
                поэтому нужен ``nonlocal``.
                """
                nonlocal member_pool_dead
                try:
                    _wait_started = time.perf_counter()
                    try:
                        extracted = handle.result(timeout=phase.extract_timeout)
                    finally:
                        # Тот же бакет, что и у страничной доводки: ожидание пула
                        # экстракции на главном потоке (член контейнера).
                        phase.timings.extract_wait_s += time.perf_counter() - _wait_started
                except TimeoutError:
                    member_pool_dead = phase.pools.note_timeout(pool_key)
                    return MemberOutcome(
                        skip_reason=SKIP_EXTRACT_FAILED,
                        detail=f"extraction timeout ({phase.extract_timeout} s)",
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
                phase.pools.note_completion(pool_key)
                return ingest_member(
                    task,
                    extractor=root_extractor,
                    chunker=phase.chunker,
                    ingest=product.ingest,
                    seen_docs=product.seen_docs,
                    seen_chunk_ids=product.seen_chunk_ids,
                    errors=phase.errors,
                    store=phase.store,
                    level=level,
                    fetched_at=page.fetched_at,
                    lang=page.lang,
                    artifact_probes=phase.artifact_probes,
                    extracted=extracted,
                    timings=phase.timings,
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
            container_for=phase.container_for,
            ingest_member=lambda task: ingest_member(
                task,
                extractor=root_extractor,
                chunker=phase.chunker,
                ingest=product.ingest,
                seen_docs=product.seen_docs,
                seen_chunk_ids=product.seen_chunk_ids,
                errors=phase.errors,
                store=phase.store,
                level=level,
                fetched_at=page.fetched_at,
                lang=page.lang,
                artifact_probes=phase.artifact_probes,
                timings=phase.timings,
                reuse_kind=reuse_kind,
            ),
            observations=phase.archive_observations,
            members=phase.corpus_member_rows,
            errors=phase.errors,
            run_id=phase.run_id,
            created_at=_now_iso(),
            defer_member=defer_member,
            #: Окно упреждения = окно экстракции корня (``extract_workers``);
            #: без пула окно не используется (inline-путь байт-в-байт).
            window=extract_window or 1,
            #: Write-through (F1): провенанс спуска durable на каждом шаге, а не в
            #: конце фазы. Отказ стора изолируется в ``errors`` синком, поэтому
            #: спуск продолжается (провенанс важнее, падение фазы — хуже).
            persist_observation=persist_observation_sync(phase.store, phase.errors),
            persist_member=persist_member_sync(phase.store, phase.errors),
        )
        observation = walker.descend(
            archive_path=raw_path,
            archive_url=archive_url,
            archive_content_hash=archive_hash,
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

    def _extend_links(page: RawPage, depth: int | None, is_revalidation: bool) -> None:
        """D1: расширение BFS ссылками страницы — общий хвост обоих режимов.

        Вынесено в один носитель ради ``crawl_only`` (F2): режим «краул без конверсии»
        обязан обходить **тот же** frontier, иначе «краул выполнен» означало бы «обошли
        только seeds». При ``max_depth=0`` извлечение не выполняется вовсе (байт-в-байт
        прежнее поведение). 09 §6.3: ревалидация — лист; её ссылки не расширяют
        discovery (иначе глубокая страница, пере-добытая на depth 0, меняла бы
        coverage/стоимость).
        """
        if not is_revalidation and limits.max_depth > 0 and looks_like_html(page):
            frontier.offer(extract_links(page.content, page.url), depth)

    def _enqueue_extract(
        pid: str, work_url: str, page: RawPage, fetch_plan: FetchPlan
    ) -> None:
        """Пишет durable-факт «документ добыт» в ``extract_queue`` (17 §4.1).

        Адрес берётся из элемента сеанса: ``retain`` уже проставил там ``content_hash``
        и ``raw_path``, посчитанные по **retained-байтам**. Пересчёт из ``page.content``
        был бы другим хешем для staged-артефакта (тело в файле, ``content`` пуст) и
        разорвал бы цепочку I19 ещё до конверсии.

        Провенанс **получения** приходит из наблюдённого результата (``page`` и
        ``fetch_plan``), а не из строки сеанса: это ровно те значения, которые ушли бы в
        ``_ExtractRequest`` этого документа (:func:`core.pipeline.corpus_ingest.
        make_extract_request`), поэтому потребитель (F6) воспроизводит условия измерения
        из строки вместо того, чтобы выводить их заново. Иначе тот же документ изъят
        другим маршрутом — другой ``extractor_version``/``doc_version_id`` — то есть
        другой корпус при тех же объявленных входах (I19, A3).

        Отсутствие адреса (``session_id``/элемент без хеша) — не повод молча потерять
        документ: факт уходит в ``errors`` фазы, то есть в ``results[].notes`` и манифест.
        """
        if phase.session_id is None:
            raise ValueError(
                "crawl_only требует активного сеанса краула: без session_id "
                "durable-очередь extract_queue не адресуема"
            )
        item = phase.items.get((pid, work_url))
        content_value = getattr(item, "content_hash", None)
        raw_value = getattr(item, "raw_path", None)
        if not content_value or not raw_value:
            phase.errors.append(
                f"{pid}/{work_url}: extract_queue: у элемента сеанса нет "
                "content_hash/raw_path — документ НЕ поставлен в очередь конверсии"
            )
            return
        phase.store.enqueue_extract_item(
            ExtractQueueItem(
                session_id=phase.session_id,
                product_id=pid,
                url=work_url,
                content_hash=str(content_value),
                raw_path=str(raw_value),
                level=level.value,
                pdf_pipeline=pdf_pipeline.value,
                #: Режим получения — **наблюдённый** (``RawPage.fetch_mode``), а не
                #: объявленный планом: браузерный адаптер вправе доставить байты
                #: HTTP-путём (``crawl_browser.py:668``), и именно это значение несла бы
                #: заявка экстракции (``make_extract_request`` → ``RawPage.fetch_mode``).
                fetch_mode=_as_text(page.fetch_mode),
                #: Ось извлечения — из плана: она объявлена краулом для этого URL и
                #: определяет ``extractor_version`` документа.
                extract_route=_as_text(fetch_plan.extract),
                #: Движок: названный страницей (фактически доставивший байты) — первым,
                #: объявленный осью — если адаптер себя не назвал.
                engine=page.engine or fetch_plan.engine,
                content_type=page.content_type,
                lang=page.lang,
                #: ``RawPage.status_code`` — тот же факт, что ушёл бы в заявку. Для
                #: retained-байтов (``resume``) это ответ **прошлого** цикла: сервер в
                #: этом прогоне не отвечал, но заявка экстракции несла ровно его.
                status_code=page.status_code,
                #: Время получения — штамп страницы: он уже посчитан там, где ответ
                #: получен (``local_fetch``) или восстановлен из элемента сеанса
                #: (``resume_page``: ``last_attempt``, иначе mtime файла). Вырожденный
                #: штамп (пустая строка у resume без mtime) заменяется временем коммита:
                #: это **верхняя граница** момента получения, и она честнее, чем NULL в
                #: колонке, которую контракт очереди требует непустой.
                fetched_at=page.fetched_at or _now_iso(),
                enqueued_at=_now_iso(),
            )
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
        ``ingest_one``; иначе ``finish_one`` с готовым ``ExtractedDoc``/``None`` и
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
            operations.emit()
            return
        # 17 §4.1 (F2): crawl-only — краул кончается durable-фактом. Документ не
        # конвертируется (ни зонда, ни спуска, ни ``ingest_one``/``finish_one``),
        # но кладётся в ``extract_queue``; frontier расширяется тем же кодом ниже
        # (``_extend_links``), поэтому обход остаётся полным.
        if phase.crawl_only:
            _enqueue_extract(pid, work_url, page, fetch_plan)
            operations.emit()
            _extend_links(page, depth, is_revalidation)
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
                container_for=phase.container_for,
            )
            phase.artifact_probes.append(probe.as_dict())
            if probe.is_container:
                # Контейнер — не документ: вместо экстракции исполняется спуск
                # (``archive_descent=plan`` — только листинг и прогноз).
                _descend_container(pid, work_url, page, raw_path, reuse_kind=reuse_kind)
                operations.emit()
                return
        if extracted is _INLINE:
            # 09 §6.3/§9: ``304`` переиспользует версию документа **до**
            # экстракции (экстрактор/чанкер не вызываются). Неудача reuse
            # (нет версии/чанков, I19-расхождение) — видимая запись уже сделана,
            # дальше обычный путь экстракции.
            reuse = operations.reuse_for_page(pid, work_url, page)
            if reuse is not None:
                operations.finish_one(
                    pid,
                    work_url,
                    page,
                    fetch_plan,
                    raw_path,
                    None,
                    product.ingest,
                    product.seen_docs,
                    product.seen_chunk_ids,
                    root_extractor,
                    level,
                    reuse_kind=reuse_kind,
                    reuse=reuse,
                )
            else:
                operations.ingest_one(
                    pid,
                    work_url,
                    page,
                    fetch_plan,
                    raw_path,
                    product.ingest,
                    product.seen_docs,
                    product.seen_chunk_ids,
                    root_extractor,
                    level,
                    reuse_kind=reuse_kind,
                )
        else:
            # 09 §6.3: пул уже извлёк документ (экстракция не отменяется — она
            # оплачена), но при ``304`` нарезка переиспользуется: чанкер не зовётся.
            operations.finish_one(
                pid,
                work_url,
                page,
                fetch_plan,
                raw_path,
                extracted,
                product.ingest,
                product.seen_docs,
                product.seen_chunk_ids,
                root_extractor,
                level,
                extract_error=extract_error,
                extract_failure_kind=extract_failure_kind,
                reuse_kind=reuse_kind,
                reuse=(
                    operations.reuse_for_page(pid, work_url, page)
                    if extract_error is None
                    else None
                ),
            )
        operations.emit()
        # D1: извлечение in-site ссылок и следующий уровень BFS (общий хвост с
        # ``crawl_only`` — см. ``_extend_links``).
        _extend_links(page, depth, is_revalidation)

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
        «готовый текст» (тот же ``finish_one``, что у результата пула), не занимая
        слот воркера и не оплачивая инициализатор. Проба в воркере остаётся защитой;
        ключ родителя и воркера — один и тот же хеш ретенированных байтов.

        Отказ пула снимает ``defer_extract`` **этого корня** (инлайн-fallback): флаг
        объявлен на уровне ``drain_root``, поэтому нужен ``nonlocal``.
        """
        nonlocal defer_extract
        if outcome is None:
            return
        page, raw_path, fetch_plan, freshly_fetched = outcome
        if defer_extract and phase.extract_cache is not None:
            # C1/L4: проба — в родителе, до submit и до проверки исчерпания пула:
            # готовый текст не требует экстракции вовсе. Ключ — ``raw_path.stem``
            # (тот же хеш, что воркер считает из прочитанных байтов).
            cached = phase.extract_cache.get(
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
        if pool_key is not None and phase.pools.abandoned(pool_key):
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

        if pool_key is None:
            # Инвариант ветки: ``defer_extract`` истинно только при окне, а окно и
            # есть ключ пула. Проверка явная — под ``-O`` assert исчез бы и ключ ушёл
            # бы в ``pools.executor`` как ``None``.
            raise AssertionError(
                f"{pid_arg}/{url_arg}: defer_extract без ключа пула "
                "(окно экстракции не задано)"
            )
        try:
            executor = phase.pools.executor(pool_key, _new_extract_pool)
        except Exception as exc:  # noqa: BLE001 — изоляция по контракту фазы
            phase.errors.append(
                f"{pid_arg}/{url_arg}: extract pool: "
                f"{type(exc).__name__}: {exc} — inline"
            )
            defer_extract = False
            _commit_outcome(pid_arg, url_arg, depth_arg, outcome)
            return
        try:
            future = executor.submit(
                phase.extraction_pool.worker,  # type: ignore[union-attr]
                _make_extract_request(page, raw_path, fetch_plan),
            )
        except Exception as exc:  # noqa: BLE001 — непиклящийся вход/сбой submit
            phase.errors.append(
                f"{pid_arg}/{url_arg}: extract submit: "
                f"{type(exc).__name__}: {exc}"
            )
            _commit_outcome(pid_arg, url_arg, depth_arg, outcome)
            return
        root_drain.pending += 1
        phase.extract_pending.append(
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
        # Окно: не более ``W`` экстракций **этого** корня «в полёте» (доводка FIFO).
        # Счёт по ``item.root``: ``len(extract_pending)`` считал бы и чужие задания, и
        # корень ждал бы окна, которого не использовал (при полосах это сериализовало
        # бы корни на пуле экстракции и подменило окно памяти максимумом по корням).
        while (
            corpus_commit.pending_count_for_root(
                extract_pending=phase.extract_pending, root_drain=root_drain
            )
            >= extract_window
        ):
            operations.finish_extract(
                popleft_for_root(
                    extract_pending=phase.extract_pending, root_drain=root_drain
                )
            )
    # D4: ограниченный пул ТОЛЬКО на сетевой fetch локального пути. Пул создаёт
    # **драйвер полосы** (``lane_width`` — одна формула на обоих), потому что полоса
    # живёт дольше одного корня: при чередовании одновременно существуют не более
    # ``host_concurrency`` пулов фетча, а не по пулу на корень. В пул идут лишь
    # work-items с живым HTTP-фетчем; браузерные элементы — никогда (см.
    # ``local_prefetch`` и ``_dispatch_wave``).
    max_workers = limits.max_workers
    pool = phase.fetch_pool
    non_poolable_pool = phase.browser_pool
    if lane_width(limits, executor=phase.executor) is not None and (
        pool is None or non_poolable_pool is None
    ):
        # Не тихая деградация: без пула живой фетч пошёл бы inline на главном потоке и
        # полосы схлопнулись бы в одну без единой ошибки в артефактах.
        raise AssertionError(
            f"{product.pid}/{root_url}: полоса без пула фетча при max_workers="
            f"{max_workers} — пул обязан создать драйвер"
        )
    # F4b: шаги корня — генератор. Отправку волны и ожидание исполняет драйвер
    # (:mod:`core.pipeline.corpus_driver`): «отправить и дождаться» не может быть
    # работой корня, потому что допуск волны (право на host'ы) решается до отправки.
    return (yield from root_steps(
        product.pid,
        root_url,
        frontier,
        root_drain=root_drain,
        limits=limits,
        items=phase.items,
        errors=phase.errors,
        emit=operations.emit,
        mark=operations.mark,
        commit_local=operations.commit_local,
        local_prefetch=operations.local_prefetch,
        process_local=operations.process_local,
        submit_collect=operations.submit_collect,
        work_item=operations.work_item,
        resolve_target=operations.resolve_target,
        finish_extract=operations.finish_extract,
        extract_pending=phase.extract_pending,
        extract_window=extract_window,
        max_attempts=phase.max_attempts,
        max_waves=phase.max_waves,
        wave_items=phase.wave_items,
        robots_enabled=robots_enabled,
        timings=phase.timings,
        executor=phase.executor,
        pool=pool,
        max_workers=max_workers,
        defer_tail=defer_tail,
        sitemap_skips=sitemap.skipped,
        _drain_for_window=_drain_for_window,
        _has_fetch_capacity=_has_fetch_capacity,
        _inflight_fresh=_inflight_fresh,
        _tail_may_extend_frontier=_tail_may_extend_frontier,
        _skip_document_prefetch=_skip_document_prefetch,
        _commit_or_defer=_commit_or_defer,
    ))
