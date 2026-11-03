"""Durable-субстрат фазы корпуса: версии документов, элементы сеанса, границы продуктов.

Модуль — часть декомпозиции ``corpus.py`` (план
``docs/reports/2026-10-30-large-modules-refactoring-plan.md``, L5 шаг 1). Из тела
:func:`core.pipeline.corpus._build_corpus` вынесены функции, у которых **нет** зависимостей
от других замыканий тела: они читают только данные фазы (порт состояния, стор, счётчики,
списки ошибок) и потому становятся обычными функциями модуля с явными параметрами.
Замыкания, которые ходят друг в друга (``_mark``/``_retain``/``_ingest_one``/``_finish_one``),
остались в теле — это следующий шаг.

**Почему это лист.** Модуль не импортирует ``core.pipeline.corpus`` — иначе получился бы
цикл (``corpus`` → этот модуль → ``corpus``), и перенос обрушил бы импорт. Поэтому всё,
что нужно общему коду с обеих сторон (dataclass-ы переиспользования, канонизация класса
сбоя, режим продукта), живёт **здесь**, а ``corpus.py`` берёт это отсюда.

**Почему имена публичные.** Имя без ведущего подчёркивания через границу модуля — норма;
приватное имя, импортируемое снаружи, — дефект, который в L1 уже убирали из ``wiring.py``.
Поэтому ``_save_run_product`` → :func:`save_run_product` и т. д. Исключение — константы
``_DOC_ACTIVE``/``_DOC_SUPERSEDED``: они нужны только внутри этого модуля и наружу не
выходят. :func:`canonical_failure_kind` публична: её зовёт и ``_record_url_state``,
оставшийся в теле ``_build_corpus``.

**Порядок записи не меняется.** Тела перенесены дословно; изменились только способы
получить то, что раньше было свободной переменной замыкания. Оракул —
``tests/pipeline/test_corpus_write_order.py`` (последовательность SQL-операторов).
"""

from __future__ import annotations

import contextlib
import datetime as _dt
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from core.config.domain_profiles import DomainProfile
from core.domain import (
    CrawlItemStatus,
    CrawlSessionItem,
    FailureKind,
    ProductMode,
    RunProductStatus,
)

# ``_FIELD_SEP`` — объявленный разделитель склейки идентификаторов (L3, ``corpus_index``):
# тот же символ обязан использоваться и здесь, иначе дайджест набора ``chunk_id`` разошёлся
# бы с тем, что считает сборка индекса.
from core.pipeline.corpus_index import _FIELD_SEP
from core.pipeline.extraction_runtime import _as_text
from core.pipeline.fetch_plan import select_domain_profile
from core.pipeline.incremental import chunk_row_id
from core.pipeline.liveness import Heartbeat
from core.pipeline.manual_artifacts import ManualArtifact, artifact_record
from core.ports.state import ChunkRow, DocumentVersionRow, ProductStatePort
from core.ports.types import RawPage
from core.util.hashing import content_hash, short_hash

#: Статус активной версии документа (``document_version.status``, 09 §4.2).
_DOC_ACTIVE = "active"
#: Статус вытесненной версии: документ больше не наблюдается по URL (``404``/``410``, 09 §6.3).
_DOC_SUPERSEDED = "superseded"

#: P0/OOM: явно очистить carry-forward поле элемента сеанса (``None`` означает «не менять»).
#: Публичное имя, потому что значение передаётся из ``corpus.py`` в :func:`mark_session` —
#: это часть контракта функции, а не её внутренность.
CLEAR_SESSION_FIELD = object()


def _now_iso() -> str:
    """Текущее время в ISO-8601 (UTC) — для провенанса элементов сеанса (L1a).

    Локальная копия по конвенции проекта: ``_now_iso`` объявлена независимо в
    ``crawl_session``, ``gates``, ``sqlite_store`` и адаптерах краула. Общий модуль времени
    ради двух строк не вводится.
    """
    return _dt.datetime.now(_dt.UTC).isoformat()


def canonical_failure_kind(reason: str | None) -> str:
    """Единый маппинг любого ``error_kind``-значения в канонический :class:`FailureKind`.

    Все записи ``crawl_session_items`` идут через :func:`mark_session`, который вызывает
    этот helper: ad-hoc-литералы (``redirect_out_of_scope``) и ``refusal_reason`` адаптеров
    (``robots_disallow``) канонизируются в одном месте, а не в каждом write-site.
    Неизвестное значение не выдумывает класс: это ``source_unavailable`` (честный дефолт).
    """
    if not reason:
        return FailureKind.SOURCE_UNAVAILABLE.value
    try:
        return FailureKind(reason).value
    except ValueError:
        return FailureKind.SOURCE_UNAVAILABLE.value


def as_product_mode(value: str | None) -> ProductMode:
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


@dataclass(frozen=True)
class ReusedChunk:
    """Переиспользованный чанк документа: текст уже верифицирован по I19.

    ``content_hash`` — hash **чистого текста** из ``chunk_store``; он сверен с
    ``chunk_meta.content_hash`` при загрузке (:func:`load_reused_chunks`), поэтому
    неверифицированный текст сюда не попадает.
    """

    chunk_no: int
    text: str
    content_hash: str
    heading_path: str
    context_prefix: str
    #: Смещения куска в тексте документа (L1b-2b). ``None``/``None`` — кусок не является
    #: точным срезом (абзацный чанкер). Переиспользование обязано вернуть провенанс
    #: ровно тем, чем он был записан: ``0``/``0`` здесь означало бы «кусок документа
    #: нулевой длины в начале», то есть выдуманный факт.
    char_start: int | None = None
    char_end: int | None = None


@dataclass(frozen=True)
class DocReuse:
    """Готовая к переиспользованию версия документа + её проверенный chunk-set.

    :param doc_version: строка ``document_version`` (``active``), чей контент совпал.
    :param chunks: чанки в порядке ``chunk_no``; непусты (пустой набор → не reuse).
    """

    doc_version: DocumentVersionRow
    chunks: tuple[ReusedChunk, ...]


def load_reused_chunks(
    state: ProductStatePort,
    doc_version: DocumentVersionRow,
    *,
    chunker_version: str,
    where: str,
    errors: list[str],
) -> tuple[ReusedChunk, ...] | None:
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
    reused: list[ReusedChunk] = []
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
            ReusedChunk(
                chunk_no=int(row.chunk_no),
                text=text,
                content_hash=row.content_hash,
                heading_path=row.heading_path or "",
                context_prefix=row.context_prefix or "",
                # Смещения — из durable-записи как есть; ``None`` остаётся ``None``.
                char_start=row.char_start,
                char_end=row.char_end,
            )
        )
    return tuple(reused)


def count_doc(
    pid: str,
    *,
    reused: bool,
    state: ProductStatePort | None,
    doc_counters: dict[str, dict[str, int]],
) -> None:
    """Считает принятый документ прогона (``total``/``new``/``reused``, 09 §6.3).

    Считается **документ**, а не URL: повтор/алиас (дедуп ``seen_docs``) не создаёт
    записи в корпусе и сюда не попадает.
    """
    if state is None:
        return
    bucket = doc_counters.setdefault(pid, {"total": 0, "new": 0, "reused": 0})
    bucket["total"] += 1
    bucket["reused" if reused else "new"] += 1


def find_doc_version(
    pid: str,
    doc_hash: str,
    extractor_token: str,
    level_text: str,
    *,
    state: ProductStatePort | None,
    errors: list[str],
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


def reuse_plan(
    pid: str,
    url: str,
    doc_version: DocumentVersionRow | None,
    *,
    state: ProductStatePort | None,
    errors: list[str],
    chunker_version: str,
) -> DocReuse | None:
    """Проверенный chunk-set версии документа либо ``None`` (не переиспользуем)."""
    if state is None or doc_version is None:
        return None
    if doc_version.status != _DOC_ACTIVE:
        return None
    chunks = load_reused_chunks(
        state,
        doc_version,
        chunker_version=chunker_version,
        where=f"{pid}/{url}",
        errors=errors,
    )
    if chunks is None:
        return None
    return DocReuse(doc_version=doc_version, chunks=chunks)


def supersede_document(
    pid: str,
    url: str,
    dv_id: str | None,
    *,
    state: ProductStatePort | None,
    errors: list[str],
) -> None:
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


def write_document_state(
    pid: str,
    url: str,
    page: RawPage,
    *,
    dv_id: str,
    doc_hash: str,
    extractor_token: str,
    level_text: str,
    raw_path: Path,
    chunks: Sequence[tuple[int, str, str, str, int | None, int | None]],
    partial_fields: Mapping[str, Any],
    partial_known: bool,
    state: ProductStatePort | None,
    errors: list[str],
    partiality_changes: list[dict[str, Any]],
    chunker_version: str,
    run_id: str | None,
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

    **Частичность (v5).** ``partial_fields``/``partial_known`` — признак, пришедший из
    извлечения (или восстановленный из этой же строки при переиспользовании). Он
    пишется вместе с версией, потому что переиспользование (``304``) — единственное
    место, где признак нельзя пере-наблюдать, и до v5 там молча подставлялось
    «полное». ``partial_known=False`` пишет ``NULL`` во все четыре колонки: «не
    измерено» не превращается в «полное».

    Смена признака при записи — видимый факт (``partiality_changes``), а не тихая
    перезапись: «было частично — стало полное» означает, что тот же ключ
    ``(байты, движок, уровень)`` дал другой объём работы (пере-извлечение, смена
    окружения), и молчать об этом значило бы терять деградацию так же, как её потерю.

    :param chunks: ``(chunk_no, text, heading_path, context_prefix, char_start,
        char_end)`` принятых чанков. Смещения пишутся в ``chunk_meta`` как есть:
        переиспользование chunk-set обязано вернуть тот же провенанс, иначе документ
        из повторного прогона потерял бы связь куска с его местом в тексте.
    """
    if state is None:
        return
    try:
        now = _now_iso()
        existing = state.get_document_version(dv_id)
        previous_partial = (
            None if existing is None else existing.extract_partial
        )
        new_partial = (
            None if not partial_known else bool(partial_fields.get("partial"))
        )
        if (
            partial_known
            and previous_partial is not None
            and bool(previous_partial) != bool(new_partial)
        ):
            partiality_changes.append(
                {
                    "product_id": pid,
                    "url": url,
                    "doc_version_id": dv_id,
                    "was": "partial" if previous_partial else "full",
                    "now": "partial" if new_partial else "full",
                    "was_pages_converted": getattr(
                        existing, "extract_pages_converted", None
                    ),
                    "was_page_count": getattr(existing, "extract_page_count", None),
                    "now_pages_converted": partial_fields.get("pages_converted"),
                    "now_page_count": partial_fields.get("page_count"),
                }
            )
        run_token = str(run_id or "")
        rows: list[tuple[ChunkRow, str]] = []
        for (
            chunk_no,
            text,
            heading_path,
            context_prefix,
            char_start,
            char_end,
        ) in chunks:
            row = ChunkRow(
                chunk_id=chunk_row_id(dv_id, chunker_version, chunk_no),
                doc_version_id=dv_id,
                product_id=pid,
                chunk_no=chunk_no,
                content_hash=content_hash(text),
                chunker_version=chunker_version,
                heading_path=heading_path or None,
                context_prefix=context_prefix or None,
                lang=page.lang,
                char_start=char_start,
                char_end=char_end,
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
                chunker_version=chunker_version,
                chunk_ids_digest=digest,
                # v5: частичность — измеренное свойство версии. При неизвестном
                # признаке в колонках остаётся NULL (не 0): «полное» здесь было бы
                # утверждением, которого никто не делал.
                extract_partial=new_partial,
                extract_partial_reason=(
                    partial_fields.get("partial_reason")
                    if new_partial
                    else None
                ),
                extract_pages_converted=(
                    partial_fields.get("pages_converted") if new_partial else None
                ),
                extract_page_count=(
                    partial_fields.get("page_count") if partial_known else None
                ),
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


def mark_session(
    pid: str,
    u: str,
    status: CrawlItemStatus,
    *,
    session: str | None,
    store: Any,
    items: dict[tuple[str, str], CrawlSessionItem],
    targets: dict[tuple[str, str], Any],
    applied_engines: dict[tuple[str, str], str | None],
    errors: list[str],
    heartbeat: Heartbeat,
    **fields: Any,
) -> None:
    """Помечает исход URL и (при активном сеансе) персистит элемент краула (L1a).

    Пишет **полную** строку ``crawl_session_items``: поля предыдущего элемента
    переносятся (carry-forward), переданные не-``None`` поля перекрывают. Счётчики
    сеанса поддерживаются стором транзакционно. Сбой изолируется в ``errors``; без
    сеанса durable-краул выключен — строка не пишется (наблюдаемость идёт из
    ``IndexBuild``-счётчиков, которые при отсутствии сеанса нулевые).

    Phase 2.1/S1: ``engine`` — **применённый** транспорт (``applied_engines``), а не
    запрошенный ``target.engine``; откат на ``target.engine`` при отсутствии записи.

    Состояние фазы (сеанс, стор, кэши элементов) приходит параметрами: функция вынесена из
    тела ``_build_corpus`` и свободных переменных замыкания больше не читает.
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
        # 17 §6 (F3): носитель канонического адреса переносится как любое другое поле
        # элемента: ``retain`` пишет строку **до** извлечения (``doc_hash`` ещё нет), и
        # последующая пометка исхода не должна стирать уже известный ключ дедупа.
        "doc_hash": base.doc_hash if base is not None else None,
        "raw_path": base.raw_path if base is not None else None,
        "error_kind": base.error_kind if base is not None else None,
        "error": base.error if base is not None else None,
        "first_seen": (base.first_seen if base is not None and base.first_seen else now),
        "last_attempt": now,
        # 17 §6 (F6): замер попытки переносится, пока это **та же** попытка получения
        # (повторная пометка того же исхода: ``retain`` → ``finish_one`` с ``doc_hash``)
        # либо исход — отказ **извлечения** на уже добытых байтах. Новая попытка (статус
        # сменился) замер не наследует: чужое время под своей меткой — ложь о приборе.
        # Явно переданные значения перекрывают перенос (``fields`` ниже).
        "fetch_started_at": (
            base.fetch_started_at
            if base is not None
            and (
                base.status is status
                or base.status is CrawlItemStatus.FETCHED
            )
            else None
        ),
        "fetch_ms": (
            base.fetch_ms
            if base is not None
            and (
                base.status is status
                or base.status is CrawlItemStatus.FETCHED
            )
            else None
        ),
    }
    for name, value in fields.items():
        if value is CLEAR_SESSION_FIELD:
            record[name] = None
            continue
        if value is not None:
            if name == "error_kind":
                # Ф0: единственная воронка записи ``crawl_session_items`` — здесь
                # канонизируем класс сбоя, чтобы persisted-значение всегда было
                # членом FailureKind (или source_unavailable для неизвестного).
                value = canonical_failure_kind(value)
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


def save_run_product(
    pid: str,
    *,
    store: Any,
    run_id: str | None,
    run_scope: str,
    product_modes: Mapping[str, str] | None,
    run_products_written: set[str],
    errors: list[str],
    status: RunProductStatus,
    coverage_complete: bool | None = None,
    docs_total: int = 0,
    docs_new: int = 0,
    docs_reused: int = 0,
    error_kind: str | None = None,
    error: str | None = None,
) -> None:
    """Пишет строку ``run_product`` на границе продукта (16 §3.2).

    Одна запись на продукт, вызывается дважды: ``running`` перед Pass A и финальный
    статус после Pass B (образец — :meth:`save_corpus_coverage`). Числа берутся из
    того, что фаза уже посчитала (``ingest.docs``/``doc_counters``/``CorpusCoverage``),
    а не считаются заново: второй расчёт той же величины разошёлся бы с первым.

    ``cells_total``/``cells_pending`` не передаются (остаются ``0``): на границе
    ``corpus`` ячеек ещё нет — их считает Ш6. Колонка означает «на границе corpus
    столько», и переписывать её из другой фазы нельзя (16 §3.2).

    Сбой записи изолируется в ``errors`` (как ``_persist_docs``): потерянная строка
    наблюдаемости хуже падения уже собранного корпуса, но и молчать нельзя.
    """
    if store is None or not run_id:
        return
    try:
        store.save_run_product(
            str(run_id),
            pid,
            mode=as_product_mode((product_modes or {}).get(pid)).value,
            status=status,
            scope=run_scope,
            phase="corpus",
            coverage_complete=coverage_complete,
            docs_total=docs_total,
            docs_reused=docs_reused,
            docs_new=docs_new,
            error_kind=error_kind,
            error=error,
        )
    except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
        errors.append(f"run_product({pid}): {type(exc).__name__}: {exc}")
    else:
        run_products_written.add(pid)


def manual_host_scope(
    product_id: str,
    *,
    url_map: Mapping[str, list[str]],
    domain_profiles: Mapping[str, DomainProfile] | None,
) -> tuple[frozenset[str], frozenset[str]]:
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


def persist_manual_audit(
    entry: ManualArtifact,
    *,
    doc_id: str | None,
    raw_path: Path | None,
    store: Any,
    run_id: str | None,
    errors: list[str],
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


def emit_progress(
    *,
    heartbeat: Heartbeat,
    store: Any,
    session: str | None,
    progress: Callable[[dict[str, int]], None] | None,
) -> None:
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
    # Сбой колбэка ``--follow`` не роняет дренаж: прогресс наблюдаем, но не измеряем.
    # Оператор увидит потерю живых строк и итоговые счётчики фазы/продукта в отчёте.
    with contextlib.suppress(Exception):
        progress(dict(store.crawl_session_counts(session)))


def emit_product(
    phase: str,
    product_id: str,
    *,
    on_product: Callable[[str, str], None] | None,
) -> None:
    """Живая граница продукта для ``on_product`` (``--follow``).

    В отличие от ``log_entries`` (материализуются в конце фазы), этот канал
    срабатывает **во время** дренажа: оператор видит, что продукт начат, а не
    гадает «потерян или ещё в очереди». Ошибки колбэка изолируются.
    """
    if on_product is None:
        return
    # Сбой колбэка границы продукта тоже не роняет фазу: продукт дренируется дальше,
    # а его исход виден в ``errors``/провенансе и в ``crawl_product`` итогового отчёта.
    with contextlib.suppress(Exception):
        on_product(phase, product_id)


def sitemap_url_for(
    seed_url: str, *, domain_profiles: Mapping[str, DomainProfile] | None
) -> str | None:
    """URL sitemap из лучшего per-site профиля seed-URL (``DomainProfile.sitemap``)."""
    profile = select_domain_profile(seed_url, domain_profiles or {})
    if profile is None:
        return None
    value = getattr(profile, "sitemap", None)
    return str(value) if value else None
