"""coHound — чистые row-конвертеры хранилища (L4): ``sqlite3.Row`` → модель/словарь.

Здесь живут только функции **без соединения и без схемы**: на входе — строка, которую
вернул ``sqlite3.Row``, на выходе — доменная модель или словарь-носитель. Ни одна функция
этого модуля не читает и не пишет в БД, не знает о транзакциях и не трогает ``SqliteStore``
(иначе получился бы цикл импорта ``sqlite_store → sqlite_rows → sqlite_store``).

Разделение введено ради читаемости: ``sqlite_store`` (L3) отвечает за SQL, транзакции и
границы записей, а конверсия строки — за восстановление модели, и она же единственная
точка, где решается «колонки нет» против «значение не записано» (read-only БД прошлой
версии). Тела функций перенесены из ``sqlite_store`` дословно; методы класса остались
тонкими обёртками, поэтому публичный фасад ``StorePort``/``GateStore`` не менялся.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from core.domain import (
    AbsenceRuleType,
    ArchiveDescent,
    ArchiveDescentStatus,
    ArchiveObservation,
    Cell,
    CellEvidence,
    CellState,
    CorpusCoverage,
    CrawlItemStatus,
    CrawlSession,
    CrawlSessionItem,
    CrawlSessionStatus,
    EvidenceScope,
    EvidenceType,
    ExtractQueueItem,
    ExtractQueueState,
    MeasurementOrigin,
    ResolvedSource,
    SourceKind,
    StaleReason,
    Status,
)
from core.ports.types import Chunk


def _as_text(value: Any) -> Any:
    """Приводит enum-значение к ``.value``; прочее возвращает без изменения."""
    if hasattr(value, "value") and not isinstance(value, (str, int, float, bool)):
        return value.value
    return value


def _as_json_text(value: Any) -> str | None:
    """JSON-текст для сложных полей; ``str``/``None`` возвращаются как есть.

    Позволяет как принимать уже сериализованный ``retrieval_params_json`` (модель ``Cell``),
    так и dict/list из вызывающего кода.
    """
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _parse_json_text(value: str | None) -> Any:
    """Разбирает JSON-текст; при ``None``/некорректном значении — ``None``."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def _has_column(row: sqlite3.Row, column: str) -> bool:
    """Есть ли колонка ``column`` в строке ``row``.

    ``sqlite3.Row`` — **не** ``dict``: у типа нет ``__contains__``, поэтому ``in row``
    перебирает *значения* строки, а не имена колонок. Проверка имени обязана идти по
    ``row.keys()``: ``"reason" in row`` было бы истинно только при совпадении *значения*
    с этой строкой, то есть почти всегда ложно — и «колонки нет» превратилось бы в
    «значение не записано» для любой существующей колонки.
    """
    return column in row.keys()  # noqa: SIM118 - sqlite3.Row: `in row` смотрит значения


def _row_optional(row: sqlite3.Row, column: str) -> Any:
    """Значение колонки или ``None``, если колонки в строке нет.

    Нужно для **read-only** открытия старой БД: ``connect_read_only`` не выполняет
    аддитивных DDL-миграций (файл только читается), поэтому колонка, добавленная позже,
    в такой БД отсутствует. Отсутствие = «значение не записано», а не сбой чтения прогона.
    """
    return row[column] if _has_column(row, column) else None


def _notes_from_text(raw: str | None) -> list[str]:
    """Разбирает ``cells.validation_notes`` в список заметок (терпимо к legacy-тексту).

    Новый писатель кладёт JSON-массив. Строка, записанная иначе (руками, прошлой
    редакцией), не должна теряться: она читается как одна заметка, а не как «пусто».
    """
    if raw is None:
        return []
    payload = _parse_json_text(raw)
    if isinstance(payload, list):
        return [text for item in payload if (text := str(item).strip())]
    text = str(raw).strip()
    return [text] if text else []


def _parse_absence_rule(raw: Any) -> AbsenceRuleType | None:
    """Правило отсутствия из колонки ``cells.absence_rule``.

    ``NULL``/отсутствующая колонка (legacy-строка, read-only БД старой версии) → ``None``:
    «правило не записано». Прежнее чтение подставляло ``explicit``, то есть **выдумывало**
    объявление каталога, и I2 по прочитанным ячейкам проверялся на выдуманном значении.
    Неизвестное значение в колонке — тоже ``None``: терпимость чтения прогона важнее,
    чем отказ открыть старую БД.
    """
    if raw is None:
        return None
    try:
        return AbsenceRuleType(str(raw))
    except ValueError:
        return None


def _parse_measurement_origin(raw: Any) -> MeasurementOrigin:
    """Происхождение ячейки из колонки ``cells.measurement_origin`` (09 §10).

    ``NULL``/отсутствующая колонка (read-only БД прошлой версии) → ``observed``: строка
    действительно была измерена, переноса тогда не существовало. Неизвестное значение —
    тоже ``observed`` по той же причине, по которой чтение прогона терпимо к историческим
    строкам: ``carried`` без ``carried_from_run_id`` модель всё равно отвергнет, поэтому
    подстановка дефолта не может выдать перенос за измерение.
    """
    if raw is None:
        return MeasurementOrigin.OBSERVED
    try:
        return MeasurementOrigin(str(raw))
    except ValueError:
        return MeasurementOrigin.OBSERVED


def _parse_evidence_scope(raw: str | None) -> EvidenceScope | None:
    """Разбирает JSON-колонку ``cells.evidence_scope`` (Вариант 2).

    Терпимость к повреждённому/историческому значению: ``NULL``, невалидный JSON или
    рассинхронизированная схема дают ``None`` («объекта нет»). Это прежнее правило
    диагностики, а не падение чтения прогона: отчёт по старому прогону обязан работать.
    Производная ``url_observation_ratio``, если она записана в артефакт, игнорируется
    (``extra="forbid"`` не применяется — ключ вырезается до валидации) и пересчитывается
    из ``observed``/``expected``, поэтому разойтись с ними не может.
    """
    payload = _parse_json_text(raw)
    if not isinstance(payload, Mapping):
        return None
    data = dict(payload)
    data.pop("url_observation_ratio", None)
    try:
        return EvidenceScope.model_validate(data)
    except ValueError:
        return None


def _cell_row(
    cell: Cell, validation_notes: str | None
) -> tuple[Any, ...]:
    """Строка ``cells`` в порядке ``sqlite_store._CELL_COLUMNS``.

    ``validation_notes`` передаётся **готовым текстом** (уже слитым с записанным),
    а не берётся из модели: слияние требует чтения БД, и делать его в «чистой»
    сборке строки значило бы либо потерять накопление, либо сходить в БД из
    канонического сериализатора.
    """
    return (
        cell.run_id,
        cell.product_id,
        cell.param_id,
        _as_text(cell.state),
        _as_text(cell.status),
        _as_text(cell.source_kind),
        cell.reviewed_by,
        cell.index_build_id,
        cell.retrieval_profile,
        _as_json_text(cell.retrieval_params_json),
        _as_text(cell.stale_reason),
        cell.review_reason,
        (
            _as_json_text(cell.evidence_scope.to_json())
            if cell.evidence_scope is not None
            else None
        ),
        _as_text(cell.absence_rule),
        validation_notes,
        _as_text(cell.measurement_origin),
        cell.carried_from_run_id,
        cell.cell_reuse_key,
    )


def _row_to_cell(row: sqlite3.Row) -> Cell:
    """Восстанавливает ``Cell`` из строки без повторной валидации инвариантов.

    Инварианты I1/I2/I3 зависят от ``evidence``/``absence_evidence``/``evidence_scope``,
    которые живут в ``cell_evidence`` (или в JSON-колонке), а не в самой строке
    ``cells``. Строка уже прошла валидацию и триггеры БД при записи, поэтому
    ``model_construct`` избегает ложного срабатывания инвариантов при чтении.

    ``evidence_scope`` разбирается **терпимо**: повреждённый JSON исторической строки —
    это «объекта нет» (прежнее правило), а не падение чтения всего прогона.

    ``absence_rule`` **не достраивается**: ``NULL`` (строка записана до появления
    колонки, или read-only БД старой версии без неё) читается как ``None`` — «правило
    не записано». Прежнее жёсткое ``explicit`` означало, что любая прочитанная ячейка
    заявляла правило, которого каталог не объявлял, и I2 по прочитанным данным
    проверялся на выдуманном значении (``review.promote`` работает именно с
    прочитанными ячейками). Доменные проверки :mod:`core.domain.invariants`
    ``None``-значение допускают: I2/I2b/I2c решаются классом отрицания
    (``evidence_scope``), а не этим полем.
    """
    status = row["status"]
    source_kind = row["source_kind"]
    stale_reason = row["stale_reason"]
    return Cell.model_construct(
        run_id=row["run_id"],
        product_id=row["product_id"],
        param_id=row["param_id"],
        state=CellState(row["state"]),
        status=Status(status) if status is not None else None,
        source_kind=SourceKind(source_kind) if source_kind is not None else None,
        absence_rule=_parse_absence_rule(_row_optional(row, "absence_rule")),
        evidence=[],
        absence_evidence=[],
        evidence_scope=_parse_evidence_scope(_row_optional(row, "evidence_scope")),
        reviewed_by=row["reviewed_by"],
        index_build_id=row["index_build_id"],
        retrieval_profile=row["retrieval_profile"],
        retrieval_params_json=row["retrieval_params_json"],
        stale_reason=StaleReason(stale_reason) if stale_reason is not None else None,
        review_reason=row["review_reason"],
        validation_notes=_notes_from_text(
            _row_optional(row, "validation_notes")
        ),
        # 09 §10: происхождение ячейки. Отсутствующая колонка (read-only БД прошлой
        # версии) — «не записано», и это **observed**: строка была измерена, переноса
        # тогда не существовало. ``carried`` без источника не конструируется (модель
        # отвергает такую пару), поэтому подстановка дефолта не создаёт ложного переноса.
        measurement_origin=_parse_measurement_origin(
            _row_optional(row, "measurement_origin")
        ),
        carried_from_run_id=_row_optional(row, "carried_from_run_id"),
        cell_reuse_key=_row_optional(row, "cell_reuse_key"),
    )


def _evidence_row(ev: CellEvidence) -> tuple[Any, ...]:
    return (
        ev.evidence_id,
        ev.run_id,
        ev.product_id,
        ev.param_id,
        _as_text(ev.source_kind),
        _as_text(ev.evidence_type),
        ev.source_id,
        ev.source_url,
        ev.citation_quote,
        ev.content_hash,
        _as_text(ev.source_scope),
        ev.scope_acceptance,
        ev.index_build_id,
        ev.partition_build_id,
        # O5: адрес версии документа (``dv_...``) — из ``chunk_id`` чанка-цитаты, не из
        # ``content_hash``. ``None`` — версия не установлена (не выдумываем).
        ev.doc_version_id,
        ev.retrieved_at,
    )


def _absence_evidence_row(entry: Mapping[str, Any]) -> tuple[Any, ...]:
    """Строка ``cell_evidence`` из записи absence-evidence (dict → колонки).

    ``evidence_type`` фиксирован (``absence``), ``partition_build_id``/``retrieved_at``
    остаются ``NULL``: у протокола отсутствия нет ни чанка-партиции, ни момента
    извлечения. ``source_id``/``content_hash``/``source_kind`` — ``NULL``, когда
    отсутствие синтетическое (цитировать нечего); это законно, PK — ``evidence_id``.
    ``doc_version_id`` берётся из записи (O5): у цитаты, попавшей в протокол, версия
    известна, у синтетической записи протокола её нет — тогда ``NULL``.
    """
    return (
        entry["evidence_id"],
        entry["run_id"],
        entry["product_id"],
        entry["param_id"],
        _as_text(entry.get("source_kind")),
        EvidenceType.ABSENCE.value,
        entry.get("source_id"),
        entry.get("source_url"),
        entry.get("citation_quote"),
        entry.get("content_hash"),
        _as_text(entry.get("source_scope")),
        entry.get("scope_acceptance"),
        entry.get("index_build_id"),
        None,
        entry.get("doc_version_id"),
        None,
    )


def _row_to_absence(row: sqlite3.Row) -> dict[str, Any]:
    """Строка ``cell_evidence`` → запись ``absence_evidence`` (носитель — словарь).

    Ключи совпадают с тем, что кладёт в ячейку Ш6 (``evaluate._absence_dict`` /
    протокол отсутствия), поэтому читатель приклеивает запись к ``Cell`` как есть.
    ``doc_version_id`` (O5) читается терпимо: в БД прошлой версии колонки нет — тогда
    «версия не записана» (``None``), а не сбой чтения.
    """
    return {
        "evidence_id": row["evidence_id"],
        "run_id": row["run_id"],
        "product_id": row["product_id"],
        "param_id": row["param_id"],
        "source_id": row["source_id"],
        "source_url": row["source_url"],
        "citation_quote": row["citation_quote"],
        "content_hash": row["content_hash"],
        "source_kind": row["source_kind"],
        "evidence_type": EvidenceType.ABSENCE.value,
        "source_scope": row["source_scope"],
        "scope_acceptance": row["scope_acceptance"],
        "index_build_id": row["index_build_id"],
        "doc_version_id": _row_optional(row, "doc_version_id"),
    }


def _row_to_crawl_session(row: sqlite3.Row) -> CrawlSession:
    """Восстанавливает ``CrawlSession`` из строки ``crawl_sessions``."""
    return CrawlSession(
        session_id=row["session_id"],
        run_id=row["run_id"],
        profile=row["profile"],
        status=CrawlSessionStatus(row["status"]),
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        total=int(row["total"] or 0),
        pending=int(row["pending"] or 0),
        running=int(row["running"] or 0),
        fetched=int(row["fetched"] or 0),
        failed=int(row["failed"] or 0),
        cancelled=int(row["cancelled"] or 0),
        error=row["error"],
        options_json=row["options_json"],
        provenance_json=row["provenance_json"],
        owner=row["owner"],
        heartbeat_at=row["heartbeat_at"],
    )


def _row_to_crawl_item(row: sqlite3.Row) -> CrawlSessionItem:
    """Восстанавливает ``CrawlSessionItem`` из строки ``crawl_session_items``."""
    attempts = row["attempts"]
    return CrawlSessionItem(
        session_id=row["session_id"],
        product_id=row["product_id"],
        url=row["url"],
        status=CrawlItemStatus(row["status"]),
        attempts=0 if attempts is None else int(attempts),
        depth=None if row["depth"] is None else int(row["depth"]),
        engine=row["engine"],
        kind=row["kind"],
        fetch_mode=row["fetch_mode"],
        extract_route=row["extract_route"],
        strategy=row["strategy"],
        http_status=row["http_status"],
        content_type=row["content_type"],
        lang=row["lang"],
        content_hash=row["content_hash"],
        # Read-only открытие старой БД не применяет аддитивных миграций: колонки нет —
        # значение «не записано», а не сбой чтения (тот же приём, что у ``corpus_docs``).
        doc_hash=_row_optional(row, "doc_hash"),
        raw_path=row["raw_path"],
        error_kind=row["error_kind"],
        error=row["error"],
        first_seen=row["first_seen"],
        last_attempt=row["last_attempt"],
        # 17 §6 (F6): метки замера попытки. Отсутствие колонки — «не записано» (старый
        # производитель), а не ноль: ``fetch_ms=0`` означал бы измеренный мгновенный фетч.
        fetch_started_at=_row_optional(row, "fetch_started_at"),
        fetch_ms=(
            None
            if (value := _row_optional(row, "fetch_ms")) is None
            else int(value)
        ),
    )


def _row_to_extract_queue_item(row: sqlite3.Row) -> ExtractQueueItem:
    """Восстанавливает ``ExtractQueueItem`` из строки ``extract_queue`` (17 §4.1).

    Провенанс получения читается через ``_row_optional``: отсутствие колонки должно давать
    ``None``, а не ``KeyError`` — так отказ называет **поле контракта**, которого нет, и
    выносит его доменный валидатор (``pending``/``claimed`` без провенанса отвергаются с
    именем поля). Терминальные состояния ``NULL`` разрешают по построению, поэтому чтение
    остаётся возможным и для строк, которые потребитель уже закрыл.
    """
    attempts = row["attempts"]
    return ExtractQueueItem(
        session_id=row["session_id"],
        product_id=row["product_id"],
        url=row["url"],
        content_hash=row["content_hash"],
        raw_path=row["raw_path"],
        level=row["level"],
        pdf_pipeline=row["pdf_pipeline"],
        fetch_mode=_row_optional(row, "fetch_mode"),
        status_code=_row_optional(row, "status_code"),
        content_type=_row_optional(row, "content_type"),
        lang=_row_optional(row, "lang"),
        engine=_row_optional(row, "engine"),
        extract_route=_row_optional(row, "extract_route"),
        fetched_at=_row_optional(row, "fetched_at"),
        state=ExtractQueueState(row["state"]),
        attempts=0 if attempts is None else int(attempts),
        enqueued_at=row["enqueued_at"],
        state_changed_at=row["state_changed_at"],
        claimed_by=row["claimed_by"],
        # 17 §4.2 п.3–4 (F6): аренда строки. Отсутствие колонки — «аренды не было»
        # (строка прежнего производителя), а не «аренда началась в эпоху».
        claimed_at=_row_optional(row, "claimed_at"),
        heartbeat_at=_row_optional(row, "heartbeat_at"),
        error_kind=row["error_kind"],
        error=row["error"],
    )


def _row_to_corpus_coverage(row: sqlite3.Row) -> CorpusCoverage:
    """Восстанавливает ``CorpusCoverage`` из строки ``corpus_coverage``."""
    raw = _parse_json_text(row["blocked_json"])
    blocked = raw if isinstance(raw, dict) else {}
    raw_origins = _parse_json_text(row["unreadable_origins_json"])
    unreadable_origins = raw_origins if isinstance(raw_origins, list) else []
    raw_partial = _parse_json_text(row["partial_blocks_json"])
    partial_blocks = raw_partial if isinstance(raw_partial, dict) else {}
    return CorpusCoverage(
        product_id=row["product_id"],
        complete=bool(row["complete"]),
        expected_sources=int(row["expected_sources"] or 0),
        observed_sources=int(row["observed_sources"] or 0),
        blocked={str(k): int(v) for k, v in blocked.items()},
        unreadable_origins=[str(origin) for origin in unreadable_origins],
        partial_blocks={
            str(origin): {str(kind): int(count) for kind, count in (counts or {}).items()}
            for origin, counts in partial_blocks.items()
        },
        truncated=bool(row["truncated"]),
        computed_at=row["computed_at"],
        reason=(row["reason"] if _has_column(row, "reason") else None),
        # 13-manual-artifacts §3.3: аддитивная колонка; старая строка без неё — 0.
        resolved_manually=(
            int(row["resolved_manually"] or 0)
            if _has_column(row, "resolved_manually")
            else 0
        ),
    )


def _archive_observation_from_row(row: sqlite3.Row) -> ArchiveObservation:
    """Строка ``archive_observations`` → доменная модель (JSON-блоки разбираются)."""
    return ArchiveObservation(
        observation_id=row["observation_id"],
        run_id=row["run_id"],
        product_id=row["product_id"],
        root_url=row["root_url"],
        archive_url=row["archive_url"],
        archive_content_hash=row["archive_content_hash"],
        archive_path=row["archive_path"],
        depth=int(row["depth"] or 0),
        descent=ArchiveDescent(row["descent"]),
        status=ArchiveDescentStatus(row["status"]),
        forecast=json.loads(row["forecast_json"] or "{}"),
        outcome=json.loads(row["outcome_json"] or "{}"),
        reason=row["reason"],
        created_at=row["created_at"],
    )


def _row_to_chunk(row: sqlite3.Row) -> Chunk:
    """Восстанавливает порт-тип ``Chunk`` из строки ``corpus_chunks``.

    Единая точка конверсии для ``get_chunk``/``iter_chunks``: ``chunk_no=None``
    читается как ``0``, ``NULL`` в текстовых полях — как пустая строка. ``NULL`` в
    ``char_start``/``char_end`` читается как ``None`` — «кусок не срез» (абзацный
    чанкер), а не как смещение ``0``: ноль — валидное начало первого куска.
    """
    return Chunk(
        chunk_id=row["chunk_id"],
        chunk_no=row["chunk_no"] if row["chunk_no"] is not None else 0,
        text=row["text"],
        heading_path=row["heading_path"] or "",
        context_prefix=row["context_prefix"] or "",
        content_hash=row["content_hash"] or "",
        char_start=row["char_start"],
        char_end=row["char_end"],
    )


def _row_to_resolved(row: sqlite3.Row) -> ResolvedSource:
    """Собирает :class:`ResolvedSource` из строки ``corpus_chunks``."""
    return ResolvedSource(
        chunk_id=row["chunk_id"],
        source_url=row["url"],
        content_hash=row["content_hash"],
        build_id=row["build_id"],
    )
