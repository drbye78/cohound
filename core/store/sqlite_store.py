"""coHound — конкретное операционное хранилище ``SqliteStore`` (L3, W3/T3.1, W3/T3.4).

Реализует ``StorePort`` и ``GateStore`` (``core/ports/store.py``) поверх схемы §4.4
(``core/store/sqlite.py``: 16 таблиц + ``schema_meta``, индексы, триггеры I4/I6/I7/I13/I18).
Также реализует ``domain.anchors.SourceResolver`` (T3.4): детерминированный
резолв ``source_id`` → URL + ``content_hash`` без сети (I10/I15/I19).

Инварианты остаются кодом: запись ячеек и evidence идёт через DDL-триггеры, поэтому
нарушения (I4/I6/I7/I13/I18) отклоняются на уровне БД. Батчевые записи — одна транзакция
на пачку (по умолчанию 200), WAL включён схемой (§B.2). Нейтрален (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import sqlite3
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

from core.domain import (
    AbsenceRuleType,
    Cell,
    CellEvidence,
    CellState,
    CorpusChunk,
    CorpusCoverage,
    CorpusDoc,
    CrawlItemStatus,
    CrawlProvenance,
    CrawlSession,
    CrawlSessionItem,
    CrawlSessionStatus,
    EndpointKind,
    EvidenceType,
    Gate,
    GateDecision,
    IndexManifestEntry,
    LLMScenario,
    ProtocolExhaustion,
    RankingPolicy,
    RegistryFact,
    ResolvedSource,
    RunConfig,
    RunManifest,
    RunMode,
    ScenarioBinding,
    SearchLogEntry,
    SourceKind,
    StaleReason,
    Status,
    TerminationReason,
)
from core.ports.store import RunIndexRef
from core.ports.types import Chunk
from core.store.sqlite import connect
from core.util.ids import parse_source_id

#: Размер пачки по умолчанию для батчевых записей (§B.2, T3.1).
DEFAULT_BATCH_SIZE = 200

#: Колонки ``cells``, которые реально несёт доменная модель ``Cell``. Прочие колонки
#: таблицы (``justification``/``rule_note``/``failure_kind``/``model_id``/… ) заполняются
#: другими контурами записи и здесь не пишутся (T3.1: «не писать колонки, которых нет в модели»).
_CELL_COLUMNS = (
    "run_id",
    "product_id",
    "param_id",
    "state",
    "status",
    "source_kind",
    "reviewed_by",
    "index_build_id",
    "retrieval_profile",
    "retrieval_params_json",
    "stale_reason",
    "review_reason",
)

#: Источники, стоящие в порядке §2.2 п.7 **ниже** реестра. Guard использует это только
#: как узкую страховку (defense-in-depth), а не как несущую логику прецедента: полный
#: порядок и решение о ``pending`` живут в ``core.pipeline.precedence`` (слой выше).
_LOWER_THAN_REGISTRY: frozenset[SourceKind] = frozenset(
    {
        SourceKind.INHERITANCE,
        SourceKind.DISTRIBUTION,
        SourceKind.DOCUMENTATION,
        SourceKind.WEB,
    }
)

#: Наличие-статусы (I2/§2.2 п.2): только для них «иной статус» означает конфликт.
_PRESENCE_STATUSES: frozenset[Status] = frozenset(
    {Status.OK, Status.PARTIAL, Status.ABSENT}
)

_EVIDENCE_COLUMNS = (
    "evidence_id",
    "run_id",
    "product_id",
    "param_id",
    "source_kind",
    "evidence_type",
    "source_id",
    "source_url",
    "citation_quote",
    "content_hash",
    "source_scope",
    "scope_attested_by",
    "index_build_id",
    "partition_build_id",
    "retrieved_at",
)

_SCENARIO_COLUMNS = (
    "run_id",
    "scenario",
    "model_id",
    "model_version",
    "endpoint_kind",
    "web_tool",
    "decoder",
    "seed",
    "temperature",
    "constrained_decoding",
)

_SEARCH_LOG_COLUMNS = (
    "run_id",
    "phase",
    "kind",
    "provider",
    "adapter_kind",
    "language",
    "query",
    "results_count",
    "selected",
    "lexical_score",
    "dense_score",
    "fused_rank",
    "rerank_score",
    "retrieval_score",
    "termination_reason",
    "ts",
)


def _now_iso() -> str:
    """Текущее время в ISO-8601 (UTC)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


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


def _parse_phase_state(raw: str | None) -> dict[str, str]:
    """Извлекает карту ``{phase: state}`` из ``runs.state`` (durable resume, 02 §A.1).

    Ожидается JSON-объект вида ``{"phase_state": {"<phase>": "<state>"}}``. Всё
    остальное (``NULL``, legacy-строка ``"{phase}:{state}"``, невалидный JSON, иная
    структура) трактуется как отсутствие данных → ``{}``. Функция не бросает исключений:
    resume обязан быть устойчив к повреждённому/старому состоянию.
    """
    payload = _parse_json_text(raw)
    if not isinstance(payload, Mapping):
        return {}
    inner = payload.get("phase_state")
    if not isinstance(inner, Mapping):
        return {}
    return {
        str(phase): str(state)
        for phase, state in inner.items()
        if phase is not None and state is not None
    }


def _get(obj: Any, name: str, default: Any = None) -> Any:
    """Чтение поля из dict/Mapping или объекта (``upsert_products`` — flexible input)."""
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


class SqliteStore:
    """Реализация ``StorePort``/``GateStore`` поверх SQLite-схемы прогона (§4.4).

    :param db_path: путь к файлу БД или ``":memory:"`` (дефолт).
    """

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self.db_path = db_path
        self.conn: sqlite3.Connection = connect(db_path)

    # -- lifecycle ------------------------------------------------------- #
    def close(self) -> None:
        """Закрывает соединение."""
        self.conn.close()

    def __enter__(self) -> SqliteStore:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        self.close()
        return False

    # -- products -------------------------------------------------------- #
    def upsert_products(self, products: Iterable[Any]) -> None:
        """Upsert продуктов.

        Принимает элементы-объекты или dict с полями
        ``product_id`` (обязательно), ``canonical_name``, ``vendor`` и опц. ``jurisdiction``.
        ``first_seen`` сохраняется при повторной записи, ``last_seen`` обновляется.
        """
        now = _now_iso()
        rows = []
        for p in products:
            rows.append(
                (
                    _get(p, "product_id"),
                    _get(p, "canonical_name", "") or "",
                    _get(p, "vendor", "") or "",
                    _get(p, "jurisdiction"),
                    now,
                    now,
                )
            )
        if not rows:
            return
        with self.conn:
            self.conn.executemany(
                "INSERT INTO products"
                " (product_id, canonical_name, vendor, jurisdiction, first_seen, last_seen)"
                " VALUES (?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(product_id) DO UPDATE SET"
                "  canonical_name = excluded.canonical_name,"
                "  vendor = excluded.vendor,"
                "  jurisdiction = excluded.jurisdiction,"
                "  last_seen = excluded.last_seen",
                rows,
            )

    # -- runs ------------------------------------------------------------ #
    def create_run(self, run_id: str, cfg: RunConfig) -> None:
        """Создаёт строку прогона в ``runs`` из ``RunConfig``.

        ``focus_modules`` — JSON-массив выбранных модулей; ``focus_text`` — JSON-массив
        выбранных параметров (конвенция хранения фокуса). Версии пакета/методологии здесь
        не фиксируются — они попадают в манифест на финальной фазе.
        """
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO runs"
                " (run_id, created_at, mode, case_id, anchor_product_id, cohort_ref,"
                "  focus_text, focus_modules, package_id, requested_ranking, snapshot_date, state)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    _now_iso(),
                    _as_text(cfg.mode),
                    cfg.case_id,
                    cfg.anchor,
                    cfg.cohort,
                    json.dumps(cfg.focus.params, ensure_ascii=False),
                    json.dumps(cfg.focus.modules, ensure_ascii=False),
                    cfg.package,
                    _as_text(cfg.ranking_policy),
                    cfg.snapshot_date.isoformat(),
                    None,
                ),
            )

    def set_phase_state(self, run_id: str, phase: str, state: str) -> None:
        """Фиксирует состояние фазы в ``runs.state`` как JSON-карту ``{phase: state}``.

        Колонка ``runs.state`` — единственный durable-носитель resume в схеме §4.4 (§A.1).
        Соседние фазы **не затираются**: новое состояние вливается в уже сохранённую карту
        (``{"phase_state": {...}}``). Legacy-значение ``"{phase}:{state}"`` или иной
        невалидный JSON молча заменяется свежей картой (устойчивость к старому состоянию).
        """
        current = self.get_phase_state(run_id)
        current[str(phase)] = str(state)
        payload = json.dumps({"phase_state": current}, ensure_ascii=False, sort_keys=True)
        with self.conn:
            self.conn.execute(
                "UPDATE runs SET state = ? WHERE run_id = ?",
                (payload, run_id),
            )

    def get_phase_state(self, run_id: str) -> dict[str, str]:
        """Возвращает сохранённую карту ``{phase: "ok"|"blocked"|"failed"}`` прогона.

        Пустой словарь — если прогон неизвестен, ``runs.state`` пуст (``NULL``) либо хранит
        legacy/повреждённое значение. Метод не бросает исключений на неизвестный ``run_id``.
        """
        row = self.conn.execute(
            "SELECT state FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            return {}
        return _parse_phase_state(row["state"])

    # -- cells ----------------------------------------------------------- #
    def _cell_row(self, cell: Cell) -> tuple[Any, ...]:
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
        )

    def _guard_against_downgrade(self, cell: Cell) -> None:
        """Defense-in-depth: запрет тихой замены evaluated-ячейки высокого источника.

        Guard срабатывает только когда **уже лежащая** ячейка ``evaluated`` и её
        ``source_kind=registry``, а входящая ячейка — нижнего источника (§2.2 п.7) с
        **иным** наличием-статусом и не является ``pending``/``reviewed``. Легитимный
        конфликт всегда оформляется как ``pending`` (или ``reviewed``) и проходит.
        Нарушение видно вызывающему: бросается :class:`ValueError` (не тихий no-op).
        """
        row = self.conn.execute(
            "SELECT state, status, source_kind FROM cells"
            " WHERE run_id = ? AND product_id = ? AND param_id = ?",
            (cell.run_id, cell.product_id, cell.param_id),
        ).fetchone()
        if row is None or row["state"] != CellState.EVALUATED.value:
            return
        if row["source_kind"] != SourceKind.REGISTRY.value:
            return
        if cell.state in {CellState.PENDING, CellState.REVIEWED}:
            return
        if cell.source_kind not in _LOWER_THAN_REGISTRY:
            return
        existing_status = row["status"]
        if existing_status is None:
            return
        existing = Status(existing_status)
        if existing not in _PRESENCE_STATUSES:
            return
        incoming = cell.status.value if cell.status is not None else None
        if incoming == existing_status:
            return
        raise ValueError(
            "прецедент источника: evaluated-ячейку высокого источника нельзя заменить "
            "ячейкой нижнего источника с иным наличием-статусом "
            f"({existing.value!r} → {incoming!r}); конфликт оформляется как "
            "pending/reviewed, а не тихой перезаписью"
        )

    def save_cell(self, cell: Cell) -> None:
        """Upsert ячейки по PK ``(run_id, product_id, param_id)``.

        Enum-поля сериализуются в ``.value``; ``status=None`` для failed/preliminary
        сохраняется как NULL. Триггеры I4/I6/I7/I13 применяются БД. Перед записью
        срабатывает guard против тихого понижения высокого источника (defense-in-depth).

        Примечание (T3.1): ``absence_rule``/``evidence``/``absence_evidence`` не имеют
        колонок в ``cells`` — evidence живёт в ``cell_evidence``, ``absence_rule`` —
        свойство каталога; при чтении восстанавливаются дефолтами модели.
        """
        placeholders = ", ".join("?" for _ in _CELL_COLUMNS)
        with self.conn:
            self._guard_against_downgrade(cell)
            self.conn.execute(
                f"INSERT OR REPLACE INTO cells ({', '.join(_CELL_COLUMNS)})"
                f" VALUES ({placeholders})",
                self._cell_row(cell),
            )

    def set_cell_provenance(
        self,
        run_id: str,
        product_id: str,
        param_id: str,
        *,
        prompt_hash: str | None = None,
        model_id: str | None = None,
    ) -> None:
        """Узкий UPDATE провенанса уже сохранённой ячейки (R7/§13).

        Колонки ``cells.prompt_hash``/``cells.model_id`` существуют в схеме §4.4, но
        доменная ``Cell`` их не несёт, поэтому ``save_cell`` их не пишет. Этот метод
        дописывает провенанс **после** успешного upsert, не расширяя ``_CELL_COLUMNS``
        для всех писателей. ``None`` не затирает существующее значение (``COALESCE``).
        """
        with self.conn:
            self.conn.execute(
                "UPDATE cells SET prompt_hash = COALESCE(?, prompt_hash), "
                "model_id = COALESCE(?, model_id) "
                "WHERE run_id = ? AND product_id = ? AND param_id = ?",
                (prompt_hash, model_id, run_id, product_id, param_id),
            )

    def save_cells_batch(self, cells: list[Cell], chunk_size: int = DEFAULT_BATCH_SIZE) -> None:
        """Пишет ячейки пачками: одна транзакция на ``chunk_size`` (по умолчанию 200).

        Каждая ячейка проходит тот же guard, что и :meth:`save_cell`.
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size обязан быть > 0")
        placeholders = ", ".join("?" for _ in _CELL_COLUMNS)
        sql = (
            f"INSERT OR REPLACE INTO cells ({', '.join(_CELL_COLUMNS)})"
            f" VALUES ({placeholders})"
        )
        for start in range(0, len(cells), chunk_size):
            part = cells[start : start + chunk_size]
            with self.conn:
                for cell in part:
                    self._guard_against_downgrade(cell)
                self.conn.executemany(sql, [self._cell_row(c) for c in part])

    def iter_cells(self, run_id: str, state: str | None = None) -> Iterator[Cell]:
        """Итерирует ячейки прогона, восстанавливая доменные ``Cell``.

        ``evidence``/``absence_evidence`` не хранятся в ``cells`` — возвращаются пустыми
        (evidence читается отдельно; Ш6 пишет его через ``add_evidence*``).
        """
        sql = (
            f"SELECT {', '.join(_CELL_COLUMNS)} FROM cells WHERE run_id = ?"
        )
        params: list[Any] = [run_id]
        if state is not None:
            sql += " AND state = ?"
            params.append(state)
        cursor = self.conn.execute(sql, params)
        for row in cursor:
            yield self._row_to_cell(row)

    def iter_registry_cells(self, run_id: str) -> Iterator[Cell]:
        """Итерирует ячейки реестрового источника прогона (документированная поверхность).

        Нужна Ш6, чтобы наложить прецедент поверх реестровых ячеек (§2.2 п.7/п.8):
        ``iter_cells`` не фильтрует по ``source_kind``, а evidence в доменной ячейке
        отсутствует (живёт в ``cell_evidence``). Порядок детерминирован (как у
        ``iter_cells``); фильтр — ``source_kind is registry``.
        """
        for cell in self.iter_cells(run_id):
            if cell.source_kind is SourceKind.REGISTRY:
                yield cell

    @staticmethod
    def _row_to_cell(row: sqlite3.Row) -> Cell:
        """Восстанавливает ``Cell`` из строки без повторной валидации инвариантов.

        Инварианты I1/I2 зависят от ``evidence``/``absence_evidence``, которые живут в
        ``cell_evidence``, а не в ``cells``. Строка уже прошла валидацию и триггеры БД при
        записи, поэтому ``model_construct`` избегает ложного срабатывания I1/I2 при чтении.
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
            absence_rule=AbsenceRuleType.EXPLICIT,
            evidence=[],
            absence_evidence=[],
            reviewed_by=row["reviewed_by"],
            index_build_id=row["index_build_id"],
            retrieval_profile=row["retrieval_profile"],
            retrieval_params_json=row["retrieval_params_json"],
            stale_reason=StaleReason(stale_reason) if stale_reason is not None else None,
            review_reason=row["review_reason"],
        )

    # -- evidence -------------------------------------------------------- #
    def _evidence_row(self, ev: CellEvidence) -> tuple[Any, ...]:
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
            ev.scope_attested_by,
            ev.index_build_id,
            ev.partition_build_id,
            ev.retrieved_at,
        )

    def add_evidence(self, ev: CellEvidence) -> None:
        """Upsert evidence по ``evidence_id`` (S8: идемпотентность при resume)."""
        placeholders = ", ".join("?" for _ in _EVIDENCE_COLUMNS)
        with self.conn:
            self.conn.execute(
                f"INSERT OR REPLACE INTO cell_evidence ({', '.join(_EVIDENCE_COLUMNS)})"
                f" VALUES ({placeholders})",
                self._evidence_row(ev),
            )

    def add_evidence_batch(self, evs: list[CellEvidence]) -> None:
        """Пишет evidence одной транзакцией (идемпотентный upsert по ``evidence_id``)."""
        if not evs:
            return
        placeholders = ", ".join("?" for _ in _EVIDENCE_COLUMNS)
        sql = (
            f"INSERT OR REPLACE INTO cell_evidence ({', '.join(_EVIDENCE_COLUMNS)})"
            f" VALUES ({placeholders})"
        )
        with self.conn:
            self.conn.executemany(sql, [self._evidence_row(ev) for ev in evs])

    # -- search log ------------------------------------------------------ #
    def log_search(self, entry: SearchLogEntry) -> None:
        """Пишет запись поискового лога (§4.4, S12, I23). ``log_id`` — AUTOINCREMENT."""
        columns: list[str] = list(_SEARCH_LOG_COLUMNS)
        values: list[Any] = [
            entry.run_id,
            _as_text(entry.phase),
            _as_text(entry.kind),
            entry.provider,
            _as_text(entry.adapter_kind),
            entry.language,
            entry.query,
            entry.results_count,
            entry.selected,
            entry.lexical_score,
            entry.dense_score,
            entry.fused_rank,
            entry.rerank_score,
            entry.retrieval_score,
            _as_text(entry.termination_reason),
            entry.ts,
        ]
        if entry.log_id is not None:
            columns.insert(0, "log_id")
            values.insert(0, entry.log_id)
        placeholders = ", ".join("?" for _ in columns)
        with self.conn:
            self.conn.execute(
                f"INSERT OR REPLACE INTO search_log ({', '.join(columns)})"
                f" VALUES ({placeholders})",
                values,
            )

    # -- exhaustion ------------------------------------------------------ #
    def save_exhaustion(self, pe: ProtocolExhaustion) -> None:
        """Upsert машиночитаемого протокола исчерпания (§5, S13)."""
        protocol = {
            "contours": pe.contours,
            "queries": pe.queries,
            "rounds": pe.rounds,
            "budget": pe.budget,
            "termination_reason": _as_text(pe.termination_reason),
            "exhausted_at": pe.exhausted_at,
            "notes": pe.notes,
        }
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO search_exhaustion"
                " (run_id, product_id, param_id, protocol_json) VALUES (?, ?, ?, ?)",
                (
                    pe.run_id,
                    pe.product_id,
                    pe.param_id,
                    json.dumps(protocol, ensure_ascii=False, sort_keys=True),
                ),
            )

    # -- registry -------------------------------------------------------- #
    def save_registry_fact(self, rf: RegistryFact) -> None:
        """Upsert реестрового факта (R1: ``source_url`` обязателен).

        Принимает доменный ``RegistryFact`` (``fact_json: str``); dict/list также
        сериализуются для совместимости с порт-структурой фетчера.
        """
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO registry_facts"
                " (run_id, product_id, registry_id, fact_json, source_url, fetched_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (
                    rf.run_id,
                    rf.product_id,
                    rf.registry_id,
                    _as_json_text(rf.fact_json),
                    rf.source_url,
                    rf.fetched_at,
                ),
            )

    # -- gates ----------------------------------------------------------- #
    def save_gate_decision(self, d: GateDecision) -> None:
        """Upsert человеческого решения гейта G1–G4 (S9, I6/I7).

        Поддерживает доменный ``GateDecision``; для обратной совместимости с transitional
        порт-структурой (``ts``/``decision``/``payload``) недостающий ``decision_id``
        выводится детерминированно.
        """
        run_id = d.run_id
        gate = _as_text(d.gate)
        target = getattr(d, "target", None)
        actor = d.actor
        decided_at = getattr(d, "decided_at", None) or getattr(d, "ts", None)
        notes = getattr(d, "notes", None)
        decision_id = getattr(d, "decision_id", None)
        if not decision_id:
            raw = f"{run_id}|{gate}|{target}|{actor}|{decided_at}".encode()
            decision_id = "gd_" + hashlib.sha256(raw).hexdigest()[:24]
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO gate_decisions"
                " (decision_id, run_id, gate, target, actor, decided_at, notes)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (decision_id, run_id, gate, target, actor, decided_at, notes),
            )

    def iter_gate_decisions(self, run_id: str) -> Iterator[GateDecision]:
        """Итерирует решения гейтов прогона как доменные ``GateDecision`` (аудит-след)."""
        cursor = self.conn.execute(
            "SELECT decision_id, run_id, gate, target, actor, decided_at, notes"
            " FROM gate_decisions WHERE run_id = ? ORDER BY decided_at, decision_id",
            (run_id,),
        )
        for row in cursor:
            yield GateDecision(
                decision_id=row["decision_id"],
                run_id=row["run_id"],
                gate=Gate(row["gate"]),
                target=row["target"],
                actor=row["actor"],
                decided_at=row["decided_at"],
                notes=row["notes"],
            )

    def iter_exhaustions(self, run_id: str) -> Iterator[ProtocolExhaustion]:
        """Итерирует протоколы исчерпания прогона из ``search_exhaustion`` (§5, S13).

        Порядок детерминирован: ``product_id, param_id``. Строки с невалидным
        ``protocol_json`` или без ``termination_reason`` пропускаются (устойчивость к
        повреждённым данным; модель требует терминацию).
        """
        cursor = self.conn.execute(
            "SELECT run_id, product_id, param_id, protocol_json FROM search_exhaustion"
            " WHERE run_id = ? ORDER BY product_id, param_id",
            (run_id,),
        )
        for row in cursor:
            protocol = _parse_json_text(row["protocol_json"])
            if not isinstance(protocol, Mapping):
                continue
            reason = protocol.get("termination_reason")
            if reason is None:
                continue
            yield ProtocolExhaustion(
                run_id=row["run_id"],
                product_id=row["product_id"],
                param_id=row["param_id"],
                contours=list(protocol.get("contours") or []),
                queries=list(protocol.get("queries") or []),
                rounds=int(protocol.get("rounds") or 0),
                budget=dict(protocol.get("budget") or {}),
                termination_reason=TerminationReason(reason),
                exhausted_at=protocol.get("exhausted_at"),
                notes=protocol.get("notes"),
            )

    # -- llm scenarios / manifest ---------------------------------------- #
    def save_llm_scenarios(self, bindings: list[ScenarioBinding]) -> None:
        """Upsert маршрутизации модель→сценарий (I21). ``evaluator`` без веб-тула (I18)."""
        if not bindings:
            return
        placeholders = ", ".join("?" for _ in _SCENARIO_COLUMNS)
        sql = (
            f"INSERT OR REPLACE INTO llm_scenarios ({', '.join(_SCENARIO_COLUMNS)})"
            f" VALUES ({placeholders})"
        )
        rows = [
            (
                b.run_id,
                _as_text(b.scenario),
                b.model_id,
                b.model_version,
                _as_text(b.endpoint_kind),
                1 if b.web_tool else 0,
                b.decoder,
                b.seed,
                b.temperature,
                b.constrained_decoding,
            )
            for b in bindings
        ]
        with self.conn:
            self.conn.executemany(sql, rows)

    # -- план-входы прогона (P1: самодостаточность runs/{run_id}) --------- #
    #: Колонки ``run_inputs`` (снимок плановых входов; P1).
    _RUN_INPUTS_COLUMNS = (
        "run_id",
        "snapshot_version",
        "payload_json",
        "payload_sha256",
        "created_at",
    )

    def save_plan_inputs(
        self, run_id: str, *, payload: Mapping[str, Any], payload_sha256: str
    ) -> None:
        """Персистит снимок плановых входов в ``run_inputs`` (P1; ``INSERT OR REPLACE``).

        ``payload`` кладётся в ``payload_json`` канонической формой
        (``sort_keys=True``/``ensure_ascii=False``) — семантически байт-идентичной при
        повторной записи. ``snapshot_version`` выносится отдельной колонкой-контрактом.
        """
        snapshot_version = int(payload.get("snapshot_version") or 0)
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO run_inputs"
                " (run_id, snapshot_version, payload_json, payload_sha256, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (
                    run_id,
                    snapshot_version,
                    payload_json,
                    payload_sha256,
                    _now_iso(),
                ),
            )

    def read_plan_inputs(self, run_id: str) -> dict[str, Any] | None:
        """Читает снимок плановых входов прогона; ``None``, если записи нет/повреждена."""
        row = self.conn.execute(
            "SELECT payload_json FROM run_inputs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            return None
        parsed = _parse_json_text(row["payload_json"])
        return parsed if isinstance(parsed, dict) else None

    def persist_manifest(self, run_id: str, manifest: RunManifest) -> None:
        """Персистит финальный манифест в строку ``runs`` (R7/§13, A3).

        Обновляет версии, индексные/ретривал-поля, ``settings_provenance_json`` и
        ``ranking_epsilon`` (провенанс настроек), ``crawl_provenance_json``
        (провенанс краула, §4.4/Phase 2), ``crawl_strategy``/``crawl_rate_limit``
        (операционные оси краула — границы ``execution_change``, §13/Backlog B),
        ``crawl_config_digest`` (§13/N6) и ``registry_spec_digest`` (§13/E2 — дайджест
        package-scoped спецификаций реестров), а также
        ``execution_contract_digest`` (09 §12.5 — отпечаток условий измерения, по которому
        ``hound diff`` решает сравнимость циклов), деплой-провенанс
        ``deploy_profile``/``edge_schema_version``/``edge_capabilities_json``
        (07 §7; execution-class, не методологическая версия). ``UPDATE`` по существующей
        строке прогона: манифест — самостоятельная запись воспроизведения, а не источник
        создания прогона (его создаёт :meth:`create_run`).
        """
        provenance = manifest.settings_provenance or {}
        crawl_provenance = manifest.crawl_provenance
        with self.conn:
            self.conn.execute(
                "UPDATE runs SET"
                " package_version = ?, catalog_version = ?, status_dict_version = ?,"
                " scoring_policy_version = ?, methodology_version = ?, embed_model_id = ?,"
                " index_build_id = ?, retrieval_profile = ?, retrieval_params_json = ?,"
                " retrieval_cutoff_K = ?, search_effort_budget_json = ?,"
                " requested_ranking = ?, computed_ranking = ?, snapshot_date = ?,"
                " settings_provenance_json = ?, ranking_epsilon = ?,"
                " crawl_provenance_json = ?,"
                " crawl_strategy = ?, crawl_rate_limit = ?,"
                " crawl_config_digest = ?, registry_spec_digest = ?,"
                " execution_contract_digest = ?,"
                " deploy_profile = ?, edge_schema_version = ?, edge_capabilities_json = ?"
                " WHERE run_id = ?",
                (
                    manifest.package_version,
                    manifest.catalog_version,
                    manifest.status_dict_version,
                    manifest.scoring_policy_version,
                    manifest.methodology_version,
                    manifest.embed_model_id,
                    manifest.index_build_id,
                    manifest.retrieval_profile,
                    manifest.retrieval_params_json,
                    manifest.retrieval_cutoff_K,
                    manifest.search_effort_budget_json,
                    _as_text(manifest.requested_ranking),
                    _as_text(manifest.computed_ranking),
                    manifest.snapshot_date,
                    json.dumps(provenance, ensure_ascii=False, sort_keys=True),
                    float(manifest.ranking_epsilon or 0.0),
                    json.dumps(
                        crawl_provenance.model_dump(mode="json"),
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    manifest.crawl_strategy,
                    manifest.crawl_rate_limit,
                    manifest.crawl_config_digest,
                    manifest.registry_spec_digest,
                    manifest.execution_contract_digest,
                    manifest.deploy_profile,
                    manifest.edge_schema_version,
                    json.dumps(
                        dict(manifest.edge_capabilities or {}),
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    run_id,
                ),
            )

    def set_run_index_build_id(self, run_id: str, index_build_id: str) -> None:
        """Записывает ``runs.index_build_id`` сразу после фазы корпуса (идемпотентно).

        Composition id известен уже на выходе Ш5, а :meth:`persist_manifest` пишет весь
        манифест только в Ш9 — слишком поздно для resume и для пост-корпусной привязки
        ретривала. Повторная запись того же значения — no-op; неизвестный ``run_id`` —
        no-op (строку прогона создаёт :meth:`create_run`, а не этот метод).
        """
        with self.conn:
            self.conn.execute(
                "UPDATE runs SET index_build_id = ? WHERE run_id = ?",
                (index_build_id, run_id),
            )

    def upsert_corpus_doc(self, doc: CorpusDoc) -> None:
        """Upsert строки ``corpus_docs`` (raw retention + provenance экстракции, §3.2)."""
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO corpus_docs"
                " (doc_id, product_id, url, fetched_at, content_hash, raw_path,"
                "  extractor_version, render_mode, lang)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    doc.doc_id,
                    doc.product_id,
                    doc.url,
                    doc.fetched_at,
                    doc.content_hash,
                    doc.raw_path,
                    doc.extractor_version,
                    _as_text(doc.render_mode),
                    doc.lang,
                ),
            )

    def add_chunk(self, chunk: CorpusChunk) -> None:
        """Upsert строки ``corpus_chunks`` (PK ``chunk_id``), идемпотентно (S8/I19)."""
        self.add_chunks_batch([chunk])

    def add_chunks_batch(self, chunks: list[CorpusChunk]) -> None:
        """Батчевый upsert чанков (одна транзакция на пачку, §B.2)."""
        if not chunks:
            return
        sql = (
            "INSERT OR REPLACE INTO corpus_chunks"
            " (chunk_id, doc_id, product_id, url, heading_path, lang, chunk_no, text,"
            "  context_prefix, content_hash, extractor_version, build_id, embed_model_id,"
            "  embedded_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        rows = [
            (
                c.chunk_id,
                c.doc_id,
                c.product_id,
                c.url,
                c.heading_path,
                c.lang,
                c.chunk_no,
                c.text,
                c.context_prefix,
                c.content_hash,
                c.extractor_version,
                c.build_id,
                c.embed_model_id,
                c.embedded_at,
            )
            for c in chunks
        ]
        with self.conn:
            self.conn.executemany(sql, rows)

    def save_index_manifest(self, entry: IndexManifestEntry) -> None:
        """Upsert строки ``index_manifest`` (PK ``(index_build_id, product_id)`` — K1/I17)."""
        with self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO index_manifest"
                " (index_build_id, partition_build_id, product_id, package_id,"
                "  partition_key, snapshot_date,"
                "  chunker_version, extractor_version, embed_model_id, embed_dim,"
                "  lexical_engine_version, vector_store, content_digest, store_path,"
                "  created_at, stats_json)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entry.index_build_id,
                    entry.partition_build_id,
                    entry.product_id,
                    entry.package_id,
                    entry.partition_key,
                    entry.snapshot_date,
                    entry.chunker_version,
                    entry.extractor_version,
                    entry.embed_model_id,
                    entry.embed_dim,
                    entry.lexical_engine_version,
                    entry.vector_store,
                    entry.content_digest,
                    entry.store_path,
                    entry.created_at,
                    _as_json_text(entry.stats_json),
                ),
            )

    # -- crawl_sessions (Lane B: durable намерение + агрегат прогресса) -- #
    #: Колонки ``crawl_sessions`` (совпадают с полями ``CrawlSession``).
    _CRAWL_SESSION_COLUMNS = (
        "session_id",
        "run_id",
        "profile",
        "status",
        "created_at",
        "started_at",
        "finished_at",
        "total",
        "pending",
        "running",
        "fetched",
        "failed",
        "cancelled",
        "error",
        "options_json",
        "provenance_json",
    )

    #: Метаданные сеанса, обновляемые при ``ON CONFLICT`` (B2): счётчики прогресса
    #: (``total/pending/running/fetched/failed/cancelled``) сюда **не** входят — их
    #: единственные владельцы ``upsert_crawl_session_item``/``recover_crawl_session``.
    _CRAWL_SESSION_METADATA_COLUMNS = (
        "run_id",
        "profile",
        "status",
        "created_at",
        "started_at",
        "finished_at",
        "error",
        "options_json",
        "provenance_json",
    )

    #: Колонки ``crawl_session_items`` (совпадают с полями ``CrawlSessionItem``).
    _CRAWL_ITEM_COLUMNS = (
        "session_id",
        "product_id",
        "url",
        "status",
        "attempts",
        "depth",
        "engine",
        "kind",
        "fetch_mode",
        "extract_route",
        "strategy",
        "http_status",
        "content_type",
        "lang",
        "content_hash",
        "raw_path",
        "error_kind",
        "error",
        "first_seen",
        "last_attempt",
    )

    @staticmethod
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
        )

    @staticmethod
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
            raw_path=row["raw_path"],
            error_kind=row["error_kind"],
            error=row["error"],
            first_seen=row["first_seen"],
            last_attempt=row["last_attempt"],
        )

    def upsert_crawl_session(self, session: CrawlSession) -> None:
        """Upsert строки ``crawl_sessions`` по PK ``session_id`` (полная запись метаданных).

        ``ON CONFLICT DO UPDATE`` (не ``INSERT OR REPLACE``): REPLACE удалял бы строку, а с
        ней — по FK — ссылающиеся элементы сеанса. При INSERT счётчики берутся из модели
        (первичная запись); при конфликте counter-колонки **исключены из SET** (B2) — их
        владельцы :meth:`upsert_crawl_session_item`/:meth:`recover_crawl_session`, иначе
        stale-модель обнуляла бы прогресс.
        """
        placeholders = ", ".join("?" for _ in self._CRAWL_SESSION_COLUMNS)
        updates = ", ".join(
            f"{name} = excluded.{name}"
            for name in self._CRAWL_SESSION_METADATA_COLUMNS
        )
        values = (
            session.session_id,
            session.run_id,
            session.profile,
            _as_text(session.status),
            session.created_at,
            session.started_at,
            session.finished_at,
            session.total,
            session.pending,
            session.running,
            session.fetched,
            session.failed,
            session.cancelled,
            session.error,
            session.options_json,
            session.provenance_json,
        )
        with self.conn:
            self.conn.execute(
                f"INSERT INTO crawl_sessions ({', '.join(self._CRAWL_SESSION_COLUMNS)})"
                f" VALUES ({placeholders}) ON CONFLICT(session_id) DO UPDATE SET {updates}",
                values,
            )

    def get_crawl_session(self, session_id: str) -> CrawlSession | None:
        """Возвращает сеанс краула как доменную ``CrawlSession`` или ``None``."""
        row = self.conn.execute(
            "SELECT * FROM crawl_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_crawl_session(row)

    def update_crawl_session_status(
        self,
        session_id: str,
        status: CrawlSessionStatus,
        *,
        started_at: str | None = None,
        finished_at: str | None = None,
        error: str | None = None,
    ) -> None:
        """Частичный upsert жизненного цикла сеанса (идемпотентный).

        Переданные (не ``None``) поля перекрывают текущие (``COALESCE``); прочие сохраняются.
        Счётчики прогресса не трогаются — их ведёт :meth:`upsert_crawl_session_item` /
        :meth:`recover_crawl_session`.

        Семантика «переход в ``running``/``created`` очищает признаки завершённости» (C3):
        при таком переходе ``finished_at`` и ``error`` сбрасываются в ``NULL`` (если не
        переданы явно) — иначе ``completed → running`` оставлял бы старый ``finished_at``.
        """
        resolved = CrawlSessionStatus(_as_text(status))
        # При возобновлении/старте сеанс вновь «не завершён»: очищаем метки финала.
        if resolved in {CrawlSessionStatus.RUNNING, CrawlSessionStatus.CREATED}:
            finished_clause = "finished_at = ?"
            error_clause = "error = ?"
        else:
            finished_clause = "finished_at = COALESCE(?, finished_at)"
            error_clause = "error = COALESCE(?, error)"
        with self.conn:
            self.conn.execute(
                "UPDATE crawl_sessions SET"
                " status = ?,"
                " started_at = COALESCE(?, started_at),"
                f" {finished_clause},"
                f" {error_clause}"
                " WHERE session_id = ?",
                (
                    resolved.value,
                    started_at,
                    finished_at,
                    error,
                    session_id,
                ),
            )

    def _recompute_crawl_session_counters(self, session_id: str) -> None:
        """Пересчитывает и персистит агрегаты сеанса из его элементов (полный recount).

        Вызывается **внутри** транзакции вызывающего: даёт простую и корректную поддержку
        счётчиков без риска расхождения инкрементов. При отсутствии элементов обнуляет.
        """
        counts = {s.value: 0 for s in CrawlItemStatus}
        total = 0
        for row in self.conn.execute(
            "SELECT status, COUNT(*) AS n FROM crawl_session_items WHERE session_id = ?"
            " GROUP BY status",
            (session_id,),
        ):
            n = int(row["n"])
            counts[row["status"]] = n
            total += n
        self.conn.execute(
            "UPDATE crawl_sessions SET total = ?, pending = ?, running = ?,"
            " fetched = ?, failed = ?, cancelled = ? WHERE session_id = ?",
            (
                total,
                counts[CrawlItemStatus.PENDING.value],
                counts[CrawlItemStatus.RUNNING.value],
                counts[CrawlItemStatus.FETCHED.value],
                counts[CrawlItemStatus.FAILED.value],
                counts[CrawlItemStatus.CANCELLED.value],
                session_id,
            ),
        )

    def upsert_crawl_session_item(self, item: CrawlSessionItem) -> None:
        """Upsert элемента сеанса по PK ``(session_id, product_id, url)`` (атомарно).

        В **одной** транзакции пишет строку элемента и пересчитывает агрегаты сеанса
        (полный recount из элементов) — прогресс не имеет разрывов. Требует существующего
        сеанса (FK на ``crawl_sessions``).
        """
        placeholders = ", ".join("?" for _ in self._CRAWL_ITEM_COLUMNS)
        values = (
            item.session_id,
            item.product_id,
            item.url,
            _as_text(item.status),
            item.attempts,
            item.depth,
            item.engine,
            item.kind,
            item.fetch_mode,
            item.extract_route,
            item.strategy,
            item.http_status,
            item.content_type,
            item.lang,
            item.content_hash,
            item.raw_path,
            item.error_kind,
            item.error,
            item.first_seen,
            item.last_attempt,
        )
        with self.conn:
            self.conn.execute(
                f"INSERT OR REPLACE INTO crawl_session_items"
                f" ({', '.join(self._CRAWL_ITEM_COLUMNS)}) VALUES ({placeholders})",
                values,
            )
            self._recompute_crawl_session_counters(item.session_id)

    def iter_crawl_session_items(
        self,
        session_id: str,
        *,
        product_id: str | None = None,
        status: CrawlItemStatus | None = None,
    ) -> Iterator[CrawlSessionItem]:
        """Итерирует элементы сеанса; порядок ``product_id, url`` (детерминизм)."""
        sql = "SELECT * FROM crawl_session_items WHERE session_id = ?"
        params: list[Any] = [session_id]
        if product_id is not None:
            sql += " AND product_id = ?"
            params.append(product_id)
        if status is not None:
            sql += " AND status = ?"
            params.append(CrawlItemStatus(_as_text(status)).value)
        sql += " ORDER BY product_id, url"
        for row in self.conn.execute(sql, params):
            yield self._row_to_crawl_item(row)

    def crawl_session_counts(self, session_id: str) -> dict[str, int]:
        """Число элементов сеанса по каждому статусу ``CrawlItemStatus``.

        Ключи — все пять статусов элемента (отсутствующие — ``0``); форма стабильна.
        """
        counts = {s.value: 0 for s in CrawlItemStatus}
        for row in self.conn.execute(
            "SELECT status, COUNT(*) AS n FROM crawl_session_items WHERE session_id = ?"
            " GROUP BY status",
            (session_id,),
        ):
            counts[row["status"]] = int(row["n"])
        return counts

    def recover_crawl_session(self, session_id: str) -> int:
        """Recovery-on-open: ``running`` → ``pending`` + пересчёт счётчиков (одна транзакция).

        Возвращает число восстановленных элементов. Идемпотентен: повторный вызов не находит
        ``running`` и возвращает ``0``.
        """
        with self.conn:
            cursor = self.conn.execute(
                "UPDATE crawl_session_items SET status = ? WHERE session_id = ? AND status = ?",
                (
                    CrawlItemStatus.PENDING.value,
                    session_id,
                    CrawlItemStatus.RUNNING.value,
                ),
            )
            recovered = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            self._recompute_crawl_session_counters(session_id)
        return recovered

    def clear_crawl_session(self, session_id: str) -> None:
        """Удаляет элементы сеанса, затем сам сеанс (одна транзакция)."""
        with self.conn:
            self.conn.execute(
                "DELETE FROM crawl_session_items WHERE session_id = ?", (session_id,)
            )
            self.conn.execute(
                "DELETE FROM crawl_sessions WHERE session_id = ?", (session_id,)
            )

    def set_crawl_session_provenance(self, session_id: str, provenance_json: str) -> None:
        """Записывает ``provenance_json`` сеанса (targeted ``UPDATE``; Phase 2).

        Обновляет только колонку провенанса — счётчики/метаданные сеанса не задеваются.
        """
        with self.conn:
            self.conn.execute(
                "UPDATE crawl_sessions SET provenance_json = ? WHERE session_id = ?",
                (provenance_json, session_id),
            )

    @staticmethod
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
            reason=(row["reason"] if "reason" in row.keys() else None),
        )

    def save_corpus_coverage(self, run_id: str, coverage: CorpusCoverage) -> None:
        """Идемпотентный upsert покрытия корпуса по PK ``(run_id, product_id)``."""
        blocked_json = json.dumps(
            dict(sorted(coverage.blocked.items())),
            ensure_ascii=False,
            sort_keys=True,
        )
        unreadable_origins_json = json.dumps(
            sorted(coverage.unreadable_origins),
            ensure_ascii=False,
            sort_keys=True,
        )
        partial_blocks_json = json.dumps(
            {
                origin: dict(sorted(coverage.partial_blocks[origin].items()))
                for origin in sorted(coverage.partial_blocks)
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        with self.conn:
            self.conn.execute(
                "INSERT INTO corpus_coverage"
                " (run_id, product_id, complete, expected_sources, observed_sources,"
                "  blocked_json, unreadable_origins_json, partial_blocks_json,"
                "  truncated, computed_at, reason)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(run_id, product_id) DO UPDATE SET"
                "  complete = excluded.complete,"
                "  expected_sources = excluded.expected_sources,"
                "  observed_sources = excluded.observed_sources,"
                "  blocked_json = excluded.blocked_json,"
                "  unreadable_origins_json = excluded.unreadable_origins_json,"
                "  partial_blocks_json = excluded.partial_blocks_json,"
                "  truncated = excluded.truncated,"
                "  computed_at = excluded.computed_at,"
                "  reason = excluded.reason",
                (
                    run_id,
                    coverage.product_id,
                    int(coverage.complete),
                    coverage.expected_sources,
                    coverage.observed_sources,
                    blocked_json,
                    unreadable_origins_json,
                    partial_blocks_json,
                    int(coverage.truncated),
                    coverage.computed_at,
                    coverage.reason,
                ),
            )

    def record_cell_failure(
        self, run_id: str, product_id: str, param_id: str, failure_kind: str
    ) -> None:
        """Узкий UPDATE класса сбоя ячейки (``cells.failure_kind``).

        Доменная ``Cell`` не несёт ``failure_kind``; колонка заполняется отдельным
        контуром (как ``prompt_hash``/``model_id``). Используется zero-source/anomaly
        путями оценки (O2/O3): ``failed``-ячейка обязана назвать класс сбоя.
        """
        with self.conn:
            self.conn.execute(
                "UPDATE cells SET failure_kind = ?"
                " WHERE run_id = ? AND product_id = ? AND param_id = ?",
                (failure_kind, run_id, product_id, param_id),
            )

    def read_corpus_coverage(
        self, run_id: str, product_id: str
    ) -> CorpusCoverage | None:
        """Читает покрытие продукта; ``None``, если записи нет."""
        row = self.conn.execute(
            "SELECT * FROM corpus_coverage WHERE run_id = ? AND product_id = ?",
            (run_id, product_id),
        ).fetchone()
        return None if row is None else self._row_to_corpus_coverage(row)

    def iter_corpus_coverage(self, run_id: str) -> Iterator[CorpusCoverage]:
        """Итерирует покрытие прогона; порядок ``product_id`` (детерминизм)."""
        for row in self.conn.execute(
            "SELECT * FROM corpus_coverage WHERE run_id = ? ORDER BY product_id",
            (run_id,),
        ):
            yield self._row_to_corpus_coverage(row)

    def iter_corpus_docs(self, run_id: str) -> Iterator[CorpusDoc]:
        """Итерирует документы корпуса прогона (детерминированно: ``product_id, doc_id``).

        Нужен ``hound diff`` для per-product doc-delta (``new``/``reused``/``removed``,
        09 §12.5): наборы ``content_hash`` по продуктам обоих прогонов. Метод **только
        читает** и схему не меняет.

        Область: ``corpus_docs`` не несёт ``run_id`` — в штатной раскладке БД принадлежит
        одному прогону (§4.4), поэтому выборка и есть корпус прогона. В общей ``--db``
        (оба цикла в одной БД) таблица — объединение корпусов, и delta вырождается в
        ``reused``; это ограничение раскладки, а не подмена данных. ``run_id``
        проверяется на присутствие в БД (иначе :class:`ValueError`): метод не выдаёт
        документы чужого/несуществующего прогона.

        :raises ValueError: строки прогона ``run_id`` нет в этой БД.
        """
        row = self.conn.execute(
            "SELECT 1 FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"прогон не найден: {run_id!r}")
        for doc in self.conn.execute(
            "SELECT * FROM corpus_docs ORDER BY product_id, doc_id"
        ):
            yield CorpusDoc(
                doc_id=doc["doc_id"],
                product_id=doc["product_id"],
                url=doc["url"],
                fetched_at=doc["fetched_at"],
                content_hash=doc["content_hash"],
                raw_path=doc["raw_path"],
                extractor_version=doc["extractor_version"],
                render_mode=doc["render_mode"],
                lang=doc["lang"],
            )

    def read_manifest(self, run_id: str) -> RunManifest:
        """Собирает ``RunManifest`` из строки ``runs`` (+ ``llm_scenarios``).

        Деплой-провенанс (``deploy_profile``/``edge_schema_version``/``edge_capabilities``,
        07 §7) и провенанс настроек (``settings_provenance``/``ranking_epsilon``) читаются
        из реальных колонок ``runs`` (схема v3.4, P1). Поля, не записанные манифестом,
        дают ``None`` (деплой-профиль/версия шва) либо пустой/дефолтный контейнер
        (``edge_capabilities``/``settings_provenance``/``CrawlProvenance``) — значения
        не выдумываются.

        :raises ValueError: если строка прогона отсутствует.
        """
        row = self.conn.execute(
            "SELECT * FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"прогон не найден: {run_id!r}")

        scenarios = [
            ScenarioBinding(
                run_id=s["run_id"],
                scenario=LLMScenario(s["scenario"]),
                model_id=s["model_id"],
                model_version=s["model_version"],
                endpoint_kind=EndpointKind(s["endpoint_kind"]),
                web_tool=bool(s["web_tool"]),
                decoder=s["decoder"],
                seed=s["seed"],
                temperature=s["temperature"],
                constrained_decoding=s["constrained_decoding"],
            )
            for s in self.conn.execute(
                "SELECT * FROM llm_scenarios WHERE run_id = ? ORDER BY scenario", (run_id,)
            )
        ]

        focus_modules = _parse_json_text(row["focus_modules"]) or []
        requested = row["requested_ranking"]
        computed = row["computed_ranking"]
        # A3 (v2.1): провенанс настроек и порог неразличимости — реальные колонки.
        settings_provenance = _parse_json_text(row["settings_provenance_json"]) or {}
        eps_raw = row["ranking_epsilon"]
        ranking_epsilon = float(eps_raw) if eps_raw is not None else 0.0
        # §4.4/Phase 2: провенанс краула (NULL → пустой ``CrawlProvenance()``).
        crawl_provenance_raw = _parse_json_text(row["crawl_provenance_json"])
        # 07 §7: версии edge-компонентов; NULL → пусто (значения не выдумываются).
        edge_capabilities_raw = _parse_json_text(row["edge_capabilities_json"])
        edge_capabilities = (
            edge_capabilities_raw if isinstance(edge_capabilities_raw, dict) else {}
        )
        return RunManifest(
            run_id=row["run_id"],
            case_id=row["case_id"],
            created_at=row["created_at"],
            mode=RunMode(row["mode"]),
            # Версии попадают в runs на финальной фазе; до этого — дефолт-заглушка,
            # чтобы манифест можно было прочитать на промежуточном состоянии.
            methodology_version=row["methodology_version"] or "0.0.0",
            package_version=row["package_version"] or "0.0.0",
            catalog_version=row["catalog_version"] or "0.0.0",
            status_dict_version=row["status_dict_version"] or "0.0.0",
            scoring_policy_version=row["scoring_policy_version"] or "0.0.0",
            anchor_product_id=row["anchor_product_id"],
            cohort_ref=row["cohort_ref"],
            focus_modules=focus_modules,
            index_build_id=row["index_build_id"],
            embed_model_id=row["embed_model_id"],
            retrieval_profile=row["retrieval_profile"],
            retrieval_cutoff_K=row["retrieval_cutoff_K"],
            retrieval_params_json=row["retrieval_params_json"],
            search_effort_budget_json=row["search_effort_budget_json"],
            requested_ranking=RankingPolicy(requested) if requested is not None else None,
            computed_ranking=RankingPolicy(computed) if computed is not None else None,
            ranking_epsilon=ranking_epsilon,
            settings_provenance=settings_provenance,
            crawl_provenance=CrawlProvenance.model_validate(crawl_provenance_raw or {}),
            # §13/Backlog B: операционные оси краула — реальные колонки.
            crawl_strategy=row["crawl_strategy"],
            crawl_rate_limit=row["crawl_rate_limit"],
            # §13/N6: дайджест разрешённой конфигурации контроля краула.
            crawl_config_digest=row["crawl_config_digest"],
            # §13/E2: дайджест package-scoped спецификаций реестров (граница ``execution_change``).
            registry_spec_digest=row["registry_spec_digest"],
            # 09 §12.5: отпечаток условий измерения (граница ``methodology_change``).
            # NULL → ``None`` (legacy-прогон; дайджест не фабрикуется).
            execution_contract_digest=row["execution_contract_digest"],
            llm_scenarios=scenarios,
            snapshot_date=row["snapshot_date"],
            deploy_profile=row["deploy_profile"],
            edge_schema_version=row["edge_schema_version"],
            edge_capabilities=edge_capabilities,
        )

    def read_run_index_ref(self, run_id: str) -> RunIndexRef | None:
        """Читает ``runs.package_id``/``runs.index_build_id`` прогона (P4b).

        Возвращает :class:`RunIndexRef` или ``None`` на неизвестный ``run_id``. Значения
        пакета/сборки — как записаны (могут быть ``None``; значения не выдумываются).
        """
        row = self.conn.execute(
            "SELECT package_id, index_build_id FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            return None
        return RunIndexRef(
            run_id=run_id,
            package_id=row["package_id"],
            index_build_id=row["index_build_id"],
        )

    def iter_index_manifest(self, index_build_id: str) -> Iterator[IndexManifestEntry]:
        """Итерирует продукт-партиции индексной сборки ``index_build_id`` (P4b).

        Зеркало :meth:`save_index_manifest`: пост-хок эксперимент разрешает пакет сборки и её
        ``embed_model_id``/``embed_dim`` из БД, не сканируя ``index_store/``. Порядок —
        ``product_id`` (детерминизм). Пустой итератор — сборки в БД нет.
        """
        for row in self.conn.execute(
            "SELECT * FROM index_manifest WHERE index_build_id = ? ORDER BY product_id",
            (index_build_id,),
        ):
            yield IndexManifestEntry(
                index_build_id=row["index_build_id"],
                partition_build_id=row["partition_build_id"],
                product_id=row["product_id"],
                package_id=row["package_id"],
                partition_key=row["partition_key"],
                snapshot_date=row["snapshot_date"],
                chunker_version=row["chunker_version"],
                extractor_version=row["extractor_version"],
                embed_model_id=row["embed_model_id"],
                embed_dim=row["embed_dim"],
                lexical_engine_version=row["lexical_engine_version"],
                vector_store=row["vector_store"],
                content_digest=row["content_digest"],
                store_path=row["store_path"],
                created_at=row["created_at"],
                stats_json=row["stats_json"],
            )

    # -- corpus ---------------------------------------------------------- #
    #: Колонки ``corpus_chunks``, которые несёт порт-тип ``Chunk``. ``url`` намеренно
    #: исключён: у ``Chunk`` нет поля ``url`` (URL резолвится через ``resolve_source_url``).
    _CHUNK_COLUMNS = (
        "chunk_id",
        "chunk_no",
        "text",
        "heading_path",
        "context_prefix",
        "content_hash",
    )

    @staticmethod
    def _row_to_chunk(row: sqlite3.Row) -> Chunk:
        """Восстанавливает порт-тип ``Chunk`` из строки ``corpus_chunks``.

        Единая точка конверсии для ``get_chunk``/``iter_chunks``: ``chunk_no=None``
        читается как ``0``, ``NULL`` в текстовых полях — как пустая строка.
        """
        return Chunk(
            chunk_id=row["chunk_id"],
            chunk_no=row["chunk_no"] if row["chunk_no"] is not None else 0,
            text=row["text"],
            heading_path=row["heading_path"] or "",
            context_prefix=row["context_prefix"] or "",
            content_hash=row["content_hash"] or "",
        )

    def get_chunk(self, chunk_id: str) -> Chunk | None:
        """Возвращает чанк как порт-тип ``Chunk`` или ``None`` (валидация цитат).

        ``corpus_chunks.url`` намеренно не переносится: у ``Chunk`` нет поля ``url``
        (URL резолвится отдельно через ``resolve_source_url``).
        """
        row = self.conn.execute(
            f"SELECT {', '.join(self._CHUNK_COLUMNS)} FROM corpus_chunks"
            " WHERE chunk_id = ?",
            (chunk_id,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_chunk(row)

    def iter_chunks(
        self, build_id: str, *, product_id: str | None = None
    ) -> Iterator[Chunk]:
        """Итерирует чанки сборки ``build_id`` как порт-типы ``Chunk`` (resume/rebuild).

        Выборка идёт по ``corpus_chunks.build_id`` (у таблицы **нет** ``run_id``),
        порядок детерминирован: ``product_id, chunk_no, chunk_id``. ``product_id=None``
        — все продукты сборки; иначе — только указанная продукт-партиция. ``url`` не
        переносится (см. :meth:`get_chunk`).
        """
        sql = (
            f"SELECT {', '.join(self._CHUNK_COLUMNS)} FROM corpus_chunks"
            " WHERE build_id = ?"
        )
        params: list[Any] = [build_id]
        if product_id is not None:
            sql += " AND product_id = ?"
            params.append(product_id)
        sql += " ORDER BY product_id, chunk_no, chunk_id"
        for row in self.conn.execute(sql, params):
            yield self._row_to_chunk(row)

    def resolve(self, source_id: str) -> ResolvedSource | None:
        """Детерминированно резолвит ``source_id`` в источник (I10/I15/I19), без сети.

        Принимает как «сырой» ``corpus_chunks.chunk_id``, так и форматированный
        ``source_id`` (``core.util.ids.source_id`` → ``product:doc:chunk_no[:build_id]``):

        1. прямой lookup по ``corpus_chunks.chunk_id``;
        2. иначе — разбор через ``parse_source_id`` и матч по
           ``(product_id, chunk_no[, build_id])`` (колонки ``corpus_chunks`` —
           ``chunk_no``/``product_id``/``build_id``).

        Малформед/неизвестный ``source_id`` → ``None`` (не исключение). Возвращает
        :class:`ResolvedSource` с ``chunk_id``/``url``/``content_hash``/``build_id``
        строки ``corpus_chunks``.
        """
        row = self.conn.execute(
            "SELECT chunk_id, url, content_hash, build_id FROM corpus_chunks"
            " WHERE chunk_id = ?",
            (source_id,),
        ).fetchone()
        if row is not None:
            return self._row_to_resolved(row)

        try:
            ref = parse_source_id(source_id)
        except ValueError:
            return None

        sql = (
            "SELECT chunk_id, url, content_hash, build_id FROM corpus_chunks"
            " WHERE product_id = ? AND doc_id = ? AND chunk_no = ?"
        )
        params: list[Any] = [ref.product_id, ref.doc_id, ref.chunk_no]
        if ref.build_id is not None:
            sql += " AND build_id = ?"
            params.append(ref.build_id)
        # Детерминизм: при нескольких кандидатах — стабильный порядок по PK.
        sql += " ORDER BY chunk_id LIMIT 1"
        row = self.conn.execute(sql, params).fetchone()
        if row is None:
            return None
        return self._row_to_resolved(row)

    @staticmethod
    def _row_to_resolved(row: sqlite3.Row) -> ResolvedSource:
        """Собирает :class:`ResolvedSource` из строки ``corpus_chunks``."""
        return ResolvedSource(
            chunk_id=row["chunk_id"],
            source_url=row["url"],
            content_hash=row["content_hash"],
            build_id=row["build_id"],
        )

    def resolve_source_url(self, source_id: str) -> str | None:
        """Резолв URL источника (I10/I15) через :meth:`resolve`.

        Сохранён как тонкая совместимая обёртка (T3.1): возвращает URL или ``None``.
        Hash-верификация вызывающим кодом — ``domain.anchors.verify_evidence_hash`` (I19).
        """
        resolved = self.resolve(source_id)
        if resolved is None:
            return None
        return resolved.source_url
