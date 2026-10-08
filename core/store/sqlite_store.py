"""coHound — конкретное операционное хранилище ``SqliteStore`` (L3, W3/T3.1).

Реализует ``StorePort`` и ``GateStore`` (``core/ports/store.py``) поверх схемы §4.4
(``core/store/sqlite.py``: 11 таблиц + ``schema_meta``, индексы, триггеры I4/I6/I7/I13/I18).

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
    EndpointKind,
    EvidenceType,
    Gate,
    GateDecision,
    LLMScenario,
    ProtocolExhaustion,
    RankingPolicy,
    RegistryFact,
    RunConfig,
    RunManifest,
    RunMode,
    ScenarioBinding,
    SearchLogEntry,
    SourceKind,
    StaleReason,
    Status,
)
from core.ports.types import Chunk
from core.store.sqlite import connect

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
        """Фиксирует состояние фазы в ``runs.state`` как ``"{phase}:{state}"`` (конвенция)."""
        with self.conn:
            self.conn.execute(
                "UPDATE runs SET state = ? WHERE run_id = ?",
                (f"{phase}:{state}", run_id),
            )

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
        )

    def save_cell(self, cell: Cell) -> None:
        """Upsert ячейки по PK ``(run_id, product_id, param_id)``.

        Enum-поля сериализуются в ``.value``; ``status=None`` для failed/preliminary
        сохраняется как NULL. Триггеры I4/I6/I7/I13 применяются БД.

        Примечание (T3.1): ``absence_rule``/``evidence``/``absence_evidence`` не имеют
        колонок в ``cells`` — evidence живёт в ``cell_evidence``, ``absence_rule`` —
        свойство каталога; при чтении восстанавливаются дефолтами модели.
        """
        placeholders = ", ".join("?" for _ in _CELL_COLUMNS)
        with self.conn:
            self.conn.execute(
                f"INSERT OR REPLACE INTO cells ({', '.join(_CELL_COLUMNS)})"
                f" VALUES ({placeholders})",
                self._cell_row(cell),
            )

    def save_cells_batch(self, cells: list[Cell], chunk_size: int = DEFAULT_BATCH_SIZE) -> None:
        """Пишет ячейки пачками: одна транзакция на ``chunk_size`` (по умолчанию 200)."""
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

    def read_manifest(self, run_id: str) -> RunManifest:
        """Собирает ``RunManifest`` из строки ``runs`` (+ ``llm_scenarios``).

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
            llm_scenarios=scenarios,
            snapshot_date=row["snapshot_date"],
        )

    # -- corpus ---------------------------------------------------------- #
    def get_chunk(self, chunk_id: str) -> Chunk | None:
        """Возвращает чанк как порт-тип ``Chunk`` или ``None`` (валидация цитат).

        ``corpus_chunks.url`` намеренно не переносится: у ``Chunk`` нет поля ``url``
        (URL резолвится отдельно через ``resolve_source_url``).
        """
        row = self.conn.execute(
            "SELECT chunk_id, chunk_no, text, heading_path, context_prefix, content_hash"
            " FROM corpus_chunks WHERE chunk_id = ?",
            (chunk_id,),
        ).fetchone()
        if row is None:
            return None
        return Chunk(
            chunk_id=row["chunk_id"],
            chunk_no=row["chunk_no"] if row["chunk_no"] is not None else 0,
            text=row["text"],
            heading_path=row["heading_path"] or "",
            context_prefix=row["context_prefix"] or "",
            content_hash=row["content_hash"] or "",
        )

    def resolve_source_url(self, source_id: str) -> str | None:
        """Базовый резолв URL источника (I10/I15): искомое — ``corpus_chunks.chunk_id``.

        Hash-верификация резолва — T3.4. Здесь: возвращает ``url`` чанка или ``None``.
        """
        row = self.conn.execute(
            "SELECT url FROM corpus_chunks WHERE chunk_id = ?",
            (source_id,),
        ).fetchone()
        if row is None:
            return None
        return row["url"]
