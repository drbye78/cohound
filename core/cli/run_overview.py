"""coHound — обзор прогонов для CLI (``hound runs``/``hound status``).

Модуль отвечает на два вопроса, на которые до сих пор не отвечала ни одна команда
(``12-cli-architecture-review.md`` §2.1/§3):

* **какие прогоны есть** — идентификатор прогона требовали 16 команд, а узнать его можно
  было только ``ls runs/``;
* **где прогон и что дальше** — состояние читается из durable-носителей прогона
  (``runs.state``/``runs.created_at``/ячейки/корпус) и превращается в **готовую следующую
  команду**, которую можно скопировать.

**Только чтение.** Всё открывается через ``build_store(..., read_only=True)``
(:func:`core.store.sqlite.connect_read_only`): каталог/файл/схема не создаются, прогон
живого писателя (WAL) читается без блокировки. Обзор никогда не пишет ни в БД, ни в файлы
прогона.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterator, Mapping
from enum import StrEnum
from pathlib import Path
from typing import Any

from core.cli.wiring import build_store, run_db_path
from core.pipeline import GATE_PHASES, PHASES
from core.store.sqlite import SchemaVersionMismatch

__all__ = [
    "DB_FILENAME",
    "RunLifecycle",
    "RunSummary",
    "effective_paths",
    "effective_runs_dir",
    "iter_run_dbs",
    "scan_runs",
    "summarize_run",
]


#: Имя операционной БД прогона в каталоге ``{runs_dir}/{run_id}/`` (03 §A.2).
DB_FILENAME = "cohound.db"


class RunLifecycle(StrEnum):
    """Состояние прогона, **выведенное** из durable-носителей (а не записанное куда-то).

    Это CLI-представление, а не доменное перечисление: значения не хранятся ни в одной
    колонке и не входят ни в один отпечаток. Источник — ``runs.state`` (карта
    ``{phase: state}``, :meth:`~core.store.sqlite_store.SqliteStore.get_phase_state`) и
    порядок :data:`core.pipeline.PHASES`. Поэтому секции в ``docs/status-dictionary.yaml``
    у него нет: словарь описывает то, что пишется и сравнивается, а не вывод команды.
    """

    #: Строки прогона в БД нет (каталог/БД созданы, но прогон не начинался).
    EMPTY = "empty"
    #: Ни одна фаза не исполнялась.
    NOT_STARTED = "not_started"
    #: Фазы исполнены, остановки нет — прогон можно продолжать.
    RUNNING = "running"
    #: Остановлен на гейте: нужно человеческое решение (G1–G4).
    BLOCKED = "blocked"
    #: Остановлен ошибкой фазы.
    FAILED = "failed"
    #: Дошёл до ``report`` — прогон полон, дальше артефакты.
    DONE = "done"
    #: БД не читается (чужая ``schema_version``, не-SQLite, ошибка I/O): прогон **не
    #: пропущен молча** — он назван в списке с причиной.
    SKIPPED = "skipped"


#: Фаза, достижение которой означает «прогон исполнен целиком».
_FINAL_PHASE = "report"


@dataclasses.dataclass(frozen=True)
class RunSummary:
    """Снимок состояния одного прогона (read-only, только наблюдённые значения).

    ``skipped_reason`` заполнен **только** для БД, которую нельзя прочитать (чужая
    ``schema_version``, не-SQLite, ошибка I/O): такая запись остаётся в списке видимой
    пометкой, а не исчезает и не роняет ``hound runs``.
    """

    run_id: str
    db: str
    lifecycle: RunLifecycle
    #: Путь к каталогу прогона (нужен для реконструкции путей ``run.yaml``/запроса гейта).
    run_dir: str
    package_id: str | None = None
    created_at: str | None = None
    #: Последняя **исполнявшаяся** фаза (по порядку :data:`core.pipeline.PHASES`).
    last_phase: str | None = None
    #: Её статус: ``ok``/``skipped``/``blocked``/``failed``.
    last_status: str | None = None
    #: Фаза-гейт, на которой прогон стоит (``g1``…``g4``), и доменный гейт (``G1``…``G4``).
    blocked_at: str | None = None
    gate: str | None = None
    #: Последнее записанное решение этого гейта (если есть): вердикт, кто и почему.
    gate_verdict: str | None = None
    gate_actor: str | None = None
    gate_notes: str | None = None
    #: Счётчики ячеек по состояниям (``pending``/``evaluated``/…); пусто — ячеек нет.
    cell_states: Mapping[str, int] = dataclasses.field(default_factory=dict)
    corpus_docs: int = 0
    corpus_chunks: int = 0
    #: Причина, по которой БД пропущена (не читается) — видимая, не тихая.
    skipped_reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (``--json``)."""
        return {
            "run_id": self.run_id,
            "db": self.db,
            "run_dir": self.run_dir,
            "package_id": self.package_id,
            "created_at": self.created_at,
            "state": self.lifecycle.value,
            "last_phase": self.last_phase,
            "last_status": self.last_status,
            "blocked_at": self.blocked_at,
            "gate": self.gate,
            "gate_decision": (
                {
                    "verdict": self.gate_verdict,
                    "actor": self.gate_actor,
                    "notes": self.gate_notes,
                }
                if self.gate_verdict is not None
                else None
            ),
            "counts": {
                "cells": dict(self.cell_states),
                "corpus_docs": self.corpus_docs,
                "corpus_chunks": self.corpus_chunks,
            },
            "skipped": self.skipped_reason,
        }


# ---------------------------------------------------------------------------
# Чтение состояния
# ---------------------------------------------------------------------------


def effective_paths(args: Any) -> Any:
    """Раскладка из composition root либо её резолв (прямой вызов хендлера)."""
    from core.cli.context import _resolve_paths

    paths = getattr(args, "paths", None)
    return paths if paths is not None else _resolve_paths(args)


def effective_runs_dir(args: Any) -> str:
    """Эффективный каталог прогонов: ``--runs-dir`` > resolved раскладка > ``runs``.

    ``main`` подставляет дефолт раскладки для команд из ``_RUNS_DIR_COMMANDS``; здесь — тот
    же расчёт для прямого вызова хендлера (тесты, встраивание), без второй формулы.
    """
    runs_dir = getattr(args, "runs_dir", None)
    return str(runs_dir) if runs_dir is not None else str(effective_paths(args).runs)


def iter_run_dbs(runs_dir: str | Path) -> Iterator[Path]:
    """Перечисляет ``{runs_dir}/*/cohound.db`` в детерминированном порядке.

    Отсутствующий каталог прогонов — **не** ошибка: итератор пуст (``hound runs`` тогда
    печатает пустой список и возвращает ``EXIT_OK``). Каталоги, где БД нет, не выдумываются.
    """
    root = Path(runs_dir)
    if not root.is_dir():
        return
    for child in sorted(root.iterdir(), key=lambda p: p.name):
        db_path = child / DB_FILENAME
        if db_path.is_file():
            yield db_path


def _phase_snapshot(phase_state: Mapping[str, str]) -> tuple[RunLifecycle, str | None, str | None]:
    """Сводит карту ``{phase: state}`` к ``(состояние, последняя фаза, фаза-гейт)``.

    Берётся **последняя по порядку PHASES** записанная фаза: она и есть точка остановки
    (``blocked``/``failed``) либо последняя отработавшая. ``blocked`` на фазе-гейте даёт
    ``BLOCKED`` + имя гейта; ``blocked`` на обычной фазе — тоже ``BLOCKED`` без гейта
    (решение принимает не человек-гейт, а предпосылка прогона).
    """
    executed = [(phase, phase_state[phase]) for phase in PHASES if phase in phase_state]
    if not executed:
        return RunLifecycle.NOT_STARTED, None, None
    last_phase, last_status = executed[-1]
    if last_status == "blocked":
        gate_phase = GATE_PHASES.get(last_phase)
        return (
            RunLifecycle.BLOCKED,
            last_phase,
            gate_phase.value if gate_phase is not None else None,
        )
    if last_status == "failed":
        return RunLifecycle.FAILED, last_phase, None
    if last_phase == _FINAL_PHASE and last_status in {"ok", "skipped"}:
        return RunLifecycle.DONE, last_phase, None
    return RunLifecycle.RUNNING, last_phase, None


def _latest_gate_decision(store: Any, run_id: str, gate: str | None) -> Any:
    """Последнее записанное решение этого гейта (``None`` — решения нет/гейт не назван).

    Нужно, чтобы ``hound status`` отличал «решения ещё никто не принимал» от «гейт
    отклонён: кто и почему». Решение ищется только по фазе-гейту: на обычной фазе
    (``blocked`` без гейта) решения не существует.
    """
    if gate is None:
        return None
    decisions = [
        decision
        for decision in store.iter_gate_decisions(run_id)
        if decision.gate.value == gate
    ]
    return decisions[-1] if decisions else None


def _cell_counts(store: Any, run_id: str) -> dict[str, int]:
    """Счётчики ячеек прогона по состояниям (``SELECT state, COUNT(*)``)."""
    rows = store.conn.execute(
        "SELECT state, COUNT(*) AS n FROM cells WHERE run_id = ? GROUP BY state ORDER BY state",
        (run_id,),
    ).fetchall()
    return {str(row["state"]): int(row["n"]) for row in rows}


def _corpus_counts(store: Any) -> tuple[int, int]:
    """``(документов, чанков)`` в корпусе БД.

    ``corpus_docs``/``corpus_chunks`` не несут ``run_id`` (раскладка — один прогон на БД;
    см. ``core.cli.commands.diff._doc_delta``), поэтому счётчики относятся к БД, а не к
    строке ``runs``. В штатной раскладке это одно и то же; в общей ``--db`` — видимое
    ограничение, а не выдуманное число.
    """
    docs = int(store.conn.execute("SELECT COUNT(*) FROM corpus_docs").fetchone()[0])
    chunks = int(store.conn.execute("SELECT COUNT(*) FROM corpus_chunks").fetchone()[0])
    return docs, chunks


def _summaries_from_store(store: Any, db_path: Path) -> list[RunSummary]:
    """Собирает сводки по всем строкам ``runs`` одной БД (обычно одна)."""
    rows = store.conn.execute(
        "SELECT run_id, package_id, created_at FROM runs ORDER BY created_at, run_id"
    ).fetchall()
    if not rows:
        # БД есть, но прогон в неё не писался: пустая схема не выдаётся за прогон.
        return [
            RunSummary(
                run_id=db_path.parent.name,
                db=str(db_path),
                run_dir=str(db_path.parent),
                lifecycle=RunLifecycle.EMPTY,
            )
        ]
    docs, chunks = _corpus_counts(store)
    summaries: list[RunSummary] = []
    for row in rows:
        run_id = str(row["run_id"])
        phase_state = store.get_phase_state(run_id)
        lifecycle, last_phase, gate = _phase_snapshot(phase_state)
        decision = _latest_gate_decision(store, run_id, gate)
        summaries.append(
            RunSummary(
                run_id=run_id,
                db=str(db_path),
                run_dir=str(db_path.parent),
                lifecycle=lifecycle,
                package_id=row["package_id"],
                created_at=row["created_at"],
                last_phase=last_phase,
                last_status=phase_state.get(last_phase) if last_phase else None,
                blocked_at=last_phase if lifecycle is RunLifecycle.BLOCKED else None,
                gate=gate,
                gate_verdict=decision.verdict.value if decision is not None else None,
                gate_actor=decision.actor if decision is not None else None,
                gate_notes=decision.notes if decision is not None else None,
                cell_states=_cell_counts(store, run_id),
                corpus_docs=docs,
                corpus_chunks=chunks,
            )
        )
    return summaries


def _skipped_summary(db_path: Path, reason: str) -> RunSummary:
    """Запись-пометка о пропущенной БД (видимая, а не тихая)."""
    return RunSummary(
        run_id=db_path.parent.name,
        db=str(db_path),
        run_dir=str(db_path.parent),
        lifecycle=RunLifecycle.SKIPPED,
        skipped_reason=reason,
    )


def summarize_run(runs_dir: str | Path, run_id: str) -> RunSummary | None:
    """Сводка по конкретному прогону; ``None`` — БД нет.

    Чужая ``schema_version``/нечитаемый файл **пробрасываются**: ``hound status`` про один
    прогон обязан сказать правду, а не показать «пусто» (список ``hound runs``, наоборот,
    пропускает такие БД с пометкой).
    """
    db_path = Path(run_db_path(runs_dir, run_id))
    if not db_path.is_file():
        return None
    with build_store(db_path, read_only=True) as store:
        summaries = _summaries_from_store(store, db_path)
    return next((item for item in summaries if item.run_id == run_id), None)


def scan_runs(runs_dir: str | Path) -> list[RunSummary]:
    """Все прогоны каталога ``runs_dir``; нечитаемые БД — пометкой ``skipped``.

    Пропуск **видим**: запись остаётся в списке с причиной. В ``runs/`` этого проекта
    лежат прогоны схем 3.9.0/3.10.0, и ``hound runs`` не имеет права падать из-за них —
    но и молча их терять тоже (иначе «список прогонов» врёт о содержимом каталога).
    """
    summaries: list[RunSummary] = []
    for db_path in iter_run_dbs(runs_dir):
        try:
            with build_store(db_path, read_only=True) as store:
                summaries.extend(_summaries_from_store(store, db_path))
        except SchemaVersionMismatch as exc:
            summaries.append(_skipped_summary(db_path, f"schema_version: {exc}"))
        except Exception as exc:  # noqa: BLE001 — нечитаемая БД не роняет список
            summaries.append(
                _skipped_summary(db_path, f"{type(exc).__name__}: {exc}")
            )
    return summaries
