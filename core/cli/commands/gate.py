"""``gate``: запись человеческого решения гейта G1–G4 (S9/I6/I7; ``03`` §E.2).

Вынесено по образцу :mod:`core.cli.commands.review`. Модуль экспортирует
:func:`configure` (регистрация аргументов подкоманды) и :func:`run` (тело команды).

**Семантика решения.** В таблице ``gate_decisions`` **нет** колонки исхода/вердикта:
гейт считается пройденным **самим фактом строки** (:func:`core.pipeline.gates.is_gate_passed`).
Поэтому команда записывает именно **утверждение**; «отказ» представляется **отсутствием**
строки — прогон остаётся ``blocked`` и продолжения не получает. Повторная подача того же
решения (run/gate/target/actor/notes) идемпотентно перезаписывает ту же строку
(детерминированный ``decision_id``), а не плодит дубли.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from typing import Any

import argparse

from core.cli.context import (
    _add_common,
    _infer_run_id,
    _print_gate,
    _print_json,
    _resolve_db_path,
)
from core.cli.errors import EXIT_OK
from core.cli.wiring import build_store
from core.domain import Gate
from core.pipeline import record_decision


def run(args: argparse.Namespace) -> int:
    """``gate``: записывает человеческое решение гейта G1–G4 (S9/I6/I7, ``03`` §E.2).

    Резолвит хранилище как ``sensitivity`` (``--db`` приоритетнее ``--runs-dir`` +
    ``--run-id``; ``run_id`` выводится, если в БД ровно один прогон), валидирует
    существование прогона через :meth:`~core.store.sqlite_store.SqliteStore.read_manifest`
    и персистит утверждение через :func:`core.pipeline.gates.record_decision`. ``actor``
    обязателен (I6/I7); пустой ``actor`` — :class:`ValueError` → ``EXIT_USAGE``. Запись
    строки делает гейт пройденным, поэтому ``hound run --until …`` перестаёт на нём
    останавливаться.
    """
    db_path = _resolve_db_path(args)
    run_id = args.run_id
    payload: dict[str, Any] = {}
    with build_store(db_path, profile=args.profile) as store:
        if run_id is None:
            run_id = _infer_run_id(store)
        store.read_manifest(run_id)  # валидация: прогон существует
        decision = record_decision(
            store,
            run_id,
            Gate(args.gate),
            actor=args.actor,
            target=args.target,
            notes=args.notes,
        )
        payload = {
            "ok": True,
            "run_id": run_id,
            "gate": decision.gate.value,
            "decision_id": decision.decision_id,
            "actor": decision.actor,
            "decided_at": decision.decided_at,
            "target": decision.target,
            "notes": decision.notes,
        }

    if args.json:
        _print_json(payload)
    else:
        _print_gate(payload)
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``gate``."""
    p_gate = sub.add_parser(
        "gate", help="записать человеческое решение гейта G1–G4 (утверждение; S9/I6/I7)"
    )
    gate_source = p_gate.add_mutually_exclusive_group(required=True)
    gate_source.add_argument("--db", default=None, help="путь к cohound.db прогона")
    gate_source.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (runs/{run_id}/cohound.db; требует --run-id)",
    )
    p_gate.add_argument(
        "--run-id",
        default=None,
        help="обязателен с --runs-dir; с --db выводится, если прогон один",
    )
    p_gate.add_argument(
        "--gate",
        required=True,
        choices=[g.value for g in Gate],
        help="гейт G1–G4 (утверждение; отказ — просто не записывать решение)",
    )
    p_gate.add_argument(
        "--actor",
        required=True,
        help="человек, принимающий решение (I6/I7)",
    )
    p_gate.add_argument(
        "--target",
        default=None,
        help="что подтверждено: cohort/cell/candidate_id (для G2 — per-candidate)",
    )
    p_gate.add_argument("--notes", default=None, help="произвольная заметка к решению")
    _add_common(p_gate)
    p_gate.set_defaults(handler=run)
