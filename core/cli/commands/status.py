"""``status``: где прогон и что делать дальше (read-only) — ответ, которого не было.

Сценарий оператора (``12-cli-architecture-review.md`` §3) — девять вызовов, четыре из
которых ручные решения, и между ними нужно помнить ``run_id``/``--package``/``--run-yaml``/
``--urls``. Ни одна команда не отвечала на вопрос «где я и что дальше»: ``run`` печатал
``stopped_at``, но команду продолжения приходилось собирать руками.

Команда читает durable-состояние прогона (``runs.state``, ячейки, корпус, решения гейтов)
**только на чтение** и печатает:

* фазу остановки и состояние (``blocked``/``failed``/``running``/``done``);
* гейт, если прогон стоит на гейте, и записанное по нему решение (кто/почему);
* счётчики (ячейки по состояниям, документы/чанки корпуса);
* **готовую следующую команду** строкой, которую можно скопировать.

Отсутствующий прогон → ``EXIT_NOT_FOUND`` (4), а не «неправильно вызвал» (2): скрипт
обязан отличать «прогон ещё не создан» от опечатки.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from core.cli.context import _add_common, _print_json
from core.cli.errors import EXIT_NOT_FOUND, EXIT_OK, CliError
from core.cli.run_overview import (
    DB_FILENAME,
    RunLifecycle,
    RunSummary,
    effective_paths,
    effective_runs_dir,
    summarize_run,
)
from core.store.sqlite import StoreNotInitialized

#: Имя иммутабельной копии входа в каталоге прогона (03 §A.2) — ``--run-yaml`` продолжения.
RUN_YAML_FILENAME = "run.yaml"
#: Относительный путь операторского корпусного входа в пакете (03 §D.6).
CORPUS_URLS_RELPATH = Path("sources") / "corpus_urls.yaml"


def _run_command(summary: RunSummary, runs_dir: str, paths: Any) -> tuple[str, list[str]]:
    """Команда продолжения прогона: ``hound run …`` с восстановленными путями.

    Источники путей (не выдумываются):

    * ``--run-yaml`` — иммутабельная копия входа в ``{runs_dir}/{run_id}/run.yaml``
      (её пишет сам прогон, ``_snapshot_run_inputs``): это ровно тот вход, что исполнялся;
    * ``--package`` — resolved ``paths.packages / package_id`` из строки прогона, если
      такой каталог существует;
    * ``--urls`` — ``{package}/sources/corpus_urls.yaml``, если он там есть (операторский
      корпусный вход необязателен).

    Чего восстановить нельзя, печатается заглушкой ``<…>`` и называется в ``notes``:
    команда с выдуманным путём выглядела бы рабочей и молча создала бы **другой** прогон.
    """
    notes: list[str] = []
    run_dir = Path(summary.run_dir)
    run_yaml = run_dir / RUN_YAML_FILENAME
    run_yaml_arg = str(run_yaml) if run_yaml.is_file() else "<run.yaml>"
    if not run_yaml.is_file():
        notes.append(
            f"копия run.yaml в {run_yaml} не найдена — подставьте исходный путь вручную"
        )
    package_id = summary.package_id or ""
    package_dir = Path(paths.packages) / package_id if package_id else None
    if package_dir is not None and package_dir.is_dir():
        package_arg = str(package_dir)
    else:
        package_arg = f"<каталог пакета {package_id}>" if package_id else "<каталог пакета>"
        notes.append(
            "каталог пакета не найден в раскладке ("
            f"{Path(paths.packages)}/{package_id or '…'}) — подставьте путь вручную"
        )
    command = (
        f"hound run --runs-dir {runs_dir} --run-yaml {run_yaml_arg} --package {package_arg}"
    )
    urls = package_dir / CORPUS_URLS_RELPATH if package_dir is not None else None
    if urls is not None and urls.is_file():
        command += f" --urls {urls}"
    return command, notes


def _next_action(summary: RunSummary, runs_dir: str, paths: Any) -> tuple[str, str, list[str]]:
    """``(команда, почему именно она, заметки)`` для текущего состояния прогона."""
    run_id = summary.run_id
    if summary.lifecycle is RunLifecycle.BLOCKED and summary.gate is not None:
        gate_request = Path(summary.run_dir) / "gates" / f"{summary.gate}-request.json"
        command = (
            f"hound gate --runs-dir {runs_dir} --run-id {run_id} --gate {summary.gate} "
            '--actor <кто> --notes "<почему>"'
        )
        if gate_request.is_file():
            reason = (
                f"гейт {summary.gate} ждёт человеческого решения; "
                f"предмет и subject_digest — в {gate_request} (гейт читает его сам)"
            )
        else:
            # Запроса нет (прогон шёл без записи запроса/каталог перенесён): без отпечатка
            # гейт откажет, поэтому флаг назван сразу, а не выясняется при вызове.
            command += " --subject-digest <digest>"
            reason = (
                f"гейт {summary.gate} ждёт решения, но файла запроса {gate_request} нет — "
                "передайте --subject-digest явно (иначе гейт откажет: решение без привязки)"
            )
        notes: list[str] = []
        if summary.gate_verdict == "rejected":
            notes.append(
                f"гейт {summary.gate} уже ОТКЛОНЁН ({summary.gate_actor}"
                f"{': ' + summary.gate_notes if summary.gate_notes else ''}) — прогон "
                "останется blocked, пока по текущему предмету не будет нового решения"
            )
        elif summary.gate_verdict == "approved":
            notes.append(
                f"по гейту {summary.gate} уже записано одобрение, но прогон стоит: "
                "предмет решения, вероятно, изменился — перезапустите прогон, чтобы "
                "увидеть актуальный предмет"
            )
        return command, reason, notes
    if summary.lifecycle is RunLifecycle.DONE:
        return (
            f"hound report --runs-dir {runs_dir} --run-id {run_id}",
            "прогон дошёл до фазы report — дальше только артефакты (отчёт не пишет в БД)",
            [],
        )
    command, notes = _run_command(summary, runs_dir, paths)
    if summary.lifecycle is RunLifecycle.BLOCKED:
        reason = (
            f"прогон заблокирован на фазе {summary.blocked_at} (не гейт): "
            "устраните причину из errors прогона и продолжите"
        )
    elif summary.lifecycle is RunLifecycle.FAILED:
        reason = (
            f"прогон остановлен ошибкой фазы {summary.last_phase} — "
            "продолжение повторяет незавершённую фазу (resume)"
        )
    elif summary.lifecycle is RunLifecycle.NOT_STARTED:
        reason = "прогон создан, но ни одна фаза не исполнялась — запустите его"
    else:
        reason = (
            f"прогон не завершён (последняя отработавшая фаза — {summary.last_phase}); "
            "продолжение подхватит его с места остановки (resume)"
        )
    return command, reason, notes


def _print_status(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод: состояние, счётчики и следующая команда."""
    lines = [
        f"run_id: {payload['run_id']}",
        f"db: {payload['db']}",
        f"package: {payload['package_id'] or '—'}",
        f"created_at: {payload['created_at'] or '—'}",
        f"state: {payload['state']}",
        f"last_phase: {payload['last_phase'] or '—'}"
        + (f" ({payload['last_status']})" if payload["last_status"] else ""),
    ]
    if payload["blocked_at"]:
        gate = f" (гейт {payload['gate']})" if payload["gate"] else ""
        lines.append(f"blocked_at: {payload['blocked_at']}{gate}")
    decision = payload.get("gate_decision")
    if decision is not None:
        note = f": {decision['notes']}" if decision.get("notes") else ""
        lines.append(
            f"gate_decision: {decision['verdict']} ({decision['actor']}){note}"
        )
    cell_counts = payload["counts"]["cells"]
    if cell_counts:
        lines.append(
            "cells: "
            + ", ".join(f"{state}={cell_counts[state]}" for state in sorted(cell_counts))
        )
    else:
        lines.append("cells: —")
    lines.append(
        "corpus: docs={corpus_docs} chunks={corpus_chunks}".format(**payload["counts"])
    )
    lines.append(f"next: {payload['next']['command']}")
    lines.append(f"why: {payload['next']['reason']}")
    for note in payload.get("notes", ()):
        lines.append(f"note: {note}")
    sys.stdout.write("\n".join(lines) + "\n")


def run(args: argparse.Namespace) -> int:
    """``status``: печатает состояние прогона и готовую следующую команду (read-only).

    :raises CliError: прогон не найден (:data:`EXIT_NOT_FOUND`) — файла БД нет либо в ней
        нет строки этого прогона. Это «сущность не найдена», а не «неверный вызов».
    """
    runs_dir = effective_runs_dir(args)
    run_id = args.run_id
    paths = effective_paths(args)
    try:
        summary = summarize_run(runs_dir, run_id)
    except StoreNotInitialized as exc:
        raise CliError(str(exc), code="not_found", exit_code=EXIT_NOT_FOUND) from exc
    if summary is None:
        raise CliError(
            f"прогон {run_id!r} не найден: {Path(runs_dir) / run_id / DB_FILENAME} "
            "отсутствует (список прогонов — hound runs)",
            code="not_found",
            exit_code=EXIT_NOT_FOUND,
        )
    if summary.lifecycle is RunLifecycle.EMPTY:
        raise CliError(
            f"прогон {run_id!r} не найден: БД {summary.db} есть, но строки прогона в ней "
            "нет (пустая схема; список прогонов — hound runs)",
            code="not_found",
            exit_code=EXIT_NOT_FOUND,
        )
    command, reason, notes = _next_action(summary, runs_dir, paths)
    payload: dict[str, Any] = {
        "ok": True,
        **summary.as_dict(),
        "next": {"command": command, "reason": reason},
        "notes": notes,
    }
    if args.json:
        _print_json(payload)
    else:
        _print_status(payload)
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``status``."""
    p_status = sub.add_parser(
        "status",
        help="состояние прогона и следующая команда (read-only): run_id, фаза, гейт, "
        "счётчики",
    )
    p_status.add_argument(
        "run_id",
        help="идентификатор прогона (имя каталога в runs/; список — hound runs)",
    )
    p_status.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (дефолт — раскладка: config/paths.yaml/COHOUND_RUNS_DIR/"
        "COHOUND_HOME, иначе runs)",
    )
    _add_common(p_status)
    p_status.set_defaults(handler=run)
