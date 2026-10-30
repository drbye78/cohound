"""``backup``/``restore``: бандл прогона и durable-состояния пакета (09 §14.4, §13).

Один модуль регистрирует **две** top-level команды (``backup`` и ``restore``): это одна
операция, разведённая на запись и чтение, и держать их врозь значило бы разнести
согласованный формат архива по двум файлам. Конвенция «модуль = команда» здесь
нарушена осознанно: ``configure`` регистрирует обе подкоманды, тела — :func:`run_backup`
и :func:`run_restore`.

Что важно в семантике (а не в коде):

* бэкап прогона — библиотечная операция :func:`core.store.backup.backup_run`;
* ``--include-state`` добавляет **согласованный снимок** ``state/{package_id}/state.db``
  (``sqlite3.Connection.backup``, а не копия файла: у живой БД есть ``-wal``) отдельным
  членом ``state/state.db``; отсутствие состояния при заданном флаге — **видимая**
  строка, не тихий пропуск;
* восстановление состояния — **явное** действие (``--state-into``), потому что
  перезапись живой ``state.db`` обязана быть произнесена, а не сделана молча;
* восстановление прогона **не** перезаписывает непустой каталог: отказ с его именем.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from core.cli.context import _add_common, _print_json, _resolve_paths
from core.cli.errors import EXIT_NOT_FOUND, EXIT_OK, EXIT_USAGE, CliError
from core.store.backup import (
    STATE_MEMBER,
    archive_has_state,
    backup_run,
    restore_run,
    restore_state,
)
from core.store.state_sqlite import state_db_path

__all__ = ["configure", "run_backup", "run_restore"]

#: Коды ошибок команд: бандл/раскладка. Раздельные — чтобы префикс сообщения называл
#: ту команду, которую оператор вызвал (``backup: …`` ‖ ``restore: …``).
_BACKUP_CODE = "backup"
_RESTORE_CODE = "restore"


def _runs_dir(args: argparse.Namespace) -> Path:
    """Эффективный каталог прогонов: ``--runs-dir`` > раскладка ``paths.runs``.

    Команда не входит в ``_RUNS_DIR_COMMANDS`` (``core.cli.main``), поэтому дефолт
    раскладки подставляется здесь — composition root уже положил ``args.paths``;
    при прямом вызове хендлера (тесты) раскладка резолвится сама.
    """
    explicit = getattr(args, "runs_dir", None)
    if explicit is not None:
        return Path(explicit)
    paths = getattr(args, "paths", None)
    if paths is None:
        paths = _resolve_paths(args)
    return Path(paths.runs)


def _state_root(args: argparse.Namespace) -> Path:
    """Эффективный корень durable-состояния (``state/{package_id}/`` живёт внутри)."""
    paths = getattr(args, "paths", None)
    if paths is None:
        paths = _resolve_paths(args)
    return Path(paths.state)


def _package_id(package: str | Path) -> str:
    """``package_id`` — имя каталога пакета (та же конвенция, что у ``state.db``)."""
    return Path(package).resolve().name


def run_backup(args: argparse.Namespace) -> int:
    """``backup``: бандл прогона ``runs/{run_id}/`` (+ durable-состояние по флагу).

    :returns: :data:`EXIT_OK` при созданном архиве.
    :raises CliError: каталог прогона не найден, либо ``--include-state`` без
        ``--package`` (:data:`EXIT_USAGE`).
    """
    run_dir = _runs_dir(args) / args.run_id
    if not run_dir.is_dir():
        raise CliError(
            f"каталог прогона не найден: {run_dir}",
            code=_BACKUP_CODE,
            exit_code=EXIT_USAGE,
        )

    notes: list[str] = []
    state_db: Path | None = None
    state_candidate: Path | None = None
    include_state = bool(getattr(args, "include_state", False))
    if include_state:
        package = getattr(args, "package", None)
        if not package:
            raise CliError(
                "--include-state требует --package: durable-состояние привязано к "
                "пакету (state/{package_id}/state.db), а не к прогону",
                code="state",
                exit_code=EXIT_USAGE,
            )
        state_candidate = state_db_path(_state_root(args), _package_id(package))
        if state_candidate.is_file():
            state_db = state_candidate
        else:
            # Тихий пропуск запрещён: оператор просил состояние и обязан узнать, что его нет.
            notes.append(f"состояние не найдено: {state_candidate} — архив без state")

    archive = backup_run(run_dir, args.dest, state_db=state_db)

    payload: dict[str, Any] = {
        "ok": True,
        "run_id": args.run_id,
        "archive": str(archive),
        "include_state": include_state,
        "state_included": state_db is not None,
        "state_db": str(state_db) if state_db is not None else None,
        "state_member": STATE_MEMBER if state_db is not None else None,
        "notes": notes,
    }
    if args.json:
        _print_json(payload)
    else:
        sys.stdout.write(f"run_id: {payload['run_id']}\n")
        sys.stdout.write(f"archive: {payload['archive']}\n")
        if state_db is not None:
            sys.stdout.write(
                f"state: включён ({STATE_MEMBER} из {state_db})\n"
            )
        elif state_candidate is not None:
            sys.stdout.write(f"state: не найден ({state_candidate}) — архив без state\n")
        else:
            sys.stdout.write("state: не запрошен (--include-state выключен)\n")
        for note in notes:
            sys.stdout.write(f"note: {note}\n")
    return EXIT_OK


def run_restore(args: argparse.Namespace) -> int:
    """``restore``: восстановление прогона и/или durable-состояния из бандла.

    Разделение намеренное: файлы прогона и ``state.db`` — разные объекты с разной
    ценой ошибки. Прогон восстанавливается в **пустой** каталог (иначе отказ с именем
    каталога); состояние — только по явному ``--state-into`` и только с ``--force``,
    если целевая ``state.db`` уже существует.

    :returns: :data:`EXIT_OK` при выполненном восстановлении.
    :raises CliError: архив не найден (:data:`EXIT_NOT_FOUND`); не задано ни одного
        назначения; каталог прогона непуст; архив без состояния при ``--state-into``;
        целевая ``state.db`` существует без ``--force`` (все — :data:`EXIT_USAGE`).
    """
    archive = Path(args.archive)
    if not archive.is_file():
        raise CliError(
            f"архив не найден: {archive}",
            code=_RESTORE_CODE,
            exit_code=EXIT_NOT_FOUND,
        )

    dest_arg = getattr(args, "dest_dir", None)
    state_arg = getattr(args, "state_into", None)
    if dest_arg is None and state_arg is None:
        raise CliError(
            "нужно указать --dest-dir (восстановить прогон) и/или --state-into "
            "(восстановить состояние пакета)",
            code=_RESTORE_CODE,
            exit_code=EXIT_USAGE,
        )

    dest: Path | None = Path(dest_arg) if dest_arg is not None else None
    if dest is not None and dest.exists() and any(dest.iterdir()):
        # Восстановление поверх живого прогона не «сливается» и не перезаписывает
        # молча: два прогона в одном каталоге — это не восстановление.
        raise CliError(
            f"каталог восстановления не пуст: {dest} — восстановление не перезаписывает "
            "существующий прогон; укажите пустой каталог",
            code=_RESTORE_CODE,
            exit_code=EXIT_USAGE,
        )

    has_state = archive_has_state(archive)
    notes: list[str] = []
    if state_arg is not None and not has_state:
        raise CliError(
            f"в архиве нет состояния ({STATE_MEMBER}): {archive} — восстановление "
            "состояния невозможно (бэкап сделан без --include-state)",
            code="state",
            exit_code=EXIT_USAGE,
        )

    state_written: Path | None = None
    try:
        if dest is not None:
            # Состояние при восстановлении прогона не трогается: это отдельное явное
            # действие (ниже), иначе перезапись живой state.db была бы побочным эффектом.
            restore_run(archive, dest)
        if state_arg is not None:
            state_written = restore_state(
                archive,
                state_arg,
                overwrite=bool(getattr(args, "force", False)),
            )
    except FileExistsError as exc:
        # Перезапись живого состояния — решение оператора, а не побочный эффект.
        raise CliError(str(exc), code="state", exit_code=EXIT_USAGE) from exc

    if dest is not None and has_state and state_arg is None:
        notes.append(
            f"архив содержит состояние ({STATE_MEMBER}); оно НЕ восстановлено — "
            "восстановление состояния это явное действие: "
            "hound restore --archive <zip> --state-into state/{package_id} [--force]"
        )

    payload: dict[str, Any] = {
        "ok": True,
        "archive": str(archive),
        "restored_dir": str(dest) if dest is not None else None,
        "state_restored": state_written is not None,
        "state_db": str(state_written) if state_written is not None else None,
        "archive_has_state": has_state,
        "notes": notes,
    }
    if args.json:
        _print_json(payload)
    else:
        sys.stdout.write(f"archive: {payload['archive']}\n")
        if dest is not None:
            sys.stdout.write(f"restored: {payload['restored_dir']}\n")
        else:
            sys.stdout.write("restored: — (только состояние)\n")
        if state_written is not None:
            sys.stdout.write(f"state: {STATE_MEMBER} -> {state_written}\n")
        else:
            sys.stdout.write("state: не восстановлено\n")
        for note in notes:
            sys.stdout.write(f"note: {note}\n")
    return EXIT_OK


def configure(sub: Any) -> None:
    """Регистрирует подкоманды ``backup`` и ``restore``."""
    p_backup = sub.add_parser(
        "backup",
        help="бандл прогона (.zip); --include-state добавляет durable-состояние пакета",
    )
    p_backup.add_argument(
        "--runs-dir",
        default=None,
        help="каталог прогонов (дефолт — раскладка paths.runs); архив кладётся рядом с прогоном",
    )
    p_backup.add_argument("--run-id", required=True, help="run_id прогона (обязателен)")
    p_backup.add_argument(
        "--dest",
        default=None,
        help="файл архива либо существующий каталог; дефолт — {runs_dir}/{run_id}.zip",
    )
    p_backup.add_argument(
        "--package",
        default=None,
        help="каталог пакета: нужен для --include-state (state/{package_id}/state.db)",
    )
    p_backup.add_argument(
        "--include-state",
        dest="include_state",
        action="store_true",
        help="включить согласованный снимок state/{package_id}/state.db отдельным членом state/state.db",
    )
    _add_common(p_backup)
    p_backup.set_defaults(handler=run_backup)

    p_restore = sub.add_parser(
        "restore",
        help="восстановление прогона из .zip (и явно — durable-состояния пакета)",
    )
    p_restore.add_argument("--archive", required=True, help="путь к .zip бандлу")
    p_restore.add_argument(
        "--dest-dir",
        dest="dest_dir",
        default=None,
        help="каталог восстановления прогона (обязан быть пустым или отсутствовать)",
    )
    p_restore.add_argument(
        "--state-into",
        dest="state_into",
        default=None,
        help="каталог для state.db (например state/{package_id}); явное восстановление состояния",
    )
    p_restore.add_argument(
        "--force",
        action="store_true",
        help="разрешить перезапись существующего state.db (опасное действие)",
    )
    _add_common(p_restore)
    p_restore.set_defaults(handler=run_restore)
