"""coHound — backup/restore прогона и GC иммутабельных индексных сборок (W12/T12.2).

Покрывает две ортогональные задачи §13 регламента:

1. **Backup/restore прогона** (``runs/{run_id}/``). Бандл — детерминированный
   ``.zip`` из stdlib: ``run.yaml``, ``run_manifest.yaml``, консистентный снапшот
   ``cohound.db``, ``logs/``, ``prompts/``, ``outputs/``. Снапшот БД делается
   штатным API ``sqlite3.Connection.backup`` (не копированием файла) — иначе
   можно получить рассогласование WAL/SHM (§A.2, §R7). Секреты в архив не
   попадают (см. :data:`_SECRET_NAMES`/:data:`_SECRET_SUFFIXES`).
2. **GC индексных сборок** (``index_store/{package}/{build}/{product_id}/``).
   Старые сборки не удаляются «как попало» (§13): политика хранит ``keep``
   новейших сборок плюс явно защищённые ``protect`` (например, сборки, на
   которые ссылаются свежие манифесты прогонов). Удаление — только при явном
   ``dry_run=False``; по умолчанию функция лишь сообщает, что *было бы* удалено.

Нейтрален (§1.3), только stdlib: ``sqlite3``/``zipfile``/``shutil``/``pathlib``/``os``.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
import zipfile
from collections.abc import Iterable
from pathlib import Path, PurePosixPath

__all__ = ["backup_run", "restore_run", "list_builds", "gc_index_builds"]

#: Имя файла операционной БД прогона (§A.2).
_DB_NAME = "cohound.db"

#: Фиксированная метка времени членов архива — воспроизводимость бандла.
#: Минимально допустимая для ZIP дата (MS-DOS epoch).
_FIXED_ZIP_DATE = (1980, 1, 1, 0, 0, 0)

#: Имена файлов, считающихся секретами и исключаемых из бандла.
_SECRET_NAMES = frozenset(
    {
        ".env",
        ".envrc",
        ".netrc",
        "credentials",
        "credentials.json",
        "secrets.yaml",
        "secrets.yml",
        "secret.yaml",
        "secret.yml",
        "auth.json",
        "token.json",
        "id_rsa",
        "id_dsa",
        "id_ecdsa",
        "id_ed25519",
    }
)

#: Расширения файлов-секретов, исключаемых из бандла.
_SECRET_SUFFIXES = (".pem", ".key", ".p12", ".pfx", ".jks", ".keystore")


def _is_excluded(rel: PurePosixPath) -> bool:
    """Является ли файл бандла служебным/секретным (не попадает в архив).

    Исключаются: sidecar-файлы SQLite (``-wal``/``-shm`` — заменяются
    консистентным снапшотом), байткод (``__pycache__``/``*.pyc``), файлы с
    секретными именами/расширениями (``.env*``, ``*.pem``, ``*.key`` и т. п.).
    """
    name = rel.name
    if name in {f"{_DB_NAME}-wal", f"{_DB_NAME}-shm"}:
        return True
    if "__pycache__" in rel.parts or name.endswith(".pyc"):
        return True
    if name in _SECRET_NAMES or name.startswith(".env"):
        return True
    if name.endswith(_SECRET_SUFFIXES):
        return True
    return False


def _snapshot_db(source_db: Path, target_db: Path) -> None:
    """Консистентный снапшот SQLite через ``sqlite3.Connection.backup``.

    Штатное API учитывает WAL и активные транзакции: снапшот содержит
    согласованное зафиксированное состояние БД, в отличие от наивного
    копирования файла (которое может «потерять» данные из ``-wal``).
    """
    src = sqlite3.connect(str(source_db))
    try:
        dst = sqlite3.connect(str(target_db))
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def _collect_run_entries(run_dir: Path, db_snapshot: Path | None, skip: Path) -> dict[str, bytes]:
    """Собирает члены бандла как ``{posix-относительный путь: bytes}``.

    ``cohound.db`` подменяется консистентным снапшотом; прочие файлы читаются
    как есть. ``skip`` — путь самого архива (если он внутри ``run_dir``),
    чтобы не включить бандл в себя.
    """
    entries: dict[str, bytes] = {}
    if db_snapshot is not None and db_snapshot.exists():
        entries[_DB_NAME] = db_snapshot.read_bytes()
    for path in sorted(run_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.resolve() == skip.resolve():
            continue
        rel = PurePosixPath(path.relative_to(run_dir).as_posix())
        if _is_excluded(rel):
            continue
        posix = rel.as_posix()
        if posix == _DB_NAME:
            # Сырую БД не берём: вместо неё — консистентный снапшот выше.
            continue
        entries[posix] = path.read_bytes()
    return entries


def backup_run(run_dir: str | Path, dest: str | Path | None = None) -> Path:
    """Создаёт воспроизводимый бандл прогона ``runs/{run_id}/``.

    Содержимое архива (члены — относительно корня прогона):

    - ``run.yaml`` и ``run_manifest.yaml`` — иммутабельные вход/манифест (§A.2);
    - ``cohound.db`` — консистентный снапшот через ``sqlite3.Connection.backup``;
    - ``logs/``, ``prompts/``, ``outputs/`` и прочие файлы прогона рекурсивно;
    - **не** включаются секреты (``.env*``, ``*.pem``, ``*.key``, …) и sidecar-файлы
      SQLite (``cohound.db-wal``/``-shm``) — они уже учтены снапшотом.

    :param run_dir: каталог прогона.
    :param dest: файл архива; если ``None`` — соседний ``{run_id}.zip``; если
        существующий каталог — ``{run_id}.zip`` внутри него.
    :returns: путь созданного ``.zip``.
    :raises FileNotFoundError: если ``run_dir`` не является каталогом.
    """
    run_path = Path(run_dir)
    if not run_path.is_dir():
        raise FileNotFoundError(f"каталог прогона не найден: {run_path}")

    if dest is None:
        archive = run_path.parent / f"{run_path.name}.zip"
    else:
        dest_path = Path(dest)
        if dest_path.is_dir():
            archive = dest_path / f"{run_path.name}.zip"
        else:
            archive = dest_path
    archive.parent.mkdir(parents=True, exist_ok=True)

    source_db = run_path / _DB_NAME
    with tempfile.TemporaryDirectory(prefix="cohound-backup-") as tmp:
        snapshot = Path(tmp) / _DB_NAME
        if source_db.exists():
            _snapshot_db(source_db, snapshot)
            db_snapshot: Path | None = snapshot
        else:
            db_snapshot = None
        entries = _collect_run_entries(run_path, db_snapshot, archive)

    # Детерминированный порядок членов + фиксированная метка времени.
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name in sorted(entries):
            info = zipfile.ZipInfo(name, date_time=_FIXED_ZIP_DATE)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zf.writestr(info, entries[name])

    return archive


def _validate_member(name: str) -> PurePosixPath:
    """Проверяет член архива на path traversal и возвращает относительный путь.

    Отвергаются абсолютные пути, ``..``-компоненты, Windows-drive/UNC и
    обратные слеши (трактуются как разделители).

    :raises ValueError: при небезопасном имени члена.
    """
    normalized = name.replace("\\", "/")
    candidate = PurePosixPath(normalized)
    if candidate.is_absolute() or normalized.startswith("/"):
        raise ValueError(f"небезопасный член архива (абсолютный путь): {name!r}")
    parts = candidate.parts
    if any(part == ".." for part in parts):
        raise ValueError(f"небезопасный член архива (path traversal): {name!r}")
    if parts and len(parts[0]) >= 2 and parts[0][1] == ":":
        raise ValueError(f"небезопасный член архива (drive/UNC): {name!r}")
    return candidate


def restore_run(archive: str | Path, dest_dir: str | Path) -> Path:
    """Распаковывает бандл прогона в ``dest_dir`` и возвращает восстановленный каталог.

    Каждый член архива проверяется на path traversal (:func:`_validate_member`):
    ``..``, абсолютные пути и drive/UNC-префиксы отклоняются исключением.

    :raises ValueError: если архив содержит небезопасный член.
    """
    archive_path = Path(archive)
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    base = dest.resolve()

    with zipfile.ZipFile(archive_path, "r") as zf:
        for info in zf.infolist():
            rel = _validate_member(info.filename)
            if info.is_dir() or rel.name == "":
                continue
            target = dest.joinpath(*rel.parts)
            resolved = target.resolve()
            if not (resolved == base or base in resolved.parents):
                raise ValueError(f"небезопасный член архива (вне назначения): {info.filename!r}")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(zf.read(info))

    return dest


def list_builds(index_root: str | Path, package: str) -> list[tuple[str, int, float]]:
    """Перечисляет индексные сборки ``index_store/{package}/{build}/{product_id}/``.

    :returns: список ``(build_id, n_partitions, mtime)``, отсортированный по
        ``build_id`` (лексикографически, детерминированно). ``n_partitions`` —
        число продуктов-партиций внутри сборки; ``mtime`` — время модификации
        каталога сборки (для выбора новейших в GC). Если пакет отсутствует —
        пустой список.
    """
    package_dir = Path(index_root) / package
    if not package_dir.is_dir():
        return []
    builds: list[tuple[str, int, float]] = []
    for build_dir in package_dir.iterdir():
        if not build_dir.is_dir():
            continue
        n_partitions = sum(1 for child in build_dir.iterdir() if child.is_dir())
        builds.append((build_dir.name, n_partitions, build_dir.stat().st_mtime))
    builds.sort(key=lambda item: item[0])
    return builds


def gc_index_builds(
    index_root: str | Path,
    package: str,
    *,
    keep: int = 5,
    protect: Iterable[str] = (),
    dry_run: bool = True,
) -> list[str]:
    """Собирает мусор среди индексных сборок, сохраняя новейшие и защищённые.

    Политика (§13): удаляются все сборки, кроме ``keep`` новейших (по
    ``(mtime, build_id)``) и явно защищённых ``protect`` (например, на которые
    ссылаются свежие ``run_manifest``). Иммутабельные сборки не трогаются
    произвольно — удаление требует явного ``dry_run=False``.

    :param keep: сколько новейших сборок сохранять; обязан быть ``>= 1``.
    :param protect: build_id, защищённые от удаления (дополнительно к ``keep``).
    :param dry_run: ``True`` (по умолчанию, безопасно) — только вернуть список
        кандидатов, ничего не удаляя; ``False`` — реально удалить.
    :returns: отсортированный список удалённых (или подлежащих удалению) build_id.
    :raises ValueError: если ``keep < 1``.
    """
    if keep < 1:
        raise ValueError("keep обязан быть >= 1")

    builds = list_builds(index_root, package)
    protected = set(protect)
    newest = sorted(builds, key=lambda item: (item[2], item[0]), reverse=True)
    survivors = protected | {item[0] for item in newest[:keep]}
    to_delete = sorted(item[0] for item in builds if item[0] not in survivors)

    if dry_run:
        return to_delete

    package_dir = Path(index_root) / package
    deleted: list[str] = []
    for build_id in to_delete:
        target = package_dir / build_id
        if target.is_dir():
            shutil.rmtree(target)
        deleted.append(build_id)
    return deleted
