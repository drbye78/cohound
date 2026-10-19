"""coHound — атомарная запись файлов с сохранением режима доступа.

Единственная реализация «temp в том же каталоге + ``os.replace``» для авторских
артефактов пакета (``package_io``) и seed-набора (``config.seeds``). Слой ``util``
не зависит от ``pipeline``/``config``, поэтому оба слоя могут её переиспользовать.

**Режим доступа.** ``tempfile.mkstemp`` создаёт файл с ``0o600``; при
``os.replace`` этот режим переезжает на целевой файл. Для артефактов пакета это
регрессия: сгенерированные ``run.yaml``/``cohort.yaml``/``domain_profiles.yaml``
становились нечитаемыми для группы (ломается shared/DMZ-развёртывание, где пакет
читает другой пользователь). Поэтому:

* существующий целевой файл — режим **сохраняется** (авторский выбор не перетирается);
* новый файл — :data:`DEFAULT_FILE_MODE` (``0o644``), как у остальных файлов пакета.

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

__all__ = ["DEFAULT_FILE_MODE", "atomic_write_text"]

#: Режим нового файла (rw-r--r--): артефакты пакета — читаемые исходники, не секреты.
DEFAULT_FILE_MODE = 0o644


def _target_mode(target: Path) -> int:
    """Режим, который получит целевой файл: существующий либо дефолт."""
    try:
        return target.stat().st_mode & 0o777
    except OSError:
        return DEFAULT_FILE_MODE


def atomic_write_text(path: str | Path, text: str) -> Path:
    """Атомарно пишет ``text`` в ``path``, сохраняя режим существующего файла.

    При любой ошибке временный файл удаляется, а целевой не меняется. Родительский
    каталог создаётся при необходимости.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    mode = _target_mode(target)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        # mkstemp даёт 0o600; без явного chmod режим переехал бы на целевой файл.
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, target)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return target
