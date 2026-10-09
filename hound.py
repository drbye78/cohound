#!/usr/bin/env python3
"""coHound CLI — исполняемая обёртка для упрощённого запуска из корня проекта.

Запуск::

    ./hound.py --help
    ./hound.py plan --run-yaml packages/ru-pg-dbms/run.yaml --package packages/ru-pg-dbms
    python hound.py run --run-yaml ... --package ... --until report

Обёртка делает две вещи:

1. **Переисполнение в проектной venv.** Если рядом есть ``.venv/bin/python`` и текущий
   интерпретатор — не он, скрипт прозрачно перезапускает себя этим Python
   (``os.execv``), чтобы гарантированно были доступны зависимости проекта. Если venv
   нет — работает текущим интерпретатором.
2. **Корень проекта в ``sys.path``.** Чтобы ``python hound.py`` работал из любого
   рабочего каталога (пакет ``core`` лежит плоско в корне, ``03`` §3.3).

Скрипт не содержит логики CLI: он лишь тонко делегирует в :func:`core.cli.main.main`.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    """Корень проекта — каталог, в котором лежит этот скрипт."""
    return Path(__file__).resolve().parent


#: Env-флаг: отключить автоперезапуск в проектной venv (``COHOUND_NO_REEXEC=1``).
ENV_NO_REEXEC = "COHOUND_NO_REEXEC"
#: Env-флаг-предохранитель от зацикливания при перезапуске.
ENV_REEXEC_GUARD = "COHOUND_REEXEC"


def _in_project_venv(root: Path) -> bool:
    """Работает ли текущий интерпретатор внутри проектной venv ``root/.venv``.

    Надёжный признак — ``sys.prefix`` (каталог venv), а **не** ``sys.executable``:
    при запуске через pyenv-шим ``sys.executable`` указывает на базовый/шимный
    интерпретатор, тогда как ``sys.prefix`` отражает активную venv. Сравнение
    ``sys.executable`` давало ложное «уже в venv» и лишало проект зависимостей.
    """
    try:
        return Path(sys.prefix).resolve() == (root / ".venv").resolve()
    except OSError:  # pragma: no cover - экзотические платформы
        return False


def _reexec_in_project_venv(root: Path) -> None:
    """Перезапускает скрипт проектным ``.venv/bin/python``, если это возможно.

    Прозрачность: если venv нет, отключено флагом ``COHOUND_NO_REEXEC`` или текущий
    интерпретатор уже из него — ничего не делает. Защита от зацикливания — сравнение
    ``sys.prefix`` (плюс env-предохранитель). На Windows (``bin/python`` отсутствует)
    остаётся текущий интерпретатор.
    """
    if os.environ.get(ENV_NO_REEXEC):
        return
    if os.environ.get(ENV_REEXEC_GUARD):  # уже перезапускались — второй раз не надо
        return
    venv_python = root / ".venv" / "bin" / "python"
    if not venv_python.exists():
        return
    if _in_project_venv(root):
        return
    os.environ[ENV_REEXEC_GUARD] = "1"
    os.execv(str(venv_python), [str(venv_python), str(Path(__file__).resolve()), *sys.argv[1:]])


def main_wrapper() -> int:
    """Готовит окружение и вызывает CLI. Возвращает код выхода."""
    root = _repo_root()
    _reexec_in_project_venv(root)
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from core.cli.main import main

    return main()


if __name__ == "__main__":  # pragma: no cover - точка входа скрипта
    raise SystemExit(main_wrapper())
