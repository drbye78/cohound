#!/usr/bin/env python3
"""Замер пика памяти при засеве лексического индекса (A1/Д-3; документ ``10`` §6 удалён
08.11.2026, дефект закрыт).

Зачем: критерий приёмки A1 в документе сформулирован как «пик памяти индекса не зависит
от числа чанков». Замер показывает, что верно **для объекта индекса** (зеркала больше
нет) и что **неверно** для процесса сборки: пик задаёт writer tantivy, у которого
собственный буфер. Без этого числа «R2 выполнен» звучало бы как утверждение о процессе.

Метод: отдельный процесс на точку (иначе RSS предыдущей точки наследуется), текст
фиксированной длины 1 KB на чанк, пик ``ru_maxrss`` процесса. Сравниваются два способа
передачи записей — генератор (новый путь) и список (Python-зеркало целиком), плюс
явные настройки writer'а.

Запуск: ``.venv/bin/python docs/reports/measurements/2026-10-28-lexical-seed-memory/probe.py``
Кладёт ``results.jsonl`` рядом с собой — сырьё публикуется вместе с выводами.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]


def main() -> int:
    """Точки замера: три размера генератором, два — списком (прежнее Python-зеркало)."""
    points = [
        (60_000, "generator"),
        (120_000, "generator"),
        (240_000, "generator"),
        (480_000, "generator"),
        (60_000, "list"),
        (120_000, "list"),
        (240_000, "list"),
    ]
    out = HERE / "results.jsonl"
    with out.open("w", encoding="utf-8") as sink:
        for n, mode in points:
            cmd = [sys.executable, str(HERE / "_child.py"), str(n), mode]
            # S603: команда собрана из литералов и sys.executable — внешнего ввода здесь нет.
            proc = subprocess.run(  # noqa: S603
                cmd, capture_output=True, text=True, cwd=REPO, check=False
            )
            if proc.returncode != 0:
                print(f"FAIL n={n} mode={mode}: {proc.stderr[-400:]}", file=sys.stderr)
                return 1
            row = json.loads(proc.stdout.strip().splitlines()[-1])
            sink.write(json.dumps(row, ensure_ascii=False) + "\n")
            sink.flush()
            print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
