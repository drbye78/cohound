#!/usr/bin/env python3
"""Гейт типизации: ноль ошибок в домене + зафиксированный baseline для остального ядра.

Две разные вещи, и они не должны сливаться в одну:

* **Гейт.** ``core/domain`` — чистый домен без I/O, ошибок там ноль, и любая новая ломает
  сборку. Это условие, а не пожелание.
* **Долг с памятью.** Остальной ``core/`` даёт 127 ошибок на 212 файлах, и он **не**
  переводится в ноль росчерком пера: 1581 упоминание ``Any`` и 1047 вызовов ``getattr``
  заражают типы, поэтому «127» — нижняя граница, а ``--strict`` даст существенно больше.
  Запись «гейт выключен» превратила бы типизацию в фикцию, а ``# type: ignore`` везде —
  хуже её отсутствия. Поэтому долг **зафиксирован числом** (``docs/architecture/
  mypy-baseline.txt``): он виден, он не растёт, и его уменьшение тоже видно.

Сравнение идёт по мультимножеству ключей ``path: сообщение [код]`` **без номеров строк**:
номера сдвигаются от любой правки выше по файлу, и baseline, привязанный к строкам,
пришлось бы перегенерировать по несколько раз в день — то есть перестать читать.

Запуск:
  python3 scripts/mypy_gate.py             # проверка (гейт + baseline)
  python3 scripts/mypy_gate.py --update    # перегенерировать baseline (осознанно)
"""

from __future__ import annotations

import argparse
import collections
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
BASELINE = ROOT / "docs" / "architecture" / "mypy-baseline.txt"

#: Жёсткий гейт: здесь ошибок быть не может.
GATE_PATHS = ("core/domain/",)

#: ``путь:строка: severity: сообщение`` — формат mypy. Итоговая строка («Found 127 …»)
#: не совпадает и отсекается сама.
_DIAG = re.compile(r"^(?P<path>[^:\n]+):(?P<line>\d+): (?P<sev>error|warning|note): (?P<msg>.+)$")

_SEV_ERROR = "error"

#: Признак того, что mypy **прекратил** проверку (токенайзер/парсер упал на файле).
#: В этом состоянии «0 новых ошибок» означало бы «проверено не всё», то есть зелёный
#: статус за непроверенный код — самое опасное из возможных исходов гейта.
_ABORTED = "errors prevented further checking"


#: Номера строк **внутри** сообщения mypy (``already defined on line 7714``): их надо
#: выбросить так же, как номер строки-адреса. Иначе ключ остаётся привязанным к строке, и
#: правка выше по файлу даёт «новую» ошибку на месте старой: гейт кричит о росте долга,
#: которого нет, а ``--update`` становится рутиной — то есть перестаёт быть осознанным.
#: Ровно это и произошло при добавлении комментариев в ``corpus.py``: 4 записи
#: «переехали», долг не изменился.
_MSG_LINE = re.compile(r"\bline \d+\b")

def _normalise_msg(msg: str) -> str:
    """Сообщение без номеров строк: ``line 7714`` → ``line <N>``."""
    return _MSG_LINE.sub("line <N>", msg)

def _run_mypy() -> tuple[collections.Counter[str], str]:
    """Запустить mypy по ``core/`` и вернуть мультимножество ошибок и сырой вывод."""
    proc = subprocess.run(
        [sys.executable, "-m", "mypy", "core/"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    out = proc.stdout + proc.stderr
    keys: collections.Counter[str] = collections.Counter()
    for line in out.splitlines():
        match = _DIAG.match(line)
        if match is None or match.group("sev") != _SEV_ERROR:
            continue
        # Номер строки выброшен намеренно: ключ должен переживать правки выше по файлу.
        # ``msg`` уже содержит ``[код]`` — это то, что делает запись осмысленной.
        keys[f"{match.group('path')}: {_normalise_msg(match.group('msg'))}"] += 1
    if not keys and proc.returncode not in (0, 1):
        # mypy упал не из-за ошибок типов (конфиг/импорт/интерпретатор): «ноль ошибок»
        # здесь означал бы «проверка не выполнялась», и молчать об этом нельзя.
        sys.stderr.write(out)
        raise SystemExit("mypy: проверка не выполнилась — см. вывод выше")
    if _ABORTED in out:
        # Проверка оборвалась: часть файлов не анализировалась вовсе. Считать это
        # «baseline сходится» нельзя — именно так гейт и пропускает новый дефект.
        sys.stderr.write(out)
        raise SystemExit(
            "mypy: проверка оборвана (синтаксическая ошибка в файле) — "
            "baseline неприменим, пока файл не парсится"
        )
    return keys, out


def _read_baseline() -> collections.Counter[str]:
    if not BASELINE.is_file():
        raise SystemExit(
            f"baseline не найден: {BASELINE.relative_to(ROOT)}\n"
            "Сгенерируйте осознанно: python3 scripts/mypy_gate.py --update"
        )
    keys: collections.Counter[str] = collections.Counter()
    for line in BASELINE.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        keys[line] += 1
    return keys


def _write_baseline(keys: collections.Counter[str]) -> None:
    header = [
        "# Baseline ошибок mypy для core/ (кроме core/domain — там гейт «ноль»).",
        "#",
        "# Это НЕ разрешение на рост: это зафиксированный долг. Строка в файле означает,",
        "# что ошибка известна и учтена; новая ошибка (которой здесь нет) блокирует сборку.",
        "# Ключ — `путь: сообщение [код]` БЕЗ номера строки: baseline должен переживать",
        "# правки выше по файлу, иначе его перегенерируют не читая.",
        "#",
        "# Перегенерация — только осознанно и с объяснением в сообщении коммита:",
        "#   python3 scripts/mypy_gate.py --update",
        f"# Записей: {sum(keys.values())}",
        "",
    ]
    body = []
    for key in sorted(keys):
        body.extend([key] * keys[key])
    BASELINE.parent.mkdir(parents=True, exist_ok=True)
    BASELINE.write_text("\n".join(header + body) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Гейт mypy: домен + baseline ядра.")
    parser.add_argument(
        "--update",
        action="store_true",
        help="перегенерировать baseline по текущему состоянию (осознанное действие)",
    )
    args = parser.parse_args(argv)

    current, raw = _run_mypy()

    if args.update:
        _write_baseline(current)
        print(f"mypy-baseline обновлён: {sum(current.values())} записей → {BASELINE.name}")
        return 0

    baseline = _read_baseline()
    gate = collections.Counter(
        {key: n for key, n in current.items() if key.startswith(GATE_PATHS)}
    )
    total = sum(current.values())
    todo = sum(baseline.values())

    print(f"mypy: {total} ошибок в core/, baseline {todo}")

    if gate:
        print("\nГЕЙТ (core/domain, ожидается ноль):", file=sys.stderr)
        for key, n in sorted(gate.items()):
            print(f"  ×{n} {key}", file=sys.stderr)
    if any(gate):
        print(
            "\nДомен обязан быть типизирован: core/domain — чистый слой без I/O,\n"
            "и его ошибки не «долг», а неверные типы в инвариантах.",
            file=sys.stderr,
        )

    new = current - baseline
    gone = baseline - current
    failed = bool(gate) or bool(new)

    if new:
        print(f"\nНОВЫЕ ошибки ({sum(new.values())}) — baseline не покрывает:", file=sys.stderr)
        for key, n in sorted(new.items()):
            print(f"  ×{n} {key}", file=sys.stderr)
        print(
            "\nРост типизационного долга блокирует сборку. Исправьте ошибку; если она\n"
            "неисправима сейчас — это отдельное решение, а не молчаливое `--update`.",
            file=sys.stderr,
        )
    if gone:
        # Уменьшение долга — не провал: иначе починка ошибки ломала бы сборку и люди
        # перестали бы чинить. Но и тишина тут неверна: baseline обязан отражать правду.
        print(
            f"\nbaseline устарел: {sum(gone.values())} записей больше не воспроизводятся "
            f"(долг уменьшился). Перегенерируйте: python3 scripts/mypy_gate.py --update"
        )

    if failed:
        return 1

    print(f"mypy: домен чист, новых ошибок вне baseline нет ({sum(gone.values())} устаревших).")
    if total and not raw.strip():
        print("предупреждение: mypy не дал вывода, но ошибки посчитаны", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
