#!/usr/bin/env python3
"""Гейт: эталон согласован с объявленным каталогом (пере-аннотация под 0.7.0).

Вопрос гейта. ``golden.csv`` — производный артефакт: он собран из снимка сравнительной
таблицы (``sources/golden_00_cells.csv``, снимок 2026-10-01) скриптом
``sources/build_from_golden_00.py``. Снимок таблицы отвечает на вопрос «что вендор написал
о продукте», но **не** на вопрос «чем это доказывается по нашему правилу»: класс
доказательства отсутствия объявлен в каталоге (``absence_rule``), а не в эталоне. Пока
каталог был целиком ``explicit``, различие было незаметно — ожидаемое свидетельство у всех
905 строк стояло ``documentation``. После пере-квалификации 18.11.2026 (``explicit`` 0,
``distribution_absence`` 158, ``parameter_reference`` 19, ``registry_absence`` 4) часть
эталонных ``❌`` доказывается **реестром**, а не исчерпанием документации, и строка эталона
обязана это называть.

Что проверяется (по каждому — адрес нарушения):

``unknown_param``
    ``param_id`` строки отсутствует в ``catalog.csv``: эталон ссылается на параметр,
    которого больше нет (переименование/снятие — ``deprecated`` + ``replaced_by``).
``status_vs_class``
    ``case_class`` не соответствует ``expected_status`` (``class_absent`` ↔ ``❌``,
    ``class_ok`` ↔ ``✅``, ``class_partial`` ↔ ``⚠️``, ``metadata`` ↔ ``✅``).
``kind_vs_rule``
    ``expected_evidence_kind`` не соответствует объявленному правилу: при
    ``absence_rule=registry_absence`` эталонный ``❌`` доказывается реестром
    (``registry``), при ``distribution_absence``/``parameter_reference`` — исчерпанием
    объявленного пространства документации (``documentation``).
``duplicate``
    две строки на одну пару ``(product_id, param_id)``.
``raw_vs_status``
    ``expected_status`` расходится с ``normalized_status`` снимка таблицы: эталон правили
    руками, и снимок перестал быть его источником (это не запрещено, но обязано быть
    видно — иначе пересборка из снимка молча вернёт прежнее значение).

Гейт **не** решает, верна ли сама разметка («этого у продукта нет»): это утверждение о
продукте, прибор его не проверяет (см. `26-open-review-items-plan.md` §O44). Он проверяет
только согласованность производного артефакта с объявленными входами.

Использование:

    python scripts/check_golden_consistency.py --package packages/ru-pg-dbms
    python scripts/check_golden_consistency.py --package packages/ru-pg-dbms --write
    python scripts/check_golden_consistency.py --package packages/ru-pg-dbms --json
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

__all__ = ["Finding", "Report", "check", "main"]

#: ``case_class`` → единственный допустимый ``expected_status``.
CLASS_STATUS = {
    "class_ok": "✅",
    "class_partial": "⚠️",
    "class_absent": "❌",
    "metadata": "✅",
}

#: Правило отсутствия → ожидаемый вид свидетельства у эталонного ``❌``.
#:
#: ``registry_absence`` — реестр объявлен исчерпывающим перечислением, носитель-снятие не
#: нужен (I2b). Остальные два правила доказываются исчерпанием объявленного пространства
#: документации (``bounded``, I2d) — там ожидаемое свидетельство остаётся документацией.
RULE_KIND = {
    "registry_absence": "registry",
    "distribution_absence": "documentation",
    "parameter_reference": "documentation",
}


@dataclass
class Finding:
    """Одно расхождение: где, что ожидалось, что записано."""

    kind: str
    product_id: str
    param_id: str
    expected: str
    actual: str

    def line(self) -> str:
        return (
            f"{self.kind}: {self.product_id}/{self.param_id} "
            f"ожидалось {self.expected!r}, записано {self.actual!r}"
        )


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)
    rows: int = 0
    absent_rows: int = 0
    #: Сколько строк ``--write`` исправит (только ``kind_vs_rule`` — единственное
    #: расхождение, где верное значение выводится из объявленного входа без решения человека).
    fixable: int = 0

    def add(self, finding: Finding) -> None:
        self.findings.append(finding)

    def by_kind(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for finding in self.findings:
            counts[finding.kind] = counts.get(finding.kind, 0) + 1
        return dict(sorted(counts.items()))

    def as_dict(self) -> dict[str, object]:
        return {
            "rows": self.rows,
            "absent_rows": self.absent_rows,
            "fixable": self.fixable,
            "by_kind": self.by_kind(),
            "findings": [
                {
                    "kind": f.kind,
                    "product_id": f.product_id,
                    "param_id": f.param_id,
                    "expected": f.expected,
                    "actual": f.actual,
                }
                for f in self.findings
            ],
        }


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _kinds(value: str) -> list[str]:
    return [item.strip() for item in value.split("|") if item.strip()]


def check(package: Path) -> Report:
    """Сверяет эталон с каталогом и со снимком таблицы. Ничего не пишет."""
    catalog = {row["param_id"]: row for row in _read_csv(package / "catalog.csv")}
    golden_path = package / "golden.csv"
    golden = _read_csv(golden_path)
    cells_path = package / "sources" / "golden_00_cells.csv"
    raw_status: dict[tuple[str, str], str] = {}
    if cells_path.exists():
        for cell in _read_csv(cells_path):
            raw_status[(cell["product_id"], cell["param_id"])] = cell["normalized_status"]

    report = Report(rows=len(golden))
    seen: dict[tuple[str, str], int] = {}
    for row in golden:
        product_id = row["product_id"]
        param_id = row["param_id"]
        status = row["expected_status"]
        case_class = row["case_class"]
        key = (product_id, param_id)
        seen[key] = seen.get(key, 0) + 1
        if seen[key] > 1:
            report.add(Finding("duplicate", product_id, param_id, "1 строка", str(seen[key])))
        catalog_row = catalog.get(param_id)
        if catalog_row is None:
            report.add(Finding("unknown_param", product_id, param_id, "есть в catalog.csv", "нет"))
            continue
        # ``case_class`` в домене — свободная строка, а не перечисление: словарь классов
        # объявлен пакетом, и у `_template` он свой (``absence_registry``). Проверяется
        # только тот набор, который объявил этот пакет; чужой класс — не расхождение.
        if case_class in CLASS_STATUS and CLASS_STATUS[case_class] != status:
            report.add(
                Finding(
                    "status_vs_class",
                    product_id,
                    param_id,
                    f"{case_class} → {CLASS_STATUS[case_class]}",
                    status,
                )
            )
        if status == "❌":
            report.absent_rows += 1
            rule = catalog_row["absence_rule"]
            want = RULE_KIND.get(rule)
            have = _kinds(row["expected_evidence_kind"])
            if want is not None and want not in have:
                report.fixable += 1
                report.add(
                    Finding("kind_vs_rule", product_id, param_id, f"{rule} → {want}", "|".join(have))
                )
        snapshot = raw_status.get(key)
        if snapshot is not None and snapshot != status:
            report.add(Finding("raw_vs_status", product_id, param_id, snapshot, status))
    return report


def write(package: Path) -> int:
    """Правит ``expected_evidence_kind`` у ``❌``-строк под объявленное правило.

    Правится **только** то, что выводится из объявленного входа: вид свидетельства у
    эталонного ``❌``. ``expected_status`` не трогается — это утверждение о продукте.
    Остальные колонки и порядок строк сохраняются байт в байт.
    """
    catalog = {row["param_id"]: row for row in _read_csv(package / "catalog.csv")}
    path = package / "golden.csv"
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    changed = 0
    for row in rows:
        if row["expected_status"] != "❌":
            continue
        catalog_row = catalog.get(row["param_id"])
        if catalog_row is None:
            continue
        want = RULE_KIND.get(catalog_row["absence_rule"])
        have = _kinds(row["expected_evidence_kind"])
        if want is None or want in have:
            continue
        row["expected_evidence_kind"] = "|".join([want, *have])
        changed += 1
    if changed:
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
    return changed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True, type=Path)
    parser.add_argument("--write", action="store_true", help="исправить выводимое из правила")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    if args.write:
        changed = write(args.package)
        print(f"исправлено строк: {changed}")

    report = check(args.package)
    if args.json:
        print(json.dumps(report.as_dict(), ensure_ascii=False, indent=1))
    else:
        print(
            f"строк эталона: {report.rows}, из них ❌: {report.absent_rows}; "
            f"расхождений: {len(report.findings)}"
        )
        for kind, count in report.by_kind().items():
            print(f"  {kind}: {count}")
        for finding in report.findings[:20]:
            print(f"  {finding.line()}")
        if len(report.findings) > 20:
            print(f"  … ещё {len(report.findings) - 20}")
    return 0 if not report.findings else 1


if __name__ == "__main__":
    raise SystemExit(main())
