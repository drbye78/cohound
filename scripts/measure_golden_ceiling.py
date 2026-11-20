#!/usr/bin/env python3
"""Прибор: сколько эталона **достижимо** при текущем каталоге и методологии.

Вопрос замера. `golden.csv` размечен как истина о продуктах («этого у продукта нет»), а
`agreement` считает совпадением только точное равенство статусов
(:func:`core.pipeline.golden._classify`). После пере-квалификации каталога 18.11.2026
(``explicit`` 177 → 0, `b56f184`) отсутствие доказуемо только исчерпанием объявленного
пространства (класс ``bounded``, I2b) с датированным снятием каждого корня (I2d) — либо
реестром. Значит для части эталонных ``❌`` методология **не позволяет** вердикт при любом
качестве понимания: лучший возможный исход там — ``❓``. Этот прибор отвечает, сколько
таких ячеек и каков из-за них потолок согласия.

Что он **не** делает. Не пересчитывает вердикты и не правит эталон: он читает
опубликованные ячейки прогона как **факт артефакта** («что этот прогон сказал»), эталон —
как объявленную истину, каталог — как объявление правила. Три источника не смешиваются.

Достижимость ``❌`` считается **доменной функцией**, а не копией правила:
:func:`core.domain.invariants.check_absence_dated` — то же, чем проверяет I2d в конвейере.
Копия разошлась бы с правилом при первой правке, и прибор показывал бы потолок для
методологии, которой нет.

Три исхода на ячейку эталона:

* ``reachable`` — ``❌`` достижим: правило ``registry_absence`` (носитель не нужен) либо
  все объявленные корни продукта удостоверены снятием;
* ``unreachable`` — ``❌`` недостижим: носителя нет (причина называется отдельно —
  ``policy_skipped``, когда корпус пришёл по объявленной политике без обхода, или
  ``carrier_missing``, когда обход был, но не исчерпал корни);
* ``unknown`` — правило ``explicit``: доказательство зависит от цитаты конкретной ячейки,
  до вердикта невычислимо. Такие ячейки из потолка **исключаются** и называются числом,
  а не молча приравниваются к достижимым.

Использование:

    python scripts/measure_golden_ceiling.py \\
        --catalog packages/ru-pg-dbms/catalog.csv \\
        --golden packages/ru-pg-dbms/golden.csv \\
        --run-db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-145ad66ca3f9/cohound.db \\
        --allow-foreign-schema --json

``--allow-foreign-schema`` обязателен, если ``schema_version`` БД прогона не равна текущей:
вердикты читаются как исторический факт, а не пересчитываются (AGENTS §1.2: артефакт
старше документа, но и он относится к своим условиям).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.config.run_config import load_catalog, load_golden  # noqa: E402
from core.domain.enums import AbsenceRuleType, DenialStrength, Status  # noqa: E402
from core.domain.invariants import check_absence_dated  # noqa: E402
from core.domain.models import CatalogRow, CorpusRootCoverage, GoldenRow  # noqa: E402
from core.pipeline.digests import catalog_digest  # noqa: E402
from core.store.sqlite import SCHEMA_VERSION  # noqa: E402

#: Правила, чей ``❌`` держится на исчерпании объявленного пространства (I2b).
BOUNDED_RULES = frozenset(
    {AbsenceRuleType.PARAMETER_REFERENCE, AbsenceRuleType.DISTRIBUTION_ABSENCE}
)

REACHABLE = "reachable"
UNREACHABLE = "unreachable"
UNKNOWN = "unknown"

#: Причина недостижимости, когда корпус пришёл по объявленной политике без обхода.
POLICY_SKIPPED = "policy_skipped"
#: Причина недостижимости, когда обход был, но корни не исчерпаны.
CARRIER_MISSING = "carrier_missing"


@dataclass
class ProductCarrier:
    """Носитель ``bounded`` у продукта: снятия объявленных корней.

    :param complete: ``corpus_coverage.complete`` как его записал прогон (сводка).
    :param roots: снятия корней — то, чем на самом деле проверяет I2d.
    :param skipped_by_policy: корпус получен по объявленной политике без обхода
        (``reason='crawl_policy_skipped'``) — «не смотрели по решению», а не «смотрели и
        не нашли».
    """

    complete: bool
    roots: tuple[CorpusRootCoverage, ...]
    skipped_by_policy: bool

    def bounded_ok(self) -> bool:
        """Удостоверяет ли носитель ``bounded``-отсутствие — **правилом I2d**."""
        ok, _, _ = check_absence_dated(
            denial_strength=DenialStrength.BOUNDED,
            cited_observations=[],
            roots=self.roots,
        )
        return ok


@dataclass
class CellVerdict:
    """Что прогон сказал об ячейке (факт артефакта, не пересчёт)."""

    status: Status | None
    quotes: tuple[str, ...] = ()


@dataclass
class Reachability:
    """Раскладка ячеек эталона по достижимости ``❌``.

    Счётчики ``absent_*`` считают **только эталонные** ``❌``: у ``✅``/``⚠️`` вопроса о
    носителе нет, и попадание их в те же поля читалось бы как «достижим» без проверки.
    ``cells`` — все строки эталона, потому что потолок согласия считается по всему набору.
    """

    cells: int = 0
    absent_reachable: int = 0
    absent_unknown: int = 0
    absent_unreachable_by_reason: dict[str, int] = field(default_factory=dict)
    #: ``(absence_rule, product_id, param_id)`` недостижимых — причина называется адресно.
    unreachable_cells: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def absent_unreachable(self) -> int:
        return sum(self.absent_unreachable_by_reason.values())

    @property
    def absent_total(self) -> int:
        return self.absent_reachable + self.absent_unknown + self.absent_unreachable

    @property
    def ceiling(self) -> float | None:
        """Потолок согласия: доля ячеек, где вердикт вообще выразим.

        Недостижимый ``❌`` даёт ``❓`` при любом качестве понимания, поэтому он вычитается
        из числителя. ``unknown`` (правило ``explicit``) вычитается и из знаменателя: там
        достижимость решает цитата конкретной ячейки, и приравнять её к нулю значило бы
        объявить недостижимым то, что не измерено (A3).

        ``None``, когда измеримого знаменателя нет: опубликовать ``0.0`` значило бы
        сказать «ничего не достижимо», хотя не измерено ничего.
        """
        denom = self.cells - self.absent_unknown
        if denom <= 0:
            return None
        return (denom - self.absent_unreachable) / denom


def classify_reachability(
    golden: list[GoldenRow],
    catalog: dict[str, CatalogRow],
    carriers: dict[str, ProductCarrier],
) -> Reachability:
    """Раскладывает **эталонные** ``❌`` по достижимости (остальные статусы не считаются).

    Достижимость считается только для ``❌``: у ``✅``/``⚠️`` вопроса о носителе нет, и
    включение их в знаменатель потолка разбавило бы число, ничего не измерив.
    """
    out = Reachability(cells=len(golden))
    for row in golden:
        if row.expected_status is not Status.ABSENT:
            continue
        param = catalog.get(row.param_id)
        rule = param.absence_rule if param is not None else AbsenceRuleType.EXPLICIT
        if rule is AbsenceRuleType.REGISTRY_ABSENCE:
            # I2b: реестр — авторитетная энумерация, снятие и подпись не нужны.
            out.absent_reachable += 1
            continue
        if rule not in BOUNDED_RULES:
            out.absent_unknown += 1
            continue
        carrier = carriers.get(row.product_id)
        if carrier is not None and carrier.bounded_ok():
            out.absent_reachable += 1
            continue
        reason = (
            POLICY_SKIPPED
            if carrier is not None and carrier.skipped_by_policy
            else CARRIER_MISSING
        )
        by_reason = out.absent_unreachable_by_reason
        by_reason[reason] = by_reason.get(reason, 0) + 1
        out.unreachable_cells.append((rule.value, row.product_id, row.param_id))
    return out


def agreement(
    golden: list[GoldenRow],
    cells: dict[tuple[str, str], CellVerdict],
    reach: Reachability,
    catalog: dict[str, CatalogRow],
) -> dict[str, Any]:
    """Согласие эталона с прогоном — как есть и без недостижимых ``❓``.

    Разделение существенно: ``❌`` эталона, недостижимый по носителю, даёт ``❓`` при
    **любом** качестве понимания. Считать это ошибкой системы значит мерить методологию
    качеством продукта. Но и обратное неверно: если прогон на такой ячейке сказал ``✅``/
    ``⚠️``, это не «методология виновата» — он утверждает **наличие** там, где эталон
    говорит об отсутствии, и это остаётся ошибкой (или ошибкой эталона — их различает
    человек, не прибор).
    """
    unreachable = {
        (product, param) for _, product, param in reach.unreachable_cells
    }
    matches = 0
    not_attributable = 0
    claims_presence = 0
    for row in golden:
        key = (row.product_id, row.param_id)
        actual = cells.get(key)
        status = actual.status if actual is not None else None
        if status is row.expected_status:
            matches += 1
            continue
        if key in unreachable and status is Status.UNKNOWN:
            # ❓ там, где ❌ недостижим, — исход методологии, а не промах прибора.
            not_attributable += 1
        elif key in unreachable and status in (Status.OK, Status.PARTIAL):
            # Утверждение наличия там, где эталон говорит об отсутствии: методология тут
            # ни при чём — вердикт не тот, который она запрещает.
            claims_presence += 1
    denom = len(golden) - not_attributable
    return {
        "golden": len(golden),
        "matches": matches,
        "agreement": matches / len(golden) if golden else 0.0,
        "not_attributable": not_attributable,
        "claims_presence_where_golden_absent": claims_presence,
        "attestable_denominator": denom,
        "attestable_agreement": matches / denom if denom else 0.0,
        "ceiling": reach.ceiling,
        # Доля от потолка не определена, когда потолок не измерен (``None``) или равен
        # нулю: делить наблюдённое согласие не на что.
        "share_of_ceiling": (
            (matches / len(golden)) / reach.ceiling if reach.ceiling else None
        ),
    }


def absent_stratum(
    golden: list[GoldenRow],
    cells: dict[tuple[str, str], CellVerdict],
    denial_marker,
) -> dict[str, Any]:
    """Качество страты ``❌`` прогона: переживает ли цитата проверку маркером (0.5.0).

    :param denial_marker: скомпилированное выражение маркера отрицания
        (``core.pipeline.evaluate._DENIAL_MARKER_RE``) — передаётся, чтобы прибор не
        импортировал приватное имя конвейера: правило маркера может переехать, и прибор
        обязан сломаться видимо, а не молча.
    """
    expected = {(row.product_id, row.param_id): row.expected_status for row in golden}
    out: dict[str, Any] = {
        "cells": 0,
        "without_quote": 0,
        "survive": 0,
        "demote": 0,
        "survive_by_golden": {},
        "demote_by_golden": {},
    }
    for key, verdict in cells.items():
        if verdict.status is not Status.ABSENT:
            continue
        out["cells"] += 1
        if not verdict.quotes:
            out["without_quote"] += 1
            continue
        survives = any(denial_marker.search(quote) for quote in verdict.quotes)
        bucket = "survive" if survives else "demote"
        out[bucket] += 1
        golden_status = expected.get(key)
        label = golden_status.value if golden_status is not None else "нет"
        counts = out[f"{bucket}_by_golden"]
        counts[label] = counts.get(label, 0) + 1
    return out


# ---------------------------------------------------------------------------
# Чтение артефактов
# ---------------------------------------------------------------------------


def _open_run_db(path: Path, allow_foreign: bool) -> sqlite3.Connection:
    """Открывает БД прогона только на чтение, отказывая на чужой схеме без флага."""
    if not path.is_file():
        raise SystemExit(f"{path}: нет файла БД прогона")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    version = conn.execute("select value from schema_meta where key='schema_version'").fetchone()
    found = version["value"] if version is not None else "неизвестна"
    if found != SCHEMA_VERSION and not allow_foreign:
        raise SystemExit(
            f"{path}: schema_version {found} ≠ текущей {SCHEMA_VERSION}. Вердикты чужой схемы "
            "читаются только явно: --allow-foreign-schema (исторический факт, не пересчёт)."
        )
    return conn


def read_carriers(conn: sqlite3.Connection) -> dict[str, ProductCarrier]:
    """Носители ``bounded`` по продуктам: снятия корней и объявленная политика."""
    out: dict[str, ProductCarrier] = {}
    for row in conn.execute(
        "select product_id, complete, reason, roots_json from corpus_coverage"
    ):
        roots = tuple(
            CorpusRootCoverage.model_validate(item)
            for item in json.loads(row["roots_json"] or "[]")
        )
        out[row["product_id"]] = ProductCarrier(
            complete=bool(row["complete"]),
            roots=roots,
            skipped_by_policy=(row["reason"] or "") == "crawl_policy_skipped",
        )
    return out


def read_cells(conn: sqlite3.Connection) -> dict[tuple[str, str], CellVerdict]:
    """Опубликованные вердикты прогона и цитаты их evidence (факт артефакта)."""
    quotes: dict[tuple[str, str], list[str]] = {}
    for row in conn.execute(
        "select product_id, param_id, citation_quote from cell_evidence "
        "where citation_quote is not null and citation_quote <> ''"
    ):
        quotes.setdefault((row["product_id"], row["param_id"]), []).append(
            row["citation_quote"]
        )
    out: dict[tuple[str, str], CellVerdict] = {}
    for row in conn.execute("select product_id, param_id, status from cells"):
        key = (row["product_id"], row["param_id"])
        status = Status(row["status"]) if row["status"] else None
        out[key] = CellVerdict(status=status, quotes=tuple(quotes.get(key, ())))
    return out


def _print_report(
    *,
    reach: Reachability,
    agree: dict[str, Any],
    stratum: dict[str, Any],
    carriers: dict[str, ProductCarrier],
    run_catalog_version: str | None,
    declared_catalog_version: str,
) -> None:
    print("== носитель bounded по продуктам (I2d)")
    for product, carrier in sorted(carriers.items()):
        complete_roots = sum(1 for root in carrier.roots if root.complete)
        print(
            f"  {product:20} complete={int(carrier.complete)} "
            f"корней {len(carrier.roots)} (complete={complete_roots}) "
            f"bounded_ok={carrier.bounded_ok()} "
            f"{'policy_skipped' if carrier.skipped_by_policy else ''}"
        )

    print("\n== достижимость ❌ эталона")
    print(f"  ячеек эталона            {reach.cells}")
    print(f"  ❌ в эталоне             {reach.absent_total}")
    print(f"  ❌ достижим              {reach.absent_reachable}")
    print(f"  ❌ недостижим            {reach.absent_unreachable}")
    for reason, count in sorted(reach.absent_unreachable_by_reason.items()):
        print(f"      {reason:16} {count}")
    if reach.absent_unknown:
        print(
            f"  правило explicit (?)     {reach.absent_unknown} — вне потолка, "
            "решает цитата ячейки"
        )
    ceiling = "не определён" if reach.ceiling is None else f"{reach.ceiling:.4f}"
    print(f"  ПОТОЛОК согласия         {ceiling}")

    print("\n== согласие")
    print(f"  совпало                  {agree['matches']} / {agree['golden']} = {agree['agreement']:.4f}")
    share = agree["share_of_ceiling"]
    print(
        "  доля от потолка          "
        + ("не определена" if share is None else f"{share:.4f}")
    )
    print(
        f"  ❓ там, где ❌ недостижим {agree['not_attributable']} — исход методологии, "
        "не промах прибора"
    )
    print(
        f"  ✅/⚠️ там, где эталон ❌   {agree['claims_presence_where_golden_absent']} — "
        "утверждение наличия, ошибка (или ошибка эталона)"
    )
    print(
        f"  аттестуемое согласие     {agree['matches']} / {agree['attestable_denominator']} "
        f"= {agree['attestable_agreement']:.4f}"
    )

    print("\n== страта ❌ прогона: переживает ли цитата маркер отрицания (0.5.0)")
    print(f"  ❌ ячеек                 {stratum['cells']}")
    print(f"  без цитаты               {stratum['without_quote']}")
    print(f"  переживают               {stratum['survive']}  по эталону {stratum['survive_by_golden']}")
    print(f"  понижаются               {stratum['demote']}  по эталону {stratum['demote_by_golden']}")

    if run_catalog_version and run_catalog_version != declared_catalog_version:
        print(
            f"\nВНИМАНИЕ: прогон размечен под каталог {run_catalog_version}, "
            f"а переданный каталог — {declared_catalog_version}.\n"
            "Правила отсутствия могли измениться между ними: достижимость считается по "
            "ПЕРЕДАННОМУ каталогу, а вердикты — по тому, что было. Числа относятся к прогону."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    parser.add_argument("--catalog", type=Path, required=True, help="catalog.csv")
    parser.add_argument("--golden", type=Path, required=True, help="golden.csv")
    parser.add_argument("--run-db", type=Path, required=True, help="cohound.db прогона")
    parser.add_argument(
        "--allow-foreign-schema",
        action="store_true",
        help="читать БД другой schema_version (только чтение, вердикты не пересчитываются)",
    )
    parser.add_argument("--json", action="store_true", help="машинный вывод")
    args = parser.parse_args(argv)

    catalog_rows = load_catalog(args.catalog)
    catalog = {row.param_id: row for row in catalog_rows}
    golden = load_golden(args.golden)
    conn = _open_run_db(args.run_db, args.allow_foreign_schema)
    try:
        carriers = read_carriers(conn)
        cells = read_cells(conn)
        run = conn.execute("select catalog_version from runs limit 1").fetchone()
        run_catalog_version = run["catalog_version"] if run is not None else None
    finally:
        conn.close()

    from core.pipeline.evaluate import _DENIAL_MARKER_RE

    reach = classify_reachability(golden, catalog, carriers)
    agree = agreement(golden, cells, reach, catalog)
    stratum = absent_stratum(golden, cells, _DENIAL_MARKER_RE)
    declared = catalog_digest(catalog_rows)

    if args.json:
        print(
            json.dumps(
                {
                    "catalog_version": declared,
                    "run_catalog_version": run_catalog_version,
                    "carriers": {
                        product: {
                            "complete": carrier.complete,
                            "roots": len(carrier.roots),
                            "bounded_ok": carrier.bounded_ok(),
                            "policy_skipped": carrier.skipped_by_policy,
                        }
                        for product, carrier in sorted(carriers.items())
                    },
                    "reachability": {
                        "cells": reach.cells,
                        "absent_total": reach.absent_total,
                        "absent_reachable": reach.absent_reachable,
                        "absent_unreachable": reach.absent_unreachable,
                        "absent_unknown": reach.absent_unknown,
                        "by_reason": reach.absent_unreachable_by_reason,
                        "ceiling": reach.ceiling,
                    },
                    "agreement": agree,
                    "absent_stratum": stratum,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    _print_report(
        reach=reach,
        agree=agree,
        stratum=stratum,
        carriers=carriers,
        run_catalog_version=run_catalog_version,
        declared_catalog_version=declared,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
