#!/usr/bin/env python3
"""Прибор: сколько эталона **достижимо** при текущем каталоге и методологии.

Вопрос замера. ``golden.csv`` размечен как истина о продуктах («этого у продукта нет»), а
``agreement`` считает совпадением только точное равенство статусов
(:func:`core.pipeline.golden._classify`). После пере-квалификации каталога 18.11.2026
(``explicit`` 181 → 0) отсутствие доказуемо только исчерпанием объявленного пространства
(класс ``bounded``, I2b) с датированным снятием каждого корня (I2d) — либо реестром.
Значит для части эталонных ``❌`` методология **не позволяет** вердикт при любом качестве
понимания: лучший возможный исход там — ``❓``. Прибор отвечает, сколько таких ячеек и
каков из-за них потолок согласия.

Семантика достижимости живёт в библиотеке — :mod:`core.pipeline.absence_reach` (её же
читает ``hound golden``), чтобы прибор и команда не расходились. Здесь остаётся только
то, чего в библиотеке нет: чтение артефактов прогона по путям, страта ``❌`` по маркеру
отрицания (это прибор, а не отчёт) и печать.

Использование:

    python scripts/measure_golden_ceiling.py \\
        --catalog packages/ru-pg-dbms/catalog.csv \\
        --golden packages/ru-pg-dbms/golden.csv \\
        --run-db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-145ad66ca3f9/cohound.db \\
        --allow-foreign-schema --json

    # носители текущими правилами (после смены формулы):
    python scripts/measure_golden_ceiling.py --catalog … --golden … \\
        --run-db …/cohound.db --allow-foreign-schema \\
        --state-db …/state.db --run-id ru-pg-dbms-… --json

``--allow-foreign-schema`` обязателен, если ``schema_version`` БД прогона не равна текущей:
вердикты читаются как исторический факт, а не пересчитываются (AGENTS §1.2: артефакт
старше документа, но и он относится к своим условиям).

**Два способа получить носители — и они отвечают на разные вопросы.**

* без ``--state-db``: читается **записанное** покрытие прогона (``corpus_coverage``) — то,
  что прогон решил своей версией правил на свою дату;
* с ``--state-db … --run-id …``: носители **пересчитываются текущими правилами** по
  первичным артефактам — снятиям ``capture`` из ``state.db`` и счётчикам фронтьера из
  ``crawl_sessions.provenance_json``. Это единственный способ сравнить правила: записанный
  ``complete`` отвечает на вопрос прежней формулы, и рост на нём был бы эффектом правки
  прибора, а не методологии.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.config.run_config import load_catalog, load_golden  # noqa: E402
from core.domain.enums import Status  # noqa: E402
from core.domain.models import (  # noqa: E402
    CorpusRootCoverage,
    GoldenRow,
)
from core.pipeline.absence_reach import (  # noqa: E402
    CellVerdict,
    ProductCarrier,
    Reachability,
    agreement_with_ceiling,
    classify_reachability,
)
from core.pipeline.digests import catalog_digest  # noqa: E402
from core.store.sqlite import SCHEMA_VERSION  # noqa: E402

__all__ = [
    "absent_stratum",
    "main",
    "read_carriers",
    "read_carriers_from_state",
    "read_cells",
]


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


def read_carriers_from_state(
    conn: sqlite3.Connection,
    state_db: Path,
    run_id: str,
) -> dict[str, ProductCarrier]:
    """Носители ``bounded``, **пересчитанные текущими правилами** по снятиям ``state.db``.

    Зачем отдельный путь. ``corpus_coverage.roots_json`` — это то, что прогон решил
    **своей** версией правил на свою дату. После смены правил (0.6.0: ``robots`` —
    граница сайта; 0.7.0: пропуск политики — переиспользование, и ``complete`` требует
    наблюдённости корня) записанный ``complete`` отвечает на старый вопрос, и сравнивать
    с ним новое правило нельзя — иначе «стало лучше» было бы эффектом правки прибора.

    Поэтому здесь читаются **первичные** артефакты: снятия ``capture`` из ``state.db``
    (у них свой ``complete``, ``policy_digest``, ``observed_how``, дата) и объявленные
    корни из БД прогона, а решение принимает **доменная** :func:`derive_corpus_coverage`
    — та же функция, что пишет покрытие в живом прогоне. Копии правила в приборе нет.

    ``declared_space_digest`` берётся из снятий **самого прогона**: ``_record_root_capture``
    и ``_persist_corpus_coverage`` получают его из одного и того же резолва политики,
    поэтому внутри прогона они равны **по построению**, а не по догадке. Если снятия
    прогона дают разные отпечатки (резолв менялся внутри прогона — это дефект), прибор
    отказывает: усреднять такое значило бы выдумать объявленное пространство.

    ``state.db`` читается **сырым** ``sqlite3``: файл стенда может быть старой версии
    схемы (``SchemaVersionMismatch`` у ``connect_state``), а читать исторический артефакт
    надо уметь. Колонки ``space_json`` в файле ≤ 13 нет — тогда материал отсутствует, и
    сравнение идёт по отпечатку, как и было до схемы 14.
    """
    from core.pipeline.corpus_coverage import derive_corpus_coverage
    from core.ports.state import CaptureRow

    declared_roots: dict[str, list[str]] = {}
    for row in conn.execute("select product_id, roots_json from corpus_coverage"):
        declared_roots[row["product_id"]] = [
            str(item.get("root_url"))
            for item in json.loads(row["roots_json"] or "[]")
            if item.get("root_url")
        ]

    state = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    state.row_factory = sqlite3.Row
    try:
        columns = {row[1] for row in state.execute("PRAGMA table_info(capture)")}
        select = [
            "capture_id",
            "product_id",
            "root_url",
            "started_at",
            "finished_at",
            "complete",
            "observed_how",
            "run_id",
            "policy_digest",
            "actor",
            "superseded_by",
        ]
        if "space_json" in columns:
            select.append("space_json")
        captures: list[CaptureRow] = []
        for row in state.execute(
            f"select {', '.join(select)} from capture"  # noqa: S608 - имена из списка выше
        ):
            captures.append(
                CaptureRow(
                    capture_id=row["capture_id"],
                    product_id=row["product_id"],
                    root_url=row["root_url"],
                    started_at=row["started_at"],
                    finished_at=row["finished_at"],
                    complete=bool(row["complete"]),
                    observed_how=row["observed_how"],
                    run_id=row["run_id"],
                    policy_digest=row["policy_digest"],
                    space_json=row["space_json"] if "space_json" in columns else None,
                    actor=row["actor"],
                    superseded_by=row["superseded_by"],
                )
            )
    finally:
        state.close()

    # ``complete`` в файле отвечает на вопрос **своей** формулы на свою дату: под 0.5.0
    # отказ ``robots`` делал корень неисчерпанным, под 0.7.0 ``complete`` требует ещё и
    # наблюдённости. Поэтому флаг пересчитывается тем же предикатом, что в живом прогоне
    # (:func:`core.pipeline.corpus._root_capture_complete`), по первичным счётчикам
    # фронтьера из ``crawl_sessions.provenance_json``. Копии формулы здесь нет.
    #
    # ``observed`` берётся как ``pages_fetched > 0``: список отказов ``robots`` в
    # провенанс не пишется, и корень, у которого ноль страниц **только** из-за отказов,
    # прочтётся как ненаблюдённый. Направление ошибки консервативное — прибор
    # недосчитывает носители, а не выдумывает их.
    from core.pipeline.corpus import _root_capture_complete

    provenance: dict[tuple[str, str], dict[str, Any]] = {}
    for row in conn.execute("select provenance_json from crawl_sessions"):
        payload = json.loads(row["provenance_json"] or "{}")
        for root in (payload.get("effective_limits") or {}).get("roots") or []:
            provenance[(str(root.get("product_id")), str(root.get("root")))] = root
    restored: list[CaptureRow] = []
    for capture in captures:
        root = provenance.get((capture.product_id, capture.root_url))
        if root is None:
            restored.append(capture)
            continue
        incomplete = bool(root.get("truncated") or root.get("skipped_documents"))
        observed = int(root.get("pages_fetched") or 0) > 0
        restored.append(
            dataclasses.replace(
                capture,
                complete=_root_capture_complete(
                    incomplete=incomplete, observed=observed
                ),
            )
        )
    captures = restored

    own = {
        capture.policy_digest
        for capture in captures
        if capture.run_id == run_id and capture.policy_digest
    }
    if len(own) > 1:
        raise SystemExit(
            f"снятия прогона {run_id} несут разные отпечатки пространства: "
            f"{sorted(own)} — объявленное пространство не определено"
        )
    declared_digest = next(iter(own)) if own else None

    out: dict[str, ProductCarrier] = {}
    for product_id, roots in declared_roots.items():
        coverage = derive_corpus_coverage(
            product_id=product_id,
            items=(),
            truncated=False,
            computed_at="1970-01-01T00:00:00+00:00",
            declared_roots=roots,
            captures=[
                capture for capture in captures if capture.product_id == product_id
            ],
            declared_space_digest=declared_digest,
        )
        out[product_id] = ProductCarrier(
            complete=coverage.complete,
            roots=tuple(coverage.roots),
            skipped_by_policy=(coverage.reason or "") == "crawl_policy_skipped",
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
    parser.add_argument(
        "--state-db",
        type=Path,
        help=(
            "state.db пакета: пересчитать носители текущими правилами по снятиям "
            "(требует --run-id), а не читать записанное покрытие"
        ),
    )
    parser.add_argument(
        "--run-id",
        help="прогон, чьи снятия объявляют пространство (вместе с --state-db)",
    )
    parser.add_argument("--json", action="store_true", help="машинный вывод")
    args = parser.parse_args(argv)

    catalog_rows = load_catalog(args.catalog)
    catalog = {row.param_id: row for row in catalog_rows}
    golden = load_golden(args.golden)
    conn = _open_run_db(args.run_db, args.allow_foreign_schema)
    try:
        if (args.state_db is None) != (args.run_id is None):
            raise SystemExit("--state-db и --run-id задаются вместе")
        carriers = (
            read_carriers(conn)
            if args.state_db is None
            else read_carriers_from_state(conn, args.state_db, args.run_id)
        )
        cells = read_cells(conn)
        run = conn.execute("select catalog_version from runs limit 1").fetchone()
        run_catalog_version = run["catalog_version"] if run is not None else None
    finally:
        conn.close()

    from core.pipeline.evaluate import _DENIAL_MARKER_RE

    reach = classify_reachability(golden, catalog, carriers)
    agree = agreement_with_ceiling(golden, cells, reach, catalog)
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
