#!/usr/bin/env python3
"""Прибор: парное сравнение двух прогонов оценщика на одном эталоне (A/B промпта).

Вопрос замера. Согласие с эталоном двигается и от ``❓``, и от разметки, поэтому как
индикатор понимания смысла оно слепое (замер 22.11.2026,
``docs/reports/measurements/2026-11-22-evaluator-confusion.md``). Прибор считает метрику,
которая не зависит от полноты ретривала: **доля присутствия при наличии цитаты** —
среди ячеек, где хотя бы одна запись ``cell_evidence`` несёт непустой ``citation_quote``,
какая доля получила ``✅``/``⚠️``. Если ретривал не принёс ничего, ячейка в знаменатель
не входит: это цена поиска, а не чтения.

Что сравнивается. Два **готовых** прогона одного пакета (тот же эталон, те же продукты),
различающиеся объявленным входом (``prompt_template``, ``lexical_spec``, …). Прибор ничего
не запускает и не пересчитывает: он читает записанные вердикты и цитаты
(``mode=ro``, AGENTS §1.2 — артефакт старше документа). Расхождение объявленных входов
печатается из манифеста, чтобы вывод не приписывал эффект одному изменению, когда их два.

Что публикуется (A3: не порог, а интервал):

* по каждому срезу — состав статусов в обоих плечах, ``присутствие`` (``✅+⚠️``),
  ``присутствие|цитата``;
* **парное** сравнение по ячейкам: дискордантные пары ``v6→v7`` (присутствие появилось /
  пропало), точный двусторонний критерий МакНемара (биномиальный, без аппроксимации);
* бутстрэп-CI (перцентильный) на разность долей присутствия и на ``присутствие|цитата``;
* срезы объявляются заранее в предрегистрации и передаются флагами, а не выбираются
  после результата.

Использование:

    python scripts/measure_evaluator_ab.py \\
        --runs-dir ~/.cohound/stands/ru-pg-dbms/runs \\
        --run-a ru-pg-dbms-b9b53eb63a53 --run-b ru-pg-dbms-902c0b71c419 \\
        --golden packages/ru-pg-dbms/golden.csv \\
        --catalog packages/ru-pg-dbms/catalog.csv \\
        --slice class_absent --exclude-hyphen-params \\
        --bootstrap 10000 --seed 20261123 --json
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import random
import re
import sqlite3
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.config.run_config import load_catalog, load_golden  # noqa: E402
from core.domain.enums import Status  # noqa: E402
from core.pipeline.prompt import render_queries  # noqa: E402

__all__ = [
    "ArmCell",
    "bootstrap_ci",
    "hyphen_params",
    "mcnemar_exact",
    "read_arm",
    "slice_report",
]

#: Присутствие — те же два статуса, что в матрице ошибок: ``❌`` присутствием не является,
#: ``❓`` — тоже (это «не знаю», а не «нет»).
PRESENCE = frozenset({Status.OK, Status.PARTIAL})

#: То же регулярное выражение, что в :func:`core.adapters.lexical_tantivy._expand_hyphen_compounds`:
#: срез «дефисные параметры» обязан быть тем же множеством, что меняет выдача, иначе
#: сравнение срезов мерило бы не то изменение. Импорт адаптера тянет tantivy, поэтому
#: выражение повторено здесь буквально; расхождение ловит
#: ``tests/scripts/test_measure_evaluator_ab.py`` (сверка с адаптером).
_HYPHEN_COMPOUND = re.compile(
    r"(?<![\w-])([A-Za-zА-Яа-яЁё0-9_]+)-([A-Za-zА-Яа-яЁё0-9_]+)(?![\w-])"
)


@dataclass(frozen=True)
class ArmCell:
    """Вердикт одной ячейки одного плеча — то, что нужно метрике и ничего больше."""

    state: str
    status: Status | None
    has_quote: bool

    @property
    def present(self) -> bool:
        return self.status in PRESENCE


def read_arm(db_path: Path) -> dict[tuple[str, str], ArmCell]:
    """Читает вердикты и наличие цитаты из БД прогона (только чтение)."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        quoted: set[tuple[str, str]] = set()
        for row in conn.execute(
            "select distinct product_id, param_id from cell_evidence "
            "where citation_quote is not null and trim(citation_quote) <> ''"
        ):
            quoted.add((row["product_id"], row["param_id"]))
        out: dict[tuple[str, str], ArmCell] = {}
        for row in conn.execute(
            "select product_id, param_id, state, status from cells"
        ):
            key = (row["product_id"], row["param_id"])
            status = Status(row["status"]) if row["status"] else None
            out[key] = ArmCell(
                state=row["state"], status=status, has_quote=key in quoted
            )
        return out
    finally:
        conn.close()


def read_prompts(run_dir: Path) -> dict[tuple[str, str], tuple[str, ...]]:
    """``cell_key`` → упорядоченный список ``chunk_id``, которые модель видела в ячейке.

    Архив промптов — единственный артефакт, который говорит, **что** получил оценщик, не
    завися от того, что модель процитировала: у воздержавшейся ячейки ``cell_evidence``
    пуст, и по нему «ретривал ничего не нашёл» неотличимо от «модель промолчала».
    """
    out: dict[tuple[str, str], tuple[str, ...]] = {}
    for path in sorted((run_dir / "prompts").glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        key = payload["cell_key"]
        cell = (
            tuple(key)
            if isinstance(key, (list, tuple))
            else tuple(ast.literal_eval(key))
        )
        out[(cell[0], cell[1])] = tuple(payload["chunk_ids"])
    return out


def chunk_text_hashes(db_path: Path, chunk_ids: set[str]) -> dict[str, str]:
    """``chunk_id`` → ``sha256`` текста чанка, только для запрошенных адресов.

    Сравнивать надо **текст**, а не ``chunk_id``: после пересборки документа адрес
    (``doc_version_id``) меняется, а текст может остаться побайтово тем же. Обратное тоже
    верно: адрес может совпасть, а текст — нет, если сборка переиспользовала адрес с
    другим содержимым (``content_hash`` обязан это ловить).
    """
    out: dict[str, str] = {}
    ids = sorted(chunk_ids)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        for start in range(0, len(ids), 500):
            batch = ids[start : start + 500]
            marks = ",".join("?" * len(batch))
            for chunk_id, text in conn.execute(
                f"select chunk_id, text from corpus_chunks where chunk_id in ({marks})",  # noqa: S608
                batch,
            ):
                out[chunk_id] = hashlib.sha256((text or "").encode("utf-8")).hexdigest()
        return out
    finally:
        conn.close()


def same_material(
    prompts_a: dict[tuple[str, str], tuple[str, ...]],
    prompts_b: dict[tuple[str, str], tuple[str, ...]],
    hashes_a: dict[str, str],
    hashes_b: dict[str, str],
) -> dict[tuple[str, str], bool]:
    """Для каждой ячейки: увидела ли модель в обоих плечах **тот же** текст фрагментов.

    Мультимножество хэшей текста, а не список: порядок фрагментов — свойство сборки
    промпта, а не материала. Ячейка без промпта в одном из плеч считается несовпавшей.
    """
    out: dict[tuple[str, str], bool] = {}
    for key, ids_a in prompts_a.items():
        ids_b = prompts_b.get(key)
        if ids_b is None:
            out[key] = False
            continue
        set_a = sorted(hashes_a.get(i, f"missing:{i}") for i in ids_a)
        set_b = sorted(hashes_b.get(i, f"missing:{i}") for i in ids_b)
        out[key] = set_a == set_b
    return out


def corpus_diff(db_a: Path, db_b: Path) -> list[dict[str, Any]]:
    """Документы и чанки по продуктам в двух плечах — цена «того же корпуса» в числах."""

    def _counts(path: Path) -> dict[str, tuple[int, int]]:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            docs = dict(
                conn.execute("select product_id, count(*) from corpus_docs group by 1")
            )
            chunks = dict(
                conn.execute(
                    "select product_id, count(*) from corpus_chunks group by 1"
                )
            )
            return {
                p: (docs.get(p, 0), chunks.get(p, 0)) for p in set(docs) | set(chunks)
            }
        finally:
            conn.close()

    left, right = _counts(db_a), _counts(db_b)
    rows = []
    for product in sorted(set(left) | set(right)):
        docs_a, chunks_a = left.get(product, (0, 0))
        docs_b, chunks_b = right.get(product, (0, 0))
        rows.append(
            {
                "product_id": product,
                "docs_a": docs_a,
                "docs_b": docs_b,
                "chunks_a": chunks_a,
                "chunks_b": chunks_b,
            }
        )
    return rows


def hyphen_params(
    catalog_path: Path, *, max_queries: int = 3, query_lang: str = "all"
) -> set[str]:
    """Параметры, в запросах которых (``render_queries``) есть дефисное составное слово.

    Множество определяется тем же составом запросов, что исполняет Ш6: ``max_queries`` и
    ``query_lang`` — те же, что в провенансе ячейки. Дефис в неисполненном кандидате
    (отрезанном бюджетом запросов) в срез не входит: выдача на нём не менялась.
    """
    out: set[str] = set()
    for param in load_catalog(catalog_path):
        queries = render_queries(param, max_queries=max_queries, query_lang=query_lang)
        if any(_HYPHEN_COMPOUND.search(query) for query in queries):
            out.add(param.param_id)
    return out


def mcnemar_exact(b: int, c: int) -> float:
    """Точный двусторонний критерий МакНемара: биномиальный тест ``p=0.5`` над ``b+c``.

    Аппроксимация ``χ²`` при малых дискордантных парах врёт (и это именно тот случай:
    сдвиг промпта ожидается в единицах ячеек), поэтому считается точная сумма хвостов.
    """
    n = b + c
    if n == 0:
        return 1.0
    k = min(b, c)
    tail = sum(math.comb(n, i) for i in range(0, k + 1)) / (2**n)
    return min(1.0, 2.0 * tail)


def bootstrap_ci(
    values: list[float], *, n: int, seed: int, alpha: float = 0.05
) -> tuple[float, float]:
    """Перцентильный бутстрэп-CI среднего по выборке ячеек (парный, по ячейкам)."""
    if not values:
        return (math.nan, math.nan)
    rng = random.Random(seed)  # noqa: S311 — воспроизводимость важнее криптостойкости
    size = len(values)
    means: list[float] = []
    for _ in range(n):
        total = 0.0
        for _ in range(size):
            total += values[rng.randrange(size)]
        means.append(total / size)
    means.sort()
    lo = means[math.floor((alpha / 2) * (n - 1))]
    hi = means[math.ceil((1 - alpha / 2) * (n - 1))]
    return (lo, hi)


def _share(num: int, den: int) -> float:
    return num / den if den else math.nan


def slice_report(
    keys: Iterable[tuple[str, str]],
    arm_a: dict[tuple[str, str], ArmCell],
    arm_b: dict[tuple[str, str], ArmCell],
    *,
    bootstrap: int,
    seed: int,
) -> dict[str, Any]:
    """Метрики одного среза: состав, присутствие, присутствие|цитата, дискордансы, CI."""
    keys = list(keys)
    n = len(keys)

    def _counts(arm: dict[tuple[str, str], ArmCell]) -> dict[str, int]:
        out: dict[str, int] = {s.value: 0 for s in Status}
        out["failed"] = 0
        out["pending"] = 0
        for key in keys:
            cell = arm.get(key)
            if cell is None:
                out["missing"] = out.get("missing", 0) + 1
                continue
            if cell.status is None:
                out[cell.state] = out.get(cell.state, 0) + 1
            else:
                out[cell.status.value] += 1
        return out

    counts_a, counts_b = _counts(arm_a), _counts(arm_b)

    def _present(arm: dict[tuple[str, str], ArmCell]) -> int:
        return sum(1 for k in keys if (c := arm.get(k)) is not None and c.present)

    def _quoted(arm: dict[tuple[str, str], ArmCell]) -> int:
        return sum(1 for k in keys if (c := arm.get(k)) is not None and c.has_quote)

    def _present_quoted(arm: dict[tuple[str, str], ArmCell]) -> int:
        return sum(
            1
            for k in keys
            if (c := arm.get(k)) is not None and c.has_quote and c.present
        )

    present_a, present_b = _present(arm_a), _present(arm_b)
    quoted_a, quoted_b = _quoted(arm_a), _quoted(arm_b)
    pq_a, pq_b = _present_quoted(arm_a), _present_quoted(arm_b)

    # Парные величины: разность присутствия по ячейкам и «присутствие|цитата» только там,
    # где цитата есть в обоих плечах (иначе знаменатели разные и парность фиктивна).
    diff_presence = [
        (1.0 if (arm_b.get(k) and arm_b[k].present) else 0.0)
        - (1.0 if (arm_a.get(k) and arm_a[k].present) else 0.0)
        for k in keys
    ]
    both_quoted = [
        k
        for k in keys
        if (a := arm_a.get(k)) is not None
        and (b := arm_b.get(k)) is not None
        and a.has_quote
        and b.has_quote
    ]
    diff_pq = [
        (1.0 if arm_b[k].present else 0.0) - (1.0 if arm_a[k].present else 0.0)
        for k in both_quoted
    ]
    # Дискордансы по присутствию: b = появилось в v7, c = пропало в v7.
    gained = sum(
        1
        for k in keys
        if arm_b.get(k) and arm_b[k].present and not (arm_a.get(k) and arm_a[k].present)
    )
    lost = sum(
        1
        for k in keys
        if arm_a.get(k) and arm_a[k].present and not (arm_b.get(k) and arm_b[k].present)
    )

    lo, hi = bootstrap_ci(diff_presence, n=bootstrap, seed=seed)
    lo_q, hi_q = bootstrap_ci(diff_pq, n=bootstrap, seed=seed)
    return {
        "cells": n,
        "statuses_a": counts_a,
        "statuses_b": counts_b,
        "present_a": present_a,
        "present_b": present_b,
        "present_share_a": _share(present_a, n),
        "present_share_b": _share(present_b, n),
        "quoted_a": quoted_a,
        "quoted_b": quoted_b,
        "present_quoted_a": pq_a,
        "present_quoted_b": pq_b,
        "present_quoted_share_a": _share(pq_a, quoted_a),
        "present_quoted_share_b": _share(pq_b, quoted_b),
        "gained": gained,
        "lost": lost,
        "discordant": gained + lost,
        "mcnemar_p": mcnemar_exact(gained, lost),
        "diff_present": _share(present_b, n) - _share(present_a, n),
        "diff_present_ci": [lo, hi],
        "paired_quoted_cells": len(both_quoted),
        "diff_present_quoted": _share(pq_b, quoted_b) - _share(pq_a, quoted_a),
        "diff_present_quoted_ci": [lo_q, hi_q],
    }


def _fmt(value: float) -> str:
    return "n/a" if value != value else f"{value:.4f}"


def _print_slice(name: str, stats: dict[str, Any]) -> None:
    sa, sb = stats["statuses_a"], stats["statuses_b"]
    print(f"\n== {name} (ячеек {stats['cells']})")
    print(
        f"  статусы v6: ✅{sa['✅']} ⚠️{sa['⚠️']} ❌{sa['❌']} ❓{sa['❓']} "
        f"failed={sa.get('failed', 0)} pending={sa.get('pending', 0)}"
    )
    print(
        f"  статусы v7: ✅{sb['✅']} ⚠️{sb['⚠️']} ❌{sb['❌']} ❓{sb['❓']} "
        f"failed={sb.get('failed', 0)} pending={sb.get('pending', 0)}"
    )
    print(
        f"  присутствие:      v6 {stats['present_a']}/{stats['cells']} = "
        f"{_fmt(stats['present_share_a'])}  →  v7 {stats['present_b']}/{stats['cells']} = "
        f"{_fmt(stats['present_share_b'])}  Δ {_fmt(stats['diff_present'])} "
        f"CI [{_fmt(stats['diff_present_ci'][0])}, {_fmt(stats['diff_present_ci'][1])}]"
    )
    print(
        f"  присутствие|цитата: v6 {stats['present_quoted_a']}/{stats['quoted_a']} = "
        f"{_fmt(stats['present_quoted_share_a'])}  →  v7 "
        f"{stats['present_quoted_b']}/{stats['quoted_b']} = "
        f"{_fmt(stats['present_quoted_share_b'])}  Δ "
        f"{_fmt(stats['diff_present_quoted'])} "
        f"CI [{_fmt(stats['diff_present_quoted_ci'][0])}, "
        f"{_fmt(stats['diff_present_quoted_ci'][1])}]"
    )
    print(
        f"  дискордансы: появилось {stats['gained']}, пропало {stats['lost']} "
        f"(пар {stats['discordant']}), McNemar p={_fmt(stats['mcnemar_p'])}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Парное сравнение двух прогонов оценщика на одном эталоне."
    )
    parser.add_argument("--runs-dir", required=True, type=Path)
    parser.add_argument("--run-a", required=True, help="базовое плечо (v6)")
    parser.add_argument("--run-b", required=True, help="сравниваемое плечо (v7)")
    parser.add_argument("--golden", required=True, type=Path)
    parser.add_argument("--catalog", required=True, type=Path)
    parser.add_argument("--query-lang", default="all")
    parser.add_argument("--max-queries", type=int, default=3)
    parser.add_argument(
        "--slice",
        action="append",
        default=[],
        help="класс эталона (class_ok|class_partial|class_absent|metadata); "
        "повторяемый; по умолчанию — все ячейки эталона",
    )
    parser.add_argument(
        "--exclude-hyphen-params",
        action="store_true",
        help="выбросить параметры с дефисным словом в запросе (чистый эффект промпта)",
    )
    parser.add_argument(
        "--only-hyphen-params",
        action="store_true",
        help="оставить только такие параметры (оба изменения сразу)",
    )
    parser.add_argument(
        "--same-material",
        action="store_true",
        help="оставить только ячейки, где модель увидела тот же текст фрагментов в обоих "
        "плечах (сверка по архиву промптов и тексту чанков, а не по адресам)",
    )
    parser.add_argument(
        "--corpus-diff",
        action="store_true",
        help="напечатать документы/чанки по продуктам в обоих плечах",
    )
    parser.add_argument(
        "--by-product", action="store_true", help="разбить срез по продуктам"
    )
    parser.add_argument("--bootstrap", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=20261123)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    db_a = args.runs_dir / args.run_a / "cohound.db"
    db_b = args.runs_dir / args.run_b / "cohound.db"
    arm_a = read_arm(db_a)
    arm_b = read_arm(db_b)
    golden = load_golden(args.golden)

    hyphen = hyphen_params(
        args.catalog, max_queries=args.max_queries, query_lang=args.query_lang
    )
    params = {row.param_id for row in golden}
    if args.exclude_hyphen_params and args.only_hyphen_params:
        parser.error("--exclude-hyphen-params и --only-hyphen-params взаимоисключающи")
    if args.exclude_hyphen_params:
        hyphen_slice = params - hyphen
    elif args.only_hyphen_params:
        hyphen_slice = params & hyphen
    else:
        hyphen_slice = params

    classes = set(args.slice)
    keys = [
        (row.product_id, row.param_id)
        for row in golden
        if row.param_id in hyphen_slice and (not classes or row.case_class in classes)
    ]

    material: dict[tuple[str, str], bool] = {}
    if args.same_material:
        prompts_a = read_prompts(args.runs_dir / args.run_a)
        prompts_b = read_prompts(args.runs_dir / args.run_b)
        ids_a = {cid for ids in prompts_a.values() for cid in ids}
        ids_b = {cid for ids in prompts_b.values() for cid in ids}
        material = same_material(
            prompts_a,
            prompts_b,
            chunk_text_hashes(db_a, ids_a),
            chunk_text_hashes(db_b, ids_b),
        )
        kept = [k for k in keys if material.get(k)]
        dropped = len(keys) - len(kept)
        keys = kept
        if not args.json:
            print(
                f"тот же материал: {len(kept)} ячеек среза, отброшено {dropped} "
                f"(корпус в плечах различается)"
            )

    stats = slice_report(keys, arm_a, arm_b, bootstrap=args.bootstrap, seed=args.seed)
    if args.json:
        payload: dict[str, Any] = {
            "run_a": args.run_a,
            "run_b": args.run_b,
            "slice": sorted(classes) or ["all"],
            "hyphen_params": len(hyphen),
            "hyphen_filter": (
                "exclude"
                if args.exclude_hyphen_params
                else "only"
                if args.only_hyphen_params
                else "none"
            ),
            "same_material": args.same_material,
            "stats": stats,
        }
        if args.same_material:
            payload["material_identical_cells"] = sum(1 for v in material.values() if v)
            payload["material_differs_cells"] = sum(
                1 for v in material.values() if not v
            )
        if args.corpus_diff:
            payload["corpus"] = corpus_diff(db_a, db_b)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print(f"A/B: {args.run_a} (v6) → {args.run_b} (v7)")
    print(
        f"срез: классы {sorted(classes) or 'все'}; дефисные параметры "
        f"{len(hyphen)} из {len(params)} — "
        f"{'исключены' if args.exclude_hyphen_params else 'только они' if args.only_hyphen_params else 'включены'}"
    )
    _print_slice("срез", stats)

    if args.by_product:
        by_product: dict[str, list[tuple[str, str]]] = {}
        for key in keys:
            by_product.setdefault(key[0], []).append(key)
        print("\n== по продуктам")
        for product in sorted(by_product):
            sub = slice_report(
                by_product[product],
                arm_a,
                arm_b,
                bootstrap=args.bootstrap,
                seed=args.seed,
            )
            print(
                f"  {product:20} ячеек {sub['cells']:>3}  "
                f"присутствие {_fmt(sub['present_share_a'])} → {_fmt(sub['present_share_b'])}  "
                f"|цитата {_fmt(sub['present_quoted_share_a'])} → "
                f"{_fmt(sub['present_quoted_share_b'])}  "
                f"цитат {sub['quoted_a']} → {sub['quoted_b']}"
            )

    if args.corpus_diff:
        print("\n== корпус в плечах (документы / чанки)")
        print(f"  {'продукт':20}{'док v6':>8}{'док v7':>8}{'чанк v6':>9}{'чанк v7':>9}")
        for row in corpus_diff(db_a, db_b):
            print(
                f"  {row['product_id']:20}{row['docs_a']:>8}{row['docs_b']:>8}"
                f"{row['chunks_a']:>9}{row['chunks_b']:>9}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
