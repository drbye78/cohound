#!/usr/bin/env python3
"""Прибор: что даёт профиль локальных инструментов оценщика — парное сравнение плеч.

Вопрос замера (план 28 §10). Набор инструментов (`--tool-profile`) объявлен условием
измерения и входит в `run_id` 37-м полем, но **эффект** его не следует из объявления:
инструменты адресуют уже собранный корпус, и выигрыш возможен только там, где модель
без них ошибалась именно из-за отсутствия контекста. Прибор сравнивает два плеча —
`none` и включённый профиль — **парно**, по одним и тем же ячейкам.

Почему парно, а не по агрегату. Разница двух агрегатов на срезе смешивает эффект оси с
составом выборки (AGENTS §1.1), а инструменты срабатывают не на всех ячейках: «средний
прирост» мог бы целиком прийти от подмножества, где вызов вообще не состоялся.

Что прибор **проверяет до сравнения** (иначе сравнение не парное):

1. Оба прогона читаются по `(product_id, param_id)` — `run_id` у плеч разный по
   определению (профиль — поле контракта), поэтому сопоставление по идентичности прогона
   невозможно и не нужно.
2. Поля `EXECUTION_CONTRACT_FIELDS`, **кроме** `tool_profile`, обязаны совпасть. Если
   разошлось что-то ещё (модель, каталог, корпус, профиль ретривала), это не эффект
   инструментов, и прибор говорит об этом прямо, а не вычитает одно из другого.

Что публикуется: согласие каждого плеча, доля, дискордантные `better`/`worse`, точный
`mcnemar_p`, bootstrap-интервал дельты и — отдельно — **цена**: сколько вызовов на ячейку
и на чём они провалились (план 28 §10.4: «эффекта нет» и «инструмент не звали» — разные
факты).

Эталон **слеп к классам** (`golden.csv` несёт `case_class` ∈ {class_ok, class_absent,
class_partial, metadata}, но не причину ошибки), поэтому вывод прибора — «профиль изменил
согласие на N из M ячеек, p=…», а не «T1 помогает». Разделение вкладов T1/T4/T7 требует
плеч по одному инструменту и разметки причин — это следующий опыт, а не этот.

Использование:

    python scripts/measure_tool_profile.py \\
        --runs-dir ~/.cohound/stands/ru-pg-dbms/runs \\
        --run-a ru-pg-dbms-<none> --run-b ru-pg-dbms-<tools> \\
        --golden packages/ru-pg-dbms/golden.csv --json

`--allow-foreign-schema` нужен, если `schema_version` плеча не равна текущей: вердикты
читаются как исторический факт (AGENTS §1.2), а не пересчитываются.
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.config.run_config import load_golden  # noqa: E402
from core.domain.enums import Status  # noqa: E402
from core.domain.models import RunManifest  # noqa: E402
from core.pipeline.digests import EXECUTION_CONTRACT_FIELDS  # noqa: E402
from core.pipeline.report import (  # noqa: E402
    _CONTRACT_FIELD_GETTERS,
    _contract_field_attr,
    _contract_field_diagnostics,
)
from core.store.sqlite import SCHEMA_VERSION  # noqa: E402
from core.store.sqlite_store import SqliteStore  # noqa: E402
from core.util.schema_gate import SchemaVersionMismatch  # noqa: E402
from core.util.stats import mcnemar_exact_p  # noqa: E402

#: Поле контракта, которым плечи **обязаны** различаться (иначе это не опыт про инструменты).
TOOL_FIELD = "tool_profile"

#: Сколько bootstrap-выборок по умолчанию. 10 000 даёт устойчивые перцентили на сотнях
#: ячеек; число объявлено, а не «сколько успели» (A3).
DEFAULT_BOOTSTRAP = 10_000


@dataclass(frozen=True)
class Arm:
    """Плечо опыта: прогон, его условия и прочитанные из него ячейки."""

    run_id: str
    path: Path
    #: Манифест прогона — единственный носитель условий контракта. ``None`` — не прочитан
    #: (чужая ``schema_version``): тогда контракт не сверяется, и это называется, а не
    #: выдаётся за «поля совпали». Читать контракт из колонок ``runs`` по именам полей
    #: нельзя: часть слагаемых ``run_id`` живёт под другим именем (``cutoff_K`` →
    #: ``retrieval_cutoff_K``), часть — в провенансе (``serp_rounds``/``serp_mode``), а
    #: ``llm_scenarios`` — в отдельной таблице. Прибор, читающий колонки напрямую,
    #: объявил бы «не восстановимо» половину контракта и не заметил бы реального
    #: расхождения по ней.
    manifest: RunManifest | None = None
    #: Почему манифест не прочитан (текст исключения) — публикуется, не глотается.
    contract_error: str | None = None
    #: ``(product_id, param_id) → Status | None``; ``None`` — ячейка не оценена
    #: (``failed``/``preliminary``), и в парное сравнение она не входит.
    cells: dict[tuple[str, str], Status | None] = field(default_factory=dict)
    #: ``(product_id, param_id) → число вызовов инструментов``; ``None`` — трассы нет.
    calls: dict[tuple[str, str], int] = field(default_factory=dict)
    #: Причины провалов вызовов: ``reason → count`` (``ok=0``).
    call_failures: dict[str, int] = field(default_factory=dict)
    #: Вызовы по имени инструмента: ``tool → count``.
    calls_by_tool: dict[str, int] = field(default_factory=dict)

    @property
    def tool_profile(self) -> str | None:
        """Объявленный профиль инструментов (``None`` — манифест не прочитан/не записан)."""
        return self.manifest.tool_profile if self.manifest is not None else None


def _open(path: Path, allow_foreign: bool) -> sqlite3.Connection:
    """Открывает БД прогона только на чтение, отказывая на чужой схеме без флага."""
    if not path.is_file():
        raise SystemExit(f"{path}: нет файла БД прогона")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "select value from schema_meta where key='schema_version'"
    ).fetchone()
    found = row["value"] if row is not None else "неизвестна"
    if found != SCHEMA_VERSION and not allow_foreign:
        raise SystemExit(
            f"{path}: schema_version {found} ≠ текущей {SCHEMA_VERSION}. Вердикты чужой схемы "
            "читаются только явно: --allow-foreign-schema (исторический факт, не пересчёт)."
        )
    return conn


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "select 1 from sqlite_master where type='table' and name=?", (name,)
    ).fetchone()
    return row is not None


def _read_manifest(run_id: str, path: Path) -> tuple[RunManifest | None, str | None]:
    """Манифест прогона или причина, по которой он не прочитан.

    Читается тем же кодом, что ``hound diff`` (:meth:`SqliteStore.read_manifest`), и это
    принципиально: контракт — не набор колонок ``runs``. ``cutoff_K`` персистится как
    ``retrieval_cutoff_K``, ``serp_rounds``/``serp_mode`` живут в провенансе контура,
    ``llm_scenarios`` — в отдельной таблице. Второй способ чтения разошёлся бы с первым
    молча — ровно так прибор и объявил бы «поля не восстановимы», не заметив расхождения.

    Чужая ``schema_version`` — не ошибка прибора, а состояние плеча: гейт схемы отвергает
    файл до чтения данных (11 §2.6), поэтому манифеста нет и контракт **не сверяется**.
    Это публикуется отдельной строкой: «не сверено» и «совпало» — разные факты.
    """
    try:
        store = SqliteStore(path, read_only=True)
    except SchemaVersionMismatch as exc:
        return None, str(exc)
    try:
        return store.read_manifest(run_id), None
    except ValueError as exc:
        return None, str(exc)
    finally:
        store.close()


def read_arm(run_id: str, path: Path, *, allow_foreign: bool) -> Arm:
    """Читает плечо: манифест, вердикты и трассу вызовов.

    Трасса читается только если таблица есть: у прогона до схемы 3.28.0 её нет, и это
    «трасса не велась», а не «вызовов не было» — различие публикуется.
    """
    conn = _open(path, allow_foreign)
    try:
        cells: dict[tuple[str, str], Status | None] = {}
        for r in conn.execute("select product_id, param_id, status from cells"):
            cells[(r["product_id"], r["param_id"])] = (
                Status(r["status"]) if r["status"] else None
            )
        calls: dict[tuple[str, str], int] = {}
        failures: dict[str, int] = {}
        by_tool: dict[str, int] = {}
        if _has_table(conn, "cell_tool_calls"):
            for r in conn.execute(
                "select product_id, param_id, tool, ok, reason, count(*) as n "
                "from cell_tool_calls group by product_id, param_id, tool, ok, reason"
            ):
                key = (r["product_id"], r["param_id"])
                calls[key] = calls.get(key, 0) + int(r["n"])
                by_tool[r["tool"]] = by_tool.get(r["tool"], 0) + int(r["n"])
                if not r["ok"]:
                    reason = r["reason"] or "без причины"
                    failures[reason] = failures.get(reason, 0) + int(r["n"])
        manifest, contract_error = _read_manifest(run_id, path)
        return Arm(
            run_id=run_id,
            path=path,
            manifest=manifest,
            contract_error=contract_error,
            cells=cells,
            calls=calls,
            call_failures=failures,
            calls_by_tool=by_tool,
        )
    finally:
        conn.close()


def _unrecoverable_fields() -> list[str]:
    """Поля контракта, которых манифест не несёт **ни в каком** прогоне.

    ``hound diff`` называет их «пределом диагностики», и это верно: ``RunManifest`` —
    единственный носитель условий, а ``prompt_template``/``chunking_spec``/
    ``evaluation_policy``/``lexical_spec``/``query_lang``/``embed_spec`` и ещё семь полей
    на нём не объявлены. Прибор обязан назвать их **отдельно**: «не сверено, потому что
    поля нет» и «сверено и совпало» — разные факты, и слитая строка ``missing`` читается
    как второе. Цена этой дыры измеряется здесь же (сколько полей из 37), а не
    замалчивается.

    Разрешение имени учитывается: ``cutoff_K`` живёт на манифесте как
    ``retrieval_cutoff_K``, ``serp_rounds``/``serp_mode`` — в провенансе контура. Они
    восстановимы и в этот список не попадают.
    """
    fields = set(RunManifest.model_fields)
    out: list[str] = []
    for name in EXECUTION_CONTRACT_FIELDS:
        if name in _CONTRACT_FIELD_GETTERS or _contract_field_attr(name) in fields:
            continue
        out.append(name)
    return out


def contract_divergence(
    a: Arm, b: Arm
) -> tuple[list[str], list[str], str | None]:
    """Поля контракта, различающиеся помимо ``tool_profile``, и недоступные.

    Сравнение — по объявленным полям через ту же функцию, что ``hound diff``
    (:func:`core.pipeline.report._contract_field_diagnostics`): дайджест ответил бы
    «разошлось», а здесь нужен ответ **что** разошлось, иначе плечи несравнимы молча.

    Возвращает третьим элементом причину, если контракт не сверялся вовсе: манифеста нет
    у одного из плеч (чужая схема). Тогда ``changed``/``missing`` пусты **не потому**, что
    всё совпало.
    """
    if a.manifest is None or b.manifest is None:
        side = a.run_id if a.manifest is None else b.run_id
        reason = (a.contract_error if a.manifest is None else b.contract_error) or "нет строки runs"
        return [], [], f"контракт не сверен: манифест {side} не прочитан ({reason})"
    changed, missing = _contract_field_diagnostics(a.manifest, b.manifest)
    # ``tool_profile`` — та самая ось, которой плечи обязаны различаться; её расхождение
    # ожидаемо и в список «лишних» не попадает.
    changed = [line for line in changed if f": {TOOL_FIELD}:" not in line]
    return changed, missing, None


def _verdict(arm: Arm, key: tuple[str, str], expected: Status) -> int | None:
    """1 — согласие с эталоном, 0 — расхождение, ``None`` — ячейка не оценена."""
    status = arm.cells.get(key)
    if status is None:
        return None
    return 1 if status == expected else 0


def compare(
    base: Arm,
    variant: Arm,
    expected: dict[tuple[str, str], Status],
    *,
    bootstrap: int = DEFAULT_BOOTSTRAP,
    seed: int = 0,
) -> dict[str, Any]:
    """Парное сравнение по ячейкам, оценённым в **обоих** плечах.

    ``n_pairs == 0`` → все числа ``None``: «сравнивать нечего» — не «разницы нет».
    """
    keys = sorted(
        key
        for key, want in expected.items()
        if key in base.cells and key in variant.cells
        and base.cells[key] is not None and variant.cells[key] is not None
    )
    count = len(keys)
    if count == 0:
        return {
            "n_pairs": 0,
            "agreement_base": None,
            "agreement_variant": None,
            "delta": None,
            "better": None,
            "worse": None,
            "mcnemar_p": None,
            "delta_ci": None,
        }
    left = [_verdict(base, key, expected[key]) for key in keys]
    right = [_verdict(variant, key, expected[key]) for key in keys]
    left_i = [int(v or 0) for v in left]
    right_i = [int(v or 0) for v in right]
    better = sum(1 for x, y in zip(left_i, right_i, strict=True) if y and not x)
    worse = sum(1 for x, y in zip(left_i, right_i, strict=True) if x and not y)
    # Криптостойкость здесь вредна: бутстрэп обязан воспроизводиться по seed (A3).
    generator = random.Random(seed)  # noqa: S311
    deltas: list[float] = []
    for _ in range(max(1, bootstrap)):
        sample = [generator.randrange(count) for _ in range(count)]
        deltas.append(sum(right_i[i] - left_i[i] for i in sample) / count)
    deltas.sort()
    return {
        "n_pairs": count,
        "agreement_base": sum(left_i) / count,
        "agreement_variant": sum(right_i) / count,
        "delta": (sum(right_i) - sum(left_i)) / count,
        "better": better,
        "worse": worse,
        "mcnemar_p": mcnemar_exact_p(better, worse),
        "delta_ci": [
            deltas[int(0.025 * (len(deltas) - 1))],
            deltas[int(0.975 * (len(deltas) - 1))],
        ],
    }


def call_cost(arm: Arm, expected: dict[tuple[str, str], Status]) -> dict[str, Any]:
    """Цена плеча: вызовы на ячейку и причины провалов (план 28 §10.4).

    ``calls_recorded is False`` — таблицы трассы нет: «вызовов не было» и «трасса не
    велась» не сливаются в одно число.
    """
    total = sum(arm.calls.values())
    cells_with_calls = sum(1 for n in arm.calls.values() if n > 0)
    scored = [key for key in expected if arm.cells.get(key) is not None]
    failures = sum(arm.call_failures.values())
    return {
        "calls_recorded": bool(arm.calls) or bool(arm.calls_by_tool),
        "calls_total": total,
        "cells_with_calls": cells_with_calls,
        "calls_per_cell": (total / len(scored)) if scored else None,
        "calls_by_tool": dict(sorted(arm.calls_by_tool.items())),
        "call_failures": dict(sorted(arm.call_failures.items())),
        "failure_share": (failures / total) if total else None,
    }


def verdicts_of_failure(result: dict[str, Any], cost: dict[str, Any]) -> list[str]:
    """Провалы опыта по §10.4 — причина публикуется, а не «эффекта не обнаружено».

    Провал — не «согласие не выросло»: это либо дефект проводки (профиль объявлен, а
    вызовов нет), либо слишком малый бюджет (опыт мерил не инструменты), либо инструмент
    адресует не тот материал.
    """
    reasons: list[str] = []
    if not cost["calls_recorded"]:
        reasons.append(
            "трасса вызовов не записана: либо профиль не включён, либо ячейки взяты из "
            "прогона до схемы 3.28.0 — «вызовов не было» и «трасса не велась» не различить"
        )
    elif cost["calls_total"] == 0:
        reasons.append("профиль объявлен, но ни одного вызова не состоялось (дефект проводки)")
    if cost["calls_per_cell"] is not None and cost["calls_per_cell"] > 0:
        share = cost["failure_share"] or 0.0
        if share > 0.5:
            reasons.append(
                f"доля провалов вызовов {share:.1%} > 50 %: инструмент адресует не тот материал"
            )
    return reasons


def _print_report(
    *, base: Arm, variant: Arm, result: dict[str, Any], cost: dict[str, Any],
    changed: list[str], missing: list[str], reasons: list[str],
    contract_note: str | None = None,
) -> None:
    print(f"плечо A (без инструментов): {base.run_id}  tool_profile={base.tool_profile!r}")
    print(f"плечо B (с инструментами):  {variant.run_id}  tool_profile={variant.tool_profile!r}")
    if contract_note:
        print(f"\n{contract_note}")
        print("  плечи НЕ подтверждены как парные: сверка условий не выполнена")
    if changed:
        print("\nВНИМАНИЕ: контракт разошёлся не только профилем инструментов —")
        for line in changed:
            print(f"  {line}")
        print("  это не парное сравнение эффекта инструментов")
    unrecoverable = _unrecoverable_fields()
    not_recorded = [name for name in missing if name not in unrecoverable]
    if not_recorded:
        print(f"\nполя контракта без значения в этом прогоне: {', '.join(not_recorded)}")
    if unrecoverable:
        print(
            f"\nне сверяется ни для какого прогона: {len(unrecoverable)} из "
            f"{len(EXECUTION_CONTRACT_FIELDS)} полей контракта не несёт манифест — "
            + ", ".join(unrecoverable)
        )
        print("  это не «совпало»: условие измерения по этим полям прибором не проверено")
    print()
    if result["n_pairs"] == 0:
        print("пар нет: нет ячеек, оценённых в обоих плечах — сравнивать нечего")
    else:
        print(f"пар (ячеек, оценённых в обоих плечах): {result['n_pairs']}")
        print(f"  согласие A             {result['agreement_base']:.4f}")
        print(f"  согласие B             {result['agreement_variant']:.4f}")
        print(f"  дельта                 {result['delta']:+.4f}")
        ci = result["delta_ci"]
        print(f"  bootstrap CI (2.5/97.5) [{ci[0]:+.4f}; {ci[1]:+.4f}]")
        print(f"  улучшилось / ухудшилось {result['better']} / {result['worse']}")
        print(f"  McNemar p              {result['mcnemar_p']:.4f}")
    print()
    per_cell = (
        f"{cost['calls_per_cell']:.2f}" if cost["calls_per_cell"] is not None else "—"
    )
    print(
        f"цена плеча B: вызовов {cost['calls_total']}, ячеек с вызовами "
        f"{cost['cells_with_calls']}, на ячейку {per_cell}"
    )
    if cost["calls_by_tool"]:
        print("  по инструментам: " + ", ".join(f"{k}={v}" for k, v in cost["calls_by_tool"].items()))
    if cost["call_failures"]:
        print("  провалы: " + ", ".join(f"{k}={v}" for k, v in cost["call_failures"].items()))
    if reasons:
        print("\nпровал опыта (план 28 §10.4):")
        for reason in reasons:
            print(f"  - {reason}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Парное сравнение профилей локальных инструментов оценщика (план 28 §10)"
    )
    parser.add_argument("--runs-dir", required=True, help="каталог прогонов")
    parser.add_argument("--run-a", required=True, help="плечо без инструментов")
    parser.add_argument("--run-b", required=True, help="плечо с инструментами")
    parser.add_argument("--golden", required=True, help="эталон (golden.csv)")
    parser.add_argument(
        "--allow-foreign-schema",
        action="store_true",
        help="читать прогон другой schema_version как исторический факт",
    )
    parser.add_argument(
        "--bootstrap", type=int, default=DEFAULT_BOOTSTRAP, help="число bootstrap-выборок"
    )
    parser.add_argument("--seed", type=int, default=0, help="seed bootstrap (воспроизводимость)")
    parser.add_argument("--json", action="store_true", help="машинный вывод")
    args = parser.parse_args(argv)

    runs = Path(args.runs_dir)
    base = read_arm(
        args.run_a, runs / args.run_a / "cohound.db", allow_foreign=args.allow_foreign_schema
    )
    variant = read_arm(
        args.run_b, runs / args.run_b / "cohound.db", allow_foreign=args.allow_foreign_schema
    )
    expected = {
        (row.product_id, row.param_id): row.expected_status
        for row in load_golden(args.golden)
    }
    changed, missing, contract_note = contract_divergence(base, variant)
    result = compare(base, variant, expected, bootstrap=args.bootstrap, seed=args.seed)
    cost = call_cost(variant, expected)
    reasons = verdicts_of_failure(result, cost)

    if args.json:
        print(
            json.dumps(
                {
                    "arms": {
                        "base": {"run_id": base.run_id, "tool_profile": base.tool_profile},
                        "variant": {
                            "run_id": variant.run_id,
                            "tool_profile": variant.tool_profile,
                        },
                    },
                    "contract_changed": changed,
                    "contract_missing": missing,
                    "contract_unrecoverable": _unrecoverable_fields(),
                    "contract_check_skipped": contract_note,
                    "paired": result,
                    "cost": cost,
                    "experiment_failures": reasons,
                    "bootstrap": args.bootstrap,
                    "seed": args.seed,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        _print_report(
            base=base, variant=variant, result=result, cost=cost,
            changed=changed, missing=missing, reasons=reasons,
            contract_note=contract_note,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
