"""Аномалии ``heading_path`` на готовом прогоне: приёмка O28 без прогона.

Зачем прибор. Агрегат O28 (`core/pipeline/heading_anomalies.py`) считается внутри
`report` и пишется в `outputs/heading_anomalies.csv`. Но `report` перезаписывает артефакты
прогона, а у эталонного прогона они подписаны G4: перезапись сделала бы подпись ложью.
Поэтому приёмка «отчёт называет страницу-виновника» проверяется тем же кодом продукта,
вызванным **только на чтение**, и сверяется с независимым прибором
`scripts/measure_md_chunking.py` (он режет сырые байты заново, а не читает `corpus_chunks`).

Что печатается: бакеты порогов (число чанков, документов, доля), топ документов-виновников
с адресом, замер контекста в символах и признак «токенный замер не производился» (без
токенизатора число не выдаётся за токены — A3).

Запуск:

    .venv/bin/python scripts/measure_heading_anomalies.py \
      --db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-2a5b51af5778/cohound.db \
      --run-id ru-pg-dbms-2a5b51af5778 \
      --out-json docs/reports/measurements/2026-11-09-heading-anomalies-acceptance/summary.json

Границы прибора: он читает `corpus_chunks` прогона, то есть **то, что было нарезано**.
Расхождение с харнессом означает либо другой чанкер, либо другой корпус — и это видно по
числу чанков, а не по «примерно совпало».
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from core.pipeline.heading_anomalies import write_heading_anomalies_artifact
from core.store.sqlite_store import SqliteStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--csv", default=None, help="куда писать CSV (по умолчанию рядом с JSON)")
    parser.add_argument("--out-json", required=True, help="куда писать машинный отчёт")
    args = parser.parse_args(argv)

    db_path = Path(args.db).expanduser()
    if not db_path.is_file():
        print(f"measure: БД не найдена: {db_path}", file=sys.stderr)
        return 2
    csv_path = (
        Path(args.csv).expanduser()
        if args.csv
        else Path(args.out_json).expanduser().with_suffix(".csv")
    )
    store = SqliteStore(db_path, read_only=True)
    result = write_heading_anomalies_artifact(store, args.run_id, csv_path)

    summary = result.get("summary")
    print(f"run_id: {args.run_id}")
    print(f"written: {result.get('written')}  reason: {result.get('reason')}")
    if not summary:
        payload = {"run_id": args.run_id, "written": False, "reason": result.get("reason")}
    else:
        payload = {"run_id": args.run_id, "written": True, "csv": str(csv_path), **summary}
        print(f"чанков: {summary['chunks_total']}  самый длинный heading: {summary['longest_heading_chars']} симв.")
        for threshold, bucket in summary["buckets"].items():
            print(
                f"  > {threshold} симв.: {bucket['chunks']} чанков на {bucket['documents']} документах"
                f" ({bucket['share']:.4%})"
            )
            for entry in bucket["top"][:3]:
                print(
                    f"     {entry['chunks']:5} чанков  max={entry['max_heading_chars']:5}  {entry.get('url')}"
                )
        chars = summary["context"]["chars"]
        print(
            f"контекст (символы) длиннее {chars['limit']:.2f}× текста: "
            f"{chars['heavy_chunks']} чанков ({chars['heavy_share']:.4%})"
        )
        tokens = summary["context"]["tokens"]
        print(
            "контекст (токены): "
            + ("измерен" if tokens["measured"] else f"не измерен — {tokens['reason']}")
        )
    target = Path(args.out_json).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"json: {target}")
    return 0 if payload.get("written") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
