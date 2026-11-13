"""Починка ложных ``parser_failed`` в БД прогона: «не пытались» ≠ «извлечение провалилось».

Зачем. До правки ``core/pipeline/extract_consume.py`` (14.11.2026) исчерпание пересборок
пула экстракции помечало **весь остаток корня** терминальным ``parser_failed`` — включая
документы, которых ни один воркер не видел. Такая строка одновременно:

* утверждает отказ экстрактора по конкретным байтам (ложная провенанс-запись, A3);
* входит в ``BLOCKING_FAILURE_KINDS`` и делает покрытие продукта неполным (I2c);
* терминальна для очереди: ``hound extract consume`` её больше не берёт, хотя байты
  документа сохранены (``extract_queue.raw_path``) и работа восстановима без сети.

Код больше так не пишет, но уже записанное надо вернуть в работу **видимо**: скрипт
печатает, что и сколько он меняет, а с ``--apply`` кладёт отчёт в
``runs/{run_id}/outputs/repair_abandoned_extract.json``.

Что чинится (две группы, вторая — по явному флагу):

``pool_exhausted``
    ``error = 'extraction pool exhausted after N rebuilds — inline disabled'`` —
    извлечения не было вовсе: строка возвращается в ``pending``, элемент сеанса — в
    ``fetched`` (байты получены, конверсии не было).

``worker_died`` (``--include-worker-died``)
    ``error = 'extraction worker died'`` — попытка была, но результата нет: воркер умер.
    Это отказ **инструмента**, а не документа; причина смерти не установлена, поэтому
    группа чинится только по явному запросу, а повтор может снова дать отказ — видимо.

Что **не** чинится: настоящие отказы экстрактора («trafilatura: извлечение не дало
текста», «текст пуст») — они описывают конкретные байты и остаются ``failed``.

Только чтение по умолчанию; запись — ``--apply``. Комментарии по-русски, идентификаторы
по-английски (§13).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.domain import CrawlItemStatus, ExtractQueueState  # noqa: E402
from core.store.sqlite_store import SqliteStore  # noqa: E402

#: Текст, который писала прежняя ветка «пул исчерпал пересборки». Точное совпадение, а не
#: подстрока: расширение шаблона сделало бы фильтр «почти тем же», и настоящий отказ
#: экстрактора с похожей формулировкой попал бы в починку.
POOL_EXHAUSTED = "extraction pool exhausted after 2 rebuilds — inline disabled"
WORKER_DIED = "extraction worker died"

REPORT_NAME = "repair_abandoned_extract.json"


def _select(store: SqliteStore, session_id: str) -> dict[str, list[Any]]:
    """Отбирает строки по **причине** записи, а не по состоянию: состояние — следствие."""
    groups: dict[str, list[Any]] = {"pool_exhausted": [], "worker_died": []}
    for row in store.iter_extract_queue(session_id):
        if row.state is not ExtractQueueState.FAILED:
            continue
        text = (row.error or "").strip()
        if text == POOL_EXHAUSTED:
            groups["pool_exhausted"].append(row)
        elif text == WORKER_DIED:
            groups["worker_died"].append(row)
    return groups


def _counts(rows: list[Any]) -> dict[str, int]:
    return dict(Counter(row.product_id for row in rows))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="БД прогона: runs/{run_id}/cohound.db")
    parser.add_argument(
        "--session",
        default=None,
        help="идентификатор сеанса (по умолчанию — единственный в БД)",
    )
    parser.add_argument(
        "--include-worker-died",
        action="store_true",
        help="вернуть в работу и строки «extraction worker died» (отказ инструмента)",
    )
    parser.add_argument(
        "--apply", action="store_true", help="записать изменения (по умолчанию — только чтение)"
    )
    parser.add_argument("--json", action="store_true", help="машинночитаемый вывод")
    args = parser.parse_args(argv)

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"БД прогона не найдена: {db_path}", file=sys.stderr)
        return 4

    store = SqliteStore(db_path)
    try:
        session_id = args.session or _default_session(store)
        groups = _select(store, session_id)
        selected = list(groups["pool_exhausted"])
        if args.include_worker_died:
            selected += groups["worker_died"]

        items = {
            (item.product_id, item.url): item
            for item in store.iter_crawl_session_items(session_id)
        }
        missing = [row for row in selected if (row.product_id, row.url) not in items]
        restored = 0
        released = 0
        if args.apply:
            for row in selected:
                item = items.get((row.product_id, row.url))
                if item is not None and item.status is CrawlItemStatus.FAILED:
                    store.upsert_crawl_session_item(
                        item.model_copy(
                            update={
                                "status": CrawlItemStatus.FETCHED,
                                "error_kind": None,
                                "error": None,
                            }
                        )
                    )
                    restored += 1
                if store.set_extract_state(
                    session_id,
                    row.product_id,
                    row.url,
                    ExtractQueueState.PENDING,
                    expect=ExtractQueueState.FAILED,
                    error_kind=None,
                    error=None,
                ):
                    released += 1
            store.conn.commit()

        run_row = store.conn.execute("SELECT run_id FROM runs LIMIT 1").fetchone()
        report: dict[str, Any] = {
            "run_id": run_row[0] if run_row is not None else None,
            "db": str(db_path),
            "session_id": session_id,
            "applied": bool(args.apply),
            "groups": {
                "pool_exhausted": {
                    "rows": len(groups["pool_exhausted"]),
                    "by_product": _counts(groups["pool_exhausted"]),
                    "selected": True,
                },
                "worker_died": {
                    "rows": len(groups["worker_died"]),
                    "by_product": _counts(groups["worker_died"]),
                    "selected": bool(args.include_worker_died),
                },
            },
            "selected_rows": len(selected),
            "session_items_restored": restored,
            "queue_rows_released": released,
            "missing_session_items": len(missing),
            "untouched": (
                "настоящие отказы экстрактора (trafilatura/пустой текст) остаются failed"
            ),
        }
        if args.apply:
            out_dir = db_path.parent / "outputs"
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / REPORT_NAME).write_text(
                json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            report["report_path"] = str(out_dir / REPORT_NAME)

        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            verb = "возвращено в работу" if args.apply else "будет возвращено в работу"
            print(f"сеанс: {session_id}")
            for name, group in report["groups"].items():
                mark = "✓" if group["selected"] else "·"
                by = ", ".join(f"{k}={v}" for k, v in sorted(group["by_product"].items()))
                print(f" {mark} {name}: {group['rows']} строк ({by})")
            print(f"{verb}: {len(selected)}")
            if args.apply:
                print(
                    f"  элементов сеанса восстановлено: {restored}, "
                    f"строк очереди в pending: {released}"
                )
            if missing:
                print(f"  ВНИМАНИЕ: нет элементов сеанса для {missing} строк")
            print(f"не тронуто: {report['untouched']}")
            if not args.apply:
                print("запись не выполнялась: добавьте --apply")
        return 0
    finally:
        store.close()


def _default_session(store: SqliteStore) -> str:
    """Сеанс прогона: ровно один на прогон (``crawl:{run_id}``), иначе — явный выбор."""
    rows = [row[0] for row in store.conn.execute("SELECT session_id FROM crawl_sessions")]
    if len(rows) == 1:
        return str(rows[0])
    if not rows:
        raise SystemExit("в БД нет ни одного сеанса краула: укажите --session")
    raise SystemExit(
        "в БД несколько сеансов: укажите --session ("
        + ", ".join(sorted(str(row) for row in rows))
        + ")"
    )


if __name__ == "__main__":
    raise SystemExit(main())
