"""R1a: цена durable очереди обхода — запись ``pending``-строк в ``crawl_session_items``.

Прибор, а не замер сам по себе: печатает три режима записи и их отношение. Отвечает на
вопрос «сколько транзакций и пересчётов счётчиков можно себе позволить», поэтому гоняется
на **копии** живой сессии (замер пишет и удаляет строки).

Три режима (все — через реальный путь ``SqliteStore.mark_crawl_pending``, не сырой SQL):

* ``A`` — пачками по ``--batch``, пересчёт счётчиков **один раз** в конце (рабочий режим
  R1a: пересчёт делает граница корня);
* ``B`` — пачками по ``--batch``, пересчёт на каждую пачку;
* ``C`` — строка на транзакцию с пересчётом на каждую: контроль ``O(n²)`` (пересчёт читает
  таблицу целиком, поэтому цифра растёт вместе с ней).

Публикация: ``docs/reports/measurements/2026-11-24-durable-queue.md``.

Запуск::

    cp <stand>/runs/<run_id>/cohound.db /tmp/opencode/bench.db
    python scripts/measure_durable_queue.py --db /tmp/opencode/bench.db \\
        --session-id crawl:<run_id> --product-id <package_id>
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Sequence

from core.domain import CrawlItemStatus, CrawlSessionItem
from core.store.sqlite_store import SqliteStore

#: Адреса замера — заведомо не существующий хост: строки удаляются после каждого режима,
#: и префикс нужен, чтобы удалить ровно свои (чужие ``pending`` не трогаются).
BENCH_URL_PREFIX = "https://bench.invalid/"


def _items(
    session_id: str, product_id: str, count: int, offset: int = 0
) -> list[CrawlSessionItem]:
    return [
        CrawlSessionItem(
            session_id=session_id,
            product_id=product_id,
            url=f"{BENCH_URL_PREFIX}{offset + i}",
            status=CrawlItemStatus.PENDING,
            depth=i % 5,
            first_seen="2026-11-24T00:00:00+00:00",
        )
        for i in range(count)
    ]


def _cleanup(store: SqliteStore, session_id: str) -> None:
    store.conn.execute(
        "DELETE FROM crawl_session_items WHERE url LIKE ?", (BENCH_URL_PREFIX + "%",)
    )
    store.conn.commit()
    store.mark_crawl_pending(session_id, [], recount=True)


def _batched(
    store: SqliteStore,
    session_id: str,
    product_id: str,
    rows: int,
    batch: int,
    *,
    recount_each: bool,
    label: str,
) -> float:
    started = time.perf_counter()
    for start in range(0, rows, batch):
        store.mark_crawl_pending(
            session_id,
            _items(session_id, product_id, min(batch, rows - start), start),
            recount=recount_each,
        )
    total = time.perf_counter() - started
    print(f"{label}: {total * 1000:.0f} ms на {rows} строк ({rows / total:.0f} строк/с)")
    return total


def _measure(
    db: str,
    session_id: str,
    product_id: str,
    *,
    rows: int,
    batch: int,
    row_by_row: int,
    repeats: int,
) -> None:
    with SqliteStore(db) as store:
        journal = store.conn.execute("PRAGMA journal_mode").fetchone()[0]
        before = store.conn.execute(
            "SELECT COUNT(*) FROM crawl_session_items WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]
        print(f"journal_mode={journal}; строк в сессии до замера: {before}")
        a_times: list[float] = []
        b_times: list[float] = []
        c_times: list[float] = []
        final_times: list[float] = []
        for _ in range(repeats):
            a_times.append(
                _batched(
                    store,
                    session_id,
                    product_id,
                    rows,
                    batch,
                    recount_each=False,
                    label="A пачками, пересчёт один раз",
                )
            )
            started = time.perf_counter()
            store.mark_crawl_pending(session_id, [], recount=True)
            final_times.append(time.perf_counter() - started)
            _cleanup(store, session_id)

            b_times.append(
                _batched(
                    store,
                    session_id,
                    product_id,
                    rows,
                    batch,
                    recount_each=True,
                    label="B пачками, пересчёт на пачку",
                )
            )
            _cleanup(store, session_id)

            started = time.perf_counter()
            for i in range(row_by_row):
                store.mark_crawl_pending(
                    session_id, _items(session_id, product_id, 1, i), recount=True
                )
            c_times.append(time.perf_counter() - started)
            _cleanup(store, session_id)

        def _median(values: Sequence[float]) -> float:
            ordered = sorted(values)
            return ordered[len(ordered) // 2]

        a, b, c, final = (
            _median(a_times),
            _median(b_times),
            _median(c_times),
            _median(final_times),
        )
        print(
            f"медиана за {repeats} повторов: "
            f"A={a * 1000:.0f} ms ({min(a_times) * 1000:.0f}–{max(a_times) * 1000:.0f}), "
            f"пересчёт на границе={final * 1000:.0f} ms, "
            f"B={b * 1000:.0f} ms, B/A={b / a:.2f}; "
            f"C={c * 1000:.0f} ms на {row_by_row} строк"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="копия cohound.db (не оригинал!)")
    parser.add_argument("--session-id", required=True, help="crawl:<run_id>")
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--rows", type=int, default=30207, help="сколько строк писать")
    parser.add_argument("--batch", type=int, default=256)
    parser.add_argument(
        "--row-by-row",
        type=int,
        default=1000,
        help="объём контроля O(n^2) (строка на транзакцию)",
    )
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    _measure(
        args.db,
        args.session_id,
        args.product_id,
        rows=args.rows,
        batch=args.batch,
        row_by_row=args.row_by_row,
        repeats=args.repeats,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
