#!/usr/bin/env python3
"""Распределение **длительности** извлечения по уровням (O30, план 26).

**Вопрос, на который прибор отвечает.** Таймаут документа — число, которое надо вывести из
распределения, а не из одного запомнившегося документа. В плане 26 зафиксировано:
`FULL: None` (таймаута нет), «117 с на 81 стр. (01.11)» и «151.4 с на 30 стр. (23.10)» — это
**два** документа, а не p95. Прибор снимает p50/p95/p99/max на выборке durable-корпуса и
публикует их вместе с условиями (уровень, механизм, устройство, память узла), потому что без
условий число не переиспользуется.

**Почему выборка, а не весь корпус.** Полный прогон `full` по 19 545 страницам — это часы,
и они не покупают ничего сверх интервала: длительность документа распределена с тяжёлым
хвостом, поэтому её надо мерить на **стратифицированной** выборке и публиковать интервал, а
не точечное число. Отбор детерминирован (`--seed`), страты — по числу страниц: короткие,
средние, длинные и очень длинные документы имеют разную цену на страницу, и «средняя по
корпусу» смешала бы состав выборки с эффектом длины.

**Почему документ мерится в дочернем процессе.** Две причины, обе измеренные, а не
предположенные:

* ``ru_maxrss``/``VmHWM`` монотонны в пределах процесса — второй документ в том же процессе
  дал бы **сумму** с первым, и «пик на документ» перестал бы быть пиком на документ;
* уровень ``full`` упирается в память: замер 16.11.2026 в одном процессе был убит
  OOM-killer'ом на 15-м документе (118 страниц, RSS 5.38 ГБ при 6.86 ГБ на узле), и от
  33 минут работы не осталось ни одного числа. Убийство ребёнка родитель переживает: он
  записывает отказ строкой и идёт дальше.

Загрузка весов (20–40 с) — цена **воркера**, а не документа, поэтому таймер стартует **после**
сборки экстрактора и в распределение не входит; её цена публикуется отдельно (`startup_s`).

**Почему результат пишется по документу.** Сводка, которая появляется только в конце, теряется
целиком при убийстве процесса. Каждая строка дописывается в ``<out>.jsonl`` сразу после замера,
а сводка пересчитывается **из журнала** — в том числе из журнала убитого прогона
(``--from-journal``): данные переживают прибор.

**Чего прибор не мерит.** Пик памяти как отдельный предмет — это O33
(``measure_extract_peak.py``); качество текста — O38/O40. Здесь только время и его
распределение, плюс пик RSS **того же** замера как условие, без которого число не читается.

Запуск::

    # распределение full на выборке из 20 документов (по 5 на страту)
    .venv/bin/python scripts/measure_extract_duration.py \
      --db ~/.cohound/stands/ru-pg-dbms/state/ru-pg-dbms/state.db \
      --level full --device cuda --sample 20 --out-json /tmp/full.json

    # сводка из журнала уже сделанного (в т.ч. убитого) прогона
    .venv/bin/python scripts/measure_extract_duration.py --db … --from-journal /tmp/full.jsonl
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
_CHILD_PREFIX = "CHILD_JSON:"

#: Страты по числу страниц: (метка, нижняя граница включительно, верхняя исключительно).
STRATA: tuple[tuple[str, int, int | None], ...] = (
    ("short", 1, 11),
    ("medium", 11, 41),
    ("long", 41, 101),
    ("huge", 101, None),
)


def _is_pdf(path: Path) -> bool:
    """PDF ли это — по сигнатуре: retained-файл называется ``{sha256}.bin``."""
    try:
        with path.open("rb") as handle:
            return handle.read(5).startswith(b"%PDF")
    except OSError:
        return False


def _candidates(db: str) -> list[dict[str, Any]]:
    """Документы с retained-байтами и заявленным числом страниц, из ``state.db``.

    Читается только объявленный факт (``raw_path``, ``extract_page_count``) и только
    читающим соединением: прибор ничего не пишет в операторскую базу.
    """
    if not Path(db).is_file():
        raise SystemExit(f"measure: нет файла state.db: {db}")
    uri = f"file:{Path(db).expanduser().resolve()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        rows = connection.execute(
            "select extract_page_count, content_hash, extractor_version, level, raw_path"
            " from document_version"
            " where raw_path is not null and extract_page_count is not null"
        ).fetchall()
    except sqlite3.Error as exc:
        raise SystemExit(f"measure: state.db не читается: {exc}") from exc
    finally:
        connection.close()
    picked: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page_count, content_hash, extractor_version, level, raw_path in rows:
        path = Path(raw_path)
        if not path.is_file() or not _is_pdf(path):
            continue
        key = str(path)
        if key in seen:  # одна версия на несколько URL — мерим один раз
            continue
        seen.add(key)
        picked.append(
            {
                "path": key,
                "page_count": int(page_count),
                "content_hash": content_hash,
                "recorded_extractor_version": extractor_version,
                "recorded_level": level,
            }
        )
    return picked


def _stratified(
    candidates: list[dict[str, Any]], sample: int, seed: int
) -> list[dict[str, Any]]:
    """Стратифицированная выборка: поровну на страту, порядок детерминирован ``seed``.

    Порядок внутри страты перемешивается **до** отбора: иначе «первые 5 длинных» были бы
    первыми пятью по порядку в базе, то есть одним продуктом, и выборка описывала бы
    продукт, а не корпус.
    """
    generator = random.Random(seed)  # noqa: S311 — воспроизводимость по seed (A3)
    per_stratum = max(1, sample // len(STRATA))
    buckets: list[tuple[str, list[dict[str, Any]]]] = []
    for label, low, high in STRATA:
        bucket = [
            item
            for item in candidates
            if item["page_count"] >= low and (high is None or item["page_count"] < high)
        ]
        bucket.sort(key=lambda item: item["path"])
        generator.shuffle(bucket)
        buckets.append((label, bucket))
    chosen: list[dict[str, Any]] = []
    taken: dict[str, int] = {}
    for label, bucket in buckets:
        for item in bucket[:per_stratum]:
            chosen.append({**item, "stratum": label})
            taken[label] = taken.get(label, 0) + 1
    # Добор: страта может быть пустой (или короче квоты), и тогда квота пропадала бы —
    # выборка из 20 документов схлопывалась бы в 4 на корпусе из одних длинных PDF.
    # Добор идёт по кругу, чтобы перекос состава не превратился в перекос страт.
    leftover = [(label, bucket[taken.get(label, 0):]) for label, bucket in buckets]
    while len(chosen) < sample and any(items for _, items in leftover):
        for index, (label, items) in enumerate(leftover):
            if len(chosen) >= sample:
                break
            if not items:
                continue
            chosen.append({**items.pop(0), "stratum": label})
            leftover[index] = (label, items)
    return chosen


def _build_extractor(mechanism: str, level: str, device: str) -> Any:
    """Экстрактор объявленного механизма — те же конструкторы, что в проде."""
    if mechanism == "text_layer":
        from core.adapters.extract_pdf_text import PdfiumDocumentExtractor

        return PdfiumDocumentExtractor()
    from core.adapters.extract_document import DoclingDocumentExtractor
    from core.domain.enums import DocumentLevel, PdfDevice

    return DoclingDocumentExtractor(DocumentLevel(level), pdf_device=PdfDevice(device))


def _hwm_mb() -> float | None:
    """Пик RSS этого процесса, МиБ (``/proc/self/status:VmHWM``).

    Публикуется рядом с длительностью, потому что на слабом хосте распределение времени
    ограничено памятью: замер 16.11.2026 был убит OOM на документе, чей RSS дошёл до 5.38 ГБ.
    """
    try:
        with open("/proc/self/status", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmHWM:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        return None
    return None


def _host_mem_total_mb() -> float | None:
    """Всего памяти на узле, МиБ (``/proc/meminfo:MemTotal``) — условие замера."""
    try:
        with open("/proc/meminfo", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemTotal:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        return None
    return None


class _Journal:
    """Дописываемый журнал замеров: строка на документ, сразу после замера.

    Прибор, который пишет результат только в конце, теряет всё при убийстве процесса — и это
    не гипотеза: прогон 16.11.2026 погиб на 15-м документе из 20, оставив 33 минуты без числа.
    """

    def __init__(self, path: Path | None) -> None:
        self.path = path
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")

    def write(self, row: dict[str, Any]) -> None:
        if self.path is None:
            return
        payload = {**row, "seconds": round(float(row["seconds"]), 3)}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
            handle.flush()


def _read_journal(path: Path) -> list[dict[str, Any]]:
    """Журнал замеров: строки, которые прибор успел записать (убитый прогон тоже годится)."""
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except ValueError as exc:
            raise SystemExit(f"measure: журнал повреждён на строке: {line[:80]!r}") from exc
    return rows


def _percentile(values: list[float], fraction: float) -> float:
    """Перцентиль по ближайшему рангу: при малом n интерполяция выдаёт числа, которых нет."""
    if not values:
        raise ValueError("перцентиль от пустого списка")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(fraction * (len(ordered) - 1))))
    return ordered[index]


def _bootstrap_ci(
    values: list[float], fraction: float, *, bootstrap: int, seed: int
) -> list[float]:
    """Интервал перцентиля бутстрэпом по документам (2.5/97.5 перцентили ресемплов).

    Единица ресемплинга — **документ**, а не страница: страницы одного документа не
    независимы (общий шрифт, общая раскладка), и бутстрэп по страницам дал бы интервал
    уже, чем есть.
    """
    if not values:
        return [0.0, 0.0]
    generator = random.Random(seed)  # noqa: S311
    n = len(values)
    samples: list[float] = []
    for _ in range(max(1, bootstrap)):
        draw = [values[generator.randrange(n)] for _ in range(n)]
        samples.append(_percentile(draw, fraction))
    samples.sort()
    return [
        samples[int(0.025 * (len(samples) - 1))],
        samples[int(0.975 * (len(samples) - 1))],
    ]


def _summarize(
    rows: list[dict[str, Any]], *, bootstrap: int, seed: int
) -> dict[str, Any]:
    """Сводка распределения: по документам и по страницам, с интервалами."""
    per_doc = [row["seconds"] for row in rows]
    per_page = [
        row["seconds"] / row["page_count"] for row in rows if row["page_count"] > 0
    ]
    peaks = [row["peak_rss_mb"] for row in rows if row.get("peak_rss_mb") is not None]
    summary: dict[str, Any] = {
        "documents": len(rows),
        "pages": sum(r["page_count"] for r in rows),
        "peak_rss_mb": max(peaks) if peaks else None,
    }
    for name, values in (("document", per_doc), ("page", per_page)):
        summary[name] = {
            "mean": sum(values) / len(values) if values else 0.0,
            "p50": _percentile(values, 0.50),
            "p95": _percentile(values, 0.95),
            "p99": _percentile(values, 0.99),
            "max": max(values) if values else 0.0,
            "p50_ci": _bootstrap_ci(values, 0.50, bootstrap=bootstrap, seed=seed),
            "p95_ci": _bootstrap_ci(values, 0.95, bootstrap=bootstrap, seed=seed),
        }
    return summary


def _run_child(args: argparse.Namespace) -> int:
    """Дочерний замер: экстрактор строится до таймера, мерится только извлечение."""
    extractor = _build_extractor(args.mechanism, args.level, args.device)
    started = time.perf_counter()
    try:
        document = extractor.extract_path(str(args.pdf), filename_hint="document.pdf")
    except Exception as exc:  # noqa: BLE001 — цена отказа тоже факт
        print(
            _CHILD_PREFIX
            + json.dumps(
                {
                    "seconds": time.perf_counter() - started,
                    "error": f"{type(exc).__name__}: {exc}",
                    "peak_rss_mb": _hwm_mb(),
                },
                ensure_ascii=False,
            )
        )
        return 0
    text = getattr(document, "text", None) or ""
    print(
        _CHILD_PREFIX
        + json.dumps(
            {
                "seconds": time.perf_counter() - started,
                "chars": len(text),
                "peak_rss_mb": _hwm_mb(),
                "error": None,
            },
            ensure_ascii=False,
        )
    )
    return 0


def _measure_one(
    item: dict[str, Any],
    *,
    mechanism: str,
    level: str,
    device: str,
    timeout: float,
) -> dict[str, Any]:
    """Один документ в дочернем процессе: убийство ребёнка не уносит весь замер."""
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--child",
        "--pdf",
        item["path"],
        "--mechanism",
        mechanism,
        "--level",
        level,
        "--device",
        device,
    ]
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, cwd=str(REPO)
        )
    except subprocess.TimeoutExpired:
        return {**item, "seconds": timeout, "error": f"timeout {timeout:.0f} s"}
    wall = round(time.perf_counter() - started, 3)
    payload: dict[str, Any] | None = None
    for line in completed.stdout.splitlines():
        if line.startswith(_CHILD_PREFIX):
            try:
                payload = json.loads(line[len(_CHILD_PREFIX) :])
            except ValueError:
                payload = None
    if payload is None:
        return {
            **item,
            "seconds": wall,
            "wall_seconds": wall,
            "error": f"ребёнок не дал замера (exit {completed.returncode})",
            "stderr_tail": completed.stderr[-500:],
        }
    return {
        **item,
        **payload,
        "wall_seconds": wall,
        "startup_s": round(wall - float(payload["seconds"]), 3),
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Распределение длительности извлечения по уровням (O30)"
    )
    parser.add_argument("--db", default="", help="state.db оператора (только чтение)")
    parser.add_argument(
        "--level",
        default="full",
        help="уровень извлечения для structured: off|text|layout|layout_tables|full",
    )
    parser.add_argument(
        "--mechanism",
        default="structured",
        choices=("structured", "text_layer"),
        help="механизм PDF (structured — docling, text_layer — pdfium)",
    )
    parser.add_argument("--device", default="cpu", choices=("cpu", "cuda"))
    parser.add_argument(
        "--sample",
        type=int,
        default=20,
        help="сколько документов мерить (поровну на страту; 0 — все)",
    )
    parser.add_argument("--seed", type=int, default=0, help="seed отбора (объявленный)")
    parser.add_argument("--bootstrap", type=int, default=1000, help="ресемплов бутстрэпа")
    parser.add_argument(
        "--timeout", type=float, default=3600.0, help="таймаут одного документа, с"
    )
    parser.add_argument("--out-json", metavar="PATH", help="сводка; журнал — рядом, .jsonl")
    parser.add_argument(
        "--from-journal",
        metavar="PATH",
        help="не мерить, а пересчитать сводку из журнала (в т.ч. убитого прогона)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="только отбор: какие документы были бы измерены, без извлечения",
    )
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--pdf", default="", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.child:
        return _run_child(args)
    if not args.db:
        print("measure: --db обязателен (кроме --child)", file=sys.stderr)
        return 2

    if args.from_journal:
        journal_path = Path(args.from_journal).expanduser()
        if not journal_path.is_file():
            print(f"measure: журнала нет: {journal_path}", file=sys.stderr)
            return 2
        rows = _read_journal(journal_path)
        if not rows:
            print("measure: журнал пуст — распределения нет", file=sys.stderr)
            return 1
        return _report(
            rows,
            args,
            conditions={
                "source": "journal",
                "journal": str(journal_path),
                "planned": None,
                "host_mem_total_mb": _host_mem_total_mb(),
            },
        )

    candidates = _candidates(args.db)
    if not candidates:
        print(
            "measure: в state.db нет документов с retained-байтами и числом страниц",
            file=sys.stderr,
        )
        return 2
    chosen = _stratified(candidates, args.sample, args.seed) if args.sample else candidates
    if not chosen:
        print("measure: выборка пуста", file=sys.stderr)
        return 2
    print(
        f"отбор: {len(chosen)} документов из {len(candidates)}; "
        f"страницы {sum(c['page_count'] for c in chosen)}; "
        f"страты {sorted({c['stratum'] for c in chosen}) if args.sample else 'все'}",
        flush=True,
    )
    if args.dry_run:
        for item in chosen:
            print(f"  {item['page_count']:>5} стр. {item['stratum']:<7} {item['path']}")
        return 0

    journal = _Journal(Path(args.out_json).with_suffix(".jsonl") if args.out_json else None)
    rows: list[dict[str, Any]] = []
    for position, item in enumerate(chosen):
        row = _measure_one(
            item,
            mechanism=args.mechanism,
            level=args.level,
            device=args.device,
            timeout=args.timeout,
        )
        rows.append(row)
        journal.write(row)
        if row.get("error"):
            print(
                f"  [{position + 1}/{len(chosen)}] {item['page_count']:>5} стр. "
                f"ОТКАЗ {row['error']}",
                flush=True,
            )
            continue
        print(
            f"  [{position + 1}/{len(chosen)}] {item['page_count']:>5} стр. "
            f"{row['seconds']:7.1f} с ({row['seconds'] / max(1, item['page_count']):.2f} с/стр), "
            f"пик {row.get('peak_rss_mb') or 0:.0f} MiB, старт {row['startup_s']:.1f} с",
            flush=True,
        )
    return _report(
        rows,
        args,
        conditions={
            "source": "measured",
            "journal": str(Path(args.out_json).with_suffix(".jsonl")) if args.out_json else None,
            "planned": len(chosen),
            "candidates": len(candidates),
            "host_mem_total_mb": _host_mem_total_mb(),
        },
    )


def _report(
    rows: list[dict[str, Any]], args: argparse.Namespace, *, conditions: dict[str, Any]
) -> int:
    """Сводка и её публикация. Отказы считаются отдельно и не исчезают."""
    measured = [row for row in rows if not row.get("error") and row.get("seconds")]
    failed = [row for row in rows if row.get("error")]
    # Отчёт пишется и когда не измерено **ничего**: «все двадцать отказали» — это результат,
    # и он должен пережить прогон. Отсутствие распределения — код возврата 1, не пустой файл.
    summary = _summarize(measured, bootstrap=args.bootstrap, seed=args.seed) if measured else None
    startups = [row["startup_s"] for row in measured if row.get("startup_s") is not None]
    payload = {
        "instrument": "scripts/measure_extract_duration.py",
        "conditions": {
            **conditions,
            "db": str(Path(args.db).expanduser()),
            "level": args.level,
            "mechanism": args.mechanism,
            "device": args.device,
            "sample": args.sample,
            "seed": args.seed,
            "bootstrap": args.bootstrap,
            "measured": len(measured),
            "failed": len(failed),
            "startup_s_median": _percentile(startups, 0.5) if startups else None,
        },
        "summary": summary,
        "documents": [
            {**row, "seconds": round(float(row["seconds"]), 3)} for row in measured
        ],
        "failed_documents": [
            {**row, "seconds": round(float(row["seconds"]), 3)} for row in failed
        ],
    }
    print()
    if summary is None:
        print(f"документов 0, отказов {len(failed)} — распределения нет")
    else:
        for name in ("document", "page"):
            block = summary[name]
            print(
                f"{name:>9}: mean {block['mean']:.2f}  p50 {block['p50']:.2f} "
                f"[{block['p50_ci'][0]:.2f}; {block['p50_ci'][1]:.2f}]  "
                f"p95 {block['p95']:.2f} [{block['p95_ci'][0]:.2f}; {block['p95_ci'][1]:.2f}]  "
                f"p99 {block['p99']:.2f}  max {block['max']:.2f}"
            )
        print(
            f"документов {summary['documents']}, страниц {summary['pages']}, "
            f"отказов {len(failed)}, пик RSS {summary['peak_rss_mb']} MiB"
        )
    if args.out_json:
        Path(args.out_json).write_text(
            json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
        )
        print(f"отчёт: {args.out_json}")
    return 0 if measured else 1


if __name__ == "__main__":
    raise SystemExit(main())
