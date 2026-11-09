"""Профиль ресурсов PDF-корпуса: страницы, символы, пустой слой, время.

Инструмент отвечает на вопрос пункта O31 (`docs/architecture/26-open-review-items-plan.md`
§14): **какой у PDF-корпуса хвост распределения**, а не каково среднее. Пороги `max_pages`
и `max_extracted_chars` (решение D9) назначаются по хвосту, поэтому скрипт печатает не
только перцентили, но и «что обрезал бы каждый кандидат» — число документов, страниц и
символов, выпавших за объявленную границу.

**Вход** — сохранённые артефакты прогона, только чтение:

* `runs/{run_id}/cohound.db` — `corpus_docs` (`page_count`, `extracted_chars`, `raw_path`);
  PDF определяется по **магии байтов** (`%PDF`), а не по имени или `content_type`;
* `corpus_raw/{product}/{hash}.bin` — сохранённые байты; для документов из контейнеров
  (`url` вида `…zip!/member.pdf`) `raw_path` указывает на контейнер, а член читается по
  имени из URL. Целостность: имя файла/контейнера в `corpus_raw` — адрес по содержимому
  (sha256 байтов), плюс сверка числа страниц файла с `corpus_docs.page_count`.

Сеть не используется, БД не пишется, `core/` не импортируется.

**Метрики.**

``pages`` / ``chars``
    Заявленные документом страницы и длина извлечённого текста (артефакт прогона,
    извлекатель `docling`). Это то, что видел конвейер.
``empty`` / ``sparse``
    Страницы, где `get_text_range()` пуст, и страницы с < ``--sparse-threshold`` символов
    (по умолчанию 40 — определение опубликованного замера 25.10.2026). Считается по
    сохранённым байтам через `pypdfium2`, то есть это свойство **файла**, а не docling.
``read_s``
    Время чтения страниц `pypdfium2` (открытие + обход) — прибор скрипта, **не** время
    docling. Время извлечения docling из артефактов не восстанавливается: единственные
    метки в `extract_queue` (`claimed_at` → `state_changed_at`) включают ожидание в
    очереди и потому длительностью извлечения не являются.

**Границы прибора** (печатаются в отчёте и в `--json`): только сохранённые байты одного
прогона; не весь интернет и не «все PDF, что существуют»; пустой слой — свойство
конкретного файла, а не оценка качества OCR.

Запуск::

    .venv/bin/python scripts/measure_pdf_profile.py
    .venv/bin/python scripts/measure_pdf_profile.py --json --json-out /tmp/profile.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import sqlite3
import sys
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import pypdfium2 as pdfium

# Границы корзин: показывают, где сидит хвост, не заменяя перцентили.
PAGE_BUCKETS = (1, 10, 25, 50, 100, 250, 500, 1000)
CHAR_BUCKETS = (10_000, 30_000, 60_000, 120_000, 300_000, 1_000_000)
# Кандидаты для D9: не рекомендация, а «что выпало бы при такой границе».
PAGE_CAPS = (250, 500, 1000, 2000, 3000)
CHAR_CAPS = (1_000_000, 2_000_000, 4_000_000, 8_000_000)
PDF_MAGIC = b"%PDF"


@dataclass
class DocRecord:
    """Один PDF-документ: заявленные величины и то, что увидел прибор."""

    doc_id: str
    product_id: str
    url: str
    pages: int
    chars: int
    raw_path: str
    content_hash: str = ""
    pages_read: int | None = None
    empty_pages: int | None = None
    sparse_pages: int | None = None
    pdfium_chars: int | None = None
    read_s: float | None = None
    failure: str | None = None


@dataclass
class Totals:
    """Счётчики целостности замера — то, что могло бы испортить числа молча."""

    resolved: int = 0
    unresolved: int = 0
    not_pdf_magic: int = 0
    raw_hash_mismatch: int = 0
    container_hash_mismatch: int = 0
    page_count_mismatch: int = 0
    failed: int = 0
    notes: list[str] = field(default_factory=list)


def peak_mb() -> float:
    """Пик RSS процесса, MiB."""
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def percentile(values: list[int] | list[float], p: float) -> float:
    """Перцентиль линейной интерполяцией (тип 7, как умолчание numpy)."""
    if not values:
        raise ValueError("пустой ряд")
    ordered = sorted(values)
    pos = (len(ordered) - 1) * p / 100.0
    low = int(pos)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


def distribution(values: list[int] | list[float], digits: int = 1) -> dict:
    """Перцентили и сумма ряда; пустой ряд — явное `measured=False`."""
    if not values:
        return {"measured": False, "n": 0}
    ordered = sorted(values)
    return {
        "measured": True,
        "n": len(ordered),
        "min": round(ordered[0], digits),
        "p50": round(percentile(ordered, 50), digits),
        "p90": round(percentile(ordered, 90), digits),
        "p95": round(percentile(ordered, 95), digits),
        "p99": round(percentile(ordered, 99), digits),
        "max": round(ordered[-1], digits),
        "mean": round(sum(ordered) / len(ordered), digits),
        "sum": round(sum(ordered), digits),
    }


def buckets(values: list[int], edges: tuple[int, ...]) -> list[dict]:
    """Корзины «до edge» + хвост: число документов и их доля."""
    total = len(values) or 1
    out: list[dict] = []
    previous = 0
    for edge in edges:
        count = sum(1 for value in values if previous < value <= edge)
        out.append({"from": previous, "to": edge, "count": count, "share": round(count / total, 4)})
        previous = edge
    tail = sum(1 for value in values if value > previous)
    out.append(
        {"from": previous, "to": None, "count": tail, "share": round(tail / total, 4)}
    )
    return out


def cap_effect(docs: list[DocRecord], attr: str, caps: tuple[int, ...]) -> list[dict]:
    """Что выпало бы, если объявить границу по `attr` (страницы или символы).

    «Выпало» — документ целиком выше границы: конвейер либо обрезал бы его до границы,
    либо отказался бы. Числа показывают цену каждой границы, а не выбирают её.
    """
    out: list[dict] = []
    total_docs = len(docs) or 1
    total_value = sum(getattr(doc, attr) for doc in docs) or 1
    for cap in caps:
        over = [doc for doc in docs if getattr(doc, attr) > cap]
        value_over = sum(getattr(doc, attr) for doc in over)
        out.append(
            {
                "cap": cap,
                "documents_over": len(over),
                "documents_share": round(len(over) / total_docs, 4),
                "value_over": value_over,
                "value_share": round(value_over / total_value, 4),
                "value_trimmed": value_over - cap * len(over),
                "urls": [doc.url for doc in sorted(over, key=lambda d: -getattr(d, attr))[:5]],
            }
        )
    return out


def _connect(db_path: Path) -> sqlite3.Connection:
    """Read-only соединение: инструмент не имеет права писать в артефакты прогона."""
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _pick_run(runs_dir: Path, run_id: str | None) -> Path:
    """Каталог прогона: явный `--run-id` или тот, где больше всего PDF-документов."""
    if run_id:
        candidate = runs_dir / run_id / "cohound.db"
        if not candidate.exists():
            raise SystemExit(f"нет БД прогона: {candidate}")
        return candidate
    best: tuple[int, Path] | None = None
    for db_path in sorted(runs_dir.glob("*/cohound.db")):
        try:
            with _connect(db_path) as conn:
                count = conn.execute(
                    "select count(*) from corpus_docs where page_count is not null"
                ).fetchone()[0]
        except sqlite3.Error:
            continue
        if best is None or count > best[0]:
            best = (count, db_path)
    if best is None or best[0] == 0:
        raise SystemExit(f"в {runs_dir} нет прогонов с page_count")
    return best[1]


def _load_docs(db_path: Path, products: set[str] | None) -> list[DocRecord]:
    """PDF-документы прогона по заявленному `page_count` (docling его пишет только для PDF)."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            select doc_id, product_id, url, page_count, extracted_chars, raw_path,
                   content_hash
            from corpus_docs
            where page_count is not null
            order by page_count desc, url
            """
        ).fetchall()
    docs = [
        DocRecord(
            doc_id=row[0],
            product_id=row[1],
            url=row[2],
            pages=int(row[3]),
            chars=int(row[4] or 0),
            raw_path=row[5] or "",
            content_hash=row[6] or "",
        )
        for row in rows
        if products is None or row[1] in products
    ]
    return docs


def _hash_file(path: Path, chunk: int = 1 << 20) -> str:
    """sha256 файла потоком: корпус не читается в память целиком."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest()


def _member_name(url: str) -> str | None:
    """Имя члена контейнера из URL `…archive.zip!/path/inside.pdf` (последний `!/`)."""
    marker = "!/"
    if marker not in url:
        return None
    return url.rsplit(marker, 1)[1]


def _materialize(
    doc: DocRecord,
    tmp_dir: Path,
    totals: Totals,
    container_hashes: dict[str, str],
) -> Path | None:
    """Путь к PDF-байтам документа: прямой файл или член контейнера, выписанный во временный.

    Магия проверяется по содержимому; имя файла и `content_type` не доказательство.
    Целостность: у прямого файла имя в `corpus_raw` — адрес по содержимому (sha256 байтов),
    у члена контейнера сверяется хеш самого контейнера (байты члена отдельно не хранятся,
    поэтому их хеш не персистится — план 3.21.0).
    """
    raw = Path(doc.raw_path)
    if not doc.raw_path or not raw.exists():
        totals.unresolved += 1
        totals.notes.append(f"нет байтов: {doc.url}")
        return None
    with raw.open("rb") as handle:
        head = handle.read(4)
    if head.startswith(PDF_MAGIC):
        if _hash_file(raw) != raw.name.removesuffix(".bin"):
            totals.raw_hash_mismatch += 1
            totals.notes.append(f"имя файла ≠ sha256 байтов: {doc.url}")
        totals.resolved += 1
        return raw
    member = _member_name(doc.url)
    if member is None or not zipfile.is_zipfile(raw):
        totals.not_pdf_magic += 1
        totals.notes.append(f"не PDF и не контейнер: {doc.url}")
        return None
    key = str(raw)
    if key not in container_hashes:
        container_hashes[key] = _hash_file(raw)
    if container_hashes[key] != raw.name.removesuffix(".bin"):
        totals.container_hash_mismatch += 1
        totals.notes.append(f"имя контейнера ≠ sha256 байтов: {raw.name}")
    try:
        with zipfile.ZipFile(raw) as archive:
            data = archive.read(member)
    except (KeyError, zipfile.BadZipFile, OSError) as exc:
        totals.unresolved += 1
        totals.notes.append(f"член не прочитан ({exc}): {doc.url}")
        return None
    if not data.startswith(PDF_MAGIC):
        totals.not_pdf_magic += 1
        totals.notes.append(f"член не PDF: {doc.url}")
        return None
    tmp_dir.mkdir(parents=True, exist_ok=True)
    target = tmp_dir / f"{hashlib.sha256(data).hexdigest()}.pdf"
    target.write_bytes(data)
    totals.resolved += 1
    return target


def _page_stats(path: Path, sparse_threshold: int) -> tuple[int, int, int, int, float]:
    """Обход страниц pdfium: (страниц, пустых, разряжённых, символов, секунд).

    Документ обрабатывается целиком и закрывается сразу: корпус в памяти не держится.
    """
    started = time.perf_counter()
    empty = 0
    sparse = 0
    chars = 0
    document = pdfium.PdfDocument(str(path))
    try:
        count = len(document)
        for index in range(count):
            textpage = document[index].get_textpage()
            try:
                text = textpage.get_text_range()
            finally:
                textpage.close()
            length = len(text.strip())
            chars += length
            if length == 0:
                empty += 1
            if length < sparse_threshold:
                sparse += 1
    finally:
        document.close()
    return count, empty, sparse, chars, time.perf_counter() - started


def _fmt_dist(name: str, dist: dict, unit: str) -> str:
    """Строка таблицы распределения."""
    if not dist.get("measured"):
        return f"{name:<22} не измерено"
    return (
        f"{name:<22} n={dist['n']:<5} "
        f"p50={dist['p50']:>9}{unit} p90={dist['p90']:>9}{unit} "
        f"p95={dist['p95']:>9}{unit} p99={dist['p99']:>10}{unit} "
        f"max={dist['max']:>10}{unit}"
    )


def _fmt_buckets(rows: list[dict]) -> list[str]:
    """Строки гистограммы: интервал, число, доля."""
    lines = []
    for row in rows:
        right = "∞" if row["to"] is None else str(row["to"])
        lines.append(
            f"  {row['from']:>6}–{right:<8} {row['count']:>6}  {row['share'] * 100:>5.1f} %"
        )
    return lines


def _build_report(
    db_path: Path,
    docs: list[DocRecord],
    totals: Totals,
    sparse_threshold: int,
    elapsed_s: float,
    pages_measured: bool,
) -> dict:
    """Сводка замера в машинном виде (её же печатает `--json`)."""
    pages = [doc.pages for doc in docs]
    chars = [doc.chars for doc in docs]
    report: dict = {
        "instrument": {
            "script": "scripts/measure_pdf_profile.py",
            "db": str(db_path),
            "pages_measured": pages_measured,
            "sparse_threshold": sparse_threshold,
            "percentile_method": "linear interpolation (type 7)",
            "sample": "сохранённые байты одного прогона; не весь интернет",
            "elapsed_s": round(elapsed_s, 1),
            "peak_rss_mb": peak_mb(),
        },
        "corpus": {
            "documents": len(docs),
            "pages": sum(pages),
            "chars": sum(chars),
            "products": sorted({doc.product_id for doc in docs}),
        },
        "integrity": {
            "resolved": totals.resolved,
            "unresolved": totals.unresolved,
            "not_pdf_magic": totals.not_pdf_magic,
            "raw_hash_mismatch": totals.raw_hash_mismatch,
            "container_hash_mismatch": totals.container_hash_mismatch,
            "page_count_mismatch": totals.page_count_mismatch,
            "failed": totals.failed,
            "notes": totals.notes[:20],
            "notes_total": len(totals.notes),
        },
        "pages": distribution(pages),
        "chars": distribution(chars),
        "pages_buckets": buckets(pages, PAGE_BUCKETS),
        "chars_buckets": buckets(chars, CHAR_BUCKETS),
        "caps": {
            "pages": cap_effect(docs, "pages", PAGE_CAPS),
            "chars": cap_effect(docs, "chars", CHAR_CAPS),
        },
        "top_pages": [
            {"url": doc.url, "product": doc.product_id, "pages": doc.pages, "chars": doc.chars}
            for doc in sorted(docs, key=lambda d: -d.pages)[:10]
        ],
        "top_chars": [
            {"url": doc.url, "product": doc.product_id, "pages": doc.pages, "chars": doc.chars}
            for doc in sorted(docs, key=lambda d: -d.chars)[:10]
        ],
    }
    measured = [doc for doc in docs if doc.pages_read is not None]
    if measured:
        empty = [doc.empty_pages or 0 for doc in measured]
        sparse = [doc.sparse_pages or 0 for doc in measured]
        read_s = [doc.read_s or 0.0 for doc in measured]
        report["empty_text_layer"] = {
            "documents_measured": len(measured),
            "pages_read": sum(doc.pages_read or 0 for doc in measured),
            "empty_pages": sum(empty),
            "empty_share": round(sum(empty) / max(1, sum(doc.pages_read or 0 for doc in measured)), 6),
            "sparse_pages": sum(sparse),
            "sparse_share": round(
                sum(sparse) / max(1, sum(doc.pages_read or 0 for doc in measured)), 6
            ),
            "documents_with_empty": sum(1 for value in empty if value),
            "documents_all_empty": sum(
                1 for doc in measured if doc.pages_read and doc.empty_pages == doc.pages_read
            ),
            "per_doc_empty_share": distribution(
                [round((doc.empty_pages or 0) / doc.pages_read, 6) for doc in measured if doc.pages_read],
                digits=4,
            ),
            "pages_distribution": distribution([doc.pages_read or 0 for doc in measured]),
        }
        report["read_time"] = {
            "note": "время pdfium (прибор скрипта), не время docling: из артефактов не восстанавливается",
            "per_doc_s": distribution(read_s, digits=3),
            "total_s": round(sum(read_s), 1),
            "pages_per_s": round(
                sum(doc.pages_read or 0 for doc in measured) / max(1e-9, sum(read_s)), 1
            ),
        }
    else:
        report["empty_text_layer"] = {"measured": False, "reason": "--no-pages"}
        report["read_time"] = {"measured": False, "reason": "--no-pages"}
    return report


def _print_report(report: dict) -> None:
    """Человекочитаемый отчёт: сначала границы прибора, потом числа."""
    inst = report["instrument"]
    corpus = report["corpus"]
    integrity = report["integrity"]
    print("=" * 78)
    print("ПРОФИЛЬ РЕСУРСОВ PDF-КОРПУСА (O31)")
    print("=" * 78)
    print(f"БД:            {inst['db']}")
    print(f"выборка:       {corpus['documents']} PDF-документов, "
          f"{corpus['pages']} страниц, {corpus['chars']} символов")
    print(f"продукты:      {', '.join(corpus['products'])}")
    print(f"границы:       {inst['sample']}")
    print("прибор:        pypdfium2 (слой текста), docling-числа — из артефакта прогона")
    print(f"время:         {inst['elapsed_s']} с, пик RSS {inst['peak_rss_mb']} MiB")
    print()
    print("целостность выборки:")
    print(f"  разрешено байтов      {integrity['resolved']}")
    print(f"  не разрешено          {integrity['unresolved']}")
    print(f"  не PDF по магии       {integrity['not_pdf_magic']}")
    print(f"  имя файла ≠ sha256    {integrity['raw_hash_mismatch']}")
    print(f"  имя контейнера ≠ sha256 {integrity['container_hash_mismatch']}")
    print(f"  страниц: файл ≠ БД    {integrity['page_count_mismatch']}")
    print()
    print("(а) страницы на документ (заявлено docling)")
    print("  " + _fmt_dist("pages", report["pages"], ""))
    print("  корзины:")
    print("\n".join(_fmt_buckets(report["pages_buckets"])))
    print()
    print("(б) символы текста на документ (извлечено docling)")
    print("  " + _fmt_dist("chars", report["chars"], ""))
    print("  корзины:")
    print("\n".join(_fmt_buckets(report["chars_buckets"])))
    print()
    empty = report["empty_text_layer"]
    print("(в) пустой текстовый слой (pdfium, сохранённые байты)")
    if empty.get("measured") is False:
        print(f"  не измерено: {empty.get('reason')}")
    else:
        print(f"  документов измерено   {empty['documents_measured']}")
        print(f"  страниц прочитано     {empty['pages_read']}")
        print(f"  пустых страниц        {empty['empty_pages']} "
              f"({empty['empty_share'] * 100:.2f} %)")
        print(f"  < {inst['sparse_threshold']} символов        {empty['sparse_pages']} "
              f"({empty['sparse_share'] * 100:.2f} %)")
        print(f"  документов с ≥1 пустой {empty['documents_with_empty']}")
        print(f"  документов целиком без слоя {empty['documents_all_empty']}")
        share = empty["per_doc_empty_share"]
        if share.get("measured"):
            print(f"  доля пустых на документ: p50={share['p50'] * 100:.1f} % "
                  f"p95={share['p95'] * 100:.1f} % p99={share['p99'] * 100:.1f} % "
                  f"max={share['max'] * 100:.1f} %")
    print()
    read_time = report["read_time"]
    print("(г) время извлечения")
    print("  из артефактов НЕ восстанавливается: в extract_cache и БД длительности нет,")
    print("  а claimed_at → state_changed_at включает ожидание в очереди.")
    if read_time.get("measured") is not False:
        per_doc = read_time["per_doc_s"]
        print(f"  справочно, pdfium-чтение (другой прибор): p50={per_doc['p50']} с "
              f"p95={per_doc['p95']} с p99={per_doc['p99']} с max={per_doc['max']} с, "
              f"{read_time['pages_per_s']} стр/с")
    print()
    print("хвост: 10 крупнейших по страницам")
    for item in report["top_pages"]:
        print(f"  {item['pages']:>5} стр {item['chars']:>9} симв  {item['url'][:90]}")
    print()
    print("хвост: 10 крупнейших по символам")
    for item in report["top_chars"]:
        print(f"  {item['chars']:>9} симв {item['pages']:>5} стр  {item['url'][:90]}")
    print()
    print("цена кандидатов max_pages (сколько выпало бы за границу)")
    for row in report["caps"]["pages"]:
        print(f"  cap={row['cap']:<6} док={row['documents_over']:<4} "
              f"({row['documents_share'] * 100:>4.1f} %) "
              f"страниц={row['value_over']:<7} ({row['value_share'] * 100:>5.1f} %) "
              f"срезано={row['value_trimmed']}")
    print()
    print("цена кандидатов max_extracted_chars")
    for row in report["caps"]["chars"]:
        print(f"  cap={row['cap']:<9} док={row['documents_over']:<4} "
              f"({row['documents_share'] * 100:>4.1f} %) "
              f"символов={row['value_over']:<9} ({row['value_share'] * 100:>5.1f} %) "
              f"срезано={row['value_trimmed']}")
    if integrity["notes"]:
        print()
        print("замечания прибора (первые):")
        for note in integrity["notes"][:10]:
            print(f"  - {note}")


def main(argv: list[str] | None = None) -> int:
    """Точка входа: собрать записи, обойти страницы, напечатать отчёт."""
    parser = argparse.ArgumentParser(
        description="Профиль ресурсов PDF-корпуса (O31): распределения и цена лимитов."
    )
    parser.add_argument(
        "--runs-dir",
        default=str(Path.home() / ".cohound" / "stands" / "ru-pg-dbms" / "runs"),
        help="каталог прогонов стенда (по умолчанию стенд ru-pg-dbms)",
    )
    parser.add_argument("--run-id", default=None, help="явный прогон; по умолчанию самый полный")
    parser.add_argument(
        "--db", default=None, help="прямой путь к cohound.db (перебивает --runs-dir/--run-id)"
    )
    parser.add_argument(
        "--product", action="append", default=None, help="ограничить продуктом (можно повторять)"
    )
    parser.add_argument("--limit", type=int, default=0, help="взять первые N документов (0 — все)")
    parser.add_argument(
        "--no-pages",
        action="store_true",
        help="только артефакты БД, без обхода страниц pdfium",
    )
    parser.add_argument(
        "--sparse-threshold",
        type=int,
        default=40,
        help="порог «разряжённой» страницы в символах (по умолчанию 40)",
    )
    parser.add_argument(
        "--tmp-dir",
        default="/tmp/opencode/pdf-profile",  # noqa: S108 - рабочий каталог замера, не секрет
        help="куда выписывать членов контейнеров",
    )
    parser.add_argument("--json", action="store_true", help="машинный вывод в stdout")
    parser.add_argument("--json-out", default=None, help="записать полный JSON (с per-doc) в файл")
    args = parser.parse_args(argv)

    db_path = Path(args.db) if args.db else _pick_run(Path(args.runs_dir), args.run_id)
    if not db_path.exists():
        print(f"нет БД прогона: {db_path}", file=sys.stderr)
        return 2
    products = set(args.product) if args.product else None
    docs = _load_docs(db_path, products)
    if args.limit:
        docs = docs[: args.limit]
    if not docs:
        print(f"PDF-документов не найдено в {db_path}", file=sys.stderr)
        return 2

    totals = Totals()
    container_hashes: dict[str, str] = {}
    tmp_dir = Path(args.tmp_dir)
    started = time.perf_counter()
    if not args.no_pages:
        for index, doc in enumerate(docs, 1):
            path = _materialize(doc, tmp_dir, totals, container_hashes)
            if path is None:
                doc.failure = "unresolved"
                totals.failed += 1
                continue
            try:
                count, empty, sparse, chars, read_s = _page_stats(path, args.sparse_threshold)
            except Exception as exc:  # noqa: BLE001 - сбой чтения обязан быть виден, а не уронить замер
                doc.failure = f"{type(exc).__name__}: {exc}"
                totals.failed += 1
                totals.notes.append(f"pdfium не открыл: {doc.url} ({doc.failure})")
                continue
            finally:
                if path != Path(doc.raw_path):
                    path.unlink(missing_ok=True)
            doc.pages_read = count
            doc.empty_pages = empty
            doc.sparse_pages = sparse
            doc.pdfium_chars = chars
            doc.read_s = read_s
            if count != doc.pages:
                totals.page_count_mismatch += 1
            if index % 200 == 0:
                print(
                    f"  … {index}/{len(docs)} документов, пик RSS {peak_mb()} MiB",
                    file=sys.stderr,
                )
    elapsed_s = time.perf_counter() - started

    report = _build_report(db_path, docs, totals, args.sparse_threshold, elapsed_s, not args.no_pages)
    if args.json_out:
        payload = dict(report)
        payload["per_doc"] = [
            {
                "doc_id": doc.doc_id,
                "product_id": doc.product_id,
                "url": doc.url,
                "content_hash": doc.content_hash,
                "pages": doc.pages,
                "chars": doc.chars,
                "pages_read": doc.pages_read,
                "empty_pages": doc.empty_pages,
                "sparse_pages": doc.sparse_pages,
                "pdfium_chars": doc.pdfium_chars,
                "read_s": round(doc.read_s, 4) if doc.read_s is not None else None,
                "failure": doc.failure,
            }
            for doc in docs
        ]
        Path(args.json_out).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        _print_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
