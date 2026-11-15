#!/usr/bin/env python3
"""Подготовка документного retrieval-эталона (O14 плана 26).

Что утверждает документный эталон: «для ячейки ``{product_id}/{param_id}`` документ,
процитированный в comparative table (``source_url``), должен попасть в top-K».
Утверждение не зависит от нарезки: документ адресуется URL-ом (устойчив к смене
чанкера и переэкстракции), а ``doc_id`` — это лишь его резолв **в конкретном
корпусе** (``doc_id = product_id:content_hash``, то есть меняется вместе с текстом).

Три подкоманды:

``build``
    Строит ``retrieval_golden_docs.candidate.csv``: одна строка на пару
    ``(product_id, source_url)`` из ``golden_00_cells.csv``. URL резолвится в документ
    корпуса **по БД прогона**, а не по похожести строк. Три машинных исхода:
    ``resolved`` (ровно один документ), ``ambiguous`` (несколько), ``missing`` (нет).
    Ничего не додумывается: ``missing`` остаётся ``missing``.

``compile``
    Превращает кандидатный CSV (после правки человеком) в JSON-эталон. Включаются
    только строки с ``decision=accept``; нерезолвленная строка с ``accept`` — отказ.

``validate``
    Проверяет готовый JSON против БД прогона: строгая схема, существование каждого
    ``doc_id``, принадлежность продукту ячейки (I17) и совпадение его URL с
    ``source_url`` ячейки. Нерезолвленный ``doc_id`` — отказ (код 2), не молчаливый
    пропуск.

``batches``
    Печатает пачки на подтверждение (``URL | документ | ячеек | статус``) по ~50 строк.

Почему скрипт, а не ``core/util/retrieval_eval.py``: это подготовка входа, а не
оценка. Формат ещё не принят пайплайном (файл-кандидат в пайплайн не подключён), а
``core/`` — категориально-нейтральный движок под гейтами mypy/нейтральности; класть
туда неподключённый формат рано. Когда формат будет принят, загрузчик переедет в
``core`` вместе с правкой контракта оценки.
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit, urlunsplit

GOLDEN_FORMAT = "cohound-retrieval-golden-docs/1"
BATCH_SIZE = 50

CANDIDATE_FIELDS = [
    "product_id",
    "param_ids",
    "cells",
    "source_url",
    "status",
    "match",
    "corpus_url",
    "doc_id",
    "decision",
    "note",
    # Живость адреса (проверка 09.11.2026, `2026-11-09-golden-url-liveness.json`).
    # Это **не** утверждение о релевантности: 301 говорит «страница переехала», 404 —
    # «её нет». Решение `accept`/`reject` всё равно за человеком: переехавшая редакция
    # (17_5 → 17_11) — тот же раздел, но другой текст.
    "live_kind",
    "live_code",
    "live_url",
]

#: Значения ``decision``, которые пишет человек. Пусто = ещё не рассмотрено.
DECISIONS = {"accept", "reject"}


# ---------------------------------------------------------------------------
# Нормализация URL
# ---------------------------------------------------------------------------
def canonical_url(url: str, *, drop_query: bool = False) -> str:
    """Каноническая форма URL для сопоставления с корпусом.

    Снимает только то, что не меняет документ по HTTP-семантике: регистр схемы и
    хоста, порт по умолчанию, фрагмент (якорь внутри страницы), повторные и хвостовой
    ``/``, percent-кодирование пути. ``http`` и ``https`` считаются одной формой:
    корпус пишет ``https``, comparative table иногда ``http``.

    ``drop_query`` — отдельный, более грубый уровень: query-строка может быть как
    подсказкой подсветки (``?h=...``), так и языковой редакцией (``?lang=ru``).
    Поэтому такой матч помечается ``canonical_query`` и уходит человеку на проверку.
    """
    parts = urlsplit(url.strip())
    host = (parts.hostname or "").lower()
    port = parts.port
    netloc = host if port in (None, 80, 443) else f"{host}:{port}"
    path = unquote(parts.path) or "/"
    while "//" in path:
        path = path.replace("//", "/")
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    query = "" if drop_query else parts.query
    return urlunsplit(("https", netloc, path, query, ""))


def host_key(url: str) -> str:
    """Хост без ``www.`` — так ``www.`` и голый хост считаются одной формой."""
    host = (urlsplit(url.strip()).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def root_matches(url: str, root: str) -> bool:
    """Лежит ли URL под объявленным корнем (хост + префикс пути по сегменту).

    Семантика та же, что у краула (AGENTS §6): поддомен не наследует право обхода,
    поэтому хост сравнивается точно (с эквивалентностью ``www.``), а префикс пути —
    только на границе сегмента.
    """
    if host_key(url) != host_key(root):
        return False
    root_path = urlsplit(root.strip()).path or "/"
    if not root_path.endswith("/"):
        root_path += "/"
    path = urlsplit(url.strip()).path or "/"
    if not path.endswith("/"):
        path += "/"
    return path == root_path or path.startswith(root_path)


# ---------------------------------------------------------------------------
# Чтение входов
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CellRow:
    product_id: str
    param_id: str
    source_url: str
    status: str


def read_cells(path: Path) -> list[CellRow]:
    """Ячейки с непустым ``source_url``; строки без URL в эталон не входят.

    Ячейка ``❌`` цитирует документ как свидетельство **отсутствия**; утверждение
    «этот документ релевантен» для неё неверно, поэтому в положительный эталон она не
    входит (её URL всё равно виден в отчёте). Ячейки ``✅``/``⚠️`` входят.
    """
    rows: list[CellRow] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            url = (row.get("source_url") or "").strip()
            if not url:
                continue
            rows.append(
                CellRow(
                    product_id=row["product_id"].strip(),
                    param_id=row["param_id"].strip(),
                    source_url=url,
                    status=(row.get("normalized_status") or "").strip(),
                )
            )
    return rows


class CorpusIndex:
    """Документы прогона, прочитанные точечно (``corpus_docs`` — тысячи строк)."""

    def __init__(self, db_path: Path) -> None:
        self._conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        self._conn.row_factory = sqlite3.Row
        self.exact: dict[tuple[str, str], list[str]] = defaultdict(list)
        self.canonical: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
        self.canonical_noquery: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(
            list
        )
        self._session_index: dict[tuple[str, str], sqlite3.Row] | None = None
        for row in self._conn.execute(
            "SELECT product_id, url, doc_id FROM corpus_docs"
        ):
            product_id = row["product_id"]
            url = row["url"]
            doc_id = row["doc_id"]
            self.exact[(product_id, url.strip())].append(doc_id)
            self.canonical[(product_id, canonical_url(url))].append((url, doc_id))
            self.canonical_noquery[(product_id, canonical_url(url, drop_query=True))].append(
                (url, doc_id)
            )

    def close(self) -> None:
        self._conn.close()

    # --- проверки для валидатора -------------------------------------------
    def doc_url(self, doc_id: str) -> str | None:
        row = self._conn.execute(
            "SELECT url FROM corpus_docs WHERE doc_id = ?", (doc_id,)
        ).fetchone()
        return None if row is None else str(row["url"])

    def doc_product(self, doc_id: str) -> str | None:
        row = self._conn.execute(
            "SELECT product_id FROM corpus_docs WHERE doc_id = ?", (doc_id,)
        ).fetchone()
        return None if row is None else str(row["product_id"])

    def declared_roots(self) -> dict[str, list[tuple[str, bool]]]:
        """Объявленные корни продукта и полнота их обхода (из покрытия прогона)."""
        roots: dict[str, list[tuple[str, bool]]] = defaultdict(list)
        for row in self._conn.execute(
            "SELECT product_id, roots_json FROM corpus_coverage"
        ):
            for root in json.loads(row["roots_json"] or "[]"):
                url = root.get("root_url")
                if url:
                    roots[row["product_id"]].append((url, bool(root.get("complete"))))
        return dict(roots)

    def truncated_products(self) -> dict[str, bool]:
        """Продукты, чей обход упёрся в бюджет (``corpus_coverage.truncated``)."""
        return {
            row["product_id"]: bool(row["truncated"])
            for row in self._conn.execute(
                "SELECT product_id, truncated FROM corpus_coverage"
            )
        }

    def unreadable_origins(self) -> dict[str, set[str]]:
        """Хосты, которые обход объявил недоступными (``unreadable_origins_json``)."""
        return {
            row["product_id"]: set(json.loads(row["unreadable_origins_json"] or "[]"))
            for row in self._conn.execute(
                "SELECT product_id, unreadable_origins_json FROM corpus_coverage"
            )
        }

    def policy_skips(self) -> dict[str, list[dict]]:
        """Причины пропуска по политике краула (из манифеста прогона)."""
        row = self._conn.execute(
            "SELECT crawl_policy_json FROM runs LIMIT 1"
        ).fetchone()
        if row is None or not row["crawl_policy_json"]:
            return {}
        payload = json.loads(row["crawl_policy_json"])
        skips: dict[str, list[dict]] = defaultdict(list)
        for skip in payload.get("policy_skips", []):
            skips[skip.get("product_id", "")].append(skip)
        return dict(skips)

    def session_item(self, product_id: str, url: str) -> sqlite3.Row | None:
        """Строка журнала краула для URL (по канону), если она есть.

        Журнал читается один раз и индексируется по канону: точечный запрос на каждый
        ``missing``-URL дал бы полный скан 6 192 строк × 257 раз.
        """
        if self._session_index is None:
            self._session_index = {}
            for row in self._conn.execute(
                "SELECT product_id, url, status, http_status, error_kind "
                "FROM crawl_session_items"
            ):
                key = (row["product_id"], canonical_url(row["url"]))
                self._session_index.setdefault(key, row)
        return self._session_index.get((product_id, canonical_url(url)))


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------
def _ambiguous_note(corpus: CorpusIndex, doc_ids: list[str]) -> str:
    """Кандидаты для ``ambiguous``: URL + doc_id, чтобы человек выбрал, а не гадал."""
    parts = []
    for doc_id in doc_ids:
        parts.append(f"{corpus.doc_url(doc_id)} ({doc_id})")
    return f"ambiguous: {len(doc_ids)} docs: " + " | ".join(parts)


def classify(
    corpus: CorpusIndex,
    roots: dict[str, list[tuple[str, bool]]],
    skips: dict[str, list[dict]],
    truncated: dict[str, bool],
    unreadable: dict[str, set[str]],
    product_id: str,
    url: str,
) -> tuple[str, str, str, str, str]:
    """Возвращает ``(status, match, corpus_url, doc_id, note)`` для пары продукт+URL.

    ``missing`` не маскируется: причина называется машинным токеном, но сам URL
    остаётся в списке на решение человека.
    """
    exact = corpus.exact.get((product_id, url), [])
    if len(exact) == 1:
        doc_url = corpus.doc_url(exact[0]) or url
        return "resolved", "exact", doc_url, exact[0], ""
    if len(exact) > 1:
        return "ambiguous", "exact", "", "", _ambiguous_note(corpus, exact)

    canonical = corpus.canonical.get((product_id, canonical_url(url)), [])
    if len(canonical) == 1:
        doc_url, doc_id = canonical[0]
        return "resolved", "canonical", doc_url, doc_id, ""
    if len(canonical) > 1:
        return "ambiguous", "canonical", "", "", _ambiguous_note(
            corpus, [doc_id for _, doc_id in canonical]
        )

    noquery = corpus.canonical_noquery.get(
        (product_id, canonical_url(url, drop_query=True)), []
    )
    if len(noquery) == 1:
        doc_url, doc_id = noquery[0]
        return "resolved", "canonical_query", doc_url, doc_id, "query differs"
    if len(noquery) > 1:
        return "ambiguous", "canonical_query", "", "", _ambiguous_note(
            corpus, [doc_id for _, doc_id in noquery]
        )

    # --- missing: назвать причину, не превращая её в догадку о документе ---
    item = corpus.session_item(product_id, url)
    if item is not None:
        detail = f"{item['status']}"
        if item["http_status"] is not None:
            detail += f"/http={item['http_status']}"
        if item["error_kind"]:
            detail += f"/{item['error_kind']}"
        return "missing", "", "", "", f"visited_no_doc:{detail}"

    for skip in skips.get(product_id, []):
        root = skip.get("root") or ""
        if root and root_matches(url, root):
            reasons = "+".join(skip.get("reasons") or [])
            return "missing", "", "", "", f"policy_skipped:{reasons}"

    if host_key(url) in unreadable.get(product_id, set()):
        return "missing", "", "", "", "unreadable_origin"

    matching = [
        (root, complete)
        for root, complete in roots.get(product_id, [])
        if root_matches(url, root)
    ]
    if matching:
        # Самый специфичный корень информативнее: `/docs/` говорит больше, чем `/`.
        root, complete = max(matching, key=lambda item: len(urlsplit(item[0]).path))
        note = (
            f"in_scope_not_reached:root={root};complete={int(complete)};"
            f"truncated={int(truncated.get(product_id, False))}"
        )
        return "missing", "", "", "", note

    return "missing", "", "", "", "out_of_scope"


def _load_liveness(path: str | None) -> dict[str, dict[str, object]]:
    """Живость адресов из файла замера: ``{url: {code, final_url, kind}}``.

    Файла нет — словарь пуст: ``build`` работает как раньше, а колонки ``live_*``
    остаются пустыми. Подставлять «живой» по умолчанию нельзя: это была бы догадка.
    """
    if not path:
        return {}
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise SystemExit(f"--liveness: ожидался объект URL→факт, получено {type(payload)}")
    return payload


def cmd_build(args: argparse.Namespace) -> int:
    cells = read_cells(Path(args.cells))
    liveness = _load_liveness(getattr(args, "liveness", None))
    grouped: dict[tuple[str, str], list[str]] = defaultdict(list)
    absence: dict[tuple[str, str], list[str]] = defaultdict(list)
    for cell in cells:
        bucket = absence if cell.status == "❌" else grouped
        bucket[(cell.product_id, cell.source_url)].append(cell.param_id)

    corpus = CorpusIndex(Path(args.run_db))
    try:
        roots = corpus.declared_roots()
        skips = corpus.policy_skips()
        truncated = corpus.truncated_products()
        unreadable = corpus.unreadable_origins()
        rows = []
        for (product_id, url), param_ids in sorted(grouped.items()):
            live = liveness.get(url) or {}
            status, match, corpus_url, doc_id, note = classify(
                corpus, roots, skips, truncated, unreadable, product_id, url
            )
            # Переехавший адрес: пробуем конечный. Это отдельный ``match``, чтобы
            # подмена адреса была видна, а не выглядела точным попаданием.
            live_url = str(live.get("final_url") or "")
            if status == "missing" and live_url and live_url != url:
                moved_status, moved_match, moved_doc_url, moved_doc_id, moved_note = (
                    classify(
                        corpus, roots, skips, truncated, unreadable, product_id, live_url
                    )
                )
                if moved_status in {"resolved", "ambiguous"}:
                    status = moved_status
                    match = f"moved_{moved_match or 'canonical'}"
                    corpus_url = moved_doc_url
                    doc_id = moved_doc_id
                    note = ";".join(
                        part for part in (f"moved_from={url}", moved_note) if part
                    )
            excluded = sorted(absence.get((product_id, url), []))
            if excluded:
                note = ";".join(
                    part
                    for part in (note, f"absence_cells_excluded={';'.join(excluded)}")
                    if part
                )
            rows.append(
                {
                    "product_id": product_id,
                    "param_ids": ";".join(sorted(param_ids)),
                    "cells": str(len(param_ids)),
                    "source_url": url,
                    "status": status,
                    "match": match,
                    "corpus_url": corpus_url,
                    "doc_id": doc_id,
                    "decision": "",
                    "note": note,
                    "live_kind": str(live.get("kind") or ""),
                    "live_code": (
                        "" if not live else str(live.get("code", ""))
                    ),
                    "live_url": live_url,
                }
            )
    finally:
        corpus.close()

    out = Path(args.out)
    with out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CANDIDATE_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    _print_summary(rows, cells, out)
    return 0


def _print_summary(rows: list[dict[str, str]], cells: list[CellRow], out: Path) -> None:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        key = row["status"]
        if row["status"] == "missing":
            key = f"missing:{row['note'].split(':', 1)[0].split(';', 1)[0]}"
        counts[key] += 1
    positive = sum(1 for cell in cells if cell.status != "❌")
    print(f"cells with source_url: {len(cells)} (positive {positive}, ❌ {len(cells) - positive})")
    print(f"unique (product, url) pairs: {len(rows)}")
    print(f"positive cells in pairs: {sum(int(row['cells']) for row in rows)}")
    for key in sorted(counts):
        print(f"  {key}: {counts[key]}")
    print(f"written: {out}")


# ---------------------------------------------------------------------------
# compile
# ---------------------------------------------------------------------------
def read_candidate(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [field for field in CANDIDATE_FIELDS if field not in (reader.fieldnames or [])]
        if missing:
            raise SystemExit(f"{path}: нет колонок {missing}")
        return list(reader)


def cmd_compile(args: argparse.Namespace) -> int:
    rows = read_candidate(Path(args.candidate))
    problems: list[str] = []
    entries: list[dict[str, object]] = []
    seen_cells: dict[str, int] = {}

    for lineno, row in enumerate(rows, start=2):
        decision = (row.get("decision") or "").strip()
        if decision not in DECISIONS:
            if decision:
                problems.append(f"строка {lineno}: неизвестное decision {decision!r}")
            continue
        if decision == "reject":
            continue
        # accept: строка обязана указывать документ — либо машинно разрешённый, либо
        # **вручную пришпиленный** человеком. Ручной пин нужен ровно там, где машина
        # честно не выбрала: ``ambiguous`` — два документа одного раздела (языковые
        # редакции ``?lang=en``/``?lang=ru-en``). Без него единственным записываемым
        # решением остаётся ``reject``, то есть ячейка выпадает из эталона из-за
        # ограничения прибора, а не из-за факта — а решение человека обязано быть
        # записываемым. Подмена не проходит молча: валидатор всё равно сверяет, что
        # ``doc_id`` резолвится в корпусе прогона, принадлежит продукту ячейки (I17) и
        # его URL совпадает с ``source_url`` (для ``canonical_query`` — без query).
        status = row.get("status")
        doc_id = (row.get("doc_id") or "").strip()
        if status != "resolved" and not (status == "ambiguous" and doc_id):
            problems.append(
                f"строка {lineno}: decision=accept при status={status!r} "
                f"({row.get('source_url')})"
                + (
                    " — у ambiguous впишите doc_id выбранного документа в колонку doc_id"
                    if status == "ambiguous"
                    else ""
                )
            )
            continue
        if not doc_id:
            problems.append(f"строка {lineno}: decision=accept без doc_id")
            continue
        for param_id in (row.get("param_ids") or "").split(";"):
            param_id = param_id.strip()
            if not param_id:
                continue
            cell = f"{row['product_id']}/{param_id}"
            if cell in seen_cells:
                problems.append(
                    f"строка {lineno}: ячейка {cell} уже объявлена строкой {seen_cells[cell]}"
                )
                continue
            seen_cells[cell] = lineno
            entries.append(
                {
                    "cell": cell,
                    "product_id": row["product_id"],
                    "param_id": param_id,
                    "source_url": row["source_url"],
                    "expected_doc_ids": [doc_id],
                    "match": row.get("match") or "",
                }
            )

    if problems:
        for problem in problems:
            print(f"ОТКАЗ: {problem}", file=sys.stderr)
        return 2
    if not entries:
        print(
            "ОТКАЗ: ни одна строка не помечена decision=accept — эталон пуст",
            file=sys.stderr,
        )
        return 2

    entries.sort(key=lambda item: (str(item["product_id"]), str(item["param_id"])))
    payload = {
        "format": GOLDEN_FORMAT,
        "case_id": args.case_id,
        "source": args.source,
        "corpus_run_id": args.run_id,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "entries": entries,
    }
    out = Path(args.out)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"written: {out} ({len(entries)} entries)")

    # Компиляция без проверки бессмысленна: сразу гоним валидатор.
    args.golden = str(out)
    return cmd_validate(args)


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------
def _check_schema(payload: object) -> tuple[list[str], list[dict]]:
    problems: list[str] = []
    if not isinstance(payload, dict):
        return ["верхний уровень — не объект"], []
    for field in ("format", "case_id", "source", "corpus_run_id", "entries"):
        if field not in payload:
            problems.append(f"нет поля {field}")
    if payload.get("format") != GOLDEN_FORMAT:
        problems.append(f"format={payload.get('format')!r}, ожидается {GOLDEN_FORMAT!r}")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        problems.append("entries — не список")
        return problems, []
    expected_keys = {
        "cell",
        "product_id",
        "param_id",
        "source_url",
        "expected_doc_ids",
        "match",
    }
    for index, entry in enumerate(entries):
        where = f"entries[{index}]"
        if not isinstance(entry, dict):
            problems.append(f"{where}: не объект")
            continue
        extra = set(entry) - expected_keys
        absent = expected_keys - set(entry)
        if extra:
            problems.append(f"{where}: лишние поля {sorted(extra)}")
        if absent:
            problems.append(f"{where}: нет полей {sorted(absent)}")
            continue
        doc_ids = entry["expected_doc_ids"]
        if not isinstance(doc_ids, list) or not doc_ids:
            problems.append(f"{where}: expected_doc_ids пуст")
        for field in ("cell", "product_id", "param_id", "source_url"):
            if not isinstance(entry[field], str) or not entry[field].strip():
                problems.append(f"{where}: {field} пуст")
    return problems, entries


def validate_payload(payload: object, corpus: CorpusIndex) -> list[str]:
    problems, entries = _check_schema(payload)
    if problems:
        return problems
    seen: dict[str, int] = {}
    for index, entry in enumerate(entries):
        where = f"entries[{index}] {entry.get('cell')}"
        cell = entry["cell"]
        if cell in seen:
            problems.append(f"{where}: дубль ячейки (первая — entries[{seen[cell]}])")
        seen[cell] = index
        expected_cell = f"{entry['product_id']}/{entry['param_id']}"
        if cell != expected_cell:
            problems.append(f"{where}: cell не совпадает с {expected_cell}")
        for doc_id in entry["expected_doc_ids"]:
            if not isinstance(doc_id, str) or not doc_id:
                problems.append(f"{where}: пустой doc_id")
                continue
            doc_url = corpus.doc_url(doc_id)
            if doc_url is None:
                problems.append(
                    f"{where}: doc_id {doc_id} не резолвится в corpus_docs "
                    f"(прогон {entry.get('corpus_run_id', '?')})"
                )
                continue
            product = corpus.doc_product(doc_id)
            if product != entry["product_id"]:
                problems.append(
                    f"{where}: doc_id {doc_id} принадлежит {product!r}, "
                    f"ячейка — {entry['product_id']!r} (I17)"
                )
            source = canonical_url(entry["source_url"])
            actual = canonical_url(doc_url)
            if entry.get("match") == "canonical_query":
                source = canonical_url(entry["source_url"], drop_query=True)
                actual = canonical_url(doc_url, drop_query=True)
            if actual != source:
                problems.append(
                    f"{where}: URL документа {doc_url} не совпадает с source_url "
                    f"{entry['source_url']} (match={entry.get('match')!r})"
                )
    return problems


def cmd_validate(args: argparse.Namespace) -> int:
    golden = Path(args.golden)
    payload = json.loads(golden.read_text(encoding="utf-8"))
    corpus = CorpusIndex(Path(args.run_db))
    try:
        problems = validate_payload(payload, corpus)
    finally:
        corpus.close()
    if problems:
        for problem in problems:
            print(f"ОТКАЗ: {problem}", file=sys.stderr)
        print(f"{golden}: {len(problems)} проблем(ы)", file=sys.stderr)
        return 2
    entries = payload["entries"]
    print(f"OK: {golden}: {len(entries)} записей, все doc_id резолвятся")
    return 0


# ---------------------------------------------------------------------------
# batches
# ---------------------------------------------------------------------------
def _batch_status(row: dict[str, str]) -> str:
    status = row["status"]
    note = (row.get("note") or "").strip()
    if status == "resolved":
        return "resolved" if row.get("match") == "exact" else f"resolved ({row['match']})"
    if status == "missing" and note:
        reason = note.split(":", 1)[0].split(";", 1)[0]
        return f"missing ({reason})"
    return status


def _batch_live(row: dict[str, str]) -> str:
    """Живость адреса из ячейки — короткой пометкой для человека.

    Колонка нужна, чтобы разметчик не сверялся с 54-килобайтным JSON: решение
    ``accept``/``reject`` зависит от того, жив ли адрес, и «переехал» от «умер»
    отличается последствием. ``moved`` называет конечный адрес: подмена адреса должна
    быть видна в таблице, а не только в CSV.
    """
    kind = (row.get("live_kind") or "").strip()
    if not kind:
        return "—"
    code = (row.get("live_code") or "").strip()
    label = f"{kind} {code}".strip() if code else kind
    final = (row.get("live_url") or "").strip()
    if kind == "moved" and final and final != row["source_url"]:
        return f"{label} → {final}"
    return label


def cmd_batches(args: argparse.Namespace) -> int:
    rows = read_candidate(Path(args.candidate))
    by_product: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_product[row["product_id"]].append(row)
    for product_id in sorted(by_product):
        product_rows = sorted(by_product[product_id], key=lambda r: r["source_url"])
        for start in range(0, len(product_rows), BATCH_SIZE):
            batch = product_rows[start : start + BATCH_SIZE]
            print(f"### {product_id} — пачка {start // BATCH_SIZE + 1} "
                  f"({len(batch)} строк)")
            print()
            print("| URL из ячейки | адрес | документ в корпусе | ячеек | статус |")
            print("|---|---|---|---|---|")
            for row in batch:
                doc = row.get("corpus_url") or "—"
                print(
                    f"| {row['source_url']} | {_batch_live(row)} | {doc} | "
                    f"{row['cells']} | {_batch_status(row)} |"
                )
            print()
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="URL → документ по БД прогона")
    build.add_argument("--cells", required=True)
    build.add_argument("--run-db", required=True)
    build.add_argument("--out", required=True)
    build.add_argument(
        "--liveness",
        default=None,
        help="файл замера живости адресов (URL→{code,final_url,kind}); "
        "без него колонки live_* пусты",
    )
    build.set_defaults(func=cmd_build)

    compile_ = sub.add_parser("compile", help="кандидатный CSV → JSON-эталон")
    compile_.add_argument("--candidate", required=True)
    compile_.add_argument("--out", required=True)
    compile_.add_argument("--run-db", required=True)
    compile_.add_argument("--case-id", default="ru-pg-dbms")
    compile_.add_argument(
        "--source", default="packages/ru-pg-dbms/sources/golden_00_cells.csv"
    )
    compile_.add_argument("--run-id", default="")
    compile_.set_defaults(func=cmd_compile)

    validate = sub.add_parser("validate", help="проверить JSON-эталон против БД")
    validate.add_argument("--golden", required=True)
    validate.add_argument("--run-db", required=True)
    validate.set_defaults(func=cmd_validate)

    batches = sub.add_parser("batches", help="пачки на подтверждение (markdown)")
    batches.add_argument("--candidate", required=True)
    batches.set_defaults(func=cmd_batches)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
