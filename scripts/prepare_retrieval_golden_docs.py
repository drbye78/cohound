#!/usr/bin/env python3
"""Подготовка документного retrieval-эталона (O14 плана 26).

Что утверждает документный эталон: «для ячейки ``{product_id}/{param_id}`` документ,
процитированный в comparative table (``source_url``), должен попасть в top-K».
Утверждение не зависит от нарезки: документ адресуется URL-ом (устойчив к смене
чанкера и переэкстракции), а ``doc_id`` — это лишь его резолв **в конкретном
корпусе** (``doc_id = product_id:content_hash``, то есть меняется вместе с текстом).

Подкоманды:

``build``
    Строит машинный отчёт по всем парам ``(product_id, source_url)`` из
    ``golden_00_cells.csv`` (CSV: одна строка на пару). URL резолвится в документ
    корпуса **по БД прогона**, а не по похожести строк. Три машинных исхода:
    ``resolved`` (ровно один документ), ``ambiguous`` (несколько), ``missing`` (нет).
    Ничего не додумывается: ``missing`` остаётся ``missing``.

``annotate``
    Пересобирает **лист на разметку** — тот, в котором работает человек. Выход по
    расширению: ``.xlsx`` (шапка с фильтром, выпадающий список решений, залитые строки,
    где решение не механическое, второй лист с инструкцией) или CSV. Дописывает
    подсказки: ``titles``/``claims`` (что утверждает ячейка) и ``cited_from`` (откуда
    цитата фактически взята). Решения человека из прежнего листа переносятся.

``compile``
    Превращает лист (XLSX или CSV — формат по расширению) после правки человеком в
    JSON-эталон. Включаются только строки с ``decision=accept``; нерезолвленная строка
    с ``accept`` — отказ.

``validate``
    Проверяет готовый JSON против БД прогона: строгая схема, существование каждого
    ``doc_id``, принадлежность продукту ячейки (I17) и совпадение его URL с
    ``source_url`` ячейки. Нерезолвленный ``doc_id`` — отказ (код 2), не молчаливый
    пропуск.

``batches``
    Печатает пачки на подтверждение (``URL | документ | что утверждается | цитата
    фактически из | ячеек | статус``) по ~50 строк — тот же лист, но читаемый в
    терминале и в переписке.

Почему скрипт, а не ``core/util/retrieval_eval.py``: это подготовка входа, а не
оценка. Формат ещё не принят пайплайном (файл-кандидат в пайплайн не подключён), а
``core/`` — категориально-нейтральный движок под гейтами mypy/нейтральности; класть
туда неподключённый формат рано. Когда формат будет принят, загрузчик переедет в
``core`` вместе с правкой контракта оценки.

Исключение — **правило «тот же документ»**: канонизация URL и индексы документов
корпуса живут в :mod:`core.pipeline.doc_refs` (``DocIndex``). Измерение документного
recall@K обязано резолвить URL тем же правилом, что и эталон; две копии канонизации
разъехались бы молча и дали бы два разных ответа на один вопрос.
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
from urllib.parse import urlsplit

# Корень репозитория на sys.path: скрипт запускается как `python scripts/…py`, и тогда
# sys.path[0] — каталог scripts, а не корень (плоская раскладка пакета `core`).
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.pipeline.doc_refs import (  # noqa: E402 — после правки sys.path
    DocIndex,
    canonical_url,
    host_key,
    root_matches,
)

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

#: Колонки-подсказки для разметчика (`annotate`): что именно утверждает ячейка.
#: Не вход `compile` — это чтение для человека, а не решение; `compile` их игнорирует.
ANNOTATION_FIELDS = ["titles", "claims", "cited_from"]

#: Колонки, которые заполняет **человек**. `comment` отделена от `note` намеренно:
#: `note` машинная (для `moved` там `moved_from=…`, для `ambiguous` — два `doc_id`), и
#: пересборка листа её перезаписывает. Одна колонка на два владельца означала бы, что
#: комментарий разметчика исчезает при первом же `annotate`.
HUMAN_FIELDS = ["decision", "comment"]

#: Значения ``decision``, которые пишет человек. Пусто = ещё не рассмотрено.
DECISIONS = {"accept", "reject"}


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
    """Документы прогона, прочитанные точечно (``corpus_docs`` — тысячи строк).

    Разрешение «URL → документ» — правило :class:`~core.pipeline.doc_refs.DocIndex`;
    здесь остаётся то, что делает именно подготовка эталона: покрытие, журнал краула и
    политика. Публичные имена индексов документов сохранены прежними (``exact``,
    ``canonical``, ``canonical_noquery``, ``doc_url``, ``doc_product``), чтобы остальной
    скрипт не знал, где живёт правило.
    """

    def __init__(self, db_path: Path) -> None:
        self._conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        self._conn.row_factory = sqlite3.Row
        self.docs = DocIndex(self._conn)
        self._session_index: dict[tuple[str, str], sqlite3.Row] | None = None

    def close(self) -> None:
        self._conn.close()

    # --- документы и их URL (правило — в core) -------------------------------
    @property
    def exact(self) -> dict[tuple[str, str], list[str]]:
        return self.docs.exact

    @property
    def canonical(self) -> dict[tuple[str, str], list[tuple[str, str]]]:
        return self.docs.canonical

    @property
    def canonical_noquery(self) -> dict[tuple[str, str], list[tuple[str, str]]]:
        return self.docs.canonical_noquery

    # --- проверки для валидатора -------------------------------------------
    def doc_url(self, doc_id: str) -> str | None:
        return self.docs.doc_url(doc_id)

    def doc_product(self, doc_id: str) -> str | None:
        return self.docs.doc_product(doc_id)

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
#: Колонки листа в порядке чтения: сначала то, по чему принимается решение (адреса и
#: уровень совпадения), затем две человеческие колонки рядом, затем машинный `note` и
#: подсказки. `compile` читает по именам, порядок нужен только глазам.
ALL_FIELDS = [
    *CANDIDATE_FIELDS[: CANDIDATE_FIELDS.index("decision")],
    "decision",
    "comment",
    *CANDIDATE_FIELDS[CANDIDATE_FIELDS.index("decision") + 1 :],
    *ANNOTATION_FIELDS,
]


def read_candidate(path: Path) -> list[dict[str, str]]:
    """Лист на разметку → строки. Формат — по расширению: ``.xlsx`` или CSV.

    XLSX — рабочий формат разметчика (Excel: выпадающий список решений, фильтр,
    закреплённая шапка), CSV — формат обмена и диффа в git. Оба читаются здесь, чтобы
    `compile` и `batches` не знали, в чём именно размечали: иначе выбор формата протекал
    бы в валидатор и в отчёт.
    """
    if path.suffix.lower() == ".xlsx":
        return _read_xlsx(path)
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [field for field in CANDIDATE_FIELDS if field not in (reader.fieldnames or [])]
        if missing:
            raise SystemExit(f"{path}: нет колонок {missing}")
        return list(reader)


def _openpyxl() -> object:
    """`openpyxl` — зависимость extra `extract` (её тянет Office-путь docling).

    Импорт ленивый и с внятным отказом: скрипт обязан работать в окружении без extra
    `extract` (CSV-путь), а не падать на импорте при запуске `build`/`validate`.
    """
    try:
        import openpyxl
    except ImportError as error:  # pragma: no cover - зависит от окружения
        raise SystemExit(
            "XLSX требует openpyxl (extra `extract`: uv sync --extra extract); "
            "без него используйте CSV"
        ) from error
    return openpyxl


def _read_xlsx(path: Path) -> list[dict[str, str]]:
    """XLSX → строки листа. Первая строка — шапка, значения читаются как текст.

    Числа и пустые ячейки приводятся к строке: `compile` сравнивает `decision` со
    множеством значений и не должен зависеть от того, как Excel сохранил ячейку.
    """
    openpyxl = _openpyxl()
    book = openpyxl.load_workbook(path, read_only=True, data_only=True)  # type: ignore[attr-defined]
    try:
        sheet = book.worksheets[0]
        rows = sheet.iter_rows(values_only=True)
        try:
            header = [("" if cell is None else str(cell)).strip() for cell in next(rows)]
        except StopIteration:
            raise SystemExit(f"{path}: пустой лист") from None
        missing = [field for field in CANDIDATE_FIELDS if field not in header]
        if missing:
            raise SystemExit(f"{path}: нет колонок {missing}")
        out: list[dict[str, str]] = []
        for values in rows:
            if not any(value not in (None, "") for value in values):
                continue
            row = {name: "" for name in header if name}
            for index, name in enumerate(header):
                if not name or index >= len(values):
                    continue
                value = values[index]
                row[name] = "" if value is None else str(value).strip()
            out.append(row)
        return out
    finally:
        book.close()


def _write_xlsx(path: Path, rows: list[dict[str, str]]) -> None:
    """Строки листа → XLSX, пригодный для разметки руками.

    Что делает файл рабочим, а не «CSV в обёртке»:

    * шапка закреплена и с фильтром — 102 строки иначе листаются вслепую;
    * колонка `decision` — выпадающий список ``accept``/``reject``: значение нельзя
      напечатать с опечаткой, а `compile` отказывает именно на неизвестном решении;
    * строки, где решение **не** механическое (адрес переехал, документ неоднозначен,
      совпадение не точное), залиты цветом — это те 14 строк, которые требуют человека;
    * `titles`/`claims`/`cited_from` переносятся по словам и имеют ширину: иначе
      подсказка обрезана и разметка превращается в угадывание.

    Второй лист — инструкция: что означает `accept` и что делать с переехавшими
    адресами. Она в файле, а не в переписке, потому что файл живёт дольше переписки.
    """
    openpyxl = _openpyxl()
    from openpyxl.styles import Alignment, Font, PatternFill  # noqa: PLC0415 - ленивый импорт
    from openpyxl.worksheet.datavalidation import DataValidation  # noqa: PLC0415

    book = openpyxl.Workbook()  # type: ignore[attr-defined]
    sheet = book.active
    sheet.title = "лист"
    sheet.append(ALL_FIELDS)
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = f"A1:{_column_letter(len(ALL_FIELDS))}1"

    # Ширины по назначению колонки, а не «по умолчанию»: адреса длинные, решения короткие.
    widths = {
        "product_id": 18,
        "param_ids": 40,
        "cells": 7,
        "source_url": 55,
        "status": 11,
        "match": 17,
        "corpus_url": 55,
        "doc_id": 30,
        "decision": 12,
        "comment": 34,
        "note": 30,
        "live_kind": 10,
        "live_code": 10,
        "live_url": 55,
        "titles": 45,
        "claims": 30,
        "cited_from": 55,
    }
    for index, name in enumerate(ALL_FIELDS, start=1):
        sheet.column_dimensions[_column_letter(index)].width = widths.get(name, 16)
    wrap = {"param_ids", "titles", "claims", "cited_from", "note", "comment", "doc_id"}
    decision_index = ALL_FIELDS.index("decision") + 1

    needs_human = PatternFill("solid", start_color="FFF2CC")
    for row_index, row in enumerate(rows, start=2):
        sheet.append([row.get(name, "") for name in ALL_FIELDS])
        for index, name in enumerate(ALL_FIELDS, start=1):
            if name in wrap:
                sheet.cell(row=row_index, column=index).alignment = Alignment(
                    wrap_text=True, vertical="top"
                )
        if _needs_human(row):
            for index in range(1, len(ALL_FIELDS) + 1):
                sheet.cell(row=row_index, column=index).fill = needs_human

    validation = DataValidation(
        type="list",
        formula1=f'"{",".join(sorted(DECISIONS))}"',
        allow_blank=True,
        showDropDown=False,
    )
    validation.error = "решение — accept или reject; пусто = ещё не рассмотрено"
    validation.errorTitle = "Неизвестное решение"
    sheet.add_data_validation(validation)
    last = max(2, len(rows) + 1)
    validation.add(
        f"{_column_letter(decision_index)}2:{_column_letter(decision_index)}{last}"
    )

    guide = book.create_sheet("как размечать")
    for line in _GUIDE_LINES:
        guide.append([line])
    guide.column_dimensions["A"].width = 110
    for cell in guide["A"]:
        cell.alignment = Alignment(wrap_text=True, vertical="top")

    book.save(path)


#: Что означает решение. Лежит вторым листом книги: файл переживает переписку.
_GUIDE_LINES: tuple[str, ...] = (
    "Размечается ОДНА колонка — decision: accept или reject (выпадающий список). "
    "Пусто = ещё не рассмотрено. Рядом колонка comment — для ваших пометок; "
    "note — машинная (её пишет build), не правьте её: при пересборке листа она "
    "пересчитывается, а при правке на месте остаётся как была.",
    "",
    "accept значит: документ корпуса (corpus_url) — это тот самый документ, на который "
    "ссылается ячейка таблицы (source_url), и он должен попасть в top-K. Это утверждение "
    "про адрес источника, а не про то, нравится ли страница.",
    "",
    "titles/claims — что утверждает ячейка (параметр и записанное значение).",
    "cited_from — откуда цитата фактически взята в прогоне. Совпадения с corpus_url "
    "меньшинство: доказательство часто лежит в другом документе. Это справка; решение "
    "принимается по corpus_url.",
    "live_kind — живость адреса на дату замера: live / moved (переехал) / gone / auth.",
    "",
    "Залитые строки — те, где решение не механическое:",
    "  moved — адрес переехал. Тот же раздел, но другой текст: решает человек.",
    "  canonical_query (ambiguous) — две редакции одного раздела. При accept впишите в "
    "колонку doc_id один из двух (они в колонке note).",
    "  canonical — совпало после канонизации (фрагмент #..., хвостовой слэш, http/https, "
    "%кодирование). Обычно accept; reject — если фрагмент указывает на другой раздел.",
    "",
    "Дальше — одна команда (compile сам прогонит валидатор и откажет на нерезолвленном "
    "doc_id, чужом продукте и несовпавшем URL).",
)


def _column_letter(index: int) -> str:
    """1 → ``A``, 27 → ``AA`` (openpyxl такой утилиты в публичном API не даёт)."""
    letters = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


def _needs_human(row: dict[str, str]) -> bool:
    """Решение не механическое: переезд адреса, неоднозначность, неточное совпадение."""
    if (row.get("status") or "") != "resolved":
        return True
    if (row.get("live_kind") or "") == "moved":
        return True
    return (row.get("match") or "") != "exact"


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


def _batch_cited(row: dict[str, str]) -> str:
    """Откуда фактически взята цитата: ``= объявлено``, иначе адрес(а) или «—».

    ``= объявлено`` печатается вместо повторения адреса: совпадение — это факт, и он
    должен читаться мгновенно. Несколько адресов — через ``<br>``, иначе markdown-строка
    распухает и таблица перестаёт читаться.
    """
    cited = (row.get("cited_from") or "").strip()
    if not cited:
        return "—"
    if cited == (row.get("corpus_url") or "").strip():
        return "= объявлено"
    return "<br>".join(part.strip() for part in cited.split(" ; ") if part.strip()).replace(
        "|", "\\|"
    )


def _batch_claim(row: dict[str, str]) -> str:
    """Утверждение ячейки для таблицы: «параметр: значение», иначе «—».

    Печатается только когда аннотация есть (`annotate`); вертикальная черта внутри
    текста заменяется, иначе она разрежет markdown-таблицу.
    """
    titles = (row.get("titles") or "").strip()
    claims = (row.get("claims") or "").strip()
    if not titles and not claims:
        return "—"
    text = f"{titles}: {claims}" if titles and claims else (titles or claims)
    return text.replace("|", "\\|")


def cmd_annotate(args: argparse.Namespace) -> int:
    """Дописывает в лист утверждение ячейки: ``titles`` и ``claims``.

    Зачем. В листе на разметку были только адреса: разметчик видел URL и документ
    корпуса, но не видел, **что** утверждается цитатой, — а решение ``accept`` означает
    именно «утверждение этой ячейки взято с этого документа». Без текста утверждения
    сверять нечего, и разметка превращается в сверку похожести адресов.

    Откуда берётся. ``golden_00_cells.csv`` — исходная comparative table: ``excel_title``
    («что за параметр») и ``raw_value`` («что записано в ячейке»). Присоединение — по
    ``(product_id, param_id)``, по **всем** ``param_ids`` строки (страница цитируется
    несколькими ячейками, и решение принимается по всем сразу).

    Чего не делает. Не решает и не додумывает: ячейка, которой нет в исходной таблице,
    остаётся с пустыми колонками. Многострочный текст сводится к одной строке и
    обрезается по ``MAX_ANNOTATION_CHARS`` — колонка читается глазами в таблице, а полный
    текст лежит в исходном CSV.
    """
    rows = read_candidate(Path(args.candidate))
    # Строки `missing` не размечаются: документа нет в корпусе, и решение по ним было бы
    # решением о покрытии, а не об эталоне. Раньше это отсекалось руками — то есть
    # правило жило в переписке, а не в приборе. `--keep-missing` возвращает их, если
    # нужен полный машинный отчёт в том же формате.
    total = len(rows)
    if not args.keep_missing:
        rows = [row for row in rows if (row.get("status") or "") != "missing"]
    cells = _read_cells(Path(args.cells))
    cited = _read_citations(Path(args.run_db)) if args.run_db else {}
    missing: list[str] = []
    for row in rows:
        titles: list[str] = []
        claims: list[str] = []
        for param_id in (row.get("param_ids") or "").split(";"):
            param_id = param_id.strip()
            if not param_id:
                continue
            cell = cells.get((row["product_id"], param_id))
            if cell is None:
                # Имя, а не только счётчик: «1 пара не нашлась» не говорит, какую править.
                missing.append(f"{row['product_id']}/{param_id}")
                continue
            titles.append(_one_line(cell.get("excel_title") or ""))
            claims.append(_one_line(cell.get("raw_value") or ""))
        row["titles"] = " | ".join(t for t in titles if t)
        row["claims"] = " | ".join(c for c in claims if c)
        urls: list[str] = []
        for param_id in (row.get("param_ids") or "").split(";"):
            for url in cited.get((row["product_id"], param_id.strip()), []):
                if url not in urls:
                    urls.append(url)
        row["cited_from"] = " ; ".join(urls)
    out = Path(args.out)
    carried = _carry_decisions(out, rows)
    if out.suffix.lower() == ".xlsx":
        _write_xlsx(out, rows)
    else:
        fields = list(ALL_FIELDS)
        with out.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key, "") for key in fields})
    print(f"written: {out} ({len(rows)} строк, колонки {', '.join(ANNOTATION_FIELDS)})")
    if len(rows) != total:
        print(
            f"отброшено {total - len(rows)} строк status=missing — документов нет в корпусе; "
            "это провал покрытия, а не эталона (вернуть: --keep-missing)"
        )
    if carried:
        print(f"сохранено решений из прежнего листа: {carried} (decision/comment)")
    if args.run_db:
        with_citation = sum(1 for row in rows if row.get("cited_from"))
        print(f"цитата есть у {with_citation} из {len(rows)} строк (адрес — в cited_from)")
    if missing:
        print(
            f"ВНИМАНИЕ: {len(missing)} пар (product, param_id) нет в исходной таблице — "
            "колонки-подсказки у них пусты: " + ", ".join(missing),
            file=sys.stderr,
        )
    return 0


#: Потолок длины подсказки в листе: колонка читается глазами, полный текст — в исходном CSV.
MAX_ANNOTATION_CHARS = 160


def _carry_decisions(out: Path, rows: list[dict[str, str]]) -> int:
    """Переносит решения человека из прежнего листа в новый; возвращает их число.

    Зачем. `annotate` пересобирает подсказки после смены корпуса или прибора, и без
    переноса повторный запуск стёр бы разметку — работу, которую машина не повторяет.
    Ключ — ``(product_id, source_url)``: он же единица листа. Строка, которой в новом
    листе нет, просто не переносится (её документ выпал из корпуса) — и это видно по
    счётчику, а не молча.

    Переносится ``decision`` и ``comment``. ``doc_id`` — только у строк, где машина его не
    выбрала (``status != resolved``): там его вписал человек (выбор редакции при
    ``ambiguous``), и потерять его значит потерять решение. У ``resolved`` значение
    пересчитывается `build` заново, и перенос старого закрепил бы устаревший адрес.

    ``note`` здесь **не** трогается: он машинный, и его источник — `build`. При правке
    листа на месте (кандидат = выход) он просто читается и записывается обратно; при
    пересборке из свежего `build` он пересчитывается вместе со всей строкой. Поэтому
    правило для разметчика — «`note` не править», а не «`annotate` его обновит».
    """
    if not out.exists():
        return 0
    try:
        previous = read_candidate(out)
    except SystemExit:
        return 0
    by_key = {(row["product_id"], row["source_url"]): row for row in previous}
    carried = 0
    for row in rows:
        old = by_key.get((row["product_id"], row["source_url"]))
        if old is None:
            continue
        for field in HUMAN_FIELDS:
            value = (old.get(field) or "").strip()
            if value:
                row[field] = value
        if (old.get("status") or "") != "resolved":
            pinned = (old.get("doc_id") or "").strip()
            if pinned:
                row["doc_id"] = pinned
        if (row.get("decision") or "").strip() or (row.get("comment") or "").strip():
            carried += 1
    return carried


def _one_line(text: str) -> str:
    """Многострочный текст ячейки → одна строка, обрезанная по ``MAX_ANNOTATION_CHARS``."""
    collapsed = " ".join(str(text).split())
    if len(collapsed) <= MAX_ANNOTATION_CHARS:
        return collapsed
    return collapsed[: MAX_ANNOTATION_CHARS - 1] + "…"


def _read_citations(run_db: Path) -> dict[tuple[str, str], list[str]]:
    """``(product_id, param_id)`` → адреса документов, из которых **фактически** взята цитата.

    Зачем это в листе. Эталон объявляет один документ на ячейку — `source_url` из
    comparative table. Оценщик же берёт цитату из любого документа корпуса, и адрес
    цитаты виден в `cell_evidence` → `corpus_chunks` (там `content_hash` **чанка**;
    `corpus_docs.content_hash` не подходит — это хеш документа, и join по нему даёт ноль).

    Расхождение — не дефект: ячейка таблицы и документ корпуса адресуют одно и то же
    утверждение по-разному, и разметчику нужно видеть оба, иначе `accept` превращается в
    сверку похожести адресов. Само решение всё равно за человеком.
    """
    conn = sqlite3.connect(f"file:{run_db}?mode=ro", uri=True)
    try:
        by_chunk = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT content_hash, url FROM corpus_chunks WHERE url IS NOT NULL"
            )
        }
        out: dict[tuple[str, str], list[str]] = defaultdict(list)
        for product_id, param_id, chunk_hash in conn.execute(
            "SELECT product_id, param_id, content_hash FROM cell_evidence"
            " WHERE citation_quote IS NOT NULL AND citation_quote != ''"
        ):
            url = by_chunk.get(chunk_hash)
            if url is None:
                continue
            key = (product_id, param_id)
            if url not in out[key]:
                out[key].append(url)
        return out
    finally:
        conn.close()


def _read_cells(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    """Исходная comparative table: ``(product_id, param_id)`` → строка ячейки."""
    with path.open(encoding="utf-8", newline="") as handle:
        return {
            (row["product_id"], row["param_id"]): row for row in csv.DictReader(handle)
        }


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
            annotated = any(row.get("claims") or row.get("titles") for row in batch)
            cited = any((row.get("cited_from") or "").strip() for row in batch)
            header = ["URL из ячейки", "адрес", "документ в корпусе"]
            if annotated:
                header.append("что утверждается")
            if cited:
                header.append("цитата фактически из")
            header += ["ячеек", "статус"]
            print("| " + " | ".join(header) + " |")
            print("|" + "---|" * len(header))
            for row in batch:
                doc = row.get("corpus_url") or "—"
                cells = [row["source_url"], _batch_live(row), doc]
                if annotated:
                    cells.append(_batch_claim(row))
                if cited:
                    cells.append(_batch_cited(row))
                cells += [row["cells"], _batch_status(row)]
                print("| " + " | ".join(cells) + " |")
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

    annotate = sub.add_parser(
        "annotate", help="дописать в лист утверждение ячейки (titles/claims)"
    )
    annotate.add_argument("--candidate", required=True)
    annotate.add_argument("--cells", required=True)
    annotate.add_argument("--out", required=True)
    annotate.add_argument(
        "--keep-missing",
        action="store_true",
        help="не отбрасывать строки status=missing (по умолчанию они не размечаются)",
    )
    annotate.add_argument(
        "--run-db",
        default=None,
        help="БД прогона: добавит cited_from — адрес документа, из которого фактически "
        "взята цитата (без него колонка пуста)",
    )
    annotate.set_defaults(func=cmd_annotate)

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
