#!/usr/bin/env python3
"""Прибор O18: лист на разметку доказательной силы цитат (50 evidence).

**Вопрос.** I22 проверяет только, что ``citation_quote`` непуста и найдена в документе.
Поддерживает ли цитата **утверждение ячейки** — не проверяется ничем. Пока это не
измерено, entailment (`EvidenceClaim`) — ещё один вызов LLM без числа: неизвестно,
какая доля цитат вообще требует проверки и сколько из них проходят её вхолостую.

**Предмет замера** — доля ``no`` в разметке: цитата подлинная (I22 пройден), но
утверждение ячейки она не поддерживает. Это шум доказательной силы, а не «плохая
страница»: адрес документа и релевантность здесь не оцениваются.

Подкоманды:

``build``
    Строит лист на разметку. Выборка **детерминированная и объявленная**: 50 evidence,
    стратифицированно по статусу ячейки (✅/⚠️/❌) пропорционально их числу в прогоне;
    внутри страты — сортировка по ``evidence_id`` и равномерный шаг (``random`` не
    используется). Одна ячейка — не больше одного evidence, иначе 50 строк описали бы
    единицы параметров.

``validate``
    Проверяет **заполненный** лист: все ``supports`` из набора, нет пустых
    ``evidence_id``, нет дублей (evidence и ячейки), число строк равно ожидаемому.

``summary``
    Печатает разложение ``yes/partial/no`` и долю ``no`` — это и есть число шума для O18.

**Почему чтение сырым ``sqlite3``.** Гейт схемы отказывает файлу прогона, чья
``schema_version`` старше кода (стенд 3.20.0 при коде 3.25.0). Прибор читает данные
``mode=ro`` и **публикует** версию схемы файла в отчёте: число относится к корпусу,
записанному этим файлом. Тот же приём, что у :mod:`scripts.measure_retrieval_error_split`.

Каталог параметров берётся из **снимка прогона** (``plan_inputs.json`` рядом с
``cohound.db``), а не из пакета: пакет мог измениться после прогона, и название
параметра тогда описывало бы не тот каталог, по которому вынесен вердикт.

Нейтрален (§1.3): ни продуктов, ни домена — БД и снимок приходят аргументами.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: Страты выборки — вердикты ячейки, у которых есть доказательство (I1). ``❓`` в
#: выборку не входит: у неё нет утверждения, которое цитата могла бы поддержать.
STRATA = ("✅", "⚠️", "❌")

#: Значения ``supports``, которые пишет человек. Пусто = ещё не размечено.
SUPPORTS = ("yes", "partial", "no")

#: Колонки листа в порядке чтения. Первые девять — машинные (их пишет ``build``),
#: последние три — человеческие: ``note`` здесь **человеческая** (чем именно не
#: поддерживает), в отличие от машинного ``note`` документного эталона (O14).
COLUMNS = (
    "evidence_id",
    "product_id",
    "param_id",
    "param_title",
    "status",
    "claims",
    "citation_quote",
    "doc_url",
    "context",
    "supports",
    "note",
    "comment",
)

#: Колонки, которые заполняет человек.
HUMAN_COLUMNS = ("supports", "note", "comment")

#: Окно контекста вокруг цитаты (символов, включая саму цитату).
CONTEXT_WINDOW = 400

#: Цитата короче этого порога почти не даёт разметчику материала для суждения —
#: такие строки заливаются в листе.
SHORT_QUOTE_CHARS = 40

#: Значение ``context``, когда цитата не нашлась ни в одном чанке своего хеша.
#: Токен машинный и видимый: пустая клетка читалась бы как «контекста нет вообще».
CONTEXT_MISSING = "context_missing: quote_not_in_chunk"

#: Сколько первых символов цитаты искать во второй попытке (цитата через границу
#: чанка или с иной разбивкой пробелов). Найденный так контекст помечается префиксом.
CONTEXT_PREFIX_CHARS = 60


@dataclass(frozen=True)
class EvidenceRow:
    """Одна строка листа: цитата + утверждение ячейки + контекст чанка.

    ``evidence_type``/``source_kind`` в лист не пишутся: они нужны только правилу
    подсветки «судить труднее» и печатаются в сводке сборки.
    """

    evidence_id: str
    product_id: str
    param_id: str
    param_title: str
    status: str
    claims: str
    citation_quote: str
    doc_url: str
    context: str
    evidence_type: str = ""
    source_kind: str = ""

    def as_row(self) -> dict[str, str]:
        return {
            "evidence_id": self.evidence_id,
            "product_id": self.product_id,
            "param_id": self.param_id,
            "param_title": self.param_title,
            "status": self.status,
            "claims": self.claims,
            "citation_quote": self.citation_quote,
            "doc_url": self.doc_url,
            "context": self.context,
            "supports": "",
            "note": "",
            "comment": "",
        }


# ---------------------------------------------------------------------------
# Выборка: чистая арифметика, проверяемая тестами без БД
# ---------------------------------------------------------------------------
def uniform_pick(items: Sequence[Any], k: int) -> list[Any]:
    """``k`` значений из ``items`` равномерным шагом (не ``random``).

    Индекс i-го — ``round(i * (m-1) / (k-1))``: первый и последний элементы попадают
    всегда, промежуточные распределены по всему ряду. ``k >= m`` — берётся всё (выборка
    не додумывает строки, которых нет).
    """
    m = len(items)
    if k <= 0:
        return []
    if k >= m:
        return list(items)
    if k == 1:
        return [items[0]]
    return [items[round(i * (m - 1) / (k - 1))] for i in range(k)]


def allocate(counts: dict[str, int], total: int) -> dict[str, int]:
    """Квоты пропорционально ``counts`` (метод наибольших остатков).

    Тай-брейк — **порядок ключей** переданного словаря: он объявлен вызывающим
    (``STRATA``), поэтому повторный запуск даёт те же квоты. Остаток, который нельзя
    раздать по дробным частям, уходит в те же страты по тому же порядку.
    """
    keys = [key for key in counts if counts[key] > 0]
    denominator = sum(counts[key] for key in keys)
    if denominator <= 0 or total <= 0:
        return {key: 0 for key in keys}
    quotas = {key: total * counts[key] / denominator for key in keys}
    out = {key: int(quotas[key]) for key in keys}
    rest = total - sum(out.values())
    order = sorted(keys, key=lambda key: (-(quotas[key] - out[key]), keys.index(key)))
    for key in order[:rest]:
        out[key] += 1
    return out


def cap_and_redistribute(
    quotas: dict[str, int], available: dict[str, int], sample: int
) -> dict[str, int]:
    """Обрезает квоты по числу доступных строк и раздаёт излишек, где есть запас.

    Страта, в которой evidence меньше квоты (например ``❌``), не добирается
    повторами: излишек уходит в страты с запасом, а расхождение печатается сборкой.
    """
    out = dict(quotas)
    for _ in range(len(out) * sample + 1):
        excess = 0
        for key in out:
            limit = available.get(key, 0)
            if out[key] > limit:
                excess += out[key] - limit
                out[key] = limit
        if not excess:
            return out
        spare = [key for key in out if out[key] < available.get(key, 0)]
        if not spare:
            return out
        for key in sorted(spare, key=lambda key: (-(available[key] - out[key]), list(out).index(key))):
            if excess <= 0:
                break
            add = min(excess, available[key] - out[key])
            out[key] += add
            excess -= add
    return out


def stratified_sample(
    rows: Sequence[EvidenceRow], counts: dict[str, int], sample: int
) -> tuple[list[EvidenceRow], dict[str, int]]:
    """Отбирает ``sample`` строк: квоты по стратам + равномерный шаг внутри страты.

    ``counts`` — число ячеек каждой страты **в прогоне** (знаменатель пропорции), а не
    число доступных evidence: пропорция объявлена по составу прогона, доступность
    корректируется отдельно (:func:`cap_and_redistribute`) и печатается.
    """
    by_stratum = {
        status: sorted(
            (row for row in rows if row.status == status), key=lambda row: row.evidence_id
        )
        for status in STRATA
    }
    available = {status: len(by_stratum[status]) for status in STRATA}
    quotas = allocate({status: counts.get(status, 0) for status in STRATA}, sample)
    quotas = cap_and_redistribute(quotas, available, sample)
    picked: list[EvidenceRow] = []
    for status in STRATA:
        picked.extend(uniform_pick(by_stratum[status], quotas.get(status, 0)))
    if len(picked) != sample:
        raise ValueError(
            f"выборка {len(picked)} вместо {sample}: доступно evidence по стратам "
            f"{available} — расширьте прогон или уменьшите --sample"
        )
    return picked, quotas


# ---------------------------------------------------------------------------
# Чтение прогона
# ---------------------------------------------------------------------------
def _open_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _single_run(conn: sqlite3.Connection) -> tuple[str, str]:
    """``(run_id, schema_version файла)``. Файл с несколькими прогонами — отказ."""
    rows = conn.execute("SELECT run_id FROM runs").fetchall()
    if len(rows) != 1:
        raise SystemExit(
            f"build: в файле {len(rows)} прогонов — назовите прогон одним файлом"
        )
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    return str(rows[0]["run_id"]), (str(row["value"]) if row is not None else "")


def load_params(snapshot: Path) -> dict[str, dict[str, str]]:
    """Каталог параметров **снимка прогона**: ``param_id → {title, rule, …}``."""
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    return {
        str(item["param_id"]): {key: str(item.get(key) or "") for key in item}
        for item in payload.get("params", [])
    }


def cell_records(
    conn: sqlite3.Connection, run_id: str
) -> dict[tuple[str, str], dict[str, str]]:
    """Ячейки прогона: ``(product_id, param_id) → {status, justification, profile}``."""
    out: dict[tuple[str, str], dict[str, str]] = {}
    for row in conn.execute(
        "SELECT product_id, param_id, status, justification, retrieval_profile"
        " FROM cells WHERE run_id = ?",
        (run_id,),
    ):
        out[(str(row["product_id"]), str(row["param_id"]))] = {
            "status": str(row["status"] or ""),
            "justification": str(row["justification"] or ""),
            "retrieval_profile": str(row["retrieval_profile"] or ""),
        }
    return out


def evidence_records(conn: sqlite3.Connection, run_id: str) -> list[sqlite3.Row]:
    """Evidence с непустой цитатой: только их I22 признаёт доказательством."""
    return conn.execute(
        "SELECT evidence_id, product_id, param_id, source_kind, evidence_type,"
        " source_url, citation_quote, content_hash, doc_version_id"
        " FROM cell_evidence"
        " WHERE run_id = ? AND citation_quote IS NOT NULL AND citation_quote != ''",
        (run_id,),
    ).fetchall()


def one_per_cell(rows: Sequence[sqlite3.Row]) -> list[sqlite3.Row]:
    """Не больше одного evidence на ячейку; выбирается первый по ``evidence_id``.

    50 строк обязаны покрыть 50 ячеек: иначе выборка описывает несколько параметров
    и ничего не говорит о шуме по карте.
    """
    chosen: dict[tuple[str, str], sqlite3.Row] = {}
    for row in sorted(rows, key=lambda item: str(item["evidence_id"])):
        key = (str(row["product_id"]), str(row["param_id"]))
        chosen.setdefault(key, row)
    return [chosen[key] for key in sorted(chosen)]


def excerpt(text: str, quote: str, window: int = CONTEXT_WINDOW) -> str:
    """Окно ~``window`` символов вокруг цитаты; пустая строка — цитаты в тексте нет."""
    index = text.find(quote)
    if index < 0:
        return ""
    half = max(0, (window - len(quote)) // 2)
    start = max(0, index - half)
    end = min(len(text), index + len(quote) + half)
    body = " ".join(text[start:end].split())
    return ("…" if start > 0 else "") + body + ("…" if end < len(text) else "")


def quote_contexts(
    conn: sqlite3.Connection, wanted: Sequence[tuple[str, str]], window: int = CONTEXT_WINDOW
) -> dict[tuple[str, str], tuple[str, str]]:
    """``(content_hash, quote) → (context, url)`` одним проходом по корпусу.

    Индекса на ``corpus_chunks.content_hash`` нет, поэтому точечный запрос на каждую
    цитату дал бы полный скан на строку. Один join с временной таблицей читает корпус
    один раз (876 evidence × 253 306 чанков — 0.4 с).

    Цитата может пересекать границу чанка: вторая попытка ищет первые
    ``CONTEXT_PREFIX_CHARS`` символов, и найденный так контекст помечается префиксом
    ``quote_prefix_match`` — сужение видно, а не выдаётся за точное попадание.
    """
    if not wanted:
        return {}
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS pick(hash TEXT, quote TEXT)")
    conn.execute("DELETE FROM pick")
    conn.executemany(
        "INSERT INTO pick VALUES (?, ?)", sorted(set(wanted))
    )
    conn.execute("CREATE INDEX IF NOT EXISTS pick_hash ON pick(hash)")
    found: dict[tuple[str, str], tuple[str, str]] = {}
    for row in conn.execute(
        "SELECT c.content_hash AS hash, p.quote AS quote, c.url AS url, c.text AS text"
        " FROM corpus_chunks c JOIN pick p ON p.hash = c.content_hash"
        " WHERE instr(c.text, p.quote) > 0"
        " ORDER BY c.chunk_id"
    ):
        key = (str(row["hash"]), str(row["quote"]))
        if key not in found:
            found[key] = (excerpt(str(row["text"]), key[1], window), str(row["url"] or ""))

    for key in sorted(set(wanted) - set(found)):
        prefix = " ".join(key[1].split())[:CONTEXT_PREFIX_CHARS]
        if not prefix:
            continue
        row = conn.execute(
            "SELECT url, text FROM corpus_chunks WHERE content_hash = ?"
            " AND instr(replace(replace(replace(text, char(10), ' '), char(9), ' '),"
            " '  ', ' '), ?) > 0 ORDER BY chunk_id LIMIT 1",
            (key[0], prefix),
        ).fetchone()
        if row is not None:
            found[key] = (
                "quote_prefix_match: " + excerpt(str(row["text"]), prefix, window),
                str(row["url"] or ""),
            )
    return found


def build_rows(
    conn: sqlite3.Connection,
    params: dict[str, dict[str, str]],
    run_id: str,
    sample: int,
) -> tuple[list[EvidenceRow], dict[str, Any]]:
    """Собирает строки листа и условия выборки (для печати и отчёта)."""
    cells = cell_records(conn, run_id)
    counts = Counter(
        record["status"] for record in cells.values() if record["status"] in STRATA
    )
    evidence = [
        row
        for row in evidence_records(conn, run_id)
        if cells.get((str(row["product_id"]), str(row["param_id"])), {}).get("status")
        in STRATA
    ]
    unique = one_per_cell(evidence)
    picked, quotas = stratified_sample(
        [
            EvidenceRow(
                evidence_id=str(row["evidence_id"]),
                product_id=str(row["product_id"]),
                param_id=str(row["param_id"]),
                param_title="",
                status=cells[(str(row["product_id"]), str(row["param_id"]))]["status"],
                claims="",
                citation_quote=str(row["citation_quote"]),
                doc_url="",
                context="",
                evidence_type=str(row["evidence_type"] or ""),
                source_kind=str(row["source_kind"] or ""),
            )
            for row in unique
        ],
        dict(counts),
        sample,
    )
    by_id = {str(row["evidence_id"]): row for row in unique}
    contexts = quote_contexts(
        conn,
        [
            (str(by_id[item.evidence_id]["content_hash"] or ""), item.citation_quote)
            for item in picked
        ],
    )
    rows: list[EvidenceRow] = []
    missing_params: list[str] = []
    for item in picked:
        raw = by_id[item.evidence_id]
        param = params.get(item.param_id)
        if param is None:
            missing_params.append(item.param_id)
            param = {}
        cell = cells[(item.product_id, item.param_id)]
        justification = cell["justification"].strip()
        rule = (param.get("rule") or "").strip()
        claims = justification
        if rule:
            claims = f"{claims}\n\nПравило: {rule}" if claims else f"Правило: {rule}"
        digest = str(raw["content_hash"] or "")
        context, chunk_url = contexts.get((digest, item.citation_quote), (CONTEXT_MISSING, ""))
        rows.append(
            EvidenceRow(
                evidence_id=item.evidence_id,
                product_id=item.product_id,
                param_id=item.param_id,
                param_title=str(param.get("title") or ""),
                status=item.status,
                claims=claims,
                citation_quote=item.citation_quote,
                doc_url=str(raw["source_url"] or "") or chunk_url,
                context=context,
                evidence_type=item.evidence_type,
                source_kind=item.source_kind,
            )
        )
    conditions = {
        "run_id": run_id,
        "cells": len(cells),
        "strata_counts": dict(counts),
        "quotas": quotas,
        "evidence_with_quote": len(evidence),
        "unique_cells": len(unique),
        "excluded_other_status": len(evidence_records(conn, run_id)) - len(evidence),
        "retrieval_profiles": dict(
            Counter(record["retrieval_profile"] for record in cells.values())
        ),
        "missing_params": missing_params,
        "context_missing": sum(1 for row in rows if row.context == CONTEXT_MISSING),
        "context_prefix": sum(
            1 for row in rows if row.context.startswith("quote_prefix_match")
        ),
    }
    return rows, conditions


# ---------------------------------------------------------------------------
# XLSX
# ---------------------------------------------------------------------------
def _openpyxl() -> Any:
    """``openpyxl`` — зависимость extra ``extract`` (её тянет Office-путь docling).

    Импорт ленивый и с внятным отказом: скрипт обязан объяснить, чего не хватает,
    а не падать трейсбеком на импорте.
    """
    try:
        import openpyxl
    except ImportError as error:  # pragma: no cover - зависит от окружения
        raise SystemExit(
            "XLSX требует openpyxl (extra `extract`: uv sync --extra extract)"
        ) from error
    return openpyxl


def _column_letter(index: int) -> str:
    """1 → ``A``, 27 → ``AA`` (openpyxl такой утилиты в публичном API не даёт)."""
    letters = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


def needs_attention(row: EvidenceRow) -> bool:
    """Строка, где суждение о поддержке труднее обычного — заливается в листе.

    Три случая, и все три измерены на прогоне, а не выдуманы: короткая цитата (мало
    материала), цитата не нашлась в чанке (контекст не проверить), evidence не
    ``direct``/``documentation`` (сила доказательства другая по природе).
    """
    if len(row.citation_quote.strip()) < SHORT_QUOTE_CHARS:
        return True
    if row.context == CONTEXT_MISSING:
        return True
    return (row.evidence_type or "") != "direct" or (row.source_kind or "") != "documentation"


def write_xlsx(path: Path, rows: Sequence[EvidenceRow]) -> None:
    """Строки листа → XLSX, пригодный для разметки руками.

    Что делает файл рабочим, а не «CSV в обёртке»: закреплённая шапка с фильтром,
    выпадающий список ``yes/partial/no`` (значение нельзя напечатать с опечаткой),
    залитые строки там, где судить труднее, перенос текста в длинных колонках и второй
    лист с инструкцией — файл живёт дольше переписки.
    """
    openpyxl = _openpyxl()
    from openpyxl.styles import Alignment, Font, PatternFill  # noqa: PLC0415 - ленивый импорт
    from openpyxl.worksheet.datavalidation import DataValidation  # noqa: PLC0415

    book = openpyxl.Workbook()
    sheet = book.active
    sheet.title = "лист"
    sheet.append(list(COLUMNS))
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = f"A1:{_column_letter(len(COLUMNS))}1"

    widths = {
        "evidence_id": 28,
        "product_id": 18,
        "param_id": 42,
        "param_title": 34,
        "status": 8,
        "claims": 55,
        "citation_quote": 55,
        "doc_url": 55,
        "context": 60,
        "supports": 12,
        "note": 40,
        "comment": 30,
    }
    wrap = {"param_id", "param_title", "claims", "citation_quote", "doc_url", "context", "note", "comment"}
    for index, name in enumerate(COLUMNS, start=1):
        sheet.column_dimensions[_column_letter(index)].width = widths.get(name, 16)

    attention = PatternFill("solid", start_color="FFF2CC")
    for row_index, row in enumerate(rows, start=2):
        sheet.append([row.as_row()[name] for name in COLUMNS])
        for index, name in enumerate(COLUMNS, start=1):
            if name in wrap:
                sheet.cell(row=row_index, column=index).alignment = Alignment(
                    wrap_text=True, vertical="top"
                )
        if needs_attention(row):
            for index in range(1, len(COLUMNS) + 1):
                sheet.cell(row=row_index, column=index).fill = attention

    supports_index = COLUMNS.index("supports") + 1
    validation = DataValidation(
        type="list",
        formula1=f'"{",".join(SUPPORTS)}"',
        allow_blank=True,
        showDropDown=False,
    )
    validation.error = "supports — yes, partial или no; пусто = ещё не размечено"
    validation.errorTitle = "Неизвестное значение"
    sheet.add_data_validation(validation)
    last = max(2, len(rows) + 1)
    validation.add(
        f"{_column_letter(supports_index)}2:{_column_letter(supports_index)}{last}"
    )

    guide = book.create_sheet("как размечать")
    for line in GUIDE_LINES:
        guide.append([line])
    guide.column_dimensions["A"].width = 110
    for cell in guide["A"]:
        cell.alignment = Alignment(wrap_text=True, vertical="top")

    path.parent.mkdir(parents=True, exist_ok=True)
    book.save(path)


#: Что означает решение. Лежит вторым листом книги: файл переживает переписку.
GUIDE_LINES: tuple[str, ...] = (
    "Размечается колонка supports — выпадающий список yes / partial / no. "
    "Пусто = ещё не размечено. note — чем именно цитата не поддерживает (свободный "
    "текст), comment — ваши пометки.",
    "",
    "Вопрос один: поддерживает ли цитата то, что утверждает ячейка (колонка claims)? "
    "Это разметка доказательной силы, а не «нравится ли страница» и не «правильный ли "
    "это документ»: адрес и релевантность здесь не оцениваются.",
    "",
    "yes — цитата прямо подтверждает утверждение: в ней сказано то же, что в claims, "
    "без натяжки. Для ячейки ❌ это значит «цитата подтверждает отсутствие» (например, "
    "прямо говорит, что маскирования нет), а не «цитата про наличие».",
    "partial — подтверждает часть утверждения или косвенно: факт рядом, но прямым "
    "утверждением не является; либо закрывает одно из нескольких условий claims.",
    "no — цитата о другом, либо утверждение сильнее цитаты (over-claim): цитата говорит "
    "«может», claims — «всегда»; цитата про смежный параметр; цитата — заголовок или "
    "пункт оглавления без содержания.",
    "",
    "Почему это число важно: I22 проверяет только, что цитата непуста и найдена в "
    "документе. Доля no — это шум, который entailment-проверка (O18) обязана убрать; "
    "без этого числа она остаётся лишним вызовом LLM.",
    "",
    "Выборка детерминированная: страты по статусу ячейки (✅/⚠️/❌) пропорционально их "
    "числу в прогоне, внутри страты — сортировка по evidence_id и равномерный шаг. "
    "Одна ячейка — не больше одного evidence. Повторная сборка тем же прибором даёт те "
    "же строки.",
    "",
    "context — фрагмент чанка вокруг цитаты (±200 символов); он для справки, решение "
    "принимается по citation_quote и claims. Если цитата через границу чанка, контекст "
    "начинается с quote_prefix_match; если не нашлась вовсе — с context_missing.",
    "",
    "Залитые строки — те, где судить труднее: короткая цитата (< 40 символов), цитата "
    "не нашлась в чанке, или evidence не direct/documentation. Их стоит разметить "
    "первыми.",
    "",
    "Когда лист заполнен: python scripts/prepare_evidence_claims.py validate "
    "--xlsx <файл>, затем summary — он печатает разложение и долю no.",
)


def read_xlsx(path: Path) -> list[dict[str, str]]:
    """XLSX → строки первого листа; значения читаются как текст."""
    openpyxl = _openpyxl()
    book = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = book.worksheets[0]
        iterator = sheet.iter_rows(values_only=True)
        try:
            header = [("" if cell is None else str(cell)).strip() for cell in next(iterator)]
        except StopIteration:
            raise SystemExit(f"{path}: пустой лист") from None
        missing = [name for name in COLUMNS if name not in header]
        if missing:
            raise SystemExit(f"{path}: нет колонок {missing}")
        out: list[dict[str, str]] = []
        for values in iterator:
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


# ---------------------------------------------------------------------------
# validate / summary
# ---------------------------------------------------------------------------
def validate_rows(rows: Sequence[dict[str, str]], expect: int) -> list[str]:
    """Проверки заполненного листа. Возвращает список проблем (пусто — лист годен)."""
    problems: list[str] = []
    seen_evidence: dict[str, int] = {}
    seen_cells: dict[tuple[str, str], int] = {}
    for lineno, row in enumerate(rows, start=2):
        evidence_id = (row.get("evidence_id") or "").strip()
        if not evidence_id:
            problems.append(f"строка {lineno}: пустой evidence_id")
        elif evidence_id in seen_evidence:
            problems.append(
                f"строка {lineno}: дубль evidence_id {evidence_id} "
                f"(первая — строка {seen_evidence[evidence_id]})"
            )
        else:
            seen_evidence[evidence_id] = lineno
        cell = (
            (row.get("product_id") or "").strip(),
            (row.get("param_id") or "").strip(),
        )
        if not cell[0] or not cell[1]:
            problems.append(f"строка {lineno}: пустой адрес ячейки {cell}")
        elif cell in seen_cells:
            problems.append(
                f"строка {lineno}: ячейка {cell[0]}/{cell[1]} уже размечена строкой "
                f"{seen_cells[cell]} — на ячейку допускается один evidence"
            )
        else:
            seen_cells[cell] = lineno
        value = (row.get("supports") or "").strip()
        if not value:
            problems.append(f"строка {lineno}: supports не заполнен ({evidence_id or '?'})")
        elif value not in SUPPORTS:
            problems.append(
                f"строка {lineno}: supports={value!r} не из {SUPPORTS} ({evidence_id or '?'})"
            )
    if len(rows) != expect:
        problems.append(f"строк {len(rows)}, ожидалось {expect}")
    return problems


def summarize(rows: Sequence[dict[str, str]]) -> dict[str, Any]:
    """Разложение ``yes/partial/no`` и доля ``no`` среди размеченных."""
    counts = Counter((row.get("supports") or "").strip() for row in rows)
    marked = sum(counts[value] for value in SUPPORTS)
    by_status = Counter((row.get("status") or "").strip() for row in rows)
    return {
        "rows": len(rows),
        "supports": {value: counts[value] for value in SUPPORTS},
        "unmarked": counts[""],
        "marked": marked,
        "noise_share": (counts["no"] / marked) if marked else None,
        "partial_share": (counts["partial"] / marked) if marked else None,
        "by_status": dict(sorted(by_status.items())),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def cmd_build(args: argparse.Namespace) -> int:
    db = Path(args.db).expanduser()
    if not db.is_file():
        print(f"build: БД не найдена: {db}", file=sys.stderr)
        return 2
    snapshot = db.parent / "plan_inputs.json"
    if not snapshot.is_file():
        print(f"build: нет снимка входов прогона: {snapshot}", file=sys.stderr)
        return 2
    params = load_params(snapshot)
    conn = _open_readonly(db)
    try:
        run_id, schema_version = _single_run(conn)
        rows, conditions = build_rows(conn, params, run_id, args.sample)
    except ValueError as error:
        print(f"build: {error}", file=sys.stderr)
        return 2
    finally:
        conn.close()

    out = Path(args.out).expanduser()
    write_xlsx(out, rows)

    print(
        f"прогон {run_id} (схема файла {schema_version}), профиль ретривала "
        f"{', '.join(f'{k or '—'}×{v}' for k, v in conditions['retrieval_profiles'].items())}"
    )
    print(
        f"ячеек {conditions['cells']}, evidence с цитатой {conditions['evidence_with_quote']} "
        f"на {conditions['unique_cells']} ячейках; вне страт ✅/⚠️/❌ — "
        f"{conditions['excluded_other_status']} (❓ не имеет утверждения для проверки)"
    )
    counts = conditions["strata_counts"]
    quotas = conditions["quotas"]
    print(
        "страты по статусу в прогоне: "
        + ", ".join(f"{status} {counts.get(status, 0)}" for status in STRATA)
        + " → квоты "
        + ", ".join(f"{status} {quotas.get(status, 0)}" for status in STRATA)
    )
    print(
        "отбор: сортировка по evidence_id, равномерный шаг round(i*(m-1)/(k-1)) внутри "
        "страты; одна ячейка — не больше одного evidence (random не используется)"
    )
    print(f"контекст: не найден у {conditions['context_missing']}, по префиксу у "
          f"{conditions['context_prefix']} из {len(rows)}")
    if conditions["missing_params"]:
        print(
            f"ВНИМАНИЕ: {len(conditions['missing_params'])} param_id нет в снимке прогона "
            f"— название параметра пусто: {', '.join(conditions['missing_params'])}",
            file=sys.stderr,
        )
    print(f"written: {out} ({len(rows)} строк, залито {sum(1 for row in rows if needs_attention(row))})")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    path = Path(args.xlsx).expanduser()
    if not path.is_file():
        print(f"validate: файл не найден: {path}", file=sys.stderr)
        return 2
    rows = read_xlsx(path)
    problems = validate_rows(rows, args.expect)
    if problems:
        for problem in problems:
            print(f"ОТКАЗ: {problem}", file=sys.stderr)
        print(f"{path}: {len(problems)} проблем(ы)", file=sys.stderr)
        return 2
    print(f"OK: {path}: {len(rows)} строк, все supports из {SUPPORTS}")
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    path = Path(args.xlsx).expanduser()
    if not path.is_file():
        print(f"summary: файл не найден: {path}", file=sys.stderr)
        return 2
    rows = read_xlsx(path)
    report = summarize(rows)
    print(f"строк: {report['rows']}")
    print("по статусу ячейки: " + ", ".join(
        f"{status} {count}" for status, count in report["by_status"].items()
    ))
    print(
        "supports: "
        + ", ".join(f"{value} {report['supports'][value]}" for value in SUPPORTS)
        + f", не размечено {report['unmarked']}"
    )
    if report["marked"] == 0:
        print("доля no: не определена — лист не размечен")
        return 0
    print(
        f"размечено {report['marked']} из {report['rows']}: "
        f"no {report['supports']['no']} = {report['noise_share']:.4f}, "
        f"partial {report['supports']['partial']} = {report['partial_share']:.4f}"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="построить лист на разметку (XLSX)")
    build.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    build.add_argument("--out", required=True, help="выходной XLSX")
    build.add_argument("--sample", type=int, default=50, help="строк в выборке (дефолт 50)")
    build.set_defaults(func=cmd_build)

    validate = sub.add_parser("validate", help="проверить заполненный лист")
    validate.add_argument("--xlsx", required=True)
    validate.add_argument("--expect", type=int, default=50, help="ожидаемое число строк")
    validate.set_defaults(func=cmd_validate)

    summary = sub.add_parser("summary", help="разложение yes/partial/no и доля no")
    summary.add_argument("--xlsx", required=True)
    summary.set_defaults(func=cmd_summary)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
