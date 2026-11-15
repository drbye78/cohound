"""Пины решения человека в подготовке документного эталона (O14 плана 26, D5).

Предмет — **не** качество эталона, а одна вещь: решение разметчика обязано быть
записываемым, и записанное обязано проверяться. Поэтому здесь три факта:

* ``resolved`` + ``accept`` → запись попадает в эталон;
* ``ambiguous`` + ``accept`` **с вписанным вручную** ``doc_id`` → запись попадает в
  эталон (машина не выбрала между языковыми редакциями, выбрал человек), и валидатор
  всё равно сверяет продукт (I17) и URL;
* ``ambiguous`` + ``accept`` **без** ``doc_id`` → отказ: молча выбрать за человека
  нельзя, иначе «пришпилено человеком» и «подставлено скриптом» неотличимы.

Третий случай — тот, ради которого написано: до правки единственным записываемым
решением для ``ambiguous`` был ``reject``, то есть ячейка выпадала из эталона из-за
ограничения прибора, а не из-за факта.

БД минимальная: скрипт читает из ``corpus_docs`` только ``product_id``/``url``/``doc_id``.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "prepare_retrieval_golden_docs.py"

PRODUCT = "p1"
DOC_EN = f"{PRODUCT}:aaa"
DOC_RU = f"{PRODUCT}:bbb"
#: Документ, чей URL буквально равен `source_url` — для `match='exact'`.
DOC_MAIN = f"{PRODUCT}:ccc"


def _load_harness() -> object:
    spec = importlib.util.spec_from_file_location("_prepare_retrieval_golden_docs", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


PREP = _load_harness()

CANDIDATE_FIELDS = PREP.CANDIDATE_FIELDS  # type: ignore[attr-defined]


@pytest.fixture()
def corpus_db(tmp_path: Path) -> Path:
    """Корпус из двух редакций одного раздела — ровно случай ``canonical_query``."""
    path = tmp_path / "cohound.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE corpus_docs (product_id TEXT, url TEXT, doc_id TEXT)"
        )
        conn.executemany(
            "INSERT INTO corpus_docs (product_id, url, doc_id) VALUES (?, ?, ?)",
            [
                (PRODUCT, "https://docs.test/tune?lang=en", DOC_EN),
                (PRODUCT, "https://docs.test/tune?lang=ru-en", DOC_RU),
                (PRODUCT, "https://docs.test/tune", DOC_MAIN),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return path


def _row(**overrides: str) -> dict[str, str]:
    row = {field: "" for field in CANDIDATE_FIELDS}
    row.update(
        {
            "product_id": PRODUCT,
            "param_ids": "param.a",
            "cells": "1",
            "source_url": "https://docs.test/tune",
            "status": "ambiguous",
            "match": "canonical_query",
            "corpus_url": "https://docs.test/tune?lang=en",
        }
    )
    row.update(overrides)
    return row


def _write(tmp_path: Path, rows: list[dict[str, str]]) -> Path:
    import csv

    path = tmp_path / "candidate.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CANDIDATE_FIELDS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def _compile(tmp_path: Path, rows: list[dict[str, str]], corpus_db: Path) -> int:
    return PREP.main(  # type: ignore[attr-defined]
        [
            "compile",
            "--candidate",
            str(_write(tmp_path, rows)),
            "--out",
            str(tmp_path / "retrieval_golden_docs.json"),
            "--run-db",
            str(corpus_db),
        ]
    )


def test_ambiguous_with_hand_pinned_doc_id_is_recorded(
    tmp_path: Path, corpus_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Человек выбрал редакцию — решение записано и прошло валидатор."""
    assert _compile(tmp_path, [_row(decision="accept", doc_id=DOC_RU)], corpus_db) == 0
    out = capsys.readouterr()
    assert "1 записей" in out.out


def test_ambiguous_without_doc_id_is_refused(
    tmp_path: Path, corpus_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Без выбранного документа ``accept`` не записывается — и отказ говорит, что делать."""
    assert _compile(tmp_path, [_row(decision="accept")], corpus_db) == 2
    err = capsys.readouterr().err
    assert "ambiguous" in err
    assert "doc_id" in err


def test_hand_pinned_doc_id_of_another_product_is_refused(
    tmp_path: Path, corpus_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Пин не обходит I17: чужой документ не становится свидетельством продукта."""
    conn = sqlite3.connect(corpus_db)
    try:
        conn.execute(
            "INSERT INTO corpus_docs (product_id, url, doc_id) VALUES (?, ?, ?)",
            ("p2", "https://docs.test/tune?lang=en", "p2:ccc"),
        )
        conn.commit()
    finally:
        conn.close()
    assert _compile(tmp_path, [_row(decision="accept", doc_id="p2:ccc")], corpus_db) == 2
    assert "I17" in capsys.readouterr().err


def test_resolved_accept_still_requires_doc_id(
    tmp_path: Path, corpus_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Машинный ``resolved`` без ``doc_id`` — по-прежнему отказ, а не пустая запись."""
    assert (
        _compile(
            tmp_path,
            [_row(status="resolved", match="exact", decision="accept")],
            corpus_db,
        )
        == 2
    )
    assert "без doc_id" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# annotate: подсказка разметчику (что утверждается и откуда фактически взята цитата)
# ---------------------------------------------------------------------------
CELLS_FIELDS = [
    "product_id",
    "excel_number",
    "section",
    "excel_title",
    "raw_value",
    "normalized_status",
    "param_id",
    "source_url",
    "source_comment",
    "hyperlink_target",
]


def _cells_csv(tmp_path: Path) -> Path:
    import csv

    path = tmp_path / "cells.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CELLS_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerow(
            {
                "product_id": PRODUCT,
                "excel_number": "5",
                "section": "Общие",
                "excel_title": "Техническая поддержка 24×7",
                "raw_value": "ДА",
                "normalized_status": "✅",
                "param_id": "param.a",
                "source_url": "https://docs.test/tune",
                "source_comment": "",
                "hyperlink_target": "https://docs.test/tune",
            }
        )
        writer.writerow(
            {
                "product_id": PRODUCT,
                "excel_number": "6",
                "section": "Общие",
                "excel_title": "Документация\nна русском",
                "raw_value": "ЧАСТИЧНО",
                "normalized_status": "⚠️",
                "param_id": "param.b",
                "source_url": "https://docs.test/tune",
                "source_comment": "",
                "hyperlink_target": "https://docs.test/tune",
            }
        )
    return path


def test_annotate_joins_every_param_of_the_row(tmp_path: Path) -> None:
    """Подсказка собирается по **всем** `param_ids`: страница цитируется несколькими ячейками."""
    import csv

    out = tmp_path / "annotated.csv"
    assert (
        PREP.main(  # type: ignore[attr-defined]
            [
                "annotate",
                "--candidate",
                str(_write(tmp_path, [_row(param_ids="param.a;param.b")])),
                "--cells",
                str(_cells_csv(tmp_path)),
                "--out",
                str(out),
            ]
        )
        == 0
    )
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert len(rows) == 1
    # Порядок — порядок `param_ids`, значения — из исходной таблицы.
    assert rows[0]["titles"] == "Техническая поддержка 24×7 | Документация на русском"
    assert rows[0]["claims"] == "ДА | ЧАСТИЧНО"
    # `decision` annotate не трогает: решение человека не подставляется.
    assert rows[0]["decision"] == ""


def test_annotate_leaves_unknown_param_empty_and_says_so(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Ячейки нет в исходной таблице — колонки пусты, и это названо, а не выдумано."""
    import csv

    out = tmp_path / "annotated.csv"
    assert (
        PREP.main(  # type: ignore[attr-defined]
            [
                "annotate",
                "--candidate",
                str(_write(tmp_path, [_row(param_ids="param.a;param.нет")])),
                "--cells",
                str(_cells_csv(tmp_path)),
                "--out",
                str(out),
            ]
        )
        == 0
    )
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert rows[0]["titles"] == "Техническая поддержка 24×7"
    assert "param.нет" in capsys.readouterr().err


def test_annotate_resolves_citation_through_chunk_hash(tmp_path: Path) -> None:
    """Адрес цитаты — через `corpus_chunks.content_hash` (хеш **чанка**, не документа).

    Проверяется именно выбор таблицы: `cell_evidence.content_hash` не совпадает с
    `corpus_docs.content_hash`, поэтому join по документам даёт ноль — и лист молча
    показывал бы «цитаты нет» там, где она есть.
    """
    import csv

    db = tmp_path / "cohound.db"
    conn = sqlite3.connect(db)
    try:
        conn.execute(
            "CREATE TABLE corpus_docs (product_id TEXT, url TEXT, doc_id TEXT, content_hash TEXT)"
        )
        conn.execute(
            "CREATE TABLE corpus_chunks (content_hash TEXT, doc_id TEXT, url TEXT)"
        )
        conn.execute(
            "CREATE TABLE cell_evidence (product_id TEXT, param_id TEXT,"
            " content_hash TEXT, citation_quote TEXT)"
        )
        conn.execute(
            "INSERT INTO corpus_docs VALUES (?, ?, ?, ?)",
            (PRODUCT, "https://docs.test/tune", f"{PRODUCT}:aaa", "DOC-HASH"),
        )
        # Хеш чанка — другой, и адрес цитаты — другой документ.
        conn.execute(
            "INSERT INTO corpus_chunks VALUES (?, ?, ?)",
            ("CHUNK-HASH", f"{PRODUCT}:aaa", "https://docs.test/other"),
        )
        conn.execute(
            "INSERT INTO cell_evidence VALUES (?, ?, ?, ?)",
            (PRODUCT, "param.a", "CHUNK-HASH", "текст цитаты"),
        )
        conn.commit()
    finally:
        conn.close()

    out = tmp_path / "annotated.csv"
    assert (
        PREP.main(  # type: ignore[attr-defined]
            [
                "annotate",
                "--candidate",
                str(_write(tmp_path, [_row(param_ids="param.a")])),
                "--cells",
                str(_cells_csv(tmp_path)),
                "--run-db",
                str(db),
                "--out",
                str(out),
            ]
        )
        == 0
    )
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert rows[0]["cited_from"] == "https://docs.test/other"


# ---------------------------------------------------------------------------
# XLSX: рабочий лист разметчика (Excel), CSV — формат обмена
# ---------------------------------------------------------------------------
def _xlsx(tmp_path: Path, name: str = "sheet.xlsx") -> Path:
    return tmp_path / name


def test_xlsx_sheet_is_workable_for_annotation(tmp_path: Path) -> None:
    """Лист пригоден для разметки: шапка, фильтр, список решений, залитые строки, инструкция.

    Проверяются именно те свойства, ради которых формат сменён: без выпадающего списка
    опечатка в `decision` доходит до `compile`; без заливки 14 неоднозначных строк
    теряются среди 102; без второго листа инструкция живёт в переписке.
    """
    from openpyxl import load_workbook

    out = _xlsx(tmp_path)
    rows = [
        _row(param_ids="param.a", status="resolved", match="exact", live_kind="live"),
        _row(param_ids="param.b", status="resolved", match="canonical", live_kind="moved"),
        _row(param_ids="param.c", status="ambiguous", match="canonical_query"),
    ]
    assert (
        PREP.main(  # type: ignore[attr-defined]
            [
                "annotate",
                "--candidate",
                str(_write(tmp_path, rows)),
                "--cells",
                str(_cells_csv(tmp_path)),
                "--out",
                str(out),
            ]
        )
        == 0
    )
    book = load_workbook(out)
    sheet = book["лист"]
    header = [cell.value for cell in sheet[1]]
    assert header[:3] == ["product_id", "param_ids", "cells"]
    # Человеческие колонки стоят рядом и отделены от машинного `note`.
    assert header[header.index("decision") + 1] == "comment"
    assert sheet.freeze_panes == "A2"
    assert sheet.auto_filter.ref is not None
    # Список решений — на всю колонку, кроме шапки.
    validations = sheet.data_validations.dataValidation
    assert len(validations) == 1
    assert validations[0].formula1 == '"accept,reject"'
    assert str(validations[0].sqref) == f"I2:I{len(rows) + 1}"
    # Залиты ровно те строки, где решение не механическое (canonical и ambiguous).
    filled = [
        index
        for index in range(2, sheet.max_row + 1)
        if sheet.cell(row=index, column=1).fill.start_color.rgb == "00FFF2CC"
    ]
    assert filled == [3, 4]
    # Второй лист — инструкция, а не пустая вкладка.
    assert book.sheetnames == ["лист", "как размечать"]
    assert any("accept" in str(cell.value) for cell in book["как размечать"]["A"])


def test_compile_reads_xlsx_and_keeps_hand_pinned_doc_id(
    tmp_path: Path, corpus_db: Path
) -> None:
    """Решение разметчика, записанное в Excel, доходит до эталона — включая ручной пин."""
    from openpyxl import load_workbook

    sheet = _xlsx(tmp_path)
    rows = [
        _row(
            param_ids="param.a",
            status="resolved",
            match="exact",
            decision="accept",
            doc_id=DOC_MAIN,
        ),
        _row(param_ids="param.b", decision="accept", doc_id=DOC_RU),
        _row(param_ids="param.c", status="resolved", match="exact", decision="reject"),
    ]
    assert (
        PREP.main(  # type: ignore[attr-defined]
            [
                "annotate",
                "--candidate",
                str(_write(tmp_path, rows)),
                "--cells",
                str(_cells_csv(tmp_path)),
                "--out",
                str(sheet),
            ]
        )
        == 0
    )
    # Excel не должен терять значения: проверяем, что они в книге.
    book = load_workbook(sheet)
    header = {cell.value: cell.column for cell in book["лист"][1]}
    assert book["лист"].cell(row=2, column=header["decision"]).value == "accept"
    book.close()

    out = tmp_path / "retrieval_golden_docs.json"
    assert (
        PREP.main(  # type: ignore[attr-defined]
            [
                "compile",
                "--candidate",
                str(sheet),
                "--out",
                str(out),
                "--run-db",
                str(corpus_db),
            ]
        )
        == 0
    )
    import json

    payload = json.loads(out.read_text(encoding="utf-8"))
    cells = {entry["cell"]: entry for entry in payload["entries"]}
    # reject в эталон не попадает; ambiguous с вписанным doc_id — попадает.
    assert set(cells) == {f"{PRODUCT}/param.a", f"{PRODUCT}/param.b"}
    assert cells[f"{PRODUCT}/param.b"]["expected_doc_ids"] == [DOC_RU]


def test_annotate_keeps_human_decisions_in_their_own_columns(tmp_path: Path) -> None:
    """Пересборка листа не стирает разметку; `comment` человека отделён от машинного `note`.

    Без переноса повторный `annotate` (после смены корпуса) обнулял бы работу человека.
    Без отдельной колонки `comment` пометка разметчика и машинная `moved_from=…` делили бы
    одну ячейку, и одна из сторон затирала бы другую при каждой пересборке.
    """
    from openpyxl import load_workbook

    sheet = _xlsx(tmp_path)
    base = _write(tmp_path, [_row(param_ids="param.a")])
    assert (
        PREP.main(  # type: ignore[attr-defined]
            ["annotate", "--candidate", str(base), "--cells", str(_cells_csv(tmp_path)), "--out", str(sheet)]
        )
        == 0
    )
    book = load_workbook(sheet)
    ws = book["лист"]
    header = {cell.value: cell.column for cell in ws[1]}
    ws.cell(row=2, column=header["decision"]).value = "reject"
    ws.cell(row=2, column=header["comment"]).value = "другой раздел"
    ws.cell(row=2, column=header["note"]).value = "устаревшая машинная пометка"
    book.save(sheet)

    assert (
        PREP.main(  # type: ignore[attr-defined]
            ["annotate", "--candidate", str(sheet), "--cells", str(_cells_csv(tmp_path)), "--out", str(sheet)]
        )
        == 0
    )
    ws = load_workbook(sheet)["лист"]
    assert ws.cell(row=2, column=header["decision"]).value == "reject"
    assert ws.cell(row=2, column=header["comment"]).value == "другой раздел"
    # `note` — машинная колонка: при правке листа на месте она читается и пишется
    # обратно как есть (пересчитывает её `build`, а не `annotate`).
    assert ws.cell(row=2, column=header["note"]).value == "устаревшая машинная пометка"
