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
