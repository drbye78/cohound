"""Прибор разложения ошибки (O15): семантика классов и адрес цитаты.

Предмет — **не** числа (они публикуются отчётом), а правила, которые легко сделать
тихо неверными:

* класс ячейки: «не измерили» (``status = null``) не должно читаться как «поиск не дал»,
  иначе провал провайдера LLM попадёт в адрес ретривала;
* адрес цитаты: ``cell_evidence.source_url`` у документационного контура пуст, документ
  восстанавливается по ``content_hash``; неизвестный хеш — «не знаем», а не «другой
  документ»;
* один хеш может принадлежать нескольким документам (одинаковый текст под разными URL) —
  тогда карта честно отдаёт все, а не выбирает один.
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "measure_retrieval_error_split.py"


def _load_harness() -> object:
    spec = importlib.util.spec_from_file_location("_measure_retrieval_error_split", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Модуль регистрируется до exec: иначе ``@dataclass`` не находит ``sys.modules``
    # по ``cls.__module__`` и падает на этапе определения класса.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


HARNESS = _load_harness()

PRODUCT = "p1"
RUN = "run-a"


def _db(path: Path) -> Path:
    """Минимальная БД: ``corpus_chunks``, ``corpus_docs``, ``cell_evidence``."""
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE corpus_chunks (chunk_id TEXT, doc_id TEXT, content_hash TEXT)"
    )
    conn.execute("CREATE TABLE corpus_docs (doc_id TEXT, url TEXT)")
    conn.execute(
        "CREATE TABLE cell_evidence (run_id TEXT, product_id TEXT, param_id TEXT,"
        " content_hash TEXT)"
    )
    conn.executemany(
        "INSERT INTO corpus_chunks VALUES (?, ?, ?)",
        [
            ("c1", "d1", "h1"),
            ("c2", "d1", "h1"),
            # один и тот же текст под двумя документами
            ("c3", "d2", "h2"),
            ("c4", "d3", "h2"),
        ],
    )
    conn.commit()
    conn.close()
    return path


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Классы ячеек
# ---------------------------------------------------------------------------


def test_status_null_is_not_a_retrieval_failure() -> None:
    """``status = null`` (failed/preliminary, I4/I13) — отсутствие измерения, не промах."""
    assert HARNESS.classify(None, False) == "no_verdict"
    assert HARNESS.classify(None, True) == "no_verdict"
    assert HARNESS.classify("", False) == "no_verdict"


def test_unknown_verdict_splits_by_whether_the_document_was_found() -> None:
    assert HARNESS.classify("❓", False) == "search_missed"
    assert HARNESS.classify("❓", True) == "evaluator_missed"


def test_positive_verdict_splits_by_whether_the_document_was_found() -> None:
    assert HARNESS.classify("✅", True) == "consistent_positive"
    assert HARNESS.classify("⚠️", True) == "consistent_positive"
    assert HARNESS.classify("✅", False) == "lucky"
    assert HARNESS.classify("⚠️", False) == "lucky"


def test_absence_cell_is_outside_the_retrieval_question() -> None:
    """❌ — утверждение об отсутствии (I2d): носитель другой, поиск тут не виноват."""
    assert HARNESS.classify("❌", False) == "absence_cell"
    assert HARNESS.classify("❌", True) == "absence_cell"


# ---------------------------------------------------------------------------
# Адрес цитаты
# ---------------------------------------------------------------------------


def test_corpus_maps_are_built_in_one_pass(tmp_path: Path) -> None:
    conn = _connect(_db(tmp_path / "cohound.db"))
    try:
        chunk_doc, hash_docs = HARNESS._corpus_maps(conn)
    finally:
        conn.close()
    assert chunk_doc == {"c1": "d1", "c2": "d1", "c3": "d2", "c4": "d3"}
    # один хеш — два документа: карта отдаёт оба, а не «первый попавшийся»
    assert hash_docs["h1"] == frozenset({"d1"})
    assert hash_docs["h2"] == frozenset({"d2", "d3"})


def test_citation_docs_resolve_through_content_hash(tmp_path: Path) -> None:
    """``source_url`` пуст — документ цитаты восстанавливается по ``content_hash``."""
    db = _db(tmp_path / "cohound.db")
    conn = _connect(db)
    try:
        conn.executemany(
            "INSERT INTO cell_evidence VALUES (?, ?, ?, ?)",
            [
                (RUN, PRODUCT, "p_one", "h1"),
                (RUN, PRODUCT, "p_two", "h2"),
                # хеш, которого нет в корпусе: «не знаем», а не «другой документ»
                (RUN, PRODUCT, "p_three", "нет-такого"),
                # чужая строка прогона не должна попасть в разложение
                ("run-b", PRODUCT, "p_one", "h2"),
            ],
        )
        conn.commit()
        chunk_doc, hash_docs = HARNESS._corpus_maps(conn)
        assert chunk_doc  # карта нужна прибору, здесь — только чтобы проход состоялся
        citations = HARNESS._citation_docs(conn, RUN, hash_docs)
    finally:
        conn.close()
    assert citations[(PRODUCT, "p_one")] == frozenset({"d1"})
    assert citations[(PRODUCT, "p_two")] == frozenset({"d2", "d3"})
    assert citations[(PRODUCT, "p_three")] == frozenset()


def test_cell_without_evidence_has_no_citation_entry(tmp_path: Path) -> None:
    db = _db(tmp_path / "cohound.db")
    conn = _connect(db)
    try:
        _, hash_docs = HARNESS._corpus_maps(conn)
        assert HARNESS._citation_docs(conn, RUN, hash_docs) == {}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# По-ячеечная таблица
# ---------------------------------------------------------------------------


def test_csv_names_the_citation_address(tmp_path: Path) -> None:
    reports = [
        {
            "profile": "R0",
            "per_cell": [
                {
                    "cell": f"{PRODUCT}/p_one",
                    "product_id": PRODUCT,
                    "param_id": "p_one",
                    "status": "✅",
                    "found": True,
                    "class": "consistent_positive",
                    "cited_doc_ids": ["d9", "d1"],
                    "citation_hits_golden": True,
                },
                {
                    "cell": f"{PRODUCT}/p_two",
                    "product_id": PRODUCT,
                    "param_id": "p_two",
                    "status": "❓",
                    "found": False,
                    "class": "search_missed",
                    "cited_doc_ids": [],
                    "citation_hits_golden": None,
                },
            ],
        }
    ]
    out = tmp_path / "cells.csv"
    HARNESS._write_csv(out, reports)
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert lines[0] == (
        "profile,cell,product_id,param_id,status,found,class,cited_docs,"
        "citation_hits_golden"
    )
    assert lines[1] == f"R0,{PRODUCT}/p_one,{PRODUCT},p_one,✅,1,consistent_positive,2,1"
    # нет цитат — пустое поле, а не «0»: «не знаем» ≠ «другой документ»
    assert lines[2] == f"R0,{PRODUCT}/p_two,{PRODUCT},p_two,❓,0,search_missed,0,"


def test_main_refuses_missing_db(tmp_path: Path) -> None:
    code = HARNESS.main(
        ["--db", str(tmp_path / "нет.db"), "--golden", str(tmp_path / "golden.json")]
    )
    assert code == 2
