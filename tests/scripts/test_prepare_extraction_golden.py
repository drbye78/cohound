"""Тесты подготовки PDF-эталона извлечения (``scripts/prepare_extraction_golden.py``, O38б/D10).

Что доказывается — и почему именно это:

* **разметка не выдумывается.** Лист кандидатов обязан оставлять пустыми все колонки
  решений (``include``/направления/поля кейса): набор из корпуса готовит AI, размечает
  человек (D10), и «заполнено по умолчанию» означало бы, что эталон придуман инструментом.
* **отбор детерминирован и покрывает направления.** Тот же seed и тот же корпус дают тот же
  набор (иначе выборку нельзя пересчитать), а документ, у которого у направления есть
  предмет (двухколоночный — прибором O37), попадает в набор даже при малом ``--target``:
  иначе «порядок чтения» измерялся бы на пустом множестве.
* **``compile`` отказывает там, где разметка не состоялась**: строка без решения, байтов нет
  по ``raw_path``, кейс без единого ожидания (эталон, который проходит всё, не проверяет
  ничего), ``reading_order=columns`` против вердикта прибора.
* **собранный набор читается тем же загрузчиком**, что и прогон
  (:func:`core.pipeline.extraction_eval.load_extraction_golden_document`), и несёт
  ``status: candidate`` — «человек ещё не подтвердил».

PDF, GPU и сеть не нужны: байты — синтетические файлы с сигнатурой ``%PDF``, порядок чтения
в тестах отбора не запускается (``--no-reading-order``), а двухколоночность задаётся
фактом ``DocFacts`` — прибор проверяется своим собственным набором тестов.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]


def _load_module():
    """Модуль инструмента по пути: `scripts/` не пакет."""
    path = _ROOT / "scripts" / "prepare_extraction_golden.py"
    spec = importlib.util.spec_from_file_location("prepare_extraction_golden", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["prepare_extraction_golden"] = module
    spec.loader.exec_module(module)
    return module


prepare = _load_module()


@pytest.fixture()
def xlsx() -> Any:
    """``openpyxl`` (extra ``extract``); без него XLSX-часть не проверяется."""
    return pytest.importorskip("openpyxl", reason="XLSX требует extra `extract`")


# ---------------------------------------------------------------------------
# Отбор: детерминизм и покрытие направлений
# ---------------------------------------------------------------------------
def _facts(
    doc_id: str,
    *,
    product: str = "p1",
    pages: int = 12,
    chars_per_page: float = 1500.0,
    partial: bool = False,
    two_column: bool | None = False,
    two_column_pages: int | None = 0,
    tables: bool | None = None,
    page_map: bool | None = None,
) -> Any:
    return prepare.DocFacts(
        doc_id=doc_id,
        product_id=product,
        url=f"https://example.test/{doc_id}",
        raw_path=f"/tmp/{doc_id}.pdf",
        pages=pages,
        chars_per_page=chars_per_page,
        chars_per_page_basis="page_count",
        extractor_version="docling-doc-4:full/cpu/x/y",
        pdf_pipeline="structured",
        partial=partial,
        two_column=two_column,
        two_column_pages=two_column_pages,
        tables=tables,
        page_map=page_map,
    )


def _pool(size: int = 30) -> list[Any]:
    """Пул без «интересных» документов: обычные многостраничные PDF двух продуктов."""
    documents = []
    for index in range(size):
        product = "p1" if index % 2 == 0 else "p2"
        digest = hashlib.sha256(f"doc-{index}".encode()).hexdigest()
        documents.append(
            _facts(f"{product}:{digest}", product=product, pages=20, chars_per_page=1200.0)
        )
    return documents


def test_selection_is_deterministic() -> None:
    """Тот же seed → тот же набор (иначе выборку нельзя пересчитать)."""
    pool = _pool()
    first = [item.case_id for item in prepare.select_documents(pool, 8, seed=0)]
    second = [item.case_id for item in prepare.select_documents(pool, 8, seed=0)]
    assert first == second
    assert len(first) == 8


def test_selection_depends_on_seed() -> None:
    """Другой seed — другой набор: seed не декоративный (проверяется на фиксированном пуле)."""
    pool = _pool()
    first = [item.case_id for item in prepare.select_documents(pool, 8, seed=0)]
    other = [item.case_id for item in prepare.select_documents(pool, 8, seed=1)]
    assert first != other


def test_two_column_document_is_always_selected() -> None:
    """Двухколоночный документ попадает в набор при любом seed и малом target.

    Иначе направление «порядок чтения» (O38б) измерялось бы на пустом множестве — ровно
    та ошибка, ради которой O37 писался.
    """
    pool = _pool(40)
    marker = _facts("p1:two_column", two_column=True, two_column_pages=3)
    pool.append(marker)
    for seed in (0, 1, 7):
        selected = {item.doc.doc_id for item in prepare.select_documents(pool, 6, seed=seed)}
        assert marker.doc_id in selected


def test_each_stratum_gets_a_seat() -> None:
    """Каждое направление с непустой стратой получает место даже в маленьком наборе."""
    pool = _pool(30)
    pool.append(_facts("p1:low", chars_per_page=10.0))
    pool.append(_facts("p1:partial", partial=True))
    pool.append(_facts("p1:tables", tables=True))
    pool.append(_facts("p1:nomap", page_map=False))
    selected = prepare.select_documents(pool, 6, seed=0)
    strata = {name for item in selected for name in item.strata}
    assert {"low_text", "partial", "tables", "no_page_map"} <= strata


def test_strata_are_derived_from_measurements() -> None:
    """Страта — измеренный факт: ``tables=False`` (читатель не даёт) не даёт страты."""
    assert prepare.matched_strata(_facts("p1:a", tables=False, page_map=True)) == ["multi_page"]
    assert prepare.matched_strata(_facts("p1:b", tables=True)) == ["tables", "multi_page"]


# ---------------------------------------------------------------------------
# Лист: колонки решений пусты
# ---------------------------------------------------------------------------
def test_candidate_rows_leave_decisions_empty() -> None:
    """Ни одно поле разметки не заполняется автоматически (граница D10 в коде)."""
    selected = prepare.select_documents(_pool(4), 4, seed=0)
    rows = prepare.candidate_rows(selected)
    assert len(rows) == 4
    for row in rows:
        for column in ("include", "completeness", "structure", "reading_order",
                       "identifiers", "provenance", "min_chars", "expected_substrings",
                       "forbidden_substrings", "expect_error", "notes"):
            assert row.get(column, "") == "", f"{column} заполнена инструментом"


def test_measured_columns_carry_numbers_not_guesses() -> None:
    """Измеренное заполняется, неизмеренное — пусто (не ``0`` и не ``no``)."""
    document = _facts("p1:x", two_column=None, two_column_pages=None, tables=None, page_map=None)
    row = prepare.candidate_rows([prepare.Selected(doc=document, strata=["multi_page"])])[0]
    assert row["two_column"] == ""
    assert row["tables"] == ""
    assert row["page_map"] == ""
    assert row["pages"] == "12"
    assert row["chars_per_page"] == "1500.0"


# ---------------------------------------------------------------------------
# compile: отказы
# ---------------------------------------------------------------------------
def _pdf(tmp_path: Path, name: str = "doc.pdf") -> Path:
    path = tmp_path / name
    path.write_bytes(b"%PDF-1.7\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF\n")
    return path


def _row(**overrides: str) -> dict[str, str]:
    row = {
        "case_id": "pdf-p1-1",
        "doc_id": "p1:1",
        "product_id": "p1",
        "url": "https://example.test/doc.pdf",
        "raw_path": "",
        "pages": "12",
        "extractor_version": "docling-doc-4:full/cpu/x/y",
        "pdf_pipeline": "structured",
        "two_column": "no",
        "two_column_pages": "0",
        "stratum": "multi_page",
        "why": "",
    }
    row.update(overrides)
    return row


def _sheet(path: Path, rows: list[dict[str, str]], policy: dict[str, Any]) -> Path:
    prepare.write_candidates_xlsx(
        path, rows, policy_rows=[(key, value) for key, value in policy.items()]
    )
    return path


def test_compile_refuses_unmarked_row(tmp_path: Path, xlsx: Any) -> None:
    """Строка без решения — отказ: набор без разметки не существует."""
    del xlsx
    sheet = _sheet(tmp_path / "s.xlsx", [_row(raw_path=str(_pdf(tmp_path)))], {"case_id": "case"})
    code = prepare.main(["compile", "--xlsx", str(sheet), "--out", str(tmp_path / "g.json")])
    assert code == 2
    assert not (tmp_path / "g.json").exists()


def test_compile_refuses_missing_bytes(tmp_path: Path, xlsx: Any, capsys: pytest.CaptureFixture[str]) -> None:
    """Байтов нет по ``raw_path`` — отказ: кейс, который нельзя прогнать, не кейс."""
    del xlsx
    row = _row(include="yes", completeness="full", min_chars="100",
               raw_path=str(tmp_path / "absent.pdf"))
    sheet = _sheet(tmp_path / "s.xlsx", [row], {"case_id": "case"})
    code = prepare.main(["compile", "--xlsx", str(sheet), "--out", str(tmp_path / "g.json")])
    captured = capsys.readouterr()
    assert code == 2
    assert "байтов нет" in captured.err


def test_compile_refuses_case_without_expectations(tmp_path: Path, xlsx: Any, capsys: pytest.CaptureFixture[str]) -> None:
    """Кейс без ожиданий — отказ: такой эталон проходит всё (дефект O27)."""
    del xlsx
    row = _row(include="yes", raw_path=str(_pdf(tmp_path)))
    sheet = _sheet(tmp_path / "s.xlsx", [row], {"case_id": "case"})
    code = prepare.main(["compile", "--xlsx", str(sheet), "--out", str(tmp_path / "g.json")])
    captured = capsys.readouterr()
    assert code == 2
    assert "без единого ожидания" in captured.err


def test_compile_refuses_full_without_min_chars(tmp_path: Path, xlsx: Any, capsys: pytest.CaptureFixture[str]) -> None:
    """``completeness=full`` без ``min_chars`` — «полнота» не проверяется ничем."""
    del xlsx
    row = _row(include="yes", completeness="full", expected_substrings="alpha",
               raw_path=str(_pdf(tmp_path)))
    sheet = _sheet(tmp_path / "s.xlsx", [row], {"case_id": "case"})
    code = prepare.main(["compile", "--xlsx", str(sheet), "--out", str(tmp_path / "g.json")])
    captured = capsys.readouterr()
    assert code == 2
    assert "min_chars" in captured.err


def test_compile_refuses_reading_order_against_instrument(tmp_path: Path, xlsx: Any, capsys: pytest.CaptureFixture[str]) -> None:
    """``reading_order=columns`` на документе, который прибор назвал не двухколоночным."""
    del xlsx
    row = _row(include="yes", reading_order="columns", expected_substrings="alpha",
               min_chars="10", raw_path=str(_pdf(tmp_path)))
    sheet = _sheet(tmp_path / "s.xlsx", [row], {"case_id": "case"})
    code = prepare.main(["compile", "--xlsx", str(sheet), "--out", str(tmp_path / "g.json")])
    captured = capsys.readouterr()
    assert code == 2
    assert "против измерения" in captured.err


def test_compile_refuses_when_nothing_accepted(tmp_path: Path, xlsx: Any, capsys: pytest.CaptureFixture[str]) -> None:
    """Пустой лист не превращается в пустой эталон."""
    del xlsx
    sheet = _sheet(tmp_path / "s.xlsx", [_row(include="no", raw_path=str(_pdf(tmp_path)))],
                   {"case_id": "case"})
    code = prepare.main(["compile", "--xlsx", str(sheet), "--out", str(tmp_path / "g.json")])
    captured = capsys.readouterr()
    assert code == 2
    assert "пустой эталон" in captured.err


# ---------------------------------------------------------------------------
# compile + validate: собранный набор
# ---------------------------------------------------------------------------
def test_compile_builds_set_and_validate_reads_it(
    tmp_path: Path, xlsx: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """Размеченный лист → набор формата ``cohound-extraction-golden/1``, читаемый прогоном."""
    del xlsx
    pdf = _pdf(tmp_path)
    row = _row(
        include="yes",
        completeness="full",
        structure="expect_tables",
        reading_order="skip",
        identifiers="expect",
        provenance="skip",
        min_chars="120",
        expected_substrings="alpha|beta",
        forbidden_substrings="Download PDF",
        notes="полнота и шапка таблицы",
        raw_path=str(pdf),
    )
    sheet = _sheet(
        tmp_path / "s.xlsx",
        [row],
        {"case_id": "case_x", "run_id": "run_x", "two_column_required": 1},
    )
    out = tmp_path / "extraction_golden.json"
    assert prepare.main(["compile", "--xlsx", str(sheet), "--out", str(out)]) == 0
    capsys.readouterr()

    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["format"] == "cohound-extraction-golden/1"
    assert payload["status"] == "candidate"
    assert payload["reviewed_by"] is None
    assert payload["case_id"] == "case_x"
    assert payload["run_id"] == "run_x"
    assert payload["cases"][0]["expected_substrings"] == ["alpha", "beta"]
    assert payload["cases"][0]["forbidden_substrings"] == ["Download PDF"]
    assert payload["cases"][0]["min_chars"] == 120
    assert payload["cases"][0]["doc_id"] == "p1:1"

    # Тот же загрузчик, которым пользуется прогон.
    from core.pipeline.extraction_eval import load_extraction_golden_document

    document = load_extraction_golden_document(out)
    assert document.status == "candidate"
    assert [case.case_id for case in document.cases] == ["pdf-p1-1"]
    assert document.cases[0].raw_path == str(pdf)

    assert prepare.main(["validate", "--golden", str(out)]) == 0
    printed = capsys.readouterr()
    assert "кейсов: 1" in printed.out
    assert "НЕ подтверждён человеком" in printed.out


def test_compile_warns_when_no_two_column_case(
    tmp_path: Path, xlsx: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    """Нет двухколоночного кейса — предупреждение, а не отказ: это свойство корпуса."""
    del xlsx
    row = _row(include="yes", min_chars="10", expected_substrings="alpha",
               raw_path=str(_pdf(tmp_path)))
    sheet = _sheet(
        tmp_path / "s.xlsx",
        [row],
        {"case_id": "case", "two_column_required": 1, "two_column_documents": 0},
    )
    out = tmp_path / "g.json"
    assert prepare.main(["compile", "--xlsx", str(sheet), "--out", str(out)]) == 0
    captured = capsys.readouterr()
    assert "ПРЕДУПРЕЖДЕНИЕ" in captured.err
    assert "двухколоночн" in captured.err
    assert json.loads(out.read_text(encoding="utf-8"))["cases"]


def test_validate_refuses_foreign_format(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Чужой конверт — отказ, а не «кейсов нет»."""
    path = tmp_path / "g.json"
    path.write_text(json.dumps({"format": "other/9", "cases": []}), encoding="utf-8")
    assert prepare.main(["validate", "--golden", str(path)]) == 2
    captured = capsys.readouterr()
    assert "чужой формат" in captured.err


# ---------------------------------------------------------------------------
# candidates: лист из корпуса
# ---------------------------------------------------------------------------
def _corpus_db(path: Path, raws: list[tuple[str, str]], *, with_page_map: bool = False) -> Path:
    """Синтетический run-DB: ``runs`` + ``corpus_docs`` (схема без поздних колонок)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE runs (run_id TEXT PRIMARY KEY, package_id TEXT)")
        connection.execute("INSERT INTO runs VALUES ('run_x', 'case_x')")
        connection.execute("CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("INSERT INTO schema_meta VALUES ('schema_version', '3.20.0')")
        columns = [
            "doc_id TEXT PRIMARY KEY",
            "product_id TEXT NOT NULL",
            "url TEXT NOT NULL",
            "raw_path TEXT",
            "page_count INTEGER",
            "pages_converted INTEGER",
            "extracted_chars INTEGER",
            "extractor_version TEXT",
            "partial INTEGER NOT NULL DEFAULT 0",
        ]
        if with_page_map:
            columns.append("page_map TEXT")
        connection.execute(f"CREATE TABLE corpus_docs ({', '.join(columns)})")  # noqa: S608
        for doc_id, raw_path in raws:
            connection.execute(
                "INSERT INTO corpus_docs (doc_id, product_id, url, raw_path, page_count,"
                " extracted_chars, extractor_version, partial)"
                " VALUES (?, 'p1', ?, ?, 12, 24000, 'docling-doc-4:full/cpu/x/y', 0)",
                (doc_id, f"https://example.test/{doc_id}", raw_path),
            )
        connection.commit()
    finally:
        connection.close()
    return path


def test_candidates_writes_unmarked_sheet(tmp_path: Path, xlsx: Any, capsys: pytest.CaptureFixture[str]) -> None:
    """Лист кандидатов: решения пусты, измеренное заполнено, схема прогона названа."""
    del xlsx
    pdf_a = _pdf(tmp_path, "a.pdf")
    pdf_b = _pdf(tmp_path, "b.pdf")
    other = tmp_path / "c.bin"
    other.write_bytes(b"not a pdf at all")
    db = _corpus_db(
        tmp_path / "runs" / "run_x" / "cohound.db",
        [("p1:a", str(pdf_a)), ("p1:b", str(pdf_b)), ("p1:c", str(other))],
    )
    out = tmp_path / "candidates.xlsx"
    code = prepare.main(
        [
            "candidates",
            "--db",
            str(db),
            "--target",
            "2",
            "--no-reading-order",
            "--out",
            str(out),
        ]
    )
    printed = capsys.readouterr()
    assert code == 0
    assert "НЕ идентифицированы прибором" in printed.out
    assert "не PDF 1" in printed.out

    rows = prepare.read_sheet(out, prepare.SHEET_CANDIDATES)
    assert len(rows) == 2
    for row in rows:
        assert row["include"] == ""
        assert row["reading_order"] == ""
        assert row["pages"] == "12"
        assert row["pdf_pipeline"] == "structured"
        assert row["two_column"] == ""

    policy = prepare.read_policy_sheet(out)
    assert policy["run_id"] == "run_x"
    assert policy["case_id"] == "case_x"
    assert policy["format"] == "cohound-extraction-golden/1"
    assert policy["status"] == "candidate"
    # Версия схемы корпуса — провенанс набора, а не украшение: прогон прошлой схемы
    # мог быть извлечён другим кодом.
    assert policy["schema_version"] == "3.20.0"
    # Схема прогона без поздних колонок: это видно, а не превращается в «нет».
    assert "page_map" in policy["missing_columns"]


def test_candidates_selection_is_reproducible(tmp_path: Path, xlsx: Any) -> None:
    """Два прогона с тем же seed дают тот же лист (тот же набор ``case_id``)."""
    del xlsx
    pdf_a = _pdf(tmp_path, "a.pdf")
    pdf_b = _pdf(tmp_path, "b.pdf")
    db = _corpus_db(
        tmp_path / "runs" / "run_x" / "cohound.db",
        [("p1:a", str(pdf_a)), ("p1:b", str(pdf_b))],
    )
    first = tmp_path / "first.xlsx"
    second = tmp_path / "second.xlsx"
    for out in (first, second):
        assert (
            prepare.main(
                ["candidates", "--db", str(db), "--target", "2", "--no-reading-order",
                 "--out", str(out)]
            )
            == 0
        )
    ids_first = [row["case_id"] for row in prepare.read_sheet(first, prepare.SHEET_CANDIDATES)]
    ids_second = [row["case_id"] for row in prepare.read_sheet(second, prepare.SHEET_CANDIDATES)]
    assert ids_first == ids_second
