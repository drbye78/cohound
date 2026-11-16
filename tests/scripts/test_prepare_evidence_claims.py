"""Пины прибора ``scripts/prepare_evidence_claims.py`` (без стенда, без LLM, без сети).

Проверяются ровно те свойства, на которых держится публикуемое число «доля ``no``» (O18),
— на синтетическом прогоне, где ответ известен заранее:

* **детерминизм**: две сборки на одной БД дают один набор ``evidence_id`` (иначе лист
  нельзя пересобрать, а число — воспроизвести);
* **стратификация**: квоты пропорциональны составу прогона, а ``❓`` в лист не попадает
  (у неё нет утверждения, которое цитата могла бы поддержать);
* **одна ячейка — не больше одного evidence**: без этого правила 50 строк описывали бы
  единицы параметров, и доля ``no`` говорила бы о выборке, а не о карте;
* ``validate`` ловит пустой/неизвестный ``supports``, дубль evidence и неверное число
  строк — иначе опечатка в разметке молча становится числом;
* ``summary`` считает доли от **размеченных**, а не от всех строк: недозаполненный лист
  не должен занижать долю шума;
* отказ без БД и отказ на пустой выборке — код 2 и путь в сообщении.

БД синтетическая: ``sqlite3`` создаётся в ``tmp_path``, каталог параметров —
``plan_inputs.json`` рядом с ней. Стенд ``~/.cohound`` и настоящие пакеты не читаются.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "prepare_evidence_claims.py"
)


def _load_harness() -> Any:
    spec = importlib.util.spec_from_file_location("_prepare_evidence_claims", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


CLAIMS = _load_harness()

OK = "✅"
PARTIAL = "⚠️"
ABSENT = "❌"
UNKNOWN = "❓"

RUN_ID = "run_synthetic"

#: Минимальный DDL: только колонки, которые прибор действительно читает
#: (``build_rows`` → ``cells``/``cell_evidence``/``corpus_chunks``, ``_single_run`` → ``runs``).
DDL = """
CREATE TABLE runs (run_id TEXT PRIMARY KEY);
CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE cells (
    run_id TEXT, product_id TEXT, param_id TEXT, status TEXT,
    justification TEXT, retrieval_profile TEXT
);
CREATE TABLE cell_evidence (
    evidence_id TEXT, run_id TEXT, product_id TEXT, param_id TEXT,
    source_kind TEXT, evidence_type TEXT, source_url TEXT,
    citation_quote TEXT, content_hash TEXT, doc_version_id TEXT
);
CREATE TABLE corpus_chunks (
    chunk_id TEXT PRIMARY KEY, content_hash TEXT, url TEXT, text TEXT
);
"""


def _make_run(
    tmp_path: Path,
    *,
    cells: list[tuple[str, str, str, str]],
    evidence: list[tuple[str, str, str, str, str]],
    chunks: list[tuple[str, str, str]] | None = None,
    params: list[dict[str, str]] | None = None,
) -> Path:
    """Синтетический прогон: ``cells``/``evidence`` — кортежи в порядке DDL.

    ``evidence`` = ``(evidence_id, product_id, param_id, citation_quote, content_hash)``,
    ``chunks`` = ``(chunk_id, content_hash, text)``. Каталог параметров — ``plan_inputs.json``
    рядом с БД: прибор читает снимок прогона, а не пакет.
    """
    db = tmp_path / "cohound.db"
    conn = sqlite3.connect(db)
    try:
        conn.executescript(DDL)
        conn.execute("INSERT INTO runs VALUES (?)", (RUN_ID,))
        conn.execute("INSERT INTO schema_meta VALUES ('schema_version', '3.25.0')")
        conn.executemany(
            "INSERT INTO cells VALUES (?, ?, ?, ?, ?, ?)",
            [(RUN_ID, product, param, status, justify, "R0") for product, param, status, justify in cells],
        )
        conn.executemany(
            "INSERT INTO cell_evidence VALUES (?, ?, ?, ?, 'documentation', 'direct',"
            " 'https://example.invalid/doc', ?, ?, NULL)",
            [
                (evidence_id, RUN_ID, product, param, quote, digest)
                for evidence_id, product, param, quote, digest in evidence
            ],
        )
        for chunk_id, digest, text in chunks or []:
            conn.execute(
                "INSERT INTO corpus_chunks VALUES (?, ?, 'https://example.invalid/doc', ?)",
                (chunk_id, digest, text),
            )
        conn.commit()
    finally:
        conn.close()
    payload = {"params": params if params is not None else []}
    (tmp_path / "plan_inputs.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )
    return db


def _status_grid() -> list[tuple[str, str, str, str]]:
    """Ячейки прогона «✅ 10, ⚠️ 6, ❌ 2, ❓ 5» — состав, на котором квоты видны насквозь."""
    cells: list[tuple[str, str, str, str]] = []
    for status, count in ((OK, 10), (PARTIAL, 6), (ABSENT, 2), (UNKNOWN, 5)):
        for index in range(count):
            param = f"{status}_p{index:02d}"
            cells.append(("prod", param, status, f"утверждение для {param}"))
    return cells


def _evidence_for(cells: list[tuple[str, str, str, str]]) -> list[tuple[str, str, str, str, str]]:
    """По одному evidence на ячейку: цитата лежит в чанке своего хеша."""
    out: list[tuple[str, str, str, str, str]] = []
    for index, (_product, param, _status, _justify) in enumerate(cells, start=1):
        quote = f"цитата для {param}, достаточно длинная для контекста"
        out.append((f"ev_{index:04d}", "prod", param, quote, f"hash{index:04d}"))
    return out


def _chunks_for(evidence: list[tuple[str, str, str, str, str]]) -> list[tuple[str, str, str]]:
    return [
        (f"chunk_{index:04d}", digest, f"вступление. {quote}. продолжение")
        for index, (_eid, _product, _param, quote, digest) in enumerate(evidence, start=1)
    ]


def _build(db: Path, out: Path, sample: int) -> int:
    return CLAIMS.main(["build", "--db", str(db), "--out", str(out), "--sample", str(sample)])


def _ids(path: Path) -> list[str]:
    return [row["evidence_id"] for row in CLAIMS.read_xlsx(path)]


def _fill(path: Path, values: list[str]) -> None:
    """Проставляет ``supports`` построчно (openpyxl), не трогая машинные колонки."""
    openpyxl = pytest.importorskip("openpyxl")
    book = openpyxl.load_workbook(path)
    try:
        sheet = book["лист"]
        column = CLAIMS.COLUMNS.index("supports") + 1
        for offset, value in enumerate(values, start=2):
            sheet.cell(row=offset, column=column).value = value
        book.save(path)
    finally:
        book.close()


def _set(path: Path, row: int, column_name: str, value: str) -> None:
    openpyxl = pytest.importorskip("openpyxl")
    book = openpyxl.load_workbook(path)
    try:
        sheet = book["лист"]
        sheet.cell(row=row, column=CLAIMS.COLUMNS.index(column_name) + 1).value = value
        book.save(path)
    finally:
        book.close()


# -- детерминизм --------------------------------------------------------------


def test_build_is_deterministic(tmp_path: Path) -> None:
    """Две сборки на одной БД дают один набор ``evidence_id``.

    Закрывает дефект «повторная сборка — другая выборка»: если отбор опирается на
    ``random`` или на порядок строк БД, число «доля no» нельзя ни воспроизвести, ни
    перепроверить другим человеком. Сравниваются строки, а не файл: XLSX хранит
    временные метки, и побайтовое равенство здесь недостижимо в принципе.
    """
    cells = _status_grid()
    evidence = _evidence_for(cells)
    db = _make_run(
        tmp_path, cells=cells, evidence=evidence, chunks=_chunks_for(evidence)
    )
    first_path = tmp_path / "first.xlsx"
    second_path = tmp_path / "second.xlsx"

    assert _build(db, first_path, 9) == 0
    assert _build(db, second_path, 9) == 0

    first = CLAIMS.read_xlsx(first_path)
    second = CLAIMS.read_xlsx(second_path)
    assert first == second
    assert len(_ids(first_path)) == 9


# -- стратификация ------------------------------------------------------------


def test_stratified_quotas_and_unknown_excluded(tmp_path: Path) -> None:
    """Квоты пропорциональны составу прогона, ``❓`` в выборку не попадает.

    Закрывает два дефекта сразу. Первый: без стратификации выборка из 9 строк на прогоне,
    где ``❌`` всего 2 из 18, либо не содержала бы ``❌`` вовсе, либо перепредставляла бы
    их — и доля ``no`` описывала бы не карту, а случайность отбора. Второй: ``❓`` не имеет
    утверждения (I12), поэтому цитата из неё нечего проверять — попадание ``❓`` в лист
    дало бы строку, на которую разметчик не может ответить.
    """
    cells = _status_grid()  # ✅ 10, ⚠️ 6, ❌ 2, ❓ 5
    evidence = _evidence_for(cells)
    db = _make_run(
        tmp_path, cells=cells, evidence=evidence, chunks=_chunks_for(evidence)
    )
    out = tmp_path / "sheet.xlsx"

    assert _build(db, out, 9) == 0

    rows = CLAIMS.read_xlsx(out)
    assert len(rows) == 9
    distribution = Counter(row["status"] for row in rows)
    # 9 × (10/18, 6/18, 2/18) = 5, 3, 1 — квоты целые, перераспределения нет.
    assert distribution == Counter({OK: 5, PARTIAL: 3, ABSENT: 1})
    assert UNKNOWN not in distribution
    # Каждая строка — своя ячейка: страта не добирается повторами.
    assert len({(row["product_id"], row["param_id"]) for row in rows}) == 9


def test_one_evidence_per_cell(tmp_path: Path) -> None:
    """У ячейки с пятью evidence в листе ровно одна строка — и это первый по ``evidence_id``.

    Закрывает дефект «50 строк описали 8 параметров»: evidence у одной ячейки — это
    повторы одного и того же утверждения, и их доля в выборке не должна зависеть от того,
    сколько цитат оценщик приложил к ячейке. Синтетика подобрана так, что без дедупликации
    равномерный шаг **внутри страты** берёт две строки одной ячейки (индексы 0 и 3 из 7) и
    теряет вторую ячейку: дефект виден в листе, а не только в коде.
    """
    cells = [
        ("prod", "p_ok_a", OK, "утверждение A"),
        ("prod", "p_ok_b", OK, "утверждение B"),
        ("prod", "p_ok_c", OK, "утверждение C"),
        ("prod", "p_absent_1", ABSENT, "отсутствие 1"),
        ("prod", "p_absent_2", ABSENT, "отсутствие 2"),
        ("prod", "p_absent_3", ABSENT, "отсутствие 3"),
    ]
    evidence = [
        (f"ev_{index:04d}", "prod", "p_ok_a", f"цитата A номер {index} про одно и то же", "hash_a")
        for index in range(1, 6)
    ] + [
        ("ev_0006", "prod", "p_ok_b", "цитата B, тоже достаточно длинная", "hash_b"),
        ("ev_0007", "prod", "p_ok_c", "цитата C, тоже достаточно длинная", "hash_c"),
        ("ev_0008", "prod", "p_absent_1", "цитата отсутствия 1, длинная", "hash_a1"),
        ("ev_0009", "prod", "p_absent_2", "цитата отсутствия 2, длинная", "hash_a2"),
        ("ev_0010", "prod", "p_absent_3", "цитата отсутствия 3, длинная", "hash_a3"),
    ]
    chunks = [
        ("chunk_a", "hash_a", "вступление. цитата A номер 1 про одно и то же. продолжение"),
        ("chunk_b", "hash_b", "вступление. цитата B, тоже достаточно длинная. продолжение"),
        ("chunk_c", "hash_c", "вступление. цитата C, тоже достаточно длинная. продолжение"),
        ("chunk_a1", "hash_a1", "вступление. цитата отсутствия 1, длинная. продолжение"),
        ("chunk_a2", "hash_a2", "вступление. цитата отсутствия 2, длинная. продолжение"),
        ("chunk_a3", "hash_a3", "вступление. цитата отсутствия 3, длинная. продолжение"),
    ]
    db = _make_run(tmp_path, cells=cells, evidence=evidence, chunks=chunks)
    out = tmp_path / "sheet.xlsx"

    assert _build(db, out, 6) == 0

    rows = CLAIMS.read_xlsx(out)
    assert len(rows) == 6
    cells_in_sheet = [(row["product_id"], row["param_id"]) for row in rows]
    assert len(set(cells_in_sheet)) == 6
    ok_rows = [row for row in rows if row["status"] == OK]
    assert {row["param_id"] for row in ok_rows} == {"p_ok_a", "p_ok_b", "p_ok_c"}
    assert [row["evidence_id"] for row in ok_rows if row["param_id"] == "p_ok_a"] == ["ev_0001"]



# -- validate -----------------------------------------------------------------


@pytest.fixture()
def filled_sheet(tmp_path: Path) -> Path:
    """Заполненный лист на 9 строк: валидный по построению, портится точечно."""
    cells = _status_grid()
    evidence = _evidence_for(cells)
    db = _make_run(
        tmp_path, cells=cells, evidence=evidence, chunks=_chunks_for(evidence)
    )
    out = tmp_path / "sheet.xlsx"
    assert _build(db, out, 9) == 0
    _fill(out, ["yes", "partial", "no"] * 3)
    return out


def test_validate_accepts_complete_sheet(filled_sheet: Path, capsys: Any) -> None:
    """Годный лист проходит: иначе отказ ``validate`` нельзя отличить от дефекта листа."""
    code = CLAIMS.main(["validate", "--xlsx", str(filled_sheet), "--expect", "9"])
    captured = capsys.readouterr()
    assert code == 0
    assert "OK:" in captured.out


def test_validate_rejects_empty_supports(filled_sheet: Path, capsys: Any) -> None:
    """Пустой ``supports`` — отказ с кодом 2: неразмеченная строка не должна попасть в счёт.

    Закрывает дефект «пустое значение прочиталось как ответ»: ``summary`` считает доли от
    размеченных, поэтому молча пропущенная пустая строка уменьшила бы знаменатель и
    завысила долю ``no``.
    """
    _set(filled_sheet, 3, "supports", "")
    code = CLAIMS.main(["validate", "--xlsx", str(filled_sheet), "--expect", "9"])
    captured = capsys.readouterr()
    assert code == 2
    assert "supports не заполнен" in captured.err


def test_validate_rejects_unknown_supports(filled_sheet: Path, capsys: Any) -> None:
    """Значение вне ``{yes,partial,no}`` — отказ: опечатка не должна стать новым классом.

    Закрывает дефект «``summary`` молча отбросил незнакомое значение»: тогда ``yes``,
    напечатанное как ``Yes``, исчезло бы из числителя и знаменателя, а отчёт остался бы
    правдоподобным.
    """
    _set(filled_sheet, 3, "supports", "maybe")
    code = CLAIMS.main(["validate", "--xlsx", str(filled_sheet), "--expect", "9"])
    captured = capsys.readouterr()
    assert code == 2
    assert "не из" in captured.err


def test_validate_rejects_duplicate_evidence(filled_sheet: Path, capsys: Any) -> None:
    """Дубль ``evidence_id`` — отказ; адрес ячейки при этом подменён, чтобы ловился именно дубль.

    Закрывает дефект «одна и та же цитата размечена дважды и посчитана дважды»: доля
    ``no`` тогда зависит от копирования строк, а не от доказательной силы.
    """
    openpyxl = pytest.importorskip("openpyxl")
    book = openpyxl.load_workbook(filled_sheet)
    try:
        sheet = book["лист"]
        duplicate = str(sheet.cell(row=2, column=1).value)
        sheet.cell(row=4, column=1).value = duplicate
        sheet.cell(row=4, column=2).value = "prod_other"
        sheet.cell(row=4, column=3).value = "p_other"
        book.save(filled_sheet)
    finally:
        book.close()

    code = CLAIMS.main(["validate", "--xlsx", str(filled_sheet), "--expect", "9"])
    captured = capsys.readouterr()
    assert code == 2
    assert "дубль evidence_id" in captured.err


def test_validate_rejects_wrong_row_count(filled_sheet: Path, capsys: Any) -> None:
    """Число строк сверяется с объявленным: лист из 9 строк при ``--expect 10`` — отказ.

    Закрывает дефект «часть листа потерялась при пересылке»: без этой сверки доля ``no``
    считалась бы по тому, что доехало, и расхождение с ``--sample`` осталось бы незамеченным.
    """
    code = CLAIMS.main(["validate", "--xlsx", str(filled_sheet), "--expect", "10"])
    captured = capsys.readouterr()
    assert code == 2
    assert "строк 9, ожидалось 10" in captured.err


# -- summary ------------------------------------------------------------------


def test_summary_shares_are_of_marked_rows(filled_sheet: Path, capsys: Any) -> None:
    """Доли считаются от **размеченных**: 4 ``yes`` + 2 ``no`` из 6 → ``no`` 0.3333, не 0.2222.

    Закрывает дефект «доля ``no`` от всех строк листа»: лист размечен на 6 строк из 9, и
    деление на 9 дало бы 0.2222 вместо 0.3333 — число шума зависело бы от скорости
    разметчика, а не от доказательной силы цитат. Разница здесь нарочно видна в четвёртом
    знаке: именно её и публикует ``summary``.
    """
    _fill(filled_sheet, ["yes"] * 4 + ["no"] * 2 + [""] * 3)
    code = CLAIMS.main(["summary", "--xlsx", str(filled_sheet)])
    captured = capsys.readouterr()
    assert code == 0
    assert "размечено 6 из 9" in captured.out
    assert "no 2 = 0.3333" in captured.out
    assert "partial 0 = 0.0000" in captured.out


# -- отказы -------------------------------------------------------------------


def test_build_refuses_without_db(tmp_path: Path, capsys: Any) -> None:
    """Нет БД — код 2 и путь в сообщении: прибор не должен молча создать пустой лист."""
    missing = tmp_path / "absent" / "cohound.db"
    code = CLAIMS.main(
        ["build", "--db", str(missing), "--out", str(tmp_path / "x.xlsx"), "--sample", "9"]
    )
    captured = capsys.readouterr()
    assert code == 2
    assert "БД не найдена" in captured.err
    assert str(missing) in captured.err
    assert not (tmp_path / "x.xlsx").exists()


def test_build_refuses_empty_selection(tmp_path: Path, capsys: Any) -> None:
    """Нет ни одного evidence с цитатой — отказ с кодом 2 и числами, а не пустой лист.

    Закрывает дефект «прибор выдал XLSX на 0 строк и код 0»: пустая выборка читалась бы
    как «шума нет», хотя измерять было нечего (I22 отбрасывает пустую цитату).
    """
    cells = _status_grid()
    evidence = [
        (f"ev_{index:04d}", "prod", param, "", "hash")
        for index, (_product, param, _status, _justify) in enumerate(cells, start=1)
    ]
    db = _make_run(tmp_path, cells=cells, evidence=evidence)
    out = tmp_path / "sheet.xlsx"

    code = _build(db, out, 9)
    captured = capsys.readouterr()
    assert code == 2
    assert "выборка 0 вместо 9" in captured.err
    assert not out.exists()
