"""Пины прибора ``scripts/prepare_parameter_semantics.py`` (без сети, без стенда).

Прибор существует потому, что `catalog.csv` профиля `ru-pg-dbms` собран генератором из
сравнительной таблицы, где каталога не было: `rule` — две захардкоженные строки,
`concept_terms` — разбиение `title`, `absence_rule` — `explicit` константой. Разметка
семантики — авторская работа, и XLSX здесь **вход для человека**, поэтому проверяются
свойства, поломка которых даёт тихо неверный вердикт, а не форма файла:

* ``validate`` отказывает на строке, чьё правило не описывает край шкалы: правило уходит
  в промпт оценщика слотом ``RULE``, и «✅ без ❌» означает, что модель не знает, за что
  ставить отрицание;
* ``validate`` отказывает на ``confidence=low`` без ``question``: красная строка без
  вопроса читается как «автор сомневается, но спросить забыл», и решение не принимается
  никем;
* ``validate`` отказывает на повторе термина **внутри** языка, но не между языками:
  латинская аббревиатура (`DBaaS`) законно стоит в обоих списках;
* ``build`` подсвечивает ровно ``low``/``medium`` и пишет лист исходных ячеек: без него
  человек сверяет разметку с таблицей по памяти;
* цена смены ``absence_rule`` печатается **из данных** (`golden.csv`), а не константой:
  иначе число «сколько ❌ перестанут публиковаться» устареет на первой же правке эталона;
* ``compile`` отказывает на незаполненном ``review`` и на правке без ``review=fix`` —
  иначе правка человека молча теряется либо, наоборот, проходит без его решения.

Пакет синтетический, в ``tmp_path``; настоящие ``packages/`` не читаются.
"""

from __future__ import annotations

import csv
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "prepare_parameter_semantics.py"
)


def _load_harness() -> Any:
    spec = importlib.util.spec_from_file_location("_prepare_parameter_semantics", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


SEMANTICS = _load_harness()

PRODUCTS = ["digital_q_database", "pangolin", "postgrespro", "tantor", "jatoba"]


def _record(n: int, **overrides: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "n": n,
        "param_id": f"xlsx_{n:03d}_param",
        "param_class": "capability",
        "confidence": "high",
        "absence_rule": "distribution_absence",
        "semantics": f"что спрашивает параметр {n}",
        "rule": "✅ — объявлено; ⚠️ — частично; ❌ — не заявлено.",
        "evidence": "руководство администратора",
        "terms_ru": [f"термин {n}"],
        "terms_en": [f"term {n}"],
        "question": "",
    }
    record.update(overrides)
    return record


def _write_package(tmp_path: Path, *, statuses: dict[int, list[str]] | None = None) -> Path:
    """Пакет из трёх параметров: каталог, эталон и исходные ячейки таблицы."""
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    with (package / "catalog.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "param_id", "title", "module", "group", "importance", "param_type",
            "rule", "absence_rule", "role_binding", "concept_terms", "search_langs",
        ])
        for n in (1, 2, 3):
            writer.writerow([
                f"xlsx_{n:03d}_param", f"Параметр {n}", "core", "gruppa", "Баз", "fact",
                "правило", "explicit", "core_engine", '{"en": [], "ru": []}', "ru|en",
            ])
    default = {1: ["✅"] * 5, 2: ["❌"] * 5, 3: ["⚠️"] * 5}
    with (package / "golden.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["product_id", "param_id", "expected_status"])
        for n in (1, 2, 3):
            marks = (statuses or default)[n]
            for product, mark in zip(PRODUCTS, marks, strict=True):
                writer.writerow([product, f"xlsx_{n:03d}_param", mark])
    with (package / "sources" / "golden_00_cells.csv").open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "product_id", "excel_number", "section", "excel_title", "raw_value",
            "normalized_status", "param_id", "source_url", "source_comment",
            "hyperlink_target",
        ])
        for n in (1, 2, 3):
            marks = (statuses or default)[n]
            for product, mark in zip(PRODUCTS, marks, strict=True):
                writer.writerow([
                    product, n, "Раздел", f"Параметр {n}", "ДА", mark,
                    f"xlsx_{n:03d}_param", "", "", "",
                ])
    return package


def _good_records() -> list[dict[str, Any]]:
    return [_record(1), _record(2), _record(3)]


def test_validate_accepts_a_complete_set(tmp_path: Path) -> None:
    package = _write_package(tmp_path)
    assert SEMANTICS.validate(_good_records(), package) == []


def test_validate_names_every_defect_it_finds(tmp_path: Path) -> None:
    """Каждый дефект — отдельная строка с номером: человек правит по адресу."""
    package = _write_package(tmp_path)
    records = [
        _record(1, rule="✅ — объявлено; ⚠️ — частично."),
        _record(2, absence_rule="net_takogo_klassa"),
        _record(3, confidence="low", question="", terms_ru=["термин", "термин"]),
    ]
    problems = SEMANTICS.validate(records, package)
    text = "\n".join(problems)
    assert "n=1: rule не описывает ❌" in text
    assert "n=2: absence_rule='net_takogo_klassa'" in text
    assert "n=3: confidence=low без question" in text
    assert "n=3: повтор термина в terms_ru" in text


def test_validate_does_not_require_marks_from_a_value_parameter(tmp_path: Path) -> None:
    """Параметр-значение отвечает величиной: ✅/❌ в его правиле нет по построению."""
    package = _write_package(tmp_path)
    records = _good_records()
    records[0]["param_class"] = "value"
    records[0]["rule"] = "Засчитывается явно названное значение."
    assert SEMANTICS.validate(records, package) == []


def test_validate_allows_one_acronym_in_both_languages(tmp_path: Path) -> None:
    """`DBaaS` — один термин для двух языков запроса, а не повтор."""
    package = _write_package(tmp_path)
    records = _good_records()
    records[0]["terms_ru"] = ["облачная СУБД", "DBaaS"]
    records[0]["terms_en"] = ["DBaaS", "cloud database service"]
    assert SEMANTICS.validate(records, package) == []


def test_validate_catches_a_missing_or_misaddressed_record(tmp_path: Path) -> None:
    package = _write_package(tmp_path)
    records = _good_records()[:2]
    problems = SEMANTICS.validate(records, package)
    assert "нет записей для номеров: [3]" in "\n".join(problems)

    records = _good_records()
    records[1]["param_id"] = "xlsx_002_chuzhoi"
    problems = SEMANTICS.validate(records, package)
    assert "n=2: param_id не совпадает с каталогом" in "\n".join(problems)


def test_build_highlights_confidence_and_carries_the_source_cells(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from openpyxl import load_workbook

    package = _write_package(tmp_path)
    records = _good_records()
    records[1]["confidence"] = "low"
    records[1]["question"] = "что именно считать механизмом?"
    records[2]["confidence"] = "medium"
    records[2]["question"] = "считать ли частичное выполнение?"
    out = tmp_path / "out.xlsx"

    assert SEMANTICS.main([
        "build", "--data", str(_write_jsonl(tmp_path, records)),
        "--package", str(package), "--out", str(out),
    ]) == 0

    wb = load_workbook(out)
    sheet = wb["semantics"]
    fills = {
        sheet.cell(row=row, column=1).value: sheet.cell(row=row, column=1).fill.fgColor.rgb
        for row in range(2, sheet.max_row + 1)
    }
    assert fills[1] == "00000000"
    assert fills[2] == "00FFC7CE"
    assert fills[3] == "00FFEB9C"
    assert wb["golden_00"].max_row == 1 + 3 * len(PRODUCTS)
    assert wb["legend"].max_row > 10

    printed = capsys.readouterr().out
    # 5 ❌ у параметра 2 — это и есть цена честного класса отсутствия.
    assert "❌ всего 5" in printed
    # С 0.4.0 подписи человека нет: класс bounded держится датированным носителем (I2d).
    assert "5 класса bounded" in printed
    assert "per-cell подписи нет" in printed


def test_build_legend_states_what_was_actually_reviewed(tmp_path: Path) -> None:
    """Легенда выводится из `review`, а не из имени файла.

    `candidate` в имени XLSX переживает подпись: без этого строка «человек её не
    подтверждал» стоит рядом с 181 подтверждённой строкой — та же ошибка, что документ
    старше артефакта (§1.2 AGENTS).
    """
    from openpyxl import load_workbook

    package = _write_package(tmp_path)
    records = _good_records()

    def legend_first(rows: list[dict[str, Any]], name: str) -> str:
        out = tmp_path / name
        assert SEMANTICS.main([
            "build", "--data", str(_write_jsonl(tmp_path, rows)),
            "--package", str(package), "--out", str(out),
        ]) == 0
        return str(load_workbook(out)["legend"]["A1"].value)

    assert legend_first(records, "candidate.xlsx") == SEMANTICS.STATE_CANDIDATE

    for record in records:
        record["review"] = "accept"
    reviewed = legend_first(records, "reviewed.xlsx")
    assert "reviewed" in reviewed and "все 3" in reviewed

    records[0]["review"] = "fix"
    records[2]["review"] = ""
    partial = legend_first(records, "partial.xlsx")
    assert "частично reviewed" in partial and "2 из 3" in partial


def test_build_carries_the_review_back_into_the_sheet(tmp_path: Path) -> None:
    """Пересборка XLSX после `compile` не стирает решение человека.

    `compile` — единственный вход в JSONL, а `build` — единственный выход в XLSX. Если
    build пишет пустой `review`, то пересборка артефакта выглядит как «никто не читал»,
    и следующая подпись начинается с нуля.
    """
    from openpyxl import load_workbook

    package = _write_package(tmp_path)
    records = _good_records()
    records[0]["review"] = "fix"
    records[0]["review_note"] = "поправлена семантика: термин вендора"
    records[1]["review"] = "accept"
    out = tmp_path / "out.xlsx"
    assert SEMANTICS.main([
        "build", "--data", str(_write_jsonl(tmp_path, records)),
        "--package", str(package), "--out", str(out),
    ]) == 0

    sheet = load_workbook(out)["semantics"]
    header = [cell.value for cell in sheet[1]]
    review_at = header.index("review") + 1
    note_at = header.index("review_note") + 1
    assert sheet.cell(row=2, column=review_at).value == "fix"
    assert sheet.cell(row=2, column=note_at).value == "поправлена семантика: термин вендора"
    assert sheet.cell(row=3, column=review_at).value == "accept"
    assert sheet.cell(row=4, column=review_at).value in (None, "")


def test_build_counts_the_price_of_the_absence_class_from_the_golden(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Число берётся из `golden.csv`, а не из константы: другой эталон — другое число."""
    package = _write_package(tmp_path, statuses={1: ["❌"] * 5, 2: ["✅"] * 5, 3: ["✅"] * 5})
    records = _good_records()
    records[2]["absence_rule"] = "registry_absence"
    assert SEMANTICS.main([
        "build", "--data", str(_write_jsonl(tmp_path, records)),
        "--package", str(package), "--out", str(tmp_path / "out.xlsx"),
    ]) == 0
    printed = capsys.readouterr().out
    assert "❌ всего 5" in printed
    assert "registry_absence 0" in printed


def _write_jsonl(tmp_path: Path, records: list[dict[str, Any]]) -> Path:
    path = tmp_path / "semantics.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return path


def test_compile_refuses_an_unreviewed_row_and_an_edit_without_fix(tmp_path: Path) -> None:
    from openpyxl import load_workbook

    package = _write_package(tmp_path)
    data = _write_jsonl(tmp_path, _good_records())
    out = tmp_path / "reviewed.jsonl"
    xlsx = tmp_path / "review.xlsx"
    assert SEMANTICS.main([
        "build", "--data", str(data), "--package", str(package), "--out", str(xlsx),
    ]) == 0

    with pytest.raises(SystemExit) as excinfo:
        SEMANTICS.compile_review(xlsx, _good_records(), out)
    assert "review не заполнен" in str(excinfo.value)

    wb = load_workbook(xlsx)
    sheet = wb["semantics"]
    header = [cell.value for cell in sheet[1]]
    review_column = header.index("review") + 1
    rule_column = header.index("Правило зачёта ✅ / ⚠️ / ❌") + 1
    for row in range(2, sheet.max_row + 1):
        sheet.cell(row=row, column=review_column, value="accept")
    sheet.cell(row=2, column=rule_column, value="подменённое правило ✅ ❌")
    wb.save(xlsx)

    with pytest.raises(SystemExit) as excinfo:
        SEMANTICS.compile_review(xlsx, _good_records(), out)
    assert "изменено ['rule'] без review=fix" in str(excinfo.value)


def test_compile_applies_a_reviewed_edit_and_keeps_the_rest(tmp_path: Path) -> None:
    from openpyxl import load_workbook

    package = _write_package(tmp_path)
    data = _write_jsonl(tmp_path, _good_records())
    out = tmp_path / "reviewed.jsonl"
    xlsx = tmp_path / "review.xlsx"
    assert SEMANTICS.main([
        "build", "--data", str(data), "--package", str(package), "--out", str(xlsx),
    ]) == 0

    wb = load_workbook(xlsx)
    sheet = wb["semantics"]
    header = [cell.value for cell in sheet[1]]
    review_column = header.index("review") + 1
    rule_column = header.index("Правило зачёта ✅ / ⚠️ / ❌") + 1
    for row in range(2, sheet.max_row + 1):
        sheet.cell(row=row, column=review_column, value="accept")
    sheet.cell(row=2, column=rule_column, value="✅ — есть; ⚠️ — часть; ❌ — нет. Проверено.")
    sheet.cell(row=2, column=review_column, value="fix")
    wb.save(xlsx)

    SEMANTICS.compile_review(xlsx, _good_records(), out)
    merged = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert [record["n"] for record in merged] == [1, 2, 3]
    assert merged[0]["rule"].endswith("Проверено.")
    assert merged[0]["review"] == "fix"
    assert merged[1]["rule"] == "✅ — объявлено; ⚠️ — частично; ❌ — не заявлено."
    assert merged[1]["review"] == "accept"


def test_apply_rewrites_only_the_semantic_columns(tmp_path: Path) -> None:
    """Прибор не пересчитывает состав пакета: важность и роль — отдельное решение.

    `catalog_digest` входит в `run_id`, поэтому «заодно» переписанная важность дала бы
    новый `run_id` без нового решения человека.
    """
    package = _write_package(tmp_path)
    records = _good_records()
    for record in records:
        record["review"] = "accept"
    records[0]["rule"] = "✅ — вендор назван; ❌ — не назван."
    records[0]["terms_ru"] = ["производитель СУБД"]
    records[0]["terms_en"] = ["vendor"]
    out = tmp_path / "catalog.csv"

    SEMANTICS.apply_catalog(records, package, out)

    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert len(rows) == 3
    assert rows[0]["rule"] == "✅ — вендор назван; ❌ — не назван."
    assert json.loads(rows[0]["concept_terms"]) == {
        "ru": ["производитель СУБД"], "en": ["vendor"]
    }
    # Не тронуто: состав пакета остался прежним.
    assert (rows[0]["module"], rows[0]["group"], rows[0]["importance"]) == (
        "core", "gruppa", "Баз"
    )
    assert rows[0]["param_type"] == "fact"
    assert rows[0]["role_binding"] == "core_engine"


def test_apply_refuses_records_without_a_human_decision(tmp_path: Path) -> None:
    package = _write_package(tmp_path)
    with pytest.raises(SystemExit) as excinfo:
        SEMANTICS.apply_catalog(_good_records(), package, tmp_path / "catalog.csv")
    message = str(excinfo.value)
    assert "apply отказал (3)" in message
    assert "review=None" in message
