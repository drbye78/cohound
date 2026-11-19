"""Пины прибора ``scripts/prepare_baseline_signatures.py`` (без сети, без стенда).

Прибор существует потому, что после пере-разведки 19.11.2026 слой фактов предка даёт
**255** `present`-строк на 8 версий, а наследует только `accepted`: без подписи человека
`baselines.csv` остаётся пустым. XLSX здесь **вход для человека**, поэтому проверяются
свойства, поломка которых даёт тихо неверную подпись, а не форма файла:

* ``low`` ловит цитату, которая не является утверждением: предметный указатель
  (`bookindex`), обрывок короче 25 символов и цитату без фразы параметра. Замер
  19.11.2026: 6 строк из `bookindex` и 22 коротких — это ровно те, что читать первыми;
* ``medium`` ловит однословную фразу: `vendor` зачёл параметр «Разработчик» цитатой о
  расширяемости — правило фразы этого не видит, потому что `parameter_phrases` содержит
  однословные элементы из `concept_terms`;
* ``compile`` отказывает на неразмеченной строке и на правке цитаты: подпись относится к
  доказательству, и подменить текст под ней нельзя;
* ``apply`` подписывает **набор** ключей одной записью и не трогает остальные строки:
  255 отдельных вызовов дали бы 255 записей журнала вместо одного решения.

Пакет синтетический, в ``tmp_path``; настоящие ``packages/`` не читаются.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts" / "prepare_baseline_signatures.py"
)


def _load_harness() -> Any:
    spec = importlib.util.spec_from_file_location("_prepare_baseline_signatures", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


SIGNATURES = _load_harness()

REFERENCE_YAML = """\
schema: reference_products/1
products:
  - baseline_id: upstream-pg
    product_id: postgresql
    canonical_name: PostgreSQL
    versions:
      - version: "18"
        docs_roots: ["https://www.postgresql.org/docs/18/"]
    current_version: "18"
"""


def _facts_csv(rows: list[dict[str, str]]) -> str:
    head = (
        "baseline_id,param_id,version,status,url,quote,content_hash,observed_at,"
        "corpus_build_id,review,reviewed_by,note\n"
    )
    lines = [head]
    for row in rows:
        lines.append(
            ",".join(
                [
                    "upstream-pg",
                    row["param_id"],
                    row.get("version", "18"),
                    row.get("status", "present"),
                    row.get("url", "https://www.postgresql.org/docs/18/func.html"),
                    '"' + row["quote"] + '"',
                    row.get("hash", "a" * 64),
                    "2026-11-19T00:00:00+00:00",
                    "ref_postgresql_18_deadbeef",
                    row.get("review", "pending"),
                    row.get("reviewed_by", ""),
                    "",
                ]
            )
            + "\n"
        )
    return "".join(lines)


def _package(tmp_path: Path, rows: list[dict[str, str]]) -> Path:
    package = tmp_path / "pkg"
    (package / "sources").mkdir(parents=True)
    (package / "package.yaml").write_text(
        "case_id: pkg\npackage_version: \"1.0.0\"\narchetype: A\n"
        "modules:\n  - module_id: m\n    weight: 100\nroles:\n  - role_id: r\n",
        encoding="utf-8",
    )
    (package / "sources" / "reference_products.yaml").write_text(
        REFERENCE_YAML, encoding="utf-8"
    )
    (package / "sources" / "baseline_facts.csv").write_text(
        _facts_csv(rows), encoding="utf-8"
    )
    (package / "catalog.csv").write_text(
        "param_id,title,module,group,importance,param_type,rule,absence_rule,"
        "role_binding,concept_terms,search_langs\n"
        'p_ok,Расширяемость,m,g,Кр,fact,"есть ✅, нет ❌",explicit,,ru=расширяемость,ru|en\n'
        'p_vendor,Разработчик,m,g,Кр,fact,"есть ✅, нет ❌",explicit,,'
        'ru=разработчик|производитель|вендор|продукта|vendor|dbms,ru|en\n'
        'p_head,LDAP-аутентификация,m,g,Кр,fact,"есть ✅, нет ❌",explicit,,'
        'ru=ldap|аутентификация|authentication,ru|en\n'
        'p_json,Расширенные возможности JSON,m,g,Кр,fact,"есть ✅, нет ❌",explicit,,'
        'ru=json_table|json,ru|en\n',
        encoding="utf-8",
    )
    (package / "baselines.csv").write_text(
        "baseline_id,param_id,min_version,inheritance_mode,note\n", encoding="utf-8"
    )
    return package


def _records(*overrides: dict[str, Any], **base_override: Any) -> list[dict[str, Any]]:
    base = {
        "n": 1,
        "param_id": "p_ok",
        "title": "Расширяемость",
        "version": "18",
        "quote": "conventional database systems can only be extended by changing hardware",
        "url": "https://www.postgresql.org/docs/18/extend.html",
        "section": "38.1. How Extensibility Works",
        "page_start": "By comparison, conventional database systems",
        "phrase": "расширяемость",
        "confidence": "high",
        "reason": "",
        "review": "pending",
        "reviewed_by": "",
        "note": "",
        "decision": "",
    }
    base = dict(base, **base_override)
    return [dict(base, **override) for override in overrides] or [base]


# ---------------------------------------------------------------------------
# build: что попадает в лист и как подсвечено
# ---------------------------------------------------------------------------


def test_build_lists_every_present_fact_and_skips_absent(tmp_path: Path) -> None:
    """В лист идут только `present`: `absent` ничего не даёт наследованию."""
    package = _package(
        tmp_path,
        [
            {"param_id": "p_ok", "quote": "the database can be extended by adding new types"},
            {"param_id": "p_vendor", "quote": "vendor"},
            {"param_id": "p_head", "status": "absent", "quote": ""},
        ],
    )
    records = SIGNATURES.build(
        package,
        baseline_id="upstream-pg",
        reference_root=None,
        versions=None,
        out_xlsx=tmp_path / "x.xlsx",
        out_data=tmp_path / "x.jsonl",
    )
    assert [record["param_id"] for record in records] == ["p_ok", "p_vendor"]
    assert (tmp_path / "x.xlsx").is_file()


def test_build_marks_index_page_as_low(tmp_path: Path) -> None:
    """Страница предметного указателя перечисляет главы, а не утверждает свойство."""
    package = _package(
        tmp_path,
        [
            {
                "param_id": "p_ok",
                "quote": "- point-in-time recovery, Backup and Restore",
                "url": "https://www.postgresql.org/docs/18/bookindex.html",
            }
        ],
    )
    records = SIGNATURES.build(
        package,
        baseline_id="upstream-pg",
        reference_root=None,
        versions=None,
        out_xlsx=tmp_path / "x.xlsx",
        out_data=tmp_path / "x.jsonl",
    )
    assert records[0]["confidence"] == "low"
    assert "указатель" in records[0]["reason"]


def test_build_marks_short_quote_as_low(tmp_path: Path) -> None:
    """`LDAP Authentication` — заголовок раздела, а не доказательство аутентификации."""
    package = _package(
        tmp_path,
        [{"param_id": "p_head", "quote": "LDAP Authentication"}],
    )
    records = SIGNATURES.build(
        package,
        baseline_id="upstream-pg",
        reference_root=None,
        versions=None,
        out_xlsx=tmp_path / "x.xlsx",
        out_data=tmp_path / "x.jsonl",
    )
    assert records[0]["confidence"] == "low"
    assert "короче" in records[0]["reason"]


def test_build_marks_single_generic_word_phrase_as_medium(tmp_path: Path) -> None:
    """`vendor` — общее слово, а не связка: цитата о расширяемости не доказывает вендора.

    Дефект измерен на пилоте 19.11.2026: `xlsx_001_razrabotchik` получил `present` по
    слову `vendor` цитатой «conventional database systems can only be extended…».
    """
    package = _package(
        tmp_path,
        [
            {
                "param_id": "p_vendor",
                "quote": "the vendor provides extensions for the database engine",
            }
        ],
    )
    records = SIGNATURES.build(
        package,
        baseline_id="upstream-pg",
        reference_root=None,
        versions=None,
        out_xlsx=tmp_path / "x.xlsx",
        out_data=tmp_path / "x.jsonl",
    )
    assert records[0]["confidence"] == "medium"
    assert "одно общее слово" in records[0]["reason"]


def test_build_keeps_specific_single_word_as_high(tmp_path: Path) -> None:
    """Имя функции с `_` отличает параметр и не подсвечивается: `JSON_TABLE` — не «слово»."""
    package = _package(
        tmp_path,
        [
            {
                "param_id": "p_json",
                "quote": "JSON_TABLE is an SQL/JSON function which queries JSON data",
            }
        ],
    )
    records = SIGNATURES.build(
        package,
        baseline_id="upstream-pg",
        reference_root=None,
        versions=None,
        out_xlsx=tmp_path / "x.xlsx",
        out_data=tmp_path / "x.jsonl",
    )
    assert records[0]["confidence"] == "high"
    assert records[0]["reason"] == ""


def test_build_marks_map_suspect_as_medium(tmp_path: Path) -> None:
    """Замечание карты («цитата не о предмете») обязано быть видно подписывающему.

    Карта — второй прибор на том же корпусе; её замечание не вердикт, но подписывать
    цитату, не увидев, что другой прибор её забраковал, значило бы спорить с измерением
    по незнанию (замер 19.11.2026: 5 принятых параметров из 13 подозреваемых).
    """
    package = _package(
        tmp_path,
        [
            {
                "param_id": "p_json",
                "quote": "JSON_TABLE is an SQL/JSON function which queries JSON data",
            }
        ],
    )
    map_dir = tmp_path / "map"
    map_dir.mkdir()
    (map_dir / "cells.jsonl").write_text(
        json.dumps(
            {
                "param_id": "p_json",
                "validation_notes": ["support_not_about_subject: 1/1 цитат не упоминают предмет"],
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    records = SIGNATURES.build(
        package,
        baseline_id="upstream-pg",
        reference_root=None,
        versions=None,
        out_xlsx=tmp_path / "x.xlsx",
        out_data=tmp_path / "x.jsonl",
        map_dir=map_dir,
    )
    assert records[0]["map_note"]
    assert records[0]["confidence"] == "medium"
    assert "карта" in records[0]["reason"]


def test_build_ignores_map_of_another_version(tmp_path: Path) -> None:
    """Карта @18 не судит о цитате @11: версия сравнивается по мажору."""
    package = _package(
        tmp_path,
        [
            {
                "param_id": "p_json",
                "version": "11",
                "quote": "JSON_TABLE is an SQL/JSON function which queries JSON data",
            }
        ],
    )
    map_dir = tmp_path / "map"
    map_dir.mkdir()
    (map_dir / "cells.jsonl").write_text(
        json.dumps(
            {"param_id": "p_json", "validation_notes": ["support_not_about_subject: 1/1"]},
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    records = SIGNATURES.build(
        package,
        baseline_id="upstream-pg",
        reference_root=None,
        versions=None,
        out_xlsx=tmp_path / "x.xlsx",
        out_data=tmp_path / "x.jsonl",
        map_dir=map_dir,
    )
    assert records[0]["map_note"] == ""
    assert records[0]["confidence"] == "high"


# ---------------------------------------------------------------------------
# compile: подпись нельзя подменить
# ---------------------------------------------------------------------------


def _fill(xlsx: Path, *, decision: str | None, quote: str | None = None) -> None:
    from openpyxl import load_workbook

    wb = load_workbook(xlsx)
    sheet = wb["signatures"]
    header = [str(cell.value or "") for cell in sheet[1]]
    index = {name: position for position, name in enumerate(header)}
    row = 2
    if quote is not None:
        sheet.cell(row=row, column=index["Цитата (дословно из документа)"] + 1, value=quote)
    if decision is not None:
        sheet.cell(
            row=row, column=index["Решение: accept / reject"] + 1, value=decision
        )
    wb.save(xlsx)


def test_compile_refuses_unfilled_decision(tmp_path: Path) -> None:
    """Пустое решение — «строку не смотрели», а не «согласен»: отказ, а не пропуск."""
    records = _records()
    xlsx = tmp_path / "x.xlsx"
    SIGNATURES._write_xlsx(records, xlsx)
    with pytest.raises(SystemExit, match="решение не заполнено"):
        SIGNATURES.compile_review(xlsx, records, tmp_path / "out.jsonl")


def test_compile_refuses_edited_quote(tmp_path: Path) -> None:
    """Цитата — доказательство: подпись под подменённым текстом недействительна."""
    records = _records()
    xlsx = tmp_path / "x.xlsx"
    SIGNATURES._write_xlsx(records, xlsx)
    _fill(xlsx, decision="accept", quote="другой текст")
    with pytest.raises(SystemExit, match="изменено поле quote"):
        SIGNATURES.compile_review(xlsx, records, tmp_path / "out.jsonl")


def test_compile_keeps_already_accepted_rows_unasked(tmp_path: Path) -> None:
    """Ранее подписанная строка не в очереди: требовать решение повторно — шум."""
    records = _records({"review": "accepted", "reviewed_by": "roger"})
    xlsx = tmp_path / "x.xlsx"
    SIGNATURES._write_xlsx(records, xlsx)
    counters = SIGNATURES.compile_review(xlsx, records, tmp_path / "out.jsonl")
    assert counters["already"] == 1
    stored = [
        json.loads(line)
        for line in (tmp_path / "out.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert stored[0]["decision"] == ""


def test_compile_applies_param_decision_to_all_versions(tmp_path: Path) -> None:
    """Решение по параметру (лист `by_param`) — на все его версии, но не на соседей.

    24 параметра пилота имеют `present` во всех восьми версиях; требовать 8 одинаковых
    решений значило бы превратить разметку в клики. Собственное решение строки сильнее.
    """
    from openpyxl import load_workbook

    records = _records(
        {"param_id": "p_ok", "version": "11", "n": 1},
        {"param_id": "p_ok", "version": "18", "n": 2},
        {"param_id": "p_vendor", "version": "18", "n": 3, "quote": "the vendor provides it"},
    )
    xlsx = tmp_path / "x.xlsx"
    SIGNATURES._write_xlsx(records, xlsx)
    wb = load_workbook(xlsx)
    sheet = wb["by_param"]
    header = [str(cell.value or "") for cell in sheet[1]]
    column = header.index("Решение по параметру (все версии)") + 1
    for row in sheet.iter_rows(min_row=2):
        if row[0].value == "p_ok":
            sheet.cell(row=row[0].row, column=column, value="accept")
        if row[0].value == "p_vendor":
            sheet.cell(row=row[0].row, column=column, value="reject")
    wb.save(xlsx)

    counters = SIGNATURES.compile_review(xlsx, records, tmp_path / "out.jsonl")
    assert counters["by_param"] == 3
    stored = {
        (item["param_id"], item["version"]): item["decision"]
        for item in (
            json.loads(line)
            for line in (tmp_path / "out.jsonl").read_text(encoding="utf-8").splitlines()
        )
    }
    assert stored == {("p_ok", "11"): "accept", ("p_ok", "18"): "accept", ("p_vendor", "18"): "reject"}


def test_row_decision_overrides_param_decision(tmp_path: Path) -> None:
    """Смешанный параметр решается построчно: `@11` не о предмете, `@18` о предмете."""
    from openpyxl import load_workbook

    records = _records(
        {"param_id": "p_ok", "version": "11", "n": 1},
        {"param_id": "p_ok", "version": "18", "n": 2},
    )
    xlsx = tmp_path / "x.xlsx"
    SIGNATURES._write_xlsx(records, xlsx)
    wb = load_workbook(xlsx)
    by_param = wb["by_param"]
    header = [str(cell.value or "") for cell in by_param[1]]
    column = header.index("Решение по параметру (все версии)") + 1
    for row in by_param.iter_rows(min_row=2):
        by_param.cell(row=row[0].row, column=column, value="accept")
    sheet = wb["signatures"]
    head = [str(cell.value or "") for cell in sheet[1]]
    index = {name: position for position, name in enumerate(head)}
    for row in sheet.iter_rows(min_row=2):
        if str(row[index["Версия"]].value) == "11":
            sheet.cell(
                row=row[0].row,
                column=index["Решение: accept / reject"] + 1,
                value="reject",
            )
    wb.save(xlsx)

    SIGNATURES.compile_review(xlsx, records, tmp_path / "out.jsonl")
    stored = {
        (item["param_id"], item["version"]): item["decision"]
        for item in (
            json.loads(line)
            for line in (tmp_path / "out.jsonl").read_text(encoding="utf-8").splitlines()
        )
    }
    assert stored == {("p_ok", "11"): "reject", ("p_ok", "18"): "accept"}


# ---------------------------------------------------------------------------
# apply: набор ключей, а не 255 вызовов
# ---------------------------------------------------------------------------


def test_apply_signs_only_given_keys(tmp_path: Path) -> None:
    """Подписывается ровно размеченное; соседние строки остаются `pending`."""
    from core.pipeline.reference_products import load_package_facts

    package = _package(
        tmp_path,
        [
            {"param_id": "p_ok", "quote": "the database can be extended by new types"},
            {"param_id": "p_vendor", "quote": "vendor"},
        ],
    )
    records = _records(decision="accept")
    result = SIGNATURES.apply(
        records, package, baseline_id="upstream-pg", actor="roger", dry_run=False
    )
    assert result["accept"] is not None
    assert len(result["accept"]["changed"]) == 1
    stored = {fact.param_id: fact for fact in load_package_facts(package)}
    assert stored["p_ok"].review.value == "accepted"
    assert stored["p_ok"].reviewed_by == "roger"
    assert stored["p_vendor"].review.value == "pending"


def test_apply_dry_run_writes_nothing(tmp_path: Path) -> None:
    """`--dry-run` не трогает файл: проверка решения не есть его запись."""
    from core.pipeline.reference_products import load_package_facts

    package = _package(tmp_path, [{"param_id": "p_ok", "quote": "extended by new types"}])
    before = (package / "sources" / "baseline_facts.csv").read_text(encoding="utf-8")
    SIGNATURES.apply(
        _records(decision="accept"), package, baseline_id="upstream-pg", actor="roger",
        dry_run=True,
    )
    assert (package / "sources" / "baseline_facts.csv").read_text(encoding="utf-8") == before
    assert load_package_facts(package)[0].review.value == "pending"
