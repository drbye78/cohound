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

import csv
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


# ---------------------------------------------------------------------------
# Правило C (только объявленный хост) и правило A (разрешение по цитате)
# ---------------------------------------------------------------------------
#: Паспорт минимальной формы §4.3: для правил нужны только ``doc_domains``.
def _passports(tmp_path: Path, domains: list[str]) -> Path:
    """Каталог паспортов: один продукт ``PRODUCT`` с объявленными ``doc_domains``."""
    root = tmp_path / "passports"
    (root / PRODUCT).mkdir(parents=True, exist_ok=True)
    lines = [
        f"product_id: {PRODUCT}",
        'canonical_name: "Docs Test"',
        'vendor: "Test"',
        "jurisdiction: RU",
        "snapshot_date: 2026-01-01",
        "doc_domains:",
        *[f'  - "{domain}"' for domain in domains],
    ]
    (root / PRODUCT / "2026-01-01.yaml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return root


def _declared(tmp_path: Path, domains: list[str]) -> object:
    return PREP.DeclaredHosts(_passports(tmp_path, domains), {PRODUCT})  # type: ignore[attr-defined]


def test_foreign_host_when_subdomain_is_not_declared(tmp_path: Path) -> None:
    """Поддомен входит в scope только если объявлен — правило краула, а не «похожий хост»."""
    declared = _declared(tmp_path, ["docs.test"])
    assert declared.is_declared(PRODUCT, "https://docs.test/tune")  # type: ignore[attr-defined]
    assert not declared.is_declared(  # type: ignore[attr-defined]
        PRODUCT, "https://api.docs.test/tune"
    )


def test_www_is_equivalent_to_bare_host(tmp_path: Path) -> None:
    """``www.`` ≡ без ``www.`` в обе стороны: корпус пишет одну форму, таблица — другую."""
    declared = _declared(tmp_path, ["docs.test"])
    assert declared.is_declared(PRODUCT, "https://www.docs.test/tune")  # type: ignore[attr-defined]
    declared_www = _declared(tmp_path, ["www.docs.test"])
    assert declared_www.is_declared(PRODUCT, "https://docs.test/tune")  # type: ignore[attr-defined]


def test_declared_subdomain_passes(tmp_path: Path) -> None:
    """Объявленный поддомен проходит: правило C — про объявление, а не про глубину."""
    declared = _declared(tmp_path, ["docs.test", "api.docs.test"])
    assert declared.is_declared(PRODUCT, "https://api.docs.test/tune")  # type: ignore[attr-defined]


def test_missing_passport_is_a_visible_error(tmp_path: Path) -> None:
    """Продукт без паспорта — отказ с именем продукта, а не тихое отключение правила C."""
    root = _passports(tmp_path, ["docs.test"])
    with pytest.raises(SystemExit) as excinfo:
        PREP.DeclaredHosts(root, {PRODUCT, "no-such-product"})  # type: ignore[attr-defined]
    assert "no-such-product" in str(excinfo.value)


# --- корпус с чанками: цитаты ищутся по тексту документов -------------------
CHUNKS_FIELDS = ("product_id", "doc_id", "chunk_no", "text")


def _corpus_db(tmp_path: Path) -> Path:
    """Минимальный run-DB: документы, чанки, доказательства и таблицы покрытия.

    Чанки — точный срез текста документа (как в корпусе прогона), поэтому документ из
    двух чанков восстанавливается их склейкой в порядке ``chunk_no``.
    """
    path = tmp_path / "cohound.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE corpus_docs (product_id TEXT, url TEXT, doc_id TEXT)")
        conn.execute(
            "CREATE TABLE corpus_chunks (product_id TEXT, doc_id TEXT, chunk_no INT, text TEXT)"
        )
        conn.execute(
            "CREATE TABLE cell_evidence (product_id TEXT, param_id TEXT, citation_quote TEXT)"
        )
        conn.execute(
            "CREATE TABLE corpus_coverage (product_id TEXT, roots_json TEXT, truncated INT,"
            " unreadable_origins_json TEXT)"
        )
        conn.execute(
            "CREATE TABLE crawl_session_items (product_id TEXT, url TEXT, status TEXT,"
            " http_status INT, error_kind TEXT)"
        )
        conn.execute("CREATE TABLE runs (crawl_policy_json TEXT)")
        conn.executemany(
            "INSERT INTO corpus_docs VALUES (?, ?, ?)",
            [
                (PRODUCT, "https://docs.test/guide", f"{PRODUCT}:guide"),
                (PRODUCT, "https://docs.test/other", f"{PRODUCT}:other"),
                (PRODUCT, "https://docs.test/tune", f"{PRODUCT}:tune"),
            ],
        )
        conn.executemany(
            "INSERT INTO corpus_chunks VALUES (?, ?, ?, ?)",
            [
                (
                    PRODUCT,
                    f"{PRODUCT}:guide",
                    0,
                    "Настройка pg_basebackup описана здесь. \\_escape остаётся в тексте."
                    " Выберите версию, форк и язык.",
                ),
                (
                    PRODUCT,
                    f"{PRODUCT}:other",
                    0,
                    "Здесь описана настройка pg_basebackup. Выберите версию, форк и язык.",
                ),
                (PRODUCT, f"{PRODUCT}:tune", 0, "Раздел про тюнинг."),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return path


def _quote_index(db: Path, needles: dict[tuple[str, str], tuple[str, ...]]) -> object:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return PREP.QuoteIndex.build(conn, needles)  # type: ignore[attr-defined]
    finally:
        conn.close()


def _classify(db: Path, url: str, *, domains: list[str], quotes: tuple[str, ...]) -> object:
    """Классификация пары на фикстуре: правило C и правило A, без покрытия и политики."""
    corpus = PREP.CorpusIndex(db)  # type: ignore[attr-defined]
    try:
        index = _quote_index(db, {(PRODUCT, url): quotes})
        return PREP.classify(  # type: ignore[attr-defined]
            corpus,
            {},
            {},
            {},
            {},
            PRODUCT,
            url,
            declared=_declared(db.parent, domains),
            quotes=quotes,
            quote_index=index,
        )
    finally:
        corpus.close()


def test_foreign_host_pair_is_not_searched(tmp_path: Path) -> None:
    """Хост не объявлен → ``foreign_host``: кандидата не ищем даже при совпадающей цитате."""
    db = _corpus_db(tmp_path)
    verdict = _classify(
        db, "https://github.com/x/y", domains=["docs.test"], quotes=("Раздел про тюнинг.",)
    )
    assert verdict.status == "foreign_host"  # type: ignore[attr-defined]
    assert verdict.doc_id == ""  # type: ignore[attr-defined]


def test_all_quotes_in_one_document_resolve(tmp_path: Path) -> None:
    """Все цитаты пары в одном документе → ``moved_quote`` с адресом, sha256 и счётом."""
    db = _corpus_db(tmp_path)
    verdict = _classify(
        db,
        "https://docs.test/guide-v17",
        domains=["docs.test"],
        quotes=("Настройка pg_basebackup описана здесь.", "\\_escape остаётся в тексте."),
    )
    assert verdict.status == "resolved"  # type: ignore[attr-defined]
    assert verdict.match == "moved_quote"  # type: ignore[attr-defined]
    assert verdict.corpus_url == "https://docs.test/guide"  # type: ignore[attr-defined]
    assert verdict.doc_id == f"{PRODUCT}:guide"  # type: ignore[attr-defined]
    assert "quotes=2/2" in verdict.note  # type: ignore[attr-defined]
    assert "sha256=" in verdict.note  # type: ignore[attr-defined]
    assert "version=" in verdict.note  # type: ignore[attr-defined]


def test_quotes_split_across_documents_are_ambiguous(tmp_path: Path) -> None:
    """Цитаты разошлись по двум документам → ``ambiguous`` с числом и счётом по каждому."""
    db = _corpus_db(tmp_path)
    verdict = _classify(
        db,
        "https://docs.test/guide-v17",
        domains=["docs.test"],
        quotes=("Раздел про тюнинг.", "Здесь описана настройка pg_basebackup."),
    )
    assert verdict.status == "ambiguous"  # type: ignore[attr-defined]
    assert verdict.match == "quote"  # type: ignore[attr-defined]
    assert verdict.quote_docs == 2  # type: ignore[attr-defined]
    assert "2 docs" in verdict.note  # type: ignore[attr-defined]
    assert "1/2" in verdict.note  # type: ignore[attr-defined]


def test_quote_found_in_two_documents_is_ambiguous(tmp_path: Path) -> None:
    """Одна цитата в двух документах — тоже неоднозначность, а не выбор первого."""
    db = _corpus_db(tmp_path)
    verdict = _classify(
        db,
        "https://docs.test/guide-v17",
        domains=["docs.test"],
        quotes=("Выберите версию, форк и язык.",),
    )
    assert verdict.status == "ambiguous"  # type: ignore[attr-defined]
    assert verdict.quote_docs == 2  # type: ignore[attr-defined]
    assert "2 docs" in verdict.note  # type: ignore[attr-defined]


def test_quote_not_found_is_missing(tmp_path: Path) -> None:
    """Цитаты нет ни в одном документе продукта → ``missing``/``quote_not_found``."""
    db = _corpus_db(tmp_path)
    verdict = _classify(
        db, "https://docs.test/guide-v17", domains=["docs.test"], quotes=("Такого текста нет.",)
    )
    assert verdict.status == "missing"  # type: ignore[attr-defined]
    assert verdict.note.startswith("quote_not_found")  # type: ignore[attr-defined]


def test_partially_found_quotes_do_not_resolve(tmp_path: Path) -> None:
    """Часть цитат найдена, часть нет — не разрешение: в примечании видно, где нашлась."""
    db = _corpus_db(tmp_path)
    verdict = _classify(
        db,
        "https://docs.test/guide-v17",
        domains=["docs.test"],
        quotes=("Раздел про тюнинг.", "Такого текста нет."),
    )
    assert verdict.status == "missing"  # type: ignore[attr-defined]
    assert "quote_not_found:found=1/2" in verdict.note  # type: ignore[attr-defined]


def test_escape_normalization_matches_the_chunks_instrument(tmp_path: Path) -> None:
    """Нормализация одна на два прибора: одна и та же строка даёт один и тот же результат.

    Расхождение здесь означало бы, что документный прибор находит цитату там, где
    чанковый её не видит, — и оба отчёта описывали бы разные корпуса.
    """
    chunks_script = (
        Path(__file__).resolve().parents[2] / "scripts" / "prepare_retrieval_golden_chunks.py"
    )
    spec = importlib.util.spec_from_file_location("_prepare_retrieval_golden_chunks", chunks_script)
    assert spec is not None and spec.loader is not None
    chunks = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = chunks
    spec.loader.exec_module(chunks)
    sample = "  pg\\_basebackup\n\n  и\tещё  "
    assert chunks.normalize(sample) == PREP.normalize(sample)  # type: ignore[attr-defined]
    # И смысл тот же, что замерен O16: экранированный underscore снимается.
    assert PREP.normalize(sample)[0] == "pg_basebackup и ещё"  # type: ignore[attr-defined]


# --- validate: новые виды совпадения ---------------------------------------
def _golden_entry(**overrides: object) -> dict[str, object]:
    entry: dict[str, object] = {
        "cell": f"{PRODUCT}/param.a",
        "product_id": PRODUCT,
        "param_id": "param.a",
        "source_url": "https://docs.test/guide-v17",
        "expected_doc_ids": [f"{PRODUCT}:guide"],
        "match": "moved_quote",
        "corpus_url": "https://docs.test/guide",
    }
    entry.update(overrides)
    return entry


def _validate(db: Path, entries: list[dict[str, object]]) -> list[str]:
    payload = {
        "format": PREP.GOLDEN_FORMAT,  # type: ignore[attr-defined]
        "case_id": "c",
        "source": "s",
        "corpus_run_id": "r",
        "entries": entries,
    }
    corpus = PREP.CorpusIndex(db)  # type: ignore[attr-defined]
    try:
        return PREP.validate_payload(payload, corpus)  # type: ignore[attr-defined]
    finally:
        corpus.close()


def test_validate_accepts_moved_quote_with_corpus_url(tmp_path: Path) -> None:
    """``moved_quote`` с адресом найденного документа проходит: адрес не равен source_url."""
    assert _validate(_corpus_db(tmp_path), [_golden_entry()]) == []


def test_validate_refuses_moved_quote_without_corpus_url(tmp_path: Path) -> None:
    """Без ``corpus_url`` запись не говорит ничего проверяемого — отказ."""
    entry = _golden_entry()
    del entry["corpus_url"]
    problems = _validate(_corpus_db(tmp_path), [entry])
    assert any("без corpus_url" in problem for problem in problems)


def test_validate_refuses_corpus_url_of_another_product(tmp_path: Path) -> None:
    """Адрес чужого продукта — отказ (I17): ретривал продукт-скоупед по построению."""
    db = _corpus_db(tmp_path)
    conn = sqlite3.connect(db)
    try:
        conn.execute(
            "INSERT INTO corpus_docs VALUES (?, ?, ?)",
            ("p2", "https://other.test/guide", "p2:guide"),
        )
        conn.commit()
    finally:
        conn.close()
    entry = _golden_entry(corpus_url="https://other.test/guide", expected_doc_ids=["p2:guide"])
    problems = _validate(db, [entry])
    assert any("I17" in problem for problem in problems)


def test_validate_refuses_foreign_host_match(tmp_path: Path) -> None:
    """``match=foreign_host`` в эталон не входит: прибор не выбирает документ по такой паре."""
    problems = _validate(_corpus_db(tmp_path), [_golden_entry(match="foreign_host")])
    assert any("foreign_host" in problem for problem in problems)


# --- build: правило A включается и выключается -----------------------------
def _cells_for_build(tmp_path: Path) -> Path:
    path = tmp_path / "cells.csv"
    rows = [
        (PRODUCT, "param.a", "https://docs.test/guide-v17", "✅"),
        (PRODUCT, "param.b", "https://docs.test/tune", "✅"),
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["product_id", "param_id", "source_url", "normalized_status"],
            lineterminator="\n",
        )
        writer.writeheader()
        for product_id, param_id, url, status in rows:
            writer.writerow(
                {
                    "product_id": product_id,
                    "param_id": param_id,
                    "source_url": url,
                    "normalized_status": status,
                }
            )
    return path


def _build(tmp_path: Path, db: Path, *, quotes: bool) -> list[dict[str, str]]:
    conn = sqlite3.connect(db)
    try:
        conn.executemany(
            "INSERT INTO cell_evidence VALUES (?, ?, ?)",
            [
                (PRODUCT, "param.a", "Настройка pg_basebackup описана здесь."),
                (PRODUCT, "param.b", "Раздел про тюнинг."),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    out = tmp_path / ("with_quotes.csv" if quotes else "no_quotes.csv")
    argv = [
        "build",
        "--cells",
        str(_cells_for_build(tmp_path)),
        "--run-db",
        str(db),
        "--passports-dir",
        str(_passports(tmp_path, ["docs.test"])),
        "--out",
        str(out),
    ]
    if not quotes:
        argv.append("--no-quotes")
    assert PREP.main(argv) == 0  # type: ignore[attr-defined]
    return list(csv.DictReader(out.open(encoding="utf-8")))


def test_no_quotes_reproduces_address_only_classification(tmp_path: Path) -> None:
    """``--no-quotes`` даёт прежнюю классификацию: адрес и ничего больше.

    Пин на фикстуре: пара, разрешимая только по цитате, при ``--no-quotes`` остаётся
    ``missing``, а адресная остаётся ``exact`` — иначе старые числа были бы
    невоспроизводимы, а это единственный способ их сравнить.
    """
    db = _corpus_db(tmp_path)
    without = {row["source_url"]: row for row in _build(tmp_path, db, quotes=False)}
    assert without["https://docs.test/guide-v17"]["status"] == "missing"
    assert without["https://docs.test/tune"]["match"] == "exact"
    with_quotes = {row["source_url"]: row for row in _build(tmp_path, db, quotes=True)}
    assert with_quotes["https://docs.test/guide-v17"]["match"] == "moved_quote"
    assert with_quotes["https://docs.test/guide-v17"]["status"] == "resolved"
    assert with_quotes["https://docs.test/tune"]["match"] == "exact"


# --- лист новых пар и compile ---------------------------------------------
def test_annotate_writes_moved_sheet_only_for_new_pairs(tmp_path: Path) -> None:
    """Лист новых пар — только ``moved_quote`` и цитатная ``ambiguous``, с числом и текстом.

    Читать решение по 43 новым парам в листе на 287 строк значит листать вслепую; при
    этом лист обязан быть **входом**, а не украшением, поэтому решения из него
    переносятся обратно (проверяется следующим тестом).
    """
    from openpyxl import load_workbook

    rows = [
        _row(
            param_ids="param.a",
            source_url="https://docs.test/guide-v17",
            status="resolved",
            match="moved_quote",
            corpus_url="https://docs.test/guide",
            doc_id=f"{PRODUCT}:guide",
            quotes_found="2",
            quotes_total="2",
            quote_docs="1",
            quote_len="36",
            quote_sample="Настройка pg_basebackup описана здесь.",
        ),
        _row(
            param_ids="param.b",
            source_url="https://docs.test/other-v1",
            status="ambiguous",
            match="quote",
            quotes_found="2",
            quotes_total="2",
            quote_docs="2",
            quote_len="21",
            quote_sample="Раздел про тюнинг.",
        ),
        _row(
            param_ids="param.c",
            status="resolved",
            match="exact",
            doc_id=DOC_MAIN,
        ),
    ]
    out = tmp_path / "sheet.xlsx"
    moved = tmp_path / "moved.xlsx"
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
                "--moved-out",
                str(moved),
            ]
        )
        == 0
    )
    book = load_workbook(moved)
    sheet = book["новые пары"]
    header = [cell.value for cell in sheet[1]]
    assert header[:4] == ["product_id", "source_url", "corpus_url", "match"]
    assert "quote_sample" in header and "decision" in header
    assert sheet.max_row == 3  # шапка + две новые пары, exact не попал
    matches = {sheet.cell(row=index, column=header.index("match") + 1).value for index in (2, 3)}
    assert matches == {"moved_quote", "quote"}
    assert book.sheetnames == ["новые пары", "как размечать"]
    book.close()


def test_annotate_carries_decision_from_moved_sheet(tmp_path: Path) -> None:
    """Решение, поставленное в листе новых пар, доезжает до основного листа и до compile."""
    from openpyxl import load_workbook

    rows = [
        _row(
            param_ids="param.a",
            source_url="https://docs.test/guide-v17",
            status="resolved",
            match="moved_quote",
            corpus_url="https://docs.test/guide",
            doc_id=f"{PRODUCT}:guide",
        )
    ]
    out = tmp_path / "sheet.xlsx"
    moved = tmp_path / "moved.xlsx"
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
                "--moved-out",
                str(moved),
            ]
        )
        == 0
    )
    book = load_workbook(moved)
    ws = book["новые пары"]
    header = {cell.value: cell.column for cell in ws[1]}
    ws.cell(row=2, column=header["decision"]).value = "accept"
    book.save(moved)
    book.close()

    assert (
        PREP.main(  # type: ignore[attr-defined]
            [
                "annotate",
                "--candidate",
                str(out),
                "--cells",
                str(_cells_csv(tmp_path)),
                "--out",
                str(out),
                "--moved-out",
                str(moved),
            ]
        )
        == 0
    )
    sheet = load_workbook(out)["лист"]
    columns = {cell.value: cell.column for cell in sheet[1]}
    assert sheet.cell(row=2, column=columns["decision"]).value == "accept"


def test_compile_records_corpus_url_and_refuses_foreign_host(
    tmp_path: Path, corpus_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``moved_quote`` несёт ``corpus_url`` в эталон; ``foreign_host`` с accept — отказ."""
    conn = sqlite3.connect(corpus_db)
    try:
        conn.execute(
            "INSERT INTO corpus_docs (product_id, url, doc_id) VALUES (?, ?, ?)",
            (PRODUCT, "https://docs.test/guide", f"{PRODUCT}:guide"),
        )
        conn.commit()
    finally:
        conn.close()
    out = tmp_path / "retrieval_golden_docs.json"
    moved = _row(
        param_ids="param.a",
        source_url="https://docs.test/guide-v17",
        status="resolved",
        match="moved_quote",
        corpus_url="https://docs.test/guide",
        doc_id=f"{PRODUCT}:guide",
        decision="accept",
    )
    assert (
        PREP.main(  # type: ignore[attr-defined]
            [
                "compile",
                "--candidate",
                str(_write(tmp_path, [moved])),
                "--out",
                str(out),
                "--run-db",
                str(corpus_db),
            ]
        )
        == 0
    )
    import json

    entry = json.loads(out.read_text(encoding="utf-8"))["entries"][0]
    assert entry["match"] == "moved_quote"
    assert entry["corpus_url"] == "https://docs.test/guide"

    foreign = _row(
        param_ids="param.b",
        source_url="https://github.com/x/y",
        status="foreign_host",
        decision="accept",
    )
    assert (
        PREP.main(  # type: ignore[attr-defined]
            [
                "compile",
                "--candidate",
                str(_write(tmp_path, [foreign])),
                "--out",
                str(tmp_path / "other.json"),
                "--run-db",
                str(corpus_db),
            ]
        )
        == 2
    )
    assert "foreign_host" in capsys.readouterr().err
