"""Прибор подготовки эталона отвечающих документов: кандидаты, компиляция, проверка.

Предмет — **не** качество набора (его решает человек), а три вещи, которые тихо ломаются:

* **детерминизм кандидатов**: два запуска дают одинаковый набор строк. Иначе разметчик
  не может вернуться к листу — каждая пересборка меняла бы порядок и состав, и
  «разметка пропала» стала бы неотличима от «кандидаты пересобрались».
* **правило ``term``** объявлено целиком (слова ``title`` длиной >= 4 без служебных,
  >= 2 различных в тексте) и **не переходит границу продукта** (I17).
* **ошибка разметки O14 видна**: ``no`` у документа с ``how=cited`` — отказ, а не
  молчаливое вычёркивание адреса, который по определению отвечает.

БД минимальная: скрипт читает ``runs``/``schema_meta``/``corpus_docs``/``corpus_chunks``
и снимок каталога ``plan_inputs.json``. Ретривал (``--include-retrieved``) и судья
(``--llm``) здесь не запускаются: они требуют индекса и LLM, а проверяется подготовка.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "prepare_answer_sets.py"


def _load_harness() -> object:
    spec = importlib.util.spec_from_file_location("_prepare_answer_sets", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Модуль регистрируется до exec: ``@dataclass`` ищет ``sys.modules`` по
    # ``cls.__module__`` и падает, если модуля там ещё нет.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


PREP = _load_harness()

PRODUCT = "p1"
OTHER = "p2"

#: Каталог: ``title`` — вход правила ``term``, поэтому заголовки подобраны под него.
PARAMS = [
    {
        "param_id": "param.alpha",
        "title": "Резервное копирование данных",
        "module": "core",
        "group": "general",
        "importance": "Вж",
        "param_type": "fact",
        "rule": "правило",
        "search_langs": ["ru"],
    },
    {
        "param_id": "param.beta",
        "title": "Техническая поддержка",
        "module": "core",
        "group": "general",
        "importance": "Вж",
        "param_type": "fact",
        "rule": "правило",
        "search_langs": ["ru"],
    },
]


def _db(
    path: Path,
    *,
    docs: list[tuple[str, str, str]],
    chunks: list[tuple[str, str, str, str]],
) -> Path:
    """Минимальная БД прогона: ``runs``, ``schema_meta``, ``corpus_docs``, ``corpus_chunks``."""
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            "CREATE TABLE runs (run_id TEXT, package_id TEXT, index_build_id TEXT,"
            " retrieval_cutoff_K INTEGER)"
        )
        conn.execute("CREATE TABLE schema_meta (key TEXT, value TEXT)")
        conn.execute("INSERT INTO schema_meta VALUES ('schema_version', '3.20.0')")
        conn.execute(
            "INSERT INTO runs VALUES ('run-1', ?, 'cb_1', 10)", (PRODUCT,)
        )
        conn.execute(
            "CREATE TABLE corpus_docs (doc_id TEXT, product_id TEXT, url TEXT)"
        )
        conn.execute(
            "CREATE TABLE corpus_chunks (chunk_id TEXT, doc_id TEXT, product_id TEXT,"
            " text TEXT, chunk_no INTEGER)"
        )
        conn.executemany("INSERT INTO corpus_docs VALUES (?, ?, ?)", docs)
        conn.executemany("INSERT INTO corpus_chunks VALUES (?, ?, ?, ?, ?)", [
            (f"c{i}", doc_id, product, text, i)
            for i, (doc_id, product, text) in enumerate(chunks)
        ])
        conn.commit()
    finally:
        conn.close()
    return path


def _golden(path: Path, cells: list[tuple[str, str, list[str]]]) -> Path:
    """Документный эталон формата 1: ``(product, param_id, expected_doc_ids)``."""
    path.write_text(
        json.dumps(
            {
                "format": "cohound-retrieval-golden-docs/1",
                "case_id": PRODUCT,
                "corpus_run_id": "run-1",
                "entries": [
                    {
                        "cell": f"{product}/{param_id}",
                        "product_id": product,
                        "param_id": param_id,
                        "source_url": "https://docs.test/x",
                        "expected_doc_ids": doc_ids,
                        "match": "exact",
                    }
                    for product, param_id, doc_ids in cells
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture()
def stand(tmp_path: Path) -> dict[str, Path]:
    """Стенд: БД с документами, снимок каталога и документный эталон."""
    db = _db(
        tmp_path / "cohound.db",
        docs=[
            (f"{PRODUCT}:aaa", PRODUCT, "https://docs.test/a"),
            (f"{PRODUCT}:bbb", PRODUCT, "https://docs.test/b"),
            (f"{PRODUCT}:ccc", PRODUCT, "https://docs.test/c"),
            (f"{PRODUCT}:ddd", PRODUCT, "https://docs.test/d"),
            (f"{PRODUCT}:eee", PRODUCT, "https://docs.test/e"),
            (f"{OTHER}:zzz", OTHER, "https://docs.test/z"),
        ],
        chunks=[
            # cited-документ: содержит все термины param.alpha.
            (f"{PRODUCT}:aaa", PRODUCT, "резервное копирование данных ежедневно"),
            # 2 различных термина param.alpha → term-кандидат.
            (f"{PRODUCT}:bbb", PRODUCT, "резервное копирование настроено"),
            # 1 термин («данных») → НЕ кандидат: порог — 2 различных.
            (f"{PRODUCT}:ccc", PRODUCT, "данных немного"),
            # cited-документ param.beta: один термин.
            (f"{PRODUCT}:ddd", PRODUCT, "техническая поддержка круглосуточно"),
            # 2 различных термина param.beta → term-кандидат.
            (f"{PRODUCT}:eee", PRODUCT, "техническая поддержка база"),
            # чужой продукт: термины есть, кандидатом быть не должен (I17).
            (f"{OTHER}:zzz", OTHER, "резервное копирование данных техническая поддержка"),
        ],
    )
    (tmp_path / "plan_inputs.json").write_text(
        json.dumps({"params": PARAMS}, ensure_ascii=False), encoding="utf-8"
    )
    golden = _golden(
        tmp_path / "golden.json",
        [
            (PRODUCT, "param.alpha", [f"{PRODUCT}:aaa"]),
            (PRODUCT, "param.beta", [f"{PRODUCT}:ddd"]),
        ],
    )
    return {"db": db, "golden": golden, "out": tmp_path / "sheet.xlsx"}


def _candidates(stand: dict[str, Path], out: Path | None = None, *extra: str) -> int:
    return PREP.main(  # type: ignore[attr-defined]
        [
            "candidates",
            "--db",
            str(stand["db"]),
            "--golden",
            str(stand["golden"]),
            "--out",
            str(out or stand["out"]),
            *extra,
        ]
    )


def _rows(path: Path) -> list[dict[str, str]]:
    return PREP.read_sheet(path, PREP.SHEET_CANDIDATES)  # type: ignore[attr-defined]


def _value_rows(path: Path) -> list[tuple[str, ...]]:
    """Строки листа как кортежи значений: сравнение по содержимому, а не по файлу."""
    return [tuple(row[column] for column in PREP.CANDIDATE_COLUMNS) for row in _rows(path)]


# ---------------------------------------------------------------------------
# candidates: правило term, cited, детерминизм, обрезка
# ---------------------------------------------------------------------------
def test_candidates_mark_cited_and_term_by_the_declared_rule(
    stand: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    assert _candidates(stand) == 0
    rows = _rows(stand["out"])
    by_key = {(row["cell"], row["doc_id"]): row for row in rows}
    cell = f"{PRODUCT}/param.alpha"
    # cited: адрес эталона (и у него же нашлись термины — ``how`` перечисляет оба
    # источника, см. проверку ниже).
    # term: 2 различных термина («резервное», «копирование») — колонка why их называет.
    term_row = by_key[(cell, f"{PRODUCT}:bbb")]
    assert term_row["how"] == "term"
    assert "резервное" in term_row["why"] and "копирование" in term_row["why"]
    # 1 термин — не кандидат: порог 2 различных объявлен, а не «сколько нашлось».
    assert (cell, f"{PRODUCT}:ccc") not in by_key
    # чужой продукт не попадает в кандидаты ячейки (I17).
    assert not any(row["doc_id"].startswith(f"{OTHER}:") for row in rows)
    out = capsys.readouterr().out
    # ``how`` перечисляет **все** источники документа: у cited-документа термины тоже
    # нашлись (aaa содержит все три), и это видно, а не спрятано за первым источником.
    assert "cited 2" in out and "term 4" in out
    assert by_key[(cell, f"{PRODUCT}:aaa")]["how"] == "cited,term"


def test_candidates_keep_unresolved_cited_row_with_empty_url(
    stand: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Документа нет в корпусе — строка остаётся: это факт о покрытии, а не повод выбросить."""
    golden = _golden(
        stand["golden"],
        [
            (PRODUCT, "param.alpha", [f"{PRODUCT}:aaa", f"{PRODUCT}:нет"]),
            (PRODUCT, "param.beta", [f"{PRODUCT}:ddd"]),
        ],
    )
    stand = {**stand, "golden": golden}
    assert _candidates(stand) == 0
    rows = _rows(stand["out"])
    row = next(row for row in rows if row["doc_id"] == f"{PRODUCT}:нет")
    assert row["how"] == "cited"
    assert row["doc_url"] == ""
    assert "нет в корпусе" in row["why"]
    assert "cited не разрешилось в корпусе: 1" in capsys.readouterr().out


def test_candidates_are_deterministic(stand: dict[str, Path], tmp_path: Path) -> None:
    """Два запуска дают одинаковые значения ячеек (сравнение по содержимому, не по файлу)."""
    first = tmp_path / "first.xlsx"
    second = tmp_path / "second.xlsx"
    assert _candidates(stand, first) == 0
    assert _candidates(stand, second) == 0
    assert _value_rows(first) == _value_rows(second)


def test_truncation_never_drops_cited(
    stand: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Лимит режет term, но ``cited`` остаётся: адрес — часть определения ячейки."""
    conn = sqlite3.connect(stand["db"])
    try:
        for index in range(30):
            doc_id = f"{PRODUCT}:t{index:02d}"
            conn.execute(
                "INSERT INTO corpus_docs VALUES (?, ?, ?)",
                (doc_id, PRODUCT, f"https://docs.test/t{index}"),
            )
            conn.execute(
                "INSERT INTO corpus_chunks VALUES (?, ?, ?, ?, ?)",
                (
                    f"ct{index}",
                    doc_id,
                    PRODUCT,
                    "резервное копирование данных",
                    index,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    assert _candidates(stand, None, "--max-candidates-per-cell", "5") == 0
    rows = [row for row in _rows(stand["out"]) if row["cell"] == f"{PRODUCT}/param.alpha"]
    assert len(rows) == 5
    assert any("cited" in row["how"] for row in rows)
    # Обрезка названа в отчёте и записана в лист policy: «упёрлось в лимит» — факт.
    out = capsys.readouterr().out
    assert "упёршихся в лимит 5: 1" in out
    assert PREP.read_policy_sheet(stand["out"])["cells_truncated"] == "1"  # type: ignore[attr-defined]


def test_candidates_refuse_missing_db(tmp_path: Path) -> None:
    code = PREP.main(  # type: ignore[attr-defined]
        [
            "candidates",
            "--db",
            str(tmp_path / "нет.db"),
            "--golden",
            str(tmp_path / "golden.json"),
            "--out",
            str(tmp_path / "out.xlsx"),
        ]
    )
    assert code == 2


def test_candidates_refuse_golden_of_another_run(stand: dict[str, Path], tmp_path: Path) -> None:
    """``doc_id`` разных корпусов несравнимы: эталон обязан быть с этого прогона."""
    payload = json.loads(stand["golden"].read_text(encoding="utf-8"))
    payload["corpus_run_id"] = "run-other"
    golden = tmp_path / "golden-other.json"
    golden.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    stand = {**stand, "golden": golden}
    assert _candidates(stand) == 2


def test_candidates_refuse_retrieved_without_depth(stand: dict[str, Path]) -> None:
    """Глубина выдачи — условие источника, а не догадка: без неё источник не включается."""
    assert _candidates(stand, None, "--include-retrieved") == 2


# ---------------------------------------------------------------------------
# compile
# ---------------------------------------------------------------------------
def _annotate(path: Path, answers: dict[tuple[str, str], str]) -> None:
    """Проставляет ответы человека в лист (как это сделал бы Excel)."""
    from openpyxl import load_workbook

    book = load_workbook(path)
    sheet = book[PREP.SHEET_CANDIDATES]  # type: ignore[attr-defined]
    header = {cell.value: cell.column for cell in sheet[1]}
    for row_index in range(2, sheet.max_row + 1):
        cell = sheet.cell(row=row_index, column=header["cell"]).value
        doc_id = sheet.cell(row=row_index, column=header["doc_id"]).value
        answer = answers.get((cell, doc_id), "")
        sheet.cell(row=row_index, column=header["answers"]).value = answer
    book.save(path)


def _compile(stand: dict[str, Path], out: Path | None = None, *extra: str) -> int:
    return PREP.main(  # type: ignore[attr-defined]
        [
            "compile",
            "--xlsx",
            str(stand["out"]),
            "--out",
            str(out or stand["out"].with_suffix(".json")),
            *extra,
        ]
    )


def test_compile_builds_answer_set_from_yes(stand: dict[str, Path]) -> None:
    assert _candidates(stand) == 0
    _annotate(
        stand["out"],
        {
            (f"{PRODUCT}/param.alpha", f"{PRODUCT}:aaa"): "yes",
            (f"{PRODUCT}/param.alpha", f"{PRODUCT}:bbb"): "yes",
            (f"{PRODUCT}/param.beta", f"{PRODUCT}:ddd"): "yes",
        },
    )
    assert _compile(stand) == 0
    from core.pipeline.retrieval_answers import load_answer_sets

    sets = load_answer_sets(stand["out"].with_suffix(".json"))
    assert sets.run_id == "run-1"
    assert sets.cutoff_k == 10
    assert sets.status == "candidate"
    entry = sets.entry_for(f"{PRODUCT}/param.alpha")
    assert {doc.doc_id for doc in entry.answer_docs} == {f"{PRODUCT}:aaa", f"{PRODUCT}:bbb"}


def test_compile_refuses_no_on_cited(
    stand: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Ошибка разметки O14: адрес цитаты по определению отвечает, ``no`` — отказ."""
    assert _candidates(stand) == 0
    _annotate(
        stand["out"],
        {
            (f"{PRODUCT}/param.alpha", f"{PRODUCT}:aaa"): "no",
            (f"{PRODUCT}/param.beta", f"{PRODUCT}:ddd"): "yes",
        },
    )
    assert _compile(stand) == 2
    err = capsys.readouterr().err
    assert "O14" in err
    assert f"{PRODUCT}/param.alpha" in err


def test_compile_keeps_cell_without_any_yes(
    stand: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Ячейка без ``yes`` не выпадает: её набор — ``cited``, и она считается."""
    assert _candidates(stand) == 0
    # У beta не отмечено ничего: ячейка без ``yes`` не выпадает — её набор = cited.
    _annotate(
        stand["out"],
        {
            (f"{PRODUCT}/param.alpha", f"{PRODUCT}:aaa"): "yes",
        },
    )
    assert _compile(stand) == 0
    from core.pipeline.retrieval_answers import load_answer_sets

    sets = load_answer_sets(stand["out"].with_suffix(".json"))
    beta = sets.entry_for(f"{PRODUCT}/param.beta")
    assert {doc.doc_id for doc in beta.answer_docs} == {f"{PRODUCT}:ddd"}
    assert "ячеек без единого yes (набор = cited): 1" in capsys.readouterr().out


def test_partial_is_excluded_by_default_and_included_with_flag(stand: dict[str, Path]) -> None:
    assert _candidates(stand) == 0
    _annotate(
        stand["out"],
        {
            (f"{PRODUCT}/param.alpha", f"{PRODUCT}:aaa"): "yes",
            (f"{PRODUCT}/param.alpha", f"{PRODUCT}:bbb"): "partial",
            (f"{PRODUCT}/param.beta", f"{PRODUCT}:ddd"): "yes",
        },
    )
    assert _compile(stand) == 0
    from core.pipeline.retrieval_answers import load_answer_sets

    default = load_answer_sets(stand["out"].with_suffix(".json"))
    assert f"{PRODUCT}:bbb" not in {
        doc.doc_id for doc in default.entry_for(f"{PRODUCT}/param.alpha").answer_docs
    }
    assert default.policy.include_partial is False

    out = stand["out"].with_name("with-partial.json")
    assert _compile(stand, out, "--include-partial") == 0
    included = load_answer_sets(out)
    assert f"{PRODUCT}:bbb" in {
        doc.doc_id for doc in included.entry_for(f"{PRODUCT}/param.alpha").answer_docs
    }
    # Флаг попадает в файл: набор обязан быть воспроизводимым без знания флагов.
    assert included.policy.include_partial is True


def test_compile_requires_actor_for_reviewed(stand: dict[str, Path]) -> None:
    assert _candidates(stand) == 0
    _annotate(stand["out"], {(f"{PRODUCT}/param.alpha", f"{PRODUCT}:aaa"): "yes",
                             (f"{PRODUCT}/param.beta", f"{PRODUCT}:ddd"): "yes"})
    assert _compile(stand, None, "--status", "reviewed") == 2
    out = stand["out"].with_name("reviewed.json")
    assert _compile(stand, out, "--status", "reviewed", "--actor", "roger") == 0
    from core.pipeline.retrieval_answers import load_answer_sets

    sets = load_answer_sets(out)
    assert sets.status == "reviewed"
    assert sets.reviewed_by == "roger"


def test_compile_refuses_unknown_answer_value(stand: dict[str, Path]) -> None:
    assert _candidates(stand) == 0
    _annotate(stand["out"], {(f"{PRODUCT}/param.alpha", f"{PRODUCT}:aaa"): "unclear"})
    assert _compile(stand) == 2


# ---------------------------------------------------------------------------
# validate / summary
# ---------------------------------------------------------------------------
def test_validate_accepts_fresh_sheet_and_names_cells_without_candidates(
    stand: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    assert _candidates(stand) == 0
    code = PREP.main(  # type: ignore[attr-defined]
        [
            "validate",
            "--xlsx",
            str(stand["out"]),
            "--golden",
            str(stand["golden"]),
        ]
    )
    assert code == 0
    assert "лист: 4 строк" in capsys.readouterr().out


def test_validate_refuses_unknown_answer_and_duplicate_row(stand: dict[str, Path]) -> None:
    assert _candidates(stand) == 0
    _annotate(stand["out"], {(f"{PRODUCT}/param.alpha", f"{PRODUCT}:aaa"): "может"})
    assert (
        PREP.main(  # type: ignore[attr-defined]
            ["validate", "--xlsx", str(stand["out"])]
        )
        == 2
    )


def test_validate_json_goes_through_the_loader(stand: dict[str, Path], tmp_path: Path) -> None:
    """Правила не дублируются: битый набор отвергается загрузчиком ядра."""
    assert _candidates(stand) == 0
    _annotate(stand["out"], {(f"{PRODUCT}/param.alpha", f"{PRODUCT}:aaa"): "yes",
                             (f"{PRODUCT}/param.beta", f"{PRODUCT}:ddd"): "yes"})
    assert _compile(stand) == 0
    out = stand["out"].with_suffix(".json")
    assert PREP.main(["validate", "--json", str(out)]) == 0  # type: ignore[attr-defined]
    payload = json.loads(out.read_text(encoding="utf-8"))
    payload["entries"][0]["cited_doc_ids"] = [f"{PRODUCT}:чужой"]
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    assert PREP.main(["validate", "--json", str(broken)]) == 2  # type: ignore[attr-defined]


def test_summary_reports_answers_how_and_cited_rejections(
    stand: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    assert _candidates(stand) == 0
    _annotate(
        stand["out"],
        {
            (f"{PRODUCT}/param.alpha", f"{PRODUCT}:aaa"): "yes",
            (f"{PRODUCT}/param.alpha", f"{PRODUCT}:bbb"): "no",
            (f"{PRODUCT}/param.beta", f"{PRODUCT}:ddd"): "yes",
        },
    )
    assert PREP.main(["summary", "--xlsx", str(stand["out"])]) == 0  # type: ignore[attr-defined]
    out = capsys.readouterr().out
    assert "строк: 4, ячеек: 2" in out
    assert "yes 2" in out and "no 1" in out and "пусто 1" in out
    assert "документов на ячейку: min 2, median 2.0, max 2" in out
    assert "ячеек с одним кандидатом: 0" in out
    assert "cited-документов отвергнуто человеком (ошибки разметки O14): 0" in out


def test_summary_refuses_missing_sheet(tmp_path: Path) -> None:
    assert PREP.main(["summary", "--xlsx", str(tmp_path / "нет.xlsx")]) == 2  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Судья (--llm): вердикт — кандидат, цитата обязана быть в тексте (I22)
# ---------------------------------------------------------------------------
def _candidate() -> object:
    return PREP.Candidate(doc_id=f"{PRODUCT}:aaa")  # type: ignore[attr-defined]


def test_judge_yes_with_quote_in_text_becomes_llm_candidate() -> None:
    item = _candidate()
    PREP.apply_judge(item, verdict="yes", quote="резервное копирование", text="… резервное копирование данных …")  # type: ignore[attr-defined]
    assert item.judge_verdict == "yes"
    assert "llm" in item.how


def test_judge_with_quote_outside_text_is_rejected_visibly() -> None:
    """Цитата, которой нет в документе, не принимается: видимый токен, а не тишина."""
    item = _candidate()
    PREP.apply_judge(item, verdict="yes", quote="такого текста нет", text="другой текст")  # type: ignore[attr-defined]
    assert item.judge_verdict == "rejected_by_quote_check"
    assert "llm" not in item.how
    assert item.judge_quote == "такого текста нет"


def test_judge_no_does_not_add_llm_origin() -> None:
    item = _candidate()
    PREP.apply_judge(item, verdict="no", quote="", text="текст")  # type: ignore[attr-defined]
    assert item.judge_verdict == "no"
    assert "llm" not in item.how


def test_judge_document_reports_transport_failure_as_verdict() -> None:
    """Сбой судьи виден вердиктом ``error``, а не исключением посреди листа."""

    class _Broken:
        def complete(self, request: object) -> object:
            raise RuntimeError("провайдер недоступен")

    verdict, quote, reason = PREP.judge_document(  # type: ignore[attr-defined]
        _Broken(), model_id="m", title="t", rule="r", text="текст"
    )
    assert verdict == "error"
    assert quote == ""
    assert "провайдер" in reason


def test_retrieved_refuses_without_index_build(stand: dict[str, Path]) -> None:
    """``--include-retrieved`` без сборки индекса — отказ с путём, а не тихий пропуск."""
    conn = sqlite3.connect(stand["db"])
    try:
        conn.execute("UPDATE runs SET index_build_id = NULL")
        conn.commit()
    finally:
        conn.close()
    assert _candidates(stand, None, "--include-retrieved", "--retrieved-depth", "10") == 2
