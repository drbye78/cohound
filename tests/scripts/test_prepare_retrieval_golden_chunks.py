"""Пины пересборки чанкового эталона из документного (остаток O14 плана 26).

Предмет — не качество эталона, а два свойства прибора:

* **адрес записи** — ожидаемые чанки берутся из цитаты доказательства (внутри чанка, на
  границе двух чанков, после нормализации пробелов и экранированного underscore), а не из
  «всех чанков документа»; ненайденная цитата и отсутствие доказательства **отбрасывают
  ячейку с названной причиной**, а не дают пустой ``expected_ids``;
* **форма выхода** — файл обязан проходить ``load_retrieval_golden`` без правок загрузчика,
  а ``validate`` — отказывать на нерезолвимом id (иначе «поиск не нашёл» и «ожидание не
  существует» снова слились бы в одно число).

БД синтетическая: прибору нужны четыре таблицы run-DB и снимок входов прогона.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "prepare_retrieval_golden_chunks.py"

PRODUCT = "p1"
RUN_ID = "p1-run"

#: Версии документов: `dv_a` — документ с тремя чанками, `dv_other` — другой продукт.
DV_MAIN = "dv_a"
DV_OTHER = "dv_other"
DOC_MAIN = f"{PRODUCT}:doc-a"
DOC_OTHER = "p2:doc-b"

#: Текст чанков: граница «гамма дельта» проходит между чанками 0 и 1 (пробел — хвост
#: нулевого чанка, как у настоящего среза текста на границе абзаца).
CHUNKS = [
    ("c0", DOC_MAIN, PRODUCT, 0, "Альфа бета гамма "),
    ("c1", DOC_MAIN, PRODUCT, 1, "дельта эпсилон"),
    ("c2", DOC_MAIN, PRODUCT, 2, "pg_basebackup полезен"),
    ("c9", DOC_OTHER, "p2", 0, "чужой продукт"),
]


def _load_harness() -> object:
    spec = importlib.util.spec_from_file_location("_prepare_retrieval_golden_chunks", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


PREP = _load_harness()


def _param(param_id: str, title: str) -> dict[str, object]:
    """Строка каталога в форме снимка входов (``plan_inputs.json:params``)."""
    return {
        "param_id": param_id,
        "title": title,
        "module": "core",
        "group": "group",
        "importance": "Вж",
        "param_type": "fact",
        "rule": "правило",
        "absence_rule": "explicit",
        "role_binding": None,
        "concept_terms": {"ru": [title], "en": []},
        "search_langs": ["ru"],
        "deprecated": False,
        "replaced_by": None,
    }


PARAMS = [
    _param("p_inside", "Внутри чанка"),
    _param("p_boundary", "На границе"),
    _param("p_missing", "Ненайденная цитата"),
    _param("p_noev", "Без доказательства"),
    _param("p_escape", "Экранирование"),
]


def _write_run_db(root: Path, evidence: list[tuple[str, str, str, str]]) -> Path:
    """Собирает минимальный run-DB и снимок входов; возвращает путь к ``cohound.db``.

    :param evidence: строки ``(param_id, doc_version_id, citation_quote, product_id)``.
    """
    run_dir = root / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    db_path = run_dir / "cohound.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE corpus_chunks (chunk_id TEXT PRIMARY KEY, doc_id TEXT, product_id TEXT,"
            " chunk_no INTEGER, text TEXT, char_start INTEGER, char_end INTEGER)"
        )
        conn.execute(
            "CREATE TABLE corpus_docs (doc_id TEXT PRIMARY KEY, product_id TEXT, url TEXT)"
        )
        conn.execute(
            "CREATE TABLE corpus_doc_version (doc_version_id TEXT PRIMARY KEY, product_id TEXT,"
            " doc_id TEXT)"
        )
        conn.execute(
            "CREATE TABLE cell_evidence (evidence_id TEXT, product_id TEXT, param_id TEXT,"
            " doc_version_id TEXT, citation_quote TEXT)"
        )
        position = 0
        for chunk_id, doc_id, product_id, chunk_no, text in CHUNKS:
            start = position if doc_id == DOC_MAIN else 0
            end = start + len(text)
            if doc_id == DOC_MAIN:
                position = end
            conn.execute(
                "INSERT INTO corpus_chunks VALUES (?, ?, ?, ?, ?, ?, ?)",
                (chunk_id, doc_id, product_id, chunk_no, text, start, end),
            )
        conn.executemany(
            "INSERT INTO corpus_docs VALUES (?, ?, ?)",
            [
                (DOC_MAIN, PRODUCT, "https://docs.test/a"),
                (DOC_OTHER, "p2", "https://docs.test/b"),
            ],
        )
        conn.executemany(
            "INSERT INTO corpus_doc_version VALUES (?, ?, ?)",
            [(DV_MAIN, PRODUCT, DOC_MAIN), (DV_OTHER, "p2", DOC_OTHER)],
        )
        for index, (param_id, doc_version_id, quote, product_id) in enumerate(evidence):
            conn.execute(
                "INSERT INTO cell_evidence VALUES (?, ?, ?, ?, ?)",
                (f"ev{index}", product_id, param_id, doc_version_id, quote),
            )
        conn.commit()
    finally:
        conn.close()
    (run_dir / "plan_inputs.json").write_text(
        json.dumps({"run_id": RUN_ID, "params": PARAMS}, ensure_ascii=False), encoding="utf-8"
    )
    return db_path


def _write_doc_golden(path: Path, cells: list[tuple[str, str]]) -> Path:
    """Документный эталон с записями ``(param_id, source_url)`` для одного продукта."""
    entries = [
        {
            "cell": f"{PRODUCT}/{param_id}",
            "product_id": PRODUCT,
            "param_id": param_id,
            "source_url": url,
            "expected_doc_ids": [DOC_MAIN],
            "match": "exact",
        }
        for param_id, url in cells
    ]
    path.write_text(
        json.dumps(
            {
                "format": "cohound-retrieval-golden-docs/1",
                "case_id": "case",
                "corpus_run_id": RUN_ID,
                "entries": entries,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture()
def workspace(tmp_path: Path) -> dict[str, Path]:
    """Стенд прибора: БД с четырьмя ячейками и документный эталон."""
    evidence = [
        # внутри чанка 0
        ("p_inside", DV_MAIN, "бета", PRODUCT),
        # на границе чанков 0/1 (плюс нормализация пробелов и перевода строки)
        ("p_boundary", DV_MAIN, "гамма \n  дельта", PRODUCT),
        # цитаты нет ни в одном чанке
        ("p_missing", DV_MAIN, "не существует такой строки", PRODUCT),
        # экранированный underscore документации (замер O16)
        ("p_escape", DV_MAIN, r"pg\_basebackup", PRODUCT),
        # `p_noev` доказательства не имеет вовсе
    ]
    db_path = _write_run_db(tmp_path, evidence)
    doc_golden = _write_doc_golden(
        tmp_path / "retrieval_golden_docs.json",
        [
            ("p_inside", "https://docs.test/a"),
            ("p_boundary", "https://docs.test/a"),
            ("p_missing", "https://docs.test/a"),
            ("p_noev", "https://docs.test/a"),
            ("p_escape", "https://docs.test/a"),
        ],
    )
    return {"db": db_path, "doc_golden": doc_golden, "out": tmp_path / "chunks.json"}


def _build(workspace: dict[str, Path], capsys: pytest.CaptureFixture[str]) -> dict:
    """Гоняет ``build`` через CLI и возвращает машинную сводку."""
    code = PREP.main(  # type: ignore[attr-defined]
        [
            "build",
            "--db",
            str(workspace["db"]),
            "--golden",
            str(workspace["doc_golden"]),
            "--out",
            str(workspace["out"]),
            "--json",
        ]
    )
    assert code == 0
    return json.loads(capsys.readouterr().out)


def _records(workspace: dict[str, Path]) -> list[dict]:
    return json.loads(workspace["out"].read_text(encoding="utf-8"))


def test_quote_inside_chunk_gives_that_chunk(
    workspace: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Цитата внутри чанка адресует ровно этот чанк."""
    _build(workspace, capsys)
    by_cell = {
        record["cell"]: record for record in _records(workspace) if record["cell"].endswith("p_inside")
    }
    assert by_cell, "ячейка с цитатой внутри чанка обязана попасть в эталон"
    for record in by_cell.values():
        assert record["expected_ids"] == ["c0"]
        assert record["query"] == "Внутри чанка"


def test_quote_on_chunk_boundary_names_both_chunks(
    workspace: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Цитата, пересекающая границу, даёт **все** пересечённые чанки и считается числом."""
    payload = _build(workspace, capsys)
    boundary = [r for r in _records(workspace) if r["cell"].endswith("p_boundary")]
    assert boundary, "цитата на границе обязана найтись в склеенном тексте документа"
    assert boundary[0]["expected_ids"] == ["c0", "c1"]
    assert payload["summary"]["cells_with_boundary_quote"] == 1
    assert payload["summary"]["quotes"]["on_boundary"] == 1


def test_missing_quote_drops_cell_with_reason(
    workspace: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Ненайденная цитата отбрасывает ячейку — пустого ``expected_ids`` не бывает."""
    payload = _build(workspace, capsys)
    cells = {record["cell"] for record in _records(workspace)}
    assert f"{PRODUCT}/p_missing" not in cells
    assert payload["summary"]["dropped_cells"]["quote_not_found"] == 1
    assert payload["summary"]["quotes"]["not_found"] == 1
    assert all(record["expected_ids"] for record in _records(workspace))


def test_cell_without_evidence_drops_with_reason(
    workspace: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Ячейка без доказательства — отдельная причина, а не «цитата не найдена»."""
    payload = _build(workspace, capsys)
    assert payload["summary"]["dropped_cells"]["no_evidence"] == 1
    assert payload["summary"]["cells_with_chunks"] == 3
    assert payload["summary"]["cells"] == 5


def test_escaped_underscore_is_matched(
    workspace: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """``\\_`` документации не мешает адресовать чанк (замер O16)."""
    _build(workspace, capsys)
    escape = [r for r in _records(workspace) if r["cell"].endswith("p_escape")]
    assert escape
    assert escape[0]["expected_ids"] == ["c2"]


def test_records_pass_loader_and_one_per_query(
    workspace: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Файл проходит ``load_retrieval_golden`` без правок; запись на каждый запрос фазы."""
    payload = _build(workspace, capsys)
    golden = PREP.load_retrieval_golden(workspace["out"])  # type: ignore[attr-defined]
    assert len(golden) == payload["summary"]["records"]
    assert len(golden) == payload["summary"]["queries_total"]
    assert all(item.expected_ids for item in golden)


def test_validate_accepts_built_file(
    workspace: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """``validate`` на собранном файле: 0 нерезолвленных, код 0."""
    _build(workspace, capsys)
    code = PREP.main(  # type: ignore[attr-defined]
        ["validate", "--golden", str(workspace["out"]), "--db", str(workspace["db"]), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["unresolved"] == 0


def test_validate_refuses_unresolved_id(
    workspace: dict[str, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """Нерезолвимый id — отказ (код 1), а не молчаливый пропуск."""
    bad = workspace["out"]
    bad.write_text(
        json.dumps(
            [
                {
                    "query": "запрос",
                    "expected_ids": ["p1:dv_нет:0"],
                    "lang": "ru",
                    "cell": f"{PRODUCT}/p_inside",
                    "product_id": PRODUCT,
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    code = PREP.main(  # type: ignore[attr-defined]
        ["validate", "--golden", str(bad), "--db", str(workspace["db"]), "--json"]
    )
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["unresolved"] == 1


@pytest.mark.parametrize(
    "record",
    [
        {"query": "", "expected_ids": ["c0"], "lang": "ru"},
        {"query": "запрос", "expected_ids": [], "lang": "ru"},
    ],
)
def test_validate_refuses_empty_query_or_expected(
    workspace: dict[str, Path], capsys: pytest.CaptureFixture[str], record: dict
) -> None:
    """Пустой ``query``/``expected_ids`` — отказ: загрузчик такой файл отвергает."""
    path = workspace["out"]
    path.write_text(json.dumps([record], ensure_ascii=False), encoding="utf-8")
    code = PREP.main(  # type: ignore[attr-defined]
        ["validate", "--golden", str(path), "--db", str(workspace["db"])]
    )
    assert code == 1


def test_quote_without_address_is_its_own_reason(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Цитата без ``doc_version_id`` (O5) — не «доказательства нет», а «адреса нет»."""
    db_path = _write_run_db(tmp_path, [("p_inside", "", "бета", PRODUCT)])
    doc_golden = _write_doc_golden(tmp_path / "docs.json", [("p_inside", "https://docs.test/a")])
    out = tmp_path / "chunks.json"
    code = PREP.main(  # type: ignore[attr-defined]
        ["build", "--db", str(db_path), "--golden", str(doc_golden), "--out", str(out), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["dropped_cells"]["quote_without_address"] == 1
    assert payload["summary"]["quotes"]["without_address"] == 1
    assert json.loads(out.read_text(encoding="utf-8")) == []


def test_foreign_doc_is_not_an_expectation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Чанк чужого продукта недостижим продукт-скоупед ретривалом (I17) — причина названа."""
    evidence = [("p_inside", DV_OTHER, "чужой", PRODUCT)]
    db_path = _write_run_db(tmp_path, evidence)
    doc_golden = _write_doc_golden(tmp_path / "docs.json", [("p_inside", "https://docs.test/a")])
    out = tmp_path / "chunks.json"
    code = PREP.main(  # type: ignore[attr-defined]
        [
            "build",
            "--db",
            str(db_path),
            "--golden",
            str(doc_golden),
            "--out",
            str(out),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["dropped_cells"]["foreign_doc"] == 1
    assert json.loads(out.read_text(encoding="utf-8")) == []
