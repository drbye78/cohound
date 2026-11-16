"""Прибор документного эталона: чтение прогона, каталога и запись артефактов (O14).

Предмет — **не** метрики (они живут в :mod:`core.pipeline.retrieval_docs` и проверены
своими тестами), а обвязка прибора: из чего он берёт условия измерения и что пишет в
дерево. Три вещи здесь дороже всего, потому что тихо ломаются:

* **Условия берутся из прогона, а не из аргументов**: ``runs.retrieval_cutoff_K`` — это
  I20 (cut-off эталона = K пайплайна), и прибор обязан отказывать, когда строк несколько
  или сборка индекса не записана, а не выбирать «первую попавшуюся».
* **Каталог — из снимка прогона** (``plan_inputs.json``), а не из пакета: запросы обязаны
  быть теми же, что рендерила фаза, даже если каталог пакета успел измениться.
* **CSV публикует ранг, а не выдачу**: сотни ``doc_id`` по 70 символов раздули бы
  артефакт, а ранг и есть то, из чего считается метрика.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "measure_retrieval_docs.py"


def _load_harness() -> object:
    spec = importlib.util.spec_from_file_location("_measure_retrieval_docs", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Модуль регистрируется до exec: иначе ``@dataclass`` не находит ``sys.modules``
    # по ``cls.__module__`` и падает на этапе определения класса.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


HARNESS = _load_harness()

PRODUCT = "p1"
PARAM = {
    "param_id": "param_one",
    "title": "Параметр",
    "module": "core",
    "group": "general",
    "importance": "Вж",
    "param_type": "fact",
    "rule": "правило",
    "search_langs": ["ru"],
}


def _run_db(path: Path, rows: list[tuple[str, str, str, int]]) -> Path:
    """Минимальная БД прогона: только то, что читает прибор (``runs`` + ``schema_meta``)."""
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE runs (run_id TEXT, package_id TEXT, index_build_id TEXT,"
        " retrieval_cutoff_K INTEGER)"
    )
    conn.execute("CREATE TABLE schema_meta (key TEXT, value TEXT)")
    conn.execute(
        "INSERT INTO schema_meta VALUES ('schema_version', '3.20.0')"
    )
    conn.executemany("INSERT INTO runs VALUES (?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return path


def _corpus_db(path: Path) -> Path:
    """БД с чанками и документами: карта ``chunk_id → doc_id`` и множество ``doc_id``."""
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE corpus_chunks (chunk_id TEXT, doc_id TEXT, product_id TEXT,"
        " text TEXT, heading_path TEXT)"
    )
    conn.execute("CREATE TABLE corpus_docs (doc_id TEXT)")
    conn.executemany(
        "INSERT INTO corpus_chunks VALUES (?, ?, ?, ?, ?)",
        [
            ("c1", "d1", PRODUCT, "текст", "h1"),
            ("c2", "d1", PRODUCT, "текст", "h1"),
            ("c3", "d2", PRODUCT, "текст", ""),
        ],
    )
    conn.executemany("INSERT INTO corpus_docs VALUES (?)", [("d1",), ("d2",)])
    conn.commit()
    conn.close()
    return path


def test_read_run_takes_cutoff_and_schema_from_the_file(tmp_path: Path) -> None:
    db = _run_db(tmp_path / "cohound.db", [("run-a", PRODUCT, "cb_1", 10)])
    conn = HARNESS._open_readonly(db)
    try:
        meta = HARNESS._read_run(conn, None)
    finally:
        conn.close()
    assert meta.run_id == "run-a"
    assert meta.build_id == "cb_1"
    assert meta.cutoff_k == 10
    assert meta.schema_version == "3.20.0"


def test_read_run_refuses_ambiguous_file(tmp_path: Path) -> None:
    db = _run_db(
        tmp_path / "cohound.db",
        [("run-a", PRODUCT, "cb_1", 10), ("run-b", PRODUCT, "cb_2", 10)],
    )
    conn = HARNESS._open_readonly(db)
    try:
        with pytest.raises(SystemExit) as exc:
            HARNESS._read_run(conn, None)
        assert "run-id" in str(exc.value)
        # с явным --run-id выбор однозначен
        assert HARNESS._read_run(conn, "run-b").build_id == "cb_2"
    finally:
        conn.close()


def test_read_run_refuses_missing_index_build(tmp_path: Path) -> None:
    """Без ``index_build_id`` ретривал не к чему привязать — это отказ, не пустая сборка."""
    db = _run_db(tmp_path / "cohound.db", [("run-a", PRODUCT, "", 10)])
    conn = HARNESS._open_readonly(db)
    try:
        with pytest.raises(SystemExit) as exc:
            HARNESS._read_run(conn, None)
    finally:
        conn.close()
    assert "index_build_id" in str(exc.value)


def test_read_run_refuses_empty_file(tmp_path: Path) -> None:
    db = _run_db(tmp_path / "cohound.db", [])
    conn = HARNESS._open_readonly(db)
    try:
        with pytest.raises(SystemExit):
            HARNESS._read_run(conn, None)
    finally:
        conn.close()


def test_chunk_doc_map_and_corpus_doc_ids(tmp_path: Path) -> None:
    db = _corpus_db(tmp_path / "cohound.db")
    conn = HARNESS._open_readonly(db)
    try:
        assert HARNESS._chunk_doc_map(conn) == {"c1": "d1", "c2": "d1", "c3": "d2"}
        assert HARNESS._corpus_doc_ids(conn) == {"d1", "d2"}
    finally:
        conn.close()


def test_load_params_prefers_run_snapshot(tmp_path: Path) -> None:
    """Снимок прогона сильнее пакета: каталог мог измениться после прогона."""
    db = _run_db(tmp_path / "cohound.db", [("run-a", PRODUCT, "cb_1", 10)])
    (tmp_path / "plan_inputs.json").write_text(
        json.dumps({"params": [PARAM]}, ensure_ascii=False), encoding="utf-8"
    )
    params = HARNESS._load_params(db, PRODUCT, package_dir=tmp_path / "нет-такого")
    assert [param.param_id for param in params] == ["param_one"]


def test_load_params_without_snapshot_and_package_is_a_refusal(tmp_path: Path) -> None:
    db = _run_db(tmp_path / "cohound.db", [("run-a", PRODUCT, "cb_1", 10)])
    with pytest.raises(SystemExit) as exc:
        HARNESS._load_params(db, PRODUCT, package_dir=None)
    assert "plan_inputs.json" in str(exc.value)


def test_partition_dim_reads_manifest(tmp_path: Path) -> None:
    partition = tmp_path / PRODUCT / "cb_1" / "p1"
    partition.mkdir(parents=True)
    (partition / "manifest.json").write_text(
        json.dumps({"dim": 1024, "embed_model_id": "BAAI/bge-m3"}), encoding="utf-8"
    )
    assert HARNESS._partition_dim(tmp_path, PRODUCT, "cb_1", "p1") == 1024
    assert (
        HARNESS._partition_embed_model(tmp_path, PRODUCT, "cb_1", "p1") == "BAAI/bge-m3"
    )


def test_partition_dim_missing_manifest_is_a_refusal(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        HARNESS._partition_dim(tmp_path, PRODUCT, "cb_1", "p1")
    assert "манифест" in str(exc.value)


def test_cells_csv_publishes_rank_not_the_retrieval(tmp_path: Path) -> None:
    """В файл идёт ранг: выдача воспроизводится командой, а артефакт остаётся маленьким."""
    reports = [
        {
            "profile": "R0",
            "label": "R0/fusion=max",
            "per_cell": [
                {
                    "cell": f"{PRODUCT}/param_one",
                    "product_id": PRODUCT,
                    "param_id": "param_one",
                    "expected_doc_ids": ["d1"],
                    "retrieved_doc_ids": ["d9", "d1"],
                    "rank": 2,
                    "ndcg_at_k": 0.6309297535714574,
                    "unresolved": False,
                },
                {
                    "cell": f"{PRODUCT}/param_two",
                    "product_id": PRODUCT,
                    "param_id": "param_two",
                    "expected_doc_ids": ["d2"],
                    "retrieved_doc_ids": ["d9"],
                    "rank": None,
                    "ndcg_at_k": 0.0,
                    "unresolved": True,
                },
            ],
        }
    ]
    out = tmp_path / "cells.csv"
    HARNESS._write_cells_csv(out, reports)
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert lines[0] == (
        "profile,cell,product_id,param_id,expected_doc_ids,retrieved_docs,rank,"
        "unresolved,label,ndcg_at_k"
    )
    assert lines[1] == (
        f"R0,{PRODUCT}/param_one,{PRODUCT},param_one,d1,2,2,0,R0/fusion=max,0.6309"
    )
    assert lines[2] == (
        f"R0,{PRODUCT}/param_two,{PRODUCT},param_two,d2,1,,1,R0/fusion=max,0.0000"
    )


def test_main_refuses_missing_db(tmp_path: Path) -> None:
    code = HARNESS.main(
        [
            "--db",
            str(tmp_path / "нет.db"),
            "--golden",
            str(tmp_path / "golden.json"),
        ]
    )
    assert code == 2


def test_main_refuses_golden_built_on_another_run(tmp_path: Path) -> None:
    """``doc_id`` разных корпусов несравнимы: эталон обязан быть собран на этом прогоне."""
    db = _run_db(tmp_path / "cohound.db", [("run-a", PRODUCT, "cb_1", 10)])
    golden = tmp_path / "golden.json"
    golden.write_text(
        json.dumps(
            {
                "format": "cohound-retrieval-golden-docs/1",
                "case_id": PRODUCT,
                "corpus_run_id": "run-b",
                "entries": [
                    {
                        "cell": f"{PRODUCT}/param_one",
                        "product_id": PRODUCT,
                        "param_id": "param_one",
                        "source_url": "https://example.test/doc",
                        "expected_doc_ids": ["d1"],
                        "match": "exact",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    code = HARNESS.main(["--db", str(db), "--golden", str(golden)])
    assert code == 2


# ---------------------------------------------------------------------------
# Сетка измерения: профиль × слияние × пул
# ---------------------------------------------------------------------------


def test_grid_expands_lists_into_combinations() -> None:
    """Число комбинаций = профили × слияния, для R2 ещё × пулы."""
    profiles = ["R0", "R1", "R2"]
    fusions = ["max", "rrf"]
    pools = [1, 4]
    grid = HARNESS._measurement_grid(profiles, fusions, pools)
    # R0/R1: слияния; R2: слияния × пулы.
    assert len(grid) == (len(profiles) - 1) * len(fusions) + len(fusions) * len(pools)
    r2 = [item for item in grid if item[0] == "R2"]
    assert len(r2) == len(fusions) * len(pools)
    # Пул участвует только у R2: у R0/R1 множитель остаётся дефолтным.
    assert {item[1] for item in grid if item[0] != "R2"} == {1}
    labels = [HARNESS._combination_label(*item) for item in grid]
    assert "R2/pool=4/fusion=rrf" in labels
    assert "R0/fusion=max" in labels
    assert len(set(labels)) == len(labels)


def test_scalar_arguments_keep_previous_behaviour() -> None:
    """Скаляры (дефолты) дают ту же сетку, что до появления списков.

    Четвёртый элемент кортежа — глубина запроса: ``None`` — не объявлена (равна
    ``cutoff_k``), поэтому метки и исполнение прежние.
    """
    assert HARNESS._resolve_pool_multipliers(1, None) == (1,)
    assert HARNESS._resolve_cross_query_fusions("max", None) == ("max",)
    grid = HARNESS._measurement_grid(
        ["R0", "R1", "R2"],
        HARNESS._resolve_cross_query_fusions("max", None),
        HARNESS._resolve_pool_multipliers(1, None),
    )
    assert grid == [
        ("R0", 1, "max", None),
        ("R1", 1, "max", None),
        ("R2", 1, "max", None),
    ]
    assert [HARNESS._combination_label(*item) for item in grid] == [
        "R0/fusion=max",
        "R1/fusion=max",
        "R2/pool=1/fusion=max",
    ]


def test_lists_override_scalars() -> None:
    assert HARNESS._resolve_pool_multipliers(1, "1,2,4,8") == (1, 2, 4, 8)
    assert HARNESS._resolve_cross_query_fusions("max", "max,rrf,per_query") == (
        "max",
        "rrf",
        "per_query",
    )
    # Дубли не размножают сетку, порядок первого появления сохраняется.
    assert HARNESS._resolve_pool_multipliers(1, "4,4,2") == (4, 2)


def test_unknown_fusion_policy_is_a_refusal_not_a_silent_skip() -> None:
    """Опечатка в политике не должна молча дать ``max``: замер описал бы не то слияние."""
    with pytest.raises(SystemExit) as exc:
        HARNESS._resolve_cross_query_fusions("max", "max,borda")
    message = str(exc.value)
    assert "borda" in message
    assert "per_query" in message  # назван список допустимых


def test_pool_multipliers_reject_non_positive_and_empty() -> None:
    with pytest.raises(SystemExit):
        HARNESS._resolve_pool_multipliers(1, "0")
    with pytest.raises(SystemExit):
        HARNESS._resolve_pool_multipliers(1, "1,x")
    with pytest.raises(SystemExit):
        HARNESS._resolve_pool_multipliers(1, " ")
    with pytest.raises(SystemExit):
        HARNESS._resolve_pool_multipliers(0, None)


class _SpyVectorIndex:
    """Порт векторного индекса, который только записывает окно запроса."""

    build_id = "cb_1"

    def __init__(self) -> None:
        self.windows: list[int] = []

    def search(self, query: object) -> list[object]:
        self.windows.append(query.top_k)  # type: ignore[attr-defined]
        return []


def test_dense_hits_cache_is_keyed_by_pool_multiplier() -> None:
    """Окно батча = ``cutoff_k * pool``, и кеш одного пула не отдаётся другому."""
    from types import SimpleNamespace

    from core.domain.models import CatalogRow
    from core.pipeline.evaluate import unique_queries

    param = CatalogRow.model_validate(PARAM)
    params = {"param_one": param}
    by_product = {
        PRODUCT: [SimpleNamespace(product_id=PRODUCT, param_id="param_one")]
    }
    texts = unique_queries([param])
    query_vectors = {text: [0.0, 0.0] for text in texts}
    index = _SpyVectorIndex()
    cache: dict[tuple[str, int], object] = {}

    def dense(pool_multiplier: int) -> object:
        return HARNESS._dense_by_product(
            by_product=by_product,
            params=params,
            profile="R2",
            cutoff_k=10,
            pool_multiplier=pool_multiplier,
            vector_index=index,
            query_vectors=query_vectors,
            cache=cache,
        )

    dense(1)
    assert index.windows == [10] * len(texts)
    dense(8)
    # Пул 8 спросил порт заново и с окном 80: кеш пула 1 не переиспользован.
    assert index.windows == [10] * len(texts) + [80] * len(texts)
    dense(8)
    # Повтор того же пула — из кеша, порт не спрашивается.
    assert index.windows == [10] * len(texts) + [80] * len(texts)


# ---------------------------------------------------------------------------
# Часть 3: answer-семейство (O42) — один ретривал, две семьи метрик
# ---------------------------------------------------------------------------
class _Entry:
    """Запись эталона: только то, что читает прибор.

    ``doc_id`` получает префикс продукта (``p1:d1``): файл ответов требует его
    инвариантом I17, и тест не должен обходить собственное правило загрузчика.
    """

    def __init__(self, cell: str, expected: tuple[str, ...]) -> None:
        product_id, param_id = cell.split("/", 1)
        self.cell = cell
        self.product_id = product_id
        self.param_id = param_id
        self.expected_doc_ids = tuple(
            doc if ":" in doc else f"{product_id}:{doc}" for doc in expected
        )


class _Param:
    """Параметр каталога: прибору нужен только ``param_id`` (запрос подменён)."""

    def __init__(self, param_id: str) -> None:
        self.param_id = param_id


class _Chunk:
    def __init__(self, chunk_id: str) -> None:
        self.chunk_id = chunk_id


class _Retrieval:
    def __init__(self, chunk_ids: tuple[str, ...]) -> None:
        self.chunks = [_Chunk(chunk_id) for chunk_id in chunk_ids]


def _fake_retrieval(
    monkeypatch: pytest.MonkeyPatch, hits: dict[str, tuple[str, ...]]
) -> list[str]:
    """Подменяет ретривал фазы: возвращает список спрошенных ячеек (для счёта вызовов)."""
    import core.pipeline.evaluate as evaluate_module

    asked: list[str] = []

    def retrieve(param: object, **kwargs: object) -> _Retrieval:
        cell = f"{kwargs['product_id']}/{param.param_id}"  # type: ignore[attr-defined]
        asked.append(cell)
        return _Retrieval(hits.get(cell, ()))

    monkeypatch.setattr(evaluate_module, "retrieve_for_cell", retrieve)
    return asked


def _answer_sets(tmp_path: Path, payload: dict[str, object]) -> Path:
    path = tmp_path / "answers.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _answer_payload(
    cells: list[tuple[str, list[str], list[list[str]]]],
    *,
    run_id: str = "run-a",
    case_id: str = PRODUCT,
) -> dict[str, object]:
    """Файл ответов: ``(cell, cited, [(doc_id, how)])`` → ``cohound-...-answers/1``."""
    return {
        "format": "cohound-retrieval-golden-answers/1",
        "case_id": case_id,
        "run_id": run_id,
        "cutoff_k": 10,
        "judge": None,
        "policy": {
            "term_min_length": 4,
            "term_min_distinct": 2,
            "retrieved_depth": 0,
            "max_candidates_per_cell": 20,
            "include_partial": False,
        },
        "status": "candidate",
        "entries": [
            {
                "cell": cell,
                "product_id": cell.split("/", 1)[0],
                "param_id": cell.split("/", 1)[1],
                "cited_doc_ids": [
                    doc if ":" in doc else f"{cell.split('/', 1)[0]}:{doc}"
                    for doc in cited
                ],
                "answer_docs": [
                    {
                        "doc_id": (
                            doc_id
                            if ":" in doc_id
                            else f"{cell.split('/', 1)[0]}:{doc_id}"
                        ),
                        "how": how,
                        "quote": "",
                    }
                    for doc_id, how in docs
                ],
            }
            for cell, cited, docs in cells
        ],
    }


def test_one_retrieval_pass_serves_both_families(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ретривал выполняется **один раз** на ячейку: второй проход удвоил бы и время, и шум."""
    entries = [
        _Entry(f"{PRODUCT}/a", ("d1",)),
        _Entry(f"{PRODUCT}/b", ("d2",)),
        _Entry(f"{PRODUCT}/c", ("d3",)),
    ]
    hits = {
        f"{PRODUCT}/a": ("c1",),
        f"{PRODUCT}/b": ("c2",),
        f"{PRODUCT}/c": ("c3",),
    }
    asked = _fake_retrieval(monkeypatch, hits)
    sets_path = _answer_sets(
        tmp_path,
        _answer_payload(
            [
                (f"{PRODUCT}/a", ["d1"], [("d1", ["cited"]), ("d9", ["term"])]),
                (f"{PRODUCT}/b", ["d2"], [("d2", ["cited"]), ("d8", ["term"])]),
                (f"{PRODUCT}/c", ["d3"], [("d3", ["cited"]), ("d7", ["term"])]),
            ]
        ),
    )
    from core.pipeline.retrieval_answers import load_answer_sets

    params = {name: _Param(name) for name in ("a", "b", "c")}
    measured = HARNESS._measure_combination(
        profile="R0",
        pool_multiplier=1,
        cross_query_fusion="max",
        entries=entries,
        params=params,
        cutoff_k=10,
        lexical_index=None,
        vector_index=None,
        query_vectors=None,
        chunk_doc={"c1": "p1:d1", "c2": "p1:d2", "c3": "p1:d3"},
        doc_ids={"p1:d1", "p1:d2", "p1:d3"},
        bootstrap=1,
        seed=0,
        answer_sets=load_answer_sets(sets_path),
    )
    # Ровно один запрос на ячейку — не два (по одному на семейство).
    assert asked == [f"{PRODUCT}/a", f"{PRODUCT}/b", f"{PRODUCT}/c"]
    assert measured["address"].hit_at_k == 1.0
    assert measured["answer"].hit_at_k == 1.0
    assert measured["sensitivity"] is not None


def test_hit_gap_separates_question_misses_from_search_misses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``hit_gap`` = answer − address: промах адреса, который не был промахом вопроса.

    Числа на синтетике: адрес нашёлся у 1 ячейки из 2 (0.5), отвечающий — у 2 из 2 (1.0),
    значит разрыв 0.5. Без этого числа «адрес нашёлся в 50 %» читалось бы как «поиск
    работает наполовину», хотя вопрос закрыт полностью.
    """
    entries = [_Entry(f"{PRODUCT}/a", ("d1",)), _Entry(f"{PRODUCT}/b", ("d2",))]
    # Выдача: в первой ячейке адрес, во второй — другой документ того же продукта.
    asked = _fake_retrieval(
        monkeypatch,
        {f"{PRODUCT}/a": ("c1",), f"{PRODUCT}/b": ("c9",)},
    )
    sets_path = _answer_sets(
        tmp_path,
        _answer_payload(
            [
                (f"{PRODUCT}/a", ["d1"], [("d1", ["cited"])]),
                (f"{PRODUCT}/b", ["d2"], [("d2", ["cited"]), ("d9", ["term"])]),
            ]
        ),
    )
    from core.pipeline.retrieval_answers import load_answer_sets

    measured = HARNESS._measure_combination(
        profile="R0",
        pool_multiplier=1,
        cross_query_fusion="max",
        entries=entries,
        params={name: _Param(name) for name in ("a", "b")},
        cutoff_k=10,
        lexical_index=None,
        vector_index=None,
        query_vectors=None,
        chunk_doc={"c1": "p1:d1", "c9": "p1:d9"},
        doc_ids={"p1:d1", "p1:d2", "p1:d9"},
        bootstrap=1,
        seed=0,
        answer_sets=load_answer_sets(sets_path),
    )
    assert asked == [f"{PRODUCT}/a", f"{PRODUCT}/b"]
    payload = HARNESS._report_payload(
        measured["address"],
        label="R0/fusion=max",
        pool_multiplier=1,
        cross_query_fusion="max",
        chunks_without_doc=measured["chunks_without_doc"],
        answer=measured["answer"],
        sensitivity=measured["sensitivity"],
        answer_by_cell=measured["answer_expected"],
        sensitivity_by_cell=measured["sensitivity_expected"],
    )
    assert payload["address_hit_at_k"] == 0.5
    assert payload["answer_hit_at_k"] == 1.0
    assert payload["hit_gap"] == pytest.approx(0.5)
    # Алиасы равны адресным: прежние потребители видят прежние числа.
    assert payload["hit_at_k"] == payload["address_hit_at_k"]
    assert payload["recall_at_k"] == payload["address_recall_at_k"]
    assert payload["mrr"] == payload["address_mrr"]
    assert payload["ndcg_at_k"] == payload["address_ndcg_at_k"]


def test_sensitivity_cell_with_only_retrieved_answers_is_a_zero_not_a_skip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ячейка, чьи ответы целиком из выдачи, даёт 0.0 и остаётся в знаменателе.

    Иначе чувствительность считалась бы только по «удобным» ячейкам и была бы
    оптимистичной ровно там, где вопрос «а если убрать подсказку системы» и задан.
    """
    entries = [_Entry(f"{PRODUCT}/a", ("d1",)), _Entry(f"{PRODUCT}/b", ("d2",))]
    _fake_retrieval(
        monkeypatch,
        {f"{PRODUCT}/a": ("c1",), f"{PRODUCT}/b": ("c2",)},
    )
    sets_path = _answer_sets(
        tmp_path,
        _answer_payload(
            [
                (f"{PRODUCT}/a", ["d1"], [("d1", ["cited"])]),
                # Только retrieved: после вычитания набор пуст.
                (f"{PRODUCT}/b", ["d2"], [("d2", ["cited"]), ("d9", ["retrieved"])]),
            ]
        ),
    )
    from core.pipeline.retrieval_answers import load_answer_sets

    measured = HARNESS._measure_combination(
        profile="R0",
        pool_multiplier=1,
        cross_query_fusion="max",
        entries=entries,
        params={name: _Param(name) for name in ("a", "b")},
        cutoff_k=10,
        lexical_index=None,
        vector_index=None,
        query_vectors=None,
        chunk_doc={"c1": "p1:d1", "c2": "p1:d2"},
        doc_ids={"p1:d1", "p1:d2"},
        bootstrap=1,
        seed=0,
        answer_sets=load_answer_sets(sets_path),
    )
    sensitivity = measured["sensitivity"]
    # Остаток ячейки b — только cited d2 (retrieved d9 вычтен), он найден ⇒ 1.0.
    assert sensitivity.hit_at_k == 1.0
    assert sensitivity.scored == 2

    # Теперь ячейка, у которой после вычитания не осталось **ничего**.
    entries = [_Entry(f"{PRODUCT}/c", ("d3",))]
    _fake_retrieval(monkeypatch, {f"{PRODUCT}/c": ("c3",)})
    sets_path = _answer_sets(
        tmp_path,
        _answer_payload([(f"{PRODUCT}/c", ["d3"], [("d3", ["retrieved"])])]),
    )
    empty = HARNESS._measure_combination(
        profile="R0",
        pool_multiplier=1,
        cross_query_fusion="max",
        entries=entries,
        params={"c": _Param("c")},
        cutoff_k=10,
        lexical_index=None,
        vector_index=None,
        query_vectors=None,
        chunk_doc={"c3": "p1:d3"},
        doc_ids={"p1:d3"},
        bootstrap=1,
        seed=0,
        answer_sets=load_answer_sets(sets_path),
    )
    assert empty["sensitivity"].scored == 1
    assert empty["sensitivity"].hit_at_k == 0.0


def test_report_without_answer_sets_keeps_the_previous_field_set() -> None:
    """Без файла ответов отчёт — прежний до поля: ни ``address_*``, ни ``hit_gap``.

    Заморожённое ожидание, а не «сравнение с собой»: поля O14/O15 публиковались в этом
    составе, и любое новое поле в этом режиме сделало бы два отчёта несравнимыми.
    """
    from core.pipeline.retrieval_docs import DocQueryResult, evaluate_documents

    results = [
        DocQueryResult(
            cell=f"{PRODUCT}/a",
            product_id=PRODUCT,
            param_id="a",
            expected_doc_ids=("d1",),
            retrieved_doc_ids=("d9", "d1"),
            rank=2,
            unresolved=False,
        )
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=1, seed=0)
    payload = HARNESS._report_payload(
        report,
        label="R0/fusion=max",
        pool_multiplier=1,
        cross_query_fusion="max",
    )
    assert payload == {
        "label": "R0/fusion=max",
        "profile": "R0",
        "pool_multiplier": 1,
        "cross_query_fusion": "max",
        "k": 5,
        "cells": 1,
        "scored": 1,
        "unresolved_cells": 0,
        "hit_at_k": 1.0,
        "hit_ci": [1.0, 1.0],
        "recall_at_k": 1.0,
        "recall_ci": [1.0, 1.0],
        "mrr": 0.5,
        "ndcg_at_k": report.ndcg_at_k,
        "per_cell": [
            {
                "cell": f"{PRODUCT}/a",
                "product_id": PRODUCT,
                "param_id": "a",
                "expected_doc_ids": ["d1"],
                "retrieved_doc_ids": ["d9", "d1"],
                "rank": 2,
                "ndcg_at_k": report.ndcg_at_k,
                "unresolved": False,
            }
        ],
    }


def test_answer_sets_missing_golden_cell_is_a_refusal(tmp_path: Path) -> None:
    """«Эталон 2, ответы 1» — отказ с числом и первыми пятью адресами."""
    entries = [_Entry(f"{PRODUCT}/a", ("d1",)), _Entry(f"{PRODUCT}/b", ("d2",))]
    path = _answer_sets(
        tmp_path,
        _answer_payload([(f"{PRODUCT}/a", ["d1"], [("d1", ["cited"])])]),
    )
    with pytest.raises(SystemExit) as exc:
        HARNESS._answer_sets_for(
            str(path), entries=entries, run_id="run-a", case_id=PRODUCT
        )
    message = str(exc.value)
    assert "1 ячеек эталона отсутствуют" in message
    assert f"{PRODUCT}/b" in message


def test_answer_sets_extra_cell_is_a_refusal(tmp_path: Path) -> None:
    entries = [_Entry(f"{PRODUCT}/a", ("d1",))]
    path = _answer_sets(
        tmp_path,
        _answer_payload(
            [
                (f"{PRODUCT}/a", ["d1"], [("d1", ["cited"])]),
                (f"{PRODUCT}/ghost", ["d2"], [("d2", ["cited"])]),
            ]
        ),
    )
    with pytest.raises(SystemExit) as exc:
        HARNESS._answer_sets_for(
            str(path), entries=entries, run_id="run-a", case_id=PRODUCT
        )
    message = str(exc.value)
    assert "1 ячеек файла ответов отсутствуют в эталоне" in message
    assert f"{PRODUCT}/ghost" in message


def test_answer_sets_of_another_run_is_a_refusal(tmp_path: Path) -> None:
    """``doc_id`` разных корпусов несравнимы: чужой набор не переносится."""
    entries = [_Entry(f"{PRODUCT}/a", ("d1",))]
    path = _answer_sets(
        tmp_path,
        _answer_payload(
            [(f"{PRODUCT}/a", ["d1"], [("d1", ["cited"])])], run_id="run-b"
        ),
    )
    with pytest.raises(SystemExit) as exc:
        HARNESS._answer_sets_for(
            str(path), entries=entries, run_id="run-a", case_id=PRODUCT
        )
    assert "run-b" in str(exc.value)


def test_answer_sets_of_another_package_is_a_refusal(tmp_path: Path) -> None:
    entries = [_Entry(f"{PRODUCT}/a", ("d1",))]
    path = _answer_sets(
        tmp_path,
        _answer_payload(
            [(f"{PRODUCT}/a", ["d1"], [("d1", ["cited"])])], case_id="other"
        ),
    )
    with pytest.raises(SystemExit) as exc:
        HARNESS._answer_sets_for(
            str(path), entries=entries, run_id="run-a", case_id=PRODUCT
        )
    assert "other" in str(exc.value)


def test_cells_csv_gains_answer_columns_only_with_answers(tmp_path: Path) -> None:
    """``answer_rank`` и ``answers`` появляются только с файлом ответов.

    Без него CSV остаётся прежним до байта — это проверяет тест выше; здесь — что
    с файлом колонки добавляются **в конец**, а прежние остаются на своих местах.
    """
    reports = [
        {
            "profile": "R0",
            "label": "R0/fusion=max",
            "per_cell": [
                {
                    "cell": f"{PRODUCT}/param_one",
                    "product_id": PRODUCT,
                    "param_id": "param_one",
                    "expected_doc_ids": ["d1"],
                    "retrieved_doc_ids": ["d9", "d1"],
                    "rank": 2,
                    "ndcg_at_k": 0.6309297535714574,
                    "unresolved": False,
                    "answer_rank": 2,
                    "answer_docs": 3,
                }
            ],
        }
    ]
    out = tmp_path / "cells.csv"
    HARNESS._write_cells_csv(out, reports, with_answers=True)
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert lines[0].endswith("ndcg_at_k,answer_rank,answers")
    assert lines[1] == (
        f"R0,{PRODUCT}/param_one,{PRODUCT},param_one,d1,2,2,0,R0/fusion=max,0.6309,2,3"
    )


def test_doc_results_keeps_cell_when_some_members_are_in_corpus() -> None:
    """Набор из 3, 2 выпали из корпуса — ячейка считается, знаменатель recall = 1."""
    entries = [_Entry(f"{PRODUCT}/a", ("d1", "gone1", "gone2"))]
    results = HARNESS._doc_results(
        entries,
        {f"{PRODUCT}/a": ("p1:d1",)},
        {f"{PRODUCT}/a": ("p1:d1", "p1:gone1", "p1:gone2")},
        {"p1:d1"},
    )
    assert len(results) == 1
    result = results[0]
    assert result.unresolved is False
    assert result.resolved_doc_ids == ("p1:d1",)
    from core.pipeline.retrieval_docs import evaluate_documents

    report = evaluate_documents(results, profile="R0", k=5, bootstrap=1, seed=0)
    assert report.scored == 1
    assert report.recall_at_k == 1.0
    assert report.unresolved_docs == 2


def test_doc_results_marks_cell_unresolved_when_no_member_is_in_corpus() -> None:
    entries = [_Entry(f"{PRODUCT}/a", ("gone1", "gone2"))]
    results = HARNESS._doc_results(
        entries,
        {f"{PRODUCT}/a": ("p1:d1",)},
        {f"{PRODUCT}/a": ("p1:gone1", "p1:gone2")},
        {"p1:d1"},
    )
    assert results[0].unresolved is True
    assert results[0].resolved_doc_ids == ()


def test_chunks_without_doc_is_counted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Чанк без записи в ``chunk_id → doc_id`` не теряется молча: он посчитан."""
    entries = [_Entry(f"{PRODUCT}/a", ("d1",))]
    _fake_retrieval(monkeypatch, {f"{PRODUCT}/a": ("c1", "c_missing", "c2")})
    retrieved, chunks_without_doc = HARNESS._retrieve_cells(
        profile="R0",
        pool_multiplier=1,
        cross_query_fusion="max",
        entries=entries,
        params={"a": _Param("a")},
        cutoff_k=10,
        lexical_index=None,
        vector_index=None,
        query_vectors=None,
        chunk_doc={"c1": "p1:d1", "c2": "p1:d2"},
    )
    assert retrieved[f"{PRODUCT}/a"] == ("p1:d1", "p1:d2")
    assert chunks_without_doc == 1


# ---------------------------------------------------------------------------
# Глубина запроса (``--query-depths``): третье измерение сетки
# ---------------------------------------------------------------------------


def test_query_depths_default_is_undeclared() -> None:
    """Без флага — одна комбинация с необъявленной глубиной (прежнее поведение)."""
    assert HARNESS._resolve_query_depths(None, 10) == (None,)


def test_query_depths_parse_in_order_and_drop_duplicates() -> None:
    assert HARNESS._resolve_query_depths("10,20,40", 10) == (10, 20, 40)
    assert HARNESS._resolve_query_depths("40,20,40", 10) == (40, 20)


def test_query_depths_reject_non_positive_and_garbage() -> None:
    """Опечатка в глубине — отказ с числом: иначе замер описал бы не объявленную сетку."""
    with pytest.raises(SystemExit) as excinfo:
        HARNESS._resolve_query_depths("0", 10)
    assert "0" in str(excinfo.value)
    with pytest.raises(SystemExit):
        HARNESS._resolve_query_depths("-5", 10)
    with pytest.raises(SystemExit):
        HARNESS._resolve_query_depths("10,x", 10)
    with pytest.raises(SystemExit):
        HARNESS._resolve_query_depths(" ", 10)


def test_grid_adds_depth_dimension() -> None:
    """Глубина размножает сетку для всех профилей: её исполняет ``retrieve``."""
    grid = HARNESS._measurement_grid(["R0", "R2"], ["max"], [1, 2], [None, 20])
    assert len(grid) == 2 + 2 * 2
    assert {item[3] for item in grid} == {None, 20}
    labels = [HARNESS._combination_label(*item, cutoff_k=10) for item in grid]
    assert labels == [
        "R0/fusion=max",
        "R0/depth=20/fusion=max",
        "R2/pool=1/fusion=max",
        "R2/pool=2/fusion=max",
        "R2/pool=1/depth=20/fusion=max",
        "R2/pool=2/depth=20/fusion=max",
    ]


def test_label_keeps_the_previous_form_when_depth_equals_cutoff() -> None:
    """Глубина, равная срезу K, метку не меняет: опубликованные числа O14/O15 не поехали."""
    assert HARNESS._combination_label("R0", 1, "max", 10, cutoff_k=10) == "R0/fusion=max"
    assert HARNESS._combination_label("R2", 2, "rrf", 10, cutoff_k=10) == (
        "R2/pool=2/fusion=rrf"
    )
    # Объявленная глубина, отличная от K, называется в метке.
    assert HARNESS._combination_label("R0", 1, "per_query", 40, cutoff_k=10) == (
        "R0/depth=40/fusion=per_query"
    )


def test_report_payload_without_query_depth_is_byte_identical() -> None:
    """Полный JSON отчёта без объявленной глубины — прежний до байта.

    Замороженная строка, а не «сравнение с собой»: на этом отчёте стоят числа O14/O15, и
    любое новое поле в этом режиме сделало бы два отчёта несравнимыми без ручной сверки.
    Тот же приём, что у ``--answer-sets``.
    """
    from core.pipeline.retrieval_docs import DocQueryResult, evaluate_documents

    results = [
        DocQueryResult(
            cell=f"{PRODUCT}/a",
            product_id=PRODUCT,
            param_id="a",
            expected_doc_ids=("d1",),
            retrieved_doc_ids=("d9", "d1"),
            rank=2,
            unresolved=False,
        )
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=1, seed=0)
    payload = HARNESS._report_payload(
        report,
        label="R0/fusion=max",
        pool_multiplier=1,
        cross_query_fusion="max",
    )
    assert json.dumps(payload, ensure_ascii=False, sort_keys=True) == (
        '{"cells": 1, "cross_query_fusion": "max", "hit_at_k": 1.0, "hit_ci": [1.0, 1.0],'
        ' "k": 5, "label": "R0/fusion=max", "mrr": 0.5,'
        ' "ndcg_at_k": 0.6309297535714575, "per_cell": [{"cell": "p1/a",'
        ' "expected_doc_ids": ["d1"], "ndcg_at_k": 0.6309297535714575, "param_id": "a",'
        ' "product_id": "p1", "rank": 2, "retrieved_doc_ids": ["d9", "d1"],'
        ' "unresolved": false}], "pool_multiplier": 1, "profile": "R0",'
        ' "recall_at_k": 1.0, "recall_ci": [1.0, 1.0], "scored": 1,'
        ' "unresolved_cells": 0}'
    )


def test_report_payload_names_the_declared_depth() -> None:
    """Объявленная глубина — в строке отчёта: иначе число невоспроизводимо."""
    from core.pipeline.retrieval_docs import DocQueryResult, evaluate_documents

    results = [
        DocQueryResult(
            cell=f"{PRODUCT}/a",
            product_id=PRODUCT,
            param_id="a",
            expected_doc_ids=("d1",),
            retrieved_doc_ids=("d1",),
            rank=1,
            unresolved=False,
        )
    ]
    report = evaluate_documents(results, profile="R0", k=5, bootstrap=1, seed=0)
    payload = HARNESS._report_payload(
        report,
        label="R0/depth=20/fusion=per_query",
        pool_multiplier=1,
        cross_query_fusion="per_query",
        query_depth=20,
    )
    assert payload["query_depth"] == 20
    assert payload["label"] == "R0/depth=20/fusion=per_query"


def test_dense_hits_cache_is_keyed_by_query_depth() -> None:
    """Окно батча = ``max(cutoff_k * pool, depth)``; кеш одной глубины не отдаётся другой."""
    from types import SimpleNamespace

    from core.domain.models import CatalogRow
    from core.pipeline.evaluate import unique_queries

    param = CatalogRow.model_validate(PARAM)
    params = {"param_one": param}
    by_product = {PRODUCT: [SimpleNamespace(product_id=PRODUCT, param_id="param_one")]}
    texts = unique_queries([param])
    query_vectors = {text: [0.0, 0.0] for text in texts}
    index = _SpyVectorIndex()
    cache: dict[tuple[str, str, int, int | None], object] = {}

    def dense(query_depth: int | None) -> object:
        return HARNESS._dense_by_product(
            by_product=by_product,
            params=params,
            profile="R2",
            cutoff_k=10,
            pool_multiplier=1,
            vector_index=index,
            query_vectors=query_vectors,
            cache=cache,
            query_depth=query_depth,
        )

    dense(None)
    assert index.windows == [10] * len(texts)
    dense(40)
    # Глубина 40 спросила порт заново и с окном 40: кеш необъявленной глубины не отдан.
    assert index.windows == [10] * len(texts) + [40] * len(texts)
    dense(40)
    assert index.windows == [10] * len(texts) + [40] * len(texts)


# ------------------------------------------------- цена комбинации (O43)
#
# Дефект O43: в смешанной сетке та же комбинация стоила в 22 раза дороже, чем
# изолированно, и это списывалось на «профиль медленнее». Носители причины — кэш партиций
# (партиция 847 MB fp32 не влезает в бюджет 256 MB) и страничные отказы. Поэтому цена
# публикуется счётчиками, а не только секундами, и только по флагу: без него отчёт обязан
# остаться прежним до байта (на нём стоят числа O14/O15).


def test_partition_cache_mb_is_undeclared_by_default() -> None:
    """Без флага бюджет не переопределяется: цена остаётся ценой дефолта кода."""
    args = HARNESS._parse_args(
        ["--db", "x.db", "--golden", "g.json", "--index-root", "idx"]
    )
    assert args.partition_cache_mb is None


def test_partition_cache_mb_is_declared_explicitly() -> None:
    args = HARNESS._parse_args(
        [
            "--db", "x.db", "--golden", "g.json", "--index-root", "idx",
            "--partition-cache-mb", "0",
        ]
    )
    assert args.partition_cache_mb == 0


def test_cost_block_is_absent_without_the_flag() -> None:
    """Флаг не задан ⇒ в отчёте нет ``cost`` (байтовая совместимость с O14/O15)."""
    from core.pipeline.retrieval_docs import DocQueryResult, evaluate_documents

    results = [
        DocQueryResult(
            cell=f"{PRODUCT}/a",
            product_id=PRODUCT,
            param_id="a",
            expected_doc_ids=("d1",),
            retrieved_doc_ids=("d1",),
            rank=1,
            unresolved=False,
        )
    ]
    report = evaluate_documents(results, profile="R1", k=5, bootstrap=1, seed=0)
    payload = HARNESS._report_payload(
        report, label="R1/fusion=max", pool_multiplier=1, cross_query_fusion="max"
    )
    assert "cost" not in payload


def test_cost_delta_publishes_faults_and_cache() -> None:
    """Дельты, а не абсолюты: вопрос цены — «что стоила эта комбинация»."""
    cost = HARNESS._cost_delta(
        memory_before={"rss_kb": 1000, "minor_faults": 10, "major_faults": 1},
        memory_after={"rss_kb": 2500, "minor_faults": 40, "major_faults": 5},
        cache_before={"budget_bytes": 256, "hits": 1, "misses": 2, "evictions": 0,
                      "bytes_used": 0, "too_large": 0},
        cache_after={"budget_bytes": 256, "hits": 3, "misses": 9, "evictions": 4,
                     "bytes_used": 128, "too_large": 5},
    )
    assert cost["minor_faults"] == 30
    assert cost["major_faults"] == 4
    assert cost["rss_kb_after"] == 2500
    assert cost["partition_cache"] == {
        "budget_bytes": 256,
        "hits": 2,
        "misses": 7,
        "evictions": 4,
        "bytes_used": 128,
        "too_large": 5,
    }


def test_cost_delta_without_cache_has_no_cache_block() -> None:
    """Лексический профиль кэша партиций не имеет — пустого блока не появляется."""
    cost = HARNESS._cost_delta(
        memory_before={"rss_kb": 1, "minor_faults": 0, "major_faults": 0},
        memory_after={"rss_kb": 2, "minor_faults": 5, "major_faults": 0},
        cache_before=None,
        cache_after=None,
    )
    assert "partition_cache" not in cost


def test_partition_cache_state_reads_the_public_counters() -> None:
    """Счётчики берутся с адаптера, а не пересчитываются прибором."""

    class _Cache:
        budget_bytes = 256
        hits = 5
        misses = 7
        evictions = 2
        bytes_used = 1024
        too_large = 1

    class _Index:
        partition_cache = _Cache()

    state = HARNESS._partition_cache_state(_Index())
    assert state == {
        "budget_bytes": 256,
        "hits": 5,
        "misses": 7,
        "evictions": 2,
        "bytes_used": 1024,
        "too_large": 1,
    }
    assert HARNESS._partition_cache_state(object()) is None


def test_dense_hits_cache_is_keyed_by_profile() -> None:
    """Дефект O43: «плеча нет» у R0 не должно отдаваться dense-профилю.

    ``precompute_dense_hits`` возвращает ``None`` для ``R0`` — это законный ответ «батч
    неприменим», а не готовые хиты. При ключе без профиля он ложился под тот же ключ, что
    у ``R1``, и dense-профиль после ``R0`` в сетке шёл поштучным путём: 396 отдельных
    ``search`` вместо 5 батчей. Метрики совпадали, цена росла в 146 раз (864.2 с против
    5.9 с) — то есть замер описывал порядок профилей, а не профиль.
    """
    from types import SimpleNamespace

    from core.domain.models import CatalogRow
    from core.pipeline.evaluate import unique_queries

    param = CatalogRow.model_validate(PARAM)
    params = {"param_one": param}
    by_product = {PRODUCT: [SimpleNamespace(product_id=PRODUCT, param_id="param_one")]}
    texts = unique_queries([param])
    query_vectors = {text: [0.0, 0.0] for text in texts}
    index = _SpyVectorIndex()
    cache: dict[tuple[str, str, int, int | None], object] = {}

    def dense(profile: str) -> object:
        return HARNESS._dense_by_product(
            by_product=by_product,
            params=params,
            profile=profile,
            cutoff_k=10,
            pool_multiplier=1,
            vector_index=index,
            query_vectors=query_vectors,
            cache=cache,
            query_depth=None,
        )

    assert dense("R0")[PRODUCT] is None
    assert index.windows == []  # R0 батч не спрашивает вовсе
    assert dense("R1")[PRODUCT] is not None
    assert index.windows == [10] * len(texts)
    # Повтор R1 берёт готовое, а не спрашивает порт заново.
    dense("R1")
    assert index.windows == [10] * len(texts)
