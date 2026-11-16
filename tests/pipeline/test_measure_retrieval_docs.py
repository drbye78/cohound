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
    """Скаляры (дефолты) дают ту же сетку, что до появления списков."""
    assert HARNESS._resolve_pool_multipliers(1, None) == (1,)
    assert HARNESS._resolve_cross_query_fusions("max", None) == ("max",)
    grid = HARNESS._measurement_grid(
        ["R0", "R1", "R2"],
        HARNESS._resolve_cross_query_fusions("max", None),
        HARNESS._resolve_pool_multipliers(1, None),
    )
    assert grid == [("R0", 1, "max"), ("R1", 1, "max"), ("R2", 1, "max")]
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
