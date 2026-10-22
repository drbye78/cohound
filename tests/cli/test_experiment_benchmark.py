"""Тесты ``hound experiment evaluator-benchmark`` — поверхность CLI и честная деградация.

Проверяется:

* парсер принимает повторяемый ``--model``;
* ``--model`` побеждает ``COHOUND_LLM_MODEL`` (регрессия identity-бага: run identity
  намеренно игнорирует env, benchmark не должен воспроизводить расхождение хостов);
* ``--json`` несёт ожидаемую форму отчёта, артефакт пишется в
  ``runs/{run_id}/benchmark/evaluator-<timestamp>.json``;
* отсутствие чанков — ненулевой код с видимой причиной (измерение не состоялось).

Сеть не используется: оценщик собирается реальным путём ``build_evaluator_llm``, но с
оффлайн-швом ``generate``; порты ретривала инъецируются. Данные нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from core.cli.main import EXIT_ERROR, EXIT_OK, EXIT_USAGE, main
from core.cli.parser import build_parser
from core.domain import (
    CorpusChunk,
    CorpusDoc,
    IndexManifestEntry,
    LLMCellOutput,
    LLMEvidence,
    Status,
)
from core.domain.enums import RankingPolicy, RunMode
from core.domain.models import Focus, RunConfig
from core.ports.vector_index import RetrievedChunk
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

PKG = "case_x"
BUILD = "ib_bench"
PRODUCT = "p1"
TEXT = "Функция Альфа доступна без дополнительной настройки."

FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
RUN_YAML = FIXTURES / "run.yaml"

# ---------------------------------------------------------------------------
# Инъекции: порты ретривала и оффлайн-оценщик
# ---------------------------------------------------------------------------

class _FakeLexicalIndex:
    """BM25-подобный фейк: отдаёт заданные чанки продукта (без индекса/сети)."""

    engine_version = "fake-bm25"

    def __init__(self, by_product: dict[str, list[RetrievedChunk]], build_id: str = BUILD) -> None:
        self.by_product = by_product
        self.build_id = build_id

    def search(
        self,
        query: str,  # noqa: ARG002
        product_ids: list[str],
        top_k: int,
        lang: str | None = None,  # noqa: ARG002
    ) -> list[RetrievedChunk]:
        found: list[RetrievedChunk] = []
        for product_id in product_ids:
            found.extend(self.by_product.get(product_id, []))
        return found[:top_k]

def _chunk(text: str = TEXT) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id="c1",
        product_id=PRODUCT,
        text=text,
        content_hash="hash-c1",
        heading_path="H",
        build_id=BUILD,
        score=1.0,
    )

def _ports(*, with_chunks: bool = True) -> SimpleNamespace:
    lexical = _FakeLexicalIndex({PRODUCT: [_chunk()] if with_chunks else []})
    return SimpleNamespace(lexical_index=lexical, vector_index=None, embedder=None)

def _generate(_prompt: str, _schema: type) -> str:
    """Оффлайн-ответ оценщика: валидный ✅ с цитатой из чанка (без сети)."""
    return LLMCellOutput(
        status=Status.OK,
        justification="Функция подтверждена документацией.",
        evidence=[LLMEvidence(source_id="c1", quote=TEXT)],
    ).model_dump_json()

# ---------------------------------------------------------------------------
# Прогон и артефакты
# ---------------------------------------------------------------------------

def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case_x",
        anchor=PRODUCT,
        cohort="cohort_case_x",
        products=[PRODUCT],
        package=PKG,
        focus=Focus(modules=["mod_a"], params=["param_a"]),
        languages=["ru"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )

def _store(db: Path, *, with_chunks: bool = True) -> None:
    db.parent.mkdir(parents=True, exist_ok=True)
    with SqliteStore(db) as store:
        store.create_run("run.bench", _cfg())
        store.conn.execute(
            "UPDATE runs SET index_build_id = ?, retrieval_profile = 'R0',"
            " retrieval_cutoff_K = 10, retrieval_params_json = ?"
            " WHERE run_id = 'run.bench'",
            (BUILD, json.dumps({"rrf_k": 60})),
        )
        if with_chunks:
            store.upsert_corpus_doc(
                CorpusDoc(doc_id="doc", product_id=PRODUCT, url="https://example.test/p")
            )
            store.add_chunks_batch(
                [
                    CorpusChunk(
                        chunk_id="c1",
                        doc_id="doc",
                        product_id=PRODUCT,
                        text=TEXT,
                        heading_path="H",
                        content_hash="hash-c1",
                        build_id=BUILD,
                    )
                ]
            )
        store.save_index_manifest(
            IndexManifestEntry(
                index_build_id=BUILD,
                product_id=PRODUCT,
                package_id=PKG,
            )
        )
        store.conn.commit()

def _golden(tmp_path: Path) -> Path:
    path = tmp_path / "golden.csv"
    path.write_text(
        "product_id,param_id,expected_status,expected_evidence_kind,case_class\n"
        "p1,param_a,✅,documentation,class_a\n",
        encoding="utf-8",
    )
    return path

def _args(db: Path, golden: Path, *extra: str) -> list[str]:
    return [
        "experiment",
        "evaluator-benchmark",
        "--db",
        str(db),
        "--run-id",
        "run.bench",
        "--golden",
        str(golden),
        "--run-yaml",
        str(RUN_YAML),
        "--package",
        str(FIXTURES),
        *extra,
    ]

@pytest.fixture(autouse=True)
def _offline_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Endpoint-заглушка: реальный путь сборки, но без сети (generate-шов)."""
    monkeypatch.setenv("COHOUND_LLM_BASE_URL", "http://bench.invalid")
    monkeypatch.delenv("COHOUND_LLM_MODEL", raising=False)
    monkeypatch.delenv("COHOUND_LLM_SLOT", raising=False)

# ---------------------------------------------------------------------------
# Парсер
# ---------------------------------------------------------------------------

def test_parser_accepts_repeated_model() -> None:
    args = build_parser().parse_args(
        [
            "experiment",
            "evaluator-benchmark",
            "--db",
            "x.db",
            "--golden",
            "g.csv",
            "--model",
            "m-one",
            "--model",
            "m-two",
            "--repeats",
            "2",
            "--limit",
            "5",
        ]
    )
    assert args.model == ["m-one", "m-two"]
    assert args.repeats == 2
    assert args.limit == 5
    assert args.handler.__name__ == "run_evaluator_benchmark"

# ---------------------------------------------------------------------------
# Успешное измерение и форма payload
# ---------------------------------------------------------------------------

def test_benchmark_json_payload_and_artifact(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store(db)
    code = main(
        _args(db, _golden(tmp_path), "--model", "flag-model", "--json"),
        ports=_ports(),
        evaluator_generate=_generate,
    )
    payload = json_out(capsys)
    assert code == EXIT_OK
    assert payload["ok"] is True
    assert payload["status"] == "measured"
    assert payload["kind"] == "evaluator_benchmark"
    assert payload["n_cells"] == 1
    assert payload["n_products"] == 1
    assert payload["retrieval"] == {"profile": "R0", "cutoff_k": 10, "rrf_k": 60}
    assert payload["contract"]["seed"] == 0
    assert payload["contract"]["temperature"] == 0.0
    assert payload["contract"]["web_tool"] is False

    model = payload["models"][0]
    assert model["label"] == "flag-model"
    assert model["axes"]["model_id"] == "flag-model"
    assert model["axes"]["web_tool"] is False
    for key in (
        "validity",
        "failure_kinds",
        "agreement",
        "abstention",
        "first_attempt_pass",
        "tokens",
        "latency_ms",
        "determinism",
    ):
        assert key in model
    assert model["validity"]["ci95"]["low"] <= model["validity"]["rate"] <= model[
        "validity"
    ]["ci95"]["high"]
    assert model["abstention"]["fabricated_support"] == 0

    artifact = Path(payload["artifact"])
    assert artifact.is_file()
    assert artifact.parent == db.parent / "benchmark"
    written = json.loads(artifact.read_text(encoding="utf-8"))
    assert written["models"][0]["axes"]["model_id"] == "flag-model"

def test_repeated_model_flag_runs_every_candidate(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store(db)
    code = main(
        _args(
            db,
            _golden(tmp_path),
            "--model",
            "model-a",
            "--model",
            "model-b",
            "--json",
        ),
        ports=_ports(),
        evaluator_generate=_generate,
    )
    payload = json_out(capsys)
    assert code == EXIT_OK
    assert [m["label"] for m in payload["models"]] == ["model-a", "model-b"]

def test_model_flag_wins_over_env(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Регрессия identity-бага: ``COHOUND_LLM_MODEL`` не выбирает модель benchmark'а."""
    monkeypatch.setenv("COHOUND_LLM_MODEL", "env-model")
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store(db)
    code = main(
        _args(db, _golden(tmp_path), "--model", "flag-model", "--json"),
        ports=_ports(),
        evaluator_generate=_generate,
    )
    payload = json_out(capsys)
    assert code == EXIT_OK
    assert payload["models"][0]["axes"]["model_id"] == "flag-model"
    assert "env-model" not in json.dumps(payload)

def test_table_output_is_compact(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store(db)
    code = main(
        _args(db, _golden(tmp_path), "--model", "flag-model"),
        ports=_ports(),
        evaluator_generate=_generate,
    )
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert "status: measured" in out
    assert "| model | validity" in out
    assert "flag-model" in out

# ---------------------------------------------------------------------------
# Честная деградация
# ---------------------------------------------------------------------------

def test_missing_chunks_exits_nonzero_with_reason(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store(db, with_chunks=False)
    code = main(
        _args(db, _golden(tmp_path), "--model", "flag-model", "--json"),
        ports=_ports(with_chunks=False),
        evaluator_generate=_generate,
    )
    payload = json_out(capsys)
    assert code == EXIT_ERROR
    assert payload["ok"] is False
    assert payload["status"] == "skipped"
    assert payload["skip_reason"] == "no_cases"
    assert "no_chunks" in payload["message"]

def test_missing_golden_is_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store(db)
    code = main(
        _args(db, tmp_path / "absent.csv", "--model", "flag-model", "--json"),
        ports=_ports(),
        evaluator_generate=_generate,
    )
    payload = json_out(capsys)
    assert code == EXIT_USAGE
    assert payload["ok"] is False
