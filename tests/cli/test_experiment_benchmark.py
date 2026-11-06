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
import shutil
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

def _chunk(text: str = TEXT, *, build_id: str = BUILD) -> RetrievedChunk:
    return RetrievedChunk(
        chunk_id="c1",
        product_id=PRODUCT,
        text=text,
        content_hash="hash-c1",
        heading_path="H",
        build_id=build_id,
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

#: Измеренная форма данных (06.11.2026, ``ru-pg-dbms-c2e7b860ac17``): строка прогона
#: несёт **композицию** ``cb_*``, а ``corpus_chunks.build_id`` — продукт-партицию ``pb_*``;
#: связывает их ``index_manifest.partition_build_id``.
COMPOSITION = "cb_bench_comp"
PARTITION = "pb_bench_part"


def _store_partitioned(db: Path) -> None:
    """Стор в измеренной форме: ``runs.index_build_id = cb_*``, чанки — под ``pb_*``."""
    db.parent.mkdir(parents=True, exist_ok=True)
    with SqliteStore(db) as store:
        store.create_run("run.bench", _cfg())
        store.conn.execute(
            "UPDATE runs SET index_build_id = ?, retrieval_profile = 'R0',"
            " retrieval_cutoff_K = 10, retrieval_params_json = ?"
            " WHERE run_id = 'run.bench'",
            (COMPOSITION, json.dumps({"rrf_k": 60})),
        )
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
                    build_id=PARTITION,
                )
            ]
        )
        store.save_index_manifest(
            IndexManifestEntry(
                index_build_id=COMPOSITION,
                partition_build_id=PARTITION,
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

def test_parser_accepts_reasoning_effort_and_max_tokens() -> None:
    """Объявленные оси замера парсятся: уровень усилия и потолок генерации."""
    args = build_parser().parse_args(
        [
            "experiment",
            "evaluator-benchmark",
            "--db",
            "x.db",
            "--golden",
            "g.csv",
            "--reasoning-effort",
            "high",
            "--max-tokens",
            "16384",
        ]
    )
    assert args.reasoning_effort == "high"
    assert args.max_tokens == 16384


def test_parser_rejects_unknown_reasoning_effort() -> None:
    """Опечатка в уровне — ошибка парсера, а не «оси нет»."""
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            [
                "experiment",
                "evaluator-benchmark",
                "--db",
                "x.db",
                "--golden",
                "g.csv",
                "--reasoning-effort",
                "ultra",
            ]
        )


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

def test_measured_shape_composition_and_partition_are_measured(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Регрессия измеренного дефекта: ``cb_*`` в строке прогона, ``pb_*`` в чанках.

    ``hound experiment evaluator-benchmark`` на реальных прогонах отвечал
    ``no_cases``/``product_not_in_build`` по всем продуктам: проба чанков шла по
    композиции, а ``corpus_chunks.build_id`` несёт партицию. Здесь стор — настоящий
    ``SqliteStore`` (``iter_chunks`` фильтрует ``WHERE build_id = ?``), поэтому тест
    падает ровно там, где падал замер.
    """
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store_partitioned(db)
    ports = SimpleNamespace(
        lexical_index=_FakeLexicalIndex(
            {PRODUCT: [_chunk(build_id=PARTITION)]}, build_id=COMPOSITION
        ),
        vector_index=None,
        embedder=None,
    )
    code = main(
        _args(db, _golden(tmp_path), "--model", "flag-model", "--json"),
        ports=ports,
        evaluator_generate=_generate,
    )
    payload = json_out(capsys)
    assert code == EXIT_OK
    assert payload["status"] == "measured"
    assert payload["n_cells"] == 1
    assert payload["skipped"] == {}
    # Провенанс сборки остаётся композицией (I17/S5), а не партицией.
    assert payload["index_build_id"] == COMPOSITION


def test_product_absent_from_build_names_it_in_message(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Продукт golden без партиции — ``no_cases``, и верхнее сообщение называет продукт."""
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store_partitioned(db)
    golden = tmp_path / "golden_absent.csv"
    golden.write_text(
        "product_id,param_id,expected_status,expected_evidence_kind,case_class\n"
        "p_absent,param_a,✅,documentation,class_a\n",
        encoding="utf-8",
    )
    code = main(
        _args(db, golden, "--model", "flag-model", "--json"),
        ports=SimpleNamespace(
            lexical_index=_FakeLexicalIndex({}, build_id=COMPOSITION),
            vector_index=None,
            embedder=None,
        ),
        evaluator_generate=_generate,
    )
    payload = json_out(capsys)
    assert code == EXIT_ERROR
    assert payload["skip_reason"] == "no_cases"
    assert "p_absent" in payload["message"]
    assert COMPOSITION in payload["message"]
    assert payload["details"]["products_not_in_build"] == ["p_absent"]


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

def test_reasoning_effort_and_max_tokens_reach_measured_axes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Уровень и потолок доезжают до адаптера и попадают в объявленные оси отчёта.

    ``reasoning_effort`` идёт тем же путём, что в прогоне (``build_evaluator_llm`` →
    ``ensure_reasoning_wire`` → ``LocalEvaluator``), ``max_tokens`` — в поле адаптера,
    откуда его читает провод. Иначе отчёт о замере не назвал бы условия замера.
    """
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store(db)
    code = main(
        _args(
            db,
            _golden(tmp_path),
            "--model",
            "deepseek-v4.1-flash-1017:yellow",
            "--reasoning-effort",
            "high",
            "--max-tokens",
            "16384",
            "--json",
        ),
        ports=_ports(),
        evaluator_generate=_generate,
    )
    payload = json_out(capsys)
    assert code == EXIT_OK
    model = payload["models"][0]
    assert model["axes"]["reasoning_effort"] == "high"
    assert model["axes"]["max_tokens"] == 16384
    # Статьи расхода уровня присутствуют в отчёте даже при оффлайн-шве (нули — честно).
    for key in ("reasoning", "finish_reason", "invalid_json", "status_counts"):
        assert key in model


def test_unsupported_reasoning_effort_is_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Невыразимый для модели уровень — ``EXIT_USAGE``, а не тихий пропуск параметра."""
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store(db)
    code = main(
        _args(
            db,
            _golden(tmp_path),
            "--model",
            "flag-model",
            "--reasoning-effort",
            "high",
            "--json",
        ),
        ports=_ports(),
        evaluator_generate=_generate,
    )
    payload = json_out(capsys)
    assert code == EXIT_USAGE
    assert payload["ok"] is False
    assert "невыразимо" in json.dumps(payload, ensure_ascii=False)


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

# ---------------------------------------------------------------------------
# P6b: объявленный отказ эталона виден как своя причина, а не как `no_cases`
# ---------------------------------------------------------------------------


def _declined_package(tmp_path: Path, *, body: str | None = None) -> Path:
    """Копия пакета-фикстуры с объявлением ``sources/golden_status.yaml`` (P6b).

    ``golden.csv`` перезаписывается двумя строками: ``p1`` есть в сборке (строка годна),
    ``p2`` — нет (``product_not_in_build``). Так видно, что главная причина — объявление,
    а причины уровня строк остаются разложением.
    """
    package = tmp_path / PKG
    shutil.copytree(FIXTURES, package)
    (package / "golden.csv").write_text(
        "product_id,param_id,expected_status,expected_evidence_kind,case_class,tier,note\n"
        "p1,param_a,✅,documentation|web,class_a,fast,примечание\n"
        "p2,param_a,✅,documentation|web,class_a,fast,примечание\n",
        encoding="utf-8",
    )
    declaration = body or (
        "schema: golden_status/1\n"
        "state: declined\n"
        "reason: эталон размечен не на те продукты\n"
        "declined_by: analyst-name\n"
        "declined_at: '2026-10-27'\n"
    )
    (package / "sources" / "golden_status.yaml").write_text(declaration, encoding="utf-8")
    return package


def _bench_args(db: Path, package: Path, *extra: str) -> list[str]:
    return [
        "experiment",
        "evaluator-benchmark",
        "--db",
        str(db),
        "--run-id",
        "run.bench",
        "--golden",
        str(package / "golden.csv"),
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
        *extra,
    ]


def test_declined_golden_is_reported_as_its_own_skip(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``skip_reason=golden_declined`` + автор/дата/причина; строки — только разложение.

    До правки здесь был ``no_cases``: инструмент, который обязан мерить точность
    оценщика, скрывал, что эталон объявлен непригодным, — и оператор читал «строки не
    сошлись со сборкой» вместо «эталон отозван человеком».
    """
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store(db)
    package = _declined_package(tmp_path)

    code = main(
        _bench_args(db, package, "--model", "flag-model", "--json"),
        ports=_ports(),
        evaluator_generate=_generate,
    )

    payload = json_out(capsys)
    assert code == EXIT_USAGE  # отозванный вход, не сбой прибора (EXIT_ERROR)
    assert payload["ok"] is False
    assert payload["status"] == "skipped"
    assert payload["skip_reason"] == "golden_declined"
    assert "analyst-name" in payload["message"]
    assert "2026-10-27" in payload["message"]
    assert "эталон размечен не на те продукты" in payload["message"]
    details = payload["details"]
    assert details["golden"]["declined_by"] == "analyst-name"
    assert details["golden"]["declined_at"] == "2026-10-27"
    # Разложение по строкам сохранено: p2 отсутствует в сборке — но это НЕ главная причина.
    assert details["row_skip_reasons"] == {
        "golden_declined": 1,
        "product_not_in_build": 1,
    }


def test_present_declaration_measures_as_before(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``state: present`` — поведение не меняется: измерение состоялось."""
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store(db)
    package = _declined_package(
        tmp_path, body="schema: golden_status/1\nstate: present\n"
    )

    code = main(
        _bench_args(db, package, "--model", "flag-model", "--json"),
        ports=_ports(),
        evaluator_generate=_generate,
    )

    payload = json_out(capsys)
    assert code == EXIT_OK
    assert payload["status"] == "measured"


def test_broken_declaration_is_a_visible_package_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Битое объявление — видимая ошибка пакета, а не «объявления нет»."""
    db = tmp_path / "runs" / "run.bench" / "cohound.db"
    _store(db)
    package = _declined_package(
        tmp_path, body="schema: golden_status/1\nstate: declined\nreason: без автора\n"
    )

    code = main(
        _bench_args(db, package, "--model", "flag-model", "--json"),
        ports=_ports(),
        evaluator_generate=_generate,
    )

    payload = json_out(capsys)
    assert code == EXIT_USAGE
    assert payload["ok"] is False
    assert "golden_status.yaml" in payload["message"]
    assert "declined_by" in payload["message"]
