"""Тесты ``hound experiment retrieval`` — тонкая поверхность + артефакт фиксации (P4b, A3).

Что доказывается (Deliverable C):

* команда разрешает зафиксированную сборку **из строки runs** (``index_build_id``/
  ``package_id`` + ``index_manifest``), прогоняет R0/R1/R2 на инъектированном
  детерминированном эмбеддере и записывает измерение в ``runs``;
* выбранный профиль/K/``rrf_k``/``embed_model_id`` и ``settings_provenance`` =
  ``{"…": {"source": "measured"}}`` действительно персистятся и видны ``read_manifest``
  + §13-границе ``diff`` (``execution_change``);
* **NB1**: расхождение эмбеддера запроса и сборки (сравнение по ``model_id``, не по ``dim``)
  → отказ с обоими идентификаторами; отсутствие сборки/эмбеддера → честный ``skipped``
  (N3), без выдуманного ранжирования и без падения.

Реальный dense-инференс не выполняется (нет локального снапшота model2vec, нет
``sentence_transformers``): механизм проверяется через шов ``embedder`` (инъекция), а не
подделкой «настоящего» измерения. Данные категорийно-нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest

from core.adapters import index_format
from core.adapters.embed_model2vec import LiteEmbedder
from core.cli.main import EXIT_ERROR, EXIT_OK, EXIT_USAGE, main
from core.cli.wiring import build_adapters, assert_embedder_matches_build, EmbedderBuildMismatch
from core.domain import CorpusChunk, CorpusDoc, IndexManifestEntry
from core.domain.enums import RankingPolicy, RunMode
from core.domain.models import Focus, RunConfig
from core.pipeline.report import diff_boundary_violations
from core.store.sqlite_store import SqliteStore
from tests.helpers.output import json_out

PKG = "pkg-exp"
BUILD = "ib_exp"
PRODUCT = "p-alpha"
MODEL_ID = "test-model@sha1"

#: Чанки продукт-партиции: текст для лексического плеча, векторы под dense-запрос [1, 0].
TEXTS = {
    "a0": "alpha document text",
    "a1": "beta-only document",
}
VECTORS = {
    "a0": [1.0, 0.0],
    "a1": [0.0, 1.0],
}


def _fake_embedder(model_id: str = MODEL_ID) -> LiteEmbedder:
    """Детерминированный эмбеддер через шов ``embed_fn`` (без модели/сети)."""
    return LiteEmbedder(embed_fn=lambda texts: [[1.0, 0.0] for _ in texts], model_id=model_id, dim=2)


def _cfg() -> RunConfig:
    return RunConfig(
        case_id="case_exp",
        anchor=PRODUCT,
        cohort="cohort_exp",
        products=[PRODUCT],
        package=PKG,
        focus=Focus(modules=["mod"], params=["param"]),
        languages=["en"],
        snapshot_date=_dt.date(2026, 6, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _write_build(root: Path, *, embed_model_id: str | None = MODEL_ID) -> None:
    manifest: dict[str, object] = {"dim": 2, "package": PKG}
    if embed_model_id is not None:
        manifest["embed_model_id"] = embed_model_id
    index_format.write_partition(
        root,
        PKG,
        BUILD,
        PRODUCT,
        [VECTORS[cid] for cid in TEXTS],
        list(TEXTS),
        manifest,
    )


def _store(
    db: Path,
    run_id: str,
    *,
    index_build_id: str | None = BUILD,
    embed_model_id: str | None = MODEL_ID,
) -> None:
    """Создаёт прогон, зафиксированную сборку в БД и персистентные чанки."""
    db.parent.mkdir(parents=True, exist_ok=True)
    with SqliteStore(db) as store:
        store.create_run(run_id, _cfg())
        if index_build_id is not None:
            store.conn.execute(
                "UPDATE runs SET index_build_id = ? WHERE run_id = ?",
                (index_build_id, run_id),
            )
        store.upsert_corpus_doc(
            CorpusDoc(doc_id="doc", product_id=PRODUCT, url="https://example.test/p")
        )
        store.add_chunks_batch(
            [
                CorpusChunk(
                    chunk_id=cid,
                    doc_id="doc",
                    product_id=PRODUCT,
                    text=text,
                    heading_path="H",
                    content_hash=f"hash-{cid}",
                    build_id=BUILD,
                )
                for cid, text in TEXTS.items()
            ]
        )
        store.save_index_manifest(
            IndexManifestEntry(
                index_build_id=BUILD,
                product_id=PRODUCT,
                package_id=PKG,
                embed_model_id=embed_model_id,
                embed_dim=2,
            )
        )
        store.conn.commit()


def _golden(tmp_path: Path) -> Path:
    path = tmp_path / "retrieval_golden.json"
    path.write_text(
        json.dumps([{"query": "alpha", "expected_ids": ["a0"], "lang": "en"}]),
        encoding="utf-8",
    )
    return path


def _ports(index_root: Path, store: SqliteStore, *, embedder: LiteEmbedder | None):
    """P4a-сборка портов с инъекцией эмбеддера (детерминированный шов)."""
    return build_adapters(
        profile="local",
        package=PKG,
        index_root=index_root,
        store=store,
        chunk_lookup=store.get_chunk,
        index_build_id=BUILD,
        embedder=embedder,
    )


# ---------------------------------------------------------------------------
# Успешное измерение + артефакт фиксации
# ---------------------------------------------------------------------------


def test_experiment_measures_and_persists_provenance(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    index_root = tmp_path / "index_store"
    _write_build(index_root)
    monkeypatch.setenv("COHOUND_INDEX_STORE_DIR", str(index_root))
    db = tmp_path / "runs" / "run.a" / "cohound.db"
    _store(db, "run.a")

    with SqliteStore(db) as store:
        ports = _ports(index_root, store, embedder=_fake_embedder())
        pre = store.read_manifest("run.a")
        code = main(
            [
                "experiment",
                "retrieval",
                "--db",
                str(db),
                "--run-id",
                "run.a",
                "--golden",
                str(_golden(tmp_path)),
                "--cutoff-k",
                "3",
                "--json",
            ],
            ports=ports,
        )
    payload = json_out(capsys)
    assert code == EXIT_OK
    assert payload["status"] == "measured"
    assert payload["selected_profile"] in {"R0", "R1", "R2"}
    assert payload["cutoff_k"] == 3
    assert payload["rrf_k"] == 60
    assert payload["embed_model_id"] == MODEL_ID
    assert payload["experiment_id"] == f"retrieval/{BUILD}/K3"
    assert payload["metrics"]["R1"]["recall_at_k"] == pytest.approx(1.0)
    assert payload["skipped"] == {}

    # Артефакт фиксации лёг в runs: измеренные поля + provenance=measured.
    with SqliteStore(db) as store:
        post = store.read_manifest("run.a")
    assert post.retrieval_profile == payload["selected_profile"]
    assert post.retrieval_cutoff_K == 3
    assert post.embed_model_id == MODEL_ID
    assert json.loads(post.retrieval_params_json or "{}")["selected_profile"] == payload[
        "selected_profile"
    ]
    for name in ("retrieval_profile", "retrieval_cutoff_K", "embed_model_id", "rrf_k"):
        assert post.settings_provenance[name]["source"] == "measured"
        assert post.settings_provenance[name]["experiment_id"] == f"retrieval/{BUILD}/K3"

    # Кто наблюдает: §13-граница diff помечает изменение как execution_change.
    violations = diff_boundary_violations(pre, post)
    assert any(v.startswith("execution_change: retrieval_profile:") for v in violations)
    assert any(v.startswith("execution_change: retrieval_cutoff_K:") for v in violations)
    assert not any(v.startswith("methodology_change") for v in violations)


def test_experiment_cutoff_falls_back_to_manifest_K(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """K без ``--cutoff-k`` берётся из зафиксированного ``runs.retrieval_cutoff_K`` (I20)."""
    index_root = tmp_path / "index_store"
    _write_build(index_root)
    monkeypatch.setenv("COHOUND_INDEX_STORE_DIR", str(index_root))
    db = tmp_path / "runs" / "run.k" / "cohound.db"
    _store(db, "run.k")
    with SqliteStore(db) as store:
        store.conn.execute("UPDATE runs SET retrieval_cutoff_K = 7 WHERE run_id = 'run.k'")
        store.conn.commit()
        ports = _ports(index_root, store, embedder=_fake_embedder())
        code = main(
            [
                "experiment",
                "retrieval",
                "--db",
                str(db),
                "--run-id",
                "run.k",
                "--golden",
                str(_golden(tmp_path)),
                "--json",
            ],
            ports=ports,
        )
    payload = json_out(capsys)
    assert code == EXIT_OK
    assert payload["cutoff_k"] == 7


# ---------------------------------------------------------------------------
# NB1: эмбеддер ↔ сборка — отказ при расхождении (по model_id, не по dim)
# ---------------------------------------------------------------------------


def test_experiment_refuses_on_embedder_build_mismatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    index_root = tmp_path / "index_store"
    _write_build(index_root, embed_model_id="other-model@xyz")
    monkeypatch.setenv("COHOUND_INDEX_STORE_DIR", str(index_root))
    db = tmp_path / "runs" / "run.m" / "cohound.db"
    _store(db, "run.m", embed_model_id="other-model@xyz")

    with SqliteStore(db) as store:
        ports = _ports(index_root, store, embedder=_fake_embedder())
        pre = store.read_manifest("run.m")
        code = main(
            [
                "experiment",
                "retrieval",
                "--db",
                str(db),
                "--run-id",
                "run.m",
                "--golden",
                str(_golden(tmp_path)),
            ],
            ports=ports,
        )
    err = capsys.readouterr().err
    assert code == EXIT_ERROR
    assert MODEL_ID in err and "other-model@xyz" in err
    # Отказ не пишет артефакт: измерение не состоялось.
    with SqliteStore(db) as store:
        post = store.read_manifest("run.m")
    assert post.retrieval_profile is None
    assert diff_boundary_violations(pre, post) == []


def test_assert_embedder_matches_build_unit() -> None:
    assert_embedder_matches_build("m1", "m1")  # равные id — ok
    with pytest.raises(EmbedderBuildMismatch) as exc:
        assert_embedder_matches_build("m1", "m2")
    assert exc.value.query_model_id == "m1"
    assert exc.value.build_model_id == "m2"


# ---------------------------------------------------------------------------
# N3: отсутствие предпосылки — честный skipped (exit 0), без подделки
# ---------------------------------------------------------------------------


def test_experiment_skips_without_index_build(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "runs" / "run.n" / "cohound.db"
    _store(db, "run.n", index_build_id=None)

    code = main(
        [
            "experiment",
            "retrieval",
            "--db",
            str(db),
            "--run-id",
            "run.n",
            "--golden",
            str(_golden(tmp_path)),
            "--json",
        ]
    )
    payload = json_out(capsys)
    assert code == EXIT_OK
    assert payload["status"] == "skipped"
    assert payload["skip_reason"] == "no_index_build"
    with SqliteStore(db) as store:
        assert store.read_manifest("run.n").retrieval_profile is None


def test_experiment_skips_when_build_lacks_embed_model_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Сборка без ``embed_model_id``: согласованность недоказуема → skip, не выдумка (NB1)."""
    index_root = tmp_path / "index_store"
    _write_build(index_root, embed_model_id=None)
    monkeypatch.setenv("COHOUND_INDEX_STORE_DIR", str(index_root))
    db = tmp_path / "runs" / "run.u" / "cohound.db"
    _store(db, "run.u", embed_model_id=None)

    with SqliteStore(db) as store:
        # Инъецированный эмбеддер доступен, но сборка не объявляет свою модель.
        ports = _ports(index_root, store, embedder=_fake_embedder())
        code = main(
            [
                "experiment",
                "retrieval",
                "--db",
                str(db),
                "--run-id",
                "run.u",
                "--golden",
                str(_golden(tmp_path)),
                "--json",
            ],
            ports=ports,
        )
    payload = json_out(capsys)
    assert code == EXIT_OK
    assert payload["skip_reason"] == "embedder_unverified"


def test_experiment_requires_args(tmp_path: Path) -> None:
    """Без обязательных аргументов (``--db``/``--runs-dir``, ``--golden``) — usage."""
    assert main(["experiment", "retrieval"]) == EXIT_USAGE
    assert main(["experiment"]) == EXIT_USAGE
