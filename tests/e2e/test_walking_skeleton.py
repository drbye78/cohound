"""Сквозной walking-skeleton e2e (L4b): реальный конвейер Ш1–Ш9 на фейк-портах.

Самый маленький **честный** сквозной прогон: настоящие фазы (``build_phases``) и
настоящий store/расчёт/отчёт (``SqliteStore`` + ``compute`` + ``report``), а внешние
системы заменены детерминированными заглушками (``tests/fakes/ports.py``). Ни сети,
ни GPU, ни LLM — ``Runner`` собирается напрямую (composition root CLI здесь не
задействован: его развивает отдельный лейн).

Инварианты, которые держит скелет:

* **I1** — у ``evaluated``-ячейки ≥1 evidence;
* **I10/I15** — ``source_id`` резолвится системой (store), URL не генерирует LLM;
* **I17/I20** — ретривал продукт-скоупед и идёт с фиксированным ``cutoff_K``;
* **I19** — цитируемый чанк прошёл путь ``corpus_raw → extract → chunk → hash → index``.
  Доказательство: ``store.resolve(source_id)`` находит чанк именно в сборке корпуса,
  его ``content_hash`` совпадает с хешем цитируемого текста, а сырые байты сохранены
  в ``corpus_raw/``.

**Гейты — только человек (S9/I6/I7).** G1–G4 пишутся в ``gate_decisions`` явным
синтетическим актором (``e2e-smoke``). Это тест **играет человека**, а не
автоматизация гейта: без соответствующей строки ``Runner`` обязан остановиться
(см. негативный тест ``test_walking_skeleton_blocks_without_g1``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml

from core.cli.wiring import build_store
from core.config.seeds import load_seeds
from core.domain import CellState, CorpusChunk, LLMCellOutput, LLMEvidence, Status
from core.pipeline.corpus import _content_token, _derive_index_build_id
from core.pipeline.gates import gate_g1, gate_g2, gate_g3, gate_g4
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import RunInputs, load_inputs
from core.pipeline.phases import build_phases
from core.pipeline.plan import RunPlan
from core.pipeline.runner import PhaseFn, Ports, Runner
from core.ports.types import RawPage
from core.ports.vector_index import RetrievedChunk
from core.util.hashing import content_hash
from tests.helpers.packages import plan_with_identity
from tests.fakes.ports import (
    FIXED_TS,
    FakeChunker,
    FakeCrawler,
    FakeEmbedder,
    FakeEvaluator,
    FakeExtractor,
    FakeIndexBuilder,
    FakeLexicalIndex,
)

#: Корень фикстур e2e (``tests/fixtures/e2e``).
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "e2e"
RUN_YAML = FIXTURES / "run.yaml"

#: Фиксированное содержимое единственной страницы (детерминизм chunk_id/content_hash).
PAGE_URL = "https://example.com/a"
PAGE_TEXT = "Функция alpha подтверждена в официальной документации продукта."
PAGE_BYTES = PAGE_TEXT.encode("utf-8")

#: ``chunk_id``, который выведет корпус: ``{product}:{doc_hash}:{chunk_no}`` (corpus._chunk_id).
#: ``doc_hash`` = SHA-256 сырых байтов (совпадает с ``FakeExtractor``/``corpus``).
DOC_HASH = content_hash(PAGE_BYTES)
CHUNK_ID = f"p1:{DOC_HASH}:0"

#: Явный синтетический актор: тест играет человека на гейтах (не автоматизация, S9/I6/I7).
ACTOR = "e2e-smoke"


class _Clock:
    """Фиксированные часы (детерминизм манифеста и решений гейтов)."""

    def now_iso(self) -> str:
        return FIXED_TS


def _load() -> tuple[RunInputs, RunPlan]:
    """Нормализует входы фикстуры: ``load_inputs → lint → plan`` (§4.5)."""
    inputs = load_inputs(RUN_YAML, FIXTURES)
    lint_run_inputs(inputs).raise_if_blocking()
    seeds = load_seeds(FIXTURES / "sources" / "seeds.yaml")
    return inputs, plan_with_identity(inputs, seeds=seeds)


def _expected_build_id(inputs: RunInputs, run_plan: RunPlan) -> str:
    """``index_build_id`` корпуса — через ту же производящую функцию (без дублирования).

    P2: id **контент-адресуемый**, поэтому тест воспроизводит отпечаток содержимого
    единственной продукт-партиции из тех же байтов страницы, что увидит корпус
    (``content_hash`` чанка — от чистого текста, I19). Пакет корпуса: у ``PackageManifest``
    нет ``package_id``, поэтому ``phases._package_id`` вернёт ``plan.run_id``.
    """
    chunk = CorpusChunk(
        chunk_id=CHUNK_ID,
        doc_id=f"p1:{DOC_HASH}",
        product_id="p1",
        text=PAGE_TEXT,
        content_hash=content_hash(PAGE_TEXT),
    )
    return _derive_index_build_id(
        package_id=run_plan.run_id,
        params_token=",".join(sorted(row.param_id for row in run_plan.params)),
        snapshot_date=str(inputs.run.snapshot_date),
        chunker_version=FakeChunker.version,
        embed_model_id=FakeEmbedder().model_id,
        extractor_version=FakeExtractor.version,
        lexical_engine_version=FakeIndexBuilder.lexical_engine_version,
        vector_store=FakeIndexBuilder.vector_store,
        content_token=_content_token({"p1": [chunk]}),
    )


def _build_ports(build_id: str) -> Ports:
    """Собирает порты: фейки внешних систем + лексический индекс, засеянный корпусом.

    ``FakeLexicalIndex`` хранит ``RetrievedChunk`` с ровно тем ``chunk_id``/``content_hash``,
    который выведет корпус из тех же байтов страницы. Это модель того, что в проде
    физический индекс строится из corpus-сборки, поэтому ретривал видит тот же чанк (I19).
    """
    lexical = FakeLexicalIndex(
        docs=[
            RetrievedChunk(
                chunk_id=CHUNK_ID,
                product_id="p1",
                text=PAGE_TEXT,
                content_hash=content_hash(PAGE_TEXT),
                heading_path="",
                build_id=build_id,
                score=1.0,
            )
        ],
        engine_version=build_id,
    )
    evaluator = FakeEvaluator(
        default=LLMCellOutput(
            status=Status.OK,
            justification="Подтверждено корпусным фрагментом.",
            evidence=[LLMEvidence(source_id=CHUNK_ID, quote=PAGE_TEXT)],
        )
    )
    return Ports(
        evaluator=evaluator,
        crawler=FakeCrawler(
            pages={PAGE_URL: RawPage(url=PAGE_URL, content=PAGE_BYTES, fetched_at=FIXED_TS)}
        ),
        extractor=FakeExtractor(),
        chunker=FakeChunker(),
        embedder=FakeEmbedder(),
        index_builder=FakeIndexBuilder(),
        lexical_index=lexical,
        vector_index=None,
        search=None,
        registry=None,
    )


def _build_phases(
    inputs: RunInputs,
    run_plan: RunPlan,
    ports: Ports,
    store: Any,
    tmp_path: Path,
) -> dict[str, PhaseFn]:
    """Собирает реальные замыкания фаз Ш1–Ш9 (гейты не переопределяются — S9)."""
    return build_phases(
        inputs=inputs,
        plan=run_plan,
        ports=ports,
        urls={"p1": [PAGE_URL]},
        passports={},  # наследования нет: только корпус (Ш4 пропускается)
        out_dir=str(tmp_path),
        raw_root=str(tmp_path / "corpus_raw"),
        snapshot_date=inputs.run.snapshot_date,
        resolver=store,  # I10/I15/I19: store — системный резолвер source_id
    )


def _record_human_gates(store: Any, run_id: str) -> None:
    """Записывает решения G1–G4 явным актором — тест играет человека (S9/I6/I7)."""
    clock = _Clock()
    for gate in (gate_g1, gate_g2, gate_g3, gate_g4):
        gate(store, run_id, actor=ACTOR, notes="walking skeleton", clock=clock)


# ---------------------------------------------------------------------------
# Позитивный скелет: полный Ш1–Ш9 до артефактов
# ---------------------------------------------------------------------------


def test_walking_skeleton_end_to_end(tmp_path: Path) -> None:
    """Ш1–Ш9 проходят целиком: одна корпусная ячейка + артефакты, оба отчёта честны."""
    inputs, run_plan = _load()
    run_id = run_plan.run_id
    build_id = _expected_build_id(inputs, run_plan)

    store = build_store(":memory:")
    store.create_run(run_id, inputs.run)
    _record_human_gates(store, run_id)
    # Аудит-след: ровно четыре человеческих решения (S9).
    assert len(list(store.iter_gate_decisions(run_id))) == 4

    ports = _build_ports(build_id)
    phases = _build_phases(inputs, run_plan, ports, store, tmp_path)
    runner = Runner(store, ports=ports, clock=_Clock(), phases=phases)

    report = runner.run(run_id)

    # Конвейер завершился без остановки/провала.
    assert report.ok is True
    assert report.stopped_at is None

    # Это РЕАЛЬНЫЕ замыкания (не echo-заглушки W4): фазы отработали.
    for phase in ("corpus", "evaluate", "compute", "report"):
        assert runner.state_of(run_id, phase) == "ok", phase

    # I1: ровно одна ячейка (p1, feature.alpha) персистентно, evaluated.
    persisted = list(store.iter_cells(run_id))
    assert len(persisted) == 1
    assert (persisted[0].product_id, persisted[0].param_id) == ("p1", "feature.alpha")
    assert persisted[0].state is CellState.EVALUATED
    assert persisted[0].status is Status.OK

    # Evidence в БД живёт в отдельной таблице, поэтому I1 (§2.3) проверяем на
    # in-memory-ячейке фазы оценки — том же объекте, что был записан через ``add_evidence``.
    evaluate_result = next(result for result in report.results if result.phase == "evaluate")
    assert len(evaluate_result.cells) == 1
    cell = evaluate_result.cells[0]
    assert (cell.product_id, cell.param_id) == ("p1", "feature.alpha")
    assert cell.state is CellState.EVALUATED
    assert cell.status is Status.OK
    assert len(cell.evidence) >= 1

    evidence = cell.evidence[0]
    assert evidence.quote == PAGE_TEXT

    # I10/I15: source_id резолвится системой, а не сгенерирован LLM.
    resolved = store.resolve(evidence.source_id)
    assert resolved is not None

    manifest_path = tmp_path / run_id / "run_manifest.yaml"
    assert manifest_path.exists()
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))

    # I19: цитируемый чанк существует ИМЕННО в корпусной сборке, его хеш совпадает,
    # а сырые байты сохранены до экстракции.
    assert resolved.build_id is not None
    assert resolved.build_id == manifest["index_build_id"]
    assert resolved.content_hash == content_hash(PAGE_TEXT)
    chunks = list(store.iter_chunks(resolved.build_id))
    assert any(chunk.chunk_id == evidence.source_id for chunk in chunks)

    raw_path = tmp_path / "corpus_raw" / "p1" / f"{DOC_HASH}.bin"
    assert raw_path.read_bytes() == PAGE_BYTES

    # I17/I20: ретривал продукт-скоупед и с зафиксированным ``retrieval_cutoff_K``.
    # Провенанс ретривала лёг и на ячейку (R7/S5).
    assert persisted[0].retrieval_profile == "R0"
    assert "cutoff_K" in (persisted[0].retrieval_params_json or "")
    assert persisted[0].index_build_id == manifest["index_build_id"]
    lexical = cast(FakeLexicalIndex, ports.lexical_index)
    assert lexical.calls  # ретривал действительно вызывался
    assert all(call[1] == ["p1"] for call in lexical.calls)

    # Ш9: артефакты. Манифест и battle card записаны (Coverage непустой → I11 не глушит).
    report_result = next(result for result in report.results if result.phase == "report")
    artifacts = report_result.log_entries[0]
    assert artifacts.manifest == manifest_path
    assert artifacts.manifest.exists()
    assert artifacts.battle_card is not None
    assert Path(artifacts.battle_card).exists()


# ---------------------------------------------------------------------------
# Негативный скелет: без человеческого G1 прогон стоит и не считает/не публикует
# ---------------------------------------------------------------------------


def test_walking_skeleton_blocks_without_g1(tmp_path: Path) -> None:
    """Без решения G1 ``Runner`` останавливается ``blocked`` и не доходит до Ш8/Ш9."""
    inputs, run_plan = _load()
    run_id = run_plan.run_id
    build_id = _expected_build_id(inputs, run_plan)

    store = build_store(":memory:")
    store.create_run(run_id, inputs.run)
    # Гейты НЕ записаны: g1 обязан остановить прогон (S9 — гейт не проходится сам).

    ports = _build_ports(build_id)
    phases = _build_phases(inputs, run_plan, ports, store, tmp_path)
    runner = Runner(store, ports=ports, clock=_Clock(), phases=phases)

    report = runner.run(run_id)

    assert report.stopped_at == "g1"
    assert report.status == "blocked"
    # compute/report не выполнялись, ячеек нет, манифест не записан.
    assert runner.state_of(run_id, "evaluate") is None
    assert runner.state_of(run_id, "compute") is None
    assert runner.state_of(run_id, "report") is None
    assert list(store.iter_cells(run_id)) == []
    assert not (tmp_path / run_id / "run_manifest.yaml").exists()
