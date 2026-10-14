"""Гейт воспроизводимости/портируемости (T12.6, кодовая часть; R7/§13).

Один и тот же набор входов, прогнанный дважды, обязан дать **идентичные,
детерминированные** выходы. Это кодовая часть «portability gate»: доменная работа
по кейсам 2–5 (авторство пакетов) вне области теста — здесь проверяется, что сам
конвейер Ш1–Ш9 не вносит недетерминизма.

Стенд переиспользует структуру walking-skeleton e2e (реальные фазы, реальный
``SqliteStore``, детерминированные фейки портов, гейты G1–G4 записаны явным
синтетическим актором) и запускает его дважды в **разные** каталоги прогонов:

* ``run_manifest.yaml`` — совпадает по всем полям, кроме недетерминированных по
  своей природе (:data:`NON_DETERMINISTIC_MANIFEST_FIELDS`);
* ``ScoreBundle.to_dict()`` — равны (R7: расчёт детерминирован);
* набор ячеек ``{(product, param): status}`` — равен;
* ``index_build_id`` — производная от входов, равна между прогонами;
* текстовые/CSV-артефакты — байт-в-байт стабильны (кроме встроенных таймстампов
  офисных форматов, которые исключаются ЯВНО, :data:`NON_DETERMINISTIC_ARTIFACT_SUFFIXES`);
* JSON-провенанс (``retrieval_params_json``/``search_effort_budget_json``) — сериализуется
  с ``sort_keys=True`` и не зависит от порядка вставки ключей (§13: версионная граница).

Всё герметично: без сети/GPU; ``created_at`` пинится детерминированными часами
(``FIXED_TS``), но как *категория* исключается — продовая композиция ставит туда
реальное время.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import yaml

from core.cli.wiring import build_store
from core.domain import (
    CorpusChunk,
    CrawlProvenance,
    CrawlSessionProvenance,
    LLMCellOutput,
    LLMEvidence,
    RunManifest,
    RunMode,
    Status,
)
from core.pipeline.compute import compute
from core.pipeline.corpus import _content_token, _derive_index_build_id
from core.pipeline.gates import gate_g1, gate_g2, gate_g3, gate_g4
from core.pipeline.lint import lint_run_inputs
from core.pipeline.normalize import RunInputs, load_inputs
from core.pipeline.phases import build_phases
from core.pipeline.plan import RunPlan, plan
from core.pipeline.report import build_manifest, write_run_manifest
from core.pipeline.runner import Ports, RunReport, Runner
from core.ports.types import RawPage
from core.ports.vector_index import RetrievedChunk
from core.util.hashing import content_hash
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
DOC_HASH = content_hash(PAGE_BYTES)
CHUNK_ID = f"p1:{DOC_HASH}:0"

#: Явный синтетический актор: тест играет человека на гейтах (S9/I6/I7).
ACTOR = "repro-gate"

#: Поля ``run_manifest.yaml``, выводимые из «настенных» часов и потому недетерминированные
#: по своей природе. В этом стенде ``created_at`` зафиксирован детерминированными часами
#: (:data:`FIXED_TS`), но исключается как **категория**: продовая композиция ставит туда
#: реальное время (``ctx.clock``), поэтому R7-гейт не должен на него опираться.
NON_DETERMINISTIC_MANIFEST_FIELDS: tuple[str, ...] = ("created_at",)

#: Артефакты, чья байтовая стабильность обязана держаться (R7).
DETERMINISTIC_ARTIFACTS: tuple[str, ...] = (
    "diff",
    "nda_questions",
    "battle_card",
    "preliminary_findings",
)

#: Суффиксы артефактов со встроенной меткой времени. XLSX (Open Packaging, zip) несёт
#: ``docProps/core.xml`` с ``created``/``modified`` от ``openpyxl`` — байты неповторимы
#: между прогонами. Такой артефакт исключается из байтового сравнения **явно** (не молча):
#: его логический состав проверяется отдельными юнит-тестами отчёта. При отсутствии
#: ``openpyxl`` battle card деградирует до CSV и тоже сравнивается побайтово.
NON_DETERMINISTIC_ARTIFACT_SUFFIXES: tuple[str, ...] = (".xlsx",)

#: Поля манифеста, обязанные совпадать точно (провенанс R7/§13: версии + индекс +
#: ретривал-контур + ранжирование). Перечислены явно, чтобы регрессия была заметной.
PROVENANCE_FIELDS: tuple[str, ...] = (
    "run_id",
    "methodology_version",
    "package_version",
    "catalog_version",
    "status_dict_version",
    "scoring_policy_version",
    "index_build_id",
    "embed_model_id",
    "chunker_version",
    "extractor_version",
    "lexical_engine_version",
    "vector_store",
    "retrieval_profile",
    "retrieval_cutoff_K",
    "retrieval_params_json",
    "search_effort_budget_json",
    "requested_ranking",
    "computed_ranking",
    "snapshot_date",
    "crawl_provenance",
    # Backlog B (§13): операционные оси краула (границы ``execution_change``).
    "crawl_strategy",
    "crawl_rate_limit",
)

#: Базовые входы ``_derive_index_build_id`` для property-проверки чистоты (P2:
#: контент-адресуемый id; ``retrieval_*`` намеренно не входит в формулу).
_BUILD_ID_BASE: dict[str, Any] = {
    "package_id": "e2e",
    "params_token": "feature.alpha",
    "snapshot_date": "2026-06-01",
    "chunker_version": "fake-chunker-1",
    "embed_model_id": "fake-embedder-1",
    "extractor_version": "fake-extractor-1",
    "lexical_engine_version": "fake-bm25-1",
    "vector_store": "fake",
    "content_token": "c0ffee",
}


class _Clock:
    """Фиксированные часы (детерминизм манифеста и решений гейтов)."""

    def now_iso(self) -> str:
        return FIXED_TS


@dataclass(frozen=True)
class SkeletonRun:
    """Результат одного прогона walking-skeleton в отдельный каталог.

    :param run_id: детерминированный идентификатор прогона (``derive_run_id``).
    :param index_build_id: производный идентификатор индексной сборки (R7/§13).
    :param report: ``RunReport`` прогона (для проверки ``ok``/точек фаз).
    :param manifest: разобранный ``run_manifest.yaml`` (dict).
    :param manifest_path: путь к ``run_manifest.yaml``.
    :param cells: ``{(product_id, param_id): status}`` (статус — строка ``.value``).
    :param cell_provenance: ``{(product_id, param_id): {index_build_id, retrieval_profile,
        retrieval_params_json}}`` — провенанс ретривала на уровне ячейки (R7).
    :param bundle: ``ScoreBundle.to_dict()`` — детерминированный снимок расчёта.
    :param artifacts: имя артефакта → путь (только реально записанные).
    """

    run_id: str
    index_build_id: str
    report: RunReport
    manifest: dict[str, Any]
    manifest_path: Path
    cells: dict[tuple[str, str], str | None]
    cell_provenance: dict[tuple[str, str], dict[str, Any]]
    bundle: dict[str, Any]
    artifacts: dict[str, Path]


# ---------------------------------------------------------------------------
# Переиспользуемая сборка walking-skeleton (копия структуры e2e-скелета)
# ---------------------------------------------------------------------------


def _load() -> tuple[RunInputs, RunPlan]:
    """Нормализует входы фикстуры: ``load_inputs → lint → plan`` (§4.5)."""
    inputs = load_inputs(RUN_YAML, FIXTURES)
    lint_run_inputs(inputs).raise_if_blocking()
    return inputs, plan(inputs)


def _expected_build_id(inputs: RunInputs, run_plan: RunPlan) -> str:
    """``index_build_id`` корпуса — через ту же производящую функцию (без дублирования).

    P2: id **контент-адресуемый**; отпечаток содержимого единственной партиции
    воспроизводится из тех же байтов страницы, что увидит корпус.
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
    который выведет корпус из тех же байтов страницы (I19).
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
    runs_dir: Path,
) -> dict[str, Any]:
    """Собирает реальные замыкания фаз Ш1–Ш9 (гейты не переопределяются — S9)."""
    return build_phases(
        inputs=inputs,
        plan=run_plan,
        ports=ports,
        urls={"p1": [PAGE_URL]},
        passports={},  # наследования нет: только корпус (Ш4 пропускается)
        out_dir=str(runs_dir),
        raw_root=str(runs_dir / "corpus_raw"),
        snapshot_date=inputs.run.snapshot_date,
        resolver=store,  # I10/I15/I19: store — системный резолвер source_id
    )


def _record_human_gates(store: Any, run_id: str) -> None:
    """Записывает решения G1–G4 явным актором — тест играет человека (S9/I6/I7)."""
    clock = _Clock()
    for gate in (gate_g1, gate_g2, gate_g3, gate_g4):
        gate(store, run_id, actor=ACTOR, notes="reproducibility gate", clock=clock)


def _run_skeleton(runs_dir: Path) -> SkeletonRun:
    """Прогоняет walking-skeleton в ``runs_dir`` и возвращает его ключевые выходы.

    Детерминизм обеспечен фиксированными часами, одним и тем же фикстурным пакетом и
    одними и теми же фейками. Гейты G1–G4 пред-записаны явным актором — без них
    ``Runner`` обязан остановиться (см. негативный тест скелета).
    """
    inputs, run_plan = _load()
    run_id = run_plan.run_id
    build_id = _expected_build_id(inputs, run_plan)

    store = build_store(":memory:")
    store.create_run(run_id, inputs.run)
    _record_human_gates(store, run_id)

    ports = _build_ports(build_id)
    phases = _build_phases(inputs, run_plan, ports, store, runs_dir)
    runner = Runner(store, ports=ports, clock=_Clock(), phases=phases)
    report = runner.run(run_id)
    assert report.ok is True, (
        f"скелет не дошёл до конца: stopped_at={report.stopped_at}, "
        f"errors={report.errors}"
    )

    manifest_path = runs_dir / run_id / "run_manifest.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))

    persisted = list(store.iter_cells(run_id))
    cells = {
        (str(cell.product_id), str(cell.param_id)): (
            cell.status.value if cell.status is not None else None
        )
        for cell in persisted
    }
    # Провенанс ретривала живёт на ячейке (R7/S5): он тоже обязан быть детерминирован.
    cell_provenance = {
        (str(cell.product_id), str(cell.param_id)): {
            "index_build_id": cell.index_build_id,
            "retrieval_profile": cell.retrieval_profile,
            "retrieval_params_json": cell.retrieval_params_json,
        }
        for cell in persisted
    }

    # Тот же расчёт, что фаза Ш8: ``compute`` читает store и делегирует домену.
    bundle = compute(
        store=store,
        run_id=run_id,
        catalog={row.param_id: row for row in run_plan.params},
        module_weights=run_plan.renormalized_weights or run_plan.module_weights,
        cohort=run_plan.cohort,
        requested_ranking=inputs.run.ranking_policy,
    )

    report_result = next(result for result in report.results if result.phase == "report")
    artifacts_obj = report_result.log_entries[0]
    artifacts = {
        name: path
        for name, path in {
            "manifest": artifacts_obj.manifest,
            "battle_card": artifacts_obj.battle_card,
            "preliminary_findings": artifacts_obj.preliminary_findings,
            "nda_questions": artifacts_obj.nda_questions,
            "diff": artifacts_obj.diff,
        }.items()
        if path is not None
    }

    return SkeletonRun(
        run_id=run_id,
        index_build_id=build_id,
        report=report,
        manifest=manifest,
        manifest_path=manifest_path,
        cells=cells,
        cell_provenance=cell_provenance,
        bundle=bundle.to_dict(),
        artifacts=artifacts,
    )


def _deterministic_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """Копия манифеста без полей, недетерминированных по своей природе."""
    return {k: v for k, v in manifest.items() if k not in NON_DETERMINISTIC_MANIFEST_FIELDS}


# ---------------------------------------------------------------------------
# 2. Манифест и расчёт идентичны между прогонами
# ---------------------------------------------------------------------------


def test_two_runs_produce_identical_manifest_and_scores(tmp_path: Path) -> None:
    """Два прогона на идентичных входах дают совпадающие манифест, баллы и ячейки (R7)."""
    first = _run_skeleton(tmp_path / "runs_a")
    second = _run_skeleton(tmp_path / "runs_b")

    # run_id — производная входов (derive_run_id), значит тоже обязан совпасть.
    assert first.run_id == second.run_id

    # Недетерминированный ``created_at`` исключён ЯВНО; всё остальное — точно равно.
    assert _deterministic_manifest(first.manifest) == _deterministic_manifest(second.manifest)
    for field in PROVENANCE_FIELDS:
        assert first.manifest[field] == second.manifest[field], field

    # Расчёт (Ш8) детерминирован: ScoreBundle.to_dict() равны (R7).
    assert first.bundle == second.bundle

    # Набор ячеек ``{(product, param): status}`` равен; статус — честный результат
    # фейк-оценщика (не подменяем).
    assert first.cells == second.cells
    assert first.cells == {("p1", "feature.alpha"): Status.OK.value}

    # Провенанс ретривала на ячейке (R7/S5) также детерминирован. Замечание: в манифесте
    # поля ретривал-контура сейчас ``None`` (build_phases не пробрасывает дефолты фазы Ш6 в
    # build_manifest) — это пробел провенанса, а не недетерминизм; фиксируем равенство того,
    # что конвейер реально записал.
    assert first.cell_provenance == second.cell_provenance

    # Идентификатор индексной сборки — производная входов (R7/§13).
    assert first.index_build_id == second.index_build_id
    assert first.manifest["index_build_id"] == first.index_build_id

    # Фазы реально отработали (не echo-заглушки) и завершились успешно.
    executed = {result.phase: result.status for result in first.report.results}
    assert first.report.stopped_at is None
    for phase in ("corpus", "evaluate", "compute", "report"):
        assert executed.get(phase) == "ok", phase


# ---------------------------------------------------------------------------
# 3. Байтовая стабильность детерминированных артефактов
# ---------------------------------------------------------------------------


def test_artifacts_are_byte_stable(tmp_path: Path) -> None:
    """Текстовые/CSV-артефакты прогона байт-в-байт совпадают между прогонами (R7).

    ``diff.md`` несёт ``run_id`` — он детерминирован (``derive_run_id``), поэтому
    байтовая стабильность здесь ожидаема и проверяется. XLSX исключается явно:
    ``openpyxl`` пишет ``created``/``modified`` в ``docProps/core.xml`` (см.
    :data:`NON_DETERMINISTIC_ARTIFACT_SUFFIXES`); молчаливого пропуска нет — факт
    исключения фиксируется в комментарии и покрыт отдельными тестами отчёта.
    """
    first = _run_skeleton(tmp_path / "runs_a")
    second = _run_skeleton(tmp_path / "runs_b")

    compared: list[str] = []
    for name in DETERMINISTIC_ARTIFACTS:
        path_a = first.artifacts.get(name)
        path_b = second.artifacts.get(name)
        assert path_a is not None and path_b is not None, f"артефакт {name} не записан"
        if path_a.suffix in NON_DETERMINISTIC_ARTIFACT_SUFFIXES:
            # Явное исключение: офисный формат несёт встроенный таймстамп.
            continue
        assert path_a.read_bytes() == path_b.read_bytes(), f"артефакт {name} нестабилен"
        compared.append(name)

    # Ключевые детерминированные артефакты обязаны быть реально сравнены.
    assert "diff" in compared
    assert "nda_questions" in compared


# ---------------------------------------------------------------------------
# 4. JSON-провенанс сериализуется независимо от порядка ключей (§13)
# ---------------------------------------------------------------------------


@dataclass
class _ManifestProbe:
    """Duck-typed run для ``build_manifest``: ``RunConfig`` не несёт ретривал-JSON полей.

    ``build_manifest`` читает поля через ``getattr``, поэтому минимального объекта
    достаточно, чтобы проверить именно сериализацию (``sort_keys=True``), не
    конструируя продовый ``RunConfig``.
    """

    case_id: str = "e2e"
    mode: RunMode = RunMode.STRICT
    snapshot_date: str | None = "2026-06-01"
    anchor: str = "p1"
    retrieval_params_json: Any = None
    search_effort_budget_json: Any = None


def _probe_manifest(
    *,
    retrieval_params: Any = None,
    search_effort_budget: Any = None,
    edge_capabilities: dict[str, Any] | None = None,
    crawl_provenance: Any = None,
) -> RunManifest:
    """Собирает манифест из duck-typed входа (для проверки детерминизма сериализации)."""
    run = _ManifestProbe(
        retrieval_params_json=retrieval_params,
        search_effort_budget_json=search_effort_budget,
    )
    inputs = SimpleNamespace(run=run)
    plan_like = SimpleNamespace(run_id="probe-run")
    return build_manifest(
        inputs=cast(RunInputs, inputs),
        plan=cast(RunPlan, plan_like),
        edge_capabilities=edge_capabilities,
        crawl_provenance=crawl_provenance,
    )


def test_json_provenance_is_sort_keyed(tmp_path: Path) -> None:
    """``retrieval_params_json``/``search_effort_budget_json`` стабильны при разном порядке ключей."""
    # Одинаковая логическая карта, разный порядок вставки.
    params_ordered = {"profile": "R0", "cutoff_K": 10, "rrf_k": 60}
    params_reordered = {"rrf_k": 60, "cutoff_K": 10, "profile": "R0"}

    manifest_a = _probe_manifest(
        retrieval_params=params_ordered, search_effort_budget=params_ordered
    )
    manifest_b = _probe_manifest(
        retrieval_params=params_reordered, search_effort_budget=params_reordered
    )

    # Байтовая строка JSON одинакова (sort_keys=True) и логически равна исходной карте.
    assert manifest_a.retrieval_params_json == manifest_b.retrieval_params_json
    assert manifest_a.search_effort_budget_json == manifest_b.search_effort_budget_json
    assert manifest_a.retrieval_params_json == (
        '{"cutoff_K": 10, "profile": "R0", "rrf_k": 60}'
    )
    assert json.loads(cast(str, manifest_a.retrieval_params_json)) == params_ordered
    assert json.loads(cast(str, manifest_a.search_effort_budget_json)) == params_ordered

    # Сериализация самого манифеста также не зависит от порядка ключей в словарях.
    manifest_c = _probe_manifest(
        edge_capabilities={"gateway": "1.0", "schema": "2026-01-01"}
    )
    manifest_d = _probe_manifest(
        edge_capabilities={"schema": "2026-01-01", "gateway": "1.0"}
    )
    path_c = write_run_manifest(manifest_c, tmp_path / "c")
    path_d = write_run_manifest(manifest_d, tmp_path / "d")
    assert path_c.read_bytes() == path_d.read_bytes()


def test_crawl_provenance_is_stable_and_sort_keyed(tmp_path: Path) -> None:
    """Phase 2: провенанс краула сериализуется детерминированно (R7)."""
    provenance = CrawlProvenance(
        sessions=[
            CrawlSessionProvenance(
                session_id="crawl:probe-run",
                profile="local",
                status="completed",
                engines=["scrapy", "http"],
                kinds=["web"],
                total=2,
                fetched=2,
            )
        ],
        engines=["http", "scrapy"],
        kinds=["web"],
        crawl_sources_digest="digest-1",
    )
    manifest_a = _probe_manifest(crawl_provenance=provenance)
    manifest_b = _probe_manifest(crawl_provenance=provenance.model_dump(mode="json"))

    # Mapping и модель дают один и тот же типизированный провенанс.
    assert manifest_a.crawl_provenance == manifest_b.crawl_provenance == provenance
    path_a = write_run_manifest(manifest_a, tmp_path / "a")
    path_b = write_run_manifest(manifest_b, tmp_path / "b")
    assert path_a.read_bytes() == path_b.read_bytes()


# ---------------------------------------------------------------------------
# 5. ``index_build_id`` — чистая функция входов (§B.1/§13)
# ---------------------------------------------------------------------------

def test_build_id_is_input_determined() -> None:
    """``_derive_index_build_id`` детерминирован и чувствителен к каждому входу (P2)."""
    base_id = _derive_index_build_id(**_BUILD_ID_BASE)
    assert base_id.startswith("ib_")
    # Чистота: повторный вызов с теми же входами → тот же id (без random/UUID).
    assert _derive_index_build_id(**_BUILD_ID_BASE) == base_id

    # Изменение любого входа (включая новые P2: extractor/lexical/vector/content) даёт другую
    # сборку (иммутабельность §B.1/§13).
    variants = [
        {"package_id": "other-package"},
        {"params_token": "feature.beta"},
        {"chunker_version": "fake-chunker-2"},
        {"embed_model_id": "fake-embedder-2"},
        {"snapshot_date": "2026-07-01"},
        {"extractor_version": "fake-extractor-2"},
        {"lexical_engine_version": "fake-bm25-2"},
        {"vector_store": "other-store"},
        {"content_token": "deadbeef"},
    ]
    variant_ids = {
        _derive_index_build_id(**(_BUILD_ID_BASE | variant)) for variant in variants
    }
    assert base_id not in variant_ids
    assert len(variant_ids) == len(variants)

    # ``None``-снимок эквивалентен пустой строке (документированный контракт функции).
    none_snapshot = {**_BUILD_ID_BASE, "snapshot_date": None}
    empty_snapshot = {**_BUILD_ID_BASE, "snapshot_date": ""}
    assert _derive_index_build_id(**none_snapshot) == _derive_index_build_id(  # type: ignore[arg-type]
        **empty_snapshot
    )

    # Отпечаток каталога — точный вход: порядок токенов значим. Каноническую сортировку
    # выполняет вызывающий (``corpus._params_token``), поэтому здесь проверяется
    # чувствительность, а не порядко-независимость.
    assert _derive_index_build_id(
        **(_BUILD_ID_BASE | {"params_token": "a,b"})
    ) != _derive_index_build_id(**(_BUILD_ID_BASE | {"params_token": "b,a"}))
