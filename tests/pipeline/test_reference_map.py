"""План 27 §9: карта reference-продукта — идентичность, форма артефактов, согласие.

Проверяется то, ради чего карта существует и чем она **не** является:

* ``map_id`` — функция объявленных входов (модель/промпт/каталог/факты), а не даты снятия;
* ``score.json`` — та же форма, что бандл когорты, плюс ``subject: reference``;
* карта не пишет в ``runs/`` и не смешивается с продуктами когорты;
* согласие «факт ↔ карта» публикует ``n`` и ``None`` вместо выдуманного κ при ``n = 0``.
"""

from __future__ import annotations

import json
from pathlib import Path

from core.adapters.lexical_bm25 import Bm25Index
from core.domain.enums import FactReview, FactStatus, Importance, ParamType, Status
from core.domain.models import (
    BaselineFact,
    CatalogRow,
    LLMCellOutput,
    LLMEvidence,
)
from core.pipeline.reference_map import (
    MAP_SCHEMA,
    MAP_SUBJECT,
    MapChunk,
    ReferenceMapLayout,
    build_reference_map,
    cell_row,
    fact_map_agreement,
    read_map_cells,
    read_map_manifest,
    reference_map_id,
    write_map,
)
from core.ports.llm import LLMResponse, TokenUsage


class _Evaluator:
    """Фейковый оценщик: `present` с цитатой на «jsonb», иначе `❓`.

    Модель не вызывается сетью — проверяется проводка (ретривал → ячейка → бандл), а не
    качество ответа.
    """

    model_id = "fake-evaluator@rev1"

    def __init__(self) -> None:
        self.calls = 0

    def complete(self, req):  # noqa: ANN001 — шов порта
        self.calls += 1
        if "jsonb" in req.prompt.lower():
            out = LLMCellOutput(
                status=Status.OK,
                justification="документация описывает jsonb",
                evidence=[LLMEvidence(source_id="c1", quote="supports jsonb storage")],
            )
        else:
            out = LLMCellOutput(
                status=Status.UNKNOWN,
                justification="не установлено",
                denial_reason="нет дословного утверждения",
            )
        return LLMResponse(
            output=out, raw="{}", prompt_hash="ph", decoder="none", usage=TokenUsage()
        )


def _param(param_id: str = "p_jsonb", rule: str = "supports jsonb storage") -> CatalogRow:
    return CatalogRow(
        param_id=param_id,
        title="JSONB storage",
        module="m",
        group="g",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule=rule,
        concept_terms={"en": ["jsonb"]},
    )


def _chunks() -> list[MapChunk]:
    return [
        MapChunk(
            chunk_id="c1",
            doc_id="d1",
            url="https://www.postgresql.org/docs/18/datatype-json.html",
            text="The database supports jsonb storage with expression indexing.",
            content_hash="a" * 64,
        )
    ]


def _build(layout: ReferenceMapLayout, **kwargs):
    base = {
        "layout": layout,
        "product_id": "postgresql",
        "version": "18",
        "reference_build_id": "ref_postgresql_18_deadbeef",
        "chunks": _chunks(),
        "params": [_param()],
        "evaluator": _Evaluator(),
        "lexical_index": Bm25Index(),
        "module_weights": {"m": 100.0},
        "catalog_version": "cat-1",
        "baseline_facts_digest": "facts-1",
        "llm_scenarios": {"model_id": "fake-evaluator@rev1"},
        "prompt_template": "v9@abc",
        "evaluation_policy": {"max_retries": 2},
        "cutoff_k": 5,
    }
    base.update(kwargs)
    return build_reference_map(**base)


# ---------------------------------------------------------------------------
# Идентичность карты (§7.3)
# ---------------------------------------------------------------------------


def test_map_id_is_a_function_of_declared_inputs() -> None:
    base = {
        "reference_build_id": "ref_pg_18_a",
        "baseline_facts_digest": "facts-1",
        "catalog_version": "cat-1",
        "llm_scenarios": {"model_id": "m1"},
        "prompt_template": "v9@abc",
        "evaluation_policy": {"max_retries": 2},
    }
    first = reference_map_id(**base)
    assert first == reference_map_id(**base)
    assert first.startswith("map_")
    assert len(first) == len("map_") + 16
    # Каждый объявленный вход двигает идентичность: карта без модели — не измерение.
    for field, other in (
        ("reference_build_id", "ref_pg_18_b"),
        ("baseline_facts_digest", "facts-2"),
        ("catalog_version", "cat-2"),
        ("llm_scenarios", {"model_id": "m2"}),
        ("prompt_template", "v10@def"),
        ("evaluation_policy", {"max_retries": 0}),
    ):
        assert reference_map_id(**{**base, field: other}) != first, field


def test_map_id_ignores_observation_time_and_order(tmp_path: Path) -> None:
    """Дата снятия и порядок словаря — провенанс, а не условие измерения."""
    layout = ReferenceMapLayout(root=tmp_path / "maps" / "x", map_id="map_x")
    first = _build(layout, observed_at="2026-11-17T00:00:00+00:00")
    second = _build(
        layout,
        observed_at="2026-12-01T00:00:00+00:00",
        llm_scenarios={"model_id": "fake-evaluator@rev1"},
        evaluation_policy={"max_retries": 2},
    )
    assert first.manifest.map_id == second.manifest.map_id
    # Порядок ключей политики не влияет: канонизация сортирует.
    reordered = _build(
        layout, evaluation_policy={"max_retries": 2}
    )
    assert reordered.manifest.map_id == first.manifest.map_id


# ---------------------------------------------------------------------------
# Артефакты (§9.4)
# ---------------------------------------------------------------------------


def test_score_json_has_the_cohort_bundle_shape_plus_subject(tmp_path: Path) -> None:
    layout = ReferenceMapLayout(root=tmp_path / "maps" / "x", map_id="map_x")
    result = _build(layout)
    paths = write_map(layout, result)
    assert set(paths) == {"manifest", "cells", "score"}
    score = json.loads(layout.score_path.read_text(encoding="utf-8"))
    # Та же форма, что ScoreBundle: читатель не учит второй формат.
    for key in (
        "capability",
        "coverage",
        "failure_rate",
        "modules",
        "comparability",
        "sensitivity",
        "requested_ranking",
        "computed_ranking",
    ):
        assert key in score, key
    assert score["subject"] == MAP_SUBJECT
    assert score["map_id"] == result.manifest.map_id
    assert score["reference_build_id"] == "ref_postgresql_18_deadbeef"


def test_manifest_round_trip_and_counts(tmp_path: Path) -> None:
    layout = ReferenceMapLayout(root=tmp_path / "maps" / "x", map_id="map_x")
    result = _build(layout)
    write_map(layout, result)
    manifest = read_map_manifest(layout.manifest_path)
    assert manifest is not None
    assert manifest.schema == MAP_SCHEMA
    assert manifest.map_id == result.manifest.map_id
    assert manifest.product_id == "postgresql"
    assert manifest.version == "18"
    assert manifest.counts.get("present") == 1
    assert read_map_manifest(tmp_path / "missing.json") is None


def test_cells_jsonl_carries_verdict_evidence_and_notes(tmp_path: Path) -> None:
    layout = ReferenceMapLayout(root=tmp_path / "maps" / "x", map_id="map_x")
    result = _build(layout)
    write_map(layout, result)
    rows = read_map_cells(layout.cells_path)
    assert len(rows) == 1
    row = rows[0]
    assert row["param_id"] == "p_jsonb"
    assert row["state"] == "evaluated"
    assert row["status"] == Status.OK.value
    assert [item["source_id"] for item in row["evidence"]] == ["c1"]
    assert "validation_notes" in row


def test_map_never_writes_into_runs(tmp_path: Path, monkeypatch) -> None:
    """Карта — не прогон: артефакт лежит в reference/…/maps/, а не в runs/ (§9.2)."""
    monkeypatch.chdir(tmp_path)
    layout = ReferenceMapLayout(
        root=tmp_path / "reference" / "pkg" / "postgresql" / "18" / "maps" / "map_x",
        map_id="map_x",
    )
    write_map(layout, _build(layout))
    assert not (tmp_path / "runs").exists()
    assert layout.score_path.is_file()


def test_dry_run_layout_is_not_created(tmp_path: Path) -> None:
    """Сборка без записи не создаёт каталог: `--dry-run` не оставляет следов."""
    layout = ReferenceMapLayout(root=tmp_path / "maps" / "x", map_id="map_x")
    _build(layout)
    assert not layout.root.exists()


def test_unknown_verdict_is_not_counted_as_absence(tmp_path: Path) -> None:
    """``❓`` карты — не «нет»: в counts это ``unknown``, а не ``absent`` (I12)."""
    layout = ReferenceMapLayout(root=tmp_path / "maps" / "x", map_id="map_x")
    other = CatalogRow(
        param_id="p_other",
        title="Streaming replication slots",
        module="m",
        group="g",
        importance=Importance.CRITICAL,
        param_type=ParamType.FACT,
        rule="physical replication slot streaming",
    )
    result = _build(layout, params=[other])
    assert result.counts == {"unknown": 1}
    assert "absent" not in result.counts


# ---------------------------------------------------------------------------
# Согласие «факт ↔ карта» (§9.5)
# ---------------------------------------------------------------------------


def _fact(param_id: str, status: FactStatus) -> BaselineFact:
    present = status is FactStatus.PRESENT
    return BaselineFact(
        baseline_id="upstream-pg",
        param_id=param_id,
        version="18",
        status=status,
        url="https://upstream.test/doc" if present else None,
        quote="upstream documents the mechanism" if present else None,
        content_hash="a" * 64 if present else None,
        review=FactReview.ACCEPTED,
        reviewed_by="roger",
    )


def test_agreement_counts_only_pairs_with_a_verdict_on_both_sides() -> None:
    facts = [
        _fact("same", FactStatus.PRESENT),
        _fact("diff", FactStatus.PRESENT),
        _fact("unknown_fact", FactStatus.UNCLEAR),
    ]
    cells = [
        {"param_id": "same", "state": "evaluated", "status": Status.OK.value},
        {"param_id": "diff", "state": "evaluated", "status": Status.ABSENT.value},
        {"param_id": "unknown_fact", "state": "evaluated", "status": Status.OK.value},
        {"param_id": "unknown_map", "state": "evaluated", "status": Status.UNKNOWN.value},
    ]
    agreement = fact_map_agreement(facts, cells)
    # Пара «факт present × карта ❓» — не расхождение о наличии: неизвестность не отрицание.
    assert agreement["n"] == 2
    assert agreement["disagreements"] == [
        {"param_id": "diff", "fact": "present", "map": "absent"}
    ]
    assert agreement["fact_present_map_absent"] == 1
    assert agreement["fact_absent_map_present"] == 0
    assert agreement["kappa"] == 0.0


def test_agreement_with_no_pairs_publishes_none_not_zero() -> None:
    """``n = 0`` — не выдуманное согласие: κ ``None`` с названной причиной (A3)."""
    agreement = fact_map_agreement([], [])
    assert agreement["n"] == 0
    assert agreement["kappa"] is None
    assert agreement["undefined_reason"]


def test_agreement_filters_by_baseline_line() -> None:
    facts = [_fact("p1", FactStatus.PRESENT)]
    cells = [{"param_id": "p1", "state": "evaluated", "status": Status.OK.value}]
    assert fact_map_agreement(facts, cells, baseline_id="upstream-pg")["n"] == 1
    assert fact_map_agreement(facts, cells, baseline_id="other")["n"] == 0


def test_agreement_reads_written_cells(tmp_path: Path) -> None:
    """Согласие считается по тому, что записано: писатель и читатель — один носитель."""
    layout = ReferenceMapLayout(root=tmp_path / "maps" / "x", map_id="map_x")
    result = _build(layout)
    write_map(layout, result)
    rows = read_map_cells(layout.cells_path)
    agreement = fact_map_agreement(
        [_fact("p_jsonb", FactStatus.PRESENT)], rows, baseline_id="upstream-pg"
    )
    assert agreement["n"] == 1
    assert agreement["disagreements"] == []
    assert cell_row(result.cells[0])["param_id"] == "p_jsonb"
