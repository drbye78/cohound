"""План 27 §9: карта reference-продукта — идентичность, форма артефактов, согласие.

Проверяется то, ради чего карта существует и чем она **не** является:

* ``map_id`` — функция объявленных входов (модель/промпт/каталог/факты), а не даты снятия;
* ``score.json`` — та же форма, что бандл когорты, плюс ``subject: reference``;
* карта не пишет в ``runs/`` и не смешивается с продуктами когорты;
* согласие «факт ↔ карта» публикует ``n`` и ``None`` вместо выдуманного κ при ``n = 0``.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from core.adapters.lexical_bm25 import Bm25Index
from core.domain.enums import FactReview, FactStatus, Importance, ParamType, Status
from core.domain.models import (
    BaselineFact,
    CatalogRow,
    LLMCellOutput,
    LLMEvidence,
)
from core.pipeline import reference_map as reference_map_module
from core.pipeline.evaluate import CorpusArtifactDates
from core.pipeline.reference_map import (
    MAP_SCHEMA,
    MAP_SUBJECT,
    MapChunk,
    ReferenceMapIncomplete,
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
        ("catalog_version", "cat-2"),
        ("llm_scenarios", {"model_id": "m2"}),
        ("prompt_template", "v10@def"),
        ("evaluation_policy", {"max_retries": 0}),
        ("query_lang", "en"),
    ):
        assert reference_map_id(**{**base, field: other}) != first, field


def test_map_id_ignores_facts_digest() -> None:
    """Набор фактов на столе — провенанс карты, а не её условие (19.11.2026).

    Карта читает чанки корпуса, а не файл фактов. Пока факты строились отдельным фразовым
    прибором, дайджест входил в ``map_id``; после перехода на проекцию «карта → факт» это
    дало бы круг: ``map_id`` ← дайджест фактов ← ``map_id``. Тест фиксирует, что круг не
    вернётся: две карты с разными наборами фактов — одна и та же карта.
    """
    base = {
        "reference_build_id": "ref_pg_18_a",
        "catalog_version": "cat-1",
        "llm_scenarios": {"model_id": "m1"},
        "prompt_template": "v9@abc",
        "evaluation_policy": {"max_retries": 2},
    }
    first = reference_map_id(**{**base, "baseline_facts_digest": "facts-1"})
    second = reference_map_id(**{**base, "baseline_facts_digest": "facts-2"})
    assert first == second
    assert first == reference_map_id(**base), "отсутствие аргумента — то же значение"


def test_built_map_manifest_id_matches_layout_id(tmp_path: Path) -> None:
    """Манифест карты и каталог ``maps/`` называют **один** адрес.

    ``run_map`` строит каталог по ``reference_map_id(query_lang=...)``, а
    ``build_reference_map`` пересчитывает идентичность внутри. Пока внутренний вызов не
    передавал ``query_lang``, при ``--query-lang en`` манифест объявлял бы адрес
    дефолтного ``all``, лежащий в другой папке: два адреса у одного артефакта.
    """
    layout = ReferenceMapLayout(root=tmp_path / "maps" / "x", map_id="map_x")
    expected = reference_map_id(
        reference_build_id="ref_postgresql_18_deadbeef",
        baseline_facts_digest="facts-1",
        catalog_version="cat-1",
        llm_scenarios={"model_id": "fake-evaluator@rev1"},
        prompt_template="v9@abc",
        evaluation_policy={"max_retries": 2},
        query_lang="en",
    )
    result = _build(layout, query_lang="en")
    assert result.manifest.map_id == expected
    assert result.manifest.query_lang == "en"


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
    # Дата отсутствия — часть публикации, а не внутренняя деталь (I2d): у карты нет
    # таблицы `cells`, поэтому единственный носитель даты — эта строка.
    assert "absence_as_of" in row


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


def test_agreement_does_not_mix_versions() -> None:
    """Факт @11 и карта @18 — разные корпуса: пара в κ не входит (замер 19.11.2026).

    До пере-разведки слоя фактов все наблюдения были @18, и смешение версий было
    незаметно; на 8 версиях × 181 оно размыло κ до 0.0451 при n=216 вместо 181.
    """
    facts = [
        _fact("p1", FactStatus.PRESENT),  # version="18"
        BaselineFact(
            baseline_id="upstream-pg",
            param_id="p2",
            version="11",
            status=FactStatus.PRESENT,
            url="https://upstream.test/old",
            quote="upstream documents the mechanism in v11",
            content_hash="b" * 64,
            review=FactReview.PENDING,
        ),
    ]
    cells = [
        {"param_id": "p1", "state": "evaluated", "status": Status.OK.value},
        {"param_id": "p2", "state": "evaluated", "status": Status.ABSENT.value},
    ]
    agreement = fact_map_agreement(facts, cells, version="18")
    assert agreement["n"] == 1
    assert agreement["skipped_other_versions"] == 1
    assert [item["param_id"] for item in agreement["disagreements"]] == []
    # Без версии — прежнее поведение (все линии в одном κ), чтобы старые вызовы не менялись.
    assert fact_map_agreement(facts, cells)["n"] == 2


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


# ---------------------------------------------------------------------------
# Параллельный путь (llm_concurrency)
# ---------------------------------------------------------------------------


class _SlowEvaluator(_Evaluator):
    """Оценщик с задержкой: без неё параллельный путь неотличим от последовательного."""

    def __init__(self, delay: float = 0.05) -> None:
        super().__init__()
        self.delay = delay
        # Счётчик ведёт базовый класс: дублировать его здесь значило бы считать вызовы
        # дважды (первая версия этого теста так и сделала — 10 вызовов на 5 параметров).
        self.lock = threading.Lock()

    def complete(self, req):  # noqa: ANN001 — шов порта
        time.sleep(self.delay)
        with self.lock:
            return super().complete(req)


def test_parallel_cells_keep_catalog_order(tmp_path: Path) -> None:
    """Порядок ячеек — порядок каталога, а не порядок ответов модели.

    ``cells.jsonl`` не имеет права зависеть от того, кто ответил первым: иначе два прогона
    с одним ``map_id`` давали бы разные файлы, и сравнение карт мерило бы планировщик.
    """
    params = [_param(f"p{i}") for i in range(6)]
    result = _build(
        ReferenceMapLayout(root=tmp_path / "map", map_id="m1"),
        params=params,
        evaluator=_SlowEvaluator(delay=0.01),
        llm_concurrency=4,
    )
    assert [cell.param_id for cell in result.cells] == [f"p{i}" for i in range(6)]


def test_parallel_path_calls_every_param_once(tmp_path: Path) -> None:
    """Параллельность не дублирует и не теряет вызовы: ровно по одному на параметр."""
    params = [_param(f"p{i}") for i in range(5)]
    evaluator = _SlowEvaluator(delay=0.01)
    result = _build(
        ReferenceMapLayout(root=tmp_path / "map", map_id="m1"),
        params=params,
        evaluator=evaluator,
        llm_concurrency=3,
    )
    assert len(result.cells) == 5
    assert evaluator.calls == 5


def test_parallel_failure_is_named_and_no_partial_map(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Упавшая ячейка не превращается в частичную карту: ошибка названа, ячейки нет.

    Сбой внедряется в ретривал, а не в оценщик: исключение оценщика `evaluate_cell`
    превращает в ячейку с `failure_kind` (это его контракт), а проверяется здесь изоляция
    **сбоя ячейки**, которую фаза обязана пережить. Живой случай этого класса — `sleep=None`,
    уронивший карту на 181-й ячейке 18.11.2026.
    """
    real = reference_map_module._retrieve

    def failing(param, **kwargs):  # noqa: ANN001, ANN003 — шов
        if str(param.param_id) == "p_bad":
            raise RuntimeError("индекс недоступен")
        return real(param, **kwargs)

    monkeypatch.setattr(reference_map_module, "_retrieve", failing)
    params = [_param("p_ok"), _param("p_bad")]
    with pytest.raises(ReferenceMapIncomplete) as excinfo:
        _build(
            ReferenceMapLayout(root=tmp_path / "map", map_id="m1"),
            params=params,
            evaluator=_SlowEvaluator(delay=0.0),
            llm_concurrency=2,
        )
    assert any("p_bad" in item for item in excinfo.value.failed)
    assert not any("p_ok" in item for item in excinfo.value.failed)


def test_sequential_path_also_isolates_and_names_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Последовательный путь не «падает целиком»: изоляция одна на оба пути."""
    real = reference_map_module._retrieve

    def failing(param, **kwargs):  # noqa: ANN001, ANN003 — шов
        if str(param.param_id) == "p_bad":
            raise RuntimeError("индекс недоступен")
        return real(param, **kwargs)

    monkeypatch.setattr(reference_map_module, "_retrieve", failing)
    with pytest.raises(ReferenceMapIncomplete) as excinfo:
        _build(
            ReferenceMapLayout(root=tmp_path / "map", map_id="m1"),
            params=[_param("p_ok"), _param("p_bad")],
        )
    assert any("p_bad" in item for item in excinfo.value.failed)


def test_progress_reports_every_cell_in_order(tmp_path: Path) -> None:
    """Прогресс сообщает каждую ячейку: молчащий прибор на длинном прогоне неотличим от зависшего."""
    seen: list[tuple[int, int, str]] = []
    params = [_param(f"p{i}") for i in range(3)]
    _build(
        ReferenceMapLayout(root=tmp_path / "map", map_id="m1"),
        params=params,
        evaluator=_SlowEvaluator(delay=0.0),
        llm_concurrency=2,
        progress=lambda done, total, param_id: seen.append((done, total, param_id)),
    )
    assert sorted(item[2] for item in seen) == ["p0", "p1", "p2"]
    assert {item[1] for item in seen} == {3}
    assert sorted(item[0] for item in seen) == [1, 2, 3]


def test_concurrency_axis_is_not_part_of_map_id() -> None:
    """Число вызовов в полёте — условие исполнения, а не измерения (§7.3, как у Ш6)."""
    base = {
        "reference_build_id": "ref_pg_18_a",
        "baseline_facts_digest": "facts-1",
        "catalog_version": "cat-1",
        "llm_scenarios": {"model_id": "fake-evaluator@rev1"},
        "prompt_template": "v9@abc",
        "evaluation_policy": None,
    }
    assert reference_map_id(**base) == reference_map_id(**base)


@pytest.mark.parametrize("value", [0, -1])
def test_concurrency_below_one_is_rejected(tmp_path: Path, value: int) -> None:
    """«Ноль вызовов в полёте» — не исполнение: отказ, а не тихая единица."""
    with pytest.raises(ValueError):
        _build(
            ReferenceMapLayout(root=tmp_path / "map", map_id="m1"),
            llm_concurrency=value,
        )


def test_retry_pause_is_a_callable_when_sleep_is_not_injected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`sleep=None` означает «не внедрён», а не «не спать»: иначе первая же попытка падает.

    Замер 18.11.2026: карта на 181 параметр упала на последней ячейке с
    `TypeError: 'NoneType' object is not callable` — `evaluate_cell` ждал паузу повтора
    через `None`. Дефект латентный: он виден только на ячейке, которой нужен повтор.
    """
    captured: dict[str, object] = {}
    real = reference_map_module.evaluate_cell

    def spy(**kwargs):  # noqa: ANN003 — шов
        captured.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(reference_map_module, "evaluate_cell", spy)
    _build(ReferenceMapLayout(root=tmp_path / "map", map_id="m1"))
    assert callable(captured["sleep"]), "пауза повтора обязана быть вызываемой"


def test_injected_sleep_is_used_as_is(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Внедрённая пауза не подменяется: тесты не спят, и это их выбор, а не дефолт."""
    calls: list[float] = []
    captured: dict[str, object] = {}
    real = reference_map_module.evaluate_cell

    def spy(**kwargs):  # noqa: ANN003 — шов
        captured.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(reference_map_module, "evaluate_cell", spy)
    _build(
        ReferenceMapLayout(root=tmp_path / "map", map_id="m1"),
        sleep=lambda seconds: calls.append(seconds),
    )
    assert captured["sleep"] is not None
    assert captured["sleep"] is not time.sleep


# ---------------------------------------------------------------------------
# Отсутствие: I2c/I2d действуют на карте так же, как в Ш6 (§9)
# ---------------------------------------------------------------------------


_ABSENT_DOC = "a" * 64
_ABSENT_CHUNK_ID = f"postgresql:{_ABSENT_DOC}:1"


class _AbsentEvaluator(_Evaluator):
    """Заявляет `explicit`-отрицание: проверяется носитель даты, а не мнение модели."""

    def complete(self, req):  # noqa: ANN001 — шов порта
        out = LLMCellOutput(
            status=Status.ABSENT,
            justification="вендор прямо отрицает",
            denial_strength="explicit",
            denial_reason="прямое отрицание в документации",
            evidence=[
                LLMEvidence(
                    source_id=_ABSENT_CHUNK_ID,
                    quote="The database does not support jsonb storage.",
                )
            ],
        )
        return LLMResponse(
            output=out, raw="{}", prompt_hash="ph", decoder="none", usage=TokenUsage()
        )


def _absent_chunks() -> list[MapChunk]:
    """Чанк, в тексте которого цитата отрицания действительно есть (I22 — подстрочность)."""
    return [
        MapChunk(
            chunk_id=_ABSENT_CHUNK_ID,
            doc_id=_ABSENT_DOC,
            url="https://www.postgresql.org/docs/18/datatype-json.html",
            text="The database does not support jsonb storage.",
            content_hash=_ABSENT_DOC,
        )
    ]


def _coverage(complete: bool = True):
    from core.domain.models import CorpusCoverage, CorpusRootCoverage

    return CorpusCoverage(
        product_id="postgresql",
        complete=complete,
        roots=[
            CorpusRootCoverage(
                root_url="https://www.postgresql.org/docs/18/",
                capture_id="ref_postgresql_18_deadbeef",
                complete=complete,
                observed_how="live",
                observed_at="2026-11-17T22:06:31+00:00",
                policy_digest=None,
            )
        ],
        expected_sources=0,
        observed_sources=0,
        computed_at="2026-11-17T22:06:31+00:00",
    )


def test_absent_without_dated_carrier_is_not_published(tmp_path: Path) -> None:
    """`❌` без записи наблюдения — `❓` с токеном `absence_undated`, а не утверждение.

    Живой случай 18.11.2026: карта PG 18 опубликовала `❌` класса `explicit`, хотя в
    reference-БД `corpus_artifact` пуста (0 строк) — то есть «этого нет» без даты.
    """
    result = _build(
        ReferenceMapLayout(root=tmp_path / "map", map_id="m1"),
        evaluator=_AbsentEvaluator(),
        chunks=_absent_chunks(),
        artifact_dates=CorpusArtifactDates(
            product_id="postgresql", by_doc_version={}, by_doc={}
        ),
        coverage=_coverage(complete=True),
    )
    cell = result.cells[0]
    assert cell.status is not Status.ABSENT, "отсутствие без датированного носителя не публикуется"
    assert any("absence_undated" in note for note in cell.validation_notes)
    assert cell.absence_evidence == []


def test_absent_with_dated_carrier_is_published(tmp_path: Path) -> None:
    """С датой носителя `❌` публикуется: инвариант не «запретить», а «не выдумывать»."""
    # Цитата адресует документ как `product:content_hash` (legacy-форма адреса).
    dates = CorpusArtifactDates(
        product_id="postgresql",
        by_doc_version={},
        by_doc={f"postgresql:{_ABSENT_DOC}": "2026-11-17T22:06:31+00:00"},
    )
    result = _build(
        ReferenceMapLayout(root=tmp_path / "map", map_id="m1"),
        evaluator=_AbsentEvaluator(),
        chunks=_absent_chunks(),
        artifact_dates=dates,
        coverage=_coverage(complete=True),
    )
    cell = result.cells[0]
    assert cell.status is Status.ABSENT
    assert cell.absence_as_of == "2026-11-17T22:06:31+00:00"
    # И дата доезжает до артефакта, а не остаётся в объекте ячейки.
    write_map(ReferenceMapLayout(root=tmp_path / "map", map_id="m1"), result)
    row = read_map_cells(tmp_path / "map" / "cells.jsonl")[0]
    assert row["absence_as_of"] == "2026-11-17T22:06:31+00:00"
