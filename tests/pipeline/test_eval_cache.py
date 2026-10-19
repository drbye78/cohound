"""Тесты межпрогонного кэша вердиктов оценщика (``core/pipeline/eval_cache.py``).

Проверяют главное свойство Ф3: повторный прогон по тем же доказательствам не делает ни
одного вызова оценщика, но проходит **весь** существующий эшелон (валидацию, гейт
покрытия Ф0, наложение прецедента) заново. Отдельно фиксируется ловушка ``prompt_hash``:
он не покрывает продукт/заголовок/сателлиты/extra-instructions/текст чанков, поэтому
ключ строится по отрендеренному промпту и конфигурации модели.

Оценщик — скриптованный in-memory (без сети, I18); чанки/каталог — фикстура
``test_evaluate``.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
from pathlib import Path

import pytest

from core.domain import (
    EVAL_SCHEMA_VERSION,
    Cell,
    CellState,
    Focus,
    LLMCellOutput,
    LLMEvidence,
    RunConfig,
    RunMode,
    Satellite,
    SatelliteState,
    SourceKind,
    Status,
    check_unknown_exhaustion,
)
from core.domain.anchors import ResolvedSource
from core.pipeline.eval_cache import (
    ENVELOPE_FORMAT,
    EVAL_CACHE_FORMAT,
    EvalVerdictCache,
    build_eval_key,
)
from core.pipeline.evaluate import evaluate_cell
from core.pipeline.prompt import compile_prompt
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash
from tests.pipeline.test_evaluate import (
    _ScriptedEvaluator,
    _absent_output,
    _bad_output,
    _chunks,
    _coverage,
    _data,
    _ok_output,
    _param,
    _RecordingStore,
)

# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _echo(**overrides: object) -> dict:
    """Полный ``echo`` ключа (поля совпадают с ``evaluate_cell``)."""
    echo: dict = {
        "format": EVAL_CACHE_FORMAT,
        "schema": EVAL_SCHEMA_VERSION,
        "template": "evaluator_v1",
        "prompt_sha256": content_hash("rendered prompt"),
        "prompt_hash": "ph",
        "model_id": "fake-model",
        "model_version": "1.0",
        "seed": 0,
        "temperature": 0.0,
        "decoder": "none",
        "endpoint_kind": "",
    }
    echo.update(overrides)
    return echo


def _unknown_output(justification: str = "нет данных") -> LLMCellOutput:
    """Валидный вердикт ``❓`` (без evidence — I1 его не требует)."""
    return LLMCellOutput(
        status=Status.UNKNOWN, justification=justification, evidence=[]
    )


def _entry(root: Path, key: str) -> Path:
    """Ожидаемый путь записи (``{root}/{key[:2]}/{key}.json``)."""
    return root / key[:2] / f"{key}.json"


def _write_envelope(path: Path, **overrides: object) -> None:
    """Пишет валидный по форме конверт с переопределениями (негативные тесты)."""
    echo = _echo()
    envelope: dict[str, object] = {
        "format": ENVELOPE_FORMAT,
        "key": build_eval_key(echo),
        "echo": echo,
        "verdict_status": Status.UNKNOWN.value,
        "output": _unknown_output().model_dump_json(),
    }
    envelope.update(overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(envelope), encoding="utf-8")


def _evaluate(data: dict, evaluator, cache, **kwargs):  # noqa: ANN001, ANN003
    """Вызов ``evaluate_cell`` с фикстурой и кэшем; остальное — переопределения."""
    param = kwargs.pop("param", _param(data))
    product_id = kwargs.pop("product_id", data["product_id"])
    run_id = kwargs.pop("run_id", data["run_id"])
    chunks = kwargs.pop("chunks", _chunks(data))
    return evaluate_cell(
        param=param,
        product_id=product_id,
        run_id=run_id,
        evaluator=evaluator,
        chunks=chunks,
        eval_cache=cache,
        **kwargs,
    )


def _run_config() -> RunConfig:
    """Минимальная валидная конфигурация прогона для ``SqliteStore.create_run``."""
    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1"],
        package="pkg-1",
        focus=Focus(),
        languages=["en"],
        snapshot_date=dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
    )


# ---------------------------------------------------------------------------
# Провенанс схемы
# ---------------------------------------------------------------------------


def test_eval_schema_version_is_pinned() -> None:
    """Версия схемы — часть ключа кэша; её случайное изменение обязано быть заметным.

    Константа существует потому, что схема ``LLMCellOutput`` до Ф3 не версионировалась:
    кэш иначе не смог бы отличить выход, разобранный старой схемой, от нового.
    """
    assert EVAL_SCHEMA_VERSION == 1


# ---------------------------------------------------------------------------
# Round-trip и промахи (не ошибки)
# ---------------------------------------------------------------------------


def test_round_trip_returns_equal_output(tmp_path: Path) -> None:
    cache = EvalVerdictCache(tmp_path / "cache")
    echo = _echo()
    key = build_eval_key(echo)
    output = _unknown_output("вердикт")

    cache.put(key, output, echo=echo)

    assert cache.get(key, echo=echo) == output
    assert cache.stats()["writes"] == 1
    assert cache.stats()["hits"] == 1


def test_key_is_deterministic_and_order_independent() -> None:
    first = _echo(model_id="m", seed=1)
    second = dict(reversed(list(first.items())))
    assert build_eval_key(first) == build_eval_key(second)
    assert len(build_eval_key(first)) == 32


def test_echo_mismatch_is_miss_and_counted(tmp_path: Path) -> None:
    cache = EvalVerdictCache(tmp_path / "cache")
    echo = _echo()
    key = build_eval_key(echo)
    cache.put(key, _unknown_output(), echo=echo)

    assert cache.get(key, echo=_echo(model_version="2.0")) is None
    assert cache.stats()["echo_mismatch"] == 1
    assert cache.stats()["misses"] == 1
    assert cache.stats()["corrupt"] == 0


def test_corrupt_json_is_miss_and_counted(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    key = build_eval_key(_echo())
    path = _entry(root, key)
    path.parent.mkdir(parents=True)
    path.write_text("{not valid json", encoding="utf-8")

    cache = EvalVerdictCache(root)
    assert cache.get(key, echo=_echo()) is None
    assert cache.stats()["corrupt"] == 1
    assert cache.stats()["misses"] == 1


def test_wrong_envelope_format_is_miss(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    key = build_eval_key(_echo())
    _write_envelope(_entry(root, key), format=ENVELOPE_FORMAT + 1)

    cache = EvalVerdictCache(root)
    assert cache.get(key, echo=_echo()) is None
    assert cache.stats()["corrupt"] == 1


def test_stale_envelope_with_other_key_is_miss(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    key = build_eval_key(_echo())
    other_key = build_eval_key(_echo(model_id="other"))
    _write_envelope(_entry(root, key), key=other_key)

    cache = EvalVerdictCache(root)
    assert cache.get(key, echo=_echo()) is None
    assert cache.stats()["misses"] == 1


def test_invalid_serialized_output_is_miss(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    key = build_eval_key(_echo())
    _write_envelope(_entry(root, key), output="{broken")

    cache = EvalVerdictCache(root)
    assert cache.get(key, echo=_echo()) is None
    assert cache.stats()["corrupt"] == 1


def test_verdict_status_mismatch_is_miss(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    key = build_eval_key(_echo())
    _write_envelope(_entry(root, key), verdict_status=Status.OK.value)

    cache = EvalVerdictCache(root)
    assert cache.get(key, echo=_echo()) is None
    assert cache.stats()["corrupt"] == 1


def test_non_cacheable_status_is_rejected(tmp_path: Path) -> None:
    """``N/A`` (I7) не является вердиктом и не пишется; обход валидатора не помогает."""
    root = tmp_path / "cache"
    cache = EvalVerdictCache(root)
    echo = _echo()
    key = build_eval_key(echo)
    output = LLMCellOutput.model_construct(
        status=Status.NOT_APPLICABLE, justification="n/a", evidence=[]
    )

    cache.put(key, output, echo=echo)

    assert cache.stats()["rejected"] == 1
    assert cache.stats()["writes"] == 0
    assert list(root.rglob("*.json")) == []


def test_cell_object_is_rejected(tmp_path: Path) -> None:
    """Даже ``Cell`` со статусом ``❓`` не кэшируется: кэш адресует только сырой выход."""
    root = tmp_path / "cache"
    cache = EvalVerdictCache(root)
    echo = _echo()
    key = build_eval_key(echo)
    cell = Cell(
        run_id="r1",
        product_id="p1",
        param_id="param",
        state=CellState.EVALUATED,
        status=Status.UNKNOWN,
    )

    cache.put(key, cell, echo=echo)  # type: ignore[arg-type]

    assert cache.stats()["rejected"] == 1
    assert cache.stats()["writes"] == 0
    assert list(root.rglob("*.json")) == []


def test_path_traversal_keys_rejected(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    cache = EvalVerdictCache(root)
    echo = _echo()
    output = _unknown_output()

    for bad in ("../../escape", "ab/cd", "..", "", "zz", "ab\\cd"):
        assert cache.get(bad, echo=echo) is None
        cache.put(bad, output, echo=echo)

    assert list(root.rglob("*")) == []
    assert not (tmp_path / "escape.json").exists()
    assert cache.stats()["invalid_key"] == 12  # 6 get + 6 put


# ---------------------------------------------------------------------------
# Атомарность и бюджет
# ---------------------------------------------------------------------------


def test_tmp_leftover_is_not_served(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    key = build_eval_key(_echo())
    shard = root / key[:2]
    shard.mkdir(parents=True)
    leftover = shard / f".{key}.dead.tmp"
    leftover.write_text("half written", encoding="utf-8")

    cache = EvalVerdictCache(root)
    assert cache.get(key, echo=_echo()) is None  # .tmp не обслуживается

    cache.put(key, _unknown_output(), echo=_echo())
    assert cache.get(key, echo=_echo()) is not None
    assert leftover.exists()  # чужой temp не трогаем


def test_envelope_bytes_are_deterministic(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    cache = EvalVerdictCache(root)
    echo = _echo()
    key = build_eval_key(echo)
    output = _unknown_output()

    cache.put(key, output, echo=echo)
    first = _entry(root, key).read_bytes()
    cache.put(key, output, echo=echo)

    assert _entry(root, key).read_bytes() == first


def test_eviction_is_oldest_first_and_stays_under_budget(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    probe = EvalVerdictCache(root)
    echo_a = _echo(prompt_sha256=content_hash("a"))
    key_a = build_eval_key(echo_a)
    probe.put(key_a, _unknown_output("a"), echo=echo_a)
    size = _entry(root, key_a).stat().st_size

    cache = EvalVerdictCache(root, max_bytes=2 * size)
    echo_b = _echo(prompt_sha256=content_hash("b"))
    key_b = build_eval_key(echo_b)
    cache.put(key_b, _unknown_output("b"), echo=echo_b)
    os.utime(_entry(root, key_a), ns=(1_000_000_000, 1_000_000_000))
    os.utime(_entry(root, key_b), ns=(2_000_000_000, 2_000_000_000))

    echo_c = _echo(prompt_sha256=content_hash("c"))
    key_c = build_eval_key(echo_c)
    cache.put(key_c, _unknown_output("c"), echo=echo_c)

    assert not _entry(root, key_a).exists()  # вытеснен самый старый
    assert _entry(root, key_b).exists()
    assert _entry(root, key_c).exists()
    assert sum(p.stat().st_size for p in root.rglob("*.json")) <= 2 * size
    assert cache.stats()["evicted"] == 1


def test_stats_snapshot_has_stable_keys(tmp_path: Path) -> None:
    stats = EvalVerdictCache(tmp_path / "cache").stats()
    assert set(stats) == {
        "hits",
        "misses",
        "writes",
        "corrupt",
        "evicted",
        "invalid_key",
        "echo_mismatch",
        "rejected",
    }
    assert all(isinstance(value, int) for value in stats.values())


# ---------------------------------------------------------------------------
# Главная регрессия: второй прогон не спрашивает оценщика
# ---------------------------------------------------------------------------


def test_second_run_makes_zero_evaluator_calls_and_equal_cell(tmp_path: Path) -> None:
    """Без кэша это падает: второй прогон обязан сделать ноль ``complete``."""
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    cache = EvalVerdictCache(tmp_path / "cache")

    first = _evaluate(data, evaluator, cache)
    calls_after_first = len(evaluator.calls)
    second = _evaluate(data, evaluator, cache)

    assert calls_after_first == 1
    assert len(evaluator.calls) == calls_after_first  # хит: живого вызова не было
    assert second.cell.model_dump_json() == first.cell.model_dump_json()
    assert cache.stats()["hits"] == 1
    assert cache.stats()["writes"] == 1


def test_cache_none_keeps_live_calls(tmp_path: Path) -> None:
    """``eval_cache=None`` — поведение прежнее (два прогона → два вызова)."""
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])

    _evaluate(data, evaluator, None)
    _evaluate(data, evaluator, None)

    assert len(evaluator.calls) == 2


def test_prompt_hash_and_model_id_persisted_for_live_and_cached_cell(
    tmp_path: Path,
) -> None:
    """Провенанс ячейки виден в БД и на живом вызове, и на хите (R7/§13)."""
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    cache = EvalVerdictCache(tmp_path / "cache")
    expected_hash = compile_prompt(
        param=_param(data),
        product_id=data["product_id"],
        chunks=_chunks(data),
    ).prompt_hash
    query = (
        "SELECT prompt_hash, model_id FROM cells "
        "WHERE run_id = ? AND product_id = ? AND param_id = ?"
    )
    key = (data["run_id"], data["product_id"], _param(data).param_id)

    with SqliteStore(":memory:") as store:
        store.create_run(data["run_id"], _run_config())

        _evaluate(data, evaluator, cache, store=store)
        first = store.conn.execute(query, key).fetchone()
        assert first is not None
        assert first[0] == expected_hash
        assert first[1] == "fake-model"

        _evaluate(data, evaluator, cache, store=store)  # хит
        second = store.conn.execute(query, key).fetchone()

    assert len(evaluator.calls) == 1
    assert second == first


# ---------------------------------------------------------------------------
# Ловушка prompt_hash: ключ обязан меняться по каждому входу промпта
# ---------------------------------------------------------------------------


def _assert_prompt_hash_equal(data: dict, **second: object) -> None:
    """``prompt_hash`` §5 не видит эти изменения — это и есть ловушка."""
    baseline = compile_prompt(
        param=_param(data),
        product_id=data["product_id"],
        chunks=_chunks(data),
    )
    changed = compile_prompt(
        param=second.get("param", _param(data)),
        product_id=second.get("product_id", data["product_id"]),
        chunks=second.get("chunks", _chunks(data)),
        satellites=second.get("satellites", ()),
        extra_instructions=second.get("extra_instructions", ""),
    )
    assert changed.prompt_hash == baseline.prompt_hash


def test_satellites_change_busts_key_despite_equal_prompt_hash(
    tmp_path: Path,
) -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    cache = EvalVerdictCache(tmp_path / "cache")
    satellite = Satellite(
        role="role_a", state=SatelliteState.DRAFT, component="component-a"
    )

    _evaluate(data, evaluator, cache)
    _assert_prompt_hash_equal(data, satellites=(satellite,))
    _evaluate(data, evaluator, cache, satellites=(satellite,))

    assert len(evaluator.calls) == 2  # разные промпты → разные ключи


def test_title_change_busts_key_despite_equal_prompt_hash(tmp_path: Path) -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    cache = EvalVerdictCache(tmp_path / "cache")
    changed = _param(data).model_copy(update={"title": "Совсем другой заголовок"})

    _evaluate(data, evaluator, cache)
    _assert_prompt_hash_equal(data, param=changed)
    _evaluate(data, evaluator, cache, param=changed)

    assert len(evaluator.calls) == 2


def test_extra_instructions_change_busts_key_despite_equal_prompt_hash(
    tmp_path: Path,
) -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    cache = EvalVerdictCache(tmp_path / "cache")
    extra = "Дополнительно: игнорировать маркетинговые формулировки."

    _evaluate(data, evaluator, cache)
    _assert_prompt_hash_equal(data, extra_instructions=extra)
    _evaluate(data, evaluator, cache, extra_instructions=extra)

    assert len(evaluator.calls) == 2


def test_product_id_change_busts_key_despite_equal_prompt_hash(tmp_path: Path) -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    cache = EvalVerdictCache(tmp_path / "cache")

    _evaluate(data, evaluator, cache)
    _assert_prompt_hash_equal(data, product_id="p2")
    _evaluate(data, evaluator, cache, product_id="p2")

    assert len(evaluator.calls) == 2


def test_chunk_text_change_busts_key_despite_equal_prompt_hash(tmp_path: Path) -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    cache = EvalVerdictCache(tmp_path / "cache")
    changed_chunks = [
        dataclasses.replace(chunk, text=chunk.text + " Дополнительный абзац.")
        for chunk in _chunks(data)
    ]

    _evaluate(data, evaluator, cache)
    _assert_prompt_hash_equal(data, chunks=changed_chunks)
    _evaluate(data, evaluator, cache, chunks=changed_chunks)

    assert len(evaluator.calls) == 2


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("model_id", "other-model"),
        ("model_version", "2.0"),
        ("seed", 7),
        ("temperature", 0.5),
        ("decoder", "xgrammar"),
        ("endpoint_kind", "self_host"),
    ],
)
def test_evaluator_config_change_busts_key(
    tmp_path: Path, attribute: str, value: object
) -> None:
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    cache = EvalVerdictCache(tmp_path / "cache")

    _evaluate(data, evaluator, cache)
    setattr(evaluator, attribute, value)
    _evaluate(data, evaluator, cache)

    assert len(evaluator.calls) == 2


# ---------------------------------------------------------------------------
# Пост-шаги обязаны пере-исполняться на хите
# ---------------------------------------------------------------------------


def test_cached_absent_still_hits_coverage_gate(tmp_path: Path) -> None:
    """Ф0: кэшированный ``❌`` при неполном корпусе снова становится ``pending``."""
    data = _data()
    evaluator = _ScriptedEvaluator([_absent_output()])
    cache = EvalVerdictCache(tmp_path / "cache")

    first = _evaluate(data, evaluator, cache, coverage=_coverage(complete=True))
    assert first.cell.state is CellState.EVALUATED
    assert first.cell.status is Status.ABSENT

    second = _evaluate(
        data,
        evaluator,
        cache,
        coverage=_coverage(complete=False, blocked={"robots_disallow": 4}),
    )

    assert len(evaluator.calls) == 1  # хит
    assert second.cell.state is CellState.PENDING
    assert second.cell.review_reason == "corpus_incomplete_absence"
    assert second.cell.reviewed_by is None  # I6/I7


def test_cached_verdict_still_hits_precedence(tmp_path: Path) -> None:
    """P6: кэшированный ``❌`` против высокой ``✅`` снова даёт pending-конфликт."""
    data = _data()
    evaluator = _ScriptedEvaluator([_absent_output()])
    cache = EvalVerdictCache(tmp_path / "cache")
    store = _RecordingStore()
    high = Cell(
        run_id=data["run_id"],
        product_id=data["product_id"],
        param_id=_param(data).param_id,
        state=CellState.EVALUATED,
        status=Status.OK,
        source_kind=SourceKind.REGISTRY,
        evidence=[LLMEvidence(source_id="reg-1", quote="registry fact")],
    )

    _evaluate(
        data, evaluator, cache, coverage=_coverage(complete=True), store=store
    )
    _evaluate(
        data,
        evaluator,
        cache,
        coverage=_coverage(complete=True),
        store=store,
        protected=high,
    )

    assert len(evaluator.calls) == 1  # хит
    saved = store.cells[-1]
    assert saved.state is CellState.PENDING
    assert saved.review_reason == "precedence_conflict"
    assert saved.source_kind is SourceKind.REGISTRY


def test_cached_output_failing_revalidation_falls_through_to_live(tmp_path: Path) -> None:
    """Смена resolver'а: кэш не проходит валидацию → живой вызов, плохой вердикт не публикуется."""
    data = _data()
    live = LLMCellOutput(
        status=Status.OK,
        justification="Подтверждено другим фрагментом.",
        evidence=[
            LLMEvidence(
                source_id="chunk_a1",
                quote="Документ описывает экспорт данных в формате CSV.",
            )
        ],
    )
    evaluator = _ScriptedEvaluator([_ok_output(data), live])
    cache = EvalVerdictCache(tmp_path / "cache")

    first = _evaluate(data, evaluator, cache)
    assert first.cell.status is Status.OK
    assert first.cell.evidence[0].source_id == "chunk_a2"

    class _Resolver:
        """Резолвит только ``chunk_a1``: цитата из ``chunk_a2`` больше не разрешима."""

        def resolve(self, source_id: str) -> ResolvedSource | None:
            if source_id != "chunk_a1":
                return None
            return ResolvedSource(
                chunk_id=source_id,
                source_url="https://example.test/chunk_a1",
                content_hash="hash_a1",
            )

    second = _evaluate(data, evaluator, cache, resolver=_Resolver())

    assert len(evaluator.calls) == 2  # кэш-попытка + живой вызов
    assert second.cell.state is CellState.EVALUATED
    assert second.cell.status is Status.OK
    assert second.cell.evidence[0].source_id == "chunk_a1"  # плохой кэш не опубликован


@pytest.mark.parametrize(
    ("strict", "state"),
    [(True, CellState.FAILED), (False, CellState.PRELIMINARY)],
)
def test_unresolved_run_never_writes_verdict(
    tmp_path: Path, strict: bool, state: CellState
) -> None:
    """``FAILED``/``PRELIMINARY`` — исходы сбойной попытки, а не вердикты: кэш пуст."""
    data = _data()
    evaluator = _ScriptedEvaluator([_bad_output(data)])
    cache = EvalVerdictCache(tmp_path / "cache")

    outcome = _evaluate(data, evaluator, cache, max_retries=2, strict=strict)

    assert outcome.cell.state is state
    assert cache.stats()["writes"] == 0
    assert list((tmp_path / "cache").rglob("*.json")) == []


def test_cached_unknown_still_requires_i3_protocol(tmp_path: Path) -> None:
    """``❓`` кэшируется, но I3-проверка исчерпания выполняется downstream всегда."""
    data = _data()
    evaluator = _ScriptedEvaluator([_unknown_output()])
    cache = EvalVerdictCache(tmp_path / "cache")

    first = _evaluate(data, evaluator, cache)
    second = _evaluate(data, evaluator, cache)

    assert len(evaluator.calls) == 1  # ❓ — легитимный вердикт, хит
    assert second.cell.status is Status.UNKNOWN
    assert second.cell.model_dump_json() == first.cell.model_dump_json()

    violations = check_unknown_exhaustion(second.cell, None)
    assert violations and violations[0].code == "I3"
