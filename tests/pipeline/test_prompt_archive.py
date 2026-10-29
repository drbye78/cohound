"""Тесты аудита **исполненных** промптов Ш6: ``runs/{run_id}/prompts/{prompt_hash}.json`` (V7).

До правки ``save_prompt`` существовал и был покрыт тестами, но конвейер его не вызывал:
каталога ``prompts/`` в живом прогоне не было вовсе, и восстановить вход модели по
артефактам прогона было нельзя (набор переданных чанков в БД не лежит — в ``cell_evidence``
попадают только процитированные). Здесь фиксируется то, что обязано держаться:

* артефакт — **ровно тот** промпт, который ушёл в модель (сверка с ``req.prompt``
  скриптованного оценщика), а не пере-сборка;
* запись content-addressed: повтор с тем же ``prompt_hash`` не перезаписывает файл, а
  расхождение содержимого видно как ``prompt_hash_conflict``;
* сбой I/O не роняет фазу и не пропадает молча — ``prompt_save_failed`` в наблюдении;
* неисполненный промпт не пишется: пустой срез (вызова нет) и хит ``eval_cache``;
* ключ ``eval_cache`` от аудита не меняется (он по тексту промпта).

Фикстуры берутся из ``test_evaluate``/``test_evaluate_batching`` (как в
``test_eval_cache``); сеть не используется, данные нейтральны (§1.3).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.domain import LLMCellOutput, Status
from core.pipeline.eval_cache import EvalVerdictCache
from core.pipeline.evaluate import evaluate_cell, evaluate_phase
from core.pipeline.prompt import (
    PROMPT_HASH_CONFLICT,
    PROMPT_SAVE_FAILED,
    PromptArchive,
    compile_prompt,
)
from core.pipeline.runner import Ports
from tests.fakes.ports import FakeEvaluator
from tests.pipeline.test_evaluate import (
    _chunks,
    _data,
    _eval_conditions,
    _ok_output,
    _param,
    _ScriptedEvaluator,
)
from tests.pipeline.test_evaluate_batching import (
    _CountingEmbedder,
    _plan,
    _RecordingVectorIndex,
    _store,
)
from tests.pipeline.test_evaluate_batching import (
    _ok_output as _batch_ok_output,
)

# ---------------------------------------------------------------------------
# Архив: content-addressed, идемпотентный, видимый при сбое
# ---------------------------------------------------------------------------


def _compiled(*, product_id: str = "p1", rule: str | None = None):  # noqa: ANN202
    data = _data()
    param = _param(data)
    if rule is not None:
        param = param.model_copy(update={"rule": rule})
    return compile_prompt(param=param, product_id=product_id, chunks=_chunks(data))


def test_archive_writes_one_file_per_prompt_hash(tmp_path: Path) -> None:
    """Два разных входа — два файла; повтор того же входа — тот же файл (не перезапись)."""
    archive = PromptArchive(tmp_path / "prompts")
    first = _compiled(product_id="p1")
    # ``prompt_hash`` покрывает правило/absence/набор chunk_id, но не текст: другое
    # правило даёт другой hash и другой файл.
    other = _compiled(rule="Подтвердить другое свойство того же продукта.")

    path = archive.record(first)
    assert path is not None
    assert path.name == f"{first.prompt_hash}.json"
    assert json.loads(path.read_text(encoding="utf-8")) == first.to_json()

    again = archive.record(first)
    assert again == path
    assert archive.saved == 1
    assert archive.reused == 1
    assert archive.bytes_written > 0
    assert archive.errors == []

    archive.record(other)
    assert archive.saved == 2
    assert len(list((tmp_path / "prompts").glob("*.json"))) == 2


def test_archive_reports_hash_conflict_without_overwriting(tmp_path: Path) -> None:
    """Файл с тем же hash и другим текстом — аномалия, а не тихая подмена.

    ``prompt_hash`` обязан адресовать вход: расхождение означает, что формула hash
    перестала его покрывать (напр. правка шаблона без bump версии), и доверять такому
    артефакту нельзя. Файл при этом не перезаписывается (аудит прошлых прогонов цел).
    """
    archive = PromptArchive(tmp_path / "prompts")
    compiled = _compiled()
    path = archive.record(compiled)
    assert path is not None
    tampered = json.dumps({**compiled.to_json(), "text": "ДРУГОЙ ТЕКСТ"}, ensure_ascii=False)
    path.write_text(tampered + "\n", encoding="utf-8")

    archive.record(compiled)

    assert archive.errors and archive.errors[0].startswith(PROMPT_HASH_CONFLICT)
    assert archive.saved == 1
    assert path.read_text(encoding="utf-8") == tampered + "\n"


def test_archive_io_failure_is_visible_and_does_not_raise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Сбой записи: фаза не падает, но и не молчит — токен + сжатый агрегат.

    Артефакт аудита вторичен к измерению: ``OSError`` из фазы отнял бы уже полученные
    вердикты. Честный исход — вердикты остаются, а причина видна в наблюдении.
    """
    archive = PromptArchive(tmp_path / "prompts")

    def boom(*_args: object, **_kwargs: object) -> Path:
        raise OSError("диск переполнен")

    monkeypatch.setattr("core.pipeline.prompt.save_prompt", boom)

    assert archive.record(_compiled()) is None
    assert archive.saved == 0
    assert archive.errors and archive.errors[0].startswith(PROMPT_SAVE_FAILED)
    assert "диск переполнен" in archive.errors[0]

    # Повторы сжимаются: иначе упавший диск дал бы по строке на каждую ячейку.
    assert archive.record(_compiled(rule="Подтвердить другое свойство.")) is None
    assert len(archive.errors) == 1
    assert "сбоев записи: 2" in archive.summary()


# ---------------------------------------------------------------------------
# Ш6: пишется ровно исполненный промпт
# ---------------------------------------------------------------------------


def test_evaluate_cell_saves_the_prompt_that_went_to_the_model(tmp_path: Path) -> None:
    """Текст файла байт-в-байт равен промпту запроса к оценщику (не пере-сборка)."""
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    archive = PromptArchive(tmp_path / "prompts")

    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=evaluator,
        chunks=_chunks(data),
        prompt_archive=archive,
    )

    assert len(evaluator.calls) == 1
    files = list((tmp_path / "prompts").glob("*.json"))
    assert len(files) == 1
    payload = json.loads(files[0].read_text(encoding="utf-8"))
    assert payload["text"] == evaluator.calls[0].prompt
    assert payload["prompt_hash"] == outcome.prompt_hash
    assert files[0].stem == outcome.prompt_hash
    assert archive.saved == 1 and archive.reused == 0 and archive.errors == []


def test_retry_saves_one_file_not_one_per_attempt(tmp_path: Path) -> None:
    """Повтор переиздаёт тот же вход: файл один, хотя вызовов больше."""
    data = _data()
    evaluator = _ScriptedEvaluator([RuntimeError("таймаут"), _ok_output(data)])
    archive = PromptArchive(tmp_path / "prompts")

    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=evaluator,
        chunks=_chunks(data),
        prompt_archive=archive,
    )

    assert len(evaluator.calls) == 2
    assert archive.saved == 1
    assert len(list((tmp_path / "prompts").glob("*.json"))) == 1


def test_empty_slice_is_not_an_executed_prompt(tmp_path: Path) -> None:
    """``chunks == ()`` — модель не вызывалась, значит и артефакта нет."""
    data = _data()
    evaluator = _ScriptedEvaluator([_ok_output(data)])
    archive = PromptArchive(tmp_path / "prompts")

    evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=evaluator,
        chunks=[],
        prompt_archive=archive,
    )

    assert evaluator.calls == []
    assert archive.saved == 0
    assert not (tmp_path / "prompts").exists()


def test_cache_hit_writes_no_prompt(tmp_path: Path) -> None:
    """Хит ``eval_cache`` — модель не вызывалась: промпт этого прогона не исполнен.

    Ключ кэша строится по **тексту** промпта, поэтому аудит на него не влияет: второй
    вызов с тем же входом — хит, и ни одной записи в каталог аудита не добавляется.
    """
    data = _data()
    cache = EvalVerdictCache(tmp_path / "cache")
    archive = PromptArchive(tmp_path / "prompts")

    first_evaluator = _ScriptedEvaluator([_ok_output(data)])
    first = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=first_evaluator,
        chunks=_chunks(data),
        eval_cache=cache,
        eval_conditions=_eval_conditions(),
        prompt_archive=archive,
    )
    assert archive.saved == 1

    second_evaluator = _ScriptedEvaluator([_ok_output(data)])
    second_archive = PromptArchive(tmp_path / "prompts")
    second = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=second_evaluator,
        chunks=_chunks(data),
        eval_cache=cache,
        eval_conditions=_eval_conditions(),
        prompt_archive=second_archive,
    )

    assert second_evaluator.calls == []  # вердикт пришёл из кэша
    assert second.prompt_hash == first.prompt_hash
    assert second_archive.saved == 0 and second_archive.reused == 0
    assert cache.stats()["hits"] == 1


# ---------------------------------------------------------------------------
# Фаза Ш6: каталог прогона и наблюдаемость
# ---------------------------------------------------------------------------


def test_evaluate_phase_writes_prompts_under_the_run_directory(tmp_path: Path) -> None:
    """Фаза пишет ``{run_dir}/prompts`` и публикует наблюдённый объём в ``log_entries``."""
    store = _store()
    plan = _plan()
    evaluator = FakeEvaluator(default=_batch_ok_output("chunk-p1"))
    ports = Ports(
        evaluator=evaluator,
        embedder=_CountingEmbedder(),
        vector_index=_RecordingVectorIndex(),
    )
    prompt_dir = tmp_path / plan.run_id / "prompts"
    try:
        result = evaluate_phase(
            plan=plan,
            ports=ports,
            store=store,
            run_id=plan.run_id,
            profile="R1",
            cutoff_k=3,
            prompt_dir=prompt_dir,
        )
    finally:
        store.close()

    assert result.status == "ok"
    assert result.errors == []
    files = sorted(prompt_dir.glob("*.json"))
    # 2 продукта × 2 параметра: у каждой ячейки свой вход (правило и/или чанки).
    assert len(files) == 4
    stored = {json.loads(path.read_text(encoding="utf-8"))["text"] for path in files}
    assert stored == {call.prompt for call in evaluator.calls}
    summary = [entry for entry in result.log_entries if "промпты" in str(entry)]
    assert summary and "4 записано" in summary[0]


def test_evaluate_phase_without_prompt_dir_writes_nothing(tmp_path: Path) -> None:
    """Модульный вызов (``prompt_dir=None``) — прежнее поведение: ни каталога, ни заметки."""
    store = _store()
    plan = _plan()
    evaluator = FakeEvaluator(default=_batch_ok_output("chunk-p1"))
    ports = Ports(
        evaluator=evaluator,
        embedder=_CountingEmbedder(),
        vector_index=_RecordingVectorIndex(),
    )
    try:
        result = evaluate_phase(
            plan=plan,
            ports=ports,
            store=store,
            run_id=plan.run_id,
            profile="R1",
            cutoff_k=3,
        )
    finally:
        store.close()

    assert result.status == "ok"
    assert [entry for entry in result.log_entries if "промпты" in str(entry)] == []
    assert not list(tmp_path.rglob("prompts"))


def test_unknown_verdict_still_saves_the_prompt(tmp_path: Path) -> None:
    """``❓`` — тоже исполненный промпт: аудит не зависит от исхода валидации."""
    data = _data()
    evaluator = _ScriptedEvaluator([LLMCellOutput(**data["unknown_output"])])
    archive = PromptArchive(tmp_path / "prompts")

    outcome = evaluate_cell(
        param=_param(data),
        product_id=data["product_id"],
        run_id=data["run_id"],
        evaluator=evaluator,
        chunks=_chunks(data),
        prompt_archive=archive,
    )

    assert outcome.cell.status is Status.UNKNOWN
    assert archive.saved == 1
