"""Накопление стоимости LLM в ``metrics.json`` по заходам прогона (M1/§15).

Дефект, который эти тесты закрывают: ``usage_summary()`` адаптера живёт **один процесс**,
а ``metrics.json`` до этого писался целиком, поэтому продолженный прогон (обычный сценарий
``--until corpus`` → продолжение) публиковал в машинном отчёте число вызовов **последнего**
захода — «цену фрагмента» под видом цены прогона. Это тот же класс дефекта, что и
``llm_calls: 0`` при ~117 живых вызовах: не «нет данных», а неверное число.

Накопление идёт через сам файл (схема БД не меняется): перед записью
:func:`core.pipeline.observability.write_run_metrics` читает существующий
``metrics.json`` (:func:`core.pipeline.metrics.read_prior_llm_usage`) и складывает вклад
предыдущих заходов с вкладом текущего
(:func:`core.pipeline.metrics.merge_llm_usage`).

Отдельно проверяется **единственность call-site**: накопление не идемпотентно по вкладу
процесса (второй вызов за тот же процесс посчитал бы вклад дважды), поэтому инвариант
«метрики пишутся один раз за процесс» держится структурно — тестом по AST всего ``core/``,
а не обещанием в комментарии.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from core.pipeline.metrics import (
    LLM_USAGE_ACCOUNTING,
    PriorLlmUsage,
    merge_llm_usage,
    read_prior_llm_usage,
)
from core.pipeline.observability import collect_llm_usage, write_run_metrics

#: Корень ``core/`` — область проверки единственности call-site.
CORE_ROOT = Path(__file__).resolve().parents[2] / "core"


def _metrics(runs_dir: Path, run_id: str) -> dict[str, Any]:
    """Читает записанный ``metrics.json`` прогона."""
    return json.loads(
        (runs_dir / run_id / "metrics.json").read_text(encoding="utf-8")
    )


# ---------------------------------------------------------------------------
# 1. Первый заход: файла нет → накопленное равно вкладу процесса
# ---------------------------------------------------------------------------


def test_first_run_without_file_cumulative_equals_process(tmp_path: Path) -> None:
    path = write_run_metrics(
        run_id="run_first",
        out_dir=tmp_path,
        llm_calls=7,
        tokens={"prompt": 100, "completion": 20},
    )
    data = json.loads(path.read_text(encoding="utf-8"))
    usage = data["llm_usage"]

    assert usage["calls_cumulative"] == 7
    assert usage["calls_this_process"] == 7
    assert usage["tokens_cumulative"] == {"completion": 20, "prompt": 100}
    assert usage["tokens_this_process"] == {"completion": 20, "prompt": 100}
    assert usage["processes"] == 1
    assert usage["previous_source"] == "none"
    assert usage["previous_error"] is None
    assert usage["current_source"] == "adapter"
    assert usage["accounting"] == LLM_USAGE_ACCOUNTING
    # Поле верхнего уровня не сломано и описывает ПРОГОН (накопленное), а не заход.
    assert data["llm_calls"] == 7
    assert data["tokens"] == {"completion": 20, "prompt": 100}


# ---------------------------------------------------------------------------
# 2. Второй заход: кумулятив растёт на вклад второго, processes == 2
# ---------------------------------------------------------------------------


def test_second_run_accumulates_over_existing_file(tmp_path: Path) -> None:
    write_run_metrics(
        run_id="run_second",
        out_dir=tmp_path,
        llm_calls=7,
        tokens={"prompt": 100, "completion": 20},
    )
    write_run_metrics(
        run_id="run_second",
        out_dir=tmp_path,
        llm_calls=3,
        tokens={"prompt": 50},
    )
    data = _metrics(tmp_path, "run_second")
    usage = data["llm_usage"]

    assert usage["calls_this_process"] == 3
    assert usage["calls_cumulative"] == 10
    assert usage["tokens_this_process"] == {"prompt": 50}
    # Токены складываются по категориям, а не перезаписываются картой процесса.
    assert usage["tokens_cumulative"] == {"completion": 20, "prompt": 150}
    assert usage["processes"] == 2
    assert usage["previous_source"] == "ok"
    assert data["llm_calls"] == 10
    assert data["tokens"] == {"completion": 20, "prompt": 150}


# ---------------------------------------------------------------------------
# 3. Три захода подряд: сумма верна, двойного счёта нет
# ---------------------------------------------------------------------------


def test_three_runs_sum_without_double_counting(tmp_path: Path) -> None:
    for calls, tokens in (
        (2, {"prompt": 10}),
        (3, {"prompt": 20, "completion": 5}),
        (5, {"completion": 15}),
    ):
        write_run_metrics(
            run_id="run_three", out_dir=tmp_path, llm_calls=calls, tokens=tokens
        )
    data = _metrics(tmp_path, "run_three")
    usage = data["llm_usage"]

    assert usage["processes"] == 3
    assert usage["calls_cumulative"] == 10  # 2 + 3 + 5, а не 2+5+10 или 20
    assert usage["calls_this_process"] == 5  # последний заход, не сумма
    assert usage["tokens_cumulative"] == {"completion": 20, "prompt": 30}
    assert usage["tokens_this_process"] == {"completion": 15}


# ---------------------------------------------------------------------------
# 4. Битый/нечитаемый metrics.json: не роняет запись, предыдущее = 0, факт виден
# ---------------------------------------------------------------------------


def test_broken_metrics_file_does_not_raise_and_is_visible(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_broken"
    run_dir.mkdir(parents=True)
    (run_dir / "metrics.json").write_text('{"llm_calls": 42, "tokens"', encoding="utf-8")

    path = write_run_metrics(
        run_id="run_broken", out_dir=tmp_path, llm_calls=4, tokens={"prompt": 9}
    )
    data = json.loads(path.read_text(encoding="utf-8"))
    usage = data["llm_usage"]

    # Прогон не упал, файл перезаписан валидным JSON.
    assert data["llm_calls"] == 4
    assert usage["calls_cumulative"] == 4
    # Ноль не выдаётся за факт: источник предыдущего назван `unreadable`, текст — рядом.
    assert usage["previous_source"] == "unreadable"
    assert usage["previous_error"]
    assert usage["processes"] == 1
    # Никакого «полуфайла» и мусора от атомарной записи не осталось.
    assert not list(run_dir.glob("metrics.json.*.tmp"))


def test_non_object_metrics_root_is_unreadable(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_list"
    run_dir.mkdir(parents=True)
    (run_dir / "metrics.json").write_text("[1, 2, 3]", encoding="utf-8")

    path = write_run_metrics(run_id="run_list", out_dir=tmp_path, llm_calls=2)
    usage = json.loads(path.read_text(encoding="utf-8"))["llm_usage"]
    assert usage["calls_cumulative"] == 2
    assert usage["previous_source"] == "unreadable"
    assert "list" in (usage["previous_error"] or "")


def test_read_prior_reports_missing_file_as_none(tmp_path: Path) -> None:
    prior = read_prior_llm_usage(tmp_path / "nope" / "metrics.json")
    assert prior == PriorLlmUsage()
    assert prior.source == "none"
    assert prior.calls == 0
    assert prior.processes == 0


# ---------------------------------------------------------------------------
# 5. Токены накапливаются по категориям (в т.ч. категории, которых нет у процесса)
# ---------------------------------------------------------------------------


def test_tokens_accumulate_per_category_not_overwritten(tmp_path: Path) -> None:
    write_run_metrics(
        run_id="run_tokens",
        out_dir=tmp_path,
        llm_calls=1,
        tokens={"prompt": 10, "completion": 4, "cached": 2},
    )
    # Второй заход не знает про `cached` — категория обязана сохраниться.
    write_run_metrics(
        run_id="run_tokens", out_dir=tmp_path, llm_calls=1, tokens={"prompt": 1}
    )
    usage = _metrics(tmp_path, "run_tokens")["llm_usage"]
    assert usage["tokens_cumulative"] == {"cached": 2, "completion": 4, "prompt": 11}
    assert usage["tokens_this_process"] == {"prompt": 1}


# ---------------------------------------------------------------------------
# 6. Шов: у адаптера нет `usage_summary` → нули, без падения, и это видно формой
# ---------------------------------------------------------------------------


def test_absent_usage_summary_yields_zeros_and_is_marked(tmp_path: Path) -> None:
    snapshot = collect_llm_usage(SimpleNamespace())
    assert snapshot.calls == 0
    assert snapshot.tokens == {}
    assert snapshot.source == "absent"
    assert snapshot.error is None

    path = write_run_metrics(
        run_id="run_seam",
        out_dir=tmp_path,
        llm_calls=snapshot.calls,
        tokens=snapshot.tokens or None,
        llm_usage_source=snapshot.source,
        llm_usage_error=snapshot.error,
    )
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["llm_calls"] == 0
    assert data["tokens"] == {}
    assert data["llm_usage"]["calls_cumulative"] == 0
    # «Не умеем считать» ≠ «вызовов не было»: источник вклада назван.
    assert data["llm_usage"]["current_source"] == "absent"


def test_failing_usage_summary_is_reported_not_raised() -> None:
    class _Broken:
        def usage_summary(self) -> dict[str, Any]:
            raise RuntimeError("провайдер отвалился")

    snapshot = collect_llm_usage(_Broken())
    assert snapshot.calls == 0
    assert snapshot.source == "error"
    assert "RuntimeError" in (snapshot.error or "")


def test_collect_llm_usage_reads_adapter_numbers() -> None:
    class _Adapter:
        def usage_summary(self) -> dict[str, Any]:
            return {"calls": 5, "tokens": {"prompt": 40, "completion": 8}}

    snapshot = collect_llm_usage(_Adapter())
    assert (snapshot.calls, snapshot.tokens) == (5, {"completion": 8, "prompt": 40})
    assert snapshot.source == "adapter"


# ---------------------------------------------------------------------------
# Совместимость с прежним форматом файла (без блока ``llm_usage``)
# ---------------------------------------------------------------------------


def test_legacy_metrics_file_counts_as_one_prior_process(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_legacy"
    run_dir.mkdir(parents=True)
    # Формат до накопления: ``llm_calls``/``tokens`` описывают один заход.
    (run_dir / "metrics.json").write_text(
        json.dumps({"llm_calls": 11, "tokens": {"prompt": 30}}), encoding="utf-8"
    )
    path = write_run_metrics(
        run_id="run_legacy", out_dir=tmp_path, llm_calls=2, tokens={"prompt": 5}
    )
    usage = json.loads(path.read_text(encoding="utf-8"))["llm_usage"]
    assert usage["calls_cumulative"] == 13
    assert usage["tokens_cumulative"] == {"prompt": 35}
    # Точное число заходов в legacy-файле не восстановить — значение честно нижняя граница.
    assert usage["processes"] == 2
    assert usage["previous_source"] == "ok"


def test_legacy_metrics_file_with_zero_usage_does_not_invent_a_process(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run_legacy_zero"
    run_dir.mkdir(parents=True)
    (run_dir / "metrics.json").write_text(
        json.dumps({"llm_calls": 0, "tokens": {}}), encoding="utf-8"
    )
    path = write_run_metrics(run_id="run_legacy_zero", out_dir=tmp_path, llm_calls=1)
    usage = json.loads(path.read_text(encoding="utf-8"))["llm_usage"]
    assert usage["calls_cumulative"] == 1
    assert usage["processes"] == 1


def test_malformed_usage_field_is_zero_with_note(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_bad_field"
    run_dir.mkdir(parents=True)
    (run_dir / "metrics.json").write_text(
        json.dumps({"llm_calls": True, "tokens": {"prompt": "много"}}),
        encoding="utf-8",
    )
    prior = read_prior_llm_usage(run_dir / "metrics.json")
    # ``bool`` — испорченное поле, а не «один вызов»; заметка объясняет ноль.
    assert prior.calls == 0
    assert prior.tokens == {}
    assert prior.error
    assert "llm_calls" in prior.error


# ---------------------------------------------------------------------------
# Единственность call-site: накопление не идемпотентно по вкладу процесса
# ---------------------------------------------------------------------------


def _write_run_metrics_calls(path: Path) -> int:
    """Число вызовов ``write_run_metrics(...)`` в файле (AST, не грепп)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return sum(
        1
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "write_run_metrics"
    )


def test_write_run_metrics_has_single_production_call_site() -> None:
    """Инвариант, на котором держится накопление: один вызов за процесс.

    Второй вызов за тот же процесс сложил бы вклад процесса дважды. Функция не может
    отличить «второй вызов» от «следующий заход» по файлу, поэтому инвариант структурный:
    call-site один — ``core/cli/commands/run.py`` (метрики пишутся один раз после фаз).
    """
    call_sites = {
        str(path.relative_to(CORE_ROOT)): count
        for path in sorted(CORE_ROOT.rglob("*.py"))
        if (count := _write_run_metrics_calls(path))
    }
    assert call_sites == {"cli/commands/run.py": 1}


# ---------------------------------------------------------------------------
# Детерминизм и чистая функция слияния
# ---------------------------------------------------------------------------


def test_merge_is_pure_and_sorted() -> None:
    block = merge_llm_usage(
        PriorLlmUsage(calls=3, tokens={"z": 1, "a": 2}, processes=1, source="ok"),
        calls_this_process=1,
        tokens_this_process={"b": 5, "a": 1},
    )
    assert block["calls_cumulative"] == 4
    assert block["tokens_cumulative"] == {"a": 3, "b": 5, "z": 1}
    assert list(block["tokens_cumulative"]) == ["a", "b", "z"]
    assert block["processes"] == 2
    # Предыдущее состояние не мутируется: слияние — функция, а не инкремент в файле.
    prior = PriorLlmUsage(calls=3, tokens={"a": 2}, processes=1, source="ok")
    merge_llm_usage(prior, calls_this_process=1, tokens_this_process={"a": 1})
    assert prior.calls == 3
    assert prior.tokens == {"a": 2}
