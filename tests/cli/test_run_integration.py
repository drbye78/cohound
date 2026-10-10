"""Интеграция CLI ``run`` с реальным реестром фаз и портами профиля (L3, W13).

Проверяют:

* ``build_adapters("local")`` собирает безопасные in-process порты (чанкер/экстрактор
  всегда; тяжёлые/сетевые — грациозно ``None``, когда зависимости/конфигурации нет);
* ``main(["run", ...])`` строит **реальный** реестр фаз (:func:`build_phases`) и порты
  профиля, а не echo-заглушки;
* гейт ``g1`` остаётся человеческим: без строки в ``gate_decisions`` прогон
  останавливается ``blocked`` и не доходит до ``compute``/``report`` (I6/I7);
* сквозной ``local``-прогон с предзаписанными решениями гейтов и внедрёнными заглушками
  внешних портов завершается и пишет ``run_manifest.yaml`` (реальные compute/report).

Данные категорийно-нейтральны (§1.3). Внешние системы заменены заглушками
``tests/fakes`` (сеть не используется; I18/I19 не ослабляются).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest
import yaml

from core.adapters.extract_html import HtmlExtractor
from core.cli.main import EXIT_BLOCKED, EXIT_OK, main
from core.cli.wiring import (
    LOCAL_PROFILE,
    SPLIT_PROFILE,
    WiringError,
    build_adapters,
    build_store,
)
from core.domain import LLMCellOutput, Status
from core.pipeline import (
    GATE_PHASES,
    IMPLEMENTED_PHASES,
    Ports,
    build_phases,
)
from core.pipeline.gates import gate_g1, gate_g2, gate_g3, gate_g4
from tests.fakes.ports import FakeEvaluator, FakeLexicalIndex
from tests.helpers.output import json_out
from tests.helpers.packages import run_argv, valid_package


# ---------------------------------------------------------------------------
# Помощники
# ---------------------------------------------------------------------------


def _has_module(name: str) -> bool:
    """Установлен ли модуль (без импорта тяжёлого пакета)."""
    return importlib.util.find_spec(name) is not None


# ---------------------------------------------------------------------------
# build_adapters("local")
# ---------------------------------------------------------------------------


def test_local_adapters_set_safe_ports_and_skip_missing_heavy() -> None:
    """Чанкер/экстрактор всегда; тяжёлые — только при доступной зависимости."""
    ports = build_adapters(profile=LOCAL_PROFILE)

    assert isinstance(ports, Ports)
    assert ports.chunker is not None
    assert ports.extractor is not None
    # Без docling экстрактор — ровно HTML-адаптер (документный путь только при наличии).
    if not _has_module("docling"):
        assert isinstance(ports.extractor, HtmlExtractor)
    # Сетевые адаптеры и не-веб контуры не инстанцируются без конфигурации.
    assert ports.crawler is None
    assert ports.search is None
    assert ports.evaluator is None
    assert ports.registry is None
    assert ports.vector_index is None
    # Тяжёлые опциональные: ровно тогда, когда доступна зависимость (graceful, без raise).
    assert (ports.embedder is not None) is _has_module("sentence_transformers")
    assert (ports.index_builder is not None) is _has_module("numpy")
    assert (ports.lexical_index is not None) is _has_module("tantivy")


def test_local_adapters_accept_injected_evaluator_and_registry() -> None:
    """Оценщик/реестр не создаются сами, но принимаются инъекцией (генератор/конфиг)."""
    evaluator = FakeEvaluator(default=LLMCellOutput(
        status=Status.ABSENT, justification="нет в срезе", evidence=[]
    ))
    registry = object()  # достаточно наличия инъекции (порт — структурный)
    ports = build_adapters(profile=LOCAL_PROFILE, evaluator=evaluator, registry=registry)  # type: ignore[arg-type]

    assert ports.evaluator is evaluator
    assert ports.registry is registry


def test_unknown_profile_still_rejected() -> None:
    with pytest.raises(WiringError):
        build_adapters(profile="nope")


def test_split_without_valid_config_still_rejected() -> None:
    """Дефолтный deploy-конфиг (local) не активирует split — честный отказ (07 §6/§7)."""
    with pytest.raises(WiringError):
        build_adapters(profile=SPLIT_PROFILE)


# ---------------------------------------------------------------------------
# run: реальный реестр фаз + блокировка на g1
# ---------------------------------------------------------------------------


def test_cmd_run_registers_real_phases_and_blocks_at_g1(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI строит реальные фазы (не заглушки); g1 без решения → ``blocked``."""
    # Дефолт --passports-dir относителен cwd: переходим в tmp_path, чтобы «нет каталога
    # passports» было детерминировано и не зависело от реальных паспортов в корне репо.
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    captured: dict[str, Any] = {}

    def spy_factory(**kwargs: Any) -> dict:
        captured.update(kwargs)
        phases = build_phases(**kwargs)
        captured["phases"] = phases
        return phases

    code = main(
        run_argv(package, run_id="g1-blocked", db=db, runs_dir=runs_dir),
        phases_factory=spy_factory,
    )
    assert code == EXIT_BLOCKED
    payload = json_out(capsys)
    assert payload["status"] == "blocked"
    assert payload["stopped_at"] == "g1"

    # Реестр — реальный: ровно не-гейтовые фазы, гейты остаются дефолтными в Runner (S9).
    phases = captured["phases"]
    assert set(phases) == set(IMPLEMENTED_PHASES)
    assert set(phases).isdisjoint(GATE_PHASES)
    assert phases["report"].__name__ == "report_phase"
    # План переиспользован: тот же run_id, что выведен CLI (без второго derive).
    assert captured["plan"].run_id == "g1-blocked"
    # Паспорта подключены (--passports-dir, дефолт passports), но каталога в cwd нет →
    # {}; URL не заданы (--urls) → {}: фазы Ш4/Ш5 легитимно no-op. doc_domains не фетчатся.
    assert captured["urls"] == {}
    assert captured["passports"] == {}
    # Не дошли до compute/report — гейт остановил раньше.
    assert {item["phase"] for item in payload["results"]}.isdisjoint({"compute", "report"})


# ---------------------------------------------------------------------------
# run: сквозной local-прогон (реальные compute/report, манифест)
# ---------------------------------------------------------------------------


def test_cmd_run_full_local_writes_manifest_with_real_compute_report(
    tmp_path: Path,
) -> None:
    """Сквозной прогон: гейты предзаписаны человеком, внешние порты — заглушки.

    Фазы — реальные ``build_phases`` (urls/паспорта пусты → Ш4/Ш5 no-op); ``report``
    пишет ``run_manifest.yaml``, значит реальные ``compute``/``report`` действительно
    выполнялись, а не echo-заглушки.
    """
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "full-local"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)

    # Предполёт: создаём строку прогона и завершаем normalize/lint/plan.
    assert main([*argv, "--until", "plan"]) == EXIT_OK

    # Человеческие решения гейтов (I6/I7): без них прогон обязан стоять на g1.
    with build_store(db) as store:
        for gate_fn in (gate_g1, gate_g2, gate_g3, gate_g4):
            gate_fn(store, run_id, actor="cli-test")

    evaluator = FakeEvaluator(
        default=LLMCellOutput(
            status=Status.ABSENT, justification="нет в срезе", evidence=[]
        )
    )
    ports = Ports(evaluator=evaluator, lexical_index=FakeLexicalIndex())

    def factory(**kwargs: Any) -> dict:
        # Фазам нужен фиксированный профиль K (I16/I20); urls/паспорта остаются пустыми.
        kwargs["retrieval_profile"] = "R0"
        kwargs["cutoff_k"] = 3
        return build_phases(**kwargs)

    code = main(argv, ports=ports, phases_factory=factory)
    assert code == EXIT_OK

    manifest_path = runs_dir / run_id / "run_manifest.yaml"
    assert manifest_path.exists()
    assert evaluator.calls, "реальная фаза evaluate должна вызвать оценщик"

    loaded = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    assert loaded["run_id"] == run_id