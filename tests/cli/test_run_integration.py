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
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from core.adapters.extract_html import HtmlExtractor
from core.cli.main import EXIT_BLOCKED, EXIT_ERROR, EXIT_OK, main
from core.cli.wiring import (
    LOCAL_PROFILE,
    SPLIT_PROFILE,
    WiringError,
    build_adapters,
    build_lite_embedder,
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
from core.ports.vector_index import RetrievedChunk
from tests.fakes.ports import (
    FakeEmbedder,
    FakeEvaluator,
    FakeIndexBuilder,
    FakeLexicalIndex,
    FakeVectorIndex,
)
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
    # Dense-плечо: без ``chunk_lookup``/зафиксированной сборки — явный ``None`` (P4a, N3).
    assert ports.vector_index is None
    # Лексическое плечо симметрично: без store/сборки порт не поднимается (P4a, N3).
    assert ports.lexical_index is None
    # Эмбеддер (P4a): оффлайн-lite при готовой модели → качественный bge-m3 при
    # sentence-transformers → иначе ``None`` (адаптер не фабрикуется, A3/I19).
    lite_ready = _has_module("model2vec") and build_lite_embedder().available
    assert (ports.embedder is not None) is (lite_ready or _has_module("sentence_transformers"))
    assert (ports.index_builder is not None) is _has_module("numpy")


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
    # R7/07 §7 (P1): деплой-провенанс манифеста реально заполнен (профиль local).
    from core.edge.seam_schema import SEAM_SCHEMA_VERSION

    assert loaded["deploy_profile"] == LOCAL_PROFILE
    assert loaded["edge_schema_version"] == SEAM_SCHEMA_VERSION
    assert loaded["edge_capabilities"] == {}


# ---------------------------------------------------------------------------
# E1 (A3/§13): не-понижающий персист измеренной фиксации ретривала
# ---------------------------------------------------------------------------


def _seed_measured_retrieval(
    db: Path, run_id: str, *, profile: str = "R2", cutoff: int = 7
) -> None:
    """Помечает ``retrieval_profile`` измеренным в строке ``runs`` (как experiment, A3)."""
    with build_store(db) as store:
        manifest = store.read_manifest(run_id)
        provenance = dict(manifest.settings_provenance or {})
        provenance["retrieval_profile"] = {
            "source": "measured",
            "experiment_id": "exp-1",
        }
        store.persist_manifest(
            run_id,
            manifest.model_copy(
                update={
                    "retrieval_profile": profile,
                    "retrieval_cutoff_K": cutoff,
                    "retrieval_params_json": json.dumps(
                        {"profile": profile, "cutoff_K": cutoff}, sort_keys=True
                    ),
                    "settings_provenance": provenance,
                }
            ),
        )


def _finalize_gates(db: Path, run_id: str) -> None:
    """Предзаписывает человеческие решения гейтов (I6/I7), чтобы прогон шёл до Ш9."""
    with build_store(db) as store:
        for gate_fn in (gate_g1, gate_g2, gate_g3, gate_g4):
            gate_fn(store, run_id, actor="cli-test")


def _absent_evaluator_ports() -> Ports:
    return Ports(
        evaluator=FakeEvaluator(
            default=LLMCellOutput(status=Status.ABSENT, justification="нет в срезе", evidence=[])
        ),
        lexical_index=FakeLexicalIndex(),
    )


def _retrieval_ports() -> Ports:
    """Порты с рабочим ретривалом (лексика + dense), чтобы Ш6 реально разрешил профиль.

    Без dense-плеча профиль R1/R2 в оценке падает в изоляцию ретривала (ячейка без
    провенанса) — тогда проверка «ячейки несут измеренный профиль» была бы вакуумной.
    """
    chunk = RetrievedChunk(
        chunk_id="chunk_a1",
        product_id="p1",
        text="Свойство А доступно.",
        content_hash="hash_a1",
        heading_path="",
        build_id="ib_fake",
        score=1.0,
    )
    return Ports(
        evaluator=FakeEvaluator(
            default=LLMCellOutput(
                status=Status.ABSENT, justification="нет в срезе", evidence=[]
            )
        ),
        lexical_index=FakeLexicalIndex(docs=[chunk]),
        vector_index=FakeVectorIndex(docs=[chunk]),
        embedder=FakeEmbedder(),
        index_builder=FakeIndexBuilder(),
    )


def test_cmd_run_plain_rerun_preserves_measured_profile(tmp_path: Path) -> None:
    """E1/A3: повторный ``run`` без флага не затирает измеренную фиксацию дефолтом."""
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "measured-rerun"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)
    assert main([*argv, "--until", "plan"]) == EXIT_OK
    _seed_measured_retrieval(db, run_id, profile="R2", cutoff=7)
    _finalize_gates(db, run_id)

    assert main(argv, ports=_absent_evaluator_ports()) == EXIT_OK

    loaded = yaml.safe_load((runs_dir / run_id / "run_manifest.yaml").read_text("utf-8"))
    assert loaded["retrieval_profile"] == "R2"
    assert loaded["retrieval_cutoff_K"] == 7
    assert loaded["settings_provenance"]["retrieval_profile"]["source"] == "measured"


def test_cmd_run_accepts_explicit_profile_equal_to_measured(tmp_path: Path) -> None:
    """E1/A3: ``--retrieval-profile``, равный измеренному, принимается."""
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "measured-equal"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)
    assert main([*argv, "--until", "plan"]) == EXIT_OK
    _seed_measured_retrieval(db, run_id, profile="R2", cutoff=7)
    _finalize_gates(db, run_id)

    assert (
        main([*argv, "--retrieval-profile", "R2"], ports=_absent_evaluator_ports())
        == EXIT_OK
    )

    loaded = yaml.safe_load((runs_dir / run_id / "run_manifest.yaml").read_text("utf-8"))
    assert loaded["retrieval_profile"] == "R2"
    assert loaded["settings_provenance"]["retrieval_profile"]["source"] == "measured"


def test_cmd_run_refuses_conflicting_explicit_profile(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """E1/A3: противоречащий измеренному явный профиль отвергнут до исполнения фаз."""
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "measured-conflict"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)
    assert main([*argv, "--until", "plan"]) == EXIT_OK
    _seed_measured_retrieval(db, run_id, profile="R2", cutoff=7)

    code = main([*argv, "--retrieval-profile", "R1"])

    assert code == EXIT_ERROR
    payload = json_out(capsys)
    assert payload["error"] == "§13"
    assert "hound experiment retrieval" in payload["message"]
    # Персистентная измеренная фиксация не переписана отказом.
    with build_store(db) as store:
        assert store.read_manifest(run_id).retrieval_profile == "R2"


def test_cmd_run_explicit_profile_free_without_measurement(tmp_path: Path) -> None:
    """E1: на прогоне без измеренной фиксации ``--retrieval-profile`` действует свободно."""
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "no-measurement"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)
    assert main([*argv, "--until", "plan"]) == EXIT_OK
    _finalize_gates(db, run_id)

    assert (
        main([*argv, "--retrieval-profile", "R1"], ports=_absent_evaluator_ports())
        == EXIT_OK
    )

    loaded = yaml.safe_load((runs_dir / run_id / "run_manifest.yaml").read_text("utf-8"))
    assert loaded["retrieval_profile"] == "R1"
    entry = (loaded.get("settings_provenance") or {}).get("retrieval_profile")
    assert entry is None or entry.get("source") != "measured"


def test_cmd_run_rerun_adopts_measured_fixation_into_cells(tmp_path: Path) -> None:
    """E1/A3: повторный прогон **усыновляет** измеренную фиксацию в оценку, а не только в манифест.

    Регрессия ревью: «переклейка» одного манифеста давала ложный провенанс — ячейки с
    дефолтным ``R0`` при манифесте ``measured R2``. Здесь оценка идёт реальным
    ретривал-путём, и провенанс **ячеек** обязан нести измеренные профиль и K.
    """
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "measured-adopt-cells"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)
    assert main([*argv, "--until", "plan"]) == EXIT_OK
    _seed_measured_retrieval(db, run_id, profile="R2", cutoff=7)
    _finalize_gates(db, run_id)

    # Без флага: эффективная настройка = измеренная фиксация (усыновление в аргументы).
    assert main(argv, ports=_retrieval_ports()) == EXIT_OK

    with build_store(db) as store:
        cells = list(store.iter_cells(run_id))
        manifest = store.read_manifest(run_id)

    assert cells, "Ш6 обязан записать ячейки (реальный ретривал-путь)"
    assert {cell.retrieval_profile for cell in cells} == {"R2"}
    assert {
        json.loads(cell.retrieval_params_json or "{}").get("cutoff_K") for cell in cells
    } == {7}
    # Манифест согласован с ячейками и несёт measured (персист-защита остаётся).
    assert manifest.retrieval_profile == "R2"
    assert manifest.retrieval_cutoff_K == 7
    assert manifest.settings_provenance["retrieval_profile"]["source"] == "measured"


def test_cmd_run_injected_profile_reaches_cells(tmp_path: Path) -> None:
    """``main(retrieval_profile=...)`` (тест-шов) применяется как явный профиль, не затирается.

    Слой adjunct: условная инъекция в ``main()`` сохраняет CLI-ввод оператора, но при явном
    значении шва профиль доходит до оценки (ячеек). Проверяем canonical-dest путь.
    """
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "injected-profile"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)
    assert main([*argv, "--until", "plan"]) == EXIT_OK
    _finalize_gates(db, run_id)

    assert main(argv, ports=_retrieval_ports(), retrieval_profile="R2") == EXIT_OK

    with build_store(db) as store:
        cells = list(store.iter_cells(run_id))
    assert {cell.retrieval_profile for cell in cells} == {"R2"}


def test_cmd_run_explicit_cutoff_wins_over_measured(tmp_path: Path) -> None:
    """Явный ``--cutoff-k`` приоритетнее фиксации: K оператора не затирается молча.

    Профиль усыновляется (измеренный R2), но явный ``--cutoff-k 25`` — ввод оператора, он
    доходит и до ячеек, и до манифеста (иначе запись была бы несогласованной).
    """
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "explicit-cutoff"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)
    assert main([*argv, "--until", "plan"]) == EXIT_OK
    _seed_measured_retrieval(db, run_id, profile="R2", cutoff=7)
    _finalize_gates(db, run_id)

    assert main([*argv, "--cutoff-k", "25"], ports=_retrieval_ports()) == EXIT_OK

    with build_store(db) as store:
        cells = list(store.iter_cells(run_id))
        manifest = store.read_manifest(run_id)
    assert {cell.retrieval_profile for cell in cells} == {"R2"}
    assert {
        json.loads(cell.retrieval_params_json or "{}").get("cutoff_K") for cell in cells
    } == {25}
    assert manifest.retrieval_profile == "R2"
    assert manifest.retrieval_cutoff_K == 25