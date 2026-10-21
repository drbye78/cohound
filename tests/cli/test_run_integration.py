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
from core.adapters.index_npy import NpyIndexBuilder, numpy_available
from core.adapters.lexical_tantivy import tantivy_available
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
from tests.fakes.crawl_probe import page
from tests.fakes.ports import (
    FakeChunker,
    FakeCrawler,
    FakeEmbedder,
    FakeEvaluator,
    FakeExtractor,
    FakeIndexBuilder,
    FakeLexicalIndex,
    FakeVectorIndex,
)
from tests.helpers.output import json_out
from tests.helpers.packages import REGISTRY_FIXTURES, run_argv, valid_package


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


_REGISTRY_SPEC = (
    "registry_id: reg_x\n"
    "jurisdiction: XX\n"
    "endpoint: 'https://registry.example.invalid/api/search/{query}'\n"
    "method: GET\n"
    "format: json\n"
    "records_path: items\n"
    "mapping:\n"
    "  name: name\n"
)


def _package_with_registry(tmp_path: Path, name: str = "reg_x") -> Path:
    """Каталог пакета с одной package-scoped спецификацией реестра (E2, 03 §D.4)."""
    pkg = tmp_path / "pkg_registry"
    (pkg / "registries").mkdir(parents=True)
    (pkg / "registries" / f"{name}.yaml").write_text(
        _REGISTRY_SPEC.replace("reg_x", name), encoding="utf-8"
    )
    return pkg


def test_build_adapters_wires_local_registry_fetchers_and_digest(tmp_path: Path) -> None:
    """E2: спецификации реестров → mapping ``Ports.registry_fetchers`` + дайджест (local)."""
    from core.adapters.registry_generic import (
        GenericRegistryFetcher,
        load_package_registry_configs,
    )

    pkg = _package_with_registry(tmp_path)
    specs = load_package_registry_configs(pkg, ["reg_x"])
    ports = build_adapters(
        profile=LOCAL_PROFILE,
        registry_specs=specs,
        registry_http_get=lambda *a, **k: b'{"items": []}',
    )

    assert ports.registry_fetchers is not None
    assert set(ports.registry_fetchers) == {"reg_x"}
    fetcher = ports.registry_fetchers["reg_x"]
    assert isinstance(fetcher, GenericRegistryFetcher)
    assert fetcher.registry_id == "reg_x"
    assert ports.registry_spec_digest


def test_registry_specs_digest_is_canonical_and_change_sensitive(tmp_path: Path) -> None:
    """E2: дайджест стабилен к комментариям, но меняется при смене эффективной спеки."""
    from core.adapters.registry_generic import load_package_registry_configs
    from core.cli.wiring import registry_specs_digest

    pkg = _package_with_registry(tmp_path)
    spec_path = pkg / "registries" / "reg_x.yaml"
    base = registry_specs_digest(load_package_registry_configs(pkg, ["reg_x"]))
    assert base and registry_specs_digest({}) is None

    spec_path.write_text("# комментарий\n" + spec_path.read_text("utf-8"), encoding="utf-8")
    commented = registry_specs_digest(load_package_registry_configs(pkg, ["reg_x"]))
    assert commented == base  # форматирование/комментарии не влияют на исполнение

    spec_path.write_text(
        spec_path.read_text("utf-8").replace("/api/search/", "/api/v2/search/"),
        encoding="utf-8",
    )
    changed = registry_specs_digest(load_package_registry_configs(pkg, ["reg_x"]))
    assert changed != base


def test_build_adapters_wires_split_registry_fetchers_multi(tmp_path: Path) -> None:
    """E2: split — по одному ``RemoteRegistry`` на объявленный реестр (multi-registry)."""
    from core.adapters.remote_gateway import HttpContentGateway
    from core.adapters.remote_registry import RemoteRegistry
    from core.config.deploy import DeployConfig

    pkg = _package_with_registry(tmp_path)
    (pkg / "registries" / "reg_y.yaml").write_text(
        _REGISTRY_SPEC.replace("reg_x", "reg_y"), encoding="utf-8"
    )
    from core.adapters.registry_generic import load_package_registry_configs

    specs = load_package_registry_configs(pkg, ["reg_x", "reg_y"])
    deploy = DeployConfig.model_validate(
        {
            "profile": "split",
            "edge": {"base_url": "https://edge.test:8443", "timeout_s": 5, "max_retries": 0},
            "extractor_zone": "edge",
            "search": {"budget": {"ru": 1}, "adapter": "example_adapter"},
        }
    )
    gateway = HttpContentGateway("https://edge.test", transport=lambda *a, **k: {})
    ports = build_adapters(
        profile=SPLIT_PROFILE, deploy=deploy, gateway=gateway, registry_specs=specs
    )

    assert ports.registry_fetchers is not None
    assert set(ports.registry_fetchers) == {"reg_x", "reg_y"}
    assert all(isinstance(f, RemoteRegistry) for f in ports.registry_fetchers.values())
    assert {f.registry_id for f in ports.registry_fetchers.values()} == {"reg_x", "reg_y"}
    assert ports.registry_spec_digest


def test_load_registry_specs_raises_on_declared_missing(tmp_path: Path) -> None:
    """E2: объявленный реестр без спецификации — понятная ошибка (не молчаливая дыра)."""
    from types import SimpleNamespace

    from core.cli.context import _load_registry_specs
    from core.cli.errors import CliError

    args = SimpleNamespace(package=str(tmp_path / "pkg"))
    inputs = SimpleNamespace(package=SimpleNamespace(registries=["reg_missing"]))
    with pytest.raises(CliError):
        _load_registry_specs(args, inputs)

    # Пакет без объявленных реестров → пусто (реестры опциональны).
    assert _load_registry_specs(args, SimpleNamespace(package=SimpleNamespace(registries=[]))) == {}


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
    # MVP-1: без --urls эффективный набор — seeds.yaml фикстуры (p1/p2).
    assert captured["urls"] == {
        "p1": ["https://p1.test/"],
        "p2": ["https://p2.test/"],
    }
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
    ports = Ports(
        evaluator=evaluator,
        lexical_index=FakeLexicalIndex(),
        **_offline_crawl_kwargs(),
    )

    def factory(**kwargs: Any) -> dict:
        # Фазам нужен фиксированный профиль K (I16/I20); urls/паспорта остаются пустыми.
        kwargs["retrieval_profile"] = "R0"
        kwargs["cutoff_k"] = 3
        # Тест утверждает, что **реальная** фаза evaluate вызвала оценщик. Межпрогонный
        # кэш вердиктов (Ф3) в сессии pytest общий: другой прогон с тем же промптом/
        # конфигурацией даёт хит, и вызов не происходит (ложный «не вызван»). Здесь кэш
        # выключается — проверяется исполнение, а не переиспользование.
        kwargs["eval_cache"] = None
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


def test_cmd_run_wires_package_registry_specs_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D3/E2: реальный CLI ``run`` (без инъекции ``ports=``) с package-scoped спекой реестра.

    Покрывает всю проводку ``_resolve_ports`` → ``_load_registry_specs`` → ``build_adapters``:
    реестровая волна Ш3 производит R-ячейки, а ``registry_spec_digest`` попадает в
    экспортированный ``run_manifest.yaml``. Сеть не используется — транспорт реестра и
    оценщик подменяются детерминированными заглушками (это ортогонально реестровой
    проводке; ``ports=`` намеренно **не** инъектируется — именно его обход и был гэпом D3).
    """
    spec = (REGISTRY_FIXTURES / "reg_x.yaml").read_text(encoding="utf-8")
    package = valid_package(tmp_path, registries={"reg_x": spec})
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "registry-e2e"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)

    from core.cli import context as context_mod
    from core.cli import wiring as wiring_mod

    # MVP-1: seeds.yaml фикстуры делает корпус непустым; сеть запрещена — подменяем
    # только краул/экстракцию, сохраняя реальную реестровую проводку composition root.
    original_build_adapters = context_mod.build_adapters

    def _offline_adapters(**kwargs: Any) -> Any:
        from dataclasses import replace

        return replace(original_build_adapters(**kwargs), **_offline_crawl_kwargs())

    monkeypatch.setattr(context_mod, "build_adapters", _offline_adapters)

    monkeypatch.setattr(
        wiring_mod._RegistryHttpClient,
        "get",
        lambda self, url, headers, timeout=None: b'{"items": [{"name": "value"}]}',
    )
    monkeypatch.setattr(
        context_mod,
        "build_evaluator_llm",
        lambda **kwargs: FakeEvaluator(
            default=LLMCellOutput(
                status=Status.ABSENT, justification="нет в срезе", evidence=[]
            )
        ),
    )

    # Предполёт (реальный composition root: спека реестра загружается уже здесь).
    assert main([*argv, "--until", "plan"]) == EXIT_OK
    with build_store(db) as store:
        for gate_fn in (gate_g1, gate_g2, gate_g3, gate_g4):
            gate_fn(store, run_id, actor="cli-test")

    # Реестровая волна Ш3 в реальном CLI-прогоне: R-ячейки пишутся в store.
    assert main([*argv, "--until", "registry"]) == EXIT_OK
    with build_store(db) as store:
        registry_cells = [
            cell
            for cell in store.iter_cells(run_id)
            if cell.source_kind is not None and cell.source_kind.value == "registry"
        ]
    assert registry_cells
    assert {cell.product_id for cell in registry_cells} == {"p1", "p2"}

    # Полный прогон (resume с Ш3) до Ш9: манифест несёт дайджест спецификаций реестров.
    assert main(argv) == EXIT_OK
    loaded = yaml.safe_load((runs_dir / run_id / "run_manifest.yaml").read_text("utf-8"))
    assert loaded["registry_spec_digest"]
    # 09 §12.5: контракт условий измерения доезжает из composition root (тот же, что у
    # ``run_id``/lineage) до манифеста и строки ``runs`` — не пересчитывается в Ш9.
    assert loaded["execution_contract_digest"]
    with build_store(db) as store:
        assert (
            store.read_manifest(run_id).execution_contract_digest
            == loaded["execution_contract_digest"]
        )


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


def _offline_crawl_kwargs() -> dict[str, Any]:
    """Офлайн-краул фикстурных seed'ов: полный корпус без сети (I19)."""
    pages = {
        url: page(
            url,
            content=b"<html><body><p>fixture documentation text</p></body></html>",
        )
        for url in ("https://p1.test/", "https://p2.test/")
    }
    return {
        "crawler": FakeCrawler(pages),
        "extractor": FakeExtractor(),
        "chunker": FakeChunker(),
    }


def _absent_evaluator_ports() -> Ports:
    return Ports(
        evaluator=FakeEvaluator(
            default=LLMCellOutput(status=Status.ABSENT, justification="нет в срезе", evidence=[])
        ),
        lexical_index=FakeLexicalIndex(),
        **_offline_crawl_kwargs(),
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
        **_offline_crawl_kwargs(),
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


def test_cmd_run_rerun_contract_carries_adopted_retrieval(tmp_path: Path) -> None:
    """Регресс (§13, 2.4.0): контракт на resume несёт усыновлённый ``R2``, а не ``None``.

    Прежний порядок вычислял контракт до усыновления измеренной фиксации: манифест и
    ячейки несли ``R2``, а контракт — ``None`` (ложная граница diff). Теперь контракт
    пересчитывается после нормализации args, но ``run_id`` остаётся идентичностью
    создания прогона (иначе resume не нашёл бы свою строку).
    """
    from types import SimpleNamespace

    from core.cli.execution_axes import (
        declared_status_dict_version,
        resolve_declared_axes,
    )
    from core.config.deploy import load_deploy_config
    from core.pipeline.digests import cohort_digest, seeds_digest
    from core.pipeline.normalize import load_inputs
    from core.pipeline.onboarding import load_effective_seeds
    from core.pipeline.run_identity import build_execution_contract

    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    run_id = "measured-contract"
    argv = run_argv(package, run_id=run_id, db=db, runs_dir=runs_dir)
    assert main([*argv, "--until", "plan"]) == EXIT_OK
    _seed_measured_retrieval(db, run_id, profile="R2", cutoff=7)
    _finalize_gates(db, run_id)

    assert main(argv, ports=_retrieval_ports()) == EXIT_OK

    with build_store(db) as store:
        manifest = store.read_manifest(run_id)

    inputs = load_inputs(package / "run.yaml", package)
    declared = resolve_declared_axes(
        args=SimpleNamespace(package=str(package), llm_slot=None),
        inputs=inputs,
        deploy=load_deploy_config(None),
    )
    seeds, _ = load_effective_seeds(
        package, inputs.run.products, profiles=inputs.domain_profiles or {}
    )
    common = {
        "cohort_digest": cohort_digest(
            inputs.cohort,
            case_id=inputs.run.case_id,
            cohort_ref=getattr(inputs.run, "cohort_ref", None),
        ),
        "seeds_digest": seeds_digest(seeds),
        "declared": declared,
        "status_dict_version": declared_status_dict_version(),
    }
    adopted = build_execution_contract(
        inputs, retrieval_profile="R2", cutoff_K=7, **common
    )
    unadopted = build_execution_contract(
        inputs, retrieval_profile=None, cutoff_K=None, **common
    )
    assert manifest.retrieval_profile == "R2"
    assert manifest.execution_contract_digest == adopted
    assert manifest.execution_contract_digest != unadopted


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

# ---------------------------------------------------------------------------
# MVP-4: ретривал привязан к композиции **этого** корпуса (09 §8.1/§8.4)
# ---------------------------------------------------------------------------

requires_retrieval = pytest.mark.skipif(
    not (numpy_available() and tantivy_available()),
    reason="numpy/tantivy не установлены (extra retrieval)",
)


@requires_retrieval
def test_cmd_run_binds_retrieval_to_corpus_composition_and_reuses_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``run`` поднимает лексический/dense индексы по composition id корпуса.

    Проверяется вся проводка, которой не хватало в живом прогоне: ``_resolve_ports``
    получает ``store``/``package``/``chunk_lookup``, пост-корпусная обёртка пишет
    ``runs.index_build_id`` **до** фазы отчёта и перепривязывает порты к композиции
    этого прогона. Второй прогон по тем же байтам переиспользует партицию: тот же
    ``partition_build_id``, ``partitions_reused == 1`` и ноль вызовов эмбеддера.
    """
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    runs_dir = tmp_path / "runs"
    index_root = tmp_path / "index_store"
    embedders: list[FakeEmbedder] = []
    offline = _offline_crawl_kwargs()

    from core.cli import context as context_mod

    def _offline_adapters(**kwargs: Any) -> Ports:
        """Офлайн-краул + реальные индексы; эмбеддер — детерминированная заглушка."""
        embedder = FakeEmbedder()
        embedders.append(embedder)
        return Ports(
            evaluator=kwargs.get("evaluator"),
            crawler=offline["crawler"],
            extractor=offline["extractor"],
            chunker=offline["chunker"],
            embedder=embedder,
            index_builder=NpyIndexBuilder(
                index_root=kwargs.get("index_root", index_root),
                package=kwargs.get("package"),
            ),
        )

    monkeypatch.setattr(context_mod, "build_adapters", _offline_adapters)
    monkeypatch.setattr(
        context_mod,
        "build_evaluator_llm",
        lambda **_: FakeEvaluator(
            default=LLMCellOutput(
                status=Status.ABSENT, justification="нет в срезе", evidence=[]
            )
        ),
    )

    corpus_results: list[Any] = []
    report_ids: list[str | None] = []
    bound_ports: list[Ports] = []

    def factory(**kwargs: Any) -> dict:
        bound_ports.append(kwargs["ports"])
        phases = build_phases(**kwargs)
        corpus_fn = phases["corpus"]
        report_fn = phases["report"]

        def corpus_wrapped(ctx: Any) -> Any:
            result = corpus_fn(ctx)
            corpus_results.append(result)
            return result

        def report_wrapped(ctx: Any) -> Any:
            # Снимок ``runs.index_build_id`` именно в момент входа в отчёт: доказывает,
            # что пост-корпусная перепривязка сработала раньше Ш9.
            ref = ctx.store.read_run_index_ref(ctx.run_id)
            report_ids.append(ref.index_build_id if ref is not None else None)
            return report_fn(ctx)

        phases["corpus"] = corpus_wrapped
        phases["report"] = report_wrapped
        return phases

    def run_once(run_id: str) -> None:
        argv = run_argv(
            package,
            run_id=run_id,
            db=db,
            runs_dir=runs_dir,
            extra=["--only-product", "p1"],
        )
        assert main([*argv, "--until", "plan"]) == EXIT_OK
        _finalize_gates(db, run_id)
        assert main(argv, phases_factory=factory) == EXIT_OK

    run_once("bind-a")
    first = corpus_results[-1].log_entries[0]
    assert first.partitions_reused == 0
    assert first.entries, "корпус обязан собрать продукт-партицию"
    partition_a = first.entries[0].partition_build_id
    composition_a = first.index_build_id
    assert composition_a.startswith("cb_")
    assert partition_a and partition_a.startswith("pb_")
    assert report_ids[-1] == composition_a, "index_build_id должен быть записан до отчёта"

    run_once("bind-b")
    second = corpus_results[-1].log_entries[0]
    # Переиспользование — предпосылка MVP-4: тот же пакет (``run.package``), те же байты.
    assert second.partitions_reused == 1
    assert second.entries[0].partition_build_id == partition_a
    assert second.index_build_id == composition_a
    assert embedders[-1].calls == [], "переиспользованная партиция не эмбеддится"
    # Порты ретривала перепривязаны к композиции **этого** прогона (не к «единственной
    # на диске»): иначе evaluate не поднял бы R0/dense вовсе.
    bound = bound_ports[-1]
    assert bound.lexical_index is not None
    assert bound.lexical_index.build_id == composition_a
    assert bound.vector_index is not None
    assert bound.vector_index.build_id == composition_a

    with build_store(db) as store:
        ref = store.read_run_index_ref("bind-b")
        cells = list(store.iter_cells("bind-b"))
    assert ref is not None and ref.index_build_id == composition_a
    assert cells, "evaluate должен записать ячейки: лексический индекс поднят"
    # R0-провенанс ячейки — composition id этого прогона (регрессия R0-фикса 6a2c3d5).
    # ``--only-product`` сужает корпус, но план по-прежнему несёт p2: его ячейка честно
    # ``failed`` без ретривала (provenance ``None``), поэтому сверяем оценённый продукт.
    evaluated = [cell for cell in cells if cell.product_id == "p1"]
    assert evaluated
    assert {cell.index_build_id for cell in evaluated} == {composition_a}
