"""Оси краула как объявленный вход: ``plan`` видит то же, чем измеряет ``run``.

Три требования, каждое проверено вызовом:

* **ось ``document_timeout_s``.** Потолок извлечения одного документа был зашит
  (``_EXTRACT_TASK_TIMEOUT_S = 1800`` в :mod:`core.pipeline.corpus`). Объявленная ось
  обязана иметь дефолт **равный прежнему поведению** — иначе первый же прогон после
  правки получил бы другой ``run_id`` без изменения измерения — и попадать в
  ``crawl_config_digest`` (``execution_change``), а не в «настройки удобства».
* **``plan`` принимает оси краула и ``--serp-rounds``.** Инвентарь осей один
  (:data:`core.cli.execution_axes.CRAWL_AXIS_DESTS`): если ``run`` и ``plan`` разъедутся,
  напечатанный ``run_id`` описывает не тот прогон, который можно запустить. Стережёт
  ``test_crawl_flag_inventory_matches_run``/``test_serp_flag_matches_run``.
* **``plan`` ничего не пишет.** Проверяется снимком дерева пакета и ``COHOUND_HOME`` до и
  после (состав + mtime) — «ничего не пишет» не остаётся обещанием в докстринге.

Флага ``--dry-run`` у ``plan`` нет: он менял ровно одно поле JSON и намекал на
существование «не-dry» режима (см. `12-cli-architecture-review.md` §2.2).

Офлайн: ``plan`` и ``run --dry-run`` не запускают сеть и не собирают корпус.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from core.cli.execution_axes import CRAWL_AXIS_DESTS, resolve_declared_axes
from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.cli.parser import build_parser
from core.config.deploy import CrawlConfig, DeployConfig
from core.domain.enums import ChangeClass, DocumentLevel, PdfPipeline
from core.domain.models import CrawlRunConfig
from core.pipeline import corpus as corpus_mod
from core.pipeline.crawl_frontier import (
    DEFAULT_DOCUMENT_TIMEOUT_S,
    CrawlLimits,
    resolve_crawl_limits,
)
from core.pipeline.normalize import load_inputs
from core.pipeline.report import crawl_control_digest
from core.pipeline.run_identity import classify_change, golden_required_for
from tests.helpers.packages import valid_package


@pytest.fixture()
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Изолированный ``COHOUND_HOME`` + CWD: деплой-конфиг — дефолтный ``local``."""
    root = tmp_path / "home"
    monkeypatch.setenv("COHOUND_HOME", str(root))
    monkeypatch.chdir(tmp_path)
    return root


def _payload(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])


def _common(package: Path) -> list[str]:
    return [
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
        "--json",
    ]


def _args(package: Path, **overrides: Any) -> Any:
    """``args`` плана на реальном парсере: ``plan`` обязан видеть те же флаги, что ``run``.

    ``None``-дефолты остальных осей держатся парсером — руками ничего не достраиваем,
    иначе тест перестал бы проверять парсер.
    """
    parser = build_parser()
    argv = ["plan", *_common(package)]
    for flag, value in overrides.items():
        argv.extend([flag, *([value] if not isinstance(value, list) else value)])
    return parser.parse_args(argv)


def _declare_run_axes(package: Path, *, crawl: Any = None, serp: Any = None) -> None:
    """Дописывает в фикстурный ``run.yaml`` секции ``crawl``/``serp`` (объявленный вход).

    Фикстурный пакет секций не содержит, поэтому «план читает ``run.yaml``» проверять
    не на чем: без объявленных осей дефолт и объявление неразличимы.
    """
    path = package / "run.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if crawl is not None:
        data["crawl"] = crawl
    if serp is not None:
        data["serp"] = serp
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), "utf-8")


# ---------------------------------------------------------------------------
# 1. Ось document_timeout_s: дефолт = прежнее поведение, задание = execution_change
# ---------------------------------------------------------------------------


def test_document_timeout_default_equals_previous_hardcoded_behaviour() -> None:
    """Дефолт новой оси **равен** зашитому таймауту, который она заменила.

    Сравнение идёт с константой, которая реально исполняется в
    :mod:`core.pipeline.corpus` (``_EXTRACT_TASK_TIMEOUT_S``), а не с литералом в тесте:
    два независимых числа разошлись бы молча, и первый прогон после правки сменил бы
    таймаут (а с ним состав корпуса на медленном источнике) без объявления.
    """
    assert CrawlLimits().document_timeout_s == corpus_mod._EXTRACT_TASK_TIMEOUT_S
    assert DEFAULT_DOCUMENT_TIMEOUT_S == corpus_mod._EXTRACT_TASK_TIMEOUT_S


def test_document_timeout_absent_by_default_in_digest() -> None:
    """Ось не задана ⇒ ключа нет в ``as_dict()`` ⇒ ``run_id`` старых прогонов не меняется."""
    payload = CrawlLimits().as_dict()
    assert "document_timeout_s" not in payload
    # Дефолт при этом виден как значение оси (провенанс), просто не как слагаемое дайджеста.
    assert CrawlLimits().document_timeout_s == DEFAULT_DOCUMENT_TIMEOUT_S


def test_document_timeout_changes_crawl_config_digest() -> None:
    """Задание оси меняет ``crawl_config_digest`` — и класс изменения ``execution_change``.

    Потолок решает, какой медленный документ успел стать корпусом: это условие измерения
    (§13), а не расписание. Поэтому смена — версионная граница ``execution_versions``:
    новый ``run_id``, обязательный golden; ``methodology_version`` не меняется.
    """
    default = crawl_control_digest(
        run_crawl=None, app=CrawlConfig(), profiles=None
    )
    slower = crawl_control_digest(
        run_crawl=None, app=CrawlConfig(document_timeout_s=60.0), profiles=None
    )
    assert default != slower

    limits = resolve_crawl_limits(
        "https://example.org/", app=CrawlConfig(document_timeout_s=60.0)
    )
    assert limits.document_timeout_s == 60.0
    assert limits.as_dict()["document_timeout_s"] == 60.0

    assert (
        classify_change(
            previous_run_id="example-aaaa",
            previous_products=["p1"],
            current_products=["p1"],
            previous_execution_contract_digest=default,
            execution_contract_digest=slower,
        )
        is ChangeClass.EXECUTION_CHANGE
    )
    assert golden_required_for(ChangeClass.EXECUTION_CHANGE) == "full"


def test_document_timeout_resolves_from_app_and_defaults_to_previous_value() -> None:
    """Ось резолвится из app-слоя, а без объявления — прежнее зашитое значение.

    Per-site и run-слой для этой оси пока недостижимы: ни ``DomainProfile``, ни
    ``CrawlRunConfig`` поля не объявляют (файлы вне полосы). Резолвер читает оба через
    ``getattr``, поэтому дальше достаточно добавить поле — вторая формула не нужна.
    Проверяется и это: run-слой без поля app-значение **не** перекрывает.
    """
    app = CrawlConfig(document_timeout_s=600.0)

    resolved = resolve_crawl_limits(
        "https://example.org/", app=app, run=CrawlRunConfig()
    )
    assert resolved.document_timeout_s == 600.0

    # ``None`` на всех слоях — прежнее зашитое значение, а не «нет таймаута».
    plain = resolve_crawl_limits("https://example.org/", app=CrawlConfig())
    assert plain.document_timeout_s == DEFAULT_DOCUMENT_TIMEOUT_S
    assert plain.as_dict().get("document_timeout_s") is None


# ---------------------------------------------------------------------------
# 2. Один инвентарь осей у run и plan
# ---------------------------------------------------------------------------


def _subparser_args(command: str) -> dict[str, Any]:
    """Аргументы подкоманды из собранного парсера (по ``dest``)."""
    parser = build_parser()
    sub = next(
        action
        for action in parser._subparsers._group_actions  # type: ignore[union-attr]
        if hasattr(action, "choices") and action.choices
    )
    return {action.dest: action for action in sub.choices[command]._actions}


def test_crawl_flag_inventory_matches_run() -> None:
    """``plan`` и ``run`` объявляют **одни и те же** оси краула (иначе ``run_id`` врёт)."""
    plan_args = _subparser_args("plan")
    run_args = _subparser_args("run")
    for dest in CRAWL_AXIS_DESTS:
        assert dest in plan_args, f"plan не принимает ось краула {dest}"
        assert dest in run_args, f"run перестал принимать ось краула {dest}"
        assert plan_args[dest].default is None
        assert run_args[dest].default is None


def test_serp_rounds_flag_matches_run() -> None:
    """``--serp-rounds`` у ``plan`` — тот же ``dest``/дефолт, что у ``run``."""
    plan_args = _subparser_args("plan")
    run_args = _subparser_args("run")
    assert plan_args["serp_rounds"].default is None
    assert run_args["serp_rounds"].default is None
    assert "--serp-rounds" in plan_args["serp_rounds"].option_strings
    assert "--serp-rounds" in run_args["serp_rounds"].option_strings


# ---------------------------------------------------------------------------
# 3. plan описывает прогон: тот же run_id, что run, при тех же осях
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "flags",
    [
        [],
        ["--max-depth", "3"],
        ["--max-documents", "42"],
        ["--exclude-document", "*.pdf"],
        ["--exclude-path", "/archive/*"],
        ["--document-level", "text"],
        ["--pdf-pipeline", "text_layer"],
    ],
)
def test_plan_run_id_matches_run(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str], flags: list[str]
) -> None:
    """Тот же вход — тот же ``run_id``: план обязан описывать прогон, а не своё мнение.

    Проверяются оси краула (``CRAWL_AXIS_DESTS``); ``--serp-rounds`` — отдельно ниже,
    потому что ``crawl run`` этого флага пока не объявляет (его ``run_id`` берёт раунды
    из ``run.yaml``/env через тот же резолвер).
    """
    package = valid_package(tmp_path)
    common = _common(package)

    assert main(["run", *common, "--dry-run", *flags]) == EXIT_OK
    run_id = _payload(capsys)["run_id"]

    assert main(["plan", *common, *flags]) == EXIT_OK
    planned = _payload(capsys)
    assert planned["run_id"] == run_id

    # ``crawl run`` — третий вход к той же идентичности.
    assert main(["crawl", "run", *common, "--until", "plan", *flags]) == EXIT_OK
    assert _payload(capsys)["run_id"] == run_id


def test_plan_serp_rounds_run_id_matches_run(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Главное требование: ``plan --serp-rounds N`` печатает ``run_id`` прогона с N.

    Раунды меняют корпус (добавляют документы в ту же сборку), поэтому входят в контракт;
    если бы план считал их иначе, оператор узнал бы свой ``run_id`` только после старта.
    """
    package = valid_package(tmp_path)
    _declare_run_axes(package, serp={"rounds": 0, "budget": 3})
    common = _common(package)

    assert main(["run", *common, "--dry-run", "--serp-rounds", "2"]) == EXIT_OK
    run_id = _payload(capsys)["run_id"]

    assert main(["plan", *common, "--serp-rounds", "2"]) == EXIT_OK
    planned = _payload(capsys)
    assert planned["run_id"] == run_id
    assert planned["serp_rounds"] == 2
    assert planned["serp_source"] == "cli"


def test_plan_digest_is_computed_from_flags_not_defaults(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``plan --max-depth 3`` считает дайджест **по флагу**, а не по ``run.yaml``.

    Ожидание строится независимо: та же чистая функция дайджеста получает run-слой с
    ``max_depth=3`` — так же, как его собирает исполнение (``model_copy`` поверх
    ``run.yaml:crawl``). Совпадение означает, что операторские оси не теряются по дороге.
    """
    package = valid_package(tmp_path)
    _declare_run_axes(package, crawl={"max_depth": 5, "max_documents": 7})
    common = _common(package)

    assert main(["plan", *common]) == EXIT_OK
    declared = _payload(capsys)
    assert main(["plan", *common, "--max-depth", "3"]) == EXIT_OK
    flagged = _payload(capsys)
    assert flagged["run_id"] != declared["run_id"]
    assert flagged["crawl_config_digest"] != declared["crawl_config_digest"]

    inputs = load_inputs(package / "run.yaml", package)
    app = DeployConfig(profile="local").crawl
    profiles = getattr(inputs, "domain_profiles", None)
    # ``run.yaml`` объявляет max_depth 5 — план без флага обязан взять именно его.
    assert declared["crawl_config_digest"] == crawl_control_digest(
        run_crawl=CrawlRunConfig(max_depth=5, max_documents=7),
        app=app,
        profiles=profiles,
    )
    # Флаг перекрывает run-слой (и именно он попадает в дайджест).
    assert flagged["crawl_config_digest"] == crawl_control_digest(
        run_crawl=CrawlRunConfig(max_depth=3, max_documents=7),
        app=app,
        profiles=profiles,
    )


def test_plan_serp_rounds_priority_is_the_declared_one(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Приоритет ``флаг > COHOUND_SERP > run.yaml > deploy.yaml > 0`` — и в плане тоже.

    Env **читается** (троттл контура), поэтому он виден в ``serp_source`` и меняет
    ``run_id``: молчаливая подмена превратила бы «аварийное выключение» в невидимое
    изменение условий измерения.
    """
    package = valid_package(tmp_path)
    _declare_run_axes(package, serp={"rounds": 1, "budget": 3})
    common = _common(package)

    assert main(["plan", *common]) == EXIT_OK
    from_run = _payload(capsys)
    assert from_run["serp_rounds"] == 1
    assert from_run["serp_source"] == "run.yaml"

    assert main(["plan", *common, "--serp-rounds", "0"]) == EXIT_OK
    off = _payload(capsys)
    assert off["serp_rounds"] == 0
    assert off["serp_source"] == "cli"
    assert off["run_id"] != from_run["run_id"]

    # Env — между флагом и run.yaml; ``plan`` обязан назвать уровень-источник.
    monkeypatch_serp = "2"
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("COHOUND_SERP", monkeypatch_serp)
        assert main(["plan", *common]) == EXIT_OK
        from_env = _payload(capsys)
    assert from_env["serp_rounds"] == 2
    assert from_env["serp_source"] == "env"
    assert from_env["run_id"] not in (from_run["run_id"], off["run_id"])


# ---------------------------------------------------------------------------
# 4. plan ничего не пишет
# ---------------------------------------------------------------------------


def _snapshot(*roots: Path) -> dict[str, tuple[int, int]]:
    """Состав дерева + mtime/размер каждого файла (пустой каталог тоже виден)."""
    snapshot: dict[str, tuple[int, int]] = {}
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            stat = path.stat()
            snapshot[str(path)] = (int(stat.st_mtime_ns), stat.st_size)
    return snapshot


def test_plan_writes_nothing(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``plan`` с осями краула не создаёт ни ``runs/``, ни файлов пакета.

    Снимок берётся до и после: «ничего не пишет» — проверяемое свойство, а не обещание
    докстринга (именно это обещание нарушал ``--dry-run``, который ничего не менял).
    """
    package = valid_package(tmp_path)
    before = _snapshot(package, home, tmp_path / "runs")

    assert (
        main(
            [
                "plan",
                *_common(package),
                "--max-depth",
                "2",
                "--exclude-path",
                "/tmp/*",
                "--serp-rounds",
                "0",
            ]
        )
        == EXIT_OK
    )
    _payload(capsys)

    assert _snapshot(package, home, tmp_path / "runs") == before
    assert not (tmp_path / "runs").exists()


def test_plan_has_no_dry_run_flag(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--dry-run`` удалён: план и так ничего не пишет, а флаг обещал «не-dry» режим.

    Отдельная проверка нужна, потому что флаг был не безобидным: он менял ровно одно поле
    JSON (``dry_run``), то есть создавал видимость режима, которого нет.
    """
    package = valid_package(tmp_path)
    # ``main`` возвращает код, а не поднимает ``SystemExit`` на ошибке argparse.
    assert main(["plan", *_common(package), "--dry-run"]) == EXIT_USAGE

    assert main(["plan", *_common(package)]) == EXIT_OK
    assert "dry_run" not in _payload(capsys)


def test_plan_axes_visible_in_declared_axes(
    tmp_path: Path, home: Path
) -> None:
    """Резолвер контракта видит переданные флаги (не только парсер)."""
    package = valid_package(tmp_path)
    inputs = load_inputs(package / "run.yaml", package)
    args = _args(package, **{"--max-depth": "1", "--document-level": "layout"})
    declared = resolve_declared_axes(
        args=args, inputs=inputs, deploy=DeployConfig(profile="local")
    )
    assert declared.crawl_config_digest == crawl_control_digest(
        run_crawl=CrawlRunConfig(
            max_depth=1, document_level=DocumentLevel.LAYOUT
        ),
        app=CrawlConfig(),
        profiles=getattr(inputs, "domain_profiles", None),
    )
    # ``pdf_pipeline`` не задан флагом — наследуется (None на run-слое), не подменяется.
    assert PdfPipeline.STRUCTURED.value == "structured"
