"""Контрактные флаги ретривала: ``plan`` принимает всё, что входит в ``run_id``.

Измеренный дефект (класс, а не случай): ``run`` передавал ``--retrieval-profile`` и
``--cutoff-k`` в :func:`core.pipeline.run_identity.build_execution_contract`, а ``plan`` этих
флагов не принимал — значит при явных значениях печатал **другой** ``run_id``, чем будет у
прогона. Инструмент, чья единственная задача — предсказать идентичность, предсказывал
неверно.

Требования и как они проверены вызовом:

* **один инвентарь.** Флаги порождены :data:`core.cli.execution_axes.CONTRACT_FLAGS`; тест
  читает **собранные** парсеры ``run`` и ``plan`` и сверяет форму (option strings, ``dest``,
  дефолт, тип, ``choices``, ``metavar``, help). Расхождение — падение
  (``test_contract_flag_inventory_matches_run``).
* **нет необъявленных разрывов.** Полное множество ``dest``, которые есть у ``run`` и нет у
  ``plan``, обязано совпадать с отрецензированным списком: «операционные» (не влияют на
  идентичность) и «известные разрывы» с адресом. Новый флаг ``run`` валит тест, пока его не
  отнесли к одной из групп (``test_run_only_flags_are_reviewed``).
* **тот же ``run_id``.** ``plan --retrieval-profile X --cutoff-k N`` == ``run --dry-run`` с
  теми же флагами (``test_plan_run_id_matches_run_with_retrieval_flags``).
* **отсутствие флагов ничего не меняет.** Без флагов контракт обязан содержать ``null`` в
  обоих полях — иначе «не объявлено» тихо превращалось бы в дефолт Ш6
  (``test_plan_absent_flags_do_not_change_contract``).
* **``plan`` ничего не пишет** — снимок пакета/``COHOUND_HOME``/``runs/`` до и после.

Офлайн: ``plan`` и ``run --dry-run`` не запускают сеть и не собирают корпус.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from core.cli.execution_axes import (
    CONTRACT_FLAG_DESTS,
    CONTRACT_FLAGS,
    contract_flag_values,
    declared_status_dict_version,
    resolve_declared_axes,
)
from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.cli.parser import build_parser
from core.config.deploy import DeployConfig
from core.pipeline.digests import cohort_digest, seeds_digest
from core.pipeline.normalize import load_inputs
from core.pipeline.onboarding import load_effective_seeds
from core.pipeline.run_identity import build_execution_contract
from tests.helpers.packages import valid_package

#: ``dest``, которые есть у ``run`` и нет у ``plan``, **и это законно**: они не входят в
#: ``run_id`` (адресация, исполнение, прогресс), поэтому предсказывать их плану нечем.
_OPERATIONAL_RUN_ONLY: frozenset[str] = frozenset(
    {
        "db",  # путь БД прогона
        "dry_run",  # режим вывода
        "follow",  # прогресс сеанса
        "from_scratch",  # обход R7-кэша
        "interval",  # период прогресса
        "max_attempts",  # потолок попыток по URL
        "passports_dir",  # каталог паспортов (вход корпуса, не контракта)
        "render_modes",  # per-URL режим рендера
        "reset",  # форсировать перезапуск
        "runs_dir",  # каталог прогонов
        # --since-run адресует predecessor для carry-forward/lineage: материал run_id
        # (cycle_id/scope/refresh/дайджесты) он не меняет.
        "since_run",
        "until",  # последняя фаза
    }
)

#: ``dest``, которые есть у ``run`` и нет у ``plan``, **и это известный разрыв**: они
#: входят в материал ``run_id``. Здесь они названы, а не забыты: закрытие разрыва обязано
#: убрать запись (иначе тест упадёт), а новый разрыв — добавить.
_KNOWN_IDENTITY_GAPS: dict[str, str] = {
    # --urls полностью заменяет seeds.yaml → seeds_digest (поле контракта!) → run_id.
    "urls": "core/cli/commands/run.py:477-486 (seeds_override → seeds_digest)",
    # --update/--cycle-id меняют cycle_id, а он — материал derive_run_id.
    "update": "core/cli/commands/run.py:462-471 (resolve_cycle_id)",
    "cycle_id": "core/cli/commands/run.py:462-471 (resolve_cycle_id)",
    # --refresh меняет refresh_policy, а она — материал derive_run_id.
    "refresh": "core/cli/commands/run.py:443-445 (resolve_refresh_policy)",
    # --only-failed/--only-product меняют run_scope — материал derive_run_id.
    "only_failed": "core/cli/commands/run.py:439-442 (resolve_run_scope)",
    "only_product": "core/cli/commands/run.py:439-442 (resolve_run_scope)",
}


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
    return ["--run-yaml", str(package / "run.yaml"), "--package", str(package), "--json"]


def _subparser_args(command: str) -> dict[str, Any]:
    """Аргументы подкоманды из собранного парсера (по ``dest``)."""
    parser = build_parser()
    sub = next(
        action
        for action in parser._subparsers._group_actions  # type: ignore[union-attr]
        if hasattr(action, "choices") and action.choices
    )
    return {action.dest: action for action in sub.choices[command]._actions}


# ---------------------------------------------------------------------------
# 1. Один инвентарь: форма флагов совпадает у run и plan
# ---------------------------------------------------------------------------


def test_contract_flag_inventory_matches_run() -> None:
    """``plan`` и ``run`` объявляют контрактные флаги **одинаково** (иначе ``run_id`` врёт).

    Сравнивается не только наличие ``dest``: ``choices``/``type``/``metavar``/``help`` —
    тоже вход. Флаг с тем же именем, но другой формой принимал бы другое значение (напр.
    ``--cutoff-k 0`` у ``plan`` и отказ у ``run``), и идентичности разъехались бы молча.
    """
    plan_args = _subparser_args("plan")
    run_args = _subparser_args("run")
    assert CONTRACT_FLAG_DESTS, "инвентарь контрактных флагов пуст"
    for spec in CONTRACT_FLAGS:
        assert spec.dest in plan_args, f"plan не принимает контрактный флаг {spec.dest}"
        assert spec.dest in run_args, f"run перестал принимать контрактный флаг {spec.dest}"
        for where, args in (("plan", plan_args), ("run", run_args)):
            action = args[spec.dest]
            assert action.option_strings == [spec.flag], (
                f"{where}: option strings {action.option_strings} != [{spec.flag}]"
            )
            assert action.default is None, f"{where}: дефолт {action.dest} не None"
            assert action.type is spec.type, f"{where}: тип {action.dest} разошёлся"
            assert action.choices == (
                list(spec.choices) if spec.choices is not None else None
            ), f"{where}: choices {action.dest} разошлись"
            assert action.metavar == spec.metavar, f"{where}: metavar {action.dest} разошёлся"
            assert action.help == spec.help, f"{where}: help {action.dest} разошёлся"
        # Обе стороны читают один ``dest``: значение, поданное флагу, видно одинаково.
        assert plan_args[spec.dest] is not run_args[spec.dest]


def test_run_only_flags_are_reviewed() -> None:
    """Каждый флаг ``run``, которого нет у ``plan``, отнесён к группе **явно**.

    Это сторож того же класса дефекта с другой стороны: ``test_contract_flag_inventory_matches_run``
    ловит расхождение **известных** флагов, а здесь ловится новый флаг ``run``. Тест падает
    на любом необъявленном ``dest`` и требует решения: «операционный» (в
    ``_OPERATIONAL_RUN_ONLY``) или «входит в идентичность» — тогда либо ``plan`` его
    принимает, либо он назван в ``_KNOWN_IDENTITY_GAPS`` с адресом.

    ``plan``-only флагов быть не должно: план не может принимать вход, которого прогон не
    знает, — такой флаг описывал бы прогон, который нельзя запустить.
    """
    plan_dests = set(_subparser_args("plan"))
    run_dests = set(_subparser_args("run"))
    run_only = run_dests - plan_dests
    reviewed = set(_OPERATIONAL_RUN_ONLY) | set(_KNOWN_IDENTITY_GAPS)
    unreviewed = sorted(run_only - reviewed)
    assert not unreviewed, (
        "новые флаги run без решения об идентичности: "
        f"{unreviewed}. Если флаг не входит в run_id — внесите в _OPERATIONAL_RUN_ONLY; "
        "если входит — plan обязан его принимать (CONTRACT_FLAGS) либо назвать в "
        "_KNOWN_IDENTITY_GAPS с file:line"
    )
    stale = sorted(reviewed - run_only)
    assert not stale, (
        f"записи обзора устарели (флаг больше не run-only): {stale}. Если plan теперь "
        "принимает флаг — удалите запись из _KNOWN_IDENTITY_GAPS"
    )
    assert not (plan_dests - run_dests), f"plan принимает флаги, которых нет у run: {sorted(plan_dests - run_dests)}"


def test_contract_flag_values_names_contract_arguments() -> None:
    """Чтение значений — из того же инвентаря и под именами параметров контракта.

    ``cutoff_k`` (флаг) и ``cutoff_K`` (поле контракта) — разные имена; перепутать их
    значило бы передать в контракт ``None`` при заданном ``--cutoff-k``. Здесь проверяется
    именно это соответствие, а не поведение ``plan`` целиком.
    """
    parser = build_parser()
    args = parser.parse_args(
        [
            "plan",
            "--run-yaml",
            "run.yaml",
            "--package",
            "pkg",
            "--retrieval-profile",
            "R1",
            "--cutoff-k",
            "7",
        ]
    )
    assert contract_flag_values(args) == {"retrieval_profile": "R1", "cutoff_K": 7}

    empty = parser.parse_args(["plan", "--run-yaml", "run.yaml", "--package", "pkg"])
    assert contract_flag_values(empty) == {"retrieval_profile": None, "cutoff_K": None}
    assert set(contract_flag_values(empty)) == {spec.contract_arg for spec in CONTRACT_FLAGS}


# ---------------------------------------------------------------------------
# 2. Тот же run_id: plan описывает прогон, а не своё мнение
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "flags, expected_profile, expected_cutoff",
    [
        ([], None, None),
        (["--retrieval-profile", "R1"], "R1", None),
        (["--cutoff-k", "7"], None, 7),
        (["--retrieval-profile", "R2", "--cutoff-k", "3"], "R2", 3),
    ],
)
def test_plan_run_id_matches_run_with_retrieval_flags(
    tmp_path: Path,
    home: Path,
    capsys: pytest.CaptureFixture[str],
    flags: list[str],
    expected_profile: str | None,
    expected_cutoff: int | None,
) -> None:
    """Главное требование: ``plan`` с флагами печатает ``run_id`` прогона с теми же флагами.

    ``plan`` обязан назвать и сами значения (``retrieval_profile``/``cutoff_K``), а не
    только хеш: «чем измеряем» должно читаться без разбора дайджеста. ``None`` — флаг не
    объявлен, и план не подставляет дефолт Ш6.
    """
    package = valid_package(tmp_path)
    common = _common(package)

    assert main(["run", *common, "--dry-run", *flags]) == EXIT_OK
    run_id = _payload(capsys)["run_id"]

    assert main(["plan", *common, *flags]) == EXIT_OK
    planned = _payload(capsys)
    assert planned["run_id"] == run_id
    assert planned["retrieval_profile"] == expected_profile
    assert planned["cutoff_K"] == expected_cutoff


def test_plan_retrieval_flags_change_run_id(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Флаги **действительно** входят в ``run_id``: иначе паритет был бы паритетом нулей.

    Если бы обе стороны одинаково игнорировали флаги, предыдущий тест проходил бы, а
    инструмент врал бы так же дружно, как раньше. Поэтому: каждое значение (и их пара) даёт
    свой ``run_id``, отличный от «не объявлено».
    """
    package = valid_package(tmp_path)
    common = _common(package)
    ids: set[str] = set()
    for flags in (
        [],
        ["--retrieval-profile", "R1"],
        ["--retrieval-profile", "R2"],
        ["--cutoff-k", "7"],
        ["--retrieval-profile", "R1", "--cutoff-k", "7"],
    ):
        assert main(["plan", *common, *flags]) == EXIT_OK
        ids.add(_payload(capsys)["run_id"])
    assert len(ids) == 5


def test_plan_absent_flags_do_not_change_contract(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Без флагов контракт считает **``null``** в обоих полях — прежнее поведение.

    Ожидание строится независимо: та же чистая функция контракта вызывается с явными
    ``retrieval_profile=None``/``cutoff_K=None``. Это и есть «``run_id`` прежний»: правка
    добавила приём флагов, а не значение по умолчанию (дефолт Ш6 в контракт не едет —
    ``run_id`` описывает объявленное).
    """
    package = valid_package(tmp_path)
    common = _common(package)
    for _ in range(2):
        assert main(["plan", *common]) == EXIT_OK
        payload = _payload(capsys)
        assert payload["retrieval_profile"] is None
        assert payload["cutoff_K"] is None

    inputs = load_inputs(package / "run.yaml", package)
    profiles = getattr(inputs, "domain_profiles", None) or {}
    seed_config, _ = load_effective_seeds(
        package, inputs.run.products, profiles=profiles
    )
    args = build_parser().parse_args(["plan", *common])
    expected = build_execution_contract(
        inputs,
        cohort_digest=cohort_digest(
            inputs.cohort,
            case_id=inputs.run.case_id,
            cohort_ref=getattr(inputs.run, "cohort_ref", None),
        ),
        seeds_digest=seeds_digest(seed_config),
        declared=resolve_declared_axes(
            args=args, inputs=inputs, deploy=DeployConfig(profile="local")
        ),
        status_dict_version=declared_status_dict_version(),
        retrieval_profile=None,
        cutoff_K=None,
    )
    assert payload["execution_contract_digest"] == expected


@pytest.mark.parametrize(
    "flags",
    [
        ["--retrieval-profile", "R9"],  # вне choices R0|R1|R2
        ["--cutoff-k", "0"],  # _positive_int: K > 0 (I20)
        ["--cutoff-k", "-3"],
    ],
)
def test_plan_rejects_invalid_retrieval_values(
    tmp_path: Path, home: Path, flags: list[str]
) -> None:
    """Недопустимое значение отвергается и планом — той же формой флага, что у ``run``.

    Иначе ``plan`` принял бы значение, с которым прогон не стартует, и напечатал бы
    ``run_id`` несуществующего прогона.
    """
    package = valid_package(tmp_path)
    assert main(["plan", *_common(package), *flags]) == EXIT_USAGE
    assert main(["run", *_common(package), "--dry-run", *flags]) == EXIT_USAGE


# ---------------------------------------------------------------------------
# 3. plan ничего не пишет
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


def test_plan_writes_nothing_with_retrieval_flags(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``plan`` с контрактными флагами не создаёт ни ``runs/``, ни файлов пакета.

    Приём нового входа не должен превращать предсказание идентичности в прогон: снимок
    берётся до и после, «ничего не пишет» остаётся проверяемым свойством.
    """
    package = valid_package(tmp_path)
    before = _snapshot(package, home, tmp_path / "runs")

    assert (
        main(
            [
                "plan",
                *_common(package),
                "--retrieval-profile",
                "R2",
                "--cutoff-k",
                "7",
            ]
        )
        == EXIT_OK
    )
    _payload(capsys)

    assert _snapshot(package, home, tmp_path / "runs") == before
    assert not (tmp_path / "runs").exists()
