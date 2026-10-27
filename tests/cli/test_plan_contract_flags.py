"""Флаги идентичности: ``plan`` принимает всё, что входит в ``run_id``.

Измеренный дефект (класс, а не случай) найден дважды. Сначала ``run`` передавал
``--retrieval-profile`` и ``--cutoff-k`` в
:func:`core.pipeline.run_identity.build_execution_contract`, а ``plan`` этих флагов не
принимал — значит при явных значениях печатал **другой** ``run_id``, чем будет у прогона.
Затем то же обнаружилось у ``--urls`` (→ ``seeds_digest``), ``--cycle-id``/``--update``
(→ ``cycle_id``), ``--only-failed``/``--only-product`` (→ ``run_scope``) и ``--refresh``
(→ ``refresh_policy``): они меняли ``run_id`` у ``run`` и отсутствовали у ``plan``.
Инструмент, чья единственная задача — предсказать идентичность, предсказывал неверно.

Требования и как они проверены вызовом:

* **один инвентарь.** Флаги порождены :data:`core.cli.execution_axes.CONTRACT_FLAGS`; тест
  читает **собранные** парсеры ``run`` и ``plan`` и сверяет форму (option strings, ``dest``,
  дефолт, тип, ``choices``, ``metavar``, help). Расхождение — падение
  (``test_contract_flag_inventory_matches_run``).
* **читатели инвентаря не отстают.** ``contract_flag_values`` возвращает ровно материал
  ``contract`` под именами параметров контракта, ``identity_flag_values`` — ровно остальные
  ``dest`` (``test_contract_flag_values_names_contract_arguments``,
  ``test_identity_flag_values_cover_inventory``). Новый материал без читателя валит второй.
* **нет необъявленных разрывов.** Полное множество ``dest``, которые есть у ``run`` и нет у
  ``plan``, обязано совпадать с отрецензированным списком: «операционные» (не влияют на
  идентичность) и «известные разрывы» с адресом. Новый флаг ``run`` валит тест, пока его не
  отнесли к одной из групп (``test_run_only_flags_are_reviewed``).
* **тот же ``run_id``.** ``plan`` с флагом печатает тот же ``run_id``, что ``run --dry-run``
  с тем же флагом — для **каждого** флага идентичности
  (``test_plan_run_id_matches_run_with_identity_flags``), и каждое значение действительно
  меняет ``run_id`` (``test_identity_flags_change_run_id``) — иначе паритет был бы паритетом
  нулей.
* **те же отказы.** ``plan`` повторяет отказ прогона на противоречивых входах
  (``--update``+``--only-failed``, ``--only-failed``+``--only-product``): предсказание
  «прогон, который не стартует» — тоже ложь об идентичности
  (``test_plan_repeats_run_refusals``).
* **``deploy_profile`` назван слагаемым.** Он в контракте (``local``/``split`` — разные
  механизмы добычи корпуса) и обязан читаться в payload плана, а не только в хеше
  (``test_plan_names_deploy_profile_and_identity_terms``).
* **``plan`` ничего не пишет** — снимок пакета/``COHOUND_HOME``/``runs/`` до и после, в том
  числе с ``--update``, который читает предыдущий цикл **без записи** в ``state.db``
  (``test_plan_writes_nothing_with_retrieval_flags``,
  ``test_plan_reads_previous_cycle_without_writing_state``).

Офлайн: ``plan`` и ``run --dry-run`` не запускают сеть и не собирают корпус.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import json
from pathlib import Path
from typing import Any

import pytest

from core.cli.execution_axes import (
    CONTRACT_FLAG_DESTS,
    CONTRACT_FLAGS,
    MATERIAL_CONTRACT,
    IdentityFlags,
    contract_flag_values,
    declared_status_dict_version,
    identity_flag_values,
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
#: входят в материал ``run_id``. Список пуст: разрывы закрыты, и это состояние
#: охраняется — новый разрыв обязан появиться здесь с адресом (или флаг обязан попасть
#: в :data:`CONTRACT_FLAGS`, что лучше). Пустая запись — не «забыли», а «разрывов нет».
_KNOWN_IDENTITY_GAPS: dict[str, str] = {}


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


def _urls_file(tmp_path: Path) -> Path:
    """``--urls``-файл фикстурного пакета (операторский вход Ш5).

    Названы **оба** продукта когорты: ``--urls`` заменяет пакетный seed-набор целиком,
    поэтому продукт без URL сделал бы предфлайт ``blocked`` (``no_seeds``) и проверка
    паритета утонула бы в посторонней причине отказа.
    """
    path = tmp_path / "urls.yaml"
    path.write_text(
        json.dumps(
            {
                "p1": ["https://alt.p1.test/"],
                "p2": ["https://alt.p2.test/"],
            }
        ),
        encoding="utf-8",
    )
    return path


def _identity_cases(tmp_path: Path) -> list[tuple[str, list[str]]]:
    """Флаг(и) идентичности и его argv — по одной записи на материал ``run_id``."""
    return [
        ("urls", ["--urls", str(_urls_file(tmp_path))]),
        ("cycle_id", ["--cycle-id", "2026-W44"]),
        ("update", ["--update"]),
        ("only_failed", ["--only-failed"]),
        ("only_product", ["--only-product", "p1"]),
        ("refresh", ["--refresh", "refresh"]),
        ("retrieval", ["--retrieval-profile", "R1", "--cutoff-k", "7"]),
    ]


# ---------------------------------------------------------------------------
# 1. Один инвентарь: форма флагов совпадает у run и plan
# ---------------------------------------------------------------------------


def test_contract_flag_inventory_matches_run() -> None:
    """``plan`` и ``run`` объявляют флаги идентичности **одинаково** (иначе ``run_id`` врёт).

    Сравнивается не только наличие ``dest``: ``choices``/``type``/``metavar``/``action``/
    ``default``/``help`` — тоже вход. Флаг с тем же именем, но другой формой принимал бы
    другое значение (напр. ``--cutoff-k 0`` у ``plan`` и отказ у ``run``), и идентичности
    разъехались бы молча. Булевы флаги сверяются ``nargs``: ``store_true`` — это ``0``
    аргументов, а не «значение по умолчанию».
    """
    plan_args = _subparser_args("plan")
    run_args = _subparser_args("run")
    assert CONTRACT_FLAG_DESTS, "инвентарь флагов идентичности пуст"
    for spec in CONTRACT_FLAGS:
        assert spec.dest in plan_args, f"plan не принимает флаг {spec.dest}"
        assert spec.dest in run_args, f"run перестал принимать флаг {spec.dest}"
        for where, args in (("plan", plan_args), ("run", run_args)):
            action = args[spec.dest]
            assert action.option_strings == [spec.flag], (
                f"{where}: option strings {action.option_strings} != [{spec.flag}]"
            )
            assert action.default == spec.default, (
                f"{where}: дефолт {action.dest} = {action.default!r}, ожидался "
                f"{spec.default!r}"
            )
            assert action.type is spec.type, f"{where}: тип {action.dest} разошёлся"
            assert action.choices == (
                list(spec.choices) if spec.choices is not None else None
            ), f"{where}: choices {action.dest} разошлись"
            assert action.metavar == spec.metavar, f"{where}: metavar {action.dest} разошёлся"
            assert action.help == spec.help, f"{where}: help {action.dest} разошёлся"
            if spec.action == "store_true":
                # ``--update``/``--only-failed`` — переключатели: значение не принимают и
                # по умолчанию выключены (прежнее поведение ≠ третье состояние).
                assert action.nargs == 0, f"{where}: {action.dest} не переключатель"
        # Обе стороны читают один ``dest``: значение, поданное флагу, видно одинаково.
        assert plan_args[spec.dest] is not run_args[spec.dest]


def test_run_only_flags_are_reviewed() -> None:
    """Каждый флаг ``run``, которого нет у ``plan``, отнесён к группе **явно**.

    Это сторож того же класса дефекта с другой стороны: ``test_contract_flag_inventory_matches_run``
    ловит расхождение **известных** флагов, а здесь ловится новый флаг ``run``. Тест падает
    на любом необъявленном ``dest`` и требует решения: «операционный» (в
    ``_OPERATIONAL_RUN_ONLY``) или «входит в идентичность» — тогда ``plan`` его принимает
    (``CONTRACT_FLAGS``) либо он назван в ``_KNOWN_IDENTITY_GAPS`` с адресом.

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
    """Чтение значений контракта — из инвентаря и под именами параметров контракта.

    ``cutoff_k`` (флаг) и ``cutoff_K`` (поле контракта) — разные имена; перепутать их
    значило бы передать в контракт ``None`` при заданном ``--cutoff-k``. Здесь проверяется
    именно это соответствие, а не поведение ``plan`` целиком. Флаги иных материалов
    (``--urls``, ``--update``, …) в этот словарь попадать не должны: их ``dest`` не являются
    параметрами ``build_execution_contract``.
    """
    contract_args = {
        spec.contract_arg
        for spec in CONTRACT_FLAGS
        if spec.material == MATERIAL_CONTRACT
    }
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
            "--urls",
            "u.yaml",
            "--update",
            "--only-failed",
            "--refresh",
            "refresh",
        ]
    )
    assert contract_flag_values(args) == {"retrieval_profile": "R1", "cutoff_K": 7}

    empty = parser.parse_args(["plan", "--run-yaml", "run.yaml", "--package", "pkg"])
    assert contract_flag_values(empty) == {"retrieval_profile": None, "cutoff_K": None}
    assert set(contract_flag_values(empty)) == contract_args


def test_identity_flag_values_cover_inventory() -> None:
    """Читатель неконтрактных флагов знает **каждый** ``dest`` инвентаря.

    ``IdentityFlags(**{spec.dest: ...})`` упал бы на неизвестном поле, но проверять это
    падением в проде — поздно. Здесь состав читателя сверяется с инвентарём, поэтому новый
    флаг идентичности без поля в читателе валит тест, а не ``plan`` у оператора.
    Уникальность ``dest`` проверяется тоже: две записи на один ``dest`` молча перекрыли бы
    друг друга в словаре.
    """
    non_contract = [
        spec.dest for spec in CONTRACT_FLAGS if spec.material != MATERIAL_CONTRACT
    ]
    assert len(non_contract) == len(set(non_contract)), "дубли dest в инвентаре"
    assert {field.name for field in dataclasses.fields(IdentityFlags)} == set(
        non_contract
    )

    parser = build_parser()
    args = parser.parse_args(
        [
            "plan",
            "--run-yaml",
            "run.yaml",
            "--package",
            "pkg",
            "--urls",
            "u.yaml",
            "--cycle-id",
            "2026-W44",
            "--only-product",
            "p1",
            "--refresh",
            "offline",
        ]
    )
    assert identity_flag_values(args) == IdentityFlags(
        urls="u.yaml",
        cycle_id="2026-W44",
        update=False,
        only_failed=False,
        only_product="p1",
        refresh="offline",
    )
    absent = parser.parse_args(["plan", "--run-yaml", "run.yaml", "--package", "pkg"])
    assert identity_flag_values(absent) == IdentityFlags(
        urls=None,
        cycle_id=None,
        update=False,
        only_failed=False,
        only_product=None,
        refresh=None,
    )


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


@pytest.mark.parametrize(
    "case", ["urls", "cycle_id", "update", "only_failed", "only_product", "refresh"]
)
def test_plan_run_id_matches_run_with_identity_flags(
    tmp_path: Path,
    home: Path,
    capsys: pytest.CaptureFixture[str],
    case: str,
) -> None:
    """Остальной материал ``run_id`` тоже предсказывается планом — по одному флагу.

    ``--urls`` (``seeds_digest``), ``--cycle-id``/``--update`` (``cycle_id``),
    ``--only-failed``/``--only-product`` (``run_scope``) и ``--refresh``
    (``refresh_policy``) входят в ``derive_run_id`` (09 §6.2). Пока ``plan`` их не принимал,
    он печатал ``full``/``auto``/пакетный seed-набор при объявленном ином входе — то есть
    ``run_id`` другого прогона.
    """
    package = valid_package(tmp_path)
    common = _common(package)
    flags = dict(_identity_cases(tmp_path))[case]

    assert main(["run", *common, "--dry-run", *flags]) == EXIT_OK
    run_id = _payload(capsys)["run_id"]

    assert main(["plan", *common, *flags]) == EXIT_OK
    assert _payload(capsys)["run_id"] == run_id


def test_identity_flags_change_run_id(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Каждое значение **действительно** входит в ``run_id`` — иначе паритет был бы пустым.

    Если бы обе стороны одинаково игнорировали флаг, паритетный тест проходил бы, а
    инструмент врал бы так же дружно, как раньше. Поэтому: базовый случай и каждый флаг
    идентичности дают **разные** ``run_id`` (семь значений — семь идентичностей).
    """
    package = valid_package(tmp_path)
    common = _common(package)
    ids: dict[str, str] = {}
    cases = [("base", []), *_identity_cases(tmp_path)]
    for name, flags in cases:
        assert main(["plan", *common, *flags]) == EXIT_OK
        ids[name] = _payload(capsys)["run_id"]
    assert len(set(ids.values())) == len(ids), f"идентичности совпали: {ids}"


def test_identity_flags_reported_in_payload(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """План **называет** объявленный материал ``run_id``, а не только хеши.

    ``cycle_id``/``run_scope``/``refresh_policy``/``deploy_profile`` — то, чем измеряют;
    без них «план не увидел флаг» и «флаг не влияет» выглядят одинаково.
    """
    package = valid_package(tmp_path)
    flags = dict(_identity_cases(tmp_path))["only_product"]
    assert main(["plan", *_common(package), *flags, "--cycle-id", "2026-W44"]) == EXIT_OK
    payload = _payload(capsys)
    assert payload["run_scope"] == "product:p1"
    assert payload["cycle_id"] == "2026-W44"
    assert payload["refresh_policy"] == "auto"
    assert payload["deploy_profile"] == "local"


def test_plan_names_deploy_profile_from_deploy_config(
    tmp_path: Path,
    home: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``deploy_profile`` в контракте и в payload: ``local`` и ``split`` — разные измерения.

    Профиль зон не «где запущен процесс», а какими механизмами добыт корпус (в ``split``
    игнорируется ``document_level``, делается двойной фетч, члены контейнеров не
    извлекаются). Поэтому он слагаемое контракта, и план обязан его печатать — иначе
    оператор увидит смену ``run_id`` без причины.
    """
    package = valid_package(tmp_path)
    common = _common(package)

    assert main(["plan", *common]) == EXIT_OK
    local = _payload(capsys)
    assert local["deploy_profile"] == "local"
    assert main(["run", *common, "--dry-run"]) == EXIT_OK
    assert _payload(capsys)["run_id"] == local["run_id"]

    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "deploy.yaml").write_text(
        "profile: split\nedge:\n  base_url: https://edge.test:8443\n", encoding="utf-8"
    )
    assert main(["plan", *common]) == EXIT_OK
    split = _payload(capsys)
    assert split["deploy_profile"] == "split"
    assert split["run_id"] != local["run_id"], "профиль зон не вошёл в run_id"
    assert main(["run", *common, "--dry-run"]) == EXIT_OK
    assert _payload(capsys)["run_id"] == split["run_id"]


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


@pytest.mark.parametrize(
    "flags",
    [
        # 09 §6.2: новый цикл по «только проваленному» подмножеству — неоднозначный work-set.
        ["--update", "--only-failed"],
        # Взаимоисключающий объём: молчаливый приоритет запрещён.
        ["--only-failed", "--only-product", "p1"],
    ],
)
def test_plan_repeats_run_refusals(
    tmp_path: Path, home: Path, flags: list[str]
) -> None:
    """Противоречивый вход отвергается **одинаково**: прогон не стартует, план тоже.

    Предсказать ``run_id`` прогона, который откажется стартовать, — та же ложь об
    идентичности, что и напечатать чужой ``run_id``. Проверка вынесена в одну функцию
    (:func:`core.pipeline.run_identity.check_update_scope`), поэтому отказ не разъедется.
    """
    package = valid_package(tmp_path)
    assert main(["plan", *_common(package), *flags]) == EXIT_USAGE
    assert main(["run", *_common(package), "--dry-run", *flags]) == EXIT_USAGE


def test_plan_reads_previous_cycle_without_writing_state(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``--update`` с прежним циклом отвергается планом, а ``state.db`` не меняется.

    Прогон отвергает ``--update``, если новый цикл равен предыдущему (09 §2.4). Чтобы план
    повторил это, ему нужен предыдущий ``cycle_id`` — и он читается **без записи**
    (``mode=ro``), иначе ``plan`` нарушал бы собственное обещание «ничего не пишет»
    (штатное открытие состояния применяет схему и пишет ``schema_meta``).
    """
    import sqlite3

    package = valid_package(tmp_path)
    today = _dt.date.today().isoformat()
    db_path = home / "state" / package.name / "state.db"
    db_path.parent.mkdir(parents=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE run_lineage ("
        "run_id TEXT PRIMARY KEY, package_id TEXT NOT NULL, cycle_id TEXT NOT NULL,"
        "previous_run_id TEXT, cohort_digest TEXT NOT NULL, seeds_digest TEXT NOT NULL,"
        "execution_contract_digest TEXT NOT NULL, change_class TEXT NOT NULL,"
        "status TEXT NOT NULL, started_at TEXT NOT NULL, completed_at TEXT, error TEXT,"
        "products_json TEXT, refresh_policy TEXT)"
    )
    conn.execute(
        "INSERT INTO run_lineage VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "prev-run",
            package.name,
            today,
            None,
            "c",
            "s",
            "ec",
            "none",
            "complete",
            "2026-01-01T00:00:00+00:00",
            None,
            None,
            None,
            None,
        ),
    )
    conn.commit()
    conn.close()
    before = (db_path.stat().st_mtime_ns, db_path.stat().st_size)

    # Предыдущий цикл равен today() → прогон откажет, значит и план обязан отказать.
    assert main(["plan", *_common(package), "--update"]) == EXIT_USAGE
    after = (db_path.stat().st_mtime_ns, db_path.stat().st_size)
    assert after == before, "plan --update изменил state.db"

    # Тот же отказ у прогона: паритет отказа проверяется отдельно от «не пишет» —
    # ``run`` открывает состояние штатно (с миграцией), и это его право, не плана.
    assert main(["run", *_common(package), "--dry-run", "--update"]) == EXIT_USAGE


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
    """``plan`` с флагами идентичности не создаёт ни ``runs/``, ни файлов пакета.

    Приём новых входов не должен превращать предсказание идентичности в прогон: снимок
    берётся до и после, «ничего не пишет» остаётся проверяемым свойством. Флаги взяты из
    всех материалов сразу — ``--urls`` читает файл, но не корпус.
    """
    package = valid_package(tmp_path)
    before = _snapshot(package, home, tmp_path / "runs")
    urls = _urls_file(tmp_path)

    assert (
        main(
            [
                "plan",
                *_common(package),
                "--retrieval-profile",
                "R2",
                "--cutoff-k",
                "7",
                "--urls",
                str(urls),
                "--update",
                "--refresh",
                "revalidate",
            ]
        )
        == EXIT_OK
    )
    _payload(capsys)

    assert _snapshot(package, home, tmp_path / "runs") == before
    assert not (tmp_path / "runs").exists()
