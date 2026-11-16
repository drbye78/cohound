"""Тесты гейта runbook'а: команды, флаги, переменные, пути, ссылки.

Гейт существует ради одной поломки: команда в документе, которой больше нет. Она молчит
— текст выглядит правильным, а читатель получает `unrecognized arguments`. Поэтому тесты
держат не «функция вернула список», а **класс дефекта**: убрали команду — гейт покраснел.

Проверки идут на временном дереве, а не на настоящем runbook'е: настоящий меняется, и тест,
привязанный к его тексту, ломался бы от правки документа, ничего не проверяя.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "check_runbook.py"


def _load_harness() -> Any:
    spec = importlib.util.spec_from_file_location("_check_runbook", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


RUNBOOK = _load_harness()


@pytest.fixture(scope="module")
def nodes() -> dict[tuple[str, ...], Any]:
    return RUNBOOK._walk(RUNBOOK._parser_tree())


@pytest.fixture(scope="module")
def flags(nodes: dict[tuple[str, ...], Any]) -> set[str]:
    return RUNBOOK._flags(nodes)


def _check(
    tmp_path: Path, text: str, *, nodes: Any, flags: set[str]
) -> tuple[list[str], list[str]]:
    """Проверяет документ во временном дереве с минимальным `.env.example`."""
    root = tmp_path / "repo"
    (root / "core").mkdir(parents=True, exist_ok=True)
    (root / "docs").mkdir(parents=True, exist_ok=True)
    (root / ".env.example").write_text("COHOUND_HOME=\n", encoding="utf-8")
    doc = root / "docs" / "doc.md"
    doc.write_text(text, encoding="utf-8")
    return RUNBOOK.check_document(doc, root=root, nodes=nodes, all_flags=flags)


# --------------------------------------------------------------------- команды


def test_existing_command_passes(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    problems, _ = _check(
        tmp_path, "```bash\n./hound.py doctor\n```\n", nodes=nodes, flags=flags
    )
    assert problems == []


def test_missing_command_is_a_problem(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    """Главный дефект, ради которого гейт существует."""
    problems, _ = _check(
        tmp_path, "```bash\n./hound.py vanish --now\n```\n", nodes=nodes, flags=flags
    )
    assert any("команды нет в CLI" in problem and "vanish" in problem for problem in problems)


def test_nested_command_resolves_to_the_longest_prefix(
    tmp_path: Path, nodes: Any, flags: set[str]
) -> None:
    """`crawl policy vanish` — ошибка, хотя префикс `crawl policy` существует.

    Если бы гейт довольствовался самым длинным **существующим** префиксом, переименование
    подкоманды прошло бы незамеченным: документ обещал бы команду, которой нет.
    """
    problems, _ = _check(
        tmp_path,
        "```bash\n./hound.py crawl policy vanish --package x\n```\n",
        nodes=nodes,
        flags=flags,
    )
    assert any("подкоманды нет у crawl policy" in problem for problem in problems)


def test_positional_after_leaf_command_is_not_a_missing_subcommand(
    tmp_path: Path, nodes: Any, flags: set[str]
) -> None:
    """`crawl status {run_id}` — позиционное значение, а не подкоманда.

    Узел без подкоманд принимает значения; проверка «следующий токен должен быть
    подкомандой» здесь была бы ложной ошибкой на исправном документе.
    """
    problems, _ = _check(
        tmp_path,
        "```bash\n./hound.py crawl status {run_id} --per-host\n```\n",
        nodes=nodes,
        flags=flags,
    )
    assert problems == []


def test_command_outside_a_code_block_is_not_checked(
    tmp_path: Path, nodes: Any, flags: set[str]
) -> None:
    """В прозе команда — это слово, а не приглашение к запуску: не ошибка.

    Но и не тишина: устаревшая команда в таблице (а таблица — инструкция) даёт
    предупреждение, потому что отличить «выполните это» от «эта команда удалена» по
    синтаксису нельзя.
    """
    problems, warnings = _check(
        tmp_path, "Команда `./hound.py vanish` больше не существует.\n", nodes=nodes, flags=flags
    )
    assert problems == []
    assert any("в прозе названа команда" in warning for warning in warnings)


def test_prose_command_placeholder_is_not_warned(
    tmp_path: Path, nodes: Any, flags: set[str]
) -> None:
    """`./hound.py <command> --help` — шаблон, а не команда."""
    problems, warnings = _check(
        tmp_path, "Справка: `./hound.py <command> --help`.\n", nodes=nodes, flags=flags
    )
    assert problems == []
    assert warnings == []


def test_python_dash_m_entry_point_is_recognised(
    tmp_path: Path, nodes: Any, flags: set[str]
) -> None:
    problems, _ = _check(
        tmp_path,
        "```bash\npython3 -m core.cli vanish\n```\n",
        nodes=nodes,
        flags=flags,
    )
    assert any("команды нет в CLI" in problem for problem in problems)


# ----------------------------------------------------------------------- флаги


def test_hound_flag_must_exist(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    problems, _ = _check(
        tmp_path,
        "```bash\n./hound.py plan --run-yaml x --nonsense\n```\n",
        nodes=nodes,
        flags=flags,
    )
    assert any("--nonsense" in problem for problem in problems)


def test_foreign_tool_flag_is_not_our_promise(
    tmp_path: Path, nodes: Any, flags: set[str]
) -> None:
    """`uv sync --frozen` — обещание о `uv`, а не о coHound.

    Без этого исключения гейт требовал бы флага чужой утилиты от нашего дерева и заставлял
    бы документ врать в другую сторону.
    """
    problems, _ = _check(
        tmp_path,
        "```bash\nuv sync --frozen --extra dev\npytest -q -n 3 --dist worksteal\n```\n",
        nodes=nodes,
        flags=flags,
    )
    assert problems == []


def test_flag_in_prose_is_checked(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    """Флаг, названный в прозе в обратных кавычках, — тоже обещание о нашем CLI."""
    problems, _ = _check(
        tmp_path,
        "Модель задаётся флагом `--model-id-that-does-not-exist`.\n",
        nodes=nodes,
        flags=flags,
    )
    assert any("--model-id-that-does-not-exist" in problem for problem in problems)


def test_instrument_flag_is_accepted(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    """Runbook документирует и приборы, у которых свои аргументы.

    `--power-mode` не принадлежит CLI, но принадлежит прибору; требовать его от дерева CLI
    значило бы запретить документу говорить о половине инструментов.
    """
    problems, _ = _check(
        tmp_path,
        "Прибор принимает `--power-mode ac-barrel`.\n",
        nodes=nodes,
        flags=flags | {"--power-mode"},
    )
    assert problems == []


def test_flag_on_a_continuation_line_is_checked(
    tmp_path: Path, nodes: Any, flags: set[str]
) -> None:
    """Флаг на второй строке команды — тоже обещание: строки запуска почти всегда многострочные.

    Проверка «на строке есть слово hound» видит только первую строку и пропускает опечатку в
    `--run-yaml`, стоящем ниже, — то есть именно тот случай, ради которого гейт и нужен.
    """
    problems, _ = _check(
        tmp_path,
        "```bash\n./hound.py plan \\\n  --run-yamll x \\\n```\n",
        nodes=nodes,
        flags=flags,
    )
    assert any("--run-yamll" in problem for problem in problems)


def test_continuation_ends_at_the_next_line(
    tmp_path: Path, nodes: Any, flags: set[str]
) -> None:
    """Без ``\\`` команда кончилась: флаг следующей строки — уже не наш. Иначе гейт
    требовал бы флагов от чужих команд, стоящих сразу после нашей."""
    problems, _ = _check(
        tmp_path,
        "```bash\n./hound.py doctor\nuv sync --frozen --extra dev\n```\n",
        nodes=nodes,
        flags=flags,
    )
    assert problems == []


def test_help_is_a_legitimate_flag(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    """`hound.py package --help` разрешается в дереве и должен проходить проверку."""
    problems, _ = _check(
        tmp_path,
        "```bash\n./hound.py package --help\n```\n",
        nodes=nodes,
        flags=flags,
    )
    assert problems == []


# ------------------------------------------------------- переменные и пути


def test_declared_env_var_passes(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    problems, _ = _check(
        tmp_path, "Задайте `COHOUND_HOME`.\n", nodes=nodes, flags=flags
    )
    assert problems == []


def test_unknown_env_var_is_a_problem(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    problems, _ = _check(
        tmp_path, "Задайте `COHOUND_NOPE`.\n", nodes=nodes, flags=flags
    )
    assert any("COHOUND_NOPE" in problem for problem in problems)


def test_env_var_prefix_is_not_a_declaration(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    """Опечатка-префикс не подтверждается сама собой.

    `COHOUND_PATHS_FIL` — префикс `COHOUND_PATHS_FILE`. При поиске подстрокой гейт находил
    бы его в собственном имени и подтверждал опечатку; совпадение должно быть по границе.
    """
    root = tmp_path / "repo"
    (root / "core").mkdir(parents=True, exist_ok=True)
    (root / "docs").mkdir(parents=True, exist_ok=True)
    (root / "core" / "paths.py").write_text(
        'ENV = "COHOUND_PATHS_FILE"\n', encoding="utf-8"
    )
    (root / ".env.example").write_text("COHOUND_HOME=\n", encoding="utf-8")
    doc = root / "docs" / "doc.md"
    doc.write_text("Задайте `COHOUND_PATHS_FIL`.\n", encoding="utf-8")
    problems, _ = RUNBOOK.check_document(doc, root=root, nodes=nodes, all_flags=flags)
    assert any("COHOUND_PATHS_FIL" in problem for problem in problems)


def test_missing_repo_path_is_a_problem(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    problems, _ = _check(
        tmp_path, "Файл `core/gone.py` не существует.\n", nodes=nodes, flags=flags
    )
    assert any("core/gone.py" in problem for problem in problems)


def test_existing_repo_path_passes(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    problems, _ = _check(
        tmp_path, "Файл `core/x.py` есть.\n", nodes=nodes, flags=flags
    )
    root = tmp_path / "repo"
    (root / "core" / "x.py").write_text("", encoding="utf-8")
    problems, _ = _check(tmp_path, "Файл `core/x.py` есть.\n", nodes=nodes, flags=flags)
    assert problems == []


def test_template_path_is_a_warning_not_a_problem(
    tmp_path: Path, nodes: Any, flags: set[str]
) -> None:
    """`packages/{case_id}` — шаблон: существованием он не проверяется."""
    problems, warnings = _check(
        tmp_path, "Пакет `packages/{case_id}/run.yaml`.\n", nodes=nodes, flags=flags
    )
    assert problems == []
    assert any("шаблон" in warning for warning in warnings)


def test_data_dir_absence_is_a_warning(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    """`runs/` — артефакт (03 §A.1.2): его отсутствие в дереве нормально.

    Требовать его существования значило бы требовать закоммитить данные, то есть ровно то,
    что запрещает `.gitignore`.
    """
    problems, warnings = _check(
        tmp_path, "Прогон живёт в `runs/`.\n", nodes=nodes, flags=flags
    )
    assert problems == []
    assert any("артефакт" in warning for warning in warnings)


def test_broken_internal_link_is_a_problem(
    tmp_path: Path, nodes: Any, flags: set[str]
) -> None:
    problems, _ = _check(
        tmp_path, "См. [`нет такого`](gone.md).\n", nodes=nodes, flags=flags
    )
    assert any("ссылка не разрешается" in problem for problem in problems)


def test_external_link_is_not_checked(tmp_path: Path, nodes: Any, flags: set[str]) -> None:
    problems, _ = _check(
        tmp_path, "См. [`регламент`](https://example.org/x).\n", nodes=nodes, flags=flags
    )
    assert problems == []


# ------------------------------------------------------------------- дерево CLI


def test_parser_tree_is_read_from_code_not_from_a_list(nodes: Any) -> None:
    """Дерево читается из argparse, поэтому новая команда появляется в гейте сама.

    Тест держит именно свойство «источник — код»: если гейт когда-нибудь начнёт сверяться
    со списком имён, эта проверка на живом дереве упадёт.
    """
    assert ("plan",) in nodes
    assert ("crawl", "policy", "set") in nodes
    assert len(nodes) > 50


def test_script_flags_are_collected_from_the_scripts(tmp_path: Path) -> None:
    """Флаги приборов берутся из их `add_argument`, а не из списка в гейте."""
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    (root / "scripts" / "measure_x.py").write_text(
        'parser.add_argument("--limit-chunks", type=int)\n'
        "parser.add_argument('--power-mode', default='unknown')\n",
        encoding="utf-8",
    )
    found = RUNBOOK._script_flags(root)
    assert found == {"--limit-chunks", "--power-mode"}


def test_default_documents_all_exist() -> None:
    """Документы по умолчанию существуют: иначе гейт в CI проверял бы пустоту."""
    root = Path(__file__).resolve().parents[2]
    missing = [name for name in RUNBOOK.DEFAULT_DOCS if not (root / name).is_file()]
    assert missing == []
