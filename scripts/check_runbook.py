#!/usr/bin/env python3
"""Проверка runbook'а: команды, флаги, переменные и пути — против кода, а не на глаз.

Runbook — документ, который **читают, чтобы действовать**: скопировал команду, нажал
Enter. Поэтому его самая частая поломка не «устаревший абзац», а команда, которой больше
нет, флаг, которого не существует, и путь, который переехал. Такая поломка молчит: текст
выглядит правильным, а читатель получает `error: unrecognized arguments` и не знает,
виноват он или документация.

Что проверяется (по коду, не по списку известных имён):

1. **Команды.** Каждый `./hound.py <команда> [<подкоманда>]` в блоках кода разрешается в
   живом argparse-дереве (:func:`core.cli.parser.build_parser`). Дерево читается, а не
   перечисляется: перечисление в гейте разошлось бы с CLI ровно так же, как документ.
2. **Флаги.** Каждый `--flag` разрешается хотя бы в одной команде дерева. Проверка
   «где-нибудь существует» намеренно слабая: привязка флага к конкретной команде требует
   разбора всей строки, а ошибка «флаг есть, но не у этой команды» ловится копированием,
   не гейтом. Слабость названа, а не замаскирована.
3. **Переменные окружения.** Каждый `COHOUND_*` объявлен в `.env.example` **или**
   встречается в `core/`. Второй источник нужен потому, что часть переменных — тестовые и
   служебные (`COHOUND_NO_DOTENV`, `COHOUND_CI_WORKERS`) и в пример окружения не входят.
4. **Пути репозитория.** Каждый путь в обратных кавычках с корнем из
   :data:`REPO_ROOTS` существует. Путь с `*`, `<`, `{`, `…` — шаблон, пропускается с
   предупреждением: шаблон нельзя проверить существованием.
5. **Внутренние ссылки markdown.** `[текст](путь)` разрешается относительно документа.

Чего гейт НЕ проверяет, и это важнее списка проверок: **смысл**. Он не знает, что
`--until report` останавливается после `report`, а не перед ним, и что «сначала
`fetch_ocr_models.py`» стоит до первого `run`. Это читает человек; гейт держит только то,
что можно проверить машиной, — чтобы человек читал смысл, а не искал опечатку.

Код возврата: ``1`` — есть проблемы и задан ``--strict``, ``2`` — файла runbook нет,
``0`` — иначе. Предупреждения код возврата не меняют.

Запуск::

    .venv/bin/python scripts/check_runbook.py
    .venv/bin/python scripts/check_runbook.py --strict
    .venv/bin/python scripts/check_runbook.py --docs docs/user-guide/runbook.md
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any

#: Корни, по которым опознаётся «путь репозитория» в обратных кавычках. Список узкий
#: намеренно: широкий превратил бы в «путь» любое слово с косой чертой (`и/или`,
#: `HTTP/JSON`) и дал бы ложные ошибки на исправном документе.
REPO_ROOTS: tuple[str, ...] = (
    "core",
    "docs",
    "scripts",
    "tests",
    "packages",
    "passports",
    "contracts",
    "config",
    "runs",
    "state",
)

#: Корни, которых в дереве может не быть **по замыслу**: это артефакты (03 §A.1.2), на
#: настроенном хосте они живут вне репозитория. Их отсутствие — предупреждение, не ошибка:
#: иначе гейт требовал бы закоммитить `runs/`, то есть ровно то, что `.gitignore` запрещает.
DATA_ROOTS: frozenset[str] = frozenset(
    {"runs", "state", "index_store", "corpus_raw", "cache", "extract_cache", "eval_cache",
     "embed_cache", "reports"}
)


#: Документы по умолчанию: runbook и README. `docs/user-guide/*.md` добавлены целиком —
#: пользовательские руководства ломаются теми же командами, что и runbook.
DEFAULT_DOCS: tuple[str, ...] = (
    "docs/user-guide/runbook.md",
    "docs/user-guide/package-profile.md",
    "docs/user-guide/package-files.md",
    "README.md",
)

#: Строка запуска CLI внутри блока кода.
_COMMAND_LINE = re.compile(
    r"^\s*(?:\./hound\.py|hound\.py|python3?\s+-m\s+core\.cli)\s+(?P<rest>\S.*)$"
)
#: Флаг в любом месте документа (в блоке кода или в тексте).
_FLAG = re.compile(r"(?<![\w-])(--[a-z][a-z0-9-]*)")

#: Инструменты, у которых **свои** флаги. Флаг на такой строке — обещание о них, а не о
#: coHound: `uv sync --frozen`, `pytest --dist`, `ruff check --fix`. Список закрытый и
#: короткий: он отделяет «чужая команда» от «наша проза про наш флаг», а не классифицирует
#: всё подряд.
FOREIGN_TOOLS: frozenset[str] = frozenset(
    {"uv", "pytest", "ruff", "mypy", "python", "python3", "bash", "git", "rg", "fdfind",
     "fd", "yq", "jq", "litecli", "gh", "py-spy", "difft", "delta", "bat", "sd", "ast-grep",
     "curl", "nvidia-smi", "docker", "make"}
)
#: Переменная окружения проекта.
_ENV_VAR = re.compile(r"\b(COHOUND_[A-Z0-9_]+)\b")
#: Путь репозитория в обратных кавычках.
_REPO_PATH = re.compile(r"`([^`\s]+)`")
#: Ссылка markdown на локальный файл.
_MD_LINK = re.compile(r"\[[^\]]*\]\((?P<target>[^)\s]+)\)")
#: Признак шаблона, а не конкретного пути.
_PLACEHOLDER = re.compile(r"[*<>{}$…]|\.\.\.|\\")


def _repo_root() -> Path:
    """Корень репозитория — от расположения скрипта, а не от текущего каталога."""
    return Path(__file__).resolve().parents[1]


def _fenced_blocks(text: str) -> list[list[str]]:
    """Строки внутри ```-блоков; открывающая и закрывающая черта не входят.

    Проверять команды по всему тексту нельзя: в прозе встречается «`run` печатает», и
    это не команда для запуска. Блок кода — ровно то, что читатель копирует.
    """
    blocks: list[list[str]] = []
    current: list[str] | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("```"):
            if current is None:
                current = []
            else:
                blocks.append(current)
                current = None
            continue
        if current is not None:
            current.append(line)
    if current is not None:  # незакрытый блок — не ошибка гейта, но команды внутри видны
        blocks.append(current)
    return blocks


def _parser_tree() -> Any:
    """Живое argparse-дерево CLI.

    Импорт ленивый: гейт должен работать и тогда, когда тяжёлые extras не установлены
    (сборка парсера их не трогает, но импорт `core.cli` тянет контекст).
    """
    from core.cli.parser import build_parser

    return build_parser()


def _walk(parser: Any, path: tuple[str, ...] = ()) -> dict[tuple[str, ...], Any]:
    """Все узлы дерева: путь из имён команд → подпарсер."""
    nodes: dict[tuple[str, ...], Any] = {path: parser}
    for action in parser._actions:  # noqa: SLF001 — публичного обхода у argparse нет
        if not isinstance(action, argparse._SubParsersAction):  # noqa: SLF001
            continue
        for name, child in action.choices.items():
            nodes.update(_walk(child, (*path, name)))
    return nodes


def _flags(nodes: dict[tuple[str, ...], Any]) -> set[str]:
    """Все флаги дерева (`--foo`), без `-h`: он есть у каждой команды и не информативен."""
    found: set[str] = set()
    for parser in nodes.values():
        for action in parser._actions:  # noqa: SLF001
            found.update(option for option in action.option_strings if option.startswith("--"))
    # `--help` не выбрасывается: документ вправе обещать `hound.py package --help`, и это
    # разрешается в дереве — значит проверка должна это подтвердить, а не обойти.
    return found


def _command_path(rest: str) -> tuple[str, ...]:
    """Путь команды из строки запуска: слова до первого флага/значения.

    Хвост вида `{case_id}` или `--run-yaml` не команда. Учитываются только слова, которые
    есть в дереве: `./hound.py plan --run-yaml x` → (`plan`,), а `./hound.py crawl policy
    set ...` → (`crawl`, `policy`, `set`).
    """
    words = [word for word in rest.split() if not word.startswith("-")]
    return tuple(words)


def _subcommands(parser: Any) -> set[str]:
    """Имена подкоманд узла; пусто, если у узла подкоманд нет (значит дальше — позиционные)."""
    for action in parser._actions:  # noqa: SLF001 — публичного обхода у argparse нет
        if isinstance(action, argparse._SubParsersAction):  # noqa: SLF001
            return set(action.choices)
    return set()


def _resolve_command(
    tokens: tuple[str, ...], nodes: dict[tuple[str, ...], Any]
) -> tuple[str, ...] | None:
    """Самый длинный префикс `tokens`, который является путём в дереве.

    Длиннее — точнее: `crawl policy set` проверяется как три уровня, а не как `crawl`.
    """
    for length in range(len(tokens), 0, -1):
        candidate = tokens[:length]
        if candidate in nodes:
            return candidate
    return None


def _unknown_subcommand(
    tokens: tuple[str, ...], resolved: tuple[str, ...], nodes: dict[tuple[str, ...], Any]
) -> str | None:
    """Первый токен после разрешённого пути, если узел ждал **подкоманду**, а не значение.

    Без этой проверки переименование подкоманды проходило молча: `crawl policy vanish`
    разрешается в `crawl policy` — префикс существует, значит «команда есть». Но у узла
    `crawl policy` есть подкоманды, и `vanish` среди них нет: это ошибка документа, а не
    позиционный аргумент (`crawl status {run_id}` — как раз позиционный, там подкоманд нет).
    """
    if len(tokens) <= len(resolved):
        return None
    expected = _subcommands(nodes[resolved])
    if not expected:
        return None
    candidate = tokens[len(resolved)]
    return None if candidate in expected else candidate


def _env_declared(root: Path, name: str) -> bool:
    """Переменная объявлена в `.env.example` или упомянута в коде `core/`.

    Совпадение в коде — **по границе имени**, а не подстрокой: `COHOUND_PATHS_FIL`
    (опечатка) является префиксом `COHOUND_PATHS_FILE` и при поиске подстрокой «находилась»
    бы в собственной опечатке. Гейт, который подтверждает опечатку, хуже отсутствия гейта.
    """
    example = root / ".env.example"
    if example.is_file() and re.search(
        rf"^\s*{re.escape(name)}\s*=", example.read_text(encoding="utf-8"), re.MULTILINE
    ):
        return True
    pattern = re.compile(rf"(?<![A-Z0-9_]){re.escape(name)}(?![A-Z0-9_])")
    for path in (root / "core").rglob("*.py"):
        try:
            if pattern.search(path.read_text(encoding="utf-8")):
                return True
        except (OSError, UnicodeDecodeError):
            continue
    return False


def _fenced_line_numbers(text: str) -> set[int]:
    """Индексы строк внутри ```-блоков (сами черты входят: они не проза)."""
    inside: set[int] = set()
    opened = False
    for index, line in enumerate(text.splitlines()):
        if line.strip().startswith("```"):
            inside.add(index)
            opened = not opened
            continue
        if opened:
            inside.add(index)
    return inside


def _command_lines(text: str) -> set[int]:
    """Индексы строк, входящих в команду `hound`, — вместе с продолжениями через ``\\``.

    Команда в документе редко помещается в строку: флаги идут ниже, а строка запуска
    заканчивается ``\\``. Проверка «на этой строке есть слово hound» ловила бы только первую
    строку и пропускала опечатку в `--run-yaml`, стоящем на второй, — то есть ровно тот
    случай, ради которого гейт существует.
    """
    lines = text.splitlines()
    ours: set[int] = set()
    for index, line in enumerate(lines):
        continues = (
            index > 0
            and (index - 1) in ours
            and lines[index - 1].rstrip().endswith("\\")
        )
        if _COMMAND_LINE.match(line) or continues:
            ours.add(index)
    return ours


def _display(doc: Path, root: Path) -> str:
    """Имя документа для сообщения: относительно корня, а вне корня — абсолютным путём.

    ``--docs`` принимает и путь вне репозитория (мутант для проверки самого гейта, чужой
    документ). ``Path.relative_to`` на таком пути бросает ``ValueError`` — гейт падал бы
    трейсбеком вместо ответа, а трейсбек читается как «проблема в документе», хотя это
    проблема вывода.
    """
    try:
        return doc.relative_to(root).as_posix()
    except ValueError:
        return doc.as_posix()


def check_document(
    doc: Path,
    *,
    root: Path,
    nodes: dict[tuple[str, ...], Any],
    all_flags: set[str],
) -> tuple[list[str], list[str]]:
    """Проверки одного документа: (проблемы, предупреждения)."""
    problems: list[str] = []
    warnings: list[str] = []
    text = doc.read_text(encoding="utf-8")
    where = _display(doc, root)

    # 1. Команды — только внутри блоков кода.
    for block in _fenced_blocks(text):
        for line in block:
            match = _COMMAND_LINE.match(line)
            if match is None:
                continue
            tokens = _command_path(match.group("rest"))
            if not tokens:
                problems.append(f"{where}: строка запуска без команды — {line.strip()!r}")
                continue
            resolved = _resolve_command(tokens, nodes)
            if resolved is None:
                problems.append(
                    f"{where}: команды нет в CLI — {line.strip()!r} "
                    f"(проверено {len(nodes)} узлов дерева)"
                )
                continue
            unknown = _unknown_subcommand(tokens, resolved, nodes)
            if unknown is not None:
                expected = sorted(_subcommands(nodes[resolved]))
                problems.append(
                    f"{where}: подкоманды нет у {' '.join(resolved)} — {unknown!r}; "
                    f"есть: {', '.join(expected)}"
                )

    # 1a. Команды в прозе (в обратных кавычках, вне блоков кода) — предупреждение, а не
    #     ошибка. Отличить «выполните это» от «команда X была удалена» по синтаксису нельзя,
    #     поэтому проза не блокирует CI, но и не исчезает: устаревшая команда в таблице
    #     (а таблица — тоже инструкция) должна быть видна читателю гейта, а не только тому,
    #     кто набрал её руками.
    fenced = _fenced_line_numbers(text)
    for index, line in enumerate(text.splitlines()):
        if index in fenced:
            continue
        for span in re.findall(r"`([^`]+)`", line):
            if _PLACEHOLDER.search(span):
                continue  # шаблон (`./hound.py <command> --help`) — не команда
            match = _COMMAND_LINE.match(span.strip())
            if match is None:
                continue
            tokens = _command_path(match.group("rest"))
            if not tokens or _resolve_command(tokens, nodes) is not None:
                continue
            warnings.append(
                f"{where}: в прозе названа команда, которой нет в CLI — {span.strip()!r}"
            )

    # 2. Флаги. Проверяются те, что обещают **наш** CLI: флаг на строке команды `hound`
    #    (включая продолжения через `\`) либо флаг в обратных кавычках на строке без чужой
    #    утилиты. Так `uv sync --frozen` и `pytest --dist` не требуют флага от нашего
    #    дерева, а «`--llm-model` > `deploy.llm.<slot>.model_id`» — требует.
    hound_lines = _command_lines(text)
    claimed: set[str] = set()
    for index, line in enumerate(text.splitlines()):
        if index in hound_lines or (
            "`" in line
            and not any(
                re.search(rf"(?<![\w/-]){re.escape(tool)}(?![\w-])", line)
                for tool in FOREIGN_TOOLS
            )
        ):
            claimed.update(_FLAG.findall(line))
    for flag in sorted(claimed):
        if flag not in all_flags:
            problems.append(f"{where}: флага нет ни в одной команде — {flag}")

    # 3. Переменные окружения.
    for name in sorted(set(_ENV_VAR.findall(text))):
        if not _env_declared(root, name):
            problems.append(
                f"{where}: переменной нет ни в .env.example, ни в core/ — {name}"
            )

    # 4. Пути репозитория.
    for raw in sorted(set(_REPO_PATH.findall(text))):
        candidate = raw.split(":", 1)[0].rstrip("/")
        root_name = candidate.split("/", 1)[0]
        if root_name not in REPO_ROOTS:
            continue
        if _PLACEHOLDER.search(candidate):
            warnings.append(f"{where}: шаблон, не проверяется — {candidate}")
            continue
        if (root / candidate).exists():
            continue
        if root_name in DATA_ROOTS:
            warnings.append(
                f"{where}: каталога данных нет в дереве (это артефакт) — {candidate}"
            )
        else:
            problems.append(f"{where}: пути нет — {candidate}")

    # 5. Внутренние ссылки markdown.
    for match in _MD_LINK.finditer(text):
        target = match.group("target")
        if target.startswith(("http://", "https://", "#", "mailto:")):
            continue
        path_part = target.split("#", 1)[0]
        if not path_part:
            continue
        if not (doc.parent / path_part).exists():
            problems.append(f"{where}: ссылка не разрешается — {target}")

    return problems, warnings


def _script_flags(root: Path) -> set[str]:
    """Флаги приборов: `scripts/*.py` объявляют свои аргументы сами.

    Runbook документирует два вида поверхности — CLI и приборы (`scripts/measure_*.py`,
    `scripts/fetch_ocr_models.py`, `scripts/check_runbook.py`). Проверять флаг прибора по
    дереву CLI значило бы требовать от документа, чтобы он молчал о половине инструментов.
    Поэтому набор флагов скриптов читается из их `add_argument` — тоже по коду, а не по
    списку известных имён.
    """
    found: set[str] = set()
    for path in sorted((root / "scripts").glob("*.py")):
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        found.update(re.findall(r"add_argument\(\s*[\"'](--[a-z][a-z0-9-]*)[\"']", text))
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="проверить runbook: команды, флаги, переменные и пути — против кода"
    )
    parser.add_argument(
        "--docs",
        nargs="*",
        default=None,
        help=f"документы (по умолчанию: {', '.join(DEFAULT_DOCS)})",
    )
    parser.add_argument(
        "--strict", action="store_true", help="код 1 при проблемах (иначе только печать)"
    )
    parser.add_argument("--show", action="store_true", help="печатать, что проверено")
    args = parser.parse_args(argv)

    root = _repo_root()
    names = args.docs if args.docs else list(DEFAULT_DOCS)
    docs = [root / name for name in names]

    missing = [doc for doc in docs if not doc.is_file()]
    for doc in missing:
        print(f"ОШИБКА  документа нет — {_display(doc, root)}")
    if len(missing) == len(docs):
        return 2

    nodes = _walk(_parser_tree())
    all_flags = _flags(nodes) | _script_flags(root)
    if args.show:
        print(f"дерево CLI: {len(nodes)} узлов, {len(all_flags)} флагов (включая приборы)")

    problems: list[str] = []
    warnings: list[str] = []
    for doc in docs:
        if not doc.is_file():
            continue
        found_problems, found_warnings = check_document(
            doc, root=root, nodes=nodes, all_flags=all_flags
        )
        problems.extend(found_problems)
        warnings.extend(found_warnings)
        if args.show:
            print(
                f"проверено: {_display(doc, root)} — "
                f"{len(found_problems)} проблем, {len(found_warnings)} предупреждений"
            )

    for problem in problems:
        print(f"ОШИБКА  {problem}")
    for warning in warnings:
        print(f"ВНИМАНИЕ  {warning}")
    print(f"проблем: {len(problems)}, предупреждений: {len(warnings)}")

    if args.strict and problems:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
