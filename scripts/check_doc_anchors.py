#!/usr/bin/env python3
"""Проверка якорей ``путь:строка`` в документации — по файлу, а не на глаз.

Документы ссылаются на код как ``core/pipeline/resources.py:542`` или
``resources.py:116-152``. Такие якоря ломаются от **любой** вставки выше по файлу, и
ломаются молча: текст читается правдоподобно, а строка указывает в другое место. Поэтому
проверка здесь механическая.

Что проверяется:

1. файл, названный в якоре, существует (разрешение — по суффиксу пути: якоря пишут и
   полный путь, и basename, поэтому ``resources.py`` ищется в дереве однозначно);
2. номер строки в диапазоне файла;
3. строка не пустая (якорь в пустую строку или в ``pass`` ничего не доказывает);
4. неоднозначный basename (два файла с одним именем) — предупреждение: якорь нельзя
   разрешить однозначно, значит он не доказательство.

Смысловое совпадение (строка действительно про то, о чём говорит текст) машина не
проверяет: ``--show`` печатает содержимое строки, чтобы это сделал человек. Отсутствие
смысловой проверки названо, а не замаскировано.

Запуск::

    python3 scripts/check_doc_anchors.py            # отчёт, код 0
    python3 scripts/check_doc_anchors.py --strict    # код 1 при находках (для CI)
    python3 scripts/check_doc_anchors.py --show      # печатать содержимое якорей
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

#: Каталоги документации, где якоря — обещание читателю.
DOC_ROOTS = ("docs", "README.md", "AGENTS.md")

#: Расширения, на которые осмысленно ссылаться построчно.
CODE_SUFFIXES = (
    ".py",
    ".yaml",
    ".yml",
    ".toml",
    ".md",
    ".sh",
    ".cfg",
    ".json",
)

#: ``path/to/file.py:123`` или ``file.py:123-456``. Границы — не часть пути.
_ANCHOR = re.compile(
    r"(?<![\w/.-])([A-Za-z0-9_][A-Za-z0-9_./-]*\.(?:py|yaml|yml|toml|md|sh|cfg|json))"
    r":(\d+)(?:-(\d+))?"
)

#: Каталоги, которые не являются частью репозитория-источника.
_SKIP_DIRS = {".git", ".venv", "node_modules", "__pycache__", ".slim", ".mypy_cache"}

#: Верхние сегменты путей, которые принадлежат **этому** репозиторию. Якорь с другим
#: первым сегментом (``docling/pipeline/base_pipeline.py``, ``base_models.py``) ссылается на
#: сторонний пакет: проверить его существование можно только в установленном окружении,
#: поэтому это предупреждение, а не ошибка. Иначе отчёт смешивал бы «наш якорь сгнил» с
#: «мы сослались на чужой файл».
REPO_ROOTS = ("core", "tests", "scripts", "docs", "packages", "edge", "contracts", "config", "hound.py", "cohound-spec-v2.md")


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _index_code_files(root: Path) -> dict[str, list[Path]]:
    """Индекс ``имя файла → пути`` по дереву репозитория (без данных прогонов)."""
    index: dict[str, list[Path]] = {}
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix not in CODE_SUFFIXES:
            continue
        if any(part in _SKIP_DIRS for part in path.parts):
            continue
        if path.parts[:1] in {("runs",), ("index_store",), ("corpus_raw",)}:
            continue
        index.setdefault(path.name, []).append(path)
    return index


def _doc_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for root_name in DOC_ROOTS:
        target = root / root_name
        if target.is_file():
            files.append(target)
        elif target.is_dir():
            files.extend(sorted(target.rglob("*.md")))
            files.extend(sorted(target.rglob("*.yaml")))
    return files


def _resolve(anchor_path: str, index: dict[str, list[Path]]) -> tuple[Path | None, int]:
    """Разрешает путь якоря; возвращает (файл, число кандидатов)."""
    name = Path(anchor_path).name
    candidates = index.get(name, [])
    if not candidates:
        return None, 0
    # Точное совпадение хвоста пути сильнее, чем basename.
    suffix = anchor_path.lstrip("./")
    exact = [p for p in candidates if str(p).endswith(suffix)]
    if len(exact) == 1:
        return exact[0], 1
    if len(candidates) == 1:
        return candidates[0], 1
    return candidates[0], len(candidates)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true", help="код 1 при находках")
    parser.add_argument("--show", action="store_true", help="печатать строку якоря")
    args = parser.parse_args(argv)

    root = _repo_root()
    index = _index_code_files(root)
    problems: list[str] = []
    warnings: list[str] = []
    checked = 0

    for doc in _doc_files(root):
        try:
            text = doc.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        lines = text.splitlines()
        for lineno, line in enumerate(lines, start=1):
            if "://" in line:  # URL — не якорь
                continue
            for match in _ANCHOR.finditer(line):
                anchor_path, start, end = match.group(1), int(match.group(2)), match.group(3)
                if anchor_path.startswith(("http", "www")):
                    continue
                checked += 1
                target, candidates = _resolve(anchor_path, index)
                where = f"{doc.relative_to(root)}:{lineno}"
                if target is None:
                    if anchor_path.lstrip("./").split("/", 1)[0] in REPO_ROOTS:
                        problems.append(f"{where}: файла нет — {anchor_path}")
                    else:
                        warnings.append(
                            f"{where}: вне репозитория, не проверить — {anchor_path}"
                        )
                    continue
                if candidates > 1:
                    warnings.append(
                        f"{where}: неоднозначно — {anchor_path} ({candidates} кандидатов)"
                    )
                total = len(target.read_text(encoding="utf-8").splitlines())
                last = int(end) if end else start
                if not 1 <= start <= last <= total:
                    problems.append(
                        f"{where}: {anchor_path}:{start}"
                        f"{'-' + str(last) if end else ''} вне файла (строк {total})"
                    )
                    continue
                if args.show:
                    snippet = " / ".join(
                        s.strip() for s in target.read_text(encoding="utf-8").splitlines()[start - 1 : last][:1]
                    )
                    print(f"OK  {where} → {anchor_path}:{start}: {snippet[:90]}")
                elif not target.read_text(encoding="utf-8").splitlines()[start - 1].strip():
                    problems.append(f"{where}: {anchor_path}:{start} — пустая строка")

    print(f"якорей проверено: {checked}")
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
