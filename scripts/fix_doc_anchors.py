"""Переставить ``path:line``-якоря в документах **по содержанию**, а не по арифметике.

**Зачем.** Правка ``core/`` сдвигает строки. Гейт (``scripts/check_doc_anchors.py``) это
ловит, но проверяет только «строка непуста»: неверный якорь, попавший на другую непустую
строку, проходит молча. Арифметика (``+N``) даёт **двойной** сдвиг, если считать от рабочего
документа. Здесь третий принцип: берётся текст строки, на которую якорь указывал в ``HEAD``,
и в текущем файле ищется строка с тем же содержимым.

**Почему только сломанные.** Искать все якоря по содержимому бессмысленно: пустые строки и
``\"\"\"`` совпадают десятками, и «однозначного» совпадения не будет никогда. Гейт уже назвал
адреса, где строка стала пустой, — это ровно те, что поехали; их и пере-указываем. Когда
содержимое в нескольких местах, берётся ближайшее к прежнему номеру (правка сдвигает
монотонно), а выбор **печатается** — молчаливой догадки нет.

Запуск::

    python scripts/fix_doc_anchors.py --dry-run     # что будет изменено
    python scripts/fix_doc_anchors.py               # применить

Нейтрален (§1.3).
"""

from __future__ import annotations

import argparse
import re
import subprocess
from collections.abc import Sequence
from pathlib import Path

#: Строка отчёта гейта: ``ОШИБКА  docs/x.md:12: core/y.py:34 — пустая строка``.
_GATE_RE = re.compile(r"^ОШИБКА\s+(?P<doc>\S+):(?P<doc_line>\d+):\s+(?P<target>\S+):(?P<line>\d+)")


def _gate_errors(root: Path) -> list[tuple[str, int, str, int]]:
    """Сломанные якоря по вердикту гейта: ``(документ, строка документа, файл, строка)``."""
    result = subprocess.run(
        ["python3", "scripts/check_doc_anchors.py", "--strict"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    out: list[tuple[str, int, str, int]] = []
    for line in result.stdout.splitlines():
        match = _GATE_RE.match(line.strip())
        if match:
            out.append(
                (
                    match.group("doc"),
                    int(match.group("doc_line")),
                    match.group("target"),
                    int(match.group("line")),
                )
            )
    return out


def _resolve(root: Path, target: str) -> str | None:
    """Путь к файлу: как задан, а если это голое имя — единственный такой в репозитории.

    Документы иногда пишут якорь без каталога (``corpus_index.py:86``). Такая запись
    разрешается только при **единственном** совпадении: угадывать между двумя файлами с
    одинаковым именем значило бы пере-указать якорь на чужой модуль.
    """
    if (root / target).is_file():
        return target
    if "/" in target:
        return None
    hits = sorted(root.rglob(target))
    hits = [h for h in hits if ".venv" not in h.parts and not h.parts[0].startswith(".")]
    return str(hits[0].relative_to(root)) if len(hits) == 1 else None


def _head_lines(path: str) -> list[str] | None:
    result = subprocess.run(
        ["git", "show", f"HEAD:{path}"], capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        return None
    return result.stdout.splitlines()


def _candidates(text: str, lines: list[str], near: int) -> list[int]:
    """1-based номера строк, равных ``text``; отсортированы по удалённости от ``near``."""
    hits = [n for n, line in enumerate(lines, start=1) if line == text]
    return sorted(hits, key=lambda n: (abs(n - near), n))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="пере-указать сломанные якоря по содержанию")
    parser.add_argument("--root", default=".", help="корень репозитория")
    parser.add_argument("--dry-run", action="store_true", help="только показать")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    errors = _gate_errors(root)
    if not errors:
        print("гейт чист: пере-указывать нечего")
        return 0

    head_cache: dict[str, list[str] | None] = {}
    current_cache: dict[str, list[str]] = {}
    by_doc: dict[str, list[tuple[int, str, int]]] = {}
    for doc, doc_line, target, line in errors:
        by_doc.setdefault(doc, []).append((doc_line, target, line))

    moved = 0
    unresolved: list[str] = []
    for doc, items in sorted(by_doc.items()):
        path = root / doc
        if not path.is_file():
            unresolved.extend(f"{doc}: файл документа не найден" for _ in items)
            continue
        doc_lines = path.read_text(encoding="utf-8").splitlines()
        changed = False
        for doc_line, target, old_line in items:
            raw_target = target  # как написано в документе — это и заменяем
            resolved = _resolve(root, target)
            if resolved is None:
                unresolved.append(
                    f"{doc}:{doc_line} → {target}:{old_line} (файл не разрешается однозначно)"
                )
                continue
            target = resolved
            if target not in head_cache:
                head_cache[target] = _head_lines(target)
            head = head_cache[target]
            if head is None or not 1 <= old_line <= len(head):
                unresolved.append(f"{doc}:{doc_line} → {target}:{old_line} (нет в HEAD)")
                continue
            text = head[old_line - 1]
            if not text.strip():
                unresolved.append(
                    f"{doc}:{doc_line} → {target}:{old_line} "
                    "(в HEAD пустая строка — якорь был неверен уже там)"
                )
                continue
            current = root / target
            if not current.is_file():
                unresolved.append(f"{doc}:{doc_line} → {target}:{old_line} (файла нет)")
                continue
            if target not in current_cache:
                current_cache[target] = current.read_text(encoding="utf-8").splitlines()
            hits = _candidates(text, current_cache[target], old_line)
            if not hits:
                unresolved.append(
                    f"{doc}:{doc_line} → {target}:{old_line} (содержимое не найдено)"
                )
                continue
            new_line = hits[0]
            if len(hits) > 1:
                print(
                    f"  {doc}:{doc_line} → {target}:{old_line}: содержимое в {len(hits)} "
                    f"местах {hits[:4]}, беру ближайшее {new_line}"
                )
            old_anchor = f"{raw_target}:{old_line}"
            if old_anchor not in doc_lines[doc_line - 1]:
                unresolved.append(f"{doc}:{doc_line}: якоря {old_anchor} нет в строке")
                continue
            doc_lines[doc_line - 1] = doc_lines[doc_line - 1].replace(
                old_anchor, f"{target}:{new_line}", 1
            )
            print(f"{doc}:{doc_line}: {old_anchor} → {target}:{new_line}")
            moved += 1
            changed = True
        if changed and not args.dry_run:
            path.write_text("\n".join(doc_lines) + "\n", encoding="utf-8")

    print(f"\nякорей пере-указано: {moved}")
    if unresolved:
        print(f"требуют человека: {len(unresolved)}")
        for item in unresolved:
            print(f"  {item}")
    if args.dry_run:
        print("(dry-run: файлы не изменены)")
    return 0 if not unresolved else 1


if __name__ == "__main__":
    raise SystemExit(main())
