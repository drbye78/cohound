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

Режим дрейфа (``--check-drift BASE``)
-------------------------------------

Проверки выше отвечают «строка по адресу непуста», и этого мало: после правки кода
(``+7`` строк в одном модуле, ``+25`` в другом) **55** якорей в ``docs/architecture/*``
уехали, а гейт сказал «проблем: 0» — на новых адресах тоже были непустые строки.
Дрейф — это «якорь указывает не на ту строку, на которую указывал», и его видно только
сравнением с ревизией.

Для каждого файла, на который ссылаются якоря, строится отображение строк ``BASE`` →
рабочее дерево через ``difflib.SequenceMatcher`` (по **содержимому** строк, не по
номерам и не арифметикой). Затем для якоря ``path:LINE``:

* ``LINE`` в ``BASE`` сопоставлена с другой строкой рабочего дерева → **дрейф**:
  ``документ:строка → path:LINE (съехало на +N; сейчас :M)``, то есть ``:M`` — то, что
  следует написать. Код возврата ``1`` — только вместе с ``--strict``;
* ``LINE`` в ``BASE`` попала в блок вставки/замены (пары нет) → **не** дрейф, а отдельная
  категория «якорь указывает на текст, которого в ``BASE`` не было»: новый якорь,
  добавленный вместе с кодом, иначе ломал бы гейт. В код возврата не входит;
* файла в ``BASE`` нет или такой строки в нём нет → предупреждение (не проблема).

Номер читается в ``BASE`` только у якоря, который **в документе не менялся**. Если строку
документа переписали, номер в ней — новый, то есть автор уже пере-указал адрес; читать его
в ``BASE`` значило бы объявить дрейфом саму правку (замер 16.11.2026, ``BASE=HEAD~3``:
54 из 64 сообщений были такими). Поэтому состояние якоря берётся из блока документа:
строка не менялась → считаем по ``BASE``; номер якоря на тот же файл изменился →
``пере-указано`` (не дрейф); якоря на тот же файл в ``BASE`` не было → ``новый`` (не дрейф).

**Голое имя файла разрешается так же, как полный путь.** Якорь ``evaluate.py:123`` и якорь
``core/pipeline/evaluate.py:123`` — один и тот же адрес: файл ищется по basename, и только
потом уточняется хвостом пути. Без этого половина якорей в таблицах ревью (там пишут
``retrieve.py:305``) не проверялась бы вовсе — ровно так ручной проход 16.11.2026 пропустил
**9** уехавших якорей: регулярка искала полный путь, а в документе стояло голое имя. Если
basename **неоднозначен** (два файла с одним именем в дереве), это предупреждение, а дрейф
по такому якорю не считается: выбрать «первый» значило бы сравнить якорь с чужим файлом и
выдать ложный вердикт.

``docs/reports/**`` проверяются, но называются **отдельной категорией** и в код возврата не
входят: дата в шапке отчёта делает номер строки **частью записи**, а не ссылкой на живой
код. Отчёт описывает дерево на дату, и «исправить» в нём якорь значит сделать датированную
запись лживой про её собственную дату. Это вся ветка, а не только ``measurements/``: по
``AGENTS.md`` §3 ``docs/reports/`` — опубликованное свидетельство (замеры и трассы
инцидентов), и план ``2026-10-30-large-modules-refactoring-plan.md`` датирован по той же
причине.

**Якорь на удалённый файл — не ошибка гейта, а свойство записи:** доказательство описывает
состояние на дату, и живой адрес у него отсутствует по определению. Ошибкой является якорь
на файл, который в репозитории есть, а строки в нём нет. Граница проходит по первому
сегменту пути, а не по факту удаления: ``util/fusion.py:102`` (сегмент не из ``REPO_ROOTS``)
— предупреждение «вне репозитория», а ``core/util/fusion.py:102`` — ``ОШИБКА: файла нет``,
хотя файла нет в обоих случаях. Это расхождение названо, а не замаскировано: датированную
запись пишут без префикса ``core/``, и это единственное, что отличает её от сгнившего якоря.
Поэтому ``util`` в ``REPO_ROOTS`` не добавляется — иначе гейт требовал бы правки правильной
записи о прошлом.

Инструмент **ничего не переписывает**: авто-правка якорей молча сделала бы «правильный
адрес, лживый смысл» там, где якорь был неверен изначально.

Чего режим дрейфа **не** ловит (названо, а не замаскировано):

* якорь, неверный **уже в** ``BASE``: сдвиг такой якорь сохраняет неверным, и отличить
  «сдвинулось» от «было неверно» по строкам нельзя — нужен смысл;
* якорь, пере-указанный **на неверный** адрес: номер изменён — значит правка признана
  намеренной, а верна ли она, решает человек (``--show``);
* смысл якоря (о чём строка) и диапазоны ``:123-456`` проверяются только по границам.

Запуск::

    python3 scripts/check_doc_anchors.py            # отчёт, код 0
    python3 scripts/check_doc_anchors.py --strict    # код 1 при находках (для CI)
    python3 scripts/check_doc_anchors.py --show      # печатать содержимое якорей
    python3 scripts/check_doc_anchors.py --check-drift HEAD          # дрейф, код 0
    python3 scripts/check_doc_anchors.py --strict --check-drift HEAD # дрейф, код 1
"""

from __future__ import annotations

import argparse
import difflib
import re
import subprocess
import sys
from dataclasses import dataclass
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


def git_ignored(root: Path) -> set[Path]:
    """Файлы, которые git игнорирует: они не часть обещания репозитория.

    Документ, не отслеживаемый git (``.gitignore``), не публикуется, поэтому его якоря —
    рабочие заметки, а не обещание читателю. Считать их провалом CI значило бы требовать
    правки в файле, который в репозиторий не попадает. Список берётся у git, а не у копии
    правил: копия разошлась бы с ``.gitignore`` молча. git недоступен — фильтра нет
    (лучше лишний отчёт, чем тишина).
    """
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-oi", "--exclude-standard"],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):  # pragma: no cover — нет git/таймаут
        return set()
    if completed.returncode != 0:
        return set()
    return {
        (root / line.strip()).resolve()
        for line in completed.stdout.splitlines()
        if line.strip()
    }


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


#: Сколько строк дрейфа печатать: 55 строк подряд читать невозможно, но число в итоге
#: должно быть полным — иначе «первые 50» читаются как «всего 50».
DRIFT_PRINT_LIMIT = 50

#: Документы, чей якорь описывает код **на дату** записи: пере-указанию не подлежат.
#: Вся ветка ``docs/reports/``, а не только ``measurements/``: по ``AGENTS.md`` §3 это
#: опубликованное свидетельство (замеры и трассы инцидентов), и дата в шапке делает номер
#: строки частью записи. «Исправленный» якорь в датированном отчёте — ложь про его дату.
DATED_REPORT_ROOT = "docs/reports/"


@dataclass(frozen=True)
class _Anchor:
    """Якорь, разрешённый в файл рабочего дерева: вход режима дрейфа."""

    doc: str  # путь документа относительно корня — то, что читает человек
    doc_line: int  # строка документа, где якорь написан
    target: Path  # файл, на который якорь ссылается (рабочее дерево)
    anchor_path: str  # путь в тексте якоря (может быть basename)
    line: int  # номер строки в якоре (начало диапазона)


def _git(root: Path, *args: str) -> str | None:
    """``git`` без исключений: нет git/репозитория — ``None``, а не падение.

    ``check=False`` обязателен: отсутствие файла в ревизии — нормальный исход проверки,
    а не сбой инструмента.
    """
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):  # pragma: no cover — нет git/таймаут
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout


def _rev_exists(root: Path, rev: str) -> bool:
    """Разрешается ли ``rev`` в коммит (``HEAD``, ``HEAD~2``, sha, тег)."""
    return _git(root, "rev-parse", "--verify", "--quiet", f"{rev}^{{commit}}") is not None


def _line_map(base: list[str], work: list[str]) -> dict[int, int]:
    """Отображение ``индекс строки BASE → индекс строки рабочего дерева`` (0-based).

    Только пары ``equal``: строка, попавшая в блок вставки/замены, пары не имеет — это и
    есть различие между «съехало» и «такого текста в ``BASE`` не было». ``autojunk=False``
    обязателен: на файлах с повторяющимися строками (DDL, словари) эвристика ``difflib``
    выбрасывает «популярные» строки из сравнения, и отображение становится случайным.
    """
    mapping: dict[int, int] = {}
    matcher = difflib.SequenceMatcher(None, base, work, autojunk=False)
    for tag, i1, i2, j1, _j2 in matcher.get_opcodes():
        if tag != "equal":
            continue
        for offset in range(i2 - i1):
            mapping[i1 + offset] = j1 + offset
    return mapping


def _read_lines(path: Path) -> list[str]:
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return []


class _DocMap:
    """Строки **документа**: ``BASE`` → рабочее дерево, блоками, а не парами.

    Нужен, чтобы отличить «якорь не пере-указывали» от «пере-указали». Правило «читать
    номер якоря в ``BASE``» верно только для якоря, который в документе не менялся: если
    строку документа переписали, номер в ней — новый (автор уже пере-указал якорь), и
    чтение его в ``BASE`` даёт ложный дрейф. Замер 16.11.2026 (``BASE=HEAD~3``, правка
    ``retrieve.py``/``evaluate.py``): из 64 сообщённых дрейфов **54** оказались
    пере-указанными, настоящих — 9. Поэтому состояние якоря определяется по блоку
    документа, а не по одному номеру.
    """

    def __init__(self, base: list[str], work: list[str]) -> None:
        self.base = base
        self.ops = difflib.SequenceMatcher(None, base, work, autojunk=False).get_opcodes()
        self._op_of = [-1] * len(work)
        for index, op in enumerate(self.ops):
            for line in range(op[3], op[4]):
                self._op_of[line] = index

    def op_for(self, work_index: int) -> tuple[str, int, int, int, int] | None:
        """Блок ``difflib``, в который попала строка рабочего документа (0-based)."""
        if not 0 <= work_index < len(self._op_of):
            return None
        index = self._op_of[work_index]
        return None if index < 0 else self.ops[index]

    def state(self, work_index: int, anchor_path: str, line: int) -> str:
        """``unchanged`` | ``repointed`` | ``new`` — что случилось с якорем в документе.

        * ``unchanged`` — строка документа не менялась: номер в ней тот же, что в ``BASE``,
          значит дрейф считается по строке ``BASE``;
        * ``repointed`` — строку переписали, и номер якоря на тот же файл **изменился**:
          автор уже пере-указал адрес, дрейфа нет;
        * ``new`` — в ``BASE`` на этом месте якоря на тот же файл не было: якорь новый
          (добавлен вместе с кодом), сверять его с ``BASE`` не с чем.
        """
        op = self.op_for(work_index)
        if op is None:  # pragma: no cover — индекс всегда внутри документа
            return "new"
        tag, i1, i2, _j1, _j2 = op
        if tag == "equal":
            return "unchanged"
        numbers = [
            int(match.group(2))
            for base_line in self.base[i1:i2]
            for match in _ANCHOR.finditer(base_line)
            if match.group(1) == anchor_path
        ]
        if not numbers:
            return "new"
        return "unchanged" if numbers[0] == line else "repointed"


def _drift_report(
    root: Path, rev: str, anchors: list[_Anchor]
) -> tuple[list[str], list[str], list[str], list[str], int]:
    """Считает дрейф: ``(дрейф, дрейф в отчётах, новый текст, предупреждения, пере-указано)``."""
    drifts: list[str] = []
    dated: list[str] = []
    fresh: list[str] = []
    warnings: list[str] = []
    base_cache: dict[Path, list[str] | None] = {}
    maps: dict[Path, dict[int, int]] = {}
    doc_maps: dict[str, _DocMap | None] = {}
    repointed = 0

    for anchor in anchors:
        where = f"{anchor.doc}:{anchor.doc_line}"
        if anchor.doc not in doc_maps:
            text = _git(root, "show", f"{rev}:{anchor.doc}")
            doc_maps[anchor.doc] = (
                None if text is None else _DocMap(text.splitlines(), _read_lines(root / anchor.doc))
            )
        doc_map = doc_maps[anchor.doc]
        if doc_map is None:
            warnings.append(f"{where}: документа нет в {rev} — дрейф не считается")
            continue
        state = doc_map.state(anchor.doc_line - 1, anchor.anchor_path, anchor.line)
        if state == "new":
            fresh.append(f"{where} → {anchor.anchor_path}:{anchor.line}")
            continue
        if state == "repointed":
            repointed += 1
            continue

        if anchor.target not in base_cache:
            rel: str | None
            try:
                rel = anchor.target.relative_to(root).as_posix()
            except ValueError:  # pragma: no cover — индекс строится от корня
                rel = None
            text = _git(root, "show", f"{rev}:{rel}") if rel else None
            base = None if text is None else text.splitlines()
            base_cache[anchor.target] = base
            if base is not None:
                maps[anchor.target] = _line_map(base, _read_lines(anchor.target))
        base = base_cache[anchor.target]
        if base is None:
            warnings.append(f"{where}: {anchor.anchor_path}:{anchor.line} — файла нет в {rev}")
            continue
        if not 1 <= anchor.line <= len(base):
            warnings.append(
                f"{where}: {anchor.anchor_path}:{anchor.line}"
                f" — в {rev} такой строки нет (строк {len(base)})"
            )
            continue
        moved = maps[anchor.target].get(anchor.line - 1)
        if moved is None:
            fresh.append(f"{where} → {anchor.anchor_path}:{anchor.line}")
            continue
        now = moved + 1
        if now == anchor.line:
            continue
        entry = (
            f"{where} → {anchor.anchor_path}:{anchor.line}"
            f" (съехало на {now - anchor.line:+d}; сейчас :{now})"
        )
        if anchor.doc.startswith(DATED_REPORT_ROOT):
            dated.append(entry)
        else:
            drifts.append(entry)
    return drifts, dated, fresh, warnings, repointed


def _print_drift(entries: list[str], prefix: str) -> None:
    """Печатает категорию с обрезкой: первая строка — адрес, читаемый машиной."""
    for entry in entries[:DRIFT_PRINT_LIMIT]:
        print(f"{prefix}  {entry}")
    if len(entries) > DRIFT_PRINT_LIMIT:
        print(f"… ещё {len(entries) - DRIFT_PRINT_LIMIT}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true", help="код 1 при находках")
    parser.add_argument("--show", action="store_true", help="печатать строку якоря")
    parser.add_argument(
        "--check-drift",
        metavar="BASE",
        default=None,
        help="проверить дрейф якорей относительно ревизии git (например HEAD)",
    )
    args = parser.parse_args(argv)

    root = _repo_root()
    if args.check_drift is not None and not _rev_exists(root, args.check_drift):
        print(f"ОШИБКА  ревизия не разрешается: {args.check_drift}")
        return 2
    index = _index_code_files(root)
    ignored = git_ignored(root)
    problems: list[str] = []
    warnings: list[str] = []
    anchors: list[_Anchor] = []
    checked = 0
    skipped_docs = 0

    for doc in _doc_files(root):
        if doc.resolve() in ignored:
            skipped_docs += 1
            continue
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
                    # Выбор «первого кандидата» годится для «строка непуста», но не для
                    # дрейфа: сравнение с чужим файлом даёт ложный вердикт. Поэтому в
                    # режиме дрейфа такой якорь пропускается, и это сказано в отчёте.
                    warnings.append(
                        f"{where}: неоднозначно — {anchor_path} ({candidates} кандидатов)"
                        + ("; дрейф не считается" if args.check_drift is not None else "")
                    )
                elif args.check_drift is not None:
                    anchors.append(
                        _Anchor(
                            doc=doc.relative_to(root).as_posix(),
                            doc_line=lineno,
                            target=target,
                            anchor_path=anchor_path,
                            line=start,
                        )
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

    drifts: list[str] = []
    dated: list[str] = []
    fresh: list[str] = []
    repointed = 0
    if args.check_drift is not None:
        drifts, dated, fresh, drift_warnings, repointed = _drift_report(
            root, args.check_drift, anchors
        )
        warnings.extend(drift_warnings)

    print(f"якорей проверено: {checked}")
    if skipped_docs:
        print(f"документов пропущено (не отслеживаются git): {skipped_docs}")
    for problem in problems:
        print(f"ОШИБКА  {problem}")
    for warning in warnings:
        print(f"ВНИМАНИЕ  {warning}")
    print(f"проблем: {len(problems)}, предупреждений: {len(warnings)}")

    if args.check_drift is not None:
        _print_drift(drifts, "ДРЕЙФ")
        _print_drift(dated, "ДРЕЙФ (датированный отчёт, пере-указанию не подлежит)")
        _print_drift(fresh, "НОВЫЙ ТЕКСТ")
        print(
            f"дрейф: {len(drifts)}, предупреждений: {len(warnings)},"
            f" проблем: {len(problems)}"
        )
        if dated:
            print(f"дрейф в датированных отчётах: {len(dated)} (код возврата не меняет)")
        if fresh:
            print(f"якорей на вставленный/заменённый текст: {len(fresh)} (не дрейф)")
        if repointed:
            print(f"пере-указано (номер якоря изменён относительно {args.check_drift}): {repointed}")

    if args.strict and (problems or drifts):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
