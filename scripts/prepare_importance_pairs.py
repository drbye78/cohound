#!/usr/bin/env python3
"""Прибор O20: пары для калибровки весов важности (60 параметров, 60 пар).

**Вопрос.** Веса ``IMPORTANCE_WEIGHTS`` (``core/domain/scoring.py``) — ``Кр=4, Вж=3,
Баз=2, Опц=1`` — помечены **provisional**: это допущение, а не измерение (A3). Проверить
его можно только парными сравнениями эксперта («какой из двух параметров важнее для
сравнения продуктов»), из которых считается матрица предпочтений и фит
(Bradley–Terry или простое упорядочение). Смена весов — ``methodology_change``.

**Что строит прибор.** Лист на разметку: 60 параметров каталога и 60 пар между ними.
Параметры выбираются стратифицированно — пропорционально ``importance`` в каталоге и
не менее одного от каждого модуля. Пары формируются по объявленному правилу
(:func:`build_pairs`): внутри одной страты важности (проверяет согласованность
эксперта) и между соседними стратами (проверяет порядок); ни один параметр не
встречается больше ``MAX_USES`` раз, чтобы 60 пар покрыли больше параметров, чем 30.

Подкоманды:

``build``
    Строит XLSX (``--catalog``, ``--out``, ``--params``, ``--pairs``).

``validate``
    Проверяет заполненный лист: все ``more_important`` из набора, нет дублей пар,
    ни один параметр не чаще ``MAX_USES`` раз, число строк равно ожидаемому.

``summary``
    Распределение ответов, число ``equal`` и список пар с ``equal`` — они первыми идут
    в разбор: ``equal`` и есть проверка, ординален ли текущий порядок весов.

Читается **каталог пакета**, а не прогон: калибровка весов — свойство методологии, она
не зависит от того, что успел измерить конкретный прогон.

Выборка детерминирована: сортировка по ``param_id`` и равномерный шаг внутри страты,
``random`` не используется — повторный запуск даёт те же пары.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any

#: Сколько раз один параметр может встретиться в парах. Три — компромисс: 60 пар × 2
#: места = 120 слотов, то есть при лимите 3 пары покрывают не менее 40 параметров
#: (при лимите 1 — ровно 30, и сравнения были бы попарно несвязными).
MAX_USES = 3

#: Значения ``more_important``, которые пишет человек. Пусто = ещё не размечено.
CHOICES = ("a", "b", "equal")

#: Колонки листа в порядке чтения.
COLUMNS = (
    "pair_id",
    "param_a",
    "title_a",
    "module_a",
    "importance_a",
    "rule_a",
    "param_b",
    "title_b",
    "module_b",
    "importance_b",
    "rule_b",
    "more_important",
    "comment",
)

#: Колонки, которые заполняет человек.
HUMAN_COLUMNS = ("more_important", "comment")

#: Колонки каталога, без которых пара не описывает параметр.
REQUIRED_CATALOG_COLUMNS = ("param_id", "title", "module", "importance", "rule")


@dataclass(frozen=True)
class Param:
    """Параметр каталога в объёме, нужном для листа."""

    param_id: str
    title: str
    module: str
    importance: str
    rule: str

    def as_cells(self) -> dict[str, str]:
        return {
            "param_id": self.param_id,
            "title": self.title,
            "module": self.module,
            "importance": self.importance,
            "rule": self.rule,
        }


@dataclass(frozen=True)
class Pair:
    pair_id: str
    a: Param
    b: Param

    def as_row(self) -> dict[str, str]:
        return {
            "pair_id": self.pair_id,
            "param_a": self.a.param_id,
            "title_a": self.a.title,
            "module_a": self.a.module,
            "importance_a": self.a.importance,
            "rule_a": self.a.rule,
            "param_b": self.b.param_id,
            "title_b": self.b.title,
            "module_b": self.b.module,
            "importance_b": self.b.importance,
            "rule_b": self.b.rule,
            "more_important": "",
            "comment": "",
        }


# ---------------------------------------------------------------------------
# Выборка параметров: чистая арифметика, проверяемая тестами без файлов
# ---------------------------------------------------------------------------
def uniform_pick(items: Sequence[Any], k: int) -> list[Any]:
    """``k`` значений из ``items`` равномерным шагом (не ``random``).

    Индекс i-го — ``round(i * (m-1) / (k-1))``: первый и последний попадают всегда.
    """
    m = len(items)
    if k <= 0:
        return []
    if k >= m:
        return list(items)
    if k == 1:
        return [items[0]]
    return [items[round(i * (m - 1) / (k - 1))] for i in range(k)]


def allocate(counts: dict[str, int], total: int) -> dict[str, int]:
    """Квоты пропорционально ``counts`` (метод наибольших остатков).

    Тай-брейк — порядок ключей переданного словаря: он объявлен вызывающим, поэтому
    повторный запуск даёт те же квоты.
    """
    keys = [key for key in counts if counts[key] > 0]
    denominator = sum(counts[key] for key in keys)
    if denominator <= 0 or total <= 0:
        return {key: 0 for key in keys}
    quotas = {key: total * counts[key] / denominator for key in keys}
    out = {key: int(quotas[key]) for key in keys}
    rest = total - sum(out.values())
    order = sorted(keys, key=lambda key: (-(quotas[key] - out[key]), keys.index(key)))
    for key in order[:rest]:
        out[key] += 1
    return out


def cap_and_redistribute(quotas: dict[str, int], available: dict[str, int]) -> dict[str, int]:
    """Обрезает квоты по доступному числу строк и раздаёт излишек, где есть запас.

    Нужно из-за остатка при делении: пропорциональная квота может превысить размер
    страты на единицу. Повторов не делается — излишек уходит в страту с запасом.
    """
    out = dict(quotas)
    keys = list(out)
    for _ in range(sum(out.values()) + len(out) + 1):
        excess = 0
        for key in keys:
            limit = available.get(key, 0)
            if out[key] > limit:
                excess += out[key] - limit
                out[key] = limit
        if not excess:
            return out
        spare = [key for key in keys if out[key] < available.get(key, 0)]
        if not spare:
            return out
        for key in sorted(spare, key=lambda key: (-(available[key] - out[key]), keys.index(key))):
            if excess <= 0:
                break
            add = min(excess, available[key] - out[key])
            out[key] += add
            excess -= add
    return out


def select_params(params: Sequence[Param], total: int) -> tuple[list[Param], dict[str, Any]]:
    """``total`` параметров: пропорционально importance, ≥1 от каждого модуля.

    Внутри страты ``(importance, module)`` — сортировка по ``param_id`` и равномерный
    шаг: выборка покрывает весь каталог, а не его начало.
    """
    if total > len(params):
        raise ValueError(f"параметров в каталоге {len(params)}, а запрошено {total}")
    by_importance: dict[str, list[Param]] = {}
    for param in params:
        by_importance.setdefault(param.importance, []).append(param)
    importance_counts = {key: len(value) for key, value in by_importance.items()}
    importance_quotas = allocate(importance_counts, total)

    # Модули внутри страты: минимум один, остаток — пропорционально числу параметров.
    cell_quotas: dict[tuple[str, str], int] = {}
    for importance, group in by_importance.items():
        quota = importance_quotas.get(importance, 0)
        modules: dict[str, int] = {}
        for param in group:
            modules[param.module] = modules.get(param.module, 0) + 1
        present = list(modules)
        if quota >= len(present):
            rest = allocate(modules, quota - len(present))
            for module in present:
                cell_quotas[(importance, module)] = 1 + rest.get(module, 0)
        else:
            # Квота меньше числа модулей: по одному — самым крупным, остальные модули в
            # выборку не попадают, и это печатается сборкой (молчаливого пропуска нет).
            for module in present[:quota]:
                cell_quotas[(importance, module)] = 1

    availability: dict[tuple[str, str], int] = {}
    for importance, group in by_importance.items():
        for param in group:
            key = (importance, param.module)
            availability[key] = availability.get(key, 0) + 1
    cell_quotas = cap_and_redistribute(cell_quotas, availability)

    picked: list[Param] = []
    for (importance, module), quota in sorted(cell_quotas.items()):
        group = sorted(
            (
                param
                for param in by_importance[importance]
                if param.module == module
            ),
            key=lambda param: param.param_id,
        )
        picked.extend(uniform_pick(group, quota))
    if len(picked) != total:
        raise ValueError(
            f"выбрано {len(picked)} параметров вместо {total}: квоты {cell_quotas}"
        )
    return sorted(picked, key=lambda param: param.param_id), {
        "importance_counts": importance_counts,
        "importance_quotas": importance_quotas,
        "cell_quotas": {f"{key[0]}/{key[1]}": value for key, value in sorted(cell_quotas.items())},
    }


def importance_order(importances: Sequence[str]) -> tuple[list[str], str]:
    """Порядок страт важности и **источник** этого порядка (печатается сборкой).

    Берётся объявленный порядок весов ``IMPORTANCE_WEIGHTS`` (он и калибруется).
    Значение, которого в объявленной шкале нет, — отказ: «соседние страты» для
    неизвестной страты не определены, и придумывать ей место нельзя.
    """
    from core.domain.scoring import IMPORTANCE_WEIGHTS

    weight = {str(key): value for key, value in IMPORTANCE_WEIGHTS.items()}
    unknown = sorted({item for item in importances if item not in weight})
    if unknown:
        raise ValueError(
            f"важность {unknown} отсутствует в IMPORTANCE_WEIGHTS — порядок страт "
            f"не определён; известны {sorted(weight)}"
        )
    ordered = sorted(
        {item for item in importances}, key=lambda item: (-weight[item], item)
    )
    return ordered, "IMPORTANCE_WEIGHTS (объявленная provisional-шкала)"


# ---------------------------------------------------------------------------
# Пары
# ---------------------------------------------------------------------------
def _pair_key(a: Param, b: Param) -> tuple[str, str]:
    """Неупорядоченный ключ пары: ``a|b`` с сортировкой, чтобы дубль был виден."""
    if a.param_id <= b.param_id:
        return (a.param_id, b.param_id)
    return (b.param_id, a.param_id)


def _orient(a: Param, b: Param) -> tuple[Param, Param]:
    """Детерминированно перемешивает ``a``/``b``: чётность хеша пары, не ``random``.

    Эксперт не должен видеть закономерности («слева всегда важнее»): иначе половина
    ответов перестаёт быть независимой.
    """
    digest = hashlib.sha256(f"{a.param_id}|{b.param_id}".encode()).digest()
    return (b, a) if digest[0] & 1 else (a, b)


def build_pairs(
    params: Sequence[Param],
    pairs_count: int,
    order: Sequence[str],
    max_uses: int = MAX_USES,
) -> tuple[list[tuple[Param, Param]], dict[str, Any]]:
    """Пары по объявленному правилу: половина внутри страты, половина между соседними.

    Правило (и его пределы):

    1. **Между соседними стратами** — ``pairs_count // 2`` пар, если это возможно.
       Возможность ограничена не желанием, а арифметикой: страта из ``k`` параметров
       даёт не более ``k * max_uses`` пар с соседом. При недостижимом 50/50 остаток
       добирается **внутри** страт, а расхождение печатается с числами — иначе прибор
       молча выдал бы другой дизайн эксперимента.
    2. **Внутри страты** — остаток: проверяет согласованность эксперта.
    3. Жадный выбор пары: минимально использованные параметры (``max(use)``, затем
       сумма), тай-брейк по ``param_id`` — так покрытие растёт равномерно, а результат
       воспроизводим.
    4. Ни один параметр не встречается больше ``max_uses`` раз; пара не повторяется.
    """
    if pairs_count <= 0:
        raise ValueError("пар должно быть больше нуля")
    use = {param.param_id: 0 for param in params}
    used: set[tuple[str, str]] = set()
    pairs: list[tuple[Param, Param]] = []

    def best_within_pair() -> tuple[Param, Param] | None:
        """Пара внутри одной страты: минимально использованные, тай-брейк по id."""
        best: tuple[tuple[int, int, str, str], tuple[Param, Param]] | None = None
        for index, first in enumerate(params):
            if use[first.param_id] >= max_uses:
                continue
            for second in params[index + 1 :]:
                if use[second.param_id] >= max_uses:
                    continue
                if first.importance != second.importance:
                    continue
                key = _pair_key(first, second)
                if key in used:
                    continue
                score = (
                    max(use[first.param_id], use[second.param_id]),
                    use[first.param_id] + use[second.param_id],
                    key[0],
                    key[1],
                )
                if best is None or score < best[0]:
                    best = (score, (first, second))
        return None if best is None else best[1]

    def best_neighbour_pair() -> tuple[Param, Param] | None:
        """Пара из двух соседних страт: минимально использованные с обеих сторон."""
        best: tuple[tuple[int, int, str, str], tuple[Param, Param]] | None = None
        for left, right in pairwise(order):
            for first in params:
                if first.importance != left or use[first.param_id] >= max_uses:
                    continue
                for second in params:
                    if second.importance != right or use[second.param_id] >= max_uses:
                        continue
                    key = _pair_key(first, second)
                    if key in used:
                        continue
                    score = (
                        max(use[first.param_id], use[second.param_id]),
                        use[first.param_id] + use[second.param_id],
                        key[0],
                        key[1],
                    )
                    if best is None or score < best[0]:
                        best = (score, (first, second))
        return None if best is None else best[1]

    target_between = pairs_count // 2
    between = 0
    while len(pairs) < target_between:
        chosen = best_neighbour_pair()
        if chosen is None:
            break
        pairs.append(chosen)
        used.add(_pair_key(*chosen))
        for param in chosen:
            use[param.param_id] += 1
        between += 1

    while len(pairs) < pairs_count:
        chosen = best_within_pair()
        if chosen is None:
            break
        pairs.append(chosen)
        used.add(_pair_key(*chosen))
        for param in chosen:
            use[param.param_id] += 1

    if len(pairs) != pairs_count:
        raise ValueError(
            f"пар {len(pairs)} вместо {pairs_count}: лимит {max_uses} использований и "
            f"размер страт {dict(Counter(param.importance for param in params))} "
            f"не позволяют больше; увеличьте --params или --pairs"
        )

    oriented = [_orient(first, second) for first, second in pairs]
    conditions = {
        "between": between,
        "within": len(pairs) - between,
        "target_between": target_between,
        "max_uses": max(use.values()) if use else 0,
        "params_used": sum(1 for value in use.values() if value),
        "params_total": len(params),
    }
    return oriented, conditions


# ---------------------------------------------------------------------------
# Ввод-вывод
# ---------------------------------------------------------------------------
def read_catalog(path: Path) -> list[Param]:
    """Каталог пакета → параметры; отсутствие колонки — отказ, а не пустое поле."""
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [
            name for name in REQUIRED_CATALOG_COLUMNS if name not in (reader.fieldnames or [])
        ]
        if missing:
            raise SystemExit(f"{path}: нет колонок {missing}")
        params = [
            Param(
                param_id=(row.get("param_id") or "").strip(),
                title=(row.get("title") or "").strip(),
                module=(row.get("module") or "").strip(),
                importance=(row.get("importance") or "").strip(),
                rule=(row.get("rule") or "").strip(),
            )
            for row in reader
        ]
    empty = [param.param_id for param in params if not param.param_id]
    if empty:
        raise SystemExit(f"{path}: строк без param_id — {len(empty)}")
    return params


def _openpyxl() -> Any:
    """``openpyxl`` — зависимость extra ``extract``; отказ объясняет, чего не хватает."""
    try:
        import openpyxl
    except ImportError as error:  # pragma: no cover - зависит от окружения
        raise SystemExit(
            "XLSX требует openpyxl (extra `extract`: uv sync --extra extract)"
        ) from error
    return openpyxl


def _column_letter(index: int) -> str:
    """1 → ``A``, 27 → ``AA``."""
    letters = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


def write_xlsx(path: Path, pairs: Sequence[Pair]) -> None:
    """Пары → XLSX для разметки: шапка с фильтром, список ``a/b/equal``, инструкция."""
    openpyxl = _openpyxl()
    from openpyxl.styles import Alignment, Font, PatternFill  # noqa: PLC0415 - ленивый импорт
    from openpyxl.worksheet.datavalidation import DataValidation  # noqa: PLC0415

    book = openpyxl.Workbook()
    sheet = book.active
    sheet.title = "лист"
    sheet.append(list(COLUMNS))
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = f"A1:{_column_letter(len(COLUMNS))}1"

    widths = {
        "pair_id": 10,
        "param_a": 40,
        "title_a": 34,
        "module_a": 12,
        "importance_a": 11,
        "rule_a": 50,
        "param_b": 40,
        "title_b": 34,
        "module_b": 12,
        "importance_b": 11,
        "rule_b": 50,
        "more_important": 15,
        "comment": 40,
    }
    wrap = {"param_a", "title_a", "rule_a", "param_b", "title_b", "rule_b", "comment"}
    for index, name in enumerate(COLUMNS, start=1):
        sheet.column_dimensions[_column_letter(index)].width = widths.get(name, 16)

    between_fill = PatternFill("solid", start_color="FFF2CC")
    for row_index, pair in enumerate(pairs, start=2):
        sheet.append([pair.as_row()[name] for name in COLUMNS])
        for index, name in enumerate(COLUMNS, start=1):
            if name in wrap:
                sheet.cell(row=row_index, column=index).alignment = Alignment(
                    wrap_text=True, vertical="top"
                )
        if pair.a.importance != pair.b.importance:
            # Пара между стратами проверяет **порядок**, а не согласованность: это
            # самые информативные строки, и их видно сразу.
            for index in range(1, len(COLUMNS) + 1):
                sheet.cell(row=row_index, column=index).fill = between_fill

    choice_index = COLUMNS.index("more_important") + 1
    validation = DataValidation(
        type="list",
        formula1=f'"{",".join(CHOICES)}"',
        allow_blank=True,
        showDropDown=False,
    )
    validation.error = "more_important — a, b или equal; пусто = ещё не размечено"
    validation.errorTitle = "Неизвестное значение"
    sheet.add_data_validation(validation)
    last = max(2, len(pairs) + 1)
    validation.add(f"{_column_letter(choice_index)}2:{_column_letter(choice_index)}{last}")

    guide = book.create_sheet("как размечать")
    for line in GUIDE_LINES:
        guide.append([line])
    guide.column_dimensions["A"].width = 110
    for cell in guide["A"]:
        cell.alignment = Alignment(wrap_text=True, vertical="top")

    path.parent.mkdir(parents=True, exist_ok=True)
    book.save(path)


#: Что означает выбор. Лежит вторым листом книги: файл переживает переписку.
GUIDE_LINES: tuple[str, ...] = (
    "Размечается колонка more_important — выпадающий список a / b / equal. "
    "Пусто = ещё не размечено. comment — свободный текст.",
    "",
    "Критерий один: какой из двух параметров важнее для сравнения продуктов по "
    "функциональности? Не «какой интереснее», не «какой легче измерить» и не «какой "
    "чаще встречается». Сравниваются параметры как признаки различия между продуктами.",
    "",
    "equal разрешён и полезен: он и есть проверка, ординален ли текущий порядок весов "
    "(Кр > Вж > Баз > Опц). Если два параметра одинаково важны, ставьте equal, а не "
    "выбирайте «наугад».",
    "",
    "Если параметры несравнимы по природе (например, факт о лицензии против "
    "характеристики производительности) — напишите это в comment, но выбор всё равно "
    "сделайте: пропуск не даёт данных, а комментарий объясняет, почему ответ условен.",
    "",
    "importance_a/importance_b — текущая provisional-важность из каталога; она не "
    "обязана совпадать с вашим ответом, и расхождение — это и есть измеряемый факт.",
    "rule_a/rule_b — правило засчитывания параметра: из него видно, что именно "
    "сравнивается (наличие, значение, поведение).",
    "",
    "Залитые строки — пары между разными стратами важности: они проверяют порядок, а не "
    "согласованность, и информативнее всего для калибровки.",
    "",
    "Выборка детерминированная: 60 параметров, пропорционально importance и не менее "
    "одного от каждого модуля; внутри страты — сортировка по param_id и равномерный "
    "шаг. 60 пар: половина внутри страт, половина между соседними (если это возможно "
    "при лимите 3 использования на параметр — предел печатает build). Порядок a/b "
    "перемешан по хешу пары, чтобы не читалась закономерность.",
    "",
    "Когда лист заполнен: python scripts/prepare_importance_pairs.py validate "
    "--xlsx <файл>, затем summary — он печатает распределение и список equal-пар.",
)


def read_xlsx(path: Path) -> list[dict[str, str]]:
    """XLSX → строки первого листа; значения читаются как текст."""
    openpyxl = _openpyxl()
    book = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = book.worksheets[0]
        iterator = sheet.iter_rows(values_only=True)
        try:
            header = [("" if cell is None else str(cell)).strip() for cell in next(iterator)]
        except StopIteration:
            raise SystemExit(f"{path}: пустой лист") from None
        missing = [name for name in COLUMNS if name not in header]
        if missing:
            raise SystemExit(f"{path}: нет колонок {missing}")
        out: list[dict[str, str]] = []
        for values in iterator:
            if not any(value not in (None, "") for value in values):
                continue
            row = {name: "" for name in header if name}
            for index, name in enumerate(header):
                if not name or index >= len(values):
                    continue
                value = values[index]
                row[name] = "" if value is None else str(value).strip()
            out.append(row)
        return out
    finally:
        book.close()


# ---------------------------------------------------------------------------
# validate / summary
# ---------------------------------------------------------------------------
def validate_rows(
    rows: Sequence[dict[str, str]], expect: int, max_uses: int = MAX_USES
) -> list[str]:
    """Проверки заполненного листа. Возвращает список проблем (пусто — лист годен)."""
    problems: list[str] = []
    seen_pairs: dict[tuple[str, str], int] = {}
    uses: Counter[str] = Counter()
    for lineno, row in enumerate(rows, start=2):
        first = (row.get("param_a") or "").strip()
        second = (row.get("param_b") or "").strip()
        if not first or not second:
            problems.append(f"строка {lineno}: пустой param_a/param_b ({first!r}, {second!r})")
            continue
        if first == second:
            problems.append(f"строка {lineno}: пара из одного параметра {first}")
        key = tuple(sorted((first, second)))
        if key in seen_pairs:  # type: ignore[arg-type]
            problems.append(
                f"строка {lineno}: дубль пары {first}/{second} "
                f"(первая — строка {seen_pairs[key]})"
            )
        else:
            seen_pairs[key] = lineno  # type: ignore[index]
        uses[first] += 1
        uses[second] += 1
        value = (row.get("more_important") or "").strip()
        if not value:
            problems.append(f"строка {lineno}: more_important не заполнен ({first}/{second})")
        elif value not in CHOICES:
            problems.append(
                f"строка {lineno}: more_important={value!r} не из {CHOICES} "
                f"({first}/{second})"
            )
    over = sorted((param, count) for param, count in uses.items() if count > max_uses)
    for param, count in over:
        problems.append(f"параметр {param} встречается {count} раз(а), лимит {max_uses}")
    if len(rows) != expect:
        problems.append(f"пар {len(rows)}, ожидалось {expect}")
    return problems


def summarize(rows: Sequence[dict[str, str]]) -> dict[str, Any]:
    """Распределение ответов, число ``equal`` и сами пары с ``equal``."""
    counts = Counter((row.get("more_important") or "").strip() for row in rows)
    uses: Counter[str] = Counter()
    for row in rows:
        for field in ("param_a", "param_b"):
            value = (row.get(field) or "").strip()
            if value:
                uses[value] += 1
    between = sum(
        1
        for row in rows
        if (row.get("importance_a") or "") != (row.get("importance_b") or "")
    )
    return {
        "pairs": len(rows),
        "choices": {value: counts[value] for value in CHOICES},
        "unmarked": counts[""],
        "equal": [
            {
                "pair_id": row.get("pair_id", ""),
                "a": row.get("param_a", ""),
                "title_a": row.get("title_a", ""),
                "b": row.get("param_b", ""),
                "title_b": row.get("title_b", ""),
            }
            for row in rows
            if (row.get("more_important") or "").strip() == "equal"
        ],
        "params": len(uses),
        "max_uses": max(uses.values()) if uses else 0,
        "between": between,
        "within": len(rows) - between,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def cmd_build(args: argparse.Namespace) -> int:
    catalog = Path(args.catalog).expanduser()
    if not catalog.is_file():
        print(f"build: каталог не найден: {catalog}", file=sys.stderr)
        return 2
    params = read_catalog(catalog)
    try:
        order, order_source = importance_order([param.importance for param in params])
        selected, selection = select_params(params, args.params)
        pairs, conditions = build_pairs(selected, args.pairs, order)
    except ValueError as error:
        print(f"build: {error}", file=sys.stderr)
        return 2

    out = Path(args.out).expanduser()
    write_xlsx(
        out,
        [
            Pair(pair_id=f"pair_{index:03d}", a=first, b=second)
            for index, (first, second) in enumerate(pairs, start=1)
        ],
    )

    print(f"каталог {catalog}: параметров {len(params)}")
    print(
        "страты по важности в каталоге: "
        + ", ".join(
            f"{key} {value}" for key, value in sorted(selection["importance_counts"].items())
        )
        + " → квоты "
        + ", ".join(
            f"{key} {value}" for key, value in sorted(selection["importance_quotas"].items())
        )
    )
    print("квоты (важность/модуль): " + ", ".join(
        f"{key} {value}" for key, value in selection["cell_quotas"].items()
    ))
    print(
        "отбор: сортировка по param_id, равномерный шаг round(i*(m-1)/(k-1)) внутри "
        "страты (важность, модуль); random не используется"
    )
    print(f"порядок страт: {' > '.join(order)} — источник: {order_source}")
    print(
        f"пары: {conditions['between']} между соседними стратами (цель "
        f"{conditions['target_between']}), {conditions['within']} внутри страт; "
        f"параметров покрыто {conditions['params_used']} из {conditions['params_total']}, "
        f"максимум использований {conditions['max_uses']}"
    )
    if conditions["between"] < conditions["target_between"]:
        smaller = min(selection["importance_quotas"].items(), key=lambda item: item[1])
        print(
            f"ВНИМАНИЕ: 50/50 недостижимо: меньшая страта {smaller[0]} — {smaller[1]} "
            f"параметров, при лимите {MAX_USES} использований она даёт не более "
            f"{smaller[1] * MAX_USES} пар с соседом; остаток добрано внутри страт",
            file=sys.stderr,
        )
    print(f"written: {out} ({len(pairs)} пар)")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    path = Path(args.xlsx).expanduser()
    if not path.is_file():
        print(f"validate: файл не найден: {path}", file=sys.stderr)
        return 2
    rows = read_xlsx(path)
    problems = validate_rows(rows, args.pairs)
    if problems:
        for problem in problems:
            print(f"ОТКАЗ: {problem}", file=sys.stderr)
        print(f"{path}: {len(problems)} проблем(ы)", file=sys.stderr)
        return 2
    print(f"OK: {path}: {len(rows)} пар, все more_important из {CHOICES}")
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    path = Path(args.xlsx).expanduser()
    if not path.is_file():
        print(f"summary: файл не найден: {path}", file=sys.stderr)
        return 2
    report = summarize(read_xlsx(path))
    print(f"пар: {report['pairs']} (между стратами {report['between']}, внутри {report['within']})")
    print(
        "ответы: "
        + ", ".join(f"{value} {report['choices'][value]}" for value in CHOICES)
        + f", не размечено {report['unmarked']}"
    )
    print(
        f"параметров покрыто {report['params']}, максимум использований {report['max_uses']}"
    )
    if not report["equal"]:
        print("equal-пар нет — разбор согласованности пуст (либо лист не размечен)")
        return 0
    print(f"equal: {len(report['equal'])} — первыми в разбор:")
    for item in report["equal"]:
        print(
            f"  {item['pair_id']}: {item['a']} ({item['title_a']}) == "
            f"{item['b']} ({item['title_b']})"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="построить лист пар (XLSX)")
    build.add_argument("--catalog", required=True, help="catalog.csv пакета")
    build.add_argument("--out", required=True, help="выходной XLSX")
    build.add_argument("--params", type=int, default=60, help="параметров в выборке")
    build.add_argument("--pairs", type=int, default=60, help="пар на разметку")
    build.set_defaults(func=cmd_build)

    validate = sub.add_parser("validate", help="проверить заполненный лист")
    validate.add_argument("--xlsx", required=True)
    validate.add_argument("--pairs", type=int, default=60, help="ожидаемое число пар")
    validate.set_defaults(func=cmd_validate)

    summary = sub.add_parser("summary", help="распределение ответов и equal-пары")
    summary.add_argument("--xlsx", required=True)
    summary.set_defaults(func=cmd_summary)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
