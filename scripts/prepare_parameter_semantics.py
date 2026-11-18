#!/usr/bin/env python3
"""Разметка семантики параметров пакета: JSONL → XLSX для человека → обратно.

**Зачем.** `catalog.csv` профиля `ru-pg-dbms` собран генератором
`sources/build_from_golden_00.py` из сравнительной таблицы, в которой каталога не было:
`rule` — две захардкоженные строки на 181 параметр, `concept_terms` — разбиение
`title` на слова, `absence_rule` — `explicit` константой. Отсюда два следствия,
измеренных 18.11.2026: recon подтверждал 45 параметров из 181 цитатой, где совпало
служебное слово (`parameter_terms` берёт `title` + `concept_terms`, а `title`
начинается с «Общие…», «Наличие…»), и `❌` публиковался как прямое отрицание вендора
там, где вендор ничего не отрицал.

Этот прибор — **артефакт для человека**, а не замер (§3 AGENTS): он пишет XLSX-лист
на разметку и собирает результат обратно, отказывая на неразмеченной или изменённой
без пометки строке. Разметку не выдумывает: `candidate` в имени файла и
`status: candidate` в легенде — состояние «человек ещё не подтвердил».

**Что в файле.** По параметру: `semantics` (что параметр спрашивает на языке
предметной области), `rule` (правило зачёта ✅/⚠️/❌ — оно уходит в промпт оценщика
слотом `RULE`), `evidence` (где доказуемо), `absence_rule` (чем доказуем `❌`,
I2b/I2c/I2d), `concept_terms` (термины ретривала; второй потребитель —
`render_queries` в Ш6), `confidence` и `question` (что подтвердить человеку).

**Подсветка.** `confidence=low` — красным (семантика под вопросом), `medium` —
жёлтым (правило работает, но граница ⚠️/❌ требует решения человека).

**`apply` не переписывает пакет молча.** Он собирает `catalog.csv` из подтверждённых
записей, меняя только `rule`/`absence_rule`/`concept_terms`, и требует `--out`: правка
каталога — `execution_change` (дайджест каталога входит в `run_id`), и это решение
человека, а не побочный эффект прибора.

**`absence_rule` меняет карту, и это не косметика.** `explicit` доказуем цитатой
отрицания вендора и не требует подписи; `parameter_reference` и
`distribution_absence` публикуются как `❌` только при `bounded`, то есть при
`scope_acceptance` (подпись человека на риск) и датированном носителе (I2d). Для
`ru-pg-dbms` смена класса с `explicit` на честный превращает 245 из 282 `❌` в `❓`
до подписания — число печатает `build`.

Usage:
    python3 scripts/prepare_parameter_semantics.py build \
        --data packages/ru-pg-dbms/sources/parameter_semantics.jsonl \
        --package packages/ru-pg-dbms \
        --out packages/ru-pg-dbms/sources/parameter_semantics.candidate.xlsx
    python3 scripts/prepare_parameter_semantics.py validate --data …
    python3 scripts/prepare_parameter_semantics.py compile \
        --xlsx …candidate.xlsx --data … --out …reviewed.jsonl
    python3 scripts/prepare_parameter_semantics.py apply \
        --data …reviewed.jsonl --out /tmp/catalog.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

#: Продукты пакета и порядок колонок в XLSX — как в сравнительной таблице.
PRODUCTS: tuple[tuple[str, str], ...] = (
    ("digital_q_database", "Digital Q"),
    ("pangolin", "Pangolin"),
    ("postgrespro", "Postgres Pro"),
    ("tantor", "Tantor"),
    ("jatoba", "Jatoba"),
)

#: Порядок и заголовки колонок листа `semantics`.
COLUMNS: tuple[tuple[str, str, int], ...] = (
    ("n", "№", 5),
    ("param_id", "param_id", 34),
    ("title", "Название параметра (дословно)", 46),
    ("section", "Раздел таблицы", 24),
    ("param_class", "Класс параметра", 12),
    ("importance", "Важн.", 7),
    ("semantics", "Семантика: что параметр спрашивает", 60),
    ("rule", "Правило зачёта ✅ / ⚠️ / ❌", 70),
    ("evidence", "Где доказуемо", 40),
    ("absence_rule", "absence_rule (чем доказуем ❌)", 22),
    ("terms_ru", "Термины ru", 34),
    ("terms_en", "Термины en", 34),
    ("confidence", "Уверенность", 12),
    ("question", "Вопрос на подтверждение", 46),
)

#: Классы параметра: что именно оценивается.
PARAM_CLASSES = ("value", "capability", "offer", "gui_feature")
CONFIDENCE = ("high", "medium", "low")

#: Порядок проверки: сомнительное — первым (`review_order`).
REVIEW_PRIORITY = ("low", "medium")
ABSENCE_RULES = ("explicit", "parameter_reference", "distribution_absence", "registry_absence")

FILL_LOW = "FFC7CE"
FILL_MEDIUM = "FFEB9C"
STATUS_MARKS = {"✅": "✅", "⚠️": "⚠️", "❌": "❌"}

LEGEND: tuple[str, ...] = (
    "СОСТОЯНИЕ: candidate — разметка машинная, человек её не подтверждал.",
    "",
    "КАК ЧИТАТЬ КОЛОНКИ",
    "semantics — что параметр спрашивает на языке предметной области (СУБД на PostgreSQL),",
    "  а не что ответила сравнительная таблица. Это вход для оценщика и для человека.",
    "rule — правило зачёта, уходит в промпт оценщика слотом RULE: что считается ✅, что ⚠️,",
    "  что ❌ и что НЕ засчитывается. Меняется здесь, а не в коде.",
    "evidence — где факт доказуем: документ вендора, представление каталога, реестр, утилита.",
    "terms_ru / terms_en — термины ретривала (3 запроса на ячейку: title + до двух терминов).",
    "  Требование: термин отличает параметр, а не повторяет общее слово из названия.",
    "question — что именно требует решения человека; пусто = вопросов нет.",
    "",
    "УВЕРЕННОСТЬ (подсветка строки)",
    "low (красный) — семантика под вопросом: у параметра есть вендорское значение, которого",
    "  нет в общем языке; без ответа на question правило будет угадано.",
    "medium (жёлтый) — семантика ясна, граница ⚠️/❌ спорна (например, «частично» против «нет»).",
    "high (без заливки) — параметр читается однозначно.",
    "",
    "absence_rule — ЧЕМ ДОКАЗУЕМ ❌ (I2b, I2c, I2d)",
    "explicit — прямым отрицанием вендора с цитатой. Подписи человека не требует.",
    "parameter_reference — ссылкой на объявленный перечень (ОС, типы секционирования, команды",
    "  SQL, форматы, расширения, пакеты). Публикуется как ❌ только при bounded, то есть",
    "  когда человек принял полноту перечня (scope_acceptance.yaml пакета, по продукту).",
    "distribution_absence — отсутствием механизма во всём корпусе документации продукта.",
    "  Тоже требует bounded: датированного снятия каждого объявленного корня и подписи риска.",
    "registry_absence — внешним реестром (ФСТЭК, каталог 1С, Гостех). Подписи не требует.",
    "",
    "ЧТО ДЕЛАЕТ СМЕНА КЛАССА (числа печатает build)",
    "В каталоге, собранном генератором, 177 параметров объявлены explicit — то есть каждый",
    "❌ якобы опирается на прямое отрицание вендора. Это не так: генератор поставил",
    "константу. При честном классе ❌ по distribution_absence/parameter_reference без",
    "подписанного scope_acceptance не публикуется и становится ❓ (not_stated) — так",
    "работает I2c/I2d. Это видимая потеря, а не регресс: карта перестаёт утверждать то,",
    "чего не измеряла.",
    "",
    "ПОРЯДОК ПРОВЕРКИ",
    "Лист review_order — только неуверенные строки (low сверху, затем medium): это и есть",
    "очередь. Лист semantics — все 181 в порядке сравнительной таблицы, с фильтром и заливкой.",
    "1. Строки с низкой уверенностью — первыми: ответ на question либо правка semantics/rule.",
    "2. Затем жёлтые: решить границу ⚠️/❌ (часто это «частично» против «нет»).",
    "3. Заполнить review: accept — как есть; fix — исправлено (правка видна в колонках);",
    "   reject — параметр сформулирован неверно, нужен новый текст от человека.",
    "4. compile откажет на строке без review и на правке без review=fix.",
)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        text = line.strip()
        if not text:
            continue
        try:
            records.append(json.loads(text))
        except json.JSONDecodeError as exc:  # pragma: no cover — диагностика входа
            raise SystemExit(f"{path}:{number}: не разобрать JSON: {exc}") from exc
    return records


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _catalog_by_number(package: Path) -> dict[int, dict[str, str]]:
    """Строки каталога по номеру параметра: `xlsx_007_…` → 7."""
    rows: dict[int, dict[str, str]] = {}
    for row in _read_csv(package / "catalog.csv"):
        prefix = row["param_id"].split("_")
        if len(prefix) < 2 or not prefix[1].isdigit():
            continue
        rows[int(prefix[1])] = row
    return rows


def _golden_cells(package: Path) -> dict[int, dict[str, dict[str, str]]]:
    """Исходные ячейки сравнительной таблицы: номер → продукт → строка."""
    path = package / "sources" / "golden_00_cells.csv"
    cells: dict[int, dict[str, dict[str, str]]] = {}
    for row in _read_csv(path):
        cells.setdefault(int(row["excel_number"]), {})[row["product_id"]] = row
    return cells


def validate(records: list[dict[str, Any]], package: Path) -> list[str]:
    """Проверки разметки. Возвращает список проблем (пустой — всё в порядке)."""
    problems: list[str] = []
    catalog = _catalog_by_number(package)
    if len(records) != len(catalog):
        problems.append(f"записей {len(records)}, параметров в каталоге {len(catalog)}")
    seen: set[int] = set()
    for record in records:
        n = record.get("n")
        if not isinstance(n, int) or n not in catalog:
            problems.append(f"n={n!r}: нет такого параметра в catalog.csv")
            continue
        if n in seen:
            problems.append(f"n={n}: дубликат")
        seen.add(n)
        if record.get("param_id") != catalog[n]["param_id"]:
            problems.append(f"n={n}: param_id не совпадает с каталогом")
        for field in ("semantics", "rule", "evidence"):
            if not str(record.get(field) or "").strip():
                problems.append(f"n={n}: пустое поле {field}")
        rule = str(record.get("rule") or "")
        # Параметр-значение отвечает не «да/нет», а величиной: ✅/❌ в его правиле нет
        # по построению, и требовать их — значит заставлять автора писать «засчитывается
        # значение» словами оценщика. Для остальных классов правило обязано называть
        # оба края шкалы.
        if record.get("param_class") != "value":
            for mark in ("✅", "❌"):
                if mark not in rule:
                    problems.append(f"n={n}: rule не описывает {mark}")
        if record.get("param_class") not in PARAM_CLASSES:
            problems.append(f"n={n}: param_class={record.get('param_class')!r}")
        if record.get("confidence") not in CONFIDENCE:
            problems.append(f"n={n}: confidence={record.get('confidence')!r}")
        if record.get("absence_rule") not in ABSENCE_RULES:
            problems.append(f"n={n}: absence_rule={record.get('absence_rule')!r}")
        if record.get("confidence") == "low" and not str(
            record.get("question") or ""
        ).strip():
            problems.append(f"n={n}: confidence=low без question")
        # Дубликаты ищутся внутри языка: латинская аббревиатура законно стоит в обоих
        # списках, и это не повтор, а один и тот же термин для двух языков запроса.
        for language in ("terms_ru", "terms_en"):
            values = [str(term).strip().casefold() for term in record.get(language) or []]
            if not values:
                problems.append(f"n={n}: нет терминов ({language})")
            if len(set(values)) != len(values):
                problems.append(f"n={n}: повтор термина в {language}")
    missing = set(range(1, len(catalog) + 1)) - seen
    if missing:
        problems.append(f"нет записей для номеров: {sorted(missing)}")
    return problems


def _absence_consequence(
    records: list[dict[str, Any]], package: Path
) -> Counter[str]:
    """Сколько `❌` приходится на каждый класс отсутствия (цена смены класса)."""
    golden = _read_csv(package / "golden.csv")
    status = {(row["param_id"], row["product_id"]): row["expected_status"] for row in golden}
    counts: Counter[str] = Counter()
    for record in records:
        for product, _label in PRODUCTS:
            if status.get((record["param_id"], product)) == "❌":
                counts[record["absence_rule"]] += 1
    return counts


def build(records: list[dict[str, Any]], package: Path, out: Path) -> None:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    catalog = _catalog_by_number(package)
    cells = _golden_cells(package)
    wb = Workbook()
    sheet = wb.active
    sheet.title = "semantics"

    header_font = Font(bold=True)
    wrap = Alignment(wrap_text=True, vertical="top")
    fills = {"low": PatternFill("solid", fgColor=FILL_LOW),
             "medium": PatternFill("solid", fgColor=FILL_MEDIUM)}

    head: list[str] = [label for _key, label, _w in COLUMNS]
    head += [label for _pid, label in PRODUCTS]
    head += ["review", "review_note"]
    sheet.append(head)
    for index, (_key, _label, width) in enumerate(COLUMNS, 1):
        sheet.column_dimensions[get_column_letter(index)].width = width
        sheet.cell(row=1, column=index).font = header_font
        sheet.cell(row=1, column=index).alignment = Alignment(
            wrap_text=True, vertical="center"
        )
    for offset in range(len(PRODUCTS)):
        letter = get_column_letter(len(COLUMNS) + offset + 1)
        sheet.column_dimensions[letter].width = 11
    review_column = len(COLUMNS) + len(PRODUCTS) + 1
    sheet.column_dimensions[get_column_letter(review_column)].width = 11
    sheet.column_dimensions[get_column_letter(review_column + 1)].width = 34

    for record in records:
        n = record["n"]
        row = catalog[n]
        values: list[Any] = [
            n,
            record["param_id"],
            row["title"],
            (cells.get(n) or {}).get(PRODUCTS[0][0], {}).get("section", ""),
            record["param_class"],
            row["importance"],
            record["semantics"],
            record["rule"],
            record["evidence"],
            record["absence_rule"],
            "; ".join(record.get("terms_ru") or []),
            "; ".join(record.get("terms_en") or []),
            record["confidence"],
            record.get("question") or "",
        ]
        for product, _label in PRODUCTS:
            cell = (cells.get(n) or {}).get(product, {})
            values.append(STATUS_MARKS.get(cell.get("normalized_status", ""), ""))
        values += ["", ""]
        sheet.append(values)
        line = sheet.max_row
        fill = fills.get(str(record["confidence"]))
        for column in range(1, len(head) + 1):
            target = sheet.cell(row=line, column=column)
            target.alignment = wrap
            if fill is not None:
                target.fill = fill
    sheet.freeze_panes = "D2"
    sheet.auto_filter.ref = f"A1:{get_column_letter(len(head))}{sheet.max_row}"

    raw = wb.create_sheet("golden_00")
    raw.append(["№", "Название", "Продукт", "Статус", "Значение в таблице", "Комментарий", "Ссылка"])
    for width, letter in zip((5, 46, 18, 8, 30, 60, 60), "ABCDEFG", strict=False):
        raw.column_dimensions[letter].width = width
    for number in sorted(cells):
        for product, label in PRODUCTS:
            cell = cells[number].get(product)
            if cell is None:
                continue
            raw.append([
                number,
                cell["excel_title"],
                label,
                STATUS_MARKS.get(cell["normalized_status"], cell["normalized_status"]),
                cell["raw_value"],
                cell["source_comment"],
                cell["source_url"] or cell["hyperlink_target"],
            ])
    raw.freeze_panes = "A2"
    raw.auto_filter.ref = f"A1:G{raw.max_row}"

    # Лист проверки: только неуверенные строки, `low` первыми. Основной лист идёт в
    # порядке сравнительной таблицы (разделы значимы), но человек, у которого есть
    # полчаса, не должен добывать порядок фильтром — он открывает этот лист и идёт сверху.
    review = wb.create_sheet("review_order")
    review.append([label for _key, label, _width in COLUMNS[:3]] + ["Уверенность", "Вопрос", "Семантика", "Правило"])
    review_order = sorted(
        (record for record in records if record["confidence"] != "high"),
        key=lambda record: (REVIEW_PRIORITY.index(record["confidence"]), record["n"]),
    )
    for record in review_order:
        review.append([
            record["n"],
            record["param_id"],
            catalog[record["n"]]["title"],
            record["confidence"],
            record.get("question") or "",
            record["semantics"],
            record["rule"],
        ])
        line = review.max_row
        fill = fills[record["confidence"]]
        for column in range(1, 8):
            target = review.cell(row=line, column=column)
            target.alignment = wrap
            target.fill = fill
    for letter, width in zip("ABCDEFG", (5, 34, 46, 12, 60, 60, 70), strict=True):
        review.column_dimensions[letter].width = width
    review.freeze_panes = "A2"
    review.auto_filter.ref = f"A1:G{review.max_row}"

    legend = wb.create_sheet("legend")
    legend.column_dimensions["A"].width = 118
    for text in LEGEND:
        legend.append([text])
    legend["A1"].font = Font(bold=True)

    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)


def compile_review(xlsx: Path, records: list[dict[str, Any]], out: Path) -> None:
    """Переносит решения человека из XLSX в JSONL, отказывая на неподтверждённом."""
    from openpyxl import load_workbook

    wb = load_workbook(xlsx, data_only=True)
    sheet = wb["semantics"]
    header = [str(cell.value or "") for cell in sheet[1]]
    index = {name: pos for pos, name in enumerate(header)}
    by_number = {record["n"]: record for record in records}
    problems: list[str] = []
    merged = 0
    for row in sheet.iter_rows(min_row=2, values_only=True):
        if not row or row[index["№"]] is None:
            continue
        n = int(row[index["№"]])
        record = by_number.get(n)
        if record is None:
            problems.append(f"n={n}: нет в исходном JSONL")
            continue
        review = str(row[index["review"]] or "").strip().lower()
        if review not in {"accept", "fix", "reject"}:
            problems.append(f"n={n}: review не заполнен")
            continue
        edited = {
            "semantics": row[index["Семантика: что параметр спрашивает"]],
            "rule": row[index["Правило зачёта ✅ / ⚠️ / ❌"]],
        }
        changed = [
            field for field, value in edited.items()
            if str(value or "").strip() != str(record[field]).strip()
        ]
        if changed and review != "fix":
            problems.append(f"n={n}: изменено {changed} без review=fix")
            continue
        for field in changed:
            record[field] = str(edited[field]).strip()
        record["review"] = review
        record["review_note"] = str(row[index["review_note"]] or "").strip()
        merged += 1
    if problems:
        _fail("compile", problems)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        for record in sorted(by_number.values(), key=lambda item: item["n"]):
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"сведено записей: {merged} → {out}")


def _fail(title: str, problems: list[str]) -> None:
    """Отказ прибора: первые десять адресов и полное число.

    Печатать все 181 строки — значит утопить настоящий дефект в списке: человек читает
    первые строки, а «сколько всего» берёт из итога.
    """
    head = "\n  ".join(problems[:10])
    tail = f"\n  … и ещё {len(problems) - 10}" if len(problems) > 10 else ""
    raise SystemExit(f"{title} отказал ({len(problems)}):\n  {head}{tail}")


def apply_catalog(records: list[dict[str, Any]], package: Path, out: Path) -> None:
    """Собирает `catalog.csv` из размеченных записей, не трогая остальные колонки.

    Меняются ровно четыре поля — `rule`, `absence_rule`, `concept_terms` и (если задан)
    `title`; `module`/`group`/`importance`/`param_type`/`role_binding`/`search_langs`
    берутся из текущего каталога как есть. Иначе разметка семантики незаметно
    переписала бы состав пакета: важность и роль — отдельные авторские решения со своими
    дайджестами (`catalog_digest` входит в `run_id`), и прибор, который их «заодно»
    пересчитал, дал бы новый `run_id` без нового решения.

    Отказывает на записи без `review`: `compile` — единственный вход, и он ставит её
    только по решению человека.
    """
    catalog = _catalog_by_number(package)
    columns = [
        "param_id", "title", "module", "group", "importance", "param_type",
        "rule", "absence_rule", "role_binding", "concept_terms", "search_langs",
    ]
    rows: list[dict[str, str]] = []
    problems: list[str] = []
    for record in sorted(records, key=lambda item: item["n"]):
        if record.get("review") not in {"accept", "fix"}:
            problems.append(f"n={record['n']}: review={record.get('review')!r}")
            continue
        source = catalog[record["n"]]
        row = {column: source.get(column, "") for column in columns}
        row["rule"] = str(record["rule"])
        row["absence_rule"] = str(record["absence_rule"])
        row["concept_terms"] = json.dumps(
            {"ru": list(record.get("terms_ru") or []), "en": list(record.get("terms_en") or [])},
            ensure_ascii=False,
        )
        rows.append(row)
    if problems:
        _fail("apply", problems)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    print(f"каталог собран: {len(rows)} строк → {out}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "validate", "compile", "apply"))
    parser.add_argument("--data", type=Path, required=True, help="JSONL разметки")
    parser.add_argument(
        "--package", type=Path, default=Path("packages/ru-pg-dbms"), help="пакет (catalog/golden)"
    )
    parser.add_argument("--out", type=Path, help="XLSX (build) или JSONL (compile)")
    parser.add_argument("--xlsx", type=Path, help="XLSX с решениями человека (compile)")
    args = parser.parse_args(argv)

    records = _read_jsonl(args.data)
    problems = validate(records, args.package)
    if problems:
        print("разметка не прошла проверку:", file=sys.stderr)
        for problem in problems:
            print("  " + problem, file=sys.stderr)
        return 1

    if args.command == "validate":
        print(f"проверка пройдена: {len(records)} параметров")
        return 0
    if args.command == "build":
        if args.out is None:
            parser.error("build требует --out")
        build(records, args.package, args.out)
        consequence = _absence_consequence(records, args.package)
        total = sum(consequence.values())
        print(f"записано {len(records)} параметров → {args.out}")
        print("уверенность: " + ", ".join(
            f"{name} {sum(1 for r in records if r['confidence'] == name)}"
            for name in CONFIDENCE
        ))
        print("класс отсутствия: " + ", ".join(
            f"{name} {sum(1 for r in records if r['absence_rule'] == name)}"
            for name in ABSENCE_RULES
        ))
        gated = consequence.get("distribution_absence", 0) + consequence.get(
            "parameter_reference", 0
        )
        print(
            f"❌ всего {total}: {gated} требуют scope_acceptance ("
            f"distribution_absence {consequence.get('distribution_absence', 0)}, "
            f"parameter_reference {consequence.get('parameter_reference', 0)}), "
            f"registry_absence {consequence.get('registry_absence', 0)} — по реестру, "
            "подписи не требуют"
        )
        return 0
    if args.command == "compile":
        if args.out is None or args.xlsx is None:
            parser.error("compile требует --xlsx и --out")
        compile_review(args.xlsx, records, args.out)
        return 0
    if args.out is None:
        parser.error("apply требует --out (перезапись catalog.csv на месте запрещена)")
    apply_catalog(records, args.package, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
