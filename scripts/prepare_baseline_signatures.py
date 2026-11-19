#!/usr/bin/env python3
"""Разметка подписей под фактами предка: JSONL → XLSX для человека → обратно.

**Зачем.** `hound baseline recon` записывает факты с `review=pending`, а наследует
только `accepted` (`inheritance.decide_inheritance`). Подпись — решение человека, и
прибор её не подставляет. Пока подписей нет, `baselines.csv` пуст или почти пуст, и
весь слой фактов работает вхолостую.

После пере-разведки 19.11.2026 (`--all-versions`, правило фразы) `present`-фактов
**255** на 8 версий, и читать их в CSV по 12 колонкам — не работа, а наказание: цитата
рвётся по ширине, соседние версии одного параметра не видны, и непонятно, какие
цитаты вообще стоит читать. Этот прибор — **артефакт для человека** (§3 AGENTS), а не
замер: он строит лист, где на параметр видны все его версии подряд, и подсвечивает
строки, где цитата похожа на шум, а не на доказательство.

**Что подсвечивается** (и почему):

* `low` (красный) — цитата из **предметного указателя** (`bookindex`/`genindex`) или
  короче 25 символов, или не содержит ни одной фразы параметра **у факта, снятого
  фразовым гейтом**: там отсутствие фразы означает, что прибор не воспроизводит
  собственную цитату, — это противоречие, а не спор о смысле. Указатель перечисляет
  разделы, а не утверждает свойство: `- point-in-time recovery, Backup and Restore`
  доказывает, что такая глава есть, и ничего — о продукте.
* `medium` (жёлтый) — цитата похожа на **голый заголовок** (≤5 слов, без точки и
  двоеточия), либо параметр `present` на 11 и отсутствует на 18 (шум отбора, а не
  регрессия — замер 19.11.2026: 4 случая, все с цитатой не о предмете), либо цитата
  делится с другим параметром той же версии (сейчас 0: правило фразы это вылечило, но
  проверка остаётся, иначе регрессия правила пройдёт незаметно).
* `high` (без заливки) — цитата — фраза с утверждением.

**Правило подсветки зависит от прибора, и это не косметика** (20.11.2026). После перехода
на `--classifier llm` зачёт решает оценщик по смыслу, и цитата законно может не содержать
ни одного объявленного термина: замер на версии 18 — **16 `present`-цитат из 76** не
содержат ни одного термина параметра (например, «Прозрачное для приложения защитное
преобразование данных» подтверждено фразой `The pgcrypto module allows certain fields to be
stored encrypted`). Прежнее правило красило бы такие строки в красный — то есть
**инструмент разметки спорил бы с прибором, который снял наблюдение**, и очередь из 65
красных строк из 76 не давала бы человеку никакого сигнала. Поэтому:

* фразовый гейт (`observed_by=phrase-1`): нет фразы → `low` (прибор не воспроизводит себя);
* оценщик (`observed_by=llm:…`): нет ни одного термина → `medium` с причиной
  «нет термина параметра (оценщик зачёл смыслом)» — читать стоит, но это не брак.

**Чего прибор не делает.** Он не заполняет `decision` и не считает «очевидные» строки
подписанными: подпись без человека неотличима от автоматики. Он лишь говорит, какие
строки читать первыми (`review_order`) и сколько их.

**`apply` пишет через тот же писатель, что `hound baseline review`** — один вызов
`review_facts(keys=…)` на все подписи: одна запись файла и одна запись журнала, где
видно, что это одно решение человека, а не 255.

Usage:
    python3 scripts/prepare_baseline_signatures.py build \
        --package packages/ru-pg-dbms --baseline upstream-pg \
        --reference-root ~/.cohound/stands/ru-pg-dbms/reference/ru-pg-dbms \
        --out-xlsx packages/ru-pg-dbms/sources/baseline_signatures.candidate.xlsx \
        --out-data packages/ru-pg-dbms/sources/baseline_signatures.candidate.jsonl
    python3 scripts/prepare_baseline_signatures.py validate \
        --data packages/ru-pg-dbms/sources/baseline_signatures.candidate.jsonl
    python3 scripts/prepare_baseline_signatures.py compile \
        --xlsx …candidate.xlsx --data …candidate.jsonl --out /tmp/signatures.reviewed.jsonl
    python3 scripts/prepare_baseline_signatures.py apply \
        --data /tmp/signatures.reviewed.jsonl --package packages/ru-pg-dbms \
        --baseline upstream-pg --actor {кто} [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from core.pipeline.inheritance import major_version

#: Колонки листа `signatures`: ключ, заголовок, ширина.
COLUMNS: tuple[tuple[str, str, int], ...] = (
    ("n", "№", 5),
    ("param_id", "param_id", 34),
    ("title", "Название параметра (дословно)", 42),
    ("version", "Версия", 8),
    ("review", "Подпись сейчас", 12),
    ("quote", "Цитата (дословно из документа)", 62),
    ("url", "Документ", 44),
    ("section", "Раздел документа", 34),
    ("page_start", "Начало страницы (контекст)", 40),
    ("phrase", "Фраза параметра, по которой зачтено", 40),
    ("confidence", "Уверенность", 11),
    ("reason", "Почему подсвечено", 34),
    ("map_note", "Карта @18: цитата не о предмете", 26),
    ("decision", "Решение: accept / reject", 16),
    ("note", "Заметка", 34),
)

FILL_LOW = "FFC7CE"
FILL_MEDIUM = "FFEB9C"
FILL_DONE = "E2EFDA"

#: Признаки предметного указателя в URL: страница перечисляет разделы.
INDEX_URL = re.compile(r"(bookindex|genindex|\.index)\.html$", re.IGNORECASE)

#: Минимальная длина цитаты, ниже которой это подпись раздела, а не утверждение.
MIN_QUOTE_CHARS = 25

#: Сколько первых символов страницы показывать как контекст.
PAGE_PREVIEW_CHARS = 160

#: Версия, к которой относится карта (``--map-dir``): карта @18 не судит о цитате @11.
MAP_VERSION = "18"

STATE_CANDIDATE = (
    "СОСТОЯНИЕ: candidate — решения не заполнены, человек факты не подписывал."
)

LEGEND: tuple[str, ...] = (
    "",
    "ЧТО ЗДЕСЬ ПОДПИСЫВАЕТСЯ",
    "Строка листа — один факт: «в версии V документация предка содержит это место».",
    "Подпись `accept` значит: цитата действительно говорит о параметре, и её можно",
    "использовать как основание наследования. `reject` — цитата не о предмете (тогда",
    "параметр остаётся без доказательства, и это правильный исход, а не потеря).",
    "Наследует ТОЛЬКО accepted: без подписи строка baselines.csv не появится.",
    "",
    "ПОЧЕМУ ПО ВЕРСИЯМ, А НЕ ПО ПАРАМЕТРАМ",
    "Минимальная версия наследования (min_version) выводится из первого объявленного",
    "мажора, где факт accepted. Подписав только 18, вы получите min_version=18 у",
    "параметра, который есть с 11 — то есть потеряете дешёвое наследование там, где",
    "доказательство уже собрано.",
    "",
    "ПОДСВЕТКА (что читать первым)",
    "low (красный) — цитата из предметного указателя, короче 25 символов или не содержит",
    "  фразы параметра. Указатель перечисляет главы, а не утверждает свойство.",
    "medium (жёлтый) — голый заголовок (≤5 слов без точки), параметр есть на 11 и нет на",
    "  18 (шум отбора, а не регрессия), либо цитата делится с другим параметром версии.",
    "high (без заливки) — цитата содержит утверждение. Читать не обязательно, но",
    "  подпись всё равно ваша: прибор не подставляет accept за человека.",
    "Колонка «Карта: цитата не о предмете» — замечание второго прибора (карта @18 читает те",
    "  же чанки и решает, доказан ли параметр). Это не вердикт для подписи, но подписывать",
    "  забракованную цитату, не увидев этого, значило бы спорить с измерением по незнанию.",
    "зелёная строка — факт уже подписан ранее; решение можно не ставить (или поставить",
    "  reject, если видите, что цитата не о предмете — подпись будет снята).",
    "",
    "ПОРЯДОК РАБОТЫ",
    "1. Лист review_order: только low и medium, красные сверху. Это и есть очередь.",
    "2. В колонке «Решение» поставить accept или reject. Пусто = строку не смотрели,",
    "   и compile откажет — молча пропустить неразмеченное он не имеет права.",
    "3. Цитату и URL не редактировать: это доказательство, а не текст. Правка ловится",
    "   при compile (сравнение с исходником), и разметка отклоняется.",
    "4. apply подписывает всё разом одной записью журнала; затем `hound baseline propose`",
    "   покажет, какие строки политики из этого следуют.",
    "",
    "ЕСЛИ ВСЕ ВЕРСИИ ПАРАМЕТРА ЧИТАЮТСЯ ОДИНАКОВО — лист by_param",
    "У 24 параметров факт есть во всех восьми версиях, и читать 8 почти одинаковых строк —",
    "работа без решения. На листе by_param одна строка на параметр: поставьте решение там,",
    "и оно применится ко всем его версиям, у которых нет собственного решения. Собственное",
    "решение строки сильнее: смешанный параметр (цитата @11 не о предмете, @18 о предмете)",
    "решается построчно. Прибор печатает, сколько строк подписано оптом.",
    "9 из 42 параметров пилота смешаны — колонка «Версии, которые решать построчно»",
    "перечисляет именно те версии, где цитата подсвечена: агрегат по параметру протащил бы",
    "их шум в min_version (например `xlsx_129`: @17 и @18 — из предметного указателя).",
)


# ---------------------------------------------------------------------------
# Вход
# ---------------------------------------------------------------------------


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


def _catalog(package: Path) -> dict[str, Any]:
    from core.pipeline.package_io import load_artifacts

    return {str(row.param_id): row for row in load_artifacts(package).catalog}


def _page_index(
    reference_root: Path, package_id: str, product_id: str, version: str
) -> dict[str, Any]:
    """Чанки версии по URL: раздел и начало страницы для контекста.

    Один проход по сборке (`iter_reference_chunks`), а не запрос на факт: индекса по
    `corpus_chunks.doc_id` в схеме нет, и точечные запросы дали бы полный скан на каждую
    из 255 строк.
    """
    from core.pipeline.reference_corpus import (
        ReferenceBuildMissing,
        iter_reference_chunks,
        reference_layout,
        resolve_build,
    )

    layout = reference_layout(
        reference_root,
        package_id=package_id,
        product_id=product_id,
        version=version,
    )
    try:
        build_id = resolve_build(layout)
    except ReferenceBuildMissing as exc:
        raise SystemExit(f"@ {version}: {exc}") from exc
    db_path = layout.db_path(build_id)
    if not db_path.is_file():
        raise SystemExit(f"@ {version}: нет БД сборки {db_path}")
    by_url: dict[str, list[Any]] = defaultdict(list)
    for chunk in iter_reference_chunks(db_path, build_id=build_id):
        if chunk.url:
            by_url[chunk.url].append(chunk)
    return by_url


def _package_id(package: Path) -> str:
    """`package_id` раскладки reference — **имя каталога пакета**, как в CLI.

    Не `case_id` из `package.yaml`: `_layout` в `baseline_products.py` собирает раскладку
    из `package.name`, и прибор, взявший другое правило, искал бы корпус не там, где его
    собрал `hound baseline build` (расхождение проявилось бы только на пакете, каталог
    которого назван не по `case_id`).
    """
    return Path(package).resolve().name


def _confidence(
    *,
    quote: str,
    url: str | None,
    phrases: list[str],
    phrase: str,
    version: str,
    versions_with_present: set[str],
    shared_with_other_param: bool,
    map_note: str = "",
    by_phrase_gate: bool = True,
) -> tuple[str, str]:
    """Уверенность строки и причина подсветки (пустая причина — `high`).

    :param by_phrase_gate: снят ли факт **фразовым гейтом** (``observed_by=phrase-1``).
        От этого зависит, чем считается отсутствие фразы параметра в цитате: у фразового
        гейта это противоречие (его собственное правило не воспроизводит его цитату —
        красный), у оценщика — законный зачёт по смыслу (жёлтый). Подсветка, спорящая с
        прибором, который снял наблюдение, обесценивает очередь.
    """
    reasons: list[str] = []
    if url and INDEX_URL.search(url):
        reasons.append("указатель, а не утверждение")
    if len(quote.strip()) < MIN_QUOTE_CHARS:
        reasons.append(f"короче {MIN_QUOTE_CHARS} символов")
    lowered = quote.casefold()
    no_phrase = bool(phrases) and not any(item.casefold() in lowered for item in phrases)
    if no_phrase and by_phrase_gate:
        reasons.append("нет фразы параметра")
    if reasons:
        return "low", "; ".join(reasons)
    if no_phrase:
        # Оценщик зачёл цитату по смыслу: терминов каталога в ней нет. Это не брак, но и
        # не то же, что цитата, называющая параметр, — человек должен видеть разницу.
        reasons.append("нет термина параметра (оценщик зачёл смыслом)")
    if _is_single_generic_word(phrase):
        # Одно общее слово — термин, а не связка: `vendor` зачёл параметр «Разработчик»
        # цитатой о расширяемости (замер 19.11.2026, 8 строк). Имя функции (`JSON_TABLE`)
        # и аббревиатура (`ASH`, `SSO`) сюда не попадают: они отличают параметр.
        reasons.append("фраза — одно общее слово (термин, а не связка)")
    if shared_with_other_param:
        reasons.append("цитата делится с другим параметром")
    words = quote.split()
    if len(words) <= 5 and not any(char in quote for char in ".:"):
        reasons.append("голый заголовок")
    if version != "11" and "11" in versions_with_present and "18" not in versions_with_present:
        reasons.append("present@11 без present@18 (шум отбора)")
    if map_note:
        # Второй прибор на том же корпусе уже забраковал цитату: подписывать её, не
        # увидев этого, значило бы спорить с измерением по незнанию.
        reasons.append("карта: цитата не о предмете")
    if reasons:
        return "medium", "; ".join(reasons)
    return "high", ""


def _is_single_generic_word(phrase: str) -> bool:
    """Фраза из одного слова в нижнем регистре — общее понятие, а не связка.

    Правило зачёта перешло со слова на фразу именно потому, что одно слово не называет
    параметр; но `parameter_phrases` всё равно содержит однословные элементы из
    `concept_terms`, и они срабатывают первыми. Аббревиатура (`ASH`, `SSO`) и имя с
    `_`/`/` (`JSON_TABLE`, `SQL/JSON`) отличают параметр и остаются `high`.
    """
    if not phrase or any(char in phrase for char in " _/-"):
        return False
    return phrase == phrase.lower()


def _first_phrase(quote: str, phrases: list[str]) -> str:
    lowered = quote.casefold()
    for phrase in phrases:
        if phrase.casefold() in lowered:
            return phrase
    return ""


def _map_suspects(map_dir: Path | None) -> dict[str, str]:
    """Параметры, про которые карта говорит «цитата не о предмете» (§9.5).

    Второй прибор на том же корпусе: карта читает те же чанки и решает, доказан ли
    параметр. Её замечание — не вердикт для подписи (у карты свой порог и свой промпт), но
    человек, подписывающий цитату, обязан видеть, что другой прибор её уже забраковал.
    """
    if map_dir is None:
        return {}
    cells_path = Path(map_dir) / "cells.jsonl"
    if not cells_path.is_file():
        raise SystemExit(f"карта не найдена: {cells_path}")
    out: dict[str, str] = {}
    for line in cells_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        cell = json.loads(line)
        notes = [
            note for note in cell.get("validation_notes") or []
            if str(note).startswith("support_not_about_subject")
        ]
        if notes:
            out[str(cell["param_id"])] = str(notes[0])
    return out


def build(
    package: Path,
    *,
    baseline_id: str,
    reference_root: Path | None,
    versions: list[str] | None,
    out_xlsx: Path,
    out_data: Path,
    map_dir: Path | None = None,
) -> list[dict[str, Any]]:
    from core.pipeline.reference_corpus import parameter_phrases
    from core.pipeline.reference_products import load_package_facts

    catalog = _catalog(package)
    facts = [
        fact
        for fact in load_package_facts(package)
        if fact.baseline_id == baseline_id and fact.status.value == "present"
    ]
    if versions:
        facts = [fact for fact in facts if fact.version in versions]
    if not facts:
        raise SystemExit(
            f"{baseline_id}: нет present-фактов"
            + (f" на версиях {versions}" if versions else "")
            + " — сначала `hound baseline recon`"
        )

    by_param_versions: dict[str, set[str]] = defaultdict(set)
    for fact in facts:
        by_param_versions[fact.param_id].add(fact.version)
    # Делимость цитаты внутри версии: считается по всему слою, а не по срезу, иначе
    # фильтр версий менял бы подсветку.
    quote_owners: dict[tuple[str, str], set[str]] = defaultdict(set)
    for fact in facts:
        quote_owners[(fact.version, fact.quote or "")].add(fact.param_id)

    pages: dict[str, dict[str, list[Any]]] = {}
    if reference_root is not None:
        product_id = _product_id(package, baseline_id)
        package_id = _package_id(package)
        for version in sorted({fact.version for fact in facts}):
            pages[version] = _page_index(reference_root, package_id, product_id, version)

    suspects = _map_suspects(map_dir)
    records: list[dict[str, Any]] = []
    for number, fact in enumerate(
        sorted(facts, key=lambda item: (item.param_id, _version_key(item.version))), 1
    ):
        row = catalog.get(fact.param_id)
        phrases = parameter_phrases(row) if row is not None else []
        section = ""
        page_start = ""
        for chunk in (pages.get(fact.version) or {}).get(fact.url or "", []):
            if fact.quote and fact.quote in chunk.text:
                section = chunk.heading_path or ""
                page_start = " ".join(chunk.text.split())[:PAGE_PREVIEW_CHARS]
                break
        shared = len(quote_owners[(fact.version, fact.quote or "")]) > 1
        phrase = _first_phrase(fact.quote or "", phrases)
        # Замечание карты — только для её версии: карта @18 не судит о цитате @11.
        map_note = (
            suspects.get(fact.param_id, "")
            if major_version(fact.version) == major_version(MAP_VERSION)
            else ""
        )
        observed_by = fact.observed_by or ""
        confidence, reason = _confidence(
            quote=fact.quote or "",
            url=fact.url,
            phrases=phrases,
            phrase=phrase,
            version=fact.version,
            versions_with_present=by_param_versions[fact.param_id],
            shared_with_other_param=shared,
            map_note=map_note,
            # Пустой ``observed_by`` — файл, написанный до появления колонки, то есть
            # фразовый гейт: другой прибор тогда не писал фактов.
            by_phrase_gate=not observed_by.startswith("llm:"),
        )
        records.append(
            {
                "n": number,
                "param_id": fact.param_id,
                "title": str(getattr(row, "title", "") or ""),
                "version": fact.version,
                "quote": fact.quote or "",
                "url": fact.url or "",
                "section": section,
                "page_start": page_start,
                "phrase": phrase,
                "confidence": confidence,
                "reason": reason,
                "map_note": map_note,
                "review": fact.review.value,
                "reviewed_by": fact.reviewed_by or "",
                "note": "",
                "observed_by": observed_by,
                "decision": "",
            }
        )

    out_data.parent.mkdir(parents=True, exist_ok=True)
    with out_data.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    _write_xlsx(records, out_xlsx)
    return records


def _version_key(version: str) -> tuple[int, str]:
    match = re.match(r"\d+", version)
    return (int(match.group()) if match else 0, version)


def _product_id(package: Path, baseline_id: str) -> str:
    from core.pipeline.reference_products import load_package_reference_products

    declaration = load_package_reference_products(package)
    if declaration is None:
        raise SystemExit(f"{package}: нет sources/reference_products.yaml")
    for product in declaration.products:
        if product.baseline_id == baseline_id:
            return str(product.product_id)
    raise SystemExit(f"{baseline_id}: линия не объявлена в reference_products.yaml")


# ---------------------------------------------------------------------------
# XLSX
# ---------------------------------------------------------------------------


def _state_line(records: list[dict[str, Any]]) -> str:
    decided = [record for record in records if record.get("decision") in {"accept", "reject"}]
    if not decided:
        return STATE_CANDIDATE
    accepted = sum(1 for record in decided if record["decision"] == "accept")
    if len(decided) == len(records):
        return (
            f"СОСТОЯНИЕ: reviewed — решения по всем {len(records)} строкам "
            f"(accept {accepted}, reject {len(decided) - accepted})."
        )
    return (
        f"СОСТОЯНИЕ: частично reviewed — решения по {len(decided)} из {len(records)} "
        f"(accept {accepted}), остальные не читались."
    )


def _write_xlsx(records: list[dict[str, Any]], out: Path) -> None:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    sheet = wb.active
    sheet.title = "signatures"
    wrap = Alignment(wrap_text=True, vertical="top")
    fills = {
        "low": PatternFill("solid", fgColor=FILL_LOW),
        "medium": PatternFill("solid", fgColor=FILL_MEDIUM),
    }
    done_fill = PatternFill("solid", fgColor=FILL_DONE)

    head = [label for _key, label, _width in COLUMNS]
    sheet.append(head)
    for index, (_key, _label, width) in enumerate(COLUMNS, 1):
        letter = get_column_letter(index)
        sheet.column_dimensions[letter].width = width
        sheet.cell(row=1, column=index).font = Font(bold=True)
        sheet.cell(row=1, column=index).alignment = Alignment(
            wrap_text=True, vertical="center"
        )

    for record in records:
        sheet.append([record.get(key, "") for key, _label, _width in COLUMNS])
        line = sheet.max_row
        fill = fills.get(str(record["confidence"]))
        for column in range(1, len(COLUMNS) + 1):
            target = sheet.cell(row=line, column=column)
            target.alignment = wrap
            if record.get("review") == "accepted":
                target.fill = done_fill
            elif fill is not None:
                target.fill = fill
    sheet.freeze_panes = "D2"
    sheet.auto_filter.ref = f"A1:{get_column_letter(len(COLUMNS))}{sheet.max_row}"

    review = wb.create_sheet("review_order")
    review_head = [
        "№", "param_id", "Название", "Версия", "Уверенность", "Почему", "Цитата",
        "Документ", "Раздел", "Решение", "Заметка",
    ]
    review.append(review_head)
    queue = [record for record in records if record["confidence"] != "high"]
    queue.sort(key=lambda record: (record["confidence"] != "low", record["n"]))
    for record in queue:
        review.append([
            record["n"], record["param_id"], record["title"], record["version"],
            record["confidence"], record["reason"], record["quote"], record["url"],
            record["section"], record["decision"], record["note"],
        ])
        line = review.max_row
        for column in range(1, len(review_head) + 1):
            target = review.cell(row=line, column=column)
            target.alignment = wrap
            target.fill = fills[record["confidence"]]
    for index, width in enumerate((5, 34, 42, 8, 11, 34, 62, 44, 34, 16, 34), 1):
        review.column_dimensions[get_column_letter(index)].width = width
    review.freeze_panes = "B2"
    review.auto_filter.ref = f"A1:K{review.max_row}"

    _write_by_param_sheet(wb, records)

    legend = wb.create_sheet("legend")
    legend.column_dimensions["A"].width = 118
    for text in (_state_line(records), *LEGEND):
        legend.append([text])
    legend["A1"].font = Font(bold=True)

    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)


def _by_param_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Сводка по параметру: все его версии одной строкой.

    Человек, у которого 255 строк, читает не 255 раз одно и то же: у 24 параметров факт
    есть во всех восьми версиях, и решение «параметр о предмете» принимается один раз.
    Прибор не решает за него — он даёт уровень агрегации и печатает, сколько строк
    подписано оптом.
    """
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[record["param_id"]].append(record)
    rows: list[dict[str, Any]] = []
    for param_id, group in grouped.items():
        ordered = sorted(group, key=lambda item: _version_key(item["version"]))
        current = next(
            (item for item in ordered if item["version"] == "18"), ordered[-1]
        )
        worst = (
            "low"
            if any(item["confidence"] == "low" for item in ordered)
            else ("medium" if any(item["confidence"] == "medium" for item in ordered) else "high")
        )
        rows.append(
            {
                "param_id": param_id,
                "title": ordered[0]["title"],
                "versions": ", ".join(item["version"] for item in ordered),
                "count": len(ordered),
                "worst": worst,
                "low": sum(1 for item in ordered if item["confidence"] == "low"),
                "medium": sum(1 for item in ordered if item["confidence"] == "medium"),
                # Версии, которые придётся решать построчно: агрегат по параметру иначе
                # протащит шум (`present@11` без `@18`) в min_version.
                "doubtful": ", ".join(
                    f"@{item['version']} ({item['confidence']})"
                    for item in ordered
                    if item["confidence"] != "high"
                ),
                "quote": current["quote"],
                "url": current["url"],
                "map_note": any(item.get("map_note") for item in ordered),
                "decision": "",
                "note": "",
            }
        )
    rows.sort(key=lambda item: ({"low": 0, "medium": 1, "high": 2}[item["worst"]], item["param_id"]))
    return rows


def _write_by_param_sheet(wb: Any, records: list[dict[str, Any]]) -> None:
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    sheet = wb.create_sheet("by_param")
    head = [
        "param_id", "Название", "Версии с present", "Строк", "Худшая уверенность",
        "low", "medium", "Версии, которые решать построчно", "Карта: цитата не о предмете",
        "Цитата @18 (или последняя)", "Документ",
        "Решение по параметру (все версии)", "Заметка",
    ]
    sheet.append(head)
    fills = {
        "low": PatternFill("solid", fgColor=FILL_LOW),
        "medium": PatternFill("solid", fgColor=FILL_MEDIUM),
    }
    wrap = Alignment(wrap_text=True, vertical="top")
    for index, width in enumerate((34, 42, 22, 7, 12, 6, 8, 30, 14, 62, 44, 22, 30), 1):
        sheet.column_dimensions[get_column_letter(index)].width = width
        sheet.cell(row=1, column=index).font = Font(bold=True)
    for row in _by_param_rows(records):
        sheet.append([
            row["param_id"], row["title"], row["versions"], row["count"], row["worst"],
            row["low"], row["medium"], row["doubtful"],
            "да" if row["map_note"] else "", row["quote"], row["url"],
            row["decision"], row["note"],
        ])
        line = sheet.max_row
        for column in range(1, len(head) + 1):
            target = sheet.cell(row=line, column=column)
            target.alignment = wrap
            if row["worst"] in fills:
                target.fill = fills[row["worst"]]
    sheet.freeze_panes = "B2"
    sheet.auto_filter.ref = f"A1:{get_column_letter(len(head))}{sheet.max_row}"


# ---------------------------------------------------------------------------
# Проверка и сборка решений
# ---------------------------------------------------------------------------


def validate(records: list[dict[str, Any]]) -> list[str]:
    """Проверки источника разметки: то, что человек увидит, обязано быть полным."""
    problems: list[str] = []
    seen: set[tuple[str, str]] = set()
    for record in records:
        key = (str(record.get("param_id")), str(record.get("version")))
        if key in seen:
            problems.append(f"{key}: дубликат строки")
        seen.add(key)
        for field in ("param_id", "version", "quote", "url", "confidence"):
            if not str(record.get(field) or "").strip():
                problems.append(f"{key}: пустое поле {field}")
        if record.get("confidence") not in {"high", "medium", "low"}:
            problems.append(f"{key}: confidence={record.get('confidence')!r}")
        if record.get("confidence") == "low" and not str(record.get("reason") or "").strip():
            problems.append(f"{key}: low без причины подсветки")
    return problems


def compile_review(xlsx: Path, records: list[dict[str, Any]], out: Path) -> dict[str, int]:
    """Переносит решения человека из XLSX в JSONL, отказывая на неразмеченном.

    Цитата и URL не редактируются: это доказательство. Изменение ловится сравнением с
    исходником, и разметка отклоняется целиком — иначе подпись относилась бы к другому
    тексту, чем тот, что проверял человек.

    Решение берётся в порядке: строка листа ``signatures`` → строка листа ``by_param``
    (решение по параметру на все его версии) → отказ. Прибор не подставляет «очевидное»:
    отсутствие решения — «строку не смотрели», а не «согласен».
    """
    from openpyxl import load_workbook

    wb = load_workbook(xlsx, data_only=True)
    by_param = _param_decisions(wb)
    sheet = wb["signatures"]
    header = [str(cell.value or "") for cell in sheet[1]]
    index = {name: position for position, name in enumerate(header)}
    by_key = {(record["param_id"], record["version"]): record for record in records}
    problems: list[str] = []
    counters: Counter[str] = Counter()
    for row in sheet.iter_rows(min_row=2, values_only=True):
        if not row or row[index["№"]] is None:
            continue
        param_id = str(row[index["param_id"]] or "")
        version = str(row[index["Версия"]] or "")
        record = by_key.get((param_id, version))
        if record is None:
            problems.append(f"{param_id}@{version}: нет в исходнике разметки")
            continue
        for field, column in (("quote", "Цитата (дословно из документа)"), ("url", "Документ")):
            value = str(row[index[column]] or "")
            if value.strip() != str(record[field]).strip():
                problems.append(f"{param_id}@{version}: изменено поле {field}")
        decision = str(row[index["Решение: accept / reject"]] or "").strip().lower()
        note = str(row[index["Заметка"]] or "").strip()
        from_param = False
        if not decision:
            decision = by_param.get(param_id, ("", ""))[0]
            if decision:
                note = note or by_param[param_id][1]
                from_param = True
        if not decision:
            if record.get("review") == "accepted":
                # Уже подписанный факт решения не требует: он не в очереди.
                record["decision"] = ""
                record["note"] = note
                counters["already"] += 1
                continue
            problems.append(f"{param_id}@{version}: решение не заполнено")
            continue
        if decision not in {"accept", "reject"}:
            problems.append(f"{param_id}@{version}: решение {decision!r}")
            continue
        record["decision"] = decision
        record["note"] = note
        # Двойного счёта нет: строка, подписанная решением параметра, считается один раз.
        counters["by_param" if from_param else decision] += 1
    if problems:
        _fail("compile", problems)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return dict(counters)


def _param_decisions(wb: Any) -> dict[str, tuple[str, str]]:
    """Решения уровня параметра из листа ``by_param``: ``{param_id: (decision, note)}``."""
    if "by_param" not in wb.sheetnames:
        return {}
    sheet = wb["by_param"]
    header = [str(cell.value or "") for cell in sheet[1]]
    index = {name: position for position, name in enumerate(header)}
    column = "Решение по параметру (все версии)"
    if column not in index:
        return {}
    out: dict[str, tuple[str, str]] = {}
    for row in sheet.iter_rows(min_row=2, values_only=True):
        if not row or row[index["param_id"]] is None:
            continue
        decision = str(row[index[column]] or "").strip().lower()
        if decision not in {"accept", "reject"}:
            continue
        out[str(row[index["param_id"]])] = (
            decision,
            str(row[index["Заметка"]] or "").strip(),
        )
    return out


def apply(
    records: list[dict[str, Any]],
    package: Path,
    *,
    baseline_id: str,
    actor: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Подписывает разметку одной записью через `review_facts` (а не правкой CSV)."""
    from core.pipeline.reference_products import review_facts

    accepted = [(r["param_id"], r["version"]) for r in records if r.get("decision") == "accept"]
    rejected = [(r["param_id"], r["version"]) for r in records if r.get("decision") == "reject"]
    result: dict[str, Any] = {"accept": None, "reject": None}
    if accepted:
        result["accept"] = review_facts(
            package,
            baseline_id=baseline_id,
            actor=actor,
            accept=True,
            keys=accepted,
            dry_run=dry_run,
        )
    if rejected:
        result["reject"] = review_facts(
            package,
            baseline_id=baseline_id,
            actor=actor,
            accept=False,
            keys=rejected,
            dry_run=dry_run,
        )
    return result


def _fail(title: str, problems: list[str]) -> None:
    head = "\n  ".join(problems[:10])
    tail = f"\n  … и ещё {len(problems) - 10}" if len(problems) > 10 else ""
    raise SystemExit(f"{title} отказал ({len(problems)}):\n  {head}{tail}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "validate", "compile", "apply"))
    parser.add_argument("--package", type=Path, default=Path("packages/ru-pg-dbms"))
    parser.add_argument("--baseline", default="upstream-pg")
    parser.add_argument("--data", type=Path, help="JSONL разметки (вход validate/compile/apply)")
    parser.add_argument("--xlsx", type=Path, help="XLSX с решениями человека (compile)")
    parser.add_argument("--out-xlsx", type=Path, help="XLSX на разметку (build)")
    parser.add_argument("--out-data", type=Path, help="JSONL источника разметки (build)")
    parser.add_argument("--out", type=Path, help="JSONL решений (compile)")
    parser.add_argument(
        "--map-dir",
        type=Path,
        help="каталог карты версии (cells.jsonl): её замечание «цитата не о предмете» "
        "попадает в подсветку",
    )
    parser.add_argument(
        "--reference-root",
        type=Path,
        help="корень reference-корпуса: …/reference/{package_id} (нужен для раздела страницы)",
    )
    parser.add_argument(
        "--version", action="append", dest="versions", help="ограничить версиями (можно повторять)"
    )
    parser.add_argument("--actor", help="кто подписывает (apply)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.command == "build":
        if args.out_xlsx is None or args.out_data is None:
            parser.error("build требует --out-xlsx и --out-data")
        records = build(
            args.package,
            baseline_id=args.baseline,
            reference_root=args.reference_root,
            versions=args.versions,
            out_xlsx=args.out_xlsx,
            out_data=args.out_data,
            map_dir=args.map_dir,
        )
        counts = Counter(record["confidence"] for record in records)
        already = sum(1 for record in records if record["review"] == "accepted")
        queue = len(records) - counts["high"] - already
        print(f"строк: {len(records)} → {args.out_data} и {args.out_xlsx}")
        print(
            "уверенность: "
            + ", ".join(f"{name} {counts[name]}" for name in ("low", "medium", "high"))
        )
        print(f"уже подписано ранее: {already}; в очереди (low+medium): {queue}")
        by_version = Counter(record["version"] for record in records)
        print("по версиям: " + ", ".join(f"@{v} {n}" for v, n in sorted(by_version.items())))
        params = {record["param_id"] for record in records}
        print(
            f"параметров с present: {len(params)} — лист by_param решает параметр "
            "один раз (24 есть во всех восьми версиях)"
        )
        return 0

    if args.data is None:
        parser.error("нужен --data")
    records = _read_jsonl(args.data)
    problems = validate(records)
    if problems:
        _fail("validate", problems)
    if args.command == "validate":
        print(f"проверка пройдена: {len(records)} строк")
        return 0
    if args.command == "compile":
        if args.out is None or args.xlsx is None:
            parser.error("compile требует --xlsx и --out")
        counters = compile_review(args.xlsx, records, args.out)
        print(f"решения: {counters} → {args.out}")
        return 0
    if not args.actor:
        parser.error("apply требует --actor")
    result = apply(
        records,
        args.package,
        baseline_id=args.baseline,
        actor=args.actor,
        dry_run=args.dry_run,
    )
    for name in ("accept", "reject"):
        payload = result[name]
        if payload is None:
            continue
        print(
            f"{name}: изменено {len(payload['changed'])}, без изменений {payload['skipped']}"
            + (" (dry-run)" if payload["dry_run"] else f" → {payload['path']}")
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
