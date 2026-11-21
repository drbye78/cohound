#!/usr/bin/env python3
"""Прибор: упоминает ли принятая цитата **адресат** параметра (субъект), а не соседа.

Вопрос замера. Измеренная ошибка оценщика — **ложное присутствие** (✅/⚠️ там, где эталон
``❌``), и она не связана с нехваткой материала: материал в top-K был. Гипотеза прибора:
часть таких ячеек принята по цитате, которая говорит о **соседнем** предмете (другой тип,
другая подсистема, другая версия), а не о субъекте параметра. Проверка — механическая, без
LLM: цитата (или её предложение) обязана упоминать хотя бы один **адресат** параметра.

Что считается адресатом (по убыванию силы):

* ``concept_terms`` каталога — термины параметра на объявленных языках (это то, чем
  ретривал искал, значит цитата, не содержащая ни одного, пришла «по соседству»);
* латинские идентификаторы из ``rule``/``title`` (``JSONB``, ``TOAST``, ``pg_cron``) —
  они не переводятся и в документации остаются дословными.

**Это прибор, а не правило.** Он ничего не демотирует и в корпус не пишет: его выход —
число перехвата и доля ложных срабатываний на ячейках, которые эталон считает
присутствием. Превращать флаг в демоцию нельзя без замера precision (A3), иначе «зачёт по
смыслу» (16 из 76 принятых цитат без терминов каталога — измерено 20.11.2026) будет
наказан как ошибка.

Использование::

    python scripts/measure_citation_address.py \\
        --catalog packages/ru-pg-dbms/catalog.csv \\
        --golden packages/ru-pg-dbms/golden.csv \\
        --run-db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-145ad66ca3f9/cohound.db \\
        --allow-foreign-schema --json
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.config.run_config import load_catalog, load_golden  # noqa: E402
from core.domain.enums import Status  # noqa: E402
from core.domain.models import CatalogRow  # noqa: E402
from core.store.sqlite import SCHEMA_VERSION  # noqa: E402

__all__ = [
    "address_terms",
    "main",
    "quote_mentions_address",
    "read_presence_cells",
]

#: Латинский идентификатор внутри правила/заголовка: ``JSONB``, ``pg_cron``, ``TOAST``.
#: Минимум 3 символа — иначе одиночные ``a``/``is`` из английской прозы правила давали бы
#: «адресат», которого нет, и прибор показывал бы перехват на пустом месте.
_IDENTIFIER_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{2,}")

#: Слова правила, которые идентификатором не являются: они встречаются в любой прозе и
#: сделали бы проверку бессмысленной. Список короткий и объявленный, а не «умный»: расширять
#: его — значит решать за каталог, какие слова значимы.
_STOPWORDS = frozenset(
    {
        "and",
        "any",
        "are",
        "can",
        "for",
        "has",
        "have",
        "into",
        "must",
        "not",
        "the",
        "use",
        "used",
        "with",
        "without",
        "may",
        "only",
        "all",
        "its",
        "own",
        "per",
        "via",
        "when",
        "where",
        "which",
        "that",
        "this",
        "from",
        "than",
        "then",
        "they",
        "their",
    }
)


def address_terms(param: CatalogRow) -> tuple[set[str], set[str]]:
    """Адресаты параметра: ``(термины каталога, латинские идентификаторы)``.

    Термины — ``concept_terms`` всех языков плюс ``title`` (он на языке каталога и входит
    в первый запрос ретривала). Идентификаторы — латинские слова правила и заголовка,
    отфильтрованные по :data:`_STOPWORDS`.

    Возвращаются два множества, потому что сила проверки у них разная: термин каталога —
    то, чем искали; идентификатор — то, что в переводе не нуждается. Прибор публикует оба
    числа, чтобы читатель видел, на чём именно держится перехват.
    """
    terms: set[str] = set()
    for values in (param.concept_terms or {}).values():
        for value in values or ():
            text = str(value).strip().casefold()
            if len(text) >= 3:
                terms.add(text)
    title = str(param.title or "").strip().casefold()
    if len(title) >= 3:
        terms.add(title)
    identifiers = {
        word.casefold()
        for word in _IDENTIFIER_RE.findall(f"{param.title} {param.rule}")
        if word.casefold() not in _STOPWORDS
    }
    return terms, identifiers


def quote_mentions_address(
    quote: str, terms: set[str], identifiers: set[str]
) -> tuple[bool, bool]:
    """Упоминает ли цитата адресат: ``(по термину каталога, по идентификатору)``.

    Сравнение подстрочное и регистронезависимое: цитата — дословный срез текста (I22), а
    термин каталога может стоять в другой словоформе («версии» / «версия»), поэтому
    требуем совпадения основы, а не слова целиком. Обратная сторона — короткие термины
    могут срабатывать внутри других слов; это видно по числу срабатываний и не делает
    прибор правилом.
    """
    text = str(quote or "").casefold()
    by_term = any(term in text for term in terms if term)
    by_identifier = any(word in text for word in identifiers if word)
    return by_term, by_identifier


def read_presence_cells(
    db_path: Path, *, allow_foreign_schema: bool
) -> list[dict[str, Any]]:
    """Ячейки со статусом присутствия и их принятые цитаты.

    Возвращает по строке на **цитату** (а не на ячейку): у ячейки их несколько, и вопрос
    прибора — про каждую отдельно. ``quotes`` пустой список не отбрасывает ячейку: она
    тоже наблюдение («присутствие без цитаты»), и её число публикуется отдельно.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        version = conn.execute(
            "select value from schema_meta where key = 'schema_version'"
        ).fetchone()
        if (
            version is not None
            and str(version[0]) != SCHEMA_VERSION
            and not allow_foreign_schema
        ):
            raise SystemExit(
                f"БД прогона имеет schema_version={version[0]}, текущая "
                f"{SCHEMA_VERSION}; передайте --allow-foreign-schema, чтобы читать "
                "вердикты как исторический факт"
            )
        rows = conn.execute(
            """
            select c.product_id, c.param_id, c.status, c.justification,
                   coalesce(e.citation_quote, ''), coalesce(e.source_id, '')
            from cells c
            left join cell_evidence e
                   on e.run_id = c.run_id and e.product_id = c.product_id
                  and e.param_id = c.param_id and e.evidence_type = 'direct'
            where c.status in (?, ?)
            order by c.product_id, c.param_id, e.evidence_id
            """,
            (Status.OK.value, Status.PARTIAL.value),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "product_id": row[0],
            "param_id": row[1],
            "status": row[2],
            "justification": row[3],
            "quote": row[4],
            "source_id": row[5],
        }
        for row in rows
    ]


def main(argv: list[str] | None = None) -> int:
    """Точка входа прибора: перехват адреса на стратах эталона."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--golden", type=Path, required=True)
    parser.add_argument("--run-db", type=Path, required=True)
    parser.add_argument("--allow-foreign-schema", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    catalog = {row.param_id: row for row in load_catalog(args.catalog)}
    golden = {
        (row.product_id, row.param_id): row.expected_status
        for row in load_golden(args.golden)
    }
    rows = read_presence_cells(
        args.run_db, allow_foreign_schema=args.allow_foreign_schema
    )

    # Страты: ячейки, где эталон говорит ❌ (там ложное присутствие), и ячейки, где эталон
    # согласен с присутствием (там проверка обязана молчать — иначе она наказывает зачёт
    # «по смыслу», который измерен и легитимен).
    strata: dict[str, dict[str, Any]] = {
        "golden_absent": {"cells": set(), "quotes": 0, "flagged": 0, "by_term": 0},
        "golden_present": {"cells": set(), "quotes": 0, "flagged": 0, "by_term": 0},
    }
    without_quote: dict[str, int] = {"golden_absent": 0, "golden_present": 0}
    per_param: dict[str, int] = {}

    for row in rows:
        key = (row["product_id"], row["param_id"])
        expected = golden.get(key)
        if expected is None:
            stratum = None
        elif expected is Status.ABSENT:
            stratum = "golden_absent"
        elif expected in (Status.OK, Status.PARTIAL):
            stratum = "golden_present"
        else:
            stratum = None
        if stratum is None:
            continue
        bucket = strata[stratum]
        bucket["cells"].add(key)
        if not row["quote"]:
            without_quote[stratum] += 1
            continue
        param = catalog.get(row["param_id"])
        if param is None:
            continue
        terms, identifiers = address_terms(param)
        by_term, by_identifier = quote_mentions_address(
            row["quote"], terms, identifiers
        )
        bucket["quotes"] += 1
        if by_term:
            bucket["by_term"] += 1
        if not (by_term or by_identifier):
            bucket["flagged"] += 1
            per_param[row["param_id"]] = per_param.get(row["param_id"], 0) + 1

    def _rate(flagged: int, total: int) -> float | None:
        return round(flagged / total, 4) if total else None

    report: dict[str, Any] = {
        "run_db": str(args.run_db),
        "schema_version": SCHEMA_VERSION,
        "strata": {
            name: {
                "cells": len(bucket["cells"]),
                "quotes": bucket["quotes"],
                "flagged": bucket["flagged"],
                "by_term": bucket["by_term"],
                "without_quote": without_quote[name],
                # Доля цитат без адресата внутри страты: для ``golden_absent`` это
                # перехват (ложное присутствие по соседу), для ``golden_present`` — цена
                # проверки (сколько верных ячеек она назвала бы подозрительными).
                "flagged_share": _rate(bucket["flagged"], bucket["quotes"]),
            }
            for name, bucket in strata.items()
        },
        "top_flagged_params": sorted(
            per_param.items(), key=lambda item: (-item[1], item[0])
        )[:15],
    }
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for name, block in report["strata"].items():
            print(
                f"{name}: ячеек {block['cells']}, цитат {block['quotes']}, "
                f"без адресата {block['flagged']} "
                f"({block['flagged_share']}), по термину {block['by_term']}, "
                f"без цитаты {block['without_quote']}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
