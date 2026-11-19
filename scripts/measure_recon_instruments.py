#!/usr/bin/env python3
"""Три прибора отбора знаний на одном reference-корпусе: кто что находит и чего это стоит.

Вопрос владельца (19.11.2026) — способен ли coHound понимать смысл параметров каталога и
отбирать релевантные знания, а не только перечислять слова. Измеримая часть вопроса: какие
параметры находит **каждый механизм отбора** и что происходит там, где они расходятся.

Три механизма, все на одном корпусе одной версии, все читают те же чанки:

* ``phrase`` — фразовый гейт (``core.pipeline.reference_corpus.derive_facts``): кандидаты
  BM25 по ``parameter_terms``, зачёт по вхождению фразы ``parameter_phrases``.
  Детерминирован, без LLM — им написаны ``sources/baseline_facts.csv`` до 19.11.2026;
* ``map`` — карта reference-продукта (``hound baseline map``): кандидаты — запросы Ш6
  (``render_queries``, ≤3) × ``cutoff_k``, вердикт — оценщик LLM;
* ``llm`` — ``hound baseline recon --classifier llm``: кандидаты — **объединение**
  словесного и фразового наборов (``core.pipeline.recon_candidates``), вердикт — тот же
  оценщик, что у карты (``evaluate_cell``, шаблон ``evaluator_v6``).

Секции вывода:

``candidates``
    Для каждого параметра, у которого **какой-то** прибор уже нашёл цитату, проверяется,
    попадает ли эта цитата в набор кандидатов каждого механизма. Это разделяет два
    объяснения расхождения приборов: «оценщик не признал» и «ретривал не донёс».
``agreement``
    Таблица сопряжённости трёх приборов на одной версии + парный точный МакНемар
    (``core.util.stats.mcnemar_exact_p``) для ``llm`` против ``map`` и ``llm`` против
    ``phrase``. Согласованные ячейки в критерий не входят.
``cross_version``
    Переносится ли найденное на соседние версии: для параметров, ``present`` на целевой
    версии, показывается, на скольких объявленных версиях факт тоже ``present``.
    Одиночное совпадение и свойство линии выглядят здесь по-разному — это независимая
    проверка от «повезло на одной версии».

Прибор **ничего не пишет** и не зовёт LLM: он читает уже снятые артефакты (факты, ячейки
карты, reference-БД) и считает. Числа воспроизводимы: те же входы — те же числа, порядок
параметров — порядок каталога (R7).

Запуск (стенд, только чтение):

    COHOUND_HOME=$HOME/.cohound/stands/ru-pg-dbms \
    COHOUND_PATHS_FILE=$HOME/.cohound/stands/ru-pg-dbms/paths.yaml \
    .venv/bin/python scripts/measure_recon_instruments.py \
      --catalog packages/ru-pg-dbms/catalog.csv \
      --corpus-db ~/.cohound/stands/.../builds/{build_id}/reference.db \
      --facts packages/ru-pg-dbms/sources/baseline_facts.csv \
      --map-cells ~/.cohound/stands/.../maps/{map_id}/cells.jsonl \
      --version 18 --query-lang en
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.adapters.lexical_bm25 import Bm25Index  # noqa: E402
from core.config.run_config import load_baseline_facts, load_catalog  # noqa: E402
from core.domain.enums import FactStatus  # noqa: E402
from core.domain.models import CatalogRow  # noqa: E402
from core.pipeline.recon_candidates import (  # noqa: E402
    PHRASES_TOP_K,
    TERMS_TOP_K,
    phrase_candidates,
    recon_candidates,
    term_candidates,
)
from core.pipeline.reference_corpus import iter_reference_chunks  # noqa: E402
from core.util.stats import mcnemar_exact_p  # noqa: E402

#: Статусы ячейки карты, которые означают «утверждение найдено».
_MAP_PRESENT = {"✅", "⚠️"}

__all__ = [
    "candidate_coverage",
    "cross_version",
    "instrument_agreement",
    "main",
]


def _chunks(corpus_db: Path) -> tuple[list[Any], dict[str, str]]:
    """Чанки корпуса и их текст по ``chunk_id`` — один проход, без запросов на документ."""
    chunks = list(iter_reference_chunks(corpus_db))
    return chunks, {chunk.chunk_id: chunk.text for chunk in chunks}


def _index(chunks: Sequence[Any], product_id: str) -> Bm25Index:
    from core.pipeline.reference_map import _IndexDoc

    index = Bm25Index()
    index.index(
        [
            _IndexDoc(chunk_id=chunk.chunk_id, product_id=product_id, text=chunk.text)
            for chunk in chunks
        ]
    )
    return index


def _map_cells(path: Path | None) -> dict[str, Mapping[str, Any]]:
    if path is None:
        return {}
    cells: dict[str, Mapping[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        cells[str(row["param_id"])] = row
    return cells


def _known_quotes(
    facts: Mapping[str, Any], cells: Mapping[str, Mapping[str, Any]]
) -> dict[str, list[str]]:
    """Цитаты, которые **какой-то** прибор уже признал доказательством.

    Это не «правильный ответ»: цитата фразового гейта может быть шумом. Она — известная
    точка, по которой видно, **доносит** ли механизм то место, где утверждение вообще
    нашлось. Качество самих цитат меряется отдельно (подписью человека), и прибор его не
    подменяет.
    """
    quotes: dict[str, list[str]] = {}
    for param_id, fact in facts.items():
        if fact.status is FactStatus.PRESENT and fact.quote:
            quotes.setdefault(param_id, []).append(fact.quote)
    for param_id, cell in cells.items():
        for evidence in cell.get("evidence") or ():
            quote = str((evidence or {}).get("quote") or "").strip()
            if quote:
                quotes.setdefault(param_id, []).append(quote)
    return quotes


def _hits(texts: Mapping[str, str], ids: Iterable[str], quotes: Sequence[str]) -> bool:
    """Хоть одна цитата дословно встречается в тексте хоть одного чанка набора."""
    haystack = [texts[chunk_id] for chunk_id in ids if chunk_id in texts]
    return any(quote in text for quote in quotes for text in haystack)


def candidate_coverage(
    *,
    catalog: Sequence[CatalogRow],
    index: Bm25Index,
    texts: Mapping[str, str],
    product_id: str,
    quotes: Mapping[str, list[str]],
    query_lang: str,
) -> dict[str, Any]:
    """Секция ``candidates``: чей набор кандидатов доносит известную цитату."""
    per_param: list[dict[str, Any]] = []
    sizes: list[int] = []
    for param in catalog:
        param_id = str(param.param_id)
        known = quotes.get(param_id)
        if not known:
            continue
        terms = term_candidates(
            param, index=index, product_id=product_id, top_k=TERMS_TOP_K, query_lang=query_lang
        )
        phrases = phrase_candidates(
            param,
            index=index,
            product_id=product_id,
            top_k=PHRASES_TOP_K,
            query_lang=query_lang,
        )
        union = recon_candidates(
            param, index=index, product_id=product_id, query_lang=query_lang
        )
        sizes.append(len(union))
        term_ids = [chunk_id for chunk_id, _ in terms]
        phrase_ids = [chunk_id for chunk_id, _ in phrases]
        union_ids = [chunk_id for chunk_id, _ in union]
        per_param.append(
            {
                "param_id": param_id,
                "terms": _hits(texts, term_ids, known),
                "phrases": _hits(texts, phrase_ids, known),
                "union": _hits(texts, union_ids, known),
                "n_terms": len(term_ids),
                "n_phrases": len(phrase_ids),
                "n_union": len(union_ids),
            }
        )
    total = len(per_param)
    counts = {
        "params_with_known_quote": total,
        "terms": sum(1 for row in per_param if row["terms"]),
        "phrases": sum(1 for row in per_param if row["phrases"]),
        "union": sum(1 for row in per_param if row["union"]),
        "unique_to_phrases": sum(
            1 for row in per_param if row["phrases"] and not row["terms"]
        ),
        "unique_to_terms": sum(
            1 for row in per_param if row["terms"] and not row["phrases"]
        ),
        "missed_by_union": [row["param_id"] for row in per_param if not row["union"]],
    }
    counts["coverage"] = {
        key: (counts[key] / total if total else None)
        for key in ("terms", "phrases", "union")
    }
    return {
        "counts": counts,
        "candidates_per_param": {
            "mean": (sum(sizes) / len(sizes)) if sizes else None,
            "min": min(sizes) if sizes else None,
            "max": max(sizes) if sizes else None,
        },
        "rows": per_param,
    }


def instrument_agreement(
    *,
    catalog: Sequence[CatalogRow],
    facts: Mapping[str, Any],
    cells: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Секция ``agreement``: сопряжённость приборов и парный точный МакНемар.

    Пара считается только там, где факт снят ``llm``-прибором (``observed_by``
    начинается с ``llm:``): иначе сравнивались бы фразовый гейт сам с собой. Ячейки карты
    берутся по тому же ``param_id`` — карта и факты отвечают на один вопрос, поэтому
    пары законны, а согласованные ячейки (оба нашли / оба нет) в критерий не входят.
    """
    by_instrument = Counter(
        "llm" if (fact.observed_by or "").startswith("llm:") else
        "phrase" if (fact.observed_by or "").startswith("phrase-1") else
        "unknown"
        for fact in facts.values()
    )
    if not by_instrument.get("llm"):
        return {
            "available": False,
            "reason": "среди фактов нет снятых оценщиком (observed_by не начинается с llm:)",
            "instruments": dict(by_instrument),
        }

    llm_only = map_only = both = neither = 0
    rows: list[dict[str, Any]] = []
    for param in catalog:
        param_id = str(param.param_id)
        fact = facts.get(param_id)
        if fact is None or not (fact.observed_by or "").startswith("llm:"):
            continue
        llm_present = fact.status is FactStatus.PRESENT
        map_present = str((cells.get(param_id) or {}).get("status") or "") in _MAP_PRESENT
        if llm_present and map_present:
            both += 1
        elif llm_present:
            llm_only += 1
        elif map_present:
            map_only += 1
        else:
            neither += 1
        rows.append(
            {
                "param_id": param_id,
                "llm": llm_present,
                "map": map_present,
                "llm_note": fact.note,
            }
        )

    map_present_total = sum(
        1
        for cell in cells.values()
        if str((cell or {}).get("status") or "") in _MAP_PRESENT
    )
    return {
        "available": True,
        "instruments": dict(by_instrument),
        "present": {"llm": llm_only + both, "map": map_present_total},
        "llm_vs_map": {
            "llm_only": llm_only,
            "map_only": map_only,
            "both": both,
            "neither": neither,
            "mcnemar_p": mcnemar_exact_p(llm_only, map_only),
        },
        "rows": rows,
    }


def cross_version(
    *,
    catalog: Sequence[CatalogRow],
    facts: Sequence[Any],
    versions: Sequence[str],
    target: str,
) -> dict[str, Any]:
    """Секция ``cross_version``: на скольких объявленных версиях параметр тоже ``present``."""
    present: dict[str, set[str]] = {}
    for fact in facts:
        if fact.status is FactStatus.PRESENT:
            present.setdefault(str(fact.param_id), set()).add(str(fact.version))
    rows: list[dict[str, Any]] = []
    for param in catalog:
        param_id = str(param.param_id)
        found = present.get(param_id, set())
        if target not in found:
            continue
        rows.append(
            {
                "param_id": param_id,
                "versions": sorted(found, key=lambda value: (len(value), value)),
                "count": len(found),
            }
        )
    distribution = Counter(row["count"] for row in rows)
    return {
        "target": target,
        "declared_versions": list(versions),
        "present_at_target": len(rows),
        "only_at_target": sum(1 for row in rows if row["count"] == 1),
        "at_two_or_more": sum(1 for row in rows if row["count"] >= 2),
        "distribution": dict(sorted(distribution.items())),
        "rows": rows,
    }


def _fmt_ratio(value: float | None) -> str:
    return "—" if value is None else f"{value:.3f}"


def _report(payload: Mapping[str, Any]) -> str:
    lines: list[str] = []
    cand = payload["candidates"]["counts"]
    sizes = payload["candidates"]["candidates_per_param"]
    lines.append("== кандидаты: доносит ли набор известную цитату")
    lines.append(f"параметров с известной цитатой: {cand['params_with_known_quote']}")
    for key, label in (("terms", "словесный"), ("phrases", "фразовый"), ("union", "объединение")):
        lines.append(
            f"  {label:12}: {cand[key]:3}  ({_fmt_ratio(cand['coverage'][key])})"
        )
    lines.append(
        f"  уникально у фраз: {cand['unique_to_phrases']}, "
        f"у слов: {cand['unique_to_terms']}, "
        f"не донёс никто: {len(cand['missed_by_union'])}"
    )
    lines.append(
        f"  кандидатов на параметр: среднее {_fmt_ratio(sizes['mean'])}, "
        f"мин {sizes['min']}, макс {sizes['max']}"
    )

    agree = payload["agreement"]
    lines.append("")
    lines.append("== согласие приборов")
    if not agree.get("available"):
        lines.append(f"недоступно: {agree.get('reason')}")
    else:
        lines.append(f"факты сняты: {agree['instruments']}")
        lines.append(f"present: llm {agree['present']['llm']}, map {agree['present']['map']}")
        block = agree["llm_vs_map"]
        lines.append(
            f"llm против map: только llm {block['llm_only']}, только map {block['map_only']}, "
            f"оба {block['both']}, точный МакНемар p={block['mcnemar_p']:.4g}"
        )

    cross = payload["cross_version"]
    lines.append("")
    lines.append(f"== перенос на другие версии (целевая {cross['target']})")
    lines.append(f"present на целевой: {cross['present_at_target']}")
    lines.append(f"  только на ней: {cross['only_at_target']}")
    lines.append(f"  на двух и более: {cross['at_two_or_more']}")
    lines.append(f"  распределение по числу версий: {cross['distribution']}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Три прибора отбора знаний на одном reference-корпусе (чтение, без LLM)"
    )
    parser.add_argument("--catalog", required=True, type=Path, help="catalog.csv профиля")
    parser.add_argument("--corpus-db", required=True, type=Path, help="reference.db сборки")
    parser.add_argument("--facts", required=True, type=Path, help="baseline_facts.csv")
    parser.add_argument("--map-cells", type=Path, default=None, help="cells.jsonl карты")
    parser.add_argument("--version", required=True, help="целевая версия (18)")
    parser.add_argument("--product-id", default="reference", help="идентификатор продукта в индексе")
    parser.add_argument(
        "--query-lang", default="all", choices=("ru", "en", "all"), help="язык терминов"
    )
    parser.add_argument("--json", action="store_true", help="машинночитаемый вывод")
    args = parser.parse_args(argv)

    catalog = load_catalog(args.catalog)
    chunks, texts = _chunks(args.corpus_db)
    if not chunks:
        parser.error(f"reference-корпус пуст: {args.corpus_db}")
    index = _index(chunks, args.product_id)
    facts_all = load_baseline_facts(args.facts)
    facts_target = {str(f.param_id): f for f in facts_all if str(f.version) == args.version}
    cells = _map_cells(args.map_cells)
    quotes = _known_quotes(facts_target, cells)

    payload: dict[str, Any] = {
        "version": args.version,
        "query_lang": args.query_lang,
        "corpus": {"chunks": len(chunks), "db": str(args.corpus_db)},
        "candidates": candidate_coverage(
            catalog=catalog,
            index=index,
            texts=texts,
            product_id=args.product_id,
            quotes=quotes,
            query_lang=args.query_lang,
        ),
        "agreement": instrument_agreement(catalog=catalog, facts=facts_target, cells=cells),
        "cross_version": cross_version(
            catalog=catalog,
            facts=facts_all,
            versions=sorted({str(f.version) for f in facts_all}),
            target=args.version,
        ),
    }
    if args.json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")
    else:
        sys.stdout.write(_report(payload) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
