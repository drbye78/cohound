#!/usr/bin/env python3
"""Прибор: меняет ли объявленный язык запроса то, что **находит** ретривал прогона.

Вопрос замера. Ось ``retrieval.query_lang`` (``ru`` | ``en`` | ``all``) объявлена условием
измерения и входит в ``run_id``, но её цена на **корпусе когорты** не была измерена —
был только reference-замер (``2026-11-19-query-lang-axis.md``: на английском корпусе
предка ``present`` 13 при ``ru`` против 42 при ``en``). Когорта и предок — разные языки
корпуса, поэтому число с одной стороны не переносится на другую: «ось полезна» и «ось
вредна» — оба утверждения требуют своего замера (AGENTS §1.1).

Что именно измеряется. Для каждого параметра каталога и каждого объявленного языка
строятся **те самые** запросы, что пошлёт Ш6 (:func:`core.pipeline.prompt.render_queries`),
и берётся объединение top-K чанков по ним (R0, лексика — как в прогоне). «Попал» = в
тексте хотя бы одного из этих чанков встречается фраза из **фиксированного** набора
параметра (``title`` + ``concept_terms`` всех языков, длина ≥ 4). Фиксированный набор —
главное условие сравнимости: если проверять `ru` только по русским фразам, а `all` — по
русским и английским, `all` выигрывает самим набором, а не выдачей. Этот дефект был
измерен на первой версии прибора (``ru`` 97, ``en`` 88, ``all`` 114 из 181) и здесь
устранён: набор фраз одинаков для всех настроек языка.

Метрика — **находимость**, а не вердикт: попадание фразы в top-K не доказывает, что
документ отвечает на вопрос параметра, и не заменяет разметку. Оценивать найденное
обязан человек или оценщик; прибор отвечает только на «достал ли поиск этот материал».

Сравнение — **парное** (AGENTS §1.1): те же параметры, те же чанки, меняется одна ось.
Публикуются дискордантные пары и точный двусторонний критерий МакНемара, а разница
долей — с бутстрэп-интервалом по параметрам (перестановки внутри пар, сид объявлен).
Две агрегированные доли без этого смешивают эффект оси с составом выборки.

Внутренняя проверка прибора. На русском каталоге ``ru`` и ``all`` обязаны давать
**один и тот же** список запросов (``all`` ставит ``ru`` первым, а бюджет —
``max_queries``), значит и одинаковые числа. Прибор печатает, у скольких параметров
наборы запросов совпали: расхождение чисел при совпавших запросах означает дефект
прибора, а не свойство корпуса.

Прибор нейтрален (§1.3): он не знает, о чём документы, и читает БД прогона **только на
чтение** (никаких вердиктов, никаких записей).

Использование:

    .venv/bin/python scripts/measure_query_lang.py \\
        --run-id ru-pg-dbms-145ad66ca3f9 --allow-legacy-schema \\
        --catalog packages/ru-pg-dbms/catalog.csv \\
        --out docs/reports/measurements/2026-11-19-query-lang-cohort.md

Стенд требует **оба** ``COHOUND_HOME`` и ``COHOUND_PATHS_FILE`` (AGENTS §3.1).
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from collections.abc import Collection, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from core.cli.commands.search import build_retrieval_ports
from core.config.deploy import load_deploy_config
from core.config.paths import resolve_paths
from core.config.run_config import load_catalog
from core.domain.models import CatalogRow
from core.pipeline.evaluate import retrieve_for_cell
from core.pipeline.prompt import render_queries
from core.pipeline.retrieve import RetrievalProfile, retrieve
from core.store.corpus_reader import CorpusReader
from core.util.retrieval_eval import GoldenQuery, RetrievalReport, evaluate_retrieval
from core.util.stats import mcnemar_exact_p

#: Настройки языка, между которыми идёт сравнение. Порядок — как в оси.
LANGS: tuple[str, ...] = ("ru", "en", "all")

#: Сколько запросов шлёт Ш6 на ячейку (``MAX_QUERIES``). Значение объявлено, а не взято
#: из модуля: прибор обязан назвать условие, при котором получены числа.
MAX_QUERIES = 3

#: Минимальная длина фразы для проверки попадания. Короткие слова («тип», «SQL») есть в
#: любом чанке и превратили бы метрику в «поиск что-нибудь нашёл».
MIN_PHRASE = 4

#: Сид бутстрэпа: число воспроизводимо, а не «примерно такое».
BOOTSTRAP_SEED = 20261119

#: Число пере-выборок. 10 000 — цена ≈0.2 с на пару при 181 параметре.
BOOTSTRAP_ITERS = 10_000


@dataclass
class ParamResult:
    """Одна ячейка замера: параметр × все настройки языка."""

    param_id: str
    hit: dict[str, bool] = field(default_factory=dict)
    queries: dict[str, list[str]] = field(default_factory=dict)
    chunks: dict[str, int] = field(default_factory=dict)


def _phrases(param: CatalogRow) -> list[str]:
    """Фиксированный набор фраз параметра — одинаковый для всех настроек языка.

    Заголовок входит всегда: он называет параметр на языке каталога и остаётся в запросах
    при любой оси. Термины — всех языков сразу, потому что проверяется **выдача**, а не
    то, что мы спросили: если английский термин нашёлся русским запросом, это находка.
    """
    out = [param.title]
    for terms in (param.concept_terms or {}).values():
        out.extend(terms)
    seen: set[str] = set()
    result: list[str] = []
    for phrase in out:
        key = phrase.strip().casefold()
        if len(key) < MIN_PHRASE or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def _top_chunks(
    *,
    param: CatalogRow,
    lang: str,
    lexical_index: object,
    products: Sequence[str],
    top_k: int,
) -> tuple[list[str], int]:
    """Запросы объявленного языка → объединение top-K чанков (по ``chunk_id``)."""
    queries = render_queries(param, max_queries=MAX_QUERIES, query_lang=lang)
    texts: dict[str, str] = {}
    for query in queries:
        found = retrieve(
            query_text=query,
            query_vector=None,
            product_ids=products,
            profile=RetrievalProfile.R0,
            cutoff_k=top_k,
            lexical_index=lexical_index,
        ).chunks
        for chunk in found:
            texts.setdefault(chunk.chunk_id, chunk.text)
    return queries, len(texts), [text.casefold() for text in texts.values()]


def _bootstrap_delta(
    a: Sequence[bool], b: Sequence[bool]
) -> tuple[float, float, float]:
    """Парный бутстрэп разницы долей: ``(delta, lo, hi)`` при 95 %.

    Пере-выборка идёт **по параметрам** (ячейка = пара «попал/не попал» в обеих
    настройках), поэтому состав выборки не смешивается с эффектом оси.
    """
    if not a or len(a) != len(b):
        raise ValueError("бутстрэп: нужны две равные непустые выборки")
    n = len(a)
    deltas = [int(x) - int(y) for x, y in zip(a, b, strict=True)]
    delta = sum(deltas) / n
    rng = random.Random(BOOTSTRAP_SEED)  # noqa: S311 — воспроизводимость, не криптография
    samples: list[float] = []
    for _ in range(BOOTSTRAP_ITERS):
        total = sum(deltas[rng.randrange(n)] for _ in range(n))
        samples.append(total / n)
    samples.sort()
    lo = samples[int(0.025 * (len(samples) - 1))]
    hi = samples[int(0.975 * (len(samples) - 1))]
    return delta, lo, hi


def _fmt(value: float) -> str:
    return f"{value:+.4f}"


def _golden_cells(path: Path) -> dict[str, tuple[str, str, frozenset[str]]]:
    """Golden-чанки → ``{cell: (product_id, param_id, expected_ids)}``.

    Один адрес эталона на ячейку: записи голдена идут по запросам (три на ячейку), а
    ожидаемые id у них общие, поэтому объединение по ячейке — тот же набор, что проверяет
    Ш6. ``cell`` = ``product_id/param_id`` (адрес ячейки в терминах отчёта).
    """
    raw = json.loads(path.read_text(encoding="utf-8"))
    cells: dict[str, tuple[str, str, set[str]]] = {}
    for item in raw:
        cell = str(item.get("cell") or "")
        product_id = str(item.get("product_id") or "")
        param_id = str(item.get("param_id") or "")
        expected = {str(cid) for cid in item.get("expected_ids") or []}
        if not cell or not product_id or not param_id or not expected:
            continue
        key = cell
        if key in cells:
            cells[key][2].update(expected)
        else:
            cells[key] = (product_id, param_id, set(expected))
    return {key: (p, q, frozenset(ids)) for key, (p, q, ids) in cells.items()}


def _golden_hits(
    *,
    cells: dict[str, tuple[str, str, frozenset[str]]],
    catalog: dict[str, CatalogRow],
    lexical_index: object,
    lang: str,
    top_k: int,
) -> tuple[list[GoldenQuery], list[list[str]], int]:
    """Ячейки эталона → (голден, выдача, число ячеек без параметра в каталоге).

    Выдача — **склейка запросов ячейки** тем же путём, что у Ш6
    (:func:`core.pipeline.evaluate.retrieve_for_cell`), а не объединение независимых
    выдач: слияние и срез до ``cutoff_k`` меняют состав, и мерить надо то, что увидит
    оценщик.
    """
    queries: list[GoldenQuery] = []
    ranked: list[list[str]] = []
    missing = 0
    for cell, (product_id, param_id, expected) in sorted(cells.items()):
        param = catalog.get(param_id)
        if param is None:
            missing += 1
            continue
        result = retrieve_for_cell(
            param,
            product_id=product_id,
            profile=RetrievalProfile.R0,
            cutoff_k=top_k,
            lexical_index=lexical_index,  # type: ignore[arg-type]
            max_queries=MAX_QUERIES,
            query_lang=lang,
        )
        queries.append(
            GoldenQuery(query=cell, expected_ids=expected, lang=lang)
        )
        ranked.append([chunk.chunk_id for chunk in result.chunks])
    return queries, ranked, missing


def _evaluate_lang(
    queries: Sequence[GoldenQuery],
    ranked: Sequence[Sequence[str]],
    k: int,
    *,
    known_ids: Collection[str] | None = None,
) -> RetrievalReport:
    """Оценка одной настройки языка: ячейка = одна голден-запись.

    Ранжировщик отдаёт выдачу **своей** ячейки по счётчику вызовов:
    :func:`evaluate_retrieval` вызывает его ровно один раз на запись, поэтому порядок
    совпадает с порядком ``queries`` — иначе пришлось бы строить словарь «запрос → выдача»,
    а адреса ячеек не уникальны по тексту.

    ``known_ids`` обязателен для честного чтения: без него «неразрешённый» значит «не
    пришёл в выдаче **этой** настройки», то есть число само зависит от измеряемой оси и
    ничего не говорит об эталоне. С ним — «адреса нет в сборке» (O4).
    """
    counter = {"i": 0}

    def ranker(_query: str, kk: int) -> list[str]:
        index = counter["i"]
        counter["i"] = index + 1
        return list(ranked[index])[:kk]

    return evaluate_retrieval(queries, ranker, k, known_ids=known_ids)


def _report_golden(
    *,
    per_lang: dict[str, tuple[list[GoldenQuery], list[list[str]]]],
    known_ids: Collection[str],
    run_id: str,
    build_id: str,
    package_id: str,
    top_k: int,
    missing: int,
) -> str:
    """Отчёт по эталону: hit@K, recall, MRR, дискордантность, интервалы."""
    lines: list[str] = []
    lines.append("## Условия\n")
    lines.append(f"- прогон: `{run_id}`, сборка `{build_id}`, пакет `{package_id}`")
    lines.append(
        f"- профиль `R0` (лексика), `top_k={top_k}`, `max_queries={MAX_QUERIES}`; "
        "запросы — те же, что шлёт Ш6 (`retrieve_for_cell`)"
    )
    lines.append(
        f"- ячеек с адресом эталона: {len(next(iter(per_lang.values()))[0])}; "
        f"параметров вне каталога: {missing}"
    )
    lines.append("")

    # Ранжировщик на ячейку — через общий хелпер: одна реализация на отчёт и на JSON.
    reports: dict[str, RetrievalReport] = {
        lang: _evaluate_lang(queries, ranked, top_k, known_ids=known_ids)
        for lang, (queries, ranked) in per_lang.items()
    }

    lines.append("## Эталон: попал ли адрес в top-K\n")
    lines.append("| язык | hit@K | recall@K | MRR | неразрешённых id |")
    lines.append("|---|---|---|---|---|")
    for lang in LANGS:
        report = reports[lang]
        lines.append(
            f"| `{lang}` | {report.hit_at_k:.4f} | {report.recall_at_k:.4f} | "
            f"{report.mrr:.4f} | {report.unresolved_expected_ids}"
            f"/{report.expected_ids_total} |"
        )
    lines.append("")

    hits = {lang: [rank is not None for rank in reports[lang].per_query_rank] for lang in LANGS}
    lines.append("## Парные сравнения\n")
    lines.append("| пара | Δ hit@K | 95 % CI | лучше/хуже | p (МакНемар) |")
    lines.append("|---|---|---|---|---|")
    for a, b in (("ru", "en"), ("ru", "all"), ("en", "all")):
        delta, lo, hi = _bootstrap_delta(hits[a], hits[b])
        better = sum(1 for x, y in zip(hits[a], hits[b], strict=True) if x and not y)
        worse = sum(1 for x, y in zip(hits[a], hits[b], strict=True) if y and not x)
        p = mcnemar_exact_p(better, worse)
        lines.append(
            f"| `{a}` − `{b}` | {_fmt(delta)} | [{_fmt(lo)}; {_fmt(hi)}] | "
            f"{better}/{worse} | {p:.4g} |"
        )
    lines.append("")
    return "\n".join(lines)


def _report(
    *,
    results: list[ParamResult],
    run_id: str,
    build_id: str,
    package_id: str,
    top_k: int,
    chunk_total: int,
) -> str:
    """Markdown-отчёт: условия, числа, дискордантность, интервалы."""
    lines: list[str] = []
    lines.append("## Условия\n")
    lines.append(f"- прогон: `{run_id}`, сборка `{build_id}`, пакет `{package_id}`")
    lines.append(f"- корпус: {chunk_total} чанков; профиль `R0` (лексика), `top_k={top_k}`")
    lines.append(f"- запросов на ячейку: `max_queries={MAX_QUERIES}`, язык — ось замера")
    lines.append(f"- параметров: {len(results)}; фраз в наборе проверки: ≥{MIN_PHRASE} символов")
    lines.append("")

    hits = {lang: [r.hit[lang] for r in results] for lang in LANGS}
    lines.append("## Находимость\n")
    lines.append("| язык | попал | доля |")
    lines.append("|---|---|---|")
    for lang in LANGS:
        count = sum(hits[lang])
        lines.append(f"| `{lang}` | {count} | {count / len(results):.4f} |")
    lines.append("")

    lines.append("## Парные сравнения\n")
    lines.append("| пара | Δ | 95 % CI | лучше/хуже | p (МакНемар) |")
    lines.append("|---|---|---|---|---|")
    pairs = [("ru", "en"), ("ru", "all"), ("en", "all")]
    for a, b in pairs:
        delta, lo, hi = _bootstrap_delta(hits[a], hits[b])
        better = sum(1 for x, y in zip(hits[a], hits[b], strict=True) if x and not y)
        worse = sum(1 for x, y in zip(hits[a], hits[b], strict=True) if y and not x)
        p = mcnemar_exact_p(better, worse)
        lines.append(
            f"| `{a}` − `{b}` | {_fmt(delta)} | [{_fmt(lo)}; {_fmt(hi)}] | "
            f"{better}/{worse} | {p:.4g} |"
        )
    lines.append("")

    same = {
        lang: sum(
            1
            for r in results
            if r.queries[lang] == r.queries["ru"]
        )
        for lang in LANGS
    }
    lines.append("## Совпадение наборов запросов (проверка прибора)\n")
    for lang in LANGS:
        lines.append(f"- `{lang}` совпал с `ru` у {same[lang]} из {len(results)} параметров")
    lines.append("")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Замер: находимость материала при разных retrieval.query_lang",
    )
    parser.add_argument("--run-id", required=True, help="идентификатор прогона")
    parser.add_argument(
        "--runs-dir", default=None, help="каталог прогонов (по умолчанию — из раскладки)"
    )
    parser.add_argument(
        "--catalog", required=True, help="catalog.csv пакета (181 строка у ru-pg-dbms)"
    )
    parser.add_argument("--top-k", type=int, default=10, help="cutoff ретривала")
    parser.add_argument(
        "--golden-chunks",
        default=None,
        help=(
            "retrieval_golden_chunks.json: мерить попадание адреса эталона в top-K "
            "вместо находимости фразы по всему каталогу"
        ),
    )
    parser.add_argument(
        "--allow-legacy-schema",
        action="store_true",
        help="читать БД прогона другой schema_version (только чтение, без вердиктов)",
    )
    parser.add_argument("--out", default=None, help="файл markdown-отчёта")
    parser.add_argument("--json", action="store_true", help="печатать итог JSON-ом")
    args = parser.parse_args(argv)

    paths = resolve_paths()
    runs_dir = Path(args.runs_dir) if args.runs_dir else Path(paths.runs)
    db_path = runs_dir / args.run_id / "cohound.db"
    if not db_path.exists():
        print(f"нет БД прогона: {db_path}", file=sys.stderr)
        return 2

    corpus = CorpusReader(db_path, allow_foreign_schema=args.allow_legacy_schema)
    ref = corpus.run_ref()
    ports = build_retrieval_ports(
        index_root=paths.index_store,
        package=ref.package_id,
        store=corpus,
        build_id=ref.index_build_id,
        deploy=load_deploy_config(None),
        dense=False,
    )
    if ports.lexical_index is None:
        print("лексический индекс не собрался — замер невозможен", file=sys.stderr)
        return 2

    partitions = corpus.partitions(ref.index_build_id)
    products = [partition.product_id for partition in partitions]
    catalog_rows = load_catalog(Path(args.catalog))
    catalog = {row.param_id: row for row in catalog_rows}
    # Чанки фильтруются по сборке **партиции** (``pb_*``), а не по композиции когорты:
    # ``corpus_chunks.build_id`` несёт именно её (I17).
    chunk_total = sum(
        corpus.count_chunks(partition.partition_build_id) for partition in partitions
    )

    if args.golden_chunks:
        cells = _golden_cells(Path(args.golden_chunks))
        # Адреса эталона проверяются по БД **до** сравнения настроек: «этого чанка нет в
        # сборке» — свойство эталона, а не выдачи, и оно не должно зависеть от языка.
        expected_all = sorted({cid for _p, _q, ids in cells.values() for cid in ids})
        known_ids = frozenset(corpus.get_chunks(expected_all))
        per_lang: dict[str, tuple[list[GoldenQuery], list[list[str]]]] = {}
        missing = 0
        for lang in LANGS:
            queries, ranked, missing = _golden_hits(
                cells=cells,
                catalog=catalog,
                lexical_index=ports.lexical_index,
                lang=lang,
                top_k=args.top_k,
            )
            per_lang[lang] = (queries, ranked)
        report = _report_golden(
            per_lang=per_lang,
            known_ids=known_ids,
            run_id=ref.run_id,
            build_id=str(ref.index_build_id),
            package_id=ref.package_id,
            top_k=args.top_k,
            missing=missing,
        )
        if args.out:
            Path(args.out).write_text(report, encoding="utf-8")
        if args.json:
            payload = {"run_id": ref.run_id, "mode": "golden_chunks", "top_k": args.top_k}
            for lang, (queries, ranked) in per_lang.items():
                evaluation = _evaluate_lang(
                    queries, ranked, args.top_k, known_ids=known_ids
                )
                payload[lang] = {
                    "cells": evaluation.queries,
                    "hit_at_k": round(evaluation.hit_at_k, 4),
                    "recall_at_k": round(evaluation.recall_at_k, 4),
                    "mrr": round(evaluation.mrr, 4),
                    "unresolved": evaluation.unresolved_expected_ids,
                    "expected_total": evaluation.expected_ids_total,
                }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(report)
        return 0

    results: list[ParamResult] = []
    for index, param in enumerate(catalog_rows, start=1):
        phrases = _phrases(param)
        result = ParamResult(param_id=param.param_id)
        for lang in LANGS:
            queries, chunk_count, texts = _top_chunks(
                param=param,
                lang=lang,
                lexical_index=ports.lexical_index,
                products=products,
                top_k=args.top_k,
            )
            result.queries[lang] = queries
            result.chunks[lang] = chunk_count
            result.hit[lang] = any(phrase in text for phrase in phrases for text in texts)
        results.append(result)
        if index % 20 == 0:
            print(f"… {index}/{len(catalog_rows)}", file=sys.stderr, flush=True)

    report = _report(
        results=results,
        run_id=ref.run_id,
        build_id=str(ref.index_build_id),
        package_id=ref.package_id,
        top_k=args.top_k,
        chunk_total=chunk_total,
    )
    if args.out:
        Path(args.out).write_text(report, encoding="utf-8")
    if args.json:
        print(
            json.dumps(
                {
                    "run_id": ref.run_id,
                    "build_id": str(ref.index_build_id),
                    "top_k": args.top_k,
                    "params": len(results),
                    "hits": {lang: sum(r.hit[lang] for r in results) for lang in LANGS},
                    "empty_retrieval": {
                        lang: sum(1 for r in results if r.chunks[lang] == 0)
                        for lang in LANGS
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
