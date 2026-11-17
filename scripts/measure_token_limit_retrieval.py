#!/usr/bin/env python3
"""Токенный лимит чанка против адреса эталона: R0 до и после включения (план 26, O21).

**Вопрос.** ``TOKEN_LIMIT`` (``core/adapters/chunker_family.py``) режет чанк сверх бюджета
токенов и приклеивает хвост. Включение — ``execution_change``: другой ``chunk_id``, другой
``run_id``, полный пере-эмбеддинг. Владелец 16.11.2026 решил **не включать** для
``ru-pg-dbms`` и назвал условие пересмотра: «``hit@10`` на эталоне O14 покажет различие при
включённом лимите». Прибор проверяет это условие на **объявленном** профиле.

**Что именно мерится.** Лексический профиль ``R0`` (``deploy.retrieval.profile`` — дефолт
прогона, dense-плечо не нужно) на документном эталоне O14: одна и та же выдача считается
дважды — по кускам из ``corpus_chunks`` и по кускам **после** ``TokenLimiter``. Ранжирование
идёт продакшен-путём: тот же движок (tantivy), тот же профиль анализатора, тот же рендер
запросов (:func:`core.pipeline.evaluate.retrieve_for_cell`), тот же срез по документам.
Меняется **только состав кусков**.

**Чего прибор не мерит и почему это названо.** Dense-плечо (``R1``/``R2``) требует
пере-эмбеддинга всего корпуса: ключ ``embed_cache`` — хеш текста, а кэш стенда покрыт на
~10 % (25 823 записи из 253 306, замер 17.11.2026), то есть цена ≈7 ч GPU — та самая, которую
владелец отклонил в O19. Ответ относится к объявленному профилю, а не ко всем трём.

**Почему адрес вообще может измениться.** ``doc_id`` куска от нарезки не зависит (кусок
принадлежит документу по построению), поэтому прямой смены адреса нет. Каналы два, оба через
ранжирование: у разрезанного документа становится **больше** кусков (длина куска входит в
BM25), и разрезанный кусок — **другой текст** (другие термы). Поэтому прибор публикует не
только ``hit@10``, но и «сколько кусков эталонных документов тронуто»: если тронуто ноль,
разница в метрике может быть только от конкурентов.

Запуск::

    .venv/bin/python scripts/measure_token_limit_retrieval.py \
        --db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-145ad66ca3f9/cohound.db \
        --golden packages/ru-pg-dbms/sources/retrieval_golden_docs.json \
        --token-limit 512 --out-json /tmp/opencode/o21/tl-r0.json

Код возврата: 0 — измерение состоялось, 1 — не состоялось (нет входа/токенизатора).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
_SCRIPTS = _ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

# Импорт из соседнего прибора, а не копия: критерий (точный McNemar + парный бутстрэп-CI
# дельты) обязан быть **один** — две реализации одной статистики разошлись бы молча, и
# читатель сравнивал бы числа, посчитанные по-разному.
from measure_embed_input_variant import paired_comparison  # noqa: E402

from core.adapters.lexical_tantivy import LexicalChunk, TantivyLexicalIndex  # noqa: E402
from core.adapters.token_limit import (  # noqa: E402
    HfTokenCounter,
    TokenizerUnavailable,
    TokenLimiter,
)
from core.domain.models import CatalogRow  # noqa: E402
from core.pipeline.retrieval_docs import (  # noqa: E402
    DocQueryResult,
    document_ids_for,
    evaluate_documents,
    load_doc_golden,
)
from core.ports.types import Chunk  # noqa: E402

#: Профиль анализатора: объявленный дефолт деплоймента (``deploy.lexical.profile``).
LEXICAL_PROFILE = "stemmed"

#: Профиль ретривала: ``R0`` — объявленный дефолт (``deploy.retrieval.profile``).
RETRIEVAL_PROFILE = "R0"

#: Срез эталона (``runs.retrieval_cutoff_K`` прогона = 10).
CUTOFF_K = 10


def _connect(db_path: Path) -> sqlite3.Connection:
    """Run-DB **только на чтение**: прибор не имеет права менять прогон."""
    if not db_path.is_file():
        raise FileNotFoundError(f"run-DB не найден: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _load_params(db_path: Path) -> dict[str, CatalogRow]:
    """Каталог прогона из снимка входов рядом с БД (те же запросы, что рендерила фаза)."""
    snapshot = db_path.parent / "plan_inputs.json"
    if not snapshot.is_file():
        raise SystemExit(
            f"measure: нет снимка входов {snapshot} — запросы обязаны быть теми же, "
            "что рендерила фаза"
        )
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    params = payload.get("params")
    if not isinstance(params, list) or not params:
        raise SystemExit(f"measure: {snapshot}: params пуст или не список")
    return {str(item["param_id"]): CatalogRow.model_validate(item) for item in params}


def _chunk(row: sqlite3.Row) -> Chunk:
    """Строка ``corpus_chunks`` → ``Chunk`` (провенанс сохраняется как есть)."""
    return Chunk(
        chunk_id=str(row["chunk_id"]),
        chunk_no=int(row["chunk_no"] or 0),
        text=str(row["text"] or ""),
        heading_path=str(row["heading_path"] or ""),
        context_prefix=str(row["context_prefix"] or ""),
        content_hash=str(row["content_hash"] or ""),
        char_start=row["char_start"],
        char_end=row["char_end"],
    )


def _records(conn: sqlite3.Connection, product: str) -> list[tuple[str, str, Chunk]]:
    """Куски продукта по порядку: ``(chunk_id, doc_id, chunk)``.

    Один запрос на продукт, а не на документ: индекса по ``corpus_chunks.doc_id`` в run-DB
    нет, и выборка «чанки документа» стоила бы полного скана таблицы на каждый документ
    (замер 17.11.2026: 40 документов не уложились в 120 с).
    """
    rows = conn.execute(
        "SELECT chunk_id, doc_id, chunk_no, text, heading_path, context_prefix,"
        " content_hash, char_start, char_end FROM corpus_chunks"
        " WHERE product_id = ? ORDER BY doc_id, chunk_no",
        (product,),
    ).fetchall()
    return [(str(r["chunk_id"]), str(r["doc_id"]), _chunk(r)) for r in rows]


def _limited(
    records: list[tuple[str, str, Chunk]], limiter: TokenLimiter
) -> tuple[list[tuple[str, str, Chunk]], dict[str, int], set[str], float]:
    """Куски после ограничителя — **по документам**, как в пайплайне.

    ``TokenLimiter`` применяется к списку кусков одного документа: он приклеивает хвост
    внутри документа и не знает о соседних. Применить его к сквозному списку значило бы
    приклеить хвост одного документа к другому.

    Возвращает ``(записи, счётчики, тронутые родители, секунды)``. Тронутый родитель —
    кусок, чей текст после ограничителя другой: разрезан на несколько либо вобрал хвост.
    Это и есть «место в тексте, где лимит что-то сделал», в отличие от числа выходных
    кусков, которое меняется не всегда.
    """
    by_doc: dict[str, list[Chunk]] = defaultdict(list)
    order: list[str] = []
    for _, doc_id, chunk in records:
        if doc_id not in by_doc:
            order.append(doc_id)
        by_doc[doc_id].append(chunk)
    out: list[tuple[str, str, Chunk]] = []
    totals = {"split_chunks": 0, "absorbed_tails": 0, "over_limit": 0}
    changed: set[str] = set()
    started = time.perf_counter()
    for doc_id in order:
        parents = by_doc[doc_id]
        limited, report = limiter.apply(parents)
        totals["split_chunks"] += report.split_chunks
        totals["absorbed_tails"] += report.absorbed_tails
        totals["over_limit"] += report.over_limit
        pieces: dict[str, list[Chunk]] = defaultdict(list)
        for chunk in limited:
            pieces[chunk.chunk_id.rsplit("#", 1)[0]].append(chunk)
        for parent in parents:
            group = pieces.get(parent.chunk_id, [])
            if len(group) == 1 and group[0].text == parent.text:
                continue
            changed.add(parent.chunk_id)
        for chunk in limited:
            out.append((chunk.chunk_id, doc_id, chunk))
    return out, totals, changed, time.perf_counter() - started


def _build(
    name: str,
    records: list[tuple[str, str, Chunk]],
    *,
    build_id: str,
) -> tuple[Any, dict[str, str], float]:
    """Лексический индекс варианта, карта «кусок → документ» и время сборки.

    Гидртация — из самих записей: индекс тела не хранит, а ``search`` без провайдера
    поднимает ``LexicalIndexUnavailable``. Текст нужен только для провенанса хитов; метрика
    считается по ``chunk_id``, но подменять текст пустой строкой прибор не имеет права.
    """
    by_id = {chunk_id: chunk for chunk_id, _, chunk in records}
    index = TantivyLexicalIndex(
        profile=LEXICAL_PROFILE,
        hydrate=lambda ids: {cid: by_id[cid] for cid in ids if cid in by_id},
    )
    entries = (
        LexicalChunk(
            chunk_id=chunk_id,
            product_id=doc_id.split(":", 1)[0],
            text=chunk.text,
            heading_path=chunk.heading_path,
            content_hash=chunk.content_hash,
            build_id=build_id,
        )
        for chunk_id, doc_id, chunk in records
    )
    started = time.perf_counter()
    index.index(entries, build_id=build_id)
    return index, {chunk_id: doc_id for chunk_id, doc_id, _ in records}, (
        time.perf_counter() - started
    )


def _measure_cells(
    index: Any,
    chunk_doc: dict[str, str],
    entries: list[Any],
    params: dict[str, CatalogRow],
) -> tuple[list[DocQueryResult], list[dict[str, Any]]]:
    """Ретривал R0 по ячейкам: результаты эталона и ранг под вариантом.

    Метрики считаются **один раз** в конце по всем ячейкам (:func:`evaluate_documents`), а не
    по продуктам: агрегат по продукту с последующим усреднением агрегатов дал бы другую
    величину, чем агрегат по ячейкам, — а эталон объявлен как 132 ячейки, не как 5 продуктов.
    """
    from core.pipeline.evaluate import retrieve_for_cell

    doc_ids = set(chunk_doc.values())
    results: list[DocQueryResult] = []
    per_cell: list[dict[str, Any]] = []
    for entry in entries:
        param = params.get(entry.param_id)
        if param is None:
            raise SystemExit(
                f"measure: ячейка {entry.cell}: param_id {entry.param_id!r} отсутствует "
                "в каталоге прогона — запрос не рендерится"
            )
        retrieval = retrieve_for_cell(
            param,
            product_id=entry.product_id,
            profile=RETRIEVAL_PROFILE,
            cutoff_k=CUTOFF_K,
            lexical_index=index,
            vector_index=None,
            query_vectors=None,
        )
        retrieved = document_ids_for(
            [chunk.chunk_id for chunk in retrieval.chunks], chunk_doc, top_k=CUTOFF_K
        )
        rank = next(
            (
                position
                for position, doc_id in enumerate(retrieved, start=1)
                if doc_id in entry.expected_doc_ids
            ),
            None,
        )
        results.append(
            DocQueryResult(
                cell=entry.cell,
                product_id=entry.product_id,
                param_id=entry.param_id,
                expected_doc_ids=tuple(entry.expected_doc_ids),
                retrieved_doc_ids=retrieved,
                rank=rank,
                unresolved=not any(doc in doc_ids for doc in entry.expected_doc_ids),
            )
        )
        per_cell.append(
            {
                "cell": entry.cell,
                "rank": rank,
                # ``unresolved`` уходит в парное сравнение: ячейка, чей ожидаемый документ
                # не резолвится в этом варианте корпуса, обязана быть исключена с **обеих**
                # сторон, иначе «документа нет» попадёт в знаменатель как провал поиска.
                "unresolved": not any(
                    doc in doc_ids for doc in entry.expected_doc_ids
                ),
            }
        )
    return results, per_cell


def _touch(
    entries: list[Any],
    doc_chunks: dict[str, list[str]],
    changed: set[str],
) -> dict[str, Any]:
    """Сколько кусков эталонных документов тронуто лимитом.

    Тронут — кусок, чей текст изменился (разрезан или вобрал хвост). Если у ячейки тронуто
    ноль кусков её ожидаемых документов, разница в ранге может прийти только от конкурентов:
    собственный текст доказательства не изменился ни на байт.
    """
    cells = 0
    cells_touched = 0
    expected_chunks = 0
    touched_chunks = 0
    per_cell: list[dict[str, Any]] = []
    for entry in entries:
        cells += 1
        own = 0
        own_touched = 0
        for doc_id in entry.expected_doc_ids:
            chunks = doc_chunks.get(doc_id, [])
            own += len(chunks)
            own_touched += sum(1 for chunk_id in chunks if chunk_id in changed)
        expected_chunks += own
        touched_chunks += own_touched
        if own_touched:
            cells_touched += 1
        per_cell.append(
            {
                "cell": entry.cell,
                "expected_chunks": own,
                "touched_chunks": own_touched,
            }
        )
    return {
        "cells": cells,
        "cells_with_touched_expected_chunks": cells_touched,
        "expected_chunks": expected_chunks,
        "touched_chunks": touched_chunks,
        "per_cell": per_cell,
    }


def _rank_changes(
    baseline: list[dict[str, Any]], limited: list[dict[str, Any]]
) -> dict[str, Any]:
    """Сколько ячеек сменили **ранг** при неизменном попадании.

    ``hit@K`` — грубая величина: попадание в top-K не меняется, а позиция внутри него
    меняется, и это видно в ``MRR``. Публиковать одно ``hit@K`` значило бы назвать
    «выдача не изменилась» там, где изменился порядок доказательств: оценщик получает
    куски в другом порядке, и это его условие, а не косметика.
    """
    left = {item["cell"]: item["rank"] for item in baseline}
    right = {item["cell"]: item["rank"] for item in limited}
    shared = sorted(set(left) & set(right))
    changed = [cell for cell in shared if left[cell] != right[cell]]
    return {
        "cells": len(shared),
        "rank_changed": len(changed),
        "rank_changed_hit_preserved": sum(
            1 for cell in changed if left[cell] is not None and right[cell] is not None
        ),
        "examples": [
            {"cell": cell, "baseline": left[cell], "limited": right[cell]}
            for cell in changed[:10]
        ],
    }


def _split_model(value: str) -> tuple[str, str | None]:
    """``repo@revision`` → пара; без ``@`` ревизия не объявлена."""
    if "@" in value:
        model, revision = value.split("@", 1)
        return model, revision or None
    return value, None


def run(
    db_path: Path,
    golden_path: Path,
    *,
    token_limit: int,
    tokenizer_model: str,
    bootstrap: int,
) -> dict[str, Any]:
    """Два варианта корпуса, один эталон, парное сравнение."""
    model, revision = _split_model(tokenizer_model)
    counter = HfTokenCounter(model, revision)
    if not HfTokenCounter.available():
        raise SystemExit(
            "measure: transformers недоступен — токены не измеряются. Символьный прокси "
            "вместо токенов не подставляется."
        )
    try:
        counter.token_ends("проверка")
    except TokenizerUnavailable as exc:
        raise SystemExit(f"measure: {exc}") from exc

    limiter = TokenLimiter(counter, token_limit)
    _case_id, _corpus_run_id, entries = load_doc_golden(golden_path)
    params = _load_params(db_path)
    by_product: dict[str, list[Any]] = defaultdict(list)
    for entry in entries:
        by_product[entry.product_id].append(entry)

    conn = _connect(db_path)
    try:
        build_id = str(
            conn.execute("SELECT index_build_id FROM runs LIMIT 1").fetchone()[0] or ""
        )
        base_results: list[DocQueryResult] = []
        lim_results: list[DocQueryResult] = []
        base_cells: list[dict[str, Any]] = []
        lim_cells: list[dict[str, Any]] = []
        totals = {"chunks_in": 0, "chunks_out": 0}
        policy = {"split_chunks": 0, "absorbed_tails": 0, "over_limit": 0}
        changed_all: set[str] = set()
        doc_chunks: dict[str, list[str]] = {}
        token_seconds = 0.0
        index_seconds = 0.0
        per_product: dict[str, dict[str, Any]] = {}
        for product in sorted(by_product):
            records = _records(conn, product)
            product_entries = by_product[product]

            index, chunk_doc, seconds = _build(
                f"baseline:{product}", records, build_id=f"tl-baseline:{product}"
            )
            index_seconds += seconds
            results_base, cells_base = _measure_cells(
                index, chunk_doc, product_entries, params
            )
            del index

            limited, part, changed, seconds = _limited(records, limiter)
            token_seconds += seconds
            index_lim, chunk_doc_lim, seconds = _build(
                f"limited:{product}", limited, build_id=f"tl-limited:{product}"
            )
            index_seconds += seconds
            results_lim, cells_lim = _measure_cells(
                index_lim, chunk_doc_lim, product_entries, params
            )
            del index_lim

            totals["chunks_in"] += len(records)
            totals["chunks_out"] += len(limited)
            for key in policy:
                policy[key] += part[key]
            changed_all |= changed
            for chunk_id, doc_id, _ in records:
                doc_chunks.setdefault(doc_id, []).append(chunk_id)
            base_results.extend(results_base)
            lim_results.extend(results_lim)
            base_cells.extend(cells_base)
            lim_cells.extend(cells_lim)
            hit_base = evaluate_documents(
                results_base, profile=RETRIEVAL_PROFILE, k=CUTOFF_K, bootstrap=1, seed=0
            ).hit_at_k
            hit_lim = evaluate_documents(
                results_lim, profile=RETRIEVAL_PROFILE, k=CUTOFF_K, bootstrap=1, seed=0
            ).hit_at_k
            per_product[product] = {
                "cells": len(product_entries),
                "chunks_in": len(records),
                "chunks_out": len(limited),
                "hit_at_k_baseline": hit_base,
                "hit_at_k_limited": hit_lim,
            }
            # Освобождается до следующего продукта: тексты продукта и его индекс вместе —
            # это и есть пик памяти прибора, и держать два продукта рядом незачем.
            del records, limited, chunk_doc, chunk_doc_lim
    finally:
        conn.close()

    base_report = evaluate_documents(
        base_results, profile=RETRIEVAL_PROFILE, k=CUTOFF_K, bootstrap=bootstrap, seed=0
    )
    lim_report = evaluate_documents(
        lim_results, profile=RETRIEVAL_PROFILE, k=CUTOFF_K, bootstrap=bootstrap, seed=0
    )
    return {
        "instrument": "scripts/measure_token_limit_retrieval.py",
        "token_limit": token_limit,
        "tokenizer": tokenizer_model,
        "profile": RETRIEVAL_PROFILE,
        "lexical_profile": LEXICAL_PROFILE,
        "cutoff_k": CUTOFF_K,
        "bootstrap": bootstrap,
        "build_id": build_id,
        "cells": len(entries),
        "products": sorted(by_product),
        "per_product": per_product,
        "corpus": {
            "chunks_in": totals["chunks_in"],
            "chunks_out": totals["chunks_out"],
            **policy,
            "touched_parents": len(changed_all),
        },
        "cost": {
            "token_seconds": round(token_seconds, 1),
            "index_seconds": round(index_seconds, 1),
        },
        "metrics": {
            "baseline": {
                "hit_at_k": base_report.hit_at_k,
                "recall_at_k": base_report.recall_at_k,
                "mrr": base_report.mrr,
                "hit_ci": list(base_report.hit_ci),
                "scored": base_report.scored,
                "unresolved_cells": base_report.unresolved_cells,
            },
            "limited": {
                "hit_at_k": lim_report.hit_at_k,
                "recall_at_k": lim_report.recall_at_k,
                "mrr": lim_report.mrr,
                "hit_ci": list(lim_report.hit_ci),
                "scored": lim_report.scored,
                "unresolved_cells": lim_report.unresolved_cells,
            },
        },
        "paired": paired_comparison(
            base_cells, lim_cells, bootstrap=bootstrap, seed=0
        ),
        "ranks": _rank_changes(base_cells, lim_cells),
        "touch": _touch(entries, doc_chunks, changed_all),
    }


def _print(payload: dict[str, Any]) -> None:
    """Строка оператора: числа, а не прилагательные."""
    metrics = payload["metrics"]
    corpus = payload["corpus"]
    print(
        f"R0 · K={payload['cutoff_k']} · ячеек {payload['cells']} · "
        f"кусков {corpus['chunks_in']} → {corpus['chunks_out']}"
    )
    for name in ("baseline", "limited"):
        item = metrics[name]
        print(
            f"  {name:<9} hit@K {item['hit_at_k']:.3f} "
            f"CI [{item['hit_ci'][0]:.3f}; {item['hit_ci'][1]:.3f}] "
            f"MRR {item['mrr']:.3f} найдено {item['scored']}"
        )
    paired = payload["paired"]
    if not paired["n_pairs"]:
        print("парно: сравнивать нечего — ни одной общей разрешённой ячейки")
    else:
        print(
            f"парно: {paired['n_pairs']} ячеек, {paired['hit_baseline']:.3f} → "
            f"{paired['hit_variant']:.3f} (Δ {paired['delta']:+.3f}, "
            f"CI [{paired['delta_ci'][0]:+.3f}; {paired['delta_ci'][1]:+.3f}]), "
            f"выиграл {paired['better']} / проиграл {paired['worse']}, "
            f"McNemar p={paired['mcnemar_p']:.4f}"
        )
    ranks = payload["ranks"]
    print(
        f"ранг: сменили {ranks['rank_changed']} из {ranks['cells']} ячеек "
        f"(попадание сохранено у {ranks['rank_changed_hit_preserved']})"
    )
    touch = payload["touch"]
    print(
        f"тронуто кусков ожидаемых документов: {touch['touched_chunks']} "
        f"из {touch['expected_chunks']}; ячеек с тронутым доказательством "
        f"{touch['cells_with_touched_expected_chunks']} из {touch['cells']}"
    )
    print(
        f"цена: токенизация {payload['cost']['token_seconds']} с, "
        f"индексы {payload['cost']['index_seconds']} с"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path, help="run-DB (mode=ro)")
    parser.add_argument("--golden", required=True, type=Path, help="документный эталон")
    parser.add_argument("--token-limit", type=int, default=512, help="бюджет токенов")
    parser.add_argument(
        "--tokenizer-model",
        default="BAAI/bge-m3@5617a9f61b028005a4858fdac845db406aefb181",
        help="токенизатор как ``repo@revision`` (локальный кэш, без сети)",
    )
    parser.add_argument("--bootstrap", type=int, default=1000, help="ресемплов для CI")
    parser.add_argument("--out-json", type=Path, default=None)
    args = parser.parse_args(argv)
    if args.token_limit <= 0:
        print("measure: --token-limit должен быть положительным", file=sys.stderr)
        return 1
    try:
        payload = run(
            args.db.expanduser(),
            args.golden.expanduser(),
            token_limit=args.token_limit,
            tokenizer_model=args.tokenizer_model,
            bootstrap=args.bootstrap,
        )
    except (FileNotFoundError, sqlite3.Error) as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        return 1
    _print(payload)
    if args.out_json is not None:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(
            json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
        )
        print(f"отчёт: {args.out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
