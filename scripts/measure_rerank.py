#!/usr/bin/env python3
"""Реранк против эталона O14: даёт ли прирост адреса (O23 плана 26).

**Вопрос.** В пути поиска реранкера нет: порядок задаёт только плечо (BM25, косинус или
RRF). Цена кросс-энкодера **уже измерена** (``scripts/measure_reranker_speed.py``, 16.11.2026),
качество — нет, и без числа выбор модели этим не решается. Прибор отвечает на вторую
половину: **меняет ли реранк адрес эталонного документа** на документном эталоне O14.

**Что именно мерится.** Профиль ``R0`` (лексика) на объявленном профиле анализатора:
одна и та же выдача считается дважды — по порядку плеча и после реранка. Базовый top-K
берётся из **той же** выдачи, что видит реранкер: сначала плечо отдаёт ``--top-n``
кандидатов (это и есть «сколько видит реранкер»), затем метрика считается по срезу
``--cutoff-k``. Оба числа публикуются: ``top_n`` — условие реранка, ``cutoff_k`` — срез
эталона, и подменять одно другим значило бы менять предмет замера молча.

**Почему R0, а не R1/R2.** Dense-плечо требует векторов всего корпуса; кэш стенда покрыт
частично (замер 17.11.2026: 25 823 из 253 306), поэтому пере-эмбеддинг — часы GPU. Вопрос
O23 — «меняет ли порядок кросс-энкодер», и на лексическом плече он ставится без этой
цены. Число принадлежит объявленному профилю, а не всем трём; это названо, а не забыто.

**Несколько запросов ячейки.** Ретривал объединяет ≤3 рендеренных запроса политикой
``max``. Реранк обязан видеть тот же материал, поэтому балл документа — **максимум по
запросам** (``rerank_query_fusion: max``), а не балл первого запроса: иначе часть
кандидатов, попавших в выдачу через термин, а не через заголовок, получила бы балл по
чужому запросу. При одном запросе это ровно один вызов ``rerank``.

**Что публикуется.** ``hit@K`` / ``recall@K`` / ``MRR`` / ``nDCG@K`` до и после, **парное**
сравнение (:func:`paired_comparison` из ``measure_embed_input_variant`` — один критерий на
все приборы: точный McNemar + бутстрэп-интервал дельты), число оценённых пар, время на
ячейку, пар/с, пик RSS, пик VRAM (при ``cuda``). Вердикт «эффект есть» прибор **не
выносит**: он публикует числа, решение — за человеком (A3).

**Режим питания — объявленный вход.** Скорость на этом хосте зависит от питания, а
программно отличить USB-C от комплектного БП нельзя, поэтому режим объявляет оператор
(``--power-mode``), и он попадает в отчёт как есть; наблюдённые признаки (``ac_online``,
``battery_status``, частота/потребление/температура карты, счётчик ``SW Power Capping``)
публикуются рядом **контролем**, парой «до/после».

**Результат пишется по ячейке, а не только в конце.** Длинный прогон (132 ячейки, ~10 пар/с
у декодера) не имеет права терять всё при падении: рядом с отчётом прибор ведёт
``*.progress.jsonl`` — строка на ячейку, сброшенная на диск сразу. В строке не только ранги,
но и ``expected_doc_ids``/``retrieved_*``: из них восстанавливается ``DocQueryResult``, то есть
прерванный прогон пересчитывается, а не перезапускается (замер 17.11.2026: первый прогон
Qwen убит OOM на 4-й минуте, и без этого файла от него не осталось бы ничего).

**Режим скоринга — объявленное условие, а не догадка по конфигу.** ``--scoring`` называет
путь к баллу: ``cross-encoder`` (``AutoModelForSequenceClassification``, балл — сырой логит)
или ``decoder-yes-no`` (``AutoModelForCausalLM``, балл — ``logit(«да») − logit(«нет»)`` на
последней позиции промпта). Второй режим требует **объявленного промпта** (``--prompt-file``)
и объявленной пары токенов решения (``--decision-tokens``): у Qwen3-Reranker карточка даёт
``yes``/``no``, у mxbai-rerank-v2 — ``1``/``0``, потому что её промпт просит бинарную оценку
«0/1». Дословные шаблоны обеих карточек записаны в докстринге
``core/adapters/rerank_local.py``; в отчёт попадают сам шаблон, его ``sha256``, источник
файла и пара токенов — другой промпт даёт другой балл, и без этого отчёт невоспроизводим.

**Батч объявлен режимом.** ``--batch-size`` по умолчанию не задан: адаптер берёт дефолт
режима (16 у кросс-энкодера, 4 у декодера — логиты декодера это ``batch × seq × vocab``,
и на карте 4 GiB батч 16 уходит в пейджинг). В отчёт пишется **фактический** батч, а не
запрошенный.

**Чего прибор не мерит.** Dense-профили (``R1``/``R2``) — цена пере-эмбеддинга; влияние
реранка на **вердикты** (это O15/O18: адрес документа — не доказательство); третью модель
за прогон (``--model`` называет одну; сравнение моделей — отдельный прогон, сводится
таблицей отчёта).

Запуск::

    .venv/bin/python scripts/measure_rerank.py \
        --db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-145ad66ca3f9/cohound.db \
        --golden packages/ru-pg-dbms/sources/retrieval_golden_docs.json \
        --model BAAI/bge-reranker-v2-m3@953dc6f6f85a1b2dbfca4c34a2796e7dde08d41e \
        --device cuda --precision fp16 --top-n 20 --bootstrap 200

Код возврата: 0 — измерение состоялось, 1 — не состоялось (нет входа/модели/устройства).

Нейтрален (§1.3): продукт, модель и эталон — аргументы, а не константы.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
import time
from collections import defaultdict
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
_SCRIPTS = _ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

# Импорт из соседнего прибора, а не копия: критерий (точный McNemar + парный бутстрэп-CI
# дельты) обязан быть **один** — две реализации одной статистики разошлись бы молча.
from measure_embed_input_variant import paired_comparison  # noqa: E402

# Снимок питания и монитор RSS — из прибора скорости реранкера: это тот же хост и те же
# признаки, и вторая копия разошлась бы с первой (например, полем счётчика троттлинга).
from measure_reranker_speed import (  # noqa: E402
    DEFAULT_POWER_MODE,
    RssMonitor,
    _counter_pair,
    _measured,
    _power_snapshot,
    _power_window,
    _tri_state,
)

from core.adapters.lexical_tantivy import LexicalChunk, TantivyLexicalIndex  # noqa: E402
from core.adapters.rerank_local import (  # noqa: E402
    DEFAULT_DECISION_TOKENS,
    SCORING_CROSS_ENCODER,
    SCORING_DECODER_YES_NO,
    LocalReranker,
    RerankerUnavailable,
)
from core.domain.models import CatalogRow  # noqa: E402
from core.pipeline.retrieval_docs import (  # noqa: E402
    DocQueryResult,
    document_ids_for,
    evaluate_documents,
    load_doc_golden,
)
from core.ports.types import Chunk  # noqa: E402

#: Профиль ретривала по умолчанию: объявленный дефолт деплоймента (``deploy.retrieval.profile``).
RETRIEVAL_PROFILE = "R0"
#: Профиль анализатора по умолчанию: объявленный дефолт (``deploy.lexical.profile``).
LEXICAL_PROFILE = "stemmed"
#: Сколько кандидатов отдаётся реранкеру и какой срез эталона считается.
DEFAULT_TOP_N = 50
DEFAULT_CUTOFF_K = 10
#: Предел длины пары в токенах (усечение объявлено: оно меняет балл).
DEFAULT_MAX_LENGTH = 512
#: Политика слияния баллов по запросам ячейки — та же, что у ретривала по умолчанию.
RERANK_QUERY_FUSION = "max"


def _decision_tokens(value: str) -> tuple[str, str]:
    """``--decision-tokens "1,0"`` → пара строк; иначе ``argparse`` отвергает значение."""
    parts = [item.strip() for item in value.split(",")]
    if len(parts) != 2 or not all(parts):
        raise argparse.ArgumentTypeError(
            f"decision-tokens={value!r}: нужна пара «да,нет» (например yes,no или 1,0)"
        )
    return parts[0], parts[1]


def _prompt_sha256(template: str | None) -> str | None:
    """Отпечаток шаблона промпта: ``None``, если промпта нет (кросс-энкодер)."""
    if template is None:
        return None
    return hashlib.sha256(template.encode("utf-8")).hexdigest()


def _token_ids(reranker: Any) -> list[int] | None:
    """Идентификаторы пары «да»/«нет»: ``None`` у кросс-энкодера (пары нет)."""
    ids = getattr(reranker, "decision_token_ids", None)
    return None if ids is None else [int(value) for value in ids]


# ---------------------------------------------------------------------------
# Вход: run-DB (только чтение) и снимок каталога
# ---------------------------------------------------------------------------


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
        raise FileNotFoundError(
            f"нет снимка входов {snapshot} — запросы обязаны быть теми же, что рендерила фаза"
        )
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    params = payload.get("params")
    if not isinstance(params, list) or not params:
        raise ValueError(f"{snapshot}: params пуст или не список")
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
    нет, и выборка «чанки документа» стоила бы полного скана на каждый документ.
    """
    rows = conn.execute(
        "SELECT chunk_id, doc_id, chunk_no, text, heading_path, context_prefix,"
        " content_hash, char_start, char_end FROM corpus_chunks"
        " WHERE product_id = ? ORDER BY doc_id, chunk_no",
        (product,),
    ).fetchall()
    return [(str(r["chunk_id"]), str(r["doc_id"]), _chunk(r)) for r in rows]


def _build(
    records: list[tuple[str, str, Chunk]],
    *,
    build_id: str,
    lexical_profile: str,
) -> tuple[Any, dict[str, str], float]:
    """Лексический индекс продукта, карта «кусок → документ» и время сборки.

    Гидртация — из самих записей: индекс тела не хранит, а ``search`` без провайдера
    поднимает ``LexicalIndexUnavailable``. Текст нужен только провенансу хитов; метрика
    считается по ``chunk_id``, но подменять текст пустой строкой прибор не имеет права.
    """
    by_id = {chunk_id: chunk for chunk_id, _, chunk in records}
    index = TantivyLexicalIndex(
        profile=lexical_profile,
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


# ---------------------------------------------------------------------------
# Реранк: баллы и порядок
# ---------------------------------------------------------------------------


def _rerank_scores(
    reranker: Any, queries: Sequence[str], texts: Sequence[str]
) -> list[float]:
    """Балл на документ: максимум по запросам ячейки (политика ``max``, как у ретривала).

    Длина результата сверяется здесь, а не только в адаптере: шов прибора — любой объект
    с ``rerank``, и подмена, вернувшая меньше баллов, обязана остановить измерение, а не
    переставить документы по неполному списку.
    """
    if not texts:
        return []
    if not queries:
        raise ValueError("ячейка без запросов: реранку нечего подать")
    best: list[float] | None = None
    for query in queries:
        part = [float(value) for value in reranker.rerank(query, list(texts))]
        if len(part) != len(texts):
            raise ValueError(
                f"реранкер вернул {len(part)} баллов на {len(texts)} документов: "
                "балл обязан соответствовать документу"
            )
        if best is None:
            best = part
        else:
            best = [max(left, right) for left, right in zip(best, part, strict=True)]
    return best or []


def _order_by_score(chunks: Sequence[Any], scores: Sequence[float]) -> list[str]:
    """``chunk_id`` в порядке убывания балла; равные баллы — по ``chunk_id``.

    Tie-break по ``chunk_id`` — тот же ключ, что у адаптеров индекса: иначе два прогона на
    одном корпусе дали бы разный порядок на равных баллах, и дельта метрики включала бы
    порядок вставки, а не реранк.
    """
    positions = sorted(
        range(len(chunks)), key=lambda i: (-scores[i], chunks[i].chunk_id)
    )
    return [chunks[i].chunk_id for i in positions]


def _doc_result(entry: Any, retrieved: tuple[str, ...], *, unresolved: bool) -> DocQueryResult:
    """Результат ячейки: документы выдачи, ранг первого ожидаемого, разрешимость."""
    return DocQueryResult(
        cell=entry.cell,
        product_id=entry.product_id,
        param_id=entry.param_id,
        expected_doc_ids=tuple(entry.expected_doc_ids),
        retrieved_doc_ids=retrieved,
        rank=next(
            (
                position
                for position, doc_id in enumerate(retrieved, start=1)
                if doc_id in entry.expected_doc_ids
            ),
            None,
        ),
        unresolved=unresolved,
    )


def _measure_cells(
    index: Any,
    chunk_doc: dict[str, str],
    entries: Sequence[Any],
    params: dict[str, CatalogRow],
    *,
    reranker: Any,
    top_n: int,
    cutoff_k: int,
    profile: str,
    on_cell: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[DocQueryResult], list[DocQueryResult], list[dict[str, Any]], dict[str, Any]]:
    """Две выдачи на ячейку: порядок плеча и порядок после реранка.

    Базовый top-K — срез **той же** выдачи, что получил реранкер: сравнение шло бы не двух
    порядков, а двух разных объёмов материала, если бы база считалась по ``cutoff_k``
    кандидатам, а реранк — по ``top_n``.
    """
    from core.pipeline.evaluate import retrieve_for_cell, run_queries_for_cell

    doc_ids = set(chunk_doc.values())
    base_results: list[DocQueryResult] = []
    rerank_results: list[DocQueryResult] = []
    per_cell: list[dict[str, Any]] = []
    pairs = 0
    seconds = 0.0
    for entry in entries:
        param = params.get(entry.param_id)
        if param is None:
            raise ValueError(
                f"ячейка {entry.cell}: param_id {entry.param_id!r} отсутствует в каталоге "
                "прогона — запрос не рендерится"
            )
        retrieval = retrieve_for_cell(
            param,
            product_id=entry.product_id,
            profile=profile,
            cutoff_k=top_n,
            lexical_index=index,
            vector_index=None,
            query_vectors=None,
        )
        candidates = list(retrieval.chunks)
        queries = run_queries_for_cell(param)
        texts = [chunk.text for chunk in candidates]

        started = time.perf_counter()
        scores = _rerank_scores(reranker, queries, texts)
        seconds += time.perf_counter() - started
        pairs += len(texts) * len(queries)

        base_ids = document_ids_for(
            [chunk.chunk_id for chunk in candidates], chunk_doc, top_k=cutoff_k
        )
        reranked_ids = document_ids_for(
            _order_by_score(candidates, scores), chunk_doc, top_k=cutoff_k
        )
        unresolved = not any(doc in doc_ids for doc in entry.expected_doc_ids)
        base_result = _doc_result(entry, base_ids, unresolved=unresolved)
        rerank_result = _doc_result(entry, reranked_ids, unresolved=unresolved)
        base_results.append(base_result)
        rerank_results.append(rerank_result)
        cell_row = {
            "cell": entry.cell,
            "product_id": entry.product_id,
            "param_id": entry.param_id,
            "candidates": len(candidates),
            "queries": len(queries),
            "rank_baseline": base_result.rank,
            "rank_reranked": rerank_result.rank,
            "unresolved": unresolved,
        }
        per_cell.append(cell_row)
        if on_cell is not None:
            # Каждая ячейка — сразу на диск: длинный прогон, чей результат живёт только
            # в памяти, теряет всё при OOM (замер 17.11.2026: прогон убит на 4-й минуте).
            # В строку прогресса идут ещё и ожидание с обеими выдачами: из них
            # восстанавливается ``DocQueryResult``, поэтому прерванный прогон
            # пересчитывается, а не перезапускается. В отчёт они не попадают — там
            # ``per_cell`` остаётся сводкой, а полная выдача лежит в CSV.
            on_cell(
                {
                    **cell_row,
                    "expected_doc_ids": list(entry.expected_doc_ids),
                    "retrieved_baseline": list(base_ids),
                    "retrieved_reranked": list(reranked_ids),
                }
            )
    stats = {"pairs": pairs, "rerank_seconds": seconds}
    return base_results, rerank_results, per_cell, stats


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def _split_model(value: str) -> tuple[str, str | None]:
    """``repo@revision`` → пара; без ``@`` ревизия не объявлена."""
    if "@" in value:
        model, revision = value.split("@", 1)
        return model, revision or None
    return value, None


def _metrics(report: Any) -> dict[str, Any]:
    """Публикуемые метрики отчёта: число, интервал, знаменатель."""
    return {
        "hit_at_k": report.hit_at_k,
        "recall_at_k": report.recall_at_k,
        "mrr": report.mrr,
        "ndcg_at_k": report.ndcg_at_k,
        "hit_ci": list(report.hit_ci),
        "scored": report.scored,
        "unresolved_cells": report.unresolved_cells,
    }


def run(
    db_path: Path,
    golden_path: Path,
    *,
    reranker: Any,
    top_n: int = DEFAULT_TOP_N,
    cutoff_k: int = DEFAULT_CUTOFF_K,
    lexical_profile: str = LEXICAL_PROFILE,
    profile: str = RETRIEVAL_PROFILE,
    bootstrap: int = 1000,
    on_cell: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Один корпус, один эталон, два порядка: базовый и после реранка.

    :param on_cell: вызывается по завершении каждой ячейки — прибор пишет прогресс на
        диск, а не держит результат только в памяти.
    """
    if cutoff_k <= 0:
        raise ValueError(f"cutoff_k обязан быть положительным, получено {cutoff_k!r}")
    if top_n < cutoff_k:
        raise ValueError(
            f"top_n={top_n} < cutoff_k={cutoff_k}: реранкер не может вернуть в срез K то, "
            "чего ему не отдали — числа описывали бы не тот объём кандидатов"
        )
    if bootstrap < 1:
        raise ValueError(f"bootstrap обязан быть >= 1, получено {bootstrap!r}")

    case_id, golden_run_id, entries = load_doc_golden(golden_path)
    params = _load_params(db_path)
    by_product: dict[str, list[Any]] = defaultdict(list)
    for entry in entries:
        by_product[entry.product_id].append(entry)

    conn = _connect(db_path)
    try:
        run_row = conn.execute(
            "SELECT run_id, package_id, index_build_id FROM runs LIMIT 1"
        ).fetchone()
        if run_row is None:
            raise ValueError("в БД нет записи прогона")
        run_id = str(run_row["run_id"])
        if golden_run_id and golden_run_id != run_id:
            raise ValueError(
                f"эталон скомпилирован на прогоне {golden_run_id!r}, а БД — {run_id!r}: "
                "doc_id разных корпусов несравнимы"
            )
        package_id = str(run_row["package_id"] or case_id)
        build_id = str(run_row["index_build_id"] or "")

        base_results: list[DocQueryResult] = []
        rerank_results: list[DocQueryResult] = []
        per_cell: list[dict[str, Any]] = []
        per_product: dict[str, dict[str, Any]] = {}
        totals = {"pairs": 0, "chunks_indexed": 0}
        rerank_seconds = 0.0
        index_seconds = 0.0
        for product in sorted(by_product):
            records = _records(conn, product)
            if not records:
                raise ValueError(
                    f"у продукта {product!r} нет кусков в корпусе — реранку нечего ранжировать"
                )
            index, chunk_doc, seconds = _build(
                records,
                build_id=f"rerank:{product}",
                lexical_profile=lexical_profile,
            )
            index_seconds += seconds
            b_results, r_results, cells, stats = _measure_cells(
                index,
                chunk_doc,
                by_product[product],
                params,
                reranker=reranker,
                top_n=top_n,
                cutoff_k=cutoff_k,
                profile=profile,
                on_cell=on_cell,
            )
            del index
            base_results.extend(b_results)
            rerank_results.extend(r_results)
            per_cell.extend(cells)
            totals["pairs"] += int(stats["pairs"])
            totals["chunks_indexed"] += len(records)
            rerank_seconds += float(stats["rerank_seconds"])
            hit_base = evaluate_documents(
                b_results, profile=profile, k=cutoff_k, bootstrap=1, seed=0
            ).hit_at_k
            hit_rerank = evaluate_documents(
                r_results, profile=profile, k=cutoff_k, bootstrap=1, seed=0
            ).hit_at_k
            per_product[product] = {
                "cells": len(by_product[product]),
                "chunks": len(records),
                "pairs": int(stats["pairs"]),
                "hit_at_k_baseline": hit_base,
                "hit_at_k_reranked": hit_rerank,
            }
            # Тексты продукта и его индекс освобождаются вместе: это и есть пик памяти
            # прибора, и держать два продукта рядом незачем.
            del records, chunk_doc
    finally:
        conn.close()

    base_report = evaluate_documents(
        base_results, profile=profile, k=cutoff_k, bootstrap=bootstrap, seed=0
    )
    rerank_report = evaluate_documents(
        rerank_results, profile=profile, k=cutoff_k, bootstrap=bootstrap, seed=0
    )
    cells_count = len(per_cell)
    return {
        "instrument": "scripts/measure_rerank.py",
        "run_id": run_id,
        "package_id": package_id,
        "build_id": build_id,
        "model": str(getattr(reranker, "model_id", "")),
        "device": str(getattr(reranker, "device", "")),
        "profile": profile,
        "lexical_profile": lexical_profile,
        "top_n": top_n,
        "cutoff_k": cutoff_k,
        "bootstrap": bootstrap,
        "rerank_query_fusion": RERANK_QUERY_FUSION,
        "cells": cells_count,
        "products": sorted(by_product),
        "per_product": per_product,
        "corpus": {
            "chunks_indexed": totals["chunks_indexed"],
            "index_seconds": round(index_seconds, 1),
        },
        "metrics": {
            "baseline": _metrics(base_report),
            "reranked": _metrics(rerank_report),
        },
        "paired": paired_comparison(
            per_cell_as_rank(per_cell, "rank_baseline"),
            per_cell_as_rank(per_cell, "rank_reranked"),
            bootstrap=bootstrap,
            seed=0,
        ),
        "cost": {
            "pairs": totals["pairs"],
            "rerank_seconds": round(rerank_seconds, 2),
            "seconds_per_cell": round(rerank_seconds / cells_count, 3) if cells_count else None,
            "pairs_per_s": round(totals["pairs"] / rerank_seconds, 2)
            if rerank_seconds > 0
            else None,
        },
        "per_cell": per_cell,
    }


def per_cell_as_rank(
    cells: Sequence[dict[str, Any]], rank_key: str
) -> list[dict[str, Any]]:
    """Вид ячейки для :func:`paired_comparison`: ``cell``/``rank``/``unresolved``.

    Парный критерий сравнивает **попадание** (``rank is not None``), поэтому он и берёт
    только эти три поля. Отдельная функция — чтобы две стороны сравнения строились одним
    правилом, а не двумя похожими выражениями в вызове.
    """
    return [
        {"cell": cell["cell"], "rank": cell[rank_key], "unresolved": cell["unresolved"]}
        for cell in cells
    ]


class _ProgressWriter:
    """Пишет по строке на ячейку в ``.jsonl`` и сразу сбрасывает её на диск.

    Смысл файла — пережить падение процесса, а буфер, сброшенный при закрытии, падения не
    переживает. Формат — одна ячейка в строке, те же поля, что у ``per_cell`` в отчёте,
    плюс ``expected_doc_ids``/``retrieved_*``: из них восстанавливается ``DocQueryResult``,
    поэтому прерванный прогон пересчитывается, а не перезапускается.
    """

    def __init__(self, path: Path, *, header: dict[str, Any] | None = None) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = path.open("w", encoding="utf-8")
        if header is not None:
            self._write(header)

    def _write(self, payload: dict[str, Any]) -> None:
        self._handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self._handle.flush()

    def __call__(self, cell: dict[str, Any]) -> None:
        self._write(cell)

    def close(self) -> None:
        self._handle.close()


# ---------------------------------------------------------------------------
# Вывод
# ---------------------------------------------------------------------------


def _write_cells_csv(path: Path, cells: Sequence[dict[str, Any]]) -> None:
    """По-ячеечно: ранг до/после. ``delta`` пуст, если один из рангов ``None``.

    ``None``, записанный как ``0``, читался бы в диффе ранжирования как «было первым
    местом»; пустая ячейка честно говорит «ранга не было».
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "cell",
                "product_id",
                "param_id",
                "candidates",
                "queries",
                "rank_baseline",
                "rank_reranked",
                "delta",
                "unresolved",
            ]
        )
        for cell in cells:
            before = cell["rank_baseline"]
            after = cell["rank_reranked"]
            delta = "" if before is None or after is None else before - after
            writer.writerow(
                [
                    cell["cell"],
                    cell["product_id"],
                    cell["param_id"],
                    cell["candidates"],
                    cell["queries"],
                    "" if before is None else before,
                    "" if after is None else after,
                    delta,
                    int(bool(cell["unresolved"])),
                ]
            )


def _power_line(payload: dict[str, Any]) -> str:
    """Объявленный режим и наблюдённые признаки — раздельно, без производных."""
    mode = str(payload.get("power_mode") or DEFAULT_POWER_MODE)
    declared = "не объявлен" if mode == DEFAULT_POWER_MODE else mode
    power = payload.get("power") or {}
    before = power.get("before") or {}
    after = power.get("after") or {}
    parts = [
        f"питание: режим {declared}",
        f"AC online {_tri_state(before.get('ac_online'))}",
        f"батарея {before.get('battery_status') or 'не измерено'}",
        f"SM {_measured(before.get('sm_clock_mhz'), ' МГц')}",
        f"потребление {_measured(before.get('power_draw_w'), ' Вт', digits=1)}",
        f"температура {_measured(before.get('temperature_c'), ' °C')}",
        _counter_pair(before.get("sw_power_cap_us"), after.get("sw_power_cap_us")),
    ]
    return " · ".join(parts)


def _print(payload: dict[str, Any]) -> None:
    """Строка оператора: числа, а не прилагательные."""
    print(
        f"{payload['profile']} · кандидатов {payload['top_n']} · K={payload['cutoff_k']} · "
        f"ячеек {payload['cells']} · модель {payload['model']} · "
        f"{payload['device']}/{payload.get('precision', '?')} · "
        f"скоринг {payload.get('scoring', '?')} (батч {payload.get('batch_size', '?')})"
    )
    if payload.get("prompt_sha256"):
        print(
            f"  промпт sha256 {payload['prompt_sha256'][:16]}… "
            f"токены решения {payload.get('decision_tokens')} → "
            f"{payload.get('decision_token_ids')}"
        )
    for name in ("baseline", "reranked"):
        item = payload["metrics"][name]
        print(
            f"  {name:<9} hit@K {item['hit_at_k']:.3f} "
            f"CI [{item['hit_ci'][0]:.3f}; {item['hit_ci'][1]:.3f}] "
            f"recall@K {item['recall_at_k']:.3f} MRR {item['mrr']:.3f} "
            f"nDCG@K {item['ndcg_at_k']:.3f} найдено {item['scored']}"
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
    cost = payload["cost"]
    vram = payload.get("peak_vram_bytes")
    rss = payload.get("peak_rss_bytes")
    rss_mib = None if rss is None else round(rss / 2**20)
    vram_mib = None if vram is None else round(vram / 2**20)
    print(
        f"цена: {cost['pairs']} пар за {cost['rerank_seconds']} с = "
        f"{_measured(cost['pairs_per_s'], ' пар/с')}, "
        f"{_measured(cost['seconds_per_cell'], ' с/ячейку')}; "
        f"RSS {_measured(rss_mib, ' MiB')}; VRAM {_measured(vram_mib, ' MiB')}; "
        f"индексы {payload['corpus']['index_seconds']} с"
    )
    print(_power_line(payload), file=sys.stderr)
    if str(payload.get("power_mode") or DEFAULT_POWER_MODE) == DEFAULT_POWER_MODE:
        print(
            "ВНИМАНИЕ: режим питания не объявлен (--power-mode unknown) — числа скорости "
            "не привязаны к режиму питания и между прогонами не сравниваются.",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="реранк против документного эталона: даёт ли прирост адреса (O23)"
    )
    parser.add_argument("--db", type=Path, required=True, help="run-DB (mode=ro)")
    parser.add_argument("--golden", type=Path, required=True, help="документный эталон O14")
    parser.add_argument(
        "--model",
        required=True,
        help="реранкер как ``repo@revision``: модели по умолчанию нет (A3)",
    )
    parser.add_argument("--device", default="cpu", choices=["cpu", "cuda"], help="устройство счёта")
    parser.add_argument(
        "--precision", default="fp32", choices=["fp32", "fp16"], help="точность счёта"
    )
    parser.add_argument(
        "--scoring",
        default=SCORING_CROSS_ENCODER,
        choices=[SCORING_CROSS_ENCODER, SCORING_DECODER_YES_NO],
        help="объявленный режим скоринга (не выводится из конфига модели)",
    )
    parser.add_argument(
        "--prompt-file",
        type=Path,
        default=None,
        help="шаблон промпта декодера с {query} и {document} (обязателен для decoder-yes-no)",
    )
    parser.add_argument(
        "--decision-tokens",
        type=_decision_tokens,
        default=DEFAULT_DECISION_TOKENS,
        help="пара токенов решения «да,нет» для decoder-yes-no (дефолт yes,no)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help="сколько кандидатов отдаётся реранкеру (условие реранка)",
    )
    parser.add_argument(
        "--cutoff-k", type=int, default=DEFAULT_CUTOFF_K, help="срез эталона top-K"
    )
    parser.add_argument(
        "--lexical-profile", default=LEXICAL_PROFILE, help="профиль анализатора индекса"
    )
    parser.add_argument("--profile", default=RETRIEVAL_PROFILE, help="профиль ретривала")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="батч реранкера; по умолчанию — дефолт режима (16 кросс-энкодер, 4 декодер)",
    )
    parser.add_argument(
        "--max-length",
        type=int,
        default=DEFAULT_MAX_LENGTH,
        help="предел длины пары в токенах (усечение объявлено)",
    )
    parser.add_argument("--bootstrap", type=int, default=1000, help="ресемплов для CI")
    parser.add_argument(
        "--power-mode",
        default=DEFAULT_POWER_MODE,
        help="объявленный оператором режим питания; в отчёт попадает как есть",
    )
    parser.add_argument("--out-json", type=Path, default=None, help="машинный отчёт")
    parser.add_argument(
        "--progress-jsonl",
        type=Path,
        default=None,
        help="прогресс по ячейке (по умолчанию — рядом с --out-json как *.progress.jsonl)",
    )
    parser.add_argument("--out-csv", type=Path, default=None, help="по-ячеечно: ранг до/после")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    db_path = Path(args.db).expanduser()
    golden_path = Path(args.golden).expanduser()
    if not db_path.is_file():
        print(f"measure: БД не найдена: {db_path}", file=sys.stderr)
        return 1
    model, revision = _split_model(args.model)
    if not revision:
        print(
            f"measure: --model {args.model!r} без ревизии: `repo@None` не адресует байты "
            "весов, и отчёт назвал бы не ту сборку (A3, D3)",
            file=sys.stderr,
        )
        return 1
    if args.top_n < args.cutoff_k or args.cutoff_k <= 0:
        print(
            f"measure: --top-n {args.top_n} < --cutoff-k {args.cutoff_k} (или K ≤ 0): "
            "реранкер не может вернуть в срез то, чего ему не отдали",
            file=sys.stderr,
        )
        return 1
    if (args.batch_size is not None and args.batch_size <= 0) or args.max_length <= 0:
        print("measure: --batch-size и --max-length обязаны быть положительными", file=sys.stderr)
        return 1
    prompt_template: str | None = None
    prompt_source: str | None = None
    if args.scoring == SCORING_DECODER_YES_NO:
        if args.prompt_file is None:
            print(
                "measure: --scoring decoder-yes-no требует --prompt-file: декодер отвечает "
                "на текст промпта, и промпт здесь условие замера, а не деталь вызова "
                "(другой промпт — другой балл)",
                file=sys.stderr,
            )
            return 1
        prompt_path = Path(args.prompt_file).expanduser()
        if not prompt_path.is_file():
            print(f"measure: файл промпта не найден: {prompt_path}", file=sys.stderr)
            return 1
        # Ровно один завершающий перевод строки снимается: это соглашение файла, а не
        # часть шаблона. Всё остальное уходит в промпт дословно и попадает в отчёт.
        prompt_template = prompt_path.read_text(encoding="utf-8").removesuffix("\n")
        prompt_source = str(prompt_path)
    elif args.prompt_file is not None:
        print(
            f"measure: --prompt-file задан при --scoring {args.scoring}: у кросс-энкодера "
            "промпта нет, и объявлять неисполняемое условие запрещено",
            file=sys.stderr,
        )
        return 1

    with_gpu = args.device == "cuda"
    power_before = _power_snapshot(with_gpu=with_gpu)
    monitor = RssMonitor()
    monitor.start()
    peak_vram: int | None = None
    progress_path = args.progress_jsonl
    if progress_path is None and args.out_json is not None:
        progress_path = args.out_json.with_name(args.out_json.stem + ".progress.jsonl")
    progress = (
        None
        if progress_path is None
        else _ProgressWriter(
            progress_path,
            header={
                "kind": "header",
                "instrument": "scripts/measure_rerank.py",
                "model": f"{model}@{revision}",
                "scoring": args.scoring,
                "top_n": args.top_n,
                "cutoff_k": args.cutoff_k,
                "prompt_sha256": _prompt_sha256(prompt_template),
            },
        )
    )
    try:
        if with_gpu:
            import torch

            if not torch.cuda.is_available():
                print(
                    "measure: --device cuda объявлено, но карта не видна "
                    f"(torch {torch.__version__}, torch.cuda.is_available()=False): тихий "
                    "откат на cpu запрещён",
                    file=sys.stderr,
                )
                return 1
            torch.cuda.empty_cache()
            torch.cuda.reset_peak_memory_stats()
        reranker = LocalReranker(
            model_id=model,
            revision=revision,
            device=args.device,
            precision=args.precision,
            scoring=args.scoring,
            batch_size=args.batch_size,
            max_length=args.max_length,
            prompt_template=prompt_template,
            decision_tokens=tuple(args.decision_tokens),
        )
        payload = run(
            db_path,
            golden_path,
            reranker=reranker,
            top_n=args.top_n,
            cutoff_k=args.cutoff_k,
            lexical_profile=args.lexical_profile,
            profile=args.profile,
            bootstrap=args.bootstrap,
            on_cell=progress,
        )
        if with_gpu:
            peak_vram = int(torch.cuda.max_memory_allocated())
    except (FileNotFoundError, sqlite3.Error, ValueError, RerankerUnavailable) as exc:
        print(f"ОШИБКА: {exc}", file=sys.stderr)
        return 1
    finally:
        peak_rss = monitor.stop()
        if progress is not None:
            progress.close()
    power_after = _power_snapshot(with_gpu=with_gpu)

    payload.update(
        {
            "precision": args.precision,
            "scoring": str(getattr(reranker, "scoring", args.scoring)),
            # Фактический батч, а не запрошенный: ``None`` у --batch-size означает «дефолт
            # режима», и отчёт обязан назвать то, чем действительно считалось.
            "batch_size": int(getattr(reranker, "batch_size", args.batch_size or 0)),
            "max_length": args.max_length,
            "prompt_source": prompt_source,
            "prompt_template": prompt_template,
            "prompt_sha256": _prompt_sha256(prompt_template),
            "decision_tokens": list(getattr(reranker, "decision_tokens", ())),
            "decision_token_ids": _token_ids(reranker),
            "peak_rss_bytes": peak_rss,
            # Окно RSS — весь замер (сборка индекса + реранк), а не только модель: индекс
            # 200k кусков держится в памяти движком, и «пик без индекса» описывал бы
            # прибор, а не цену замера.
            "peak_rss_scope": "index+rerank",
            "peak_vram_bytes": peak_vram,
            "power_mode": str(args.power_mode),
            "power": _power_window(power_before, power_after),
        }
    )
    _print(payload)
    if progress_path is not None:
        print(f"прогресс: {progress_path}")
    if args.out_json is not None:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(
            json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
        )
        print(f"отчёт: {args.out_json}")
    if args.out_csv is not None:
        _write_cells_csv(args.out_csv, payload["per_cell"])
        print(f"по-ячеечно: {args.out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
