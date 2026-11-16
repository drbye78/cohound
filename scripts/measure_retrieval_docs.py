"""Прибор: документный retrieval-эталон на сохранённом прогоне (O14/O11 плана 26).

**Что измеряется.** «Попал ли документ, из которого взята цитата ячейки, в top-K выдачи
ретривала». Единица релевантности — **документ** (``doc_id``), а не чанк: чанковый эталон
ломается при смене чанкера, документ переживает перечанкинг. Эталон —
``packages/{case_id}/sources/retrieval_golden_docs.json`` (формат
``cohound-retrieval-golden-docs/1``), логика метрик — :mod:`core.pipeline.retrieval_docs`.

**Почему прибор, а не ``hound experiment retrieval``.** Тот меряет **чанковый** эталон
(``expected_ids`` = ``chunk_id``) и другой вопрос: «вернулся ли тот самый чанк». Здесь
вопрос другой и путь другой — ретривал идёт тем же композером, что у фазы Ш6
(:func:`core.pipeline.evaluate.retrieve_for_cell`), поэтому число сравнимо с тем, что
фаза реально делает, а не с приближением.

**Почему чтение сырым ``sqlite3``, а не через стор.** Гейт схемы
(:class:`core.util.schema_gate.SchemaVersionMismatch`) отказывает файлу прогона, чья
``schema_version`` старше кода: прогон переносится пересозданием, а не чтением. Прибор
читает **данные** (чанки, документы, манифест прогона), а не контракт стора, поэтому
открывает файл ``mode=ro`` и **публикует прочитанную версию схемы** в отчёте: число
относится к корпусу, записанному этим файлом, и читатель видит, к какому именно.

**Что публикуется** (числа, не оценки):

``profiles``
    По каждой **комбинации сетки** (профиль × политика кросс-запросного слияния, для
    ``R2`` ещё × множитель пула плеч): ``hit@K``, ``recall@K``, ``MRR``, ``nDCG@K``,
    бутстрэп-интервалы ``hit``/``recall`` (1000 ресемплов по ячейкам, ``seed=0``), число
    ячеек, число разрешённых (``scored``) и неразрешённых (``unresolved``) и ``per_cell`` —
    по каждой ячейке ожидаемые и найденные документы, ранг, nDCG.
``conditions``
    Профиль/K/``rrf_k``, объявленные оси эмбеддера, сборка композиции, версия схемы файла,
    профиль лексического анализатора, число чанков в индексе и документов в корпусе,
    состав сетки (``pool_multipliers``/``cross_query_fusions``/``grid``).

**Сетка — один проход, один индекс.** Сборка индекса стоит ~30 с, поэтому комбинации
сетки меряются в одном запуске: лексический и векторный индексы строятся один раз,
запросные векторы считаются один раз, а dense-плечо кешируется по
``(продукт, множитель пула)`` — окно батча равно ``cutoff_k * pool_multiplier``, поэтому
готовые хиты другого пула несравнимы и кеш между пулами не переиспользуется.

**Чего прибор НЕ измеряет:** κ оценщика (это Ш6 и ``evaluator-benchmark``), качество
цитат (I22), стоимость эмбеддинга корпуса. Только ретривал.

**Два семейства (O42), один ретривал.** С ``--answer-sets``
(``cohound-retrieval-golden-answers/1``, :mod:`core.pipeline.retrieval_answers`) прибор
отвечает на второй вопрос той же выдачей: «нашёлся ли хоть один документ, который решает
параметр» (``answer_*``) — рядом с прежним «нашёлся ли названный адрес» (``address_*``;
``hit_at_k`` и т.д. остаются алиасами адресных). Разность публикуется как ``hit_gap``:
столько промахов адреса не были промахами вопроса. Ретривал при этом выполняется **один
раз** — второй проход удвоил бы и время, и шум.

**Две границы, которые публикуются, а не замалчиваются.**

* ``document_ids_for`` режет по **чанкам** до дедупа, поэтому ``len(retrieved_doc_ids)``
  обычно много меньше ``K``: doc-level ``hit@10`` строже, чем «десять документов», а
  doc-level MRR несравним с chunk-level MRR из ``hound experiment retrieval``.
* ``recall@K`` растёт с размером набора (``answers`` в CSV, ``docs_per_cell`` в условиях):
  больше отвечающих документов — труднее найти все, поэтому recall публикуется рядом с
  размером набора, а не как одно число.

**K** берётся из ``runs.retrieval_cutoff_K`` (I20: cut-off эталона = K пайплайна), а не из
флага по умолчанию: иначе число описывало бы не тот прибор, которым получены вердикты.

Нейтрален (§1.3): ни продуктов, ни домена — пакет приходит аргументом.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: Размерность вектора берётся из манифеста партиции, а не объявляется здесь.
_MANIFEST_NAME = "manifest.json"


# ---------------------------------------------------------------------------
# Чтение прогона (read-only, без гейта схемы — см. докстринг модуля)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunMeta:
    """Строка ``runs`` + версия схемы файла: условия измерения одного прогона."""

    run_id: str
    package_id: str
    build_id: str
    cutoff_k: int
    schema_version: str


def _open_readonly(db_path: Path) -> sqlite3.Connection:
    """Соединение ``mode=ro`` со строками-словарями: прибор ничего не пишет в прогон."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _read_run(conn: sqlite3.Connection, run_id: str | None) -> RunMeta:
    """Строка ``runs`` прогона и версия схемы файла.

    :raises SystemExit: нет строки, несколько прогонов без ``--run-id`` или не объявлена
        сборка индекса (``index_build_id``) — без неё ретривал не к чему привязать.
    """
    sql = (
        "SELECT run_id, package_id, index_build_id, retrieval_cutoff_K FROM runs"
    )
    params: list[Any] = []
    if run_id is not None:
        sql += " WHERE run_id = ?"
        params.append(run_id)
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        raise SystemExit(f"measure: прогон не найден: run_id={run_id!r}")
    if len(rows) > 1:
        names = ", ".join(str(row["run_id"]) for row in rows[:5])
        raise SystemExit(
            f"measure: в файле {len(rows)} прогонов ({names}…): назовите --run-id, "
            "иначе неясно, чей корпус измеряется"
        )
    row = rows[0]
    build_id = row["index_build_id"]
    if not build_id:
        raise SystemExit(
            f"measure: у прогона {row['run_id']!r} не записан index_build_id — "
            "сборка индекса не разрешается, ретривал не к чему привязать"
        )
    schema_row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    return RunMeta(
        run_id=str(row["run_id"]),
        package_id=str(row["package_id"]),
        build_id=str(build_id),
        cutoff_k=int(row["retrieval_cutoff_K"] or 0),
        schema_version=str(schema_row["value"]) if schema_row is not None else "",
    )


def _load_params(db_path: Path, package_id: str, package_dir: Path | None) -> list[Any]:
    """Строки каталога прогона: ``plan_inputs.json`` рядом с БД, иначе пакет.

    Снимок прогона предпочтительнее пакета: каталог мог измениться после прогона, а
    запросы обязаны быть теми же, что рендерила фаза.
    """
    from core.domain.models import CatalogRow

    snapshot = db_path.parent / "plan_inputs.json"
    if snapshot.is_file():
        payload = json.loads(snapshot.read_text(encoding="utf-8"))
        params = payload.get("params")
        if not isinstance(params, list) or not params:
            raise SystemExit(f"measure: {snapshot}: params пуст или не список")
        return [CatalogRow.model_validate(item) for item in params]
    if package_dir is None:
        raise SystemExit(
            f"measure: нет снимка входов {snapshot} — назовите --package, чтобы взять "
            "каталог из пакета"
        )
    from core.config import load_catalog

    return list(load_catalog(package_dir / "catalog.csv"))


def _chunk_doc_map(conn: sqlite3.Connection) -> dict[str, str]:
    """``chunk_id → doc_id`` по всему корпусу: адрес документа для top-K выдачи."""
    return {
        str(row["chunk_id"]): str(row["doc_id"])
        for row in conn.execute("SELECT chunk_id, doc_id FROM corpus_chunks")
    }


def _corpus_doc_ids(conn: sqlite3.Connection) -> set[str]:
    """``doc_id`` корпуса: по ним считается ``unresolved`` (документа нет — не провал)."""
    return {str(row["doc_id"]) for row in conn.execute("SELECT doc_id FROM corpus_docs")}


# ---------------------------------------------------------------------------
# Порты ретривала (тот же путь, что у фазы Ш6)
# ---------------------------------------------------------------------------


def _iter_lexical_records(
    conn: sqlite3.Connection, build_id: str
) -> Iterator[Any]:
    """Записи лексического индекса **потоком**: список 253k чанков в память не берётся."""
    from core.adapters.lexical_tantivy import LexicalChunk

    cursor = conn.execute(
        "SELECT chunk_id, product_id, text, heading_path, content_hash FROM corpus_chunks"
    )
    for row in cursor:
        yield LexicalChunk(
            chunk_id=str(row["chunk_id"]),
            product_id=str(row["product_id"]),
            text=str(row["text"] or ""),
            heading_path=str(row["heading_path"] or ""),
            content_hash=str(row["content_hash"] or ""),
            build_id=build_id,
        )


def _chunk_lookup(conn: sqlite3.Connection) -> Any:
    """Точка-чтение чанка по ``chunk_id`` (гидртация top-K, без N+1-сканом корпуса)."""
    from core.ports.types import Chunk
    from core.store.sqlite_rows import _row_to_chunk

    def lookup(chunk_id: str) -> Chunk | None:
        row = conn.execute(
            "SELECT chunk_id, chunk_no, text, heading_path, context_prefix, content_hash,"
            " char_start, char_end FROM corpus_chunks WHERE chunk_id = ?",
            (chunk_id,),
        ).fetchone()
        return None if row is None else _row_to_chunk(row)

    return lookup


def _partition_dim(index_root: Path, package_id: str, partition_build_id: str,
                   product_id: str) -> int:
    """Размерность вектора из манифеста партиции (объявленное условие, не догадка)."""
    path = index_root / package_id / partition_build_id / product_id / _MANIFEST_NAME
    if not path.is_file():
        raise SystemExit(f"measure: нет манифеста партиции: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    dim = int(payload.get("dim") or 0)
    if dim <= 0:
        raise SystemExit(f"measure: {path}: dim не объявлен")
    return dim


def _partition_embed_model(index_root: Path, package_id: str,
                           partition_build_id: str, product_id: str) -> str:
    """Наблюдённая модель весов партиции (провенанс, не ось)."""
    path = index_root / package_id / partition_build_id / product_id / _MANIFEST_NAME
    payload = json.loads(path.read_text(encoding="utf-8"))
    return str(payload.get("embed_model_id") or "")


# ---------------------------------------------------------------------------
# Сетка измерения: профиль × слияние запросов × множитель пула
# ---------------------------------------------------------------------------


def _split_csv(raw: str) -> list[str]:
    """Элементы списка через запятую, без пустых: пробелы вокруг — не значение."""
    return [item.strip() for item in raw.split(",") if item.strip()]


def _resolve_pool_multipliers(scalar: int, listed: str | None) -> tuple[int, ...]:
    """Множители пула плеч R2: список перекрывает скаляр.

    Пул — исполняемое условие (``retrieve`` отказывает при ``< 1``), поэтому опечатка
    в списке — отказ с числом, а не тихое выбрасывание элемента: иначе замер описал бы
    не объявленную сетку.

    :raises SystemExit: пустой список, не число или значение ``< 1``.
    """
    if listed is None:
        if scalar < 1:
            raise SystemExit(
                f"measure: --pool-multiplier обязан быть >= 1, получено {scalar}"
            )
        return (scalar,)
    values: list[int] = []
    for item in _split_csv(listed):
        try:
            value = int(item)
        except ValueError as exc:
            raise SystemExit(
                f"measure: --pool-multipliers: {item!r} — не целое число"
            ) from exc
        if value < 1:
            raise SystemExit(
                f"measure: --pool-multipliers: {value} < 1 "
                "(пул плеч R2 = cutoff_k * pool_multiplier)"
            )
        if value not in values:
            values.append(value)
    if not values:
        raise SystemExit("measure: --pool-multipliers пуст")
    return tuple(values)


def _resolve_cross_query_fusions(
    scalar: str, listed: str | None
) -> tuple[str, ...]:
    """Политики кросс-запросного слияния: список перекрывает скаляр.

    :raises SystemExit: неизвестная политика (со списком допустимых) или пустой список.
    """
    from core.pipeline.retrieve import CROSS_QUERY_FUSION_POLICIES

    raw = [scalar] if listed is None else _split_csv(listed)
    if not raw:
        raise SystemExit("measure: --cross-query-fusions пуст")
    policies: list[str] = []
    for item in raw:
        if item not in CROSS_QUERY_FUSION_POLICIES:
            raise SystemExit(
                f"measure: неизвестная политика слияния запросов {item!r} "
                f"(допустимо: {', '.join(CROSS_QUERY_FUSION_POLICIES)})"
            )
        if item not in policies:
            policies.append(item)
    return tuple(policies)


def _measurement_grid(
    profiles: Sequence[str],
    fusions: Sequence[str],
    pool_multipliers: Sequence[int],
) -> list[tuple[str, int, str]]:
    """Комбинации сетки: ``(профиль, множитель пула, политика слияния)``.

    Пул участвует только у ``R2``: у ``R0``/``R1`` его нет в исполнении, и лишние
    комбинации мерили бы одно и то же под разными метками. У ``R0``/``R1`` множитель
    остаётся ``1`` — как дефолт, а не как объявленное условие.
    """
    grid: list[tuple[str, int, str]] = []
    for profile in profiles:
        for fusion in fusions:
            if profile == "R2":
                grid.extend((profile, pool, fusion) for pool in pool_multipliers)
            else:
                grid.append((profile, 1, fusion))
    return grid


def _combination_label(profile: str, pool_multiplier: int, fusion: str) -> str:
    """Метка строки отчёта: пул называется только там, где он исполняется (``R2``)."""
    if profile == "R2":
        return f"{profile}/pool={pool_multiplier}/fusion={fusion}"
    return f"{profile}/fusion={fusion}"


# ---------------------------------------------------------------------------
# Прогон одной комбинации сетки
# ---------------------------------------------------------------------------


def _dense_by_product(
    *,
    by_product: Mapping[str, Sequence[Any]],
    params: Mapping[str, Any],
    profile: str,
    cutoff_k: int,
    pool_multiplier: int,
    vector_index: Any,
    query_vectors: Any,
    cache: dict[tuple[str, int], Any] | None = None,
) -> dict[str, Any]:
    """Dense-плечо по продуктам; кеш ключуется ``(продукт, множитель пула)``.

    Окно батча — ``cutoff_k * pool_multiplier``, поэтому готовые хиты **разного пула
    несравнимы**: отдать их другой комбинации значило бы померить пул 1 под меткой
    ``pool=8``. Множитель поэтому входит в ключ, а не только продукт.

    :param cache: накопитель между комбинациями одной сетки; ``None`` — без кеша.
    """
    from core.pipeline.evaluate import precompute_dense_hits

    store = cache if cache is not None else {}
    dense: dict[str, Any] = {}
    for product_id, product_entries in by_product.items():
        key = (product_id, pool_multiplier)
        if key not in store:
            store[key] = precompute_dense_hits(
                [params[entry.param_id] for entry in product_entries],
                product_ids=[product_id],
                profile=profile,
                cutoff_k=cutoff_k,
                vector_index=vector_index,
                query_vectors=query_vectors,
                pool_multiplier=pool_multiplier,
            )
        dense[product_id] = store[key]
    return dense


def _retrieve_cells(
    *,
    profile: str,
    pool_multiplier: int,
    cross_query_fusion: str,
    entries: Sequence[Any],
    params: Mapping[str, Any],
    cutoff_k: int,
    lexical_index: Any,
    vector_index: Any,
    query_vectors: Any,
    chunk_doc: dict[str, str],
    dense_cache: dict[tuple[str, int], Any] | None = None,
) -> tuple[dict[str, tuple[str, ...]], int]:
    """Ретривал **один раз** на ячейку: ``cell → doc_id top-K`` и чанки без документа.

    Разделение ретривала и метрик — не косметика. С появлением второго семейства
    (набор отвечающих документов) метрики считаются по двум наборам ожиданий на **одной**
    выдаче; второй проход удвоил бы и время, и шум (две выдачи одного запроса при
    плавающих баллах не гарантированно совпадают).

    Второй элемент результата — сколько чанков среза ``cutoff_k`` поиск вернул **без
    записи** в ``chunk_id → doc_id``: ``document_ids_for`` такие чанки пропускает молча,
    и без счётчика просевший recall выглядел бы свойством поиска, а не корпуса.

    :raises SystemExit: ``param_id`` ячейки отсутствует в каталоге (запрос не рендерится).
    """
    from core.pipeline.evaluate import retrieve_for_cell
    from core.pipeline.retrieval_docs import document_ids_for

    by_product: dict[str, list[Any]] = {}
    for entry in entries:
        by_product.setdefault(entry.product_id, []).append(entry)

    dense_by_product = _dense_by_product(
        by_product=by_product,
        params=params,
        profile=profile,
        cutoff_k=cutoff_k,
        pool_multiplier=pool_multiplier,
        vector_index=vector_index,
        query_vectors=query_vectors,
        cache=dense_cache,
    )

    retrieved_by_cell: dict[str, tuple[str, ...]] = {}
    chunks_without_doc = 0
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
            profile=profile,
            cutoff_k=cutoff_k,
            lexical_index=lexical_index,
            vector_index=vector_index,
            query_vectors=query_vectors,
            dense_by_query=dense_by_product.get(entry.product_id),
            pool_multiplier=pool_multiplier,
            cross_query_fusion=cross_query_fusion,
        )
        # ``per_query`` намеренно отдаёт больше ``cutoff_k`` чанков («сколько бы нашли,
        # если бы не резали»). Документные метрики смотрят **срез** ``cutoff_k``: иначе
        # политика выигрывала бы длиной списка, а не порядком.
        chunk_ids = [chunk.chunk_id for chunk in retrieval.chunks]
        chunks_without_doc += sum(
            1 for chunk_id in chunk_ids[:cutoff_k] if chunk_id not in chunk_doc
        )
        retrieved_by_cell[entry.cell] = document_ids_for(
            chunk_ids, chunk_doc, top_k=cutoff_k
        )
    return retrieved_by_cell, chunks_without_doc


def _doc_results(
    entries: Sequence[Any],
    retrieved_by_cell: Mapping[str, tuple[str, ...]],
    expected_by_cell: Mapping[str, Sequence[str]],
    doc_ids: set[str],
    *,
    force_scored: bool = False,
) -> list[Any]:
    """Ячейки с ожиданиями ``expected_by_cell`` поверх **той же** выдачи.

    Правило ``unresolved`` (пункт 3 разбора): ячейка неразрешена, только если **ни один**
    член набора не найден в корпусе; знаменатель ``recall`` — найденные члены
    (``resolved_doc_ids``). Формат 1 не меняется: там набор — один документ, и правило
    сводится к прежнему «либо 1/1, либо ячейка исключена».

    :param force_scored: держать ячейку в знаменателе даже при пустом остатке — так
        считается чувствительность (ячейка, чьи ответы целиком пришли из выдачи, даёт
        честный ноль, а не выпадает из среднего).
    """
    from core.pipeline.retrieval_docs import DocQueryResult, first_hit_rank

    results: list[DocQueryResult] = []
    for entry in entries:
        expected = tuple(expected_by_cell.get(entry.cell, ()))
        retrieved = retrieved_by_cell.get(entry.cell, ())
        resolved = tuple(doc_id for doc_id in expected if doc_id in doc_ids)
        results.append(
            DocQueryResult(
                cell=entry.cell,
                product_id=entry.product_id,
                param_id=entry.param_id,
                expected_doc_ids=expected,
                retrieved_doc_ids=retrieved,
                # Ранг — производная от выдачи: метрики пересчитывают его сами
                # (``first_hit_rank``), поэтому здесь он только для отчёта.
                rank=first_hit_rank(expected, retrieved),
                unresolved=False if force_scored else not resolved,
                resolved_doc_ids=resolved,
            )
        )
    return results


def _measure_combination(
    *,
    profile: str,
    pool_multiplier: int,
    cross_query_fusion: str,
    entries: Sequence[Any],
    params: Mapping[str, Any],
    cutoff_k: int,
    lexical_index: Any,
    vector_index: Any,
    query_vectors: Any,
    chunk_doc: dict[str, str],
    doc_ids: set[str],
    bootstrap: int,
    seed: int,
    dense_cache: dict[tuple[str, int], Any] | None = None,
    answer_sets: Any = None,
) -> dict[str, Any]:
    """Одна комбинация сетки: **один** ретривал, затем метрики по обоим семействам.

    Адресное семейство — прежнее поведение без изменений. Answer-семейство считается по
    **своему** правилу разрешённости (ячейка неразрешена, только если ни один член набора
    не найден в корпусе): его знаменатель — вопрос «нашёлся ли хоть один отвечающий
    документ», и урезать его адресной разрешённостью значило бы выкинуть ровно те ячейки,
    где адрес исчез из корпуса, а ответ остался. Отсюда два знаменателя; оба публикуются
    (``scored`` и ``answer_scored``), а ``hit_gap`` — их разность, как и объявлено.

    Чувствительность — тот же answer-``hit`` на наборе без документов, чьё происхождение
    содержит ``retrieved``; ячейки с пустым остатком остаются в знаменателе с нулём.
    """
    from core.pipeline.retrieval_answers import (
        answer_doc_ids,
        answer_doc_ids_without_retrieved,
    )
    from core.pipeline.retrieval_docs import evaluate_documents

    retrieved, chunks_without_doc = _retrieve_cells(
        profile=profile,
        pool_multiplier=pool_multiplier,
        cross_query_fusion=cross_query_fusion,
        entries=entries,
        params=params,
        cutoff_k=cutoff_k,
        lexical_index=lexical_index,
        vector_index=vector_index,
        query_vectors=query_vectors,
        chunk_doc=chunk_doc,
        dense_cache=dense_cache,
    )
    address_results = _doc_results(
        entries,
        retrieved,
        {entry.cell: tuple(entry.expected_doc_ids) for entry in entries},
        doc_ids,
    )
    address = evaluate_documents(
        address_results, profile=profile, k=cutoff_k, bootstrap=bootstrap, seed=seed
    )
    out: dict[str, Any] = {
        "retrieved": retrieved,
        "chunks_without_doc": chunks_without_doc,
        "address": address,
        "answer": None,
        "sensitivity": None,
        "answer_expected": None,
        "sensitivity_expected": None,
    }
    if answer_sets is None:
        return out

    by_cell = answer_sets.by_cell()
    answer_expected = {entry.cell: answer_doc_ids(by_cell[entry.cell]) for entry in entries}
    sensitivity_expected = {
        entry.cell: answer_doc_ids_without_retrieved(by_cell[entry.cell])
        for entry in entries
    }
    out["answer_expected"] = answer_expected
    out["sensitivity_expected"] = sensitivity_expected
    out["answer"] = evaluate_documents(
        _doc_results(entries, retrieved, answer_expected, doc_ids),
        profile=profile,
        k=cutoff_k,
        bootstrap=bootstrap,
        seed=seed,
    )
    out["sensitivity"] = evaluate_documents(
        _doc_results(entries, retrieved, sensitivity_expected, doc_ids, force_scored=True),
        profile=profile,
        k=cutoff_k,
        bootstrap=bootstrap,
        seed=seed,
    )
    return out


def _report_payload(
    report: Any,
    *,
    label: str,
    pool_multiplier: int,
    cross_query_fusion: str,
    chunks_without_doc: int = 0,
    answer: Any = None,
    sensitivity: Any = None,
    answer_by_cell: Mapping[str, Sequence[str]] | None = None,
    sensitivity_by_cell: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Отчёт комбинации как JSON: числа + по-ячеечная расшифровка (читается глазами).

    **Два семейства, одно поле-источник.** ``address_*`` — прежний вопрос «нашёлся ли
    названный адрес»; ``hit_at_k``/``recall_at_k``/``mrr``/``ndcg_at_k`` остаются
    **алиасами** адресных, потому что на них стоят публикации O14/O15 и прежние
    потребители. ``answer_*`` — «нашёлся ли хоть один документ, который решает параметр».
    ``hit_gap = answer_hit_at_k − address_hit_at_k`` — главное число §O42: сколько из
    промахов адреса были промахами вопроса, а не поиска.

    Без файла ответов (:data:`answer` ``is None``) отчёт остаётся **прежним до байта**:
    тот же набор полей и те же значения, что публиковались до появления второго
    семейства. Это не осторожность ради осторожности — на этом отчёте стоят числа
    O14/O15, и «почти то же» здесь означает, что два отчёта несравнимы без ручной
    сверки. Поэтому новые поля (``address_*``, ``unresolved_docs``,
    ``chunks_without_doc``) появляются **только вместе с** ``--answer-sets``.
    """
    from core.pipeline.retrieval_docs import first_hit_rank

    payload: dict[str, Any] = {
        "label": label,
        "profile": report.profile,
        "pool_multiplier": pool_multiplier,
        "cross_query_fusion": cross_query_fusion,
        "k": report.k,
        "cells": report.cells,
        "scored": report.scored,
        "unresolved_cells": report.unresolved_cells,
        "hit_at_k": report.hit_at_k,
        "hit_ci": list(report.hit_ci),
        "recall_at_k": report.recall_at_k,
        "recall_ci": list(report.recall_ci),
        "mrr": report.mrr,
        "ndcg_at_k": report.ndcg_at_k,
    }
    if answer is not None:
        # Адресное семейство дублируется под явным именем: ``hit_at_k`` и т.д. остаются
        # алиасами (на них стоят публикации O14/O15), а ``address_*`` называет смысл.
        payload.update(
            {
                "unresolved_docs": report.unresolved_docs,
                "chunks_without_doc": chunks_without_doc,
                "address_hit_at_k": report.hit_at_k,
                "address_recall_at_k": report.recall_at_k,
                "address_mrr": report.mrr,
                "address_ndcg_at_k": report.ndcg_at_k,
                "answer_scored": answer.scored,
                "answer_unresolved_cells": answer.unresolved_cells,
                "answer_unresolved_docs": answer.unresolved_docs,
                "answer_hit_at_k": answer.hit_at_k,
                "answer_hit_ci": list(answer.hit_ci),
                "answer_recall_at_k": answer.recall_at_k,
                "answer_recall_ci": list(answer.recall_ci),
                "answer_mrr": answer.mrr,
                "answer_ndcg_at_k": answer.ndcg_at_k,
                "hit_gap": answer.hit_at_k - report.hit_at_k,
                # Разность двух средних осмысленна как «разложение промахов», только если
                # знаменатели совпали. Ответ публикуется флагом, а не подразумевается:
                # иначе читатель не отличит разложение от сравнения разных выборок.
                "hit_gap_comparable": answer.scored == report.scored,
            }
        )
        if sensitivity is not None:
            payload.update(
                {
                    "answer_hit_at_k_wo_retrieved": sensitivity.hit_at_k,
                    "answer_hit_ci_wo_retrieved": list(sensitivity.hit_ci),
                    "answer_wo_retrieved_cells": sensitivity.scored,
                    "answer_wo_retrieved_unresolved_docs": sensitivity.unresolved_docs,
                }
            )
    payload["per_cell"] = [
        _cell_payload(
            cell,
            k=report.k,
            answer_by_cell=answer_by_cell,
            sensitivity_by_cell=sensitivity_by_cell,
            rank=first_hit_rank(cell.expected_doc_ids, cell.retrieved_doc_ids),
        )
        for cell in report.per_cell
    ]
    return payload


def _cell_payload(
    cell: Any,
    *,
    k: int,
    rank: int | None,
    answer_by_cell: Mapping[str, Sequence[str]] | None = None,
    sensitivity_by_cell: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    """Одна ячейка отчёта. ``rank``/``answer_rank`` — из выдачи, а не из поля результата.

    Ранг пересчитывается (:func:`first_hit_rank`), потому что поле ``DocQueryResult.rank``
    — то, что показывает отчёт, а не вход метрики: рассогласованный ранг развёл бы
    ``hit``/``MRR`` с ``recall``/``nDCG`` молча. ``answer_rank`` — ранг первого
    **отвечающего** документа; он не заменяет ``rank``, потому что это два разных
    вопроса к одной выдаче, и подмена сделала бы прежние числа O14 невоспроизводимыми.
    """
    from core.pipeline.retrieval_docs import first_hit_rank
    from core.util.retrieval_eval import ndcg_at_k

    payload: dict[str, Any] = {
        "cell": cell.cell,
        "product_id": cell.product_id,
        "param_id": cell.param_id,
        "expected_doc_ids": list(cell.expected_doc_ids),
        "retrieved_doc_ids": list(cell.retrieved_doc_ids),
        "rank": rank,
        # По-ячеечный nDCG публикуется и у ``unresolved``: это формула на
        # наблюдённой выдаче, а в среднее такие ячейки не входят (``scored``).
        "ndcg_at_k": ndcg_at_k(cell.expected_doc_ids, cell.retrieved_doc_ids, k),
        "unresolved": cell.unresolved,
    }
    if answer_by_cell is not None:
        answer_ids = tuple(answer_by_cell.get(cell.cell, ()))
        payload.update(
            {
                "resolved_docs": (
                    None if cell.resolved_doc_ids is None else len(cell.resolved_doc_ids)
                ),
                "answer_doc_ids": list(answer_ids),
                "answer_docs": len(answer_ids),
                "answer_rank": first_hit_rank(answer_ids, cell.retrieved_doc_ids),
            }
        )
    if sensitivity_by_cell is not None:
        payload["sensitivity_doc_ids"] = list(sensitivity_by_cell.get(cell.cell, ()))
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    """Пишет JSON с созданием каталога: отчёт — артефакт, а не временный файл."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


#: Колонки по-ячеечной таблицы: ранг — первичное доказательство, выдача воспроизводима.
#: ``label`` называет комбинацию сетки (пул и слияние), ``ndcg_at_k`` — по-ячеечная
#: метрика позиций; прежние колонки остаются на своих местах.
CELLS_CSV_COLUMNS = (
    "profile",
    "cell",
    "product_id",
    "param_id",
    "expected_doc_ids",
    "retrieved_docs",
    "rank",
    "unresolved",
    "label",
    "ndcg_at_k",
)

#: Колонки, которые добавляет answer-семейство: ``answer_rank`` — ранг первого
#: отвечающего документа, ``answers`` — сколько документов решает параметр. Без
#: ``--answer-sets`` этих колонок нет: прежний CSV остаётся прежним до байта.
CELLS_CSV_ANSWER_COLUMNS = ("answer_rank", "answers")


def _write_cells_csv(
    path: Path, reports: Sequence[dict[str, Any]], *, with_answers: bool = False
) -> None:
    """Пишет по-ячеечную таблицу (CSV): ранг на ячейку и комбинацию сетки.

    Выдача документов top-K в файл **не** попадает: она воспроизводится командой, а
    сотни ``doc_id`` по 70 символов раздули бы артефакт в разы. Публикуется то, из чего
    считается метрика: ожидания, ранг, признак неразрешённости и размер выдачи.

    :param with_answers: добавить ``answer_rank``/``answers`` (только с ``--answer-sets``).
    """
    import csv

    columns = CELLS_CSV_COLUMNS + (CELLS_CSV_ANSWER_COLUMNS if with_answers else ())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for report in reports:
            label = report.get("label", report["profile"])
            for cell in report["per_cell"]:
                ndcg = cell.get("ndcg_at_k")
                row = [
                    report["profile"],
                    cell["cell"],
                    cell["product_id"],
                    cell["param_id"],
                    " ".join(cell["expected_doc_ids"]),
                    len(cell["retrieved_doc_ids"]),
                    "" if cell["rank"] is None else cell["rank"],
                    "1" if cell["unresolved"] else "0",
                    label,
                    "" if ndcg is None else f"{ndcg:.4f}",
                ]
                if with_answers:
                    answer_rank = cell.get("answer_rank")
                    row.append("" if answer_rank is None else answer_rank)
                    row.append(cell.get("answer_docs", 0))
                writer.writerow(row)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "документный retrieval-эталон: hit/recall@K и MRR на сохранённом прогоне "
            "(O14 плана 26)"
        )
    )
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument("--golden", required=True, help="retrieval_golden_docs.json")
    parser.add_argument(
        "--index-root", default="index_store", help="корень сборок индекса"
    )
    parser.add_argument(
        "--package", default=None, help="каталог пакета (фолбэк каталога, если нет снимка)"
    )
    parser.add_argument("--run-id", default=None, help="run_id (если в файле их несколько)")
    parser.add_argument(
        "--profiles", default="R0,R1,R2", help="профили через запятую (дефолт R0,R1,R2)"
    )
    parser.add_argument(
        "--pool-multiplier",
        type=int,
        default=None,
        help="множитель пула плеч R2 (скаляр; дефолт — DEFAULT_POOL_MULTIPLIER кода)",
    )
    parser.add_argument(
        "--pool-multipliers",
        default=None,
        help="множители пула R2 через запятую (например 1,2,4,8); перекрывает "
        "--pool-multiplier. Применяется только к R2",
    )
    parser.add_argument(
        "--cross-query-fusion",
        default=None,
        help="политика кросс-запросного слияния (скаляр; дефолт — DEFAULT_CROSS_QUERY_FUSION)",
    )
    parser.add_argument(
        "--cross-query-fusions",
        default=None,
        help="политики слияния через запятую (например max,rrf,per_query); "
        "перекрывает --cross-query-fusion",
    )
    parser.add_argument(
        "--cutoff-k",
        type=int,
        default=0,
        help="K эталона; 0 — из runs.retrieval_cutoff_K (I20), иначе 10",
    )
    parser.add_argument(
        "--deploy", default="config/deploy.yaml", help="deploy-конфиг (оси эмбеддера)"
    )
    parser.add_argument("--bootstrap", type=int, default=1000, help="ресемплов бутстрэпа")
    parser.add_argument("--seed", type=int, default=0, help="seed бутстрэпа (объявленный)")
    parser.add_argument(
        "--limit-cells",
        type=int,
        default=0,
        help="первые N ячеек (0 — все); для дымового прогона, число попадает в отчёт",
    )
    parser.add_argument(
        "--out-json",
        metavar="PATH",
        help="полный машинный отчёт (с по-ячеечной выдачей документов)",
    )
    parser.add_argument(
        "--out-summary",
        metavar="PATH",
        help="сводка без по-ячеечной выдачи (то, что коммитится как доказательство)",
    )
    parser.add_argument(
        "--out-csv",
        metavar="PATH",
        help="по-ячеечная таблица: профиль, ячейка, ожидаемые документы, ранг",
    )
    parser.add_argument(
        "--answer-sets",
        metavar="PATH",
        default=None,
        help=(
            "эталон отвечающих документов (cohound-retrieval-golden-answers/1): "
            "включает answer-семейство метрик и hit_gap"
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    db_path = Path(args.db).expanduser()
    if not db_path.is_file():
        print(f"measure: БД не найдена: {db_path}", file=sys.stderr)
        return 2
    index_root = Path(args.index_root).expanduser()

    from core.pipeline.retrieval_docs import load_doc_golden

    case_id, golden_run_id, entries = load_doc_golden(args.golden)
    if args.limit_cells > 0:
        entries = entries[: args.limit_cells]

    conn = _open_readonly(db_path)
    chunks_conn = _open_readonly(db_path)
    try:
        meta = _read_run(conn, args.run_id)
        if golden_run_id and golden_run_id != meta.run_id:
            print(
                f"measure: эталон собран на прогоне {golden_run_id!r}, а измеряется "
                f"{meta.run_id!r}: doc_id разных корпусов несравнимы — нужен эталон, "
                "скомпилированный на этом прогоне",
                file=sys.stderr,
            )
            return 2
        package_id = meta.package_id or case_id
        params = {
            str(param.param_id): param
            for param in _load_params(
                db_path,
                package_id,
                Path(args.package).expanduser() if args.package else None,
            )
        }
        cutoff_k = args.cutoff_k or meta.cutoff_k or 10
        if cutoff_k <= 0:
            print("measure: K должен быть положительным", file=sys.stderr)
            return 2

        from core.pipeline.composition import read_composition

        view = read_composition(index_root, package_id, meta.build_id)
        if view is None:
            print(
                f"measure: композиция {meta.build_id!r} не найдена под {index_root}: "
                "сборка индекса не разрешается",
                file=sys.stderr,
            )
            return 2
        first_product = next(iter(view.members))
        dim = _partition_dim(
            index_root, package_id, view.members[first_product], first_product
        )
        index_embed_model = _partition_embed_model(
            index_root, package_id, view.members[first_product], first_product
        )

        profiles = [item.strip().upper() for item in args.profiles.split(",") if item.strip()]
        if not profiles:
            print("measure: --profiles пуст", file=sys.stderr)
            return 2

        from core.pipeline.retrieve import (
            DEFAULT_CROSS_QUERY_FUSION,
            DEFAULT_POOL_MULTIPLIER,
        )

        pool_scalar = (
            DEFAULT_POOL_MULTIPLIER if args.pool_multiplier is None else args.pool_multiplier
        )
        fusion_scalar = (
            DEFAULT_CROSS_QUERY_FUSION
            if args.cross_query_fusion is None
            else args.cross_query_fusion
        )
        try:
            pool_multipliers = _resolve_pool_multipliers(
                pool_scalar, args.pool_multipliers
            )
            cross_query_fusions = _resolve_cross_query_fusions(
                fusion_scalar, args.cross_query_fusions
            )
        except SystemExit as exc:
            print(str(exc), file=sys.stderr)
            return 2
        grid = _measurement_grid(profiles, cross_query_fusions, pool_multipliers)

        missing_params = [entry.cell for entry in entries if entry.param_id not in params]
        if missing_params:
            print(
                f"measure: {len(missing_params)} ячеек эталона ссылаются на param_id, "
                f"которых нет в каталоге прогона (первые: {', '.join(missing_params[:5])}) — "
                "запрос не рендерится",
                file=sys.stderr,
            )
            return 2

        # Файл ответов сверяется с эталоном **до** сборки индекса: расхождение составов —
        # отказ, и платить за лексический индекс, чтобы затем отказать, незачем.
        answer_sets = None
        if args.answer_sets:
            try:
                answer_sets = _answer_sets_for(
                    args.answer_sets,
                    entries=entries,
                    run_id=meta.run_id,
                    case_id=case_id,
                )
            except SystemExit as exc:
                print(str(exc), file=sys.stderr)
                return 2

        from core.config.deploy import load_deploy_config

        deploy = load_deploy_config(args.deploy)
        notices: list[str] = []
        embedder = None
        if any(profile in ("R1", "R2") for profile in profiles):
            # Объявленные оси эмбеддера, а не «первый доступный адаптер»: та же сборка,
            # что у прогона (сверка модели ниже), иначе запрос и документы окажутся в
            # разных пространствах под одним именем профиля.
            from core.cli.wiring import _resolve_embedder

            embedder = _resolve_embedder(
                deploy.embed.device,
                precision=deploy.embed.precision,
                model_id=deploy.embed.model_id,
                revision=deploy.embed.revision,
                notices=notices,
            )
            if embedder is None:
                print(
                    "measure: dense-профиль запрошен, но embed.model_id не объявлен в "
                    f"{args.deploy} — вектор запроса нечем посчитать",
                    file=sys.stderr,
                )
                return 2
            observed = str(getattr(embedder, "model_id", ""))
            if index_embed_model and not observed.startswith(index_embed_model):
                print(
                    f"measure: объявленная модель {observed!r} не та, которой построены "
                    f"векторы партиции ({index_embed_model!r}): запрос и документы были бы "
                    "в разных пространствах",
                    file=sys.stderr,
                )
                return 2

        from core.adapters.lexical_tantivy import TantivyLexicalIndex
        from core.adapters.vector_npy import NpyVectorIndex

        lookup = _chunk_lookup(chunks_conn)
        started = time.perf_counter()
        lexical_index = None
        if any(profile in ("R0", "R2") for profile in profiles):
            lexical_index = TantivyLexicalIndex(
                profile=deploy.lexical.profile,
                hydrate=lambda ids: {
                    chunk_id: chunk
                    for chunk_id in ids
                    if (chunk := lookup(chunk_id)) is not None
                },
                view=view,
            )
            lexical_index.index(
                _iter_lexical_records(conn, meta.build_id), build_id=meta.build_id
            )
        lexical_seconds = time.perf_counter() - started

        vector_index = None
        if any(profile in ("R1", "R2") for profile in profiles):
            vector_index = NpyVectorIndex(
                index_root,
                package_id,
                meta.build_id,
                dim,
                lookup,
                view=view,
            )

        chunk_doc = _chunk_doc_map(conn)
        doc_ids = _corpus_doc_ids(conn)

        from core.pipeline.evaluate import precompute_query_vectors

        query_vectors = None
        if embedder is not None:
            # Векторы запросов считаются один раз на всю сетку: они не зависят ни от
            # политики слияния, ни от множителя пула — только от текста и модели.
            query_vectors = precompute_query_vectors(
                [params[entry.param_id] for entry in entries],
                profile="R1",  # вектор нужен любому dense-профилю; R0 его не использует
                embedder=embedder,
            )

        reports: list[dict[str, Any]] = []
        timings: dict[str, float] = {"lexical_index_seconds": round(lexical_seconds, 2)}
        # Кеш dense-плеча — по ``(продукт, множитель пула)``: окно батча равно
        # ``cutoff_k * pool_multiplier``, поэтому хиты одного пула нельзя отдать
        # комбинации с другим (иначе ``pool=8`` померил бы пул 1). Пул в ключе делает
        # переиспользование внутри сетки безопасным и при этом дешёвым.
        dense_cache: dict[tuple[str, int], Any] = {}
        for profile, pool_multiplier, cross_query_fusion in grid:
            label = _combination_label(profile, pool_multiplier, cross_query_fusion)
            combination_started = time.perf_counter()
            measured = _measure_combination(
                profile=profile,
                pool_multiplier=pool_multiplier,
                cross_query_fusion=cross_query_fusion,
                entries=entries,
                params=params,
                cutoff_k=cutoff_k,
                lexical_index=lexical_index,
                vector_index=vector_index,
                query_vectors=query_vectors,
                chunk_doc=chunk_doc,
                doc_ids=doc_ids,
                bootstrap=args.bootstrap,
                seed=args.seed,
                dense_cache=dense_cache,
                answer_sets=answer_sets,
            )
            timings[f"{label}_seconds"] = round(
                time.perf_counter() - combination_started, 2
            )
            reports.append(
                _report_payload(
                    measured["address"],
                    label=label,
                    pool_multiplier=pool_multiplier,
                    cross_query_fusion=cross_query_fusion,
                    chunks_without_doc=measured["chunks_without_doc"],
                    answer=measured["answer"],
                    sensitivity=measured["sensitivity"],
                    answer_by_cell=measured["answer_expected"],
                    sensitivity_by_cell=measured["sensitivity_expected"],
                )
            )

        payload: dict[str, Any] = {
            "run_id": meta.run_id,
            "case_id": case_id,
            "conditions": {
                "schema_version_of_file": meta.schema_version,
                "index_build_id": meta.build_id,
                "cutoff_k": cutoff_k,
                "rrf_k": _rrf_k(),
                "lexical_profile": deploy.lexical.profile,
                "embed_spec": getattr(embedder, "model_id", None),
                "index_embed_model_id": index_embed_model,
                "dim": dim,
                "profiles": profiles,
                "pool_multipliers": list(pool_multipliers),
                "cross_query_fusions": list(cross_query_fusions),
                "grid": [report["label"] for report in reports],
                "bootstrap": args.bootstrap,
                "seed": args.seed,
                "cells_in_golden": len(entries),
                "limit_cells": args.limit_cells,
                "chunks_in_index": len(chunk_doc),
                "docs_in_corpus": len(doc_ids),
                "wiring_notices": notices,
            },
            "timings": timings,
            "reports": reports,
        }
        if answer_sets is not None:
            # Условия answer-семейства: без них ``hit_gap`` не воспроизводится — набор
            # ответов, его политика и статус разметки суть входы числа, а не украшение.
            from core.pipeline.retrieval_answers import answers_summary

            payload["conditions"]["answer_sets"] = {
                "path": str(args.answer_sets),
                "status": answer_sets.status,
                "reviewed_by": answer_sets.reviewed_by,
                "judge": (
                    None
                    if answer_sets.judge is None
                    else {
                        "model_id": answer_sets.judge.model_id,
                        "revision": answer_sets.judge.revision,
                        "prompt_hash": answer_sets.judge.prompt_hash,
                    }
                ),
                "policy": answer_sets.policy.as_dict(),
                "summary": answers_summary(answer_sets.entries),
            }
        if args.out_json:
            _write_json(Path(args.out_json).expanduser(), payload)
        if args.out_summary:
            summary = {
                key: value
                for key, value in payload.items()
                if key != "reports"
            }
            summary["reports"] = [
                {key: value for key, value in report.items() if key != "per_cell"}
                for report in reports
            ]
            _write_json(Path(args.out_summary).expanduser(), summary)
        if args.out_csv:
            _write_cells_csv(
                Path(args.out_csv).expanduser(),
                reports,
                with_answers=answer_sets is not None,
            )

        print(
            f"прогон {meta.run_id} (схема файла {meta.schema_version}), сборка "
            f"{meta.build_id}, K={cutoff_k}, ячеек {len(entries)}, чанков "
            f"{len(chunk_doc)}, документов {len(doc_ids)}"
        )
        print("")
        print(
            f"{'комбинация':<28} {'scored':>6} {'unres':>6} {'hit@K':>7} {'hit CI':>17} "
            f"{'recall@K':>9} {'recall CI':>17} {'MRR':>7} {'nDCG@K':>7}"
        )
        for report in reports:
            hit_low, hit_high = report["hit_ci"]
            rec_low, rec_high = report["recall_ci"]
            print(
                f"{report['label']:<28} {report['scored']:>6} "
                f"{report['unresolved_cells']:>6} {report['hit_at_k']:>7.3f} "
                f"[{hit_low:.3f}; {hit_high:.3f}] "
                f"{report['recall_at_k']:>9.3f} [{rec_low:.3f}; {rec_high:.3f}] "
                f"{report['mrr']:>7.3f} {report['ndcg_at_k']:>7.3f}"
            )
        print("")
        costs: list[str] = []
        if lexical_index is not None:
            costs.append(f"лексический индекс {timings['lexical_index_seconds']:.1f} с")
        for report in reports:
            label = report["label"]
            costs.append(f"{label} {timings[f'{label}_seconds']:.1f} с")
        print("цена: " + ", ".join(costs))
        if answer_sets is not None:
            _print_answer_table(reports)
            from core.pipeline.retrieval_answers import answers_summary

            summary = answers_summary(answer_sets.entries)
            print(
                f"набор ответов: {summary['cells']} ячеек, документов "
                f"{summary['documents']} (различных {summary['distinct_documents']}), "
                f"статус {answer_sets.status}"
            )
            sources = summary["cells_sources"]
            print(
                "ячеек по источникам: "
                + ", ".join(f"{name} {count}" for name, count in sources.items())
            )
            for report in reports:
                if "unresolved_docs" in report:
                    print(
                        f"{report['label']}: членов набора не разрешилось в корпусе "
                        f"{report['answer_unresolved_docs']}, чанков без документа в "
                        f"выдаче {report['chunks_without_doc']}"
                    )
        for notice in notices:
            print(f"замечание проводки: {notice}")
    finally:
        chunks_conn.close()
        conn.close()
    return 0


def _rrf_k() -> int:
    """Исполненная RRF-константа по умолчанию (условие, а не настройка прибора)."""
    from core.pipeline.retrieve import DEFAULT_RRF_K

    return DEFAULT_RRF_K


def _answer_sets_for(
    path: str,
    *,
    entries: Sequence[Any],
    run_id: str,
    case_id: str,
) -> Any:
    """Загружает файл ответов и сверяет его с эталоном **в обе стороны**.

    Расхождение составов — отказ, а не предупреждение: «эталон покрывает 132 ячейки,
    ответы 90» не должно пройти молча, иначе ``hit_gap`` считался бы по разным выборкам
    и читался как свойство поиска. Причина отказа печатается с числом и первыми пятью
    адресами — чтобы её можно было починить, не открывая файл.

    :raises SystemExit: файл не читается, чужой ``run_id``/``case_id`` или составы
        ячеек не совпадают.
    """
    from core.pipeline.retrieval_answers import load_answer_sets

    try:
        sets = load_answer_sets(path)
    except (OSError, ValueError) as exc:
        raise SystemExit(f"answer-sets: {path}: {exc}") from exc
    if sets.run_id != run_id:
        raise SystemExit(
            f"answer-sets: набор собран на прогоне {sets.run_id!r}, а измеряется "
            f"{run_id!r}: doc_id разных корпусов несравнимы"
        )
    if sets.case_id != case_id:
        raise SystemExit(
            f"answer-sets: набор собран для пакета {sets.case_id!r}, а эталон — "
            f"{case_id!r}"
        )
    golden_cells = {entry.cell for entry in entries}
    answer_cells = set(sets.by_cell())
    missing = sorted(golden_cells - answer_cells)
    extra = sorted(answer_cells - golden_cells)
    if missing:
        raise SystemExit(
            f"answer-sets: {len(missing)} ячеек эталона отсутствуют в файле ответов "
            f"(первые: {', '.join(missing[:5])}) — answer-семейство считалось бы по "
            "другой выборке"
        )
    if extra:
        raise SystemExit(
            f"answer-sets: {len(extra)} ячеек файла ответов отсутствуют в эталоне "
            f"(первые: {', '.join(extra[:5])}) — набор описывает ячейки, которых нет "
            "в измерении"
        )
    return sets


def _print_answer_table(reports: Sequence[dict[str, Any]]) -> None:
    """Строки answer-семейства: главное число §O42 (``hit_gap``) и чувствительность.

    Печатается отдельно от адресной таблицы, потому что это ответ на другой вопрос:
    «нашёлся ли хоть один документ, решающий параметр», а не «нашёлся ли названный адрес».
    """
    print("")
    print(
        f"{'комбинация':<28} {'answer_hit':>10} {'answer CI':>17} {'answer_rec':>10} "
        f"{'answer_MRR':>10} {'answer_nDCG':>11} {'hit_gap':>8} "
        f"{'wo_retrieved':>12} {'wo_ret CI':>17}"
    )
    for report in reports:
        if "answer_hit_at_k" not in report:
            continue
        low, high = report["answer_hit_ci"]
        wo_low, wo_high = report.get("answer_hit_ci_wo_retrieved", (0.0, 0.0))
        print(
            f"{report['label']:<28} {report['answer_hit_at_k']:>10.3f} "
            f"[{low:.3f}; {high:.3f}] {report['answer_recall_at_k']:>10.3f} "
            f"{report['answer_mrr']:>10.3f} {report['answer_ndcg_at_k']:>11.3f} "
            f"{report['hit_gap']:>+8.3f} "
            f"{report.get('answer_hit_at_k_wo_retrieved', 0.0):>12.3f} "
            f"[{wo_low:.3f}; {wo_high:.3f}]"
        )
    print(
        "hit_gap = answer_hit@K − address_hit@K: сколько промахов адреса были промахами "
        "вопроса, а не поиска"
    )
    for report in reports:
        if "hit_gap_comparable" in report and not report["hit_gap_comparable"]:
            print(
                f"ВНИМАНИЕ  {report['label']}: знаменатели разные (адрес {report['scored']}, "
                f"ответ {report['answer_scored']}) — hit_gap сравнивает две выборки; "
                "разложение промахов читается по ячейкам, а не по разности средних"
            )


if __name__ == "__main__":
    raise SystemExit(main())
