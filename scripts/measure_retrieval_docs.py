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
    По каждому профилю (``R0``/``R1``/``R2``): ``hit@K``, ``recall@K``, ``MRR``,
    бутстрэп-интервалы (1000 ресемплов по ячейкам, ``seed=0``), число ячеек, число
    разрешённых (``scored``) и неразрешённых (``unresolved``) и ``per_cell`` — по каждой
    ячейке ожидаемые и найденные документы, ранг.
``conditions``
    Профиль/K/``rrf_k``, объявленные оси эмбеддера, сборка композиции, версия схемы файла,
    профиль лексического анализатора, число чанков в индексе и документов в корпусе.

**Чего прибор НЕ измеряет:** κ оценщика (это Ш6 и ``evaluator-benchmark``), качество
цитат (I22), стоимость эмбеддинга корпуса. Только ретривал.

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
from collections.abc import Iterator, Sequence
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
# Прогон одного профиля
# ---------------------------------------------------------------------------


def _measure_profile(
    *,
    profile: str,
    entries: Sequence[Any],
    params: dict[str, Any],
    cutoff_k: int,
    lexical_index: Any,
    vector_index: Any,
    embedder: Any,
    chunk_doc: dict[str, str],
    doc_ids: set[str],
    bootstrap: int,
    seed: int,
) -> Any:
    """Ячейки одного профиля: ретривал композером фазы → документы top-K → метрики."""
    from core.pipeline.evaluate import (
        precompute_dense_hits,
        precompute_query_vectors,
        retrieve_for_cell,
    )
    from core.pipeline.retrieval_docs import DocQueryResult, document_ids_for

    by_product: dict[str, list[Any]] = {}
    for entry in entries:
        by_product.setdefault(entry.product_id, []).append(entry)

    query_vectors = precompute_query_vectors(
        [params[entry.param_id] for entry in entries],
        profile=profile,
        embedder=embedder,
    )
    dense_by_product: dict[str, Any] = {}
    for product_id, product_entries in by_product.items():
        dense_by_product[product_id] = precompute_dense_hits(
            [params[entry.param_id] for entry in product_entries],
            product_ids=[product_id],
            profile=profile,
            cutoff_k=cutoff_k,
            vector_index=vector_index,
            query_vectors=query_vectors,
        )

    results: list[DocQueryResult] = []
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
            embedder=embedder,
            query_vectors=query_vectors,
            dense_by_query=dense_by_product.get(entry.product_id),
        )
        retrieved = document_ids_for(
            [chunk.chunk_id for chunk in retrieval.chunks], chunk_doc, top_k=cutoff_k
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

    from core.pipeline.retrieval_docs import evaluate_documents

    return evaluate_documents(results, profile=profile, k=cutoff_k,
                              bootstrap=bootstrap, seed=seed)


def _report_payload(report: Any) -> dict[str, Any]:
    """Отчёт профиля как JSON: числа + по-ячеечная расшифровка (читается глазами)."""
    return {
        "profile": report.profile,
        "k": report.k,
        "cells": report.cells,
        "scored": report.scored,
        "unresolved_cells": report.unresolved_cells,
        "hit_at_k": report.hit_at_k,
        "hit_ci": list(report.hit_ci),
        "recall_at_k": report.recall_at_k,
        "recall_ci": list(report.recall_ci),
        "mrr": report.mrr,
        "per_cell": [
            {
                "cell": cell.cell,
                "product_id": cell.product_id,
                "param_id": cell.param_id,
                "expected_doc_ids": list(cell.expected_doc_ids),
                "retrieved_doc_ids": list(cell.retrieved_doc_ids),
                "rank": cell.rank,
                "unresolved": cell.unresolved,
            }
            for cell in report.per_cell
        ],
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    """Пишет JSON с созданием каталога: отчёт — артефакт, а не временный файл."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


#: Колонки по-ячеечной таблицы: ранг — первичное доказательство, выдача воспроизводима.
CELLS_CSV_COLUMNS = (
    "profile",
    "cell",
    "product_id",
    "param_id",
    "expected_doc_ids",
    "retrieved_docs",
    "rank",
    "unresolved",
)


def _write_cells_csv(path: Path, reports: Sequence[dict[str, Any]]) -> None:
    """Пишет по-ячеечную таблицу (CSV): ранг на ячейку и профиль.

    Выдача документов top-K в файл **не** попадает: она воспроизводится командой, а
    сотни ``doc_id`` по 70 символов раздули бы артефакт в разы. Публикуется то, из чего
    считается метрика: ожидания, ранг, признак неразрешённости и размер выдачи.
    """
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(CELLS_CSV_COLUMNS)
        for report in reports:
            for cell in report["per_cell"]:
                writer.writerow(
                    [
                        report["profile"],
                        cell["cell"],
                        cell["product_id"],
                        cell["param_id"],
                        " ".join(cell["expected_doc_ids"]),
                        len(cell["retrieved_doc_ids"]),
                        "" if cell["rank"] is None else cell["rank"],
                        "1" if cell["unresolved"] else "0",
                    ]
                )


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

        reports: list[dict[str, Any]] = []
        timings: dict[str, float] = {"lexical_index_seconds": round(lexical_seconds, 2)}
        for profile in profiles:
            profile_started = time.perf_counter()
            report = _measure_profile(
                profile=profile,
                entries=entries,
                params=params,
                cutoff_k=cutoff_k,
                lexical_index=lexical_index,
                vector_index=vector_index,
                embedder=embedder,
                chunk_doc=chunk_doc,
                doc_ids=doc_ids,
                bootstrap=args.bootstrap,
                seed=args.seed,
            )
            timings[f"{profile}_seconds"] = round(time.perf_counter() - profile_started, 2)
            reports.append(_report_payload(report))

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
            _write_cells_csv(Path(args.out_csv).expanduser(), reports)

        print(
            f"прогон {meta.run_id} (схема файла {meta.schema_version}), сборка "
            f"{meta.build_id}, K={cutoff_k}, ячеек {len(entries)}, чанков "
            f"{len(chunk_doc)}, документов {len(doc_ids)}"
        )
        print("")
        print(
            f"{'профиль':<8} {'scored':>6} {'unres':>6} {'hit@K':>7} {'hit CI':>17} "
            f"{'recall@K':>9} {'recall CI':>17} {'MRR':>7}"
        )
        for report in reports:
            hit_low, hit_high = report["hit_ci"]
            rec_low, rec_high = report["recall_ci"]
            print(
                f"{report['profile']:<8} {report['scored']:>6} "
                f"{report['unresolved_cells']:>6} {report['hit_at_k']:>7.3f} "
                f"[{hit_low:.3f}; {hit_high:.3f}] "
                f"{report['recall_at_k']:>9.3f} [{rec_low:.3f}; {rec_high:.3f}] "
                f"{report['mrr']:>7.3f}"
            )
        print("")
        costs: list[str] = []
        if lexical_index is not None:
            costs.append(f"лексический индекс {timings['lexical_index_seconds']:.1f} с")
        costs.extend(
            f"{profile} {timings[f'{profile}_seconds']:.1f} с" for profile in profiles
        )
        print("цена: " + ", ".join(costs))
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


if __name__ == "__main__":
    raise SystemExit(main())
