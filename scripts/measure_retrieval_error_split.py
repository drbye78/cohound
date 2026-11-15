"""Прибор: разложение ошибки на «поиск не дал» и «оценщик не взял» (O15 плана 26).

**Вопрос.** Для ячейки с вердиктом `❓` (или `✅`, полученным вопреки промаху поиска) надо
знать, был ли релевантный документ в top-K. Если был — провал на стороне оценщика; если не
был — на стороне поиска. Это два разных адресата вложений, и без разложения выбор между ними
делается по вере.

**Предмет измерения — таблица сопряжённости** «вердикт прогона × найден ли ожидаемый
документ эталона в top-K». Она читается как число, а не как мнение:

``search_missed``
    Ожидаемого документа нет в top-K, а вердикт `❓` — провал поиска (или отсутствия
    документа в корпусе, см. ниже).
``evaluator_missed``
    Документ в top-K есть, а вердикт `❓` — материал был, вердикт не вынесен.
``consistent_positive``
    Документ в top-K, вердикт `✅`/`⚠️` — контур сработал.
``lucky``
    Документа в top-K нет, а вердикт `✅`/`⚠️` — вердикт опирается не на тот документ,
    который объявил эталон (не обязательно дефект: цитата могла быть из другого документа,
    но это надо видеть, а не предполагать).

**Граница замера, названная честно.** Разложение возможно только для ячеек, у которых
**объявлен** ожидаемый документ (документный эталон O14). Ячейки без эталона в разложение не
входят и считаются отдельно (`cells_without_golden`): «поиск не дал» для них не измеряется,
потому что неизвестно, что он должен был дать.

**Почему чтение сырым ``sqlite3``.** Гейт схемы отказывает файлу прогона, чья
``schema_version`` старше кода; прибор читает данные `mode=ro` и **публикует** версию схемы
файла в отчёте — число относится к корпусу, записанному этим файлом.

Нейтрален (§1.3): ни продуктов, ни домена — пакет и эталон приходят аргументами.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

#: Вердикты, которые считаются «контур дал ответ» (✅/⚠️ — есть доказательство).
_POSITIVE = ("✅", "⚠️")
#: Вердикты «ответа нет» (❓) и «отсутствие» (❌ — предмет другого замера, I2d).
_UNKNOWN = ("❓",)


def _open_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _iter_records(conn: sqlite3.Connection, build_id: str) -> Iterator[Any]:
    """Записи лексического индекса потоком (список 253k чанков в память не берётся)."""
    from core.adapters.lexical_tantivy import LexicalChunk

    for row in conn.execute(
        "SELECT chunk_id, product_id, text, heading_path, content_hash FROM corpus_chunks"
    ):
        yield LexicalChunk(
            chunk_id=str(row["chunk_id"]),
            product_id=str(row["product_id"]),
            text=str(row["text"] or ""),
            heading_path=str(row["heading_path"] or ""),
            content_hash=str(row["content_hash"] or ""),
            build_id=build_id,
        )


def _chunk_lookup(conn: sqlite3.Connection) -> Any:
    """Гидртация top-K: точка-чтение чанка по ``chunk_id``."""
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


def classify(status: str | None, found: bool) -> str:
    """Класс ячейки: «кто промахнулся» — поиск, оценщик, или всё сработало.

    ``status=None`` (``failed``/``preliminary``, I4/I13) — не вердикт, а отсутствие
    измерения: ячейка попадает в ``no_verdict`` и в разложение не входит. Иначе «не
    измерили» читалось бы как «поиск не дал».
    """
    if status is None or not status:
        return "no_verdict"
    if status in _UNKNOWN:
        return "evaluator_missed" if found else "search_missed"
    if status in _POSITIVE:
        return "consistent_positive" if found else "lucky"
    # ❌ — отсутствие; поиск документа тут не предмет (I2d: носитель — наблюдение/исчерпание).
    return "absence_cell"


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "разложение ошибки: поиск или оценщик (O15 плана 26) на документном эталоне"
        )
    )
    parser.add_argument("--db", required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument("--golden", required=True, help="retrieval_golden_docs.json")
    parser.add_argument("--index-root", default="index_store")
    parser.add_argument("--package", default=None, help="каталог пакета (фолбэк каталога)")
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--profiles", default="R0", help="профили через запятую (дефолт R0 — как в прогоне)"
    )
    parser.add_argument("--cutoff-k", type=int, default=0, help="0 — из runs.retrieval_cutoff_K")
    parser.add_argument("--lexical-profile", default="stemmed")
    parser.add_argument("--out-json", metavar="PATH", help="машинный отчёт")
    parser.add_argument("--out-csv", metavar="PATH", help="по-ячеечная таблица")
    return parser.parse_args(argv)


def _cell_statuses(conn: sqlite3.Connection, run_id: str) -> dict[tuple[str, str], str | None]:
    """Вердикт ячейки прогона: ``(product_id, param_id) → status | None``."""
    out: dict[tuple[str, str], str | None] = {}
    for row in conn.execute(
        "SELECT product_id, param_id, status FROM cells WHERE run_id = ?", (run_id,)
    ):
        out[(str(row["product_id"]), str(row["param_id"]))] = row["status"]
    return out


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    db = Path(args.db).expanduser()
    if not db.is_file():
        print(f"measure: БД не найдена: {db}", file=sys.stderr)
        return 2

    from core.pipeline.retrieval_docs import load_doc_golden

    case_id, golden_run_id, entries = load_doc_golden(args.golden)

    conn = _open_readonly(db)
    chunks_conn = _open_readonly(db)
    try:
        rows = conn.execute(
            "SELECT run_id, package_id, index_build_id, retrieval_cutoff_K FROM runs"
        ).fetchall()
        if len(rows) != 1:
            print(
                f"measure: в файле {len(rows)} прогонов — назовите прогон одним файлом",
                file=sys.stderr,
            )
            return 2
        run = rows[0]
        run_id = str(run["run_id"])
        if golden_run_id and golden_run_id != run_id:
            print(
                f"measure: эталон собран на прогоне {golden_run_id!r}, а читается "
                f"{run_id!r}: doc_id разных корпусов несравнимы",
                file=sys.stderr,
            )
            return 2
        build_id = str(run["index_build_id"] or "")
        if not build_id:
            print("measure: у прогона нет index_build_id", file=sys.stderr)
            return 2
        cutoff_k = args.cutoff_k or int(run["retrieval_cutoff_K"] or 0) or 10
        schema_row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        schema_version = str(schema_row["value"]) if schema_row is not None else ""

        snapshot = db.parent / "plan_inputs.json"
        if not snapshot.is_file():
            print(f"measure: нет снимка входов прогона: {snapshot}", file=sys.stderr)
            return 2
        from core.domain.models import CatalogRow

        payload = json.loads(snapshot.read_text(encoding="utf-8"))
        params = {
            str(item["param_id"]): CatalogRow.model_validate(item)
            for item in payload.get("params", [])
        }

        statuses = _cell_statuses(conn, run_id)
        lookup = _chunk_lookup(chunks_conn)
        # Один проход по корпусу на весь замер: карта «чанк → документ» и обратная
        # «хеш текста → документы». Второй нужен, чтобы назвать документ, из которого
        # взята цитата: ``cell_evidence.source_url`` у документационного контура пуст,
        # адрес есть только у текста через ``content_hash``.
        chunk_doc, hash_docs = _corpus_maps(conn)
        citations = _citation_docs(conn, run_id, hash_docs)

        from core.adapters.lexical_tantivy import TantivyLexicalIndex
        from core.pipeline.evaluate import retrieve_for_cell
        from core.pipeline.retrieval_docs import document_ids_for

        index = TantivyLexicalIndex(
            profile=args.lexical_profile,
            hydrate=lambda ids: {
                cid: chunk for cid in ids if (chunk := lookup(cid)) is not None
            },
        )
        index.index(_iter_records(conn, build_id), build_id=build_id)

        profiles = [item.strip().upper() for item in args.profiles.split(",") if item.strip()]
        reports: list[dict[str, Any]] = []
        for profile in profiles:
            cells: list[dict[str, Any]] = []
            for entry in entries:
                param = params.get(entry.param_id)
                if param is None:
                    print(
                        f"measure: ячейка {entry.cell}: param_id отсутствует в каталоге",
                        file=sys.stderr,
                    )
                    return 2
                result = retrieve_for_cell(
                    param,
                    product_id=entry.product_id,
                    profile=profile,
                    cutoff_k=cutoff_k,
                    lexical_index=index,
                )
                docs = document_ids_for(
                    [chunk.chunk_id for chunk in result.chunks], chunk_doc, top_k=cutoff_k
                )
                found = any(doc in entry.expected_doc_ids for doc in docs)
                status = statuses.get((entry.product_id, entry.param_id))
                cited = citations.get((entry.product_id, entry.param_id), frozenset())
                expected = set(entry.expected_doc_ids)
                cells.append(
                    {
                        "cell": entry.cell,
                        "product_id": entry.product_id,
                        "param_id": entry.param_id,
                        "status": status,
                        "found": found,
                        "class": classify(status, found),
                        "cited_doc_ids": sorted(cited),
                        "citation_hits_golden": (
                            None if not cited else bool(cited & expected)
                        ),
                    }
                )
            counts = Counter(cell["class"] for cell in cells)
            # «Повезло» различается по адресу цитаты: документ эталона или другой.
            # Это не косметика: если ответ пришёл из другого документа, промах поиска
            # по эталону — свойство эталона (один свидетель из нескольких), а не поиска.
            lucky = [cell for cell in cells if cell["class"] == "lucky"]
            lucky_split = Counter(
                "golden"
                if cell["citation_hits_golden"] is True
                else "other"
                if cell["citation_hits_golden"] is False
                else "unknown"
                for cell in lucky
            )
            reports.append(
                {
                    "profile": profile,
                    "cells": len(cells),
                    "counts": dict(sorted(counts.items())),
                    "lucky_citation": dict(sorted(lucky_split.items())),
                    "per_cell": cells,
                }
            )

        out: dict[str, Any] = {
            "run_id": run_id,
            "case_id": case_id,
            "conditions": {
                "schema_version_of_file": schema_version,
                "index_build_id": build_id,
                "cutoff_k": cutoff_k,
                "lexical_profile": args.lexical_profile,
                "profiles": profiles,
                "cells_in_golden": len(entries),
                "cells_without_golden": max(0, _cells_total(conn, run_id) - len(entries)),
            },
            "reports": reports,
        }
        if args.out_json:
            _write_json(Path(args.out_json).expanduser(), out)
        if args.out_csv:
            _write_csv(Path(args.out_csv).expanduser(), reports)

        print(
            f"прогон {run_id} (схема файла {schema_version}), сборка {build_id}, K={cutoff_k}, "
            f"ячеек эталона {len(entries)} из {_cells_total(conn, run_id)} в прогоне"
        )
        print("")
        print(f"{'профиль':<8} {'ячеек':>6} " + " ".join(
            f"{name:>20}" for name in (
                "search_missed", "evaluator_missed", "consistent_positive", "lucky",
                "absence_cell", "no_verdict",
            )
        ))
        for report in reports:
            counts = report["counts"]
            lucky = report["lucky_citation"]
            print(
                f"{report['profile']:<8} {report['cells']:>6} "
                + " ".join(
                    f"{counts.get(name, 0):>20}"
                    for name in (
                        "search_missed", "evaluator_missed", "consistent_positive",
                        "lucky", "absence_cell", "no_verdict",
                    )
                )
            )
            print(
                f"{'':8} {'':>6} из «повезло» цитата: из документа эталона "
                f"{lucky.get('golden', 0)}, из другого документа {lucky.get('other', 0)}, "
                f"адрес неизвестен {lucky.get('unknown', 0)}"
            )
    finally:
        chunks_conn.close()
        conn.close()
    return 0


def _cells_total(conn: sqlite3.Connection, run_id: str) -> int:
    """Сколько ячеек у прогона (знаменатель «без эталона»)."""
    row = conn.execute(
        "SELECT count(*) AS n FROM cells WHERE run_id = ?", (run_id,)
    ).fetchone()
    return int(row["n"]) if row is not None else 0


def _chunk_doc(conn: sqlite3.Connection) -> dict[str, str]:
    """``chunk_id → doc_id``: адрес документа для top-K выдачи (один проход по корпусу)."""
    return {
        str(row["chunk_id"]): str(row["doc_id"])
        for row in conn.execute("SELECT chunk_id, doc_id FROM corpus_chunks")
    }


def _corpus_maps(
    conn: sqlite3.Connection,
) -> tuple[dict[str, str], dict[str, frozenset[str]]]:
    """Две карты **одним** проходом: ``chunk_id → doc_id`` и ``content_hash → doc_id``.

    Вторая — чтобы назвать документ цитаты: в ``cell_evidence`` у документационного
    контура ``source_url`` пуст, и адрес восстанавливается по ``content_hash``. Один хеш
    может принадлежать нескольким документам (одинаковый текст под разными URL) — тогда
    карта честно отдаёт все, а не выбирает один.
    """
    chunk_doc: dict[str, str] = {}
    hash_docs: dict[str, set[str]] = {}
    for row in conn.execute("SELECT chunk_id, doc_id, content_hash FROM corpus_chunks"):
        doc_id = str(row["doc_id"])
        chunk_doc[str(row["chunk_id"])] = doc_id
        digest = str(row["content_hash"] or "")
        if digest:
            hash_docs.setdefault(digest, set()).add(doc_id)
    return chunk_doc, {digest: frozenset(docs) for digest, docs in hash_docs.items()}


def _citation_docs(
    conn: sqlite3.Connection,
    run_id: str,
    hash_docs: dict[str, frozenset[str]],
) -> dict[tuple[str, str], frozenset[str]]:
    """Документы, из которых взяты цитаты ячейки: ``(product, param) → doc_id``.

    Цитата, чей ``content_hash`` не нашёлся в корпусе, документа не получает: «не знаем»,
    а не «другой документ». Пустое множество — цитат нет вовсе.
    """
    out: dict[tuple[str, str], set[str]] = {}
    for row in conn.execute(
        "SELECT product_id, param_id, content_hash FROM cell_evidence WHERE run_id = ?",
        (run_id,),
    ):
        digest = str(row["content_hash"] or "")
        key = (str(row["product_id"]), str(row["param_id"]))
        docs = out.setdefault(key, set())
        docs.update(hash_docs.get(digest, ()))
    return {key: frozenset(docs) for key, docs in out.items()}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


#: Колонки по-ячеечной таблицы: класс — первичное доказательство, выдача воспроизводима.
#: ``cited_docs`` — сколько документов назвали цитаты ячейки (число, а не список: адреса
#: воспроизводятся запросом к ``cell_evidence``), ``citation_hits_golden`` — из них есть
#: документ эталона (``1``/``0``/пусто — цитат нет или адрес неизвестен).
CELLS_CSV_COLUMNS = (
    "profile",
    "cell",
    "product_id",
    "param_id",
    "status",
    "found",
    "class",
    "cited_docs",
    "citation_hits_golden",
)


def _write_csv(path: Path, reports: Sequence[dict[str, Any]]) -> None:
    """Пишет по-ячеечную таблицу: вердикт, найден ли документ, класс, адрес цитаты."""
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(CELLS_CSV_COLUMNS)
        for report in reports:
            for cell in report["per_cell"]:
                hits = cell["citation_hits_golden"]
                writer.writerow(
                    [
                        report["profile"],
                        cell["cell"],
                        cell["product_id"],
                        cell["param_id"],
                        cell["status"] or "",
                        "1" if cell["found"] else "0",
                        cell["class"],
                        len(cell["cited_doc_ids"]),
                        "" if hits is None else ("1" if hits else "0"),
                    ]
                )


if __name__ == "__main__":
    raise SystemExit(main())
