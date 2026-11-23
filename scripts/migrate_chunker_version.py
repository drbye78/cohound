#!/usr/bin/env python3
"""Пере-штамповка ``chunker_version`` в durable-субстрате пакета — миграция, а не прогон.

**Зачем инструмент.** ``chunker_version`` — контрактное поле (``run_id``): смена версии
объявляет ``execution_change``, и переиспользование chunk-set'а из ``state.db`` разрешено
только при совпадении версии (:func:`core.pipeline.corpus_persist.reuse_doc_chunks`, гейт
``doc_version.chunker_version != chunker_version``). Но бамп версии не всегда означает, что
текст корпуса изменился. Измерено 17.11.2026
(``docs/reports/measurements/2026-11-17-policy6-retrieval.md``): весь корпус пере-нарезан
``policy-5`` → ``policy-6``, получено **253 306 из 253 306 идентичных чанков**, а
re-embedding изменил ранжирование на **0** из 15 golden-ячеек (McNemar p=1.0). Разница
``policy-6`` — только поле ``table_ref`` (адрес чанка внутри таблицы), которого нет ни в
индексе, ни во входе эмбеддера, ни в поиске.

Значит для такого класса эквивалентности правильный ход — привести **данные** к
объявленной версии, а не заново нарезать корпус (часы GPU и краула ради того же текста).
Инструмент делает ровно это и ничего больше: таблица эквивалентности версий в рантайме
не нужна, код остаётся честным, а факт пере-штамповки виден в аудите.

**Что меняется.** Две таблицы, где живёт версия:

* ``document_version.chunker_version`` — версия, по которой чанкер решает «переиспользовать
  или нарезать заново» (гейт переиспользования смотрит именно сюда);
* ``chunk_meta.chunker_version`` — версия самого набора кусков.

Обе меняются **одним условием** ``= --from`` и в одной транзакции: половина миграции
оставила бы версию документа и версию его кусков в разных состояниях, то есть подсунула бы
гейту переиспользования набор чужой версии.

**Идентификаторы пересчитываются, а не остаются прежними.** PK строки ``chunk_meta`` —
``chunk_id = short_hash(doc_version_id, chunker_version, chunk_no)``
(:func:`core.pipeline.incremental.chunk_row_id`), то есть id **выводится** из версии.
Оставить id прежним значило бы записать строку, противоречащую собственной формуле, — и
следующий писатель (он строит id той же формулой) создал бы для того же куска вторую
строку, а ``iter_doc_chunks`` вернул бы оба набора: молчаливое удвоение корпуса. Поэтому
``chunk_id`` пере-штамповывается той же формулой, а ``document_version.chunk_ids_digest``
(дайджест набора id) пересчитывается той же формулой, что у писателя.

**``table_ref`` не трогается, адреса не выдумываются.** Разница ``policy-5`` → ``policy-6``
и есть ``table_ref``, которого в старом наборе нет. У мигрированных чанков адрес остаётся
``NULL``, и это документированное значение: ``NULL`` = «адрес неизвестен»
(:class:`core.domain.doc_tables.TableRef`), а не «кусок не из таблицы». Инструмент не
достраивает адреса задним числом: их не из чего вывести, а выдуманный адрес хуже
отсутствующего.

**Отказы (код ``2``, до записи).**

* ``--to`` не равен текущей объявленной версии (:func:`composite_chunker_version`):
  миграция «в произвольную версию» объявила бы чанки тем, чем они не являются. Обе строки
  печатаются.
* ``--from`` не встречается ни в одной таблице: печатается фактическое распределение
  версий с количествами — «мы искали не то» должно быть видно, а не молча ничего не делать.
* файл не читается как SQLite / не объявляет ``schema_version`` / схема не текущая: набор
  кусков чужой схемы переиспользованию всё равно не подлежит, а запись в такой файл —
  тихий догон, который проект запрещает (решение владельца 01.11.2026).
* состояние смешано: у версии документа есть куски другой версии (в том числе уже
  существующий набор ``--to`` — пере-штамповка дала бы два набора на один документ), либо
  у набора кусков нет соответствующей версии документа.

**Аудит.** Единственный след пере-штамповки — одна строка JSONL в
``{каталог state.db}/chunker_version_migrations.jsonl``: ``ts``, ``from``, ``to``,
``reason``, ``rows`` (строк ``document_version``), ``chunk_rows`` (строк ``chunk_meta``),
``by_product``. Без неё провенанс пере-штампован, но неотличим от «так и было».

**Проверка — до коммита.** Все записи идут в одной транзакции; целостность файла,
отсутствие нарушений внешних ключей, исчезновение ``--from`` из обеих таблиц, сохранность
числа строк и совпадение ``chunk_id``/``chunk_ids_digest`` с формулой проверяются **внутри**
неё. Любое расхождение — ``rollback`` и код ``1``: файл остаётся прежним.

Коды возврата
-------------

* ``0`` — успех (dry-run: отчёт напечатан, ничего не записано; apply: записано и проверено);
* ``1`` — провал проверки: транзакция откачена (или аудит не удалось дописать — тогда об
  этом сказано прямо, а данные уже записаны);
* ``2`` — отказ до записи: нет обязательного аргумента, ``--to`` не равен объявленной
  версии, неизвестный ``--from``, чужой файл/схема, смешанное состояние.

Запуск::

    # отчёт (по умолчанию; ничего не пишется)
    python3 scripts/migrate_chunker_version.py --state-db PATH \\
        --from 'md-ast-1:…/policy-5+text-para-4' --to 'md-ast-1:…/policy-6+text-para-4' \\
        --reason 'policy-6: table_ref не входит в индекс/embed_input; 253306/253306 чанков идентичны'

    # запись
    python3 scripts/migrate_chunker_version.py --state-db PATH --from … --to … --reason … --apply
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.adapters.chunker_family import composite_chunker_version  # noqa: E402
from core.pipeline.incremental import _FIELD_SEP, chunk_row_id  # noqa: E402
from core.store.state_sqlite import STATE_SCHEMA_VERSION  # noqa: E402
from core.util.hashing import short_hash  # noqa: E402
from core.util.schema_gate import declared_schema_version  # noqa: E402

#: Коды возврата (названы в докстринге модуля).
EXIT_OK = 0
EXIT_CHECK_FAILED = 1
EXIT_USAGE = 2

#: Имя файла аудита рядом с ``state.db``. Одна строка JSONL на миграцию.
AUDIT_NAME = "chunker_version_migrations.jsonl"

#: Таблицы, где живёт версия чанкера, и колонка в каждой. Обе меняются одним условием:
#: гейт переиспользования читает ``document_version``, а набор кусков — ``chunk_meta``.
DV_TABLE = "document_version"
CM_TABLE = "chunk_meta"

#: Сколько примеров печатать, чтобы отчёт не превращался в дамп.
_SAMPLE = 5


# ------------------------------------------------------------------------ соединения


def _connect_ro(path: Path) -> sqlite3.Connection:
    """Только чтение: файл не меняется, каталог не создаётся."""
    conn = sqlite3.connect(f"file:{path.absolute().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def _connect_rw(path: Path) -> sqlite3.Connection:
    """Чтение-запись: транзакцией управляет вызывающий, не ``with conn``."""
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


# --------------------------------------------------------------------------- вспомогательное


def _quote(name: str) -> str:
    """Имя объекта в кавычках (имена приходят из файла, не из кода)."""
    return '"' + name.replace('"', '""') + '"'


def _utc_now_iso() -> str:
    """Текущее время UTC в ISO-8601."""
    return datetime.now(UTC).isoformat()


def chunk_ids_digest(chunk_ids: list[str]) -> str:
    """Дайджест набора ``chunk_id`` — та же формула, что у писателя корпуса.

    ``corpus_persist`` пишет ``short_hash(_FIELD_SEP.join(sorted(ids)), 24)``; разделитель
    импортируется, а не дублируется, иначе две копии формулы разошлись бы молча.
    """
    return short_hash(_FIELD_SEP.join(sorted(chunk_ids)), 24)


def version_distribution(conn: sqlite3.Connection, table: str) -> list[tuple[str, int]]:
    """Фактическое распределение версий в таблице (для отказа на неизвестном ``--from``)."""
    rows = conn.execute(
        f"SELECT chunker_version AS v, count(*) AS n FROM {_quote(table)}"  # noqa: S608 - имя из константы
        " GROUP BY chunker_version ORDER BY n DESC, v"
    ).fetchall()
    return [(str(row["v"]), int(row["n"])) for row in rows]


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({_quote(table)})")}


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row["name"])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


# ------------------------------------------------------------------------------- план


class _Refusal(RuntimeError):
    """Отказ входа: печатается как ``ОТКАЗ`` и даёт код ``2``."""


class Plan:
    """Что будет изменено: строки по продуктам и адреса наборов, которые двигаются."""

    def __init__(
        self,
        *,
        from_version: str,
        to_version: str,
        reason: str,
        dv_by_product: Counter[str],
        cm_by_product: Counter[str],
        doc_ids: list[str],
        digests: dict[str, str | None],
    ) -> None:
        self.from_version = from_version
        self.to_version = to_version
        self.reason = reason
        self.dv_by_product = dv_by_product
        self.cm_by_product = cm_by_product
        self.doc_ids = doc_ids
        #: ``chunk_ids_digest`` до миграции: ``None`` — «не записано» и таким остаётся.
        self.digests = digests

    @property
    def dv_rows(self) -> int:
        return sum(self.dv_by_product.values())

    @property
    def cm_rows(self) -> int:
        return sum(self.cm_by_product.values())


def build_plan(
    conn: sqlite3.Connection, from_version: str, to_version: str, reason: str
) -> Plan:
    """Собирает план и проверяет согласованность состояния; отказ — исключением.

    Проверки состояния — не формальность: пере-штамповка половины строк оставила бы
    версию документа и версию его кусков в разных состояниях, а уже существующий набор
    ``--to`` дал бы два набора на один документ (``iter_doc_chunks`` вернул бы оба).
    """
    dv_by_product: Counter[str] = Counter(
        {
            str(row["product_id"]): int(row["n"])
            for row in conn.execute(
                f"SELECT product_id, count(*) AS n FROM {_quote(DV_TABLE)}"  # noqa: S608
                " WHERE chunker_version = ? GROUP BY product_id",
                (from_version,),
            )
        }
    )
    cm_by_product: Counter[str] = Counter(
        {
            str(row["product_id"]): int(row["n"])
            for row in conn.execute(
                f"SELECT product_id, count(*) AS n FROM {_quote(CM_TABLE)}"  # noqa: S608
                " WHERE chunker_version = ? GROUP BY product_id",
                (from_version,),
            )
        }
    )
    rows = conn.execute(
        f"SELECT doc_version_id, chunk_ids_digest FROM {_quote(DV_TABLE)}"  # noqa: S608
        " WHERE chunker_version = ? ORDER BY doc_version_id",
        (from_version,),
    ).fetchall()
    doc_ids = [str(row["doc_version_id"]) for row in rows]
    digests = {
        str(row["doc_version_id"]): (
            None if row["chunk_ids_digest"] is None else str(row["chunk_ids_digest"])
        )
        for row in rows
    }
    plan = Plan(
        from_version=from_version,
        to_version=to_version,
        reason=reason,
        dv_by_product=dv_by_product,
        cm_by_product=cm_by_product,
        doc_ids=doc_ids,
        digests=digests,
    )
    if not doc_ids and not cm_by_product:
        raise _Refusal(
            f"версии {from_version!r} нет ни в {DV_TABLE}, ни в {CM_TABLE}:"
            " мигрировать нечего\n" + _distribution_report(conn)
        )

    # 1. Версия документа и версия его кусков разошлись в любую сторону. Один запрос, а
    #    не IN-список из идентификаторов: на большом корпусе список упёрся бы в предел
    #    числа параметров SQLite, то есть инструмент отказал бы там, где он нужнее всего.
    mismatch = conn.execute(
        f"SELECT DISTINCT cm.doc_version_id AS doc_version_id FROM {_quote(CM_TABLE)} cm"  # noqa: S608
        f" JOIN {_quote(DV_TABLE)} dv ON dv.doc_version_id = cm.doc_version_id"
        " WHERE (dv.chunker_version = ? AND cm.chunker_version <> ?)"
        "    OR (cm.chunker_version = ? AND (dv.chunker_version IS NULL"
        "        OR dv.chunker_version <> ?)) LIMIT ?",
        (from_version, from_version, from_version, from_version, _SAMPLE),
    ).fetchall()
    if mismatch:
        raise _Refusal(
            "версия документа и версия его кусков разошлись:"
            f" {[str(row['doc_version_id']) for row in mismatch]!r} — состояние смешано,"
            " пере-штамповка половины набора запрещена"
        )

    # 2. У набора кусков нет версии документа вовсе — обратное расхождение.
    orphan = conn.execute(
        f"SELECT DISTINCT cm.doc_version_id AS doc_version_id FROM {_quote(CM_TABLE)} cm"  # noqa: S608
        f" LEFT JOIN {_quote(DV_TABLE)} dv ON dv.doc_version_id = cm.doc_version_id"
        " WHERE cm.chunker_version = ? AND dv.doc_version_id IS NULL LIMIT ?",
        (from_version, _SAMPLE),
    ).fetchall()
    if orphan:
        raise _Refusal(
            f"в {CM_TABLE} есть наборы версии {from_version!r} без версии документа:"
            f" {[str(row['doc_version_id']) for row in orphan]!r} — состояние смешано"
        )
    return plan


def _distribution_report(conn: sqlite3.Connection) -> str:
    """Фактические версии обеих таблиц — печатается на отказе, чтобы «искали не то» было видно."""
    lines = ["фактическое распределение версий:"]
    for table in (DV_TABLE, CM_TABLE):
        lines.append(f"  {table}:")
        for version, count in version_distribution(conn, table):
            lines.append(f"    {count:>8}  {version!r}")
    return "\n".join(lines)


# ------------------------------------------------------------------------------ запись


def apply_plan(conn: sqlite3.Connection, plan: Plan) -> None:
    """Все записи миграции: версии обеих таблиц, ``chunk_id`` и ``chunk_ids_digest``.

    Вызывается **внутри** открытой транзакции: коммит и проверку делает :func:`migrate`,
    чтобы провал проверки откатил всё целиком.
    """
    chunk_updates: list[tuple[str, str, str]] = []  # новый id, to, старый id
    version_updates: list[tuple[str, str]] = []  # to, старый id
    dv_digest_updates: list[tuple[str, str, str]] = []  # to, digest, doc_version_id
    dv_version_updates: list[tuple[str, str]] = []  # to, doc_version_id
    for doc_id in plan.doc_ids:
        rows = conn.execute(
            f"SELECT chunk_id, chunk_no FROM {_quote(CM_TABLE)}"  # noqa: S608
            " WHERE doc_version_id = ? ORDER BY chunk_no",
            (doc_id,),
        ).fetchall()
        new_ids: list[str] = []
        for row in rows:
            old_id = str(row["chunk_id"])
            new_id = chunk_row_id(doc_id, plan.to_version, int(row["chunk_no"]))
            new_ids.append(new_id)
            if new_id == old_id:
                version_updates.append((plan.to_version, old_id))
            else:
                chunk_updates.append((new_id, plan.to_version, old_id))
        if plan.digests[doc_id] is None:
            # «Не записано» остаётся «не записано»: NULL не выдумывается (та же дисциплина,
            # что у ``page_map``/``ocr_probe``), а версия всё равно мигрирует.
            dv_version_updates.append((plan.to_version, doc_id))
        else:
            dv_digest_updates.append(
                (plan.to_version, chunk_ids_digest(new_ids), doc_id)
            )
    if chunk_updates:
        conn.executemany(
            f"UPDATE {_quote(CM_TABLE)} SET chunk_id = ?, chunker_version = ?"  # noqa: S608
            " WHERE chunk_id = ?",
            chunk_updates,
        )
    if version_updates:
        conn.executemany(
            f"UPDATE {_quote(CM_TABLE)} SET chunker_version = ?"  # noqa: S608
            " WHERE chunk_id = ?",
            version_updates,
        )
    if dv_digest_updates:
        conn.executemany(
            f"UPDATE {_quote(DV_TABLE)} SET chunker_version = ?, chunk_ids_digest = ?"  # noqa: S608
            " WHERE doc_version_id = ?",
            dv_digest_updates,
        )
    if dv_version_updates:
        conn.executemany(
            f"UPDATE {_quote(DV_TABLE)} SET chunker_version = ?"  # noqa: S608
            " WHERE doc_version_id = ?",
            dv_version_updates,
        )


def verify(conn: sqlite3.Connection, plan: Plan, rows_before: dict[str, int]) -> list[str]:
    """Проверки **результата** внутри транзакции. Пустой список — можно коммитить.

    Проверяется не «UPDATE выполнился», а состояние: ``--from`` исчез из обеих таблиц,
    число строк не изменилось, ``chunk_id``/``chunk_ids_digest`` совпадают с формулой
    писателя (``NULL``-дайджест остаётся ``NULL``), целостность файла и внешние ключи в
    порядке.
    """
    problems: list[str] = []
    for table in (DV_TABLE, CM_TABLE):
        left = int(
            conn.execute(
                f"SELECT count(*) FROM {_quote(table)}"  # noqa: S608 - имя из константы
                " WHERE chunker_version = ?",
                (plan.from_version,),
            ).fetchone()[0]
        )
        if left:
            problems.append(f"{table}: версия {plan.from_version!r} осталась у {left} строк")
        after = int(
            conn.execute(f"SELECT count(*) FROM {_quote(table)}").fetchone()[0]  # noqa: S608
        )
        if after != rows_before[table]:
            problems.append(
                f"{table}: строк было {rows_before[table]}, стало {after}"
            )

    if plan.doc_ids:
        # Проверка идёт по документам, а не IN-списком: 14 870 параметров упёрлись бы в
        # предел SQLite, и проверка отказала бы именно на большом корпусе. Чтение по
        # ``doc_version_id`` — индекс ``idx_chunk_meta_doc``.
        by_doc: dict[str, list[str]] = {}
        for doc_id in plan.doc_ids:
            rows = conn.execute(
                f"SELECT chunk_id, chunk_no, chunker_version FROM {_quote(CM_TABLE)}"  # noqa: S608
                " WHERE doc_version_id = ? ORDER BY chunk_no",
                (doc_id,),
            ).fetchall()
            ids: list[str] = []
            seen: set[int] = set()
            failure: str | None = None
            for row in rows:
                number = int(row["chunk_no"])
                expected = chunk_row_id(doc_id, plan.to_version, number)
                if str(row["chunk_id"]) != expected:
                    failure = (
                        f"{CM_TABLE}.chunk_id: {str(row['chunk_id'])!r} != {expected!r}"
                        f" (документ {doc_id}, кусок {number})"
                    )
                    break
                if str(row["chunker_version"]) != plan.to_version:
                    failure = (
                        f"{CM_TABLE}.chunker_version у документа {doc_id}:"
                        f" {str(row['chunker_version'])!r}"
                    )
                    break
                if number in seen:
                    failure = (
                        f"у документа {doc_id} два куска с номером {number}"
                        " — пере-штамповка дала бы дубль набора"
                    )
                    break
                seen.add(number)
                ids.append(str(row["chunk_id"]))
            if failure is not None:
                problems.append(failure)
                break
            by_doc[doc_id] = ids
        for doc_id in plan.doc_ids:
            if doc_id not in by_doc:
                break
            stored = conn.execute(
                f"SELECT chunk_ids_digest FROM {_quote(DV_TABLE)}"  # noqa: S608
                " WHERE doc_version_id = ?",
                (doc_id,),
            ).fetchone()[0]
            if plan.digests[doc_id] is None:
                if stored is not None:
                    problems.append(
                        f"{DV_TABLE}.chunk_ids_digest у документа {doc_id}: был NULL,"
                        f" стал {stored!r} — «не записано» обязано остаться «не записано»"
                    )
                    break
                continue
            expected_digest = chunk_ids_digest(by_doc[doc_id])
            if stored != expected_digest:
                problems.append(
                    f"{DV_TABLE}.chunk_ids_digest у документа {doc_id}:"
                    f" {stored!r} != {expected_digest!r}"
                )
                break

    integrity = [str(row[0]) for row in conn.execute("PRAGMA integrity_check")]
    if integrity != ["ok"]:
        problems.append(f"PRAGMA integrity_check: {integrity[:_SAMPLE]!r}")
    violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        problems.append(
            f"PRAGMA foreign_key_check: {len(violations)} нарушений,"
            f" первые: {[tuple(row) for row in violations[:_SAMPLE]]!r}"
        )
    return problems


def write_audit(state_db: Path, plan: Plan) -> Path:
    """Дописывает **одну** строку аудита рядом с ``state.db`` и возвращает путь."""
    path = state_db.parent / AUDIT_NAME
    record: dict[str, Any] = {
        "ts": _utc_now_iso(),
        "from": plan.from_version,
        "to": plan.to_version,
        "reason": plan.reason,
        "rows": plan.dv_rows,
        "chunk_rows": plan.cm_rows,
        "by_product": dict(sorted(plan.dv_by_product.items())),
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return path


# ------------------------------------------------------------------------------- печать


def _print_plan(plan: Plan, *, schema_version: str, reason: str) -> None:
    print(f"схема:       {schema_version!r}")
    print(f"объявленная chunker_version: {composite_chunker_version()!r}")
    print(f"версия:      {plan.from_version!r}")
    print(f"         →   {plan.to_version!r}")
    print(f"причина:     {reason}")
    print(f"{DV_TABLE}: {plan.dv_rows} строк (верность документа — гейт переиспользования)")
    for product, count in plan.dv_by_product.most_common():
        print(f"  {product:<24} {count}")
    print(f"{CM_TABLE}: {plan.cm_rows} строк (сам набор кусков)")
    for product, count in plan.cm_by_product.most_common():
        print(f"  {product:<24} {count}")
    print(
        f"пересчитывается: {CM_TABLE}.chunk_id = chunk_row_id(doc, to, no),"
        f" {DV_TABLE}.chunk_ids_digest — та же формула, что у писателя корпуса"
    )
    missing_digests = sum(1 for value in plan.digests.values() if value is None)
    if missing_digests:
        print(
            f"{DV_TABLE}.chunk_ids_digest не записан у {missing_digests} документов —"
            " остаётся NULL («не записано»), значение не выдумывается"
        )
    print(
        f"{CM_TABLE}.table_ref не трогается: у мигрированных чанков адрес остаётся NULL"
        " («адрес неизвестен», TableRef), адреса не выдумываются"
    )


# ------------------------------------------------------------------------------- сценарий


def migrate(
    state_db: Path,
    *,
    from_version: str,
    to_version: str,
    reason: str,
    apply: bool,
) -> int:
    """Основной сценарий: проверки → отчёт → (запись → проверки → коммит → аудит)."""
    try:
        return _migrate(
            state_db,
            from_version=from_version,
            to_version=to_version,
            reason=reason,
            apply=apply,
        )
    except _Refusal as refusal:
        print(f"ОТКАЗ: {refusal}")
        return EXIT_USAGE


def _migrate(
    state_db: Path,
    *,
    from_version: str,
    to_version: str,
    reason: str,
    apply: bool,
) -> int:
    declared = composite_chunker_version()
    if to_version != declared:
        raise _Refusal(
            f"--to {to_version!r} не равен объявленной версии {declared!r}:"
            " миграция «в произвольную версию» объявила бы чанки тем, чем они не являются"
        )
    if from_version == to_version:
        raise _Refusal(
            f"--from и --to совпадают ({from_version!r}) — мигрировать нечего"
        )
    if not state_db.is_file():
        raise _Refusal(f"файла нет: {state_db}")

    try:
        conn = _connect_ro(state_db)
    except sqlite3.DatabaseError as exc:
        raise _Refusal(f"файл не читается как SQLite: {state_db} ({exc})") from exc
    try:
        tables = _table_names(conn)
        if not tables:
            raise _Refusal(f"в файле нет таблиц: {state_db} — это не state.db")
        for table in (DV_TABLE, CM_TABLE):
            if table not in tables:
                raise _Refusal(
                    f"в файле нет таблицы {table}: {state_db} — это не state.db"
                )
            if "chunker_version" not in _columns(conn, table):
                raise _Refusal(
                    f"в таблице {table} нет колонки chunker_version —"
                    " файл не той схемы, мигрировать нечего"
                )
        has_version, schema_version = declared_schema_version(conn)
        if not has_version or schema_version is None:
            raise _Refusal(
                f"файл {state_db} не объявляет schema_version — версия неизвестна"
            )
        if schema_version != STATE_SCHEMA_VERSION:
            raise _Refusal(
                f"схема файла {schema_version!r}, а код читает {STATE_SCHEMA_VERSION!r}:"
                " набор кусков чужой схемы переиспользованию не подлежит, запись в такой"
                " файл — тихий догон схемы"
            )
        plan = build_plan(conn, from_version, to_version, reason)
        rows_before = {
            DV_TABLE: int(conn.execute(f"SELECT count(*) FROM {_quote(DV_TABLE)}").fetchone()[0]),  # noqa: S608
            CM_TABLE: int(conn.execute(f"SELECT count(*) FROM {_quote(CM_TABLE)}").fetchone()[0]),  # noqa: S608
        }
    finally:
        conn.close()

    print(f"файл:        {state_db}")
    _print_plan(plan, schema_version=str(schema_version), reason=reason)

    if not apply:
        print("режим: --dry-run (ничего не записано); запись — с --apply")
        return EXIT_OK

    conn = _connect_rw(state_db)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            apply_plan(conn, plan)
            problems = verify(conn, plan, rows_before)
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        if problems:
            conn.execute("ROLLBACK")
            print(f"ПРОВЕРКА НЕ ПРОЙДЕНА: {len(problems)} расхождений — транзакция откачена")
            for line in problems:
                print(f"  {line}")
            return EXIT_CHECK_FAILED
        conn.execute("COMMIT")
    finally:
        conn.close()

    try:
        audit_path = write_audit(state_db, plan)
    except OSError as exc:
        print(
            f"ВНИМАНИЕ: данные записаны и проверены, но строку аудита дописать не удалось:"
            f" {exc} — пере-штамповка применена, а следа о ней нет"
        )
        return EXIT_CHECK_FAILED

    print(
        f"пере-штамповано: {DV_TABLE} {plan.dv_rows} строк,"
        f" {CM_TABLE} {plan.cm_rows} строк"
    )
    for product, count in plan.dv_by_product.most_common():
        print(
            f"  {product:<24} {count} документов,"
            f" {plan.cm_by_product.get(product, 0)} кусков"
        )
    print(f"аудит:       {audit_path}")
    print(
        "проверки: --from не осталось ни в одной таблице, число строк сохранено,"
        " chunk_id/chunk_ids_digest = формула писателя, integrity_check ok,"
        " foreign_key_check пуст"
    )
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--state-db", type=Path, metavar="PATH", help="файл state.db пакета")
    parser.add_argument(
        "--from", dest="from_version", metavar="VERSION", help="мигрируемая версия чанкера"
    )
    parser.add_argument(
        "--to", dest="to_version", metavar="VERSION", help="целевая версия (обязана совпасть с объявленной)"
    )
    parser.add_argument(
        "--reason", metavar="TEXT", help="причина миграции (уходит в строку аудита)"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="записать изменения (без флага — отчёт, ничего не пишется)",
    )
    args = parser.parse_args(argv)

    missing = [
        flag
        for flag, value in (
            ("--state-db", args.state_db),
            ("--from", args.from_version),
            ("--to", args.to_version),
            ("--reason", args.reason),
        )
        if value is None or (isinstance(value, str) and not value.strip())
    ]
    if missing:
        print(f"ОТКАЗ: обязательны {', '.join(missing)} (см. --help)")
        return EXIT_USAGE

    return migrate(
        args.state_db,
        from_version=str(args.from_version).strip(),
        to_version=str(args.to_version).strip(),
        reason=str(args.reason).strip(),
        apply=args.apply,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
