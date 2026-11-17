#!/usr/bin/env python3
"""Явный одноразовый перенос ``state.db`` пакета между версиями схемы (12 → 13).

**Почему инструмент, а не догон при открытии.** Штатное открытие
(:func:`core.store.state_sqlite.connect_state`) читает ``schema_meta.schema_version``
**до** ``STATE_DDL`` и файл другой версии отвергает
:class:`~core.store.sqlite.SchemaVersionMismatch` — миграций при открытии нет ни одной
(решение владельца 01.11.2026: тихий ``ALTER TABLE`` объявлял бы файл текущим, не будучи
им). Но ``state.db`` — durable-состояние пакета: в нём лежит то, что живым краулом
собирается часами и гигабайтами (``url_state`` — рабочий набор прохода без сети,
``document_version``/``container_state`` — байты на диске). Пересоздать файл значит
потерять эту работу, поэтому перенос делается **явно, отдельной командой и один раз**.
Инструмент ничего не меняет в поведении штатного открытия и не импортируется из него.

**Что переносится (12 → 13).** Дельта измерена, а не предположена: единственное отличие
схемы v13 от v12 — колонка ``chunk_meta.table_ref`` (адрес куска внутри таблицы, O39
фаза 2 плана ``26``). Поэтому перенос — ``ALTER TABLE chunk_meta ADD COLUMN table_ref
TEXT`` плюс ``UPDATE schema_meta SET value='13'``. Данные не конвертируются:
``table_ref = NULL`` документирован в DDL как «адрес неизвестен» — законное значение, а не
ложь; старые наборы кусков (``policy-5``/``md-heading-4``) прогон ``policy-6`` всё равно
не переиспользует.

**Но инструмент не вправе это предполагать.** Он проверяет результат, а не объявляет
его: после переноса схема рабочей копии сравнивается **структурно** с чистым v13,
созданным :func:`connect_state` во временном каталоге (``PRAGMA table_info``,
``PRAGMA foreign_key_list``, ``PRAGMA index_list``/``index_info``, нормализованный текст
триггеров, набор ``(type, name)``), плюс ``integrity_check``, ``foreign_key_check``,
число строк по таблицам и ``table_ref IS NULL`` у всех кусков. Любое расхождение —
провал проверки: копия выбрасывается, целевой файл остаётся прежним. «Применить DDL и
поверить» здесь запрещено: DDL идемпотентен (``IF NOT EXISTS``), он **не** замечает
ни чужого шима, ни недостающего индекса, ни колонки, объявленной иначе.

**Работа только на копии.** Оригинал не пишется до последнего шага: сначала бэкап
(``state.db.v12`` рядом, имя содержит исходную версию), затем рабочая копия в том же
каталоге, на ней ``ALTER``/``UPDATE``, затем проверки, и только после них
``os.replace`` копии на целевой путь (атомарно, в пределах одной ФС). Провал любой
проверки оставляет оригинал нетронутым.

**WAL.** Если рядом с файлом лежит **непустой** ``-wal``, инструмент отказывается
работать: содержимое бэкапа снимается через backup API (включая закоммиченные кадры
WAL), но подмена основного файла под живым писателем — единственный способ потерять
данные, ради которых всё и делается. Пустой ``-wal`` и ``-shm`` удаляются после подмены:
они описывают **старый** файл, а новый уже содержит всё закоммиченное.

Коды возврата
-------------

* ``0`` — успех: перенос выполнен и проверен, либо файл уже текущей версии (ничего не
  записано);
* ``2`` — **отказ до записи**: неподдерживаемая версия, файл без таблиц/без версии,
  несогласованный файл (версия 12, а ``table_ref`` уже есть), неизвестный объект в
  ``sqlite_master``, иная дельта схемы, занятый путь бэкапа без ``--force``, непустой
  ``-wal``, БД держит другой процесс;
* ``1`` — **провал проверки после переноса**: копия не совпала с чистым v13 либо не
  прошла ``integrity_check``/``foreign_key_check``/сверку числа строк. Целевой файл
  **не** подменён.

Отказ и провал проверки — разные вещи: первый случается до записи и означает «мы не
знаем, что это за файл», второй — после и означает «перенос получился не тем, чем должен
был». Оба оставляют целевой файл неизменным.

Запуск::

    python3 scripts/migrate_state.py --db ~/.cohound/stands/p/state/p/state.db
    python3 scripts/migrate_state.py --db … --apply
    python3 scripts/migrate_state.py --db … --apply --no-backup --force

Без ``--apply`` работает ``--dry-run`` (по умолчанию): печатает дельту, число строк,
мёртвый вес и наличие байтов на диске, не создавая ни копии, ни бэкапа.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.store.state_sqlite import (  # noqa: E402 — после правки sys.path
    STATE_SCHEMA_VERSION,
    connect_state,
)
from core.util.schema_gate import declared_schema_version  # noqa: E402

#: Единственная поддерживаемая исходная версия. Инструмент знает **одну** пару, а не
#: «любую старую»: у каждой пары своя дельта, и угадывать её здесь нечем.
SOURCE_VERSION = "12"

#: Целевая версия — та, что объявляет код. Если код ушёл вперёд, а инструмент нет,
#: это видимый отказ (см. :func:`main`), а не тихий перенос в промежуточную версию.
TARGET_VERSION = STATE_SCHEMA_VERSION

#: Колонка, которая и есть вся дельта 12 → 13.
ADDED_COLUMN_TABLE = "chunk_meta"
ADDED_COLUMN_NAME = "table_ref"
ADDED_COLUMN_DECL = "TEXT"

#: Коды возврата (названы в докстринге модуля).
EXIT_OK = 0
EXIT_CHECK_FAILED = 1
EXIT_REFUSED = 2

#: Сколько примеров нарушения печатать, чтобы отчёт не превращался в дамп.
_SAMPLE = 5


# --------------------------------------------------------------------------- схема


def _connect_ro(path: Path) -> sqlite3.Connection:
    """Только чтение: файл не меняется, каталог не создаётся."""
    conn = sqlite3.connect(f"file:{path.absolute().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def _quote(name: str) -> str:
    """Имя объекта в кавычках для ``PRAGMA`` (имена приходят из файла, не из кода)."""
    return '"' + name.replace('"', '""') + '"'


def _normalize_sql(sql: str | None) -> str:
    """Нормализация текста триггера: пробельные серии схлопнуты."""
    return re.sub(r"\s+", " ", sql or "").strip()


def schema_objects(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Все объекты ``sqlite_master``, кроме внутренних ``sqlite_*`` (``(type, name)``)."""
    return [
        (str(row["type"]), str(row["name"]))
        for row in conn.execute(
            "SELECT type, name FROM sqlite_master"
            " WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        )
    ]


def _table_names(conn: sqlite3.Connection) -> list[str]:
    return [
        str(row["name"])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
            " AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]


def _columns(conn: sqlite3.Connection, table: str) -> list[tuple]:
    """``PRAGMA table_info`` без ``cid``: порядок задаётся самим списком."""
    return [
        (row["name"], row["type"], row["notnull"], row["dflt_value"], row["pk"])
        for row in conn.execute(f"PRAGMA table_info({_quote(table)})")
    ]


def _foreign_keys(conn: sqlite3.Connection, table: str) -> list[tuple]:
    return [
        (
            row["table"],
            row["from"],
            row["to"],
            row["on_update"],
            row["on_delete"],
            row["match"],
        )
        for row in conn.execute(f"PRAGMA foreign_key_list({_quote(table)})")
    ]


def _indexes(conn: sqlite3.Connection) -> dict[str, dict[str, object]]:
    """Индексы всех таблиц: уникальность, происхождение, состав.

    ``origin`` (``c`` — ``CREATE INDEX``, ``u``/``pk`` — из ограничения таблицы) входит в
    подпись: два индекса с одинаковым составом, но разного происхождения — разные
    объекты схемы. Авто-индексы (``sqlite_autoindex_*``) в ``sqlite_master`` не видны,
    поэтому берутся здесь же через ``PRAGMA index_list``.
    """
    indexes: dict[str, dict[str, object]] = {}
    for table in _table_names(conn):
        for row in conn.execute(f"PRAGMA index_list({_quote(table)})"):
            name = str(row["name"])
            columns = [
                str(info["name"]) if info["name"] is not None else None
                for info in conn.execute(f"PRAGMA index_info({_quote(name)})")
            ]
            indexes[name] = {
                "table": table,
                "unique": int(row["unique"]),
                "origin": str(row["origin"]),
                "partial": int(row["partial"]),
                "columns": columns,
            }
    return indexes


def _triggers(conn: sqlite3.Connection) -> dict[str, dict[str, str]]:
    return {
        str(row["name"]): {
            "table": str(row["tbl_name"]),
            "sql": _normalize_sql(row["sql"]),
        }
        for row in conn.execute(
            "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger'"
        )
    }


def schema_signature(conn: sqlite3.Connection) -> dict[str, object]:
    """Структурная подпись схемы — единица сравнения (не текст SQL).

    Сравнивать ``sqlite_master.sql`` строкой нельзя: тот же ``CREATE TABLE`` пишется
    файлом с другим форматированием, а перенесённая таблица вообще приходит из
    ``ALTER TABLE``. Сравниваются **факты о структуре**: набор объектов, колонки
    (имя/тип/``NOT NULL``/умолчание/``pk``/порядок), внешние ключи, индексы
    (уникальность/происхождение/состав) и триггеры (нормализованный текст + таблица).
    """
    tables = {
        table: {
            "columns": _columns(conn, table),
            "foreign_keys": _foreign_keys(conn, table),
        }
        for table in _table_names(conn)
    }
    return {
        "objects": schema_objects(conn),
        "tables": tables,
        "indexes": _indexes(conn),
        "triggers": _triggers(conn),
    }


def compare_signatures(
    actual: dict[str, object],
    reference: dict[str, object],
    *,
    actual_label: str = "копия",
    reference_label: str = "эталон v13",
) -> list[str]:
    """Разница двух подписей — списком читаемых расхождений (пусто = структуры равны).

    Одна и та же функция служит двум разным вопросам: «что именно переносит
    инструмент» (``--dry-run``: ``actual`` — источник) и «получилось ли то, что должно
    было» (проверка после переноса: ``actual`` — рабочая копия). Второго сравнения нет
    намеренно: два похожих сравнения разошлись бы на первой правке.
    """
    problems: list[str] = []
    actual_objects = {tuple(item) for item in actual["objects"]}  # type: ignore[union-attr]
    reference_objects = {tuple(item) for item in reference["objects"]}  # type: ignore[union-attr]
    for kind, name in sorted(actual_objects - reference_objects):
        problems.append(
            f"{actual_label}: лишний объект {kind} {name} — в {reference_label} его нет"
        )
    for kind, name in sorted(reference_objects - actual_objects):
        problems.append(
            f"{actual_label}: нет объекта {kind} {name} — в {reference_label} он есть"
        )

    actual_tables = actual["tables"]  # type: ignore[assignment]
    reference_tables = reference["tables"]  # type: ignore[assignment]
    for table in sorted(set(actual_tables) | set(reference_tables)):
        if table not in actual_tables:
            problems.append(f"{actual_label}: нет таблицы {table}")
            continue
        if table not in reference_tables:
            problems.append(f"{actual_label}: лишняя таблица {table}")
            continue
        problems.extend(
            _compare_table(
                table, actual_tables[table], reference_tables[table], actual_label
            )
        )

    actual_indexes = actual["indexes"]  # type: ignore[assignment]
    reference_indexes = reference["indexes"]  # type: ignore[assignment]
    for name in sorted(set(actual_indexes) | set(reference_indexes)):
        if name not in actual_indexes:
            problems.append(f"{actual_label}: нет индекса {name}")
        elif name not in reference_indexes:
            problems.append(f"{actual_label}: лишний индекс {name}")
        elif actual_indexes[name] != reference_indexes[name]:
            problems.append(
                f"{actual_label}: индекс {name} объявлен иначе:"
                f" {actual_indexes[name]!r} vs {reference_indexes[name]!r}"
            )

    actual_triggers = actual["triggers"]  # type: ignore[assignment]
    reference_triggers = reference["triggers"]  # type: ignore[assignment]
    for name in sorted(set(actual_triggers) | set(reference_triggers)):
        if name not in actual_triggers:
            problems.append(f"{actual_label}: нет триггера {name}")
        elif name not in reference_triggers:
            problems.append(f"{actual_label}: лишний триггер {name}")
        elif actual_triggers[name] != reference_triggers[name]:
            problems.append(f"{actual_label}: триггер {name} объявлен иначе")
    return problems


def _compare_table(
    table: str, actual: dict[str, object], reference: dict[str, object], label: str
) -> list[str]:
    """Колонки и внешние ключи одной таблицы — с указанием **имени** колонки."""
    problems: list[str] = []
    actual_columns = list(actual["columns"])  # type: ignore[arg-type]
    reference_columns = list(reference["columns"])  # type: ignore[arg-type]
    actual_by_name = {column[0]: column for column in actual_columns}
    reference_by_name = {column[0]: column for column in reference_columns}
    for column in sorted(set(actual_by_name) - set(reference_by_name)):
        problems.append(f"{label}: таблица {table} — лишняя колонка {column}")
    for column in sorted(set(reference_by_name) - set(actual_by_name)):
        problems.append(f"{label}: таблица {table} — нет колонки {column}")
    for column in sorted(set(actual_by_name) & set(reference_by_name)):
        if actual_by_name[column] != reference_by_name[column]:
            problems.append(
                f"{label}: таблица {table} — колонка {column} объявлена иначе:"
                f" {actual_by_name[column]!r} vs {reference_by_name[column]!r}"
            )
    # Порядок сравнивается только при совпадающем составе: иначе «нет колонки» уже
    # сказано, и второе сообщение о том же только шумит.
    if set(actual_by_name) == set(reference_by_name) and [
        column[0] for column in actual_columns
    ] != [column[0] for column in reference_columns]:
        problems.append(f"{label}: таблица {table} — порядок колонок отличается")
    if actual["foreign_keys"] != reference["foreign_keys"]:
        problems.append(f"{label}: таблица {table} — внешние ключи отличаются")
    return problems


def table_row_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Число строк по каждой таблице (``schema_meta`` включена: она тоже данные)."""
    return {
        table: int(
            conn.execute(
                f"SELECT count(*) FROM {_quote(table)}"  # noqa: S608 - имя из sqlite_master
            ).fetchone()[0]
        )
        for table in _table_names(conn)
    }


def read_version(conn: sqlite3.Connection) -> tuple[bool, str | None]:
    """Версия схемы файла — тем же читателем, что и штатный гейт."""
    return declared_schema_version(conn)


# ------------------------------------------------------------------- отчёт о содержимом


def chunk_dead_weight(conn: sqlite3.Connection) -> dict[str, object]:
    """Что из ``chunk_store``/``chunk_meta`` прогон ``policy-6`` не переиспользует.

    Справка, не действие: инструмент мёртвый вес **не удаляет** (это отдельное решение
    и отдельный замер). «Мёртвый» здесь значит буквально «не будет переиспользован»:
    наборы кусков в файле нарезаны прежними версиями чанкера, а прогон ищет набор
    текущей версии, поэтому в ``state.db`` они лежат балластом — но балластом, который
    ещё может понадобиться для сверки прошлых прогонов.
    """
    groups = [
        (str(row["chunker_version"]), int(row["n"]))
        for row in conn.execute(
            "SELECT chunker_version, count(*) AS n FROM chunk_meta"
            " GROUP BY chunker_version ORDER BY n DESC"
        )
    ]
    store_rows, chars, octets = conn.execute(
        "SELECT count(*), coalesce(sum(length(text)), 0),"
        " coalesce(sum(length(cast(text AS BLOB))), 0) FROM chunk_store"
    ).fetchone()
    meta_rows = int(conn.execute("SELECT count(*) FROM chunk_meta").fetchone()[0])
    return {
        "chunk_meta_rows": meta_rows,
        "chunk_meta_by_version": groups,
        "chunk_store_rows": int(store_rows),
        # ``length(TEXT)`` считает символы, ``length(CAST(... AS BLOB))`` — байты UTF-8:
        # для русского текста это разные числа, и путать их в отчёте нельзя.
        "chunk_store_chars": int(chars),
        "chunk_store_bytes": int(octets),
    }


def raw_path_stats(conn: sqlite3.Connection, table: str) -> dict[str, object]:
    """Сколько строк ссылается на байты на диске и у скольких файла нет.

    Это то, что делает перенос осмысленным или бессмысленным: если байтов нет ни у
    одной строки, переносить нечего — дешевле пересоздать файл.
    """
    total = 0
    null = 0
    present = 0
    missing: list[str] = []
    for row in conn.execute(f"SELECT raw_path FROM {_quote(table)}"):  # noqa: S608 - имя таблицы из литерала
        total += 1
        value = row[0]
        if value is None:
            null += 1
            continue
        if Path(str(value)).is_file():
            present += 1
        elif len(missing) < _SAMPLE:
            missing.append(str(value))
    return {
        "table": table,
        "rows": total,
        "null": null,
        "present": present,
        "missing": total - null - present,
        "missing_examples": missing,
    }


# ------------------------------------------------------------------------- перенос


class MigrationError(RuntimeError):
    """Внутренняя ошибка переноса (не отказ входа и не провал проверки)."""


def apply_schema_change(work_path: Path) -> None:
    """Единственная запись переноса: ``ADD COLUMN`` + версия в ``schema_meta``.

    Выполняется на **рабочей копии**. WAL снимается (``journal_mode=DELETE``), иначе
    после ``os.replace`` основной файл остался бы без кадров, которые ещё лежали в
    ``-wal``: подменялся бы файл без переноса.
    """
    conn = sqlite3.connect(str(work_path))
    try:
        conn.execute(
            f"ALTER TABLE {_quote(ADDED_COLUMN_TABLE)}"
            f" ADD COLUMN {_quote(ADDED_COLUMN_NAME)} {ADDED_COLUMN_DECL}"
        )
        cursor = conn.execute(
            "UPDATE schema_meta SET value = ? WHERE key = 'schema_version'",
            (TARGET_VERSION,),
        )
        if cursor.rowcount != 1:
            raise MigrationError(
                "schema_meta.schema_version: ожидалась ровно одна строка,"
                f" обновлено {cursor.rowcount}"
            )
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode = DELETE")
        conn.commit()
    finally:
        conn.close()
    _drop_sidecars(work_path)


def _drop_sidecars(path: Path) -> None:
    """Убрать ``-wal``/``-shm`` рядом с файлом (после checkpoint их содержимое уже в нём)."""
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(path) + suffix)
        if sidecar.exists():
            sidecar.unlink()


def create_reference(tmp_dir: Path) -> Path:
    """Чистый v13 в временном каталоге — эталон сравнения, а не «примерно та схема»."""
    reference = tmp_dir / "reference-v13.db"
    conn = connect_state(reference)
    conn.close()
    _drop_sidecars(reference)
    return reference


def verify_migrated_copy(
    copy_path: Path, reference_path: Path, rows_before: dict[str, int]
) -> list[str]:
    """Проверки перенесённой копии. Пустой список — копия годна к подмене.

    Проверяется не «DDL применился», а **результат**: целостность файла, отсутствие
    нарушений внешних ключей, структурное равенство чистому v13 (той же функцией
    сравнения, что печатает дельту в ``--dry-run``), сохранность числа строк в каждой
    таблице и ``table_ref IS NULL`` у всех кусков. Это шов для теста провала проверки:
    подмена этой функции обязана оставить целевой файл нетронутым.
    """
    problems: list[str] = []
    conn = _connect_ro(copy_path)
    try:
        integrity = [str(row[0]) for row in conn.execute("PRAGMA integrity_check")]
        if integrity != ["ok"]:
            problems.append(f"PRAGMA integrity_check: {integrity[:_SAMPLE]!r}")

        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            problems.append(
                f"PRAGMA foreign_key_check: {len(violations)} нарушений,"
                f" первые: {[tuple(row) for row in violations[:_SAMPLE]]!r}"
            )

        rows_after = table_row_counts(conn)
        for table in sorted(set(rows_before) | set(rows_after)):
            before = rows_before.get(table)
            after = rows_after.get(table)
            if before != after:
                problems.append(
                    f"таблица {table}: строк было {before}, стало {after}"
                )

        has_tables, version = read_version(conn)
        if not has_tables or version != TARGET_VERSION:
            problems.append(
                f"schema_meta.schema_version после переноса: {version!r},"
                f" ожидалось {TARGET_VERSION!r}"
            )

        if ADDED_COLUMN_TABLE in _table_names(conn):
            filled = int(
                conn.execute(
                    f"SELECT count(*) FROM {_quote(ADDED_COLUMN_TABLE)}"  # noqa: S608 - имена из констант
                    f" WHERE {_quote(ADDED_COLUMN_NAME)} IS NOT NULL"
                ).fetchone()[0]
            )
            if filled:
                problems.append(
                    f"{ADDED_COLUMN_TABLE}.{ADDED_COLUMN_NAME}: заполнено {filled} строк —"
                    " перенос обязан оставить NULL («адрес неизвестен»)"
                )

        migrated = schema_signature(conn)
    finally:
        conn.close()

    reference = _connect_ro(reference_path)
    try:
        expected = schema_signature(reference)
    finally:
        reference.close()
    problems.extend(
        compare_signatures(migrated, expected, actual_label="копия", reference_label="v13")
    )
    return problems


def _in_use_reason(path: Path) -> str | None:
    """Держит ли файл кто-то ещё: проба эксклюзивной блокировкой, снимается сразу.

    Подмена файла под живым писателем — единственный способ потерять данные, ради
    которых делается перенос, поэтому занятость проверяется явно, а не «наверное, никто
    не работает». Проба только берёт и отпускает блокировку: данные не меняются.
    """
    try:
        conn = sqlite3.connect(str(path), timeout=0)
    except sqlite3.Error as exc:
        return f"не удалось открыть файл для проверки блокировки: {exc}"
    try:
        conn.execute("BEGIN EXCLUSIVE")
        conn.execute("ROLLBACK")
    except sqlite3.OperationalError as exc:
        if "lock" in str(exc).lower() or "busy" in str(exc).lower():
            return f"файл держит другой процесс ({exc})"
        return None
    finally:
        conn.close()
    return None


def _wal_refusal(db_path: Path) -> str | None:
    """Непустой ``-wal`` рядом с файлом — отказ (см. докстринг модуля)."""
    wal = Path(str(db_path) + "-wal")
    if wal.exists() and wal.stat().st_size > 0:
        return (
            f"рядом лежит непустой {wal.name} ({wal.stat().st_size} байт): закройте БД и"
            " дайте WAL сняться (checkpoint), затем повторите — подменять основной файл"
            " под живым писателем нельзя"
        )
    return None


def _make_backup(src: sqlite3.Connection, backup_path: Path) -> None:
    """Бэкап через backup API: снимок включает закоммиченные кадры WAL, без ``-wal``."""
    target = sqlite3.connect(str(backup_path))
    try:
        src.backup(target)
    finally:
        target.close()


def _make_work_copy(
    src: sqlite3.Connection, work_path: Path, backup_path: Path | None
) -> None:
    """Рабочая копия: из бэкапа, если он есть (тот уже согласован), иначе из источника."""
    if backup_path is not None:
        shutil.copy2(backup_path, work_path)
    else:
        _make_backup(src, work_path)


# -------------------------------------------------------------------------- печать


def _mib(octets: int) -> float:
    return octets / (1024 * 1024)


def _print_header(db_path: Path, version: str | None, size: int) -> None:
    print(f"файл:       {db_path}")
    print(f"версия:     {version!r} → {TARGET_VERSION!r}")
    print(f"размер:     {size} байт ({_mib(size):.1f} МиБ)")


def _print_counts(counts: dict[str, int]) -> None:
    print("строк по таблицам:")
    for table in sorted(counts):
        print(f"  {table:<24} {counts[table]}")


def _print_delta(delta: list[str]) -> None:
    if not delta:
        print("дельта схемы: пусто — структура уже совпадает с v13")
        return
    print(f"дельта схемы (что появится): {len(delta)}")
    for line in delta:
        print(f"  {line}")


def _print_dead_weight(dead: dict[str, object]) -> None:
    print(
        f"мёртвый вес: chunk_meta {dead['chunk_meta_rows']} строк,"
        f" chunk_store {dead['chunk_store_rows']} строк"
        f" = {_mib(int(dead['chunk_store_bytes'])):.1f} МиБ"
        f" ({dead['chunk_store_bytes']} байт UTF-8, {dead['chunk_store_chars']} символов)"
    )
    groups = dead["chunk_meta_by_version"]
    if groups:
        joined = ", ".join(f"{version}: {n}" for version, n in groups)  # type: ignore[misc]
        print(f"  по chunker_version: {joined}")
    print("  (справка: прогон policy-6 эти наборы не переиспользует; ничего не удаляется)")


def _print_raw_paths(stats: dict[str, object]) -> None:
    print(
        f"байты на диске, {stats['table']}.raw_path: строк {stats['rows']},"
        f" файл есть {stats['present']}, нет {stats['missing']}, NULL {stats['null']}"
    )
    for example in stats["missing_examples"]:  # type: ignore[union-attr]
        print(f"  нет файла: {example}")


# --------------------------------------------------------------------------- режимы


def _refuse(message: str) -> int:
    print(f"ОТКАЗ: {message}")
    return EXIT_REFUSED


def migrate(
    db_path: Path,
    *,
    apply: bool,
    backup: bool,
    force: bool,
) -> int:
    """Основной сценарий: проверки → отчёт → (перенос → проверки → подмена)."""
    if TARGET_VERSION != "13":
        return _refuse(
            f"инструмент знает только перенос {SOURCE_VERSION} → 13,"
            f" а код объявляет {TARGET_VERSION!r}: дельта не описана"
        )
    if not db_path.is_file():
        return _refuse(f"файла нет: {db_path}")

    source: sqlite3.Connection | None = None
    tmp_dir = Path(tempfile.mkdtemp(prefix="cohound-state-migrate-"))
    try:
        try:
            source = _connect_ro(db_path)
            has_tables, version = read_version(source)
        except sqlite3.DatabaseError as exc:
            # Опечатка в пути или не-SQLite файл: отказ, а не трейсбек.
            return _refuse(f"файл не читается как SQLite: {db_path} ({exc})")
        if not has_tables:
            return _refuse(f"в файле нет таблиц: {db_path} — это не state.db")
        if version is None:
            return _refuse(
                f"файл {db_path} не объявляет schema_version — версия неизвестна,"
                " сверять не с чем"
            )
        if version == TARGET_VERSION:
            print(f"файл уже текущей версии {TARGET_VERSION!r} — ничего не записано")
            return EXIT_OK
        if version != SOURCE_VERSION:
            return _refuse(
                f"поддерживается только пара {SOURCE_VERSION} → {TARGET_VERSION},"
                f" а файл объявляет {version!r}"
            )

        if ADDED_COLUMN_TABLE not in _table_names(source):
            return _refuse(
                f"версия {version!r}, но таблицы {ADDED_COLUMN_TABLE} нет —"
                " файл не согласован с известным состоянием v12"
            )
        existing_columns = {column[0] for column in _columns(source, ADDED_COLUMN_TABLE)}
        if ADDED_COLUMN_NAME in existing_columns:
            return _refuse(
                f"версия {version!r}, но {ADDED_COLUMN_TABLE}.{ADDED_COLUMN_NAME} уже есть —"
                " файл несогласован (частичный шим?): переносить нечего, а объявлять"
                " версию молча нельзя"
            )

        reference_path = create_reference(tmp_dir)
        reference = _connect_ro(reference_path)
        try:
            reference_signature = schema_signature(reference)
        finally:
            reference.close()
        source_signature = schema_signature(source)

        known = {tuple(item) for item in reference_signature["objects"]}  # type: ignore[union-attr]
        unknown = [item for item in source_signature["objects"] if tuple(item) not in known]  # type: ignore[union-attr]
        if unknown:
            listed = ", ".join(f"{kind} {name}" for kind, name in unknown[:_SAMPLE])
            more = "" if len(unknown) <= _SAMPLE else f" (+{len(unknown) - _SAMPLE})"
            return _refuse(
                f"в файле {len(unknown)} объектов, которых нет в чистом v13:"
                f" {listed}{more} — это не известное состояние v12"
            )

        delta = compare_signatures(
            source_signature,
            reference_signature,
            actual_label="источник",
            reference_label="эталон v13",
        )
        expected_delta = [f"источник: таблица {ADDED_COLUMN_TABLE} — нет колонки {ADDED_COLUMN_NAME}"]
        if delta != expected_delta:
            listed = "; ".join(delta[:_SAMPLE]) or "пусто"
            return _refuse(
                "дельта схемы не равна известной дельте v12 → v13"
                f" ({ADDED_COLUMN_TABLE}.{ADDED_COLUMN_NAME}): {listed}"
            )

        rows_before = table_row_counts(source)
        size_before = db_path.stat().st_size
        _print_header(db_path, version, size_before)
        _print_delta(delta)
        _print_counts(rows_before)
        _print_dead_weight(chunk_dead_weight(source))
        _print_raw_paths(raw_path_stats(source, "document_version"))
        _print_raw_paths(raw_path_stats(source, "container_state"))

        if not apply:
            print("режим: --dry-run (ничего не записано); перенос — с --apply")
            return EXIT_OK

        wal_refusal = _wal_refusal(db_path)
        if wal_refusal is not None:
            return _refuse(wal_refusal)
        if not os.access(db_path.parent, os.W_OK):
            # Копию и подмену делать негде — это отказ входа, а не провал проверки.
            return _refuse(
                f"каталог {db_path.parent} недоступен для записи: рабочую копию и"
                " подмену файла сделать негде"
            )
        in_use = _in_use_reason(db_path)
        if in_use is not None:
            return _refuse(in_use)

        backup_path = db_path.with_name(f"{db_path.name}.v{version}") if backup else None
        if backup_path is not None and backup_path.exists():
            if not force:
                return _refuse(
                    f"путь бэкапа занят: {backup_path} — удалите его или передайте --force"
                )
            backup_path.unlink()

        work_path = db_path.with_name(f".{db_path.name}.migrating-{os.getpid()}")
        if work_path.exists():
            work_path.unlink()
        started = time.monotonic()
        replaced = False
        try:
            if backup_path is not None:
                _make_backup(source, backup_path)
                print(f"бэкап:      {backup_path} ({backup_path.stat().st_size} байт)")
            _make_work_copy(source, work_path, backup_path)
            apply_schema_change(work_path)
            problems = verify_migrated_copy(work_path, reference_path, rows_before)
            if problems:
                print(f"ПРОВЕРКА НЕ ПРОЙДЕНА: {len(problems)} расхождений — файл не подменён")
                for line in problems:
                    print(f"  {line}")
                return EXIT_CHECK_FAILED

            # Источник закрыт **до** подмены: его ``-wal``/``-shm`` относятся к старому
            # inode и будут удалены; держать их открытыми незачем.
            source.close()
            os.replace(work_path, db_path)
            replaced = True
            _drop_sidecars(db_path)
        finally:
            if not replaced and work_path.exists():
                work_path.unlink()

        elapsed = time.monotonic() - started
        size_after = db_path.stat().st_size
        print(f"перенесено: {db_path}")
        print(f"размер:     {size_before} → {size_after} байт"
              f" ({_mib(size_before):.1f} → {_mib(size_after):.1f} МиБ)")
        print(f"длительность: {elapsed:.1f} с")
        after = _connect_ro(db_path)
        try:
            _print_counts(table_row_counts(after))
            added = [
                (kind, name)
                for kind, name in schema_objects(after)
                if (kind, name) not in {tuple(item) for item in source_signature["objects"]}  # type: ignore[union-attr]
            ]
        finally:
            after.close()
        print(f"объектов, которых не было в источнике: {len(added)}")
        for kind, name in added:
            print(f"  {kind} {name}")
        print("проверки: integrity_check ok, foreign_key_check пуст, схема = чистый v13,"
              " число строк сохранено, table_ref IS NULL у всех")
        return EXIT_OK
    finally:
        if source is not None:
            source.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", required=True, metavar="PATH", help="файл state.db пакета")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="выполнить перенос (без флага — --dry-run: ничего не пишется)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="только отчёт и дельта (режим по умолчанию; флаг для явности)",
    )
    parser.add_argument(
        "--keep-backup",
        dest="backup",
        action="store_true",
        help="оставить копию рядом: <db>.v12 (по умолчанию)",
    )
    parser.add_argument(
        "--no-backup",
        dest="backup",
        action="store_false",
        help="не оставлять бэкап (копия источника всё равно делается — работа идёт на ней)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="перезаписать существующий файл бэкапа",
    )
    parser.set_defaults(backup=True)
    args = parser.parse_args(argv)
    if args.apply and args.dry_run:
        parser.error("--apply и --dry-run несовместимы")
    return migrate(
        Path(args.db),
        apply=args.apply,
        backup=args.backup,
        force=args.force,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
