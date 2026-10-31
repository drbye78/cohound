"""coHound — общий гейт версии схемы SQLite: класс отказа и текст сообщения (11 §2.6).

Три БД coHound версионируются **одинаково** (решение владельца 2026-11-01):
``runs/{run_id}/cohound.db``, ``state/{package_id}/state.db``,
``{state_dir}/edge_jobs.db``. Файл объявляет версию в ``schema_meta(key, value)``,
код сверяет её **до** DDL, несовпадение — отказ с путём файла, и файл при отказе не
меняется. Аддитивных ``ALTER TABLE ... ADD COLUMN`` в штатном открытии нет ни одного:
тихий догон объявлял бы файл текущим, не будучи им (A3: тихая деградация — худший
исход, видимый отказ — валидный).

**Почему в ``core/util``, а не в ``core/store/sqlite.py``.** Тот же отказ поднимает и
DMZ — :func:`core.edge.scheduler.EdgeScheduler._connect` (``edge_jobs.db``). Матрица
слоёв (``01`` §1, гейт ``tests/selfcheck.py``) запрещает ``edge → store``: DMZ не
пишет в операционную БД ядра и не импортирует её (I18/I19). Класс, оставшийся в
``store``, был бы для edge недостижим, а **второй** такой же класс означал бы, что
``except SchemaVersionMismatch`` в composition root (``core/cli/main.py``) ловит
половину отказов: edge-отказ уехал бы в общий обработчик как ``EXIT_ERROR`` вместо
``EXIT_USAGE`` с кодом ``schema_version``. Один класс на все три БД, один формат
сообщения.

``sqlite3`` здесь только в аннотации: гейт ничего не открывает и не создаёт.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

__all__ = [
    "SCHEMA_VERSION_CODE",
    "SchemaVersionMismatch",
    "declared_schema_version",
    "schema_mismatch_message",
]

#: Код ошибки гейта версии схемы (``03`` §A.2, ``11`` §2.6): файл чужой схемы не читается.
SCHEMA_VERSION_CODE = "schema_version"


class SchemaVersionMismatch(RuntimeError):
    """Файл БД coHound объявляет другую версию схемы, чем ожидает код.

    Гейт версии (``11`` §2.6): прогон, durable-состояние пакета и edge-очередь —
    артефакты, но **не** переносятся между версиями схемы. Читать старую БД молча
    нельзя: аддитивные дельты сделали бы «старую» строку неотличимой от «новой», а
    артефакт объявлял бы условия, которых не применял. Поэтому несовпадение — отказ с
    внятным сообщением, а не тихий апгрейд.

    Один класс на **все три** БД: ``runs/{run_id}/cohound.db``,
    ``state/{package_id}/state.db``, ``{state_dir}/edge_jobs.db``. Формат сообщения —
    :func:`schema_mismatch_message`, обработчик в CLI — ``core/cli/main.py``
    (``CODE: message``, ``EXIT_USAGE``).

    ``code`` — код правила для ``CODE: message`` в CLI; сама строка сообщения кода не
    повторяет (иначе получилось бы ``schema_version: schema_version: …``).
    """

    #: Код ошибки (совпадает с :data:`SCHEMA_VERSION_CODE`).
    code = SCHEMA_VERSION_CODE


def schema_mismatch_message(
    db_path: str | Path,
    *,
    found: str | None,
    expected: str,
    what: str,
    action: str,
) -> str:
    """Единый текст отказа гейта версии схемы — для **любой** БД coHound.

    Один формат на все места (``store/sqlite._reject_foreign_schema``,
    ``store/state_sqlite.connect_state``, ``edge/scheduler._connect``, чтение таблицы без
    ожидаемых колонок): оператор обязан узнать из сообщения **какой файл** отвергнут,
    какую версию он объявляет и что именно сделать. Путь к файлу — обязательная часть:
    без него неясно, какую из трёх БД пересоздавать.

    :param db_path: путь отвергнутого файла (печатается как есть).
    :param found: версия, объявленная файлом; ``None`` — версия не объявлена вовсе.
    :param expected: версия, которую ожидает код.
    :param what: ссылка на правило (например ``11 §2.6``) — почему это отказ.
    :param action: что делать оператору (пересоздание, а не миграция).
    """
    declared = (
        f"объявляет schema_version={found!r}"
        if found is not None
        else "не объявляет schema_version вовсе"
    )
    return (
        f"схема БД несовместима: файл {str(db_path)!r} {declared}, "
        f"а код ожидает {expected!r} ({what}). "
        f"Молчаливое чтение чужой схемы запрещено: {action}"
    )


def declared_schema_version(conn: sqlite3.Connection) -> tuple[bool, str | None]:
    """Что уже лежит в файле: есть ли таблицы и какую версию он объявляет.

    Читается **до** DDL и до любых PRAGMA, меняющих файл (``journal_mode`` пишется в
    заголовок): гейт обязан увидеть исходное состояние, а не результат
    ``CREATE TABLE IF NOT EXISTS``. Общий читатель для всех трёх БД — версия хранится
    одним способом (``schema_meta(key, value)``), значит и читается одним.

    * ``(False, None)`` — файл пуст (новый путь): сверять нечего;
    * ``(True, None)`` — таблицы есть, а версия не объявлена: до-версионный или
      полуразрушенный файл. Это **не** «новая БД»: ``CREATE TABLE IF NOT EXISTS`` его не
      тронет, а объявление текущим приписало бы строки неизвестной схемы текущей;
    * ``(True, "<версия>")`` — версия объявлена и сверяется с ожидаемой.

    Требует ``conn.row_factory = sqlite3.Row`` (доступ по имени, как в остальном коде).
    """
    tables = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
            " AND name NOT LIKE 'sqlite_%'"
        )
    }
    if not tables:
        return False, None
    if "schema_meta" not in tables:
        return True, None
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        return True, None
    return True, str(row["value"])