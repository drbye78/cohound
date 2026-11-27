#!/usr/bin/env python3
"""Удаление артефактов на диске, на которые не ссылается ни одна запись ``state.db``.

**Зачем инструмент.** Два каталога растут монотонно и не имеют владельца-сборщика:

* ``corpus_raw/{product_id}/{hash}.bin`` — retention сырых байтов (I19). Файл живёт
  ровно до тех пор, пока на него ссылается ``document_version.raw_path`` **или**
  ``container_state.raw_path``: контейнер — не документ, версии документа у него нет по
  построению (:class:`core.store.state_sqlite` §15), и его байты учтены только второй
  таблицей. Ссылку снимает retention/вытеснение версии, файл при этом остаётся;
* ``cache/http/{key}.bin`` (+ сайдкар ``{key}.meta.json``) — глобальный
  content-addressed кэш внешних вызовов (R7, ADR 0016). Ключ — ``http_key(url)``; записи
  вне объявленного пространства продукта в корпус попасть не могут (§6), но их байты
  остаются в кэше навсегда.

Инструмент **не** трогает схему и **не** пишет в БД: он читает ``state.db`` в режиме
``mode=ro`` и удаляет файлы. Схема не меняется, прогон не нужен, ``run_id`` не меняется
(это подложка, а не условия измерения).

Главное правило: живое множество — объединение по **всем** переданным БД
----------------------------------------------------------------------

Один каталог ``corpus_raw`` делят несколько ``state.db``: у пакета есть канонная БД под
``{home}/state/{package}/state.db`` и отдельные БД стендов
(``{home}/stands/{package}/state/{package}/state.db``), а байты у них общие. Замер
27.11.2026 на ``ru-pg-dbms``: канонная БД — 8 738 версий и 8 738 живых путей (таблицы
``container_state`` в ней нет), стенд — 43 825 версий и 28 266 живых путей, пересечение
живых путей — **3 283**. Прогон по одной стендовой БД давал 74 879 кандидатов, из которых
**5 455 файлов / 1.09 GB нужны канонной БД**: ``--apply`` по одной БД уничтожил бы чужой
корпус. Поэтому ``--state-db`` повторяемый, ``--state-root`` добавляет все найденные под
каталогом БД, а живое множество и набор продуктов считаются **объединением**:

    живое = ⋃(document_version.raw_path ∪ container_state.raw_path) по всем БД

Одна БД на общий корень — это удаление чужого, а не экономия. Отчёт печатает список
участвовавших БД и число живых путей в каждой: оператор обязан видеть, что он объединил.
Если рядом с переданной БД (до 4 уровней вверх) лежит ``state.db``, не включённая в набор,
и в ней есть версии продукта пакета — печатается предупреждение (наблюдение, **не**
отказ: иначе инструмент перестал бы работать на любой раскладке с чужими БД).

Правило и его цена
------------------

Живость байтов определяется **только** ``state.db``: файл, на который не ссылается
``document_version.raw_path`` или ``container_state.raw_path``, удаляется — по решению
владельца 27.11.2026 это пересмотр прежнего «``corpus_raw`` хранится бессрочно» (I19).
Причина: ``state.db`` держит **активную** версию документа, а прогонные БД
(``runs/{run_id}/cohound.db``) — провенанс, а не живость; хранить байты ради прогонов,
которые уже не будут переиспользованы, значит платить диском за архив.

Но удаление имеет **цену**, и инструмент обязан её назвать, а не заплатить молча.
Прогонные БД ссылаются на байты тремя путями: ``corpus_docs.raw_path`` (опубликованное
evidence), ``extract_queue.raw_path`` (очередь конвертации) и
``crawl_session_items.raw_path`` — последнее и есть **переигровка байтов без сети**
(:func:`core.pipeline.corpus_ingest.retrieve_retained`, ``resume``/``frozen``): после
удаления такой прогон пойдёт в сеть. Замер 27.11.2026 на ``ru-pg-dbms`` — из 69 424
кандидатов ``corpus_raw_unreferenced`` **69 104 (99.5 %)** упомянуты прогонными БД:

======================  =======  ======
класс ссылки            файлов   GB
======================  =======  ======
нет ссылок вообще       320      0.07
только мёртвые прогоны  14 499   1.70
только прерванный       40 725   7.85
дренаж ``e11e04cbb3d9``
нужны живым прогонам    13 880   2.63
======================  =======  ======

Поэтому кандидат, на который ссылается **хотя бы одна** прогонная БД, выделяется в
отдельный класс ``corpus_raw_run_history`` (удаляется так же — это цена, а не запрет), а
его ссылки публикуются разбивкой ``by_live_runs``/``by_dead_runs``/
``by_resumable_session``. Без ``--runs-dir``/``--runs-root`` цена **неизвестна**: класс не
вычисляется, все кандидаты остаются в ``corpus_raw_unreferenced``, в stderr печатается
предупреждение. Это наблюдение, а не отказ: инструмент обязан работать и без прогонных БД.

Общее пространство — не только у ``corpus_raw``
----------------------------------------------

``cache/http`` тоже глобальный каталог раскладки: его делят **все** пакеты, а ключ кэша
описывает адрес, а не пакет. Поэтому решение «вне объявленного пространства» принимается
против пространства **всех** пакетов набора: ``--package`` (основной, всегда входит) и
``--also-package`` (повторяемый). Ключ удаляется, только если ни один атрибутированный ему
адрес не входит в пространство ни одного пакета набора; отчёт печатает
``rescued_by_other_packages`` — сколько файлов осталось бы удалено при одном ``--package``.
Замер 27.11.2026 (``ru-pg-dbms`` + ``demo-absence``): спасено **0 файлов / 0 байт** —
сегодняшние раскладки не пересекаются, и это опубликовано числом, а не выдано за
исправленный дефект.

Классы
------

Удаляемые (по умолчанию все, ``--only`` ограничивает):

``corpus_raw_unreferenced``
    ``{corpus_raw}/{product}/{hash}.bin``, где ``product`` — продукт **этого пакета**, а
    канонический путь файла не входит в живое множество
    ``document_version.raw_path ∪ container_state.raw_path`` **и** не упомянут ни одной
    прогонной БД (истинная сирота).
``corpus_raw_run_history``
    Тот же кандидат, но на него ссылается хотя бы одна прогонная БД. Удаляется наравне:
    разница только в том, что цена названа. Разбивка — :attr:`RunPrice`.
``http_cache_outside_space``
    ``{cache}/http/{key}.bin`` вместе с парным ``{key}.meta.json``, где ``key``
    атрибутирован строке ``url_state`` продукта пакета и **все** атрибутированные адреса
    вне объявленного пространства продукта.

Только для отчёта (не удаляются никогда):

``corpus_raw_foreign_product``
    Файл ``.bin`` в подкаталоге ``corpus_raw``, чей каталог не является продуктом пакета
    (а также файл прямо в корне ``corpus_raw``). Чужой продукт живёт в том же каталоге и
    той же ``state.db`` его может не быть вовсе — удалять по неполному знанию нельзя.
``http_cache_unattributed``
    Ключ кэша, для которого в ``url_state`` нет строки **вообще**. Кэш глобальный и общий
    между пакетами: отсутствие строки означает «атрибуция неизвестна», а не «мусор».

Предикат пространства **не** переписан здесь: он импортируется из прибора
:mod:`scripts.measure_substrate_reuse` (``declared_scopes``/``_in_space``), а frontiers
строятся ровно так же, как в его ``census()``. Второе определение пространства
разошлось бы с краулом на первой же правке — и прибор стал бы лживым.

Отказы (код ``2``, ничего не пишется)
-------------------------------------

* не передан ни ``--state-db``, ни ``--state-root``;
* ``state.db`` не открывается / в нём нет ``document_version`` или ``url_state``.
  ``container_state`` **необязательна**: её отсутствие значит «контейнеров в этой БД не
  было», а не «решение неполно» — канонная БД без неё реальна (замер 27.11.2026), и отказ
  на ней заблокировал бы объединение;
* каталог ``corpus_raw/{product}`` содержит ``.bin``, а ``product_id`` не встречается ни
  в ``document_version``, ни в ``url_state``, ни в ``container_state`` ни одной из
  переданных БД: «ссылок нет» и «смотрели не туда» неотличимы, а цена ошибки — весь продукт;
* ``document_version`` пуста **во всех** переданных БД, а ``corpus_raw`` непуст:
  опорожнённые БД читались бы как «удалить всё»;
* набор продуктов пакета пуст (ни ``sources/corpus_urls.yaml``, ни ``url_state``, ни
  ``document_version`` не дают ни одного ``product_id``);
* ``--only`` с неизвестным классом;
* ``--apply`` без ``--actor``;
* ``--runs-dir``/``--runs-root`` указывают на несуществующий каталог (это опечатка, а не
  «прогонов нет»: отсутствие прогонных БД выражается их отсутствием, а не кривым путём);
* ``--also-package`` указывает на каталог без ``sources/corpus_urls.yaml``: тихое «пакета
  нет» расширило бы удаление вместо того, чтобы его ограничить.

Прогонные БД **не** дают отказа ни в каком случае: нечитаемая или старая БД считается
мёртвой (``live=False``, 0 сеансов) — «не прочитали» не доказывает ни живости, ни её
отсутствия (A3).

Аудит
-----

При ``--apply`` — JSONL в ``--audit`` (по умолчанию ``{каталог первой state.db}/prune_orphans.jsonl``,
дописыванием): первая строка — сводка
``{at, actor, package, state_dbs, classes, run_census, total}``, где ``run_census`` =
``{runs_dir, runs: [{path, run_id, live, resumable_sessions}], price: {by_live_runs,
by_dead_runs, by_resumable_session}}``; далее по строке на каждый удалённый файл
``{class, path, bytes, runs}``, где ``runs`` — отсортированные ``run_id``, ссылавшиеся на
файл (пустой список = истинная сирота). Путь аудита печатается.

Коды возврата
-------------

* ``0`` — успех (dry-run: отчёт напечатан, ничего не записано);
* ``1`` — часть файлов удалить не удалось (отчёт печатается, ошибки называются);
* ``2`` — отказ до удаления (см. выше).

Запуск::

    # отчёт (по умолчанию; ничего не удаляется) — по всем БД, делящим корень
    python3 scripts/prune_orphans.py --package packages/ru-pg-dbms \\
        --state-db ~/.cohound/stands/ru-pg-dbms/state/ru-pg-dbms/state.db \\
        --state-db ~/.cohound/data/state/ru-pg-dbms/state.db --json

    # то же, но БД находятся, а не перечисляются
    python3 scripts/prune_orphans.py --package packages/ru-pg-dbms \\
        --state-root ~/.cohound --json

    # удаление с аудитом и ценой
    python3 scripts/prune_orphans.py --package packages/ru-pg-dbms \\
        --state-root ~/.cohound/state \\
        --runs-root ~/.cohound/stands/ru-pg-dbms/runs --apply --actor roger
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.pipeline.crawl_frontier import CrawlLimits, Frontier  # noqa: E402
from core.util.cache import http_key  # noqa: E402
from scripts.measure_substrate_reuse import (  # noqa: E402
    _in_space,
    declared_scopes,
)

__all__ = [
    "AUDIT_NAME",
    "CLASS_ORDER",
    "ONLY_GROUPS",
    "Orphan",
    "PruneRefused",
    "main",
    "scan",
]

#: Имя файла аудита рядом с ``state.db`` (дописывается, одна строка — одно событие).
AUDIT_NAME = "prune_orphans.jsonl"

#: Удаляемые классы. Порядок — от «цена ошибки больше» к «меньше»; печать идёт в том же
#: порядке, чтобы оператор видел цену раньше, чем итог.
DELETABLE_CLASSES: tuple[str, ...] = (
    "corpus_raw_unreferenced",
    "corpus_raw_run_history",
    "http_cache_outside_space",
)

#: Классы, которые только считаются и печатаются.
REPORT_ONLY_CLASSES: tuple[str, ...] = (
    "corpus_raw_foreign_product",
    "http_cache_unattributed",
)

#: Порядок печати.
CLASS_ORDER: tuple[str, ...] = DELETABLE_CLASSES + REPORT_ONLY_CLASSES

#: Группы для ``--only``: имя группы → классы, которые она включает.
ONLY_GROUPS: dict[str, tuple[str, ...]] = {
    "corpus-raw": (
        "corpus_raw_unreferenced",
        "corpus_raw_run_history",
        "corpus_raw_foreign_product",
    ),
    "http-cache": ("http_cache_outside_space", "http_cache_unattributed"),
}

#: Сколько примеров печатать в человекочитаемом отчёте, чтобы он не стал дампом.
_SAMPLES = 3

#: Таблицы, без которых решение об удалении неполно. ``container_state`` здесь **нет**
#: намеренно: её отсутствие значит «контейнеров в этой БД не было», а не «решение
#: неполно» — канонная БД без неё реальна (замер 27.11.2026), и отказ на ней заблокировал
#: бы объединение. ``document_version``/``url_state`` без таблицы — это не та БД.
_REQUIRED_TABLES: tuple[str, ...] = (
    "document_version",
    "url_state",
)

#: Глубина поиска «соседних» БД вверх от каталога переданной (наблюдение, не отказ).
_NEIGHBOUR_LEVELS = 4

#: Имя файла подложки.
STATE_DB_NAME = "state.db"


class PruneRefused(Exception):
    """Отказ до удаления: условие, при котором решение об удалении было бы ложным."""


@dataclass(frozen=True)
class Orphan:
    """Один файл в одном классе: путь, размер и (для байтов) ссылки прогонных БД.

    ``runs`` — отсортированные ``run_id`` прогонов, чьи БД ссылаются на файл; пустой
    список значит «истинная сирота». ``live``/``resumable`` — флаги цены: ссылка есть у
    живого прогона и ссылка есть у возобновляемого сеанса (переигровка без сети).
    """

    cls: str
    path: Path
    size: int
    runs: tuple[str, ...] = ()
    live: bool = False
    resumable: bool = False


@dataclass(frozen=True)
class RunDb:
    """Одна прогонная БД и её роль в цене удаления."""

    path: Path
    run_id: str
    live: bool
    resumable_sessions: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "run_id": self.run_id,
            "live": self.live,
            "resumable_sessions": self.resumable_sessions,
        }


@dataclass(frozen=True)
class RunPrice:
    """Цена удаления: сколько байтов прогоны потеряют и чем именно.

    ``by_resumable_session`` — **флаг, а не третья категория**: он пересекается и с
    ``by_live_runs``, и с ``by_dead_runs`` (сеанс может принадлежать и живому прогону).
    ``by_drain`` — тоже флаг и подмножество ``by_dead_runs ∩ by_resumable_session``:
    единственная ссылка на файл — возобновляемый сеанс мёртвого прогона. Именно эти байты
    и есть незавершённая докачка: удалив их, дренаж начнётся заново (часы краула).
    Складывать любые два числа здесь нельзя — это не разбиение.
    """

    by_live_runs: dict[str, int]
    by_dead_runs: dict[str, int]
    by_resumable_session: dict[str, int]
    by_drain: dict[str, int]

    def as_dict(self) -> dict[str, dict[str, int]]:
        return {
            "by_live_runs": dict(self.by_live_runs),
            "by_dead_runs": dict(self.by_dead_runs),
            "by_resumable_session": dict(self.by_resumable_session),
            "by_drain": dict(self.by_drain),
        }


@dataclass
class RunCensus:
    """Прочитанные прогонные БД: список, карта «файл → прогоны» и флаги."""

    runs_dirs: list[str]
    dbs: list[RunDb]
    #: Канонический путь байтов → ``run_id`` прогонов, чьи БД на него ссылаются.
    paths: dict[Path, set[str]]
    live_runs: set[str]
    resumable_paths: set[Path]
    #: Цена удаления; заполняется :func:`_run_price` после обхода ``corpus_raw``.
    price: RunPrice | None = None

    @property
    def known(self) -> bool:
        """Прогонные БД вообще передавались (иначе цена неизвестна)."""
        return bool(self.runs_dirs)

    def as_dict(self) -> dict[str, Any]:
        return {
            "runs_dir": list(self.runs_dirs),
            "runs": [db.as_dict() for db in self.dbs],
            "price": (self.price or RunPrice({}, {}, {}, {})).as_dict(),
        }


@dataclass(frozen=True)
class StateDb:
    """Одна переданная БД и то, что из неё прочитано.

    ``live_paths`` хранится числом: оператору нужен счёт живого **по каждой** БД — сколько
    она добавила в объединение, — а не одно общее число.
    """

    path: Path
    live_paths: int
    products: frozenset[str]
    version_rows: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "live_paths": self.live_paths,
            "version_rows": self.version_rows,
            "products": sorted(self.products),
        }


@dataclass
class StateSet:
    """Объединение по всем переданным БД: живое множество, продукты, счётчики."""

    dbs: list[StateDb]
    live: set[Path]
    products: set[str]
    connections: list[sqlite3.Connection]

    @property
    def version_rows(self) -> int:
        return sum(db.version_rows for db in self.dbs)

    def close(self) -> None:
        for conn in self.connections:
            conn.close()
        self.connections.clear()

    def as_dict(self) -> dict[str, Any]:
        return {
            "state_dbs": [db.as_dict() for db in self.dbs],
            "live_paths": len(self.live),
            "version_rows": self.version_rows,
        }


# ------------------------------------------------------------------ вспомогательное


def _canonical(path: Path) -> Path:
    """Канонический путь для сверки «файл ↔ запись БД».

    ``raw_path`` в ``state.db`` абсолютен (писатель кладёт
    ``raw_root/{product_id}/{hash}.bin``), но сравнение строк хрупко: раскладка может
    быть симлинком, а прогон — идти из другого корня. Поэтому обе стороны приводятся к
    одному виду. ``strict=False`` — путь файла может уже не существовать.
    """
    try:
        return path.resolve()
    except OSError:  # noqa: BLE001 — кривой путь не должен ронять замер
        return Path(os.path.abspath(path))


def _human_bytes(size: int) -> str:
    """Размер в человекочитаемом виде (ГБ/МБ/КБ)."""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{value:.1f} TB"


def _utc_now_iso() -> str:
    """Текущее время UTC в ISO-8601."""
    return datetime.now(UTC).isoformat()


def _read_yaml(path: Path) -> Any:
    if not path.exists():
        return None
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _table_names(conn: sqlite3.Connection) -> set[str]:
    """Имена таблиц файла — по ним решается, что читать."""
    return {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _open_state(state_db: Path) -> tuple[sqlite3.Connection, set[str]]:
    """Открыть ``state.db`` только на чтение и проверить обязательные таблицы.

    ``container_state`` обязательной **не** считается: канонная БД пакета её не имеет
    (замер 27.11.2026), и отказ на ней заблокировал бы объединение живых множеств.
    """
    if not state_db.is_file():
        raise PruneRefused(f"state.db не найден: {state_db}")
    try:
        conn = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
        tables = _table_names(conn)
    except sqlite3.Error as exc:
        raise PruneRefused(
            f"state.db не читается как SQLite: {state_db}: {exc}"
        ) from exc
    missing = [name for name in _REQUIRED_TABLES if name not in tables]
    if missing:
        conn.close()
        raise PruneRefused(
            f"в {state_db} нет таблиц {', '.join(missing)} — это не подложка пакета"
        )
    return conn, tables


def _declared_products(package: Path) -> set[str]:
    """Продукты, объявленные пакетом (ключи ``sources/corpus_urls.yaml``)."""
    declared = _read_yaml(package / "sources" / "corpus_urls.yaml") or {}
    if not isinstance(declared, dict):
        return set()
    return {str(key) for key in declared} - {""}


def _live_raw_paths(conn: sqlite3.Connection, tables: set[str]) -> set[Path]:
    """Живое множество байтов одной БД: ``document_version`` **и** ``container_state``.

    Вторая таблица — не украшение: контейнер не документ, ``document_version`` у него нет
    по построению, и без неё все архивы выглядели бы сиротами. Читается, только если
    объявлена (канонная БД её не имеет — контейнеров в ней не было).
    """
    live: set[Path] = set()
    sqls = ["SELECT raw_path FROM document_version WHERE raw_path IS NOT NULL"]
    if "container_state" in tables:
        sqls.append("SELECT raw_path FROM container_state WHERE raw_path IS NOT NULL")
    for sql in sqls:
        for (raw,) in conn.execute(sql):
            text = str(raw).strip()
            if text:
                live.add(_canonical(Path(text)))
    return live


def _db_products(conn: sqlite3.Connection, tables: set[str]) -> set[str]:
    """Продукты, встречающиеся в строках одной БД."""
    products: set[str] = set()
    sqls = [
        "SELECT DISTINCT product_id FROM url_state",
        "SELECT DISTINCT product_id FROM document_version",
    ]
    if "container_state" in tables:
        sqls.append("SELECT DISTINCT product_id FROM container_state")
    for sql in sqls:
        products.update(str(row[0]) for row in conn.execute(sql))
    products.discard("")
    return products


def _load_state(state_dbs: list[Path]) -> StateSet:
    """Объединить все переданные БД: живое множество и продукты — по всем сразу.

    Одна БД на общий корень — это удаление чужого (замер 27.11.2026: 5 455 файлов /
    1.09 GB, нужных канонной БД, попадали в кандидаты прогона по стендовой), поэтому
    объединение, а не выбор «главной».
    """
    dbs: list[StateDb] = []
    live: set[Path] = set()
    products: set[str] = set()
    connections: list[sqlite3.Connection] = []
    try:
        for path in state_dbs:
            conn, tables = _open_state(path)
            connections.append(conn)
            own_live = _live_raw_paths(conn, tables)
            own_products = _db_products(conn, tables)
            rows = int(
                conn.execute("SELECT count(*) FROM document_version").fetchone()[0]
            )
            dbs.append(
                StateDb(
                    path=path,
                    live_paths=len(own_live),
                    products=frozenset(own_products),
                    version_rows=rows,
                )
            )
            live |= own_live
            products |= own_products
    except BaseException:
        for conn in connections:
            conn.close()
        raise
    return StateSet(dbs=dbs, live=live, products=products, connections=connections)


def _find_state_dbs(root: Path) -> list[Path]:
    """Все ``state.db`` под каталогом (рекурсивно), в детерминированном порядке."""
    if not root.is_dir():
        raise PruneRefused(f"--state-root не каталог: {root}")
    return sorted({_canonical(path) for path in root.rglob(STATE_DB_NAME)})


def _neighbour_state_dbs(state_dbs: list[Path]) -> list[Path]:
    """``state.db`` до 4 уровней вверх от каталогов переданных, не входящие в набор.

    Это **наблюдение**, а не основание отказа: соседняя БД может принадлежать другому
    пакету, и блокировать удаление из-за неё значило бы сделать инструмент неработающим
    на любой раскладке с чужими БД.
    """
    known = {_canonical(path) for path in state_dbs}
    seen: set[Path] = set()
    for path in state_dbs:
        directory = path.parent
        for _ in range(_NEIGHBOUR_LEVELS):
            if directory.parent == directory:
                break
            directory = directory.parent
            candidate = directory / STATE_DB_NAME
            if candidate.is_file():
                canonical = _canonical(candidate)
                if canonical not in known:
                    seen.add(canonical)
    return sorted(seen)


def _uncovered_products(neighbour: Path, products: set[str]) -> list[str] | None:
    """Продукты пакета, у которых есть версии в соседней БД.

    ``None`` — соседняя БД не читается как подложка (наблюдение тогда не о чем: она не
    доказывает ни полноты, ни её отсутствия).
    """
    try:
        conn, _tables = _open_state(neighbour)
    except PruneRefused:
        return None
    try:
        hit = {str(row[0]) for row in conn.execute(
            "SELECT DISTINCT product_id FROM document_version"
        )}
        return sorted(hit & products)
    except sqlite3.Error:
        return None
    finally:
        conn.close()


# ------------------------------------------------------------------ прогонные БД

#: Имя файла прогонной БД (``runs/{run_id}/cohound.db``).
RUN_DB_NAME = "cohound.db"

#: Таблицы прогонной БД, чьи ``raw_path`` ссылаются на байты ``corpus_raw``.
#: ``crawl_session_items`` — переигровка без сети (``resume``/``frozen``), поэтому её
#: ссылки отдельно помечаются как возобновляемые.
RUN_RAW_TABLES: tuple[str, ...] = (
    "corpus_docs",
    "extract_queue",
    "crawl_session_items",
)

#: Статусы сеанса, чьи байты ещё могут быть переиграны.
RESUMABLE_STATUSES: tuple[str, ...] = ("running", "interrupted")


def _find_run_dbs(
    runs_dirs: list[Path] | None, runs_roots: list[Path] | None
) -> list[Path]:
    """Прогонные БД: ``{dir}/*/cohound.db`` из ``--runs-dir`` ∪ ``rglob`` из ``--runs-root``.

    Дедуп по каноническому пути: один и тот же файл, названный дважды, не должен
    читаться дважды (и не должен удваивать счёт в отчёте).
    """
    found: dict[Path, None] = {}
    for directory in runs_dirs or []:
        if not directory.is_dir():
            raise PruneRefused(f"--runs-dir не каталог: {directory}")
        for child in sorted(directory.iterdir()):
            candidate = child / RUN_DB_NAME
            if candidate.is_file():
                found[_canonical(candidate)] = None
    for runs_root in runs_roots or []:
        if not runs_root.is_dir():
            raise PruneRefused(f"--runs-root не каталог: {runs_root}")
        for candidate in sorted(runs_root.rglob(RUN_DB_NAME)):
            found[_canonical(candidate)] = None
    return list(found)


def _run_id_of(path: Path) -> str:
    """``run_id`` прогонной БД — имя её каталога (``runs/{run_id}/cohound.db``)."""
    return path.parent.name


def _read_run_db(path: Path) -> tuple[RunDb, dict[Path, None], set[Path]]:
    """Прочитать одну прогонную БД: ссылки на байты, живость, возобновляемые сеансы.

    Таблиц может не быть (старая схема, недописанный прогон) — отсутствующая таблица
    значит «ссылок этого вида нет», а не ошибку. Файл читается ``mode=ro``.
    """
    run_id = _run_id_of(path)
    refs: dict[Path, None] = {}
    resumable: set[Path] = set()
    live = False
    sessions = 0
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.Error:
        # Нечитаемая прогонная БД не доказывает ни живости, ни её отсутствия: считаем
        # прогон мёртвым и говорим об этом числом (live=False, 0 сеансов).
        return RunDb(path=path, run_id=run_id, live=False, resumable_sessions=0), refs, resumable
    try:
        tables = _table_names(conn)
        for table in RUN_RAW_TABLES:
            if table not in tables:
                continue
            try:
                rows = conn.execute(
                    f"SELECT raw_path FROM {table} WHERE raw_path IS NOT NULL"  # noqa: S608 - имя из константы RUN_RAW_TABLES
                ).fetchall()
            except sqlite3.Error:
                continue
            for (raw,) in rows:
                text = str(raw).strip()
                if text:
                    refs[_canonical(Path(text))] = None
        if "gate_decisions" in tables:
            live = live or bool(
                conn.execute(
                    "SELECT 1 FROM gate_decisions WHERE gate = 'G4' "
                    "AND verdict = 'approved' LIMIT 1"
                ).fetchone()
            )
        if "cells" in tables:
            live = live or bool(
                conn.execute("SELECT 1 FROM cells LIMIT 1").fetchone()
            )
        if "crawl_sessions" in tables:
            # Статусы фильтруются в Python, а не в SQL: ``IN (...)`` со списком значений
            # потребовал бы собирать текст запроса, а сеансов в прогоне десятки.
            resumable_ids = {
                str(row[0])
                for row in conn.execute("SELECT session_id, status FROM crawl_sessions")
                if str(row[1]) in RESUMABLE_STATUSES
            }
            sessions = len(resumable_ids)
            if resumable_ids and "crawl_session_items" in tables:
                for session_id, raw in conn.execute(
                    "SELECT session_id, raw_path FROM crawl_session_items "
                    "WHERE raw_path IS NOT NULL"
                ):
                    if str(session_id) not in resumable_ids:
                        continue
                    text = str(raw).strip()
                    if text:
                        resumable.add(_canonical(Path(text)))
    except sqlite3.Error:
        pass
    finally:
        conn.close()
    return RunDb(path=path, run_id=run_id, live=live, resumable_sessions=sessions), refs, resumable


def _load_runs(
    runs_dirs: list[Path] | None, runs_roots: list[Path] | None
) -> RunCensus:
    """Прочитать все прогонные БД в один цензус.

    ``paths`` — объединение ссылок по всем БД: файл, упомянутый хотя бы одной, перестаёт
    быть истинной сиротой. Живость прогона не влияет на то, попадёт ли файл в цензус, —
    она влияет только на разбивку цены.
    """
    roots: list[str] = [str(path) for path in runs_dirs or []]
    roots.extend(str(path) for path in runs_roots or [])
    paths: dict[Path, set[str]] = {}
    live_runs: set[str] = set()
    resumable_paths: set[Path] = set()
    dbs: list[RunDb] = []
    for path in _find_run_dbs(runs_dirs, runs_roots):
        db, refs, resumable = _read_run_db(path)
        dbs.append(db)
        if db.live:
            live_runs.add(db.run_id)
        for ref in refs:
            paths.setdefault(ref, set()).add(db.run_id)
        resumable_paths |= resumable
    return RunCensus(
        runs_dirs=roots,
        dbs=dbs,
        paths=paths,
        live_runs=live_runs,
        resumable_paths=resumable_paths,
    )


def _run_price(orphans: list[Orphan]) -> RunPrice:
    """Цена удаления по классу ``corpus_raw_run_history``.

    ``by_resumable_session`` пересекается с остальными двумя — это флаг, не категория.
    """
    live = {"files": 0, "bytes": 0}
    dead = {"files": 0, "bytes": 0}
    resumable = {"files": 0, "bytes": 0}
    drain = {"files": 0, "bytes": 0}
    for orphan in orphans:
        if orphan.cls != "corpus_raw_run_history":
            continue
        bucket = live if orphan.live else dead
        bucket["files"] += 1
        bucket["bytes"] += orphan.size
        if orphan.resumable:
            resumable["files"] += 1
            resumable["bytes"] += orphan.size
        if orphan.resumable and not orphan.live:
            # Ссылка есть только у возобновляемого сеанса мёртвого прогона — это и есть
            # незавершённая докачка (дренаж), а не архив пройденного цикла.
            drain["files"] += 1
            drain["bytes"] += orphan.size
    return RunPrice(
        by_live_runs=live,
        by_dead_runs=dead,
        by_resumable_session=resumable,
        by_drain=drain,
    )


# ------------------------------------------------------------------ corpus_raw


def _scan_corpus_raw(
    *,
    corpus_raw: Path,
    products: set[str],
    live: set[Path],
    runs: RunCensus,
    wanted: frozenset[str],
) -> tuple[list[Orphan], int]:
    """Файлы ``corpus_raw``: сироты, история прогонов и чужие продукты.

    Кандидат (нет в ``state.db``) делится на два класса: на него ссылается хотя бы одна
    прогонная БД — ``corpus_raw_run_history`` (цена названа), иначе
    ``corpus_raw_unreferenced`` (истинная сирота). Удаляются оба; разница только в цене.
    Без прогонных БД (``runs.known`` ложно) класс истории не вычисляется.

    Возвращает найденные записи и **число файлов ``.bin``**, увиденных вообще — по нему
    ловится состояние «``document_version`` пуста, а байты на диске есть».
    """
    orphans: list[Orphan] = []
    seen = 0
    if not corpus_raw.is_dir():
        return orphans, seen
    for entry in sorted(corpus_raw.iterdir()):
        if entry.is_dir():
            if entry.name not in products:
                if "corpus_raw_foreign_product" in wanted:
                    orphans.extend(_files_of(entry, "corpus_raw_foreign_product"))
                continue
            for file in sorted(entry.iterdir()):
                if not file.is_file() or file.suffix != ".bin":
                    continue
                seen += 1
                canonical = _canonical(file)
                if canonical in live:
                    continue
                run_ids = tuple(sorted(runs.paths.get(canonical, ())))
                if run_ids:
                    if "corpus_raw_run_history" not in wanted:
                        continue
                    orphans.append(
                        Orphan(
                            "corpus_raw_run_history",
                            file,
                            _size_of(file),
                            runs=run_ids,
                            live=bool(runs.live_runs & set(run_ids)),
                            resumable=canonical in runs.resumable_paths,
                        )
                    )
                    continue
                if "corpus_raw_unreferenced" not in wanted:
                    continue
                orphans.append(
                    Orphan("corpus_raw_unreferenced", file, _size_of(file))
                )
        elif entry.suffix == ".bin":
            # Файл прямо в корне ``corpus_raw``: раскладка такого не пишет, значит адрес
            # неизвестен — считаем и не трогаем.
            if "corpus_raw_foreign_product" in wanted:
                orphans.append(
                    Orphan("corpus_raw_foreign_product", entry, _size_of(entry))
                )
    return orphans, seen


def _files_of(directory: Path, cls: str) -> list[Orphan]:
    """Все файлы каталога (рекурсивно) в одном классе — только для отчёта."""
    return [
        Orphan(cls, path, _size_of(path))
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    ]


def _size_of(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


# ------------------------------------------------------------------ http cache


def _cache_key_files(http_dir: Path) -> dict[str, list[Path]]:
    """Ключ кэша → его файлы (``.bin`` и сайдкар ``.meta.json``)."""
    grouped: dict[str, list[Path]] = {}
    if not http_dir.is_dir():
        return grouped
    for entry in sorted(http_dir.iterdir()):
        if not entry.is_file():
            continue
        if entry.name.endswith(".meta.json"):
            key = entry.name[: -len(".meta.json")]
        elif entry.name.endswith(".bin"):
            key = entry.name[: -len(".bin")]
        else:
            continue
        grouped.setdefault(key, []).append(entry)
    return grouped


def _package_space(package: Path) -> tuple[dict[str, list[str]], dict[str, frozenset[str]]]:
    """Пространство одного пакета — тем же прибором, что у краула."""
    if not (package / "sources" / "corpus_urls.yaml").is_file():
        raise PruneRefused(
            f"{package}: нет sources/corpus_urls.yaml — это не пакет, пространство "
            "объявить нечем"
        )
    return declared_scopes(package)


def _scan_http_cache(
    *,
    cache: Path,
    connections: list[sqlite3.Connection],
    spaces: list[tuple[dict[str, list[str]], dict[str, frozenset[str]]]],
    products: set[str],
    wanted: frozenset[str],
) -> tuple[list[Orphan], dict[str, int]]:
    """Ключи кэша вне пространства **всех** пакетов набора и ключи без атрибуции.

    ``cache/http`` — глобальный каталог раскладки, общий для всех пакетов (та же природа,
    что у ``corpus_raw``). Поэтому ключ удаляется, только если **ни один**
    атрибутированный адрес не входит в пространство **ни одного** пакета набора
    (``spaces[0]`` — основной пакет, дальше ``--also-package``). Иначе прогон по одному
    профилю удалял бы байты, объявленные соседним.

    Атрибуция — через ``url_state.normalized_url`` **продуктов набора**: ключ, чей адрес
    лежит в строке чужого продукта, считается неатрибутированным (кэш глобальный и общий
    между пакетами, а чужая строка не описывает наши).

    Возвращает записи и счёт «спасённых набором» — файлов, которые остались бы удалены при
    одном основном пакете. Это измеренная цена правила, а не украшение отчёта.
    """
    key_files = _cache_key_files(cache / "http")
    if not key_files:
        return [], {"files": 0, "bytes": 0}

    attributed: dict[str, list[tuple[str, str]]] = {}
    for conn in connections:
        for product_id, url in conn.execute(
            "SELECT product_id, normalized_url FROM url_state"
        ):
            pid = str(product_id)
            if pid not in products:
                continue
            attributed.setdefault(http_key(str(url)), []).append((pid, str(url)))

    # Построители frontiers — по одному набору на пакет: пространство у пакетов разное.
    builders: list[_SpaceLookup] = [_SpaceLookup(roots, scope_hosts) for roots, scope_hosts in spaces]

    orphans: list[Orphan] = []
    rescued = {"files": 0, "bytes": 0}
    for key, files in sorted(key_files.items()):
        rows = attributed.get(key)
        if not rows:
            if "http_cache_unattributed" in wanted:
                orphans.extend(
                    Orphan("http_cache_unattributed", f, _size_of(f)) for f in files
                )
            continue
        if "http_cache_outside_space" not in wanted:
            continue
        in_any = any(_admits_any(builders, pid, url) for pid, url in rows)
        if in_any:
            # Спасает ли набор то, что не спас бы один основной пакет?
            if not any(_admits_any(builders[:1], pid, url) for pid, url in rows):
                rescued["files"] += len(files)
                rescued["bytes"] += sum(_size_of(f) for f in files)
            continue
        orphans.extend(
            Orphan("http_cache_outside_space", f, _size_of(f)) for f in files
        )
    return orphans, rescued


class _SpaceLookup:
    """Пространство одного пакета с кэшем frontiers и решений по host."""

    def __init__(
        self,
        roots: dict[str, list[str]],
        scope_hosts: dict[str, frozenset[str]],
    ) -> None:
        self._roots = roots
        self._scope_hosts = scope_hosts
        self._frontiers: dict[str, list[Frontier]] = {}
        self._host_caches: dict[str, dict[tuple[str, str], bool]] = {}

    def knows(self, product_id: str) -> bool:
        """Объявляет ли этот пакет продукт вовсе (есть ли у него корни)."""
        return bool(self._roots.get(product_id))

    def admits(self, product_id: str, url: str) -> bool:
        """Допускает ли адрес хоть один объявленный корень продукта этого пакета.

        Пакет, который продукта не объявляет, **голоса не имеет**: иначе соседний пакет
        «допускал» бы любой адрес чужого продукта просто потому, что ничего о нём не знает,
        и объединение пространств расширяло бы удаление вместо того, чтобы его ограничить.
        """
        roots = self._roots.get(product_id) or []
        built = self._frontiers.get(product_id)
        if built is None:
            hosts = self._scope_hosts.get(product_id, frozenset())
            built = [Frontier(root, CrawlLimits(), scope_hosts=hosts) for root in roots]
            self._frontiers[product_id] = built
            self._host_caches[product_id] = {}
        return _in_space(url, built, self._host_caches[product_id])


def _admits_any(
    builders: list[_SpaceLookup], pid: str, url: str
) -> bool:
    """Допускает ли адрес хоть один пакет, **объявляющий** этот продукт.

    Ни один пакет набора продукта не объявляет — «вне пространства» доказать нечем, и
    адрес считается допущенным (консервативно: файл остаётся).
    """
    opinions = [builder.admits(pid, url) for builder in builders if builder.knows(pid)]
    return any(opinions) if opinions else True


# ------------------------------------------------------------------ план и отчёт


def _resolve_state_dbs(
    state_dbs: list[Path] | None, state_root: Path | None
) -> list[Path]:
    """Набор БД: перечисленные ``--state-db`` ∪ найденные под ``--state-root``.

    Пустой набор — отказ: «живое множество не читали» и «живого нет» неотличимы, а цена
    ошибки — весь корпус.
    """
    resolved: dict[Path, None] = {}
    for path in state_dbs or []:
        resolved[_canonical(path)] = None
    if state_root is not None:
        for path in _find_state_dbs(state_root):
            resolved[path] = None
    if not resolved:
        raise PruneRefused(
            "не передана ни одна state.db: укажи --state-db PATH (можно несколько) "
            "или --state-root DIR — без подложки живое множество неизвестно"
        )
    return list(resolved)


def _check_products_have_rows(
    *,
    corpus_raw: Path,
    products: set[str],
    state: StateSet,
) -> None:
    """Отказ, если у продукта есть байты, а строк нет ни в одной переданной БД.

    «Ссылок нет» и «смотрели не туда» неотличимы, а цена ошибки — весь продукт.
    """
    if not corpus_raw.is_dir():
        return
    for entry in sorted(corpus_raw.iterdir()):
        if not entry.is_dir() or entry.name not in products:
            continue
        if not any(child.suffix == ".bin" for child in entry.iterdir() if child.is_file()):
            continue
        if entry.name in state.products:
            continue
        raise PruneRefused(
            f"{entry.name}: байты на диске есть, а строк в переданных state.db нет "
            f"({len(state.dbs)} БД) — БД не та или набор неполон"
        )


@dataclass
class Plan:
    """Результат замера: что удалять, что только считать и по каким БД считали."""

    orphans: list[Orphan]
    state: StateSet
    runs: RunCensus
    uncovered_state_dbs: list[dict[str, Any]]
    #: Все пакеты, чьи пространства участвовали в решении по http-кэшу (первый — основной).
    packages: list[Path]
    #: Файлы, которые остались бы удалены при одном основном пакете, но спасены набором.
    rescued_by_other_packages: dict[str, int]

    @property
    def price(self) -> RunPrice:
        """Цена удаления (нули, если прогонные БД не передавались)."""
        return self.runs.price or RunPrice({}, {}, {}, {})


def build_plan(
    *,
    package: Path,
    also_packages: list[Path] | None = None,
    state_dbs: list[Path] | None = None,
    state_root: Path | None = None,
    runs_dirs: list[Path] | None = None,
    runs_roots: list[Path] | None = None,
    corpus_raw: Path,
    cache: Path,
    only: frozenset[str] | None = None,
) -> Plan:
    """Собрать план удаления (и отчётные классы) — без единой записи на диск.

    :param package: основной пакет (``--package``); его пространство решает всегда.
    :param also_packages: дополнительные пакеты (``--also-package``, повторяемый): их
        пространства тоже участвуют в решении по http-кэшу, потому что ``cache/http``
        глобальный и общий. Продукты дополнительных пакетов добавляются в набор.
    :param state_dbs: перечисленные БД (``--state-db``, повторяемый).
    :param state_root: каталог, под которым ищутся все ``state.db`` (``--state-root``).
    :param runs_dirs: каталоги прогонов (``--runs-dir``, повторяемый) — для цены удаления.
    :param runs_roots: каталоги, под которыми ищутся все ``cohound.db`` (``--runs-root``).
    :param only: имена классов из :data:`CLASS_ORDER`; ``None`` — все.
    :raises PruneRefused: условие, при котором удаление было бы ложным.
    """
    wanted = frozenset(CLASS_ORDER if only is None else only)
    unknown = wanted - set(CLASS_ORDER)
    if unknown:
        raise PruneRefused(
            f"неизвестные классы: {', '.join(sorted(unknown))}; известны: {', '.join(CLASS_ORDER)}"
        )
    packages: list[Path] = [package]
    for extra in also_packages or []:
        if extra not in packages:
            packages.append(extra)
    # Пространства читаются до работы: пакет без ``corpus_urls.yaml`` — отказ, а не
    # «пакета нет» (иначе опечатка в пути тихо расширила бы удаление).
    spaces = [_package_space(item) for item in packages]

    paths = _resolve_state_dbs(state_dbs, state_root)
    state = _load_state(paths)
    runs = _load_runs(runs_dirs, runs_roots)
    try:
        declared = set().union(*(_declared_products(item) for item in packages))
        products = declared | state.products
        if not products:
            raise PruneRefused(
                f"набор продуктов пакета пуст: {package}/sources/corpus_urls.yaml не даёт "
                "ни одного продукта, и подложка тоже"
            )
        orphans: list[Orphan] = []
        seen_bins = 0
        if wanted & set(ONLY_GROUPS["corpus-raw"]):
            _check_products_have_rows(
                corpus_raw=corpus_raw, products=products, state=state
            )
            found, seen_bins = _scan_corpus_raw(
                corpus_raw=corpus_raw,
                products=products,
                live=state.live,
                runs=runs,
                wanted=wanted,
            )
            orphans.extend(found)
            runs.price = _run_price(found)
        if state.version_rows == 0 and seen_bins:
            # Опорожнённый набор БД — не основание удалять байты: «ссылок нет» и «таблица
            # пуста» неотличимы, а цена ошибки — весь корпус.
            raise PruneRefused(
                f"document_version пуста во всех переданных БД ({len(state.dbs)}), "
                f"а в {corpus_raw} лежит {seen_bins} файлов .bin: живое множество "
                "неизвестно, удаление было бы удалением всего корпуса"
            )
        rescued = {"files": 0, "bytes": 0}
        if wanted & set(ONLY_GROUPS["http-cache"]):
            found_cache, rescued = _scan_http_cache(
                cache=cache,
                connections=state.connections,
                spaces=spaces,
                products=products,
                wanted=wanted,
            )
            orphans.extend(found_cache)
        uncovered: list[dict[str, Any]] = []
        for neighbour in _neighbour_state_dbs(paths):
            hit = _uncovered_products(neighbour, products)
            if hit:
                uncovered.append({"path": str(neighbour), "products": hit})
        return Plan(
            orphans=orphans,
            state=state,
            runs=runs,
            uncovered_state_dbs=uncovered,
            packages=packages,
            rescued_by_other_packages=rescued,
        )
    finally:
        state.close()


def scan(
    *,
    package: Path,
    state_db: Path | list[Path] | None = None,
    state_root: Path | None = None,
    runs_dir: Path | list[Path] | None = None,
    runs_root: Path | list[Path] | None = None,
    corpus_raw: Path,
    cache: Path,
    only: frozenset[str] | None = None,
) -> list[Orphan]:
    """План удаления без служебных полей — удобно тестам и приборам.

    Обёртка над :func:`build_plan`: ``state_db`` принимает и один путь, и список.
    """
    if state_db is None:
        dbs: list[Path] | None = None
    elif isinstance(state_db, (list, tuple)):
        dbs = list(state_db)
    else:
        dbs = [state_db]
    if runs_dir is None:
        dirs: list[Path] | None = None
    elif isinstance(runs_dir, (list, tuple)):
        dirs = list(runs_dir)
    else:
        dirs = [runs_dir]
    if runs_root is None:
        roots: list[Path] | None = None
    elif isinstance(runs_root, (list, tuple)):
        roots = list(runs_root)
    else:
        roots = [runs_root]
    plan = build_plan(
        package=package,
        state_dbs=dbs,
        state_root=state_root,
        runs_dirs=dirs,
        runs_roots=roots,
        corpus_raw=corpus_raw,
        cache=cache,
        only=only,
    )
    return plan.orphans


def _aggregate(orphans: list[Orphan]) -> dict[str, dict[str, int]]:
    """Класс → ``{files, bytes}`` (все классы в :data:`CLASS_ORDER`, включая нулевые)."""
    totals: dict[str, dict[str, int]] = {
        cls: {"files": 0, "bytes": 0} for cls in CLASS_ORDER
    }
    for orphan in orphans:
        bucket = totals[orphan.cls]
        bucket["files"] += 1
        bucket["bytes"] += orphan.size
    return totals


def _sum(classes: dict[str, dict[str, int]], names: tuple[str, ...]) -> dict[str, int]:
    return {
        "files": sum(classes[name]["files"] for name in names),
        "bytes": sum(classes[name]["bytes"] for name in names),
    }


def _print_packages(plan: Plan) -> None:
    """Пакеты, чьи пространства решали судьбу http-кэша, и цена этого правила."""
    if len(plan.packages) > 1:
        print("пакеты (пространства объединены):")
        for item in plan.packages:
            print(f"  {item}")
    rescued = plan.rescued_by_other_packages
    print(
        f"спасено соседними пакетами: {rescued['files']} файлов "
        f"({_human_bytes(rescued['bytes'])}) — остались бы удалены при одном --package"
    )


def _print_run_census(runs: RunCensus) -> None:
    """Сколько прогонных БД прочитано и сколько из них живых — контекст цены."""
    if not runs.known:
        print("прогонные БД: не переданы — цена удаления неизвестна")
        return
    print(
        f"прогонных БД: {len(runs.dbs)}, живых {len(runs.live_runs)}, "
        f"ссылок на байты {len(runs.paths)}"
    )


def _print_state_dbs(state: StateSet) -> None:
    """Список участвовавших БД и живых путей в каждой — что именно объединили."""
    print(f"state.db в наборе: {len(state.dbs)}")
    for db in state.dbs:
        print(
            f"  {db.path}: версий {db.version_rows}, живых путей {db.live_paths}"
        )
    print(f"  объединение: живых путей {len(state.live)} (объединение по всем БД)")


def _print_price(price: RunPrice) -> None:
    """Разбивка цены под классом ``corpus_raw_run_history``.

    Три строки — **не** разбиение: ``by_resumable_session`` пересекается с обеими
    остальными (флаг), поэтому и подписан флагом, а не категорией.
    """
    rows = (
        ("цена: живые прогоны", price.by_live_runs, ""),
        ("цена: только мёртвые прогоны", price.by_dead_runs, ""),
        (
            "цена: возобновляемые сеансы",
            price.by_resumable_session,
            "  (флаг, пересекается с двумя строками выше)",
        ),
    )
    for label, bucket, note in rows:
        print(
            f"   {label:<28}{bucket['files']:>8} файлов "
            f"{_human_bytes(bucket['bytes']):>12}{note}"
        )
    print(
        "   цена: байты переигровки без сети и возобновляемых сеансов — "
        "resume/frozen по этим прогонам пойдут в сеть"
    )
    drain = price.by_drain
    print(
        f"   цена: дренаж/возобновляемый сеанс — {drain['files']} файлов "
        f"({_human_bytes(drain['bytes'])}); после удаления докачка начнётся заново"
    )


def _print_report(
    orphans: list[Orphan],
    totals: dict[str, dict[str, int]],
    price: RunPrice,
) -> None:
    for cls in CLASS_ORDER:
        bucket = totals[cls]
        if not bucket["files"]:
            continue
        mark = "удаляется" if cls in DELETABLE_CLASSES else "только отчёт"
        print(
            f"{cls:<28}{bucket['files']:>8} файлов {_human_bytes(bucket['bytes']):>12}  "
            f"({mark})"
        )
        for orphan in [item for item in orphans if item.cls == cls][:_SAMPLES]:
            print(f"    {orphan.path}")
        if cls == "corpus_raw_run_history":
            _print_price(price)
    deletable = _sum(totals, DELETABLE_CLASSES)
    reported = _sum(totals, REPORT_ONLY_CLASSES)
    print(
        f"{'ИТОГО удаляется':<28}{deletable['files']:>8} файлов "
        f"{_human_bytes(deletable['bytes']):>12}"
    )
    print(
        f"{'ИТОГО в отчёте':<28}{reported['files']:>8} файлов "
        f"{_human_bytes(reported['bytes']):>12}"
    )


def _delete(orphans: list[Orphan]) -> list[tuple[Path, str]]:
    """Удалить файлы; вернуть пары ``(путь, причина)`` для неудач."""
    failures: list[tuple[Path, str]] = []
    for orphan in orphans:
        try:
            orphan.path.unlink()
        except FileNotFoundError:
            continue
        except OSError as exc:
            failures.append((orphan.path, str(exc)))
    return failures


def _write_audit(
    *,
    audit: Path,
    actor: str,
    package: Path,
    state_dbs: list[str],
    runs: RunCensus,
    totals: dict[str, dict[str, int]],
    deleted: list[Orphan],
) -> None:
    """Дописать в аудит строку-сводку и по строке на каждый удалённый файл.

    Цензус прогонных БД входит в сводку: без него «удалено N файлов» не отличить от
    «удалена история M прогонов». ``runs`` построчной записи — адреса, которые на файл
    ссылались: пустой список значит «истинная сирота», и это тоже провенанс.
    """
    audit.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "at": _utc_now_iso(),
        "actor": actor,
        "package": str(package),
        "state_dbs": list(state_dbs),
        "classes": {
            cls: {"files": totals[cls]["files"], "bytes": totals[cls]["bytes"]}
            for cls in CLASS_ORDER
        },
        "run_census": runs.as_dict(),
        "total": _sum(totals, DELETABLE_CLASSES),
    }
    with audit.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(summary, ensure_ascii=False) + "\n")
        for orphan in deleted:
            handle.write(
                json.dumps(
                    {
                        "class": orphan.cls,
                        "path": str(orphan.path),
                        "bytes": orphan.size,
                        "runs": list(orphan.runs),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


# ------------------------------------------------------------------ CLI


def _parse_only(raw: str | None) -> frozenset[str] | None:
    """``--only corpus-raw,http-cache`` → набор классов (``None`` — все)."""
    if raw is None:
        return None
    classes: set[str] = set()
    for token in (part.strip() for part in raw.split(",")):
        if not token:
            continue
        if token in ONLY_GROUPS:
            classes.update(ONLY_GROUPS[token])
        elif token in CLASS_ORDER:
            classes.add(token)
        else:
            raise PruneRefused(
                f"неизвестный класс/группа в --only: {token!r}; "
                f"известны: {', '.join(sorted(ONLY_GROUPS))} "
                f"(или имена классов: {', '.join(CLASS_ORDER)})"
            )
    if not classes:
        raise PruneRefused("--only пуст: не указан ни один класс")
    return frozenset(classes)


def _resolve_dirs(
    corpus_raw: Path | None, cache: Path | None
) -> tuple[Path, Path]:
    """Корни ``corpus_raw``/``cache``: флаг > объявленная раскладка > дефолт модели.

    Раскладка резолвится той же функцией, что и у CLI
    (:func:`core.config.paths.resolve_paths`): ``COHOUND_PATHS_FILE``/``COHOUND_HOME``
    читаются из окружения, относительные пути приклеиваются к дому. Когда оба корня
    названы флагами, раскладка не читается вовсе — тест не должен зависеть от того,
    что лежит в ``~/.cohound`` у хоста.
    """
    if corpus_raw is not None and cache is not None:
        return corpus_raw, cache
    from core.config.paths import resolve_paths

    config = resolve_paths()
    return (
        corpus_raw if corpus_raw is not None else config.corpus_raw,
        cache if cache is not None else config.cache,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Удалить артефакты на диске, не упомянутые ни в одной из переданных state.db "
            "(dry-run по умолчанию). Живое множество — ОБЪЕДИНЕНИЕ по всем БД: один "
            "каталог corpus_raw делят канонная БД и БД стендов, и прогон по одной БД "
            "удаляет чужой корпус (замер 27.11.2026: 5 455 файлов / 1.09 GB)."
        )
    )
    parser.add_argument("--package", required=True, type=Path)
    parser.add_argument(
        "--also-package",
        action="append",
        type=Path,
        default=None,
        metavar="DIR",
        help=(
            "дополнительный пакет: его пространство тоже участвует в решении по "
            "http-кэшу (cache/http общий для всех пакетов); повторяемый"
        ),
    )
    parser.add_argument(
        "--state-db",
        action="append",
        type=Path,
        default=None,
        metavar="PATH",
        help="state.db пакета; повторяемый — живое множество считается по всем сразу",
    )
    parser.add_argument(
        "--state-root",
        type=Path,
        default=None,
        metavar="DIR",
        help="добавить все найденные под DIR state.db (rglob) к набору",
    )
    parser.add_argument(
        "--runs-dir",
        action="append",
        type=Path,
        default=None,
        metavar="DIR",
        help=(
            "каталог прогонов: добавляет {DIR}/*/cohound.db — по ним считается ЦЕНА "
            "удаления (класс corpus_raw_run_history); повторяемый"
        ),
    )
    parser.add_argument(
        "--runs-root",
        action="append",
        type=Path,
        default=None,
        metavar="DIR",
        help="добавить все найденные под DIR cohound.db (rglob); повторяемый",
    )
    parser.add_argument("--corpus-raw", type=Path, default=None)
    parser.add_argument("--cache", type=Path, default=None)
    parser.add_argument(
        "--only",
        default=None,
        help="ограничить группы: corpus-raw,http-cache (или имена классов)",
    )
    parser.add_argument("--json", action="store_true", help="машинный вывод")
    parser.add_argument(
        "--apply", action="store_true", help="удалить файлы (без флага — только отчёт)"
    )
    parser.add_argument("--actor", default=None, help="кто удаляет (обязателен с --apply)")
    parser.add_argument(
        "--audit", type=Path, default=None, help="JSONL-аудит (по умолчанию рядом с первой state.db набора)"
    )
    args = parser.parse_args(argv)

    try:
        if args.apply and not args.actor:
            raise PruneRefused("--apply без --actor: удаление без имени в аудите запрещено")
        only = _parse_only(args.only)
        corpus_raw, cache = _resolve_dirs(args.corpus_raw, args.cache)
        plan = build_plan(
            package=args.package,
            also_packages=args.also_package,
            state_dbs=args.state_db,
            state_root=args.state_root,
            runs_dirs=args.runs_dir,
            runs_roots=args.runs_root,
            corpus_raw=corpus_raw,
            cache=cache,
            only=only,
        )
    except PruneRefused as exc:
        print(f"ОТКАЗ: {exc}", file=sys.stderr)
        return 2

    orphans = plan.orphans
    state = plan.state
    state_dbs = [str(db.path) for db in state.dbs]
    totals = _aggregate(orphans)
    to_delete = [item for item in orphans if item.cls in DELETABLE_CLASSES]
    failures: list[tuple[Path, str]] = []
    audit_path = args.audit or (state.dbs[0].path.parent / AUDIT_NAME)
    deleted: list[Orphan] = []

    if not plan.runs.known:
        print(
            "ВНИМАНИЕ: прогонные БД не переданы (--runs-dir/--runs-root) — цена удаления "
            "неизвестна; класс corpus_raw_run_history не вычисляется",
            file=sys.stderr,
        )

    for entry in plan.uncovered_state_dbs:
        print(
            f"ВНИМАНИЕ: рядом есть state.db вне набора — {entry['path']} "
            f"(продукты: {', '.join(entry['products'])}); живое множество может быть "
            "неполным, добавь --state-db или --state-root",
            file=sys.stderr,
        )

    # Отсутствующий корень — не «файлов нет», а «смотреть было некуда»: молчаливый ноль
    # читался бы как чистый каталог (та же ловушка, что у поиска, уважающего .gitignore).
    missing_roots = [
        str(path) for path in (corpus_raw, cache) if not path.is_dir()
    ]

    if args.apply and to_delete:
        failures = _delete(to_delete)
        failed = {path for path, _ in failures}
        deleted = [item for item in to_delete if item.path not in failed]
        if deleted:
            _write_audit(
                audit=audit_path,
                actor=str(args.actor),
                package=args.package,
                state_dbs=state_dbs,
                runs=plan.runs,
                totals=totals,
                deleted=deleted,
            )

    if args.json:
        print(
            json.dumps(
                {
                    "package": str(args.package),
                    "packages": [str(item) for item in plan.packages],
                    "rescued_by_other_packages": plan.rescued_by_other_packages,
                    "state_dbs": state_dbs,
                    "state_db_info": state.as_dict()["state_dbs"],
                    "live_paths": len(state.live),
                    "uncovered_state_dbs": plan.uncovered_state_dbs,
                    "run_census": plan.runs.as_dict(),
                    "price": plan.price.as_dict(),
                    "corpus_raw": str(corpus_raw),
                    "cache": str(cache),
                    "apply": bool(args.apply),
                    "classes": {
                        cls: {
                            "files": totals[cls]["files"],
                            "bytes": totals[cls]["bytes"],
                            "deletable": cls in DELETABLE_CLASSES,
                        }
                        for cls in CLASS_ORDER
                    },
                    "missing_roots": missing_roots,
                    "total": _sum(totals, DELETABLE_CLASSES),
                    "report_only": _sum(totals, REPORT_ONLY_CLASSES),
                    "deleted": {
                        "files": len(deleted),
                        "bytes": sum(item.size for item in deleted),
                    },
                    "failed": [{"path": str(p), "reason": r} for p, r in failures],
                },
                ensure_ascii=False,
                indent=1,
            )
        )
    else:
        _print_packages(plan)
        _print_state_dbs(state)
        _print_run_census(plan.runs)
        print()
        _print_report(orphans, totals, plan.price)
        for root in missing_roots:
            print(f"ВНИМАНИЕ: каталога нет — {root}", file=sys.stderr)
        if not args.apply:
            print("dry-run: ничего не удалено (--apply --actor NAME для удаления)")
        else:
            print(
                f"удалено файлов: {len(deleted)}, "
                f"освобождено {_human_bytes(sum(item.size for item in deleted))}"
            )
            if deleted:
                print(f"аудит: {audit_path}")
        for path, reason in failures:
            print(f"НЕ УДАЛЕНО: {path}: {reason}", file=sys.stderr)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
