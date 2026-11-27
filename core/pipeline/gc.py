"""``hound gc`` — уборка артефактов на диске, на которые не ссылается ни одна ``state.db``.

**Почему это в ``core``, а не в скрипте.** Логика была прибором
(``scripts/prune_orphans.py``), но операция перестала быть замером: она решает, что
удалять, и цена ошибки — чужой корпус. Прибор с одной реализацией живёт ровно до первой
правки; команда обязана иметь один дом. Скрипт остался тонкой обёрткой над
:func:`core.cli.commands.gc.main`.

**Зачем уборка.** Два каталога растут монотонно и не имеют владельца-сборщика:

* ``corpus_raw/{product_id}/{hash}.bin`` — retention сырых байтов (I19). Файл живёт
  ровно до тех пор, пока на него ссылается ``document_version.raw_path`` **или**
  ``container_state.raw_path``: контейнер — не документ, версии документа у него нет по
  построению, и его байты учтены только второй таблицей;
* ``cache/http/{key}.bin`` (+ сайдкар ``{key}.meta.json``) — глобальный
  content-addressed кэш внешних вызовов (R7, ADR 0016). Ключ — ``http_key(url)``; записи
  вне объявленного пространства продукта в корпус попасть не могут (§6), но их байты
  остаются в кэше навсегда.

Модуль **не** трогает схему и **не** пишет в БД: он читает ``state.db`` в режиме
``mode=ro`` и удаляет файлы. Схема не меняется, прогон не нужен, ``run_id`` не меняется
(это подложка, а не условия измерения).

Главное правило: живое множество — объединение по **всем** переданным БД
---------------------------------------------------------------------

Один каталог ``corpus_raw`` делят несколько ``state.db``: у пакета есть канонная БД под
``{home}/state/{package}/state.db`` и отдельные БД стендов, а байты у них общие. Замер
27.11.2026 на пилотном пакете: канонная БД — 8 738 версий, стенд — 43 825, пересечение
живых путей — **3 283**. Прогон по одной стендовой БД давал 74 879 кандидатов, из которых
**5 455 файлов / 1.09 GB нужны канонной БД**: ``--apply`` по одной БД уничтожил бы чужой
корпус. Поэтому ``--state-db`` повторяемый, ``--state-root`` добавляет все найденные под
каталогом БД, а живое множество и набор продуктов считаются **объединением**:

    живое = ⋃(document_version.raw_path ∪ container_state.raw_path) по всем БД

Если рядом с переданной БД (до 4 уровней вверх) лежит ``state.db``, не включённая в набор,
и в ней есть версии продукта пакета — печатается предупреждение (наблюдение, **не**
отказ: иначе команда перестала бы работать на любой раскладке с чужими БД).

Правило и его цена
------------------

Живость байтов определяется **только** ``state.db``: файл, на который не ссылается
``document_version.raw_path`` или ``container_state.raw_path``, удаляется — по решению
владельца 27.11.2026 это пересмотр прежнего «``corpus_raw`` хранится бессрочно» (I19).
Причина: ``state.db`` держит **активную** версию документа, а прогонные БД
(``runs/{run_id}/cohound.db``) — провенанс, а не живость.

Но удаление имеет **цену**, и модуль обязан её назвать, а не заплатить молча. Прогонные
БД ссылаются на байты тремя путями: ``corpus_docs.raw_path`` (опубликованное evidence),
``extract_queue.raw_path`` (очередь конвертации) и ``crawl_session_items.raw_path`` —
последнее и есть **переигровка байтов без сети** (``resume``/``frozen``): после удаления
такой прогон пойдёт в сеть. Поэтому кандидат, на который ссылается **хотя бы одна**
прогонная БД, выделяется в отдельный класс ``corpus_raw_run_history`` (удаляется так же —
это цена, а не запрет), а его ссылки публикуются разбивкой ``by_live_runs``/
``by_dead_runs``/``by_resumable_session``/``by_drain``. Без ``--runs-dir``/``--runs-root``
цена **неизвестна**: класс не вычисляется, все кандидаты остаются в
``corpus_raw_unreferenced``, в stderr печатается предупреждение.

Общее пространство — не только у ``corpus_raw``
----------------------------------------------

``cache/http`` тоже глобальный каталог раскладки: его делят **все** пакеты, а ключ кэша
описывает адрес, а не пакет. Поэтому решение «вне объявленного пространства» принимается
против пространства **всех** пакетов набора: ``--package`` (основной) и
``--also-package`` (повторяемый). Ключ удаляется, только если ни один атрибутированный
ему адрес не входит в пространство ни одного пакета набора; отчёт печатает
``rescued_by_other_packages`` — сколько файлов осталось бы удалено при одном ``--package``.

Классы
------

Удаляемые (по умолчанию все, ``--only`` ограничивает):

``corpus_raw_unreferenced``
    ``{corpus_raw}/{product}/{hash}.bin``, где ``product`` — продукт пакета, а
    канонический путь файла не входит в живое множество **и** не упомянут ни одной
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
    Файл ``.bin`` в подкаталоге ``corpus_raw``, чей каталог не является продуктом пакета.
``http_cache_unattributed``
    Ключ кэша, для которого в ``url_state`` нет строки **вообще**.
``runs_signed``
    Прогон с подписанным G4 — это опубликованное evidence, удалять нельзя.
``runs_resumable``
    Прогон с возобновляемым сеансом (``running``/``interrupted``) — его байты ещё
    переигрываются без сети.
``runs_unsigned``
    Всё остальное. **Не** список кандидатов на удаление: waiver вместо G4 легален, а
    прогон с ячейками уже произвёл вердикты, и его ``outputs/`` без него не
    восстановить. Удаление — только явным ``--prune-run RUN_ID``.
``cache_budget``
    Объём ``extract_cache``/``embed_cache``/``eval_cache`` на диске против объявленного
    бюджета (``deploy.extract.cache_max_bytes``; у остальных бюджета нет — ``None``).

Предикат пространства **не** переписан здесь вторым определением: он собран из
:class:`~core.pipeline.crawl_frontier.Frontier` (``host_in_scope``) и
``_path_within``/``decoded_scope_path`` — тех же функций, которыми решает краул.
Совпадение с прибором ``scripts/measure_substrate_reuse.py`` проверяется тестом
``tests/scripts/test_prune_orphans.py::test_space_predicate_parity_with_instrument``:
две копии предиката разошлись бы молча, и это в репозитории уже случалось.

Отказы (код ``2``, ничего не пишется)
------------------------------------

* не передан ни ``--state-db``, ни ``--state-root``;
* ``state.db`` не открывается / в нём нет ``document_version`` или ``url_state``
  (``container_state`` **необязательна**);
* каталог ``corpus_raw/{product}`` содержит ``.bin``, а ``product_id`` не встречается ни
  в одной из переданных БД: «ссылок нет» и «смотрели не туда» неотличимы;
* ``document_version`` пуста **во всех** переданных БД, а ``corpus_raw`` непуст;
* набор продуктов пакета пуст;
* ``--only`` с неизвестным классом;
* ``--apply`` без ``--actor``;
* ``--runs-dir``/``--runs-root`` указывают на несуществующий каталог;
* ``--also-package`` указывает на каталог без ``sources/corpus_urls.yaml``;
* ``--prune-run`` называет прогон, которого нет, который подписан G4 или у которого есть
  возобновляемый сеанс.

Прогонные БД **не** дают отказа ни в каком случае: нечитаемая или старая БД считается
мёртвой (``live=False``, 0 сеансов) — «не прочитали» не доказывает ни живости, ни её
отсутствия (A3).

Аудит
-----

При ``--apply`` — JSONL в ``--audit`` (по умолчанию ``{каталог первой state.db}/
prune_orphans.jsonl``, дописыванием): первая строка — сводка
``{at, actor, package, state_dbs, classes, run_census, runs_pruned, total}``; далее по
строке на каждый удалённый файл ``{class, path, bytes, runs}`` и на каждый удалённый
прогон ``{class: "prune_run", run_id, path, bytes, cells}``.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров — продукт
приходит только как ``--package``.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from core.config.seeds import CORPUS_URLS_RELPATH, load_corpus_urls
from core.pipeline.crawl_frontier import (
    CrawlLimits,
    Frontier,
    _path_within,
    decoded_scope_path,
    normalize_scope_host,
)
from core.util.cache import http_key

__all__ = [
    "AUDIT_NAME",
    "CACHE_BUDGET_CLASS",
    "CLASS_ORDER",
    "DEFAULT_DOCS_ROOT",
    "DELETABLE_CLASSES",
    "ONLY_GROUPS",
    "REPORT_ONLY_CLASSES",
    "REPO_ROOT",
    "RUN_CLASS_ORDER",
    "CacheBudget",
    "GcRefused",
    "Orphan",
    "Plan",
    "PruneRun",
    "RunCensus",
    "RunDb",
    "RunPrice",
    "WipePlan",
    "apply_gc",
    "build_gc_plan",
    "declared_scopes",
    "http_cache_files",
    "human_bytes",
    "in_space",
    "plan_wipe_http_cache",
    "scan_gc",
]

#: Имя файла аудита рядом с ``state.db`` (дописывается, одна строка — одно событие).
AUDIT_NAME = "prune_orphans.jsonl"

#: Удаляемые классы. Порядок — от «цена ошибки больше» к «меньше»; печать идёт в том же
#: порядке, чтобы оператор видел цену раньше, чем итог.
DELETABLE_CLASSES: tuple[str, ...] = (
    "corpus_raw_unreferenced",
    "corpus_raw_run_history",
    "http_cache_outside_space",
    # Вытеснение по объявленному бюджету (``cache.http_max_bytes``) — последним в порядке
    # печати: это не потеря доказательства, а граница роста кэша (см. ``cache/http``).
    "http_cache_over_budget",
)

#: Классы, которые только считаются и печатаются.
REPORT_ONLY_CLASSES: tuple[str, ...] = (
    "corpus_raw_foreign_product",
    "http_cache_unattributed",
)

#: Порядок печати файловых классов.
CLASS_ORDER: tuple[str, ...] = DELETABLE_CLASSES + REPORT_ONLY_CLASSES

#: Классы прогонов — отчётные, удаляются только явным ``--prune-run``.
RUN_CLASS_ORDER: tuple[str, ...] = (
    "runs_signed",
    "runs_resumable",
    "runs_unsigned",
)

#: Отчётный класс бюджета кэшей (не файл и не прогон: агрегат по каталогу).
CACHE_BUDGET_CLASS = "cache_budget"

#: Все имена классов, которые понимает ``--only``.
ALL_CLASSES: tuple[str, ...] = CLASS_ORDER + RUN_CLASS_ORDER + (CACHE_BUDGET_CLASS,)

#: Группы для ``--only``: имя группы → классы, которые она включает.
ONLY_GROUPS: dict[str, tuple[str, ...]] = {
    "corpus-raw": (
        "corpus_raw_unreferenced",
        "corpus_raw_run_history",
        "corpus_raw_foreign_product",
    ),
    "http-cache": (
        "http_cache_outside_space",
        "http_cache_unattributed",
        "http_cache_over_budget",
    ),
    "runs": RUN_CLASS_ORDER,
    "cache-budget": (CACHE_BUDGET_CLASS,),
}

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

#: Корень репозитория — от ``__file__``, а не от CWD: цензус цитирования не должен
#: зависеть от того, откуда запущена команда.
REPO_ROOT = Path(__file__).resolve().parents[2]

#: Корень документов по умолчанию (``--docs-root``): ``docs/`` репозитория.
DEFAULT_DOCS_ROOT = REPO_ROOT / "docs"

#: Хвост ``run_id``, который документы называют вместо полного имени: 12 hex-символов.
_RUN_HASH_RE = re.compile(r"[0-9a-f]{12}")


class GcRefused(Exception):
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
    """Одна прогонная БД и её роль в цене удаления и в отчёте по прогонам.

    ``live`` — прежнее определение цены (G4 подписан **или** есть ячейки).
    ``g4`` — именно подпись G4: по ней прогон попадает в ``runs_signed`` и не может быть
    удалён. ``cells``/``outputs``/``bytes`` печатаются рядом с ``run_id``, иначе
    1267-ячеечная карта неотличима от пустой пробы.

    ``cited_in`` — относительные пути документов из ``--docs-root``, в которых встречается
    ``run_id``. Это цензус цитирования, а не вердикт: замер 28.11.2026 показал, что 12 из
    19 незанятых прогонов — **подложка опубликованных замеров** (3.18 GiB из 3.19), в том
    числе прогоны с нулём вердиктов (``208472049479`` — перепись CJK, ``48bf65507223`` —
    датапоинт text-layer). «Нет G4» и «нет ячеек» не отвечают на вопрос «на что это
    опирается».
    """

    path: Path
    run_id: str
    live: bool
    resumable_sessions: int
    g4: bool = False
    cells: int = 0
    outputs: int = 0
    bytes: int = 0
    cited_in: tuple[str, ...] = ()

    @property
    def run_dir(self) -> Path:
        """Каталог прогона (``runs/{run_id}``) — единица удаления ``--prune-run``."""
        return self.path.parent

    @property
    def cls(self) -> str:
        """Отчётный класс прогона: подпись G4 > возобновляемый сеанс > прочее."""
        if self.g4:
            return "runs_signed"
        if self.resumable_sessions:
            return "runs_resumable"
        return "runs_unsigned"

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "run_id": self.run_id,
            "live": self.live,
            "resumable_sessions": self.resumable_sessions,
            "g4": self.g4,
            "cells": self.cells,
            "outputs": self.outputs,
            "bytes": self.bytes,
            "cited_in": list(self.cited_in),
        }


@dataclass(frozen=True)
class RunPrice:
    """Цена удаления: сколько байтов прогоны потеряют и чем именно.

    ``by_resumable_session`` — **флаг, а не третья категория**: он пересекается и с
    ``by_live_runs``, и с ``by_dead_runs``. ``by_drain`` — тоже флаг и подмножество
    ``by_dead_runs ∩ by_resumable_session``: единственная ссылка на файл — возобновляемый
    сеанс мёртвого прогона. Складывать любые два числа здесь нельзя — это не разбиение.
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

    def by_class(self) -> dict[str, list[RunDb]]:
        """Прогоны по отчётным классам (порядок каталогов — детерминированный)."""
        grouped: dict[str, list[RunDb]] = {cls: [] for cls in RUN_CLASS_ORDER}
        for db in self.dbs:
            grouped[db.cls].append(db)
        return grouped

    def as_dict(self) -> dict[str, Any]:
        return {
            "runs_dir": list(self.runs_dirs),
            "runs": [db.as_dict() for db in self.dbs],
            "price": (self.price or RunPrice({}, {}, {}, {})).as_dict(),
        }


@dataclass(frozen=True)
class PruneRun:
    """Прогон, названный явно для удаления (``--prune-run``)."""

    db: RunDb

    @property
    def run_id(self) -> str:
        return self.db.run_id

    @property
    def path(self) -> Path:
        return self.db.run_dir

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "path": str(self.path),
            "bytes": self.db.bytes,
            "cells": self.db.cells,
        }


@dataclass(frozen=True)
class CacheBudget:
    """Объём кэша на диске против объявленного бюджета (``None`` — бюджета нет)."""

    name: str
    path: Path
    bytes: int
    budget: int | None

    @property
    def over_budget(self) -> bool:
        """Превышен ли объявленный бюджет (без бюджета — никогда)."""
        return self.budget is not None and self.bytes > self.budget

    @property
    def share(self) -> float | None:
        """Доля бюджета, занятая кэшем; ``None`` — бюджета нет."""
        if self.budget is None or self.budget <= 0:
            return None
        return self.bytes / self.budget

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": str(self.path),
            "bytes": self.bytes,
            "budget": self.budget,
            "over_budget": self.over_budget,
            "share": self.share,
        }


@dataclass(frozen=True)
class StateDb:
    """Одна переданная БД и то, что из неё прочитано."""

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

    ``raw_path`` в ``state.db`` абсолютен, но сравнение строк хрупко: раскладка может
    быть симлинком, а прогон — идти из другого корня. ``strict=False`` — путь файла может
    уже не существовать.
    """
    try:
        return path.resolve()
    except OSError:  # noqa: BLE001 — кривой путь не должен ронять замер
        return Path(os.path.abspath(path))


def human_bytes(size: int) -> str:
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
        raise GcRefused(f"state.db не найден: {state_db}")
    try:
        conn = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
        tables = _table_names(conn)
    except sqlite3.Error as exc:
        raise GcRefused(f"state.db не читается как SQLite: {state_db}: {exc}") from exc
    missing = [name for name in _REQUIRED_TABLES if name not in tables]
    if missing:
        conn.close()
        raise GcRefused(
            f"в {state_db} нет таблиц {', '.join(missing)} — это не подложка пакета"
        )
    return conn, tables


def _declared_products(package: Path) -> set[str]:
    """Продукты, объявленные пакетом (ключи ``sources/corpus_urls.yaml``)."""
    try:
        declared = load_corpus_urls(package / CORPUS_URLS_RELPATH)
    except ValueError as exc:
        raise GcRefused(f"{package}: sources/corpus_urls.yaml не читается: {exc}") from exc
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
    """Объединить все переданные БД: живое множество и продукты — по всем сразу."""
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
        raise GcRefused(f"--state-root не каталог: {root}")
    return sorted({_canonical(path) for path in root.rglob(STATE_DB_NAME)})


def _neighbour_state_dbs(state_dbs: list[Path]) -> list[Path]:
    """``state.db`` до 4 уровней вверх от каталогов переданных, не входящие в набор.

    Это **наблюдение**, а не основание отказа: соседняя БД может принадлежать другому
    пакету, и блокировать удаление из-за неё значило бы сделать команду неработающей на
    любой раскладке с чужими БД.
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
    except GcRefused:
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


# ------------------------------------------------------------------ пространство


def declared_scopes(
    package: Path,
) -> tuple[dict[str, list[str]], dict[str, frozenset[str]]]:
    """Корни продуктов и допущенные поддомены — **тем же** правилом, что у краула.

    Построение повторяет блок ``scope_hosts_by_product`` в
    :func:`core.pipeline.corpus.build_corpus`: объявленные хосты — ключи
    ``domain_profiles.yaml`` плюс все seed-корни; хост, объявленный собственным корнем
    продукта, из «поддомен-допуска» этого продукта исключается (у него свой бюджет).
    """
    roots: dict[str, list[str]] = {
        str(pid): [str(url) for url in (urls or [])]
        for pid, urls in load_corpus_urls(package / CORPUS_URLS_RELPATH).items()
    }
    profiles = _read_yaml(package / "domain_profiles.yaml") or {}
    all_root_hosts = {
        normalize_scope_host(urlsplit(url).hostname or "")
        for urls in roots.values()
        for url in urls
    }
    profile_hosts = {
        normalize_scope_host(str(key).split("/", 1)[0]) for key in profiles
    }
    declared_hosts = (all_root_hosts | profile_hosts) - {""}
    scope_hosts = {
        pid: frozenset(
            declared_hosts
            - {normalize_scope_host(urlsplit(url).hostname or "") for url in urls}
        )
        for pid, urls in roots.items()
    }
    return roots, scope_hosts


def in_space(
    url: str,
    frontiers: list[Frontier],
    host_cache: dict[tuple[str, str], bool],
) -> bool:
    """Допускает ли URL хоть один объявленный корень продукта (host + path-prefix).

    Предикат тот же, что у краула (:meth:`Frontier.scope_admission`), но без
    ``validate_url``: SSRF — гейт **фетча**, а не определение пространства, и адрес,
    который его не прошёл, в ``url_state`` не попадал. Прогонять SSRF на десятках тысяч
    адресов значило бы платить минуты за ответ, который уже известен.
    """
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    decoded = decoded_scope_path(parsed.path or "/")
    if decoded is None:
        return False
    for frontier in frontiers:
        key = (frontier.root_host, host)
        allowed = host_cache.get(key)
        if allowed is None:
            allowed = frontier.host_in_scope(host)
            host_cache[key] = allowed
        if allowed and _path_within(decoded, frontier.path_prefix):
            return True
    return False


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
            raise GcRefused(f"--runs-dir не каталог: {directory}")
        for child in sorted(directory.iterdir()):
            candidate = child / RUN_DB_NAME
            if candidate.is_file():
                found[_canonical(candidate)] = None
    for runs_root in runs_roots or []:
        if not runs_root.is_dir():
            raise GcRefused(f"--runs-root не каталог: {runs_root}")
        for candidate in sorted(runs_root.rglob(RUN_DB_NAME)):
            found[_canonical(candidate)] = None
    return list(found)


def _run_id_of(path: Path) -> str:
    """``run_id`` прогонной БД — имя её каталога (``runs/{run_id}/cohound.db``)."""
    return path.parent.name


def _dir_bytes(path: Path) -> int:
    """Суммарный размер файлов под каталогом (0, если каталога нет)."""
    total = 0
    if not path.is_dir():
        return 0
    for child in path.rglob("*"):
        if child.is_file():
            total += _size_of(child)
    return total


def _count_files(path: Path) -> int:
    """Число файлов под каталогом (0, если каталога нет)."""
    if not path.is_dir():
        return 0
    return sum(1 for child in path.rglob("*") if child.is_file())


def _read_run_db(path: Path) -> tuple[RunDb, dict[Path, None], set[Path]]:
    """Прочитать одну прогонную БД: ссылки на байты, живость, возобновляемые сеансы.

    Таблиц может не быть (старая схема, недописанный прогон) — отсутствующая таблица
    значит «ссылок этого вида нет», а не ошибку. Файл читается ``mode=ro``.
    """
    run_id = _run_id_of(path)
    refs: dict[Path, None] = {}
    resumable: set[Path] = set()
    g4 = False
    cells = 0
    sessions = 0
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.Error:
        # Нечитаемая прогонная БД не доказывает ни живости, ни её отсутствия: считаем
        # прогон мёртвым и говорим об этом числом (live=False, 0 сеансов).
        return (
            RunDb(
                path=path,
                run_id=run_id,
                live=False,
                resumable_sessions=0,
                bytes=_dir_bytes(path.parent),
                outputs=_count_files(path.parent / "outputs"),
            ),
            refs,
            resumable,
        )
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
            g4 = bool(
                conn.execute(
                    "SELECT 1 FROM gate_decisions WHERE gate = 'G4' "
                    "AND verdict = 'approved' LIMIT 1"
                ).fetchone()
            )
        if "cells" in tables:
            try:
                cells = int(conn.execute("SELECT count(*) FROM cells").fetchone()[0])
            except sqlite3.Error:
                cells = 0
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
    return (
        RunDb(
            path=path,
            run_id=run_id,
            live=g4 or bool(cells),
            resumable_sessions=sessions,
            g4=g4,
            cells=cells,
            outputs=_count_files(path.parent / "outputs"),
            bytes=_dir_bytes(path.parent),
        ),
        refs,
        resumable,
    )


def _read_docs(docs_root: Path | None) -> list[tuple[str, str]]:
    """Документы цензуса: ``(относительный путь, текст)``; отсутствующий корень — пусто.

    Обычное текстовое чтение, а не grep: цензус обязан работать там, где внешней утилиты
    нет, и быть воспроизводимым без окружения.
    """
    if docs_root is None or not docs_root.is_dir():
        return []
    documents: list[tuple[str, str]] = []
    for path in sorted(docs_root.rglob("*.md")):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        try:
            relative = str(path.relative_to(docs_root))
        except ValueError:  # pragma: no cover - rglob всегда внутри корня
            relative = str(path)
        documents.append((relative, text))
    return documents


def _citation_keys(run_id: str) -> tuple[str, ...]:
    """Подстроки, которыми прогон цитируется в документах.

    Полное ``run_id`` — не единственная форма: в замерах прогон почти всегда называют
    **коротким хешем** (``9c8dd0345f3f``), потому что префикс пакета одинаков во всех
    строках таблицы. Замер 28.11.2026: прогон, из которого измерен потолок согласия 0.9381,
    встречается в четырёх документах только в короткой форме, и цензус по полному
    ``run_id`` объявлял его нецитируемым. Это тот самый прогон, ради которого цензус и
    заведён, поэтому форма обязана быть частью правила, а не догадкой читателя.

    Хвост берётся только если он похож на хеш прогона (12 hex): имя вида
    ``{пакет}-{роль}`` хеша не несёт, и его короткая форма совпала бы со словом.
    """
    parts = run_id.rsplit("-", 1)
    if len(parts) == 2 and _RUN_HASH_RE.fullmatch(parts[1]):
        return (run_id, parts[1])
    return (run_id,)


def _citations(
    documents: list[tuple[str, str]], run_ids: list[str]
) -> dict[str, tuple[str, ...]]:
    """``run_id`` → отсортированные относительные пути документов, где он встречается.

    Подстрочное вхождение, а не регулярка: ``run_id`` — имя каталога прогона, и его
    упоминание в тексте и есть цитирование (замер 28.11.2026: 12 из 19 незанятых
    прогонов цитируются, 3.18 GiB из 3.19 — подложка опубликованных замеров).
    Принимается и полная, и короткая форма (:func:`_citation_keys`).
    """
    found: dict[str, tuple[str, ...]] = {}
    for run_id in run_ids:
        keys = _citation_keys(run_id)
        found[run_id] = tuple(
            relative
            for relative, text in documents
            if any(key in text for key in keys)
        )
    return found


def _load_runs(
    runs_dirs: list[Path] | None,
    runs_roots: list[Path] | None,
    *,
    docs_root: Path | None = None,
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
    located = _find_run_dbs(runs_dirs, runs_roots)
    documents = _read_docs(docs_root)
    citations = _citations(documents, [_run_id_of(path) for path in located])
    for path in located:
        db, refs, resumable = _read_run_db(path)
        db = replace(db, cited_in=citations.get(db.run_id, ()))
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
        raise GcRefused(
            f"{package}: нет sources/corpus_urls.yaml — это не пакет, пространство "
            "объявить нечем"
        )
    return declared_scopes(package)


def _key_mtime(files: list[Path]) -> float:
    """Время последней записи пары ``{key}.bin`` + ``{key}.meta.json``.

    Сайдкар пишется после байтов, поэтому ``max`` — это «когда пара была записана».
    Отсутствующий файл (уже удалён) в расчёт не идёт; пара без единого файла — ``0.0``
    (самая старая), чтобы порядок был определён и без статистики.
    """
    stamps: list[float] = [
        stamp for path in files if (stamp := _stat_mtime(path)) is not None
    ]
    return max(stamps) if stamps else 0.0


def _stat_mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def _scan_http_cache(
    *,
    cache: Path,
    connections: list[sqlite3.Connection],
    spaces: list[tuple[dict[str, list[str]], dict[str, frozenset[str]]]],
    products: set[str],
    wanted: frozenset[str],
    budget: int | None = None,
) -> tuple[list[Orphan], dict[str, int], dict[str, int] | None]:
    """Ключи кэша вне пространства **всех** пакетов набора, ключи без атрибуции и вытеснение.

    ``cache/http`` — глобальный каталог раскладки, общий для всех пакетов. Поэтому ключ
    удаляется, только если **ни один** атрибутированный адрес не входит в пространство
    **ни одного** пакета набора (``spaces[0]`` — основной пакет, дальше
    ``--also-package``). Иначе прогон по одному профилю удалял бы байты, объявленные
    соседним.

    Атрибуция — через ``url_state.normalized_url`` **продуктов набора**: ключ, чей адрес
    лежит в строке чужого продукта, считается неатрибутированным.

    **Вытеснение по бюджету** (``cache.http_max_bytes``) считается **после** классов
    пространства: файл, который иначе удалялся бы как «вне пространства», в бюджетном
    классе уже не участвует (его и так нет). Ключи отчётных классов (атрибуция
    неизвестна) бюджетом **не** вытесняются, но занимают место в сумме — решение о них не
    принимается ни по пространству, ни по возрасту.

    Возвращает записи, счёт «спасённых набором» и **что останется в каталоге после
    вытеснения** (``None`` — бюджет не объявлен либо класс не запрошен).

    ``mtime`` здесь — «последняя **запись**», а не «последнее **использование**»: попадание
    в кэш ``mtime`` не обновляет. Поэтому это граница РОСТА, а не уборка — та же оговорка,
    что у ``extract.cache_max_bytes``.

    ``cache/http`` — глобальный каталог раскладки, общий для всех пакетов. Поэтому ключ
    удаляется, только если **ни один** атрибутированный адрес не входит в пространство
    **ни одного** пакета набора (``spaces[0]`` — основной пакет, дальше
    ``--also-package``). Иначе прогон по одному профилю удалял бы байты, объявленные
    соседним.

    Атрибуция — через ``url_state.normalized_url`` **продуктов набора**: ключ, чей адрес
    лежит в строке чужого продукта, считается неатрибутированным.

    Возвращает записи и счёт «спасённых набором» — файлов, которые остались бы удалены при
    одном основном пакете. Это измеренная цена правила, а не украшение отчёта.
    """
    key_files = _cache_key_files(cache / "http")
    if not key_files:
        return [], {"files": 0, "bytes": 0}, (
            {"files": 0, "bytes": 0}
            if budget is not None and "http_cache_over_budget" in wanted
            else None
        )

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
    builders: list[_SpaceLookup] = [
        _SpaceLookup(roots, scope_hosts) for roots, scope_hosts in spaces
    ]

    orphans: list[Orphan] = []
    rescued = {"files": 0, "bytes": 0}
    #: Ключи, чьи файлы уже назначены удалению как «вне пространства».
    outside: set[str] = set()
    #: Ключи отчётных классов: бюджетом не вытесняются (решение о них не принимается).
    report_only: set[str] = set()
    for key, files in sorted(key_files.items()):
        rows = attributed.get(key)
        if not rows:
            report_only.add(key)
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
        outside.add(key)
        orphans.extend(
            Orphan("http_cache_outside_space", f, _size_of(f)) for f in files
        )

    if budget is None or "http_cache_over_budget" not in wanted:
        return orphans, rescued, None

    # Сумма — по тому, что физически останется: удалённое «вне пространства» не считается.
    remaining = {
        key: files for key, files in key_files.items() if key not in outside
    }
    total = sum(_size_of(path) for files in remaining.values() for path in files)
    evictable = [
        (key, files) for key, files in remaining.items() if key not in report_only
    ]
    evictable.sort(key=lambda item: (_key_mtime(item[1]), item[0]))
    evicted_files = 0
    evicted_bytes = 0
    for _key, files in evictable:
        if total <= budget:
            break
        for path in files:
            size = _size_of(path)
            orphans.append(Orphan("http_cache_over_budget", path, size))
            total -= size
            evicted_files += 1
            evicted_bytes += size
    after_files = sum(len(files) for files in remaining.values()) - evicted_files
    return (
        orphans,
        rescued,
        {"files": after_files, "bytes": total, "evicted_files": evicted_files, "evicted_bytes": evicted_bytes, "budget": budget},
    )


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
        return in_space(url, built, self._host_caches[product_id])


def _admits_any(builders: list[_SpaceLookup], pid: str, url: str) -> bool:
    """Допускает ли адрес хоть один пакет, **объявляющий** этот продукт.

    Ни один пакет набора продукта не объявляет — «вне пространства» доказать нечем, и
    адрес считается допущенным (консервативно: файл остаётся).
    """
    opinions = [builder.admits(pid, url) for builder in builders if builder.knows(pid)]
    return any(opinions) if opinions else True


# ------------------------------------------------------------------ кэши и прогоны


def _cache_budgets(
    *,
    extract_cache: Path | None,
    embed_cache: Path | None,
    eval_cache: Path | None,
    http_cache: Path | None,
    extract_cache_budget: int | None,
    http_cache_budget: int | None,
) -> list[CacheBudget]:
    """Объём кэшей на диске против объявленных бюджетов.

    Бюджет объявлен у ``extract_cache`` (``deploy.extract.cache_max_bytes``, D8) и у
    ``http_cache`` (``deploy.cache.http_max_bytes``); у ``embed_cache``/``eval_cache`` его
    нет — ``None`` значит «без бюджета», а не «0».
    """
    rows: list[CacheBudget] = []
    for name, path, budget in (
        ("extract_cache", extract_cache, extract_cache_budget),
        ("http_cache", http_cache, http_cache_budget),
        ("embed_cache", embed_cache, None),
        ("eval_cache", eval_cache, None),
    ):
        if path is None:
            continue
        rows.append(
            CacheBudget(name=name, path=path, bytes=_dir_bytes(path), budget=budget)
        )
    return rows


def _resolve_prune_runs(census: RunCensus, run_ids: list[str]) -> tuple[list[PruneRun], list[str]]:
    """Проверить явно названные прогоны: отказ на подписанном/возобновляемом/неизвестном.

    Прогон с ячейками удалять **можно** (это решение человека), но молчать об этом нельзя:
    предупреждение собирается здесь и печатается до удаления.
    """
    by_id = {db.run_id: db for db in census.dbs}
    chosen: list[PruneRun] = []
    warnings: list[str] = []
    seen: set[str] = set()
    for run_id in run_ids:
        if run_id in seen:
            continue
        seen.add(run_id)
        db = by_id.get(run_id)
        if db is None:
            raise GcRefused(
                f"--prune-run {run_id}: прогон не найден среди прочитанных БД "
                f"({len(census.dbs)} шт.); каталог назван неверно или не передан "
                "--runs-dir/--runs-root"
            )
        if db.g4:
            raise GcRefused(
                f"--prune-run {run_id}: G4 подписан — это опубликованное evidence, "
                "удаление запрещено"
            )
        if db.resumable_sessions:
            raise GcRefused(
                f"--prune-run {run_id}: возобновляемый сеанс "
                f"({db.resumable_sessions}) — его байты ещё переигрываются без сети"
            )
        if db.cells:
            warnings.append(
                f"ВНИМАНИЕ: {run_id}: {db.cells} ячеек и {db.outputs} файлов outputs/ — "
                "это произведённые вердикты, восстановить их без прогона нельзя"
            )
        if db.cited_in:
            shown = ", ".join(db.cited_in[:5])
            warnings.append(
                f"ВНИМАНИЕ: {run_id} цитируется в {len(db.cited_in)} документах — "
                "удаление уничтожит подложку опубликованного замера: "
                f"{shown}"
            )
        chosen.append(PruneRun(db=db))
    return chosen, warnings


# ------------------------------------------------------------------ план


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
        raise GcRefused(
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
        raise GcRefused(
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
    #: Объём кэшей против объявленных бюджетов (отчёт).
    cache_budgets: list[CacheBudget] = field(default_factory=list)
    #: Прогоны, названные явно для удаления (``--prune-run``).
    prune_runs: list[PruneRun] = field(default_factory=list)
    #: Громкие предупреждения о названных прогонах (печатаются до удаления).
    prune_warnings: list[str] = field(default_factory=list)
    #: Что останется в ``cache/http`` после вытеснения по бюджету (``None`` — бюджет не
    #: объявлен либо класс не запрошен): ``{files, bytes, evicted_files, evicted_bytes,
    #: budget}``.
    http_cache_after: dict[str, int] | None = None

    @property
    def price(self) -> RunPrice:
        """Цена удаления (нули, если прогонные БД не передавались)."""
        return self.runs.price or RunPrice({}, {}, {}, {})

    def runs_by_class(self) -> dict[str, list[RunDb]]:
        """Прогоны по отчётным классам."""
        return self.runs.by_class()


def build_gc_plan(
    *,
    package: Path,
    also_packages: list[Path] | None = None,
    state_dbs: list[Path] | None = None,
    state_root: Path | None = None,
    runs_dirs: list[Path] | None = None,
    runs_roots: list[Path] | None = None,
    corpus_raw: Path,
    cache: Path,
    extract_cache: Path | None = None,
    embed_cache: Path | None = None,
    eval_cache: Path | None = None,
    extract_cache_budget: int | None = None,
    http_cache_budget: int | None = None,
    docs_root: Path | None = None,
    prune_runs: list[str] | None = None,
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
    :param corpus_raw: корень сырых байтов.
    :param cache: корень ``cache`` (http-кэш — ``{cache}/http``).
    :param extract_cache: каталог кэша экстракции (отчёт ``cache_budget``).
    :param embed_cache: каталог кэша векторов (отчёт ``cache_budget``).
    :param eval_cache: каталог кэша вердиктов (отчёт ``cache_budget``).
    :param extract_cache_budget: объявленный бюджет ``extract_cache`` (``None`` — нет).
    :param http_cache_budget: объявленный бюджет ``{cache}/http`` (``None`` — нет
        вытеснения; класс ``http_cache_over_budget`` тогда пуст).
    :param docs_root: корень документов для цензуса цитирования прогонов (``None`` —
        ``docs/`` корня репозитория, определённого от ``__file__``, а не от CWD).
    :param prune_runs: ``run_id``, названные явно для удаления (``--prune-run``).
    :param only: имена классов из :data:`ALL_CLASSES`; ``None`` — все.
    :raises GcRefused: условие, при котором удаление было бы ложным.
    """
    wanted = frozenset(ALL_CLASSES if only is None else only)
    unknown = wanted - set(ALL_CLASSES)
    if unknown:
        raise GcRefused(
            f"неизвестные классы: {', '.join(sorted(unknown))}; известны: "
            f"{', '.join(ALL_CLASSES)}"
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
    runs = _load_runs(runs_dirs, runs_roots, docs_root=docs_root)
    try:
        declared = set().union(*(_declared_products(item) for item in packages))
        products = declared | state.products
        if not products:
            raise GcRefused(
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
            raise GcRefused(
                f"document_version пуста во всех переданных БД ({len(state.dbs)}), "
                f"а в {corpus_raw} лежит {seen_bins} файлов .bin: живое множество "
                "неизвестно, удаление было бы удалением всего корпуса"
            )
        rescued = {"files": 0, "bytes": 0}
        http_after: dict[str, int] | None = None
        if wanted & set(ONLY_GROUPS["http-cache"]):
            found_cache, rescued, http_after = _scan_http_cache(
                cache=cache,
                connections=state.connections,
                spaces=spaces,
                products=products,
                wanted=wanted,
                budget=http_cache_budget,
            )
            orphans.extend(found_cache)
        uncovered: list[dict[str, Any]] = []
        for neighbour in _neighbour_state_dbs(paths):
            hit = _uncovered_products(neighbour, products)
            if hit:
                uncovered.append({"path": str(neighbour), "products": hit})
        budgets: list[CacheBudget] = []
        if CACHE_BUDGET_CLASS in wanted:
            budgets = _cache_budgets(
                extract_cache=extract_cache,
                embed_cache=embed_cache,
                eval_cache=eval_cache,
                http_cache=cache / "http",
                extract_cache_budget=extract_cache_budget,
                http_cache_budget=http_cache_budget,
            )
        chosen, prune_warnings = (
            _resolve_prune_runs(runs, list(prune_runs or []))
            if prune_runs
            else ([], [])
        )
        return Plan(
            orphans=orphans,
            state=state,
            runs=runs,
            uncovered_state_dbs=uncovered,
            packages=packages,
            rescued_by_other_packages=rescued,
            cache_budgets=budgets,
            prune_runs=chosen,
            prune_warnings=prune_warnings,
            http_cache_after=http_after,
        )
    finally:
        state.close()


def scan_gc(
    *,
    package: Path,
    state_db: Path | list[Path] | None = None,
    state_root: Path | None = None,
    runs_dir: Path | list[Path] | None = None,
    runs_root: Path | list[Path] | None = None,
    corpus_raw: Path,
    cache: Path,
    extract_cache: Path | None = None,
    embed_cache: Path | None = None,
    eval_cache: Path | None = None,
    extract_cache_budget: int | None = None,
    http_cache_budget: int | None = None,
    docs_root: Path | None = None,
    only: frozenset[str] | None = None,
) -> list[Orphan]:
    """План удаления без служебных полей — удобно тестам и приборам.

    Обёртка над :func:`build_gc_plan`: ``state_db`` принимает и один путь, и список.
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
    plan = build_gc_plan(
        package=package,
        state_dbs=dbs,
        state_root=state_root,
        runs_dirs=dirs,
        runs_roots=roots,
        corpus_raw=corpus_raw,
        cache=cache,
        extract_cache=extract_cache,
        embed_cache=embed_cache,
        eval_cache=eval_cache,
        extract_cache_budget=extract_cache_budget,
        http_cache_budget=http_cache_budget,
        docs_root=docs_root,
        only=only,
    )
    return plan.orphans


# ------------------------------------------------------------------ применение


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


def _delete_paths(paths: tuple[Path, ...] | list[Path]) -> list[tuple[Path, str]]:
    """Удалить перечисленные файлы; вернуть неудачи ``(путь, причина)``."""
    failures: list[tuple[Path, str]] = []
    for path in paths:
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        except OSError as exc:
            failures.append((path, str(exc)))
    return failures


def _delete_runs(runs: list[PruneRun]) -> list[tuple[Path, str]]:
    """Удалить каталоги названных прогонов; вернуть неудачи ``(путь, причина)``."""
    failures: list[tuple[Path, str]] = []
    for run in runs:
        try:
            shutil.rmtree(run.path)
        except FileNotFoundError:
            continue
        except OSError as exc:
            failures.append((run.path, str(exc)))
    return failures


def http_cache_files(cache: Path) -> list[Path]:
    """Файлы ``{cache}/http`` (``.bin`` + ``.meta.json``), в детерминированном порядке."""
    http_dir = cache / "http"
    if not http_dir.is_dir():
        return []
    return [
        path
        for path in sorted(http_dir.iterdir())
        if path.is_file()
        and (path.name.endswith(".bin") or path.name.endswith(".meta.json"))
    ]


@dataclass(frozen=True)
class WipePlan:
    """Что даст ``--wipe-http-cache``: каталог, файлы и их сумма."""

    path: Path
    files: tuple[Path, ...]

    @property
    def bytes(self) -> int:
        return sum(_size_of(path) for path in self.files)

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "files": len(self.files),
            "bytes": self.bytes,
        }


def plan_wipe_http_cache(cache: Path) -> WipePlan:
    """План полной очистки ``cache/http`` (dry-run считает, ``--apply`` удаляет)."""
    return WipePlan(path=cache / "http", files=tuple(http_cache_files(cache)))


@dataclass
class ApplyResult:
    """Итог применения: что удалено, что не удалось, куда записан аудит."""

    deleted: list[Orphan]
    pruned: list[PruneRun]
    failures: list[tuple[Path, str]]
    audit_path: Path
    wiped: list[Path] = field(default_factory=list)
    #: Байты, освобождённые полной очисткой кэша: файлы уже удалены, и ``stat`` по ним
    #: больше не ответит — сумма снимается до удаления.
    wiped_bytes: int = 0


def apply_gc(
    plan: Plan,
    *,
    actor: str,
    audit: Path,
    package: Path,
    totals: dict[str, dict[str, int]],
    wipe: WipePlan | None = None,
) -> ApplyResult:
    """Удалить файлы удаляемых классов, названные прогоны и (по флагу) весь ``cache/http``.

    Порядок — файлы, затем прогоны, затем полная очистка кэша: если удаление файла упадёт,
    прогон всё равно уйдёт (решение о нём принято человеком явно), а неудачи называются в
    отчёте. Полная очистка идёт последней, чтобы её счёт не смешивался с пофайловым.
    """
    to_delete = [item for item in plan.orphans if item.cls in DELETABLE_CLASSES]
    failures = _delete(to_delete)
    failed = {path for path, _ in failures}
    deleted = [item for item in to_delete if item.path not in failed]
    run_failures = _delete_runs(plan.prune_runs)
    failed_runs = {path for path, _ in run_failures}
    pruned = [item for item in plan.prune_runs if item.path not in failed_runs]
    failures.extend(run_failures)
    wiped: list[Path] = []
    wiped_bytes = 0
    if wipe is not None:
        # Сумма снимается ДО удаления: после него ``stat`` по этим путям уже не ответит,
        # и «сколько освободили» пришлось бы считать по плану, включая неудачные файлы.
        sizes = {path: _size_of(path) for path in wipe.files}
        wipe_failures = _delete_paths(wipe.files)
        wiped_failed = {path for path, _ in wipe_failures}
        wiped = [path for path in wipe.files if path not in wiped_failed]
        wiped_bytes = sum(sizes.get(path, 0) for path in wiped)
        failures.extend(wipe_failures)
    audit_path = audit
    if deleted or pruned or wiped:
        _write_audit(
            audit=audit_path,
            actor=actor,
            package=package,
            state_dbs=[str(db.path) for db in plan.state.dbs],
            runs=plan.runs,
            totals=totals,
            deleted=deleted,
            pruned=pruned,
            wiped=wipe if wipe is not None else None,
        )
    return ApplyResult(
        deleted=deleted,
        pruned=pruned,
        failures=failures,
        audit_path=audit_path,
        wiped=wiped,
        wiped_bytes=wiped_bytes,
    )


def _write_audit(
    *,
    audit: Path,
    actor: str,
    package: Path,
    state_dbs: list[str],
    runs: RunCensus,
    totals: dict[str, dict[str, int]],
    deleted: list[Orphan],
    pruned: list[PruneRun] | None = None,
    wiped: WipePlan | None = None,
) -> None:
    """Дописать в аудит строку-сводку, по строке на файл и на удалённый прогон.

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
        "runs_pruned": [item.as_dict() for item in (pruned or [])],
        "wiped": wiped.as_dict() if wiped is not None else None,
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
        for run in pruned or []:
            handle.write(
                json.dumps(
                    {
                        "class": "prune_run",
                        "run_id": run.run_id,
                        "path": str(run.path),
                        "bytes": run.db.bytes,
                        "cells": run.db.cells,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
        if wiped is not None:
            handle.write(
                json.dumps(
                    {
                        "class": "wipe_http_cache",
                        "path": str(wiped.path),
                        "files": len(wiped.files),
                        "bytes": wiped.bytes,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def aggregate(orphans: list[Orphan]) -> dict[str, dict[str, int]]:
    """Класс → ``{files, bytes}`` (все файловые классы, включая нулевые)."""
    totals: dict[str, dict[str, int]] = {
        cls: {"files": 0, "bytes": 0} for cls in CLASS_ORDER
    }
    for orphan in orphans:
        bucket = totals[orphan.cls]
        bucket["files"] += 1
        bucket["bytes"] += orphan.size
    return totals


def total(classes: dict[str, dict[str, int]], names: tuple[str, ...]) -> dict[str, int]:
    """Сумма ``{files, bytes}`` по названным классам."""
    return {
        "files": sum(classes[name]["files"] for name in names),
        "bytes": sum(classes[name]["bytes"] for name in names),
    }


def _sum(classes: dict[str, dict[str, int]], names: tuple[str, ...]) -> dict[str, int]:
    """Внутренний алиас :func:`total` (имя сохранено из прежнего прибора)."""
    return total(classes, names)
