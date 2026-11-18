"""coHound — читатель корпуса прогона: строка ``runs`` и чанки (только чтение).

**Зачем он есть.** ``hound search`` отвечает по **уже собранному** корпусу прогона, а не
пересобирает его. На стенде девять прогонов объявляют ``schema_version`` 3.20.0 при
``SCHEMA_VERSION`` 3.26.0, и общий гейт версии
(:class:`core.util.schema_gate.SchemaVersionMismatch`) отвергает такие файлы целиком — и
отвергает правильно: писать в чужую схему и «догонять» её ``ALTER TABLE`` при открытии
запрещено (решение владельца 01.11.2026). Владелец разрешил (18.11.2026) **узкое**
исключение: читать корпус старой БД, если оператор объявил это явно.

**Что читатель отказывается делать.** Он не пишет (соединение ``mode=ro``), не мигрирует
схему, не создаёт файл, не отдаёт вердикты и ячейки (``cells``): карта прогона — не
корпус, и её чтение подчиняется своим правилам (I1–I23). Здесь ровно два факта: **строка
прогона** (какими условиями он объявлен) и **корпус** (чанки продукт-партиции).

**``allow_foreign_schema`` — решение владельца, а не поблажка.** По умолчанию гейт
сохраняется: файл чужой версии отвергается :class:`SchemaVersionMismatch` до любого
чтения данных. Разрешение объявляет вызывающий (CLI) и он же отвечает за то, что увидит
оператор; сам читатель ничего не печатает в stderr и не превращает несовпадение в
предупреждение — несовпадение видно полем :attr:`CorpusReader.mismatch`.

**Терпимость к колонкам — явная, а не «на всякий случай».** Версии 3.20.0…3.26.0
отличаются аддитивно (``table_ref``, ``page_map``, ``ocr_probe``…). Отсутствие колонки в
чужой схеме — **не** ошибка чтения: имена колонок читаются один раз из
``PRAGMA table_info``, и :class:`core.ports.types.Chunk` собирается только из
существующих. ``NULL`` при этом сохраняет смысл и не подменяется нулём: ``char_start``/
``char_end`` ``NULL`` → ``None`` («кусок не является точным срезом»), ``table_ref``
``NULL`` **или отсутствующая колонка** → ``None`` («адрес неизвестен» — ровно как
документировано у :attr:`core.ports.types.Chunk.table_ref`).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from core.domain.doc_tables import table_ref_from_json
from core.ports.types import Chunk
from core.store.sqlite import SCHEMA_VERSION
from core.util.schema_gate import (
    SchemaVersionMismatch,
    declared_schema_version,
    schema_mismatch_message,
)

__all__ = [
    "CorpusReader",
    "CorpusReaderError",
    "PartitionRef",
    "RunCorpusRef",
]

#: Сколько ``?`` ставится в один ``IN (...)``. Потолок параметров у SQLite есть (в старых
#: сборках — 999), а список цитат приходит из отчёта целиком; поэтому батч, а не «сколько
#: получится».
_PARAM_BATCH = 500

#: Таблицы, которые читатель вообще открывает. Список закрыт: имя таблицы подставляется в
#: SQL текстом (``PRAGMA table_info`` параметров не принимает), поэтому подстановка
#: проверяется по нему, а не по вводу вызывающего.
_KNOWN_TABLES = frozenset({"corpus_chunks", "runs", "index_manifest"})

#: Колонки чанка, которые читатель умеет отдать. Порядок не важен, важно, что список
#: **закрыт**: колонка, которой нет в схеме файла, просто не попадает в ``SELECT``.
_CHUNK_FIELDS = (
    "chunk_id",
    "doc_id",
    "product_id",
    "url",
    "heading_path",
    "lang",
    "chunk_no",
    "text",
    "context_prefix",
    "content_hash",
    "char_start",
    "char_end",
    "table_ref",
    "build_id",
)

#: Поля метаданных чанка, которые отдаёт :meth:`CorpusReader.chunk_meta`.
_META_FIELDS = ("url", "lang", "heading_path", "product_id")

#: Поля строки прогона, которые отдаёт :meth:`CorpusReader.run_ref`.
_RUN_FIELDS = (
    "run_id",
    "package_id",
    "index_build_id",
    "retrieval_profile",
    "retrieval_cutoff_K",
    "embed_model_id",
)

#: Поля продукт-партиции, которые отдаёт :meth:`CorpusReader.partitions`.
_PARTITION_FIELDS = (
    "product_id",
    "partition_build_id",
    "embed_model_id",
    "embed_dim",
)


class CorpusReaderError(ValueError):
    """Отказ читателя корпуса: файла нет, таблиц нет, прогон не разрешён.

    Отдельный класс, а не общий ``ValueError``: CLI обязан отличить «такого прогона
    нет / файл не тот» (``EXIT_NOT_FOUND``/``EXIT_USAGE``) от сбоя чтения данных. Гейт
    версии сюда **не** входит: чужой файл отвергается
    :class:`SchemaVersionMismatch` — это другой класс и другой код.
    """


@dataclass(frozen=True)
class RunCorpusRef:
    """Строка ``runs``: какими условиями объявлен прогон, чей корпус читается.

    Это **объявление**, а не измерение: ``retrieval_profile``/``retrieval_cutoff_k`` —
    условия, при которых корпус индексировался и искался (I16/I20), и по ним читатель
    корпуса не решает ничего. ``schema_version`` — версия, **объявленная файлом**;
    ``None`` — файл не объявляет её вовсе (тогда чтение без явного разрешения запрещено).
    """

    run_id: str
    package_id: str | None
    index_build_id: str | None
    retrieval_profile: str | None
    retrieval_cutoff_k: int | None
    embed_model_id: str | None
    schema_version: str | None


@dataclass(frozen=True)
class PartitionRef:
    """Продукт-партиция сборки: чем продукт индексирован (I17).

    ``partition_build_id`` — сборка **продукта**, а не композиция когорты
    (``index_build_id``): чанки фильтруются именно по ней, поэтому пустая строка здесь
    означала бы «неизвестно, чей это корпус», и такая строка пропускается.
    """

    product_id: str
    partition_build_id: str
    embed_model_id: str | None
    embed_dim: int | None


class CorpusReader:
    """Чтение корпуса прогона и его строки ``runs``; файл не меняется.

    Открывается **существующий** файл в ``mode=ro``: ни каталога, ни схемы, ни ``-wal``
    читатель не создаёт. Гейт версии — тот же, что у
    :func:`core.store.sqlite.connect_read_only`: файл чужой ``schema_version`` отвергается
    :class:`SchemaVersionMismatch`, если вызывающий не объявил ``allow_foreign_schema``.

    :param db_path: путь к ``runs/{run_id}/cohound.db``.
    :param run_id: какой прогон описывает файл; ``None`` — файл обязан содержать ровно
        один прогон (иначе :class:`CorpusReaderError` с числом найденных).
    :param allow_foreign_schema: читать файл, объявляющий другую версию схемы. Решение
        владельца 18.11.2026; по умолчанию **выключено**.
    :raises CorpusReaderError: файла нет, таблицы нет, прогон не выбран однозначно.
    :raises SchemaVersionMismatch: файл объявляет другую ``schema_version``.
    """

    #: Версия схемы, которую ожидает код (:data:`core.store.sqlite.SCHEMA_VERSION`).
    expected_schema_version: str = SCHEMA_VERSION

    #: Версия, объявленная файлом; ``None`` — ``schema_meta`` нет.
    schema_version: str | None

    #: ``True``, если объявленная версия не совпала с :attr:`expected_schema_version`.
    mismatch: bool

    #: Соединение только на чтение (``row_factory = sqlite3.Row``).
    conn: sqlite3.Connection

    def __init__(
        self,
        db_path: str | Path,
        *,
        run_id: str | None = None,
        allow_foreign_schema: bool = False,
    ) -> None:
        path = Path(db_path)
        # Читатель не создаёт файл: пустая схема неотличима от прогона, который ничего не
        # нашёл, и от опечатки в пути (то же правило, что у ``connect_read_only``).
        if not path.exists():
            raise CorpusReaderError(
                f"БД прогона не найдена: {path} — читатель корпуса файл не создаёт"
            )
        conn = sqlite3.connect(f"file:{path.absolute().as_posix()}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            # ``busy_timeout`` — ожидание чужой блокировки, не запись: живой писатель держит
            # WAL недолго, и без таймаута читатель получал бы мгновенный ``database is locked``.
            conn.execute("PRAGMA busy_timeout = 5000")
            self.conn = conn
            self._path = path
            self._run_id = run_id
            has_tables, found = declared_schema_version(conn)
            self.schema_version = found
            self.mismatch = found != SCHEMA_VERSION
            # Гейт тот же, что у ``connect_read_only``: пустой файл версии не объявляет и
            # отвергается не гейтом, а отсутствием таблицы ниже — с более внятным текстом.
            if (
                self.mismatch
                and not allow_foreign_schema
                and not (found is None and not has_tables)
            ):
                raise SchemaVersionMismatch(
                    schema_mismatch_message(
                        path,
                        found=found,
                        expected=SCHEMA_VERSION,
                        what="11 §2.6",
                        action=(
                            "если корпус этой версии нужен только для чтения, разрешите "
                            "это явно (allow_foreign_schema), либо пересоберите прогон."                        ),
                    )
                )
            self._tables = frozenset(
                str(row["name"])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                    " AND name NOT LIKE 'sqlite_%'"
                )
            )
            if "corpus_chunks" not in self._tables:
                raise CorpusReaderError(
                    f"в БД {path} нет таблицы corpus_chunks — это не БД прогона coHound"
                )
            self._columns_cache: dict[str, frozenset[str]] = {}
            self._chunk_columns = self._column_names("corpus_chunks")
            if "chunk_id" not in self._chunk_columns:
                raise CorpusReaderError(
                    f"в БД {path} таблица corpus_chunks без колонки chunk_id — "
                    "чанк без адреса прочитать нельзя"
                )
            self._chunk_select = self._select("corpus_chunks", _CHUNK_FIELDS)
        except BaseException:
            conn.close()
            raise

    # ------------------------------------------------------------------ схема файла

    def _column_names(self, table: str) -> frozenset[str]:
        """Имена колонок таблицы в нижнем регистре (SQLite к регистру нечувствителен).

        Читается один раз на таблицу и кэшируется: ``PRAGMA`` на каждый запрос был бы
        платой за то, что уже известно, а набор колонок за время жизни читателя не меняется
        (соединение только на чтение).
        """
        if table not in _KNOWN_TABLES:
            raise CorpusReaderError(f"таблица {table!r} не читается читателем корпуса")
        cached = self._columns_cache.get(table)
        if cached is None:
            cached = frozenset(
                str(row["name"]).lower()
                for row in self.conn.execute(f"PRAGMA table_info({table})")
            )
            self._columns_cache[table] = cached
        return cached

    def _select(self, table: str, wanted: Sequence[str]) -> str:
        """Список колонок для ``SELECT``: только существующие, имена приведены к нижнему.

        Псевдоним ``AS`` делает регистр объявления неважным для чтения строки:
        ``retrieval_cutoff_K`` приходит ключом ``retrieval_cutoff_k``.
        """
        present = self._column_names(table)
        return ", ".join(
            f"{name} AS {name.lower()}" for name in wanted if name.lower() in present
        )

    def _text(self, row: sqlite3.Row, name: str) -> str:
        """Текстовое поле чанка: отсутствующая колонка и ``NULL`` читаются как ``""``."""
        if name not in self._chunk_columns:
            return ""
        value = row[name]
        return "" if value is None else str(value)

    def _number(self, row: sqlite3.Row, name: str) -> int | None:
        """Числовое поле чанка: отсутствующая колонка и ``NULL`` читаются как ``None``.

        Ноль здесь был бы подменой: ``char_start=0`` — валидное начало первого куска, и
        «смещение неизвестно» неотличимо от него стало бы молча неверным.
        """
        if name not in self._chunk_columns:
            return None
        value = row[name]
        return None if value is None else int(value)

    def _row_to_chunk(self, row: sqlite3.Row) -> Chunk:
        """Собирает :class:`core.ports.types.Chunk` только из существующих колонок."""
        chunk_no = self._number(row, "chunk_no")
        return Chunk(
            chunk_id=self._text(row, "chunk_id"),
            chunk_no=0 if chunk_no is None else chunk_no,
            text=self._text(row, "text"),
            heading_path=self._text(row, "heading_path"),
            context_prefix=self._text(row, "context_prefix"),
            content_hash=self._text(row, "content_hash"),
            char_start=self._number(row, "char_start"),
            char_end=self._number(row, "char_end"),
            # Колонки ``table_ref`` в схеме 3.20.0 нет вовсе: адрес неизвестен, и это тот
            # же смысл, что у ``NULL`` (см. ``Chunk.table_ref``).
            table_ref=(
                table_ref_from_json(row["table_ref"])
                if "table_ref" in self._chunk_columns
                else None
            ),
        )

    # ------------------------------------------------------------------ строка прогона

    def run_ref(self) -> RunCorpusRef:
        """Строка ``runs`` выбранного прогона.

        ``run_id`` не задан — в файле обязан быть **ровно один** прогон: ноль или
        несколько означают, что файл описывает не один корпус, и выбирать за оператора
        (первый по порядку) значило бы отдать корпус другого прогона.

        :raises CorpusReaderError: таблицы ``runs`` нет, прогонов не один, ``run_id``
            не найден.
        """
        if "runs" not in self._tables:
            raise CorpusReaderError(f"в БД {self._path} нет таблицы runs")
        columns = self._column_names("runs")
        if "run_id" not in columns:
            raise CorpusReaderError(
                f"в БД {self._path} таблица runs без колонки run_id"
            )
        select = self._select("runs", _RUN_FIELDS)
        if self._run_id is None:
            total = self.conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()
            count = 0 if total is None else int(total["n"])
            if count != 1:
                raise CorpusReaderError(
                    f"в БД {self._path} {count} прогонов; укажите run_id, чтобы выбрать один"
                )
            row = self.conn.execute(
                f"SELECT {select} FROM runs"  # noqa: S608 - проекция из _RUN_FIELDS
            ).fetchone()
        else:
            row = self.conn.execute(
                f"SELECT {select} FROM runs WHERE run_id = ?",  # noqa: S608 - проекция из _RUN_FIELDS, run_id — bind
                (self._run_id,),
            ).fetchone()
            if row is None:
                raise CorpusReaderError(
                    f"в БД {self._path} нет прогона {self._run_id!r}"
                )
        if row is None:
            raise CorpusReaderError(
                f"в БД {self._path} не удалось прочитать строку runs"
            )
        return RunCorpusRef(
            run_id=str(row["run_id"]),
            package_id=_optional_text(row, columns, "package_id"),
            index_build_id=_optional_text(row, columns, "index_build_id"),
            retrieval_profile=_optional_text(row, columns, "retrieval_profile"),
            retrieval_cutoff_k=_optional_int(row, columns, "retrieval_cutoff_k"),
            embed_model_id=_optional_text(row, columns, "embed_model_id"),
            schema_version=self.schema_version,
        )

    # ------------------------------------------------------------------ партиции

    def partitions(self, build_id: str) -> list[PartitionRef]:
        """Продукт-партиции сборки ``build_id`` (``index_manifest.index_build_id``).

        Дубли по ``product_id`` снимает фильтр по сборке; если строка всё же повторилась,
        остаётся первая (порядок задан ``ORDER BY product_id``) — выбор виден, а не
        случаен. Строки без продукта или без сборки партиции пропускаются: «неизвестно,
        чей это корпус» — не партиция.
        """
        if "index_manifest" not in self._tables:
            raise CorpusReaderError(
                f"в БД {self._path} нет таблицы index_manifest — партиции неоткуда взять"
            )
        columns = self._column_names("index_manifest")
        for required in ("index_build_id", "product_id"):
            if required not in columns:
                raise CorpusReaderError(
                    f"в БД {self._path} таблица index_manifest без колонки {required}"
                )
        select = self._select("index_manifest", _PARTITION_FIELDS)
        rows = self.conn.execute(
            f"SELECT {select} FROM index_manifest WHERE index_build_id = ?"  # noqa: S608 - проекция из _PARTITION_FIELDS, build_id — bind
            " ORDER BY product_id",
            (build_id,),
        )
        found: list[PartitionRef] = []
        seen: set[str] = set()
        for row in rows:
            product_id = _optional_text(row, columns, "product_id") or ""
            partition_build_id = (
                _optional_text(row, columns, "partition_build_id") or ""
            )
            if not product_id or not partition_build_id or product_id in seen:
                continue
            seen.add(product_id)
            found.append(
                PartitionRef(
                    product_id=product_id,
                    partition_build_id=partition_build_id,
                    embed_model_id=_optional_text(row, columns, "embed_model_id"),
                    embed_dim=_optional_int(row, columns, "embed_dim"),
                )
            )
        return found

    # ------------------------------------------------------------------ чанки

    def _chunk_filter(
        self, build_id: str, product_id: str | None
    ) -> tuple[str, list[str]]:
        """``WHERE`` по продукт-партиции и (опционально) продукту.

        ``build_id`` — сборка **продукт-партиции** (``pb_*``), не композиция когорты:
        фильтр по композиции отдал бы чанки всех продуктов, а индекс продукт-специфичен
        (I17). Отсутствие колонки — отказ, а не «отдадим всё»: молча проигнорированный
        фильтр вернул бы чужой корпус под видом запрошенного.
        """
        if "build_id" not in self._chunk_columns:
            raise CorpusReaderError(
                f"в БД {self._path} таблица corpus_chunks без колонки build_id — "
                "корпус по партиции не фильтруется"
            )
        clauses = ["build_id = ?"]
        params = [build_id]
        if product_id is not None:
            if "product_id" not in self._chunk_columns:
                raise CorpusReaderError(
                    f"в БД {self._path} таблица corpus_chunks без колонки product_id — "
                    "продукт-фильтр невозможен"
                )
            clauses.append("product_id = ?")
            params.append(product_id)
        return " AND ".join(clauses), params

    def _chunk_order(self) -> str:
        """Детерминированный порядок: продукт → документ → номер куска → адрес.

        ``chunk_id`` последним ключом не украшение: при ``chunk_no IS NULL`` (или его
        повторе) без него порядок строк не определён, и два чтения одного файла давали бы
        разные последовательности.
        """
        names = [
            name
            for name in ("product_id", "doc_id", "chunk_no", "chunk_id")
            if name in self._chunk_columns
        ]
        return ", ".join(names)

    def iter_chunks(
        self, build_id: str, *, product_id: str | None = None
    ) -> Iterator[Chunk]:
        """Чанки продукт-партиции ``build_id`` в детерминированном порядке.

        Один запрос на всю последовательность: индекса по ``doc_id`` в схеме нет, поэтому
        обход «документ за документом» превратился бы в N полных сканов
        ``corpus_chunks``.

        :param build_id: сборка продукт-партиции (``pb_*``).
        :param product_id: ограничить одним продуктом.
        """
        where, params = self._chunk_filter(build_id, product_id)
        cursor = self.conn.execute(
            f"SELECT {self._chunk_select} FROM corpus_chunks WHERE {where}"  # noqa: S608 - проекция и WHERE из литералов, значения — bind
            f" ORDER BY {self._chunk_order()}",
            params,
        )
        return (self._row_to_chunk(row) for row in cursor)

    def count_chunks(self, build_id: str, *, product_id: str | None = None) -> int:
        """Сколько чанков в продукт-партиции (с тем же фильтром, что ``iter_chunks``)."""
        where, params = self._chunk_filter(build_id, product_id)
        row = self.conn.execute(
            f"SELECT COUNT(*) AS n FROM corpus_chunks WHERE {where}",  # noqa: S608 - WHERE из литералов, значения — bind
            params,
        ).fetchone()
        return 0 if row is None else int(row["n"])

    def get_chunk(self, chunk_id: str) -> Chunk | None:
        """Один чанк по адресу; ``None`` — такого чанка в файле нет."""
        row = self.conn.execute(
            f"SELECT {self._chunk_select} FROM corpus_chunks WHERE chunk_id = ?",  # noqa: S608 - проекция из _CHUNK_FIELDS, chunk_id — bind
            (chunk_id,),
        ).fetchone()
        return None if row is None else self._row_to_chunk(row)

    def get_chunks(self, chunk_ids: Sequence[str]) -> dict[str, Chunk]:
        """Чанки по списку адресов: только найденные, ключ — ``chunk_id``.

        Пачками по :data:`_PARAM_BATCH` параметров: список приходит из отчёта целиком, а
        потолок числа параметров у SQLite конечен. Отсутствующие адреса в результат не
        попадают — «чанка нет» и «чанк пустой» не должны слиться.
        """
        found: dict[str, Chunk] = {}
        # Повторы снимаются до батча: они не меняют ответ, но занимают параметры.
        unique = list(dict.fromkeys(chunk_ids))
        for start in range(0, len(unique), _PARAM_BATCH):
            batch = unique[start : start + _PARAM_BATCH]
            placeholders = ", ".join("?" for _ in batch)
            cursor = self.conn.execute(
                f"SELECT {self._chunk_select} FROM corpus_chunks"  # noqa: S608 - проекция из _CHUNK_FIELDS, адреса — bind
                f" WHERE chunk_id IN ({placeholders})",
                batch,
            )
            for row in cursor:
                chunk = self._row_to_chunk(row)
                found[chunk.chunk_id] = chunk
        return found

    def chunk_meta(self, chunk_ids: Sequence[str]) -> dict[str, dict[str, str]]:
        """Метаданные чанков: ``{chunk_id: {url, lang, heading_path, product_id}}``.

        Только существующие колонки: в чужой схеме ``lang``/``heading_path`` может не
        быть, и выдумывать ключ с пустым значением значило бы объявить «язык не записан»
        там, где его никто не объявлял. ``NULL`` существующей колонки читается как ``""``.
        """
        columns = self._column_names("corpus_chunks")
        fields = [name for name in _META_FIELDS if name in columns]
        select = ", ".join(["chunk_id AS chunk_id", *(f"{f} AS {f}" for f in fields)])
        meta: dict[str, dict[str, str]] = {}
        unique = list(dict.fromkeys(chunk_ids))
        for start in range(0, len(unique), _PARAM_BATCH):
            batch = unique[start : start + _PARAM_BATCH]
            placeholders = ", ".join("?" for _ in batch)
            cursor = self.conn.execute(
                f"SELECT {select} FROM corpus_chunks WHERE chunk_id IN ({placeholders})",  # noqa: S608 - проекция из _META_FIELDS, адреса — bind
                batch,
            )
            for row in cursor:
                meta[str(row["chunk_id"])] = {
                    name: ("" if row[name] is None else str(row[name]))
                    for name in fields
                }
        return meta

    # ------------------------------------------------------------------ ресурс

    def close(self) -> None:
        """Закрывает соединение. Повторный вызов безопасен (``sqlite3`` это допускает)."""
        self.conn.close()

    def __enter__(self) -> CorpusReader:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _optional_text(row: sqlite3.Row, columns: frozenset[str], name: str) -> str | None:
    """Текстовое поле строки: отсутствующая колонка и ``NULL`` → ``None``.

    Не ``""``: пустая строка — законное значение (например, пустой ``package_id`` в
    чужой записи), и свести её с «колонки нет» значило бы потерять различие.
    """
    if name.lower() not in columns:
        return None
    value = row[name]
    return None if value is None else str(value)


def _optional_int(row: sqlite3.Row, columns: frozenset[str], name: str) -> int | None:
    """Числовое поле строки: отсутствующая колонка и ``NULL`` → ``None``."""
    if name.lower() not in columns:
        return None
    value = row[name]
    return None if value is None else int(value)
