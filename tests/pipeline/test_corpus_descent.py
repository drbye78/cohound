"""Спуск в контейнеры: исполнение, вложенность, бюджет, цикл, видимость (B3).

Тесты работают на **настоящих** zip-архивах, собранных ``zipfile`` в ``tmp_path``: спуск
проверяется по фактическому индексу (central directory), а не по моку. Экстрактор —
заглушка, читающая байты по ``page.source_path`` (диск-первый контракт I19): так видно,
что член попадает в экстрактор **своим** файлом, а не буфером.

Проверяется:

* zip с pdf+md+png: pdf и md ингестятся **разными** ``CorpusDoc``, png — видимый пропуск
  с причиной (``not_ingestible_format``), прогноз фиксируется до извлечения;
* вложенность 3 уровня ингестится — потолка глубины нет; цепочка **5000** различных
  вложенных zip проходит до конца без падений и без ``errors`` (F1: обход итеративен);
* повтор того же архива внутри корня останавливается циклом (``archive_cycle``) — именно
  это делает спуск без потолка глубины конечным;
* член больше ``archive_max_member_bytes`` пропускается, а его соседи ингестятся;
* ``archive_descent=plan`` — только листинг и прогноз, ноль документов;
* битый архив — видимый ``unreadable`` + ``parser_failed`` (блокирующий класс покрытия),
  без падения фазы;
* кумулятивный бюджет корня — видимая остановка ``archive_budget_exhausted``;
* ``split``-экстрактор (не принимает локальные байты) — видимый пропуск члена
  ``extract_unavailable``, а не тихая подмена.

Данные нейтральны (§1.3).
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime as _dt
import io
import json
import pathlib
import tempfile
import threading
import time
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from core.adapters.container_any import container_for
from core.adapters.index_npy import NpyIndexBuilder
from core.domain import (
    AnchorRelation,
    ArchiveDescent,
    ArchiveDescentStatus,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlItemStatus,
    CrawlRunConfig,
    FailureKind,
    RunConfig,
    RunMode,
)
from core.domain.enums import ContainerEncryption
from core.pipeline import RunPlan
from core.pipeline.archive_descent import (
    ENCRYPTION_UNVERIFIED,
    SKIP_READ_FAILED,
    SKIP_STREAM_TOO_LARGE,
)
from core.pipeline.corpus import (
    ExtractionPool,
    IndexBuild,
    build_corpus,
)
from core.pipeline.crawl_session import default_session_id
from core.pipeline.extract_cache import ExtractionCache
from core.pipeline.extraction_runtime import _ExtractRequest
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

RUN_ID = "run-descent"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"
ZIP_URL = "https://docs.test/files/bundle.zip"


# ---------------------------------------------------------------------------
# План/стор/порты (тот же каркас, что у прочих корпусных тестов)
# ---------------------------------------------------------------------------


def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=_catalog(),
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id="case",
            frozen_at=_dt.date(2026, 1, 1),
            entries=[
                CohortEntry(
                    product_id="p1",
                    role_overlap=1.0,
                    selection_reason="anchor",
                    anchor_relation=AnchorRelation.ANCHOR,
                )
            ],
        ),
        golden=[],
    )


def _make_run(store: SqliteStore) -> None:
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
        ),
    )


class _BufferedCrawler:
    """Тело в памяти: архив невелик, staging не нужен."""

    version = "crawler-1"
    engine = "http"

    def __init__(self, pages: dict[str, bytes], content_types: dict[str, str]):
        self.pages = dict(pages)
        self.content_types = dict(content_types)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта, FetchPlan frozen
        self.calls.append(url)
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            content_type=self.content_types.get(url),
            lang="en",
        )


class _FileExtractor:
    """Экстрактор-заглушка: читает **файл** члена по ``page.source_path`` (I19).

    Хеш считается от байтов файла, поэтому pdf и md дают разные ``content_hash`` —
    дедуп не схлопывает членов в один документ.
    """

    version = "extractor-descent-1"
    supports_source_path = True

    def __init__(self) -> None:
        self.calls: list[tuple[str, ExtractionRoute]] = []
        self.source_sizes: list[int] = []

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls.append((page.url, route))
        data = (
            Path(page.source_path).read_bytes()
            if page.source_path
            else _page_bytes(page)
        )
        self.source_sizes.append(len(data))
        return ExtractedDoc(
            text=data.decode("utf-8", errors="replace"),
            content_hash=content_hash(data),
            extractor_version=self.version,
        )


class _RemoteLikeExtractor(_FileExtractor):
    """``split``-профиль: шов работает по URL и локальные байты не принимает."""

    version = "remote-extractor-1"
    supports_source_path = False


class _Chunker:
    version = "chunker-1"

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        return [
            Chunk(
                chunk_id=f"{document_id}:0",
                chunk_no=0,
                text=doc.text,
                heading_path="h1",
                context_prefix="ctx",
                content_hash=doc.content_hash,
            )
        ]


@contextlib.contextmanager
def _run(
    tmp_path: Path,
    archive: bytes,
    *,
    crawl_defaults: CrawlRunConfig | None = None,
    extractor: Any = None,
    extraction_pool: Any = None,
    index_root: Path | None = None,
    embedder: Any = None,
    index_builder: Any = None,
    container_reader: Any = None,
    extract_cache: Any = None,
) -> Iterator[tuple[SqliteStore, Any, Any]]:
    """Один прогон фазы корпуса над одним архивом; читатель контейнеров — реальный.

    ``container_reader`` — фабрика читателя контейнеров (``path → ContainerPort``):
    по умолчанию настоящий ``container_any.container_for``, в тестах отказов чтения —
    обёртка над ним (индекс правдив, тело члена ведёт себя заданно).
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    fake_extractor = extractor if extractor is not None else _FileExtractor()
    try:
        result = build_corpus(
            _plan(),
            _BufferedCrawler(
                {ZIP_URL: archive}, {ZIP_URL: "application/zip"}
            ),
            fake_extractor,
            _Chunker(),
            store,
            urls={"p1": [ZIP_URL]},
            raw_root=tmp_path,
            run_id=RUN_ID,
            crawl_defaults=crawl_defaults,
            container_for=container_reader or container_for,
            extraction_pool=extraction_pool,
            index_root=index_root,
            embedder=embedder,
            index_builder=index_builder,
            extract_cache=extract_cache,
        )
        yield store, result, fake_extractor
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Фикстуры архивов
# ---------------------------------------------------------------------------


def _zip(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def _pdf(pages: int = 3) -> bytes:
    """Минимальный PDF: дерево страниц с ``/Count`` и N объектов ``/Type /Page``.

    Рендерить его не нужно (и нечем): проверяется именно **шапка** — ``page_count``
    берётся из структуры файла, а не из docling.
    """
    body: list[bytes] = [
        b"%PDF-1.7\n",
        b"1 0 obj\n<< /Type /Pages /Kids [] /Count %d >>\nendobj\n" % pages,
    ]
    for index in range(pages):
        body.append(
            b"%d 0 obj\n<< /Type /Page /Parent 1 0 R >>\nendobj\n" % (index + 2)
        )
    body.append(b"trailer\n<< /Root 1 0 R >>\n%%EOF\n")
    return b"".join(body)


def _build_of(result: Any) -> IndexBuild:
    builds = [e for e in result.log_entries if isinstance(e, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def _docs(store: SqliteStore) -> list[Any]:
    """Документы корпуса **из стора**: результат фазы их больше не несёт (Д-2/A4).

    Порядок — детерминированный ``product_id, doc_id`` (не порядок обхода): порядок
    обхода, если он важен, читается отдельно по вставке чанков
    (:func:`_commit_order_urls`).
    """
    return list(store.iter_corpus_docs(RUN_ID))

def _commit_order_urls(store: SqliteStore) -> list[str]:
    """URL документов в порядке **записи** чанков (``corpus_chunks.rowid``).

    Это тот же порядок обхода членов, в котором продукт коммитится на своей границе:
    держать его в памяти как список документов больше не нужно (Д-2/A4), а порядок
    остаётся наблюдаемым фактом.
    """
    rows = store.conn.execute(
        "SELECT url FROM corpus_chunks ORDER BY rowid"
    ).fetchall()
    return list(dict.fromkeys(row["url"] for row in rows))

def _observations(store: SqliteStore) -> list[Any]:
    return list(store.iter_archive_observations(RUN_ID))


def _members(store: SqliteStore) -> list[Any]:
    return list(store.iter_corpus_members())


# ---------------------------------------------------------------------------
# (1) Члены: документы ингестятся, png — видимый пропуск
# ---------------------------------------------------------------------------


def test_zip_members_ingested_with_png_skipped(tmp_path: Path) -> None:
    archive = _zip(
        {
            "docs/manual.pdf": _pdf(3),
            "notes/readme.md": b"# hello\nbody\n",
            "img/logo.png": b"\x89PNG\r\n\x1a\n" + bytes(64),
        }
    )
    with _run(tmp_path, archive) as (store, result, extractor):
        urls = sorted(doc.url for doc in _docs(store))
        assert urls == [
            f"{ZIP_URL}!/docs/manual.pdf",
            f"{ZIP_URL}!/notes/readme.md",
        ]
        # Каждый член — свой документ и свой файл для экстрактора (не буфер архива).
        assert len(extractor.calls) == 2
        assert all(size > 0 for size in extractor.source_sizes)

        observations = _observations(store)
        assert len(observations) == 1
        root = observations[0]
        assert root.status is ArchiveDescentStatus.DESCENDED
        assert root.depth == 0
        assert root.forecast["members_total"] == 3
        assert root.forecast["members_accepted"] == 2
        assert root.forecast["formats"] == {".md": 1, ".pdf": 1}
        assert root.outcome["members_ingested"] == 2
        # png пропущен **с причиной** (прогноз — часть провенанса, не тишина).
        assert root.outcome["skipped"]["not_ingestible_format"]["count"] == 1
        assert root.outcome["skipped"]["not_ingestible_format"]["examples"] == [
            "img/logo.png"
        ]

        rows = {row.member_path: row for row in _members(store)}
        assert set(rows) == {"docs/manual.pdf", "notes/readme.md"}
        assert rows["docs/manual.pdf"].doc_id is not None
        assert rows["docs/manual.pdf"].member_format == "pdf"
        assert rows["docs/manual.pdf"].skip_reason is None
        # Байты членов не ретейнятся: raw_path документа — сам архив (§6.7).
        raw_paths = {doc.raw_path for doc in _docs(store)}
        assert len(raw_paths) == 1
        assert Path(next(iter(raw_paths))).name == f"{content_hash(archive)}.bin"

        # Зонд: pdf-член классифицирован как документ, страницы — по шапке файла.
        probes = [
            entry
            for entry in result.log_entries
            if isinstance(entry, dict) and entry.get("url") == f"{ZIP_URL}!/docs/manual.pdf"
        ]
        assert probes and probes[0]["kind"] == "document"
        assert probes[0]["page_count"] == 3
        # Зонд самого архива: листинг по индексу (central directory).
        archive_probe = [
            entry
            for entry in result.log_entries
            if isinstance(entry, dict) and entry.get("url") == ZIP_URL
        ]
        assert archive_probe and archive_probe[0]["container"]["members_total"] == 3


# ---------------------------------------------------------------------------
# (2) Вложенность: потолка глубины нет
# ---------------------------------------------------------------------------


def test_nested_archives_three_levels_are_ingested(tmp_path: Path) -> None:
    deep = _zip({"deep.md": b"deep text"})
    middle = _zip({"inner.zip": deep})
    outer = _zip({"mid.zip": middle})

    with _run(tmp_path, outer) as (store, _result, _extractor):
        docs = _docs(store)
        assert len(docs) == 1
        assert docs[0].url == f"{ZIP_URL}!/mid.zip!/inner.zip!/deep.md"
        # Наблюдение на каждый уровень: 0 — корень, 1 — mid.zip, 2 — inner.zip.
        observations = _observations(store)
        assert sorted(item.depth for item in observations) == [0, 1, 2]
        assert all(
            item.status is ArchiveDescentStatus.DESCENDED for item in observations
        )


#: Глубина цепочки в тесте F1. Замер: 5000 различных вложенных zip (см. ниже) — это
#: ~990 уровней прежней рекурсивной формы, то есть точка, где она уже падала.
_CHAIN_DEPTH = 5000


def _zip_chain(depth: int, innermost: dict[str, bytes]) -> bytes:
    """Цепочка ``depth`` **различных** вложенных zip; на дне — ``innermost``.

    ``ZIP_STORED``: тело уровня — уже сжатый zip, дефлейт дал бы только время. Хеши
    содержимого у всех уровней разные (размер и заголовок предыдущего входят в байты),
    поэтому цикл ``archive_cycle`` здесь не срабатывает: конечность даёт только бюджет
    байтов — а он крошечные уровни не ловит, поэтому единственная защита от падения —
    итеративный обход (F1).
    """
    payload = _zip_stored(innermost)
    for _ in range(depth):
        payload = _zip_stored({"a.zip": payload})
    return payload


def _zip_stored(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def _walker(
    path: Path,
    *,
    observations: list[Any],
    members: list[Any],
    errors: list[str],
) -> Any:
    """Walker уровня модуля (без фазы): для тестов формы обхода, не провенанса."""
    from core.pipeline.archive_descent import (
        ArchiveDescentWalker,
        DescentBudget,
        MemberOutcome,
    )

    return ArchiveDescentWalker(
        product_id="p1",
        root_url=ZIP_URL,
        descent=ArchiveDescent.FULL,
        budget=DescentBudget(max_member_bytes=1024 * 1024),
        container_for=container_for,
        ingest_member=lambda task: MemberOutcome(
            doc_id=f"p1:{task.member_path}", content_hash="h" * 64
        ),
        observations=observations,
        members=members,
        errors=errors,
        run_id=RUN_ID,
    )


def test_chain_of_5000_distinct_archives_reaches_the_end(tmp_path: Path) -> None:
    """F1: цепочка 5000 различных вложенных zip проходит до конца (нет падения).

    Прежняя рекурсивная форма умирала на ~990 уровнях: ``RecursionError`` гасился в
    выборе читателя и выходил как обманчивое ``container_reader_unavailable`` (или
    наружу из ``plan_members``). Явный стек кадров делает обещание «потолка глубины
    нет» правдой: наблюдений — по одному на уровень, ``errors`` пуст, исход видим.
    """
    payload = _zip_chain(_CHAIN_DEPTH, {"note.md": b"bottom"})
    path = tmp_path / "chain.bin"
    path.write_bytes(payload)

    observations: list[Any] = []
    members: list[Any] = []
    errors: list[str] = []
    walker = _walker(path, observations=observations, members=members, errors=errors)
    observation = walker.descend(
        archive_path=path,
        archive_url=ZIP_URL,
        archive_content_hash=content_hash(payload),
    )

    assert errors == []
    # По наблюдению на каждый уровень: корень (0) + 5000 вложенных.
    assert len(observations) == _CHAIN_DEPTH + 1
    assert max(item.depth for item in observations) == _CHAIN_DEPTH
    assert all(
        item.status is ArchiveDescentStatus.DESCENDED for item in observations
    )
    assert observation.outcome["cycle_stops"] == 0
    # Документ на дне цепочки извлечён (последняя строка — он, а не пропуск).
    assert members[-1].member_path == "note.md"
    assert members[-1].doc_id is not None


def test_document_at_the_end_of_a_chain_is_ingested(tmp_path: Path) -> None:
    """F1: документ на дне вложенной цепочки доходит до ингеста со своим URL."""
    depth = 50
    payload = _zip_chain(depth, {"note.md": b"bottom"})
    path = tmp_path / "chain.bin"
    path.write_bytes(payload)

    observations: list[Any] = []
    members: list[Any] = []
    errors: list[str] = []
    walker = _walker(path, observations=observations, members=members, errors=errors)
    walker.descend(
        archive_path=path,
        archive_url=ZIP_URL,
        archive_content_hash=content_hash(payload),
    )

    assert errors == []
    document = members[-1]
    assert document.member_path == "note.md"
    assert document.doc_id is not None
    assert document.depth == depth
    assert document.member_url == f"{ZIP_URL}{'!/a.zip' * depth}!/note.md"


# ---------------------------------------------------------------------------
# (3) Цикл по хешу содержимого: спуск без потолка глубины остаётся конечным
# ---------------------------------------------------------------------------


def test_repeated_nested_archive_stops_on_cycle(tmp_path: Path) -> None:
    """Архив, чьи байты уже пройдены в корне, повторно не разворачивается.

    Литеральный «архив, содержащий копию себя» байт-в-байт невозможен (фиксированной
    точки нет: длина архива входила бы в его же содержимое), поэтому проверяется
    **конструктивная** форма того же правила: один и тот же вложенный архив лежит в двух
    местах корня. Первый спуск исполняется, второй останавливается ``archive_cycle`` —
    ровно это гарантирует конечность спуска без ограничения глубины.
    """
    inner = _zip({"x.md": b"cycle content"})
    outer = _zip({"a/inner.zip": inner, "b/inner.zip": inner})

    with _run(tmp_path, outer) as (store, _result, _extractor):
        docs = _docs(store)
        assert len(docs) == 1
        root = _observations(store)[0]
        assert root.outcome["cycle_stops"] == 1
        assert root.outcome["skipped"]["archive_cycle"]["count"] == 1
        assert root.outcome["skipped"]["archive_cycle"]["examples"] == ["b/inner.zip"]
        # Вложенный архив развёрнут ровно один раз (одно наблюдение глубины 1).
        assert [item.depth for item in _observations(store)].count(1) == 1


# ---------------------------------------------------------------------------
# (4) Потолок одного члена: соседи всё равно ингестятся
# ---------------------------------------------------------------------------


def test_member_over_ceiling_skipped_siblings_ingested(tmp_path: Path) -> None:
    archive = _zip({"big.txt": b"x" * 4096, "small.md": b"ok"})
    with _run(
        tmp_path,
        archive,
        crawl_defaults=CrawlRunConfig(archive_max_member_bytes=1024),
    ) as (store, _result, _extractor):
        docs = _docs(store)
        assert [doc.url for doc in docs] == [f"{ZIP_URL}!/small.md"]
        root = _observations(store)[0]
        assert root.forecast["skipped"]["member_too_large"]["count"] == 1
        assert root.forecast["skipped"]["member_too_large"]["examples"] == ["big.txt"]


# ---------------------------------------------------------------------------
# (5) archive_descent=plan: листинг и прогноз без извлечения
# ---------------------------------------------------------------------------


def test_plan_descent_lists_without_extracting(tmp_path: Path) -> None:
    archive = _zip({"a.md": b"a", "b.pdf": _pdf(2)})
    with _run(
        tmp_path,
        archive,
        crawl_defaults=CrawlRunConfig(archive_descent=ArchiveDescent.PLAN),
    ) as (store, result, extractor):
        assert extractor.calls == []
        assert _build_of(result).doc_count == 0
        observations = _observations(store)
        assert len(observations) == 1
        assert observations[0].status is ArchiveDescentStatus.PLANNED
        assert observations[0].forecast["members_accepted"] == 2
        assert observations[0].outcome["members_ingested"] == 0
        # Планирование не создаёт строк членов: спуск не исполнялся.
        assert _members(store) == []


# ---------------------------------------------------------------------------
# (6) Битый архив: видимый unreadable, не падение
# ---------------------------------------------------------------------------


def test_corrupt_archive_is_visible_unreadable(tmp_path: Path) -> None:
    broken = b"PK\x03\x04" + bytes(256)
    with _run(tmp_path, broken) as (store, result, extractor):
        assert extractor.calls == []
        assert _build_of(result).doc_count == 0
        assert any("archive_unreadable" in error for error in result.errors)
        observation = _observations(store)[0]
        assert observation.status is ArchiveDescentStatus.UNREADABLE
        assert observation.reason
        # Нечитаемый контейнер — блокирующий класс покрытия (содержимое ненаблюдено).
        items = {item.url: item for item in store.iter_crawl_session_items(SESSION_ID)}
        assert items[ZIP_URL].status is CrawlItemStatus.FAILED
        assert items[ZIP_URL].error_kind == FailureKind.PARSER_FAILED.value
        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is False
        assert coverage.blocking_failures.get(FailureKind.PARSER_FAILED.value) == 1


# ---------------------------------------------------------------------------
# (7) Кумулятивный бюджет корня: видимая остановка
# ---------------------------------------------------------------------------


def test_cumulative_budget_exhaustion_is_visible(tmp_path: Path) -> None:
    archive = _zip(
        {"a.txt": b"A" * 1024, "b.txt": b"B" * 1024, "c.txt": b"C" * 1024}
    )
    with _run(
        tmp_path,
        archive,
        crawl_defaults=CrawlRunConfig(archive_max_total_uncompressed_bytes=1024),
    ) as (store, result, _extractor):
        docs = _docs(store)
        # Первый член уложился ровно в бюджет, остальные — остановлены.
        assert len(docs) == 1
        observation = _observations(store)[0]
        assert observation.status is ArchiveDescentStatus.BUDGET_EXHAUSTED
        assert observation.outcome["stopped_reason"] == "archive_budget_exhausted"
        assert observation.outcome["budget_limit_bytes"] == 1024
        assert any(
            "archive_budget_exhausted" in error for error in result.errors
        ), "исчерпание бюджета обязано быть видимым, а не тихим"
        # F1: срезанный бюджетом член даёт **строку** (раньше его не было в
        # ``corpus_members`` вовсе, и «почему этого файла нет в корпусе» не отвечал никто).
        rows = {row.member_path: row for row in _members(store)}
        assert set(rows) == {"a.txt", "b.txt", "c.txt"}
        assert rows["a.txt"].doc_id is not None
        for cut in ("b.txt", "c.txt"):
            assert rows[cut].skip_reason == "archive_budget_exhausted"
            assert rows[cut].doc_id is None
            assert rows[cut].extracted_content_hash is None
            assert rows[cut].declared_size == 1024


# ---------------------------------------------------------------------------
# (8) split: член не извлекается молча
# ---------------------------------------------------------------------------


def test_extractor_without_local_bytes_skips_members_visibly(tmp_path: Path) -> None:
    archive = _zip({"a.md": b"a", "b.txt": b"b"})
    with _run(
        tmp_path, archive, extractor=_RemoteLikeExtractor()
    ) as (store, result, extractor):
        assert extractor.calls == []  # шов не вызывается на локальных байтах
        assert _build_of(result).doc_count == 0
        observation = _observations(store)[0]
        assert observation.status is ArchiveDescentStatus.DESCENDED
        assert observation.outcome["skipped"]["extract_unavailable"]["count"] == 2
        assert any("extract_unavailable" in error for error in result.errors)
        rows = _members(store)
        assert len(rows) == 2
        assert all(row.skip_reason == "extract_unavailable" for row in rows)


# ---------------------------------------------------------------------------
# (8b) Отказ чтения члена: строка ``corpus_members`` обязательна
# ---------------------------------------------------------------------------
#
# Дефект: ветка ``except Exception`` вокруг чтения тела члена писала причину в ``errors`` и
# в ``skipped``, но **не** вызывала ``_emit_skipped_member`` — соседние причины
# (``archive_budget_exhausted``, ``member_stream_too_large``) вызывали. Следствие: в
# ``corpus_members`` непрочитанного члена не было вовсе, и «член не прочитан» выглядело
# ровно как «члена не было». Для архива вендора (47 членов одного zip) это существенно.
#
# Читатель здесь — обёртка над настоящим: индекс берётся из central directory (прогноз и
# состав правдивы), а тело указанного члена ведёт себя заданно. Так проверяется именно
# ветка отказа, а не поведение ``zipfile``.


class _ReadFailingReader:
    """Обёртка над читателем контейнера: индекс правдив, чтение члена бросает."""

    def __init__(self, inner: Any, broken: set[str]) -> None:
        self._inner = inner
        self._broken = set(broken)
        self.version = getattr(inner, "version", "wrapped-1")

    def available(self) -> bool:
        return self._inner.available()

    def list_members(self, path: str) -> Any:
        return self._inner.list_members(path)

    def open_member(self, path: str, member_path: str) -> Iterator[bytes]:
        if member_path in self._broken:
            # Отказ на первом шаге стрима: тело члена недоступно (битый член/нет бэкенда).
            raise zipfile.BadZipFile(f"повреждённый член: {member_path}")
        yield from self._inner.open_member(path, member_path)


class _OverlongReader:
    """Обёртка над читателем: тело указанного члена **длиннее** заявленного в индексе.

    Так выглядит zip с ложной шапкой (заявленный размер меньше фактического): список
    принимает член по индексу, а поток пробивает потолок — второй гейт bomb-guard.
    """

    def __init__(self, inner: Any, overlong: set[str], *, padding: int = 4096) -> None:
        self._inner = inner
        self._overlong = set(overlong)
        self._padding = padding
        self.version = getattr(inner, "version", "wrapped-1")

    def available(self) -> bool:
        return self._inner.available()

    def list_members(self, path: str) -> Any:
        return self._inner.list_members(path)

    def open_member(self, path: str, member_path: str) -> Iterator[bytes]:
        if member_path in self._overlong:
            yield b"x" * self._padding
            return
        yield from self._inner.open_member(path, member_path)


def test_member_read_failure_writes_visible_row(tmp_path: Path) -> None:
    """Не прочитанный член — строка ``corpus_members`` с причиной, а не пустое место."""
    archive = _zip({"a.md": b"# a\n", "b.md": b"# b\n", "c.md": b"# c\n"})

    def _reader(path: str) -> Any:
        inner = container_for(path)
        return None if inner is None else _ReadFailingReader(inner, {"b.md"})

    with _run(tmp_path, archive, container_reader=_reader) as (store, result, _):
        rows = {row.member_path: row for row in _members(store)}
        assert set(rows) == {"a.md", "b.md", "c.md"}, (
            "непрочитанный член обязан иметь строку: иначе он неотличим от отсутствующего"
        )
        assert rows["b.md"].skip_reason == SKIP_READ_FAILED
        assert rows["b.md"].doc_id is None
        assert rows["b.md"].extracted_content_hash is None
        assert rows["b.md"].declared_size == 4
        # Соседи не задеты: отказ одного члена изолирован.
        assert rows["a.md"].doc_id is not None
        assert rows["c.md"].doc_id is not None
        assert sorted(doc.url for doc in _docs(store)) == [
            f"{ZIP_URL}!/a.md",
            f"{ZIP_URL}!/c.md",
        ]
        # Причина видна и в ошибках фазы (прежний путь), и в исходе спуска.
        assert any(SKIP_READ_FAILED in error for error in result.errors), result.errors
        observation = _observations(store)[0]
        assert observation.outcome["skipped"][SKIP_READ_FAILED]["count"] == 1


def test_member_read_failure_keeps_other_errors(tmp_path: Path) -> None:
    """Отказ записи строки не съедает уже собранные ``errors`` (порядок: причина → строка)."""
    archive = _zip({"a.md": b"# a\n", "b.md": b"# b\n"})

    def _reader(path: str) -> Any:
        inner = container_for(path)
        return None if inner is None else _ReadFailingReader(inner, {"a.md", "b.md"})

    with _run(tmp_path, archive, container_reader=_reader) as (store, result, _extractor):
        rows = _members(store)
        assert [row.skip_reason for row in rows] == [SKIP_READ_FAILED, SKIP_READ_FAILED]
        # Две ошибки чтения — и обе на месте: причина пишется до эмита строки.
        assert sum(SKIP_READ_FAILED in error for error in result.errors) == 2


class _EncryptionStatingReader:
    """Обёртка над читателем: индекс правдив, но заявление о шифровании — заданное.

    Так моделируется контейнер, у которого читатель **не умеет** определять шифрование
    (``UNKNOWN``): предикат молчит, чтение члена отказывает. Прежний булев предикат отдавал
    в этой ситуации «не зашифрован», и отказ чтения оставался без причины.
    """

    def __init__(self, inner: Any, encryption: ContainerEncryption, broken: set[str]) -> None:
        self._inner = inner
        self._encryption = encryption
        self._broken = set(broken)
        self.version = getattr(inner, "version", "wrapped-1")

    def available(self) -> bool:
        return self._inner.available()

    def list_members(self, path: str) -> Any:
        return dataclasses.replace(
            self._inner.list_members(path), encryption=self._encryption
        )

    def open_member(self, path: str, member_path: str) -> Iterator[bytes]:
        if member_path in self._broken:
            raise zipfile.BadZipFile(f"пароль не принят: {member_path}")
        yield from self._inner.open_member(path, member_path)

def test_unverified_encryption_is_named_on_read_failure(tmp_path: Path) -> None:
    """``UNKNOWN`` не выдаёт себя за «не зашифрован»: отказ чтения называет непроверенный предикат."""
    archive = _zip({"a.md": b"# a\n", "b.md": b"# b\n"})

    def _reader(path: str) -> Any:
        inner = container_for(path)
        if inner is None:
            return None
        return _EncryptionStatingReader(inner, ContainerEncryption.UNKNOWN, {"b.md"})

    with _run(tmp_path, archive, container_reader=_reader) as (store, result, _):
        # Наблюдение спуска называет состояние предиката фактом (прогноз — до извлечения).
        observation = _observations(store)[0]
        # Прогноз читается из БД: значение — та же строка (``unknown``), не bool.
        assert observation.forecast["encrypted"] == ContainerEncryption.UNKNOWN
        # Причина отказа члена названа: иначе «падает позже» остаётся без объяснения.
        error = next(item for item in result.errors if SKIP_READ_FAILED in item)
        assert ENCRYPTION_UNVERIFIED in error
        assert "пароль" in error
        rows = {row.member_path: row for row in _members(store)}
        assert rows["b.md"].skip_reason == SKIP_READ_FAILED
        assert rows["a.md"].doc_id is not None

def test_stated_negative_encryption_is_not_marked_unverified(tmp_path: Path) -> None:
    """Если предикат заявлен и отрицателен — приписки о непроверенном предикате нет."""
    archive = _zip({"a.md": b"# a\n", "b.md": b"# b\n"})

    def _reader(path: str) -> Any:
        inner = container_for(path)
        if inner is None:
            return None
        return _EncryptionStatingReader(
            inner, ContainerEncryption.NOT_ENCRYPTED, {"b.md"}
        )

    with _run(tmp_path, archive, container_reader=_reader) as (store, result, _):
        observation = _observations(store)[0]
        assert observation.forecast["encrypted"] == ContainerEncryption.NOT_ENCRYPTED
        error = next(item for item in result.errors if SKIP_READ_FAILED in item)
        assert ENCRYPTION_UNVERIFIED not in error

def test_probe_without_reader_reports_unknown_encryption(tmp_path: Path) -> None:
    """Не подключённый читатель: предикат не спрашивался — ``unknown``, а не «не зашифрован»."""
    from core.pipeline.artifact_probe import probe_artifact

    path = tmp_path / "bundle.zip"
    path.write_bytes(_zip({"a.md": b"# a\n"}))
    record = probe_artifact(
        path, product_id="p1", url=ZIP_URL, container_for=None
    )

    assert record.container is not None
    assert record.container["encrypted"] is ContainerEncryption.UNKNOWN
    assert record.container["unreadable"].startswith("container_reader_unavailable")

def test_stream_over_ceiling_keeps_row(tmp_path: Path) -> None:
    """``member_stream_too_large`` продолжает писать строку (соседняя причина не сломана)."""
    archive = _zip({"big.txt": b"B" * 10, "small.md": b"ok"})

    def _reader(path: str) -> Any:
        inner = container_for(path)
        return None if inner is None else _OverlongReader(inner, {"big.txt"})

    with _run(
        tmp_path,
        archive,
        crawl_defaults=CrawlRunConfig(archive_max_member_bytes=1024),
        container_reader=_reader,
    ) as (store, result, _extractor):
        rows = {row.member_path: row for row in _members(store)}
        assert rows["big.txt"].skip_reason == SKIP_STREAM_TOO_LARGE
        assert rows["big.txt"].doc_id is None
        assert rows["big.txt"].declared_size == 10
        assert rows["small.md"].doc_id is not None
        assert any(SKIP_STREAM_TOO_LARGE in error for error in result.errors)



# ---------------------------------------------------------------------------
# (9+) Пул экстракции: члены извлекаются воркерами, коммит — в порядке обхода
# ---------------------------------------------------------------------------
#
# Почему это отдельный блок. До правки члены контейнера извлекались **в родителе**
# синхронно: 47 PDF одного zip занимали один поток, а ``extract_workers`` простаивали.
# Теперь член идёт в тот же фазовый пул, что и страница, с окном упреждения K и
# доводкой FIFO. Приёмочная планка — **наблюдаемая эквивалентность** inline-пути:
# порядок документов, порядок чанков, ``errors``, победитель дедупа, строки
# ``corpus_members``, ``archive_observations`` и порядок зондов. Поэтому ниже каждый
# тест сравнивает два прогона на одном и том же архиве, а не «смотрит, что что-то есть».
#
# Порядок чанков здесь не косметика: ``_content_digest`` (``core/adapters/index_npy``)
# склеивает ``chunk_ids``/хеши/матрицу **в порядке вставки**, а ``partition_build_id``
# порядко-независим. Коммит в порядке завершения дал бы тот же build id при другом
# дайджесте → ``ImmutableBuildError``. Отсюда тест с обратным порядком завершения: он
# сравнивает ``vector_digest`` манифеста двух прогонов и **доказывает**, что порядок
# вставки совпал.


class _Future:
    """Future-подобный: задание считается в отдельном потоке (реальный порядок финиша)."""

    def __init__(self) -> None:
        self._done = threading.Event()
        self._value: Any = None
        self._error: BaseException | None = None

    def result(self, timeout: float | None = None) -> Any:
        if not self._done.wait(timeout):
            raise TimeoutError()
        if self._error is not None:
            raise self._error
        return self._value


class _ThreadPool:
    """Исполнитель с настоящей асинхронностью: финиш ≠ порядок ``submit``.

    ``delays`` расходуется по одному значению на задание (первое — сам архив): так
    первый член можно сделать **медленнее** остальных и получить обратный порядок
    завершения. ``fail_urls`` — задания, падающие в воркере.
    """

    def __init__(
        self,
        size: int,
        *,
        delays: tuple[float, ...] = (),
        fail_urls: tuple[str, ...] = (),
    ) -> None:
        self.size = size
        self._delays = list(delays)
        self._fail_urls = tuple(fail_urls)
        self.submitted: list[str] = []
        self.requests: list[_ExtractRequest] = []
        self.completed: list[str] = []
        self.max_inflight = 0
        self.shutdown_calls = 0
        self.max_tasks_per_child: int | None = None
        self._open = 0

    def submit(self, fn: Any, request: _ExtractRequest) -> _Future:
        self.submitted.append(request.url)
        self.requests.append(request)
        delay = self._delays.pop(0) if self._delays else 0.0
        self._open += 1
        self.max_inflight = max(self.max_inflight, self._open)
        future = _Future()

        def _run() -> None:
            try:
                if delay:
                    time.sleep(delay)
                if request.url in self._fail_urls:
                    raise RuntimeError("boom")
                future._value = fn(request)
            except BaseException as exc:  # noqa: BLE001 — эмуляция отказа воркера
                future._error = exc
            finally:
                self._open -= 1
                self.completed.append(request.url)
                future._done.set()

        threading.Thread(target=_run, daemon=True).start()
        return future

    def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
        self.shutdown_calls += 1


def _file_worker(request: _ExtractRequest) -> ExtractedDoc:
    """Воркер, эквивалентный :class:`_FileExtractor`: читает файл по ``raw_path`` (I19)."""
    data = Path(request.raw_path).read_bytes()
    return ExtractedDoc(
        text=data.decode("utf-8", errors="replace"),
        content_hash=content_hash(data),
        extractor_version="extractor-descent-1",
    )


def _pool_factory(
    created: list[_ThreadPool], **kw: Any
) -> Any:
    def factory(
        n: int,
        level: Any = None,
        *,
        pdf_pipeline: Any = None,
        max_tasks_per_child: int | None = None,
    ) -> Any:
        pool = _ThreadPool(n, **kw)
        pool.max_tasks_per_child = max_tasks_per_child
        created.append(pool)
        return pool

    return factory


def _pool(created: list[_ThreadPool], **kw: Any) -> ExtractionPool:
    return ExtractionPool(pool_factory=_pool_factory(created, **kw), worker=_file_worker)


class _DeterministicEmbedder:
    """Эмбеддер, зависящий **только от текста**: вектора двух прогонов совпадают.

    Это необходимо тесту с обратным порядком завершения: ``vector_digest`` включает
    байты матрицы,
    поэтому эмбеддер с зависимостью от индекса батча сделал бы дайджесты несравнимыми.
    """

    model_id = "embed-descent"
    dim = 3

    def embed(self, texts: Any) -> list[list[float]]:
        out: list[list[float]] = []
        for text in texts:
            digest = content_hash(str(text))
            out.append([int(digest[i : i + 2], 16) / 255.0 for i in (0, 2, 4)])
        return out


def _snapshot(store: SqliteStore, result: Any) -> dict[str, Any]:
    """Полный наблюдаемый слепок прогона — сравнимый между inline и пулом."""
    build = _build_of(result)
    chunk_rows = list(
        store.conn.execute(
            "SELECT chunk_id, doc_id, url FROM corpus_chunks ORDER BY rowid"
        )
    )
    return {
        # Порядок документов = порядок обхода членов (``raw_path`` — имя файла: каталоги
        # прогонов разные, а идентичность архива — хеш в имени, I19).
        "docs": [
            (
                doc.url,
                doc.content_hash,
                Path(doc.raw_path).name,
                doc.extractor_version,
                doc.lang,
            )
            for doc in _docs(store)
        ],
        # Порядок **вставки** чанков (rowid), а не сортировка по chunk_no.
        "chunks": [row["chunk_id"] for row in chunk_rows],
        "chunk_urls": [row["url"] for row in chunk_rows],
        "index_build_id": build.index_build_id,
        "chunk_count": build.chunk_count,
        "entries": [
            (entry.product_id, entry.partition_build_id, entry.content_digest)
            for entry in build.entries
        ],
        "errors": list(result.errors),
        "members": [
            (
                row.member_path,
                row.doc_id,
                row.skip_reason,
                row.extracted_content_hash,
                row.declared_size,
                row.depth,
                row.member_format,
            )
            for row in _members(store)
        ],
        "observations": [
            (
                item.depth,
                item.status,
                item.reason,
                json.dumps(item.outcome, sort_keys=True),
                json.dumps(item.forecast, sort_keys=True),
            )
            for item in _observations(store)
        ],
        "probes": [
            entry["url"]
            for entry in result.log_entries
            if isinstance(entry, dict) and "url" in entry
        ],
    }


def _rich_archive() -> bytes:
    """zip с дедупом, разными маршрутами, пропуском по формату и вложенным архивом."""
    inner = _zip({"nested/one.md": b"nested one", "nested/two.md": b"nested two"})
    return _zip(
        {
            "a/first.pdf": _pdf(2),
            "b/readme.md": b"# readme\nbody\n",
            "c/copy.md": b"same bytes",
            "d/copy.md": b"same bytes",  # дедуп: победитель — первый по порядку обхода
            "e/logo.png": b"\x89PNG\r\n\x1a\n" + bytes(32),
            "f/inner.zip": inner,
            "g/last.txt": b"last",
        }
    )


def test_pooled_members_are_byte_identical_to_inline(tmp_path: Path) -> None:
    """Путь пула даёт **тот же** наблюдаемый корпус, что inline-путь (K=2)."""
    archive = _rich_archive()

    with _run(tmp_path / "inline", archive) as (inline_store, inline_result, extractor):
        inline = _snapshot(inline_store, inline_result)
        # Inline-путь: каждый член — свой вызов экстрактора в родителе
        # (5 документов верхнего уровня + 2 члена вложенного архива; png отсеян планом).
        assert len(extractor.calls) == 7

    created: list[_ThreadPool] = []
    with _run(
        tmp_path / "pooled",
        archive,
        crawl_defaults=CrawlRunConfig(extract_workers=2),
        extraction_pool=_pool(created),
    ) as (pooled_store, pooled_result, _extractor):
        pooled = _snapshot(pooled_store, pooled_result)

    # Ровно один пул на ключ (C1), и члены в него действительно попадали.
    assert len(created) == 1
    member_urls = [url for url in created[0].submitted if "!/" in url]
    assert len(member_urls) == 7, "все члены-документы обязаны уйти в пул"

    for key in (
        "docs",
        "chunks",
        "chunk_urls",
        "index_build_id",
        "chunk_count",
        "entries",
        "errors",
        "members",
        "observations",
        "probes",
    ):
        assert pooled[key] == inline[key], f"расхождение наблюдаемого артефакта: {key}"

    # Окно не шире ``extract_workers`` (страница архива уже доведена до спуска).
    assert created[0].max_inflight <= 2

    # Маршрут члена — по **его** суффиксу, а не по суффиксу архива.
    routes = {
        request.url.rsplit("!/", 1)[-1]: request.route
        for request in created[0].requests
    }
    assert routes["a/first.pdf"] is ExtractionRoute.DOCUMENT
    assert routes["b/readme.md"] is ExtractionRoute.AUTO

    # S2/план 23: ключ кэша члена — хеш **байтов** члена, а не номер temp-файла
    # (``member-N.bin``): иначе разные члены архива делили бы одну запись кэша.
    for request in created[0].requests:
        assert request.cache_key, "у задания члена обязан быть явный ключ кэша"
        assert request.cache_key != Path(request.raw_path).stem

    # Операторская строка: доведённые задания пула = **члены** (7), без самой страницы
    # архива. До F5 архив тоже уходил в пул (и его «извлечение» выбрасывалось), поэтому
    # счётчик был 8; теперь зонд артефакта идёт до submit (``_start_row``), и за контейнер
    # воркер не платит. Это уменьшение работы при том же наблюдаемом корпусе — и оно
    # запинено здесь именно как число.
    assert any(
        isinstance(entry, str) and "документов через пул: 7" in entry
        for entry in pooled_result.log_entries
    ), pooled_result.log_entries


def test_member_extraction_cache_is_reused_across_runs(tmp_path: Path) -> None:
    """S2/план 23: второй прогон над теми же байтами не конвертирует членов заново.

    Кэш межпрогонный: retained-файл архива назван его хешем, байты членов те же, поэтому
    экстрактор во втором прогоне не зовётся ни разу — а наблюдаемый корпус обязан
    совпасть с первым байт-в-байт (кэш не меняет артефакт, только его цену). Тест идёт
    **inline**-путём (пула нет): раньше кэш там не спрашивался вовсе, и ``frozen``/
    повторный тираж конвертировал все члены заново.
    """
    archive = _rich_archive()
    cache = ExtractionCache(tmp_path / "extract_cache")

    with _run(tmp_path / "first", archive, extract_cache=cache) as (
        first_store,
        first_result,
        first_extractor,
    ):
        first = _snapshot(first_store, first_result)
        # 7 членов-документов, но ``c/copy.md`` и ``d/copy.md`` — одни и те же байты:
        # второй уже не конвертируется (S2). Дедуп текста схлопнул бы их и раньше, разница
        # в том, что прежде обе копии платили конвертацию.
        assert len(first_extractor.calls) == 6

    with _run(tmp_path / "second", archive, extract_cache=cache) as (
        second_store,
        second_result,
        second_extractor,
    ):
        second = _snapshot(second_store, second_result)

    assert second_extractor.calls == [], "повторные байты членов ушли в конвертер второй раз"
    for key in (
        "docs",
        "chunks",
        "chunk_urls",
        "index_build_id",
        "chunk_count",
        "entries",
        "errors",
        "members",
        "observations",
        "probes",
    ):
        assert second[key] == first[key], f"кэш изменил наблюдаемый артефакт: {key}"


def test_descent_closes_container_reader_at_the_end(tmp_path: Path) -> None:
    """Конец спуска закрывает открытые хендлы читателя контейнеров (S2/план 23).

    ``ZipContainer`` держит один ``ZipFile`` на архив на всё время обхода (иначе индекс
    читался бы на каждого члена). События «архив закончился» в порте нет, поэтому
    закрытие — обязанность спуска: иначе последний архив остался бы открытым до конца
    процесса. Читатель без ``close`` (7z/rar, тестовые обёртки) ничего не обязан уметь.
    """
    archive = _zip({"a.md": b"# a\n", "b.md": b"# b\n"})
    closed: list[Any] = []

    class _ClosingReader:
        def __init__(self, inner: Any) -> None:
            self._inner = inner
            self.version = getattr(inner, "version", "wrapped-1")

        def available(self) -> bool:
            return self._inner.available()

        def list_members(self, path: str) -> Any:
            return self._inner.list_members(path)

        def open_member(self, path: str, member_path: str) -> Iterator[bytes]:
            yield from self._inner.open_member(path, member_path)

        def close(self) -> None:
            closed.append(self)
            self._inner.close()

    reader: _ClosingReader | None = None

    def _reader(path: str) -> Any:
        nonlocal reader
        inner = container_for(path)
        if inner is None:
            return None
        reader = _ClosingReader(inner)
        return reader

    with _run(tmp_path, archive, container_reader=_reader) as (store, _result, _):
        assert len(_docs(store)) == 2

    assert closed, "конец спуска обязан закрыть читателя контейнеров"
    assert reader is not None and reader._inner._open == {}  # type: ignore[attr-defined]


def test_pooled_commit_order_is_walk_order_not_completion_order(
    tmp_path: Path,
) -> None:
    """Порядок вставки чанков — порядок обхода, даже если члены финишируют наоборот.

    Доказательство сильнее сравнения списков: два прогона (inline и пул с **обратным**
    порядком завершения) собирают партицию настоящим ``NpyIndexBuilder`` в разные
    ``index_root``. ``partition_build_id`` порядко-независим, а ``vector_digest``
    манифеста — нет (``_content_digest`` склеивает чанки и матрицу в порядке вставки).
    Совпадение обоих означает: коммит шёл в порядке обхода. Обратный порядок дал бы тот
    же build id при другом дайджесте — ровно тот дефект, который запрещён (§13).
    """
    archive = _zip(
        {
            "m1.md": b"member one",
            "m2.md": b"member two",
            "m3.md": b"member three",
            "m4.md": b"member four",
        }
    )

    def _build(root: Path, **kw: Any) -> dict[str, Any]:
        with _run(
            root / "raw",
            archive,
            index_root=root / "index",
            embedder=_DeterministicEmbedder(),
            index_builder=NpyIndexBuilder(root / "index"),
            **kw,
        ) as (store, result, _extractor):
            manifests = sorted((root / "index").rglob("manifest.json"))
            assert len(manifests) == 1, manifests
            manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
            assert not [
                error for error in result.errors if "ImmutableBuildError" in error
            ], result.errors
            chunk_rows = list(
                store.conn.execute(
                    "SELECT chunk_id, url FROM corpus_chunks ORDER BY rowid"
                )
            )
            return {
                # Порядок обхода виден в порядке записи чанков; документы из результата
                # фазы больше не читаются (Д-2/A4).
                "docs": list(dict.fromkeys(row["url"] for row in chunk_rows)),
                "partition_build_id": manifest["partition_build_id"],
                "vector_digest": manifest["vector_digest"],
                "chunks": [row["chunk_id"] for row in chunk_rows],
            }

    inline = _build(tmp_path / "inline")

    created: list[_ThreadPool] = []
    # Первый **член** — самый медленный: порядок завершения обратен порядку обхода.
    # До F5 первым в пул уходила страница архива (слот 0 задержек), поэтому задержка
    # стояла на индексе 1; теперь контейнер в пул не попадает, и слот 0 — это ``m1``.
    pooled = _build(
        tmp_path / "pooled",
        crawl_defaults=CrawlRunConfig(extract_workers=2),
        extraction_pool=_pool(created, delays=(0.12, 0.0, 0.0, 0.0)),
    )

    members = [url for url in created[0].submitted if "!/" in url]
    finished = [url for url in created[0].completed if "!/" in url]
    assert finished != members, (
        "тест бессмыслен без обратного порядка завершения: "
        f"submit={members}, finish={finished}"
    )
    assert finished[0] == members[1], finished

    assert pooled["docs"] == inline["docs"]
    assert pooled["chunks"] == inline["chunks"]
    assert pooled["partition_build_id"] == inline["partition_build_id"]
    assert pooled["vector_digest"] == inline["vector_digest"]


def test_pooled_nested_archive_commits_members_in_walk_order(tmp_path: Path) -> None:
    """Вложенный архив спускается, а его члены коммитятся **после** старших соседей.

    Порядок обхода — детерминированная сортировка нормализованных путей
    (``plan_members``), поэтому архив назван ``b.zip``: он в середине, и его члены
    обязаны оказаться между ``a.md`` и ``c.md``, а не в конце.
    """
    inner = _zip({"x.md": b"x", "y.md": b"y"})
    outer = _zip({"a.md": b"a", "b.zip": inner, "c.md": b"c"})

    created: list[_ThreadPool] = []
    with _run(
        tmp_path,
        outer,
        crawl_defaults=CrawlRunConfig(extract_workers=2),
        extraction_pool=_pool(created),
    ) as (store, _result, _extractor):
        urls = _commit_order_urls(store)
        assert urls == [
            f"{ZIP_URL}!/a.md",
            f"{ZIP_URL}!/b.zip!/x.md",
            f"{ZIP_URL}!/b.zip!/y.md",
            f"{ZIP_URL}!/c.md",
        ]
        assert sorted(item.depth for item in _observations(store)) == [0, 1]
        rows = {row.member_path: row for row in _members(store)}
        assert rows["b.zip"].skip_reason == "nested_descended"
        assert rows["a.md"].doc_id is not None
        assert rows["c.md"].doc_id is not None


def test_pooled_member_failure_is_visible_and_siblings_continue(
    tmp_path: Path,
) -> None:
    """Отказ извлечения члена в воркере — видимый пропуск, соседи не страдают."""
    archive = _zip({"ok1.md": b"one", "bad.md": b"two", "ok2.md": b"three"})
    created: list[_ThreadPool] = []
    with _run(
        tmp_path,
        archive,
        crawl_defaults=CrawlRunConfig(extract_workers=2),
        extraction_pool=_pool(created, fail_urls=(f"{ZIP_URL}!/bad.md",)),
    ) as (store, result, _extractor):
        docs = _docs(store)
        assert sorted(doc.url for doc in docs) == [
            f"{ZIP_URL}!/ok1.md",
            f"{ZIP_URL}!/ok2.md",
        ]
        rows = {row.member_path: row for row in _members(store)}
        assert rows["bad.md"].skip_reason == "extract_failed"
        assert rows["bad.md"].doc_id is None
        assert rows["ok1.md"].doc_id is not None
        assert rows["ok2.md"].doc_id is not None
        root = _observations(store)[0]
        assert root.outcome["members_failed"] == 1
        assert root.outcome["members_ingested"] == 2
        assert any(
            "extract_failed" in error and "bad.md" in error for error in result.errors
        ), result.errors


def test_pooled_budget_and_cycle_match_inline(tmp_path: Path) -> None:
    """Бюджет корня и цикл по хешу — потоковые решения, от пула не зависят."""
    budget_archive = _zip(
        {"a.txt": b"A" * 1024, "b.txt": b"B" * 1024, "c.txt": b"C" * 1024}
    )
    cycle_archive = _zip(
        {
            "a/inner.zip": _zip({"x.md": b"cycle content"}),
            "b/inner.zip": _zip({"x.md": b"cycle content"}),
        }
    )
    for label, archive, base in (
        (
            "budget",
            budget_archive,
            CrawlRunConfig(archive_max_total_uncompressed_bytes=1024),
        ),
        ("cycle", cycle_archive, CrawlRunConfig()),
    ):
        with _run(tmp_path / f"{label}-inline", archive, crawl_defaults=base) as (
            inline_store,
            inline_result,
            _e,
        ):
            inline = _snapshot(inline_store, inline_result)
        created: list[_ThreadPool] = []
        with _run(
            tmp_path / f"{label}-pooled",
            archive,
            crawl_defaults=base.model_copy(update={"extract_workers": 2}),
            extraction_pool=_pool(created),
        ) as (pooled_store, pooled_result, _e2):
            pooled = _snapshot(pooled_store, pooled_result)
        # Ось ``extract_workers`` без пула даёт собственную запись в ``errors`` (не тихо);
        # сравниваются наблюдаемые последствия бюджета/цикла, а не эта запись.
        assert [e for e in pooled["errors"] if "extract_workers" not in e] == [
            e for e in inline["errors"] if "extract_workers" not in e
        ], label
        assert pooled["observations"] == inline["observations"], label
        assert pooled["docs"] == inline["docs"], label
        assert pooled["members"] == inline["members"], label


def test_pooled_descent_removes_temp_files_on_failure(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Временные файлы членов не переживают спуск — и на отказах тоже."""
    import core.pipeline.archive_descent as descent_module

    archive = _zip({"a.md": b"a", "b.md": b"b"})
    created_dirs: list[str] = []
    real_temporary_directory = tempfile.TemporaryDirectory

    def _recording(*args: Any, **kwargs: Any) -> Any:
        tmpdir = real_temporary_directory(*args, **kwargs)
        created_dirs.append(tmpdir.name)
        return tmpdir

    monkeypatch.setattr(descent_module.tempfile, "TemporaryDirectory", _recording)

    created: list[_ThreadPool] = []
    with _run(
        tmp_path,
        archive,
        crawl_defaults=CrawlRunConfig(extract_workers=2),
        extraction_pool=_pool(
            created, fail_urls=(f"{ZIP_URL}!/a.md", f"{ZIP_URL}!/b.md")
        ),
    ) as (_store, result, _extractor):
        assert _build_of(result).doc_count == 0

    assert created_dirs, "спуск обязан был создать временный каталог"
    assert all(not Path(name).exists() for name in created_dirs), created_dirs


def test_pooled_split_capitulation_stays_parent_side(tmp_path: Path) -> None:
    """``split``: член не уходит в пул вовсе — капитуляция видна и не тратит воркер."""
    archive = _zip({"a.md": b"a", "b.txt": b"b"})
    created: list[_ThreadPool] = []
    with _run(
        tmp_path,
        archive,
        crawl_defaults=CrawlRunConfig(extract_workers=2),
        extractor=_RemoteLikeExtractor(),
        extraction_pool=_pool(created),
    ) as (store, _result, extractor):
        assert extractor.calls == []  # шов не вызывается на локальных байтах
        # Члены отклонены **в родителе** до submit, поэтому пул не создан вовсе: до F5
        # в него уходила страница архива (и это был единственный submit), теперь зонд
        # артефакта идёт раньше submit — капитуляция не стоит ни одного воркера.
        assert created == []
        observation = _observations(store)[0]
        assert observation.outcome["skipped"]["extract_unavailable"]["count"] == 2
        assert all(row.skip_reason == "extract_unavailable" for row in _members(store))


def test_pooled_window_one_matches_inline(tmp_path: Path) -> None:
    """K=1 (рециклинг воркеров) — тождественно inline-пути байт-в-байт."""
    archive = _rich_archive()
    with _run(tmp_path / "inline", archive) as (inline_store, inline_result, _e):
        inline = _snapshot(inline_store, inline_result)

    created: list[_ThreadPool] = []

    pool = ExtractionPool(
        pool_factory=_pool_factory(created),
        worker=_file_worker,
        supports_worker_recycling=True,
    )
    with _run(
        tmp_path / "pooled",
        archive,
        crawl_defaults=CrawlRunConfig(extract_max_tasks_per_child=1),
        extraction_pool=pool,
    ) as (pooled_store, pooled_result, _e2):
        pooled = _snapshot(pooled_store, pooled_result)

    assert created[0].max_tasks_per_child == 1  # ось рециклинга дошла до фабрики
    assert created[0].max_inflight == 1, "окно K=1: не более одного задания в полёте"
    for key in ("docs", "chunks", "errors", "members", "observations", "probes"):
        assert pooled[key] == inline[key], key


def test_pooled_submit_failure_falls_back_to_inline(tmp_path: Path) -> None:
    """Отказ пула на ``submit`` не теряет членов: видимая запись + inline (как страница)."""
    archive = _zip({"a.md": b"a", "b.md": b"b"})

    class _BrokenPool:
        def __init__(self, size: int) -> None:
            self.size = size
            self.submitted = 0

        def submit(self, fn: Any, request: Any) -> Any:
            self.submitted += 1
            raise RuntimeError("factory boom")

        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            pass

    pool = ExtractionPool(
        pool_factory=lambda n, level=None, **_: _BrokenPool(n), worker=_file_worker
    )
    with _run(
        tmp_path,
        archive,
        crawl_defaults=CrawlRunConfig(extract_workers=2),
        extraction_pool=pool,
    ) as (store, result, extractor):
        # Члены всё равно ингестятся — inline, после явной записи о сбое пула.
        assert sorted(doc.url for doc in _docs(store)) == [
            f"{ZIP_URL}!/a.md",
            f"{ZIP_URL}!/b.md",
        ]
        assert len(extractor.calls) == 2
        # Одна запись о деградации спуска (отказ submit самой страницы архива — отдельная,
        # её пишет страничный путь).
        member_lines = [
            error for error in result.errors if "— члены контейнера inline" in error
        ]
        assert len(member_lines) == 1, result.errors
        assert all(row.skip_reason is None for row in _members(store))


# ---------------------------------------------------------------------------
# (10+) Write-through (F1): провенанс спуска durable на каждом шаге
# ---------------------------------------------------------------------------
#
# До правки ``archive_observations``/``corpus_members`` копились в памяти фазы и писались
# **одной пачкой в конце** (``corpus.py``): SIGKILL/OOM терял весь спуск, хотя документы
# уже были извлечены. Тот же класс дефекта лечили для документов (``_persist_docs`` пишет
# строку после каждого документа) — здесь то же лечение: волкер получает два необязательных
# синка и пишет строку сразу за её появлением. Проверяется наблюдаемое снаружи: содержимое
# store **после** жёсткого убийства, отсутствие дублей при повторном спуске, устойчивость
# к отказу стора и совпадение состава/порядка при окне упреждения ``K=2``.


def _member_rows_in_insert_order(store: SqliteStore) -> list[tuple[Any, ...]]:
    """Строки членов в порядке **вставки** (``rowid``): порядок записи = порядок обхода."""
    return [
        (
            row["member_path"],
            row["doc_id"],
            row["skip_reason"],
            row["extracted_content_hash"],
            row["depth"],
        )
        for row in store.conn.execute(
            "SELECT member_path, doc_id, skip_reason, extracted_content_hash, depth"
            " FROM corpus_members ORDER BY rowid"
        )
    ]


def test_kill_mid_descent_keeps_settled_member_rows(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Жёсткое убийство посреди спуска оставляет в store доведённые строки, а не ноль.

    Убийство эмулируется ``KeyboardInterrupt`` на третьем члене — это ``BaseException``,
    то есть ровно тот класс, который изоляция фазы **не** ловит (OOM/прерывание оператора).
    До write-through в store было бы ноль строк: всё копилось до конца фазы.
    """
    import core.pipeline.corpus as corpus_module

    archive = _zip({"a.md": b"a", "b.md": b"b", "c.md": b"c", "d.md": b"d"})
    real = corpus_module._ingest_member_doc
    calls: list[str] = []

    def _dying(task: Any, **kwargs: Any) -> Any:
        calls.append(task.member_path)
        if len(calls) > 2:
            raise KeyboardInterrupt("убийство после двух членов")
        return real(task, **kwargs)

    monkeypatch.setattr(corpus_module, "_ingest_member_doc", _dying)

    store = SqliteStore(":memory:")
    _make_run(store)
    try:
        with pytest.raises(KeyboardInterrupt):
            build_corpus(
                _plan(),
                _BufferedCrawler({ZIP_URL: archive}, {ZIP_URL: "application/zip"}),
                _FileExtractor(),
                _Chunker(),
                store,
                urls={"p1": [ZIP_URL]},
                raw_root=tmp_path,
                run_id=RUN_ID,
                container_for=container_for,
            )
        rows = _members(store)
        assert calls == ["a.md", "b.md", "c.md"]
        assert [row.member_path for row in rows] == ["a.md", "b.md"]
        assert all(row.doc_id is not None for row in rows)
        # Наблюдение корня не дописано: контейнер был «в полёте» — ровно это и потеряно,
        # и это видно (нет строки), а не выдаётся за пустой спуск.
        assert _observations(store) == []
    finally:
        store.close()


def test_redescent_with_write_through_does_not_duplicate_rows(tmp_path: Path) -> None:
    """Повторный спуск того же архива не растит ``count(*)``: синк — UPSERT.

    Идемпотентность обеспечена идентичностью строки (``observation_id`` детерминирован,
    ``corpus_members`` — UPSERT), а не отложенной записью: write-through её не ломает.
    """
    from core.pipeline.archive_descent import (
        ArchiveDescentWalker,
        DescentBudget,
        MemberOutcome,
    )

    archive = _zip({"a.md": b"a", "b.md": b"b", "c.md": b"c"})
    path = tmp_path / "archive.bin"
    path.write_bytes(archive)
    archive_hash = content_hash(archive)

    store = SqliteStore(":memory:")
    try:

        def _counts() -> tuple[int, int]:
            members = store.conn.execute(
                "SELECT COUNT(*) AS n FROM corpus_members"
            ).fetchone()["n"]
            observations = store.conn.execute(
                "SELECT COUNT(*) AS n FROM archive_observations"
            ).fetchone()["n"]
            return int(members), int(observations)

        def _descend() -> None:
            walker = ArchiveDescentWalker(
                product_id="p1",
                root_url=ZIP_URL,
                descent=ArchiveDescent.FULL,
                budget=DescentBudget(max_member_bytes=1024 * 1024),
                container_for=container_for,
                ingest_member=lambda task: MemberOutcome(
                    doc_id=f"p1:{task.content_hash}", content_hash=task.content_hash
                ),
                observations=[],
                members=[],
                errors=[],
                run_id=RUN_ID,
                persist_observation=store.save_archive_observation,
                persist_member=lambda row: store.save_corpus_members([row]),
            )
            walker.descend(
                archive_path=path,
                archive_url=ZIP_URL,
                archive_content_hash=archive_hash,
            )

        _descend()
        first = _counts()
        assert first == (3, 1)  # три члена + наблюдение корня
        _descend()
        assert _counts() == first
        assert [row.member_path for row in _members(store)] == ["a.md", "b.md", "c.md"]
    finally:
        store.close()


def test_store_failure_does_not_kill_descent(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Отказ стора изолирован в ``errors``: спуск доходит до конца, потеря видима.

    Провенанс важнее потерянной строки, но падение фазы хуже всего: синк пишет ошибку и
    живёт дальше — как ``_persist_docs``. Финальный reconciliation-pass падает так же
    видимо, а не тихо.
    """
    archive = _zip({"a.md": b"a", "b.md": b"b"})

    def _boom(self: Any, members: Any) -> None:
        raise RuntimeError("store down")

    monkeypatch.setattr(SqliteStore, "save_corpus_members", _boom)

    with _run(tmp_path, archive) as (store, result, extractor):
        assert sorted(doc.url for doc in _docs(store)) == [
            f"{ZIP_URL}!/a.md",
            f"{ZIP_URL}!/b.md",
        ]
        assert len(extractor.calls) == 2
        assert _members(store) == []
        per_member = [
            error for error in result.errors if error.startswith("save_corpus_members(")
        ]
        assert len(per_member) == 2, result.errors
        assert any(
            error.startswith("corpus_members:") for error in result.errors
        ), result.errors
        # Наблюдения spуска пишутся другим синком и отказа не видят.
        assert len(_observations(store)) == 1


def test_write_through_window_matches_inline_order(tmp_path: Path) -> None:
    """``K=2`` пишет **тот же** состав строк в том же порядке, что inline (write-through).

    Порядок вставки (``rowid``) — доказательство, что синк идёт по обходу, а не по факту
    завершения воркера: иначе ``corpus_members`` описывал бы другой порядок, чем документы.
    """
    archive = _rich_archive()

    with _run(tmp_path / "inline", archive) as (inline_store, _r, _e):
        inline = _member_rows_in_insert_order(inline_store)

    created: list[_ThreadPool] = []
    with _run(
        tmp_path / "pooled",
        archive,
        crawl_defaults=CrawlRunConfig(extract_workers=2),
        extraction_pool=_pool(created),
    ) as (pooled_store, _r2, _e2):
        pooled = _member_rows_in_insert_order(pooled_store)

    assert created[0].max_inflight <= 2
    assert pooled == inline
    paths = [row[0] for row in pooled]
    assert paths == [
        "a/first.pdf",
        "b/readme.md",
        "c/copy.md",
        "d/copy.md",
        "f/inner.zip",
        "nested/one.md",
        "nested/two.md",
        "g/last.txt",
    ], paths

# ---------------------------------------------------------------------------
# Частичный член: «прочитал часть» — законный исход, видимый в корпусе и наблюдении
# ---------------------------------------------------------------------------

class _PartialMemberExtractor(_FileExtractor):
    """Как ``_FileExtractor``, но PDF-член читается **частично** (1 страница из 7).

    Ровно то, что присылает docling на ``PARTIAL_SUCCESS``: текст прочитанной части есть.
    Бинарный исход члена («ингестнут») без признака выдавал бы это за полное чтение.
    """

    version = "extractor-descent-partial-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        doc = super().extract(page, route)
        if not page.url.endswith(".pdf"):
            return doc
        return dataclasses.replace(
            doc,
            page_count=7,
            partial=True,
            partial_reason="docling: PARTIAL_SUCCESS, получено страниц 1 из 7",
            pages_converted=1,
        )

def test_partial_member_is_visible_in_corpus_and_observation(tmp_path: Path) -> None:
    """Частично прочитанный член: документ в корпусе, признак в документе и в исходе спуска.

    Проверяется весь путь: извлечение члена → ``corpus_docs.partial`` (свидетельство) →
    ``MemberOutcome.partial`` → ``archive_observations.outcome.members_partial`` →
    ``CorpusCoverage.complete=False``. До этого частичный член выглядел как полностью
    прочитанный: ``members_ingested`` рос, а признака не было нигде.
    """
    archive = _zip(
        {
            "docs/manual.pdf": _pdf(3),
            "notes/readme.md": b"# hello\nbody\n",
        }
    )
    with _run(tmp_path, archive, extractor=_PartialMemberExtractor()) as (
        store,
        _result,
        _extractor,
    ):
        docs = {doc.url: doc for doc in _docs(store)}
        pdf = docs[f"{ZIP_URL}!/docs/manual.pdf"]
        assert pdf.partial is True
        assert (pdf.pages_converted, pdf.page_count) == (1, 7)
        assert "PARTIAL_SUCCESS" in (pdf.partial_reason or "")
        # Сосед прочитан целиком — признак не размазывается по всем членам архива.
        md = docs[f"{ZIP_URL}!/notes/readme.md"]
        assert md.partial is False

        # Строка члена бинарна (doc_id/skip_reason), а свидетельство частичности —
        # сама строка документа выше; спуск называет частичные члены отдельным счётчиком.
        rows = {row.member_path: row for row in _members(store)}
        assert rows["docs/manual.pdf"].doc_id is not None
        assert rows["docs/manual.pdf"].skip_reason is None

        root = _observations(store)[0]
        assert root.outcome["members_ingested"] == 2
        assert root.outcome["members_partial"] == 1
        assert root.outcome["partial"] == {
            "docling: PARTIAL_SUCCESS, получено страниц 1 из 7": {
                "count": 1,
                "examples": ["docs/manual.pdf"],
            }
        }

        coverage = store.read_corpus_coverage(RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is False
        assert coverage.blocked == {"partial_extraction": 1}


def _page_bytes(page: Any) -> bytes:
    """Байты страницы: тело или retained-файл (потребитель очереди не держит тело в RAM).

    До F5 конверсия шла внутри краула, и ``RawPage.content`` страницы был непуст. Теперь
    документ конвертируется из строки очереди: ``content`` пуст, тело лежит по
    ``source_path`` (I19, ``corpus_raw/{hash}.bin``) — ровно так его получает воркер пула.
    Фейковый экстрактор обязан читать тот же носитель, что и адаптер, иначе он проверял бы
    не контракт порта, а прежний способ доставки байтов.
    """
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return pathlib.Path(path).read_bytes() if path else b""
