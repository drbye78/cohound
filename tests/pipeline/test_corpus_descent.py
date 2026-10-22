"""Спуск в контейнеры: исполнение, вложенность, бюджет, цикл, видимость (B3).

Тесты работают на **настоящих** zip-архивах, собранных ``zipfile`` в ``tmp_path``: спуск
проверяется по фактическому индексу (central directory), а не по моку. Экстрактор —
заглушка, читающая байты по ``page.source_path`` (диск-первый контракт I19): так видно,
что член попадает в экстрактор **своим** файлом, а не буфером.

Проверяется:

* zip с pdf+md+png: pdf и md ингестятся **разными** ``CorpusDoc``, png — видимый пропуск
  с причиной (``not_ingestible_format``), прогноз фиксируется до извлечения;
* вложенность 3 уровня ингестится — потолка глубины нет;
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
import datetime as _dt
import io
import json
import zipfile
from pathlib import Path
from typing import Any, Iterator

from core.adapters.container_any import container_for
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
from core.pipeline import RunPlan
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.crawl_session import default_session_id
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

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
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
            else page.content
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
) -> Iterator[tuple[SqliteStore, Any, Any]]:
    """Один прогон фазы корпуса над одним архивом; читатель контейнеров — реальный."""
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
            container_for=container_for,
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
        build = _build_of(result)
        urls = sorted(doc.url for doc in build.docs)
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
        raw_paths = {doc.raw_path for doc in build.docs}
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

    with _run(tmp_path, outer) as (store, result, _extractor):
        docs = _build_of(result).docs
        assert len(docs) == 1
        assert docs[0].url == f"{ZIP_URL}!/mid.zip!/inner.zip!/deep.md"
        # Наблюдение на каждый уровень: 0 — корень, 1 — mid.zip, 2 — inner.zip.
        observations = _observations(store)
        assert sorted(item.depth for item in observations) == [0, 1, 2]
        assert all(
            item.status is ArchiveDescentStatus.DESCENDED for item in observations
        )


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

    with _run(tmp_path, outer) as (store, result, _extractor):
        docs = _build_of(result).docs
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
    ) as (store, result, _extractor):
        docs = _build_of(result).docs
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
        assert _build_of(result).docs == []
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
        assert _build_of(result).docs == []
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
        docs = _build_of(result).docs
        # Первый член уложился ровно в бюджет, остальные — остановлены.
        assert len(docs) == 1
        observation = _observations(store)[0]
        assert observation.status is ArchiveDescentStatus.BUDGET_EXHAUSTED
        assert observation.outcome["stopped_reason"] == "archive_budget_exhausted"
        assert observation.outcome["budget_limit_bytes"] == 1024
        assert any(
            "archive_budget_exhausted" in error for error in result.errors
        ), "исчерпание бюджета обязано быть видимым, а не тихим"


# ---------------------------------------------------------------------------
# (8) split: член не извлекается молча
# ---------------------------------------------------------------------------


def test_extractor_without_local_bytes_skips_members_visibly(tmp_path: Path) -> None:
    archive = _zip({"a.md": b"a", "b.txt": b"b"})
    with _run(
        tmp_path, archive, extractor=_RemoteLikeExtractor()
    ) as (store, result, extractor):
        assert extractor.calls == []  # шов не вызывается на локальных байтах
        assert _build_of(result).docs == []
        observation = _observations(store)[0]
        assert observation.status is ArchiveDescentStatus.DESCENDED
        assert observation.outcome["skipped"]["extract_unavailable"]["count"] == 2
        assert any("extract_unavailable" in error for error in result.errors)
        rows = _members(store)
        assert len(rows) == 2
        assert all(row.skip_reason == "extract_unavailable" for row in rows)
