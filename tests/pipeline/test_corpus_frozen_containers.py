"""``frozen`` воспроизводит **членов контейнеров** из сохранённых байтов (план 19 §E.7).

Прежде проход без сети пере-извлекал только верхнеуровневые документы: путь и хеш байтов
архива жили в БД прогона (``archive_observations``/``corpus_members``), а ``url_state``/
``document_version`` про контейнер не знают (версии документа у него нет — он попадал в
``no_version``). Члены приходили только из живого ``ArchiveDescentWalker``. Замер стенда:
``auto`` 9 документов/597 чанков, ``frozen`` 7/326 — не хватало 2 членов (271 чанк).

Проверяется:

* живой спуск пишет durable-наблюдение контейнера (``container_state``, STATE 10);
* ``frozen`` на подготовленном субстрате даёт **тех же** членов (``doc_id``, текст
  чанков), что и живой прогон, при краулере, падающем на любом вызове; второй ``frozen``
  даёт тот же результат (идемпотентность);
* контейнер в состоянии без файла байтов — ``stored_bytes_missing``, не ``failed`` (I4);
* ``no_version`` остаётся для URL без версии **и** без контейнерной записи.

Субстрат наполняется **реальным** путём (прогон ``auto`` с настоящим zip и настоящим
читателем контейнеров), а не собран руками. Данные нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import io
import pathlib
import zipfile
from pathlib import Path
from typing import Any

from core.adapters.container_any import container_for
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CorpusDoc,
    CrawlRunConfig,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import IndexBuild, build_corpus
from core.ports.state import UrlStateRow
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore
from core.util.hashing import content_hash

RUN_ID = "run-frozen-containers"
TS = "2026-01-01T00:00:00+00:00"
ZIP_URL = "https://docs.test/files/bundle.zip"
PAGE_URL = "https://docs.test/p1/print"


# ---------------------------------------------------------------------------
# Порты прогона: краулеры, экстрактор, чанкер (тот же каркас, что у прочих тестов)
# ---------------------------------------------------------------------------


class _BufferedCrawler:
    """Наполняющий прогон: тело в памяти (архив невелик, staging не нужен)."""

    version = "crawler-1"
    engine = "http"

    def __init__(self, pages: dict[str, bytes], content_types: dict[str, str]) -> None:
        self.pages = dict(pages)
        self.content_types = dict(content_types)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        self.calls.append(url)
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            content_type=self.content_types.get(url),
            lang="en",
            status_code=200,
        )


class _NoNetworkCrawler:
    """Краулер, падающий на **любом** вызове: доказательство отсутствия сети."""

    version = "crawler-1"
    engine: str | None = None

    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        self.calls.append(url)
        raise AssertionError(f"frozen: сетевой вызов краулера по {url!r}")


class _FileExtractor:
    """Экстрактор-заглушка: читает **файл** по ``page.source_path`` (I19).

    Хеш считается от байтов файла, поэтому члены с разным текстом дают разные
    ``content_hash`` — дедуп не схлопывает их в один документ.
    """

    version = "extractor-descent-1"
    supports_source_path = True

    def __init__(self) -> None:
        self.calls: list[str] = []

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls.append(page.url)
        data = (
            Path(page.source_path).read_bytes()
            if page.source_path
            else page.content
        )
        return ExtractedDoc(
            text=data.decode("utf-8", errors="replace"),
            content_hash=content_hash(data),
            extractor_version=self.version,
        )


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


# ---------------------------------------------------------------------------
# План/стор/субстрат
# ---------------------------------------------------------------------------


def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a"],
        params=[
            CatalogRow(
                param_id="param_a",
                title="Param A",
                module="mod_a",
                group="grp_1",
                importance="Кр",
                param_type="fact",
                rule="presence of mechanism a",
                absence_rule="registry_absence",
                role_binding="role_a",
            )
        ],
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


def _zip(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return buffer.getvalue()


def _build_of(result: Any) -> IndexBuild:
    builds = [entry for entry in result.log_entries if isinstance(entry, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def _chunks(store: SqliteStore, result: Any) -> list[tuple[str, str]]:
    """``(chunk_id, text)`` сборки — сравнение идёт по составу, не по порядку готовности.

    Выборка — по ``partition_build_id``: ``corpus_chunks.build_id`` несёт её, а не
    ``index_build_id`` композиции (у таблицы нет ``run_id``, ключ — партиция).
    """
    build = _build_of(result)
    if not build.entries:
        # Продукт без документов не даёт партиции — пустой состав, а не ошибка теста.
        return []
    (entry,) = build.entries
    return sorted(
        (chunk.chunk_id, chunk.text)
        for chunk in store.iter_chunks(entry.partition_build_id)
    )


def _frozen_event(result: Any) -> dict[str, Any]:
    """``frozen``-блок события ``product_end`` — факты прохода, которые видит оператор."""
    events = [
        entry
        for entry in result.log_entries
        if isinstance(entry, dict) and entry.get("event") == "product_end"
    ]
    assert events, "нет события product_end"
    payload = events[0].get("frozen")
    assert isinstance(payload, dict), f"нет frozen-отчёта в product_end: {events[0]}"
    return payload


def _seed(
    tmp_path: Path, state: SqliteStateStore, archive: bytes
) -> tuple[list[CorpusDoc], list[tuple[str, str]]]:
    """Наполняет субстрат **живым** прогоном ``auto`` (настоящий zip, настоящий спуск)."""
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _BufferedCrawler({ZIP_URL: archive}, {ZIP_URL: "application/zip"})
    try:
        result = build_corpus(
            _plan(),
            crawler,
            _FileExtractor(),
            _Chunker(),
            store,
            urls={"p1": [ZIP_URL]},
            raw_root=tmp_path / "raw",
            run_id=RUN_ID,
            state=state,
            product_modes={"p1": "incremental"},
            refresh_policy="auto",
            container_for=container_for,
        )
        assert crawler.calls, "наполняющий прогон не фетчил — субстрат не создан"
        return list(store.iter_corpus_docs(RUN_ID)), _chunks(store, result)
    finally:
        store.close()


def _frozen(
    tmp_path: Path,
    state: SqliteStateStore,
    *,
    roots: list[str],
    container_reader: Any = None,
) -> tuple[Any, SqliteStore, _NoNetworkCrawler]:
    """``frozen``-прогон с краулером, падающим на любом вызове."""
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _NoNetworkCrawler()
    result = build_corpus(
        _plan(),
        crawler,
        _FileExtractor(),
        _Chunker(),
        store,
        urls={"p1": list(roots)},
        raw_root=tmp_path / "raw",
        run_id=RUN_ID,
        state=state,
        product_modes={"p1": "incremental"},
        refresh_policy="frozen",
        crawl_defaults=CrawlRunConfig(host_concurrency=2),
        container_for=container_reader or container_for,
    )
    return result, store, crawler


def _archive() -> bytes:
    return _zip({"a.md": b"member alpha text", "b.txt": b"member beta text"})


# ---------------------------------------------------------------------------
# Тесты
# ---------------------------------------------------------------------------


def test_live_descent_writes_container_state(tmp_path: Path) -> None:
    """Живой спуск пишет durable-наблюдение: путь, хеш байтов, формат и исход."""
    archive = _archive()
    with SqliteStateStore(":memory:") as state:
        _seed(tmp_path, state, archive)

        row = state.get_container_state("p1", ZIP_URL)
        assert row is not None, "спуск не записал container_state"
        assert row.url == ZIP_URL
        assert row.archive_content_hash == content_hash(archive), (
            "хеш архива — хеш ретейнённых байтов (имя файла retention), а не текста"
        )
        assert pathlib.Path(row.raw_path).is_file()
        assert row.raw_path.endswith(f"{row.archive_content_hash}.bin")
        assert row.format == "zip"
        assert row.status == "descended"


def test_frozen_replays_container_members_idempotently(tmp_path: Path) -> None:
    """``frozen`` даёт тех же членов, что живой прогон, и повторяем без сети."""
    archive = _archive()
    with SqliteStateStore(":memory:") as state:
        live_docs, live_chunks = _seed(tmp_path, state, archive)
        assert live_docs, "живой спуск не дал документов — сравнивать нечего"
        assert {doc.doc_id for doc in live_docs} == {
            f"p1:{content_hash(b'member alpha text')}",
            f"p1:{content_hash(b'member beta text')}",
        }

        first, store1, crawler1 = _frozen(tmp_path, state, roots=[ZIP_URL])
        try:
            assert crawler1.calls == [], "frozen обязан не трогать сеть"
            docs1 = list(store1.iter_corpus_docs(RUN_ID))
            assert [doc.doc_id for doc in docs1] == [doc.doc_id for doc in live_docs]
            chunks1 = _chunks(store1, first)
            assert chunks1 == live_chunks, (
                "членов контейнера нет в корпусе frozen или их текст другой"
            )
            # Строки сеанса не пишутся: frozen — не фетч.
            assert list(store1.iter_crawl_session_items("crawl:" + RUN_ID)) == []
            report = _frozen_event(first)
            assert report["containers"] == 1
            assert report["container_docs"] == len(live_docs)
            assert report["container_chunks"] == len(live_chunks)
            assert report["docs"] == len(live_docs)
            assert any(
                "frozen_container_replayed:1" in error for error in first.errors
            ), f"нет видимого факта воспроизведения: {first.errors}"
        finally:
            store1.close()

        # Второй проход — из того же состояния и тех же байтов: результат тот же.
        second, store2, crawler2 = _frozen(tmp_path, state, roots=[ZIP_URL])
        try:
            assert crawler2.calls == []
            docs2 = list(store2.iter_corpus_docs(RUN_ID))
            assert [doc.doc_id for doc in docs2] == [doc.doc_id for doc in docs1]
            assert _chunks(store2, second) == chunks1
        finally:
            store2.close()


def test_frozen_container_without_bytes_is_visible_not_failed(tmp_path: Path) -> None:
    """Контейнер в состоянии без файла байтов — ``stored_bytes_missing``, не ``failed``."""
    archive = _archive()
    with SqliteStateStore(":memory:") as state:
        _seed(tmp_path, state, archive)
        row = state.get_container_state("p1", ZIP_URL)
        assert row is not None
        Path(row.raw_path).unlink()

        result, store, crawler = _frozen(tmp_path, state, roots=[ZIP_URL])
        try:
            assert crawler.calls == []
            assert list(store.iter_corpus_docs(RUN_ID)) == []
            assert any(
                "stored_bytes_missing:1" in error for error in result.errors
            ), f"нет видимой причины с числом: {result.errors}"
            report = _frozen_event(result)
            assert report["stored_bytes_missing"] == [ZIP_URL]
            assert report["containers"] == 0
            assert report["container_unreadable"] == []
            # I4: проход состоялся — продукт не объявлен ``failed``.
            assert store.read_corpus_coverage(RUN_ID, "p1") is not None
        finally:
            store.close()


def test_frozen_container_unreadable_is_a_named_token(tmp_path: Path) -> None:
    """Читателя контейнеров нет — ``frozen_container_unreadable``, а не тишина и не ``failed``."""
    archive = _archive()
    with SqliteStateStore(":memory:") as state:
        _seed(tmp_path, state, archive)

        result, store, crawler = _frozen(
            tmp_path,
            state,
            roots=[ZIP_URL],
            # Читатель не подключён: индекс не прочитан, но байты на месте и их хеш сходится.
            container_reader=lambda _path: None,
        )
        try:
            assert crawler.calls == []
            assert list(store.iter_corpus_docs(RUN_ID)) == []
            report = _frozen_event(result)
            assert report["containers"] == 0
            assert report["container_unreadable"] == [ZIP_URL]
            assert any(
                "frozen_container_unreadable" in error and ZIP_URL in error
                for error in result.errors
            ), f"нет видимой причины нечитаемого контейнера: {result.errors}"
            # ``stored_bytes_missing`` здесь именно НЕТ: байты есть, не прочитан индекс.
            assert report["stored_bytes_missing"] == []
        finally:
            store.close()


def test_frozen_container_without_ingestible_members_is_named_empty(
    tmp_path: Path,
) -> None:
    """Спуск исполнен, но членов в корпусе нет — ``frozen_container_empty``, не ``failed``."""
    archive = _zip({"picture.png": b"\x89PNG\r\n\x1a\nnot-a-document"})
    with SqliteStateStore(":memory:") as state:
        _seed(tmp_path, state, archive)

        result, store, crawler = _frozen(tmp_path, state, roots=[ZIP_URL])
        try:
            assert crawler.calls == []
            assert list(store.iter_corpus_docs(RUN_ID)) == []
            report = _frozen_event(result)
            assert report["containers"] == 1, "спуск исполнен — контейнер воспроизведён"
            assert report["container_docs"] == 0
            assert report["container_empty"] == [ZIP_URL]
            assert any(
                "frozen_container_empty" in error and ZIP_URL in error
                for error in result.errors
            ), f"нет видимой причины пустого спуска: {result.errors}"
        finally:
            store.close()


def test_frozen_keeps_no_version_for_url_without_container_record(
    tmp_path: Path,
) -> None:
    """URL без версии документа и без контейнерной записи остаётся ``no_version``."""
    archive = _archive()
    with SqliteStateStore(":memory:") as state:
        _seed(tmp_path, state, archive)
        state.upsert_url_state(
            UrlStateRow(
                product_id="p1",
                url=PAGE_URL,
                normalized_url=PAGE_URL,
                doc_version_id=None,
            )
        )

        result, store, crawler = _frozen(tmp_path, state, roots=[ZIP_URL, PAGE_URL])
        try:
            assert crawler.calls == []
            report = _frozen_event(result)
            assert report["no_version"] == [PAGE_URL]
            assert report["containers"] == 1, "контейнер всё равно воспроизведён"
            assert any(
                PAGE_URL in error and "нет активной версии" in error
                for error in result.errors
            ), f"нет видимой причины no_version: {result.errors}"
        finally:
            store.close()