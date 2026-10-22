"""Допуск артефактов-контейнеров в корпусе (P6, без спуска).

Проверяются: (1) staging-артефакт публикуется в ``corpus_raw`` **переездом** файла
(байт-идентично, ``content_hash`` — потоковый хеш тех же байтов), staging-файл исчезает,
экстрактор не вызывается (контейнер — не документ; спуск — следующий слой); (2) класс-aware
resume: retained-артефакт 30 MiB резюмится без фетча и без чтения в RAM, а тот же файл
без заявленного класса — терминальный ``payload_too_large``; (3) объявленный артефакт с
бинарным мусором — ``unsupported_binary`` (класс не отменяет проверку байтов);
(4) OOXML (ZIP с ``[Content_Types].xml``) — документ, а не контейнер: извлекается.
Данные нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
from pathlib import Path
from typing import Any

from core.domain import (
    AnchorRelation,
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
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.util.document_formats import BINARY_SNIFF_BYTES
from core.util.hashing import content_hash

RUN_ID = "run-artifacts"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"
ROOT = "https://docs.test/"
ZIP_URL = "https://docs.test/files/bundle.zip"
DOCX_URL = "https://docs.test/files/spec.docx"
MIB = 1024 * 1024
ZIP_MAGIC = b"PK\x03\x04"
OOXML_MARKER = b"[Content_Types].xml"


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
    """Прежний путь: тело в памяти (страница или маленький контейнер)."""

    version = "crawler-1"
    engine = "http"

    def __init__(self, pages: dict[str, bytes], content_types: dict[str, str] | None = None):
        self.pages = dict(pages)
        self.content_types = dict(content_types or {})
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


class _StagingCrawler:
    """Файловый staging: пишет тело чанками, возвращает ``staged_path`` (RAM не растёт)."""

    version = "crawler-1"
    engine = "http"

    def __init__(
        self,
        url: str,
        *,
        total: int,
        staging_dir: Path,
        content_type: str = "application/zip",
        magic: bytes = ZIP_MAGIC,
    ) -> None:
        self.url = url
        self.total = total
        self.staging_dir = staging_dir
        self.content_type = content_type
        self.magic = magic
        self.calls: list[str] = []
        self.digest = ""
        self.path: Path | None = None

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        assert url == self.url
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        path = self.staging_dir / f"incoming-{len(self.calls)}.part"
        hasher = hashlib.sha256()
        with path.open("wb") as handle:
            written = 0
            while written < self.total:
                size = min(MIB, self.total - written)
                chunk = bytes(size)
                if written == 0:
                    chunk = self.magic + chunk[len(self.magic) :]
                handle.write(chunk)
                hasher.update(chunk)
                written += size
        self.digest = hasher.hexdigest()
        self.path = path
        with path.open("rb") as handle:
            head = handle.read(BINARY_SNIFF_BYTES)
        return RawPage(
            url=url,
            content=b"",
            fetched_at=TS,
            fetch_mode=plan.fetch,
            content_type=self.content_type,
            lang="en",
            staged_path=str(path),
            head=head,
        )


class _ExplodingCrawler:
    """Живого фетча быть не должно: любой вызов — провал теста."""

    version = "crawler-1"
    engine = "http"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        raise AssertionError(f"живой фетч при resume: {url}")


class _Extractor:
    version = "extractor-1"

    def __init__(self) -> None:
        self.calls = 0

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        return ExtractedDoc(
            text=page.content.decode("utf-8", errors="replace"),
            content_hash=content_hash(page.content),
            extractor_version=self.version,
            page_count=1,
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


def _build(
    tmp_path: Path,
    *,
    crawler: Any,
    urls: list[str],
    store: SqliteStore,
    extractor: Any = None,
    crawl_defaults: CrawlRunConfig | None = None,
):
    return build_corpus(
        _plan(),
        crawler,
        _Extractor() if extractor is None else extractor,
        _Chunker(),
        store,
        urls={"p1": list(urls)},
        raw_root=tmp_path,
        run_id=RUN_ID,
        crawl_defaults=crawl_defaults,
    )


def _items(store: SqliteStore) -> dict[str, Any]:
    return {i.url: i for i in store.iter_crawl_session_items(SESSION_ID)}


def _build_of(result: Any) -> IndexBuild:
    builds = [e for e in result.log_entries if isinstance(e, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


# ---------------------------------------------------------------------------
# (1) Staging-артефакт: публикация переездом, без экстракции
# ---------------------------------------------------------------------------


def test_staged_artifact_is_retained_byte_identical_without_extraction(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "incoming"
    crawler = _StagingCrawler(ZIP_URL, total=2 * MIB, staging_dir=staging)
    extractor = _Extractor()
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path,
            crawler=crawler,
            urls=[ZIP_URL],
            store=store,
            extractor=extractor,
        )

        item = _items(store)[ZIP_URL]
        assert item.status is CrawlItemStatus.FETCHED
        # Хеш — потоковый SHA-256 тех же байтов, что лежат в corpus_raw (I19).
        assert item.content_hash == crawler.digest
        assert item.raw_path is not None
        raw = Path(item.raw_path)
        assert raw.name == f"{crawler.digest}.bin"
        assert raw.parent == tmp_path / "p1"
        assert raw.stat().st_size == 2 * MIB
        assert content_hash(raw.read_bytes()) == crawler.digest
        # Staging-файл переехал (os.replace), а не скопирован: каталог пуст.
        assert not any(staging.iterdir())
        # Контейнер — не документ: экстрактор не звался, спуск — следующий слой.
        assert extractor.calls == 0
        assert _build_of(result).docs == []
        assert any("attachment_skip" in e for e in result.errors)
    finally:
        store.close()


def test_small_buffered_container_is_skipped_not_parsed(tmp_path: Path) -> None:
    """Маленький zip (≤ потолка страницы) — тот же класс: skip по байтам, не парсинг."""
    body = ZIP_MAGIC + b"\x14\x00\x00\x00" + bytes(1024)
    crawler = _BufferedCrawler({ZIP_URL: body}, {ZIP_URL: "application/zip"})
    extractor = _Extractor()
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path, crawler=crawler, urls=[ZIP_URL], store=store, extractor=extractor
        )

        item = _items(store)[ZIP_URL]
        assert item.status is CrawlItemStatus.FETCHED
        assert item.content_hash == content_hash(body)
        assert extractor.calls == 0
        assert any("attachment_skip" in e for e in result.errors)
    finally:
        store.close()


def test_ooxml_zip_is_a_document_not_a_container(tmp_path: Path) -> None:
    """``[Content_Types].xml`` в голове ⇒ документ: обычная экстракция, не skip."""
    body = ZIP_MAGIC + b"\x14\x00\x00\x00" + OOXML_MARKER + bytes(1024)
    crawler = _BufferedCrawler(
        {DOCX_URL: body},
        {DOCX_URL: "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
    )
    extractor = _Extractor()
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path, crawler=crawler, urls=[DOCX_URL], store=store, extractor=extractor
        )

        assert extractor.calls == 1
        assert len(_build_of(result).docs) == 1
        assert not any("attachment_skip" in e for e in result.errors)
    finally:
        store.close()


# ---------------------------------------------------------------------------
# (2) Класс-aware resume: 30 MiB retained-артефакт
# ---------------------------------------------------------------------------


def _retain_large_artifact(tmp_path: Path) -> tuple[SqliteStore, str]:
    """Первый прогон: 30 MiB zip ретейнён (staging → corpus_raw)."""
    staging = tmp_path / "incoming"
    crawler = _StagingCrawler(ZIP_URL, total=30 * MIB, staging_dir=staging)
    store = SqliteStore(":memory:")
    _make_run(store)
    extractor = _Extractor()
    result = _build(
        tmp_path, crawler=crawler, urls=[ZIP_URL], store=store, extractor=extractor
    )
    item = _items(store)[ZIP_URL]
    assert item.status is CrawlItemStatus.FETCHED
    assert item.content_hash == crawler.digest
    assert extractor.calls == 0
    assert any("attachment_skip" in e for e in result.errors)
    return store, crawler.digest


def test_retained_artifact_resumes_without_fetch(tmp_path: Path) -> None:
    store, digest = _retain_large_artifact(tmp_path)
    try:
        item = _items(store)[ZIP_URL]
        raw = Path(item.raw_path)
        assert raw.stat().st_size == 30 * MIB

        crawler = _ExplodingCrawler()
        extractor = _Extractor()
        result = _build(
            tmp_path, crawler=crawler, urls=[ZIP_URL], store=store, extractor=extractor
        )

        assert crawler.calls == []
        resumed = _items(store)[ZIP_URL]
        # Артефакт вправе быть больше in-memory потолка страницы: класс-aware resume.
        assert resumed.status is CrawlItemStatus.FETCHED
        assert resumed.content_hash == digest
        assert resumed.raw_path == str(raw)
        assert raw.is_file()
        assert not any("payload_too_large" in e for e in result.errors)
        assert extractor.calls == 0
    finally:
        store.close()


def test_retained_artifact_without_declared_class_is_payload_too_large(
    tmp_path: Path,
) -> None:
    store, _digest = _retain_large_artifact(tmp_path)
    try:
        item = _items(store)[ZIP_URL]
        raw = Path(item.raw_path)
        assert raw.is_file()

        crawler = _ExplodingCrawler()
        # Класс не заявлен: потолок — страничный (20 MiB), 30 MiB ⇒ терминальный отказ.
        result = _build(
            tmp_path,
            crawler=crawler,
            urls=[ZIP_URL],
            store=store,
            crawl_defaults=CrawlRunConfig(attachment_formats=[]),
        )

        assert crawler.calls == []
        rejected = _items(store)[ZIP_URL]
        assert rejected.status is CrawlItemStatus.FAILED
        assert rejected.error_kind == FailureKind.PAYLOAD_TOO_LARGE.value
        assert rejected.raw_path is None
        assert not raw.exists()
        assert any("payload_too_large" in e for e in result.errors)
    finally:
        store.close()


# ---------------------------------------------------------------------------
# (3) Объявленный артефакт с бинарным мусором — unsupported_binary
# ---------------------------------------------------------------------------


def test_declared_artifact_with_garbage_bytes_is_unsupported_binary(
    tmp_path: Path,
) -> None:
    body = ZIP_MAGIC + b"\x14\x00\x00\x00" + bytes(512)
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        _build(
            tmp_path,
            crawler=_BufferedCrawler({ZIP_URL: body}, {ZIP_URL: "application/zip"}),
            urls=[ZIP_URL],
            store=store,
        )
        item = _items(store)[ZIP_URL]
        raw = Path(item.raw_path)
        garbage = bytes(2048)
        raw.write_bytes(garbage)
        store.upsert_crawl_session_item(
            item.model_copy(update={"content_hash": content_hash(garbage)})
        )

        crawler = _ExplodingCrawler()
        # Страничный потолок 1024 < 2048: без класса это был бы payload_too_large.
        result = _build(
            tmp_path,
            crawler=crawler,
            urls=[ZIP_URL],
            store=store,
            crawl_defaults=CrawlRunConfig(max_response_bytes=1024),
        )

        assert crawler.calls == []
        rejected = _items(store)[ZIP_URL]
        assert rejected.status is CrawlItemStatus.FAILED
        assert rejected.error_kind == FailureKind.UNSUPPORTED_BINARY.value
        assert rejected.raw_path is None
        assert not raw.exists()
        assert any("unsupported_binary" in e for e in result.errors)
    finally:
        store.close()


# ---------------------------------------------------------------------------
# (4) P6b: документ — тоже терминальный артефакт (потолок допуска и resume)
# ---------------------------------------------------------------------------

PDF_URL = "https://docs.test/files/manual.pdf"
PDF_MAGIC = b"%PDF-1.7\n"


class _DocumentExtractor:
    """Экстрактор-заглушка: не материализует текст из десятков MiB (тест — про допуск).

    Записывает **оба** источника: размер ``page.content`` (сколько байтов поднял в RAM
    вызывающий) и размер файла по ``page.source_path`` (что реально предложено
    экстрактору). Диск-первый контракт — ``content`` пуст, ``source_path`` ведёт на
    retained-файл (I19); материализация в байты была бы регрессом RAM.
    """

    version = "extractor-1"

    def __init__(self) -> None:
        self.calls = 0
        self.sizes: list[int] = []
        self.source_sizes: list[int] = []

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        self.sizes.append(len(page.content))
        self.source_sizes.append(
            Path(page.source_path).stat().st_size if page.source_path else 0
        )
        return ExtractedDoc(
            text="document text",
            content_hash=content_hash(b"document text"),
            extractor_version=self.version,
            page_count=1,
        )


def test_staged_pdf_is_retained_and_extracted(tmp_path: Path) -> None:
    """25 MiB PDF (> страничных 20 MiB) ретейнится и **извлекается**, а не падает."""
    staging = tmp_path / "incoming"
    crawler = _StagingCrawler(
        PDF_URL,
        total=25 * MIB,
        staging_dir=staging,
        content_type="application/pdf",
        magic=PDF_MAGIC,
    )
    extractor = _DocumentExtractor()
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path, crawler=crawler, urls=[PDF_URL], store=store, extractor=extractor
        )

        item = _items(store)[PDF_URL]
        assert item.status is CrawlItemStatus.FETCHED
        assert item.content_hash == crawler.digest
        raw = Path(item.raw_path)
        assert raw.stat().st_size == 25 * MIB
        # Staging переехал, документ извлечён (не «контейнерный» skip).
        assert not any(staging.iterdir())
        assert extractor.calls == 1
        # Экстрактору предложен **retained-файл** (I19), а не пустой staging-буфер:
        # 25 MiB по пути, и при этом ноль байтов поднято в RAM вызывающего.
        assert extractor.source_sizes == [25 * MIB]
        assert extractor.sizes == [0]
        assert len(_build_of(result).docs) == 1
        assert not any("payload_too_large" in e for e in result.errors)
        assert not any("attachment_skip" in e for e in result.errors)
    finally:
        store.close()


def test_retained_pdf_resumes_under_artifact_ceiling(tmp_path: Path) -> None:
    """Класс-aware resume: retained PDF 25 MiB резюмится, а не режется страничным потолком."""
    staging = tmp_path / "incoming"
    first = _StagingCrawler(
        PDF_URL,
        total=25 * MIB,
        staging_dir=staging,
        content_type="application/pdf",
        magic=PDF_MAGIC,
    )
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        _build(
            tmp_path,
            crawler=first,
            urls=[PDF_URL],
            store=store,
            extractor=_DocumentExtractor(),
        )
        item = _items(store)[PDF_URL]
        assert item.status is CrawlItemStatus.FETCHED
        raw = Path(item.raw_path)
        assert raw.stat().st_size == 25 * MIB

        extractor = _DocumentExtractor()
        result = _build(
            tmp_path,
            crawler=_ExplodingCrawler(),
            urls=[PDF_URL],
            store=store,
            extractor=extractor,
        )

        resumed = _items(store)[PDF_URL]
        assert resumed.status is CrawlItemStatus.FETCHED
        assert resumed.content_hash == item.content_hash
        assert resumed.raw_path == str(raw)
        assert not any("payload_too_large" in e for e in result.errors)
        # Документ переизвлечён из retained-файла (resume не теряет содержимое),
        # и тело снова не поднято в RAM вызывающего.
        assert extractor.calls == 1
        assert extractor.source_sizes == [25 * MIB]
        assert extractor.sizes == [0]
    finally:
        store.close()


def test_artifact_outside_path_prefix_is_visible_in_provenance(tmp_path: Path) -> None:
    """Обход path-prefix артефактом: PDF вне ``/docs`` добыт, обход виден в провенансе."""
    root = "https://docs.test/docs/index.html"
    body = (
        b"<html><body>"
        b'<a href="/files/manual.pdf">pdf</a>'
        b'<a href="/files/page.html">html</a>'
        b"</body></html>"
    )
    pdf_body = PDF_MAGIC + bytes(512)
    crawler = _BufferedCrawler(
        {root: body, PDF_URL: pdf_body},
        {root: "text/html", PDF_URL: "application/pdf"},
    )
    store = SqliteStore(":memory:")
    try:
        _make_run(store)
        result = _build(
            tmp_path,
            crawler=crawler,
            urls=[root],
            store=store,
            crawl_defaults=CrawlRunConfig(max_depth=1),
        )

        # Артефакт вне префикса добыт; HTML вне префикса — нет (прежнее поведение).
        assert PDF_URL in crawler.calls
        assert "https://docs.test/files/page.html" not in crawler.calls

        record = _build_of(result).truncation[0]
        assert record["root"] == root
        assert record["artifacts_outside_prefix"] == {
            "count": 1,
            "examples": [PDF_URL],
        }
        # Обход — не усечение и не ошибка: молчаливым он быть не должен, но и
        # ``truncated`` он не поднимает (полнота корня не страдает).
        assert record["truncated"] is False
    finally:
        store.close()
