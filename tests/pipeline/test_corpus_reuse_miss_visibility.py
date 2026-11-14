"""Видимость промахов ``304``: одна строка на причину, а не строка на URL.

Измерено 14.11.2026 на прогоне ``ru-pg-dbms-145ad66ca3f9``: фаза корпуса отдавала **590**
строк ``304 без переиспользуемой версии документа — обычная экстракция`` (406 URL без
версии + 184 с отвергнутым chunk-set). Исход был верный — шла обычная экстракция, — но
нормальный фолбэк забивал ``errors`` и делал настоящие отказы нечитаемыми.

Здесь проверяется: причина считается **фактически** (нет версии / не ``active`` /
отвергнут chunk-set), а публикуется одна строка на продукт с числом URL по каждой причине.
Тесты офлайн: SQLite в памяти, порты — структурные заглушки. Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import build_corpus
from core.pipeline.corpus_persist import (
    REUSE_MISS_CHUNK_SET_REJECTED,
    REUSE_MISS_NO_DOC_VERSION,
    REUSE_MISS_VERSION_NOT_ACTIVE,
)
from core.ports.state import DocumentVersionRow, UrlStateRow
from core.ports.types import Chunk, ExtractedDoc, ExtractionRoute, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore
from core.util.hashing import content_hash

RUN_ID = "run-reuse-miss"
FIXTURES = Path(__file__).parent / "fixtures"
TS = "2026-01-01T00:00:00+00:00"
ETAG = '"v1"'
URL_NO_VERSION = "https://docs.test/p1/no-version"
URL_NOT_ACTIVE = "https://docs.test/p1/not-active"
URL_CHUNKS_REJECTED = "https://docs.test/p1/chunks-rejected"
URLS = (URL_NO_VERSION, URL_NOT_ACTIVE, URL_CHUNKS_REJECTED)


# --------------------------------------------------------------------------- #
# Заглушки портов
# --------------------------------------------------------------------------- #


class _Crawler:
    """Краулер с условным GET: ``304`` — только при совпавшем ``If-None-Match``."""

    version = "crawler-1"
    engine: str | None = None

    def __init__(self, pages: dict[str, bytes], *, not_modified: set[str]) -> None:
        self.pages = dict(pages)
        self.not_modified = set(not_modified)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 — сигнатура порта
        self.calls.append(url)
        confirmed = url in self.not_modified and plan.if_none_match == ETAG
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
            status_code=304 if confirmed else 200,
            etag=ETAG,
            # ``not_modified`` — объявленный флаг порта (так делают адаптеры), а не вывод
            # из ``status_code``: по нему краул называет происхождение наблюдения.
            not_modified=confirmed,
        )


class _Extractor:
    """Экстрактор: текст из URL (байты не нужны — проверяется видимость, не разбор)."""

    version = "extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        text = f"text for {page.url}"
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(text),
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    """Чанкер: один чанк на документ."""

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


# --------------------------------------------------------------------------- #
# План и стор
# --------------------------------------------------------------------------- #


def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=["p1"],
        anchor="p1",
        modules=["mod_a"],
        roles=["role_a", "role_b"],
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


def _store() -> SqliteStore:
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor="p1",
            cohort="case",
            products=["p1"],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    return store


# --------------------------------------------------------------------------- #
# Durable-состояние: три URL, три разные причины промаха
# --------------------------------------------------------------------------- #


def _version(
    doc_version_id: str, url: str, *, status: str
) -> DocumentVersionRow:
    return DocumentVersionRow(
        doc_version_id=doc_version_id,
        doc_id=f"p1:{doc_version_id}",
        product_id="p1",
        url=url,
        # Хеш **другого** текста: прогон запишет свою версию, и ключ
        # ``(content_hash, extractor, level)`` не столкнётся с посеянной строкой.
        content_hash=content_hash(f"stale text for {url}"),
        extractor_version="extractor-1",
        level="full",
        first_seen_run_id="run-previous",
        last_seen_run_id="run-previous",
        first_seen_at=TS,
        last_seen_at=TS,
        status=status,
        # Версия чанкера совпадает с прогоном: промах обязан быть «нет chunk-set»,
        # а не «другая версия чанкера» — иначе тест проверял бы не ту причину.
        chunker_version="chunker-1",
    )


def _seed_state(state: SqliteStateStore) -> None:
    """Три URL: нет версии / версия не ``active`` / ``active`` без chunk-set."""
    for url in URLS:
        state.upsert_url_state(
            UrlStateRow(
                product_id="p1",
                url=url,
                normalized_url=url,
                etag=ETAG,
                last_http_status=200,
            )
        )
    state.upsert_document_version(
        _version("dv_not_active", URL_NOT_ACTIVE, status="superseded")
    )
    state.upsert_document_version(
        _version("dv_no_chunks", URL_CHUNKS_REJECTED, status="active")
    )
    for url, doc_version_id in (
        (URL_NOT_ACTIVE, "dv_not_active"),
        (URL_CHUNKS_REJECTED, "dv_no_chunks"),
    ):
        row = state.get_url_state("p1", url)
        assert row is not None
        state.upsert_url_state(
            UrlStateRow(
                product_id="p1",
                url=url,
                normalized_url=url,
                doc_version_id=doc_version_id,
                etag=ETAG,
                last_http_status=200,
            )
        )


def _run(
    tmp_path: Path, state: SqliteStateStore, *, not_modified: set[str]
) -> tuple[Any, SqliteStore]:
    store = _store()
    pages = {url: b"bytes" for url in URLS}
    result = build_corpus(
        _plan(),
        _Crawler(pages, not_modified=not_modified),
        _Extractor(),
        _Chunker(),
        store,
        urls={"p1": list(URLS)},
        raw_root=tmp_path,
        run_id=RUN_ID,
        out_dir=tmp_path / "runs",
        state=state,
        product_modes={"p1": "incremental"},
    )
    return result, store


# --------------------------------------------------------------------------- #
# Агрегация
# --------------------------------------------------------------------------- #


def test_304_misses_are_counted_by_reason_not_per_url(tmp_path: Path) -> None:
    """Три URL трёх разных причин → **одна** строка с числами, а не три строки URL."""
    with SqliteStateStore(":memory:") as state:
        _seed_state(state)
        result, store = _run(tmp_path, state, not_modified=set(URLS))
        try:
            misses = [err for err in result.errors if "304 без переиспользуемой версии" in err]
            assert len(misses) == 1, misses
            line = misses[0]
            assert "1 chunk_set_rejected" in line
            assert "1 no_doc_version" in line
            assert "1 version_not_active" in line
            assert "всего 3 URL" in line
            # Адреса не перечисляются: строка — факт фазы, а не журнал по URL.
            assert not any(url in line for url in URLS)

            # Исход верный: переиспользовать было нечего — документы добыты заново.
            assert len(list(store.iter_corpus_docs(RUN_ID))) == 3
            assert store.read_corpus_coverage(RUN_ID, "p1") is not None
        finally:
            store.close()


def test_reuse_success_does_not_produce_miss_line(tmp_path: Path) -> None:
    """Успешное переиспользование не считается промахом: строки нет вовсе."""
    with SqliteStateStore(":memory:") as state:
        # Полный durable-путь: первый прогон добывает версию и chunk-set, второй получает
        # 304 и переиспользует — промахов нет ни по одной причине.
        first, first_store = _run(tmp_path / "r1", state, not_modified=set())
        first_store.close()
        assert first.errors == []

        second, second_store = _run(
            tmp_path / "r2", state, not_modified=set(URLS)
        )
        try:
            assert not any(
                "304 без переиспользуемой версии" in err for err in second.errors
            ), second.errors
            assert len(list(second_store.iter_corpus_docs(RUN_ID))) == 3
            # Промахов нет именно потому, что переиспользование состоялось: иначе тест
            # проходил бы и на «304 не случился вовсе».
            assert {
                state.get_url_state("p1", url).reuse_kind for url in URLS
            } == {"revalidated"}
        finally:
            second_store.close()


def test_reuse_miss_tokens_are_distinct() -> None:
    """Токены причин — машинные и различимые: по ним считается счётчик, не по тексту."""
    assert len(
        {
            REUSE_MISS_NO_DOC_VERSION,
            REUSE_MISS_VERSION_NOT_ACTIVE,
            REUSE_MISS_CHUNK_SET_REJECTED,
        }
    ) == 3
