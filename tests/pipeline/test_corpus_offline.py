"""``--refresh offline``: корпус из durable-субстрата и **ноль** сетевых вызовов (09 §6.3).

Проверяется семантика политики ``offline`` целиком:

1. ноль сетевых вызовов — краулер, экстрактор, чанкер и внешний поиск подменены
   заглушками, которые падают на любом вызове;
2. известный URL (``url_state`` + ``document_version`` + chunk-set) переиспользуется:
   ``reuse_kind=reused``, HTTP-статус ``NULL``, экстрактор и чанкер не вызываются;
3. авторский корень, известный состоянию, тоже ``reuse`` — «без сети» не начинается с
   GET корня; корень, которого в состоянии нет, — видимая ошибка с продуктом и URL;
4. известный URL без пригодной версии — видимая запись с причиной, документа в корпусе нет;
5. честность покрытия: ``observed_sources=0``, ``complete=False``, причина
   ``offline_no_network_observation`` — оффлайн-прогон не выдаёт себя за полный корпус.

Субстрат наполняется **реальным** путём (первый прогон с ``auto``), а не собран руками:
иначе тест доказывал бы согласие заглушки с заглушкой, а не работоспособность
durable-субстрата. Нейтральные фикстуры: ``p1``, ``docs.test``.
"""

from __future__ import annotations

import datetime as _dt
import pathlib
from pathlib import Path
from typing import Any

import pytest

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CorpusDoc,
    CrawlRunConfig,
    CrawlSessionItem,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import (
    COVERAGE_OFFLINE_NO_OBSERVATION,
    IndexBuild,
    build_corpus,
)
from core.pipeline.corpus_ingest import (
    OFFLINE_NO_VERSION_TEXT,
    OFFLINE_UNKNOWN_ROOT_TEXT,
    OfflineReuseReport,
)
from core.pipeline.crawl_session import default_session_id
from core.ports.state import UrlStateRow
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore
from core.util.hashing import content_hash

RUN_ID = "run-offline"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"

ROOT = "https://docs.test/p1/index"
PAGE = "https://docs.test/p1/print"
BODY = b"page one text"

# --------------------------------------------------------------------------- #
# Порты: всё, что ходит в сеть, падает на первом вызове
# --------------------------------------------------------------------------- #


class _NoNetworkCrawler:
    """Краулер, падающий на **любом** вызове: доказательство отсутствия сети.

    ``AssertionError`` (а не тихий ``RuntimeError``) — потому что фазовый контракт
    изолирует исключения краулера в ``errors``: сбой обязан быть виден как падение
    теста, а не как «фаза отработала с ошибкой».
    """

    version = "crawler-1"
    engine: str | None = None

    def __init__(self) -> None:
        self.calls: list[tuple[str, FetchPlan]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        self.calls.append((url, plan))
        raise AssertionError(f"offline: сетевой вызов краулера по {url!r}")


class _CountingExtractor:
    """Экстрактор, падающий при вызове: оффлайн-переиспользование его не зовёт."""

    version = "extractor-1"

    def __init__(self) -> None:
        self.calls = 0

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        raise AssertionError(f"offline: экстрактор вызван для {page.url!r}")


class _CountingChunker:
    """Чанкер, падающий при вызове: переиспользованный chunk-set не пере-нарезается."""

    version = "chunker-1"

    def __init__(self) -> None:
        self.calls = 0

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        self.calls += 1
        raise AssertionError(f"offline: чанкер вызван для {document_id!r}")


class _FirstRunExtractor:
    """Экстрактор **первого** (наполняющего) прогона: текст = декодированные байты."""

    version = "extractor-1"

    def __init__(self) -> None:
        self.calls = 0

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        text = _page_bytes(page).decode("utf-8", errors="replace")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(_page_bytes(page)),
            extractor_version=self.version,
            page_count=1,
        )


class _FirstRunChunker:
    """Чанкер первого прогона: один чанк на документ."""

    version = "chunker-1"

    def __init__(self) -> None:
        self.calls = 0

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        self.calls += 1
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


class _FirstRunCrawler:
    """Краулер первого прогона: отдаёт заранее заданные байты."""

    version = "crawler-1"
    engine: str | None = None

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)
        self.calls: list[tuple[str, FetchPlan]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        self.calls.append((url, plan))
        return RawPage(
            url=url,
            content=self.pages[url],
            fetched_at=TS,
            fetch_mode=plan.fetch,
            lang="en",
            status_code=200,
        )


class _NoSearch:
    """Внешний поиск, падающий на вызове: ``offline`` не трогает и SERP-контур."""

    def __init__(self) -> None:
        self.calls = 0

    def search(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        raise AssertionError("offline: вызов внешнего поиска (SERP-контур)")


# --------------------------------------------------------------------------- #
# План/стор/хелперы
# --------------------------------------------------------------------------- #


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
    """Строка прогона: нужна для FK ``crawl_sessions.run_id`` (L1a)."""
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


def _seed_substrate(
    tmp_path: Path,
    state: SqliteStateStore,
    *,
    pages: dict[str, bytes],
    roots: list[str],
) -> None:
    """Наполняет ``state.db`` **реальным** путём: прогон ``auto`` с живым краулером.

    Ничего не пишется руками в таблицы: ``url_state``/``document_version``/``chunk_meta``/
    ``chunk_store`` создаёт штатный писатель, а оффлайн-проход затем читает именно его.
    """
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _FirstRunCrawler(pages)
    try:
        build_corpus(
            _plan(),
            crawler,
            _FirstRunExtractor(),
            _FirstRunChunker(),
            store,
            urls={"p1": list(roots)},
            raw_root=tmp_path / "raw-first",
            run_id=RUN_ID,
            state=state,
            product_modes={"p1": "incremental"},
            refresh_policy="auto",
        )
    finally:
        store.close()
    # Наполняющий прогон обязан был реально сходить за документами: иначе «оффлайн ничего
    # не вызвал» доказывало бы согласие заглушек, а не отсутствие сети.
    assert crawler.calls, "наполняющий прогон не фетчил — субстрат не создан"


def _offline_run(
    tmp_path: Path,
    state: SqliteStateStore,
    *,
    roots: list[str],
    extractor: _CountingExtractor | None = None,
    chunker: _CountingChunker | None = None,
    **kwargs: Any,
) -> tuple[Any, SqliteStore, _NoNetworkCrawler]:
    """Оффлайн-прогон с краулером, падающим на любом вызове."""
    store = SqliteStore(":memory:")
    _make_run(store)
    crawler = _NoNetworkCrawler()
    result = build_corpus(
        _plan(),
        crawler,
        extractor or _CountingExtractor(),
        chunker or _CountingChunker(),
        store,
        urls={"p1": list(roots)},
        raw_root=tmp_path / "raw-offline",
        run_id=RUN_ID,
        state=state,
        product_modes={"p1": "incremental"},
        refresh_policy="offline",
        **kwargs,
    )
    return result, store, crawler


def _docs(store: SqliteStore) -> list[CorpusDoc]:
    return list(store.iter_corpus_docs(RUN_ID))


def _build_of(result: Any) -> IndexBuild:
    builds = [e for e in result.log_entries if isinstance(e, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def _offline_events(result: Any) -> list[dict[str, Any]]:
    return [
        entry
        for entry in result.log_entries
        if isinstance(entry, dict) and entry.get("event") == "product_end"
    ]


def _session_items(store: SqliteStore) -> list[CrawlSessionItem]:
    return list(store.iter_crawl_session_items(SESSION_ID))


# --------------------------------------------------------------------------- #
# 1/2. Известный корень и известный URL: reuse без сети
# --------------------------------------------------------------------------- #


def test_offline_reuses_known_root_and_page_without_any_network_call(
    tmp_path: Path,
) -> None:
    """Корень и страница известны состоянию → ``reused``, ноль сетевых вызовов.

    Это главный тест семантики: ``offline`` не ходит в сеть **ни разу** — ни за корнем
    (иначе «без сети» начиналось бы с GET), ни за страницей, ни условным GET.
    """
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(
            tmp_path, state, pages={ROOT: BODY, PAGE: BODY}, roots=[ROOT, PAGE]
        )
        version_before = state.get_url_state("p1", ROOT).doc_version_id
        assert version_before is not None

        extractor, chunker = _CountingExtractor(), _CountingChunker()
        result, store, crawler = _offline_run(
            tmp_path, state, roots=[ROOT, PAGE], extractor=extractor, chunker=chunker
        )
        try:
            # 1. Ноль сетевых вызовов и ноль вызовов приборов.
            assert crawler.calls == [], "offline обязан не трогать сеть"
            assert extractor.calls == 0, "переиспользование не зовёт экстрактор"
            assert chunker.calls == 0, "переиспользование не зовёт чанкер"
            assert result.errors == []

            # 2. Документ в корпусе — из durable-субстрата, без ответа сервера.
            docs = _docs(store)
            assert len(docs) == 1, "два URL одного контента схлопнулись дедупом"
            doc = docs[0]
            assert doc.content_hash == content_hash(BODY)
            assert doc.reuse_kind is not None and doc.reuse_kind.value == "reused"
            assert doc.http_status is None, "сервер не отвечал — статус NULL, а не 200"
            assert doc.fetched_at is None, "получения не было"
            # 3. Чанки переиспользованы: текст тот же, что был проиндексирован (I19).
            build = _build_of(result)
            assert build.chunk_count == 1
            # 4. Счётчики честные: документ переиспользован, а не «новый».
            assert build.doc_counters == {
                "p1": {"total": 1, "new": 0, "reused": 1}
            }
            # 5. Элементов сеанса нет: ``crawl_session_items`` — учёт фетчей.
            assert _session_items(store) == []
            # 6. ``url_state`` не переписан: наблюдения не было, провенанс не подменяется.
            assert state.get_url_state("p1", ROOT).doc_version_id == version_before
        finally:
            store.close()


def test_offline_does_not_call_search_contour(tmp_path: Path) -> None:
    """SERP-контур — тоже сеть: под ``offline`` он не исполняется и говорит причину."""
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        search = _NoSearch()
        result, store, crawler = _offline_run(
            tmp_path,
            state,
            roots=[ROOT],
            search=search,
            serp_rounds=1,
            serp_source="declared",
        )
        try:
            assert crawler.calls == []
            assert search.calls == 0, "offline обязан не трогать внешний поиск"
            observations = [
                entry
                for entry in result.log_entries
                if isinstance(entry, dict) and "rounds" in entry and "budget" in entry
            ]
            assert observations, "наблюдение контура обязано присутствовать"
            rounds = observations[0]["rounds"]
            assert rounds["not_executed"] == 1
            assert rounds["not_executed_reason"] == "offline_refresh_policy"
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# 3. Корень, которого в состоянии нет
# --------------------------------------------------------------------------- #


def test_offline_unknown_root_is_a_visible_error_not_a_fetch(tmp_path: Path) -> None:
    """Корень вне ``url_state``: видимая ошибка с продуктом и URL, а не GET и не тишина."""
    with SqliteStateStore(":memory:") as state:
        result, store, crawler = _offline_run(tmp_path, state, roots=[ROOT])
        try:
            assert crawler.calls == [], "неизвестный корень не повод сходить в сеть"
            assert _docs(store) == []
            assert any(
                ROOT in error
                and "p1" in error
                and OFFLINE_UNKNOWN_ROOT_TEXT in error
                for error in result.errors
            ), f"нет видимой ошибки о неизвестном корне: {result.errors}"
            # Причина zero-source названа политикой, а не «нет seed'ов».
            coverage = store.read_corpus_coverage(RUN_ID, "p1")
            assert coverage is not None
            assert coverage.reason == COVERAGE_OFFLINE_NO_OBSERVATION
            events = _offline_events(result)
            assert events and events[0]["reason"] == COVERAGE_OFFLINE_NO_OBSERVATION
            offline = events[0]["offline"]
            assert offline["unknown_roots"] == [ROOT]
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# 4. Известный URL без пригодной версии
# --------------------------------------------------------------------------- #


def test_offline_known_url_without_version_is_visible_and_excluded(
    tmp_path: Path,
) -> None:
    """``url_state`` без версии: переиспользовать нечего — запись есть, документа нет."""
    with SqliteStateStore(":memory:") as state:
        state.upsert_url_state(
            UrlStateRow(product_id="p1", url=ROOT, normalized_url=ROOT)
        )
        result, store, crawler = _offline_run(tmp_path, state, roots=[ROOT])
        try:
            assert crawler.calls == []
            assert _docs(store) == [], "нечего переиспользовать — документа в корпусе нет"
            assert any(
                OFFLINE_NO_VERSION_TEXT in error and ROOT in error
                for error in result.errors
            ), f"нет видимой причины: {result.errors}"
            events = _offline_events(result)
            assert events[0]["offline"]["no_version"] == [ROOT]
            assert events[0]["offline"]["reused_docs"] == 0
        finally:
            store.close()


def test_offline_chunker_version_mismatch_does_not_reuse_or_fetch(
    tmp_path: Path,
) -> None:
    """Чужой ``chunker_version`` в версии: chunk-set не переиспользуется и не фетчится."""
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        result, store, crawler = _offline_run(
            tmp_path, state, roots=[ROOT], chunker_version="chunker-OTHER"
        )
        try:
            assert crawler.calls == []
            assert _docs(store) == []
            assert any("chunker" in error for error in result.errors), result.errors
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# 5. Честность покрытия
# --------------------------------------------------------------------------- #


def test_offline_run_is_complete_by_capture_and_names_its_own_reason(
    tmp_path: Path,
) -> None:
    """``reused``: наблюдений этого прогона нет, но корпус удостоверен снятием (F3).

    План 20 §11.2: ``complete`` — факт **корпуса** (датированное исчерпание корня), а
    ``observed_sources = 0`` — факт **прогона**. Переиспользованный документ по-прежнему
    называется переиспользованным, а ``reason`` называет, что сеть не трогалась;
    полнота при этом не выдумана, а унаследована вместе с наблюдением.
    """
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        result, store, crawler = _offline_run(tmp_path, state, roots=[ROOT])
        try:
            assert crawler.calls == []
            coverage = store.read_corpus_coverage(RUN_ID, "p1")
            assert coverage is not None
            assert coverage.observed_sources == 0, "наблюдений этого прогона нет"
            assert coverage.complete is True
            assert [root.complete for root in coverage.roots] == [True]
            assert coverage.reason == COVERAGE_OFFLINE_NO_OBSERVATION
            # Строка продукта: прогон завершился, документ есть, охват неполон.
            assert coverage.expected_sources == 0
            # Отчёт говорит об оффлайне строкой, а не только структурой.
            notes = [e for e in result.log_entries if isinstance(e, str)]
            assert any("offline" in note and "сеть не трогалась" in note for note in notes), (
                notes
            )
            # И ``docs`` прогона — переиспользованные, а не «наблюдённые».
            events = _offline_events(result)
            assert events[0]["offline"]["reused_docs"] == 1
            assert events[0]["fetched"] == 0
            assert events[0]["session_items"] == 0
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# Единица: отчёт и тексты причин
# --------------------------------------------------------------------------- #


def test_offline_report_note_names_both_facts() -> None:
    """Строка продукта называет и переиспользованное, и непереиспользуемое."""
    report = OfflineReuseReport(
        reused=3,
        chunks=7,
        no_version=("https://docs.test/a",),
        unknown_roots=("https://docs.test/b",),
    )
    note = report.note("p1")
    assert "offline p1" in note
    assert "документов 3" in note
    assert "чанков 7" in note
    assert "переиспользовать нечего 1" in note
    assert "корней вне состояния 1" in note
    # F3: «переиспользовано» не отменяет `❌` — отменяет отсутствие даты у носителя (I2d).
    assert "наблюдений этого прогона нет" in note
    assert "только с датой носителя, I2d" in note
    assert "❌ не публикуется" not in note


@pytest.mark.parametrize("text", [OFFLINE_UNKNOWN_ROOT_TEXT, OFFLINE_NO_VERSION_TEXT])
def test_offline_reason_texts_name_the_policy_and_the_next_step(text: str) -> None:
    """Тексты причин не молчат о политике и не обещают «потом подтянется»."""
    assert text.startswith("offline:")
    assert "--refresh" in text or "сеть не трогается" in text


def test_offline_does_not_respect_revalidate_cap_axis(tmp_path: Path) -> None:
    """Потолок ревалидации — ось **сети**; оффлайн-проход его не применяет.

    ``crawl.revalidate_max_documents`` ограничивает живой условный GET. Оффлайн ничего
    не фетчит, поэтому ось не должна ни откладывать переиспользование, ни писать
    ``stale``: иначе документы молча выпали бы из корпуса «по политике усечения».
    """
    extra = "https://docs.test/p1/print"
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(
            tmp_path, state, pages={ROOT: BODY, extra: b"second body"}, roots=[ROOT, extra]
        )
        result, store, crawler = _offline_run(
            tmp_path,
            state,
            roots=[ROOT, extra],
            crawl_defaults=CrawlRunConfig(revalidate_max_documents=1),
        )
        try:
            assert crawler.calls == []
            assert len(_docs(store)) == 2, "потолок сети не режет переиспользование"
            assert not any("revalidate_max_documents" in e for e in result.errors)
        finally:
            store.close()


def test_offline_repair_work_set_is_reuse_not_revalidate(tmp_path: Path) -> None:
    """``--only-failed`` + ``offline``: ремонт известного URL идёт из состояния, не в сеть.

    Ремонт — повторная проверка **известных** URL. Под ``offline`` проверять можно только
    состояние: элемент ``revalidate`` без валидаторов был бы безусловным GET, то есть
    той же сетью, которую политика запрещает.
    """
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={PAGE: BODY}, roots=[ROOT, PAGE])
        result, store, crawler = _offline_run(
            tmp_path,
            state,
            roots=[ROOT],
            only_failed_repairs={"p1": [PAGE]},
        )
        try:
            assert crawler.calls == [], "ремонт под offline не ходит в сеть"
            docs = {doc.url for doc in _docs(store)}
            assert PAGE in docs, "ремонтируемый URL переиспользован из состояния"
            assert not any("reuse не реализован" in error for error in result.errors)
        finally:
            store.close()


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
