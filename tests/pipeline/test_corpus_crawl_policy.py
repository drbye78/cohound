"""Исполнение политики краула в Ш5 (план 19 §P2): ``on_request`` и ``network_denied``.

Главное утверждение — **ноль сетевых вызовов** там, где политика запретила сеть, и
гейт **выше** первого сетевого вызова:

1. ``network_denied`` по продукту: краулер/внешний поиск падают на любом вызове,
   ``expand_directory`` (feed/channel/sitemap — сеть) подменён шпионом, который падает
   на вызове; вызовов ноль, продукт остаётся на карте (``PARTIAL``, не ``BLOCKED``),
   покрытие неполно и названо токеном ``crawl_policy_skipped``.
2. ``on_request`` с наблюдением и **без** ``--refresh-scope``: сеть запрещена, корпус
   берётся из durable-субстрата тем же generalized-проходом, что ``offline``
   («продукт → корень»), а не вторым путём; ``url_state`` не переписывается.
3. ``--refresh-scope`` снимает запрет ``on_request`` — вызывается ровно названный корень.
4. Продукт «1 живой + 1 запрещённый корень»: ``complete=False``, названный токен,
   ``partial``-сеанс, строка в ``notes``.
5. Дайджест: ``--refresh-scope`` меняет решение (а значит ``run_id``), его отсутствие — нет.
6. Статически: ни одно значение ``CrawlPolicyMode``/``RefreshPolicy`` не падает в
   «по умолчанию сеть», и гейт стоит выше резолва цели/развёртки каталога.

Нейтральные фикстуры: ``p1``, ``docs.test``. Сеть — только заглушки, падающие на вызове.
"""

from __future__ import annotations

import ast
import datetime as _dt
import pathlib
from pathlib import Path
from typing import Any

import pytest

from core.config.crawl_policy import CrawlPolicyConfig
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
    CrawlSessionStatus,
    RunConfig,
    RunMode,
)
from core.domain.enums import CrawlPolicyMode, RefreshPolicy, RunProductStatus
from core.pipeline import RunPlan
from core.pipeline import corpus as corpus_mod
from core.pipeline import crawl_policy as crawl_policy_mod
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.corpus_ingest import OFFLINE_UNKNOWN_ROOT_TEXT
from core.pipeline.crawl_policy import (
    CRAWL_POLICY_SKIPPED_REASON,
    DeclaredRoot,
    parse_requested_scopes,
    resolve_crawl_policy,
)
from core.pipeline.crawl_session import default_session_id
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

RUN_ID = "run-crawl-policy"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
NOW = _dt.datetime(2026, 1, 10, 12, 0, tzinfo=_dt.UTC)

ROOT = "https://docs.test/p1/index"
PAGE = "https://docs.test/p1/print"
OTHER = "https://other.test/p1/index"
BODY = b"page one text"


# --------------------------------------------------------------------------- #
# Порты: всё, что ходит в сеть, падает на первом вызове
# --------------------------------------------------------------------------- #


class _NoNetworkCrawler:
    """Краулер, падающий на **любом** вызове (счётчик фиксирует сам факт попытки)."""

    version = "crawler-1"
    engine: str | None = None

    def __init__(self) -> None:
        self.calls: list[tuple[str, FetchPlan]] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта
        self.calls.append((url, plan))
        raise AssertionError(f"crawl_policy: сетевой вызов краулера по {url!r}")


class _CountingExtractor:
    """Экстрактор, падающий при вызове: переиспользование его не зовёт."""

    version = "extractor-1"

    def __init__(self) -> None:
        self.calls = 0

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        raise AssertionError(f"crawl_policy: экстрактор вызван для {page.url!r}")


class _CountingChunker:
    """Чанкер, падающий при вызове: переиспользованный chunk-set не пере-нарезается."""

    version = "chunker-1"

    def __init__(self) -> None:
        self.calls = 0

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        self.calls += 1
        raise AssertionError(f"crawl_policy: чанкер вызван для {document_id!r}")


class _FirstRunExtractor:
    """Экстрактор наполняющего прогона: текст = декодированные байты."""

    version = "extractor-1"

    def __init__(self) -> None:
        self.calls = 0

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        self.calls += 1
        raw = _page_bytes(page)
        text = raw.decode("utf-8", errors="replace")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(raw),
            extractor_version=self.version,
            page_count=1,
        )


class _FirstRunChunker:
    """Чанкер наполняющего прогона: один чанк на документ."""

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
    """Краулер наполняющего прогона: отдаёт заранее заданные байты."""

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
    """Внешний поиск, падающий на вызове: запрещённая сеть не трогает и SERP."""

    def __init__(self) -> None:
        self.calls = 0

    def search(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        raise AssertionError("crawl_policy: вызов внешнего поиска (SERP-контур)")


# --------------------------------------------------------------------------- #
# План/стор/резолв политики
# --------------------------------------------------------------------------- #


def _page_bytes(page: Any) -> bytes:
    """Байты страницы: тело либо retained-файл (потребитель очереди не держит тело в RAM)."""
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return pathlib.Path(path).read_bytes() if path else b""


def _plan(product_id: str = "p1") -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=[product_id],
        anchor=product_id,
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
                    product_id=product_id,
                    role_overlap=1.0,
                    selection_reason="anchor",
                    anchor_relation=AnchorRelation.ANCHOR,
                )
            ],
        ),
        golden=[],
    )


def _make_run(store: SqliteStore) -> None:
    """Строка прогона: нужна для FK ``crawl_sessions.run_id``."""
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


def _root(url: str, product_id: str = "p1") -> DeclaredRoot:
    from urllib.parse import urlsplit

    parsed = urlsplit(url)
    return DeclaredRoot(product_id, parsed.hostname or "", parsed.path or "")


def _resolution(
    mode: str | CrawlPolicyMode,
    *,
    roots: list[str] = (ROOT,),
    product_id: str = "p1",
    requested: tuple[str, ...] = (),
    observed: bool = True,
):
    """Резолв политики без БД: дефолт пакета + объявленные корни + наблюдения.

    ``observed=False`` — первичный дискавери (наблюдения нет): под ``on_request`` это
    разрешает сеть, и тест обязан отличать этот случай от «обновление без запроса».
    """
    config = CrawlPolicyConfig.model_validate(
        {"products": {product_id: {"mode": mode}}}
    )
    declared = [_root(url, product_id) for url in roots]
    observed_at = (
        {(root.product_id, root.host, root.path): NOW - _dt.timedelta(days=1) for root in declared}
        if observed
        else {}
    )
    return resolve_crawl_policy(
        package_policy=config,
        operator_rows=[],
        roots=declared,
        now=NOW,
        observed_at=observed_at,
        requested=parse_requested_scopes(
            requested, product_ids={product_id}
        ),
    )


def _run_corpus(
    tmp_path: Path,
    *,
    policy: Any,
    urls: dict[str, list[str]],
    crawler: Any,
    extractor: Any,
    chunker: Any,
    state: SqliteStateStore | None = None,
    search: Any = None,
    serp_rounds: int = 0,
    product_modes: dict[str, str] | None = None,
    refresh_policy: str = "auto",
    products: list[str] | None = None,
    crawl_defaults: Any = None,
):
    store = SqliteStore(":memory:")
    _make_run(store)
    result = build_corpus(
        _plan(),
        crawler,
        extractor,
        chunker,
        store,
        urls=urls,
        raw_root=tmp_path / "raw",
        run_id=RUN_ID,
        state=state,
        product_modes=product_modes or {pid: "incremental" for pid in urls},
        refresh_policy=refresh_policy,
        crawl_policy=policy,
        search=search,
        serp_rounds=serp_rounds,
        serp_source="declared",
        crawl_defaults=crawl_defaults,
    )
    if products is None:
        products = sorted(urls)
    coverages = {row.product_id: row for row in store.iter_corpus_coverage(RUN_ID)}
    run_products = {row["product_id"]: row for row in store.iter_run_products(RUN_ID)}
    session = store.get_crawl_session(SESSION_ID)
    return result, store, coverages, run_products, session


def _notes(result: Any) -> str:
    return "\n".join(str(entry) for entry in result.log_entries)


def _seed_substrate(
    tmp_path: Path,
    state: SqliteStateStore,
    *,
    pages: dict[str, bytes],
    roots: list[str],
    crawl_defaults: Any = None,
) -> None:
    """Наполняет ``state.db`` **реальным** путём: прогон ``auto`` с живым краулером."""
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
            crawl_defaults=crawl_defaults,
        )
    finally:
        store.close()
    assert crawler.calls, "наполняющий прогон не фетчил — субстрат не создан"


# --------------------------------------------------------------------------- #
# 1. network_denied: ноль сетевых вызовов
# --------------------------------------------------------------------------- #


def test_network_denied_makes_zero_network_calls_and_names_the_skip(
    tmp_path: Path,
) -> None:
    """``network_denied`` — ни одного запроса; пропуск назван токеном, не ``failed``."""
    crawler = _NoNetworkCrawler()
    search = _NoSearch()
    result, store, coverages, run_products, session = _run_corpus(
        tmp_path,
        policy=_resolution(CrawlPolicyMode.NETWORK_DENIED),
        urls={"p1": [ROOT]},
        crawler=crawler,
        extractor=_CountingExtractor(),
        chunker=_CountingChunker(),
        search=search,
        serp_rounds=1,
    )
    try:
        # 1. Ноль сетевых вызовов: краулер не позван ни разу, внешний поиск тоже.
        assert crawler.calls == [], "network_denied обязан не трогать сеть"
        assert search.calls == 0, "SERP-контур — сеть: под запретом он не исполняется"

        # 2. Покрытие неполно и **названо**: иначе пропуск читался бы как «корпус собран».
        coverage = coverages["p1"]
        assert coverage.complete is False
        assert coverage.reason == CRAWL_POLICY_SKIPPED_REASON
        assert coverage.expected_sources == 0
        assert coverage.observed_sources == 0

        # 3. Продукт остаётся на карте: PARTIAL, а не BLOCKED («несостоявшееся измерение»).
        assert run_products["p1"]["status"] == RunProductStatus.PARTIAL.value
        assert run_products["p1"]["coverage_complete"] is False
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL

        # 4. Пропуск виден в строке продукта и в провенансе корня.
        notes = _notes(result)
        assert CRAWL_POLICY_SKIPPED_REASON in notes
        assert "network_denied" in notes or "crawl_network_denied" in notes
        build = next(entry for entry in result.log_entries if isinstance(entry, IndexBuild))
        assert build.doc_counters == {}, "документов нет и не выдумано"
    finally:
        store.close()


def test_expand_directory_spy_is_not_called_for_denied_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``expand_directory`` (feed/channel/sitemap — сеть) не вызывается для ``deny``.

    Шпион — на **функции**, а не на сети: тест обязан ловить и sitemap/feed-разворот,
    который до фронтьера, то есть ниже гейта политики.
    """
    calls: list[tuple[str, str]] = []

    def _spy(product_id: str, url: str, target: Any) -> list[str] | None:
        calls.append((product_id, url))
        raise AssertionError(f"crawl_policy: expand_directory для {url!r}")

    monkeypatch.setattr(corpus_mod.corpus_ingest, "expand_directory", _spy)

    result, store, *_ = _run_corpus(
        tmp_path,
        policy=_resolution(CrawlPolicyMode.NETWORK_DENIED),
        urls={"p1": [ROOT]},
        crawler=_NoNetworkCrawler(),
        extractor=_CountingExtractor(),
        chunker=_CountingChunker(),
    )
    try:
        assert calls == []
        assert CRAWL_POLICY_SKIPPED_REASON in _notes(result)
    finally:
        store.close()


def test_gate_is_above_crawl_target_and_expand_directory() -> None:
    """Статически: гейт политики — до резолва цели и развёртки каталога.

    Проверка по AST, а не по тексту: ``_author_root_plans`` — генератор, и порядок
    операторов в его теле и есть порядок исполнения. Гейт ниже ``expand_directory``
    нарушал бы «ноль запросов» на feed/channel-корне.
    """
    tree = ast.parse(Path(corpus_mod.__file__).read_text(encoding="utf-8"))
    fn = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_build_corpus"
    )
    author = next(
        node
        for node in fn.body
        if isinstance(node, ast.FunctionDef) and node.name == "_author_root_plans"
    )
    line_of: dict[str, int] = {}
    for node in ast.walk(author):
        if isinstance(node, ast.Call):
            name = ""
            if isinstance(node.func, ast.Name):
                name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                name = node.func.attr
            line_of.setdefault(name, node.lineno)
    gate = line_of.get("_policy_execution")
    assert gate is not None, "в _author_root_plans нет гейта политики"
    for lower in ("crawl_target", "expand_directory"):
        assert lower in line_of, f"ожидался вызов {lower} в _author_root_plans"
        assert gate < line_of[lower], (
            f"гейт политики ниже {lower}: запрет сети нарушался бы до фронтьера"
        )


# --------------------------------------------------------------------------- #
# 2/3. on_request: без запроса — deny + chunks; с запросом — fetch
# --------------------------------------------------------------------------- #


def test_on_request_without_refresh_scope_denies_update_and_reuses_chunks(
    tmp_path: Path,
) -> None:
    """``on_request`` + наблюдение без запроса: сеть запрещена, корпус из субстрата."""
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        version_before = state.get_url_state("p1", ROOT).doc_version_id
        assert version_before is not None

        crawler = _NoNetworkCrawler()
        extractor, chunker = _CountingExtractor(), _CountingChunker()
        _result, store, coverages, run_products, _session = _run_corpus(
            tmp_path,
            policy=_resolution(CrawlPolicyMode.ON_REQUEST),
            urls={"p1": [ROOT]},
            crawler=crawler,
            extractor=extractor,
            chunker=chunker,
            state=state,
        )
        try:
            # 1. Ноль сетевых вызовов и ноль приборов: корпус — из durable-субстрата.
            assert crawler.calls == []
            assert extractor.calls == 0 and chunker.calls == 0

            # 2. Документ есть, но он **переиспользован**, а не наблюдён.
            docs = list(store.iter_corpus_docs(RUN_ID))
            assert [doc.url for doc in docs] == [ROOT]
            assert docs[0].http_status is None
            assert docs[0].reuse_kind is not None
            assert docs[0].reuse_kind.value == "reused"

            # 3. Покрытие неполно по политике, причина названа.
            assert coverages["p1"].complete is False
            assert coverages["p1"].reason == CRAWL_POLICY_SKIPPED_REASON
            assert run_products["p1"]["status"] == RunProductStatus.PARTIAL.value

            # 4. ``url_state`` не переписан: наблюдения не было, «последней версией»
            #    переиспользованное не становится (иначе следующий живой прогон соврал бы).
            assert state.get_url_state("p1", ROOT).doc_version_id == version_before
        finally:
            store.close()


def test_refresh_scope_turns_on_request_into_fetch(tmp_path: Path) -> None:
    """``--refresh-scope`` снимает запрет ``on_request``: ровно названный корень в сети."""
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])
        crawler = _NoNetworkCrawler()
        result, store, coverages, _products, _session = _run_corpus(
            tmp_path,
            policy=_resolution(CrawlPolicyMode.ON_REQUEST, requested=("docs.test",)),
            urls={"p1": [ROOT]},
            crawler=crawler,
            extractor=_FirstRunExtractor(),
            chunker=_FirstRunChunker(),
            state=state,
        )
        try:
            # Корень назван запросом — он идёт обычным путём (и падает, потому что сеть
            # в этом тесте — заглушка, доказывающая факт вызова).
            assert crawler.calls, "запрошенный корень обязан пойти в сеть"
            assert crawler.calls[0][0] == ROOT
            assert CRAWL_POLICY_SKIPPED_REASON not in _notes(result)
            assert coverages["p1"].reason != CRAWL_POLICY_SKIPPED_REASON
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# 4. Смешанный продукт: 1 живой + 1 запрещённый корень
# --------------------------------------------------------------------------- #


def test_one_live_and_one_denied_root_is_partial_with_named_token(
    tmp_path: Path,
) -> None:
    """Живой корень наблюдён, запрещённый пропущен: продукт неполон, но не ``BLOCKED``."""
    # Поверхность решений: продукт ``auto``, второй host под запретом.
    policy = resolve_crawl_policy(
        package_policy=CrawlPolicyConfig.model_validate(
            {
                "products": {"p1": {"mode": "auto"}},
                "sites": {"p1": {"other.test": {"mode": "network_denied"}}},
            }
        ),
        operator_rows=[],
        roots=[_root(ROOT), _root(OTHER)],
        now=NOW,
        observed_at={},
    )
    crawler = _FirstRunCrawler({ROOT: BODY})
    result, store, coverages, run_products, session = _run_corpus(
        tmp_path,
        policy=policy,
        urls={"p1": [ROOT, OTHER]},
        crawler=crawler,
        extractor=_FirstRunExtractor(),
        chunker=_FirstRunChunker(),
    )
    try:
        # 1. Сеть тронута ровно живым корнем.
        assert [url for url, _plan in crawler.calls] == [ROOT]

        # 2. Покрытие неполно и названо; ожидаемое число источников не «усохло» молча:
        #    сеанс знает один элемент, а неполнота названа политическим токеном.
        coverage = coverages["p1"]
        assert coverage.complete is False
        assert coverage.reason == CRAWL_POLICY_SKIPPED_REASON
        assert coverage.observed_sources == 1

        # 3. Продукт на карте: PARTIAL (не BLOCKED) и сеанс partial.
        assert run_products["p1"]["status"] == RunProductStatus.PARTIAL.value
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL

        # 4. Запись в ``results[].notes`` (строка product_end) называет пропуск.
        notes = _notes(result)
        assert "пропущено политикой краула корней 1" in notes
        assert any(
            isinstance(entry, dict)
            and entry.get("event") == "product_end"
            and entry.get("policy_skips")
            for entry in result.log_entries
        )
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# 4b. E.6 плана 19: смешанный продукт — часть корней fetch, часть chunks
# --------------------------------------------------------------------------- #


def _mixed_policy(*, deny_site: str, roots: list[str]):
    """Продукт ``auto``; запись сайта ``deny_site`` — ``network_denied``.

    Остальные корни продукта остаются ``fetch``: это и есть смешанный продукт
    (часть корней в сеть, часть — из durable-субстрата).
    """
    return resolve_crawl_policy(
        package_policy=CrawlPolicyConfig.model_validate(
            {
                "products": {"p1": {"mode": "auto"}},
                "sites": {"p1": {deny_site: {"mode": "network_denied"}}},
            }
        ),
        operator_rows=[],
        roots=[_root(url) for url in roots],
        now=NOW,
        observed_at={},
    )


def _chunk_rows(store: SqliteStore) -> list[tuple[str, str, str]]:
    """``(chunk_id, content_hash, url)`` всех строк ``corpus_chunks`` прогона."""
    return [
        (row["chunk_id"], row["content_hash"], row["url"])
        for row in store.conn.execute(
            "SELECT chunk_id, content_hash, url FROM corpus_chunks ORDER BY rowid"
        )
    ]


def test_denied_root_reuses_its_chunks_next_to_a_fetched_root(
    tmp_path: Path,
) -> None:
    """E.6: живой корень + запрещённый корень с субстратом дают **один** корпус.

    Смешанный продукт — не «или сеть, или чанки»: живой корень наблюдается этим
    прогоном, запрещённый берёт свой документ из ``state.db`` тем же проходом, что
    ``offline`` (``label=crawl_policy``), но **в хвосте продукта** — после живых
    документов. Провенанс каждой строки при этом честный: у живой — ответ этого
    прогона, у переиспользованной — носитель, и ни один чанк не вставлен дважды.
    """
    other_body = b"other page text"
    with SqliteStateStore(":memory:") as state:
        # Наполняющий прогон: оба корня наблюдены живым путём (дефолт ``max_depth=0``
        # — ровно по документу на корень).
        _seed_substrate(
            tmp_path,
            state,
            pages={ROOT: BODY, OTHER: other_body},
            roots=[ROOT, OTHER],
        )

        crawler = _FirstRunCrawler({ROOT: BODY})
        result, store, coverages, run_products, session = _run_corpus(
            tmp_path,
            policy=_mixed_policy(deny_site="other.test", roots=[ROOT, OTHER]),
            urls={"p1": [ROOT, OTHER]},
            crawler=crawler,
            extractor=_FirstRunExtractor(),
            chunker=_FirstRunChunker(),
            state=state,
        )
        try:
            # 1. Сеть тронута ровно живым корнем.
            assert [url for url, _plan in crawler.calls] == [ROOT]

            # 2. Корпус продукта — **оба** документа: наблюдённый и переиспользованный.
            docs = {doc.url: doc for doc in store.iter_corpus_docs(RUN_ID)}
            assert set(docs) == {ROOT, OTHER}
            live = docs[ROOT]
            assert live.http_status == 200, "живой корень — ответ этого прогона"
            assert (
                live.reuse_kind is not None and live.reuse_kind.value == "revalidated"
            )
            reused = docs[OTHER]
            assert reused.http_status is None, "сети не было — статус NULL, а не 200"
            assert reused.reuse_kind is not None and reused.reuse_kind.value == "reused"
            assert reused.content_hash == content_hash(other_body)

            # 3. Чанки не задвоены: ``chunk_id`` — PK, но проверяем и по контенту —
            #    два пути (fetch и reuse) не должны оставить две записи одного куска.
            rows = _chunk_rows(store)
            assert len(rows) == 2, f"ожидался один чанк на документ: {rows}"
            assert len({row[0] for row in rows}) == len(rows)
            assert len({row[1] for row in rows}) == len(rows)
            # Счётчики честные: один фетч (живой корень) и ни одной новой нарезки — живой
            # корень ответил 200 с тем же хешем (``revalidated``), chunk-set обоих
            # документов взят из субстрата. Смешанный путь виден именно парой
            # «фетч = 1, документов = 2».
            build = next(
                entry for entry in result.log_entries if isinstance(entry, IndexBuild)
            )
            assert build.crawl_fetched == 1
            assert build.doc_counters["p1"] == {"total": 2, "new": 0, "reused": 2}

            # 4. Пропуск назван и продукт остаётся на карте.
            notes = _notes(result)
            assert CRAWL_POLICY_SKIPPED_REASON in notes
            coverage = coverages["p1"]
            assert coverage.complete is False
            assert coverage.reason == CRAWL_POLICY_SKIPPED_REASON
            assert run_products["p1"]["status"] == RunProductStatus.PARTIAL.value
            assert session is not None and session.status is CrawlSessionStatus.PARTIAL
        finally:
            store.close()


#: Перекрывающиеся корни: ``OVERLAP_PAGE`` лежит под обоими. Первый — живой (сеть),
#: второй — ``network_denied``: ровно случай «документ пришёл двумя путями».
OVERLAP_ROOT = "https://p1.test/"
OVERLAP_OTHER = "https://p1.test/docs/"
OVERLAP_PAGE = "https://p1.test/docs/a"
OVERLAP_ROOT_BODY = b'<html>root <a href="/docs/a">a</a></html>'
OVERLAP_OTHER_BODY = b'<html>docs <a href="/docs/a">a</a></html>'
OVERLAP_LEAF = b"<html>leaf text</html>"
OVERLAP_NEW_LEAF = b"<html>new text</html>"


def _overlap_run(
    tmp_path: Path,
    state: SqliteStateStore,
    *,
    seed_leaf: bytes,
    live_leaf: bytes,
):
    """Смешанный прогон «перекрывающиеся корни»: живой ``/`` + запрещённый ``/docs/``.

    Состояние наполняется **реальным** путём (``auto``, глубина 1) от запрещённого
    корня, поэтому ``url_state`` знает и ``/docs/``, и лист ``/docs/a``. В самом
    прогоне ``OVERLAP_ROOT`` идёт в сеть (BFS находит лист), ``OVERLAP_OTHER`` —
    ``network_denied``: его durable-документы (``overlap_page``) переиспользуются в
    хвосте. ``seed_leaf``/``live_leaf`` различаются только в тесте изменённого контента.
    """
    _seed_substrate(
        tmp_path,
        state,
        pages={OVERLAP_OTHER: OVERLAP_OTHER_BODY, OVERLAP_PAGE: seed_leaf},
        roots=[OVERLAP_OTHER],
        crawl_defaults=CrawlRunConfig(max_depth=1),
    )
    crawler = _FirstRunCrawler(
        {OVERLAP_ROOT: OVERLAP_ROOT_BODY, OVERLAP_PAGE: live_leaf}
    )
    result, store, coverages, run_products, _session = _run_corpus(
        tmp_path,
        policy=_mixed_policy(
            deny_site="p1.test/docs", roots=[OVERLAP_ROOT, OVERLAP_OTHER]
        ),
        urls={"p1": [OVERLAP_ROOT, OVERLAP_OTHER]},
        crawler=crawler,
        extractor=_FirstRunExtractor(),
        chunker=_FirstRunChunker(),
        state=state,
        crawl_defaults=CrawlRunConfig(max_depth=1),
    )
    return result, store, coverages, run_products, crawler


def test_overlapping_roots_do_not_duplicate_a_document(tmp_path: Path) -> None:
    """E.6: документ под двумя корнями (живым и запрещённым) входит **один** раз.

    ``OVERLAP_ROOT`` покрывает ``/docs/a`` как BFS-находку, а ``OVERLAP_OTHER``
    (запрещённый) — как известный URL своего корня (самый специфичный
    ``root_path_prefix`` выигрывает группировку). Строка корпуса одна: переиспользование
    пропускает URL, у которого строка уже есть (``skipped_present``), поэтому
    ``duplicates`` (алиасы контента) здесь пуст — схлопывание идёт по URL, а не по
    ``doc_hash``. Счётчик ``fetched``/``docs`` не удваивается, покрытие неполно по
    политическому токену.
    """
    with SqliteStateStore(":memory:") as state:
        result, store, coverages, run_products, crawler = _overlap_run(
            tmp_path, state, seed_leaf=OVERLAP_LEAF, live_leaf=OVERLAP_LEAF
        )
        try:
            # 1. Живой корень сходил в сеть за собой и за листом; запрещённый — нет.
            assert [url for url, _plan in crawler.calls] == [
                OVERLAP_ROOT,
                OVERLAP_PAGE,
            ]

            # 2. Документ под обоими корнями — **одна** строка, и он попал в корпус
            #    один раз, хотя к нему вели два пути.
            docs = list(store.iter_corpus_docs(RUN_ID))
            under_page = [doc for doc in docs if doc.url == OVERLAP_PAGE]
            assert len(under_page) == 1, f"дубль документа: {[doc.url for doc in docs]}"
            assert len(docs) == 3, "живой корень + корень запрещённого + его лист"
            # Пропуск назван строкой прохода: «два пути» не молчат.
            assert "уже в корпусе этого прогона 1" in _notes(result)
            build = next(
                entry for entry in result.log_entries if isinstance(entry, IndexBuild)
            )
            assert build.duplicates == {}, build.duplicates
            assert build.doc_counters["p1"]["total"] == len(docs)
            # Счётчик фетчей — только живые (корень и лист), переиспользование его
            # не удваивает: иначе «два пути» читались бы как два обхода.
            assert build.crawl_fetched == 2
            # Ни один чанк не повторён по контенту (и fetch, и reuse пишут в один набор).
            hashes = [row[1] for row in _chunk_rows(store)]
            assert len(hashes) == len(set(hashes))
            assert len(hashes) == len(docs), "один чанк на документ, ни одного лишнего"

            # 3. Покрытие неполно по политике, продукт на карте.
            assert coverages["p1"].complete is False
            assert coverages["p1"].reason == CRAWL_POLICY_SKIPPED_REASON
            assert run_products["p1"]["status"] == RunProductStatus.PARTIAL.value
            assert CRAWL_POLICY_SKIPPED_REASON in _notes(result)
        finally:
            store.close()


def test_live_200_over_a_reused_row_keeps_the_observation_provenance(
    tmp_path: Path,
) -> None:
    """Правило 2 владельца: провенанс строки называет наблюдение, а не носитель.

    Тот же контент, живой 200 поверх переиспользованной копии: строка **одна**, и она
    200 этого прогона (``revalidated`` — 200 с прежним ``content_hash``), с
    ``fetched_at`` наблюдения. ``reused``/``http_status=NULL`` здесь были бы ложной
    записью: сеть в этом прогоне трогали, и I19 требует называть это наблюдением.
    """
    with SqliteStateStore(":memory:") as state:
        _result, store, _coverages, _products, _crawler = _overlap_run(
            tmp_path, state, seed_leaf=OVERLAP_LEAF, live_leaf=OVERLAP_LEAF
        )
        try:
            rows = [
                doc
                for doc in store.iter_corpus_docs(RUN_ID)
                if doc.url == OVERLAP_PAGE
            ]
            assert len(rows) == 1, [doc.url for doc in rows]
            doc = rows[0]
            assert doc.http_status == 200, "сервер отвечал в этом прогоне"
            assert doc.fetched_at is not None, "время получения — факт наблюдения"
            assert doc.reuse_kind is not None
            assert doc.reuse_kind.value == "revalidated", doc.reuse_kind
        finally:
            store.close()


def test_changed_live_document_does_not_coexist_with_its_reused_copy(
    tmp_path: Path,
) -> None:
    """E.6: один ``url`` — одна строка корпуса, даже если контент изменился.

    Замер 05.11.2026 (до правки): ``corpus_docs`` нёс **обе** версии
    ``https://p1.test/docs/a`` — ``reuse_kind=reused``, ``http_status=NULL`` (из
    ``state.db``) и ``reuse_kind=live``, ``http_status=200`` (живой фетч прогона).
    Причина — порядок: переиспользование запрещённого корня писалось в планировании,
    а живой путь конвертируется в хвосте, поэтому дедуп по ``doc_hash`` их не сталкивал
    (хеши разные, URL один), и устаревший текст оставался кандидатом в доказательства
    (I19). Теперь переиспользование идёт в хвосте после живых документов и пропускает
    URL, у которого строка уже есть.
    """
    with SqliteStateStore(":memory:") as state:
        result, store, coverages, _products, crawler = _overlap_run(
            tmp_path, state, seed_leaf=b"<html>old text</html>", live_leaf=OVERLAP_NEW_LEAF
        )
        try:
            assert [url for url, _plan in crawler.calls] == [
                OVERLAP_ROOT,
                OVERLAP_PAGE,
            ]
            # Покрытие неполно по токену: дефект не читается как «корпус собран».
            assert coverages["p1"].reason == CRAWL_POLICY_SKIPPED_REASON
            assert CRAWL_POLICY_SKIPPED_REASON in _notes(result)
            docs = list(store.iter_corpus_docs(RUN_ID))
            under_page = [doc for doc in docs if doc.url == OVERLAP_PAGE]
            assert len(under_page) == 1, (
                "один URL — одна строка корпуса; в корпусе обе версии: "
                + ", ".join(
                    f"{doc.content_hash[:8]}/{doc.reuse_kind}" for doc in under_page
                )
            )
            assert under_page[0].http_status == 200, "выживает наблюдение этого прогона"
            # Устаревшей копии нет и в чанках: единственный чанк под этим URL — новый.
            texts = [
                row[2]
                for row in store.conn.execute(
                    "SELECT content_hash, url, text FROM corpus_chunks WHERE url = ?",
                    (OVERLAP_PAGE,),
                )
            ]
            assert texts == [OVERLAP_NEW_LEAF.decode()], texts
        finally:
            store.close()


#: Алиас по контенту: ``ALIAS_PAGE`` (запрещённый корень) и ``ALIAS_TWIN`` (живой корень)
#: приносят одинаковый контент, поэтому в корпусе остаётся одна строка — близнеца.
#: В ``state.db`` у ``ALIAS_PAGE`` при этом лежит **другой**, старый текст.
ALIAS_ROOT = "https://p1.test/"
ALIAS_DENIED = "https://p1.test/docs/"
ALIAS_TWIN = "https://p1.test/a"
ALIAS_PAGE = "https://p1.test/docs/x"
ALIAS_OLD = b"<html>old durable text</html>"
ALIAS_SHARED = b"<html>shared observed text</html>"
ALIAS_ROOT_BODY = b'<html>root <a href="/a">a</a><a href="/docs/x">x</a></html>'
ALIAS_DENIED_BODY = b'<html>docs <a href="/docs/x">x</a></html>'


class _NoVersionWrites:
    """Состояние, которому нельзя записать версию: ``upsert_document_version`` падает.

    Моделируется реальный, а не гипотетический отказ (занятая/недоступная БД): он
    видим в ``errors`` и не роняет фазу. Чтение — как у настоящего состояния.
    """

    def __init__(self, inner: SqliteStateStore) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def upsert_document_version(self, row: Any) -> None:
        raise RuntimeError("state: база занята (инъекция теста)")


def _alias_run(
    tmp_path: Path,
    state: SqliteStateStore,
    *,
    skip_version_writes: bool,
):
    """Смешанный прогон «алиас по контенту»: живой ``/`` + запрещённый ``/docs/``.

    Порядок обхода — по адресу (frontier ``(depth,url)``), поэтому ``/a`` идёт раньше
    ``/docs/x``: строку корпуса создаёт близнец, а запрещённый URL становится алиасом
    ``doc_hash`` и выходит до ``ingest.docs``.
    """
    crawler = _FirstRunCrawler(
        {
            ALIAS_ROOT: ALIAS_ROOT_BODY,
            ALIAS_TWIN: ALIAS_SHARED,
            ALIAS_PAGE: ALIAS_SHARED,
        }
    )
    port: Any = _NoVersionWrites(state) if skip_version_writes else state
    result, store, coverages, run_products, _session = _run_corpus(
        tmp_path,
        policy=_mixed_policy(
            deny_site="p1.test/docs", roots=[ALIAS_ROOT, ALIAS_DENIED]
        ),
        urls={"p1": [ALIAS_ROOT, ALIAS_DENIED]},
        crawler=crawler,
        extractor=_FirstRunExtractor(),
        chunker=_FirstRunChunker(),
        state=port,
        crawl_defaults=CrawlRunConfig(max_depth=1),
    )
    return result, store, coverages, run_products, crawler


def test_observed_alias_does_not_bring_back_its_stale_durable_copy(
    tmp_path: Path,
) -> None:
    """E.6, правило 2: наблюдённый URL не получает ``reused``-строку со старым текстом.

    Алиас по ``doc_hash`` выходит из ``finish_one`` **до** ``ingest.docs``, поэтому
    строка корпуса у него отсутствует — и раньше это читалось как «наблюдения не было»:
    переиспользование запрещённого корня вставляло ``ALIAS_PAGE`` с
    ``reuse_kind=reused``/``http_status=NULL`` и **старым** текстом, хотя прогон видел
    этот URL своими глазами (200) и его контент уже лежит в корпусе строкой близнеца
    (I19: устаревшая версия того же URL — не кандидат в доказательства).

    Условие достижимости названо честно: в простом случае алиас сам продвигает
    ``url_state.doc_version_id`` к наблюдённой версии (см. соседний пин), и
    переиспользование разрешает уже наблюдённый хеш. Дыра открывается, когда этой
    записи нет — например, запись версии в ``state.db`` отказала (видимая ошибка), а
    ``COALESCE``-upsert сохранил прежний указатель на старый носитель.
    """
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(
            tmp_path,
            state,
            pages={ALIAS_DENIED: ALIAS_DENIED_BODY, ALIAS_PAGE: ALIAS_OLD},
            roots=[ALIAS_DENIED],
            crawl_defaults=CrawlRunConfig(max_depth=1),
        )
        result, store, coverages, _products, crawler = _alias_run(
            tmp_path, state, skip_version_writes=True
        )
        try:
            # 1. Сеть — только живой корень: запрещённый не тронут, но алиас наблюдён.
            assert [url for url, _plan in crawler.calls] == [
                ALIAS_ROOT,
                ALIAS_TWIN,
                ALIAS_PAGE,
            ]

            # 2. Строки со старым носителем нет ни в корпусе, ни в чанках.
            docs = list(store.iter_corpus_docs(RUN_ID))
            by_url = {doc.url: doc for doc in docs}
            assert ALIAS_PAGE not in by_url, (
                "наблюдённый алиас получил строку: "
                f"{by_url[ALIAS_PAGE].reuse_kind}/{by_url[ALIAS_PAGE].content_hash[:8]}"
            )
            old_texts = [
                row[0]
                for row in store.conn.execute(
                    "SELECT text FROM corpus_chunks WHERE url = ?", (ALIAS_PAGE,)
                )
            ]
            assert old_texts == [], old_texts
            all_texts = [
                row[0] for row in store.conn.execute("SELECT text FROM corpus_chunks")
            ]
            assert ALIAS_OLD.decode() not in all_texts, "старый текст попал в корпус"

            # 3. Остальной корпус на месте: живой корень, близнец и сам запрещённый
            #    корень (его durable-документ) — состав корня не сузился.
            assert set(by_url) == {ALIAS_ROOT, ALIAS_TWIN, ALIAS_DENIED}, sorted(by_url)
            assert by_url[ALIAS_DENIED].reuse_kind is not None
            assert by_url[ALIAS_DENIED].reuse_kind.value == "reused"

            # 4. Пропуск назван, а не проглочен: иначе «наблюдение победило» читалось бы
            #    как «URL не было в состоянии».
            assert "уже в корпусе этого прогона" in _notes(result)

            # 5. ``duplicates`` честный: один алиас (X схлопнулся с Y), переиспользование
            #    его не удвоило.
            build = next(
                entry for entry in result.log_entries if isinstance(entry, IndexBuild)
            )
            assert build.duplicates.get("p1", 0) == 1, build.duplicates

            # 6. Покрытие неполно по политическому токену, продукт на карте.
            assert coverages["p1"].complete is False
            assert coverages["p1"].reason == CRAWL_POLICY_SKIPPED_REASON
        finally:
            store.close()


def test_observed_alias_points_state_at_the_observed_version(tmp_path: Path) -> None:
    """Почему простой алиас дыру не открывает: наблюдение продвигает ``url_state``.

    Алиас находит версию по **наблюдённому** ``content_hash`` (``find_doc_version``) и
    пишет её в ``url_state.doc_version_id``, поэтому переиспользование того же URL
    разрешает уже наблюдённый контент, а не вставляет прежний носитель. Пин держит
    первую линию защиты правила 2; вторую (носитель наблюдений) проверяет соседний тест —
    он же отвечает за то, чтобы тот же алиас не был посчитан ``duplicates`` дважды
    (наблюдение + кандидат переиспользования одного и того же URL).
    """
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(
            tmp_path,
            state,
            pages={ALIAS_DENIED: ALIAS_DENIED_BODY, ALIAS_PAGE: ALIAS_OLD},
            roots=[ALIAS_DENIED],
            crawl_defaults=CrawlRunConfig(max_depth=1),
        )
        result, store, coverages, _products, _crawler = _alias_run(
            tmp_path, state, skip_version_writes=False
        )
        try:
            docs = list(store.iter_corpus_docs(RUN_ID))
            by_url = {doc.url: doc for doc in docs}
            assert ALIAS_PAGE not in by_url, sorted(by_url)
            row = state.get_url_state("p1", ALIAS_PAGE)
            assert row is not None and row.doc_version_id
            version = state.get_document_version(row.doc_version_id)
            assert version is not None
            assert version.content_hash == content_hash(ALIAS_SHARED), (
                "указатель состояния обязан называть наблюдённый контент, а не старый"
            )
            build = next(
                entry for entry in result.log_entries if isinstance(entry, IndexBuild)
            )
            assert build.duplicates.get("p1", 0) == 1, build.duplicates
            assert coverages["p1"].reason == CRAWL_POLICY_SKIPPED_REASON
        finally:
            store.close()


def test_denied_root_without_prior_chunks_is_not_silently_empty(
    tmp_path: Path,
) -> None:
    """E.6: запрещённый корень без субстрата даёт **видимый** ноль, а не «пусто».

    Переиспользовать нечего — это отдельный факт от «корень ничего не содержит»:
    он называется в ``errors`` (``crawl_policy:`` + текст «нет в durable-состоянии»)
    и в операторской строке прохода (``переиспользовано документов 0``).
    """
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(tmp_path, state, pages={ROOT: BODY}, roots=[ROOT])

        crawler = _FirstRunCrawler({ROOT: BODY})
        result, store, coverages, run_products, _session = _run_corpus(
            tmp_path,
            policy=_mixed_policy(deny_site="other.test", roots=[ROOT, OTHER]),
            urls={"p1": [ROOT, OTHER]},
            crawler=crawler,
            extractor=_FirstRunExtractor(),
            chunker=_FirstRunChunker(),
            state=state,
        )
        try:
            # 1. Сеть — только живой корень.
            assert [url for url, _plan in crawler.calls] == [ROOT]

            # 2. Запрещённый корень в корпусе отсутствует, и это названо причиной:
            #    переиспользовать нечего, а не «корень пуст».
            docs = {doc.url for doc in store.iter_corpus_docs(RUN_ID)}
            assert docs == {ROOT}
            unknown = OFFLINE_UNKNOWN_ROOT_TEXT.replace("offline:", "crawl_policy:", 1)
            assert any(unknown in error for error in result.errors), result.errors
            notes = _notes(result)
            assert "переиспользовано документов 0" in notes

            # 3. Покрытие неполно по токену, продукт не ``BLOCKED``.
            assert coverages["p1"].complete is False
            assert coverages["p1"].reason == CRAWL_POLICY_SKIPPED_REASON
            assert run_products["p1"]["status"] == RunProductStatus.PARTIAL.value
        finally:
            store.close()


def test_denied_root_in_full_mode_replays_all_its_durable_documents(
    tmp_path: Path,
) -> None:
    """E.6: запрещённый корень берёт **все** свои durable-документы, не только корень.

    Продукт в режиме ``full``: ревалидационного work-set нет вовсе
    (``plan_incremental_items`` под сетью возвращает пусто для ``new``/``full``), и
    ``known_urls`` из ревалидационной группы был бы пуст — лист корня терялся бы молча
    (замер до правки: 2 документа в ``state.db`` → 1 в корпусе, ``errors`` пуст). Работа
    корня без сети — это durable-субстрат целиком: ``url_state`` и есть work-set
    (AGENTS §6.1; тот же измеренный failure mode — roots-only терял 7 документов из 9).
    """
    denied = "https://other.test/docs/"
    leaf = "https://other.test/docs/a"
    with SqliteStateStore(":memory:") as state:
        _seed_substrate(
            tmp_path,
            state,
            pages={denied: b'<html>docs <a href="/docs/a">a</a></html>', leaf: b"<html>a</html>"},
            roots=[denied],
            crawl_defaults=CrawlRunConfig(max_depth=1),
        )
        crawler = _FirstRunCrawler({ROOT: BODY})
        result, store, coverages, run_products, _session = _run_corpus(
            tmp_path,
            policy=_mixed_policy(deny_site="other.test", roots=[ROOT, denied]),
            urls={"p1": [ROOT, denied]},
            crawler=crawler,
            extractor=_FirstRunExtractor(),
            chunker=_FirstRunChunker(),
            state=state,
            # Режим продукта ``full`` — ревалидационной группы не существует.
            product_modes={"p1": "full"},
        )
        try:
            # 1. Сеть — только живой корень: запрещённый корень не тронут.
            assert [url for url, _plan in crawler.calls] == [ROOT]

            # 2. В корпусе — живой документ и **оба** durable-документа запрещённого
            #    корня: сам корень и его лист.
            docs = {doc.url: doc for doc in store.iter_corpus_docs(RUN_ID)}
            assert set(docs) == {ROOT, denied, leaf}, sorted(docs)
            for url in (denied, leaf):
                assert docs[url].reuse_kind is not None
                assert docs[url].reuse_kind.value == "reused"
                assert docs[url].http_status is None

            # 3. Потери не замаскированы ошибкой: их просто нет.
            assert not [error for error in result.errors if denied in error], result.errors

            # 4. Покрытие неполно по политическому токену, продукт на карте.
            assert coverages["p1"].complete is False
            assert coverages["p1"].reason == CRAWL_POLICY_SKIPPED_REASON
            assert run_products["p1"]["status"] == RunProductStatus.PARTIAL.value
        finally:
            store.close()


# --------------------------------------------------------------------------- #
# 5. Идентичность: ``--refresh-scope`` меняет решение, его отсутствие — нет
# --------------------------------------------------------------------------- #


def test_refresh_scope_changes_digest_and_absence_does_not() -> None:
    """Запрос входит в материал дайджеста; повтор без запроса — тот же отпечаток."""
    plain = _resolution(CrawlPolicyMode.ON_REQUEST)
    repeated = _resolution(CrawlPolicyMode.ON_REQUEST)
    assert plain.digest == repeated.digest

    requested = _resolution(CrawlPolicyMode.ON_REQUEST, requested=("docs.test",))
    assert requested.digest != plain.digest
    assert requested.decisions[0].network_mode.value == "fetch"
    assert plain.decisions[0].network_mode.value == "deny"

    # Запрос, не совпавший ни с одним корнем, решения не меняет и виден отдельно.
    stray = _resolution(CrawlPolicyMode.ON_REQUEST, requested=("nowhere.test",))
    assert stray.digest == plain.digest
    assert [item.raw for item in stray.unmatched_requested] == ["nowhere.test"]


def test_network_denied_is_not_overridden_by_refresh_scope() -> None:
    """Запрос — очередь обновления, а не отмена запрета: ``network_denied`` остаётся."""
    requested = _resolution(
        CrawlPolicyMode.NETWORK_DENIED, requested=("docs.test",)
    )
    decision = requested.decisions[0]
    assert decision.network_mode.value == "deny"
    assert decision.reason == crawl_policy_mod.REASON_NETWORK_DENIED


# --------------------------------------------------------------------------- #
# 6. Диспетчеризация: ни одного «по умолчанию сеть»
# --------------------------------------------------------------------------- #


def test_every_crawl_policy_mode_has_an_execution_branch() -> None:
    """Каждое значение ``CrawlPolicyMode`` названо в ``_execution_axes`` (AST).

    Неизвестный режим — ``AssertionError`` в коде, а не фолбэк в сеть: значение enum,
    добавленное без ветки исполнения, обязано падать, а не краулить молча.
    """
    tree = ast.parse(Path(crawl_policy_mod.__file__).read_text(encoding="utf-8"))
    fn = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_execution_axes"
    )
    named = {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "CrawlPolicyMode"
    }
    assert named == {mode.name for mode in CrawlPolicyMode}

    # И исполнение каждого значения определено (не исключение).
    for mode in CrawlPolicyMode:
        axes = crawl_policy_mod._execution_axes(
            mode=mode, decision=mode.value, requested=False, observed_at=NOW
        )
        assert axes[0].value in {"fetch", "deny"}
        assert axes[1].value in {"network", "chunks"}


def test_every_refresh_policy_value_round_trips_and_unknown_is_error() -> None:
    """``RefreshPolicy`` — без тихого ``auto``: все значения проходят, остальное — отказ."""
    for policy in RefreshPolicy:
        assert corpus_mod._as_refresh_policy(policy.value) is policy
    with pytest.raises(ValueError, match="неизвестная политика"):
        corpus_mod._as_refresh_policy("bogus")