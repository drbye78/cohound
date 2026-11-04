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
    CrawlSessionStatus,
    RunConfig,
    RunMode,
)
from core.domain.enums import CrawlPolicyMode, RefreshPolicy, RunProductStatus
from core.pipeline import RunPlan
from core.pipeline import corpus as corpus_mod
from core.pipeline import crawl_policy as crawl_policy_mod
from core.pipeline.corpus import IndexBuild, build_corpus
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