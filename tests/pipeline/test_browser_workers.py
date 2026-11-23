"""Ось ``browser_workers``: N браузерных движков, по одному на поток.

**Зачем.** Браузерный фетч был сериализован намеренно: ``BrowserCrawler`` водит одну
``_page`` без лока, а ``_PlaywrightBackend`` привязан к потоку-создателю
(``_assert_owner_thread``). Отсюда две половины одной правки, и обе проверяются здесь:

* **ось.** ``browser_workers`` резолвится цепочкой app < run < site (как ``max_documents``,
  а не как ``host_concurrency``, у которой per-site слоя нет) и входит в
  ``crawl_config_digest`` **только при объявлении** — прогон без оси обязан сохранить
  прежний ``run_id`` (та же дисциплина, что у ``host_concurrency``);
* **маршрут.** Браузерный живой фетч назван явно (``LocalFetch.route == "browser"``) и идёт
  в **свой** пул; при объявленном ``N > 1`` каждый поток поднимает свой движок через
  фабрику (thread-local), иначе все потоки взяли бы один движок и Playwright отказал бы
  каждому, кроме владельца.

**Что не проверяется здесь.** Реальный Playwright: он требует браузерных бинарей и не
живёт в CI. Проверяется контракт — «сколько потоков», «чей движок», «куда ушёл фетч»; сам
движок подменён фейком, который записывает поток каждого вызова.
"""

from __future__ import annotations

import datetime as _dt
import json
import pathlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

from core.config.deploy import CrawlConfig
from core.config.domain_profiles import DomainProfile
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import build_corpus
from core.pipeline.corpus_drain import _dispatch_wave, _WaveEntry
from core.pipeline.corpus_ingest import LocalFetch
from core.pipeline.crawl_frontier import (
    browser_worker_bound,
    resolve_crawl_limits,
)
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    ExtractionRoute,
    FetchMode,
    FetchPlan,
    RawPage,
)
from core.util.hashing import content_hash

RUN_ID = "run-browser-workers"
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures"
ROOT = "https://docs.test/"

# --------------------------------------------------------------------------- #
# (1) Ось: цепочка app < run < site и дайджест только при объявлении
# --------------------------------------------------------------------------- #

def _limits(
    *,
    app: CrawlConfig | None = None,
    run: CrawlRunConfig | None = None,
    profiles: dict[str, DomainProfile] | None = None,
    url: str = "https://docs.test/",
):
    """Разрешить оси для ``url`` той же функцией, что исполнение."""
    return resolve_crawl_limits(
        url,
        app=app if app is not None else CrawlConfig(),
        run=run,
        profiles=profiles or {},
    )


def test_browser_workers_default_does_not_change_digest() -> None:
    """Без объявления ключ не просачивается из дефолта: ``run_id`` прежний.

    Та же дисциплина, что у ``test_host_concurrency_default_does_not_change_digest``:
    дефолт — отсутствие оси, а не её значение.
    """
    limits = _limits(run=CrawlRunConfig())

    assert limits.browser_workers is None
    assert limits.browser_workers_explicit is False
    assert "browser_workers" not in limits.as_dict()


def test_browser_workers_declared_enters_digest() -> None:
    """Объявленная ось входит в словарь дайджеста: ``execution_change``, новый ``run_id``."""
    limits = _limits(run=CrawlRunConfig(browser_workers=3))

    assert limits.browser_workers == 3
    assert limits.browser_workers_explicit is True
    assert limits.as_dict()["browser_workers"] == 3


def test_browser_workers_chain_is_app_run_site() -> None:
    """Цепочка app < run < site: побеждает самый специфичный объявленный слой."""
    site = {
        "docs.test": DomainProfile(fetch=FetchMode.BROWSER, browser_workers=4),
    }
    # app
    assert _limits(app=CrawlConfig(browser_workers=1)).browser_workers == 1
    # run сильнее app
    assert (
        _limits(
            app=CrawlConfig(browser_workers=1), run=CrawlRunConfig(browser_workers=2)
        ).browser_workers
        == 2
    )
    # site сильнее run (per-site слой у этой оси есть — в отличие от host_concurrency)
    assert (
        _limits(
            app=CrawlConfig(browser_workers=1),
            run=CrawlRunConfig(browser_workers=2),
            profiles=site,
        ).browser_workers
        == 4
    )
    # необъявленный site-слой не перебивает run
    assert (
        _limits(
            run=CrawlRunConfig(browser_workers=2),
            profiles={"docs.test": DomainProfile(fetch=FetchMode.BROWSER)},
        ).browser_workers
        == 2
    )


def test_browser_worker_bound_is_the_upper_bound_of_declared_layers() -> None:
    """Размер пула когорты — максимум объявленных слоёв, а не второй резолв.

    Пул создаётся до того, как известны корни, поэтому per-URL резолв для него недоступен;
    максимум по объявленным значениям — верхняя граница любого per-URL резолва.
    """
    assert browser_worker_bound(CrawlConfig(), None, None) is None
    assert (
        browser_worker_bound(
            CrawlConfig(browser_workers=2),
            CrawlRunConfig(browser_workers=1),
            {"a.test": DomainProfile(browser_workers=3)},
        )
        == 3
    )
    assert (
        browser_worker_bound(CrawlConfig(), CrawlRunConfig(), {"a.test": DomainProfile()})
        is None
    )


# --------------------------------------------------------------------------- #
# (2) Маршрут: браузерные фетчи идут в свой пул, каждый поток — со своим движком
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


def _page_bytes(page: Any) -> bytes:
    """Байты страницы: тело или retained-файл (потребитель очереди не держит тело в RAM)."""
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return pathlib.Path(path).read_bytes() if path else b""


class _Extractor:
    """Минимальный экстрактор: текст = байты страницы (нейтрально, §1.3).

    Тело берётся из ``content`` **или** из retained-файла: потребитель очереди конверсии
    держит тело не в RAM, а по ``source_path`` (I19) — ровно так его получает воркер пула.
    """

    version = "test-extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        body = _page_bytes(page)
        return ExtractedDoc(
            text=body.decode("utf-8", "replace"),
            content_hash=content_hash(body),
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    """Один чанк на документ — нарезка здесь не предмет проверки."""

    version = "test-chunker-1"

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


class _Gate:
    """Общий счётчик одновременных фетчей **поперёк** движков.

    Держит каждый фетч, пока в полёте не окажется ``hold`` вызовов (или не истечёт
    ``hold_timeout``). Счётчик общий намеренно: параллелизм здесь — свойство **когорты**
    (N движков на N потоках), а счётчик внутри одного движка показал бы только, что один
    движок не ведёт два фетча одновременно, и ничего не сказал бы об оси.
    """

    def __init__(self, hold: int = 1, hold_timeout: float = 2.0) -> None:
        self.hold = hold
        self.hold_timeout = hold_timeout
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()

    def enter(self) -> None:
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)

    def leave(self) -> None:
        with self.lock:
            self.active -= 1

    def wait(self) -> None:
        """Ждать, пока в полёте не окажется ``hold`` фетчей (или истечёт таймаут)."""
        deadline = time.monotonic() + self.hold_timeout
        while time.monotonic() < deadline:
            with self.lock:
                if self.active >= self.hold:
                    return
            time.sleep(0.005)


class _ThreadRecordingCrawler:
    """Фейковый «браузерный движок»: отдаёт объявленные байты и пишет поток каждого вызова."""

    version = "fake-browser-1"
    engine = "browser"
    supports_browser = True

    def __init__(self, pages: dict[str, bytes], *, gate: _Gate | None = None) -> None:
        self.pages = dict(pages)
        self.threads: list[int] = []
        self.calls = 0
        self.closed = False
        self._gate = gate
        self._lock = threading.Lock()

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        with self._lock:
            self.calls += 1
            self.threads.append(threading.get_ident())
        if self._gate is not None:
            self._gate.enter()
        try:
            if self._gate is not None:
                self._gate.wait()
            return RawPage(
                url=url,
                content=self.pages[url],
                fetched_at=TS,
                fetch_mode=plan.fetch,
                lang="en",
            )
        finally:
            if self._gate is not None:
                self._gate.leave()

    def close(self) -> None:
        """Teardown: закрытие обязано произойти на потоке-владельце (проверяет тест)."""
        self.closed = True
        self.closed_thread = threading.get_ident()


class _Factory:
    """Фабрика движка на поток: каждый вызов — новый движок, и он запоминает поток."""

    def __init__(self, pages: dict[str, bytes], *, hold: int = 1) -> None:
        self._pages = dict(pages)
        self.gate = _Gate(hold=hold)
        self.engines: list[_ThreadRecordingCrawler] = []
        self._lock = threading.Lock()

    def __call__(self) -> _ThreadRecordingCrawler:
        engine = _ThreadRecordingCrawler(self._pages, gate=self.gate)
        with self._lock:
            self.engines.append(engine)
        return engine

    @property
    def threads(self) -> list[int]:
        return [tid for engine in self.engines for tid in engine.threads]

    @property
    def calls(self) -> int:
        return sum(engine.calls for engine in self.engines)


def _browser_pages(n: int) -> dict[str, bytes]:
    """Корень с ``n`` дочерними страницами — все требуют браузерного рендера."""
    links = "".join(f'<a href="https://docs.test/{i}">{i}</a>' for i in range(n))
    pages: dict[str, bytes] = {ROOT: f"<html>{links}</html>".encode()}
    for i in range(n):
        pages[f"https://docs.test/{i}"] = f"<html>page-{i}</html>".encode()
    return pages


def _browser_notes(result: Any) -> list[str]:
    """Строка наблюдения о браузерном маршруте из канала ``notes`` фазы."""
    return [
        entry
        for entry in result.log_entries
        if isinstance(entry, str) and "браузер:" in entry
    ]


def _build(tmp_path: Path, *, crawler: Any, **kwargs: Any):
    return build_corpus(
        _plan(),
        crawler,
        _Extractor(),
        _Chunker(),
        None,
        urls=kwargs.pop("urls", {"p1": [ROOT]}),
        raw_root=tmp_path,
        **kwargs,
    )


def test_browser_workers_two_runs_two_engines_on_two_threads(tmp_path: Path) -> None:
    """``browser_workers=2``: два живых браузерных фетча — два потока, два движка.

    Общий движок из карты ``crawlers`` в этом режиме не используется вовсе: иначе все
    потоки, кроме владельца, получили бы отказ ``_assert_owner_thread``.
    """
    pages = _browser_pages(2)
    shared = _ThreadRecordingCrawler(pages)  # движок из карты: не должен фетчить
    factory = _Factory(pages, hold=2)  # ждёт перекрытия: иначе «2 потока» — совпадение
    explicit = {url: FetchMode.BROWSER for url in pages}

    result = _build(
        tmp_path,
        crawler=shared,
        browser_factory=factory,
        explicit_fetch=explicit,
        crawl_defaults=CrawlRunConfig(max_depth=1, max_workers=2, browser_workers=2),
    )

    assert result.status == "ok"
    assert result.errors == []
    # Каждый движок — свой поток, и потоков ровно два (по одному на движок).
    assert len(factory.engines) == 2
    assert len(set(factory.threads)) == 2
    # Параллелизм действительно случился: счётчик общий на когорту и видел два фетча
    # в полёте одновременно. Без этого «два потока» могло быть совпадением расписания.
    assert factory.gate.max_active == 2
    assert factory.calls == 3
    # Общий движок из карты не фетчил: маршрут разрешил движок внутри воркера.
    assert shared.calls == 0
    # Teardown закрыл **каждый** движок и закрыл на его собственном потоке: иначе
    # браузерный процесс жил бы до конца прогона (сотни МБ), а Playwright отказал бы
    # закрытию из чужого потока.
    assert all(engine.closed for engine in factory.engines)
    for engine in factory.engines:
        assert engine.closed_thread in engine.threads
    # Наблюдение параллелизма опубликовано строкой провенанса.
    notes = _browser_notes(result)
    assert len(notes) == 1
    assert "пик в полёте 2" in notes[0]


def test_browser_workers_one_keeps_the_previous_single_engine(tmp_path: Path) -> None:
    """``browser_workers=1``: фабрика не включается — прежний один движок и поток.

    Ось объявлена единицей, то есть поведение не изменилось: движок берётся из карты
    ``crawlers`` (как до оси), фабрика не вызывается, поток один.
    """
    pages = _browser_pages(2)
    shared = _ThreadRecordingCrawler(pages)
    factory = _Factory(pages, hold=1)
    explicit = {url: FetchMode.BROWSER for url in pages}

    result = _build(
        tmp_path,
        crawler=shared,
        browser_factory=factory,
        explicit_fetch=explicit,
        crawl_defaults=CrawlRunConfig(max_depth=1, max_workers=2, browser_workers=1),
    )

    assert result.status == "ok"
    assert result.errors == []
    assert factory.engines == []  # фабрика не тронута
    assert shared.calls == 3
    assert len(set(shared.threads)) == 1  # один поток-владелец: пул шириной 1


def test_browser_workers_unset_keeps_the_previous_single_engine(tmp_path: Path) -> None:
    """Ось не объявлена: тот же маршрут, что до правки (один движок, один поток)."""
    pages = _browser_pages(2)
    shared = _ThreadRecordingCrawler(pages)
    factory = _Factory(pages, hold=1)
    explicit = {url: FetchMode.BROWSER for url in pages}

    result = _build(
        tmp_path,
        crawler=shared,
        browser_factory=factory,
        explicit_fetch=explicit,
        crawl_defaults=CrawlRunConfig(max_depth=1, max_workers=2),
    )

    assert result.status == "ok"
    assert factory.engines == []
    assert shared.calls == 3
    assert len(set(shared.threads)) == 1


def test_browser_blocked_items_never_reach_a_pool(tmp_path: Path) -> None:
    """``browser_blocked`` — живого фетча нет: ни HTTP-пул, ни пул движков не получают его.

    Движок без ``supports_browser`` (HTTP-краулер) и объявленный ``browser_workers``:
    маршрут не должен поднять фабрику ради записи, которой нечего фетчить.
    """
    pages = _browser_pages(1)
    shared = _ThreadRecordingCrawler(pages)
    shared.supports_browser = False  # браузерный рендер невозможен
    factory = _Factory(pages, hold=1)
    explicit = {url: FetchMode.BROWSER for url in pages}

    result = _build(
        tmp_path,
        crawler=shared,
        browser_factory=factory,
        explicit_fetch=explicit,
        crawl_defaults=CrawlRunConfig(max_depth=1, browser_workers=2),
    )

    # Фетчей не было вовсе, и это названо: отказ виден, а не проглочен.
    assert shared.calls == 0
    assert factory.engines == []
    assert any("browser" in line for line in result.errors)


# --------------------------------------------------------------------------- #
# (3) Инварианты маршрута: «не poolable» больше не означает «браузерный»
# --------------------------------------------------------------------------- #

def test_dispatch_rejects_live_fetch_without_route() -> None:
    """Живой фетч без названного маршрута — дефект сборки дескриптора, а не режим."""
    desc = LocalFetch(
        pid="p1",
        wu=ROOT,
        depth=0,
        fetch_plan=FetchPlan(FetchMode.HTTP),
        active=None,
        engine_value=None,
        prior_item=None,
        resume=None,
        browser_blocked=False,
        poolable=False,  # не в HTTP-пул…
        route="fetch",  # …и не браузерный маршрут: состояние, которого не бывает
        host="docs.test",
        scope_host=None,
        enforce_redirect_scope=False,
    )
    entries = [_WaveEntry("p1", ROOT, 0, "fetch", desc=desc)]
    with (
        ThreadPoolExecutor(max_workers=1) as pool,
        pytest.raises(AssertionError, match="без маршрута"),
    ):
        _dispatch_wave(
            entries,
            pool=pool,
            browser_pool=pool,
            local_fetch=lambda d: RawPage(url=d.wu, content=b"", fetched_at=TS),
        )


def test_dispatch_rejects_browser_route_without_its_pool() -> None:
    """Браузерный маршрут без пула движков — инвариант, а не тихий inline."""
    desc = LocalFetch(
        pid="p1",
        wu=ROOT,
        depth=0,
        fetch_plan=FetchPlan(FetchMode.BROWSER),
        active=None,
        engine_value="browser",
        prior_item=None,
        resume=None,
        browser_blocked=False,
        poolable=False,
        route="browser",
        host="docs.test",
        scope_host=None,
        enforce_redirect_scope=False,
    )
    entries = [_WaveEntry("p1", ROOT, 0, "fetch", desc=desc)]
    with (
        ThreadPoolExecutor(max_workers=1) as pool,
        pytest.raises(AssertionError, match="браузерный маршрут без своего пула"),
    ):
        _dispatch_wave(
            entries,
            pool=pool,
            browser_pool=None,
            local_fetch=lambda d: RawPage(url=d.wu, content=b"", fetched_at=TS),
        )
