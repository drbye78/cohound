"""F8 (17 §7.2 п.2): объявленная вежливость по host'ам **пишется** в провенанс корня.

Критерий приёмки §7.2 №1 — «``observed_min_interval_s ≥ объявленного минимума`` на каждом
host». Проверить его без durable-записи объявленного значения нечем (читатель подставлять
его не вправе), поэтому проверяется **producer**: `rate_limits` появляется в записи корня
``effective_limits.roots[*]``, значение — из плана, которым фетчили
(``FetchPlan.rate_limit`` после резолва профиля сайта), ключ — host лимитера.

Проверки:

1. мульти-хост: в карте оба host'а с объявленными минимумами профилей, и
   :func:`core.pipeline.crawl_metric.build_crawl_metric` из этого провенанса даёт непустые
   ``declared_min_interval_s``/``ratio`` (раньше там был ``null``);
2. корень с одним host'ом — в карте ровно он;
3. объявленный host без попыток в карту не попадает;
4. нарушение вежливости видно, когда наблюдённый интервал меньше объявленного;
5. ``min_s: 0`` пишется как объявленный факт (не выбрасывается) и даёт ``ratio: null``,
   ``violation: false``;
6. старый провенанс (per-root ``limits``) читается как прежде.

Данные нейтральны (§1.3): ``docs.test``/``h0.docs.test``/``p1``.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Any

from core.config.domain_profiles import DomainProfile
from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlItemStatus,
    CrawlRunConfig,
    RateLimitSpec,
    RunConfig,
    RunMode,
)
from core.pipeline import RunPlan
from core.pipeline.corpus import build_corpus
from core.pipeline.corpus_ingest import FETCH_RETRY_TIMES
from core.pipeline.crawl_metric import (
    build_crawl_metric,
    declared_min_intervals,
)
from core.pipeline.crawl_session import default_session_id, read_provenance
from core.ports.crawl_errors import CrawlError, CrawlNotFound
from core.ports.types import (
    Chunk,
    ExtractedDoc,
    FetchPlan,
    RawPage,
)
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash
from core.util.http_audit import note_request

RUN_ID = "run-rate-provenance"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-01-01T00:00:00+00:00"
ROOT = "https://docs.test/"
FIXTURES = Path(__file__).parent / "fixtures"

#: Объявленные минимумы: корень 0.05 с, ``h0`` 0.02 с, ``h1`` 0.03 с. Значения малы, но
#: **различны** — карта обязана назвать каждый host своим числом.
ROOT_MIN = 0.05
H0_MIN = 0.02
H1_MIN = 0.03


def _catalog() -> list[CatalogRow]:
    import json

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


class _Crawler:
    """Краулер ``url -> bytes``; сеть не нужна (контракт порта)."""

    version = "crawler-1"
    engine = "http"
    supports_browser = False

    def __init__(self, pages: dict[str, bytes]) -> None:
        self.pages = dict(pages)

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        body = self.pages.get(url, b"")
        return RawPage(url=url, content=body, fetched_at=TS)


class _FlakyCrawler(_Crawler):
    """Краулер по сценарию: отдаёт отказы из очереди, затем страницу.

    Каждый вызов отмечает запрос в ``http_audit`` — так же, как настоящий транспорт:
    без этого прибор не отличил бы повтор от одного запроса, и проверка
    ``retries == http_requests - items`` была бы тавтологией.
    """

    def __init__(self, pages: dict[str, bytes], failures: list[Exception]) -> None:
        super().__init__(pages)
        self.failures = list(failures)
        self.calls = 0

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        self.calls += 1
        note_request(url)
        if self.failures:
            raise self.failures.pop(0)
        return super().fetch(url, plan)


class _Extractor:
    """Детерминированный экстрактор: тело страницы как есть.

    ``ExtractedDoc`` — доменный тип с обязательными ``text``/``content_hash``/
    ``extractor_version``: поля ``url``/``title``/``lang`` у него нет, и конструктор с
    ними падал ``TypeError`` внутри извлечения. Пайплайн ловит сбой экстрактора как
    грациозную деградацию, поэтому тест оставался зелёным, не доходя ни до чанкера,
    ни до корпуса.

    Байты берутся **тем же носителем, что у адаптера**: конверсия идёт из строки
    очереди (F5), ``RawPage.content`` там пуст, тело лежит по ``source_path``. Чтение
    только ``content`` давало пустой текст — и документ с нулевой длиной.
    """

    version = "extractor-1"

    def extract(self, page: RawPage, route: Any = None) -> ExtractedDoc:
        text = _page_bytes(page).decode("utf-8", errors="replace")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(text),
            extractor_version=self.version,
        )


def _page_bytes(page: RawPage) -> bytes:
    """Байты страницы: тело или retained-файл (``content`` у строки очереди пуст)."""
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return Path(path).read_bytes() if path else b""


class _Chunker:
    """Детерминированный чанкер: один чанк на документ (кусок — весь текст, срез).

    Сигнатура — порт ``Chunk(document_id, text)``… точнее, порт-путь
    ``chunk(doc, document_id)``: прежняя версия принимала второй аргумент только
    ключевым (``**kwargs``), пайплайн звал позиционно, ``TypeError`` глотался
    ``try`` вокруг нарезки — тест был зелёным, ни разу не проверив чанк.
    """

    version = "chunker-1"

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]:
        return [
            Chunk(
                chunk_id=f"{document_id}#0",
                chunk_no=0,
                text=doc.text,
                heading_path="",
                context_prefix="",
                content_hash=content_hash(doc.text),
                # Кусок — весь документ, срез честный: смещения не выдуманы.
                char_start=0,
                char_end=len(doc.text),
            )
        ]


def _profiles() -> dict[str, DomainProfile]:
    """Профили сайтов с **объявленным** ``rate_limit`` на host (тот, что резолвит план)."""
    return {
        "h0.docs.test": DomainProfile(
            rate_limit=RateLimitSpec(min_s=H0_MIN, max_s=H0_MIN)
        ),
        "h1.docs.test": DomainProfile(
            rate_limit=RateLimitSpec(min_s=H1_MIN, max_s=H1_MIN)
        ),
    }


def _pages(*links: str) -> dict[str, bytes]:
    anchors = "".join(f'<a href="{url}">{url}</a>' for url in links)
    pages: dict[str, bytes] = {ROOT: f"<html>{anchors}</html>".encode()}
    for url in links:
        pages[url] = f"<html>{url}</html>".encode()
    return pages


def _crawl(tmp_path: Path, pages: dict[str, bytes], **kwargs: Any) -> SqliteStore:
    """Синтетический краул в свой ``:memory:``-store; сеанс остаётся в нём же."""
    store = SqliteStore(":memory:")
    crawler = kwargs.pop("crawler", None) or _Crawler(pages)
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
    build_corpus(
        _plan(),
        crawler,
        _Extractor(),
        _Chunker(),
        store,
        urls={"p1": [ROOT]},
        raw_root=tmp_path,
        run_id=RUN_ID,
        domain_profiles=kwargs.pop("domain_profiles", _profiles()),
        crawl_defaults=kwargs.pop(
            "crawl_defaults",
            CrawlRunConfig(
                max_depth=1,
                max_workers=2,
                rate_limit=RateLimitSpec(min_s=ROOT_MIN, max_s=ROOT_MIN),
            ),
        ),
        **kwargs,
    )
    return store


def _root_record(store: SqliteStore) -> dict[str, Any]:
    """Запись корня из durable-провенанса сеанса (``effective_limits.roots[0]``)."""
    session = store.get_crawl_session(SESSION_ID)
    assert session is not None
    provenance = read_provenance(session)
    roots = (provenance.get("effective_limits") or {}).get("roots") or []
    assert len(roots) == 1, roots
    return dict(roots[0])


def _items(store: SqliteStore) -> list[Any]:
    """Элементы сеанса — вход метрики (те же durable-факты)."""
    return list(store.iter_crawl_session_items(SESSION_ID))


# --------------------------------------------------------------------------- #
# (1) мульти-хост: карта по двум host'ам + метрика перестаёт быть null
# --------------------------------------------------------------------------- #


def test_root_record_carries_rate_limits_per_host_and_metric_uses_them(
    tmp_path: Path,
) -> None:
    """Оба host'а попыток — в ``rate_limits``, и ``declared``/``ratio`` из неё непусты."""
    store = _crawl(
        tmp_path,
        _pages("https://h0.docs.test/a", "https://h1.docs.test/b"),
    )
    try:
        record = _root_record(store)
        # Значения — ровно объявленные: профиль сайта для h0/h1, run-дефолт для корня.
        assert record["rate_limits"] == {
            "docs.test": {"min_s": ROOT_MIN, "max_s": ROOT_MIN},
            "h0.docs.test": {"min_s": H0_MIN, "max_s": H0_MIN},
            "h1.docs.test": {"min_s": H1_MIN, "max_s": H1_MIN},
        }
        # Пишется туда же, где живёт политика усечения, — в запись корня, не в ``limits``:
        # вежливость не входит в ``crawl_config_digest`` и ``run_id`` не сдвигает.
        assert "rate_limits" not in record["limits"]
        # Пин антидефекта: чанкер обязан быть вызван по порту, иначе ``TypeError``
        # глотается ``try`` вокруг нарезки и тест проверяет всё, кроме чанков.
        chunked = store.conn.execute(
            "SELECT COUNT(*) AS n FROM corpus_chunks"
        ).fetchone()["n"]
        assert chunked > 0, "нарезка не дошла до corpus_chunks"

        session = store.get_crawl_session(SESSION_ID)
        metric = build_crawl_metric(
            session=session,
            items=_items(store),
            provenance=read_provenance(session),
        )
        declared = {
            row["host"]: row["declared_min_interval_s"] for row in metric["per_host"]
        }
        # Критерий приёмки: там, где профиль объявлен, ``null`` быть не может.
        assert declared == {
            "docs.test": ROOT_MIN,
            "h0.docs.test": H0_MIN,
            "h1.docs.test": H1_MIN,
        }
        # Один запрос на host — интервала нет, но объявленное значение уже измеримо:
        # отсутствие наблюдения не выдаётся за отсутствие объявления.
        assert all(row["ratio"] is None for row in metric["per_host"])
        assert not any("не восстановим" in note for note in metric["notes"])
    finally:
        store.close()


def test_two_attempts_on_one_host_give_ratio_against_declared(
    tmp_path: Path,
) -> None:
    """Две попытки на одном host'е: ``observed`` есть, ``ratio`` считается и ≥ 1.

    Лимитер пейсит ``uniform(min,max)``; при ``min == max`` интервал между соседними
    попытками не меньше объявленного — это и есть проверка §7.2 №1 на живом пути.
    """
    store = _crawl(
        tmp_path,
        _pages("https://h0.docs.test/a", "https://h0.docs.test/b"),
        crawl_defaults=CrawlRunConfig(
            max_depth=1,
            max_workers=2,
            rate_limit=RateLimitSpec(min_s=0.0, max_s=0.0),
        ),
    )
    try:
        record = _root_record(store)
        assert record["rate_limits"]["h0.docs.test"] == {
            "min_s": H0_MIN,
            "max_s": H0_MIN,
        }

        session = store.get_crawl_session(SESSION_ID)
        metric = build_crawl_metric(
            session=session,
            items=_items(store),
            provenance=read_provenance(session),
        )
        row = next(r for r in metric["per_host"] if r["host"] == "h0.docs.test")
        assert row["requests"] == 2
        assert row["declared_min_interval_s"] == H0_MIN
        assert row["observed_min_interval_s"] is not None
        assert row["ratio"] is not None and row["ratio"] >= 1.0
        assert row["politeness_violation"] is False
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (2) корень с одним host'ом
# --------------------------------------------------------------------------- #


def test_single_host_root_maps_only_its_own_host(tmp_path: Path) -> None:
    """Корень без внешних ссылок: в карте только host самого корня."""
    store = _crawl(
        tmp_path,
        _pages(),
        crawl_defaults=CrawlRunConfig(
            max_depth=0,
            rate_limit=RateLimitSpec(min_s=0.0, max_s=0.0),
        ),
    )
    try:
        record = _root_record(store)
        assert record["rate_limits"] == {"docs.test": {"min_s": 0.0, "max_s": 0.0}}
        # ``min_s: 0`` — объявленный факт, а не «не объявлено»: ключ есть, значение 0.
        assert declared_min_intervals({"effective_limits": {"roots": [record]}}) == {
            "docs.test": (0.0,)
        }

        session = store.get_crawl_session(SESSION_ID)
        metric = build_crawl_metric(
            session=session,
            items=_items(store),
            provenance=read_provenance(session),
        )
        row = metric["per_host"][0]
        # Одна попытка → интервала нет; ноль в объявлении значит «нарушать нечего».
        assert row["observed_min_interval_s"] is None
        assert row["declared_min_interval_s"] == 0.0
        assert row["ratio"] is None
        assert row["politeness_violation"] is False
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (3) объявленный, но не посещённый host — не в карте
# --------------------------------------------------------------------------- #


def test_declared_host_without_attempts_is_absent_from_map(tmp_path: Path) -> None:
    """Профиль для ``h1`` объявлен, но попыток не было — карта его не называет."""
    profiles = _profiles()
    store = _crawl(
        tmp_path,
        _pages("https://h0.docs.test/a"),
        domain_profiles=profiles,
        crawl_defaults=CrawlRunConfig(
            max_depth=1,
            max_workers=1,
            rate_limit=RateLimitSpec(min_s=0.0, max_s=0.0),
        ),
    )
    try:
        record = _root_record(store)
        assert set(record["rate_limits"]) == {"docs.test", "h0.docs.test"}
        assert "h1.docs.test" not in record["rate_limits"]
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (4) нарушение вежливости считается из записанного объявления
# --------------------------------------------------------------------------- #


def test_violation_is_computed_from_recorded_declaration(tmp_path: Path) -> None:
    """Интервал наблюдённый меньше объявленного → ``politeness_violation``, а не тишина.

    Элементы берутся из настоящего прогона, но метки попыток подменяются на заведомо
    более близкие: проверяется именно **сравнение с записанным** объявлением (если бы
    провенанс его не нёс, вернулся бы ``null``, и нарушение осталось бы незамеченным).
    """
    store = _crawl(
        tmp_path,
        _pages("https://h0.docs.test/a", "https://h0.docs.test/b"),
        crawl_defaults=CrawlRunConfig(
            max_depth=1,
            max_workers=2,
            rate_limit=RateLimitSpec(min_s=0.0, max_s=0.0),
        ),
    )
    try:
        session = store.get_crawl_session(SESSION_ID)
        provenance = read_provenance(session)
        # Подменяются метки **только у попыток h0** (соседние в 10 мс против объявленных
        # 20 мс): сравнивается записанное объявление, а не форма очереди сеанса.
        attempts = sorted(
            (item for item in _items(store) if item.url.startswith("https://h0.docs.test/")),
            key=lambda item: item.url,
        )
        assert len(attempts) == 2
        stamps = (
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00.010000+00:00",
        )
        items = [
            item.model_copy(update={"fetch_started_at": started})
            for item, started in zip(attempts, stamps, strict=True)
        ]
        metric = build_crawl_metric(
            session=session, items=items, provenance=provenance
        )
        row = next(r for r in metric["per_host"] if r["host"] == "h0.docs.test")
        assert row["observed_min_interval_s"] == 0.01
        assert row["declared_min_interval_s"] == H0_MIN
        assert row["ratio"] == 0.01 / H0_MIN
        assert row["politeness_violation"] is True
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (4b) прибор темпа: ожидание лимитера и наблюдённая частота — durable-факты
# --------------------------------------------------------------------------- #


def test_limiter_waits_and_rate_are_published_with_declared_pause(
    tmp_path: Path,
) -> None:
    """Объявленная пауза видна числами: интервалы соблюдены, ожидание посчитано.

    Проверяется связка двух носителей одного факта: интервалы — из элементов сеанса
    (``fetch_started_at``), а ``limiter_waits``/``limiter_waited_s`` — из блока ``pacing``
    провенанса (считает сам лимитер). Метрика краула обязана показать оба, не подменяя
    одно другим.
    """
    store = _crawl(
        tmp_path,
        _pages("https://h0.docs.test/a", "https://h0.docs.test/b"),
        crawl_defaults=CrawlRunConfig(
            max_depth=1,
            max_workers=2,
            rate_limit=RateLimitSpec(min_s=ROOT_MIN, max_s=ROOT_MIN),
        ),
    )
    try:
        session = store.get_crawl_session(SESSION_ID)
        provenance = read_provenance(session)
        pacing = provenance["pacing"]
        h0 = pacing["hosts"]["h0.docs.test"]
        assert h0["requests"] == 2
        # Оба запроса к h0 прошли через пейсинг с объявленным минимумом профиля.
        assert h0["limiter_waits"] >= 2
        assert h0["limiter_waited_s"] >= 2 * H0_MIN
        # Пик на h0 — 1: два запроса одного host'а разнесены паузой, а не одновременны.
        assert h0["in_flight_max"] == 1
        assert h0["requests_per_s"] is not None and h0["requests_per_s"] > 0.0
        assert pacing["total"]["requests"] == 3  # корень + два ребёнка

        metric = build_crawl_metric(
            session=session,
            items=_items(store),
            provenance=provenance,
        )
        row = next(r for r in metric["per_host"] if r["host"] == "h0.docs.test")
        # Метрика читает тот же блок: второго расчёта нет.
        assert row["limiter_waits"] == h0["limiter_waits"]
        assert row["limiter_waited_s"] == h0["limiter_waited_s"]
        assert row["in_flight_max"] == 1
        assert row["requests_per_s"] == h0["requests_per_s"]
        # Интервалы соблюдены: метка попытки — момент резерва, шаг ≥ объявленного.
        assert row["observed_min_interval_s"] >= H0_MIN
        assert row["politeness_violation"] is False
    finally:
        store.close()


def test_metric_without_pacing_block_names_it_not_zero(tmp_path: Path) -> None:
    """Сеанс без блока темпа: поля ``null`` и заметка, а не ``0`` (не измеряли ≠ ноль)."""
    store = _crawl(
        tmp_path,
        _pages(),
        crawl_defaults=CrawlRunConfig(
            max_depth=0,
            rate_limit=RateLimitSpec(min_s=0.0, max_s=0.0),
        ),
    )
    try:
        session = store.get_crawl_session(SESSION_ID)
        provenance = read_provenance(session)
        provenance.pop("pacing", None)  # как у старого/прерванного сеанса
        metric = build_crawl_metric(
            session=session, items=_items(store), provenance=provenance
        )
        row = metric["per_host"][0]
        assert row["requests_per_s"] is None
        assert row["in_flight_max"] is None
        assert row["limiter_waits"] is None
        assert row["limiter_waited_s"] is None
        assert metric["pacing"] is None
        assert any("pacing: не измерялся" in note for note in metric["notes"])
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (5) совместимость: старый провенанс (per-root ``limits``) читается как прежде
# --------------------------------------------------------------------------- #


def test_legacy_per_root_provenance_still_reads() -> None:
    """Провенанс без ``rate_limits`` (только ``limits``) — прежний путь чтения."""
    legacy = {
        "effective_limits": {
            "roots": [
                {
                    "product_id": "p1",
                    "root": "https://legacy.test/docs",
                    "limits": {"rate_limit": {"min_s": 1.5, "max_s": 3.0}},
                }
            ]
        }
    }
    assert declared_min_intervals(legacy) == {"legacy.test": (1.5,)}


def test_per_host_map_wins_over_root_scoped_fallback() -> None:
    """Если в записи есть и карта, и прежний ключ — читается карта (она точнее)."""
    mixed = {
        "effective_limits": {
            "roots": [
                {
                    "root": "https://a.test/",
                    "rate_limit": {"min_s": 9.0},
                    "rate_limits": {
                        "a.test": {"min_s": 1.0},
                        "cdn.a.test": {"min_s": 0.25},
                    },
                }
            ]
        }
    }
    assert declared_min_intervals(mixed) == {"a.test": (1.0,), "cdn.a.test": (0.25,)}

# --------------------------------------------------------------------------- #
# (6) ретраи: каждая попытка — свой слот лимитера, повтор виден в приборе
# --------------------------------------------------------------------------- #


def test_transient_500_is_retried_with_a_new_slot_and_item_still_fetched(
    tmp_path: Path,
) -> None:
    """500 → 200: элемент ``fetched`` (``attempts = 1``), а прибор называет повтор числом.

    Дефект 07.11.2026: ретрай жил в движке, внутри одного резерва лимитера, и на хост
    уходило до трёх запросов без интервала. Здесь повтор идёт слоем фетча — значит,
    ``retries`` в провенансе и ``http_requests`` обязаны это показать.
    """
    crawler = _FlakyCrawler(
        {ROOT: b"<html>root</html>"},
        [CrawlError("HTTP 500", http_status=500)],
    )
    store = _crawl(
        tmp_path, {ROOT: b"<html>root</html>"}, crawler=crawler
    )
    try:
        items = _items(store)
        assert len(items) == 1
        assert items[0].status is CrawlItemStatus.FETCHED
        # Попытка элемента работы одна: сеть повторялась внутри неё, а не снаружи
        # (семантика ``--max-attempts`` не меняется).
        assert items[0].attempts == 1

        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        host = read_provenance(session)["pacing"]["hosts"]["docs.test"]
        assert crawler.calls == 2  # первая попытка + один повтор
        assert host["requests"] == 2  # две фетч-попытки
        assert host["http_requests"] == 2  # по одному запросу на попытку
        assert host["retries"] == 1
        # Тождество прибора: повтор — ровно лишний запрос сверх элемента работы
        # (у внедрённого шва нет ни ленты, ни хопов).
        assert host["retries"] == host["http_requests"] - len(items)
    finally:
        store.close()


def test_terminal_404_is_not_retried(tmp_path: Path) -> None:
    """404 — определённый ответ, а не транзиентный сбой: одна попытка, ``retries = 0``."""
    crawler = _FlakyCrawler(
        {ROOT: b"<html>root</html>"},
        [CrawlNotFound("HTTP 404", http_status=404)],
    )
    store = _crawl(
        tmp_path, {ROOT: b"<html>root</html>"}, crawler=crawler
    )
    try:
        items = _items(store)
        assert len(items) == 1
        assert items[0].status is CrawlItemStatus.FAILED
        assert items[0].error_kind == "not_found"

        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        host = read_provenance(session)["pacing"]["hosts"]["docs.test"]
        assert crawler.calls == 1  # повтор адресовался бы тому, что уже ответило
        assert host["retries"] == 0
        assert host["requests"] == 1
    finally:
        store.close()


def test_exhausted_network_failure_marks_item_failed_after_all_attempts(
    tmp_path: Path,
) -> None:
    """Сетевой сбой на каждой попытке: ``failed``/``source_unavailable``, ``attempts = 1``.

    Повторов ровно :data:`FETCH_RETRY_TIMES` — столько же, сколько было у движка; каждая
    попытка взяла свой слот, и это видно числом, а не выводится из текста ошибки.
    """
    crawler = _FlakyCrawler(
        {ROOT: b"<html>root</html>"},
        [CrawlError("обрыв соединения") for _ in range(1 + FETCH_RETRY_TIMES)],
    )
    store = _crawl(
        tmp_path, {ROOT: b"<html>root</html>"}, crawler=crawler
    )
    try:
        items = _items(store)
        assert len(items) == 1
        assert items[0].status is CrawlItemStatus.FAILED
        assert items[0].error_kind == "source_unavailable"
        # Сеть повторялась, попытка элемента — одна.
        assert items[0].attempts == 1

        session = store.get_crawl_session(SESSION_ID)
        assert session is not None
        host = read_provenance(session)["pacing"]["hosts"]["docs.test"]
        assert crawler.calls == 1 + FETCH_RETRY_TIMES
        assert host["retries"] == FETCH_RETRY_TIMES
        assert host["http_requests"] == 1 + FETCH_RETRY_TIMES
    finally:
        store.close()
