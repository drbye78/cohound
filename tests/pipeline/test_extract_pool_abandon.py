"""Abandon пула экстракции — свойство **пула**, а не документа (A3, I2c).

Измеренный дефект (прогон ``ru-pg-dbms-145ad66ca3f9``, 13.11.2026): 16 770 адресов
получили ``crawl_session_items.status='failed'``/``error_kind='parser_failed'``, хотя ни
один из них не извлекался — пул ключа ``(level, pipeline, window)`` был abandon'ен, и код
помечал **весь остаток корня** терминальным отказом. ``parser_failed`` входит в
``BLOCKING_FAILURE_KINDS``, поэтому покрытие продукта объявлялось неполным (I2c), а
``corpus_chunks`` по продукту оставался нулевым — при сохранённых байтах всех документов.

Правильный факт — «не пытались»: строка остаётся ``pending`` (или возвращается из
``claimed``), элемент сеанса не трогается, причина видна одной записью в ``errors`` на
ключ, и работа дочиняется существующим путём (``hound extract consume`` без сети).

Что пинится:

1. строки abandon'енного ключа не забираются (``pending``), а забранные возвращаются;
2. элемента сеанса ``failed`` по ним нет — «не пытались» ≠ «извлечение провалилось»;
3. цикл завершается: обращений к пулу ровно столько, сколько строк успело дойти до
   abandon'а, и пересборки остаются ограниченными;
4. член контейнера при abandon'е получает ``not_attempted``, а не ``extract_failed``;
   строка контейнера остаётся работой очереди, спуск не считается провалившимся;
5. настоящий отказ экстрактора по-прежнему ``parser_failed`` (фикс не «вылечил» провалы);
6. текста «терминально FAILED» в ``errors`` при abandon'е нет — обещание снято вместе с
   поведением.

Нейтральные данные (§1.3): ``p1``, ``docs.test``. Сеть не используется.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import tests.pipeline.test_corpus_extract_pool as base
from core.domain import (
    CrawlItemStatus,
    CrawlRunConfig,
    DocumentLevel,
    ExtractQueueState,
    PdfPipeline,
)
from core.pipeline.archive_descent import SKIP_EXTRACT_FAILED, SKIP_NOT_ATTEMPTED
from core.pipeline.extract_consume import member_pool_exhausted_outcome
from core.pipeline.extraction_runtime import _EXTRACT_MAX_POOL_REBUILDS, ExtractPools
from core.store.sqlite_store import SqliteStore

# Прогон — тот же, что у модуля-харнесса: общий ``run_id`` не мешает, потому что
# каждая фикстура живёт в своей ``:memory:``-БД. Свой ``run_id`` потребовал бы
# подменять модульную константу харнесса, а это утечка между тестами.
RUN_ID = base.RUN_ID
SESSION_ID = base.default_session_id(RUN_ID)
ROOT = base.ROOT
PAGES = 8
ZIP_URL = "https://docs.test/z.zip"


# --------------------------------------------------------------------------- #
# Харнесс: тот же, что у пула (фейковый исполнитель + нейтральные порты)
# --------------------------------------------------------------------------- #


def _pages(*, with_container: bool = False) -> dict[str, bytes]:
    """Корень со ссылками на ``p0..p7`` (и контейнер ``z.zip`` — последним по URL)."""
    links = "".join(f'<a href="/p{i}">p{i}</a>' for i in range(PAGES))
    if with_container:
        links += '<a href="/z.zip">zip</a>'
    pages: dict[str, bytes] = {ROOT: f"<html>{links}</html>".encode()}
    for i in range(PAGES):
        pages[f"https://docs.test/p{i}"] = f"<html>page {i}</html>".encode()
    if with_container:
        pages[ZIP_URL] = _zip({"a.md": b"member a", "b.md": b"member b"})
    return pages


def _zip(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return buffer.getvalue()


class _TypedCrawler(base._Crawler):
    """Краулер с ``Content-Type``: контейнер обязан быть виден зонду, а не только по URL."""

    def fetch(self, url: str, plan: Any = base.FetchPlan()) -> Any:  # noqa: B008 — сигнатура порта
        page = super().fetch(url, plan)
        content_type = "application/zip" if url.endswith(".zip") else "text/html"
        return base.RawPage(
            url=page.url,
            content=page.content,
            fetched_at=page.fetched_at,
            fetch_mode=page.fetch_mode,
            content_type=content_type,
            lang="en",
            status_code=200,
            engine=self.engine,
        )


def _build_crashing(
    tmp_path: Path,
    *,
    store: SqliteStore,
    pages: dict[str, bytes],
    with_container: bool = False,
) -> tuple[Any, list[base._FakePool]]:
    """Прогон фазы с пулом, который гарантированно исчерпает пересборки."""
    created: list[base._FakePool] = []

    def factory(n: int, level: Any = None, pdf_pipeline: Any = None) -> Any:
        # Каждый пул падает на первом ``result()``: пересборки исчерпываются за 2 волны.
        pool = base._FakePool(n, crash_on=1)
        created.append(pool)
        return pool

    from core.adapters.container_any import container_for

    result = base.build_corpus(
        base._plan(),
        _TypedCrawler(pages),
        base._Extractor(),
        base._Chunker(),
        store,
        urls={"p1": [ROOT]},
        raw_root=tmp_path,
        run_id=RUN_ID,
        crawl_defaults=CrawlRunConfig(max_depth=1, extract_workers=2),
        extraction_pool=base.ExtractionPool(
            pool_factory=factory, worker=base._reconstruct_worker
        ),
        container_for=container_for if with_container else None,
    )
    return result, created


def _rows(store: SqliteStore) -> dict[str, Any]:
    return {row.url: row for row in store.iter_extract_queue(SESSION_ID)}


def _session_items(store: SqliteStore) -> dict[str, Any]:
    return {item.url: item for item in store.iter_crawl_session_items(SESSION_ID)}


def _build_store() -> SqliteStore:
    store = SqliteStore(":memory:")
    base._make_run(store)
    return store


# --------------------------------------------------------------------------- #
# (1)–(3) Строки остаются pending; сеанс не врёт; цикл завершается
# --------------------------------------------------------------------------- #


def test_abandoned_pool_keeps_rows_pending(tmp_path: Path) -> None:
    """Abandon: строки ``pending``, сеанс без ``failed``, один ``errors``-вход на ключ."""
    store = _build_store()
    try:
        result, created = _build_crashing(tmp_path, store=store, pages=_pages())
        rows = _rows(store)
        items = _session_items(store)

        # Пересборки ограничены: ровно бюджет + первый пул. Inline docling не включён —
        # иначе остаток корня дал бы документы (и ещё один пул).
        assert len(created) == _EXTRACT_MAX_POOL_REBUILDS + 1
        assert base._build_of(result).doc_count == 0

        pending = [url for url, row in rows.items() if row.state is ExtractQueueState.PENDING]
        assert len(pending) == 4, rows
        # Ни один оставленный документ не назван отказом извлечения: его не извлекали.
        for url in pending:
            assert items[url].status is not CrawlItemStatus.FAILED
            assert items[url].error_kind is None
        # (2) Забранная строка вернулась в pending с причиной «исхода нет», а не «failed».
        released = [row for row in rows.values() if "возвращена в pending" in (row.error or "")]
        assert len(released) == 1, rows
        assert released[0].state is ExtractQueueState.PENDING
        assert released[0].error_kind is None
        # (3) Обращений к пулу ровно столько, сколько строк дошло до abandon'а: после
        # отказа ключ больше не обслуживается (остаток — 3 строки — не отправлялся).
        submitted = sum(len(pool.submitted_requests) for pool in created)
        assert submitted == 6, submitted  # 5 отказов + 1 возвращённая строка
        assert submitted < len(rows)

        # (6) Один вход на ключ с числом оставленных строк; обещания «терминально FAILED»
        # больше нет ни в одной строке.
        named = [error for error in result.errors if "abandon'ен" in error]
        assert len(named) == 1, result.errors
        assert f"оставлено pending {len(pending)}" in named[0]
        assert not any("терминально FAILED" in error for error in result.errors)
        # Корпус честно объявлен неполным, а не «собранным».
        assert result.status == "failed"
        assert any("корпус продукта не полон" in error for error in result.errors)
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (4) Член контейнера: «не пытались», а не «извлечение отказало»
# --------------------------------------------------------------------------- #


def test_member_outcome_on_abandoned_pool_is_not_a_failure() -> None:
    """Исход члена при abandon'е — ``not_attempted``, и он не равен ``extract_failed``."""
    pools = ExtractPools(max_rebuilds=_EXTRACT_MAX_POOL_REBUILDS)
    key = (DocumentLevel.FULL, PdfPipeline.STRUCTURED, 2)
    # Довести ключ до abandon'а ровно тем путём, каким это делает фаза.
    for _ in range(_EXTRACT_MAX_POOL_REBUILDS):
        assert pools.rebuild(key) is True
    assert pools.rebuild(key) is False
    assert pools.abandoned(key) is True

    outcome = member_pool_exhausted_outcome(key)
    assert outcome.skip_reason == SKIP_NOT_ATTEMPTED
    assert outcome.skip_reason != SKIP_EXTRACT_FAILED
    assert outcome.doc_id is None
    assert f"{_EXTRACT_MAX_POOL_REBUILDS}" in (outcome.detail or "")
    assert "inline" in (outcome.detail or "")


def test_abandoned_pool_leaves_container_row_pending(tmp_path: Path) -> None:
    """Контейнер abandon'енного ключа не спускается: строка ``pending``, членов нет.

    Спуск — работа очереди, а не отдельный источник строк: если извлекать членов нечем,
    контейнер не закрывается (``done``) и не объявляется провалившимся — он остаётся
    работой следующего исполнения, а его байты уже в ``corpus_raw`` (I19).
    """
    store = _build_store()
    try:
        result, created = _build_crashing(
            tmp_path, store=store, pages=_pages(with_container=True), with_container=True
        )
        rows = _rows(store)
        items = _session_items(store)

        assert rows[ZIP_URL].state is ExtractQueueState.PENDING
        assert items[ZIP_URL].status is not CrawlItemStatus.FAILED
        assert items[ZIP_URL].error_kind is None
        # Сценарий не вырожденный: abandon действительно случился, и байты архива
        # ретейнены — спуск восстановим без сети (I19).
        assert any("abandon'ен" in error for error in result.errors), result.errors
        assert items[ZIP_URL].raw_path
        # Спуска не было: ни одной строки члена и ни одного задания по адресу члена.
        assert list(store.iter_corpus_members()) == []
        submitted = [request.url for pool in created for request in pool.submitted_requests]
        assert not [url for url in submitted if "!/" in url], submitted
        # Контейнер не назван отказом нигде: ни в ``errors``, ни в элементах сеанса.
        assert not [
            error
            for error in result.errors
            if ZIP_URL in error and "extract" in error
        ], result.errors
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (5) Настоящий отказ экстрактора остаётся отказом (фикс не «лечит» провалы)
# --------------------------------------------------------------------------- #


def test_real_extractor_failure_is_still_parser_failed(tmp_path: Path) -> None:
    """Экстрактор упал на конкретных байтах — ``parser_failed`` и ``failed``, как было."""
    store = _build_store()
    try:
        created: list[base._FakePool] = []
        result = base.build_corpus(
            base._plan(),
            _TypedCrawler({ROOT: b"<html>hello</html>"}),
            base._Extractor(),
            base._Chunker(),
            store,
            urls={"p1": [ROOT]},
            raw_root=tmp_path,
            run_id=RUN_ID,
            crawl_defaults=CrawlRunConfig(extract_workers=2),
            extraction_pool=base.ExtractionPool(
                pool_factory=base._recording_factory(created),
                worker=base._raise_worker,
            ),
        )
        row = _rows(store)[ROOT]
        item = _session_items(store)[ROOT]
        assert row.state is ExtractQueueState.FAILED
        assert row.error_kind == "parser_failed"
        assert item.status is CrawlItemStatus.FAILED
        assert item.error_kind == "parser_failed"
        # Никакого «abandon'ен» здесь нет: пул жив, отказ — свойство документа.
        assert not any("abandon'ен" in error for error in result.errors)
        assert not any("not_attempted" in error for error in result.errors)
    finally:
        store.close()
