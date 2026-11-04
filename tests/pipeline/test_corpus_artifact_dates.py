"""Даты артефактов в БД прогона (план 20 §F2): отчёт читает их без ``state.db``.

Проверяется носитель, а не вердикты:

1. живой прогон пишет по строке на документ: ``observed_at``/``observed_how='live'``/
   ``capture_id``/``root_url`` заполнены, ``url_key`` — тот же, что в снимке корпуса;
2. документ без наблюдения (adoption, субстрат без ``observed_at``) — строка есть, все
   четыре датных поля ``NULL``: документ в корпусе — факт, дата — нет;
3. повторный проход не удваивает строк (набор продукта **заменяется**, а не копится);
4. пустой корпус продукта — ноль строк и никакого падения.

Субстрат наполняется реальным путём (``build_corpus``) тем же харнессом, что и снимок
корпуса (``test_corpus_snapshot``): здесь проверяется та же граница продукта.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import tests.pipeline.test_corpus_snapshot as snapshot_case
from core.pipeline.corpus import build_corpus
from core.pipeline.digests import snapshot_url_key
from core.store.sqlite_store import SqliteStore
from core.store.state_store import SqliteStateStore

RUN_ID = snapshot_case.RUN_ID
ROOT_A = snapshot_case.ROOT_A
ROOT_B = snapshot_case.ROOT_B
ROOT_C = snapshot_case.ROOT_C
TS_A = snapshot_case.TS_A
TS_B = snapshot_case.TS_B
TS_C = snapshot_case.TS_C


def _live_run(
    tmp_path: Path,
    state: Any,
    *,
    pages: dict[str, tuple[bytes, str]],
    roots: list[str],
) -> tuple[Any, SqliteStore]:
    return snapshot_case._live_run(tmp_path, state, pages=pages, roots=roots)


def test_live_run_writes_one_row_per_document(tmp_path: Path) -> None:
    """По строке на документ; дата, способ, снятие и корень — из наблюдения, не из прогона.

    ``corpus_as_of`` прогона — min/max тех же дат, поэтому строка обязана нести **свою**
    дату: при одной дате на прогон три документа разошлись бы с ``corpus_as_of``.
    """
    pages = {
        ROOT_A: (b"text a", TS_A),
        ROOT_B: (b"text b", TS_B),
        ROOT_C: (b"text c", TS_C),
    }
    with SqliteStateStore(":memory:") as state:
        result, store = _live_run(
            tmp_path, state, pages=pages, roots=[ROOT_A, ROOT_B, ROOT_C]
        )
        try:
            assert result.status == "ok"
            rows = list(store.iter_corpus_artifacts(RUN_ID, "p1"))
            assert len(rows) == 3
            by_key = {row.url_key: row for row in rows}
            # Ключ адреса — тот же, по которому артефакт входит в снимок корпуса.
            assert set(by_key) == {snapshot_url_key(url) for url in pages}

            # Снятия прогона: корень → capture_id. Строка обязана ссылаться на своё снятие.
            raw = store.read_corpus_snapshot(RUN_ID)
            assert raw is not None
            capture_of = {
                capture["root_url"]: capture["capture_id"]
                for capture in raw["captures"]
            }
            for url, (_content, ts) in pages.items():
                row = by_key[snapshot_url_key(url)]
                assert row.observed_at == ts
                assert row.observed_how == "live"
                assert row.capture_id == capture_of[url]
                assert row.root_url == url
                # Версия документа записана живым путём (адрес версии, не ``''``).
                assert row.doc_version_id
        finally:
            store.close()


def test_document_without_observation_keeps_a_row_without_dates(
    tmp_path: Path,
) -> None:
    """Нет наблюдения — все четыре датных поля ``NULL``, но строка документа есть.

    Сценарий — ``url_state`` без ``observed_at`` (запись до появления колонок наблюдения
    либо артефакт, пришедший без сети). Дату подставлять нечем: ``created_at`` прогона —
    не дата наблюдения (A3), а ``root_url`` без снятия был бы выдуман.
    """
    pages = {ROOT_A: (b"text a", TS_A), ROOT_B: (b"text b", TS_B)}
    with SqliteStateStore(":memory:") as inner:
        state = snapshot_case._NoObservationState(inner)
        result, store = _live_run(tmp_path, state, pages=pages, roots=[ROOT_A, ROOT_B])
        try:
            assert result.status == "ok"
            rows = list(store.iter_corpus_artifacts(RUN_ID, "p1"))
            assert len(rows) == 2
            assert {row.url_key for row in rows} == {
                snapshot_url_key(url) for url in pages
            }
            for row in rows:
                assert row.observed_at is None
                assert row.observed_how is None
                assert row.capture_id is None
                assert row.root_url is None
                # Версия — факт корпуса, а не наблюдение: она остаётся записанной.
                assert row.doc_version_id
        finally:
            store.close()


def test_repeated_pass_does_not_double_rows(tmp_path: Path) -> None:
    """Повторный проход того же прогона (resume/``--reset``) — тот же набор строк.

    Строка артефакта — проекция набора, а не журнал: копиться ей нельзя, иначе отчёт
    насчитал бы два наблюдения на один документ и назвал бы дату дважды.
    """
    pages = {ROOT_A: (b"text a", TS_A), ROOT_B: (b"text b", TS_B)}
    with SqliteStateStore(":memory:") as state:
        store = SqliteStore(":memory:")
        try:
            snapshot_case._make_run(store, RUN_ID)
            for _ in range(2):
                result = build_corpus(
                    snapshot_case._plan(RUN_ID),
                    snapshot_case._Crawler(pages),
                    snapshot_case._Extractor(),
                    snapshot_case._Chunker(),
                    store,
                    urls={"p1": [ROOT_A, ROOT_B]},
                    raw_root=tmp_path / "raw",
                    run_id=RUN_ID,
                    state=state,
                    product_modes={"p1": "incremental"},
                    refresh_policy="auto",
                    crawl_defaults=snapshot_case.CrawlRunConfig(
                        max_depth=0, host_concurrency=1
                    ),
                )
                assert result.status == "ok"

            rows = list(store.iter_corpus_artifacts(RUN_ID, "p1"))
            assert len(rows) == 2
            assert [row.url_key for row in rows] == sorted(
                snapshot_url_key(url) for url in pages
            )
            assert all(row.observed_at is not None for row in rows)
        finally:
            store.close()


def test_empty_corpus_writes_no_rows_and_does_not_crash(tmp_path: Path) -> None:
    """Пустой корпус продукта — ноль строк артефактов, фаза не падает.

    «Документов нет» — законный исход (продукт без источников/сеть запрещена политикой):
    проекция пуста, и это читается отчётом как «дат нет», а не как сбой записи.
    """
    with SqliteStateStore(":memory:") as state:
        store = SqliteStore(":memory:")
        try:
            snapshot_case._make_run(store, RUN_ID)
            result = build_corpus(
                snapshot_case._plan(RUN_ID),
                snapshot_case._Crawler({}),
                snapshot_case._Extractor(),
                snapshot_case._Chunker(),
                store,
                urls={"p1": []},
                raw_root=tmp_path / "raw",
                run_id=RUN_ID,
                state=state,
                product_modes={"p1": "incremental"},
                refresh_policy="auto",
                crawl_defaults=snapshot_case.CrawlRunConfig(max_depth=0),
            )
            assert result.status in {"ok", "skipped"}
            assert list(store.iter_corpus_artifacts(RUN_ID, "p1")) == []
            assert list(store.iter_corpus_artifacts(RUN_ID)) == []
        finally:
            store.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))