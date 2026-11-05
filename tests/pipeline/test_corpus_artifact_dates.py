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
from core.domain import CorpusCaptureRef
from core.pipeline.corpus import _corpus_artifact_rows, build_corpus
from core.pipeline.corpus_freshness import build_corpus_freshness
from core.pipeline.digests import snapshot_url_key
from core.ports.state import UrlStateRow
from core.ports.store import CorpusArtifactRow
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


# --- Член контейнера наследует наблюдение контейнера (план 20 §F2) ---------- #

#: Корень продукта и внешний контейнер в нём: у контейнера наблюдение есть.
ROOT = "https://docs.test/p1/root"
CONTAINER = "https://docs.test/p1/bundle.zip"
#: Вложенный контейнер: его собственное наблюдение — **другое** (другая дата и снятие),
#: поэтому тест различает «унаследовал от внешнего» и «унаследовал от ближнего».
NESTED_CONTAINER = f"{CONTAINER}!/inner.zip"
MEMBER = f"{CONTAINER}!/docs/manual.pdf"
NESTED_MEMBER = f"{NESTED_CONTAINER}!/docs/deep.pdf"
ORPHAN_MEMBER = "https://docs.test/p1/absent.zip!/docs/x.pdf"


def _capture(capture_id: str, root_url: str, started_at: str) -> CorpusCaptureRef:
    return CorpusCaptureRef(
        capture_id=capture_id,
        product_id="p1",
        root_url=root_url,
        started_at=started_at,
        complete=True,
        observed_how="live",
    )


def _observed(url: str, observed_at: str, capture_id: str) -> UrlStateRow:
    return UrlStateRow(
        product_id="p1",
        url=url,
        normalized_url=url,
        observed_at=observed_at,
        observed_how="live",
        capture_id=capture_id,
    )


def _without_observation(rows: list[CorpusArtifactRow]) -> int:
    """Счётчик отчёта: сколько артефактов без даты (то же число, что в ``corpus_freshness``)."""
    freshness = build_corpus_freshness(run_id=RUN_ID, artifacts=rows)
    return int(
        freshness.document["products"]["p1"]["artifacts_without_observation"]
    )


def test_container_member_inherits_the_container_observation() -> None:
    """Член контейнера несёт **ту же** дату, что контейнер: байты пришли одним ответом.

    Иначе провенанс соврал бы: член — не отдельно наблюдённый URL, но и не
    «ненаблюдённый» артефакт. ``root_url`` наследуется вместе со ``capture_id`` — через
    снятие контейнера, а не подстановкой.

    Вложенный член (два ``!/``) наследует от **внешнего** контейнера даже когда у
    вложенного есть собственное наблюдение с другой датой: префикс до первого ``!/``.
    """
    artifacts = [
        (ROOT, "dv_root"),
        (CONTAINER, "dv_container"),
        (NESTED_CONTAINER, "dv_inner"),
        (MEMBER, "dv_member"),
        (NESTED_MEMBER, "dv_deep"),
    ]
    observations = [
        _observed(ROOT, TS_A, "cap_root"),
        _observed(CONTAINER, TS_B, "cap_container"),
        _observed(NESTED_CONTAINER, TS_C, "cap_inner"),
    ]
    captures = [
        _capture("cap_root", ROOT, TS_A),
        _capture("cap_container", CONTAINER, TS_B),
        _capture("cap_inner", NESTED_CONTAINER, TS_C),
    ]

    rows = _corpus_artifact_rows(
        RUN_ID, "p1", artifacts, observations, captures
    )
    by_key = {row.url_key: row for row in rows}
    # Ключи адресов различны: член — отдельная строка, а не перезапись контейнера.
    assert len(rows) == 5
    container = by_key[snapshot_url_key(CONTAINER)]

    def dates(row: CorpusArtifactRow) -> tuple[Any, Any, Any, Any]:
        return (row.observed_at, row.observed_how, row.capture_id, row.root_url)

    member = by_key[snapshot_url_key(MEMBER)]
    assert member.url_key != container.url_key
    assert member.doc_version_id == "dv_member"
    assert dates(member) == dates(container)
    assert dates(by_key[snapshot_url_key(MEMBER)]) == (TS_B, "live", "cap_container", CONTAINER)
    # Вложенный член — от внешнего контейнера, а не от ``inner.zip`` (у него TS_C).
    assert dates(by_key[snapshot_url_key(NESTED_MEMBER)]) == dates(container)
    # Сам вложенный контейнер отвечает своим наблюдением — наследование не переписывает его.
    assert dates(by_key[snapshot_url_key(NESTED_CONTAINER)]) == (
        TS_C,
        "live",
        "cap_inner",
        NESTED_CONTAINER,
    )
    # Все артефакты набора датированы: счётчик «без наблюдения» — ноль.
    assert _without_observation(rows) == 0


def test_member_without_a_container_observation_stays_undated() -> None:
    """Нет наблюдения контейнера — член остаётся ``NULL``, и это видно счётчиком.

    Наследование не выдумывает дату: отсутствие наблюдения контейнера означает отсутствие
    наблюдения байтов, а не «дата не важна».
    """
    artifacts = [(CONTAINER, "dv_container"), (ORPHAN_MEMBER, "dv_orphan")]
    observations = [_observed(CONTAINER, TS_B, "cap_container")]
    captures = [_capture("cap_container", CONTAINER, TS_B)]

    rows = _corpus_artifact_rows(
        RUN_ID, "p1", artifacts, observations, captures
    )
    by_key = {row.url_key: row for row in rows}
    orphan = by_key[snapshot_url_key(ORPHAN_MEMBER)]
    assert (orphan.observed_at, orphan.observed_how) == (None, None)
    assert (orphan.capture_id, orphan.root_url) == (None, None)
    # Строка документа в корпусе есть — отсутствует именно наблюдение.
    assert orphan.doc_version_id == "dv_orphan"
    assert _without_observation(rows) == 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))