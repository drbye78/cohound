"""Снимок корпуса прогона (план 20 §4/§5, F1): провенанс, а не объявленный вход.

Проверяется то, ради чего снимок заведён:

1. даты наблюдений артефактов **разные** и ``corpus_as_of`` — их минимум/максимум, а не
   дата прогона; ``captures[]`` и ``corpus_snapshot_id`` непусты и видны в
   ``run_manifest.yaml``;
2. наблюдений нет — ``corpus_as_of is None``: ``created_at`` прогона не подставляется;
3. ``pb`` продукта **не** зависит от снимка: тот же корпус при других датах даёт тот же
   ``partition_build_id`` (снимок не вошёл в формулу ``_derive_partition_build_id``).

Субстрат наполняется реальным путём (``build_corpus``), а не собран руками.
"""

from __future__ import annotations

import datetime as _dt
import pathlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlRunConfig,
    RunConfig,
    RunMode,
)
from core.domain.models import CorpusSnapshot
from core.pipeline import RunPlan
from core.pipeline.corpus import IndexBuild, build_corpus
from core.pipeline.corpus_index import _PARTITION_SCHEMA
from core.pipeline.report import build_manifest, write_run_manifest
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

RUN_ID = "run-snapshot"
FROZEN_RUN_ID = "run-snapshot-frozen"

ROOT_A = "https://docs.test/p1/a"
ROOT_B = "https://docs.test/p1/b"
ROOT_C = "https://docs.test/p1/c"

#: Три **разные** даты наблюдения — это и есть предмет снимка (план 20 §1).
TS_A = "2026-01-01T00:00:00+00:00"
TS_B = "2026-02-01T00:00:00+00:00"
TS_C = "2026-03-01T00:00:00+00:00"


class _Crawler:
    """Живой краулер: отдаёт заданные байты и **свою** дату ответа для каждого URL."""

    version = "crawler-1"
    engine: str | None = None

    def __init__(self, pages: dict[str, tuple[bytes, str]]) -> None:
        self.pages = dict(pages)
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        self.calls.append(url)
        content, fetched_at = self.pages[url]
        return RawPage(
            url=url,
            content=content,
            fetched_at=fetched_at,
            fetch_mode=plan.fetch,
            lang="en",
            status_code=200,
        )


class _Extractor:
    """Текст = байты страницы (``content`` либо retained-файл); хеш — их хеш."""

    version = "extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        data = _page_bytes(page)
        return ExtractedDoc(
            text=data.decode("utf-8", errors="replace"),
            content_hash=content_hash(data),
            extractor_version=self.version,
            page_count=1,
        )


def _page_bytes(page: Any) -> bytes:
    """Байты страницы: ``content`` либо retained-файл (``frozen`` читает файл)."""
    if page.content:
        return page.content
    path = getattr(page, "source_path", None)
    return pathlib.Path(path).read_bytes() if path else b""


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


class _NoNetworkCrawler:
    """``frozen`` не ходит в сеть: любой вызов — доказательство сети."""

    version = "crawler-1"
    engine: str | None = None

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008
        raise AssertionError(f"frozen: сетевой вызов по {url!r}")


class _NoObservationState:
    """Читатель состояния без дат наблюдения (legacy-строки ``url_state``).

    Записи делегируются (фаза пишет как обычно), но чтение ``url_state`` отдаёт строки с
    ``observed_at=None`` — ровно то, что видит прогон над состоянием, записанным до
    появления колонок наблюдения. Снимок обязан сказать «даты нет», а не подставить
    ``created_at`` прогона.
    """

    def __init__(self, inner: SqliteStateStore) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def iter_url_state(self, product_id: str) -> list[Any]:
        return [
            replace(row, observed_at=None)
            for row in self._inner.iter_url_state(product_id)
        ]


def _plan(run_id: str = RUN_ID) -> RunPlan:
    return RunPlan(
        run_id=run_id,
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


def _make_run(store: SqliteStore, run_id: str = RUN_ID) -> None:
    store.create_run(
        run_id,
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


def _live_run(
    tmp_path: Path,
    state: Any,
    *,
    pages: dict[str, tuple[bytes, str]],
    roots: list[str],
    run_id: str = RUN_ID,
) -> tuple[Any, SqliteStore]:
    store = SqliteStore(":memory:")
    _make_run(store, run_id)
    result = build_corpus(
        _plan(run_id),
        _Crawler(pages),
        _Extractor(),
        _Chunker(),
        store,
        urls={"p1": list(roots)},
        raw_root=tmp_path / "raw",
        run_id=run_id,
        state=state,
        product_modes={"p1": "incremental"},
        refresh_policy="auto",
        crawl_defaults=CrawlRunConfig(max_depth=0, host_concurrency=1),
    )
    return result, store


def _frozen_run(
    tmp_path: Path,
    state: Any,
    *,
    roots: list[str],
) -> tuple[Any, SqliteStore]:
    """Проход без сети другим ``run_id``: корпус из сохранённых байтов, без наблюдений."""
    store = SqliteStore(":memory:")
    _make_run(store, FROZEN_RUN_ID)
    result = build_corpus(
        _plan(FROZEN_RUN_ID),
        _NoNetworkCrawler(),
        _Extractor(),
        _Chunker(),
        store,
        urls={"p1": list(roots)},
        raw_root=tmp_path / "raw",
        run_id=FROZEN_RUN_ID,
        state=state,
        product_modes={"p1": "incremental"},
        refresh_policy="frozen",
        crawl_defaults=CrawlRunConfig(max_depth=0),
    )
    return result, store


def _build_of(result: Any) -> IndexBuild:
    builds = [entry for entry in result.log_entries if isinstance(entry, IndexBuild)]
    assert len(builds) == 1
    return builds[0]


def test_snapshot_carries_distinct_observation_dates(tmp_path: Path) -> None:
    """Три артефакта с разными ``observed_at`` → ``corpus_as_of`` = min/max.

    Даты берутся из наблюдений артефактов, а не из ``created_at`` прогона: если бы модель
    была «одна дата у всех», ``from`` и ``to`` совпали бы, и это было бы видно здесь.
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
            raw = store.read_corpus_snapshot(RUN_ID)
            assert raw is not None, "снимок обязан быть записан фазой corpus"
            snapshot = CorpusSnapshot.model_validate(raw)

            assert snapshot.corpus_as_of == {"from": TS_A, "to": TS_C}
            assert len(snapshot.captures) == 3
            # Сортировка — по содержанию, а не по порядку завершения корней.
            assert [capture.root_url for capture in snapshot.captures] == [
                ROOT_A,
                ROOT_B,
                ROOT_C,
            ]
            assert all(capture.observed_how == "live" for capture in snapshot.captures)
            assert all(capture.complete for capture in snapshot.captures)
            assert all(capture.doc_set_digest for capture in snapshot.captures)
            assert snapshot.corpus_snapshot_id

            # Пост-хок ``hound report`` читает снимок из строки манифеста и не теряет его.
            reread = store.read_manifest(RUN_ID)
            assert reread.corpus_snapshot is not None
            assert reread.corpus_snapshot.corpus_as_of == snapshot.corpus_as_of
            from core.pipeline.report import manifest_payload

            assert manifest_payload(reread)["corpus_snapshot"][
                "corpus_snapshot_id"
            ] == snapshot.corpus_snapshot_id

            # Манифест получает тот же снимок (пересчёта содержания нет) и печатает его.
            cfg = RunConfig(
                case_id="case",
                anchor="p1",
                cohort="case",
                products=["p1"],
                package="pkg",
                snapshot_date=_dt.date(2026, 1, 1),
                mode=RunMode.STRICT,
            )
            manifest = build_manifest(
                inputs=SimpleNamespace(run=cfg, package=None),
                plan=SimpleNamespace(
                    run_id=RUN_ID, anchor="p1", modules=[], cohort_ref="case"
                ),
                created_at="2026-01-01T00:00:00Z",
                corpus_snapshot=raw,
            )  # type: ignore[arg-type]
            assert manifest.corpus_snapshot is not None
            assert (
                manifest.corpus_snapshot.corpus_as_of
                == snapshot.corpus_as_of
            )
            path = write_run_manifest(manifest, tmp_path / "out")
            exported = yaml.safe_load(path.read_text(encoding="utf-8"))
            assert exported["corpus_snapshot"]["corpus_as_of"] == {
                "from": TS_A,
                "to": TS_C,
            }
            assert len(exported["corpus_snapshot"]["captures"]) == 3
            assert exported["corpus_snapshot"]["corpus_snapshot_id"] == (
                snapshot.corpus_snapshot_id
            )
        finally:
            store.close()


def test_snapshot_does_not_invent_a_date_without_observations(
    tmp_path: Path,
) -> None:
    """Наблюдений нет → ``corpus_as_of is None``; ``created_at`` не подставляется.

    Сценарий — состояние с ``url_state`` **без** дат наблюдения (legacy-строки, записанные
    до колонок ``observed_at``). Корпус собран, снимок продукта есть, снятия прогона есть,
    а даты артефактов не выдумываются: ``created_at`` прогона — не дата наблюдения.
    """
    pages = {ROOT_A: (b"text a", TS_A), ROOT_B: (b"text b", TS_B)}
    with SqliteStateStore(":memory:") as inner:
        state = _NoObservationState(inner)
        result, store = _live_run(
            tmp_path, state, pages=pages, roots=[ROOT_A, ROOT_B]
        )
        try:
            assert result.status == "ok"
            raw = store.read_corpus_snapshot(RUN_ID)
            assert raw is not None
            snapshot = CorpusSnapshot.model_validate(raw)
            assert snapshot.corpus_as_of is None
            # Снимок существует и снятия на нём есть — отсутствует именно дата.
            assert snapshot.corpus_snapshot_id
            assert len(snapshot.captures) == 2
        finally:
            store.close()


def test_partition_build_id_does_not_depend_on_the_snapshot(tmp_path: Path) -> None:
    """Пин: снимок не входит в формулу ``pb`` (тот же корпус — тот же ``partition_build_id``).

    Два прогона одного содержимого с **разными** датами наблюдений дают разные снимки
    продуктов, но обязаны дать один ``partition_build_id``: иначе добавление снимка
    сменило бы сборку у всех партиций (``execution_change`` без причины).
    """
    pages_a = {ROOT_A: (b"same text", TS_A)}
    pages_b = {ROOT_A: (b"same text", TS_B)}
    with SqliteStateStore(":memory:") as state_a:
        result_a, store_a = _live_run(
            tmp_path / "a", state_a, pages=pages_a, roots=[ROOT_A]
        )
        snapshot_a = store_a.read_corpus_snapshot(RUN_ID)
        pb_a = _build_of(result_a).entries[0].partition_build_id
        store_a.close()
    with SqliteStateStore(":memory:") as state_b:
        result_b, store_b = _live_run(
            tmp_path / "b", state_b, pages=pages_b, roots=[ROOT_A]
        )
        snapshot_b = store_b.read_corpus_snapshot(RUN_ID)
        pb_b = _build_of(result_b).entries[0].partition_build_id
        store_b.close()

    assert snapshot_a is not None and snapshot_b is not None
    assert snapshot_a["corpus_as_of"] != snapshot_b["corpus_as_of"], (
        "даты разные — иначе тест не доказывает независимость pb от снимка"
    )
    assert snapshot_a["corpus_snapshot_id"] == snapshot_b["corpus_snapshot_id"], (
        "снимок продукта считается от содержимого и политики, а не от дат"
    )
    assert pb_a == pb_b
    assert pb_a.startswith("pb_")
    # Версия формулы не менялась снимком: он провенанс, а не слагаемое. ``partition-3``
    # (ось ``embed.precision``, 06.11.2026) — смена **другого** слагаемого; пин остаётся,
    # потому что он ловит случайный бамп схемы, а не потому, что снимок на неё влияет.
    assert _PARTITION_SCHEMA == "partition-3"


def test_split_wrapper_reads_snapshot_without_recomputing() -> None:
    """``read_corpus_snapshot`` читает ту же запись, что написала фаза (round-trip).

    Носитель — ``runs.corpus_snapshot_json``: манифест (и ``hound report``) обязан читать
    снимок отсюда, а не пересчитывать содержание. Снимок корпуса — провенанс, поэтому он
    не входит ни в ``EXECUTION_FIELDS``, ни в ``METHODOLOGY_FIELDS``.
    """
    from core.pipeline.report import EXECUTION_FIELDS, METHODOLOGY_FIELDS

    assert "corpus_snapshot" not in EXECUTION_FIELDS
    assert "corpus_snapshot" not in METHODOLOGY_FIELDS

    with SqliteStore(":memory:") as store:
        _make_run(store)
        assert store.read_corpus_snapshot(RUN_ID) is None
        payload = {
            "corpus_snapshot_id": "snap_1",
            "captures": [],
            "corpus_as_of": {"from": TS_A, "to": TS_C},
        }
        store.set_run_corpus_snapshot(RUN_ID, payload)
        assert store.read_corpus_snapshot(RUN_ID) == payload
        # Идемпотентность: повторная запись того же — тот же факт.
        store.set_run_corpus_snapshot(RUN_ID, payload)
        assert store.read_corpus_snapshot(RUN_ID) == payload

def test_frozen_snapshot_inherits_the_captures_it_relies_on(tmp_path: Path) -> None:
    """Дефект стенда 05.11.2026: у ``frozen`` снимок был **без единого снятия**.

    Снятия — не «что этот прогон снял», а «чем удостоверен вошедший артефакт». Проход без
    сети не наблюдает, поэтому снятий не создаёт; но его документы опираются на
    ``url_state.capture_id`` живого прогона. Снимок обязан назвать **те** снятия, с их
    настоящими датами и ``observed_how='live'``, иначе манифест читается как «корпус ничем
    не удостоверен».

    Идентичность снимка при этом не меняется: источник снятий (этот прогон или прошлый) —
    провенанс, а ``corpus_snapshot_id`` считается от содержания и политики.
    """
    pages = {ROOT_A: (b"text a", TS_A), ROOT_B: (b"text b", TS_B)}
    with SqliteStateStore(":memory:") as state:
        live_result, live_store = _live_run(
            tmp_path, state, pages=pages, roots=[ROOT_A, ROOT_B]
        )
        live_raw = live_store.read_corpus_snapshot(RUN_ID)
        live_store.close()
        assert live_result.status == "ok"
        assert live_raw is not None

        frozen_result, frozen_store = _frozen_run(
            tmp_path, state, roots=[ROOT_A, ROOT_B]
        )
        try:
            assert frozen_result.status == "ok"
            frozen_raw = frozen_store.read_corpus_snapshot(FROZEN_RUN_ID)
            assert frozen_raw is not None
            live = CorpusSnapshot.model_validate(live_raw)
            frozen = CorpusSnapshot.model_validate(frozen_raw)

            assert [c.capture_id for c in frozen.captures] == [
                c.capture_id for c in live.captures
            ]
            assert [
                (c.root_url, c.started_at, c.finished_at, c.complete, c.observed_how)
                for c in frozen.captures
            ] == [
                (c.root_url, c.started_at, c.finished_at, c.complete, c.observed_how)
                for c in live.captures
            ]
            assert all(c.observed_how == "live" for c in frozen.captures)
            # Снятия пришли из прошлого прогона, но опираются на этот же корпус.
            assert frozen.corpus_as_of == live.corpus_as_of
            assert frozen.corpus_snapshot_id == live.corpus_snapshot_id
        finally:
            frozen_store.close()


def test_percent_encoding_of_the_address_does_not_change_the_snapshot_id() -> None:
    """Дефект стенда 05.11.2026: форма записи адреса меняла снимок продукта.

    Измерено: живой путь записал ``…/%D0%9B%D0%B8%D1%86…1.pdf``, проход по сохранённым
    байтам — ``…/ЛицензионноеСоглашение1.pdf``; ``doc_id``/``content_hash``/
    ``doc_version_id`` при этом совпадают. Это один артефакт, значит и снимок один.
    """
    from core.pipeline.digests import (
        corpus_snapshot_product_id,
        snapshot_url_key,
    )

    encoded = "https://jatoba.ru/%D0%9B%D0%B8%D1%86%D0%B5%D0%BD%D0%B7%D0%B8%D1%8F1.pdf"
    decoded = "https://jatoba.ru/Лицензия1.pdf"
    assert encoded != decoded
    # Ключ один и он идемпотентен: применять к уже канонизированному адресу безопасно.
    assert snapshot_url_key(encoded) == snapshot_url_key(decoded)
    assert snapshot_url_key(snapshot_url_key(decoded)) == snapshot_url_key(decoded)

    first = corpus_snapshot_product_id(
        product_id="p1", artifacts=[(encoded, "dv_1")], crawl_policy_digest="d1"
    )
    second = corpus_snapshot_product_id(
        product_id="p1", artifacts=[(decoded, "dv_1")], crawl_policy_digest="d1"
    )
    assert first == second
    # Содержание и версия по-прежнему входят: другая версия — другой снимок.
    assert (
        corpus_snapshot_product_id(
            product_id="p1", artifacts=[(decoded, "dv_2")], crawl_policy_digest="d1"
        )
        != first
    )
