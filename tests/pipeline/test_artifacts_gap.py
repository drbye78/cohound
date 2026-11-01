"""Тесты ``artifacts_gap.yaml`` (13-manual-artifacts §4): отчёт о недоступном.

Покрывают:

* в ``gaps`` попадают **только** терминальные ``failed`` блокирующих классов
  (:data:`~core.domain.enums.BLOCKING_FAILURE_KINDS`);
* ``not_found``/``source_unavailable``/``redirect_out_of_scope`` в ``gaps`` не идут, но
  видны в ``summary.excluded_by_kind`` (короткий список ≠ «всё скачалось»);
* детерминированный порядок ``(product_id, error_kind, url)`` при перемешанном входе;
* пустой список — валидный результат: файл всё равно создаётся;
* фазы ``corpus`` и ``report`` дают **байт-идентичный** файл для одного состояния БД
  (один генератор, два вызова);
* заготовка ``provide`` содержит ``path: null``/``provided_by: null`` — её заполняет
  человек, машина не выдумывает путь.

Офлайн: SQLite в памяти, краулер-заглушка; данные нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
import pathlib
import random
from pathlib import Path
from typing import Any

import pytest
import yaml

from core.domain import (
    AnchorRelation,
    CatalogRow,
    Cohort,
    CohortEntry,
    CrawlItemStatus,
    CrawlSessionItem,
    FailureKind,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.domain.models import RunManifest
from core.domain.scoring import Comparability, Coverage, Score, SensitivityReport
from core.pipeline import RunPlan
from core.pipeline.artifacts_gap import (
    ARTIFACTS_GAP_FILENAME,
    ARTIFACTS_GAP_SCHEMA,
    artifacts_gap_path,
    collect_artifact_gaps,
    write_artifacts_gap,
)
from core.pipeline.compute import ScoreBundle
from core.pipeline.corpus import build_corpus
from core.pipeline.crawl_session import default_session_id, open_session
from core.pipeline.report import write_reports
from core.ports.types import Chunk, ExtractedDoc, FetchPlan, RawPage
from core.store.sqlite_store import SqliteStore
from core.util.hashing import content_hash

RUN_ID = "run-gap"
SESSION_ID = default_session_id(RUN_ID)
TS = "2026-10-25T00:20:11Z"
PRODUCT_A = "p1"
PRODUCT_B = "p2"
URL_A = "https://docs.test/files/a.pdf"
URL_B = "https://docs.test/files/b.pdf"
URL_C = "https://other.test/files/c.pdf"
FIXTURES = Path(__file__).parent / "fixtures"

# ---------------------------------------------------------------------------
# Фикстуры: стор, элементы сеанса, план/порты для сквозного прогона
# ---------------------------------------------------------------------------


def _store() -> SqliteStore:
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor=PRODUCT_A,
            cohort="case",
            products=[PRODUCT_A, PRODUCT_B],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 10, 25),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    return store


def _item(
    url: str,
    error_kind: FailureKind | None,
    *,
    product_id: str = PRODUCT_A,
    status: CrawlItemStatus = CrawlItemStatus.FAILED,
    attempts: int = 1,
    http_status: int | None = None,
    last_attempt: str | None = TS,
) -> CrawlSessionItem:
    return CrawlSessionItem(
        session_id=SESSION_ID,
        product_id=product_id,
        url=url,
        status=status,
        attempts=attempts,
        http_status=http_status,
        error_kind=error_kind.value if error_kind is not None else None,
        last_attempt=last_attempt,
    )


def _write_items(store: SqliteStore, items: list[CrawlSessionItem]) -> None:
    open_session(store, session_id=SESSION_ID, run_id=RUN_ID)
    for item in items:
        store.upsert_crawl_session_item(item)


def _catalog() -> list[CatalogRow]:
    raw = json.loads((FIXTURES / "neutral_inputs.json").read_text(encoding="utf-8"))
    return [CatalogRow.model_validate(row) for row in raw["catalog"]]


def _plan() -> RunPlan:
    return RunPlan(
        run_id=RUN_ID,
        products=[PRODUCT_A],
        anchor=PRODUCT_A,
        modules=["mod_a"],
        roles=["role_a"],
        params=_catalog(),
        module_weights={"mod_a": 100.0},
        renormalized_weights={"mod_a": 1.0},
        cohort=Cohort(
            case_id="case",
            frozen_at=_dt.date(2026, 10, 25),
            entries=[
                CohortEntry(
                    product_id=PRODUCT_A,
                    role_overlap=1.0,
                    selection_reason="anchor",
                    anchor_relation=AnchorRelation.ANCHOR,
                )
            ],
        ),
        golden=[],
    )


class _RefusedError(RuntimeError):
    """Robots-отказ: ``refusal_reason`` вместо ``failure_kind`` (D1/§10)."""

    refusal_reason = "robots_disallow"


class _RefusedCrawler:
    """Краулер, на каждом URL поднимающий robots-отказ (блокирующий класс)."""

    version = "crawler-1"
    engine = "http"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:  # noqa: B008 - сигнатура порта, FetchPlan frozen
        self.calls.append(url)
        raise _RefusedError(f"robots.txt запрещает краул {url!r}")


class _Extractor:
    """Экстрактор-заглушка: при отказе фетча не вызывается."""

    version = "extractor-1"

    def extract(self, page: RawPage, route: Any = None) -> ExtractedDoc:  # noqa: ANN401
        text = _page_bytes(page).decode("utf-8", errors="replace")
        return ExtractedDoc(
            text=text,
            content_hash=content_hash(_page_bytes(page)),
            extractor_version=self.version,
            page_count=1,
        )


class _Chunker:
    """Чанкер-заглушка: один чанк на документ."""

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


def _manifest() -> RunManifest:
    """Минимальный валидный манифест (нейтральный, как в тестах Ш9)."""
    return RunManifest(
        run_id=RUN_ID,
        case_id="case",
        created_at="2026-10-25T00:00:00+00:00",
        mode=RunMode.STRICT,
        methodology_version="2.0.0",
        package_version="1.0.0",
        catalog_version="1.0.0",
        status_dict_version="1.0.0",
        scoring_policy_version="1.0.0",
        anchor_product_id=PRODUCT_A,
        cohort_ref="cohort-1",
        focus_modules=[],
        index_build_id="build.1",
        embed_model_id="embed.1",
        chunker_version="chunker.1",
        extractor_version="extractor.1",
        lexical_engine_version="lexical.1",
        vector_store="npy",
        retrieval_profile="baseline",
        retrieval_cutoff_K=10,
        retrieval_params_json='{"rrf_k": 60}',
        search_effort_budget_json='{"serp_rounds": 3}',
        requested_ranking=RankingPolicy.CAPABILITY_RANKED,
        computed_ranking=RankingPolicy.CAPABILITY_RANKED,
        snapshot_date="2026-10-25",
    )


def _undefined_bundle() -> ScoreBundle:
    """Бандл без Coverage (``defined=False``): battle card не эмитится (I11/G4)."""
    return ScoreBundle(
        capability=Score(value=0.0, numerator=0.0, denominator=0.0, n_used=0),
        coverage=Coverage(
            value=None,
            numerator=0.0,
            denominator=0.0,
            n_used=0,
            universe=0.0,
            completed=0.0,
            defined=False,
        ),
        failure_rate=0.0,
        modules=[],
        comparability=Comparability(
            level="insufficient", min_coverage=0.0, cross_cycle=True
        ),
        sensitivity=SensitivityReport(
            stable=True,
            per_axis={},
            unstable_pairs=[],
            requested_ranking="capability_ranked",
            computed_ranking="capability_ranked",
        ),
        requested_ranking="capability_ranked",
        computed_ranking="capability_ranked",
    )


def _load(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


# ---------------------------------------------------------------------------
# (а) только блокирующие классы
# ---------------------------------------------------------------------------


def test_only_blocking_failure_kinds_become_gaps() -> None:
    """В ``gaps`` — только ``failed`` с ``error_kind`` из ``BLOCKING_FAILURE_KINDS``."""
    items = [
        _item(URL_A, FailureKind.ROBOTS_DISALLOW, http_status=None),
        _item(URL_B, FailureKind.PARSER_FAILED),
        _item(URL_C, FailureKind.ATTEMPTS_EXHAUSTED, product_id=PRODUCT_B),
        # Не терминальный отказ: очередь, а не пробел.
        _item(
            "https://docs.test/files/pending.pdf",
            None,
            status=CrawlItemStatus.PENDING,
            attempts=0,
            last_attempt=None,
        ),
    ]
    report = collect_artifact_gaps(items, run_id=RUN_ID)

    assert [gap.error_kind for gap in report.gaps] == [
        FailureKind.PARSER_FAILED,
        FailureKind.ROBOTS_DISALLOW,
        FailureKind.ATTEMPTS_EXHAUSTED,
    ]
    assert {gap.url for gap in report.gaps} == {URL_A, URL_B, URL_C}
    assert report.summary()["total"] == 3
    assert report.summary()["by_error_kind"] == {
        "attempts_exhausted": 1,
        "parser_failed": 1,
        "robots_disallow": 1,
    }
    assert report.summary()["by_product"] == {PRODUCT_A: 2, PRODUCT_B: 1}
    assert report.summary()["by_origin"] == {"docs.test": 2, "other.test": 1}
    assert report.excluded_by_kind == {}


# ---------------------------------------------------------------------------
# (б) неблокирующие отказы не в gaps, но видны в сводке
# ---------------------------------------------------------------------------


def test_non_blocking_failures_are_excluded_but_published() -> None:
    """``not_found``/``source_unavailable``/``redirect_out_of_scope`` — в ``excluded_by_kind``."""
    items = [
        _item(URL_A, FailureKind.ROBOTS_DISALLOW),
        _item("https://docs.test/files/gone.pdf", FailureKind.NOT_FOUND, http_status=404),
        _item(
            "https://docs.test/files/down.pdf",
            FailureKind.SOURCE_UNAVAILABLE,
            http_status=503,
        ),
        _item(
            "https://docs.test/files/away.pdf",
            FailureKind.REDIRECT_OUT_OF_SCOPE,
            product_id=PRODUCT_B,
        ),
    ]
    report = collect_artifact_gaps(items, run_id=RUN_ID)

    assert [gap.url for gap in report.gaps] == [URL_A]
    expected_excluded = {
        "not_found": 1,
        "redirect_out_of_scope": 1,
        "source_unavailable": 1,
    }
    assert report.excluded_by_kind == expected_excluded
    assert report.summary()["excluded_by_kind"] == expected_excluded
    assert report.summary()["total"] == 1

    # Человеческая строка: разбивка по классам и origin'ам, а не «пусто».
    line = report.human_line()
    assert line.startswith("1 документов недоступно: robots_disallow=1 (docs.test).")
    assert "Заполните `path` и перезапустите" in line


# ---------------------------------------------------------------------------
# (в) детерминированный порядок
# ---------------------------------------------------------------------------


def test_gap_order_is_deterministic_for_shuffled_input() -> None:
    """Порядок ``(product_id, error_kind, url)`` не зависит от порядка обхода."""
    items = [
        _item(URL_B, FailureKind.ROBOTS_DISALLOW, product_id=PRODUCT_B),
        _item(URL_A, FailureKind.ROBOTS_DISALLOW),
        _item(URL_C, FailureKind.PARSER_FAILED, product_id=PRODUCT_A),
        _item(URL_B, FailureKind.ATTEMPTS_EXHAUSTED),
    ]
    shuffled = list(items)
    random.Random(7).shuffle(shuffled)

    first = collect_artifact_gaps(items, run_id=RUN_ID)
    second = collect_artifact_gaps(shuffled, run_id=RUN_ID)

    expected_order = [
        (PRODUCT_A, "attempts_exhausted", URL_B),
        (PRODUCT_A, "parser_failed", URL_C),
        (PRODUCT_A, "robots_disallow", URL_A),
        (PRODUCT_B, "robots_disallow", URL_B),
    ]
    assert [gap.sort_key for gap in first.gaps] == expected_order
    assert [gap.sort_key for gap in second.gaps] == expected_order
    assert first.render() == second.render()


def test_duplicate_url_across_attempts_keeps_latest() -> None:
    """Дубль ``(product_id, url)`` схлопывается по последней попытке, а не по порядку."""
    older = _item(URL_A, FailureKind.SOURCE_UNAVAILABLE, last_attempt="2026-10-01T00:00:00Z")
    newer = _item(URL_A, FailureKind.ROBOTS_DISALLOW, last_attempt="2026-10-24T00:00:00Z")
    report = collect_artifact_gaps([older, newer], run_id=RUN_ID)
    report_reversed = collect_artifact_gaps([newer, older], run_id=RUN_ID)

    assert [gap.error_kind for gap in report.gaps] == [FailureKind.ROBOTS_DISALLOW]
    assert report.render() == report_reversed.render()


# ---------------------------------------------------------------------------
# (г) пустой список — валидный результат, файл создаётся
# ---------------------------------------------------------------------------


def test_empty_gap_list_still_writes_valid_file(tmp_path: Path) -> None:
    """Пробелов нет — файл всё равно пишется (стабильная форма, ``total: 0``)."""
    store = _store()
    _write_items(
        store,
        [_item(URL_A, None, status=CrawlItemStatus.FETCHED, attempts=1)],
    )

    report = write_artifacts_gap(store, run_id=RUN_ID, out_dir=tmp_path)

    path = artifacts_gap_path(tmp_path, RUN_ID)
    assert path == tmp_path / RUN_ID / "outputs" / ARTIFACTS_GAP_FILENAME
    assert report.path == path
    payload = _load(path)
    assert payload == {
        "schema": ARTIFACTS_GAP_SCHEMA,
        "run_id": RUN_ID,
        "gaps": [],
        "summary": {
            "total": 0,
            "by_error_kind": {},
            "by_product": {},
            "by_origin": {},
            "excluded_by_kind": {},
        },
    }
    assert report.human_line() == "0 документов недоступно: блокирующих отказов нет"


def test_file_is_written_without_any_store(tmp_path: Path) -> None:
    """``store is None`` — не «нет артефакта», а пустой отчёт: форма стабильна."""
    report = write_artifacts_gap(None, run_id=RUN_ID, out_dir=tmp_path)
    payload = _load(report.path)  # type: ignore[arg-type]
    assert payload["summary"]["total"] == 0
    assert payload["gaps"] == []


# ---------------------------------------------------------------------------
# (д) corpus и report дают одинаковый файл
# ---------------------------------------------------------------------------


def test_corpus_and_report_write_identical_gap_file(tmp_path: Path) -> None:
    """Один генератор, два вызова: байт-идентичный файл для одного состояния БД."""
    store = _store()
    urls = {PRODUCT_A: [URL_A, URL_B]}
    result = build_corpus(
        _plan(),
        _RefusedCrawler(),
        _Extractor(),
        _Chunker(),
        store,
        urls=urls,
        run_id=RUN_ID,
        raw_root=tmp_path / "corpus_raw",
        out_dir=tmp_path,
    )
    assert result.status == "ok"
    # Per-URL отказы краула видны и в ``errors`` фазы (наблюдаемость), но фазу не роняют.

    path = artifacts_gap_path(tmp_path, RUN_ID)
    from_corpus = path.read_bytes()
    corpus_payload = _load(path)
    assert corpus_payload["summary"]["total"] == 2
    assert corpus_payload["summary"]["by_error_kind"] == {"robots_disallow": 2}
    assert [gap["url"] for gap in corpus_payload["gaps"]] == sorted([URL_A, URL_B])

    # Человеческая строка фазы — в ``log_entries`` (операторский канал корпуса).
    lines = [entry for entry in result.log_entries if isinstance(entry, str)]
    assert any(
        line.startswith("corpus: 2 документов недоступно: robots_disallow=2")
        for line in lines
    )

    with pytest.warns(RuntimeWarning):  # G4: Coverage отсутствует — battle card не эмитится
        artifacts = write_reports(
            bundle=_undefined_bundle(),
            cells=[],
            catalog=_catalog(),
            manifest=_manifest(),
            out_dir=tmp_path,
            run_id=RUN_ID,
            store=store,
        )

    assert artifacts.artifacts_gap == path
    assert path.read_bytes() == from_corpus


# ---------------------------------------------------------------------------
# (е) заготовка входа следующего прогона
# ---------------------------------------------------------------------------


def test_provide_stub_is_filled_by_human_only(tmp_path: Path) -> None:
    """``provide`` содержит ``path: null``/``provided_by: null``; ``reason`` — машинный."""
    store = _store()
    _write_items(
        store,
        [_item(URL_A, FailureKind.ROBOTS_DISALLOW, attempts=2, http_status=None)],
    )
    report = write_artifacts_gap(store, run_id=RUN_ID, out_dir=tmp_path)

    assert report.path is not None
    payload = _load(report.path)
    assert payload["gaps"] == [
        {
            "product_id": PRODUCT_A,
            "url": URL_A,
            "error_kind": "robots_disallow",
            "http_status": None,
            "attempts": 2,
            "last_attempt": TS,
            "origin": "docs.test",
            "provide": {
                "product_id": PRODUCT_A,
                "url": URL_A,
                "path": None,
                "reason": "robots_disallow",
                "provided_by": None,
            },
        }
    ]
    assert payload["summary"] == {
        "total": 1,
        "by_error_kind": {"robots_disallow": 1},
        "by_product": {PRODUCT_A: 1},
        "by_origin": {"docs.test": 1},
        "excluded_by_kind": {},
    }


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
