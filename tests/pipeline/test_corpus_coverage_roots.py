"""Полнота покрытия по **снятиям** корней и датированный носитель ``❌`` (F3, I2d).

Проверяется дефект замера 05.11.2026 (``demo-absence``): проход без сети на том же
корпусе терял все ячейки, потому что ``CorpusCoverage.complete`` считался по элементам
**сеанса**, а у ``frozen`` их нет. Владелец утвердил перенос носителя (план 20 §11.2):
полноту удостоверяет датированное снятие объявленного корня, а у ``❌`` появляется
дата слабой опоры (``absence_as_of = min``).

Тесты офлайн: SQLite в памяти, без сети. Нейтральны (§1.3).
"""

from __future__ import annotations

import datetime as _dt

import pytest

from core.domain import (
    CrawlItemStatus,
    CrawlSessionItem,
    Focus,
    RankingPolicy,
    RunConfig,
    RunMode,
)
from core.domain.invariants import check_absence_dated
from core.domain.models import CorpusRootCoverage
from core.pipeline.corpus_coverage import (
    _persist_corpus_coverage,
    derive_corpus_coverage,
)
from core.pipeline.crawl_session import default_session_id, open_session
from core.ports.state import CaptureRow
from core.store.sqlite_store import SqliteStore

RUN_ID = "run-roots"
PRODUCT = "p1"
ROOT = "https://docs.test/p1/"
TS = "2026-01-01T00:00:00+00:00"


def _store() -> SqliteStore:
    store = SqliteStore(":memory:")
    store.create_run(
        RUN_ID,
        RunConfig(
            case_id="case",
            anchor=PRODUCT,
            cohort="case",
            products=[PRODUCT],
            package="pkg",
            focus=Focus(),
            snapshot_date=_dt.date(2026, 1, 1),
            mode=RunMode.STRICT,
            ranking_policy=RankingPolicy.CAPABILITY_RANKED,
        ),
    )
    return store


def _capture(
    *,
    capture_id: str = "cap-1",
    root_url: str = ROOT,
    complete: bool = True,
    observed_how: str = "live",
    started_at: str = "2026-01-01T00:00:00+00:00",
    finished_at: str | None = "2026-01-01T00:10:00+00:00",
    superseded_by: str | None = None,
    policy_digest: str | None = None,
) -> CaptureRow:
    return CaptureRow(
        capture_id=capture_id,
        product_id=PRODUCT,
        root_url=root_url,
        started_at=started_at,
        complete=complete,
        observed_how=observed_how,
        finished_at=finished_at,
        superseded_by=superseded_by,
        policy_digest=policy_digest,
    )


def _item(url: str, status: CrawlItemStatus, error_kind: str | None = None) -> CrawlSessionItem:
    return CrawlSessionItem(
        session_id="crawl:" + RUN_ID,
        product_id=PRODUCT,
        url=url,
        status=status,
        error_kind=error_kind,
    )


# --------------------------------------------------------------------------- #
# Полнота — по снятиям, а не по элементам сеанса
# --------------------------------------------------------------------------- #


def test_complete_capture_makes_empty_session_full() -> None:
    """``frozen``-подобный случай: элементов сеанса нет, снятие ``complete=1`` есть."""
    coverage = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[_capture()],
    )

    assert coverage.complete is True
    assert coverage.expected_sources == 0
    assert [root.complete for root in coverage.roots] == [True]
    assert coverage.roots[0].capture_id == "cap-1"
    assert coverage.roots[0].observed_at == "2026-01-01T00:10:00+00:00"


def test_root_without_capture_is_not_complete() -> None:
    """Корень без снятия — «не удостоверён», и никакой сеанс этого не заменяет."""
    coverage = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[_item(ROOT, CrawlItemStatus.FETCHED)],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
    )

    assert coverage.complete is False
    assert coverage.roots == [
        CorpusRootCoverage(root_url=ROOT, capture_id=None, complete=False)
    ]


def test_blocking_failure_beats_complete_capture() -> None:
    """Живой сеанс с блокирующим ``FAILED`` — неполон даже при ``complete=1`` (I2c)."""
    coverage = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[
            _item(ROOT, CrawlItemStatus.FETCHED),
            _item(ROOT + "closed", CrawlItemStatus.FAILED, "robots_disallow"),
        ],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[_capture()],
    )

    assert coverage.complete is False
    assert coverage.blocking_failures == {"robots_disallow": 1}


def test_superseded_capture_is_not_chosen() -> None:
    """Отозванное снятие не удостоверяет корень; берётся новейшее неотозванное."""
    old = _capture(
        capture_id="cap-old",
        complete=True,
        started_at="2025-01-01T00:00:00+00:00",
        finished_at="2025-01-01T00:10:00+00:00",
        superseded_by="cap-new",
    )
    new = _capture(
        capture_id="cap-new",
        complete=False,
        started_at="2026-01-01T00:00:00+00:00",
        finished_at="2026-01-01T00:10:00+00:00",
    )
    coverage = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[old, new],
    )

    assert coverage.roots[0].capture_id == "cap-new"
    assert coverage.complete is False

    only_old = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[old],
    )
    assert only_old.roots[0].capture_id is None
    assert only_old.complete is False


def test_root_address_forms_are_one_root() -> None:
    """Percent-encoded и декодированная формы — **один** корень (форма записи не решает)."""
    encoded = "https://docs.test/p1/%D0%9B%D0%B8%D1%86%D0%B5%D0%BD%D0%B7%D0%B8%D1%8F.pdf"
    decoded = "https://docs.test/p1/Лицензия.pdf"
    coverage = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[encoded, decoded],
        captures=[_capture(root_url=decoded)],
    )

    assert len(coverage.roots) == 1
    assert coverage.roots[0].complete is True
    assert coverage.complete is True


def test_truncated_run_is_incomplete_even_with_capture() -> None:
    """Усечение/пропуск политикой — решение прогона, действует и без элементов."""
    coverage = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=True,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[_capture()],
    )

    assert coverage.complete is False


def test_persist_writes_root_backed_coverage_for_empty_session() -> None:
    """Граница персиста: пустой сеанс + снятие ``complete=1`` ⇒ ``complete=True``."""
    store = _store()
    session = default_session_id(RUN_ID)
    open_session(store, session_id=session, run_id=RUN_ID)
    try:
        written = _persist_corpus_coverage(
            store,
            RUN_ID,
            session,
            set(),
            TS,
            products=[PRODUCT],
            declared_roots={PRODUCT: [ROOT]},
            captures={PRODUCT: [_capture()]},
        )

        coverage = written[PRODUCT]
        assert coverage.complete is True
        assert coverage.expected_sources == 0
        stored = store.read_corpus_coverage(RUN_ID, PRODUCT)
        assert stored is not None
        assert stored.complete is True
        assert stored.roots == coverage.roots
    finally:
        store.close()


def test_roots_json_roundtrip() -> None:
    """``roots_json`` переживает запись/чтение без потери дат и идентификаторов."""
    from core.domain import CorpusCoverage

    store = _store()
    try:
        coverage = CorpusCoverage(
            product_id=PRODUCT,
            complete=True,
            roots=[
                CorpusRootCoverage(
                    root_url=ROOT,
                    capture_id="cap-1",
                    complete=True,
                    observed_how="live",
                    observed_at="2026-01-01T00:10:00+00:00",
                )
            ],
            computed_at=TS,
        )
        store.save_corpus_coverage(RUN_ID, coverage)
        stored = store.read_corpus_coverage(RUN_ID, PRODUCT)

        assert stored is not None
        assert stored.roots == coverage.roots
        assert stored.model_dump(mode="json") == coverage.model_dump(mode="json")
    finally:
        store.close()


def test_cells_absence_as_of_roundtrip() -> None:
    """``cells.absence_as_of`` переносится через store (писать будет Ш6, полоса B)."""
    from core.domain import Cell, CellState

    store = _store()
    try:
        store.save_cell(
            Cell(
                run_id=RUN_ID,
                product_id=PRODUCT,
                param_id="param_a",
                state=CellState.PENDING,
                status=None,
                absence_as_of="2026-01-01T00:10:00+00:00",
            )
        )
        cell = store.get_cell(RUN_ID, PRODUCT, "param_a")
        assert cell is not None
        assert cell.absence_as_of == "2026-01-01T00:10:00+00:00"
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# Сверка удостоверяемого пространства снятия с объявленным (план 20 §11.2, I2d)
# --------------------------------------------------------------------------- #


def test_matching_space_digest_keeps_capture_completeness() -> None:
    """Отпечатки известны и равны ⇒ ``complete`` как у снятия, ``policy_matched=True``."""
    coverage = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[_capture(policy_digest="space-a")],
        declared_space_digest="space-a",
    )

    root = coverage.roots[0]
    assert root.policy_digest == "space-a"
    assert root.policy_matched is True
    assert root.complete is True
    assert coverage.complete is True


def test_mismatched_space_digest_forces_incomplete() -> None:
    """Разошедшийся отпечаток — снятие удостоверяет другое пространство ⇒ неполно."""
    coverage = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[_capture(policy_digest="space-old")],
        declared_space_digest="space-new",
    )

    root = coverage.roots[0]
    assert root.policy_digest == "space-old"
    assert root.policy_matched is False
    assert root.complete is False
    assert coverage.complete is False


def test_missing_space_digest_is_not_a_mismatch() -> None:
    """Нет отпечатка у снятия ⇒ ``policy_matched=None``, ``complete`` не тронут (A3)."""
    coverage = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[_capture(policy_digest=None)],
        declared_space_digest="space-new",
    )

    root = coverage.roots[0]
    assert root.policy_digest is None
    assert root.policy_matched is None
    assert root.complete is True
    assert coverage.complete is True

    # Обратная сторона: объявленного отпечатка нет — тоже «сверять не с чем».
    other = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[_capture(policy_digest="space-old")],
        declared_space_digest=None,
    )
    assert other.roots[0].policy_digest == "space-old"
    assert other.roots[0].policy_matched is None
    assert other.roots[0].complete is True


def _space_resolution(
    *,
    now: _dt.datetime,
    interval_hours: int = 24,
    crawl_config_digest: str | None = None,
):
    """Резолв политики для сверки: один объявленный корень, одно наблюдение.

    Наблюдение фиксировано (01.01 00:00): при ``interval_hours=24`` прогон 01.01 00:30
    получает ``reuse``, а 03.01 — ``refresh``, то есть решение по часам переворачивается
    при одном объявленном режиме и одних лимитах.
    """
    from core.config.crawl_policy import CrawlPolicyConfig
    from core.pipeline.crawl_policy import DeclaredRoot, resolve_crawl_policy

    config = CrawlPolicyConfig.model_validate(
        {
            "products": {
                PRODUCT: {"mode": "scheduled", "interval_hours": interval_hours}
            }
        }
    )
    root = DeclaredRoot(PRODUCT, "docs.test", "/p1")
    return resolve_crawl_policy(
        package_policy=config,
        operator_rows=[],
        roots=[root],
        now=now,
        observed_at={
            (PRODUCT, "docs.test", "/p1"): _dt.datetime(
                2026, 1, 1, tzinfo=_dt.UTC
            )
        },
        crawl_config_digest=crawl_config_digest,
    )


def test_capture_attests_after_scheduled_decision_flips() -> None:
    """Снятие удостоверяет полноту, даже если следующий прогон решил иначе по часам.

    Дефект F3: ``scheduled`` при просроченном корне даёт ``refresh``, при свежем —
    ``reuse``; решения по часам входят в поверхность решений, но не в пространство.
    Снятие, сделанное при ``refresh``, обязано удостоверять ``reuse``-прогон.
    """
    due = _space_resolution(now=_dt.datetime(2026, 1, 3, tzinfo=_dt.UTC))
    fresh = _space_resolution(
        now=_dt.datetime(2026, 1, 1, 0, 30, tzinfo=_dt.UTC)
    )
    assert due.decisions[0].decision == "refresh"
    assert fresh.decisions[0].decision == "reuse"
    assert due.digest != fresh.digest
    assert due.space_digest == fresh.space_digest

    coverage = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[_capture(policy_digest=due.space_digest)],
        declared_space_digest=fresh.space_digest,
    )

    root = coverage.roots[0]
    assert root.policy_matched is True
    assert root.complete is True
    assert coverage.complete is True


def test_capture_does_not_attest_other_crawl_limits() -> None:
    """Другие лимиты краула — другое пространство, даже при том же режиме."""
    narrow = _space_resolution(
        now=_dt.datetime(2026, 1, 2, tzinfo=_dt.UTC),
        crawl_config_digest="limits-100",
    )
    wide = _space_resolution(
        now=_dt.datetime(2026, 1, 2, tzinfo=_dt.UTC),
        crawl_config_digest="limits-500",
    )

    coverage = derive_corpus_coverage(
        product_id=PRODUCT,
        items=[],
        truncated=False,
        computed_at=TS,
        declared_roots=[ROOT],
        captures=[_capture(policy_digest=narrow.space_digest)],
        declared_space_digest=wide.space_digest,
    )

    root = coverage.roots[0]
    assert root.policy_matched is False
    assert root.complete is False
    assert coverage.complete is False


def test_policy_fields_roundtrip_through_store() -> None:
    """``policy_digest``/``policy_matched`` переживают ``roots_json`` без потери."""
    from core.domain import CorpusCoverage

    store = _store()
    try:
        coverage = CorpusCoverage(
            product_id=PRODUCT,
            complete=False,
            roots=[
                CorpusRootCoverage(
                    root_url=ROOT,
                    capture_id="cap-1",
                    complete=False,
                    observed_how="live",
                    observed_at="2026-01-01T00:10:00+00:00",
                    policy_digest="space-old",
                    policy_matched=False,
                )
            ],
            computed_at=TS,
        )
        store.save_corpus_coverage(RUN_ID, coverage)
        stored = store.read_corpus_coverage(RUN_ID, PRODUCT)

        assert stored is not None
        assert stored.roots == coverage.roots
        assert stored.roots[0].policy_matched is False
        assert stored.roots[0].policy_digest == "space-old"
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# I2d: датированный носитель ❌ и absence_as_of = min
# --------------------------------------------------------------------------- #


def test_explicit_without_date_is_undated() -> None:
    ok, as_of, missing = check_absence_dated(
        denial_strength="explicit",
        cited_observations=[("https://docs.test/a", None)],
        roots=[],
    )
    assert ok is False
    assert as_of is None
    assert any("https://docs.test/a" in item for item in missing)


def test_explicit_dates_give_min() -> None:
    ok, as_of, missing = check_absence_dated(
        denial_strength="explicit",
        cited_observations=[
            ("https://docs.test/a", "2026-03-01T00:00:00+00:00"),
            ("https://docs.test/b", "2026-01-01T00:00:00+00:00"),
        ],
        roots=[],
    )
    assert ok is True
    assert missing == []
    assert as_of == "2026-01-01T00:00:00+00:00"


def test_bounded_rejects_adopted_capture() -> None:
    ok, as_of, missing = check_absence_dated(
        denial_strength="bounded",
        cited_observations=[],
        roots=[
            CorpusRootCoverage(
                root_url=ROOT,
                capture_id="cap-1",
                complete=True,
                observed_how="adopted",
                observed_at="2026-01-01T00:10:00+00:00",
            )
        ],
    )
    assert ok is False
    assert as_of is None
    assert any("adopted" in item for item in missing)


def test_bounded_two_roots_take_earlier_date() -> None:
    ok, as_of, missing = check_absence_dated(
        denial_strength="bounded",
        cited_observations=[],
        roots=[
            CorpusRootCoverage(
                root_url="https://docs.test/a",
                capture_id="cap-a",
                complete=True,
                observed_how="live",
                observed_at="2026-03-01T00:00:00+00:00",
            ),
            CorpusRootCoverage(
                root_url="https://docs.test/b",
                capture_id="cap-b",
                complete=True,
                observed_how="live",
                observed_at="2026-01-01T00:00:00+00:00",
            ),
        ],
    )
    assert ok is True
    assert missing == []
    assert as_of == "2026-01-01T00:00:00+00:00"


def test_bounded_equal_dates_have_nonempty_as_of() -> None:
    """Равные даты — одна опора: ``as_of`` непуст (конъюнкция верна до этого мига)."""
    ok, as_of, _missing = check_absence_dated(
        denial_strength="bounded",
        cited_observations=[],
        roots=[
            CorpusRootCoverage(
                root_url="https://docs.test/a",
                capture_id="cap-a",
                complete=True,
                observed_how="live",
                observed_at="2026-01-01T00:10:00+00:00",
            ),
            CorpusRootCoverage(
                root_url="https://docs.test/b",
                capture_id="cap-b",
                complete=True,
                observed_how="scheduled",
                observed_at="2026-01-01T00:10:00+00:00",
            ),
        ],
    )
    assert ok is True
    assert as_of
    assert _dt.datetime.fromisoformat(as_of) == _dt.datetime(
        2026, 1, 1, 0, 10, tzinfo=_dt.UTC
    )


@pytest.mark.parametrize("strength", ["unobserved", "not_stated"])
def test_unknown_strengths_are_not_applicable(strength: str) -> None:
    assert check_absence_dated(
        denial_strength=strength, cited_observations=[], roots=[]
    ) == (True, None, [])