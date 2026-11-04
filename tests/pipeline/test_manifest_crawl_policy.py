"""Политика краула в манифесте и диагностике diff (план 19 §P4).

Здесь проверяется то, что прежде было **дефектом диагностики**, а не пределом:
``crawl_policy_digest`` входил в ``EXECUTION_CONTRACT_FIELDS`` (значит менял ``run_id``),
но не записывался ни в ``runs``, ни на ``RunManifest``. Поэтому ``hound diff`` двух
прогонов с разной политикой честно печатал «различающееся поле не восстановимо из
манифеста»: ось есть, а назвать её нельзя.

Тесты фиксируют три свойства:

* запись/чтение манифеста несут дайджест и провенанс политики;
* diff **называет** ``crawl_policy_digest``, а не предел диагностики; при одинаковой
  политике — молчит;
* ``refresh_policy``/``frozen_source``/``crawl_policy_unmatched``/``captured_at`` видны в
  манифесте и в ``manifest_payload`` (тот же словарь, что ``run_manifest.yaml`` и
  ``hound report --json``), причём ``captured_at`` заполнен только для ``frozen``.

Нейтральные фикстуры (§1.3), сеть не используется.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from core.domain import RunManifest, RunMode
from core.domain.enums import (
    CrawlCorpusSource,
    CrawlNetworkMode,
    CrawlPolicyMode,
    CrawlPolicyScopeKind,
)
from core.pipeline.plan import plan
from core.pipeline.report import (
    CrossCycleDiffError,
    build_manifest,
    crawl_policy_provenance,
    diff_boundary_violations,
    manifest_payload,
    render_diff,
)
from core.store.sqlite_store import SqliteStore

FIXTURES = Path(__file__).resolve().parent.parent / "config" / "fixtures"
RUN_YAML = FIXTURES / "run.yaml"

RUN_ID = "run-policy-1"


def _manifest(**overrides: object) -> RunManifest:
    values: dict[str, object] = {
        "run_id": RUN_ID,
        "case_id": "case-1",
        "created_at": "2026-01-01T00:00:00Z",
        "mode": RunMode.STRICT,
        "methodology_version": "0.2.0",
        "package_version": "pkg-1",
        "catalog_version": "cat-1",
        "status_dict_version": "sd-1",
        "scoring_policy_version": "sp-1",
        "anchor_product_id": "p1",
    }
    values.update(overrides)
    return RunManifest(**values)  # type: ignore[arg-type]


def _cfg():
    import datetime as _dt

    from core.domain import Focus, RankingPolicy, RunConfig

    return RunConfig(
        case_id="case-1",
        anchor="p1",
        cohort="cohort.yaml",
        products=["p1", "p2"],
        package="pkg-1",
        focus=Focus(modules=["m1"], params=[]),
        languages=["ru"],
        snapshot_date=_dt.date(2026, 1, 1),
        mode=RunMode.STRICT,
        ranking_policy=RankingPolicy.CAPABILITY_RANKED,
    )


def _fake_resolution() -> SimpleNamespace:
    """Резолв политики в форме настоящего объекта (только читаемые поля)."""
    decision = SimpleNamespace(
        product_id="p1",
        scope_kind=CrawlPolicyScopeKind.ROOT,
        scope="root:p1@docs.test/docs",
        host="docs.test",
        path="/docs",
        mode=CrawlPolicyMode.NETWORK_DENIED,
        decision="network_denied",
        network_mode=CrawlNetworkMode.DENY,
        corpus_source=CrawlCorpusSource.CHUNKS,
        requested=False,
        reason="crawl_network_denied",
    )
    normal = SimpleNamespace(
        product_id="p2",
        scope_kind=CrawlPolicyScopeKind.HOST,
        scope="host:p2@docs.test",
        host="docs.test",
        path=None,
        mode=CrawlPolicyMode.AUTO,
        decision="auto",
        network_mode=CrawlNetworkMode.FETCH,
        corpus_source=CrawlCorpusSource.NETWORK,
        requested=False,
        reason=None,
    )
    unmatched = SimpleNamespace(
        product_id="p9",
        scope_kind=CrawlPolicyScopeKind.PRODUCT,
        host=None,
        path=None,
        mode=CrawlPolicyMode.ON_REQUEST,
        source="state.db",
    )
    return SimpleNamespace(
        decisions=(decision, normal),
        unmatched=(unmatched,),
        digest="cpd_1",
    )


# --------------------------------------------------------------------------- #
# 1. Круг: запись манифеста → чтение
# --------------------------------------------------------------------------- #


def test_persist_manifest_roundtrips_crawl_policy_axis_and_provenance() -> None:
    """``persist_manifest``→``read_manifest`` несут ось и её провенанс.

    Без колонки ``runs.crawl_policy_digest`` diff не мог назвать расхождение политики;
    без блока провенанса ``refresh_policy``/``frozen_source``/несовпавшие записи читались
    бы только из ``state.db``.
    """
    with SqliteStore(":memory:") as store:
        store.create_run(RUN_ID, _cfg())
        assert store.read_manifest(RUN_ID).crawl_policy_digest is None

        store.persist_manifest(
            RUN_ID,
            _manifest(
                crawl_policy_digest="cpd_1",
                refresh_policy="frozen",
                frozen_source="bytes",
                crawl_policy_unmatched=[
                    {"product_id": "p9", "scope_kind": "product", "mode": "on_request"}
                ],
                policy_skips=[{"product_id": "p1", "root": "https://docs.test/docs"}],
                policy_zero_reasons={"p1": "crawl_policy_skipped"},
                captured_at="2026-11-04T10:00:00+00:00",
                crawl_policy_roots=[{"scope": "root:p1@docs.test/docs"}],
            ),
        )
        read = store.read_manifest(RUN_ID)
        assert read.crawl_policy_digest == "cpd_1"
        assert read.refresh_policy == "frozen"
        assert read.frozen_source == "bytes"
        assert read.crawl_policy_unmatched == [
            {"product_id": "p9", "scope_kind": "product", "mode": "on_request"}
        ]
        assert read.policy_skips == [
            {"product_id": "p1", "root": "https://docs.test/docs"}
        ]
        assert read.policy_zero_reasons == {"p1": "crawl_policy_skipped"}
        assert read.captured_at == "2026-11-04T10:00:00+00:00"
        assert read.crawl_policy_roots == [{"scope": "root:p1@docs.test/docs"}]


# --------------------------------------------------------------------------- #
# 2. Диагностика diff: ось названа, а не «не восстановимо»
# --------------------------------------------------------------------------- #


def test_diff_names_crawl_policy_digest_instead_of_diagnostic_limit(tmp_path: Path) -> None:
    """Разные дайджесты политики — названная ось, а не «предел диагностики»."""
    prev = _manifest(execution_contract_digest="ec_1", crawl_policy_digest="cpd_1")
    curr = _manifest(execution_contract_digest="ec_2", crawl_policy_digest="cpd_2")
    reasons = diff_boundary_violations(prev, curr)

    assert reasons[0] == "execution_change: execution_contract_digest: ec_1 → ec_2"
    assert (
        "execution_change: execution_contract_digest: crawl_policy_digest: "
        "'cpd_1' → 'cpd_2'"
    ) in reasons
    limit = [reason for reason in reasons if "не восстановимо" in reason]
    assert all("crawl_policy_digest" not in line for line in limit)

    # Живой diff подтверждает, что строка доходит до отказа, а не остаётся в helper'е.
    with pytest.raises(CrossCycleDiffError) as excinfo:
        render_diff(prev, curr, [], [], tmp_path / "diff.md")
    message = str(excinfo.value)
    assert "crawl_policy_digest" in message
    assert "'cpd_1' → 'cpd_2'" in message
    assert not (tmp_path / "diff.md").exists()


def test_diff_is_silent_when_politics_are_equal() -> None:
    """Одинаковая политика при том же контракте — границы нет вовсе."""
    prev = _manifest(execution_contract_digest="ec_1", crawl_policy_digest="cpd_1")
    curr = _manifest(execution_contract_digest="ec_1", crawl_policy_digest="cpd_1")
    assert diff_boundary_violations(prev, curr) == []


def test_legacy_row_without_policy_digest_is_missing_not_changed() -> None:
    """Строка прошлой схемы: ``None`` — «не записано», а не «политика изменилась»."""
    prev = _manifest(execution_contract_digest="ec_1")
    curr = _manifest(execution_contract_digest="ec_2", crawl_policy_digest="cpd_2")
    reasons = diff_boundary_violations(prev, curr)

    assert not any("crawl_policy_digest: None" in reason for reason in reasons)
    limit = [reason for reason in reasons if "не восстановимо" in reason]
    assert limit and "crawl_policy_digest" in limit[0]


# --------------------------------------------------------------------------- #
# 3. Провенанс: frozen/auto, unmatched, оси по корням
# --------------------------------------------------------------------------- #


def test_captured_at_is_filled_for_frozen_and_none_for_auto() -> None:
    """``captured_at`` — только для ``frozen``: при ``auto`` снятия не было."""
    resolution = _fake_resolution()

    frozen = crawl_policy_provenance(
        refresh_policy="frozen",
        frozen_source="bytes",
        crawl_policy=resolution,
        captured_at="2026-11-04T10:00:00+00:00",
    )
    assert frozen["frozen_source"] == "bytes"
    assert frozen["captured_at"] == "2026-11-04T10:00:00+00:00"

    auto = crawl_policy_provenance(
        refresh_policy="auto",
        frozen_source="bytes",  # поданный, но не действующий флаг
        crawl_policy=resolution,
        captured_at="2026-11-04T10:00:00+00:00",
    )
    assert auto["frozen_source"] == "n/a"
    assert auto["captured_at"] is None


def test_provenance_lists_only_roots_that_differ_from_network_fetch() -> None:
    """Оси по корням — только отличия: «как обычно» не выдаётся за решение."""
    policy = crawl_policy_provenance(
        refresh_policy="auto",
        frozen_source=None,
        crawl_policy=_fake_resolution(),
    )
    assert [root["scope"] for root in policy["roots"]] == ["root:p1@docs.test/docs"]
    assert policy["roots"][0]["network_mode"] == "deny"
    assert policy["roots"][0]["corpus_source"] == "chunks"
    assert policy["unmatched"][0]["product_id"] == "p9"


# --------------------------------------------------------------------------- #
# 4. Видимость в манифесте и отчёте
# --------------------------------------------------------------------------- #


def test_build_manifest_carries_effective_policy_into_report_payload() -> None:
    """``build_manifest`` → ``manifest_payload`` (файл и ``--json``) несут политику."""
    from core.pipeline.normalize import load_inputs

    inputs = load_inputs(RUN_YAML, FIXTURES)
    manifest = build_manifest(
        inputs=inputs,
        plan=plan(inputs, run_id=RUN_ID),
        crawl_policy=_fake_resolution(),
        refresh_policy="frozen",
        frozen_source="bytes",
        policy_provenance={
            "captured_at": "2026-11-04T10:00:00+00:00",
            "policy_skips": [{"product_id": "p1", "root": "https://docs.test/docs"}],
            "policy_zero_reasons": {"p1": "crawl_policy_skipped"},
        },
    )

    assert manifest.crawl_policy_digest == "cpd_1"
    assert manifest.refresh_policy == "frozen"
    assert manifest.frozen_source == "bytes"
    assert manifest.captured_at == "2026-11-04T10:00:00+00:00"
    assert manifest.crawl_policy_unmatched[0]["product_id"] == "p9"
    assert manifest.policy_zero_reasons == {"p1": "crawl_policy_skipped"}

    payload = manifest_payload(manifest)
    assert payload["crawl_policy_digest"] == "cpd_1"
    assert payload["refresh_policy"] == "frozen"
    assert payload["frozen_source"] == "bytes"
    assert payload["crawl_policy_unmatched"][0]["product_id"] == "p9"
    assert payload["policy_skips"][0]["root"] == "https://docs.test/docs"
    assert payload["captured_at"] == "2026-11-04T10:00:00+00:00"
    assert payload["crawl_policy_roots"][0]["scope"] == "root:p1@docs.test/docs"


@pytest.mark.parametrize("policy", ["auto", "revalidate", "refresh", "offline"])
def test_non_frozen_policy_canonicalizes_frozen_source(policy: str) -> None:
    """Вне ``frozen`` ось ``frozen_source`` — ``n/a`` (иначе ложный ``execution_change``)."""
    assert (
        crawl_policy_provenance(
            refresh_policy=policy,
            frozen_source="chunks",
            crawl_policy=None,
        )["frozen_source"]
        == "n/a"
    )


class _FakeCapture:
    captured_at = "2026-11-04T10:00:00+00:00"


class _FakeCoverage:
    product_id = "p1"
    reason = "crawl_policy_skipped"


class _FakeSession:
    provenance_json = (
        '{"effective_limits": {"roots": ['
        '{"product_id": "p1", "root": "https://docs.test/docs", '
        '"truncated_reason": "crawl_policy_skipped", '
        '"policy_reasons": ["crawl_policy_skipped", "crawl_network_denied"], '
        '"policy_scope": "root:p1@docs.test/docs", '
        '"network_mode": "deny", "corpus_source": "chunks"}]}}'
    )


class _FakeStore:
    def get_crawl_session(self, session_id: str) -> _FakeSession:
        return _FakeSession()

    def iter_corpus_coverage(self, run_id: str) -> list[_FakeCoverage]:
        return [_FakeCoverage()]


class _FakeState:
    def last_corpus_capture(self, product_id: str) -> _FakeCapture:
        return _FakeCapture()


def test_phase_provenance_reads_capture_only_for_frozen() -> None:
    """Провенанс фазы: снятие — только для ``frozen``; пропуски — из durable-источников."""
    from core.pipeline.phases import _crawl_policy_provenance_facts

    frozen = _crawl_policy_provenance_facts(
        _FakeStore(),
        _FakeState(),
        run_id=RUN_ID,
        products=["p1"],
        refresh_policy="frozen",
    )
    assert frozen["captured_at"] == "2026-11-04T10:00:00+00:00"
    assert frozen["policy_skips"] == [
        {
            "product_id": "p1",
            "root": "https://docs.test/docs",
            "scope": "root:p1@docs.test/docs",
            "reasons": ["crawl_policy_skipped", "crawl_network_denied"],
            "network_mode": "deny",
            "corpus_source": "chunks",
        }
    ]
    assert frozen["policy_zero_reasons"] == {"p1": "crawl_policy_skipped"}

    auto = _crawl_policy_provenance_facts(
        _FakeStore(),
        _FakeState(),
        run_id=RUN_ID,
        products=["p1"],
        refresh_policy="auto",
    )
    assert auto["captured_at"] is None
    # Пропуски — факт исполнения, а не следствие ``frozen``: они видны и при ``auto``.
    assert auto["policy_zero_reasons"] == {"p1": "crawl_policy_skipped"}