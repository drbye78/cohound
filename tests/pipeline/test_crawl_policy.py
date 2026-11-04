"""Разрешение политики краула по областям (план 19 §4–§6, слой P1).

Проверяется чистая функция: матрица «область × специфичность × источник», конфликт
равной специфичности (ошибка, а не тай-брейк), видимость несовпавших строк и
стабильность дайджеста. Сеть и БД не участвуют: ``now``/``observed_at`` — аргументы,
поэтому «due» детерминирован.
"""

from __future__ import annotations

import datetime as dt

import pytest

from core.config.crawl_policy import CrawlPolicyConfig
from core.domain.enums import CrawlPolicyMode, CrawlPolicyScopeKind
from core.pipeline.crawl_policy import (
    CrawlPolicyConflict,
    DeclaredRoot,
    declared_roots,
    observations_by_root,
    resolve_crawl_policy,
)
from core.ports.state import CrawlPolicyRow

NOW = dt.datetime(2026, 11, 4, 12, 0, tzinfo=dt.UTC)
ROOT = DeclaredRoot("p1", "docs.test", "/docs")
ROOT_OTHER = DeclaredRoot("p1", "docs.test", "/other")


def _config(**data: object) -> CrawlPolicyConfig:
    return CrawlPolicyConfig.model_validate(data)


def _row(
    *,
    product_id: str = "p1",
    kind: CrawlPolicyScopeKind,
    mode: CrawlPolicyMode,
    host: str | None = None,
    path: str | None = None,
    interval: int | None = None,
    note: str | None = None,
    actor: str = "tester",
    policy_id: int = 1,
) -> CrawlPolicyRow:
    return CrawlPolicyRow(
        policy_id=policy_id,
        product_id=product_id,
        scope_kind=kind,
        mode=mode,
        host=host,
        path=path,
        interval_hours=interval,
        note=note,
        actor=actor,
        created_at="2026-11-01T00:00:00Z",
    )


def _resolve(
    config: CrawlPolicyConfig,
    roots: list[DeclaredRoot],
    rows: list[CrawlPolicyRow] | None = None,
    *,
    observed: dict[tuple[str, str, str], dt.datetime] | None = None,
    now: dt.datetime = NOW,
):
    return resolve_crawl_policy(
        package_policy=config,
        operator_rows=rows or [],
        roots=roots,
        now=now,
        observed_at=observed,
    )


def test_priority_path_beats_root_host_product() -> None:
    """``path`` > ``root`` > ``host`` > ``product`` — независимо от источника."""
    config = _config(
        defaults={"mode": "auto"},
        products={"p1": {"mode": "on_request"}},
        sites={"p1": {"docs.test": {"mode": "scheduled", "interval_hours": 5}}},
    )
    rows = [
        _row(
            kind=CrawlPolicyScopeKind.PATH,
            mode=CrawlPolicyMode.NETWORK_DENIED,
            host="docs.test",
            path="/docs",
        )
    ]
    decision = _resolve(config, [ROOT], rows).decisions[0]
    assert decision.scope_kind is CrawlPolicyScopeKind.PATH
    assert decision.mode is CrawlPolicyMode.NETWORK_DENIED
    assert decision.source == "operator"


def test_longest_literal_prefix_wins_within_kind() -> None:
    """Внутри вида побеждает самый длинный префикс, а не порядок вставки/файла."""
    config = _config(
        sites={
            "p1": {
                "docs.test/docs": {"mode": "on_request"},
                "docs.test/docs/api": {"mode": "network_denied"},
            }
        }
    )
    root = DeclaredRoot("p1", "docs.test", "/docs/api/v1")
    decision = _resolve(config, [root]).decisions[0]
    assert decision.path == "/docs/api"
    assert decision.mode is CrawlPolicyMode.NETWORK_DENIED


def test_operator_overrides_package_at_equal_specificity() -> None:
    """``state.db`` переопределяет пакет ровно на той же области."""
    config = _config(
        sites={"p1": {"docs.test/docs": {"mode": "on_request"}}}
    )
    rows = [
        _row(
            kind=CrawlPolicyScopeKind.ROOT,
            mode=CrawlPolicyMode.AUTO,
            host="docs.test",
            path="/docs",
        )
    ]
    decision = _resolve(config, [ROOT], rows).decisions[0]
    assert decision.source == "operator"
    assert decision.mode is CrawlPolicyMode.AUTO


def test_conflict_equal_specificity_names_both_records() -> None:
    """Равная специфичность с разными режимами — ошибка с токеном и обеими записями."""
    rows = [
        _row(
            kind=CrawlPolicyScopeKind.HOST,
            mode=CrawlPolicyMode.AUTO,
            host="docs.test",
            policy_id=1,
            actor="alice",
        ),
        _row(
            kind=CrawlPolicyScopeKind.HOST,
            mode=CrawlPolicyMode.NETWORK_DENIED,
            host="docs.test",
            policy_id=2,
            actor="bob",
        ),
    ]
    with pytest.raises(CrawlPolicyConflict) as excinfo:
        _resolve(_config(), [ROOT], rows)
    message = str(excinfo.value)
    assert "crawl_policy_conflict" in message
    assert "policy_id=1" in message and "policy_id=2" in message
    assert "auto" in message and "network_denied" in message
    assert excinfo.value.code == "crawl_policy_conflict"


def test_unmatched_is_visible_and_does_not_change_digest() -> None:
    """Строка, не совпавшая ни с одним корнем, видима, но вне материала идентичности."""
    base = _resolve(_config(), [ROOT])
    rows = [
        _row(
            kind=CrawlPolicyScopeKind.HOST,
            mode=CrawlPolicyMode.NETWORK_DENIED,
            host="other.test",
        ),
        _row(
            kind=CrawlPolicyScopeKind.PRODUCT,
            mode=CrawlPolicyMode.NETWORK_DENIED,
            product_id="p9",
        ),
    ]
    with_unmatched = _resolve(_config(), [ROOT], rows)
    assert {item.host or item.product_id for item in with_unmatched.unmatched} == {
        "other.test",
        "p9",
    }
    assert with_unmatched.digest == base.digest
    assert with_unmatched.decisions == base.decisions


def test_digest_ignores_order_and_note_actor_but_tracks_mode() -> None:
    """Дайджест — функция решений: порядок/заметка/автор не входят, режим — входит."""
    first = _row(
        kind=CrawlPolicyScopeKind.HOST,
        mode=CrawlPolicyMode.AUTO,
        host="docs.test",
        note="one",
        actor="alice",
        policy_id=1,
    )
    second = _row(
        kind=CrawlPolicyScopeKind.PRODUCT,
        mode=CrawlPolicyMode.NETWORK_DENIED,
        note="two",
        actor="bob",
        policy_id=2,
    )
    a = _resolve(_config(), [ROOT], [first, second])
    b = _resolve(_config(), [ROOT], [second, first])
    assert a.digest == b.digest

    renoted = CrawlPolicyRow(
        policy_id=999,
        product_id=first.product_id,
        scope_kind=first.scope_kind,
        mode=first.mode,
        host=first.host,
        path=first.path,
        interval_hours=None,
        note="changed",
        actor="carol",
        created_at="2027-01-01T00:00:00Z",
    )
    c = _resolve(_config(), [ROOT], [renoted, second])
    assert c.digest == a.digest

    changed = CrawlPolicyRow(
        policy_id=1,
        product_id=first.product_id,
        scope_kind=first.scope_kind,
        mode=CrawlPolicyMode.NETWORK_DENIED,
        host=first.host,
        path=first.path,
        interval_hours=None,
        note=first.note,
        actor=first.actor,
        created_at=first.created_at,
    )
    d = _resolve(_config(), [ROOT], [changed, second])
    assert d.digest != a.digest


def test_scheduled_due_is_a_decision_and_enters_digest_by_clock() -> None:
    """``scheduled`` разрешается в refresh/reuse; смена решения меняет дайджест."""
    config = _config(
        sites={"p1": {"docs.test/docs": {"mode": "scheduled", "interval_hours": 5}}}
    )
    observed = {("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=2)}
    fresh = _resolve(config, [ROOT], observed=observed)
    stale = _resolve(
        config,
        [ROOT],
        observed={("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=9)},
    )
    assert fresh.decisions[0].decision == "reuse"
    assert stale.decisions[0].decision == "refresh"
    assert fresh.digest != stale.digest
    # Интервал — не материал, но решение (refresh/reuse) — материал.
    same_decision = _resolve(
        config,
        [ROOT],
        observed={("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=10)},
    )
    assert same_decision.digest == stale.digest


def test_scheduled_without_observation_is_discovery() -> None:
    """Наблюдения нет — первичный дискавери (``refresh``), а не выдуманное «свежо»."""
    config = _config(
        sites={"p1": {"docs.test/docs": {"mode": "scheduled", "interval_hours": 5}}}
    )
    decision = _resolve(config, [ROOT]).decisions[0]
    assert decision.decision == "refresh"


def test_no_policy_is_stable_empty_surface() -> None:
    """Пустая политика — стабильный отпечаток пустого множества, а не ``None``."""
    empty = _resolve(_config(), [ROOT])
    assert empty.decisions == ()
    assert empty.digest
    assert empty.digest == _resolve(_config(), [ROOT]).digest


def test_declared_roots_and_observations_by_root() -> None:
    """Корень — из эффективных seeds; наблюдение корня — максимум по URL под ним."""
    from core.domain.models import SeedsConfig

    seeds = SeedsConfig.model_validate(
        {
            "schema": "seeds/1",
            "products": {
                "p1": {
                    "state": "ready",
                    "seeds": [
                        {
                            "url": "https://docs.test/docs/api/",
                            "normalized_url": "https://docs.test/docs/api/",
                            "role": "primary",
                            "source": "operator",
                            "added_at": "2026-11-01",
                            "added_by": "tester",
                        }
                    ],
                }
            },
        }
    )
    roots = declared_roots(seeds)
    assert roots == [DeclaredRoot("p1", "docs.test", "/docs/api")]
    observed = observations_by_root(
        {
            ("p1", "https://docs.test/docs/api/a"): "2026-11-04T09:00:00Z",
            ("p1", "https://docs.test/docs/api/b"): "2026-11-04T11:00:00Z",
            ("p1", "https://docs.test/other"): "2026-11-04T12:00:00Z",
        },
        roots,
    )
    assert observed[("p1", "docs.test", "/docs/api")] == dt.datetime(
        2026, 11, 4, 11, 0, tzinfo=dt.UTC
    )


def test_www_host_is_equivalent_to_bare_host() -> None:
    """``www.`` снимается с обеих сторон: иначе запрет молча не совпал бы с корнем."""
    config = _config(
        sites={"p1": {"www.docs.test/docs": {"mode": "network_denied"}}}
    )
    decision = _resolve(config, [ROOT]).decisions[0]
    assert decision.mode is CrawlPolicyMode.NETWORK_DENIED
    assert decision.host == "docs.test"