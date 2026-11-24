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
from core.domain.enums import CrawlNetworkMode, CrawlPolicyMode, CrawlPolicyScopeKind
from core.pipeline.crawl_policy import (
    CrawlPolicyConflict,
    DeclaredRoot,
    RootSpace,
    capture_spaces_by_root,
    declared_roots,
    observations_by_root,
    parse_requested_scopes,
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
    requested: tuple[str, ...] = (),
    crawl_config_digest: str | None = None,
    spaces: dict[tuple[str, str, str], RootSpace] | None = None,
    root_limits: dict[tuple[str, str, str], dict[str, object]] | None = None,
):
    return resolve_crawl_policy(
        package_policy=config,
        operator_rows=rows or [],
        roots=roots,
        now=now,
        observed_at=observed,
        requested=parse_requested_scopes(
            requested, product_ids={root.product_id for root in roots}
        ),
        crawl_config_digest=crawl_config_digest,
        capture_spaces=spaces,
        root_limits=root_limits,
    )

def _limits(marker: str, *, max_depth: int = 3) -> dict[str, object]:
    """Резолвнутые лимиты корня в форме ``CrawlLimits.as_dict()`` (минимальная выборка).

    ``root_limits_digest`` хеширует словарь как есть, поэтому для теста достаточно
    отличимого значения: важно, что **этот** корень объявлен с другими лимитами.
    """
    return {"max_depth": max_depth, "document_level": marker}


def test_execution_for_carries_scheduled_mode_and_interval() -> None:
    """``execution_for`` отдаёт режим и интервал записи — провенанс обязательства §E.8.

    Гейт Ш5 читает ``network_mode`` × ``corpus_source``; ``mode``/``interval_hours``
    нужны писателю ``url_state.next_check_at``: срок = наблюдение + объявленный такт.
    """
    config = _config(
        sites={"p1": {"docs.test": {"mode": "scheduled", "interval_hours": 168}}}
    )
    execution = _resolve(config, [ROOT]).execution_for("p1", "https://docs.test/docs/api")
    assert execution.mode is CrawlPolicyMode.SCHEDULED
    assert execution.interval_hours == 168


def test_execution_for_auto_declares_no_interval() -> None:
    """``auto`` такта не объявляет: ``mode=AUTO``, ``interval_hours=None``."""
    execution = _resolve(_config(defaults={"mode": "auto"}), [ROOT]).execution_for(
        "p1", "https://docs.test/docs/api"
    )
    assert execution.mode is CrawlPolicyMode.AUTO
    assert execution.interval_hours is None


def test_execution_for_unmatched_address_has_no_mode() -> None:
    """Адрес вне объявленных корней: политика его не называла ⇒ обязательства нет."""
    execution = _resolve(_config(), [ROOT]).execution_for("p1", "https://other.test/x")
    assert execution.mode is None
    assert execution.interval_hours is None
    assert execution.network_mode is CrawlNetworkMode.FETCH

def test_product_decision_covers_only_its_declared_root() -> None:
    """Продуктовое решение покрывает адрес **своего корня**, а не всего продукта.

    Пакетный ``defaults`` — запись вида ``product`` (product-wildcard), поэтому у всех
    корней продукта решение одно и то же по режиму, но **исполнение** считается по
    корню: ``on_request`` разрешает сеть только запрошенному корню, ``scheduled``
    решает по часам/пространству своего корня. Пока ``execution_for`` оставлял первое
    решение продукта, запрошенный корень получал ``deny`` соседа (измерено 24.11.2026
    на стенде: корень с решением ``refresh`` краулился ``deny`` первого корня продукта,
    а корень с ``reuse`` скачался по сети на 1 155 страниц). Ранг продуктового решения —
    ``(0, длина пути своего корня)``.
    """
    roots = [
        DeclaredRoot("p1", "a.test", "", root_url="https://a.test/"),
        DeclaredRoot("p1", "b.test", "/docs", root_url="https://b.test/docs/"),
    ]
    resolution = _resolve(
        _config(defaults={"mode": "on_request"}),
        roots,
        # Первый корень уже наблюдался, второй — нет: ``on_request`` обновляет только
        # запрошенное, а первое обнаружение делается автоматически.
        observed={("p1", "a.test", ""): NOW - dt.timedelta(days=1)},
        # Запрошен только второй корень: первый обязан остаться без сети.
        requested=("https://b.test/docs/",),
    )
    assert [
        (item.root_url, item.network_mode) for item in resolution.decisions
    ] == [
        ("https://a.test/", CrawlNetworkMode.DENY),
        ("https://b.test/docs/", CrawlNetworkMode.FETCH),
    ]

    # Адрес под каждым корнем получает решение **своего** корня.
    assert resolution.execution_for("p1", "https://a.test/page").network_mode is (
        CrawlNetworkMode.DENY
    )
    assert resolution.execution_for("p1", "https://b.test/docs/api").network_mode is (
        CrawlNetworkMode.FETCH
    )
    # Адрес вне объявленных корней — прежнее поведение: первое решение продукта.
    outside = resolution.execution_for("p1", "https://c.test/x")
    assert outside.network_mode is CrawlNetworkMode.DENY
    assert outside.scope == "product:p1"


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


def test_scheduled_space_digest_does_not_move_with_the_clock() -> None:
    """Пространство не зависит от того, какое решение по часам принял прогон.

    ``scheduled`` при одном и том же объявленном режиме и лимитах даёт разные
    ``digest`` (поверхность решений входит в ``run_id``) и один ``space_digest``
    (удостоверяемое пространство — носитель I2d).
    """
    config = _config(
        sites={"p1": {"docs.test/docs": {"mode": "scheduled", "interval_hours": 24}}}
    )
    observed = {("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=1)}
    due = _resolve(
        config, [ROOT], observed=observed, now=NOW + dt.timedelta(hours=25)
    )
    fresh = _resolve(config, [ROOT], observed=observed, now=NOW)
    assert due.decisions[0].decision == "refresh"
    assert fresh.decisions[0].decision == "reuse"
    assert due.digest != fresh.digest
    assert due.space_digest == fresh.space_digest


def test_on_request_space_digest_does_not_move_with_the_request() -> None:
    """``--refresh-scope`` меняет поверхность решений, но не объявленное пространство."""
    config = _config(products={"p1": {"mode": "on_request"}})
    observed = {("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=1)}
    plain = _resolve(config, [ROOT], observed=observed)
    requested = _resolve(
        config, [ROOT], observed=observed, requested=("docs.test",)
    )
    assert plain.decisions[0].network_mode != requested.decisions[0].network_mode
    assert plain.digest != requested.digest
    assert plain.space_digest == requested.space_digest


def test_space_digest_tracks_crawl_limits() -> None:
    """Лимиты краула входят в пространство: исчерпание при 100 не удостоверяет 500."""
    config = _config(products={"p1": {"mode": "auto"}})
    first = _resolve(config, [ROOT], crawl_config_digest="limits-100")
    second = _resolve(config, [ROOT], crawl_config_digest="limits-500")
    assert first.space_digest != second.space_digest
    # Поверхность решений лимитов не знает — ``run_id`` от них не меняется.
    assert first.digest == second.digest


def test_space_digest_tracks_declared_mode() -> None:
    """Смена объявленного режима — другое пространство, а не другое решение."""
    auto = _resolve(_config(products={"p1": {"mode": "auto"}}), [ROOT])
    denied = _resolve(
        _config(products={"p1": {"mode": "network_denied"}}), [ROOT]
    )
    assert auto.space_digest != denied.space_digest


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

# ---------------------------------------------------------------------------
# Пространство обхода — второй якорь решения (0.7.0+)
# ---------------------------------------------------------------------------


def _scheduled_config() -> CrawlPolicyConfig:
    return _config(
        sites={"p1": {"docs.test/docs": {"mode": "scheduled", "interval_hours": 168}}}
    )


def test_space_mismatch_forces_refresh_before_the_interval_expires() -> None:
    """Корпус собран в другом пространстве ⇒ обход, хотя сутки не истекли.

    Расписание отвечает «когда обновлять», но не «в каком пространстве собран корпус».
    Переиспользование снятия, сделанного при других лимитах, измерило бы старое
    пространство под новым объявлением (решение владельца 21.11.2026).

    Снятие записано **per-root** формой с другими лимитами — это наблюдённое расхождение
    пространства, а не смена формы записи (для неё — ``None``, см.
    ``test_legacy_global_material_decides_by_clock``).
    """
    import json

    from core.pipeline.digests import crawl_root_space_material

    observed = {("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=2)}
    foreign = crawl_root_space_material(
        product_id="p1",
        scope_kind="root",
        host="docs.test",
        path="/docs",
        mode="scheduled",
        limits_digest="lim-old",
        semantics="crawl-4",
    )
    spaces = {
        ("p1", "docs.test", "/docs"): RootSpace(
            policy_digest="old",
            space_json=json.dumps(foreign, sort_keys=True, separators=(",", ":")),
        )
    }
    decision = _resolve(
        _scheduled_config(),
        [ROOT],
        observed=observed,
        crawl_config_digest="limits-new",
        spaces=spaces,
        root_limits={("p1", "docs.test", "/docs"): _limits("text")},
    ).decisions[0]
    assert decision.space_matches is False
    assert decision.decision == "refresh"
    assert decision.network_mode is CrawlNetworkMode.FETCH


def test_space_match_keeps_clock_decision() -> None:
    """Пространство совпало — решение по часам: ``reuse`` до срока, ``refresh`` после.

    Снятие берётся в той форме, которую пишет ``_record_root_capture``
    (:func:`_root_space_from`): отпечаток **и** материал этого корня. Без материала
    сравнение per-root невозможно (его отпечаток посчитан другой формулой), и решение
    ушло бы по часам как «неизвестно» — это отдельный тест
    (``test_legacy_capture_without_material_is_not_compared_per_root``).
    """
    observed = {("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=2)}
    limits = {("p1", "docs.test", "/docs"): _limits("text")}
    declared = _resolve(
        _scheduled_config(),
        [ROOT],
        observed=observed,
        crawl_config_digest="limits-same",
        root_limits=limits,
    )
    spaces = {
        ("p1", "docs.test", "/docs"): _root_space_from(
            declared, "p1", "https://docs.test/docs/"
        )
    }
    fresh = _resolve(
        _scheduled_config(),
        [ROOT],
        observed=observed,
        crawl_config_digest="limits-same",
        spaces=spaces,
        root_limits=limits,
    ).decisions[0]
    assert fresh.space_matches is True
    assert fresh.decision == "reuse"
    stale = _resolve(
        _scheduled_config(),
        [ROOT],
        observed={("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=200)},
        crawl_config_digest="limits-same",
        spaces=spaces,
        root_limits=limits,
    ).decisions[0]
    assert stale.decision == "refresh"


def test_unknown_space_decides_by_clock() -> None:
    """Снятия нет или отпечатка нет ⇒ ``None``: отсутствие факта не есть расхождение (A3)."""
    observed = {("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=2)}
    no_capture = _resolve(
        _scheduled_config(),
        [ROOT],
        observed=observed,
        crawl_config_digest="limits-new",
    ).decisions[0]
    assert no_capture.space_matches is None
    assert no_capture.decision == "reuse"
    # Снятие есть, но ни материала, ни отпечатка (самая старая строка): сравнивать не с чем.
    unknown = _resolve(
        _scheduled_config(),
        [ROOT],
        observed=observed,
        crawl_config_digest="limits-new",
        spaces={("p1", "docs.test", "/docs"): RootSpace()},
    ).decisions[0]
    assert unknown.space_matches is None
    assert unknown.decision == "reuse"


def test_legacy_capture_without_material_is_not_compared_per_root() -> None:
    """Строка прежней схемы (материала нет) с per-root объявлением — решение по часам.

    Её ``policy_digest`` посчитан **глобальной** формулой (список всех корней пакета), а
    объявленный — формулой корня, поэтому отпечатки разойдутся у **любого** такого снятия
    независимо от того, менялось ли пространство. Запасное правило «оба отпечатка известны
    и различны ⇒ ``False``» здесь дало бы ложное «другое пространство» и разовый пере-обход
    всего пакета — то есть ровно тот дефект (``81dac2e``), ради которого сверка стала
    per-root. Правило «без материала сравниваем по отпечатку» остаётся верным только для
    **глобального** объявления, и оно закреплено на уровне ``space_attests``
    (``test_legacy_capture_without_material_compares_by_digest``).
    """
    decision = _resolve(
        _scheduled_config(),
        [ROOT],
        observed={("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=2)},
        crawl_config_digest="limits-new",
        spaces={("p1", "docs.test", "/docs"): RootSpace(policy_digest="other")},
        root_limits={("p1", "docs.test", "/docs"): _limits("text")},
    ).decisions[0]
    assert decision.space_matches is None
    assert decision.decision == "reuse"


def test_space_check_requires_declared_limits() -> None:
    """Без объявленного ``crawl_config_digest`` стороны сравнения нет — решение по часам.

    ``crawl policy list`` лимитов не резолвит; сравнение с ``None`` дало бы ложное
    «разошлось» и объявило бы обход всем корням сразу.
    """
    decision = _resolve(
        _scheduled_config(),
        [ROOT],
        observed={("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=2)},
        spaces={("p1", "docs.test", "/docs"): RootSpace(policy_digest="d")},
    ).decisions[0]
    assert decision.space_matches is None
    assert decision.decision == "reuse"


def test_requested_scope_still_wins_over_space_match() -> None:
    """Ручной запрос — сильнее сверки: он и так даёт ``refresh``."""
    decision = _resolve(
        _scheduled_config(),
        [ROOT],
        observed={("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=2)},
        requested=("p1",),
        crawl_config_digest="limits-same",
        spaces={("p1", "docs.test", "/docs"): RootSpace(policy_digest="limits-same")},
    ).decisions[0]
    assert decision.decision == "refresh"
    assert decision.requested is True


def test_non_scheduled_modes_ignore_space() -> None:
    """``auto``/``network_denied`` от сверки не зависят: они не решают по часам.

    Сверка при этом **состоялась** (``space_matches=False`` — снятие в другом per-root
    пространстве), и режим её просто не читает: иначе тест не отличал бы «не зависит» от
    «не проверяли».
    """
    for mode, expected in (("auto", "auto"), ("network_denied", "network_denied")):
        config = _config(defaults={"mode": mode})
        observed = {("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=2)}
        declared = _resolve(
            config,
            [ROOT],
            observed=observed,
            crawl_config_digest="limits-same",
            root_limits={("p1", "docs.test", "/docs"): _limits("text")},
        )
        spaces = {
            ("p1", "docs.test", "/docs"): _root_space_from(
                declared, "p1", "https://docs.test/docs/"
            )
        }
        decision = _resolve(
            config,
            [ROOT],
            observed=observed,
            crawl_config_digest="limits-same",
            spaces=spaces,
            # Лимиты этого корня изменились — пространство другое.
            root_limits={("p1", "docs.test", "/docs"): _limits("layout")},
        ).decisions[0]
        assert decision.decision == expected
        assert decision.space_matches is False


def test_capture_spaces_by_root_maps_urls_to_declared_roots() -> None:
    """URL снятия → ключ корня; чужой URL и повтор отбрасываются (новейшее — первым)."""
    roots = [ROOT, ROOT_OTHER]
    spaces = capture_spaces_by_root(
        [
            ("p1", "https://docs.test/docs/", "new", '{"a":1}'),
            ("p1", "https://docs.test/docs/", "older", None),
            ("p1", "https://docs.test/retired/", "gone", None),
            ("p2", "https://docs.test/docs/", "other-product", None),
            ("p1", "https://docs.test/other/", "other-root", None),
        ],
        roots,
    )
    assert spaces == {
        ("p1", "docs.test", "/docs"): RootSpace(policy_digest="new", space_json='{"a":1}'),
        ("p1", "docs.test", "/other"): RootSpace(policy_digest="other-root"),
    }

# ---------------------------------------------------------------------------
# Вариант 3: пространство удостоверяется **по корню**, а не дайджестом пакета
# ---------------------------------------------------------------------------

ROOT_P2 = DeclaredRoot("p2", "other.test", "/docs")

def _root_space_from(resolution, product_id: str, root_url: str) -> RootSpace:
    """Снятие корня в форме, которую пишет ``_record_root_capture`` (per-root)."""
    digest, material = resolution.space_for_root(product_id, root_url)
    assert digest is not None
    import json

    return RootSpace(
        policy_digest=digest,
        space_json=json.dumps(material, sort_keys=True, separators=(",", ":")),
    )

def test_foreign_site_key_does_not_move_neighbour_space() -> None:
    """Добавление site-ключа чужого продукта не меняет пространство соседнего корня.

    Дефект ``81dac2e``: один ключ ``support.huaweicloud.com`` сдвинул глобальный
    ``crawl_config_digest`` у всего пакета, и ``scheduled`` переобошёл чужой продукт.
    Пространство корня = режим этого корня + лимиты этого адреса, поэтому глобальная
    смена здесь не видна.
    """
    observed = {("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=2)}
    config = _config(
        sites={
            "p1": {"docs.test/docs": {"mode": "scheduled", "interval_hours": 168}},
            "p2": {"other.test/docs": {"mode": "scheduled", "interval_hours": 168}},
        }
    )
    p1_limits = {("p1", "docs.test", "/docs"): _limits("text")}
    p2_limits = {("p2", "other.test", "/docs"): _limits("layout")}
    declared = _resolve(
        config,
        [ROOT, ROOT_P2],
        observed=observed,
        crawl_config_digest="limits-before",
        root_limits={**p1_limits, **p2_limits},
    )
    spaces = {
        ("p1", "docs.test", "/docs"): _root_space_from(
            declared, "p1", "https://docs.test/docs/"
        )
    }
    # Чужой site-ключ уже был; его добавление меняет **глобальный** отпечаток, но не
    # per-root лимиты соседнего корня.
    after = _resolve(
        config,
        [ROOT, ROOT_P2],
        observed=observed,
        crawl_config_digest="limits-after-foreign-key",
        spaces=spaces,
        root_limits={**p1_limits, **p2_limits},
    )
    p1 = next(item for item in after.decisions if item.product_id == "p1")
    assert p1.space_matches is True
    assert p1.decision == "reuse"

def test_own_root_limit_change_forces_refresh() -> None:
    """Смена лимитов **этого** корня — другое пространство ⇒ ``refresh`` до срока."""
    observed = {("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=2)}
    declared = _resolve(
        _scheduled_config(),
        [ROOT],
        observed=observed,
        crawl_config_digest="limits",
        root_limits={("p1", "docs.test", "/docs"): _limits("text", max_depth=3)},
    )
    spaces = {
        ("p1", "docs.test", "/docs"): _root_space_from(
            declared, "p1", "https://docs.test/docs/"
        )
    }
    changed = _resolve(
        _scheduled_config(),
        [ROOT],
        observed=observed,
        crawl_config_digest="limits",
        spaces=spaces,
        root_limits={("p1", "docs.test", "/docs"): _limits("text", max_depth=8)},
    ).decisions[0]
    assert changed.space_matches is False
    assert changed.decision == "refresh"

def test_legacy_global_material_decides_by_clock() -> None:
    """Снятие глобальной формы (список всех корней) не сравнивается — решение по часам.

    Формат другой, а не пространство: ``False`` обнулил бы ``complete`` состоявшегося
    снятия только из-за смены формы записи.
    """
    import json

    legacy_material = {
        "crawl_space": [
            ["p1", "root", "docs.test", "/docs", "scheduled"],
            ["p2", "root", "other.test", "/docs", "scheduled"],
        ],
        "crawl_config_digest": "limits",
        "semantics": "crawl-4",
    }
    spaces = {
        ("p1", "docs.test", "/docs"): RootSpace(
            policy_digest="legacy-digest",
            space_json=json.dumps(legacy_material, sort_keys=True),
        )
    }
    decision = _resolve(
        _scheduled_config(),
        [ROOT],
        observed={("p1", "docs.test", "/docs"): NOW - dt.timedelta(hours=2)},
        crawl_config_digest="limits",
        spaces=spaces,
        root_limits={("p1", "docs.test", "/docs"): _limits("text")},
    ).decisions[0]
    assert decision.space_matches is None
    assert decision.decision == "reuse"
