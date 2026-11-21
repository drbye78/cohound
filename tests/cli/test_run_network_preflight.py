"""Preflight сети перед фазами прогона (план 26 §21.3, N3).

Проверяется **правило отказа**, а не сеть: ``probe`` инъектируется, поэтому тесты
детерминированы и ни одного пакета не отправляют. Три исхода объявлены владельцем и
меняются только вместе с методологией:

* **все** объявленные хосты недоступны, а политика требует сети → отказ ``EXIT_USAGE``
  с числами и названным выходом (``--refresh offline``/``frozen``);
* **часть** недоступна → предупреждение с перечнем недоступных хостов, прогон идёт;
* сеть политике не нужна (``offline``/``frozen`` либо ``network_mode=deny`` у всех
  корней) → проверка не выполняется вовсе: отказывать прогону, которому сеть не нужна,
  нечем.

Проверка — не измерение: в ``run_id`` не входит, в корпус не пишет. Нейтрально к
предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as dt

import httpx
import pytest

from core.cli.commands.run import _network_preflight_hosts
from core.cli.wiring import (
    NETWORK_PREFLIGHT_TIMEOUT_S,
    NetworkPreflightError,
    _probe_host_http,
    preflight_network,
)
from core.config.crawl_policy import CrawlPolicyConfig
from core.domain import RefreshPolicy
from core.domain.enums import (
    CrawlNetworkMode,
    CrawlPolicyMode,
    CrawlPolicyScopeKind,
    OnboardingState,
    SeedRole,
    SeedSource,
)
from core.domain.models import ProductSeeds, SeedEntry, SeedsConfig
from core.pipeline.crawl_policy import declared_roots, resolve_crawl_policy
from core.ports.state import CrawlPolicyRow

NOW = dt.datetime(2026, 11, 21, 12, 0, tzinfo=dt.UTC)


def _seed_config(products: dict[str, list[str]]) -> SeedsConfig:
    """Seed-набор из карты ``product_id → [url]`` (по одному активному seed'у на URL)."""
    return SeedsConfig(
        products={
            product_id: ProductSeeds(
                state=OnboardingState.READY,
                seeds=[
                    SeedEntry(
                        url=url,
                        normalized_url=url,
                        role=SeedRole.DOCS,
                        source=SeedSource.OPERATOR,
                        added_at=dt.date(2026, 1, 1),
                        added_by="tester",
                    )
                    for url in urls
                ],
            )
            for product_id, urls in products.items()
        }
    )


def _deny_row(product_id: str, host: str) -> CrawlPolicyRow:
    """Операторская запись «сеть запрещена» на host-область."""
    return CrawlPolicyRow(
        policy_id=1,
        product_id=product_id,
        scope_kind=CrawlPolicyScopeKind.HOST,
        mode=CrawlPolicyMode.NETWORK_DENIED,
        host=host,
        path=None,
        interval_hours=None,
        note=None,
        actor="tester",
        created_at="2026-11-01T00:00:00Z",
    )


def _resolution(seed_config: SeedsConfig, rows: list[CrawlPolicyRow] | None = None):
    """Разрешённая политика по тем же корням, что увидит прогон."""
    return resolve_crawl_policy(
        package_policy=CrawlPolicyConfig.model_validate({}),
        operator_rows=rows or [],
        roots=declared_roots(seed_config),
        now=NOW,
    )


# --- preflight_network: три исхода ------------------------------------------ #


def test_all_hosts_unreachable_is_an_error_with_numbers_and_way_out() -> None:
    """Все хосты недоступны ⇒ отказ с числами и названным выходом (не «сети нет»)."""
    error, warnings = preflight_network(
        hosts=["a.test", "b.test"], timeout_s=2.5, probe=lambda host, timeout: False
    )
    assert isinstance(error, NetworkPreflightError)
    assert warnings == ()
    text = error.describe()
    assert "объявлено хостов=2" in text
    assert "недоступно=2" in text
    assert "timeout=2.5 s" in text
    # Выход обязан быть назван: иначе оператор не знает, что делать.
    assert "--refresh offline" in text
    assert "--refresh frozen" in text


def test_part_of_hosts_unreachable_is_a_warning_naming_only_them() -> None:
    """Часть недоступна ⇒ предупреждение с перечнем именно недоступных хостов."""
    reachable = {"a.test", "c.test"}
    error, warnings = preflight_network(
        hosts=["a.test", "b.test", "c.test"],
        timeout_s=1.0,
        probe=lambda host, timeout: host in reachable,
    )
    assert error is None
    assert len(warnings) == 1
    assert "b.test" in warnings[0]
    assert "a.test" not in warnings[0]
    assert "c.test" not in warnings[0]
    assert "1 из 3" in warnings[0]


def test_all_hosts_reachable_gives_no_error_and_no_warnings() -> None:
    """Все доступны ⇒ ни отказа, ни предупреждений."""
    error, warnings = preflight_network(
        hosts=["a.test", "b.test"], probe=lambda host, timeout: True
    )
    assert error is None
    assert warnings == ()


def test_duplicate_hosts_cost_one_probe() -> None:
    """Дедуп по хосту: один хост, объявленный несколькими URL, стоит одной пробы."""
    calls: list[str] = []

    def probe(host: str, timeout: float) -> bool:
        calls.append(host)
        return True

    error, warnings = preflight_network(
        hosts=["docs.test", "docs.test", "docs.test"], probe=probe
    )
    assert error is None
    assert warnings == ()
    assert calls == ["docs.test"]


def test_empty_hosts_do_not_probe_at_all() -> None:
    """Пустой набор хостов — не повод ходить в сеть: проверка не выполняется."""
    calls: list[str] = []

    def probe(host: str, timeout: float) -> bool:
        calls.append(host)
        return False

    assert preflight_network(hosts=[], probe=probe) == (None, ())
    assert calls == []


# --- реальная проба: HTTP-код не важен, важен факт соединения ---------------- #


def test_http_error_response_means_network_is_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """403/500 — сеть есть: важен факт соединения, а не код ответа.

    Код ответа не делает прогон неисполнимым: 403 сайт объявляет сам (``robots``), а
    500 — сбой приложения, а не отсутствие сети.
    """
    seen: list[tuple[str, dict[str, object]]] = []

    def fake_get(url: str, **kwargs: object) -> object:
        seen.append((url, kwargs))
        return object()  # любой ответ httpx — уже успех

    monkeypatch.setattr(httpx, "get", fake_get)
    assert _probe_host_http("docs.test", 1.5) is True
    assert seen[0][0] == "https://docs.test/"
    assert seen[0][1]["timeout"] == 1.5
    # HEAD не используется: часть серверов отвечает на него иначе, чем на GET.
    assert seen[0][1].get("follow_redirects") is False


def test_connection_error_means_no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Исключение соединения/таймаут ⇒ «нет сети» (а не падение прогона)."""

    def boom(url: str, **kwargs: object) -> object:
        raise httpx.ConnectError("нет маршрута")

    monkeypatch.setattr(httpx, "get", boom)
    assert _probe_host_http("docs.test", 1.0) is False


def test_plain_http_host_is_not_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """Хост без TLS — не «нет сети»: объявленный хост это имя, а не пара (схема, порт).

    Иначе прогон по корню, открытому только по ``http``, отказывал бы на preflight,
    хотя материал доступен: проверяется факт сетевого пути, а не протокол.
    """
    seen: list[str] = []

    def fake_get(url: str, **kwargs: object) -> object:
        seen.append(url)
        if url.startswith("https://"):
            raise httpx.ConnectError("tls нет")
        return object()

    monkeypatch.setattr(httpx, "get", fake_get)
    assert _probe_host_http("docs.test", 1.0) is True
    assert seen == ["https://docs.test/", "http://docs.test/"]


def test_ipv6_literal_is_bracketed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Литерал IPv6 без скобок не разбирается как authority — значит, нужны скобки."""
    seen: list[str] = []

    def fake_get(url: str, **kwargs: object) -> object:
        seen.append(url)
        return object()

    monkeypatch.setattr(httpx, "get", fake_get)
    assert _probe_host_http("2001:db8::1", 1.0) is True
    assert seen == ["https://[2001:db8::1]/"]


def test_default_timeout_is_declared_and_short() -> None:
    """Таймаут по умолчанию объявлен константой: это проверка связности, не скорости."""
    assert NETWORK_PREFLIGHT_TIMEOUT_S > 0
    assert NETWORK_PREFLIGHT_TIMEOUT_S <= 10


# --- гейт политики: когда проверка не выполняется вовсе ---------------------- #


def test_offline_and_frozen_do_not_require_network() -> None:
    """``--refresh offline``/``frozen`` — ноль запросов, проверять нечего."""
    seeds = _seed_config({"p1": ["https://docs.test/docs"]})
    resolution = _resolution(seeds)
    for policy in (RefreshPolicy.OFFLINE, RefreshPolicy.FROZEN):
        assert (
            _network_preflight_hosts(
                policy_resolution=resolution,
                seed_config=seeds,
                refresh_policy=policy,
            )
            == []
        )


def test_all_roots_denied_do_not_require_network() -> None:
    """``network_mode=deny`` у всех корней ⇒ прогону сеть не нужна, отказа нет."""
    seeds = _seed_config({"p1": ["https://docs.test/docs"]})
    resolution = _resolution(seeds, [_deny_row("p1", "docs.test")])
    assert resolution.decisions[0].network_mode is CrawlNetworkMode.DENY
    assert (
        _network_preflight_hosts(
            policy_resolution=resolution,
            seed_config=seeds,
            refresh_policy=RefreshPolicy.AUTO,
        )
        == []
    )


def test_only_fetch_roots_are_probed_and_hosts_are_deduped() -> None:
    """Проверяются только хосты ``fetch``-корней, по одному разу на хост."""
    seeds = _seed_config(
        {
            "p1": ["https://docs.test/docs", "https://docs.test/api"],
            "p2": ["https://denied.test/docs"],
        }
    )
    resolution = _resolution(seeds, [_deny_row("p2", "denied.test")])
    hosts = _network_preflight_hosts(
        policy_resolution=resolution,
        seed_config=seeds,
        refresh_policy=RefreshPolicy.AUTO,
    )
    assert hosts == ["docs.test"]
