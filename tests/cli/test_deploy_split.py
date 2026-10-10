"""Тесты CLI профиля ``split`` и команды ``deploy-check`` (W13/T13.4+T13.6; ``07``).

Покрывают:

* ``local`` без изменений (пустой :class:`Ports`; split без валидного deploy-конфига — отказ);
* сборку remote-прокси (``RemoteCrawler``/``RemoteSearch``/``RemoteRegistry``/
  ``RemoteExtractor``) через инъектируемый транспорт шва — **без сокета**;
* ``hound deploy-check``: успешный путь (health+capabilities валидны), недостижимость
  (ненулевой код), несовпадение ``schema_version``, нарушение формы конверта, ``--json``;
* сверку конвертов транспорта с **опубликованным** контрактом ``core/edge/seam_schema``;
* ГЭП манифеста: ``manifest_deploy_fields`` кладёт ``deploy_profile``/``edge_schema_version``
  в ``edge_capabilities`` (отдельных доменных полей нет).

Данные категорийно-нейтральны (§1.3). Сеть не используется: ``transport`` зеркалит
edge-сервер, как в ``tests/edge/``.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from core.adapters.remote_crawler import RemoteCrawler
from core.adapters.remote_extractor import RemoteExtractor
from core.adapters.remote_gateway import (
    DEFAULT_SCHEMA_VERSION,
    GatewaySchemaError,
    GatewayTransportError,
    HttpContentGateway,
)
from core.adapters.remote_registry import RemoteRegistry
from core.adapters.remote_search import RemoteSearch
from core.cli.main import EXIT_ERROR, EXIT_OK, EXIT_USAGE, main
from core.cli.wiring import (
    LOCAL_PROFILE,
    SPLIT_PROFILE,
    WiringError,
    build_adapters,
    build_gateway,
    manifest_deploy_fields,
)
from core.config.deploy import DeployConfig, load_deploy_config
from core.edge.seam_schema import SEAM_SCHEMA_VERSION, validate_envelope, validate_payload
from core.pipeline import Ports
from core.ports.search import SearchQuery
from tests.helpers.output import json_out
from tests.helpers.packages import write_text

# ---------------------------------------------------------------------------
# Фикстуры и швы (категорийно-нейтральные)
# ---------------------------------------------------------------------------

_SPLIT_YAML = """\
profile: split
edge:
  base_url: "https://edge.test:8443"
  tls:
    ca: certs/ca.pem
  timeout_s: 5
  max_retries: 0
  max_response_bytes: 52428800
extractor_zone: edge
search:
  budget: {ru: 1, en: 1}
  adapter: example_adapter
"""

_LOCAL_YAML = """\
profile: local
extractor_zone: core
search:
  budget: {ru: 1, en: 1}
  adapter: example_adapter
"""

_HEALTH = {"status": "ok"}
_CAPABILITIES = {
    "extractor_version": "x-1",
    "crawler_version": "c-1",
    "search_engine": "s",
    "supported_formats": ["html"],
}
_SEARCH = [{"url": "https://a.test", "title": "A", "snippet": "sa", "rank": 1}]


def _envelope(op: str, request_id: str, payload: Any, *, schema: str = "1") -> dict[str, Any]:
    """Конверт ответа edge в форме ``07`` §3 (как у ``core/edge/server.py``)."""
    return {
        "schema_version": schema,
        "request_id": request_id,
        "status": "ok",
        "payload": payload,
        "error": None,
    }


def _edge_transport(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
    """Зеркало edge-сервера: ``(op, envelope) -> envelope``; без сети."""
    rid = str(envelope["request_id"])
    payloads: dict[str, Any] = {
        "health": _HEALTH,
        "capabilities": _CAPABILITIES,
        "search": _SEARCH,
    }
    if op not in payloads:
        return {
            "schema_version": DEFAULT_SCHEMA_VERSION,
            "request_id": rid,
            "status": "error",
            "payload": None,
            "error": f"неизвестная операция {op!r}",
        }
    return _envelope(op, rid, payloads[op])


def _split_config() -> DeployConfig:
    return DeployConfig.model_validate(
        {
            "profile": "split",
            "edge": {"base_url": "https://edge.test:8443", "timeout_s": 5, "max_retries": 0},
            "extractor_zone": "edge",
            "search": {"budget": {"ru": 1}, "adapter": "example_adapter"},
        }
    )


# ---------------------------------------------------------------------------
# Профиль local — без изменений; split без валидного конфига — отказ
# ---------------------------------------------------------------------------


def test_local_profile_sets_safe_ports_and_no_network() -> None:
    """``local``: чанкер/экстрактор есть, сетевые/оценщик — ``None`` (L3, без регрессии split)."""
    ports = build_adapters(profile=LOCAL_PROFILE)
    assert isinstance(ports, Ports)
    assert ports.chunker is not None
    assert ports.extractor is not None
    assert ports.crawler is None
    assert ports.evaluator is None


def test_split_with_local_deploy_config_is_rejected() -> None:
    """Профиль split требует deploy-конфиг с profile split/split-hardened."""
    with pytest.raises(WiringError):
        build_adapters(profile=SPLIT_PROFILE, deploy=DeployConfig(profile="local"))


def test_unknown_profile_is_rejected() -> None:
    with pytest.raises(WiringError):
        build_adapters(profile="nope")


# ---------------------------------------------------------------------------
# Сборка remote-прокси (инъектируемый транспорт, без сокета)
# ---------------------------------------------------------------------------


def test_split_wiring_builds_remote_proxies() -> None:
    ports = build_adapters(
        profile=SPLIT_PROFILE, deploy=_split_config(), transport=_edge_transport
    )
    assert isinstance(ports.crawler, RemoteCrawler)
    assert isinstance(ports.search, RemoteSearch)
    assert isinstance(ports.extractor, RemoteExtractor)
    assert isinstance(ports.registry, RemoteRegistry)
    # Контракт портов не изменён: remote-прокси доступны, не-веб порты ещё не собраны.
    assert ports.crawler.available is True
    assert ports.search.adapter_kind == "example_adapter"
    assert ports.registry.registry_id  # данные конфигурации реестра
    assert ports.evaluator is None


def test_split_search_proxy_drop_in_returns_candidates() -> None:
    ports = build_adapters(
        profile=SPLIT_PROFILE, deploy=_split_config(), transport=_edge_transport
    )
    assert isinstance(ports.search, RemoteSearch)
    results = ports.search.search(SearchQuery(text="q", lang="ru", limit=1, phase="corpus"))
    assert [r.url for r in results] == ["https://a.test"]
    assert all(r.raw_bytes is None for r in results)  # I19: кандидат, не evidence


def test_split_wiring_accepts_injected_gateway() -> None:
    gateway = HttpContentGateway("https://edge.test", transport=_edge_transport)
    ports = build_adapters(profile=SPLIT_PROFILE, deploy=_split_config(), gateway=gateway)
    assert isinstance(ports.crawler, RemoteCrawler)
    assert isinstance(ports.search, RemoteSearch)
    assert ports.search.available is True


def test_build_gateway_uses_edge_config_without_pinning() -> None:
    gateway = build_gateway(_split_config())
    assert isinstance(gateway, HttpContentGateway)
    assert gateway.schema_version == DEFAULT_SCHEMA_VERSION


# ---------------------------------------------------------------------------
# deploy-check
# ---------------------------------------------------------------------------


def test_deploy_check_success_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)

    code = main(["deploy-check", "--deploy", str(cfg), "--json"], deploy_transport=_edge_transport)
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["split"] is True
    assert payload["profile"] == "split"
    assert payload["base_url"] == "https://edge.test:8443"
    assert payload["health"] == _HEALTH
    assert payload["capabilities"] == _CAPABILITIES
    assert payload["edge_versions"]["crawler_version"] == "c-1"
    assert payload["deploy_profile"] == "split"
    assert payload["edge_schema_version"] == SEAM_SCHEMA_VERSION
    assert payload["issues"] == []


def test_deploy_check_human_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)

    code = main(["deploy-check", "--deploy", str(cfg)], deploy_transport=_edge_transport)
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "profile: split" in out
    assert "base_url: https://edge.test:8443" in out
    assert "ok: yes" in out


def test_deploy_check_local_profile_is_ok(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = write_text(tmp_path, "deploy.yaml", _LOCAL_YAML)

    code = main(["deploy-check", "--deploy", str(cfg), "--json"])
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["split"] is False
    assert payload["profile"] == "local"


def test_deploy_check_unreachable_is_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)

    def _down(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        raise GatewayTransportError("edge недостижим")

    code = main(["deploy-check", "--deploy", str(cfg), "--json"], deploy_transport=_down)
    assert code == EXIT_ERROR
    payload = json_out(capsys)
    assert payload["ok"] is False
    assert payload["health"] is None
    assert any("health:" in issue for issue in payload["issues"])


def test_deploy_check_schema_mismatch_is_rejected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)

    def _bumped(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        rid = str(envelope["request_id"])
        return _envelope(op, rid, _HEALTH, schema="2")

    code = main(["deploy-check", "--deploy", str(cfg), "--json"], deploy_transport=_bumped)
    assert code == EXIT_ERROR
    payload = json_out(capsys)
    assert payload["ok"] is False
    assert any(
        ("version" in issue) or ("schema" in issue) or ("schema_version" in issue)
        for issue in payload["issues"]
    )


def test_deploy_check_invalid_envelope_shape_is_rejected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Форма конверта (request_id) ловится сверкой с опубликованным контрактом."""
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)

    def _no_request_id(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        payloads = {"health": _HEALTH, "capabilities": _CAPABILITIES}
        return {
            "schema_version": DEFAULT_SCHEMA_VERSION,
            "status": "ok",
            "payload": payloads[op],
            "error": None,
        }

    code = main(
        ["deploy-check", "--deploy", str(cfg), "--json"],
        deploy_transport=_no_request_id,
    )
    assert code == EXIT_ERROR
    payload = json_out(capsys)
    assert payload["ok"] is False
    assert any("request_id" in issue for issue in payload["issues"])


def test_deploy_check_missing_base_url_is_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = write_text(tmp_path, "deploy.yaml", "profile: split\n")

    code = main(["deploy-check", "--deploy", str(cfg)])
    assert code == EXIT_USAGE
    assert "base_url" in capsys.readouterr().err


def test_deploy_check_gateway_factory_injection(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)
    seen: dict[str, str] = {}

    def factory(config: DeployConfig) -> HttpContentGateway:
        seen["profile"] = config.profile
        return HttpContentGateway("https://edge.test", transport=_edge_transport)

    code = main(["deploy-check", "--deploy", str(cfg), "--json"], gateway_factory=factory)
    assert code == EXIT_OK
    assert seen["profile"] == "split"
    assert json_out(capsys)["ok"] is True


# ---------------------------------------------------------------------------
# Сверка с опубликованным контрактом и ГЭП манифеста
# ---------------------------------------------------------------------------


def test_transport_envelopes_match_published_contract() -> None:
    """Конверты диагностического шва проходят ``core/edge/seam_schema`` (07 §3)."""
    envelope = _edge_transport("health", {"request_id": "r1"})
    assert validate_envelope(envelope) == []
    assert validate_payload("health", envelope["payload"]) == []

    caps = _edge_transport("capabilities", {"request_id": "r1"})
    assert validate_envelope(caps) == []
    assert validate_payload("capabilities", caps["payload"]) == []


def test_manifest_deploy_fields_gap_documented() -> None:
    """ГЭП: отдельного доменного поля нет — поля идут в ``edge_capabilities``."""
    config = _split_config()
    fields = manifest_deploy_fields(config)
    assert fields == {"deploy_profile": "split", "edge_schema_version": SEAM_SCHEMA_VERSION}

    merged = manifest_deploy_fields(config, {"crawler_version": "c-1"})
    assert merged["deploy_profile"] == "split"
    assert merged["edge_schema_version"] == SEAM_SCHEMA_VERSION
    assert merged["crawler_version"] == "c-1"


def test_default_deploy_config_is_local() -> None:
    """Канонический ``config/deploy.yaml`` — профиль local (split не активируется сам)."""
    assert load_deploy_config(None).profile == "local"
