"""Тесты CLI профиля ``split`` и команды ``deploy-check`` (W13/T13.4+T13.6; ``07``).

Покрывают:

* ``local`` без изменений (пустой :class:`Ports`; split без валидного deploy-конфига — отказ);
* сборку remote-прокси (``RemoteCrawler``/``RemoteSearch``/``RemoteRegistry``/
  ``RemoteExtractor``) через инъектируемый транспорт шва — **без сокета**;
* ``hound deploy-check``: успешный путь (health+capabilities валидны), недостижимость
  (ненулевой код), несовпадение ``schema_version``, нарушение формы конверта, ``--json``;
* сверку конвертов транспорта с **опубликованным** контрактом ``core/edge/seam_schema``;
* деплой-провенанс манифеста: ``manifest_deploy_fields`` даёт ``deploy_profile``/
  ``edge_schema_version``/``edge_capabilities`` и они текут через ``run`` в фазы Ш9
  (``run_manifest.yaml`` + строка ``runs``) для профилей ``local`` и ``split``.

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
from tests.helpers.packages import (
    capturing_factory,
    run_argv,
    valid_package,
    write_text,
)

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


def _error_envelope(request_id: str, error: str, *, schema: str = "1") -> dict[str, Any]:
    """Конверт ответа edge со ``status=error`` (как у ``core/edge/server.py``)."""
    return {
        "schema_version": schema,
        "request_id": request_id,
        "status": "error",
        "payload": None,
        "error": error,
    }


def _crawl_transport(crawl_error: str, *, ops: list[str] | None = None) -> Any:
    """Транспорт с health/capabilities + настраиваемым ответом ``crawl/status``.

    ``crawl_error`` — текст ``status=error`` для crawl-зонда (имитация живого планировщика,
    не сконфигурированного планировщика или неподдерживаемой операции). Остальные ops,
    кроме health/capabilities, отвечают «неизвестная операция».
    """

    def _transport(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        if ops is not None:
            ops.append(op)
        rid = str(envelope["request_id"])
        if op == "crawl/status":
            return _error_envelope(rid, crawl_error)
        payloads: dict[str, Any] = {"health": _HEALTH, "capabilities": _CAPABILITIES}
        if op not in payloads:
            return _error_envelope(rid, f"неизвестная операция {op!r}")
        return _envelope(op, rid, payloads[op])

    return _transport


# ---------------------------------------------------------------------------
# deploy-check: crawl-зонд (Phase 3.4b)
# ---------------------------------------------------------------------------


def test_deploy_check_reports_crawl_available_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Живой планировщик: ``crawl/status`` отвечает «неизвестное задание» → available."""
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)
    told = _crawl_transport(
        "ValueError: неизвестное задание краула: '__deploy_check_probe__'"
    )

    code = main(["deploy-check", "--deploy", str(cfg), "--json"], deploy_transport=told)
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["crawl"] == {"available": True, "reason": None}
    assert payload["issues"] == []


def test_deploy_check_reports_crawl_unavailable_scheduler_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Операция есть, планировщик не сконфигурирован → unavailable, но ``ok``/код не падают."""
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)
    told = _crawl_transport("crawl scheduler is not available on this edge node")

    code = main(["deploy-check", "--deploy", str(cfg), "--json"], deploy_transport=told)
    assert code == EXIT_OK  # аддитивно: недоступность crawl не меняет код возврата
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["crawl"]["available"] is False
    assert "планировщик" in payload["crawl"]["reason"]
    assert payload["issues"] == []


def test_deploy_check_reports_crawl_not_advertised_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Edge не публикует crawl-операции (``_edge_transport``) → unavailable, ``ok`` True."""
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)

    code = main(
        ["deploy-check", "--deploy", str(cfg), "--json"], deploy_transport=_edge_transport
    )
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["crawl"]["available"] is False
    assert "не публикует crawl-операции" in payload["crawl"]["reason"]


def test_deploy_check_crawl_probe_transport_error_degrades(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Транспортный сбой только на crawl-зонде деградирует, health остаётся ``ok``."""
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)

    def _transport(op: str, envelope: Mapping[str, Any]) -> Mapping[str, Any]:
        if op == "crawl/status":
            raise GatewayTransportError("edge: crawl недостижим")
        rid = str(envelope["request_id"])
        payloads = {"health": _HEALTH, "capabilities": _CAPABILITIES}
        return _envelope(op, rid, payloads[op])

    code = main(["deploy-check", "--deploy", str(cfg), "--json"], deploy_transport=_transport)
    assert code == EXIT_OK
    payload = json_out(capsys)
    assert payload["ok"] is True
    assert payload["crawl"]["available"] is False


def test_deploy_check_human_output_has_crawl_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)
    told = _crawl_transport(
        "ValueError: неизвестное задание краула: '__deploy_check_probe__'"
    )

    code = main(["deploy-check", "--deploy", str(cfg)], deploy_transport=told)
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "crawl: available" in out


def test_deploy_check_human_output_crawl_unavailable_reason(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)
    told = _crawl_transport("crawl scheduler is not available on this edge node")

    code = main(["deploy-check", "--deploy", str(cfg)], deploy_transport=told)
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "crawl: unavailable (crawl-планировщик не сконфигурирован на edge)" in out


def test_deploy_check_crawl_probe_is_read_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Зонд не мутирует edge: вызывается только ``crawl/status``, никаких submit/cancel/collect."""
    cfg = write_text(tmp_path, "deploy.yaml", _SPLIT_YAML)
    ops: list[str] = []
    told = _crawl_transport("crawl scheduler is not available on this edge node", ops=ops)

    code = main(["deploy-check", "--deploy", str(cfg), "--json"], deploy_transport=told)
    assert code == EXIT_OK
    assert ops == ["health", "capabilities", "crawl/status"]
    for mutating in ("crawl/submit", "crawl/cancel", "crawl/collect"):
        assert mutating not in ops


# ---------------------------------------------------------------------------
# Профиль local — без изменений; split без валидного конфига — отказ
# ---------------------------------------------------------------------------


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
# Сверка с опубликованным контрактом и деплой-провенанс манифеста
# ---------------------------------------------------------------------------


def test_transport_envelopes_match_published_contract() -> None:
    """Конверты диагностического шва проходят ``core/edge/seam_schema`` (07 §3)."""
    envelope = _edge_transport("health", {"request_id": "r1"})
    assert validate_envelope(envelope) == []
    assert validate_payload("health", envelope["payload"]) == []

    caps = _edge_transport("capabilities", {"request_id": "r1"})
    assert validate_envelope(caps) == []
    assert validate_payload("capabilities", caps["payload"]) == []


def test_manifest_deploy_fields_structured() -> None:
    """``manifest_deploy_fields`` даёт раздельные поля манифеста + версии edge (07 §7)."""
    config = _split_config()
    fields = manifest_deploy_fields(config)
    assert fields == {
        "deploy_profile": "split",
        "edge_schema_version": SEAM_SCHEMA_VERSION,
        "edge_capabilities": {},
    }

    with_caps = manifest_deploy_fields(config, {"crawler_version": "c-1"})
    assert with_caps["deploy_profile"] == "split"
    assert with_caps["edge_schema_version"] == SEAM_SCHEMA_VERSION
    assert with_caps["edge_capabilities"] == {"crawler_version": "c-1"}


def test_run_passes_local_deploy_provenance_to_phases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``run`` (local) прокидывает деплой-поля в фазы Ш9 из resolved deploy-конфига.

    Без ``config/deploy.yaml`` дефолт — ``local``; версии edge-компонентов неизвестны
    (cmd_run не зовёт ``/v1/capabilities``) → ``edge_capabilities`` пуст (не выдумываем).
    """
    monkeypatch.chdir(tmp_path)
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    captured, factory = capturing_factory()

    code = main(
        run_argv(package, run_id="dp-local", db=db, until="plan"),
        phases_factory=factory,
    )
    assert code == EXIT_OK
    assert captured["deploy_profile"] == LOCAL_PROFILE
    assert captured["edge_schema_version"] == SEAM_SCHEMA_VERSION
    assert captured["edge_capabilities"] == {}


def test_run_passes_split_deploy_provenance_to_phases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``run --profile split`` прокидывает деплой-поля в фазы Ш9 (split deploy.yaml)."""
    monkeypatch.chdir(tmp_path)
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    (cfg_dir / "deploy.yaml").write_text(_SPLIT_YAML, encoding="utf-8")
    package = valid_package(tmp_path)
    db = tmp_path / "cohound.db"
    captured, factory = capturing_factory()

    code = main(
        run_argv(
            package, run_id="dp-split", db=db, until="plan", extra=["--profile", "split"]
        ),
        phases_factory=factory,
    )
    assert code == EXIT_OK
    assert captured["deploy_profile"] == SPLIT_PROFILE
    assert captured["edge_schema_version"] == SEAM_SCHEMA_VERSION
    assert captured["edge_capabilities"] == {}


def test_default_deploy_config_is_local() -> None:
    """Канонический ``config/deploy.yaml`` — профиль local (split не активируется сам)."""
    assert load_deploy_config(None).profile == "local"
