"""Тесты middleware шва core↔edge: auth → authz → лимит тела → аудит (07 §8).

Проверяется то, что раньше **декларировалось, но не исполнялось**: запрос проходит
аутентификацию/авторизацию, лимит тела и аудит, а composition root действительно
конструирует и протягивает эти компоненты. Сеть не используется: HTTP-слой
вызывается на «рукописном» handler-экземпляре (``BaseHTTPRequestHandler.__new__``),
``build_server`` — с инъецированной фабрикой. Данные нейтральны (§1.3).
"""

from __future__ import annotations

import email.message
import io
import json
import re
from pathlib import Path
from typing import Any

import pytest

import core.edge.__main__ as edge_main
from core.edge.audit import (
    AUDIT_STATUS_ALLOW,
    AUDIT_STATUS_DENY,
    AUDIT_STATUS_ERROR,
    Classification,
    FileAuditSink,
    MemoryAuditSink,
)
from core.edge.auth import (
    AUTHORIZED_OPS,
    PUBLIC_OPS,
    MTLSAuthenticator,
    _OP_SCOPE,
)
from core.edge.config import EdgeRuntimeConfig, load_edge_runtime_config
from core.edge.seam_schema import ENDPOINT_OPS
from core.edge.server import (
    DEFAULT_MAX_BODY_BYTES,
    EdgeGateway,
    EdgeSecurityError,
    _KNOWN_OPS,
    build_server,
    is_loopback_host,
    make_handler,
    route_request,
    serve,
)
from core.ports.types import ExtractedDoc, ExtractionRoute, FetchPlan, RawPage

_FIXED_ID = "f" * 32
_FIXED_TIME = "2026-01-01T00:00:00+00:00"

_SUBJECTS = {
    "svc-core": ["fetch", "search", "registry", "capabilities", "crawl"],
    "svc-fetch": ["fetch"],
}


# ---------------------------------------------------------------------------
# Стенд gateway (без сети)
# ---------------------------------------------------------------------------


class _Crawler:
    version = "fake-crawler-1"

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        return RawPage(
            url=url,
            content=b"<html>neutral</html>",
            fetched_at=_FIXED_TIME,
            status_code=200,
            content_type="text/html",
        )


class _Extractor:
    version = "fake-extractor-1"

    def extract(
        self, page: RawPage, route: ExtractionRoute = ExtractionRoute.AUTO
    ) -> ExtractedDoc:
        return ExtractedDoc(
            text="clean", content_hash="h", extractor_version=self.version
        )


def _edge() -> EdgeGateway:
    """Детерминированный gateway: фиксированные ``request_id`` и часы."""
    return EdgeGateway(
        _Crawler(),
        _Extractor(),
        clock=lambda: _FIXED_TIME,
        id_factory=lambda: _FIXED_ID,
    )


# ---------------------------------------------------------------------------
# HTTP-стенд без сокета
# ---------------------------------------------------------------------------


class _TrackingReader(io.BytesIO):
    """``rfile``-двойник: запоминает запрошенные размеры ``read``."""

    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self.read_sizes: list[int] = []

    def read(self, n: int = -1) -> bytes:  # noqa: D401 - контракт file-like
        self.read_sizes.append(n)
        return super().read(n)


class _LyingReader(io.BytesIO):
    """``rfile``-двойник, игнорирующий ``n``: имитирует тело больше объявленного."""

    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self.read_sizes: list[int] = []

    def read(self, n: int = -1) -> bytes:  # noqa: D401 - контракт file-like
        self.read_sizes.append(n)
        return self.getvalue()


def _invoke(
    handler_cls: type,
    *,
    path: str,
    method: str = "POST",
    headers: dict[str, str] | None = None,
    body: bytes = b"",
    reader_cls: type = _TrackingReader,
) -> Any:
    """Создаёт handler без сокета и прогоняет один запрос через ``_dispatch``."""
    handler = handler_cls.__new__(handler_cls)
    handler.headers = email.message.Message()
    for key, value in (headers or {}).items():
        handler.headers[key] = value
    handler.path = path
    handler.command = method
    handler.request_version = "HTTP/1.1"
    handler.requestline = f"{method} {path} HTTP/1.1"
    handler.client_address = ("10.1.2.3", 54321)
    handler.rfile = reader_cls(body)
    handler.wfile = io.BytesIO()
    handler.close_connection = False
    handler._headers_buffer = []
    handler._dispatch(method)
    return handler


def _response(handler: Any) -> tuple[int, dict[str, str], dict[str, Any] | None]:
    """Разбирает записанный HTTP-ответ: ``(status, headers, envelope)``."""
    raw = handler.wfile.getvalue()
    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            key, _, value = line.partition(":")
            headers[key.strip().lower()] = value.strip()
    return status, headers, json.loads(body) if body else None


def _fetch_body(url: str = "https://site.test/x") -> bytes:
    return json.dumps({"url": url}).encode("utf-8")


# ---------------------------------------------------------------------------
# 1. Дефолт: без authenticator/audit поведение прежнее
# ---------------------------------------------------------------------------


def test_default_route_without_middleware_unchanged() -> None:
    code, env = route_request(_edge(), "POST", "/v1/fetch", _fetch_body())
    assert code == 200
    assert env["status"] == "ok"
    assert env["payload"]["final_url"] == "https://site.test/x"
    assert env["request_id"] == _FIXED_ID


def test_default_handler_without_middleware_succeeds() -> None:
    handler = _invoke(
        make_handler(_edge()),
        path="/v1/fetch",
        headers={"Content-Length": str(len(_fetch_body()))},
        body=_fetch_body(),
    )
    status, _headers, env = _response(handler)
    assert status == 200
    assert env is not None and env["status"] == "ok"


# ---------------------------------------------------------------------------
# 2. Аутентификация → авторизация (401/403) + аудит DENY
# ---------------------------------------------------------------------------


def test_authenticated_op_without_credentials_is_401_and_audited_deny() -> None:
    sink = MemoryAuditSink()
    code, env = route_request(
        _edge(),
        "POST",
        "/v1/fetch",
        _fetch_body(),
        authenticator=MTLSAuthenticator(_SUBJECTS),
        auth_request={},
        audit_sink=sink,
        remote="10.1.2.3",
    )
    assert code == 401
    assert env["status"] == "error"
    assert env["request_id"] == _FIXED_ID
    assert len(sink.events) == 1
    event = sink.events[0]
    assert event.status == AUDIT_STATUS_DENY
    assert event.op == "fetch"
    assert event.request_id == env["request_id"]
    assert event.principal is None
    assert event.remote == "10.1.2.3"


def test_principal_without_scope_is_403_and_audited_deny() -> None:
    sink = MemoryAuditSink()
    code, env = route_request(
        _edge(),
        "POST",
        "/v1/search",
        json.dumps({"query": "q"}).encode("utf-8"),
        authenticator=MTLSAuthenticator(_SUBJECTS),
        auth_request={"X-Client-Cert-Subject": "svc-fetch"},
        audit_sink=sink,
    )
    assert code == 403
    assert env["status"] == "error"
    assert sink.events[0].status == AUDIT_STATUS_DENY
    assert sink.events[0].principal == "svc-fetch"


def test_principal_with_scope_is_200_and_audited_allow() -> None:
    sink = MemoryAuditSink()
    code, env = route_request(
        _edge(),
        "POST",
        "/v1/fetch",
        _fetch_body(),
        authenticator=MTLSAuthenticator(_SUBJECTS),
        auth_request={"X-Client-Cert-Subject": "svc-fetch"},
        audit_sink=sink,
    )
    assert code == 200
    assert env["status"] == "ok"
    assert sink.events[0].status == AUDIT_STATUS_ALLOW
    assert sink.events[0].principal == "svc-fetch"
    assert sink.events[0].request_id == env["request_id"]


def test_public_health_needs_no_credentials() -> None:
    sink = MemoryAuditSink()
    code, env = route_request(
        _edge(),
        "GET",
        "/v1/health",
        authenticator=MTLSAuthenticator(_SUBJECTS),
        auth_request={},
        audit_sink=sink,
    )
    assert code == 200 and env["status"] == "ok"
    assert sink.events[0].status == AUDIT_STATUS_ALLOW


def test_handler_propagates_authenticator_to_route_request() -> None:
    """Конфигурированный authenticator действует через HTTP-слой (не только напрямую)."""
    sink = MemoryAuditSink()
    handler = _invoke(
        make_handler(_edge(), authenticator=MTLSAuthenticator(_SUBJECTS), audit_sink=sink),
        path="/v1/fetch",
        headers={"Content-Length": str(len(_fetch_body()))},
        body=_fetch_body(),
    )
    status, _headers, env = _response(handler)
    assert status == 401
    assert env is not None and env["status"] == "error"
    assert sink.events[0].status == AUDIT_STATUS_DENY


# ---------------------------------------------------------------------------
# 3. request_id: аудит и конверт несут одно значение
# ---------------------------------------------------------------------------


def test_audit_request_id_matches_envelope_when_client_supplies_header() -> None:
    sink = MemoryAuditSink()
    _code, env = route_request(
        _edge(),
        "POST",
        "/v1/fetch",
        _fetch_body(),
        request_id="rid-42",
        authenticator=MTLSAuthenticator(_SUBJECTS),
        auth_request={"X-Client-Cert-Subject": "svc-fetch"},
        audit_sink=sink,
    )
    assert env["request_id"] == "rid-42"
    assert sink.events[0].request_id == "rid-42"


def test_audit_request_id_matches_envelope_when_generated() -> None:
    sink = MemoryAuditSink()
    _code, env = route_request(
        _edge(),
        "POST",
        "/v1/fetch",
        _fetch_body(),
        authenticator=MTLSAuthenticator(_SUBJECTS),
        auth_request={"X-Client-Cert-Subject": "svc-fetch"},
        audit_sink=sink,
    )
    # Один идентификатор на вызов: второго (сгенерированного в аудите) нет.
    assert env["request_id"] == _FIXED_ID
    assert sink.events[0].request_id == _FIXED_ID


def test_handler_generates_one_request_id_for_audit_and_envelope() -> None:
    sink = MemoryAuditSink()
    handler = _invoke(
        make_handler(_edge(), authenticator=MTLSAuthenticator(_SUBJECTS), audit_sink=sink),
        path="/v1/fetch",
        headers={"Content-Length": str(len(_fetch_body()))},
        body=_fetch_body(),
    )
    status, _headers, env = _response(handler)
    assert status == 401
    assert env is not None
    assert env["request_id"] == _FIXED_ID
    assert sink.events[0].request_id == _FIXED_ID


# ---------------------------------------------------------------------------
# 4. Лимит тела: 413 до чтения / ограниченное чтение / Connection: close
# ---------------------------------------------------------------------------


def test_route_rejects_body_over_limit_with_413_and_audit_error() -> None:
    sink = MemoryAuditSink()
    code, env = route_request(
        _edge(),
        "POST",
        "/v1/fetch",
        b"x" * 65,
        audit_sink=sink,
        max_body_bytes=64,
    )
    assert code == 413
    assert env["status"] == "error"
    assert sink.events[0].status == AUDIT_STATUS_ERROR


def test_handler_rejects_declared_length_over_limit_without_reading() -> None:
    handler = _invoke(
        make_handler(_edge(), max_body_bytes=64),
        path="/v1/fetch",
        headers={"Content-Length": "5000000000"},
    )
    status, headers, env = _response(handler)
    assert status == 413
    assert env is not None and env["status"] == "error"
    # Тело не читалось вовсе: декларация на 5 ГБ не привела к аллокации.
    assert handler.rfile.read_sizes == []
    # Соединение закрыто: непрочитанное тело не станет «следующим запросом».
    assert headers["connection"] == "close"
    assert handler.close_connection is True


def test_handler_caps_read_when_length_absent_and_chunked_body_is_oversized() -> None:
    handler = _invoke(
        make_handler(_edge(), max_body_bytes=64),
        path="/v1/fetch",
        headers={"Transfer-Encoding": "chunked"},
        body=b"x" * 4096,
    )
    status, headers, env = _response(handler)
    assert status == 413
    assert env is not None
    # Чтение ограничено лимитом+1, а не объявленным/фактическим объёмом.
    assert handler.rfile.read_sizes == [65]
    assert headers["connection"] == "close"


def test_handler_rejects_lying_content_length_with_413() -> None:
    handler = _invoke(
        make_handler(_edge(), max_body_bytes=64),
        path="/v1/fetch",
        headers={"Content-Length": "16"},
        body=b"x" * 4096,
        reader_cls=_LyingReader,
    )
    status, headers, env = _response(handler)
    assert status == 413
    assert env is not None and env["status"] == "error"
    # Запрошено не больше объявленного и не больше лимита+1.
    assert handler.rfile.read_sizes == [16]
    assert headers["connection"] == "close"


# ---------------------------------------------------------------------------
# 5. Аудит: классификация из Classification.for_call, отказы не теряются
# ---------------------------------------------------------------------------


def test_audit_classification_comes_from_classification_for_call() -> None:
    sink = MemoryAuditSink()
    marker = Classification(
        label="internal",
        override=lambda op, payload: f"{op}:{payload.get('url', '')}",
    )
    code, _env = route_request(
        _edge(),
        "POST",
        "/v1/fetch",
        _fetch_body("https://site.test/classified"),
        audit_sink=sink,
        classification=marker,
    )
    assert code == 200
    assert sink.events[0].classification == "fetch:https://site.test/classified"


def test_audit_default_classification_is_internal() -> None:
    sink = MemoryAuditSink()
    route_request(_edge(), "GET", "/v1/health", audit_sink=sink)
    assert sink.events[0].classification == "internal"


def test_audit_emits_error_for_transport_failures() -> None:
    sink = MemoryAuditSink()
    code, _env = route_request(_edge(), "POST", "/v1/fetch", b"{bad", audit_sink=sink)
    assert code == 400
    assert sink.events[0].status == AUDIT_STATUS_ERROR


def test_audit_emits_error_for_unknown_route() -> None:
    sink = MemoryAuditSink()
    code, _env = route_request(_edge(), "GET", "/v1/nope", audit_sink=sink)
    assert code == 404
    assert sink.events[0].status == AUDIT_STATUS_ERROR


# ---------------------------------------------------------------------------
# 6. Fail-fast: bind вне петли без аутентификации
# ---------------------------------------------------------------------------


def _fake_server_factory(address: tuple[str, int], handler: Any) -> Any:
    return object()


@pytest.mark.parametrize(
    "host", ["127.0.0.1", "127.0.0.2", "::1", "[::1]", "localhost", "LOCALHOST"]
)
def test_loopback_hosts_are_allowed_without_auth(host: str) -> None:
    assert is_loopback_host(host) is True
    assert build_server(host, 0, _edge(), server_factory=_fake_server_factory)


@pytest.mark.parametrize(
    "host", ["0.0.0.0", "::", "192.168.1.10", "edge.internal", ""]
)
def test_non_loopback_without_auth_refuses_to_start(host: str) -> None:
    assert is_loopback_host(host) is False
    with pytest.raises(EdgeSecurityError) as excinfo:
        build_server(host, 0, _edge(), server_factory=_fake_server_factory)
    # Ошибка называет адрес — оператор понимает, что именно отвергнуто.
    assert f"host={host!r}" in str(excinfo.value)


def test_serve_refuses_non_loopback_without_auth() -> None:
    with pytest.raises(EdgeSecurityError):
        serve("0.0.0.0", 0, _edge(), server_factory=_fake_server_factory)


def test_non_loopback_with_authenticator_starts() -> None:
    server = build_server(
        "0.0.0.0",
        0,
        _edge(),
        server_factory=_fake_server_factory,
        authenticator=MTLSAuthenticator(_SUBJECTS),
    )
    assert server is not None


# ---------------------------------------------------------------------------
# 7. Allowlist consistency: auth.AUTHORIZED_OPS ↔ server._KNOWN_OPS
# ---------------------------------------------------------------------------


def test_authorized_ops_agree_with_routable_ops() -> None:
    """Расхождение allowlist'ов разрешало бы не-маршрутизируемую операцию
    или запрещало маршрутизируемую — поэтому наборы обязаны совпадать."""
    assert AUTHORIZED_OPS == _KNOWN_OPS
    assert set(_OP_SCOPE) == AUTHORIZED_OPS
    assert PUBLIC_OPS <= AUTHORIZED_OPS
    assert ENDPOINT_OPS == _KNOWN_OPS


# ---------------------------------------------------------------------------
# 8. OpenAPI: 401/403/413 и схемы безопасности
# ---------------------------------------------------------------------------


def test_openapi_publishes_auth_and_body_limit_responses() -> None:
    path = Path(__file__).resolve().parents[2] / "contracts" / "seam" / "openapi.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    schemes = doc["components"]["securitySchemes"]
    assert schemes["mtls"]["type"] == "mutualTLS"
    assert schemes["bearerJWT"]["type"] == "http"
    assert schemes["bearerJWT"]["scheme"] == "bearer"
    for op_path, item in doc["paths"].items():
        for method, operation in item.items():
            if not isinstance(operation, dict):
                continue
            for code in ("401", "403", "413"):
                response = operation["responses"][code]
                schema = response["content"]["application/json"]["schema"]
                assert schema["$ref"] == "#/components/schemas/Envelope", (
                    op_path,
                    method,
                    code,
                )


# ---------------------------------------------------------------------------
# 9. Composition root: конфиг → конструкторы → путь запроса
# ---------------------------------------------------------------------------


def test_config_defaults_reproduce_todays_behaviour() -> None:
    config = EdgeRuntimeConfig()
    assert config.max_body_bytes == DEFAULT_MAX_BODY_BYTES
    assert config.auth.mode == "none"
    assert config.audit.sink == "none"
    assert edge_main._build_authenticator(config) is None
    assert edge_main._build_audit_sink(config) is None


def test_config_rejects_incomplete_auth_blocks(tmp_path: Path) -> None:
    bad_mtls = tmp_path / "mtls.yaml"
    bad_mtls.write_text("auth: {mode: mtls}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="subjects"):
        load_edge_runtime_config(bad_mtls)

    bad_oidc = tmp_path / "oidc.yaml"
    bad_oidc.write_text("auth: {mode: oidc}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="issuer"):
        load_edge_runtime_config(bad_oidc)

    bad_audit = tmp_path / "audit.yaml"
    bad_audit.write_text("audit: {sink: file}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="audit.path"):
        load_edge_runtime_config(bad_audit)


def test_launcher_wiring_reaches_request_path_end_to_end(tmp_path: Path) -> None:
    """Тот же конфиг/конструкторы, что у лаунчера, реально отказывают в запросе.

    Это регрессионный тест на «построено, но не подключено»: authenticator и
    audit sink собираются из ``edge.yaml`` теми же функциями, что в ``main``,
    затем проходят через ``build_server`` → handler → ``route_request``.
    """
    audit_path = tmp_path / "audit.jsonl"
    config_path = tmp_path / "edge.yaml"
    config_path.write_text(
        "host: 127.0.0.1\n"
        "auth:\n"
        "  mode: mtls\n"
        "  subjects:\n"
        "    svc-core: [fetch, search, registry, capabilities, crawl]\n"
        "audit:\n"
        "  sink: file\n"
        f"  path: {audit_path}\n",
        encoding="utf-8",
    )
    config = load_edge_runtime_config(config_path)
    authenticator = edge_main._build_authenticator(config)
    audit_sink = edge_main._build_audit_sink(config)
    assert isinstance(authenticator, MTLSAuthenticator)
    assert isinstance(audit_sink, FileAuditSink)

    captured: dict[str, Any] = {}

    def factory(address: tuple[str, int], handler: Any) -> Any:
        captured["handler"] = handler
        return object()

    build_server(
        config.host,
        config.port,
        _edge(),
        server_factory=factory,
        authenticator=authenticator,
        audit_sink=audit_sink,
        max_body_bytes=config.max_body_bytes,
    )

    # Без креденшелов — 401 через настоящую handler-фабрику.
    denied = _invoke(
        captured["handler"],
        path="/v1/fetch",
        headers={"Content-Length": str(len(_fetch_body()))},
        body=_fetch_body(),
    )
    status, _headers, env = _response(denied)
    assert status == 401
    assert env is not None and env["status"] == "error"

    # С субъектом и scope — 200.
    allowed = _invoke(
        captured["handler"],
        path="/v1/fetch",
        headers={
            "Content-Length": str(len(_fetch_body())),
            "X-Client-Cert-Subject": "svc-core",
        },
        body=_fetch_body(),
    )
    status, _headers, env = _response(allowed)
    assert status == 200 and env is not None and env["status"] == "ok"

    events = [
        json.loads(line)
        for line in audit_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [event["status"] for event in events] == ["deny", "allow"]
    assert [event["principal"] for event in events] == [None, "svc-core"]
    assert events[0]["request_id"] == _FIXED_ID


def test_main_passes_constructed_middleware_to_serve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``main`` не только строит, но и передаёт auth/audit/лимит в ``serve``."""
    audit_path = tmp_path / "audit.jsonl"
    config_path = tmp_path / "edge.yaml"
    config_path.write_text(
        "auth:\n"
        "  mode: mtls\n"
        "  subjects:\n"
        "    svc-core: [fetch]\n"
        "audit:\n"
        "  sink: file\n"
        f"  path: {audit_path}\n"
        "max_body_bytes: 2048\n",
        encoding="utf-8",
    )
    captured: dict[str, Any] = {}

    def _fake_serve(host: str, port: int, gateway: Any, **kwargs: Any) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(edge_main, "serve", _fake_serve)
    assert edge_main.main(["serve", "--config", str(config_path)]) == edge_main.EXIT_OK

    assert isinstance(captured["authenticator"], MTLSAuthenticator)
    assert isinstance(captured["audit_sink"], FileAuditSink)
    assert captured["max_body_bytes"] == 2048


def test_main_refuses_non_loopback_without_auth(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "edge.yaml"
    config_path.write_text("host: 0.0.0.0\n", encoding="utf-8")
    assert edge_main.main(["serve", "--config", str(config_path)]) == edge_main.EXIT_USAGE
    err = capsys.readouterr().err
    assert "отказ запуска" in err
    assert "0.0.0.0" in err


def test_main_refuses_non_loopback_without_auth_even_for_check(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "edge.yaml"
    config_path.write_text("host: 0.0.0.0\n", encoding="utf-8")
    assert (
        edge_main.main(["serve", "--config", str(config_path), "--check"])
        == edge_main.EXIT_USAGE
    )
    assert re.search(r"host='0\.0\.0\.0'", capsys.readouterr().err)
