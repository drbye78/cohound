"""Тесты аутентификации/авторизации шва — ``core/edge/auth.py`` (W13/T13.8; 07 §8.2).

Сеть/реальный TLS-handshake отсутствуют: mTLS-субъект приходит доверенным
заголовком, OIDC-верификатор инъецируется. Ключевая проверка честности —
stdlib-декодер JWT **не** делает токен проверенным: подделанный токен с
корректными на вид claims отвергается. Фикстуры категорийно нейтральны (§1.3).
"""

from __future__ import annotations

import base64
import json
import re
from collections.abc import Mapping
from pathlib import Path

import pytest

import core.edge.auth as auth_module
from core.edge.auth import (
    AUTH_METHOD_MTLS,
    AUTH_METHOD_OIDC,
    AUTHORIZED_OPS,
    PUBLIC_OPS,
    SCOPE_CAPABILITIES,
    SCOPE_CRAWL,
    SCOPE_FETCH,
    SCOPE_REGISTRY,
    SCOPE_SEARCH,
    AuthDecision,
    Authenticator,
    MTLSAuthenticator,
    OIDCAuthenticator,
    Principal,
    TokenVerificationError,
    VerifiedClaims,
    authorize,
    required_scope,
    scopes_from_claims,
    unverified_claims_from_token,
)

_ISSUER = "https://idp.test/realms/example"
_AUDIENCE = "cohound-edge"
_NOW = 1_000_000.0

_SUBJECTS = {
    "svc-core": ["fetch", "search", "registry", "capabilities"],
    "svc-readonly": ["fetch"],
}


def _clock() -> float:
    """Детерминированные часы (epoch-секунды) для проверки ``exp``."""
    return _NOW


def _b64url(data: Mapping[str, object]) -> str:
    """Кодирует JSON-объект в base64url без padding (сегмент JWT)."""
    raw = json.dumps(dict(data), separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _make_token(claims: Mapping[str, object]) -> str:
    """Собирает JWT с корректными на вид claims и заведомо подделанной подписью."""
    header = {"alg": "none", "typ": "JWT"}
    return f"{_b64url(header)}.{_b64url(claims)}.forged-signature"


def _valid_claims() -> dict[str, object]:
    """Корректные claims (iss/aud/exp/sub/scope)."""
    return {
        "iss": _ISSUER,
        "aud": _AUDIENCE,
        "exp": _NOW + 3_600,
        "sub": "svc-core",
        "scope": "fetch search registry capabilities",
    }


def _verified_verifier(claims):
    """Инъецируемый верификатор: объявляет claims проверенными.

    Имитирует успешную проверку подписи IDP — используется только для тестов
    accept-пути; реальная криптография здесь не выполняется.
    """

    def verify(_token: str) -> VerifiedClaims:
        return VerifiedClaims(claims=claims, verified=True, reason="test: verified")

    return verify


def _mtls() -> MTLSAuthenticator:
    """mTLS-аутентификатор с детерминированным реестром идентичностей."""
    return MTLSAuthenticator(_SUBJECTS, issuer="corp-ca")


# ---------------------------------------------------------------------------
# mTLS
# ---------------------------------------------------------------------------


def test_mtls_known_subject_maps_to_scopes() -> None:
    decision = _mtls().authenticate({"X-Client-Cert-Subject": "svc-core"})
    assert decision.allowed is True
    assert decision.principal is not None
    assert decision.principal.subject == "svc-core"
    assert decision.principal.method == AUTH_METHOD_MTLS
    assert decision.principal.issuer == "corp-ca"
    assert decision.principal.scopes == frozenset(
        {"fetch", "search", "registry", "capabilities"}
    )


def test_mtls_unknown_subject_denied() -> None:
    decision = _mtls().authenticate({"X-Client-Cert-Subject": "svc-unknown"})
    assert decision.allowed is False
    assert decision.principal is None
    assert "не зарегистрирован" in decision.reason


def test_mtls_missing_subject_denied() -> None:
    decision = _mtls().authenticate({})
    assert decision.allowed is False
    assert decision.principal is None


def test_mtls_header_case_insensitive() -> None:
    """HTTP-заголовки регистронезависимы."""
    decision = _mtls().authenticate({"x-client-cert-subject": "svc-readonly"})
    assert decision.allowed is True
    assert decision.principal is not None
    assert decision.principal.scopes == frozenset({"fetch"})


def test_mtls_non_mapping_request_denied() -> None:
    decision = _mtls().authenticate(None)  # type: ignore[arg-type]
    assert decision.allowed is False


# ---------------------------------------------------------------------------
# OIDC / JWT
# ---------------------------------------------------------------------------


def _oidc(claims=None, **kwargs) -> OIDCAuthenticator:
    """OIDC-аутентификатор с инъецированным верификатором и часами."""
    verifier = _verified_verifier(claims if claims is not None else _valid_claims())
    return OIDCAuthenticator(
        verifier=verifier,
        issuer=_ISSUER,
        audience=_AUDIENCE,
        now=_clock,
        **kwargs,
    )


def _bearer(claims: Mapping[str, object] | None = None) -> dict[str, str]:
    """Заголовки запроса с bearer-токеном."""
    return {"Authorization": f"Bearer {_make_token(claims or _valid_claims())}"}


def test_oidc_accept_with_injected_verifier() -> None:
    decision = _oidc().authenticate(_bearer())
    assert decision.allowed is True
    assert decision.principal is not None
    assert decision.principal.subject == "svc-core"
    assert decision.principal.method == AUTH_METHOD_OIDC
    assert decision.principal.issuer == _ISSUER
    assert decision.principal.scopes == frozenset(
        {"fetch", "search", "registry", "capabilities"}
    )


def test_oidc_rejects_bad_issuer() -> None:
    claims = {**_valid_claims(), "iss": "https://evil.test"}
    decision = _oidc(claims).authenticate(_bearer(claims))
    assert decision.allowed is False
    assert "issuer" in decision.reason


def test_oidc_rejects_bad_audience() -> None:
    claims = {**_valid_claims(), "aud": "other-service"}
    decision = _oidc(claims).authenticate(_bearer(claims))
    assert decision.allowed is False
    assert "audience" in decision.reason


def test_oidc_accepts_audience_list() -> None:
    claims = {**_valid_claims(), "aud": ["other", _AUDIENCE]}
    decision = _oidc(claims).authenticate(_bearer(claims))
    assert decision.allowed is True


def test_oidc_rejects_expired() -> None:
    claims = {**_valid_claims(), "exp": _NOW - 1}
    decision = _oidc(claims).authenticate(_bearer(claims))
    assert decision.allowed is False
    assert "exp" in decision.reason


def test_oidc_rejects_missing_exp() -> None:
    claims = {key: value for key, value in _valid_claims().items() if key != "exp"}
    decision = _oidc(claims).authenticate(_bearer(claims))
    assert decision.allowed is False


def test_oidc_rejects_missing_subject() -> None:
    claims = {key: value for key, value in _valid_claims().items() if key != "sub"}
    decision = _oidc(claims).authenticate(_bearer(claims))
    assert decision.allowed is False
    assert "sub" in decision.reason


def test_oidc_unauthenticated_request_rejected() -> None:
    decision = _oidc().authenticate({})
    assert decision.allowed is False
    assert "bearer" in decision.reason


def test_oidc_rejects_non_bearer_scheme() -> None:
    token = _make_token(_valid_claims())
    decision = _oidc().authenticate({"Authorization": f"Basic {token}"})
    assert decision.allowed is False


def test_oidc_verifier_failure_is_denied() -> None:
    """Ошибка верификатора не пропускает запрос (fail-closed)."""

    def broken(_token: str) -> VerifiedClaims:
        raise TokenVerificationError("подпись недействительна")

    auth = OIDCAuthenticator(
        verifier=broken, issuer=_ISSUER, audience=_AUDIENCE, now=_clock
    )
    decision = auth.authenticate(_bearer())
    assert decision.allowed is False
    assert "подпись недействительна" in decision.reason


def test_stdlib_fallback_marks_claims_unverified() -> None:
    """stdlib-декодер разбирает payload, но честно помечает его непроверенным."""
    result = unverified_claims_from_token(_make_token(_valid_claims()))
    assert result.verified is False
    assert result.claims["sub"] == "svc-core"
    assert "unverified" in result.reason or "не проверял" in result.reason


def test_stdlib_fallback_never_verifies_forged_token() -> None:
    """Подделанный токен с корректными на вид claims НЕ считается проверенным."""
    token = _make_token(_valid_claims())
    auth = OIDCAuthenticator(
        verifier=unverified_claims_from_token,
        issuer=_ISSUER,
        audience=_AUDIENCE,
        now=_clock,
    )
    decision = auth.authenticate({"Authorization": f"Bearer {token}"})
    assert decision.allowed is False
    assert decision.principal is None
    assert "не подтверждён" in decision.reason


# ---------------------------------------------------------------------------
# Авторизация по операции
# ---------------------------------------------------------------------------


def _principal(scopes: set[str]) -> Principal:
    return Principal(subject="svc", scopes=frozenset(scopes), method=AUTH_METHOD_MTLS)


def test_authorize_op_scope_matrix() -> None:
    assert authorize(_principal({SCOPE_FETCH}), "fetch") is True
    assert authorize(_principal({SCOPE_SEARCH}), "search") is True
    assert authorize(_principal({SCOPE_REGISTRY}), "registry") is True
    assert authorize(_principal({SCOPE_CAPABILITIES}), "capabilities") is True
    # Чужой scope не даёт прав.
    assert authorize(_principal({SCOPE_FETCH}), "search") is False
    assert authorize(_principal(set()), "fetch") is False


def test_authorize_crawl_ops_require_crawl_scope() -> None:
    """Все crawl-операции покрыты единым scope ``crawl`` (Phase 3.1; Wave 5.1 poll)."""
    for op in ("crawl/submit", "crawl/status", "crawl/cancel", "crawl/collect", "crawl/poll"):
        assert authorize(_principal({SCOPE_CRAWL}), op) is True
        assert authorize(_principal({SCOPE_FETCH}), op) is False
        assert authorize(None, op) is False


def test_authorize_health_is_public() -> None:
    """``health`` — публичная диагностика: доступна без принципала."""
    assert authorize(None, "health") is True
    assert authorize(_principal(set()), "health") is True


def test_authorize_requires_principal_for_non_public() -> None:
    assert authorize(None, "fetch") is False
    assert authorize(None, "capabilities") is False


def test_authorize_unknown_op_denied() -> None:
    assert authorize(_principal({SCOPE_FETCH}), "delete") is False
    assert authorize(_principal({SCOPE_FETCH}), "") is False


def test_authorized_ops_allowlist_exact() -> None:
    assert AUTHORIZED_OPS == frozenset(
        {
            "fetch",
            "search",
            "registry",
            "capabilities",
            "health",
            "crawl/submit",
            "crawl/status",
            "crawl/cancel",
            "crawl/collect",
            "crawl/poll",
        }
    )
    assert PUBLIC_OPS == frozenset({"health"})


def test_required_scope_lookup() -> None:
    assert required_scope("fetch") == SCOPE_FETCH
    assert required_scope("Search") == SCOPE_SEARCH
    assert required_scope("crawl/submit") == SCOPE_CRAWL
    assert required_scope("crawl/poll") == SCOPE_CRAWL
    assert required_scope("nope") is None


def test_scopes_from_claims_variants() -> None:
    assert scopes_from_claims({"scope": "a b"}) == frozenset({"a", "b"})
    assert scopes_from_claims({"scp": ["x", "y"]}) == frozenset({"x", "y"})
    assert scopes_from_claims({"scopes": "m"}) == frozenset({"m"})
    assert scopes_from_claims({}) == frozenset()


def test_principal_rejects_empty_subject() -> None:
    with pytest.raises(ValueError):
        Principal(subject="  ")


def test_authenticator_protocol_runtime_checkable() -> None:
    assert isinstance(_mtls(), Authenticator)
    assert isinstance(_oidc(), Authenticator)


def test_auth_decision_shape() -> None:
    decision = AuthDecision(allowed=False, principal=None, reason="r")
    assert decision.allowed is False and decision.principal is None


# ---------------------------------------------------------------------------
# Нейтральность (§1.3)
# ---------------------------------------------------------------------------

_FORBIDDEN = re.compile(
    r"postgres|pangolin|sber|сбер|copywala|платформа", re.IGNORECASE
)


def test_source_is_neutral() -> None:
    assert auth_module.__file__ is not None
    source = Path(auth_module.__file__).read_text(encoding="utf-8")
    assert not _FORBIDDEN.search(source)
