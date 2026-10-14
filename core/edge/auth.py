"""Аутентификация и авторизация шва core↔edge (W13/T13.8; 07 §8.2).

DMZ-сервер (Z1) публикует веб-операции ``/v1/*``; входящий запрос обязан быть
**атрибутирован** к рабочей идентичности (workload-identity) и **авторизован** по
операции. Допускаются два метода (07 §7/§8.2):

* **mTLS** — клиентский сертификат от корпоративного PKI; coHound не делает
  handshake сам, а получает уже проверенный субъект сертификата от доверенного
  терминирующего прокси (инъецированное отображение/заголовок).;
* **OIDC/JWT** — bearer-токен от корпоративного IDP; проверка подписи идёт через
  **инъецируемый шов-верификатор**, из которого берутся claims (``sub``, ``iss``,
  ``aud``, ``exp``, scopes).

Инварианты дизайна:

* **Fail-closed.** Любой сбой/неизвестность → ``AuthDecision(allowed=False)``.
* **Явный allowlist.** :data:`AUTHORIZED_OPS` — единственный набор операций;
  неизвестная операция отвергается. ``health`` — публичная (диагностика 07 §3).
* **Честность верификации.** ``pyjwt`` — опциональная/ленивая зависимость.
  :func:`unverified_claims_from_token` — stdlib-декодер JWT **без проверки
  подписи**, предназначенный **только для тестов**; он возвращает
  ``VerifiedClaims.verified=False``, и :class:`OIDCAuthenticator` такие claims
  **никогда не принимает**. «Подделанный» токен не может быть объявлен
  проверенным только потому, что его payload структурно разобрался.

Модуль **категорийно нейтрален** (§1.3): никаких продуктов/вендоров/предметных
терминов; всё специфичное — данные (scopes, отображения субъектов), задаваемые
оператором при wiring.
"""

from __future__ import annotations

import base64
import binascii
import json
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "AUTH_METHOD_MTLS",
    "AUTH_METHOD_OIDC",
    "SCOPE_FETCH",
    "SCOPE_SEARCH",
    "SCOPE_REGISTRY",
    "SCOPE_CAPABILITIES",
    "SCOPE_HEALTH",
    "SCOPE_CRAWL",
    "AUTHORIZED_OPS",
    "PUBLIC_OPS",
    "AuthRequest",
    "Principal",
    "AuthDecision",
    "VerifiedClaims",
    "AuthError",
    "TokenVerificationError",
    "TokenVerifier",
    "Authenticator",
    "MTLSAuthenticator",
    "OIDCAuthenticator",
    "PyJWTVerifier",
    "unverified_claims_from_token",
    "required_scope",
    "authorize",
    "scopes_from_claims",
]

# ---------------------------------------------------------------------------
# Константы: методы аутентификации и scopes (07 §8.2)
# ---------------------------------------------------------------------------

#: Идентичность подтверждена клиентским сертификатом (workload-identity).
AUTH_METHOD_MTLS = "mtls"
#: Идентичность подтверждена OIDC/JWT bearer-токеном.
AUTH_METHOD_OIDC = "oidc"

#: Scope, требуемый для операции ``fetch``.
SCOPE_FETCH = "fetch"
#: Scope, требуемый для операции ``search``.
SCOPE_SEARCH = "search"
#: Scope, требуемый для операции ``registry``.
SCOPE_REGISTRY = "registry"
#: Scope, требуемый для операции ``capabilities``.
SCOPE_CAPABILITIES = "capabilities"
#: Scope операции ``health`` (по умолчанию операция публична).
SCOPE_HEALTH = "health"
#: Scope, требуемый для всех операций очереди краула ``crawl/{submit,status,cancel,collect}``.
SCOPE_CRAWL = "crawl"

#: Allowlist операций шва (07 §3/§5). Неизвестная операция авторизации не подлежит.
AUTHORIZED_OPS: frozenset[str] = frozenset(
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

#: Публичные операции: доступны без аутентификации (диагностика, 07 §3).
PUBLIC_OPS: frozenset[str] = frozenset({"health"})

#: Соответствие операции требуемому scope (07 §8.2: allowlist identity → операция).
_OP_SCOPE: dict[str, str] = {
    "fetch": SCOPE_FETCH,
    "search": SCOPE_SEARCH,
    "registry": SCOPE_REGISTRY,
    "capabilities": SCOPE_CAPABILITIES,
    "health": SCOPE_HEALTH,
    "crawl/submit": SCOPE_CRAWL,
    "crawl/status": SCOPE_CRAWL,
    "crawl/cancel": SCOPE_CRAWL,
    "crawl/collect": SCOPE_CRAWL,
    "crawl/poll": SCOPE_CRAWL,
}

#: Входящий запрос шва, как его видит аутентификатор: отображение заголовков.
AuthRequest = Mapping[str, Any]

#: Callable-шов верификатора токена: ``token -> VerifiedClaims`` (в тестах — без сети).
TokenVerifier = Callable[[str], "VerifiedClaims"]


# ---------------------------------------------------------------------------
# Ошибки
# ---------------------------------------------------------------------------


class AuthError(RuntimeError):
    """Базовый сбой аутентификации/верификации (fail-closed)."""


class TokenVerificationError(AuthError):
    """Токен не подтверждён верификатором: подпись/структура/параметры."""


# ---------------------------------------------------------------------------
# Модели
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Principal:
    """Подтверждённая рабочая идентичность (workload-identity) вызывающего.

    :param subject: сервисный субъект (mTLS — subject сертификата; OIDC — claim ``sub``).
    :param scopes: множество предоставленных scopes.
    :param issuer: издатель (IDP/CA); ``None``, если не задан.
    :param method: метод подтверждения — :data:`AUTH_METHOD_MTLS` или
        :data:`AUTH_METHOD_OIDC`.
    """

    subject: str
    scopes: frozenset[str] = field(default_factory=frozenset)
    issuer: str | None = None
    method: str = AUTH_METHOD_MTLS

    def __post_init__(self) -> None:
        if not str(self.subject).strip():
            raise ValueError("Principal.subject не может быть пустым")
        # Приводим scopes к frozenset[str] (удобство для вызывающих).
        object.__setattr__(self, "scopes", frozenset(str(s) for s in self.scopes))


@dataclass(frozen=True)
class AuthDecision:
    """Результат аутентификации.

    :param allowed: ``True`` — идентичность подтверждена; ``False`` — отклонено.
    :param principal: подтверждённая идентичность (``None`` при отказе).
    :param reason: человекочитаемая причина (для аудита; без секретов).
    """

    allowed: bool
    principal: Principal | None
    reason: str


@dataclass(frozen=True)
class VerifiedClaims:
    """Claims токена плюс явный признак криптографической проверки.

    :param claims: разобранные claims JWT.
    :param verified: ``True`` — подпись проверена верификатором; ``False`` —
        claims разобраны без проверки (stdlib-fallback, **только тесты**).
    :param reason: пояснение способа верификации (для аудита).
    """

    claims: Mapping[str, Any]
    verified: bool
    reason: str = ""


# ---------------------------------------------------------------------------
# Извлечение данных из запроса
# ---------------------------------------------------------------------------


def _header(request: Any, name: str) -> str | None:
    """Регистронезависимо достаёт заголовок из отображения запроса.

    Возвращает ``None``, если запрос не отображение или заголовок отсутствует.
    """
    if not isinstance(request, Mapping):
        return None
    if name in request:
        value = request[name]
        return None if value is None else str(value)
    lowered = name.lower()
    for key, value in request.items():
        if isinstance(key, str) and key.lower() == lowered and value is not None:
            return str(value)
    return None


def _extract_bearer(raw: str | None) -> str | None:
    """Извлекает токен из заголовка ``Authorization: Bearer <token>``.

    Строгая схема: без ``Bearer`` запрос считается неаутентифицированным.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    scheme, separator, token = text.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def scopes_from_claims(claims: Mapping[str, Any]) -> frozenset[str]:
    """Собирает scopes из claims OIDC.

    Поддерживаются ``scope`` (строка, разделённая пробелами) и ``scp``/``scopes``
    (строка или список) — разные IDP-конвенции. Результат — ``frozenset[str]``.
    """
    result: set[str] = set()
    if not isinstance(claims, Mapping):
        return frozenset()
    for key in ("scope", "scp", "scopes"):
        if key not in claims:
            continue
        raw = claims[key]
        if isinstance(raw, str):
            result.update(part for part in raw.split() if part)
        elif isinstance(raw, Iterable):
            result.update(str(item) for item in raw if str(item).strip())
    return frozenset(result)


def _audience_matches(aud: Any, expected: str) -> bool:
    """``aud`` может быть строкой или списком строк (RFC 7519)."""
    if isinstance(aud, str):
        return aud == expected
    if isinstance(aud, Iterable):
        return any(str(item) == expected for item in aud)
    return False


def _exp_valid(exp: Any, now: float) -> bool:
    """``exp`` обязателен и должен быть в будущем относительно ``now``."""
    if exp is None or isinstance(exp, bool):
        return False
    try:
        return float(exp) > float(now)
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Протокол аутентификатора
# ---------------------------------------------------------------------------


@runtime_checkable
class Authenticator(Protocol):
    """Контракт аутентификатора входящего запроса (07 §8.2)."""

    def authenticate(self, request: AuthRequest) -> AuthDecision: ...


# ---------------------------------------------------------------------------
# mTLS
# ---------------------------------------------------------------------------


class MTLSAuthenticator:
    """Аутентификация по клиентскому сертификату (workload-identity, 07 §8.2).

    Реальный TLS-handshake завершается на доверенном терминирующем прокси
    (LB/TLS-inspection, 07 §8.1); сюда приходит уже проверенный субъект
    сертификата через инъецированное отображение или заголовок. **Никакого
    pinning** здесь нет — доверие к корп. CA настраивается на транспорте.

    :param subject_scopes: отображение ``subject сертификата -> итерабл scopes``
        (реестр рабочих идентичностей). Неизвестный субъект отвергается.
    :param header: имя заголовка с субъектом (по умолчанию
        ``X-Client-Cert-Subject``).
    :param issuer: издатель (CA) для атрибуции; ``None``, если не задан.
    """

    def __init__(
        self,
        subject_scopes: Mapping[str, Iterable[str]],
        *,
        header: str = "X-Client-Cert-Subject",
        issuer: str | None = None,
    ) -> None:
        self._subject_scopes = {
            str(subject): frozenset(str(scope) for scope in scopes)
            for subject, scopes in dict(subject_scopes).items()
        }
        self.header = str(header)
        self.issuer = issuer

    def authenticate(self, request: AuthRequest) -> AuthDecision:
        """Подтверждает идентичность по субъекту сертификата из доверенного заголовка."""
        subject = _header(request, self.header)
        if subject is None or not subject.strip():
            return AuthDecision(
                False, None, "mTLS: отсутствует субъект клиентского сертификата"
            )
        subject = subject.strip()
        scopes = self._subject_scopes.get(subject)
        if scopes is None:
            return AuthDecision(
                False, None, f"mTLS: субъект {subject!r} не зарегистрирован"
            )
        principal = Principal(
            subject=subject, scopes=scopes, issuer=self.issuer, method=AUTH_METHOD_MTLS
        )
        return AuthDecision(True, principal, "mTLS: идентичность подтверждена")


# ---------------------------------------------------------------------------
# OIDC / JWT
# ---------------------------------------------------------------------------


class PyJWTVerifier:
    """Верификатор JWT поверх ``pyjwt`` (ленивый импорт; fail-closed).

    ``pyjwt`` — **необязательная** зависимость: при её отсутствии верификатор
    бросает :class:`TokenVerificationError` (никакого тихого принятия
    неподписанного токена). Ключ/алгоритмы задаются оператором при wiring.

    :param key: ключ проверки подписи (публичный ключ IDP / секрет HS*).
    :param algorithms: допустимые алгоритмы (``["RS256"]`` и т.п.).
    :param audience: ожидаемый ``aud`` (делегируется ``pyjwt``).
    :param issuer: ожидаемый ``iss`` (делегируется ``pyjwt``).
    :param leeway: допуск по времени, секунды.
    :param options: переопределение опций ``pyjwt.decode``.
    """

    def __init__(
        self,
        *,
        key: Any = None,
        algorithms: list[str] | None = None,
        audience: str | None = None,
        issuer: str | None = None,
        leeway: float = 0.0,
        options: Mapping[str, Any] | None = None,
    ) -> None:
        self._key = key
        self._algorithms = algorithms
        self._audience = audience
        self._issuer = issuer
        self._leeway = leeway
        self._options = dict(options) if options is not None else None

    def __call__(self, token: str) -> VerifiedClaims:
        """Проверяет подпись токена; при ``pyjwt`` отсутствии — отказ."""
        try:
            import jwt  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - зависит от окружения
            raise TokenVerificationError(
                "pyjwt не установлен: подпись OIDC-токена не может быть проверена "
                "(fail-closed)"
            ) from exc
        try:
            claims = jwt.decode(
                token,
                key=self._key,
                algorithms=self._algorithms,
                audience=self._audience,
                issuer=self._issuer,
                leeway=self._leeway,
                options=self._options,
            )
        except Exception as exc:  # noqa: BLE001 - унификация ошибок верификации
            raise TokenVerificationError(f"OIDC: токен отклонён верификатором: {exc}") from exc
        if not isinstance(claims, Mapping):  # pragma: no cover - защитно
            raise TokenVerificationError("OIDC: верификатор вернул не-объект claims")
        return VerifiedClaims(
            claims=dict(claims), verified=True, reason="pyjwt: подпись проверена"
        )


def unverified_claims_from_token(token: str) -> VerifiedClaims:
    """stdlib-декодер JWT **без проверки подписи** — ТОЛЬКО для тестов.

    Разбирает payload (base64url) и возвращает claims с ``verified=False``.
    Предназначен исключительно для тестового шва: :class:`OIDCAuthenticator`
    **никогда не принимает** такие claims, поэтому подделанный токен не может
    быть объявлен проверенным лишь из-за структурной разборности.
    """
    parts = str(token).split(".")
    if len(parts) != 3:
        raise TokenVerificationError(
            "JWT должен состоять из трёх сегментов (header.payload.signature)"
        )
    payload = parts[1]
    padded = payload + "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("ascii"))
        claims = json.loads(decoded.decode("utf-8"))
    except (binascii.Error, ValueError, UnicodeDecodeError) as exc:
        raise TokenVerificationError(f"JWT payload не декодируется: {exc}") from exc
    if not isinstance(claims, Mapping):
        raise TokenVerificationError("JWT payload должен быть JSON-объектом")
    return VerifiedClaims(
        claims=dict(claims),
        verified=False,
        reason="stdlib: подпись не проверялась (unverified decoder)",
    )


class OIDCAuthenticator:
    """Аутентификация по OIDC/JWT bearer-токену (07 §8.2).

    Верификация подписи делегируется **инъецируемому** шву ``verifier`` (по
    умолчанию — :class:`PyJWTVerifier`). Аутентификатор дополнительно проверяет
    claims: ``iss``, ``aud``, ``exp`` и наличие субъекта (``sub``), а scopes
    берутся из claims. Claims с ``verified=False`` (stdlib-fallback) всегда
    отвергаются.

    :param verifier: callable ``token -> VerifiedClaims``; ``None`` → pyjwt.
    :param issuer: ожидаемый ``iss`` (обязателен — проверяется всегда).
    :param audience: ожидаемый ``aud`` (обязателен — проверяется всегда).
    :param header: заголовок с bearer-токеном (``Authorization``).
    :param now: часы epoch-секунд для проверки ``exp`` (внедряемые в тестах).
    """

    def __init__(
        self,
        *,
        verifier: TokenVerifier | None = None,
        issuer: str | None = None,
        audience: str | None = None,
        header: str = "Authorization",
        now: Callable[[], float] | None = None,
    ) -> None:
        self._verifier: TokenVerifier = verifier or PyJWTVerifier(
            issuer=issuer, audience=audience
        )
        self.issuer = issuer
        self.audience = audience
        self.header = str(header)
        self._now = now or time.time

    def authenticate(self, request: AuthRequest) -> AuthDecision:
        """Проверяет bearer-токен и собирает :class:`Principal` из claims."""
        token = _extract_bearer(_header(request, self.header))
        if token is None:
            return AuthDecision(False, None, "OIDC: отсутствует bearer-токен")

        try:
            result = self._verifier(token)
        except TokenVerificationError as exc:
            return AuthDecision(False, None, f"OIDC: {exc}")
        except Exception as exc:  # noqa: BLE001 - fail-closed на любой сбой верификатора
            return AuthDecision(False, None, f"OIDC: сбой верификатора: {exc}")

        if not result.verified:
            return AuthDecision(
                False,
                None,
                "OIDC: токен не подтверждён криптографически "
                f"({result.reason or 'unverified'})",
            )

        claims = result.claims
        iss = claims.get("iss")
        if self.issuer is not None and iss != self.issuer:
            return AuthDecision(
                False, None, f"OIDC: несовпадение issuer {iss!r} != {self.issuer!r}"
            )
        if self.audience is not None and not _audience_matches(
            claims.get("aud"), self.audience
        ):
            return AuthDecision(False, None, "OIDC: несовпадение audience")
        if not _exp_valid(claims.get("exp"), self._now()):
            return AuthDecision(False, None, "OIDC: токен истёк или отсутствует exp")

        subject = claims.get("sub")
        if subject is None or not str(subject).strip():
            return AuthDecision(False, None, "OIDC: отсутствует субъект (sub)")

        principal = Principal(
            subject=str(subject),
            scopes=scopes_from_claims(claims),
            issuer=str(iss) if iss is not None else None,
            method=AUTH_METHOD_OIDC,
        )
        return AuthDecision(True, principal, "OIDC: токен подтверждён")


# ---------------------------------------------------------------------------
# Авторизация по операции (allowlist, 07 §8.2)
# ---------------------------------------------------------------------------


def required_scope(op: str) -> str | None:
    """Scope, требуемый для операции; ``None`` для неизвестной операции.

    Публичные операции возвращают свой декларативный scope, но не требуют его
    наличия у принципала (см. :func:`authorize`).
    """
    return _OP_SCOPE.get(str(op).strip().lower())


def authorize(principal: Principal | None, op: str) -> bool:
    """Проверяет право принципала на операцию (явный allowlist).

    * неизвестная операция → отказ;
    * публичная операция (:data:`PUBLIC_OPS`) → разрешено без принципала;
    * иначе принципал обязателен и должен нести требуемый scope.
    """
    key = str(op).strip().lower()
    if key not in AUTHORIZED_OPS:
        return False
    if key in PUBLIC_OPS:
        return True
    if principal is None:
        return False
    required = _OP_SCOPE.get(key)
    if required is None:  # pragma: no cover - AUTHORIZED_OPS и _OP_SCOPE синхронны
        return False
    return required in principal.scopes
