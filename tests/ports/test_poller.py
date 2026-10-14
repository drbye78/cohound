"""Тесты ``PollPort`` (Phase 5.1): конформность порта и матрица ``validate_params``.

``PollPort`` — ``@runtime_checkable``: оба поллера (``LocalFeedPoller``/``RemoteFeedPoller``)
структурно удовлетворяют контракту. ``validate_params`` отвергает неизвестный ``kind``,
плохой ``limit``/``since`` и инлайн-секреты (секреты — только через ``token_env``).
Сети нет: краулер/шов подменяются заглушками. Нейтральные фикстуры (§1.3).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from core.adapters.feed_poller import LocalFeedPoller, RemoteFeedPoller
from core.ports.poller import PollPort
from core.ports.types import FetchPlan, RawPage

JSON_DOC = b"[]"


class _FakeCrawler:
    """Минимальный ``CrawlerPort``: отдаёт заданные байты; журналирует URL."""

    version = "fake-crawler-1"

    def __init__(self, content: bytes = JSON_DOC) -> None:
        self._content = content
        self.calls: list[str] = []

    def fetch(self, url: str, plan: FetchPlan = FetchPlan()) -> RawPage:
        self.calls.append(url)
        return RawPage(url=url, content=self._content, fetched_at="2026-01-01T00:00:00Z")


class _FakeGateway:
    """Минимальный ``ContentGateway``: ``call(op, payload) -> payload`` + журнал."""

    schema_version = "1"

    def __init__(self, response: Any = None) -> None:
        self._response = (
            response
            if response is not None
            else {"entries": [], "count": 0, "truncated": False}
        )
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def call(self, op: str, payload: Mapping[str, Any]) -> Any:
        self.calls.append((op, dict(payload)))
        return self._response

    def capabilities(self) -> Mapping[str, Any]:  # pragma: no cover - не используется
        return {}


# ---------------------------------------------------------------------------
# Конформность порта
# ---------------------------------------------------------------------------


def test_local_feed_poller_satisfies_port() -> None:
    poller = LocalFeedPoller(_FakeCrawler())
    assert isinstance(poller, PollPort)
    assert isinstance(poller.version, str) and poller.version


def test_remote_feed_poller_satisfies_port() -> None:
    poller = RemoteFeedPoller(_FakeGateway())
    assert isinstance(poller, PollPort)
    assert isinstance(poller.version, str) and poller.version


# ---------------------------------------------------------------------------
# validate_params (обе реализации)
# ---------------------------------------------------------------------------


@pytest.fixture(params=["local", "remote"])
def poller(request: pytest.FixtureRequest) -> PollPort:
    """Обе реализации ``PollPort`` — одинаковая валидация ``kind``/``params``."""
    if request.param == "local":
        return LocalFeedPoller(_FakeCrawler())
    return RemoteFeedPoller(_FakeGateway())


def test_validate_params_ok(poller: PollPort) -> None:
    assert poller.validate_params("feed", {}) == []
    assert poller.validate_params("channel", {"limit": 3, "since": "2026-01-01T00:00:00Z"}) == []


def test_validate_params_allows_token_env(poller: PollPort) -> None:
    """``token_env`` — легитимный канал секрета (env/deploy), не инлайн."""
    assert poller.validate_params("channel", {"token_env": "MY_CHANNEL_TOKEN"}) == []


def test_validate_params_unknown_kind(poller: PollPort) -> None:
    messages = poller.validate_params("sitemap", {})
    assert messages
    assert any("kind" in message for message in messages)


@pytest.mark.parametrize("bad_limit", [0, -1, "5", 1.5, True, None])
def test_validate_params_bad_limit(poller: PollPort, bad_limit: Any) -> None:
    if bad_limit is None:
        # ``limit=None`` задан явно — это не «плохой тип», а отсутствие ограничения.
        assert poller.validate_params("feed", {"limit": None}) == []
        return
    messages = poller.validate_params("feed", {"limit": bad_limit})
    assert any("limit" in message for message in messages)


@pytest.mark.parametrize("bad_since", ["not-a-date", 20260101, ""])
def test_validate_params_bad_since(poller: PollPort, bad_since: Any) -> None:
    messages = poller.validate_params("feed", {"since": bad_since})
    assert any("since" in message for message in messages)


@pytest.mark.parametrize(
    "bad_since",
    ["2026-01-01T00:00:00Z", "2026-01-01", "Tue, 06 Oct 2026 10:00:00 +0000"],
)
def test_validate_params_good_since(poller: PollPort, bad_since: str) -> None:
    assert poller.validate_params("feed", {"since": bad_since}) == []


@pytest.mark.parametrize(
    "secret_key",
    ["token", "api_key", "apikey", "authorization", "secret", "password", "access_token"],
)
def test_validate_params_rejects_inline_secret(poller: PollPort, secret_key: str) -> None:
    messages = poller.validate_params("channel", {secret_key: "value"})
    assert messages
    assert any("token_env" in message for message in messages)


def test_validate_params_lenient_to_unknown_non_secret_key(poller: PollPort) -> None:
    """Незнакомый не-секретный ключ не отвергается (forward-compat параметров)."""
    assert poller.validate_params("feed", {"max_items": 10}) == []
