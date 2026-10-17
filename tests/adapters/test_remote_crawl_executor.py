"""Тесты ``EdgeCrawlExecutor`` — remote-исполнитель краула (Phase 3.3; 07 §4).

Сети нет: внедряется фейковый ``ContentGateway``, возвращающий payload'ы ``crawl/*``.
Проверяются: формы запросов (op + payload), разбор ответов, декодирование base64,
повторный хеш (I19 — хэш сверяется/пересчитывается, edge не доверяем), пагинация
``next_cursor``/``done`` и защита от раздутых батчей. Нейтральные фикстуры (§1.3).
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from core.adapters.remote_crawl_executor import (
    ACTIVE_JOB_STATUSES,
    DEFAULT_MAX_BATCH_ITEMS,
    EdgeCrawlExecutor,
)
from core.adapters.remote_gateway import RemoteContentError
from core.domain.enums import CrawlStrategy, ExtractionRoute, FetchMode
from core.domain.models import RateLimitSpec
from core.ports.crawl_executor import (
    CrawlBatch,
    CrawlExecutorPort,
    CrawlProgress,
    CrawlResultItem,
    CrawlSubmitResult,
    CrawlWorkItem,
)
from core.util.hashing import content_hash

URL_A = "https://docs.test/p1/a"
URL_B = "https://docs.test/p1/b"


class _FakeGateway:
    """Минимальный ``ContentGateway``: ``call(op, payload) -> payload`` + журнал вызовов."""

    schema_version = "1"

    def __init__(self, responses: Mapping[str, Any] | None = None) -> None:
        self._responses = dict(responses or {})
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def call(self, op: str, payload: Mapping[str, Any]) -> Any:
        self.calls.append((op, dict(payload)))
        response = self._responses.get(op)
        if callable(response):
            return response(payload)
        if response is None:
            raise AssertionError(f"нет заготовленного ответа для op={op!r}")
        return response

    def capabilities(self) -> Mapping[str, Any]:  # pragma: no cover - не используется
        return {}


def _work_item(**overrides: Any) -> CrawlWorkItem:
    data: dict[str, Any] = {"product_id": "p1", "url": URL_A}
    data.update(overrides)
    return CrawlWorkItem(**data)


def _raw(b64: str, *, claimed: str | None) -> dict[str, Any]:
    raw: dict[str, Any] = {"bytes_b64": b64, "size": len(base64.b64decode(b64))}
    if claimed is not None:
        raw["hash"] = claimed
    return raw


def _item(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "product_id": "p1",
        "url": URL_A,
        "status": "fetched",
        "engine": "scrapy",
        "kind": "web",
    }
    data.update(overrides)
    return data


# ---------------------------------------------------------------------------
# Контракт порта
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# submit
# ---------------------------------------------------------------------------


def test_submit_builds_payload_and_returns_job_id() -> None:
    gateway = _FakeGateway({"crawl/submit": {"job_id": "job-1", "accepted": True, "total": 1}})
    executor = EdgeCrawlExecutor(gateway, profile="split")

    submission = executor.submit(
        [
            _work_item(
                engine="scrapy",
                kind="web",
                fetch=FetchMode.BROWSER,
                extract=ExtractionRoute.HTML,
                strategy=CrawlStrategy.HYBRID,
                wait_for="selector",
                lang="ru",
            )
        ],
        session_id="sess-1",
        run_id="run-1",
        profile="split",
        idempotency_key="idem-1",
    )

    assert isinstance(submission, CrawlSubmitResult)
    assert submission.job_id == "job-1"
    assert submission.accepted is True
    assert submission.retry_after_s is None
    op, payload = gateway.calls[-1]
    assert op == "crawl/submit"
    assert payload["run_id"] == "run-1"
    assert payload["session_id"] == "sess-1"
    assert payload["idempotency_key"] == "idem-1"
    assert payload["profile"] == "split"
    assert payload["items"] == [
        {
            "product_id": "p1",
            "url": URL_A,
            "engine": "scrapy",
            "kind": "web",
            "fetch": "browser",
            "extract": "html",
            "strategy": "hybrid",
            "wait_for": "selector",
            "lang": "ru",
            "robots": False,
            "rate_limit": None,
        }
    ]


def test_submit_defaults_axes_when_unset() -> None:
    gateway = _FakeGateway({"crawl/submit": {"job_id": "job-2"}})
    executor = EdgeCrawlExecutor(gateway)

    executor.submit(
        [_work_item()],
        session_id="s",
        run_id="r",
        profile="split",
        idempotency_key="k",
    )

    item = gateway.calls[-1][1]["items"][0]
    assert item["fetch"] == "http"
    assert item["extract"] == "auto"
    assert item["strategy"] == "auto"
    assert item["engine"] is None and item["kind"] is None
    assert item["robots"] is False
    assert item["rate_limit"] is None


def test_submit_emits_robots_and_structured_rate_limit() -> None:
    """§6b/§11.1: ``robots`` — bool, ``rate_limit`` — структурные числа ``{min_s,max_s}``."""
    gateway = _FakeGateway({"crawl/submit": {"job_id": "job-rl"}})
    executor = EdgeCrawlExecutor(gateway)

    executor.submit(
        [_work_item(robots=True, rate_limit=RateLimitSpec(min_s=0.5, max_s=1.5))],
        session_id="s",
        run_id="r",
        profile="split",
        idempotency_key="k",
    )

    item = gateway.calls[-1][1]["items"][0]
    assert item["robots"] is True
    assert item["rate_limit"] == {"min_s": 0.5, "max_s": 1.5}
    # именно структурные числа, а не строка canonical_rate_limit
    assert all(isinstance(v, float) for v in item["rate_limit"].values())


def test_submit_rate_limit_none_is_null() -> None:
    """§11.6: ``rate_limit=None`` сериализуется как ``null`` (без пейсинга)."""
    gateway = _FakeGateway({"crawl/submit": {"job_id": "job-none"}})
    executor = EdgeCrawlExecutor(gateway)

    executor.submit(
        [_work_item(robots=False, rate_limit=None)],
        session_id="s",
        run_id="r",
        profile="split",
        idempotency_key="k",
    )

    item = gateway.calls[-1][1]["items"][0]
    assert item["rate_limit"] is None


def _submit_request_schema() -> dict[str, Any]:
    """Опубликованная схема ``#/$defs/submit_request`` для jsonschema-валидации.

    Собирает самодостаточный документ из опубликованного ``crawl.schema.json``
    (draft 2020-12), чтобы валидировать именно публикуемую форму запроса.
    """
    root = Path(__file__).resolve().parents[2]
    doc = json.loads(
        (root / "contracts" / "seam" / "crawl.schema.json").read_text(encoding="utf-8")
    )
    return {
        "$schema": doc["$schema"],
        "$defs": doc["$defs"],
        "$ref": "#/$defs/submit_request",
    }


def test_submit_emits_user_agent_when_set() -> None:
    """§6b: ``user_agent`` — request-level поле, схемо-валидное, вне элемента items."""
    jsonschema = pytest.importorskip("jsonschema")
    gateway = _FakeGateway({"crawl/submit": {"job_id": "job-ua"}})
    executor = EdgeCrawlExecutor(gateway, user_agent="ua/9")

    executor.submit(
        [_work_item()], session_id="s", run_id="r", profile="split", idempotency_key="k"
    )

    payload = gateway.calls[-1][1]
    assert payload["user_agent"] == "ua/9"
    assert "user_agent" not in payload["items"][0]  # request-level, не per-item
    # порядок: рядом с прочими request-level полями, до items
    assert list(payload) == [
        "run_id",
        "session_id",
        "idempotency_key",
        "profile",
        "user_agent",
        "items",
    ]
    jsonschema.Draft202012Validator(_submit_request_schema()).validate(payload)


def test_submit_omits_user_agent_when_unset() -> None:
    """§6b: без ``user_agent`` ключ отсутствует (не ``null``) и payload схемо-валиден."""
    jsonschema = pytest.importorskip("jsonschema")
    gateway = _FakeGateway({"crawl/submit": {"job_id": "job-no-ua"}})
    executor = EdgeCrawlExecutor(gateway)  # user_agent не задан

    executor.submit(
        [_work_item()], session_id="s", run_id="r", profile="split", idempotency_key="k"
    )

    payload = gateway.calls[-1][1]
    assert "user_agent" not in payload
    jsonschema.Draft202012Validator(_submit_request_schema()).validate(payload)


def test_submit_omits_empty_user_agent() -> None:
    """§6b: пустая строка ⇒ ключ не отправляется (схема объявляет строку, не ``null``)."""
    gateway = _FakeGateway({"crawl/submit": {"job_id": "job-empty"}})
    executor = EdgeCrawlExecutor(gateway, user_agent="")

    executor.submit(
        [_work_item()], session_id="s", run_id="r", profile="split", idempotency_key="k"
    )

    assert "user_agent" not in gateway.calls[-1][1]


def test_submit_rejects_empty_work() -> None:
    executor = EdgeCrawlExecutor(_FakeGateway())
    with pytest.raises(RemoteContentError):
        executor.submit(
            [], session_id="s", run_id="r", profile="split", idempotency_key="k"
        )


def test_submit_missing_job_id_raises() -> None:
    executor = EdgeCrawlExecutor(_FakeGateway({"crawl/submit": {"accepted": True}}))
    with pytest.raises(RemoteContentError):
        executor.submit(
            [_work_item()], session_id="s", run_id="r", profile="split", idempotency_key="k"
        )


# ---------------------------------------------------------------------------
# status / cancel
# ---------------------------------------------------------------------------


def test_status_parses_progress() -> None:
    gateway = _FakeGateway(
        {
            "crawl/status": {
                "job_id": "job-1",
                "status": "running",
                "total": 5,
                "pending": 2,
                "running": 1,
                "fetched": 1,
                "failed": 1,
                "cancelled": 0,
                "engines": ["http", "scrapy"],
                "kinds": ["web"],
            }
        }
    )
    executor = EdgeCrawlExecutor(gateway)

    progress = executor.status("job-1")

    assert progress == CrawlProgress(
        job_id="job-1",
        status="running",
        total=5,
        pending=2,
        running=1,
        fetched=1,
        failed=1,
        cancelled=0,
        engines=["http", "scrapy"],
        kinds=["web"],
    )
    assert gateway.calls[-1] == ("crawl/status", {"job_id": "job-1"})


def test_cancel_parses_progress_with_defaults() -> None:
    gateway = _FakeGateway({"crawl/cancel": {"job_id": "job-1", "status": "cancelled", "cancelled": 3}})
    executor = EdgeCrawlExecutor(gateway)

    progress = executor.cancel("job-1")

    assert progress.job_id == "job-1"
    assert progress.status == "cancelled"
    assert progress.cancelled == 3
    assert progress.total == 0 and progress.engines == []
    assert gateway.calls[-1] == ("crawl/cancel", {"job_id": "job-1"})


def test_status_missing_status_raises() -> None:
    executor = EdgeCrawlExecutor(_FakeGateway({"crawl/status": {"job_id": "job-1"}}))
    with pytest.raises(RemoteContentError):
        executor.status("job-1")


# ---------------------------------------------------------------------------
# collect: декодирование + I19
# ---------------------------------------------------------------------------


def test_collect_decodes_bytes_and_rehashes() -> None:
    payload_bytes = "привет".encode("utf-8")
    b64 = base64.b64encode(payload_bytes).decode("ascii")
    gateway = _FakeGateway(
        {
            "crawl/collect": {
                "items": [
                    _item(
                        content_hash=content_hash(payload_bytes),
                        http_status=200,
                        content_type="text/html",
                        fetched_at="2026-01-01T00:00:00Z",
                        raw=_raw(b64, claimed=content_hash(payload_bytes)),
                    )
                ],
                "next_cursor": None,
                "done": True,
            }
        }
    )
    executor = EdgeCrawlExecutor(gateway)

    batch = executor.collect("job-1")

    assert isinstance(batch, CrawlBatch)
    assert batch.done is True and batch.next_cursor is None
    (item,) = batch.items
    assert isinstance(item, CrawlResultItem)
    assert item.product_id == "p1" and item.url == URL_A
    assert item.raw_bytes == payload_bytes
    assert item.content_hash == content_hash(payload_bytes)  # пересчитано ядром
    assert item.engine == "scrapy" and item.kind == "web"
    assert item.http_status == 200 and item.content_type == "text/html"
    assert gateway.calls[-1] == ("crawl/collect", {"job_id": "job-1"})


def test_collect_hash_mismatch_raises_i19() -> None:
    b64 = base64.b64encode(b"payload").decode("ascii")
    gateway = _FakeGateway(
        {
            "crawl/collect": {
                "items": [_item(raw=_raw(b64, claimed="deadbeef"))],
                "done": True,
            }
        }
    )
    executor = EdgeCrawlExecutor(gateway)

    with pytest.raises(RemoteContentError, match="I19"):
        executor.collect("job-1")


def test_collect_bad_base64_raises() -> None:
    gateway = _FakeGateway(
        {
            "crawl/collect": {
                "items": [_item(raw={"bytes_b64": "!!!not-base64!!!", "hash": "x"})],
                "done": True,
            }
        }
    )
    executor = EdgeCrawlExecutor(gateway)

    with pytest.raises(RemoteContentError):
        executor.collect("job-1")


def test_collect_without_raw_keeps_error_and_no_hash() -> None:
    gateway = _FakeGateway(
        {
            "crawl/collect": {
                "items": [_item(status="failed", error="timeout", engine="http", kind=None)],
                "done": True,
            }
        }
    )
    executor = EdgeCrawlExecutor(gateway)

    (item,) = executor.collect("job-1").items
    assert item.raw_bytes is None
    assert item.content_hash is None  # заявленному edge хэшу не доверяем (I19)
    assert item.error == "timeout"


def test_collect_parses_final_url() -> None:
    """§6a: опциональный ``final_url`` (финальный URL после редиректов) доезжает."""
    gateway = _FakeGateway(
        {
            "crawl/collect": {
                "items": [_item(final_url="https://docs.test/p1/final/")],
                "done": True,
            }
        }
    )
    executor = EdgeCrawlExecutor(gateway)

    (item,) = executor.collect("job-1").items
    assert item.final_url == "https://docs.test/p1/final/"
    assert item.url == URL_A  # запрошенный URL сохранён (ключ элемента сеанса)


def test_collect_final_url_absent_is_none() -> None:
    """R7: отсутствующий ``final_url`` ⇒ ``None`` (ядро трактует как запрошенный URL)."""
    gateway = _FakeGateway({"crawl/collect": {"items": [_item()], "done": True}})
    executor = EdgeCrawlExecutor(gateway)

    (item,) = executor.collect("job-1").items
    assert item.final_url is None


def test_collect_final_url_malformed_degrades_to_none() -> None:
    """Malformed ``final_url`` (не-строка) не роняет сбор — деградирует в ``None``."""
    gateway = _FakeGateway(
        {"crawl/collect": {"items": [_item(final_url=123)], "done": True}}
    )
    executor = EdgeCrawlExecutor(gateway)

    (item,) = executor.collect("job-1").items
    assert item.final_url is None


def test_collect_error_item_is_not_trusted_as_success() -> None:
    """Сбойный элемент присутствует в батче (raw отсутствует) — не роняет сбор."""
    gateway = _FakeGateway(
        {
            "crawl/collect": {
                "items": [
                    _item(status="failed", error="http_500", engine="http"),
                    _item(url=URL_B, status="fetched", raw=_raw(
                        base64.b64encode(b"ok").decode("ascii"),
                        claimed=content_hash(b"ok"),
                    )),
                ],
                "done": True,
            }
        }
    )
    executor = EdgeCrawlExecutor(gateway)

    batch = executor.collect("job-1")
    assert [i.status for i in batch.items] == ["failed", "fetched"]
    assert batch.items[0].raw_bytes is None
    assert batch.items[1].raw_bytes == b"ok"


# ---------------------------------------------------------------------------
# collect: пагинация и лимиты
# ---------------------------------------------------------------------------


def test_collect_pagination_passes_cursor_and_limit() -> None:
    gateway = _FakeGateway(
        {
            "crawl/collect": {
                "items": [_item()],
                "next_cursor": "cur-2",
                "done": False,
            }
        }
    )
    executor = EdgeCrawlExecutor(gateway)

    batch = executor.collect("job-1", cursor="cur-1", limit=7)

    assert batch.next_cursor == "cur-2" and batch.done is False
    assert gateway.calls[-1] == (
        "crawl/collect",
        {"job_id": "job-1", "cursor": "cur-1", "limit": 7},
    )


def test_collect_omits_cursor_and_limit_when_absent() -> None:
    gateway = _FakeGateway({"crawl/collect": {"items": [], "done": True}})
    executor = EdgeCrawlExecutor(gateway)

    executor.collect("job-1")

    assert gateway.calls[-1] == ("crawl/collect", {"job_id": "job-1"})


def test_collect_done_defaults_from_cursor() -> None:
    gateway = _FakeGateway(
        {"crawl/collect": {"items": [_item()], "next_cursor": "cur-2"}}  # без done
    )
    executor = EdgeCrawlExecutor(gateway)

    batch = executor.collect("job-1")

    assert batch.done is False  # есть курсор → не последняя страница
    assert batch.next_cursor == "cur-2"


def test_collect_rejects_too_many_items() -> None:
    gateway = _FakeGateway(
        {"crawl/collect": {"items": [_item(url=f"{URL_A}/{i}") for i in range(3)], "done": True}}
    )
    executor = EdgeCrawlExecutor(gateway, max_batch_items=2)

    with pytest.raises(RemoteContentError, match="лимит"):
        executor.collect("job-1")


def test_collect_rejects_oversized_payload() -> None:
    blob = base64.b64encode(b"x" * 4096).decode("ascii")
    gateway = _FakeGateway(
        {"crawl/collect": {"items": [_item(raw=_raw(blob, claimed=None))], "done": True}}
    )
    executor = EdgeCrawlExecutor(gateway, max_batch_bytes=256)

    with pytest.raises(RemoteContentError):
        executor.collect("job-1")


def test_collect_items_must_be_list() -> None:
    gateway = _FakeGateway({"crawl/collect": {"items": "nope", "done": True}})
    executor = EdgeCrawlExecutor(gateway)

    with pytest.raises(RemoteContentError):
        executor.collect("job-1")


def test_collect_rejects_non_string_cursor() -> None:
    gateway = _FakeGateway(
        {"crawl/collect": {"items": [], "next_cursor": 5, "done": False}}
    )
    executor = EdgeCrawlExecutor(gateway)

    with pytest.raises(RemoteContentError):
        executor.collect("job-1")


def test_collect_item_missing_required_fields_raises() -> None:
    executor = EdgeCrawlExecutor(
        _FakeGateway({"crawl/collect": {"items": [{"url": URL_A}], "done": True}})
    )
    with pytest.raises(RemoteContentError):
        executor.collect("job-1")


def test_non_mapping_gateway_response_raises() -> None:
    executor = EdgeCrawlExecutor(_FakeGateway({"crawl/status": ["not", "a", "mapping"]}))
    with pytest.raises(RemoteContentError):
        executor.status("job-1")


def test_submit_backpressure_returns_not_accepted_with_retry_after() -> None:
    """C2: ``accepted=false`` → ``CrawlSubmitResult`` с пустым ``job_id`` и ``retry_after_s``."""
    gateway = _FakeGateway(
        {"crawl/submit": {"job_id": "", "accepted": False, "retry_after": 1.5}}
    )
    executor = EdgeCrawlExecutor(gateway)

    submission = executor.submit(
        [_work_item()], session_id="s", run_id="r", profile="split", idempotency_key="k"
    )

    assert isinstance(submission, CrawlSubmitResult)
    assert submission.accepted is False
    assert submission.job_id == ""
    assert submission.retry_after_s == pytest.approx(1.5)


def test_submit_accepted_defaults_true_when_absent() -> None:
    """C2: отсутствие поля ``accepted`` трактуется как принятие (обратная совместимость)."""
    gateway = _FakeGateway({"crawl/submit": {"job_id": "job-9"}})
    executor = EdgeCrawlExecutor(gateway)

    submission = executor.submit(
        [_work_item()], session_id="s", run_id="r", profile="split", idempotency_key="k"
    )

    assert submission.accepted is True
    assert submission.job_id == "job-9"
    assert submission.retry_after_s is None


def test_collect_rejects_non_public_url() -> None:
    """C1: SSRF-перепроверка собранного URL — непубличный адрес отвергается ядром."""
    gateway = _FakeGateway(
        {
            "crawl/collect": {
                "items": [_item(url="http://127.0.0.1/secret")],
                "done": True,
            }
        }
    )
    executor = EdgeCrawlExecutor(gateway)

    with pytest.raises(RemoteContentError, match="небезопасный url"):
        executor.collect("job-1")


def test_collect_rejects_metadata_url() -> None:
    """C1: metadata-эндпоинт облака отвергается на стороне ядра."""
    gateway = _FakeGateway(
        {
            "crawl/collect": {
                "items": [_item(url="http://169.254.169.254/latest/meta-data")],
                "done": True,
            }
        }
    )
    executor = EdgeCrawlExecutor(gateway)

    with pytest.raises(RemoteContentError, match="небезопасный url"):
        executor.collect("job-1")


def test_collect_accepts_public_url() -> None:
    """C1: публичный URL проходит SSRF-перепроверку (не ложное срабатывание)."""
    payload = b"ok"
    gateway = _FakeGateway(
        {
            "crawl/collect": {
                "items": [
                    _item(
                        raw=_raw(
                            base64.b64encode(payload).decode("ascii"),
                            claimed=content_hash(payload),
                        )
                    )
                ],
                "done": True,
            }
        }
    )
    executor = EdgeCrawlExecutor(gateway)

    (item,) = executor.collect("job-1").items
    assert item.url == URL_A  # https://docs.test/... — публичный
    assert item.raw_bytes == payload
