"""Remote-исполнитель краула — ``EdgeCrawlExecutor`` (Phase 3.3; 07 §4).

Реализация ``CrawlExecutorPort`` в ядре для профиля ``split``: планирование/исполнение
краула делегируется DMZ-планировщику (edge), а ядро общается с ним через
``ContentGateway`` (``crawl/submit`` → ``crawl/status`` → ``crawl/collect``). Это
замещает per-URL ``RemoteCrawler``-путь для split: одна работа — один batch-запрос к шву.

I19: ``collect`` декодирует ``raw.bytes_b64`` в сырые байты и **пересчитывает**
``content_hash`` сам (:func:`core.adapters.remote_gateway.verify_content_hash`);
заявленному edge хэшу ядро не доверяет (несовпадение — ошибка содержимого). Retention
делает **corpus** (Ш5), не адаптер.

Валидация ответов — минимальная проверка формы в стиле
:mod:`core.adapters.gateway_validate` (``core.edge`` не импортируется: матрица слоёв
01 §1 запрещает ``adapters → edge``). Нейтрален (§1.3).
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
from typing import Any

from core.adapters.gateway_validate import (
    MAX_RESPONSE_BYTES,
    check_size,
)
from core.adapters.remote_gateway import RemoteContentError, verify_content_hash
from core.domain.models import RateLimitSpec
from core.ports.crawl_executor import (
    ACTIVE_JOB_STATUSES,
    CrawlBatch,
    CrawlProgress,
    CrawlResultItem,
    CrawlSubmitResult,
    CrawlWorkItem,
)
from core.ports.gateway import ContentGateway
from core.util.security import validate_url

__all__ = ["ACTIVE_JOB_STATUSES", "DEFAULT_MAX_BATCH_ITEMS", "EdgeCrawlExecutor"]

#: Лимит числа элементов в одной странице ``crawl/collect`` (защита ядра от раздутых батчей).
DEFAULT_MAX_BATCH_ITEMS = 10_000


def _as_text(value: Any) -> Any:
    """Приводит enum-значение (``StrEnum``) к строке ``.value``; прочее — как есть."""
    return getattr(value, "value", value)


def _opt_str(value: Any) -> str | None:
    """Строка или ``None`` (непустая); иначе ``None`` (необязательное поле провенанса)."""
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _opt_url(value: Any) -> str | None:
    """URL-строка или ``None``: не-строка/пустая → ``None`` (malformed не роняет сбор).

    Строже :func:`_opt_str`: для ``final_url`` коэрция числа/объекта в строку дала бы
    мусорный URL — безопаснее трактовать как отсутствие (R7: нет ``final_url`` ⇒
    запрошенный URL).
    """
    if isinstance(value, str) and value:
        return value
    return None


def _opt_int(value: Any) -> int | None:
    """``int`` или ``None``; ``bool`` не принимается (это подкласс ``int``)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _opt_float(value: Any) -> float | None:
    """``float`` или ``None``; ``bool`` не принимается (это подкласс ``int``)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _str_list(value: Any) -> list[str]:
    """Список строк из значения (``None`` → пустой список; не-список игнорируется)."""
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item) for item in value if item is not None]


class EdgeCrawlExecutor:
    """``CrawlExecutorPort`` поверх DMZ-шва (submit/status/cancel/collect).

    :param gateway: ``ContentGateway`` — транспорт ядро→edge (07 §3).
    :param profile: деплой-профиль зон, передаваемый в ``crawl/submit`` (``split``/
        ``split-hardened``); ``None`` → ``"split"``.
    :param version: версия адаптера (провенанс/манифест).
    :param user_agent: значение ``User-Agent`` для egress/robots-политики исполнителя
        (та же операционная настройка, что ``deploy.yaml:crawl.user_agent``). Пустое/
        ``None`` ⇒ ключ ``user_agent`` в ``crawl/submit`` **не отправляется** (схема
        объявляет его строкой, не ``null``).
    :param max_batch_items: лимит числа элементов в одной странице ``collect``.
    :param max_batch_bytes: лимит размера декодированной/сериализованной страницы
        ``collect`` (защита ядра, 07 §7).
    """

    def __init__(
        self,
        gateway: ContentGateway,
        *,
        profile: str | None = "split",
        version: str = "edge-crawl-executor-1",
        user_agent: str | None = None,
        max_batch_items: int = DEFAULT_MAX_BATCH_ITEMS,
        max_batch_bytes: int = MAX_RESPONSE_BYTES,
    ) -> None:
        self._gateway = gateway
        self.profile = profile or "split"
        self.version = version
        self._user_agent = user_agent
        self._max_batch_items = max(0, int(max_batch_items))
        self._max_batch_bytes = int(max_batch_bytes)

    # -- контракт CrawlExecutorPort ------------------------------------------- #

    def submit(
        self,
        items: list[CrawlWorkItem],
        *,
        session_id: str,
        run_id: str,
        profile: str,
        idempotency_key: str,
    ) -> CrawlSubmitResult:
        """``POST /v1/crawl/submit`` → :class:`CrawlSubmitResult`.

        Payload: ``{run_id, session_id, idempotency_key, profile, [user_agent], items}``;
        каждый элемент — ``{product_id, url, engine, kind, fetch, extract, strategy,
        wait_for, lang, robots, rate_limit}`` (оси — строковые значения enum;
        ``robots``/``rate_limit`` — additive, §6b/§11.1). ``user_agent`` — request-level
        (§6b), отправляется только когда он непустой (схема объявляет его строкой).
        Пустой набор работ — ошибка содержимого.

        Back-pressure (C2): ``accepted`` (по умолчанию ``True``, если поле отсутствует) и
        опциональный ``retry_after`` (секунды) прокидываются в результат — ядро решает,
        повторять ли подачу. При ``accepted=True`` отсутствие/пустота ``job_id`` — ошибка.
        """
        if not items:
            raise RemoteContentError("crawl/submit: пустой список работ")
        payload: dict[str, Any] = {
            "run_id": run_id,
            "session_id": session_id,
            "idempotency_key": idempotency_key,
            "profile": profile,
        }
        # §6b: request-level egress-политика; ключ — только для непустой строки
        # (схема ``submit_request.user_agent`` = ``{"type": "string"}``, ``null`` запрещён).
        if self._user_agent:
            payload["user_agent"] = self._user_agent
        payload["items"] = [self._work_item_payload(item) for item in items]
        response = self._call("crawl/submit", payload)
        accepted_raw = response.get("accepted")
        accepted = True if accepted_raw is None else bool(accepted_raw)
        job_id = response.get("job_id")
        if accepted and (not isinstance(job_id, str) or not job_id):
            raise RemoteContentError("crawl/submit: ответ без непустого job_id")
        return CrawlSubmitResult(
            job_id=job_id if isinstance(job_id, str) else "",
            accepted=accepted,
            retry_after_s=_opt_float(response.get("retry_after")),
        )

    def status(self, handle: str) -> CrawlProgress:
        """``POST /v1/crawl/status`` → прогресс задания."""
        return self._progress(self._call("crawl/status", {"job_id": handle}))

    def cancel(self, handle: str) -> CrawlProgress:
        """``POST /v1/crawl/cancel`` → прогресс отменённого задания."""
        return self._progress(self._call("crawl/cancel", {"job_id": handle}))

    def collect(
        self, handle: str, *, cursor: str | None = None, limit: int | None = None
    ) -> CrawlBatch:
        """``POST /v1/crawl/collect`` → страница результатов (сырые байты, I19).

        ``cursor``/``limit`` кладутся в payload только если заданы. Размер страницы
        ограничен (``max_batch_items``/``max_batch_bytes``) — превышение отвергается.
        """
        payload: dict[str, Any] = {"job_id": handle}
        if cursor is not None:
            payload["cursor"] = cursor
        if limit is not None:
            payload["limit"] = int(limit)
        return self._batch(self._call("crawl/collect", payload))

    # -- транспорт ------------------------------------------------------------ #

    def _call(self, op: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        """Один вызов шва; не-``Mapping`` ответ — ошибка содержимого."""
        response = self._gateway.call(op, payload)
        if not isinstance(response, Mapping):
            raise RemoteContentError(
                f"{op}: payload ответа edge не является отображением"
            )
        return response

    # -- сериализация запроса -------------------------------------------------- #

    def _work_item_payload(self, item: CrawlWorkItem) -> dict[str, Any]:
        """Элемент ``items`` запроса ``crawl/submit`` (оси — строковые значения enum).

        Additive (§11.1/§6b): ``robots`` (bool, egress-вежливость) и ``rate_limit`` —
        резолвнутый per-item диапазон как **структурные числа** ``{min_s, max_s}``
        (или ``None``), чтобы edge восстановил ``RateLimitSpec`` (не строка
        ``canonical_rate_limit``).
        """
        payload = {
            "product_id": item.product_id,
            "url": item.url,
            "engine": item.engine,
            "kind": item.kind,
            "fetch": _as_text(item.fetch),
            "extract": _as_text(item.extract),
            "strategy": _as_text(item.strategy),
            "wait_for": item.wait_for,
            "lang": item.lang,
            "robots": bool(item.robots),
            "rate_limit": self._rate_limit_payload(item.rate_limit),
        }
        # Условный запрос (additive): ключи только при наличии валидаторов — старые
        # edge без этих полей продолжают работать, безусловный GET не меняется.
        if item.if_none_match:
            payload["if_none_match"] = item.if_none_match
        if item.if_modified_since:
            payload["if_modified_since"] = item.if_modified_since
        return payload

    @staticmethod
    def _rate_limit_payload(spec: RateLimitSpec | None) -> dict[str, float] | None:
        """``RateLimitSpec`` → ``{"min_s", "max_s"}``; ``None`` → ``None`` (без пейсинга)."""
        if spec is None:
            return None
        return {"min_s": float(spec.min_s), "max_s": float(spec.max_s)}

    # -- разбор ответов -------------------------------------------------------- #

    def _progress(self, response: Mapping[str, Any]) -> CrawlProgress:
        """Собирает ``CrawlProgress`` из payload ``crawl/status``/``crawl/cancel``."""
        job_id = response.get("job_id")
        status = response.get("status")
        if not isinstance(job_id, str) or not job_id:
            raise RemoteContentError("crawl: ответ без непустого job_id")
        if not isinstance(status, str) or not status:
            raise RemoteContentError("crawl: ответ без непустого status")
        return CrawlProgress(
            job_id=job_id,
            status=status,
            total=_opt_int(response.get("total")) or 0,
            pending=_opt_int(response.get("pending")) or 0,
            running=_opt_int(response.get("running")) or 0,
            fetched=_opt_int(response.get("fetched")) or 0,
            failed=_opt_int(response.get("failed")) or 0,
            cancelled=_opt_int(response.get("cancelled")) or 0,
            engines=_str_list(response.get("engines")),
            kinds=_str_list(response.get("kinds")),
        )

    def _batch(self, response: Mapping[str, Any]) -> CrawlBatch:
        """Собирает ``CrawlBatch`` из payload ``crawl/collect`` (лимиты, I19)."""
        size_issue = check_size(response, self._max_batch_bytes)
        if size_issue is not None:
            raise RemoteContentError(f"crawl/collect: {size_issue.message}")

        raw_items = response.get("items")
        if not isinstance(raw_items, list):
            raise RemoteContentError("crawl/collect: поле items должно быть списком")
        if self._max_batch_items and len(raw_items) > self._max_batch_items:
            raise RemoteContentError(
                f"crawl/collect: {len(raw_items)} элементов > лимита {self._max_batch_items}"
            )

        items = [self._result_item(item) for item in raw_items]

        next_cursor = response.get("next_cursor")
        if next_cursor is not None and not isinstance(next_cursor, str):
            raise RemoteContentError("crawl/collect: next_cursor должен быть строкой или null")
        next_cursor = next_cursor or None
        done_raw = response.get("done")
        done = bool(done_raw) if done_raw is not None else (next_cursor is None)
        return CrawlBatch(items=items, next_cursor=next_cursor, done=done)

    def _result_item(self, item: Any) -> CrawlResultItem:
        """Собирает ``CrawlResultItem`` из элемента ``items`` ``crawl/collect``.

        I19: сырые байты декодируются, ``content_hash`` **пересчитывается** ядром
        (:func:`verify_content_hash`); заявленный edge хэш только сверяется.
        """
        if not isinstance(item, Mapping):
            raise RemoteContentError("crawl/collect.items: элемент должен быть объектом")
        product_id = item.get("product_id")
        url = item.get("url")
        if not isinstance(product_id, str) or not product_id:
            raise RemoteContentError("crawl/collect.items: product_id — непустая строка")
        if not isinstance(url, str) or not url:
            raise RemoteContentError("crawl/collect.items: url — непустая строка")
        # C1: SSRF-перепроверка URL на стороне ядра (edge не доверяем): непубличные/
        # невалидные адреса (в т.ч. литеральные private/loopback/metadata) отвергаются —
        # их контент не принимается (§5/§10).
        try:
            validate_url(url)
        except ValueError as exc:
            raise RemoteContentError(
                f"crawl/collect.items: небезопасный url ({exc})"
            ) from exc

        raw = item.get("raw")
        claimed_hash = item.get("content_hash")
        raw_bytes: bytes | None = None
        if isinstance(raw, Mapping):
            claimed_hash = raw.get("hash") or claimed_hash
            raw_bytes = self._decode_raw(raw)

        computed_hash: str | None = None
        if raw_bytes is not None:
            computed_hash = verify_content_hash(
                raw_bytes, None if claimed_hash is None else str(claimed_hash)
            )

        not_modified = item.get("not_modified") is True
        if not_modified and not raw_bytes:
            # 304 без байтов представления — неконсистентный ответ edge.
            raise RemoteContentError(
                "crawl/collect.items: not_modified=true без закэшированных байтов"
            )

        return CrawlResultItem(
            product_id=product_id,
            url=url,
            status=str(item.get("status") or ""),
            engine=_opt_str(item.get("engine")),
            kind=_opt_str(item.get("kind")),
            http_status=_opt_int(item.get("http_status")),
            content_type=_opt_str(item.get("content_type")),
            fetched_at=_opt_str(item.get("fetched_at")),
            # Авторитетен только пересчитанный ядром хэш (I19); заявленный — не доверяем.
            content_hash=computed_hash,
            raw_bytes=raw_bytes,
            error=_opt_str(item.get("error")),
            final_url=_opt_url(item.get("final_url")),
            etag=_opt_str(item.get("etag")),
            last_modified=_opt_str(item.get("last_modified")),
            not_modified=not_modified,
        )

    def _decode_raw(self, raw: Mapping[str, Any]) -> bytes | None:
        """Декодирует сырые байты ``raw`` (``bytes_b64`` → ``content``); иначе ``None``.

        Битый base64 — ошибка содержимого (без тихого фолбэка), как
        ``remote_gateway._decode_raw_bytes``.
        """
        encoded = raw.get("bytes_b64")
        if encoded is not None:
            try:
                return base64.b64decode(str(encoded), validate=True)
            except (binascii.Error, ValueError) as exc:
                raise RemoteContentError(f"raw.bytes_b64 не декодируется: {exc}") from exc
        content = raw.get("content")
        if content is None:
            return None
        return content if isinstance(content, bytes) else str(content).encode("utf-8")
