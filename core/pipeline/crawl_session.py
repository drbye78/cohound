"""coHound — локальная оркестрация сеанса краула (Lane B/L1a) для фазы корпуса.

Тонкий in-core слой над ``StorePort`` (ядро-владелец сеанса, §4.4 ``crawl_sessions`` /
``crawl_session_items``): открытие/восстановление сеанса, чтение априорных элементов и
финализация. Исполнение краула здесь **не** абстрагируется: сменный порт-исполнитель
(``CrawlExecutorPort`` для ``split``/edge-планировщика) отложен до Phase 3 (A1/YAGNI).
Модуль — чистая оркестрация над стором, без I/O и без сетевых вызовов.

Сеанс идемпотентен на прогон: ``session_id`` по умолчанию — ``f"crawl:{run_id}"``
(:func:`default_session_id`), поэтому повторный вызов фазы открывает тот же сеанс и
восстанавливает ``running``-элементы в ``pending`` (:func:`open_session` →
``store.recover_crawl_session``).

Нейтрален к предметной категории (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import json
from collections.abc import Mapping, Sequence
from typing import Any

from core.domain import (
    CrawlItemStatus,
    CrawlSession,
    CrawlSessionItem,
    CrawlSessionStatus,
)

__all__ = [
    "default_session_id",
    "open_session",
    "finalize_session",
    "prior_items",
]


def _now_iso() -> str:
    """Текущее время в ISO-8601 (UTC)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def default_session_id(run_id: str) -> str:
    """Детерминированный ``session_id`` сеанса краула прогона (идемпотентность resume).

    Один прогон → один сеанс, пока ``session_id`` не задан явно.
    """
    return f"crawl:{run_id}"


def open_session(
    store: Any,
    *,
    session_id: str,
    run_id: str | None,
    profile: str = "local",
) -> CrawlSession:
    """Get-or-create сеанса + recovery-on-open + переход в ``RUNNING``.

    Если сеанса нет — создаётся ``CrawlSession`` в статусе ``CREATED``. Затем вызывается
    ``store.recover_crawl_session``: элементы, застрявшие в ``running`` от прерванного
    процесса, возвращаются в ``pending``, счётчики пересчитываются.

    Далее сеанс переводится в ``RUNNING`` с ``started_at`` = текущее UTC-время
    (:meth:`StorePort.update_crawl_session_status`): открытие сеанса означает начало его
    исполнения. Это применимо и к терминальному сеансу (``completed``/``partial``/
    ``cancelled``/``failed``), переоткрываемому для resume — resume есть **новый запуск** того
    же сеанса, поэтому он снова ``RUNNING``, а ``update_crawl_session_status`` сбрасывает
    ``finished_at``/``error`` (C3), снимая признаки прежнего завершения. Возвращает актуальный
    (после восстановления и перехода) сеанс.
    """
    if store.get_crawl_session(session_id) is None:
        store.upsert_crawl_session(
            CrawlSession(
                session_id=session_id,
                run_id=run_id,
                profile=profile,
                status=CrawlSessionStatus.CREATED,
                created_at=_now_iso(),
            )
        )
    store.recover_crawl_session(session_id)
    store.update_crawl_session_status(
        session_id, CrawlSessionStatus.RUNNING, started_at=_now_iso()
    )
    session = store.get_crawl_session(session_id)
    if session is None:  # pragma: no cover — защита от рассинхрона store
        raise RuntimeError(f"сеанс краула не открылся: {session_id!r}")
    return session


def finalize_session(
    store: Any,
    session_id: str,
    *,
    finished_at: str,
    engines: list[str] | None = None,
    kinds: list[str] | None = None,
    crawl_sources_digest: str | None = None,
    truncated: bool = False,
    effective_limits: Mapping[str, Any] | None = None,
    pages_fetched: int | None = None,
    truncated_products: Sequence[str] | None = None,
) -> CrawlSessionStatus:
    """Финализирует сеанс: ``COMPLETED`` (нет сбоев) либо ``PARTIAL`` (есть failed/cancelled).

    Статус вычисляется из фактических элементов (``store.crawl_session_counts``): любой
    ``FAILED``/``CANCELLED`` делает сеанс ``PARTIAL`` (часть краула добыта). ``finished_at``
    проставляется в той же записи статуса. Возвращает применённый статус.

    D1/G («честность усечения»): **явно усечённый** краул (потолок ``max_documents`` или
    граница ``max_depth``) **никогда** не рапортуется ``COMPLETED`` — используется
    ``PARTIAL`` (частичный статус сеанса), чтобы политико-усечённый корпус не выглядел
    полным. Усечение, эффективные лимиты и число добытых страниц попадают в провенанс.

    Phase 2: в ``CrawlSession.provenance_json`` пишется компактный JSON-провенанс
    ``{engines, kinds, crawl_sources_digest, status, truncated, effective_limits,
    pages_fetched}``. Персистенция — через :meth:`StorePort.set_crawl_session_provenance`
    (targeted ``UPDATE``).
    """
    counts = store.crawl_session_counts(session_id)
    defects = counts.get(CrawlItemStatus.FAILED.value, 0) + counts.get(
        CrawlItemStatus.CANCELLED.value, 0
    )
    status = (
        CrawlSessionStatus.PARTIAL
        if truncated or defects > 0
        else CrawlSessionStatus.COMPLETED
    )
    store.update_crawl_session_status(session_id, status, finished_at=finished_at)
    provenance: dict[str, Any] = {
        "engines": sorted({str(e) for e in (engines or []) if e}),
        "kinds": sorted({str(k) for k in (kinds or []) if k}),
        "crawl_sources_digest": crawl_sources_digest,
        "status": status.value,
        # D1/G: машинно-читаемая запись о политике усечения (всегда присутствует).
        "truncated": bool(truncated),
        "effective_limits": dict(effective_limits or {}),
        "pages_fetched": pages_fetched,
        # D1/G: продукты, чей краул усечён (для аномалии exhaustive-absence в отчёте).
        "truncated_products": sorted({str(p) for p in (truncated_products or []) if p}),
    }
    store.set_crawl_session_provenance(
        session_id,
        json.dumps(provenance, ensure_ascii=False, sort_keys=True),
    )
    return status


def prior_items(
    store: Any, session_id: str
) -> dict[tuple[str, str], CrawlSessionItem]:
    """Априорные элементы сеанса, ключ ``(product_id, url)`` (для resume без ре-фетча).

    Ключ совпадает с PK ``crawl_session_items`` без ``session_id``: один и тот же URL под
    разными продуктами имеет независимые записи (I17/§4.4: партиция и её retained raw
    принадлежат продукту).
    """
    return {
        (item.product_id, item.url): item
        for item in store.iter_crawl_session_items(session_id)
    }
