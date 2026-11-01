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
from core.pipeline.liveness import current_owner, recover_interrupted_sessions

__all__ = [
    "default_session_id",
    "finalize_session",
    "open_session",
    "prior_items",
    "read_provenance",
    "truncation_summary",
]


def _now_iso() -> str:
    """Текущее время в ISO-8601 (UTC)."""
    return _dt.datetime.now(_dt.UTC).isoformat()


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

    Если сеанса нет — создаётся ``CrawlSession`` в статусе ``CREATED``. Сначала вызывается
    recovery-on-open (:func:`core.pipeline.liveness.recover_interrupted_sessions`): сеансы,
    оставшиеся в ``running`` от убитого процесса (SIGKILL/OOM — такую развязку Python не
    перехватывает), получают ``interrupted`` с причиной. Затем
    ``store.recover_crawl_session``: элементы, застрявшие в ``running`` от прерванного
    процесса, возвращаются в ``pending``, счётчики пересчитываются.

    Далее сеанс переводится в ``RUNNING`` с ``started_at`` = текущее UTC-время
    (:meth:`StorePort.update_crawl_session_status`) и получает свидетельство живости
    (``owner`` + ``heartbeat_at``): открытие сеанса означает начало его исполнения.
    Это применимо и к терминальному сеансу (``completed``/``partial``/``cancelled``/
    ``failed``/``interrupted``), переоткрываемому для resume — resume есть **новый запуск**
    того же сеанса, поэтому он снова ``RUNNING``, а ``update_crawl_session_status``
    сбрасывает ``finished_at``/``error`` (C3), снимая признаки прежнего завершения.
    Возвращает актуальный (после восстановления и перехода) сеанс.
    """
    # F1/Д-6: recovery-on-open **до** открытия своего сеанса — иначе протухший ``running``
    # прошлой попытки остался бы фантомом навсегда.
    recover_interrupted_sessions(store)
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
    now = _now_iso()
    store.update_crawl_session_status(
        session_id, CrawlSessionStatus.RUNNING, started_at=now
    )
    store.touch_crawl_session_heartbeat(session_id, at=now, owner=current_owner())
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
    revalidation_fetched: int | None = None,
    truncated_products: Sequence[str] | None = None,
    shard: Mapping[str, Any] | None = None,
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
    pages_fetched, revalidation_fetched}``. Персистенция — через
    :meth:`StorePort.set_crawl_session_provenance`
    (targeted ``UPDATE``).

    **F7 (17 §2): шард узла.** ``shard`` — провенанс развёртывания
    ``{node, hosts, declared, filtered, reason}``. Блок пишется, только когда шард
    объявлен/выбран (иначе артефакт прогона без шардов не меняется). Если узел видел
    **не все** объявленные host'ы (``filtered``), сеанс закрывается ``PARTIAL`` даже
    при нуле сбоев: часть корпуса принадлежит другим узлам, и «всё, что мы пытались,
    добыто» — не то же самое, что «корпус собран». ``completed`` здесь был бы
    утверждением о полноте, которого шардированный узел сделать не может.
    """
    counts = store.crawl_session_counts(session_id)
    defects = counts.get(CrawlItemStatus.FAILED.value, 0) + counts.get(
        CrawlItemStatus.CANCELLED.value, 0
    )
    shard_block = dict(shard) if shard is not None else None
    shard_filtered = bool(shard_block and shard_block.get("filtered"))
    status = (
        CrawlSessionStatus.PARTIAL
        if truncated or defects > 0 or shard_filtered
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
        # S1: ревалидационный трафик виден отдельно; ``pages_fetched`` остаётся discovery.
        "revalidation_fetched": revalidation_fetched,
        # D1/G: продукты, чей краул усечён (для аномалии exhaustive-absence в отчёте).
        "truncated_products": sorted({str(p) for p in (truncated_products or []) if p}),
    }
    # F7: шард узла — deployment provenance. Ключ появляется только у шардированного
    # прогона: ``node_id`` durable, а принадлежность host'а шарду выводима из карты,
    # которая уже лежит в ``effective_limits.roots[*].limits.host_shards``.
    if shard_block is not None:
        provenance["shard"] = shard_block
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


def read_provenance(session: Any) -> dict[str, Any]:
    """Читает ``provenance_json`` сеанса как dict; пусто/сбой → ``{}`` (N4).

    Публичная обёртка над разбором (единый источник для CLI: метрики/код возврата
    читают политику усечения отсюда, не дублируя формат).
    """
    raw = getattr(session, "provenance_json", None)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def truncation_summary(provenance: Mapping[str, Any] | None) -> dict[str, Any]:
    """Сводка политического усечения для payload/метрик (N4).

    Форма — стабильная: ``truncated``/``truncated_reason``/``truncated_products``/
    ``pages_fetched``/``revalidation_fetched``/``effective_limits``.
    ``truncated_reason`` — первая crawl-причина среди записей ``effective_limits.roots``.
    Список причин **не** закрыт двумя: ``max_documents``/``max_depth``/``round_budget`` —
    операторские потолки, ``extract_backlog_full`` (F6) — остановка приёма по объявленному
    watermark'у бэклога, ``disk_low_headroom`` (F8) — остановка сторожем места,
    ``host_out_of_shard`` (F7) — корень принадлежит чужому узлу. Все они приходят одним
    механизмом (``Frontier.mark_truncated``), поэтому перечисление здесь — пояснение, а не
    проверка; подставлять «известную» причину вместо записанной нельзя.
    Отсутствие провенанса → нейтральный «не усечён» (не выдумываем усечение).

    09 §6.3: ревалидация известных URL сама по себе **не** расходует ``max_documents``
    и не расширяет BFS. Операторский потолок ``revalidate_max_documents`` — отдельная,
    видимая причина усечения продукта, но **precedence**: crawl-потолки
    (``max_documents``/``max_depth``/``round_budget``) ранжируются раньше неё. Поэтому
    выбираем первую не-revalidation причину; только если crawl-усечения нет — сообщаем
    ``revalidate_max_documents``. Это сохраняет прежний порядок crawl-причин.
    """
    prov = provenance or {}
    roots = (prov.get("effective_limits") or {}).get("roots") or []
    reason: str | None = None
    revalidation_reason: str | None = None
    for record in roots:
        if not (isinstance(record, Mapping) and record.get("truncated")):
            continue
        candidate = record.get("truncated_reason")
        if candidate == "revalidate_max_documents":
            if revalidation_reason is None:
                revalidation_reason = candidate
            continue
        reason = candidate
        break
    if reason is None:
        reason = revalidation_reason
    return {
        "truncated": bool(prov.get("truncated")),
        "truncated_reason": reason,
        "truncated_products": sorted(
            {str(p) for p in (prov.get("truncated_products") or [])}
        ),
        "pages_fetched": prov.get("pages_fetched"),
        "revalidation_fetched": prov.get("revalidation_fetched"),
        "effective_limits": dict(prov.get("effective_limits") or {}),
    }
