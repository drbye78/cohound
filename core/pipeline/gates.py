"""Гейты G1–G4 — решения и их персистенция (02 §A.2, S9, I6/I7).

**Два типа гейтов, один механизм.**

* ``G1``/``G2`` — **внутренняя** аттестация: человек утверждает профиль пакета-якоря
  (когорта/роли/объём/паспорта) и матрицу роль×продукт. Профиль квази-константен
  (правки 2–5 раз в год при еженедельных прогонах), поэтому второй путь прохождения —
  аттестация содержимого профиля (:mod:`core.pipeline.profile_attestation`).
* ``G3``/``G4`` — **точки интеграции**: strict-ревью и утверждение публикации выполняет
  внешняя система. coHound не принимает эти решения — он экспортирует предмет решения
  (``gates/{gate}-request.json``) и **записывает** полученное решение.

Гейт **никогда не проходится автоматически**: оркестратор не «одобряет» гейт сам, он лишь
регистрирует решение. Носитель и аудит-след — таблица ``gate_decisions`` (S9).

**Решение привязано к предмету.** ``subject_digest`` — отпечаток того, о чём решали
(профиль для G1/G2, очередь ревью для G3, Coverage/Capability/артефакты для G4).
Раньше любая строка проходила гейт навсегда, независимо от состояния прогона; привязка
делает устаревшее одобрение невозможным: изменился предмет — решение больше не действует.

**Вердикт обязателен.** ``verdict='rejected'`` — такая же запись, как одобрение. Без
вердикта внешняя интеграция вырождалась бы в «внешняя система умеет только одобрять»:
отказ выражался бы молчанием, а молчание неотличимо от «ещё никто не смотрел».

Нейтрален (§1.3).
"""

from __future__ import annotations

import datetime as _dt
import hashlib
from dataclasses import dataclass
from typing import Any, Literal

from core.domain import Gate, GateDecision, GateSource, GateVerdict
from core.ports.store import GateStore

__all__ = [
    "WAIVER_REF_PREFIX",
    "GateCheck",
    "GateCheckStatus",
    "evaluate_gate",
    "record_decision",
    "gate_g1",
    "gate_g2",
    "gate_g3",
    "gate_g4",
    "is_gate_passed",
    "gate_decision_for",
]


#: Префикс ``external_ref`` waiver-строки (делегирование гейта внешнему процессу).
WAIVER_REF_PREFIX = "waiver:"


def _now_iso() -> str:
    """Текущее время в ISO-8601 (UTC)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _as_gate(gate: Gate | str) -> Gate:
    """Нормализует гейт к ``Gate`` (принимает enum или строку ``"G1"``)."""
    return gate if isinstance(gate, Gate) else Gate(gate)


def _decision_id(
    run_id: str,
    gate: Gate,
    target: str | None,
    actor: str,
    notes: str | None,
    subject_digest: str | None = None,
    source: str = "internal",
    external_ref: str | None = None,
    verdict: str = "approved",
) -> str:
    """Детерминированный ``decision_id`` решения гейта.

    Ключ **без** метки времени: повторная подача того же решения идемпотентно
    перезаписывает ту же строку ``gate_decisions`` (S9), а не плодит дубли. В ключ входят
    предмет (``subject_digest``), источник, внешняя ссылка и вердикт: одобрение и отказ по
    одному и тому же предмету — **разные** решения и оба остаются в аудит-следе.
    """
    raw = (
        f"{run_id}|{gate.value}|{target or ''}|{actor}|{notes or ''}"
        f"|{subject_digest or ''}|{source}|{external_ref or ''}|{verdict}"
    ).encode()
    return "gd_" + hashlib.sha256(raw).hexdigest()[:24]


def record_decision(
    store: GateStore,
    run_id: str,
    gate: Gate | str,
    *,
    actor: str,
    target: str | None = None,
    notes: str | None = None,
    clock: Any | None = None,
    subject_digest: str | None = None,
    source: GateSource | str = GateSource.INTERNAL,
    external_ref: str | None = None,
    verdict: GateVerdict | str = GateVerdict.APPROVED,
) -> GateDecision:
    """Строит и персистит решение гейта (S9).

    Гейты **не** автоматические (I6/I7): ``actor`` обязателен и непуст. Решение пишется
    через ``GateStore.save_gate_decision``; без этой строки гейт не считается пройденным.

    :param subject_digest: отпечаток предмета решения. ``None`` — решение **без привязки**
        (legacy/явный override); такое решение проходит гейт без сверки предмета.
    :param source: ``internal`` (человек в coHound) либо ``external`` (внешняя система).
        Для ``external`` обязательны ``external_ref`` и ``notes``: решение без причины и
        без ссылки на решающую систему — не аудит-след.
    :param verdict: ``approved``/``rejected``. Отказ блокирует гейт с явным сообщением.
    :param clock: опциональные часы с ``now_iso()`` (тестируемость); иначе системное UTC.
    :raises ValueError: пустой ``actor``; внешнее решение без ``external_ref``/``notes``.
    """
    normalized = _as_gate(gate)
    actor = (actor or "").strip()
    if not actor:
        raise ValueError(
            f"гейт {normalized.value} не может быть пройден без actor (человек, I6/I7)"
        )
    normalized_source = (
        source if isinstance(source, GateSource) else GateSource(source)
    )
    normalized_verdict = (
        verdict if isinstance(verdict, GateVerdict) else GateVerdict(verdict)
    )
    if normalized_source is GateSource.EXTERNAL:
        if not (external_ref or "").strip():
            raise ValueError(
                f"внешнее решение гейта {normalized.value} обязано нести external_ref "
                "(ссылка на решающую систему)"
            )
        if not (notes or "").strip():
            raise ValueError(
                f"внешнее решение гейта {normalized.value} обязано нести notes: "
                "решение без причины — не аудит-след"
            )
    decided_at = clock.now_iso() if clock is not None else _now_iso()
    decision = GateDecision(
        decision_id=_decision_id(
            run_id,
            normalized,
            target,
            actor,
            notes,
            subject_digest,
            normalized_source.value,
            external_ref,
            normalized_verdict.value,
        ),
        run_id=run_id,
        gate=normalized,
        target=target,
        actor=actor,
        decided_at=decided_at,
        notes=notes,
        subject_digest=subject_digest,
        source=normalized_source,
        external_ref=external_ref,
        verdict=normalized_verdict,
    )
    store.save_gate_decision(decision)
    return decision


def gate_g1(
    store: GateStore,
    run_id: str,
    *,
    actor: str,
    target: str | None = None,
    notes: str | None = None,
    clock: Any | None = None,
    subject_digest: str | None = None,
    source: GateSource | str = GateSource.INTERNAL,
    external_ref: str | None = None,
    verdict: GateVerdict | str = GateVerdict.APPROVED,
) -> GateDecision:
    """G1 — утверждение профиля пакета-якоря (когорта/роли/объём/паспорта)."""
    return record_decision(
        store,
        run_id,
        Gate.G1,
        actor=actor,
        target=target,
        notes=notes,
        clock=clock,
        subject_digest=subject_digest,
        source=source,
        external_ref=external_ref,
        verdict=verdict,
    )


def gate_g2(
    store: GateStore,
    run_id: str,
    *,
    actor: str,
    target: str | None = None,
    notes: str | None = None,
    clock: Any | None = None,
    subject_digest: str | None = None,
    source: GateSource | str = GateSource.INTERNAL,
    external_ref: str | None = None,
    verdict: GateVerdict | str = GateVerdict.APPROVED,
) -> GateDecision:
    """G2 — утверждение матрицы роль×продукт (relationship_evidence)."""
    return record_decision(
        store,
        run_id,
        Gate.G2,
        actor=actor,
        target=target,
        notes=notes,
        clock=clock,
        subject_digest=subject_digest,
        source=source,
        external_ref=external_ref,
        verdict=verdict,
    )


def gate_g3(
    store: GateStore,
    run_id: str,
    *,
    actor: str,
    target: str | None = None,
    notes: str | None = None,
    clock: Any | None = None,
    subject_digest: str | None = None,
    source: GateSource | str = GateSource.INTERNAL,
    external_ref: str | None = None,
    verdict: GateVerdict | str = GateVerdict.APPROVED,
) -> GateDecision:
    """G3 — strict-ревью ячеек: решение **внешней** системы, записанное coHound.

    coHound не ревьюит: он экспортирует очередь ревью (``gates/G3-request.json``) и
    принимает ответ (``approved``/``rejected``) с ``subject_digest`` очереди.
    """
    return record_decision(
        store,
        run_id,
        Gate.G3,
        actor=actor,
        target=target,
        notes=notes,
        clock=clock,
        subject_digest=subject_digest,
        source=source,
        external_ref=external_ref,
        verdict=verdict,
    )


def gate_g4(
    store: GateStore,
    run_id: str,
    *,
    actor: str,
    target: str | None = None,
    notes: str | None = None,
    clock: Any | None = None,
    subject_digest: str | None = None,
    source: GateSource | str = GateSource.INTERNAL,
    external_ref: str | None = None,
    verdict: GateVerdict | str = GateVerdict.APPROVED,
) -> GateDecision:
    """G4 — утверждение публикации: решение **внешней** системы, записанное coHound.

    coHound не решает, публиковать ли: он экспортирует Coverage/Capability и состав
    артефактов (``gates/G4-request.json``) и принимает ответ с ``subject_digest``.
    I11 («нет Coverage — нет публикации») остаётся в коде отчёта и от этого гейта не
    зависит.
    """
    return record_decision(
        store,
        run_id,
        Gate.G4,
        actor=actor,
        target=target,
        notes=notes,
        clock=clock,
        subject_digest=subject_digest,
        source=source,
        external_ref=external_ref,
        verdict=verdict,
    )


#: Исход сверки решений гейта с текущим предметом.
GateCheckStatus = Literal[
    "passed",          # есть одобрение текущего предмета
    "missing",         # решений нет вовсе
    "stale",           # одобрение есть, но предмет изменился
    "rejected",        # отказ по текущему предмету
    "rejected_stale",  # отказ относится к устаревшему предмету
]


@dataclass(frozen=True)
class GateCheck:
    """Результат сверки решений гейта с текущим предметом (``subject_digest``).

    ``decision`` — решающая строка (одобрение/отказ текущего предмета), ``previous`` —
    ближайшая строка, объясняющая блок (устаревшее одобрение или устаревший отказ).
    """

    status: GateCheckStatus
    decision: GateDecision | None = None
    previous: GateDecision | None = None


def evaluate_gate(
    store: GateStore,
    run_id: str,
    gate: Gate | str,
    subject_digest: str | None = None,
) -> GateCheck:
    """Сверяет решения гейта с текущим предметом и вердиктом.

    Правила (в порядке приоритета):

    1. **Точное совпадение предмета** — решает **последнее** решение с этим
       ``subject_digest``: ``approved`` → ``passed``, ``rejected`` → ``rejected``.
       Так отказ, пришедший после одобрения того же предмета, честно блокирует гейт.
    2. **Решение без привязки** (``subject_digest is None``: legacy-строка) — проходит,
       если текущий предмет не задан. Это обратная совместимость существующих прогонов:
       строки, записанные до появления привязки, не переписываются и не пере-аттестуются.
    3. **Отказ с другим предметом** → ``rejected_stale`` (отказ относится к устаревшему
       состоянию: повторное ревью может быть обосновано).
    4. Иначе → ``stale``: одобрение было, но предмет изменился.
    5. Решений нет → ``missing``.

    :param subject_digest: отпечаток текущего предмета. ``None`` — сверка без привязки
        (путь legacy-совместимости; ``_gate_phase`` всегда передаёт отпечаток, если может
        его вычислить).
    """
    normalized = _as_gate(gate)
    # Waiver-строки (``external_ref='waiver:...'``) — **не** решения об одобрении, а запись
    # делегирования: они не проходят гейт. Иначе смена ``run.yaml:gates.<gate>`` с
    # ``waived`` на ``external`` оставила бы гейт «пройденным» фактом делегирования.
    decisions = [
        d
        for d in store.iter_gate_decisions(run_id)
        if d.gate is normalized
        and not (d.external_ref or "").startswith(WAIVER_REF_PREFIX)
    ]
    if not decisions:
        return GateCheck(status="missing")

    if subject_digest is not None:
        exact = [d for d in decisions if d.subject_digest == subject_digest]
        if exact:
            last = exact[-1]
            if last.verdict is GateVerdict.REJECTED:
                return GateCheck(status="rejected", decision=last)
            return GateCheck(status="passed", decision=last)
        unbound = [d for d in decisions if not d.subject_digest]
        if unbound:
            last_unbound = unbound[-1]
            if last_unbound.verdict is GateVerdict.REJECTED:
                return GateCheck(status="rejected", decision=last_unbound)
            return GateCheck(status="passed", decision=last_unbound)
        rejected = [d for d in decisions if d.verdict is GateVerdict.REJECTED]
        if rejected:
            return GateCheck(status="rejected_stale", previous=rejected[-1])
        return GateCheck(status="stale", previous=decisions[-1])

    # Сверка без привязки: принимаются только решения без отпечатка предмета.
    unbound = [d for d in decisions if not d.subject_digest]
    if not unbound:
        return GateCheck(status="missing")
    last = unbound[-1]
    if last.verdict is GateVerdict.REJECTED:
        return GateCheck(status="rejected", decision=last)
    return GateCheck(status="passed", decision=last)


def is_gate_passed(
    store: GateStore,
    run_id: str,
    gate: Gate | str,
    subject_digest: str | None = None,
) -> bool:
    """Пройден ли гейт: есть ли **одобрение текущего предмета** (S9).

    Читает ``GateStore.iter_gate_decisions`` и сверяет предмет через
    :func:`evaluate_gate`: одного факта строки недостаточно (устаревшее одобрение не
    действует), и отказ (``verdict='rejected'``) гейт не проходит.

    :param subject_digest: отпечаток текущего предмета; ``None`` — сверка без привязки
        (проходят только решения без отпечатка — legacy-совместимость).
    """
    return evaluate_gate(store, run_id, gate, subject_digest).status == "passed"


def gate_decision_for(
    store: GateStore, run_id: str, gate: Gate | str
) -> GateDecision | None:
    """Последнее решение по гейту (аудит/CLI), либо ``None``.

    ``iter_gate_decisions`` упорядочивает по ``decided_at``; при равных метках — по
    ``decision_id``. Возвращается последняя строка.
    """
    normalized = _as_gate(gate)
    last: GateDecision | None = None
    for decision in store.iter_gate_decisions(run_id):
        if decision.gate is normalized:
            last = decision
    return last
