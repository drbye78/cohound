"""coHound — кандидаты авторского применения пакета (итерация I2).

Мост между **предложениями** разведки/эвристик (:mod:`core.pipeline.package_discovery`)
и **фактом** пакета. Повторяет паттерн ``pipeline/discovery.py`` (G2): разведка только
возвращает кандидатов; превратить кандидата в факт можно **единственной** функцией
:func:`apply_candidate` — и только при явном человеческом акторе.

Ключевые инварианты
-------------------
* **A3 / I6/I7**: никакого auto-approve. ``plan_candidates``/``suggest_*`` ничего не пишут;
  :func:`apply_candidate` без непустого ``actor`` поднимает
  :class:`~core.pipeline.package_admin.PackageAdminError` (``code="actor_required"``).
* **Главный инвариант записи**: применяемая операция сама проходит проспективный линт и
  отказывается писать при новых блокирующих нарушениях (см. ``package_admin._finalize``).
* **Батч не атомарен**: :func:`apply_candidates` применяет последовательно и падает на
  первой блокировке; уже применённые кандидаты **остаются** (fail-fast, без отката).
* **§1.3**: нейтрально к предметной категории.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from core.pipeline import package_admin as _admin
from core.pipeline import package_discovery as _discovery
from core.pipeline.package_admin import OpResult, PackageAdminError
from core.pipeline.package_io import load_artifacts

__all__ = [
    "CANDIDATE_KINDS",
    "Candidate",
    "plan_candidates",
    "apply_candidate",
    "apply_candidates",
    "dumps_candidates",
    "loads_candidates",
]

#: Допустимые виды кандидатов (маршрутизируются в ``add_*``/``update_*``).
CANDIDATE_KINDS: tuple[str, ...] = ("param", "module", "role", "baseline", "cohort_member")

#: Дефолтный вес модуля-кандидата, если профиль не подсказал (assumption, A3).
_DEFAULT_MODULE_WEIGHT = 10.0


@dataclass
class Candidate:
    """Предложение изменения пакета — кандидат, **не** факт (A3).

    :param kind: ``param`` | ``module`` | ``role`` | ``baseline`` | ``cohort_member``.
    :param payload: готовые аргументы соответствующей ``add_*``/``update_*`` операции.
    :param confidence: эвристическая уверенность ∈ [0, 1] (assumption, не измерение).
    :param rationale: человекочитаемое обоснование.
    :param source: происхождение: ``infer`` | ``compose`` | ``heuristic``.
    :param needs_human: требуется человеческое решение (по умолчанию ``True``).
    """

    kind: str
    payload: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.5
    rationale: str = ""
    source: str = "heuristic"
    needs_human: bool = True

    def __post_init__(self) -> None:
        if self.kind not in CANDIDATE_KINDS:
            raise ValueError(
                f"kind кандидата должен быть одним из {CANDIDATE_KINDS}: {self.kind!r}"
            )
        try:
            self.confidence = float(self.confidence)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"confidence должен быть числом: {self.confidence!r}") from exc
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence вне [0, 1]: {self.confidence}")
        if not isinstance(self.payload, dict):
            raise ValueError("payload кандидата должен быть словарём")

    def key(self) -> str:
        """Детерминированный ключ дедупликации (вид + нормализованный payload)."""
        return json.dumps(
            {"kind": self.kind, "payload": self.payload},
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление кандидата."""
        return {
            "kind": self.kind,
            "payload": dict(self.payload),
            "confidence": self.confidence,
            "rationale": self.rationale,
            "source": self.source,
            "needs_human": self.needs_human,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Candidate:
        """Восстанавливает кандидата из JSON-объекта (валидация полей).

        :raises ValueError: не объект / неизвестный ``kind`` / битые типы.
        """
        if not isinstance(data, dict):
            raise ValueError("кандидат должен быть JSON-объектом")
        payload = data.get("payload", {})
        if not isinstance(payload, dict):
            raise ValueError("payload кандидата должен быть словарём")
        return cls(
            kind=data.get("kind", ""),
            payload=dict(payload),
            confidence=data.get("confidence", 0.5),
            rationale=str(data.get("rationale", "")),
            source=str(data.get("source", "heuristic")),
            needs_human=bool(data.get("needs_human", True)),
        )


def _dedup_sorted(candidates: Sequence[Candidate]) -> list[Candidate]:
    """Убирает дубли по :meth:`Candidate.key` и сортирует детерминированно."""
    seen: set[str] = set()
    unique: list[Candidate] = []
    for candidate in candidates:
        key = candidate.key()
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return sorted(unique, key=lambda c: (c.kind, c.key()))


def _param_id_of(candidate: Candidate) -> str | None:
    value = candidate.payload.get("param_id")
    return str(value) if value else None


def plan_candidates(
    package_dir: str | Path,
    *,
    profile: dict[str, Any] | None = None,
    scope: Any = "all",
) -> list[Candidate]:
    """Собирает предложения изменения пакета. **Ничего не пишет** (A3).

    Агрегирует: ``suggest_modules``/``suggest_roles``/``suggest_cohort_members``
    (эвристики профиля) + ``compose_baselines`` (строки наследования) + явные
    ``profile["param_candidates"]``. Дедуп: против уже существующих элементов пакета
    и между собой; порядок детерминирован (``(kind, key)``).
    """
    data = dict(profile or {})
    artifacts = load_artifacts(package_dir)
    manifest = artifacts.manifest

    existing_modules = {m.module_id for m in manifest.modules} if manifest else set()
    existing_roles = {r.role_id for r in manifest.roles} if manifest else set()
    existing_params = {row.param_id for row in artifacts.catalog}
    existing_baselines = {(row.baseline_id, row.param_id) for row in artifacts.baselines}
    existing_cohort = (
        {entry.product_id for entry in artifacts.cohort.entries}
        if artifacts.cohort is not None
        else set()
    )

    collected: list[Candidate] = []

    for candidate in _discovery.suggest_modules(data):
        if candidate.payload.get("module_id") not in existing_modules:
            collected.append(candidate)
    for candidate in _discovery.suggest_roles(data):
        if candidate.payload.get("role_id") not in existing_roles:
            collected.append(candidate)
    for candidate in _discovery.suggest_cohort_members(data):
        if candidate.payload.get("product_id") not in existing_cohort:
            collected.append(candidate)

    proposal = _discovery.compose_baselines(data, artifacts.catalog, scope=scope)
    for row in proposal.rows:
        if (row.baseline_id, row.param_id) not in existing_baselines:
            collected.append(
                Candidate(
                    kind="baseline",
                    payload=row.model_dump(mode="json"),
                    confidence=0.7,
                    rationale=f"compose_baselines: строка наследования {row.param_id}",
                    source="compose",
                )
            )

    for entry in data.get("param_candidates", []) or []:
        if not isinstance(entry, dict):
            continue
        candidate = Candidate(
            kind="param",
            payload={key: value for key, value in entry.items() if key not in ("confidence", "rationale")},
            confidence=entry.get("confidence", 0.5),
            rationale=str(entry.get("rationale", "")),
            source="heuristic",
        )
        if _param_id_of(candidate) not in existing_params:
            collected.append(candidate)

    return _dedup_sorted(collected)


def apply_candidate(
    package_dir: str | Path,
    candidate: Candidate,
    *,
    actor: str,
    dry_run: bool = False,
) -> OpResult:
    """Единственная точка превращения кандидата в факт (человеческий шлюз, I6/I7).

    :raises PackageAdminError: без непустого ``actor`` (``code="actor_required"``),
        неизвестный ``kind`` или блокировка проспективным линтом (``kind="blocked"``).
    """
    if not actor or not str(actor).strip():
        raise PackageAdminError(
            "apply требует непустого actor (человеческий шлюз, I6/I7)",
            code="actor_required",
            kind="usage",
        )
    actor = str(actor).strip()
    payload = dict(candidate.payload)

    if candidate.kind == "module":
        return _admin.add_module(
            package_dir,
            payload["module_id"],
            payload.get("weight", _DEFAULT_MODULE_WEIGHT),
            actor=actor,
            dry_run=dry_run,
        )
    if candidate.kind == "role":
        return _admin.add_role(
            package_dir,
            payload["role_id"],
            payload.get("definition"),
            markers=payload.get("markers"),
            query_patterns=payload.get("query_patterns"),
            core_coverage_allowed=bool(payload.get("core_coverage_allowed", False)),
            multi_instance=bool(payload.get("multi_instance", False)),
            actor=actor,
            dry_run=dry_run,
        )
    if candidate.kind == "param":
        return _admin.add_param(
            package_dir,
            payload["title"],
            payload.get("rule", ""),
            module=payload["module"],
            group=payload.get("group", ""),
            importance=payload.get("importance", "Баз"),
            param_type=payload.get("param_type", "fact"),
            absence_rule=payload.get("absence_rule", "explicit"),
            role_binding=payload.get("role_binding"),
            concept_terms=payload.get("concept_terms"),
            search_langs=payload.get("search_langs"),
            param_id=payload.get("param_id"),
            actor=actor,
            dry_run=dry_run,
        )
    if candidate.kind == "baseline":
        return _admin.add_baseline(
            package_dir,
            payload["baseline_id"],
            payload["param_id"],
            inheritance_mode=payload.get("inheritance_mode", "direct"),
            min_version=payload.get("min_version"),
            note=payload.get("note"),
            actor=actor,
            dry_run=dry_run,
        )
    if candidate.kind == "cohort_member":
        artifacts = load_artifacts(package_dir)
        entries = (
            [entry.model_dump(mode="json") for entry in artifacts.cohort.entries]
            if artifacts.cohort is not None
            else []
        )
        entries.append(
            {
                "product_id": payload["product_id"],
                "role_overlap": payload.get("role_overlap", 0.5),
                # A3: без явного обоснования поле остаётся незаполненным (None), а не
                # подменяется правдоподобным текстом. Прогон такого пакета запрещён.
                "selection_reason": payload.get("selection_reason"),
                "anchor_relation": payload.get("anchor_relation", "analog"),
            }
        )
        return _admin.update_cohort(package_dir, {"entries": entries}, actor=actor, dry_run=dry_run)

    raise PackageAdminError(
        f"неизвестный kind кандидата: {candidate.kind!r}", code="candidate", kind="usage"
    )


def apply_candidates(
    package_dir: str | Path,
    candidates: Sequence[Candidate],
    *,
    actor: str,
    dry_run: bool = False,
) -> list[OpResult]:
    """Применяет кандидатов последовательно, fail-fast на первой блокировке.

    **Батч не атомарен**: уже применённые кандидаты остаются на диске (это нормально —
    каждый apply журналируется отдельно). ``actor`` обязателен (см. :func:`apply_candidate`).
    """
    results: list[OpResult] = []
    for candidate in candidates:
        results.append(apply_candidate(package_dir, candidate, actor=actor, dry_run=dry_run))
    return results


def dumps_candidates(candidates: Sequence[Candidate]) -> str:
    """Детерминированный JSON-массив кандидатов (``sort_keys``, UTF-8, ``\\n`` в конце)."""
    payload = [candidate.as_dict() for candidate in candidates]
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def loads_candidates(text: str) -> list[Candidate]:
    """Разбирает JSON-массив кандидатов (обратно к :func:`dumps_candidates`).

    :raises ValueError: не массив / битый элемент / неизвестный ``kind``.
    """
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"некорректный JSON кандидатов: {exc}") from exc
    if not isinstance(data, list):
        raise ValueError("ожидался JSON-массив кандидатов")
    return [Candidate.from_dict(item) for item in data]
