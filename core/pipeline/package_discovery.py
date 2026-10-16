"""coHound — базовый разведочный инференс пакета категории (итерация I1).

Модуль реализует **только эвристики** из ``§1.5`` (архетип) и черновую сборку
наследования базовой линии. Сети здесь нет: на вход подаётся уже собранный словарь
профиля (результат ручного/внешнего анализа), на выходе — структуры-предложения.

Ключевые инварианты
-------------------
* **A3**: эвристика — это assumption/кандидат, а не измерение. :func:`infer_archetype` и
  :func:`compose_baselines` **ничего не пишут** и не одобряют автоматически; запись —
  только явными CRUD-операциями (:mod:`core.pipeline.package_admin`, I2 добавит ``apply``).
* **§1.3**: нейтрально к предметной категории — никаких названий продуктов/вендоров.
* Слой ``pipeline`` без ``ports`` на этом шаге (I1 без сети).

Ожидаемые ключи ``profile`` (все опциональны; неизвестные игнорируются)::

    upstream_name        # явный общий OSS-upstream/лидер (str)
    upstream_shared      # один upstream обслуживает несколько продуктов (bool)
    alternatives         # несколько OSS-альтернатив (list[str])
    commercial_derivatives  # коммерческие производные (list[str] | int)
    cross_line           # кросс-линейный набор (bool)
    upstream_id          # идентификатор upstream-участника базовой линии (str)
    params               # {param_id: {min_version, inheritance_mode, note}} (dict)
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from core.domain.enums import Archetype, InheritanceMode
from core.domain.models import BaselineRow, CatalogRow
from core.util.security import sanitize_slug

__all__ = [
    "ArchetypeHypothesis",
    "BaselineProposal",
    "infer_archetype",
    "compose_baselines",
    "suggest_modules",
    "suggest_roles",
    "suggest_cohort_members",
]

#: Уровень уверенности, ниже которого предложение требует человеческого решения.
_NEEDS_HUMAN_THRESHOLD = 0.75

#: Дефолтный вес модуля-кандидата (assumption; уточняется человеком, A3).
_DEFAULT_MODULE_WEIGHT = 10.0


@dataclass
class ArchetypeHypothesis:
    """Гипотеза архетипа категории (§1.5): ``A`` | ``B`` | ``C`` | ``None``.

    :param archetype: предложенный архетип или ``None``, если данных нет.
    :param confidence: эвристическая уверенность ∈ [0, 1] (assumption, не измерение, A3).
    :param signals: сработавшие признаки профиля (человекочитаемо).
    :param rationale: обоснование гипотезы.
    :param source: происхождение (здесь всегда ``heuristic``).
    :param needs_human: требуется ли человеческое решение (нет данных / низкая уверенность).
    """

    archetype: Archetype | None
    confidence: float
    signals: list[str] = field(default_factory=list)
    rationale: str = ""
    source: str = "heuristic"
    needs_human: bool = True

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (``archetype`` → строка/``None``)."""
        return {
            "archetype": self.archetype.value if self.archetype is not None else None,
            "confidence": self.confidence,
            "signals": list(self.signals),
            "rationale": self.rationale,
            "source": self.source,
            "needs_human": self.needs_human,
        }


@dataclass
class BaselineProposal:
    """Предложение базовой линии: кандидаты :class:`BaselineRow` (не запись).

    :param baseline_id: предложенный slug базовой линии (из имени upstream).
    :param baseline_participant: предложенный ``product_id`` участника базовой линии.
    :param rows: кандидаты строк наследования.
    :param notes: человекочитаемые заметки/пропуски.
    :param needs_human: всегда ``True`` в I1 (кандидаты, не auto-approve, A3).
    """

    baseline_id: str
    baseline_participant: str
    rows: list[BaselineRow] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    needs_human: bool = True

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление (строки — через ``model_dump(mode="json")``)."""
        return {
            "baseline_id": self.baseline_id,
            "baseline_participant": self.baseline_participant,
            "rows": [row.model_dump(mode="json") for row in self.rows],
            "notes": list(self.notes),
            "needs_human": self.needs_human,
        }


def _as_list(value: Any) -> list[str]:
    """Приводит значение к списку непустых строк (``None``/скаляр → ``[]``/``[скаляр]``)."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Mapping):
        return [str(key) for key in value]
    try:
        items = list(value)
    except TypeError:
        return [str(value)]
    return [str(item) for item in items if str(item).strip()]


def _first(profile: Mapping[str, Any], *keys: str) -> Any:
    """Первое непустое значение профиля по списку ключей-синонимов."""
    for key in keys:
        value = profile.get(key)
        if value not in (None, "", [], {}, 0):
            return value
    return None


def infer_archetype(profile: dict[str, Any]) -> ArchetypeHypothesis:
    """Эвристика архетипа категории (§1.5). Ничего не пишет (A3).

    Правила:

    * явный общий OSS-upstream/лидер → **A**;
    * несколько OSS-альтернатив **и** (коммерческие производные / кросс-линейность) → **B**;
    * несколько OSS-альтернатив без общего лидера (или иные данные без лидера) → **C**;
    * данных нет → ``archetype=None, confidence=0, needs_human=True``.
    """
    if not isinstance(profile, Mapping) or not profile:
        return ArchetypeHypothesis(
            archetype=None,
            confidence=0.0,
            signals=[],
            rationale="данных профиля нет — архетип не определён",
            needs_human=True,
        )

    upstream = _first(profile, "upstream_name", "upstream", "leader", "oss_upstream")
    shared = bool(profile.get("upstream_shared"))
    alternatives = _as_list(_first(profile, "alternatives", "oss_alternatives"))
    derivatives = _first(profile, "commercial_derivatives", "derivatives", "commercial")
    cross_line = bool(profile.get("cross_line") or profile.get("cross_linear"))

    if upstream:
        signals = [f"upstream: {upstream}"]
        confidence = 0.9 if shared else 0.8
        if shared:
            signals.append("upstream общий для нескольких продуктов")
        if alternatives:
            signals.append(f"OSS-альтернативы: {', '.join(alternatives)}")
        return ArchetypeHypothesis(
            archetype=Archetype.A,
            confidence=confidence,
            signals=signals,
            rationale="явный общий OSS-upstream/лидер → архетип A",
            needs_human=confidence < _NEEDS_HUMAN_THRESHOLD,
        )

    if alternatives and (derivatives or cross_line):
        signals = [f"OSS-альтернативы: {', '.join(alternatives)}"]
        if derivatives:
            signals.append("есть коммерческие производные")
        if cross_line:
            signals.append("кросс-линейный набор продуктов")
        return ArchetypeHypothesis(
            archetype=Archetype.B,
            confidence=0.7,
            signals=signals,
            rationale="несколько OSS-альтернатив + производные/кросс-линейность → архетип B",
            needs_human=True,
        )

    if alternatives or cross_line or derivatives:
        signals = []
        if alternatives:
            signals.append(f"OSS-альтернативы: {', '.join(alternatives)}")
        if cross_line:
            signals.append("кросс-линейный набор продуктов")
        if derivatives:
            signals.append("есть коммерческие производные")
        return ArchetypeHypothesis(
            archetype=Archetype.C,
            confidence=0.6,
            signals=signals,
            rationale="нет явного общего OSS-лидера → архетип C",
            needs_human=True,
        )

    return ArchetypeHypothesis(
        archetype=Archetype.C,
        confidence=0.4,
        signals=[],
        rationale="данные есть, но признаков лидера/альтернатив нет — предварительно C",
        needs_human=True,
    )


def _param_ids(catalog: Sequence[CatalogRow], scope: Any) -> list[str]:
    """Отбирает ``param_id`` каталога по ``scope`` (``"all"`` | коллекция | модуль)."""
    ordered = sorted({row.param_id for row in catalog})
    if scope in (None, "all"):
        return ordered
    if isinstance(scope, str):
        wanted = [scope]
    else:
        wanted = [str(item) for item in scope]
    wanted_set = set(wanted)
    # Если scope не пересекается с param_id, трактуем его как имя модуля.
    if wanted_set & set(ordered):
        return [param_id for param_id in ordered if param_id in wanted_set]
    modules = {row.module for row in catalog if row.module in wanted_set}
    if modules:
        return sorted({row.param_id for row in catalog if row.module in modules})
    return [param_id for param_id in ordered if param_id in wanted_set]


def _param_hint(profile: Mapping[str, Any], param_id: str) -> Mapping[str, Any]:
    """Подсказка профиля по конкретному ``param_id`` (пустой словарь, если нет)."""
    params = profile.get("params")
    if isinstance(params, Mapping):
        entry = params.get(param_id)
        if isinstance(entry, Mapping):
            return entry
    return {}


def compose_baselines(
    profile: dict[str, Any],
    catalog: Sequence[CatalogRow],
    *,
    scope: Any = "all",
    unreferenced: str = "skip",
) -> BaselineProposal:
    """Собирает черновик наследования базовой линии. Ничего не пишет (A3).

    ``baseline_id`` — slug имени upstream; ``baseline_participant`` — ``upstream_id``
    (или slug имени). Для каждого ``param_id`` из ``scope`` формируется кандидат
    :class:`BaselineRow`; параметры без референса в профиле обрабатываются политикой
    ``unreferenced``:

    * ``"skip"`` (дефолт) — кросс-линейный случай: строку не предлагаем;
    * ``"direct"`` / ``"conditional"`` — предлагаем строку с соответствующим режимом.

    :raises ValueError: неизвестная политика ``unreferenced``.
    """
    if unreferenced not in ("skip", "direct", "conditional"):
        raise ValueError(
            "unreferenced должен быть одним из: skip | direct | conditional; "
            f"получено {unreferenced!r}"
        )
    profile = profile if isinstance(profile, Mapping) else {}
    upstream_name = _first(profile, "upstream_name", "upstream", "leader", "oss_upstream")
    upstream_id = profile.get("upstream_id")

    if not upstream_name:
        return BaselineProposal(
            baseline_id="",
            baseline_participant=str(upstream_id or ""),
            rows=[],
            notes=["в профиле нет upstream_name — базовая линия не определена"],
            needs_human=True,
        )

    baseline_id = sanitize_slug(str(upstream_name).lower())
    participant = str(upstream_id) if upstream_id else baseline_id
    proposals: list[BaselineRow] = []
    notes: list[str] = []

    for param_id in _param_ids(catalog, scope):
        hint = _param_hint(profile, param_id)
        if hint:
            mode_raw = hint.get("inheritance_mode", InheritanceMode.DIRECT.value)
            try:
                mode = InheritanceMode(str(mode_raw))
            except ValueError as exc:
                raise ValueError(
                    f"params[{param_id}].inheritance_mode: неизвестное значение {mode_raw!r}"
                ) from exc
            proposals.append(
                BaselineRow(
                    baseline_id=baseline_id,
                    param_id=param_id,
                    min_version=(str(hint["min_version"]) if hint.get("min_version") else None),
                    inheritance_mode=mode,
                    note=(str(hint["note"]) if hint.get("note") else None),
                )
            )
            continue

        if unreferenced == "skip":
            notes.append(f"{param_id}: нет референса в профиле — пропущен (кросс-линейный)")
            continue
        mode = (
            InheritanceMode.DIRECT
            if unreferenced == "direct"
            else InheritanceMode.CONDITIONAL
        )
        proposals.append(
            BaselineRow(
                baseline_id=baseline_id,
                param_id=param_id,
                min_version=None,
                inheritance_mode=mode,
                note="unreferenced policy (черновик)",
            )
        )

    return BaselineProposal(
        baseline_id=baseline_id,
        baseline_participant=participant,
        rows=proposals,
        notes=notes,
        needs_human=True,
    )


# ---------------------------------------------------------------------------
# Suggest-* (лёгкие эвристики профиля → Candidate; ничего не пишут)
# ---------------------------------------------------------------------------


def _spec_list(raw: Any) -> list[dict[str, Any]]:
    """Нормализует вход профиля (список/dict/строка) в список словарей-спецификаций.

    Поддерживаются формы: ``["a", {"id": "b", ...}]``, ``{"id": {...}}`` и скаляр.
    Каждый элемент несёт ключ ``id`` (или собственные поля).
    """
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        out: list[dict[str, Any]] = []
        for key, value in raw.items():
            if isinstance(value, Mapping):
                out.append({"id": str(key), **{str(k): v for k, v in value.items()}})
            else:
                out.append({"id": str(key), "value": value})
        return out
    if isinstance(raw, str):
        return [{"id": raw}]
    try:
        items = list(raw)
    except TypeError:
        return [{"id": str(raw)}]
    out = []
    for item in items:
        if isinstance(item, Mapping):
            out.append({str(k): v for k, v in item.items()})
        else:
            out.append({"id": str(item)})
    return out


def _candidate(**kwargs: Any) -> Any:
    """Ленивая фабрика :class:`~core.pipeline.package_candidates.Candidate`.

    Локальный импорт разрывает цикл модулей (``package_candidates`` импортирует этот
    модуль на уровне модуля; обратный импорт — только по вызову).
    """
    from core.pipeline.package_candidates import Candidate

    return Candidate(**kwargs)


def _spec_id(spec: Mapping[str, Any], *keys: str) -> str:
    """Первый непустой идентификатор спецификации по списку ключей-синонимов."""
    for key in (*keys, "id", "name"):
        value = spec.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def suggest_modules(profile: dict[str, Any]) -> list[Any]:
    """Предлагает модули из ``profile`` (ключи ``modules``/``module_hints``). Ничего не пишет.

    Формы значения: ``["m1", ...]``, ``{"m1": 30}``, ``[{"module_id": "m1", "weight": 30}]``.
    Вес по умолчанию — :data:`_DEFAULT_MODULE_WEIGHT` (assumption; ``needs_human=True``).
    """
    profile = profile if isinstance(profile, Mapping) else {}
    result: list[Any] = []
    seen: set[str] = set()
    for spec in _spec_list(_first(profile, "modules", "module_hints", "module_candidates")):
        module_id = _spec_id(spec, "module_id")
        if not module_id or module_id in seen:
            continue
        seen.add(module_id)
        raw_weight = spec.get("weight", spec.get("value", _DEFAULT_MODULE_WEIGHT))
        try:
            weight = float(raw_weight)
        except (TypeError, ValueError):
            weight = _DEFAULT_MODULE_WEIGHT
        result.append(
            _candidate(
                kind="module",
                payload={"module_id": module_id, "weight": weight},
                confidence=0.5,
                rationale=f"модуль из профиля: {module_id}",
                source="heuristic",
                needs_human=True,
            )
        )
    return result


def suggest_roles(profile: dict[str, Any]) -> list[Any]:
    """Предлагает роли из ``profile`` (ключи ``roles``/``role_hints``). Ничего не пишет."""
    profile = profile if isinstance(profile, Mapping) else {}
    result: list[Any] = []
    seen: set[str] = set()
    for spec in _spec_list(_first(profile, "roles", "role_hints", "role_candidates")):
        role_id = _spec_id(spec, "role_id")
        if not role_id or role_id in seen:
            continue
        seen.add(role_id)
        payload: dict[str, Any] = {"role_id": role_id}
        # A3: обоснование/описание — только из профиля. Нет данных — поле не выдумывается
        # (кандидат остаётся неполным; заполнит человек через явную CRUD-операцию).
        definition = spec.get("definition")
        if definition is not None:
            payload["definition"] = str(definition)
        for key in ("markers", "query_patterns", "core_coverage_allowed", "multi_instance"):
            if key in spec:
                payload[key] = spec[key]
        result.append(
            _candidate(
                kind="role",
                payload=payload,
                confidence=0.5,
                rationale=f"роль из профиля: {role_id}",
                source="heuristic",
                needs_human=True,
            )
        )
    return result


def suggest_cohort_members(profile: dict[str, Any]) -> list[Any]:
    """Предлагает участников когорты из ``profile`` (``cohort_members``/``cohort``/``products``).

    Новые участники по умолчанию — ``anchor_relation="analog"`` (якорь уже задан когортой,
    §1.6). Ничего не пишет; ``needs_human=True``.
    """
    profile = profile if isinstance(profile, Mapping) else {}
    result: list[Any] = []
    seen: set[str] = set()
    raw = _first(profile, "cohort_members", "cohort", "products")
    for spec in _spec_list(raw):
        product_id = _spec_id(spec, "product_id")
        if not product_id or product_id in seen:
            continue
        seen.add(product_id)
        payload: dict[str, Any] = {
            "product_id": product_id,
            "role_overlap": spec.get("role_overlap", 0.5),
            "anchor_relation": str(spec.get("anchor_relation", "analog")),
        }
        # A3: обоснование выбора — только из профиля. Нет данных — поле не выдумывается.
        if spec.get("selection_reason") is not None:
            payload["selection_reason"] = str(spec["selection_reason"])
        result.append(
            _candidate(
                kind="cohort_member",
                payload=payload,
                confidence=0.5,
                rationale=f"участник когорты из профиля: {product_id}",
                source="heuristic",
                needs_human=True,
            )
        )
    return result
