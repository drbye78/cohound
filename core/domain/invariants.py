"""coHound — инварианты домена (§2.3): чистые проверки, возвращающие ``Violation``.

T2.2: логика вынесена из валидаторов Pydantic (``Cell._enforce_invariants``,
``CellEvidence._check``, ``Satellite._check``) в чистые функции. Нарушение инварианта —
это **значение**, а не исключение: функции возвращают типизированные ``Violation``,
чтобы pipeline, store-слой и selfcheck переиспользовали одни и те же правила без
Pydantic (см. docs/architecture/01-modules-and-contracts.md §2.3).

Инварианты — это код, а не комментарии (AGENTS.md). Модуль категорийно нейтрален
(§1.3): без предметных терминов и без I/O. Комментарии/докстринги — на русском.
"""

from __future__ import annotations

from core.domain.enums import (
    AbsenceRuleType,
    CellState,
    EvidenceType,
    InheritanceMode,
    SatelliteState,
    SourceKind,
    SourceScope,
    Status,
)
from core.domain.models import (
    BaselineRow,
    Cell,
    CellEvidence,
    Satellite,
    UpstreamRef,
    Violation,
)

# Контракт derived-evidence (K5) допускает два пути:
#   * наследование — ``source_kind: inheritance``;
#   * сателлитный путь — документы сателлита индексируются в партицию продукта-владельца,
#     поэтому их evidence имеет ``source_kind: documentation``.
_DERIVED_SOURCE_KINDS: frozenset[SourceKind] = frozenset(
    {SourceKind.INHERITANCE, SourceKind.DOCUMENTATION}
)

# Корпусные источники: внешний контент становится evidence только после
# corpus_raw → extract → chunk → content_hash → index (I19).
_CORPUS_SOURCE_KINDS: frozenset[SourceKind] = frozenset(
    {SourceKind.DOCUMENTATION, SourceKind.WEB, SourceKind.DISTRIBUTION}
)


def _violation(code: str, message: str, field: str | None = None) -> Violation:
    """Собрать ``Violation`` — машинный ``code`` + человекочитаемое сообщение."""
    return Violation(code=code, message=message, field=field)


def _has_exhaustive_scope(absences: list[dict[str, object]]) -> bool:
    """Есть ли среди absence-evidence запись со ``source_scope: exhaustive``."""
    return any(e.get("source_scope") == SourceScope.EXHAUSTIVE for e in absences)


def check_cell(cell: Cell) -> list[Violation]:
    """Инварианты ячейки: I1, I2, I2b, I4, I6, I7, I13 (§2.3).

    Зеркалит ``Cell._enforce_invariants``, но не возбуждает исключений — возвращает
    список нарушений. Пустой список означает, что инварианты выполнены.
    """
    violations: list[Violation] = []

    # I4: failed → status = null (failed никогда не несёт статус).
    if cell.state is CellState.FAILED and cell.status is not None:
        violations.append(
            _violation("I4", "I4: failed → status обязан быть null", "status")
        )

    # I13: preliminary исключена из Capability/Coverage/публикации.
    if cell.state is CellState.PRELIMINARY and cell.status is not None:
        violations.append(
            _violation(
                "I13",
                "I13: preliminary-ячейка не имеет разрешённого status",
                "status",
            )
        )

    # I6/I7: N/A — только человеком (reviewed_by), никогда автоматически.
    if cell.status is Status.NOT_APPLICABLE and not cell.reviewed_by:
        violations.append(
            _violation("I7", "I6/I7: N/A → reviewed_by обязателен", "reviewed_by")
        )

    if cell.state is CellState.EVALUATED:
        # I1: evaluated ✅/⚠️ → evidence ≥ 1.
        if cell.status in {Status.OK, Status.PARTIAL} and not cell.evidence:
            violations.append(
                _violation("I1", "I1: evaluated ✅/⚠️ → evidence ≥ 1", "evidence")
            )

        # I2/I2b: evaluated ❌ → absence-evidence по absence_rule параметра.
        if cell.status is Status.ABSENT:
            if not cell.absence_evidence:
                # Нет absence-evidence — I2; scope проверять не на чем (I2b не добавляем).
                violations.append(
                    _violation(
                        "I2",
                        "I2: evaluated ❌ → absence-evidence обязателен",
                        "absence_evidence",
                    )
                )
            elif cell.absence_rule in {
                AbsenceRuleType.PARAMETER_REFERENCE,
                AbsenceRuleType.DISTRIBUTION_ABSENCE,
            }:
                # I2b: исчерпанность справочника должна быть подтверждена человеком.
                has_attested = any(
                    e.get("source_scope") == SourceScope.EXHAUSTIVE
                    and bool(e.get("scope_attested_by"))
                    for e in cell.absence_evidence
                )
                if not has_attested:
                    violations.append(
                        _violation(
                            "I2b",
                            "I2b: parameter_reference/distribution_absence требуют "
                            "source_scope=exhaustive и scope_attested_by",
                            "absence_evidence",
                        )
                    )
            elif cell.absence_rule is AbsenceRuleType.REGISTRY_ABSENCE:
                # I2b: реестр — авторитетная энумерация, exhaustive по построению;
                # scope_attested_by не требуется.
                if not _has_exhaustive_scope(cell.absence_evidence):
                    violations.append(
                        _violation(
                            "I2b",
                            "I2b: registry_absence — exhaustive по построению",
                            "absence_evidence",
                        )
                    )

    return violations


def check_evidence(ev: CellEvidence, cell: Cell | None = None) -> list[Violation]:
    """Инварианты evidence: I10/I15, I19, I22 (§2.3).

    ``cell`` — необязательный контекст (для сообщения); проверки выполняются по самому
    ``ev``. I22: отсутствие цитаты — не доказательство наличия (главный инвариант).
    """
    violations: list[Violation] = []

    # I10/I15: evidence обязан ссылаться на резолвимый источник.
    if not ev.source_id:
        violations.append(
            _violation("I10", "I10/I15: evidence обязан иметь source_id", "source_id")
        )

    # I19: внешний/корпусный контент обязан нести content_hash.
    if ev.source_kind in _CORPUS_SOURCE_KINDS and not ev.content_hash:
        violations.append(
            _violation(
                "I19",
                "I19: корпусный evidence обязан иметь content_hash",
                "content_hash",
            )
        )

    # I22: прямая претензия на наличие обязана опираться на непустую цитату.
    # Ручное (manual) evidence — исключение: носитель человеческой аттестации.
    if ev.evidence_type is EvidenceType.DIRECT and ev.source_kind is not SourceKind.MANUAL:
        quote = ev.citation_quote or ""
        if not quote.strip():
            where = f" ({cell.product_id}/{cell.param_id})" if cell is not None else ""
            violations.append(
                _violation(
                    "I22",
                    f"I22: прямая претензия на наличие обязана нести непустую "
                    f"citation_quote{where}",
                    "citation_quote",
                )
            )

    return violations


def check_inheritance(
    cell: Cell,
    baseline: BaselineRow,
    evidence: list[CellEvidence] | None = None,
) -> list[Violation]:
    """Инвариант I8 + derived-evidence для наследования (K5, §4.2 регламента).

    * ``blocked`` — ячейка не должна нести унаследованный статус (материализованное
      наследование ``source_kind: inheritance`` при ``state: evaluated``);
    * ``direct`` — унаследованный ✅/⚠️ обязан нести ≥1 derived-evidence
      (``evidence_type: derived`` + ``source_kind: inheritance`` + ``source_id``).

    ``evidence=None`` трактуется как «derived-evidence не предоставлено» (fail-closed):
    для прямого унаследованного ✅/⚠️ это нарушение I8.
    """
    violations: list[Violation] = []
    inherited = cell.source_kind is SourceKind.INHERITANCE

    # I8: заблокированный upstream-факт не переносится.
    if baseline.inheritance_mode is InheritanceMode.BLOCKED:
        if cell.state is CellState.EVALUATED and inherited:
            violations.append(
                _violation(
                    "I8",
                    "I8: inheritance_mode=blocked — унаследованный статус недопустим",
                    "source_kind",
                )
            )
        return violations

    # I8: прямой унаследованный ✅/⚠️ обязан опираться на derived-evidence.
    if baseline.inheritance_mode is InheritanceMode.DIRECT:
        resolves_inherited = (
            cell.state is CellState.EVALUATED
            and inherited
            and cell.status in {Status.OK, Status.PARTIAL}
        )
        if resolves_inherited:
            evs = evidence or []
            has_derived = any(
                e.evidence_type is EvidenceType.DERIVED
                and e.source_kind is SourceKind.INHERITANCE
                and bool(e.source_id)
                for e in evs
            )
            if not has_derived:
                violations.append(
                    _violation(
                        "I8",
                        "I8: унаследованный ✅/⚠️ обязан нести derived-evidence "
                        "(evidence_type=derived, source_kind=inheritance, source_id)",
                        "evidence",
                    )
                )

    return violations


def check_derived_evidence(ev: CellEvidence, upstream: UpstreamRef) -> list[Violation]:
    """Контракт derived-evidence для наследования и сателлитов (K5).

    Проверяет: ``evidence_type: derived`` (I1), допустимый ``source_kind`` (I8),
    непустой ``source_id`` (I10), резолвимость ``source_url`` (I15),
    ``content_hash`` и его согласованность с upstream (I19).
    """
    violations: list[Violation] = []

    # I1: производное доказательство должно быть производного типа.
    if ev.evidence_type is not EvidenceType.DERIVED:
        violations.append(
            _violation(
                "I1",
                "I1: derived-evidence обязан иметь evidence_type=derived",
                "evidence_type",
            )
        )

    # I8: допустимы только inheritance или сателлитный (documentation) путь.
    if ev.source_kind not in _DERIVED_SOURCE_KINDS:
        violations.append(
            _violation(
                "I8",
                "I8: derived-evidence допускает только inheritance или "
                "сателлитный путь (documentation)",
                "source_kind",
            )
        )

    # I10: носитель ссылки обязателен.
    if not ev.source_id:
        violations.append(
            _violation(
                "I10", "I10: derived-evidence обязан иметь source_id", "source_id"
            )
        )

    # I15: источник обязан резолвиться в URL (свой или upstream).
    if not (ev.source_url or upstream.source_url):
        violations.append(
            _violation(
                "I15",
                "I15: derived-evidence обязан резолвиться в source_url",
                "source_url",
            )
        )

    # I19: hash обязателен и не должен расходиться с upstream.
    if not ev.content_hash:
        violations.append(
            _violation(
                "I19", "I19: derived-evidence обязан иметь content_hash", "content_hash"
            )
        )
    elif upstream.content_hash and ev.content_hash != upstream.content_hash:
        violations.append(
            _violation(
                "I19",
                "I19: content_hash derived-evidence расходится с upstream",
                "content_hash",
            )
        )

    return violations


def check_satellite(sat: Satellite) -> list[Violation]:
    """Инвариант I9: установленный сателлит обязан нести relationship evidence.

    Для ``approved``/``edited_by_human`` требуются ``component``, ``origin``,
    ``relationship``, ``evidence_state`` и непустой ``relationship_evidence``.
    """
    if sat.state not in {SatelliteState.APPROVED, SatelliteState.EDITED_BY_HUMAN}:
        return []

    missing = [
        field
        for field in ("component", "origin", "relationship", "evidence_state")
        if getattr(sat, field) is None
    ]
    if sat.relationship_evidence is None:
        missing.append("relationship_evidence")

    if missing:
        return [
            _violation(
                "I9",
                "I9: установленный сателлит требует relationship evidence; "
                f"не заданы: {', '.join(missing)}",
                missing[0],
            )
        ]
    return []
