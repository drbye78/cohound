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

from collections.abc import Sequence

from core.domain.enums import (
    ABSENT_DENIAL_STRENGTHS,
    UNKNOWN_DENIAL_STRENGTHS,
    CellState,
    DenialStrength,
    EvidenceType,
    InheritanceMode,
    SatelliteState,
    SourceKind,
    Status,
)
from core.domain.models import (
    BaselineRow,
    Cell,
    CellEvidence,
    CorpusCoverage,
    EvidenceScope,
    ProtocolExhaustion,
    Satellite,
    UpstreamRef,
    Violation,
)

#: Исторический перечень контуров протокола исчерпания (``corpus/serp/registry/inheritance``).
#:
#: **Не инвариант.** Прежняя редакция I3 требовала все четыре контура для strict-``❓``, то
#: есть задавала рецепт реализации: параметр, который нельзя доказать поиском, не должен
#: требовать SERP для честного «не установлено». Теперь ``❓`` обосновывается классом
#: отрицания (``unobserved``/``not_stated``) и его причиной, а перечень остаётся как
#: справочник того, что вообще наблюдается: протокол исчерпания — провенанс поиска
#: (``core.pipeline.exhaustion``), а не условие вердикта.
EXHAUSTION_CONTOURS: tuple[str, ...] = ("corpus", "serp", "registry", "inheritance")

#: Deprecated-алиас :data:`EXHAUSTION_CONTOURS` (публичный символ домена; читается
#: отчётом и старыми вызовами). Смысл изменился: список контуров больше не требование.
REQUIRED_EXHAUSTION_CONTOURS: tuple[str, ...] = EXHAUSTION_CONTOURS

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


def check_unknown_exhaustion(
    cell: Cell,
    exhaustion: ProtocolExhaustion | None = None,
    *,
    required: Sequence[str] | None = None,
) -> list[Violation]:
    """I3 (Вариант 2): ``❓`` обязан **назвать причину незнания** — и всё.

    Прежняя редакция требовала «полный протокол исчерпания» (``corpus``+``serp``+
    ``registry``+``inheritance``). Это был рецепт реализации, а не инвариант: он делал
    честное ``❓`` недостижимым для параметра, который нельзя доказать поиском (нужен
    SERP-контур, которого у параметра нет), и при этом ничего не говорил о том, **почему**
    отсутствие не установлено. Теперь причина — свойство самой ячейки: класс
    ``unobserved``/``not_stated`` и непустая ``reason`` в :class:`EvidenceScope`.

    Дизъюнкция сохраняется: человеческое решение (``reviewed_by``) само по себе
    удовлетворяет I3 (регламент §2.3).

    **Исторические строки.** Ячейка без объекта (записана до варианта 2) проверяется
    прежним правилом по ``exhaustion``: удалить это нельзя — старые прогоны обязаны
    остаться читаемыми и честно диагностируемыми. Новый писатель всегда несёт объект.

    :param exhaustion: протокол исчерпания (провенанс поиска). Используется **только**
        для исторических строк без объекта.
    :param required: deprecated: перечень контуров больше не требуется (принят ради
        совместимости вызовов); игнорируется.
    """
    if cell.reviewed_by:
        return []
    scope = cell.evidence_scope
    if scope is not None:
        if scope.denial_strength in UNKNOWN_DENIAL_STRENGTHS and (
            scope.reason or ""
        ).strip():
            return []
        return [
            _violation(
                "I3",
                "I3: ❓ требует denial_strength ∈ "
                f"{sorted(s.value for s in UNKNOWN_DENIAL_STRENGTHS)} и непустую reason; "
                f"получено {scope.denial_strength.value!r} "
                f"(reason={scope.reason!r})",
                "evidence_scope",
            )
        ]
    # -- историческая строка без объекта: прежнее правило -----------------------
    if exhaustion is None:
        return [
            _violation(
                "I3",
                "I3: strict-❓ без причины незнания (нет evidence_scope и нет "
                "протокола исчерпания)",
                "evidence_scope",
            )
        ]
    if exhaustion.termination_reason is None:
        return [
            _violation(
                "I3",
                "I3: протокол исчерпания без termination_reason",
                "termination_reason",
            )
        ]
    return []


def _scope_violations(scope: EvidenceScope, field: str = "evidence_scope") -> list[Violation]:
    """I2/I2b (Вариант 2): требования **внутри** объекта силы отрицания.

    Зеркалит валидатор :class:`EvidenceScope` для ячеек, собранных ``model_construct``
    (чтение из store) и для диагностики: каждое требование класса — именованное нарушение,
    а не исключение.

    * ``explicit`` → непустая цитата (отрицание без цитаты — мнение);
    * ``bounded`` → причина **и** принятие риска человеком;
    * ``unobserved``/``not_stated``/``instrument_failed`` → непустая причина;
    * ``authoritative`` несовместим с заведомо неполной энумерацией.
    """
    violations: list[Violation] = []
    strength = scope.denial_strength
    if strength is DenialStrength.EXPLICIT and not (scope.citation_quote or "").strip():
        violations.append(
            _violation(
                "I2b",
                "I2b: explicit-отрицание обязано нести непустую citation_quote",
                field,
            )
        )
    requires_reason = {
        DenialStrength.UNOBSERVED,
        DenialStrength.NOT_STATED,
        DenialStrength.BOUNDED,
        DenialStrength.INSTRUMENT_FAILED,
    }
    if strength in requires_reason and not (scope.reason or "").strip():
        violations.append(
            _violation(
                "I2b",
                f"I2b: denial_strength={strength.value} требует непустую reason",
                field,
            )
        )
    if strength is DenialStrength.BOUNDED and not (scope.scope_acceptance or "").strip():
        violations.append(
            _violation(
                "I2b",
                "I2b: bounded-отрицание требует scope_acceptance (принятия риска): "
                "измерение не покрывает пространство",
                field,
            )
        )
    if strength is DenialStrength.AUTHORITATIVE and scope.enumerated is False:
        violations.append(
            _violation(
                "I2b",
                "I2b: authoritative несовместим с enumerated=False",
                field,
            )
        )
    return violations


def check_cell(cell: Cell) -> list[Violation]:
    """Инварианты ячейки: I1, I2, I4, I6, I7, I13 (§2.3).

    I2/I2b/I2c/I3 (бывшие три независимые блокировки) сведены к **одному** механизму:
    вопрос «можно ли утверждать отсутствие» решает заполненность
    :class:`~core.domain.models.EvidenceScope`:

    * ``❌`` → класс ``authoritative|explicit|bounded`` (+ требования класса, :func:`_scope_violations`);
    * ``❓`` → класс ``unobserved|not_stated`` и непустая причина (:func:`check_unknown_exhaustion`);
    * ``failed`` → класс ``instrument_failed`` (I4: статуса нет).

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

    scope = cell.evidence_scope
    if scope is not None:
        violations.extend(_scope_violations(scope))

    if cell.state is CellState.EVALUATED:
        # I1: evaluated ✅/⚠️ → evidence ≥ 1.
        if cell.status in {Status.OK, Status.PARTIAL} and not cell.evidence:
            violations.append(
                _violation("I1", "I1: evaluated ✅/⚠️ → evidence ≥ 1", "evidence")
            )

        # I2: evaluated ❌ → сила отрицания достаточна, чтобы публиковать «нет».
        if cell.status is Status.ABSENT:
            if scope is None:
                # Историческая строка (до варианта 2) либо ячейка, собранная не тем
                # путём: вердикт без силы отрицания недоказуем.
                violations.append(
                    _violation(
                        "I2",
                        "I2: evaluated ❌ требует evidence_scope с denial_strength ∈ "
                        f"{sorted(s.value for s in ABSENT_DENIAL_STRENGTHS)}",
                        "evidence_scope",
                    )
                )
            elif scope.denial_strength not in ABSENT_DENIAL_STRENGTHS:
                violations.append(
                    _violation(
                        "I2",
                        "I2: ❌ требует denial_strength ∈ "
                        f"{sorted(s.value for s in ABSENT_DENIAL_STRENGTHS)}; "
                        f"получено {scope.denial_strength.value}",
                        "evidence_scope",
                    )
                )

    # I4: failed несёт только сбой прибора.
    if cell.state is CellState.FAILED and scope is not None:
        if scope.denial_strength is not DenialStrength.INSTRUMENT_FAILED:
            violations.append(
                _violation(
                    "I4",
                    "I4: failed несёт только denial_strength=instrument_failed",
                    "evidence_scope",
                )
            )

    return violations


def check_absence_coverage(
    cell: Cell, coverage: CorpusCoverage | None
) -> list[Violation]:
    """I2c (Вариант 2): класс отрицания согласован с **наблюдённым** охватом.

    Что изменилось. Прежняя редакция требовала человеческой подписи всякий раз, когда
    корпус неполон, — то есть делала вывод «отсутствие» зависимым от полноты краула даже
    тогда, когда вендор прямо отрицает свойство. Теперь условие зависит от **класса**:

    * ``explicit`` — это свидетельство вендора (цитата), а не вывод из полноты корпуса:
      неполнота охвата его не отменяет;
    * ``authoritative`` — авторитетная энумерация по построению: тоже не зависит;
    * ``bounded`` — зависит от пространства, поэтому требует ``scope_acceptance``
      (проверяется в :func:`_scope_violations`); при неполном охвате без принятия риска
      публиковать ``❌`` нельзя;
    * ``unobserved``/``not_stated`` — не дают ``❌`` вообще.

    ``coverage=None`` — записи покрытия нет (legacy/без краула): проверка неприменима.
    """
    if coverage is None or coverage.complete:
        return []
    if cell.state is not CellState.EVALUATED or cell.status is not Status.ABSENT:
        return []
    scope = cell.evidence_scope
    if scope is None:
        # Историческая строка: прежнее правило — принятие риска в absence-записи.
        # Читаются оба имени: колонка БД мигрируется при открытии, а in-memory словари
        # прошлых прогонов (и фикстур) несут прежний ключ ``scope_attested_by``.
        if any(
            entry.get("scope_acceptance") or entry.get("scope_attested_by")
            for entry in cell.absence_evidence
        ):
            return []
        return [
            _violation(
                "I2c",
                "I2c: ❌ без evidence_scope при неполном покрытии корпуса недоказуем "
                "(историческая строка без принятия риска); блокирующие сбои: "
                f"{dict(coverage.blocking_failures) or '{}'}, нечитаемые origin'ы: "
                f"{list(coverage.unreadable_origins) or '[]'}",
                "absence_evidence",
            )
        ]
    if scope.denial_strength in UNKNOWN_DENIAL_STRENGTHS:
        return [
            _violation(
                "I2c",
                "I2c: класс "
                f"{scope.denial_strength.value} не даёт ❌ — при неполном покрытии "
                "ячейка обязана быть ❓ с этой причиной; блокирующие сбои: "
                f"{dict(coverage.blocking_failures) or '{}'}, нечитаемые origin'ы: "
                f"{list(coverage.unreadable_origins) or '[]'}",
                "evidence_scope",
            )
        ]
    if scope.denial_strength is DenialStrength.BOUNDED and not (
        scope.scope_acceptance or ""
    ).strip():
        return [
            _violation(
                "I2c",
                "I2c: bounded при неполном покрытии требует scope_acceptance "
                "(принятия риска человеком); блокирующие сбои: "
                f"{dict(coverage.blocking_failures) or '{}'}",
                "evidence_scope",
            )
        ]
    return []


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
