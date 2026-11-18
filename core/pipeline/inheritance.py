"""Волна наследования — Ш4 (без LLM): перенос upstream-факта по ``baselines.csv`` (02 §D.2).

Детерминированная волна (детерминированное раньше поиска, §2.2 п.7). По строке
``baselines.csv`` и совпадению upstream-паспорта:

======  ===================================================================
mode    действие
======  ===================================================================
direct  upstream-факт переносится как статус (✅) — **при** несущем
        derived-evidence (I8/K5): ``evidence_type=derived``,
        ``source_kind=inheritance``, резолв ``source_url`` (I10/I15) и
        ``content_hash`` (I19). Контракт проверяется
        :func:`core.domain.invariants.check_inheritance` /
        :func:`core.domain.invariants.check_derived_evidence`; нарушение →
        ячейка не выпускается, ошибка собирается в ``errors``.
conditional
        уходит в ревью/кандидат: ``state=pending``, ``status=None`` (НЕ
        разрешённый статус) — но **только** если у линии есть принятый факт
        ``present``: строка «без доказательства» не отменяет наследование от
        дальнего предка, где доказательство есть.
blocked upstream-факт не используется — пропуск (ячейка не производится).
======  ===================================================================

Нет строки ``baselines.csv`` = кросс-линейный параметр (архетип B) — здесь не
производится (только корпус, Ш5/Ш6).

**П>В** (§2.2 п.8): прямое доказательство Ш6 старше наследования. Противоречащий
прямой статус **понижает** унаследованный, но не перезаписывает его автоматически —
см. :func:`apply_precedence`: возвращается кандидат на ревью (``pending``).

**Наследование не доказывает downstream-факт** (регламент, стр. 288): оно переносит
возможность проверить применимость upstream-факта. Паспортная цитата у продукта одна,
поэтому унаследованная ячейка получает тот же замер «о предмете ли доказательство», что и
оценённая (:func:`core.pipeline.evaluate.support_subject_overlap`): постороннее
доказательство видно в ``validation_notes``/``review_reason``, но вердикт не меняется
(A3). Базис волны — :func:`inheritance_basis` — публикует «объявлено/выпущено» по
каждому режиму, чтобы ``direct`` и ``conditional`` не сливались в один итог.

Нейтрален (§1.3): нет названий продуктов/вендоров/реестров.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from core.domain import (
    BaselineFact,
    BaselineRow,
    CatalogRow,
    Cell,
    CellEvidence,
    CellState,
    EvidenceType,
    FactReview,
    FactStatus,
    InheritanceMode,
    LLMEvidence,
    Passport,
    SourceKind,
    Status,
    UpstreamRef,
    check_derived_evidence,
    check_inheritance,
    derived_evidence_id,
)
from core.pipeline.evaluate import (
    SUPPORT_STATE_UNRELATED,
    compose_review_reason,
    support_subject_overlap,
)
from core.pipeline.precedence import (
    CONDITIONAL_REVIEW_REASON,
    PRECEDENCE_REVIEW_REASON,
    apply_precedence,
    collect_protected_cells,
    resolve_precedence,
)
from core.pipeline.runner import PhaseResult

__all__ = [
    "InheritanceDecision",
    "apply_precedence",
    "decide_inheritance",
    "inheritance_basis",
    "inheritance_wave",
    "major_version",
]

# Машинные токены причин «не наследовали» (план 27 §6.2). Публикуются по продукту и
# параметру в ``inheritance_basis``: «объявлено 181, выпущено 140» без причин читается
# как дефект конвейера, а не как «41 факт ещё не разведан».
REASON_NO_POLICY_ROW = "no_policy_row"
REASON_MODE_BLOCKED = "mode_blocked"
REASON_MODE_CONDITIONAL = "mode_conditional"
REASON_BELOW_MIN_VERSION = "below_min_version"
REASON_VERSION_UNPARSABLE = "version_unparsable"
REASON_FACT_MISSING = "baseline_fact_missing"

#: Причина «факт есть, но не принят» — ``baseline_fact_pending``/``..._rejected``.
REASON_FACT_REVIEW = "baseline_fact_{review}"

#: Причина «ячейка наследования не выпущена: реестр (высокий источник) уже дал статус».
#: Параметр не наследован, хотя факт был и линия его описывает — и это не проседание к
#: следующему предку, а поглощение более высоким источником (Fork D).
REASON_SUPERSEDED_BY_REGISTRY = "superseded_by_registry"

#: Причина «факт принят, но статус не ``present``» — ``baseline_fact_absent``/``..._unclear``.
REASON_FACT_STATUS = "baseline_fact_{status}"


def _passport_index(passports: Any) -> dict[str, Passport]:
    """Индексирует паспорта по ``product_id`` (принимает mapping или итерируемое)."""
    if passports is None:
        return {}
    if isinstance(passports, Mapping):
        return {str(key): value for key, value in passports.items()}
    if isinstance(passports, Iterable):
        return {
            str(passport.product_id): passport
            for passport in passports
            if passport is not None
        }
    return {}


def _baseline_index(baselines: Iterable[BaselineRow]) -> dict[tuple[str, str], BaselineRow]:
    """Индексирует строки baseline по ``(baseline_id, param_id)``.

    Дублей ключа здесь не бывает: ``load_baselines`` отвергает повтор
    ``(baseline_id, param_id)`` как испорченный файл (две строки объявляли бы два режима
    одного параметра). Поэтому индекс — обычный словарь: тай-брейка по режиму нет и не
    должно быть, иначе «победил алфавит режима» стал бы решением, которого никто не
    принимал.
    """
    return {
        (row.baseline_id, row.param_id): row
        for row in baselines
        if row is not None
    }


def _fact_index(
    facts: Iterable[BaselineFact],
) -> dict[tuple[str, str, str], BaselineFact]:
    """Индексирует факты по ``(baseline_id, param_id, версия)`` (план 27 §6.2).

    Ключ включает **версию**: один параметр в v11 и v18 — два разных факта, и lookup
    обязан их различать (I15). При дублях остаётся первая по сортировке ключа; повторная
    запись того же ключа — дефект файла, и он виден в ``hound baseline facts``, а не
    разрешается молча последним вхождением.
    """
    ordered = sorted(
        (fact for fact in facts if fact is not None),
        key=lambda fact: (fact.baseline_id, fact.param_id, fact.version),
    )
    index: dict[tuple[str, str, str], BaselineFact] = {}
    for fact in ordered:
        index.setdefault((fact.baseline_id, fact.param_id, fact.version), fact)
    return index


def _numeric_version(value: str | None) -> tuple[int, ...] | None:
    """Числовые компоненты версии (``"16.2-beta1"`` → ``(16, 2, 1)``)."""
    if not value:
        return None
    parts = re.findall(r"\d+", str(value))
    return tuple(int(part) for part in parts) if parts else None


def _satisfies_min_version(version: str | None, min_version: str | None) -> bool:
    """Проверяет ``upstream.version >= min_version`` (мягко: нераспознанное — пропуск).

    ``min_version=None`` → всегда ``True``. Если хотя бы одна версия не содержит числовых
    компонент, сравнение невозможно: возвращается ``True`` (fail-open) — решение
    документируется вызывающим; жёсткая блокировка по недоказуемому сравнению была бы
    произволом (A3).
    """
    if min_version is None:
        return True
    current = _numeric_version(version)
    minimum = _numeric_version(min_version)
    if current is None or minimum is None:
        return True
    length = max(len(current), len(minimum))
    current = current + (0,) * (length - len(current))
    minimum = minimum + (0,) * (length - len(minimum))
    return current >= minimum


def major_version(value: str | None) -> str | None:
    """Мажорная версия из строки (``"18.4.1"`` → ``"18"``), ``None`` — не разбирается.

    План 27 §7.2: факт ищется **ровно на объявленной мажорной версии**, а не «где-нибудь
    ≤ V»: если в v18 параметр исчез, а в v17 был, наследовать нельзя — это регрессия
    upstream, и она обязана быть видна. Нераспознанная версия — линт-ошибка, а не
    «пропустим»: ``None`` здесь означает «сравнивать нечем», и вызывающий публикует
    причину, а не угадывает версию.
    """
    if not value:
        return None
    parts = re.findall(r"\d+", str(value))
    if not parts:
        return None
    return str(int(parts[0]))


def _upstream_source_id(baseline_id: str, param_id: str, version: str) -> str:
    """Нейтральный ``source_id`` факта upstream (K5, §4.2 регламента; план 27 §6.3).

    ``@version`` — не косметика: один параметр в v11 и v18 это **два разных факта**, и
    адрес обязан их различать (I15: детерминированный lookup). Без версии две записи
    доказательства схлопнулись бы в один ``source_id``.
    """
    return f"upstream://{baseline_id}#{param_id}@{version}"


def _derived_evidence(
    run_id: str,
    product_id: str,
    param_id: str,
    fact: BaselineFact,
) -> tuple[CellEvidence, UpstreamRef] | None:
    """Строит derived-evidence наследования из **факта** и upstream-ссылку (K5).

    Цитата, URL и хеш берутся у факта, а не у паспорта (план 27 §6.3): паспортная цитата
    у продукта одна на все параметры, поэтому 181 «✅» опирался на одну визитку «мы на
    базе X». Факт — это измерение конкретного места в документации конкретной версии.

    ``content_hash`` — хеш **текста чанка** (I19), записанный прибором, а не пересчитанный
    от URL+цитаты: адрес переезжает, текст — нет.

    ``None`` — если у факта нет цитаты или хеша (нарушение I1/I19): наследовать нечем.
    """
    if not (fact.quote or "").strip() or not (fact.content_hash or "").strip():
        return None
    source_id = _upstream_source_id(fact.baseline_id, param_id, fact.version)
    url = fact.url
    quote = fact.quote or ""
    digest = fact.content_hash
    evidence = CellEvidence(
        evidence_id=derived_evidence_id(
            run_id, product_id, param_id, source_id, SourceKind.INHERITANCE.value, 0
        ),
        run_id=run_id,
        product_id=product_id,
        param_id=param_id,
        source_kind=SourceKind.INHERITANCE,
        evidence_type=EvidenceType.DERIVED,
        source_id=source_id,
        source_url=url,
        citation_quote=quote,
        content_hash=digest,
    )
    upstream_ref = UpstreamRef(
        product_id=product_id,
        param_id=param_id,
        source_id=source_id,
        source_url=url,
        content_hash=digest,
    )
    return evidence, upstream_ref


def _annotate_support(cell: Cell, param: CatalogRow) -> Cell:
    """Пишет замер «о предмете ли доказательство» в унаследованную ячейку (K5/§4.2).

    Наследование переносит **возможность проверить применимость** upstream-факта, а не
    утверждение о downstream-параметре (регламент, стр. 288). Паспортная цитата у
    продукта одна, поэтому без замера 19 разных параметров выглядели бы подтверждёнными
    одной визиткой («мы на базе X»), и ни один из них не был бы подтверждён по существу.

    Замер — тот же, что у оценённой ячейки (:func:`support_subject_overlap`), без второй
    реализации и без копии стеммера. Что пишется:

    * ``validation_notes`` — строка ``support_not_about_subject: N/M …`` (только для
      ``unrelated_support``);
    * ``review_reason`` — та же строка **для ``✅``** и только в склейке с уже стоящей
      причиной (:func:`compose_review_reason`): ``conditional``-причина и конфликт
      источников не затираются;
    * вердикт не меняется (A3): это наблюдение, а не решение.

    ``no_quotes`` здесь не наша аномалия (унаследованный ``✅`` без цитаты запрещён I1/K5
    и отсекается :func:`check_derived_evidence`); ``supported`` и неизмеримый предмет
    молчат — заметки публикуют дефект, а не норму.
    """
    measured = support_subject_overlap(param, [ev.quote for ev in cell.evidence])
    if measured is None or measured.state != SUPPORT_STATE_UNRELATED:
        return cell
    note = measured.note()
    if note is None:  # pragma: no cover - защита от рассинхронизации state/note
        return cell
    notes = list(cell.validation_notes)
    if note in notes:
        # Повторный проход (resume, S8/R7) уже сказал это: не плодим записи и не
        # склеиваем причину саму с собой.
        return cell
    notes.append(note)
    reason = cell.review_reason
    if cell.status is Status.OK:
        reason = compose_review_reason(reason, note)
    return cell.model_copy(update={"validation_notes": notes, "review_reason": reason})


def _direct_cell(
    run_id: str,
    product_id: str,
    param: CatalogRow,
    baseline: BaselineRow,
    fact: BaselineFact,
    errors: list[str],
) -> tuple[Cell | None, CellEvidence | None]:
    """Строит унаследованную ✅-ячейку ``direct`` из факта; при нарушении K5 — ``(None, None)``.

    Возвращает ``(ячейка, derived-evidence)``. Ошибки инвариантов собираются в ``errors``.
    """
    param_id = str(param.param_id)
    built = _derived_evidence(run_id, product_id, param_id, fact)
    if built is None:
        errors.append(
            f"I8/I15: {product_id}/{param_id}: direct-наследование без цитаты/хеша факта "
            f"({fact.baseline_id}@{fact.version}) — ячейка не выпущена"
        )
        return None, None
    evidence, upstream_ref = built

    # K5: контракт производного доказательства (I1/I8/I10/I15/I19).
    derived_violations = check_derived_evidence(evidence, upstream_ref)
    if derived_violations:
        errors.append(
            f"{product_id}/{param_id}: derived-evidence наследования невалиден: "
            + "; ".join(f"{v.code}: {v.message}" for v in derived_violations)
        )
        return None, None

    cell = Cell(
        run_id=run_id,
        product_id=product_id,
        param_id=param_id,
        state=CellState.EVALUATED,
        status=Status.OK,
        source_kind=SourceKind.INHERITANCE,
        evidence=[
            LLMEvidence(source_id=evidence.source_id, quote=evidence.citation_quote or "")
        ],
    )

    # I8 + derived-evidence: правило наследования обязано быть выполнено.
    violations = check_inheritance(cell, baseline, [evidence])
    if violations:
        errors.append(
            f"{product_id}/{param_id}: наследование нарушено: "
            + "; ".join(f"{v.code}: {v.message}" for v in violations)
        )
        return None, None
    # Наблюдение «о предмете ли доказательство» — после инвариантов: оно описывает
    # выпущенную ячейку и вердикт не оспаривает.
    return _annotate_support(cell, param), evidence


def _conditional_cell(
    run_id: str,
    product_id: str,
    param_id: str,
) -> Cell:
    """Кандидат на ревью для ``conditional``: ``pending`` без разрешённого статуса."""
    return Cell(
        run_id=run_id,
        product_id=product_id,
        param_id=param_id,
        state=CellState.PENDING,
        status=None,
        source_kind=SourceKind.INHERITANCE,
        review_reason=CONDITIONAL_REVIEW_REASON,
    )


@dataclass(frozen=True)
class InheritanceDecision:
    """Результат разрешения одного параметра по объявленному порядку предков (§6.2).

    ``reasons`` — **все** рассмотренные предки с причиной отказа, а не только последний:
    «почему не унаследовали» обязано называть каждую линию, иначе читатель решит, что
    параметр просто забыли. ``None`` в ``cell`` — наследования нет, и параметр
    оценивается обычным путём (Ш5/Ш6): это отсутствие ускорения, а не наблюдение (§1.1).
    """

    product_id: str
    param_id: str
    cell: Cell | None = None
    evidence: CellEvidence | None = None
    reasons: tuple[dict[str, str], ...] = ()
    #: ``baseline_id`` линии, от которой пришла ячейка (``None`` — наследования нет).
    inherited_from: str | None = None
    #: ``baseline_id`` линии, чья ячейка **не выпущена** из-за более высокого источника:
    #: реестр уже дал согласный статус, и волне писать нечего. Параметр не наследован —
    #: он виден в ``not_inherited`` с токеном ``superseded_by_registry``, а не в
    #: ``ancestor_fallbacks`` (проседания к другому предку не было).
    superseded_by: str | None = None


def decide_inheritance(
    *,
    run_id: str,
    product_id: str,
    param: CatalogRow,
    upstreams: Sequence[Any],
    baselines: Mapping[tuple[str, str], BaselineRow],
    facts: Mapping[tuple[str, str, str], BaselineFact],
    errors: list[str] | None = None,
) -> InheritanceDecision:
    """Разрешает наследование параметра по предкам **в объявленном порядке** (план 27 §6.2).

    Правило одно и детерминированное, без тай-брейка по часам или порядку вставки:
    перебираем ``upstreams``; наследует **первый**, у которого есть строка
    ``baselines.csv`` **и** принятый факт ``present`` на его версии. Проседание к
    следующему предку разрешено и правильно: если ближний предок параметр не описывает, а
    дальний описывает, возможность пришла из дальнего — продукт объявил обе линии.

    Порядок проверок строки один для всех режимов: ``нет строки → blocked →
    min_version → версия не парсится → нет факта → review не accepted → status не
    present → conditional → direct``. ``conditional`` стоит **после** проверки
    доказательства: условная строка без факта — это «доказательства нет», и она не вправе
    отменить наследование от дальнего предка, где доказательство есть. Принятый факт у
    ``conditional`` даёт ``pending``-кандидата (решает человек), у ``direct`` — ячейку.

    Факт ищется **ровно на мажорной версии** предка (§7.2): «где-нибудь ≤ V» скрыло бы
    регрессию upstream, при которой параметр исчез в новой версии.

    :param upstreams: ``Passport.upstreams`` — порядок и есть приоритет.
    :param baselines: индекс ``(baseline_id, param_id) → BaselineRow``.
    :param facts: индекс ``(baseline_id, param_id, версия) → BaselineFact``.
    :param errors: накопитель нарушений инвариантов (K5); ``None`` — молча.
    """
    param_id = str(param.param_id)
    reasons: list[dict[str, str]] = []
    sink: list[str] = errors if errors is not None else []

    for upstream in upstreams:
        baseline_id = str(upstream.baseline_id)
        version = str(upstream.version)
        line = {"baseline_id": baseline_id, "version": version}

        policy = baselines.get((baseline_id, param_id))
        if policy is None:
            reasons.append({**line, "reason": REASON_NO_POLICY_ROW})
            continue
        if policy.inheritance_mode is InheritanceMode.BLOCKED:
            reasons.append({**line, "reason": REASON_MODE_BLOCKED})
            continue
        if not _satisfies_min_version(version, policy.min_version):
            reasons.append({**line, "reason": REASON_BELOW_MIN_VERSION})
            continue

        major = major_version(version)
        if major is None:
            reasons.append({**line, "reason": REASON_VERSION_UNPARSABLE})
            continue
        fact = facts.get((baseline_id, param_id, major))
        if fact is None:
            reasons.append({**line, "reason": REASON_FACT_MISSING})
            continue
        if fact.review is not FactReview.ACCEPTED:
            reasons.append(
                {**line, "reason": REASON_FACT_REVIEW.format(review=fact.review.value)}
            )
            continue
        if fact.status is not FactStatus.PRESENT:
            reasons.append(
                {**line, "reason": REASON_FACT_STATUS.format(status=fact.status.value)}
            )
            continue

        if policy.inheritance_mode is InheritanceMode.CONDITIONAL:
            # Условное наследование с **принятым** доказательством: кандидат на ревью,
            # решает человек. Без факта линия выше уже дала причину и пропущена.
            reasons.append({**line, "reason": REASON_MODE_CONDITIONAL})
            return InheritanceDecision(
                product_id=product_id,
                param_id=param_id,
                cell=_conditional_cell(run_id, product_id, param_id),
                reasons=tuple(reasons),
                inherited_from=baseline_id,
            )

        cell, evidence = _direct_cell(run_id, product_id, param, policy, fact, sink)
        if cell is None:
            # Инвариант K5 не пропустил ячейку: причина уже в ``errors``, но линия
            # обязана быть названа и в базисе — иначе «не наследовали» без причины.
            reasons.append({**line, "reason": "derived_evidence_invalid"})
            continue
        return InheritanceDecision(
            product_id=product_id,
            param_id=param_id,
            cell=cell,
            evidence=evidence,
            reasons=tuple(reasons),
            inherited_from=baseline_id,
        )

    return InheritanceDecision(
        product_id=product_id, param_id=param_id, reasons=tuple(reasons)
    )


def _apply_higher_source(
    cell: Cell,
    protected: Mapping[tuple[str, str], Cell],
    product_id: str,
    param_id: str,
) -> Cell | None:
    """Накладывает более высокий источник (реестр) на унаследованную ячейку (Fork D).

    Возвращает унаследованную ячейку, ``pending``-кандидата (расхождение) либо ``None``
    (согласие/inconclusive: высокий источник уже в store, писать нечего).

    ``None`` — не «наследования не было»: ячейка была, но её поглотил реестр. Вызывающий
    обязан это назвать (:attr:`InheritanceDecision.superseded_by`), иначе параметр
    попадёт в базис как унаследованный, хотя не выпущено ничего.
    """
    high = protected.get((product_id, param_id))
    if high is None or high.source_kind is not SourceKind.REGISTRY:
        return cell
    result = resolve_precedence(high, cell)
    return None if result.kept_high else result.cell


#: Ключи состояний выпущенных ячеек в публикуемом базисе (порядок фиксирован — артефакт
#: должен быть воспроизводим, R7). ``other`` — состояние вне трёх ожидаемых (``reviewed``,
#: ``preliminary``): не прячем, иначе базис перестал бы сходиться с числом ячеек.
_RELEASED_STATE_KEYS: tuple[str, ...] = ("evaluated", "pending", "failed", "other")


def _state_key(state: Any) -> str:
    """Ключ состояния ячейки для публикуемого базиса (``evaluated/pending/failed/other``)."""
    value = str(getattr(state, "value", state))
    return value if value in _RELEASED_STATE_KEYS else "other"


def _basis_summary(
    product_id: str,
    planned: Mapping[str, int],
    released: Mapping[str, Mapping[str, int]],
    unattributed: int,
    reason_counts: Mapping[str, int] | None = None,
    fallback_counts: Mapping[str, int] | None = None,
) -> str:
    """Одна строка «что объявлено / что выпущено» продукта — для ``log_entries``.

    Читается как ``direct 19/19 evaluated; conditional 0/14 pending; blocked 0 skipped``:
    оператор видит режим каждого параметра, а не только итог «19 ✅». Именно разница
    «объявлено N, выпущено M» показывает, что режим не сработал (порог версии, отказ
    инварианта), а не что параметров не было.
    """
    direct = released.get("direct", {})
    conditional = released.get("conditional", {})
    parts = [
        f"direct {direct.get('evaluated', 0)}/{planned.get('direct', 0)} evaluated",
    ]
    extra = [
        f"{key} {direct.get(key, 0)}"
        for key in ("pending", "failed", "other")
        if direct.get(key, 0)
    ]
    if extra:
        parts.append("direct " + ", ".join(extra))
    parts.append(
        f"conditional {conditional.get('pending', 0)}/{planned.get('conditional', 0)} pending"
    )
    extra = [
        f"{key} {conditional.get(key, 0)}"
        for key in ("evaluated", "failed", "other")
        if conditional.get(key, 0)
    ]
    if extra:
        parts.append("conditional " + ", ".join(extra))
    parts.append(f"blocked {planned.get('blocked', 0)} skipped")
    if unattributed:
        parts.append(f"unattributed {unattributed}")
    if reason_counts:
        # Причины «не наследовали» одной строкой: оператор видит, это «факт не разведан»
        # или «порог версии/режим», не открывая артефакт.
        details = ", ".join(f"{token}: {count}" for token, count in reason_counts.items())
        parts.append(f"not_inherited {sum(reason_counts.values())} ({details})")
    if fallback_counts:
        details = ", ".join(
            f"{token}: {count}" for token, count in fallback_counts.items()
        )
        parts.append(f"fallback {sum(fallback_counts.values())} ({details})")
    return f"{product_id}: " + "; ".join(parts)


def inheritance_basis(
    baselines: Sequence[BaselineRow],
    *,
    products: Iterable[str],
    passports: Any = None,
    params: Iterable[Any] | None = None,
    cells: Iterable[Cell] = (),
    decisions: Iterable[InheritanceDecision] = (),
) -> dict[str, Any]:
    """**Базис наследования**: объявленный режим против фактически выпущенного.

    Итог волны («15 ✅, 4 pending») этого не показывает: по нему нельзя отличить
    ``direct``-ячейку от ``conditional``-кандидата, а «объявлено 19, выпущено 19» —
    единственное свидетельство, что режим не был тихо подавлен (порог ``min_version``,
    отказ K5). Поэтому публикуются обе стороны по каждому продукту.

    Откуда берутся числа (второй раз ничего не считается):

    * **объявлено** (``planned``) — **различные параметры** строк ``baselines.csv``
      объявленных линий продукта, режим каждого — режим линии, которую выбрал резолвер
      (без решений — режим первой объявленной строки); при заданном ``params`` — только
      по объявленным параметрам плана;
    * **выпущено** (``released``) — ``cells`` **результата волны** (``PhaseResult.cells``
      Ш4), а не повторное чтение стора: базис описывает работу этой волны. Режим ячейки
      берётся из той же строки ``baselines.csv`` по ``param_id``. Ячейка **другого**
      источника (корпусная оценка того же параметра) режиму не приписывается — она не
      работа Ш4 — но её число публикуется как ``foreign_cells``: исключение видно;
      унаследованная ячейка без строки базлайна попадает в ``unattributed_cells``.

    :param baselines: строки ``baselines.csv``.
    :param products: ``product_id`` плана (итерируемое).
    :param passports: ``Passport``-набор (mapping либо итерируемое) — источник
        ``upstreams[].baseline_id`` (план 27 §3.2: предков может быть несколько, и
        «объявлено» считается по объединению их строк).
    :param params: объявленные параметры плана (``CatalogRow`` или ``param_id``);
        ``None`` — фильтра нет (считаются все строки базлайна).
    :param cells: ячейки результата волны.
    :param decisions: решения волны (:class:`InheritanceDecision`) — источник причин
        «не наследовали». Пусто — причины не публикуются (вызов без волны: базис
        считает только объявленное/выпущенное). Параметр, чья унаследованная ячейка
        поглощена более высоким источником (:attr:`InheritanceDecision.superseded_by`),
        попадает в ``not_inherited`` с токеном ``superseded_by_registry``, а не в
        ``ancestor_fallbacks``: ячейки нет, значит наследования нет.
    :returns: JSON-совместимый словарь по продуктам (``planned``/``released``/
        ``cells_emitted``/``unattributed_cells``/``not_inherited``/``reason_counts``/
        ``summary``), итоги и список строк ``summary``. В БД и в файлы функция ничего
        не пишет.
    """
    passport_by_product = _passport_index(passports)
    wanted: set[str] | None = None
    if params is not None:
        wanted = {str(getattr(param, "param_id", param)) for param in params}

    cells_by_product: dict[str, list[Cell]] = {}
    for cell in cells:
        cells_by_product.setdefault(str(cell.product_id), []).append(cell)

    report: dict[str, dict[str, Any]] = {}
    planned_total = {mode.value: 0 for mode in InheritanceMode}
    released_total = {
        mode.value: {key: 0 for key in _RELEASED_STATE_KEYS} for mode in InheritanceMode
    }
    summaries: list[str] = []
    reasons_by_product: dict[str, list[dict[str, Any]]] = {}
    #: ``product_id → param_id → baseline_id`` линии, которую выбрал резолвер волны.
    #: Нужен, чтобы режим выпущенной ячейки был режимом **выигравшего** предка, а не
    #: первой строки файла (иначе «blocked 1 skipped» печатается рядом с ✅ от ``direct``).
    winners_by_product: dict[str, dict[str, str]] = {}
    for decision in decisions:
        if decision.inherited_from is not None:
            winners_by_product.setdefault(str(decision.product_id), {})[
                decision.param_id
            ] = decision.inherited_from
        # Поглощение высоким источником — тоже факт решения, даже когда проигравших
        # линий нет: параметр не наследован, и это обязано быть видно.
        if not decision.reasons and decision.superseded_by is None:
            continue
        # Публикуются причины **всех рассмотренных линий**, в том числе когда ячейка
        # выпущена: проигравшие предки у решения уже перечислены, и это ровно ответ на
        # вопрос «почему не от первого предка» (§3.2, §6.2). Отбросить их — значит
        # показать наследование «от второго», не назвав, чем плох первый; читатель
        # прочтёт это как произвол порядка, а не как измерение.
        # ``inherited`` отличает «не наследовали вовсе» от «наследовали, но не сразу».
        # Ячейка, поглощённая реестром, не выпущена — значит параметр не наследован, и в
        # ``ancestor_fallbacks`` ему не место: проседания к другому предку не было.
        inherited = (
            decision.superseded_by is None
            and decision.cell is not None
            and decision.cell.state is CellState.EVALUATED
        )
        entries = [dict(item) for item in decision.reasons]
        if decision.superseded_by is not None:
            entries.append(
                {
                    "baseline_id": decision.superseded_by,
                    "reason": REASON_SUPERSEDED_BY_REGISTRY,
                }
            )
        reasons_by_product.setdefault(str(decision.product_id), []).append(
            {
                "param_id": decision.param_id,
                "inherited": inherited,
                "reasons": entries,
            }
        )

    for product_id in sorted(str(pid) for pid in products):
        passport = passport_by_product.get(product_id)
        upstreams = list(passport.upstreams) if passport is not None else []
        baseline_ids = [str(upstream.baseline_id) for upstream in upstreams]

        # Объявлено: **различные параметры** объявленных линий продукта, режим каждого —
        # режим линии, которую выбрал резолвер (``upstreams`` в объявленном порядке:
        # первый, у кого есть принятый факт). Без решения (базис считают без волны) —
        # режим первой объявленной строки, т.е. того же первого предка.
        #
        # Второй проход по строкам файла завышал бы план на число дополнительных линий
        # («direct 1/2 evaluated» при единственной выпущенной ячейке) и брал режим по
        # порядку файла, а не по выигравшему предку (§3.2, §6.2).
        rows_by_baseline: dict[str, dict[str, BaselineRow]] = {}
        for row in baselines:
            if row is None or row.baseline_id not in baseline_ids:
                continue
            if wanted is not None and row.param_id not in wanted:
                continue
            rows_by_baseline.setdefault(row.baseline_id, {})[row.param_id] = row
        winners = winners_by_product.get(product_id, {})
        declared_params = {
            param_id for rows in rows_by_baseline.values() for param_id in rows
        }
        mode_by_param: dict[str, InheritanceMode] = {}
        for param_id in sorted(declared_params):
            winner = winners.get(param_id)
            if winner is not None:
                won_row = rows_by_baseline.get(winner, {}).get(param_id)
                if won_row is not None:
                    mode_by_param[param_id] = won_row.inheritance_mode
                    continue
            for baseline_id in baseline_ids:
                fallback_row = rows_by_baseline.get(baseline_id, {}).get(param_id)
                if fallback_row is not None:
                    mode_by_param[param_id] = fallback_row.inheritance_mode
                    break
        planned = {mode.value: 0 for mode in InheritanceMode}
        for declared_mode in mode_by_param.values():
            planned[declared_mode.value] += 1

        # Выпущено: ровно то, что вернула волна (никакого второго чтения стора). Ячейка
        # другого источника — не работа Ш4, даже если у параметра есть строка базлайна
        # (корпусная оценка того же параметра): она не приписывается режиму, но и не
        # прячется — её число публикуется как ``foreign_cells``. Кандидат ревью, который
        # волна выпустила поверх реестра, наследует ``source_kind`` высокого источника —
        # он остаётся работой Ш4 (иначе базис «терял» бы свои же ячейки).
        released = {
            mode.value: {key: 0 for key in _RELEASED_STATE_KEYS}
            for mode in InheritanceMode
        }
        unattributed = 0
        foreign = 0
        product_cells = cells_by_product.get(product_id, [])
        for cell in product_cells:
            wave_cell = (
                cell.source_kind is SourceKind.INHERITANCE
                or cell.review_reason == PRECEDENCE_REVIEW_REASON
            )
            mode = mode_by_param.get(str(cell.param_id))
            if mode is None:
                if wave_cell:
                    unattributed += 1
                continue
            if not wave_cell:
                foreign += 1
                continue
            released[mode.value][_state_key(cell.state)] += 1
        wave_cells = sum(
            count
            for states in released.values()
            for count in states.values()
        )

        # Причины «не наследовали» — по продукту и параметру (§6.2): «объявлено 181,
        # выпущено 140» без них читается как дефект конвейера, а не как «41 факт ещё не
        # разведан». Счётчик по токенам — для строки итога: адресный список остаётся в
        # артефакте, а не в однострочном логе.
        all_reasons = sorted(
            reasons_by_product.get(product_id, []), key=lambda item: item["param_id"]
        )
        # Два разных факта, и оба нужны: «параметр не унаследован вовсе» (это цена
        # неразведанного факта) и «унаследован, но не от первого предка» (это цена
        # проседания по линиям). Слить их в один счётчик — значит потерять различие.
        not_inherited = [item for item in all_reasons if not item["inherited"]]
        ancestor_fallbacks = [item for item in all_reasons if item["inherited"]]
        reason_counts: dict[str, int] = {}
        for item in not_inherited:
            for entry in item["reasons"]:
                token = str(entry.get("reason", ""))
                reason_counts[token] = reason_counts.get(token, 0) + 1
        reason_counts = dict(sorted(reason_counts.items()))
        fallback_counts: dict[str, int] = {}
        for item in ancestor_fallbacks:
            for entry in item["reasons"]:
                token = str(entry.get("reason", ""))
                fallback_counts[token] = fallback_counts.get(token, 0) + 1
        fallback_counts = dict(sorted(fallback_counts.items()))

        summary = _basis_summary(
            product_id, planned, released, unattributed, reason_counts, fallback_counts
        )
        for mode in InheritanceMode:
            planned_total[mode.value] += planned[mode.value]
            for key in _RELEASED_STATE_KEYS:
                released_total[mode.value][key] += released[mode.value][key]
        report[product_id] = {
            "baseline_ids": baseline_ids,
            "planned": planned,
            "released": released,
            "cells_emitted": wave_cells,
            "unattributed_cells": unattributed,
            "foreign_cells": foreign,
            "not_inherited": not_inherited,
            "reason_counts": reason_counts,
            # Проседание к следующему предку: параметр унаследован, но не от первого
            # объявленного. Причины называют, чем именно не подошла каждая линия выше.
            "ancestor_fallbacks": ancestor_fallbacks,
            "fallback_counts": fallback_counts,
            "summary": summary,
        }
        # Продукт без объявленных строк и без выпущенных ячеек не несёт факта:
        # «0/0» в артефакте выглядело бы наблюдением.
        if sum(planned.values()) or wave_cells:
            summaries.append(summary)

    return {
        "measurement": "inheritance_basis",
        "products": report,
        "totals": {"planned": planned_total, "released": released_total},
        "summary": summaries,
    }


def inheritance_wave(
    plan: Any,
    baselines: list[BaselineRow],
    passports: Any,
    store: Any = None,
    facts: Sequence[BaselineFact] = (),
) -> PhaseResult:
    """Ш4: волна наследования. Возвращает ``PhaseResult`` (без LLM).

    Наследование — **ускоритель, а не ворота** (план 27 §1.1): нет факта, факт
    ``pending``/``rejected``/``absent`` — ячейка не наследуется, и параметр оценивается
    обычным путём (Ш5/Ш6). Ни ``❓``, ни ``❌`` из этого не следует: отсутствие ускорения
    — не наблюдение о продукте.

    Предки перебираются **в объявленном порядке** (``Passport.upstreams``, §3.2):
    наследует первый, у которого есть строка ``baselines.csv`` и принятый факт
    ``present`` на его версии. Проседание к следующему предку разрешено и правильно.

    :param plan: ``RunPlan`` (продукты, параметры, ``run_id``).
    :param baselines: строки ``baselines.csv`` (политика наследования).
    :param passports: ``Passport``-набор — mapping ``product_id → Passport`` либо итерируемое.
    :param store: опциональный ``StorePort`` (пишет ячейки и derived-evidence).
    :param facts: строки ``sources/baseline_facts.csv`` (наблюдения о документации
        предка). Пусто — наследования по фактам не будет, и причины это назовут
        (``baseline_fact_missing``), а не «наследовать нечему».
    :raises ValueError: если ``baselines`` не список (контракт входа).
    """
    if not isinstance(baselines, list):
        raise ValueError("baselines обязан быть списком BaselineRow")

    run_id = str(getattr(plan, "run_id", ""))
    products = sorted(str(pid) for pid in getattr(plan, "products", []))
    params = sorted(
        getattr(plan, "params", []), key=lambda row: getattr(row, "param_id", "")
    )
    passport_by_product = _passport_index(passports)
    baseline_by_key = _baseline_index(baselines)
    fact_by_key = _fact_index(facts)
    # Ш3 уже отработал (PHASES: registry → inheritance): реестровые ячейки — высокий
    # источник, который Ш4 не должен молча затирать (Fork D, §2.2 п.7).
    protected = collect_protected_cells(store, run_id)

    cells: list[Cell] = []
    evidence_items: list[CellEvidence] = []
    errors: list[str] = []
    decisions: list[InheritanceDecision] = []

    for product_id in products:
        passport = passport_by_product.get(product_id)
        if passport is None:
            # Паспорт — предусловие G2; без него upstream-совпадение недоказуемо.
            errors.append(f"нет паспорта для продукта {product_id!r} — Ш4 пропущен")
            continue
        upstreams = list(passport.upstreams)
        if not upstreams:
            # Нет upstream-линий → кросс-линейный параметр (архетип B): не здесь.
            continue

        for row in params:
            decision = decide_inheritance(
                run_id=run_id,
                product_id=product_id,
                param=row,
                upstreams=upstreams,
                baselines=baseline_by_key,
                facts=fact_by_key,
                errors=errors,
            )
            cell = decision.cell
            if cell is not None and cell.state is CellState.EVALUATED:
                cell = _apply_higher_source(cell, protected, product_id, row.param_id)
                if cell is None and decision.inherited_from is not None:
                    # Реестр уже дал согласный статус: писать нечего, ячейки нет. Решение
                    # обязано это назвать, иначе параметр попадёт в «проседание к предку»
                    # как унаследованный, хотя не выпущено ничего.
                    decision = replace(decision, superseded_by=decision.inherited_from)
            decisions.append(decision)
            if cell is not None:
                cells.append(cell)
            # Evidence пишется только вместе с ячейкой. Поглощённое реестром решение
            # (``superseded_by``) ячейки не имеет, и его derived-evidence «подтверждал» бы
            # цитату, которой в этом прогоне не видно ни одной ячейки, — ровно тот класс,
            # от которого защищает ``_carried_rules_hold`` у переноса.
            if decision.evidence is not None and decision.superseded_by is None:
                evidence_items.append(decision.evidence)

    cells.sort(key=lambda cell: (cell.product_id, cell.param_id))
    _persist(store, cells, evidence_items, errors)
    # Базис наследования — тот же результат волны, а не второе чтение стора: оператор
    # видит «direct N/M evaluated, conditional K/L pending» там же, где итог фазы
    # (``results[].notes`` в выводе прогона), и не догадывается, что под «✅» разные режимы.
    # Причины «не наследовали» — из решений той же волны (§6.2).
    basis = inheritance_basis(
        baselines,
        products=products,
        passports=passport_by_product,
        params=params,
        cells=cells,
        decisions=decisions,
    )
    log_entries = (
        ["inheritance_basis: " + " | ".join(basis["summary"])]
        if basis["summary"]
        else []
    )
    return PhaseResult(
        status="ok",
        phase="inheritance",
        cells=cells,
        errors=errors,
        log_entries=log_entries,
    )


def _persist(
    store: Any,
    cells: list[Cell],
    evidence: list[CellEvidence],
    errors: list[str],
) -> None:
    """Пишет ячейки/evidence; сбой персистенции изолируется (не роняет волну)."""
    if store is None:
        return
    for ev in evidence:
        try:
            store.add_evidence(ev)
        except Exception as exc:  # noqa: BLE001 — наблюдаемость важнее персистенции
            errors.append(f"add_evidence: {type(exc).__name__}: {exc}")
    for cell in cells:
        try:
            store.save_cell(cell)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"save_cell: {type(exc).__name__}: {exc}")
