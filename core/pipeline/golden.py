"""coHound — golden-харнесс и калибровка порогов (W12/T12.4, §4.2/§11.3, R6/R11/R12).

Сравнивает вердикты конвейера с эталонным ``golden.csv`` (доменные
:class:`~core.domain.models.GoldenRow`) и вычисляет метрики приёмки:

* **R6** — agreement (доля совпавших ячеек) ≥ порога (временный порог калибровки MVP);
* критические классы (§11.1, допустимо 0): любой **false ✅**; **false ❌ на Кр-параметрах**;
* **R12** — наличие страты ожидаемых ❌ в golden-наборе.

Пороги — **калибруемые дефолты** (:class:`GoldenThresholds`), а не истина (A3): они
фиксируются в манифесте отчёта (:meth:`GoldenReport.to_manifest_json`) и заменяются по
измерению. Формулы не дублируют домен: actual-статусы берутся из ячеек (``failed`` →
``status=None``, I4) или передаются отображением ``(product_id, param_id) -> Status | None``.

Ядро сравнения :func:`run_golden` — чистое (без I/O) и детерминированное; удобная обёртка
:func:`evaluate_golden` читает статусы из ``StorePort.iter_cells(run_id)``. Модуль
категорийно нейтрален (§1.3) и использует только stdlib + домен.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from core.domain.enums import Importance, Status
from core.domain.models import CatalogRow, GoldenRow
from core.ports.store import StorePort

__all__ = [
    "ERROR_CLASSES",
    "GoldenThresholds",
    "GoldenCaseResult",
    "GoldenReport",
    "GoldenThresholdError",
    "run_golden",
    "evaluate_golden",
    "check_thresholds",
    "assert_golden",
]

#: Допустимые machine-классы ошибки сравнения одной golden-строки.
ERROR_CLASSES: frozenset[str] = frozenset(
    {"false_ok", "false_absence", "mismatch", "missing"}
)


# ---------------------------------------------------------------------------
# Пороги приёмки (калибруемые дефолты, A3)
# ---------------------------------------------------------------------------


@dataclass
class GoldenThresholds:
    """Пороги приёмки golden (§11.1/§11.3).

    Значения — **калибровочные дефолты MVP**, не догма (A3): ``min_agreement=0.80`` —
    временный порог R6; критические классы допускают 0 нарушений.

    :param min_agreement: минимальный agreement по R6 (доля совпавших ячеек).
    :param max_false_ok: допустимое число «false ✅» (любой false ✅ — нарушение по §11.1).
    :param max_critical_false_absence: допустимое число «false ❌» на Кр-параметрах (R12).
    """

    min_agreement: float = 0.80
    max_false_ok: int = 0
    max_critical_false_absence: int = 0

    def as_dict(self) -> dict[str, float | int]:
        """Детерминированное представление порогов для манифеста/отчёта."""
        return {
            "min_agreement": self.min_agreement,
            "max_false_ok": self.max_false_ok,
            "max_critical_false_absence": self.max_critical_false_absence,
        }


# ---------------------------------------------------------------------------
# Результат по одной golden-строке и агрегат
# ---------------------------------------------------------------------------


@dataclass
class GoldenCaseResult:
    """Результат сравнения одной golden-строки с фактическим вердиктом.

    :param importance: важность параметра из каталога (``None``, если каталог не задан
        или параметр в него не входит) — нужна для правила «false ❌ на Кр».
    :param matches: ``actual == expected`` (сравнение при ``actual is not None``).
    :param error_class: ``None`` при совпадении; иначе один из :data:`ERROR_CLASSES`:

        * ``"false_ok"`` — система вернула ✅ там, где ожидался не-✅ (переоценка);
        * ``"false_absence"`` — система вернула ❌ там, где ожидался не-❌ (ложное отсутствие);
        * ``"mismatch"`` — иное расхождение статусов;
        * ``"missing"`` — фактического статуса нет (``failed``/нет ячейки, I4).
    """

    product_id: str
    param_id: str
    expected: Status
    actual: Status | None
    case_class: str
    importance: Importance | None
    matches: bool
    error_class: str | None


@dataclass
class GoldenReport:
    """Агрегат golden-прогона: метрики приёмки и по-классовые счётчики.

    :param results: результаты по строкам (в порядке golden-набора — детерминизм).
    :param agreement: доля совпавших ячеек (``matches / total``; ``0.0`` для пустого набора).
    :param false_ok: число «false ✅» (R6/§11.1: допустимо 0).
    :param critical_false_absence: число «false ❌» **только на Кр-параметрах** (R12).
    :param missing: число строк без фактического статуса (``failed``/нет ячейки).
    :param thresholds: применённые пороги (калибровка, A3) — фиксируются в манифесте.
    :param error_counts: счётчик по :data:`ERROR_CLASSES` (отсутствующие классы не хранятся).
    :param case_class_counts: счётчик по ``case_class`` golden-строк.
    :param expected_counts: счётчик по ожидаемым статусам (``.value``).
    """

    results: list[GoldenCaseResult]
    agreement: float
    false_ok: int
    critical_false_absence: int
    missing: int
    thresholds: GoldenThresholds = field(default_factory=GoldenThresholds)
    error_counts: dict[str, int] = field(default_factory=dict)
    case_class_counts: dict[str, int] = field(default_factory=dict)
    expected_counts: dict[str, int] = field(default_factory=dict)

    @property
    def total(self) -> int:
        """Общее число golden-строк."""
        return len(self.results)

    @property
    def matches(self) -> int:
        """Число совпавших строк."""
        return sum(1 for r in self.results if r.matches)

    @property
    def has_absent_stratum(self) -> bool:
        """R12/§4.2: в наборе есть хотя бы одна строка с ожидаемым ❌."""
        return any(r.expected is Status.ABSENT for r in self.results)

    def passes(self, thresholds: GoldenThresholds | None = None) -> bool:
        """Проходит ли набор пороги R6 и критические классы.

        Проверяются: agreement ≥ ``min_agreement``, ``false_ok ≤ max_false_ok``,
        ``critical_false_absence ≤ max_critical_false_absence``. Наличие страты ❌ (R12)
        проверяется отдельно :meth:`has_absent_stratum` / :func:`check_thresholds`.
        """
        t = thresholds if thresholds is not None else self.thresholds
        return (
            self.agreement >= t.min_agreement
            and self.false_ok <= t.max_false_ok
            and self.critical_false_absence <= t.max_critical_false_absence
        )

    def to_dict(self) -> dict[str, Any]:
        """JSON-совместимый словарь отчёта (детерминированный, сортированные ключи)."""
        return {
            "agreement": self.agreement,
            "case_class_counts": dict(sorted(self.case_class_counts.items())),
            "critical_false_absence": self.critical_false_absence,
            "error_counts": dict(sorted(self.error_counts.items())),
            "expected_counts": dict(sorted(self.expected_counts.items())),
            "false_ok": self.false_ok,
            "has_absent_stratum": self.has_absent_stratum,
            "missing": self.missing,
            "results": [
                {
                    "actual": r.actual.value if r.actual is not None else None,
                    "case_class": r.case_class,
                    "error_class": r.error_class,
                    "expected": r.expected.value,
                    "importance": r.importance.value if r.importance is not None else None,
                    "matches": r.matches,
                    "param_id": r.param_id,
                    "product_id": r.product_id,
                }
                for r in self.results
            ],
            "thresholds": self.thresholds.as_dict(),
            "total": self.total,
        }

    def to_manifest_json(self) -> str:
        """Детерминированный JSON отчёта (``sort_keys``) для манифеста/отчёта."""
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True)

    def summary(self) -> str:
        """Человекочитаемая сводка (стабильная строка для логов/CLI)."""
        verdict = "PASS" if self.passes() else "FAIL"
        return (
            f"golden: agreement={self.agreement:.1%} ({self.matches}/{self.total}), "
            f"false_ok={self.false_ok}, "
            f"critical_false_absence={self.critical_false_absence}, "
            f"missing={self.missing}, "
            f"absent_stratum={self.has_absent_stratum} -> {verdict}"
        )


# ---------------------------------------------------------------------------
# Сравнение (чистая функция)
# ---------------------------------------------------------------------------


def _classify(expected: Status, actual: Status | None) -> tuple[bool, str | None]:
    """Классифицирует пару (expected, actual) → ``(matches, error_class)``.

    I4: ``actual is None`` (``failed``/нет ячейки) — это ``"missing"``, а не статус.
    """
    if actual is None:
        return False, "missing"
    if actual is expected:
        return True, None
    if actual is Status.OK:
        return False, "false_ok"
    if actual is Status.ABSENT:
        return False, "false_absence"
    return False, "mismatch"


def run_golden(
    golden: list[GoldenRow],
    actual_by_key: Mapping[tuple[str, str], Status | None],
    *,
    catalog: Mapping[str, CatalogRow] | None = None,
    thresholds: GoldenThresholds | None = None,
) -> GoldenReport:
    """Сравнивает golden-набор с фактическими вердиктами (чисто, без I/O).

    :param golden: эталонные строки (порядок сохраняется в отчёте).
    :param actual_by_key: ``(product_id, param_id) -> Status | None``. Отсутствие ключа
        и значение ``None`` равнозначны ``missing`` (I4: ``failed`` → ``status=None``).
    :param catalog: ``{param_id: CatalogRow}`` — источник важности для правила
        «false ❌ на Кр-параметрах»; ``None`` → важность неизвестна (Кр не считается).
    :param thresholds: применяемые пороги (по умолчанию :class:`GoldenThresholds`).

    Детерминирован: результат зависит только от входа; порядок ``results`` — порядок
    ``golden``. Пустой набор даёт ``agreement=0.0`` без деления на ноль.
    """
    applied = thresholds if thresholds is not None else GoldenThresholds()

    results: list[GoldenCaseResult] = []
    matches = 0
    false_ok = 0
    critical_false_absence = 0
    missing = 0
    error_counts: dict[str, int] = {}
    case_class_counts: dict[str, int] = {}
    expected_counts: dict[str, int] = {}

    for row in golden:
        actual = actual_by_key.get((row.product_id, row.param_id))
        cat_row = catalog.get(row.param_id) if catalog is not None else None
        importance = cat_row.importance if cat_row is not None else None

        matched, error_class = _classify(row.expected_status, actual)
        if matched:
            matches += 1
        if error_class is not None:
            error_counts[error_class] = error_counts.get(error_class, 0) + 1
            if error_class == "missing":
                missing += 1
            elif error_class == "false_ok":
                false_ok += 1
            elif error_class == "false_absence" and importance is Importance.CRITICAL:
                critical_false_absence += 1

        case_class_counts[row.case_class] = case_class_counts.get(row.case_class, 0) + 1
        expected_counts[row.expected_status.value] = (
            expected_counts.get(row.expected_status.value, 0) + 1
        )

        results.append(
            GoldenCaseResult(
                product_id=row.product_id,
                param_id=row.param_id,
                expected=row.expected_status,
                actual=actual,
                case_class=row.case_class,
                importance=importance,
                matches=matched,
                error_class=error_class,
            )
        )

    total = len(golden)
    agreement = matches / total if total else 0.0
    return GoldenReport(
        results=results,
        agreement=agreement,
        false_ok=false_ok,
        critical_false_absence=critical_false_absence,
        missing=missing,
        thresholds=applied,
        error_counts=error_counts,
        case_class_counts=case_class_counts,
        expected_counts=expected_counts,
    )


def evaluate_golden(
    *,
    golden: list[GoldenRow],
    store: StorePort,
    run_id: str,
    catalog: Mapping[str, CatalogRow],
    thresholds: GoldenThresholds | None = None,
) -> GoldenReport:
    """Сравнивает golden с фактическими ячейками прогона из ``store``.

    Читает ``store.iter_cells(run_id)`` и строит ``(product_id, param_id) -> cell.status``
    (``failed``/``preliminary`` несут ``status=None``, I4/I13), затем делегирует
    :func:`run_golden`. Удобная обёртка для CLI/CI; ядро сравнения остаётся чистым.
    """
    actual_by_key: dict[tuple[str, str], Status | None] = {}
    for cell in store.iter_cells(run_id):
        actual_by_key[(cell.product_id, cell.param_id)] = cell.status
    return run_golden(
        golden, actual_by_key, catalog=catalog, thresholds=thresholds
    )


# ---------------------------------------------------------------------------
# Гейты приёмки
# ---------------------------------------------------------------------------


class GoldenThresholdError(AssertionError):
    """Нарушение порогов приёмки golden (R6/R12, §11.3)."""


def check_thresholds(
    report: GoldenReport, thresholds: GoldenThresholds | None = None
) -> list[str]:
    """Возвращает человекочитаемые нарушения приёмки (пустой список — ок).

    Проверяются R6 (agreement + критические классы: любой false ✅ и false ❌ на Кр) и
    R12/§4.2 (наличие страты ожидаемых ❌). ``thresholds=None`` → пороги из отчёта.
    """
    t = thresholds if thresholds is not None else report.thresholds
    violations: list[str] = []
    if report.agreement < t.min_agreement:
        violations.append(
            f"R6: golden agreement {report.agreement:.1%} < порога {t.min_agreement:.1%}"
        )
    if report.false_ok > t.max_false_ok:
        violations.append(
            f"R6/§11.1: false ✅ = {report.false_ok} > {t.max_false_ok} "
            "(любой false ✅ недопустим)"
        )
    if report.critical_false_absence > t.max_critical_false_absence:
        violations.append(
            f"R12: false ❌ на Кр-параметрах = {report.critical_false_absence} > "
            f"{t.max_critical_false_absence}"
        )
    if not report.has_absent_stratum:
        violations.append(
            "R12/§4.2: golden-набор не содержит страты ожидаемых ❌ "
            "(registry_absence/distribution_absence)"
        )
    return violations


def assert_golden(
    report: GoldenReport, thresholds: GoldenThresholds | None = None
) -> None:
    """Поднимает :class:`GoldenThresholdError`, если приёмка golden не пройдена."""
    violations = check_thresholds(report, thresholds)
    if violations:
        raise GoldenThresholdError("; ".join(violations))
