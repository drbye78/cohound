"""``baseline-agreement``: согласие двух аннотаторов ручного baseline-эксперимента (§11.5).

Тонкая CLI-поверхность (A1) над чистой арифметикой :mod:`core.util.agreement`: команда
читает **уже подготовленный человеком** CSV-файл разметки (два независимых аннотатора,
≥30–40 ячеек включая страту ❌ и N/A-решения — §11.5) и печатает согласие. Инструмент
**ничего не аннотирует и не решает за человека**: N/A — человеческое решение (I6/I7), и
никакой UI/рабочий процесс разметки здесь не строится (это осознанно вне рамок, A1).

Публикуются два числа, как требует §11.5:

* κ по полному набору меток (как размечено);
* κ **отдельно** по бинарному решению «N/A vs не-N/A» — потому что N/A задаёт знаменатель
  Coverage (I6/I7);
* счётчики (всего, парные, N/A у каждого аннотатора, расхождения) — κ на малой страте
  неустойчива, читателю нужно видеть N;
* вырожденные случаи (``n = 0``, нулевая вариация маргиналов) помечаются ``undefined`` с
  причиной, а не подменяются числом; непарные ячейки (размеченные лишь одним аннотатором)
  **не** считаются согласием и показываются списком.

Контракт CSV задан явно. Обязательные колонки: ``product_id``, ``param_id`` (идентичность
ячейки) и ``annotator_a``, ``annotator_b`` (две метки). Порядок колонок произвольный, лишние
игнорируются; пустая ячейка метки = аннотатор ячейку не размечал. Неверная форма (нет
обязательной колонки, пустой ``product_id``/``param_id``, дубликат ячейки, строка без
единой метки) **отвергается**, а не додумывается. Метка вне словаря статусов не роняет
расчёт — попадает в ``unknown_labels`` и делает результат ``ok: no``
(:data:`~core.cli.errors.EXIT_USAGE`).

Нейтрален к предметной категории (§1.3): словарь меток читается из домена
(:class:`~core.domain.enums.Status`), а не хардкодится.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from core.cli.context import _add_common, _print_json
from core.cli.errors import EXIT_OK, EXIT_USAGE, CliError
from core.cli.output import concise
from core.domain.enums import Status
from core.util.agreement import (
    AgreementReport,
    ConfusionMatrix,
    KappaResult,
    compute_agreement,
    parse_annotations_csv,
)

__all__ = ["run", "configure"]


def _kappa_payload(result: KappaResult) -> dict[str, Any]:
    """JSON-представление одного κ (число либо честная неопределённость)."""
    return {
        "kappa": result.kappa,
        "defined": result.defined,
        "observed_agreement": result.observed_agreement,
        "expected_agreement": result.expected_agreement,
        "n": result.n,
        "undefined_reason": result.undefined_reason,
    }


def _confusion_payload(matrix: ConfusionMatrix | None) -> dict[str, Any] | None:
    """JSON-представление матрицы ошибок (``None`` — не считалась)."""
    if matrix is None:
        return None
    return {
        "labels": list(matrix.labels),
        "counts": {row: dict(cols) for row, cols in matrix.counts.items()},
    }


def _report_payload(report: AgreementReport, path: str) -> dict[str, Any]:
    """Собирает машиночитаемый отчёт (§11.5): оба κ + счётчики + матрицы + аномалии.

    ``ok`` — нет меток вне словаря статусов; вырожденный κ (``undefined``) не делает
    результат ошибочным: это легитимный исход расчёта, честно помеченный.
    """
    return {
        "ok": not report.unknown_labels,
        "annotations": path,
        "total_items": report.total_items,
        "paired_items": report.paired_items,
        "unpaired_items": list(report.unpaired_items),
        "overall": _kappa_payload(report.overall),
        "na": _kappa_payload(report.na_binary) if report.na_binary is not None else None,
        "na_label": report.na_label,
        "na_counts": {
            "annotator_a": report.na_count_a,
            "annotator_b": report.na_count_b,
        },
        "disagreements": list(report.disagreements),
        "na_disagreements": list(report.na_disagreements),
        "confusion": _confusion_payload(report.overall_confusion),
        "na_confusion": _confusion_payload(report.na_confusion),
        "unknown_labels": dict(report.unknown_labels),
    }


def _fmt_kappa(data: dict[str, Any]) -> str:
    """Человекочитаемое κ: число (4 знака) либо ``undefined`` с причиной."""
    if data["kappa"] is not None:
        return f"{data['kappa']:.4f}"
    return f"undefined ({data['undefined_reason']})"


def _fmt_agreement(data: dict[str, Any], label: str) -> str:
    """Строка ``label: κ (po, pe, n)`` из JSON-представления κ (честный ``undefined``)."""
    observed = (
        f"{data['observed_agreement']:.4f}"
        if data["observed_agreement"] is not None
        else "—"
    )
    expected = (
        f"{data['expected_agreement']:.4f}"
        if data["expected_agreement"] is not None
        else "—"
    )
    return (
        f"{label}: {_fmt_kappa(data)} "
        f"(po={observed}, pe={expected}, n={data['n']})"
    )


def _print_report(payload: dict[str, Any]) -> None:
    """Человекочитаемый вывод ``baseline-agreement`` (оба κ + счётчики)."""
    lines = [
        f"annotations: {payload['annotations']}",
        f"items: {payload['total_items']} "
        f"(paired {payload['paired_items']}, unpaired {len(payload['unpaired_items'])})",
    ]
    confusion = payload["confusion"]
    lines.append(f"labels: {', '.join(confusion['labels']) or '—'}")
    lines.append(f"overall {_fmt_agreement(payload['overall'], 'kappa')}")
    na = payload["na"]
    if na is None:
        lines.append("na kappa: не считался (na_label не задан)")
    else:
        counts = payload["na_counts"]
        lines.append(
            f"na {_fmt_agreement(na, 'kappa')} "
            f"[{payload['na_label']}: a={counts['annotator_a']}, b={counts['annotator_b']}]"
        )
    lines.append(f"disagreements: {len(payload['disagreements'])}")
    if na is not None:
        lines.append(f"na disagreements: {len(payload['na_disagreements'])}")
    for cell_id in payload["unpaired_items"]:
        lines.append(f"unpaired: {cell_id} (размечен одним аннотатором — в κ не входит)")
    for label, count in payload["unknown_labels"].items():
        lines.append(f"unknown label: {label!r} × {count} (вне словаря статусов)")
    lines.append(
        f"note: N/A-решения задают знаменатель Coverage (I6/I7) — κ по N/A публикуется "
        "отдельно (§11.5)"
    )
    lines.append(f"ok: {'yes' if payload['ok'] else 'no'}")
    sys.stdout.write("\n".join(lines) + "\n")


def run(args: argparse.Namespace) -> int:
    """``hound baseline-agreement``: согласие аннотаторов из CSV (§11.5).

    Читает разметку, считает κ по полному набору меток и отдельно по N/A-решению, печатает
    счётчики и матрицы. Словарь меток читается из :class:`~core.domain.enums.Status`
    (CLI не хардкодит список статусов).

    **Контракт кода возврата.** :data:`EXIT_OK` — расчёт выполнен и меток вне словаря нет;
    :data:`EXIT_USAGE` — неверная форма CSV либо метки вне словаря статусов (печать отчёта
    сохраняется, чтобы читатель видел аномалию). Вырожденный κ — не ошибка (``ok`` не меняет).

    :param args: ``--annotations`` (путь), ``--json``.
    :raises CliError: файл не читается либо CSV не соответствует контракту.
    """
    path = Path(args.annotations)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise CliError(
            f"не удалось прочитать {path}: {concise(exc)}",
            code="baseline",
            exit_code=EXIT_USAGE,
        ) from exc
    try:
        rows = parse_annotations_csv(text)
    except ValueError as exc:
        raise CliError(concise(exc), code="baseline", exit_code=EXIT_USAGE) from exc

    report = compute_agreement(
        rows,
        na_label=Status.NOT_APPLICABLE.value,
        known_labels=[status.value for status in Status],
    )
    payload = _report_payload(report, str(path))
    if args.json:
        _print_json(payload)
    else:
        _print_report(payload)
    return EXIT_OK if payload["ok"] else EXIT_USAGE


def configure(sub: Any) -> None:
    """Регистрирует подкоманду ``baseline-agreement``."""
    parser = sub.add_parser(
        "baseline-agreement",
        help="согласие двух аннотаторов ручного baseline-эксперимента (Cohen's κ, "
        "отдельно по N/A; §11.5)",
    )
    parser.add_argument(
        "--annotations",
        required=True,
        help="путь к CSV-разметке (колонки product_id, param_id, annotator_a, "
        "annotator_b; пустая ячейка = не размечено)",
    )
    _add_common(parser)
    parser.set_defaults(handler=run)
