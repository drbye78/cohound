"""Согласие двух аннотаторов: Cohen's κ на разметке ячеек (§11.5).

Чистая арифметика согласия **без зависимостей** (stdlib; никаких numpy/pandas) и **без
I/O**: модуль оперирует уже разобранными строками разметки. Категорийно нейтрален (§1.3):
работает с «метками как даны», не знает словаря статусов — известный набор меток и метка
N/A передаются вызывающим (CLI читает их из домена/словаря).

Зачем нужен отдельный расчёт N/A (§11.5). N/A-решения задают **знаменатель Coverage**
(I6/I7), поэтому согласие по N/A публикуется **отдельно** от согласия по полному набору
меток: κ по всему набору и κ по бинарному решению «N/A vs не-N/A» могут расходиться, и
именно расхождение — предмет интереса методологической калибровки.

Контракт (детерминированный, без «угадывания»):

* метки берутся **как даны** — никакой нормализации, кроме обрезки внешних пробелов;
* ячейка, размеченная только одним аннотатором, **не** считается согласием и **исключается**
  из обоих κ (парность требуется обоим);
* κ **не определён** при ``n = 0`` либо когда ожидаемое согласие ``pe = 1`` (нулевая
  вариация маргиналов, обе шкалы вырождены в одну метку) — тогда ``kappa is None`` с
  причиной, а не выдуманное число;
* метка вне известного словаря не роняет расчёт: она попадает в :attr:`AgreementReport
  .unknown_labels` (счётчик по всем клеткам разметки) — обработка явная, не молчаливая.
"""

from __future__ import annotations

import csv
import io
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

__all__ = [
    "REQUIRED_COLUMNS",
    "NOT_NA",
    "AnnotationRow",
    "ConfusionMatrix",
    "KappaResult",
    "AgreementReport",
    "parse_annotations_csv",
    "compute_agreement",
]

#: Обязательные колонки CSV-разметки (порядок не важен; лишние колонки игнорируются).
#: Две колонки меток — два **независимых** аннотатора.
REQUIRED_COLUMNS: tuple[str, ...] = (
    "product_id",
    "param_id",
    "annotator_a",
    "annotator_b",
)

#: Метка-комплемент бинарной N/A-матрицы: всё, что не совпало с ``na_label``.
NOT_NA = "not_na"


@dataclass(frozen=True)
class AnnotationRow:
    """Одна размеченная ячейка: идентичность ``(product_id, param_id)`` + две метки.

    ``None`` в ``label_a``/``label_b`` означает «этот аннотатор ячейку не размечал»
    (пустая ячейка CSV) — такая строка непарная и в κ не входит.
    """

    product_id: str
    param_id: str
    label_a: str | None
    label_b: str | None

    @property
    def cell_id(self) -> str:
        """Канонический идентификатор ячейки ``product_id/param_id`` (для отчётов)."""
        return f"{self.product_id}/{self.param_id}"


@dataclass(frozen=True)
class ConfusionMatrix:
    """Матрица ошибок: строки — метки аннотатора A, столбцы — метки аннотатора B."""

    labels: tuple[str, ...]
    #: ``counts[a][b]`` — число ячеек с меткой ``a`` у A и ``b`` у B (0 заполнен).
    counts: Mapping[str, Mapping[str, int]]


@dataclass(frozen=True)
class KappaResult:
    """Cohen's κ с наблюдаемым/ожидаемым согласием и честной неопределённостью.

    ``kappa is None`` ⇔ расчёт не определён (``n = 0`` либо ``pe = 1``); ``undefined_reason``
    называет причину. ``observed_agreement``/``expected_agreement`` заполняются и тогда,
    когда они вычислимы (``po`` определён всегда при ``n > 0``).
    """

    kappa: float | None
    observed_agreement: float | None
    expected_agreement: float | None
    n: int
    undefined_reason: str | None = None

    @property
    def defined(self) -> bool:
        """Определён ли κ (``False`` — расчёт вырожден, число не выдумывается)."""
        return self.kappa is not None


@dataclass(frozen=True)
class AgreementReport:
    """Полный отчёт согласия по одному CSV-набору разметки (§11.5)."""

    total_items: int
    paired_items: int
    #: Идентификаторы непарных ячеек (размечены ровно одним аннотатором) — в κ не входят.
    unpaired_items: tuple[str, ...]
    #: κ по полному набору меток (как размечено).
    overall: KappaResult
    #: κ по бинарному решению «N/A vs не-N/A» (``None``, если ``na_label`` не задан).
    na_binary: KappaResult | None
    overall_confusion: ConfusionMatrix
    na_confusion: ConfusionMatrix | None
    #: Ячейки, где метки разошлись (по полному набору), — идентификаторы.
    disagreements: tuple[str, ...]
    #: Ячейки, где разошлось именно N/A-решение (подмножество ``disagreements``).
    na_disagreements: tuple[str, ...]
    na_label: str | None
    #: Число N/A-решений каждого аннотатора (по парным ячейкам).
    na_count_a: int
    na_count_b: int
    #: Метки вне переданного ``known_labels``: метка → число клеток (A и B суммарно).
    unknown_labels: Mapping[str, int] = field(default_factory=dict)


def _strip(value: object) -> str | None:
    """Обрезает внешние пробелы; пустая/отсутствующая ячейка → ``None`` (не размечено)."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_annotations_csv(text: str) -> list[AnnotationRow]:
    """Разбирает CSV-разметку в :class:`AnnotationRow` (контракт жёсткий, без догадок).

    Ожидаемые колонки — :data:`REQUIRED_COLUMNS`; порядок произвольный, лишние колонки
    игнорируются. Пустая ячейка метки → ``None`` (аннотатор не размечал).

    :raises ValueError: нет обязательной колонки; пустая ``product_id``/``param_id``;
        дубликат ячейки ``(product_id, param_id)`` (неоднозначная идентичность);
        строка без единой метки (нечего анализировать).
    """
    reader = csv.DictReader(io.StringIO(text))
    fieldnames = reader.fieldnames or []
    missing = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
    if missing:
        raise ValueError(
            "разметка: отсутствуют обязательные колонки "
            f"{missing} (ожидаются {list(REQUIRED_COLUMNS)})"
        )

    rows: list[AnnotationRow] = []
    seen: set[tuple[str, str]] = set()
    for lineno, raw in enumerate(reader, start=2):
        product_id = _strip(raw.get("product_id"))
        param_id = _strip(raw.get("param_id"))
        if product_id is None or param_id is None:
            raise ValueError(
                f"разметка строка {lineno}: product_id и param_id обязательны и непусты"
            )
        key = (product_id, param_id)
        if key in seen:
            raise ValueError(
                f"разметка строка {lineno}: дубликат ячейки {product_id}/{param_id}"
            )
        seen.add(key)
        label_a = _strip(raw.get("annotator_a"))
        label_b = _strip(raw.get("annotator_b"))
        if label_a is None and label_b is None:
            raise ValueError(
                f"разметка строка {lineno}: ячейка {product_id}/{param_id} без единой метки"
            )
        rows.append(AnnotationRow(product_id, param_id, label_a, label_b))
    return rows


def _confusion(pairs: Sequence[tuple[str, str]]) -> ConfusionMatrix:
    """Строит квадратную матрицу ошибок по парным меткам (строки — A, столбцы — B)."""
    labels = tuple(sorted({a for a, _ in pairs} | {b for _, b in pairs}))
    counts: dict[str, dict[str, int]] = {
        row: {col: 0 for col in labels} for row in labels
    }
    for a, b in pairs:
        counts[a][b] += 1
    return ConfusionMatrix(labels=labels, counts=counts)


def _cohen_kappa(pairs: Sequence[tuple[str, str]]) -> KappaResult:
    """Считает Cohen's κ для пар меток; ``None`` вместо числа при вырожденном расчёте.

    ``undefined`` ⇔ ``n = 0`` либо ``pe = 1`` (нулевая вариация маргиналов). В этих случаях
    возвращается ``kappa=None`` и названная причина — подставлять произвольное число
    (0.0/1.0) запрещено: это было бы выдуманное согласие.
    """
    n = len(pairs)
    if n == 0:
        return KappaResult(None, None, None, 0, "нет парных ячеек для расчёта")

    labels = sorted({a for a, _ in pairs} | {b for _, b in pairs})
    count_a = Counter(a for a, _ in pairs)
    count_b = Counter(b for _, b in pairs)
    observed = sum(1 for a, b in pairs if a == b) / n
    expected = sum((count_a[label] / n) * (count_b[label] / n) for label in labels)

    if expected >= 1.0 - 1e-12:
        return KappaResult(
            None,
            observed,
            expected,
            n,
            "нулевая вариация маргиналов (ожидаемое согласие = 1)",
        )
    kappa = (observed - expected) / (1.0 - expected)
    return KappaResult(kappa, observed, expected, n)


def compute_agreement(
    rows: Sequence[AnnotationRow],
    *,
    na_label: str | None = None,
    known_labels: Sequence[str] | None = None,
) -> AgreementReport:
    """Считает согласие двух аннотаторов: полный набор меток и бинарное N/A-решение.

    :param na_label: метка, означающая N/A; при ``None`` N/A-специфичный κ не считается.
    :param known_labels: известный словарь меток (для отчёта ``unknown_labels``); при
        ``None`` проверка на неизвестные метки не выполняется.
    """
    paired = [r for r in rows if r.label_a is not None and r.label_b is not None]
    unpaired = [r for r in rows if r.label_a is None or r.label_b is None]
    pairs: list[tuple[str, str]] = [
        (r.label_a, r.label_b)
        for r in paired
        if r.label_a is not None and r.label_b is not None
    ]

    overall = _cohen_kappa(pairs)
    overall_confusion = _confusion(pairs)
    disagreements = tuple(r.cell_id for r in paired if r.label_a != r.label_b)

    na_binary: KappaResult | None = None
    na_confusion: ConfusionMatrix | None = None
    na_disagreements: tuple[str, ...] = ()
    na_count_a = 0
    na_count_b = 0
    if na_label is not None:
        na_pairs = [
            (na_label if a == na_label else NOT_NA, na_label if b == na_label else NOT_NA)
            for a, b in pairs
        ]
        na_binary = _cohen_kappa(na_pairs)
        na_confusion = _confusion(na_pairs)
        na_count_a = sum(1 for a, _ in pairs if a == na_label)
        na_count_b = sum(1 for _, b in pairs if b == na_label)
        na_disagreements = tuple(
            r.cell_id
            for r in paired
            if (r.label_a == na_label) != (r.label_b == na_label)
        )

    unknown: dict[str, int] = {}
    if known_labels is not None:
        known = set(known_labels)
        for row in rows:
            for label in (row.label_a, row.label_b):
                if label is not None and label not in known:
                    unknown[label] = unknown.get(label, 0) + 1

    return AgreementReport(
        total_items=len(rows),
        paired_items=len(paired),
        unpaired_items=tuple(r.cell_id for r in unpaired),
        overall=overall,
        na_binary=na_binary,
        overall_confusion=overall_confusion,
        na_confusion=na_confusion,
        disagreements=disagreements,
        na_disagreements=na_disagreements,
        na_label=na_label,
        na_count_a=na_count_a,
        na_count_b=na_count_b,
        unknown_labels=dict(sorted(unknown.items())),
    )
