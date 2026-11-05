"""Даты актуальности артефактов корпуса в отчёте (план ``20`` §6, §7 F2).

План 20 §2: дата входит в **смысл** только ``bounded ❌`` и Coverage; для ``✅``/``⚠️``
и ``explicit ❌`` она — провенанс. Эта фаза публикует провенанс и **не** меняет ни одного
вердикта: ``evaluate``/``corpus_coverage`` здесь не вызываются, порогов «устарело» нет
(§6). Возраст измеряется и публикуется распределением, решение остаётся за читателем
(A3/AGENTS §2: «не ограничивать, а измерять»).

**Опорная точка — наблюдение, а не часы.** ``reference`` — самое свежее наблюдение
**прогона** (:attr:`~core.domain.models.CorpusSnapshot.corpus_as_of` ``["to"]``), а не
``now()``: отчёт обязан быть воспроизводимым (два вызова на одной БД дают байт-идентичные
файлы). «Насколько это старо сегодня» — вопрос читателя к своим часам.

**Знаменатель рядом с числителем.** ``cells_without_dated_evidence`` печатается вместе с
``cells`` (по продукту и по прогону): без него ноль читается как «все ячейки датированы»,
хотя ячеек могло не быть вовсе (прогон, остановленный до ``evaluate``).

Носители (второго источника нет):

* артефакт → наблюдение — ``corpus_artifact`` в БД **прогона** (план 20 §F2); у
  пост-хок ``hound report`` нет ``state.db``, а проекция уже содержит дату;
* evidence → артефакт — ``cell_evidence.doc_version_id`` (точное совпадение), затем
  ``cell_evidence.source_url`` → ``corpus_artifact.url_key`` через
  :func:`core.pipeline.digests.snapshot_url_key` — тот же ключ адреса, по которому
  артефакт входит в снимок корпуса. Новых формул адреса не заводится.

Модуль категорийно нейтрален (§1.3): продукт/параметр — строки данных, не термины.
"""

from __future__ import annotations

import csv
import datetime as _dt
import statistics
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from core.pipeline.digests import snapshot_url_key

__all__ = [
    "CELL_FRESHNESS_ARTIFACT",
    "CORPUS_FRESHNESS_ARTIFACT",
    "FRESHNESS_WARN_DAYS_DEFAULT",
    "CorpusFreshness",
    "build_corpus_freshness",
    "collect_corpus_freshness",
    "document_observation_freshness",
    "freshness_lines",
    "lag_days",
    "lag_seconds",
    "observation_freshness",
    "write_cell_freshness_rows",
    "write_corpus_freshness_artifact",
]

#: Имена артефактов в ``outputs/`` (конвенция каталога: ``yaml`` — измерение, ``csv`` — таблица).
CORPUS_FRESHNESS_ARTIFACT = "corpus_freshness.yaml"
CELL_FRESHNESS_ARTIFACT = "cell_freshness.csv"

#: Колонки ``cell_freshness.csv`` — по строке на ячейку **с** датой.
#:
#: ``evidence_as_of_*`` — даты артефактов, на которые опираются цитаты ячейки (F2);
#: ``absence_as_of`` — дата **слабой опоры** ``❌`` (I2d, план 20 §11.2), читается из
#: самой ячейки (``cells.absence_as_of``), а не пересчитывается: конвейер пишет её на Ш6
#: по носителю (наблюдение документа / снятие корня), и второй расчёт здесь разошёлся бы
#: с вердиктом. Пустое поле — «даты нет» (ячейка наличия, ``❓``/``failed``, строка до F3),
#: и это не ноль и не выдуманная дата.
CELL_FRESHNESS_FIELDS: tuple[str, ...] = (
    "product_id",
    "param_id",
    "evidence_as_of_from",
    "evidence_as_of_to",
    "absence_as_of",
    "artifacts",
)

#: Дефолт порога **отображения** для ``plan`` (дней). Не гейт и не слагаемое ``run_id``:
#: неделя — такт публикационного обновления (AGENTS §1.1), поэтому разброс больше недели
#: печатается предупреждением, а не молчанием. Значение объявлено здесь, чтобы докстринг
#: флага и код называли одно число.
FRESHNESS_WARN_DAYS_DEFAULT: float = 7.0


# ---------------------------------------------------------------------------
# Время: разбор, лаг, округление
# ---------------------------------------------------------------------------


def _instant(value: Any) -> _dt.datetime | None:
    """ISO-строка/``datetime`` → aware ``datetime`` (naive трактуется как UTC).

    ``None``/пустая строка/неразбираемое значение → ``None``: «даты нет» не выдаётся за
    дату. Naive-время приводится к UTC, иначе сравнение aware/naive уронило бы отчёт на
    смешанных записях (legacy-строки без смещения).
    """
    if isinstance(value, _dt.datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = _dt.datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_dt.UTC)
    return parsed


def lag_seconds(reference: Any, observed: Any) -> int | None:
    """Точный лаг в секундах: ``round((reference − observed).total_seconds())``.

    Это величина **решения**, а не печати: «корень отстал» определяется ею
    (``> 0``), поэтому разница в 5 секунд остаётся 5, а не превращается в ноль
    округлением до суток. ``None`` — одна из дат отсутствует или неразбираема.
    """
    ref = _instant(reference)
    obs = _instant(observed)
    if ref is None or obs is None:
        return None
    return round((ref - obs).total_seconds())


def lag_days(reference: Any, observed: Any) -> float | None:
    """Лаг в сутках: ``(reference − observed) / 86400``, округление до 2 знаков.

    Опорная точка — **наблюдение**, а не ``now()`` (план 20 §6): отчёт воспроизводим.
    ``None`` — одна из дат отсутствует или неразбираема; ноль не подставляется.

    Решение «отстаёт ли корень» принимается по **точному** интервалу
    (:func:`lag_seconds`), а эта величина — печать: поэтому корень с разницей в секунды
    попадает в ``stale_roots`` с ``lag_seconds > 0`` и ``lag_days: 0.0`` — это не
    противоречие, а разная точность, и ``lag_seconds`` его снимает.
    """
    ref = _instant(reference)
    obs = _instant(observed)
    if ref is None or obs is None:
        return None
    return round((ref - obs).total_seconds() / 86400.0, 2)


def _quantile_p90(values: list[float]) -> float:
    """90-й процентиль: ``statistics.quantiles(n=10, method="inclusive")[8]``.

    Способ назван явно, чтобы величина была воспроизводима: ``method="inclusive"``
    (десять корзин, крайние точки входят) даёт на тех же данных то же число. Одно
    наблюдение даёт само себя: квартильный расчёт на одном элементе не определён, а
    «не измерено» здесь было бы ложью — величина измерена, она одна.
    """
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=10, method="inclusive")[8]


def _distribution(lags: Iterable[float]) -> dict[str, Any]:
    """Распределение возраста: число артефактов, медиана, p90, максимум (дни, 2 знака).

    Вход — **точные** лаги в сутках (из ``total_seconds()``, без промежуточного
    округления): здесь только арифметика распределения, и округление выполняется один
    раз — при выводе. Поэтому ``max_days``/``p90_days`` считаются по миллисекундам и лишь
    потом печатаются. Пустой вход — нули и ``None`` вместо выдуманных чисел: «артефактов
    с датой нет» — честное состояние (§6: дата не выдумывается).
    """
    material = list(lags)
    if not material:
        return {"artifacts": 0, "median_days": None, "p90_days": None, "max_days": None}
    return {
        "artifacts": len(material),
        "median_days": round(statistics.median(material), 2),
        "p90_days": round(_quantile_p90(material), 2),
        "max_days": round(max(material), 2),
    }


# ---------------------------------------------------------------------------
# Внутреннее представление строки артефакта
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Artifact:
    """Артефакт прогона в терминах отчёта: адрес, версия, наблюдение, корень."""

    product_id: str
    url_key: str
    doc_version_id: str | None
    observed_at: str | None
    instant: _dt.datetime | None
    root_url: str | None


def _field(row: Any, name: str) -> Any:
    """Поле строки-объекта либо словаря: отчёт не зависит от формы носителя."""
    if isinstance(row, Mapping):
        return row.get(name)
    return getattr(row, name, None)


def _as_artifact(row: Any) -> _Artifact:
    """Нормализует строку ``corpus_artifact`` (или совместимый словарь)."""
    observed_at = _field(row, "observed_at")
    text = None if observed_at is None else str(observed_at)
    return _Artifact(
        product_id=str(_field(row, "product_id")),
        url_key=str(_field(row, "url_key")),
        doc_version_id=(
            None if _field(row, "doc_version_id") is None else str(_field(row, "doc_version_id"))
        ),
        observed_at=text,
        instant=_instant(text),
        root_url=(
            None if _field(row, "root_url") is None else str(_field(row, "root_url"))
        ),
    )


def _cell_key(cell: Any) -> tuple[str, str] | None:
    product_id = _field(cell, "product_id")
    param_id = _field(cell, "param_id")
    if product_id is None or param_id is None:
        return None
    return str(product_id), str(param_id)


def _refs_from_cells(cells: Iterable[Any]) -> list[dict[str, Any]]:
    """Ссылки evidence, которые ячейка несёт сама (``evidence``/``absence_evidence``).

    Штатно ``iter_cells`` evidence **не** восстанавливает (цитаты живут в
    ``cell_evidence``), поэтому основной источник — :meth:`StorePort.iter_cell_evidence_refs`.
    Здесь читается то, что уже приклеено к ячейке (синтетические/тестовые ячейки) — иначе
    отчёт зависел бы от наличия таблицы там, где дата уже есть в объекте.
    """
    refs: list[dict[str, Any]] = []
    for cell in cells:
        key = _cell_key(cell)
        if key is None:
            continue
        product_id, param_id = key
        items = list(_field(cell, "evidence") or []) + list(
            _field(cell, "absence_evidence") or []
        )
        for item in items:
            source_url = _field(item, "source_url")
            doc_version_id = _field(item, "doc_version_id")
            if not source_url and not doc_version_id:
                continue
            refs.append(
                {
                    "product_id": product_id,
                    "param_id": param_id,
                    "source_url": source_url,
                    "doc_version_id": doc_version_id,
                }
            )
    return refs


# ---------------------------------------------------------------------------
# Сборка
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CorpusFreshness:
    """Результат сборки: документ YAML/``--json`` и строки ``cell_freshness.csv``.

    Один расчёт — два выхода: файл и машинный payload не могут разойтись (конвенция
    «один сборщик», ``report.py``: манифест). ``document`` — JSON-совместимый словарь,
    который пишется ``yaml.safe_dump`` байт-в-байт детерминированно.
    """

    document: dict[str, Any]
    cell_rows: list[dict[str, Any]]


def _resolve_evidence(
    ref: Mapping[str, Any],
    *,
    by_version: Mapping[str, list[_Artifact]],
    by_key: Mapping[str, _Artifact],
) -> _Artifact | None:
    """Evidence → артефакт: сначала версия документа, затем адрес (план 20 §F2).

    Совпадение по версии предпочитает **датированную** строку: у одного
    ``doc_version_id`` бывает несколько адресов (дедуп по содержанию), и выбирать
    недатированную значило бы терять дату, которая в прогоне есть. Порядок внутри
    набора — ``url_key`` (содержание, не порядок обхода).
    """
    version = ref.get("doc_version_id")
    if version:
        candidates = sorted(by_version.get(str(version), ()), key=lambda row: row.url_key)
        dated = [row for row in candidates if row.instant is not None]
        if dated:
            return dated[0]
        if candidates:
            return candidates[0]
    source_url = ref.get("source_url")
    if source_url:
        return by_key.get(snapshot_url_key(str(source_url)))
    return None


def build_corpus_freshness(
    *,
    run_id: str,
    artifacts: Iterable[Any] = (),
    cells: Iterable[Any] = (),
    evidence_refs: Iterable[Mapping[str, Any]] = (),
    snapshot_as_of: Mapping[str, Any] | None = None,
    captures: Iterable[Any] = (),
) -> CorpusFreshness:
    """Собирает даты актуальности: прогон → продукты → ячейки (план 20 §6).

    :param artifacts: строки ``corpus_artifact`` прогона (единственный носитель дат).
    :param cells: ячейки прогона — множество ячеек, для которого считаются ``cells`` и
        ``cells_without_dated_evidence``; дата ячейки берётся из evidence, а не из ячейки.
        ``cells_without_dated_evidence`` печатается **вместе с** ``cells``: иначе ноль не
        отличим от «ячеек не было» (прогон, остановленный до ``evaluate``).
    :param evidence_refs: ссылки ``(product_id, param_id, source_url, doc_version_id)``
        из ``cell_evidence``.
    :param snapshot_as_of: ``corpus_as_of`` снимка корпуса прогона — **объявленная**
        опорная точка; при отсутствии берётся максимум по строкам артефактов (те же
        наблюдения, поэтому это не второй расчёт, а тот же факт без снимка).
    :param captures: снятия снимка (:class:`~core.domain.models.CorpusCaptureRef`) —
        источник корней продукта без датированных артефактов.

    Возраст **продукта** считается относительно ``reference`` (самое свежее наблюдение
    прогона): так разные продукты сравнимы одним числом. ``stale_roots`` — относительно
    самого свежего артефакта **своего** продукта (корень «отстал» внутри продукта; иначе
    свежий продукт объявил бы все свои корни устаревшими из-за соседа). Решение «stale»
    принимается по **точному** интервалу (:func:`lag_seconds` > 0), а ``lag_days`` —
    только для чтения: корень с разницей в секунды попадает в ``stale_roots`` с
    ``lag_seconds > 0`` и ``lag_days: 0.0`` — это разная точность, не противоречие.
    """
    rows = [_as_artifact(row) for row in artifacts]
    # Материализуется: ячейки читаются дважды (ссылки evidence и множество ячеек продукта),
    # и генератор на входе не должен исчерпаться на первом проходе.
    cell_list = list(cells)
    refs = [*_refs_from_cells(cell_list), *[dict(ref) for ref in evidence_refs]]
    captures_list = list(captures)

    all_instants = [row.instant for row in rows if row.instant is not None]

    # Опорная точка: объявленный снимок прогона, иначе максимум по строкам. Обе величины —
    # наблюдения артефактов, а не часы отчёта.
    declared_to = None if snapshot_as_of is None else snapshot_as_of.get("to")
    reference: _dt.datetime | None = _instant(declared_to)
    reference_raw = None if declared_to is None else str(declared_to)
    if reference is None:
        reference = max(all_instants) if all_instants else None
        reference_raw = None if reference is None else reference.isoformat()

    # ``corpus_as_of`` прогона: объявленный снимок, иначе min/max по датированным строкам.
    declared_from = None if snapshot_as_of is None else snapshot_as_of.get("from")
    if declared_from is not None and declared_to is not None:
        corpus_as_of: dict[str, str] | None = {
            "from": str(declared_from),
            "to": str(declared_to),
        }
    elif all_instants:
        run_pairs: list[tuple[_dt.datetime, str]] = [
            (row.instant, str(row.observed_at))
            for row in rows
            if row.instant is not None
        ]
        run_earliest = min(run_pairs, key=lambda pair: pair[0])
        run_latest = max(run_pairs, key=lambda pair: pair[0])
        corpus_as_of = {"from": run_earliest[1], "to": run_latest[1]}
    else:
        corpus_as_of = None

    products = {
        row.product_id for row in rows
    } | {
        str(_field(capture, "product_id"))
        for capture in captures_list
        if _field(capture, "product_id") is not None
    } | {
        key[0] for key in (_cell_key(cell) for cell in cell_list) if key is not None
    } | {str(ref.get("product_id")) for ref in refs if ref.get("product_id") is not None}

    by_product: dict[str, list[_Artifact]] = {}
    for row in rows:
        by_product.setdefault(row.product_id, []).append(row)

    # Корни продукта: адреса артефактов + корни снятий. Артефакт без корня корня не
    # образует (связь «артефакт → снятие» не выдумывается).
    roots_by_product: dict[str, set[str]] = {}
    for row in rows:
        if row.root_url:
            roots_by_product.setdefault(row.product_id, set()).add(row.root_url)
    for capture in captures_list:
        product_id = _field(capture, "product_id")
        root_url = _field(capture, "root_url")
        if product_id is not None and root_url:
            roots_by_product.setdefault(str(product_id), set()).add(str(root_url))

    # Ячейки по продуктам: полный набор ячеек прогона плюс ячейки, у которых есть evidence.
    cells_by_product: dict[str, set[str]] = {}
    #: Дата слабой опоры ``❌`` (I2d) — из записи ячейки, без второго прохода по ``cells``
    #: и без пересчёта: конвейер уже выбрал носитель, отчёт только публикует.
    absence_as_of_by_cell: dict[tuple[str, str], str | None] = {}
    for cell in cell_list:
        key = _cell_key(cell)
        if key is not None:
            cells_by_product.setdefault(key[0], set()).add(key[1])
            absence_as_of_by_cell.setdefault(key, getattr(cell, "absence_as_of", None))
    for link in refs:
        product_id = link.get("product_id")
        param_id = link.get("param_id")
        if product_id is not None and param_id is not None:
            cells_by_product.setdefault(str(product_id), set()).add(str(param_id))

    refs_by_cell: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for link in refs:
        product_id = link.get("product_id")
        param_id = link.get("param_id")
        if product_id is None or param_id is None:
            continue
        refs_by_cell.setdefault((str(product_id), str(param_id)), []).append(link)

    cell_rows: list[dict[str, Any]] = []
    product_documents: dict[str, dict[str, Any]] = {}

    for product_id in sorted(products):
        own = by_product.get(product_id, [])
        dated = [row for row in own if row.instant is not None]

        product_as_of: dict[str, str] | None = None
        if dated:
            pairs: list[tuple[_dt.datetime, str]] = [
                (row.instant, str(row.observed_at))
                for row in dated
                if row.instant is not None
            ]
            earliest = min(pairs, key=lambda pair: pair[0])
            latest = max(pairs, key=lambda pair: pair[0])
            product_as_of = {"from": earliest[1], "to": latest[1]}
        dated_instants = [row.instant for row in dated if row.instant is not None]
        product_reference = max(dated_instants) if dated_instants else None

        lags = (
            []
            if reference is None
            else [
                (reference - row.instant).total_seconds() / 86400.0  # type: ignore[operator]
                for row in dated
            ]
        )

        # Индексы резолва evidence → артефакт (в пределах продукта: чужой продукт не может
        # быть источником цитаты, I17).
        by_version: dict[str, list[_Artifact]] = {}
        by_key: dict[str, _Artifact] = {}
        for row in own:
            if row.doc_version_id:
                by_version.setdefault(row.doc_version_id, []).append(row)
            by_key.setdefault(row.url_key, row)

        product_cells = sorted(cells_by_product.get(product_id, ()))
        dated_count = 0
        for param_id in product_cells:
            hits: list[tuple[_dt.datetime, str, str]] = []
            for evidence_link in refs_by_cell.get((product_id, param_id), ()):
                resolved = _resolve_evidence(
                    evidence_link, by_version=by_version, by_key=by_key
                )
                if resolved is None or resolved.instant is None:
                    continue
                hits.append((resolved.instant, str(resolved.observed_at), resolved.url_key))
            if not hits:
                continue
            dated_count += 1
            hit_earliest = min(hits, key=lambda hit: hit[0])
            hit_latest = max(hits, key=lambda hit: hit[0])
            cell_rows.append(
                {
                    "product_id": product_id,
                    "param_id": param_id,
                    "evidence_as_of_from": hit_earliest[1],
                    "evidence_as_of_to": hit_latest[1],
                    "absence_as_of": absence_as_of_by_cell.get((product_id, param_id)),
                    "artifacts": len({hit[2] for hit in hits}),
                }
            )

        root_dates: dict[str, list[tuple[_dt.datetime, str]]] = {
            root: [] for root in roots_by_product.get(product_id, set())
        }
        for row in dated:
            if row.root_url:
                root_dates.setdefault(row.root_url, []).append(
                    (row.instant, str(row.observed_at))  # type: ignore[arg-type]
                )
        stale_roots: list[dict[str, Any]] = []
        roots_without = 0
        for root_url, entries in root_dates.items():
            if not entries:
                roots_without += 1
                continue
            root_latest = max(entries, key=lambda entry: entry[0])
            if product_reference is None:
                continue
            # Решение — по **точному** интервалу: разница в секунды делает корень
            # отставшим, даже если в сутках она печатается как 0.0. Округление в
            # решении не участвует ни здесь, ни для равных дат (ничьи — не stale).
            exact_seconds = lag_seconds(product_reference, root_latest[0])
            if exact_seconds is None or exact_seconds <= 0:
                continue
            stale_roots.append(
                {
                    "root_url": root_url,
                    "as_of": str(root_latest[1]),
                    "lag_seconds": exact_seconds,
                    "lag_days": lag_days(product_reference, root_latest[0]),
                }
            )
        stale_roots.sort(
            key=lambda item: (-int(item["lag_seconds"]), item["root_url"])
        )

        products_document = {
            "corpus_as_of": product_as_of,
            "corpus_age": _distribution(lags),
            "stale_roots": stale_roots,
            "roots_without_observation": roots_without,
            "artifacts_without_observation": len(own) - len(dated),
            # Знаменатель рядом с числителем: без него ноль не отличим от «ячеек не было».
            "cells": len(product_cells),
            "cells_without_dated_evidence": max(len(product_cells) - dated_count, 0),
        }
        product_documents[product_id] = products_document

    cell_rows.sort(key=lambda row: (row["product_id"], row["param_id"]))
    # Ячейки прогона — тот же вход, что у ``cells_without_dated_evidence`` (сумма по
    # продуктам), чтобы и на уровне прогона ноль имел знаменатель.
    total_cells = sum(
        len(cells_by_product.get(product_id, ())) for product_id in products
    )
    document: dict[str, Any] = {
        "artifact": "corpus_freshness",
        "run_id": run_id,
        "reference": reference_raw,
        "corpus_as_of": corpus_as_of,
        "cells": total_cells,
        "products": product_documents,
    }
    return CorpusFreshness(document=document, cell_rows=cell_rows)

# ---------------------------------------------------------------------------
# Чтение носителей и запись артефактов
# ---------------------------------------------------------------------------


def collect_corpus_freshness(
    *,
    store: Any,
    run_id: str,
    cells: Iterable[Any] = (),
    manifest: Any = None,
) -> CorpusFreshness:
    """Читает носители прогона и собирает даты (единственная точка сборки для Ш9).

    Источники — только БД **прогона**: ``corpus_artifact`` (даты артефактов) и
    ``cell_evidence`` (ссылки evidence на артефакты). ``state.db`` отчёт не открывает:
    у пост-хок ``hound report`` его нет, а проекция уже содержит те же наблюдения
    (план 20 §F2).

    ``store`` без читателя (лёгкий/синтетический стенд) — пустые строки, а не отказ:
    «дат не записано» видно в документе нулями (``corpus_as_of: null``), что честно
    отличается от «ноль артефактов с датой, потому что всё свежее».
    """
    artifacts: list[Any] = []
    artifact_reader = getattr(store, "iter_corpus_artifacts", None)
    if callable(artifact_reader):
        artifacts = list(artifact_reader(run_id))
    evidence_refs: list[Mapping[str, Any]] = []
    refs_reader = getattr(store, "iter_cell_evidence_refs", None)
    if callable(refs_reader):
        evidence_refs = list(refs_reader(run_id))
    snapshot = getattr(manifest, "corpus_snapshot", None)
    snapshot_as_of = getattr(snapshot, "corpus_as_of", None) if snapshot is not None else None
    captures = list(getattr(snapshot, "captures", ()) or ()) if snapshot is not None else []
    return build_corpus_freshness(
        run_id=run_id,
        artifacts=artifacts,
        cells=cells,
        evidence_refs=evidence_refs,
        snapshot_as_of=snapshot_as_of,
        captures=captures,
    )


def write_corpus_freshness_artifact(
    document: Mapping[str, Any], out_path: str | Path
) -> Path:
    """Пишет ``outputs/corpus_freshness.yaml`` тем же сериализатором, что манифест.

    ``sort_keys=True`` и отсутствие времени в документе дают байт-идентичный файл при
    повторном вызове на той же БД: опорная точка — наблюдение, а не часы (план 20 §6).
    Файл пишется **всегда**, в том числе пустым по данным: ``corpus_as_of: null`` —
    это факт «дат не записано», и он не должен выглядеть как отсутствие артефакта.
    """
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    text = yaml.safe_dump(
        dict(document), sort_keys=True, allow_unicode=True, default_flow_style=False
    )
    path.write_text(text, encoding="utf-8")
    return path


def write_cell_freshness_rows(
    rows: Iterable[Mapping[str, Any]], out_path: str | Path
) -> Path | None:
    """Пишет ``outputs/cell_freshness.csv`` — по строке на ячейку **с** датой.

    Пустой список → ``None`` (файл не создаётся): пустая таблица читалась бы как «ни у
    одной ячейки дат нет», тогда как отсутствие видно числом
    ``cells_without_dated_evidence`` в YAML. Тот же приём, что у ``stale_cells.csv``.
    """
    material = [
        {field: row.get(field) for field in CELL_FRESHNESS_FIELDS} for row in rows
    ]
    if not material:
        return None
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(CELL_FRESHNESS_FIELDS),
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(material)
    return path


# ---------------------------------------------------------------------------
# ``plan``: разброс дат наблюдений в state.db (порог отображения, не гейт)
# ---------------------------------------------------------------------------


def observation_freshness(
    observations: Mapping[str, Iterable[Any]],
    *,
    warn_days: float = FRESHNESS_WARN_DAYS_DEFAULT,
) -> dict[str, Any]:
    """Разброс дат наблюдений по продуктам для ``plan`` (план 20 §7 F2).

    Источник — ``state.db`` (``url_state.observed_at``), а не БД прогона: ``plan``
    исполняется **до** прогона, и ``corpus_artifact`` ещё не существует. Это не второй
    расчёт той же величины, а другая фаза и другой носитель (план 20 §7 F2).

    ``warn_days`` — **параметр отображения**: печатается и подсказывает читателю, но не
    блокирует прогон, не входит в ``EXECUTION_CONTRACT_FIELDS``/``run_id``/дайджесты и не
    является порогом «устарело» (§6: возраст публикуется числом). ``spread_days``
    печатается **всегда**, когда наблюдения есть; ``warnings`` — только при
    ``spread_days > warn_days``.
    """
    threshold = float(warn_days)
    products: list[dict[str, Any]] = []
    warnings: list[str] = []
    for key in sorted(str(item) for item in observations):
        pairs = [
            (instant, str(raw))
            for raw in observations[key]
            if (instant := _instant(raw)) is not None
        ]
        if not pairs:
            continue
        earliest = min(pairs, key=lambda pair: pair[0])
        latest = max(pairs, key=lambda pair: pair[0])
        spread = round((latest[0] - earliest[0]).total_seconds() / 86400.0, 2)
        products.append(
            {
                "product_id": key,
                "observations": len(pairs),
                "from": earliest[1],
                "to": latest[1],
                "spread_days": spread,
            }
        )
        if spread > threshold:
            warnings.append(
                f"наблюдения продукта {key} разнесены на {spread:.2f} дн. "
                f"(порог отображения {threshold:.2f}): части корпуса наблюдались в разные "
                "даты — evidence_as_of на карте будут разными"
            )
    return {"threshold_days": threshold, "products": products, "warnings": warnings}


def freshness_lines(block: Mapping[str, Any]) -> list[str]:
    """Человекочитаемые строки ``plan``: разброс — числом, всегда, когда он есть."""
    return [
        f"observation freshness: {row['product_id']} "
        f"{row['observations']} набл., {row['from']} → {row['to']}, "
        f"разброс {row['spread_days']:.2f} дн."
        for row in block.get("products") or ()
    ]


def document_observation_freshness(
    document: Mapping[str, Any],
    *,
    warn_days: float = FRESHNESS_WARN_DAYS_DEFAULT,
) -> dict[str, Any]:
    """Тот же разброс, но из **отчёта** прогона (``corpus_freshness``), а не из ``state.db``.

    Нужен ``run``: к концу прогона наблюдения ``state.db`` уже сдвинуты этим прогоном, и
    «разброс до измерения» по ним не восстановить. Документ отчёта — запись того, что
    реально вошло в корпус, поэтому разброс считается по его ``corpus_as_of``: интервал
    ``from → to`` и есть разнесение частей корпуса. Форма блока — та же, что у
    :func:`observation_freshness`, чтобы вывод двух фаз читался одинаково.
    """
    threshold = float(warn_days)
    products: list[dict[str, Any]] = []
    warnings: list[str] = []
    for product_id, block in sorted((document.get("products") or {}).items()):
        as_of = block.get("corpus_as_of") or {}
        if not as_of.get("from") or not as_of.get("to"):
            continue
        spread = lag_days(as_of["to"], as_of["from"])
        if spread is None:
            continue
        products.append(
            {
                "product_id": str(product_id),
                "observations": int((block.get("corpus_age") or {}).get("artifacts") or 0),
                "from": str(as_of["from"]),
                "to": str(as_of["to"]),
                "spread_days": spread,
            }
        )
        if spread > threshold:
            warnings.append(
                f"наблюдения продукта {product_id} разнесены на {spread:.2f} дн. "
                f"(порог отображения {threshold:.2f}): части корпуса наблюдались в разные "
                "даты — evidence_as_of на карте будут разными"
            )
    return {"threshold_days": threshold, "products": products, "warnings": warnings}
