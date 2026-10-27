"""Качество поддержки карты: два замера, которые раньше не имели производителя (A3).

Оба замера считаются в :mod:`core.pipeline.evaluate` и до этого модуля вызывались только
из тестов: числа существовали, но их не видел ни один артефакт. Здесь они получают
единственного производителя — отчёт Ш9.

**Первый замер — «о предмете ли доказательство»** (:func:`support_overlap_scan`). I22
требует непустую цитату, и строка шапки документа формально её исполняет. Живой прогон
показал масштаб: у 9 из 32 ``✅`` продукта-документации ни одна цитата не упоминает
предмет параметра, у 8 из них единственная «цитата» — шапка вида
«<продукт> <версия> (<дата>)». Этот класс называется ``fabricated_support`` и не ловится
ни I22, ни ``relevant_evidence_coverage`` (шапка дословно присутствует в чанке).

**Второй замер — концентрация поддержки** (:func:`support_concentration`). Там, где
носителем доказательства служит наследование, одна паспортная цитата продукта
размножается на все его параметры: 19 разных параметров держатся на одной и той же
визитке. Карта при этом утверждает больше, чем доказала, и ``distinct_quotes = 1`` при
``max_share = 1.0`` говорит об этом одним числом, без порогов и без изменения вердиктов.

Оба замера — **наблюдения**: они не меняют статус ячейки, не влияют на Capability,
Coverage и публикуемость и не вводят порогов (A3: подобранное число было бы
предположением). Публикуется величина и адреса, решение остаётся за человеком.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import yaml

from core.pipeline.evaluate import support_concentration, support_overlap_scan

__all__ = [
    "SUPPORT_QUALITY_ARTIFACT",
    "support_quality",
    "support_quality_line",
    "write_support_quality_artifact",
]

#: Имя артефакта в ``outputs/``.
SUPPORT_QUALITY_ARTIFACT = "support_quality.yaml"


def _params_by_id(catalog: Any) -> dict[str, Any]:
    """Каталог по ``param_id`` — независимо от того, дали его словарём или списком."""
    if isinstance(catalog, Mapping):
        return {str(key): row for key, row in catalog.items()}
    return {str(row.param_id): row for row in (catalog or ())}


def _quotes_by_cell(store: Any, run_id: str) -> dict[tuple[str, str], list[str]]:
    """Цитаты доказательств прогона по ключу ячейки — из ``cell_evidence``.

    Носитель один (:meth:`~core.ports.store.StorePort.iter_cell_quotes`): замер обязан
    видеть и ``direct``, и ``derived``, потому что наблюдавшийся дефект живёт именно в
    производных цитатах наследования. Стор без читателя (синтетический бандл в тестах) —
    пустая карта: «не прочитано» не выдаётся за «цитат нет» на уровне агрегата, оба
    замера в этом случае просто не находят материала.
    """
    grouped: dict[tuple[str, str], list[str]] = {}
    reader = getattr(store, "iter_cell_quotes", None)
    if not callable(reader):
        return grouped
    for entry in reader(run_id):
        key = (str(entry["product_id"]), str(entry["param_id"]))
        grouped.setdefault(key, []).append(str(entry["citation_quote"]))
    return grouped


def support_quality(
    *,
    cells: Iterable[Any],
    catalog: Any,
    store: Any = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Считает оба замера качества поддержки по прогону (JSON-совместимый словарь).

    :param cells: ячейки прогона.
    :param catalog: каталог параметров (источник предмета замера).
    :param store: источник цитат; без него оба замера не находят материала.
    :param run_id: идентификатор прогона для чтения ``cell_evidence``.
    :returns: ``{"overlap": …, "concentration": …}`` — два независимых замера, а не
        сведённый балл: они отвечают на разные вопросы («цитата о том ли?» и «сколько
        карт держится на одной цитате?»), и складывать их значило бы выдумать метрику.
    """
    material = list(cells)
    quotes = (
        _quotes_by_cell(store, run_id)
        if store is not None and run_id is not None
        else {}
    )
    return {
        "overlap": support_overlap_scan(material, _params_by_id(catalog), quotes),
        "concentration": support_concentration(quotes, material),
    }


def write_support_quality_artifact(
    payload: Mapping[str, Any],
    out_path: str | Path,
    *,
    run_id: str | None = None,
) -> Path | None:
    """Пишет ``outputs/support_quality.yaml`` — оба замера рядом с их адресами.

    Файл пишется только когда есть что показать (хотя бы один presence-элемент): пустой
    артефакт читался бы как «всё в порядке», хотя означает «замер не нашёл материала».
    ``None`` — файла нет.
    """
    overlap = payload.get("overlap") or {}
    concentration = payload.get("concentration") or {}
    if not overlap.get("cells_considered") and not concentration.get("cells_present"):
        return None
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    document: dict[str, Any] = {
        "artifact": "support_quality",
        "note": (
            "наблюдения, не гейты: вердикты, Capability, Coverage и публикуемость от них "
            "не зависят; порогов нет — публикуются числа и адреса"
        ),
        "overlap": dict(overlap),
        "concentration": dict(concentration),
    }
    if run_id is not None:
        document["run_id"] = run_id
    path.write_text(
        yaml.safe_dump(document, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return path


def support_quality_line(payload: Mapping[str, Any]) -> str | None:
    """Одна строка для оператора: ``None`` — замерам нечего сказать.

    Строка называет **оба** факта, потому что по отдельности каждый выглядит терпимо:
    «14 цитат не о предмете» и «19 ячеек на одной цитате» — это разные дефекты одной
    карты, и человеку нужен повод открыть файл.
    """
    overlap = payload.get("overlap") or {}
    concentration = payload.get("concentration") or {}
    parts: list[str] = []
    flagged = int(overlap.get("cells_flagged") or 0)
    considered = int(overlap.get("cells_considered") or 0)
    if flagged:
        by_status = overlap.get("by_status") or {}
        detail = ", ".join(f"{key} {value}" for key, value in sorted(by_status.items()))
        parts.append(
            f"доказательство не о предмете: {flagged} из {considered}"
            + (f" ({detail})" if detail else "")
        )
    worst: tuple[float, str, int, int] | None = None
    for product_id, entry in sorted((concentration.get("products") or {}).items()):
        share = float(entry.get("max_share") or 0.0)
        count = int(entry.get("top_quote_count") or 0)
        if count < 2:
            continue
        if worst is None or share > worst[0]:
            worst = (share, str(product_id), count, int(entry.get("distinct_quotes") or 0))
    if worst is not None:
        share, product_id, count, distinct = worst
        parts.append(
            f"концентрация поддержки: {product_id} — {count} ячеек на одной цитате "
            f"(доля {share:.2f}, различных цитат {distinct})"
        )
    return "; ".join(parts) if parts else None
