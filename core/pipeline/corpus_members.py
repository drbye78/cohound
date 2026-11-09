"""Члены контейнеров, не попавшие в корпус: **почему** файла из архива нет — файлом и числом.

Зачем отдельный артефакт. Спуск в контейнер пишет строку на **каждый** член
(``corpus_members``, B3): ингестнутый получает ``doc_id``, пропущенный — ``skip_reason``
(лимит размера, неподдерживаемый формат, битый член, цикл по хешу). Причина пропуска
существовала только в таблице прогона и никуда не публиковалась, поэтому вопрос «почему
этого файла из архива нет в корпусе» не имел ответа в ``outputs/``: отсутствие документа
выглядело одинаково при «файла не было», «файл отбит лимитом» и «краул до него не дошёл».

Что публикуется:

* ``outputs/corpus_members.csv`` — строка на **пропущенного** члена: продукт, архив,
  путь члена, синтетический URL, формат, заявленный размер, причина, глубина, URL
  архива. Пустое число означает «не измерено», не ноль (A3);
* блок сводки (:func:`corpus_members_summary`) — те же строки, свёрнутые до продукта и
  причины, чтобы факт был виден без разбора CSV (``--json`` отчёта).

Источник — ``store.iter_corpus_members()``: read-only проекция персистентного спуска, а
не память прогона, поэтому resume и пост-хок ``hound report`` называют тот же факт.
Сбой чтения не роняет отчёт (он вторичен к фазе): сводка тогда пуста, а файл не
создаётся — «не прочитали» честнее выдуманного «пропусков не было».

Границы (важно не смешивать три разных предмета):

* :mod:`core.pipeline.partial_docs` (``partial_docs.csv``) — документ, который **был**
  в корпусе, но прочитан не целиком (страницы/символы). Здесь — член контейнера,
  которого в корпусе **нет вовсе**, и вопрос не «сколько прочитано», а «почему пропущен».
* ``dedup_ledger.csv`` (план 23 §465) — решения **дедупликации** уже принятых
  документов (какой дубль кем заменён). Здесь дедупа нет: ``skip_reason`` — отказ
  принять член, а не выбор между двумя принятыми.
* ``artifacts_gap.yaml`` — пробелы **краула** (что не удалось скачать). Член архива,
  отбитый политикой контейнера, краулом получен: это решение прибора, а не пробел сети.

Нейтрален (§1.3): здесь нет названий продуктов/вендоров — только классы факта.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable
from pathlib import Path
from typing import Any

__all__ = [
    "CSV_COLUMNS",
    "corpus_members_summary",
    "write_corpus_members_artifact",
]

#: Колонки ``outputs/corpus_members.csv`` (порядок стабилен: файл читают глазами и скриптом).
CSV_COLUMNS: tuple[str, ...] = (
    "product_id",
    "archive_content_hash",
    "member_path",
    "member_url",
    "member_format",
    "declared_size",
    "skip_reason",
    "depth",
    "archive_url",
)


def _skipped_members(store: Any, run_id: str | None) -> list[Any]:
    """Члены контейнеров с ``skip_reason`` (детерминированно), сбои чтения → ``[]``.

    ``iter_corpus_members`` не принимает ``run_id``: в штатной раскладке БД принадлежит
    одному прогону, поэтому пустой ``run_id`` здесь означает «источника нет», а не повод
    читать чужую проекцию. В общей ``--db`` таблица — объединение прогонов, поэтому
    строка с чужим ``run_id`` отбрасывается (``None`` — старые строки без прогона: их
    принадлежность не установить, и терять измеренный факт из-за отсутствия метки нельзя).
    Отбор по ``skip_reason is not None`` — тот же предикат, что у
    :func:`corpus_members_summary` и CSV: сводка и файл считаются из одного набора.
    """
    if store is None or not run_id:
        return []
    reader = getattr(store, "iter_corpus_members", None)
    if not callable(reader):
        return []
    try:
        members = [
            member
            for member in reader()
            if getattr(member, "skip_reason", None) is not None
            and getattr(member, "run_id", None) in (None, run_id)
        ]
    except Exception:  # noqa: BLE001 — отчёт вторичен к фазе
        return []
    return sorted(
        members,
        key=lambda member: (
            str(getattr(member, "product_id", "")),
            str(getattr(member, "archive_content_hash", "")),
            str(getattr(member, "member_path", "")),
        ),
    )


def corpus_members_summary(
    store: Any,
    run_id: str | None,
    *,
    products: Iterable[str] = (),
) -> dict[str, Any]:
    """Блок ``corpus_members`` отчёта: пропущенные члены по продукту и причине.

    Всегда возвращает словарь, даже пустой: отсутствие пропусков — тоже факт, и он
    должен быть виден нулём, а не отсутствием ключа. ``by_product`` содержит **нули**
    для всех продуктов плана (симметрия с ``partial_docs_summary``/``manual_artifacts``):
    «у этого продукта пропусков нет» отличается от «продукт забыли». Продукты вне плана
    не выдумываются, но встреченные в строках сохраняются — их число реально измерено.
    """
    planned = [str(product) for product in products]
    empty: dict[str, Any] = {
        "members_without_document": 0,
        "by_product": {product: 0 for product in planned},
        "by_reason": {},
        "by_product_reason": {},
    }
    members = _skipped_members(store, run_id)
    if not members:
        return empty
    by_product: dict[str, int] = {product: 0 for product in planned}
    by_reason: dict[str, int] = {}
    by_product_reason: dict[str, dict[str, int]] = {}
    for member in members:
        product_id = str(member.product_id)
        reason = str(member.skip_reason)
        by_product[product_id] = by_product.get(product_id, 0) + 1
        by_reason[reason] = by_reason.get(reason, 0) + 1
        reasons = by_product_reason.setdefault(product_id, {})
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "members_without_document": len(members),
        "by_product": dict(sorted(by_product.items())),
        "by_reason": dict(sorted(by_reason.items())),
        "by_product_reason": {
            product_id: dict(sorted(reasons.items()))
            for product_id, reasons in sorted(by_product_reason.items())
        },
    }


def write_corpus_members_artifact(
    store: Any, run_id: str | None, path: str | Path
) -> Path | None:
    """Пишет ``outputs/corpus_members.csv``; пропущенных членов нет → ``None``.

    Пустой файл — не артефакт (как ``partial_docs.csv``): он выглядел бы как «искали и
    ничего не нашли» там, где контейнеров не было вовсе. Отсутствие строк возвращает
    ``None``, а не таблицу с одной шапкой.

    Число печатается пустой строкой, если размер не измерен: ``0`` байт и «неизвестно» —
    разные факты. Порядок строк детерминирован — ``(product_id, archive_content_hash,
    member_path)``: повторный отчёт не должен давать другой файл на тех же данных.
    """
    if not run_id:
        return None
    members = _skipped_members(store, run_id)
    if not members:
        return None
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(CSV_COLUMNS), extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        for member in members:
            writer.writerow(
                {
                    "product_id": str(member.product_id),
                    "archive_content_hash": str(member.archive_content_hash),
                    "member_path": str(member.member_path),
                    "member_url": str(getattr(member, "member_url", None) or ""),
                    "member_format": str(getattr(member, "member_format", None) or ""),
                    "declared_size": _number(getattr(member, "declared_size", None)),
                    "skip_reason": str(member.skip_reason),
                    "depth": _number(getattr(member, "depth", None)),
                    "archive_url": str(getattr(member, "archive_url", None) or ""),
                }
            )
    return target


def _number(value: Any) -> str:
    """Число в CSV либо пустая строка («не измерено» ≠ ``0``)."""
    return str(int(value)) if isinstance(value, int) and not isinstance(value, bool) else ""