"""coHound — reference-продукты профиля (план 27): декларация, факты, роли, refresh.

Три вещи и граница между ними:

* **декларация** (``sources/reference_products.yaml``) — какие reference-продукты у
  профиля, какие у них версии и корни документации. Авторское решение, git;
* **факты** (``sources/baseline_facts.csv``) — наблюдение «параметр P в версии V описан
  здесь», с дословной цитатой, хешем текста и датой. Прибор + подпись человека, git;
* **политика** (``baselines.csv``) — что наследуем, в каком режиме и с каким полом
  версии. Отдельный носитель, потому что у него другой владелец и другой срок жизни
  (§3.4 плана).

Модуль читает и пишет **только файлы пакета**: ни ``runs/``, ни ``state.db``, ни прогона
он не касается. Запись идёт через ``.authoring/apply_log.jsonl`` — существующий контур
авторских правок (I1).

Нейтрален к предметной категории (§1.3): ни одного названия продукта или вендора.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from core.config.passports import load_passports
from core.config.run_config import (
    load_baseline_facts,
    load_reference_products,
)
from core.domain.enums import FactReview, FactStatus
from core.domain.models import (
    BaselineFact,
    BaselineRow,
    CatalogRow,
    Passport,
    ReferenceProducts,
)
from core.pipeline.inheritance import (
    decide_inheritance,
    inheritance_basis,
    major_version,
)
from core.pipeline.package_io import append_apply_log, atomic_write_csv

__all__ = [
    "BASELINE_FACTS_RELPATH",
    "FACT_COLUMNS",
    "REFERENCE_PRODUCTS_RELPATH",
    "apply_refresh",
    "baseline_roles",
    "fact_rows",
    "load_facts",
    "load_package_facts",
    "load_package_reference_products",
    "refresh_diff",
    "review_facts",
    "write_facts",
]

#: Декларация reference-продуктов (git, формат ``reference_products/1``).
REFERENCE_PRODUCTS_RELPATH = "sources/reference_products.yaml"
#: Наблюдения о документации предка (git, формат ``baseline_facts/1``).
BASELINE_FACTS_RELPATH = "sources/baseline_facts.csv"

#: Канонический порядок колонок ``baseline_facts.csv`` (порядок — часть формата:
#: файл читается человеком и диффится в git, случайный порядок давал бы шумные диффы).
FACT_COLUMNS: tuple[str, ...] = (
    "baseline_id",
    "param_id",
    "version",
    "status",
    "url",
    "quote",
    "content_hash",
    "observed_at",
    "corpus_build_id",
    "review",
    "reviewed_by",
    "note",
)

#: Роли reference-продукта относительно когорты (вычисляемые, не объявляемые).
ROLE_ANCESTOR = "ancestor"
ROLE_PARALLEL = "parallel"


# ---------------------------------------------------------------------------
# Чтение
# ---------------------------------------------------------------------------


def load_package_reference_products(package_dir: str | Path) -> ReferenceProducts | None:
    """Декларация reference-продуктов пакета; ``None`` — файла нет.

    ``None`` и «объявлено пусто» различимы: первое — профиль ещё не завёл
    reference-продуктов, второе — завёл и объявил пустой набор (это осознанное решение,
    и линтер его увидит).
    """
    path = Path(package_dir) / REFERENCE_PRODUCTS_RELPATH
    return load_reference_products(path) if path.is_file() else None


def load_package_facts(package_dir: str | Path) -> list[BaselineFact]:
    """Факты пакета; пустой список — файла нет или он пуст (обе формы читаемы)."""
    path = Path(package_dir) / BASELINE_FACTS_RELPATH
    return load_baseline_facts(path) if path.is_file() else []


def load_facts(path: str | Path) -> list[BaselineFact]:
    """Факты из явного пути (``--facts`` для внешних наборов, тесты)."""
    return load_baseline_facts(path)


def load_package_passports(passports_dir: str | Path | None) -> dict[str, Passport]:
    """Паспорта каталога; отсутствующий каталог — ``{}`` (как у остальных читателей)."""
    if passports_dir is None:
        return {}
    return load_passports(passports_dir)


# ---------------------------------------------------------------------------
# Роль reference-продукта: вычисляется, а не объявляется
# ---------------------------------------------------------------------------


def baseline_roles(
    declaration: ReferenceProducts | None,
    passports: Mapping[str, Passport],
    *,
    cohort_products: Iterable[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Роль каждого reference-продукта относительно когорты (§3.1 плана).

    Роль **вычисляется**, а не объявляется полем ``kind``: в архетипе A «предок/параллель»
    совпадает для всех продуктов, в B — нет (один и тот же reference-продукт будет предком
    для одного продукта когорты и параллелью для другого). Отношение принадлежит паре
    «продукт × reference» и живёт в паспорте продукта; объявленное поле разошлось бы с
    этим источником (A3).

    :returns: ``{baseline_id: {"role": ancestor|parallel, "ancestors_of": [...],
        "current_version": ..., "versions": [...]}}``. ``ancestors_of`` — продукты
        когорты, объявившие эту линию предком, **в порядке паспорта** (порядок предков
        значим: он решает, кто победит при наследовании).
    """
    if declaration is None:
        return {}
    cohort = {str(pid) for pid in cohort_products} if cohort_products is not None else None
    declared_by: dict[str, list[str]] = {
        product.baseline_id: [] for product in declaration.products
    }
    for product_id in sorted(passports):
        if cohort is not None and product_id not in cohort:
            continue
        for upstream in passports[product_id].upstreams:
            declared_by.setdefault(str(upstream.baseline_id), []).append(product_id)
    roles: dict[str, dict[str, Any]] = {}
    for product in declaration.products:
        ancestors = declared_by.get(product.baseline_id, [])
        roles[product.baseline_id] = {
            "role": ROLE_ANCESTOR if ancestors else ROLE_PARALLEL,
            "ancestors_of": list(ancestors),
            "product_id": product.product_id,
            "canonical_name": product.canonical_name,
            "current_version": product.current_version,
            "versions": [version.version for version in product.versions],
        }
    return roles


# ---------------------------------------------------------------------------
# Факты: фильтры, запись, ревью
# ---------------------------------------------------------------------------


def fact_rows(
    facts: Sequence[BaselineFact],
    *,
    baseline_id: str | None = None,
    version: str | None = None,
    param_id: str | None = None,
    review: FactReview | str | None = None,
    status: FactStatus | str | None = None,
) -> list[BaselineFact]:
    """Факты с фильтрами (read-only); порядок — ``(baseline_id, param_id, version)``.

    Фильтр сравнивает **строки как есть** (в т.ч. версию): «18» и «18.4.1» в файле —
    разные записи, и приводить их здесь к мажору значило бы скрыть дефект декларации от
    того, кто смотрит на файл.
    """
    want_review = str(getattr(review, "value", review)) if review is not None else None
    want_status = str(getattr(status, "value", status)) if status is not None else None
    selected = [
        fact
        for fact in facts
        if (baseline_id is None or fact.baseline_id == baseline_id)
        and (version is None or fact.version == version)
        and (param_id is None or fact.param_id == param_id)
        and (want_review is None or fact.review.value == want_review)
        and (want_status is None or fact.status.value == want_status)
    ]
    return sorted(
        selected, key=lambda fact: (fact.baseline_id, fact.param_id, fact.version)
    )


def write_facts(package_dir: str | Path, facts: Sequence[BaselineFact]) -> Path:
    """Пишет ``sources/baseline_facts.csv`` детерминированно (атомарно, ``\\n``).

    Порядок строк — ``(baseline_id, param_id, version)``: файл живёт в git, и случайный
    порядок давал бы шумные диффы там, где изменилась одна строка. Колонки — строго
    :data:`FACT_COLUMNS`; ``None`` пишется пустой ячейкой.
    """
    ordered = sorted(
        facts, key=lambda fact: (fact.baseline_id, fact.param_id, fact.version)
    )
    path = Path(package_dir) / BASELINE_FACTS_RELPATH
    rows = [
        [
            fact.baseline_id,
            fact.param_id,
            fact.version,
            fact.status.value,
            fact.url,
            fact.quote,
            fact.content_hash,
            fact.observed_at,
            fact.corpus_build_id,
            fact.review.value,
            fact.reviewed_by,
            fact.note,
        ]
        for fact in ordered
    ]
    return atomic_write_csv(path, FACT_COLUMNS, rows)


def review_facts(
    package_dir: str | Path,
    *,
    baseline_id: str,
    actor: str,
    param_id: str | None = None,
    version: str | None = None,
    accept: bool | None = None,
    note: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Человеческая подпись под фактом: ``accepted``/``rejected`` (§5 плана).

    Наследует **только** ``accepted``, поэтому ревью — не украшение, а переключатель
    набора унаследованных ячеек. ``actor`` обязателен: подпись без автора неотличима от
    автоматики, а ``reviewed_by`` входит в провенанс факта (в ``run_id`` — только
    ``review``, см. §7.1).

    :param accept: ``True`` — принять, ``False`` — отклонить, ``None`` — снять подпись
        обратно в ``pending`` (полезно, когда факт пере-разведан).
    :returns: ``{"changed": [...], "skipped": n, "path": ..., "dry_run": bool}``.
    """
    if not (actor or "").strip():
        raise ValueError("baseline review: --actor обязателен (подпись без автора недоказуема)")
    target = FactReview.ACCEPTED if accept is True else (
        FactReview.REJECTED if accept is False else FactReview.PENDING
    )
    facts = load_package_facts(package_dir)
    changed: list[dict[str, str]] = []
    skipped = 0
    updated: list[BaselineFact] = []
    for fact in facts:
        matches = fact.baseline_id == baseline_id and (
            param_id is None or fact.param_id == param_id
        ) and (version is None or fact.version == version)
        if not matches:
            updated.append(fact)
            continue
        if fact.review is target and (note is None or fact.note == note):
            skipped += 1
            updated.append(fact)
            continue
        changed.append(
            {
                "param_id": fact.param_id,
                "version": fact.version,
                "from": fact.review.value,
                "to": target.value,
            }
        )
        updated.append(
            fact.model_copy(
                update={
                    "review": target,
                    "reviewed_by": actor if target is not FactReview.PENDING else None,
                    "note": note if note is not None else fact.note,
                }
            )
        )
    path = Path(package_dir) / BASELINE_FACTS_RELPATH
    if changed and not dry_run:
        write_facts(package_dir, updated)
        append_apply_log(
            package_dir,
            {
                "op": "baseline-review",
                "actor": actor,
                "baseline_id": baseline_id,
                "param_id": param_id,
                "version": version,
                "review": target.value,
                "changed": changed,
            },
        )
    return {
        "baseline_id": baseline_id,
        "review": target.value,
        "changed": changed,
        "skipped": skipped,
        "path": str(path),
        "dry_run": dry_run,
    }


# ---------------------------------------------------------------------------
# Refresh: новая мажорная версия — перемер и diff, без записи политики
# ---------------------------------------------------------------------------

#: Классы diff фактов между версиями (§13 плана).
DIFF_ADDED = "added"
DIFF_REMOVED = "removed"
DIFF_CHANGED = "changed"
DIFF_UNCHANGED = "unchanged"
DIFF_UNCLEAR = "unclear"


def refresh_diff(
    previous: Sequence[BaselineFact],
    current: Sequence[BaselineFact],
) -> dict[str, list[dict[str, Any]]]:
    """Классифицирует факты новой версии против принятых фактов предыдущей (§13).

    * ``added`` — ``present@new``, а в предыдущей версии факта не было (или он не
      ``present``) → кандидат строки, предлагается ``min_version`` = новая версия;
    * ``removed`` — ``present@old``, ``absent@new`` → **регрессия**: не применяется
      автоматически, показывается человеку (это может быть и переезд документации, и
      дефект извлечения — различить может только человек);
    * ``changed`` — ``present`` в обеих, но другая цитата/хеш → кандидат на замену;
    * ``unchanged`` — ``present`` в обеих с тем же текстом;
    * ``unclear`` — новая версия дала ``unclear`` → в очередь ревью.

    Сравнение — по ``(param_id)`` внутри одного ``baseline_id``: версии разные по
    построению, поэтому версия в ключ не входит.
    """
    def index(rows: Sequence[BaselineFact]) -> dict[str, BaselineFact]:
        chosen: dict[str, BaselineFact] = {}
        for fact in sorted(rows, key=lambda item: (item.param_id, item.version)):
            chosen.setdefault(fact.param_id, fact)
        return chosen

    old = index(previous)
    new = index(current)
    out: dict[str, list[dict[str, Any]]] = {
        DIFF_ADDED: [],
        DIFF_REMOVED: [],
        DIFF_CHANGED: [],
        DIFF_UNCHANGED: [],
        DIFF_UNCLEAR: [],
    }
    for param_id in sorted(set(old) | set(new)):
        before = old.get(param_id)
        after = new.get(param_id)
        was_present = before is not None and before.status is FactStatus.PRESENT
        if after is None:
            if before is not None and was_present:
                out[DIFF_REMOVED].append(
                    {
                        "param_id": param_id,
                        "reason": "missing_in_new_version",
                        "previous_version": before.version,
                    }
                )
            continue
        if after.status is FactStatus.UNCLEAR:
            out[DIFF_UNCLEAR].append(
                {"param_id": param_id, "version": after.version}
            )
            continue
        if after.status is FactStatus.ABSENT:
            if before is not None and was_present:
                out[DIFF_REMOVED].append(
                    {
                        "param_id": param_id,
                        "reason": "absent_in_new_version",
                        "previous_version": before.version,
                        "version": after.version,
                    }
                )
            continue
        if not was_present or before is None:
            out[DIFF_ADDED].append(
                {"param_id": param_id, "version": after.version}
            )
            continue
        same_text = (before.content_hash or "") == (after.content_hash or "") and (
            (before.quote or "") == (after.quote or "")
        )
        entry = {
            "param_id": param_id,
            "version": after.version,
            "previous_version": before.version,
        }
        if same_text:
            out[DIFF_UNCHANGED].append(entry)
        else:
            out[DIFF_CHANGED].append(entry)
    return out


def apply_refresh(
    package_dir: str | Path,
    *,
    baseline_id: str,
    version: str,
    actor: str,
    dry_run: bool = False,
    current_facts: Sequence[BaselineFact] | None = None,
) -> dict[str, Any]:
    """Применяет ``added``/``changed`` новой версии к ``baseline_facts.csv`` (§13).

    Применяются **только** ``added`` и ``changed``: ``removed`` не пишется никогда —
    исчезновение описания может быть регрессией upstream, переездом документации или
    дефектом извлечения, и различить их может только человек. ``baselines.csv`` не
    трогается вовсе: предложенные ``min_version`` выводятся отдельным списком, потому что
    неверный пол молча **пере**- или **недо**-наследует.

    :param current_facts: факты новой версии (уже собранные recon'ом); ``None`` — берутся
        из файла пакета (тогда ``added`` пуст по построению и применяются только ревью).
    """
    if not (actor or "").strip():
        raise ValueError("baseline refresh: --actor обязателен")
    existing = load_package_facts(package_dir)
    previous_version = sorted(
        {
            fact.version
            for fact in existing
            if fact.baseline_id == baseline_id
            and major_version(fact.version) != major_version(version)
        }
    )
    previous = [
        fact
        for fact in existing
        if fact.baseline_id == baseline_id
        and major_version(fact.version) != major_version(version)
    ]
    new_rows = (
        list(current_facts)
        if current_facts is not None
        else [
            fact
            for fact in existing
            if fact.baseline_id == baseline_id
            and major_version(fact.version) == major_version(version)
        ]
    )
    diff = refresh_diff(previous, new_rows)

    # Слияние: старая версия остаётся как история (git-факты — все объявленные версии),
    # новая перезаписывает свои строки.
    merged = [
        fact
        for fact in existing
        if not (
            fact.baseline_id == baseline_id
            and major_version(fact.version) == major_version(version)
        )
    ]
    applied: list[dict[str, Any]] = []
    for row in new_rows:
        if any(item["param_id"] == row.param_id for item in diff[DIFF_ADDED] + diff[DIFF_CHANGED]):
            merged.append(row)
            applied.append({"param_id": row.param_id, "version": row.version})
        elif any(item["param_id"] == row.param_id for item in diff[DIFF_UNCHANGED]):
            merged.append(row)
    if applied and not dry_run:
        write_facts(package_dir, merged)
        append_apply_log(
            package_dir,
            {
                "op": "baseline-refresh",
                "actor": actor,
                "baseline_id": baseline_id,
                "version": version,
                "applied": applied,
            },
        )
    return {
        "baseline_id": baseline_id,
        "version": version,
        "previous_versions": previous_version,
        "diff": diff,
        "applied": applied,
        "dry_run": dry_run,
        # §13: `min_version` — предложение, а не запись. Печатается отдельным списком.
        "proposed_min_version": [item["param_id"] for item in diff[DIFF_ADDED]],
    }


# ---------------------------------------------------------------------------
# Report: что унаследовано, кем и от какого предка
# ---------------------------------------------------------------------------


def _facts_summary(facts: Sequence[BaselineFact]) -> dict[str, Any]:
    """Распределение фактов по статусу/ревью/версии (числа, без интерпретации)."""
    by_status: dict[str, int] = {}
    by_review: dict[str, int] = {}
    by_version: dict[str, int] = {}
    for fact in facts:
        by_status[fact.status.value] = by_status.get(fact.status.value, 0) + 1
        by_review[fact.review.value] = by_review.get(fact.review.value, 0) + 1
        by_version[fact.version] = by_version.get(fact.version, 0) + 1
    return {
        "total": len(facts),
        "by_status": dict(sorted(by_status.items())),
        "by_review": dict(sorted(by_review.items())),
        "by_version": dict(sorted(by_version.items())),
    }


def inheritance_report(
    *,
    baselines: Sequence[BaselineRow],
    products: Sequence[str],
    passports: Mapping[str, Passport],
    facts: Sequence[BaselineFact],
    params: Sequence[CatalogRow] | None = None,
    run_id: str = "baseline-report",
) -> dict[str, Any]:
    """Что унаследовано, кем и от какого предка — **без прогона** (§10 плана).

    Считается тем же кодом, что Ш4 (:func:`decide_inheritance` /
    :func:`inheritance_basis`): второй формулы наследования нет, иначе отчёт описывал бы
    не то, что произойдёт в прогоне. Ничего не пишет.

    ``run_id`` здесь — только метка для идентификаторов evidence (прогона нет); на
    идентичность он не влияет, потому что никуда не сохраняется.
    """
    baseline_index = {(row.baseline_id, row.param_id): row for row in baselines}
    fact_index = {
        (fact.baseline_id, fact.param_id, fact.version): fact for fact in facts
    }
    rows: list[CatalogRow] = list(params or ())
    decisions = []
    errors: list[str] = []
    for product_id in sorted(str(pid) for pid in products):
        passport = passports.get(product_id)
        upstreams = list(passport.upstreams) if passport is not None else []
        if not upstreams:
            continue
        for param in rows:
            decisions.append(
                decide_inheritance(
                    run_id=run_id,
                    product_id=product_id,
                    param=param,
                    upstreams=upstreams,
                    baselines=baseline_index,
                    facts=fact_index,
                    errors=errors,
                )
            )
    cells = [decision.cell for decision in decisions if decision.cell is not None]
    basis = inheritance_basis(
        list(baselines),
        products=products,
        passports=passports,
        params=rows or None,
        cells=cells,
        decisions=decisions,
    )
    inherited = sum(
        1 for cell in cells if cell.state.value == "evaluated"
    )
    return {
        "measurement": "baseline_inheritance",
        "products": basis["products"],
        "totals": basis["totals"],
        "summary": basis["summary"],
        "inherited_cells": inherited,
        "errors": errors,
    }


def read_csv_preview(path: str | Path, limit: int = 5) -> list[dict[str, str]]:
    """Первые строки CSV как словари (для человекочитаемого вывода; ошибок не глотает)."""
    p = Path(path)
    if not p.is_file():
        return []
    with p.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        out: list[dict[str, str]] = []
        for index, row in enumerate(reader):
            if index >= limit:
                break
            out.append({key: value or "" for key, value in row.items()})
    return out


def facts_csv_text(facts: Sequence[BaselineFact]) -> str:
    """CSV-текст набора фактов (для ``--dry-run``: показать, что было бы записано)."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(list(FACT_COLUMNS))
    for fact in sorted(
        facts, key=lambda item: (item.baseline_id, item.param_id, item.version)
    ):
        writer.writerow(
            [
                fact.baseline_id,
                fact.param_id,
                fact.version,
                fact.status.value,
                fact.url or "",
                fact.quote or "",
                fact.content_hash or "",
                fact.observed_at or "",
                fact.corpus_build_id or "",
                fact.review.value,
                fact.reviewed_by or "",
                fact.note or "",
            ]
        )
    return buffer.getvalue()
