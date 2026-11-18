"""coHound — предложение строк политики наследования из наблюдений (план 27 §13).

Разрыв, который закрывает модуль: ``hound baseline recon`` пишет **наблюдения**
(``sources/baseline_facts.csv``), ``hound baseline review`` их подписывает, а
``baselines.csv`` — **политику** (что наследуем, с какого пола версии и в каком режиме) —
до сих пор писал только человек через ``hound package baseline add``. Строки политики не
появлялись из фактов вовсе, поэтому подписанный факт не давал ни одной унаследованной
ячейки: между наблюдением и политикой стояла ручная сверка 181 параметра.

Здесь она заменена **детерминированным предложением** (без LLM и без сети):

* ``direct`` — есть ``present`` **и** ``accepted`` факт хотя бы на одной объявленной
  версии; ``min_version`` — **первый объявленный мажор**, где такой факт есть (порядок
  версий берётся из декларации, а не из алфавита: ``"18"`` раньше ``"9"`` не потому, что
  строка короче, а потому что так объявлено);
* ``conditional`` — то же, но карта **этой** версии несёт на ячейке параметра
  ``review_reason``, начинающийся с ``support_not_about_subject``: цитата найдена, но она
  не о предмете, поэтому наследование возможно, а не безусловно;
* ``needs_signature`` — факт ``present``, но подписи нет: **прибор не подписывает за
  человека** (A3), поэтому строка публикуется отдельным списком, а не предлагается;
* ``no_evidence`` — только ``unclear``/``absent`` либо факта нет ни на одной версии;
* ``existing`` — строка ``(baseline_id, param_id)`` уже есть в ``baselines.csv``: она не
  предлагается повторно и ``--apply`` её не трогает.

Артефакт — ``sources/baselines.candidate.csv``: ``decision`` у предложенных строк **пуста**
(её заполняет человек), у непредложенных — ``skip``/``existing``, чтобы отказ прибора был
виден, а не выглядел пропуском. ``--apply`` пишет ``baselines.csv`` только по заполненному
``decision`` и отказывает, если хоть одна предложенная строка не размечена: разметку не
выдумываем (та же конвенция, что у ``scripts/prepare_*.py``).

Нейтрален к предметной категории (§1.3): ни одного названия продукта или вендора.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from core.domain.enums import FactReview, FactStatus, InheritanceMode
from core.domain.models import BaselineFact, BaselineRow, CatalogRow
from core.pipeline.inheritance import major_version
from core.pipeline.package_io import atomic_write_csv

__all__ = [
    "CANDIDATES_RELPATH",
    "CANDIDATE_COLUMNS",
    "DECISION_ACCEPT",
    "DECISION_EXISTING",
    "DECISION_REJECT",
    "DECISION_SKIP",
    "REASON_EXISTING",
    "REASON_FACT_PRESENT_ACCEPTED",
    "REASON_MAP_SUPPORT_UNRELATED",
    "REASON_NEEDS_SIGNATURE",
    "REASON_NO_EVIDENCE",
    "SUPPORT_NOT_ABOUT_SUBJECT",
    "apply_candidates",
    "candidates_path",
    "load_candidates",
    "propose_baselines",
    "write_candidates",
]

#: Куда пишется предложение: рядом с фактами и политикой, в ``sources/`` (git, разметка).
#: **Файл на линию**, а не на пакет: у архетипа B предков несколько, и общий файл означал бы,
#: что предложение по второй линии затирает незаполненные решения первой (``apply``
#: отказал бы на строках чужой линии, но разметка была бы уже потеряна).
CANDIDATES_RELPATH = "sources/baselines.{baseline_id}.candidate.csv"


def candidates_path(package_dir: str | Path, baseline_id: str) -> Path:
    """Путь артефакта предложения для линии (``sources/baselines.{id}.candidate.csv``).

    ``baseline_id`` — slug объявленной линии (``upstream-pg``), поэтому имя файла не
    содержит разделителей пути; проверка всё равно есть: идентификатор приходит из
    ``reference_products.yaml``, а не из аргумента командной строки.
    """
    if "/" in baseline_id or "\\" in baseline_id or baseline_id in {"", ".", ".."}:
        raise ValueError(f"baseline_id {baseline_id!r} не годится для имени файла")
    return Path(package_dir) / CANDIDATES_RELPATH.format(baseline_id=baseline_id)

#: Канонический порядок колонок кандидата. Порядок — часть формата: файл читает и
#: заполняет человек, поэтому ``decision`` стоит рядом с ``inheritance_mode``, а
#: ``reason`` — последним машинным полем перед версиями-носителями.
CANDIDATE_COLUMNS: tuple[str, ...] = (
    "baseline_id",
    "param_id",
    "min_version",
    "inheritance_mode",
    "decision",
    "reason",
    "evidence_versions",
)

#: Решения человека над предложенной строкой (пишутся в ``decision``).
DECISION_ACCEPT = "accept"
DECISION_REJECT = "reject"
DECISION_SKIP = "skip"
#: Строка уже есть в ``baselines.csv`` — ``--apply`` её не трогает.
DECISION_EXISTING = "existing"

#: Машинные причины (токены, а не проза: по ним считается сводка и снимается регрессия).
REASON_FACT_PRESENT_ACCEPTED = "fact_present_accepted"
REASON_MAP_SUPPORT_UNRELATED = "map_support_unrelated"
REASON_NEEDS_SIGNATURE = "needs_signature"
REASON_EXISTING = "existing"
REASON_NO_EVIDENCE = "no_evidence"

#: Причина на ячейке карты: цитата найдена, но она **не о предмете** параметра.
#:
#: Токен пишет оценщик (``core.pipeline.evaluate.SUPPORT_NOT_ABOUT_SUBJECT``); здесь он
#: только **читается** с ячейки ``cells.jsonl``. Своя константа, а не импорт: модуль
#: предложения не должен тянуть оценщик (LLM-порты) ради одной строки, а расхождение
#: значений ловит тест ``test_support_token_matches_evaluator``.
SUPPORT_NOT_ABOUT_SUBJECT = "support_not_about_subject"

#: Разделитель списка версий внутри одной ячейки CSV (``|``, а не ``,``: колонка читается
#: глазом и не должна ломать разбор файла).
_VERSION_SEP = "|"


# ---------------------------------------------------------------------------
# Правило: наблюдение → строка политики
# ---------------------------------------------------------------------------


def _facts_by_param(
    facts: Iterable[BaselineFact], baseline_id: str
) -> dict[str, list[BaselineFact]]:
    """Факты одной линии, сгруппированные по ``param_id`` (порядок строк файла сохранён)."""
    grouped: dict[str, list[BaselineFact]] = {}
    for fact in facts:
        if fact.baseline_id != baseline_id:
            continue
        grouped.setdefault(fact.param_id, []).append(fact)
    return grouped


def _versions_with(
    rows: Sequence[BaselineFact],
    declared: Sequence[str],
    *,
    review: FactReview | None = None,
) -> list[str]:
    """Объявленные версии (в порядке декларации), где факт ``present`` (и подпись, если задана).

    Сравнение — по **мажору**: факт записан версией как в документации (``18.4.1``), а
    декларация объявляет мажор (``18``). Приводить версию факта к мажору здесь обязательно:
    иначе подписанный факт ``18.4.1`` не подтвердил бы объявленную ``18`` и строка политики
    не появилась бы при полном наборе доказательств.
    """
    found: list[str] = []
    for version in declared:
        major = major_version(version)
        for fact in rows:
            if fact.status is not FactStatus.PRESENT:
                continue
            if review is not None and fact.review is not review:
                continue
            if major is not None and major_version(fact.version) == major:
                found.append(version)
                break
    return found


def _cells_by_param(
    cells: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Mapping[str, Any]]:
    """Ячейки карты по ``param_id`` (``cells.jsonl`` — одна строка на параметр)."""
    index: dict[str, Mapping[str, Any]] = {}
    for row in cells or ():
        param_id = row.get("param_id")
        if param_id is None:
            continue
        index[str(param_id)] = row
    return index


def _support_reason(
    cells: Mapping[str, Mapping[str, Any]],
    param_id: str,
    *,
    version: str,
    map_version: str | None,
) -> str | None:
    """``review_reason`` карты, если цитата «не о предмете»; иначе ``None``.

    Карта применяется **только к своей версии**: ``cells.jsonl`` — карта одной сборки
    одной версии, и переносить её суждение на факт другой версии значило бы выдать
    измерение одной версии за свойство линии. Карта без названной версии не применяется
    вовсе (``map_version is None``) — это видно в выводе (``map_applied``), а не молчит.
    """
    if map_version is None or major_version(map_version) != major_version(version):
        return None
    cell = cells.get(param_id)
    if cell is None:
        return None
    reason = str(cell.get("review_reason") or "")
    return reason if reason.startswith(SUPPORT_NOT_ABOUT_SUBJECT) else None


def propose_baselines(
    *,
    baseline_id: str,
    facts: Sequence[BaselineFact],
    versions: Sequence[str],
    catalog: Sequence[CatalogRow],
    baselines: Sequence[BaselineRow] = (),
    map_cells: Sequence[Mapping[str, Any]] | None = None,
    map_version: str | None = None,
) -> dict[str, Any]:
    """Предложение строк политики по наблюдениям (детерминированно, без сети и LLM).

    :param versions: объявленные версии линии **в порядке декларации** — он и решает, какая
        версия станет ``min_version`` (алфавит дал бы ``"10"`` раньше ``"18"``).
    :param map_cells: строки ``cells.jsonl`` одной версии (``None`` — карты нет).
    :param map_version: версия, к которой относится карта; ``None`` — карта не применяется
        (её суждение о цитате не переносится на неизвестную версию).
    :returns: ``{"rows": [...], "proposed": [...], "direct": [...], ...}``; строки идут в
        порядке каталога (авторский порядок параметров, а не алфавит slug'ов).
    """
    if not (baseline_id or "").strip():
        raise ValueError("baseline_id обязателен: строка политики адресуется линией предка")
    declared = [str(version) for version in versions]
    existing = {(row.baseline_id, row.param_id): row for row in baselines}
    cells = _cells_by_param(map_cells)
    map_applied = bool(cells) and map_version is not None
    grouped = _facts_by_param(facts, baseline_id)

    rows: list[dict[str, Any]] = []
    for param in catalog:
        param_id = str(param.param_id)
        own = grouped.get(param_id, [])
        evidence = _versions_with(own, declared)
        row: dict[str, Any] = {
            "baseline_id": baseline_id,
            "param_id": param_id,
            "min_version": "",
            "inheritance_mode": "",
            "decision": "",
            "reason": "",
            "evidence_versions": _VERSION_SEP.join(evidence),
        }
        current = existing.get((baseline_id, param_id))
        if current is not None:
            # Строка уже есть: политику не перепредлагаем, но показываем, что именно стоит.
            row.update(
                min_version=current.min_version or "",
                inheritance_mode=current.inheritance_mode.value,
                decision=DECISION_EXISTING,
                reason=REASON_EXISTING,
            )
            rows.append(row)
            continue
        accepted = _versions_with(own, declared, review=FactReview.ACCEPTED)
        if accepted:
            min_version = accepted[0]
            support = _support_reason(
                cells, param_id, version=min_version, map_version=map_version
            )
            row.update(
                min_version=min_version,
                inheritance_mode=(
                    InheritanceMode.CONDITIONAL.value
                    if support is not None
                    else InheritanceMode.DIRECT.value
                ),
                reason=(
                    REASON_MAP_SUPPORT_UNRELATED
                    if support is not None
                    else REASON_FACT_PRESENT_ACCEPTED
                ),
            )
            if support is not None:
                # Причина названа: подробность живёт в выводе (в CSV — только токен).
                row["map_review_reason"] = support
            rows.append(row)
            continue
        pending = _versions_with(own, declared, review=FactReview.PENDING)
        if pending:
            # Прибор не подписывает за человека: предлагать нечего, но и молчать нельзя.
            row.update(decision=DECISION_SKIP, reason=REASON_NEEDS_SIGNATURE)
            rows.append(row)
            continue
        row.update(decision=DECISION_SKIP, reason=REASON_NO_EVIDENCE)
        rows.append(row)

    def _params(reason: str) -> list[str]:
        return [row["param_id"] for row in rows if row["reason"] == reason]

    proposed = [row["param_id"] for row in rows if row["decision"] == ""]
    payload: dict[str, Any] = {
        "baseline_id": baseline_id,
        "declared_versions": declared,
        "map_version": map_version,
        "map_applied": map_applied,
        "rows": rows,
        "proposed": proposed,
        "direct": _params(REASON_FACT_PRESENT_ACCEPTED),
        "conditional": _params(REASON_MAP_SUPPORT_UNRELATED),
        "needs_signature": _params(REASON_NEEDS_SIGNATURE),
        "existing": _params(REASON_EXISTING),
        "no_evidence": _params(REASON_NO_EVIDENCE),
    }
    payload["counts"] = {
        "catalog": len(rows),
        "proposed": len(payload["proposed"]),
        "direct": len(payload["direct"]),
        "conditional": len(payload["conditional"]),
        "needs_signature": len(payload["needs_signature"]),
        "existing": len(payload["existing"]),
        "no_evidence": len(payload["no_evidence"]),
    }
    return payload


# ---------------------------------------------------------------------------
# Артефакт: предложение на разметку
# ---------------------------------------------------------------------------


def write_candidates(
    package_dir: str | Path,
    baseline_id: str,
    rows: Sequence[Mapping[str, Any]],
) -> Path:
    """Пишет предложение линии (``sources/baselines.{id}.candidate.csv``) атомарно.

    Порядок строк — как передали (каталог), а не алфавит: файл читает человек, и порядок
    параметров должен совпадать с ``catalog.csv``, иначе сверка идёт глазами по двум
    разным сортировкам.
    """
    path = candidates_path(package_dir, baseline_id)
    return atomic_write_csv(
        path,
        CANDIDATE_COLUMNS,
        ([row.get(column) for column in CANDIDATE_COLUMNS] for row in rows),
    )


def load_candidates(path: str | Path) -> list[dict[str, str]]:
    """Читает артефакт предложения; чужой файл — отказ, а не «нет строк».

    Отсутствие файла и «файл без строк» различимы: первое — предложение не строили,
    второе — построили и нечего применять. Заголовок проверяется целиком: файл, собранный
    другой версией формата, не должен читаться наполовину (колонка ``decision`` из чужого
    порядка заполнила бы не то поле).
    """
    file = Path(path)
    if not file.is_file():
        raise ValueError(
            f"предложение не найдено: {file}. Постройте его: `hound baseline propose "
            f"--package {file.parent.parent} --baseline …`"
        )
    with file.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        header = tuple(reader.fieldnames or ())
        if header != CANDIDATE_COLUMNS:
            raise ValueError(
                f"{file}: заголовок не совпадает с форматом предложения "
                f"(ожидались {', '.join(CANDIDATE_COLUMNS)}; получены "
                f"{', '.join(header) or '—'})"
            )
        return [{key: (value or "") for key, value in row.items()} for row in reader]


def _decision_of(row: Mapping[str, str]) -> str:
    """``decision`` строки в нижнем регистре (``accept``/``reject``/``skip``/``existing``/'')."""
    return (row.get("decision") or "").strip().lower()


def apply_candidates(
    package_dir: str | Path,
    *,
    baseline_id: str,
    actor: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Пишет ``baselines.csv`` **только** по заполненному ``decision`` (``accept``).

    Три отказа вместо догадки:

    * предложенная строка без ``decision`` — отказ с перечнем ``param_id`` и путём файла
      (разметку не выдумываем; это конвенция ``scripts/prepare_*.py``);
    * ``accept`` без ``min_version``/``inheritance_mode`` — отказ: неверный пол молча
      пере- или недо-наследует, а режим решает, условна ли строка;
    * строки другой линии в файле — отказ: предложение адресуется одной линии, и тихо
      применить половину файла значило бы изменить политику, которую не смотрели.

    Запись идёт через :func:`core.pipeline.package_admin.add_baseline` — существующий
    писатель ``baselines.csv`` (проспективный линт, ``apply_log``, атомарная запись), а не
    ручной CSV: вторая запись того же файла разошлась бы с первой по линту и журналу.
    """
    if not (actor or "").strip():
        raise ValueError("baseline propose --apply: --actor обязателен (подпись без автора недоказуема)")
    from core.pipeline.package_admin import add_baseline

    path = candidates_path(package_dir, baseline_id)
    rows = load_candidates(path)
    foreign = sorted({row["baseline_id"] for row in rows} - {baseline_id})
    if foreign:
        raise ValueError(
            f"{path}: файл содержит строки другой линии ({', '.join(foreign)}), "
            f"а применяется {baseline_id!r}: постройте предложение заново "
            "(`hound baseline propose --baseline …`)"
        )
    unfilled = [row["param_id"] for row in rows if _decision_of(row) == ""]
    if unfilled:
        shown = ", ".join(unfilled[:10]) + (" …" if len(unfilled) > 10 else "")
        raise ValueError(
            f"разметка не заполнена: decision пуста у {len(unfilled)} строк ({shown}); "
            f"заполните {path} (accept|reject|skip) — решение о строке политики "
            "прибор не выводит"
        )
    unknown = [
        (row["param_id"], _decision_of(row))
        for row in rows
        if _decision_of(row)
        not in {DECISION_ACCEPT, DECISION_REJECT, DECISION_SKIP, DECISION_EXISTING}
    ]
    if unknown:
        raise ValueError(
            f"{path}: неизвестное decision у {len(unknown)} строк "
            f"({', '.join(f'{param}: {value}' for param, value in unknown[:10])}); "
            "допустимы accept|reject|skip|existing"
        )

    accepted = [row for row in rows if _decision_of(row) == DECISION_ACCEPT]
    missing: list[str] = []
    for row in accepted:
        if not (row.get("min_version") or "").strip():
            missing.append(f"{row['param_id']}: min_version")
        if not (row.get("inheritance_mode") or "").strip():
            missing.append(f"{row['param_id']}: inheritance_mode")
    if missing:
        raise ValueError(
            f"{path}: строка accepted без обязательного поля — "
            + "; ".join(missing[:10])
            + (" …" if len(missing) > 10 else "")
        )

    added: list[dict[str, str]] = []
    for row in accepted:
        add_baseline(
            package_dir,
            row["baseline_id"],
            row["param_id"],
            inheritance_mode=row["inheritance_mode"].strip(),
            min_version=row["min_version"].strip(),
            actor=actor,
            dry_run=dry_run,
        )
        added.append(
            {
                "param_id": row["param_id"],
                "min_version": row["min_version"].strip(),
                "inheritance_mode": row["inheritance_mode"].strip(),
                "reason": row.get("reason", ""),
            }
        )
    skipped = [
        {"param_id": row["param_id"], "decision": _decision_of(row)}
        for row in rows
        if _decision_of(row) != DECISION_ACCEPT
    ]
    return {
        "baseline_id": baseline_id,
        "path": str(path),
        "added": added,
        "skipped": skipped,
        "counts": {
            "rows": len(rows),
            "accepted": len(accepted),
            "added": len(added),
            "skipped": len(skipped),
        },
        "dry_run": dry_run,
    }
